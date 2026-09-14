"""File-based logging for the Big QMT bridge.

Diagnostics used to be print()-only, which is lost when the QMT output panel
scrolls or the terminal restarts. This module wires Python's stdlib ``logging``
to a rotating file so errors survive restarts and can be reviewed after a
crash.

Usage (any module, both server and client):

    from bigqmt_signal_trader.logging_setup import get_logger
    log = get_logger("rpc")
    log.info("started")
    log.error("download failed: %s", exc)

Behavior:
- Log directory resolves to ``<qmt_python_dir>/logs`` when running inside QMT
  (found via a sys.path entry ending in ``\\python``), else ``~/.cache/bigqmt/logs``.
- Rotates at midnight into ``bigqmt.log.YYYY-MM-DD`` backups, keeping the last
  7 days by default (override with env BIGQMT_LOG_RETENTION_DAYS).
- Each record is also printed to stdout so the QMT output panel still shows it.
- Thread-safe (logging is; the print side is best-effort wrapped).
- Never raises: a logging failure must not bring down the strategy.
- Opt out via env BIGQMT_LOG_ENABLED=0 / BIGQMT_LOG_TO_STDOUT=0.
"""

import datetime as _dt
import os
import sys
import time
import traceback

try:
    import logging
    import logging.handlers
except ImportError:
    # Some broker QMT Python bundles omit logging from python36.zip.  The
    # bridge still needs a minimal file/stdout logger to start and report why
    # later operations fail.
    logging = None

_LOGGER_NAME = "bigqmt"
_HANDLER_KIND_ATTR = "_bigqmt_managed_handler_kind"
_LOG_FORMAT = "%(asctime)s [%(levelname)s] [%(name)s] %(message)s"
_LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
_initialized = False


def _env_bool(name, default=True):
    value = os.environ.get(name)
    if value in (None, ""):
        return default
    return str(value).strip().lower() in ("1", "true", "yes", "y", "on")


def _resolve_log_dir():
    """Pick a writable log dir. Prefers the QMT python dir (the sys.path entry
    ending in ``\\python``) so logs sit beside the deployed strategy; falls back
    to a user cache dir otherwise. Deliberately does NOT use this package's own
    src/ directory (a repo checkout is not a writable runtime location)."""
    candidates = []
    for entry in sys.path:
        try:
            if entry and entry.endswith(r"\python") and os.path.isdir(entry):
                candidates.append(entry)
        except Exception:
            continue
    candidates.append(os.path.join(os.path.expanduser("~"), ".cache", "bigqmt"))
    for base in candidates:
        try:
            path = os.path.join(base, "logs")
            os.makedirs(path, exist_ok=True)
            # probe writability
            probe = os.path.join(path, ".write_test")
            with open(probe, "w"):
                pass
            try:
                os.remove(probe)
            except Exception:
                pass
            return path
        except Exception:
            continue
    return None


if logging is not None:
    class _SafeStreamHandler(logging.Handler):
        """print() the record so the QMT output panel shows it; never raises."""

        def emit(self, record):
            try:
                print(self.format(record))
            except Exception:
                pass


class _FallbackLogger:
    """Small logger used only when the broker Python omits stdlib logging."""

    def __init__(self, name):
        self.name = name

    def _write(self, level, message, args, exception_text=None):
        try:
            rendered = str(message) % args if args else str(message)
        except Exception:
            rendered = "%s %s" % (message, args)
        if exception_text:
            rendered = "%s\n%s" % (rendered, exception_text)
        line = "%s [%s] [%s] %s" % (
            _dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), level, self.name, rendered,
        )
        if _env_bool("BIGQMT_LOG_ENABLED", True):
            log_dir = _resolve_log_dir()
            if log_dir is not None:
                try:
                    with open(os.path.join(log_dir, "bigqmt.log"), "a") as log_file:
                        log_file.write(line + "\n")
                except Exception:
                    pass
        if _env_bool("BIGQMT_LOG_TO_STDOUT", True):
            try:
                print(line)
            except Exception:
                pass

    def debug(self, message, *args):
        self._write("DEBUG", message, args)

    def info(self, message, *args):
        self._write("INFO", message, args)

    def warning(self, message, *args):
        self._write("WARNING", message, args)

    def error(self, message, *args):
        self._write("ERROR", message, args)

    def exception(self, message, *args):
        self._write("ERROR", message, args, traceback.format_exc())


def _cleanup_old_logs(log_dir, retention_days):
    """Delete rotated log files older than retention_days.

    TimedRotatingFileHandler only prunes backups at rotation time; this sweeps
    stale files on startup too (e.g. after a weekend gap or a config change).
    """
    try:
        cutoff = time.time() - retention_days * 86400
        for name in os.listdir(log_dir):
            if not (name.startswith("bigqmt") and name.endswith(".log") or ".log." in name):
                continue
            path = os.path.join(log_dir, name)
            try:
                if os.path.getmtime(path) < cutoff:
                    os.remove(path)
            except Exception:
                continue
    except Exception:
        pass


def _uses_legacy_formatter(handler):
    try:
        formatter = handler.formatter
        return (
            formatter is not None
            and getattr(getattr(formatter, "_style", None), "_fmt", None)
            == _LOG_FORMAT
            and formatter.datefmt == _LOG_DATE_FORMAT
        )
    except Exception:
        return False


def _is_legacy_handler(handler, kind, file_path=None, retention_days=None):
    """Match only the exact handler shape created before ownership tagging."""
    try:
        if kind == "file":
            if type(handler) is not logging.handlers.TimedRotatingFileHandler:
                return False
            existing_path = os.path.normcase(os.path.abspath(handler.baseFilename))
            wanted_path = os.path.normcase(os.path.abspath(file_path))
            return (
                existing_path == wanted_path
                and handler.when == "MIDNIGHT"
                and handler.interval == 86400
                and handler.backupCount == retention_days
                and handler.utc is False
                and handler.delay is False
                and handler.level == logging.NOTSET
                and not handler.filters
                and _uses_legacy_formatter(handler)
            )
        if kind == "stdout":
            return (
                handler.__class__.__name__ == "_SafeStreamHandler"
                and handler.__class__.__module__.startswith(
                    "bigqmt_signal_trader.logging_setup"
                )
                and handler.__class__.__bases__ == (logging.Handler,)
                and handler.level == logging.INFO
                and not handler.filters
                and _uses_legacy_formatter(handler)
            )
    except Exception:
        return False
    return False


def _reconcile_managed_handlers(
    logger, kind, file_path=None, retention_days=None
):
    """Keep one owned handler and close duplicate handlers left by reloads."""
    candidates = []
    for handler in list(logger.handlers):
        try:
            handler_kind = getattr(handler, _HANDLER_KIND_ATTR, None)
            if handler_kind == kind or (
                handler_kind is None
                and _is_legacy_handler(
                    handler,
                    kind,
                    file_path=file_path,
                    retention_days=retention_days,
                )
            ):
                candidates.append(handler)
        except Exception:
            continue
    if not candidates:
        return None
    kept_handler = candidates[0]
    setattr(kept_handler, _HANDLER_KIND_ATTR, kind)
    for duplicate in candidates[1:]:
        logger.removeHandler(duplicate)
        try:
            duplicate.close()
        except Exception:
            pass
    return kept_handler


def _add_managed_handler(logger, handler, kind):
    setattr(handler, _HANDLER_KIND_ATTR, kind)
    logger.addHandler(handler)


def _setup():
    global _initialized
    if _initialized:
        return
    if logging is None:
        _initialized = True
        return
    # The logging registry outlives a QMT module reload.  Its own lock makes
    # handler discovery/addition atomic even when two module instances overlap.
    logging._acquireLock()
    try:
        if _initialized:
            return
        logger = logging.getLogger(_LOGGER_NAME)
        logger.setLevel(logging.DEBUG)
        logger.propagate = False
        if not _env_bool("BIGQMT_LOG_ENABLED", True):
            if _reconcile_managed_handlers(logger, "null") is None:
                _add_managed_handler(logger, logging.NullHandler(), "null")
            _initialized = True
            return

        fmt = logging.Formatter(
            fmt=_LOG_FORMAT,
            datefmt=_LOG_DATE_FORMAT,
        )

        # File handler: rotate at midnight, keep the last 7 days only.
        log_dir = _resolve_log_dir()
        if log_dir is not None:
            try:
                fname = os.path.join(log_dir, "bigqmt.log")
                retention_days = int(os.environ.get("BIGQMT_LOG_RETENTION_DAYS", 7))
                if _reconcile_managed_handlers(
                    logger,
                    "file",
                    file_path=fname,
                    retention_days=retention_days,
                ) is None:
                    file_handler = logging.handlers.TimedRotatingFileHandler(
                        fname,
                        when="midnight",
                        interval=1,
                        backupCount=retention_days,
                        encoding="utf-8",
                        utc=False,
                    )
                    file_handler.setFormatter(fmt)
                    _add_managed_handler(logger, file_handler, "file")
                _cleanup_old_logs(log_dir, retention_days)
            except Exception:
                pass

        # Stdout handler so the QMT panel still shows logs.
        if (
            _env_bool("BIGQMT_LOG_TO_STDOUT", True)
            and _reconcile_managed_handlers(logger, "stdout") is None
        ):
            stream = _SafeStreamHandler()
            stream.setLevel(logging.INFO)
            stream.setFormatter(fmt)
            _add_managed_handler(logger, stream, "stdout")

        if not logger.handlers:
            _add_managed_handler(logger, logging.NullHandler(), "null")
        _initialized = True
    finally:
        logging._releaseLock()


def get_logger(name=""):
    """Return a module logger under the shared ``bigqmt`` root.

    ``get_logger("rpc")`` -> logger named ``bigqmt.rpc``; the tag is shown in
    each log line so the old ``[bigqmt_rpc]`` prefixes remain visible.
    """
    _setup()
    suffix = str(name or "").strip(".")
    full = _LOGGER_NAME if not suffix else "%s.%s" % (_LOGGER_NAME, suffix)
    if logging is None:
        return _FallbackLogger(full)
    return logging.getLogger(full)


def log_file_path():
    """Return the current log file path (or None if file logging is off)."""
    _setup()
    log_dir = _resolve_log_dir()
    if log_dir is None:
        return None
    return os.path.join(log_dir, "bigqmt.log")
