import builtins
import io
import logging
import logging.handlers
from pathlib import Path


def test_logging_fallback_writes_when_broker_python_omits_logging(monkeypatch, tmp_path):
    source_path = Path(__file__).parents[2] / "src" / "bigqmt_signal_trader" / "logging_setup.py"
    original_import = builtins.__import__

    def import_without_logging(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "logging" or name.startswith("logging."):
            raise ImportError("broker python omits logging")
        return original_import(name, globals, locals, fromlist, level)

    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("BIGQMT_LOG_TO_STDOUT", "0")
    namespace = {
        "__name__": "bigqmt_logging_fallback_test",
        "__builtins__": dict(vars(builtins), __import__=import_without_logging),
    }
    exec(compile(source_path.read_bytes(), str(source_path), "exec"), namespace)

    logger = namespace["get_logger"]("rpc")
    logger.error("startup failed: %s", "missing module")

    log_path = Path(namespace["log_file_path"]())
    assert "[bigqmt.rpc] startup failed: missing module" in log_path.read_text()


def test_logging_reload_reuses_managed_handlers_and_preserves_external_handler(
    monkeypatch, tmp_path
):
    source_path = Path(__file__).parents[2] / "src" / "bigqmt_signal_trader" / "logging_setup.py"
    root_logger = logging.getLogger("bigqmt")
    original_handlers = list(root_logger.handlers)
    original_level = root_logger.level
    original_propagate = root_logger.propagate
    external_output = io.StringIO()
    external_handler = logging.StreamHandler(external_output)

    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("BIGQMT_LOG_ENABLED", "1")
    monkeypatch.setenv("BIGQMT_LOG_TO_STDOUT", "0")
    for handler in original_handlers:
        root_logger.removeHandler(handler)
    root_logger.addHandler(external_handler)

    try:
        for reload_index in range(2):
            namespace = {
                "__name__": "bigqmt_signal_trader.logging_setup_reload_%s" % reload_index,
                "__builtins__": vars(builtins),
            }
            exec(compile(source_path.read_bytes(), str(source_path), "exec"), namespace)
            logger = namespace["get_logger"]("rpc")

        logger.error("reload duplicate evidence")
        for handler in root_logger.handlers:
            handler.flush()

        log_path = tmp_path / ".cache" / "bigqmt" / "logs" / "bigqmt.log"
        assert log_path.read_text().count("reload duplicate evidence") == 1
        assert external_output.getvalue().count("reload duplicate evidence") == 1
        assert external_handler in root_logger.handlers
    finally:
        for handler in list(root_logger.handlers):
            root_logger.removeHandler(handler)
            if handler not in original_handlers and handler is not external_handler:
                handler.close()
        external_handler.close()
        for handler in original_handlers:
            root_logger.addHandler(handler)
        root_logger.setLevel(original_level)
        root_logger.propagate = original_propagate


def test_logging_upgrade_collapses_duplicate_unmarked_legacy_handlers(
    monkeypatch, tmp_path
):
    source_path = Path(__file__).parents[2] / "src" / "bigqmt_signal_trader" / "logging_setup.py"
    root_logger = logging.getLogger("bigqmt")
    original_handlers = list(root_logger.handlers)
    original_level = root_logger.level
    original_propagate = root_logger.propagate
    log_dir = tmp_path / ".cache" / "bigqmt" / "logs"
    log_dir.mkdir(parents=True)
    legacy_formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    legacy_file_handlers = []
    for _ in range(2):
        handler = logging.handlers.TimedRotatingFileHandler(
            str(log_dir / "bigqmt.log"),
            when="midnight",
            interval=1,
            backupCount=7,
            encoding="utf-8",
            utc=False,
        )
        handler.setFormatter(legacy_formatter)
        legacy_file_handlers.append(handler)
    legacy_stream_output = io.StringIO()
    legacy_stream_type = type(
        "_SafeStreamHandler",
        (logging.Handler,),
        {
            "__module__": "bigqmt_signal_trader.logging_setup",
            "emit": lambda self, record: print(
                self.format(record), file=legacy_stream_output
            ),
        },
    )
    legacy_stream_handlers = [legacy_stream_type(), legacy_stream_type()]
    for handler in legacy_stream_handlers:
        handler.setLevel(logging.INFO)
        handler.setFormatter(legacy_formatter)
    external_output = io.StringIO()
    external_handler = logging.StreamHandler(external_output)

    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("BIGQMT_LOG_ENABLED", "1")
    monkeypatch.setenv("BIGQMT_LOG_TO_STDOUT", "1")
    for handler in original_handlers:
        root_logger.removeHandler(handler)
    for handler in legacy_file_handlers + legacy_stream_handlers:
        root_logger.addHandler(handler)
    root_logger.addHandler(external_handler)

    try:
        for reload_index in range(2):
            namespace = {
                "__name__": "bigqmt_signal_trader.logging_setup",
                "__builtins__": vars(builtins),
            }
            exec(compile(source_path.read_bytes(), str(source_path), "exec"), namespace)
            logger = namespace["get_logger"]("rpc")

        logger.error("legacy duplicate evidence")
        for handler in root_logger.handlers:
            handler.flush()

        assert legacy_file_handlers[0] in root_logger.handlers
        assert legacy_file_handlers[1] not in root_logger.handlers
        assert legacy_file_handlers[1]._closed
        assert legacy_stream_handlers[0] in root_logger.handlers
        assert legacy_stream_handlers[1] not in root_logger.handlers
        assert legacy_stream_handlers[1]._closed
        assert external_handler in root_logger.handlers
        assert len(root_logger.handlers) == 3
        assert (log_dir / "bigqmt.log").read_text().count("legacy duplicate evidence") == 1
        assert legacy_stream_output.getvalue().count("legacy duplicate evidence") == 1
        assert external_output.getvalue().count("legacy duplicate evidence") == 1
    finally:
        for handler in list(root_logger.handlers):
            root_logger.removeHandler(handler)
            if handler not in original_handlers:
                handler.close()
        for handler in original_handlers:
            root_logger.addHandler(handler)
        root_logger.setLevel(original_level)
        root_logger.propagate = original_propagate


def test_logging_upgrade_preserves_same_path_external_file_handler(
    monkeypatch, tmp_path
):
    source_path = Path(__file__).parents[2] / "src" / "bigqmt_signal_trader" / "logging_setup.py"
    root_logger = logging.getLogger("bigqmt")
    original_handlers = list(root_logger.handlers)
    original_level = root_logger.level
    original_propagate = root_logger.propagate
    log_dir = tmp_path / ".cache" / "bigqmt" / "logs"
    log_dir.mkdir(parents=True)
    external_handler = logging.handlers.TimedRotatingFileHandler(
        str(log_dir / "bigqmt.log"), when="H", backupCount=1, encoding="utf-8"
    )
    external_handler.setFormatter(logging.Formatter("external %(message)s"))

    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("BIGQMT_LOG_ENABLED", "1")
    monkeypatch.setenv("BIGQMT_LOG_TO_STDOUT", "0")
    for handler in original_handlers:
        root_logger.removeHandler(handler)
    root_logger.addHandler(external_handler)

    try:
        namespace = {
            "__name__": "bigqmt_signal_trader.logging_setup",
            "__builtins__": vars(builtins),
        }
        exec(compile(source_path.read_bytes(), str(source_path), "exec"), namespace)
        namespace["get_logger"]("rpc")

        assert external_handler in root_logger.handlers
        assert len(root_logger.handlers) == 2
    finally:
        for handler in list(root_logger.handlers):
            root_logger.removeHandler(handler)
            if handler not in original_handlers:
                handler.close()
        for handler in original_handlers:
            root_logger.addHandler(handler)
        root_logger.setLevel(original_level)
        root_logger.propagate = original_propagate
