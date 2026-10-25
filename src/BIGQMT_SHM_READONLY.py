# coding: utf-8
"""独立共享内存只读桥；必须先通过专用构建器内嵌直接依赖。"""

import base64
import builtins
import ctypes
import datetime as _dt
import json
import math
import threading
import time
import types


BIGQMT_ACCOUNT_ID = "YOUR_ACCOUNT_ID"
BIGQMT_ENVIRONMENT = "SIM"
READONLY_SHM_NAME = "Local\\AQP_SHM_READONLY_SIM"
READONLY_SYMBOL = "000001.SZ"
READONLY_SYMBOLS = None  # None 保留旧单股票配置；SIM允许名单最多64、显式LIVE最多1024只。
READONLY_ENABLE_MARKET_RPC = False
READONLY_MAX_REQUESTS = 180
READONLY_MAX_SECONDS = 60
READONLY_PERSISTENT_READONLY = False  # 仅显式55013746禁写驻留候选；默认仍有界。
READONLY_PERSISTENT_MARKET_RPC = False  # 双源读写候选显式开放已有市场只读方法。
READONLY_PERSISTENT_LIVE_READ_WRITE = False  # 仅显式实盘双源候选，读源始终禁写。
RPC_REVISION = "shm-readonly-single-file-v2"
QUOTE_SUBSCRIPTION_CONTRACT = "SCOPED_TICK_CAPTURE_V1"
QUOTE_SUBSCRIPTION_LIMIT = 128
HISTORY_PAYLOAD_LIMIT_BYTES = 8 * 1024 * 1024 - 16384
_EMBEDDED_SOURCES = {}  # BUILD_READONLY_EMBEDDED
_MODULE_CACHE = {}
_ORIGINAL_IMPORT = builtins.__import__
_PACKAGES = ("bigqmt_signal_trader", "bigqmt_signal_trader.adapters")
_STANDARD_IMPORTS = frozenset(("base64", "builtins", "ctypes", "datetime", "enum", "json", "math", "os", "re", "struct", "threading", "time", "types", "typing"))
_SERVER = None


def _native_windows_session(native_pid):
    kernel = getattr(getattr(ctypes, "windll", None), "kernel32", None)
    if kernel is None:
        return None  # 非Windows本地测试不伪造原生会话；正式代理会拒绝缺失证据。
    session = ctypes.c_uint()
    if not kernel.ProcessIdToSessionId(native_pid, ctypes.byref(session)):
        raise RuntimeError("native Windows session lookup failed")
    return session.value


def _bundle_import(name, module_globals=None, module_locals=None, fromlist=(), level=0):
    if level:
        package = (module_globals or {}).get("__package__", "")
        parts = package.split(".")
        name = ".".join(parts[:len(parts) - level + 1] + ([name] if name else []))
    if name in _EMBEDDED_SOURCES or name in _PACKAGES:
        module = _load_embedded(name)
        return module if fromlist else _load_embedded(name.split(".")[0])
    if name.split(".")[0] not in _STANDARD_IMPORTS:
        raise ImportError("module is outside readonly embedded allowlist: %s" % name)
    return _ORIGINAL_IMPORT(name, module_globals, module_locals, fromlist, 0)


def _load_embedded(name):
    if name in _MODULE_CACHE:
        return _MODULE_CACHE[name]
    if name not in _PACKAGES and name not in _EMBEDDED_SOURCES:
        raise ImportError("embedded readonly module is missing: %s" % name)
    module = types.ModuleType(name)
    is_package = name in _PACKAGES
    module.__package__ = name if is_package else name.rpartition(".")[0]
    module.__file__ = "<embedded:%s>" % name
    if is_package:
        module.__path__ = []
    module_builtins = dict(vars(builtins))
    module_builtins["__import__"] = _bundle_import
    module.__dict__["__builtins__"] = module_builtins
    _MODULE_CACHE[name] = module
    try:
        if not is_package:
            source = base64.b64decode(_EMBEDDED_SOURCES[name]).decode("utf-8")
            exec(compile(source, module.__file__, "exec"), module.__dict__)
        if "." in name:
            parent, _, child = name.rpartition(".")
            setattr(_load_embedded(parent), child, module)
    except Exception:
        _MODULE_CACHE.pop(name, None)
        raise
    return module


class ReadOnlyRequestError(ValueError):
    def __init__(self, code, message, data=None):
        super(ReadOnlyRequestError, self).__init__(message)
        self.code = code
        self.data = data


class ShmReadOnlyBridge:
    def __init__(self, context, query, mailbox=None):
        if not isinstance(BIGQMT_ACCOUNT_ID, str) or not BIGQMT_ACCOUNT_ID or BIGQMT_ACCOUNT_ID == "YOUR_ACCOUNT_ID" or len(BIGQMT_ACCOUNT_ID) > 64:
            raise ValueError("configure the confirmed simulation account")
        symbols = [READONLY_SYMBOL] if READONLY_SYMBOLS is None else READONLY_SYMBOLS
        symbol_limit = 128 if BIGQMT_ENVIRONMENT == "LIVE" and READONLY_PERSISTENT_LIVE_READ_WRITE is True else 64
        allowed_symbol_limit = 1024 if BIGQMT_ENVIRONMENT == "LIVE" and READONLY_PERSISTENT_LIVE_READ_WRITE is True else 64
        if (not isinstance(symbols, (list, tuple)) or not 1 <= len(symbols) <= allowed_symbol_limit
                or any(not isinstance(symbol, str) or len(symbol) != 9
                       or not all("0" <= char <= "9" for char in symbol[:6])
                       or symbol[6:] not in (".SH", ".SZ", ".BJ") for symbol in symbols)
                or len(set(symbols)) != len(symbols)):
            raise ValueError("configure 1-%d unique SH/SZ/BJ quote symbols" % allowed_symbol_limit)
        if (type(READONLY_PERSISTENT_READONLY) is not bool or type(READONLY_PERSISTENT_MARKET_RPC) is not bool
                or type(READONLY_PERSISTENT_LIVE_READ_WRITE) is not bool
                or type(READONLY_ENABLE_MARKET_RPC) is not bool):
            raise ValueError("explicit boolean persistent readonly selection required")
        if BIGQMT_ENVIRONMENT not in ("SIM", "LIVE"):
            raise ValueError("explicit SIM/LIVE environment required")
        if BIGQMT_ENVIRONMENT == "LIVE":
            if (not READONLY_PERSISTENT_LIVE_READ_WRITE or not READONLY_PERSISTENT_READONLY
                    or not all("0" <= char <= "9" for char in BIGQMT_ACCOUNT_ID)
                    or BIGQMT_ACCOUNT_ID == "55013746"):
                raise ValueError("LIVE read source requires explicit persistent LIVE policy and live numeric account")
        elif READONLY_PERSISTENT_LIVE_READ_WRITE:
            raise ValueError("persistent LIVE policy requires LIVE environment")
        if READONLY_PERSISTENT_MARKET_RPC and (not READONLY_PERSISTENT_READONLY or not READONLY_ENABLE_MARKET_RPC):
            raise ValueError("persistent market opt-in requires explicit persistent readonly market source")
        if READONLY_PERSISTENT_READONLY:
            if ((BIGQMT_ACCOUNT_ID != "55013746" and not READONLY_PERSISTENT_LIVE_READ_WRITE)
                    or (READONLY_ENABLE_MARKET_RPC and not READONLY_PERSISTENT_MARKET_RPC)
                    or type(READONLY_MAX_REQUESTS) is not int or READONLY_MAX_REQUESTS != 0
                    or type(READONLY_MAX_SECONDS) is not int or READONLY_MAX_SECONDS != 0):
                raise ValueError("persistent readonly requires simulation55013746, explicit market opt-in and zero budgets")
        elif type(READONLY_MAX_REQUESTS) is not int or not 1 <= READONLY_MAX_REQUESTS <= 1000 or type(READONLY_MAX_SECONDS) is not int or not 1 <= READONLY_MAX_SECONDS <= 1800:
            raise ValueError("readonly request and session budgets must be bounded integers")
        if not callable(query) or not callable(getattr(context, "get_full_tick", None)):
            raise RuntimeError("QMT native readonly query functions are unavailable")
        self.context = context
        self.account_id = BIGQMT_ACCOUNT_ID
        self.environment = BIGQMT_ENVIRONMENT
        self.quote_symbols = tuple(symbols)
        self.quote_symbol_limit = symbol_limit
        self.callback_thread_id = threading.get_ident()
        self.windows_session_id = None
        self.created_at = time.monotonic()
        self.created_at_unix = time.time()
        self.persistent_readonly = READONLY_PERSISTENT_READONLY
        self.request_count = 0
        self.seen_request_ids = {} if self.persistent_readonly else set()
        self.pending = None
        self.closed = False
        self.market_enabled = READONLY_ENABLE_MARKET_RPC is True
        self.history_downloader = globals().get("download_history_data")
        self.event_lock = threading.Lock()
        self.events = []
        self.event_sequence = 0
        self.dropped_count = 0
        self.subscription = None
        self.subscription_generation = 0
        self.callback_error = None
        self.capture_contract = None
        self.capture_subscriptions = {}
        self.capture_error = None
        self.cleanup_error = None
        self.serializer = _load_embedded("aqp_readonly_serializer").to_jsonable
        positions = _load_embedded("bigqmt_signal_trader.adapters.position_bigqmt")
        self.positions = positions.BigQmtPositionProvider(query, account_type="STOCK")
        self.accounts = _load_embedded("bigqmt_signal_trader.adapters.account_readonly").ReadOnlyAccountProvider(query)
        transport = _load_embedded("aqp_readonly_transport")
        transport.validate_name(READONLY_SHM_NAME)
        self.mailbox = mailbox if mailbox is not None else transport.ReadOnlyMailbox(READONLY_SHM_NAME, create=True)

    def start(self):
        self.mailbox.mark_ready()

    def process_request(self, request, sequence=None):
        fields = request if isinstance(request, dict) else {}
        request_id = fields.get("request_id")
        method = fields.get("method")
        response = {
            "schema_version": 1,
            "request_id": request_id if isinstance(request_id, str) and len(request_id) <= 128 else "",
            "account_id": self.account_id,
            "environment": self.environment,
            "method": method if isinstance(method, str) and len(method) <= 64 else "",
            "session_id": self.mailbox.session_id,
            "sequence": fields.get("sequence") if sequence is None else sequence,
            "ok": False, "data": None, "error": "", "server_error": "",
            "handled_at": _dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        try:
            if self.closed or (not self.persistent_readonly and (
                    self.request_count >= READONLY_MAX_REQUESTS or time.monotonic() - self.created_at >= READONLY_MAX_SECONDS)):
                raise ReadOnlyRequestError("SESSION_CLOSED", "readonly session is closed or exhausted")
            if threading.get_ident() != self.callback_thread_id:
                raise ReadOnlyRequestError("WRONG_CALLBACK_THREAD", "QMT calls require the original callback thread")
            if not isinstance(request, dict):
                raise ReadOnlyRequestError("INVALID_REQUEST", "request must be an object")
            if type(fields.get("schema_version")) is not int or fields["schema_version"] != 1:
                raise ReadOnlyRequestError("INVALID_SCHEMA", "schema_version must be integer 1")
            if not isinstance(request_id, str) or not request_id or len(request_id) > 128 or any(ord(char) < 33 or ord(char) > 126 for char in request_id):
                raise ReadOnlyRequestError("INVALID_REQUEST_ID", "request_id requires 1-128 printable ASCII characters without spaces")
            if fields.get("session_id") != self.mailbox.session_id:
                raise ReadOnlyRequestError("SESSION_MISMATCH", "request belongs to another session")
            if type(fields.get("sequence")) is not int or fields["sequence"] <= 0 or fields["sequence"] != response["sequence"]:
                raise ReadOnlyRequestError("SEQUENCE_MISMATCH", "request sequence does not match the mailbox")
            if fields.get("account_id") != self.account_id:
                raise ReadOnlyRequestError("ACCOUNT_MISMATCH", "account_id mismatch")
            if fields.get("environment", None if self.environment == "LIVE" else "SIM") != self.environment:
                raise ReadOnlyRequestError("ENVIRONMENT_MISMATCH", "request environment mismatch")
            params = fields.get("params", {})
            if not isinstance(params, dict):
                raise ReadOnlyRequestError("INVALID_PARAMS", "params must be an object")
            if "account_id" in params and params["account_id"] != self.account_id:
                raise ReadOnlyRequestError("ACCOUNT_MISMATCH", "params account_id mismatch")
            allowed_methods = ("ping", "get_positions", "get_full_tick", "get_ticks", "get_asset", "query_orders", "query_trades")
            if self.market_enabled:
                allowed_methods += ("get_local_data", "get_market_data_ex", "subscribe_quote", "poll_quote_events", "unsubscribe_quote")
                if callable(self.history_downloader):
                    allowed_methods += ("download_history_data",)
            if method == "download_history_data" and self.market_enabled and not callable(self.history_downloader):
                raise ReadOnlyRequestError("CAPABILITY_MISSING", "native download_history_data is not injected")
            if method not in allowed_methods:
                raise ReadOnlyRequestError("METHOD_DENIED", "method is outside the readonly allowlist")
            expires_at = fields.get("read_expires_at_unix")
            if isinstance(expires_at, bool) or not isinstance(expires_at, (int, float)) or not math.isfinite(expires_at) or expires_at > time.time() + 60:
                raise ReadOnlyRequestError("INVALID_DEADLINE", "read deadline must be finite and bounded to 60 seconds")
            if time.time() >= expires_at:
                raise ReadOnlyRequestError("READ_EXPIRED", "read RPC expired before execution")
            if request_id in self.seen_request_ids:
                raise ReadOnlyRequestError("DUPLICATE_REQUEST", "request_id was already processed in this session")
            if self.persistent_readonly:
                if len(self.seen_request_ids) >= 1024:
                    raise ReadOnlyRequestError("REPLAY_CAPACITY_EXHAUSTED", "unexpired or unconfirmed read records are not evicted")
                self.seen_request_ids[request_id] = (expires_at, response["sequence"])
            else:
                self.seen_request_ids.add(request_id)
            if method == "ping":
                data = {"pong": True, "account_id": self.account_id, "environment": self.environment, "allow_order_methods": False,
                        "quote_allowed_symbol_count": len(self.quote_symbols),
                        "quote_request_symbol_limit": self.quote_symbol_limit,
                        "rpc_revision": RPC_REVISION, "server_time": _dt.datetime.now(),
                        "transport": "shm_readonly",
                        "runtime_policy": "PERSISTENT_READONLY" if self.persistent_readonly else "BOUNDED_READONLY",
                        "started_at_unix": self.created_at_unix,
                        "source_deadline_unix": None if self.persistent_readonly else self.created_at_unix + READONLY_MAX_SECONDS,
                        "max_seconds": READONLY_MAX_SECONDS, "max_requests": READONLY_MAX_REQUESTS,
                        "windows_session_id": self.windows_session_id,
                        "background_threads": False, "callback_thread_id": self.callback_thread_id,
                        "managed_history_periods": ["1m", "5m", "15m", "30m", "60m", "1d"] if self.market_enabled and all(
                            callable(getattr(self.context, name, None)) for name in
                            ("get_all_subscription", "subscribe_quote", "unsubscribe_quote", "get_market_data_ex")) else [],
                        "session_id": self.mailbox.session_id, "native_pid": self.mailbox.server_pid, "capabilities": list(allowed_methods)}
                data.update(self.quote_subscription_metadata())
            elif method in ("get_local_data", "get_market_data_ex", "download_history_data", "subscribe_quote", "poll_quote_events", "unsubscribe_quote"):
                data = self.market_request(method, params)
                data.update(account_id=self.account_id, session_id=self.mailbox.session_id,
                            native_pid=self.mailbox.server_pid, windows_session_id=self.windows_session_id)
            elif method == "get_positions":
                evidence = self.positions.get_positions_with_evidence(self.account_id)
                if not evidence["source_evidence"]["complete"]:
                    response["data"] = self.serializer(evidence)
                    raise ReadOnlyRequestError(evidence["source_evidence"]["error_code"] or "POSITION_INCOMPLETE", "native position evidence is incomplete")
                data = evidence if params.get("include_source_evidence") is True else evidence["positions"]
            elif method in ("get_asset", "query_orders", "query_trades"):
                allowed_params = {"account_id", "include_source_evidence"} if method == "get_asset" else {"account_id", "strategy_name", "include_source_evidence"}
                if set(params) - allowed_params:
                    raise ReadOnlyRequestError("INVALID_PARAMS", "unsupported readonly account query parameter")
                if "include_source_evidence" in params and type(params["include_source_evidence"]) is not bool:
                    raise ReadOnlyRequestError("INVALID_PARAMS", "evidence selection must be boolean")
                strategy_name = params.get("strategy_name", "")
                if not isinstance(strategy_name, str) or len(strategy_name) > 128 or any(ord(char) < 32 for char in strategy_name):
                    raise ReadOnlyRequestError("INVALID_PARAMS", "strategy_name must be a bounded string")
                data = self.accounts.get_asset(self.account_id) if method == "get_asset" else self.accounts.query_records(
                    self.account_id, "ORDER" if method == "query_orders" else "DEAL", strategy_name)
                if not data["source_evidence"]["complete"]:
                    response["data"] = self.serializer(data)
                    raise ReadOnlyRequestError(data["source_evidence"]["error_code"] or "ACCOUNT_READ_INCOMPLETE", "native account read evidence is incomplete")
            else:
                if set(params) - {"account_id", "codes", "code"} or ("codes" in params) == ("code" in params):
                    raise ReadOnlyRequestError("INVALID_PARAMS", "provide exactly one quote selector")
                codes = params.get("codes", params.get("code"))
                if isinstance(codes, str):
                    codes = [codes]
                if (not isinstance(codes, list) or not 1 <= len(codes) <= self.quote_symbol_limit
                        or any(not isinstance(code, str) for code in codes)
                        or len(set(codes)) != len(codes)):
                    raise ReadOnlyRequestError("INVALID_PARAMS", "codes must contain 1-%d unique symbols" % self.quote_symbol_limit)
                if any(code not in self.quote_symbols for code in codes):
                    raise ReadOnlyRequestError("SYMBOL_DENIED", "quote symbol is outside the configured allowlist")
                data = self.context.get_full_tick(codes)
                if not isinstance(data, dict):
                    raise ReadOnlyRequestError("QMT_QUOTE_UNAVAILABLE", "native quotes are not an object")
                missing = [code for code in codes if code not in data]
                unexpected = [code for code in data if code not in codes]
                invalid = []
                symbol_errors = {code: {"reasons": ["MISSING_NATIVE_QUOTE"]} for code in missing}
                for code in codes:
                    if code not in data:
                        continue
                    quote = data[code]
                    price = quote.get("lastPrice") if isinstance(quote, dict) else None
                    timestamp = quote.get("time") if isinstance(quote, dict) else None
                    reasons = []
                    if not isinstance(quote, dict):
                        reasons.append("NATIVE_QUOTE_NOT_OBJECT")
                    if price is None:
                        reasons.append("LAST_PRICE_MISSING")
                    elif isinstance(price, bool) or not isinstance(price, (int, float)):
                        reasons.append("LAST_PRICE_NOT_NUMERIC")
                    elif not math.isfinite(price):
                        reasons.append("LAST_PRICE_NONFINITE")
                    elif price <= 0:
                        reasons.append("LAST_PRICE_NONPOSITIVE")
                    if timestamp is None:
                        reasons.append("NATIVE_TIME_MISSING")
                    elif type(timestamp) is not int:
                        reasons.append("NATIVE_TIME_NOT_INTEGER")
                    elif timestamp <= 0:
                        reasons.append("NATIVE_TIME_NONPOSITIVE")
                    if reasons:
                        invalid.append(code)
                        symbol_errors[code] = {"reasons": reasons, "native_last_price": price,
                                               "native_time": timestamp}
                if missing or unexpected or invalid:
                    response["data"] = self.serializer({
                        "quotes": {code: data[code] for code in codes if code in data},
                        "requested_symbols": codes, "missing_symbols": missing,
                        "invalid_symbols": invalid, "unexpected_symbols": unexpected,
                        "symbol_errors": symbol_errors,
                        "source_evidence": {
                            "contract_version": 1, "account_id": self.account_id,
                            "session_id": self.mailbox.session_id, "native_pid": self.mailbox.server_pid,
                            "source": "native_get_full_tick", "queried_at": _dt.datetime.now(_dt.timezone.utc).isoformat(),
                            "requested_symbol_count": len(codes), "returned_symbol_count": len(codes) - len(missing),
                            "valid_symbol_count": len(codes) - len(missing) - len(invalid),
                            "missing_symbol_count": len(missing), "invalid_symbol_count": len(invalid),
                            "unexpected_symbol_count": len(unexpected), "complete": False,
                            "status": "PARTIAL" if len(codes) > len(missing) + len(invalid) else "UNAVAILABLE",
                            "error_code": "QMT_QUOTE_UNAVAILABLE",
                        },
                    })
                    raise ReadOnlyRequestError("QMT_QUOTE_UNAVAILABLE", "native quote batch is incomplete or invalid")
            if time.time() >= expires_at:
                raise ReadOnlyRequestError("READ_EXPIRED_DURING_EXECUTION", "read finished after its deadline; result is unconfirmed")
            response.update(ok=True, data=self.serializer(data))
        except Exception as exc:
            if isinstance(exc, ReadOnlyRequestError) and exc.data is not None:
                response["data"] = self.serializer(exc.data)
            response["error"] = "%s: %s" % (type(exc).__name__, exc)
            response["server_error"] = exc.code if isinstance(exc, ReadOnlyRequestError) else "QMT_QUERY_FAILED"
        if isinstance(response["data"], dict) and isinstance(response["data"].get("source_evidence"), dict):
            response["data"]["source_evidence"].update(session_id=self.mailbox.session_id,
                native_pid=self.mailbox.server_pid, windows_session_id=self.windows_session_id,
                environment=self.environment)
        self.request_count += 1
        print("AQP_SHM_READONLY_RPC " + json.dumps({
            "request_id": response["request_id"], "method": response["method"], "sequence": response["sequence"],
            "ok": response["ok"], "error": response["error"], "callback_thread_id": threading.get_ident(),
        }, sort_keys=True))
        return response

    @staticmethod
    def _history_symbol(code):
        return (isinstance(code, str) and len(code) == 9
                and all("0" <= char <= "9" for char in code[:6]) and code[6:] in (".SH", ".SZ", ".BJ"))

    @staticmethod
    def _history_timestamp(value, period):
        text = str(value)
        if period == "tick" and len(text) in (10, 13) and text.isdigit():
            seconds = int(text) / (1000.0 if len(text) == 13 else 1.0)
            stamp = _dt.datetime.fromtimestamp(seconds, _dt.timezone(_dt.timedelta(hours=8)))
            return stamp.strftime("%Y%m%d%H%M%S") + ".%03d" % (stamp.microsecond // 1000)
        if period == "tick" and len(text) == 17 and text.isdigit():
            text = text[:14] + "." + text[14:]
        compact, separator, fraction = text.partition(".")
        expected = 8 if period == "1d" else 14
        if (len(compact) != expected or not compact.isdigit()
                or separator and (period != "tick" or not fraction.isdigit() or len(fraction) > 6)):
            raise ReadOnlyRequestError("HISTORY_SHAPE_INVALID", "native history index is not a supported timestamp")
        try:
            _dt.datetime.strptime(compact, "%Y%m%d" if period == "1d" else "%Y%m%d%H%M%S")
        except ValueError:
            raise ReadOnlyRequestError("HISTORY_SHAPE_INVALID", "native history index is not a valid timestamp")
        return text

    def _history_request(self, method, params):
        allowed = {"account_id", "code", "period", "start_time", "end_time", "count", "divid_type"}
        if method == "get_market_data_ex":
            allowed = {"account_id", "code", "period", "start_time", "end_time", "count", "dividend_type", "fill_data", "subscribe"}
        code, period = params.get("code"), params.get("period")
        start, end, count = params.get("start_time"), params.get("end_time"), params.get("count")
        if (set(params) - allowed or not self._history_symbol(code)
                or period not in ("1m", "5m", "15m", "30m", "60m", "1d", "tick")
                or params.get("divid_type", "none") != "none" or type(count) is not int or not 1 <= count <= 10000
                or any(not isinstance(v, str) or (v and (len(v) != 14 or not v.isdigit())) for v in (start, end))
                or start and end and start > end):
            raise ReadOnlyRequestError("INVALID_PARAMS", "history requires an explicit SH/SZ/BJ symbol, supported period, count 1-10000 and ordered optional times")
        if method == "get_market_data_ex" and (
                params.get("dividend_type", "none") != "none" or type(params.get("fill_data")) is not bool
                or params.get("subscribe") is not False):
            raise ReadOnlyRequestError("INVALID_PARAMS", "native cached history requires explicit fill_data boolean and subscribe=False")
        for value in (start, end):
            if value:
                try:
                    _dt.datetime.strptime(value, "%Y%m%d%H%M%S")
                except ValueError:
                    raise ReadOnlyRequestError("INVALID_PARAMS", "history timestamp is invalid")
        result = {"status": "UNAVAILABLE", "code": code, "period": period, "index": [], "columns": [], "rows": [],
                  "source_evidence": {"native_method": method, "count": count, "start_time": start, "end_time": end,
                                      "divid_type": "none", "row_count": 0, "cache_only": True,
                                      "download_performed": False, "row_limit": 10000, "payload_limit_bytes": HISTORY_PAYLOAD_LIMIT_BYTES,
                                      "symbol_scope": "EXPLICIT_HISTORY_SYMBOL", "requested_symbol": code}}
        if method == "get_market_data_ex":
            result["source_evidence"].update(dividend_type="none", fill_data=params["fill_data"], subscribe=False)
        native = getattr(self.context, method, None)
        if not callable(native):
            result["source_evidence"]["reason"] = "NATIVE_METHOD_UNAVAILABLE"
            return result
        if method == "get_market_data_ex":
            fields = [] if period == "tick" else ["open", "high", "low", "close", "volume", "amount"]
            raw = native(fields, [code], period=period, start_time=start, end_time=end, count=count,
                         dividend_type="none", fill_data=params["fill_data"], subscribe=False)
            if raw is not None:
                if not isinstance(raw, dict) or set(raw) - {code}:
                    raise ReadOnlyRequestError("HISTORY_SHAPE_INVALID", "native history escaped the requested symbol")
                if code not in raw:
                    result.update(status="EMPTY")
                    result["source_evidence"]["reason"] = "NATIVE_CODE_ABSENT"
                    return result
                raw = raw[code]
        else:
            raw = native(stock_code=code, period=period, start_time=start, end_time=end, count=count, divid_type="none")
        if raw is None:
            result["source_evidence"]["reason"] = "NATIVE_RETURNED_NONE"
            return result
        if isinstance(raw, dict):
            index = list(raw)
            if len(index) > count or any(not isinstance(row, dict) or len(row) > 64 for row in raw.values()):
                raise ReadOnlyRequestError("HISTORY_SHAPE_INVALID", "native history must contain bounded timetag rows")
            columns = []
            for row in raw.values():
                for field in row:
                    if not isinstance(field, str) or not 1 <= len(field) <= 64:
                        raise ReadOnlyRequestError("HISTORY_SHAPE_INVALID", "history field must be a bounded string")
                    if field not in columns:
                        columns.append(field)
            rows = [[raw[tag].get(field) for field in columns] for tag in index]
            result["native_rows"] = self.serializer([{"index": tag, "fields": raw[tag]} for tag in index])
        else:
            if not all(hasattr(raw, attr) for attr in ("index", "columns", "values", "shape")) or len(raw.shape) != 2:
                raise ReadOnlyRequestError("HISTORY_SHAPE_INVALID", "history requires native timetag mapping or two-dimensional frame")
            if raw.shape[0] > count or raw.shape[1] > 64:
                raise ReadOnlyRequestError("HISTORY_SHAPE_INVALID", "native frame exceeds requested bounded shape")
            rows, columns, index = raw.values.tolist(), list(raw.columns), list(raw.index)
        if (len(rows) > count or len(index) != len(rows) or len(columns) > 64
                or any(not isinstance(field, str) or not 1 <= len(field) <= 64 for field in columns)
                or len(set(columns)) != len(columns)
                or any(len(row) != len(columns) for row in rows)):
            raise ReadOnlyRequestError("HISTORY_SHAPE_INVALID", "history exceeds count or has inconsistent axes")
        result.update(status="OK" if rows else "EMPTY", index=self.serializer(index), columns=self.serializer(columns), rows=self.serializer(rows))
        result["source_evidence"]["row_count"] = len(rows)
        if method == "get_market_data_ex" and rows:
            required = {"lastPrice", "volume", "amount"} if period == "tick" else {"open", "high", "low", "close", "volume", "amount"}
            if not required <= set(result["columns"]) or period != "tick" and set(result["columns"]) != required:
                raise ReadOnlyRequestError("HISTORY_SHAPE_INVALID", "native history is missing required price/volume/amount fields")
            previous_tag = ""
            unavailable_last_price_count = 0
            for tag, row in zip(result["index"], result["rows"]):
                text = self._history_timestamp(tag, period)
                compact = text.split(".", 1)[0]
                lower = start[:8] if period == "1d" else start
                upper = end[:8] if period == "1d" else end
                if (lower and compact < lower or upper and compact > upper
                        or previous_tag and (text < previous_tag or period != "tick" and text == previous_tag)):
                    raise ReadOnlyRequestError("HISTORY_SHAPE_INVALID", "native history index escaped the requested window or is unordered")
                previous_tag = text
                record = dict(zip(result["columns"], row))
                for field in required:
                    value = record[field]
                    if (isinstance(value, bool) or not isinstance(value, (int, float))
                            or not math.isfinite(value) or value < 0
                            or field in ("open", "high", "low", "close", "lastPrice") and value == 0
                            and not (period == "tick" and field == "lastPrice"
                                     and record.get("volume") == 0 and record.get("amount") == 0)):
                        raise ReadOnlyRequestError("HISTORY_SHAPE_INVALID", "native history has unavailable or invalid numeric fields")
                if period == "tick" and record["lastPrice"] == 0:
                    unavailable_last_price_count += 1
                if period == "tick" and "time" in record and (type(record["time"]) is not int or record["time"] <= 0):
                    raise ReadOnlyRequestError("HISTORY_SHAPE_INVALID", "native tick time is invalid")
            if unavailable_last_price_count:
                result["source_evidence"]["unavailable_last_price_count"] = unavailable_last_price_count
        if len(json.dumps(result, ensure_ascii=True, allow_nan=False, separators=(",", ":")).encode("ascii")) > HISTORY_PAYLOAD_LIMIT_BYTES:
            raise ReadOnlyRequestError("HISTORY_PAYLOAD_TOO_LARGE", "native history exceeds bounded 8MiB response; request a smaller explicit window/count, no rows were truncated")
        return result

    def _download_request(self, params):
        if not callable(self.history_downloader):
            raise ReadOnlyRequestError("CAPABILITY_MISSING", "native download_history_data is not injected")
        allowed = {"account_id", "code", "period", "start_time", "end_time", "incrementally"}
        code, period = params.get("code"), params.get("period")
        start, end = params.get("start_time"), params.get("end_time")
        incremental = params.get("incrementally")
        if (set(params) - allowed or not self._history_symbol(code) or period not in ("tick", "1m", "5m", "1d")
                or type(incremental) not in (bool, type(None))
                or any(not isinstance(value, str) or len(value) != 8 or not value.isdigit() for value in (start, end))
                or start != end):
            raise ReadOnlyRequestError("INVALID_PARAMS", "download requires an explicit SH/SZ/BJ symbol and one trading day")
        try:
            _dt.datetime.strptime(start, "%Y%m%d")
        except ValueError:
            raise ReadOnlyRequestError("INVALID_PARAMS", "download trading day is invalid")
        args = (code, period, start, end)
        if incremental is None:
            returned = self.history_downloader(*args)
        else:
            returned = self.history_downloader(*args, incrementally=incremental)
        if returned is False:
            raise ReadOnlyRequestError("NATIVE_DOWNLOAD_REJECTED", "native history download returned False")
        return {"status": "REQUESTED", "code": code, "period": period,
                "completion_confirmed": False, "readback_required": True,
                "source_evidence": {"native_method": "download_history_data", "start_time": start,
                                    "end_time": end, "incrementally": incremental,
                                    "native_returned_none": returned is None}}

    def quote_subscription_metadata(self):
        if (self.environment == "LIVE" and self.market_enabled
                and callable(getattr(self.context, "subscribe_quote", None))
                and callable(getattr(self.context, "unsubscribe_quote", None))):
            return {"quote_subscription_contract": QUOTE_SUBSCRIPTION_CONTRACT,
                    "quote_subscription_limit": QUOTE_SUBSCRIPTION_LIMIT}
        return {}

    def _quote_callback(self, subscription, scoped=False):
        def callback(raw):
            with self.event_lock:
                current = (self.capture_subscriptions.get(subscription["subscription_id"])
                           if scoped else self.subscription)
                if self.closed or current is not subscription or not subscription["active"]:
                    return
                try:
                    if not isinstance(raw, dict) or set(raw) != {subscription["code"]}:
                        raise ValueError("callback must contain exactly subscribed symbol")
                    quotes = raw[subscription["code"]]
                    quotes = [quotes] if isinstance(quotes, dict) else quotes
                    if not isinstance(quotes, list) or not 1 <= len(quotes) <= 64:
                        raise ValueError("callback batch must contain 1-64 ticks")
                    for quote in quotes:
                        price = quote.get("lastPrice") if isinstance(quote, dict) else None
                        timestamp = quote.get("time") if isinstance(quote, dict) else None
                        if (isinstance(price, bool) or not isinstance(price, (int, float)) or not math.isfinite(price)
                                or price <= 0 or type(timestamp) is not int or timestamp <= 0):
                            raise ValueError("callback price/time invalid")
                    payload = json.loads(json.dumps(self.serializer(raw), allow_nan=False))
                    sequence = subscription["event_sequence"] if scoped else self.event_sequence
                    event = {"sequence": sequence + 1, "account_id": self.account_id,
                             "session_id": self.mailbox.session_id, "subscription_id": subscription["subscription_id"],
                             "native_time": [quote["time"] for quote in quotes],
                             "received_at": _dt.datetime.now().isoformat(), "data": payload}
                    if scoped:
                        event.update(capture_id=subscription["capture"]["capture_id"], code=subscription["code"])
                    if len(json.dumps(event, ensure_ascii=True, allow_nan=False).encode("ascii")) > 2048:
                        raise ValueError("callback batch exceeds 2048-byte event bound")
                    events = subscription["events"] if scoped else self.events
                    if scoped:
                        subscription["event_sequence"] += 1
                    else:
                        self.event_sequence += 1
                    if len(events) == 32:
                        events.pop(0)
                        if scoped:
                            subscription["dropped_count"] += 1
                        else:
                            self.dropped_count += 1
                    events.append(event)
                except Exception as exc:
                    error = "%s: %s" % (type(exc).__name__, exc)
                    if scoped:
                        subscription["callback_error"] = subscription["callback_error"] or error
                    else:
                        self.callback_error = error
        return callback

    def _capture_params(self, capture, code):
        if (not isinstance(capture, dict)
                or set(capture) != {"capture_id", "trade_date", "start_time", "end_time", "symbols"}):
            raise ReadOnlyRequestError("INVALID_PARAMS", "explicit complete capture contract required")
        identity, day = capture["capture_id"], capture["trade_date"]
        start, end, symbols = capture["start_time"], capture["end_time"], capture["symbols"]
        if (not isinstance(identity, str) or not 1 <= len(identity) <= 128 or identity.strip() != identity
                or any(ord(char) < 32 for char in identity)
                or not isinstance(day, str) or len(day) != 8 or not all("0" <= char <= "9" for char in day)
                or any(not isinstance(value, str) or len(value) != 14
                       or not all("0" <= char <= "9" for char in value) or not value.startswith(day)
                       for value in (start, end)) or start >= end
                or not isinstance(symbols, list) or not 1 <= len(symbols) <= QUOTE_SUBSCRIPTION_LIMIT
                or any(not self._history_symbol(symbol) for symbol in symbols)
                or len(set(symbols)) != len(symbols) or code not in symbols):
            raise ReadOnlyRequestError("INVALID_PARAMS", "capture identity, same-day times and unique symbols are required")
        try:
            _dt.datetime.strptime(day, "%Y%m%d")
            for value in (start, end):
                _dt.datetime.strptime(value, "%Y%m%d%H%M%S")
        except ValueError:
            raise ReadOnlyRequestError("INVALID_PARAMS", "capture date/time is invalid")
        if any(symbol not in self.quote_symbols for symbol in symbols):
            raise ReadOnlyRequestError("SYMBOL_DENIED", "the complete capture must belong to the configured allowlist")
        return dict(capture_id=identity, trade_date=day, start_time=start, end_time=end, symbols=sorted(symbols))

    @staticmethod
    def _capture_subscription_result(subscription):
        keys = ("subscription_id", "account_id", "session_id", "code", "period", "active", "native_handle",
                "initial_sequence", "capture", "registration_error", "cleanup_error", "unconfirmed_native_handle")
        return json.loads(json.dumps({key: subscription[key] for key in keys if key in subscription}))

    def _capture_finished(self):
        return not self.capture_error and bool(self.capture_subscriptions) and all(
            subscription.get("cancel_result") and subscription.get("tail_delivered")
            and not any(subscription.get(key) for key in (
                "delivery_error", "callback_error", "registration_error", "cleanup_error"))
            for subscription in self.capture_subscriptions.values())

    def _subscribe_capture(self, params):
        if (set(params) - {"account_id", "code", "period", "result_type", "dividend_type", "capture"}
                or self.environment != "LIVE" or not self.quote_subscription_metadata()
                or params.get("period", "tick") != "tick" or params.get("result_type", "dict") != "dict"
                or params.get("dividend_type", "none") != "none"):
            raise ReadOnlyRequestError("INVALID_PARAMS", "scoped capture requires explicit LIVE tick/dict/none")
        capture = self._capture_params(params["capture"], params.get("code"))
        if self.subscription is not None:
            raise ReadOnlyRequestError("SUBSCRIPTION_ACTIVE", "legacy and scoped subscriptions cannot mix")
        if self.capture_error:
            raise ReadOnlyRequestError("SUBSCRIPTION_UNCONFIRMED", self.capture_error)
        if self.capture_contract is not None and capture != self.capture_contract:
            if capture["capture_id"] == self.capture_contract["capture_id"] or not self._capture_finished():
                raise ReadOnlyRequestError("CAPTURE_CONFLICT", "another capture contract is retained")
            self.capture_subscriptions = {}
            self.capture_contract = None
        self.capture_contract = capture
        for subscription in self.capture_subscriptions.values():
            if subscription["code"] == params["code"]:
                return self._capture_subscription_result(subscription)
        self.subscription_generation += 1
        subscription = dict(subscription_id="%s-%s" % (self.mailbox.session_id, self.subscription_generation),
            account_id=self.account_id, session_id=self.mailbox.session_id, code=params["code"], period="tick",
            active=True, native_handle=None, initial_sequence=0, capture=capture,
            event_sequence=0, events=[], dropped_count=0, callback_error=None, tail_delivered=False)
        with self.event_lock:
            self.capture_subscriptions[subscription["subscription_id"]] = subscription
        try:
            handle = self.context.subscribe_quote(subscription["code"], period="tick", dividend_type="none", result_type="dict",
                                                  callback=self._quote_callback(subscription, scoped=True))
            if type(handle) is int:
                subscription["unconfirmed_native_handle"] = handle
            if type(handle) is not int or handle < 0:
                raise ReadOnlyRequestError("SUBSCRIPTION_UNCONFIRMED", "native registration returned an unknown handle")
            if any(other is not subscription and other.get("native_handle") == handle
                   for other in self.capture_subscriptions.values()):
                raise ReadOnlyRequestError("SUBSCRIPTION_UNCONFIRMED", "native handle belongs to another scoped owner")
            subscription["native_handle"] = handle
            subscription.pop("unconfirmed_native_handle", None)
        except Exception as exc:
            with self.event_lock:
                subscription["active"] = False
                subscription["registration_error"] = "%s: %s" % (type(exc).__name__, exc)
                self.capture_error = subscription["registration_error"]
            raise ReadOnlyRequestError("SUBSCRIPTION_UNCONFIRMED", self.capture_error,
                                       self._capture_subscription_result(subscription))
        return self._capture_subscription_result(subscription)

    def _poll_capture(self, params):
        after = params.get("after_sequence")
        identity = params.get("subscription_id")
        subscription = self.capture_subscriptions.get(identity) if isinstance(identity, str) else None
        if (set(params) - {"account_id", "after_sequence", "subscription_id"}
                or type(after) is not int or after < 0 or subscription is None):
            raise ReadOnlyRequestError("INVALID_PARAMS", "explicit scoped SID and nonnegative cursor required")
        with self.event_lock:
            latest, events = subscription["event_sequence"], subscription["events"]
            if after > latest:
                raise ReadOnlyRequestError("INVALID_PARAMS", "cursor exceeds scoped latest sequence")
            oldest = events[0]["sequence"] if events else latest + 1
            result = dict(events=[event for event in events if event["sequence"] > after], latest_sequence=latest,
                oldest_sequence=oldest, dropped_count=subscription["dropped_count"], gap=after < oldest - 1,
                subscription_id=subscription["subscription_id"], account_id=self.account_id,
                session_id=self.mailbox.session_id, capture=subscription["capture"], code=subscription["code"],
                active=subscription["active"], callback_error=subscription["callback_error"], delivery="poll",
                active_subscription_count=sum(item["active"] for item in self.capture_subscriptions.values()))
            for key in ("cleanup_error", "registration_error"):
                if key in subscription:
                    result[key] = subscription[key]
            error = ("CAPTURE_EVENT_GAP" if result["gap"] else subscription.get("callback_error")
                     or subscription.get("registration_error") or subscription.get("cleanup_error"))
            if error:
                subscription["delivery_error"] = subscription.get("delivery_error") or error
                self.capture_error = self.capture_error or subscription["delivery_error"]
                subscription["tail_delivered"] = False
            if subscription.get("delivery_error"):
                result["delivery_error"] = subscription["delivery_error"]
            elif subscription.get("cancel_result"):
                subscription["tail_delivered"] = True
            return result

    def _cancel_capture_subscription(self, subscription):
        with self.event_lock:
            subscription["active"] = False
        if subscription.get("cancel_result"):
            return dict(subscription["cancel_result"])
        error = subscription.get("cleanup_error") or subscription.get("registration_error")
        if error:
            raise ReadOnlyRequestError("CLEANUP_FAILED", error, self._capture_subscription_result(subscription))
        try:
            if self.context.unsubscribe_quote(subscription["native_handle"]) is not True:
                raise RuntimeError("native unsubscribe did not confirm success")
        except Exception as exc:
            subscription["cleanup_error"] = "%s: %s" % (type(exc).__name__, exc)
            self.capture_error = self.capture_error or subscription["cleanup_error"]
            raise ReadOnlyRequestError("CLEANUP_FAILED", subscription["cleanup_error"],
                                       self._capture_subscription_result(subscription))
        result = dict(cancelled=True, cleanup_status="CANCELLED", subscription_id=subscription["subscription_id"],
                      account_id=self.account_id, session_id=self.mailbox.session_id, code=subscription["code"],
                      capture=subscription["capture"])
        subscription["cancel_result"] = result
        return dict(result)

    def market_request(self, method, params):
        if method in ("get_local_data", "get_market_data_ex"):
            return self._history_request(method, params)
        if method == "download_history_data":
            return self._download_request(params)
        if method == "subscribe_quote":
            if "capture" in params:
                return self._subscribe_capture(params)
            if self.capture_subscriptions and not self._capture_finished():
                raise ReadOnlyRequestError("SUBSCRIPTION_ACTIVE", "legacy and scoped subscriptions cannot mix")
            if self.capture_subscriptions:
                raise ReadOnlyRequestError("CAPTURE_CONFLICT", "retained scoped records require a new explicit capture")
            if (set(params) - {"account_id", "code", "period", "result_type", "dividend_type"}
                    or params.get("code") not in self.quote_symbols
                    or params.get("period", "tick") not in ("tick", "1m", "5m", "15m", "30m", "60m", "1d")
                    or params.get("result_type", "dict") != "dict"
                    or params.get("dividend_type", "none") != "none"):
                raise ReadOnlyRequestError("INVALID_PARAMS", "subscription requires allowlisted symbol, supported period and dict/none")
            if self.subscription is not None:
                raise ReadOnlyRequestError("SUBSCRIPTION_ACTIVE", "only one subscription is permitted")
            if not callable(getattr(self.context, "subscribe_quote", None)) or not callable(getattr(self.context, "unsubscribe_quote", None)):
                raise ReadOnlyRequestError("SUBSCRIPTION_UNAVAILABLE", "native subscription and cancellation are required")
            period = params.get("period", "tick")
            subscriptions_before = None
            if period != "tick":
                snapshot = getattr(self.context, "get_all_subscription", None)
                if not callable(snapshot):
                    raise ReadOnlyRequestError("SUBSCRIPTION_UNAVAILABLE", "managed history requires a native subscription snapshot")
                subscriptions_before = snapshot()
                if not isinstance(subscriptions_before, dict):
                    raise ReadOnlyRequestError("SUBSCRIPTION_UNAVAILABLE", "native subscription snapshot is not a mapping")
                subscriptions_before = dict(subscriptions_before)
            self.subscription_generation += 1
            subscription = {"subscription_id": "%s-%s" % (self.mailbox.session_id, self.subscription_generation),
                            "account_id": self.account_id, "session_id": self.mailbox.session_id,
                            "code": params["code"], "period": period, "active": True, "native_handle": None}
            with self.event_lock:
                subscription["initial_sequence"] = self.event_sequence
                self.subscription = subscription
                self.dropped_count += len(self.events)
                self.events = []
                self.callback_error = None
            callback = self._quote_callback(subscription)
            try:
                handle = self.context.subscribe_quote(subscription["code"], period=period, dividend_type="none", result_type="dict",
                                                      callback=callback if period == "tick" else None)
                if type(handle) is not int or handle < 0:
                    raise ReadOnlyRequestError("SUBSCRIPTION_UNAVAILABLE", "native subscription returned invalid handle")
                if subscriptions_before is not None and (handle in subscriptions_before or str(handle) in subscriptions_before):
                    raise ReadOnlyRequestError("SUBSCRIPTION_UNCONFIRMED", "returned native handle already existed; never cancel another owner")
                subscription["native_handle"] = handle
                if subscriptions_before is not None:
                    subscription["subscriptions_before_count"] = len(subscriptions_before)
                    self.managed_subscriptions_before = subscriptions_before
            except Exception:
                with self.event_lock:
                    subscription["active"] = False
                    self.subscription = None
                raise
            return dict(subscription)
        if method == "poll_quote_events":
            if "subscription_id" in params:
                return self._poll_capture(params)
            if self.capture_subscriptions:
                if not self._capture_finished():
                    raise ReadOnlyRequestError("SUBSCRIPTION_ACTIVE", "scoped events require an explicit subscription_id")
                if set(params) - {"account_id", "after_sequence"} or type(params.get("after_sequence")) is not int or params["after_sequence"] != 0:
                    raise ReadOnlyRequestError("INVALID_PARAMS", "idle scoped baseline requires cursor zero")
                return dict(events=[], latest_sequence=0, oldest_sequence=1, dropped_count=0, gap=False,
                            subscription_id=None, callback_error=None, delivery="poll", active_subscription_count=0)
            after = params.get("after_sequence")
            if set(params) - {"account_id", "after_sequence"} or type(after) is not int or after < 0:
                raise ReadOnlyRequestError("INVALID_PARAMS", "after_sequence must be nonnegative integer")
            with self.event_lock:
                if after > self.event_sequence:
                    raise ReadOnlyRequestError("INVALID_PARAMS", "after_sequence exceeds latest sequence")
                oldest = self.events[0]["sequence"] if self.events else self.event_sequence + 1
                return {"events": [event for event in self.events if event["sequence"] > after],
                        "latest_sequence": self.event_sequence, "oldest_sequence": oldest,
                        "dropped_count": self.dropped_count, "gap": after < oldest - 1,
                        "subscription_id": self.subscription["subscription_id"] if self.subscription else None,
                        "callback_error": self.callback_error, "delivery": "poll",
                        "active_subscription_count": int(self.subscription is not None and self.subscription["active"])}
        identity = params.get("subscription_id")
        scoped = self.capture_subscriptions.get(identity) if isinstance(identity, str) else None
        if scoped is not None and not set(params) - {"account_id", "subscription_id"}:
            return self._cancel_capture_subscription(scoped)
        if set(params) - {"account_id", "subscription_id"} or self.subscription is None or params.get("subscription_id") != self.subscription["subscription_id"]:
            raise ReadOnlyRequestError("INVALID_PARAMS", "explicit active subscription_id is required")
        return self.cancel_subscription()

    def cancel_subscription(self):
        subscription = self.subscription
        if subscription is None:
            return {"cancelled": True, "cleanup_status": "NO_SUBSCRIPTION"}
        # Close callback gate even if native cancellation fails; retain handle for cleanup evidence.
        with self.event_lock:
            subscription["active"] = False
        if subscription.get("cleanup_error"):
            raise ReadOnlyRequestError("CLEANUP_FAILED", subscription["cleanup_error"])
        try:
            if self.context.unsubscribe_quote(subscription["native_handle"]) is not True:
                raise RuntimeError("native unsubscribe did not confirm success")
            if subscription.get("period", "tick") != "tick":
                after = self.context.get_all_subscription()
                if not isinstance(after, dict) or after != self.managed_subscriptions_before:
                    raise RuntimeError("native subscription table did not return to its original state")
        except Exception as exc:
            subscription["cleanup_error"] = "%s: %s" % (type(exc).__name__, exc)
            raise ReadOnlyRequestError("CLEANUP_FAILED", subscription["cleanup_error"])
        self.subscription = None
        result = {"cancelled": True, "cleanup_status": "CANCELLED", "subscription_id": subscription["subscription_id"]}
        if "subscriptions_before_count" in subscription:
            result.update(subscriptions_before=subscription["subscriptions_before_count"], subscriptions_after=len(after))
        return result

    def drain(self):
        if self.closed:
            return
        if threading.get_ident() != self.callback_thread_id:
            raise ReadOnlyRequestError("WRONG_CALLBACK_THREAD", "mailbox polling requires the original QMT callback thread")
        try:
            if self.persistent_readonly and self.pending is None and self.mailbox.region.acquire(0):
                try:
                    confirmed = self.mailbox._header()[6]
                finally:
                    self.mailbox.region.release()
                now = time.time()
                for rid, (expires, sequence) in list(self.seen_request_ids.items()):
                    if expires <= now and sequence <= confirmed:
                        self.seen_request_ids.pop(rid)
            if not self.persistent_readonly and time.monotonic() - self.created_at >= READONLY_MAX_SECONDS:
                self.close("deadline")
                return
            if self.pending is None:
                if not self.persistent_readonly and self.request_count >= READONLY_MAX_REQUESTS:
                    self.close("request_limit")
                    return
                incoming = self.mailbox.read_request()
                if incoming is None:
                    return
                sequence, request = incoming
                self.pending = (sequence, self.process_request(request, sequence=sequence))
            sequence, response = self.pending
            if self.mailbox.publish_response(sequence, response):
                self.pending = None
                if not self.persistent_readonly and self.request_count >= READONLY_MAX_REQUESTS:
                    self.close("request_limit")
        except Exception as exc:
            print("AQP_SHM_READONLY_ERROR %s: %s" % (type(exc).__name__, exc))
            try:
                self.close("callback_error", failed=True)
            except Exception as cleanup:
                print("AQP_SHM_READONLY_CLEANUP_ERROR %s: %s" % (type(cleanup).__name__, cleanup))
            raise

    def close(self, reason="stop", failed=False):
        if self.closed:
            if self.cleanup_error is not None:
                raise self.cleanup_error
            return
        self.closed = True
        cleanup_error = None
        try:
            self.cancel_subscription()
        except Exception as exc:
            cleanup_error = exc
        for subscription in self.capture_subscriptions.values():
            try:
                self._cancel_capture_subscription(subscription)
            except Exception as exc:
                cleanup_error = cleanup_error or exc
        self.cleanup_error = cleanup_error
        try:
            self.mailbox.close(failed=failed or cleanup_error is not None)
        except Exception as exc:
            self.cleanup_error = self.cleanup_error or exc
            print("AQP_SHM_READONLY_CLOSED " + json.dumps({"reason": reason, "requests": self.request_count, "native_handles_closed": False, "cleanup_error": str(exc)}, sort_keys=True))
            raise
        print("AQP_SHM_READONLY_CLOSED " + json.dumps({"reason": reason, "requests": self.request_count, "native_handles_closed": True,
              "subscription_cancelled": cleanup_error is None, "cleanup_status": "CLEANUP_FAILED" if cleanup_error else "COMPLETE", "cleanup_error": str(cleanup_error) if cleanup_error else None}, sort_keys=True))
        if cleanup_error is not None:
            raise cleanup_error


def create_readonly_client(name, account_id):
    return _load_embedded("aqp_readonly_transport").ShmReadOnlyClient(name, account_id)


def init(ContextInfo):
    global _SERVER
    if _SERVER is not None:
        _SERVER.close("reload")
        _SERVER = None
    if not _EMBEDDED_SOURCES:
        raise RuntimeError("build with tools/build_shm_readonly_single_file.py before running")
    try:
        _SERVER = ShmReadOnlyBridge(ContextInfo, globals().get("get_trade_detail_data"))
        _SERVER.windows_session_id = _native_windows_session(_SERVER.mailbox.server_pid)
        _SERVER.start()
        start_at = (_dt.datetime.now() + _dt.timedelta(seconds=1)).strftime("%Y-%m-%d %H:%M:%S")
        ContextInfo.run_time("aqp_shm_readonly_adjust", "100nMilliSecond", start_at)
    except Exception as exc:
        if _SERVER is not None:
            try:
                _SERVER.close("init_failed", failed=True)
            except Exception as cleanup:
                print("AQP_SHM_READONLY_CLEANUP_ERROR %s: %s" % (type(cleanup).__name__, cleanup))
        print("AQP_SHM_READONLY_INIT_FAILED %s: %s" % (type(exc).__name__, exc))
        raise
    print("AQP_SHM_READONLY_READY " + json.dumps({
        "account_id": _SERVER.account_id, "environment": _SERVER.environment, "name": READONLY_SHM_NAME, "symbol": READONLY_SYMBOL,
        "symbols": list(_SERVER.quote_symbols),
        "quote_allowed_symbol_count": len(_SERVER.quote_symbols),
        "quote_request_symbol_limit": _SERVER.quote_symbol_limit,
        "session_id": _SERVER.mailbox.session_id, "native_pid": _SERVER.mailbox.server_pid,
        "windows_session_id": _SERVER.windows_session_id,
        "allow_order_methods": False,
        "max_requests": READONLY_MAX_REQUESTS, "max_seconds": READONLY_MAX_SECONDS,
        "runtime_policy": "PERSISTENT_READONLY" if _SERVER.persistent_readonly else "BOUNDED_READONLY",
        "started_at_unix": _SERVER.created_at_unix,
        "source_deadline_unix": None if _SERVER.persistent_readonly else _SERVER.created_at_unix + READONLY_MAX_SECONDS,
        "background_threads": False, "rpc_revision": RPC_REVISION,
        **_SERVER.quote_subscription_metadata(),
    }, sort_keys=True))


def aqp_shm_readonly_adjust(ContextInfo):
    if _SERVER is not None:
        _SERVER.drain()


def handlebar(ContextInfo):
    return None


def stop(ContextInfo):
    if _SERVER is not None:
        _SERVER.close()
