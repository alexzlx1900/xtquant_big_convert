# coding: utf-8
"""独立共享内存只读桥；必须先通过专用构建器内嵌直接依赖。"""

import base64
import builtins
import datetime as _dt
import json
import math
import threading
import time
import types


BIGQMT_ACCOUNT_ID = "YOUR_ACCOUNT_ID"
READONLY_SHM_NAME = "Local\\AQP_SHM_READONLY_SIM"
READONLY_SYMBOL = "000001.SZ"
READONLY_SYMBOLS = None  # None 保留旧单股票配置；显式列表最多 64 只。
READONLY_ENABLE_MARKET_RPC = False
READONLY_MAX_REQUESTS = 180
READONLY_MAX_SECONDS = 60
RPC_REVISION = "shm-readonly-single-file-v2"
_EMBEDDED_SOURCES = {}  # BUILD_READONLY_EMBEDDED
_MODULE_CACHE = {}
_ORIGINAL_IMPORT = builtins.__import__
_PACKAGES = ("bigqmt_signal_trader", "bigqmt_signal_trader.adapters")
_STANDARD_IMPORTS = frozenset(("base64", "builtins", "ctypes", "datetime", "enum", "json", "math", "os", "re", "struct", "threading", "time", "types", "typing"))
_SERVER = None


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
    def __init__(self, code, message):
        super(ReadOnlyRequestError, self).__init__(message)
        self.code = code


class ShmReadOnlyBridge:
    def __init__(self, context, query, mailbox=None):
        if not isinstance(BIGQMT_ACCOUNT_ID, str) or not BIGQMT_ACCOUNT_ID or BIGQMT_ACCOUNT_ID == "YOUR_ACCOUNT_ID" or len(BIGQMT_ACCOUNT_ID) > 64:
            raise ValueError("configure the confirmed simulation account")
        symbols = [READONLY_SYMBOL] if READONLY_SYMBOLS is None else READONLY_SYMBOLS
        if (not isinstance(symbols, (list, tuple)) or not 1 <= len(symbols) <= 64
                or any(not isinstance(symbol, str) or len(symbol) != 9
                       or not all("0" <= char <= "9" for char in symbol[:6])
                       or symbol[6:] not in (".SH", ".SZ", ".BJ") for symbol in symbols)
                or len(set(symbols)) != len(symbols)):
            raise ValueError("configure 1-64 unique SH/SZ/BJ quote symbols")
        if type(READONLY_MAX_REQUESTS) is not int or not 1 <= READONLY_MAX_REQUESTS <= 1000 or type(READONLY_MAX_SECONDS) is not int or not 1 <= READONLY_MAX_SECONDS <= 1800:
            raise ValueError("readonly request and session budgets must be bounded integers")
        if not callable(query) or not callable(getattr(context, "get_full_tick", None)):
            raise RuntimeError("QMT native readonly query functions are unavailable")
        self.context = context
        self.account_id = BIGQMT_ACCOUNT_ID
        self.quote_symbols = tuple(symbols)
        self.callback_thread_id = threading.get_ident()
        self.created_at = time.monotonic()
        self.request_count = 0
        self.seen_request_ids = set()
        self.pending = None
        self.closed = False
        self.market_enabled = READONLY_ENABLE_MARKET_RPC is True
        self.event_lock = threading.Lock()
        self.events = []
        self.event_sequence = 0
        self.dropped_count = 0
        self.subscription = None
        self.subscription_generation = 0
        self.callback_error = None
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
            "method": method if isinstance(method, str) and len(method) <= 64 else "",
            "session_id": self.mailbox.session_id,
            "sequence": fields.get("sequence") if sequence is None else sequence,
            "ok": False, "data": None, "error": "", "server_error": "",
            "handled_at": _dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        try:
            if self.closed or self.request_count >= READONLY_MAX_REQUESTS or time.monotonic() - self.created_at >= READONLY_MAX_SECONDS:
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
            params = fields.get("params", {})
            if not isinstance(params, dict):
                raise ReadOnlyRequestError("INVALID_PARAMS", "params must be an object")
            if "account_id" in params and params["account_id"] != self.account_id:
                raise ReadOnlyRequestError("ACCOUNT_MISMATCH", "params account_id mismatch")
            allowed_methods = ("ping", "get_positions", "get_full_tick", "get_ticks", "get_asset", "query_orders", "query_trades")
            if self.market_enabled:
                allowed_methods += ("get_local_data", "get_market_data_ex", "subscribe_quote", "poll_quote_events", "unsubscribe_quote")
            if method not in allowed_methods:
                raise ReadOnlyRequestError("METHOD_DENIED", "method is outside the readonly allowlist")
            expires_at = fields.get("read_expires_at_unix")
            if isinstance(expires_at, bool) or not isinstance(expires_at, (int, float)) or not math.isfinite(expires_at) or expires_at > time.time() + 60:
                raise ReadOnlyRequestError("INVALID_DEADLINE", "read deadline must be finite and bounded to 60 seconds")
            if time.time() >= expires_at:
                raise ReadOnlyRequestError("READ_EXPIRED", "read RPC expired before execution")
            if request_id in self.seen_request_ids:
                raise ReadOnlyRequestError("DUPLICATE_REQUEST", "request_id was already processed in this session")
            self.seen_request_ids.add(request_id)
            if method == "ping":
                data = {"pong": True, "account_id": self.account_id, "allow_order_methods": False,
                        "rpc_revision": RPC_REVISION, "server_time": _dt.datetime.now(),
                        "background_threads": False, "callback_thread_id": self.callback_thread_id,
                        "session_id": self.mailbox.session_id, "native_pid": self.mailbox.server_pid, "capabilities": list(allowed_methods)}
            elif method in ("get_local_data", "get_market_data_ex", "subscribe_quote", "poll_quote_events", "unsubscribe_quote"):
                data = self.market_request(method, params)
            elif method == "get_positions":
                evidence = self.positions.get_positions_with_evidence(self.account_id)
                if not evidence["source_evidence"]["complete"]:
                    response["data"] = self.serializer(evidence)
                    raise ReadOnlyRequestError(evidence["source_evidence"]["error_code"] or "POSITION_INCOMPLETE", "native position evidence is incomplete")
                data = evidence if params.get("include_source_evidence") is True else evidence["positions"]
            elif method in ("get_asset", "query_orders", "query_trades"):
                allowed_params = {"account_id"} if method == "get_asset" else {"account_id", "strategy_name"}
                if set(params) - allowed_params:
                    raise ReadOnlyRequestError("INVALID_PARAMS", "unsupported readonly account query parameter")
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
                if (not isinstance(codes, list) or not 1 <= len(codes) <= 64
                        or any(not isinstance(code, str) for code in codes)
                        or len(set(codes)) != len(codes)):
                    raise ReadOnlyRequestError("INVALID_PARAMS", "codes must contain 1-64 unique symbols")
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
            response["error"] = "%s: %s" % (type(exc).__name__, exc)
            response["server_error"] = exc.code if isinstance(exc, ReadOnlyRequestError) else "QMT_QUERY_FAILED"
        self.request_count += 1
        print("AQP_SHM_READONLY_RPC " + json.dumps({
            "request_id": response["request_id"], "method": response["method"], "sequence": response["sequence"],
            "ok": response["ok"], "error": response["error"], "callback_thread_id": threading.get_ident(),
        }, sort_keys=True))
        return response

    def market_request(self, method, params):
        if method in ("get_local_data", "get_market_data_ex"):
            allowed = {"account_id", "code", "period", "start_time", "end_time", "count", "divid_type"}
            if method == "get_market_data_ex":
                allowed = {"account_id", "code", "period", "start_time", "end_time", "count", "dividend_type", "fill_data", "subscribe"}
            if set(params) - allowed:
                raise ReadOnlyRequestError("INVALID_PARAMS", "unsupported history parameter")
            code, period = params.get("code"), params.get("period")
            start, end, count = params.get("start_time"), params.get("end_time"), params.get("count")
            if (code not in self.quote_symbols or period not in ("1m", "1d")
                    or params.get("divid_type", "none") != "none" or type(count) is not int or not 1 <= count <= 10
                    or any(not isinstance(v, str) or len(v) != 14 or not v.isdigit() for v in (start, end))
                    or start > end):
                raise ReadOnlyRequestError("INVALID_PARAMS", "history requires allowlisted symbol, period, times and count 1-10")
            if method == "get_market_data_ex" and (
                    start[:8] != end[:8] or params.get("dividend_type", "none") != "none"
                    or params.get("fill_data") is not False or params.get("subscribe") is not False):
                raise ReadOnlyRequestError("INVALID_PARAMS", "native cached history requires one day and fill_data/subscribe=False")
            for value in (start, end):
                try:
                    _dt.datetime(int(value[:4]), int(value[4:6]), int(value[6:8]), int(value[8:10]), int(value[10:12]), int(value[12:14]))
                except ValueError:
                    raise ReadOnlyRequestError("INVALID_PARAMS", "history timestamp is invalid")
            result = {"status": "UNAVAILABLE", "code": code, "period": period, "index": [], "columns": [], "rows": [],
                      "source_evidence": {"native_method": method, "count": count, "start_time": start, "end_time": end,
                                          "divid_type": "none", "row_count": 0}}
            if method == "get_market_data_ex":
                result["source_evidence"].update(dividend_type="none", fill_data=False, subscribe=False)
            native = getattr(self.context, method, None)
            if not callable(native):
                result["source_evidence"]["reason"] = "NATIVE_METHOD_UNAVAILABLE"
                return result
            if method == "get_market_data_ex":
                raw = native(["open", "high", "low", "close", "volume", "amount"], [code],
                             period=period, start_time=start, end_time=end, count=count,
                             dividend_type="none", fill_data=False, subscribe=False)
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
                if (len(index) > count or any(not isinstance(row, dict) or len(row) > 64 for row in raw.values())
                        or any((type(tag) is not int and not isinstance(tag, str)) or isinstance(tag, bool)
                               or (type(tag) is int and tag <= 0) or (isinstance(tag, str) and (not tag or len(tag) > 32)) for tag in index)):
                    raise ReadOnlyRequestError("HISTORY_SHAPE_INVALID", "native history must contain bounded timetag rows")
                columns = []
                for row in raw.values():
                    for field in row:
                        if not isinstance(field, str):
                            raise ReadOnlyRequestError("HISTORY_SHAPE_INVALID", "history field must be a string")
                        if field not in columns:
                            columns.append(field)
                # Keep sparse native rows explicitly; absent fields remain None, never filled from another row.
                rows = [[raw[tag].get(field) for field in columns] for tag in index]
                result["native_rows"] = self.serializer([{ "index": tag, "fields": raw[tag]} for tag in index])
            else:
                if not all(hasattr(raw, attr) for attr in ("index", "columns", "values", "shape")) or len(raw.shape) != 2:
                    raise ReadOnlyRequestError("HISTORY_SHAPE_INVALID", "history requires native timetag mapping or two-dimensional frame")
                rows = raw.values.tolist()
                columns = list(raw.columns)
                index = list(raw.index)
            if len(rows) > count or len(index) != len(rows) or any(len(row) != len(columns) for row in rows):
                raise ReadOnlyRequestError("HISTORY_SHAPE_INVALID", "history exceeds count or has inconsistent axes")
            if len(columns) > 64:
                raise ReadOnlyRequestError("HISTORY_SHAPE_INVALID", "history field count exceeds bounded shape")
            result.update(status="OK" if rows else "EMPTY", index=self.serializer(index), columns=self.serializer(columns), rows=self.serializer(rows))
            result["source_evidence"]["row_count"] = len(rows)
            if method == "get_market_data_ex" and rows:
                if (len(set(result["columns"])) != len(result["columns"])
                        or set(result["columns"]) != {"open", "high", "low", "close", "volume", "amount"}):
                    raise ReadOnlyRequestError("HISTORY_SHAPE_INVALID", "native cached history requires six OHLCV fields")
                previous_tag = ""
                for tag in result["index"]:
                    text = str(tag)
                    expected_length = 14 if period == "1m" else 8
                    if (len(text) != expected_length or not text.isdigit()
                            or text[:8] != start[:8] or text <= previous_tag
                            or (period == "1m" and not start <= text <= end)):
                        raise ReadOnlyRequestError("HISTORY_SHAPE_INVALID", "native history index is outside the requested window or unordered")
                    try:
                        _dt.datetime.strptime(text, "%Y%m%d%H%M%S" if period == "1m" else "%Y%m%d")
                    except ValueError:
                        raise ReadOnlyRequestError("HISTORY_SHAPE_INVALID", "native history index is not a valid timestamp")
                    previous_tag = text
                for row in result["rows"]:
                    for field, value in zip(result["columns"], row):
                        if (isinstance(value, bool) or not isinstance(value, (int, float))
                                or not math.isfinite(value) or value < 0
                                or (field in ("open", "high", "low", "close") and value == 0)):
                            raise ReadOnlyRequestError("HISTORY_SHAPE_INVALID", "native history has unavailable or invalid numeric fields")
            if len(json.dumps(result, ensure_ascii=True, allow_nan=False).encode("ascii")) > 32768:
                raise ReadOnlyRequestError("HISTORY_SHAPE_INVALID", "native history payload exceeds bounded size")
            return result
        if method == "subscribe_quote":
            if (set(params) - {"account_id", "code", "period", "result_type", "dividend_type"}
                    or params.get("code") not in self.quote_symbols
                    or params.get("period", "tick") != "tick" or params.get("result_type", "dict") != "dict"
                    or params.get("dividend_type", "none") != "none"):
                raise ReadOnlyRequestError("INVALID_PARAMS", "subscription requires allowlisted symbol and tick/dict/none")
            if self.subscription is not None:
                raise ReadOnlyRequestError("SUBSCRIPTION_ACTIVE", "only one subscription is permitted")
            if not callable(getattr(self.context, "subscribe_quote", None)) or not callable(getattr(self.context, "unsubscribe_quote", None)):
                raise ReadOnlyRequestError("SUBSCRIPTION_UNAVAILABLE", "native subscription and cancellation are required")
            self.subscription_generation += 1
            subscription = {"subscription_id": "%s-%s" % (self.mailbox.session_id, self.subscription_generation),
                            "account_id": self.account_id, "session_id": self.mailbox.session_id,
                            "code": params["code"], "active": True, "native_handle": None}
            with self.event_lock:
                self.subscription = subscription
                self.dropped_count += len(self.events)
                self.events = []
                self.callback_error = None
            def callback(raw):
                with self.event_lock:
                    if self.closed or self.subscription is not subscription or not subscription["active"]:
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
                        payload = self.serializer(raw)
                        # JSON roundtrip freezes nested containers before returning to QMT.
                        payload = json.loads(json.dumps(payload, allow_nan=False))
                        event = {"sequence": self.event_sequence + 1, "account_id": self.account_id,
                                 "session_id": self.mailbox.session_id, "subscription_id": subscription["subscription_id"],
                                 "native_time": [quote["time"] for quote in quotes],
                                 "received_at": _dt.datetime.now().isoformat(), "data": payload}
                        if len(json.dumps(event, ensure_ascii=True, allow_nan=False).encode("ascii")) > 2048:
                            raise ValueError("callback batch exceeds 2048-byte event bound")
                        self.event_sequence += 1
                        if len(self.events) == 32:
                            self.events.pop(0)
                            self.dropped_count += 1
                        self.events.append(event)
                    except Exception as exc:
                        self.callback_error = "%s: %s" % (type(exc).__name__, exc)
            try:
                handle = self.context.subscribe_quote(subscription["code"], period="tick", dividend_type="none", result_type="dict", callback=callback)
                if type(handle) is not int or handle < 0:
                    raise ReadOnlyRequestError("SUBSCRIPTION_UNAVAILABLE", "native subscription returned invalid handle")
                subscription["native_handle"] = handle
            except Exception:
                with self.event_lock:
                    subscription["active"] = False
                    self.subscription = None
                raise
            return dict(subscription)
        if method == "poll_quote_events":
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
                        "callback_error": self.callback_error, "delivery": "poll"}
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
        try:
            if self.context.unsubscribe_quote(subscription["native_handle"]) is not True:
                raise RuntimeError("native unsubscribe did not confirm success")
        except Exception as exc:
            raise ReadOnlyRequestError("CLEANUP_FAILED", "%s: %s" % (type(exc).__name__, exc))
        self.subscription = None
        return {"cancelled": True, "cleanup_status": "CANCELLED", "subscription_id": subscription["subscription_id"]}

    def drain(self):
        if self.closed:
            return
        if threading.get_ident() != self.callback_thread_id:
            raise ReadOnlyRequestError("WRONG_CALLBACK_THREAD", "mailbox polling requires the original QMT callback thread")
        try:
            if time.monotonic() - self.created_at >= READONLY_MAX_SECONDS:
                self.close("deadline")
                return
            if self.pending is None:
                if self.request_count >= READONLY_MAX_REQUESTS:
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
                if self.request_count >= READONLY_MAX_REQUESTS:
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
            return
        self.closed = True
        cleanup_error = None
        try:
            self.cancel_subscription()
        except Exception as exc:
            cleanup_error = exc
        try:
            self.mailbox.close(failed=failed or cleanup_error is not None)
        except Exception as exc:
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
        "account_id": _SERVER.account_id, "name": READONLY_SHM_NAME, "symbol": READONLY_SYMBOL,
        "symbols": list(_SERVER.quote_symbols),
        "session_id": _SERVER.mailbox.session_id, "native_pid": _SERVER.mailbox.server_pid,
        "max_requests": READONLY_MAX_REQUESTS, "max_seconds": READONLY_MAX_SECONDS,
        "background_threads": False, "rpc_revision": RPC_REVISION,
    }, sort_keys=True))


def aqp_shm_readonly_adjust(ContextInfo):
    if _SERVER is not None:
        _SERVER.drain()


def handlebar(ContextInfo):
    return None


def stop(ContextInfo):
    if _SERVER is not None:
        _SERVER.close()
