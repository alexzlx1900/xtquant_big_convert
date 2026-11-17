# coding: utf-8
"""独立、有限时长的 QMT 只读入口；先用专用构建器内嵌依赖。"""

import base64
import builtins
import datetime as _dt
import json
import re
import threading
import time
import types


BIGQMT_ACCOUNT_ID = "YOUR_ACCOUNT_ID"
READONLY_ENDPOINT = "tcp://127.0.0.1:57941"
READONLY_SYMBOL = "000001.SZ"  # 初始代码元数据，不限制请求中的合法证券。
READONLY_MAX_REQUESTS = 5
READONLY_MAX_SECONDS = 300

# 构建时替换；模块只缓存在当前策略，保留其他策略的共享模块状态。
_EMBEDDED_SOURCES = {}  # BUILD_READONLY_EMBEDDED
_MODULE_CACHE = {}
_ORIGINAL_IMPORT = builtins.__import__
_SERVER = None


def _bundle_import(name, module_globals=None, module_locals=None, fromlist=(), level=0):
    if level:
        package = (module_globals or {}).get("__package__", "")
        parts = package.split(".")
        name = ".".join(parts[:len(parts) - level + 1] + ([name] if name else []))
    if name in _EMBEDDED_SOURCES or name in ("bigqmt_signal_trader", "bigqmt_signal_trader.adapters"):
        module = _load_embedded(name)
        return module if fromlist else _load_embedded(name.split(".")[0])
    return _ORIGINAL_IMPORT(name, module_globals, module_locals, fromlist, 0)


def _load_embedded(name):
    if name in _MODULE_CACHE:
        return _MODULE_CACHE[name]
    module = types.ModuleType(name)
    is_package = name in ("bigqmt_signal_trader", "bigqmt_signal_trader.adapters")
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


class ReadOnlyBridge:
    def __init__(self, context, query):
        if not BIGQMT_ACCOUNT_ID or BIGQMT_ACCOUNT_ID == "YOUR_ACCOUNT_ID":
            raise ValueError("configure the confirmed simulation account")
        prefix = "tcp://127.0.0.1:"
        if not READONLY_ENDPOINT.startswith(prefix):
            raise ValueError("readonly endpoint must use IPv4 loopback")
        port = READONLY_ENDPOINT[len(prefix):]
        if not port.isdigit() or not 1 <= int(port) <= 65535:
            raise ValueError("invalid readonly port")
        if not callable(query):
            raise RuntimeError("QMT get_trade_detail_data is unavailable")
        if not 1 <= int(READONLY_MAX_REQUESTS) <= 1000 or not 1 <= int(READONLY_MAX_SECONDS) <= 1800:
            raise ValueError("readonly session must be bounded")
        self.context = context
        self.account_id = str(BIGQMT_ACCOUNT_ID)
        self.created_at = time.monotonic()
        self.request_count = 0
        self.closed = False
        self.serializer = _load_embedded("aqp_readonly_serializer").to_jsonable
        position_module = _load_embedded("bigqmt_signal_trader.adapters.position_bigqmt")
        self.positions = position_module.BigQmtPositionProvider(query, account_type="STOCK")
        transport_module = _load_embedded("aqp_readonly_transport")
        self.transport = transport_module.ZmqTransport(
            bind_address=READONLY_ENDPOINT, account_id=self.account_id,
            print_prefix="[aqp_readonly]", recv_timeout_seconds=0.1,
        )

    def start(self):
        self.transport.start_receiving(self.process_request, background_threads=False)

    def process_request(self, request):
        request = dict(request or {})
        request_id = str(request.get("request_id") or "")
        method = str(request.get("method") or "")
        response = {
            "schema_version": 1, "request_id": request_id,
            "account_id": self.account_id, "method": method,
            "ok": False, "data": None, "error": "", "server_error": "",
            "handled_at": _dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        try:
            if not request_id:
                raise ValueError("request_id is required")
            if str(request.get("account_id") or "") != self.account_id:
                raise PermissionError("account_id mismatch")
            params = request.get("params") or {}
            if not isinstance(params, dict):
                raise ValueError("params must be an object")
            if "account_id" in params and str(params["account_id"]) != self.account_id:
                raise PermissionError("params account_id mismatch")
            if method not in ("ping", "get_positions", "get_full_tick", "get_ticks"):
                raise PermissionError("method is not in readonly allowlist")
            expires_at = request.get("read_expires_at_unix")
            if expires_at is not None and time.time() >= float(expires_at):
                raise TimeoutError("read RPC expired before execution")
            if method == "ping":
                data = {
                    "pong": True, "account_id": self.account_id,
                    "allow_order_methods": False, "rpc_revision": "readonly-single-file-v1",
                    "server_time": _dt.datetime.now(), "background_threads": False,
                    "callback_thread_id": threading.get_ident(),
                }
            elif method == "get_positions":
                evidence = self.positions.get_positions_with_evidence(self.account_id)
                if not evidence["source_evidence"]["complete"]:
                    response["data"] = self.serializer(evidence)
                    raise RuntimeError(evidence["source_evidence"]["error_code"] or "POSITION_INCOMPLETE")
                data = evidence if params.get("include_source_evidence") is True else evidence["positions"]
            else:
                if set(params) - {"account_id", "codes", "code"} or ("codes" in params) == ("code" in params):
                    raise ValueError("provide exactly one quote selector")
                codes = params.get("codes", params.get("code"))
                if isinstance(codes, str):
                    codes = [codes]
                if (not isinstance(codes, list) or len(codes) != 1
                        or not isinstance(codes[0], str)
                        or re.fullmatch(r"[0-9]{6}\.(SH|SZ|BJ)", codes[0]) is None):
                    raise ValueError("codes must contain one valid SH/SZ/BJ symbol")
                data = self.context.get_full_tick(codes)
                if (not isinstance(data, dict) or set(data) != set(codes)
                        or not isinstance(data[codes[0]], dict) or not data[codes[0]]):
                    raise RuntimeError("QMT_QUOTE_UNAVAILABLE")
            response.update(ok=True, data=self.serializer(data))
        except Exception as exc:
            response["error"] = "%s: %s" % (type(exc).__name__, exc)
        self.request_count += 1
        print("AQP_READONLY_RPC " + json.dumps({
            "request_id": request_id, "method": method, "ok": response["ok"],
            "error": response["error"], "callback_thread_id": threading.get_ident(),
        }, sort_keys=True))
        return response

    def drain(self):
        if self.closed:
            return
        if time.monotonic() - self.created_at >= READONLY_MAX_SECONDS:
            self.close("deadline")
            return
        remaining = READONLY_MAX_REQUESTS - self.request_count
        self.transport.drain_request_queue(max_items=min(3, remaining))
        if self.request_count >= READONLY_MAX_REQUESTS:
            self.close("request_limit")

    def close(self, reason="stop"):
        if not self.closed:
            self.closed = True
            self.transport.stop()
            print("AQP_READONLY_STOPPED reason=%s requests=%s" % (reason, self.request_count))


def init(ContextInfo):
    global _SERVER
    if _SERVER is not None:
        _SERVER.close("reload")
    if not _EMBEDDED_SOURCES:
        raise RuntimeError("build this entry with tools/build_readonly_single_file.py first")
    _SERVER = ReadOnlyBridge(ContextInfo, globals().get("get_trade_detail_data"))
    try:
        _SERVER.start()
        start_at = (_dt.datetime.now() + _dt.timedelta(seconds=1)).strftime("%Y-%m-%d %H:%M:%S")
        ContextInfo.run_time("aqp_readonly_adjust", "100nMilliSecond", start_at)
    except Exception:
        _SERVER.close("init_failed")
        raise
    print("AQP_READONLY_READY " + json.dumps({
        "account_id": _SERVER.account_id, "endpoint": READONLY_ENDPOINT,
        "symbol": READONLY_SYMBOL, "background_threads": False,
        "max_requests": READONLY_MAX_REQUESTS, "max_seconds": READONLY_MAX_SECONDS,
    }, sort_keys=True))


def aqp_readonly_adjust(ContextInfo):
    if _SERVER is not None:
        _SERVER.drain()


def handlebar(ContextInfo):
    return None


def stop(ContextInfo):
    if _SERVER is not None:
        _SERVER.close()
