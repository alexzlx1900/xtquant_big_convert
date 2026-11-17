# coding: utf-8
"""将原 Redis 只读入口和两个 SHM 运行体组合到同一 QMT 模型。

此构建证明通讯运行体共存，不表示两个独立图表模型能够同时运行。
"""

import argparse
import ast
import base64
import importlib.util
import math
import os
from pathlib import Path
import re


ROOT = Path(__file__).resolve().parent.parent


def _builder(filename):
    spec = importlib.util.spec_from_file_location("p1_" + filename[:-3], ROOT / "tools" / filename)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


REDIS_BUILDER = _builder("build_single_file.py")
READ_BUILDER = _builder("build_shm_readonly_single_file.py")
EXEC_BUILDER = _builder("build_shm_rpc_single_file.py")


def _settings(source, values):
    lines = source.splitlines(keepends=True)
    found = set()
    for node in reversed(ast.parse(source).body):
        if isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
            name = node.targets[0].id
            if name in values:
                found.add(name)
                lines[node.lineno - 1:node.end_lineno] = ["%s = %r\n" % (name, values[name])]
    if found != set(values):
        raise ValueError("missing configured source fields: %s" % sorted(set(values) - found))
    return "".join(lines)


def build_source(config_values, native_pid, windows_session, hard_deadline_unix,
                 read_name, execution_name, symbols, max_seconds=900, max_requests=1000):
    if not isinstance(config_values, dict) or config_values.get("BIGQMT_ACCOUNT_ID") != "55013746" or config_values.get("BIGQMT_ACCOUNT_TYPE") != "STOCK":
        raise ValueError("confirmed simulation STOCK account 55013746 required")
    config = config_values.get("BIGQMT_REDIS_CONFIG")
    if not isinstance(config, dict) or str(config.get("transport", "redis")).lower() not in ("redis", "", "default") or config.get("rpc_background_threads", False) is not False:
        raise ValueError("original Redis configuration without background RPC threads required")
    if type(native_pid) is not int or native_pid <= 0 or type(windows_session) is not int or windows_session < 0:
        raise ValueError("explicit native PID and Windows session required")
    if isinstance(hard_deadline_unix, bool) or not isinstance(hard_deadline_unix, (float, int)) or not math.isfinite(hard_deadline_unix) or hard_deadline_unix <= 0:
        raise ValueError("finite hard deadline required")
    if type(max_seconds) is not int or not 1 <= max_seconds <= 900 or type(max_requests) is not int or not 1 <= max_requests <= 1000:
        raise ValueError("bounded seconds [1,900] and request budget [1,1000] required")
    if not isinstance(read_name, str) or not re.fullmatch(r"Local\\AQP_SHM_READONLY_[A-Za-z0-9_-]{1,58}", read_name):
        raise ValueError("independent readonly mailbox required")
    if not isinstance(execution_name, str) or not re.fullmatch(r"Local\\AQP_SHM_TRADE_[A-Za-z0-9_-]{1,58}", execution_name):
        raise ValueError("independent execution mailbox required")
    if not isinstance(symbols, (list, tuple)) or not 1 <= len(symbols) <= 64 or len(set(symbols)) != len(symbols) or any(not isinstance(code, str) or not re.fullmatch(r"[0-9]{6}\.(SH|SZ|BJ)", code) for code in symbols):
        raise ValueError("1-64 unique initial quote symbols required")
    sources = {
        "redis": REDIS_BUILDER.build_source(config_values, isolated=True),
        "read": _settings(READ_BUILDER.build_source(), {
            "BIGQMT_ACCOUNT_ID": "55013746", "READONLY_SHM_NAME": read_name,
            "READONLY_SYMBOLS": list(symbols), "READONLY_MAX_SECONDS": max_seconds,
            "READONLY_MAX_REQUESTS": max_requests, "READONLY_ENABLE_MARKET_RPC": True}),
        "execution": EXEC_BUILDER.build_source(account_id="55013746", name=execution_name,
            max_seconds=max_seconds, max_requests=max_requests, bind_account_callbacks=True),
    }
    encoded = {key: base64.b64encode(value.encode("utf-8")).decode("ascii") for key, value in sources.items()}
    settings = dict(native_pid=native_pid, windows_session=windows_session,
        hard_deadline_unix=float(hard_deadline_unix), max_seconds=max_seconds, max_requests=max_requests)
    output = TEMPLATE.replace("__SOURCE_PAYLOADS__", repr(encoded)).replace("__P1_SETTINGS__", repr(settings))
    output = output.encode("ascii", "backslashreplace").decode("ascii")
    ast.parse(output, feature_version=(3, 6))
    return output


TEMPLATE = '''#coding:gbk
"""One QMT model, isolated original Redis/read-SHM/execution-SHM runtimes."""
import base64
import ctypes
import json
import os
import time

P1_SETTINGS = __P1_SETTINGS__
_SOURCE_PAYLOADS = __SOURCE_PAYLOADS__
_RUNTIMES = {}
_INIT_ATTEMPTED = False
_CLOSED = False
_STARTED_AT = None
_ACTIVE_DEADLINE = None
_OWN_REDIS_CLIENTS = []
_CALLBACK_NAMES = ("adjust", "aqp_shm_readonly_adjust", "aqp_shm_rpc_adjust")
_NATIVE_READ_NAMES = (
    "get_trade_detail_data", "get_history_trade_detail_data", "get_value_by_order_id",
    "get_last_order_id", "get_ipo_data", "get_new_purchase_limit", "get_assure_contract",
    "get_enable_short_contract", "get_unclosed_compacts", "get_closed_compacts",
    "get_debt_contract", "get_option_subject_position", "get_comb_option",
    "get_hkt_exchange_rate", "download_history_data", "download_history_data2", "down_history_data")
_NATIVE_BINDINGS = _NATIVE_READ_NAMES + ("passorder", "cancel")

# Loading creates private modules only: no Redis connection or mailbox yet.
for _key in ("redis", "read", "execution"):
    _namespace = {"__name__": "__p1_coexist_" + _key + "__",
                  "__file__": globals().get("__file__", "<p1-coexist>")}
    for _name in _NATIVE_BINDINGS:
        if callable(globals().get(_name)):
            _namespace[_name] = globals()[_name]
    exec(compile(base64.b64decode(_SOURCE_PAYLOADS[_key]), "<p1-" + _key + ">", "exec"), _namespace)
    _RUNTIMES[_key] = _namespace


def _identity():
    if os.getpid() != P1_SETTINGS["native_pid"]:
        raise RuntimeError("native PID mismatch")
    session = ctypes.c_uint()
    if not ctypes.windll.kernel32.ProcessIdToSessionId(os.getpid(), ctypes.byref(session)) or session.value != P1_SETTINGS["windows_session"]:
        raise RuntimeError("Windows session mismatch")


def _skip_gil_tuning():
    # Composite-only: do not alter scheduling of unrelated models.
    return None


def _configure_private_redis_safety(redis):
    strategy = redis["_strategy"]
    rpc_config = dict(strategy._build_config().get("rpc") or {})
    rpc_config["warm_context_data"] = False
    strategy.configure(rpc=rpc_config)
    strategy._apply_gil_tuning = _skip_gil_tuning
    common = redis["_load_local_module"]("bigqmt_signal_trader.adapters.redis_common")
    original_build = common.build_redis_client

    def own_build(config=None):
        client = original_build(config)
        _OWN_REDIS_CLIENTS.append(client)
        return client

    common.build_redis_client = own_build


def _release_private_redis(redis):
    strategy = redis["_strategy"]
    service = strategy._rpc_service
    runner = redis["_load_local_module"]("bigqmt_signal_trader.runner")
    app = runner.get_app()
    errors = []
    try:
        strategy.reset_app()
    except Exception as exc:
        errors.append("reset:" + type(exc).__name__)
    finally:
        seen_pools = set()
        own_client_ids = set(id(client) for client in _OWN_REDIS_CLIENTS)
        for client in _OWN_REDIS_CLIENTS:
            pool = getattr(client, "connection_pool", None)
            if pool is not None and id(pool) not in seen_pools:
                seen_pools.add(id(pool))
                try:
                    pool.disconnect()
                except Exception as exc:
                    errors.append("pool:" + type(exc).__name__)
        if service is not None:
            # Delete the instance closure, exposing the class method again.
            service.__dict__.pop("process_request", None)
            transport = getattr(service, "_transport", None)
            for owner in (service, transport):
                if owner is not None:
                    for name in ("redis", "listen_redis"):
                        if id(getattr(owner, name, None)) in own_client_ids:
                            setattr(owner, name, None)
            if transport is not None:
                transport.on_raw_payload = None
                transport.on_request = None
            if id(getattr(service.handlers, "download_job_redis_client", None)) in own_client_ids:
                service.handlers.download_job_redis_client = None
        if app is not None:
            for owner in (app, getattr(app, "signal_source", None), getattr(app, "position_sync_sink", None), getattr(app, "state_store", None)):
                if owner is not None and id(getattr(owner, "redis", None)) in own_client_ids:
                    owner.redis = None
        _OWN_REDIS_CLIENTS[:] = []
    if errors:
        raise RuntimeError("private Redis cleanup incomplete: " + ",".join(errors))


def _within_budget(ContextInfo):
    if _CLOSED or not _INIT_ATTEMPTED or _STARTED_AT is None:
        return False
    if time.time() >= _ACTIVE_DEADLINE or time.monotonic() - _STARTED_AT >= P1_SETTINGS["max_seconds"]:
        stop(ContextInfo)
        return False
    redis_service = _RUNTIMES["redis"]["_strategy"]._rpc_service
    if redis_service is not None and redis_service._processed_count >= P1_SETTINGS["max_requests"]:
        stop(ContextInfo)
        return False
    return True


def init(ContextInfo):
    global _INIT_ATTEMPTED, _STARTED_AT, _ACTIVE_DEADLINE
    if _INIT_ATTEMPTED or _CLOSED:
        raise RuntimeError("coexist init may only be attempted once; use a fresh candidate")
    _INIT_ATTEMPTED = True
    try:
        _identity()
        _STARTED_AT = time.monotonic()
        _ACTIVE_DEADLINE = min(P1_SETTINGS["hard_deadline_unix"], time.time() + P1_SETTINGS["max_seconds"])
        seconds = int(_ACTIVE_DEADLINE - time.time())
        if seconds < 1:
            raise RuntimeError("source deadline exhausted before initialization")
        query = globals().get("get_trade_detail_data")
        if not callable(query) or not callable(getattr(ContextInfo, "run_time", None)):
            raise RuntimeError("native query and timers required")
        redis = _RUNTIMES["redis"]
        runtime = redis["_runtime"]
        strategy = redis["_strategy"]
        if runtime.ACCOUNT_ID != "55013746" or runtime.RPC_ALLOW_ORDER_METHODS is not False:
            raise RuntimeError("original Redis account or write gate mismatch")
        runtime.bind_runtime_api(passorder_func=globals().get("passorder"), cancel_func=globals().get("cancel"),
            get_trade_detail_data_func=query,
            extra_funcs={name: globals()[name] for name in _NATIVE_READ_NAMES if callable(globals().get(name))})
        _configure_private_redis_safety(redis)
        app = redis["init"](ContextInfo)
        service = strategy._rpc_service
        if app is None or service is None or not service._running.is_set() or not strategy._scheduled_adjust or service.handlers.allow_order_methods is not False or strategy._build_config().get("run_app_tick") is not False:
            raise RuntimeError("original Redis readonly runtime did not initialize completely")
        original_process = service.process_request

        def bounded_process(request):
            if _CLOSED or time.time() >= _ACTIVE_DEADLINE or service._processed_count >= P1_SETTINGS["max_requests"]:
                raise RuntimeError("original Redis request budget exhausted")
            return original_process(request)

        service.process_request = bounded_process
        _RUNTIMES["read"]["READONLY_MAX_SECONDS"] = seconds
        _RUNTIMES["execution"]["SHM_RPC_MAX_SECONDS"] = seconds
        for key in ("read", "execution"):
            _RUNTIMES[key]["get_trade_detail_data"] = query
            _RUNTIMES[key]["init"](ContextInfo)
        read_server = _RUNTIMES["read"]["_SERVER"]
        execution_server = _RUNTIMES["execution"]["_SERVER"]
        if read_server.mailbox.server_pid != P1_SETTINGS["native_pid"] or execution_server.bridge.mailbox.server_pid != P1_SETTINGS["native_pid"] or read_server.mailbox.session_id == execution_server.bridge.mailbox.session_id:
            raise RuntimeError("SHM source identity mismatch")
        print("AQP_P1_COEXIST_READY " + json.dumps(dict(
            account_id="55013746", native_pid=P1_SETTINGS["native_pid"],
            windows_session=P1_SETTINGS["windows_session"], allow_order_methods=False,
            coexistence_scope="single_model_three_runtimes", callbacks=list(_CALLBACK_NAMES),
            redis_transport="redis", read_name=_RUNTIMES["read"]["READONLY_SHM_NAME"],
            execution_name=_RUNTIMES["execution"]["SHM_RPC_NAME"],
            warm_context_data=False, gil_tuning_enabled=False,
            read_session_id=read_server.mailbox.session_id,
            execution_session_id=execution_server.bridge.mailbox.session_id,
            source_deadline_unix=_ACTIVE_DEADLINE, max_requests=P1_SETTINGS["max_requests"]), sort_keys=True))
        # Native lifecycle callbacks have no return value. The private Redis
        # runner continues to own the app; its business init contract is intact.
        return None
    except Exception:
        stop(ContextInfo)
        raise


def adjust(ContextInfo):
    if _within_budget(ContextInfo):
        return _RUNTIMES["redis"]["adjust"](ContextInfo)


def aqp_shm_readonly_adjust(ContextInfo):
    if _within_budget(ContextInfo):
        return _RUNTIMES["read"]["aqp_shm_readonly_adjust"](ContextInfo)


def aqp_shm_rpc_adjust(ContextInfo):
    if _within_budget(ContextInfo):
        return _RUNTIMES["execution"]["aqp_shm_rpc_adjust"](ContextInfo)


def handlebar(ContextInfo):
    return None


def account_callback(ContextInfo, accountInfo):
    if _within_budget(ContextInfo):
        return _RUNTIMES["execution"]["account_callback"](ContextInfo, accountInfo)


def order_callback(ContextInfo, orderInfo):
    if _within_budget(ContextInfo):
        _RUNTIMES["redis"]["order_callback"](ContextInfo, orderInfo)
        return _RUNTIMES["execution"]["order_callback"](ContextInfo, orderInfo)


def deal_callback(ContextInfo, dealInfo):
    if _within_budget(ContextInfo):
        _RUNTIMES["redis"]["deal_callback"](ContextInfo, dealInfo)
        return _RUNTIMES["execution"]["deal_callback"](ContextInfo, dealInfo)


def orderError_callback(ContextInfo, orderArgs, errMsg):
    if _within_budget(ContextInfo):
        return _RUNTIMES["execution"]["orderError_callback"](ContextInfo, orderArgs, errMsg)


def stop(ContextInfo):
    global _CLOSED
    if _CLOSED:
        return
    _CLOSED = True
    errors = []
    for key in ("execution", "read", "redis"):
        try:
            if key == "redis":
                _release_private_redis(_RUNTIMES[key])
            else:
                _RUNTIMES[key]["stop"](ContextInfo)
        except Exception as exc:
            errors.append(key + ": " + type(exc).__name__)
    if errors:
        raise RuntimeError("coexist cleanup incomplete: " + ", ".join(errors))
'''


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config-snapshot", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--native-pid", type=int, required=True)
    parser.add_argument("--windows-session", type=int, required=True)
    parser.add_argument("--hard-deadline-unix", type=float, required=True)
    parser.add_argument("--read-name", required=True)
    parser.add_argument("--execution-name", required=True)
    parser.add_argument("--symbols", required=True, help="初始行情代码元数据，逗号分隔；不限制后续合法代码")
    parser.add_argument("--max-seconds", type=int, default=900)
    parser.add_argument("--max-requests", type=int, default=1000)
    args = parser.parse_args()
    values = REDIS_BUILDER.load_config_snapshot(args.config_snapshot)
    output = build_source(values, args.native_pid, args.windows_session, args.hard_deadline_unix,
        args.read_name, args.execution_name, args.symbols.split(","), args.max_seconds, args.max_requests)
    fd = open_private(args.output)
    with fd as stream:
        stream.write(output)
    print("built P1 single-model coexist candidate: %s (%d bytes)" % (args.output, len(output.encode("utf-8"))))


def open_private(path):
    # 配置候选可能含连接凭据，创建时即限制权限且不覆盖已有文件。
    fd = os.open(str(path), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    return os.fdopen(fd, "w", encoding="utf-8", newline="\n")


if __name__ == "__main__":
    main()
