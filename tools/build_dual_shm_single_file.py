# coding: utf-8
"""构建固定账户双SHM入口；默认模拟禁写，实盘须显式政策，不在QMT内初始化Redis。"""

import argparse
import ast
import base64
import importlib.util
import math
import os
from pathlib import Path
import re


ROOT = Path(__file__).resolve().parent.parent
ACCOUNT = "55013746"


def _load_builder(filename):
    spec = importlib.util.spec_from_file_location("dual_" + filename[:-3], ROOT / "tools" / filename)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


READ_BUILDER = _load_builder("build_shm_readonly_single_file.py")
EXECUTION_BUILDER = _load_builder("build_shm_rpc_single_file.py")


def _configure_source(source, settings):
    lines = source.splitlines(keepends=True)
    found = set()
    for node in reversed(ast.parse(source).body):
        if isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
            name = node.targets[0].id
            if name in settings:
                found.add(name)
                lines[node.lineno - 1:node.end_lineno] = ["%s = %r\n" % (name, settings[name])]
    if found != set(settings):
        raise ValueError("single-source configuration fields missing: %s" % sorted(set(settings) - found))
    return "".join(lines)


def build_source(*, native_pid, windows_session, hard_deadline_unix, read_name,
                 execution_name, symbols, account_id=ACCOUNT, max_seconds=900,
                 max_requests=1000, enable_market_rpc=False, persistent_readonly=False,
                 persistent_simulation_read_write=False, environment="SIM", persistent_live_read_write=False,
                 live_account_query_login=False):
    if environment not in ("SIM", "LIVE"):
        raise ValueError("explicit SIM/LIVE environment required")
    if type(live_account_query_login) is not bool or (live_account_query_login and environment != "LIVE"):
        raise ValueError("ACCOUNT query login authority requires explicit LIVE selection")
    if environment == "LIVE":
        if (persistent_live_read_write is not True or not isinstance(account_id, str) or not account_id
                or len(account_id) > 64 or account_id == ACCOUNT
                or not all("0" <= char <= "9" for char in account_id)):
            raise ValueError("LIVE build requires explicit persistent LIVE policy and live numeric account")
    elif persistent_live_read_write:
        raise ValueError("persistent LIVE policy requires LIVE environment")
    elif account_id != ACCOUNT:
        raise ValueError("this candidate is limited to simulation account 55013746")
    if type(native_pid) is not int or native_pid <= 0 or type(windows_session) is not int or windows_session < 0:
        raise ValueError("explicit native PID and Windows session required")
    if (type(persistent_readonly) is not bool or type(persistent_simulation_read_write) is not bool
            or type(persistent_live_read_write) is not bool):
        raise ValueError("persistent readonly selection must be explicit boolean")
    if sum((persistent_readonly, persistent_simulation_read_write, persistent_live_read_write)) > 1:
        raise ValueError("choose exactly one persistent runtime policy")
    read_write = persistent_simulation_read_write or persistent_live_read_write
    persistent = persistent_readonly or read_write
    if persistent_readonly:
        if (hard_deadline_unix is not None or type(max_seconds) is not int or max_seconds != 0
                or type(max_requests) is not int or max_requests != 0 or enable_market_rpc is not False):
            raise ValueError("persistent readonly requires no deadline, explicit zero budgets and market disabled")
    elif read_write:
        if (hard_deadline_unix is not None or type(max_seconds) is not int or max_seconds != 0
                or type(max_requests) is not int or max_requests != 0):
            raise ValueError("persistent read-write requires no deadline and explicit zero budgets")
    else:
        if isinstance(hard_deadline_unix, bool) or not isinstance(hard_deadline_unix, (int, float)) or not math.isfinite(hard_deadline_unix) or hard_deadline_unix <= 0:
            raise ValueError("finite positive absolute deadline required")
        if type(max_seconds) is not int or not 1 <= max_seconds <= 900 or type(max_requests) is not int or not 1 <= max_requests <= 1000:
            raise ValueError("seconds [1,900] and per-source requests [1,1000] required")
    if type(enable_market_rpc) is not bool:
        raise ValueError("market RPC selection must be boolean")
    if not isinstance(read_name, str) or not re.fullmatch(r"Local\\AQP_SHM_READONLY_[A-Za-z0-9_-]{1,58}", read_name):
        raise ValueError("independent readonly mailbox name required")
    if not isinstance(execution_name, str) or not re.fullmatch(r"Local\\AQP_SHM_TRADE_[A-Za-z0-9_-]{1,58}", execution_name):
        raise ValueError("independent execution mailbox name required")
    symbol_limit = 128 if environment == "LIVE" else 64
    if not isinstance(symbols, (list, tuple)) or not 1 <= len(symbols) <= symbol_limit or any(not isinstance(code, str) or not re.fullmatch(r"[0-9]{6}\.(SH|SZ|BJ)", code) for code in symbols) or len(set(symbols)) != len(symbols):
        raise ValueError("1-%d unique confirmed SH/SZ/BJ symbols required" % symbol_limit)
    read = _configure_source(READ_BUILDER.build_source(), {
        "BIGQMT_ACCOUNT_ID": account_id, "BIGQMT_ENVIRONMENT": environment, "READONLY_SHM_NAME": read_name,
        "READONLY_SYMBOLS": list(symbols), "READONLY_MAX_SECONDS": max_seconds,
        "READONLY_MAX_REQUESTS": max_requests, "READONLY_ENABLE_MARKET_RPC": enable_market_rpc,
        "READONLY_PERSISTENT_READONLY": persistent,
        "READONLY_PERSISTENT_MARKET_RPC": read_write and enable_market_rpc,
        "READONLY_PERSISTENT_LIVE_READ_WRITE": persistent_live_read_write,
    })
    execution = EXECUTION_BUILDER.build_source(account_id=account_id, name=execution_name,
        native_stock_gateway=read_write, environment=environment,
        persistent_live_read_write=persistent_live_read_write,
        max_seconds=0 if persistent_live_read_write else None,
        max_requests=0 if persistent_live_read_write else None)
    execution = _configure_source(execution, {
        "SHM_RPC_BIND_ACCOUNT_CALLBACKS": False, "SHM_RPC_ALLOW_ORDER_METHODS": read_write,
        "SHM_RPC_ORDER_GATEWAY": None,
        "SHM_RPC_MAX_SECONDS": max_seconds, "SHM_RPC_MAX_REQUESTS": max_requests,
        "SHM_RPC_PERSISTENT_READONLY": persistent_readonly,
        "SHM_RPC_PERSISTENT_SIMULATION_READ_WRITE": persistent_simulation_read_write,
        "SHM_RPC_PERSISTENT_LIVE_READ_WRITE": persistent_live_read_write,
        "SHM_RPC_ACCOUNT_ALREADY_BOUND": read_write,
        "SHM_RPC_LIVE_ACCOUNT_QUERY_LOGIN": live_account_query_login,
    })
    sources = {"read": read, "execution": execution}
    for key, source in sources.items():
        ast.parse(source, filename=key, feature_version=(3, 6))
    payloads = {key: base64.b64encode(source.encode("utf-8")).decode("ascii") for key, source in sources.items()}
    settings = dict(account_id=account_id, environment=environment, native_pid=native_pid, windows_session=windows_session,
        hard_deadline_unix=None if persistent else float(hard_deadline_unix), max_seconds=max_seconds,
        max_requests_per_source=max_requests, market_rpc_enabled=enable_market_rpc,
        persistent_readonly=persistent_readonly, persistent_simulation_read_write=persistent_simulation_read_write,
        persistent_live_read_write=persistent_live_read_write, live_account_query_login=live_account_query_login)
    output = TEMPLATE.replace("__DUAL_SETTINGS__", repr(settings)).replace("__DUAL_PAYLOADS__", repr(payloads))
    output = output.encode("ascii", "backslashreplace").decode("ascii")
    ast.parse(output, feature_version=(3, 6))
    return output


TEMPLATE = '''#coding:gbk
"""Fixed account/environment, one native binding and two private SHM sources."""
import base64
import ctypes
import json
import os
import time

DUAL_SHM_SETTINGS = __DUAL_SETTINGS__
_SOURCE_PAYLOADS = __DUAL_PAYLOADS__
_RUNTIMES = {}
_INIT_ATTEMPTED = False
_CLOSED = False
_STARTED_AT = None
_STARTED_AT_UNIX = None
_SOURCE_DEADLINE = None
_READ_CAPABILITIES = ["ping", "get_asset", "get_positions", "get_full_tick", "get_ticks", "query_orders", "query_trades"]

for _key in ("read", "execution"):
    _namespace = {"__name__": "__aqp_dual_shm_" + _key + "__"}
    exec(compile(base64.b64decode(_SOURCE_PAYLOADS[_key]), "<dual-shm-" + _key + ">", "exec"), _namespace)
    _RUNTIMES[_key] = _namespace


def _verify_identity():
    if os.getpid() != DUAL_SHM_SETTINGS["native_pid"]:
        raise RuntimeError("native PID mismatch")
    session = ctypes.c_uint()
    if not ctypes.windll.kernel32.ProcessIdToSessionId(os.getpid(), ctypes.byref(session)) or session.value != DUAL_SHM_SETTINGS["windows_session"]:
        raise RuntimeError("Windows session mismatch")


def init(ContextInfo):
    global _INIT_ATTEMPTED, _STARTED_AT, _STARTED_AT_UNIX, _SOURCE_DEADLINE
    if _INIT_ATTEMPTED or _CLOSED:
        raise RuntimeError("init may only be attempted once; build a fresh source")
    _INIT_ATTEMPTED = True
    try:
        _verify_identity()
        _STARTED_AT = time.monotonic()
        _STARTED_AT_UNIX = time.time()
        live_write = DUAL_SHM_SETTINGS["persistent_live_read_write"]
        read_write = DUAL_SHM_SETTINGS["persistent_simulation_read_write"] or live_write
        environment = DUAL_SHM_SETTINGS["environment"]
        account = DUAL_SHM_SETTINGS["account_id"]
        if (environment not in ("SIM", "LIVE") or type(live_write) is not bool
                or sum((DUAL_SHM_SETTINGS["persistent_readonly"], DUAL_SHM_SETTINGS["persistent_simulation_read_write"], live_write)) > 1
                or environment == "LIVE" and (not live_write or not isinstance(account, str) or not account
                    or len(account) > 64 or account == "55013746" or not all("0" <= char <= "9" for char in account))
                or environment == "SIM" and (live_write or account != "55013746")):
            raise RuntimeError("fixed account/environment policy changed")
        persistent = DUAL_SHM_SETTINGS["persistent_readonly"] or read_write
        if persistent:
            if (DUAL_SHM_SETTINGS["hard_deadline_unix"] is not None
                    or DUAL_SHM_SETTINGS["max_seconds"] != 0 or DUAL_SHM_SETTINGS["max_requests_per_source"] != 0
                    or DUAL_SHM_SETTINGS["persistent_readonly"] and DUAL_SHM_SETTINGS["market_rpc_enabled"] is not False):
                raise RuntimeError("persistent runtime policy changed")
            _SOURCE_DEADLINE = None
            remaining = 0
        else:
            _SOURCE_DEADLINE = min(DUAL_SHM_SETTINGS["hard_deadline_unix"], _STARTED_AT_UNIX + DUAL_SHM_SETTINGS["max_seconds"])
            remaining = int(_SOURCE_DEADLINE - time.time())
            if remaining < 1:
                raise RuntimeError("source deadline expired before initialization")
        query = globals().get("get_trade_detail_data")
        if not callable(query) or not callable(getattr(ContextInfo, "set_account", None)) or not callable(getattr(ContextInfo, "run_time", None)):
            raise RuntimeError("native account/query/timer APIs required")
        execution = _RUNTIMES["execution"]
        expected_factory = execution["create_native_stock_order_gateway"] if read_write else None
        if (execution["SHM_RPC_ALLOW_ORDER_METHODS"] is not read_write or execution["SHM_RPC_BIND_ACCOUNT_CALLBACKS"] is not False
                or execution["SHM_RPC_ORDER_GATEWAY"] is not None or execution["SHM_RPC_ORDER_GATEWAY_FACTORY"] is not expected_factory):
            raise RuntimeError("readonly execution source gate changed")
        if (execution["SHM_RPC_ACCOUNT_ALREADY_BOUND"] is not read_write
                or execution["SHM_RPC_PERSISTENT_SIMULATION_READ_WRITE"] is not DUAL_SHM_SETTINGS["persistent_simulation_read_write"]
                or execution["SHM_RPC_PERSISTENT_LIVE_READ_WRITE"] is not live_write
                or execution["BIGQMT_ENVIRONMENT"] != environment or execution["BIGQMT_ACCOUNT_ID"] != account
                or execution["SHM_RPC_PERSISTENT_READONLY"] is not DUAL_SHM_SETTINGS["persistent_readonly"]):
            raise RuntimeError("execution source runtime policy changed")
        read = _RUNTIMES["read"]
        if (read["READONLY_PERSISTENT_READONLY"] is not persistent or read["READONLY_ENABLE_MARKET_RPC"] is not DUAL_SHM_SETTINGS["market_rpc_enabled"]
                or read["READONLY_PERSISTENT_MARKET_RPC"] is not (read_write and DUAL_SHM_SETTINGS["market_rpc_enabled"])
                or read["READONLY_PERSISTENT_LIVE_READ_WRITE"] is not live_write
                or read["BIGQMT_ENVIRONMENT"] != environment or read["BIGQMT_ACCOUNT_ID"] != account):
            raise RuntimeError("readonly source runtime policy changed")
        if read_write and (not callable(globals().get("passorder")) or not callable(globals().get("cancel"))):
            raise RuntimeError("explicit native %s order/cancel functions required" % ("LIVE" if live_write else "simulation"))
        ContextInfo.set_account(DUAL_SHM_SETTINGS["account_id"])
        _RUNTIMES["read"]["READONLY_MAX_SECONDS"] = remaining
        execution["SHM_RPC_MAX_SECONDS"] = remaining
        for key in ("read", "execution"):
            _RUNTIMES[key]["get_trade_detail_data"] = query
            if key == "read":
                _RUNTIMES[key]["download_history_data"] = globals().get("download_history_data")
            if key == "execution" and read_write:
                _RUNTIMES[key]["passorder"] = globals().get("passorder")
                _RUNTIMES[key]["cancel"] = globals().get("cancel")
            _RUNTIMES[key]["init"](ContextInfo)
        read_server = _RUNTIMES["read"]["_SERVER"]
        execution_server = execution["_SERVER"]
        if read_server.mailbox.server_pid != DUAL_SHM_SETTINGS["native_pid"] or execution_server.bridge.mailbox.server_pid != DUAL_SHM_SETTINGS["native_pid"] or read_server.mailbox.session_id == execution_server.bridge.mailbox.session_id:
            raise RuntimeError("dual source PID/session identity mismatch")
        if (read_server.windows_session_id != DUAL_SHM_SETTINGS["windows_session"]
                or execution_server.bridge.runtime_metadata["windows_session_id"] != DUAL_SHM_SETTINGS["windows_session"]):
            raise RuntimeError("dual source actual Windows session mismatch")
        read_capabilities = list(_READ_CAPABILITIES)
        if read_server.market_enabled:
            read_capabilities += ["get_local_data", "get_market_data_ex", "subscribe_quote", "poll_quote_events", "unsubscribe_quote"]
            if callable(read_server.history_downloader):
                read_capabilities += ["download_history_data"]
        rpc_transport = execution["_load_embedded"]("bigqmt_signal_trader.transports.shm_rpc")
        execution_capabilities = sorted(rpc_transport.READ_METHODS | (rpc_transport.WRITE_METHODS if read_write else frozenset()))
        print("AQP_DUAL_SHM_READY " + json.dumps(dict(
            account_id=DUAL_SHM_SETTINGS["account_id"], environment=environment, native_pid=DUAL_SHM_SETTINGS["native_pid"],
            windows_session=DUAL_SHM_SETTINGS["windows_session"],
            read_name=_RUNTIMES["read"]["READONLY_SHM_NAME"], execution_name=execution["SHM_RPC_NAME"],
            read_session_id=read_server.mailbox.session_id, execution_session_id=execution_server.bridge.mailbox.session_id,
            allow_order_methods=read_write, source_deadline_unix=_SOURCE_DEADLINE,
            runtime_policy="PERSISTENT_LIVE_READ_WRITE" if live_write else "PERSISTENT_SIMULATION_READ_WRITE" if read_write else "PERSISTENT_READONLY" if persistent else "BOUNDED_READONLY",
            started_at_unix=_STARTED_AT_UNIX, account_bound_once=True,
            account_callback_route="outer_to_execution", execution_bind_account_callbacks=False,
            max_seconds=DUAL_SHM_SETTINGS["max_seconds"], max_requests_per_source=DUAL_SHM_SETTINGS["max_requests_per_source"],
            market_rpc_enabled=read_server.market_enabled, read_capabilities=read_capabilities,
            execution_capabilities=execution_capabilities), sort_keys=True))
        return None
    except BaseException:
        stop(ContextInfo)
        raise


def _within_budget(ContextInfo):
    if _CLOSED or _STARTED_AT is None:
        return False
    if not (DUAL_SHM_SETTINGS["persistent_readonly"] or DUAL_SHM_SETTINGS["persistent_simulation_read_write"] or DUAL_SHM_SETTINGS["persistent_live_read_write"]) and (time.time() >= _SOURCE_DEADLINE or time.monotonic() - _STARTED_AT >= DUAL_SHM_SETTINGS["max_seconds"]):
        stop(ContextInfo)
        return False
    if any(runtime["_SERVER"] is not None and runtime["_SERVER"].closed for runtime in _RUNTIMES.values()):
        stop(ContextInfo)
        return False
    return True


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
        return _RUNTIMES["execution"]["order_callback"](ContextInfo, orderInfo)


def deal_callback(ContextInfo, dealInfo):
    if _within_budget(ContextInfo):
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
    for key in ("execution", "read"):
        try:
            _RUNTIMES[key]["stop"](ContextInfo)
        except BaseException as exc:
            errors.append(key + ":" + type(exc).__name__)
    if errors:
        raise RuntimeError("dual source cleanup incomplete: " + ",".join(errors))
'''


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--account-id", default=ACCOUNT)
    parser.add_argument("--environment", choices=("SIM", "LIVE"), default="SIM")
    parser.add_argument("--native-pid", type=int, required=True)
    parser.add_argument("--windows-session", type=int, required=True)
    parser.add_argument("--hard-deadline-unix", type=float)
    parser.add_argument("--read-name", required=True)
    parser.add_argument("--execution-name", required=True)
    parser.add_argument("--symbols", required=True, help="已确认白名单代码，逗号分隔")
    parser.add_argument("--max-seconds", type=int, default=900)
    parser.add_argument("--max-requests", type=int, default=1000)
    parser.add_argument("--enable-market-rpc", action="store_true", help="复用已有市场读能力；默认关闭")
    parser.add_argument("--persistent-readonly", action="store_true", help="仅55013746禁写驻留，必须显式max-seconds=0/max-requests=0且无deadline")
    parser.add_argument("--persistent-simulation-read-write", action="store_true", help="显式55013746模拟读写驻留；读源禁写，执行源启用普通股票限价gateway；必须零预算无deadline")
    parser.add_argument("--persistent-live-read-write", action="store_true", help="显式实盘常驻读写候选；仅构建不启动，必须零预算无deadline")
    args = parser.parse_args()
    source = build_source(account_id=args.account_id, native_pid=args.native_pid,
        windows_session=args.windows_session, hard_deadline_unix=args.hard_deadline_unix,
        read_name=args.read_name, execution_name=args.execution_name, symbols=args.symbols.split(","),
        max_seconds=args.max_seconds, max_requests=args.max_requests, enable_market_rpc=args.enable_market_rpc,
        persistent_readonly=args.persistent_readonly,
        persistent_simulation_read_write=args.persistent_simulation_read_write,
        environment=args.environment, persistent_live_read_write=args.persistent_live_read_write)
    descriptor = os.open(str(args.output), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as stream:
        stream.write(source)
    print("built dual SHM source: %s (%d bytes); no native process started" % (args.output, len(source.encode("utf-8"))))


if __name__ == "__main__":
    main()
