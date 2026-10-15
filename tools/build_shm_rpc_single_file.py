# coding: utf-8
"""构建默认禁写SHM RPC单文件；只提取既有业务handler，不嵌Redis服务。"""

import argparse
import ast
import base64
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
SOURCE_FILES = {
    "bigqmt_signal_trader.models": "src/bigqmt_signal_trader/models.py",
    "bigqmt_signal_trader.code_utils": "src/bigqmt_signal_trader/code_utils.py",
    "bigqmt_signal_trader.adapters.position_bigqmt": "src/bigqmt_signal_trader/adapters/position_bigqmt.py",
    "bigqmt_signal_trader.adapters.account_readonly": "src/bigqmt_signal_trader/adapters/account_readonly.py",
    "bigqmt_signal_trader.exec_events": "src/bigqmt_signal_trader/exec_events.py",
    "bigqmt_signal_trader.transports.readonly_shm": "src/bigqmt_signal_trader/transports/readonly_shm.py",
    "bigqmt_signal_trader.transports.shm_rpc": "src/bigqmt_signal_trader/transports/shm_rpc.py",
    "bigqmt_signal_trader.shm_rpc_bridge": "src/bigqmt_signal_trader/shm_rpc_bridge.py",
}
_RPC_FUNCTIONS = frozenset(("_maybe_scalar", "to_jsonable", "_bool_value", "_credit_action_of", "_credit_optype_of", "_deployed_version", "_normalize_mapping_value", "_normalize_detail_rows"))
_RPC_CLASSES = frozenset(("OrderSettlement", "BigQmtRpcHandlers"))
_RPC_CONSTANTS = frozenset(("_monotonic", "RPC_REVISION", "READ_METHODS", "ORDER_METHODS", "METHOD_ALIASES", "BUY_ORDER_TYPES", "SELL_ORDER_TYPES", "CANCELABLE_ORDER_STATUSES", "MARKET_DATA_METHODS"))


def extract_rpc_handlers():
    source = (ROOT / "src/bigqmt_signal_trader/redis_rpc.py").read_text(encoding="utf-8")
    lines = source.splitlines(keepends=True)
    selected, found = [], set()
    for node in ast.parse(source).body:
        name = node.name if isinstance(node, (ast.FunctionDef, ast.ClassDef)) else None
        if isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
            name = node.targets[0].id
        if name in _RPC_FUNCTIONS | _RPC_CLASSES | _RPC_CONSTANTS:
            selected.append("".join(lines[node.lineno - 1:node.end_lineno]))
            found.add(name)
    expected = _RPC_FUNCTIONS | _RPC_CLASSES | _RPC_CONSTANTS
    if found != expected:
        raise RuntimeError("required existing handler definitions missing: %s" % sorted(expected - found))
    header = "import datetime as _dt\nimport json\nimport math\nimport time\nimport uuid\nfrom .code_utils import normalize_stock_code\nfrom .models import AccountSnapshot, OrderRef, OrderRequest\n\n"
    return header + "\n\n".join(selected)


def build_source(account_id=None, name=None, max_seconds=None, max_requests=None, native_stock_gateway=False,
                 bind_account_callbacks=False):
    if account_id is not None and (not isinstance(account_id, str) or not account_id or len(account_id) > 64):
        raise ValueError("explicit fixed account required")
    if name is not None and (not isinstance(name, str) or not re.fullmatch(r"Local\\AQP_SHM_TRADE_[A-Za-z0-9_-]{1,58}", name)):
        raise ValueError("explicit independent trade mailbox required")
    if max_seconds is not None and (type(max_seconds) is not int or not 1 <= max_seconds <= 1800):
        raise ValueError("runtime seconds must be within [1,1800]")
    if max_requests is not None and (type(max_requests) is not int or not 1 <= max_requests <= 1000):
        raise ValueError("request budget must be within [1,1000]")
    if type(native_stock_gateway) is not bool:
        raise ValueError("native stock gateway selection must be explicit boolean")
    if type(bind_account_callbacks) is not bool:
        raise ValueError("callback binding selection must be explicit boolean")
    sources = {name: (ROOT / path).read_text(encoding="utf-8") for name, path in SOURCE_FILES.items()}
    sources["bigqmt_signal_trader.redis_rpc"] = extract_rpc_handlers()
    for module_name, module_source in sources.items():
        ast.parse(module_source, filename=module_name, feature_version=(3, 6))
    embedded = {name: base64.b64encode(source.encode("utf-8")).decode("ascii") for name, source in sources.items()}
    entry = (ROOT / "src/BIGQMT_SHM_RPC.py").read_text(encoding="utf-8")
    settings = {"BIGQMT_ACCOUNT_ID": account_id, "SHM_RPC_NAME": name,
                "SHM_RPC_MAX_SECONDS": max_seconds, "SHM_RPC_MAX_REQUESTS": max_requests,
                "SHM_RPC_BIND_ACCOUNT_CALLBACKS": True if bind_account_callbacks else None}
    lines = entry.splitlines(keepends=True)
    for node in ast.parse(entry).body:
        if isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
            key = node.targets[0].id
            if key in settings and settings[key] is not None:
                lines[node.lineno - 1:node.end_lineno] = ["%s = %r\n" % (key, settings[key])]
    entry = "".join(lines)
    if native_stock_gateway:
        entry += "\nSHM_RPC_ORDER_GATEWAY_FACTORY = create_native_stock_order_gateway\n"
    marker = "_EMBEDDED_SOURCES = {}  # BUILD_SHM_RPC_EMBEDDED"
    if entry.count(marker) != 1:
        raise RuntimeError("SHM RPC build marker missing or duplicated")
    output = entry.replace(marker, "_EMBEDDED_SOURCES = " + repr(embedded))
    output = output.encode("ascii", "backslashreplace").decode("ascii")
    ast.parse(output, feature_version=(3, 6))
    return output


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--account-id")
    parser.add_argument("--name")
    parser.add_argument("--max-seconds", type=int)
    parser.add_argument("--max-requests", type=int)
    parser.add_argument("--native-stock-gateway", action="store_true", help="选择原生股票factory；保持默认禁写")
    parser.add_argument("--bind-account-callbacks", action="store_true", help="显式绑定账户回报；保持默认禁写")
    args = parser.parse_args()
    output = build_source(account_id=args.account_id, name=args.name, max_seconds=args.max_seconds,
                          max_requests=args.max_requests, native_stock_gateway=args.native_stock_gateway,
                          bind_account_callbacks=args.bind_account_callbacks)
    with args.output.open("x", encoding="utf-8", newline="\n") as stream:
        stream.write(output)
    print("built SHM RPC single file: %s (%s bytes)" % (args.output, len(output.encode("ascii"))))


if __name__ == "__main__":
    main()
