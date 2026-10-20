# coding: utf-8
"""自包含构建共享内存只读入口；只嵌入直接依赖和既有序列化函数。"""

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
    "aqp_readonly_transport": "src/bigqmt_signal_trader/transports/readonly_shm.py",
    "bigqmt_signal_trader.adapters.account_readonly": "src/bigqmt_signal_trader/adapters/account_readonly.py",
}


def build_source(account_id=None, environment="SIM", persistent_live_read_write=False, name=None):
    if name is not None and (not isinstance(name, str) or not re.fullmatch(r"Local\\AQP_SHM_READONLY_[A-Za-z0-9_-]{1,58}", name)):
        raise ValueError("explicit independent read mailbox required")
    if environment not in ("SIM", "LIVE") or type(persistent_live_read_write) is not bool:
        raise ValueError("explicit SIM/LIVE environment and boolean LIVE policy required")
    if environment == "LIVE":
        if (not persistent_live_read_write or not isinstance(account_id, str) or not account_id
                or len(account_id) > 64 or account_id == "55013746" or name is None
                or not all("0" <= char <= "9" for char in account_id)):
            raise ValueError("LIVE read source requires explicit persistent LIVE policy and live numeric account")
    elif persistent_live_read_write:
        raise ValueError("persistent LIVE policy requires LIVE environment")
    sources = {name: (ROOT / path).read_text(encoding="utf-8") for name, path in SOURCE_FILES.items()}
    rpc_source = (ROOT / "src/bigqmt_signal_trader/redis_rpc.py").read_text(encoding="utf-8")
    tree = ast.parse(rpc_source)
    helpers = []
    lines = rpc_source.splitlines(keepends=True)
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name in ("_maybe_scalar", "to_jsonable"):
            helpers.append("".join(lines[node.lineno - 1:node.end_lineno]))
    if len(helpers) != 2:
        raise RuntimeError("existing RPC serializer helpers were not found")
    sources["aqp_readonly_serializer"] = "import datetime as _dt\nimport math\n\n" + "\n\n".join(helpers)
    embedded = {name: base64.b64encode(source.encode("utf-8")).decode("ascii") for name, source in sources.items()}
    entry = (ROOT / "src/BIGQMT_SHM_READONLY.py").read_text(encoding="utf-8")
    settings = {"BIGQMT_ACCOUNT_ID": account_id, "BIGQMT_ENVIRONMENT": environment, "READONLY_SHM_NAME": name}
    if persistent_live_read_write:
        settings.update(READONLY_PERSISTENT_LIVE_READ_WRITE=True, READONLY_PERSISTENT_READONLY=True,
                        READONLY_MAX_SECONDS=0, READONLY_MAX_REQUESTS=0)
    lines = entry.splitlines(keepends=True)
    for node in reversed(ast.parse(entry).body):
        if isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
            key = node.targets[0].id
            if key in settings and settings[key] is not None:
                lines[node.lineno - 1:node.end_lineno] = ["%s = %r\n" % (key, settings[key])]
    entry = "".join(lines)
    marker = "_EMBEDDED_SOURCES = {}  # BUILD_READONLY_EMBEDDED"
    if entry.count(marker) != 1:
        raise RuntimeError("readonly build marker is missing or duplicated")
    source = entry.replace(marker, "_EMBEDDED_SOURCES = " + repr(embedded))
    # 保持旧构建器的 ASCII 外壳及精确编码顺序，兼容 QMT 的 GBK 保存。
    source = source.encode("ascii", "backslashreplace").decode("ascii")
    ast.parse(source, feature_version=(3, 6))
    for name, module_source in sources.items():
        ast.parse(module_source, filename=name, feature_version=(3, 6))
    return source


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--account-id")
    parser.add_argument("--name")
    parser.add_argument("--environment", choices=("SIM", "LIVE"), default="SIM")
    parser.add_argument("--persistent-live-read-write", action="store_true")
    args = parser.parse_args()
    source = build_source(account_id=args.account_id, environment=args.environment,
                          persistent_live_read_write=args.persistent_live_read_write, name=args.name)
    # 已存在产物不覆盖，便于审查候选与保持可恢复性。
    with args.output.open("x", encoding="utf-8", newline="\n") as stream:
        stream.write(source)
    print("built shm readonly single file: %s (%s bytes)" % (args.output, len(source.encode("utf-8"))))


if __name__ == "__main__":
    main()
