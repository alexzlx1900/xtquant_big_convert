# coding: utf-8
"""自包含构建共享内存只读入口；只嵌入直接依赖和既有序列化函数。"""

import argparse
import ast
import base64
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
SOURCE_FILES = {
    "bigqmt_signal_trader.models": "src/bigqmt_signal_trader/models.py",
    "bigqmt_signal_trader.code_utils": "src/bigqmt_signal_trader/code_utils.py",
    "bigqmt_signal_trader.adapters.position_bigqmt": "src/bigqmt_signal_trader/adapters/position_bigqmt.py",
    "aqp_readonly_transport": "src/bigqmt_signal_trader/transports/readonly_shm.py",
    "bigqmt_signal_trader.adapters.account_readonly": "src/bigqmt_signal_trader/adapters/account_readonly.py",
}


def build_source():
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
    args = parser.parse_args()
    source = build_source()
    # 已存在产物不覆盖，便于审查候选与保持可恢复性。
    with args.output.open("x", encoding="utf-8", newline="\n") as stream:
        stream.write(source)
    print("built shm readonly single file: %s (%s bytes)" % (args.output, len(source.encode("utf-8"))))


if __name__ == "__main__":
    main()
