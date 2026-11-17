# coding: utf-8
"""只内嵌本次只读入口需要的既有转换、序列化及无 Redis ZMQ 实现。"""

import argparse
import ast
import base64
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
SOURCE_FILES = {
    "bigqmt_signal_trader.models": "src/bigqmt_signal_trader/models.py",
    "bigqmt_signal_trader.code_utils": "src/bigqmt_signal_trader/code_utils.py",
    "bigqmt_signal_trader.adapters.position_bigqmt": "src/bigqmt_signal_trader/adapters/position_bigqmt.py",
    "aqp_readonly_transport": "bigqmt_no_redis/zmq_transport.py",
}


def build_source(entry_filename="BIGQMT_ZMQ_READONLY.py", transport_file=None, extra_source_files=None):
    source_files = dict(SOURCE_FILES)
    if transport_file is not None:
        source_files["aqp_readonly_transport"] = transport_file
    if extra_source_files:
        source_files.update(extra_source_files)
    sources = {name: (ROOT / path).read_text(encoding="utf-8") for name, path in source_files.items()}
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
    entry = (ROOT / "src" / entry_filename).read_text(encoding="utf-8")
    marker = "_EMBEDDED_SOURCES = {}  # BUILD_READONLY_EMBEDDED"
    if entry.count(marker) != 1:
        raise RuntimeError("readonly build marker is missing or duplicated")
    source = entry.replace(marker, "_EMBEDDED_SOURCES = " + repr(embedded))
    # QMT 编辑器按 GBK 保存；ASCII 外壳同时兼容其保存方式及 UTF-8 声明。
    # 中文仅出现在注释和文档字符串，转义后仍保留原文语义。
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
    with args.output.open("w", encoding="utf-8", newline="\n") as stream:
        stream.write(source)
    print("built readonly single file: %s (%s bytes)" % (args.output, len(source.encode("utf-8"))))


if __name__ == "__main__":
    main()
