"""模型更新身份只声明源码内容，不连接 QMT 或发起业务请求。"""
import ast
import hashlib
import importlib.util
from pathlib import Path
import re

import pytest


ROOT = Path(__file__).resolve().parent.parent
SPEC = importlib.util.spec_from_file_location("runtime_update_dual_builder", ROOT / "tools/build_dual_shm_single_file.py")
BUILDER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(BUILDER)


def settings(**changes):
    values = dict(account_id="8882152350", environment="LIVE", native_pid=0, windows_session=1,
        native_executable=r"C:\QMT\XtItClient.exe", source_identity_path=r"C:\AQP\source.json",
        runtime_launcher=dict(python=r"C:\Python\python.exe", script=r"C:\AQP\supervise_qmt_runtime.py",
                              config=r"C:\AQP\supervisor.json"),
        hard_deadline_unix=None, max_seconds=0, max_requests=0, persistent_live_read_write=True,
        read_name=r"Local\AQP_SHM_READONLY_UPDATE", execution_name=r"Local\AQP_SHM_TRADE_UPDATE",
        symbols=["000300.SH"], runtime_update_identity=True)
    values.update(changes)
    return values


def literal(source, name):
    return next(ast.literal_eval(node.value) for node in ast.parse(source).body
                if isinstance(node, ast.Assign) and len(node.targets) == 1
                and isinstance(node.targets[0], ast.Name) and node.targets[0].id == name)


def test_explicit_model_identity_covers_the_complete_generated_source():
    source = BUILDER.build_source(**settings())
    identity = literal(source, "AQP_NATIVE_SOURCE_ID")
    assert re.fullmatch(r"[0-9a-f]{64}", identity)
    assert literal(source, "DUAL_SHM_SETTINGS")["runtime_update_identity"] is True
    declaration = "AQP_NATIVE_SOURCE_ID = %r" % identity
    canonical = source.replace(declaration, "AQP_NATIVE_SOURCE_ID = 'AQP_NATIVE_SOURCE_ID_PLACEHOLDER'", 1)
    assert hashlib.sha256(canonical.encode("ascii")).hexdigest() == identity
    assert "identity[\"runtime_update_model_id\"] = AQP_NATIVE_SOURCE_ID" in source
    assert "identity[\"runtime_update_contract\"] = \"QMT_RUNTIME_UPDATE_MODEL_V1\"" in source
    assert BUILDER.build_source(**settings()) == source


def test_identity_is_opt_in_and_requires_a_resident_live_launcher():
    source = BUILDER.build_source(**settings(runtime_update_identity=False))
    assert "AQP_NATIVE_SOURCE_ID =" not in source
    assert "runtime_update_identity" not in literal(source, "DUAL_SHM_SETTINGS")
    for change in (dict(runtime_update_identity="true"), dict(runtime_launcher=None),
                   dict(native_pid=123, native_executable=None, source_identity_path=None)):
        with pytest.raises(ValueError):
            BUILDER.build_source(**settings(**change))


def test_win_model_identity_tracks_its_config_without_changing_existing_trade_contract():
    launcher = dict(python=r"D:\miniconda3\envs\qmt_310\python.exe",
                    script=r"C:\AQP\win-fixture\aqp_windows_gateway\supervise_qmt_runtime.py",
                    config=r"C:\AQP\win-fixture\prepared\supervisor.json")
    arguments = settings(windows_session=2, runtime_launcher=launcher, execution_event_guard=False,
                         read_fault_isolation=False)
    source = BUILDER.build_source(**arguments)
    actual = literal(source, "DUAL_SHM_SETTINGS")
    assert actual["runtime_launcher"] == launcher and actual["windows_session"] == 2
    assert actual["execution_event_guard"] is False and actual["read_fault_isolation"] is False
    assert actual["persistent_live_read_write"] is True
    assert BUILDER.build_source(**arguments) == source
    changed = BUILDER.build_source(**{**arguments, "runtime_launcher": {
        **launcher, "config": r"C:\AQP\win-fixture\another\supervisor.json"}})
    assert literal(changed, "AQP_NATIVE_SOURCE_ID") != literal(source, "AQP_NATIVE_SOURCE_ID")
