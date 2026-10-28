"""不启动服务的隔离 Redis 容器、候选路径和子进程环境负测。"""

import copy
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest


ROOT = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("isolated_relay", ROOT / "windows_loopback_redis_relay.py")
RELAY = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(RELAY)
CONTAINER, VOLUME, PASSWORD = "a" * 64, "b" * 64, "fixture-secret-only"


@pytest.fixture
def container():
    return dict(Id=CONTAINER, State={"Running": True}, Config=dict(Image="redis:7",
        Cmd=["redis-server", "--save", "", "--appendonly", "no", "--requirepass", PASSWORD]),
        NetworkSettings={"Ports": {"6379/tcp": [{"HostIp": "127.0.0.1", "HostPort": "55965"}]}},
        Mounts=[dict(Type="volume", Name=VOLUME, Destination="/data")])


@pytest.mark.parametrize("fault", [None, "wrong_id", "stopped", "wrong_password", "save", "aof", "public_bind", "other_port", "host_mount", "wrong_volume", "extra_mount"])
def test_container_contract_requires_exact_isolated_container(monkeypatch, container, fault):
    item = copy.deepcopy(container)
    if fault == "wrong_id":
        item["Id"] = "c" * 64
    elif fault == "stopped":
        item["State"]["Running"] = False
    elif fault == "wrong_password":
        item["Config"]["Cmd"][-1] = "wrong"
    elif fault == "save":
        item["Config"]["Cmd"][2] = "60 1"
    elif fault == "aof":
        item["Config"]["Cmd"][4] = "yes"
    elif fault == "public_bind":
        item["NetworkSettings"]["Ports"]["6379/tcp"][0]["HostIp"] = "0.0.0.0"
    elif fault == "other_port":
        item["NetworkSettings"]["Ports"]["6379/tcp"][0]["HostPort"] = "6379"
    elif fault == "host_mount":
        item["Mounts"][0]["Type"] = "bind"
    elif fault == "wrong_volume":
        item["Mounts"][0]["Name"] = "c" * 64
    elif fault == "extra_mount":
        item["Mounts"].append(dict(Type="bind", Destination="/other"))
    monkeypatch.setattr(RELAY.subprocess, "run", lambda *args, **kwargs: SimpleNamespace(stdout=json.dumps([item]).encode()))
    if fault is None:
        RELAY.verify_container(CONTAINER, VOLUME, 55965, PASSWORD)
    else:
        with pytest.raises(RuntimeError, match="contract mismatch"):
            RELAY.verify_container(CONTAINER, VOLUME, 55965, PASSWORD)


def test_only_explicit_system_and_isolated_environment_reach_guest_child():
    source = dict(SystemRoot=r"C:\Windows", TEMP=r"C:\Temp", TMP=r"C:\Temp", PATH=r"C:\prod\bin",
                  BIGQMT_CONFIG="production", REDIS_PASSWORD="production-secret", CODEX_HOME=r"C:\prod",
                  BIGQMT_LOG_ENABLED="1")
    values = {"AQP_ISOLATED_RUN_ID": "c" * 32, "AQP_ISOLATED_REDIS_PASSWORD": PASSWORD}
    result = RELAY.isolated_child_environment(values, r"C:\isolated\python.exe", 12345, source)
    assert result["AQP_ISOLATED_REDIS_PASSWORD"] == PASSWORD and result["AQP_ISOLATED_REDIS_PORT"] == "12345"
    assert result["PYTHONNOUSERSITE"] == "1" and r"C:\prod" not in result["PATH"]
    assert result["BIGQMT_LOG_ENABLED"] == "0"
    assert all(key not in result for key in ("BIGQMT_CONFIG", "REDIS_PASSWORD", "CODEX_HOME"))
    with pytest.raises(RuntimeError, match="environment is invalid"):
        RELAY.isolated_child_environment({"REDIS_PASSWORD": "production"}, r"C:\isolated\python.exe", 1, source)


def test_isolated_environment_disables_converter_log_creation_and_cleanup(monkeypatch, tmp_path):
    source = {"SystemRoot": r"C:\Windows", "BIGQMT_LOG_ENABLED": "1"}
    env = RELAY.isolated_child_environment({}, r"C:\isolated\python.exe", 12345, source)
    monkeypatch.setenv("BIGQMT_LOG_ENABLED", env["BIGQMT_LOG_ENABLED"])
    path = ROOT.parent / "src/bigqmt_signal_trader/logging_setup.py"
    spec = importlib.util.spec_from_file_location("isolated_converter_logging_fixture", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module._LOGGER_NAME = "bigqmt.isolated_relay_test"
    denied_dir, denied_cleanup = mock.Mock(), mock.Mock()
    monkeypatch.setattr(module, "_resolve_log_dir", denied_dir)
    monkeypatch.setattr(module, "_cleanup_old_logs", denied_cleanup)
    monkeypatch.setattr(module.logging.handlers, "TimedRotatingFileHandler", mock.Mock())
    monkeypatch.chdir(tmp_path)
    module.get_logger("probe")
    denied_dir.assert_not_called()
    denied_cleanup.assert_not_called()
    module.logging.handlers.TimedRotatingFileHandler.assert_not_called()
    assert list(tmp_path.iterdir()) == []


def test_windows_candidate_paths_cannot_escape_explicit_root():
    root = r"Z:\.codex\tmp\candidate\converter"
    assert RELAY.under_windows_root(root + r"\tests\probe.py", root)
    assert not RELAY.under_windows_root(root + r"\..\other\probe.py", root)
    assert not RELAY.under_windows_root(r"Z:\Projects\xtquant_big_convert\tests\probe.py", root)
    assert not RELAY.under_windows_root(r"..\tests\probe.py", root)


def test_resolved_candidate_paths_reject_symlink_escape(tmp_path):
    candidate = tmp_path / "candidate"
    candidate.mkdir()
    accepted = candidate / "driver.py"
    accepted.write_text("pass", encoding="ascii")
    outside = tmp_path / "workspace.py"
    outside.write_text("pass", encoding="ascii")
    alias = candidate / "alias.py"
    alias.symlink_to(outside)
    assert RELAY.resolved_under(accepted, candidate)
    assert not RELAY.resolved_under(alias, candidate)


def test_launch_diagnostic_never_copies_untrusted_stderr():
    result = RELAY.prlctl_failure_detail(255, "Unable to connect; SECRET-DO-NOT-LOG")
    assert "TRANSPORT_UNAVAILABLE" in result and "255" in result and "SECRET-DO-NOT-LOG" not in result
    result = RELAY.prlctl_failure_detail(255, "PrlJob_GetResult: Invalid argument. An invalid argument was passed.")
    assert "PRLCTL_JOB_INVALID_ARGUMENT" in result and "PrlJob_GetResult" not in result
