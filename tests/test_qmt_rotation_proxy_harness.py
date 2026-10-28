# coding: utf-8
"""仅测试隔离进程入口的硬边界；不连接 Redis、SHM 或交易源。"""

import json
from types import SimpleNamespace

import pytest

from bigqmt_signal_trader.transports.readonly_shm import ShmProtocolError
from bigqmt_signal_trader.transports.shm_rpc import ALL_WRITE_METHODS
import qmt_rotation_proxy_harness as harness
from qmt_rotation_proxy_harness import (IsolatedReadonlyProxy, gateway_read_marker_matches, isolated_config,
                                       observe_callback_gap, observe_execution_cursor, verify_isolated_redis)


@pytest.fixture
def isolated_environment(tmp_path, monkeypatch):
    run_id = "a" * 32
    monkeypatch.setattr(harness.tempfile, "gettempdir", lambda: str(tmp_path))
    root = tmp_path / ("aqp-rotation-" + run_id)
    root.mkdir()
    read_base = "Local\\AQP_SHM_READONLY_ISO_" + run_id
    execution_base = "Local\\AQP_SHM_TRADE_ISO_" + run_id
    source = dict(account_id="8882152350", environment="LIVE", runtime_policy="PERSISTENT_LIVE_READ_WRITE",
                  read_name=read_base + "_" + "1" * 16, execution_name=execution_base + "_" + "2" * 16,
                  read_session_id="3" * 32, execution_session_id="4" * 32, native_pid=567,
                  native_creation_filetime=12345, windows_session=1)
    source_path = root / "source.json"
    source_path.write_text(json.dumps(source), encoding="utf-8")
    env = dict(AQP_ISOLATED_RUN_ID=run_id, AQP_ISOLATED_ROOT=str(root),
               AQP_ISOLATED_SOURCE_PATH=str(source_path), AQP_ISOLATED_READ_BASE=read_base,
               AQP_ISOLATED_EXECUTION_BASE=execution_base, AQP_ISOLATED_REDIS_PORT="55965",
               AQP_ISOLATED_REDIS_DB="3", AQP_ISOLATED_REDIS_PASSWORD="isolated-password-value",
               AQP_ISOLATED_WINDOWS_SESSION="1", AQP_ISOLATED_OLD_READ_RID="isolated-original-read",
               AQP_ISOLATED_NATIVE_EXE=str(tmp_path / "python.exe"))
    return env, source, source_path


def test_harness_accepts_only_explicit_isolated_binding(isolated_environment):
    env, _, _ = isolated_environment
    config = isolated_config(env)
    assert config["redis"] == dict(host="127.0.0.1", port=55965, db=3, password=env["AQP_ISOLATED_REDIS_PASSWORD"])
    assert config["run_id"] == env["AQP_ISOLATED_RUN_ID"]


def test_two_generations_require_distinct_original_request_ids(isolated_environment):
    env, _, _ = isolated_environment
    env.update(AQP_ISOLATED_SCENARIO="two_generations", AQP_ISOLATED_OLD_READ_RID="b" * 32,
               AQP_ISOLATED_SECOND_READ_RID="c" * 32)
    assert isolated_config(env)["second_rid"] == "c" * 32
    env["AQP_ISOLATED_SECOND_READ_RID"] = env["AQP_ISOLATED_OLD_READ_RID"]
    with pytest.raises(ValueError):
        isolated_config(env)


@pytest.mark.parametrize("field,value", [
    ("AQP_ISOLATED_RUN_ID", "not-a-random-run-id"),
    ("AQP_ISOLATED_READ_BASE", "Local\\AQP_SHM_READONLY_EXISTING"),
    ("AQP_ISOLATED_EXECUTION_BASE", "Local\\AQP_SHM_TRADE_EXISTING"),
    ("AQP_ISOLATED_REDIS_DB", "0"),
    ("AQP_ISOLATED_REDIS_PORT", "637"),
    ("AQP_ISOLATED_REDIS_PORT", "6379"),
    ("AQP_ISOLATED_REDIS_PASSWORD", "short"),
    ("AQP_ISOLATED_WINDOWS_SESSION", "2"),
    ("AQP_ISOLATED_OLD_READ_RID", ""),
    ("AQP_ISOLATED_SCENARIO", "production"),
])
def test_harness_rejects_unbound_environment(isolated_environment, field, value):
    env, _, _ = isolated_environment
    env[field] = value
    with pytest.raises(ValueError):
        isolated_config(env)


@pytest.mark.parametrize("field,value", [
    ("account_id", "55013746"),
    ("environment", "SIM"),
    ("runtime_policy", "PERSISTENT_SIMULATION_READ_WRITE"),
    ("read_name", "Local\\AQP_SHM_READONLY_EXISTING"),
    ("execution_name", "Local\\AQP_SHM_TRADE_EXISTING"),
    ("native_creation_filetime", None),
    ("native_pid", True),
    ("read_session_id", "bad"),
])
def test_harness_rejects_unbound_source(isolated_environment, field, value):
    env, source, path = isolated_environment
    source[field] = value
    path.write_text(json.dumps(source), encoding="utf-8")
    with pytest.raises(ValueError):
        isolated_config(env)


@pytest.mark.parametrize("value,ttl", [(None, -1), ("other", -1), ("a" * 32, 60)])
def test_harness_requires_exact_nonexpiring_redis_owner(value, ttl):
    class Redis:
        def get(self, key):
            return value

        def ttl(self, key):
            return ttl

    with pytest.raises(RuntimeError):
        verify_isolated_redis(Redis(), "a" * 32)


@pytest.mark.parametrize("method", sorted(ALL_WRITE_METHODS))
def test_harness_rejects_every_financial_method_before_normal_request_handling(method):
    proxy = object.__new__(IsolatedReadonlyProxy)
    with pytest.raises(ShmProtocolError, match="readonly methods only"):
        proxy._validate_request({"method": method})


@pytest.mark.parametrize("failure", [False, True])
def test_isolated_ping_observation_contains_only_bounded_evidence(monkeypatch, capsys, failure):
    def underlying(proxy, request):
        if failure:
            raise TimeoutError("credential-that-must-not-appear")
        return {"ok": False, "status": "UNKNOWN", "server_error": "SHM_RESULT_UNCONFIRMED",
                "publication_state": "UNKNOWN", "publication_scope": "SHM", "automatic_retry": False,
                "previous_unconfirmed": False, "sequence": 3, "data": "secret-response"}

    monkeypatch.setattr(harness.RedisShmReadOnlyProxy, "process_request", underlying)
    proxy = object.__new__(IsolatedReadonlyProxy)
    request = {"method": "ping", "request_id": "1" * 32, "read_expires_at_unix": 1234.0,
               "params": {"secret": "secret-request"}}
    if failure:
        with pytest.raises(TimeoutError):
            proxy.process_request(request)
    else:
        proxy.process_request(request)
    raw = capsys.readouterr().out
    observation = json.loads(raw)
    assert observation["event"] == "ISOLATED_PING_OBSERVATION" and observation["request_id"] == "1" * 32
    assert observation["read_expires_at_unix"] == 1234.0 and type(observation["proxy_received_at_unix"]) is float
    assert "secret-request" not in raw and "secret-response" not in raw and "credential-that-must-not-appear" not in raw
    if failure:
        assert observation["outcome"] == "EXCEPTION" and observation["error_type"] == "TimeoutError"
    else:
        assert observation["publication_state"] == "UNKNOWN" and observation["previous_unconfirmed"] is False
        assert observation["server_error"] == "SHM_RESULT_UNCONFIRMED" and observation["sequence"] == 3


@pytest.mark.parametrize("scenario", ["valid", "invalid_native", "control_claimed"])
def test_gap_harness_leaves_the_final_execution_poll_frame_untouched(isolated_environment, scenario):
    env, _, _ = isolated_environment
    config = isolated_config(env)
    (config["root"] / "gap-verified.marker").write_text(config["run_id"], encoding="ascii")
    calls = []

    class Redis:
        def hlen(self, key):
            return 0

    class Proxy:
        timeout_seconds = 0.2
        _read_rotation_key = "rotation"
        _rotation_control_attempted = set()
        _unconfirmed_read_ids = {"readonly": {config["old_rid"]}}
        _write_block_reasons = []
        transport = SimpleNamespace(redis=Redis())

        def _execution_control_ping(self):
            calls.append("ping")
            return {"data": {"writes_blocked": scenario != "invalid_native",
                "write_block_reason": "EXECUTION_EVENT_LOSS; outcome=UNKNOWN", "read_rotation_evidence": None,
                "native_creation_filetime": config["source"]["native_creation_filetime"]}}

        def prepare_readonly_rotation(self):
            calls.append("prepare")
            if scenario == "control_claimed":
                self._rotation_control_attempted.add(("transition", "PREPARE"))
            raise ShmProtocolError("gap prevents preparation")

        def process_request(self, request):
            calls.append("poll")
            assert request["method"] == "poll_execution_events" and request["params"] == {"after_sequence": 0}
            self._write_block_reasons.append("EXECUTION_EVENT_GAP")
            return {"ok": True, "data": {"gap": True, "lost_sequence": 1, "automatic_retry": False,
                "events": [{"event": {"event_type": "data_loss", "data_loss": True}}]}}

    proxy = Proxy()
    if scenario == "valid":
        assert observe_callback_gap(proxy, config, 10**12)["status"] == "GAP_VERIFIED"
        assert calls == ["ping", "prepare", "poll"]
        assert (config["root"] / "gap-observed.marker").read_text(encoding="ascii") == config["run_id"]
    else:
        with pytest.raises(ShmProtocolError):
            observe_callback_gap(proxy, config, 10**12)
        assert "poll" not in calls and not (config["root"] / "gap-observed.marker").exists()


def test_cursor_barrier_sends_exactly_one_poll_and_waits_for_manager_capture(isolated_environment):
    env, _, _ = isolated_environment
    config = isolated_config(env)
    (config["root"] / "execution-cursor-initial-captured.marker").write_text(config["run_id"], encoding="ascii")
    calls = []
    binding = {"read_session_id": "1" * 32, "execution_session_id": "2" * 32, "native_pid": 3, "windows_session_id": 1}

    class Proxy:
        def identity_snapshot(self):
            return {"source_binding": binding}

        def process_request(self, request):
            calls.append(request)
            return {"ok": True, "data": {"events": [], "latest_sequence": 0, "lost_sequence": 0}}

        def _execution_event_state(self, data, request):
            return "HEALTHY"

    observe_execution_cursor(Proxy(), config, "initial", 10**12)
    assert len(calls) == 1 and calls[0]["method"] == "poll_execution_events"
    assert calls[0]["params"] == {"after_sequence": 0} and calls[0]["expected_source_binding"] == binding
    assert (config["root"] / "execution-cursor-initial.marker").read_text(encoding="ascii") == config["run_id"]


@pytest.mark.parametrize("bad_marker", [False, True])
def test_two_generation_harness_only_synchronizes_and_delegates_stages(isolated_environment, monkeypatch, bad_marker):
    env, _, _ = isolated_environment
    env.update(AQP_ISOLATED_SCENARIO="two_generations", AQP_ISOLATED_OLD_READ_RID="b" * 32,
               AQP_ISOLATED_SECOND_READ_RID="c" * 32)
    config = isolated_config(env)
    for name in ("gateway-initial-pings-complete", "old-frame-one-captured", "old-frame-two-captured",
                 "generation-one-ready", "gateway-final-next-ready", "manager-complete"):
        (config["root"] / (name + ".marker")).write_text(config["run_id"], encoding="ascii")
    for name, rid in (("gateway-read-unknown.marker", "b" * 32),
                      ("gateway-read-unknown-two.marker", "c" * 32)):
        (config["root"] / name).write_text(json.dumps({"run_id": config["run_id"],
            "request_id": "changed" if bad_marker and rid == "b" * 32 else rid}), encoding="utf-8")
    calls, cursors = [], []
    stages = {"IDLE": "CAPABILITY", "CAPABILITY": "COMMITTED", "COMMITTED": "ACTIVATED",
              "ACTIVATED": "GATEWAY_VERIFIED", "GATEWAY_VERIFIED": "IDLE"}

    class Driver:
        def __init__(self, proxy):
            self.stage, self.blocked, self.error_type = "IDLE", False, None

        def step(self):
            calls.append(self.stage)
            self.stage = stages[self.stage]

    monkeypatch.setattr(harness, "ReadonlyRotationDriver", Driver)
    monkeypatch.setattr(harness, "old_frame_completed", lambda proxy, rid: rid in ("b" * 32, "c" * 32))
    monkeypatch.setattr(harness, "gateway_read_marker_matches", lambda proxy, config, name: True)
    monkeypatch.setattr(harness, "observe_execution_cursor", lambda proxy, config, kind, deadline: cursors.append(kind))
    monkeypatch.setattr(harness.time, "sleep", lambda seconds: None)
    proxy = SimpleNamespace(stopped=False, timeout_seconds=0.2, poll_once=lambda: None)
    if bad_marker:
        with pytest.raises(ShmProtocolError, match="marker identity changed"):
            harness.run_two_generations_with_driver(proxy, config, 10**12)
        assert not calls and cursors == ["initial"]
    else:
        result = harness.run_two_generations_with_driver(proxy, config, 10**12)
        assert result == {"status": "TWO_GENERATIONS_CLOSED", "read_rotation_accepted": True,
                          "automatic_driver": True, "automatic_retry": False}
        assert calls == list(stages) * 2 and cursors == ["initial", "final"]
        assert (config["root"] / "old-frame-one-ready.marker").is_file()
        assert (config["root"] / "old-frame-two-ready.marker").is_file()


@pytest.mark.parametrize("changed", [False, True])
def test_gateway_read_marker_is_only_a_bound_synchronization_hint(isolated_environment, changed):
    env, _, _ = isolated_environment
    config = isolated_config(env)
    proxy = SimpleNamespace(_accepted_rotation_record={"transition_id": "1" * 32}, _sessions={"readonly": "2" * 32})
    marker = {"run_id": config["run_id"], "transition_id": "1" * 32,
              "read_session_id": "3" * 32 if changed else "2" * 32}
    (config["root"] / "gateway-first-read.marker").write_text(json.dumps(marker), encoding="utf-8")
    if changed:
        with pytest.raises(ShmProtocolError):
            gateway_read_marker_matches(proxy, config, "gateway-first-read.marker")
    else:
        assert gateway_read_marker_matches(proxy, config, "gateway-first-read.marker")
