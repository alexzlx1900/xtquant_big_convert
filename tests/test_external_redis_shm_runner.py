# coding: utf-8
"""外置入口默认离线、明确会话/预算、无隐式重试与退出所有权。"""

from concurrent.futures import ThreadPoolExecutor
import importlib.util
import json
import os
from pathlib import Path
import socket
import threading
import time
from types import SimpleNamespace
from unittest import mock

import pytest


ROOT = Path(__file__).resolve().parents[1]


def load_file(name, relative):
    spec = importlib.util.spec_from_file_location(name, ROOT / relative)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


RUNNER = load_file("external_redis_runner_fixture", "tools/run_external_redis_shm_proxy.py")
PIPELINE = load_file("external_redis_runner_pipeline", "tests/test_external_redis_dual_shm_integration.py")
PROXY_FIXTURES = load_file("external_redis_runner_proxy_fixtures", "tests/bigqmt_signal_trader/test_redis_shm_proxy.py")


def config(**changes):
    value = dict(account_id="55013746", allow_order_methods=False, native_pid=7000, windows_session=1,
                 read_name="Local\\AQP_SHM_READONLY_RUNNER", execution_name="Local\\AQP_SHM_TRADE_RUNNER",
                 read_session_id="1" * 32, execution_session_id="2" * 32,
                 source_deadline_unix=time.time() + 300, timeout_seconds=1.0,
                 max_seconds=60, max_requests=10,
                 redis=dict(host="127.0.0.1", port=6379, db=6, username=None, password="private-secret"))
    return dict(value, **changes)


@pytest.mark.parametrize("change", [
    {"account_id": "8882152350"}, {"allow_order_methods": True},
    {"read_session_id": "2" * 32}, {"max_requests": True},
    {"source_deadline_unix": float("nan")}, {"source_deadline_unix": time.time() - 1},
    {"redis": dict(host="remote.invalid", port=6379, db=6)},
    {"redis": dict(host="127.0.0.1", port=6379, db=6, url="redis://remote.invalid")},
])
def test_invalid_or_expanded_configuration_is_rejected_before_runtime(change):
    with pytest.raises(ValueError):
        RUNNER.validate_config(config(**change))


def test_target_config_requires_exact_vm_profile_and_keeps_same_account_legacy_by_default():
    sim = {"gateway_id": "qmt-vm-001", "profile_id": "sim-bigqmt"}
    live = {"gateway_id": "qmt-vm-001", "profile_id": "live-stock"}
    assert RUNNER.validate_config(config(execution_cursor_guard_target=sim))["execution_cursor_guard_target"] == sim
    legacy = RUNNER.validate_config(config(gateway_id="qmt-win-001"))
    assert legacy["execution_cursor_guard_target"] is None
    live_value = config(account_id="8882152350", environment="LIVE", runtime_policy="PERSISTENT_LIVE_READ_WRITE",
                        allow_order_methods=True, source_deadline_unix=None, max_requests=0, max_seconds=0,
                        execution_cursor_guard_target=live)
    assert RUNNER.validate_config(live_value)["execution_cursor_guard_target"] == live
    for target in ({"gateway_id": "qmt-win-001", "profile_id": "sim-bigqmt"}, live,
                   {"gateway_id": "qmt-vm-001", "profile_id": "sim-bigqmt", "extra": True}):
        with pytest.raises(ValueError, match="exact authorized VM target"):
            RUNNER.validate_config(config(execution_cursor_guard_target=target))
    with pytest.raises(ValueError, match="conflicts with explicit runner"):
        RUNNER.validate_config(config(execution_cursor_guard_target=sim, gateway_id="qmt-win-001"))


def rotation_driver_fixture(*, target=None, account="8882152350", environment="LIVE", previous=False):
    request = dict(request_id="old-read", session_id="old-sid", sequence=3, read_expires_at_unix=time.time() - 1)
    raw = json.dumps(request).encode("ascii")
    region = SimpleNamespace(acquire=lambda milliseconds: True, release=lambda: None,
                             read=lambda offset, length: raw[:length])
    mailbox = SimpleNamespace(closed=False, region=region, _header=lambda: [0, 0, 0, 0, 3, 3, 2, len(raw)])
    proxy = SimpleNamespace(
        stopped=False, execution_cursor_guard_target=target or {"gateway_id": "qmt-vm-001", "profile_id": "live-stock"},
        account_id=account, environment=environment, _supervisor_rotation_channel=object(), _supervisor_write_gate=object(),
        _route_faults={"readonly": dict(state="RESULT_UNKNOWN", publication_state="UNKNOWN", previous_unconfirmed=previous,
                                        sequence=3, trigger_request_id="old-read"), "execution": None},
        _clients={"readonly": SimpleNamespace(mailbox=mailbox)}, _sessions={"readonly": "old-sid"},
        _read_rotation_blocked=False, _read_rotation_stage=None, _rotation_capability_attempted=False,
        _rotation_capability=None, _last_execution_identity_success_at=None,
        _source_identity={"execution": {"read_rotation_authorization_ready": True}})
    calls = []

    def attest():
        calls.append("CAPABILITY")
        proxy._rotation_capability_attempted = True
        proxy._rotation_capability = {"verified_at": time.monotonic()}
        return {"status": "VERIFIED", "native_rotation_ready": True}

    def prepare():
        calls.append("PREPARE")
        proxy._read_rotation_stage = "COMMITTED"
        return {"status": "COMMITTED"}

    def activate():
        calls.append("ACTIVATE")
        proxy._read_rotation_stage = "ACTIVATED"
        return {"status": "ACTIVATED"}

    def accept():
        calls.append("ACCEPT")
        proxy._read_rotation_stage = "GATEWAY_VERIFIED"
        return {"status": "READ_ROTATION_ACCEPTED"}

    def close():
        calls.append("CLOSE")
        proxy._read_rotation_stage = None
        proxy._route_faults["readonly"] = None
        return {"status": "ROTATION_CLOSED"}

    proxy.attest_read_rotation_capability, proxy.prepare_readonly_rotation = attest, prepare
    proxy.activate_readonly_rotation, proxy.accept_gateway_rotation, proxy.close_readonly_rotation = activate, accept, close
    return proxy, calls


def _driver_step(driver):
    driver.next_observation = 0
    driver.step()


def test_live_rotation_driver_advances_only_complete_original_read_and_keeps_one_shot_phases():
    proxy, calls = rotation_driver_fixture()
    driver = RUNNER.ReadonlyRotationDriver(proxy)
    for _ in range(5):
        _driver_step(driver)
    assert calls == ["CAPABILITY", "PREPARE", "ACTIVATE", "ACCEPT", "CLOSE"]
    assert driver.stage == "IDLE" and not driver.blocked


@pytest.mark.parametrize("outcome", ["two_generations", "prepare_unknown"])
def test_candidate_loop_schedules_rotation_and_never_retries_unknown(monkeypatch, capsys, outcome):
    """调用真实运行入口和调度器；原生、Redis、监督器均为无网络替身。"""
    proxy, calls = rotation_driver_fixture()
    value = config(account_id="8882152350", environment="LIVE", runtime_policy="PERSISTENT_LIVE_READ_WRITE",
                   allow_order_methods=True, source_deadline_unix=None, max_requests=0, max_seconds=0,
                   execution_cursor_guard_target={"gateway_id": "qmt-vm-001", "profile_id": "live-stock"})
    stop = threading.Event()
    provider = SimpleNamespace(connection_pool=SimpleNamespace(disconnect=mock.Mock()))
    clients = [SimpleNamespace(close=mock.Mock()), SimpleNamespace(close=mock.Mock())]
    gate, channel = SimpleNamespace(close=mock.Mock()), SimpleNamespace(close=mock.Mock())
    monotonic = [time.monotonic()]

    def tick():
        monotonic[0] += 0.3
        return monotonic[0]

    monkeypatch.setattr(RUNNER, "time", SimpleNamespace(time=time.time, monotonic=tick, sleep=lambda seconds: None))
    monkeypatch.setattr(RUNNER, "windows_session_id", lambda pid: 1)
    monkeypatch.setattr(RUNNER.InheritedSupervisorWriteGate, "from_environment", lambda: gate)
    monkeypatch.setattr(RUNNER.InheritedRotationChannel, "from_environment", lambda: channel)
    monkeypatch.setattr(RUNNER, "RedisShmReadOnlyProxy", lambda **kwargs: proxy)
    proxy.verified, proxy.stop_reason, proxy.processed_count = True, "", 0
    proxy.attach_native_supervisor = mock.Mock()
    proxy.verify_identity = mock.Mock()
    proxy._block_writes = mock.Mock()
    proxy.identity_snapshot = lambda: dict(writes_blocked=True, write_ready=False, write_block_reason="OPERATOR_WRITE_DISABLED")
    proxy.diagnostic_snapshot = lambda: dict(first_error=None, last_error=None)
    proxy.close = mock.Mock()
    poll_count, original_ids = [], []

    def poll():
        poll_count.append(1)
        if len(poll_count) > 20:
            stop.set()
        return None

    proxy.poll_once = poll
    original_attest, original_close = proxy.attest_read_rotation_capability, proxy.close_readonly_rotation

    def attest():
        original_ids.append(proxy._route_faults["readonly"]["trigger_request_id"])
        result = original_attest()
        proxy._rotation_capability["verified_at"] = RUNNER.time.monotonic()
        return result

    def close_generation():
        result = original_close()
        if len(original_ids) == 2:
            stop.set()
        else:
            request = dict(request_id="second-read", session_id="second-sid", sequence=7,
                           read_expires_at_unix=time.time() - 1)
            raw = json.dumps(request).encode("ascii")
            mailbox = proxy._clients["readonly"].mailbox
            mailbox.region.read = lambda offset, length: raw[:length]
            mailbox._header = lambda: [0, 0, 0, 0, 7, 7, 6, len(raw)]
            proxy._sessions["readonly"] = "second-sid"
            proxy._route_faults["readonly"] = dict(state="RESULT_UNKNOWN", publication_state="UNKNOWN",
                previous_unconfirmed=False, sequence=7, trigger_request_id="second-read")
            proxy._rotation_capability_attempted = False
        return result

    proxy.attest_read_rotation_capability, proxy.close_readonly_rotation = attest, close_generation
    if outcome == "prepare_unknown":
        def fail_prepare():
            calls.append("PREPARE")
            proxy._read_rotation_blocked = True
            raise TimeoutError("fixture outcome unknown")
        proxy.prepare_readonly_rotation = fail_prepare

    result = RUNNER.run_candidate(value, lambda target: provider, mock.Mock(side_effect=clients),
                                  stop_event=stop, ready_observer=lambda proof: None)
    assert result["status"] == "STOP_REQUESTED" and result["resources_closed"]
    assert result["write_block_reason"] == "OPERATOR_WRITE_DISABLED" and result["write_ready"] is False
    assert proxy.attach_native_supervisor.call_count == proxy.verify_identity.call_count == 1
    assert provider.connection_pool.disconnect.call_count == 1 and all(client.close.call_count == 1 for client in clients)
    if outcome == "two_generations":
        assert original_ids == ["old-read", "second-read"]
        assert calls == ["CAPABILITY", "PREPARE", "ACTIVATE", "ACCEPT", "CLOSE"] * 2
    else:
        assert original_ids == ["old-read"] and calls == ["CAPABILITY", "PREPARE"]
        assert capsys.readouterr().out.count('"event": "READ_ROTATION_DRIVER_BLOCKED"') == 1


@pytest.mark.parametrize("previous", [None, True])
def test_live_rotation_driver_requires_explicit_previous_ack_evidence(previous):
    proxy, calls = rotation_driver_fixture(previous=previous)
    driver = RUNNER.ReadonlyRotationDriver(proxy)
    _driver_step(driver)
    assert not calls and not driver.blocked


def test_live_rotation_driver_rejects_missing_prior_ack_and_nonadjacent_ack():
    proxy, calls = rotation_driver_fixture()
    driver = RUNNER.ReadonlyRotationDriver(proxy)
    proxy._route_faults["readonly"].pop("previous_unconfirmed")
    _driver_step(driver)
    proxy._route_faults["readonly"]["previous_unconfirmed"] = False
    mailbox = proxy._clients["readonly"].mailbox
    header = mailbox._header()
    mailbox._header = lambda: [*header[:6], 1, *header[7:]]
    _driver_step(driver)
    assert not calls and not driver.blocked


@pytest.mark.parametrize("target,account,environment", [
    ({"gateway_id": "qmt-win-001", "profile_id": "live-stock"}, "8882152350", "LIVE"),
    ({"gateway_id": "qmt-vm-001", "profile_id": "live-stock"}, "55013746", "SIM"),
])
def test_rotation_driver_does_not_change_other_instances(target, account, environment):
    proxy, calls = rotation_driver_fixture(target=target, account=account, environment=environment)
    _driver_step(RUNNER.ReadonlyRotationDriver(proxy))
    assert not calls


@pytest.mark.parametrize("stage", ["capability_unknown", "prepare_unknown", "activation_unknown", "accept_unknown", "close_unknown"])
def test_rotation_driver_never_repeats_a_phase_with_unknown_outcome(stage):
    proxy, calls = rotation_driver_fixture()
    driver = RUNNER.ReadonlyRotationDriver(proxy)
    def fail(name):
        calls.append(name)
        if name == "CAPABILITY":
            proxy._rotation_capability_attempted = True
        if name == "PREPARE":
            proxy._read_rotation_stage, proxy._read_rotation_blocked = "PREPARE_PENDING", True
        if name == "ACTIVATE":
            proxy._read_rotation_stage = "ACTIVATE_PENDING"
        raise TimeoutError("injected outcome unknown")
    field, name = {"capability_unknown": ("attest_read_rotation_capability", "CAPABILITY"),
                   "prepare_unknown": ("prepare_readonly_rotation", "PREPARE"),
                   "activation_unknown": ("activate_readonly_rotation", "ACTIVATE"),
                   "accept_unknown": ("accept_gateway_rotation", "ACCEPT"),
                   "close_unknown": ("close_readonly_rotation", "CLOSE")}[stage]
    setattr(proxy, field, lambda: fail(name))
    for _ in range(8):
        _driver_step(driver)
    assert driver.blocked and calls.count(name) == 1


def test_rotation_driver_waits_for_native_positive_capability_then_stops_after_proof_expires():
    proxy, calls = rotation_driver_fixture()
    proxy._source_identity["execution"]["read_rotation_authorization_ready"] = False
    original = proxy.attest_read_rotation_capability
    proxy.attest_read_rotation_capability = lambda: dict(original(), native_rotation_ready=False)
    driver = RUNNER.ReadonlyRotationDriver(proxy)
    _driver_step(driver)
    assert calls == ["CAPABILITY"] and not driver.blocked
    proxy._rotation_capability["verified_at"] -= 21
    _driver_step(driver)
    _driver_step(driver)
    assert driver.blocked and calls == ["CAPABILITY"]


def test_rotation_driver_cannot_use_pre_challenge_native_capability_ping():
    proxy, calls = rotation_driver_fixture()
    proxy._last_execution_identity_success_at = time.monotonic() - 1
    original = proxy.attest_read_rotation_capability
    proxy.attest_read_rotation_capability = lambda: dict(original(), native_rotation_ready=False)
    driver = RUNNER.ReadonlyRotationDriver(proxy)
    _driver_step(driver)
    assert calls == ["CAPABILITY"] and not driver.blocked
    proxy._last_execution_identity_success_at = time.monotonic()
    _driver_step(driver)
    assert calls == ["CAPABILITY", "PREPARE"]


@pytest.mark.parametrize("published_or_claimed", [False, True])
def test_rotation_driver_only_reobserves_busy_before_prepare_publication(published_or_claimed):
    proxy, calls = rotation_driver_fixture()
    driver = RUNNER.ReadonlyRotationDriver(proxy)
    def busy():
        calls.append("PREPARE")
        if published_or_claimed:
            proxy._route_faults["execution"] = {"state": "RESULT_UNKNOWN"}
        raise RUNNER.ShmBusyError("fixture local mailbox contention")
    proxy.prepare_readonly_rotation = busy
    _driver_step(driver)
    assert driver.blocked is published_or_claimed
    _driver_step(driver)
    assert calls.count("PREPARE") == (1 if published_or_claimed else 2)


def test_private_rotation_channel_poisoned_on_unknown_and_closes_once():
    class Pipe:
        def __init__(self):
            self.messages = []

        def write(self, value):
            self.messages.append(value)

        def read(self):
            raise TimeoutError("fixture reply unknown")

    kernel, pipe = SimpleNamespace(close=mock.Mock()), Pipe()
    channel = RUNNER.InheritedRotationChannel(11, 12, kernel, pipe)
    with pytest.raises(TimeoutError):
        channel.register({"stage": "PREPARE"})
    with pytest.raises(RuntimeError, match="transport unavailable"):
        channel.register({"stage": "PREPARE"})
    assert pipe.messages == [{"stage": "PREPARE"}]
    channel.close()
    channel.close()
    assert kernel.close.call_args_list == [mock.call(11), mock.call(12)]


@pytest.mark.parametrize("valid", [True, False])
def test_private_rotation_inherited_pipe_requires_exact_types_and_rights(monkeypatch, valid):
    monkeypatch.setattr(RUNNER, "os", SimpleNamespace(name="nt", environ={
        "AQP_ROTATION_REPLY_READ_HANDLE": "11", "AQP_ROTATION_REQUEST_WRITE_HANDLE": "12"}))
    monkeypatch.setattr(RUNNER, "_inspect_supervisor_handle_metadata", lambda handles: [
        ("File", 0x00100001), ("File", 0x00100002 if valid else 0x00100003)])
    kernel = SimpleNamespace(close=mock.Mock())
    monkeypatch.setattr(RUNNER, "WindowsPipeKernel", lambda: kernel)
    monkeypatch.setattr(RUNNER, "BoundedSynchronousPipeIO", lambda actual: actual)
    monkeypatch.setattr(RUNNER, "FramedControlPipe", lambda *args, **kwargs: SimpleNamespace())
    if not valid:
        with pytest.raises(RuntimeError, match="handle identity invalid"):
            RUNNER.InheritedRotationChannel.from_environment()
        kernel.close.assert_not_called()
    else:
        channel = RUNNER.InheritedRotationChannel.from_environment()
        channel.close()
        assert kernel.close.call_args_list == [mock.call(11), mock.call(12)]


def test_default_cli_dry_run_never_constructs_connections_or_prints_credentials(tmp_path, monkeypatch, capsys):
    path = tmp_path / "candidate.json"
    path.write_text(json.dumps(config()))
    denied = mock.Mock(side_effect=AssertionError("dry-run must not establish runtime"))
    monkeypatch.setattr(RUNNER, "run_candidate", denied)
    monkeypatch.setattr(RUNNER, "create_redis_client", denied)
    assert RUNNER.main(["--config", str(path)]) == 0
    output = capsys.readouterr().out
    assert json.loads(output)["connections_created"] == 0
    assert "private-secret" not in output
    denied.assert_not_called()


def reconciliation_plan():
    return dict(request_id="4644075e811a4e2e942782ad0f2e930a", order_sys_id="1258",
                stopped_proxy_pid=11092,
                order_identity=dict(user_order_id="CO-original", stock_code="000001.SZ",
                                    action="BUY", volume=100, price=11.12))


def test_reconciliation_cli_default_is_offline_and_apply_requires_exact_preview(tmp_path, monkeypatch, capsys):
    path, plan_path = tmp_path / "config.json", tmp_path / "plan.json"
    value = config(runtime_policy="PERSISTENT_SIMULATION_READ_WRITE", allow_order_methods=True,
                   source_deadline_unix=None, max_seconds=0, max_requests=0)
    path.write_text(json.dumps(value))
    plan_path.write_text(json.dumps(reconciliation_plan()))
    denied = mock.Mock(side_effect=AssertionError("offline reconciliation dry-run must not connect"))
    monkeypatch.setattr(RUNNER, "run_reconciliation", denied)
    monkeypatch.setattr(RUNNER, "create_redis_client", denied)
    assert RUNNER.main(["--config", str(path), "--reconcile-plan", str(plan_path)]) == 0
    assert json.loads(capsys.readouterr().out)["connections_created"] == 0
    assert RUNNER.main(["--config", str(path), "--reconcile-plan", str(plan_path), "--apply-reconciliation"]) == 1
    assert json.loads(capsys.readouterr().out)["status"] == "FAILED"
    denied.assert_not_called()


def reconciliation_fixture():
    value = config(runtime_policy="PERSISTENT_SIMULATION_READ_WRITE", allow_order_methods=True,
                   native_pid=2852, source_deadline_unix=None, max_seconds=0, max_requests=0,
                   start_write_blocked=True)
    provider = PROXY_FIXTURES.FakeRedis()
    provider.connection_pool = SimpleNamespace(disconnect=mock.Mock())
    identity = reconciliation_plan()["order_identity"]
    snapshot = PROXY_FIXTURES.complete_execution_snapshot(dict(account_id="55013746",
        orders=[dict(identity, order_sys_id="1258", status="54", traded_volume=0, order_time=int(time.time()))], trades=[]),
        session_id=value["execution_session_id"])
    clients = []

    def factory(name, account):
        route = "readonly" if name == value["read_name"] else "execution"
        client = PROXY_FIXTURES.FakeRawClient(route)
        client.mailbox.session_id = value["read_session_id" if route == "readonly" else "execution_session_id"]
        client.runtime_policy = "PERSISTENT_READONLY" if route == "readonly" else value["runtime_policy"]
        client.allow_order_methods = route == "execution"
        client.result = snapshot
        clients.append(client)
        return client

    seed_read, seed_exec = factory(value["read_name"], "55013746"), factory(value["execution_name"], "55013746")
    seed = RUNNER.RedisShmReadOnlyProxy(PROXY_FIXTURES.RedisTransport(provider, account_id="55013746"),
        seed_read, seed_exec, expected_readonly_session_id=value["read_session_id"],
        expected_execution_session_id=value["execution_session_id"], expected_native_pid=2852,
        runtime_policy=value["runtime_policy"], allow_order_methods=True, max_requests=0)
    seed.verify_identity()
    seed_exec.result = dict(status="UNKNOWN", automatic_retry=False)
    original = PROXY_FIXTURES.request("submit_order", rid=reconciliation_plan()["request_id"],
        params=dict(stock_code="000001.SZ", action="BUY", volume=100, price=11.12, remark="CO-original"))
    original["expected_source_binding"] = seed.identity_snapshot()["source_binding"]
    seed.process_request(original)
    seed.close()
    clients.clear()
    return value, provider, clients, factory


def test_reconciliation_runner_only_queries_native_and_preserves_operator_block(monkeypatch):
    monkeypatch.setattr(RUNNER, "windows_session_id", lambda pid: 1)
    monkeypatch.setattr(RUNNER, "confirmed_proxy_stopped", lambda pid: pid == 11092)
    value, provider, clients, factory = reconciliation_fixture()
    before = json.dumps(provider.hashes, sort_keys=True)
    preview = RUNNER.run_reconciliation(value, reconciliation_plan(), redis_factory=lambda target: provider, shm_factory=factory)
    assert preview["status"] == "RECONCILIATION_PREVIEW" and preview["resources_closed"]
    assert json.dumps(provider.hashes, sort_keys=True) == before
    assert all(call[0]["method"] in ("ping", "query_execution_snapshot") for client in clients for call in client.calls)
    assert all(client.closed for client in clients)
    apply_plan = dict(preview["apply_plan"], operator="human-reviewed")
    result = RUNNER.run_reconciliation(value, apply_plan, apply=True,
                                      redis_factory=lambda target: provider, shm_factory=factory)
    assert result["status"] == "RECONCILED" and result["ledger_records_written"] == 1
    assert result["native_write_calls"] == 0 and result["writes_blocked"]
    assert result["write_block_reason"] == "OPERATOR_WRITE_DISABLED"
    record = json.loads(next(iter(provider.hashes.values()))[apply_plan["request_id"]])
    assert record["response"]["status"] == "UNKNOWN" and record["state"] == "RECONCILED"
    provider.connection_pool.disconnect.assert_has_calls([mock.call(), mock.call()])


def test_reconciliation_runner_rejects_foreign_session_before_any_connection(monkeypatch):
    monkeypatch.setattr(RUNNER, "windows_session_id", lambda pid: 2)
    value = config(runtime_policy="PERSISTENT_SIMULATION_READ_WRITE", allow_order_methods=True,
                   source_deadline_unix=None, max_seconds=0, max_requests=0)
    denied = mock.Mock()
    with pytest.raises(RuntimeError, match="share"):
        RUNNER.run_reconciliation(value, reconciliation_plan(), redis_factory=denied, shm_factory=denied)
    denied.assert_not_called()


def test_reconciliation_runner_rejects_live_or_unidentified_proxy_before_connections(monkeypatch):
    monkeypatch.setattr(RUNNER, "windows_session_id", lambda pid: 1)
    stopped = mock.Mock(return_value=False)
    monkeypatch.setattr(RUNNER, "confirmed_proxy_stopped", stopped)
    value = config(runtime_policy="PERSISTENT_SIMULATION_READ_WRITE", allow_order_methods=True,
                   source_deadline_unix=None, max_seconds=0, max_requests=0)
    denied = mock.Mock()
    with pytest.raises(RuntimeError, match="unique old proxy"):
        RUNNER.run_reconciliation(value, reconciliation_plan(), redis_factory=denied, shm_factory=denied)
    without_pid = reconciliation_plan()
    without_pid.pop("stopped_proxy_pid")
    with pytest.raises(ValueError, match="stopped_proxy_pid"):
        RUNNER.run_reconciliation(value, without_pid, redis_factory=denied, shm_factory=denied)
    denied.assert_not_called()


@pytest.mark.parametrize("corruption", ["session", "pid", "partial_evidence"])
def test_explicit_legacy_shm_factory_cannot_bypass_source_or_fact_validation(monkeypatch, corruption):
    monkeypatch.setattr(RUNNER, "windows_session_id", lambda pid: 1)
    monkeypatch.setattr(RUNNER, "confirmed_proxy_stopped", lambda pid: pid == 11092)
    value, provider, _, factory = reconciliation_fixture()
    before = json.dumps(provider.hashes, sort_keys=True)
    def explicit_factory(name, account):
        client = factory(name, account)
        if name == value["execution_name"]:
            if corruption == "session":
                client.mailbox.session_id = "old-or-foreign-session"
            elif corruption == "pid":
                client.mailbox.server_pid = 999
            else:
                client.result["orders_source_evidence"]["complete"] = False
        return client
    result = RUNNER.run_reconciliation(value, reconciliation_plan(),
                redis_factory=lambda target: provider, shm_factory=explicit_factory)
    assert result["status"] == "FAILED" and result["ledger_records_written"] == 0
    assert json.dumps(provider.hashes, sort_keys=True) == before


def test_source_or_relative_budget_too_short_rejects_before_factories(monkeypatch):
    monkeypatch.setattr(RUNNER, "windows_session_id", lambda pid: 1)
    factory = mock.Mock(side_effect=AssertionError("insufficient budget must not create resources"))
    for value in (config(source_deadline_unix=time.time() + 1), config(max_seconds=1)):
        with pytest.raises(RuntimeError, match="insufficient"):
            RUNNER.run_candidate(value, redis_factory=factory, shm_factory=factory)
    factory.assert_not_called()


def test_foreign_windows_session_rejects_before_factories(monkeypatch):
    monkeypatch.setattr(RUNNER, "windows_session_id", lambda pid: 2)
    factory = mock.Mock()
    with pytest.raises(RuntimeError, match="share"):
        RUNNER.run_candidate(config(), redis_factory=factory, shm_factory=factory)
    factory.assert_not_called()


def test_partial_client_creation_failure_releases_first_handle_and_owned_redis(monkeypatch):
    monkeypatch.setattr(RUNNER, "windows_session_id", lambda pid: 1)
    first = SimpleNamespace(close=mock.Mock())
    provider = SimpleNamespace(connection_pool=SimpleNamespace(disconnect=mock.Mock()))
    factory = mock.Mock(side_effect=[first, RuntimeError("mapping unavailable")])
    result = RUNNER.run_candidate(config(), redis_factory=lambda target: provider, shm_factory=factory)
    assert result["status"] == "FAILED" and result["resources_closed"]
    first.close.assert_called_once()
    provider.connection_pool.disconnect.assert_called_once()
    assert factory.call_count == 2


def test_explicit_redis_constructor_ignores_url_env_and_disables_library_retry(monkeypatch):
    if RUNNER.redis is None:
        pytest.fail("project Redis optional dependency unavailable in the existing test environment")
    monkeypatch.setenv("BIGQMT_REDIS_URL", "redis://remote.invalid/9")
    monkeypatch.setattr(socket, "socket", mock.Mock(side_effect=AssertionError("constructor must not connect")))
    client = RUNNER.create_redis_client(dict(host="127.0.0.1", port=16379, db=6))
    try:
        values = client.connection_pool.connection_kwargs
        assert (values["host"], values["port"], values["db"]) == ("127.0.0.1", 16379, 6)
        assert values["health_check_interval"] == 15
        operation = mock.Mock(side_effect=RUNNER.redis.exceptions.ConnectionError("unconfirmed"))
        with pytest.raises(RUNNER.redis.exceptions.ConnectionError):
            values["retry"].call_with_retry(operation, lambda error: None)
        operation.assert_called_once()
    finally:
        client.connection_pool.disconnect()


@pytest.mark.parametrize("target", [None, {"gateway_id": "qmt-vm-001", "profile_id": "sim-bigqmt"}])
def test_runner_processes_one_original_request_through_generated_source_and_closes(monkeypatch, target):
    source = PIPELINE.DUAL.Fixture(monkeypatch)
    source.start()
    provider = PIPELINE.MemoryRedis()
    provider.connection_pool = SimpleNamespace(disconnect=mock.Mock())
    request = dict(schema_version=1, request_id="runner-original", account_id="55013746",
                   method="get_positions", params={})
    provider.rpush("bigqmt:rpc:queue:55013746", PIPELINE.encode_rpc_request_payload(request))
    value = config(native_pid=os.getpid(), max_requests=1, timeout_seconds=1.0,
                   read_name=source.settings["read_name"], execution_name=source.settings["execution_name"],
                   read_session_id=source.clients["read"].mailbox.session_id,
                   execution_session_id=source.clients["execution"].mailbox.session_id,
                   execution_cursor_guard_target=target,
                   gateway_id="qmt-vm-001" if target else "qmt-win-001", profile_id="sim-bigqmt")
    monkeypatch.setattr(RUNNER, "windows_session_id", lambda pid: 1)
    clients = []
    ready = []

    def factory(name, account):
        key = "read" if name == value["read_name"] else "execution"
        client = source.transports[key].ShmReadOnlyClient(name, account)
        clients.append(client)
        return client

    try:
        with ThreadPoolExecutor(1) as executor:
            future = executor.submit(RUNNER.run_candidate, value, lambda target: provider, factory,
                                     ready_observer=ready.append)
            until = time.monotonic() + 3
            while not future.done() and time.monotonic() < until:
                source.namespace["aqp_shm_readonly_adjust"](source.context)
                source.namespace["aqp_shm_rpc_adjust"](source.context)
                time.sleep(0.001)
            result = future.result(timeout=0.2)
        assert result["status"] == "BOUNDED_STOP"
        assert len(ready) == 1
        assert ready[0].get("execution_cursor_guard_target") == target
        assert ("read_source_state" in ready[0]) is (target is not None)
        assert result["identity_verified"] and result["processed_requests"] == result["received_requests"] == 1
        assert result["resources_closed"] and not result["full_duration_acceptance_proved"]
        response = json.loads(provider.values["bigqmt:rpc:resp:55013746:runner-original"])
        assert response["ok"] and response["data"]["000001.SZ"]["volume"] == 100
        assert all(client.mailbox.closed for client in clients)
        assert all(subscription.closed for subscription in provider.subscriptions)
        provider.connection_pool.disconnect.assert_called_once()
    finally:
        source.close()


@pytest.mark.parametrize("policy, allow", [
    ("PERSISTENT_READONLY", False), ("PERSISTENT_SIMULATION_READ_WRITE", True),
])
def test_explicit_persistent_configuration_accepts_only_null_zero_zero_and_finite_cache(policy, allow):
    value = config(runtime_policy=policy, allow_order_methods=allow,
                   source_deadline_unix=None, max_seconds=0, max_requests=0)
    result = RUNNER.validate_config(value)
    assert result["runtime_policy"] == policy and result["allow_order_methods"] is allow
    for changes in ({"source_deadline_unix": time.time() + 60}, {"max_seconds": 1}, {"max_requests": 1},
                    {"max_requests": False}, {"read_cache_capacity": 0}, {"write_ledger_capacity": 1001},
                    {"allow_order_methods": not allow}):
        with pytest.raises(ValueError):
            RUNNER.validate_config(dict(value, **changes))


def persistent_resources(policy):
    value = config(runtime_policy=policy, allow_order_methods=policy == "PERSISTENT_SIMULATION_READ_WRITE",
                   source_deadline_unix=None, max_seconds=0, max_requests=0, native_pid=2852)
    provider = PROXY_FIXTURES.FakeRedis()
    provider.connection_pool = SimpleNamespace(disconnect=mock.Mock())
    clients = []

    def factory(name, account):
        route = "readonly" if name == value["read_name"] else "execution"
        client = PROXY_FIXTURES.FakeRawClient(route)
        client.mailbox.session_id = value["read_session_id" if route == "readonly" else "execution_session_id"]
        client.runtime_policy = "PERSISTENT_READONLY" if route == "readonly" else policy
        client.allow_order_methods = route == "execution" and value["allow_order_methods"]
        clients.append(client)
        return client

    return value, provider, clients, factory


@pytest.mark.parametrize("policy", ["PERSISTENT_READONLY", "PERSISTENT_SIMULATION_READ_WRITE"])
def test_persistent_runner_emits_actual_ready_before_consumption_and_stop_event_closes_owned_resources(monkeypatch, policy):
    value, provider, clients, factory = persistent_resources(policy)
    monkeypatch.setattr(RUNNER, "windows_session_id", lambda pid: 1)
    stop = threading.Event()
    observed = []

    def ready(proof):
        observed.append(proof)
        assert [len(client.calls) for client in clients] == [1, 1]
        assert not any(operation[0] == "lpop" for operation in provider.operations)
        assert proof["read_source"]["session_id"] == value["read_session_id"]
        assert proof["execution_source"]["session_id"] == value["execution_session_id"]
        assert proof["writes_blocked"] is value["allow_order_methods"]
        assert proof["write_ready"] is False
        stop.set()

    result = RUNNER.run_candidate(value, lambda target: provider, factory, stop_event=stop, ready_observer=ready)
    assert result["status"] == "STOP_REQUESTED" and result["identity_verified"]
    assert result["received_requests"] == result["processed_requests"] == 0
    assert observed[0]["status"] == "READY" and result["resources_closed"]
    assert all(client.closed for client in clients)
    provider.connection_pool.disconnect.assert_called_once()
    assert not any(operation[0] == "delete" for operation in provider.operations)


def test_ready_observer_failure_prevents_consumption_and_releases_owned_connections(monkeypatch):
    value, provider, clients, factory = persistent_resources("PERSISTENT_READONLY")
    monkeypatch.setattr(RUNNER, "windows_session_id", lambda pid: 1)
    ready = mock.Mock(side_effect=RuntimeError("observer unavailable"))
    result = RUNNER.run_candidate(value, lambda target: provider, factory, ready_observer=ready)
    assert result["status"] == "FAILED" and result["resources_closed"]
    assert result["error_phase"] == "READY_OBSERVER"
    assert result["error_message"] == "observer unavailable"
    assert not any(operation[0] == "lpop" for operation in provider.operations)
    assert all(client.closed for client in clients)


def test_runner_records_exact_idle_timeout_phase_message_and_limit_without_consumption(monkeypatch):
    value, provider, clients, factory = persistent_resources("PERSISTENT_READONLY")
    monkeypatch.setattr(RUNNER, "windows_session_id", lambda pid: 1)
    provider.llen = mock.Mock(side_effect=RUNNER.redis.exceptions.TimeoutError("idle private-secret connection read"))
    result = RUNNER.run_candidate(value, lambda target: provider, factory, ready_observer=lambda proof: None)
    assert result["status"] == "FAILED" and result["resources_closed"]
    assert result["error_phase"] == "REDIS_QUEUE_INSPECT" and result["error_type"] == "TimeoutError"
    assert result["stop_reason"] == "REDIS_IDLE_INSPECTION_TIMEOUT_LIMIT"
    assert result["error_message"] == "idle <redacted> connection read"
    assert result["diagnostics"]["last_error"]["error_message"] == result["error_message"]
    assert result["diagnostics"]["idle_receive_timeouts"] == 3
    assert result["received_requests"] == result["processed_requests"] == 0
    assert result["runtime_running"] is False and result["identity_verified"]
    assert not any(operation[0] == "lpop" for operation in provider.operations)
    assert [len(client.calls) for client in clients] == [1, 1]


def test_redis_factory_failure_has_phase_and_redacted_message(monkeypatch):
    monkeypatch.setattr(RUNNER, "windows_session_id", lambda pid: 1)
    provider = mock.Mock(side_effect=RUNNER.redis.exceptions.TimeoutError("private-secret connect timed out"))
    result = RUNNER.run_candidate(config(), redis_factory=provider)
    assert result["error_phase"] == "REDIS_CLIENT_CREATE"
    assert result["error_message"] == "<redacted> connect timed out"
    assert result["status"] == "FAILED" and result["resources_closed"]


def test_recoverable_response_error_is_logged_and_first_cause_is_redacted(monkeypatch, capsys):
    value, provider, clients, factory = persistent_resources("PERSISTENT_READONLY")
    value["receive_policy"] = "QUEUE_ONLY"
    stop = threading.Event()
    monkeypatch.setattr(RUNNER, "windows_session_id", lambda pid: 1)
    provider.queues["bigqmt:rpc:queue:55013746"] = [json.dumps(PROXY_FIXTURES.request())]
    def lost_reply(*args):
        stop.set()
        raise RUNNER.redis.exceptions.TimeoutError("private-secret response lost")
    provider.setex = lost_reply
    result = RUNNER.run_candidate(value, lambda target: provider, factory,
                                  stop_event=stop, ready_observer=lambda proof: None)
    assert result["status"] == "STOP_REQUESTED" and result["resources_closed"]
    assert result["diagnostics"]["read_publish_failures"] == 1
    assert result["diagnostics"]["first_error"]["error_message"] == "<redacted> response lost"
    logged = capsys.readouterr().out
    assert '"event": "PROXY_TRANSPORT_ERROR"' in logged and "private-secret" not in logged
    assert [len(client.calls) for client in clients] == [1, 2]


def test_confirmed_runner_seal_also_preserves_native_timeout_phase(monkeypatch):
    value, provider, clients, factory = persistent_resources("PERSISTENT_READONLY")
    monkeypatch.setattr(RUNNER, "windows_session_id", lambda pid: 1)
    provider.queues["bigqmt:rpc:queue:55013746"] = [json.dumps(PROXY_FIXTURES.request())]
    def ready(proof):
        clients[1].failure = PROXY_FIXTURES.ShmTimeoutError("native result private-secret unknown")
    result = RUNNER.run_candidate(value, lambda target: provider, factory, ready_observer=ready)
    assert result["status"] == "PROXY_STOPPED" and result["resources_closed"]
    assert result["stop_reason"] == "SHM_RESULT_UNCONFIRMED"
    assert result["diagnostics"]["last_error"]["error_phase"] == "SHM_FORWARD"
    assert result["diagnostics"]["last_error"]["error_message"] == "native result <redacted> unknown"


def test_persistent_keyboard_interrupt_releases_resources_and_does_not_clear_redis_objects(monkeypatch):
    value, provider, clients, factory = persistent_resources("PERSISTENT_READONLY")
    monkeypatch.setattr(RUNNER, "windows_session_id", lambda pid: 1)
    provider.subscription.get_message = mock.Mock(side_effect=KeyboardInterrupt())
    result = RUNNER.run_candidate(value, lambda target: provider, factory, ready_observer=lambda evidence: None)
    assert result["status"] == "STOP_REQUESTED" and result["stop_reason"] == "KEYBOARD_INTERRUPT"
    assert result["resources_closed"] and all(client.closed for client in clients)
    assert provider.subscription.closed
    assert not any(operation[0] == "delete" for operation in provider.operations)


def test_persistent_source_permission_mismatch_never_emits_ready_or_consumes_business(monkeypatch):
    value, provider, clients, base_factory = persistent_resources("PERSISTENT_SIMULATION_READ_WRITE")
    monkeypatch.setattr(RUNNER, "windows_session_id", lambda pid: 1)
    ready = mock.Mock()

    def factory(name, account):
        client = base_factory(name, account)
        client.allow_order_methods = False
        return client

    result = RUNNER.run_candidate(value, lambda target: provider, factory, ready_observer=ready)
    assert result["status"] == "FAILED" and result["resources_closed"]
    ready.assert_not_called()
    assert not any(operation[0] == "lpop" for operation in provider.operations)


def test_persistent_cli_default_remains_inert_even_with_explicit_write_capability(tmp_path, monkeypatch, capsys):
    value, _, _, _ = persistent_resources("PERSISTENT_SIMULATION_READ_WRITE")
    path = tmp_path / "persistent-simulation.json"
    path.write_text(json.dumps(value))
    run = mock.Mock(side_effect=AssertionError("dry-run cannot launch"))
    monkeypatch.setattr(RUNNER, "run_candidate", run)
    assert RUNNER.main(["--config", str(path)]) == 0
    proof = json.loads(capsys.readouterr().out)
    assert proof["connections_created"] == 0 and proof["allow_order_methods"] is True
    assert proof["runtime_policy"] == "PERSISTENT_SIMULATION_READ_WRITE"
    assert proof["start_write_blocked"] is True
    run.assert_not_called()


@pytest.mark.parametrize("invalid", [None, 0, 1, "false", "true", [], {}])
def test_start_write_blocked_requires_strict_bool_before_any_connection(monkeypatch, invalid):
    value, _, _, _ = persistent_resources("PERSISTENT_SIMULATION_READ_WRITE")
    value["start_write_blocked"] = invalid
    factory = mock.Mock(side_effect=AssertionError("invalid configuration cannot connect"))
    with pytest.raises(ValueError, match="start_write_blocked"):
        RUNNER.run_candidate(value, factory, factory)
    factory.assert_not_called()


def test_start_write_blocked_defaults_follow_policy_without_changing_capability_contract():
    assert RUNNER.validate_config(config())["start_write_blocked"] is False
    for policy in ("PERSISTENT_READONLY", "PERSISTENT_SIMULATION_READ_WRITE"):
        value, _, _, _ = persistent_resources(policy)
        parsed = RUNNER.validate_config(value)
        assert parsed["start_write_blocked"] is (policy == "PERSISTENT_SIMULATION_READ_WRITE")
        assert parsed["allow_order_methods"] is value["allow_order_methods"]


def test_default_readwrite_gate_blocks_submit_cancel_before_ledger_and_native_but_reads_continue(monkeypatch):
    value, provider, clients, factory = persistent_resources("PERSISTENT_SIMULATION_READ_WRITE")
    monkeypatch.setattr(RUNNER, "windows_session_id", lambda pid: 1)
    stop = threading.Event()
    binding = dict(read_session_id=value["read_session_id"], execution_session_id=value["execution_session_id"],
                   native_pid=value["native_pid"], windows_session_id=value["windows_session"])
    requests = []
    for index, method in enumerate(("get_asset", "get_positions", "submit_order", "cancel_order", "query_orders", "get_positions")):
        message = PROXY_FIXTURES.request(method, rid="operator-gate-%d" % index)
        message["expected_source_binding"] = binding
        requests.append(json.dumps(message))
    provider.queues["bigqmt:rpc:queue:55013746"] = requests
    original_pop = provider.lpop

    def pop_and_stop_after_last(queue):
        result = original_pop(queue)
        if not provider.queues[queue]:
            stop.set()
        return result

    provider.lpop = pop_and_stop_after_last

    def ready(proof):
        assert proof["writes_blocked"] is True and proof["write_ready"] is False
        assert proof["write_block_reason"] == "OPERATOR_WRITE_DISABLED"
        assert proof["allow_order_methods"] is True
        assert proof["execution_source"]["allow_order_methods"] is True
        assert {"submit_order", "cancel_order"} <= set(proof["capabilities"])
        assert [len(client.calls) for client in clients] == [1, 1]

    result = RUNNER.run_candidate(value, lambda target: provider, factory, stop_event=stop, ready_observer=ready)
    assert result["status"] == "STOP_REQUESTED" and result["resources_closed"]
    assert result["received_requests"] == 6
    assert result["writes_blocked"] is True and result["write_ready"] is False
    assert result["write_block_reason"] == "OPERATOR_WRITE_DISABLED"
    assert [call[0]["method"] for call in clients[1].calls] == [
        "ping", "get_asset", "get_positions", "query_orders", "get_positions"]
    assert not any(operation[0] == "eval" for operation in provider.operations)
    assert provider.hashes == {}
    published = [json.loads(operation[3]) for operation in provider.operations if operation[0] == "setex"]
    rejected = [response for response in published if response["method"] in ("submit_order", "cancel_order")]
    assert len(rejected) == 2 and all(response["status"] == "REJECTED" for response in rejected)
    assert all(response["ok"] for response in published if response["method"] not in ("submit_order", "cancel_order"))


def test_explicit_false_preserves_authorized_local_readwrite_ready_capability(monkeypatch):
    value, provider, clients, factory = persistent_resources("PERSISTENT_SIMULATION_READ_WRITE")
    value["start_write_blocked"] = False
    monkeypatch.setattr(RUNNER, "windows_session_id", lambda pid: 1)
    stop = threading.Event()

    def ready(proof):
        assert proof["write_ready"] is True and proof["writes_blocked"] is False
        assert proof["allow_order_methods"] is True
        assert {"submit_order", "cancel_order"} <= set(proof["capabilities"])
        stop.set()

    result = RUNNER.run_candidate(value, lambda target: provider, factory, stop_event=stop, ready_observer=ready)
    assert result["status"] == "STOP_REQUESTED" and result["resources_closed"]
    assert result["write_ready"] is True and result["writes_blocked"] is False
