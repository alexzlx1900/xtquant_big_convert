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


def test_runner_processes_one_original_request_through_generated_source_and_closes(monkeypatch):
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
                   execution_session_id=source.clients["execution"].mailbox.session_id)
    monkeypatch.setattr(RUNNER, "windows_session_id", lambda pid: 1)
    clients = []

    def factory(name, account):
        key = "read" if name == value["read_name"] else "execution"
        client = source.transports[key].ShmReadOnlyClient(name, account)
        clients.append(client)
        return client

    try:
        with ThreadPoolExecutor(1) as executor:
            future = executor.submit(RUNNER.run_candidate, value, lambda target: provider, factory)
            until = time.monotonic() + 3
            while not future.done() and time.monotonic() < until:
                source.namespace["aqp_shm_readonly_adjust"](source.context)
                source.namespace["aqp_shm_rpc_adjust"](source.context)
                time.sleep(0.001)
            result = future.result(timeout=0.2)
        assert result["status"] == "BOUNDED_STOP"
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
