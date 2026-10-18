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
        assert proof["write_ready"] is value["allow_order_methods"]
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
    assert not any(operation[0] == "lpop" for operation in provider.operations)
    assert all(client.closed for client in clients)


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
    run.assert_not_called()
