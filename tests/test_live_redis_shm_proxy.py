# coding: utf-8
"""实盘候选合同；只使用内存Redis/SHM，不接触实机或共享服务。"""

import copy
import importlib.util
import json
from pathlib import Path
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


FIXTURE = load_file("live_proxy_fixtures", "tests/bigqmt_signal_trader/test_redis_shm_proxy.py")
RUNNER = load_file("live_proxy_runner", "tools/run_external_redis_shm_proxy.py")
ACCOUNT = "8882152350"
POLICY = "PERSISTENT_LIVE_READ_WRITE"
LEDGER = "aqp:redis_shm:write_ledger:%s:v1" % ACCOUNT


class LiveClient(FIXTURE.FakeRawClient):
    def __init__(self, route):
        super().__init__(route)
        self.account_id = ACCOUNT
        self.environment = "LIVE"
        self.runtime_policy = POLICY if route == "execution" else "PERSISTENT_READONLY"
        self.allow_order_methods = route == "execution"
        self.mutate = None

    def send_request(self, request, timeout_seconds):
        assert request["environment"] == "LIVE"
        response = super().send_request(request, timeout_seconds)
        response.update(account_id=self.account_id, environment=self.environment)
        if request["method"] == "ping":
            response["data"].update(account_id=self.account_id, environment=self.environment)
        elif request["method"] == "get_asset":
            response["data"]["source_evidence"]["environment"] = self.environment
        if self.mutate is not None:
            self.mutate(request, response)
        return response


def make_proxy(*, verify=True, provider=None, client_mutator=None, **changes):
    provider = provider or FIXTURE.FakeRedis()
    read, execution = LiveClient("readonly"), LiveClient("execution")
    if client_mutator:
        client_mutator(read, execution)
    kwargs = dict(account_id=ACCOUNT, environment="LIVE", runtime_policy=POLICY,
                  allow_order_methods=True, start_write_blocked=True,
                  expected_readonly_session_id="readonly-sid", expected_execution_session_id="execution-sid",
                  expected_native_pid=2852, expected_windows_session_id=1, max_requests=0)
    kwargs.update(changes)
    proxy = RUNNER.RedisShmReadOnlyProxy(FIXTURE.RedisTransport(provider, account_id=ACCOUNT), read, execution, **kwargs)
    if verify:
        proxy.verify_identity()
    return proxy, provider, read, execution


def request(method="get_positions", rid="live-original", **changes):
    value = FIXTURE.request(method, rid)
    value.update(account_id=ACCOUNT, environment="LIVE")
    for key in ("reply_key", "reply_list", "reply_channel"):
        value[key] = value[key].replace(FIXTURE.ACCOUNT, ACCOUNT)
    value.update(changes)
    return value


def config(**changes):
    value = dict(account_id=ACCOUNT, environment="LIVE", runtime_policy=POLICY,
                 allow_order_methods=True, start_write_blocked=True, native_pid=2852, windows_session=1,
                 read_name="Local\\AQP_SHM_READONLY_LIVE_FIXTURE", execution_name="Local\\AQP_SHM_TRADE_LIVE_FIXTURE",
                 read_session_id="1" * 32, execution_session_id="2" * 32,
                 source_deadline_unix=None, max_seconds=0, max_requests=0, timeout_seconds=1,
                 redis=dict(host="127.0.0.1", port=6379, db=6, password="fixture-secret"))
    value.update(changes)
    return value


@pytest.mark.parametrize("changes", [
    dict(account_id="55013746"), dict(account_id=8882152350), dict(account_id="8882152350 "),
    dict(account_id="account"), dict(account_id=""), dict(account_id="８８８２１５２３５０"),
    dict(environment="SIM"), dict(environment="live"), dict(environment=None),
    dict(runtime_policy="PERSISTENT_SIMULATION_READ_WRITE"), dict(runtime_policy="PERSISTENT_READONLY"),
    dict(allow_order_methods=False), dict(start_write_blocked=False), dict(start_write_blocked=1),
    dict(max_requests=1), dict(max_seconds=30), dict(source_deadline_unix=time.time() + 60),
])
def test_live_configuration_requires_explicit_environment_account_policy_and_initial_block(changes):
    with pytest.raises(ValueError):
        RUNNER.validate_config(config(**changes))


@pytest.mark.parametrize("field", ["environment", "start_write_blocked", "runtime_policy"])
def test_live_configuration_missing_permission_fields_is_rejected_offline(field):
    value = config()
    value.pop(field)
    with pytest.raises(ValueError):
        RUNNER.validate_config(value)


def test_live_proxy_constructor_cannot_omit_or_disable_initial_write_block():
    for blocked in (False, None, 1):
        with pytest.raises(ValueError, match="start_write_blocked"):
            make_proxy(start_write_blocked=blocked)
    with pytest.raises(ValueError, match="expected Windows session"):
        make_proxy(expected_windows_session_id=None)


def test_live_identity_and_namespace_are_account_scoped_and_start_blocked():
    provider = FIXTURE.FakeRedis()
    provider.hashes["aqp:redis_shm:write_ledger:55013746:v1"] = {"untouched": "invalid SIM fixture"}
    proxy, provider, read, execution = make_proxy(provider=provider)
    proof = proxy.identity_snapshot()
    assert proof["environment"] == proof["read_source"]["environment"] == proof["execution_source"]["environment"] == "LIVE"
    assert proof["writes_blocked"] is True and proof["write_ready"] is False
    assert proof["write_block_reason"] == "OPERATOR_WRITE_DISABLED"
    assert proof["runtime_policy"] == POLICY
    assert proxy.transport.request_queue == "bigqmt:rpc:queue:" + ACCOUNT
    assert proxy.transport.request_channel == "bigqmt:rpc:req:" + ACCOUNT
    assert provider.operations == [("hgetall", LEDGER)]
    assert proxy._write_binding()["environment"] == "LIVE"
    assert len(read.calls) == len(execution.calls) == 1


@pytest.mark.parametrize("route", ["readonly", "execution"])
@pytest.mark.parametrize("mutation", ["envelope_missing", "envelope_sim", "source_missing", "source_sim", "policy", "sid", "pid", "session"])
def test_live_handshake_rejects_missing_environment_and_source_identity_mismatch(route, mutation):
    def mutate(read, execution):
        client = read if route == "readonly" else execution

        def change(_, response):
            if mutation == "envelope_missing":
                response.pop("environment")
            elif mutation == "envelope_sim":
                response["environment"] = "SIM"
            elif mutation == "source_missing":
                response["data"].pop("environment")
            elif mutation == "source_sim":
                response["data"]["environment"] = "SIM"
            else:
                response["data"][dict(policy="runtime_policy", sid="session_id", pid="native_pid", session="windows_session_id")[mutation]] = "wrong"
        client.mutate = change

    with pytest.raises(FIXTURE.ShmProtocolError):
        make_proxy(client_mutator=mutate)


@pytest.mark.parametrize("environment", [None, "SIM", "live"])
def test_live_business_request_requires_environment_before_native_call(environment):
    proxy, _, _, execution = make_proxy()
    value = request(environment=environment)
    if environment is None:
        value.pop("environment")
    response = proxy.process_request(value)
    assert response["ok"] is False and response["environment"] == "LIVE"
    assert len(execution.calls) == 1


def test_live_read_and_confirmed_cache_preserve_environment_without_repeat_native_call():
    proxy, _, _, execution = make_proxy()
    value = request()
    response = proxy.process_request(value)
    assert response["ok"] is True and response["environment"] == "LIVE"
    assert execution.calls[-1][0]["environment"] == "LIVE"
    assert proxy.process_request(value) == response == proxy.get_cached_response(value["request_id"])
    assert len(execution.calls) == 2


@pytest.mark.parametrize("mutation", ["envelope_missing", "envelope_sim", "source_missing", "source_sim"])
def test_live_business_response_mixed_environment_seals_proxy_without_resend(mutation):
    proxy, _, _, execution = make_proxy()

    def change(_, response):
        if mutation == "envelope_missing":
            response.pop("environment")
        elif mutation == "envelope_sim":
            response["environment"] = "SIM"
        elif mutation == "source_missing":
            response["data"]["source_evidence"].pop("environment")
        else:
            response["data"]["source_evidence"]["environment"] = "SIM"
    execution.mutate = change
    value = request("get_asset")
    response = proxy.process_request(value)
    assert response["status"] == "UNKNOWN" and response["automatic_retry"] is False
    assert proxy.stopped and len(execution.calls) == 2
    with pytest.raises(FIXTURE.ShmProtocolError, match="do not resend"):
        proxy.process_request(value)
    assert len(execution.calls) == 2


def test_live_initial_block_rejects_new_write_without_claim_or_native_write():
    proxy, provider, _, execution = make_proxy()
    response = proxy.process_request(request("submit_order"))
    assert response["ok"] is False and "blocked" in response["error"]
    assert len(execution.calls) == 1
    assert not any(op[0] == "eval" for op in provider.operations)


def test_live_unknown_survives_restart_and_duplicate_never_calls_native():
    proxy, provider, _, _ = make_proxy()
    value = request("submit_order")
    fingerprint = proxy._validate_request(value)
    original = proxy._response(value, "fixture unknown", "SHM_RESULT_UNCONFIRMED", unknown=True)
    record = dict(request_id=value["request_id"], fingerprint=fingerprint, binding=proxy._write_binding(),
                  state="UNKNOWN", token="fixture-token", claimed_at_unix=time.time(),
                  response=original, automatic_retry=False)
    provider.hashes[LEDGER] = {value["request_id"]: json.dumps(record)}
    restarted, _, _, execution = make_proxy(provider=provider)
    response = restarted.process_request(value)
    assert response["status"] == "UNKNOWN" and response["automatic_retry"] is False
    assert restarted.identity_snapshot()["writes_blocked"] is True
    assert len(execution.calls) == 1
    assert json.loads(provider.hashes[LEDGER][value["request_id"]]) == record


def test_live_reconciliation_preserves_operator_write_block_after_cas_and_restart():
    proxy, provider, _, execution = make_proxy()
    identity = dict(user_order_id="live-fixture-original", stock_code="000001.SZ", action="BUY", volume=100, price=10.0)
    params = dict(stock_code=identity["stock_code"], action="BUY", volume=100, price=10.0, remark=identity["user_order_id"])
    value = request("submit_order", params=params)
    original = proxy._response(value, "fixture unknown", "SHM_RESULT_UNCONFIRMED", unknown=True)
    record = dict(request_id=value["request_id"], fingerprint=proxy._validate_request(value), binding=proxy._write_binding(),
                  state="UNKNOWN", token="fixture-token", claimed_at_unix=time.time() - 1,
                  response=original, automatic_retry=False)
    provider.hashes[LEDGER] = {value["request_id"]: json.dumps(record)}
    snapshot = FIXTURE.complete_execution_snapshot(dict(account_id=ACCOUNT,
        orders=[dict(identity, order_sys_id="LIVE_FIXTURE_CONTRACT", status="54", traded_volume=0, order_time=int(time.time()))], trades=[]))
    for key in ("source_evidence", "orders_source_evidence", "trades_source_evidence"):
        snapshot[key].update(account_id=ACCOUNT, environment="LIVE")
    execution.result = snapshot
    preview = proxy.reconcile_unknown_write(value["request_id"], "LIVE_FIXTURE_CONTRACT", identity)
    assert preview["status"] == "RECONCILIATION_PREVIEW" and preview["ledger_records_written"] == 0
    result = proxy.reconcile_unknown_write(value["request_id"], "LIVE_FIXTURE_CONTRACT", identity,
        expected_record=preview["expected_record"], operator="offline fixture", apply=True)
    assert result["status"] == "RECONCILED" and result["native_write_calls"] == 0
    assert result["writes_blocked"] is True and proxy.identity_snapshot()["write_ready"] is False
    saved = json.loads(provider.hashes[LEDGER][value["request_id"]])
    assert saved["response"] == original and saved["binding"]["environment"] == "LIVE"
    restarted, _, _, fresh = make_proxy(provider=provider)
    assert restarted.identity_snapshot()["writes_blocked"] is True
    response = restarted.process_request(value)
    assert response["status"] == "RECONCILED" and response["environment"] == "LIVE"
    assert len(fresh.calls) == 1


def test_live_ledger_rejects_account_or_environment_substitution():
    proxy, provider, _, _ = make_proxy()
    for field, value in (("account_id", "55013746"), ("environment", "SIM"), ("runtime_policy", "PERSISTENT_SIMULATION_READ_WRITE")):
        binding = proxy._write_binding()
        binding[field] = value
        provider.hashes[LEDGER] = {"old": json.dumps(dict(request_id="old", fingerprint="fixture", binding=binding, state="CONFIRMED"))}
        with pytest.raises(FIXTURE.ShmProtocolError, match="ledger identity"):
            make_proxy(provider=provider)


def test_live_cli_dry_run_has_no_connections_and_does_not_print_credentials(tmp_path, monkeypatch, capsys):
    path = tmp_path / "live.json"
    path.write_text(json.dumps(config()))
    deny = mock.Mock(side_effect=AssertionError("offline preparation must not connect"))
    monkeypatch.setattr(RUNNER, "run_candidate", deny)
    monkeypatch.setattr(RUNNER, "create_redis_client", deny)
    assert RUNNER.main(["--config", str(path)]) == 0
    output = capsys.readouterr().out
    result = json.loads(output)
    assert result["account_id"] == ACCOUNT and result["environment"] == "LIVE"
    assert result["connections_created"] == 0 and result["start_write_blocked"] is True
    assert "fixture-secret" not in output
    deny.assert_not_called()


def test_live_runner_propagates_exact_account_and_initial_block_to_injected_resources(monkeypatch):
    value = config()
    provider = FIXTURE.FakeRedis()
    provider.connection_pool = SimpleNamespace(disconnect=mock.Mock())
    clients, seen = [], []
    stop = threading.Event()

    def factory(name, account):
        assert account == ACCOUNT
        route = "readonly" if name == value["read_name"] else "execution"
        client = LiveClient(route)
        client.mailbox.session_id = value["read_session_id" if route == "readonly" else "execution_session_id"]
        clients.append(client)
        return client

    def observe(proof):
        seen.append(copy.deepcopy(proof))
        stop.set()

    monkeypatch.setattr(RUNNER, "windows_session_id", lambda _: 1)
    result = RUNNER.run_candidate(value, redis_factory=lambda _: provider, shm_factory=factory,
                                  stop_event=stop, ready_observer=observe)
    assert result["status"] == "STOP_REQUESTED" and result["account_id"] == ACCOUNT
    assert result["environment"] == "LIVE" and result["writes_blocked"] is True
    assert seen[0]["write_ready"] is False and seen[0]["writes_blocked"] is True
    assert result["processed_requests"] == 0
    assert all(client.closed and len(client.calls) == 1 for client in clients)
    provider.connection_pool.disconnect.assert_called_once()
