# coding: utf-8
"""实盘候选合同；只使用内存Redis/SHM，不接触实机或共享服务。"""

import copy
from concurrent.futures import ThreadPoolExecutor
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from types import SimpleNamespace
from unittest import mock

import pytest
import redis

from test_external_redis_dual_shm_integration import DUAL
from bigqmt_signal_trader.transports.readonly_shm import ShmReadOnlyClient


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

    def send_request(self, request, timeout_seconds, *, after_publication=None):
        assert request["environment"] == "LIVE"
        response = super().send_request(request, timeout_seconds, after_publication=after_publication)
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
                  expected_native_pid=2852, expected_windows_session_id=1, max_requests=0,
                  execution_cursor_guard_target={"gateway_id": "qmt-vm-001", "profile_id": "live-stock"},
                  supervisor_write_gate=FIXTURE.FakeSupervisorWriteGate())
    kwargs.update(changes)
    proxy = RUNNER.RedisShmReadOnlyProxy(FIXTURE.RedisTransport(provider, account_id=ACCOUNT), read, execution, **kwargs)
    if verify:
        # 单元模型模拟预先通过的 ATTACH；正式 runner 不注入此状态。
        proxy._supervisor_attach_verified = True
        proxy.verify_identity()
    return proxy, provider, read, execution


def request(method="get_positions", rid="live-original", **changes):
    value = FIXTURE.request(method, rid)
    value.update(account_id=ACCOUNT, environment="LIVE")
    for key in ("reply_key", "reply_list", "reply_channel"):
        value[key] = value[key].replace(FIXTURE.ACCOUNT, ACCOUNT)
    value.update(changes)
    if method in FIXTURE.ALL_WRITE_METHODS:
        value["params"] = dict(value["params"])
        if method == "submit_order":
            value["params"].setdefault("remark", rid)
        elif method == "cancel_order":
            value["params"].setdefault("order_sys_id", rid)
        value["params"].setdefault("execution_event_guard", dict(contract="EXECUTION_CURSOR_WRITE_GATE_V1",
                                   execution_session_id="execution-sid", native_pid=2852, sequence=0))
        value["params"].setdefault("supervisor_gate_epoch", dict(contract="SUPERVISOR_GATE_EPOCH_V1", epoch=1))
    return value


class SelectiveReadRedis(FIXTURE.FakeRedis):
    """为内存测试提供与真实 Redis 队列相同的原子选择语义。"""

    def lrange(self, queue, start, end):
        self._record("lrange", queue, start, end)
        return self.queues.get(queue, [])[start:end + 1]

    def eval(self, script, key_count, *args):
        if "TAKE_VERIFIED_READ_V1" not in script:
            return super().eval(script, key_count, *args)
        assert key_count == 2
        queue, ledger, index, raw, rid = args
        self._record("select_read", queue, index, rid)
        values = self.queues.get(queue, [])
        if rid in self.hashes.get(ledger, {}) or index >= len(values) or values[index] != raw:
            return 0
        values.remove(raw)
        return 1


def pending_identity(proxy):
    gate_error = sys.modules[RUNNER.RedisShmReadOnlyProxy.__module__].SupervisorWriteGateError
    proxy._supervisor_write_gate.check = mock.Mock(side_effect=gate_error(
        "SUPERVISOR_IDENTITY_NOT_READY", permanent=False))
    proxy._check_native_process_identity = mock.Mock()
    proxy._verified_process_identity = {"native_creation_filetime": 789}


def config(**changes):
    value = dict(account_id=ACCOUNT, environment="LIVE", runtime_policy=POLICY,
                 allow_order_methods=True, start_write_blocked=True, native_pid=2852, windows_session=1,
                 read_name="Local\\AQP_SHM_READONLY_LIVE_FIXTURE", execution_name="Local\\AQP_SHM_TRADE_LIVE_FIXTURE",
                 read_session_id="1" * 32, execution_session_id="2" * 32,
                 source_deadline_unix=None, max_seconds=0, max_requests=0, timeout_seconds=1,
                 redis=dict(host="127.0.0.1", port=6379, db=6, password="fixture-secret"))
    value.update(changes)
    return value


def test_legacy_vm_captures_native_process_identity_for_late_ping_recovery(monkeypatch):
    native = dict(native_pid=2852, native_creation_filetime=789, windows_session_id=1, executable="qmt.exe")
    proxy_module = sys.modules[RUNNER.RedisShmReadOnlyProxy.__module__]
    monkeypatch.setattr(proxy_module, "_windows_native_process_identity", lambda _pid: dict(native))

    def install(read, execution):
        read.mutate = execution.mutate = lambda _request, response: response["data"].update(
            native_creation_filetime=789)

    proxy, _, _, _ = make_proxy(client_mutator=install, execution_cursor_guard_target=None,
        supervisor_write_gate=None, previous_source_unknown_target={"gateway_id": "qmt-vm-001", "profile_id": "live-stock"},
        receive_policy="QUEUE_ONLY", start_write_blocked=False, live_write_authorized=True)
    assert proxy._verified_process_identity == native
    proxy._check_native_process_identity()


def test_legacy_vm_waits_for_exact_late_ping_then_resumes_new_requests(isolated_redis):
    proxy, provider, _, execution = make_proxy(provider=isolated_redis, execution_cursor_guard_target=None, supervisor_write_gate=None,
        previous_source_unknown_target={"gateway_id": "qmt-vm-001", "profile_id": "live-stock"},
        receive_policy="QUEUE_ONLY", start_write_blocked=False, live_write_authorized=True)
    original_send = execution.send_request
    calls = []

    def timed_out_ping(value, timeout_seconds, **kwargs):
        if value["method"] != "ping":
            return original_send(value, timeout_seconds, **kwargs)
        calls.append(value["request_id"])
        execution.poisoned = True
        error = FIXTURE.ShmTimeoutError("response timed out")
        error.publication_state, error.previous_unconfirmed = "UNKNOWN", False
        error.request_id, error.session_id, error.sequence = value["request_id"], "execution-sid", 2
        raise error

    execution.send_request = timed_out_ping
    proxy._check_native_process_identity = mock.Mock()
    pending = request("ping", rid="late-ping")
    result = proxy.process_request(pending)
    assert result["status"] == "UNKNOWN" and not proxy.isolation_reason and calls == ["late-ping"]
    next_request = json.dumps(request("get_positions", rid="after-ping"))
    provider.rpush(proxy.transport.request_queue, next_request)
    execution.recover_completed_request = mock.Mock(side_effect=[False, True])
    blocked = proxy.poll_once()
    assert blocked["request_id"] == "after-ping" and blocked["publication_state"] == "NOT_SENT"
    assert provider.lrange(proxy.transport.request_queue, 0, -1) == []
    execution.poisoned = False
    assert proxy.poll_once() is None
    provider.rpush(proxy.transport.request_queue, json.dumps(request("get_positions", rid="new-after-ping")))
    assert proxy.poll_once()["request_id"] == "new-after-ping"
    assert calls == ["late-ping"] and execution.recover_completed_request.call_count == 2
    assert proxy.get_cached_response("late-ping") == result


def test_vm_late_read_keeps_existing_unknown_write_and_block(isolated_redis):
    proxy, provider, _, execution = make_proxy(provider=isolated_redis, execution_cursor_guard_target=None,
        supervisor_write_gate=None, previous_source_unknown_target={"gateway_id": "qmt-vm-001", "profile_id": "live-stock"},
        receive_policy="QUEUE_ONLY", start_write_blocked=False, live_write_authorized=True)
    old_record = json.dumps({"state": "UNKNOWN", "request_id": "old-unknown-write"})
    provider.hset(LEDGER, "old-unknown-write", old_record)
    proxy._block_writes("NATIVE_WRITE_RESULT_UNKNOWN")

    def timeout(value, timeout_seconds, **kwargs):
        execution.poisoned = True
        error = FIXTURE.ShmTimeoutError("response timed out")
        error.publication_state, error.previous_unconfirmed = "UNKNOWN", False
        error.request_id, error.session_id, error.sequence = value["request_id"], "execution-sid", 2
        raise error

    execution.send_request = timeout
    proxy._check_native_process_identity = mock.Mock()
    original = request("get_positions", rid="late-with-old-write")
    assert proxy.process_request(original)["status"] == "UNKNOWN"

    def confirm(*args):
        execution.poisoned = False
        return True

    execution.recover_completed_request = mock.Mock(side_effect=confirm)
    assert proxy.poll_once() is None and not proxy._pending_request_recoveries
    assert not proxy._writes_blocked and "NATIVE_WRITE_RESULT_UNKNOWN" in proxy._write_block_reasons
    assert provider.hget(LEDGER, "old-unknown-write") == old_record
    assert proxy.get_cached_response(original["request_id"])["status"] == "UNKNOWN"


def test_vm_late_read_never_acknowledges_changed_native_identity(isolated_redis):
    proxy, _, _, execution = make_proxy(provider=isolated_redis, execution_cursor_guard_target=None,
        supervisor_write_gate=None, previous_source_unknown_target={"gateway_id": "qmt-vm-001", "profile_id": "live-stock"},
        receive_policy="QUEUE_ONLY", start_write_blocked=False, live_write_authorized=True)

    def timeout(value, timeout_seconds, **kwargs):
        execution.poisoned = True
        error = FIXTURE.ShmTimeoutError("response timed out")
        error.publication_state, error.previous_unconfirmed = "UNKNOWN", False
        error.request_id, error.session_id, error.sequence = value["request_id"], "execution-sid", 2
        raise error

    execution.send_request = timeout
    execution.recover_completed_request = mock.Mock()
    original = request("get_positions", rid="late-changed-native")
    assert proxy.process_request(original)["status"] == "UNKNOWN"
    proxy._check_native_process_identity = mock.Mock(side_effect=FIXTURE.ShmProtocolError("native identity changed"))
    assert proxy.poll_once() is None and not proxy.isolation_reason
    execution.recover_completed_request.assert_not_called()
    assert execution.poisoned and proxy.get_cached_response(original["request_id"])["status"] == "UNKNOWN"


def test_legacy_live_runner_stays_up_after_quote_timeout_and_rejects_later_requests(monkeypatch):
    value = config(receive_policy="QUEUE_ONLY", start_write_blocked=False, live_write_authorized=True)
    provider = FIXTURE.FakeRedis()
    provider.connection_pool = SimpleNamespace(disconnect=mock.Mock())
    clients = []
    original = request("get_ticks", rid="runner-read", params={"code": "000001.SZ"})
    subsequent = request("get_positions", rid="runner-position")
    provider.queues["bigqmt:rpc:queue:" + ACCOUNT] = [json.dumps(original), json.dumps(subsequent)]
    stop = threading.Event()
    monkeypatch.setattr(RUNNER, "windows_session_id", lambda pid: 1)

    def factory(name, account):
        assert account == ACCOUNT
        route = "readonly" if name == value["read_name"] else "execution"
        client = LiveClient(route)
        client.mailbox.session_id = value["read_session_id" if route == "readonly" else "execution_session_id"]
        clients.append(client)
        return client

    def ready(proof):
        assert proof["status"] == "READY"
        clients[0].failure = FIXTURE.ShmTimeoutError("read result unconfirmed")

    with ThreadPoolExecutor(1) as pool:
        future = pool.submit(RUNNER.run_candidate, value, lambda target: provider, factory,
                             stop_event=stop, ready_observer=ready)
        try:
            deadline = time.monotonic() + 3
            while len([item for item in provider.operations if item[0] == "setex"]) < 2 and time.monotonic() < deadline:
                time.sleep(0.01)
            responses = [json.loads(item[3]) for item in provider.operations if item[0] == "setex"]
            assert len(responses) == 2 and not future.done()
            assert responses[0]["status"] == "UNKNOWN" and responses[0]["automatic_retry"] is False
            assert responses[1]["ok"] is True
            assert len(clients[0].calls) == 2 and len(clients[1].calls) == 2
        finally:
            stop.set()
        result = future.result(timeout=3)
    assert result["status"] == "STOP_REQUESTED" and result["resources_closed"]
    assert result["writes_blocked"] is False and result["write_ready"] is True


def test_runner_reports_read_recovery_transitions_without_waiting_for_periodic_health(monkeypatch, capsys):
    value = config(receive_policy="QUEUE_ONLY", start_write_blocked=False, live_write_authorized=True)
    provider = FIXTURE.FakeRedis()
    provider.connection_pool = SimpleNamespace(disconnect=mock.Mock())
    stop = threading.Event()
    monkeypatch.setattr(RUNNER, "windows_session_id", lambda pid: 1)

    def factory(name, account):
        route = "readonly" if name == value["read_name"] else "execution"
        client = LiveClient(route)
        client.mailbox.session_id = value["read_session_id" if route == "readonly" else "execution_session_id"]
        return client

    calls = []

    def transition(proxy):
        calls.append(1)
        proxy._pending_execution_read_recovery = {"request": {"method": "get_positions"}} if len(calls) == 1 else None
        if len(calls) == 2:
            stop.set()
        return None

    monkeypatch.setattr(RUNNER.RedisShmReadOnlyProxy, "poll_once", transition)
    result = RUNNER.run_candidate(value, lambda target: provider, factory, stop_event=stop,
                                  ready_observer=lambda proof: None)
    events = [json.loads(line) for line in capsys.readouterr().out.splitlines() if '"event": "PROXY_HEALTH"' in line]
    assert result["status"] == "STOP_REQUESTED"
    assert [event["status"] for event in events] == ["RUNNING", "DEGRADED", "RUNNING"]
    assert [event["writes_blocked"] for event in events] == [False, True, False]


def attach_fixture(monkeypatch, *, corruption=None, provider=None):
    proxy_module = sys.modules[RUNNER.RedisShmReadOnlyProxy.__module__]
    parent_pid = os.getppid()
    assert parent_pid != 2852
    native = dict(native_pid=2852, native_creation_filetime=789, windows_session_id=1, executable="qmt.exe")
    parent = dict(native_pid=parent_pid, native_creation_filetime=456, windows_session_id=1, executable="supervisor.exe")
    monkeypatch.setattr(proxy_module, "_windows_native_process_identity",
                        lambda pid: copy.deepcopy(native if pid == 2852 else parent))

    def mutate(request, response):
        if request["method"] != "ping":
            return
        data = response["data"]
        data.update(native_creation_filetime=789, read_fault_isolation_contract="READ_FAILURE_ISOLATION_V1")
        if data["transport"] == "shm_rpc":
            data.update(native_read_source_state="UNKNOWN", native_read_session_id="readonly-sid")
            if corruption == "not_ready":
                data.update(native_write_runtime_guard_ready=False, native_callback_threading_verified=False)
            field = next((item for item in ("supervisor_attach", "supervisor_attach_status")
                          if item in request["params"]), None)
            if field is not None:
                receipt = dict(contract="QMT_NATIVE_SUPERVISOR_ATTACH_V1", attach_id=request["params"][field]["attach_id"],
                    status="PENDING" if field == "supervisor_attach" else "ATTACHED", automatic_retry=False,
                    native_pid=2852, native_creation_filetime=789, windows_session_id=1,
                    execution_session_id="execution-sid", supervisor_pid=parent_pid,
                    supervisor_creation_filetime=456)
                if field == "supervisor_attach_status" and corruption == "receipt_pid":
                    receipt["native_pid"] += 1
                elif field == "supervisor_attach_status" and corruption == "receipt_birth":
                    receipt["supervisor_creation_filetime"] += 1
                elif field == "supervisor_attach_status" and corruption == "receipt_session":
                    receipt["execution_session_id"] = "other"
                elif field == "supervisor_attach_status" and corruption == "receipt_attach_id":
                    receipt["attach_id"] = "f" * 32
                elif field == "supervisor_attach_status" and corruption == "receipt_extra":
                    receipt["extra"] = True
                data[field] = receipt
                if field == "supervisor_attach_status" and corruption == "missing_epoch_contract":
                    data.pop("supervisor_gate_epoch_contract")
                elif field == "supervisor_attach_status" and corruption == "callback_unverified":
                    data["native_callback_threading_verified"] = False
                elif field == "supervisor_attach_status" and corruption == "not_attached":
                    data["native_write_runtime_guard_attached"] = False
                elif field == "supervisor_attach_status" and corruption == "read_session":
                    data["native_read_session_id"] = "other"

    def install(read, execution):
        read.mutate = execution.mutate = mutate

    return make_proxy(verify=False, provider=provider, client_mutator=install,
                      start_write_blocked=False, live_write_authorized=True)


def test_supervisor_attach_is_single_use_and_binds_verified_processes(monkeypatch):
    proxy, provider, _, execution = attach_fixture(monkeypatch)
    receipt = proxy.attach_native_supervisor()
    assert receipt["status"] == "ATTACHED" and receipt["execution_session_id"] == "execution-sid"
    attach = execution.calls[0][0]
    status = execution.calls[1][0]
    assert attach["params"]["supervisor_attach"]["contract"] == "QMT_NATIVE_SUPERVISOR_ATTACH_V1"
    assert status["params"]["supervisor_attach_status"] == attach["params"]["supervisor_attach"]
    assert attach["request_id"] != status["request_id"]
    assert provider.ttl(proxy._supervisor_attach_key) == -1
    assert json.loads(provider.hashes[proxy._supervisor_attach_key]["execution-sid"])["state"] == "ATTACHED"
    with pytest.raises(Exception, match="single verified LIVE"):
        proxy.attach_native_supervisor()
    assert len(execution.calls) == 2
    proxy.verify_identity()
    assert proxy.identity_snapshot()["write_ready"] is True


@pytest.mark.parametrize("corruption", ["receipt_pid", "receipt_birth", "receipt_session", "receipt_extra",
                                        "receipt_attach_id", "missing_epoch_contract", "callback_unverified",
                                        "not_attached", "read_session"])
def test_invalid_supervisor_attach_receipt_stops_and_is_never_resent(monkeypatch, corruption):
    proxy, provider, _, execution = attach_fixture(monkeypatch, corruption=corruption)
    with pytest.raises(Exception, match="attachment receipt or capability is invalid"):
        proxy.attach_native_supervisor()
    assert not proxy.stopped and not proxy.isolation_reason and len(execution.calls) == 2
    assert json.loads(provider.hashes[proxy._supervisor_attach_key]["execution-sid"])["state"] == "ATTACH_PENDING"
    with pytest.raises(Exception):
        proxy.attach_native_supervisor()
    assert len(execution.calls) == 2


def test_supervisor_attach_without_callback_proof_is_recorded_but_keeps_writes_blocked(monkeypatch):
    proxy, provider, _, execution = attach_fixture(monkeypatch, corruption="not_ready")
    assert proxy.attach_native_supervisor()["status"] == "ATTACHED"
    assert json.loads(provider.hashes[proxy._supervisor_attach_key]["execution-sid"])["state"] == "ATTACHED"
    proxy.verify_identity()
    snapshot = proxy.identity_snapshot()
    assert snapshot["execution_source"]["native_write_runtime_guard_attached"] is True
    assert snapshot["execution_source"]["native_callback_threading_verified"] is False
    assert snapshot["write_ready"] is False and "NATIVE_SUPERVISOR_GATE_UNAVAILABLE" in snapshot["write_block_reasons"]
    response = proxy.process_request(request("submit_order", rid="callback-unverified"))
    assert response["status"] == "REJECTED" and len(execution.calls) == 3
    assert not provider.hashes.get(proxy._write_ledger_key)


def test_unknown_supervisor_attach_result_stops_and_is_never_resent(monkeypatch):
    proxy, provider, _, execution = attach_fixture(monkeypatch)
    execution.failure = FIXTURE.ShmTimeoutError("fixture attach result unknown")
    execution.failure.publication_state = "UNKNOWN"
    with pytest.raises(FIXTURE.ShmTimeoutError):
        proxy.attach_native_supervisor()
    assert not proxy.stopped and not proxy.isolation_reason and len(execution.calls) == 1
    with pytest.raises(Exception):
        proxy.attach_native_supervisor()
    assert len(execution.calls) == 1
    restarted, _, _, restarted_execution = attach_fixture(monkeypatch, provider=provider)
    with pytest.raises(Exception, match="previously attempted"):
        restarted.attach_native_supervisor()
    assert not restarted_execution.calls


@pytest.mark.parametrize("failure", ["status_unknown", "failed", "timeout"])
def test_attach_status_failure_never_reissues_the_original_or_enables_writes(monkeypatch, failure):
    proxy, provider, _, execution = attach_fixture(monkeypatch)
    proxy_module = sys.modules[RUNNER.RedisShmReadOnlyProxy.__module__]
    monkeypatch.setattr(proxy_module, "SUPERVISOR_ATTACH_BUDGET_SECONDS", 0.15)
    send = execution.send_request

    def injected(value, timeout_seconds, **kwargs):
        response = send(value, timeout_seconds, **kwargs)
        if "supervisor_attach_status" in value["params"]:
            if failure == "status_unknown":
                error = FIXTURE.ShmTimeoutError("published status response unknown")
                error.publication_state = "UNKNOWN"
                raise error
            response["data"]["supervisor_attach_status"]["status"] = "FAILED" if failure == "failed" else "PENDING"
        return response

    execution.send_request = injected
    with pytest.raises(Exception):
        proxy.attach_native_supervisor()
    assert not proxy.stopped and not proxy.isolation_reason and proxy._supervisor_attach_verified is False
    assert json.loads(provider.hashes[proxy._supervisor_attach_key]["execution-sid"])["state"] == "ATTACH_PENDING"
    requests = [item[0] for item in execution.calls]
    assert sum("supervisor_attach" in item["params"] for item in requests) == 1
    assert len({item["request_id"] for item in requests}) == len(requests)
    restarted, _, _, restarted_execution = attach_fixture(monkeypatch, provider=provider)
    with pytest.raises(Exception, match="previously attempted"):
        restarted.attach_native_supervisor()
    assert not restarted_execution.calls


@pytest.mark.parametrize("changed", ["native", "supervisor"])
def test_attach_refuses_process_identity_changed_during_observation(monkeypatch, changed):
    proxy, provider, _, execution = attach_fixture(monkeypatch)
    proxy_module = sys.modules[RUNNER.RedisShmReadOnlyProxy.__module__]
    original = proxy_module._windows_native_process_identity
    calls = {}

    def changed_identity(pid):
        calls[pid] = calls.get(pid, 0) + 1
        result = original(pid)
        if calls[pid] > 1 and ((changed == "native") is (pid == 2852)):
            result["native_creation_filetime"] += 1
        return result

    monkeypatch.setattr(proxy_module, "_windows_native_process_identity", changed_identity)
    with pytest.raises(Exception, match="identity changed during attachment"):
        proxy.attach_native_supervisor()
    assert not proxy.stopped and not proxy.isolation_reason and len(execution.calls) == 2
    assert json.loads(provider.hashes[proxy._supervisor_attach_key]["execution-sid"])["state"] == "ATTACH_PENDING"


@pytest.mark.parametrize("accepted", [True, False])
def test_rotation_private_registration_checks_durable_identity_without_storing_secret(monkeypatch, accepted):
    proxy, provider, _, execution = attach_fixture(monkeypatch)
    proxy.attach_native_supervisor()
    proxy.verify_identity()
    record = dict(schema_version=1, state="PREPARE_PENDING", transition_id="a" * 32, account_id=ACCOUNT,
                  environment="LIVE", native_pid=2852, native_creation_filetime=789, windows_session_id=1,
                  execution_session_id="execution-sid", old_read_session_id="readonly-sid",
                  old_request_id="old-read", old_sequence=1, prepare_nonce="b" * 32, automatic_retry=False)
    proxy._claim_read_rotation(record)
    requests = []

    class Channel:
        def register(self, request):
            requests.append(dict(request))
            value = dict(contract="SUPERVISOR_ROTATION_AUTH_V1", action="REGISTER", status="REGISTERED",
                         stage="PREPARE", transition_id=record["transition_id"], nonce=record["prepare_nonce"],
                         capability="c" * 64)
            if not accepted:
                value.pop("capability")
                value["status"] = "UNAVAILABLE"
            return value

    proxy._supervisor_rotation_channel = Channel()
    count = len(execution.calls)
    if accepted:
        assert proxy._register_rotation_authorization(record, "PREPARE") == "c" * 64
    else:
        with pytest.raises(Exception, match="registration response is unconfirmed"):
            proxy._register_rotation_authorization(record, "PREPARE")
    with pytest.raises(Exception, match="already attempted"):
        proxy._register_rotation_authorization(record, "PREPARE")
    assert len(requests) == 1 and requests[0]["candidate_read_session_id"] is None
    assert len(execution.calls) == count and not proxy.isolation_reason
    assert "c" * 64 not in json.dumps(provider.hashes)


@pytest.mark.parametrize("case", ["verified", "native_not_ready", "begin_unavailable", "status_unknown", "manager_mismatch", "stale"])
def test_rotation_capability_requires_independent_matching_native_and_supervisor_proofs(monkeypatch, case):
    proxy, provider, _, execution = attach_fixture(monkeypatch)
    proxy.attach_native_supervisor()
    proxy.verify_identity()
    native, supervisor = proxy._verified_process_identity, proxy._supervisor_attach_receipt["supervisor"]
    proxy_identity = dict(native_pid=os.getpid(), native_creation_filetime=123, windows_session_id=1, executable="proxy.exe")
    proxy_module = sys.modules[RUNNER.RedisShmReadOnlyProxy.__module__]
    monkeypatch.setattr(proxy_module, "_windows_native_process_identity", lambda pid: copy.deepcopy(
        native if pid == 2852 else proxy_identity if pid == os.getpid() else supervisor))
    previous = execution.mutate

    def capability_receipt(nonce):
        return dict(contract="QMT_NATIVE_ROTATION_PIPE_V1", action="CAPABILITY_PROBE", nonce=nonce,
            account_id=ACCOUNT, environment="LIVE", native_pid=2852, native_creation_filetime=789,
            windows_session_id=1, execution_session_id="execution-sid", read_session_id="readonly-sid",
            supervisor_pid=os.getppid(), supervisor_creation_filetime=456, status="READY")

    def native_challenge(request, response):
        previous(request, response)
        for field in ("read_rotation_capability", "read_rotation_capability_status"):
            if field in request["params"]:
                nonce = request["params"][field]["nonce"]
                value = dict(contract="READ_ROTATION_CAPABILITY_V1", nonce=nonce, status="PENDING", automatic_retry=False)
                if field.endswith("_status"):
                    value.update(status="UNKNOWN" if case == "status_unknown" else "READY",
                                 receipt=None if case == "status_unknown" else capability_receipt(nonce))
                    response["data"].update(read_rotation_capability_contract="READ_ROTATION_CAPABILITY_V1",
                        read_rotation_authorization_ready=case not in ("native_not_ready", "status_unknown"))
                response["data"][field] = value

    execution.mutate = native_challenge
    calls = []

    class Channel:
        def register(self, request):
            calls.append(dict(request))
            if request["action"] == "BEGIN":
                return dict(contract=request["contract"], action="BEGIN",
                            status="UNAVAILABLE" if case == "begin_unavailable" else "PENDING", nonce=request["nonce"])
            receipt = capability_receipt(request["nonce"])
            if case == "manager_mismatch":
                receipt["supervisor_creation_filetime"] += 1
            return dict(contract=request["contract"], action="VERIFY", status="VERIFIED", nonce=request["nonce"], receipt=receipt)

        def close(self):
            pass

    proxy._supervisor_rotation_channel = Channel()
    before = len(execution.calls)
    if case == "begin_unavailable":
        assert proxy.attest_read_rotation_capability() is None
        assert proxy._rotation_capability_attempted is False and len(execution.calls) == before
    elif case in ("status_unknown", "manager_mismatch"):
        with pytest.raises(Exception):
            proxy.attest_read_rotation_capability()
        with pytest.raises(Exception, match="capability is unavailable"):
            proxy.attest_read_rotation_capability()
        assert proxy._rotation_capability is None
    else:
        result = proxy.attest_read_rotation_capability()
        assert result["status"] == "VERIFIED"
        observed = dict(proxy._source_identity["execution"], read_rotation_capability_contract="READ_ROTATION_CAPABILITY_V1",
                        read_rotation_authorization_ready=case != "native_not_ready")
        if case == "stale":
            proxy._rotation_capability["verified_at"] -= 21
        assert proxy._native_rotation_authorization_verified({"data": observed}) is (case == "verified")
        assert len({item["request_id"] for item, _ in execution.calls}) == len(execution.calls)
    assert proxy._read_rotation_key not in provider.hashes
    assert not any(item[0]["method"] in FIXTURE.ALL_WRITE_METHODS for item in execution.calls)


def test_generated_native_capability_challenge_is_readonly_and_does_not_enable_rotation(monkeypatch):
    source = DUAL.Fixture(monkeypatch, **DUAL.live_settings(account_id=ACCOUNT, execution_event_guard=True,
                                                           read_fault_isolation=True))
    source.namespace["_native_creation_filetime"] = lambda: 789
    source.start(enable_fixture_write_gate=False)
    parent_pid = os.getppid()
    identity = dict(pid=parent_pid, creation_filetime=456, windows_session_id=1)
    source.namespace["DUAL_SHM_SETTINGS"]["native_pid"] = 0
    source.namespace["_verify_identity"] = lambda: os.getpid()
    source.namespace["_SUPERVISOR_PROCESS"] = object()
    source.namespace["_SUPERVISOR_IDENTITY"] = identity
    source.namespace["_supervisor_identity"] = lambda process: dict(identity)
    source.namespace["_SUPERVISOR_ATTACH_STATE"] = {"status": "ATTACHED"}

    class NativePipe:
        poisoned = False
        handle = 99

        def __init__(self):
            self.requests = []

        def exchange(self, request):
            self.requests.append(dict(request))
            return dict(request, status="READY")

        def close(self):
            pass

    native_pipe = NativePipe()
    source.namespace["_SUPERVISOR_PIPE"] = native_pipe
    source.namespace["_SUPERVISOR_GATE"] = SimpleNamespace(failed=False, close=lambda: None)
    native_identity = dict(native_pid=os.getpid(), native_creation_filetime=789, windows_session_id=1,
                           executable="fixture-native")
    supervisor = dict(native_pid=parent_pid, native_creation_filetime=456, windows_session_id=1,
                      executable="fixture-supervisor")
    proxy_module = sys.modules[RUNNER.RedisShmReadOnlyProxy.__module__]
    monkeypatch.setattr(proxy_module, "_windows_native_process_identity",
                        lambda pid: copy.deepcopy(native_identity if pid == os.getpid() else supervisor))
    provider = FIXTURE.FakeRedis()
    read, execution = source.clients["read"], source.clients["execution"]
    proxy = RUNNER.RedisShmReadOnlyProxy(FIXTURE.RedisTransport(provider, account_id=ACCOUNT), read, execution,
        account_id=ACCOUNT, environment="LIVE", runtime_policy=POLICY, allow_order_methods=True,
        start_write_blocked=False, live_write_authorized=True, max_requests=0, timeout_seconds=1,
        expected_readonly_session_id=read.mailbox.session_id, expected_execution_session_id=execution.mailbox.session_id,
        expected_native_pid=os.getpid(), expected_windows_session_id=1,
        execution_cursor_guard_target={"gateway_id": "qmt-vm-001", "profile_id": "live-stock"},
        supervisor_write_gate=FIXTURE.FakeSupervisorWriteGate())
    proxy._supervisor_attach_verified = True
    proxy._supervisor_attach_receipt = {"native": native_identity, "supervisor": supervisor,
        "receipt": {"native_creation_filetime": 789}}
    calls = []

    class SupervisorChannel:
        def register(self, request):
            calls.append(dict(request))
            if request["action"] == "BEGIN":
                return dict(contract=request["contract"], action="BEGIN", status="PENDING", nonce=request["nonce"])
            return dict(contract=request["contract"], action="VERIFY", status="VERIFIED", nonce=request["nonce"],
                        receipt=dict(native_pipe.requests[-1], status="READY"))

        def close(self):
            pass

    proxy._supervisor_rotation_channel = SupervisorChannel()

    def drive(function):
        with ThreadPoolExecutor(1) as executor:
            future = executor.submit(function)
            until = time.monotonic() + 3
            while not future.done() and time.monotonic() < until:
                source.namespace["aqp_shm_readonly_adjust"](source.context)
                source.namespace["aqp_shm_rpc_adjust"](source.context)
                time.sleep(0.001)
            return future.result(timeout=0.2)

    try:
        drive(proxy.verify_identity)
        proof = drive(proxy.attest_read_rotation_capability)
        assert proof == {"status": "VERIFIED", "native_rotation_ready": False, "automatic_retry": False}
        assert [item["action"] for item in calls] == ["BEGIN", "VERIFY"]
        assert len(native_pipe.requests) == 1 and native_pipe.requests[0]["action"] == "CAPABILITY_PROBE"
        assert calls[0]["nonce"] == calls[1]["nonce"] == native_pipe.requests[0]["nonce"]
        assert proxy.identity_snapshot()["write_ready"] is False
        assert proxy._read_rotation_key not in provider.hashes
        source.writes.assert_not_called()
    finally:
        proxy.close()
        source.close()


def test_supervisor_attach_missing_gate_is_not_sent(monkeypatch):
    proxy, _, _, execution = attach_fixture(monkeypatch)
    proxy._supervisor_write_gate = None
    with pytest.raises(Exception, match="single verified LIVE"):
        proxy.attach_native_supervisor()
    assert not execution.calls and proxy._supervisor_attach_attempted is False


def test_native_self_report_cannot_replace_verified_supervisor_attach():
    proxy, provider, _, execution = make_proxy(verify=False, start_write_blocked=False, live_write_authorized=True)
    proxy.verify_identity()
    assert proxy.identity_snapshot()["write_ready"] is False
    assert "NATIVE_SUPERVISOR_GATE_UNAVAILABLE" in proxy.identity_snapshot()["write_block_reasons"]
    response = proxy.process_request(request("submit_order", rid="native-self-report-only"))
    assert response["status"] == "REJECTED" and len(execution.calls) == 1
    assert not provider.hashes.get(proxy._write_ledger_key)


@pytest.mark.parametrize("failure", ["hgetall", "claim", "ttl"])
def test_supervisor_attach_redis_failure_never_sends_native(monkeypatch, failure):
    proxy, provider, _, execution = attach_fixture(monkeypatch)
    if failure == "claim":
        provider.fail_eval = "claim"
    else:
        provider.fail_operation = failure
    with pytest.raises(Exception):
        proxy.attach_native_supervisor()
    assert not proxy.stopped and not proxy.isolation_reason and not execution.calls


@pytest.mark.parametrize("phase", ["claim", "settle"])
def test_supervisor_attach_lost_redis_reply_never_retries_after_restart(monkeypatch, phase):
    proxy, provider, _, execution = attach_fixture(monkeypatch)
    original_eval = provider.eval

    def lost_reply(script, *args):
        result = original_eval(script, *args)
        if ("HLEN" in script) is (phase == "claim"):
            raise TimeoutError("fixture Redis reply lost after mutation")
        return result

    provider.eval = lost_reply
    with pytest.raises(TimeoutError):
        proxy.attach_native_supervisor()
    assert not proxy.stopped and not proxy.isolation_reason and len(execution.calls) == (0 if phase == "claim" else 2)
    assert provider.ttl(proxy._supervisor_attach_key) == -1
    provider.eval = original_eval
    restarted, _, _, restarted_execution = attach_fixture(monkeypatch, provider=provider)
    with pytest.raises(Exception, match="previously attempted"):
        restarted.attach_native_supervisor()
    assert not restarted_execution.calls


@pytest.mark.parametrize("fail_attach,enable_driver", [(False, False), (True, False), (False, True)])
def test_runner_generated_live_attach_records_installed_gate_without_enabling_writes(monkeypatch, fail_attach, enable_driver):
    source = DUAL.Fixture(monkeypatch, **DUAL.live_settings(account_id=ACCOUNT, execution_event_guard=True,
                                                           read_fault_isolation=True))
    source.start(enable_fixture_write_gate=False)
    parent_pid = os.getppid()
    supervisor = dict(pid=parent_pid, creation_filetime=456, windows_session_id=1)
    source.namespace["DUAL_SHM_SETTINGS"]["native_pid"] = 0
    source.namespace["_BOUND_NATIVE_CREATION_FILETIME"] = 789
    source.namespace["_native_creation_filetime"] = lambda: 789
    source.namespace["_verify_identity"] = lambda: os.getpid()
    source.namespace["_SUPERVISOR_PROCESS"] = object()
    source.namespace["_SUPERVISOR_IDENTITY"] = supervisor
    source.namespace["_supervisor_identity"] = lambda process: dict(supervisor)
    source.namespace["_RUNTIMES"]["read"]["_SERVER"].native_creation_filetime = 789
    source.namespace["_RUNTIMES"]["execution"]["_SERVER"].bridge.runtime_metadata["native_creation_filetime"] = 789
    acknowledged = []

    class ModelPipe:
        def __init__(self, *args):
            pass

        def connect(self):
            pass

        def exchange(self, value):
            response = dict(value, status="HANDLE_OFFER", guard_handles={})
            if fail_attach:
                response["supervisor_creation_filetime"] += 1
            return response

        def close(self):
            pass

    class ModelNativeGate:
        def __init__(self, *args):
            pass

        def acknowledge(self):
            acknowledged.append(True)

        def close(self):
            pass

    native_guard = source.namespace["_NATIVE_GUARD"]
    native_guard.update(WindowsNativeKernel=lambda: object(), NativeControlPipe=ModelPipe, NativeWriteGate=ModelNativeGate)
    provider = FIXTURE.FakeRedis()
    provider.connection_pool = SimpleNamespace(disconnect=mock.Mock())
    native_identity = dict(native_pid=os.getpid(), native_creation_filetime=789, windows_session_id=1,
                           executable="qmt.exe")
    parent_identity = dict(native_pid=parent_pid, native_creation_filetime=456, windows_session_id=1,
                           executable="supervisor.exe")
    proxy_module = sys.modules[RUNNER.RedisShmReadOnlyProxy.__module__]
    monkeypatch.setattr(proxy_module, "_windows_native_process_identity",
                        lambda pid: copy.deepcopy(native_identity if pid == os.getpid() else parent_identity))
    monkeypatch.setattr(RUNNER, "windows_session_id", lambda pid: 1)
    monkeypatch.setattr(RUNNER.InheritedSupervisorWriteGate, "from_environment", lambda: FIXTURE.FakeSupervisorWriteGate())
    values = config(native_pid=os.getpid(), live_write_authorized=True, start_write_blocked=False,
                    execution_cursor_guard_target={"gateway_id": "qmt-vm-001", "profile_id": "live-stock"},
                    read_name=source.clients["read"].mailbox.name,
                    execution_name=source.clients["execution"].mailbox.name,
                    read_session_id=source.clients["read"].mailbox.session_id,
                    execution_session_id=source.clients["execution"].mailbox.session_id)
    stop, ready, observed = threading.Event(), [], []
    steps = []
    if enable_driver:
        channel = SimpleNamespace(register=mock.Mock(), close=mock.Mock())
        monkeypatch.setattr(RUNNER.InheritedRotationChannel, "from_environment", lambda: channel)
        original_step = RUNNER.ReadonlyRotationDriver.step
        def observe_driver(driver):
            steps.append(driver.proxy)
            original_step(driver)
            stop.set()
        monkeypatch.setattr(RUNNER.ReadonlyRotationDriver, "step", observe_driver)
    else:
        stop.set()

    def factory(name, account):
        key = "read" if name == values["read_name"] else "execution"
        client = source.transports[key].ShmReadOnlyClient(name, account)
        send = client.send_request

        def capture(request, timeout_seconds, **kwargs):
            response = send(request, timeout_seconds, **kwargs)
            observed.append((request, response))
            return response

        client.send_request = capture
        return client

    try:
        with ThreadPoolExecutor(1) as executor:
            future = executor.submit(RUNNER.run_candidate, values, lambda _: provider, factory,
                                     stop_event=stop, ready_observer=ready.append)
            until = time.monotonic() + 3
            while not future.done() and time.monotonic() < until:
                source.namespace["aqp_shm_readonly_adjust"](source.context)
                source.namespace["aqp_shm_rpc_adjust"](source.context)
                time.sleep(0.001)
            result = future.result(timeout=0.2)
        record = provider.hashes["aqp:redis_shm:supervisor_attach:%s:v1" % ACCOUNT]
        if fail_attach:
            assert result["status"] == "FAILED" and result["identity_verified"] is False, json.dumps(result)
            assert result["writes_blocked"] is True and result["write_ready"] is False
            assert result["write_block_reason"] == "SOURCE_IDENTITY_UNVERIFIED"
            assert acknowledged == [] and ready == []
            assert json.loads(record[values["execution_session_id"]])["state"] == "ATTACH_PENDING"
            source.writes.assert_not_called()
            return
        assert result["status"] == "STOP_REQUESTED" and result["identity_verified"] is True, json.dumps([result, observed])
        assert acknowledged == [True] and len(ready) == 1
        assert ready[0]["execution_source"]["native_write_runtime_guard_attached"] is True
        assert ready[0]["execution_source"]["native_callback_threading_verified"] is False
        assert ready[0]["execution_source"]["native_write_runtime_guard_ready"] is False
        assert ready[0]["write_ready"] is False
        assert json.loads(record[values["execution_session_id"]])["state"] == "ATTACHED"
        assert provider.ttl("aqp:redis_shm:supervisor_attach:%s:v1" % ACCOUNT) == -1
        if enable_driver:
            assert len(steps) == 1 and steps[0].execution_cursor_guard_target == values["execution_cursor_guard_target"]
            channel.register.assert_not_called()
        source.writes.assert_not_called()
    finally:
        source.close()


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


@pytest.mark.parametrize("field", ["environment", "runtime_policy"])
def test_live_configuration_missing_permission_fields_is_rejected_offline(field):
    value = config()
    value.pop(field)
    with pytest.raises(ValueError):
        RUNNER.validate_config(value)


def test_live_proxy_constructor_cannot_disable_initial_write_block_without_authorization():
    for blocked in (False, 1):
        with pytest.raises(ValueError, match="start_write_blocked"):
            make_proxy(start_write_blocked=blocked)
    proxy, _, _, _ = make_proxy(start_write_blocked=None)
    assert proxy.identity_snapshot()["writes_blocked"] is True
    with pytest.raises(ValueError, match="expected Windows session"):
        make_proxy(expected_windows_session_id=None)


def test_live_default_authorization_and_restart_configuration_remain_write_blocked():
    value = config()
    value.pop("start_write_blocked")
    parsed = RUNNER.validate_config(value)
    assert parsed["start_write_blocked"] is True and parsed["live_write_authorized"] is False
    for _ in range(2):
        proxy, _, _, _ = make_proxy()
        assert proxy.identity_snapshot()["write_ready"] is False


@pytest.mark.parametrize("authorized", [None, 0, 1, "true"])
def test_live_authorization_flag_is_strict_boolean(authorized):
    with pytest.raises(ValueError, match="live_write_authorized"):
        RUNNER.validate_config(config(live_write_authorized=authorized))
    with pytest.raises(ValueError, match="live_write_authorized"):
        make_proxy(live_write_authorized=authorized)


def test_explicit_live_authorization_allows_future_configured_write_capability():
    parsed = RUNNER.validate_config(config(live_write_authorized=True, start_write_blocked=False))
    assert parsed["live_write_authorized"] is True and parsed["start_write_blocked"] is False
    proxy, provider, _, execution = make_proxy(live_write_authorized=True, start_write_blocked=False)
    assert proxy.identity_snapshot()["write_ready"] is True
    execution.result = dict(status="SUBMITTED", order_sys_id="LIVE_FIXTURE_ONLY")
    value = request("submit_order")
    response = proxy.process_request(value)
    assert response["ok"] is True and response["environment"] == "LIVE"
    assert len(execution.calls) == 2
    assert proxy.process_request(value) == response
    assert len(execution.calls) == 2
    record = json.loads(provider.hashes[LEDGER][value["request_id"]])
    assert record["state"] == "CONFIRMED" and record["binding"]["environment"] == "LIVE"


def test_explicit_authorization_cannot_override_unknown_ledger_on_restart():
    proxy, provider, _, execution = make_proxy(live_write_authorized=True, start_write_blocked=False)
    execution.result = dict(status="UNKNOWN", error="fixture native result unknown")
    value = request("submit_order")
    response = proxy.process_request(value)
    assert response["status"] == "UNKNOWN" and len(execution.calls) == 2
    assert proxy.identity_snapshot()["writes_blocked"] is False
    restarted, _, _, fresh = make_proxy(provider=provider, live_write_authorized=True, start_write_blocked=False)
    assert restarted.identity_snapshot()["writes_blocked"] is False
    assert restarted.identity_snapshot()["write_ready"] is True
    duplicate = restarted.process_request(value)
    assert duplicate["status"] == "UNKNOWN" and len(fresh.calls) == 1
    fresh.result = dict(status="SUBMITTED", order_sys_id="NEW-INDEPENDENT")
    accepted = restarted.process_request(request("submit_order", rid="another-live-fixture"))
    assert accepted["ok"] is True and len(fresh.calls) == 2


def test_vm_new_source_allows_fresh_writes_but_never_replays_old_unknown():
    proxy, provider, _, execution = make_proxy(live_write_authorized=True, start_write_blocked=False,
        execution_cursor_guard_target=None, supervisor_write_gate=None)
    execution.result = dict(status="UNKNOWN", error="fixture native result unknown")
    original = request("submit_order", rid="old-unknown")
    assert proxy.process_request(original)["status"] == "UNKNOWN"
    record = json.loads(provider.hashes[LEDGER]["old-unknown"])
    record["binding"]["read_session_id"] = "retired-read"
    record["binding"]["execution_session_id"] = "retired-execution"
    provider.hashes[LEDGER]["old-unknown"] = json.dumps(record)
    saved = provider.hashes[LEDGER]["old-unknown"]
    restarted, _, _, fresh = make_proxy(provider=provider, live_write_authorized=True, start_write_blocked=False,
        execution_cursor_guard_target=None, supervisor_write_gate=None,
        previous_source_unknown_target={"gateway_id": "qmt-vm-001", "profile_id": "live-stock"})
    assert restarted.identity_snapshot()["write_ready"] is True
    fresh.result = dict(status="SUBMITTED", order_sys_id="NEW_FIXTURE")
    assert restarted.process_request(request("submit_order", rid="new-order"))["ok"] is True
    assert provider.hashes[LEDGER]["old-unknown"] == saved
    before = len(fresh.calls)
    repeated = restarted.process_request(original)
    assert repeated["ok"] is False and len(fresh.calls) == before
    assert provider.hashes[LEDGER]["old-unknown"] == saved
    assert restarted.process_request(request("submit_order", rid="another-new-order"))["ok"] is True
    assert len(fresh.calls) == before + 1


def test_vm_current_source_unknown_still_blocks_even_when_old_sources_are_allowed():
    proxy, provider, _, execution = make_proxy(live_write_authorized=True, start_write_blocked=False,
        execution_cursor_guard_target=None, supervisor_write_gate=None)
    execution.result = dict(status="UNKNOWN", error="fixture native result unknown")
    assert proxy.process_request(request("submit_order", rid="current-unknown"))["status"] == "UNKNOWN"
    restarted, _, _, _ = make_proxy(provider=provider, live_write_authorized=True, start_write_blocked=False,
        execution_cursor_guard_target=None, supervisor_write_gate=None,
        previous_source_unknown_target={"gateway_id": "qmt-vm-001", "profile_id": "live-stock"})
    assert restarted.identity_snapshot()["writes_blocked"] is False


@pytest.mark.parametrize("target", [
    {"gateway_id": "qmt-win-001", "profile_id": "live-stock"},
    {"gateway_id": "qmt-vm-001", "profile_id": "sim-bigqmt"},
])
def test_previous_source_unknown_policy_rejects_non_vm_live_targets(target):
    with pytest.raises(ValueError):
        RUNNER.validate_config(config(previous_source_unknown_target=target))


def test_vm_incomplete_old_binding_remains_blocked():
    proxy, provider, _, execution = make_proxy(live_write_authorized=True, start_write_blocked=False,
        execution_cursor_guard_target=None, supervisor_write_gate=None)
    execution.result = dict(status="UNKNOWN", error="fixture native result unknown")
    assert proxy.process_request(request("submit_order", rid="old-incomplete"))["status"] == "UNKNOWN"
    record = json.loads(provider.hashes[LEDGER]["old-incomplete"])
    record["binding"].pop("execution_session_id")
    provider.hashes[LEDGER]["old-incomplete"] = json.dumps(record)
    restarted, _, _, _ = make_proxy(provider=provider, live_write_authorized=True, start_write_blocked=False,
        execution_cursor_guard_target=None, supervisor_write_gate=None,
        previous_source_unknown_target={"gateway_id": "qmt-vm-001", "profile_id": "live-stock"})
    assert restarted.identity_snapshot()["writes_blocked"] is False


def test_live_authorization_flag_cannot_expand_sim_policy():
    value = config(account_id="55013746", environment="SIM", runtime_policy="PERSISTENT_SIMULATION_READ_WRITE",
                   live_write_authorized=True)
    with pytest.raises(ValueError, match="LIVE-only"):
        RUNNER.validate_config(value)


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
    assert provider.operations == [
        ("hgetall", "aqp:redis_shm:read_rotation:%s:v1" % ACCOUNT),
        ("hgetall", "aqp:redis_shm:read_rotation_close:%s:v1" % ACCOUNT)]
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
    assert not proxy.stopped and not proxy.isolation_reason and len(execution.calls) == 2
    assert proxy.process_request(value) == response
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
        restarted, _, _, native = make_proxy(provider=provider)
        assert restarted.verified and len(native.calls) == 1
        assert provider.hashes[LEDGER]["old"]


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


@pytest.mark.parametrize("target", [None, {"gateway_id": "qmt-vm-001", "profile_id": "live-stock"}])
def test_live_runner_propagates_exact_account_and_initial_block_to_injected_resources(monkeypatch, target):
    value = config(receive_policy="QUEUE_ONLY", execution_cursor_guard_target=target)
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
    assert result["receive_policy"] == seen[0]["receive_policy"] == "QUEUE_ONLY"
    assert seen[0]["write_ready"] is False and seen[0]["writes_blocked"] is True
    assert seen[0].get("execution_cursor_guard_target") == target
    assert "read_source_state" in seen[0]
    assert result["processed_requests"] == 0
    assert all(client.closed and len(client.calls) == 1 for client in clients)
    provider.connection_pool.disconnect.assert_called_once()


def test_legacy_live_unknown_write_preserves_original_and_serves_independent_read():
    proxy, provider, read, execution = make_proxy(execution_cursor_guard_target=None,
        supervisor_write_gate=None, receive_policy="QUEUE_ONLY", start_write_blocked=False, live_write_authorized=True)
    original = request("submit_order", rid="unknown-original")
    original["params"].pop("execution_event_guard")
    original["params"].pop("supervisor_gate_epoch")
    execution.failure = FIXTURE.ShmTimeoutError("original outcome unconfirmed")
    response = proxy.process_request(original)
    assert response["status"] == "UNKNOWN" and not proxy.isolation_reason
    saved = provider.hashes[LEDGER][original["request_id"]]
    provider.queues[proxy.transport.request_queue] = [json.dumps(request("get_ticks", rid="later", params={"code": "000001.SZ"}))]
    assert proxy.poll_once()["ok"] is True
    assert proxy.process_request(original) == response
    assert len(execution.calls) == 2 and len(read.calls) == 2
    assert provider.hashes[LEDGER][original["request_id"]] == saved


def test_live_unconfirmed_atomic_dequeue_does_not_forward_its_original(isolated_redis):
    proxy, provider, read, execution = make_proxy(provider=isolated_redis, execution_cursor_guard_target=None,
        supervisor_write_gate=None, receive_policy="QUEUE_ONLY", start_write_blocked=False, live_write_authorized=True)
    queue = proxy.transport.request_queue
    raw = json.dumps(request("submit_order", rid="unknown-dequeue"))
    provider.rpush(queue, raw, json.dumps(request("get_positions", rid="after-dequeue")))
    actual = provider.eval
    def lost_reply(script, count, *args):
        actual(script, count, *args)
        provider.eval = actual
        raise redis.exceptions.TimeoutError("claim receipt unavailable")
    provider.eval = lost_reply
    assert proxy.poll_once() is None and not proxy.isolation_reason
    proxy._redis_retry_at = 0
    assert proxy.poll_once()["request_id"] == "after-dequeue"
    assert len(execution.calls) == 2 and len(read.calls) == 1
    assert proxy.process_request(request("submit_order", rid="unknown-dequeue"))["status"] == "UNKNOWN"
    assert len(execution.calls) == 2


def test_live_supervisor_fatal_gate_rejects_only_current_write_and_keeps_reads():
    proxy, provider, read, execution = make_proxy(receive_policy="QUEUE_ONLY", start_write_blocked=False, live_write_authorized=True)
    gate_error = sys.modules[RUNNER.RedisShmReadOnlyProxy.__module__].SupervisorWriteGateError
    proxy._supervisor_write_gate.check = mock.Mock(side_effect=gate_error("SUPERVISOR_WRITE_BLOCKED", permanent=True))
    queue = proxy.transport.request_queue
    provider.queues[queue] = [json.dumps(request("submit_order", rid="unverified-write")),
                             json.dumps(request("get_ticks", rid="independent-quote", params={"code": "000001.SZ"}))]
    assert proxy.poll_once()["publication_state"] == "NOT_SENT"
    assert proxy.poll_once()["ok"] is True
    assert not provider.queues[queue] and not proxy.isolation_reason
    assert len(read.calls) == 2 and len(execution.calls) == 1
    assert not provider.hashes.get(LEDGER)


def test_identity_pending_consumes_requests_without_shadowing_financial_receipt():
    provider = SelectiveReadRedis()
    proxy, provider, read, execution = make_proxy(provider=provider, receive_policy="QUEUE_ONLY")
    pending_identity(proxy)
    values = [request("submit_order", rid="old-order"), request("cancel_order", rid="old-cancel"),
              request("get_ticks", rid="old-order", params={"code": "000001.SZ"}),
              request("ping", rid="control", params={"supervisor_attach": {}}),
              request("get_full_tick", rid="independent-read", params={"codes": ["000001.SZ"]})]
    queue = proxy.transport.request_queue
    provider.queues[queue] = [json.dumps(row) for row in values]
    ledger_record = '{"state":"UNKNOWN","automatic_retry":false}'
    provider.hashes[LEDGER] = {"old-order": ledger_record}
    results = [proxy.poll_once() for _ in values]
    assert all(not row["ok"] for row in results[:-1]) and results[-1]["ok"] is True
    assert provider.queues[queue] == [] and provider.hashes[LEDGER] == {"old-order": ledger_record}
    assert [row[0]["method"] for row in read.calls] == ["ping", "get_full_tick"]
    assert len(execution.calls) == 1 and not proxy.isolation_reason


def test_identity_pending_rejects_each_write_and_eventually_serves_the_quote():
    provider = SelectiveReadRedis()
    proxy, provider, read, execution = make_proxy(provider=provider, receive_policy="QUEUE_ONLY")
    pending_identity(proxy)
    values = [request("submit_order", rid="order-%s" % number) for number in range(130)]
    values.append(request("get_ticks", rid="quote-after-orders", params={"code": "000001.SZ"}))
    queue = proxy.transport.request_queue
    provider.queues[queue] = [json.dumps(row) for row in values]
    for _ in range(130):
        result = proxy.poll_once()
        assert result["status"] == "REJECTED" and result["publication_state"] == "NOT_SENT"
    assert proxy.poll_once()["request_id"] == "quote-after-orders"
    assert not provider.queues[queue] and len(read.calls) == 2 and len(execution.calls) == 1
    assert LEDGER not in provider.hashes


def test_identity_ready_again_preserves_explicit_operator_stop_only():
    provider = SelectiveReadRedis()
    proxy, provider, read, execution = make_proxy(provider=provider, receive_policy="QUEUE_ONLY")
    pending_identity(proxy)
    queue = proxy.transport.request_queue
    provider.queues[queue] = [json.dumps(request("submit_order", rid="stopped-order")),
                             json.dumps(request("get_ticks", rid="first-read", params={"code": "000001.SZ"}))]
    assert proxy.poll_once()["publication_state"] == "NOT_SENT"
    assert proxy.poll_once()["request_id"] == "first-read"
    proxy._supervisor_write_gate.check.side_effect = None
    provider.queues[queue].append(json.dumps(request("get_ticks", rid="second-read", params={"code": "000001.SZ"})))
    assert proxy.poll_once()["request_id"] == "second-read"
    assert not provider.queues[queue] and len(read.calls) == 3 and len(execution.calls) == 1
    assert proxy.identity_snapshot()["write_block_reason"] == "OPERATOR_WRITE_DISABLED"


def test_identity_ready_again_resumes_normal_queue_when_no_write_block_remains():
    provider = SelectiveReadRedis()
    proxy, provider, read, execution = make_proxy(provider=provider, receive_policy="QUEUE_ONLY",
        start_write_blocked=False, live_write_authorized=True)
    pending_identity(proxy)
    queue = proxy.transport.request_queue
    first = json.dumps(request("get_ticks", rid="first-read", params={"code": "000001.SZ"}))
    second = json.dumps(request("get_ticks", rid="second-read", params={"code": "000001.SZ"}))
    provider.queues[queue] = [first]
    assert proxy.poll_once()["request_id"] == "first-read"
    proxy._supervisor_write_gate.check.side_effect = None
    provider.queues[queue].append(second)
    assert proxy.poll_once()["request_id"] == "second-read"
    assert provider.queues[queue] == []
    assert len(read.calls) == 3 and len(execution.calls) == 1
    assert any(op[:2] == ("eval", "dequeue") for op in provider.operations)


def test_source_identity_failure_rejects_current_request_and_recovers_next_request():
    provider = SelectiveReadRedis()
    proxy, provider, read, execution = make_proxy(provider=provider, receive_policy="QUEUE_ONLY")
    pending_identity(proxy)
    proxy._check_native_process_identity.side_effect = FIXTURE.ShmProtocolError("identity changed")
    queue = proxy.transport.request_queue
    provider.queues[queue] = [json.dumps(request("get_ticks", rid="wrong-source", params={"code": "000001.SZ"}))]
    response = proxy.poll_once()
    assert response["publication_state"] == "NOT_SENT" and not response["ok"]
    assert provider.queues[queue] == [] and len(read.calls) == len(execution.calls) == 1
    proxy._check_native_process_identity.side_effect = None
    provider.queues[queue].append(json.dumps(request("get_ticks", rid="current-source", params={"code": "000001.SZ"})))
    assert proxy.poll_once()["ok"] is True and len(read.calls) == 2
    assert not proxy.isolation_reason


@pytest.mark.parametrize("fatal_on_check", [2, 3])
def test_identity_pending_turning_fatal_does_not_stop_independent_reads(fatal_on_check):
    provider = SelectiveReadRedis()
    proxy, provider, read, execution = make_proxy(provider=provider, receive_policy="QUEUE_ONLY", start_write_blocked=False, live_write_authorized=True)
    pending_identity(proxy)
    gate_error = sys.modules[RUNNER.RedisShmReadOnlyProxy.__module__].SupervisorWriteGateError
    proxy._supervisor_write_gate.check.side_effect = gate_error("SUPERVISOR_WRITE_BLOCKED", permanent=True)
    queue = proxy.transport.request_queue
    provider.queues[queue] = [json.dumps(request("submit_order", rid="unconfirmed-gate")),
                             json.dumps(request("get_ticks", rid="independent-read", params={"code": "000001.SZ"}))]
    assert proxy.poll_once()["publication_state"] == "NOT_SENT"
    assert proxy.poll_once()["ok"] is True and not provider.queues[queue]
    assert not proxy.isolation_reason and len(read.calls) == 2 and len(execution.calls) == 1


def test_identity_pending_dequeue_failure_keeps_later_request_eligible(isolated_redis):
    proxy, provider, read, execution = make_proxy(provider=isolated_redis, receive_policy="QUEUE_ONLY")
    pending_identity(proxy)
    queue = proxy.transport.request_queue
    raw = json.dumps(request("get_ticks", rid="unknown-dequeue", params={"code": "000001.SZ"}))
    provider.rpush(queue, raw, json.dumps(request("get_ticks", rid="after-dequeue", params={"code": "000001.SZ"})))
    actual = provider.eval
    def lost_reply(script, count, *args):
        actual(script, count, *args)
        provider.eval = actual
        raise redis.exceptions.TimeoutError("claim receipt unavailable")
    provider.eval = lost_reply
    assert proxy.poll_once() is None
    proxy._redis_retry_at = 0
    assert proxy.poll_once()["request_id"] == "after-dequeue"
    assert not proxy.isolation_reason and len(read.calls) == 2 and len(execution.calls) == 1


@pytest.fixture
def isolated_redis(tmp_path):
    executable = shutil.which("redis-server")
    if executable is None or os.name == "nt":
        pytest.skip("isolated Unix-socket Redis is unavailable")
    # macOS 的 AF_UNIX 路径长度小于 pytest 默认临时路径长度。
    with tempfile.TemporaryDirectory(prefix="aqp-rd-", dir="/tmp") as short_directory:
        socket_path = str(Path(short_directory) / "redis.sock")
        server = subprocess.Popen([executable, "--port", "0", "--unixsocket", socket_path,
                                   "--save", "", "--appendonly", "no", "--dir", str(tmp_path)],
                                  stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        client = redis.Redis(unix_socket_path=socket_path, decode_responses=True, socket_timeout=1)
        try:
            deadline = time.monotonic() + 5
            while True:
                try:
                    client.ping()
                    break
                except redis.exceptions.ConnectionError:
                    if server.poll() is not None or time.monotonic() >= deadline:
                        raise RuntimeError("isolated Redis did not start")
                    time.sleep(0.01)
            yield client
        finally:
            client.close()
            server.terminate()
            server.wait(timeout=5)


@pytest.mark.parametrize("claim_ran", [False, True])
def test_vm_uncertain_dequeue_preserves_exact_request_and_continues(isolated_redis, claim_ran):
    target = {"gateway_id": "qmt-vm-001", "profile_id": "live-stock"}
    proxy, provider, _, execution = make_proxy(provider=isolated_redis, receive_policy="QUEUE_ONLY",
        execution_cursor_guard_target=None, supervisor_write_gate=None, previous_source_unknown_target=target,
        start_write_blocked=False, live_write_authorized=True)
    original = request("submit_order", rid="lost-dequeue")
    original["params"].pop("execution_event_guard")
    original["params"].pop("supervisor_gate_epoch")
    raw = FIXTURE.encode_rpc_request_payload(original)
    later = FIXTURE.encode_rpc_request_payload(request("get_positions", rid="independent-read"))
    queue = proxy.transport.request_queue
    provider.rpush(queue, raw, later)
    actual_eval = provider.eval

    def claim_timeout(script, key_count, *args):
        assert "VM_QUEUE_CLAIM_V1" in script
        if claim_ran:
            assert actual_eval(script, key_count, *args) == 1
        provider.eval = actual_eval
        raise redis.exceptions.TimeoutError("dequeue response lost")

    provider.eval = claim_timeout
    assert proxy.poll_once() is None
    pending = dict(proxy._pending_dequeue_recovery)
    assert not proxy.isolation_reason and len(execution.calls) == 1
    proxy._redis_retry_at = 0
    assert proxy.poll_once()["request_id"] == "independent-read"
    assert provider.get(pending["marker"]) == raw and provider.ttl(pending["marker"]) == -1
    assert provider.lrange(queue, 0, -1) == []
    provider.rpush(queue, raw)
    # 原取件即使在恢复之后才抵达，也不能取走重现的相同请求。
    proxy_module = sys.modules[RUNNER.RedisShmReadOnlyProxy.__module__]
    assert actual_eval(proxy_module.VM_QUEUE_CLAIM, 2, queue, pending["marker"], raw) == -1
    response = proxy.poll_once()
    assert response["request_id"] == "lost-dequeue" and response["status"] == "UNKNOWN"
    assert [call[0]["method"] for call in execution.calls] == ["ping", "get_positions"]


def test_vm_uncertain_dequeue_preservation_can_retry_its_own_lost_reply(isolated_redis):
    target = {"gateway_id": "qmt-vm-001", "profile_id": "live-stock"}
    proxy, provider, _, execution = make_proxy(provider=isolated_redis, receive_policy="QUEUE_ONLY",
        execution_cursor_guard_target=None, supervisor_write_gate=None, previous_source_unknown_target=target,
        start_write_blocked=False, live_write_authorized=True)
    raw = FIXTURE.encode_rpc_request_payload(request("get_positions", rid="unknown-read"))
    queue = proxy.transport.request_queue
    provider.rpush(queue, raw)
    actual_eval = provider.eval

    def fail_claim(script, key_count, *args):
        provider.eval = actual_eval
        raise redis.exceptions.TimeoutError("claim reply lost")

    provider.eval = fail_claim
    assert proxy.poll_once() is None
    marker = proxy._pending_dequeue_recovery["marker"]
    proxy._redis_retry_at = 0

    def lose_preserve_reply(script, key_count, *args):
        assert actual_eval(script, key_count, *args) == 1
        provider.eval = actual_eval
        raise redis.exceptions.TimeoutError("preserve reply lost")

    provider.eval = lose_preserve_reply
    assert proxy.poll_once() is None and proxy._pending_dequeue_recovery is not None
    proxy._redis_retry_at = 0
    assert proxy.poll_once() is None and proxy._pending_dequeue_recovery is None
    assert provider.get(marker) == raw and provider.llen(queue) == 0
    assert not proxy.isolation_reason and len(execution.calls) == 1


@pytest.mark.parametrize("timed_out_method", ["ping", "get_asset", "get_positions", "query_orders", "query_trades", "query_execution_snapshot", "get_trade_login_state"])
@pytest.mark.parametrize("timing", ["late_frame", "acknowledged_after_deadline"])
@pytest.mark.parametrize("guarded_candidate", [False, True])
def test_vm_late_account_read_recovers_real_mailbox_and_reads_again(monkeypatch, isolated_redis, capsys, timed_out_method, timing, guarded_candidate):
    native_birth = 134567890123456789
    source = DUAL.Fixture(monkeypatch, account_id=ACCOUNT, environment="LIVE",
        hard_deadline_unix=None, max_seconds=0, max_requests=0, persistent_live_read_write=True,
        persistent_readonly=False, persistent_simulation_read_write=False, enable_market_rpc=True,
        read_fault_isolation=True, execution_event_guard=guarded_candidate)
    source.namespace["_native_creation_filetime"] = lambda: native_birth
    source.start()
    clients = {name: ShmReadOnlyClient(source.clients[name].name, ACCOUNT, region=DUAL.Region(source.states[name]))
               for name in ("read", "execution")}
    native = dict(native_pid=os.getpid(), native_creation_filetime=native_birth, windows_session_id=1,
                  executable="fixture-qmt.exe")
    proxy_module = sys.modules[RUNNER.RedisShmReadOnlyProxy.__module__]
    monkeypatch.setattr(proxy_module, "_windows_native_process_identity", lambda _pid: dict(native))
    proxy = RUNNER.RedisShmReadOnlyProxy(FIXTURE.RedisTransport(isolated_redis, account_id=ACCOUNT),
        clients["read"], clients["execution"], account_id=ACCOUNT, environment="LIVE", runtime_policy=POLICY,
        allow_order_methods=True, start_write_blocked=False, live_write_authorized=True, receive_policy="QUEUE_ONLY",
        previous_source_unknown_target=None if guarded_candidate else {"gateway_id": "qmt-vm-001", "profile_id": "live-stock"},
        execution_cursor_guard_target={"gateway_id": "qmt-vm-001", "profile_id": "live-stock"} if guarded_candidate else None,
        supervisor_write_gate=FIXTURE.FakeSupervisorWriteGate() if guarded_candidate else None,
        expected_readonly_session_id=clients["read"].mailbox.session_id,
        expected_execution_session_id=clients["execution"].mailbox.session_id,
        expected_native_pid=os.getpid(), expected_windows_session_id=1, timeout_seconds=1, max_requests=0)
    if guarded_candidate:
        # 本地模型模拟已建立的监督器句柄，不代表 VM 已启用该候选。
        proxy._supervisor_attach_verified = True

    def drain():
        source.namespace["aqp_shm_readonly_adjust"](source.context)
        source.namespace["aqp_shm_rpc_adjust"](source.context)

    def with_source(callback):
        with ThreadPoolExecutor(1) as pool:
            future = pool.submit(callback)
            deadline = time.monotonic() + 3
            while not future.done() and time.monotonic() < deadline:
                drain()
                time.sleep(.001)
            return future.result(timeout=.2)

    try:
        with_source(proxy.verify_identity)
        assert proxy._verified_process_identity == native
        proxy.timeout_seconds = .05
        original_send = clients["execution"].send_request
        calls = []

        def monitored_send(value, timeout_seconds, **kwargs):
            calls.append(value["request_id"])
            result = original_send(value, timeout_seconds=timeout_seconds, **kwargs)
            if timing == "acknowledged_after_deadline" and value["request_id"] == "late-real-read":
                time.sleep(.08)
            return result

        monkeypatch.setattr(clients["execution"], "send_request", monitored_send)
        queue = proxy.transport.request_queue
        original = request(timed_out_method, rid="late-real-read")
        isolated_redis.rpush(queue, FIXTURE.encode_rpc_request_payload(original))
        response = proxy.poll_once() if timing == "late_frame" else with_source(proxy.poll_once)
        assert response["status"] == "UNKNOWN" and not proxy.isolation_reason
        health_config = dict(environment="LIVE", runtime_policy=POLICY, account_id=ACCOUNT,
                             native_pid=os.getpid(), execution_session_id=clients["execution"].mailbox.session_id)
        RUNNER._emit_proxy_health(health_config, proxy)
        health = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
        if timing == "late_frame":
            assert health["status"] == "DEGRADED" and health["writes_blocked"] is False
            assert clients["execution"].poisoned and bool(proxy._pending_request_recoveries)
            assert proxy.poll_once() is None and bool(proxy._pending_request_recoveries)
            drain()
            assert proxy.poll_once() is None and not proxy._pending_request_recoveries
        else:
            assert health["status"] == "RUNNING" and not proxy._pending_request_recoveries
        assert not clients["execution"].poisoned
        assert not proxy._unconfirmed_read_ids["execution"]
        proxy.timeout_seconds = 1
        isolated_redis.rpush(queue, FIXTURE.encode_rpc_request_payload(request("get_positions", rid="after-real-read")))
        assert with_source(proxy.poll_once)["request_id"] == "after-real-read"
        assert proxy.get_cached_response("late-real-read")["status"] == "UNKNOWN"
        assert proxy.process_request(original)["status"] == "UNKNOWN"
        assert calls.count("late-real-read") == 1
        source.writes.assert_not_called()
    finally:
        proxy.close()
        source.close()


def test_identity_pending_real_redis_claim_preserves_a_changed_head(isolated_redis):
    proxy, provider, read, execution = make_proxy(provider=isolated_redis, receive_policy="QUEUE_ONLY")
    pending_identity(proxy)
    queue = proxy.transport.request_queue
    order = json.dumps(request("submit_order", rid="old-order"))
    quote = json.dumps(request("get_ticks", rid="independent-read", params={"code": "000001.SZ"}))
    injected = json.dumps(request("cancel_order", rid="other-order"))
    provider.rpush(queue, order, quote)
    saved = '{"state":"UNKNOWN","automatic_retry":false}'
    provider.hset(LEDGER, "old-order", saved)
    actual = provider.eval
    def insert(script, count, *args):
        provider.lpush(queue, injected)
        provider.eval = actual
        return actual(script, count, *args)
    provider.eval = insert
    assert proxy.poll_once() is None and provider.lrange(queue, 0, -1) == [injected, order, quote]
    assert not proxy.poll_once()["ok"] and not proxy.poll_once()["ok"]
    assert proxy.poll_once()["request_id"] == "independent-read"
    assert provider.lrange(queue, 0, -1) == [] and provider.hget(LEDGER, "old-order") == saved
    assert len(read.calls) == 2 and len(execution.calls) == 1
    original_response = provider.get("bigqmt:rpc:resp:%s:old-order" % ACCOUNT)
    provider.rpush(queue, json.dumps(request("get_ticks", rid="old-order", params={"code": "000001.SZ"})))
    assert not proxy.poll_once()["ok"]
    assert provider.get("bigqmt:rpc:resp:%s:old-order" % ACCOUNT) == original_response


def test_identity_pending_does_not_stop_pubsub_or_queue_reads():
    proxy, provider, read, execution = make_proxy(receive_policy="QUEUE_AND_PUBSUB")
    pending_identity(proxy)
    queue = proxy.transport.request_queue
    provider.queues[queue] = [json.dumps(request("get_ticks", rid="independent-quote", params={"code": "000001.SZ"}))]
    assert proxy.poll_once()["ok"] is True
    assert not provider.queues[queue] and len(read.calls) == 2 and len(execution.calls) == 1


@pytest.mark.parametrize("initially_blocked", [True, False])
def test_live_startup_identity_failure_remains_resident_without_opening_sources(monkeypatch, initially_blocked):
    stop, reported = threading.Event(), threading.Event()
    provider = mock.Mock()
    health = RUNNER._emit_proxy_health

    def capture(*args, **kwargs):
        health(*args, **kwargs)
        reported.set()

    monkeypatch.setattr(RUNNER, "windows_session_id", lambda pid: 2)
    monkeypatch.setattr(RUNNER, "_emit_proxy_health", capture)
    with ThreadPoolExecutor(1) as pool:
        value = config(start_write_blocked=initially_blocked, live_write_authorized=not initially_blocked)
        future = pool.submit(RUNNER.run_candidate, value, provider, mock.Mock(), stop_event=stop)
        try:
            assert reported.wait(2) and not future.done()
            provider.assert_not_called()
        finally:
            stop.set()
        result = future.result(timeout=2)
    assert result["status"] == "FAILED" and result["error_phase"] == "SESSION_IDENTITY_VERIFY"
    assert result["identity_verified"] is False and result["write_ready"] is False and result["writes_blocked"] is True


@pytest.mark.parametrize("phase", ["claim", "settle", "publish"])
def test_live_write_redis_uncertainty_does_not_block_fresh_business_or_replay_original(phase):
    proxy, provider, read, execution = make_proxy(execution_cursor_guard_target=None,
        supervisor_write_gate=None, receive_policy="QUEUE_ONLY", start_write_blocked=False, live_write_authorized=True)
    original = request("submit_order", rid="uncertain-" + phase)
    original["params"].pop("execution_event_guard")
    original["params"].pop("supervisor_gate_epoch")
    execution.result = {"status": "SUBMITTED", "order_sys_id": "fixture-only"}
    if phase in ("claim", "settle"):
        provider.fail_eval = phase
        result = proxy.process_request(original)
        assert result["status"] == "UNKNOWN"
        assert result["automatic_retry"] is False
    else:
        provider.fail_operation = "setex"
        with pytest.raises(Exception):
            proxy.process_request(original)
        assert json.loads(provider.hashes[LEDGER][original["request_id"]])["state"] == "PUBLISH_UNCONFIRMED"
    before = len(execution.calls)
    saved = provider.hashes.get(LEDGER, {}).get(original["request_id"])
    provider.fail_eval = provider.fail_operation = None
    assert not proxy.process_request(original)["ok"] or phase == "publish"
    assert len(execution.calls) == before
    assert proxy.process_request(request("submit_order", rid="new-after-redis-recovery"))["ok"] is True
    assert len(execution.calls) == before + 1 and not proxy.isolation_reason
    assert provider.hashes.get(LEDGER, {}).get(original["request_id"]) == saved


def test_target_live_execution_timeout_keeps_independent_quote_source_available():
    proxy, provider, read, execution = make_proxy(receive_policy="QUEUE_ONLY")
    execution.failure = FIXTURE.ShmTimeoutError("execution result unknown")
    response = proxy.process_request(request("get_positions", rid="execution-read-unknown"))
    assert response["status"] == "UNKNOWN" and response["automatic_retry"] is False
    assert not proxy.stopped and not proxy.isolation_reason
    provider.queues[proxy.transport.request_queue] = [json.dumps(request("get_ticks", rid="later-quote", params={"code": "000001.SZ"}))]
    assert proxy.poll_once()["ok"] is True
    assert len(read.calls) == 2 and len(execution.calls) == 2
