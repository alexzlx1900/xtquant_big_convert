# coding: utf-8
"""实盘候选合同；只使用内存Redis/SHM，不接触实机或共享服务。"""

import copy
from concurrent.futures import ThreadPoolExecutor
import importlib.util
import json
import os
from pathlib import Path
import sys
import threading
import time
from types import SimpleNamespace
from unittest import mock

import pytest

from test_external_redis_dual_shm_integration import DUAL


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
        value["params"].setdefault("execution_event_guard", dict(contract="EXECUTION_CURSOR_WRITE_GATE_V1",
                                   execution_session_id="execution-sid", native_pid=2852, sequence=0))
        value["params"].setdefault("supervisor_gate_epoch", dict(contract="SUPERVISOR_GATE_EPOCH_V1", epoch=1))
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
    assert proxy.stopped and len(execution.calls) == 2
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
    assert snapshot["write_ready"] is False and snapshot["write_block_reason"] == "NATIVE_SUPERVISOR_GATE_UNAVAILABLE"
    response = proxy.process_request(request("submit_order", rid="callback-unverified"))
    assert response["status"] == "REJECTED" and len(execution.calls) == 3
    assert not provider.hashes.get(proxy._write_ledger_key)


def test_unknown_supervisor_attach_result_stops_and_is_never_resent(monkeypatch):
    proxy, provider, _, execution = attach_fixture(monkeypatch)
    execution.failure = FIXTURE.ShmTimeoutError("fixture attach result unknown")
    execution.failure.publication_state = "UNKNOWN"
    with pytest.raises(FIXTURE.ShmTimeoutError):
        proxy.attach_native_supervisor()
    assert proxy.stopped and len(execution.calls) == 1
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
    assert proxy.stopped and proxy._supervisor_attach_verified is False
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
    assert proxy.stopped and len(execution.calls) == 2
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
    assert len(execution.calls) == count and proxy.identity_snapshot()["write_ready"] is False
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
    assert proxy.identity_snapshot()["write_block_reason"] == "NATIVE_SUPERVISOR_GATE_UNAVAILABLE"
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
    assert proxy.stopped and not execution.calls


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
    assert proxy.stopped and len(execution.calls) == (0 if phase == "claim" else 2)
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
            assert result["write_block_reason"] == "SUPERVISOR_ATTACH_UNCONFIRMED"
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
    assert proxy.identity_snapshot()["writes_blocked"] is True
    restarted, _, _, fresh = make_proxy(provider=provider, live_write_authorized=True, start_write_blocked=False)
    assert restarted.identity_snapshot()["writes_blocked"] is True
    assert restarted.identity_snapshot()["write_ready"] is False
    duplicate = restarted.process_request(value)
    assert duplicate["status"] == "UNKNOWN" and len(fresh.calls) == 1
    rejected = restarted.process_request(request("submit_order", rid="another-live-fixture"))
    assert rejected["ok"] is False and "blocked" in rejected["error"] and len(fresh.calls) == 1


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
        ("hgetall", "aqp:redis_shm:read_rotation_close:%s:v1" % ACCOUNT), ("hgetall", LEDGER)]
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
    assert ("read_source_state" in seen[0]) is (target is not None)
    assert result["processed_requests"] == 0
    assert all(client.closed and len(client.calls) == 1 for client in clients)
    provider.connection_pool.disconnect.assert_called_once()
