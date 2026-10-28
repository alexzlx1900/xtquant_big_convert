# coding: utf-8
"""仅使用内存 Redis/共享内存，验证分路故障及原始帧保留。"""

import copy
import ctypes
import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

import test_live_redis_shm_proxy as LIVE
import test_async_redis_shm_ledger as ASYNC
import test_dual_shm_single_file as DUAL
from test_shm_readonly import MemoryRegion, MemoryState
import bigqmt_signal_trader.redis_shm_proxy as proxy_module
from bigqmt_signal_trader.redis_shm_proxy import QUOTE_METHODS, RedisShmReadOnlyProxy
from bigqmt_signal_trader.transports.readonly_shm import (
    REQUEST_OFFSET, ReadOnlyMailbox, ShmBusyError, ShmProtocolError, ShmReadOnlyClient, ShmTimeoutError, decode_payload,
)
from bigqmt_signal_trader.transports.redis_transport import RedisTransport


FIXTURE = LIVE.FIXTURE
ACCOUNT = FIXTURE.ACCOUNT


def test_late_read_frame_is_not_acknowledged_or_resent_and_execution_stays_readable():
    name = "Local\\AQP_SHM_PROXY_FAULT_ISOLATION_TEST"
    state = MemoryState()
    server_region = MemoryRegion(state)
    server_region.pid = 2852
    server = ReadOnlyMailbox(name, create=True, region=server_region)
    server.mark_ready()
    read = ShmReadOnlyClient(name, ACCOUNT, region=MemoryRegion(state))
    execution = FIXTURE.FakeRawClient("execution")
    execution.runtime_policy = "PERSISTENT_SIMULATION_READ_WRITE"
    execution.allow_order_methods = True
    redis = FIXTURE.FakeRedis()
    proxy = RedisShmReadOnlyProxy(
        RedisTransport(redis, account_id=ACCOUNT), read, execution,
        expected_readonly_session_id=server.session_id, expected_execution_session_id="execution-sid",
        expected_native_pid=2852, expected_windows_session_id=1, timeout_seconds=0.1, max_requests=0,
        runtime_policy="PERSISTENT_SIMULATION_READ_WRITE", allow_order_methods=True,
        execution_cursor_guard_target={"gateway_id": "qmt-vm-001", "profile_id": "sim-bigqmt"},
        supervisor_write_gate=FIXTURE.FakeSupervisorWriteGate(),
    )
    proxy._supervisor_attach_verified = True  # 本地原生回调模型，不代表真实 ATTACH。
    errors = []

    def serve_identity():
        try:
            until = time.monotonic() + 2
            item = None
            while item is None and time.monotonic() < until:
                item = server.read_request()
                time.sleep(0.001)
            assert item is not None
            sequence, request = item
            data = dict(pong=True, account_id=ACCOUNT, allow_order_methods=False,
                        session_id=server.session_id, native_pid=2852, windows_session_id=1,
                        transport="shm_readonly", runtime_policy="PERSISTENT_READONLY",
                        source_deadline_unix=None, max_seconds=0, max_requests=0,
                        capabilities=sorted(QUOTE_METHODS))
            assert server.publish_response(sequence, dict(
                schema_version=1, request_id=request["request_id"], account_id=ACCOUNT,
                method="ping", session_id=server.session_id, sequence=sequence,
                ok=True, data=data, error="", server_error="", handled_at="2026-10-28T00:00:00"))
        except Exception as exc:
            errors.append(exc)

    worker = threading.Thread(target=serve_identity)
    worker.start()
    try:
        proxy.verify_identity()
        worker.join(timeout=2)
        assert not worker.is_alive() and not errors
        before = server._header()
        original = FIXTURE.request("get_ticks", rid="original-read", params={"code": "000001.SZ"})
        response = proxy.process_request(original)
        pending = server._header()
        assert response["status"] == "UNKNOWN" and response["automatic_retry"] is False
        assert response["publication_state"] == "UNKNOWN" and response["publication_scope"] == "SHM"
        assert response["sequence"] == pending[4]
        assert pending[4] == before[4] + 1 and pending[5:7] == before[5:7]
        original_frame = server_region.read(REQUEST_OFFSET, pending[7])
        frame = decode_payload(original_frame)
        assert frame["request_id"] == original["request_id"]
        assert frame["sequence"] == pending[4] and frame["session_id"] == server.session_id
        assert read.poisoned is True and proxy._clients["readonly"] is read
        assert proxy.process_request(original) == response
        assert not proxy.process_request(FIXTURE.request("get_ticks", rid="new-read",
                                                       params={"code": "000001.SZ"}))["ok"]

        # 即使 native 迟到发布原请求的响应，也不回填 confirmed 或再次执行。
        assert server.publish_response(pending[4], dict(
            schema_version=1, request_id=original["request_id"], account_id=ACCOUNT,
            method="get_ticks", session_id=server.session_id, sequence=pending[4],
            ok=True, data={"000001.SZ": {"lastPrice": 10}}, error="", server_error="",
            handled_at="2026-10-28T00:00:01"))
        late = server._header()
        assert late[4] == late[5] == pending[4] and late[6] == before[6]
        assert server_region.read(REQUEST_OFFSET, pending[7]) == original_frame
        assert proxy.process_request(original) == response

        status = proxy.process_request(FIXTURE.request("ping", rid="execution-ping"))
        assert status["ok"] and status["data"]["read_source_state"] == "RESULT_UNKNOWN"
        assert status["data"]["execution_source_state"] == "HEALTHY"
        assert status["data"]["write_ready"] is False
        assert status["data"]["write_block_reason"] == "READ_SOURCE_UNTRUSTED"
        assert proxy.process_request(FIXTURE.request("get_positions", rid="positions"))["ok"]
        write = FIXTURE.request("submit_order", rid="write-after-read-fault")
        write["expected_source_binding"] = proxy.identity_snapshot()["source_binding"]
        assert proxy.process_request(write)["status"] == "REJECTED"
        assert not redis.hashes and all(call[0]["method"] != "submit_order" for call in execution.calls)
        assert server._header()[4:7] == late[4:7]
    finally:
        worker.join(timeout=2)
        proxy.close()
        server.close()


@pytest.mark.parametrize("previous_unconfirmed", [False, True, None])
def test_not_sent_requires_exact_structured_evidence_and_keeps_prior_unknown(previous_unconfirmed):
    proxy, _, read, execution = FIXTURE.RedisShmReadOnlyProxyTest().make_proxy(
        policy="PERSISTENT_SIMULATION_READ_WRITE")
    original_send = read.send_request

    def fail_before_send(request, timeout_seconds):
        error = ShmBusyError("before publication") if previous_unconfirmed is True else ShmTimeoutError("before publication")
        error.publication_state = "NOT_SENT"
        error.request_id = request["request_id"]
        error.session_id = read.mailbox.session_id
        error.sequence = None
        error.previous_unconfirmed = previous_unconfirmed
        raise error

    read.send_request = fail_before_send
    request = FIXTURE.request("get_ticks", rid="not-sent", params={"code": "000001.SZ"})
    response = proxy.process_request(request)
    assert response["status"] == "REJECTED" and response["server_error"] == "SHM_NOT_SENT"
    assert response["publication_state"] == "NOT_SENT" and response["publication_scope"] == "SHM"
    assert response["automatic_retry"] is False
    assert response["previous_unconfirmed"] is previous_unconfirmed
    assert proxy.identity_snapshot()["read_source_state"] == ("UNKNOWN" if previous_unconfirmed is False else "RESULT_UNKNOWN")
    assert proxy.identity_snapshot()["write_ready"] is (previous_unconfirmed is False)
    if previous_unconfirmed is not False:
        assert proxy.identity_snapshot()["read_source_fault"]["previous_unconfirmed"] is previous_unconfirmed
    read.send_request = original_send
    assert proxy.process_request(request) == response
    next_read = proxy.process_request(FIXTURE.request("get_ticks", rid="next-read", params={"code": "000001.SZ"}))
    assert next_read["ok"] is (previous_unconfirmed is False)
    assert proxy.process_request(FIXTURE.request("ping", rid="execution-ping"))["ok"]
    assert all(call[0]["method"] != "submit_order" for call in execution.calls)


def test_claimed_not_sent_with_mismatched_evidence_is_unknown_and_faults_only_its_route():
    proxy, _, read, _ = FIXTURE.RedisShmReadOnlyProxyTest().make_proxy(
        policy="PERSISTENT_SIMULATION_READ_WRITE")
    error = ShmTimeoutError("untrusted evidence")
    error.publication_state, error.request_id, error.session_id = "NOT_SENT", "other-rid", "readonly-sid"
    error.sequence, error.previous_unconfirmed = 23, False
    read.failure = error
    response = proxy.process_request(FIXTURE.request("get_ticks", rid="actual-rid", params={"code": "000001.SZ"}))
    assert response["status"] == "UNKNOWN" and response["publication_state"] == "UNKNOWN"
    assert "sequence" not in response
    assert proxy.identity_snapshot()["read_source_state"] == "RESULT_UNKNOWN"
    assert proxy.process_request(FIXTURE.request("ping", rid="execution-ping"))["ok"]


@pytest.mark.parametrize("case", ["healthy", "gap", "lost", "missing_account", "session", "pid", "windows_session",
                                  "environment", "cursor", "contradictory_gap", "event_identity", "missing_event"])
def test_target_execution_event_cursor_is_verified_and_loss_is_sticky(case):
    proxy, provider, _, execution = FIXTURE.RedisShmReadOnlyProxyTest().make_proxy(
        policy="PERSISTENT_SIMULATION_READ_WRITE")
    event = dict(sequence=1, session_id="execution-sid", account_id=ACCOUNT,
                 event=dict(event_type="data_loss", data_loss=True, account_id=ACCOUNT))
    data = dict(events=[event], session_id="execution-sid", native_pid=2852, windows_session_id=1,
                environment="SIM", account_id=ACCOUNT, latest_sequence=1, oldest_sequence=1,
                dropped_before_sequence=0, lost_sequence=1, gap=True, automatic_retry=False)
    after = 0
    if case == "healthy":
        data.update(events=[], latest_sequence=0, oldest_sequence=1, lost_sequence=0, gap=False)
    elif case == "lost":
        after = 1
        data.update(events=[], gap=False)  # 当前游标已过缺口，历史丢失仍阻断写。
    elif case == "missing_account":
        data.pop("account_id")
    elif case == "session":
        data["session_id"] = "other-session"
    elif case == "pid":
        data["native_pid"] = True
    elif case == "windows_session":
        data["windows_session_id"] = 2
    elif case == "environment":
        data["environment"] = "LIVE"
    elif case == "cursor":
        data["latest_sequence"] = 0
    elif case == "contradictory_gap":
        data["gap"] = False
    elif case == "event_identity":
        data["events"][0]["session_id"] = "other-session"
    elif case == "missing_event":
        data["events"] = []
    execution.result = data
    response = proxy.process_request(FIXTURE.request("poll_execution_events", rid="event-state-" + case,
                                                      params={"after_sequence": after}))
    if case == "healthy":
        assert response["ok"] and proxy.identity_snapshot()["write_ready"] is True
    elif case in ("gap", "lost"):
        assert response["ok"] and "EXECUTION_EVENT_GAP" in proxy._write_block_reasons
        assert proxy.identity_snapshot()["write_ready"] is False
    else:
        assert response["status"] == "UNKNOWN" and response["server_error"] == "EXECUTION_EVENT_STATE_UNKNOWN"
        assert "EXECUTION_EVENT_STATE_UNKNOWN" in proxy._write_block_reasons
        assert proxy.identity_snapshot()["write_ready"] is False
    assert not provider.hashes.get(proxy._write_ledger_key)
    assert all(call[0]["method"] not in proxy_module.ALL_WRITE_METHODS for call in execution.calls)


def test_legacy_other_instance_keeps_existing_event_poll_contract():
    proxy, _, _, execution = FIXTURE.RedisShmReadOnlyProxyTest().make_proxy(
        policy="PERSISTENT_SIMULATION_READ_WRITE", target=False)
    execution.result = {"gap": True, "lost_sequence": 1}  # 旧协议不要求新的来源字段。
    response = proxy.process_request(FIXTURE.request("poll_execution_events", rid="legacy-event",
                                                      params={"after_sequence": 0}))
    assert response["ok"] and response["data"] == execution.result
    assert proxy.identity_snapshot()["write_ready"] is True


@pytest.mark.parametrize("change", ["sid", "response-rid", "response-account"])
def test_read_identity_or_protocol_fault_blocks_writes_without_stopping_execution(change):
    proxy, _, read, execution = FIXTURE.RedisShmReadOnlyProxyTest().make_proxy(
        policy="PERSISTENT_SIMULATION_READ_WRITE")
    if change == "sid":
        read.mailbox.session_id = "changed-read-sid"
    else:
        read.change_response = {"request_id" if change == "response-rid" else "account_id": "unexpected"}
    response = proxy.process_request(FIXTURE.request("get_ticks", rid="broken-read", params={"code": "000001.SZ"}))
    assert response["ok"] is False
    assert response["status"] == ("REJECTED" if change == "sid" else "UNKNOWN")
    assert proxy.identity_snapshot()["read_source_state"] == "IDENTITY_OR_PROTOCOL_ERROR"
    assert proxy.identity_snapshot()["write_ready"] is False and not proxy.stopped
    assert proxy.process_request(FIXTURE.request("ping", rid="execution-ping"))["ok"]
    assert all(call[0]["method"] != "submit_order" for call in execution.calls)


def test_read_fault_does_not_exhaust_other_lane_capacity():
    proxy, _, read, _ = FIXTURE.RedisShmReadOnlyProxyTest().make_proxy(
        policy="PERSISTENT_SIMULATION_READ_WRITE", read_capacity=1)
    read.failure = ShmTimeoutError("unknown")
    response = proxy.process_request(FIXTURE.request("get_ticks", rid="unknown-read", params={"code": "000001.SZ"}))
    assert response["status"] == "UNKNOWN"
    assert proxy.process_request(FIXTURE.request("ping", rid="execution-ping"))["ok"]


def test_empty_events_and_login_result_do_not_clear_read_fault_write_gate():
    proxy, _, read, execution = FIXTURE.RedisShmReadOnlyProxyTest().make_proxy(
        policy="PERSISTENT_SIMULATION_READ_WRITE")
    read.failure = ShmTimeoutError("unknown")
    proxy.process_request(FIXTURE.request("get_ticks", rid="unknown-read", params={"code": "000001.SZ"}))
    execution.result = dict(events=[], gap=False, latest_sequence=0, oldest_sequence=1, lost_sequence=0,
                            account_id=ACCOUNT, session_id="execution-sid", native_pid=2852,
                            windows_session_id=1, environment="SIM", dropped_before_sequence=0, automatic_retry=False)
    assert proxy.process_request(FIXTURE.request("poll_execution_events", rid="empty-events",
                                               params={"after_sequence": 0}))["ok"]
    execution.result = dict(status="READY", positive_contract_verified=True, account_id=ACCOUNT,
                            session_id="execution-sid", native_pid=2852)
    assert proxy.process_request(FIXTURE.request("get_trade_login_state", rid="login"))["ok"]
    assert proxy.identity_snapshot()["write_ready"] is False
    assert proxy.identity_snapshot()["write_block_reason"] == "READ_SOURCE_UNTRUSTED"


def test_native_fault_observation_requires_dual_contract_and_never_resets_write_gate():
    proxy, _, read, execution = FIXTURE.RedisShmReadOnlyProxyTest().make_proxy(
        verify=False, policy="PERSISTENT_SIMULATION_READ_WRITE")
    read_send, execution_send = read.send_request, execution.send_request
    native_state = ["UNKNOWN"]

    def read_with_contract(request, timeout_seconds):
        response = read_send(request, timeout_seconds)
        if request["method"] == "ping":
            response["data"].update(environment="SIM", read_fault_isolation_contract="READ_FAILURE_ISOLATION_V1")
        return response

    def execution_with_contract(request, timeout_seconds):
        response = execution_send(request, timeout_seconds)
        if request["method"] == "ping":
            response["data"].update(environment="SIM", read_fault_isolation_contract="READ_FAILURE_ISOLATION_V1",
                                    native_read_source_state=native_state[0], native_read_session_id="readonly-sid")
        return response

    read.send_request, execution.send_request = read_with_contract, execution_with_contract
    proxy._supervisor_attach_verified = True  # 模型预置已验收门，只验证只读故障。
    proxy.verify_identity()
    initial = proxy.identity_snapshot()
    assert initial["read_fault_isolation_contract"] == "READ_FAILURE_ISOLATION_V1"
    assert initial["shm_publication_evidence_contract"] == "SHM_PUBLICATION_STATE_V1"
    assert initial["read_source_state"] == "UNKNOWN"
    proxy._last_execution_success_at -= 61
    assert proxy.identity_snapshot()["execution_source_state"] == "UNKNOWN"
    assert proxy.process_request(FIXTURE.request("ping", rid="fresh-ping"))["data"]["execution_source_state"] == "HEALTHY"
    assert proxy.process_request(FIXTURE.request("get_ticks", rid="positive-read", params={"code": "000001.SZ"}))["ok"]
    assert proxy.identity_snapshot()["read_source_state"] == "HEALTHY"
    proxy._last_read_success_at -= 61
    assert proxy.identity_snapshot()["read_source_state"] == "UNKNOWN"
    native_state[0] = "RESULT_UNKNOWN"
    status = proxy.process_request(FIXTURE.request("ping", rid="native-fault-ping"))
    assert status["ok"] and status["data"]["read_source_state"] == "RESULT_UNKNOWN"
    assert status["data"]["write_ready"] is False and status["data"]["write_block_reason"] == "READ_SOURCE_UNTRUSTED"
    native_state[0] = "UNKNOWN"
    assert proxy.process_request(FIXTURE.request("ping", rid="later-ping"))["data"]["read_source_state"] == "RESULT_UNKNOWN"
    assert proxy.identity_snapshot()["write_ready"] is False


def test_old_native_without_fault_contract_remains_unknown_after_successful_read():
    proxy, _, _, _ = FIXTURE.RedisShmReadOnlyProxyTest().make_proxy(
        policy="PERSISTENT_SIMULATION_READ_WRITE")
    assert proxy.process_request(FIXTURE.request("get_ticks", rid="legacy-read", params={"code": "000001.SZ"}))["ok"]
    identity = proxy.identity_snapshot()
    assert identity["read_source_state"] == "UNKNOWN"
    assert "read_fault_isolation_contract" not in identity


def test_reconciliation_does_not_clear_a_later_read_fault_write_gate():
    fixture = FIXTURE.RedisShmReadOnlyProxyTest()
    proxy, _, _, original, identity = fixture._unknown_reconciliation()
    read = proxy._clients["readonly"]
    read.result = {"status": "UNKNOWN", "error": "source data is incomplete"}
    quote = FIXTURE.request("get_ticks", rid="unknown-quote", params={"code": "000001.SZ"})
    assert proxy.process_request(quote)["status"] == "UNKNOWN"
    read.result = None
    preview = proxy.reconcile_unknown_write(original["request_id"], "1258", identity)
    result = proxy.reconcile_unknown_write(original["request_id"], "1258", identity,
        expected_record=preview["expected_record"], operator="fixture", apply=True)
    assert result["status"] == "RECONCILED" and result["writes_blocked"] is True
    assert result["write_block_reason"] == "READ_SOURCE_UNTRUSTED"
    assert proxy.identity_snapshot()["write_ready"] is False


def test_read_fault_does_not_modify_another_account_proxy_or_its_ledger():
    provider = FIXTURE.FakeRedis()
    live, _, live_read, live_execution = LIVE.make_proxy(
        provider=provider, start_write_blocked=False, live_write_authorized=True)
    sim, _, _, sim_execution = FIXTURE.RedisShmReadOnlyProxyTest().make_proxy(
        redis=provider, policy="PERSISTENT_SIMULATION_READ_WRITE")
    before = copy.deepcopy(provider.hashes)
    live_read.failure = ShmTimeoutError("outcome unconfirmed")
    response = live.process_request(LIVE.request("get_ticks", rid="live-read-fault", params={"code": "000001.SZ"}))
    assert response["status"] == "UNKNOWN" and live.identity_snapshot()["write_ready"] is False
    assert sim.identity_snapshot()["write_ready"] is True
    assert sim.process_request(FIXTURE.request("get_positions", rid="sim-independent"))["ok"]
    assert provider.hashes == before
    assert all(call[0]["method"] != "submit_order" for call in live_execution.calls + sim_execution.calls)


@pytest.mark.parametrize("stage", ["PREPARE_PENDING", "PREPARED", "COMMIT_PENDING", "COMMITTED",
                                   "SUPERVISOR_VERIFIED", "ACTIVATE_PENDING", "ACTIVATED", "GATEWAY_VERIFYING",
                                   "GATEWAY_VERIFIED"])
def test_existing_rotation_record_survives_restart_and_keeps_writes_blocked(stage):
    provider = FIXTURE.FakeRedis()
    key = "aqp:redis_shm:read_rotation:%s:v1" % ACCOUNT
    transition_id = "a" * 32
    original = dict(schema_version=1, transition_id=transition_id, account_id=ACCOUNT,
                    environment="SIM", state=stage, automatic_retry=False)
    provider.hashes[key] = {transition_id: json.dumps(original)}
    before = copy.deepcopy(provider.hashes)
    proxy, _, _, execution = FIXTURE.RedisShmReadOnlyProxyTest().make_proxy(
        redis=provider, policy="PERSISTENT_SIMULATION_READ_WRITE")
    assert proxy.identity_snapshot()["write_ready"] is False
    assert proxy.identity_snapshot()["write_block_reason"] == "READ_ROTATION_UNCONFIRMED"
    assert proxy.process_request(FIXTURE.request("submit_order", rid="blocked-write"))["status"] == "REJECTED"
    assert len(execution.calls) == 1 and provider.hashes == before
    assert not any(op[0] in ("delete", "expire", "eval") and key in str(op) for op in provider.operations)


@pytest.mark.parametrize("control", ["read_rotation", "read_rotation_control_status", "supervisor_attach",
                                     "supervisor_attach_status", "read_rotation_capability", "read_rotation_capability_status"])
def test_redis_ping_cannot_invoke_native_rotation_control(control):
    proxy, _, _, execution = FIXTURE.RedisShmReadOnlyProxyTest().make_proxy(
        policy="PERSISTENT_SIMULATION_READ_WRITE")
    response = proxy.process_request(FIXTURE.request("ping", rid="external-control",
                                                   params={control: {"operation": "PREPARE"}}))
    assert response["status"] == "REJECTED" and len(execution.calls) == 1
    assert not proxy.stopped


def test_rotation_claim_is_nonexpiring_and_cas_retains_original_evidence_on_restart():
    proxy, provider, _, execution = FIXTURE.RedisShmReadOnlyProxyTest().make_proxy(
        policy="PERSISTENT_SIMULATION_READ_WRITE")
    original = dict(schema_version=1, transition_id="b" * 32, account_id=ACCOUNT, environment="SIM",
                    state="PREPARE_PENDING", automatic_retry=False,
                    old_read_session_id="readonly-sid", old_request_id="old-rid", old_sequence=7)
    encoded = proxy._claim_read_rotation(original)
    key = proxy._read_rotation_key
    assert provider.ttl(key) == -1 and json.loads(provider.hashes[key][original["transition_id"]]) == original
    prepared = dict(original, state="PREPARED", candidate_read_session_id="new-read-sid")
    proxy._advance_read_rotation(encoded, prepared)
    before = copy.deepcopy(provider.hashes)
    with pytest.raises(Exception, match="compare-and-set"):
        proxy._advance_read_rotation(encoded, dict(original, state="PREPARED", candidate_read_session_id="other-sid"))
    assert proxy.stopped and provider.hashes == before and len(execution.calls) == 1
    restarted, _, _, _ = FIXTURE.RedisShmReadOnlyProxyTest().make_proxy(
        redis=provider, policy="PERSISTENT_SIMULATION_READ_WRITE")
    assert restarted.identity_snapshot()["write_ready"] is False
    assert provider.hashes == before


def test_lost_rotation_claim_reply_stops_without_native_control_or_retry():
    proxy, provider, _, execution = FIXTURE.RedisShmReadOnlyProxyTest().make_proxy(
        policy="PERSISTENT_SIMULATION_READ_WRITE")
    original_eval = provider.eval

    def claim_then_lose_reply(*args):
        original_eval(*args)
        raise RuntimeError("claim reply lost")

    provider.eval = claim_then_lose_reply
    record = dict(schema_version=1, transition_id="c" * 32, account_id=ACCOUNT, environment="SIM",
                  state="PREPARE_PENDING", automatic_retry=False)
    with pytest.raises(RuntimeError, match="reply lost"):
        proxy._claim_read_rotation(record)
    assert proxy.stopped and len(execution.calls) == 1
    provider.eval = original_eval
    restarted, _, _, _ = FIXTURE.RedisShmReadOnlyProxyTest().make_proxy(
        redis=provider, policy="PERSISTENT_SIMULATION_READ_WRITE")
    assert restarted.identity_snapshot()["write_ready"] is False
    assert json.loads(provider.hashes[proxy._read_rotation_key][record["transition_id"]]) == record


def _retained_rotation_history(proxy, provider, *, transition="a" * 32, closed=True):
    """直接构造已验收证据，仅用于验证历史记录原子准入，不代表可信进程认证。"""
    record = dict(schema_version=1, transition_id=transition, account_id=ACCOUNT,
                  environment="SIM", state="GATEWAY_VERIFIED", automatic_retry=False)
    raw = json.dumps(record, sort_keys=True, separators=(",", ":"))
    provider.hashes[proxy._read_rotation_key] = {transition: raw}
    proxy._accepted_rotation_record = dict(transition_id=transition, raw=raw)
    proxy._accepted_rotation_records[transition] = raw
    proxy._rotation_open_delivery, proxy._rotation_open_acknowledged = "CONFIRMED", True
    close_raw = None
    if closed:
        close = dict(transition_id=transition, account_id=ACCOUNT, environment="SIM",
                     state="CLOSED", automatic_retry=False)
        close_raw = json.dumps(close, sort_keys=True, separators=(",", ":"))
        provider.hashes[proxy._read_rotation_close_key] = {transition: close_raw}
        proxy._rotation_closed_records[transition] = close_raw
    return transition, raw, close_raw


def test_next_rotation_claim_atomically_preserves_prior_accepted_and_closed_evidence():
    proxy, provider, _, execution = FIXTURE.RedisShmReadOnlyProxyTest().make_proxy(
        policy="PERSISTENT_SIMULATION_READ_WRITE")
    transition, raw, close_raw = _retained_rotation_history(proxy, provider)
    next_record = dict(schema_version=1, transition_id="b" * 32, account_id=ACCOUNT, environment="SIM",
                       state="PREPARE_PENDING", automatic_retry=False)
    saved = proxy._claim_read_rotation(next_record)
    assert provider.hashes[proxy._read_rotation_key] == {transition: raw, next_record["transition_id"]: saved}
    assert provider.hashes[proxy._read_rotation_close_key] == {transition: close_raw}
    assert provider.ttl(proxy._read_rotation_key) == provider.ttl(proxy._read_rotation_close_key) == -1
    assert len(execution.calls) == 1 and proxy.identity_snapshot()["write_ready"] is False


@pytest.mark.parametrize("change", ["unclosed", "delivery_unknown", "old_record", "old_close", "extra_close", "ttl"])
def test_next_rotation_rejects_incomplete_or_changed_history_without_native_control(change):
    proxy, provider, _, execution = FIXTURE.RedisShmReadOnlyProxyTest().make_proxy(
        policy="PERSISTENT_SIMULATION_READ_WRITE")
    transition, _, _ = _retained_rotation_history(proxy, provider, closed=change != "unclosed")
    if change == "delivery_unknown":
        proxy._rotation_open_delivery = "UNKNOWN"
    elif change == "old_record":
        provider.hashes[proxy._read_rotation_key][transition] += " "
    elif change == "old_close":
        provider.hashes[proxy._read_rotation_close_key][transition] += " "
    elif change == "extra_close":
        provider.hashes[proxy._read_rotation_close_key]["f" * 32] = "{}"
    elif change == "ttl":
        provider.ttl = lambda key: 60 if key == proxy._read_rotation_close_key else -1
    before = copy.deepcopy(provider.hashes)
    next_record = dict(schema_version=1, transition_id="b" * 32, account_id=ACCOUNT, environment="SIM",
                       state="PREPARE_PENDING", automatic_retry=False)
    with pytest.raises(ShmProtocolError):
        proxy._claim_read_rotation(next_record)
    assert provider.hashes == before and len(execution.calls) == 1


def test_rotation_capacity_stops_before_claim_without_removing_history():
    proxy, provider, _, execution = FIXTURE.RedisShmReadOnlyProxyTest().make_proxy(
        policy="PERSISTENT_SIMULATION_READ_WRITE")
    _retained_rotation_history(proxy, provider)
    proxy.read_cache_capacity = 1
    before = copy.deepcopy(provider.hashes)
    record = dict(schema_version=1, transition_id="b" * 32, account_id=ACCOUNT, environment="SIM",
                  state="PREPARE_PENDING", automatic_retry=False)
    with pytest.raises(ShmProtocolError, match="READ_ROTATION_CAPACITY_BLOCKED"):
        proxy._claim_read_rotation(record)
    assert not proxy.stopped and provider.hashes == before and len(execution.calls) == 1


def test_restart_cannot_rely_on_closed_redis_labels_as_process_acceptance():
    proxy, provider, _, _ = FIXTURE.RedisShmReadOnlyProxyTest().make_proxy(
        policy="PERSISTENT_SIMULATION_READ_WRITE")
    _retained_rotation_history(proxy, provider)
    before = copy.deepcopy(provider.hashes)
    restarted, _, read, execution = FIXTURE.RedisShmReadOnlyProxyTest().make_proxy(
        redis=provider, policy="PERSISTENT_SIMULATION_READ_WRITE")
    assert restarted.identity_snapshot()["write_ready"] is False
    assert restarted.identity_snapshot()["write_block_reason"] == "READ_ROTATION_UNCONFIRMED"
    assert not restarted._accepted_rotation_records and not restarted._rotation_closed_records
    assert provider.hashes == before and len(execution.calls) == 1
    assert restarted.process_request(FIXTURE.request("get_ticks", rid="restart-read", params={"code": "000001.SZ"}))["status"] == "REJECTED"
    assert len(read.calls) == 1 and restarted.process_request(FIXTURE.request("ping", rid="restart-execution"))["ok"]


def test_current_read_rejects_tampered_closed_history_before_new_shm_request(monkeypatch):
    proxy, provider, read, _ = FIXTURE.RedisShmReadOnlyProxyTest().make_proxy(
        policy="PERSISTENT_SIMULATION_READ_WRITE")
    transition, _, _ = _retained_rotation_history(proxy, provider)
    monkeypatch.setattr(proxy, "_check_native_process_identity", lambda: None)
    provider.hashes[proxy._read_rotation_close_key][transition] += " "
    response = proxy.process_request(FIXTURE.request("get_ticks", rid="history-changed", params={"code": "000001.SZ"}))
    assert response["status"] == "REJECTED" and response["publication_state"] == "NOT_SENT"
    assert len(read.calls) == 1 and proxy._rotation_integrity_fault
    assert proxy._rotation_waiting_gateway and proxy.identity_snapshot()["write_ready"] is False


@pytest.mark.parametrize("change", ["current", "predecessor", "predecessor_unclosed", "older"])
def test_read_checks_current_and_predecessor_but_full_history_still_gates_next_rotation(monkeypatch, change):
    proxy, provider, read, _ = FIXTURE.RedisShmReadOnlyProxyTest().make_proxy(
        policy="PERSISTENT_SIMULATION_READ_WRITE")
    monkeypatch.setattr(proxy, "_check_native_process_identity", lambda: None)
    originals, closures = {}, {}
    for character in ("a", "b", "c"):
        transition = character * 32
        original = dict(schema_version=1, transition_id=transition, account_id=ACCOUNT,
                        environment="SIM", state="GATEWAY_VERIFIED", automatic_retry=False)
        originals[transition] = json.dumps(original, sort_keys=True, separators=(",", ":"))
        if character != "c":
            closure = dict(transition_id=transition, account_id=ACCOUNT, environment="SIM",
                           state="CLOSED", automatic_retry=False)
            closures[transition] = json.dumps(closure, sort_keys=True, separators=(",", ":"))
    provider.hashes[proxy._read_rotation_key] = dict(originals)
    provider.hashes[proxy._read_rotation_close_key] = dict(closures)
    proxy._accepted_rotation_records = dict(originals)
    proxy._rotation_closed_records = dict(closures)
    proxy._accepted_rotation_record = dict(transition_id="c" * 32, raw=originals["c" * 32])
    proxy._rotation_open_delivery, proxy._rotation_open_acknowledged = "CONFIRMED", True
    proxy._block_writes("NATIVE_WRITE_RESULT_UNKNOWN")
    if change == "current":
        provider.hashes[proxy._read_rotation_key]["c" * 32] += " "
    elif change == "predecessor":
        provider.hashes[proxy._read_rotation_close_key]["b" * 32] += " "
    elif change == "predecessor_unclosed":
        del provider.hashes[proxy._read_rotation_close_key]["b" * 32]
        del proxy._rotation_closed_records["b" * 32]
    else:
        provider.hashes[proxy._read_rotation_key]["a" * 32] += " "
    response = proxy.process_request(FIXTURE.request("get_ticks", rid="direct-dependency-read",
                                                     params={"code": "000001.SZ"}))
    assert "NATIVE_WRITE_RESULT_UNKNOWN" in proxy._write_block_reasons
    if change == "older":
        assert response["ok"] is True and len(read.calls) == 2
        close = dict(transition_id="c" * 32, account_id=ACCOUNT, environment="SIM", state="CLOSED", automatic_retry=False)
        close_raw = json.dumps(close, sort_keys=True, separators=(",", ":"))
        provider.hashes[proxy._read_rotation_close_key]["c" * 32] = close_raw
        proxy._rotation_closed_records["c" * 32] = close_raw
        next_record = dict(schema_version=1, transition_id="d" * 32, account_id=ACCOUNT, environment="SIM",
                           state="PREPARE_PENDING", automatic_retry=False)
        with pytest.raises(ShmProtocolError, match="claim or persistence"):
            proxy._claim_read_rotation(next_record)
        assert "d" * 32 not in provider.hashes[proxy._read_rotation_key]
    else:
        assert response["status"] == "REJECTED" and response["publication_state"] == "NOT_SENT"
        assert len(read.calls) == 1 and proxy._rotation_integrity_fault


def test_expiring_rotation_record_is_not_accepted_on_restart():
    provider = FIXTURE.FakeRedis()
    record = dict(schema_version=1, transition_id="d" * 32, account_id=ACCOUNT, environment="SIM",
                  state="PREPARE_PENDING", automatic_retry=False)
    provider.hashes["aqp:redis_shm:read_rotation:%s:v1" % ACCOUNT] = {record["transition_id"]: json.dumps(record)}
    provider.ttl = lambda key: 60
    with pytest.raises(Exception, match="must not expire"):
        FIXTURE.RedisShmReadOnlyProxyTest().make_proxy(redis=provider, policy="PERSISTENT_SIMULATION_READ_WRITE")


def test_rotation_record_created_after_handshake_is_checked_before_a_new_write():
    proxy, provider, _, execution = FIXTURE.RedisShmReadOnlyProxyTest().make_proxy(
        policy="PERSISTENT_SIMULATION_READ_WRITE")
    record = dict(schema_version=1, transition_id="e" * 32, account_id=ACCOUNT, environment="SIM",
                  state="COMMIT_PENDING", automatic_retry=False)
    provider.hashes[proxy._read_rotation_key] = {record["transition_id"]: json.dumps(record)}
    result = proxy.process_request(FIXTURE.request("submit_order", rid="after-external-rotation"))
    assert result["status"] == "REJECTED" and result["publication_state"] == "NOT_SENT"
    assert proxy.identity_snapshot()["write_ready"] is False
    assert len(execution.calls) == 1 and proxy._write_ledger_key not in provider.hashes


def test_rotation_gate_read_failure_stops_writes_without_native_call():
    proxy, provider, _, execution = FIXTURE.RedisShmReadOnlyProxyTest().make_proxy(
        policy="PERSISTENT_SIMULATION_READ_WRITE")
    provider.fail_operation = "hgetall"
    result = proxy.process_request(FIXTURE.request("submit_order", rid="unreadable-rotation-gate"))
    assert result["status"] == "REJECTED" and result["publication_state"] == "NOT_SENT"
    assert proxy.identity_snapshot()["write_ready"] is False and len(execution.calls) == 1
    provider.fail_operation = None
    result = proxy.process_request(FIXTURE.request("submit_order", rid="later-write"))
    assert result["status"] == "REJECTED" and len(execution.calls) == 1
    assert proxy._write_ledger_key not in provider.hashes


def test_rotation_gate_read_failure_at_start_never_marks_proxy_verified():
    provider = FIXTURE.FakeRedis()
    proxy, _, _, execution = FIXTURE.RedisShmReadOnlyProxyTest().make_proxy(
        verify=False, redis=provider, policy="PERSISTENT_SIMULATION_READ_WRITE")
    provider.fail_operation = "hgetall"
    with pytest.raises(RuntimeError):
        proxy.verify_identity()
    assert proxy.verified is False and proxy.stopped
    assert len(execution.calls) == 1 and proxy._write_ledger_key not in provider.hashes


def test_generated_native_default_has_no_rotation_authority_or_transition_side_effect(monkeypatch):
    native = DUAL.Fixture(monkeypatch, **DUAL.persistent_settings(read_fault_isolation=True))
    creation = 134567890123456789
    native.namespace["_native_creation_filetime"] = lambda: creation
    native.start()
    identity = dict(native_pid=os.getpid(), native_creation_filetime=creation,
                    windows_session_id=1, executable="fixture-native")
    monkeypatch.setattr(proxy_module, "_windows_native_process_identity", lambda pid: dict(identity))
    read, execution = native.clients["read"], native.clients["execution"]
    redis = FIXTURE.FakeRedis()
    proxy = RedisShmReadOnlyProxy(
        RedisTransport(redis, account_id=ACCOUNT), read, execution,
        expected_readonly_session_id=read.mailbox.session_id,
        expected_execution_session_id=execution.mailbox.session_id, expected_native_pid=os.getpid(),
        expected_windows_session_id=1, timeout_seconds=0.1, max_requests=0,
        runtime_policy="PERSISTENT_READONLY", allow_order_methods=False,
        execution_cursor_guard_target={"gateway_id": "qmt-vm-001", "profile_id": "sim-bigqmt"},
    )

    def drive(function):
        with ThreadPoolExecutor(1) as pool:
            future = pool.submit(function)
            until = time.monotonic() + 2
            while not future.done() and time.monotonic() < until:
                native.namespace["aqp_shm_readonly_adjust"](native.context)
                native.namespace["aqp_shm_rpc_adjust"](native.context)
                time.sleep(0.001)
            return future.result(timeout=0.2)

    try:
        drive(proxy.verify_identity)
        original = FIXTURE.request("get_ticks", rid="old-no-authority", params={"code": "000001.SZ"})
        result = proxy.process_request(original)
        assert result["status"] == "UNKNOWN"
        native.namespace["aqp_shm_readonly_adjust"](native.context)
        old_frame = bytes(native.states["read"].data)
        old_execution_sid = execution.mailbox.session_id
        with pytest.raises(Exception, match="authorization is not verified"):
            drive(proxy.prepare_readonly_rotation)
        assert proxy._read_rotation_key not in redis.hashes
        assert native.namespace["_READ_ROTATION"] is None
        assert execution.mailbox.session_id == old_execution_sid
        assert bytes(native.states["read"].data) == old_frame
        assert proxy.process_request(original) == result
        # 一个未经独立认证的 True 仍不能绕过缺失的授权合同。
        native.namespace["_RUNTIMES"]["execution"]["_SERVER"].bridge.runtime_metadata["read_rotation_authorization_ready"] = True
        with pytest.raises(Exception, match="authorization is not verified"):
            drive(proxy.prepare_readonly_rotation)
        assert proxy._read_rotation_key not in redis.hashes
        assert native.namespace["_READ_ROTATION"] is None and bytes(native.states["read"].data) == old_frame
        native.writes.assert_not_called()
    finally:
        proxy.close()
        native.close()


def prepared_proxy_retirement(monkeypatch, tmp_path):
    """生成 native + 内存邮箱模型；私有 manager 回执仅为隔离替身。"""
    native, proxy, provider, pipe, old, old_state, old_frame, candidate, record, _, drive = (
        prepared_proxy_activation(monkeypatch, tmp_path))
    assert drive(proxy.activate_readonly_rotation)["status"] == "ACTIVATED"
    active = json.loads(provider.hashes[proxy._read_rotation_key][record["transition_id"]])
    accepted = dict(active, state="GATEWAY_VERIFIED")
    raw = json.dumps(accepted, sort_keys=True, allow_nan=False, separators=(",", ":"))
    provider.hashes[proxy._read_rotation_key][record["transition_id"]] = raw
    proxy._accepted_rotation_record = dict(transition_id=record["transition_id"], raw=raw)
    proxy._accepted_rotation_records[record["transition_id"]] = raw
    proxy._gateway_acceptance_proof = {"raw": raw}
    proxy._read_rotation_stage, proxy._rotation_waiting_gateway = "GATEWAY_VERIFIED", False
    proxy._read_rotation_blocked = False
    proxy._rotation_open_delivery, proxy._rotation_open_acknowledged = "CONFIRMED", True
    proxy._unconfirmed_read_ids["readonly"].discard(record["old_request_id"])
    proxy._remove_resolved_write_block_reasons({"READ_SOURCE_UNTRUSTED", "READ_ROTATION_UNCONFIRMED"})
    # SIM 模型仅注入本地私有 manager 替身；不代表该 SIM 目标已有真实 supervisor 接入。
    proxy._block_writes("NATIVE_WRITE_RESULT_UNKNOWN")
    proxy._block_writes("EXECUTION_EVENT_GAP")
    proxy._block_writes("OPERATOR_WRITE_DISABLED")
    native.namespace["_ROTATION_CAPABILITY"] = dict(status="REVOKED", reason="COMMITTED_READ_SID", request=dict(
        read_session_id=record["old_read_session_id"], execution_session_id=record["execution_session_id"],
        native_pid=os.getpid(), native_creation_filetime=record["native_creation_filetime"]))
    native.namespace["_ROTATION_PINNED_TRANSITION"] = record["transition_id"]
    native.namespace["_RUNTIMES"]["execution"]["_SERVER"].bridge.runtime_metadata["read_rotation_authorization_ready"] = False
    return native, proxy, provider, pipe, old, old_state, old_frame, candidate, record, raw, drive


@pytest.mark.parametrize("outcome", ["success", "ready_unavailable", "ready_unknown", "register_unknown", "native_unknown",
                                     "settle_unknown", "commit_unknown", "ack_unknown", "next_unavailable", "next_unknown",
                                     "wrong_scope", "wrong_gateway", "wrong_account", "old_changed"]
                         + ["busy%d_%s" % (stage, route) for stage in (1, 2, 3, 4) for route in ("old", "execution")])
def test_rotation_retirement_keeps_old_evidence_and_requires_every_single_use_receipt(monkeypatch, tmp_path, outcome):
    native, proxy, provider, pipe, old, old_state, old_frame, candidate, record, raw, drive = (
        prepared_proxy_retirement(monkeypatch, tmp_path))
    calls = []

    class Channel:
        def register(self, request):
            calls.append(dict(request))
            action = request["action"]
            if outcome == "ready_unknown" and action == "READY":
                raise TimeoutError("readiness unknown")
            if outcome == "register_unknown" and action == "REGISTER":
                raise TimeoutError("registration unknown")
            if outcome == "commit_unknown" and action == "CLOSE_COMMITTED":
                raise TimeoutError("close commit unknown")
            if outcome == "ack_unknown" and action == "CLOSE_ACKNOWLEDGED":
                raise TimeoutError("close acknowledgement unknown")
            if outcome == "next_unknown" and action == "NEXT_READY":
                raise TimeoutError("next readiness unknown")
            if action == "REGISTER":
                assert request["stage"] == "CLOSE"
                return dict(contract="SUPERVISOR_ROTATION_AUTH_V1", action="REGISTER", status="REGISTERED",
                            stage="CLOSE", transition_id=request["transition_id"], nonce=request["nonce"],
                            capability="4" * 64)
            if action in ("READY", "NEXT_READY"):
                status = "UNAVAILABLE" if ((outcome == "ready_unavailable" and action == "READY")
                    or (outcome == "next_unavailable" and action == "NEXT_READY")) and sum(
                    item["action"] == action for item in calls) == 1 else "READY"
                return dict(request, status=status)
            if action in ("CLOSE_COMMITTED", "CLOSE_ACKNOWLEDGED"):
                return dict(contract=request["contract"], action=action,
                            status="ACK" if action == "CLOSE_COMMITTED" else "ACKNOWLEDGED",
                            transition_id=request["transition_id"], record_sha256=request["record_sha256"],
                            close_record_sha256=request["close_record_sha256"])
            raise AssertionError("unexpected manager control")

        def close(self):
            pass

    proxy._supervisor_rotation_channel = Channel()
    if outcome == "wrong_scope":
        proxy.execution_cursor_guard_target = None
    if outcome == "wrong_gateway":
        proxy.execution_cursor_guard_target = {"gateway_id": "qmt-win-001", "profile_id": "sim-bigqmt"}
    if outcome == "wrong_account":
        proxy.account_id = "8882152350"
    if outcome == "old_changed":
        old_state.data[REQUEST_OFFSET] ^= 1
    if outcome == "native_unknown":
        pipe.fail_retire = True
    if outcome == "settle_unknown":
        original_eval = provider.eval

        def settle_then_lose_reply(*args):
            result = original_eval(*args)
            if "ROTATION_CLOSE_SETTLE_V1" in args[0]:
                raise RuntimeError("settlement reply lost")
            return result

        provider.eval = settle_then_lose_reply
    try:
        if outcome.startswith("busy"):
            stage, route = outcome[4:].split("_")
            region = old.mailbox.region if route == "old" else proxy._clients["execution"].mailbox.region
            original_acquire, attempts = region.acquire, []
            original_frames, inside = proxy._rotation_close_frames_ready, [False]

            def tracked_frames(value):
                inside[0] = True
                try:
                    return original_frames(value)
                finally:
                    inside[0] = False

            monkeypatch.setattr(proxy, "_rotation_close_frames_ready", tracked_frames)

            def observe_with_competing_mutex(milliseconds):
                if not inside[0]:
                    return original_acquire(milliseconds)
                attempts.append(milliseconds)
                if len(attempts) == int(stage):
                    assert region.state.lock.acquire(False)
                    try:
                        return original_acquire(milliseconds)
                    finally:
                        region.state.lock.release()
                return original_acquire(milliseconds)

            monkeypatch.setattr(region, "acquire", observe_with_competing_mutex)
            assert drive(proxy.close_readonly_rotation) is None
            prior_calls = [item["action"] for item in calls]
            native_close_count = sum(item["action"] == "CLOSE_PERMIT" for item in pipe.requests)
            assert native_close_count == (1 if int(stage) >= 3 else 0)
            result = drive(proxy.close_readonly_rotation)
            assert result["status"] == "ROTATION_CLOSED"
            assert [item["action"] for item in calls].count("READY") == 1
            assert [item["action"] for item in calls].count("REGISTER") == 1
            assert [item["action"] for item in calls].count("CLOSE_COMMITTED") == 1
            assert [item["action"] for item in calls].count("CLOSE_ACKNOWLEDGED") == 1
            assert [item["action"] for item in calls].count("NEXT_READY") == 1
            assert [item["action"] for item in calls][:len(prior_calls)] == prior_calls
        elif outcome in ("success", "ready_unavailable", "next_unavailable"):
            if outcome != "success":
                assert drive(proxy.close_readonly_rotation) is None
            result = drive(proxy.close_readonly_rotation)
            assert result["status"] == "ROTATION_CLOSED"
            assert [item["action"] for item in calls].count("REGISTER") == 1
            assert [item["action"] for item in calls].count("CLOSE_COMMITTED") == 1
            assert [item["action"] for item in calls].count("CLOSE_ACKNOWLEDGED") == 1
            assert [item["action"] for item in pipe.requests].count("CLOSE_PERMIT") == 1
        else:
            with pytest.raises(Exception):
                drive(proxy.close_readonly_rotation)
            before_calls, before_pipe = copy.deepcopy(calls), copy.deepcopy(pipe.requests)
            with pytest.raises(Exception):
                drive(proxy.close_readonly_rotation)
            assert calls == before_calls and pipe.requests == before_pipe
            assert not proxy._rotation_closed_records
        assert provider.hashes[proxy._read_rotation_key][record["transition_id"]] == raw
        assert record["old_request_id"] in proxy._replays and record["old_request_id"] in proxy._retired_read_ids
        assert proxy._clients["execution"].mailbox.session_id == record["execution_session_id"]
        assert "NATIVE_WRITE_RESULT_UNKNOWN" in proxy._write_block_reasons
        assert "EXECUTION_EVENT_GAP" in proxy._write_block_reasons and "OPERATOR_WRITE_DISABLED" in proxy._write_block_reasons
        assert "4" * 64 not in json.dumps(provider.hashes)
        if outcome != "old_changed":
            assert bytes(old_state.data) == old_frame and not old.mailbox.closed
        if outcome in ("success", "ready_unavailable", "next_unavailable") or outcome.startswith("busy"):
            saved = json.loads(provider.hashes[proxy._read_rotation_close_key][record["transition_id"]])
            assert saved["state"] == "CLOSED" and saved["close_evidence"]["status"] == "CLOSED"
            assert proxy._rotation_closed_records[record["transition_id"]] == provider.hashes[proxy._read_rotation_close_key][record["transition_id"]]
            assert len(native.namespace["_RETIRED_READ_ROTATIONS"]) == 1
            assert proxy.identity_snapshot()["write_ready"] is False
    finally:
        proxy.close()
        native.close()


def test_real_proxy_second_generation_reaches_native_commit_after_first_retirement(monkeypatch, tmp_path):
    """两代真实 proxy 与生成 native 使用内存邮箱；监督器仅为本地替身。"""
    native, proxy, provider, pipe, old, old_state, old_frame, candidate, first, raw, drive = (
        prepared_proxy_retirement(monkeypatch, tmp_path))
    registrations = []

    class Channel:
        def register(self, request):
            registrations.append(copy.deepcopy(request))
            action = request["action"]
            if action == "REGISTER":
                return dict(contract="SUPERVISOR_ROTATION_AUTH_V1", action="REGISTER", status="REGISTERED",
                            stage=request["stage"], transition_id=request["transition_id"], nonce=request["nonce"],
                            capability="4" * 64)
            if action in ("READY", "NEXT_READY"):
                return dict(request, status="READY")
            if action in ("CLOSE_COMMITTED", "CLOSE_ACKNOWLEDGED"):
                return dict(contract=request["contract"], action=action,
                            status="ACK" if action == "CLOSE_COMMITTED" else "ACKNOWLEDGED",
                            transition_id=request["transition_id"], record_sha256=request["record_sha256"],
                            close_record_sha256=request["close_record_sha256"])
            raise AssertionError("unexpected manager control")

        def close(self):
            pass

    proxy._supervisor_rotation_channel = Channel()
    try:
        assert drive(proxy.close_readonly_rotation)["status"] == "ROTATION_CLOSED"
        assert proxy._clients["readonly"] is candidate and bytes(old_state.data) == old_frame
        assert provider.hashes[proxy._read_rotation_key][first["transition_id"]] == raw
        proxy.timeout_seconds = 0.15
        request = FIXTURE.request("get_ticks", rid="rotation-old-second", params={"code": "000001.SZ"})
        request["expected_source_binding"] = proxy._source_binding()
        original_ticks = native.context.get_full_tick
        def late_ticks(codes):
            time.sleep(0.3)
            return original_ticks(codes)
        native.context.get_full_tick = late_ticks
        with ThreadPoolExecutor(1) as executor:
            pending = executor.submit(proxy.process_request, request)
            until = time.monotonic() + 2
            while time.monotonic() < until:
                if candidate.mailbox.region.acquire(0):
                    try:
                        published = candidate.mailbox._header()[4] == 2
                    finally:
                        candidate.mailbox.region.release()
                    if published:
                        break
                time.sleep(0.001)
            else:
                raise AssertionError("second readonly request was not published")
            native.namespace["aqp_shm_readonly_adjust"](native.context)
            result = pending.result(timeout=1)
        assert result["status"] == "UNKNOWN" and result["previous_unconfirmed"] is False, (result.get("error"), result.get("server_error"))
        second_old_frame = bytes(candidate.mailbox.region.state.data)
        assert candidate.mailbox._header()[4:7] == [2, 2, 1]
        proxy.timeout_seconds = 1
        # 仅隔离模型注入能力；真实原生要通过独立双通道验证。
        bridge = native.namespace["_RUNTIMES"]["execution"]["_SERVER"].bridge
        bridge.runtime_metadata.update(read_rotation_authorization_ready=True,
                                       read_rotation_capability_contract="READ_ROTATION_CAPABILITY_V1")
        proxy._rotation_capability = dict(verified_at=time.monotonic(), receipt=dict(
            read_session_id=candidate.mailbox.session_id, execution_session_id=first["execution_session_id"]))
        monkeypatch.setattr(proxy_module, "ShmReadOnlyClient", native.transports["read"].ShmReadOnlyClient)
        second = drive(proxy.prepare_readonly_rotation)
        assert second["status"] == "COMMITTED" and second["transition_id"] != first["transition_id"]
        record = json.loads(provider.hashes[proxy._read_rotation_key][second["transition_id"]])
        assert record["old_request_id"] == request["request_id"] and record["old_sequence"] == 2
        assert record["old_previous_unconfirmed"] is False and record["execution_session_id"] == first["execution_session_id"]
        evidence = record["native_evidence"]
        assert evidence["native_started_at_unix"] < evidence["read_expires_at_unix"] <= evidence["native_finished_at_unix"]
        assert [item["stage"] for item in registrations if item["action"] == "REGISTER"] == ["CLOSE", "PREPARE", "COMMIT"]
        assert sum(item["action"] == "COMMIT" and item["transition_id"] == second["transition_id"]
                   for item in pipe.requests) == 1
        assert bytes(old_state.data) == old_frame and bytes(candidate.mailbox.region.state.data) == second_old_frame
        assert request["request_id"] in proxy._unconfirmed_read_ids["readonly"]
        assert "EXECUTION_EVENT_GAP" in proxy._write_block_reasons and "OPERATOR_WRITE_DISABLED" in proxy._write_block_reasons
        native.writes.assert_not_called()
    finally:
        proxy.close()
        native.close()


@pytest.mark.parametrize("mutation", ["missing", "sid", "pid", "bool-pid", "sequence", "bool-sequence", "contract", "extra"])
def test_write_cursor_guard_is_validated_before_ledger_claim_or_native_call(mutation):
    proxy, provider, _, execution = FIXTURE.RedisShmReadOnlyProxyTest().make_proxy(
        policy="PERSISTENT_SIMULATION_READ_WRITE")
    request = FIXTURE.request("submit_order", rid="bad-cursor")
    guard = request["params"]["execution_event_guard"]
    if mutation == "missing":
        del request["params"]["execution_event_guard"]
    elif mutation == "sid":
        guard["execution_session_id"] = "other-sid"
    elif mutation == "pid":
        guard["native_pid"] = 123
    elif mutation == "bool-pid":
        guard["native_pid"] = True
    elif mutation == "sequence":
        guard["sequence"] = -1
    elif mutation == "bool-sequence":
        guard["sequence"] = True
    elif mutation == "contract":
        guard["contract"] = "other"
    else:
        guard["extra"] = "unexpected"
    response = proxy.process_request(request)
    assert response["status"] == "REJECTED" and response["publication_state"] == "NOT_SENT"
    assert len(execution.calls) == 1 and proxy._write_ledger_key not in provider.hashes


def test_legacy_native_without_cursor_contract_starts_with_writes_blocked():
    proxy, provider, _, execution = FIXTURE.RedisShmReadOnlyProxyTest().make_proxy(
        verify=False, policy="PERSISTENT_SIMULATION_READ_WRITE")
    send = execution.send_request

    def old_native(request, timeout_seconds):
        response = send(request, timeout_seconds)
        if request["method"] == "ping":
            response["data"].pop("execution_event_guard_contract", None)
        return response

    execution.send_request = old_native
    proxy.verify_identity()
    assert proxy.identity_snapshot()["write_ready"] is False
    assert proxy.identity_snapshot()["write_block_reason"] == "EXECUTION_CURSOR_GATE_UNAVAILABLE"
    response = proxy.process_request(FIXTURE.request("submit_order", rid="legacy-no-guard"))
    assert response["status"] == "REJECTED" and len(execution.calls) == 1
    assert proxy._write_ledger_key not in provider.hashes


def test_same_sim_account_without_vm_target_keeps_legacy_write_and_fault_contract():
    proxy, provider, read, execution = FIXTURE.RedisShmReadOnlyProxyTest().make_proxy(
        verify=False, policy="PERSISTENT_SIMULATION_READ_WRITE", target=False)
    provider.hashes[proxy._read_rotation_key] = {"vm-only": "unrelated transition record"}
    send = execution.send_request

    def old_native(request, timeout_seconds):
        response = send(request, timeout_seconds)
        if request["method"] == "ping":
            response["data"].pop("execution_event_guard_contract", None)
        return response

    execution.send_request = old_native
    proxy.verify_identity()
    assert proxy.identity_snapshot()["write_ready"] is True
    assert "execution_cursor_guard_target" not in proxy.identity_snapshot()
    assert "read_source_state" not in proxy.identity_snapshot()
    assert not any(op[0] == "hgetall" and "read_rotation" in str(op) for op in provider.operations)
    write = FIXTURE.request("submit_order", rid="legacy-same-account")
    del write["params"]["execution_event_guard"]
    del write["params"]["supervisor_gate_epoch"]
    execution.result = {"status": "SUBMITTED", "order_sys_id": "legacy-fixture"}
    assert proxy.process_request(write)["ok"]
    read.failure = ShmTimeoutError("legacy read unknown")
    response = proxy.process_request(FIXTURE.request("get_ticks", rid="legacy-read", params={"code": "000001.SZ"}))
    assert response["status"] == "UNKNOWN" and proxy.stopped
    assert "publication_state" not in response


class GateKernel:
    def __init__(self):
        self.states = {1: 0x102, 2: 0, 3: 0, 4: 0x102}
        self.released = []
        self.closed = []
        self.epoch = ctypes.c_uint64(1)

    def WaitForSingleObject(self, handle, timeout):
        assert timeout == 0
        return self.states[handle]

    def ReleaseMutex(self, handle):
        self.released.append(handle)
        return True

    def CloseHandle(self, handle):
        self.closed.append(handle)
        return True

    def UnmapViewOfFile(self, address):
        assert address == ctypes.addressof(self.epoch)
        return True


@pytest.mark.parametrize("bad_name", [False, True])
def test_nt_object_inspection_uses_exact_basic_structure_and_rejects_invalid_name(monkeypatch, bad_name):
    class UnicodeString(ctypes.Structure):
        _fields_ = [("length", ctypes.c_uint16), ("maximum", ctypes.c_uint16), ("buffer", ctypes.c_void_p)]

    class NtQuery:
        def __init__(self):
            self.basic_sizes = []

        def __call__(self, handle, kind, buffer, size, returned):
            if kind == 0:
                self.basic_sizes.append(size)
                if size != 56:
                    return -1  # Windows ObjectBasicInformation 拒绝非精确长度。
                address = ctypes.cast(buffer, ctypes.c_void_p).value
                ctypes.c_uint32.from_address(address + 4).value = 0x00100000
                return 0
            assert kind == 2
            name = UnicodeString.from_buffer(buffer)
            word = ctypes.create_unicode_buffer("Event")
            address = ctypes.addressof(buffer) + ctypes.sizeof(UnicodeString)
            ctypes.memmove(address, ctypes.addressof(word), ctypes.sizeof(word))
            name.length = name.maximum = len("Event") * 2
            name.buffer = 0 if bad_name else address
            return 0

    class Native:
        def __init__(self, query):
            self.NtQueryObject = query

    query = NtQuery()
    monkeypatch.setattr(proxy_module.ctypes, "WinDLL", lambda *_args, **_kwargs: Native(query), raising=False)
    if bad_name:
        with pytest.raises(proxy_module.SupervisorWriteGateError, match="INSPECTION_FAILED"):
            proxy_module._inspect_supervisor_handle_metadata([1])
    else:
        assert proxy_module._inspect_supervisor_handle_metadata([1]) == [("Event", 0x00100000)]
    assert query.basic_sizes == [56]


@pytest.mark.parametrize("change", [
    (0, "Mutant", 0x00100000), (1, "Mutant", 0x00100000),
    (2, "Event", 0x00100001), (3, "Process", 0x00101040),
    (0, "Event", 0x00100002), (4, "Section", 0x00000006),
])
def test_supervisor_handle_type_and_minimum_access_are_required(change):
    metadata = [("Event", 0x00100000), ("Event", 0x00100000),
                ("Mutant", 0x00100001), ("Process", 0x00101000), ("Section", 0x00000004)]
    proxy_module._validate_supervisor_handle_metadata(metadata)
    index, name, access = change
    metadata[index] = (name, access)
    with pytest.raises(proxy_module.SupervisorWriteGateError, match="TYPE_OR_ACCESS"):
        proxy_module._validate_supervisor_handle_metadata(metadata)


@pytest.mark.parametrize("case,permanent", [
    ("blocked", True), ("parent_exit", True), ("invalid", True),
    ("identity_busy", False), ("mutex_busy", False), ("mutex_abandoned", True),
])
def test_inherited_supervisor_gate_refuses_write_before_claim(case, permanent):
    proxy, provider, _, execution = FIXTURE.RedisShmReadOnlyProxyTest().make_proxy(
        policy="PERSISTENT_SIMULATION_READ_WRITE")
    kernel = GateKernel()
    gate = proxy_module.InheritedSupervisorWriteGate((1, 2, 3, 4, 5), kernel, ctypes.addressof(kernel.epoch))
    proxy._supervisor_write_gate = gate
    if case == "blocked":
        kernel.states[1] = 0
    elif case == "parent_exit":
        kernel.states[4] = 0
    elif case == "invalid":
        kernel.states[1] = 0xFFFFFFFF
    elif case == "identity_busy":
        kernel.states[2] = 0x102
    elif case == "mutex_busy":
        kernel.states[3] = 0x102
    else:
        kernel.states[3] = 0x80
    response = proxy.process_request(FIXTURE.request("submit_order", rid="gate-" + case))
    assert response["status"] == "REJECTED" and response["publication_state"] == "NOT_SENT"
    assert response["publication_scope"] == "SHM" and len(execution.calls) == 1
    assert proxy._write_ledger_key not in provider.hashes
    assert proxy._writes_blocked is permanent
    if case == "mutex_abandoned":
        assert kernel.released == [3]
    gate.close()
    assert kernel.closed == [1, 2, 3, 4, 5]


def test_target_without_inherited_gate_keeps_reads_open_and_writes_blocked():
    proxy, provider, _, execution = FIXTURE.RedisShmReadOnlyProxyTest().make_proxy(
        policy="PERSISTENT_SIMULATION_READ_WRITE", include_gate=False)
    assert proxy.identity_snapshot()["write_ready"] is False
    assert proxy.identity_snapshot()["write_block_reason"] == "SUPERVISOR_GATE_UNAVAILABLE"
    assert proxy.process_request(FIXTURE.request("get_positions", rid="still-readable"))["ok"]
    assert proxy.process_request(FIXTURE.request("submit_order", rid="missing-gate"))["status"] == "REJECTED"
    assert proxy._write_ledger_key not in provider.hashes
    assert all(call[0]["method"] != "submit_order" for call in execution.calls)


def test_native_without_verified_inherited_gate_is_not_write_ready():
    proxy, provider, _, execution = FIXTURE.RedisShmReadOnlyProxyTest().make_proxy(
        verify=False, policy="PERSISTENT_SIMULATION_READ_WRITE")
    send = execution.send_request

    def missing_native_gate(request, timeout_seconds, **kwargs):
        response = send(request, timeout_seconds, **kwargs)
        if request["method"] == "ping":
            response["data"]["native_write_runtime_guard_ready"] = False
        return response

    execution.send_request = missing_native_gate
    proxy.verify_identity()
    assert proxy.identity_snapshot()["write_ready"] is False
    assert proxy.identity_snapshot()["write_block_reason"] == "NATIVE_SUPERVISOR_GATE_UNAVAILABLE"
    assert proxy.process_request(FIXTURE.request("submit_order", rid="native-missing-gate"))["status"] == "REJECTED"
    assert proxy._write_ledger_key not in provider.hashes and len(execution.calls) == 1


@pytest.mark.parametrize("failure_check", [2, 3])
def test_supervisor_gate_change_after_claim_preserves_unknown_without_retry(failure_check):
    proxy, provider, _, execution = FIXTURE.RedisShmReadOnlyProxyTest().make_proxy(
        policy="PERSISTENT_SIMULATION_READ_WRITE")

    class ChangingGate(FIXTURE.FakeSupervisorWriteGate):
        def __init__(self):
            super().__init__()
            self.checks = 0

        def check(self):
            self.checks += 1
            if self.checks == failure_check:
                raise proxy_module.SupervisorWriteGateError("SUPERVISOR_WRITE_BLOCKED", permanent=True)

    gate = ChangingGate()
    proxy._supervisor_write_gate = gate
    request = FIXTURE.request("submit_order", rid="gate-race-%s" % failure_check)
    response = proxy.process_request(request)
    assert response["status"] == "UNKNOWN" and response["automatic_retry"] is False
    assert response["publication_state"] == ("NOT_SENT" if failure_check == 2 else "UNKNOWN")
    assert json.loads(provider.hashes[proxy._write_ledger_key][request["request_id"]])["state"] == "UNKNOWN"
    assert not gate.held and proxy.identity_snapshot()["write_ready"] is False
    assert len(execution.calls) == (1 if failure_check == 2 else 2)


def test_supervisor_mutex_is_released_after_header_publication_before_response_wait():
    proxy, provider, _, execution = FIXTURE.RedisShmReadOnlyProxyTest().make_proxy(
        policy="PERSISTENT_SIMULATION_READ_WRITE")
    events = []

    class TraceGate(FIXTURE.FakeSupervisorWriteGate):
        def acquire(self):
            events.append("acquire")
            super().acquire()

        def check(self):
            events.append("check")

        def release(self):
            events.append("release")
            super().release()

    gate = TraceGate()
    proxy._supervisor_write_gate = gate
    evaluate, send = provider.eval, execution.send_request

    def trace_eval(script, *args):
        if "HLEN" in script:
            events.append("claim")
        return evaluate(script, *args)

    def trace_send(request, timeout_seconds, *, after_publication=None):
        events.append("publish")
        result = send(request, timeout_seconds, after_publication=after_publication)
        assert not gate.held
        events.append("response")
        return result

    provider.eval, execution.send_request = trace_eval, trace_send
    execution.result = {"status": "SUBMITTED", "order_sys_id": "fixture"}
    assert proxy.process_request(FIXTURE.request("submit_order", rid="gate-order"))["ok"]
    assert events == ["acquire", "check", "claim", "check", "publish", "check", "release", "response"]


def test_missing_publication_callback_cannot_report_a_confirmed_write():
    proxy, provider, _, execution = FIXTURE.RedisShmReadOnlyProxyTest().make_proxy(
        policy="PERSISTENT_SIMULATION_READ_WRITE")
    send = execution.send_request

    def ignores_publication_callback(request, timeout_seconds, *, after_publication=None):
        return send(request, timeout_seconds)

    execution.send_request = ignores_publication_callback
    execution.result = {"status": "SUBMITTED", "order_sys_id": "untrusted"}
    request = FIXTURE.request("submit_order", rid="missing-release-callback")
    response = proxy.process_request(request)
    assert response["status"] == "UNKNOWN" and proxy.identity_snapshot()["write_ready"] is False
    assert json.loads(provider.hashes[proxy._write_ledger_key][request["request_id"]])["state"] == "UNKNOWN"
    assert proxy._supervisor_write_gate.held is False


@pytest.mark.parametrize("mutation", ["missing", "zero", "negative", "bool", "overflow", "contract", "extra"])
def test_supervisor_epoch_is_required_before_ledger_or_native_write(mutation):
    proxy, provider, _, execution = FIXTURE.RedisShmReadOnlyProxyTest().make_proxy(
        policy="PERSISTENT_SIMULATION_READ_WRITE")
    request = FIXTURE.request("submit_order", rid="bad-epoch")
    epoch = request["params"]["supervisor_gate_epoch"]
    if mutation == "missing":
        del request["params"]["supervisor_gate_epoch"]
    elif mutation == "zero":
        epoch["epoch"] = 0
    elif mutation == "negative":
        epoch["epoch"] = -1
    elif mutation == "bool":
        epoch["epoch"] = True
    elif mutation == "overflow":
        epoch["epoch"] = 2 ** 64
    elif mutation == "contract":
        epoch["contract"] = "other"
    else:
        epoch["extra"] = True
    response = proxy.process_request(request)
    assert response["status"] == "REJECTED" and response["publication_state"] == "NOT_SENT"
    assert proxy._write_ledger_key not in provider.hashes and len(execution.calls) == 1


def test_old_supervisor_epoch_cannot_reach_ledger_and_a_new_rid_can_use_current_epoch():
    proxy, provider, _, execution = FIXTURE.RedisShmReadOnlyProxyTest().make_proxy(
        policy="PERSISTENT_SIMULATION_READ_WRITE")
    proxy._supervisor_write_gate.epoch = 2
    stale = FIXTURE.request("submit_order", rid="old-epoch")
    response = proxy.process_request(stale)
    assert response["status"] == "REJECTED" and response["publication_state"] == "NOT_SENT"
    assert proxy._write_ledger_key not in provider.hashes and len(execution.calls) == 1
    current = FIXTURE.request("submit_order", rid="new-epoch")
    current["params"]["supervisor_gate_epoch"]["epoch"] = 2
    execution.result = {"status": "SUBMITTED", "order_sys_id": "fixture"}
    assert proxy.process_request(current)["ok"] and len(execution.calls) == 2


@pytest.mark.parametrize("change_at", [2, 3])
def test_supervisor_epoch_change_after_claim_is_unknown_and_never_resent(change_at):
    proxy, provider, _, execution = FIXTURE.RedisShmReadOnlyProxyTest().make_proxy(
        policy="PERSISTENT_SIMULATION_READ_WRITE")

    class ChangingEpoch(FIXTURE.FakeSupervisorWriteGate):
        def __init__(self):
            super().__init__()
            self.checks = 0

        def check(self):
            self.checks += 1
            if self.checks == change_at:
                self.epoch = 2

    gate = ChangingEpoch()
    proxy._supervisor_write_gate = gate
    request = FIXTURE.request("submit_order", rid="changed-epoch-%s" % change_at)
    response = proxy.process_request(request)
    assert response["status"] == "UNKNOWN" and response["automatic_retry"] is False
    assert response["publication_state"] == ("NOT_SENT" if change_at == 2 else "UNKNOWN")
    assert json.loads(provider.hashes[proxy._write_ledger_key][request["request_id"]])["state"] == "UNKNOWN"
    assert len(execution.calls) == (1 if change_at == 2 else 2) and not gate.held


@pytest.mark.parametrize("target", [
    {"gateway_id": "qmt-win-001", "profile_id": "sim-bigqmt"},
    {"gateway_id": "qmt-vm-001", "profile_id": "live-stock"},
    {"gateway_id": "qmt-vm-001", "profile_id": "sim-bigqmt", "extra": True},
])
def test_other_instance_or_mismatched_profile_cannot_enable_vm_guard(target):
    with pytest.raises(ValueError, match="exact authorized VM target"):
        proxy_module.validate_execution_cursor_guard_target(target, ACCOUNT, "SIM")


def cursor_no_write_receipt(request, environment):
    guard = request["params"]["execution_event_guard"]
    return dict(status="BLOCKED", no_write_contract="EXECUTION_CURSOR_NO_WRITE_V1",
                error="EXECUTION_EVENT_CURSOR_NOT_CURRENT", request_id=request["request_id"],
                account_id=request["account_id"], environment=environment,
                session_id=guard["execution_session_id"], native_pid=guard["native_pid"], windows_session_id=1,
                native_write_executed=False, automatic_retry=False, writes_blocked=False, write_block_reason="",
                execution_event_guard=copy.deepcopy(guard), native_event_sequence=guard["sequence"] + 1,
                native_lost_sequence=0)


def epoch_no_write_receipt(request, environment):
    guard = request["params"]["execution_event_guard"]
    epoch = request["params"]["supervisor_gate_epoch"]
    return dict(status="BLOCKED", no_write_contract="SUPERVISOR_GATE_NO_WRITE_V1",
                error="SUPERVISOR_GATE_EPOCH_MISMATCH", request_id=request["request_id"],
                account_id=request["account_id"], environment=environment,
                session_id=guard["execution_session_id"], native_pid=guard["native_pid"], windows_session_id=1,
                native_write_executed=False, automatic_retry=False, writes_blocked=False, write_block_reason="",
                supervisor_gate_epoch=copy.deepcopy(epoch), native_supervisor_gate_epoch=epoch["epoch"] + 1)


@pytest.mark.parametrize("method", ["submit_order", "cancel_order"])
def test_sync_cursor_rejection_is_durable_not_executed_and_never_resent(method):
    proxy, provider, _, execution = FIXTURE.RedisShmReadOnlyProxyTest().make_proxy(
        policy="PERSISTENT_SIMULATION_READ_WRITE")
    request = FIXTURE.request(method, rid="cursor-no-write")
    execution.result = cursor_no_write_receipt(request, "SIM")
    response = proxy.process_request(request)
    assert response["ok"] and response["data"]["native_write_executed"] is False
    record = json.loads(provider.hashes[proxy._write_ledger_key][request["request_id"]])
    assert record["state"] == "NOT_EXECUTED"
    assert proxy.process_request(request) == response and len(execution.calls) == 2
    altered = copy.deepcopy(request)
    altered["params"]["execution_event_guard"]["sequence"] += 1
    assert proxy.process_request(altered)["status"] == "REJECTED" and len(execution.calls) == 2
    restarted, _, _, fresh = FIXTURE.RedisShmReadOnlyProxyTest().make_proxy(
        redis=provider, policy="PERSISTENT_SIMULATION_READ_WRITE")
    assert restarted.process_request(request) == response and len(fresh.calls) == 1


def test_async_cursor_rejection_is_durable_not_executed_and_never_resent():
    proxy, provider, _, execution = ASYNC.make_async_proxy()
    request = ASYNC.async_request("async-cursor-no-write", "cursor-cid")

    def mutate(value, response):
        if value["method"] == ASYNC.METHOD:
            data = cursor_no_write_receipt(value, "LIVE")
            data.update(client_order_id=value["params"]["client_order_id"], broker_accepted=False,
                        qmt_async_seq=None, order_sys_id="", writes_blocked=False, write_block_reason="",
                        async_submit_contract=ASYNC.CONTRACT)
            response["data"] = data

    execution.mutate = mutate
    response = proxy.process_request(request)
    assert response["ok"] and response["data"]["native_write_executed"] is False
    assert ASYNC.record(provider, request["request_id"])["state"] == "NOT_EXECUTED"
    assert proxy.process_request(request) == response and len(execution.calls) == 2
    restarted, _, _, fresh = ASYNC.make_async_proxy(provider=provider)
    assert restarted.process_request(request) == response and len(fresh.calls) == 1


def test_async_cursor_no_write_with_event_loss_stays_blocked_across_restart():
    proxy, provider, _, execution = ASYNC.make_async_proxy()
    request = ASYNC.async_request("async-event-loss", "lost-cid")

    def mutate(value, response):
        if value["method"] == ASYNC.METHOD:
            data = cursor_no_write_receipt(value, "LIVE")
            data.update(client_order_id=value["params"]["client_order_id"], broker_accepted=False,
                        qmt_async_seq=None, order_sys_id="", writes_blocked=True,
                        write_block_reason="EXECUTION_EVENT_GAP", async_submit_contract=ASYNC.CONTRACT,
                        native_lost_sequence=1)
            response["data"] = data

    execution.mutate = mutate
    response = proxy.process_request(request)
    assert response["ok"] and response["data"]["native_write_executed"] is False
    assert ASYNC.record(provider, request["request_id"])["state"] == "NOT_EXECUTED"
    assert proxy.identity_snapshot()["write_ready"] is False
    restarted, _, _, fresh = ASYNC.make_async_proxy(provider=provider)
    assert restarted.identity_snapshot()["write_ready"] is False
    assert restarted.identity_snapshot()["write_block_reason"] == "EXECUTION_EVENT_GAP"
    assert restarted.process_request(request) == response and len(fresh.calls) == 1


@pytest.mark.parametrize("method", ["submit_order", "cancel_order"])
def test_native_epoch_mismatch_receipt_is_durable_not_executed(method):
    proxy, provider, _, execution = FIXTURE.RedisShmReadOnlyProxyTest().make_proxy(
        policy="PERSISTENT_SIMULATION_READ_WRITE")
    request = FIXTURE.request(method, rid="epoch-no-write")
    execution.result = epoch_no_write_receipt(request, "SIM")
    response = proxy.process_request(request)
    assert response["ok"] and response["data"]["native_write_executed"] is False
    assert json.loads(provider.hashes[proxy._write_ledger_key][request["request_id"]])["state"] == "NOT_EXECUTED"
    assert proxy.process_request(request) == response and len(execution.calls) == 2
    restarted, _, _, fresh = FIXTURE.RedisShmReadOnlyProxyTest().make_proxy(
        redis=provider, policy="PERSISTENT_SIMULATION_READ_WRITE")
    assert restarted.process_request(request) == response and len(fresh.calls) == 1


def test_async_native_epoch_mismatch_receipt_is_durable_not_executed():
    proxy, provider, _, execution = ASYNC.make_async_proxy()
    request = ASYNC.async_request("async-epoch-no-write", "epoch-cid")

    def mutate(value, response):
        if value["method"] == ASYNC.METHOD:
            data = epoch_no_write_receipt(value, "LIVE")
            data.update(client_order_id=value["params"]["client_order_id"], broker_accepted=False,
                        qmt_async_seq=None, order_sys_id="", async_submit_contract=ASYNC.CONTRACT)
            response["data"] = data

    execution.mutate = mutate
    response = proxy.process_request(request)
    assert response["ok"] and response["data"]["native_write_executed"] is False
    assert ASYNC.record(provider, request["request_id"])["state"] == "NOT_EXECUTED"
    assert proxy.process_request(request) == response and len(execution.calls) == 2
    restarted, _, _, fresh = ASYNC.make_async_proxy(provider=provider)
    assert restarted.process_request(request) == response and len(fresh.calls) == 1


@pytest.mark.parametrize("mutation", ["guard", "lost_without_block", "invalid_block_type"])
def test_invalid_cursor_no_write_receipt_is_unknown_and_blocks_later_writes(mutation):
    proxy, provider, _, execution = FIXTURE.RedisShmReadOnlyProxyTest().make_proxy(
        policy="PERSISTENT_SIMULATION_READ_WRITE")
    request = FIXTURE.request("submit_order", rid="bad-no-write-evidence")
    execution.result = cursor_no_write_receipt(request, "SIM")
    if mutation == "guard":
        execution.result["execution_event_guard"]["sequence"] += 1
    elif mutation == "lost_without_block":
        execution.result["native_lost_sequence"] = 1
    else:
        execution.result["writes_blocked"] = "false"
    response = proxy.process_request(request)
    assert response["status"] == "UNKNOWN" and proxy.identity_snapshot()["write_ready"] is False
    assert json.loads(provider.hashes[proxy._write_ledger_key][request["request_id"]])["state"] == "UNKNOWN"


@pytest.mark.parametrize("case", ["new_event", "off_thread_loss"])
def test_generated_native_cursor_race_is_not_executed_and_receipt_is_preserved(monkeypatch, case):
    native = DUAL.Fixture(monkeypatch, **DUAL.read_write_settings(execution_event_guard=True))
    _, submit, cancel = DUAL._enable_fixture_simulation_orders(native)
    native.start()
    DUAL._fixture_native_login(native)
    native.namespace["_RUNTIMES"]["execution"]["_SERVER"].bridge.runtime_metadata.update(
        supervisor_write_gate_contract="INHERITED_SUPERVISOR_GATE_V1", native_write_runtime_guard_ready=True,
        supervisor_gate_epoch_contract="SUPERVISOR_GATE_EPOCH_V1", native_write_runtime_guard_attached=True,
        native_callback_threading_verified=True)
    read, execution = native.clients["read"], native.clients["execution"]
    provider = FIXTURE.FakeRedis()
    proxy = RedisShmReadOnlyProxy(
        RedisTransport(provider, account_id=ACCOUNT), read, execution,
        expected_readonly_session_id=read.mailbox.session_id,
        expected_execution_session_id=execution.mailbox.session_id, expected_native_pid=os.getpid(),
        expected_windows_session_id=1, timeout_seconds=1, max_requests=0,
        runtime_policy="PERSISTENT_SIMULATION_READ_WRITE", allow_order_methods=True,
        execution_cursor_guard_target={"gateway_id": "qmt-vm-001", "profile_id": "sim-bigqmt"},
        supervisor_write_gate=FIXTURE.FakeSupervisorWriteGate(),
    )

    proxy._supervisor_attach_verified = True  # 原生游标模型模拟已完成受监督接入。

    def drive(function):
        with ThreadPoolExecutor(1) as pool:
            future = pool.submit(function)
            until = time.monotonic() + 3
            while not future.done() and time.monotonic() < until:
                native.namespace["aqp_shm_readonly_adjust"](native.context)
                native.namespace["aqp_shm_rpc_adjust"](native.context)
                time.sleep(0.001)
            return future.result(timeout=0.2)

    try:
        drive(proxy.verify_identity)
        event = dict(m_strAccountID=ACCOUNT, m_strOrderSysID="event-1")
        if case == "new_event":
            native.namespace["order_callback"](native.context, event)
        else:
            worker = threading.Thread(target=native.namespace["order_callback"], args=(native.context, event))
            worker.start()
            worker.join(timeout=2)
            assert not worker.is_alive()
        write = FIXTURE.request("submit_order", rid="cursor-race", params={
            "stock_code": "000001.SZ", "action": "BUY", "volume": 100, "price": 10.5,
            "execution_event_guard": dict(contract="EXECUTION_CURSOR_WRITE_GATE_V1",
                execution_session_id=execution.mailbox.session_id, native_pid=os.getpid(), sequence=0),
        })
        write["expected_source_binding"] = proxy.identity_snapshot()["source_binding"]
        response = drive(lambda: proxy.process_request(write))
        assert response["ok"] and response["data"]["no_write_contract"] == "EXECUTION_CURSOR_NO_WRITE_V1"
        assert response["data"]["native_event_sequence"] >= 1
        assert response["data"]["native_write_executed"] is False
        if case == "off_thread_loss":
            assert response["data"]["native_lost_sequence"] > 0
            assert response["data"]["writes_blocked"] is True
            assert proxy.identity_snapshot()["write_ready"] is False
        assert json.loads(provider.hashes[proxy._write_ledger_key]["cursor-race"])["state"] == "NOT_EXECUTED"
        assert proxy.process_request(write) == response
        submit.assert_not_called()
        cancel.assert_not_called()
    finally:
        proxy.close()
        native.close()


@pytest.mark.parametrize("outcome", ["committed", "unknown"])
def test_generated_native_rotation_control_status_never_repeats_original(monkeypatch, tmp_path, outcome):
    pipe = DUAL.FixtureSupervisorAuthorizationPipe()
    native, _, execution, old_client, old_state, old_frame, identity_path, _, prepared = (
        DUAL._prepared_read_rotation(monkeypatch, tmp_path, async_pipe=pipe))
    candidate = native.clients["read"]
    assert native.call("read", "ping", request_id=prepared["probe_request_id"])["sequence"] == 1
    if outcome == "unknown":
        pipe.fail = True
    proxy = RedisShmReadOnlyProxy(
        RedisTransport(FIXTURE.FakeRedis(), account_id=ACCOUNT), candidate, native.clients["execution"],
        expected_readonly_session_id=candidate.mailbox.session_id,
        expected_execution_session_id=execution.bridge.mailbox.session_id, expected_native_pid=os.getpid(),
        expected_windows_session_id=1, timeout_seconds=1, max_requests=0,
        runtime_policy="PERSISTENT_READONLY", allow_order_methods=False,
        execution_cursor_guard_target={"gateway_id": "qmt-vm-001", "profile_id": "sim-bigqmt"},
    )
    command = dict(contract="READ_ROTATION_CONTROL_V1", action="COMMIT", transition_id=prepared["transition_id"],
                   nonce="2" * 32, capability="2" * 64, token=prepared["token"],
                   probe_request_id=prepared["probe_request_id"], probe_sequence=1)

    def drive():
        with ThreadPoolExecutor(1) as executor:
            future = executor.submit(proxy._execution_rotation_control, command)
            until = time.monotonic() + 3
            while not future.done() and time.monotonic() < until:
                native.namespace["aqp_shm_readonly_adjust"](native.context)
                native.namespace["aqp_shm_rpc_adjust"](native.context)
                time.sleep(0.001)
            return future.result(timeout=0.2)

    try:
        if outcome == "committed":
            receipt = drive()
            assert receipt["status"] == "COMMITTED"
            assert json.loads(identity_path.read_text())["execution_session_id"] == execution.bridge.mailbox.session_id
        else:
            with pytest.raises(Exception, match="authorization outcome is unconfirmed"):
                drive()
            assert native.namespace["_ROTATION_CONTROL_JOBS"][(command["transition_id"], "COMMIT")]["status"] == "UNKNOWN"
        assert [item["action"] for item in pipe.requests] == ["PREPARE", "COMMIT"]
        with pytest.raises(Exception, match="control was already attempted"):
            proxy._execution_rotation_control(command)
        assert [item["action"] for item in pipe.requests] == ["PREPARE", "COMMIT"]
        assert bytes(old_state.data) == old_frame
        assert execution.bridge.mailbox.session_id == proxy._sessions["execution"]
        assert "2" * 64 not in json.dumps(proxy.transport.redis.hashes)
    finally:
        old_client.close()
        native.close()


def prepared_proxy_activation(monkeypatch, tmp_path):
    pipe = DUAL.FixtureSupervisorAuthorizationPipe()
    native, _, execution, old, old_state, old_frame, identity_path, _, prepared = DUAL._prepared_read_rotation(
        monkeypatch, tmp_path, async_pipe=pipe, target_guard=True,
        old_method="get_ticks", old_params={"code": "000001.SZ"})
    candidate = native.clients["read"]
    probe_response = native.call("read", "ping", request_id=prepared["probe_request_id"])
    assert probe_response["ok"] and probe_response["sequence"] == 1
    commit = dict(contract="READ_ROTATION_CONTROL_V1", action="COMMIT", transition_id=prepared["transition_id"],
                  nonce="2" * 32, capability="2" * 64, token=prepared["token"],
                  probe_request_id=prepared["probe_request_id"], probe_sequence=1)
    assert native.call("execution", "ping", {"read_rotation": commit})["data"]["read_rotation_control"]["status"] == "AUTH_PENDING"
    commit_receipt = DUAL._rotation_control_status(native, "COMMIT", prepared["transition_id"])["receipt"]
    execution_data = native.call("execution", "ping")["data"]
    provider = FIXTURE.FakeRedis()
    proxy = RedisShmReadOnlyProxy(
        RedisTransport(provider, account_id=ACCOUNT), old, native.clients["execution"],
        expected_readonly_session_id=old.mailbox.session_id,
        expected_execution_session_id=execution.bridge.mailbox.session_id, expected_native_pid=os.getpid(),
        expected_windows_session_id=1, timeout_seconds=1, max_requests=0,
        runtime_policy="PERSISTENT_SIMULATION_READ_WRITE", allow_order_methods=True,
        execution_cursor_guard_target={"gateway_id": "qmt-vm-001", "profile_id": "sim-bigqmt"},
        supervisor_write_gate=FIXTURE.FakeSupervisorWriteGate())
    birth = prepared["old_evidence"]["native_creation_filetime"]
    parent_pid = os.getppid()
    identity = dict(native_pid=os.getpid(), native_creation_filetime=birth, windows_session_id=1, executable="fixture-native")
    supervisor = dict(native_pid=parent_pid, native_creation_filetime=456, windows_session_id=1, executable="fixture-supervisor")
    monkeypatch.setattr(proxy_module, "_windows_native_process_identity",
                        lambda pid: copy.deepcopy(identity if pid == os.getpid() else supervisor))
    proxy._verified_process_identity = identity
    proxy._supervisor_attach_receipt = {"native": identity, "supervisor": supervisor}
    proxy._supervisor_attach_verified = True
    proxy._source_identity = {"readonly": dict(probe_response["data"], session_id=old.mailbox.session_id),
                              "execution": execution_data}
    proxy._verified = True
    proxy._capabilities = frozenset(proxy_module.EXECUTION_METHODS | proxy_module.QUOTE_METHODS | proxy_module.ALL_WRITE_METHODS)
    proxy._read_isolation_supported = proxy._execution_cursor_gate_supported = True
    proxy._native_supervisor_gate_supported = True
    proxy._read_rotation_candidate = candidate
    proxy._read_rotation_stage = "COMMITTED"
    proxy._read_rotation_blocked = True
    proxy._block_writes("READ_SOURCE_UNTRUSTED")
    proxy._block_writes("READ_ROTATION_UNCONFIRMED")
    evidence = prepared["old_evidence"]
    old_request = decode_payload(old.mailbox.region.read(REQUEST_OFFSET, old.mailbox._header()[7]))
    fingerprint = json.dumps([1, ACCOUNT, old_request["method"], old_request["params"]],
                             sort_keys=True, allow_nan=False, separators=(",", ":"))
    proxy._route_faults["readonly"] = dict(state="RESULT_UNKNOWN", trigger_request_id=evidence["request_id"],
        sequence=evidence["sequence"], previous_unconfirmed=False)
    proxy._unconfirmed_read_ids["readonly"].add(evidence["request_id"])
    proxy._replays[evidence["request_id"]] = (fingerprint, dict(status="UNKNOWN", sequence=evidence["sequence"]))
    record = dict(schema_version=1, state="COMMITTED", transition_id=prepared["transition_id"],
        account_id=ACCOUNT, environment="SIM", automatic_retry=False, native_pid=os.getpid(),
        native_creation_filetime=birth, windows_session_id=1, execution_session_id=execution.bridge.mailbox.session_id,
        old_read_name=old.mailbox.name, old_read_session_id=old.mailbox.session_id,
        old_request_id=evidence["request_id"], old_method=evidence["method"], old_sequence=evidence["sequence"],
        old_previous_unconfirmed=False,
        old_read_expires_at_unix=evidence["read_expires_at_unix"], native_evidence=evidence,
        prepare_nonce="1" * 32, candidate_read_name=prepared["candidate_read_name"],
        candidate_read_session_id=prepared["candidate_read_session_id"], probe_request_id=prepared["probe_request_id"],
        probe_sequence=1, probe_source=probe_response["data"], token=prepared["token"], commit_nonce="2" * 32,
        commit_evidence=commit_receipt)
    proxy._rotation_committed_record = copy.deepcopy(record)
    supervisor_evidence = dict(contract="READ_ROTATION_SUPERVISOR_EVIDENCE_V1", transition_id=record["transition_id"],
        account_id=ACCOUNT, environment="SIM", native_pid=os.getpid(), native_creation_filetime=birth,
        windows_session_id=1, execution_session_id=record["execution_session_id"], old_read_name=record["old_read_name"],
        old_read_session_id=record["old_read_session_id"], old_request_id=record["old_request_id"],
        old_method=record["old_method"], old_sequence=record["old_sequence"],
        old_confirmed_sequence=evidence["confirmed_sequence"], old_expires_at_unix=record["old_read_expires_at_unix"],
        new_read_name=record["candidate_read_name"], new_read_session_id=record["candidate_read_session_id"],
        new_probe_request_id=record["probe_request_id"], new_probe_sequence=1, automatic_retry=False)
    verified = dict(record, state="SUPERVISOR_VERIFIED", supervisor_pid=parent_pid,
                    supervisor_create_time=proxy_module.psutil.Process(parent_pid).create_time(),
                    supervisor_creation_filetime=456, supervisor_verified_at_unix=time.time(),
                    supervisor_evidence=supervisor_evidence)
    provider.hashes[proxy._read_rotation_key] = {record["transition_id"]: json.dumps(verified, sort_keys=True,
                                                                               separators=(",", ":"))}

    class Registration:
        def register(self, request):
            return dict(contract="SUPERVISOR_ROTATION_AUTH_V1", action="REGISTER", status="REGISTERED",
                        stage=request["stage"], transition_id=request["transition_id"], nonce=request["nonce"],
                        capability="3" * 64)

        def close(self):
            pass

    proxy._supervisor_rotation_channel = Registration()

    def drive(function):
        with ThreadPoolExecutor(1) as executor:
            future = executor.submit(function)
            until = time.monotonic() + 3
            while not future.done() and time.monotonic() < until:
                native.namespace["aqp_shm_readonly_adjust"](native.context)
                native.namespace["aqp_shm_rpc_adjust"](native.context)
                time.sleep(0.001)
            return future.result(timeout=0.2)

    return native, proxy, provider, pipe, old, old_state, old_frame, candidate, record, verified, drive


@pytest.mark.parametrize("previous", [False, None, True, "missing"])
def test_rotation_old_read_requires_explicit_no_previous_unconfirmed(monkeypatch, tmp_path, previous):
    native, proxy, _, _, _, old_state, old_frame, _, record, _, _ = prepared_proxy_activation(monkeypatch, tmp_path)
    try:
        if previous == "missing":
            del proxy._route_faults["readonly"]["previous_unconfirmed"]
        else:
            proxy._route_faults["readonly"]["previous_unconfirmed"] = previous
        if previous is False:
            proxy._verify_old_read_rotation_evidence(record["native_evidence"])
        else:
            with pytest.raises(ShmProtocolError, match="old read completion evidence"):
                proxy._verify_old_read_rotation_evidence(record["native_evidence"])
        assert bytes(old_state.data) == old_frame and record["old_request_id"] in proxy._replays
    finally:
        proxy.close()
        native.close()


def test_rotation_old_read_requires_immediately_preceding_confirmation(monkeypatch, tmp_path):
    native, proxy, _, _, _, old_state, old_frame, _, record, _, _ = prepared_proxy_activation(monkeypatch, tmp_path)
    try:
        evidence = dict(record["native_evidence"], sequence=3, request_sequence=3, response_sequence=3, confirmed_sequence=1)
        proxy._route_faults["readonly"]["sequence"] = 3
        proxy._replays[record["old_request_id"]][1]["sequence"] = 3
        with pytest.raises(ShmProtocolError, match="sequence"):
            proxy._verify_old_read_rotation_evidence(evidence)
        assert bytes(old_state.data) == old_frame
    finally:
        proxy.close()
        native.close()


@pytest.mark.parametrize("change", [None, "wrong_error", "wrong_operation", "wrong_transition", "wrong_identity",
                                   "wrong_old_rid", "old_frame_changed", "old_frame_busy", "wrong_persisted_state"])
def test_native_capacity_evidence_requires_exact_binding_and_preserved_old_frame(monkeypatch, tmp_path, change):
    native, proxy, provider, _, old, old_state, old_frame, _, record, _, _ = prepared_proxy_activation(monkeypatch, tmp_path)
    try:
        pending = dict(record, state="PREPARE_PENDING")
        if change == "wrong_persisted_state":
            pending["state"] = "COMMIT_PENDING"
        provider.hashes[proxy._read_rotation_key][record["transition_id"]] = json.dumps(pending, sort_keys=True,
                                                                                        separators=(",", ":"))
        command = dict(contract="READ_ROTATION_CONTROL_V1", action="PREPARE", transition_id=record["transition_id"],
                       nonce="4" * 32, capability="4" * 64, old_evidence=record["native_evidence"])
        evidence = dict(contract="READ_ROTATION_CAPACITY_V1", status="READ_ROTATION_CAPACITY_BLOCKED",
            native_winerror=1455, native_operation="CreateFileMappingW", transition_id=record["transition_id"],
            account_id=ACCOUNT, environment="SIM", native_pid=os.getpid(), native_creation_filetime=record["native_creation_filetime"],
            windows_session_id=1, old_read_name=old.mailbox.name, old_read_session_id=old.mailbox.session_id,
            old_request_id=record["old_request_id"], old_sequence=record["old_sequence"],
            execution_session_id=record["execution_session_id"], old_evidence=record["native_evidence"],
            old_frame_unchanged=True, automatic_retry=False)
        if change == "wrong_error":
            evidence["native_winerror"] = 5
        elif change == "wrong_operation":
            evidence["native_operation"] = "MapViewOfFile"
        elif change == "wrong_transition":
            evidence["transition_id"] = "f" * 32
        elif change == "wrong_identity":
            evidence["native_pid"] = os.getpid() + 1
        elif change == "wrong_old_rid":
            evidence["old_request_id"] = "other-rid"
        elif change == "old_frame_changed":
            old_state.data[REQUEST_OFFSET] ^= 1
        calls = []

        def control(command_value=None, *, field="read_rotation", timeout_seconds=None):
            calls.append(field)
            identity = dict(contract="READ_ROTATION_CONTROL_V1", action="PREPARE", transition_id=record["transition_id"],
                            automatic_retry=False)
            if field == "read_rotation":
                return {"data": {"read_rotation_control": dict(identity, status="AUTH_PENDING")}}
            return {"data": {"read_rotation_control_status": dict(identity, status="UNKNOWN", receipt=None),
                             "read_rotation_capacity_evidence": evidence}}

        monkeypatch.setattr(proxy, "_execution_control_ping", control)
        if change == "old_frame_busy":
            assert old_state.lock.acquire(False)
        try:
            with pytest.raises(ShmProtocolError, match="outcome is unconfirmed"):
                proxy._execution_rotation_control(command)
        finally:
            if change == "old_frame_busy":
                old_state.lock.release()
        observed = proxy.identity_snapshot().get("read_rotation_capacity_evidence")
        assert observed == (evidence if change is None else None)
        assert calls == ["read_rotation", "read_rotation_control_status"]
        with pytest.raises(ShmProtocolError, match="already attempted"):
            proxy._execution_rotation_control(command)
        assert calls == ["read_rotation", "read_rotation_control_status"]
        assert proxy.identity_snapshot()["write_ready"] is False and record["old_request_id"] in proxy._replays
        if change != "old_frame_changed":
            assert bytes(old_state.data) == old_frame
    finally:
        proxy.close()
        native.close()


@pytest.mark.parametrize("outcome", ["activated", "unknown", "bad_supervisor"])
def test_generated_rotation_activation_preserves_frames_execution_and_gateway_gate(monkeypatch, tmp_path, outcome):
    native, proxy, provider, pipe, old, old_state, old_frame, candidate, record, verified, drive = (
        prepared_proxy_activation(monkeypatch, tmp_path))
    original_exec = proxy._clients["execution"]
    if outcome == "bad_supervisor":
        verified["supervisor_evidence"]["new_read_session_id"] = "foreign-session"
        provider.hashes[proxy._read_rotation_key][record["transition_id"]] = json.dumps(verified, sort_keys=True,
                                                                                       separators=(",", ":"))
    elif outcome == "unknown":
        pipe.fail = True
    try:
        if outcome == "activated":
            assert drive(proxy.activate_readonly_rotation)["status"] == "ACTIVATED"
            saved = json.loads(provider.hashes[proxy._read_rotation_key][record["transition_id"]])
            assert saved["state"] == "ACTIVATED" and saved["activation_evidence"]["status"] == "ACTIVATED"
            assert proxy._clients["readonly"] is candidate and proxy._clients["execution"] is original_exec
            assert proxy._rotation_waiting_gateway is True and proxy.identity_snapshot()["write_ready"] is False
            request = FIXTURE.request("get_ticks", rid="new-read", params={"code": "000001.SZ"})
            request["expected_source_binding"] = proxy.identity_snapshot()["source_binding"]
            assert proxy.process_request(request)["status"] == "REJECTED"
            assert candidate.mailbox._header()[4:7] == [1, 1, 1]
            request = FIXTURE.request("get_ticks", rid=record["old_request_id"], params={"code": "000001.SZ"})
            request["expected_source_binding"] = proxy.identity_snapshot()["source_binding"]
            assert proxy.process_request(request)["status"] == "REJECTED"
            assert record["old_request_id"] in proxy._replays
        else:
            with pytest.raises(Exception):
                drive(proxy.activate_readonly_rotation)
            saved = json.loads(provider.hashes[proxy._read_rotation_key][record["transition_id"]])
            assert saved["state"] == ("ACTIVATE_PENDING" if outcome == "unknown" else "SUPERVISOR_VERIFIED")
            assert proxy._clients["readonly"] is old and proxy._clients["execution"] is original_exec
        assert bytes(old_state.data) == old_frame and not old.mailbox.closed
        assert len([item for item in pipe.requests if item["action"] == "ACTIVATE"]) == (0 if outcome == "bad_supervisor" else 1)
        assert "3" * 64 not in json.dumps(provider.hashes)
    finally:
        proxy.close()
        native.close()


@pytest.mark.parametrize("outcome", ["accepted", "concurrent", "clean", "multiple_read", "ledger_unknown", "manager_unavailable",
                                     "manager_unknown", "manager_busy_unknown", "bad_hash", "record_changed", "prepare_unavailable",
                                     "prepare_unknown", "commit_unknown", "ack_unknown"]
                         + ["busy%d_%s" % (stage, route) for stage in (1, 2, 3)
                            for route in ("old", "candidate", "execution")])
def test_gateway_acceptance_only_releases_rotation_gate_and_keeps_other_blocks(monkeypatch, tmp_path, outcome):
    native, proxy, provider, pipe, old, old_state, old_frame, candidate, record, _, drive = (
        prepared_proxy_activation(monkeypatch, tmp_path))
    try:
        assert drive(proxy.activate_readonly_rotation)["status"] == "ACTIVATED"
        active = json.loads(provider.hashes[proxy._read_rotation_key][record["transition_id"]])
        gateway_pid, gateway_birth = 912345, 1234.5
        binding = proxy.identity_snapshot()["source_binding"]
        gateway_evidence = dict(contract="GATEWAY_READ_ROTATION_V1", transition_id=record["transition_id"],
            old_read_session_id=record["old_read_session_id"], new_read_session_id=record["candidate_read_session_id"],
            execution_session_id=record["execution_session_id"], native_pid=os.getpid(),
            native_creation_filetime=record["native_creation_filetime"], windows_session_id=1,
            gateway_pid=gateway_pid, gateway_create_time=gateway_birth, read_client_binding=dict(binding),
            order_client_binding=dict(binding), profile_binding=dict(binding), execution_cursor_before=4,
            execution_cursor_after=4, financial_and_event_state_preserved=True)
        accepted = dict(active, state="GATEWAY_VERIFIED", gateway_pid=gateway_pid, gateway_create_time=gateway_birth,
                        gateway_evidence=gateway_evidence)
        raw = json.dumps(accepted, sort_keys=True, allow_nan=False, separators=(",", ":"))
        provider.hashes[proxy._read_rotation_key][record["transition_id"]] = raw

        real_psutil = proxy_module.psutil

        class GatewayProcess:
            def is_running(self):
                return True

            def ppid(self):
                return os.getppid()

            def create_time(self):
                return gateway_birth

        monkeypatch.setattr(proxy_module, "psutil", type("ProcessProvider", (), {
            "Process": staticmethod(lambda pid: GatewayProcess() if pid == gateway_pid else real_psutil.Process(pid))}))
        calls, entered, release = [], threading.Event(), threading.Event()

        class Acceptance:
            def register(self, request):
                calls.append(dict(request))
                if outcome == "concurrent" and request["action"] == "VERIFY":
                    entered.set()
                    assert release.wait(2)
                if request["contract"] == "SUPERVISOR_ROTATION_PROXY_ACCEPTANCE_V1":
                    action = request["action"]
                    if (outcome == "prepare_unknown" and action == "OPEN_PREPARE"
                            or outcome == "commit_unknown" and action == "OPEN_COMMITTED"
                            or outcome == "ack_unknown" and action == "OPEN_ACKNOWLEDGED"):
                        raise TimeoutError("fixture open receipt unknown")
                    status = ({"OPEN_PREPARE": "PREPARED", "OPEN_COMMITTED": "ACK",
                               "OPEN_ACKNOWLEDGED": "ACKNOWLEDGED"}[action])
                    if outcome == "prepare_unavailable" and action == "OPEN_PREPARE":
                        status = "UNAVAILABLE"
                    return dict(contract=request["contract"], action=action, status=status,
                                transition_id=request["transition_id"], record_sha256=request["record_sha256"])
                if outcome == "manager_unknown":
                    raise TimeoutError("acceptance response unknown")
                if outcome == "manager_busy_unknown":
                    raise ShmBusyError("private transport outcome unknown")
                if outcome == "manager_unavailable":
                    return dict(contract=request["contract"], action="VERIFY", status="UNAVAILABLE",
                                transition_id=request["transition_id"])
                if outcome == "record_changed":
                    provider.hashes[proxy._read_rotation_key][record["transition_id"]] = raw + " "
                return dict(contract=request["contract"], action="VERIFY", status="VERIFIED",
                    transition_id=request["transition_id"],
                    record_sha256=("0" * 64 if outcome == "bad_hash" else proxy_module.hashlib.sha256(raw.encode()).hexdigest()),
                    supervisor_pid=os.getppid(), supervisor_creation_filetime=456,
                    gateway_pid=gateway_pid, gateway_create_time=gateway_birth, native_pid=os.getpid(),
                    native_creation_filetime=record["native_creation_filetime"], windows_session_id=1,
                    execution_session_id=record["execution_session_id"], read_session_id=record["candidate_read_session_id"],
                    verified_at_unix=time.time(), rotation_read_only=True)

            def close(self):
                pass

        proxy._supervisor_rotation_channel = Acceptance()
        if outcome not in ("clean", "multiple_read", "ledger_unknown"):
            for reason in ("NATIVE_WRITE_RESULT_UNKNOWN", "EXECUTION_EVENT_GAP", "OPERATOR_WRITE_DISABLED"):
                proxy._block_writes(reason)
        if outcome == "multiple_read":
            proxy._unconfirmed_read_ids["readonly"].add("other-unknown-read")
            proxy._retired_read_ids.add("other-unknown-read")
        if outcome == "ledger_unknown":
            unknown = dict(request_id="other-write", state="UNKNOWN", binding={"account_id": ACCOUNT},
                           fingerprint=json.dumps([1, ACCOUNT, "submit_order", {}]))
            provider.hashes[proxy._write_ledger_key] = {"other-write": json.dumps(unknown)}
        prior_writes = (proxy._writes_blocked, proxy._write_block_reason, proxy._source_write_block_reason,
                        list(proxy._write_block_reasons))
        if outcome in ("accepted", "concurrent", "clean", "multiple_read", "ledger_unknown", "commit_unknown", "ack_unknown") or outcome.startswith("busy"):
            if outcome == "concurrent":
                with ThreadPoolExecutor(1) as executor:
                    future = executor.submit(proxy.accept_gateway_rotation)
                    assert entered.wait(2)
                    old_write = FIXTURE.request("submit_order", rid="queued-old-write")
                    old_write["expected_source_binding"] = dict(binding, read_session_id=record["old_read_session_id"])
                    with pytest.raises(Exception, match="in-flight operation"):
                        proxy.process_request(old_write)
                    release.set()
                    result = future.result(timeout=2)
                assert proxy.process_request(old_write)["status"] == "REJECTED"
                assert not provider.hashes.get(proxy._write_ledger_key)
                native.writes.assert_not_called()
            elif outcome.startswith("busy"):
                stage, route = outcome[4:].split("_")
                control_calls = copy.deepcopy(pipe.requests)
                old_header = old.mailbox._header()[4:7]
                region = {"old": old.mailbox.region, "candidate": candidate.mailbox.region,
                          "execution": proxy._clients["execution"].mailbox.region}[route]
                original_acquire = region.acquire
                attempts = []

                def observe_with_competing_mutex(milliseconds):
                    attempts.append(milliseconds)
                    if len(attempts) == int(stage):
                        assert region.state.lock.acquire(False)
                        try:
                            return original_acquire(milliseconds)
                        finally:
                            region.state.lock.release()
                    return original_acquire(milliseconds)

                monkeypatch.setattr(region, "acquire", observe_with_competing_mutex)
                assert proxy.accept_gateway_rotation() is None
                assert [call["action"] for call in calls] == ["VERIFY", "OPEN_PREPARE"][:int(stage) - 1]
                assert proxy._rotation_waiting_gateway and proxy._read_rotation_blocked
                assert bytes(old_state.data) == old_frame and candidate.mailbox._header()[4:7] == [1, 1, 1]
                assert old.mailbox._header()[4:7] == old_header and pipe.requests == control_calls
                assert (proxy._writes_blocked, proxy._write_block_reason, proxy._source_write_block_reason,
                        proxy._write_block_reasons) == prior_writes
                result = proxy.accept_gateway_rotation()
                assert [call["action"] for call in calls] == ["VERIFY", "OPEN_PREPARE", "OPEN_COMMITTED", "OPEN_ACKNOWLEDGED"]
                assert old.mailbox._header()[4:7] == old_header and pipe.requests == control_calls
            else:
                result = proxy.accept_gateway_rotation()
            assert result["status"] == ("READ_OPEN_DELIVERY_UNKNOWN" if outcome in ("commit_unknown", "ack_unknown")
                                       else "READ_ROTATION_ACCEPTED")
            if outcome in ("commit_unknown", "ack_unknown"):
                assert proxy._rotation_open_delivery == "UNKNOWN" and proxy._rotation_open_acknowledged is False
                with pytest.raises(Exception, match="already been attempted"):
                    proxy._claim_read_rotation(dict(schema_version=1, state="PREPARE_PENDING", transition_id="f" * 32,
                        account_id=ACCOUNT, environment="SIM", automatic_retry=False))
            assert not proxy._rotation_waiting_gateway and not proxy._read_rotation_blocked
            assert record["old_request_id"] not in proxy._unconfirmed_read_ids["readonly"]
            assert record["old_request_id"] in proxy._retired_read_ids and record["old_request_id"] in proxy._replays
            if outcome == "clean":
                assert proxy.identity_snapshot()["write_ready"] is True and proxy._write_block_reasons == []
            elif outcome == "multiple_read":
                assert proxy.identity_snapshot()["write_ready"] is False
                assert proxy._write_block_reasons == ["READ_SOURCE_UNTRUSTED"]
                assert "other-unknown-read" in proxy._unconfirmed_read_ids["readonly"]
            elif outcome == "ledger_unknown":
                assert proxy.identity_snapshot()["write_ready"] is False
                assert proxy._write_block_reasons == ["PERSISTED_WRITE_UNCONFIRMED"]
                assert json.loads(provider.hashes[proxy._write_ledger_key]["other-write"])["state"] == "UNKNOWN"
            else:
                assert proxy.identity_snapshot()["write_ready"] is False
                assert proxy._write_block_reasons == ["NATIVE_WRITE_RESULT_UNKNOWN", "EXECUTION_EVENT_GAP", "OPERATOR_WRITE_DISABLED"]
            proxy._inspect_read_rotation()
            assert proxy._read_rotation_blocked is False
            read = FIXTURE.request("get_ticks", rid="post-acceptance", params={"code": "000001.SZ"})
            read["expected_source_binding"] = binding
            assert drive(lambda: proxy.process_request(read))["ok"] is True
            assert candidate.mailbox._header()[4:7] == [2, 2, 2]
            original = FIXTURE.request("get_ticks", rid=record["old_request_id"], params={"code": "000001.SZ"})
            original["expected_source_binding"] = binding
            assert proxy.process_request(original)["status"] == "REJECTED"
            assert candidate.mailbox._header()[4:7] == [2, 2, 2]
        elif outcome in ("manager_unavailable", "prepare_unavailable"):
            assert proxy.accept_gateway_rotation() is None
            assert (proxy._gateway_acceptance_attempted is False) is (outcome == "manager_unavailable")
        else:
            with pytest.raises(Exception):
                proxy.accept_gateway_rotation()
            assert proxy._gateway_acceptance_attempted is True
            with pytest.raises(Exception, match="changed" if outcome == "record_changed" else "already attempted"):
                proxy.accept_gateway_rotation()
        if outcome not in ("accepted", "concurrent", "clean", "multiple_read", "ledger_unknown", "commit_unknown", "ack_unknown") and not outcome.startswith("busy"):
            assert proxy._rotation_waiting_gateway and proxy._read_rotation_blocked
            assert (proxy._writes_blocked, proxy._write_block_reason, proxy._source_write_block_reason,
                    proxy._write_block_reasons) == prior_writes
            assert candidate.mailbox._header()[4:7] == [1, 1, 1]
        expected_calls = (4 if outcome in ("accepted", "concurrent", "clean", "multiple_read", "ledger_unknown", "ack_unknown") or outcome.startswith("busy")
                          else 3 if outcome == "commit_unknown" else 2 if outcome in ("prepare_unavailable", "prepare_unknown") else 1)
        assert len(calls) == expected_calls and bytes(old_state.data) == old_frame and not old.mailbox.closed
        assert record["old_request_id"] in proxy._replays
    finally:
        proxy.close()
        native.close()
