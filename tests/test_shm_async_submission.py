# coding: utf-8
"""本地 SHM ASYNC 合同：原生调用、回报与通信未知必须保持不同状态。"""

import time

import pytest

import test_shm_rpc as SYNC
from bigqmt_signal_trader.models import OrderSubmitResult
from bigqmt_signal_trader.shm_rpc_bridge import ShmRpcBridge
from bigqmt_signal_trader.transports.readonly_shm import ShmProtocolError
from bigqmt_signal_trader.transports.shm_rpc import (
    ALL_WRITE_METHODS, ASYNC_SUBMIT_CONTRACT, ASYNC_WRITE_METHODS, WRITE_METHODS, ShmRpcClient, ShmRpcError,
)


METHOD = "submit_order_async"


@pytest.fixture
def harness():
    instances = []

    def make(*, windows_session_id=2, handshake=True, **kwargs):
        value = SYNC.Harness(True, **kwargs)
        instances.append(value)
        if windows_session_id is not None:
            value.bridge.runtime_metadata["windows_session_id"] = windows_session_id
        value.gateway.identity = False

        def forbidden_lookup(*args, **lookup_kwargs):
            raise AssertionError("ASYNC must return without a settlement query")

        value.gateway.query_orders = forbidden_lookup
        if handshake:
            value.handshake()
        return value

    yield make
    for value in instances:
        value.close()


def order(cid="cid-a", **changes):
    result = dict(SYNC.ORDER, client_order_id=cid, remark=cid)
    result.update(changes)
    return result


def submit(value, rid="rid-a", cid="cid-a", **changes):
    return value.execute(lambda: value.client.call(METHOD, order(cid, **changes), request_id=rid))


def ping(value):
    observer = ShmRpcClient(name=SYNC.NAME, account_id=SYNC.ACCOUNT,
        client_factory=lambda name, account: SYNC.ShmReadOnlyClient(name, account, region=SYNC.Region(value.state)))
    try:
        return value.execute(lambda: observer.call("ping"))
    finally:
        observer.close()


def native_order(cid="cid-a", **changes):
    result = dict(m_strAccountID=SYNC.ACCOUNT, m_strInstrumentID="000001", m_strExchangeID="SZ",
                  m_strRemark=cid, m_nOpType=23, m_nOffsetFlag=48, m_nVolumeTotalOriginal=100,
                  m_dLimitPrice=10, m_strOrderSysID="123456")
    result.update(changes)
    return result


def assert_pending(value, data, *, rid="rid-a", cid="cid-a"):
    expected = dict(status="PENDING_CONFIRMATION", receipt_type="NATIVE_CALL_RETURNED",
                    async_submit_contract=ASYNC_SUBMIT_CONTRACT, request_id=rid, client_order_id=cid,
                    account_id=SYNC.ACCOUNT, environment="SIM", session_id=value.bridge.mailbox.session_id,
                    native_pid=777, windows_session_id=2, native_write_executed=True, broker_accepted=False,
                    qmt_async_seq=None, order_sys_id="", automatic_retry=False)
    assert {key: data[key] for key in expected} == expected


def test_async_is_an_explicit_additive_method_and_both_native_calls_precede_confirmation(harness):
    assert WRITE_METHODS == frozenset(("submit_order", "cancel_order"))
    assert ASYNC_WRITE_METHODS == frozenset((METHOD,))
    assert ALL_WRITE_METHODS == WRITE_METHODS | ASYNC_WRITE_METHODS
    value = harness()
    identity = ping(value)
    assert METHOD in identity["capabilities"]
    assert identity["async_submit_contract"] == ASYNC_SUBMIT_CONTRACT
    assert identity["writes_blocked"] is False
    first = submit(value)
    assert_pending(value, first)
    assert ping(value)["async_pending_count"] == 1
    second = submit(value, "rid-b", "cid-b")
    assert_pending(value, second, rid="rid-b", cid="cid-b")
    assert [request.remark for request in value.gateway.submits] == ["cid-a", "cid-b"]
    assert ping(value)["async_pending_count"] == 2


def test_rid_replay_and_cross_rid_same_cid_do_not_repeat_native_submission(harness):
    value = harness()
    first = submit(value)
    assert submit(value) == first
    duplicate = submit(value, "different-rid", "cid-a")
    assert duplicate["status"] == "BLOCKED" and duplicate["native_write_executed"] is False
    assert duplicate["request_id"] == "different-rid" and duplicate["client_order_id"] == "cid-a"
    assert duplicate["writes_blocked"] is False
    assert len(value.gateway.submits) == 1


@pytest.mark.parametrize("rid", ["rid-a", "different-rid"])
def test_same_rid_or_cid_with_conflicting_fingerprint_poison_all_later_writes(harness, rid):
    value = harness()
    submit(value)
    try:
        result = submit(value, rid, "cid-a", volume=200)
    except ShmRpcError as exc:
        result = exc.response.get("data")
        assert result is not None and result["status"] == "UNKNOWN"
    else:
        assert result["status"] == "UNKNOWN"
    assert ping(value)["writes_blocked"] is True
    with pytest.raises(ShmProtocolError):
        submit(value, "rid-c", "cid-c")
    assert len(value.gateway.submits) == 1


def test_reverse_order_callbacks_bind_each_original_request_and_release_capacity(harness):
    value = harness(async_pending_capacity=2)
    submit(value)
    submit(value, "rid-b", "cid-b")
    value.bridge.record_execution_event("order", native_order("cid-b", m_strOrderSysID="654321"))
    assert ping(value)["async_pending_count"] == 1
    third = submit(value, "rid-c", "cid-c")
    assert_pending(value, third, rid="rid-c", cid="cid-c")
    value.bridge.record_execution_event("order", native_order())
    events = value.execute(lambda: value.client.poll_execution_events(0))["events"]
    assert [(item["event"]["client_order_id"], item["event"]["request_id"],
             item["event"]["order_sys_id"]) for item in events] == [
                 ("cid-b", "rid-b", "654321"), ("cid-a", "rid-a", "123456")]
    assert ping(value)["async_pending_count"] == 1
    assert len(value.gateway.submits) == 3


def test_callback_during_native_call_is_not_lost_and_receipt_still_does_not_claim_broker_acceptance(harness):
    value = harness(async_pending_capacity=1)
    original = value.gateway.submit

    def callback_before_return(request):
        result = original(request)
        value.bridge.record_execution_event("order", native_order(request.remark,
            m_strOrderSysID="contract-" + request.remark))
        return result

    value.gateway.submit = callback_before_return
    first = submit(value)
    assert_pending(value, first)
    assert ping(value)["async_pending_count"] == 0
    assert_pending(value, submit(value, "rid-b", "cid-b"), rid="rid-b", cid="cid-b")
    assert len(value.gateway.submits) == 2


def test_two_different_cids_cannot_claim_the_same_broker_contract(harness):
    value = harness()
    submit(value)
    submit(value, "rid-b", "cid-b")
    value.bridge.record_execution_event("order", native_order())
    value.bridge.record_execution_event("order", native_order("cid-b"))
    assert ping(value)["writes_blocked"] is True
    result = submit(value, "rid-c", "cid-c")
    assert result["status"] == "UNKNOWN" and result["native_write_executed"] is False
    assert len(value.gateway.submits) == 2


@pytest.mark.parametrize("changes", [dict(m_nVolume=101), dict(m_nVolume=40.5), dict(m_nOffsetFlag=49)])
def test_impossible_trade_facts_for_claimed_cid_poison_native_writes(harness, changes):
    value = harness()
    submit(value)
    trade = dict(m_strAccountID=SYNC.ACCOUNT, m_strInstrumentID="000001", m_strExchangeID="SZ",
                 m_strRemark="cid-a", m_nOffsetFlag=48, m_nVolume=40, m_dPrice=9.99,
                 m_strOrderSysID="123456", m_strTradeID="trade-a")
    trade.update(changes)
    value.bridge.record_execution_event("trade", trade)
    assert ping(value)["writes_blocked"] is True
    result = submit(value, "rid-b", "cid-b")
    assert result["status"] == "UNKNOWN" and result["native_write_executed"] is False
    assert len(value.gateway.submits) == 1


@pytest.mark.parametrize("field", [
    "m_strAccountID", "m_strInstrumentID", "m_strRemark", "m_nVolumeTotalOriginal", "m_dLimitPrice",
])
def test_incomplete_native_callback_keeps_pending_and_capacity_rejection_proves_no_native_call(harness, field):
    value = harness(async_pending_capacity=1)
    submit(value)
    callback = native_order()
    callback.pop(field)
    value.bridge.record_execution_event("order", callback)
    assert ping(value)["async_pending_count"] == 1
    blocked = submit(value, "rid-b", "cid-b")
    assert blocked["status"] == "BLOCKED" and blocked["native_write_executed"] is False
    assert blocked["writes_blocked"] is False and blocked["broker_accepted"] is False
    assert blocked["request_id"] == "rid-b" and blocked["client_order_id"] == "cid-b"
    assert len(value.gateway.submits) == 1


@pytest.mark.parametrize("contract", [None, "", "0", "-1", "UNKNOWN"])
def test_local_order_id_or_sequence_without_real_contract_never_confirms(harness, contract):
    value = harness()
    submit(value)
    raw = native_order(m_strOrderSysID=contract, order_id="LOCAL-123", seq=88)
    value.bridge.record_execution_event("order", raw)
    identity = ping(value)
    assert identity["async_pending_count"] == 1 and identity["writes_blocked"] is False
    event = value.execute(lambda: value.client.poll_execution_events(0))["events"][0]["event"]
    assert event["order_sys_id"] == "" and event["identity_status"] == "UNKNOWN"
    assert event["native_order_id"] == "LOCAL-123" and event["native_sequence"] == 88


@pytest.mark.parametrize("changes", [
    dict(m_strInstrumentID="600000", m_strExchangeID="SH"),
    dict(m_nOpType=24, m_nOffsetFlag=49), dict(m_nVolumeTotalOriginal=200), dict(m_dLimitPrice=11),
])
def test_matching_cid_with_conflicting_native_facts_makes_writes_unknown_without_resend(harness, changes):
    value = harness()
    submit(value)
    value.bridge.record_execution_event("order", native_order(**changes))
    assert ping(value)["writes_blocked"] is True
    blocked = submit(value, "rid-b", "cid-b")
    assert blocked["status"] == "UNKNOWN" and blocked["native_write_executed"] is False
    assert blocked["automatic_retry"] is False and blocked["writes_blocked"] is True
    assert len(value.gateway.submits) == 1


def test_pending_timeout_is_sticky_and_neither_new_request_nor_same_rid_resends(harness):
    value = harness(async_confirmation_timeout_seconds=0.04)
    assert_pending(value, submit(value))
    time.sleep(0.05)
    identity = ping(value)
    assert identity["writes_blocked"] is True and identity["write_block_reason"]
    unknown = submit(value, "rid-b", "cid-b")
    assert unknown["status"] == "UNKNOWN" and unknown["native_write_executed"] is False
    with pytest.raises(ShmProtocolError):
        submit(value)
    assert len(value.gateway.submits) == 1


def test_unconsumed_callback_gap_blocks_native_writes(harness):
    value = harness(event_capacity=1)
    submit(value)
    value.bridge.record_execution_event("order", native_order("unrelated-1"))
    value.bridge.record_execution_event("order", native_order("unrelated-2"))
    assert ping(value)["writes_blocked"] is True
    result = submit(value, "rid-b", "cid-b")
    assert result["status"] == "UNKNOWN" and result["native_write_executed"] is False
    assert len(value.gateway.submits) == 1


def test_source_session_change_does_not_repeat_a_pending_native_call(harness):
    value = harness()
    submit(value)
    old_session = value.bridge.mailbox.session_id
    value.bridge.close()
    value.bridge = ShmRpcBridge(name=SYNC.NAME, account_id=SYNC.ACCOUNT, handlers=value.handlers,
                                allow_order_methods=True, region=SYNC.Region(value.state))
    value.bridge.runtime_metadata["windows_session_id"] = 2
    assert value.bridge.mailbox.session_id != old_session
    result = submit(value, "rid-b", "cid-b")
    assert result["status"] == "UNKNOWN" and result["automatic_retry"] is False
    assert len(value.gateway.submits) == 1


@pytest.mark.parametrize("params", [
    dict(client_order_id=None), dict(client_order_id=""), dict(client_order_id="has space"),
    dict(client_order_id="x" * 129), dict(remark="different"), dict(order_remark="different"),
])
def test_invalid_cid_and_native_remark_conflict_are_rejected_before_native_call(harness, params):
    value = harness()
    with pytest.raises(ShmProtocolError):
        submit(value, **params)
    assert value.gateway.submits == []


@pytest.mark.parametrize("windows_session_id", [None, True, -1, "2"])
def test_unverified_windows_session_refuses_new_async_contract_and_keeps_sync_available(harness, windows_session_id):
    value = harness(windows_session_id=windows_session_id)
    with pytest.raises(ShmProtocolError):
        submit(value)
    value.gateway.query_orders = lambda account_id, stock_code: [
        SYNC.SimpleNamespace(user_order_id=value.gateway.submits[-1].remark, order_sys_id="REAL-SYNC")]
    sync = value.execute(lambda: value.client.call("submit_order", SYNC.ORDER, request_id="sync-rid"))
    assert sync["status"] == "CONFIRMED" and sync["order_sys_id"] == "REAL-SYNC"
    assert len(value.gateway.submits) == 1


@pytest.mark.parametrize("result", [
    OrderSubmitResult("REJECTED", "cid-a"), OrderSubmitResult("SUBMITTED", "another-cid"),
])
def test_native_return_without_exact_submitted_identity_is_unknown(harness, result):
    value = harness()
    calls = []

    def ambiguous_return(request):
        calls.append(request)
        return result

    value.gateway.submit = ambiguous_return
    data = submit(value)
    assert data["status"] == "UNKNOWN" and data["automatic_retry"] is False
    assert ping(value)["writes_blocked"] is True
    with pytest.raises(ShmProtocolError):
        submit(value, "rid-b", "cid-b")
    assert len(calls) == 1


@pytest.mark.parametrize("mutate", [
    lambda data: data.update(windows_session_id=99),
    lambda data: data.update(status="CONFIRMED", broker_accepted=True, order_sys_id="FORGED"),
    lambda data: (data.clear(), data.update(status="BLOCKED", native_write_executed=False)),
    lambda data: data.update(status="UNKNOWN", automatic_retry=False),
], ids=["different-windows-session", "forged-confirmed", "unbound-blocked", "unknown"])
def test_public_client_refuses_unsafe_async_body_and_cannot_send_again(harness, monkeypatch, mutate):
    value = harness()
    original = value.client._client.send_request

    def changed_response(request, **kwargs):
        response = original(request, **kwargs)
        if request["method"] == METHOD:
            mutate(response["data"])
        return response

    monkeypatch.setattr(value.client._client, "send_request", changed_response)
    result = submit(value)
    assert result["status"] == "UNKNOWN" and result["automatic_retry"] is False
    with pytest.raises(ShmProtocolError):
        submit(value, "rid-b", "cid-b")
    assert len(value.gateway.submits) == 1


def test_public_client_requires_a_real_method_list_to_negotiate_async(harness, monkeypatch):
    value = harness(handshake=False)
    value.client._client = SYNC.ShmReadOnlyClient(SYNC.NAME, SYNC.ACCOUNT, region=SYNC.Region(value.state))
    original = value.client._client.send_request

    def malformed_capabilities(request, **kwargs):
        response = original(request, **kwargs)
        if request["method"] == "ping":
            response["data"]["capabilities"] = METHOD
        return response

    monkeypatch.setattr(value.client._client, "send_request", malformed_capabilities)
    value.handshake()
    with pytest.raises(ShmProtocolError):
        submit(value)
    assert value.gateway.submits == []
