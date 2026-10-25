# coding: utf-8
"""ASYNC 耐久收据与去重；仅使用内存替身，不接触 Redis/QMT 实机。"""

import copy
import json

import pytest

import test_live_redis_shm_proxy as LIVE
from bigqmt_signal_trader.transports.readonly_shm import ShmProtocolError


METHOD = "submit_order_async"
CONTRACT = "NATIVE_CALL_RETURNED_V1"
LEDGER = LIVE.LEDGER
CID_INDEX = LEDGER + ":cid"


class AsyncRedis(LIVE.FIXTURE.FakeRedis):
    """只扩展 ASYNC 双键原子 claim，旧单键 CAS 沿用现有 fixture。"""

    def eval(self, script, key_count, *args):
        if key_count == 1:
            return super().eval(script, key_count, *args)
        assert key_count == 2
        key, cid_key, rid, capacity, encoded, cid = args
        self._record("eval", "claim", key, rid)
        if self.fail_eval == "claim":
            raise RuntimeError("injected ledger failure")
        with self.ledger_lock:
            values = self.hashes.setdefault(key, {})
            index = self.hashes.setdefault(cid_key, {})
            if rid in values:
                return [0, values[rid]]
            if cid in index:
                return [-2, index[cid]]
            if len(values) >= int(capacity):
                return [-1, ""]
            values[rid] = encoded
            index[cid] = rid
            return [1, encoded]


def async_request(rid="async-rid-a", cid="async-cid-a", **param_changes):
    params = dict(client_order_id=cid, stock_code="000001.SZ", action="BUY",
                  volume=100, price=10, remark=cid)
    params.update(param_changes)
    return LIVE.request(METHOD, rid=rid, params=params, expected_source_binding=dict(
        read_session_id="readonly-sid", execution_session_id="execution-sid",
        native_pid=2852, windows_session_id=1))


def pending_receipt(request, execution):
    return dict(status="PENDING_CONFIRMATION", receipt_type="NATIVE_CALL_RETURNED",
                async_submit_contract=CONTRACT, request_id=request["request_id"],
                client_order_id=request["params"]["client_order_id"],
                account_id=LIVE.ACCOUNT, environment="LIVE",
                session_id=execution.mailbox.session_id, native_pid=2852,
                windows_session_id=1, native_write_executed=True, broker_accepted=False,
                qmt_async_seq=None, order_sys_id="", automatic_retry=False)


def make_async_proxy(*, provider=None, mutate=None, execution_session_id="execution-sid", **kwargs):
    def configure(_, execution):
        execution.mailbox.session_id = execution_session_id
        execution.additional_capabilities = frozenset((METHOD,))

        def response_change(request, response):
            if request["method"] == "ping":
                response["data"].update(async_submit_contract=CONTRACT,
                                        writes_blocked=False, write_block_reason="")
            elif request["method"] == METHOD:
                response["data"] = pending_receipt(request, execution)
            if mutate is not None:
                mutate(request, response)

        execution.mutate = response_change

    kwargs.setdefault("expected_execution_session_id", execution_session_id)
    return LIVE.make_proxy(provider=provider or AsyncRedis(), client_mutator=configure,
                           live_write_authorized=True, start_write_blocked=False, **kwargs)


def async_calls(execution):
    return [item for item in execution.calls if item[0]["method"] == METHOD]


def record(provider, rid):
    return json.loads(provider.hashes[LEDGER][rid])


def assert_new_write_refused(proxy, request):
    if proxy.stopped:
        with pytest.raises(ShmProtocolError):
            proxy.process_request(request)
    else:
        assert proxy.process_request(request)["ok"] is False


def test_normal_pending_receipts_do_not_block_the_next_source_submission():
    proxy, provider, _, execution = make_async_proxy()
    first, second = async_request(), async_request("async-rid-b", "async-cid-b")
    a, b = proxy.process_request(first), proxy.process_request(second)
    assert len(async_calls(execution)) == 2
    for request, response in ((first, a), (second, b)):
        assert response["ok"] is True
        assert response["data"] == pending_receipt(request, execution)
        assert record(provider, request["request_id"])["state"] == "SUBMITTED_PENDING"
    assert proxy.identity_snapshot()["writes_blocked"] is False
    assert provider.hashes[CID_INDEX] == {"async-cid-a": "async-rid-a", "async-cid-b": "async-rid-b"}


def test_same_rid_replay_and_cross_rid_same_cid_never_repeat_the_source_call():
    proxy, provider, _, execution = make_async_proxy()
    original = async_request()
    first = proxy.process_request(original)
    assert proxy.process_request(original) == first
    conflict = proxy.process_request(async_request("another-rid", "async-cid-a"))
    assert conflict["ok"] is False
    assert len(async_calls(execution)) == 1
    assert set(provider.hashes[LEDGER]) == {original["request_id"]}
    assert provider.hashes[CID_INDEX] == {"async-cid-a": original["request_id"]}


@pytest.mark.parametrize("rid", ["async-rid-a", "another-rid"])
def test_conflicting_fingerprint_on_claimed_rid_or_cid_stops_every_later_write(rid):
    proxy, provider, _, execution = make_async_proxy()
    original = async_request()
    first = proxy.process_request(original)
    before = copy.deepcopy(provider.hashes)
    conflict = proxy.process_request(async_request(rid, "async-cid-a", volume=200))
    assert conflict["status"] == "UNKNOWN" and conflict["automatic_retry"] is False
    assert proxy.identity_snapshot()["writes_blocked"] is True
    assert_new_write_refused(proxy, async_request("next-rid", "next-cid"))
    assert len(async_calls(execution)) == 1
    assert provider.hashes == before
    assert first["data"]["broker_accepted"] is False


def test_restarted_proxy_preserves_pending_receipt_and_cid_claim_without_resend():
    proxy, provider, _, execution = make_async_proxy()
    value = async_request()
    first = proxy.process_request(value)
    before = copy.deepcopy(provider.hashes)
    restarted, _, _, fresh = make_async_proxy(provider=provider)
    assert restarted.identity_snapshot()["writes_blocked"] is False
    assert restarted.process_request(value) == first
    rejected = restarted.process_request(async_request("retry-rid", "async-cid-a"))
    assert rejected["ok"] is False
    assert len(async_calls(execution)) == 1 and async_calls(fresh) == []
    assert provider.hashes == before


def test_transport_unknown_is_durable_and_blocks_new_writes_without_retry():
    proxy, provider, _, execution = make_async_proxy()
    execution.failure = RuntimeError("fixture transport outcome unknown")
    original = async_request()
    first = proxy.process_request(original)
    assert first["status"] == "UNKNOWN" and first["automatic_retry"] is False
    assert record(provider, original["request_id"])["state"] == "UNKNOWN"
    restarted, _, _, fresh = make_async_proxy(provider=provider)
    assert restarted.identity_snapshot()["writes_blocked"] is True
    assert restarted.process_request(original)["status"] == "UNKNOWN"
    blocked = restarted.process_request(async_request("new-rid", "new-cid"))
    assert blocked["ok"] is False
    assert async_calls(fresh) == []


@pytest.mark.parametrize("failure", ["settlement", "fanout"])
def test_response_persistence_or_fanout_failure_retains_claim_and_never_resends_on_restart(failure):
    proxy, provider, _, execution = make_async_proxy()
    request = async_request()
    if failure == "settlement":
        provider.fail_eval = "settle"
        assert proxy.process_request(request)["status"] == "UNKNOWN"
        assert record(provider, request["request_id"])["state"] == "CLAIMED"
        provider.fail_eval = None
    else:
        provider.fail_operation = "rpush"
        with pytest.raises(RuntimeError):
            proxy.process_request(request)
        assert record(provider, request["request_id"])["state"] == "PUBLISH_UNCONFIRMED"
        provider.fail_operation = None
    assert provider.hashes[CID_INDEX] == {"async-cid-a": "async-rid-a"}
    restarted, _, _, fresh = make_async_proxy(provider=provider)
    assert restarted.identity_snapshot()["writes_blocked"] is True
    assert restarted.process_request(request)["status"] == "UNKNOWN"
    assert_new_write_refused(restarted, async_request("another-rid", "async-cid-a"))
    assert async_calls(fresh) == [] and len(async_calls(execution)) == 1


@pytest.mark.parametrize("field", [
    "request_id", "client_order_id", "account_id", "environment", "session_id",
    "native_pid", "windows_session_id", "status", "receipt_type", "async_submit_contract",
    "native_write_executed", "broker_accepted", "qmt_async_seq", "order_sys_id", "automatic_retry",
])
def test_incomplete_pending_receipt_cannot_be_treated_as_safe_submitted_pending(field):
    def mutate(request, response):
        if request["method"] == METHOD:
            response["data"].pop(field)

    proxy, provider, _, execution = make_async_proxy(mutate=mutate)
    value = async_request()
    response = proxy.process_request(value)
    assert response["status"] == "UNKNOWN" and response["automatic_retry"] is False
    assert proxy.identity_snapshot()["writes_blocked"] is True
    assert record(provider, value["request_id"])["state"] == "UNKNOWN"
    assert_new_write_refused(proxy, async_request("next-rid", "next-cid"))
    assert len(async_calls(execution)) == 1


@pytest.mark.parametrize("field,value", [
    ("request_id", "other-rid"), ("client_order_id", "other-cid"),
    ("account_id", "55013746"), ("environment", "SIM"),
    ("session_id", "other-sid"), ("native_pid", 9999), ("native_pid", True),
    ("windows_session_id", 2), ("windows_session_id", True),
    ("receipt_type", "QUEUED"), ("async_submit_contract", "other-contract"),
    ("broker_accepted", True), ("qmt_async_seq", 123), ("order_sys_id", "LOCAL-ONLY"),
    ("native_write_executed", False), ("automatic_retry", True),
])
def test_receipt_identity_or_semantics_mismatch_poison_the_write_path(field, value):
    def mutate(request, response):
        if request["method"] == METHOD:
            response["data"][field] = value

    proxy, provider, _, execution = make_async_proxy(mutate=mutate)
    request = async_request()
    result = proxy.process_request(request)
    assert result["status"] == "UNKNOWN" and result["automatic_retry"] is False
    assert proxy.identity_snapshot()["writes_blocked"] is True
    assert record(provider, request["request_id"])["state"] == "UNKNOWN"
    assert len(async_calls(execution)) == 1


def blocked_receipt(request, execution):
    result = pending_receipt(request, execution)
    result.pop("receipt_type")
    result.update(status="BLOCKED", native_write_executed=False,
                  writes_blocked=False, write_block_reason="", error="ASYNC_PENDING_CAPACITY")
    return result


def test_capacity_backpressure_is_not_executed_and_survives_restart_without_resend():
    def mutate(request, response):
        if request["method"] == METHOD:
            response["data"] = blocked_receipt(request, execution)

    proxy, provider, _, execution = make_async_proxy(mutate=mutate)
    value = async_request()
    first = proxy.process_request(value)
    assert first["data"]["status"] == "BLOCKED"
    assert first["data"]["native_write_executed"] is False
    assert record(provider, value["request_id"])["state"] == "NOT_EXECUTED"
    assert proxy.identity_snapshot()["writes_blocked"] is False
    restarted, _, _, fresh = make_async_proxy(provider=provider)
    assert restarted.process_request(value) == first
    assert restarted.process_request(async_request("new-rid", "async-cid-a"))["ok"] is False
    assert async_calls(fresh) == []


def test_native_unknown_gate_is_not_a_definite_capacity_rejection():
    def mutate(request, response):
        if request["method"] == METHOD:
            response["data"].update(status="UNKNOWN", native_write_executed=False,
                                    writes_blocked=True, write_block_reason="ASYNC_CONFIRMATION_TIMEOUT")

    proxy, provider, _, execution = make_async_proxy(mutate=mutate)
    value = async_request()
    response = proxy.process_request(value)
    assert response["status"] == "UNKNOWN"
    assert proxy.identity_snapshot()["writes_blocked"] is True
    assert record(provider, value["request_id"])["state"] == "UNKNOWN"
    assert_new_write_refused(proxy, async_request("new-rid", "new-cid"))
    assert len(async_calls(execution)) == 1


@pytest.mark.parametrize("field", [
    "request_id", "client_order_id", "account_id", "environment", "session_id",
    "native_pid", "windows_session_id",
])
@pytest.mark.parametrize("wrong_value", [False, True], ids=["missing", "different"])
def test_not_executed_rejection_requires_the_same_complete_identity(field, wrong_value):
    def mutate(request, response):
        if request["method"] == METHOD:
            data = blocked_receipt(request, execution)
            if wrong_value:
                data[field] = "other" if field not in ("native_pid", "windows_session_id") else True
            else:
                data.pop(field)
            response["data"] = data

    proxy, provider, _, execution = make_async_proxy(mutate=mutate)
    value = async_request()
    response = proxy.process_request(value)
    assert response["status"] == "UNKNOWN" and response["automatic_retry"] is False
    assert proxy.identity_snapshot()["writes_blocked"] is True
    assert record(provider, value["request_id"])["state"] == "UNKNOWN"
    assert_new_write_refused(proxy, async_request("new-rid", "new-cid"))
    assert len(async_calls(execution)) == 1


@pytest.mark.parametrize("not_executed", [False, True], ids=["pending", "not-executed"])
@pytest.mark.parametrize("field,value", [
    ("request_id", "other-rid"), ("client_order_id", "other-cid"),
    ("account_id", "55013746"), ("environment", "SIM"), ("session_id", "other-sid"),
    ("native_pid", True), ("windows_session_id", True),
])
def test_restart_rejects_tampered_persisted_async_identity(field, value, not_executed):
    def mutate(request, response):
        if not_executed and request["method"] == METHOD:
            response["data"] = blocked_receipt(request, execution)

    proxy, provider, _, execution = make_async_proxy(mutate=mutate)
    request = async_request()
    proxy.process_request(request)
    existing = record(provider, request["request_id"])
    existing["response"]["data"][field] = value
    provider.hashes[LEDGER][request["request_id"]] = json.dumps(existing)
    with pytest.raises(ShmProtocolError):
        make_async_proxy(provider=provider)
    assert len(async_calls(execution)) == 1


def test_pending_receipt_from_previous_execution_session_blocks_after_restart_without_replay():
    proxy, provider, _, execution = make_async_proxy()
    proxy.process_request(async_request())
    restarted, _, _, fresh = make_async_proxy(provider=provider, execution_session_id="new-execution-sid")
    assert restarted.identity_snapshot()["writes_blocked"] is True
    value = async_request("new-rid", "new-cid")
    value["expected_source_binding"]["execution_session_id"] = "new-execution-sid"
    assert restarted.process_request(value)["ok"] is False
    assert async_calls(fresh) == [] and len(async_calls(execution)) == 1


def test_native_ping_unknown_gate_blocks_restart_even_with_valid_historical_receipt():
    proxy, provider, _, execution = make_async_proxy()
    proxy.process_request(async_request())

    def blocked_source(request, response):
        if request["method"] == "ping":
            response["data"].update(writes_blocked=True, write_block_reason="ASYNC_CONFIRMATION_DEADLINE")

    restarted, _, _, fresh = make_async_proxy(provider=provider, mutate=blocked_source)
    assert restarted.identity_snapshot()["writes_blocked"] is True
    assert restarted.process_request(async_request("new-rid", "new-cid"))["ok"] is False
    assert async_calls(fresh) == [] and len(async_calls(execution)) == 1


def test_missing_cid_index_cannot_be_recovered_by_guessing_or_resubmitting():
    proxy, provider, _, execution = make_async_proxy()
    proxy.process_request(async_request())
    provider.hashes[CID_INDEX].clear()
    with pytest.raises(ShmProtocolError):
        make_async_proxy(provider=provider)
    assert len(async_calls(execution)) == 1


def test_old_source_rejects_async_before_claim_and_keeps_existing_sync_submission():
    provider = AsyncRedis()
    proxy, _, _, execution = LIVE.make_proxy(provider=provider, live_write_authorized=True,
                                            start_write_blocked=False)
    response = proxy.process_request(async_request())
    assert response["ok"] is False and async_calls(execution) == []
    assert not any(item[0] == "eval" for item in provider.operations)
    execution.result = dict(status="SUBMITTED", order_sys_id="SYNC-FIXTURE-CONTRACT")
    value = proxy.process_request(LIVE.request("submit_order", rid="old-sync-rid"))
    assert value["ok"] is True
    assert record(provider, "old-sync-rid")["state"] == "CONFIRMED"
