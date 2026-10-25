"""ASYNC单RID终态核对；内存Redis/SHM，无现场访问或原生写入。"""

import copy
import datetime
import json
import time

import pytest

from test_async_redis_shm_ledger import (
    AsyncRedis, CID_INDEX, LEDGER, METHOD, async_calls, async_request, make_async_proxy,
)
from test_live_redis_shm_proxy import ACCOUNT, RUNNER
from bigqmt_signal_trader.transports.readonly_shm import ShmProtocolError


class ReconciliationRedis(AsyncRedis):
    """核对CAS同时比较原ledger原文与CID所有权；不改变CID索引。"""

    cas_mutator = None
    unknown_after_commit = False

    def eval(self, script, key_count, *args):
        if key_count != 2 or "HLEN" in script:
            return super().eval(script, key_count, *args)
        key, cid_key, rid, expected, replacement, cid = args
        self._record("eval", "reconcile", key, rid)
        if self.fail_eval == "reconcile":
            raise RuntimeError("injected reconciliation CAS failure")
        with self.ledger_lock:
            if self.cas_mutator is not None:
                self.cas_mutator(self)
            ledger = self.hashes.setdefault(key, {})
            index = self.hashes.setdefault(cid_key, {})
            if index.get(cid) != rid or ledger.get(rid) != expected:
                return 0
            ledger[rid] = replacement
            if self.unknown_after_commit:
                raise RuntimeError("CAS committed but acknowledgement was lost")
            return 1


IDENTITY = dict(user_order_id="async-cid-a", stock_code="000001.SZ", action="BUY", volume=100, price=10)


def unknown_submit():
    def mutate(request, response):
        if request["method"] == METHOD:
            response["data"] = dict(status="UNKNOWN", automatic_retry=False, error="fixture native outcome unknown")

    proxy, redis, _, execution = make_async_proxy(provider=ReconciliationRedis(), mutate=mutate)
    request = async_request()
    assert proxy.process_request(request)["status"] == "UNKNOWN"
    original = json.loads(redis.hashes[LEDGER][request["request_id"]])
    return proxy, redis, execution, request, original


def fresh_proxy(redis, *, read_sid="fresh-read-sid", execution_sid="fresh-execution-sid", native_pid=2853,
                native_blocked=False, start_write_blocked=True):
    proxy, _, read, execution = make_async_proxy(
        provider=redis, verify=False, execution_session_id=execution_sid,
        expected_readonly_session_id=read_sid, expected_native_pid=native_pid,
    )
    if start_write_blocked:
        proxy._block_writes("OPERATOR_WRITE_DISABLED")
    read.mailbox.session_id, read.mailbox.server_pid = read_sid, native_pid
    execution.mailbox.server_pid = native_pid

    def read_change(request, response):
        if request["method"] == "ping":
            response["data"]["native_pid"] = native_pid

    original_change = execution.mutate

    def execution_change(request, response):
        original_change(request, response)
        if request["method"] == "ping":
            response["data"].update(native_pid=native_pid, writes_blocked=native_blocked,
                                    write_block_reason="SOURCE_UNKNOWN" if native_blocked else "")

    read.mutate, execution.mutate = read_change, execution_change
    proxy.verify_identity()
    return proxy, execution


def terminal_snapshot(record, binding, *, status="54", filled=0):
    stamp = datetime.datetime.fromtimestamp(time.time(), datetime.timezone.utc).isoformat()
    order_time = int(record["claimed_at_unix"])
    order = dict(IDENTITY, order_sys_id="native-contract", status=status, traded_volume=filled, order_time=order_time)
    trades = ([dict(IDENTITY, order_sys_id="native-contract", trade_id="native-deal-1", volume=filled,
                    price=9.9, traded_time=order_time)] if filled else [])
    data = dict(account_id=ACCOUNT, environment="LIVE", orders=[order], trades=trades)
    source = dict(session_id=binding["execution_session_id"], native_pid=binding["native_pid"],
                  windows_session_id=binding["windows_session_id"], environment="LIVE")
    for key, detail, rows in (("orders_source_evidence", "ORDER", [order]), ("trades_source_evidence", "DEAL", trades)):
        data[key] = dict(contract_version=1, account_id=ACCOUNT, account_type="STOCK", detail_type=detail,
                         source="native_get_trade_detail_data", strategy_name="", complete=True,
                         status="OK" if rows else "EMPTY", error_code="", unknown_fields=[], row_errors=[],
                         raw_row_count=len(rows), converted_row_count=len(rows), returned_row_count=len(rows),
                         skipped_row_count=0, duplicate_row_count=0, queried_at=stamp, **source)
    data["source_evidence"] = dict(contract_version=1, account_id=ACCOUNT, source="native_ORDER_DEAL_queries",
                                   complete=True, status="OK", error_code="", queried_at=stamp,
                                   raw_row_count=1 + len(trades), returned_row_count=1 + len(trades), **source)
    return data


def crossing():
    old, redis, _, request, record = unknown_submit()
    proxy, execution = fresh_proxy(redis)
    transition = dict(original_binding=copy.deepcopy(record["binding"]), evidence_binding=proxy._write_binding())
    execution.result = terminal_snapshot(record, proxy._write_binding())
    return proxy, redis, execution, request, record, transition


def apply(proxy, request, preview, transition=None, **changes):
    kwargs = dict(expected_record=preview["expected_record"], operator="local-test-operator",
                  reason="exact original order terminal reconciliation", source_transition=transition, apply=True)
    kwargs.update(changes)
    return proxy.reconcile_unknown_write(request["request_id"], "native-contract", IDENTITY, **kwargs)


@pytest.mark.parametrize("status,filled", [("54", 0), ("57", 0), ("53", 40), ("56", 100)])
def test_same_source_async_terminal_proof_preserves_unknown_and_never_opens_writes(status, filled):
    proxy, redis, execution, request, record = unknown_submit()
    execution.result = terminal_snapshot(record, proxy._write_binding(), status=status, filled=filled)
    before = copy.deepcopy(redis.hashes)
    operations = len(redis.operations)
    preview = proxy.reconcile_unknown_write(request["request_id"], "native-contract", IDENTITY)
    assert redis.hashes == before and preview["proof"]["request_result"] == "BROKER_ORDER_FOUND"
    assert not any(op[0] in {"eval", "publish", "rpush", "setex", "expire"} for op in redis.operations[operations:])
    result = apply(proxy, request, preview)
    saved = json.loads(redis.hashes[LEDGER][request["request_id"]])
    assert result["status"] == "RECONCILED" and result["ledger_records_written"] == 1
    assert saved["binding"] == record["binding"] and saved["response"] == record["response"]
    assert saved["fingerprint"] == record["fingerprint"] and saved["client_order_id"] == record["client_order_id"]
    assert saved["response"]["status"] == "UNKNOWN" and saved["reconciliation"]["original_result"] == record["response"]
    assert saved["reconciliation"]["reason"] == "exact original order terminal reconciliation"
    assert redis.hashes[CID_INDEX] == before[CID_INDEX]
    assert proxy.identity_snapshot()["write_ready"] is False
    assert len(async_calls(execution)) == 1


def test_two_new_sources_reconcile_one_rid_keep_old_lineage_and_return_saved_result_only():
    proxy, redis, execution, request, record, transition = crossing()
    with pytest.raises(ShmProtocolError, match="transition"):
        proxy.reconcile_unknown_write(request["request_id"], "native-contract", IDENTITY)
    preview = proxy.reconcile_unknown_write(request["request_id"], "native-contract", IDENTITY,
                                            source_transition=transition)
    before_index = copy.deepcopy(redis.hashes[CID_INDEX])
    result = apply(proxy, request, preview, transition)
    saved = json.loads(redis.hashes[LEDGER][request["request_id"]])
    assert saved["binding"] == record["binding"] and saved["response"] == record["response"]
    assert saved["reconciliation"]["source_transition"] == transition
    assert saved["reconciled_response"]["session_id"] == transition["evidence_binding"]["execution_session_id"]
    assert result["writes_blocked"] is True
    restarted, fresh = fresh_proxy(redis)
    query = dict(request, expected_source_binding=restarted.identity_snapshot()["source_binding"])
    assert restarted.process_request(query)["status"] == "RECONCILED"
    assert restarted.identity_snapshot()["write_ready"] is False
    authorized, approved_client = fresh_proxy(redis, start_write_blocked=False)
    before_reason = authorized.identity_snapshot()["write_block_reason"]
    rejected = authorized.process_request(async_request("different-rid") | {
        "params": request["params"], "expected_source_binding": authorized.identity_snapshot()["source_binding"]})
    assert rejected["status"] == "REJECTED" and rejected["data"]["original_request_id"] == request["request_id"]
    assert authorized.identity_snapshot()["write_block_reason"] == before_reason
    assert redis.hashes[CID_INDEX] == before_index
    assert async_calls(execution) == async_calls(fresh) == async_calls(approved_client) == []


@pytest.mark.parametrize("field,bad", [("reason", None), ("reason", ""), ("reason", True),
                                      ("operator", ""), ("expected_record", None)])
def test_async_apply_requires_explicit_operator_reason_and_exact_preview(field, bad):
    proxy, redis, execution, request, record = unknown_submit()
    execution.result = terminal_snapshot(record, proxy._write_binding())
    preview = proxy.reconcile_unknown_write(request["request_id"], "native-contract", IDENTITY)
    before = copy.deepcopy(redis.hashes)
    with pytest.raises(ShmProtocolError):
        apply(proxy, request, preview, **{field: bad})
    assert redis.hashes == before


@pytest.mark.parametrize("field,bad", [("account_id", "other"), ("environment", "SIM"),
                                      ("windows_session_id", 2), ("windows_session_id", True),
                                      ("allow_order_methods", 1), ("runtime_policy", "BOUNDED"),
                                      ("native_pid", True), ("native_pid", -1)])
def test_cross_source_binding_cannot_change_target_policy_session_or_types(field, bad):
    proxy, redis, execution, request, record, transition = crossing()
    transition["evidence_binding"][field] = bad
    before, count = copy.deepcopy(redis.hashes), len(execution.calls)
    with pytest.raises(ShmProtocolError):
        proxy.reconcile_unknown_write(request["request_id"], "native-contract", IDENTITY, source_transition=transition)
    assert redis.hashes == before and len(execution.calls) == count


@pytest.mark.parametrize("change", ["missing_order", "nonterminal", "missing_source", "missing_trade",
                                  "wrong_cid", "wrong_limit", "wrong_day", "duplicate_contract"])
def test_absent_incomplete_conflicting_or_nonterminal_native_evidence_stays_unknown(change):
    proxy, redis, execution, request, record, transition = crossing()
    data = execution.result
    if change == "missing_order":
        data["orders"] = []
    elif change == "nonterminal":
        data["orders"][0]["status"] = "50"
    elif change == "missing_source":
        data.pop("orders_source_evidence")
    elif change == "missing_trade":
        data["orders"][0].update(status="56", traded_volume=100)
    elif change == "wrong_cid":
        data["orders"][0]["user_order_id"] = "another-cid"
    elif change == "wrong_limit":
        data["orders"][0]["price"] = 11
    elif change == "wrong_day":
        data["orders"][0]["order_time"] -= 86400
    else:
        data["orders"].append(dict(data["orders"][0]))
    before = copy.deepcopy(redis.hashes)
    with pytest.raises(ShmProtocolError):
        proxy.reconcile_unknown_write(request["request_id"], "native-contract", IDENTITY, source_transition=transition)
    assert redis.hashes == before and async_calls(execution) == []


def test_cid_change_during_cas_rejects_without_modifying_original_record():
    proxy, redis, execution, request, record, transition = crossing()
    preview = proxy.reconcile_unknown_write(request["request_id"], "native-contract", IDENTITY, source_transition=transition)
    original = redis.hashes[LEDGER][request["request_id"]]
    redis.cas_mutator = lambda provider: provider.hashes[CID_INDEX].update({"async-cid-a": "changed-rid"})
    with pytest.raises(ShmProtocolError, match="CAS"):
        apply(proxy, request, preview, transition)
    assert redis.hashes[LEDGER][request["request_id"]] == original
    assert proxy.identity_snapshot()["write_ready"] is False and async_calls(execution) == []


@pytest.mark.parametrize("committed", [False, True])
def test_cas_acknowledgement_failure_keeps_unknown_and_never_retries(committed):
    proxy, redis, execution, request, record, transition = crossing()
    preview = proxy.reconcile_unknown_write(request["request_id"], "native-contract", IDENTITY, source_transition=transition)
    redis.unknown_after_commit = committed
    redis.fail_eval = None if committed else "reconcile"
    result = apply(proxy, request, preview, transition)
    assert result["status"] == "UNKNOWN" and result["ledger_records_written"] is None
    assert proxy.identity_snapshot()["write_ready"] is False
    assert json.loads(redis.hashes[LEDGER][request["request_id"]])["response"] == record["response"]
    assert async_calls(execution) == []


def test_runner_transition_and_reason_plan_validation_is_offline_and_strict():
    proxy, _, _, request, record, transition = crossing()
    plan = dict(request_id=request["request_id"], order_sys_id="native-contract", order_identity=IDENTITY,
                expected_record=json.dumps(record), operator="local-test", reason="exact source transition",
                source_transition=transition, stopped_proxy_pid=1234)
    assert RUNNER.validate_reconciliation_plan(plan, apply=True)["source_transition"] == transition
    for mutate in (lambda p: p.pop("reason"), lambda p: p["source_transition"].update(extra=True),
                   lambda p: p["source_transition"]["evidence_binding"].update(windows_session_id=2)):
        broken = copy.deepcopy(plan)
        mutate(broken)
        with pytest.raises((ValueError, ShmProtocolError)):
            RUNNER.validate_reconciliation_plan(broken, apply=True)


@pytest.mark.parametrize("side", ["read_session_id", "execution_session_id"])
def test_cross_source_requires_two_fresh_sessions(side):
    proxy, redis, execution, request, record, transition = crossing()
    transition["evidence_binding"][side] = transition["original_binding"][side]
    before, calls = copy.deepcopy(redis.hashes), len(execution.calls)
    with pytest.raises(ShmProtocolError):
        proxy.reconcile_unknown_write(request["request_id"], "native-contract", IDENTITY, source_transition=transition)
    assert redis.hashes == before and len(execution.calls) == calls


@pytest.mark.parametrize("mutate", [
    lambda record: record["reconciliation"].pop("source_transition"),
    lambda record: record["reconciliation"]["source_transition"]["evidence_binding"].update(native_pid=999),
    lambda record: record["reconciliation"].update(reason=""),
    lambda record: record["reconciliation"]["original_result"].update(status="CONFIRMED"),
    lambda record: record["reconciled_response"]["source_binding"].update(execution_session_id="wrong-sid"),
])
def test_restart_revalidates_saved_cross_source_proof_without_rewriting_original(mutate):
    proxy, redis, _, request, record, transition = crossing()
    preview = proxy.reconcile_unknown_write(request["request_id"], "native-contract", IDENTITY, source_transition=transition)
    apply(proxy, request, preview, transition)
    saved = json.loads(redis.hashes[LEDGER][request["request_id"]])
    mutate(saved)
    redis.hashes[LEDGER][request["request_id"]] = json.dumps(saved)
    before = copy.deepcopy(redis.hashes)
    with pytest.raises(ShmProtocolError):
        fresh_proxy(redis)
    assert redis.hashes == before


def test_async_reconciliation_keeps_current_native_write_gate_and_other_unknown_record():
    _, redis, _, request, record = unknown_submit()
    other = copy.deepcopy(record)
    other["request_id"], other["client_order_id"] = "other-rid", "other-cid"
    fingerprint = json.loads(other["fingerprint"])
    fingerprint[3].update(client_order_id="other-cid", remark="other-cid")
    other["fingerprint"] = json.dumps(fingerprint)
    other["response"]["request_id"] = "other-rid"
    redis.hashes[LEDGER]["other-rid"] = json.dumps(other)
    redis.hashes[CID_INDEX]["other-cid"] = "other-rid"
    proxy, execution = fresh_proxy(redis, native_blocked=True)
    transition = dict(original_binding=record["binding"], evidence_binding=proxy._write_binding())
    execution.result = terminal_snapshot(record, proxy._write_binding())
    before = redis.hashes[LEDGER]["other-rid"]
    preview = proxy.reconcile_unknown_write(request["request_id"], "native-contract", IDENTITY, source_transition=transition)
    result = apply(proxy, request, preview, transition)
    assert result["writes_blocked"] is True and proxy.identity_snapshot()["write_ready"] is False
    assert proxy.identity_snapshot()["execution_source"]["writes_blocked"] is True
    assert redis.hashes[LEDGER]["other-rid"] == before


def test_reconciled_cid_cannot_be_reused_with_a_different_business_fingerprint():
    proxy, redis, _, request, record, transition = crossing()
    preview = proxy.reconcile_unknown_write(request["request_id"], "native-contract", IDENTITY, source_transition=transition)
    apply(proxy, request, preview, transition)
    authorized, execution = fresh_proxy(redis, start_write_blocked=False)
    before = copy.deepcopy(redis.hashes)
    response = authorized.process_request(async_request("conflicting-rid", volume=200) | {
        "expected_source_binding": authorized.identity_snapshot()["source_binding"]})
    assert response["status"] == "UNKNOWN" and response["data"]["native_write_executed"] is False
    assert authorized.identity_snapshot()["write_block_reason"] == "ASYNC_IDENTITY_CONFLICT"
    assert redis.hashes == before and async_calls(execution) == []


@pytest.mark.parametrize("fingerprint", ["broken-json", json.dumps([True, ACCOUNT, "submit_order", {}])])
def test_old_sync_unknown_with_malformed_fingerprint_cannot_be_ready(fingerprint):
    _, redis, _, request, record = unknown_submit()
    record.pop("client_order_id")
    record["fingerprint"] = fingerprint
    record["response"]["method"] = "submit_order"
    redis.hashes[LEDGER][request["request_id"]] = json.dumps(record)
    redis.hashes.pop(CID_INDEX)
    with pytest.raises(ShmProtocolError, match="fingerprint"):
        fresh_proxy(redis)
