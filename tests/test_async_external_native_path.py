# coding: utf-8
"""实际嵌入源码和passorder适配器的本地异步路径；不连接QMT或Redis。"""

from concurrent.futures import ThreadPoolExecutor
import json
import os
import time
from types import SimpleNamespace

import pytest

from bigqmt_signal_trader.redis_shm_proxy import RedisShmReadOnlyProxy
from bigqmt_signal_trader.transports.readonly_shm import ShmProtocolError
from bigqmt_signal_trader.transports.redis_transport import RedisTransport
from test_async_redis_shm_ledger import AsyncRedis
from test_external_redis_dual_shm_integration import DUAL
from test_live_redis_shm_proxy import ACCOUNT, LEDGER, POLICY, request


CODE = "000001.SZ"


class NativePath:
    def __init__(self, monkeypatch, *, callback_during_submit=False, fail_native=False,
                 redis_provider=None, native_query=None, start_write_blocked=False):
        self.source = DUAL.Fixture(
            monkeypatch, account_id=ACCOUNT, environment="LIVE",
            persistent_live_read_write=True, hard_deadline_unix=None,
            max_seconds=0, max_requests=0, symbols=[CODE],
        )
        self.redis = AsyncRedis() if redis_provider is None else redis_provider
        self.native_calls = []
        self.callback_during_submit = callback_during_submit
        self.fail_native = fail_native
        self.source.namespace["passorder"] = self.passorder
        if native_query is not None:
            self.source.namespace["get_trade_detail_data"] = (
                lambda *args, **kwargs: native_query(self.source.query, *args, **kwargs))
        self.source.start()
        self.source.namespace["account_callback"](
            self.source.context,
            SimpleNamespace(m_strAccountID=ACCOUNT, m_strStatus="登录成功",
                            m_nBrokerType=2, m_strAccountKey="LIVE-STOCK-FIXTURE-ONLY"),
        )
        self.proxy = RedisShmReadOnlyProxy(
            RedisTransport(self.redis, account_id=ACCOUNT),
            self.source.clients["read"], self.source.clients["execution"],
            account_id=ACCOUNT, environment="LIVE", runtime_policy=POLICY,
            allow_order_methods=True, start_write_blocked=start_write_blocked, live_write_authorized=True,
            receive_policy="QUEUE_ONLY", expected_native_pid=os.getpid(),
            expected_windows_session_id=1, timeout_seconds=1, max_requests=0,
            expected_readonly_session_id=self.source.clients["read"].mailbox.session_id,
            expected_execution_session_id=self.source.clients["execution"].mailbox.session_id,
        )
        self.execute(self.proxy.verify_identity)

    def execute(self, function, *args):
        with ThreadPoolExecutor(1) as pool:
            future = pool.submit(function, *args)
            deadline = time.monotonic() + 3
            while not future.done() and time.monotonic() < deadline:
                self.source.namespace["aqp_shm_readonly_adjust"](self.source.context)
                self.source.namespace["aqp_shm_rpc_adjust"](self.source.context)
                time.sleep(0.001)
            return future.result(timeout=0.2)

    def passorder(self, *args):
        cid = args[9]
        rid = self.redis.hget(LEDGER + ":cid", cid)
        assert rid, "调用原生前必须耐久认领CID"
        claim = json.loads(self.redis.hget(LEDGER, rid))
        assert claim["state"] == "CLAIMED" and claim["client_order_id"] == cid
        self.native_calls.append(args)
        if self.fail_native:
            raise RuntimeError("fixture native call outcome unknown")
        if self.callback_during_submit:
            self.report(cid, "contract-" + cid)

    def report(self, cid, contract):
        self.source.namespace["order_callback"](
            self.source.context,
            SimpleNamespace(
                m_strAccountID=ACCOUNT, m_strOrderSysID=contract,
                m_strInstrumentID="000001", m_strExchangeID="SZ", m_strRemark=cid,
                m_strStrategyName="async-local", m_nOffsetFlag=48, m_nOpType=23,
                m_nOrderStatus=50, m_nVolumeTotalOriginal=100, m_nVolumeTraded=0,
                m_dLimitPrice=10.0, m_dTradedPrice=0.0,
            ),
        )

    def send(self, rid, cid):
        value = request("submit_order_async", rid=rid, params=dict(
            client_order_id=cid, stock_code=CODE, action="BUY", volume=100,
            price=10.0, strategy_name="async-local",
        ), expected_source_binding=self.proxy.identity_snapshot()["source_binding"])
        return self.execute(self.proxy.process_request, value)

    def close(self):
        self.proxy.close()
        self.source.close()


def assert_pending(response, rid, cid):
    assert response["ok"] is True
    data = response["data"]
    assert data["status"] == "PENDING_CONFIRMATION"
    assert data["receipt_type"] == "NATIVE_CALL_RETURNED"
    assert data["request_id"] == rid and data["client_order_id"] == cid
    assert data["native_write_executed"] is True and data["broker_accepted"] is False
    assert data["qmt_async_seq"] is None and data["order_sys_id"] == ""


def test_actual_native_path_calls_second_before_first_contract_and_keeps_receipts_pending(monkeypatch):
    path = NativePath(monkeypatch)
    try:
        first = path.send("native-rid-a", "native-cid-a")
        assert_pending(first, "native-rid-a", "native-cid-a")
        second = path.send("native-rid-b", "native-cid-b")
        assert_pending(second, "native-rid-b", "native-cid-b")
        assert [call[9] for call in path.native_calls] == ["native-cid-a", "native-cid-b"]
        assert all(call[2:4] == (ACCOUNT, CODE) for call in path.native_calls)
        waiting = path.execute(path.proxy.process_request, request("ping", rid="two-native-pending"))
        assert waiting["data"]["execution_source"]["async_pending_count"] == 2
        assert all(json.loads(record)["state"] == "SUBMITTED_PENDING"
                   for record in path.redis.hgetall(LEDGER).values())
        path.report("native-cid-b", "contract-b")
        path.report("native-cid-a", "contract-a")
        events = path.execute(path.proxy.process_request, request(
            "poll_execution_events", rid="native-events", params={"after_sequence": 0}))
        reports = [item["event"] for item in events["data"]["events"]]
        assert [(row["user_order_id"], row["order_sys_id"]) for row in reports] == [
            ("native-cid-b", "contract-b"), ("native-cid-a", "contract-a")]
        settled = path.execute(path.proxy.process_request, request("ping", rid="both-native-reported"))
        assert settled["data"]["execution_source"]["async_pending_count"] == 0
        assert_pending(path.send("native-rid-a", "native-cid-a"), "native-rid-a", "native-cid-a")
        path.send("new-rid-same-cid", "native-cid-a")
        assert len(path.native_calls) == 2
    finally:
        path.close()


def test_native_callback_before_receipt_does_not_fabricate_async_acceptance(monkeypatch):
    path = NativePath(monkeypatch, callback_during_submit=True)
    try:
        assert_pending(path.send("early-rid-a", "early-cid-a"), "early-rid-a", "early-cid-a")
        assert_pending(path.send("early-rid-b", "early-cid-b"), "early-rid-b", "early-cid-b")
        assert len(path.native_calls) == 2
        state = path.execute(path.proxy.process_request, request("ping", rid="early-callback-state"))
        assert state["data"]["execution_source"]["async_pending_count"] == 0
    finally:
        path.close()


def test_partial_trade_before_order_with_price_improvement_keeps_next_submission_open(monkeypatch):
    path = NativePath(monkeypatch)
    try:
        assert_pending(path.send("partial-rid-a", "partial-cid-a"), "partial-rid-a", "partial-cid-a")
        path.source.namespace["deal_callback"](
            path.source.context,
            SimpleNamespace(
                m_strAccountID=ACCOUNT, m_strOrderSysID="partial-contract-a",
                m_strInstrumentID="000001", m_strExchangeID="SZ", m_strRemark="partial-cid-a",
                m_strTradeID="partial-trade-a", m_nOffsetFlag=48, m_nOpType=23,
                m_nVolume=40, m_dPrice=9.99, m_dTradeAmount=399.6,
            ),
        )
        state = path.execute(path.proxy.process_request, request("ping", rid="partial-trade-state"))
        assert state["data"]["writes_blocked"] is False
        assert state["data"]["execution_source"]["async_pending_count"] == 0
        assert_pending(path.send("partial-rid-b", "partial-cid-b"), "partial-rid-b", "partial-cid-b")
        assert len(path.native_calls) == 2
    finally:
        path.close()


def test_native_exception_stops_later_native_calls_without_resend(monkeypatch):
    path = NativePath(monkeypatch, fail_native=True)
    try:
        first = path.send("unknown-rid-a", "unknown-cid-a")
        assert first.get("status") == "UNKNOWN" or first["data"]["status"] == "UNKNOWN"
        path.send("unknown-rid-b", "unknown-cid-b")
        assert len(path.native_calls) == 1
        assert path.proxy.identity_snapshot()["writes_blocked"] is True
    finally:
        path.close()


def test_conflicting_contract_after_first_report_preserves_original_and_stops_new_native_calls(monkeypatch):
    path = NativePath(monkeypatch)
    try:
        assert_pending(path.send("late-rid-a", "late-cid-a"), "late-rid-a", "late-cid-a")
        path.report("late-cid-a", "original-contract-a")
        path.report("late-cid-a", "conflicting-contract-a")
        state = path.execute(path.proxy.process_request, request("ping", rid="conflicting-contract-state"))
        assert state["data"]["writes_blocked"] is True
        path.send("late-rid-b", "late-cid-b")
        assert len(path.native_calls) == 1
        claim = json.loads(path.redis.hget(LEDGER, "late-rid-a"))
        assert claim["response"]["data"]["receipt_type"] == "NATIVE_CALL_RETURNED"
        assert claim["response"]["data"]["broker_accepted"] is False
    finally:
        path.close()


def test_corrupted_receipt_after_native_call_reports_write_blocked_and_never_resends(monkeypatch):
    path = NativePath(monkeypatch)
    try:
        client = path.source.clients["execution"]
        send = client.send_request

        def corrupt_receipt(value, timeout_seconds):
            response = send(value, timeout_seconds=timeout_seconds)
            if value["method"] == "submit_order_async":
                response["data"]["windows_session_id"] = 99
            return response

        monkeypatch.setattr(client, "send_request", corrupt_receipt)
        response = path.send("corrupt-rid-a", "corrupt-cid-a")
        assert response["status"] == "UNKNOWN"
        state = path.proxy.identity_snapshot()
        assert state["writes_blocked"] is True and state["write_ready"] is False
        assert json.loads(path.redis.hget(LEDGER, "corrupt-rid-a"))["state"] == "UNKNOWN"
        with pytest.raises(ShmProtocolError):
            path.send("corrupt-rid-b", "corrupt-cid-b")
        assert len(path.native_calls) == 1
    finally:
        path.close()
