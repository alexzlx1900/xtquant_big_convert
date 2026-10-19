# coding: utf-8
"""本地邮箱协议 + 既有 handlers 验证，不构成真实下单验收。"""

import threading
import time
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

import pytest

from bigqmt_signal_trader.models import OrderRequest, OrderSubmitResult
from bigqmt_signal_trader.redis_rpc import BigQmtRpcHandlers, OrderSettlement
from bigqmt_signal_trader.shm_rpc_bridge import ShmRpcBridge
from bigqmt_signal_trader.transports.readonly_shm import MAP_SIZE, ShmReadOnlyClient, ShmProtocolError
from bigqmt_signal_trader.transports.shm_rpc import ShmRpcClient, ShmRpcError


NAME, ACCOUNT = "Local\\AQP_SHM_TRADE_TEST", "SIM_TEST"


class State:
    def __init__(self):
        self.data, self.lock = bytearray(MAP_SIZE), threading.Lock()


class Region:
    def __init__(self, state):
        self.state, self.pid, self.closed = state, 777, False

    def acquire(self, milliseconds):
        return self.state.lock.acquire(False) if not milliseconds else self.state.lock.acquire(timeout=milliseconds / 1000)

    def release(self):
        self.state.lock.release()

    def read(self, offset, length):
        return bytes(self.state.data[offset:offset + length])

    def write(self, offset, raw):
        self.state.data[offset:offset + len(raw)] = raw

    def close(self):
        self.closed = True


class Gateway:
    def __init__(self):
        self.submits, self.cancels, self.identity = [], [], True

    def submit(self, request):
        self.submits.append(request)
        return OrderSubmitResult("SUBMITTED", request.remark)

    def query_orders(self, account_id, stock_code):
        if self.identity and self.submits:
            return [SimpleNamespace(user_order_id=self.submits[-1].remark, order_sys_id="REAL-SYSID")]
        return []

    def cancel(self, ref):
        self.cancels.append(ref)
        return True


class Harness:
    def __init__(self, enabled=False, **kwargs):
        self.state, self.gateway = State(), Gateway()
        self.handlers = BigQmtRpcHandlers(ACCOUNT, None, None, self.gateway, allow_order_methods=enabled,
                                         order_settle_timeout_seconds=0.001)
        self.bridge = ShmRpcBridge(name=NAME, account_id=ACCOUNT, handlers=self.handlers,
                                   allow_order_methods=enabled, region=Region(self.state), **kwargs)
        # 原订单协议正向fixture显式注入登录证据替身；不代表真实QMT登录。
        self.bridge.trade_login_reader = lambda: dict(status="LOGGED_IN", account_id=ACCOUNT,
            session_id=self.bridge.mailbox.session_id, native_pid=777, source="native_account_callback", fixture_only=True,
            raw_status="FIXTURE_ONLY", event_observed_at_utc="2026-10-15T00:00:00Z",
            age_seconds=0.0, fresh=True, positive_contract_verified=True, reason="")
        self.client = ShmRpcClient(name=NAME, account_id=ACCOUNT, allow_order_methods=enabled,
                                   client_factory=lambda name, account: ShmReadOnlyClient(name, account, region=Region(self.state)))

    def execute(self, fn):
        with ThreadPoolExecutor(1) as pool:
            future = pool.submit(fn)
            deadline = time.monotonic() + 2
            while not future.done() and time.monotonic() < deadline:
                self.bridge.poll_once()
                time.sleep(0.001)
            return future.result(timeout=1)

    def handshake(self):
        return self.execute(lambda: self.client.verify_identity(expected_session_id=self.bridge.mailbox.session_id, expected_native_pid=777))

    def close(self):
        self.client.close()
        self.bridge.close()


ORDER = dict(stock_code="000001.SZ", action="BUY", volume=100, price=10)


def test_default_rejects_before_open_and_handler_permission_mismatch():
    h = Harness()
    with pytest.raises(ShmProtocolError):
        h.client.call("submit_order", ORDER)
    assert h.client._client is None and h.gateway.submits == []
    with pytest.raises(ValueError):
        ShmRpcBridge(name=NAME, account_id=ACCOUNT, handlers=h.handlers, allow_order_methods=True, region=Region(State()))
    h.close()


def test_fixed_account_and_explicit_identity():
    h = Harness(True)
    with pytest.raises(ShmProtocolError):
        h.client.call("submit_order", ORDER)
    with pytest.raises(ShmProtocolError):
        h.client.call("ping", {"account": {"id": "OTHER"}})
    h.handshake()
    with pytest.raises(ShmProtocolError):
        h.client.call("submit_order", ORDER, account_id="OTHER")
    assert not h.gateway.submits
    h.close()


def test_submit_uses_handlers_settlement_identity_and_replay():
    h = Harness(True)
    h.handshake()
    one = h.execute(lambda: h.client.call("submit_order", ORDER, request_id="stable-order"))
    replay = h.execute(lambda: h.client.call("submit_order", ORDER, request_id="stable-order"))
    assert one == replay and one["status"] == "CONFIRMED" and one["order_sys_id"] == "REAL-SYSID"
    assert len(h.gateway.submits) == 1 and h.gateway.submits[0].remark == "shm:stable-order"
    with pytest.raises(ShmRpcError):
        h.execute(lambda: h.client.call("submit_order", dict(ORDER, volume=200), request_id="stable-order"))
    assert len(h.gateway.submits) == 1
    h.close()


def test_missing_contract_and_cancel_are_unknown_not_fake_success():
    h = Harness(True)
    h.gateway.identity = False
    h.handshake()
    value = h.execute(lambda: h.client.call("submit_order", ORDER))
    assert value["status"] == "UNKNOWN" and value["automatic_retry"] is False and not value["order_sys_id"]
    value = h.execute(lambda: h.client.call("cancel_order", {"order_sys_id": "REAL-SYSID"}))
    assert value["status"] == "UNKNOWN" and len(h.gateway.cancels) == 1
    h.close()


def test_order_lookup_failure_remains_pending_until_deadline_and_retains_redacted_evidence():
    h = Harness(True)
    try:
        request = OrderRequest(signal_id="lookup", account_id=ACCOUNT, stock_code="000001.SZ", action="BUY",
                               volume=100, price=11.12, price_type="LIMIT", strategy_name="", remark="exact")
        settlement = OrderSettlement(request, OrderSubmitResult("SUBMITTED", "exact"), 0)

        def failed_query(*args):
            raise RuntimeError("redis://user:secret@host token=private")

        h.gateway.query_orders = failed_query
        assert h.handlers._apply_order_lookup(settlement, final=False) is False
        assert settlement.server_error == "ORDER_CONFIRMATION_LOOKUP_FAILED:RuntimeError; lookups=1; outcome=UNKNOWN"
        assert h.handlers._apply_order_lookup(settlement, final=True) is True
        assert "lookups=2" in settlement.server_error
        assert "secret" not in settlement.server_error and "private" not in settlement.server_error
        assert not settlement.result.order_sys_id and h.gateway.submits == []
    finally:
        h.close()


def test_order_lookup_transient_failure_can_be_confirmed_by_later_query_without_resubmission():
    h = Harness(True)
    try:
        request = OrderRequest(signal_id="lookup", account_id=ACCOUNT, stock_code="000001.SZ", action="BUY",
                               volume=100, price=11.12, price_type="LIMIT", strategy_name="", remark="exact")
        settlement = OrderSettlement(request, OrderSubmitResult("SUBMITTED", "exact"), 0)
        calls = []

        def lookup(*args):
            calls.append(True)
            if len(calls) == 1:
                raise RuntimeError("temporary native query failure")
            return [SimpleNamespace(user_order_id="exact", order_sys_id="1258")]

        h.gateway.query_orders = lookup
        assert h.handlers._apply_order_lookup(settlement) is False
        assert "outcome=UNKNOWN" in settlement.server_error
        assert h.handlers._apply_order_lookup(settlement) is True
        assert settlement.result.order_sys_id == "1258" and settlement.server_error == ""
        assert len(calls) == 2 and h.gateway.submits == []
    finally:
        h.close()


@pytest.mark.parametrize("contracts", [["1258", "1259"], ["UNKNOWN"], [""], ["  "], ["0.0"], ["-1.0"]])
def test_order_lookup_cannot_confirm_duplicate_remark_or_invalid_contract(contracts):
    h = Harness(True)
    try:
        request = OrderRequest(signal_id="lookup", account_id=ACCOUNT, stock_code="000001.SZ", action="BUY",
                               volume=100, price=11.12, price_type="LIMIT", strategy_name="", remark="exact")
        settlement = OrderSettlement(request, OrderSubmitResult("SUBMITTED", "exact"), 0)
        h.gateway.query_orders = lambda *args: [SimpleNamespace(user_order_id="exact", order_sys_id=value) for value in contracts]
        assert h.handlers._apply_order_lookup(settlement, final=True) is True
        assert not settlement.result.order_sys_id and "outcome=UNKNOWN" in settlement.server_error
    finally:
        h.close()


def test_timeout_poison_and_late_request_not_executed():
    h = Harness(True)
    h.handshake()
    # 没有callback处理时，协议真实发送后超时；不得重发。
    value = h.client.call("submit_order", ORDER, timeout_seconds=0.02)
    assert value["status"] == "UNKNOWN" and value["automatic_retry"] is False
    with pytest.raises(ShmProtocolError):
        h.client.call("submit_order", ORDER)
    h.bridge.poll_once()
    assert not h.gateway.submits
    h.close()


def test_execution_events_normalization_gap_and_missing_identity():
    h = Harness(event_capacity=2)
    for n in range(3):
        h.bridge.record_execution_event("order", {"m_strAccountID": ACCOUNT, "m_strInstrumentID": "000001",
                                                 "m_strExchangeID": "SZ", "m_strRemark": "order-%d" % n})
    result = h.execute(lambda: h.client.poll_execution_events(0))
    assert result["gap"] is True and result["dropped_before_sequence"] == 1
    assert [e["sequence"] for e in result["events"]] == [2, 3]
    assert all(e["event"]["identity_status"] == "UNKNOWN" and not e["event"]["order_sys_id"] for e in result["events"])
    assert all(e["event"]["stock_code"] == "000001.SZ" for e in result["events"])
    with pytest.raises(ShmProtocolError):
        h.bridge.record_execution_event("order", {"m_strAccountID": "OTHER"})
    with ThreadPoolExecutor(1) as pool:
        with pytest.raises(ShmProtocolError):
            pool.submit(h.bridge.poll_once).result()
    h.close()


def test_cache_full_refuses_new_requests_without_evicting_old():
    h = Harness(True, replay_capacity=2)
    h.handshake()
    h.execute(lambda: h.client.call("submit_order", ORDER, request_id="stable"))
    with pytest.raises(ShmRpcError):
        h.execute(lambda: h.client.call("submit_order", ORDER, request_id="new"))
    h.execute(lambda: h.client.call("submit_order", ORDER, request_id="stable"))
    assert len(h.gateway.submits) == 1
    h.close()


def test_handler_target_drift_refuses_and_oversized_event_exposes_loss():
    h = Harness(True)
    h.handshake()
    h.handlers.account_id = "OTHER"
    with pytest.raises(ShmRpcError):
        h.execute(lambda: h.client.call("submit_order", ORDER))
    assert not h.gateway.submits
    h.handlers.account_id = ACCOUNT
    h.bridge.record_execution_event("order", {"m_strAccountID": ACCOUNT, "m_strRemark": "x" * 5000})
    result = h.execute(lambda: h.client.poll_execution_events(0))
    assert result["gap"] and result["lost_sequence"] == 1
    assert result["events"][0]["event"]["data_loss"] is True
    h.close()


def test_connection_loss_after_send_attempt_is_unknown_and_closure_releases_views():
    h = Harness(True)
    h.handshake()
    original = h.client._client.send_request
    def disconnected(*args, **kwargs):
        raise OSError("connection lost during publication")
    h.client._client.send_request = disconnected
    value = h.client.call("submit_order", ORDER)
    assert value["status"] == "UNKNOWN" and value["automatic_retry"] is False
    h.client._client.send_request = original
    h.close()
    assert h.client._client.mailbox.region.closed and h.bridge.mailbox.region.closed


def test_server_default_gate_blocks_raw_protocol_write():
    h = Harness(False)
    raw = ShmReadOnlyClient(NAME, ACCOUNT, region=Region(h.state))
    value = h.execute(lambda: raw.send_request(dict(request_id="bypass", account_id=ACCOUNT,
                                                   method="submit_order", params=ORDER)))
    assert value["ok"] is False and value["server_error"] == "PermissionError"
    assert not h.gateway.submits
    raw.close()
    h.close()


def test_server_rejects_raw_nested_account_mismatch():
    h = Harness(True)
    raw = ShmReadOnlyClient(NAME, ACCOUNT, region=Region(h.state))
    value = h.execute(lambda: raw.send_request(dict(request_id="bad-target", account_id=ACCOUNT,
                                                   method="submit_order", params=dict(ORDER, account="OTHER"))))
    assert value["ok"] is False and not h.gateway.submits
    raw.close()
    h.close()


def test_late_order_settlement_does_not_turn_timeout_into_confirmed_or_resubmit():
    h = Harness(True)
    h.gateway.identity = False
    h.handlers.order_settle_timeout_seconds = 0.5
    h.handshake()
    value = h.execute(lambda: h.client.call("submit_order", ORDER, timeout_seconds=0.02, request_id="late"))
    assert value["status"] == "UNKNOWN" and len(h.gateway.submits) == 1
    h.gateway.identity = True
    h.bridge.poll_once()
    with pytest.raises(ShmProtocolError):
        h.client.call("submit_order", ORDER, request_id="late")
    assert len(h.gateway.submits) == 1
    h.close()


@pytest.mark.parametrize("event_type", ["order", "trade", "order_error", "cancel_error"])
@pytest.mark.parametrize("as_object", [False, True])
def test_native_local_order_id_is_not_contract_for_any_callback(event_type, as_object):
    h = Harness()
    source = dict(account_id=ACCOUNT, order_id=123, seq=456, request_id="request-only", user_order_id="remark-only")
    native = SimpleNamespace(**source) if as_object else source
    h.bridge.record_execution_event(event_type, native)
    event = h.bridge._events[-1]["event"]
    assert event["order_sys_id"] == "" and event["identity_status"] == "UNKNOWN"
    assert event["native_order_id"] == 123 and event["native_sequence"] == 456
    assert event["request_id"] == "request-only" and event["user_order_id"] == "remark-only"
    h.close()


@pytest.mark.parametrize("event_type", ["order", "trade", "order_error", "cancel_error"])
@pytest.mark.parametrize("contract", [None, "", "  ", 0, "0", -1, "-1"])
def test_invalid_explicit_contract_never_falls_back_to_local_order_id(event_type, contract):
    h = Harness()
    h.bridge.record_execution_event(event_type, dict(account_id=ACCOUNT, order_id=888, m_strOrderSysID=contract,
                                                   order_sys_id=contract, order_sysid=contract))
    event = h.bridge._events[-1]["event"]
    assert event["order_sys_id"] == "" and event["identity_status"] == "UNKNOWN"
    h.close()


@pytest.mark.parametrize("event_type", ["order", "trade", "order_error", "cancel_error"])
@pytest.mark.parametrize("field", ["m_strOrderSysID", "order_sys_id", "order_sysid"])
def test_real_explicit_contract_remains_known_in_each_callback(event_type, field):
    h = Harness()
    h.bridge.record_execution_event(event_type, dict(account_id=ACCOUNT, order_id=123, **{field: "SYS-REAL-42"}))
    event = h.bridge._events[-1]["event"]
    assert event["order_sys_id"] == "SYS-REAL-42" and event["identity_status"] == "KNOWN"
    assert event["native_order_id"] == 123
    h.close()
