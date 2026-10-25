# coding: utf-8
"""实际生成源的分证券Tick归属与边界；仅原生API及Win32使用本地替身。"""

import copy

import pytest

import test_dual_shm_single_file as DUAL
from bigqmt_signal_trader.transports.readonly_shm import ShmProtocolError
from bigqmt_signal_trader.transports.readonly_shm_client import ShmReadOnlyRpcClient


A, B = "000001.SZ", "600000.SH"
CONTRACT = "SCOPED_TICK_CAPTURE_V1"


def capture(**changes):
    value = dict(capture_id="capture-two", trade_date="20261022", start_time="20261022135900",
                 end_time="20261022140100", symbols=[A, B])
    value.update(changes)
    return value


def tick(**changes):
    value = dict(lastPrice=11.5, time=1792648800000, volume=100, amount=1150.0)
    value.update(changes)
    return value


class ScopedSource(DUAL.Fixture):
    def __init__(self, monkeypatch, codes=(A, B), *, live=True):
        values = DUAL.live_settings(symbols=list(codes)) if live else dict(
            symbols=list(codes), hard_deadline_unix=None, max_seconds=0, max_requests=0,
            persistent_simulation_read_write=True, enable_market_rpc=True)
        super().__init__(monkeypatch, **values)
        self.native_subscribes, self.native_cancels, self.callbacks = [], [], {}
        self.early, self.fail_cancels = {}, set()
        self.expected_cleanup_failure = False

        def subscribe(code, **kwargs):
            handle = 410 + len(self.native_subscribes)
            self.native_subscribes.append((code, handle))
            self.callbacks[code] = kwargs["callback"]
            if code in self.early:
                kwargs["callback"](copy.deepcopy(self.early[code]))
            return handle

        def unsubscribe(handle):
            self.native_cancels.append(handle)
            if handle in self.fail_cancels:
                raise RuntimeError("fixture native cancel outcome unknown")
            return True

        self.context.subscribe_quote, self.context.unsubscribe_quote = subscribe, unsubscribe

    def subscribe(self, code=A, contract=None, **changes):
        params = dict(code=code, period="tick", result_type="dict", dividend_type="none")
        if contract is not None:
            params["capture"] = contract
        params.update(changes)
        return self.call("read", "subscribe_quote", params)

    def poll(self, sid=None, after=0):
        params = dict(after_sequence=after)
        if sid is not None:
            params["subscription_id"] = sid
        return self.call("read", "poll_quote_events", params)

    def cancel(self, sid):
        return self.call("read", "unsubscribe_quote", {"subscription_id": sid})

    def finish(self):
        for client in self.clients.values():
            client.close()
        try:
            self.namespace["stop"](self.context)
        except (ValueError, RuntimeError):
            if not self.expected_cleanup_failure:
                raise
        assert len(self.native_cancels) == len(set(self.native_cancels))
        self.writes.assert_not_called()


@pytest.fixture
def source(monkeypatch):
    values = []

    def make(codes=(A, B), **kwargs):
        value = ScopedSource(monkeypatch, codes, **kwargs)
        values.append(value)
        value.start()
        return value

    yield make
    for value in values:
        value.finish()


@pytest.mark.parametrize("contract,code", [
    (capture(symbols=["%06d.SZ" % number for number in range(129)]), "000000.SZ"),
    (capture(symbols=[A, A]), A), (capture(symbols=[A, "999999.BJ"]), A),
    (capture(trade_date="20260230"), A), (capture(end_time="20261022135800"), A),
    (capture(end_time="20261022135900"), A),
    (capture(start_time="20261021135900"), A), (capture(), "999999.BJ"),
])
def test_entire_scoped_contract_is_rejected_before_the_first_native_subscription(source, contract, code):
    value = source(codes=sorted({A, B} | {"%06d.SZ" % number for number in range(129)}))
    response = value.subscribe(code, contract)
    assert response["ok"] is False
    assert value.native_subscribes == [] and value.native_cancels == []


def test_exact_capture_and_code_are_idempotent_and_conflicts_do_not_replace_handles(source):
    value = source()
    ping = value.call("read", "ping")["data"]
    assert ping["quote_subscription_contract"] == CONTRACT and ping["quote_subscription_limit"] == 128
    first = value.subscribe(A, capture())["data"]
    second = value.subscribe(B, capture())["data"]
    assert first["subscription_id"] != second["subscription_id"]
    assert value.subscribe(A, capture())["data"]["subscription_id"] == first["subscription_id"]
    assert len(value.native_subscribes) == 2
    assert value.subscribe(A, capture(end_time="20261022140200"))["ok"] is False
    assert value.subscribe(A)["ok"] is False
    assert value.native_cancels == [] and len(value.native_subscribes) == 2
    assert value.poll()["ok"] is False


def test_legacy_subscription_cannot_be_joined_by_a_scoped_capture(source):
    value = source()
    assert value.subscribe(A)["ok"] is True
    assert value.subscribe(B, capture())["ok"] is False
    assert len(value.native_subscribes) == 1 and value.native_cancels == []


@pytest.mark.parametrize("count", [82, 128])
def test_complete_live_scope_is_simultaneously_active_and_simulation_does_not_gain_permission(source, count):
    codes = ["%06d.SZ" % number for number in range(300000, 300000 + count)]
    live = source(codes)
    contract = capture(symbols=codes)
    owners = {code: live.subscribe(code, contract)["data"]["subscription_id"] for code in codes}
    assert len(set(owners.values())) == len(live.native_subscribes) == count
    for code, sid in owners.items():
        live.callbacks[code]({code: tick()})
        polled = live.poll(sid)["data"]
        assert polled["active"] is True and polled["active_subscription_count"] == count
        assert polled["capture"] == contract and polled["code"] == code
        assert polled["events"][0]["subscription_id"] == sid
        assert polled["events"][0]["sequence"] == 1
    for sid in owners.values():
        assert live.cancel(sid)["ok"] is True
        assert live.poll(sid)["data"]["active"] is False
    assert len(live.native_cancels) == count
    simulation = source(live=False)
    assert simulation.subscribe(A, capture())["ok"] is False
    assert simulation.native_subscribes == []


def test_native_synchronous_early_callback_keeps_capture_code_and_zero_based_initial_cursor(source):
    value = source()
    value.early[A] = {A: tick()}
    response = value.subscribe(A, capture())["data"]
    assert response["capture"] == capture() and response["initial_sequence"] == 0
    polled = value.poll(response["subscription_id"])["data"]
    assert polled["gap"] is False and polled["latest_sequence"] == 1
    event = polled["events"][0]
    assert event["capture_id"] == "capture-two" and event["code"] == A
    assert event["subscription_id"] == response["subscription_id"] and event["sequence"] == 1
    assert event["data"] == {A: tick()}


def test_each_subscription_has_independent_cursor_and_exact_cancel_tail(source):
    value = source()
    a = value.subscribe(A, capture())["data"]["subscription_id"]
    b = value.subscribe(B, capture())["data"]["subscription_id"]
    value.callbacks[A]({A: tick()})
    value.callbacks[A]({A: tick(time=1792648800001)})
    value.callbacks[B]({B: tick(time=1792648800002)})
    assert [row["sequence"] for row in value.poll(a)["data"]["events"]] == [1, 2]
    assert [row["sequence"] for row in value.poll(b)["data"]["events"]] == [1]
    assert value.poll(a, 2)["data"]["events"] == []
    assert value.cancel(a)["data"]["cancelled"] is True
    value.callbacks[A]({A: tick(time=1792648800003)})
    tail_a = value.poll(a, 2)["data"]
    assert tail_a["active"] is False and tail_a["events"] == []
    value.callbacks[B]({B: tick(time=1792648800004)})
    assert [row["sequence"] for row in value.poll(b, 1)["data"]["events"]] == [2]
    assert value.cancel(b)["data"]["cancelled"] is True
    assert value.poll(b, 2)["data"]["active"] is False
    baseline = value.poll()["data"]
    assert baseline["subscription_id"] is None and baseline["events"] == [] and baseline["active_subscription_count"] == 0
    assert value.native_cancels == [handle for _, handle in value.native_subscribes]


def test_event_ring_overflow_is_local_to_its_sid(source):
    value = source()
    a = value.subscribe(A, capture())["data"]["subscription_id"]
    b = value.subscribe(B, capture())["data"]["subscription_id"]
    for number in range(33):
        value.callbacks[A]({A: tick(time=1792648800000 + number)})
    value.callbacks[B]({B: tick()})
    overflow = value.poll(a)["data"]
    assert overflow["gap"] is True and overflow["dropped_count"] == 1
    assert overflow["oldest_sequence"] == 2 and len(overflow["events"]) == 32
    other = value.poll(b)["data"]
    assert other["gap"] is False and other["callback_error"] is None and len(other["events"]) == 1


@pytest.mark.parametrize("raw", [{B: tick()}, {A: tick(blob="x" * 3000)}, {A: tick(lastPrice=0)}])
def test_wrong_symbol_oversized_or_invalid_callback_preserves_failure_for_only_its_owner(source, raw):
    value = source()
    a = value.subscribe(A, capture())["data"]["subscription_id"]
    b = value.subscribe(B, capture())["data"]["subscription_id"]
    value.callbacks[A](raw)
    failure = value.poll(a)["data"]
    assert failure["callback_error"] and failure["events"] == []
    value.callbacks[B]({B: tick()})
    assert value.poll(b)["data"]["callback_error"] is None


def test_failed_cancel_closes_callback_gate_preserves_owner_and_never_retries_on_close_or_reload(source):
    value = source()
    response = value.subscribe(A, capture())["data"]
    sid, handle = response["subscription_id"], response["native_handle"]
    value.fail_cancels.add(handle)
    value.expected_cleanup_failure = True
    assert value.cancel(sid)["ok"] is False
    value.callbacks[A]({A: tick()})
    polled = value.poll(sid)["data"]
    assert polled["active"] is False and polled["events"] == []
    assert polled.get("cleanup_error")
    assert value.cancel(sid)["ok"] is False
    runtime = value.namespace["_RUNTIMES"]["read"]
    for _ in range(2):
        with pytest.raises((ValueError, RuntimeError)):
            runtime["_SERVER"].close("reload")
    with pytest.raises((ValueError, RuntimeError)):
        runtime["init"](value.context)
    assert value.native_cancels == [handle]


def test_a_new_source_session_rejects_the_old_scoped_sid(source):
    first = source()
    old = first.subscribe(A, capture())["data"]["subscription_id"]
    first.finish()
    second = source()
    assert second.poll(old)["ok"] is False and second.native_subscribes == []


def test_client_validator_accepts_the_new_scoped_shape_without_changing_legacy_history():
    original = capture()
    before = copy.deepcopy(original)
    assert ShmReadOnlyRpcClient._capture_params(original, A) == original
    assert original == before
    params = dict(code=A, period="tick", result_type="dict", dividend_type="none", capture=original)
    assert ShmReadOnlyRpcClient._market_params("subscribe_quote", params) == params
    poll = dict(after_sequence=0, subscription_id="source-sid-1")
    assert ShmReadOnlyRpcClient._market_params("poll_quote_events", poll) == poll
    legacy = dict(code=A, period="1m", start_time="20261022093000", end_time="20261022150100", count=301, divid_type="none")
    assert ShmReadOnlyRpcClient._market_params("get_local_data", legacy) == legacy
    with pytest.raises(ShmProtocolError):
        ShmReadOnlyRpcClient._quote_params({"codes": ["%06d.SZ" % number for number in range(65)]})


@pytest.mark.parametrize("count", [82, 128, 129])
def test_client_enforces_the_complete_capture_symbol_boundary(count):
    codes = ["%06d.SZ" % number for number in range(300000, 300000 + count)]
    original = capture(symbols=codes)
    if count == 129:
        with pytest.raises(ShmProtocolError):
            ShmReadOnlyRpcClient._capture_params(original, codes[0])
    else:
        assert ShmReadOnlyRpcClient._capture_params(original, codes[0]) == original


@pytest.mark.parametrize("bad", [capture(symbols=[A, A]), capture(capture_id=" "), capture(trade_date="20260230"),
    capture(start_time="20261021135900"), capture(end_time="20261022135900"),
    capture(symbols=["%06d.SZ" % number for number in range(129)])])
def test_client_capture_validation_rejects_invalid_contracts(bad):
    with pytest.raises(ShmProtocolError):
        ShmReadOnlyRpcClient._capture_params(bad, A)


@pytest.mark.parametrize("sid", ["", "has space", 123, "x" * 129])
def test_client_scope_selector_must_be_an_explicit_bounded_sid(sid):
    with pytest.raises(ShmProtocolError):
        ShmReadOnlyRpcClient._market_params("poll_quote_events", {"after_sequence": 0, "subscription_id": sid})


@pytest.mark.parametrize("fault", ["gap", "callback"])
def test_cancelled_capture_never_releases_ownership_after_a_failed_terminal_read(source, fault):
    value = source()
    original = capture()
    a = value.subscribe(A, original)["data"]["subscription_id"]
    b = value.subscribe(B, original)["data"]["subscription_id"]
    if fault == "gap":
        for number in range(33):
            value.callbacks[A]({A: tick(time=1792648800000 + number)})
    else:
        value.callbacks[A]({A: tick(lastPrice=0)})
    assert value.cancel(a)["data"]["cancelled"] is True
    assert value.cancel(b)["data"]["cancelled"] is True
    failed = value.poll(a)["data"]
    if fault == "gap":
        assert failed["gap"] is True
    else:
        assert failed["callback_error"]
    assert value.poll(b)["ok"] is True
    before = list(value.native_subscribes)
    later = value.poll(a, failed["latest_sequence"])["data"]
    assert later["delivery_error"]
    if failed.get("delivery_error"):
        assert later["delivery_error"] == failed["delivery_error"]
    if fault == "gap":
        assert later["gap"] is False and later["dropped_count"] == failed["dropped_count"] == 1
    else:
        assert later["callback_error"] == failed["callback_error"]
    assert value.subscribe(A, capture(capture_id="next-capture"))["ok"] is False
    bridge = value.namespace["_RUNTIMES"]["read"]["_SERVER"]
    assert bridge.capture_contract == original
    assert a in bridge.capture_subscriptions and b in bridge.capture_subscriptions
    assert bridge.capture_subscriptions[a]["tail_delivered"] is False
    assert value.native_subscribes == before

    if fault == "gap":
        consumed = source()
        ca = consumed.subscribe(A, original)["data"]["subscription_id"]
        cb = consumed.subscribe(B, original)["data"]["subscription_id"]
        for number in range(31):
            consumed.callbacks[A]({A: tick(time=1792648800000 + number)})
        assert consumed.poll(ca)["data"]["latest_sequence"] == 31
        for number in (31, 32):
            consumed.callbacks[A]({A: tick(time=1792648800000 + number)})
        consumed.cancel(ca)
        consumed.cancel(cb)
        clean_tail = consumed.poll(ca, 31)["data"]
        assert clean_tail["dropped_count"] == 1 and clean_tail["gap"] is False
        assert [event["sequence"] for event in clean_tail["events"]] == [32, 33]
        assert consumed.poll(cb)["ok"] is True
        assert consumed.subscribe(A, capture(capture_id="next-clean-capture"))["ok"] is True
