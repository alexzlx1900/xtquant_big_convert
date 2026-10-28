"""事件轮询超时证据：只用本地邮箱和内存替身，不连接真实账户。"""

import importlib.util
from pathlib import Path
import time

import pytest

import test_shm_rpc as rpc_fixture
from bigqmt_signal_trader import shm_rpc_bridge
from bigqmt_signal_trader.transports import readonly_shm


_SPEC = importlib.util.spec_from_file_location(
    "poll_timing_proxy_fixture", Path(__file__).parent / "bigqmt_signal_trader/test_redis_shm_proxy.py")
PROXY_FIXTURE = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(PROXY_FIXTURE)


class Clock:
    def __init__(self):
        self.wall_jump = 0.0
        self.monotonic_jump = 0.0

    def time(self):
        return time.time() + self.wall_jump

    def monotonic(self):
        return time.monotonic() + self.monotonic_jump

    def sleep(self, seconds):
        time.sleep(seconds)


def poll_request():
    return dict(request_id="poll-timing", method="poll_execution_events", account_id=rpc_fixture.ACCOUNT,
                params={"after_sequence": 0})


@pytest.mark.parametrize("monotonic_jump", [0.0, 2.0])
def test_expired_poll_keeps_both_clocks_and_never_runs_the_event_poll(monkeypatch, monotonic_jump):
    harness = rpc_fixture.Harness()
    client = readonly_shm.ShmReadOnlyClient(rpc_fixture.NAME, rpc_fixture.ACCOUNT,
                                           region=rpc_fixture.Region(harness.state))
    clock = Clock()
    original = harness.bridge._response
    calls = []

    def delay_validation(sequence, request):
        response = original(sequence, request)
        clock.wall_jump = 2.0
        clock.monotonic_jump = monotonic_jump
        return response

    monkeypatch.setattr(shm_rpc_bridge, "time", clock)
    monkeypatch.setattr(harness.bridge, "_response", delay_validation)
    monkeypatch.setattr(harness.bridge, "_poll_events", lambda params: calls.append(params))
    try:
        result = harness.execute(lambda: client.send_request(poll_request(), timeout_seconds=1))
        assert result["ok"] is False
        assert result["error"] == "request deadline expired/invalid; not executed"
        timing = result["read_timing"]
        wall_elapsed = timing["native_finished_at_unix"] - timing["native_started_at_unix"]
        monotonic_elapsed = timing["native_finished_at_monotonic"] - timing["native_started_at_monotonic"]
        lower = timing["native_finished_at_monotonic"] - timing["native_started_at_monotonic_after"]
        upper = timing["native_finished_at_monotonic_after"] - timing["native_started_at_monotonic"]
        assert wall_elapsed >= 1.9
        assert abs(monotonic_elapsed - monotonic_jump) < 0.5
        assert lower <= upper
        if monotonic_jump:
            assert lower - .01 <= wall_elapsed <= upper + .01
        else:
            assert wall_elapsed > upper + 1.0
        assert not calls
    finally:
        client.close()
        harness.close()


def test_native_clock_sample_reports_if_observation_itself_was_interrupted(monkeypatch):
    harness = rpc_fixture.Harness()

    class InterruptedClock(Clock):
        def time(self):
            self.wall_jump = 2.0
            self.monotonic_jump = 2.0
            return super().time()

    clock = InterruptedClock()
    monkeypatch.setattr(shm_rpc_bridge, "time", clock)
    try:
        timing = harness.bridge._response(1, poll_request())["read_timing"]
        assert timing["native_started_at_monotonic_after"] - timing["native_started_at_monotonic"] >= 1.9
    finally:
        harness.close()


def test_late_event_response_remains_unacknowledged_and_client_is_poisoned(monkeypatch):
    harness = rpc_fixture.Harness()
    clock = Clock()

    class LateResponseRegion(rpc_fixture.Region):
        def read(self, offset, length):
            raw = super().read(offset, length)
            if offset == readonly_shm.RESPONSE_OFFSET:
                clock.wall_jump = 2.0
                clock.monotonic_jump = 2.0
            return raw

    client = readonly_shm.ShmReadOnlyClient(rpc_fixture.NAME, rpc_fixture.ACCOUNT,
                                           region=LateResponseRegion(harness.state))
    monkeypatch.setattr(readonly_shm, "time", clock)
    try:
        with pytest.raises(readonly_shm.ShmTimeoutError, match="after its deadline") as failure:
            harness.execute(lambda: client.send_request(poll_request(), timeout_seconds=1))
        timing = failure.value.read_timing
        assert timing["native_finished_at_monotonic"] >= timing["native_started_at_monotonic"]
        assert timing["native_started_at_monotonic_after"] >= timing["native_started_at_monotonic"]
        assert timing["shm_failed_at_monotonic"] >= timing["shm_response_observed_at_monotonic"]
        assert timing["shm_failed_at_monotonic_after"] >= timing["shm_failed_at_monotonic"]
        assert client.poisoned and client.mailbox._header()[4:7] == [1, 1, 0]
        with pytest.raises(readonly_shm.ShmProtocolError, match="unconfirmed"):
            client.send_request(poll_request())
        assert client.mailbox._header()[4:7] == [1, 1, 0]
    finally:
        client.close()
        harness.close()


def test_proxy_records_poll_timeout_without_replaying_or_relaxing_unknown():
    proxy, redis, _, execution = PROXY_FIXTURE.RedisShmReadOnlyProxyTest().make_proxy()
    failure = readonly_shm.ShmTimeoutError("shared memory response timeout; result unconfirmed")
    failure.read_timing = {"shm_published_at_unix": 10.0, "shm_published_at_monotonic": 20.0,
                           "shm_failed_at_unix": 25.0, "shm_failed_at_monotonic": 35.0,
                           "untrusted_extra": "must-not-propagate"}
    execution.failure = failure
    request = PROXY_FIXTURE.request("poll_execution_events", rid="event-timeout", params={"after_sequence": 0})
    try:
        result = proxy.process_request(request)
        assert result["status"] == "UNKNOWN" and result["automatic_retry"] is False
        assert proxy.stopped and proxy.stop_reason == "SHM_RESULT_UNCONFIRMED"
        timing = proxy.diagnostic_snapshot()["last_error"]["read_timing"]
        assert timing["shm_published_at_monotonic"] == 20.0
        assert timing["shm_failed_at_monotonic"] == 35.0
        assert timing["proxy_shm_call_at_monotonic"] >= timing["proxy_received_at_monotonic"]
        assert timing["proxy_received_at_monotonic_after"] >= timing["proxy_received_at_monotonic"]
        assert "untrusted_extra" not in timing
        assert len([call for call, _ in execution.calls if call["method"] == "poll_execution_events"]) == 1
        assert not redis.hashes
    finally:
        proxy.close()
