"""持仓阶段计时的本地协议验收；不连接QMT、Redis或交易账户。"""

import importlib.util
from pathlib import Path
from types import SimpleNamespace
import time

import pytest

import test_shm_rpc as rpc_fixture
from bigqmt_signal_trader.transports import readonly_shm


_SPEC = importlib.util.spec_from_file_location(
    "position_timing_proxy_fixture", Path(__file__).parent / "bigqmt_signal_trader/test_redis_shm_proxy.py")
PROXY_FIXTURE = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(PROXY_FIXTURE)


@pytest.fixture
def native():
    harness = rpc_fixture.Harness()
    harness.handlers.position_provider = SimpleNamespace(get_positions=lambda account: {})
    yield harness
    harness.close()


@pytest.fixture
def proxy():
    values = PROXY_FIXTURE.RedisShmReadOnlyProxyTest().make_proxy()
    yield values
    values[0].close()


def request(rid="timing", method="get_positions"):
    return dict(request_id=rid, method=method, account_id=rpc_fixture.ACCOUNT, params={})


def test_native_completion_and_client_receive_are_separate_and_replay_keeps_native_time(native):
    client = readonly_shm.ShmReadOnlyClient(rpc_fixture.NAME, rpc_fixture.ACCOUNT,
                                          region=rpc_fixture.Region(native.state))
    try:
        first = native.execute(lambda: client.send_request(request(), timeout_seconds=1))
        second = native.execute(lambda: client.send_request(request(), timeout_seconds=1))
        timing = first["read_timing"]
        assert timing["shm_published_at_unix"] <= timing["native_started_at_unix"]
        assert timing["native_started_at_unix"] <= timing["native_finished_at_unix"]
        assert timing["native_finished_at_unix"] <= timing["shm_response_observed_at_unix"]
        assert timing["shm_response_observed_at_unix"] <= timing["shm_ack_at_unix"]
        for field in ("native_started_at_unix", "native_finished_at_unix"):
            assert second["read_timing"][field] == timing[field]
        assert client.mailbox._header()[4:7] == [2, 2, 2]
    finally:
        client.close()


def test_late_observation_preserves_verified_completion_and_leaves_ack_unconfirmed(native, monkeypatch):
    class Clock:
        jump = 0.0

        def time(self):
            return time.time() + self.jump

        def monotonic(self):
            return time.monotonic() + self.jump

        def sleep(self, seconds):
            time.sleep(seconds)

    clock = Clock()

    class SlowDecodeRegion(rpc_fixture.Region):
        def read(self, offset, length):
            raw = super().read(offset, length)
            if offset == readonly_shm.RESPONSE_OFFSET:
                clock.jump = 2.0
            return raw

    client = readonly_shm.ShmReadOnlyClient(rpc_fixture.NAME, rpc_fixture.ACCOUNT,
                                          region=SlowDecodeRegion(native.state))
    monkeypatch.setattr(readonly_shm, "time", clock)
    try:
        with pytest.raises(readonly_shm.ShmTimeoutError, match="after its deadline") as failure:
            native.execute(lambda: client.send_request(request(), timeout_seconds=1))
        timing = failure.value.read_timing
        assert timing["native_finished_at_unix"] < timing["shm_deadline_at_unix"]
        assert timing["shm_response_validated_at_unix"] > timing["shm_deadline_at_unix"]
        assert "shm_ack_at_unix" not in timing
        assert client.poisoned and client.mailbox._header()[4:7] == [1, 1, 0]
        with pytest.raises(readonly_shm.ShmProtocolError, match="unconfirmed"):
            client.send_request(request("second"))
        assert client.mailbox._header()[4] == 1
    finally:
        client.close()


def test_unobserved_response_has_no_fabricated_native_completion(native):
    client = readonly_shm.ShmReadOnlyClient(rpc_fixture.NAME, rpc_fixture.ACCOUNT,
                                          region=rpc_fixture.Region(native.state))
    try:
        with pytest.raises(readonly_shm.ShmTimeoutError) as failure:
            client.send_request(request(), timeout_seconds=.01)
        assert "shm_published_at_unix" in failure.value.read_timing
        assert "native_finished_at_unix" not in failure.value.read_timing
        assert client.mailbox._header()[4:7] == [1, 0, 0]
    finally:
        client.close()


def test_mutex_timeout_does_not_publish_or_poison_an_unsent_read(native):
    client = readonly_shm.ShmReadOnlyClient(rpc_fixture.NAME, rpc_fixture.ACCOUNT,
                                          region=rpc_fixture.Region(native.state))
    native.state.lock.acquire()
    try:
        with pytest.raises(readonly_shm.ShmTimeoutError, match="request not sent") as failure:
            client.send_request(request(), timeout_seconds=.01)
        assert "shm_published_at_unix" not in failure.value.read_timing
        assert not client.poisoned
    finally:
        native.state.lock.release()
        assert client.mailbox._header()[4:7] == [0, 0, 0]
        client.close()


@pytest.mark.parametrize("metadata", [None, {"native_started_at_unix": -1,
    "native_finished_at_unix": 10 ** 500, "password": "must-not-propagate"}])
def test_old_or_invalid_optional_native_timing_does_not_change_read(native, monkeypatch, metadata):
    publish = native.bridge.mailbox.publish_response

    def original_source(sequence, response):
        response = dict(response)
        response.pop("read_timing", None)
        if metadata is not None:
            response["read_timing"] = metadata
        return publish(sequence, response)

    monkeypatch.setattr(native.bridge.mailbox, "publish_response", original_source)
    client = readonly_shm.ShmReadOnlyClient(rpc_fixture.NAME, rpc_fixture.ACCOUNT,
                                          region=rpc_fixture.Region(native.state))
    try:
        response = native.execute(lambda: client.send_request(request()))
        assert response["ok"] and response["data"] == {}
        assert "native_finished_at_unix" not in response["read_timing"]
        assert "password" not in response["read_timing"]
        assert client.mailbox._header()[4:7] == [1, 1, 1]
    finally:
        client.close()


def test_identity_mismatch_does_not_accept_native_timing(native, monkeypatch):
    original = native.bridge._response

    def wrong_identity(sequence, payload):
        response = original(sequence, payload)
        response["account_id"] = "OTHER"
        return response

    monkeypatch.setattr(native.bridge, "_response", wrong_identity)
    client = readonly_shm.ShmReadOnlyClient(rpc_fixture.NAME, rpc_fixture.ACCOUNT,
                                          region=rpc_fixture.Region(native.state))
    try:
        with pytest.raises(readonly_shm.ShmProtocolError, match="account_id mismatch") as failure:
            native.execute(lambda: client.send_request(request()))
        assert "native_finished_at_unix" not in failure.value.read_timing
        assert client.mailbox._header()[6] == 0
    finally:
        client.close()


def test_other_methods_do_not_receive_position_timing(native):
    client = readonly_shm.ShmReadOnlyClient(rpc_fixture.NAME, rpc_fixture.ACCOUNT,
                                          region=rpc_fixture.Region(native.state))
    try:
        response = native.execute(lambda: client.send_request(request(method="ping")))
        assert "read_timing" not in response
    finally:
        client.close()


def test_proxy_preserves_phase_evidence_on_success_and_cached_replay(proxy):
    bridge, _, _, execution = proxy
    execution.change_response = {"read_timing": {"native_started_at_unix": 10.0,
        "native_finished_at_unix": 11.0, "password": "must-not-propagate"}}
    payload = PROXY_FIXTURE.request()
    first = bridge.process_request(payload)
    second = bridge.process_request(payload)
    assert first["read_timing"]["native_finished_at_unix"] == 11.0
    assert second["read_timing"]["native_finished_at_unix"] == 11.0
    assert "password" not in first["read_timing"]
    assert first["read_timing"]["proxy_received_at_unix"] <= first["read_timing"]["proxy_shm_call_at_unix"]
    assert len([r for r, _ in execution.calls if r["method"] == "get_positions"]) == 1


def test_proxy_timeout_still_stops_and_records_verified_late_read_without_writes(proxy):
    bridge, redis, _, execution = proxy
    failure = readonly_shm.ShmTimeoutError("response arrived after its deadline; result unconfirmed")
    failure.read_timing = {"shm_published_at_unix": 10.0, "native_finished_at_unix": 11.0,
                           "shm_response_validated_at_unix": 12.0, "unknown": "not-a-timestamp"}
    execution.failure = failure
    response = bridge.process_request(PROXY_FIXTURE.request())
    assert response["status"] == "UNKNOWN" and response["automatic_retry"] is False
    assert bridge.stopped and bridge.stop_reason == "SHM_RESULT_UNCONFIRMED"
    assert response["read_timing"]["native_finished_at_unix"] == 11.0
    diagnostic = bridge.diagnostic_snapshot()["first_error"]["read_timing"]
    assert diagnostic["native_finished_at_unix"] == 11.0 and "unknown" not in diagnostic
    assert not redis.hashes
