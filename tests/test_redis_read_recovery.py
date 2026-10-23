"""只读通信故障恢复；交易和原生未知结果保持阻断，不连接真实服务。"""
import asyncio
import json
import time
from types import SimpleNamespace
from unittest import mock

import pytest
from redis.exceptions import ConnectionError as RedisConnectionError, TimeoutError as RedisTimeoutError

from test_live_redis_shm_proxy import LEDGER, make_proxy, request
from test_external_redis_gateway_readwrite_integration import Harness, gateway_modules


@pytest.mark.parametrize("phase", ["setex", "rpush", "expire", "publish"])
@pytest.mark.parametrize("error", [RedisTimeoutError, RedisConnectionError])
def test_completed_read_partial_fanout_failure_keeps_source_and_next_read(phase, error):
    proxy, provider, _, native = make_proxy(receive_policy="QUEUE_ONLY")
    original = getattr(provider, phase)
    failed = request("poll_execution_events", rid="lost-response", params={"after_sequence": 0})
    native.result = {"events": [], "latest_sequence": 0}
    with mock.patch.object(provider, phase, side_effect=error("read response unavailable")):
        response = proxy.process_request(failed)
    assert response["ok"] and not proxy.stopped
    assert proxy.get_cached_response("lost-response") == response
    assert len(native.calls) == 2
    assert sum(op[0] == "setex" for op in provider.operations) <= 1
    proof = proxy.diagnostic_snapshot()
    assert proof["read_publish_failures"] == 1
    assert proof["first_error"]["error_phase"] == "REDIS_RESPONSE_PUBLISH"
    assert proof["first_error"]["request_id"] == "lost-response"
    assert not provider.hashes.get(LEDGER)
    # 每个独立新请求仅执行一次；没有重新执行或自动发布失败请求。
    assert proxy.process_request(request(rid="new-read"))["ok"]
    assert [r[0]["request_id"] for r in native.calls[1:]] == ["lost-response", "new-read"]
    assert proxy.diagnostic_snapshot()["first_error"] == proof["first_error"]
    assert getattr(provider, phase) == original
    proxy.close()


@pytest.mark.parametrize("error", [RedisTimeoutError, RedisConnectionError])
def test_queue_inspection_outage_does_not_consume_or_exit_and_recovers(error, monkeypatch):
    proxy, provider, _, native = make_proxy(receive_policy="QUEUE_ONLY")
    pending = json.dumps(request(rid="after-outage", read_expires_at_unix=time.time() + 30))
    provider.queues[proxy.transport.request_queue] = [pending]
    clock = [100.0]
    monkeypatch.setattr("bigqmt_signal_trader.redis_shm_proxy.time.monotonic", lambda: clock[0])
    with mock.patch.object(provider, "llen", side_effect=error("offline")) as inspect:
        for _ in range(5):
            assert proxy.poll_once() is None
            assert proxy.poll_once() is None  # backoff prevents a second Redis call
            clock[0] += 1.1
        assert inspect.call_count == 5
    assert not proxy.stopped and len(native.calls) == 1
    assert not any(op[0] in ("lpop", "eval") for op in provider.operations)
    assert proxy.poll_once()["ok"]
    assert len(native.calls) == 2 and not provider.queues[proxy.transport.request_queue]
    assert proxy.diagnostic_snapshot()["consecutive_idle_timeouts"] == 0
    proxy.close()


@pytest.mark.parametrize("method", ["submit_order", "cancel_order"])
def test_write_publication_timeout_still_seals_and_preserves_ledger(method):
    proxy, provider, _, native = make_proxy(receive_policy="QUEUE_ONLY", start_write_blocked=False,
                                          live_write_authorized=True)
    with mock.patch.object(provider, "rpush", side_effect=RedisTimeoutError("partial write response")):
        with pytest.raises(RedisTimeoutError):
            proxy.process_request(request(method, rid="uncertain-write"))
    assert proxy.stopped and len(native.calls) == 2
    assert json.loads(provider.hashes[LEDGER]["uncertain-write"])["state"] == "PUBLISH_UNCONFIRMED"
    assert proxy.diagnostic_snapshot()["read_publish_failures"] == 0
    proxy.close()


def test_native_unknown_then_redis_timeout_cannot_use_read_recovery():
    proxy, provider, _, native = make_proxy(receive_policy="QUEUE_ONLY")
    native.failure = TimeoutError("native result not confirmed")
    with mock.patch.object(provider, "setex", side_effect=RedisTimeoutError("offline")):
        with pytest.raises(RedisTimeoutError):
            proxy.process_request(request())
    assert proxy.stopped and proxy.stop_reason == "SHM_RESULT_UNCONFIRMED"
    assert proxy.diagnostic_snapshot()["first_error"]["error_phase"] == "SHM_FORWARD"
    assert proxy.diagnostic_snapshot()["read_publish_failures"] == 0
    proxy.close()


def test_actual_native_event_and_gateway_recover_without_losing_or_duplicating_event(monkeypatch, gateway_modules):
    h = Harness(monkeypatch, gateway_modules, live=True)
    h.proxy.receive_policy = "QUEUE_ONLY"

    async def scenario():
        try:
            await h.start()
            await h.operation(h.order.connect_execution_events())
            while not h.multi._shared_events.empty():
                h.multi._shared_events.get_nowait()
            h.source.namespace["order_callback"](h.source.context, SimpleNamespace(
                m_strAccountID=h.account, m_strOrderSysID="existing-7620", m_strInstrumentID="000001",
                m_strExchangeID="SZ", m_strRemark="existing", m_strStrategyName="",
                m_nOffsetFlag=48, m_nOpType=23, m_nOrderStatus=50, m_nVolumeTotalOriginal=100,
                m_nVolumeTraded=0, m_dLimitPrice=11.42, m_dTradedPrice=0.0, m_nOrderPriceType=50,
                m_strInsertDate="20261023", m_strInsertTime="100000"))
            original = h.redis.setex
            failed_ids = []
            def fail_one_reply(key, ttl, payload):
                value = json.loads(payload)
                if value["method"] == "poll_execution_events" and not failed_ids:
                    failed_ids.append(value["request_id"])
                    raise RedisTimeoutError("completed event response lost")
                return original(key, ttl, payload)
            monkeypatch.setattr(h.redis, "setex", fail_one_reply)
            await h.operation(h.multi.next_event(timeout=2))
            assert failed_ids and not h.proxy.stopped
            assert h.order.is_execution_polling_started and h.order._execution_event_sequence == 0
            assert h.order.execution_event_subscription_health()["status"] == "ERROR"
            while not h.multi._shared_events.empty():
                h.multi._shared_events.get_nowait()
            event = await h.operation(h.multi.next_event(timeout=2))
            assert event.event_type == "order.report" and event.payload["shm_sequence"] == 1
            assert h.order._execution_event_sequence == 1
            assert h.order.execution_event_subscription_health()["status"] == "MANUAL_POLL"
            assert await h.operation(h.multi.next_event(timeout=.2)) is None
            assert (await h.operation(h.read.query_asset()))["source_evidence"]["complete"]
            assert (await h.operation(h.read.query_positions_with_evidence()))["source_evidence"]["complete"]
            assert h.proxy.diagnostic_snapshot()["read_publish_failures"] == 1
            assert h.proxy.get_cached_response(failed_ids[0]) is None  # 原截止过期，不重新执行。
            assert not h.submit_calls and not h.cancel_calls and not h.redis.hashes.get(LEDGER)
        finally:
            await h.close()
    asyncio.run(scenario())
