"""VM 故障恢复贯通生成原生源码、Redis、正式 Gateway；全部输入为本地替身。"""

import asyncio
import time

import pytest

from test_external_redis_gateway_readwrite_integration import CODE, DUAL, Harness, ShmReadOnlyClient, gateway_modules
from test_live_redis_shm_proxy import isolated_redis


TARGET = {"gateway_id": "qmt-vm-001", "profile_id": "live-stock"}


def harness(monkeypatch, gateway, provider, **kwargs):
    h = Harness(monkeypatch, gateway, live=True, receive_policy="QUEUE_ONLY", start_write_blocked=False,
                live_write_authorized=True, gateway_identity=TARGET, redis_factory=lambda: provider, **kwargs)
    h.order.gateway_id = h.read.gateway_id = h.multi.gateway_id = TARGET["gateway_id"]
    for source, route in (("read", "readonly"), ("execution", "execution")):
        h.proxy._clients[route] = ShmReadOnlyClient(h.source.clients[source].name, h.account,
                                                   region=DUAL.Region(h.source.states[source]))
    h.proxy._check_native_process_identity = lambda: None
    return h


def order(h, cid):
    return dict(profile_id=h.profile_id, account_id=h.account, symbol=CODE, side="BUY",
                quantity=100, price_type=11, price=11.42, client_order_id=cid)


@pytest.mark.parametrize("mode", ["SYNC", "ASYNC"])
def test_unknown_original_does_not_disable_independent_native_submission(monkeypatch, gateway_modules, isolated_redis, mode):
    h = harness(monkeypatch, gateway_modules, isolated_redis, unknown_submit=True, order_submit_mode=mode)

    async def run():
        try:
            await h.start()
            original = await h.operation(h.order.submit_order(order(h, "unknown-original"), profile_id=h.profile_id))
            assert original.status == "UNKNOWN" and len(h.submit_calls) == 1
            ledger = "aqp:redis_shm:write_ledger:%s:v1" % h.account
            saved = isolated_redis.hgetall(ledger)
            assert len(saved) == 1
            h.unknown_submit = False
            independent = await h.operation(h.order.submit_order(order(h, "new-independent"), profile_id=h.profile_id))
            assert independent.status == ("ACCEPTED" if mode == "SYNC" else "PENDING_ASYNC")
            assert len(h.submit_calls) == 2 and not h.proxy.identity_snapshot()["writes_blocked"]
            duplicate = None
            try:
                duplicate = await h.operation(h.order.submit_order(order(h, "unknown-original"), profile_id=h.profile_id))
            except Exception as exc:
                assert any(word in str(exc).lower() for word in ("business", "client_order_id", "unconfirmed"))
            if duplicate is not None:
                assert duplicate.status in ("UNKNOWN", "REJECTED", "BLOCKED")
            assert len(h.submit_calls) == 2
            for rid, raw in saved.items():
                assert isolated_redis.hget(ledger, rid) == raw
            assert len(h.orders) == 1 and not h.cancel_calls
            cancelled = await h.operation(h.order.cancel_order("7620", profile_id=h.profile_id,
                account_id=h.account, order_sysid="7620", user_order_id="new-independent"))
            assert cancelled["status"] == "CANCELLED" and len(h.cancel_calls) == 1
            for rid, raw in saved.items():
                assert isolated_redis.hget(ledger, rid) == raw
        finally:
            await h.close()

    asyncio.run(run())


def test_late_native_write_releases_only_its_frame_without_resending_unknown_business(monkeypatch, gateway_modules, isolated_redis):
    h = harness(monkeypatch, gateway_modules, isolated_redis)
    native_gateway = h.source.namespace["_RUNTIMES"]["execution"]["_SERVER"].bridge.handlers.order_gateway
    submit = native_gateway.passorder
    def slow_submit(*args):
        result = submit(*args)
        time.sleep(.12)
        return result
    monkeypatch.setattr(native_gateway, "passorder", slow_submit)

    async def run():
        try:
            await h.start()
            h.proxy.timeout_seconds = .05
            original = await h.operation(h.order.submit_order(order(h, "late-original"), profile_id=h.profile_id))
            assert original.status == "UNKNOWN" and len(h.submit_calls) == 1
            ledger = "aqp:redis_shm:write_ledger:%s:v1" % h.account
            saved = isolated_redis.hgetall(ledger)
            assert len(saved) == 1
            h.proxy.timeout_seconds = 1
            monkeypatch.setattr(native_gateway, "passorder", submit)
            result = await h.operation(h.order.submit_order(order(h, "new-after-late-write"), profile_id=h.profile_id))
            assert result.status == "ACCEPTED" and len(h.submit_calls) == 2
            for rid, raw in saved.items():
                assert isolated_redis.hget(ledger, rid) == raw
            assert not h.proxy._pending_request_recoveries
        finally:
            await h.close()
    asyncio.run(run())


def test_native_publication_exception_after_submission_preserves_unknown_and_continues(monkeypatch, gateway_modules, isolated_redis):
    h = harness(monkeypatch, gateway_modules, isolated_redis)
    runtime = h.source.namespace["_RUNTIMES"]["execution"]["_SERVER"]
    finish, failures = runtime.bridge._finish, []
    def interrupted_finish(sequence, request, response, fingerprint):
        if request["method"] == "submit_order" and not failures:
            failures.append(request["request_id"])
            raise RuntimeError("fixture publication interrupted after native submission")
        return finish(sequence, request, response, fingerprint)
    monkeypatch.setattr(runtime.bridge, "_finish", interrupted_finish)
    async def run():
        try:
            await h.start()
            first = await h.operation(h.order.submit_order(order(h, "publication-unknown"), profile_id=h.profile_id))
            assert first.status == "UNKNOWN" and len(h.submit_calls) == 1
            assert runtime.closed is False and failures
            second = await h.operation(h.order.submit_order(order(h, "new-after-publication-error"), profile_id=h.profile_id))
            assert second.status == "ACCEPTED" and len(h.submit_calls) == 2
            assert not h.proxy.isolation_reason
        finally:
            await h.close()
    asyncio.run(run())


def test_inflight_quote_does_not_veto_execution_and_completed_frame_is_released(monkeypatch, gateway_modules, isolated_redis):
    h = harness(monkeypatch, gateway_modules, isolated_redis)

    async def run():
        try:
            await h.start()
            drain_read = h.source.namespace["aqp_shm_readonly_adjust"]
            monkeypatch.setitem(h.source.namespace, "aqp_shm_readonly_adjust", lambda context: None)
            with pytest.raises(Exception):
                await h.operation(h.read.query_quotes([CODE]))
            deadline = time.monotonic() + 2
            while not h.proxy._pending_request_recoveries and time.monotonic() < deadline:
                await asyncio.sleep(.005)
            assert "readonly" in h.proxy._pending_request_recoveries
            pending = h.proxy._clients["readonly"].mailbox._header()[4:7]
            placed = await h.operation(h.order.submit_order(order(h, "during-read-fault"), profile_id=h.profile_id))
            assert placed.status == "ACCEPTED" and len(h.submit_calls) == 1
            assert h.proxy._clients["readonly"].mailbox._header()[4:7] == pending
            monkeypatch.setitem(h.source.namespace, "aqp_shm_readonly_adjust", drain_read)
            deadline = time.monotonic() + 2
            while h.proxy._pending_request_recoveries and time.monotonic() < deadline:
                h.drain()
                await asyncio.sleep(.005)
            assert not h.proxy._pending_request_recoveries
            quotes = await h.operation(h.read.query_quotes([CODE]))
            assert quotes[CODE]["last_price"] == 11.47
            assert len(h.submit_calls) == 1 and not h.cancel_calls
        finally:
            await h.close()

    asyncio.run(run())
