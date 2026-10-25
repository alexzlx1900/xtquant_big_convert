"""多证券SHM采集的最小本地整链；不重做VM账户、页面或交易验收。"""

import asyncio
from datetime import datetime, timedelta
import os
from pathlib import Path
import sys
import time
from zoneinfo import ZoneInfo

import pytest


GATEWAY_ROOT = os.environ.get("AQP_GATEWAY_TEST_ROOT")
if os.environ.get("AQP_TICK_CONTRACT") != "1" or not GATEWAY_ROOT:
    pytest.skip("explicit AQP_TICK_CONTRACT=1 and AQP_GATEWAY_TEST_ROOT required", allow_module_level=True)
sys.path.insert(0, str(Path(GATEWAY_ROOT).resolve()))

from gateway_server import GatewayCommandRouter
from tick_subscription_shadow import TickSubscriptionShadowStore
from test_external_redis_gateway_readwrite_integration import Harness, gateway_modules


CODES = ["000001.SZ", "600000.SH"]
NOW = datetime(2026, 10, 22, 9, 29, 45, tzinfo=ZoneInfo("Asia/Shanghai"))
CONTRACT = dict(capture_id="local-win-multi", trade_date="20261022", start_time="093000",
                end_time="093500", symbols=CODES, profile_id="live-stock")


def tick(stamp, price):
    return dict(time=int(stamp.timestamp() * 1000), lastPrice=price, volume=100, amount=price * 100)


async def rows_reached(store, count):
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline:
        current = await asyncio.to_thread(store.capture_status, CONTRACT["capture_id"])
        if current["rows_total"] >= count:
            return
        await asyncio.sleep(0.01)
    raise AssertionError("scoped native events did not reach the real shadow writer")


@pytest.mark.parametrize("codes", [CODES, ["%06d.SZ" % n for n in range(300000, 300082)]], ids=["two", "82"])
def test_scoped_native_subscriptions_register_early_persist_read_tail_and_stop_once(
    monkeypatch, gateway_modules, tmp_path, codes,
):
    store = TickSubscriptionShadowStore(tmp_path / "multi-capture.db")
    contract = {**CONTRACT, "symbols": codes}

    async def scenario():
        harness = Harness(monkeypatch, gateway_modules, live=True, symbols=codes)
        clock = [NOW]
        harness.read = gateway_modules.readonly.BigQmtReadOnlyAdapter(
            "qmt-local-test", harness.profile, client_factory=harness.read._client_factory,
            tick_subscription_shadow_store=store, market_now_factory=lambda: clock[0],
        )
        harness.multi._bigqmt_read_adapters[harness.profile_id] = harness.read
        router = GatewayCommandRouter(harness.multi)
        callbacks, handles, subscribed, cancelled = {}, {}, [], []

        def subscribe(code, **params):
            assert params["period"] == "tick" and params["dividend_type"] == "none"
            handle = 7000 + len(subscribed)
            subscribed.append(code)
            callbacks[code], handles[handle] = params["callback"], code
            # QMT可在订阅调用返回前同步回调；09:29事件仍应被真实窗口过滤。
            callbacks[code]({code: tick(NOW, 10.0)})
            return handle

        def unsubscribe(handle):
            cancelled.append(handle)
            code = handles[handle]
            # 原生退订期间迟到回调必须已关门，不得进入有界尾段。
            callbacks[code]({code: tick(NOW.replace(hour=9, minute=34, second=59, microsecond=500000), 99.0)})
            return True

        harness.source.context.subscribe_quote = subscribe
        harness.source.context.unsubscribe_quote = unsubscribe
        try:
            await harness.start()
            started = await harness.operation(router.dispatch("shadow.tick_subscription.start", contract))
            assert started["status"] == "OK" and started["data"]["capture_created"] is True, started
            assert subscribed == codes
            repeated = await harness.operation(router.dispatch("shadow.tick_subscription.start", contract))
            assert repeated["status"] == "OK" and repeated["data"]["capture_created"] is False
            conflict = await harness.operation(router.dispatch(
                "shadow.tick_subscription.start", {**contract, "end_time": "093600"},
            ))
            assert conflict["status"] == "ERROR" and subscribed == codes

            clock[0] = NOW.replace(hour=9, minute=30, second=5)
            for index, code in enumerate(codes):
                callbacks[code]({code: tick(clock[0], 11.5 + index * 10)})
                callbacks[code]({code: tick(clock[0] + timedelta(seconds=1), 11.6 + index * 10)})
            await harness.operation(rows_reached(store, 2 * len(codes)))
            read = await harness.operation(router.dispatch("shadow.tick_subscription.read", {
                "capture_id": CONTRACT["capture_id"], "profile_id": harness.profile_id,
            }))
            assert read["status"] == "OK" and read["data"]["rows_by_symbol_count"] == dict.fromkeys(codes, 2)

            clock[0] = NOW.replace(hour=9, minute=34, second=59)
            for index, code in enumerate(codes):
                callbacks[code]({code: tick(clock[0], 11.7 + index * 10)})
                callbacks[code]({code: tick(clock[0] + timedelta(seconds=2), 90.0)})
            stopped = await harness.operation(router.dispatch("shadow.tick_subscription.stop", {
                "capture_id": CONTRACT["capture_id"], "profile_id": harness.profile_id,
            }))
            assert stopped["status"] == "OK" and stopped["data"]["status"] == "STOPPED", stopped
            assert stopped["data"]["cleanup_status"] == "CANCELLED"
            repeated_start = await harness.operation(router.dispatch("shadow.tick_subscription.start", contract))
            assert repeated_start["status"] == "OK" and repeated_start["data"]["status"] == "STOPPED"
            assert repeated_start["data"]["capture_created"] is False and subscribed == codes
            repeated_stop = await harness.operation(router.dispatch("shadow.tick_subscription.stop", {
                "capture_id": CONTRACT["capture_id"], "profile_id": harness.profile_id,
            }))
            assert repeated_stop["status"] == "OK" and len(cancelled) == len(codes)
            final = await harness.operation(router.dispatch("shadow.tick_subscription.read", {
                "capture_id": CONTRACT["capture_id"], "profile_id": harness.profile_id,
            }))
            data = final["data"]
            assert data["rows_total"] == 3 * len(codes) and data["rows_by_symbol_count"] == dict.fromkeys(codes, 3)
            assert data["missing_symbols"] == [] and data["dropped_batches"] == 0
            for index, code in enumerate(codes):
                assert [row["lastPrice"] for row in data["rows_by_symbol"][code]] == [
                    11.5 + index * 10, 11.6 + index * 10, 11.7 + index * 10,
                ]
                callbacks[code]({code: tick(clock[0] + timedelta(milliseconds=10), 98.0)})
            assert sorted(cancelled) == sorted(handles) and len(cancelled) == len(codes)
            assert (await asyncio.to_thread(store.capture_status, CONTRACT["capture_id"]))["rows_total"] == 3 * len(codes)
            assert harness.submit_calls == harness.cancel_calls == []
            assert harness.proxy._writes_blocked is True
        finally:
            await harness.close()

    try:
        asyncio.run(scenario())
    finally:
        store.close()
    assert store._writer.is_alive() is False
    reopened = TickSubscriptionShadowStore(tmp_path / "multi-capture.db")
    try:
        saved = reopened.read_capture(CONTRACT["capture_id"], expected_provider="BIGQMT")
        assert saved["capture"]["status"] == "STOPPED" and saved["rows_total"] == 3 * len(codes)
        assert saved["rows_by_symbol_count"] == dict.fromkeys(codes, 3)
    finally:
        reopened.close()
