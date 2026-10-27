"""真实生成源码 → 内存 SHM/Redis → 正式 Gateway；不访问运行实例。"""

import asyncio
from unittest import mock

import pytest

from test_external_redis_gateway_readwrite_integration import Harness, gateway_modules


CODES = ["%06d.SZ" % index for index in range(1, 50)] + ["301718.SZ"]


@pytest.mark.parametrize("live", [False, True])
def test_50_stock_minute_batch_crosses_native_proxy_and_formal_gateway(monkeypatch, gateway_modules, live):
    h = Harness(monkeypatch, gateway_modules, live=live, symbols=CODES)
    h.source.context.get_market_data_ex = mock.Mock(side_effect=lambda fields, symbols, **kwargs: {
        code: {"20261015100100": dict(open=11.4, high=11.5, low=11.3, close=11.42, volume=100, amount=1142.0)}
        for code in symbols})
    native_read = h.source.namespace["_RUNTIMES"]["read"]["_SERVER"]
    native_read.history_downloader = mock.Mock(return_value=None)

    async def run():
        try:
            await h.start()
            result = await h.operation(h.multi.query_minute_bars(CODES,
                start_time="20261015000000", end_time="20261015235959", count=301,
                period="1m", fill_data=False, profile_id=h.profile_id))
            assert list(result) == CODES
            assert all(len(rows) == 1 and rows[0]["close"] == 11.42 and rows[0]["volume"] == 100
                       and rows[0]["amount"] == 1142.0 for rows in result.values())
            h.source.context.get_market_data_ex.assert_called_once()
            receipt = await h.operation(h.multi.download_history_data(None, account_id=h.account,
                profile_id=h.profile_id, symbols=CODES, period="1m", start_time="20261015",
                end_time="20261015", confirm_live_data_write=live))
            assert receipt["status"] == "REQUESTED" and receipt["completion_confirmed"] is False
            assert receipt["readback_required"] is True and receipt["automatic_retry"] is False
            assert native_read.history_downloader.call_count == 50
            for code in CODES:
                item = receipt["results"][code]
                assert item["status"] == "REQUESTED" and item["account_id"] == h.account
                assert item["source_evidence"]["account_id"] == h.account
                assert item["windows_session_id"] == 1 and item["environment"] == h.environment
            assert not h.submit_calls and not h.cancel_calls
        finally:
            await h.close()

    asyncio.run(run())
