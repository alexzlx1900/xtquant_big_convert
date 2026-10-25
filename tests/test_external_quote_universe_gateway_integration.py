"""微盘真实分批入口到生成源的本地兼容合同；行情与Redis均使用替身。"""

import asyncio
import os
from pathlib import Path
import sys
import time
from types import SimpleNamespace

import pytest


GATEWAY_ROOT = os.environ.get("AQP_GATEWAY_TEST_ROOT")
if os.environ.get("AQP_QUOTE_CONTRACT") != "1" or not GATEWAY_ROOT:
    pytest.skip("explicit AQP_QUOTE_CONTRACT=1 and AQP_GATEWAY_TEST_ROOT required", allow_module_level=True)
AQP_ROOT = Path(GATEWAY_ROOT).resolve().parent
sys.path[:0] = [str(AQP_ROOT), str(Path(GATEWAY_ROOT).resolve())]

from api.services.microcap_research import live_gateway
from api.services.microcap_research.live_runtime import BENCHMARK, LiveResearch
from api.services.microcap_research.operations import load_operations
from gateway_server import GatewayCommandRouter
from test_external_redis_gateway_readwrite_integration import Harness, gateway_modules
from test_quote_universe_bounds import named_symbols


GATEWAY_ID = "qmt-local-test"


class LocalQuoteDispatcher:
    """仅替换外部路由与命令传输，保留正式consumer、router及Gateway读实现。"""

    def __init__(self, harness):
        self.router = GatewayCommandRouter(harness.multi)
        self.profile_id = harness.profile_id
        self.queries, self.responses = [], []

    async def probe_route(self, gateway_id):
        assert gateway_id == GATEWAY_ID
        return {"reachable": True, "ready": True, "route_status": "READY", "queue_depth": 0}

    async def request_query(self, config, message_type, payload, *, timeout_seconds, deadline):
        assert config.gateway_id == GATEWAY_ID and message_type == "query.quotes"
        assert payload["profile_id"] == self.profile_id
        assert 0 < deadline - time.monotonic() <= timeout_seconds
        self.queries.append(list(payload["symbols"]))
        result = await self.router.dispatch(message_type, {
            **payload, "_request_timeout_seconds": max(0, deadline - time.monotonic()),
        })
        response = {"status": "ACK", "result": result}
        self.responses.append(response)
        return response


@pytest.mark.parametrize("fault", [None, "missing_benchmark", "invalid_benchmark"])
def test_microcap_actual_100_batch_consumer_reads_400_members_and_explicit_benchmark(
    monkeypatch, gateway_modules, fault,
):
    members = named_symbols(400)
    codes = list(dict.fromkeys(members + [BENCHMARK]))
    assert len(codes) == 401

    async def scenario():
        harness = Harness(monkeypatch, gateway_modules, live=True, symbols=codes)
        native_calls = []

        def quote_source(requested):
            native_calls.append(list(requested))
            quotes = {code: dict(lastPrice=11.47, lastClose=11.3, time=int(time.time() * 1000))
                      for code in requested}
            if BENCHMARK in quotes and fault == "missing_benchmark":
                quotes.pop(BENCHMARK)
            if BENCHMARK in quotes and fault == "invalid_benchmark":
                quotes[BENCHMARK]["lastPrice"] = 0
            return quotes

        harness.source.context.get_full_tick = quote_source
        dispatcher = LocalQuoteDispatcher(harness)
        monkeypatch.setattr(live_gateway, "gateway_command_dispatcher", dispatcher)
        monkeypatch.setattr(live_gateway, "get_gateway_connector_config",
                            lambda gateway_id: SimpleNamespace(gateway_id=gateway_id))
        options = load_operations()
        assert options.quote_batch_size == 100 and options.intraday_enabled is False
        options = options.model_copy(update={"gateway_id": GATEWAY_ID, "gateway_profile_id": harness.profile_id})
        consumer = LiveResearch(None, options, quote_provider=live_gateway.routed_quotes)
        try:
            await harness.start()
            quotes, errors, stats = await harness.operation(consumer.quote_batch(codes, with_stats=True))
            expected_batches = [codes[offset:offset + 100] for offset in range(0, len(codes), 100)]
            assert native_calls == dispatcher.queries == expected_batches
            assert [len(batch) for batch in native_calls] == [100, 100, 100, 100, 1]
            assert stats["symbols"] == 401 and stats["batches"] == 5
            assert len(stats["read_evidence"]) == 5
            assert harness.submit_calls == harness.cancel_calls == []
            assert harness.proxy._writes_blocked is True
            if fault is None:
                assert errors == [] and set(quotes) == set(codes)
                assert all(row["last_price"] > 0 for row in quotes.values())
                assert all(item["status"] == "OK" and item["native_success"]
                           and item["missing_symbol_count"] == 0 for item in stats["read_evidence"])
            else:
                assert errors == ["QUOTE_SOURCE_REJECTED"]
                assert set(quotes) == set(members) and BENCHMARK not in quotes
                failure = stats["read_evidence"][-1]
                assert failure["status"] == "UNAVAILABLE" and failure["native_success"] is False
                assert failure["missing_symbols"] == [BENCHMARK]
                result = dispatcher.responses[-1]["result"]
                assert result["status"] == "ERROR" and result["server_error"] == "QMT_QUOTE_UNAVAILABLE"
                detail = "missing_symbols" if fault == "missing_benchmark" else "invalid_symbols"
                assert result["data"][detail] == [BENCHMARK]
        finally:
            await harness.close()

    asyncio.run(scenario())
