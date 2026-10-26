"""行情只按请求代码读取，不依赖初始代码集合或当前持仓。"""

import pytest

from test_quote_universe_bounds import QuoteFixture


@pytest.mark.parametrize("live", [True, False])
@pytest.mark.parametrize("method", ["get_ticks", "get_full_tick"])
def test_quotes_outside_initial_symbols_are_independent_of_account_positions(monkeypatch, live, method):
    harness = QuoteFixture(monkeypatch, ["600000.SH"], live=live)
    try:
        harness.start()
        def unavailable_positions(*args):
            raise AssertionError("行情不得查询持仓来决定代码准入")
        harness.namespace["_RUNTIMES"]["read"]["_SERVER"].positions.get_trade_detail_data = unavailable_positions
        codes = ["000001.SZ", "000300.SH", "830799.BJ"]
        result = harness.call("read", method, {"codes": codes})
        assert result["ok"] is True and set(result["data"]) == set(codes)
        assert harness.quote_calls == [codes] and harness.query_calls == []
    finally:
        harness.close()


def test_quotes_remain_readable_after_a_position_disappears(monkeypatch):
    harness = QuoteFixture(monkeypatch, ["600000.SH"])
    try:
        harness.start()
        assert harness.call("read", "get_ticks", {"codes": ["000001.SZ"]})["ok"] is True
        harness.namespace["_RUNTIMES"]["read"]["_SERVER"].positions.get_trade_detail_data = lambda *args: []
        assert harness.call("read", "get_ticks", {"codes": ["000001.SZ"]})["ok"] is True
        assert harness.quote_calls == [["000001.SZ"], ["000001.SZ"]]
        assert harness.query_calls == []
    finally:
        harness.close()


@pytest.mark.parametrize("scoped", [False, True])
def test_subscription_outside_initial_symbols_needs_no_position_or_static_permission(monkeypatch, scoped):
    harness = QuoteFixture(monkeypatch, ["600000.SH"])
    try:
        harness.start()
        params = {"code": "000300.SH", "period": "tick", "result_type": "dict", "dividend_type": "none"}
        if scoped:
            params["capture"] = {"capture_id": "quote-no-static-permission", "trade_date": "20261026",
                                 "start_time": "20261026093000", "end_time": "20261026150000",
                                 "symbols": ["000300.SH", "688096.SH"]}
        result = harness.call("read", "subscribe_quote", params)
        assert result["ok"] is True, result
        assert result["data"]["code"] == "000300.SH" and result["data"]["active"] is True
        assert harness.query_calls == [] and harness.quote_calls == []
    finally:
        harness.close()
