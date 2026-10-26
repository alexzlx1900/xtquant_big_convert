"""实盘报价仅将当次完整核验的当前正持仓加入允许范围。"""

import types

import pytest

from test_quote_universe_bounds import QuoteFixture


@pytest.mark.parametrize("method", ["get_ticks", "get_full_tick"])
def test_live_current_holding_outside_static_list_reads_and_is_rechecked_after_sale(monkeypatch, method):
    harness = QuoteFixture(monkeypatch, ["600000.SH"])
    try:
        harness.start()
        assert harness.call("read", method, {"codes": ["000001.SZ"]})["ok"] is True
        assert harness.quote_calls == [["000001.SZ"]]
        assert harness.query_calls == [(harness.account, "STOCK", "POSITION", "")]
        harness.namespace["_RUNTIMES"]["read"]["_SERVER"].positions.get_trade_detail_data = lambda *args: []
        result = harness.call("read", method, {"codes": ["000001.SZ"]})
        assert result["server_error"] == "SYMBOL_DENIED"
        assert result["data"]["denied_symbols"] == ["000001.SZ"]
        assert harness.quote_calls == [["000001.SZ"]]
    finally:
        harness.close()


@pytest.mark.parametrize("volume,error", [(0, "SYMBOL_DENIED"), (-1, "POSITION_CONVERSION_INCOMPLETE"), (None, "POSITION_CONVERSION_INCOMPLETE")])
def test_zero_negative_or_unknown_positions_do_not_authorize_quotes(monkeypatch, volume, error):
    harness = QuoteFixture(monkeypatch, ["600000.SH"])
    try:
        harness.start()
        provider = harness.namespace["_RUNTIMES"]["read"]["_SERVER"].positions
        provider.get_trade_detail_data = lambda *args: [types.SimpleNamespace(
            m_strInstrumentID="000001", m_strExchangeID="SZ", m_nVolume=volume,
            m_nCanUseVolume=0, m_dOpenPrice=10.0, m_dMarketValue=0.0, m_dLastPrice=10.0)]
        assert harness.call("read", "get_ticks", {"codes": ["000001.SZ"]})["server_error"] == error
        assert harness.quote_calls == []
    finally:
        harness.close()


@pytest.mark.parametrize("rows", [None, [object()]])
def test_failed_or_incomplete_position_read_never_authorizes_quotes(monkeypatch, rows):
    harness = QuoteFixture(monkeypatch, ["600000.SH"])
    try:
        harness.start()
        harness.namespace["_RUNTIMES"]["read"]["_SERVER"].positions.get_trade_detail_data = lambda *args: rows
        result = harness.call("read", "get_ticks", {"codes": ["000001.SZ"]})
        assert result["ok"] is False
        assert result["server_error"] in ("POSITION_ROWS_UNAVAILABLE", "POSITION_CONVERSION_INCOMPLETE")
        assert harness.quote_calls == []
    finally:
        harness.close()


def test_simulation_keeps_its_static_scope_even_for_a_current_holding(monkeypatch):
    harness = QuoteFixture(monkeypatch, ["600000.SH"], live=False)
    try:
        harness.start()
        result = harness.call("read", "get_ticks", {"codes": ["000001.SZ"]})
        assert result["server_error"] == "SYMBOL_DENIED"
        assert harness.query_calls == [] and harness.quote_calls == []
    finally:
        harness.close()
