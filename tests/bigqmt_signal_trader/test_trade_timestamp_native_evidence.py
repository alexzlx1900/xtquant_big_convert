"""原生 DEAL 五位时钟回归；样本日期显式给定，不由查询日推断成交日。"""

import datetime as dt
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

from bigqmt_signal_trader.adapters.account_readonly import ReadOnlyAccountProvider, _timestamp  # noqa: E402


ACCOUNT = "TIME-FIXTURE-ACCOUNT"
SHANGHAI = dt.timezone(dt.timedelta(hours=8))


def trade_row(traded_at="93816", trade_date="20000103", **changes):
    fields = dict(m_strAccountID=ACCOUNT, m_strTradeID="time-fixture-trade", m_strOrderSysID="time-fixture-order",
        m_strInstrumentID="000001", m_strExchangeID="SZ", m_nOffsetFlag=48, m_nVolume=100,
        m_dPrice=11.86, m_dTradeAmount=1186.0, m_strTradeDate=trade_date, m_strTradeTime=traded_at,
        m_strRemark="", m_strStrategyName="")
    fields.update(changes)
    return SimpleNamespace(**fields)


def read_trade(row):
    calls = []

    def query(*args):
        calls.append(args)
        return [row]

    result = ReadOnlyAccountProvider(query).query_records(ACCOUNT, "DEAL", "")
    assert calls == [(ACCOUNT, "STOCK", "DEAL", "")]
    return result


@pytest.mark.parametrize("clock,hour,minute,second", [
    ("93816", 9, 38, 16), ("94655", 9, 46, 55), ("93507", 9, 35, 7),
    ("093816", 9, 38, 16), ("150201", 15, 2, 1), ("09:38:16", 9, 38, 16),
])
def test_native_clock_is_normalized_without_changing_original_or_units(clock, hour, minute, second):
    result = read_trade(trade_row(traded_at=clock))
    evidence = result["source_evidence"]
    assert evidence["complete"] is True and evidence["status"] == "OK"
    assert [evidence[k] for k in ("raw_row_count", "converted_row_count", "returned_row_count",
        "skipped_row_count", "duplicate_row_count")] == [1, 1, 1, 0, 0]
    assert evidence["unknown_fields"] == [] and evidence["row_errors"] == []
    trade = result["trades"][0]
    assert trade.traded_at == clock
    assert trade.traded_time == int(dt.datetime(2000, 1, 3, hour, minute, second, tzinfo=SHANGHAI).timestamp())
    assert (trade.price, trade.volume, trade.amount) == (11.86, 100, 1186.0)


@pytest.mark.parametrize("clock", ["", "0", "9381", "0938160", "9:38:16", "93816.1", "９３８１６", "99999", "96000", "90060"])
def test_missing_invalid_or_unobserved_clock_format_stays_unknown(clock):
    result = read_trade(trade_row(traded_at=clock))
    assert result["source_evidence"]["complete"] is False
    assert result["source_evidence"]["error_code"] == "DEAL_CONVERSION_INCOMPLETE"
    assert result["source_evidence"]["unknown_fields"] == [{"row_index": 0, "fields": ["traded_time"]}]
    assert result["trades"][0].traded_time == 0


@pytest.mark.parametrize("trade_date", [None, "", 20261021, "2000013", "20000230", "２００００１０３"])
def test_five_digit_clock_does_not_supply_or_repair_native_date(trade_date):
    result = read_trade(trade_row(trade_date=trade_date))
    assert result["source_evidence"]["complete"] is False
    assert result["source_evidence"]["unknown_fields"] == [{"row_index": 0, "fields": ["traded_time"]}]


def test_non_string_clock_is_rejected_instead_of_coerced():
    result = read_trade(trade_row(traded_at=93816))
    evidence = result["source_evidence"]
    assert evidence["complete"] is False and evidence["skipped_row_count"] == 1
    assert evidence["row_errors"][0]["error_code"] == "TEXT_INVALID"
    assert result["trades"] == []


def test_clock_fix_does_not_hide_unrelated_missing_amount():
    result = read_trade(trade_row(m_dTradeAmount=None))
    assert result["trades"][0].traded_time > 0
    assert result["source_evidence"]["complete"] is False
    assert result["source_evidence"]["unknown_fields"] == [{"row_index": 0, "fields": ["amount"]}]


def test_shared_order_timestamp_parser_keeps_existing_strict_format():
    assert _timestamp("20000103", "93816") == 0
    assert _timestamp("20000103", "093816") == int(dt.datetime(2000, 1, 3, 9, 38, 16, tzinfo=SHANGHAI).timestamp())
