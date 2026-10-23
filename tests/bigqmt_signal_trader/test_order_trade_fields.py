# coding: utf-8
"""issue #133: query_stock_orders / query_stock_trades were missing fields.

@sumo225270 reported three things, all confirmed live against the deployed
bridge before any of this was written:

    query_stock_orders  -> no account_type, no instrument_name, strategy_name ''
    query_stock_trades  -> no account_type,                     strategy_name ''

The first two are absent attributes, so a caller gets AttributeError -- the
same shape as #130. The third is worse: the field exists and is always "".

Cause for each, and they are all different:

  * account_type     the bridge simply never sent it. Positions did, hardcoded
                     to 2 (SECURITY_ACCOUNT), which is wrong on a credit
                     deployment -- the same silence that made #92 expensive.
  * instrument_name  never read off the row. Position rows carry
                     m_strInstrumentName (position_bigqmt reads it), so the
                     order/deal rows are read the same way, with
                     ContextInfo.get_stock_name as the fallback.
  * strategy_name    on trades this echoed the QUERY FILTER and nothing else.
                     The default filter is "" (query everything), so the
                     default answer was "" for every deal regardless of what
                     the order was submitted under. Orders already read
                     m_strStrategyName off the row; deals now do too.

The MiniQMT contract being matched is src/xtquant/xttype.py: XtOrder and
XtTrade both take secu_account and instrument_name, and both set account_type
in __init__. XtTrade additionally has commission. Those are filled here as
well -- one field at a time is how this became three issues.
"""

import os
import sys
import unittest


ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "src"))

from xtquant.xtconstant import CREDIT_ACCOUNT, SECURITY_ACCOUNT

from bigqmt_signal_trader.adapters.order_bigqmt import BigQmtOrderGateway
from bigqmt_signal_trader.xtquant_compat import BigQmtXtTrader, StockAccount


ACCOUNT = "8886800503"


class Row(object):
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


def _order_row(**overrides):
    base = dict(
        m_strOrderSysID="635030450",
        m_strRemark="",
        m_strInstrumentID="600722",
        m_strExchangeID="SH",
        m_strInstrumentName="金隅集团",
        m_nOffsetFlag=49,
        m_nVolumeTotalOriginal=100,
        m_nVolumeTraded=100,
        m_nOrderStatus=56,
        m_dLimitPrice=17.03,
        m_strStrategyName="alpha",
        m_strShareholderID="A123456789",
    )
    base.update(overrides)
    return Row(**base)


def _deal_row(**overrides):
    base = dict(
        m_strTradeID="23381005",
        m_strOrderSysID="635046749",
        m_strInstrumentID="600654",
        m_strExchangeID="SH",
        m_strInstrumentName="中安科",
        m_nOffsetFlag=49,
        m_nVolume=800,
        m_dPrice=3.41,
        m_strTradeTime="100026",
        m_strTradeDate="20260901",
        m_dTradeAmount=2728.0,
        m_strStrategyName="alpha",
        m_strShareholderID="A123456789",
        m_dComssion=1.36,
    )
    base.update(overrides)
    return Row(**base)


class _Context(object):
    """Only get_stock_name -- the instrument-name fallback path."""

    def __init__(self, names=None):
        self.names = names or {}
        self.lookups = []

    def get_stock_name(self, stock):
        self.lookups.append(stock)
        return self.names.get(stock, "")


def _gateway(order_rows=(), deal_rows=(), account_type="STOCK", context=None):
    def query(account_id, acct_type, detail_type, strategy_name=""):
        if detail_type == "ORDER":
            return list(order_rows)
        if detail_type in ("DEAL", "TRADE"):
            return list(deal_rows)
        return []

    return BigQmtOrderGateway(
        context_info=context,
        account_id=ACCOUNT,
        get_trade_detail_data_func=query,
        account_type=account_type,
    )


# ----------------------------------------------------------------- server


class OrderFieldsTest(unittest.TestCase):
    def test_instrument_name_comes_off_the_row(self):
        order = _gateway(order_rows=[_order_row()]).query_orders_strict(ACCOUNT, "")[0]

        self.assertEqual(order.instrument_name, "金隅集团")

    def test_account_type_is_the_configured_one_as_a_number(self):
        order = _gateway(order_rows=[_order_row()]).query_orders_strict(ACCOUNT, "")[0]

        self.assertEqual(order.account_type, SECURITY_ACCOUNT)

    def test_a_credit_deployment_reports_credit(self):
        """#92: a credit account read as STOCK answers all zeros, silently."""
        gateway = _gateway(order_rows=[_order_row()], account_type="CREDIT")

        self.assertEqual(
            gateway.query_orders_strict(ACCOUNT, "")[0].account_type, CREDIT_ACCOUNT)

    def test_an_unknown_type_name_does_not_crash_the_query(self):
        gateway = _gateway(order_rows=[_order_row()], account_type="NONSENSE")

        self.assertEqual(
            gateway.query_orders_strict(ACCOUNT, "")[0].account_type, SECURITY_ACCOUNT)

    def test_shareholder_id_becomes_secu_account(self):
        order = _gateway(order_rows=[_order_row()]).query_orders_strict(ACCOUNT, "")[0]

        self.assertEqual(order.secu_account, "A123456789")

    def test_offset_flag_is_carried_through(self):
        order = _gateway(order_rows=[_order_row()]).query_orders_strict(ACCOUNT, "")[0]

        self.assertEqual(order.offset_flag, 49)

    def test_strategy_name_still_comes_off_the_row(self):
        order = _gateway(order_rows=[_order_row()]).query_orders_strict(ACCOUNT, "")[0]

        self.assertEqual(order.strategy_name, "alpha")


class InstrumentNameFallbackTest(unittest.TestCase):
    """Whether QMT puts a name on order rows is a fact about the terminal."""

    def test_context_info_answers_when_the_row_has_no_name(self):
        context = _Context({"600722.SH": "金隅集团"})
        gateway = _gateway(order_rows=[_order_row(m_strInstrumentName="")],
                           context=context)

        self.assertEqual(
            gateway.query_orders_strict(ACCOUNT, "")[0].instrument_name, "金隅集团")

    def test_the_row_wins_when_it_has_one(self):
        context = _Context({"600722.SH": "should not be asked"})
        gateway = _gateway(order_rows=[_order_row()], context=context)

        gateway.query_orders_strict(ACCOUNT, "")
        self.assertEqual(context.lookups, [])

    def test_repeated_codes_are_looked_up_once(self):
        """A day of orders repeats the same code; get_stock_name is not free."""
        context = _Context({"600722.SH": "金隅集团"})
        rows = [_order_row(m_strInstrumentName="") for _ in range(5)]
        gateway = _gateway(order_rows=rows, context=context)

        gateway.query_orders_strict(ACCOUNT, "")

        self.assertEqual(context.lookups, ["600722.SH"])

    def test_no_context_info_leaves_it_empty_rather_than_failing(self):
        gateway = _gateway(order_rows=[_order_row(m_strInstrumentName="")])

        self.assertEqual(
            gateway.query_orders_strict(ACCOUNT, "")[0].instrument_name, "")

    def test_a_raising_context_info_leaves_it_empty(self):
        class Angry(object):
            def get_stock_name(self, stock):
                raise RuntimeError("no")

        gateway = _gateway(order_rows=[_order_row(m_strInstrumentName="")],
                           context=Angry())

        self.assertEqual(
            gateway.query_orders_strict(ACCOUNT, "")[0].instrument_name, "")


class TradeStrategyNameTest(unittest.TestCase):
    """The reported 恒为空: it echoed the filter, and the filter defaults to ''."""

    def test_it_comes_off_the_row_when_querying_everything(self):
        trade = _gateway(deal_rows=[_deal_row()]).query_trades_strict(ACCOUNT, "")[0]

        self.assertEqual(trade.strategy_name, "alpha")

    def test_the_filter_is_still_the_fallback(self):
        """Filtered queries return only that strategy's rows by construction,
        so echoing the filter is right when the row says nothing."""
        gateway = _gateway(deal_rows=[_deal_row(m_strStrategyName="")])

        self.assertEqual(
            gateway.query_trades_strict(ACCOUNT, "beta")[0].strategy_name, "beta")

    def test_with_neither_it_is_empty(self):
        gateway = _gateway(deal_rows=[_deal_row(m_strStrategyName="")])

        self.assertEqual(gateway.query_trades_strict(ACCOUNT, "")[0].strategy_name, "")


class TradeFieldsTest(unittest.TestCase):
    def test_account_type_is_reported(self):
        trade = _gateway(deal_rows=[_deal_row()]).query_trades_strict(ACCOUNT, "")[0]

        self.assertEqual(trade.account_type, SECURITY_ACCOUNT)

    def test_instrument_name_is_reported(self):
        trade = _gateway(deal_rows=[_deal_row()]).query_trades_strict(ACCOUNT, "")[0]

        self.assertEqual(trade.instrument_name, "中安科")

    def test_commission_reads_qmts_own_spelling(self):
        """QMT spells it m_dComssion. Both are tried rather than guessed at."""
        trade = _gateway(deal_rows=[_deal_row()]).query_trades_strict(ACCOUNT, "")[0]

        self.assertEqual(trade.commission, 1.36)

    def test_commission_also_reads_the_correct_spelling(self):
        gateway = _gateway(deal_rows=[_deal_row(m_dComssion=None,
                                                m_dCommission=2.5)])

        self.assertEqual(gateway.query_trades_strict(ACCOUNT, "")[0].commission, 2.5)

    def test_secu_account_is_reported(self):
        trade = _gateway(deal_rows=[_deal_row()]).query_trades_strict(ACCOUNT, "")[0]

        self.assertEqual(trade.secu_account, "A123456789")




# ----------------------------------------------------------------- client


class FakeClient(object):
    def __init__(self, orders=(), trades=(), account_type="STOCK"):
        self.account_id = ACCOUNT
        self.local_cache_config = {}
        self.full_tick_cache_config = {}
        self.orders = list(orders)
        self.trades = list(trades)
        self._account_type = account_type

    def call(self, method, params=None, account_id=None, timeout_seconds=None):
        if method == "ping":
            return {"pong": True, "account_id": ACCOUNT,
                    "account_type": self._account_type}
        if method == "query_stock_orders":
            return self.orders
        if method == "query_stock_trades":
            return self.trades
        return {}

    def _redis(self):
        raise AssertionError("redis not expected here")


def _trader(orders=(), trades=(), account_type="STOCK", connect=True):
    trader = BigQmtXtTrader(account_id=ACCOUNT)
    trader.client = FakeClient(orders, trades, account_type)
    if connect:
        trader.connect()
    return trader


ORDER_PAYLOAD = {
    "order_sys_id": "635030450", "stock_code": "600722.SH", "action": "SELL",
    "volume": 100, "traded_volume": 100, "status": "56", "price": 17.03,
    "strategy_name": "alpha", "instrument_name": "金隅集团",
    "secu_account": "A123456789", "offset_flag": 49, "account_type": 2,
}

TRADE_PAYLOAD = {
    "trade_id": "23381005", "order_sys_id": "635046749", "stock_code": "600654.SH",
    "action": "SELL", "volume": 800, "price": 3.41, "amount": 2728.0,
    "strategy_name": "alpha", "instrument_name": "中安科",
    "secu_account": "A123456789", "commission": 1.36, "account_type": 2,
}


class ClientOrderTest(unittest.TestCase):
    def test_the_three_reported_fields_are_all_present(self):
        order = _trader(orders=[ORDER_PAYLOAD]).query_stock_orders(
            StockAccount(ACCOUNT))[0]

        self.assertEqual(order.account_type, SECURITY_ACCOUNT)
        self.assertEqual(order.instrument_name, "金隅集团")
        self.assertEqual(order.strategy_name, "alpha")

    def test_the_rest_of_the_xtorder_contract_is_there_too(self):
        order = _trader(orders=[ORDER_PAYLOAD]).query_stock_orders(
            StockAccount(ACCOUNT))[0]

        self.assertEqual(order.secu_account, "A123456789")
        self.assertEqual(order.offset_flag, 49)


class ClientTradeTest(unittest.TestCase):
    def test_account_type_and_strategy_name_arrive(self):
        trade = _trader(trades=[TRADE_PAYLOAD]).query_stock_trades(
            StockAccount(ACCOUNT))[0]

        self.assertEqual(trade.account_type, SECURITY_ACCOUNT)
        self.assertEqual(trade.strategy_name, "alpha")

    def test_commission_and_instrument_name_arrive(self):
        trade = _trader(trades=[TRADE_PAYLOAD]).query_stock_trades(
            StockAccount(ACCOUNT))[0]

        self.assertEqual(trade.commission, 1.36)
        self.assertEqual(trade.instrument_name, "中安科")


class OlderDeploymentTest(unittest.TestCase):
    """A client upgrades before the QMT side is synced and restarted.

    That gap is normal here -- a deploy does nothing until the strategy is
    restarted -- so the payload arrives without any of the new keys. The
    fields must still exist, or the upgrade trades #133's AttributeError for
    the same AttributeError.
    """

    BARE_ORDER = {"order_sys_id": "1", "stock_code": "600000.SH",
                  "action": "BUY", "volume": 100, "status": "50"}
    BARE_TRADE = {"trade_id": "1", "order_sys_id": "1",
                  "stock_code": "600000.SH", "action": "BUY",
                  "volume": 100, "price": 10.0}

    def test_orders_still_carry_every_field(self):
        order = _trader(orders=[self.BARE_ORDER]).query_stock_orders(
            StockAccount(ACCOUNT))[0]

        self.assertEqual(order.instrument_name, "")
        self.assertEqual(order.secu_account, "")
        self.assertIsNone(order.offset_flag)

    def test_trades_still_carry_every_field(self):
        trade = _trader(trades=[self.BARE_TRADE]).query_stock_trades(
            StockAccount(ACCOUNT))[0]

        self.assertEqual(trade.instrument_name, "")
        self.assertEqual(trade.commission, 0.0)

    def test_account_type_falls_back_to_what_the_server_said(self):
        order = _trader(orders=[self.BARE_ORDER],
                        account_type="CREDIT").query_stock_orders(
            StockAccount(ACCOUNT))[0]

        self.assertEqual(order.account_type, CREDIT_ACCOUNT)

    def test_with_nothing_at_all_it_is_security_account(self):
        """Never absent: MiniQMT's own XtOrder always has a value here."""
        trader = _trader(orders=[self.BARE_ORDER], connect=False)
        trader.client._account_type = ""

        order = trader.query_stock_orders(StockAccount(ACCOUNT))[0]

        self.assertEqual(order.account_type, SECURITY_ACCOUNT)


class PositionsAgreeTest(unittest.TestCase):
    """Positions hardcoded 2. Same field, same account -- must not disagree."""

    def test_a_credit_deployment_no_longer_reports_stock(self):
        trader = _trader(account_type="CREDIT")

        position = trader._position_object(ACCOUNT, {"stock_code": "600000.SH",
                                                     "volume": 100})

        self.assertEqual(position.account_type, CREDIT_ACCOUNT)

    def test_a_stock_deployment_is_unchanged(self):
        trader = _trader(account_type="STOCK")

        position = trader._position_object(ACCOUNT, {"stock_code": "600000.SH",
                                                     "volume": 100})

        self.assertEqual(position.account_type, SECURITY_ACCOUNT)


if __name__ == "__main__":
    unittest.main()
