"""选择性字段合并的旧调用、订单类型和身份故障边界。"""

from collections import OrderedDict
import json
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from bigqmt_signal_trader import xtquant_compat
from bigqmt_signal_trader.adapters.account_readonly import _commission
from bigqmt_signal_trader.adapters.order_bigqmt import BigQmtOrderGateway
from bigqmt_signal_trader.exec_events import order_identity_map
from bigqmt_signal_trader.models import OrderSnapshot, TradeSnapshot
from bigqmt_signal_trader.redis_rpc import BigQmtRpcHandlers, to_jsonable


ACCOUNT = "field-compat-account"
ORDER = "field-compat-order"
IDENTITY = {"account_id": ACCOUNT, "user_order_id": ORDER, "strategy_name": "alpha"}


@pytest.mark.parametrize("fields,expected", [
    ({}, None),
    ({"m_dComssion": float("nan"), "m_dCommission": -1}, None),
    ({"m_dComssion": 0, "m_dCommission": 1.36}, 1.36),
    ({"m_dComssion": 1.36, "m_dCommission": 0}, 1.36),
    ({"m_dComssion": 0, "m_dCommission": 0}, 0.0),
])
def test_native_commission_preserves_unknown_and_nonzero_alias(fields, expected):
    assert _commission(SimpleNamespace(**fields)) == expected


@pytest.mark.parametrize("method", ["query_orders_strict", "query_trades_strict"])
@pytest.mark.parametrize("native,query_filter,expected", [
    ("native", "filtered", "native"), ("", "filtered", "filtered"), ("", "", ""),
])
def test_strategy_name_keeps_native_or_explicit_filter_evidence(method, native, query_filter, expected):
    row = SimpleNamespace(m_strInstrumentID="600000", m_strExchangeID="SH",
                          m_strStrategyName=native)
    gateway = BigQmtOrderGateway(None, account_id=ACCOUNT,
                                get_trade_detail_data_func=lambda *args: [row])
    assert getattr(gateway, method)(ACCOUNT, query_filter)[0].strategy_name == expected


class IdentityRedis:
    def __init__(self, rows):
        self.rows = rows

    def mget(self, keys):
        return self.rows


def test_old_positional_order_and_credit_type_survive_new_fields():
    snapshot = OrderSnapshot(
        "12345", ORDER, "600000.SH", "BUY", 100, 40, "50", 11.42,
        "alpha", ORDER, 1700000000, "accepted", 11.4, 50, 70, 40,
        account_type=3, instrument_name="fixture", secu_account="shareholder",
        offset_flag=48, direction=48,
    )
    payload = to_jsonable(snapshot)
    assert payload["op_type"] == 70 and payload["order_type"] == 40
    assert payload["order_time"] == 1700000000 and payload["price_type"] == 50
    trader = xtquant_compat.BigQmtXtTrader.__new__(xtquant_compat.BigQmtXtTrader)
    trader._order_sys_ids = OrderedDict()
    trader._server_account_type = "CREDIT"
    trader._declared_account_type = ""
    order = trader._order_from_dict(ACCOUNT, payload)
    assert order.order_type == 40 and order.account_type == 3
    assert order.instrument_name == "fixture" and order.secu_account == "shareholder"


def test_old_positional_trade_preserves_strategy_amount_and_timestamp():
    trade = TradeSnapshot("trade", "12345", "600000.SH", "SELL", 40, 11.4,
                          "100000", ORDER, 456.0, "alpha", 1700000000)
    payload = to_jsonable(trade)
    assert payload["strategy_name"] == "alpha" and payload["amount"] == 456.0
    assert payload["traded_time"] == 1700000000 and payload["commission"] == 0.0


@pytest.mark.parametrize("payload", [
    [], [IDENTITY], True, "alpha", None,
    {**IDENTITY, "account_id": "another-account"},
    {**IDENTITY, "user_order_id": "another-order"},
    {**IDENTITY, "strategy_name": ["alpha"]},
    {"strategy_name": "alpha"},
])
def test_invalid_or_mismatched_identity_keeps_original_query(payload):
    handlers = BigQmtRpcHandlers(market_data=None, position_provider=None, account_id=ACCOUNT)
    handlers.order_identity_redis_client = IdentityRedis([json.dumps(payload)])
    row = SimpleNamespace(user_order_id=ORDER, strategy_name="", volume=100)
    result = handlers._attribute_to_strategies(ACCOUNT, [row])
    assert result == [row] and result[0] is row
    assert row.strategy_name == "" and row.volume == 100


@pytest.mark.parametrize("rows", [None, {}, "invalid", [], [None, None]])
def test_invalid_batch_response_cannot_shift_order_identity(rows):
    assert order_identity_map(IdentityRedis(rows), ACCOUNT, [ORDER]) == {}


def test_legacy_identity_reader_stops_after_first_network_failure():
    class LegacyRedis:
        def __init__(self):
            self.calls = 0

        def get(self, key):
            self.calls += 1
            raise TimeoutError("fixture transport unavailable")

    client = LegacyRedis()
    assert order_identity_map(client, ACCOUNT, ["a", "b", "c"]) == {}
    assert client.calls == 1


def test_numeric_account_declaration_works_without_account_type_dictionary(monkeypatch):
    monkeypatch.setattr(xtquant_compat, "_xtconstant",
                        SimpleNamespace(SECURITY_ACCOUNT=2, CREDIT_ACCOUNT=3))
    assert xtquant_compat._account_type_name(3) == "CREDIT"
    assert xtquant_compat._account_type_name(2) == "STOCK"
    assert xtquant_compat._account_type_name(99) == ""
    assert xtquant_compat._account_type_code("CREDIT") == 3
    assert xtquant_compat._account_type_code("UNKNOWN") == 0


def test_execution_snapshot_attributes_orders_and_trades_in_one_lookup():
    orders = [SimpleNamespace(user_order_id=ORDER, strategy_name="")]
    trades = [SimpleNamespace(user_order_id=ORDER, strategy_name="")]
    gateway = SimpleNamespace(query_orders=lambda account, strategy: orders,
                              query_trades=lambda account, strategy: trades)

    class CountingRedis(IdentityRedis):
        def __init__(self):
            super().__init__([json.dumps(IDENTITY)])
            self.calls = []

        def mget(self, keys):
            self.calls.append(keys)
            return super().mget(keys)

    handlers = BigQmtRpcHandlers(market_data=None, position_provider=None,
                                account_id=ACCOUNT, order_gateway=gateway)
    handlers.order_identity_redis_client = redis = CountingRedis()
    result = handlers._handle_query_execution_snapshot({"order_strategy_name": ""})
    assert result["orders"] is orders and result["trades"] is trades
    assert orders[0].strategy_name == trades[0].strategy_name == "alpha"
    assert len(redis.calls) == 1 and len(redis.calls[0]) == 1
