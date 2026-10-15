"""原生批量缺价错误证据，不将部分有效行改成完整成功。"""

from unittest import mock

from test_shm_readonly import BridgeFixture


class QuoteFailureEvidenceTest(BridgeFixture):
    quote_symbols = ("000001.SZ", "113037.SH", "123059.SZ")

    def test_missing_and_zero_bond_prices_have_distinct_native_evidence(self):
        self.context.get_full_tick = mock.Mock(return_value={
            "000001.SZ": {"lastPrice": 11.92, "time": 1792047600000},
            "113037.SH": {"lastPrice": 0, "time": 0},
        })
        response = self.process("get_ticks", params={"codes": list(self.quote_symbols)})
        assert response["ok"] is False and response["server_error"] == "QMT_QUOTE_UNAVAILABLE"
        data = response["data"]
        assert data["missing_symbols"] == ["123059.SZ"]
        assert data["invalid_symbols"] == ["113037.SH"]
        assert data["symbol_errors"]["123059.SZ"]["reasons"] == ["MISSING_NATIVE_QUOTE"]
        assert data["symbol_errors"]["113037.SH"] == {
            "reasons": ["LAST_PRICE_NONPOSITIVE", "NATIVE_TIME_NONPOSITIVE"],
            "native_last_price": 0, "native_time": 0,
        }
        source = data["source_evidence"]
        assert source["complete"] is False and source["status"] == "PARTIAL"
        assert source["requested_symbol_count"] == 3 and source["valid_symbol_count"] == 1
        assert source["returned_symbol_count"] == 2 and source["missing_symbol_count"] == 1
        assert source["invalid_symbol_count"] == 1
        assert source["session_id"] == self.mailbox.session_id and source["native_pid"] == self.mailbox.server_pid
        self.context.get_full_tick.assert_called_once_with(list(self.quote_symbols))

    def test_foreign_native_quotes_are_not_exposed_as_requested_quotes(self):
        self.context.get_full_tick = mock.Mock(return_value={
            "000001.SZ": {"lastPrice": 11.92, "time": 1792047600000},
            "foreign-secret": {"lastPrice": 99, "time": 1792047600000},
        })
        response = self.process("get_ticks", params={"codes": ["000001.SZ"]})
        assert response["ok"] is False
        assert set(response["data"]["quotes"]) == {"000001.SZ"}
        assert response["data"]["unexpected_symbols"] == ["foreign-secret"]
        assert response["data"]["source_evidence"]["unexpected_symbol_count"] == 1
