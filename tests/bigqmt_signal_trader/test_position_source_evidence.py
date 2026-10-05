"""原始持仓完整性证据与旧 RPC 返回格式的针对性验证。"""

import datetime as dt
import os
import sys
import unittest
from types import SimpleNamespace

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

from bigqmt_signal_trader.adapters.position_bigqmt import BigQmtPositionProvider
from bigqmt_signal_trader.redis_rpc import BigQmtRpcHandlers, to_jsonable


def row(**changes):
    values = dict(m_strInstrumentID="600000", m_strExchangeID="SH",
                  m_nVolume=100, m_nCanUseVolume=100, m_dOpenPrice=10)
    values.update(changes)
    return SimpleNamespace(**values)


class PositionSourceEvidenceTest(unittest.TestCase):
    def provider(self, rows):
        self.calls = []

        def query(*args):
            self.calls.append(args)
            if isinstance(rows, Exception):
                raise rows
            return rows

        return BigQmtPositionProvider(query)

    def assert_counts(self, evidence, raw, converted, returned, skipped, duplicate):
        self.assertEqual([evidence[key] for key in (
            "raw_row_count", "converted_row_count", "returned_row_count",
            "skipped_row_count", "duplicate_row_count")],
            [raw, converted, returned, skipped, duplicate])
        self.assertEqual(raw, converted + skipped)
        self.assertEqual(converted, returned + duplicate)

    def test_complete_same_native_query_and_serializable_rpc(self):
        provider = self.provider([row(), row(m_strInstrumentID="000001", m_strExchangeID="SZ")])
        handlers = BigQmtRpcHandlers("test-account", None, provider)
        result = handlers.handle("get_positions", {"include_source_evidence": True})
        evidence = result["source_evidence"]
        self.assertEqual(self.calls, [("test-account", "STOCK", "POSITION")])
        self.assertEqual(evidence["status"], "OK")
        self.assertTrue(evidence["complete"])
        self.assertEqual(evidence["contract_version"], 1)
        self.assertEqual(evidence["source"], "native_get_trade_detail_data")
        self.assertEqual(evidence["account_id"], "test-account")
        self.assertEqual(dt.datetime.fromisoformat(evidence["queried_at"]).utcoffset(), dt.timedelta(0))
        self.assert_counts(evidence, 2, 2, 2, 0, 0)
        self.assertEqual(to_jsonable(result)["positions"]["600000.SH"]["volume"], 100)

    def test_real_empty_list_is_complete(self):
        result = self.provider([]).get_positions_with_evidence("test-account")
        self.assertTrue(result["source_evidence"]["complete"])
        self.assertEqual(result["source_evidence"]["status"], "OK")
        self.assert_counts(result["source_evidence"], 0, 0, 0, 0, 0)

    def test_failure_none_non_sequence_never_claim_empty(self):
        for rows in (RuntimeError("sensitive detail"), None, {}, "invalid", iter([])):
            with self.subTest(rows=type(rows).__name__):
                result = self.provider(rows).get_positions_with_evidence("test-account")
                evidence = result["source_evidence"]
                self.assertFalse(evidence["complete"])
                self.assertEqual(evidence["status"], "FAILED")
                self.assertIsNone(evidence["raw_row_count"])
                self.assertIsNone(evidence["returned_row_count"])
                self.assertNotIn("sensitive detail", str(result))
                self.assertEqual(len(self.calls), 1)

    def test_unavailable_query_is_failed(self):
        result = BigQmtPositionProvider(None).get_positions_with_evidence("test-account")
        self.assertEqual(result["source_evidence"]["error_code"], "POSITION_QUERY_FAILED")

    def test_skipped_security_and_duplicate_counts(self):
        rows = [row(), row(m_nVolume=200), row(m_strInstrumentID=""), row(m_strInstrumentID="bad")]
        result = self.provider(rows).get_positions_with_evidence("test-account")
        evidence = result["source_evidence"]
        self.assertFalse(evidence["complete"])
        self.assertEqual(evidence["status"], "PARTIAL")
        self.assert_counts(evidence, 4, 2, 1, 2, 1)
        self.assertEqual(result["positions"]["600000.SH"].volume, 200)

    def test_invalid_quantity_not_truncated_and_invalid_finance_rejected(self):
        cases = [dict(m_nVolume=value) for value in (1.1, -1, float("nan"), float("inf"), True, None)]
        cases += [dict(m_nCanUseVolume=1.1), dict(m_dOpenPrice=float("inf")),
                  dict(m_dMarketValue=float("nan")), dict(m_nFrozenVolume=0.5)]
        for changes in cases:
            with self.subTest(changes=changes):
                result = self.provider([row(**changes)]).get_positions_with_evidence("test-account")
                self.assertFalse(result["source_evidence"]["complete"])
                self.assert_counts(result["source_evidence"], 1, 0, 0, 1, 0)

    def test_negative_cost_and_available_above_volume_are_valid(self):
        result = self.provider([row(m_dOpenPrice=-2.5, m_nCanUseVolume=120)]).get_positions_with_evidence("test-account")
        self.assertTrue(result["source_evidence"]["complete"])
        self.assertEqual(result["positions"]["600000.SH"].cost, -2.5)

    def test_native_eight_digit_options_remain_in_source_coverage(self):
        result = self.provider([row(m_strInstrumentID="10000001")]).get_positions_with_evidence("test-account")
        self.assertTrue(result["source_evidence"]["complete"])
        self.assertIn("10000001.SH", result["positions"])

    def test_legacy_unchanged_and_only_boolean_true_opts_in(self):
        provider = self.provider([row(m_nVolume=1.1)])
        handlers = BigQmtRpcHandlers("test-account", None, provider)
        for params in ({}, {"include_source_evidence": False}, {"include_source_evidence": "true"}):
            result = handlers.handle("get_positions", params)
            self.assertNotIn("source_evidence", result)
            self.assertEqual(result["600000.SH"].volume, 1)
        self.assertFalse(handlers.handle("get_positions", {"include_source_evidence": True})["source_evidence"]["complete"])

    def test_unsupported_never_calls_legacy_and_returns_unknown_counts(self):
        class LegacyProvider:
            def get_positions(self, account_id):
                raise AssertionError("opt-in must not silently fall back")

        result = BigQmtRpcHandlers("test-account", None, LegacyProvider()).handle(
            "query_stock_positions", {"include_source_evidence": True})
        self.assertEqual(result["source_evidence"]["status"], "UNSUPPORTED")
        self.assertFalse(result["source_evidence"]["complete"])
        self.assertIsNone(result["source_evidence"]["raw_row_count"])


if __name__ == "__main__":
    unittest.main()
