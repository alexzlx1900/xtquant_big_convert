# coding: utf-8
"""受限文件环境、账户隔离和只读权限的针对性验证。"""

import importlib.util
import sys
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import zmq


ROOT = Path(__file__).resolve().parent.parent
SPEC = importlib.util.spec_from_file_location("readonly_builder", ROOT / "tools/build_readonly_single_file.py")
BUILDER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(BUILDER)
SOURCE = BUILDER.build_source()


class FakeContext:
    def __init__(self):
        self.calls = []

    def get_full_tick(self, codes):
        self.calls.append(codes)
        return {codes[0]: {"lastPrice": 11.57, "time": 1791874800000}}


def make_namespace():
    ns = {"__name__": "readonly_test"}
    exec(compile(SOURCE, "<readonly-single-file>", "exec"), ns)
    ns["BIGQMT_ACCOUNT_ID"] = "SIM_TEST_ACCOUNT"
    return ns


def position_row():
    return SimpleNamespace(
        m_strInstrumentID="000001", m_strExchangeID="SZ", m_strInstrumentName="样本",
        m_nVolume=100, m_nCanUseVolume=100, m_dOpenPrice=10.0,
        m_dMarketValue=1157.0, m_dLastPrice=11.57,
    )


class ReadOnlyBoundaryTest(unittest.TestCase):
    def setUp(self):
        self.ns = make_namespace()
        self.query = mock.Mock(return_value=[position_row()])
        self.context = FakeContext()
        self.bridge = self.ns["ReadOnlyBridge"](self.context, self.query)

    def tearDown(self):
        self.bridge.close()

    def request(self, method, **overrides):
        request = {"request_id": "test-1", "account_id": "SIM_TEST_ACCOUNT", "method": method, "params": {}}
        request.update(overrides)
        return self.bridge.process_request(request)

    def test_write_download_subscription_and_unknown_methods_never_call_qmt(self):
        for method in ("submit_order", "cancel_order", "download_history_data", "subscribe_whole_quote", "unknown"):
            with self.subTest(method=method):
                reply = self.request(method)
                self.assertFalse(reply["ok"])
                self.assertIn("readonly allowlist", reply["error"])
        self.query.assert_not_called()
        self.assertEqual(self.context.calls, [])

    def test_header_and_parameter_account_mismatches_never_call_qmt(self):
        for override in ({"account_id": "OTHER"}, {"params": {"account_id": "OTHER"}}, {"account_id": ""}):
            reply = self.request("get_positions", **override)
            self.assertFalse(reply["ok"])
            self.assertIn("account_id mismatch", reply["error"])
        self.query.assert_not_called()

    def test_expired_read_does_not_call_qmt(self):
        reply = self.request("get_positions", read_expires_at_unix=time.time() - 1)
        self.assertFalse(reply["ok"])
        self.query.assert_not_called()

    def test_existing_position_fields_and_completeness_are_preserved(self):
        reply = self.request("get_positions", params={"include_source_evidence": True})
        self.assertTrue(reply["ok"], reply["error"])
        data = reply["data"]
        self.assertTrue(data["source_evidence"]["complete"])
        self.assertEqual(data["source_evidence"]["raw_row_count"], 1)
        self.assertEqual(data["positions"]["000001.SZ"]["volume"], 100)
        self.assertEqual(data["positions"]["000001.SZ"]["market_value"], 1157.0)
        self.query.assert_called_once_with("SIM_TEST_ACCOUNT", "STOCK", "POSITION")

    def test_failed_native_query_is_failure_with_evidence_not_empty_success(self):
        self.query.side_effect = RuntimeError("native failure")
        reply = self.request("get_positions")
        self.assertFalse(reply["ok"])
        self.assertEqual(reply["data"]["source_evidence"]["status"], "FAILED")
        self.assertIn("POSITION_QUERY_FAILED", reply["error"])

    def test_partial_conversion_is_not_reported_as_complete(self):
        self.query.return_value.append(SimpleNamespace(m_strInstrumentID="bad"))
        reply = self.request("get_positions")
        self.assertFalse(reply["ok"])
        self.assertEqual(reply["data"]["source_evidence"]["skipped_row_count"], 1)

    def test_native_infinite_market_value_uses_existing_finite_alias(self):
        row = position_row()
        row.m_strInstrumentID = "110073"
        row.m_strExchangeID = "SH"
        row.m_nVolume = row.m_nCanUseVolume = 5710
        row.m_dMarketValue = float("inf")
        row.m_dInstrumentValue = 610484.65
        self.query.return_value = [row]
        reply = self.request("get_positions", params={"include_source_evidence": True})
        self.assertTrue(reply["ok"], reply["error"])
        self.assertEqual(reply["data"]["positions"]["110073.SH"]["market_value"], 610484.65)

    def test_all_market_value_aliases_invalid_still_fail_and_zero_is_preserved(self):
        row = position_row()
        row.m_dMarketValue = float("inf")
        row.m_dInstrumentValue = float("nan")
        self.query.return_value = [row]
        reply = self.request("get_positions")
        self.assertFalse(reply["ok"])
        row.m_dMarketValue = 0.0
        good = self.request("get_positions")
        self.assertTrue(good["ok"])
        self.assertEqual(good["data"]["000001.SZ"]["market_value"], 0.0)

    def test_quote_outside_initial_symbol_is_allowed_and_snapshot_time_is_preserved(self):
        for code in ("000001.SZ", "600000.SH", "920000.BJ"):
            with self.subTest(code=code):
                reply = self.request("get_full_tick", params={"codes": [code]})
                self.assertTrue(reply["ok"], reply["error"])
                self.assertEqual(reply["data"][code]["time"], 1791874800000)
                self.assertEqual(self.context.calls[-1], [code])
        for selector in ({"code": "600000.SH"}, {"codes": "600000.SH"}):
            reply = self.request("get_ticks", params=selector)
            self.assertTrue(reply["ok"], reply["error"])
            self.assertEqual(self.context.calls[-1], ["600000.SH"])

    def test_invalid_or_oversized_quote_selectors_never_call_qmt(self):
        selectors = ({}, {"codes": []}, {"codes": ["000001.SZ", "600000.SH"]},
            {"codes": ["600000.SH", "600000.SH"]}, {"codes": ["bad"]},
            {"codes": [None]}, {"codes": 1}, {"codes": {"600000.SH": True}},
            {"codes": ["600000.SH"], "code": "600000.SH"},
            {"codes": ["600000.SH"], "subscribe": True})
        for params in selectors:
            with self.subTest(params=params):
                reply = self.request("get_full_tick", params=params)
                self.assertFalse(reply["ok"])
        self.assertEqual(self.context.calls, [])

    def test_missing_or_unexpected_native_quotes_are_not_success(self):
        for data in (None, {}, {"000001.SZ": {"lastPrice": 11.57}},
                {"600000.SH": {}}, {"600000.SH": None},
                {"600000.SH": {"lastPrice": 11.57}, "000001.SZ": {"lastPrice": 11.57}}):
            with self.subTest(data=data):
                self.context.get_full_tick = mock.Mock(return_value=data)
                reply = self.request("get_full_tick", params={"codes": ["600000.SH"]})
                self.assertFalse(reply["ok"])
                self.assertIn("QMT_QUOTE_UNAVAILABLE", reply["error"])
                self.context.get_full_tick.assert_called_once_with(["600000.SH"])

    def test_endpoint_refuses_external_interfaces(self):
        self.ns["READONLY_ENDPOINT"] = "tcp://0.0.0.0:57941"
        with self.assertRaisesRegex(ValueError, "loopback"):
            self.ns["ReadOnlyBridge"](self.context, self.query)


class EmbeddedLoadingTest(unittest.TestCase):
    def test_qmt_gbk_save_preserves_a_valid_ascii_strategy(self):
        self.assertTrue(SOURCE.isascii())
        compile(SOURCE.encode("gbk"), "<qmt-saved-strategy>", "exec")

    def test_loading_and_queries_need_no_files_and_preserve_shared_modules(self):
        before = {name: module for name, module in sys.modules.items() if name.startswith("bigqmt_signal_trader")}
        # 原生 ZMQ 及标准库已在文件头加载；禁止入口读取任何运行时文件。
        with mock.patch("builtins.open", side_effect=PermissionError("Foribdden FileIO")):
            ns = make_namespace()
            bridge = ns["ReadOnlyBridge"](FakeContext(), lambda *args: [position_row()])
            reply = bridge.process_request({"request_id": "isolated", "account_id": "SIM_TEST_ACCOUNT", "method": "get_positions"})
            self.assertTrue(reply["ok"], reply["error"])
            bridge.close()
        after = {name: module for name, module in sys.modules.items() if name.startswith("bigqmt_signal_trader")}
        self.assertEqual(before, after)

    def test_scheduling_uses_manual_transport_and_releases_on_failure(self):
        ns = make_namespace()
        context = SimpleNamespace(run_time=mock.Mock(side_effect=RuntimeError("no timer")))
        ns["get_trade_detail_data"] = lambda *args: []
        transport = ns["_load_embedded"]("aqp_readonly_transport")
        with mock.patch.object(transport, "ZmqTransport") as factory:
            with self.assertRaisesRegex(RuntimeError, "no timer"):
                ns["init"](context)
            factory.return_value.start_receiving.assert_called_once()
            self.assertFalse(factory.return_value.start_receiving.call_args.kwargs["background_threads"])
            factory.return_value.stop.assert_called_once()


if __name__ == "__main__":
    unittest.main()
