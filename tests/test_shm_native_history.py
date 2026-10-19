"""已有原生真样本回放与有界历史 RPC；不连接 Windows 或下载历史。"""

import importlib.util
import sys
import time
import types
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from bigqmt_signal_trader.transports.readonly_shm import ShmProtocolError, ShmReadOnlyClient  # noqa: E402
from bigqmt_signal_trader.transports.readonly_shm_client import ShmReadOnlyRpcClient  # noqa: E402


SPEC = importlib.util.spec_from_file_location("native_history_fixture", ROOT / "tests/test_shm_readonly.py")
FIXTURE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(FIXTURE)
FIELDS = ["open", "high", "low", "close", "volume", "amount"]
# 原始 AQP_P1_CACHE_SAMPLE 日志的两行数值与原生索引，保持成交量/金额原单位。
SAMPLES = {
    "1m": ("20260904130100", [11.9, 11.92, 11.89, 11.92, 7431.0, 8843066.0]),
    "1d": ("20260904", [11.86, 12.0, 11.85, 11.92, 507504.0, 604797827.0]),
}


def params(period="1m", **overrides):
    value = {"code": "000001.SZ", "period": period, "start_time": "20260904000000",
             "end_time": "20260904235959", "count": 1, "dividend_type": "none",
             "fill_data": False, "subscribe": False}
    value.update(overrides)
    return value


def frame(period="1m"):
    tag, row = SAMPLES[period]
    return SimpleNamespace(index=[tag], columns=list(FIELDS), shape=(1, 6),
                           values=SimpleNamespace(tolist=lambda: [list(row)]))


class NativeHistoryTests(FIXTURE.BridgeFixture):
    def setUp(self):
        super().setUp()
        self.bridge.market_enabled = True
        self.context.get_market_data_ex = mock.Mock(return_value={"000001.SZ": frame()})

    def test_saved_nonempty_minute_and_daily_have_original_units(self):
        for period in ("1m", "1d"):
            self.context.get_market_data_ex.return_value = {"000001.SZ": frame(period)}
            response = self.process("get_market_data_ex", params=params(period))
            self.assertTrue(response["ok"], response)
            result = response["data"]
            self.assertEqual(result["status"], "OK")
            self.assertEqual(result["index"], [SAMPLES[period][0]])
            self.assertEqual(result["rows"], [SAMPLES[period][1]])
            self.assertEqual(result["source_evidence"]["row_count"], 1)
            self.context.get_market_data_ex.assert_called_with(FIELDS, ["000001.SZ"],
                period=period, start_time="20260904000000", end_time="20260904235959",
                count=1, dividend_type="none", fill_data=False, subscribe=False)

    def test_missing_and_empty_native_source_remain_distinct(self):
        self.context.get_market_data_ex.return_value = None
        self.assertEqual(self.process("get_market_data_ex", params=params())["data"]["status"], "UNAVAILABLE")
        self.context.get_market_data_ex.return_value = {}
        self.assertEqual(self.process("get_market_data_ex", params=params())["data"]["status"], "EMPTY")

    def test_source_symbol_shape_numeric_and_window_conflicts_are_rejected(self):
        invalid = [{"600000.SH": frame()}, {"000001.SZ": frame("1d")},
                   {"000001.SZ": SimpleNamespace(index=["20260904130100"], columns=FIELDS,
                    shape=(1, 6), values=SimpleNamespace(tolist=lambda: [[11.9, 12, 11.8, None, 2, 3]]))}]
        for raw in invalid:
            self.context.get_market_data_ex.return_value = raw
            result = self.process("get_market_data_ex", params=params())
            self.assertFalse(result["ok"])
            self.assertEqual(result["server_error"], "HISTORY_SHAPE_INVALID")

    def test_unsafe_or_unbounded_request_never_calls_native(self):
        for overrides in ({"subscribe": True}, {"fill_data": "true"}, {"count": 10001},
                          {"code": "bad"}, {"end_time": "20260903235959"},
                          {"dividend_type": "front"}, {"download": True}, {"period": "follow"},
                          {"start_time": "20260904000060"}):
            self.assertFalse(self.process("get_market_data_ex", params=params(**overrides))["ok"])
        self.context.get_market_data_ex.assert_not_called()

    def test_wire_requires_fixed_identity_and_advertised_capability(self):
        def factory(name, account):
            native = ShmReadOnlyClient(name, account, region=FIXTURE.MemoryRegion(self.state))
            self.clients.append(native)
            return native

        client = ShmReadOnlyRpcClient(name=FIXTURE.NAME, account_id=FIXTURE.ACCOUNT, client_factory=factory)
        with pytest.raises(ShmProtocolError, match="identity handshake"):
            client.call("get_market_data_ex", params())
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(client.verify_identity, expected_session_id=self.mailbox.session_id,
                                     expected_native_pid=self.mailbox.server_pid)
            deadline = time.monotonic() + 2
            while not future.done() and time.monotonic() < deadline:
                self.bridge.drain()
                time.sleep(0.001)
            future.result(timeout=1)
            future = executor.submit(client.call, "get_market_data_ex", params())
            deadline = time.monotonic() + 2
            while not future.done() and time.monotonic() < deadline:
                self.bridge.drain()
                time.sleep(0.001)
            self.assertEqual(future.result(timeout=1)["index"], [SAMPLES["1m"][0]])
        client.close()

    def test_latest_count_and_native_fill_cover_chart_and_strategy_contracts(self):
        value = params(period="5m", code="000002.SZ", start_time="", end_time="", count=500, fill_data=True)
        self.context.get_market_data_ex.return_value = {"000002.SZ": frame()}
        result = self.process("get_market_data_ex", params=value)
        self.assertTrue(result["ok"], result)
        self.assertEqual(result["data"]["source_evidence"]["symbol_scope"], "EXPLICIT_HISTORY_SYMBOL")
        self.assertIs(result["data"]["source_evidence"]["fill_data"], True)
        self.context.get_market_data_ex.assert_called_once_with(FIELDS, ["000002.SZ"],
            period="5m", start_time="", end_time="", count=500,
            dividend_type="none", fill_data=True, subscribe=False)
        # 历史代码范围不改变原快照白名单。
        self.assertFalse(self.process("get_ticks", params={"codes": ["000002.SZ"]})["ok"])

    def test_cross_day_daily_preserves_all_native_rows(self):
        data = frame("1d")
        data.index = ["20260903", "20260904"]
        data.shape = (2, 6)
        data.values = SimpleNamespace(tolist=lambda: [SAMPLES["1d"][1], SAMPLES["1d"][1]])
        self.context.get_market_data_ex.return_value = {"000001.SZ": data}
        result = self.process("get_market_data_ex", params=params("1d", start_time="20260903000000", count=500))
        self.assertTrue(result["ok"], result)
        self.assertEqual(result["data"]["index"], ["20260903", "20260904"])

    def test_tick_fields_duplicate_millisecond_and_units_are_preserved(self):
        fields = ["time", "lastPrice", "volume", "amount", "bidPrice"]
        stamp = 1788498060123
        data = SimpleNamespace(index=["20260904130100123", "20260904130100123"], columns=fields,
            shape=(2, 5), values=SimpleNamespace(tolist=lambda: [[stamp, 11.92, 7431, 8843066, [11.91]]] * 2))
        self.context.get_market_data_ex.return_value = {"000001.SZ": data}
        result = self.process("get_market_data_ex", params=params("tick", count=10000))
        self.assertTrue(result["ok"], result)
        self.assertEqual(result["data"]["rows"][0], [stamp, 11.92, 7431, 8843066, [11.91]])
        self.assertEqual(result["data"]["source_evidence"]["row_count"], 2)
        self.assertEqual(self.context.get_market_data_ex.call_args.args[0], [])

    def test_oversize_history_fails_without_truncation(self):
        data = SimpleNamespace(index=["20260904130100000"] * 10000,
            columns=["lastPrice", "volume", "amount", "native_extra"], shape=(10000, 4),
            values=SimpleNamespace(tolist=lambda: [[11.92, 7431, 8843066, "x" * 1024]] * 10000))
        self.context.get_market_data_ex.return_value = {"000001.SZ": data}
        result = self.process("get_market_data_ex", params=params("tick", count=10000))
        self.assertFalse(result["ok"])
        self.assertEqual(result["server_error"], "HISTORY_PAYLOAD_TOO_LARGE")
        self.assertIsNone(result["data"])

    def test_download_capability_requires_native_injection_and_void_is_only_requested(self):
        request = dict(code="000002.SZ", period="tick", start_time="20260904", end_time="20260904")
        self.assertNotIn("download_history_data", self.process("ping")["data"]["capabilities"])
        self.assertEqual(self.process("download_history_data", params=request)["server_error"], "CAPABILITY_MISSING")
        self.bridge.history_downloader = mock.Mock(return_value=None)
        self.assertIn("download_history_data", self.process("ping")["data"]["capabilities"])
        result = self.process("download_history_data", params=request)
        self.assertTrue(result["ok"], result)
        self.assertEqual(result["data"]["status"], "REQUESTED")
        self.assertIs(result["data"]["completion_confirmed"], False)
        self.bridge.history_downloader.assert_called_once_with("000002.SZ", "tick", "20260904", "20260904")

    def test_old_client_rejects_new_large_reply_before_reading_oversize_payload(self):
        legacy = types.ModuleType("legacy_small_response_transport")
        source = (ROOT / "src/bigqmt_signal_trader/transports/readonly_shm.py").read_text()
        exec(compile(source.replace("RESPONSE_CAPACITY = 8 * 1024 * 1024", "RESPONSE_CAPACITY = 262144"),
                     "<legacy-response-capacity-fixture>", "exec"), legacy.__dict__)
        client = legacy.ShmReadOnlyClient(FIXTURE.NAME, FIXTURE.ACCOUNT, region=FIXTURE.MemoryRegion(self.state))
        self.clients.append(client)
        self.context.get_market_data_ex.return_value = {"000001.SZ": SimpleNamespace(
            index=["20260904130100000"] * 10000, columns=["lastPrice", "volume", "amount"],
            shape=(10000, 3), values=SimpleNamespace(tolist=lambda: [[11.92, 7431, 8843066]] * 10000))}
        with pytest.raises(legacy.ShmProtocolError, match="invalid response payload length"):
            self.roundtrip(client, "get_market_data_ex", params=params("tick", count=10000))

    def test_pretrade_zero_price_keeps_native_quote_and_reports_unavailable_count(self):
        fields = ["time", "lastPrice", "volume", "amount", "askPrice"]
        native_rows = [[1792372500000, 0, 0, 0, [11.42, 0, 0, 0, 0]],
                       [1792373400000, 11.48, 5404, 6202490, [11.48, 11.49, 11.5, 11.51, 11.52]]]
        self.context.get_market_data_ex.return_value = {"000001.SZ": SimpleNamespace(
            index=["20261019091500000", "20261019093000000"], columns=fields,
            shape=(2, len(fields)), values=SimpleNamespace(tolist=lambda: native_rows))}
        result = self.process("get_market_data_ex", params=params("tick", count=2,
            start_time="20261019000000", end_time="20261019235959"))
        self.assertTrue(result["ok"], result)
        self.assertEqual(result["data"]["rows"], native_rows)
        self.assertEqual(result["data"]["source_evidence"]["row_count"], 2)
        self.assertEqual(result["data"]["source_evidence"]["unavailable_last_price_count"], 1)

    def test_tick_missing_trade_price_does_not_accept_other_bad_numeric_fields(self):
        for price, volume, amount in [(0, 1, 0), (0, 0, 1), (-1, 0, 0),
                                      (None, 0, 0), (float("nan"), 0, 0),
                                      (False, 0, 0), (0, False, 0), (0, 0, -1)]:
            with self.subTest(price=price, volume=volume, amount=amount):
                self.context.get_market_data_ex.return_value = {"000001.SZ": SimpleNamespace(
                    index=["20260904091500000"], columns=["lastPrice", "volume", "amount"],
                    shape=(1, 3), values=SimpleNamespace(tolist=lambda: [[price, volume, amount]]))}
                result = self.process("get_market_data_ex", params=params("tick"))
                self.assertFalse(result["ok"])
                self.assertEqual(result["server_error"], "HISTORY_SHAPE_INVALID")

    def test_bar_zero_price_is_still_rejected(self):
        data = frame()
        data.values = SimpleNamespace(tolist=lambda: [[11.9, 12, 11.8, 0, 0, 0]])
        self.context.get_market_data_ex.return_value = {"000001.SZ": data}
        self.assertEqual(self.process("get_market_data_ex", params=params())["server_error"], "HISTORY_SHAPE_INVALID")


@pytest.mark.parametrize("overrides", [
    {"subscribe": True}, {"fill_data": "true"}, {"count": 10001}, {"count": True},
    {"end_time": "20260903235959"}, {"dividend_type": "front"}, {"download": True},
])
def test_unsafe_client_params_do_not_open_mailbox(overrides):
    factory = mock.Mock()
    client = ShmReadOnlyRpcClient(name=FIXTURE.NAME, account_id=FIXTURE.ACCOUNT, client_factory=factory)
    with pytest.raises(ShmProtocolError):
        client.call("get_market_data_ex", params(**overrides))
    factory.assert_not_called()
