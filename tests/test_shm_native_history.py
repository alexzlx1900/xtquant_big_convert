"""已有原生真样本回放与有界历史 RPC；不连接 Windows 或下载历史。"""

import importlib.util
import sys
import time
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
        for overrides in ({"subscribe": True}, {"fill_data": True}, {"count": 11},
                          {"code": "600000.SH"}, {"end_time": "20260905235959"},
                          {"dividend_type": "front"}, {"download": True}, {"period": "tick"},
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


@pytest.mark.parametrize("overrides", [
    {"subscribe": True}, {"fill_data": True}, {"count": 11}, {"count": True},
    {"end_time": "20260905235959"}, {"dividend_type": "front"}, {"download": True},
])
def test_unsafe_client_params_do_not_open_mailbox(overrides):
    factory = mock.Mock()
    client = ShmReadOnlyRpcClient(name=FIXTURE.NAME, account_id=FIXTURE.ACCOUNT, client_factory=factory)
    with pytest.raises(ShmProtocolError):
        client.call("get_market_data_ex", params(**overrides))
    factory.assert_not_called()
