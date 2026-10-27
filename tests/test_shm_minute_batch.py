"""单日分钟批量协议：只使用原生样本 fixture 和内存邮箱。"""

import importlib.util
import json
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest import mock

import pytest


SPEC = importlib.util.spec_from_file_location("minute_batch_history_fixture", Path(__file__).with_name("test_shm_native_history.py"))
HISTORY = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(HISTORY)
CONTRACT = "SCOPED_MINUTE_HISTORY_BATCH_V1"
SYMBOLS = ["000001.SZ", "600000.SH"]


def read_params(**overrides):
    values = {key: value for key, value in HISTORY.params(count=301).items() if key != "code"}
    return {**values, "symbols": list(SYMBOLS), **overrides}


def download_params(**overrides):
    return {"symbols": list(SYMBOLS), "period": "1m", "start_time": "20260904",
            "end_time": "20260904", "incrementally": None, **overrides}


class MinuteBatchTests(HISTORY.FIXTURE.BridgeFixture):
    def setUp(self):
        super().setUp()
        self.bridge.market_enabled = True
        self.context.get_market_data_ex = mock.Mock(return_value={code: HISTORY.frame() for code in SYMBOLS})
        self.bridge.history_downloader = mock.Mock(return_value=None)

    def test_real_memory_mailbox_handshake_read_and_download_batch(self):
        def factory(name, account):
            native = HISTORY.ShmReadOnlyClient(name, account, region=HISTORY.FIXTURE.MemoryRegion(self.state))
            self.clients.append(native)
            return native

        client = HISTORY.ShmReadOnlyRpcClient(name=HISTORY.FIXTURE.NAME,
            account_id=HISTORY.FIXTURE.ACCOUNT, client_factory=factory)
        with ThreadPoolExecutor(max_workers=1) as executor:
            calls = [(client.verify_identity, (), {"expected_session_id": self.mailbox.session_id,
                "expected_native_pid": self.mailbox.server_pid}),
                (client.call, ("get_market_data_ex_batch", read_params()), {}),
                (client.call, ("download_history_data_batch", download_params()), {})]
            replies = []
            for function, args, kwargs in calls:
                future = executor.submit(function, *args, **kwargs)
                deadline = time.monotonic() + 2
                while not future.done() and time.monotonic() < deadline:
                    self.bridge.drain()
                    time.sleep(0.001)
                replies.append(future.result(timeout=1))
        self.assertEqual(replies[0]["minute_history_batch_contract"], CONTRACT)
        self.assertEqual(list(replies[1]["results"]), SYMBOLS)
        self.assertEqual(replies[2]["status"], "REQUESTED")
        self.context.get_market_data_ex.assert_called_once()
        self.assertEqual(self.bridge.history_downloader.call_count, 2)
        client._minute_batch_contract = None
        with pytest.raises(HISTORY.ShmProtocolError, match="contract"):
            client.call("get_market_data_ex_batch", read_params())
        self.context.get_market_data_ex.assert_called_once()
        client.close()

    def test_batch_read_is_one_native_call_and_keeps_single_symbol_values(self):
        result = self.process("get_market_data_ex_batch", params=read_params())
        self.assertTrue(result["ok"], result)
        value = result["data"]
        self.assertEqual(value["contract"], CONTRACT)
        self.assertEqual(value["source_evidence"]["native_call_count"], 1)
        self.context.get_market_data_ex.assert_called_once_with(HISTORY.FIELDS, SYMBOLS,
            period="1m", start_time="20260904000000", end_time="20260904235959", count=301,
            dividend_type="none", fill_data=False, subscribe=False)
        for code in SYMBOLS:
            self.context.get_market_data_ex.return_value = {code: HISTORY.frame()}
            old = self.process("get_market_data_ex", params=HISTORY.params(code=code, count=301))["data"]
            item = value["results"][code]
            for key in ["index", "rows", "columns", "source_evidence"]:
                self.assertEqual(item[key], old[key])
            self.assertEqual(item["native_pid"], self.mailbox.server_pid)
            self.assertEqual(item["session_id"], self.mailbox.session_id)

    def test_response_limit_includes_per_symbol_identity_and_outer_envelope(self):
        bare = self.bridge._history_batch_request(read_params())
        limit = len(json.dumps(bare, ensure_ascii=True, allow_nan=False, separators=(",", ":")).encode("ascii")) + 1
        self.ns["HISTORY_PAYLOAD_LIMIT_BYTES"] = limit
        result = self.process("get_market_data_ex_batch", params=read_params())
        self.assertFalse(result["ok"])
        self.assertEqual(result["server_error"], "HISTORY_PAYLOAD_TOO_LARGE")
        self.assertIsNone(result["data"])

    def test_maximum_50_symbols_and_301_rows_budget_is_explicit(self):
        codes = ["%06d.SZ" % index for index in range(1, 51)]
        self.context.get_market_data_ex.return_value = {code: HISTORY.frame() for code in codes}
        result = self.process("get_market_data_ex_batch", params=read_params(symbols=codes))
        self.assertTrue(result["ok"], result)
        self.assertEqual(len(result["data"]["results"]), 50)
        self.context.get_market_data_ex.assert_called_once()

    def test_missing_native_and_known_empty_are_distinct(self):
        self.context.get_market_data_ex.return_value = None
        value = self.process("get_market_data_ex_batch", params=read_params())["data"]
        self.assertEqual(value["status"], "UNAVAILABLE")
        self.assertTrue(all(item["status"] == "UNAVAILABLE" for item in value["results"].values()))
        self.context.get_market_data_ex.return_value = {}
        value = self.process("get_market_data_ex_batch", params=read_params())["data"]
        self.assertEqual(value["status"], "OK")
        self.assertTrue(all(item["status"] == "EMPTY" for item in value["results"].values()))

    def test_bad_request_is_rejected_before_native_execution(self):
        for overrides in [{"symbols": []}, {"symbols": SYMBOLS * 26}, {"symbols": ["000001.SZ"] * 2},
                          {"symbols": [1]}, {"symbols": ["000001.sz"]}, {"count": 302}, {"count": True},
                          {"fill_data": True}, {"subscribe": True}, {"period": "tick"},
                          {"end_time": "20260905235959"}, {"start_time": "20260230000000"}, {"download": True}]:
            self.assertFalse(self.process("get_market_data_ex_batch", params=read_params(**overrides))["ok"])
        self.context.get_market_data_ex.assert_not_called()

    def test_extra_symbol_and_oversize_response_are_not_truncated(self):
        self.context.get_market_data_ex.return_value["600001.SH"] = HISTORY.frame()
        result = self.process("get_market_data_ex_batch", params=read_params())
        self.assertEqual(result["server_error"], "HISTORY_SHAPE_INVALID")
        self.context.get_market_data_ex.return_value.pop("600001.SH")
        with mock.patch.dict(self.ns, {"HISTORY_PAYLOAD_LIMIT_BYTES": 1}):
            result = self.process("get_market_data_ex_batch", params=read_params())
        self.assertEqual(result["server_error"], "HISTORY_PAYLOAD_TOO_LARGE")
        self.assertIsNone(result["data"])

    def test_download_ack_is_per_symbol_and_requires_independent_readback(self):
        result = self.process("download_history_data_batch", params=download_params())
        self.assertTrue(result["ok"], result)
        value = result["data"]
        self.assertEqual(value["status"], "REQUESTED")
        self.assertIs(value["completion_confirmed"], False)
        self.assertIs(value["readback_required"], True)
        self.assertIs(value["automatic_retry"], False)
        self.assertEqual([call.args for call in self.bridge.history_downloader.call_args_list],
                         [(code, "1m", "20260904", "20260904") for code in SYMBOLS])
        self.assertTrue(all(item["status"] == "REQUESTED" for item in value["results"].values()))
        self.assertTrue(all(item["source_evidence"]["account_id"] == HISTORY.FIXTURE.ACCOUNT
                            for item in value["results"].values()))

    def test_partial_rejection_and_unknown_stop_without_resubmitting_or_losing_prefix(self):
        codes = SYMBOLS + ["000002.SZ"]
        for outcome, expected in [(False, "PARTIAL"), (RuntimeError("native outcome lost"), "UNKNOWN")]:
            self.bridge.history_downloader = mock.Mock(side_effect=[None, outcome])
            value = self.process("download_history_data_batch", params=download_params(symbols=codes))["data"]
            self.assertEqual(value["status"], expected)
            self.assertEqual(value["results"][codes[0]]["status"], "REQUESTED")
            self.assertEqual(value["results"][codes[2]]["status"], "NOT_ATTEMPTED")
            self.assertEqual(self.bridge.history_downloader.call_count, 2)

    def test_deadline_after_native_call_keeps_unknown_and_does_not_start_next_symbol(self):
        now = self.ns["time"].time()
        clock = [now]
        def download(*_args):
            clock[0] += 6
        self.bridge.history_downloader = mock.Mock(side_effect=download)
        request = self.envelope("download_history_data_batch", params=download_params(), read_expires_at_unix=now + 5)
        with mock.patch.object(self.ns["time"], "time", side_effect=lambda: clock[0]):
            result = self.bridge.process_request(request, sequence=self.counter)
        self.assertEqual(result["server_error"], "READ_EXPIRED_DURING_EXECUTION")
        self.assertEqual(result["data"]["status"], "UNKNOWN")
        self.assertEqual(result["data"]["results"][SYMBOLS[0]]["status"], "UNKNOWN")
        self.assertEqual(result["data"]["results"][SYMBOLS[1]]["status"], "NOT_ATTEMPTED")
        self.bridge.history_downloader.assert_called_once()


@pytest.mark.parametrize("method,values", [
    ("get_market_data_ex_batch", read_params(symbols=SYMBOLS * 26)),
    ("get_market_data_ex_batch", read_params(end_time="20260905235959")),
    ("get_market_data_ex_batch", read_params(count=302)),
    ("download_history_data_batch", download_params(symbols=SYMBOLS * 26)),
    ("download_history_data_batch", download_params(end_time="20260905")),
])
def test_client_rejects_unsafe_batch_before_transport(method, values):
    with pytest.raises(HISTORY.ShmProtocolError):
        HISTORY.ShmReadOnlyRpcClient._market_params(method, values)
