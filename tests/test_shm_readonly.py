# coding: utf-8
"""本地协议及受限依赖验收；内存区域和 Win32 调用替身不构成模拟端实测。"""

import ast
import base64
import builtins
import ctypes
import importlib.util
import os
import socket
import sys
import tempfile
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from types import SimpleNamespace
from unittest import mock


ROOT = Path(__file__).resolve().parent.parent
SPEC = importlib.util.spec_from_file_location("shm_readonly_builder", ROOT / "tools/build_shm_readonly_single_file.py")
BUILDER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(BUILDER)
SOURCE = BUILDER.build_source()
NAME = "Local\\AQP_SHM_TEST"
ACCOUNT = "SIM_TEST_ACCOUNT"


class MemoryState:
    def __init__(self):
        self.data = bytearray(64 + 8192 + 8 * 1024 * 1024)
        self.lock = threading.Lock()
        self.views = []


class MemoryRegion:
    def __init__(self, state):
        self.state = state
        self.pid = 7000
        self.closed = False
        self.close_error = None
        state.views.append(self)

    def acquire(self, milliseconds):
        if self.closed:
            raise RuntimeError("test region is closed")
        if milliseconds == 0:
            return self.state.lock.acquire(False)
        return self.state.lock.acquire(timeout=milliseconds / 1000.0)

    def release(self):
        self.state.lock.release()

    def read(self, offset, length):
        return bytes(self.state.data[offset:offset + length])

    def write(self, offset, raw):
        self.state.data[offset:offset + len(raw)] = raw

    def close(self):
        self.closed = True
        if self.close_error:
            raise RuntimeError(self.close_error)


class FakeContext:
    def __init__(self):
        self.calls = []
        self.run_time = mock.Mock()

    def get_full_tick(self, codes):
        self.calls.append(codes)
        return {code: {"lastPrice": 9.6 if code == "600000.SH" else 11.57,
                       "time": 1791874801000 if code == "600000.SH" else 1791874800000}
                for code in codes}


def position_row(code="000001"):
    return SimpleNamespace(m_strInstrumentID=code, m_strExchangeID="SZ", m_strInstrumentName="样本",
                           m_nVolume=100, m_nCanUseVolume=100, m_dOpenPrice=10.0,
                           m_dMarketValue=1157.0, m_dLastPrice=11.57)


def account_row(**overrides):
    values = {"m_strAccountID": ACCOUNT, "m_dAvailable": 3601968.16,
              "m_dBalance": 17879634.87, "m_dFrozenCash": 0.0,
              "m_dInstrumentValue": 14292546.310000047}
    values.update(overrides)
    return SimpleNamespace(**values)


def order_row(**overrides):
    values = {"m_strAccountID": ACCOUNT, "m_strInstrumentID": "000001", "m_strExchangeID": "SZ",
              "m_strOrderSysID": "ORDER-1", "m_strRemark": "remark-1", "m_nOffsetFlag": 49,
              "m_nVolumeTotalOriginal": 100, "m_nVolumeTraded": 20, "m_nOrderStatus": 55,
              "m_dLimitPrice": 11.61, "m_dTradedPrice": 11.61, "m_nOrderPriceType": 11,
              "m_nOpType": 2147483647, "m_strInsertDate": "20261014", "m_strInsertTime": "09:30:15.123"}
    values.update(overrides)
    return SimpleNamespace(**values)


def trade_row(**overrides):
    values = {"m_strAccountID": ACCOUNT, "m_strInstrumentID": "000001", "m_strExchangeID": "SZ",
              "m_strTradeID": "DEAL-1", "m_strOrderSysID": "ORDER-1", "m_strRemark": "remark-1",
              "m_nOffsetFlag": 49, "m_nVolume": 20, "m_dPrice": 11.61, "m_dTradeAmount": 232.2,
              "m_strTradeDate": "20261014", "m_strTradeTime": "09:30:16"}
    values.update(overrides)
    return SimpleNamespace(**values)


def make_namespace(quote_symbols=None):
    namespace = {"__name__": "shm_readonly_test"}
    exec(compile(SOURCE, "<shm-readonly-single-file>", "exec"), namespace)
    namespace["BIGQMT_ACCOUNT_ID"] = ACCOUNT
    namespace["READONLY_SHM_NAME"] = NAME
    namespace["READONLY_SYMBOLS"] = quote_symbols
    return namespace


class BridgeFixture(unittest.TestCase):
    quote_symbols = None

    def setUp(self):
        self.output = StringIO()
        self.redirect = redirect_stdout(self.output)
        self.redirect.__enter__()
        self.ns = make_namespace(self.quote_symbols)
        self.transport = self.ns["_load_embedded"]("aqp_readonly_transport")
        self.state = MemoryState()
        self.region = MemoryRegion(self.state)
        self.mailbox = self.transport.ReadOnlyMailbox(NAME, create=True, region=self.region)
        self.context = FakeContext()
        self.query = mock.Mock(return_value=[position_row()])
        self.bridge = self.ns["ShmReadOnlyBridge"](self.context, self.query, mailbox=self.mailbox)
        self.bridge.start()
        self.clients = []
        self.counter = 0

    def tearDown(self):
        try:
            for client in self.clients:
                client.close()
            if self.bridge is not None:
                self.bridge.close()
        finally:
            self.redirect.__exit__(None, None, None)

    def envelope(self, method="ping", **overrides):
        self.counter += 1
        value = {"schema_version": 1, "request_id": "test-%s" % self.counter, "account_id": ACCOUNT,
                 "method": method, "params": {}, "session_id": self.mailbox.session_id,
                 "sequence": self.counter, "read_expires_at_unix": time.time() + 5}
        value.update(overrides)
        return value

    def process(self, method="ping", **overrides):
        value = self.envelope(method, **overrides)
        return self.bridge.process_request(value, sequence=self.counter)

    def client(self):
        value = self.transport.ShmReadOnlyClient(NAME, ACCOUNT, region=MemoryRegion(self.state))
        self.clients.append(value)
        return value

    def roundtrip(self, client, method="ping", **overrides):
        payload = self.envelope(method, **overrides)
        payload.pop("sequence", None)
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(client.send_request, payload, 1.0)
            deadline = time.monotonic() + 2
            while not future.done() and time.monotonic() < deadline:
                self.bridge.drain()
                time.sleep(0.001)
            return future.result(timeout=0.2)

    def post_raw(self, payload=None, raw=None):
        raw = self.transport.encode_payload(payload, self.transport.REQUEST_CAPACITY) if raw is None else raw
        header = self.mailbox._header()
        sequence = header[4] + 1
        self.region.write(self.transport.REQUEST_OFFSET, raw)
        header[4], header[7] = sequence, len(raw)
        self.mailbox._write_header(header)
        return sequence


class RequestBoundaryTest(BridgeFixture):
    def test_methods_outside_allowlist_do_not_call_qmt(self):
        for method in ("submit_order", "cancel_order", "download_history_data", "subscribe_quote", "get_assure_contract", "unknown"):
            with self.subTest(method=method):
                response = self.process(method)
                self.assertFalse(response["ok"])
                self.assertEqual(response["server_error"], "METHOD_DENIED")
        self.query.assert_not_called()
        self.assertEqual(self.context.calls, [])

    def test_account_header_and_params_require_exact_match(self):
        for overrides in ({"account_id": "OTHER"}, {"account_id": None}, {"params": {"account_id": "OTHER"}}):
            self.assertEqual(self.process("get_positions", **overrides)["server_error"], "ACCOUNT_MISMATCH")
        self.query.assert_not_called()

    def test_params_and_schema_are_strict(self):
        for params in (None, [], False, ""):
            self.assertEqual(self.process("get_positions", params=params)["server_error"], "INVALID_PARAMS")
        for version in (None, True, "1", 2):
            self.assertEqual(self.process(schema_version=version)["server_error"], "INVALID_SCHEMA")
        self.query.assert_not_called()

    def test_request_id_must_be_bounded_ascii(self):
        for request_id in (None, 1, "", "has space", "\n", "中文", "x" * 129):
            self.assertEqual(self.process(request_id=request_id)["server_error"], "INVALID_REQUEST_ID")

    def test_missing_or_old_session_is_rejected(self):
        for session in (None, "00" * 16):
            self.assertEqual(self.process("get_positions", session_id=session)["server_error"], "SESSION_MISMATCH")
        self.query.assert_not_called()

    def test_sequence_type_and_wire_value_are_checked(self):
        for sequence in (True, 0, -1, "1", 999):
            self.assertEqual(self.process("get_positions", sequence=sequence)["server_error"], "SEQUENCE_MISMATCH")
        self.query.assert_not_called()

    def test_nonfinite_missing_or_unbounded_deadlines_are_rejected(self):
        for deadline in (None, True, "1", float("inf"), float("nan"), time.time() + 120):
            self.assertEqual(self.process("get_positions", read_expires_at_unix=deadline)["server_error"], "INVALID_DEADLINE")
        self.query.assert_not_called()

    def test_expired_request_does_not_call_qmt(self):
        self.assertEqual(self.process("get_positions", read_expires_at_unix=time.time() - 1)["server_error"], "READ_EXPIRED")
        self.query.assert_not_called()

    def test_duplicate_request_id_does_not_repeat_native_query(self):
        first = self.process("get_positions", request_id="same")
        second = self.process("get_positions", request_id="same")
        self.assertTrue(first["ok"])
        self.assertEqual(second["server_error"], "DUPLICATE_REQUEST")
        self.query.assert_called_once()

    def test_native_query_only_runs_on_original_thread(self):
        with ThreadPoolExecutor(max_workers=1) as executor:
            response = executor.submit(self.bridge.process_request, self.envelope("get_positions")).result()
        self.assertEqual(response["server_error"], "WRONG_CALLBACK_THREAD")
        self.query.assert_not_called()

    def test_non_object_request_is_a_failure(self):
        for value in (None, [], 1, "bad"):
            self.assertEqual(self.bridge.process_request(value, sequence=1)["server_error"], "INVALID_REQUEST")

    def test_position_source_counts_and_fields_are_preserved(self):
        response = self.process("get_positions", params={"include_source_evidence": True})
        self.assertTrue(response["ok"])
        evidence = response["data"]["source_evidence"]
        self.assertTrue(evidence["complete"])
        self.assertEqual([evidence[key] for key in ("raw_row_count", "converted_row_count", "returned_row_count")], [1, 1, 1])
        self.assertEqual(response["data"]["positions"]["000001.SZ"]["volume"], 100)

    def test_failed_native_position_query_retains_failure_evidence(self):
        self.query.side_effect = RuntimeError("native source unavailable")
        response = self.process("get_positions")
        self.assertFalse(response["ok"])
        self.assertEqual(response["server_error"], "POSITION_QUERY_FAILED")
        self.assertEqual(response["data"]["source_evidence"]["status"], "FAILED")

    def test_partial_positions_cannot_be_empty_success(self):
        self.query.return_value.append(SimpleNamespace(m_strInstrumentID="bad"))
        response = self.process("get_positions")
        self.assertFalse(response["ok"])
        self.assertEqual(response["data"]["source_evidence"]["skipped_row_count"], 1)

    def test_existing_finite_alias_and_zero_market_value_semantics(self):
        row = self.query.return_value[0]
        row.m_dMarketValue, row.m_dInstrumentValue = float("inf"), 1157.0
        response = self.process("get_positions")
        self.assertTrue(response["ok"])
        self.assertEqual(response["data"]["000001.SZ"]["market_value"], 1157.0)
        row.m_dMarketValue = 0.0
        self.assertEqual(self.process("get_positions")["data"]["000001.SZ"]["market_value"], 0.0)

    def test_quote_symbol_and_original_time_are_preserved(self):
        outside = self.process("get_full_tick", params={"codes": ["600000.SH"]})
        self.assertTrue(outside["ok"])
        self.assertEqual(outside["data"]["600000.SH"]["time"], 1791874801000)
        self.assertEqual(self.context.calls, [["600000.SH"]])
        for method in ("get_full_tick", "get_ticks"):
            response = self.process(method, params={"codes": ["000001.SZ"]})
            self.assertTrue(response["ok"])
            self.assertEqual(response["data"]["000001.SZ"]["time"], 1791874800000)

    def test_missing_quote_and_native_exception_are_not_success(self):
        self.context.get_full_tick = mock.Mock(return_value={})
        self.assertEqual(self.process("get_full_tick", params={"codes": ["000001.SZ"]})["server_error"], "QMT_QUOTE_UNAVAILABLE")
        self.context.get_full_tick.side_effect = RuntimeError("quote failure")
        response = self.process("get_full_tick", params={"codes": ["000001.SZ"]})
        self.assertEqual(response["server_error"], "QMT_QUERY_FAILED")
        self.assertIn("quote failure", response["error"])

    def test_native_query_finishing_after_deadline_is_unconfirmed(self):
        value = self.envelope("get_positions", read_expires_at_unix=1000.5)
        with mock.patch.object(self.ns["time"], "time", side_effect=[1000, 1000, 1001]):
            response = self.bridge.process_request(value, sequence=self.counter)
        self.assertEqual(response["server_error"], "READ_EXPIRED_DURING_EXECUTION")
        self.assertIsNone(response["data"])


class MultiSymbolQuoteTest(BridgeFixture):
    quote_symbols = ("000001.SZ", "600000.SH")

    def test_aliases_read_one_native_batch_and_preserve_each_symbol_time(self):
        for method in ("get_full_tick", "get_ticks"):
            codes = ["600000.SH", "000001.SZ"]
            result = self.roundtrip(self.client(), method, params={"codes": codes})
            self.assertTrue(result["ok"])
            self.assertEqual(set(result["data"]), set(codes))
            self.assertEqual(result["data"]["600000.SH"], {"lastPrice": 9.6, "time": 1791874801000})
            self.assertEqual(result["data"]["000001.SZ"]["time"], 1791874800000)
        self.assertEqual(self.context.calls, [["600000.SH", "000001.SZ"]] * 2)
        self.query.assert_not_called()

    def test_legacy_single_symbol_selectors_remain_supported(self):
        for params in ({"code": "000001.SZ"}, {"codes": "600000.SH"}, {"codes": ["000001.SZ"]}):
            self.assertTrue(self.process("get_ticks", params=params)["ok"])
        self.assertEqual(self.context.calls, [["000001.SZ"], ["600000.SH"], ["000001.SZ"]])

    def test_invalid_requests_and_symbols_do_not_call_native(self):
        for params in ({}, {"codes": []}, {"codes": ["000001.SZ"] * 2},
                       {"codes": ["000001.SZ"] * 65}, {"codes": [None]},
                       {"codes": ["000001.SZ"], "code": "000001.SZ"},
                       {"codes": ["000001.SZ"], "subscribe": True}):
            with self.subTest(params=params):
                self.assertEqual(self.process("get_ticks", params=params)["server_error"], "INVALID_PARAMS")
        for code in ("*.SH", "bad.SZ", "000001.XX", "SH000001", "600001.SH "):
            self.assertEqual(self.process("get_ticks", params={"codes": [code]})["server_error"], "INVALID_PARAMS")
        self.assertEqual(self.context.calls, [])

    def test_missing_symbol_preserves_partial_result_as_failure(self):
        self.context.get_full_tick = mock.Mock(return_value={"000001.SZ": {"lastPrice": 11.57, "time": 1791874800000}})
        response = self.process("get_ticks", params={"codes": list(self.quote_symbols)})
        self.assertFalse(response["ok"])
        self.assertEqual(response["server_error"], "QMT_QUOTE_UNAVAILABLE")
        self.assertEqual(response["data"]["missing_symbols"], ["600000.SH"])
        self.assertEqual(response["data"]["quotes"]["000001.SZ"]["lastPrice"], 11.57)
        self.context.get_full_tick.assert_called_once_with(list(self.quote_symbols))

    def test_invalid_price_time_and_unrequested_symbol_cannot_be_success(self):
        for field, value in (("lastPrice", None), ("lastPrice", 0), ("lastPrice", float("nan")),
                             ("lastPrice", True), ("time", None), ("time", 0), ("time", True)):
            with self.subTest(field=field, value=value):
                quote = {"lastPrice": 9.6, "time": 1791874801000, field: value}
                self.context.get_full_tick = mock.Mock(return_value={"600000.SH": quote})
                response = self.process("get_ticks", params={"codes": ["600000.SH"]})
                self.assertFalse(response["ok"])
                self.assertEqual(response["data"]["invalid_symbols"], ["600000.SH"])
        self.context.get_full_tick = mock.Mock(return_value={
            "600000.SH": {"lastPrice": 9.6, "time": 1791874801000}, "600001.SH": {},
        })
        response = self.process("get_ticks", params={"codes": ["600000.SH"]})
        self.assertFalse(response["ok"])
        self.assertEqual(response["data"]["unexpected_symbols"], ["600001.SH"])


class MailboxProtocolTest(BridgeFixture):
    def test_initial_mutex_contention_waits_then_publishes_request_once(self):
        client = self.client()
        payload = self.envelope("get_positions")
        payload.pop("sequence")
        waiting = threading.Event()
        acquire = client.mailbox.region.acquire
        def observed_acquire(milliseconds):
            if milliseconds > 0:
                waiting.set()
            return acquire(milliseconds)
        self.state.lock.acquire()
        held = True
        try:
            with mock.patch.object(client.mailbox.region, "acquire", side_effect=observed_acquire) as lock_calls, \
                    mock.patch.object(client.mailbox.region, "write", wraps=client.mailbox.region.write) as writes, \
                    ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(client.send_request, payload, 1.0)
                self.assertTrue(waiting.wait(0.5))
                self.assertFalse(future.done())
                self.state.lock.release()
                held = False
                deadline = time.monotonic() + 1.5
                while not future.done() and time.monotonic() < deadline:
                    self.bridge.drain()
                    time.sleep(0.001)
                self.assertTrue(future.result(timeout=0.1)["ok"])
                self.assertGreater(lock_calls.call_args_list[0].args[0], 0)
                self.assertLessEqual(lock_calls.call_args_list[0].args[0], 1000)
                posted = [call for call in writes.call_args_list if call.args[0] == self.transport.REQUEST_OFFSET]
                self.assertEqual(len(posted), 1)
            self.query.assert_called_once()
            self.assertFalse(client.poisoned)
            self.assertEqual(self.mailbox._header()[4:7], [1, 1, 1])
        finally:
            if held:
                self.state.lock.release()

    def test_initial_mutex_timeout_uses_earlier_read_deadline_and_posts_nothing(self):
        client = self.client()
        before = bytes(self.state.data)
        payload = self.envelope(read_expires_at_unix=time.time() + 0.03)
        payload.pop("sequence")
        self.state.lock.acquire()
        try:
            with mock.patch.object(client.mailbox.region, "acquire", wraps=client.mailbox.region.acquire) as acquire, \
                    mock.patch.object(client.mailbox.region, "write", wraps=client.mailbox.region.write) as writes:
                with self.assertRaisesRegex(self.transport.ShmTimeoutError, "before publication; request not sent"):
                    client.send_request(payload, 1.0)
                self.assertGreater(acquire.call_args.args[0], 0)
                self.assertLessEqual(acquire.call_args.args[0], 30)
                writes.assert_not_called()
        finally:
            self.state.lock.release()
        self.assertEqual(bytes(self.state.data), before)
        self.assertFalse(client.poisoned)
        self.query.assert_not_called()

    def test_initial_mutex_acquired_after_deadline_releases_without_publication(self):
        client = self.client()
        before = bytes(self.state.data)
        payload = self.envelope()
        payload.pop("sequence")
        acquire = client.mailbox.region.acquire
        def late_acquire(milliseconds):
            time.sleep(0.02)
            return acquire(0)
        with mock.patch.object(client.mailbox.region, "acquire", side_effect=late_acquire), \
                mock.patch.object(client.mailbox.region, "release", wraps=client.mailbox.region.release) as release, \
                mock.patch.object(client.mailbox.region, "write", wraps=client.mailbox.region.write) as writes:
            with self.assertRaisesRegex(self.transport.ShmTimeoutError, "before publication; request not sent"):
                client.send_request(payload, 0.01)
            writes.assert_not_called()
            release.assert_called_once()
        self.assertEqual(bytes(self.state.data), before)
        self.assertFalse(client.poisoned)
        self.query.assert_not_called()

    def test_full_client_server_roundtrip_and_confirmation(self):
        client = self.client()
        ping = self.roundtrip(client)
        self.assertTrue(ping["ok"])
        self.assertFalse(ping["data"]["allow_order_methods"])
        positions = self.roundtrip(client, "get_positions", params={"include_source_evidence": True})
        self.assertTrue(positions["data"]["source_evidence"]["complete"])
        quote = self.roundtrip(client, "get_full_tick", params={"codes": ["000001.SZ"]})
        self.assertTrue(quote["ok"])
        self.assertEqual(self.mailbox._header()[4:7], [3, 3, 3])

    def test_completed_response_still_blocks_until_client_confirms(self):
        value = self.envelope()
        sequence = self.post_raw(value)
        self.bridge.drain()
        before = bytes(self.state.data)
        with self.assertRaises(self.transport.ShmBusyError):
            self.client().send_request(self.envelope(), 0.05)
        self.assertEqual(bytes(self.state.data), before)
        self.assertEqual(self.mailbox._header()[4:7], [sequence, sequence, 0])

    def test_pending_request_cannot_be_overwritten(self):
        self.post_raw(self.envelope())
        before = bytes(self.state.data)
        with self.assertRaises(self.transport.ShmBusyError):
            self.client().send_request(self.envelope(), 0.05)
        self.assertEqual(bytes(self.state.data), before)

    def test_timeout_poisoning_prevents_same_or_new_client_resubmit(self):
        client = self.client()
        with self.assertRaises(self.transport.ShmTimeoutError):
            client.send_request(self.envelope(), 0.02)
        self.assertTrue(client.poisoned)
        with self.assertRaises(self.transport.ShmProtocolError):
            client.send_request(self.envelope(), 0.02)
        with self.assertRaises(self.transport.ShmBusyError):
            self.client().send_request(self.envelope(), 0.02)
        self.bridge.drain()
        self.assertEqual(self.mailbox._header()[6], 0)
        self.query.assert_not_called()

    def test_oversized_request_does_not_modify_mailbox(self):
        client = self.client()
        before = bytes(self.state.data)
        with self.assertRaises(self.transport.ShmCapacityError):
            client.send_request(self.envelope(params={"large": "x" * 9000}), 0.1)
        self.assertEqual(bytes(self.state.data), before)

    def test_oversized_response_is_explicit_failure_not_truncated(self):
        self.context.get_full_tick = lambda codes: {codes[0]: {"lastPrice": 11.57, "time": 1791874800000,
            "large": "x" * (self.transport.RESPONSE_CAPACITY + 1024)}}
        response = self.roundtrip(self.client(), "get_full_tick", params={"codes": ["000001.SZ"]})
        self.assertFalse(response["ok"])
        self.assertEqual(response["server_error"], "RESPONSE_TOO_LARGE")
        self.assertIsNone(response["data"])

    def test_nonfinite_json_duplicate_keys_and_invalid_bytes_fail_closed(self):
        for raw in (b'{"x":NaN}', b'{"x":1e999}', b'{"x":1,"x":2}', b'bad', b'\xff'):
            with self.subTest(raw=raw):
                with self.assertRaises(self.transport.ShmProtocolError):
                    self.transport.decode_payload(raw)
        self.post_raw(raw=b'{"x":1,"x":2}')
        with self.assertRaises(self.transport.ShmProtocolError):
            self.bridge.drain()
        self.assertTrue(self.bridge.closed)
        self.assertTrue(self.region.closed)
        self.query.assert_not_called()

    def test_corrupt_length_fails_before_memory_read_or_qmt(self):
        self.post_raw(self.envelope("get_positions"))
        header = self.mailbox._header()
        header[7] = self.transport.REQUEST_CAPACITY + 1
        self.mailbox._write_header(header)
        with self.assertRaises(self.transport.ShmProtocolError):
            self.bridge.drain()
        self.query.assert_not_called()

    def test_pending_publish_is_retried_without_repeating_query(self):
        self.post_raw(self.envelope("get_positions"))
        with mock.patch.object(self.mailbox, "publish_response", wraps=self.mailbox.publish_response, side_effect=[False, mock.DEFAULT]):
            self.bridge.drain()
            self.assertIsNotNone(self.bridge.pending)
            self.bridge.drain()
        self.query.assert_called_once()
        self.assertIsNone(self.bridge.pending)
        self.assertEqual(self.mailbox._header()[5], 1)

    def test_response_metadata_mismatch_is_not_accepted(self):
        client = self.client()
        request = self.envelope()
        good = dict(request, ok=True, data={}, error="", server_error="", handled_at="2026-10-14 10:00:00")
        client._validate_response(request, good)
        for field in ("request_id", "account_id", "method", "session_id", "sequence", "schema_version", "ok"):
            with self.subTest(field=field):
                bad = dict(good)
                bad[field] = "WRONG"
                with self.assertRaises(self.transport.ShmProtocolError):
                    client._validate_response(request, bad)

    def test_missing_result_and_inconsistent_error_fields_are_rejected(self):
        client = self.client()
        request = self.envelope()
        good = dict(request, ok=True, data={}, error="", server_error="", handled_at="2026-10-14 10:00:00")
        for overrides in ({"data": None}, {"error": []}, {"server_error": "BAD"}, {"ok": False}):
            with self.assertRaises(self.transport.ShmProtocolError):
                client._validate_response(request, dict(good, **overrides))
        for missing in ("data", "error", "server_error", "handled_at"):
            broken = dict(good)
            broken.pop(missing)
            with self.assertRaises(self.transport.ShmProtocolError):
                client._validate_response(request, broken)

    def test_mismatched_response_poisoning_keeps_confirmation_unset(self):
        client = self.client()
        payload = self.envelope()
        payload.pop("sequence")
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(client.send_request, payload, 1.0)
            deadline = time.monotonic() + 0.5
            incoming = None
            while incoming is None and time.monotonic() < deadline:
                incoming = self.mailbox.read_request()
                time.sleep(0.001)
            self.assertIsNotNone(incoming)
            sequence, request = incoming
            response = self.bridge.process_request(request, sequence=sequence)
            response["request_id"] = "another-request"
            self.assertTrue(self.mailbox.publish_response(sequence, response))
            with self.assertRaises(self.transport.ShmProtocolError):
                future.result(timeout=0.5)
        self.assertTrue(client.poisoned)
        self.assertEqual(self.mailbox._header()[6], 0)

    def test_bad_client_envelope_and_timeout_do_not_post(self):
        client = self.client()
        before = bytes(self.state.data)
        for timeout in (0, -1, True, float("nan"), float("inf"), 61):
            with self.assertRaises(ValueError):
                client.send_request(self.envelope(), timeout)
        for overrides in ({"request_id": ""}, {"method": None}, {"account_id": "OTHER"}, {"session_id": "old"}, {"sequence": True}):
            with self.assertRaises((ValueError, self.transport.ShmProtocolError)):
                client.send_request(self.envelope(**overrides), 0.1)
        self.assertEqual(bytes(self.state.data), before)

    def test_changed_session_and_corrupt_header_are_rejected(self):
        client = self.client()
        header = self.mailbox._header()
        header[9] = b'x' * 16
        self.mailbox._write_header(header)
        with self.assertRaises(self.transport.ShmProtocolError):
            client.send_request(self.envelope(), 0.1)
        header[9] = bytes.fromhex(self.mailbox.session_id)
        header[1] = 999
        self.mailbox._write_header(header)
        with self.assertRaises(self.transport.ShmProtocolError):
            self.mailbox.read_request()
        header[1] = self.transport.PROTOCOL_VERSION
        self.mailbox._write_header(header)

    def test_stop_is_idempotent_and_clients_see_closed_state(self):
        client = self.client()
        self.bridge.close()
        self.bridge.close()
        with self.assertRaises(self.transport.ShmProtocolError):
            client.send_request(self.envelope(), 0.1)
        self.assertTrue(self.region.closed)

    def test_deadline_and_request_budget_close_service(self):
        self.ns["READONLY_MAX_REQUESTS"] = 1
        response = self.roundtrip(self.client())
        self.assertTrue(response["ok"])
        self.assertTrue(self.bridge.closed)
        self.assertTrue(self.region.closed)

    def test_hard_session_deadline_stops_before_query(self):
        self.post_raw(self.envelope("get_positions"))
        self.bridge.created_at -= 61
        self.bridge.drain()
        self.assertTrue(self.bridge.closed)
        self.query.assert_not_called()

    def test_other_thread_cannot_consume_or_overwrite_pending_request(self):
        self.post_raw(self.envelope("get_positions"))
        before = bytes(self.state.data)
        with ThreadPoolExecutor(max_workers=1) as executor:
            with self.assertRaises(self.ns["ReadOnlyRequestError"]):
                executor.submit(self.bridge.drain).result()
        self.assertEqual(bytes(self.state.data), before)
        self.assertFalse(self.bridge.closed)
        self.query.assert_not_called()
        self.bridge.drain()
        self.query.assert_called_once()

    def test_direct_handler_also_enforces_hard_session_deadline(self):
        self.bridge.created_at -= 61
        self.assertEqual(self.process("get_positions")["server_error"], "SESSION_CLOSED")
        self.query.assert_not_called()


class BuildAndDependencyTest(unittest.TestCase):
    def test_builder_needs_only_entry_and_direct_sources_without_untracked_helper(self):
        with tempfile.TemporaryDirectory(prefix="readonly-builder-isolated-") as temporary:
            isolated = Path(temporary)
            paths = list(BUILDER.SOURCE_FILES.values()) + [
                "src/BIGQMT_SHM_READONLY.py", "src/bigqmt_signal_trader/redis_rpc.py",
                "tools/build_shm_readonly_single_file.py",
            ]
            for relative in paths:
                target = isolated / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes((ROOT / relative).read_bytes())
            self.assertFalse((isolated / "tools/build_readonly_single_file.py").exists())
            spec = importlib.util.spec_from_file_location(
                "isolated_readonly_builder", isolated / "tools/build_shm_readonly_single_file.py"
            )
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            self.assertEqual(module.build_source(), SOURCE)

    def test_ascii_gbk_and_python36_syntax_for_every_embedded_module(self):
        self.assertTrue(SOURCE.isascii())
        compile(SOURCE.encode("gbk"), "<qmt-gbk-strategy>", "exec")
        ast.parse(SOURCE, feature_version=(3, 6))
        ns = make_namespace()
        for name, encoded in ns["_EMBEDDED_SOURCES"].items():
            decoded = base64.b64decode(encoded).decode("utf-8")
            tree = ast.parse(decoded, filename=name, feature_version=(3, 6))
            self.assertFalse(any(isinstance(node, ast.Attribute) and node.attr == "isascii" for node in ast.walk(tree)))

    def test_closed_dependency_graph_has_no_restricted_modules_or_io(self):
        ns = make_namespace()
        self.assertEqual(set(ns["_EMBEDDED_SOURCES"]), {"bigqmt_signal_trader.models", "bigqmt_signal_trader.code_utils", "bigqmt_signal_trader.adapters.position_bigqmt", "bigqmt_signal_trader.adapters.account_readonly", "aqp_readonly_serializer", "aqp_readonly_transport"})
        sources = [SOURCE] + [base64.b64decode(value).decode("utf-8") for value in ns["_EMBEDDED_SOURCES"].values()]
        for source in sources:
            tree = ast.parse(source)
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    self.assertTrue(all(alias.name.split(".")[0] in ns["_STANDARD_IMPORTS"] for alias in node.names))
                if isinstance(node, ast.ImportFrom) and not node.level:
                    self.assertIn(node.module.split(".")[0], ns["_STANDARD_IMPORTS"])
                if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
                    self.assertNotIn(node.func.id, ("open", "Redis", "socket"))
        for name in ("redis", "zmq", "socket", "sqlite3", "bigqmt_signal_trader.redis_rpc", "bigqmt_signal_trader.exec_events"):
            with self.assertRaises(ImportError):
                ns["_bundle_import"](name)

    def test_embedded_queries_work_with_file_and_network_access_disabled(self):
        before = {name: value for name, value in sys.modules.items() if name.startswith("bigqmt_signal_trader")}
        with mock.patch.object(builtins, "open", side_effect=PermissionError("Foribdden FileIO")), mock.patch.object(Path, "open", side_effect=PermissionError("blocked")), mock.patch.object(socket, "socket", side_effect=PermissionError("network blocked")), redirect_stdout(StringIO()):
            ns = make_namespace()
            ns["READONLY_SYMBOLS"] = ["000001.SZ", "600000.SH"]
            transport = ns["_load_embedded"]("aqp_readonly_transport")
            state = MemoryState()
            mailbox = transport.ReadOnlyMailbox(NAME, create=True, region=MemoryRegion(state))
            bridge = ns["ShmReadOnlyBridge"](FakeContext(), lambda *args: [position_row()], mailbox=mailbox)
            bridge.start()
            request = {"schema_version": 1, "request_id": "no-io", "account_id": ACCOUNT, "method": "get_positions", "params": {}, "session_id": mailbox.session_id, "sequence": 1, "read_expires_at_unix": time.time() + 5}
            self.assertTrue(bridge.process_request(request, sequence=1)["ok"])
            request.update(request_id="no-network", method="get_full_tick", params={"codes": ["000001.SZ", "600000.SH"]}, sequence=2)
            self.assertTrue(bridge.process_request(request, sequence=2)["ok"])
            bridge.close()
        after = {name: value for name, value in sys.modules.items() if name.startswith("bigqmt_signal_trader")}
        self.assertEqual(before, after)

    def test_placeholder_and_invalid_config_fail_before_native_creation(self):
        for field, value in (("BIGQMT_ACCOUNT_ID", "YOUR_ACCOUNT_ID"), ("READONLY_SYMBOL", "０００００１.SZ"), ("READONLY_MAX_REQUESTS", True), ("READONLY_MAX_SECONDS", 0), ("READONLY_SHM_NAME", "Global\\AQP_SHM_BAD")):
            ns = make_namespace()
            ns[field] = value
            with self.assertRaises(ValueError):
                ns["ShmReadOnlyBridge"](FakeContext(), lambda *args: [])

    def test_multi_quote_configuration_is_bounded_and_unique(self):
        for symbols in ([], "000001.SZ", [None], ["000001.SZ"] * 2, ["000001.SZ"] * 65,
                        ["０００００１.SZ"], ["000001.XX"]):
            with self.subTest(symbols=symbols):
                ns = make_namespace(symbols)
                with self.assertRaises(ValueError):
                    ns["ShmReadOnlyBridge"](FakeContext(), lambda *args: [])

    def test_scheduling_failure_keeps_original_error_and_closes_resources(self):
        ns = make_namespace()
        bridge = mock.Mock()
        context = FakeContext()
        context.run_time.side_effect = RuntimeError("timer registration failed")
        ns["get_trade_detail_data"] = lambda *args: []
        bridge.close.side_effect = RuntimeError("cleanup detail")
        with mock.patch.dict(ns, {"ShmReadOnlyBridge": mock.Mock(return_value=bridge)}), redirect_stdout(StringIO()) as output:
            with self.assertRaisesRegex(RuntimeError, "timer registration failed"):
                ns["init"](context)
        bridge.close.assert_called_once_with("init_failed", failed=True)
        self.assertIn("cleanup detail", output.getvalue())
        self.assertIn("timer registration failed", output.getvalue())

    def test_initialization_failure_is_not_masked_by_an_unassigned_variable(self):
        ns = make_namespace()
        with mock.patch.dict(ns, {"ShmReadOnlyBridge": mock.Mock(side_effect=RuntimeError("mapping create denied"))}), redirect_stdout(StringIO()):
            with self.assertRaisesRegex(RuntimeError, "mapping create denied"):
                ns["init"](FakeContext())


class Win32FailurePathTest(unittest.TestCase):
    def setUp(self):
        self.transport = make_namespace()["_load_embedded"]("aqp_readonly_transport")
        self.buffer = ctypes.create_string_buffer(self.transport.MAP_SIZE)
        results = {"CreateFileMappingW": 101, "OpenFileMappingW": 101, "MapViewOfFile": ctypes.addressof(self.buffer), "UnmapViewOfFile": True, "CreateMutexW": 102, "OpenMutexW": 102, "WaitForSingleObject": 0, "ReleaseMutex": True, "CloseHandle": True, "GetCurrentProcessId": 7000}
        self.kernel = SimpleNamespace(**{name: mock.Mock(return_value=value) for name, value in results.items()})
        self.last_error = mock.patch.object(ctypes, "get_last_error", return_value=0, create=True)
        self.reset_error = mock.patch.object(ctypes, "set_last_error", create=True)
        self.last_error.start()
        self.reset_error.start()

    def tearDown(self):
        self.last_error.stop()
        self.reset_error.stop()

    def test_client_only_opens_existing_native_objects(self):
        region = self.transport.Win32Region(NAME, kernel=self.kernel)
        self.kernel.CreateFileMappingW.assert_not_called()
        self.kernel.CreateMutexW.assert_not_called()
        self.kernel.OpenFileMappingW.assert_called_once()
        self.kernel.OpenMutexW.assert_called_once()
        region.close()
        self.assertEqual(self.kernel.CloseHandle.call_count, 2)

    def test_partial_mapping_failure_releases_mapping_handle(self):
        self.kernel.MapViewOfFile.return_value = None
        with self.assertRaisesRegex(self.transport.ShmNativeError, "MapViewOfFile"):
            self.transport.Win32Region(NAME, create=True, kernel=self.kernel)
        self.kernel.CloseHandle.assert_called_once_with(101)

    def test_new_client_refuses_old_smaller_mapping_and_preserves_wire_offsets(self):
        legacy_size = 64 + 8192 + 262144
        self.assertEqual(self.transport.HEADER_SIZE, 64)
        self.assertEqual(self.transport.REQUEST_OFFSET, 64)
        self.assertEqual(self.transport.RESPONSE_OFFSET, 8256)
        self.assertEqual(self.transport.RESPONSE_CAPACITY, 8 * 1024 * 1024)
        self.kernel.MapViewOfFile.side_effect = lambda mapping, access, high, low, size: (
            ctypes.addressof(self.buffer) if size <= legacy_size else None)
        with self.assertRaisesRegex(self.transport.ShmNativeError, "MapViewOfFile"):
            self.transport.Win32Region(NAME, create=False, kernel=self.kernel)
        self.kernel.CreateFileMappingW.assert_not_called()
        self.kernel.MapViewOfFile.assert_called_once_with(101, 6, 0, 0, self.transport.MAP_SIZE)
        self.kernel.CloseHandle.assert_called_once_with(101)

    def test_mutex_creation_failure_releases_view_and_mapping(self):
        self.kernel.CreateMutexW.return_value = None
        with self.assertRaisesRegex(self.transport.ShmNativeError, "mutex open/create"):
            self.transport.Win32Region(NAME, create=True, kernel=self.kernel)
        self.kernel.UnmapViewOfFile.assert_called_once()
        self.kernel.CloseHandle.assert_called_once_with(101)

    def test_existing_mapping_is_never_overwritten_or_attached_by_server(self):
        ctypes.get_last_error.return_value = 183
        with self.assertRaisesRegex(self.transport.ShmNativeError, "already exists"):
            self.transport.Win32Region(NAME, create=True, kernel=self.kernel)
        self.kernel.MapViewOfFile.assert_not_called()
        self.kernel.CloseHandle.assert_called_once_with(101)

    def test_abandoned_mutex_is_released_and_session_rejected(self):
        region = self.transport.Win32Region(NAME, create=True, kernel=self.kernel)
        self.kernel.WaitForSingleObject.return_value = 128
        with self.assertRaisesRegex(self.transport.ShmProtocolError, "abandoned"):
            region.acquire(0)
        self.kernel.ReleaseMutex.assert_called_once()
        region.close()

    def test_cleanup_attempts_all_handles_and_reports_partial_failure(self):
        region = self.transport.Win32Region(NAME, create=True, kernel=self.kernel)
        self.kernel.UnmapViewOfFile.return_value = False
        with self.assertRaisesRegex(self.transport.ShmNativeError, "cleanup incomplete"):
            region.close()
        self.assertEqual(self.kernel.CloseHandle.call_count, 2)
        self.assertTrue(region.cleanup_errors)
        region.close()
        self.assertEqual(self.kernel.CloseHandle.call_count, 2)

    def test_memory_bounds_fail_before_native_access(self):
        region = self.transport.Win32Region(NAME, create=True, kernel=self.kernel)
        for offset, length in ((-1, 1), (self.transport.MAP_SIZE, 1), (True, 1)):
            with self.assertRaises(self.transport.ShmProtocolError):
                region.read(offset, length)
        region.close()

    def test_unsupported_host_is_reported_without_side_effects(self):
        if os.name != "nt":
            with self.assertRaisesRegex(self.transport.ShmNativeError, "requires Windows"):
                self.transport.Win32Region(NAME)


class AccountReadTest(BridgeFixture):
    def test_native_asset_values_and_reconciliation_are_kept(self):
        self.query.return_value = [account_row()]
        response = self.roundtrip(self.client(), "get_asset")
        self.assertTrue(response["ok"])
        asset = response["data"]["asset"]
        self.assertEqual(asset["cash"], 3601968.16)
        self.assertEqual(asset["total_asset"], 17879634.87)
        self.assertEqual(asset["frozen_cash"], 0.0)
        self.assertEqual(asset["market_value"], 14292546.310000047)
        evidence = response["data"]["source_evidence"]
        self.assertEqual(evidence["field_sources"]["frozen_cash"], "m_dFrozenCash")
        self.assertEqual(evidence["reconciliation"]["status"], "MISMATCH")
        self.assertAlmostEqual(evidence["reconciliation"]["total_minus_components"], -14879.60, places=2)
        self.query.assert_called_once_with(ACCOUNT, "STOCK", "ACCOUNT")

    def test_missing_frozen_cash_stays_unknown(self):
        self.query.return_value = [account_row(m_dFrozenCash=None)]
        response = self.process("get_asset")
        self.assertFalse(response["ok"])
        self.assertIsNone(response["data"]["asset"]["frozen_cash"])
        self.assertEqual(response["server_error"], "ACCOUNT_CONVERSION_INCOMPLETE")
        self.assertEqual(response["data"]["source_evidence"]["reconciliation"]["status"], "UNKNOWN")

    def test_asset_finite_alias_and_zero_have_distinct_meanings(self):
        self.query.return_value = [account_row(m_dInstrumentValue=float("inf"), m_dMarketValue=0.0,
                                               m_dAvailable=0.0, m_dBalance=0.0)]
        response = self.process("get_asset")
        self.assertTrue(response["ok"])
        self.assertEqual(response["data"]["asset"]["market_value"], 0.0)
        self.assertEqual(response["data"]["source_evidence"]["reconciliation"]["status"], "MATCH")

    def test_zero_total_with_nonzero_components_is_not_complete_or_reconstructed(self):
        cases = [
            dict(m_dAvailable=772.5, m_dFrozenCash=0, m_dInstrumentValue=1422341.90),
            dict(m_dAvailable=1, m_dFrozenCash=0, m_dInstrumentValue=0),
            dict(m_dAvailable=0, m_dFrozenCash=1, m_dInstrumentValue=0),
            dict(m_dAvailable=0, m_dFrozenCash=0, m_dInstrumentValue=1),
        ]
        for fields in cases:
            with self.subTest(fields=fields):
                self.query.return_value = [account_row(m_dBalance=0, m_dAsset=1423114.4, **fields)]
                response = self.process("get_asset")
                self.assertFalse(response["ok"])
                self.assertEqual(response["server_error"], "ACCOUNT_TOTAL_ASSET_ZERO_CONFLICT")
                self.assertEqual(response["data"]["asset"]["total_asset"], 0)
                evidence = response["data"]["source_evidence"]
                self.assertFalse(evidence["complete"])
                self.assertEqual(evidence["status"], "PARTIAL")
                self.assertEqual(evidence["field_sources"]["total_asset"], "m_dBalance")
                self.assertEqual(evidence["reconciliation"]["status"], "MISMATCH")

    def test_zero_total_with_only_rounding_difference_stays_valid(self):
        self.query.return_value = [account_row(m_dBalance=0, m_dAvailable=0.01,
                                               m_dFrozenCash=0, m_dInstrumentValue=0)]
        response = self.process("get_asset")
        self.assertTrue(response["ok"])
        self.assertTrue(response["data"]["source_evidence"]["complete"])
        self.assertEqual(response["data"]["source_evidence"]["reconciliation"]["status"], "MATCH")

    def test_empty_or_multiple_asset_rows_do_not_select_an_arbitrary_row(self):
        for rows, code in (([], "ACCOUNT_ROWS_UNAVAILABLE"), ([account_row(), account_row()], "ACCOUNT_ROWS_AMBIGUOUS")):
            with self.subTest(code=code):
                self.query.return_value = rows
                response = self.process("get_asset")
                self.assertFalse(response["ok"])
                self.assertIsNone(response["data"]["asset"])
                self.assertEqual(response["server_error"], code)

    def test_native_empty_order_and_deal_are_complete_single_queries(self):
        self.query.return_value = []
        for method, key, kind in (("query_orders", "orders", "ORDER"), ("query_trades", "trades", "DEAL")):
            self.query.reset_mock()
            response = self.roundtrip(self.client(), method)
            self.assertTrue(response["ok"])
            self.assertEqual(response["data"][key], [])
            evidence = response["data"]["source_evidence"]
            self.assertTrue(evidence["complete"])
            self.assertEqual(evidence["status"], "EMPTY")
            self.assertEqual(evidence["raw_row_count"], 0)
            self.query.assert_called_once_with(ACCOUNT, "STOCK", kind, "")

    def test_query_failure_never_becomes_empty_records(self):
        self.query.side_effect = RuntimeError("source offline")
        for method, key, kind in (("get_asset", "asset", "ACCOUNT"), ("query_orders", "orders", "ORDER"), ("query_trades", "trades", "DEAL")):
            response = self.process(method)
            self.assertFalse(response["ok"])
            self.assertIsNone(response["data"][key])
            evidence = response["data"]["source_evidence"]
            self.assertEqual(evidence["status"], "FAILED")
            self.assertIsNone(evidence["raw_row_count"])
            self.assertEqual(response["server_error"], kind + "_QUERY_FAILED")

    def test_missing_or_invalid_native_rows_are_not_valid_empty_results(self):
        for rows in (None, {}, "", False):
            self.query.return_value = rows
            for method, key in (("get_asset", "asset"), ("query_orders", "orders"), ("query_trades", "trades")):
                response = self.process(method)
                self.assertFalse(response["ok"])
                self.assertIsNone(response["data"][key])

    def test_payload_and_native_accounts_are_both_isolated(self):
        for method, row in (("get_asset", account_row(m_strAccountID="OTHER")),
                            ("query_orders", order_row(m_strAccountID="OTHER")),
                            ("query_trades", trade_row(m_strAccountID="OTHER"))):
            self.query.return_value = [row]
            self.query.reset_mock()
            self.assertEqual(self.process(method, params={"account_id": "OTHER"})["server_error"], "ACCOUNT_MISMATCH")
            self.query.assert_not_called()
            response = self.process(method)
            self.assertFalse(response["ok"])
            self.assertNotIn("OTHER", str(response["data"]))

    def test_query_params_are_validated_before_native_calls(self):
        for method in ("query_orders", "query_trades"):
            for params in ({"strategy_name": None}, {"strategy_name": False}, {"strategy_name": "x" * 129},
                           {"strategy_name": "bad\nname"}, {"cancelable_only": True}, {"history_date": "20261013"}):
                self.assertEqual(self.process(method, params=params)["server_error"], "INVALID_PARAMS")
        self.assertEqual(self.process("get_asset", params={"strategy_name": "x"})["server_error"], "INVALID_PARAMS")
        self.query.assert_not_called()

    def test_order_fixture_preserves_identity_sell_status_and_china_time(self):
        self.query.return_value = [order_row(m_strCancelInfo="拒单原因", m_strErrorMsg="较泛信息")]
        response = self.roundtrip(self.client(), "query_orders")
        self.assertTrue(response["ok"])
        row = response["data"]["orders"][0]
        self.assertEqual((row["stock_code"], row["order_sys_id"], row["user_order_id"]), ("000001.SZ", "ORDER-1", "remark-1"))
        self.assertEqual((row["action"], row["order_type"], row["status"]), ("SELL", 24, "55"))
        self.assertEqual(row["status_msg"], "拒单原因")
        self.assertEqual(row["order_time"], 1791941415)
        self.assertEqual(row["op_type"], 2147483647)

    def test_trade_fixture_keeps_amount_time_and_order_link(self):
        self.query.return_value = [trade_row()]
        response = self.roundtrip(self.client(), "query_trades", params={"strategy_name": "策略一"})
        self.assertTrue(response["ok"])
        row = response["data"]["trades"][0]
        self.assertEqual((row["trade_id"], row["order_sys_id"], row["amount"]), ("DEAL-1", "ORDER-1", 232.2))
        self.assertEqual(row["traded_time"], 1791941416)
        self.assertEqual(row["strategy_name"], "策略一")
        self.query.assert_called_once_with(ACCOUNT, "STOCK", "DEAL", "策略一")

    def test_direction_unknown_never_defaults_to_sell(self):
        for method, factory, key in (("query_orders", order_row, "orders"), ("query_trades", trade_row, "trades")):
            for direction in (None, True, 2147483647, 51, "48"):
                self.query.return_value = [factory(m_nOffsetFlag=direction)]
                response = self.process(method)
                self.assertFalse(response["ok"])
                self.assertEqual(response["data"][key][0]["action"], "UNKNOWN")
                self.assertEqual(response["data"]["source_evidence"]["status"], "PARTIAL")

    def test_invalid_quantities_prices_and_missing_identity_are_counted(self):
        for field, bad in (("m_nVolumeTotalOriginal", 1.5), ("m_nVolumeTraded", 101),
                           ("m_dLimitPrice", float("nan")), ("m_strOrderSysID", "")):
            self.query.return_value = [order_row(), order_row(**{field: bad})]
            response = self.process("query_orders")
            self.assertFalse(response["ok"])
            evidence = response["data"]["source_evidence"]
            self.assertEqual((evidence["raw_row_count"], evidence["returned_row_count"], evidence["skipped_row_count"]), (2, 1, 1))

    def test_duplicate_records_are_partial_without_silent_overwrite(self):
        for method, factory in (("query_orders", order_row), ("query_trades", trade_row)):
            self.query.return_value = [factory(), factory()]
            response = self.process(method)
            self.assertFalse(response["ok"])
            evidence = response["data"]["source_evidence"]
            self.assertEqual((evidence["raw_row_count"], evidence["converted_row_count"], evidence["returned_row_count"], evidence["duplicate_row_count"]), (2, 2, 1, 1))

    def test_unknown_status_and_time_are_explicit(self):
        self.query.return_value = [order_row(m_nOrderStatus=None, m_strInsertTime="", m_strInsertDate="")]
        response = self.process("query_orders")
        self.assertFalse(response["ok"])
        row = response["data"]["orders"][0]
        self.assertEqual((row["status"], row["order_time"]), ("UNKNOWN", 0))
        self.query.return_value = [trade_row(m_strTradeTime="25:00:00")]
        response = self.process("query_trades")
        self.assertFalse(response["ok"])
        self.assertEqual(response["data"]["trades"][0]["traded_time"], 0)

    def test_strategy_filter_mismatch_is_not_relabelled(self):
        for method, factory in (("query_orders", order_row), ("query_trades", trade_row)):
            self.query.return_value = [factory(m_strStrategyName="other")]
            response = self.process(method, params={"strategy_name": "requested"})
            self.assertFalse(response["ok"])
            self.assertEqual(response["data"]["source_evidence"]["row_errors"][0]["error_code"], "ROW_STRATEGY_MISMATCH")

    def test_incomplete_asset_number_does_not_turn_into_zero(self):
        for bad in (None, True, float("inf"), float("nan"), -1.0, "invalid"):
            self.query.return_value = [account_row(m_dAvailable=bad)]
            response = self.process("get_asset")
            self.assertFalse(response["ok"])
            self.assertIsNone(response["data"]["asset"]["cash"])

    def test_new_reads_work_without_file_or_network_access(self):
        self.query.side_effect = lambda account, kind, detail, *args: {
            "ACCOUNT": [account_row()], "ORDER": [order_row()], "DEAL": [trade_row()],
        }[detail]
        with mock.patch.object(builtins, "open", side_effect=PermissionError("file denied")), mock.patch.object(socket, "socket", side_effect=PermissionError("network denied")):
            for method in ("get_asset", "query_orders", "query_trades"):
                self.assertTrue(self.roundtrip(self.client(), method)["ok"])


if __name__ == "__main__":
    unittest.main()


class MarketRpcTest(BridgeFixture):
    def setUp(self):
        super().setUp()
        self.bridge.market_enabled = True
        self.context.get_local_data = mock.Mock(return_value={20261014145800: {"close": 11.5, "volume": 3}})
        self.context.subscribe_quote = mock.Mock(side_effect=self.subscribe)
        self.context.unsubscribe_quote = mock.Mock(return_value=True)
        self.callback = None

    def subscribe(self, code, **kwargs):
        self.callback = kwargs["callback"]
        return 27

    def history(self, **overrides):
        params = {"code": "000001.SZ", "period": "1m", "start_time": "20261014145800",
                  "end_time": "20261014150000", "count": 2, "divid_type": "none"}
        params.update(overrides)
        return self.process("get_local_data", params=params)

    def begin(self):
        result = self.process("subscribe_quote", params={"code": "000001.SZ"})
        self.assertTrue(result["ok"], result)
        return result["data"]["subscription_id"]

    def tick(self):
        return {"000001.SZ": {"lastPrice": 11.5, "time": 1792040000000, "bidPrice": [11.4]}}

    def test_native_timetag_dict_preserves_rows_and_no_fallback(self):
        result = self.history()["data"]
        self.assertEqual(result["status"], "OK")
        self.assertEqual(result["index"], [20261014145800])
        self.assertEqual(result["columns"], ["close", "volume"])
        self.assertEqual(result["rows"], [[11.5, 3]])
        self.assertEqual(result["native_rows"][0]["fields"], {"close": 11.5, "volume": 3})
        self.context.get_local_data.assert_called_once_with(stock_code="000001.SZ", period="1m", start_time="20261014145800",
                                                           end_time="20261014150000", count=2, divid_type="none")
        self.assertEqual(self.context.calls, [])

    def test_history_empty_unavailable_and_bounds(self):
        self.context.get_local_data.return_value = {}
        self.assertEqual(self.history()["data"]["status"], "EMPTY")
        self.context.get_local_data.return_value = None
        self.assertEqual(self.history()["data"]["status"], "UNAVAILABLE")
        for args in ({"count": 0}, {"count": True}, {"count": 10001}, {"period": "follow"}, {"divid_type": "front"},
                     {"code": "bad"}, {"start_time": "20260230090000"}, {"download": True}):
            self.assertFalse(self.history(**args)["ok"])
        self.context.get_local_data.return_value = {1: {}, 2: {}, 3: {}}
        self.assertEqual(self.history()["server_error"], "HISTORY_SHAPE_INVALID")
        self.context.get_local_data.return_value = {1: 3}
        self.assertEqual(self.history()["server_error"], "HISTORY_SHAPE_INVALID")
        self.context.get_local_data.side_effect = RuntimeError("missing native file")
        self.assertEqual(self.history()["server_error"], "QMT_QUERY_FAILED")

    def test_callback_other_thread_copy_queue_gap_cancel_and_late_delivery(self):
        subscription_id = self.begin()
        raw = self.tick()
        with ThreadPoolExecutor(max_workers=1) as pool:
            pool.submit(self.callback, raw).result()
        raw["000001.SZ"]["bidPrice"][0] = 999
        for _ in range(33):
            self.callback(self.tick())
        result = self.process("poll_quote_events", params={"after_sequence": 0})["data"]
        self.assertEqual(len(result["events"]), 32)
        self.assertEqual(result["latest_sequence"], 34)
        self.assertEqual(result["dropped_count"], 2)
        self.assertTrue(result["gap"])
        self.assertEqual(result["events"][0]["subscription_id"], subscription_id)
        self.assertEqual(result["events"][0]["account_id"], ACCOUNT)
        self.assertEqual(result["events"][0]["native_time"], [1792040000000])
        self.assertEqual(result["delivery"], "poll")
        self.assertEqual(self.process("subscribe_quote", params={"code": "000001.SZ"})["server_error"], "SUBSCRIPTION_ACTIVE")
        self.assertTrue(self.process("unsubscribe_quote", params={"subscription_id": subscription_id})["data"]["cancelled"])
        self.callback(self.tick())
        self.assertEqual(self.bridge.event_sequence, 34)
        self.context.unsubscribe_quote.assert_called_once_with(27)

    def test_first_subscription_reports_zero_sequence_before_native_registration(self):
        def subscribe(code, **kwargs):
            self.assertTrue(self.bridge.event_lock.acquire(False))
            self.bridge.event_lock.release()
            self.assertEqual(self.bridge.subscription["initial_sequence"], 0)
            return self.subscribe(code, **kwargs)

        self.context.subscribe_quote.side_effect = subscribe
        result = self.process("subscribe_quote", params={"code": "000001.SZ"})["data"]
        self.assertIs(type(result["initial_sequence"]), int)
        self.assertEqual(result["initial_sequence"], 0)

    def test_same_session_resubscription_keeps_real_start_after_old_queue_overflow(self):
        old_id = self.begin()
        old_callback = self.callback
        for _ in range(101):
            self.callback(self.tick())
        self.assertTrue(self.process("unsubscribe_quote", params={"subscription_id": old_id})["data"]["cancelled"])
        subscription = self.process("subscribe_quote", params={"code": "000001.SZ"})["data"]
        self.assertEqual(subscription["initial_sequence"], 101)
        self.assertIs(type(subscription["initial_sequence"]), int)
        self.assertNotEqual(subscription["subscription_id"], old_id)
        self.assertEqual(subscription["session_id"], self.mailbox.session_id)
        old_callback(self.tick())
        self.assertEqual(self.bridge.event_sequence, 101)
        self.callback(self.tick())
        result = self.process("poll_quote_events", params={"after_sequence": subscription["initial_sequence"]})["data"]
        self.assertFalse(result["gap"])
        self.assertEqual([event["sequence"] for event in result["events"]], [102])
        self.assertEqual(result["events"][0]["subscription_id"], subscription["subscription_id"])
        self.assertTrue(self.process("unsubscribe_quote", params={"subscription_id": subscription["subscription_id"]})["data"]["cancelled"])
        self.callback(self.tick())
        self.assertEqual(self.bridge.event_sequence, 102)
        self.assertEqual(self.context.unsubscribe_quote.call_count, 2)

    def test_synchronous_native_callback_first_event_is_not_skipped(self):
        self.bridge.event_sequence = 101
        def subscribe(code, **kwargs):
            self.assertEqual(self.bridge.subscription["initial_sequence"], 101)
            self.subscribe(code, **kwargs)
            kwargs["callback"](self.tick())
            return 27

        self.context.subscribe_quote.side_effect = subscribe
        subscription = self.process("subscribe_quote", params={"code": "000001.SZ"})["data"]
        self.assertEqual(subscription["initial_sequence"], 101)
        self.assertEqual(self.bridge.event_sequence, 102)
        result = self.process("poll_quote_events", params={"after_sequence": subscription["initial_sequence"]})["data"]
        self.assertEqual([event["sequence"] for event in result["events"]], [102])
        self.assertFalse(result["gap"])

    def test_real_new_subscription_overflow_still_reports_gap(self):
        self.bridge.event_sequence = 101
        subscription = self.process("subscribe_quote", params={"code": "000001.SZ"})["data"]
        for _ in range(33):
            self.callback(self.tick())
        result = self.process("poll_quote_events", params={"after_sequence": subscription["initial_sequence"]})["data"]
        self.assertTrue(result["gap"])
        self.assertEqual(result["oldest_sequence"], 103)
        self.assertEqual(result["dropped_count"], 1)
        self.assertEqual(len(result["events"]), 32)

    def test_callback_copy_and_invalid_payload(self):
        self.begin()
        raw = self.tick()
        self.callback(raw)
        raw["000001.SZ"]["bidPrice"][0] = 999
        self.assertEqual(self.bridge.events[0]["data"]["000001.SZ"]["bidPrice"], [11.4])
        self.callback({"600000.SH": {"lastPrice": 1, "time": 1}})
        self.assertEqual(self.bridge.event_sequence, 1)
        self.assertIsNotNone(self.bridge.callback_error)
        self.assertFalse(self.process("poll_quote_events", params={"after_sequence": 2})["ok"])

    def test_close_cancel_failure_still_releases_mailbox(self):
        self.begin()
        self.context.unsubscribe_quote.return_value = False
        with self.assertRaisesRegex(ValueError, "native unsubscribe"):
            self.bridge.close("deadline")
        self.assertTrue(self.mailbox.closed)
        self.assertIn("CLEANUP_FAILED", self.output.getvalue())
        self.callback(self.tick())
        self.assertEqual(self.bridge.event_sequence, 0)
        with self.assertRaisesRegex(ValueError, "native unsubscribe"):
            self.bridge.close("reload")
        self.context.unsubscribe_quote.assert_called_once_with(27)
        self.bridge = None  # 已验证并处理的清理失败，不由通用tearDown再次触发。

    def test_dataframe_axes_and_sparse_dict_fields_preserved(self):
        self.context.get_local_data.return_value = SimpleNamespace(
            shape=(1, 2), index=[20261014145900], columns=["close", "open"], values=SimpleNamespace(tolist=lambda: [[11.5, 11.4]]))
        data = self.history()["data"]
        self.assertEqual(data["index"], [20261014145900])
        self.assertEqual(data["columns"], ["close", "open"])
        self.assertEqual(data["rows"], [[11.5, 11.4]])
        self.context.get_local_data.return_value = {1: {"close": 1}, 2: {"volume": 2}}
        data = self.history()["data"]
        self.assertEqual(data["rows"], [[1, None], [None, 2]])
        self.assertEqual(data["native_rows"], [{"index": 1, "fields": {"close": 1}}, {"index": 2, "fields": {"volume": 2}}])

    def test_oversized_callbacks_are_rejected_without_advancing_sequence(self):
        self.begin()
        raw = self.tick()
        raw["000001.SZ"]["oversized"] = "x" * 3000
        self.callback(raw)
        self.assertEqual(self.bridge.event_sequence, 0)
        self.assertEqual(self.bridge.events, [])
        self.assertIn("2048-byte", self.bridge.callback_error)
        self.callback(self.tick())
        self.assertEqual(self.bridge.event_sequence, 1)

    def test_subscribe_native_failure_closes_gate_and_own_subscription_only(self):
        self.context.subscribe_quote.side_effect = RuntimeError("native refused")
        result = self.process("subscribe_quote", params={"code": "000001.SZ"})
        self.assertFalse(result["ok"])
        self.assertIsNone(self.bridge.subscription)
        self.context.unsubscribe_quote.assert_not_called()
        self.bridge.close()
        self.context.unsubscribe_quote.assert_not_called()
