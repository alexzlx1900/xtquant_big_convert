# coding: utf-8
"""显式具名全集与单次读取分开验证；原生API和Win32仅使用本地替身。"""

import json
import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from unittest import mock

import pytest

import test_dual_shm_single_file as DUAL
from bigqmt_signal_trader.transports.readonly_shm import ShmProtocolError
from bigqmt_signal_trader.transports.readonly_shm_client import ShmReadOnlyRpcClient


def named_symbols(count, start=300000):
    return ["%06d.SZ" % number for number in range(start, start + count)]


def native_quotes(codes):
    return {code: dict(lastPrice=10 + int(code[:6]) / 100000.0, time=1792110000000 + int(code[:6])) for code in codes}


class QuoteFixture(DUAL.Fixture):
    def __init__(self, monkeypatch, codes, *, live=True, source_text=None, **changes):
        values = DUAL.live_settings(symbols=list(codes)) if live else dict(symbols=list(codes))
        values.update(changes)
        if source_text is None:
            super().__init__(monkeypatch, **values)
        else:
            with mock.patch.object(DUAL.BUILDER, "build_source", return_value=source_text):
                super().__init__(monkeypatch, **values)
        self.quote_calls = []

        def quotes(requested):
            self.quote_calls.append(list(requested))
            return native_quotes(requested)

        self.context.get_full_tick = quotes


def drive(harness, operation):
    with ThreadPoolExecutor(1) as pool:
        future = pool.submit(operation)
        deadline = time.monotonic() + 2
        while not future.done() and time.monotonic() < deadline:
            harness.namespace["aqp_shm_readonly_adjust"](harness.context)
            time.sleep(0.001)
        return future.result(timeout=0.2)


@pytest.mark.parametrize("method", ["get_ticks", "get_full_tick"])
def test_live_400_members_and_benchmark_outside_initial_symbols_read_in_bounded_batches(monkeypatch, method):
    codes = named_symbols(400) + ["000300.SH"]
    harness = QuoteFixture(monkeypatch, ["600000.SH"])
    try:
        harness.start()
        combined = {}
        for offset in range(0, len(codes), 100):
            batch = codes[offset:offset + 100]
            response = harness.call("read", method, {"codes": batch})
            assert response["ok"] is True and response["environment"] == "LIVE"
            assert response["data"] == native_quotes(batch)
            combined.update(response["data"])
        assert harness.quote_calls == [codes[offset:offset + 100] for offset in range(0, len(codes), 100)]
        assert set(combined) == set(codes) and len(combined) == 401
        assert harness.query_calls == []
    finally:
        harness.close()


@pytest.mark.parametrize("params", [
    {"codes": named_symbols(129)}, {"codes": []},
    {"codes": ["300000.SZ", "300000.SZ"]}, {"codes": [None]}, {"codes": ["*.SZ"]},
])
def test_large_universe_rejects_oversized_duplicate_or_invalid_requests(monkeypatch, params):
    harness = QuoteFixture(monkeypatch, named_symbols(400))
    try:
        harness.start()
        response = harness.call("read", "get_ticks", params)
        assert response["ok"] is False and response["server_error"] == "INVALID_PARAMS"
        assert harness.quote_calls == []
        assert harness.query_calls == []
    finally:
        harness.close()


@pytest.mark.parametrize("codes", [[], named_symbols(1025), ["300000.SZ"] * 2, ["*.SZ"], ["bad.SZ"], [None]])
def test_live_constructor_requires_a_bounded_unique_explicit_named_universe(codes):
    with pytest.raises(ValueError):
        DUAL.BUILDER.build_source(**DUAL.settings(**DUAL.live_settings(symbols=codes)))


@pytest.mark.parametrize("count", [128, 1024])
def test_live_legacy_128_and_full_1024_universe_keep_the_same_single_request_limit(monkeypatch, count):
    codes = named_symbols(count)
    harness = QuoteFixture(monkeypatch, codes)
    try:
        harness.start()
        assert harness.call("read", "get_ticks", {"codes": codes[:128]})["ok"] is True
        assert harness.quote_calls == [codes[:128]]
        oversized = codes[:129] if count > 128 else codes + ["999999.BJ"]
        assert harness.call("read", "get_ticks", {"codes": oversized})["server_error"] == "INVALID_PARAMS"
        assert harness.quote_calls == [codes[:128]]
    finally:
        harness.close()


def test_actual_cli_output_accepts_400_names_and_executes_a_bounded_live_read(monkeypatch, tmp_path):
    codes = named_symbols(400)
    output = tmp_path / "dual-400.py"
    names = dict(read_name=r"Local\AQP_SHM_READONLY_UNIVERSE_CLI", execution_name=r"Local\AQP_SHM_TRADE_UNIVERSE_CLI")
    command = [sys.executable, str(DUAL.ROOT / "tools/build_dual_shm_single_file.py"), "--output", str(output),
        "--account-id", "8765432100", "--environment", "LIVE", "--persistent-live-read-write",
        "--native-pid", str(os.getpid()), "--windows-session", "1", "--max-seconds", "0", "--max-requests", "0",
        "--enable-market-rpc", "--read-name", names["read_name"], "--execution-name", names["execution_name"],
        "--symbols", ",".join(codes)]
    built = subprocess.run(command, capture_output=True, text=True, timeout=20)
    assert built.returncode == 0, built.stderr
    assert "no native process started" in built.stdout and output.exists()
    harness = QuoteFixture(monkeypatch, codes, source_text=output.read_text(encoding="utf-8"), **names)
    try:
        harness.start()
        assert harness.call("read", "get_ticks", {"codes": codes[:100]})["data"] == native_quotes(codes[:100])
        assert harness.quote_calls == [codes[:100]]
    finally:
        harness.close()


def test_ping_and_both_ready_metadata_report_actual_universe_count_and_request_limit(monkeypatch, capsys):
    codes = named_symbols(400)
    harness = QuoteFixture(monkeypatch, codes)
    try:
        harness.start()
        lines = capsys.readouterr().out.splitlines()
        single = json.loads(next(line.split(" ", 1)[1] for line in lines if line.startswith("AQP_SHM_READONLY_READY ")))
        dual = json.loads(next(line.split(" ", 1)[1] for line in lines if line.startswith("AQP_DUAL_SHM_READY ")))
        ping = harness.call("read", "ping")["data"]
        for value in (single, dual, ping):
            assert value["quote_initial_symbol_count"] == 400
            assert value["quote_symbol_scope"] == "EXPLICIT_REQUEST_SYMBOLS"
            assert "quote_allowed_symbol_count" not in value
            assert value["quote_request_symbol_limit"] == 128
        assert single["symbols"] == codes
        assert single["rpc_revision"] == ping["rpc_revision"]
        assert dual["native_pid"] == ping["native_pid"] == os.getpid()
        assert harness.quote_calls == []
    finally:
        harness.close()


@pytest.mark.parametrize("fault", ["missing", "invalid", "unexpected"])
def test_partial_native_quote_evidence_is_not_zero_filled_or_promoted_to_complete(monkeypatch, fault):
    harness = QuoteFixture(monkeypatch, named_symbols(400))
    requested = named_symbols(100)

    def incomplete(codes):
        harness.quote_calls.append(list(codes))
        result = native_quotes(codes)
        if fault == "missing":
            result.pop(codes[-1])
        elif fault == "invalid":
            result[codes[-1]]["lastPrice"] = 0
        else:
            result["999999.BJ"] = dict(lastPrice=20, time=1792110000000)
        return result

    harness.context.get_full_tick = incomplete
    try:
        harness.start()
        response = harness.call("read", "get_ticks", {"codes": requested})
        assert response["ok"] is False and response["server_error"] == "QMT_QUOTE_UNAVAILABLE"
        data = response["data"]
        assert data["requested_symbols"] == requested and data["source_evidence"]["complete"] is False
        assert data["source_evidence"]["environment"] == "LIVE"
        if fault == "missing":
            assert data["missing_symbols"] == requested[-1:] and requested[-1] not in data["quotes"]
        elif fault == "invalid":
            assert data["invalid_symbols"] == requested[-1:] and data["quotes"][requested[-1]]["lastPrice"] == 0
        else:
            assert data["unexpected_symbols"] == ["999999.BJ"]
        assert harness.quote_calls == [requested]
    finally:
        harness.close()


def test_account_asset_and_positions_are_independent_of_the_quote_universe(monkeypatch):
    codes = named_symbols(400)
    assert DUAL.CODE not in codes
    harness = QuoteFixture(monkeypatch, codes)
    try:
        harness.start()
        asset = harness.call("read", "get_asset", {"include_source_evidence": True})
        positions = harness.call("read", "get_positions", {"include_source_evidence": True})
        assert asset["ok"] and asset["data"]["asset"]["total_asset"] == 2147
        assert asset["data"]["source_evidence"]["complete"] is True
        assert positions["ok"] and set(positions["data"]["positions"]) == {DUAL.CODE}
        assert positions["data"]["positions"][DUAL.CODE]["volume"] == 100
        assert positions["data"]["source_evidence"]["complete"] is True
        assert harness.quote_calls == []
    finally:
        harness.close()


@pytest.mark.parametrize("method", ["get_local_data", "get_market_data_ex"])
def test_explicit_cached_history_outside_quote_names_keeps_its_independent_301_row_contract(monkeypatch, method):
    harness = QuoteFixture(monkeypatch, named_symbols(400))
    code = "999998.BJ"
    first = datetime(2026, 10, 20, 9, 30)
    rows = {int((first + timedelta(minutes=number)).strftime("%Y%m%d%H%M%S")): dict(open=10.0, high=10.3, low=9.9, close=10.25,
            volume=number + 1, amount=10.25 * (number + 1)) for number in range(301)}
    native = mock.Mock(return_value=rows if method == "get_local_data" else {code: rows})
    setattr(harness.context, method, native)
    params = dict(code=code, period="1m", start_time="", end_time="", count=301)
    if method == "get_local_data":
        params.update(divid_type="none")
    else:
        params.update(dividend_type="none", fill_data=False, subscribe=False)
    try:
        harness.start()
        response = harness.call("read", method, params)
        assert response["ok"], response
        assert response["data"]["status"] == "OK"
        assert len(response["data"]["rows"]) == 301
        assert response["data"]["source_evidence"]["symbol_scope"] == "EXPLICIT_HISTORY_SYMBOL"
        assert response["data"]["source_evidence"]["download_performed"] is False
        native.assert_called_once()
        assert harness.quote_calls == []
    finally:
        harness.close()


def test_sim_universe_and_high_level_wrapper_remain_capped_at_64(monkeypatch):
    with pytest.raises(ValueError):
        DUAL.BUILDER.build_source(**DUAL.settings(symbols=named_symbols(65)))
    codes = named_symbols(64)
    harness = QuoteFixture(monkeypatch, codes, live=False)
    client = None
    try:
        harness.start()
        client = ShmReadOnlyRpcClient(name=harness.settings["read_name"], account_id=harness.account,
            client_factory=lambda name, account: harness.clients["read"])
        drive(harness, lambda: client.verify_identity(expected_session_id=harness.clients["read"].mailbox.session_id,
                                                      expected_native_pid=os.getpid()))
        assert drive(harness, lambda: client.call("get_ticks", {"codes": codes})) == native_quotes(codes)
        with pytest.raises(ShmProtocolError):
            client.call("get_ticks", {"codes": codes + ["999999.BJ"]})
        assert harness.quote_calls == [codes]
        ping = harness.call("read", "ping")["data"]
        assert ping["quote_initial_symbol_count"] == ping["quote_request_symbol_limit"] == 64
        assert ping["quote_symbol_scope"] == "EXPLICIT_REQUEST_SYMBOLS"
    finally:
        if client is not None:
            client.close()
        harness.close()
