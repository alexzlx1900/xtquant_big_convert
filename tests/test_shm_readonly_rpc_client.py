"""Gateway 客户端与现有独立桥的本地协议互通验收。"""

import importlib.util
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest import mock

import pytest


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from bigqmt_signal_trader.transports.readonly_shm import (  # noqa: E402
    ShmBusyError, ShmProtocolError, ShmReadOnlyClient, ShmTimeoutError,
)
from bigqmt_signal_trader.transports.readonly_shm_client import (  # noqa: E402
    ShmReadOnlyRpcClient, ShmReadOnlyRpcError,
)


SPEC = importlib.util.spec_from_file_location("shm_rpc_fixture", ROOT / "tests/test_shm_readonly.py")
FIXTURE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(FIXTURE)


class RpcWireTest(FIXTURE.BridgeFixture):
    quote_symbols = ("000001.SZ", "600000.SH")

    def make_rpc(self):
        def native_factory(name, account):
            client = ShmReadOnlyClient(name, account, region=FIXTURE.MemoryRegion(self.state))
            self.clients.append(client)
            return client

        self.factory = mock.Mock(side_effect=native_factory)
        return ShmReadOnlyRpcClient(
            name=FIXTURE.NAME, account_id=FIXTURE.ACCOUNT, timeout_seconds=1,
            client_factory=self.factory,
        )

    def rpc_roundtrip(self, client, method, params=None):
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(client.call, method, params)
            deadline = time.monotonic() + 2
            while not future.done() and time.monotonic() < deadline:
                self.bridge.drain()
                time.sleep(0.001)
            return future.result(timeout=0.2)

    def test_account_queries_keep_native_evidence_and_mismatch(self):
        rows = {
            "ACCOUNT": [FIXTURE.account_row()], "POSITION": [FIXTURE.position_row()],
            "ORDER": [FIXTURE.order_row()], "DEAL": [FIXTURE.trade_row()],
        }
        self.query.side_effect = lambda account, kind, detail, *args: rows[detail]
        client = self.make_rpc()
        assert self.rpc_roundtrip(client, "ping")["allow_order_methods"] is False
        asset = self.rpc_roundtrip(client, "get_asset")
        assert asset["asset"]["cash"] == 3601968.16
        assert asset["source_evidence"]["reconciliation"]["status"] == "MISMATCH"
        positions = self.rpc_roundtrip(client, "get_positions", {"include_source_evidence": True})
        assert positions["source_evidence"]["returned_row_count"] == 1
        orders = self.rpc_roundtrip(client, "query_orders")
        trades = self.rpc_roundtrip(client, "query_trades")
        assert orders["orders"][0]["order_sys_id"] == "ORDER-1"
        assert trades["trades"][0]["trade_id"] == "DEAL-1"
        assert orders["source_evidence"]["complete"] is True
        assert trades["source_evidence"]["complete"] is True
        self.factory.assert_called_once()
        client.close()
        client.close()
        assert client._client.mailbox.closed

    def test_successful_empty_records_keep_empty_evidence(self):
        self.query.return_value = []
        client = self.make_rpc()
        for method, key in (("query_orders", "orders"), ("query_trades", "trades")):
            result = self.rpc_roundtrip(client, method)
            assert result[key] == []
            assert result["source_evidence"]["status"] == "EMPTY"
            assert result["source_evidence"]["raw_row_count"] == 0

    def test_multi_quotes_are_one_request_with_native_prices_and_times(self):
        client = self.make_rpc()
        result = self.rpc_roundtrip(client, "get_ticks", {"codes": list(self.quote_symbols)})
        assert result["000001.SZ"] == {"lastPrice": 11.57, "time": 1791874800000}
        assert result["600000.SH"] == {"lastPrice": 9.6, "time": 1791874801000}
        assert self.context.calls == [list(self.quote_symbols)] and self.bridge.request_count == 1
        self.query.assert_not_called()

    def test_quote_failure_keeps_partial_results_without_retry(self):
        self.context.get_full_tick = mock.Mock(return_value={"000001.SZ": {"lastPrice": 11.57, "time": 1791874800000}})
        with pytest.raises(ShmReadOnlyRpcError) as caught:
            self.rpc_roundtrip(self.make_rpc(), "get_full_tick", {"codes": list(self.quote_symbols)})
        assert caught.value.server_error == "QMT_QUOTE_UNAVAILABLE"
        assert caught.value.data["missing_symbols"] == ["600000.SH"]
        assert caught.value.data["quotes"]["000001.SZ"]["time"] == 1791874800000
        self.context.get_full_tick.assert_called_once_with(list(self.quote_symbols))
        assert self.bridge.request_count == 1

    def test_source_failure_keeps_error_and_unavailable_records(self):
        self.query.side_effect = RuntimeError("native query unavailable")
        client = self.make_rpc()
        with pytest.raises(ShmReadOnlyRpcError) as caught:
            self.rpc_roundtrip(client, "query_orders")
        error = caught.value
        assert error.server_error == "ORDER_QUERY_FAILED"
        assert error.data["orders"] is None
        assert error.source_evidence["status"] == "FAILED"
        assert error.response["ok"] is False

    def test_partial_conversion_keeps_unknown_direction_and_raises(self):
        self.query.return_value = [FIXTURE.order_row(m_nOffsetFlag=999)]
        client = self.make_rpc()
        with pytest.raises(ShmReadOnlyRpcError) as caught:
            self.rpc_roundtrip(client, "query_orders")
        assert caught.value.data["orders"][0]["action"] == "UNKNOWN"
        assert caught.value.source_evidence["status"] == "PARTIAL"
        assert caught.value.source_evidence["complete"] is False

    def test_timeout_does_not_reopen_or_resubmit(self):
        client = self.make_rpc()
        with pytest.raises(ShmTimeoutError):
            client.call("ping", timeout_seconds=0.03)
        pending = self.mailbox.read_request()
        with pytest.raises(ShmProtocolError, match="unconfirmed"):
            client.call("ping", timeout_seconds=0.03)
        assert self.mailbox.read_request() == pending
        self.factory.assert_called_once()
        assert self.query.call_count == 0

    def test_concurrent_query_is_rejected_without_second_wire_request(self):
        client = self.make_rpc()
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(client.call, "ping")
            deadline = time.monotonic() + 1
            while self.mailbox.read_request() is None and time.monotonic() < deadline:
                time.sleep(0.001)
            with pytest.raises(ShmBusyError):
                client.call("get_asset")
            self.bridge.drain()
            assert future.result(timeout=1)["pong"] is True
        assert self.bridge.request_count == 1


@pytest.mark.parametrize("method", ["submit_order", "cancel_order", "passorder", "download_history_data2"])
def test_denied_method_does_not_open_mailbox(method):
    factory = mock.Mock()
    client = ShmReadOnlyRpcClient(name=FIXTURE.NAME, account_id=FIXTURE.ACCOUNT, client_factory=factory)
    with pytest.raises(ShmProtocolError, match="readonly allowlist"):
        client.call(method)
    factory.assert_not_called()


@pytest.mark.parametrize("kwargs", [
    {"account_id": "OTHER"}, {"params": {"account_id": "OTHER"}},
    {"use_formula": True}, {"timeout_seconds": 0},
    {"timeout_seconds": float("nan")}, {"timeout_seconds": 61},
])
def test_invalid_target_or_budget_does_not_open_mailbox(kwargs):
    factory = mock.Mock()
    client = ShmReadOnlyRpcClient(name=FIXTURE.NAME, account_id=FIXTURE.ACCOUNT, client_factory=factory)
    with pytest.raises((ShmProtocolError, ValueError)):
        client.call("ping", **kwargs)
    factory.assert_not_called()


def test_close_before_first_call_does_not_open_mailbox():
    factory = mock.Mock()
    client = ShmReadOnlyRpcClient(name=FIXTURE.NAME, account_id=FIXTURE.ACCOUNT, client_factory=factory)
    client.close()
    with pytest.raises(ShmProtocolError, match="closed"):
        client.call("ping")
    factory.assert_not_called()


@pytest.mark.parametrize("params", [
    {}, {"codes": []}, {"codes": ["000001.SZ"] * 2}, {"codes": ["000001.SZ"] * 65},
    {"codes": [None]}, {"codes": ["０００００１.SZ"]}, {"codes": ["000001.XX"]},
    {"codes": ["000001.SZ"], "code": "000001.SZ"},
    {"codes": ["000001.SZ"], "subscribe": True},
])
def test_invalid_quote_params_do_not_open_mailbox(params):
    factory = mock.Mock()
    client = ShmReadOnlyRpcClient(name=FIXTURE.NAME, account_id=FIXTURE.ACCOUNT, client_factory=factory)
    with pytest.raises(ShmProtocolError):
        client.call("get_ticks", params)
    factory.assert_not_called()


class MarketRpcWireTest(RpcWireTest):
    def setUp(self):
        super().setUp()
        self.bridge.market_enabled = True
        self.context.get_local_data = mock.Mock(return_value={20261014150000: {"close": 11.5}})

    def verify(self, client, **overrides):
        args = {"expected_session_id": self.mailbox.session_id, "expected_native_pid": self.mailbox.server_pid}
        args.update(overrides)
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(client.verify_identity, **args)
            deadline = time.monotonic() + 2
            while not future.done() and time.monotonic() < deadline:
                self.bridge.drain()
                time.sleep(0.001)
            return future.result(timeout=0.2)

    def test_new_capability_requires_explicit_verified_identity(self):
        client = self.make_rpc()
        params = {"code": "000001.SZ", "period": "1m", "start_time": "20261014145800", "end_time": "20261014150000", "count": 1}
        with pytest.raises(ShmProtocolError, match="identity handshake"):
            client.call("get_local_data", params)
        self.factory.assert_not_called()
        assert "get_local_data" in self.verify(client)["capabilities"]
        result = self.rpc_roundtrip(client, "get_local_data", params)
        assert result["status"] == "OK"
        assert result["index"] == [20261014150000]
        client.close()

    def test_identity_mismatch_closes_without_market_rpc(self):
        client = self.make_rpc()
        with pytest.raises(ShmProtocolError, match="identity verification"):
            self.verify(client, expected_native_pid=self.mailbox.server_pid + 1)
        assert client._closed
        self.context.get_local_data.assert_not_called()

    def test_no_automatic_recovery_or_unknown_request_resend(self):
        client = self.make_rpc()
        self.verify(client)
        with pytest.raises(ShmTimeoutError):
            client.call("ping", timeout_seconds=0.03)
        pending = self.mailbox.read_request()
        with pytest.raises(ShmProtocolError, match="unconfirmed"):
            client.call("ping")
        assert self.mailbox.read_request() == pending
        self.factory.assert_called_once()
        client.close()

    def test_legacy_bridge_new_method_not_advertised(self):
        self.bridge.market_enabled = False
        client = self.make_rpc()
        self.verify(client)
        with pytest.raises(ShmProtocolError, match="advertised capability"):
            client.call("poll_quote_events", {"after_sequence": 0})
        client.close()
