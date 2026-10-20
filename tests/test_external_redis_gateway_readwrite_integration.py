# coding: utf-8
"""真实生成源码、外置代理和正式Gateway的本地读写路径；不连接外部服务。"""

import asyncio
from concurrent.futures import ThreadPoolExecutor
import importlib
import json
import os
from pathlib import Path
import sys
import threading
import time
from types import SimpleNamespace

import pytest


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from bigqmt_signal_trader.redis_shm_proxy import RedisShmReadOnlyProxy
from bigqmt_signal_trader.transports.redis_transport import RedisTransport
from bigqmt_signal_trader.transports.readonly_shm_client import ShmReadOnlyRpcClient
from bigqmt_signal_trader.transports.readonly_shm import ShmProtocolError
from test_external_redis_dual_shm_integration import DUAL, MemoryRedis


ACCOUNT = "55013746"
CODE = "000001.SZ"
REMARK = "native-short"


@pytest.fixture
def gateway_modules(monkeypatch):
    location = os.environ.get("AQP_GATEWAY_TEST_ROOT")
    if not location:
        pytest.skip("local cross-repository check requires explicit AQP_GATEWAY_TEST_ROOT")
    root = Path(location).resolve()
    if not (root / "redis_shm_rpc_client.py").is_file():
        raise AssertionError("explicit AQP gateway candidate is unavailable")
    monkeypatch.syspath_prepend(str(root))
    return SimpleNamespace(
        config=importlib.import_module("gateway_config"),
        readonly=importlib.import_module("bigqmt_readonly_adapter"),
        order=importlib.import_module("bigqmt_order_adapter"),
        client=importlib.import_module("redis_shm_rpc_client"),
        multi=importlib.import_module("multi_account_adapter"),
    )


class Harness:
    def __init__(self, monkeypatch, gateway, *, allow_writes=True, unknown_submit=False,
                 live=False, start_write_blocked=None, live_write_authorized=False, symbols=None):
        self.allow_writes, self.unknown_submit = allow_writes, unknown_submit
        self.account = "8882152350" if live else ACCOUNT
        self.environment = "LIVE" if live else "SIM"
        self.profile_id = "live-stock" if live else "sim-stock"
        self.policy = ("PERSISTENT_LIVE_READ_WRITE" if live else
                       "PERSISTENT_SIMULATION_READ_WRITE" if allow_writes else "PERSISTENT_READONLY")
        initial_block = live if start_write_blocked is None else start_write_blocked
        self.source = DUAL.Fixture(
            monkeypatch, hard_deadline_unix=None, max_seconds=0, max_requests=0,
            account_id=self.account, environment=self.environment,
            persistent_readonly=not allow_writes, persistent_live_read_write=live,
            persistent_simulation_read_write=allow_writes and not live,
            enable_market_rpc=allow_writes,
            symbols=[CODE] if symbols is None else symbols,
        )
        self.gateway, self.redis = gateway, MemoryRedis()
        self.orders, self.submit_calls, self.cancel_calls = [], [], []
        original_query = self.source.query

        def query(account, kind, detail, strategy_name=""):
            if detail == "ORDER":
                return self.orders
            if detail == "DEAL":
                return []
            return original_query(account, kind, detail, strategy_name)

        self.source.namespace.update(
            get_trade_detail_data=query, passorder=self.submit, cancel=self.cancel,
        )
        self.source.context.get_market_data_ex = lambda *args, **kwargs: {
            CODE: {"20261015100100": dict(open=11.4, high=11.5, low=11.3, close=11.42,
                                       volume=100, amount=1142.0)}
        }
        self.source.start()
        self.source.namespace["account_callback"](
            self.source.context,
            SimpleNamespace(m_strAccountID=self.account, m_strStatus="登录成功"),
        )
        self.proxy = RedisShmReadOnlyProxy(
            RedisTransport(self.redis, account_id=self.account),
            self.source.clients["read"], self.source.clients["execution"],
            account_id=self.account, environment=self.environment,
            start_write_blocked=initial_block, live_write_authorized=live_write_authorized,
            expected_readonly_session_id=self.source.clients["read"].mailbox.session_id,
            expected_execution_session_id=self.source.clients["execution"].mailbox.session_id,
            expected_native_pid=os.getpid(), expected_windows_session_id=1,
            timeout_seconds=1.0, max_requests=0, deadline_unix=None,
            allow_order_methods=allow_writes,
            runtime_policy=self.policy,
        )
        self.pool = ThreadPoolExecutor(1)
        self.stop = threading.Event()
        self.pump = None
        self.profile = gateway.config.GatewayAccountProfile(
            profile_id=self.profile_id, label="local external fixture", environment=self.environment,
            account_id=self.account, userdata_mini_path="fixture-not-accessed", order_submit_mode="SYNC",
            bigqmt_rpc=gateway.config.BigQmtRpcConfig(
                transport="redis_shm", timeout_seconds=1.0, allow_order_methods=allow_writes,
                expected_rpc_revision="external-redis-dual-shm-v1",
                expected_session_id=self.source.clients["execution"].mailbox.session_id,
                expected_read_session_id=self.source.clients["read"].mailbox.session_id,
                expected_native_pid=os.getpid(), expected_windows_session_id=1,
                expected_source_policy=self.policy,
            ),
        )
        self.source_binding = {
            "read_session_id": self.source.clients["read"].mailbox.session_id,
            "execution_session_id": self.source.clients["execution"].mailbox.session_id,
            "native_pid": os.getpid(), "windows_session_id": 1,
        }
        self.multi = gateway.multi.MultiAccountMiniQmtAdapter(gateway.config.WindowsGatewayLocalConfig(
            gateway_id="qmt-local-test", default_profile_id=self.profile_id, profiles=[self.profile],
        ))

        def client_factory(**kwargs):
            return gateway.client.RedisShmRpcClient(
                account_id=self.account, environment=self.environment,
                expected_source_policy=self.policy, allow_order_methods=allow_writes,
                redis_config={}, timeout_seconds=1.0,
                transport=RedisTransport(self.redis, account_id=self.account),
                expected_source_binding=self.source_binding,
            )

        self.read = gateway.readonly.BigQmtReadOnlyAdapter("qmt-local-test", self.profile, client_factory=client_factory)
        self.order = gateway.order.BigQmtOrderAdapter(
            "qmt-local-test", self.profile, trader_factory=client_factory,
            shared_events=self.multi._shared_events,
        )
        self.multi._bigqmt_read_adapters[self.profile_id] = self.read
        self.multi._bigqmt_order_adapters[self.profile_id] = self.order

    def submit(self, *args):
        self.submit_calls.append(args)
        if self.unknown_submit:
            raise RuntimeError("fixture native submission outcome is unknown")
        row = SimpleNamespace(
            m_strAccountID=self.account, m_strOrderSysID="7620", m_strInstrumentID="000001",
            m_strExchangeID="SZ", m_strRemark=REMARK, m_strStrategyName="",
            m_nOffsetFlag=48, m_nOpType=23, m_nOrderStatus=50,
            m_nVolumeTotalOriginal=100, m_nVolumeTraded=0, m_dLimitPrice=11.42,
            m_dTradedPrice=0.0, m_nOrderPriceType=50,
            m_strInsertDate="20261016", m_strInsertTime="100000",
        )
        self.orders.append(row)
        self.source.namespace["order_callback"](self.source.context, row)
        return None

    def cancel(self, *args):
        self.cancel_calls.append(args)
        self.orders[0].m_nOrderStatus = 54
        self.source.namespace["order_callback"](self.source.context, self.orders[0])
        return True

    def drain(self):
        self.source.namespace["aqp_shm_readonly_adjust"](self.source.context)
        self.source.namespace["aqp_shm_rpc_adjust"](self.source.context)

    async def operation(self, operation):
        task = asyncio.ensure_future(operation)
        end = time.monotonic() + 4
        while not task.done() and time.monotonic() < end:
            self.drain()
            await asyncio.sleep(0.001)
        return await asyncio.wait_for(task, 0.2)

    async def start(self):
        future = self.pool.submit(self.proxy.verify_identity)
        end = time.monotonic() + 3
        while not future.done() and time.monotonic() < end:
            self.drain()
            await asyncio.sleep(0.001)
        future.result(timeout=0.2)

        def receive():
            while not self.stop.is_set() and not self.proxy.stopped:
                self.proxy.poll_once()
                time.sleep(0.001)

        self.pump = self.pool.submit(receive)
        await self.operation(self.read.connect())

    async def close(self):
        self.stop.set()
        if self.pump is not None:
            self.pump.result(timeout=2)
        await self.read.disconnect()
        await self.order.disconnect()
        self.proxy.close()
        self.source.close()
        self.pool.shutdown(wait=True)


def test_formal_gateway_reads_native_evidence_then_submits_cancels_and_consumes_reports(monkeypatch, gateway_modules):
    h = Harness(monkeypatch, gateway_modules)

    async def run():
        try:
            await h.start()
            asset = await h.operation(h.read.query_asset())
            assert asset["cash"] == 1000 and asset["source_evidence"]["complete"] is True
            positions = await h.operation(h.read.query_positions_with_evidence())
            assert positions["positions"][0]["symbol"] == CODE
            assert positions["source_evidence"]["returned_row_count"] == 1
            quotes = await h.operation(h.read.query_quotes([CODE]))
            assert quotes[CODE]["last_price"] == 11.47
            await h.operation(h.order.connect_execution_events())
            placed = await h.operation(h.order.submit_order({
                "profile_id": "sim-stock", "account_id": ACCOUNT, "symbol": CODE,
                "side": "BUY", "quantity": 100, "price_type": 11, "price": 11.42,
                "client_order_id": REMARK, "strategy_name": "local-test",
            }, profile_id="sim-stock"))
            assert placed.status == "ACCEPTED" and placed.broker_order_id == "7620"
            assert len(h.submit_calls) == 1
            reports = await h.operation(h.order.poll_execution_events_once())
            assert any(item["event"]["order_sys_id"] == "7620" for item in reports["events"])
            cancelled = await h.operation(h.order.cancel_order(
                "7620", profile_id="sim-stock", account_id=ACCOUNT,
                order_sysid="7620", user_order_id=REMARK,
            ))
            assert cancelled["status"] == "CANCELLED" and cancelled["automatic_retry"] is False
            assert cancelled["cancel_result"]["result"]["cancellation_confirmed"] is True
            assert len(h.cancel_calls) == 1
            rows = await h.operation(h.read.query_orders_with_evidence())
            assert rows["source_evidence"]["complete"] is True and len(rows["orders"]) == 1
            reports = await h.operation(h.order.poll_execution_events_once())
            assert any(item["event"].get("status") == 54 for item in reports["events"])
            assert h.proxy.identity_snapshot()["write_ready"] is True
        finally:
            await h.close()

    asyncio.run(run())


def test_same_formal_transport_supports_readonly_with_no_native_writes(monkeypatch, gateway_modules):
    h = Harness(monkeypatch, gateway_modules, allow_writes=False)

    async def run():
        try:
            await h.start()
            assert (await h.operation(h.read.query_positions_with_evidence()))["source_evidence"]["complete"]
            await h.operation(h.order.connect_execution_events())
            with pytest.raises(Exception, match="disabled|read.only|False|false"):
                await h.operation(h.order.submit_order({
                    "profile_id": "sim-stock", "account_id": ACCOUNT, "symbol": CODE,
                    "side": "BUY", "quantity": 100, "price_type": 11, "price": 11.42,
                    "client_order_id": REMARK,
                }, profile_id="sim-stock"))
            assert not h.submit_calls and not h.cancel_calls
        finally:
            await h.close()

    asyncio.run(run())


def test_actual_multi_account_profile_uses_same_proxy_for_market_reads_and_execution_events(monkeypatch, gateway_modules):
    h = Harness(monkeypatch, gateway_modules)

    async def run():
        try:
            await h.start()
            connected = await h.operation(h.multi.connect())
            assert connected["account_id"] == ACCOUNT
            positions = await h.operation(h.multi.query_positions_with_evidence(
                profile_id="sim-stock", account_id=ACCOUNT,
            ))
            assert positions["source_evidence"]["complete"] is True
            history = await h.operation(h.multi.query_minute_bars(
                [CODE], start_time="20261015090000", end_time="20261015150000",
                count=1, period="1m", fill_data=False, profile_id="sim-stock",
            ))
            assert len(history[CODE]) == 1 and history[CODE][0]["close"] == 11.42
            subscription = await h.operation(h.multi.subscribe_quote(CODE, profile_id="sim-stock"))
            h.source.context.callback({CODE: dict(lastPrice=11.48, time=1792110001000)})
            events = await h.operation(h.multi.poll_quote_events(
                subscription["initial_sequence"], profile_id="sim-stock",
            ))
            assert len(events["events"]) == 1 and events["gap"] is False
            stopped = await h.operation(h.multi.unsubscribe_quote(
                subscription["subscription_id"], profile_id="sim-stock",
            ))
            assert stopped["cancelled"] is True
            snapshot = await h.operation(h.multi.query_execution_snapshot(profile_id="sim-stock"))
            assert snapshot["orders_source_evidence"]["complete"] is True
            assert snapshot["trades_source_evidence"]["complete"] is True
            assert not h.submit_calls and not h.cancel_calls
        finally:
            await h.close()

    asyncio.run(run())


def test_unknown_native_write_stops_new_writes_but_keeps_formal_account_queries(monkeypatch, gateway_modules):
    h = Harness(monkeypatch, gateway_modules, unknown_submit=True)

    async def run():
        order = {"profile_id": "sim-stock", "account_id": ACCOUNT, "symbol": CODE,
                 "side": "BUY", "quantity": 100, "price_type": 11, "price": 11.42,
                 "client_order_id": REMARK}
        try:
            await h.start()
            result = await h.operation(h.order.submit_order(order, profile_id="sim-stock"))
            assert result.status == "UNKNOWN" and result.broker_order_id is None
            assert len(h.submit_calls) == 1
            asset = await h.operation(h.read.query_asset())
            assert asset["source_evidence"]["complete"] is True
            await h.operation(h.order.connect_execution_events())
            await h.operation(h.order.poll_execution_events_once())
            with pytest.raises(Exception, match="UNKNOWN|blocked|disabled|stopped"):
                await h.operation(h.order.submit_order(
                    dict(order, client_order_id="native-second"), profile_id="sim-stock",
                ))
            assert len(h.submit_calls) == 1
        finally:
            await h.close()

    asyncio.run(run())


def test_live_default_delivery_reads_actual_dual_source_but_never_writes(monkeypatch, gateway_modules):
    h = Harness(monkeypatch, gateway_modules, live=True)

    async def run():
        try:
            await h.start()
            asset = await h.operation(h.read.query_asset())
            positions = await h.operation(h.read.query_positions_with_evidence())
            assert asset["source_evidence"]["environment"] == "LIVE"
            assert positions["source_evidence"]["environment"] == "LIVE"
            proof = h.proxy.identity_snapshot()
            assert proof["environment"] == "LIVE" and proof["writes_blocked"] is True
            assert proof["live_write_authorized"] is False and proof["write_ready"] is False
            await h.operation(h.order.connect_execution_events())
            with pytest.raises(Exception, match="(?i)blocked|disabled|write|写|ready"):
                await h.operation(h.order.submit_order({
                    "profile_id": h.profile_id, "account_id": h.account, "symbol": CODE,
                    "side": "BUY", "quantity": 100, "price_type": 11, "price": 11.42,
                    "client_order_id": REMARK,
                }, profile_id=h.profile_id))
            assert not h.submit_calls and not h.cancel_calls
            assert not h.redis.hashes.get("aqp:redis_shm:write_ledger:%s:v1" % h.account)
            assert "aqp:redis_shm:write_ledger:55013746:v1" not in h.redis.hashes
        finally:
            await h.close()

    asyncio.run(run())


def test_live_explicit_future_activation_confirms_order_and_deduplicates_same_rid_across_real_wire(monkeypatch, gateway_modules):
    h = Harness(monkeypatch, gateway_modules, live=True,
                live_write_authorized=True, start_write_blocked=False)

    async def run():
        try:
            await h.start()
            await h.operation(h.order.connect_execution_events())
            result = await h.operation(h.order.submit_order({
                "profile_id": h.profile_id, "account_id": h.account, "symbol": CODE,
                "side": "BUY", "quantity": 100, "price_type": 11, "price": 11.42,
                "client_order_id": REMARK,
            }, profile_id=h.profile_id))
            assert result.status == "ACCEPTED" and result.broker_order_id == "7620"
            assert len(h.submit_calls) == 1 and h.submit_calls[0][2] == h.account
            ledger = h.redis.hashes["aqp:redis_shm:write_ledger:%s:v1" % h.account]
            assert len(ledger) == 1
            record = json.loads(next(iter(ledger.values())))
            assert record["state"] == "CONFIRMED" and record["binding"]["environment"] == "LIVE"
            schema, account, method, params = json.loads(record["fingerprint"])
            assert schema == 1 and account == h.account and method == "submit_order"
            # 直接经正式客户端、原编码、同一代理和原生双源重读精确RID；不制造第二个订单。
            replay = await h.operation(asyncio.to_thread(h.order._get_shm_rpc_client().call,
                method, params, request_id=record["request_id"]))
            assert replay["status"] == "CONFIRMED" and replay["order_sys_id"] == "7620"
            assert len(h.submit_calls) == 1 and len(ledger) == 1
            assert h.proxy.identity_snapshot()["write_ready"] is True
            assert "aqp:redis_shm:write_ledger:55013746:v1" not in h.redis.hashes
            snapshot = await h.operation(h.read.query_execution_snapshot())
            assert snapshot["orders_source_evidence"]["environment"] == "LIVE"
            assert len(snapshot["orders"]) == 1
        finally:
            await h.close()

    asyncio.run(run())


def test_live_unknown_native_submission_blocks_second_order_and_keeps_account_reads(monkeypatch, gateway_modules):
    h = Harness(monkeypatch, gateway_modules, live=True, unknown_submit=True,
                live_write_authorized=True, start_write_blocked=False)

    async def run():
        order = dict(profile_id=h.profile_id, account_id=h.account, symbol=CODE,
                     side="BUY", quantity=100, price_type=11, price=11.42, client_order_id=REMARK)
        try:
            await h.start()
            result = await h.operation(h.order.submit_order(order, profile_id=h.profile_id))
            assert result.status == "UNKNOWN" and len(h.submit_calls) == 1
            assert h.proxy.identity_snapshot()["writes_blocked"] is True
            record = json.loads(next(iter(h.redis.hashes["aqp:redis_shm:write_ledger:%s:v1" % h.account].values())))
            assert record["state"] == "UNKNOWN" and record["binding"]["environment"] == "LIVE"
            asset = await h.operation(h.read.query_asset())
            assert asset["source_evidence"]["environment"] == "LIVE"
            with pytest.raises(Exception, match="UNKNOWN|blocked|disabled|stopped"):
                await h.operation(h.order.submit_order(dict(order, client_order_id="native-second"), profile_id=h.profile_id))
            assert len(h.submit_calls) == 1 and not h.cancel_calls
        finally:
            await h.close()

    asyncio.run(run())


def test_live_sixty_seven_quote_symbols_cross_generated_source_proxy_and_formal_adapter(monkeypatch, gateway_modules):
    symbols = ["%06d.SH" % code for code in range(600000, 600067)]
    h = Harness(monkeypatch, gateway_modules, live=True, symbols=symbols)
    native_calls = []
    original = h.source.context.get_full_tick

    def native_ticks(codes):
        native_calls.append(list(codes))
        return original(codes)

    h.source.context.get_full_tick = native_ticks

    async def run():
        try:
            await h.start()
            quotes = await h.operation(h.read.query_quotes(symbols))
            assert set(quotes) == set(symbols) and len(quotes) == 67
            assert all(quote["last_price"] == 11.47 for quote in quotes.values())
            full = await h.operation(asyncio.to_thread(h.read._get_client().call, "get_full_tick", {"codes": symbols}))
            assert set(full) == set(symbols) and all(tick["lastPrice"] == 11.47 for tick in full.values())
            assert native_calls == [symbols, symbols]
            assert h.proxy.identity_snapshot()["environment"] == "LIVE"
            assert h.proxy.identity_snapshot()["writes_blocked"] is True
            assert not h.submit_calls and not h.cancel_calls
        finally:
            await h.close()

    asyncio.run(run())


@pytest.mark.parametrize("live,count,limit", [(True, 129, 128), (False, 65, 64)])
@pytest.mark.parametrize("method", ["get_ticks", "get_full_tick"])
def test_proxy_quote_limit_rejects_live_129_and_sim_65_before_native_io(monkeypatch, gateway_modules, live, count, limit, method):
    symbols = ["%06d.SH" % code for code in range(600000, 600000 + count)]
    h = Harness(monkeypatch, gateway_modules, live=live, symbols=symbols[:limit])
    native_calls = []
    original = h.source.context.get_full_tick

    def native_ticks(codes):
        native_calls.append(list(codes))
        return original(codes)

    h.source.context.get_full_tick = native_ticks

    async def run():
        try:
            await h.start()
            before = len(h.redis.values)
            with pytest.raises(Exception, match="1-%s" % limit):
                await h.operation(h.read.query_quotes(symbols))
            assert len(h.redis.values) == before and not native_calls
            # 绕过页面层数量校验，仍走正式Gateway客户端和真实Redis wire，
            # 证明拒绝来自代理本身，而不是只验证原生白名单或前端限制。
            with pytest.raises(Exception):
                await h.operation(asyncio.to_thread(h.read._get_client().call, method, {"codes": symbols}))
            # 拒绝在真实代理响应wire中可核对；现有客户端会将缺session_id的
            # 拒绝信封标成route mismatch，不把该既有表现伪装为原生处理。
            responses = [json.loads(value) for key, value in h.redis.values.items()
                         if key.startswith("bigqmt:rpc:resp:%s:" % h.account)]
            rejected = [value for value in responses if value.get("server_error") == "PROXY_REQUEST_REJECTED"]
            assert len(rejected) == 1 and rejected[0]["method"] == method
            assert "1-%s" % limit in rejected[0]["error"]
            assert not native_calls and not h.submit_calls and not h.cancel_calls
            assert h.proxy.stopped is False
        finally:
            await h.close()

    asyncio.run(run())


def test_direct_readonly_quote_client_keeps_default_sixty_four_symbol_limit():
    symbols = ["%06d.SH" % code for code in range(600000, 600067)]
    with pytest.raises(ShmProtocolError, match="1-64"):
        ShmReadOnlyRpcClient._quote_params({"codes": symbols})
    assert ShmReadOnlyRpcClient._quote_params({"codes": symbols}, max_symbols=128)["codes"] == symbols
