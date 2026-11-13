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

from bigqmt_signal_trader.redis_shm_proxy import RedisShmReadOnlyProxy, SupervisorWriteGateError
from bigqmt_signal_trader.transports.redis_transport import RedisTransport
from bigqmt_signal_trader.transports.readonly_shm_client import ShmReadOnlyRpcClient
from bigqmt_signal_trader.transports.readonly_shm import ShmProtocolError, ShmReadOnlyClient
from test_external_redis_dual_shm_integration import DUAL, MemoryRedis
from test_live_redis_shm_proxy import RUNNER as LIVE_RUNNER
from test_live_redis_shm_proxy import isolated_redis


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


class FixtureSupervisorWriteGate:
    """本地线程模型，不能作为 Windows 句柄或生产授权的证据。"""

    def __init__(self):
        self.held = False

    def acquire(self):
        assert not self.held
        self.held = True

    def check(self):
        return None

    def read_epoch(self):
        assert self.held
        return 1

    def release(self):
        assert self.held
        self.held = False

    def close(self):
        assert not self.held


class Harness:
    def __init__(self, monkeypatch, gateway, *, allow_writes=True, unknown_submit=False,
                 live=False, start_write_blocked=None, live_write_authorized=False, symbols=None,
                 order_submit_mode="SYNC", redis_factory=MemoryRedis,
                 receive_policy="QUEUE_AND_PUBSUB", execution_cursor_guard_target=None,
                 test_supervisor_gate=False, gateway_identity=None, guarded_candidate=False):
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
            execution_event_guard=execution_cursor_guard_target is not None,
            read_fault_isolation=execution_cursor_guard_target is not None,
            enable_market_rpc=allow_writes,
            symbols=[CODE] if symbols is None else symbols,
        )
        self.gateway, self.redis = gateway, redis_factory()
        self.orders, self.trades, self.submit_calls, self.cancel_calls = [], [], [], []
        original_query = self.source.query

        def query(account, kind, detail, strategy_name=""):
            if detail == "ORDER":
                return self.orders
            if detail == "DEAL":
                return self.trades
            return original_query(account, kind, detail, strategy_name)

        self.source.namespace.update(
            get_trade_detail_data=query, passorder=self.submit, cancel=self.cancel,
        )
        self.source.context.get_market_data_ex = lambda *args, **kwargs: {
            CODE: {"20261015100100": dict(open=11.4, high=11.5, low=11.3, close=11.42,
                                       volume=100, amount=1142.0)}
        }
        self.source.start(enable_fixture_write_gate=test_supervisor_gate)
        if test_supervisor_gate:
            # 仅限本地模型；真实生成源码默认不会声明可信运行门就绪。
            self.source.namespace["_RUNTIMES"]["execution"]["_SERVER"].bridge.runtime_metadata.update(
                supervisor_gate_epoch_contract="SUPERVISOR_GATE_EPOCH_V1",
                native_write_runtime_guard_attached=True, native_callback_threading_verified=True)
        self.source.namespace["account_callback"](
            self.source.context,
            SimpleNamespace(m_strAccountID=self.account, m_strStatus="登录成功", m_nBrokerType=2,
                            m_strAccountKey="LIVE-STOCK-FIXTURE-ONLY"),
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
            receive_policy=receive_policy,
            execution_cursor_guard_target=execution_cursor_guard_target,
            previous_source_unknown_target=(gateway_identity if gateway_identity and
                gateway_identity.get("gateway_id") == "qmt-vm-001" else None),
            supervisor_write_gate=FixtureSupervisorWriteGate() if test_supervisor_gate else None,
        )
        if test_supervisor_gate:
            # 仅贯通本地模型，不能作为真实进程/内核 ATTACH 验收。
            self.proxy._supervisor_attach_verified = True
        self.pool = ThreadPoolExecutor(1)
        self.stop = threading.Event()
        self.pump = None
        self.profile = gateway.config.GatewayAccountProfile(
            profile_id=self.profile_id, label="local external fixture", environment=self.environment,
            account_id=self.account, userdata_mini_path="fixture-not-accessed", order_submit_mode=order_submit_mode,
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

        if guarded_candidate and not (live and test_supervisor_gate and execution_cursor_guard_target == {
                "gateway_id": "qmt-vm-001", "profile_id": "live-stock"}):
            raise ValueError("guarded candidate fixture requires the complete VM LIVE candidate")

        def client_factory(**kwargs):
            client = gateway.client.RedisShmRpcClient(
                account_id=self.account, environment=self.environment,
                expected_source_policy=self.policy, allow_order_methods=allow_writes,
                redis_config={}, timeout_seconds=1.0,
                transport=RedisTransport(self.redis, account_id=self.account),
                expected_source_binding=self.source_binding,
                gateway_id=(gateway_identity or execution_cursor_guard_target or {}).get("gateway_id"),
                profile_id=(gateway_identity or execution_cursor_guard_target or {}).get("profile_id"),
            )
            if guarded_candidate:
                # 当前 VM LIVE 保持现用非守卫模式；这里只模拟尚未部署的守卫候选。
                client._execution_guard_required = True
            return client

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
            m_strAccountID=self.account, m_strOrderSysID=str(7620 + len(self.orders)), m_strInstrumentID="000001",
            m_strExchangeID="SZ", m_strRemark=str(args[9]) if len(args) > 9 else REMARK, m_strStrategyName="",
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


@pytest.mark.parametrize("prior_unknown_write", [False, True])
@pytest.mark.parametrize("timing", ["late_frame", "acknowledged_after_deadline"])
@pytest.mark.parametrize("guarded_candidate", [False, True])
def test_vm_late_account_read_recovers_through_gateway(monkeypatch, gateway_modules, isolated_redis, prior_unknown_write, timing, guarded_candidate):
    target = {"gateway_id": "qmt-vm-001", "profile_id": "live-stock"}
    h = Harness(monkeypatch, gateway_modules, live=True, receive_policy="QUEUE_ONLY", start_write_blocked=False,
                live_write_authorized=True,
                redis_factory=lambda: isolated_redis, gateway_identity=None if guarded_candidate else target,
                execution_cursor_guard_target=target if guarded_candidate else None,
                test_supervisor_gate=guarded_candidate, guarded_candidate=guarded_candidate)
    for source_name, route in (("read", "readonly"), ("execution", "execution")):
        h.proxy._clients[route] = ShmReadOnlyClient(h.source.clients[source_name].name, h.account,
                                                   region=DUAL.Region(h.source.states[source_name]))
    # 本地进程仅模拟同一 Windows 原生来源，不能作为 VM 身份证据。
    h.proxy._check_native_process_identity = lambda: None

    async def scenario():
        try:
            await h.start()
            client = h.read._get_client()
            assert client._execution_guard_required is guarded_candidate
            client._uncertain_write = prior_unknown_write
            execution = h.proxy._clients["execution"]
            original_send = execution.send_request
            calls = []
            late_method = "query_orders" if guarded_candidate else "get_positions"

            def tracked_send(value, timeout_seconds, **kwargs):
                calls.append(value["request_id"])
                result = original_send(value, timeout_seconds=timeout_seconds, **kwargs)
                if timing == "acknowledged_after_deadline" and value["request_id"] == "gateway-late-position":
                    time.sleep(.08)
                return result

            monkeypatch.setattr(execution, "send_request", tracked_send)
            h.proxy.timeout_seconds = .05
            with pytest.raises(gateway_modules.client.RedisShmReadUnavailable) as failure:
                operation = asyncio.to_thread(client.call, late_method, {}, request_id="gateway-late-position")
                await (operation if timing == "late_frame" else h.operation(operation))
            assert failure.value.publication_state == "UNKNOWN" and failure.value.automatic_retry is False
            assert not client.writes_blocked
            with pytest.raises(RuntimeError, match="original request is unconfirmed"):
                client.call(late_method, {}, request_id="gateway-late-position")
            assert bool(h.proxy._pending_request_recoveries) is (timing == "late_frame")
            deadline = time.monotonic() + 2
            while h.proxy._pending_request_recoveries and time.monotonic() < deadline:
                h.drain()
                await asyncio.sleep(.005)
            assert not h.proxy._pending_request_recoveries and not h.proxy.isolation_reason
            h.proxy.timeout_seconds = 1
            assert (await h.operation(h.read.healthcheck()))["status"] == "CONNECTED"
            positions = await h.operation(h.read.query_positions())
            assert len(positions) == 1 and positions[0]["symbol"] == "000001.SZ"
            assert positions[0]["quantity"] == 100 and positions[0]["account_id"] == h.account
            assert not client.writes_blocked and client._uncertain_write is prior_unknown_write
            assert client.write_block_reason == ""
            assert calls.count("gateway-late-position") == 1
            assert h.proxy.get_cached_response("gateway-late-position")["status"] == "UNKNOWN"
            assert not h.submit_calls and not h.cancel_calls
        finally:
            await h.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("live", [False, True])
def test_new_fields_cross_generated_source_proxy_and_gateway_without_writes(monkeypatch, gateway_modules, live):
    h = Harness(monkeypatch, gateway_modules, live=live)
    common = dict(m_strAccountID=h.account, m_strOrderSysID="7620",
                  m_strInstrumentID="000001", m_strExchangeID="SZ",
                  m_strRemark=REMARK, m_strStrategyName="native-strategy",
                  m_strInstrumentName="fixture-stock", m_strShareholderID="fixture-shareholder",
                  m_nOffsetFlag=48, m_nDirection=48)
    h.orders.append(SimpleNamespace(
        **common, m_nOpType=23, m_nOrderStatus=50, m_nVolumeTotalOriginal=100,
        m_nVolumeTraded=20, m_dLimitPrice=11.42, m_dTradedPrice=11.4,
        m_nOrderPriceType=50, m_strInsertDate="20261016", m_strInsertTime="100000"))
    h.trades.append(SimpleNamespace(
        **common, m_strTradeID="fixture-trade", m_nVolume=20, m_dPrice=11.4,
        m_dTradeAmount=228.0, m_strTradeDate="20261016", m_strTradeTime="100001",
        m_dComssion=0.0, m_dCommission=1.36))

    async def run():
        try:
            await h.start()
            await h.operation(h.order.connect_execution_events())
            client = h.order._get_shm_rpc_client()
            orders = await h.operation(asyncio.to_thread(client.call, "query_orders", {}))
            trades = await h.operation(asyncio.to_thread(client.call, "query_trades", {}))
            for item in (orders[0], trades[0]):
                assert item["account_type"] == 2
                assert item["instrument_name"] == "fixture-stock"
                assert item["secu_account"] == "fixture-shareholder"
                assert item["strategy_name"] == "native-strategy"
                assert item["offset_flag"] == 48 and item["direction"] == 48
            assert orders[0]["op_type"] == 23 and orders[0]["order_type"] == 23
            assert trades[0]["commission"] == 1.36 and trades[0]["amount"] == 228.0
            asset = await h.operation(h.read.query_asset())
            positions = await h.operation(h.read.query_positions_with_evidence())
            assert asset["cash"] == 1000 and asset["source_evidence"]["complete"] is True
            assert positions["positions"][0]["symbol"] == CODE
            assert positions["source_evidence"]["complete"] is True
            assert not h.submit_calls and not h.cancel_calls
            assert not h.redis.hashes.get("aqp:redis_shm:write_ledger:%s:v1" % h.account)
        finally:
            await h.close()

    asyncio.run(run())


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
    h.order.gateway_id = "qmt-vm-001"

    async def run():
        order = dict(profile_id=h.profile_id, account_id=h.account, symbol=CODE,
                     side="BUY", quantity=100, price_type=11, price=11.42, client_order_id=REMARK)
        try:
            await h.start()
            result = await h.operation(h.order.submit_order(order, profile_id=h.profile_id))
            assert result.status == "UNKNOWN" and len(h.submit_calls) == 1
            assert h.proxy.identity_snapshot()["writes_blocked"] is False
            record = json.loads(next(iter(h.redis.hashes["aqp:redis_shm:write_ledger:%s:v1" % h.account].values())))
            assert record["state"] == "UNKNOWN" and record["binding"]["environment"] == "LIVE"
            asset = await h.operation(h.read.query_asset())
            assert asset["source_evidence"]["environment"] == "LIVE"
            h.unknown_submit = False
            independent = await h.operation(h.order.submit_order(dict(order, client_order_id="native-second"), profile_id=h.profile_id))
            assert independent.status == "ACCEPTED"
            assert len(h.submit_calls) == 2 and not h.cancel_calls
        finally:
            await h.close()

    asyncio.run(run())


def test_live_quote_timeout_keeps_proxy_and_gateway_path_alive_without_resend(monkeypatch, gateway_modules):
    h = Harness(monkeypatch, gateway_modules, live=True, receive_policy="QUEUE_ONLY",
                live_write_authorized=True, start_write_blocked=False)

    async def run():
        try:
            await h.start()
            before = h.source.clients["read"].mailbox._header()
            monkeypatch.setitem(h.source.namespace, "aqp_shm_readonly_adjust", lambda context: None)
            with pytest.raises(Exception):
                await h.operation(h.read.query_quotes([CODE]))
            deadline = time.monotonic() + 2
            while h.proxy._route_faults["readonly"] is None and time.monotonic() < deadline:
                await asyncio.sleep(0.01)
            assert h.proxy._route_faults["readonly"]["state"] == "RESULT_UNKNOWN"
            assert not h.proxy.stopped and not h.pump.done()
            assert h.proxy.identity_snapshot()["write_ready"] is True
            pending = h.source.clients["read"].mailbox._header()
            assert pending[4] == before[4] + 1 and pending[6] == before[6]
            asset = await h.operation(h.read.query_asset())
            assert asset["source_evidence"]["environment"] == "LIVE"
            health = await h.operation(h.multi.healthcheck(profile_id=h.profile_id))
            assert health["status"] == "CONNECTED"
            assert health["order_provider_health"]["write_readiness"] == "WRITE_RUNTIME_READY"
            proof = await h.operation(asyncio.to_thread(h.read._get_client().call, "ping", {}))
            assert proof["read_source_state"] == "RESULT_UNKNOWN" and proof["read_source_fault"]
            assert h.source.clients["read"].mailbox._header()[4:7] == pending[4:7]
            assert not h.submit_calls and not h.cancel_calls
        finally:
            await h.close()

    asyncio.run(run())


def test_live_proxy_health_is_understood_by_actual_supervisor(gateway_modules, tmp_path, capsys):
    supervisor = importlib.import_module("supervise_qmt_runtime")
    config = dict(environment="LIVE", runtime_policy="PERSISTENT_LIVE_READ_WRITE",
                  account_id="8882152350", native_pid=2852, execution_session_id="e" * 32)
    proxy = SimpleNamespace(isolation_reason="REDIS_RECEIVE_OR_PROCESS_FAILED", _route_faults={},
                            _write_block_reasons=["PROXY_ISOLATED_REDIS_RECEIVE_OR_PROCESS_FAILED"],
                            _writes_blocked=True)
    LIVE_RUNNER._emit_proxy_health(config, proxy)
    log = tmp_path / "proxy.stdout.log"
    log.write_text(capsys.readouterr().out, encoding="utf-8")
    manager = object.__new__(supervisor.RuntimeSupervisor)
    manager.proxy_log_path = log
    manager.config = {"account_id": config["account_id"]}
    manager.children = {"proxy": SimpleNamespace(pid=os.getpid())}
    health = manager.proxy_health({"native_pid": config["native_pid"],
                                   "execution_session_id": config["execution_session_id"]})
    assert health["status"] == "ISOLATED" and health["reason"] == proxy.isolation_reason
    assert health["writes_blocked"] is True


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


def test_identity_pending_thirteen_quotes_cross_gateway_without_touching_order(
        monkeypatch, gateway_modules, isolated_redis):
    symbols = ["%06d.SH" % code for code in range(600000, 600013)]
    target = {"gateway_id": "qmt-vm-001", "profile_id": "live-stock"}
    h = Harness(monkeypatch, gateway_modules, live=True, symbols=symbols,
                redis_factory=lambda: isolated_redis, receive_policy="QUEUE_ONLY",
                execution_cursor_guard_target=target, test_supervisor_gate=True, guarded_candidate=True)
    h.source.namespace["_RUNTIMES"]["execution"]["_SERVER"].bridge.runtime_metadata.update(
        native_callback_threading_verified=False, native_write_runtime_guard_ready=False)

    def identity_pending():
        raise SupervisorWriteGateError("SUPERVISOR_IDENTITY_NOT_READY", permanent=False)

    h.proxy._supervisor_write_gate.check = identity_pending
    # 模型中的进程并非 Windows QMT；此替身不构成真实进程身份验收。
    h.proxy._check_native_process_identity = lambda: None
    queue = h.proxy.transport.request_queue
    queued_order = json.dumps({"schema_version": 1, "request_id": "old-order", "account_id": h.account,
                               "environment": "LIVE", "method": "submit_order", "params": {}})

    async def run():
        try:
            await h.start()
            h.redis.rpush(queue, queued_order)
            quotes = await h.operation(h.read.query_quotes(symbols))
            assert set(quotes) == set(symbols) and len(quotes) == 13
            assert all(quote["last_price"] == 11.47 for quote in quotes.values())
            assert h.redis.lrange(queue, 0, -1) == []
            rejected = json.loads(h.redis.get("bigqmt:rpc:resp:%s:old-order" % h.account))
            assert rejected["status"] == "REJECTED" and rejected["publication_state"] == "NOT_SENT"
            for method in ("submit_order", "cancel_order"):
                with pytest.raises(Exception, match="unverified|disabled|required|blocked|supervisor"):
                    await asyncio.to_thread(h.read._get_client().call, method, {})
            assert not h.submit_calls and not h.cancel_calls
            assert h.proxy.identity_snapshot()["write_ready"] is False
            assert not h.redis.hgetall("aqp:redis_shm:write_ledger:%s:v1" % h.account)
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
