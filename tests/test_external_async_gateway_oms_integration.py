"""本地实际Gateway/Converter适配链与PostgreSQL OMS；不连接现场。"""

import asyncio
from dataclasses import asdict
import importlib.util
import os
from pathlib import Path
import sys
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest


GATEWAY_ROOT = os.environ.get("AQP_GATEWAY_TEST_ROOT")
if os.environ.get("AQP_OMS_CONTRACT") != "1" or not GATEWAY_ROOT:
    pytest.skip("explicit AQP_OMS_CONTRACT=1 and AQP_GATEWAY_TEST_ROOT required", allow_module_level=True)
AQP_ROOT = Path(GATEWAY_ROOT).resolve().parent
sys.path[:0] = [str(AQP_ROOT), str(AQP_ROOT / "tests")]

from sqlalchemy import select

from api.models.live_trading import BrokerOrder, LiveAccount, OrderIntent, TradeGateway
from api.models.market import StockBasic
from api.services.live_trading import oms as oms_module
from api.services.live_trading.gateway import GatewayManager
from test_bigqmt_late_order_binding import binding_db


from test_external_redis_dual_shm_integration import MemoryRedis
from test_external_redis_gateway_readwrite_integration import Harness, gateway_modules


FIXTURE_SPEC = importlib.util.spec_from_file_location("_aqp_external_postgres_contract", AQP_ROOT / "tests/conftest.py")
FIXTURES = importlib.util.module_from_spec(FIXTURE_SPEC)
FIXTURE_SPEC.loader.exec_module(FIXTURES)
postgresql_contract_engine = FIXTURES.postgresql_contract_engine
anyio_backend = FIXTURES.anyio_backend
GATEWAY = "qmt-local-test"
CODE = "000001.SZ"
CID_A, CID_B = "oms-async-native-a", "oms-async-native-b"


class AsyncMemoryRedis(MemoryRedis):
    def eval(self, script, numkeys, *args):
        if numkeys == 1:
            return super().eval(script, numkeys, *args)
        assert numkeys == 2
        key, cid_key, rid, capacity, encoded, cid = args
        with self.lock:
            ledger = self.hashes.setdefault(key, {})
            index = self.hashes.setdefault(cid_key, {})
            if rid in ledger:
                return [0, ledger[rid]]
            if cid in index:
                return [-2, index[cid]]
            if len(ledger) >= int(capacity):
                return [-1, ""]
            ledger[rid], index[cid] = encoded, rid
            return [1, encoded]


class AsyncNativeHarness(Harness):
    def __init__(self, monkeypatch, gateway):
        super().__init__(
            monkeypatch, gateway, live=True, start_write_blocked=False,
            live_write_authorized=True, order_submit_mode="ASYNC",
            redis_factory=AsyncMemoryRedis, receive_policy="QUEUE_ONLY",
        )

    def submit(self, *args):
        self.submit_calls.append(args)

    def report(self, cid, contract, status=50):
        row = SimpleNamespace(
            m_strAccountID=self.account, m_strOrderSysID=contract,
            m_strInstrumentID="000001", m_strExchangeID="SZ", m_strRemark=cid,
            m_strStrategyName="local-async", m_nOffsetFlag=48, m_nOpType=23,
            m_nOrderStatus=status, m_nVolumeTotalOriginal=100, m_nVolumeTraded=0,
            m_dLimitPrice=11.42, m_dTradedPrice=0.0, m_nOrderPriceType=50,
            m_strInsertDate="20261025", m_strInsertTime="113000",
        )
        self.orders.append(row)
        self.source.namespace["order_callback"](self.source.context, row)

    def cancel(self, *args):
        self.cancel_calls.append(args)
        row = next(row for row in self.orders if row.m_strOrderSysID == str(args[0]))
        row.m_nOrderStatus = 54
        self.source.namespace["order_callback"](self.source.context, row)
        return True

    async def operation(self, operation):
        task = asyncio.ensure_future(operation)
        deadline = time.monotonic() + 10
        while not task.done() and time.monotonic() < deadline:
            self.drain()
            await asyncio.sleep(0.001)
        return await asyncio.wait_for(task, 0.2)

    async def order_report(self):
        await self.operation(self.order.poll_execution_events_once())
        while True:
            event = await self.order.next_event(timeout=0.05)
            assert event is not None, "真实Gateway回报必须保留到OMS消费"
            if event.event_type == "order.report":
                return event.payload


class LocalConnector:
    def __init__(self, harness):
        self.harness = harness

    async def submit_order(self, payload, *, profile_id):
        result = await self.harness.order.submit_order(payload, profile_id=profile_id)
        return {"result": {**asdict(result), "gateway_id": GATEWAY}}

    async def cancel_order(self, broker_order_id, *, profile_id, account_id, user_order_id):
        result = await self.harness.order.cancel_order(
            broker_order_id, profile_id=profile_id, account_id=account_id,
            order_sysid=broker_order_id, user_order_id=user_order_id,
        )
        cancel = result.get("cancel_result") or {}
        assert result["status"] == "CANCELLED", (
            cancel.get("error"), cancel.get("server_error"), (cancel.get("result") or {}).get("error"),
        )
        return {"result": {**result, "gateway_id": GATEWAY}}


async def prepare_oms(factory, monkeypatch, harness):
    async with factory.kw["bind"].begin() as connection:
        await connection.run_sync(StockBasic.__table__.create)
    monkeypatch.setattr(oms_module, "AsyncSessionLocal", factory)
    monkeypatch.setattr(oms_module, "build_windows_gateway_connector", lambda _: LocalConnector(harness))
    monkeypatch.setattr(oms_module.LiveOmsService, "_notify_order_event", AsyncMock())
    monkeypatch.setattr(oms_module.LiveOmsService, "assert_trading_allowed", AsyncMock())
    async with factory() as session:
        session.add_all([
            LiveAccount(account_id=harness.account, environment="LIVE",
                        metadata_json={"gateway_id": GATEWAY, "profile_id": "live-stock"}),
            TradeGateway(gateway_id=GATEWAY),
        ])
        await session.flush()
        intents = [OrderIntent(
            client_order_id=cid, account_id=harness.account, symbol=CODE, side="BUY",
            quantity=100, price=11.42, status="SENT", execution_mode="LIVE",
            request_payload={"gateway_id": GATEWAY, "profile_id": "live-stock"},
        ) for cid in (CID_A, CID_B)]
        session.add_all(intents)
        await session.commit()
        return [row.id for row in intents]


def submit_payload(harness, cid):
    return dict(gateway_id=GATEWAY, profile_id="live-stock", account_id=harness.account,
                client_order_id=cid, symbol=CODE, side="BUY", quantity=100,
                price=11.42, price_type=11, strategy_name="local-async")


@pytest.mark.anyio
async def test_actual_async_gateway_to_oms_delayed_contract_cancels_once(
    monkeypatch, gateway_modules, binding_db, tmp_path,
):
    monkeypatch.chdir(tmp_path)
    factory, _ = binding_db
    harness = AsyncNativeHarness(monkeypatch, gateway_modules)
    ids = await prepare_oms(factory, monkeypatch, harness)
    service = oms_module.LiveOmsService()
    try:
        await harness.start()
        await harness.operation(harness.order.connect_execution_events())
        await harness.operation(asyncio.gather(*(
            service._submit_live_order(order_id, submit_payload(harness, cid))
            for order_id, cid in zip(ids, (CID_A, CID_B))
        )))
        assert len(harness.submit_calls) == 2
        assert {args[9] for args in harness.submit_calls} == {CID_A, CID_B}
        async with factory() as session:
            for order_id in ids:
                intent = await session.get(OrderIntent, order_id)
                broker = await session.scalar(select(BrokerOrder).where(BrokerOrder.order_intent_id == order_id))
                assert intent.status == "UNKNOWN_RECONCILING"
                assert broker.broker_status == "PENDING_ASYNC" and broker.submit_mode == "ASYNC"
                assert broker.broker_order_id is None and broker.qmt_async_seq is None
                assert broker.accepted_at is None
        await service.cancel_order(ids[0], actor="local-contract-test")
        assert not harness.cancel_calls
        async with factory() as session:
            assert (await session.get(OrderIntent, ids[0])).status == "CANCEL_PENDING"
        harness.report(CID_A, "real-contract-a")
        payload = await harness.order_report()
        assert payload["order_remark"] == CID_A
        assert payload["broker_order_id"] == "real-contract-a"
        managers = (GatewayManager(), GatewayManager())
        await harness.operation(asyncio.gather(*(
            manager.process_inbound_message(message_type="order.report", payload=payload, gateway_id=GATEWAY)
            for manager in managers
        )))
        await harness.operation(managers[0].process_inbound_message(
            message_type="order.report", payload=payload, gateway_id=GATEWAY))
        async with factory() as session:
            intent = await session.get(OrderIntent, ids[0])
            broker = await session.scalar(select(BrokerOrder).where(BrokerOrder.order_intent_id == ids[0]))
            assert intent.status == "CANCELLED", (
                intent.status, intent.reason, broker.broker_status,
                ((broker.submitted_payload or {}).get("shm_async_cancel_claim") or {}).get("result_message"),
            )
            assert broker.broker_order_id == "real-contract-a" and broker.qmt_async_seq is None
            assert (await session.get(OrderIntent, ids[1])).status == "UNKNOWN_RECONCILING"
        assert len(harness.cancel_calls) == 1
        assert str(harness.cancel_calls[0][0]) == "real-contract-a"
    finally:
        await harness.close()
