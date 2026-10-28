# coding: utf-8
"""生成的原生模型、代理及 Gateway 客户端的本地故障合同；不连接实盘。"""

import asyncio
import json
from types import SimpleNamespace

import pytest

from test_external_redis_gateway_readwrite_integration import DUAL, Harness, gateway_modules
from bigqmt_signal_trader.redis_rpc import decode_rpc_request_payload
from bigqmt_signal_trader.transports.redis_transport import RedisTransport
from bigqmt_signal_trader.transports.readonly_shm import ShmTimeoutError


TARGET = {"gateway_id": "qmt-vm-001", "profile_id": "sim-bigqmt"}


@pytest.mark.parametrize("state,previous", [("UNKNOWN", None), ("NOT_SENT", False)])
def test_generated_native_proxy_and_gateway_client_preserve_read_failure(
        monkeypatch, gateway_modules, state, previous):
    h = Harness(monkeypatch, gateway_modules, execution_cursor_guard_target=TARGET,
                test_supervisor_gate=True)

    async def scenario():
        try:
            await h.start()
            client = h.read._get_client()
            ping = await h.operation(asyncio.to_thread(client.call, "ping", {}, request_id="integration-ping"))
            assert ping["read_fault_isolation_contract"] == "READ_FAILURE_ISOLATION_V1"
            assert ping["shm_publication_evidence_contract"] == "SHM_PUBLICATION_STATE_V1"
            read = h.source.clients["read"]
            original_send = read.send_request

            def injected_failure(request, timeout_seconds):
                if request["method"] != "get_ticks":
                    return original_send(request, timeout_seconds)
                error = ShmTimeoutError("local fault injection")
                error.publication_state = state
                error.previous_unconfirmed = previous
                error.request_id = request["request_id"]
                error.session_id = h.source_binding["read_session_id"]
                raise error

            monkeypatch.setattr(read, "send_request", injected_failure)
            with pytest.raises(gateway_modules.client.RedisShmReadUnavailable) as failure:
                await h.operation(asyncio.to_thread(
                    client.call, "get_ticks", {"code": "000001.SZ"}, request_id="integration-read"))
            assert failure.value.publication_state == state
            assert failure.value.publication_scope == ("NATIVE_SHM_NOT_PUBLISHED" if state == "NOT_SENT" else "UNKNOWN")
            assert failure.value.request_id == "integration-read"
            assert failure.value.session_id == h.source_binding["read_session_id"]
            assert failure.value.sequence is None and failure.value.source_binding_verified is True
            assert failure.value.previous_unconfirmed is previous
            assert failure.value.automatic_retry is False
            assert client.writes_blocked is (state == "UNKNOWN")
            if state == "UNKNOWN":
                with pytest.raises(RuntimeError, match="original request is unconfirmed"):
                    client.call("get_ticks", {"code": "000001.SZ"}, request_id="integration-read")
                assert h.proxy.identity_snapshot()["write_ready"] is False
            result = await h.operation(asyncio.to_thread(client.call, "get_positions", {},
                                                        request_id="integration-execution-read"))
            assert isinstance(result, (dict, list))
            assert h.proxy.identity_snapshot()["execution_source_state"] == "HEALTHY"
            assert not h.submit_calls and not h.cancel_calls
        finally:
            await h.close()

    asyncio.run(scenario())


def test_generated_native_guard_crosses_proxy_and_gateway_without_financial_execution(monkeypatch, gateway_modules):
    h = Harness(monkeypatch, gateway_modules, execution_cursor_guard_target=TARGET,
                test_supervisor_gate=True)

    async def scenario():
        try:
            await h.start()
            client = gateway_modules.client.RedisShmRpcClient(
                account_id=h.account, environment=h.environment, redis_config={},
                expected_source_binding=h.source_binding, transport=RedisTransport(h.redis, account_id=h.account),
                allow_order_methods=True, expected_source_policy=h.policy,
                gateway_id="qmt-vm-001", profile_id="sim-bigqmt", execution_event_sink_probe=lambda: True)
            ping = await h.operation(asyncio.to_thread(client.call, "ping", {}, request_id="guard-ping"))
            contract = "EXECUTION_CURSOR_WRITE_GATE_V1"
            assert ping["execution_event_guard_contract"] == contract
            assert ping["execution_source"]["execution_event_guard_contract"] == contract
            assert ping["supervisor_gate_epoch_contract"] == "SUPERVISOR_GATE_EPOCH_V1"
            assert ping["execution_source"]["supervisor_gate_epoch_contract"] == "SUPERVISOR_GATE_EPOCH_V1"
            params = {"stock_code": "000001.SZ", "action": "BUY", "volume": 100, "price": 10.5}
            count = len(h.redis.inbound_wires)
            with pytest.raises(ValueError, match="explicit matching durable execution event guard"):
                client.call("submit_order", params, request_id="no-cursor-proof")
            assert len(h.redis.inbound_wires) == count
            h.source.namespace["order_callback"](h.source.context,
                dict(m_strAccountID=h.account, m_strOrderSysID="unpersisted-event"))
            guard = {"contract": contract, "execution_session_id": h.source_binding["execution_session_id"],
                     "native_pid": h.source_binding["native_pid"], "sequence": 0}
            epoch = {"contract": "SUPERVISOR_GATE_EPOCH_V1", "epoch": 1}
            result = await h.operation(asyncio.to_thread(client.call, "submit_order",
                {**params, "execution_event_guard": guard, "supervisor_gate_epoch": epoch},
                request_id="stale-cursor"))
            assert result["status"] == "BLOCKED"
            assert result["no_write_contract"] == "EXECUTION_CURSOR_NO_WRITE_V1"
            assert result["native_write_executed"] is False and result["automatic_retry"] is False
            encoded = h.redis.inbound_wires[-1]
            if isinstance(encoded, bytes):
                encoded = encoded.decode("utf-8")
            sent = json.loads(decode_rpc_request_payload(encoded))
            assert sent["request_id"] == "stale-cursor" and sent["params"]["execution_event_guard"] == guard
            assert sent["params"]["supervisor_gate_epoch"] == epoch
            ledger = h.redis.hashes[h.proxy._write_ledger_key]
            assert json.loads(ledger["stale-cursor"])["state"] == "NOT_EXECUTED"
            assert not h.submit_calls and not h.cancel_calls
        finally:
            await h.close()

    asyncio.run(scenario())


def test_generated_native_without_supervisor_gate_never_claims_or_publishes_write(monkeypatch, gateway_modules):
    h = Harness(monkeypatch, gateway_modules, execution_cursor_guard_target=TARGET)

    async def scenario():
        try:
            await h.start()
            snapshot = h.proxy.identity_snapshot()
            assert snapshot["write_ready"] is False
            assert snapshot["execution_source"]["native_write_runtime_guard_ready"] is False
            assert "supervisor_gate_epoch_contract" not in snapshot
            request = dict(schema_version=1, request_id="missing-supervisor-gate", account_id=h.account,
                           environment=h.environment, method="submit_order", params={
                               "stock_code": "000001.SZ", "action": "BUY", "volume": 100, "price": 10.5,
                               "execution_event_guard": {"contract": "EXECUTION_CURSOR_WRITE_GATE_V1",
                                   "execution_session_id": h.source_binding["execution_session_id"],
                                   "native_pid": h.source_binding["native_pid"], "sequence": 0},
                               "supervisor_gate_epoch": {"contract": "SUPERVISOR_GATE_EPOCH_V1", "epoch": 1},
                           }, expected_source_binding=h.source_binding)
            sequence = h.source.clients["execution"].mailbox._header()[4]
            response = h.proxy.process_request(request)
            assert response["ok"] is False and response["publication_state"] == "NOT_SENT"
            assert response["publication_scope"] == "SHM"
            assert h.source.clients["execution"].mailbox._header()[4] == sequence
            assert not h.redis.hashes.get(h.proxy._write_ledger_key)
            assert not h.submit_calls and not h.cancel_calls
        finally:
            await h.close()

    asyncio.run(scenario())


def test_inactive_event_sink_never_publishes_to_redis(monkeypatch, gateway_modules):
    h = Harness(monkeypatch, gateway_modules, execution_cursor_guard_target=TARGET,
                test_supervisor_gate=True)

    async def scenario():
        try:
            await h.start()
            client = gateway_modules.client.RedisShmRpcClient(
                account_id=h.account, environment=h.environment, redis_config={},
                expected_source_binding=h.source_binding, transport=RedisTransport(h.redis, account_id=h.account),
                allow_order_methods=True, expected_source_policy=h.policy,
                gateway_id="qmt-vm-001", profile_id="sim-bigqmt")
            await h.operation(asyncio.to_thread(client.call, "ping", {}, request_id="sink-ping"))
            guard = {"contract": "EXECUTION_CURSOR_WRITE_GATE_V1",
                     "execution_session_id": h.source_binding["execution_session_id"],
                     "native_pid": h.source_binding["native_pid"], "sequence": 0}
            epoch = {"contract": "SUPERVISOR_GATE_EPOCH_V1", "epoch": 1}
            count = len(h.redis.inbound_wires)
            with pytest.raises(gateway_modules.client.RedisShmWriteNotSent) as failure:
                client.call("submit_order", {"stock_code": "000001.SZ", "action": "BUY", "volume": 100,
                            "price": 10.5, "execution_event_guard": guard, "supervisor_gate_epoch": epoch},
                            request_id="inactive-event-sink")
            assert failure.value.publication_scope == "REDIS_NOT_PUBLISHED"
            assert len(h.redis.inbound_wires) == count and not h.submit_calls and not h.cancel_calls
        finally:
            await h.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("method", ["submit_order", "submit_order_async", "cancel_order"])
def test_generated_native_epoch_rejection_is_preserved_through_proxy_and_client(monkeypatch, gateway_modules, method):
    h = Harness(monkeypatch, gateway_modules, execution_cursor_guard_target=TARGET,
                test_supervisor_gate=True)

    class NewEpochLease(DUAL.FixtureRuntimeWriteLease):
        def read_epoch(self):
            return 2

    async def scenario():
        try:
            await h.start()
            client = gateway_modules.client.RedisShmRpcClient(
                account_id=h.account, environment=h.environment, redis_config={},
                expected_source_binding=h.source_binding, transport=RedisTransport(h.redis, account_id=h.account),
                allow_order_methods=True, expected_source_policy=h.policy,
                gateway_id="qmt-vm-001", profile_id="sim-bigqmt", execution_event_sink_probe=lambda: True)
            await h.operation(asyncio.to_thread(client.call, "ping", {}, request_id="epoch-ping"))
            bridge = h.source.namespace["_RUNTIMES"]["execution"]["_SERVER"].bridge
            cursor = bridge._event_sequence
            await h.operation(asyncio.to_thread(client.call, "poll_execution_events", {"after_sequence": cursor},
                                                request_id="epoch-cursor-poll"))
            bridge.native_write_runtime_guard = NewEpochLease
            if method == "cancel_order":
                h.orders.append(SimpleNamespace(
                    m_strAccountID=h.account, m_strOrderSysID="fixture-order", m_strInstrumentID="000001",
                    m_strExchangeID="SZ", m_strRemark="fixture-user-order", m_strStrategyName="",
                    m_nOffsetFlag=48, m_nOpType=23, m_nOrderStatus=50, m_nVolumeTotalOriginal=100,
                    m_nVolumeTraded=0, m_dLimitPrice=10.5, m_dTradedPrice=0.0, m_nOrderPriceType=50,
                    m_strInsertDate="20261016", m_strInsertTime="100000"))
            guard = {"contract": "EXECUTION_CURSOR_WRITE_GATE_V1",
                     "execution_session_id": h.source_binding["execution_session_id"],
                     "native_pid": h.source_binding["native_pid"], "sequence": cursor}
            epoch = {"contract": "SUPERVISOR_GATE_EPOCH_V1", "epoch": 1}
            params = {"stock_code": "000001.SZ", "action": "BUY", "volume": 100, "price": 10.5,
                      "order_sys_id": "fixture-order", "user_order_id": "fixture-user-order",
                      "execution_event_guard": guard,
                      "supervisor_gate_epoch": epoch}
            if method == "submit_order_async":
                params.update(client_order_id="fixture-epoch-cid", remark="fixture-epoch-cid")
            response = await h.operation(asyncio.to_thread(client.call, method, params, request_id="epoch-stale"))
            assert response.get("no_write_contract") == "SUPERVISOR_GATE_NO_WRITE_V1", json.dumps(response)
            assert response["error"] == "SUPERVISOR_GATE_EPOCH_MISMATCH"
            assert response["supervisor_gate_epoch"] == epoch and response["native_supervisor_gate_epoch"] == 2
            assert response["native_write_executed"] is False and response["automatic_retry"] is False
            assert json.loads(h.redis.hashes[h.proxy._write_ledger_key]["epoch-stale"])["state"] == "NOT_EXECUTED"
            sent_count = len(h.redis.inbound_wires)
            with pytest.raises(RuntimeError, match="original write request was already sent"):
                client.call(method, {**params, "supervisor_gate_epoch": {**epoch, "epoch": 2}},
                            request_id="epoch-stale")
            assert len(h.redis.inbound_wires) == sent_count
            assert not h.submit_calls and not h.cancel_calls
        finally:
            await h.close()

    asyncio.run(scenario())
