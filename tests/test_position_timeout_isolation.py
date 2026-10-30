# coding: utf-8
"""持仓读取超时不得静默重发或停止整条网关链路。"""

import asyncio
import importlib
import time

import pytest

import test_live_redis_shm_proxy as LIVE
import bigqmt_signal_trader.redis_shm_proxy as proxy_module
from bigqmt_signal_trader.transports.readonly_shm import ShmTimeoutError
from test_external_redis_gateway_readwrite_integration import Harness, gateway_modules


POSITION_ROUTE = "POSITION_READ_ROUTE_V1"


def position_route_capable(read, execution):
    read.additional_capabilities = frozenset({"get_positions"})

    def read_mutation(request, response):
        if request["method"] == "ping":
            response["data"]["read_fault_isolation_contract"] = "READ_FAILURE_ISOLATION_V1"

    def execution_mutation(request, response):
        if request["method"] == "ping":
            response["data"].update(read_fault_isolation_contract="READ_FAILURE_ISOLATION_V1",
                                    native_read_source_state="UNKNOWN", native_read_session_id="readonly-sid")

    read.mutate = read_mutation
    execution.mutate = execution_mutation


def test_live_position_route_requires_negotiation_and_preserves_legacy_routing():
    proxy, _, read, execution = LIVE.make_proxy(client_mutator=position_route_capable)
    assert proxy.identity_snapshot()["position_read_route_contract"] == POSITION_ROUTE
    legacy = proxy.process_request(LIVE.request("get_positions", rid="legacy-position"))
    assert legacy["ok"] is True and execution.calls[-1][0]["method"] == "get_positions"
    before_execution = len(execution.calls)
    routed = proxy.process_request(LIVE.request("get_positions", rid="read-position",
                                                position_read_route_contract=POSITION_ROUTE))
    assert routed["ok"] is True and routed["session_id"] == "readonly-sid"
    assert read.calls[-1][0]["method"] == "get_positions"
    assert len(execution.calls) == before_execution

    invalid = proxy.process_request(LIVE.request("get_positions", rid="invalid-route",
                                                 position_read_route_contract="unexpected"))
    assert invalid["ok"] is False and len(execution.calls) == before_execution
    assert read.calls[-1][0]["request_id"] == "read-position"


def test_position_route_marker_cannot_change_existing_request_identity():
    proxy, _, _, execution = LIVE.make_proxy(client_mutator=position_route_capable)
    original = LIVE.request("get_positions", rid="same-position")
    assert proxy.process_request(original)["ok"] is True
    count = len(execution.calls)
    changed = LIVE.request("get_positions", rid="same-position", position_read_route_contract=POSITION_ROUTE)
    assert proxy.process_request(changed)["ok"] is False
    assert len(execution.calls) == count


def test_position_route_marker_cannot_change_other_account_routing():
    proxy, _, read, execution = LIVE.FIXTURE.RedisShmReadOnlyProxyTest().make_proxy(
        policy="PERSISTENT_SIMULATION_READ_WRITE")
    request = LIVE.FIXTURE.request("get_positions", rid="sim-unsupported-route")
    request["position_read_route_contract"] = POSITION_ROUTE
    read_count, execution_count = len(read.calls), len(execution.calls)
    assert proxy.process_request(request)["ok"] is False
    assert len(read.calls) == read_count and len(execution.calls) == execution_count


def test_live_position_timeout_isolates_execution_source_without_replay():
    proxy, redis, read, execution = LIVE.make_proxy()
    request = LIVE.request("get_positions", rid="position-timeout")
    timeout = ShmTimeoutError("response arrived after its deadline; result unconfirmed")
    timeout.publication_state = "UNKNOWN"
    timeout.request_id = request["request_id"]
    timeout.session_id = "execution-sid"
    timeout.sequence = 926
    timeout.previous_unconfirmed = False
    execution.failure = timeout

    result = proxy.process_request(request)

    assert result["status"] == "UNKNOWN"
    assert result["server_error"] == "SHM_RESULT_UNCONFIRMED"
    assert result["automatic_retry"] is False
    assert result["publication_state"] == "UNKNOWN"
    assert result["sequence"] == 926
    assert proxy.stopped is False
    assert proxy.identity_snapshot()["execution_source_state"] == "RESULT_UNKNOWN"
    assert proxy.identity_snapshot()["write_ready"] is False

    calls_after_timeout = len(execution.calls)
    assert proxy.process_request(request) == result
    assert len(execution.calls) == calls_after_timeout
    other_position = proxy.process_request(LIVE.request("get_positions", rid="another-position"))
    assert other_position["ok"] is False
    assert len(execution.calls) == calls_after_timeout

    quote = proxy.process_request(LIVE.request("get_ticks", rid="independent-quote",
                                               params={"code": "000001.SZ"}))
    assert quote["ok"] is True
    assert len(execution.calls) == calls_after_timeout
    assert all(call[0]["method"] not in ("submit_order", "cancel_order") for call in execution.calls)
    assert not redis.hashes
    assert read.closed is False


def test_late_validated_quote_response_does_not_stop_proxy_or_replay(monkeypatch):
    proxy, _, read, execution = LIVE.make_proxy(client_mutator=position_route_capable)
    original_send = read.send_request

    def return_after_deadline(request, timeout_seconds, **kwargs):
        response = original_send(request, timeout_seconds, **kwargs)
        time.sleep(0.65)
        return response

    monkeypatch.setattr(read, "send_request", return_after_deadline)
    request = LIVE.request("get_ticks", rid="late-confirmed-quote", params={"code": "000001.SZ"},
                           read_expires_at_unix=time.time() + 0.5)
    result = proxy.process_request(request)
    assert result["status"] == "UNKNOWN" and result["automatic_retry"] is False
    assert result["publication_state"] == "UNKNOWN" and result["server_error"] == "SHM_RESULT_UNCONFIRMED"
    assert proxy.diagnostic_snapshot()["last_error"]["error_phase"] == "SHM_RESPONSE_VALIDATE"
    assert proxy.stopped is False and proxy.identity_snapshot()["execution_source_state"] == "HEALTHY"
    assert proxy.identity_snapshot()["read_source_state"] == "RESULT_UNKNOWN"
    assert proxy.identity_snapshot()["write_ready"] is False
    count = len(read.calls)
    assert proxy.process_request(request) == result and len(read.calls) == count
    assert all(call[0]["method"] not in ("submit_order", "cancel_order") for call in execution.calls)


def test_pending_position_callback_defers_execution_without_publication():
    proxy, _, read, execution = LIVE.make_proxy(client_mutator=position_route_capable)
    request = LIVE.request("get_positions", rid="slow-position", position_read_route_contract=POSITION_ROUTE)
    timeout = ShmTimeoutError("position callback still running")
    timeout.publication_state, timeout.previous_unconfirmed = "UNKNOWN", False
    timeout.request_id, timeout.session_id, timeout.sequence = request["request_id"], "readonly-sid", 2
    read.failure = timeout
    assert proxy.process_request(request)["status"] == "UNKNOWN"
    count = len(execution.calls)
    result = proxy.process_request(LIVE.request("ping", rid="ping-during-position"))
    assert result["status"] == "REJECTED" and result["server_error"] == "SHM_NOT_SENT"
    assert result["publication_state"] == "NOT_SENT" and result["previous_unconfirmed"] is False
    assert len(execution.calls) == count
    assert proxy.identity_snapshot()["execution_source_state"] == "HEALTHY" and not proxy.stopped


@pytest.mark.parametrize("fault", [
    {"state": "IDENTITY_OR_PROTOCOL_ERROR"},
    {"state": "RESULT_UNKNOWN", "publication_state": "UNKNOWN", "previous_unconfirmed": None,
     "sequence": 2, "trigger_request_id": "prior-read"},
])
def test_unverifiable_read_fault_defers_execution(fault):
    proxy, _, _, execution = LIVE.make_proxy(client_mutator=position_route_capable)
    proxy._route_faults["readonly"] = fault
    count = len(execution.calls)
    result = proxy.process_request(LIVE.request("ping", rid="ping-with-unverifiable-read"))
    assert result["server_error"] == "SHM_NOT_SENT" and result["publication_state"] == "NOT_SENT"
    assert result["previous_unconfirmed"] is False and len(execution.calls) == count


def test_live_position_timeout_crosses_gateway_without_stopping_proxy(monkeypatch, gateway_modules):
    target = {"gateway_id": "qmt-vm-001", "profile_id": "live-stock"}
    harness = Harness(monkeypatch, gateway_modules, live=True, receive_policy="QUEUE_ONLY",
                      execution_cursor_guard_target=target, test_supervisor_gate=True)

    async def scenario():
        try:
            await harness.start()
            client = harness.read._get_client()
            read = harness.source.clients["read"]
            original_send = read.send_request

            def inject_timeout(request, timeout_seconds, **kwargs):
                if request["method"] != "get_positions":
                    return original_send(request, timeout_seconds, **kwargs)
                error = ShmTimeoutError("local position timeout")
                error.publication_state = "UNKNOWN"
                error.previous_unconfirmed = False
                error.request_id = request["request_id"]
                error.session_id = harness.source_binding["read_session_id"]
                error.sequence = 926
                raise error

            monkeypatch.setattr(read, "send_request", inject_timeout)
            with pytest.raises(gateway_modules.client.RedisShmReadUnavailable) as failure:
                await harness.operation(asyncio.to_thread(client.call, "get_positions", {},
                                                          request_id="integration-position-timeout"))
            assert failure.value.publication_state == "UNKNOWN"
            assert failure.value.automatic_retry is False
            assert failure.value.sequence == 926
            evidence = client._verified_read_fault_locked("readonly", "integration-position-timeout")
            assert evidence is not None
            assert evidence["request"]["position_read_route_contract"] == POSITION_ROUTE
            assert client.writes_blocked is True
            assert harness.proxy.stopped is False
            assert harness.proxy.identity_snapshot()["read_source_state"] == "RESULT_UNKNOWN"
            assert harness.proxy.identity_snapshot()["execution_source_state"] == "HEALTHY"
            assert harness.pump.done() is False
            count = len(harness.redis.inbound_wires)
            with pytest.raises(RuntimeError, match="original request is unconfirmed"):
                client.call("get_positions", {}, request_id="integration-position-timeout")
            assert len(harness.redis.inbound_wires) == count

            for rid in ("execution-not-published", "execution-still-not-published"):
                with pytest.raises(gateway_modules.client.RedisShmReadUnavailable) as pending:
                    await harness.operation(asyncio.to_thread(client.call, "ping", {}, request_id=rid))
                assert pending.value.publication_state == "NOT_SENT" and pending.value.previous_unconfirmed is False
            assert client._identity_invalidated is False
            assert harness.proxy.identity_snapshot()["execution_source_state"] == "HEALTHY"
            assert not harness.submit_calls and not harness.cancel_calls
        finally:
            await harness.close()

    asyncio.run(scenario())


def test_position_old_frame_can_be_verified_without_ack_or_execution_change(monkeypatch, gateway_modules):
    target = {"gateway_id": "qmt-vm-001", "profile_id": "live-stock"}
    harness = Harness(monkeypatch, gateway_modules, live=True, receive_policy="QUEUE_ONLY",
                      execution_cursor_guard_target=target, test_supervisor_gate=True)
    # 跨仓测试以本地替身提供固定出生身份，不能解释为真实 Windows 进程验收。
    identity = {"native_pid": harness.source_binding["native_pid"], "native_creation_filetime": 123456,
                "windows_session_id": 1, "executable": "fixture-native"}
    harness.source.namespace["_BOUND_NATIVE_CREATION_FILETIME"] = 123456
    harness.source.namespace["_SOURCE_IDENTITY"]["native_creation_filetime"] = 123456
    harness.source.namespace["_RUNTIMES"]["read"]["_SERVER"].native_creation_filetime = 123456
    harness.source.namespace["_RUNTIMES"]["execution"]["_SERVER"].bridge.runtime_metadata["native_creation_filetime"] = 123456
    monkeypatch.setattr(proxy_module, "_windows_native_process_identity", lambda pid: identity)

    async def scenario():
        try:
            await harness.start()
            client = harness.read._get_client()
            read = harness.source.clients["read"]
            native_read = harness.source.namespace["_RUNTIMES"]["read"]["_SERVER"]
            transport = harness.source.transports["read"]
            execution = harness.source.clients["execution"]
            before_execution = execution.mailbox._header()[4:7]
            original_send = read.send_request
            original_adjust = harness.source.namespace["aqp_shm_readonly_adjust"]
            delayed = []

            def short_send(request, timeout_seconds, **kwargs):
                if request["method"] == "get_positions":
                    return original_send(request, min(timeout_seconds, 0.01), **kwargs)
                return original_send(request, timeout_seconds, **kwargs)

            def delayed_adjust(context):
                header = native_read.mailbox._header()
                if not delayed and header[4] != header[5] and header[7] > 0:
                    request = transport.decode_payload(native_read.mailbox.region.read(transport.REQUEST_OFFSET, header[7]))
                    if request.get("method") == "get_positions":
                        delayed.append(True)
                        time.sleep(0.03)
                return original_adjust(context)

            monkeypatch.setattr(read, "send_request", short_send)
            harness.source.namespace["aqp_shm_readonly_adjust"] = delayed_adjust
            with pytest.raises(gateway_modules.client.RedisShmReadUnavailable) as failure:
                await harness.operation(asyncio.to_thread(client.call, "get_positions", {}, request_id="late-position"))
            assert failure.value.publication_state == "UNKNOWN" and failure.value.automatic_retry is False
            assert delayed
            harness.drain()
            old_header = native_read.mailbox._header()
            assert old_header[4] == old_header[5] and old_header[6] == old_header[4] - 1
            head = native_read.mailbox.region.read(0, 56)
            request_frame = native_read.mailbox.region.read(transport.REQUEST_OFFSET, old_header[7])
            response_frame = native_read.mailbox.region.read(transport.RESPONSE_OFFSET, old_header[8])
            evidence = native_read.read_rotation_evidence(harness.source_binding["execution_session_id"], 123456)
            assert evidence["method"] == "get_positions" and evidence["request_id"] == "late-position"
            harness.proxy._verified_process_identity = identity
            harness.proxy._verify_old_read_rotation_evidence(evidence)
            assert native_read.mailbox.region.read(0, 56) == head
            assert execution.mailbox._header()[4:7] == before_execution
            assert harness.proxy._readonly_callback_finished() is True
            ping = await harness.operation(asyncio.to_thread(client.call, "ping", {}, request_id="after-position-return"))
            assert ping["execution_source_state"] == "HEALTHY"
            assert native_read.mailbox.region.read(0, 56) == head
            assert native_read.mailbox.region.read(transport.REQUEST_OFFSET, old_header[7]) == request_frame
            assert native_read.mailbox.region.read(transport.RESPONSE_OFFSET, old_header[8]) == response_frame
            assert not harness.submit_calls and not harness.cancel_calls
        finally:
            await harness.close()

    asyncio.run(scenario())


def test_gateway_router_returns_unknown_for_position_timeout_and_keeps_proxy_alive(monkeypatch, gateway_modules):
    target = {"gateway_id": "qmt-vm-001", "profile_id": "live-stock"}
    harness = Harness(monkeypatch, gateway_modules, live=True, receive_policy="QUEUE_ONLY",
                      execution_cursor_guard_target=target, test_supervisor_gate=True)

    async def scenario():
        try:
            await harness.start()
            read = harness.source.clients["read"]
            original_send = read.send_request

            def inject_timeout(request, timeout_seconds, **kwargs):
                if request["method"] != "get_positions":
                    return original_send(request, timeout_seconds, **kwargs)
                error = ShmTimeoutError("fixture position timeout")
                error.publication_state, error.previous_unconfirmed = "UNKNOWN", False
                error.request_id, error.session_id = request["request_id"], harness.source_binding["read_session_id"]
                error.sequence = 926
                raise error

            monkeypatch.setattr(read, "send_request", inject_timeout)
            router = importlib.import_module("gateway_server").GatewayCommandRouter(harness.multi)
            response = await harness.operation(router.dispatch("query.positions", {
                "gateway_id": "qmt-local-test", "profile_id": "live-stock", "account_id": harness.account,
                "include_source_evidence": True,
            }))
            assert response["status"] == "UNKNOWN" and response["automatic_retry"] is False
            assert response["publication_state"] == "UNKNOWN"
            assert harness.proxy.stopped is False and harness.pump.done() is False
            client = harness.read._get_client()
            with pytest.raises(gateway_modules.client.RedisShmReadUnavailable) as pending:
                await harness.operation(asyncio.to_thread(client.call, "ping", {}))
            assert pending.value.publication_state == "NOT_SENT" and pending.value.previous_unconfirmed is False
            assert client._identity_invalidated is False
            assert harness.proxy.identity_snapshot()["execution_source_state"] == "HEALTHY"
            assert not harness.submit_calls and not harness.cancel_calls
        finally:
            await harness.close()

    asyncio.run(scenario())


def test_rotated_read_source_requires_new_position_capability_handshake(monkeypatch, gateway_modules):
    target = {"gateway_id": "qmt-vm-001", "profile_id": "live-stock"}
    harness = Harness(monkeypatch, gateway_modules, live=True, receive_policy="QUEUE_ONLY",
                      execution_cursor_guard_target=target, test_supervisor_gate=True)

    async def scenario():
        try:
            await harness.start()
            read = harness.read._get_client()
            execution = harness.order._get_shm_rpc_client()
            assert read.position_read_route_verified is True
            old = dict(read.expected_source_binding)
            new = {**old, "read_session_id": "f" * 32}
            with gateway_modules.client.RedisShmRpcClient.lock_read_rotation(read, execution, timeout_seconds=1) as actions:
                actions.commit(old, new, transition_id="a" * 32, old_request_id="old-position")
            assert read.position_read_route_verified is False
            before = len(harness.redis.inbound_wires)
            with pytest.raises(gateway_modules.client.RedisShmReadUnavailable) as failure:
                read.call("get_positions", {}, request_id="new-position-before-handshake")
            assert failure.value.publication_state == "NOT_SENT"
            assert len(harness.redis.inbound_wires) == before
            assert not harness.submit_calls and not harness.cancel_calls
        finally:
            await harness.close()

    asyncio.run(scenario())


def test_failed_ping_cannot_fall_back_to_execution_position_source(monkeypatch, gateway_modules):
    target = {"gateway_id": "qmt-vm-001", "profile_id": "live-stock"}
    harness = Harness(monkeypatch, gateway_modules, live=True, receive_policy="QUEUE_ONLY",
                      execution_cursor_guard_target=target, test_supervisor_gate=True)

    async def scenario():
        try:
            await harness.start()
            read = harness.read._get_client()
            assert read.position_read_route_verified is True

            def unavailable(*args, **kwargs):
                raise TimeoutError("fixture ping result unknown")

            monkeypatch.setattr(read._get_transport(), "send_request", unavailable)
            with pytest.raises(Exception):
                read.call("ping", {}, request_id="failed-capability-ping")
            assert read.position_read_route_verified is False
            before = len(harness.redis.inbound_wires)
            with pytest.raises(gateway_modules.client.RedisShmReadUnavailable) as failure:
                read.call("get_positions", {}, request_id="position-after-failed-ping")
            assert failure.value.publication_state == "NOT_SENT"
            assert len(harness.redis.inbound_wires) == before
            assert not harness.submit_calls and not harness.cancel_calls
        finally:
            await harness.close()

    asyncio.run(scenario())


def test_verified_live_position_query_uses_independent_read_source(monkeypatch, gateway_modules):
    target = {"gateway_id": "qmt-vm-001", "profile_id": "live-stock"}
    harness = Harness(monkeypatch, gateway_modules, live=True, receive_policy="QUEUE_ONLY",
                      execution_cursor_guard_target=target, test_supervisor_gate=True)

    async def scenario():
        try:
            await harness.start()
            result = await harness.operation(harness.read.query_positions_with_evidence())
            evidence = result["source_evidence"]
            assert evidence["complete"] is True
            assert evidence["session_id"] == harness.source_binding["read_session_id"]
            assert evidence["native_pid"] == harness.source_binding["native_pid"]
            assert evidence["returned_row_count"] == len(result["positions"])
            assert not harness.submit_calls and not harness.cancel_calls
        finally:
            await harness.close()

    asyncio.run(scenario())


def test_new_gateway_keeps_execution_route_when_proxy_does_not_advertise_contract(monkeypatch, gateway_modules):
    target = {"gateway_id": "qmt-vm-001", "profile_id": "live-stock"}
    harness = Harness(monkeypatch, gateway_modules, live=True, receive_policy="QUEUE_ONLY",
                      execution_cursor_guard_target=target, test_supervisor_gate=True)
    original_identity = harness.proxy.identity_snapshot

    def legacy_identity():
        result = original_identity()
        result.pop("position_read_route_contract", None)
        return result

    monkeypatch.setattr(harness.proxy, "identity_snapshot", legacy_identity)

    async def scenario():
        try:
            await harness.start()
            assert harness.read._get_client().position_read_route_verified is False
            result = await harness.operation(harness.read.query_positions_with_evidence())
            assert result["source_evidence"]["session_id"] == harness.source_binding["execution_session_id"]
            assert not harness.submit_calls and not harness.cancel_calls
        finally:
            await harness.close()

    asyncio.run(scenario())


def test_other_profile_keeps_execution_position_route(monkeypatch, gateway_modules):
    target = {"gateway_id": "qmt-vm-001", "profile_id": "sim-bigqmt"}
    harness = Harness(monkeypatch, gateway_modules, receive_policy="QUEUE_ONLY",
                      execution_cursor_guard_target=target, test_supervisor_gate=True)

    async def scenario():
        try:
            await harness.start()
            assert harness.read._get_client().position_read_route_verified is False
            result = await harness.operation(harness.read.query_positions_with_evidence())
            assert result["source_evidence"]["session_id"] == harness.source_binding["execution_session_id"]
            assert not harness.submit_calls and not harness.cancel_calls
        finally:
            await harness.close()

    asyncio.run(scenario())
