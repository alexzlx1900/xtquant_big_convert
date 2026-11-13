"""VM 请求级故障合同：只用本地 Redis/原生替身，不连接 QMT。"""

import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from test_live_redis_shm_proxy import FIXTURE, LEDGER, RUNNER, LiveClient, config, isolated_redis, make_proxy, request


def vm_proxy(**changes):
    values = dict(execution_cursor_guard_target=None, supervisor_write_gate=None,
                  previous_source_unknown_target={"gateway_id": "qmt-vm-001", "profile_id": "live-stock"},
                  receive_policy="QUEUE_ONLY", start_write_blocked=False, live_write_authorized=True)
    values.update(changes)
    return make_proxy(**values)


@pytest.mark.parametrize("method", ["get_ticks", "get_positions", "get_asset", "poll_execution_events"])
def test_read_failure_does_not_poison_a_new_business(method):
    proxy, provider, read, execution = vm_proxy()
    source = read if method == "get_ticks" else execution
    source.failure = FIXTURE.ShmTimeoutError("fixture response unconfirmed")
    params = {"code": "000001.SZ"} if method == "get_ticks" else {"after_sequence": 0} if method == "poll_execution_events" else {}
    original = request(method, rid="failed-read", params=params)
    response = proxy.process_request(original)
    assert response["status"] == "UNKNOWN"
    source.failure = None
    execution.result = dict(status="SUBMITTED", order_sys_id="new-business")
    before = len(execution.calls)
    result = proxy.process_request(request("submit_order", rid="new-business"))
    assert result["ok"] is True
    assert len(execution.calls) == before + 1
    assert not proxy.isolation_reason and not proxy.identity_snapshot()["writes_blocked"]
    assert proxy.get_cached_response("failed-read")["status"] == "UNKNOWN"


def test_unknown_write_blocks_only_the_original_business():
    proxy, provider, read, execution = vm_proxy()
    execution.failure = FIXTURE.ShmTimeoutError("fixture write unconfirmed")
    original = request("submit_order", rid="unknown-order")
    assert proxy.process_request(original)["status"] == "UNKNOWN"
    saved = provider.hget(LEDGER, "unknown-order")
    execution.failure = None
    execution.result = dict(status="SUBMITTED", order_sys_id="next-order")
    before = len(execution.calls)
    assert proxy.process_request(request("submit_order", rid="next-order"))["ok"] is True
    assert len(execution.calls) == before + 1
    before = len(execution.calls)
    assert proxy.process_request(original)["status"] == "UNKNOWN"
    assert len(execution.calls) == before
    assert provider.hget(LEDGER, "unknown-order") == saved


@pytest.mark.parametrize("method", ["submit_order", "cancel_order"])
def test_changing_rid_does_not_repeat_the_same_business(isolated_redis, method):
    proxy, provider, read, execution = vm_proxy(provider=isolated_redis)
    execution.failure = FIXTURE.ShmTimeoutError("fixture original write unknown")
    params = {"remark": "stable-business"} if method == "submit_order" else {"order_sys_id": "stable-original-contract"}
    original = request(method, rid="original-business", params=params)
    assert proxy.process_request(original)["status"] == "UNKNOWN"
    saved = provider.hget(LEDGER, "original-business")
    execution.failure = None
    execution.result = dict(status="SUBMITTED", order_sys_id="independent-order")
    before = len(execution.calls)
    duplicate = proxy.process_request(request(method, rid="different-rid", params=params))
    assert duplicate["status"] == "UNKNOWN" and len(execution.calls) == before
    assert proxy.process_request(request("submit_order", rid="independent-order"))["ok"] is True
    assert len(execution.calls) == before + 1
    restarted, _, _, fresh = vm_proxy(provider=provider)
    fresh.result = dict(status="SUBMITTED", order_sys_id="after-restart")
    assert restarted.process_request(request("submit_order", rid="after-restart"))["ok"] is True
    before = len(fresh.calls)
    assert restarted.process_request(original)["status"] == "UNKNOWN"
    assert len(fresh.calls) == before
    assert provider.hget(LEDGER, "original-business") == saved


def test_expired_original_after_proxy_restart_keeps_exact_unknown_receipt(isolated_redis):
    proxy, provider, read, execution = vm_proxy(provider=isolated_redis)
    execution.failure = FIXTURE.ShmTimeoutError("fixture original business remains unconfirmed")
    original = request("submit_order", rid="original-before-restart", read_expires_at_unix=time.time() + 1)
    result = proxy.process_request(original)
    assert result["status"] == "UNKNOWN"
    saved = provider.hget(LEDGER, original["request_id"])
    restarted, _, _, native = vm_proxy(provider=provider)
    expired = dict(original, read_expires_at_unix=time.time() - 10)
    assert restarted.process_request(expired) == result
    assert len(native.calls) == 1 and provider.hget(LEDGER, original["request_id"]) == saved


def test_legacy_sync_business_without_new_index_still_rejects_a_changed_rid(isolated_redis):
    proxy, provider, read, execution = vm_proxy(provider=isolated_redis)
    execution.failure = FIXTURE.ShmTimeoutError("fixture old synchronous business unknown")
    original = request("submit_order", rid="legacy-rid", params={"remark": "legacy-business"})
    assert proxy.process_request(original)["status"] == "UNKNOWN"
    legacy = json.loads(provider.hget(LEDGER, "legacy-rid"))
    legacy.pop("business_key")
    saved = json.dumps(legacy)
    provider.hset(LEDGER, "legacy-rid", saved)
    provider.delete(LEDGER + ":business")  # 仅当前测试专用 Redis，模拟升级前没有业务索引。
    restarted, _, _, native = vm_proxy(provider=provider)
    result = restarted.process_request(request("submit_order", rid="aliased-rid", params={"remark": "legacy-business"}))
    assert result["status"] == "UNKNOWN" and result["data"]["original_request_id"] == "legacy-rid"
    assert len(native.calls) == 1 and provider.hget(LEDGER, "legacy-rid") == saved


def test_conflicting_native_remark_rejects_only_that_request():
    proxy, provider, read, execution = vm_proxy()
    before = len(execution.calls)
    result = proxy.process_request(request("submit_order", rid="conflicting-tag",
        params={"client_order_id": "business-one", "remark": "business-two"}))
    assert not result["ok"] and result["publication_state"] == "NOT_SENT" and len(execution.calls) == before
    execution.result = dict(status="SUBMITTED", order_sys_id="independent")
    assert proxy.process_request(request("submit_order", rid="valid-after-conflict"))["ok"] is True
    assert len(execution.calls) == before + 1


def test_bad_queue_message_does_not_stop_the_next_request(isolated_redis):
    proxy, provider, read, execution = vm_proxy(provider=isolated_redis)
    execution.result = dict(status="SUBMITTED", order_sys_id="after-bad-message")
    provider.rpush(proxy.transport.request_queue, "{broken-json",
                   json.dumps(request("submit_order", rid="after-bad-message")))
    proxy.poll_once()
    result = proxy.poll_once()
    assert result["request_id"] == "after-bad-message" and result["ok"] is True
    assert not proxy.isolation_reason
    assert provider.lrange("aqp:redis_shm:invalid_requests:" + proxy.account_id, 0, -1) == ["{broken-json"]


def test_startup_missing_mapping_consumes_requests_then_recovers_without_manual_unlock(monkeypatch, isolated_redis):
    value = config(execution_cursor_guard_target=None, receive_policy="QUEUE_ONLY",
        previous_source_unknown_target={"gateway_id": "qmt-vm-001", "profile_id": "live-stock"},
        start_write_blocked=False, live_write_authorized=True)
    stop, ready = threading.Event(), threading.Event()
    clients, attempts = {}, {"readonly": 0}
    monkeypatch.setattr(RUNNER, "windows_session_id", lambda pid: 1)
    def factory(name, account):
        route = "readonly" if name == value["read_name"] else "execution"
        if route == "readonly":
            attempts[route] += 1
            if attempts[route] <= 2:
                raise OSError("fixture mapping temporarily absent")
        client = LiveClient(route)
        client.mailbox.session_id = value["read_session_id" if route == "readonly" else "execution_session_id"]
        if route == "execution":
            client.result = {"status": "SUBMITTED", "order_sys_id": "after-source-return"}
        clients[route] = client
        return client
    queue = "bigqmt:rpc:queue:" + value["account_id"]
    isolated_redis.rpush(queue, json.dumps(request("submit_order", rid="during-source-outage")))
    with ThreadPoolExecutor(1) as pool:
        future = pool.submit(RUNNER.run_candidate, value, lambda _: isolated_redis, factory,
                             stop_event=stop, ready_observer=lambda proof: ready.set())
        try:
            key = "bigqmt:rpc:resp:%s:during-source-outage" % value["account_id"]
            deadline = time.monotonic() + 3
            while isolated_redis.get(key) is None and time.monotonic() < deadline:
                time.sleep(.01)
            response = json.loads(isolated_redis.get(key))
            assert response["publication_state"] == "NOT_SENT" and not response["ok"]
            assert ready.wait(3)
            binding = dict(read_session_id=value["read_session_id"], execution_session_id=value["execution_session_id"],
                           native_pid=value["native_pid"], windows_session_id=value["windows_session"])
            isolated_redis.rpush(queue, json.dumps(request("submit_order", rid="after-source-return", expected_source_binding=binding)))
            key = "bigqmt:rpc:resp:%s:after-source-return" % value["account_id"]
            deadline = time.monotonic() + 3
            while isolated_redis.get(key) is None and time.monotonic() < deadline:
                time.sleep(.01)
            assert json.loads(isolated_redis.get(key))["ok"] is True
            assert [item[0]["request_id"] for item in clients["execution"].calls if item[0]["method"] == "submit_order"] == ["after-source-return"]
        finally:
            stop.set()
        assert future.result(timeout=3)["status"] == "STOP_REQUESTED"


def test_fault_does_not_disable_explicit_operator_write_stop():
    proxy, provider, read, execution = vm_proxy(start_write_blocked=True)
    read.failure = FIXTURE.ShmTimeoutError("fixture quote fault")
    proxy.process_request(request("get_market_data_ex", rid="read-before-operator-stop"))
    before = len(execution.calls)
    result = proxy.process_request(request("submit_order", rid="operator-stopped-order"))
    assert not result["ok"] and len(execution.calls) == before
    assert proxy.identity_snapshot()["write_block_reason"] == "OPERATOR_WRITE_DISABLED"
