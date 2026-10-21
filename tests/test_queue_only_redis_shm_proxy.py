# coding: utf-8
"""不同DB但共享全局Pub/Sub的本地模型；不启动Redis或访问实机。"""

from concurrent.futures import ThreadPoolExecutor
import json
import threading
import time

import pytest

from test_external_redis_dual_shm_integration import MemoryRedis
from test_external_redis_gateway_readwrite_integration import gateway_modules
from test_live_redis_shm_proxy import ACCOUNT, POLICY, RUNNER, config, make_proxy, request


class SharedPubSub:
    def __init__(self):
        self.lock = threading.RLock()
        self.subscriptions = []


class LogicalDB(MemoryRedis):
    """键/列表按DB分离，Pub/Sub跨DB；与Redis的这两个边界一致。"""

    def __init__(self, number, bus, *, forbid_pubsub=False):
        super().__init__()
        self.number, self.lock, self.subscriptions = number, bus.lock, bus.subscriptions
        self.forbid_pubsub = forbid_pubsub
        self.pubsub_calls, self.rpush_keys, self.published_channels = 0, [], []

    def pubsub(self, **kwargs):
        self.pubsub_calls += 1
        if self.forbid_pubsub:
            raise AssertionError("QUEUE_ONLY must never create a Pub/Sub client")
        return super().pubsub(**kwargs)

    def rpush(self, key, value):
        self.rpush_keys.append(key)
        return super().rpush(key, value)

    def publish(self, channel, value):
        self.published_channels.append(channel)
        return super().publish(channel, value)


@pytest.mark.parametrize("policy", [None, False, True, 0, [], {}, "", "queue_only", "UNSUPPORTED"])
def test_receive_policy_is_strict_and_rejected_before_runner_connections(policy, monkeypatch):
    with pytest.raises(ValueError, match="receive_policy"):
        make_proxy(receive_policy=policy)
    with pytest.raises(ValueError, match="receive_policy"):
        RUNNER.validate_config(config(receive_policy=policy))
    monkeypatch.setattr(RUNNER, "create_redis_client", lambda _: pytest.fail("invalid policy must not connect"))
    with pytest.raises(ValueError, match="receive_policy"):
        RUNNER.run_candidate(config(receive_policy=policy))


def test_default_receive_policy_preserves_sim_and_existing_live_configuration():
    assert RUNNER.validate_config(config())["receive_policy"] == "QUEUE_AND_PUBSUB"
    simulated = config(account_id="55013746", environment="SIM", runtime_policy="PERSISTENT_SIMULATION_READ_WRITE")
    assert RUNNER.validate_config(simulated)["receive_policy"] == "QUEUE_AND_PUBSUB"


def test_global_pubsub_model_reproduces_cross_db_delivery_under_existing_default_policy():
    bus = SharedPubSub()
    old, fresh = LogicalDB(5, bus), LogicalDB(7, bus)
    proxy, _, _, execution = make_proxy(provider=fresh)
    try:
        assert proxy.poll_once() is None
        assert fresh.pubsub_calls == 1
        old.publish("bigqmt:rpc:req:" + ACCOUNT, json.dumps(request(rid="global-pubsub-control")))
        response = proxy.poll_once()
        assert response["ok"] is True and response["request_id"] == "global-pubsub-control"
        assert len(execution.calls) == 2
    finally:
        proxy.close()


def test_queue_only_db7_ignores_db5_and_global_requests_while_formal_gateway_reads_own_queue(gateway_modules):
    bus = SharedPubSub()
    old, fresh = LogicalDB(5, bus), LogicalDB(7, bus, forbid_pubsub=True)
    channel, queue = "bigqmt:rpc:req:" + ACCOUNT, "bigqmt:rpc:queue:" + ACCOUNT
    old_sub = old.pubsub(ignore_subscribe_messages=True)
    old_sub.subscribe(channel)
    old_wire = json.dumps(request(rid="old-db5-must-remain"))
    old.rpush(queue, old_wire)
    old.publish(channel, old_wire)
    proxy, _, _, execution = make_proxy(provider=fresh, receive_policy="QUEUE_ONLY")
    client = gateway_modules.client.RedisShmRpcClient(
        account_id=ACCOUNT, environment="LIVE", expected_source_policy=POLICY, allow_order_methods=True,
        redis_config={}, timeout_seconds=1,
        transport=RUNNER.RedisTransport(fresh, account_id=ACCOUNT),
        expected_source_binding=proxy.identity_snapshot()["source_binding"],
    )
    try:
        assert proxy.poll_once() is None
        assert old.lists[queue] == [old_wire] and len(old_sub.messages) == 1
        assert len(execution.calls) == 1 and fresh.pubsub_calls == 0
        with ThreadPoolExecutor(1) as pool:
            future = pool.submit(client.call, "get_positions", {})
            end = time.monotonic() + 2
            while not future.done() and time.monotonic() < end:
                proxy.poll_once()
                time.sleep(0.001)
            result = future.result(timeout=0.2)
        assert result == [{"order_id": "native-1"}] and len(execution.calls) == 2
        assert queue in fresh.rpush_keys and channel not in fresh.published_channels
        assert old.lists[queue] == [old_wire] and old_sub.get_message(timeout=0)["data"] == old_wire
        assert fresh.pubsub_calls == 0 and proxy._pubsub is None
        proof = proxy.identity_snapshot()
        assert proof["receive_policy"] == "QUEUE_ONLY"
        assert proof["live_write_authorized"] is False and proof["writes_blocked"] is True
        assert proof["write_ready"] is False
        rejected = proxy.process_request(request("submit_order", rid="new-db7-write-still-disabled"))
        assert rejected["ok"] is False and "blocked" in rejected["error"]
        assert len(execution.calls) == 2
        assert not fresh.hashes.get("aqp:redis_shm:write_ledger:%s:v1" % ACCOUNT)
    finally:
        client.close()
        proxy.close()
        old_sub.close()


def test_queue_only_offline_preview_reports_policy_without_connections(tmp_path, monkeypatch, capsys):
    value = config(receive_policy="QUEUE_ONLY", redis=dict(host="127.0.0.1", port=6379, db=7))
    path = tmp_path / "vm-live-queue-only.json"
    path.write_text(json.dumps(value))
    monkeypatch.setattr(RUNNER, "create_redis_client", lambda _: pytest.fail("offline preview must not connect"))
    assert RUNNER.main(["--config", str(path)]) == 0
    proof = json.loads(capsys.readouterr().out)
    assert proof["receive_policy"] == "QUEUE_ONLY" and proof["connections_created"] == 0
    assert proof["live_write_authorized"] is False and proof["start_write_blocked"] is True
