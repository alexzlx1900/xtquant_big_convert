# coding: utf-8
"""旧调用入口→外置代理→实际双源生成物→原handler，本地闭环验收。"""

from concurrent.futures import ThreadPoolExecutor
import importlib.util
import json
import os
from pathlib import Path
import sys
import threading
import time

import pytest


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from bigqmt_signal_trader.redis_rpc import call_redis_rpc, encode_rpc_request_payload
from bigqmt_signal_trader.redis_shm_proxy import RedisShmReadOnlyProxy
from bigqmt_signal_trader.transports.redis_transport import RedisTransport


def load_fixture(name, filename):
    spec = importlib.util.spec_from_file_location(name, ROOT / "tests" / filename)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


DUAL = load_fixture("external_dual_generated_fixture", "test_dual_shm_single_file.py")
ROWS = load_fixture("external_native_rows_fixture", "test_shm_readonly.py")
ACCOUNT = "55013746"
CODE = "000001.SZ"


class MemoryPubSub:
    def __init__(self, redis):
        self.redis, self.channels, self.messages = redis, set(), []
        self.closed = False

    def subscribe(self, channel):
        self.channels.add(channel)

    def get_message(self, timeout=0):
        until = time.monotonic() + timeout
        while True:
            with self.redis.lock:
                if self.messages:
                    return self.messages.pop(0)
            if time.monotonic() >= until:
                return None
            time.sleep(0.001)

    def close(self):
        self.closed = True


class MemoryRedis:
    def __init__(self):
        self.lock = threading.Lock()
        self.values, self.lists, self.ttls, self.subscriptions = {}, {}, {}, []
        self.hashes = {}
        self.inbound_wires = []

    def hgetall(self, key):
        with self.lock:
            return dict(self.hashes.get(key, {}))

    def hget(self, key, field):
        with self.lock:
            return self.hashes.get(key, {}).get(field)

    def eval(self, script, numkeys, key, *args):
        """原子Hash容量门与CAS；匹配Redis语义，不模拟代理业务路由。"""
        assert numkeys == 1
        with self.lock:
            values = self.hashes.setdefault(key, {})
            if "HLEN" in script:
                rid, capacity, record = args
                if rid in values:
                    return [0, values[rid]]
                if len(values) >= int(capacity):
                    return [-1, ""]
                values[rid] = record
                return [1, record]
            rid, old, new = args
            if values.get(rid) != old:
                return 0
            values[rid] = new
            return 1

    def setex(self, key, ttl, value):
        with self.lock:
            self.values[key], self.ttls[key] = value, ttl

    def get(self, key):
        with self.lock:
            return self.values.get(key)

    def rpush(self, key, value):
        with self.lock:
            self.lists.setdefault(key, []).append(value)
            if key == "bigqmt:rpc:queue:" + ACCOUNT:
                self.inbound_wires.append(value)
            return len(self.lists[key])

    def lpop(self, key):
        with self.lock:
            values = self.lists.get(key, [])
            return values.pop(0) if values else None

    def blpop(self, key, timeout):
        until = time.monotonic() + timeout
        while time.monotonic() < until:
            value = self.lpop(key)
            if value is not None:
                return key, value
            time.sleep(0.001)
        return None

    def expire(self, key, ttl):
        self.ttls[key] = ttl

    def delete(self, key):
        # 仅旧客户端删除自己的响应list；代理没有清理业务键。
        with self.lock:
            self.lists.pop(key, None)

    def publish(self, channel, value):
        with self.lock:
            targets = [s for s in self.subscriptions if not s.closed and channel in s.channels]
            for subscription in targets:
                subscription.messages.append({"type": "message", "data": value})
            return len(targets)

    def pubsub(self, **kwargs):
        result = MemoryPubSub(self)
        self.subscriptions.append(result)
        return result


class Harness:
    def __init__(self, monkeypatch, *, timeout=1.0, symbols=None):
        self.source = DUAL.Fixture(monkeypatch, symbols=symbols or [CODE])
        self.original_query = self.source.query
        self.source.namespace["get_trade_detail_data"] = self.query
        self.source.start()
        self.redis = MemoryRedis()
        self.proxy = RedisShmReadOnlyProxy(
            RedisTransport(self.redis, account_id=ACCOUNT),
            self.source.clients["read"], self.source.clients["execution"],
            account_id=ACCOUNT, expected_readonly_session_id=self.source.clients["read"].mailbox.session_id,
            expected_execution_session_id=self.source.clients["execution"].mailbox.session_id,
            expected_native_pid=os.getpid(), timeout_seconds=timeout,
            deadline_unix=time.time() + 30,
        )
        self.execute(self.proxy.verify_identity)

    def query(self, account, kind, detail, strategy_name=""):
        if detail == "ORDER":
            self.source.query_calls.append((account, kind, detail, strategy_name))
            return [ROWS.order_row(m_strAccountID=ACCOUNT, m_strStrategyName=strategy_name)]
        if detail == "DEAL":
            self.source.query_calls.append((account, kind, detail, strategy_name))
            return [ROWS.trade_row(m_strAccountID=ACCOUNT, m_strStrategyName=strategy_name)]
        return self.original_query(account, kind, detail, strategy_name)

    def drain(self):
        self.source.namespace["aqp_shm_readonly_adjust"](self.source.context)
        self.source.namespace["aqp_shm_rpc_adjust"](self.source.context)

    def execute(self, function, *args):
        with ThreadPoolExecutor(1) as executor:
            future = executor.submit(function, *args)
            until = time.monotonic() + 3
            while not future.done() and time.monotonic() < until:
                self.drain()
                time.sleep(0.001)
            return future.result(timeout=0.2)

    def legacy_call(self, method, params=None, transport="queue"):
        with ThreadPoolExecutor(2) as executor:
            future = executor.submit(call_redis_rpc, self.redis, ACCOUNT, method,
                                     params=params, timeout_seconds=2, transport=transport)
            current = None
            until = time.monotonic() + 3
            while not future.done() and time.monotonic() < until:
                if current is None or current.done():
                    if current is not None:
                        current.result()
                    current = executor.submit(self.proxy.poll_once)
                self.drain()
                time.sleep(0.001)
            value = future.result(timeout=0.2)
            if current is not None:
                current.result(timeout=0.2)
            return value

    def close(self):
        self.proxy.close()
        self.source.close()
        assert all(subscription.closed for subscription in self.redis.subscriptions)


def test_original_queue_and_pubsub_clients_use_actual_two_source_protocols(monkeypatch):
    h = Harness(monkeypatch)
    try:
        positions = h.legacy_call("get_positions")
        assert positions["ok"] and positions["data"][CODE]["volume"] == 100
        asset = h.legacy_call("get_asset")
        assert asset["ok"] and asset["data"]["cash"] == 1000
        assert "asset" not in asset["data"] and asset["source_evidence"]["complete"]
        orders = h.legacy_call("query_orders", {"cancelable_only": True, "strategy_name": "original"})
        assert orders["ok"] and len(orders["data"]) == 1
        assert orders["data"][0]["order_sys_id"] == "ORDER-1"
        assert orders["data"][0]["status"] == "55"
        trades = h.legacy_call("query_trades")
        assert trades["ok"] and trades["data"][0]["trade_id"] == "DEAL-1"
        quote = h.legacy_call("get_ticks", {"code": CODE}, transport="pubsub")
        assert quote["ok"] and quote["data"][CODE]["lastPrice"] == 11.47
        assert all(wire.startswith("b64s:") for wire in h.redis.inbound_wires)
        assert all(ttl == 60 for ttl in h.redis.ttls.values())
        assert h.source.context.accounts == [ACCOUNT]
        h.source.writes.assert_not_called()
    finally:
        h.close()


def test_partial_quotes_preserve_native_error_and_available_rows(monkeypatch):
    h = Harness(monkeypatch, symbols=[CODE, "600000.SH"])
    h.source.context.get_full_tick = lambda codes: {CODE: dict(lastPrice=11.47, time=1792110000000)}
    try:
        value = h.legacy_call("get_full_tick", {"codes": [CODE, "600000.SH"]})
        assert value["ok"] is False and value["server_error"] == "QMT_QUOTE_UNAVAILABLE"
        assert value["data"]["missing_symbols"] == ["600000.SH"]
        assert value["data"]["quotes"][CODE]["lastPrice"] == 11.47
        assert not h.proxy.stopped
    finally:
        h.close()


def test_actual_native_timeout_is_unknown_sticky_and_late_expired_query_does_not_run(monkeypatch):
    h = Harness(monkeypatch, timeout=0.03)
    request = dict(schema_version=1, request_id="unknown-once", account_id=ACCOUNT,
                   method="get_positions", params={})
    before = len(h.source.query_calls)
    try:
        # 不运行QMT callback，真实写入内存邮箱后超时。
        response = h.proxy.process_raw(encode_rpc_request_payload(request))
        assert response["status"] == "UNKNOWN" and not response["ok"]
        assert response["automatic_retry"] is False and h.proxy.stopped
        with pytest.raises(Exception, match="stopped"):
            h.proxy.process_request(request)
        h.drain()
        assert len(h.source.query_calls) == before
        assert h.proxy.processed_count == 1
    finally:
        h.close()
