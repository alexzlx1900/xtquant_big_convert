# coding: utf-8
"""只测外置兼容合同；使用内存 Redis/SHM，不连接真实服务或 QMT。"""

import copy
import json
import os
import sys
import threading
import time
import unittest
from types import SimpleNamespace
from unittest import mock

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

from bigqmt_signal_trader.redis_rpc import encode_rpc_request_payload
from bigqmt_signal_trader.redis_shm_proxy import EXECUTION_METHODS, QUOTE_METHODS, RedisShmReadOnlyProxy
from bigqmt_signal_trader.transports.readonly_shm_client import MARKET_RPC_METHODS
from bigqmt_signal_trader.transports.shm_rpc import WRITE_METHODS
from bigqmt_signal_trader.transports.readonly_shm import ShmProtocolError, ShmTimeoutError
from bigqmt_signal_trader.transports.redis_transport import RedisTransport


ACCOUNT = "55013746"


class FakePubSub:
    def __init__(self):
        self.messages = []
        self.channels = []
        self.timeouts = []
        self.closed = False

    def subscribe(self, channel):
        self.channels.append(channel)

    def get_message(self, timeout):
        self.timeouts.append(timeout)
        return self.messages.pop(0) if self.messages else None

    def close(self):
        self.closed = True


class FakeRedis:
    def __init__(self):
        self.operations = []
        self.queues = {}
        self.subscription = FakePubSub()
        self.fail_operation = None
        self.closed = False
        self.hashes = {}
        self.fail_eval = None
        self.ledger_lock = threading.Lock()

    def eval(self, script, key_count, key, rid, old_or_capacity, encoded):
        operation = "claim" if "HLEN" in script else "settle"
        self._record("eval", operation, key, rid)
        if self.fail_eval == operation:
            raise RuntimeError("injected ledger failure")
        with self.ledger_lock:
            values = self.hashes.setdefault(key, {})
            if operation == "claim":
                if rid in values:
                    return [0, values[rid]]
                if len(values) >= int(old_or_capacity):
                    return [-1, ""]
                values[rid] = encoded
                return [1, encoded]
            if values.get(rid) != old_or_capacity:
                return 0
            values[rid] = encoded
            return 1

    def hgetall(self, key):
        self._record("hgetall", key)
        return dict(self.hashes.get(key, {}))

    def hget(self, key, rid):
        self._record("hget", key, rid)
        return self.hashes.get(key, {}).get(rid)

    def _record(self, operation, *args):
        self.operations.append((operation,) + args)
        if self.fail_operation == operation:
            raise RuntimeError("injected Redis failure")

    def setex(self, *args):
        self._record("setex", *args)

    def rpush(self, *args):
        self._record("rpush", *args)

    def expire(self, *args):
        self._record("expire", *args)

    def publish(self, *args):
        self._record("publish", *args)
        return 1

    def lpop(self, queue):
        self._record("lpop", queue)
        values = self.queues.get(queue, [])
        return values.pop(0) if values else None

    def pubsub(self, **kwargs):
        self._record("pubsub", kwargs)
        return self.subscription

    def close(self):
        self.closed = True


class FakeRawClient:
    def __init__(self, route):
        self.route = route
        self.account_id = ACCOUNT
        self.mailbox = SimpleNamespace(session_id=route + "-sid", server_pid=2852, closed=False)
        self.poisoned = False
        self.calls = []
        self.result = None
        self.failure = None
        self.change_response = None
        self.closed = False
        self.entered = None
        self.release = None
        self.allow_order_methods = False
        self.runtime_policy = "BOUNDED"
        self.additional_capabilities = frozenset()

    def send_request(self, request, timeout_seconds):
        self.calls.append((copy.deepcopy(request), timeout_seconds))
        if self.entered is not None:
            self.entered.set()
            if not self.release.wait(2):
                raise RuntimeError("test did not release bounded request")
        if self.failure is not None:
            raise self.failure
        if request["method"] == "ping":
            data = dict(pong=True, account_id=ACCOUNT, allow_order_methods=self.allow_order_methods,
                        session_id=self.mailbox.session_id, native_pid=2852,
                        transport="shm_rpc" if self.route == "execution" else "shm_readonly",
                        runtime_policy=self.runtime_policy,
                        source_deadline_unix=None, max_seconds=0, max_requests=0, windows_session_id=1,
                        capabilities=sorted((EXECUTION_METHODS if self.route == "execution" else QUOTE_METHODS)
                                            | self.additional_capabilities
                                            | (WRITE_METHODS if self.allow_order_methods else frozenset())))
            if self.route == "execution":
                data["transport"] = "shm_rpc"
        elif self.result is not None:
            data = copy.deepcopy(self.result)
        elif request["method"] == "get_asset":
            data = {"asset": {"cash": 1000}, "source_evidence": {"complete": True, "status": "MISMATCH"}}
        elif request["method"] in QUOTE_METHODS:
            data = {"000001.SZ": {"lastPrice": 10}}
        else:
            data = [{"order_id": "native-1"}]
        response = dict(schema_version=1, request_id=request["request_id"],
                        account_id=ACCOUNT, method=request["method"], session_id=self.mailbox.session_id,
                        sequence=len(self.calls), ok=True, data=data, error="", server_error="",
                        handled_at="2026-10-17T07:00:00")
        if self.change_response is not None:
            response.update(self.change_response)
        return response

    def close(self):
        self.closed = self.mailbox.closed = True


def request(method="get_positions", rid="request-1", params=None):
    value = dict(schema_version=1, request_id=rid, account_id=ACCOUNT, method=method,
                params={} if params is None else params,
                reply_key="bigqmt:rpc:resp:%s:%s" % (ACCOUNT, rid),
                reply_list="bigqmt:rpc:respq:%s:%s" % (ACCOUNT, rid),
                reply_channel="bigqmt:rpc:resp:%s:%s" % (ACCOUNT, rid), ttl_seconds=9)
    if method in WRITE_METHODS:
        value["expected_source_binding"] = dict(read_session_id="readonly-sid", execution_session_id="execution-sid",
                                                native_pid=2852, windows_session_id=1)
    return value


class RedisShmReadOnlyProxyTest(unittest.TestCase):
    def make_proxy(self, *, verify=True, capacity=1000, deadline=None, redis=None,
                   policy="BOUNDED", read_capacity=1000, write_capacity=1000, market=False):
        redis = redis or FakeRedis()
        transport = RedisTransport(redis, account_id=ACCOUNT)
        readonly, execution = FakeRawClient("readonly"), FakeRawClient("execution")
        if policy != "BOUNDED":
            capacity = 0
            readonly.runtime_policy = "PERSISTENT_READONLY"
            execution.runtime_policy = policy
            execution.allow_order_methods = policy == "PERSISTENT_SIMULATION_READ_WRITE"
        if market:
            readonly.additional_capabilities = MARKET_RPC_METHODS
        proxy = RedisShmReadOnlyProxy(
            transport, readonly, execution, expected_readonly_session_id="readonly-sid",
            expected_execution_session_id="execution-sid", expected_native_pid=2852,
            timeout_seconds=5, max_requests=capacity, deadline_unix=deadline,
            runtime_policy=policy, allow_order_methods=execution.allow_order_methods,
            read_cache_capacity=read_capacity, write_ledger_capacity=write_capacity)
        if verify:
            proxy.verify_identity()
        return proxy, redis, readonly, execution

    def test_constructor_is_inert_and_two_handshakes_are_required(self):
        proxy, redis, readonly, execution = self.make_proxy(verify=False)
        self.assertEqual(redis.operations, [])
        self.assertEqual(readonly.calls + execution.calls, [])
        with self.assertRaises(ShmProtocolError):
            proxy.process_request(request())
        proof = proxy.verify_identity()
        self.assertTrue(proxy.verified)
        self.assertEqual(set(proof), {"readonly", "execution"})
        self.assertEqual(redis.operations, [])
        self.assertEqual(len(readonly.calls), 1)
        self.assertEqual(len(execution.calls), 1)
        with self.assertRaises(ShmProtocolError):
            proxy.verify_identity()

    def test_two_redis_response_clients_are_rejected_before_any_io(self):
        redis, other = FakeRedis(), FakeRedis()
        transport = RedisTransport(redis, account_id=ACCOUNT, response_redis_client=other)
        with self.assertRaises(ValueError):
            RedisShmReadOnlyProxy(
                transport, FakeRawClient("readonly"), FakeRawClient("execution"),
                expected_readonly_session_id="readonly-sid", expected_execution_session_id="execution-sid",
                expected_native_pid=2852)
        self.assertEqual(redis.operations + other.operations, [])

    def test_candidate_rejects_other_account_even_when_all_injected_accounts_match(self):
        redis = FakeRedis()
        transport = RedisTransport(redis, account_id="other-account")
        readonly, execution = FakeRawClient("readonly"), FakeRawClient("execution")
        readonly.account_id = execution.account_id = "other-account"
        with self.assertRaisesRegex(ValueError, "55013746"):
            RedisShmReadOnlyProxy(
                transport, readonly, execution, account_id="other-account",
                expected_readonly_session_id="readonly-sid", expected_execution_session_id="execution-sid",
                expected_native_pid=2852)
        self.assertEqual(redis.operations, [])
        self.assertEqual(readonly.calls + execution.calls, [])

    def test_custom_receive_queue_or_channel_is_rejected_before_io(self):
        for kwargs in ({"request_queue_template": "bigqmt:rpc:queue:LIVE"},
                       {"request_channel_template": "unrelated:channel"}):
            with self.subTest(kwargs=kwargs):
                redis = FakeRedis()
                transport = RedisTransport(redis, account_id=ACCOUNT, **kwargs)
                with self.assertRaisesRegex(ValueError, "standard simulation request"):
                    RedisShmReadOnlyProxy(
                        transport, FakeRawClient("readonly"), FakeRawClient("execution"),
                        expected_readonly_session_id="readonly-sid", expected_execution_session_id="execution-sid",
                        expected_native_pid=2852)
                self.assertEqual(redis.operations, [])

    def test_absolute_deadline_validation_and_expired_identity_are_inert(self):
        for deadline in (True, 0, -1, float("inf"), float("nan")):
            with self.subTest(deadline=deadline):
                with self.assertRaises(ValueError):
                    self.make_proxy(verify=False, deadline=deadline)
        with mock.patch("bigqmt_signal_trader.redis_shm_proxy.time.time", return_value=1000):
            proxy, redis, readonly, execution = self.make_proxy(verify=False, deadline=999)
            with self.assertRaises(ShmProtocolError):
                proxy.verify_identity()
            self.assertTrue(proxy.stopped)
            self.assertEqual(proxy.stop_reason, "ABSOLUTE_DEADLINE_EXPIRED")
            self.assertEqual(readonly.calls + execution.calls, [])
            self.assertEqual(redis.operations, [])

    def test_absolute_deadline_clamps_ping_and_prevents_second_lane_after_expiry(self):
        clock = [1000]
        with mock.patch("bigqmt_signal_trader.redis_shm_proxy.time.time", side_effect=lambda: clock[0]):
            proxy, _, readonly, execution = self.make_proxy(verify=False, deadline=1002)
            original_send = readonly.send_request
            def consume_budget(request, timeout_seconds):
                response = original_send(request, timeout_seconds)
                clock[0] = 1002
                return response
            readonly.send_request = consume_budget
            with self.assertRaises(ShmProtocolError):
                proxy.verify_identity()
            sent, timeout = readonly.calls[0]
            self.assertEqual(sent["read_expires_at_unix"], 1002)
            self.assertEqual(timeout, 2)
            self.assertEqual(execution.calls, [])
            self.assertTrue(proxy.stopped)

    def test_absolute_deadline_clamps_business_and_cache_cannot_reopen_expired_session(self):
        clock = [1000]
        with mock.patch("bigqmt_signal_trader.redis_shm_proxy.time.time", side_effect=lambda: clock[0]):
            proxy, _, _, execution = self.make_proxy(deadline=1002)
            original = request()
            original["read_expires_at_unix"] = 1005
            confirmed = proxy.process_request(original)
            sent, timeout = execution.calls[-1]
            self.assertEqual(sent["read_expires_at_unix"], 1002)
            self.assertEqual(timeout, 2)
            clock[0] = 1002
            with self.assertRaises(ShmProtocolError):
                proxy.process_request(original)
            self.assertEqual(len(execution.calls), 2)
            self.assertTrue(proxy.stopped)
            self.assertEqual(proxy.get_cached_response(original["request_id"]), confirmed)

    def test_absolute_deadline_rechecked_after_redis_pop_before_native_forward(self):
        clock = [1000]
        with mock.patch("bigqmt_signal_trader.redis_shm_proxy.time.time", side_effect=lambda: clock[0]):
            proxy, redis, readonly, execution = self.make_proxy(deadline=1002)
            redis.queues[proxy.transport.request_queue] = [json.dumps(request())]
            original_lpop = redis.lpop
            def expired_pop(queue):
                value = original_lpop(queue)
                clock[0] = 1002
                return value
            redis.lpop = expired_pop
            with self.assertRaises(ShmProtocolError):
                proxy.poll_once()
            self.assertEqual(len(readonly.calls) + len(execution.calls), 2)
            self.assertFalse(any(op[0] in ("setex", "rpush", "publish") for op in redis.operations))
            self.assertTrue(proxy.stopped)

    def test_original_b64_bytes_fanout_and_identity_are_preserved(self):
        proxy, redis, readonly, execution = self.make_proxy()
        original = request("query_orders", params={"cancelable_only": True, "strategy_name": "old"})
        response = proxy.process_raw(encode_rpc_request_payload(original).encode("utf-8"))
        forwarded, timeout = execution.calls[-1]
        self.assertTrue(response["ok"])
        self.assertEqual(response["data"], [{"order_id": "native-1"}])
        self.assertEqual(forwarded["request_id"], original["request_id"])
        self.assertEqual(forwarded["params"], original["params"])
        self.assertEqual(forwarded["schema_version"], 1)
        self.assertTrue(0 < timeout <= 5)
        self.assertEqual(len(readonly.calls), 1)
        self.assertEqual([op[0] for op in redis.operations], ["setex", "rpush", "expire", "publish"])
        self.assertEqual(redis.operations[0][1:3], (original["reply_key"], 9))
        self.assertEqual(redis.operations[2][1:], (original["reply_list"], 9))
        wire = json.loads(redis.operations[0][3])
        for field in ("schema_version", "request_id", "account_id", "method", "ok", "data", "error", "server_error", "handled_at"):
            self.assertEqual(wire[field], response[field])

    def test_quote_and_execution_routes_are_exact_restricted_subset(self):
        proxy, _, readonly, execution = self.make_proxy()
        for index, method in enumerate(sorted(EXECUTION_METHODS | QUOTE_METHODS)):
            params = {"code": "000001.SZ"} if method in QUOTE_METHODS else {}
            if method == "poll_execution_events":
                params["after_sequence"] = 0
            self.assertTrue(proxy.process_request(request(method, rid="r%d" % index, params=params))["ok"])
        self.assertEqual([call[0]["method"] for call in readonly.calls[1:]], sorted(QUOTE_METHODS))
        self.assertEqual([call[0]["method"] for call in execution.calls[1:]], sorted(EXECUTION_METHODS))

    def test_asset_complete_unwrap_preserves_mismatch_evidence(self):
        proxy, _, _, execution = self.make_proxy()
        response = proxy.process_request(request("get_asset"))
        self.assertTrue(response["ok"])
        self.assertEqual(response["data"], {"cash": 1000})
        self.assertEqual(response["source_evidence"], {"complete": True, "status": "MISMATCH"})
        self.assertEqual(execution.calls[-1][0]["method"], "get_asset")

    def test_asset_incomplete_keeps_partial_payload_and_false_result(self):
        proxy, _, _, execution = self.make_proxy()
        execution.result = {"asset": {"cash": None}, "source_evidence": {"complete": False, "status": "PARTIAL"}}
        response = proxy.process_request(request("get_asset"))
        self.assertFalse(response["ok"])
        self.assertEqual(response["data"], execution.result)
        self.assertEqual(response["server_error"], "ASSET_SOURCE_INCOMPLETE")
        self.assertFalse(proxy.stopped)

    def test_native_failure_keeps_partial_source_evidence(self):
        proxy, _, _, execution = self.make_proxy()
        execution.result = {"positions": {"000001.SZ": {"volume": 100}}, "source_evidence": {"complete": False}}
        execution.change_response = {"ok": False, "error": "incomplete native rows", "server_error": "POSITION_INCOMPLETE"}
        response = proxy.process_request(request(params={"include_source_evidence": True}))
        self.assertFalse(response["ok"])
        self.assertEqual(response["data"], execution.result)
        self.assertEqual(response["server_error"], "POSITION_INCOMPLETE")
        self.assertTrue(execution.calls[-1][0]["params"]["include_source_evidence"])

    def test_orders_download_aliases_subscriptions_and_formula_are_rejected_without_forward(self):
        for method in ("submit_order", "cancel_order", "submit_orders_batch", "order_stock", "download_history_data",
                       "sync_positions", "subscribe_quote", "query_stock_positions", "get_local_data", "call_formula"):
            with self.subTest(method=method):
                proxy, _, readonly, execution = self.make_proxy()
                response = proxy.process_request(request(method))
                self.assertFalse(response["ok"])
                self.assertEqual(len(readonly.calls) + len(execution.calls), 2)
        proxy, _, readonly, execution = self.make_proxy()
        original = request()
        original["use_formula"] = True
        self.assertFalse(proxy.process_request(original)["ok"])
        self.assertEqual(len(readonly.calls) + len(execution.calls), 2)

    def test_envelope_nested_account_and_quote_parameters_are_validated(self):
        variants = [
            {"schema_version": True}, {"schema_version": 2}, {"account_id": "other"},
            {"params": {"account_id": "other"}}, {"params": {"account": {"accountID": "other"}}},
            {"params": []}, {"method": "poll_execution_events", "params": {"after_sequence": -1}},
            {"method": "get_ticks", "params": {"codes": ["000001.SZ"], "types": ["price"]}},
            {"method": "get_ticks", "params": {"code": "bad"}},
        ]
        for updates in variants:
            with self.subTest(updates=updates):
                proxy, _, readonly, execution = self.make_proxy()
                original = request()
                original.update(updates)
                self.assertFalse(proxy.process_request(original)["ok"])
                self.assertEqual(len(readonly.calls) + len(execution.calls), 2)

    def test_invalid_reply_target_ttl_and_rid_do_not_write_or_forward(self):
        for updates in ({"reply_key": "other"}, {"reply_list": "other"}, {"reply_channel": "other"},
                        {"ttl_seconds": 0}, {"ttl_seconds": 3601}, {"ttl_seconds": True},
                        {"request_id": "has space"}, {"request_id": ""}):
            with self.subTest(updates=updates):
                proxy, redis, readonly, execution = self.make_proxy()
                original = request()
                original.update(updates)
                with self.assertRaises(ShmProtocolError):
                    proxy.process_request(original)
                self.assertEqual(redis.operations, [])
                self.assertEqual(len(readonly.calls) + len(execution.calls), 2)

    def test_missing_reply_targets_preserve_original_key_channel_only_default(self):
        proxy, redis, _, _ = self.make_proxy()
        original = request()
        for field in ("reply_key", "reply_list", "reply_channel", "ttl_seconds"):
            original.pop(field)
        self.assertTrue(proxy.process_request(original)["ok"])
        self.assertEqual([op[0] for op in redis.operations], ["setex", "publish"])
        self.assertEqual(redis.operations[0][2], 60)

    def test_expired_deadline_does_not_forward_and_future_deadline_only_tightens(self):
        proxy, _, _, execution = self.make_proxy()
        original = request()
        original["read_expires_at_unix"] = time.time() - 1
        self.assertFalse(proxy.process_request(original)["ok"])
        self.assertEqual(len(execution.calls), 1)
        deadline = time.time() + 2
        original["read_expires_at_unix"] = deadline
        self.assertTrue(proxy.process_request(original)["ok"])
        self.assertEqual(execution.calls[-1][0]["read_expires_at_unix"], deadline)
        self.assertTrue(0 < execution.calls[-1][1] <= 2)
        original = request(rid="far-future")
        original["read_expires_at_unix"] = time.time() + 3600
        self.assertTrue(proxy.process_request(original)["ok"])
        self.assertLess(execution.calls[-1][0]["read_expires_at_unix"], original["read_expires_at_unix"])

    def test_confirmed_duplicate_replies_from_cache_and_conflict_does_not_pollute(self):
        proxy, _, _, execution = self.make_proxy()
        original = request()
        confirmed = proxy.process_request(original)
        self.assertEqual(proxy.process_request(original), confirmed)
        self.assertEqual(len(execution.calls), 2)
        conflict = request(params={"include_source_evidence": True})
        self.assertFalse(proxy.process_request(conflict)["ok"])
        self.assertEqual(proxy.get_cached_response("request-1"), confirmed)
        self.assertEqual(proxy.process_request(original), confirmed)
        self.assertEqual(len(execution.calls), 2)
        self.assertEqual(proxy.processed_count, 1)

    def test_confirmed_duplicate_with_expired_deadline_returns_original_cache_without_native_read(self):
        proxy, _, _, execution = self.make_proxy()
        original = request()
        confirmed = proxy.process_request(original)
        original["read_expires_at_unix"] = time.time() - 1
        self.assertEqual(proxy.process_request(original), confirmed)
        self.assertEqual(proxy.get_cached_response(original["request_id"]), confirmed)
        self.assertEqual(len(execution.calls), 2)

    def test_timeout_unknown_is_sticky_and_never_resubmitted(self):
        proxy, redis, _, execution = self.make_proxy()
        execution.failure = ShmTimeoutError("unconfirmed request; do not resubmit")
        original = request()
        response = proxy.process_request(original)
        self.assertFalse(response["ok"])
        self.assertEqual(response["status"], "UNKNOWN")
        self.assertFalse(response["automatic_retry"])
        self.assertTrue(proxy.stopped)
        operations = len(redis.operations)
        with self.assertRaises(ShmProtocolError):
            proxy.process_request(original)
        self.assertEqual(len(redis.operations), operations)
        self.assertEqual(len(execution.calls), 2)
        self.assertEqual(proxy.get_cached_response(original["request_id"]), response)

    def test_changed_sid_pid_poison_and_false_response_identity_seal_proxy(self):
        for change in ("sid", "pid", "poison", "response-account", "response-rid"):
            with self.subTest(change=change):
                proxy, _, _, execution = self.make_proxy()
                if change == "sid":
                    execution.mailbox.session_id = "new-session"
                elif change == "pid":
                    execution.mailbox.server_pid += 1
                elif change == "poison":
                    execution.poisoned = True
                elif change == "response-account":
                    execution.change_response = {"account_id": "other"}
                else:
                    execution.change_response = {"request_id": "other"}
                response = proxy.process_request(request())
                self.assertEqual(response["status"], "UNKNOWN")
                self.assertTrue(proxy.stopped)

    def test_unused_quote_lane_changed_sid_stops_execution_lane_without_forward(self):
        proxy, _, readonly, execution = self.make_proxy()
        readonly.mailbox.session_id = "new-readonly-sid"
        response = proxy.process_request(request())
        self.assertEqual(response["status"], "UNKNOWN")
        self.assertTrue(proxy.stopped)
        self.assertEqual(len(execution.calls), 1)

    def test_changed_sid_on_duplicate_seals_and_preserves_confirmed_local_cache(self):
        proxy, _, readonly, execution = self.make_proxy()
        original = request()
        confirmed = proxy.process_request(original)
        readonly.mailbox.session_id = "new-readonly-sid"
        response = proxy.process_request(original)
        self.assertEqual(response["status"], "UNKNOWN")
        self.assertTrue(proxy.stopped)
        self.assertEqual(proxy.get_cached_response(original["request_id"]), confirmed)
        self.assertEqual(len(execution.calls), 2)

    def test_handshake_wrong_pid_or_enabled_orders_blocks_all_business(self):
        for updates in ({"native_pid": 9999}, {"allow_order_methods": True}, {"capabilities": []}):
            with self.subTest(updates=updates):
                proxy, redis, readonly, _ = self.make_proxy(verify=False)
                original_send = readonly.send_request
                def changed_send(request, timeout_seconds):
                    response = original_send(request, timeout_seconds)
                    response["data"].update(updates)
                    return response
                readonly.send_request = changed_send
                with self.assertRaises(ShmProtocolError):
                    proxy.verify_identity()
                self.assertFalse(proxy.verified)
                self.assertTrue(proxy.stopped)
                self.assertEqual(redis.operations, [])

    def test_partial_publish_failure_keeps_confirmed_local_cache_but_does_not_republish(self):
        proxy, redis, _, execution = self.make_proxy()
        redis.fail_operation = "rpush"
        original = request()
        with self.assertRaises(RuntimeError):
            proxy.process_request(original)
        self.assertTrue(proxy.get_cached_response(original["request_id"])["ok"])
        self.assertEqual([op[0] for op in redis.operations], ["setex", "rpush"])
        self.assertTrue(proxy.stopped)
        operations = len(redis.operations)
        with self.assertRaises(ShmProtocolError):
            proxy.process_request(original)
        self.assertEqual(len(redis.operations), operations)
        self.assertEqual(len(execution.calls), 2)

    def test_poll_pubsub_then_queue_is_nonblocking_and_capacity_stops_before_lpop(self):
        proxy, redis, _, execution = self.make_proxy(capacity=2)
        redis.subscription.messages = [{"type": "message", "data": json.dumps(request(rid="pub"))}]
        redis.queues[proxy.transport.request_queue] = [encode_rpc_request_payload(request(rid="queue")), "untouched"]
        self.assertTrue(proxy.poll_once()["ok"])
        self.assertTrue(proxy.poll_once()["ok"])
        self.assertEqual(redis.subscription.timeouts, [0, 0])
        before = list(redis.operations)
        self.assertIsNone(proxy.poll_once())
        self.assertTrue(proxy.stopped)
        self.assertEqual(redis.operations, before)
        self.assertEqual(redis.queues[proxy.transport.request_queue], ["untouched"])
        self.assertEqual(len(execution.calls), 3)

    def test_concurrent_poll_is_rejected_before_consuming_queue(self):
        proxy, redis, _, execution = self.make_proxy()
        entered, release = threading.Event(), threading.Event()
        redis.queues[proxy.transport.request_queue] = [json.dumps(request())]
        original_get_message = redis.subscription.get_message
        def blocked_get_message(timeout):
            entered.set()
            if not release.wait(2):
                raise RuntimeError("test did not release bounded poll")
            return original_get_message(timeout)
        redis.subscription.get_message = blocked_get_message
        responses = []
        thread = threading.Thread(target=lambda: responses.append(proxy.poll_once()))
        thread.start()
        self.assertTrue(entered.wait(1))
        try:
            with self.assertRaises(ShmProtocolError):
                proxy.poll_once()
            with self.assertRaises(ShmProtocolError):
                proxy.close()
            self.assertEqual(len(redis.queues[proxy.transport.request_queue]), 1)
        finally:
            release.set()
            thread.join(2)
        self.assertTrue(responses[0]["ok"])
        self.assertEqual(len(execution.calls), 2)

    def test_inflight_request_is_serial_and_cannot_be_closed_or_forwarded_again(self):
        proxy, _, _, execution = self.make_proxy()
        execution.entered, execution.release = threading.Event(), threading.Event()
        responses = []
        thread = threading.Thread(target=lambda: responses.append(proxy.process_request(request())))
        thread.start()
        self.assertTrue(execution.entered.wait(1))
        try:
            with self.assertRaises(ShmProtocolError):
                proxy.process_request(request(rid="second"))
            with self.assertRaises(ShmProtocolError):
                proxy.close()
        finally:
            execution.release.set()
            thread.join(2)
        self.assertFalse(thread.is_alive())
        self.assertTrue(responses[0]["ok"])
        self.assertEqual(len(execution.calls), 2)

    def test_close_releases_owned_shm_and_subscription_preserves_injected_redis_pool(self):
        proxy, redis, readonly, execution = self.make_proxy()
        proxy.poll_once()
        proxy.close()
        proxy.close()
        self.assertTrue(proxy.stopped)
        self.assertTrue(readonly.closed)
        self.assertTrue(execution.closed)
        self.assertTrue(redis.subscription.closed)
        self.assertFalse(redis.closed)
        self.assertFalse(any(op[0] == "delete" for op in redis.operations))

    def test_unified_ping_carries_distinct_actual_sources_and_real_capabilities(self):
        proxy, _, readonly, execution = self.make_proxy(policy="PERSISTENT_SIMULATION_READ_WRITE", market=True)
        response = proxy.process_request(request("ping"))
        data = response["data"]
        self.assertEqual(data["transport"], "redis_shm")
        self.assertEqual(data["rpc_revision"], "external-redis-dual-shm-v1")
        self.assertEqual(data["session_id"], execution.mailbox.session_id)
        self.assertEqual(data["read_source"]["session_id"], readonly.mailbox.session_id)
        self.assertFalse(data["read_source"]["allow_order_methods"])
        self.assertTrue(data["execution_source"]["allow_order_methods"])
        self.assertEqual(set(data["capabilities"]), EXECUTION_METHODS | QUOTE_METHODS | MARKET_RPC_METHODS | WRITE_METHODS)
        self.assertTrue(data["write_ready"])

    def test_persistent_policy_cannot_silently_open_writes_or_accept_bounded_budgets(self):
        transport = RedisTransport(FakeRedis(), account_id=ACCOUNT)
        base = dict(expected_readonly_session_id="readonly-sid", expected_execution_session_id="execution-sid",
                    expected_native_pid=2852, max_requests=0)
        for changes in ({"runtime_policy": "PERSISTENT_SIMULATION_READ_WRITE"},
                        {"runtime_policy": "PERSISTENT_READONLY", "allow_order_methods": True},
                        {"runtime_policy": "PERSISTENT_READONLY", "deadline_unix": time.time() + 1},
                        {"runtime_policy": "PERSISTENT_READONLY", "max_requests": 1}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                RedisShmReadOnlyProxy(transport, FakeRawClient("readonly"), FakeRawClient("execution"),
                                     **dict(base, **changes))

    def test_persistent_identity_requires_actual_native_policy_and_windows_session(self):
        proxy, _, readonly, _ = self.make_proxy(verify=False, policy="PERSISTENT_READONLY")
        readonly.runtime_policy = "BOUNDED"
        with self.assertRaises(ShmProtocolError):
            proxy.verify_identity()
        self.assertFalse(proxy.verified)
        proxy, _, readonly, _ = self.make_proxy(verify=False)
        proxy.expected_windows_session_id = 2
        with self.assertRaises(ShmProtocolError):
            proxy.verify_identity()
        self.assertEqual(len(readonly.calls), 1)

    def test_asset_explicit_evidence_preserves_native_wrapper_for_real_redis_client(self):
        proxy, _, _, _ = self.make_proxy()
        response = proxy.process_request(request("get_asset", params={"include_source_evidence": True}))
        self.assertEqual(response["data"], {"asset": {"cash": 1000},
                                           "source_evidence": {"complete": True, "status": "MISMATCH"}})

    def test_market_methods_only_route_when_actual_source_declares_capability(self):
        proxy, _, readonly, execution = self.make_proxy(policy="PERSISTENT_READONLY", market=True)
        for method, params in (
                ("get_local_data", {"code": "000001.SZ", "period": "1d", "start_time": "20261016000000",
                                    "end_time": "20261016235959", "count": 1}),
                ("subscribe_quote", {"code": "000001.SZ"}),
                ("poll_quote_events", {"after_sequence": 0}),
                ("unsubscribe_quote", {"subscription_id": "native-1"})):
            self.assertTrue(proxy.process_request(request(method, rid=method, params=params))["ok"])
        self.assertEqual([call[0]["method"] for call in readonly.calls[1:]],
                         ["get_local_data", "subscribe_quote", "poll_quote_events", "unsubscribe_quote"])
        self.assertEqual(len(execution.calls), 1)
        disabled, _, disabled_read, _ = self.make_proxy(policy="PERSISTENT_READONLY")
        self.assertFalse(disabled.process_request(request("subscribe_quote", params={"code": "000001.SZ"}))["ok"])
        self.assertEqual(len(disabled_read.calls), 1)

    def test_persistent_expired_confirmed_reads_reclaim_bounded_cache_after_1100_requests(self):
        clock = [1000.0]
        with mock.patch("bigqmt_signal_trader.redis_shm_proxy.time.time", side_effect=lambda: clock[0]):
            proxy, _, _, execution = self.make_proxy(policy="PERSISTENT_READONLY", read_capacity=2)
            for index in range(1100):
                self.assertTrue(proxy.process_request(request(rid="read-%d" % index))["ok"])
                clock[0] += 6
            self.assertEqual(proxy.processed_count, 1100)
            self.assertLessEqual(len(proxy._replays), 2)
            self.assertEqual(len(execution.calls), 1101)
            self.assertFalse(proxy.stopped)

    def test_persistent_active_read_capacity_rejects_without_evicting_or_native_forward(self):
        proxy, _, _, execution = self.make_proxy(policy="PERSISTENT_READONLY", read_capacity=1)
        first = proxy.process_request(request(rid="first"))
        self.assertFalse(proxy.process_request(request(rid="second"))["ok"])
        self.assertEqual(proxy.get_cached_response("first"), first)
        self.assertEqual(len(execution.calls), 2)
        self.assertFalse(proxy.stopped)

    def test_confirmed_write_once_and_restart_reuses_exact_result_without_native_resubmit(self):
        proxy, redis, readonly, execution = self.make_proxy(policy="PERSISTENT_SIMULATION_READ_WRITE")
        execution.result = {"status": "SUBMITTED", "order_sys_id": "native-1"}
        original = request("submit_order", params={"stock_code": "000001.SZ", "volume": 100})
        response = proxy.process_request(original)
        self.assertEqual(proxy.process_request(original), response)
        self.assertEqual(len(execution.calls), 2)
        self.assertEqual(len(readonly.calls), 1)
        proxy.close()
        restarted, _, _, fresh_execution = self.make_proxy(redis=redis, policy="PERSISTENT_SIMULATION_READ_WRITE")
        self.assertEqual(restarted.process_request(original), response)
        self.assertEqual(len(fresh_execution.calls), 1)
        ledger = next(iter(redis.hashes.values()))
        self.assertEqual(json.loads(ledger[original["request_id"]])["state"], "CONFIRMED")
        self.assertFalse(any(op[0] in ("delete", "expire") and "write_ledger" in str(op) for op in redis.operations))

    def test_confirmed_write_parameter_or_source_binding_conflict_never_replays_native(self):
        proxy, redis, _, execution = self.make_proxy(policy="PERSISTENT_SIMULATION_READ_WRITE")
        execution.result = {"status": "SUBMITTED", "order_sys_id": "native-1"}
        original = request("submit_order", params={"stock_code": "000001.SZ", "volume": 100})
        proxy.process_request(original)
        restarted, _, _, fresh_execution = self.make_proxy(redis=redis, policy="PERSISTENT_SIMULATION_READ_WRITE")
        conflict = request("submit_order", params={"stock_code": "000001.SZ", "volume": 200})
        self.assertEqual(restarted.process_request(conflict)["status"], "UNKNOWN")
        self.assertEqual(len(fresh_execution.calls), 1)
        source_changed, _, _, changed_execution = self.make_proxy(redis=redis, policy="PERSISTENT_SIMULATION_READ_WRITE")
        source_changed._sessions["readonly"] = "new-read-sid"
        source_changed._clients["readonly"].mailbox.session_id = "new-read-sid"
        self.assertEqual(source_changed.process_request(original)["status"], "REJECTED")
        self.assertEqual(len(changed_execution.calls), 1)
        # 使用新来源绑定仍不能让已确认的旧 RID 在新来源重复写入。
        rebound = copy.deepcopy(original)
        rebound["expected_source_binding"]["read_session_id"] = "new-read-sid"
        self.assertEqual(source_changed.process_request(rebound)["status"], "UNKNOWN")
        self.assertEqual(len(changed_execution.calls), 1)

    def test_confirmed_unknown_login_state_keeps_read_channel_open_without_proving_login(self):
        proxy, _, _, execution = self.make_proxy(policy="PERSISTENT_SIMULATION_READ_WRITE")
        execution.result = dict(status="UNKNOWN", account_id=ACCOUNT,
                                session_id="execution-sid", native_pid=2852,
                                source="native_account_callback", fresh=False,
                                positive_contract_verified=False,
                                reason="TRADE_LOGIN_CALLBACK_NOT_OBSERVED")
        response = proxy.process_request(request("get_trade_login_state", rid="login-state"))
        self.assertTrue(response["ok"])
        self.assertEqual(response["data"]["status"], "UNKNOWN")
        self.assertFalse(response["data"]["positive_contract_verified"])
        self.assertFalse(proxy.stopped)
        execution.result = [{"symbol": "000001.SZ", "volume": 100}]
        self.assertTrue(proxy.process_request(request("get_positions", rid="positions-after-login"))["ok"])

    def test_unknown_login_with_untrusted_identity_still_seals_channel(self):
        for changes in ({"account_id": "other"}, {"session_id": "old-sid"},
                        {"native_pid": True}, {"source": "other"},
                        {"positive_contract_verified": True}):
            with self.subTest(changes=changes):
                proxy, _, _, execution = self.make_proxy(policy="PERSISTENT_SIMULATION_READ_WRITE")
                execution.result = dict(status="UNKNOWN", account_id=ACCOUNT,
                                        session_id="execution-sid", native_pid=2852,
                                        source="native_account_callback",
                                        positive_contract_verified=False)
                execution.result.update(changes)
                response = proxy.process_request(request("get_trade_login_state"))
                self.assertFalse(response["ok"])
                self.assertEqual(response["server_error"], "SHM_RESULT_UNKNOWN")
                self.assertTrue(proxy.stopped)

    def test_unrelated_unknown_read_still_seals_channel(self):
        proxy, _, _, execution = self.make_proxy(policy="PERSISTENT_SIMULATION_READ_WRITE")
        execution.result = dict(status="UNKNOWN", account_id=ACCOUNT,
                                session_id="execution-sid", native_pid=2852,
                                source="native_account_callback", positive_contract_verified=False)
        response = proxy.process_request(request("get_positions"))
        self.assertFalse(response["ok"])
        self.assertEqual(response["server_error"], "SHM_RESULT_UNKNOWN")
        self.assertTrue(proxy.stopped)

    def test_native_write_unknown_blocks_new_writes_but_read_queries_and_events_continue(self):
        proxy, redis, _, execution = self.make_proxy(policy="PERSISTENT_SIMULATION_READ_WRITE")
        execution.result = {"status": "UNKNOWN", "automatic_retry": False, "order_sys_id": "native-1"}
        original = request("cancel_order", params={"order_sys_id": "native-1"})
        response = proxy.process_request(original)
        self.assertTrue(response["ok"])
        self.assertEqual(response["status"], "UNKNOWN")
        self.assertFalse(proxy.stopped)
        self.assertFalse(proxy.identity_snapshot()["write_ready"])
        execution.result = [{"order_sys_id": "native-1", "status": "CANCELED"}]
        self.assertTrue(proxy.process_request(request("query_orders", rid="read-after-unknown"))["ok"])
        self.assertTrue(proxy.process_request(request("poll_execution_events", rid="events-after-unknown",
                                                      params={"after_sequence": 0}))["ok"])
        count = len(execution.calls)
        self.assertFalse(proxy.process_request(request("submit_order", rid="new-write"))["ok"])
        self.assertEqual(len(execution.calls), count)
        proxy.close()
        restarted, _, _, fresh_execution = self.make_proxy(redis=redis, policy="PERSISTENT_SIMULATION_READ_WRITE")
        self.assertFalse(restarted.identity_snapshot()["write_ready"])
        self.assertEqual(restarted.process_request(original)["status"], "UNKNOWN")
        self.assertEqual(len(fresh_execution.calls), 1)
        self.assertFalse(restarted.stopped)

    def test_write_timeout_reservation_survives_restart_and_is_never_resubmitted(self):
        proxy, redis, _, execution = self.make_proxy(policy="PERSISTENT_SIMULATION_READ_WRITE")
        execution.failure = ShmTimeoutError("write may have run")
        original = request("submit_order")
        self.assertEqual(proxy.process_request(original)["status"], "UNKNOWN")
        self.assertTrue(proxy.stopped)
        restarted, _, _, fresh_execution = self.make_proxy(redis=redis, policy="PERSISTENT_SIMULATION_READ_WRITE")
        self.assertEqual(restarted.process_request(original)["status"], "UNKNOWN")
        self.assertEqual(len(fresh_execution.calls), 1)

    def test_write_reservation_and_settlement_failures_do_not_allow_second_native_write(self):
        for failure, native_calls in (("claim", 1), ("settle", 2)):
            with self.subTest(failure=failure):
                proxy, redis, _, execution = self.make_proxy(policy="PERSISTENT_SIMULATION_READ_WRITE")
                redis.fail_eval = failure
                original = request("submit_order")
                self.assertEqual(proxy.process_request(original)["status"], "UNKNOWN")
                self.assertEqual(len(execution.calls), native_calls)
                self.assertTrue(proxy.stopped)
                redis.fail_eval = None
                if failure == "settle":
                    restarted, _, _, fresh_execution = self.make_proxy(redis=redis, policy="PERSISTENT_SIMULATION_READ_WRITE")
                    self.assertEqual(restarted.process_request(original)["status"], "UNKNOWN")
                    self.assertEqual(len(fresh_execution.calls), 1)

    def test_partial_write_response_publish_is_persistently_unknown_after_restart(self):
        proxy, redis, _, execution = self.make_proxy(policy="PERSISTENT_SIMULATION_READ_WRITE")
        execution.result = {"status": "SUBMITTED", "order_sys_id": "native-1"}
        redis.fail_operation = "rpush"
        original = request("submit_order")
        with self.assertRaises(RuntimeError):
            proxy.process_request(original)
        self.assertEqual(json.loads(next(iter(redis.hashes.values()))["request-1"])["state"], "PUBLISH_UNCONFIRMED")
        redis.fail_operation = None
        restarted, _, _, fresh_execution = self.make_proxy(redis=redis, policy="PERSISTENT_SIMULATION_READ_WRITE")
        self.assertEqual(restarted.process_request(original)["status"], "UNKNOWN")
        self.assertEqual(len(fresh_execution.calls), 1)

    def test_lost_publish_failure_marker_keeps_prior_pending_record_and_restart_blocks_writes(self):
        proxy, redis, _, execution = self.make_proxy(policy="PERSISTENT_SIMULATION_READ_WRITE")
        execution.result = {"status": "SUBMITTED", "order_sys_id": "native-1"}
        original_eval = redis.eval
        settle_count = [0]

        def failed_second_settlement(script, *args):
            if "HLEN" not in script:
                settle_count[0] += 1
                if settle_count[0] >= 2:
                    raise RuntimeError("Redis disconnected after pending record was stored")
            return original_eval(script, *args)

        redis.eval = failed_second_settlement
        redis.fail_operation = "rpush"
        original = request("submit_order")
        with self.assertRaises(RuntimeError):
            proxy.process_request(original)
        self.assertEqual(json.loads(next(iter(redis.hashes.values()))["request-1"])["state"], "PUBLISH_PENDING")
        redis.eval, redis.fail_operation = original_eval, None
        restarted, _, _, fresh_execution = self.make_proxy(redis=redis, policy="PERSISTENT_SIMULATION_READ_WRITE")
        self.assertFalse(restarted.identity_snapshot()["write_ready"])
        response = restarted.process_request(original)
        self.assertEqual(response["status"], "UNKNOWN")
        self.assertEqual(response["data"]["order_sys_id"], "native-1")
        self.assertEqual(len(fresh_execution.calls), 1)

    def test_malformed_persistent_ledger_prevents_ready_and_all_business_consumption(self):
        redis = FakeRedis()
        redis.hashes["aqp:redis_shm:write_ledger:55013746:v1"] = {"foreign": "{}"}
        proxy, _, _, _ = self.make_proxy(redis=redis, verify=False, policy="PERSISTENT_SIMULATION_READ_WRITE")
        with self.assertRaises(ShmProtocolError):
            proxy.verify_identity()
        self.assertFalse(proxy.verified)
        self.assertTrue(proxy.stopped)
        self.assertFalse(any(operation[0] == "lpop" for operation in redis.operations))

    def test_missing_or_wrong_write_source_binding_rejects_before_ledger_or_native(self):
        variants = [None, {}, {"read_session_id": "old-read"}, {"execution_session_id": "old-execution"},
                    {"native_pid": 999}, {"windows_session_id": 2}, {"windows_session_id": True},
                    {"native_pid": True}, {"unexpected": "extra-field"}]
        for change in variants:
            with self.subTest(change=change):
                proxy, redis, readonly, execution = self.make_proxy(policy="PERSISTENT_SIMULATION_READ_WRITE")
                value = request("submit_order")
                if change is None:
                    value.pop("expected_source_binding")
                elif change == {}:
                    value["expected_source_binding"] = {}
                else:
                    value["expected_source_binding"].update(change)
                before = list(redis.operations)
                response = proxy.process_request(value)
                self.assertFalse(response["ok"])
                self.assertEqual(response["status"], "REJECTED")
                self.assertEqual(len(readonly.calls), 1)
                self.assertEqual(len(execution.calls), 1)
                self.assertFalse(any(operation[0] == "eval" for operation in redis.operations[len(before):]))
                self.assertEqual(redis.hashes, {})
                self.assertFalse(proxy.stopped)

    def test_read_explicit_source_binding_is_exact_and_old_read_wire_remains_supported(self):
        proxy, redis, readonly, execution = self.make_proxy(policy="PERSISTENT_READONLY")
        bad = request(rid="old-bound-read")
        bad["expected_source_binding"] = dict(read_session_id="old-read", execution_session_id="old-execution",
                                              native_pid=999, windows_session_id=2)
        self.assertFalse(proxy.process_request(bad)["ok"])
        self.assertEqual(len(execution.calls), 1)
        old_wire = proxy.process_request(request(rid="legacy-no-binding"))
        self.assertTrue(old_wire["ok"])
        exact = request(rid="bound-read")
        exact["expected_source_binding"] = proxy.identity_snapshot()["source_binding"]
        response = proxy.process_request(exact)
        self.assertTrue(response["ok"])
        self.assertEqual(response["source_binding"], exact["expected_source_binding"])
        self.assertEqual(proxy.get_cached_response("bound-read"), response)
        self.assertEqual(len(readonly.calls), 1)
        self.assertFalse(any(operation[0] == "eval" for operation in redis.operations))

    def test_native_response_and_ready_binding_match_actual_windows_and_distinct_sessions(self):
        proxy, _, _, execution = self.make_proxy(policy="PERSISTENT_SIMULATION_READ_WRITE")
        proof = proxy.identity_snapshot()
        expected = dict(read_session_id="readonly-sid", execution_session_id="execution-sid",
                        native_pid=2852, windows_session_id=1)
        self.assertEqual(proof["source_binding"], expected)
        execution.result = {"status": "SUBMITTED", "order_sys_id": "native-1"}
        response = proxy.process_request(request("submit_order"))
        self.assertTrue(response["ok"])
        self.assertEqual(response["source_binding"], expected)
        self.assertEqual(proxy.get_cached_response("request-1"), response)

    def test_write_capacity_and_read_reclamation_never_evict_persisted_order_record(self):
        clock = [1000.0]
        with mock.patch("bigqmt_signal_trader.redis_shm_proxy.time.time", side_effect=lambda: clock[0]):
            proxy, redis, _, execution = self.make_proxy(policy="PERSISTENT_SIMULATION_READ_WRITE", write_capacity=1)
            execution.result = {"status": "SUBMITTED", "order_sys_id": "native-1"}
            original = request("submit_order", rid="permanent-write")
            proxy.process_request(original)
            for index in range(3):
                clock[0] += 6
                proxy.process_request(request(rid="read-%d" % index))
            ledger = next(iter(redis.hashes.values()))
            self.assertIn("permanent-write", ledger)
            self.assertIsNotNone(proxy.get_cached_response("permanent-write"))
            before = len(execution.calls)
            self.assertEqual(proxy.process_request(request("submit_order", rid="overflow"))["status"], "REJECTED")
            self.assertEqual(len(execution.calls), before)
            self.assertEqual(set(ledger), {"permanent-write"})
            self.assertFalse(proxy.stopped)


if __name__ == "__main__":
    unittest.main()
