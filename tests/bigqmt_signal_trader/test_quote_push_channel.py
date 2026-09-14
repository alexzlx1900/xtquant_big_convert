import json
import os
import sys
import threading
import time
import unittest


ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

from bigqmt_signal_trader.quote_push_channel import (
    _HAS_MSGPACK,
    RedisQuotePushChannel,
    ZmqQuotePushChannel,
    decode_push_payload,
    encode_push_payload,
)


def _msgpack_packb(payload):
    import msgpack

    return msgpack.packb(payload, use_bin_type=True)


class FakePubSub:
    def __init__(self, redis_client):
        self._redis = redis_client
        self._channels = []
        self._closed = False

    def subscribe(self, *channels):
        self._channels.extend(channels)

    def get_message(self, timeout=0.1):
        # Pull one queued message for a subscribed channel, if any.
        for _ in range(50):
            for channel in self._channels:
                queue = self._redis.messages.setdefault(channel, [])
                if queue:
                    return {"type": "message", "channel": channel, "data": queue.pop(0)}
            time.sleep(0.01)
        return None

    def close(self):
        self._closed = True


class FakeRedis:
    def __init__(self):
        self.messages = {}

    def publish(self, channel, value):
        self.messages.setdefault(channel, []).append(value)
        return 1

    def pubsub(self, ignore_subscribe_messages=True):
        return FakePubSub(self)


class DisconnectOncePubSub(FakePubSub):
    def get_message(self, timeout=0.1):
        raise ConnectionError("temporary disconnect")


class DisconnectOnceRedis(FakeRedis):
    def __init__(self):
        super().__init__()
        self.pubsub_calls = 0

    def pubsub(self, ignore_subscribe_messages=True):
        self.pubsub_calls += 1
        if self.pubsub_calls == 1:
            return DisconnectOncePubSub(self)
        return FakePubSub(self)


class BlockingMessagePubSub(FakePubSub):
    def __init__(self, redis_client):
        super().__init__(redis_client)
        self.entered = threading.Event()
        self.release = threading.Event()

    def get_message(self, timeout=0.1):
        self.entered.set()
        self.release.wait(2.0)
        return {"type": "message", "channel": self._channels[0],
                "data": encode_push_payload({"combo_key": "SH", "data": {"late": 1}})}


class BlockingMessageRedis(FakeRedis):
    def __init__(self):
        super().__init__()
        self.blocking_pubsub = BlockingMessagePubSub(self)

    def pubsub(self, ignore_subscribe_messages=True):
        return self.blocking_pubsub


class PayloadCodecTest(unittest.TestCase):
    def test_roundtrip(self):
        payload = {"combo_key": "SH,SZ", "data": {"000001.SZ": {"lastPrice": 10.5}}, "ts": 1.5}
        blob = encode_push_payload(payload)
        self.assertEqual(decode_push_payload(blob), payload)

    def test_binary_blob(self):
        # Wire encoding must be bytes (msgpack or utf-8 json), not str.
        blob = encode_push_payload({"a": 1})
        self.assertIsInstance(blob, (bytes, bytearray))

    def test_decode_json_wire_with_msgpack_available(self):
        # 服务端未装 msgpack 时用 json 兜底编码;若客户端装了 msgpack,
        # msgpack.unpackb 会把 json 文本首字节 '{'(0x7B) 当整数解析并抛
        # ExtraData("unpack(b) received extra data")——真实联调中触发。
        # decode 必须识别并回退到 json。
        payload = {"combo_key": "000001.SZ", "data": {"000001.SZ": {"lastPrice": 11.19}}}
        wire = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
        self.assertIsInstance(wire, bytes)
        self.assertEqual(decode_push_payload(wire), payload)

    def test_decode_msgpack_wire_without_msgpack_available(self):
        # 反向:服务端装了 msgpack、客户端未装(仅 json 可用)。msgpack 字节流
        # 通常不是合法 utf-8,json 解码会失败——此时应显式尝试 msgpack 解码,
        # 而不是吞掉异常返回 None。
        payload = {"combo_key": "SH,SZ", "data": {"600000.SH": {"lastPrice": 9.9}}}
        if not _HAS_MSGPACK:
            self.skipTest("msgpack not installed on this client")
        wire = _msgpack_packb(payload)
        self.assertEqual(decode_push_payload(wire), payload)


class ZmqPushChannelTest(unittest.TestCase):
    def test_pub_sub_roundtrip_and_topic_filter(self):
        zmq = __import__("zmq")
        ctx = zmq.Context.instance()
        pub_addr = "inproc://quote-push-test-%d" % id(self)

        server = ZmqQuotePushChannel(bind_address=pub_addr, context=ctx)
        server.start_publisher()

        received = []
        done = threading.Event()

        client = ZmqQuotePushChannel(connect_address=pub_addr, context=ctx)

        def on_msg(topic, data):
            received.append((topic, data))
            done.set()

        client.start_subscriber(["SH,SZ"], on_msg)
        try:
            # Give the SUB a moment to connect + apply the subscription filter.
            time.sleep(0.2)
            server.publish("SH,SZ", {"000001.SZ": {"lastPrice": 10.5}})
            server.publish("SH", {"600000.SH": {"lastPrice": 9.9}})  # filtered out
            self.assertTrue(done.wait(2.0), "subscriber did not receive the SH,SZ push")
        finally:
            client.stop()
            server.stop()

        self.assertEqual(len(received), 1)
        topic, data = received[0]
        self.assertEqual(topic, "SH,SZ")
        self.assertEqual(data["000001.SZ"]["lastPrice"], 10.5)

    def test_stop_from_foreign_thread_while_sub_active_does_not_crash(self):
        """Regression: stop() used to close the SUB socket from the calling
        thread while the sub thread was still polling it -> Windows ZMQ
        signaler abort -> QMT process crash ("auto-exit"). The sub thread must
        close its own socket; repeated stop/start (topic changes) must be safe."""
        zmq = __import__("zmq")
        ctx = zmq.Context.instance()
        pub_addr = "inproc://quote-push-stop-%d" % id(self)

        server = ZmqQuotePushChannel(bind_address=pub_addr, context=ctx)
        server.start_publisher()
        client = ZmqQuotePushChannel(connect_address=pub_addr, context=ctx)
        try:
            for i in range(5):
                # Simulate _sync_subscriber_locked topic changes: start a
                # subscriber, immediately stop it from this (foreign) thread.
                client.start_subscriber(["T%d" % i], lambda t, d: None)
                time.sleep(0.05)
                client.stop()  # must not raise / abort
        finally:
            client.stop()
            server.stop()
        # Reaching here without an abort/crash is the assertion.
        self.assertTrue(True)


class RedisPushChannelTest(unittest.TestCase):
    def test_pub_sub_roundtrip(self):
        redis_client = FakeRedis()
        server = RedisQuotePushChannel(redis_client, account_id="acct")
        server.start_publisher()

        received = []
        done = threading.Event()
        client = RedisQuotePushChannel(redis_client, account_id="acct")

        def on_msg(topic, data):
            received.append((topic, data))
            done.set()

        client.start_subscriber(["SH,SZ"], on_msg)
        try:
            time.sleep(0.1)
            server.publish("SH,SZ", {"000001.SZ": {"lastPrice": 10.5}})
            self.assertTrue(done.wait(2.0), "redis subscriber did not receive the push")
        finally:
            client.stop()
            server.stop()

        self.assertEqual(received[0][0], "SH,SZ")
        self.assertEqual(received[0][1]["000001.SZ"]["lastPrice"], 10.5)

    def test_channel_name_scoped_by_account(self):
        redis_client = FakeRedis()
        server = RedisQuotePushChannel(redis_client, account_id="acct")
        server.start_publisher()
        server.publish("SH", {"x": 1})
        server.stop()
        self.assertIn("bigqmt:quote_push:acct:SH", redis_client.messages)

    def test_bad_message_is_skipped_and_next_message_is_delivered(self):
        redis_client = FakeRedis()
        received = []
        done = threading.Event()
        client = RedisQuotePushChannel(redis_client, account_id="acct")
        client.start_subscriber(["SH"], lambda topic, data: (received.append(data), done.set()))
        try:
            redis_client.publish("bigqmt:quote_push:acct:SH", b"not-json-or-msgpack")
            redis_client.publish("bigqmt:quote_push:acct:SH",
                                 encode_push_payload({"combo_key": "SH", "data": {"ok": 1}}))
            self.assertTrue(done.wait(2.0))
            self.assertEqual(received, [{"ok": 1}])
            self.assertEqual(client.last_error["stage"], "decode")
            self.assertEqual(client.last_error["message"], "invalid push payload")
        finally:
            client.stop()

    def test_disconnect_recreates_pubsub_and_restores_topics(self):
        redis_client = DisconnectOnceRedis()
        received = []
        done = threading.Event()
        client = RedisQuotePushChannel(redis_client, account_id="acct")
        client.start_subscriber(["SH"], lambda topic, data: (received.append(data), done.set()))
        try:
            deadline = time.time() + 2.0
            while time.time() < deadline and redis_client.pubsub_calls < 2:
                time.sleep(0.01)
            redis_client.publish("bigqmt:quote_push:acct:SH",
                                 encode_push_payload({"combo_key": "SH", "data": {"recovered": 1}}))
            self.assertTrue(done.wait(2.0))
            self.assertEqual(received, [{"recovered": 1}])
        finally:
            client.stop()

    def test_stop_generation_drops_message_returned_after_stop(self):
        redis_client = BlockingMessageRedis()
        received = []
        client = RedisQuotePushChannel(redis_client, account_id="acct")
        client.start_subscriber(["SH"], lambda topic, data: received.append(data))
        self.assertTrue(redis_client.blocking_pubsub.entered.wait(1.0))
        stopper = threading.Thread(target=client.stop)
        stopper.start()
        time.sleep(0.02)
        redis_client.blocking_pubsub.release.set()
        stopper.join(1.0)
        self.assertFalse(stopper.is_alive())
        self.assertEqual(received, [])


if __name__ == "__main__":
    unittest.main()
