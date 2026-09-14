import json
import os
import sys
import threading
import time
import unittest
from unittest import mock


ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

from bigqmt_signal_trader.whole_quote_session import WholeQuoteClientSession
from bigqmt_signal_trader import whole_quote_session as whole_quote_session_module


class CapturingLogger:
    def __init__(self):
        self.lines = []

    def warning(self, template, value):
        self.lines.append(template % value)


class FakeRpc:
    """Records control RPCs and returns canned subscribe responses."""

    def __init__(self):
        self.calls = []
        self._lock = threading.Lock()

    def __call__(self, method, params):
        with self._lock:
            self.calls.append((method, dict(params)))
        if method == "subscribe_whole_quote":
            codes = sorted(str(c).upper() for c in params.get("codes") or [])
            return {"combo_key": ",".join(codes), "topic": ",".join(codes)}
        return {}

    def methods(self):
        with self._lock:
            return [m for m, _ in self.calls]


class FakeRpcWithRestart(FakeRpc):
    """Simulates a server restart: keepalive fails for a window of calls,
    then the server is back (subscribe succeeds again)."""

    def __init__(self, fail_start=0, fail_count=3):
        super().__init__()
        self.fail_start = fail_start
        self.fail_count = fail_count

    def __call__(self, method, params):
        with self._lock:
            self.calls.append((method, dict(params)))
            if method == "quote_keepalive":
                n = sum(1 for m, _ in self.calls if m == "quote_keepalive") - 1  # 本次
        if method == "quote_keepalive" and self.fail_start <= n < self.fail_start + self.fail_count:
            raise RuntimeError("server restarting")
        if method == "subscribe_whole_quote":
            codes = sorted(str(c).upper() for c in params.get("codes") or [])
            return {"combo_key": ",".join(codes), "topic": ",".join(codes)}
        return {}


class FakePushChannel:
    """Client-side push channel stand-in: lets tests inject server pushes."""

    def __init__(self):
        self.subscriptions = []  # list of (topics_tuple, on_msg)
        self.started = False
        self.stopped = False
        self._on_msg = None

    def start_subscriber(self, topics, on_msg):
        self.started = True
        self._on_msg = on_msg
        self.subscriptions.append(tuple(topics))

    def inject(self, topic, data):
        if self._on_msg is not None:
            self._on_msg(topic, data)

    def stop(self):
        self.stopped = True

    def subscriber_thread_alive(self):
        return self.started and not self.stopped


class FakePushChannelWithTopics(FakePushChannel):
    """Like FakePushChannel but tracks the currently subscribed topic set, so
    the session can diff and reuse an existing subscriber."""

    def __init__(self):
        super().__init__()
        self.active_topics = frozenset()

    def start_subscriber(self, topics, on_msg):
        super().start_subscriber(topics, on_msg)
        self.active_topics = frozenset(topics)

    def stop(self):
        super().stop()
        self.active_topics = frozenset()


class WholeQuoteSessionTest(unittest.TestCase):
    def _session(self, **kwargs):
        rpc = FakeRpc()
        channel = FakePushChannel()
        session = WholeQuoteClientSession(
            rpc_call=rpc,
            push_channel=channel,
            client_id="client-test",
            heartbeat_interval_seconds=kwargs.pop("heartbeat_interval_seconds", 0.05),
            **kwargs,
        )
        return session, rpc, channel

    def test_subscribe_sends_rpc_and_returns_sub_id(self):
        session, rpc, _channel = self._session()
        sub_id = session.subscribe_whole_quote(["SH", "SZ"], callback=lambda d: None)
        self.assertIsNotNone(sub_id)
        self.assertIn("subscribe_whole_quote", rpc.methods())

    def test_subscribe_starts_push_channel_with_topic(self):
        session, _rpc, channel = self._session()
        session.subscribe_whole_quote(["SH", "SZ"], callback=lambda d: None)
        self.assertTrue(channel.started)
        self.assertIn(("SH,SZ",), channel.subscriptions)

    def test_incoming_push_invokes_callback(self):
        session, _rpc, channel = self._session()
        received = []
        session.subscribe_whole_quote(["SH"], callback=received.append)
        channel.inject("SH", {"000001.SZ": {"lastPrice": 10.5}})
        self.assertEqual(received, [{"000001.SZ": {"lastPrice": 10.5}}])

    def test_two_subscriptions_same_combo_share_one_push(self):
        session, _rpc, channel = self._session()
        got_a, got_b = [], []
        session.subscribe_whole_quote(["SH", "SZ"], callback=got_a.append)
        session.subscribe_whole_quote(["sz", "sh"], callback=got_b.append)
        channel.inject("SH,SZ", {"000001.SZ": {"lastPrice": 1.0}})
        self.assertEqual(len(got_a), 1)
        self.assertEqual(len(got_b), 1)

    def test_unsubscribe_stops_callback_and_sends_rpc(self):
        session, rpc, channel = self._session()
        received = []
        sub_id = session.subscribe_whole_quote(["SH"], callback=received.append)
        session.unsubscribe_quote(sub_id)
        self.assertIn("unsubscribe_whole_quote", rpc.methods())
        channel.inject("SH", {"x": 1})
        self.assertEqual(received, [])

    def test_keepalive_sent_for_active_subscriptions(self):
        session, rpc, _channel = self._session(heartbeat_interval_seconds=0.05)
        session.subscribe_whole_quote(["SH"], callback=lambda d: None)
        session.start()
        try:
            deadline = time.time() + 1.5
            while time.time() < deadline and rpc.methods().count("quote_keepalive") < 2:
                time.sleep(0.02)
        finally:
            session.stop()
        self.assertGreaterEqual(rpc.methods().count("quote_keepalive"), 2)

    def test_keepalive_stops_after_unsubscribe(self):
        session, rpc, _channel = self._session(heartbeat_interval_seconds=0.05)
        sub_id = session.subscribe_whole_quote(["SH"], callback=lambda d: None)
        session.start()
        time.sleep(0.15)
        session.unsubscribe_quote(sub_id)
        count_at_unsub = rpc.methods().count("quote_keepalive")
        time.sleep(0.2)
        session.stop()
        self.assertEqual(rpc.methods().count("quote_keepalive"), count_at_unsub)

    def test_replay_resubscribes_all_active(self):
        session, rpc, _channel = self._session()
        session.subscribe_whole_quote(["SH"], callback=lambda d: None)
        session.subscribe_whole_quote(["SZ"], callback=lambda d: None)
        subscribes_before = rpc.methods().count("subscribe_whole_quote")
        session.replay_subscriptions()
        self.assertEqual(rpc.methods().count("subscribe_whole_quote"), subscribes_before + 2)

    def test_client_id_used_in_rpc(self):
        session, rpc, _channel = self._session()
        session.subscribe_whole_quote(["SH"], callback=lambda d: None)
        sub_params = [p for m, p in rpc.calls if m == "subscribe_whole_quote"][0]
        self.assertEqual(sub_params["client_id"], "client-test")

    def test_replay_exception_does_not_kill_heartbeat(self):
        class ReplayFails(FakeRpc):
            def __call__(self, method, params):
                if method == "subscribe_whole_quote" and self.methods().count(method):
                    with self._lock:
                        self.calls.append((method, dict(params)))
                    raise RuntimeError("subscribe timeout")
                return super().__call__(method, params)

        rpc = ReplayFails()
        session = WholeQuoteClientSession(
            rpc, FakePushChannel(), "client-test", heartbeat_interval_seconds=0.01,
            push_silence_replay_heartbeats=1,
        )
        session.subscribe_whole_quote(["SH"], callback=lambda data: None)
        session.start()
        try:
            deadline = time.time() + 1.0
            while time.time() < deadline and session.subscription_health()["recovery_attempts"] == 0:
                time.sleep(0.01)
            health = session.subscription_health()
            self.assertTrue(health["heartbeat_thread_alive"])
            self.assertEqual(health["last_error"]["stage"], "replay")
            self.assertEqual(health["last_error"]["type"], "RuntimeError")
            self.assertEqual(health["recovery_state"], "BACKING_OFF")
        finally:
            session.stop()

    def test_replay_error_log_preserves_request_correlation_and_behavior(self):
        class ReplayFails(FakeRpc):
            def __call__(self, method, params):
                if method == "subscribe_whole_quote" and self.methods().count(method):
                    with self._lock:
                        self.calls.append((method, dict(params)))
                    raise TimeoutError(
                        "redis rpc timeout request_id=req-replay-123 password=do-not-log")
                return super().__call__(method, params)

        captured = CapturingLogger()
        session = WholeQuoteClientSession(ReplayFails(), FakePushChannel(), "client-test")
        session.subscribe_whole_quote(["SH"], callback=lambda data: None)
        with mock.patch.object(whole_quote_session_module, "log", captured):
            with self.assertRaises(TimeoutError):
                session.replay_subscriptions()

        self.assertEqual(len(captured.lines), 1)
        event = json.loads(captured.lines[0])
        self.assertEqual(event["event"], "qmt.subscription_rpc_trace")
        self.assertEqual(event["method"], "subscribe_whole_quote")
        self.assertEqual(event["request_id"], "req-replay-123")
        self.assertEqual(event["subscription_id"], "1")
        self.assertEqual(event["topic"], "SH")
        self.assertEqual(event["outcome"], "TIMEOUT")
        self.assertEqual(event["stage"], "client_timeout")
        self.assertEqual(event["error_type"], "TimeoutError")
        self.assertTrue(event["timestamp"].endswith("Z"))
        self.assertNotIn("do-not-log", captured.lines[0])
        self.assertLessEqual(len(event["error_message"]), 512)

    def test_replay_log_format_failure_preserves_original_exception(self):
        class UnprintableError(RuntimeError):
            def __str__(self):
                raise RuntimeError("format failed")

        class ReplayFails(FakeRpc):
            def __call__(self, method, params):
                if method == "subscribe_whole_quote" and self.methods().count(method):
                    with self._lock:
                        self.calls.append((method, dict(params)))
                    raise UnprintableError()
                return super().__call__(method, params)

        session = WholeQuoteClientSession(ReplayFails(), FakePushChannel(), "client-test")
        session.subscribe_whole_quote(["SH"], callback=lambda data: None)
        with self.assertRaises(UnprintableError):
            session.replay_subscriptions()

    def test_replay_error_message_redacts_quoted_and_spaced_credentials(self):
        cases = [
            ("password=abc request_id=req-1", "abc"),
            ("{'password': 'a b', 'detail': 'safe'} request_id=req-2", "a b"),
            ('token="c d" request_id=req-3', "c d"),
            ("credential='e f' request_id=req-4", "e f"),
            ('shared_secret="unterminated secret value request_id=req-5',
             "unterminated secret value"),
        ]
        for raw, secret in cases:
            with self.subTest(raw=raw):
                sanitized = whole_quote_session_module._safe_error_message(raw)
                self.assertNotIn(secret, sanitized)
                self.assertIn("***", sanitized)
                expected_request_id = raw.rsplit("request_id=", 1)[-1]
                self.assertEqual(
                    whole_quote_session_module._request_id_from_error(raw),
                    expected_request_id,
                )

    def test_start_replaces_dead_heartbeat_when_started_flag_is_stale(self):
        session, _rpc, _channel = self._session()
        dead = threading.Thread(target=lambda: None)
        dead.start()
        dead.join()
        session._started = True
        session._heartbeat_thread = dead
        session.start()
        try:
            self.assertIsNot(session._heartbeat_thread, dead)
            self.assertTrue(session._heartbeat_thread.is_alive())
        finally:
            session.stop()

    def test_stop_then_start_uses_a_new_live_generation(self):
        session, _rpc, _channel = self._session()
        session.start()
        old_thread = session._heartbeat_thread
        old_generation = session._heartbeat_generation
        session.stop()
        session.start()
        try:
            self.assertGreater(session._heartbeat_generation, old_generation)
            self.assertIsNot(session._heartbeat_thread, old_thread)
            self.assertTrue(session.subscription_health()["heartbeat_thread_alive"])
        finally:
            session.stop()

    def test_blocked_old_heartbeat_cannot_update_health_after_restart(self):
        class BlockingFirstKeepalive(FakeRpc):
            def __init__(self):
                super().__init__()
                self.entered = threading.Event()
                self.release = threading.Event()
                self.keepalive_calls = 0

            def __call__(self, method, params):
                if method == "quote_keepalive":
                    self.keepalive_calls += 1
                    if self.keepalive_calls == 1:
                        self.entered.set()
                        self.release.wait(2.0)
                        raise RuntimeError("obsolete generation failure")
                return super().__call__(method, params)

        rpc = BlockingFirstKeepalive()
        session = WholeQuoteClientSession(
            rpc, FakePushChannel(), "client-test", heartbeat_interval_seconds=0.01)
        session.subscribe_whole_quote(["SH"], callback=lambda data: None)
        session.start()
        self.assertTrue(rpc.entered.wait(1.0))
        stopper = threading.Thread(target=session.stop)
        stopper.start()
        stopper.join(1.5)
        session.start()
        rpc.release.set()
        try:
            deadline = time.time() + 1.0
            while time.time() < deadline and rpc.keepalive_calls < 2:
                time.sleep(0.01)
            self.assertIsNone(session.subscription_health()["last_error"])
        finally:
            session.stop()

    def test_blocked_old_replay_cannot_update_health_or_continue_entries(self):
        class BlockingFirstReplay(FakeRpc):
            def __init__(self):
                super().__init__()
                self.initial_subscribes = 0
                self.entered = threading.Event()
                self.release = threading.Event()
                self.replay_threads = []

            def __call__(self, method, params):
                if method == "subscribe_whole_quote":
                    self.initial_subscribes += 1
                    if self.initial_subscribes > 2:
                        self.replay_threads.append(threading.get_ident())
                    if self.initial_subscribes == 3:
                        self.entered.set()
                        self.release.wait(2.0)
                        raise RuntimeError("obsolete replay failure")
                return super().__call__(method, params)

        rpc = BlockingFirstReplay()
        session = WholeQuoteClientSession(
            rpc, FakePushChannel(), "client-test", heartbeat_interval_seconds=0.01,
            push_silence_replay_heartbeats=1)
        session.subscribe_whole_quote(["SH"], callback=lambda data: None)
        session.subscribe_whole_quote(["SZ"], callback=lambda data: None)
        session.start()
        old_ident = session._heartbeat_thread.ident
        self.assertTrue(rpc.entered.wait(1.0))
        stopper = threading.Thread(target=session.stop)
        stopper.start()
        stopper.join(1.5)
        session.start()
        rpc.release.set()
        try:
            deadline = time.time() + 1.0
            while time.time() < deadline and rpc.replay_threads.count(old_ident) < 1:
                time.sleep(0.01)
            self.assertEqual(rpc.replay_threads.count(old_ident), 1)
            self.assertIsNone(session.subscription_health()["last_error"])
        finally:
            session.stop()

    def test_same_topics_restart_a_dead_push_thread(self):
        session, _rpc, channel = self._session()
        session.subscribe_whole_quote(["SH"], callback=lambda data: None)
        channel.stopped = True
        session.subscribe_whole_quote(["SH"], callback=lambda data: None)
        self.assertEqual(len(channel.subscriptions), 2)

    def test_cancel_during_replay_compensates_late_subscribe(self):
        class BlockingReplay(FakeRpc):
            def __init__(self):
                super().__init__()
                self.entered = threading.Event()
                self.release = threading.Event()

            def __call__(self, method, params):
                if method == "subscribe_whole_quote" and self.methods().count(method):
                    self.entered.set()
                    self.release.wait(1.0)
                return super().__call__(method, params)

        rpc = BlockingReplay()
        channel = FakePushChannel()
        received = []
        session = WholeQuoteClientSession(rpc, channel, "client-test")
        sub_id = session.subscribe_whole_quote(["SH"], callback=received.append)
        replay = threading.Thread(target=session.replay_subscriptions)
        replay.start()
        self.assertTrue(rpc.entered.wait(1.0))
        session.unsubscribe_quote(sub_id)
        channel.inject("SH", {"x": 1})
        rpc.release.set()
        replay.join(1.0)
        self.assertEqual(received, [])
        self.assertEqual(rpc.methods().count("unsubscribe_whole_quote"), 2)

    def test_auto_replay_after_server_restart(self):
        """服务端重启后, 心跳线程应检测到 keepalive 失败并在服务端恢复后
        自动重放订阅(否则推送永久中断)。"""
        rpc = FakeRpcWithRestart(fail_start=1, fail_count=3)  # 第1-3次keepalive失败
        channel = FakePushChannelWithTopics()
        session = WholeQuoteClientSession(
            rpc_call=rpc, push_channel=channel, client_id="client-test",
            heartbeat_interval_seconds=0.05,
        )
        session.subscribe_whole_quote(["SH"], callback=lambda d: None)
        session.start()
        try:
            deadline = time.time() + 2.0
            # 等待: keepalive 失败触发重放, 重放后又有新的 keepalive
            while time.time() < deadline:
                if rpc.methods().count("subscribe_whole_quote") >= 2 and \
                   rpc.methods().count("quote_keepalive") >= 5:
                    break
                time.sleep(0.02)
            subs = rpc.methods().count("subscribe_whole_quote")
            kps = rpc.methods().count("quote_keepalive")
            print("auto-replay: subscribe=%d keepalive=%d" % (subs, kps))
            self.assertGreaterEqual(subs, 2, "服务端恢复后应重放订阅")
            self.assertGreaterEqual(kps, 5)
        finally:
            session.stop()

    def test_no_replay_when_server_healthy(self):
        """服务端健康时不应反复重放订阅(只保留初始 subscribe)。"""
        session, rpc, _channel = self._session(heartbeat_interval_seconds=0.05)
        session.subscribe_whole_quote(["SH"], callback=lambda d: None)
        session.start()
        try:
            time.sleep(0.4)
            self.assertEqual(rpc.methods().count("subscribe_whole_quote"), 1)
        finally:
            session.stop()

    def test_replay_when_push_silent_after_restart(self):
        """服务端重启后 keepalive 可能不失败(redis 队列兜住),但推送会静默。
        客户端应在推送静默超过阈值后自动重放订阅。"""
        rpc = FakeRpc()  # keepalive 从不失败
        channel = FakePushChannelWithTopics()
        session = WholeQuoteClientSession(
            rpc_call=rpc, push_channel=channel, client_id="client-test",
            heartbeat_interval_seconds=0.05,
            push_silence_replay_heartbeats=2,  # 2 个心跳周期无推送即重放
        )
        session.subscribe_whole_quote(["SH"], callback=lambda d: None)
        session.start()
        try:
            deadline = time.time() + 1.5
            while time.time() < deadline:
                if rpc.methods().count("subscribe_whole_quote") >= 2:
                    break
                time.sleep(0.02)
            self.assertGreaterEqual(
                rpc.methods().count("subscribe_whole_quote"), 2,
                "推送静默超阈值应自动重放订阅",
            )
        finally:
            session.stop()

    def test_subscriber_reused_when_topic_set_unchanged(self):
        """订阅/退订不应为同一 topic 集合反复重建订阅线程(线程泄漏)。"""
        rpc = FakeRpc()
        channel = FakePushChannelWithTopics()
        session = WholeQuoteClientSession(
            rpc_call=rpc, push_channel=channel, client_id="client-test",
            heartbeat_interval_seconds=0.05,
        )
        sub1 = session.subscribe_whole_quote(["SH"], callback=lambda d: None)
        sub2 = session.subscribe_whole_quote(["SH"], callback=lambda d: None)
        # 两次同 topic 订阅:共享订阅线程,只 start 一次
        self.assertEqual(len(channel.subscriptions), 1)
        # 退订一个 sub_id:topic 集合不变,不应重启线程
        session.unsubscribe_quote(sub1)
        self.assertEqual(len(channel.subscriptions), 1)
        self.assertFalse(channel.stopped)
        # 退订最后一个 sub_id:topic 集合变空,应停掉线程
        session.unsubscribe_quote(sub2)
        self.assertTrue(channel.stopped)
        self.assertEqual(len(channel.subscriptions), 1)

    def test_subscriber_restarts_when_topic_set_changes(self):
        """topic 集合变化时重建订阅线程,但旧线程先 stop。"""
        rpc = FakeRpc()
        channel = FakePushChannelWithTopics()
        session = WholeQuoteClientSession(
            rpc_call=rpc, push_channel=channel, client_id="client-test",
            heartbeat_interval_seconds=0.05,
        )
        session.subscribe_whole_quote(["SH"], callback=lambda d: None)
        session.subscribe_whole_quote(["SZ"], callback=lambda d: None)
        # topic 集合从 {SH} 变成 {SH,SZ} -> 重建(但旧 stop)
        self.assertEqual(len(channel.subscriptions), 2)
        self.assertTrue(channel.stopped)
        # 新线程订阅 {SH,SZ}
        self.assertEqual(channel.active_topics, frozenset(["SH", "SZ"]))


if __name__ == "__main__":
    unittest.main()
