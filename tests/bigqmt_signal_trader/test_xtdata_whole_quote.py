import os
import sys
import unittest


ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

import bigqmt_signal_trader.xtquant_compat as compat


class FakeSession:
    """Captures BigQmtXtData -> WholeQuoteClientSession delegation."""

    def __init__(self):
        self.subscribed = []
        self.unsubscribed = []
        self.started = False
        self._next = 0
        self._active = set()

    def subscribe_whole_quote(self, code_list, callback=None):
        self._next += 1
        self._active.add(self._next)
        self.subscribed.append((list(code_list), callback))
        return self._next

    def unsubscribe_quote(self, sub_id):
        self.unsubscribed.append(sub_id)
        self._active.discard(sub_id)
        return 0

    def has_subscription(self, sub_id):
        return sub_id in self._active

    def start(self):
        self.started = True

    def stop(self):
        pass

    def subscription_health(self):
        return {"supported": True, "status": "HEALTHY", "started": True,
                "active_subscription_count": len(self._active),
                "heartbeat_thread_alive": True, "push_thread_alive": True,
                "last_heartbeat_monotonic": 1.0, "last_push_monotonic": 2.0,
                "last_error": None, "recovery_state": "HEALTHY", "recovery_attempts": 0,
                "last_recovery_monotonic": None, "last_recovery_success_monotonic": None}


class FakeClient:
    def __init__(self):
        self.account_id = "acct"
        self.local_cache_config = {}
        self.full_tick_cache_config = {}
        self.transport_name = "redis"
        self.calls = []

    def call(self, method, params=None, **kwargs):
        self.calls.append((method, params))
        if method == "get_full_tick":
            return {c: {"lastPrice": 1.0} for c in (params or {}).get("codes") or []}
        return {}

    def _redis(self):
        raise AssertionError("redis not expected in this test")


class XtDataWholeQuoteDelegationTest(unittest.TestCase):
    def _xtdata(self, session):
        client = FakeClient()
        data = compat.BigQmtXtData(client)
        data._quote_session_factory = lambda: session
        return data, client

    def test_subscribe_delegates_to_session_and_primes_full_tick(self):
        session = FakeSession()
        data, _client = self._xtdata(session)
        received = []
        sub_id = data.subscribe_whole_quote(["000001.SZ"], callback=received.append)
        # Delegated to the session.
        self.assertEqual(session.subscribed[0][0], ["000001.SZ"])
        self.assertEqual(sub_id, 1)
        # Primed via get_full_tick so the callback fires once with the snapshot.
        self.assertEqual(received, [{"000001.SZ": {"lastPrice": 1.0}}])

    def test_subscribe_starts_session_once(self):
        session = FakeSession()
        data, _client = self._xtdata(session)
        data.subscribe_whole_quote(["SH"], callback=lambda d: None)
        self.assertTrue(session.started)

    def test_unsubscribe_delegates_to_session(self):
        session = FakeSession()
        data, _client = self._xtdata(session)
        sub_id = data.subscribe_whole_quote(["SH"], callback=lambda d: None)
        data.unsubscribe_quote(sub_id)
        self.assertEqual(session.unsubscribed, [sub_id])

    def test_session_reused_across_subscriptions(self):
        session = FakeSession()
        data, _client = self._xtdata(session)
        data.subscribe_whole_quote(["SH"], callback=lambda d: None)
        data.subscribe_whole_quote(["SZ"], callback=lambda d: None)
        self.assertEqual(len(session.subscribed), 2)

    def test_health_without_session_is_idle_and_lazy(self):
        data, _client = self._xtdata(FakeSession())
        health = data.subscription_health()
        self.assertEqual(health["status"], "NOT_STARTED")
        self.assertEqual(health["active_subscription_count"], 0)
        self.assertIsNone(data._quote_session)

    def test_health_delegates_live_thread_evidence(self):
        session = FakeSession()
        data, _client = self._xtdata(session)
        data.subscribe_whole_quote(["SH"], callback=lambda data: None)
        health = data.subscription_health()
        self.assertTrue(health["heartbeat_thread_alive"])
        self.assertTrue(health["push_thread_alive"])
        self.assertEqual(health["active_subscription_count"], 1)


if __name__ == "__main__":
    unittest.main()
