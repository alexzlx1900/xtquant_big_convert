"""Client-side whole-quote subscription session.

Owns the per-process state for ``subscribe_whole_quote``: the local
subscription table, the shared push-channel subscriber thread, and the
keepalive heartbeat thread. One session is shared by every ``subscribe_whole_quote``
call in the process (``BigQmtXtData`` delegates here), so all subscriptions ride
a single push-channel connection and a single heartbeat loop.

The big-QMT whole-quote callback is INCREMENTAL (only changed symbols), so a
subscription does not by itself deliver an initial full snapshot — callers layer
a ``get_full_tick`` prime on top (done in ``BigQmtXtData.subscribe_whole_quote``).
"""

import threading
import time


def _norm_topic(code_list):
    return ",".join(sorted({str(c).strip().upper() for c in (code_list or []) if str(c or "").strip()}))


class WholeQuoteClientSession(object):
    def __init__(self, rpc_call, push_channel, client_id, heartbeat_interval_seconds=3.0, sub_id_func=None,
                 push_silence_replay_heartbeats=10):
        """``rpc_call`` is ``client.call``-shaped: fn(method, params) -> dict.
        ``push_channel`` is a QuotePushChannel used purely as a subscriber.
        ``sub_id_func`` (optional) mints subscription ids; defaults to a counter.
        ``push_silence_replay_heartbeats``: after this many heartbeat rounds
        without any push, replay subscriptions (covers server restarts where
        keepalive keeps succeeding because the redis request queue buffers
        during the restart window but the subscription table was reset)."""
        self._rpc = rpc_call
        self._channel = push_channel
        self.client_id = str(client_id or "")
        self._heartbeat_interval = float(heartbeat_interval_seconds)
        self._push_silence_replay_heartbeats = int(push_silence_replay_heartbeats)
        self._sub_id_func = sub_id_func
        self._seq = 0
        self._lock = threading.RLock()
        self._subscriptions = {}  # sub_id -> {"topic": str, "callback": fn, "codes": [...]}
        self._started = False
        self._subscriber_active = False
        self._subscribed_topics = frozenset()  # topic set the subscriber covers now
        self._heartbeat_thread = None
        self._heartbeat_stop = None
        self._heartbeat_generation = 0
        self._last_push_time = None  # monotonic time of last incoming push
        self._last_heartbeat_time = None
        self._last_error = None
        self._recovery_state = "IDLE"
        self._recovery_attempts = 0
        self._last_recovery_time = None
        self._last_recovery_success_time = None

    # -- subscription lifecycle ---------------------------------------------
    def subscribe_whole_quote(self, code_list, callback=None):
        codes = [str(c) for c in (code_list or []) if str(c or "").strip()]
        if not codes:
            raise ValueError("code_list is required")
        with self._lock:
            sub_id = self._next_sub_id()
        result = self._rpc(
            "subscribe_whole_quote",
            {"client_id": self.client_id, "sub_id": sub_id, "codes": codes},
        ) or {}
        topic = str(result.get("topic") or result.get("combo_key") or _norm_topic(codes))
        with self._lock:
            self._subscriptions[sub_id] = {"topic": topic, "callback": callback, "codes": codes,
                                           "token": object(), "cancel_confirmed": False}
            self._sync_subscriber_locked()
        return sub_id

    def unsubscribe_quote(self, sub_id):
        with self._lock:
            entry = self._subscriptions.pop(sub_id, None)
        if entry is None:
            return 0
        try:
            self._rpc("unsubscribe_whole_quote", {"client_id": self.client_id, "sub_id": sub_id})
            entry["cancel_confirmed"] = True
        except Exception as exc:
            self._record_error("unsubscribe", exc, "unsubscribe failed")
        finally:
            with self._lock:
                self._sync_subscriber_locked()
        return 0

    def has_subscription(self, sub_id):
        with self._lock:
            return sub_id in self._subscriptions

    def replay_subscriptions(self, stop_event=None, generation=None):
        """Re-send subscribe for every active sub_id (server restart recovery).
        Idempotent on the server (keyed by client_id+combo), so replays are safe."""
        with self._lock:
            items = [(sid, entry, entry["token"], list(entry["codes"]))
                     for sid, entry in self._subscriptions.items()]
        for sub_id, entry, token, codes in items:
            with self._lock:
                if not self._generation_current_locked(stop_event, generation):
                    return False
                if self._subscriptions.get(sub_id) is not entry or entry.get("token") is not token:
                    continue
            try:
                self._rpc("subscribe_whole_quote",
                          {"client_id": self.client_id, "sub_id": sub_id, "codes": codes})
            except Exception:
                with self._lock:
                    if not self._generation_current_locked(stop_event, generation):
                        return False
                raise
            with self._lock:
                still_active = (self._subscriptions.get(sub_id) is entry and
                                entry.get("token") is token)
                generation_current = self._generation_current_locked(stop_event, generation)
            if not still_active:
                try:
                    self._rpc("unsubscribe_whole_quote",
                              {"client_id": self.client_id, "sub_id": sub_id})
                    entry["cancel_confirmed"] = True
                except Exception as exc:
                    self._record_error("unsubscribe_compensation", exc,
                                       "late replay compensation unconfirmed")
            if not generation_current:
                return False
        return True

    # -- heartbeat -------------------------------------------------------------
    def start(self):
        with self._lock:
            if (self._heartbeat_thread is not None and self._heartbeat_thread.is_alive() and
                    self._heartbeat_stop is not None and not self._heartbeat_stop.is_set()):
                self._started = True
                return
            old_stop = self._heartbeat_stop
            if old_stop is not None:
                old_stop.set()
            self._heartbeat_generation += 1
            generation = self._heartbeat_generation
            stop_event = threading.Event()
            self._heartbeat_stop = stop_event
            self._started = True
            self._heartbeat_thread = threading.Thread(
                target=self._heartbeat_loop, args=(stop_event, generation),
                name="bigqmt-quote-keepalive", daemon=True
            )
            self._heartbeat_thread.start()

    def stop(self):
        with self._lock:
            self._started = False
            stop_event = self._heartbeat_stop
            thread = self._heartbeat_thread
            self._heartbeat_generation += 1
            if stop_event is not None:
                stop_event.set()
        if thread is not None:
            thread.join(timeout=1.0)
        with self._lock:
            if self._heartbeat_thread is thread and (thread is None or not thread.is_alive()):
                self._heartbeat_thread = None
                self._heartbeat_stop = None

    def _heartbeat_loop(self, stop_event, generation):
        consecutive_failures = 0
        replay_failures = 0
        silence_rounds = 0
        prev_last_push = None
        while not stop_event.is_set():
            with self._lock:
                if generation != self._heartbeat_generation or not self._started:
                    return
                sub_ids = list(self._subscriptions.keys())
                last_push = self._last_push_time
            if not sub_ids:
                stop_event.wait(self._heartbeat_interval)
                continue
            failures = 0
            for sub_id in sub_ids:
                try:
                    self._rpc("quote_keepalive", {"client_id": self.client_id, "sub_id": sub_id})
                    with self._lock:
                        if generation != self._heartbeat_generation or stop_event.is_set():
                            return
                        self._last_heartbeat_time = time.monotonic()
                except Exception as exc:
                    with self._lock:
                        if generation != self._heartbeat_generation or stop_event.is_set():
                            return
                    failures += 1
                    self._record_error("heartbeat", exc, "quote_keepalive failed")
            with self._lock:
                if generation != self._heartbeat_generation or stop_event.is_set():
                    return
            if failures:
                consecutive_failures += 1
                with self._lock:
                    self._recovery_state = "BACKING_OFF"
            elif consecutive_failures >= 3:
                # Server is back after a restart window: replay subscriptions so
                # the restarted server re-creates the big-QMT subscriptions (its
                # state is gone). Idempotent on the server, so replays are safe.
                if self._recover_replay(stop_event, generation):
                    consecutive_failures = 0
                    replay_failures = 0
                else:
                    replay_failures += 1
            else:
                if consecutive_failures:
                    with self._lock:
                        self._recovery_state = "RECOVERED"
                consecutive_failures = 0
            # Push-silence detection: a server restart can survive with keepalive
            # succeeding (the redis request queue buffers during the restart
            # window) while the subscription table was reset, so pushes stop.
            # Replay when no push arrived for several heartbeat rounds (also
            # covers the case where the very first prime push never arrived).
            if last_push != prev_last_push:
                silence_rounds = 0  # a push arrived since the last round
            else:
                silence_rounds += 1
            prev_last_push = last_push
            if silence_rounds >= self._push_silence_replay_heartbeats:
                if self._recover_replay(stop_event, generation):
                    replay_failures = 0
                else:
                    replay_failures += 1
                silence_rounds = 0
            failure_depth = max(consecutive_failures, replay_failures)
            backoff = min(self._heartbeat_interval * (2 ** min(failure_depth, 3)),
                          max(self._heartbeat_interval, 5.0))
            stop_event.wait(backoff)

    def _recover_replay(self, stop_event, generation):
        with self._lock:
            if not self._generation_current_locked(stop_event, generation):
                return False
            self._recovery_state = "REPLAYING"
            self._recovery_attempts += 1
            self._last_recovery_time = time.monotonic()
        try:
            completed = self.replay_subscriptions(stop_event=stop_event, generation=generation)
        except Exception as exc:
            with self._lock:
                if not self._generation_current_locked(stop_event, generation):
                    return False
            self._record_error("replay", exc, "subscription replay failed")
            with self._lock:
                self._recovery_state = "BACKING_OFF"
            return False
        if not completed:
            return False
        with self._lock:
            if not self._generation_current_locked(stop_event, generation):
                return False
            self._recovery_state = "RECOVERED"
            self._last_recovery_success_time = time.monotonic()
        return True

    def _generation_current_locked(self, stop_event, generation):
        if stop_event is None or generation is None:
            return True
        return (not stop_event.is_set() and generation == self._heartbeat_generation and
                self._started)

    # -- push routing ------------------------------------------------------------
    def _on_push(self, topic, data):
        now = time.monotonic()
        with self._lock:
            self._last_push_time = now
            callbacks = [
                entry["callback"]
                for entry in self._subscriptions.values()
                if entry["topic"] == topic and entry["callback"] is not None
            ]
        for callback in callbacks:
            try:
                callback(data)
            except Exception:
                pass

    def _record_error(self, stage, exc, message):
        with self._lock:
            self._last_error = {
                "stage": str(stage), "type": type(exc).__name__,
                "message": str(message)[:200], "monotonic": time.monotonic(),
            }

    def subscription_health(self):
        with self._lock:
            heartbeat_alive = bool(self._heartbeat_thread and self._heartbeat_thread.is_alive())
            push_alive = False
            checker = getattr(self._channel, "subscriber_thread_alive", None)
            if callable(checker):
                push_alive = bool(checker())
            active_count = len(self._subscriptions)
            if active_count:
                status = "RUNNING" if (
                    heartbeat_alive and push_alive and
                    self._recovery_state not in ("BACKING_OFF", "REPLAYING")
                ) else "DEGRADED"
            elif heartbeat_alive:
                status = "RUNNING"
            else:
                status = "STOPPED" if self._heartbeat_generation else "NOT_STARTED"
            channel_error = getattr(self._channel, "last_error", None)
            errors = [item for item in (self._last_error, channel_error) if isinstance(item, dict)]
            last_error = max(errors, key=lambda item: item.get("monotonic") or 0.0) if errors else None
            return {
                "supported": True, "status": status, "started": heartbeat_alive,
                "active_subscription_count": active_count,
                "heartbeat_thread_alive": heartbeat_alive, "push_thread_alive": push_alive,
                "last_heartbeat_monotonic": self._last_heartbeat_time,
                "last_push_monotonic": self._last_push_time,
                "last_error": last_error,
                "recovery_state": self._recovery_state if active_count else "IDLE",
                "recovery_attempts": self._recovery_attempts,
                "last_recovery_monotonic": self._last_recovery_time,
                "last_recovery_success_monotonic": self._last_recovery_success_time,
            }

    def _sync_subscriber_locked(self):
        """(Re)start the push-channel subscriber to cover exactly the active
        topics. Reuses an existing subscriber when the topic set is unchanged;
        stops it before restarting when the set changed. No-op when nothing is
        subscribed (and stops the running subscriber in that case)."""
        topics = sorted({entry["topic"] for entry in self._subscriptions.values()})
        active = frozenset(topics)
        if active == self._subscribed_topics:
            checker = getattr(self._channel, "subscriber_thread_alive", None)
            if not active or not callable(checker) or checker():
                return
            self._subscriber_active = False
        if not active:
            if self._subscriber_active:
                try:
                    self._channel.stop()
                except Exception:
                    pass
                self._subscriber_active = False
            self._subscribed_topics = active
            return
        if self._subscriber_active:
            try:
                self._channel.stop()
            except Exception:
                pass
        self._channel.start_subscriber(topics, self._on_push)
        self._subscriber_active = True
        self._subscribed_topics = active

    def _next_sub_id(self):
        if self._sub_id_func is not None:
            return self._sub_id_func()
        self._seq += 1
        return self._seq
