"""Server→client whole-quote push channel.

The RPC transport is request/response only; whole-quote data needs the opposite
direction — the server pushes each incremental tick batch to every client
subscribed to that combination. This module provides one abstract channel with
two interchangeable implementations:

* :class:`ZmqQuotePushChannel` — a ``PUB`` socket on the server, a ``SUB`` socket
  per client. Native to no-redis deployments. Fire-and-forget: a client that is
  down simply misses frames (acceptable for incremental quote pushes).
* :class:`RedisQuotePushChannel` — redis ``publish``/``subscribe`` on a
  per-account, per-combination channel, for redis deployments.

Wire encoding is msgpack when available (smaller + faster for the
``{code: {field: number}}`` payload shape), falling back to stdlib json so the
channel stays usable without the optional dependency.
"""

import datetime
import json
import threading
import time

from .logging_setup import get_logger


log = get_logger("quote_push_channel")

try:
    import msgpack

    _HAS_MSGPACK = True
except Exception:  # pragma: no cover - depends on optional dependency
    msgpack = None
    _HAS_MSGPACK = False


def encode_push_payload(payload):
    """Encode a push payload dict to bytes (msgpack preferred, json fallback)."""
    if _HAS_MSGPACK:
        return msgpack.packb(payload, use_bin_type=True)
    return json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")


def _decode_push_payload(blob, attempts=None):
    """Inverse of :func:`encode_push_payload`. Accepts bytes or str.

    Encoding is not symmetric across deployments: a server without msgpack
    falls back to json while a client with msgpack installed decodes with
    msgpack — ``msgpack.unpackb`` then raises ``ExtraData`` on the json text
    (its first byte ``{`` parses as an int, leaving trailing bytes). So try
    msgpack first, and fall back to json when the bytes are not a single
    valid msgpack object.
    """
    if blob is None:
        return None
    if isinstance(blob, str):
        blob = blob.encode("utf-8")
    if _HAS_MSGPACK:
        try:
            result = msgpack.unpackb(blob, raw=False)
            if attempts is not None:
                attempts.append({"codec": "msgpack", "outcome": "SUCCESS"})
            return result
        except Exception as exc:
            if attempts is not None:
                attempts.append({"codec": "msgpack", "outcome": "ERROR",
                                 "error_type": type(exc).__name__[:128]})
    try:
        result = json.loads(blob.decode("utf-8"))
        if attempts is not None:
            attempts.append({"codec": "json", "outcome": "SUCCESS"})
        return result
    except Exception as exc:
        if attempts is not None:
            attempts.append({"codec": "json", "outcome": "ERROR",
                             "error_type": type(exc).__name__[:128]})
        raise


def decode_push_payload(blob):
    return _decode_push_payload(blob)


def _wire_bytes(blob):
    if isinstance(blob, bytes):
        return blob
    if isinstance(blob, bytearray):
        return bytes(blob)
    if isinstance(blob, str):
        return blob.encode("utf-8")
    return None


def _single_line_error(exc, limit=512):
    text = str(exc)
    text = text.replace("\\", "\\\\").replace("\r", "\\r").replace("\n", "\\n").replace("\t", "\\t")
    return text.encode("ascii", "backslashreplace").decode("ascii")[:limit]


def _error_location(exc):
    if isinstance(exc, json.JSONDecodeError):
        return "char=%d,line=%d,column=%d" % (exc.pos, exc.lineno, exc.colno)
    if isinstance(exc, UnicodeDecodeError):
        return "byte_start=%d,byte_end=%d" % (exc.start, exc.end)
    return "UNKNOWN"


def _redact_sensitive_bytes(value):
    # Snippets may begin in the middle of a secret value, outside the reach of
    # a key-based redactor. Preserve only JSON framing punctuation/whitespace;
    # mask all content bytes so arbitrary credentials cannot be reconstructed.
    safe = bytearray(b'{}[],:."\'\\-+ \t\r\n')
    return bytes(value if value in safe else ord("*") for value in bytearray(value))


def _escape_bytes(value):
    return "".join("\\x%02x" % item for item in bytearray(_redact_sensitive_bytes(value)))


def _error_context_bytes(exc):
    if not isinstance(exc, json.JSONDecodeError):
        return b""
    start = max(0, exc.pos - 16)
    return exc.doc[start:exc.pos + 16].encode("utf-8")[:32]


def _log_decode_failure(channel, blob, attempts, exc):
    try:
        raw = _wire_bytes(blob)
        prefix = raw[:64] if raw is not None else b""
        tail = raw[-32:] if raw is not None and len(raw) > 64 else b""
        error_context = _error_context_bytes(exc)
        channel_text = str(channel).replace("\r", "\\r").replace("\n", "\\n")
        channel_text = channel_text.encode("ascii", "backslashreplace").decode("ascii")[:96]
        event = {
            "event": "qmt.quote_push_decode_failed",
            "timestamp": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "channel": channel_text,
            "wire_bytes": len(raw) if raw is not None else None,
            "codec_strategy": "msgpack_then_json" if _HAS_MSGPACK else "json",
            "codec_attempts": list(attempts),
            "error_type": type(exc).__name__[:128],
            "error_location": _error_location(exc),
            "error_message": _single_line_error(exc, limit=192),
            "wire_prefix_escaped": _escape_bytes(prefix),
            "wire_prefix_bytes": len(prefix),
            "wire_tail_escaped": _escape_bytes(tail),
            "wire_tail_bytes": len(tail),
            "error_context_escaped": _escape_bytes(error_context),
            "error_context_bytes": len(error_context),
            "error_context_position_unit": "character" if error_context else "UNKNOWN",
            "snippet_source_bytes": len(prefix) + len(tail) + len(error_context),
            "snippet_truncated": bool(raw is not None and len(raw) > len(prefix)),
            "snippet_limit_bytes": 256,
        }
        line = json.dumps(event, ensure_ascii=True, separators=(",", ":"))
        if len(line.encode("utf-8")) > 2048:
            event["error_message"] = event["error_message"][:64]
            event["channel"] = event["channel"][:32]
            for attempt in event["codec_attempts"]:
                if "error_type" in attempt:
                    attempt["error_type"] = attempt["error_type"][:32]
            line = json.dumps(event, ensure_ascii=True, separators=(",", ":"))
        if len(line.encode("utf-8")) > 2048:
            event["error_message"] = ""
            event["error_type"] = event["error_type"][:32]
            event["channel"] = event["channel"][:16]
            line = json.dumps(event, ensure_ascii=True, separators=(",", ":"))
        log.warning("%s", line)
    except Exception:
        pass


class QuotePushChannel(object):
    """Abstract push channel. Server side: ``start_publisher`` + ``publish``.
    Client side: ``start_subscriber(topics, on_msg)``. A single instance may act
    as publisher or subscriber depending on which start method is called."""

    def start_publisher(self):
        raise NotImplementedError

    def start_subscriber(self, topics, on_msg):
        raise NotImplementedError

    def publish(self, topic, data):
        raise NotImplementedError

    def stop(self):
        raise NotImplementedError


class ZmqQuotePushChannel(QuotePushChannel):
    def __init__(self, bind_address=None, connect_address=None, context=None, print_prefix="[bigqmt_quote_push]"):
        self.bind_address = bind_address
        self.connect_address = connect_address
        self.print_prefix = print_prefix
        self._zmq = None
        self._context = context
        self._pub = None
        self._pub_lock = threading.Lock()
        self._sub = None
        self._sub_thread = None
        self._running = False
        self._sub_lock = threading.RLock()
        self._sub_stop = None
        self._sub_generation = 0
        self.last_error = None

    def _ensure_context(self):
        if self._zmq is None:
            import zmq

            self._zmq = zmq
            if self._context is None:
                self._context = zmq.Context.instance()
        return self._zmq, self._context

    # -- server side ---------------------------------------------------------
    def start_publisher(self):
        zmq, ctx = self._ensure_context()
        if not self.bind_address:
            raise ValueError("bind_address is required to start a publisher")
        self._pub = ctx.socket(zmq.PUB)
        self._pub.bind(self.bind_address)
        self._running = True

    def publish(self, topic, data):
        payload = encode_push_payload({"combo_key": topic, "data": data})
        frame = [str(topic).encode("utf-8"), payload]
        # PUB socket is not thread-safe; serialize under the lock and read the
        # socket inside it so a concurrent stop() (which nulls _pub) can't hand
        # us a closed socket.
        with self._pub_lock:
            pub = self._pub
            if pub is None:
                return
            try:
                pub.send_multipart(frame)
            except Exception as exc:
                print("%s zmq publish failed: %s" % (self.print_prefix, exc))

    # -- client side ---------------------------------------------------------
    def start_subscriber(self, topics, on_msg):
        zmq, ctx = self._ensure_context()
        if not self.connect_address:
            raise ValueError("connect_address is required to start a subscriber")
        with self._sub_lock:
            if (self._sub_thread is not None and self._sub_thread.is_alive() and
                    self._sub_stop is not None and not self._sub_stop.is_set()):
                return
            self._sub_generation += 1
            generation = self._sub_generation
            stop_event = threading.Event()
            self._sub_stop = stop_event
            sub = ctx.socket(zmq.SUB)
            sub.connect(self.connect_address)
            for topic in topics or []:
                sub.setsockopt(zmq.SUBSCRIBE, str(topic).encode("utf-8"))
            self._sub = sub
            self._running = True
            self._sub_thread = threading.Thread(
                target=self._sub_loop, args=(sub, on_msg, stop_event, generation),
                name="bigqmt-quote-push-sub", daemon=True)
            self._sub_thread.start()

    def _sub_loop(self, sub, on_msg, stop_event, generation):
        # The SUB socket is owned by THIS thread; it must be closed HERE (in a
        # finally) and never from another thread. Closing a ZMQ socket cross-
        # thread trips a Windows signaler assertion and aborts the whole QMT
        # process (the "auto-exit" users hit).
        poller = self._zmq.Poller()
        poller.register(sub, self._zmq.POLLIN)
        try:
            while not stop_event.is_set():
                with self._sub_lock:
                    if generation != self._sub_generation:
                        return
                try:
                    events = dict(poller.poll(200))
                except Exception:
                    break
                if sub not in events:
                    continue
                try:
                    frames = sub.recv_multipart(self._zmq.NOBLOCK)
                except Exception:
                    continue
                if len(frames) < 2:
                    continue
                topic = frames[0].decode("utf-8", errors="ignore")
                attempts = []
                try:
                    data = _decode_push_payload(frames[-1], attempts)
                except Exception as exc:
                    self._set_error("decode", exc, "invalid push payload")
                    _log_decode_failure(topic, frames[-1], attempts, exc)
                    continue
                payload_data = data.get("data") if isinstance(data, dict) else data
                with self._sub_lock:
                    if generation != self._sub_generation or stop_event.is_set():
                        return
                try:
                    on_msg(topic, payload_data)
                except Exception as exc:
                    print("%s subscriber callback failed: %s" % (self.print_prefix, exc))
        finally:
            try:
                sub.close(linger=0)
            except Exception:
                pass

    def stop(self):
        # Signal the sub thread to exit and let IT close its own socket (see
        # _sub_loop). Closing the SUB socket from this (foreign) thread would
        # trip the Windows ZMQ signaler abort and crash QMT.
        with self._sub_lock:
            self._running = False
            thread = self._sub_thread
            stop_event = self._sub_stop
            self._sub_generation += 1
            if stop_event is not None:
                stop_event.set()
        if thread is not None and thread.is_alive():
            thread.join(1.0)
        with self._sub_lock:
            if self._sub_thread is thread and (thread is None or not thread.is_alive()):
                self._sub_thread = None
                self._sub_stop = None
                self._sub = None
        # The PUB socket is only touched by publisher threads under _pub_lock;
        # null it first so a racing publish() sees None and bails, then close.
        with self._pub_lock:
            pub = self._pub
            self._pub = None
        if pub is not None:
            try:
                pub.close(linger=0)
            except Exception:
                pass

    def subscriber_thread_alive(self):
        with self._sub_lock:
            return bool(self._sub_thread and self._sub_thread.is_alive())

    def _set_error(self, stage, exc, message):
        self.last_error = {"stage": stage, "type": type(exc).__name__,
                           "message": message, "monotonic": time.monotonic()}


class RedisQuotePushChannel(QuotePushChannel):
    def __init__(self, redis_client, account_id="", channel_template="bigqmt:quote_push:{account_id}:{topic}", print_prefix="[bigqmt_quote_push]"):
        self.redis = redis_client
        self.account_id = str(account_id or "")
        self.channel_template = channel_template
        self.print_prefix = print_prefix
        self._running = False
        self._pubsub = None
        self._thread = None
        self._lock = threading.RLock()
        self._stop_event = None
        self._generation = 0
        self.last_error = None

    def _channel(self, topic):
        return self.channel_template.format(account_id=self.account_id, topic=topic)

    # -- server side ---------------------------------------------------------
    def start_publisher(self):
        # Redis publish needs no setup; present for interface symmetry.
        self._running = True

    def publish(self, topic, data):
        payload = encode_push_payload({"combo_key": topic, "data": data})
        try:
            self.redis.publish(self._channel(topic), payload)
        except Exception as exc:
            print("%s redis publish failed: %s" % (self.print_prefix, exc))

    # -- client side ---------------------------------------------------------
    def start_subscriber(self, topics, on_msg):
        with self._lock:
            if (self._thread is not None and self._thread.is_alive() and
                    self._stop_event is not None and not self._stop_event.is_set()):
                return
            self._generation += 1
            generation = self._generation
            stop_event = threading.Event()
            self._stop_event = stop_event
            self._running = True
            self._thread = threading.Thread(
                target=self._sub_loop,
                args=(list(topics or []), on_msg, stop_event, generation),
                name="bigqmt-quote-push-sub", daemon=True)
            self._thread.start()

    def _sub_loop(self, topics, on_msg, stop_event, generation):
        # The pubsub connection is owned by THIS thread and closed HERE so a
        # concurrent stop() can't close it out from under us.
        channels = [self._channel(topic) for topic in topics]
        attempt = 0
        while not stop_event.is_set():
            with self._lock:
                if generation != self._generation:
                    return
            pubsub = None
            try:
                pubsub = self.redis.pubsub(ignore_subscribe_messages=True)
                with self._lock:
                    if generation != self._generation:
                        return
                    self._pubsub = pubsub
                pubsub.subscribe(*channels)
                while not stop_event.is_set():
                    with self._lock:
                        if generation != self._generation:
                            return
                    try:
                        message = pubsub.get_message(timeout=0.2)
                    except Exception as exc:
                        self._set_error("receive", exc, "redis receive failed")
                        break
                    if not message or message.get("type") != "message":
                        continue
                    channel = message.get("channel")
                    if isinstance(channel, bytes):
                        channel = channel.decode("utf-8", errors="ignore")
                    topic = str(channel).rsplit(":", 1)[-1]
                    attempts = []
                    try:
                        data = _decode_push_payload(message.get("data"), attempts)
                    except Exception as exc:
                        self._set_error("decode", exc, "invalid push payload")
                        _log_decode_failure(channel, message.get("data"), attempts, exc)
                        continue
                    payload_data = data.get("data") if isinstance(data, dict) else data
                    with self._lock:
                        if generation != self._generation or stop_event.is_set():
                            return
                    try:
                        on_msg(topic, payload_data)
                        attempt = 0
                    except Exception as exc:
                        self._set_error("callback", exc, "subscriber callback failed")
            except Exception as exc:
                self._set_error("subscribe", exc, "redis subscribe failed")
            finally:
                try:
                    if pubsub is not None:
                        pubsub.close()
                except Exception:
                    pass
            if stop_event.is_set():
                return
            attempt += 1
            stop_event.wait(min(0.1 * (2 ** min(attempt - 1, 5)), 2.0))

    def stop(self):
        with self._lock:
            self._running = False
            thread = self._thread
            stop_event = self._stop_event
            self._generation += 1
            if stop_event is not None:
                stop_event.set()
        if thread is not None and thread.is_alive():
            thread.join(1.0)
        with self._lock:
            if self._thread is thread and (thread is None or not thread.is_alive()):
                self._thread = None
                self._stop_event = None
                self._pubsub = None

    def subscriber_thread_alive(self):
        with self._lock:
            return bool(self._thread and self._thread.is_alive())

    def _set_error(self, stage, exc, message):
        self.last_error = {"stage": stage, "type": type(exc).__name__,
                           "message": message, "monotonic": time.monotonic()}
