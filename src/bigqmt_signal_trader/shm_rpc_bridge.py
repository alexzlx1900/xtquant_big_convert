# coding: utf-8
"""显式 SHM RPC callback 桥，复用已有订单处理器与回报归一化。"""

import datetime
import json
import threading
import time
from collections import deque

from .exec_events import normalize_order_event, normalize_trade_event, normalize_order_error_event, normalize_cancel_error_event
from .redis_rpc import to_jsonable
from .transports.readonly_shm import ReadOnlyMailbox, ShmProtocolError, encode_payload, ShmCapacityError
from .transports.shm_rpc import READ_METHODS, WRITE_METHODS


class ShmRpcBridge:
    def __init__(self, *, name, account_id, handlers, allow_order_methods=False,
                 region=None, event_capacity=64, replay_capacity=1024, trade_login_reader=None):
        if not isinstance(account_id, str) or not account_id or handlers.account_id != account_id:
            raise ValueError("handler fixed account mismatch")
        if type(allow_order_methods) is not bool or handlers.allow_order_methods is not allow_order_methods:
            raise ValueError("handler write permission mismatch")
        if type(event_capacity) is not int or not 1 <= event_capacity <= 128 or type(replay_capacity) is not int or not 1 <= replay_capacity <= 10000:
            raise ValueError("bounded capacities required")
        if getattr(handlers, "settle_orders_inline", False):
            raise ValueError("inline blocking settlement is unsupported")
        self.account_id, self.handlers = account_id, handlers
        self.allow_order_methods = allow_order_methods
        self.trade_login_reader = trade_login_reader
        self._thread = threading.get_ident()
        self._events, self._event_sequence = deque(maxlen=event_capacity), 0
        self._lost_sequence = 0
        self._replays, self._replay_capacity = {}, replay_capacity
        self._pending = None
        self.mailbox = ReadOnlyMailbox(name, create=True, region=region)
        try:
            self.mailbox.mark_ready()
        except Exception as exc:
            try:
                self.mailbox.close(failed=True)
            except Exception as cleanup:
                # 初始化错误仍为主异常，清理失败证据独立保留。
                exc.cleanup_error = str(cleanup)
            raise

    def _callback_thread(self):
        if threading.get_ident() != self._thread:
            raise ShmProtocolError("QMT callback thread required")

    def record_execution_event(self, event_type, native_object):
        self._callback_thread()
        normalizers = {"order": normalize_order_event, "trade": normalize_trade_event,
                       "order_error": normalize_order_error_event, "cancel_error": normalize_cancel_error_event}
        if event_type not in normalizers:
            raise ShmProtocolError("unknown execution event type")
        event = to_jsonable(normalizers[event_type](native_object, self.account_id))
        if event.get("account_id") != self.account_id:
            raise ShmProtocolError("event account mismatch")
        # 归一化旧兼容逻辑会回退到order_id；本桥必须只信显式合同字段。
        contract = ""
        for field in ("m_strOrderSysID", "order_sys_id", "order_sysid"):
            value = native_object.get(field) if isinstance(native_object, dict) else getattr(native_object, field, None)
            if type(value) not in (str, int):
                continue
            text = str(value).strip()
            if text and text.upper() not in ("0", "-1", "0.0", "-1.0", "NONE", "NULL", "UNKNOWN"):
                contract = text
                break
        event["order_sys_id"] = contract
        # 请求标识、native局部订单ID及序号只保留归属证据，不能填合同号。
        for source, target in (("request_id", "request_id"), ("order_id", "native_order_id"), ("seq", "native_sequence")):
            value = native_object.get(source) if isinstance(native_object, dict) else getattr(native_object, source, None)
            if type(value) in (str, int):
                event[target] = value
        event["identity_status"] = "KNOWN" if event.get("order_sys_id") else "UNKNOWN"
        self._event_sequence += 1
        try:
            encode_payload(event, 1536)
        except (ShmCapacityError, ValueError, TypeError):
            self._lost_sequence = self._event_sequence
            event = dict(event_type=event_type, account_id=self.account_id,
                         identity_status="UNKNOWN", order_sys_id="", data_loss=True,
                         error="EXECUTION_EVENT_TOO_LARGE_OR_INVALID")
        self._events.append(dict(sequence=self._event_sequence, session_id=self.mailbox.session_id,
                                 account_id=self.account_id, event=event))
        return self._event_sequence

    def _poll_events(self, params):
        after = params.get("after_sequence")
        if type(after) is not int or not 0 <= after <= self._event_sequence:
            raise ShmProtocolError("event cursor is invalid for this session")
        oldest = self._events[0]["sequence"] if self._events else self._event_sequence + 1
        return dict(events=[dict(e) for e in self._events if e["sequence"] > after],
                    session_id=self.mailbox.session_id, native_pid=self.mailbox.server_pid,
                    account_id=self.account_id, latest_sequence=self._event_sequence,
                    oldest_sequence=oldest, gap=after < oldest - 1 or after < self._lost_sequence,
                    dropped_before_sequence=oldest - 1, lost_sequence=self._lost_sequence, automatic_retry=False)

    def _response(self, sequence, request):
        return dict(schema_version=1, sequence=sequence, session_id=self.mailbox.session_id,
                    request_id=request.get("request_id"), account_id=self.account_id,
                    method=request.get("method"), ok=False, data=None, error="", server_error="",
                    handled_at=datetime.datetime.utcnow().isoformat())

    def _write_result(self, method, result, error=""):
        data = to_jsonable(result)
        sysid = data.get("order_sys_id") if isinstance(data, dict) else None
        status = "CONFIRMED" if method == "submit_order" and sysid and not error else "UNKNOWN"
        return dict(status=status, result=data, order_sys_id=str(sysid or ""),
                    automatic_retry=False, error=str(error or ""))

    def _trade_login(self):
        unknown = dict(status="UNKNOWN", account_id=self.account_id, session_id=self.mailbox.session_id,
                       native_pid=self.mailbox.server_pid, source="NONE", raw_status=None,
                       event_observed_at_utc=None, age_seconds=None, fresh=False,
                       positive_contract_verified=False, reason="TRADE_LOGIN_SOURCE_UNAVAILABLE")
        if not callable(self.trade_login_reader):
            return unknown
        try:
            value = self.trade_login_reader()
            if not isinstance(value, dict):
                raise ValueError("trade login evidence must be object")
            if (value.get("account_id"), value.get("session_id"), value.get("native_pid")) != (
                    self.account_id, self.mailbox.session_id, self.mailbox.server_pid):
                unknown["reason"] = "TRADE_LOGIN_IDENTITY_MISMATCH"
                return unknown
            if value.get("status") == "LOGGED_IN" and (value.get("fresh") is not True
                    or value.get("positive_contract_verified") is not True
                    or type(value.get("age_seconds")) not in (int, float)
                    or not 0 <= value["age_seconds"] <= 15 or not value.get("event_observed_at_utc")
                    or value.get("source") != "native_account_callback"):
                unknown.update(source=value.get("source"), raw_status=value.get("raw_status"),
                               reason="TRADE_LOGIN_POSITIVE_EVIDENCE_UNVERIFIED_OR_STALE")
                return unknown
            return value
        except Exception as exc:
            unknown.update(reason="TRADE_LOGIN_SOURCE_FAILED", error=str(exc))
            return unknown

    def _finish(self, sequence, request, response, fingerprint):
        self._replays[request["request_id"]] = (fingerprint, dict(response))
        # 即使发布互斥暂忙也缓存响应，下一 callback 仅重发响应不执行业务。
        self._pending = (sequence, request, response, fingerprint, None)

    def poll_once(self):
        self._callback_thread()
        if self._pending is not None:
            sequence, request, response, fingerprint, settlement = self._pending
            if settlement is not None:
                try:
                    done = self.handlers._apply_order_lookup(settlement, final=time.monotonic() >= settlement.deadline)
                    if not done:
                        return False
                    response.update(ok=True, data=self._write_result("submit_order", settlement.result, settlement.server_error))
                except Exception as exc:
                    response.update(ok=True, data=self._write_result("submit_order", settlement.result, str(exc)))
                self._finish(sequence, request, response, fingerprint)
            if self.mailbox.publish_response(sequence, response):
                self._pending = None
                return True
            return False
        item = self.mailbox.read_request()
        if item is None:
            return False
        sequence, request = item
        if not isinstance(request, dict):
            raise ShmProtocolError("request envelope must be object")
        response = self._response(sequence, request)
        rid, method, params = request.get("request_id"), request.get("method"), request.get("params")
        fingerprint = None
        validated = False
        try:
            if not isinstance(rid, str) or not rid or len(rid) > 128 or any(ord(c) < 33 or ord(c) > 126 for c in rid):
                raise ShmProtocolError("invalid request identity")
            if type(request.get("schema_version")) is not int or request.get("schema_version") != 1 or type(request.get("sequence")) is not int or request.get("sequence") != sequence or request.get("session_id") != self.mailbox.session_id:
                raise ShmProtocolError("request protocol identity mismatch")
            if request.get("account_id") != self.account_id or not isinstance(params, dict):
                raise ShmProtocolError("fixed account/params mismatch")
            nested = params.get("account", self.account_id)
            if isinstance(nested, dict):
                nested = nested.get("account_id", nested.get("accountID", nested.get("id")))
            if params.get("account_id", self.account_id) != self.account_id or nested != self.account_id:
                raise ShmProtocolError("nested account mismatch")
            if method not in READ_METHODS | WRITE_METHODS:
                raise ShmProtocolError("method not allowed")
            if self.handlers.account_id != self.account_id or self.handlers.allow_order_methods is not self.allow_order_methods:
                raise ShmProtocolError("handler identity/permission changed")
            if method in WRITE_METHODS and not self.allow_order_methods:
                raise PermissionError("orders disabled")
            fingerprint = json.dumps([method, params, self.account_id], sort_keys=True, allow_nan=False)
            if rid in self._replays:
                prior_fp, prior = self._replays[rid]
                if prior_fp != fingerprint:
                    raise ShmProtocolError("request_id conflict")
                response.update(prior)
                response["sequence"] = sequence
                self._pending = (sequence, request, response, fingerprint, None)
                return self.poll_once()
            if len(self._replays) >= self._replay_capacity:
                raise ShmProtocolError("session replay capacity exhausted; do not resend unknown writes")
            expires = request.get("read_expires_at_unix")
            if type(expires) not in (int, float) or expires != expires or expires <= time.time() or expires > time.time() + 60:
                raise ShmProtocolError("request deadline expired/invalid; not executed")
            validated = True
            if method == "ping":
                result = dict(pong=True, transport="shm_rpc", account_id=self.account_id,
                              allow_order_methods=self.allow_order_methods, session_id=self.mailbox.session_id,
                              native_pid=self.mailbox.server_pid, capabilities=sorted(READ_METHODS | (WRITE_METHODS if self.allow_order_methods else frozenset())))
            elif method == "poll_execution_events":
                result = self._poll_events(params)
            elif method == "get_trade_login_state":
                diagnostic = getattr(self, "trade_login_diagnostic_reader", None)
                result = diagnostic() if callable(diagnostic) else self._trade_login()
            else:
                if method in WRITE_METHODS:
                    login = self._trade_login()
                    if login.get("status") != "LOGGED_IN":
                        response.update(ok=True, data=dict(status="BLOCKED", automatic_retry=False,
                            native_write_executed=False, order_sys_id="", result=None,
                            error=login.get("reason") or "TRADE_LOGIN_NOT_CONFIRMED", trade_login_state=login))
                        self._finish(sequence, request, response, fingerprint)
                        return self.poll_once()
                business_params = dict(params)
                business_params["account_id"] = self.account_id
                if method == "submit_order":
                    business_params.setdefault("signal_id", rid)
                    business_params.setdefault("remark", "shm:%s" % rid)
                    validate = getattr(self.handlers.order_gateway, "validate_submit_params", None)
                    if callable(validate):
                        validate(business_params)
                result = self.handlers.handle(method, business_params)
                if method in WRITE_METHODS:
                    settlement = self.handlers.take_pending_settlement()
                    response.update(ok=True, data=self._write_result(method, result))
                    if settlement is not None:
                        self._pending = (sequence, request, response, fingerprint, settlement)
                        return self.poll_once()
                    result = response["data"]
            response.update(ok=True, data=to_jsonable(result))
        except Exception as exc:
            if validated and method in WRITE_METHODS:
                response.update(ok=True, data=dict(status="UNKNOWN", automatic_retry=False, error=str(exc), result=None, order_sys_id=""))
            else:
                response.update(error=str(exc), server_error=type(exc).__name__)
        if validated:
            self._finish(sequence, request, response, fingerprint)
        else:
            self._pending = (sequence, request, response, fingerprint, None)
        return self.poll_once()

    def close(self):
        self._callback_thread()
        self.mailbox.close()
