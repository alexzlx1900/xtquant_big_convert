# coding: utf-8
"""显式 SHM RPC callback 桥，复用已有订单处理器与回报归一化。"""

import datetime
import json
import math
import threading
import time
from collections import deque

from .code_utils import normalize_stock_code
from .exec_events import _attr as _event_attr, normalize_order_event, normalize_trade_event, normalize_order_error_event, normalize_cancel_error_event
from .redis_rpc import to_jsonable
from .transports.readonly_shm import ReadOnlyMailbox, ShmProtocolError, encode_payload
from .transports.shm_rpc import (ALL_WRITE_METHODS, ASYNC_SUBMIT_CONTRACT, ASYNC_WRITE_METHODS,
                                 READ_METHODS, WRITE_METHODS, validate_async_submit_params)


EXECUTION_EVENT_GUARD_CONTRACT = "EXECUTION_CURSOR_WRITE_GATE_V1"
EXECUTION_EVENT_NO_WRITE_CONTRACT = "EXECUTION_CURSOR_NO_WRITE_V1"
SUPERVISOR_GATE_EPOCH_CONTRACT = "SUPERVISOR_GATE_EPOCH_V1"
SUPERVISOR_GATE_NO_WRITE_CONTRACT = "SUPERVISOR_GATE_NO_WRITE_V1"


class ExecutionEventGuardRejected(ShmProtocolError):
    pass


class SupervisorGateEpochRejected(ShmProtocolError):
    def __init__(self, current_epoch=None):
        self.current_epoch = current_epoch
        super(SupervisorGateEpochRejected, self).__init__("SUPERVISOR_GATE_EPOCH_MISMATCH")


class ShmRpcBridge:
    def __init__(self, *, name, account_id, handlers, allow_order_methods=False,
                 region=None, event_capacity=64, replay_capacity=1024, trade_login_reader=None,
                 persistent_readonly=False, persistent_simulation_read_write=False,
                 environment="SIM", persistent_live_read_write=False,
                 async_pending_capacity=64, async_confirmation_timeout_seconds=30,
                 execution_event_guard_required=False):
        if not isinstance(account_id, str) or not account_id or handlers.account_id != account_id:
            raise ValueError("handler fixed account mismatch")
        if type(allow_order_methods) is not bool or handlers.allow_order_methods is not allow_order_methods:
            raise ValueError("handler write permission mismatch")
        if type(persistent_readonly) is not bool or (persistent_readonly and (
                account_id != "55013746" or allow_order_methods is not False)):
            raise ValueError("persistent read replay requires fixed simulation and writes disabled")
        if (type(persistent_simulation_read_write) is not bool or persistent_readonly and persistent_simulation_read_write
                or persistent_simulation_read_write and (account_id != "55013746" or allow_order_methods is not True)):
            raise ValueError("persistent read-write replay requires explicit fixed simulation writes")
        if environment not in ("SIM", "LIVE") or type(persistent_live_read_write) is not bool:
            raise ValueError("explicit SIM/LIVE environment and boolean LIVE policy required")
        if environment == "LIVE":
            if (not persistent_live_read_write or persistent_readonly or persistent_simulation_read_write
                    or allow_order_methods is not True or account_id == "55013746"
                    or len(account_id) > 64 or not all("0" <= char <= "9" for char in account_id)):
                raise ValueError("LIVE bridge requires exclusive persistent LIVE writes and live numeric account")
        elif persistent_live_read_write:
            raise ValueError("persistent LIVE policy requires LIVE environment")
        if type(event_capacity) is not int or not 1 <= event_capacity <= 128 or type(replay_capacity) is not int or not 1 <= replay_capacity <= 10000:
            raise ValueError("bounded capacities required")
        if type(async_pending_capacity) is not int or not 1 <= async_pending_capacity <= 128:
            raise ValueError("async pending capacity must be within [1,128]")
        if type(execution_event_guard_required) is not bool:
            raise ValueError("execution event guard selection must be boolean")
        if (type(async_confirmation_timeout_seconds) not in (int, float)
                or not math.isfinite(async_confirmation_timeout_seconds)
                or not 0 < async_confirmation_timeout_seconds <= 300):
            raise ValueError("async confirmation timeout must be within (0,300]")
        if getattr(handlers, "settle_orders_inline", False):
            raise ValueError("inline blocking settlement is unsupported")
        self.account_id, self.handlers = account_id, handlers
        self.environment = environment
        self.allow_order_methods = allow_order_methods
        self.trade_login_reader = trade_login_reader
        self._thread = threading.get_ident()
        self._events, self._event_sequence = deque(maxlen=event_capacity), 0
        self._lost_sequence = 0
        self._event_observed_cursor = 0
        self._event_cursor_observed = False
        self._off_thread_event_loss = threading.Event()
        self._read_publication_lock = threading.Lock()
        self._off_thread_event_loss_recorded = False
        self._replays, self._replay_capacity = {}, replay_capacity
        self.persistent_readonly = persistent_readonly
        self.persistent_simulation_read_write = persistent_simulation_read_write
        self.persistent_live_read_write = persistent_live_read_write
        self.persistent = persistent_readonly or persistent_simulation_read_write or persistent_live_read_write
        self.runtime_metadata = {}
        self.read_rotation_evidence_reader = None
        self.read_rotation_control = None
        self.read_rotation_control_status_reader = None
        self.read_rotation_capability_control = None
        self.read_rotation_capability_status_reader = None
        self.supervisor_attach_control = None
        self.supervisor_attach_status_reader = None
        self._async_pending, self._async_claims, self._async_contract_claims = {}, {}, {}
        self._async_pending_capacity = async_pending_capacity
        self._async_confirmation_timeout_seconds = async_confirmation_timeout_seconds
        self._async_write_block_reason = ""
        self._read_replay_expiries = {}
        self._active_write_event_guard = None
        self._active_supervisor_gate_epoch = None
        self.execution_event_guard_required = execution_event_guard_required
        self.native_write_runtime_guard = None
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

    def _async_data(self, request, status, *, native_write_executed=False, error=""):
        params = request.get("params") or {}
        data = dict(status=status, request_id=request.get("request_id"),
                    client_order_id=params.get("client_order_id"), account_id=self.account_id,
                    environment=self.environment, session_id=self.mailbox.session_id,
                    native_pid=self.mailbox.server_pid,
                    windows_session_id=self.runtime_metadata.get("windows_session_id"),
                    async_submit_contract=ASYNC_SUBMIT_CONTRACT, native_write_executed=native_write_executed,
                    broker_accepted=False, qmt_async_seq=None, order_sys_id="", automatic_retry=False,
                    writes_blocked=bool(self._async_write_block_reason),
                    write_block_reason=self._async_write_block_reason, error=str(error or ""),
                    source_evidence={"account_id": self.account_id})
        if status == "PENDING_CONFIRMATION":
            data["receipt_type"] = "NATIVE_CALL_RETURNED"
        return self._attach_source_identity(data)

    def _check_execution_event_guard(self, guard):
        self._callback_thread()
        if not self.execution_event_guard_required:
            return
        if self._off_thread_event_loss.is_set():
            self._flush_off_thread_event_loss()
        oldest = self._events[0]["sequence"] if self._events else self._event_sequence + 1
        if (not isinstance(guard, dict) or set(guard) != {"contract", "execution_session_id", "native_pid", "sequence"}
                or guard.get("contract") != EXECUTION_EVENT_GUARD_CONTRACT
                or guard.get("execution_session_id") != self.mailbox.session_id
                or type(guard.get("native_pid")) is not int or guard["native_pid"] != self.mailbox.server_pid
                or type(guard.get("sequence")) is not int or guard["sequence"] < 0
                or not self._event_cursor_observed or guard["sequence"] != self._event_sequence
                or guard["sequence"] > self._event_observed_cursor
                or guard["sequence"] < oldest - 1 or self._lost_sequence != 0
                or self._off_thread_event_loss.is_set()):
            raise ExecutionEventGuardRejected("EXECUTION_EVENT_CURSOR_NOT_CURRENT")

    def check_native_write_guard(self):
        self._check_execution_event_guard(self._active_write_event_guard)

    def check_native_supervisor_gate_epoch(self, lease):
        expected = self._active_supervisor_gate_epoch
        if (not isinstance(expected, dict) or set(expected) != {"contract", "epoch"}
                or expected.get("contract") != SUPERVISOR_GATE_EPOCH_CONTRACT
                or type(expected.get("epoch")) is not int or not 1 <= expected["epoch"] <= 0xFFFFFFFFFFFFFFFF):
            raise SupervisorGateEpochRejected()
        try:
            current = lease.read_epoch()
        except Exception:
            current = None
        if type(current) is not int or not 1 <= current <= 0xFFFFFFFFFFFFFFFF:
            raise SupervisorGateEpochRejected()
        if current != expected["epoch"]:
            raise SupervisorGateEpochRejected(current)

    def _execution_guard_blocked(self, request):
        data = (self._async_data(request, "BLOCKED", error="EXECUTION_EVENT_CURSOR_NOT_CURRENT")
                if request.get("method") in ASYNC_WRITE_METHODS else
                dict(status="BLOCKED", request_id=request.get("request_id"), account_id=self.account_id,
                     environment=self.environment, session_id=self.mailbox.session_id,
                     native_pid=self.mailbox.server_pid, windows_session_id=self.runtime_metadata.get("windows_session_id"),
                     native_write_executed=False, automatic_retry=False, order_sys_id="", result=None,
                     writes_blocked=bool(self._async_write_block_reason), write_block_reason=self._async_write_block_reason,
                     error="EXECUTION_EVENT_CURSOR_NOT_CURRENT"))
        data.update(no_write_contract=EXECUTION_EVENT_NO_WRITE_CONTRACT,
                    execution_event_guard=(request.get("params") or {}).get("execution_event_guard"),
                    native_event_sequence=self._event_sequence, native_lost_sequence=self._lost_sequence)
        return data

    def _runtime_guard_blocked(self, request):
        error = "SUPERVISOR_WRITE_GATE_UNAVAILABLE"
        self._block_async_writes(error)
        data = (self._async_data(request, "BLOCKED", error=error) if request.get("method") in ASYNC_WRITE_METHODS else
                dict(status="BLOCKED", request_id=request.get("request_id"), account_id=self.account_id,
                     environment=self.environment, session_id=self.mailbox.session_id,
                     native_pid=self.mailbox.server_pid, windows_session_id=self.runtime_metadata.get("windows_session_id"),
                     native_write_executed=False, automatic_retry=False, order_sys_id="", result=None,
                     writes_blocked=True, write_block_reason=self._async_write_block_reason, error=error))
        return data

    def _supervisor_epoch_blocked(self, request, current_epoch):
        error = "SUPERVISOR_GATE_EPOCH_MISMATCH" if current_epoch is not None else "SUPERVISOR_GATE_EPOCH_UNVERIFIED"
        if current_epoch is None:
            self._block_async_writes(error)
        data = (self._async_data(request, "BLOCKED", error=error) if request.get("method") in ASYNC_WRITE_METHODS else
                dict(status="BLOCKED", request_id=request.get("request_id"), account_id=self.account_id,
                     environment=self.environment, session_id=self.mailbox.session_id,
                     native_pid=self.mailbox.server_pid, windows_session_id=self.runtime_metadata.get("windows_session_id"),
                     native_write_executed=False, automatic_retry=False, order_sys_id="", result=None,
                     writes_blocked=bool(self._async_write_block_reason), write_block_reason=self._async_write_block_reason,
                     error=error))
        data.update(no_write_contract=SUPERVISOR_GATE_NO_WRITE_CONTRACT,
                    supervisor_gate_epoch=(request.get("params") or {}).get("supervisor_gate_epoch"),
                    native_supervisor_gate_epoch=current_epoch)
        return data

    def _block_async_writes(self, reason, record=None):
        if not self._async_write_block_reason:
            self._async_write_block_reason = reason
        records = list(self._async_pending.values()) if record is None else [record]
        for pending in records:
            if pending["state"] in ("CONFIRMED", "REJECTED"):
                continue
            pending["state"], pending["error"] = "UNKNOWN", reason
            data = pending.get("response_data")
            if data is not None:
                data.clear()
                data.update(self._async_data(pending["request"], "UNKNOWN",
                    native_write_executed=pending["native_write_executed"], error=reason))

    def _refresh_async_pending(self):
        now = time.monotonic()
        for pending in list(self._async_pending.values()):
            if pending["state"] != "UNKNOWN" and now >= pending["deadline"]:
                self._block_async_writes("ASYNC_CONFIRMATION_DEADLINE; outcome=UNKNOWN", pending)

    def _async_order_facts(self, params):
        action = self.handlers._order_action_from_params(params)
        if action not in ("BUY", "SELL"):
            raise ShmProtocolError("async order action must be BUY or SELL")
        stock_code = normalize_stock_code(str(params.get("stock_code") or ""))
        if not stock_code:
            raise ShmProtocolError("explicit async order stock_code required")
        volume = params.get("volume") or params.get("order_volume") or 0
        price = params.get("price")
        price = 0 if price in (None, "") else price
        if (isinstance(volume, bool) or isinstance(price, bool) or int(volume) <= 0
                or float(volume) != int(volume) or not math.isfinite(float(price)) or float(price) < 0):
            raise ShmProtocolError("explicit valid async order quantity and price required")
        return dict(stock_code=stock_code, action=action, volume=int(volume), price=float(price))

    def _submit_async(self, request, fingerprint):
        params, rid = request["params"], request["request_id"]
        cid = validate_async_submit_params(params)
        if type(self.runtime_metadata.get("windows_session_id")) is not int or self.runtime_metadata["windows_session_id"] < 0:
            return self._async_data(request, "BLOCKED", error="ASYNC_WINDOWS_SESSION_UNVERIFIED")
        prior = self._async_claims.get(cid)
        if prior is not None:
            if prior["fingerprint"] != fingerprint:
                self._block_async_writes("ASYNC_IDENTITY_CONFLICT; outcome=UNKNOWN", prior)
                return self._async_data(request, "UNKNOWN", error=self._async_write_block_reason)
            return self._async_data(request, "BLOCKED", error="CLIENT_ORDER_ID_ALREADY_CLAIMED")
        if len(self._async_pending) >= self._async_pending_capacity:
            return self._async_data(request, "BLOCKED", error="ASYNC_PENDING_CAPACITY_EXHAUSTED")
        try:
            facts = self._async_order_facts(params)
            business_params = dict(params, account_id=self.account_id, remark=cid, wait_settlement=False)
            business_params.pop("execution_event_guard", None)
            business_params.pop("supervisor_gate_epoch", None)
            business_params.setdefault("signal_id", rid)
            validate = getattr(self.handlers.order_gateway, "validate_submit_params", None)
            if callable(validate):
                validate(business_params)
        except (ValueError, TypeError, OverflowError, ShmProtocolError) as exc:
            return self._async_data(request, "BLOCKED", error=str(exc))
        pending = dict(request=request, request_id=rid, client_order_id=cid, fingerprint=fingerprint,
                       facts=facts, deadline=time.monotonic() + self._async_confirmation_timeout_seconds,
                       state="NATIVE_CALL_IN_PROGRESS", native_write_executed=None, error="")
        # passorder可能在返回前同步触发回调，必须先登记完整归属。
        self._async_claims[cid] = self._async_pending[cid] = pending
        try:
            result = to_jsonable(self.handlers.handle("submit_order", business_params))
            if (not isinstance(result, dict) or result.get("status") != "SUBMITTED"
                    or result.get("user_order_id") != cid or result.get("confirmation_error")
                    or result.get("error") or result.get("server_error")
                    or result.get("explicit_failure") is True or result.get("success") is False):
                raise ShmProtocolError("native async submit result identity/outcome unconfirmed")
            pending["native_write_executed"] = True
            if pending["state"] == "NATIVE_CALL_IN_PROGRESS":
                pending["state"] = "PENDING_CONFIRMATION"
            self._refresh_async_pending()
            if self._async_write_block_reason and pending["state"] not in ("CONFIRMED", "REJECTED"):
                self._block_async_writes(self._async_write_block_reason, pending)
            status = "UNKNOWN" if pending["state"] == "UNKNOWN" else "PENDING_CONFIRMATION"
            data = self._async_data(request, status, native_write_executed=True, error=pending["error"])
        except ExecutionEventGuardRejected:
            pending["state"], pending["native_write_executed"] = "NOT_EXECUTED", False
            self._async_pending.pop(cid, None)
            data = self._execution_guard_blocked(request)
        except SupervisorGateEpochRejected as exc:
            pending["state"], pending["native_write_executed"] = "NOT_EXECUTED", False
            self._async_pending.pop(cid, None)
            data = self._supervisor_epoch_blocked(request, exc.current_epoch)
        except Exception as exc:
            error = "NATIVE_ASYNC_CALL_UNCONFIRMED:%s; outcome=UNKNOWN" % exc.__class__.__name__
            self._block_async_writes(error, pending)
            data = self._async_data(request, "UNKNOWN", native_write_executed=pending["native_write_executed"], error=error)
        pending["response_data"] = data
        return data

    def _confirm_async_event(self, event_type, native_object, event):
        self._refresh_async_pending()
        if event_type not in ("order", "trade"):
            return
        cid = _event_attr(native_object, ("m_strRemark", "client_order_id", "user_order_id", "order_remark", "remark"))
        if not isinstance(cid, str):
            return
        pending = self._async_claims.get(cid)
        if pending is None or pending["state"] == "UNKNOWN":
            return
        if pending["state"] == "NOT_EXECUTED":
            self._block_async_writes("ASYNC_BLOCKED_CID_EVENT_CONFLICT; outcome=UNKNOWN")
            return
        account = _event_attr(native_object, ("m_strAccountID", "account_id"))
        quantity_fields = (("m_nVolumeTotalOriginal", "order_volume", "volume") if event_type == "order"
                           else ("m_nVolume", "volume", "traded_volume"))
        quantity = _event_attr(native_object, quantity_fields)
        price = event.get("price")
        rejected = event_type == "order" and event.get("status") in (57, "57")
        if (account is None or not event.get("stock_code") or not event.get("action")
                or quantity is None or price is None or (not event.get("order_sys_id") and not rejected)):
            return
        try:
            facts = pending["facts"]
            matches = (account == self.account_id and event["stock_code"] == facts["stock_code"]
                       and event["action"] == facts["action"] and not isinstance(quantity, bool)
                       and not isinstance(price, bool) and math.isfinite(float(quantity))
                       and float(quantity).is_integer() and math.isfinite(float(price)))
            if event_type == "trade":
                # 成交量/价是实际执行事实，部分成交及价格改善不等于委托身份冲突。
                matches = matches and 0 < float(quantity) <= facts["volume"] and float(price) > 0
            else:
                matches = matches and float(quantity) == facts["volume"] and float(price) == facts["price"]
        except (TypeError, ValueError, OverflowError):
            matches = False
        if not matches:
            self._block_async_writes("ASYNC_CALLBACK_IDENTITY_CONFLICT; outcome=UNKNOWN", pending)
            return
        contract = event["order_sys_id"]
        if pending["state"] == "REJECTED":
            if not rejected:
                self._block_async_writes("ASYNC_REJECTED_CALLBACK_CONFLICT; outcome=UNKNOWN", pending)
                return
            if contract:
                prior_contract = pending.get("order_sys_id")
                owner = self._async_contract_claims.get(contract)
                if prior_contract and prior_contract != contract:
                    pending["conflicting_order_sys_id"] = contract
                    self._block_async_writes("ASYNC_CONTRACT_CONFLICT; outcome=UNKNOWN", pending)
                    return
                if owner is not None and owner != cid:
                    pending["conflicting_order_sys_id"] = contract
                    self._block_async_writes("ASYNC_CONTRACT_OWNERSHIP_CONFLICT; outcome=UNKNOWN", pending)
                    return
                # 同一完整原单的首次迟到合同只补身份，拒单终态保持。
                pending["order_sys_id"] = contract
                self._async_contract_claims[contract] = cid
            event.update(client_order_id=cid, request_id=pending["request_id"],
                         async_submit_contract=ASYNC_SUBMIT_CONTRACT, async_terminal_status="REJECTED")
            return
        if rejected and not contract:
            if pending["state"] == "CONFIRMED":
                return
            pending["state"], pending["native_write_executed"] = "REJECTED", True
            self._async_pending.pop(cid, None)
            event.update(client_order_id=cid, request_id=pending["request_id"],
                         async_submit_contract=ASYNC_SUBMIT_CONTRACT, async_terminal_status="REJECTED")
            return
        if pending["state"] == "CONFIRMED" and contract != pending["order_sys_id"]:
            pending["conflicting_order_sys_id"] = contract
            self._block_async_writes("ASYNC_CONTRACT_CONFLICT; outcome=UNKNOWN", pending)
            return
        owner = self._async_contract_claims.get(contract)
        if owner is not None and owner != cid:
            pending["conflicting_order_sys_id"] = contract
            self._block_async_writes("ASYNC_CONTRACT_OWNERSHIP_CONFLICT; outcome=UNKNOWN", pending)
            return
        pending["state"], pending["native_write_executed"] = "REJECTED" if rejected else "CONFIRMED", True
        pending["order_sys_id"] = contract
        self._async_contract_claims[contract] = cid
        self._async_pending.pop(cid, None)
        event["client_order_id"], event["request_id"] = cid, pending["request_id"]
        event["async_submit_contract"] = ASYNC_SUBMIT_CONTRACT
        if rejected:
            event["async_terminal_status"] = "REJECTED"

    def _append_execution_event(self, sequence, event):
        if (self._events and len(self._events) == self._events.maxlen
                and self._events[0]["sequence"] > self._event_observed_cursor):
            self._lost_sequence = max(self._lost_sequence, self._events[0]["sequence"])
            self._block_async_writes("EXECUTION_EVENT_GAP; outcome=UNKNOWN")
        self._events.append(dict(sequence=sequence, session_id=self.mailbox.session_id,
                                 account_id=self.account_id, event=event))

    def flag_off_thread_execution_event(self):
        # 只与响应头提交短暂串行；原生调用及监督器 pipe 等待均不持此锁。
        with self._read_publication_lock:
            self._off_thread_event_loss.set()

    def _flush_off_thread_event_loss(self):
        if self._off_thread_event_loss.is_set() and not self._off_thread_event_loss_recorded:
            self._off_thread_event_loss_recorded = True
            self.record_execution_event_loss("UNKNOWN", "EXECUTION_CALLBACK_WRONG_THREAD")

    def record_execution_event_loss(self, event_type, reason, sequence=None):
        if self.execution_event_guard_required and threading.get_ident() != self._thread:
            self.flag_off_thread_execution_event()
            return None
        self._callback_thread()
        if sequence is None:
            self._event_sequence += 1
            sequence = self._event_sequence
        self._lost_sequence = max(self._lost_sequence, sequence)
        self._block_async_writes("EXECUTION_EVENT_LOSS; outcome=UNKNOWN")
        self._append_execution_event(sequence, dict(event_type="data_loss", source_event_type=event_type,
            account_id=self.account_id, identity_status="UNKNOWN", data_loss=True,
            error=str(reason or "EXECUTION_EVENT_INVALID")))
        return sequence

    def record_execution_event(self, event_type, native_object):
        if self.execution_event_guard_required and threading.get_ident() != self._thread:
            self.flag_off_thread_execution_event()
            return None
        self._callback_thread()
        normalizers = {"order": normalize_order_event, "trade": normalize_trade_event,
                       "order_error": normalize_order_error_event, "cancel_error": normalize_cancel_error_event}
        if event_type not in normalizers:
            raise ShmProtocolError("unknown execution event type")
        sequence = None
        if self.execution_event_guard_required:
            self._event_sequence += 1
            sequence = self._event_sequence
        try:
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
            for source, target in (("request_id", "request_id"), ("order_id", "native_order_id"), ("seq", "native_sequence")):
                value = native_object.get(source) if isinstance(native_object, dict) else getattr(native_object, source, None)
                if type(value) in (str, int):
                    event[target] = value
            event["identity_status"] = "KNOWN" if event.get("order_sys_id") else "UNKNOWN"
            if (not self.execution_event_guard_required and self._async_claims and self._events
                    and len(self._events) == self._events.maxlen
                    and self._events[0]["sequence"] > self._event_observed_cursor):
                self._block_async_writes("ASYNC_EXECUTION_EVENT_GAP; outcome=UNKNOWN")
            self._confirm_async_event(event_type, native_object, event)
        except Exception as exc:
            if self.execution_event_guard_required:
                return self.record_execution_event_loss(event_type, "EXECUTION_EVENT_INVALID:%s" % type(exc).__name__, sequence)
            raise
        if not self.execution_event_guard_required:
            self._event_sequence += 1
            sequence = self._event_sequence
        try:
            encode_payload(event, 1536)
        except Exception as exc:
            if self.execution_event_guard_required:
                return self.record_execution_event_loss(event_type, "EXECUTION_EVENT_INVALID:%s" % type(exc).__name__, sequence)
            if not isinstance(exc, (ValueError, TypeError)):
                raise
            self._lost_sequence = sequence
            if self._async_claims:
                self._block_async_writes("ASYNC_EXECUTION_EVENT_LOSS; outcome=UNKNOWN")
            event = dict(event_type=event_type, account_id=self.account_id,
                         identity_status="UNKNOWN", order_sys_id="", data_loss=True,
                         error="EXECUTION_EVENT_TOO_LARGE_OR_INVALID")
        if self.execution_event_guard_required:
            self._append_execution_event(sequence, event)
        else:
            self._events.append(dict(sequence=sequence, session_id=self.mailbox.session_id,
                                     account_id=self.account_id, event=event))
        return sequence

    def _poll_events(self, params):
        after = params.get("after_sequence")
        if type(after) is not int or not 0 <= after <= self._event_sequence:
            raise ShmProtocolError("event cursor is invalid for this session")
        oldest = self._events[0]["sequence"] if self._events else self._event_sequence + 1
        gap = after < oldest - 1 or after < self._lost_sequence
        self._event_observed_cursor = max(self._event_observed_cursor, after)
        self._event_cursor_observed = True
        if gap and (self.execution_event_guard_required or self._async_claims):
            reason = "EXECUTION_EVENT_GAP" if self.execution_event_guard_required else "ASYNC_EXECUTION_EVENT_GAP"
            self._block_async_writes(reason + "; outcome=UNKNOWN")
        return dict(events=[dict(e) for e in self._events if e["sequence"] > after],
                    session_id=self.mailbox.session_id, native_pid=self.mailbox.server_pid,
                    windows_session_id=self.runtime_metadata.get("windows_session_id"), environment=self.environment,
                    account_id=self.account_id, latest_sequence=self._event_sequence,
                    oldest_sequence=oldest, gap=gap,
                    dropped_before_sequence=oldest - 1, lost_sequence=self._lost_sequence, automatic_retry=False)

    def _response(self, sequence, request):
        event_poll = request.get("method") == "poll_execution_events"
        started_monotonic = time.monotonic() if event_poll else None
        response = dict(schema_version=1, sequence=sequence, session_id=self.mailbox.session_id,
                    request_id=request.get("request_id"), account_id=self.account_id, environment=self.environment,
                    method=request.get("method"), ok=False, data=None, error="", server_error="",
                    handled_at=datetime.datetime.utcnow().isoformat())
        if request.get("method") in ("get_positions", "poll_execution_events"):
            response["read_timing"] = {"native_started_at_unix": time.time()}
            if event_poll:
                response["read_timing"].update(native_started_at_monotonic=started_monotonic,
                                               native_started_at_monotonic_after=time.monotonic())
        return response

    def _write_result(self, method, result, error=""):
        data = to_jsonable(result)
        sysid = str(data.get("order_sys_id") or "").strip() if isinstance(data, dict) else ""
        valid_contract = sysid.upper() not in ("", "0", "-1", "0.0", "-1.0", "NONE", "NULL", "UNKNOWN")
        cancellation = (method == "cancel_order" and isinstance(data, dict)
                        and data.get("cancellation_confirmed") is True and data.get("success") is True
                        and data.get("order_terminal") is True and data.get("user_order_id")
                        and data.get("confirmation_source") == "native_query_orders_strict"
                        and str(data.get("native_order_status")) in ("53", "54"))
        if not error and isinstance(data, dict):
            error = data.get("confirmation_error") or ""
        status = "CONFIRMED" if valid_contract and not error and (method == "submit_order" or cancellation) else "UNKNOWN"
        return dict(status=status, result=data, order_sys_id=str(sysid or ""),
                    automatic_retry=False, error=str(error or ""))

    def _poll_cancel_confirmation(self, settlement):
        final = time.monotonic() >= settlement["deadline"]
        result = settlement["result"]
        try:
            result = self.handlers.order_gateway.query_cancel_confirmation(result)
            settlement["result"] = result
            settlement["error"] = ""
        except Exception as exc:
            settlement["error"] = "CANCEL_CONFIRMATION_LOOKUP_FAILED:%s; outcome=UNKNOWN" % exc.__class__.__name__
        pending = getattr(result, "confirmation_pending", False) is True
        if not final and (pending or settlement["error"]):
            return None
        error = settlement["error"]
        if pending and not error:
            error = "CANCEL_CONFIRMATION_DEADLINE; outcome=UNKNOWN"
        return self._write_result("cancel_order", result, error)

    def _trade_login_query_valid(self, value):
        diagnostic = value.get("query_diagnostic")
        age, stamp = value.get("sample_age_seconds"), value.get("sample_observed_at_utc")
        key = value.get("raw_account_key")
        if not (self.environment == "LIVE" and type(value.get("login_evidence_version")) is int
                and value["login_evidence_version"] == 3 and value.get("source") == "native_account_query"
                and value.get("freshness_basis") == "current_native_account_query"
                and value.get("account_instance_contract") == "LIVE_STOCK_ACCOUNT_QUERY_V1"
                and value.get("fresh") is True and value.get("positive_contract_verified") is True
                and type(age) in (int, float) and math.isfinite(age) and 0 <= age <= 15
                and value.get("raw_account_id") == self.account_id and value.get("raw_status") == "\u767b\u5f55\u6210\u529f"
                and type(value.get("raw_broker_type")) is int and value["raw_broker_type"] == 2
                and isinstance(key, str) and 0 < len(key) <= 256 and key.strip()
                and value.get("bound_account_key") == key and isinstance(diagnostic, dict)
                and diagnostic.get("source") == "native_get_trade_detail_data_ACCOUNT"
                and diagnostic.get("proves_login") is True and not diagnostic.get("error")
                and diagnostic.get("query_observed_at_utc") == stamp
                and all(diagnostic.get(field) == value.get(field) for field in
                        ("raw_account_id", "raw_status", "raw_broker_type", "raw_account_key"))
                and isinstance(stamp, str) and stamp.endswith("Z")):
            return False
        for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ"):
            try:
                wall_age = (datetime.datetime.utcnow() - datetime.datetime.strptime(stamp, fmt)).total_seconds()
                return -1 <= wall_age <= 15 and abs(wall_age - age) <= 1
            except ValueError:
                pass
        return False

    def _trade_login(self):
        unknown = dict(status="UNKNOWN", account_id=self.account_id, session_id=self.mailbox.session_id,
                       native_pid=self.mailbox.server_pid, source="NONE", raw_status=None,
                       event_observed_at_utc=None, age_seconds=None, fresh=False,
                       positive_contract_verified=False, reason="TRADE_LOGIN_SOURCE_UNAVAILABLE")
        if self.execution_event_guard_required and (self._off_thread_event_loss.is_set() or self._lost_sequence):
            unknown["reason"] = "NATIVE_CALLBACK_CONTINUITY_UNVERIFIED"
            return unknown
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
            valid = (value.get("fresh") is True and value.get("positive_contract_verified") is True
                     and type(value.get("age_seconds")) in (int, float) and math.isfinite(value["age_seconds"])
                     and value["age_seconds"] >= 0 and value.get("event_observed_at_utc")
                     and value.get("source") == "native_account_callback")
            if type(value.get("login_evidence_version")) is int and value["login_evidence_version"] == 3:
                valid = self._trade_login_query_valid(value)
            elif "login_evidence_version" in value:
                diagnostic = value.get("query_diagnostic")
                valid = (valid and type(value["login_evidence_version"]) is int and value["login_evidence_version"] == 2
                    and value.get("freshness_basis") == "native_callback_state_with_current_account_sample"
                    and type(value.get("state_event_age_seconds")) in (int, float)
                    and value["state_event_age_seconds"] == value["age_seconds"]
                    and type(value.get("sample_age_seconds")) in (int, float)
                    and math.isfinite(value["sample_age_seconds"]) and 0 <= value["sample_age_seconds"] <= 15
                    and isinstance(value.get("sample_observed_at_utc"), str) and bool(value["sample_observed_at_utc"])
                    and value.get("raw_account_id") == self.account_id and value.get("raw_status") == "\u767b\u5f55\u6210\u529f"
                    and isinstance(diagnostic, dict) and diagnostic.get("proves_login") is False
                    and diagnostic.get("source") == "native_query_ACCOUNT_cache"
                    and diagnostic.get("query_observed_at_utc") == value["sample_observed_at_utc"]
                    and diagnostic.get("raw_account_id") == self.account_id
                    and diagnostic.get("raw_status") == value["raw_status"] and not diagnostic.get("error"))
                if valid:
                    # v2由本次原生采样产生；拒绝伪造日期或冻结旧sample_age的旧采样。
                    for field in ("event_observed_at_utc", "sample_observed_at_utc"):
                        stamp = value[field]
                        if not isinstance(stamp, str) or not stamp.endswith("Z"):
                            valid = False
                            break
                        parsed = None
                        for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ"):
                            try:
                                parsed = datetime.datetime.strptime(stamp, fmt)
                                break
                            except ValueError:
                                pass
                        if parsed is None:
                            valid = False
                            break
                        if field == "sample_observed_at_utc":
                            wall_age = (datetime.datetime.utcnow() - parsed).total_seconds()
                            valid = -1 <= wall_age <= 15 and abs(wall_age - value["sample_age_seconds"]) <= 1
            else:
                valid = valid and value["age_seconds"] <= 15
            if value.get("status") == "LOGGED_IN" and not valid:
                unknown.update(source=value.get("source"), raw_status=value.get("raw_status"),
                               reason="TRADE_LOGIN_POSITIVE_EVIDENCE_UNVERIFIED_OR_STALE")
                return unknown
            return value
        except Exception as exc:
            unknown.update(reason="TRADE_LOGIN_SOURCE_FAILED", error=str(exc))
            return unknown

    def _finish(self, sequence, request, response, fingerprint):
        self._replays[request["request_id"]] = (fingerprint, dict(response))
        if request["method"] in ASYNC_WRITE_METHODS:
            record = self._async_claims.get(request["params"].get("client_order_id"))
            if record is not None and record["request_id"] == request["request_id"]:
                # to_jsonable会复制字典；让未知转移更新实际缓存/待发布的响应。
                record["response_data"] = response["data"]
        if self.persistent and request["method"] in READ_METHODS:
            self._read_replay_expiries[request["request_id"]] = (request["read_expires_at_unix"], sequence)
        # 即使发布互斥暂忙也缓存响应，下一 callback 仅重发响应不执行业务。
        self._pending = (sequence, request, response, fingerprint, None)

    def _attach_source_identity(self, value):
        if not isinstance(value, dict):
            return value
        for key in ("source_evidence", "orders_source_evidence", "trades_source_evidence"):
            evidence = value.get(key)
            if isinstance(evidence, dict):
                value[key] = dict(evidence, session_id=self.mailbox.session_id,
                    native_pid=self.mailbox.server_pid, windows_session_id=self.runtime_metadata.get("windows_session_id"),
                    environment=self.environment)
        return value

    def _read_with_evidence(self, method, params):
        provider = self.handlers.position_provider
        if method in ("query_orders", "query_trades") and params.get("include_source_evidence") is True:
            function = getattr(provider, "get_orders_with_evidence" if method == "query_orders" else "get_trades_with_evidence", None)
            if callable(function):
                if set(params) - {"account_id", "strategy_name", "include_source_evidence"}:
                    raise ShmProtocolError("unsupported evidence record parameter")
                strategy = params.get("strategy_name", "")
                if not isinstance(strategy, str) or len(strategy) > 128 or any(ord(char) < 32 for char in strategy):
                    raise ShmProtocolError("bounded strategy string required")
                return function(self.account_id, strategy)
            raise ShmProtocolError("native record evidence unavailable")
        if method == "query_execution_snapshot":
            function = getattr(provider, "get_execution_snapshot_with_evidence", None)
            if callable(function):
                order_strategy = params.get("order_strategy_name", params.get("strategy_name", "bigqmt_signal_trader"))
                trade_strategy = params.get("trade_strategy_name", "")
                if any(not isinstance(value, str) or len(value) > 128 or any(ord(char) < 32 for char in value)
                       for value in (order_strategy, trade_strategy)):
                    raise ShmProtocolError("bounded snapshot strategy strings required")
                return function(self.account_id, order_strategy, trade_strategy)
        return self.handlers.handle(method, params)

    def _expire_confirmed_read_replays(self):
        if not self.persistent or self._pending is not None:
            return
        region = self.mailbox.region
        if not region.acquire(0):
            return
        try:
            confirmed = self.mailbox._header()[6]
        finally:
            region.release()
        now = time.time()
        for rid, (expires, sequence) in list(self._read_replay_expiries.items()):
            if expires <= now and sequence <= confirmed:
                self._read_replay_expiries.pop(rid)
                self._replays.pop(rid, None)

    def poll_once(self):
        self._callback_thread()
        if self.execution_event_guard_required:
            self._flush_off_thread_event_loss()
        self._refresh_async_pending()
        self._expire_confirmed_read_replays()
        if self._pending is not None:
            sequence, request, response, fingerprint, settlement = self._pending
            if settlement is not None:
                try:
                    if request["method"] == "cancel_order":
                        data = self._poll_cancel_confirmation(settlement)
                        if data is None:
                            return False
                    else:
                        done = self.handlers._apply_order_lookup(settlement, final=time.monotonic() >= settlement.deadline)
                        if not done:
                            return False
                        data = self._write_result("submit_order", settlement.result, settlement.server_error)
                    response.update(ok=True, data=data)
                except Exception as exc:
                    result = settlement["result"] if request["method"] == "cancel_order" else settlement.result
                    response.update(ok=True, data=self._write_result(request["method"], result,
                        "WRITE_CONFIRMATION_FAILED:%s; outcome=UNKNOWN" % exc.__class__.__name__))
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
            if request.get("environment", None if self.environment == "LIVE" else "SIM") != self.environment:
                raise ShmProtocolError("request environment mismatch")
            nested = params.get("account", self.account_id)
            if isinstance(nested, dict):
                nested = nested.get("account_id", nested.get("accountID", nested.get("id")))
            if params.get("account_id", self.account_id) != self.account_id or nested != self.account_id:
                raise ShmProtocolError("nested account mismatch")
            if method not in READ_METHODS | ALL_WRITE_METHODS:
                raise ShmProtocolError("method not allowed")
            if self.handlers.account_id != self.account_id or self.handlers.allow_order_methods is not self.allow_order_methods:
                raise ShmProtocolError("handler identity/permission changed")
            if method in ALL_WRITE_METHODS and not self.allow_order_methods:
                raise PermissionError("orders disabled")
            expires = request.get("read_expires_at_unix")
            if self.persistent and (type(expires) not in (int, float) or expires != expires
                                            or expires <= time.time() or expires > time.time() + 60):
                raise ShmProtocolError("request deadline expired/invalid; not executed")
            if method in ASYNC_WRITE_METHODS:
                validate_async_submit_params(params)
            fingerprint = json.dumps([method, params, self.account_id], sort_keys=True, allow_nan=False)
            if rid in self._replays:
                prior_fp, prior = self._replays[rid]
                if prior_fp != fingerprint:
                    if method in ASYNC_WRITE_METHODS or prior.get("method") in ASYNC_WRITE_METHODS:
                        self._block_async_writes("ASYNC_IDENTITY_CONFLICT; outcome=UNKNOWN")
                        if method in ASYNC_WRITE_METHODS:
                            response.update(ok=True, data=self._async_data(request, "UNKNOWN", error=self._async_write_block_reason))
                            # 冲突请求不能覆盖原RID的历史收据或指纹。
                            self._pending = (sequence, request, response, fingerprint, None)
                            return self.poll_once()
                    raise ShmProtocolError("request_id conflict")
                if self.persistent and method in READ_METHODS:
                    original_expires = self._read_replay_expiries[rid][0]
                    if original_expires <= time.time():
                        raise ShmProtocolError("original read request expired; cached identity cannot be renewed")
                    self._read_replay_expiries[rid] = (original_expires, sequence)
                response.update(prior)
                response["sequence"] = sequence
                self._pending = (sequence, request, response, fingerprint, None)
                return self.poll_once()
            if len(self._replays) >= self._replay_capacity:
                raise ShmProtocolError("session replay capacity exhausted; do not resend unknown writes")
            if type(expires) not in (int, float) or expires != expires or expires <= time.time() or expires > time.time() + 60:
                raise ShmProtocolError("request deadline expired/invalid; not executed")
            validated = True
            if method == "ping":
                result = dict(pong=True, transport="shm_rpc", account_id=self.account_id, environment=self.environment,
                              allow_order_methods=self.allow_order_methods, session_id=self.mailbox.session_id,
                              native_pid=self.mailbox.server_pid, capabilities=sorted(READ_METHODS | (ALL_WRITE_METHODS if self.allow_order_methods else frozenset())))
                result.update(self.runtime_metadata)
                if callable(self.read_rotation_evidence_reader):
                    try:
                        result["read_rotation_evidence"] = self.read_rotation_evidence_reader()
                    except Exception:
                        result["read_rotation_evidence"] = None
                if "read_rotation" in params:
                    if set(params) != {"read_rotation"} or not callable(self.read_rotation_control):
                        raise ShmProtocolError("read rotation control is unavailable")
                    result["read_rotation_control"] = self.read_rotation_control(params["read_rotation"])
                if "read_rotation_control_status" in params:
                    if set(params) != {"read_rotation_control_status"} or not callable(self.read_rotation_control_status_reader):
                        raise ShmProtocolError("read rotation control status is unavailable")
                    result["read_rotation_control_status"] = self.read_rotation_control_status_reader(params["read_rotation_control_status"])
                if "read_rotation_capability" in params:
                    if set(params) != {"read_rotation_capability"} or not callable(self.read_rotation_capability_control):
                        raise ShmProtocolError("read rotation capability is unavailable")
                    result["read_rotation_capability"] = self.read_rotation_capability_control(params["read_rotation_capability"])
                if "read_rotation_capability_status" in params:
                    if set(params) != {"read_rotation_capability_status"} or not callable(self.read_rotation_capability_status_reader):
                        raise ShmProtocolError("read rotation capability status is unavailable")
                    result["read_rotation_capability_status"] = self.read_rotation_capability_status_reader(params["read_rotation_capability_status"])
                if "supervisor_attach" in params:
                    if set(params) != {"supervisor_attach"} or not callable(self.supervisor_attach_control):
                        raise ShmProtocolError("supervisor attachment is unavailable")
                    result["supervisor_attach"] = self.supervisor_attach_control(params["supervisor_attach"])
                    result.update(self.runtime_metadata)
                if "supervisor_attach_status" in params:
                    if set(params) != {"supervisor_attach_status"} or not callable(self.supervisor_attach_status_reader):
                        raise ShmProtocolError("supervisor attachment status is unavailable")
                    result["supervisor_attach_status"] = self.supervisor_attach_status_reader(params["supervisor_attach_status"])
                result.update(async_submit_contract=ASYNC_SUBMIT_CONTRACT,
                              writes_blocked=bool(self._async_write_block_reason),
                              write_block_reason=self._async_write_block_reason,
                              async_pending_count=len(self._async_pending),
                              async_unknown_count=sum(row["state"] == "UNKNOWN" for row in self._async_pending.values()))
            elif method == "poll_execution_events":
                result = self._poll_events(params)
            elif method == "get_trade_login_state":
                diagnostic = getattr(self, "trade_login_diagnostic_reader", None)
                if self.execution_event_guard_required and (self._off_thread_event_loss.is_set() or self._lost_sequence):
                    result = self._trade_login()
                else:
                    result = diagnostic() if callable(diagnostic) else self._trade_login()
            else:
                if method in ALL_WRITE_METHODS:
                    self._check_execution_event_guard(params.get("execution_event_guard"))
                    if (self.execution_event_guard_required and
                            (self.runtime_metadata.get("native_write_runtime_guard_ready") is not True
                             or self.runtime_metadata.get("native_write_runtime_guard_attached") is not True
                             or self.runtime_metadata.get("native_callback_threading_verified") is not True
                             or self.runtime_metadata.get("supervisor_write_gate_contract") != "INHERITED_SUPERVISOR_GATE_V1"
                             or self.runtime_metadata.get("supervisor_gate_epoch_contract") != "SUPERVISOR_GATE_EPOCH_V1"
                             or not callable(self.native_write_runtime_guard))):
                        response.update(ok=True, data=self._runtime_guard_blocked(request))
                        self._finish(sequence, request, response, fingerprint)
                        return self.poll_once()
                    if self._async_write_block_reason:
                        data = (self._async_data(request, "UNKNOWN", error=self._async_write_block_reason)
                                if method in ASYNC_WRITE_METHODS else
                                dict(status="UNKNOWN", automatic_retry=False, native_write_executed=False,
                                     order_sys_id="", result=None, writes_blocked=True,
                                     write_block_reason=self._async_write_block_reason, error=self._async_write_block_reason))
                        response.update(ok=True, data=data)
                        self._finish(sequence, request, response, fingerprint)
                        return self.poll_once()
                    login = self._trade_login()
                    if login.get("status") != "LOGGED_IN":
                        error = login.get("reason") or "TRADE_LOGIN_NOT_CONFIRMED"
                        data = (self._async_data(request, "BLOCKED", error=error) if method in ASYNC_WRITE_METHODS else
                                dict(status="BLOCKED", automatic_retry=False, native_write_executed=False,
                                     order_sys_id="", result=None, error=error))
                        data["trade_login_state"] = login
                        response.update(ok=True, data=data)
                        self._finish(sequence, request, response, fingerprint)
                        return self.poll_once()
                    if self._active_write_event_guard is not None:
                        raise ExecutionEventGuardRejected("EXECUTION_EVENT_CURSOR_NOT_CURRENT")
                    self._check_execution_event_guard(params.get("execution_event_guard"))
                if method in ASYNC_WRITE_METHODS:
                    self._active_write_event_guard = params.get("execution_event_guard")
                    self._active_supervisor_gate_epoch = params.get("supervisor_gate_epoch")
                    try:
                        result = self._submit_async(request, fingerprint)
                    finally:
                        self._active_write_event_guard = None
                        self._active_supervisor_gate_epoch = None
                else:
                    business_params = dict(params)
                    business_params.pop("execution_event_guard", None)
                    business_params.pop("supervisor_gate_epoch", None)
                    business_params["account_id"] = self.account_id
                    if method == "submit_order":
                        business_params.setdefault("signal_id", rid)
                        business_params.setdefault("remark", "shm:%s" % rid)
                        validate = getattr(self.handlers.order_gateway, "validate_submit_params", None)
                        if callable(validate):
                            validate(business_params)
                    if method in READ_METHODS:
                        result = self._read_with_evidence(method, business_params)
                    else:
                        self._active_write_event_guard = params.get("execution_event_guard")
                        self._active_supervisor_gate_epoch = params.get("supervisor_gate_epoch")
                        try:
                            result = self.handlers.handle(method, business_params)
                        finally:
                            self._active_write_event_guard = None
                            self._active_supervisor_gate_epoch = None
                    if method in WRITE_METHODS:
                        settlement = self.handlers.take_pending_settlement()
                        if (method == "cancel_order" and getattr(result, "confirmation_pending", False) is True
                                and callable(getattr(self.handlers.order_gateway, "query_cancel_confirmation", None))):
                            settlement = dict(result=result, error="", deadline=time.monotonic() + self.handlers.order_settle_timeout_seconds)
                        response.update(ok=True, data=self._write_result(method, result))
                        if settlement is not None:
                            self._pending = (sequence, request, response, fingerprint, settlement)
                            return self.poll_once()
                        result = response["data"]
            response.update(ok=True, data=self._attach_source_identity(to_jsonable(result)))
        except ExecutionEventGuardRejected:
            response.update(ok=True, data=self._execution_guard_blocked(request))
        except SupervisorGateEpochRejected as exc:
            response.update(ok=True, data=self._supervisor_epoch_blocked(request, exc.current_epoch))
        except Exception as exc:
            if validated and method in ASYNC_WRITE_METHODS:
                error = "ASYNC_BRIDGE_UNCONFIRMED:%s; outcome=UNKNOWN" % exc.__class__.__name__
                self._block_async_writes(error)
                response.update(ok=True, data=self._async_data(request, "UNKNOWN", native_write_executed=None, error=error))
            elif validated and method in WRITE_METHODS:
                response.update(ok=True, data=dict(status="UNKNOWN", automatic_retry=False, error=str(exc), result=None, order_sys_id=""))
            else:
                response.update(error=str(exc), server_error=type(exc).__name__)
        if method in ("get_positions", "poll_execution_events") and "native_finished_at_unix" not in response.get("read_timing", {}):
            # 业务处理及结果归一化完成，尚未编码发布；缓存不得刷新原生处理时刻。
            finished_monotonic = time.monotonic() if method == "poll_execution_events" else None
            response.setdefault("read_timing", {})["native_finished_at_unix"] = time.time()
            if method == "poll_execution_events":
                response["read_timing"].update(native_finished_at_monotonic=finished_monotonic,
                                               native_finished_at_monotonic_after=time.monotonic())
        if validated:
            self._finish(sequence, request, response, fingerprint)
        else:
            self._pending = (sequence, request, response, fingerprint, None)
        return self.poll_once()

    def close(self):
        self._callback_thread()
        self.mailbox.close()
