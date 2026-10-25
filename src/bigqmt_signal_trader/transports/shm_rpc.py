# coding: utf-8
"""独立显式交易邮箱客户端；未知写请求禁止自动重试。"""

import math
import threading
import uuid

from .readonly_shm import ShmBusyError, ShmProtocolError, ShmReadOnlyClient, validate_name


WRITE_METHODS = frozenset(("submit_order", "cancel_order"))
ASYNC_WRITE_METHODS = frozenset(("submit_order_async",))
ALL_WRITE_METHODS = WRITE_METHODS | ASYNC_WRITE_METHODS
ASYNC_SUBMIT_CONTRACT = "NATIVE_CALL_RETURNED_V1"
READ_METHODS = frozenset(("ping", "get_asset", "get_positions", "query_orders", "query_trades", "query_execution_snapshot", "poll_execution_events", "get_trade_login_state"))


def validate_async_submit_params(params):
    client_order_id = params.get("client_order_id")
    if (not isinstance(client_order_id, str) or not 1 <= len(client_order_id) <= 128
            or any(ord(char) < 33 or ord(char) > 126 for char in client_order_id)):
        raise ShmProtocolError("bounded explicit ASCII client_order_id required")
    if any(field in params and params[field] != client_order_id for field in ("remark", "order_remark")):
        raise ShmProtocolError("async native remark must equal client_order_id")
    return client_order_id


class ShmRpcError(RuntimeError):
    def __init__(self, response):
        self.response = response
        super().__init__("%s: %s" % (response.get("server_error"), response.get("error")))


class ShmRpcClient:
    transport_name = "shm_rpc"

    def __init__(self, *, name, account_id, allow_order_methods=False,
                 timeout_seconds=5.0, client_factory=None, environment="SIM",
                 persistent_live_read_write=False):
        validate_name(name)
        if not isinstance(account_id, str) or not account_id or len(account_id) > 64:
            raise ValueError("fixed account required")
        if type(allow_order_methods) is not bool:
            raise ValueError("allow_order_methods must be boolean")
        if environment not in ("SIM", "LIVE") or type(persistent_live_read_write) is not bool:
            raise ValueError("explicit SIM/LIVE environment and boolean LIVE policy required")
        if environment == "LIVE":
            if (not persistent_live_read_write or not allow_order_methods or account_id == "55013746"
                    or not all("0" <= char <= "9" for char in account_id)):
                raise ValueError("LIVE client requires explicit persistent LIVE policy and live numeric account")
        elif persistent_live_read_write:
            raise ValueError("persistent LIVE policy requires LIVE environment")
        self._timeout(timeout_seconds)
        self.name, self.account_id = name, account_id
        self.environment = environment
        self.allow_order_methods, self.timeout_seconds = allow_order_methods, timeout_seconds
        self._factory = client_factory or ShmReadOnlyClient
        self._client, self._identity = None, None
        self._async_submit_enabled = False
        self._async_windows_session_id = None
        self._async_write_uncertain = False
        self._closed, self._uncertain = False, False
        self._lock = threading.Lock()

    @staticmethod
    def _timeout(value):
        if type(value) not in (int, float) or not math.isfinite(value) or not 0 < value <= 60:
            raise ValueError("timeout must be within (0,60]")

    def verify_identity(self, *, expected_session_id, expected_native_pid):
        if not isinstance(expected_session_id, str) or not expected_session_id or type(expected_native_pid) is not int or expected_native_pid <= 0:
            raise ShmProtocolError("explicit session/PID required")
        result = self.call("ping")
        if (not isinstance(result, dict) or result.get("account_id") != self.account_id
                or result.get("session_id") != expected_session_id
                or result.get("native_pid") != expected_native_pid
                or result.get("transport") != "shm_rpc"
                or result.get("environment", None if self.environment == "LIVE" else "SIM") != self.environment
                or self.environment == "LIVE" and result.get("runtime_policy") != "PERSISTENT_LIVE_READ_WRITE"
                or result.get("allow_order_methods") is not self.allow_order_methods):
            self.close()
            raise ShmProtocolError("bridge identity mismatch")
        self._identity = (expected_session_id, expected_native_pid)
        capabilities = result.get("capabilities")
        self._async_submit_enabled = (
            isinstance(capabilities, list) and all(isinstance(value, str) for value in capabilities)
            and "submit_order_async" in capabilities
            and result.get("async_submit_contract") == ASYNC_SUBMIT_CONTRACT
            and type(result.get("windows_session_id")) is int and result["windows_session_id"] >= 0)
        self._async_windows_session_id = result.get("windows_session_id") if self._async_submit_enabled else None
        if self._async_submit_enabled and result.get("writes_blocked") is True:
            self._async_write_uncertain = True
        return result

    def call(self, method, params=None, *, account_id=None, timeout_seconds=None,
             use_formula=False, request_id=None):
        if method not in READ_METHODS | ALL_WRITE_METHODS or use_formula is not False:
            raise ShmProtocolError("unsupported SHM RPC method/routing")
        target = self.account_id if account_id is None else account_id
        values = dict(params or {})
        account = values.get("account", self.account_id)
        if isinstance(account, dict):
            account = account.get("account_id", account.get("accountID", account.get("id")))
        if target != self.account_id or values.get("account_id", self.account_id) != self.account_id or account != self.account_id:
            raise ShmProtocolError("fixed account mismatch")
        if method in ALL_WRITE_METHODS and (not self.allow_order_methods or self._identity is None):
            raise ShmProtocolError("write disabled or identity not verified")
        if method in ALL_WRITE_METHODS and self._async_write_uncertain:
            raise ShmProtocolError("async write outcome unconfirmed; no further writes")
        if method in ASYNC_WRITE_METHODS:
            validate_async_submit_params(values)
            if not self._async_submit_enabled:
                raise ShmProtocolError("verified native async submission contract required")
        if method == "poll_execution_events" and (type(values.get("after_sequence")) is not int or values["after_sequence"] < 0):
            raise ShmProtocolError("bounded nonnegative event cursor required")
        timeout = self.timeout_seconds if timeout_seconds is None else timeout_seconds
        self._timeout(timeout)
        if not self._lock.acquire(False):
            raise ShmBusyError("single in-flight request only")
        rid = request_id or uuid.uuid4().hex
        try:
            if self._closed or self._uncertain:
                raise ShmProtocolError("closed/unconfirmed client cannot be reused")
            if self._client is None:
                self._client = self._factory(self.name, self.account_id)
            if self._identity is not None and (self._client.mailbox.session_id, self._client.mailbox.server_pid) != self._identity:
                raise ShmProtocolError("bridge identity changed")
            request = dict(schema_version=1, request_id=rid, account_id=self.account_id, method=method, params=values)
            if self.environment == "LIVE":
                request["environment"] = "LIVE"
            try:
                response = self._client.send_request(request, timeout_seconds=timeout)
            except Exception as exc:
                if getattr(self._client, "poisoned", False) or (method in ALL_WRITE_METHODS and not isinstance(exc, (ShmBusyError, ValueError))):
                    self._uncertain = True
                    return dict(status="UNKNOWN", automatic_retry=False, request_id=rid, error=str(exc))
                raise
            if response.get("environment", None if self.environment == "LIVE" else "SIM") != self.environment:
                self._uncertain = True
                raise ShmProtocolError("response environment mismatch; outcome is unconfirmed")
            if not response["ok"]:
                raise ShmRpcError(response)
            data = response["data"]
            if method in ASYNC_WRITE_METHODS:
                valid = (isinstance(data, dict) and data.get("async_submit_contract") == ASYNC_SUBMIT_CONTRACT
                        and data.get("request_id") == rid and data.get("client_order_id") == values["client_order_id"]
                        and data.get("account_id") == self.account_id and data.get("environment") == self.environment
                        and data.get("session_id") == self._identity[0]
                        and type(data.get("native_pid")) is int and data["native_pid"] == self._identity[1]
                        and type(data.get("windows_session_id")) is int
                        and data["windows_session_id"] == self._async_windows_session_id)
                valid = (valid and data.get("broker_accepted") is False and "qmt_async_seq" in data
                         and data["qmt_async_seq"] is None and data.get("order_sys_id") == ""
                         and data.get("automatic_retry") is False and type(data.get("writes_blocked")) is bool
                         and isinstance(data.get("write_block_reason"), str))
                if valid:
                    status = data.get("status")
                    if status == "PENDING_CONFIRMATION":
                        valid = (data.get("receipt_type") == "NATIVE_CALL_RETURNED"
                                 and data.get("native_write_executed") is True
                                 and bool(data["write_block_reason"]) is data["writes_blocked"])
                    elif status == "BLOCKED":
                        valid = (data.get("native_write_executed") is False and data["writes_blocked"] is False
                                 and data["write_block_reason"] == "" and data.get("receipt_type") is None)
                    elif status == "UNKNOWN":
                        valid = data["writes_blocked"] is True and bool(data["write_block_reason"])
                    else:
                        valid = False
                if not valid:
                    self._uncertain = True
                    return dict(status="UNKNOWN", automatic_retry=False, request_id=rid,
                                error="NATIVE_ASYNC_RECEIPT_UNCONFIRMED")
                if data.get("status") == "UNKNOWN" or data["writes_blocked"]:
                    self._async_write_uncertain = True
            return data
        finally:
            self._lock.release()

    def poll_execution_events(self, after_sequence=0):
        return self.call("poll_execution_events", {"after_sequence": after_sequence})

    def get_trade_login_state(self):
        return self.call("get_trade_login_state")

    def close(self):
        if not self._lock.acquire(False):
            raise ShmBusyError("cannot close in-flight request")
        try:
            if not self._closed:
                self._closed = True
                if self._client is not None:
                    self._client.close()
        finally:
            self._lock.release()
