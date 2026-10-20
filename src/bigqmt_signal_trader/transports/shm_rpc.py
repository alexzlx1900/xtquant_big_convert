# coding: utf-8
"""独立显式交易邮箱客户端；未知写请求禁止自动重试。"""

import math
import threading
import uuid

from .readonly_shm import ShmBusyError, ShmProtocolError, ShmReadOnlyClient, validate_name


WRITE_METHODS = frozenset(("submit_order", "cancel_order"))
READ_METHODS = frozenset(("ping", "get_asset", "get_positions", "query_orders", "query_trades", "query_execution_snapshot", "poll_execution_events", "get_trade_login_state"))


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
        return result

    def call(self, method, params=None, *, account_id=None, timeout_seconds=None,
             use_formula=False, request_id=None):
        if method not in READ_METHODS | WRITE_METHODS or use_formula is not False:
            raise ShmProtocolError("unsupported SHM RPC method/routing")
        target = self.account_id if account_id is None else account_id
        values = dict(params or {})
        account = values.get("account", self.account_id)
        if isinstance(account, dict):
            account = account.get("account_id", account.get("accountID", account.get("id")))
        if target != self.account_id or values.get("account_id", self.account_id) != self.account_id or account != self.account_id:
            raise ShmProtocolError("fixed account mismatch")
        if method in WRITE_METHODS and (not self.allow_order_methods or self._identity is None):
            raise ShmProtocolError("write disabled or identity not verified")
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
                if getattr(self._client, "poisoned", False) or (method in WRITE_METHODS and not isinstance(exc, (ShmBusyError, ValueError))):
                    self._uncertain = True
                    return dict(status="UNKNOWN", automatic_retry=False, request_id=rid, error=str(exc))
                raise
            if response.get("environment", None if self.environment == "LIVE" else "SIM") != self.environment:
                self._uncertain = True
                raise ShmProtocolError("response environment mismatch; outcome is unconfirmed")
            if not response["ok"]:
                raise ShmRpcError(response)
            return response["data"]
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
