# coding: utf-8
"""将独立只读邮箱适配为 Gateway 的同步 RPC 客户端。"""

import datetime as _dt
import math
import threading
import uuid

from .readonly_shm import ShmBusyError, ShmProtocolError, ShmReadOnlyClient, validate_name


MARKET_RPC_METHODS = frozenset(("get_local_data", "get_market_data_ex", "download_history_data", "subscribe_quote", "poll_quote_events", "unsubscribe_quote"))


READONLY_RPC_METHODS = frozenset((
    "ping", "get_positions", "get_asset", "query_orders", "query_trades",
    "get_full_tick", "get_ticks",
)) | MARKET_RPC_METHODS


class ShmReadOnlyRpcError(RuntimeError):
    """保留失败响应和部分来源证据，不将失败转换为成功空集。"""

    def __init__(self, response):
        self.response = dict(response)
        self.server_error = response.get("server_error")
        self.data = response.get("data")
        self.source_evidence = (
            self.data.get("source_evidence") if isinstance(self.data, dict) else None
        )
        super(ShmReadOnlyRpcError, self).__init__(
            "%s: %s" % (self.server_error, response.get("error"))
        )


class ShmReadOnlyRpcClient:
    """固定账户、单个在途请求；拒绝方法在打开邮箱之前校验。"""

    transport_name = "shm_readonly"

    def __init__(self, *, name, account_id, timeout_seconds=5.0, client_factory=None):
        validate_name(name)
        if not isinstance(account_id, str) or not account_id or len(account_id) > 64:
            raise ValueError("a fixed account_id is required")
        self._validate_timeout(timeout_seconds)
        self.name = name
        self.account_id = account_id
        self.timeout_seconds = timeout_seconds
        self._client_factory = client_factory or ShmReadOnlyClient
        self._client = None
        self._lock = threading.Lock()
        self._closed = False
        self._verified_identity = None
        self._capabilities = frozenset()

    @staticmethod
    def _validate_timeout(value):
        if (isinstance(value, bool) or not isinstance(value, (int, float))
                or not math.isfinite(value) or not 0 < value <= 60):
            raise ValueError("timeout must be finite and within (0, 60] seconds")

    @staticmethod
    def _quote_params(values, *, max_symbols=64):
        if type(max_symbols) is not int or max_symbols not in (64, 128):
            raise ShmProtocolError("explicit quote limit must be 64 or 128")
        if set(values) - {"account_id", "codes", "code"} or ("codes" in values) == ("code" in values):
            raise ShmProtocolError("provide exactly one quote selector")
        codes = values.get("codes", values.get("code"))
        if isinstance(codes, str):
            codes = [codes]
        if (not isinstance(codes, list) or not 1 <= len(codes) <= max_symbols
                or any(not isinstance(code, str) or len(code) != 9
                       or not all("0" <= char <= "9" for char in code[:6])
                       or code[6:] not in (".SH", ".SZ", ".BJ") for code in codes)
                or len(set(codes)) != len(codes)):
            raise ShmProtocolError("codes must contain 1-%s unique SH/SZ/BJ symbols" % max_symbols)
        result = dict(values)
        result.pop("code", None)
        result["codes"] = list(codes)
        return result

    @staticmethod
    def _market_params(method, values):
        allowed = {
            "get_local_data": {"account_id", "code", "period", "start_time", "end_time", "count", "divid_type"},
            "get_market_data_ex": {"account_id", "code", "period", "start_time", "end_time", "count", "dividend_type", "fill_data", "subscribe"},
            "download_history_data": {"account_id", "code", "period", "start_time", "end_time", "incrementally"},
            "subscribe_quote": {"account_id", "code", "period", "dividend_type", "result_type"},
            "poll_quote_events": {"account_id", "after_sequence"},
            "unsubscribe_quote": {"account_id", "subscription_id"},
        }[method]
        if set(values) - allowed:
            raise ShmProtocolError("unsupported market parameter")
        if method in ("get_local_data", "get_market_data_ex", "download_history_data", "subscribe_quote"):
            ShmReadOnlyRpcClient._quote_params({"code": values.get("code")})
        if method in ("get_local_data", "get_market_data_ex"):
            start, end, count = values.get("start_time"), values.get("end_time"), values.get("count")
            if (values.get("period") not in ("1m", "5m", "15m", "30m", "60m", "1d", "tick")
                    or values.get("divid_type", "none") != "none" or type(count) is not int or not 1 <= count <= 10000
                    or any(not isinstance(v, str) or v and (len(v) != 14 or not v.isdigit()) for v in (start, end))
                    or start and end and start > end):
                raise ShmProtocolError("history requires bounded period, times and count")
            for value in (start, end):
                if value:
                    try:
                        _dt.datetime.strptime(value, "%Y%m%d%H%M%S")
                    except ValueError:
                        raise ShmProtocolError("history timestamp is invalid")
            if method == "get_market_data_ex" and (
                    values.get("dividend_type", "none") != "none"
                    or type(values.get("fill_data")) is not bool or values.get("subscribe") is not False):
                raise ShmProtocolError("native cached history requires explicit fill_data and subscribe=False")
        elif method == "download_history_data":
            start, end = values.get("start_time"), values.get("end_time")
            if (values.get("period") not in ("tick", "1m", "5m", "1d")
                    or type(values.get("incrementally")) not in (bool, type(None))
                    or any(not isinstance(v, str) or len(v) != 8 or not v.isdigit() for v in (start, end))
                    or start != end):
                raise ShmProtocolError("download requires one explicit trading day and supported period")
            try:
                _dt.datetime.strptime(start, "%Y%m%d")
            except ValueError:
                raise ShmProtocolError("download trading day is invalid")
        elif method == "subscribe_quote":
            if (values.get("period", "tick") != "tick" or values.get("dividend_type", "none") != "none"
                    or values.get("result_type", "dict") != "dict"):
                raise ShmProtocolError("subscription requires tick/dict/none")
        elif method == "poll_quote_events":
            after = values.get("after_sequence")
            if type(after) is not int or after < 0:
                raise ShmProtocolError("after_sequence must be nonnegative integer")
        else:
            identity = values.get("subscription_id")
            if not isinstance(identity, str) or not identity or len(identity) > 128:
                raise ShmProtocolError("explicit bounded subscription_id required")
        return dict(values)

    def verify_identity(self, *, expected_session_id, expected_native_pid):
        """新客户端显式握手；核对失败关闭，未知请求不重发。"""
        if (not isinstance(expected_session_id, str) or not expected_session_id
                or type(expected_native_pid) is not int or expected_native_pid <= 0):
            raise ShmProtocolError("expected session and native PID are required")
        result = self.call("ping")
        if (not isinstance(result, dict) or result.get("pong") is not True
                or result.get("account_id") != self.account_id or result.get("allow_order_methods") is not False
                or result.get("session_id") != expected_session_id or result.get("native_pid") != expected_native_pid):
            self.close()
            raise ShmProtocolError("explicit bridge identity verification failed")
        capabilities = result.get("capabilities", [])
        if not isinstance(capabilities, list) or any(not isinstance(item, str) for item in capabilities):
            self.close()
            raise ShmProtocolError("bridge capabilities are malformed")
        self._verified_identity = (expected_session_id, expected_native_pid)
        self._capabilities = frozenset(capabilities)
        return result

    def call(self, method, params=None, *, account_id=None, timeout_seconds=None,
             use_formula=False):
        if method not in READONLY_RPC_METHODS:
            raise ShmProtocolError("method is outside the readonly allowlist: %s" % method)
        if use_formula is not False:
            raise ShmProtocolError("formula routing is unavailable for the readonly mailbox")
        target = self.account_id if account_id is None else account_id
        if target != self.account_id:
            raise ShmProtocolError("request account_id mismatch")
        values = {} if params is None else params
        if not isinstance(values, dict):
            raise ValueError("params must be an object")
        if values.get("account_id", self.account_id) != self.account_id:
            raise ShmProtocolError("params account_id mismatch")
        if method in ("get_full_tick", "get_ticks"):
            values = self._quote_params(values)
        if method in MARKET_RPC_METHODS:
            values = self._market_params(method, values)
            if self._verified_identity is None or method not in self._capabilities:
                raise ShmProtocolError("explicit identity handshake and advertised capability required")
        timeout = self.timeout_seconds if timeout_seconds is None else timeout_seconds
        self._validate_timeout(timeout)
        if not self._lock.acquire(False):
            raise ShmBusyError("readonly client already has an in-flight request")
        try:
            if self._closed:
                raise ShmProtocolError("readonly client is closed")
            if self._client is None:
                self._client = self._client_factory(self.name, self.account_id)
            if getattr(self._client, "poisoned", False):
                raise ShmProtocolError("poisoned unconfirmed client requires a new instance and explicit identity handshake")
            if self._verified_identity is not None:
                mailbox = self._client.mailbox
                if (mailbox.session_id, mailbox.server_pid) != self._verified_identity:
                    raise ShmProtocolError("verified bridge identity changed; create a new client")
            request = {
                "schema_version": 1, "request_id": uuid.uuid4().hex,
                "account_id": self.account_id, "method": method, "params": dict(values),
            }
            response = self._client.send_request(request, timeout_seconds=timeout)
            if not response["ok"]:
                raise ShmReadOnlyRpcError(response)
            return response["data"]
        finally:
            self._lock.release()

    def close(self):
        if not self._lock.acquire(False):
            raise ShmBusyError("cannot close an in-flight readonly request")
        try:
            if self._closed:
                return
            self._closed = True
            if self._client is not None:
                self._client.close()
        finally:
            self._lock.release()
