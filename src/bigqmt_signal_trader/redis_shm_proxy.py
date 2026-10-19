# coding: utf-8
"""QMT 外的显式模拟 Redis → 双共享内存兼容代理。

注入的两个 SHM 客户端由代理接管并关闭；Redis 连接池仍归调用方。
本模块不创建后台线程，不连接 QMT Socket，也不重开或重发未知请求。
"""

import copy
import datetime
import json
import math
import threading
import time
import uuid

from .adapters.redis_common import decode_text
from .redis_rpc import decode_rpc_request_payload
from .transports.readonly_shm import ShmProtocolError
from .transports.readonly_shm_client import MARKET_RPC_METHODS, ShmReadOnlyRpcClient
from .transports.redis_transport import _is_redis_timeout
from .transports.shm_rpc import WRITE_METHODS


EXECUTION_METHODS = frozenset((
    "ping", "get_asset", "get_positions", "query_orders", "query_trades",
    "query_execution_snapshot", "get_trade_login_state", "poll_execution_events",
))
QUOTE_METHODS = frozenset(("get_ticks", "get_full_tick"))
RUNTIME_POLICIES = frozenset(("BOUNDED", "PERSISTENT_READONLY", "PERSISTENT_SIMULATION_READ_WRITE"))
_WRITE_LEDGER_KEY = "aqp:redis_shm:write_ledger:55013746:v1"
# 柜台订单秒精度时钟与代理本机时钟不同；1258真实样本偏差为-3.049735秒。
_BROKER_PROXY_CLOCK_TOLERANCE_SECONDS = 5.0
_CLAIM_WRITE = """
local prior = redis.call('HGET', KEYS[1], ARGV[1])
if prior then return {0, prior} end
if redis.call('HLEN', KEYS[1]) >= tonumber(ARGV[2]) then return {-1, ''} end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[3])
return {1, ARGV[3]}
"""
_SETTLE_WRITE = """
if redis.call('HGET', KEYS[1], ARGV[1]) ~= ARGV[2] then return 0 end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[3])
return 1
"""
_REPLY_TEMPLATES = {
    "reply_key": "bigqmt:rpc:resp:{account_id}:{request_id}",
    "reply_list": "bigqmt:rpc:respq:{account_id}:{request_id}",
    "reply_channel": "bigqmt:rpc:resp:{account_id}:{request_id}",
}


class RedisShmReadOnlyProxy:
    """兼容原只读类名；写入须显式模拟政策且持久留痕，不自动恢复或重发。"""

    def __init__(self, redis_transport, readonly_client, execution_client, *,
                 account_id="55013746", expected_readonly_session_id,
                 expected_execution_session_id, expected_native_pid,
                 timeout_seconds=5.0, max_requests=1000, deadline_unix=None,
                 runtime_policy="BOUNDED", allow_order_methods=False,
                 read_cache_capacity=1000, write_ledger_capacity=1000,
                 expected_windows_session_id=None):
        if (account_id != "55013746"
                or redis_transport.account_id != account_id):
            raise ValueError("this candidate only accepts simulation account 55013746")
        if redis_transport.listen_redis is not redis_transport.redis:
            # 原 transport 对两池中的单方写失败内部容错，本候选要求任一
            # fanout 阶段失败可见，避免未确认的部分发布被包装成成功。
            raise ValueError("one injected Redis client must own receive and response fanout")
        if (redis_transport.request_queue != "bigqmt:rpc:queue:55013746"
                or redis_transport.request_channel != "bigqmt:rpc:req:55013746"):
            raise ValueError("candidate requires the standard simulation request queue and channel")
        if (not isinstance(expected_readonly_session_id, str) or not expected_readonly_session_id
                or not isinstance(expected_execution_session_id, str) or not expected_execution_session_id
                or expected_readonly_session_id == expected_execution_session_id
                or type(expected_native_pid) is not int or expected_native_pid <= 0):
            raise ValueError("two distinct fixed sessions and a native PID are required")
        if type(timeout_seconds) not in (int, float) or not math.isfinite(timeout_seconds) or not 0 < timeout_seconds <= 60:
            raise ValueError("timeout must be within (0, 60] seconds")
        if runtime_policy not in RUNTIME_POLICIES:
            raise ValueError("explicit supported runtime policy required")
        if type(allow_order_methods) is not bool or allow_order_methods is not (runtime_policy == "PERSISTENT_SIMULATION_READ_WRITE"):
            raise ValueError("write permission must explicitly match the simulation read/write policy")
        if runtime_policy == "BOUNDED":
            if type(max_requests) is not int or not 1 <= max_requests <= 1000:
                raise ValueError("max_requests must be within [1, 1000]")
        elif type(max_requests) is not int or max_requests != 0 or deadline_unix is not None:
            raise ValueError("persistent policies require null deadline and max_requests=0")
        for capacity in (read_cache_capacity, write_ledger_capacity):
            if type(capacity) is not int or not 1 <= capacity <= 1000:
                raise ValueError("finite read/write record capacity within [1, 1000] required")
        if deadline_unix is not None and (
                type(deadline_unix) not in (int, float) or not math.isfinite(deadline_unix) or deadline_unix <= 0):
            raise ValueError("absolute deadline must be a finite positive timestamp")
        if readonly_client is execution_client or any(
                client.account_id != account_id for client in (readonly_client, execution_client)):
            raise ValueError("distinct SHM clients with the fixed account are required")
        self.transport = redis_transport
        self.account_id = account_id
        self.timeout_seconds = float(timeout_seconds)
        self.max_requests = max_requests
        self.deadline_unix = deadline_unix
        self.runtime_policy = runtime_policy
        self.allow_order_methods = allow_order_methods
        self.read_cache_capacity = read_cache_capacity
        self.write_ledger_capacity = write_ledger_capacity
        if expected_windows_session_id is not None and (
                type(expected_windows_session_id) is not int or expected_windows_session_id < 0):
            raise ValueError("Windows session must be an explicit nonnegative integer")
        self.expected_windows_session_id = expected_windows_session_id
        self._clients = {"readonly": readonly_client, "execution": execution_client}
        self._sessions = {"readonly": expected_readonly_session_id, "execution": expected_execution_session_id}
        self.expected_native_pid = expected_native_pid
        self._verified = False
        self._closed = False
        self._resources_closed = False
        self.stop_reason = ""
        self._lock = threading.Lock()
        self._poll_lock = threading.Lock()
        self._pubsub = None
        self._replays = {}
        self._read_expiries = {}
        self._processed_count = 0
        self._source_identity = {}
        self._capabilities = frozenset()
        self._writes_blocked = False
        self._write_block_reason = ""
        self._operator_write_blocked = False
        self._write_record_count = 0
        self._operation_phase = "INITIALIZED"
        self._request_context = {}
        self._last_error = {}
        self._idle_receive_timeouts = 0
        self._consecutive_idle_timeouts = 0

    def _record_error(self, exc):
        self._last_error = dict(error_phase=self._operation_phase,
            error_type=type(exc).__name__, error_message=str(exc)[:2048],
            error_at_unix=time.time(), **self._request_context)

    def diagnostic_snapshot(self):
        """有界运行证据，不包含请求参数、Redis凭据或任何重发入口。"""
        return dict(operation_phase=self._operation_phase, last_error=copy.deepcopy(self._last_error),
                    idle_receive_timeouts=self._idle_receive_timeouts,
                    consecutive_idle_timeouts=self._consecutive_idle_timeouts)

    @property
    def closed(self):
        return self._closed

    @property
    def stopped(self):
        return self._closed

    @property
    def verified(self):
        return self._verified

    @property
    def processed_count(self):
        return self._processed_count

    def _reclaim_reads(self):
        if self.runtime_policy == "BOUNDED":
            return
        now = time.time()
        for rid, expires in list(self._read_expiries.items()):
            if expires <= now:
                self._read_expiries.pop(rid, None)
                self._replays.pop(rid, None)

    def identity_snapshot(self):
        """仅返回实际双源握手证据，未握手不能声明 READY。"""
        if not self._verified:
            raise ShmProtocolError("explicit identity handshake required")
        result = dict(pong=True, transport="redis_shm", rpc_revision="external-redis-dual-shm-v1",
                    account_id=self.account_id, session_id=self._sessions["execution"],
                    native_pid=self.expected_native_pid, allow_order_methods=self.allow_order_methods,
                    runtime_policy=self.runtime_policy, capabilities=sorted(self._capabilities),
                    writes_blocked=self._writes_blocked, write_block_reason=self._write_block_reason,
                    write_ready=self.allow_order_methods and not self._writes_blocked,
                    read_source=copy.deepcopy(self._source_identity["readonly"]),
                    execution_source=copy.deepcopy(self._source_identity["execution"]))
        if "windows_session_id" in self._source_identity["execution"]:
            result["windows_session_id"] = self._source_identity["execution"]["windows_session_id"]
        binding = self._source_binding()
        if binding is not None:
            result["source_binding"] = binding
        return result

    def _source_binding(self):
        """来源身份取自两路实际握手；旧只读握手缺会话字段时不补造。"""
        read = self._source_identity.get("readonly", {})
        execution = self._source_identity.get("execution", {})
        windows_session = execution.get("windows_session_id")
        if (type(windows_session) is not int or windows_session < 0
                or type(read.get("windows_session_id")) is not int
                or read["windows_session_id"] != windows_session):
            return None
        return dict(read_session_id=self._sessions["readonly"],
                    execution_session_id=self._sessions["execution"],
                    native_pid=self.expected_native_pid, windows_session_id=windows_session)

    def _validate_source_binding(self, request):
        required = request.get("method") in WRITE_METHODS
        if not required and "expected_source_binding" not in request:
            return
        binding = request.get("expected_source_binding")
        actual = self._source_binding()
        if (not isinstance(binding, dict) or actual is None or set(binding) != set(actual)
                or any(not isinstance(binding.get(field), str) or not binding[field]
                       for field in ("read_session_id", "execution_session_id"))
                or type(binding.get("native_pid")) is not int or binding["native_pid"] <= 0
                or type(binding.get("windows_session_id")) is not int or binding["windows_session_id"] < 0
                or binding != actual):
            raise ShmProtocolError("request expected_source_binding must exactly match the verified native sources")

    def _block_writes(self, reason):
        if reason == "OPERATOR_WRITE_DISABLED":
            self._operator_write_blocked = True
        self._writes_blocked = True
        self._write_block_reason = self._write_block_reason or reason

    def _inspect_write_ledger(self):
        if not self.allow_order_methods:
            return
        records = self.transport.redis.hgetall(_WRITE_LEDGER_KEY)
        if not isinstance(records, dict) or len(records) > 1000:
            raise ShmProtocolError("persistent write ledger is malformed or over its fixed bound")
        self._write_record_count = len(records)
        if len(records) >= self.write_ledger_capacity:
            self._block_writes("WRITE_LEDGER_CAPACITY_EXHAUSTED")
        for rid, encoded in records.items():
            record = json.loads(decode_text(encoded))
            if (not isinstance(record, dict) or record.get("request_id") != decode_text(rid)
                    or not isinstance(record.get("fingerprint"), str)
                    or not isinstance(record.get("binding"), dict)
                    or record["binding"].get("account_id") != self.account_id
                    or record.get("state") not in ("CLAIMED", "CONFIRMED", "UNKNOWN", "PUBLISH_PENDING", "PUBLISH_UNCONFIRMED", "RECONCILED")):
                raise ShmProtocolError("persistent write ledger identity is malformed")
            if record["state"] == "RECONCILED":
                self._validate_reconciled_record(record)
            elif record["state"] != "CONFIRMED":
                self._block_writes("PERSISTED_WRITE_UNCONFIRMED")

    def _validate_identity_data(self, route, data):
        required = QUOTE_METHODS if route == "readonly" else EXECUTION_METHODS
        allowed_write = self.allow_order_methods if route == "execution" else False
        if (not isinstance(data, dict) or data.get("pong") is not True
                or data.get("account_id") != self.account_id
                or data.get("allow_order_methods") is not allowed_write
                or data.get("session_id") != self._sessions[route]
                or type(data.get("native_pid")) is not int or data["native_pid"] != self.expected_native_pid
                or not isinstance(data.get("capabilities"), list)
                or any(not isinstance(item, str) for item in data["capabilities"])
                or not required <= set(data["capabilities"])
                or (route == "execution" and data.get("transport") != "shm_rpc")
                or (allowed_write and not WRITE_METHODS <= set(data["capabilities"]))
                or (not allowed_write and bool(WRITE_METHODS & set(data["capabilities"])))):
            raise ShmProtocolError("explicit bridge identity/capabilities verification failed")
        if (self.expected_windows_session_id is not None
                and (type(data.get("windows_session_id")) is not int
                     or data["windows_session_id"] != self.expected_windows_session_id)):
            raise ShmProtocolError("native Windows session does not match explicit binding")
        if self.runtime_policy != "BOUNDED":
            expected_policy = self.runtime_policy if route == "execution" else "PERSISTENT_READONLY"
            if (data.get("runtime_policy") != expected_policy
                    or (route == "readonly" and data.get("transport") != "shm_readonly")
                    or data.get("source_deadline_unix", "missing") is not None
                    or type(data.get("max_seconds")) is not int or data["max_seconds"] != 0
                    or type(data.get("max_requests")) is not int or data["max_requests"] != 0):
                raise ShmProtocolError("native persistent policy does not match explicit runner policy")

    def _seal(self, reason):
        self._closed = True
        self.stop_reason = self.stop_reason or reason

    def _ensure_open(self):
        if self.deadline_unix is not None and time.time() >= self.deadline_unix:
            self._seal("ABSOLUTE_DEADLINE_EXPIRED")
        if self._closed:
            raise ShmProtocolError("proxy is stopped: %s; do not resend" % self.stop_reason)

    def _check_client(self, route):
        client = self._clients[route]
        mailbox = client.mailbox
        if (client.poisoned or mailbox.closed or
                (mailbox.session_id, mailbox.server_pid) != (self._sessions[route], self.expected_native_pid)):
            raise ShmProtocolError("SHM client closed, unconfirmed, or identity changed")
        return client

    @staticmethod
    def _check_response(request, response, session_id):
        if (not isinstance(response, dict) or type(response.get("schema_version")) is not int
                or response["schema_version"] != 1 or type(response.get("ok")) is not bool
                or type(response.get("sequence")) is not int or response["sequence"] <= 0
                or response.get("session_id") != session_id):
            raise ShmProtocolError("invalid SHM response envelope")
        for field in ("request_id", "account_id", "method"):
            if response.get(field) != request.get(field):
                raise ShmProtocolError("SHM response %s mismatch" % field)
        if ("data" not in response or any(not isinstance(response.get(field), str)
                                         for field in ("error", "server_error", "handled_at"))
                or (response["ok"] and (response["data"] is None or response["error"] or response["server_error"]))
                or (not response["ok"] and (not response["error"] or not response["server_error"]))):
            raise ShmProtocolError("SHM response result/error fields are inconsistent")

    def verify_identity(self):
        """两路显式握手全部成功后才允许接收 Redis 业务。"""
        if not self._lock.acquire(False):
            raise ShmProtocolError("proxy already has an in-flight operation")
        try:
            self._ensure_open()
            if self._verified:
                raise ShmProtocolError("identity verification is single-use")
            result = {}
            try:
                for route in ("readonly", "execution"):
                    self._operation_phase = "SHM_HANDSHAKE_" + route.upper()
                    self._ensure_open()
                    client = self._check_client(route)
                    now = time.time()
                    deadline = now + self.timeout_seconds
                    if self.deadline_unix is not None:
                        deadline = min(deadline, self.deadline_unix)
                    remaining = deadline - time.time()
                    if remaining <= 0:
                        self._seal("ABSOLUTE_DEADLINE_EXPIRED")
                        raise ShmProtocolError("absolute deadline expired before identity ping")
                    request = dict(schema_version=1, request_id=uuid.uuid4().hex,
                                   account_id=self.account_id, method="ping", params={},
                                   read_expires_at_unix=deadline)
                    response = client.send_request(request, timeout_seconds=remaining)
                    self._ensure_open()
                    self._check_response(request, response, self._sessions[route])
                    data = response["data"]
                    if response["ok"] is not True:
                        raise ShmProtocolError("native identity ping rejected")
                    self._validate_identity_data(route, data)
                    result[route] = copy.deepcopy(data)
                self._source_identity = result
                if self.allow_order_methods and self._source_binding() is None:
                    raise ShmProtocolError("write source requires two matching actual Windows sessions")
                self._capabilities = frozenset(EXECUTION_METHODS | QUOTE_METHODS
                    | (MARKET_RPC_METHODS & set(result["readonly"]["capabilities"]))
                    | (WRITE_METHODS if self.allow_order_methods else frozenset()))
                self._operation_phase = "WRITE_LEDGER_INSPECT"
                self._inspect_write_ledger()
                self._verified = True
                self._operation_phase = "IDENTITY_VERIFIED"
                return result
            except Exception as exc:
                self._record_error(exc)
                self._seal("IDENTITY_VERIFICATION_FAILED")
                raise
        finally:
            self._lock.release()

    @staticmethod
    def _request_id(request):
        rid = request.get("request_id")
        if (not isinstance(rid, str) or not rid or len(rid) > 128
                or any(ord(char) < 33 or ord(char) > 126 for char in rid)):
            raise ShmProtocolError("request_id requires 1-128 printable ASCII characters without spaces")
        return rid

    def _reply_request(self, request):
        rid = self._request_id(request)
        reply = dict(request)
        for field, template in _REPLY_TEMPLATES.items():
            canonical = template.format(account_id=self.account_id, request_id=rid)
            value = request.get(field)
            if value not in (None, "", canonical):
                raise ShmProtocolError("custom Redis reply target is unavailable")
            reply[field] = "" if field == "reply_list" and value in (None, "") else canonical
        ttl = request.get("ttl_seconds", self.transport.response_ttl_seconds)
        if type(ttl) is not int or not 0 < ttl <= 3600:
            raise ShmProtocolError("Redis response TTL must be within [1, 3600]")
        reply["ttl_seconds"] = ttl
        return reply

    def _validate_request(self, request):
        if type(request.get("schema_version")) is not int or request["schema_version"] != 1:
            raise ShmProtocolError("schema_version must be integer 1")
        if request.get("account_id") != self.account_id:
            raise ShmProtocolError("request account_id mismatch")
        self._validate_source_binding(request)
        method, params = request.get("method"), request.get("params")
        if method not in self._capabilities:
            raise ShmProtocolError("method is unavailable or not declared by the verified native source")
        if not isinstance(params, dict) or request.get("use_formula", False) is not False:
            raise ShmProtocolError("params must be object and formula routing is unavailable")
        account = params.get("account", self.account_id)
        if isinstance(account, dict):
            account = account.get("account_id", account.get("accountID", account.get("id")))
        if params.get("account_id", self.account_id) != self.account_id or account != self.account_id:
            raise ShmProtocolError("nested account mismatch")
        if method in QUOTE_METHODS:
            ShmReadOnlyRpcClient._quote_params(params)
        if method in MARKET_RPC_METHODS:
            ShmReadOnlyRpcClient._market_params(method, params)
        if method == "get_asset" and type(params.get("include_source_evidence", False)) is not bool:
            raise ShmProtocolError("include_source_evidence must be boolean")
        if method == "poll_execution_events" and (type(params.get("after_sequence")) is not int or params["after_sequence"] < 0):
            raise ShmProtocolError("bounded nonnegative execution event cursor required")
        return json.dumps([request["schema_version"], self.account_id, method, params],
                          sort_keys=True, allow_nan=False, separators=(",", ":"))

    def _write_binding(self):
        return dict(account_id=self.account_id, native_pid=self.expected_native_pid,
                    read_session_id=self._sessions["readonly"],
                    execution_session_id=self._sessions["execution"],
                    windows_session_id=self._source_binding()["windows_session_id"],
                    runtime_policy=self.runtime_policy, allow_order_methods=self.allow_order_methods)

    def _claim_write(self, request, fingerprint):
        """原生调用前原子占用永不过期 RID；失败或已占用均不会再次原生写入。"""
        record = dict(request_id=request["request_id"], fingerprint=fingerprint,
                      binding=self._write_binding(), state="CLAIMED", token=uuid.uuid4().hex,
                      claimed_at_unix=time.time(), automatic_retry=False)
        encoded = json.dumps(record, sort_keys=True, allow_nan=False, separators=(",", ":"))
        try:
            claimed = self.transport.redis.eval(_CLAIM_WRITE, 1, _WRITE_LEDGER_KEY,
                request["request_id"], self.write_ledger_capacity, encoded)
            if (not isinstance(claimed, (list, tuple)) or len(claimed) != 2
                    or type(claimed[0]) is not int or claimed[0] not in (-1, 0, 1)):
                raise ShmProtocolError("write reservation returned an invalid result")
            if claimed[0] == -1:
                self._block_writes("WRITE_LEDGER_CAPACITY_EXHAUSTED")
                raise ShmProtocolError("persistent write capacity exhausted; records must not be evicted")
            if claimed[0] == 1:
                if decode_text(claimed[1]) != encoded:
                    raise ShmProtocolError("write reservation identity mismatch")
                self._write_record_count += 1
                if self._write_record_count >= self.write_ledger_capacity:
                    self._block_writes("WRITE_LEDGER_CAPACITY_EXHAUSTED")
                return encoded, None
            prior = json.loads(decode_text(claimed[1]))
            if (not isinstance(prior, dict) or prior.get("request_id") != request["request_id"]
                    or prior.get("fingerprint") != fingerprint or prior.get("binding") != record["binding"]):
                raise ShmProtocolError("persisted write RID has conflicting parameters or source binding")
            replay = prior.get("reconciled_response") if prior.get("state") == "RECONCILED" else prior.get("response")
            if prior.get("state") in ("CONFIRMED", "RECONCILED") and isinstance(replay, dict):
                self._check_response(request, replay, self._sessions["execution"])
                return None, copy.deepcopy(replay)
            response = self._response(request, "prior write is unconfirmed; do not resend",
                                      "PERSISTED_WRITE_UNCONFIRMED", unknown=True)
            if isinstance(prior.get("response"), dict):
                response["data"] = copy.deepcopy(prior["response"].get("data"))
            self._block_writes("PERSISTED_WRITE_UNCONFIRMED")
            return None, response
        except Exception:
            self._seal("WRITE_LEDGER_UNCONFIRMED")
            raise

    def _settle_write(self, request, reservation, response, state=None):
        record = json.loads(reservation)
        record.update(state=state or ("UNKNOWN" if response.get("status") == "UNKNOWN" else "CONFIRMED"),
                      response=copy.deepcopy(response), settled_at_unix=time.time())
        encoded = json.dumps(record, sort_keys=True, allow_nan=False, separators=(",", ":"))
        if self.transport.redis.eval(_SETTLE_WRITE, 1, _WRITE_LEDGER_KEY,
                                     request["request_id"], reservation, encoded) != 1:
            raise ShmProtocolError("write settlement identity changed; do not resend")
        return encoded

    def _reconciliation_record(self, request_id, expected_record=None):
        """只读取精确 RID；旧请求、来源、原 UNKNOWN 必须完整保留。"""
        self._request_id({"request_id": request_id})
        encoded = self.transport.redis.hget(_WRITE_LEDGER_KEY, request_id)
        if encoded is None:
            raise ShmProtocolError("the exact write record does not exist")
        encoded = decode_text(encoded)
        if expected_record is not None and encoded != expected_record:
            raise ShmProtocolError("write record changed since the explicit preview")
        record = json.loads(encoded)
        if (not isinstance(record, dict) or record.get("request_id") != request_id
                or record.get("state") != "UNKNOWN" or record.get("binding") != self._write_binding()
                or not isinstance(record.get("token"), str) or not record["token"]
                or not isinstance(record.get("response"), dict)):
            raise ShmProtocolError("only the original UNKNOWN record on the same verified sources may be reconciled")
        original = json.loads(record.get("fingerprint", ""))
        if (not isinstance(original, list) or len(original) != 4 or type(original[0]) is not int
                or original[:2] != [1, self.account_id]
                or original[2] not in ("submit_order", "cancel_order") or not isinstance(original[3], dict)):
            raise ShmProtocolError("single submit/cancel fingerprint required; batch recovery is unavailable")
        response = record["response"]
        if (response.get("request_id") != request_id or response.get("account_id") != self.account_id
                or response.get("method") != original[2] or response.get("status") != "UNKNOWN"):
            raise ShmProtocolError("original UNKNOWN response identity mismatch")
        return encoded, record, original[2], original[3]

    @staticmethod
    def _validate_reconciliation_target(method, params, order_sys_id, order_identity):
        if (not isinstance(order_sys_id, str) or not order_sys_id
                or not isinstance(order_identity, dict) or set(order_identity) != {
                    "user_order_id", "stock_code", "action", "volume", "price"}):
            raise ShmProtocolError("explicit contract and complete original order identity required")
        identity = dict(order_identity)
        if (not isinstance(identity["user_order_id"], str) or not identity["user_order_id"]
                or not isinstance(identity["stock_code"], str) or not identity["stock_code"]
                or identity["action"] not in ("BUY", "SELL") or type(identity["volume"]) is not int
                or identity["volume"] <= 0 or type(identity["price"]) not in (int, float)
                or not math.isfinite(identity["price"]) or identity["price"] <= 0):
            raise ShmProtocolError("invalid original order identity")
        if method == "submit_order":
            tag = str(params.get("remark") or params.get("order_remark") or "").strip()
            if not tag and params.get("signal_id"):
                tag = "bqrpc:%s" % params["signal_id"]
            expected = dict(user_order_id=tag, stock_code=params.get("stock_code"),
                            action=params.get("action"), volume=params.get("volume", params.get("order_volume")),
                            price=params.get("price"))
            if expected != identity:
                raise ShmProtocolError("preview identity does not exactly match original submit parameters/remark")
        elif (str(params.get("order_sys_id") or params.get("order_sysid") or params.get("order_id") or "") != order_sys_id
                or params.get("user_order_id") != identity["user_order_id"]):
            raise ShmProtocolError("preview contract/remark does not match original cancel parameters")
        return identity

    @staticmethod
    def _evidence_timestamp(value):
        if not isinstance(value, str) or not value.endswith(("+00:00", "Z")):
            raise ShmProtocolError("native query evidence requires an explicit UTC timestamp")
        text = value[:-6] if value.endswith("+00:00") else value[:-1]
        for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S"):
            try:
                return datetime.datetime.strptime(text, fmt).replace(tzinfo=datetime.timezone.utc).timestamp()
            except ValueError:
                pass
        raise ShmProtocolError("native query evidence timestamp is invalid")

    def _validate_reconciliation_snapshot(self, method, params, order_sys_id, order_identity,
                                          data, claimed_at, query_started_at, observed_at, source_binding):
        identity = self._validate_reconciliation_target(method, params, order_sys_id, order_identity)
        if (not isinstance(source_binding, dict) or set(source_binding) != {
                "read_session_id", "execution_session_id", "native_pid", "windows_session_id"}
                or any(not isinstance(source_binding.get(key), str) or not source_binding[key]
                       for key in ("read_session_id", "execution_session_id"))
                or source_binding["read_session_id"] == source_binding["execution_session_id"]
                or type(source_binding.get("native_pid")) is not int or source_binding["native_pid"] <= 0
                or type(source_binding.get("windows_session_id")) is not int or source_binding["windows_session_id"] < 0):
            raise ShmProtocolError("native evidence requires a complete original dual-source binding")
        for timestamp in (claimed_at, query_started_at, observed_at):
            if type(timestamp) not in (int, float) or not math.isfinite(timestamp) or timestamp <= 0:
                raise ShmProtocolError("original claim and native query timestamps are required")
        if not claimed_at <= observed_at or not query_started_at <= observed_at <= query_started_at + 60:
            raise ShmProtocolError("native evidence observation falls outside its query deadline")
        if (not isinstance(data, dict) or data.get("account_id") != self.account_id
                or not isinstance(data.get("orders"), list) or not isinstance(data.get("trades"), list)
                or len(data["orders"]) > 10000 or len(data["trades"]) > 10000):
            raise ShmProtocolError("native full-account snapshot is missing or exceeds the recovery bound")
        try:
            evidence_bytes = json.dumps(data, sort_keys=True, allow_nan=False, separators=(",", ":")).encode("utf-8")
        except (TypeError, ValueError) as exc:
            raise ShmProtocolError("native recovery evidence is not finite JSON data") from exc
        if len(evidence_bytes) > 2 * 1024 * 1024:
            raise ShmProtocolError("native recovery evidence exceeds the fixed 2 MiB record bound")
        counts = []
        for key, kind, rows in (("orders_source_evidence", "ORDER", data["orders"]),
                                ("trades_source_evidence", "DEAL", data["trades"])):
            evidence = data.get(key)
            if (not isinstance(evidence, dict) or type(evidence.get("contract_version")) is not int
                    or evidence["contract_version"] != 1 or evidence.get("account_id") != self.account_id
                    or evidence.get("account_type") != "STOCK" or evidence.get("detail_type") != kind
                    or evidence.get("source") != "native_get_trade_detail_data"
                    or evidence.get("strategy_name") != "" or evidence.get("complete") is not True
                    or evidence.get("status") != ("OK" if rows else "EMPTY")
                    or evidence.get("error_code") not in (None, "")
                    or evidence.get("unknown_fields") != [] or evidence.get("row_errors") != []
                    or any(type(evidence.get(field)) is not int or evidence[field] != len(rows)
                           for field in ("raw_row_count", "converted_row_count", "returned_row_count"))
                    or any(type(evidence.get(field)) is not int or evidence[field] != 0
                           for field in ("skipped_row_count", "duplicate_row_count"))):
                raise ShmProtocolError("native order/deal completeness evidence is missing or inconsistent")
            if (evidence.get("session_id") != source_binding["execution_session_id"]
                    or type(evidence.get("native_pid")) is not int
                    or evidence["native_pid"] != source_binding["native_pid"]
                    or type(evidence.get("windows_session_id")) is not int
                    or evidence["windows_session_id"] != source_binding["windows_session_id"]):
                raise ShmProtocolError("native order/deal evidence source identity differs from the original session")
            when = self._evidence_timestamp(evidence.get("queried_at"))
            if not query_started_at - 1 <= when <= observed_at + 1:
                raise ShmProtocolError("native order/deal query evidence is stale or future")
            counts.append(len(rows))
        aggregate = data.get("source_evidence")
        if (not isinstance(aggregate, dict) or type(aggregate.get("contract_version")) is not int
                or aggregate["contract_version"] != 1 or aggregate.get("account_id") != self.account_id
                or aggregate.get("source") != "native_ORDER_DEAL_queries" or aggregate.get("complete") is not True
                or aggregate.get("status") != "OK" or aggregate.get("error_code") not in (None, "")
                or any(type(aggregate.get(field)) is not int or aggregate[field] != sum(counts)
                       for field in ("raw_row_count", "returned_row_count"))
                or aggregate.get("queried_at") != data["trades_source_evidence"]["queried_at"]):
            raise ShmProtocolError("native aggregate snapshot completeness evidence is inconsistent")
        if (aggregate.get("session_id") != source_binding["execution_session_id"]
                or type(aggregate.get("native_pid")) is not int or aggregate["native_pid"] != source_binding["native_pid"]
                or type(aggregate.get("windows_session_id")) is not int
                or aggregate["windows_session_id"] != source_binding["windows_session_id"]):
            raise ShmProtocolError("native aggregate evidence source identity differs from the original session")
        related = lambda row: (row.get("order_sys_id") == order_sys_id
                               or row.get("user_order_id") == identity["user_order_id"])
        if any(not isinstance(row, dict) or not isinstance(row.get("order_sys_id"), str)
               or not row["order_sys_id"] or not isinstance(row.get("user_order_id"), str)
               for row in data["orders"] + data["trades"]):
            raise ShmProtocolError("native snapshot contains malformed rows")
        orders = [row for row in data["orders"] if related(row)]
        trades = [row for row in data["trades"] if related(row)]
        if len(orders) != 1:
            raise ShmProtocolError("the exact contract/remark must identify one native order")
        order = orders[0]
        for row in orders + trades:
            if (row.get("account_id", self.account_id) != self.account_id
                    or row.get("order_sys_id") != order_sys_id
                    or any(row.get(field) != identity[field] for field in ("user_order_id", "stock_code", "action"))):
                raise ShmProtocolError("native contract/remark/account/order identity conflict")
        price = order.get("price")
        if (type(order.get("volume")) is not int or order["volume"] != identity["volume"]
                or type(price) not in (int, float) or not math.isfinite(price)
                or not math.isclose(price, identity["price"], rel_tol=0.0, abs_tol=1e-8)):
            raise ShmProtocolError("native quantity/limit price differs from the original order")
        status, filled = order.get("status"), order.get("traded_volume")
        if (status not in ("53", "54", "56", "57") or type(filled) is not int
                or not 0 <= filled <= identity["volume"]
                or (status in ("54", "57") and filled != 0)
                or (status == "53" and not 0 < filled < identity["volume"])
                or (status == "56" and filled != identity["volume"])):
            raise ShmProtocolError("native order is not an unambiguous cancelled/rejected/filled terminal result")
        shanghai = datetime.timezone(datetime.timedelta(hours=8))
        order_time = order.get("order_time")
        if (type(order_time) not in (int, float) or not math.isfinite(order_time) or order_time <= 0
                or order_time > observed_at + 1
                or (method == "submit_order" and order_time < claimed_at - _BROKER_PROXY_CLOCK_TOLERANCE_SECONDS)
                or (method == "cancel_order" and order_time > claimed_at + 1)
                or datetime.datetime.fromtimestamp(order_time, shanghai).date() !=
                    datetime.datetime.fromtimestamp(claimed_at, shanghai).date()):
            raise ShmProtocolError("native order event time does not match the original claim trading day")
        seen, quantity = set(), 0
        for trade in trades:
            trade_id, volume, trade_price = trade.get("trade_id"), trade.get("volume"), trade.get("price")
            trade_time = trade.get("traded_time")
            if (not isinstance(trade_id, str) or not trade_id or trade_id in seen
                    or type(volume) is not int or volume <= 0 or type(trade_price) not in (int, float)
                    or not math.isfinite(trade_price) or trade_price <= 0
                    or (identity["action"] == "BUY" and trade_price > identity["price"] + 1e-8)
                    or (identity["action"] == "SELL" and trade_price < identity["price"] - 1e-8)
                    or type(trade_time) not in (int, float) or not math.isfinite(trade_time)
                    or not order_time <= trade_time <= observed_at + 1
                    or datetime.datetime.fromtimestamp(trade_time, shanghai).date() !=
                        datetime.datetime.fromtimestamp(claimed_at, shanghai).date()):
                raise ShmProtocolError("native deal identity/quantity/price/time is invalid or duplicated")
            seen.add(trade_id)
            quantity += volume
        if quantity != filled:
            raise ShmProtocolError("native deal quantities do not close the terminal order")
        return dict(order_sys_id=order_sys_id, order_identity=identity, order=copy.deepcopy(order),
                    trades=copy.deepcopy(trades), terminal_verified=True,
                    request_result="BROKER_ORDER_FOUND" if method == "submit_order" else
                        ("CANCEL_CONFIRMED" if status in ("53", "54") else "ORDER_TERMINAL_CANCEL_NOT_PROVEN"),
                    business_terminal_status=status, filled_volume=filled,
                    broker_order_time_precision="seconds", clocks_assumed_identical=False,
                    broker_proxy_clock_offset_seconds=order_time - claimed_at,
                    broker_proxy_clock_tolerance_seconds=_BROKER_PROXY_CLOCK_TOLERANCE_SECONDS,
                    snapshot=copy.deepcopy(data), original_claimed_at_unix=claimed_at,
                    query_started_at_unix=query_started_at, observed_at_unix=observed_at)

    def _terminal_reconciliation_proof(self, method, params, order_sys_id, order_identity, claimed_at):
        """新请求只读原生完整账户订单/成交；不接受外部传入的订单证明。"""
        self._validate_reconciliation_target(method, params, order_sys_id, order_identity)
        for route in self._clients:
            self._check_client(route)
        now = time.time()
        query = dict(schema_version=1, request_id=uuid.uuid4().hex, account_id=self.account_id,
                     method="query_execution_snapshot", params=dict(account_id=self.account_id,
                         order_strategy_name="", trade_strategy_name=""),
                     read_expires_at_unix=now + self.timeout_seconds)
        response = self._clients["execution"].send_request(query, timeout_seconds=self.timeout_seconds)
        for route in self._clients:
            self._check_client(route)
        self._check_response(query, response, self._sessions["execution"])
        if time.time() >= query["read_expires_at_unix"] or not response["ok"] or response.get("status") == "UNKNOWN":
            raise ShmProtocolError("fresh native execution snapshot was not confirmed")
        proof = self._validate_reconciliation_snapshot(method, params, order_sys_id, order_identity,
                    response["data"], claimed_at, now, time.time(), self._source_binding())
        proof.update(source_binding=self._source_binding(), evidence_request_id=query["request_id"],
                     evidence_sequence=response["sequence"])
        return proof

    def _validate_reconciled_record(self, record):
        """启动只信完整保存事实；RECONCILED 标签或布尔值本身不解除停写。"""
        proof, original, replay = record.get("reconciliation"), record.get("response"), record.get("reconciled_response")
        if (not isinstance(proof, dict) or proof.get("original_state") != "UNKNOWN"
                or proof.get("terminal_verified") is not True or not isinstance(original, dict)
                or record.get("automatic_retry") is not False
                or not isinstance(record.get("token"), str) or not record["token"]
                or record["binding"].get("runtime_policy") != "PERSISTENT_SIMULATION_READ_WRITE"
                or record["binding"].get("allow_order_methods") is not True
                or proof.get("original_result") != original or original.get("status") != "UNKNOWN"
                or original.get("automatic_retry") is not False
                or original.get("request_id") != record.get("request_id")
                or original.get("account_id") != self.account_id or not isinstance(replay, dict)
                or not isinstance(proof.get("operator"), str) or not proof["operator"].strip()
                or type(proof.get("reconciled_at_unix")) not in (int, float)
                or not math.isfinite(proof["reconciled_at_unix"])
                or proof.get("original_claimed_at_unix") != record.get("claimed_at_unix")
                or not isinstance(proof.get("source_binding"), dict)
                or any(proof["source_binding"].get(field) != record["binding"].get(field)
                       for field in ("read_session_id", "execution_session_id", "native_pid", "windows_session_id"))
                or not isinstance(proof.get("evidence_request_id"), str) or not proof["evidence_request_id"]
                or type(proof.get("evidence_sequence")) is not int or proof["evidence_sequence"] <= 0):
            raise ShmProtocolError("persistent reconciliation has incomplete original result/source/evidence")
        fingerprint = json.loads(record["fingerprint"])
        if (not isinstance(fingerprint, list) or len(fingerprint) != 4
                or type(fingerprint[0]) is not int or fingerprint[:2] != [1, self.account_id]
                or fingerprint[2] not in ("submit_order", "cancel_order") or not isinstance(fingerprint[3], dict)
                or original.get("method") != fingerprint[2]):
            raise ShmProtocolError("persistent reconciliation original fingerprint is invalid")
        validated = self._validate_reconciliation_snapshot(fingerprint[2], fingerprint[3],
            proof.get("order_sys_id"), proof.get("order_identity"), proof.get("snapshot"),
            record.get("claimed_at_unix"), proof.get("query_started_at_unix"), proof.get("observed_at_unix"),
            proof.get("source_binding"))
        if any(proof.get(field) != value for field, value in validated.items()):
            raise ShmProtocolError("persistent reconciliation facts conflict with its saved native evidence")
        if not proof["observed_at_unix"] <= proof["reconciled_at_unix"]:
            raise ShmProtocolError("persistent reconciliation apply time predates observation")
        expected_data = dict(status="RECONCILED", order_sys_id=proof["order_sys_id"],
                             user_order_id=proof["order_identity"]["user_order_id"],
                             request_result=proof["request_result"], business_terminal_status=proof["business_terminal_status"],
                             filled_volume=proof["filled_volume"], automatic_retry=False)
        if (replay.get("status") != "RECONCILED" or replay.get("ok") is not True
                or replay.get("automatic_retry") is not False
                or replay.get("data") != expected_data or replay.get("source_binding") != proof["source_binding"]
                or replay.get("sequence") != proof["evidence_sequence"]
                or replay.get("session_id") != record["binding"].get("execution_session_id")):
            raise ShmProtocolError("persistent reconciliation replay differs from saved facts")
        self._check_response(dict(schema_version=1, request_id=record["request_id"], account_id=self.account_id,
                                  method=fingerprint[2]), replay, record["binding"]["execution_session_id"])

    def reconcile_unknown_write(self, request_id, order_sys_id, order_identity, *,
                                expected_record=None, operator=None, apply=False):
        """显式单 RID 核对。默认只读预览；apply 仅 CAS 原账本，不发业务消息。

        终态核对不把原提交改为 CONFIRMED，也不宣称 OMS 回报已经落地。
        """
        if not self._lock.acquire(False):
            raise ShmProtocolError("proxy already has an in-flight operation")
        try:
            self._ensure_open()
            if not self._verified or not self.allow_order_methods:
                raise ShmProtocolError("verified simulation read/write sources are required")
            if type(apply) is not bool or (apply and (not isinstance(expected_record, str) or not expected_record
                    or not isinstance(operator, str) or not operator.strip() or len(operator) > 128)):
                raise ShmProtocolError("apply requires exact previewed record and explicit operator")
            encoded, record, method, params = self._reconciliation_record(request_id, expected_record)
            proof = self._terminal_reconciliation_proof(method, params, order_sys_id, order_identity,
                                                        record.get("claimed_at_unix"))
            if decode_text(self.transport.redis.hget(_WRITE_LEDGER_KEY, request_id)) != encoded:
                raise ShmProtocolError("write record changed during native evidence collection")
            preview = dict(status="RECONCILIATION_PREVIEW", request_id=request_id, account_id=self.account_id,
                           expected_record=encoded, proof=proof, ledger_records_written=0,
                           native_write_calls=0, original_result_preserved=True, oms_terminal_proven=False)
            if not apply:
                return preview
            reconciled = copy.deepcopy(record)
            reconciled.update(state="RECONCILED", reconciliation=dict(proof, original_state=record["state"],
                                original_result=copy.deepcopy(record["response"]), operator=operator.strip(),
                                reconciled_at_unix=time.time()))
            replay = copy.deepcopy(record["response"])
            replay.update(status="RECONCILED", ok=True, error="", server_error="", automatic_retry=False,
                          session_id=self._sessions["execution"], sequence=proof["evidence_sequence"],
                          data=dict(status="RECONCILED", order_sys_id=order_sys_id,
                                    user_order_id=order_identity["user_order_id"],
                                    request_result=proof["request_result"],
                                    business_terminal_status=proof["business_terminal_status"],
                                    filled_volume=proof["filled_volume"], automatic_retry=False),
                          source_binding=self._source_binding())
            reconciled["reconciled_response"] = replay
            replacement = json.dumps(reconciled, sort_keys=True, allow_nan=False, separators=(",", ":"))
            try:
                committed = self.transport.redis.eval(_SETTLE_WRITE, 1, _WRITE_LEDGER_KEY, request_id, encoded, replacement)
            except Exception as exc:
                self._block_writes("RECONCILIATION_COMMIT_UNCONFIRMED")
                preview.update(status="UNKNOWN", ledger_records_written=None, automatic_retry=False,
                               error="reconciliation CAS result is unknown: %s" % type(exc).__name__,
                               writes_blocked=True, write_block_reason=self._write_block_reason)
                return preview
            if committed != 1:
                raise ShmProtocolError("write reconciliation CAS rejected; do not retry automatically")
            self._replays.pop(request_id, None)
            # 只撤销已核销账本带来的门禁；人工停写和其他未知项继续生效。
            if self._write_block_reason in ("PERSISTED_WRITE_UNCONFIRMED", "NATIVE_WRITE_RESULT_UNKNOWN"):
                self._writes_blocked, self._write_block_reason = False, ""
            if self._operator_write_blocked:
                self._block_writes("OPERATOR_WRITE_DISABLED")
            try:
                if decode_text(self.transport.redis.hget(_WRITE_LEDGER_KEY, request_id)) != replacement:
                    raise ShmProtocolError("reconciliation exact readback changed")
                self._inspect_write_ledger()
            except Exception as exc:
                self._block_writes("RECONCILIATION_POSTCHECK_UNCONFIRMED")
                preview.update(status="UNKNOWN", ledger_records_written=1, automatic_retry=False,
                               error="reconciliation postcheck is unknown: %s" % type(exc).__name__,
                               writes_blocked=True, write_block_reason=self._write_block_reason)
                return preview
            preview.update(status="RECONCILED", ledger_records_written=1,
                           writes_blocked=self._writes_blocked, write_block_reason=self._write_block_reason)
            return preview
        finally:
            self._lock.release()

    def _response(self, request, error, code, *, unknown=False):
        response = dict(schema_version=1, request_id=request.get("request_id"), account_id=self.account_id,
                    method=request.get("method"), ok=False, data=None, error=str(error), server_error=code,
                    handled_at=datetime.datetime.utcnow().isoformat(),
                    status="UNKNOWN" if unknown else "REJECTED", automatic_retry=False)
        binding = self._source_binding()
        if binding is not None:
            response["source_binding"] = binding
        return response

    def process_raw(self, raw_payload, source="queue"):
        """沿用原 JSON/b64s 编码；无法取得合法身份时不写 Redis。"""
        self._operation_phase = "REQUEST_DECODE"
        request = raw_payload if isinstance(raw_payload, dict) else json.loads(
            decode_rpc_request_payload(decode_text(raw_payload)))
        return self.process_request(request)

    def process_request(self, request):
        """一次处理并应答；缓存只复用已确认结果，不再次调用 SHM。"""
        if not self._lock.acquire(False):
            raise ShmProtocolError("proxy already has an in-flight operation")
        try:
            self._ensure_open()
            if not self._verified:
                raise ShmProtocolError("explicit identity handshake required")
            if not isinstance(request, dict):
                raise ShmProtocolError("request must be object")
            request = copy.deepcopy(request)
            self._request_context = {key: request[key][:128] for key in ("request_id", "method")
                                     if isinstance(request.get(key), str)}
            self._operation_phase = "REQUEST_VALIDATE"
            # 先拒绝无法安全寻址的请求；不写任意 custom 回复键。
            reply = self._reply_request(request)
            rid = request["request_id"]
            reservation = None
            write_settlement = None
            try:
                fingerprint = self._validate_request(request)
                self._reclaim_reads()
                if rid in self._replays:
                    # 已确认结果可以在旧 read_expires 到期后返回原缓存；该
                    # 期限只控制原生执行，不刷新 handled_at 或重新查询 QMT。
                    prior_fp, prior = self._replays[rid]
                    if prior_fp != fingerprint:
                        raise ShmProtocolError("request_id semantic conflict")
                    try:
                        self._operation_phase = "SHM_IDENTITY_CHECK"
                        for checked_route in self._clients:
                            self._check_client(checked_route)
                        response = copy.deepcopy(prior)
                    except Exception as exc:
                        # 旧会话已确认结果仍留本地供核对；新 SID 不能让旧
                        # 代理继续正常应答，也不能覆盖原确认缓存。
                        response = self._response(request, exc, "SHM_IDENTITY_CHANGED", unknown=True)
                        self._seal("SHM_IDENTITY_CHANGED")
                else:
                    is_write = request["method"] in WRITE_METHODS
                    self._operation_phase = "WRITE_GATE_CHECK" if is_write else "REQUEST_VALIDATE"
                    if (is_write and self._writes_blocked
                            and self.transport.redis.hget(_WRITE_LEDGER_KEY, rid) is None):
                        raise ShmProtocolError("new writes are blocked: %s; inspect existing results" % self._write_block_reason)
                    if self.runtime_policy == "BOUNDED" and self._processed_count >= self.max_requests:
                        self._seal("SESSION_CAPACITY_EXHAUSTED")
                        raise ShmProtocolError("session request capacity exhausted")
                    if (self.runtime_policy != "BOUNDED" and not is_write
                            and len(self._read_expiries) >= self.read_cache_capacity):
                        raise ShmProtocolError("confirmed reads have not expired; finite cache capacity exhausted")
                    now = time.time()
                    expires = request.get("read_expires_at_unix", now + self.timeout_seconds)
                    if type(expires) not in (int, float) or not math.isfinite(expires) or expires <= now:
                        raise ShmProtocolError("read request is expired or has an invalid deadline")
                    # 正式缓存历史入口可显式申请最多 15 秒；消费前已耗时
                    # 仍由调用方原始绝对截止扣除，其他方法保留原预算。
                    request_budget = (15.0 if request["method"] == "get_market_data_ex"
                                      and "read_expires_at_unix" in request
                                      else self.timeout_seconds)
                    deadline = min(expires, now + request_budget)
                    if self.deadline_unix is not None:
                        deadline = min(deadline, self.deadline_unix)
                    route = "readonly" if request["method"] in QUOTE_METHODS | MARKET_RPC_METHODS else "execution"
                    forwarded = dict(schema_version=1, request_id=rid, account_id=self.account_id,
                                     method=request["method"], params=dict(request["params"]),
                                     read_expires_at_unix=deadline)
                    try:
                        # 两路是一次固定身份合同；任何一路换 SID 都不能让旧
                        # 代理继续沿另一通道运行。
                        self._operation_phase = "SHM_IDENTITY_CHECK"
                        for checked_route in self._clients:
                            self._check_client(checked_route)
                        if is_write:
                            self._operation_phase = "WRITE_LEDGER_CLAIM"
                            reservation, prior_write = self._claim_write(request, fingerprint)
                        else:
                            prior_write = None
                        client = self._clients[route]
                        self._ensure_open()
                        remaining = deadline - time.time()
                        if remaining <= 0:
                            raise ShmProtocolError("read deadline expired before forwarding")
                        self._operation_phase = "SHM_FORWARD"
                        response = (prior_write if prior_write is not None else
                                    client.send_request(forwarded, timeout_seconds=remaining))
                        self._operation_phase = "SHM_RESPONSE_VALIDATE"
                        if prior_write is None:
                            self._check_response(forwarded, response, self._sessions[route])
                        response = copy.deepcopy(response)
                        binding = self._source_binding()
                        if binding is not None:
                            response["source_binding"] = binding
                        if time.time() >= deadline:
                            raise ShmProtocolError("SHM response arrived after deadline")
                        data = response.get("data")
                        # 登录回调尚未观测是已确认的业务未知，不能当作 SHM
                        # 请求结果未知；仅接受当前原生账户/会话的明确回读。
                        login_state_unknown = (
                            request["method"] == "get_trade_login_state" and response["ok"]
                            and isinstance(data, dict) and data.get("status") == "UNKNOWN"
                            and data.get("source") == "native_account_callback"
                            and data.get("account_id") == self.account_id
                            and data.get("session_id") == self._sessions[route]
                            and type(data.get("native_pid")) is int
                            and data["native_pid"] == self.expected_native_pid
                            and data.get("positive_contract_verified") is False
                        )
                        if isinstance(data, dict) and data.get("status") == "UNKNOWN" and not login_state_unknown:
                            response.update(status="UNKNOWN", automatic_retry=False)
                            if is_write:
                                self._block_writes("NATIVE_WRITE_RESULT_UNKNOWN")
                            else:
                                response.update(ok=False, error=response["data"].get("error") or "SHM result is UNKNOWN",
                                                server_error="SHM_RESULT_UNKNOWN")
                                self._seal("SHM_RESULT_UNKNOWN")
                        if request["method"] == "ping" and response["ok"]:
                            self._validate_identity_data("execution", response["data"])
                            self._source_identity["execution"] = copy.deepcopy(response["data"])
                            response["data"] = self.identity_snapshot()
                        if request["method"] == "get_asset" and response["ok"]:
                            wrapped = response.get("data")
                            if (not isinstance(wrapped, dict) or not isinstance(wrapped.get("source_evidence"), dict)
                                    or wrapped["source_evidence"].get("complete") is not True
                                    or not isinstance(wrapped.get("asset"), dict)):
                                response.update(ok=False, error="native asset source is incomplete",
                                                server_error="ASSET_SOURCE_INCOMPLETE")
                            elif not request["params"].get("include_source_evidence", False):
                                response["data"] = wrapped["asset"]
                                response["source_evidence"] = wrapped["source_evidence"]
                    except Exception as exc:
                        self._record_error(exc)
                        response = self._response(request, exc, "SHM_RESULT_UNCONFIRMED", unknown=True)
                        self._seal("SHM_RESULT_UNCONFIRMED")
                    if reservation is not None:
                        try:
                            self._operation_phase = "WRITE_LEDGER_SETTLE"
                            write_settlement = self._settle_write(request, reservation, response,
                                "UNKNOWN" if response.get("status") == "UNKNOWN" else "PUBLISH_PENDING")
                        except Exception as exc:
                            self._record_error(exc)
                            response = self._response(request, exc, "WRITE_LEDGER_SETTLEMENT_UNCONFIRMED", unknown=True)
                            self._seal("WRITE_LEDGER_SETTLEMENT_UNCONFIRMED")
                    self._replays[rid] = (fingerprint, copy.deepcopy(response))
                    self._processed_count += 1
                    if (not is_write and response.get("status") != "UNKNOWN"):
                        self._read_expiries[rid] = deadline
            except (ShmProtocolError, ValueError, TypeError) as exc:
                response = self._response(request, exc, "PROXY_REQUEST_REJECTED")
            binding = self._source_binding()
            if binding is not None:
                response["source_binding"] = binding
            try:
                self._operation_phase = "REDIS_RESPONSE_PUBLISH"
                self.transport.send_response(reply, response)
                if write_settlement is not None and response.get("status") != "UNKNOWN":
                    # 先持久留下发布待确认，再 fanout，全部完成才晋级；
                    # 即使最后一次 Redis 写失联，重启也不能解除停写门。
                    self._operation_phase = "WRITE_LEDGER_CONFIRM"
                    write_settlement = self._settle_write(request, write_settlement, response, "CONFIRMED")
            except Exception as exc:
                self._record_error(exc)
                # 一次 fanout 的部分写入也可能已对外可见，只封闭并留本地缓存。
                self._seal("REDIS_RESPONSE_PUBLISH_FAILED")
                if write_settlement is not None:
                    try:
                        self._settle_write(request, write_settlement, response, "PUBLISH_UNCONFIRMED")
                    except Exception:
                        # 至少原子占用记录仍在；同 RID 永远不能重新原生执行。
                        self.stop_reason = "REDIS_RESPONSE_AND_WRITE_LEDGER_UNCONFIRMED"
                raise
            return response
        finally:
            self._lock.release()

    def get_cached_response(self, request_id):
        """只读本地结果；发布失败后也不触发 Redis 或 SHM 重放。"""
        prior = self._replays.get(request_id)
        return None if prior is None else copy.deepcopy(prior[1])

    def poll_once(self):
        """空队列只读检查；仅未消费业务的LLEN超时可有限重检，其余失败封闭。"""
        if not self._poll_lock.acquire(False):
            raise ShmProtocolError("proxy already has an in-flight poll")
        try:
            self._ensure_open()
            if not self._verified:
                raise ShmProtocolError("explicit identity handshake required")
            if self.runtime_policy == "BOUNDED" and self._processed_count >= self.max_requests:
                self._seal("SESSION_CAPACITY_EXHAUSTED")
                return None
            try:
                self._request_context = {}
                if self._pubsub is None:
                    self._operation_phase = "REDIS_PUBSUB_CREATE"
                    self._pubsub = self.transport.listen_redis.pubsub(ignore_subscribe_messages=True)
                    self._operation_phase = "REDIS_SUBSCRIBE"
                    self._pubsub.subscribe(self.transport.request_channel)
                self._operation_phase = "REDIS_PUBSUB_RECEIVE"
                message = self._pubsub.get_message(timeout=0)
                if message and message.get("type") == "message":
                    return self.process_raw(message.get("data"), source="pubsub")
                # 到此尚未取得PubSub业务，也没有发出破坏性LPOP。空闲
                # TCP检查超时可以丢弃该只读结果，下轮重新检查而不重发业务。
                self._operation_phase = "REDIS_QUEUE_INSPECT"
                try:
                    length = self.transport.listen_redis.llen(self.transport.request_queue)
                except Exception as exc:
                    if not _is_redis_timeout(exc):
                        raise
                    self._record_error(exc)
                    self._idle_receive_timeouts += 1
                    self._consecutive_idle_timeouts += 1
                    if self._consecutive_idle_timeouts >= 3:
                        self._seal("REDIS_IDLE_INSPECTION_TIMEOUT_LIMIT")
                        raise
                    return None
                if type(length) is not int or length < 0:
                    raise ShmProtocolError("queue length must be a nonnegative integer")
                self._consecutive_idle_timeouts = 0
                if length == 0:
                    self._operation_phase = "IDLE"
                    return None
                self._operation_phase = "REDIS_QUEUE_LPOP"
                raw = self.transport.listen_redis.lpop(self.transport.request_queue)
                return None if raw is None else self.process_raw(raw, source="queue")
            except Exception as exc:
                self._record_error(exc)
                self._seal("REDIS_RECEIVE_OR_PROCESS_FAILED")
                raise
        finally:
            self._poll_lock.release()

    def close(self):
        """释放接管的客户端与自有订阅，不关闭共享 Redis 连接池或删除键。"""
        if not self._poll_lock.acquire(False):
            raise ShmProtocolError("cannot close an in-flight poll")
        if not self._lock.acquire(False):
            self._poll_lock.release()
            raise ShmProtocolError("cannot close an in-flight operation")
        try:
            if self._resources_closed:
                return
            self._seal("CLOSED")
            errors = []
            for resource in list(self._clients.values()) + ([self._pubsub] if self._pubsub is not None else []):
                try:
                    resource.close()
                except Exception as exc:
                    errors.append(str(exc))
            try:
                self.transport.stop()
            except Exception as exc:
                errors.append(str(exc))
            self._resources_closed = not errors
            if errors:
                raise ShmProtocolError("proxy resource release incomplete: " + "; ".join(errors))
        finally:
            self._lock.release()
            self._poll_lock.release()


# 旧只读类名继续使用默认禁写合同；新入口只是同一显式政策实现的别名。
RedisShmProxy = RedisShmReadOnlyProxy
