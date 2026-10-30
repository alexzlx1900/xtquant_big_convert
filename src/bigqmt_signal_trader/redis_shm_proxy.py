# coding: utf-8
"""QMT 外的显式账户 Redis → 双共享内存兼容代理。

注入的两个 SHM 客户端由代理接管并关闭；Redis 连接池仍归调用方。
本模块不创建后台线程，不连接 QMT Socket，也不重开或重发未知请求。
"""

import copy
import ctypes
import datetime
import hashlib
import json
import math
import os
import re
import struct
import threading
import time
import uuid

try:
    import psutil
except ImportError:
    psutil = None

from redis.exceptions import ConnectionError as RedisConnectionError, TimeoutError as RedisTimeoutError

from .adapters.redis_common import decode_text
from .redis_rpc import decode_rpc_request_payload
from .transports.readonly_shm import (
    READY, REQUEST_CAPACITY, REQUEST_OFFSET, RESPONSE_CAPACITY, RESPONSE_OFFSET,
    ShmBusyError, ShmProtocolError, ShmReadOnlyClient, ShmTimeoutError, decode_payload,
)
from .transports.readonly_shm_client import MARKET_RPC_METHODS, MINUTE_BATCH_CONTRACT, ShmReadOnlyRpcClient
from .transports.redis_transport import _is_redis_timeout
from .transports.shm_rpc import (
    ALL_WRITE_METHODS, ASYNC_SUBMIT_CONTRACT, ASYNC_WRITE_METHODS, WRITE_METHODS, validate_async_submit_params,
)


EXECUTION_METHODS = frozenset((
    "ping", "get_asset", "get_positions", "query_orders", "query_trades",
    "query_execution_snapshot", "get_trade_login_state", "poll_execution_events",
))
QUOTE_METHODS = frozenset(("get_ticks", "get_full_tick"))
PURE_READ_METHODS = EXECUTION_METHODS | QUOTE_METHODS | frozenset(("get_local_data", "get_market_data_ex", "get_market_data_ex_batch"))
RUNTIME_POLICIES = frozenset(("BOUNDED", "PERSISTENT_READONLY", "PERSISTENT_SIMULATION_READ_WRITE", "PERSISTENT_LIVE_READ_WRITE"))
WRITE_POLICIES = frozenset(("PERSISTENT_SIMULATION_READ_WRITE", "PERSISTENT_LIVE_READ_WRITE"))
RECEIVE_POLICIES = frozenset(("QUEUE_AND_PUBSUB", "QUEUE_ONLY"))
READ_FAULT_ISOLATION_CONTRACT = "READ_FAILURE_ISOLATION_V1"
READ_PY_CALLBACK_RETURNED_CONTRACT = "READ_PY_CALLBACK_RETURNED_V1"
READ_PY_CALLBACK_WITNESS_TTL_SECONDS = 3.0
_READ_CALLBACK_WITNESS = struct.Struct("<II")
_READ_CALLBACK_WITNESS_OFFSET = 56
POSITION_READ_ROUTE_CONTRACT = "POSITION_READ_ROUTE_V1"
EXECUTION_CURSOR_WRITE_GATE_CONTRACT = "EXECUTION_CURSOR_WRITE_GATE_V1"
EXECUTION_CURSOR_NO_WRITE_CONTRACT = "EXECUTION_CURSOR_NO_WRITE_V1"
SUPERVISOR_WRITE_GATE_CONTRACT = "INHERITED_SUPERVISOR_GATE_V1"
SUPERVISOR_GATE_EPOCH_CONTRACT = "SUPERVISOR_GATE_EPOCH_V1"
SUPERVISOR_GATE_NO_WRITE_CONTRACT = "SUPERVISOR_GATE_NO_WRITE_V1"
SOURCE_OBSERVATION_TTL_SECONDS = 60.0
SUPERVISOR_ATTACH_BUDGET_SECONDS = 20.0
POSITION_READ_TIMING_FIELDS = (
    "native_started_at_unix", "native_finished_at_unix", "shm_client_started_at_unix",
    "shm_deadline_at_unix", "shm_lock_acquired_at_unix", "shm_published_at_unix",
    "shm_response_observed_at_unix", "shm_response_validated_at_unix", "shm_ack_at_unix",
)
EVENT_POLL_TIMING_FIELDS = POSITION_READ_TIMING_FIELDS + (
    "native_started_at_monotonic", "native_started_at_monotonic_after",
    "native_finished_at_monotonic", "native_finished_at_monotonic_after",
    "shm_client_started_at_monotonic", "shm_client_started_at_monotonic_after",
    "shm_published_at_monotonic", "shm_published_at_monotonic_after",
    "shm_response_observed_at_monotonic", "shm_response_observed_at_monotonic_after",
    "shm_failed_at_unix", "shm_failed_at_monotonic", "shm_failed_at_monotonic_after",
)
# 柜台订单秒精度时钟与代理本机时钟不同；1258真实样本偏差为-3.049735秒。
_BROKER_PROXY_CLOCK_TOLERANCE_SECONDS = 5.0
_CLAIM_WRITE = """
local prior = redis.call('HGET', KEYS[1], ARGV[1])
if prior then return {0, prior} end
if redis.call('HLEN', KEYS[1]) >= tonumber(ARGV[2]) then return {-1, ''} end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[3])
return {1, ARGV[3]}
"""
_CLAIM_ROTATION = """
-- ROTATION_HISTORY_CLAIM_V1
local history = cjson.decode(ARGV[4])
local count = #history
if redis.call('HLEN', KEYS[1]) ~= count or redis.call('HLEN', KEYS[2]) ~= count then return {-2, ''} end
if count > 0 and (redis.call('TTL', KEYS[1]) ~= -1 or redis.call('TTL', KEYS[2]) ~= -1) then return {-2, ''} end
for _, item in ipairs(history) do
  if redis.call('HGET', KEYS[1], item[1]) ~= item[2] or redis.call('HGET', KEYS[2], item[1]) ~= item[3] then
    return {-2, ''}
  end
end
if redis.call('HGET', KEYS[1], ARGV[1]) then return {0, ''} end
if count >= tonumber(ARGV[2]) then return {-1, ''} end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[3])
return {1, ARGV[3]}
"""
_VERIFY_ROTATION_DEPENDENCIES = """
-- ROTATION_DEPENDENCIES_VERIFY_V1
local dependencies = cjson.decode(ARGV[1])
if redis.call('TTL', KEYS[1]) ~= -1 then return 0 end
if ARGV[2] == 'FULL' then
  local count = 0
  for _, item in ipairs(dependencies) do if item[3] ~= false then count = count + 1 end end
  if redis.call('HLEN', KEYS[1]) ~= #dependencies or redis.call('HLEN', KEYS[2]) ~= count then return 0 end
end
for _, item in ipairs(dependencies) do
  if redis.call('HGET', KEYS[1], item[1]) ~= item[2] then return 0 end
  local closed = redis.call('HGET', KEYS[2], item[1])
  if item[3] == false then
    if closed then return 0 end
  elseif redis.call('TTL', KEYS[2]) ~= -1 or closed ~= item[3] then
    return 0
  end
end
return 1
"""
_CLAIM_ROTATION_CLOSE = """
-- ROTATION_CLOSE_CLAIM_V1
if redis.call('TTL', KEYS[1]) ~= -1 or redis.call('HGET', KEYS[1], ARGV[1]) ~= ARGV[2] then return 0 end
local close_ttl = redis.call('TTL', KEYS[2])
if close_ttl ~= -1 and close_ttl ~= -2 then return 0 end
local history = cjson.decode(ARGV[4])
local closed_count = 0
for _, item in ipairs(history) do if item[3] ~= false then closed_count = closed_count + 1 end end
if redis.call('HLEN', KEYS[1]) ~= #history or redis.call('HLEN', KEYS[2]) ~= closed_count then return 0 end
for _, item in ipairs(history) do
  if redis.call('HGET', KEYS[1], item[1]) ~= item[2] then return 0 end
  local closed = redis.call('HGET', KEYS[2], item[1])
  if item[3] == false then
    if closed then return 0 end
  elseif close_ttl ~= -1 or closed ~= item[3] then return 0 end
end
if redis.call('HGET', KEYS[2], ARGV[1]) then return 0 end
redis.call('HSET', KEYS[2], ARGV[1], ARGV[3])
return 1
"""
_SETTLE_ROTATION_CLOSE = """
-- ROTATION_CLOSE_SETTLE_V1
if redis.call('TTL', KEYS[1]) ~= -1 or redis.call('TTL', KEYS[2]) ~= -1 then return 0 end
if redis.call('HGET', KEYS[1], ARGV[1]) ~= ARGV[2] or redis.call('HGET', KEYS[2], ARGV[1]) ~= ARGV[3] then return 0 end
redis.call('HSET', KEYS[2], ARGV[1], ARGV[4])
return 1
"""
_CLAIM_ASYNC_WRITE = """
local prior = redis.call('HGET', KEYS[1], ARGV[1])
if prior then return {0, prior} end
local prior_rid = redis.call('HGET', KEYS[2], ARGV[4])
if prior_rid then return {-2, prior_rid} end
if redis.call('HLEN', KEYS[1]) >= tonumber(ARGV[2]) then return {-1, ''} end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[3])
redis.call('HSET', KEYS[2], ARGV[4], ARGV[1])
return {1, ARGV[3]}
"""
_SETTLE_WRITE = """
if redis.call('HGET', KEYS[1], ARGV[1]) ~= ARGV[2] then return 0 end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[3])
return 1
"""
_RECONCILE_ASYNC_WRITE = """
if redis.call('HGET', KEYS[2], ARGV[4]) ~= ARGV[1] then return 0 end
if redis.call('HGET', KEYS[1], ARGV[1]) ~= ARGV[2] then return 0 end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[3])
return 1
"""
_REPLY_TEMPLATES = {
    "reply_key": "bigqmt:rpc:resp:{account_id}:{request_id}",
    "reply_list": "bigqmt:rpc:respq:{account_id}:{request_id}",
    "reply_channel": "bigqmt:rpc:resp:{account_id}:{request_id}",
}


def validate_execution_cursor_guard_target(target, account_id, environment):
    """新故障合同只在显式绑定的两个 VM 实例开启。"""
    if target is None:
        return None
    if (not isinstance(target, dict) or set(target) != {"gateway_id", "profile_id"}
            or target.get("gateway_id") != "qmt-vm-001"
            or (target.get("profile_id"), account_id, environment) not in (
                ("live-stock", "8882152350", "LIVE"), ("sim-bigqmt", "55013746", "SIM"))):
        raise ValueError("execution cursor guard requires an exact authorized VM target")
    return dict(target)


class SupervisorWriteGateError(RuntimeError):
    def __init__(self, reason, *, permanent):
        self.reason = reason
        self.permanent = permanent
        super().__init__(reason)


def _validate_supervisor_handle_metadata(metadata):
    expected = (("Event", 0x00100000), ("Event", 0x00100000),
                ("Mutant", 0x00100001), ("Process", 0x00101000), ("Section", 0x00000004))
    if tuple(metadata) != expected:
        raise SupervisorWriteGateError("SUPERVISOR_HANDLE_TYPE_OR_ACCESS_INVALID", permanent=True)


def _inspect_supervisor_handle_metadata(handles):
    """在等待句柄前查询内核对象类型与实际 GrantedAccess。"""
    class Basic(ctypes.Structure):
        _fields_ = [("attributes", ctypes.c_uint32), ("access", ctypes.c_uint32),
                    ("handle_count", ctypes.c_uint32), ("pointer_count", ctypes.c_uint32),
                    ("reserved", ctypes.c_uint32 * 10)]

    class UnicodeString(ctypes.Structure):
        _fields_ = [("length", ctypes.c_uint16), ("maximum_length", ctypes.c_uint16), ("buffer", ctypes.c_void_p)]

    native = ctypes.WinDLL("ntdll", use_last_error=True)
    query = native.NtQueryObject
    query.argtypes = [ctypes.c_void_p, ctypes.c_uint32, ctypes.c_void_p, ctypes.c_uint32,
                      ctypes.POINTER(ctypes.c_uint32)]
    query.restype = ctypes.c_int32
    result = []
    for handle in handles:
        basic, kind, returned = Basic(), ctypes.create_string_buffer(16384), ctypes.c_uint32()
        if (query(handle, 0, ctypes.byref(basic), ctypes.sizeof(basic), ctypes.byref(returned)) != 0
                or query(handle, 2, kind, len(kind), ctypes.byref(returned)) != 0):
            raise SupervisorWriteGateError("SUPERVISOR_HANDLE_INSPECTION_FAILED", permanent=True)
        access = basic.access
        name = UnicodeString.from_buffer(kind)
        start, end = ctypes.addressof(kind), ctypes.addressof(kind) + len(kind)
        if (not name.buffer or name.length == 0 or name.length % 2 or name.length > name.maximum_length
                or name.buffer < start or name.buffer + name.length > end):
            raise SupervisorWriteGateError("SUPERVISOR_HANDLE_INSPECTION_FAILED", permanent=True)
        result.append((ctypes.wstring_at(name.buffer, name.length // 2), access))
    return result


class InheritedSupervisorWriteGate:
    """受限继承的监督进程、单调停写事件、身份事件和跨进程互斥。"""

    _FIELDS = ("AQP_WRITE_BLOCK_EVENT_HANDLE", "AQP_IDENTITY_READY_EVENT_HANDLE",
               "AQP_WRITE_GATE_MUTEX_HANDLE", "AQP_SUPERVISOR_PROCESS_HANDLE", "AQP_IDENTITY_EPOCH_MAPPING_HANDLE")
    _WAIT_OBJECT_0, _WAIT_ABANDONED, _WAIT_TIMEOUT = 0, 0x80, 0x102

    def __init__(self, handles, kernel, epoch_view=None):
        self._handles, self._kernel, self._held, self._closed = tuple(handles), kernel, False, False
        self._epoch_view = epoch_view

    @classmethod
    def from_environment(cls):
        if os.name != "nt":
            raise SupervisorWriteGateError("SUPERVISOR_GATE_UNAVAILABLE", permanent=True)
        raw = [os.environ.get(key) for key in cls._FIELDS]
        if any(not isinstance(value, str) or not value or any(char < "0" or char > "9" for char in value) for value in raw):
            raise SupervisorWriteGateError("SUPERVISOR_GATE_UNAVAILABLE", permanent=True)
        handles = [int(value) for value in raw]
        if any(value <= 0 or value >= (1 << (8 * ctypes.sizeof(ctypes.c_void_p))) for value in handles) or len(set(handles)) != 5:
            raise SupervisorWriteGateError("SUPERVISOR_GATE_UNAVAILABLE", permanent=True)
        _validate_supervisor_handle_metadata(_inspect_supervisor_handle_metadata(handles))
        kernel = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel.WaitForSingleObject.argtypes = [ctypes.c_void_p, ctypes.c_uint32]
        kernel.WaitForSingleObject.restype = ctypes.c_uint32
        kernel.ReleaseMutex.argtypes = [ctypes.c_void_p]
        kernel.ReleaseMutex.restype = ctypes.c_int
        kernel.GetProcessId.argtypes = [ctypes.c_void_p]
        kernel.GetProcessId.restype = ctypes.c_uint32
        kernel.CloseHandle.argtypes = [ctypes.c_void_p]
        kernel.CloseHandle.restype = ctypes.c_int
        kernel.MapViewOfFile.argtypes = [ctypes.c_void_p, ctypes.c_uint32, ctypes.c_uint32, ctypes.c_uint32, ctypes.c_size_t]
        kernel.MapViewOfFile.restype = ctypes.c_void_p
        kernel.UnmapViewOfFile.argtypes = [ctypes.c_void_p]
        kernel.UnmapViewOfFile.restype = ctypes.c_int
        if kernel.GetProcessId(handles[3]) != os.getppid():
            raise SupervisorWriteGateError("SUPERVISOR_PARENT_UNVERIFIED", permanent=True)
        epoch_view = kernel.MapViewOfFile(handles[4], 0x0004, 0, 0, ctypes.sizeof(ctypes.c_uint64))
        if not epoch_view:
            raise SupervisorWriteGateError("SUPERVISOR_EPOCH_UNAVAILABLE", permanent=True)
        gate = cls(handles, kernel, epoch_view)
        try:
            gate.check()
        except SupervisorWriteGateError as exc:
            if exc.permanent:
                gate.close()
                raise
        return gate

    def _wait(self, handle):
        if self._closed:
            raise SupervisorWriteGateError("SUPERVISOR_GATE_UNAVAILABLE", permanent=True)
        return self._kernel.WaitForSingleObject(handle, 0)

    def check(self):
        blocked, ready, _, parent, _ = self._handles
        parent_state = self._wait(parent)
        if parent_state != self._WAIT_TIMEOUT:
            raise SupervisorWriteGateError("SUPERVISOR_PARENT_UNAVAILABLE", permanent=True)
        block_state = self._wait(blocked)
        if block_state == self._WAIT_OBJECT_0:
            raise SupervisorWriteGateError("SUPERVISOR_WRITE_BLOCKED", permanent=True)
        if block_state != self._WAIT_TIMEOUT:
            raise SupervisorWriteGateError("SUPERVISOR_GATE_UNAVAILABLE", permanent=True)
        ready_state = self._wait(ready)
        if ready_state == self._WAIT_TIMEOUT:
            raise SupervisorWriteGateError("SUPERVISOR_IDENTITY_NOT_READY", permanent=False)
        if ready_state != self._WAIT_OBJECT_0:
            raise SupervisorWriteGateError("SUPERVISOR_GATE_UNAVAILABLE", permanent=True)

    def acquire(self):
        if self._held:
            raise SupervisorWriteGateError("SUPERVISOR_GATE_REENTRANT", permanent=True)
        state = self._wait(self._handles[2])
        if state == self._WAIT_TIMEOUT:
            raise SupervisorWriteGateError("SUPERVISOR_MUTEX_BUSY", permanent=False)
        if state == self._WAIT_ABANDONED:
            self._kernel.ReleaseMutex(self._handles[2])
            raise SupervisorWriteGateError("SUPERVISOR_MUTEX_ABANDONED", permanent=True)
        if state != self._WAIT_OBJECT_0:
            raise SupervisorWriteGateError("SUPERVISOR_GATE_UNAVAILABLE", permanent=True)
        self._held = True

    def release(self):
        if not self._held:
            raise SupervisorWriteGateError("SUPERVISOR_GATE_NOT_HELD", permanent=True)
        self._held = False
        if not self._kernel.ReleaseMutex(self._handles[2]):
            raise SupervisorWriteGateError("SUPERVISOR_GATE_RELEASE_FAILED", permanent=True)

    def read_epoch(self):
        if not self._held or not self._epoch_view:
            raise SupervisorWriteGateError("SUPERVISOR_EPOCH_UNAVAILABLE", permanent=True)
        value = ctypes.c_uint64.from_address(self._epoch_view).value
        if value <= 0:
            raise SupervisorWriteGateError("SUPERVISOR_EPOCH_UNAVAILABLE", permanent=True)
        return value

    def close(self):
        if self._closed:
            return
        if self._held:
            self.release()
        self._closed = True
        unmapped = not self._epoch_view or bool(self._kernel.UnmapViewOfFile(self._epoch_view))
        closed = [self._kernel.CloseHandle(handle) for handle in self._handles]
        if not unmapped or not all(closed):
            raise SupervisorWriteGateError("SUPERVISOR_GATE_CLOSE_FAILED", permanent=True)


def _windows_native_process_identity(pid):
    """独立读取当前 Windows 原生进程出生时间、路径与 Session。"""
    if os.name != "nt" or type(pid) is not int or pid <= 0:
        raise ShmProtocolError("Windows native process identity is unavailable")

    class FileTime(ctypes.Structure):
        _fields_ = [("low", ctypes.c_uint32), ("high", ctypes.c_uint32)]

    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.OpenProcess.argtypes = [ctypes.c_uint32, ctypes.c_int, ctypes.c_uint32]
    kernel.OpenProcess.restype = ctypes.c_void_p
    kernel.GetProcessTimes.argtypes = [ctypes.c_void_p, ctypes.POINTER(FileTime), ctypes.POINTER(FileTime),
                                      ctypes.POINTER(FileTime), ctypes.POINTER(FileTime)]
    kernel.GetProcessTimes.restype = ctypes.c_int
    kernel.GetExitCodeProcess.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_uint32)]
    kernel.GetExitCodeProcess.restype = ctypes.c_int
    kernel.QueryFullProcessImageNameW.argtypes = [ctypes.c_void_p, ctypes.c_uint32, ctypes.c_wchar_p,
                                                 ctypes.POINTER(ctypes.c_uint32)]
    kernel.QueryFullProcessImageNameW.restype = ctypes.c_int
    kernel.ProcessIdToSessionId.argtypes = [ctypes.c_uint32, ctypes.POINTER(ctypes.c_uint32)]
    kernel.ProcessIdToSessionId.restype = ctypes.c_int
    kernel.CloseHandle.argtypes = [ctypes.c_void_p]
    handle = kernel.OpenProcess(0x1000, False, pid)
    if not handle:
        raise ShmProtocolError("native process cannot be opened for identity verification")
    try:
        created, exited, kernel_time, user_time = FileTime(), FileTime(), FileTime(), FileTime()
        exit_code, session = ctypes.c_uint32(), ctypes.c_uint32()
        path, size = ctypes.create_unicode_buffer(32768), ctypes.c_uint32(32768)
        if (not kernel.GetProcessTimes(handle, ctypes.byref(created), ctypes.byref(exited),
                                       ctypes.byref(kernel_time), ctypes.byref(user_time))
                or not kernel.GetExitCodeProcess(handle, ctypes.byref(exit_code)) or exit_code.value != 259
                or not kernel.QueryFullProcessImageNameW(handle, 0, path, ctypes.byref(size))
                or not kernel.ProcessIdToSessionId(pid, ctypes.byref(session)) or not path.value):
            raise ShmProtocolError("native process identity cannot be fully verified")
        return dict(native_pid=pid, native_creation_filetime=(created.high << 32) | created.low,
                    windows_session_id=session.value, executable=os.path.normcase(path.value))
    finally:
        kernel.CloseHandle(handle)


def validate_account_policy(account_id, environment, runtime_policy):
    """显式实盘合同不能由账户或环境单一字段推断。"""
    if environment == "LIVE":
        if (not isinstance(account_id, str) or not re.fullmatch(r"[0-9]{1,32}", account_id)
                or account_id == "55013746" or runtime_policy != "PERSISTENT_LIVE_READ_WRITE"):
            raise ValueError("LIVE requires a precise numeric non-simulation account and PERSISTENT_LIVE_READ_WRITE")
    elif environment != "SIM" or account_id != "55013746" or runtime_policy == "PERSISTENT_LIVE_READ_WRITE":
        raise ValueError("simulation account 55013746 and matching SIM policy are required")


class RedisShmReadOnlyProxy:
    """兼容原类名；写入须显式账户政策且持久留痕，不自动恢复或重发。"""

    def __init__(self, redis_transport, readonly_client, execution_client, *,
                 account_id="55013746", expected_readonly_session_id,
                 expected_execution_session_id, expected_native_pid,
                 timeout_seconds=5.0, max_requests=1000, deadline_unix=None,
                 runtime_policy="BOUNDED", allow_order_methods=False,
                 read_cache_capacity=1000, write_ledger_capacity=1000,
                 expected_windows_session_id=None, environment="SIM", start_write_blocked=None,
                 live_write_authorized=False, receive_policy="QUEUE_AND_PUBSUB",
                 execution_cursor_guard_target=None, supervisor_write_gate=None, supervisor_rotation_channel=None):
        validate_account_policy(account_id, environment, runtime_policy)
        execution_cursor_guard_target = validate_execution_cursor_guard_target(execution_cursor_guard_target, account_id, environment)
        if not isinstance(receive_policy, str) or receive_policy not in RECEIVE_POLICIES:
            raise ValueError("explicit supported receive_policy required")
        if redis_transport.account_id != account_id:
            raise ValueError("transport account must match the explicit account")
        if redis_transport.listen_redis is not redis_transport.redis:
            # 原 transport 对两池中的单方写失败内部容错，本候选要求任一
            # fanout 阶段失败可见，避免未确认的部分发布被包装成成功。
            raise ValueError("one injected Redis client must own receive and response fanout")
        if (redis_transport.request_queue != "bigqmt:rpc:queue:%s" % account_id
                or redis_transport.request_channel != "bigqmt:rpc:req:%s" % account_id):
            raise ValueError("candidate requires the standard simulation request queue and channel or exact LIVE account queue and channel")
        if (not isinstance(expected_readonly_session_id, str) or not expected_readonly_session_id
                or not isinstance(expected_execution_session_id, str) or not expected_execution_session_id
                or expected_readonly_session_id == expected_execution_session_id
                or type(expected_native_pid) is not int or expected_native_pid <= 0):
            raise ValueError("two distinct fixed sessions and a native PID are required")
        if type(timeout_seconds) not in (int, float) or not math.isfinite(timeout_seconds) or not 0 < timeout_seconds <= 60:
            raise ValueError("timeout must be within (0, 60] seconds")
        if runtime_policy not in RUNTIME_POLICIES:
            raise ValueError("explicit supported runtime policy required")
        if type(allow_order_methods) is not bool or allow_order_methods is not (runtime_policy in WRITE_POLICIES):
            raise ValueError("write permission must explicitly match the account read/write policy")
        if start_write_blocked is not None and type(start_write_blocked) is not bool:
            raise ValueError("start_write_blocked must be a boolean")
        if type(live_write_authorized) is not bool or (environment != "LIVE" and live_write_authorized):
            raise ValueError("live_write_authorized must be an explicit LIVE-only boolean")
        if environment == "LIVE":
            if start_write_blocked is None:
                start_write_blocked = True
            if not start_write_blocked and not live_write_authorized:
                raise ValueError("LIVE start_write_blocked=false requires explicit live_write_authorized=true")
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
        self.environment = environment
        self.live_write_authorized = live_write_authorized
        self.receive_policy = receive_policy
        self.execution_cursor_guard_target = copy.deepcopy(execution_cursor_guard_target)
        self._supervisor_write_gate = supervisor_write_gate
        self._supervisor_rotation_channel = supervisor_rotation_channel
        self._write_ledger_key = "aqp:redis_shm:write_ledger:%s:v1" % account_id
        self._async_cid_key = self._write_ledger_key + ":cid"
        self._read_rotation_key = "aqp:redis_shm:read_rotation:%s:v1" % account_id
        self._read_rotation_close_key = "aqp:redis_shm:read_rotation_close:%s:v1" % account_id
        self._supervisor_attach_key = "aqp:redis_shm:supervisor_attach:%s:v1" % account_id
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
        if environment == "LIVE" and expected_windows_session_id is None:
            raise ValueError("LIVE requires an explicit expected Windows session")
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
        self._unconfirmed_read_ids = {"readonly": set(), "execution": set()}
        self._processed_count = 0
        self._source_identity = {}
        self._verified_process_identity = None
        # 一路故障不得把另一路伪装成故障；故障证据只增不清，旧客户端保留。
        self._route_faults = {"readonly": None, "execution": None}
        self._read_isolation_supported = False
        self._execution_cursor_gate_supported = False
        self._native_supervisor_gate_supported = False
        self._supervisor_attach_attempted = False
        self._supervisor_attach_receipt = None
        self._supervisor_attach_verified = False
        self._native_read_state = "UNKNOWN"
        self._last_read_success_at = None
        self._last_execution_success_at = None
        self._last_execution_identity_success_at = None
        self._last_native_read_observation_at = None
        self._capabilities = frozenset()
        self._writes_blocked = False
        self._write_block_reason = ""
        self._source_write_block_reason = ""
        self._write_block_reasons = []
        self._read_rotation_blocked = False
        self._rotation_integrity_fault = False
        self._rotation_registration_attempted = set()
        self._rotation_control_attempted = set()
        self._rotation_capability_attempted = False
        self._rotation_capability = None
        self._read_rotation_capacity_evidence = None
        self._read_rotation_candidate = None
        self._read_rotation_stage = None
        self._rotation_committed_record = None
        self._rotation_activated_record = None
        self._rotation_waiting_gateway = False
        self._gateway_acceptance_attempted = False
        self._gateway_acceptance_proof = None
        self._rotation_open_prepare_attempted = False
        self._rotation_open_prepared = False
        self._rotation_open_delivery = "NOT_SENT"
        self._rotation_open_acknowledged = False
        self._accepted_rotation_record = None
        self._accepted_rotation_records = {}
        self._rotation_closed_records = {}
        self._rotation_close_pending_records = {}
        self._rotation_close_ready_attempted = False
        self._rotation_close_ready_proof = None
        self._rotation_close_claim_attempted = False
        self._rotation_close_pending_raw = None
        self._rotation_close_record = None
        self._rotation_close_native_receipt = None
        self._rotation_close_settle_attempted = False
        self._rotation_close_closed_raw = None
        self._rotation_close_commit_attempted = False
        self._rotation_close_committed = False
        self._rotation_close_ack_attempted = False
        self._rotation_close_acknowledged = False
        self._rotation_close_next_ready_attempted = False
        self._rotation_close_next_ready_proof = None
        self._retired_read_clients = []
        self._retired_read_faults = []
        self._retired_read_ids = set()
        self._operator_write_blocked = False
        if start_write_blocked:
            self._block_writes("OPERATOR_WRITE_DISABLED")
        if self.execution_cursor_guard_target is not None and self.allow_order_methods and supervisor_write_gate is None:
            self._block_writes("SUPERVISOR_GATE_UNAVAILABLE")
        self._write_record_count = 0
        self._operation_phase = "INITIALIZED"
        self._request_context = {}
        self._last_error = {}
        self._first_error = {}
        self._read_publish_failures = 0
        self._redis_retry_at = 0.0
        self._idle_receive_timeouts = 0
        self._consecutive_idle_timeouts = 0

    def _record_error(self, exc):
        self._last_error = dict(error_phase=self._operation_phase,
            error_type=type(exc).__name__, error_message=str(exc)[:2048],
            error_at_unix=time.time(), **self._request_context)
        if not self._first_error:
            self._first_error = copy.deepcopy(self._last_error)

    @staticmethod
    def _read_timing(value, event_poll=False):
        if not isinstance(value, dict):
            return {}
        fields = EVENT_POLL_TIMING_FIELDS if event_poll else POSITION_READ_TIMING_FIELDS
        return {key: value[key] for key in fields
                if type(value.get(key)) in (int, float) and 0 < value[key] < 1e12}

    def diagnostic_snapshot(self):
        """有界运行证据，不包含请求参数、Redis凭据或任何重发入口。"""
        result = dict(operation_phase=self._operation_phase, last_error=copy.deepcopy(self._last_error),
                      first_error=copy.deepcopy(self._first_error), read_publish_failures=self._read_publish_failures,
                      idle_receive_timeouts=self._idle_receive_timeouts,
                      consecutive_idle_timeouts=self._consecutive_idle_timeouts)
        if self.execution_cursor_guard_target is not None:
            result["source_faults"] = copy.deepcopy(self._route_faults)
        return result

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

    def _position_read_route_supported(self):
        read = self._source_identity.get("readonly", {})
        capabilities = read.get("capabilities")
        return (self.execution_cursor_guard_target == {"gateway_id": "qmt-vm-001", "profile_id": "live-stock"}
                and self.account_id == "8882152350" and self.environment == "LIVE"
                and self._read_isolation_supported and isinstance(capabilities, list)
                and "get_positions" in capabilities)

    @staticmethod
    def _request_route(request):
        method = request.get("method")
        if (method in QUOTE_METHODS | MARKET_RPC_METHODS
                or (method == "get_positions"
                    and request.get("position_read_route_contract") == POSITION_READ_ROUTE_CONTRACT)):
            return "readonly"
        return "execution"

    def _request_fingerprint(self, request):
        values = [request.get("schema_version"), self.account_id, request.get("method"), request.get("params")]
        if "position_read_route_contract" in request:
            values.append(request["position_read_route_contract"])
        return json.dumps(values, sort_keys=True, allow_nan=False, separators=(",", ":"))

    def identity_snapshot(self):
        """仅返回实际双源握手证据，未握手不能声明 READY。"""
        if not self._verified:
            raise ShmProtocolError("explicit identity handshake required")
        result = dict(pong=True, transport="redis_shm", rpc_revision="external-redis-dual-shm-v1",
                    account_id=self.account_id, environment=self.environment, session_id=self._sessions["execution"],
                    native_pid=self.expected_native_pid, allow_order_methods=self.allow_order_methods,
                    runtime_policy=self.runtime_policy, capabilities=sorted(self._capabilities),
                    receive_policy=self.receive_policy,
                    writes_blocked=self._writes_blocked, write_block_reason=self._write_block_reason,
                    write_ready=self.allow_order_methods and not self._writes_blocked,
                    read_source=copy.deepcopy(self._source_identity["readonly"]),
                    execution_source=copy.deepcopy(self._source_identity["execution"]))
        if self.environment == "LIVE":
            result["live_write_authorized"] = self.live_write_authorized
        if self.execution_cursor_guard_target is not None:
            result["execution_cursor_guard_target"] = copy.deepcopy(self.execution_cursor_guard_target)
            result.update(shm_publication_evidence_contract="SHM_PUBLICATION_STATE_V1",
                          read_source_state=self._route_state("readonly"),
                          execution_source_state=self._route_state("execution"),
                          write_block_reasons=list(self._write_block_reasons),
                          read_source_fault=copy.deepcopy(self._route_faults["readonly"]),
                          execution_source_fault=copy.deepcopy(self._route_faults["execution"]))
            if self._read_rotation_capacity_evidence is not None:
                result["read_rotation_capacity_evidence"] = copy.deepcopy(self._read_rotation_capacity_evidence)
        if self.execution_cursor_guard_target is not None and self._read_isolation_supported:
            result["read_fault_isolation_contract"] = READ_FAULT_ISOLATION_CONTRACT
        if self._position_read_route_supported():
            result["position_read_route_contract"] = POSITION_READ_ROUTE_CONTRACT
        if self.execution_cursor_guard_target is not None and self._execution_cursor_gate_supported:
            result["execution_event_guard_contract"] = EXECUTION_CURSOR_WRITE_GATE_CONTRACT
        if (self.execution_cursor_guard_target is not None and self._supervisor_write_gate is not None
                and self._native_supervisor_gate_supported):
            result["supervisor_write_gate_contract"] = SUPERVISOR_WRITE_GATE_CONTRACT
            result["supervisor_gate_epoch_contract"] = SUPERVISOR_GATE_EPOCH_CONTRACT
            if self.allow_order_methods and not self._writes_blocked:
                try:
                    self._check_supervisor_write_gate()
                except SupervisorWriteGateError as exc:
                    if exc.permanent:
                        self._block_writes(exc.reason)
                    result.update(writes_blocked=True, write_ready=False, write_block_reason=exc.reason)
        if self._verified_process_identity is not None:
            result["native_creation_filetime"] = self._verified_process_identity["native_creation_filetime"]
        if ASYNC_WRITE_METHODS <= self._capabilities:
            result["async_submit_contract"] = ASYNC_SUBMIT_CONTRACT
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
        required = (request.get("method") in ALL_WRITE_METHODS
                    or (self.execution_cursor_guard_target is not None and bool(self._retired_read_clients)))
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

    @staticmethod
    def _is_source_block_reason(reason):
        return ((reason in ("READ_SOURCE_UNTRUSTED", "EXECUTION_SOURCE_UNTRUSTED", "EXECUTION_READ_RESULT_UNKNOWN",
                       "READ_ROTATION_UNCONFIRMED", "EXECUTION_CURSOR_GATE_UNAVAILABLE", "EXECUTION_EVENT_GAP", "EXECUTION_EVENT_STATE_UNKNOWN",
                       "SUPERVISOR_GATE_UNAVAILABLE", "NATIVE_SUPERVISOR_GATE_UNAVAILABLE",
                       "SUPERVISOR_WRITE_BLOCKED", "SUPERVISOR_PARENT_UNAVAILABLE", "SUPERVISOR_MUTEX_ABANDONED",
                       "SUPERVISOR_GATE_RELEASE_FAILED", "SUPERVISOR_PARENT_UNVERIFIED")
             or reason.startswith("SUPERVISOR_") and reason not in ("SUPERVISOR_IDENTITY_NOT_READY", "SUPERVISOR_MUTEX_BUSY")))

    def _block_writes(self, reason):
        if reason == "OPERATOR_WRITE_DISABLED":
            self._operator_write_blocked = True
        if reason not in self._write_block_reasons:
            self._write_block_reasons.append(reason)
        if self._is_source_block_reason(reason) and not self._source_write_block_reason:
            self._source_write_block_reason = reason
        self._writes_blocked = True
        self._write_block_reason = self._write_block_reason or reason

    def _remove_resolved_write_block_reasons(self, reasons):
        """仅移除已由独立证据解决的具体原因，绝不重置其余门。"""
        self._write_block_reasons = [item for item in self._write_block_reasons if item not in reasons]
        self._writes_blocked = bool(self._write_block_reasons)
        self._write_block_reason = self._write_block_reasons[0] if self._write_block_reasons else ""
        self._source_write_block_reason = next((item for item in self._write_block_reasons
                                                if self._is_source_block_reason(item)), "")

    def _check_supervisor_write_gate(self, expected_epoch=None):
        try:
            self._supervisor_write_gate.check()
            if expected_epoch is not None and self._supervisor_write_gate.read_epoch() != expected_epoch:
                raise SupervisorWriteGateError("SUPERVISOR_EPOCH_NOT_CURRENT", permanent=False)
        except SupervisorWriteGateError:
            raise
        except Exception as exc:
            raise SupervisorWriteGateError("SUPERVISOR_GATE_UNAVAILABLE", permanent=True) from exc

    def _acquire_supervisor_write_gate(self):
        try:
            self._supervisor_write_gate.acquire()
        except SupervisorWriteGateError:
            raise
        except Exception as exc:
            raise SupervisorWriteGateError("SUPERVISOR_GATE_UNAVAILABLE", permanent=True) from exc

    def _route_state(self, route):
        fault = self._route_faults[route]
        if fault is not None:
            return fault["state"]
        now = time.monotonic()
        if route == "readonly":
            observations = (self._last_read_success_at, self._last_native_read_observation_at)
            if not self._read_isolation_supported or self._native_read_state != "UNKNOWN":
                return "UNKNOWN"
        else:
            observations = (self._last_execution_success_at,)
        if any(value is None or not 0 <= now - value <= SOURCE_OBSERVATION_TTL_SECONDS for value in observations):
            return "UNKNOWN"
        return "HEALTHY"

    def _observe_native_read_state(self, data, request):
        """只接受经双源握手及执行响应绑定的原生只读故障观测。"""
        read = self._source_identity.get("readonly", {})
        contract = data.get("read_fault_isolation_contract")
        fields = ("native_read_source_state", "native_read_session_id")
        if contract is None and not any(key in data for key in fields):
            self._read_isolation_supported = False
            self._native_read_state = "UNKNOWN"
            self._last_native_read_observation_at = None
            return
        if (contract != READ_FAULT_ISOLATION_CONTRACT
                or data.get("native_read_source_state") not in ("UNKNOWN", "RESULT_UNKNOWN")):
            raise ShmProtocolError("native readonly fault observation contract is invalid")
        expected_read_sid = self._sessions["readonly"]
        if (self._read_rotation_stage in ("COMMITTED", "SUPERVISOR_VERIFIED", "ACTIVATE_PENDING")
                and self._read_rotation_candidate is not None
                and data.get("native_read_session_id") == self._read_rotation_candidate.mailbox.session_id):
            # 发布与监督验收之间保留旧代理绑定，不能将候选伪装成已接管。
            self._read_isolation_supported = False
            self._native_read_state = "UNKNOWN"
            self._last_native_read_observation_at = None
            return
        if data.get("native_read_session_id") != expected_read_sid:
            raise ShmProtocolError("native readonly fault observation identity is invalid")
        if (read.get("read_fault_isolation_contract") != READ_FAULT_ISOLATION_CONTRACT
                or read.get("environment") != self.environment or data.get("environment") != self.environment
                or self._source_binding() is None):
            self._read_isolation_supported = False
            self._native_read_state = "UNKNOWN"
            self._last_native_read_observation_at = None
            return
        self._read_isolation_supported = True
        self._native_read_state = data["native_read_source_state"]
        self._last_native_read_observation_at = time.monotonic()
        if self._native_read_state == "RESULT_UNKNOWN":
            self._mark_route_fault("readonly", "RESULT_UNKNOWN", request, "UNKNOWN", previous_unconfirmed=None)

    def _capture_native_process_identity(self):
        """旧版或无法独立读取进程身份时允许普通查询，但不允许换代。"""
        read = self._source_identity.get("readonly", {})
        execution = self._source_identity.get("execution", {})
        birth = execution.get("native_creation_filetime")
        if (type(birth) is not int or birth <= 0 or read.get("native_creation_filetime") != birth
                or self._source_binding() is None):
            return
        try:
            actual = _windows_native_process_identity(self.expected_native_pid)
        except Exception:
            return
        if (actual["native_creation_filetime"] == birth
                and actual["windows_session_id"] == self._source_binding()["windows_session_id"]):
            self._verified_process_identity = actual

    def _mark_route_fault(self, route, state, request, publication_state, *,
                          sequence=None, previous_unconfirmed=False):
        """保留第一条来源故障；不更换 SID、改写旧帧或解除交易门。"""
        if self._route_faults[route] is None:
            fault = dict(state=state, session_id=self._sessions[route],
                         trigger_request_id=request.get("request_id"),
                         publication_state=publication_state, publication_scope="SHM",
                         previous_unconfirmed=previous_unconfirmed,
                         automatic_retry=False)
            if type(sequence) is int and sequence > 0:
                fault["sequence"] = sequence
            self._route_faults[route] = fault
        self._block_writes("READ_SOURCE_UNTRUSTED" if route == "readonly" else "EXECUTION_SOURCE_UNTRUSTED")

    def _readonly_callback_finished(self, *, mailbox_locked=False):
        """旧帧与同源短时 Python 回调返回见证同时成立；不确认或重发旧结果。"""
        fault = self._route_faults["readonly"]
        if fault is None:
            return True
        if not isinstance(fault, dict) or fault.get("state") != "RESULT_UNKNOWN":
            return False
        sequence, rid = fault.get("sequence"), fault.get("trigger_request_id")
        if (fault.get("publication_state") != "UNKNOWN" or fault.get("previous_unconfirmed") is not False
                or fault.get("session_id") != self._sessions["readonly"]
                or type(sequence) is not int or sequence <= 0 or not isinstance(rid, str)):
            return False
        read_identity = self._source_identity.get("readonly", {})
        execution_identity = self._source_identity.get("execution", {})
        binding = self._source_binding()
        native = self._verified_process_identity
        if (not self._read_isolation_supported or binding is None
                or read_identity.get("read_py_callback_returned_contract") != READ_PY_CALLBACK_RETURNED_CONTRACT
                or execution_identity.get("read_py_callback_returned_contract") != READ_PY_CALLBACK_RETURNED_CONTRACT
                or read_identity.get("session_id") != binding["read_session_id"]
                or execution_identity.get("session_id") != binding["execution_session_id"]
                or not isinstance(native, dict) or native.get("native_pid") != binding["native_pid"]
                or native.get("windows_session_id") != binding["windows_session_id"]
                or type(native.get("native_creation_filetime")) is not int
                or native["native_creation_filetime"] <= 0
                or read_identity.get("native_creation_filetime") != native["native_creation_filetime"]
                or execution_identity.get("native_creation_filetime") != native["native_creation_filetime"]):
            return False
        try:
            if _windows_native_process_identity(binding["native_pid"]) != native:
                return False
            execution_mailbox = self._clients["execution"].mailbox
            if (execution_mailbox.closed or execution_mailbox.session_id != binding["execution_session_id"]
                    or execution_mailbox.server_pid != binding["native_pid"]
                    or not execution_mailbox.region.acquire(0)):
                return False
            try:
                execution_header = execution_mailbox._header()
                if execution_header[2] != READY:
                    return False
            finally:
                execution_mailbox.region.release()
        except Exception:
            return False
        mailbox = self._clients["readonly"].mailbox
        region = getattr(mailbox, "region", None)
        if (mailbox.closed or region is None or mailbox.session_id != binding["read_session_id"]
                or mailbox.server_pid != binding["native_pid"]):
            return False
        if not mailbox_locked:
            try:
                acquired = region.acquire(0)
            except Exception:
                return False
            if not acquired:
                return False
        try:
            header = mailbox._header()
            if (header[2] != READY or header[4:6] != [sequence, sequence]
                    or header[6] != sequence - 1
                    or not 0 < header[7] <= REQUEST_CAPACITY or not 0 < header[8] <= RESPONSE_CAPACITY):
                return False
            witnessed_sequence, witnessed_at = _READ_CALLBACK_WITNESS.unpack(
                region.read(_READ_CALLBACK_WITNESS_OFFSET, _READ_CALLBACK_WITNESS.size))
            age = time.time() - witnessed_at
            if (witnessed_sequence != sequence or witnessed_at <= 0
                    or not 0 <= age <= READ_PY_CALLBACK_WITNESS_TTL_SECONDS):
                return False
            request = decode_payload(region.read(REQUEST_OFFSET, header[7]))
            response = decode_payload(region.read(RESPONSE_OFFSET, header[8]))
            prior = self._replays.get(rid)
            if (not isinstance(request, dict) or not isinstance(response, dict)
                    or request.get("request_id") != rid or request.get("account_id") != self.account_id
                    or request.get("session_id") != self._sessions["readonly"]
                    or request.get("sequence") != sequence or response.get("sequence") != sequence
                    or not prior or prior[1].get("status") != "UNKNOWN"
                    or self._request_fingerprint(request) != prior[0]):
                return False
            self._check_response(request, response, self._sessions["readonly"])
            return True
        except Exception:
            return False
        finally:
            if not mailbox_locked:
                try:
                    region.release()
                except Exception:
                    return False

    def _execution_event_state(self, data, request):
        """仅据同账户、同执行源和自洽游标识别回报缺口；不把无效结果当作零。"""
        binding = self._source_binding()
        after = request.get("params", {}).get("after_sequence")
        if (not isinstance(data, dict) or binding is None or type(after) is not int or after < 0
                or data.get("account_id") != self.account_id or data.get("environment") != self.environment
                or data.get("session_id") != binding["execution_session_id"]
                or type(data.get("native_pid")) is not int or data["native_pid"] != binding["native_pid"]
                or type(data.get("windows_session_id")) is not int
                or data["windows_session_id"] != binding["windows_session_id"]
                or data.get("automatic_retry") is not False or type(data.get("gap")) is not bool):
            return "UNKNOWN"
        latest, oldest, dropped, lost = (data.get(key) for key in
            ("latest_sequence", "oldest_sequence", "dropped_before_sequence", "lost_sequence"))
        if (any(type(value) is not int for value in (latest, oldest, dropped, lost))
                or not 0 <= after <= latest or not 1 <= oldest <= latest + 1
                or dropped != oldest - 1 or not 0 <= lost <= latest
                or data["gap"] is not (after < dropped or after < lost)
                or not isinstance(data.get("events"), list)):
            return "UNKNOWN"
        expected_sequence = max(after + 1, oldest)
        for item in data["events"]:
            if (not isinstance(item, dict) or type(item.get("sequence")) is not int
                    or item["sequence"] != expected_sequence or item["sequence"] > latest
                    or item.get("session_id") != binding["execution_session_id"]
                    or item.get("account_id") != self.account_id or not isinstance(item.get("event"), dict)
                    or item["event"].get("account_id") != self.account_id
                    or (item["event"].get("data_loss") is True and item["sequence"] > lost)):
                return "UNKNOWN"
            expected_sequence += 1
        if expected_sequence != latest + 1:
            return "UNKNOWN"
        return "GAP" if data["gap"] or lost > 0 else "HEALTHY"

    def _inspect_write_ledger(self):
        if not self.allow_order_methods:
            return
        records = self.transport.redis.hgetall(self._write_ledger_key)
        if not isinstance(records, dict) or len(records) > 1000:
            raise ShmProtocolError("persistent write ledger is malformed or over its fixed bound")
        self._write_record_count = len(records)
        has_async_records = False
        if len(records) >= self.write_ledger_capacity:
            self._block_writes("WRITE_LEDGER_CAPACITY_EXHAUSTED")
        for rid, encoded in records.items():
            record = json.loads(decode_text(encoded))
            if (not isinstance(record, dict) or record.get("request_id") != decode_text(rid)
                    or not isinstance(record.get("fingerprint"), str)
                    or not isinstance(record.get("binding"), dict)
                    or record["binding"].get("account_id") != self.account_id
                    or (self.environment == "LIVE" and (record["binding"].get("environment") != "LIVE"
                        or record["binding"].get("runtime_policy") != self.runtime_policy))
                    or record.get("state") not in ("CLAIMED", "CONFIRMED", "UNKNOWN", "PUBLISH_PENDING", "PUBLISH_UNCONFIRMED", "RECONCILED", "SUBMITTED_PENDING", "NOT_EXECUTED")):
                raise ShmProtocolError("persistent write ledger identity is malformed")
            try:
                fingerprint = json.loads(record["fingerprint"])
            except ValueError as exc:
                raise ShmProtocolError("persistent write fingerprint is not valid JSON") from exc
            if (not isinstance(fingerprint, list) or len(fingerprint) != 4
                    or type(fingerprint[0]) is not int or fingerprint[:2] != [1, self.account_id]
                    or not isinstance(fingerprint[2], str) or fingerprint[2] not in ALL_WRITE_METHODS
                    or not isinstance(fingerprint[3], dict)):
                raise ShmProtocolError("persistent write fingerprint identity is malformed")
            async_record = fingerprint[2] in ASYNC_WRITE_METHODS
            if async_record:
                if (fingerprint[:2] != [1, self.account_id] or type(fingerprint[0]) is not int
                        or not isinstance(fingerprint[3], dict)
                        or record.get("client_order_id") != validate_async_submit_params(fingerprint[3])):
                    raise ShmProtocolError("persistent async request fingerprint lost its client_order_id")
            elif "client_order_id" in record:
                raise ShmProtocolError("persistent async CID is attached to a non-async fingerprint")
            has_async_records = has_async_records or async_record
            if record["state"] == "RECONCILED":
                self._validate_reconciled_record(record)
            elif record["state"] == "SUBMITTED_PENDING":
                self._validate_pending_record(record)
            elif record["state"] == "NOT_EXECUTED":
                self._validate_not_executed_record(record)
            elif record["state"] != "CONFIRMED":
                self._block_writes("PERSISTED_WRITE_UNCONFIRMED")
        # CID只对新异步合同生效；旧SYNC记录不要求新增索引。
        if not ASYNC_WRITE_METHODS <= self._capabilities and not has_async_records:
            return
        index = self.transport.redis.hgetall(self._async_cid_key)
        if not isinstance(index, dict) or len(index) > self.write_ledger_capacity:
            raise ShmProtocolError("persistent async CID index is malformed or over its fixed bound")
        indexed = {decode_text(cid): decode_text(rid) for cid, rid in index.items()}
        expected = {}
        for encoded in records.values():
            record = json.loads(decode_text(encoded))
            cid = record.get("client_order_id")
            if cid is not None:
                validate_async_submit_params(dict(client_order_id=cid))
                if cid in expected:
                    raise ShmProtocolError("persistent async CID has multiple request identities")
                if record["state"] == "CONFIRMED":
                    raise ShmProtocolError("async native-call receipts cannot be persisted as broker confirmations")
                expected[cid] = record["request_id"]
        if indexed != expected:
            raise ShmProtocolError("persistent async CID index differs from the preserved write ledger")

    def _inspect_read_rotation(self):
        """只读检查不可过期的换代记录；任何既有阶段都不能自动解除停写。"""
        records = self.transport.redis.hgetall(self._read_rotation_key)
        closed = self.transport.redis.hgetall(self._read_rotation_close_key)
        if (not isinstance(records, dict) or not isinstance(closed, dict)
                or len(records) > self.read_cache_capacity or len(closed) > len(records)):
            raise ShmProtocolError("persistent read rotation record is malformed")
        if records and self.transport.redis.ttl(self._read_rotation_key) != -1:
            raise ShmProtocolError("persistent read rotation record must not expire")
        if closed and self.transport.redis.ttl(self._read_rotation_close_key) != -1:
            raise ShmProtocolError("persistent read rotation close record must not expire")
        record_ids = {decode_text(key) for key in records}
        if any(decode_text(key) not in record_ids for key in closed):
            raise ShmProtocolError("persistent read rotation close has no original record")
        for transition_id, encoded in closed.items():
            try:
                close = json.loads(decode_text(encoded))
            except (TypeError, ValueError) as exc:
                raise ShmProtocolError("persistent read rotation close record is invalid") from exc
            if (not isinstance(close, dict) or close.get("transition_id") != decode_text(transition_id)
                    or close.get("account_id") != self.account_id or close.get("environment") != self.environment
                    or close.get("state") not in ("CLOSE_PENDING", "CLOSED") or close.get("automatic_retry") is not False):
                raise ShmProtocolError("persistent read rotation close identity is invalid")
            closed_id, closed_raw = decode_text(transition_id), decode_text(encoded)
            if (self._rotation_closed_records.get(closed_id) != closed_raw
                    and self._rotation_close_pending_records.get(closed_id) != closed_raw):
                self._read_rotation_blocked = True
                self._block_writes("READ_ROTATION_UNCONFIRMED")
        for transition_id, encoded in records.items():
            try:
                record = json.loads(decode_text(encoded))
            except (TypeError, ValueError) as exc:
                raise ShmProtocolError("persistent read rotation record is invalid") from exc
            if (not isinstance(record, dict) or type(record.get("schema_version")) is not int
                    or record["schema_version"] != 1
                    or record.get("transition_id") != decode_text(transition_id)
                    or not re.fullmatch(r"[0-9a-f]{32}", record["transition_id"])
                    or record.get("account_id") != self.account_id or record.get("environment") != self.environment
                    or record.get("automatic_retry") is not False
                    or record.get("state") not in ("PREPARE_PENDING", "PREPARED", "COMMIT_PENDING", "COMMITTED",
                                                   "SUPERVISOR_VERIFIED", "ACTIVATE_PENDING", "ACTIVATED",
                                                   "GATEWAY_VERIFYING", "GATEWAY_VERIFIED")):
                raise ShmProtocolError("persistent read rotation identity or state is invalid")
            if (self._accepted_rotation_records.get(decode_text(transition_id)) == decode_text(encoded)
                    and record["state"] == "GATEWAY_VERIFIED"):
                continue
            self._read_rotation_blocked = True
            self._rotation_waiting_gateway = True
            self._block_writes("READ_ROTATION_UNCONFIRMED")

    def _verify_accepted_rotation_history(self):
        """新读仅核当前代和直接前代；申请新代时另以 Lua 核全部历史。"""
        if not self._accepted_rotation_record or not self._accepted_rotation_records:
            raise ShmProtocolError("accepted readonly rotation evidence is unavailable")
        current = self._accepted_rotation_record["transition_id"]
        generations = list(self._accepted_rotation_records)
        if generations[-1] != current or self._accepted_rotation_records[current] != self._accepted_rotation_record["raw"]:
            raise ShmProtocolError("accepted readonly rotation identity changed")
        if len(generations) > 1 and generations[-2] not in self._rotation_closed_records:
            raise ShmProtocolError("direct predecessor rotation has not been closed")
        dependencies = [[transition, self._accepted_rotation_records[transition],
                         self._rotation_closed_records.get(transition,
                             self._rotation_close_pending_records.get(transition, False))] for transition in generations[-2:]]
        verified = self.transport.redis.eval(_VERIFY_ROTATION_DEPENDENCIES, 2, self._read_rotation_key,
            self._read_rotation_close_key, json.dumps(dependencies, ensure_ascii=True, separators=(",", ":")))
        if verified != 1:
            raise ShmProtocolError("accepted readonly rotation dependency changed")

    def _verify_full_rotation_history(self):
        """关代等低频状态变更前，原子核验此前所有已接受记录。"""
        if not self._accepted_rotation_records:
            raise ShmProtocolError("accepted readonly rotation history is unavailable")
        if set(self._rotation_closed_records) & set(self._rotation_close_pending_records):
            raise ShmProtocolError("rotation close history is ambiguous")
        close = dict(self._rotation_closed_records)
        close.update(self._rotation_close_pending_records)
        if not set(close) <= set(self._accepted_rotation_records):
            raise ShmProtocolError("rotation close has no accepted original")
        if any(transition not in self._rotation_closed_records for transition in list(self._accepted_rotation_records)[:-1]):
            raise ShmProtocolError("a predecessor rotation has not been closed")
        dependencies = [[transition, raw, close.get(transition, False)]
                        for transition, raw in self._accepted_rotation_records.items()]
        verified = self.transport.redis.eval(_VERIFY_ROTATION_DEPENDENCIES, 2, self._read_rotation_key,
            self._read_rotation_close_key, json.dumps(dependencies, ensure_ascii=True, separators=(",", ":")), "FULL")
        if verified != 1:
            raise ShmProtocolError("accepted readonly rotation history changed")

    def _claim_read_rotation(self, record):
        """PREPARE 之前原子留存唯一停写记录；调用结果不明时禁止原生控制。"""
        if (self.execution_cursor_guard_target is None or not isinstance(record, dict)
                or self._read_rotation_blocked or record.get("state") != "PREPARE_PENDING"
                or self._rotation_open_delivery == "UNKNOWN"
                or (self._accepted_rotation_records and
                    (not self._rotation_open_acknowledged
                     or set(self._accepted_rotation_records) != set(self._rotation_closed_records)))
                or type(record.get("schema_version")) is not int or record["schema_version"] != 1
                or not isinstance(record.get("transition_id"), str)
                or not re.fullmatch(r"[0-9a-f]{32}", record["transition_id"])
                or record.get("account_id") != self.account_id or record.get("environment") != self.environment
                or record.get("automatic_retry") is not False):
            raise ShmProtocolError("read rotation has already been attempted")
        if len(self._accepted_rotation_records) >= self.read_cache_capacity:
            raise ShmProtocolError("READ_ROTATION_CAPACITY_BLOCKED")
        self._read_rotation_blocked = True
        self._block_writes("READ_ROTATION_UNCONFIRMED")
        encoded = json.dumps(record, sort_keys=True, allow_nan=False, separators=(",", ":"))
        try:
            history = [[transition, raw, self._rotation_closed_records[transition]]
                       for transition, raw in sorted(self._accepted_rotation_records.items())]
            claimed = self.transport.redis.eval(_CLAIM_ROTATION, 2, self._read_rotation_key,
                                                self._read_rotation_close_key, record["transition_id"],
                                                self.read_cache_capacity, encoded,
                                                json.dumps(history, ensure_ascii=True, separators=(",", ":")))
            if (not isinstance(claimed, (list, tuple)) or len(claimed) != 2
                    or claimed[0] != 1 or decode_text(claimed[1]) != encoded
                    or self.transport.redis.ttl(self._read_rotation_key) != -1):
                raise ShmProtocolError("read rotation claim or persistence is unconfirmed")
        except Exception:
            self._seal("READ_ROTATION_RECORD_UNCONFIRMED")
            raise
        self._rotation_open_delivery = "NOT_SENT"
        self._rotation_open_acknowledged = False
        return encoded

    def _advance_read_rotation(self, prior, record):
        """只比较完整旧记录并 CAS 推进，不在不明结果后重试。"""
        try:
            original = json.loads(prior)
        except (TypeError, ValueError) as exc:
            raise ShmProtocolError("read rotation prior record is invalid") from exc
        transitions = {("PREPARE_PENDING", "PREPARED"), ("PREPARED", "PREPARED"),
                       ("PREPARED", "COMMIT_PENDING"), ("COMMIT_PENDING", "COMMITTED"),
                       ("COMMITTED", "SUPERVISOR_VERIFIED"), ("SUPERVISOR_VERIFIED", "ACTIVATE_PENDING"),
                       ("ACTIVATE_PENDING", "ACTIVATED"), ("ACTIVATED", "GATEWAY_VERIFYING"),
                       ("GATEWAY_VERIFYING", "GATEWAY_VERIFIED")}
        if (not isinstance(original, dict) or not isinstance(record, dict)
                or (original.get("state"), record.get("state")) not in transitions
                or any(key != "state" and record.get(key) != value for key, value in original.items())
                or record == original):
            raise ShmProtocolError("read rotation state or immutable evidence changed")
        encoded = json.dumps(record, sort_keys=True, allow_nan=False, separators=(",", ":"))
        try:
            changed = self.transport.redis.eval(_SETTLE_WRITE, 1, self._read_rotation_key,
                                                record["transition_id"], prior, encoded)
            if changed != 1 or self.transport.redis.ttl(self._read_rotation_key) != -1:
                raise ShmProtocolError("read rotation compare-and-set is unconfirmed")
        except Exception:
            self._seal("READ_ROTATION_RECORD_UNCONFIRMED")
            raise
        return encoded

    def _check_native_process_identity(self):
        if self._verified_process_identity is None:
            raise ShmProtocolError("native process birth and executable were not independently verified at handshake")
        if _windows_native_process_identity(self.expected_native_pid) != self._verified_process_identity:
            raise ShmProtocolError("native process birth, executable or Windows session changed")

    def _verify_old_read_rotation_evidence(self, evidence, *, old_client=None, old_fault=None):
        """逐项核对原生凭证、代理 UNKNOWN 缓存和旧邮箱；只读，不确认旧帧。"""
        self._check_native_process_identity()
        old = self._clients["readonly"] if old_client is None else old_client
        fault = self._route_faults["readonly"] if old_fault is None else old_fault
        mailbox = old.mailbox
        expected_read_session = self._sessions["readonly"] if old_client is None else mailbox.session_id
        required = {"contract", "status", "account_id", "environment", "native_pid", "native_creation_filetime",
                    "windows_session_id", "read_session_id", "execution_session_id", "request_id", "method",
                    "sequence", "request_sequence", "response_sequence", "confirmed_sequence",
                    "read_expires_at_unix", "native_started_at_unix", "native_finished_at_unix", "automatic_retry"}
        if (not isinstance(evidence, dict) or set(evidence) != required
                or evidence.get("contract") != "READ_ROTATION_EVIDENCE_V1" or evidence.get("status") != "ELIGIBLE"
                or evidence.get("account_id") != self.account_id or evidence.get("environment") != self.environment
                or type(evidence.get("native_pid")) is not int or evidence["native_pid"] != self.expected_native_pid
                or type(evidence.get("native_creation_filetime")) is not int
                or evidence["native_creation_filetime"] != self._verified_process_identity["native_creation_filetime"]
                or type(evidence.get("windows_session_id")) is not int
                or evidence["windows_session_id"] != self._verified_process_identity["windows_session_id"]
                or evidence.get("read_session_id") != expected_read_session
                or evidence.get("execution_session_id") != self._sessions["execution"]
                or evidence.get("automatic_retry") is not False
                or not isinstance(fault, dict) or fault.get("state") != "RESULT_UNKNOWN"
                or fault.get("previous_unconfirmed") is not False
                or fault.get("trigger_request_id") != evidence.get("request_id")
                or fault.get("sequence") != evidence.get("sequence")
                or evidence.get("method") not in QUOTE_METHODS | {"get_positions", "get_local_data", "get_market_data_ex", "get_market_data_ex_batch"}):
            raise ShmProtocolError("old read completion evidence is incomplete or mismatched")
        sequence = evidence.get("sequence")
        if (type(sequence) is not int or sequence <= 0 or evidence.get("request_sequence") != sequence
                or evidence.get("response_sequence") != sequence
                or type(evidence.get("confirmed_sequence")) is not int
                or evidence["confirmed_sequence"] != sequence - 1):
            raise ShmProtocolError("old read completion sequence is not confirmed as pending")
        for field in ("read_expires_at_unix", "native_started_at_unix", "native_finished_at_unix"):
            value = evidence.get(field)
            if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
                raise ShmProtocolError("old read completion timing is invalid")
        if not (evidence["native_started_at_unix"] <= evidence["native_finished_at_unix"] <= time.time()
                and evidence["read_expires_at_unix"] <= time.time()):
            raise ShmProtocolError("old read callback or deadline is not complete")
        prior = self._replays.get(evidence["request_id"])
        if not prior or prior[1].get("status") != "UNKNOWN" or prior[1].get("sequence") != sequence:
            raise ShmProtocolError("original read UNKNOWN RID and sequence are not retained")
        if mailbox.closed or (mailbox.session_id, mailbox.server_pid) != (expected_read_session, self.expected_native_pid):
            raise ShmProtocolError("original readonly mailbox identity changed")
        region = mailbox.region
        if not region.acquire(0):
            raise ShmBusyError("original readonly mailbox lock is unavailable")
        try:
            header = mailbox._header()
            if (header[4:7] != [sequence, sequence, evidence["confirmed_sequence"]]
                    or not 0 < header[7] <= REQUEST_CAPACITY or not 0 < header[8] <= RESPONSE_CAPACITY):
                raise ShmProtocolError("original readonly mailbox completion differs from evidence")
            request = decode_payload(region.read(REQUEST_OFFSET, header[7]))
            response = decode_payload(region.read(RESPONSE_OFFSET, header[8]))
            if (not isinstance(request, dict) or request.get("request_id") != evidence["request_id"]
                    or request.get("method") != evidence["method"] or request.get("sequence") != sequence
                    or request.get("session_id") != evidence["read_session_id"]
                    or request.get("read_expires_at_unix") != evidence["read_expires_at_unix"]
                    or self._request_fingerprint(request) != prior[0]):
                raise ShmProtocolError("original readonly request differs from retained UNKNOWN")
            self._check_response(request, response, evidence["read_session_id"])
            if response.get("sequence") != sequence:
                raise ShmProtocolError("original readonly response sequence differs")
        finally:
            region.release()

    def _execution_control_ping(self, command=None, *, field="read_rotation", timeout_seconds=None):
        """仅内部使用原执行邮箱；任何结果不明都不重发。"""
        if field not in ("read_rotation", "read_rotation_control_status", "read_rotation_capability",
                         "read_rotation_capability_status"):
            raise ShmProtocolError("unsupported internal execution control field")
        if (self.execution_cursor_guard_target == {"gateway_id": "qmt-vm-001", "profile_id": "live-stock"}
                and self._route_faults["readonly"] is not None):
            # 换代控制需要原生回调读取旧邮箱。跨执行请求持该锁会阻止读取；
            # 不持锁又无法排除覆盖竞争，当前禁止发送直到另有完整换代合同。
            error = ShmBusyError("readonly fault prevents internal execution rotation control; request not sent")
            error.previous_unconfirmed = False
            raise error
        client = self._check_client("execution")
        timeout = self.timeout_seconds if timeout_seconds is None else min(self.timeout_seconds, timeout_seconds)
        if timeout <= 0:
            raise ShmProtocolError("internal execution control deadline exceeded")
        deadline = time.time() + timeout
        request = dict(schema_version=1, request_id=uuid.uuid4().hex, account_id=self.account_id,
                       method="ping", params={} if command is None else {field: command},
                       read_expires_at_unix=deadline)
        if self.environment == "LIVE":
            request["environment"] = self.environment
        try:
            response = client.send_request(request, timeout_seconds=timeout)
            self._check_response(request, response, self._sessions["execution"])
            if response["ok"] is not True or time.time() >= deadline:
                raise ShmProtocolError("native execution control was rejected")
            self._validate_identity_data("execution", response["data"])
            if self._verified_process_identity is not None:
                if (response["data"].get("native_creation_filetime")
                        != self._verified_process_identity["native_creation_filetime"]):
                    raise ShmProtocolError("native execution control process birth changed")
                self._check_native_process_identity()
            return response
        except Exception:
            self._mark_route_fault("execution", "RESULT_UNKNOWN", request, "UNKNOWN", previous_unconfirmed=None)
            raise

    def _execution_rotation_control(self, command):
        """原 CONTROL 只发一次；后续只读状态帧独立发出并逐一验证。"""
        if (not isinstance(command, dict) or command.get("contract") != "READ_ROTATION_CONTROL_V1"
                or command.get("action") not in ("PREPARE", "COMMIT", "ACTIVATE", "CLOSE")
                or not isinstance(command.get("transition_id"), str)
                or re.fullmatch(r"[0-9a-f]{32}", command["transition_id"]) is None
                or not isinstance(command.get("nonce"), str)
                or re.fullmatch(r"[0-9a-f]{32}", command["nonce"]) is None
                or not isinstance(command.get("capability"), str)
                or re.fullmatch(r"[0-9a-f]{64}", command["capability"]) is None):
            raise ShmProtocolError("rotation control authorization is invalid")
        identity = dict(contract="READ_ROTATION_CONTROL_V1", action=command["action"],
                        transition_id=command["transition_id"])
        control_key = (command["transition_id"], command["action"])
        if control_key in self._rotation_control_attempted:
            raise ShmProtocolError("rotation control was already attempted; do not retry")
        self._rotation_control_attempted.add(control_key)
        response = self._execution_control_ping(command)
        initial = response["data"].get("read_rotation_control")
        if (not isinstance(initial, dict) or set(initial) != set(identity) | {"status", "automatic_retry"}
                or any(initial.get(key) != value for key, value in identity.items())
                or initial.get("status") != "AUTH_PENDING" or initial.get("automatic_retry") is not False):
            raise ShmProtocolError("rotation control did not acknowledge pending authorization")
        deadline = time.monotonic() + SUPERVISOR_ATTACH_BUDGET_SECONDS
        expected_status = {"PREPARE": "PREPARED", "COMMIT": "COMMITTED", "ACTIVATE": "ACTIVATED", "CLOSE": "CLOSED"}[command["action"]]
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise ShmProtocolError("rotation authorization outcome is unconfirmed")
            time.sleep(min(0.05, remaining))
            response = self._execution_control_ping(identity, field="read_rotation_control_status",
                                                    timeout_seconds=deadline - time.monotonic())
            status = response["data"].get("read_rotation_control_status")
            if (not isinstance(status, dict) or set(status) != set(identity) | {"status", "automatic_retry", "receipt"}
                    or any(status.get(key) != value for key, value in identity.items())
                    or status.get("automatic_retry") is not False):
                raise ShmProtocolError("rotation authorization status identity is invalid")
            if (status.get("receipt") is None and (status.get("status") == "AUTH_PENDING"
                    or (command["action"] == "ACTIVATE"
                        and status.get("status") in ("ACTIVATION_PENDING", "READ_RELEASE_PENDING"))
                    or (command["action"] == "CLOSE" and status.get("status") == "RETIRE_ACK_PENDING"))):
                continue
            receipt = status.get("receipt")
            if command["action"] == "PREPARE" and status.get("status") == "UNKNOWN":
                self._observe_rotation_capacity_evidence(response["data"].get("read_rotation_capacity_evidence"), command)
            if (status.get("status") != expected_status or not isinstance(receipt, dict)
                    or receipt.get("contract") != identity["contract"] or receipt.get("action") != identity["action"]
                    or receipt.get("transition_id") != identity["transition_id"]
                    or receipt.get("status") != expected_status):
                raise ShmProtocolError("rotation authorization outcome is unconfirmed")
            return receipt

    def _observe_rotation_capacity_evidence(self, evidence, command):
        """仅透传原生可核的 Win32 资源错误；控制结果仍为 UNKNOWN。"""
        old_evidence = command.get("old_evidence") if isinstance(command, dict) else None
        if (self.execution_cursor_guard_target is None or not isinstance(old_evidence, dict)
                or not isinstance(evidence, dict) or self._verified_process_identity is None):
            return
        expected = dict(contract="READ_ROTATION_CAPACITY_V1", status="READ_ROTATION_CAPACITY_BLOCKED",
            native_operation="CreateFileMappingW", transition_id=command.get("transition_id"), account_id=self.account_id,
            environment=self.environment, native_pid=self.expected_native_pid,
            native_creation_filetime=self._verified_process_identity["native_creation_filetime"],
            windows_session_id=self.expected_windows_session_id, old_read_name=getattr(self._clients["readonly"], "name", None),
            old_read_session_id=self._sessions["readonly"], old_request_id=old_evidence.get("request_id"),
            old_sequence=old_evidence.get("sequence"), execution_session_id=self._sessions["execution"],
            old_evidence=old_evidence, old_frame_unchanged=True, automatic_retry=False)
        if (set(evidence) != set(expected) | {"native_winerror"}
                or any(type(evidence.get(key)) is not type(value) or evidence.get(key) != value for key, value in expected.items())
                or type(evidence.get("native_winerror")) is not int or evidence["native_winerror"] not in (8, 14, 1455)):
            return
        try:
            _, record = self._rotation_record(command["transition_id"])
            if (record.get("state") != "PREPARE_PENDING" or record.get("native_evidence") != old_evidence
                    or record.get("old_previous_unconfirmed") is not False
                    or record.get("old_read_session_id") != self._sessions["readonly"]
                    or record.get("execution_session_id") != self._sessions["execution"]):
                return
            self._verify_old_read_rotation_evidence(old_evidence)
            self._verify_rotation_execution_header()
            self._check_native_process_identity()
        except Exception:
            return
        self._read_rotation_capacity_evidence = copy.deepcopy(evidence)

    def attest_read_rotation_capability(self):
        """监督器私有 pipe 与原生独立 pipe 交叉挑战；未知不重发。"""
        if not self._poll_lock.acquire(False):
            raise ShmBusyError("cannot attest rotation while Redis polling is active")
        if not self._lock.acquire(False):
            self._poll_lock.release()
            raise ShmBusyError("cannot attest rotation while another proxy operation is active")
        try:
            self._ensure_open()
            if (not self._verified or self.execution_cursor_guard_target !=
                    {"gateway_id": "qmt-vm-001", "profile_id": "live-stock"}
                    or not self._read_isolation_supported or not self._supervisor_attach_verified
                    or self._supervisor_attach_receipt is None or self._supervisor_rotation_channel is None
                    or self._verified_process_identity is None or self._rotation_capability_attempted):
                raise ShmProtocolError("independent readonly rotation capability is unavailable")
            self._check_native_process_identity()
            supervisor = self._supervisor_attach_receipt["supervisor"]
            native = self._verified_process_identity
            proxy_identity = _windows_native_process_identity(os.getpid())
            if (_windows_native_process_identity(os.getppid()) != supervisor
                    or proxy_identity.get("native_pid") != os.getpid()
                    or proxy_identity.get("windows_session_id") != self.expected_windows_session_id
                    or type(proxy_identity.get("native_creation_filetime")) is not int
                    or proxy_identity["native_creation_filetime"] <= 0):
                raise ShmProtocolError("supervisor or proxy process identity changed")
            nonce = uuid.uuid4().hex
            begin = dict(contract="SUPERVISOR_ROTATION_CAPABILITY_V1", action="BEGIN", nonce=nonce,
                account_id=self.account_id, environment=self.environment, native_pid=self.expected_native_pid,
                native_creation_filetime=native["native_creation_filetime"], windows_session_id=self.expected_windows_session_id,
                execution_session_id=self._sessions["execution"], read_session_id=self._sessions["readonly"],
                proxy_pid=os.getpid(), proxy_creation_filetime=proxy_identity["native_creation_filetime"])
            self._rotation_capability_attempted = True
            response = self._supervisor_rotation_channel.register(begin)
            unavailable = dict(contract=begin["contract"], action="BEGIN", status="UNAVAILABLE", nonce=nonce)
            if response == unavailable:
                # 已确认未注册；下一次观察使用全新 nonce，不曾向原生发送。
                self._rotation_capability_attempted = False
                return None
            expected_begin = dict(contract=begin["contract"], action="BEGIN", status="PENDING", nonce=nonce)
            if response != expected_begin:
                raise ShmProtocolError("supervisor capability begin is unconfirmed")
            command = dict(contract="READ_ROTATION_CAPABILITY_V1", nonce=nonce)
            initial = self._execution_control_ping(command, field="read_rotation_capability")
            expected_initial = dict(contract=command["contract"], nonce=nonce, status="PENDING", automatic_retry=False)
            if initial["data"].get("read_rotation_capability") != expected_initial:
                raise ShmProtocolError("native capability challenge is unconfirmed")
            deadline = time.monotonic() + SUPERVISOR_ATTACH_BUDGET_SECONDS
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise ShmProtocolError("native capability challenge timed out")
                time.sleep(min(0.05, remaining))
                observed = self._execution_control_ping(command, field="read_rotation_capability_status",
                                                        timeout_seconds=deadline - time.monotonic())
                status = observed["data"].get("read_rotation_capability_status")
                expected_status = {"contract", "nonce", "status", "automatic_retry", "receipt"}
                if (not isinstance(status, dict) or set(status) != expected_status
                        or status.get("contract") != command["contract"] or status.get("nonce") != nonce
                        or status.get("automatic_retry") is not False):
                    raise ShmProtocolError("native capability status identity changed")
                if status.get("status") == "PENDING" and status.get("receipt") is None:
                    continue
                receipt = status.get("receipt")
                expected_receipt = dict(contract="QMT_NATIVE_ROTATION_PIPE_V1", action="CAPABILITY_PROBE", nonce=nonce,
                    account_id=self.account_id, environment=self.environment, native_pid=self.expected_native_pid,
                    native_creation_filetime=native["native_creation_filetime"], windows_session_id=self.expected_windows_session_id,
                    execution_session_id=self._sessions["execution"], read_session_id=self._sessions["readonly"],
                    supervisor_pid=os.getppid(), supervisor_creation_filetime=supervisor["native_creation_filetime"], status="READY")
                if (status.get("status") != "READY" or not isinstance(receipt, dict)
                        or set(receipt) != set(expected_receipt)
                        or any(type(receipt[key]) is not type(value) or receipt[key] != value
                               for key, value in expected_receipt.items())):
                    raise ShmProtocolError("native capability proof is unconfirmed")
                break
            verified = self._supervisor_rotation_channel.register(dict(contract=begin["contract"], action="VERIFY", nonce=nonce))
            if verified != dict(contract=begin["contract"], action="VERIFY", status="VERIFIED", nonce=nonce, receipt=receipt):
                raise ShmProtocolError("supervisor capability proof is unconfirmed")
            if (_windows_native_process_identity(os.getppid()) != supervisor
                    or _windows_native_process_identity(os.getpid()) != proxy_identity):
                raise ShmProtocolError("capability process identity changed")
            self._check_native_process_identity()
            self._rotation_capability = dict(receipt=copy.deepcopy(receipt), verified_at=time.monotonic())
            return dict(status="VERIFIED", native_rotation_ready=observed["data"].get("read_rotation_authorization_ready") is True,
                        automatic_retry=False)
        finally:
            self._lock.release()
            self._poll_lock.release()

    def _native_rotation_authorization_verified(self, response):
        """只信同进程、同双 SID、仍新鲜的双通道挑战和原生明确能力。"""
        proof = self._rotation_capability
        data = response.get("data") if isinstance(response, dict) else None
        if (not isinstance(proof, dict) or not isinstance(data, dict)
                or not 0 <= time.monotonic() - proof.get("verified_at", float("-inf")) <= 20
                or data.get("read_rotation_authorization_ready") is not True
                or data.get("read_rotation_capability_contract") != "READ_ROTATION_CAPABILITY_V1"
                or data.get("native_read_session_id") != self._sessions["readonly"]
                or proof.get("receipt", {}).get("read_session_id") != self._sessions["readonly"]
                or proof["receipt"].get("execution_session_id") != self._sessions["execution"]):
            return False
        try:
            self._check_native_process_identity()
            return (_windows_native_process_identity(os.getppid()) == self._supervisor_attach_receipt["supervisor"])
        except Exception:
            return False

    def _register_rotation_authorization(self, record, stage):
        """只通过继承的私有管道注册一次；秘密不存入 Redis 或日志。"""
        nonce_field, expected_state = {"PREPARE": ("prepare_nonce", "PREPARE_PENDING"),
                                      "COMMIT": ("commit_nonce", "COMMIT_PENDING"),
                                      "ACTIVATE": ("activate_nonce", "ACTIVATE_PENDING"),
                                      "CLOSE": ("close_nonce", "CLOSE_PENDING")}.get(stage, (None, None))
        if (nonce_field is None or not isinstance(record, dict) or record.get("state") != expected_state
                or self._supervisor_rotation_channel is None or not self._supervisor_attach_verified
                or self._supervisor_attach_receipt is None or self._supervisor_write_gate is None):
            raise ShmProtocolError("independent supervisor rotation registration is unavailable")
        try:
            self._supervisor_write_gate.check()
        except SupervisorWriteGateError as exc:
            if exc.reason != "SUPERVISOR_IDENTITY_NOT_READY" or exc.permanent:
                raise
        self._check_native_process_identity()
        if (_windows_native_process_identity(os.getppid()) != self._supervisor_attach_receipt["supervisor"]
                or self.transport.redis.ttl(self._read_rotation_key) != -1):
            raise ShmProtocolError("supervisor or persistent transition identity changed")
        key = self._read_rotation_close_key if stage == "CLOSE" else self._read_rotation_key
        if self.transport.redis.ttl(key) != -1:
            raise ShmProtocolError("persistent rotation stage is unavailable")
        raw = decode_text(self.transport.redis.hget(key, record["transition_id"]))
        encoded = json.dumps(record, sort_keys=True, allow_nan=False, separators=(",", ":"))
        if raw != encoded:
            raise ShmProtocolError("rotation transition changed before registration")
        if stage == "CLOSE":
            accepted, _ = self._rotation_record(record["transition_id"])
            if (self._accepted_rotation_record is None or accepted != self._accepted_rotation_record["raw"]
                    or record.get("gateway_verified_sha256") != hashlib.sha256(accepted.encode("utf-8")).hexdigest()):
                raise ShmProtocolError("rotation close original record changed")
        request = dict(contract="SUPERVISOR_ROTATION_AUTH_V1", action="REGISTER", stage=stage,
            transition_id=record["transition_id"], nonce=record[nonce_field], account_id=self.account_id,
            environment=self.environment, native_pid=self.expected_native_pid,
            native_creation_filetime=record["native_creation_filetime"], windows_session_id=self.expected_windows_session_id,
            execution_session_id=self._sessions["execution"], old_read_session_id=record["old_read_session_id"],
            candidate_read_session_id=record.get("candidate_read_session_id"))
        if stage == "CLOSE":
            request["gateway_verified_sha256"] = record["gateway_verified_sha256"]
        registration_key = (record["transition_id"], stage)
        if registration_key in self._rotation_registration_attempted:
            raise ShmProtocolError("rotation registration was already attempted; do not retry")
        self._rotation_registration_attempted.add(registration_key)
        response = self._supervisor_rotation_channel.register(request)
        expected = dict(contract="SUPERVISOR_ROTATION_AUTH_V1", action="REGISTER", status="REGISTERED",
                        stage=stage, transition_id=record["transition_id"], nonce=record[nonce_field])
        if (not isinstance(response, dict) or set(response) != set(expected) | {"capability"}
                or any(type(response.get(key)) is not type(value) or response.get(key) != value
                       for key, value in expected.items())
                or not isinstance(response.get("capability"), str)
                or re.fullmatch(r"[0-9a-f]{64}", response["capability"]) is None):
            raise ShmProtocolError("supervisor rotation registration response is unconfirmed")
        return response["capability"]

    def prepare_readonly_rotation(self):
        """一次准备和提交候选；监督器验收前保留旧代理绑定并持续停写。"""
        if not self._poll_lock.acquire(False):
            raise ShmProtocolError("cannot rotate while Redis polling is active")
        if not self._lock.acquire(False):
            self._poll_lock.release()
            raise ShmProtocolError("cannot rotate while another proxy operation is active")
        try:
            self._ensure_open()
            if (self.execution_cursor_guard_target is None or not self._verified
                    or not self._read_isolation_supported or self._read_rotation_blocked):
                raise ShmProtocolError("verified read fault isolation and an unattempted rotation are required")
            self._check_native_process_identity()
            self._check_client("execution")
            observation = self._execution_control_ping()
            if (observation["data"].get("read_rotation_authorization_ready") is not True
                    or not self._native_rotation_authorization_verified(observation)):
                raise ShmProtocolError("native read rotation authorization is not verified")
            evidence = observation["data"].get("read_rotation_evidence")
            self._verify_old_read_rotation_evidence(evidence)
            old_name = getattr(self._clients["readonly"], "name", None)
            if not isinstance(old_name, str) or not old_name:
                raise ShmProtocolError("original readonly mailbox name is unavailable")
            transition = uuid.uuid4().hex
            record = dict(schema_version=1, transition_id=transition, account_id=self.account_id,
                          environment=self.environment, state="PREPARE_PENDING", automatic_retry=False,
                          native_pid=self.expected_native_pid,
                          native_creation_filetime=evidence["native_creation_filetime"],
                          windows_session_id=evidence["windows_session_id"],
                          execution_session_id=self._sessions["execution"], old_read_name=old_name,
                          old_read_session_id=self._sessions["readonly"], old_request_id=evidence["request_id"],
                          old_method=evidence["method"], old_sequence=evidence["sequence"], old_previous_unconfirmed=False,
                          old_read_expires_at_unix=evidence["read_expires_at_unix"],
                          prepare_nonce=uuid.uuid4().hex,
                          native_evidence=copy.deepcopy(evidence))
            encoded = self._claim_read_rotation(record)
            self._read_rotation_stage = "PREPARE_PENDING"
            capability = self._register_rotation_authorization(record, "PREPARE")
            command = dict(contract="READ_ROTATION_CONTROL_V1", action="PREPARE", transition_id=transition,
                           nonce=record["prepare_nonce"], capability=capability, old_evidence=copy.deepcopy(evidence))
            prepared = self._execution_rotation_control(command)
            capability = command = None
            fields = {"contract", "action", "status", "transition_id", "old_read_name", "old_evidence",
                      "candidate_read_name", "candidate_read_session_id", "probe_request_id", "token"}
            if (not isinstance(prepared, dict) or set(prepared) != fields
                    or prepared.get("contract") != "READ_ROTATION_CONTROL_V1" or prepared.get("action") != "PREPARE"
                    or prepared.get("status") != "PREPARED" or prepared.get("transition_id") != transition
                    or prepared.get("old_read_name") != old_name or prepared.get("old_evidence") != evidence
                    or not isinstance(prepared.get("candidate_read_name"), str)
                    or prepared["candidate_read_name"] == old_name
                    or not isinstance(prepared.get("candidate_read_session_id"), str)
                    or not prepared["candidate_read_session_id"]
                    or prepared["candidate_read_session_id"] in self._sessions.values()
                    or prepared.get("probe_request_id") != "read-rotation-" + transition
                    or not isinstance(prepared.get("token"), str)
                    or not re.fullmatch(r"[0-9a-f]{32}", prepared["token"])):
                raise ShmProtocolError("native read rotation preparation receipt is invalid")
            record = dict(record, state="PREPARED", candidate_read_name=prepared["candidate_read_name"],
                          candidate_read_session_id=prepared["candidate_read_session_id"],
                          probe_request_id=prepared["probe_request_id"], token=prepared["token"])
            encoded = self._advance_read_rotation(encoded, record)
            self._read_rotation_stage = "PREPARED"
            candidate = ShmReadOnlyClient(record["candidate_read_name"], self.account_id)
            self._read_rotation_candidate = candidate
            if (candidate.mailbox.session_id != record["candidate_read_session_id"]
                    or candidate.mailbox.server_pid != self.expected_native_pid):
                raise ShmProtocolError("candidate readonly mailbox identity differs from preparation")
            deadline = time.time() + self.timeout_seconds
            probe = dict(schema_version=1, request_id=record["probe_request_id"], account_id=self.account_id,
                         method="ping", params={}, read_expires_at_unix=deadline)
            if self.environment == "LIVE":
                probe["environment"] = self.environment
            probe_response = candidate.send_request(probe, timeout_seconds=self.timeout_seconds)
            self._check_response(probe, probe_response, record["candidate_read_session_id"])
            self._validate_identity_data("readonly", probe_response["data"], session_id=record["candidate_read_session_id"])
            if (probe_response["ok"] is not True or probe_response.get("sequence") != 1
                    or probe_response["data"].get("environment") != self.environment
                    or probe_response["data"].get("native_creation_filetime") != record["native_creation_filetime"]
                    or probe_response["data"].get("read_fault_isolation_contract") != READ_FAULT_ISOLATION_CONTRACT
                    or not set(self._source_identity["readonly"]["capabilities"]) <= set(probe_response["data"]["capabilities"])):
                raise ShmProtocolError("candidate readonly probe identity is invalid")
            if not candidate.mailbox.region.acquire(0):
                raise ShmProtocolError("candidate probe mailbox lock is unavailable")
            try:
                header = candidate.mailbox._header()
                if header[4:7] != [1, 1, 1]:
                    raise ShmProtocolError("candidate probe acknowledgement is not confirmed")
            finally:
                candidate.mailbox.region.release()
            record = dict(record, probe_sequence=1, probe_source=copy.deepcopy(probe_response["data"]))
            encoded = self._advance_read_rotation(encoded, record)
            self._verify_old_read_rotation_evidence(evidence)
            self._check_native_process_identity()
            record = dict(record, state="COMMIT_PENDING", commit_nonce=uuid.uuid4().hex)
            encoded = self._advance_read_rotation(encoded, record)
            self._read_rotation_stage = "COMMIT_PENDING"
            capability = self._register_rotation_authorization(record, "COMMIT")
            command = dict(contract="READ_ROTATION_CONTROL_V1", action="COMMIT", transition_id=transition,
                           nonce=record["commit_nonce"], capability=capability,
                           token=record["token"], probe_request_id=record["probe_request_id"], probe_sequence=1)
            receipt = self._execution_rotation_control(command)
            capability = command = None
            if (not isinstance(receipt, dict) or receipt.get("contract") != "READ_ROTATION_CONTROL_V1"
                    or receipt.get("action") != "COMMIT" or receipt.get("status") != "COMMITTED"
                    or receipt.get("transition_id") != transition
                    or receipt.get("candidate_read_name") != record["candidate_read_name"]
                    or receipt.get("candidate_read_session_id") != record["candidate_read_session_id"]
                    or receipt.get("execution_session_id") != self._sessions["execution"]):
                raise ShmProtocolError("native read rotation commit is unconfirmed")
            record = dict(record, state="COMMITTED", commit_evidence=copy.deepcopy(receipt))
            self._advance_read_rotation(encoded, record)
            self._rotation_committed_record = copy.deepcopy(record)
            self._read_rotation_stage = "COMMITTED"
            return dict(status="COMMITTED", transition_id=transition, writes_blocked=True,
                        read_source_state="RESULT_UNKNOWN", supervisor_verification_required=True,
                        automatic_retry=False)
        finally:
            self._lock.release()
            self._poll_lock.release()

    def _rotation_record(self, transition_id):
        if (not isinstance(transition_id, str) or re.fullmatch(r"[0-9a-f]{32}", transition_id) is None
                or self.transport.redis.ttl(self._read_rotation_key) != -1):
            raise ShmProtocolError("durable rotation record is unavailable")
        raw = decode_text(self.transport.redis.hget(self._read_rotation_key, transition_id))
        if not isinstance(raw, str) or not raw or len(raw.encode("utf-8")) > 1024 * 1024:
            raise ShmProtocolError("durable rotation record is unavailable")
        record = json.loads(raw)
        if (not isinstance(record, dict) or record.get("transition_id") != transition_id
                or record.get("account_id") != self.account_id or record.get("environment") != self.environment
                or record.get("automatic_retry") is not False):
            raise ShmProtocolError("durable rotation record identity is invalid")
        return raw, record

    def _verify_rotation_candidate_frame(self, record):
        candidate = self._read_rotation_candidate
        if (candidate is None or candidate.mailbox.closed or candidate.mailbox.name != record.get("candidate_read_name")
                or candidate.mailbox.session_id != record.get("candidate_read_session_id")
                or candidate.mailbox.server_pid != self.expected_native_pid):
            raise ShmProtocolError("candidate readonly source identity changed")
        mailbox, region = candidate.mailbox, candidate.mailbox.region
        if not region.acquire(0):
            raise ShmBusyError("candidate readonly mailbox is busy")
        try:
            header = mailbox._header()
            if (header[2] != READY or header[4:7] != [1, 1, 1]
                    or not 0 < header[7] <= REQUEST_CAPACITY or not 0 < header[8] <= RESPONSE_CAPACITY):
                raise ShmProtocolError("candidate probe frame is unavailable or changed")
            request = decode_payload(region.read(REQUEST_OFFSET, header[7]))
            response = decode_payload(region.read(RESPONSE_OFFSET, header[8]))
            self._validate_environment(request, "candidate probe request")
            if (not isinstance(request, dict) or request.get("account_id") != self.account_id
                    or request.get("method") != "ping" or request.get("params") != {}
                    or request.get("request_id") != record.get("probe_request_id")
                    or request.get("session_id") != record["candidate_read_session_id"]
                    or type(request.get("sequence")) is not int or request["sequence"] != 1):
                raise ShmProtocolError("candidate probe request identity changed")
            self._check_response(request, response, record["candidate_read_session_id"])
            if (response.get("ok") is not True or response.get("sequence") != 1
                    or response.get("data") != record.get("probe_source")):
                raise ShmProtocolError("candidate probe response differs from original handshake")
            self._validate_identity_data("readonly", response["data"], session_id=record["candidate_read_session_id"])
        finally:
            region.release()

    def _verify_rotation_execution_header(self):
        client = self._check_client("execution")
        mailbox, region = client.mailbox, client.mailbox.region
        if not region.acquire(0):
            raise ShmBusyError("execution mailbox is busy")
        try:
            header = mailbox._header()
            if header[2] != READY or not header[4] == header[5] == header[6]:
                raise ShmProtocolError("execution mailbox has an unconfirmed request")
        finally:
            region.release()

    def _gateway_acceptance_frames_ready(self, record):
        """只读检查三块物理帧；竞争中的 mutex 不构成帧失效或外部操作。"""
        try:
            self._verify_old_read_rotation_evidence(record["native_evidence"],
                old_client=self._retired_read_clients[-1], old_fault=self._retired_read_faults[-1])
            self._verify_rotation_candidate_frame(record)
            self._verify_rotation_execution_header()
        except ShmBusyError:
            return False
        return True

    def _rotation_close_frames_ready(self, record):
        """关代仅重核旧帧与现行执行源；新读 probe 已可能被正常行情覆盖。"""
        self._check_native_process_identity()
        current = self._check_client("readonly")
        if (current.mailbox.name != record.get("candidate_read_name")
                or current.mailbox.session_id != record.get("candidate_read_session_id")):
            raise ShmProtocolError("current readonly source differs from the accepted rotation")
        try:
            self._verify_old_read_rotation_evidence(record["native_evidence"],
                old_client=self._retired_read_clients[-1], old_fault=self._retired_read_faults[-1])
            self._verify_rotation_execution_header()
        except ShmBusyError:
            return False
        return True

    def _verify_rotation_supervisor_receipt(self, record):
        if (self._rotation_committed_record is None or self._supervisor_attach_receipt is None
                or record.get("state") != "SUPERVISOR_VERIFIED" or psutil is None):
            raise ShmProtocolError("supervisor rotation evidence is unavailable")
        for field, value in self._rotation_committed_record.items():
            if field != "state" and (field not in record or type(record[field]) is not type(value) or record[field] != value):
                raise ShmProtocolError("committed rotation evidence changed")
        supervisor = self._supervisor_attach_receipt["supervisor"]
        if (_windows_native_process_identity(os.getppid()) != supervisor
                or record.get("supervisor_pid") != os.getppid()
                or type(record.get("supervisor_creation_filetime")) is not int
                or record["supervisor_creation_filetime"] != supervisor["native_creation_filetime"]):
            raise ShmProtocolError("rotation supervisor process identity changed")
        parent, current = psutil.Process(os.getppid()), psutil.Process(os.getpid())
        parent_birth, proxy_birth = parent.create_time(), current.create_time()
        if (not parent.is_running() or not current.is_running() or current.ppid() != os.getppid()
                or type(parent_birth) not in (int, float) or not math.isfinite(parent_birth) or parent_birth <= 0
                or type(proxy_birth) not in (int, float) or not math.isfinite(proxy_birth) or proxy_birth <= 0
                or type(record.get("supervisor_create_time")) is not type(parent_birth)
                or record["supervisor_create_time"] != parent_birth):
            raise ShmProtocolError("rotation supervisor or proxy birth is unavailable")
        when = record.get("supervisor_verified_at_unix")
        native = record.get("native_evidence")
        if (type(when) not in (int, float) or not math.isfinite(when) or not 0 < when <= time.time()
                or not isinstance(native, dict)):
            raise ShmProtocolError("rotation supervisor observation is invalid")
        expected = dict(contract="READ_ROTATION_SUPERVISOR_EVIDENCE_V1", transition_id=record["transition_id"],
            account_id=self.account_id, environment=self.environment, native_pid=self.expected_native_pid,
            native_creation_filetime=record["native_creation_filetime"], windows_session_id=self.expected_windows_session_id,
            execution_session_id=self._sessions["execution"], old_read_name=record["old_read_name"],
            old_read_session_id=record["old_read_session_id"], old_request_id=record["old_request_id"],
            old_method=record["old_method"], old_sequence=record["old_sequence"],
            old_confirmed_sequence=native.get("confirmed_sequence"), old_expires_at_unix=record["old_read_expires_at_unix"],
            new_read_name=record["candidate_read_name"], new_read_session_id=record["candidate_read_session_id"],
            new_probe_request_id=record["probe_request_id"], new_probe_sequence=1, automatic_retry=False)
        evidence = record.get("supervisor_evidence")
        if (not isinstance(evidence, dict) or set(evidence) != set(expected)
                or any(type(evidence[key]) is not type(value) or evidence[key] != value for key, value in expected.items())):
            raise ShmProtocolError("rotation supervisor receipt is inconsistent")
        self._verify_old_read_rotation_evidence(native)
        self._verify_rotation_candidate_frame(record)
        self._verify_rotation_execution_header()
        self._check_native_process_identity()
        return proxy_birth

    def activate_readonly_rotation(self):
        """监督器验收后只激活新读邮箱；旧邮箱和执行对象继续保留。"""
        if not self._poll_lock.acquire(False):
            raise ShmBusyError("cannot rotate while Redis polling is active")
        if not self._lock.acquire(False):
            self._poll_lock.release()
            raise ShmBusyError("cannot rotate while another proxy operation is active")
        try:
            self._ensure_open()
            if (self._read_rotation_stage != "COMMITTED" or self._rotation_committed_record is None
                    or not self._read_rotation_blocked):
                raise ShmProtocolError("no committed readonly rotation is awaiting supervisor acceptance")
            raw, record = self._rotation_record(self._rotation_committed_record["transition_id"])
            if record.get("state") == "COMMITTED":
                return None
            birth = self._verify_rotation_supervisor_receipt(record)
            pending = dict(record, state="ACTIVATE_PENDING", activate_nonce=uuid.uuid4().hex)
            encoded = self._advance_read_rotation(raw, pending)
            self._read_rotation_stage = "ACTIVATE_PENDING"
            capability = self._register_rotation_authorization(pending, "ACTIVATE")
            command = dict(contract="READ_ROTATION_CONTROL_V1", action="ACTIVATE",
                           transition_id=record["transition_id"], nonce=pending["activate_nonce"],
                           capability=capability, token=record["token"])
            receipt = self._execution_rotation_control(command)
            capability = command = None
            expected = dict(contract="READ_ROTATION_CONTROL_V1", action="ACTIVATE", status="ACTIVATED",
                transition_id=record["transition_id"], candidate_read_name=record["candidate_read_name"],
                candidate_read_session_id=record["candidate_read_session_id"], execution_session_id=self._sessions["execution"])
            if (not isinstance(receipt, dict) or set(receipt) != set(expected)
                    or any(type(receipt[key]) is not type(value) or receipt[key] != value for key, value in expected.items())):
                raise ShmProtocolError("native readonly activation receipt is invalid")
            self._verify_old_read_rotation_evidence(record["native_evidence"])
            self._verify_rotation_candidate_frame(record)
            self._verify_rotation_execution_header()
            self._check_native_process_identity()
            if _windows_native_process_identity(os.getppid()) != self._supervisor_attach_receipt["supervisor"]:
                raise ShmProtocolError("supervisor process changed during readonly activation")
            old = self._clients["readonly"]
            self._retired_read_clients.append(old)
            self._retired_read_faults.append(copy.deepcopy(self._route_faults["readonly"]))
            self._retired_read_ids.update(self._unconfirmed_read_ids["readonly"])
            self._retired_read_ids.add(record["old_request_id"])
            self._clients["readonly"] = self._read_rotation_candidate
            self._sessions["readonly"] = record["candidate_read_session_id"]
            self._source_identity["readonly"] = copy.deepcopy(record["probe_source"])
            self._route_faults["readonly"] = None
            self._last_read_success_at = None
            self._last_native_read_observation_at = None
            self._rotation_waiting_gateway = True
            activated = dict(pending, state="ACTIVATED", activation_evidence=copy.deepcopy(receipt),
                             proxy_pid=os.getpid(), proxy_create_time=birth)
            self._advance_read_rotation(encoded, activated)
            self._rotation_activated_record = copy.deepcopy(activated)
            self._read_rotation_stage = "ACTIVATED"
            return dict(status="ACTIVATED", transition_id=record["transition_id"],
                        writes_blocked=True, gateway_verification_required=True, automatic_retry=False)
        finally:
            self._lock.release()
            self._poll_lock.release()

    def accept_gateway_rotation(self):
        """仅解除本次只读换代门；金融写入和其他未知状态保持原样。"""
        if not self._poll_lock.acquire(False):
            raise ShmBusyError("cannot accept rotation while Redis polling is active")
        if not self._lock.acquire(False):
            self._poll_lock.release()
            raise ShmBusyError("cannot accept rotation while another proxy operation is active")
        try:
            self._ensure_open()
            if (self._read_rotation_stage != "ACTIVATED" or not self._rotation_waiting_gateway
                    or self._rotation_activated_record is None or not self._retired_read_clients
                    or self._supervisor_rotation_channel is None or self._supervisor_attach_receipt is None
                    or self._rotation_integrity_fault):
                raise ShmProtocolError("activated rotation is not awaiting Gateway acceptance")
            transition = self._rotation_activated_record["transition_id"]
            raw, record = self._rotation_record(transition)
            if record.get("state") in ("ACTIVATED", "GATEWAY_VERIFYING"):
                return None
            if record.get("state") != "GATEWAY_VERIFIED":
                raise ShmProtocolError("Gateway rotation state is unconfirmed")
            for field, value in self._rotation_activated_record.items():
                if field != "state" and (field not in record or type(record[field]) is not type(value) or record[field] != value):
                    raise ShmProtocolError("activated rotation evidence changed")
            if (psutil is None or (self._gateway_acceptance_attempted and self._gateway_acceptance_proof is None)
                    or (self._rotation_open_prepare_attempted and not self._rotation_open_prepared)):
                raise ShmProtocolError("independent Gateway acceptance is unavailable or already attempted")
            gateway_pid, gateway_birth = record.get("gateway_pid"), record.get("gateway_create_time")
            if (type(gateway_pid) is not int or gateway_pid <= 0
                    or type(gateway_birth) not in (int, float) or not math.isfinite(gateway_birth) or gateway_birth <= 0):
                raise ShmProtocolError("Gateway process identity is unavailable")
            gateway = psutil.Process(gateway_pid)
            if (not gateway.is_running() or gateway.ppid() != os.getppid()
                    or gateway.create_time() != gateway_birth):
                raise ShmProtocolError("Gateway process identity changed")
            supervisor = self._supervisor_attach_receipt["supervisor"]
            if _windows_native_process_identity(os.getppid()) != supervisor:
                raise ShmProtocolError("rotation supervisor process identity changed")
            self._check_native_process_identity()
            binding = self._source_binding()
            evidence = record.get("gateway_evidence")
            expected = dict(contract="GATEWAY_READ_ROTATION_V1", transition_id=transition,
                old_read_session_id=record["old_read_session_id"], new_read_session_id=self._sessions["readonly"],
                execution_session_id=self._sessions["execution"], native_pid=self.expected_native_pid,
                native_creation_filetime=self._verified_process_identity["native_creation_filetime"],
                windows_session_id=self.expected_windows_session_id, gateway_pid=gateway_pid, gateway_create_time=gateway_birth)
            if (not isinstance(evidence, dict)
                    or any(type(evidence.get(key)) is not type(value) or evidence.get(key) != value for key, value in expected.items())
                    or binding is None or any(evidence.get(key) != binding for key in
                        ("read_client_binding", "order_client_binding", "profile_binding"))
                    or type(evidence.get("execution_cursor_before")) is not int
                    or evidence["execution_cursor_before"] < 0
                    or evidence.get("execution_cursor_after") != evidence["execution_cursor_before"]
                    or evidence.get("financial_and_event_state_preserved") is not True):
                raise ShmProtocolError("Gateway rotation receipt is inconsistent")
            if not self._gateway_acceptance_frames_ready(record):
                return None
            identity = dict(contract="SUPERVISOR_ROTATION_GATEWAY_ACCEPTANCE_V1", action="VERIFY", status="VERIFIED",
                transition_id=transition, record_sha256=hashlib.sha256(raw.encode("utf-8")).hexdigest(),
                supervisor_pid=os.getppid(), supervisor_creation_filetime=supervisor["native_creation_filetime"],
                gateway_pid=gateway_pid, gateway_create_time=gateway_birth, native_pid=self.expected_native_pid,
                native_creation_filetime=self._verified_process_identity["native_creation_filetime"],
                windows_session_id=self.expected_windows_session_id, execution_session_id=self._sessions["execution"],
                read_session_id=self._sessions["readonly"], rotation_read_only=True)
            if self._gateway_acceptance_proof is None:
                self._gateway_acceptance_attempted = True
                response = self._supervisor_rotation_channel.register(dict(contract=identity["contract"],
                                                                          action="VERIFY", transition_id=transition))
                if response == dict(contract=identity["contract"], action="VERIFY", status="UNAVAILABLE",
                                    transition_id=transition):
                    self._gateway_acceptance_attempted = False
                    return None
                when = response.get("verified_at_unix") if isinstance(response, dict) else None
                if (not isinstance(response, dict) or set(response) != set(identity) | {"verified_at_unix"}
                        or any(type(response[key]) is not type(value) or response[key] != value for key, value in identity.items())
                        or type(when) not in (int, float) or not math.isfinite(when) or not 0 < when <= time.time()):
                    raise ShmProtocolError("supervisor Gateway acceptance is unconfirmed")
                self._gateway_acceptance_proof = dict(raw=raw, receipt=copy.deepcopy(response))
            elif self._gateway_acceptance_proof["raw"] != raw:
                raise ShmProtocolError("verified Gateway transition changed")
            current_raw, _ = self._rotation_record(transition)
            if (current_raw != raw or _windows_native_process_identity(os.getppid()) != supervisor
                    or not gateway.is_running() or gateway.ppid() != os.getppid() or gateway.create_time() != gateway_birth):
                raise ShmProtocolError("Gateway acceptance identity changed")
            self._check_native_process_identity()
            if not self._gateway_acceptance_frames_ready(record):
                return None
            proxy_identity = _windows_native_process_identity(os.getpid())
            if (proxy_identity.get("native_pid") != os.getpid()
                    or proxy_identity.get("windows_session_id") != self.expected_windows_session_id
                    or type(proxy_identity.get("native_creation_filetime")) is not int
                    or proxy_identity["native_creation_filetime"] <= 0):
                raise ShmProtocolError("proxy process identity is unavailable")
            opening = dict(contract="SUPERVISOR_ROTATION_PROXY_ACCEPTANCE_V1", action="OPEN_PREPARE",
                transition_id=transition, record_sha256=identity["record_sha256"], proxy_pid=os.getpid(),
                proxy_creation_filetime=proxy_identity["native_creation_filetime"], native_pid=self.expected_native_pid,
                native_creation_filetime=self._verified_process_identity["native_creation_filetime"],
                windows_session_id=self.expected_windows_session_id, old_read_session_id=record["old_read_session_id"],
                read_session_id=self._sessions["readonly"], execution_session_id=self._sessions["execution"],
                old_request_id=record["old_request_id"], old_sequence=record["old_sequence"])
            if not self._rotation_open_prepared:
                self._rotation_open_prepare_attempted = True
                prepared = self._supervisor_rotation_channel.register(opening)
                expected_open = dict(contract=opening["contract"], action="OPEN_PREPARE", status="PREPARED",
                                     transition_id=transition, record_sha256=identity["record_sha256"])
                if prepared == dict(expected_open, status="UNAVAILABLE"):
                    self._rotation_open_prepare_attempted = False
                    return None
                if prepared != expected_open:
                    raise ShmProtocolError("supervisor read gate preparation is unconfirmed")
                self._rotation_open_prepared = True
            current_raw, _ = self._rotation_record(transition)
            if (current_raw != raw or _windows_native_process_identity(os.getppid()) != supervisor
                    or _windows_native_process_identity(os.getpid()) != proxy_identity
                    or not gateway.is_running() or gateway.ppid() != os.getppid() or gateway.create_time() != gateway_birth):
                raise ShmProtocolError("prepared read gate identity changed")
            self._check_native_process_identity()
            if not self._gateway_acceptance_frames_ready(record):
                return None
            if self.allow_order_methods:
                self._inspect_write_ledger()
                if self._source_identity["execution"].get("writes_blocked") is True:
                    self._block_writes("NATIVE_WRITE_RESULT_UNKNOWN")
                if not self._execution_cursor_gate_supported:
                    self._block_writes("EXECUTION_CURSOR_GATE_UNAVAILABLE")
                if not self._native_supervisor_gate_supported:
                    self._block_writes("NATIVE_SUPERVISOR_GATE_UNAVAILABLE")
            unresolved_reads = set(self._unconfirmed_read_ids["readonly"]) - {record["old_request_id"]}
            self._accepted_rotation_record = dict(transition_id=transition, raw=raw)
            self._accepted_rotation_records[transition] = raw
            self._unconfirmed_read_ids["readonly"].discard(record["old_request_id"])
            self._read_rotation_blocked = False
            self._rotation_waiting_gateway = False
            self._read_rotation_stage = "GATEWAY_VERIFIED"
            resolved = {"READ_ROTATION_UNCONFIRMED"}
            if (not unresolved_reads and self._route_faults["readonly"] is None
                    and self._retired_read_faults[-1].get("trigger_request_id") == record["old_request_id"]):
                resolved.add("READ_SOURCE_UNTRUSTED")
            self._remove_resolved_write_block_reasons(resolved)
            self._rotation_open_delivery = "UNKNOWN"
            try:
                committed = self._supervisor_rotation_channel.register(dict(opening, action="OPEN_COMMITTED"))
                expected_commit = dict(contract=opening["contract"], action="OPEN_COMMITTED", status="ACK",
                                       transition_id=transition, record_sha256=identity["record_sha256"])
                if committed != expected_commit:
                    raise ShmProtocolError("supervisor read gate commit is unconfirmed")
                acknowledged = self._supervisor_rotation_channel.register(dict(opening, action="OPEN_ACKNOWLEDGED"))
                expected_ack = dict(contract=opening["contract"], action="OPEN_ACKNOWLEDGED", status="ACKNOWLEDGED",
                                    transition_id=transition, record_sha256=identity["record_sha256"])
                if acknowledged != expected_ack:
                    raise ShmProtocolError("supervisor read gate acknowledgement is unconfirmed")
            except Exception:
                # 本地 OPEN 已发生，不能回滚或重发；manager 回执投递未知且不得进入下一代。
                return dict(status="READ_OPEN_DELIVERY_UNKNOWN", transition_id=transition,
                            writes_blocked=self._writes_blocked, automatic_retry=False)
            self._rotation_open_delivery = "CONFIRMED"
            self._rotation_open_acknowledged = True
            return dict(status="READ_ROTATION_ACCEPTED", transition_id=transition, writes_blocked=self._writes_blocked,
                        automatic_retry=False)
        finally:
            self._lock.release()
            self._poll_lock.release()

    def close_readonly_rotation(self):
        """单次封存已验收代；任何不明结果均保留记录和旧帧，不进入下一代。"""
        if not self._poll_lock.acquire(False):
            raise ShmBusyError("cannot close rotation while Redis polling is active")
        if not self._lock.acquire(False):
            self._poll_lock.release()
            raise ShmBusyError("cannot close rotation while another proxy operation is active")
        try:
            self._ensure_open()
            try:
                target = validate_execution_cursor_guard_target(self.execution_cursor_guard_target, self.account_id, self.environment)
            except ValueError as exc:
                raise ShmProtocolError("retirement target identity is invalid") from exc
            if (target is None or self._read_rotation_stage != "GATEWAY_VERIFIED" or self._accepted_rotation_record is None
                    or not self._rotation_open_acknowledged or self._rotation_open_delivery != "CONFIRMED"
                    or self._rotation_waiting_gateway or self._rotation_integrity_fault
                    or not self._retired_read_clients or self._supervisor_rotation_channel is None
                    or self._supervisor_attach_receipt is None):
                raise ShmProtocolError("accepted rotation is not eligible for retirement")
            transition = self._accepted_rotation_record["transition_id"]
            raw, accepted = self._rotation_record(transition)
            if (raw != self._accepted_rotation_record["raw"] or accepted.get("state") != "GATEWAY_VERIFIED"
                    or self._gateway_acceptance_proof is None or self._gateway_acceptance_proof["raw"] != raw):
                raise ShmProtocolError("accepted rotation original evidence changed")
            self._verify_full_rotation_history()
            digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()
            supervisor = self._supervisor_attach_receipt["supervisor"]
            if _windows_native_process_identity(os.getppid()) != supervisor:
                raise ShmProtocolError("retirement supervisor process identity changed")
            if not self._rotation_close_frames_ready(accepted):
                return None
            if self._rotation_close_ready_proof is None:
                if self._rotation_close_ready_attempted:
                    raise ShmProtocolError("rotation retirement readiness is unconfirmed; do not retry")
                request = dict(contract="SUPERVISOR_ROTATION_CLOSE_V1", action="READY",
                               transition_id=transition, record_sha256=digest)
                self._rotation_close_ready_attempted = True
                response = self._supervisor_rotation_channel.register(request)
                if response == dict(request, status="UNAVAILABLE"):
                    self._rotation_close_ready_attempted = False
                    return None
                if response != dict(request, status="READY"):
                    raise ShmProtocolError("supervisor retirement readiness is unconfirmed")
                self._rotation_close_ready_proof = copy.deepcopy(response)
            if self._rotation_close_pending_raw is None:
                if self._rotation_close_claim_attempted:
                    raise ShmProtocolError("rotation retirement claim is unconfirmed; do not retry")
                if not self._rotation_close_frames_ready(accepted):
                    return None
                pending = dict(accepted, state="CLOSE_PENDING", gateway_verified_sha256=digest,
                               close_nonce=uuid.uuid4().hex)
                encoded = json.dumps(pending, sort_keys=True, allow_nan=False, separators=(",", ":"))
                self._rotation_close_claim_attempted = True
                self._read_rotation_blocked = True
                self._block_writes("READ_ROTATION_UNCONFIRMED")
                try:
                    history = [[prior, previous, self._rotation_closed_records.get(prior, False)]
                               for prior, previous in self._accepted_rotation_records.items()]
                    claimed = self.transport.redis.eval(_CLAIM_ROTATION_CLOSE, 2, self._read_rotation_key,
                        self._read_rotation_close_key, transition, raw, encoded,
                        json.dumps(history, ensure_ascii=True, separators=(",", ":")))
                    if claimed != 1 or self.transport.redis.ttl(self._read_rotation_close_key) != -1:
                        raise ShmProtocolError("rotation retirement persistence is unconfirmed")
                except Exception:
                    self._rotation_integrity_fault = True
                    raise
                self._rotation_close_record, self._rotation_close_pending_raw = pending, encoded
                self._rotation_close_pending_records[transition] = encoded
                capability = self._register_rotation_authorization(pending, "CLOSE")
                command = dict(contract="READ_ROTATION_CONTROL_V1", action="CLOSE", transition_id=transition,
                               nonce=pending["close_nonce"], capability=capability, gateway_verified_sha256=digest)
                try:
                    receipt = self._execution_rotation_control(command)
                finally:
                    capability = command = None
                expected = dict(contract="READ_ROTATION_CONTROL_V1", action="CLOSE", status="CLOSED",
                    transition_id=transition, account_id=self.account_id, environment=self.environment,
                    native_pid=self.expected_native_pid, native_creation_filetime=accepted["native_creation_filetime"],
                    windows_session_id=self.expected_windows_session_id, old_read_session_id=accepted["old_read_session_id"],
                    candidate_read_session_id=accepted["candidate_read_session_id"], execution_session_id=self._sessions["execution"],
                    old_request_id=accepted["old_request_id"], old_sequence=accepted["old_sequence"],
                    probe_request_id=accepted["probe_request_id"], probe_sequence=1, gateway_verified_sha256=digest)
                if (not isinstance(receipt, dict) or set(receipt) != set(expected)
                        or any(type(receipt[key]) is not type(value) or receipt[key] != value for key, value in expected.items())):
                    raise ShmProtocolError("native retirement receipt is unconfirmed")
                self._rotation_close_native_receipt = copy.deepcopy(receipt)
            elif self._rotation_close_native_receipt is None:
                raise ShmProtocolError("native retirement outcome is unconfirmed; do not retry")
            if not self._rotation_close_frames_ready(accepted):
                return None
            if self._rotation_close_closed_raw is None:
                if self._rotation_close_settle_attempted:
                    raise ShmProtocolError("rotation retirement settlement is unconfirmed; do not retry")
                closed = dict(self._rotation_close_record, state="CLOSED",
                              close_evidence=copy.deepcopy(self._rotation_close_native_receipt))
                encoded = json.dumps(closed, sort_keys=True, allow_nan=False, separators=(",", ":"))
                self._rotation_close_settle_attempted = True
                try:
                    settled = self.transport.redis.eval(_SETTLE_ROTATION_CLOSE, 2, self._read_rotation_key,
                        self._read_rotation_close_key, transition, raw, self._rotation_close_pending_raw, encoded)
                    if settled != 1 or self.transport.redis.ttl(self._read_rotation_close_key) != -1:
                        raise ShmProtocolError("rotation retirement settlement is unconfirmed")
                except Exception:
                    self._rotation_integrity_fault = True
                    raise
                self._rotation_close_closed_raw = encoded
                self._rotation_close_pending_records[transition] = encoded
            self._verify_full_rotation_history()
            closed_digest = hashlib.sha256(self._rotation_close_closed_raw.encode("utf-8")).hexdigest()
            proxy_identity = _windows_native_process_identity(os.getpid())
            if (proxy_identity.get("native_pid") != os.getpid()
                    or proxy_identity.get("windows_session_id") != self.expected_windows_session_id
                    or type(proxy_identity.get("native_creation_filetime")) is not int
                    or proxy_identity["native_creation_filetime"] <= 0):
                raise ShmProtocolError("retirement proxy process identity is unavailable")
            message = dict(contract="SUPERVISOR_ROTATION_CLOSE_V1", action="CLOSE_COMMITTED", transition_id=transition,
                record_sha256=digest, close_record_sha256=closed_digest, proxy_pid=os.getpid(),
                proxy_creation_filetime=proxy_identity["native_creation_filetime"], native_pid=self.expected_native_pid,
                native_creation_filetime=accepted["native_creation_filetime"], windows_session_id=self.expected_windows_session_id,
                old_read_session_id=accepted["old_read_session_id"], read_session_id=accepted["candidate_read_session_id"],
                execution_session_id=self._sessions["execution"], old_request_id=accepted["old_request_id"],
                old_sequence=accepted["old_sequence"])
            if not self._rotation_close_committed:
                if self._rotation_close_commit_attempted:
                    raise ShmProtocolError("rotation close delivery is unconfirmed; do not retry")
                self._rotation_close_commit_attempted = True
                response = self._supervisor_rotation_channel.register(message)
                expected = dict(contract=message["contract"], action="CLOSE_COMMITTED", status="ACK", transition_id=transition,
                                record_sha256=digest, close_record_sha256=closed_digest)
                if response != expected:
                    raise ShmProtocolError("rotation close delivery is unconfirmed")
                self._rotation_close_committed = True
            if not self._rotation_close_acknowledged:
                if self._rotation_close_ack_attempted:
                    raise ShmProtocolError("rotation close acknowledgement is unconfirmed; do not retry")
                self._rotation_close_ack_attempted = True
                response = self._supervisor_rotation_channel.register(dict(message, action="CLOSE_ACKNOWLEDGED"))
                expected = dict(contract=message["contract"], action="CLOSE_ACKNOWLEDGED", status="ACKNOWLEDGED",
                                transition_id=transition, record_sha256=digest, close_record_sha256=closed_digest)
                if response != expected:
                    raise ShmProtocolError("rotation close acknowledgement is unconfirmed")
                self._rotation_close_acknowledged = True
            if self._rotation_close_next_ready_proof is None:
                if self._rotation_close_next_ready_attempted:
                    raise ShmProtocolError("next rotation readiness is unconfirmed; do not retry")
                request = dict(contract=message["contract"], action="NEXT_READY", transition_id=transition,
                               record_sha256=digest, close_record_sha256=closed_digest)
                self._rotation_close_next_ready_attempted = True
                response = self._supervisor_rotation_channel.register(request)
                if response == dict(request, status="UNAVAILABLE"):
                    self._rotation_close_next_ready_attempted = False
                    return None
                if response != dict(request, status="READY"):
                    raise ShmProtocolError("next rotation readiness is unconfirmed")
                self._rotation_close_next_ready_proof = copy.deepcopy(response)
            if not self._rotation_close_frames_ready(accepted):
                # READY 已被监督器消费；留存原结果，不重复发送，等待本地帧可观察。
                return None
            self._verify_full_rotation_history()
            self._rotation_closed_records[transition] = self._rotation_close_closed_raw
            self._rotation_close_pending_records.pop(transition, None)
            self._read_rotation_blocked = False
            if not self._route_faults["readonly"] and not self._unconfirmed_read_ids["readonly"]:
                self._remove_resolved_write_block_reasons({"READ_ROTATION_UNCONFIRMED"})
            self._read_rotation_stage = None
            self._rotation_capability_attempted, self._rotation_capability = False, None
            self._read_rotation_candidate = self._rotation_committed_record = self._rotation_activated_record = None
            self._gateway_acceptance_attempted, self._gateway_acceptance_proof = False, None
            self._rotation_open_prepare_attempted, self._rotation_open_prepared = False, False
            self._rotation_close_ready_attempted, self._rotation_close_ready_proof = False, None
            self._rotation_close_claim_attempted, self._rotation_close_pending_raw, self._rotation_close_record = False, None, None
            self._rotation_close_native_receipt = self._rotation_close_closed_raw = None
            self._rotation_close_settle_attempted = self._rotation_close_commit_attempted = False
            self._rotation_close_committed = self._rotation_close_ack_attempted = self._rotation_close_acknowledged = False
            self._rotation_close_next_ready_attempted, self._rotation_close_next_ready_proof = False, None
            return dict(status="ROTATION_CLOSED", transition_id=transition, writes_blocked=self._writes_blocked,
                        automatic_retry=False)
        finally:
            self._lock.release()
            self._poll_lock.release()

    def _validate_identity_data(self, route, data, *, session_id=None):
        self._validate_environment(data, "native source")
        required = QUOTE_METHODS if route == "readonly" else EXECUTION_METHODS
        allowed_write = self.allow_order_methods if route == "execution" else False
        if (not isinstance(data, dict) or data.get("pong") is not True
                or data.get("account_id") != self.account_id
                or data.get("allow_order_methods") is not allowed_write
                or data.get("session_id") != (self._sessions[route] if session_id is None else session_id)
                or type(data.get("native_pid")) is not int or data["native_pid"] != self.expected_native_pid
                or not isinstance(data.get("capabilities"), list)
                or any(not isinstance(item, str) for item in data["capabilities"])
                or not required <= set(data["capabilities"])
                or (route == "execution" and data.get("transport") != "shm_rpc")
                or (allowed_write and not WRITE_METHODS <= set(data["capabilities"]))
                or (not allowed_write and bool(ALL_WRITE_METHODS & set(data["capabilities"])))):
            raise ShmProtocolError("explicit bridge identity/capabilities verification failed")
        if (ASYNC_WRITE_METHODS & set(data["capabilities"])
                and (not ASYNC_WRITE_METHODS <= set(data["capabilities"])
                     or data.get("async_submit_contract") != ASYNC_SUBMIT_CONTRACT
                     or type(data.get("writes_blocked")) is not bool
                     or not isinstance(data.get("write_block_reason"), str)
                     or bool(data["write_block_reason"]) is not data["writes_blocked"])):
            raise ShmProtocolError("explicit async submit contract verification failed")
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
        if mailbox.closed or (mailbox.session_id, mailbox.server_pid) != (self._sessions[route], self.expected_native_pid):
            raise ShmProtocolError("SHM client closed or identity changed")
        if client.poisoned:
            error = ShmProtocolError("SHM client has a previous unconfirmed request")
            error.previous_unconfirmed = True
            raise error
        return client

    def _validate_environment(self, value, context):
        if (not isinstance(value, dict) or
                (self.environment == "LIVE" and value.get("environment") != "LIVE") or
                ("environment" in value and value["environment"] != self.environment)):
            raise ShmProtocolError("%s environment mismatch or missing LIVE environment" % context)

    def _check_source_environments(self, data):
        if not isinstance(data, dict):
            return
        for key in ("source_evidence", "orders_source_evidence", "trades_source_evidence"):
            if key in data:
                self._validate_environment(data[key], key)

    def _check_response(self, request, response, session_id):
        self._validate_environment(response, "SHM response")
        self._check_source_environments(response)
        self._check_source_environments(response.get("data"))
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

    def attach_native_supervisor(self):
        """仅目标 LIVE 初次握手前接入一次；未知后不重发也不开放代理。"""
        if not self._lock.acquire(False):
            raise ShmProtocolError("proxy already has an in-flight operation")
        try:
            self._ensure_open()
            if (self._verified or self._supervisor_attach_attempted or not self.allow_order_methods
                    or self.execution_cursor_guard_target != {"gateway_id": "qmt-vm-001", "profile_id": "live-stock"}
                    or self.environment != "LIVE" or self.account_id != "8882152350"
                    or self._supervisor_write_gate is None):
                raise ShmProtocolError("single verified LIVE supervisor attachment is required")
            self._check_client("readonly")
            client = self._check_client("execution")

            def check_gate():
                try:
                    self._supervisor_write_gate.check()
                except SupervisorWriteGateError as exc:
                    # 启动核验阶段 identity-ready 允许暂未置位；其他异常不允许发送。
                    if exc.reason != "SUPERVISOR_IDENTITY_NOT_READY" or exc.permanent:
                        raise

            check_gate()
            native = _windows_native_process_identity(self.expected_native_pid)
            supervisor = _windows_native_process_identity(os.getppid())
            for actual, pid in ((native, self.expected_native_pid), (supervisor, os.getppid())):
                if (not isinstance(actual, dict) or actual.get("native_pid") != pid
                        or type(actual.get("native_creation_filetime")) is not int
                        or actual["native_creation_filetime"] <= 0
                        or type(actual.get("windows_session_id")) is not int
                        or actual["windows_session_id"] != self.expected_windows_session_id
                        or not isinstance(actual.get("executable"), str) or not actual["executable"]):
                    raise ShmProtocolError("native or supervisor process identity is unavailable")
            attach_id, request_id = uuid.uuid4().hex, uuid.uuid4().hex
            # 先持久 claim 原执行 SID；进程重启也不能重复未知的 ATTACH。
            self._supervisor_attach_attempted = True
            self._operation_phase = "NATIVE_SUPERVISOR_ATTACH"
            try:
                existing = self.transport.redis.hgetall(self._supervisor_attach_key)
                if existing and self.transport.redis.ttl(self._supervisor_attach_key) != -1:
                    raise ShmProtocolError("supervisor attachment ledger must not expire")
                pending = dict(schema_version=1, state="ATTACH_PENDING", account_id=self.account_id,
                    environment=self.environment, execution_session_id=self._sessions["execution"],
                    read_session_id=self._sessions["readonly"], native_pid=self.expected_native_pid,
                    native_creation_filetime=native["native_creation_filetime"],
                    windows_session_id=self.expected_windows_session_id, supervisor_pid=os.getppid(),
                    supervisor_creation_filetime=supervisor["native_creation_filetime"], attach_id=attach_id,
                    attach_request_id=request_id, automatic_retry=False)
                encoded = json.dumps(pending, sort_keys=True, allow_nan=False, separators=(",", ":"))
                claimed = self.transport.redis.eval(_CLAIM_WRITE, 1, self._supervisor_attach_key,
                    self._sessions["execution"], self.write_ledger_capacity, encoded)
                if (not isinstance(claimed, (list, tuple)) or len(claimed) != 2 or type(claimed[0]) is not int
                        or claimed[0] != 1 or decode_text(claimed[1]) != encoded
                        or self.transport.redis.ttl(self._supervisor_attach_key) != -1):
                    raise ShmProtocolError("supervisor attachment was previously attempted or ledger is unconfirmed")
                expected = dict(contract="QMT_NATIVE_SUPERVISOR_ATTACH_V1", attach_id=attach_id,
                    native_pid=self.expected_native_pid, native_creation_filetime=native["native_creation_filetime"],
                    windows_session_id=self.expected_windows_session_id,
                    execution_session_id=self._sessions["execution"], supervisor_pid=os.getppid(),
                    supervisor_creation_filetime=supervisor["native_creation_filetime"], automatic_retry=False)
                operation_deadline = time.monotonic() + SUPERVISOR_ATTACH_BUDGET_SECONDS

                def observe(field, rid):
                    check_gate()
                    remaining = min(self.timeout_seconds, operation_deadline - time.monotonic())
                    if remaining <= 0:
                        raise ShmProtocolError("native supervisor attachment exceeded its deadline")
                    deadline = time.time() + remaining
                    request = dict(schema_version=1, request_id=rid, account_id=self.account_id,
                        environment="LIVE", method="ping", read_expires_at_unix=deadline,
                        params={field: {"contract": "QMT_NATIVE_SUPERVISOR_ATTACH_V1", "attach_id": attach_id}})
                    response = client.send_request(request, timeout_seconds=remaining)
                    self._check_response(request, response, self._sessions["execution"])
                    if time.time() >= deadline or response.get("ok") is not True:
                        raise ShmProtocolError("native supervisor attachment response is unconfirmed")
                    data = response.get("data")
                    self._validate_identity_data("execution", data)
                    receipt = data.get(field)
                    if (not isinstance(receipt, dict) or set(receipt) != set(expected) | {"status"}
                            or any(type(receipt[key]) is not type(value) or receipt[key] != value
                                   for key, value in expected.items())
                            or receipt.get("status") not in ("PENDING", "ATTACHED", "FAILED")
                            or data.get("native_creation_filetime") != native["native_creation_filetime"]
                            or data.get("native_read_session_id") != self._sessions["readonly"]
                            or data.get("read_fault_isolation_contract") != READ_FAULT_ISOLATION_CONTRACT
                            or data.get("execution_event_guard_contract") != EXECUTION_CURSOR_WRITE_GATE_CONTRACT):
                        raise ShmProtocolError("native supervisor attachment receipt or capability is invalid")
                    return data, receipt

                data, receipt = observe("supervisor_attach", request_id)
                if receipt["status"] != "PENDING":
                    raise ShmProtocolError("initial supervisor attachment must be pending")
                while receipt["status"] == "PENDING":
                    remaining = operation_deadline - time.monotonic()
                    if remaining <= 0:
                        raise ShmProtocolError("native supervisor attachment exceeded its deadline")
                    time.sleep(min(0.05, remaining))
                    data, receipt = observe("supervisor_attach_status", uuid.uuid4().hex)
                if (receipt["status"] != "ATTACHED"
                        or data.get("supervisor_write_gate_contract") != SUPERVISOR_WRITE_GATE_CONTRACT
                        or data.get("supervisor_gate_epoch_contract") != SUPERVISOR_GATE_EPOCH_CONTRACT
                        or data.get("native_write_runtime_guard_attached") is not True
                        or type(data.get("native_callback_threading_verified")) is not bool
                        or type(data.get("native_write_runtime_guard_ready")) is not bool
                        or (data["native_write_runtime_guard_ready"] is True
                            and data["native_callback_threading_verified"] is not True)):
                    raise ShmProtocolError("native supervisor attachment receipt or capability is invalid")
                check_gate()
                if (_windows_native_process_identity(self.expected_native_pid) != native
                        or _windows_native_process_identity(os.getppid()) != supervisor):
                    raise ShmProtocolError("native or supervisor identity changed during attachment")
                settled = dict(pending, state="ATTACHED", receipt=copy.deepcopy(receipt))
                replacement = json.dumps(settled, sort_keys=True, allow_nan=False, separators=(",", ":"))
                if (self.transport.redis.eval(_SETTLE_WRITE, 1, self._supervisor_attach_key,
                        self._sessions["execution"], encoded, replacement) != 1
                        or self.transport.redis.ttl(self._supervisor_attach_key) != -1):
                    raise ShmProtocolError("supervisor attachment ledger settlement is unconfirmed")
                self._supervisor_attach_receipt = dict(receipt=copy.deepcopy(receipt), native=native, supervisor=supervisor)
                self._supervisor_attach_verified = True
                return copy.deepcopy(receipt)
            except Exception:
                self._block_writes("SUPERVISOR_ATTACH_UNCONFIRMED")
                self._seal("SUPERVISOR_ATTACH_UNCONFIRMED")
                raise
        finally:
            self._lock.release()

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
                    if self.environment == "LIVE":
                        request["environment"] = self.environment
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
                if self.execution_cursor_guard_target is not None:
                    self._capture_native_process_identity()
                    self._observe_native_read_state(result["execution"], request)
                    self._last_execution_success_at = time.monotonic()
                    self._last_execution_identity_success_at = self._last_execution_success_at
                if self._supervisor_attach_receipt is not None:
                    attached = self._supervisor_attach_receipt
                    if (self._verified_process_identity != attached["native"]
                            or _windows_native_process_identity(os.getppid()) != attached["supervisor"]
                            or result["execution"].get("native_creation_filetime")
                                != attached["receipt"]["native_creation_filetime"]):
                        raise ShmProtocolError("supervisor attachment does not match verified source identities")
                self._capabilities = frozenset(EXECUTION_METHODS | QUOTE_METHODS
                    | (MARKET_RPC_METHODS & set(result["readonly"]["capabilities"]))
                    | (ALL_WRITE_METHODS & set(result["execution"]["capabilities"])
                       if self.allow_order_methods else frozenset()))
                if result["execution"].get("writes_blocked") is True:
                    self._block_writes("NATIVE_WRITE_RESULT_UNKNOWN")
                self._execution_cursor_gate_supported = (
                    result["execution"].get("execution_event_guard_contract") == EXECUTION_CURSOR_WRITE_GATE_CONTRACT)
                if (self.execution_cursor_guard_target is not None and self.allow_order_methods
                        and not self._execution_cursor_gate_supported):
                    self._block_writes("EXECUTION_CURSOR_GATE_UNAVAILABLE")
                self._native_supervisor_gate_supported = (
                    self._supervisor_attach_verified
                    and result["execution"].get("supervisor_write_gate_contract") == SUPERVISOR_WRITE_GATE_CONTRACT
                    and result["execution"].get("supervisor_gate_epoch_contract") == SUPERVISOR_GATE_EPOCH_CONTRACT
                    and result["execution"].get("native_write_runtime_guard_attached") is True
                    and result["execution"].get("native_callback_threading_verified") is True
                    and result["execution"].get("native_write_runtime_guard_ready") is True)
                if (self.execution_cursor_guard_target is not None and self.allow_order_methods
                        and not self._native_supervisor_gate_supported):
                    self._block_writes("NATIVE_SUPERVISOR_GATE_UNAVAILABLE")
                if self.execution_cursor_guard_target is not None and self.allow_order_methods:
                    self._operation_phase = "READ_ROTATION_INSPECT"
                    self._inspect_read_rotation()
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
        self._validate_environment(request, "Redis request")
        self._validate_source_binding(request)
        method, params = request.get("method"), request.get("params")
        if method not in self._capabilities:
            raise ShmProtocolError("method is unavailable or not declared by the verified native source")
        if "position_read_route_contract" in request and (
                method != "get_positions" or request["position_read_route_contract"] != POSITION_READ_ROUTE_CONTRACT
                or not self._position_read_route_supported()):
            raise ShmProtocolError("position read route is not explicitly supported by the verified source")
        if not isinstance(params, dict) or request.get("use_formula", False) is not False:
            raise ShmProtocolError("params must be object and formula routing is unavailable")
        if (method == "ping" and ("read_rotation" in params or "read_rotation_control_status" in params or
                (self.execution_cursor_guard_target is not None
                 and any(field in params for field in ("supervisor_attach", "supervisor_attach_status",
                     "read_rotation_capability", "read_rotation_capability_status"))))):
            raise ShmProtocolError("native control is unavailable through Redis requests")
        if method in ALL_WRITE_METHODS and self.execution_cursor_guard_target is not None:
            guard = params.get("execution_event_guard")
            if (not self._execution_cursor_gate_supported or not isinstance(guard, dict)
                    or set(guard) != {"contract", "execution_session_id", "native_pid", "sequence"}
                    or guard.get("contract") != EXECUTION_CURSOR_WRITE_GATE_CONTRACT
                    or guard.get("execution_session_id") != self._sessions["execution"]
                    or type(guard.get("native_pid")) is not int or guard["native_pid"] != self.expected_native_pid
                    or type(guard.get("sequence")) is not int or guard["sequence"] < 0):
                raise ShmProtocolError("verified native execution cursor and durable event guard are required")
            epoch = params.get("supervisor_gate_epoch")
            if (not isinstance(epoch, dict) or set(epoch) != {"contract", "epoch"}
                    or epoch.get("contract") != SUPERVISOR_GATE_EPOCH_CONTRACT
                    or type(epoch.get("epoch")) is not int or not 1 <= epoch["epoch"] < 2 ** 64):
                raise ShmProtocolError("explicit supervisor gate epoch is required")
        account = params.get("account", self.account_id)
        if isinstance(account, dict):
            account = account.get("account_id", account.get("accountID", account.get("id")))
        if params.get("account_id", self.account_id) != self.account_id or account != self.account_id:
            raise ShmProtocolError("nested account mismatch")
        if method in QUOTE_METHODS:
            ShmReadOnlyRpcClient._quote_params(params, max_symbols=128 if self.environment == "LIVE" else 64)
        if method in MARKET_RPC_METHODS:
            ShmReadOnlyRpcClient._market_params(method, params)
            if method in {"get_market_data_ex_batch", "download_history_data_batch"}:
                source = self._source_identity.get("readonly", {})
                if (source.get("minute_history_batch_contract") != MINUTE_BATCH_CONTRACT
                        or source.get("minute_history_batch_max_symbols") != 50):
                    raise ShmProtocolError("explicit verified minute batch contract required")
        if method == "get_asset" and type(params.get("include_source_evidence", False)) is not bool:
            raise ShmProtocolError("include_source_evidence must be boolean")
        if method == "poll_execution_events" and (type(params.get("after_sequence")) is not int or params["after_sequence"] < 0):
            raise ShmProtocolError("bounded nonnegative execution event cursor required")
        if method in ASYNC_WRITE_METHODS:
            validate_async_submit_params(params)
        return self._request_fingerprint(request)

    def _validate_async_body_identity(self, request, data, binding):
        if (not isinstance(data, dict) or data.get("request_id") != request["request_id"]
                or data.get("client_order_id") != request["params"]["client_order_id"]
                or data.get("account_id") != self.account_id or data.get("environment") != self.environment
                or data.get("session_id") != binding["execution_session_id"]
                or type(data.get("native_pid")) is not int or data["native_pid"] != binding["native_pid"]
                or type(data.get("windows_session_id")) is not int
                or data["windows_session_id"] != binding["windows_session_id"]):
            raise ShmProtocolError("async response body does not bind the exact request and native source")

    def _validate_async_receipt(self, request, response, binding=None):
        """收据仅证明原生调用返回，绝不证明柜台受理或订单终态。"""
        binding = self._write_binding() if binding is None else binding
        self._check_response(request, response, binding["execution_session_id"])
        data = response.get("data")
        self._validate_async_body_identity(request, data, binding)
        if (response.get("ok") is not True or response.get("status") == "UNKNOWN"
                or not isinstance(data, dict) or data.get("status") != "PENDING_CONFIRMATION"
                or data.get("receipt_type") != "NATIVE_CALL_RETURNED"
                or data.get("async_submit_contract") != ASYNC_SUBMIT_CONTRACT
                or data.get("native_write_executed") is not True or data.get("broker_accepted") is not False
                or data.get("qmt_async_seq", "missing") is not None or data.get("order_sys_id") != ""
                or data.get("automatic_retry") is not False):
            raise ShmProtocolError("async native-call receipt is incomplete or conflicts with the exact request/source")
        return data

    def _validate_async_not_executed(self, request, response, binding=None):
        binding = self._write_binding() if binding is None else binding
        self._check_response(request, response, binding["execution_session_id"])
        data = response.get("data")
        self._validate_async_body_identity(request, data, binding)
        cursor_blocked = (data.get("no_write_contract") in (EXECUTION_CURSOR_NO_WRITE_CONTRACT, SUPERVISOR_GATE_NO_WRITE_CONTRACT)
                          and data.get("writes_blocked") is True
                          and isinstance(data.get("write_block_reason"), str) and bool(data["write_block_reason"]))
        if (response.get("ok") is not True or response.get("status") == "UNKNOWN"
                or data.get("status") != "BLOCKED" or data.get("native_write_executed") is not False
                or not (cursor_blocked or (data.get("writes_blocked") is False and data.get("write_block_reason") == ""))
                or data.get("broker_accepted", False) is not False or data.get("qmt_async_seq") is not None
                or data.get("order_sys_id", "") != ""
                or data.get("automatic_retry") is not False):
            raise ShmProtocolError("async rejection does not prove no native write")

    def _validate_cursor_not_executed(self, request, response, binding=None):
        """游标不一致仅在原生回调的完整同源收据明确无写时才可持久确认。"""
        binding = self._write_binding() if binding is None else binding
        self._check_response(request, response, binding["execution_session_id"])
        data = response.get("data")
        guard = request.get("params", {}).get("execution_event_guard")
        if (response.get("ok") is not True or response.get("status") == "UNKNOWN"
                or not isinstance(data, dict) or data.get("status") != "BLOCKED"
                or data.get("no_write_contract") != EXECUTION_CURSOR_NO_WRITE_CONTRACT
                or data.get("error") != "EXECUTION_EVENT_CURSOR_NOT_CURRENT"
                or data.get("request_id") != request.get("request_id")
                or data.get("account_id") != self.account_id or data.get("environment") != self.environment
                or data.get("session_id") != binding["execution_session_id"]
                or type(data.get("native_pid")) is not int or data["native_pid"] != binding["native_pid"]
                or type(data.get("windows_session_id")) is not int
                or data["windows_session_id"] != binding["windows_session_id"]
                or data.get("native_write_executed") is not False or data.get("automatic_retry") is not False
                or type(data.get("writes_blocked")) is not bool
                or not isinstance(data.get("write_block_reason"), str)
                or bool(data["write_block_reason"]) is not data["writes_blocked"]
                or not isinstance(guard, dict) or data.get("execution_event_guard") != guard
                or type(data.get("native_event_sequence")) is not int or data["native_event_sequence"] < 0
                or type(data.get("native_lost_sequence")) is not int
                or not 0 <= data["native_lost_sequence"] <= data["native_event_sequence"]
                or (data["native_lost_sequence"] > 0 and data["writes_blocked"] is not True)):
            raise ShmProtocolError("execution cursor rejection does not prove an exact no-write result")

    def _validate_epoch_not_executed(self, request, response, binding=None):
        """旧监督身份代次只在同源原生明确无写的收据下标记 NOT_EXECUTED。"""
        binding = self._write_binding() if binding is None else binding
        self._check_response(request, response, binding["execution_session_id"])
        data = response.get("data")
        epoch = request.get("params", {}).get("supervisor_gate_epoch")
        if (response.get("ok") is not True or response.get("status") == "UNKNOWN"
                or not isinstance(data, dict) or data.get("status") != "BLOCKED"
                or data.get("no_write_contract") != SUPERVISOR_GATE_NO_WRITE_CONTRACT
                or data.get("error") != "SUPERVISOR_GATE_EPOCH_MISMATCH"
                or data.get("request_id") != request.get("request_id")
                or data.get("account_id") != self.account_id or data.get("environment") != self.environment
                or data.get("session_id") != binding["execution_session_id"]
                or type(data.get("native_pid")) is not int or data["native_pid"] != binding["native_pid"]
                or type(data.get("windows_session_id")) is not int
                or data["windows_session_id"] != binding["windows_session_id"]
                or data.get("native_write_executed") is not False or data.get("automatic_retry") is not False
                or type(data.get("writes_blocked")) is not bool
                or not isinstance(data.get("write_block_reason"), str)
                or bool(data["write_block_reason"]) is not data["writes_blocked"]
                or not isinstance(epoch, dict) or data.get("supervisor_gate_epoch") != epoch
                or type(data.get("native_supervisor_gate_epoch")) is not int
                or not 1 <= data["native_supervisor_gate_epoch"] < 2 ** 64
                or data["native_supervisor_gate_epoch"] == epoch.get("epoch")):
            raise ShmProtocolError("supervisor epoch rejection does not prove an exact no-write result")

    def _async_record_request(self, record):
        fingerprint = json.loads(record["fingerprint"])
        if (not isinstance(fingerprint, list) or len(fingerprint) != 4
                or fingerprint[:3] != [1, self.account_id, "submit_order_async"]
                or type(fingerprint[0]) is not int or not isinstance(fingerprint[3], dict)
                or record.get("client_order_id") != fingerprint[3].get("client_order_id")
                or not isinstance(record.get("token"), str) or not record["token"]
                or record.get("automatic_retry") is not False
                or type(record.get("claimed_at_unix")) not in (int, float)
                or not math.isfinite(record["claimed_at_unix"]) or record["claimed_at_unix"] <= 0):
            raise ShmProtocolError("persistent async pending identity is incomplete")
        binding = record["binding"]
        if (type(binding.get("native_pid")) is not int or binding["native_pid"] <= 0
                or type(binding.get("windows_session_id")) is not int or binding["windows_session_id"] < 0
                or any(not isinstance(binding.get(key), str) or not binding[key]
                       for key in ("read_session_id", "execution_session_id"))
                or binding.get("allow_order_methods") is not True or binding.get("runtime_policy") not in WRITE_POLICIES):
            raise ShmProtocolError("persistent async source binding is incomplete")
        response = record.get("response")
        source = response.get("source_binding") if isinstance(response, dict) else None
        keys = ("read_session_id", "execution_session_id", "native_pid", "windows_session_id")
        if (not isinstance(source, dict) or set(source) != set(keys)
                or any(type(source[key]) is not type(binding[key]) or source[key] != binding[key] for key in keys)):
            raise ShmProtocolError("persistent async response source differs from its exact claim")
        validate_async_submit_params(fingerprint[3])
        return dict(schema_version=1, request_id=record["request_id"],
                    account_id=self.account_id, method="submit_order_async", params=fingerprint[3])

    def _validate_async_record_binding(self, record):
        if (record["binding"] != self._write_binding()
                or not ASYNC_WRITE_METHODS <= self._capabilities):
            self._block_writes("PERSISTED_ASYNC_SOURCE_CHANGED")

    def _validate_pending_record(self, record):
        self._validate_async_receipt(self._async_record_request(record), record.get("response"), record["binding"])
        self._validate_async_record_binding(record)

    def _validate_not_executed_record(self, record):
        fingerprint = json.loads(record["fingerprint"])
        if fingerprint[2] in ASYNC_WRITE_METHODS:
            request = self._async_record_request(record)
            self._validate_async_not_executed(request, record.get("response"), record["binding"])
            self._validate_async_record_binding(record)
            if record["response"].get("data", {}).get("no_write_contract") is None:
                return
        else:
            request = dict(schema_version=1, request_id=record["request_id"], account_id=self.account_id,
                           method=fingerprint[2], params=fingerprint[3])
        data = record["response"]["data"]
        if data.get("no_write_contract") == EXECUTION_CURSOR_NO_WRITE_CONTRACT:
            self._validate_cursor_not_executed(request, record.get("response"), record["binding"])
            if data["native_lost_sequence"] > 0 or data.get("writes_blocked") is True:
                self._block_writes("EXECUTION_EVENT_GAP")
        elif data.get("no_write_contract") == SUPERVISOR_GATE_NO_WRITE_CONTRACT:
            self._validate_epoch_not_executed(request, record.get("response"), record["binding"])
            if data.get("writes_blocked") is True:
                self._block_writes("SUPERVISOR_WRITE_BLOCKED")
        else:
            raise ShmProtocolError("persisted no-write contract is unsupported")

    def _write_binding(self):
        binding = dict(account_id=self.account_id, native_pid=self.expected_native_pid,
                    read_session_id=self._sessions["readonly"],
                    execution_session_id=self._sessions["execution"],
                    windows_session_id=self._source_binding()["windows_session_id"],
                    runtime_policy=self.runtime_policy, allow_order_methods=self.allow_order_methods)
        if self.environment == "LIVE":
            binding["environment"] = self.environment
        return binding

    def _claim_write(self, request, fingerprint):
        """原生调用前原子占用永不过期 RID；失败或已占用均不会再次原生写入。"""
        record = dict(request_id=request["request_id"], fingerprint=fingerprint,
                      binding=self._write_binding(), state="CLAIMED", token=uuid.uuid4().hex,
                      claimed_at_unix=time.time(), automatic_retry=False)
        asynchronous = request["method"] in ASYNC_WRITE_METHODS
        if asynchronous:
            record["client_order_id"] = request["params"]["client_order_id"]
        encoded = json.dumps(record, sort_keys=True, allow_nan=False, separators=(",", ":"))
        try:
            if asynchronous:
                claimed = self.transport.redis.eval(_CLAIM_ASYNC_WRITE, 2, self._write_ledger_key,
                    self._async_cid_key, request["request_id"], self.write_ledger_capacity, encoded,
                    record["client_order_id"])
            else:
                claimed = self.transport.redis.eval(_CLAIM_WRITE, 1, self._write_ledger_key,
                    request["request_id"], self.write_ledger_capacity, encoded)
            if (not isinstance(claimed, (list, tuple)) or len(claimed) != 2
                    or type(claimed[0]) is not int or claimed[0] not in ((-2, -1, 0, 1) if asynchronous else (-1, 0, 1))):
                raise ShmProtocolError("write reservation returned an invalid result")
            if claimed[0] == -2:
                prior_rid = decode_text(claimed[1])
                self._request_id(dict(request_id=prior_rid))
                prior = json.loads(decode_text(self.transport.redis.hget(self._write_ledger_key, prior_rid)))
                if (not isinstance(prior, dict) or prior.get("request_id") != prior_rid
                        or prior.get("client_order_id") != record["client_order_id"]):
                    raise ShmProtocolError("persistent async CID index has no exact original request")
                reconciled = prior.get("state") == "RECONCILED"
                if reconciled:
                    self._validate_reconciled_record(prior)
                conflict = (prior.get("fingerprint") != fingerprint
                            or not reconciled and prior.get("binding") != record["binding"])
                if conflict:
                    self._block_writes("ASYNC_IDENTITY_CONFLICT")
                elif prior.get("state") == "SUBMITTED_PENDING":
                    self._validate_pending_record(prior)
                elif prior.get("state") == "NOT_EXECUTED":
                    self._validate_not_executed_record(prior)
                elif not reconciled:
                    self._block_writes("PERSISTED_WRITE_UNCONFIRMED")
                unknown = conflict or prior.get("state") not in ("SUBMITTED_PENDING", "NOT_EXECUTED", "RECONCILED")
                response = self._response(request, "client_order_id was already claimed; inspect the original request",
                    "ASYNC_CLIENT_ORDER_ID_CONFLICT" if conflict else "ASYNC_CLIENT_ORDER_ID_ALREADY_CLAIMED",
                    unknown=unknown)
                response["data"] = dict(client_order_id=record["client_order_id"], original_request_id=prior_rid,
                                        native_write_executed=False, automatic_retry=False)
                return None, response
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
            binding_matches = isinstance(prior, dict) and prior.get("binding") == record["binding"]
            if asynchronous and isinstance(prior, dict) and prior.get("state") == "RECONCILED":
                self._validate_reconciled_record(prior)
                transition = prior["reconciliation"].get("source_transition")
                evidence_binding = self._validate_source_transition(transition, prior["binding"])
                binding_matches = evidence_binding == record["binding"]
            if (not isinstance(prior, dict) or prior.get("request_id") != request["request_id"]
                    or prior.get("fingerprint") != fingerprint or not binding_matches):
                raise ShmProtocolError("persisted write RID has conflicting parameters or source binding")
            replay = prior.get("reconciled_response") if prior.get("state") == "RECONCILED" else prior.get("response")
            if prior.get("state") == "SUBMITTED_PENDING":
                self._validate_pending_record(prior)
                return None, copy.deepcopy(replay)
            if prior.get("state") == "NOT_EXECUTED":
                self._validate_not_executed_record(prior)
                return None, copy.deepcopy(replay)
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
        if self.transport.redis.eval(_SETTLE_WRITE, 1, self._write_ledger_key,
                                     request["request_id"], reservation, encoded) != 1:
            raise ShmProtocolError("write settlement identity changed; do not resend")
        return encoded

    @staticmethod
    def _validate_source_transition(transition, original_binding, evidence_binding=None):
        if transition is None:
            return original_binding
        required = {"account_id", "environment", "runtime_policy", "allow_order_methods",
                    "read_session_id", "execution_session_id", "native_pid", "windows_session_id"}
        if not isinstance(transition, dict) or set(transition) != {"original_binding", "evidence_binding"}:
            raise ShmProtocolError("explicit original/evidence source bindings are required")
        old, new = transition["original_binding"], transition["evidence_binding"]
        for binding in (old, new):
            if (not isinstance(binding, dict) or set(binding) != required
                    or not isinstance(binding.get("account_id"), str) or binding.get("environment") != "LIVE"
                    or binding.get("runtime_policy") != "PERSISTENT_LIVE_READ_WRITE"
                    or binding.get("allow_order_methods") is not True
                    or type(binding.get("native_pid")) is not int or binding["native_pid"] <= 0
                    or type(binding.get("windows_session_id")) is not int or binding["windows_session_id"] < 0
                    or any(not isinstance(binding.get(field), str) or not binding[field]
                           for field in ("read_session_id", "execution_session_id"))
                    or binding["read_session_id"] == binding["execution_session_id"]):
                raise ShmProtocolError("source transition requires two complete explicit LIVE bindings")
            try:
                validate_account_policy(binding["account_id"], binding["environment"], binding["runtime_policy"])
            except ValueError as exc:
                raise ShmProtocolError("source transition requires the original explicit LIVE account") from exc
        changing = {"read_session_id", "execution_session_id", "native_pid"}
        if (old != original_binding or old == new
                or {old["read_session_id"], old["execution_session_id"]}
                    & {new["read_session_id"], new["execution_session_id"]}
                or any(type(old[field]) is not type(new[field]) or old[field] != new[field]
                       for field in required - changing)
                or (evidence_binding is not None and new != evidence_binding)):
            raise ShmProtocolError("only explicit SID/PID changes on the original LIVE target may be reconciled")
        return new

    def _reconciliation_record(self, request_id, expected_record=None, source_transition=None):
        """只读取精确 RID；旧请求、来源、原 UNKNOWN 必须完整保留。"""
        self._request_id({"request_id": request_id})
        encoded = self.transport.redis.hget(self._write_ledger_key, request_id)
        if encoded is None:
            raise ShmProtocolError("the exact write record does not exist")
        encoded = decode_text(encoded)
        if expected_record is not None and encoded != expected_record:
            raise ShmProtocolError("write record changed since the explicit preview")
        record = json.loads(encoded)
        if (not isinstance(record, dict) or record.get("request_id") != request_id
                or record.get("state") != "UNKNOWN" or not isinstance(record.get("binding"), dict)
                or not isinstance(record.get("token"), str) or not record["token"]
                or not isinstance(record.get("response"), dict)):
            raise ShmProtocolError("only the exact original UNKNOWN record may be reconciled")
        original = json.loads(record.get("fingerprint", ""))
        if (not isinstance(original, list) or len(original) != 4 or type(original[0]) is not int
                or original[:2] != [1, self.account_id]
                or not isinstance(original[2], str) or original[2] not in ALL_WRITE_METHODS
                or not isinstance(original[3], dict)):
            raise ShmProtocolError("single submit/cancel fingerprint required; batch recovery is unavailable")
        if source_transition is not None:
            if original[2] not in ASYNC_WRITE_METHODS or self.environment != "LIVE":
                raise ShmProtocolError("cross-source reconciliation requires an explicit LIVE async submit")
            self._validate_source_transition(source_transition, record["binding"], self._write_binding())
        elif record["binding"] != self._write_binding():
            raise ShmProtocolError("source changed; same verified sources required without a supported explicit original/evidence transition")
        if original[2] in ASYNC_WRITE_METHODS:
            self._async_record_request(record)
            cid = validate_async_submit_params(original[3])
            if decode_text(self.transport.redis.hget(self._async_cid_key, cid)) != request_id:
                raise ShmProtocolError("the original async CID reservation is missing or changed")
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
        if method in ("submit_order", "submit_order_async"):
            if method == "submit_order_async":
                tag = validate_async_submit_params(params)
                if params.get("price_type", "LIMIT") not in (11, "11", "LIMIT"):
                    raise ShmProtocolError("async reconciliation requires the original explicit limit order")
            else:
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
        self._check_source_environments(data)
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
                or (method in ("submit_order", "submit_order_async")
                    and order_time < claimed_at - _BROKER_PROXY_CLOCK_TOLERANCE_SECONDS)
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
                    request_result="BROKER_ORDER_FOUND" if method in ("submit_order", "submit_order_async") else
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
        if self.environment == "LIVE":
            query["environment"] = self.environment
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
        transition = proof.get("source_transition") if isinstance(proof, dict) else None
        evidence_binding = self._validate_source_transition(transition, record["binding"])
        if (not isinstance(proof, dict) or proof.get("original_state") != "UNKNOWN"
                or proof.get("terminal_verified") is not True or not isinstance(original, dict)
                or record["binding"].get("account_id") != self.account_id
                or record.get("automatic_retry") is not False
                or not isinstance(record.get("token"), str) or not record["token"]
                or record["binding"].get("runtime_policy") not in WRITE_POLICIES
                or (self.environment == "LIVE" and record["binding"].get("environment") != "LIVE")
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
                or any(proof["source_binding"].get(field) != evidence_binding.get(field)
                       for field in ("read_session_id", "execution_session_id", "native_pid", "windows_session_id"))
                or not isinstance(proof.get("evidence_request_id"), str) or not proof["evidence_request_id"]
                or type(proof.get("evidence_sequence")) is not int or proof["evidence_sequence"] <= 0):
            raise ShmProtocolError("persistent reconciliation has incomplete original result/source/evidence")
        fingerprint = json.loads(record["fingerprint"])
        if (not isinstance(fingerprint, list) or len(fingerprint) != 4
                or type(fingerprint[0]) is not int or fingerprint[:2] != [1, self.account_id]
                or not isinstance(fingerprint[2], str) or fingerprint[2] not in ALL_WRITE_METHODS
                or not isinstance(fingerprint[3], dict)
                or original.get("method") != fingerprint[2]):
            raise ShmProtocolError("persistent reconciliation original fingerprint is invalid")
        if transition is not None and fingerprint[2] not in ASYNC_WRITE_METHODS:
            raise ShmProtocolError("cross-source proof is limited to an exact LIVE async submit")
        if fingerprint[2] in ASYNC_WRITE_METHODS:
            self._async_record_request(record)
            if (not isinstance(proof.get("reason"), str) or not proof["reason"].strip()
                    or len(proof["reason"]) > 1024):
                raise ShmProtocolError("async reconciliation requires its explicit operator reason")
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
                or replay.get("session_id") != evidence_binding.get("execution_session_id")):
            raise ShmProtocolError("persistent reconciliation replay differs from saved facts")
        self._check_response(dict(schema_version=1, request_id=record["request_id"], account_id=self.account_id,
                                  method=fingerprint[2]), replay, evidence_binding["execution_session_id"])

    def reconcile_unknown_write(self, request_id, order_sys_id, order_identity, *,
                                expected_record=None, operator=None, reason=None, source_transition=None, apply=False):
        """显式单 RID 核对。默认只读预览；apply 仅 CAS 原账本，不发业务消息。

        终态核对不把原提交改为 CONFIRMED，也不宣称 OMS 回报已经落地。
        """
        if not self._lock.acquire(False):
            raise ShmProtocolError("proxy already has an in-flight operation")
        try:
            self._ensure_open()
            if not self._verified or not self.allow_order_methods:
                raise ShmProtocolError("verified account read/write sources are required")
            if apply and self._read_rotation_blocked:
                raise ShmProtocolError("read rotation remains review-required; write reconciliation is blocked")
            if type(apply) is not bool or (apply and (not isinstance(expected_record, str) or not expected_record
                    or not isinstance(operator, str) or not operator.strip() or len(operator) > 128)):
                raise ShmProtocolError("apply requires exact previewed record and explicit operator")
            encoded, record, method, params = self._reconciliation_record(request_id, expected_record, source_transition)
            asynchronous = method in ASYNC_WRITE_METHODS
            if ((reason is not None and (not isinstance(reason, str) or not reason.strip() or len(reason) > 1024))
                    or (apply and asynchronous and reason is None)):
                raise ShmProtocolError("async apply requires an explicit operator reason within 1024 characters")
            proof = self._terminal_reconciliation_proof(method, params, order_sys_id, order_identity,
                                                        record.get("claimed_at_unix"))
            if source_transition is not None:
                proof["source_transition"] = copy.deepcopy(source_transition)
            if reason is not None:
                proof["reason"] = reason.strip()
            if decode_text(self.transport.redis.hget(self._write_ledger_key, request_id)) != encoded:
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
            if asynchronous:
                self._block_writes("OPERATOR_WRITE_DISABLED")
            try:
                if asynchronous:
                    committed = self.transport.redis.eval(_RECONCILE_ASYNC_WRITE, 2, self._write_ledger_key,
                        self._async_cid_key, request_id, encoded, replacement, record["client_order_id"])
                else:
                    committed = self.transport.redis.eval(_SETTLE_WRITE, 1, self._write_ledger_key, request_id, encoded, replacement)
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
                if self.execution_cursor_guard_target is not None:
                    self._remove_resolved_write_block_reasons({"PERSISTED_WRITE_UNCONFIRMED", "NATIVE_WRITE_RESULT_UNKNOWN"})
                else:
                    self._writes_blocked, self._write_block_reason = False, ""
            if self._operator_write_blocked:
                self._block_writes("OPERATOR_WRITE_DISABLED")
            if self._source_identity["execution"].get("writes_blocked") is True:
                self._block_writes("NATIVE_WRITE_RESULT_UNKNOWN")
            if self._source_write_block_reason:
                self._block_writes(self._source_write_block_reason)
            try:
                if decode_text(self.transport.redis.hget(self._write_ledger_key, request_id)) != replacement:
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
                    environment=self.environment, method=request.get("method"), ok=False, data=None, error=str(error), server_error=code,
                    handled_at=datetime.datetime.utcnow().isoformat(),
                    status="UNKNOWN" if unknown else "REJECTED", automatic_retry=False)
        binding = self._source_binding()
        if binding is not None:
            response["source_binding"] = binding
            if request.get("method") in self._capabilities:
                route = self._request_route(request)
                response["session_id"] = self._sessions[route]
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
            event_poll = request.get("method") == "poll_execution_events"
            received_monotonic = time.monotonic() if event_poll else None
            read_timing = ({"proxy_received_at_unix": time.time()}
                           if request.get("method") == "get_positions" or event_poll else None)
            if event_poll:
                read_timing.update(proxy_received_at_monotonic=received_monotonic,
                                   proxy_received_at_monotonic_after=time.monotonic())
            if read_timing is not None:
                self._request_context["read_timing"] = read_timing
            self._operation_phase = "REQUEST_VALIDATE"
            # 先拒绝无法安全寻址的请求；不写任意 custom 回复键。
            reply = self._reply_request(request)
            rid = request["request_id"]
            reservation = None
            write_settlement = None
            confirmed_no_write = False
            try:
                fingerprint = self._validate_request(request)
                route = self._request_route(request)
                if rid in self._retired_read_ids:
                    raise ShmProtocolError("original readonly request belongs to a retired session; do not resend")
                if route == "readonly" and self._rotation_waiting_gateway:
                    raise ShmProtocolError("candidate readonly source awaits independent Gateway acceptance")
                if route == "readonly" and self._accepted_rotation_record is not None:
                    try:
                        self._verify_accepted_rotation_history()
                        self._check_native_process_identity()
                    except Exception as exc:
                        self._rotation_integrity_fault = True
                        self._rotation_waiting_gateway = self._read_rotation_blocked = True
                        self._block_writes("READ_ROTATION_UNCONFIRMED")
                        raise ShmProtocolError("accepted readonly source identity is unconfirmed") from exc
                self._reclaim_reads()
                if rid in self._replays:
                    # 已确认结果可以在旧 read_expires 到期后返回原缓存；该
                    # 期限只控制原生执行，不刷新 handled_at 或重新查询 QMT。
                    prior_fp, prior = self._replays[rid]
                    if prior_fp != fingerprint:
                        if request["method"] in ASYNC_WRITE_METHODS:
                            self._block_writes("ASYNC_IDENTITY_CONFLICT")
                        raise ShmProtocolError("request_id semantic conflict")
                    try:
                        self._operation_phase = "SHM_IDENTITY_CHECK"
                        if self.execution_cursor_guard_target is None:
                            for checked_route in self._clients:
                                self._check_client(checked_route)
                        elif prior.get("status") != "UNKNOWN":
                            # UNKNOWN 已固定归属于原 SID/RID，不重发旧邮箱。
                            self._check_client(route)
                        response = copy.deepcopy(prior)
                    except Exception as exc:
                        if self.execution_cursor_guard_target is None:
                            response = self._response(request, exc, "SHM_IDENTITY_CHANGED", unknown=True)
                            self._seal("SHM_IDENTITY_CHANGED")
                        else:
                            self._record_error(exc)
                            state = "RESULT_UNKNOWN" if getattr(exc, "previous_unconfirmed", False) else "IDENTITY_OR_PROTOCOL_ERROR"
                            self._mark_route_fault(route, state, request, "NOT_SENT",
                                                   previous_unconfirmed=getattr(exc, "previous_unconfirmed", False) is True)
                            if route == "execution" and state == "IDENTITY_OR_PROTOCOL_ERROR":
                                self._seal("SHM_IDENTITY_OR_PROTOCOL_ERROR")
                            response = self._response(request, exc, "SHM_IDENTITY_OR_PROTOCOL_ERROR", unknown=True)
                            response["publication_state"] = "NOT_SENT"
                            response["publication_scope"] = "SHM"
                else:
                    is_write = request["method"] in ALL_WRITE_METHODS
                    self._operation_phase = "WRITE_GATE_CHECK" if is_write else "REQUEST_VALIDATE"
                    if is_write and self.execution_cursor_guard_target is not None:
                        # 其他代理或监督进程可能在握手后建立换代门；在占用写账本
                        # 或调用原生接口前再次读取。无法读取本身也必须停写。
                        self._operation_phase = "READ_ROTATION_INSPECT"
                        try:
                            self._inspect_read_rotation()
                        except Exception as exc:
                            self._record_error(exc)
                            self._rotation_integrity_fault = True
                            self._read_rotation_blocked = True
                            self._block_writes("READ_ROTATION_UNCONFIRMED")
                            raise ShmProtocolError("read rotation gate could not be confirmed") from exc
                        if self._read_rotation_blocked:
                            raise ShmProtocolError("read rotation remains review-required; writes are blocked")
                    if (is_write and self._writes_blocked
                            and self.transport.redis.hget(self._write_ledger_key, rid) is None):
                        raise ShmProtocolError("new writes are blocked: %s; inspect existing results" % self._write_block_reason)
                    if is_write and self.execution_cursor_guard_target is not None and self._supervisor_write_gate is None:
                        raise ShmProtocolError("supervisor write gate is unavailable")
                    if self.execution_cursor_guard_target is not None and self._route_faults[route] is not None:
                        raise ShmProtocolError("%s source is restricted: %s; do not resend" % (route, self._route_state(route)))
                    if self.runtime_policy == "BOUNDED" and self._processed_count >= self.max_requests:
                        self._seal("SESSION_CAPACITY_EXHAUSTED")
                        raise ShmProtocolError("session request capacity exhausted")
                    if (self.runtime_policy != "BOUNDED" and not is_write
                            and len(self._read_expiries) + (len(self._unconfirmed_read_ids[route])
                                if self.execution_cursor_guard_target is not None else 0) >= self.read_cache_capacity):
                        raise ShmProtocolError("retained read results exceed finite cache capacity")
                    now = time.time()
                    expires = request.get("read_expires_at_unix", now + self.timeout_seconds)
                    if type(expires) not in (int, float) or not math.isfinite(expires) or expires <= now:
                        raise ShmProtocolError("read request is expired or has an invalid deadline")
                    # 读取最多 15 秒、显式下载批次最多 60 秒；消费前已耗时
                    # 仍由调用方原始绝对截止扣除，其他方法保留原预算。
                    request_budget = self.timeout_seconds
                    if "read_expires_at_unix" in request:
                        if request["method"] in {"get_market_data_ex", "get_market_data_ex_batch"}:
                            request_budget = 15.0
                        elif request["method"] == "download_history_data_batch":
                            request_budget = 60.0
                    deadline = min(expires, now + request_budget)
                    if self.deadline_unix is not None:
                        deadline = min(deadline, self.deadline_unix)
                    forwarded = dict(schema_version=1, request_id=rid, account_id=self.account_id,
                                     method=request["method"], params=dict(request["params"]),
                                     read_expires_at_unix=deadline)
                    if "position_read_route_contract" in request:
                        # 旧帧换代核验必须能重建包含路由标记的原请求身份。
                        forwarded["position_read_route_contract"] = request["position_read_route_contract"]
                    if self.environment == "LIVE":
                        forwarded["environment"] = self.environment
                    fault_route = route
                    native_response = None
                    native_response_validated = False
                    gate_held = False
                    readonly_guard = None
                    try:
                        # 执行查询独立验证执行路；写入仍须验证原双源绑定。
                        self._operation_phase = "SHM_IDENTITY_CHECK"
                        routes_to_check = (("readonly", "execution") if is_write
                                           or self.execution_cursor_guard_target is None else (route,))
                        for checked_route in routes_to_check:
                            fault_route = checked_route
                            self._check_client(checked_route)
                        fault_route = route
                        if is_write and self.execution_cursor_guard_target is not None:
                            self._operation_phase = "SUPERVISOR_GATE_CHECK"
                            self._acquire_supervisor_write_gate()
                            gate_held = True
                            self._check_supervisor_write_gate(request["params"]["supervisor_gate_epoch"]["epoch"])
                        if is_write:
                            self._operation_phase = "WRITE_LEDGER_CLAIM"
                            reservation, prior_write = self._claim_write(request, fingerprint)
                        else:
                            prior_write = None
                        client = self._clients[route]
                        self._operation_phase = "SHM_PRE_FORWARD"
                        self._ensure_open()
                        if (route == "execution" and not is_write
                                and self.execution_cursor_guard_target == {"gateway_id": "qmt-vm-001", "profile_id": "live-stock"}
                                and self._route_faults["readonly"] is not None):
                            readonly_guard = getattr(self._clients["readonly"].mailbox, "region", None)
                            if readonly_guard is None or not readonly_guard.acquire(0):
                                readonly_guard = None
                                deferred = ShmBusyError("readonly evidence lock is unavailable; execution request not sent")
                                deferred.previous_unconfirmed = False
                                raise deferred
                            if not self._readonly_callback_finished(mailbox_locked=True):
                                deferred = ShmBusyError("readonly callback completion is unconfirmed; execution request not sent")
                                deferred.previous_unconfirmed = False
                                raise deferred
                        remaining = deadline - time.time()
                        if remaining <= 0:
                            raise ShmTimeoutError("read deadline expired before forwarding")
                        if gate_held:
                            self._check_supervisor_write_gate(request["params"]["supervisor_gate_epoch"]["epoch"])
                        self._operation_phase = "SHM_FORWARD"
                        if read_timing is not None:
                            if event_poll:
                                read_timing["proxy_shm_call_at_monotonic"] = time.monotonic()
                            read_timing["proxy_shm_call_at_unix"] = time.time()
                            if event_poll:
                                read_timing["proxy_shm_call_at_monotonic_after"] = time.monotonic()
                        if prior_write is not None:
                            response = prior_write
                        elif gate_held:
                            def release_after_publication():
                                nonlocal gate_held
                                self._check_supervisor_write_gate(request["params"]["supervisor_gate_epoch"]["epoch"])
                                self._supervisor_write_gate.release()
                                gate_held = False
                            response = client.send_request(forwarded, timeout_seconds=remaining,
                                                           after_publication=release_after_publication)
                            if gate_held:
                                raise ShmProtocolError("SHM publication callback did not release supervisor mutex")
                        else:
                            response = client.send_request(forwarded, timeout_seconds=remaining)
                        if prior_write is None:
                            native_response = response
                        self._operation_phase = "SHM_RESPONSE_VALIDATE"
                        if prior_write is None:
                            self._check_response(forwarded, response, self._sessions[route])
                            native_response_validated = True
                        response = copy.deepcopy(response)
                        if read_timing is not None:
                            read_timing.update(self._read_timing(response.get("read_timing"), event_poll))
                            response["read_timing"] = dict(read_timing)
                        binding = self._source_binding()
                        if binding is not None:
                            response["source_binding"] = binding
                        if time.time() >= deadline:
                            raise ShmTimeoutError("SHM response arrived after deadline")
                        data = response.get("data")
                        if request["method"] in ASYNC_WRITE_METHODS and prior_write is None:
                            if response.get("ok") is not True:
                                raise ShmProtocolError("async native execution lacks an explicit call-return or no-write receipt")
                            if isinstance(data, dict) and data.get("status") == "BLOCKED":
                                self._validate_async_not_executed(request, response)
                            elif not (isinstance(data, dict) and data.get("status") == "UNKNOWN"):
                                self._validate_async_receipt(request, response)
                        if (is_write and prior_write is None and isinstance(data, dict)
                                and ("no_write_contract" in data
                                     or data.get("error") in ("EXECUTION_EVENT_CURSOR_NOT_CURRENT",
                                                               "SUPERVISOR_GATE_EPOCH_MISMATCH"))):
                            if data.get("no_write_contract") == EXECUTION_CURSOR_NO_WRITE_CONTRACT:
                                self._validate_cursor_not_executed(request, response)
                                if data["native_lost_sequence"] > 0 or data.get("writes_blocked") is True:
                                    self._block_writes("EXECUTION_EVENT_GAP")
                            elif data.get("no_write_contract") == SUPERVISOR_GATE_NO_WRITE_CONTRACT:
                                self._validate_epoch_not_executed(request, response)
                                if data.get("writes_blocked") is True:
                                    self._block_writes("SUPERVISOR_WRITE_BLOCKED")
                            else:
                                raise ShmProtocolError("native no-write contract is unsupported")
                            confirmed_no_write = True
                        if is_write and isinstance(data, dict) and data.get("writes_blocked") is True:
                            self._block_writes("NATIVE_WRITE_RESULT_UNKNOWN")
                        # 登录回调尚未观测是已确认的业务未知，不能当作 SHM
                        # 请求结果未知；仅接受当前原生账户/会话的明确回读。
                        login_state_unknown = (
                            request["method"] == "get_trade_login_state" and response["ok"]
                            and isinstance(data, dict) and data.get("status") == "UNKNOWN"
                            and data.get("source") in ("native_account_callback", "native_account_query")
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
                                if self.execution_cursor_guard_target is None:
                                    self._seal("SHM_RESULT_UNKNOWN")
                                else:
                                    # 已确认的业务 UNKNOWN 不等于邮箱未确认。
                                    self._block_writes("READ_SOURCE_UNTRUSTED" if route == "readonly"
                                                       else "EXECUTION_READ_RESULT_UNKNOWN")
                        if (event_poll and response["ok"] is True and self.execution_cursor_guard_target is not None):
                            event_state = self._execution_event_state(response.get("data"), request)
                            if event_state == "UNKNOWN":
                                self._block_writes("EXECUTION_EVENT_STATE_UNKNOWN")
                                response.update(ok=False, status="UNKNOWN", error="execution event identity or cursor is unconfirmed",
                                                server_error="EXECUTION_EVENT_STATE_UNKNOWN", automatic_retry=False)
                            elif event_state == "GAP":
                                self._block_writes("EXECUTION_EVENT_GAP")
                        if request["method"] == "ping" and response["ok"]:
                            self._validate_identity_data("execution", response["data"])
                            if (self.execution_cursor_guard_target is not None and self.allow_order_methods
                                    and response["data"].get("execution_event_guard_contract")
                                    != EXECUTION_CURSOR_WRITE_GATE_CONTRACT):
                                self._execution_cursor_gate_supported = False
                                self._block_writes("EXECUTION_CURSOR_GATE_UNAVAILABLE")
                            if (self.execution_cursor_guard_target is not None and self.allow_order_methods
                                    and (response["data"].get("supervisor_write_gate_contract") != SUPERVISOR_WRITE_GATE_CONTRACT
                                         or response["data"].get("supervisor_gate_epoch_contract") != SUPERVISOR_GATE_EPOCH_CONTRACT
                                         or response["data"].get("native_write_runtime_guard_attached") is not True
                                         or response["data"].get("native_callback_threading_verified") is not True
                                         or response["data"].get("native_write_runtime_guard_ready") is not True)):
                                self._native_supervisor_gate_supported = False
                                self._block_writes("NATIVE_SUPERVISOR_GATE_UNAVAILABLE")
                            if (ASYNC_WRITE_METHODS <= self._capabilities
                                    and not ASYNC_WRITE_METHODS <= set(response["data"]["capabilities"])):
                                self._block_writes("NATIVE_ASYNC_CAPABILITY_CHANGED")
                            self._source_identity["execution"] = copy.deepcopy(response["data"])
                            if self.execution_cursor_guard_target is not None:
                                self._observe_native_read_state(response["data"], request)
                            if response["data"].get("writes_blocked") is True:
                                self._block_writes("NATIVE_WRITE_RESULT_UNKNOWN")
                            self._last_execution_success_at = time.monotonic()
                            self._last_execution_identity_success_at = self._last_execution_success_at
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
                        if native_response_validated:
                            if route == "execution":
                                self._last_execution_success_at = time.monotonic()
                            elif (request["method"] in PURE_READ_METHODS and response["ok"] is True
                                  and response.get("status") != "UNKNOWN"):
                                self._last_read_success_at = time.monotonic()
                    except Exception as exc:
                        if read_timing is not None:
                            read_timing.update(self._read_timing(getattr(exc, "read_timing", None), event_poll))
                        self._record_error(exc)
                        if self.execution_cursor_guard_target is None:
                            response = self._response(request, exc, "SHM_RESULT_UNCONFIRMED", unknown=True)
                            if read_timing is not None:
                                response["read_timing"] = dict(read_timing)
                            self._seal("SHM_RESULT_UNCONFIRMED")
                        else:
                            phase = self._operation_phase
                            previous_unconfirmed = getattr(exc, "previous_unconfirmed", None)
                            if previous_unconfirmed is not True and previous_unconfirmed is not False:
                                previous_unconfirmed = None
                            evidence_matches = (getattr(exc, "request_id", None) == rid
                                                and getattr(exc, "session_id", None) == self._sessions[route])
                            if phase == "SHM_FORWARD" and not evidence_matches:
                                previous_unconfirmed = None
                            # 仅接受与当前 RID、SID 绑定的明确未发送证据。
                            not_sent = (phase in ("SHM_IDENTITY_CHECK", "SUPERVISOR_GATE_CHECK", "SHM_PRE_FORWARD", "WRITE_LEDGER_CLAIM") or
                                        (phase == "SHM_FORWARD" and getattr(exc, "publication_state", None) == "NOT_SENT"
                                         and evidence_matches))
                            publication_state = "NOT_SENT" if not_sent else "UNKNOWN"
                            sequence = getattr(exc, "sequence", None) if evidence_matches else None
                            if native_response_validated:
                                sequence = native_response.get("sequence")
                            prior_state_unknown = phase == "SHM_FORWARD" and not_sent and previous_unconfirmed is None
                            protocol_fault = isinstance(exc, ShmProtocolError) and previous_unconfirmed is not True
                            if not self.stopped and (previous_unconfirmed is True or prior_state_unknown
                                                     or protocol_fault or not not_sent):
                                state = "IDENTITY_OR_PROTOCOL_ERROR" if protocol_fault else "RESULT_UNKNOWN"
                                self._mark_route_fault(fault_route, state, request, publication_state,
                                                       sequence=sequence, previous_unconfirmed=previous_unconfirmed)
                                if fault_route == "execution" and protocol_fault:
                                    self._seal("SHM_IDENTITY_OR_PROTOCOL_ERROR")
                            # 已占账本而缺少原生 NOT_EXECUTED 凭证时，保留 UNKNOWN。
                            unknown = (not not_sent or reservation is not None
                                       or (is_write and phase == "WRITE_LEDGER_CLAIM"))
                            if is_write and unknown:
                                self._block_writes("NATIVE_WRITE_RESULT_UNKNOWN")
                            if isinstance(exc, SupervisorWriteGateError) and exc.permanent:
                                self._block_writes(exc.reason)
                            code = ("SHM_IDENTITY_OR_PROTOCOL_ERROR" if protocol_fault else
                                    "SHM_NOT_SENT" if not_sent and not unknown else "SHM_RESULT_UNCONFIRMED")
                            response = self._response(request, exc, code, unknown=unknown)
                            response["publication_state"] = publication_state
                            response["publication_scope"] = "SHM"
                            response["previous_unconfirmed"] = previous_unconfirmed
                            if type(sequence) is int and sequence > 0 and previous_unconfirmed is not True:
                                response["sequence"] = sequence
                            if read_timing is not None:
                                response["read_timing"] = dict(read_timing)
                    finally:
                        if readonly_guard is not None:
                            try:
                                readonly_guard.release()
                            except Exception as exc:
                                self._record_error(exc)
                                self._block_writes("READ_SOURCE_UNTRUSTED")
                                self._seal("READ_EVIDENCE_LOCK_RELEASE_FAILED")
                                unknown = self._operation_phase == "SHM_FORWARD" or native_response is not None
                                response = self._response(request, exc, "READ_EVIDENCE_LOCK_RELEASE_FAILED", unknown=unknown)
                                response.update(publication_state="UNKNOWN" if unknown else "NOT_SENT", publication_scope="SHM")
                        if gate_held:
                            try:
                                self._supervisor_write_gate.release()
                            except Exception as exc:
                                self._record_error(exc)
                                self._block_writes("SUPERVISOR_GATE_RELEASE_FAILED")
                                self._seal("SUPERVISOR_GATE_RELEASE_FAILED")
                                if reservation is not None:
                                    response = self._response(request, exc, "SUPERVISOR_GATE_RELEASE_FAILED", unknown=True)
                                    response.update(publication_state="UNKNOWN", publication_scope="SHM")
                    if reservation is not None:
                        try:
                            self._operation_phase = "WRITE_LEDGER_SETTLE"
                            write_settlement = self._settle_write(request, reservation, response,
                                "UNKNOWN" if response.get("status") == "UNKNOWN" else "PUBLISH_PENDING")
                        except Exception as exc:
                            self._record_error(exc)
                            response = self._response(request, exc, "WRITE_LEDGER_SETTLEMENT_UNCONFIRMED", unknown=True)
                            self._seal("WRITE_LEDGER_SETTLEMENT_UNCONFIRMED")
                    duplicate_cid = (is_write and reservation is None and isinstance(response.get("data"), dict)
                                     and "original_request_id" in response["data"])
                    if not duplicate_cid:
                        self._replays[rid] = (fingerprint, copy.deepcopy(response))
                    self._processed_count += 1
                    if not is_write:
                        if response.get("status") == "UNKNOWN":
                            self._unconfirmed_read_ids[route].add(rid)
                        else:
                            self._read_expiries[rid] = deadline
            except (ShmProtocolError, ValueError, TypeError) as exc:
                response = self._response(request, exc, "PROXY_REQUEST_REJECTED",
                    unknown=request.get("method") in ASYNC_WRITE_METHODS
                    and self._write_block_reason == "ASYNC_IDENTITY_CONFLICT")
                if self.execution_cursor_guard_target is not None:
                    response.update(publication_state="NOT_SENT", publication_scope="SHM", previous_unconfirmed=None)
            if request["method"] in ASYNC_WRITE_METHODS and response.get("status") == "UNKNOWN":
                self._block_writes("NATIVE_WRITE_RESULT_UNKNOWN")
            binding = self._source_binding()
            if binding is not None:
                response["source_binding"] = binding
            if self.environment == "LIVE":
                response["environment"] = self.environment
            try:
                self._operation_phase = "REDIS_RESPONSE_PUBLISH"
                self.transport.send_response(reply, response)
                if write_settlement is not None and response.get("status") != "UNKNOWN":
                    # 先持久留下发布待确认，再 fanout，全部完成才晋级；
                    # 即使最后一次 Redis 写失联，重启也不能解除停写门。
                    self._operation_phase = "WRITE_LEDGER_CONFIRM"
                    state = ("NOT_EXECUTED" if confirmed_no_write else
                             "SUBMITTED_PENDING" if request["method"] in ASYNC_WRITE_METHODS
                             and isinstance(response.get("data"), dict)
                             and response["data"].get("status") == "PENDING_CONFIRMATION" else
                             "NOT_EXECUTED" if request["method"] in ASYNC_WRITE_METHODS else "CONFIRMED")
                    write_settlement = self._settle_write(request, write_settlement, response, state)
            except Exception as exc:
                self._record_error(exc)
                if request["method"] in ASYNC_WRITE_METHODS:
                    self._block_writes("REDIS_RESPONSE_PUBLISH_FAILED")
                if (not self.stopped and self.receive_policy == "QUEUE_ONLY"
                        and self.runtime_policy != "BOUNDED" and request["method"] in PURE_READ_METHODS
                        and isinstance(exc, (RedisConnectionError, RedisTimeoutError))):
                    # 原生结果已确认。仅保留此次响应与失败证据，不重发 fanout 或原生请求。
                    # 调用方仍按原截止失败；后续独立读取可以继续，事件游标由调用方确认后推进。
                    self._read_publish_failures += 1
                    self._redis_retry_at = time.monotonic() + 1.0
                    return response
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
        """空队列检查可恢复；破坏性接收或原生结果未确认仍封闭。"""
        if not self._poll_lock.acquire(False):
            raise ShmProtocolError("proxy already has an in-flight poll")
        try:
            self._ensure_open()
            if not self._verified:
                raise ShmProtocolError("explicit identity handshake required")
            if time.monotonic() < self._redis_retry_at:
                return None
            if self.runtime_policy == "BOUNDED" and self._processed_count >= self.max_requests:
                self._seal("SESSION_CAPACITY_EXHAUSTED")
                return None
            try:
                self._request_context = {}
                if self.receive_policy == "QUEUE_AND_PUBSUB":
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
                    if (self.receive_policy == "QUEUE_ONLY" and self.runtime_policy != "BOUNDED"
                            and isinstance(exc, (RedisConnectionError, RedisTimeoutError))):
                        self._record_error(exc)
                        self._idle_receive_timeouts += 1
                        self._consecutive_idle_timeouts += 1
                        self._redis_retry_at = time.monotonic() + 1.0
                        return None
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
            resources = list(self._clients.values())
            resources.extend(item for item in self._retired_read_clients
                             if all(item is not existing for existing in resources))
            if self._read_rotation_candidate is not None and all(
                    self._read_rotation_candidate is not item for item in resources):
                resources.append(self._read_rotation_candidate)
            if self._supervisor_write_gate is not None:
                resources.append(self._supervisor_write_gate)
            if self._supervisor_rotation_channel is not None:
                resources.append(self._supervisor_rotation_channel)
            if self._pubsub is not None:
                resources.append(self._pubsub)
            for resource in resources:
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
