# coding: utf-8
"""外置 Redis→双 SHM 候选入口；默认只校验配置，不建立连接。"""

import argparse
import ctypes
import inspect
import json
import math
import os
from pathlib import Path
import re
import sys
import threading
import time

try:
    import redis
except ImportError:
    redis = None

try:
    from redis.backoff import NoBackoff
    from redis.retry import Retry
except ImportError:
    NoBackoff = None
    Retry = None

try:
    from qmt_runtime_guard import BoundedSynchronousPipeIO, FramedControlPipe, WindowsPipeKernel
except ImportError:
    BoundedSynchronousPipeIO = FramedControlPipe = WindowsPipeKernel = None


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
os.environ.setdefault("BIGQMT_LOG_ENABLED", "0")

from bigqmt_signal_trader.redis_shm_proxy import (
    RECEIVE_POLICIES, RUNTIME_POLICIES, WRITE_POLICIES, InheritedSupervisorWriteGate, RedisShmReadOnlyProxy,
    _inspect_supervisor_handle_metadata, validate_account_policy, validate_execution_cursor_guard_target,
)
from bigqmt_signal_trader.transports.readonly_shm import (
    REQUEST_CAPACITY, REQUEST_OFFSET, ShmBusyError, ShmProtocolError, ShmReadOnlyClient, decode_payload, validate_name,
)
from bigqmt_signal_trader.transports.redis_transport import RedisTransport


class InheritedRotationChannel:
    """监督器继承的私有单向管道；未知后永久污染，不重复注册。"""

    def __init__(self, reader, writer, kernel, pipe):
        self.reader, self.writer, self.kernel, self.pipe = reader, writer, kernel, pipe
        self.lock, self.closed, self.poisoned = threading.Lock(), False, False

    @classmethod
    def from_environment(cls):
        if (os.name != "nt" or any(item is None for item in
                (BoundedSynchronousPipeIO, FramedControlPipe, WindowsPipeKernel))):
            raise RuntimeError("private supervisor rotation transport unavailable")
        raw = [os.environ.get("AQP_ROTATION_REPLY_READ_HANDLE"),
               os.environ.get("AQP_ROTATION_REQUEST_WRITE_HANDLE")]
        if any(not isinstance(value, str) or not value or not value.isascii() or not value.isdecimal() for value in raw):
            raise RuntimeError("private supervisor rotation handles unavailable")
        reader, writer = [int(value) for value in raw]
        if (reader <= 0 or writer <= 0 or reader == writer
                or max(reader, writer) >= 1 << (8 * ctypes.sizeof(ctypes.c_void_p))
                or tuple(_inspect_supervisor_handle_metadata((reader, writer))) !=
                   (("File", 0x00100001), ("File", 0x00100002))):
            raise RuntimeError("private supervisor rotation handle identity invalid")
        kernel = WindowsPipeKernel()
        pipe = FramedControlPipe(BoundedSynchronousPipeIO(kernel), reader, writer, timeout_seconds=5)
        return cls(reader, writer, kernel, pipe)

    def register(self, message):
        with self.lock:
            if self.closed or self.poisoned:
                raise RuntimeError("private supervisor rotation transport unavailable")
            try:
                self.pipe.write(message)
                return self.pipe.read()
            except BaseException:
                self.poisoned = True
                raise

    def close(self):
        if self.closed:
            return
        self.closed = True
        errors = []
        for handle in (self.reader, self.writer):
            try:
                self.kernel.close(handle)
            except Exception:
                errors.append(handle)
        if errors:
            raise RuntimeError("private supervisor rotation handles did not close")


class ReadonlyRotationDriver:
    """仅推进当前受监督来源的一次只读换代；任何未知操作均粘滞停止。"""

    def __init__(self, proxy):
        self.proxy = proxy
        self.stage = "IDLE"
        self.blocked = False
        self.error_type = None
        self.next_observation = 0.0

    def _old_frame_ready(self):
        proxy = self.proxy
        fault = proxy._route_faults["readonly"]
        if (not isinstance(fault, dict) or fault.get("state") != "RESULT_UNKNOWN"
                or fault.get("publication_state") != "UNKNOWN" or fault.get("previous_unconfirmed") is not False
                or type(fault.get("sequence")) is not int or fault["sequence"] <= 0
                or not isinstance(fault.get("trigger_request_id"), str)):
            return False
        mailbox = proxy._clients["readonly"].mailbox
        if mailbox.closed or not mailbox.region.acquire(0):
            return False
        try:
            header = mailbox._header()
            sequence = fault["sequence"]
            if header[4:7] != [sequence, sequence, sequence - 1]:
                return False
            if not 0 < header[7] <= REQUEST_CAPACITY:
                raise ShmProtocolError("original readonly request length is invalid")
            request = decode_payload(mailbox.region.read(REQUEST_OFFSET, header[7]))
            deadline = request.get("read_expires_at_unix") if isinstance(request, dict) else None
            if (not isinstance(request, dict) or request.get("request_id") != fault["trigger_request_id"]
                    or request.get("session_id") != proxy._sessions["readonly"]
                    or request.get("sequence") != sequence or type(deadline) not in (int, float)
                    or not math.isfinite(deadline) or deadline <= 0):
                raise ShmProtocolError("original readonly request identity is invalid")
            return deadline <= time.time()
        finally:
            mailbox.region.release()

    def step(self):
        proxy = self.proxy
        now = time.monotonic()
        if (self.blocked or proxy.stopped or now < self.next_observation
                or proxy.execution_cursor_guard_target != {"gateway_id": "qmt-vm-001", "profile_id": "live-stock"}
                or proxy.account_id != "8882152350" or proxy.environment != "LIVE"
                or proxy._supervisor_rotation_channel is None or proxy._supervisor_write_gate is None):
            return
        self.next_observation = now + 0.25
        try:
            if self.stage == "IDLE":
                if proxy._read_rotation_blocked or not self._old_frame_ready():
                    return
                result = proxy.attest_read_rotation_capability()
                if result is None:  # 监督器明确未登记，本次没有发原生 challenge。
                    return
                if result.get("status") != "VERIFIED":
                    raise ShmProtocolError("rotation capability was not independently verified")
                self.stage = "CAPABILITY"
                self.native_ready = result.get("native_rotation_ready") is True
            if self.stage == "CAPABILITY":
                proof = proxy._rotation_capability
                if (not isinstance(proof, dict) or not 0 <= time.monotonic() - proof.get("verified_at", float("-inf")) <= 20):
                    raise ShmProtocolError("rotation capability expired; do not repeat the challenge")
                if not self.native_ready:
                    # A cached ping from before this challenge, or a later non-ping
                    # execution operation, is not fresh capability evidence.
                    observed_at = proxy._last_execution_identity_success_at
                    if (type(observed_at) not in (int, float) or observed_at < proof["verified_at"]
                            or proxy._source_identity.get("execution", {}).get("read_rotation_authorization_ready") is not True):
                        return
                self.native_ready = True
                proxy.prepare_readonly_rotation()
                self.stage = "COMMITTED"
                return
            if self.stage == "COMMITTED":
                result = proxy.activate_readonly_rotation()
                if result is not None:
                    if result.get("status") != "ACTIVATED":
                        raise ShmProtocolError("rotation activation is unconfirmed")
                    self.stage = "ACTIVATED"
                return
            if self.stage == "ACTIVATED":
                result = proxy.accept_gateway_rotation()
                if result is not None:
                    if result.get("status") != "READ_ROTATION_ACCEPTED":
                        raise ShmProtocolError("rotation read gate delivery is unconfirmed")
                    self.stage = "GATEWAY_VERIFIED"
                return
            if self.stage == "GATEWAY_VERIFIED":
                result = proxy.close_readonly_rotation()
                if result is not None:
                    if result.get("status") != "ROTATION_CLOSED":
                        raise ShmProtocolError("rotation retirement is unconfirmed")
                    self.stage = "IDLE"
                return
        except ShmBusyError as exc:
            # 仅本地尚无状态变更的锁竞争可再次观察；任何已发阶段/管道不明都停止。
            local_busy = (not proxy._route_faults["execution"] and
                (self.stage == "IDLE" and not proxy._rotation_capability_attempted
                 or self.stage == "CAPABILITY" and proxy._read_rotation_stage is None and not proxy._read_rotation_blocked
                 or self.stage == "COMMITTED" and proxy._read_rotation_stage == "COMMITTED"
                 or self.stage in ("ACTIVATED", "GATEWAY_VERIFIED") and str(exc) in (
                    "cannot accept rotation while Redis polling is active", "cannot accept rotation while another proxy operation is active",
                    "cannot close rotation while Redis polling is active", "cannot close rotation while another proxy operation is active")))
            if local_busy:
                return
            self.blocked, self.error_type = True, type(exc).__name__
        except Exception as exc:
            self.blocked, self.error_type = True, type(exc).__name__


def _integer(value, name, minimum, maximum):
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValueError("%s must be an integer in [%s, %s]" % (name, minimum, maximum))
    return value


def _finite(value, name, minimum, maximum=None):
    if type(value) not in (int, float) or not math.isfinite(value) or value <= minimum or (maximum is not None and value > maximum):
        raise ValueError("%s must be finite and within its bounded range" % name)
    return float(value)


def validate_config(value, now_unix=None):
    """检查当次来源及本地 Redis 目标，不读取客户端或运行状态。"""
    if not isinstance(value, dict):
        raise ValueError("explicit account configuration required")
    policy = value.get("runtime_policy", "BOUNDED")
    if policy not in RUNTIME_POLICIES:
        raise ValueError("explicit supported runtime_policy required")
    environment = value.get("environment", "SIM")
    validate_account_policy(value.get("account_id"), environment, policy)
    if type(value.get("allow_order_methods")) is not bool or value["allow_order_methods"] is not (policy in WRITE_POLICIES):
        raise ValueError("allow_order_methods must explicitly match account runtime policy")
    result = dict(value)
    result["environment"] = environment
    result["runtime_policy"] = policy
    result["execution_cursor_guard_target"] = validate_execution_cursor_guard_target(
        value.get("execution_cursor_guard_target"), value["account_id"], environment)
    if result["execution_cursor_guard_target"] is not None:
        for field in ("gateway_id", "profile_id"):
            if field in value and value[field] != result["execution_cursor_guard_target"][field]:
                raise ValueError("execution cursor guard conflicts with explicit runner %s" % field)
    result["receive_policy"] = value.get("receive_policy", "QUEUE_AND_PUBSUB")
    if not isinstance(result["receive_policy"], str) or result["receive_policy"] not in RECEIVE_POLICIES:
        raise ValueError("explicit supported receive_policy required")
    result["live_write_authorized"] = value.get("live_write_authorized", False)
    if type(result["live_write_authorized"]) is not bool or (environment != "LIVE" and result["live_write_authorized"]):
        raise ValueError("live_write_authorized must be an explicit LIVE-only boolean")
    result["start_write_blocked"] = value.get("start_write_blocked", policy in WRITE_POLICIES)
    if type(result["start_write_blocked"]) is not bool:
        raise ValueError("start_write_blocked must be an explicit boolean")
    if environment == "LIVE" and not result["start_write_blocked"] and not result["live_write_authorized"]:
        raise ValueError("LIVE start_write_blocked=false requires explicit live_write_authorized=true")
    result["native_pid"] = _integer(value.get("native_pid"), "native_pid", 1, 0xFFFFFFFF)
    result["windows_session"] = _integer(value.get("windows_session"), "windows_session", 0, 0xFFFFFFFF)
    for field, prefix in (("read_name", "Local\\AQP_SHM_READONLY_"), ("execution_name", "Local\\AQP_SHM_TRADE_")):
        name = value.get(field)
        validate_name(name)
        if not name.startswith(prefix):
            raise ValueError("%s must use its independent mailbox prefix" % field)
    for field in ("read_session_id", "execution_session_id"):
        if not isinstance(value.get(field), str) or not re.fullmatch(r"[0-9a-f]{32}", value[field]):
            raise ValueError("%s must be an explicit native session ID" % field)
    if value["read_session_id"] == value["execution_session_id"]:
        raise ValueError("the two native sessions must be distinct")
    now = time.time() if now_unix is None else now_unix
    if policy == "BOUNDED":
        result["source_deadline_unix"] = _finite(value.get("source_deadline_unix"), "source_deadline_unix", now)
        result["max_seconds"] = _integer(value.get("max_seconds", 60), "max_seconds", 1, 900)
        result["max_requests"] = _integer(value.get("max_requests", 100), "max_requests", 1, 1000)
    else:
        if value.get("source_deadline_unix", "missing") is not None:
            raise ValueError("persistent source deadline must explicitly be null")
        result["source_deadline_unix"] = None
        result["max_seconds"] = _integer(value.get("max_seconds"), "max_seconds", 0, 0)
        result["max_requests"] = _integer(value.get("max_requests"), "max_requests", 0, 0)
    result["read_cache_capacity"] = _integer(value.get("read_cache_capacity", 1000), "read_cache_capacity", 1, 1000)
    result["write_ledger_capacity"] = _integer(value.get("write_ledger_capacity", 1000), "write_ledger_capacity", 1, 1000)
    result["timeout_seconds"] = _finite(value.get("timeout_seconds", 2.0), "timeout_seconds", 0, 60)
    target = value.get("redis")
    if not isinstance(target, dict) or set(target) - {"host", "port", "db", "username", "password"}:
        raise ValueError("an explicit bounded Redis target is required")
    if target.get("host") not in ("127.0.0.1", "localhost", "::1"):
        raise ValueError("the candidate only connects to the local Redis provider")
    target = dict(target)
    target["port"] = _integer(target.get("port"), "redis.port", 1, 65535)
    target["db"] = _integer(target.get("db"), "redis.db", 0, 15)
    for field in ("username", "password"):
        if target.get(field) is not None and not isinstance(target[field], str):
            raise ValueError("redis.%s must be a string or null" % field)
    result["redis"] = target
    return result


def windows_session_id(pid):
    if os.name != "nt":
        raise RuntimeError("explicit run requires Windows; dry-run works locally")
    session = ctypes.c_uint()
    if not ctypes.windll.kernel32.ProcessIdToSessionId(pid, ctypes.byref(session)):
        raise RuntimeError("Windows process session could not be confirmed")
    return session.value


def create_redis_client(target):
    if redis is None or Retry is None or NoBackoff is None:
        raise RuntimeError("the existing environment needs the project Redis optional dependency")
    # Direct constructor keeps this exact local target independent of URL/env fallbacks.
    kwargs = dict(target, socket_connect_timeout=1.0, socket_timeout=1.0, health_check_interval=15,
                  retry=Retry(NoBackoff(), 0), retry_on_timeout=False)
    if "protocol" in inspect.signature(redis.Redis.__init__).parameters:
        kwargs["protocol"] = 2
    return redis.Redis(**kwargs)


def _emit_ready(evidence):
    print(json.dumps(evidence, ensure_ascii=False, sort_keys=True), flush=True)


def _safe_message(message, target):
    value = str(message)
    for key in ("password", "username"):
        if target.get(key):
            value = value.replace(target[key], "<redacted>")
    return value[:2048]


def run_candidate(value, redis_factory=None, shm_factory=None, *, stop_event=None, ready_observer=None):
    """显式运行政策；仅空闲只读队列检查允许有限恢复，不重发业务、不删键。"""
    config = validate_config(value)
    if windows_session_id(os.getpid()) != config["windows_session"] or windows_session_id(config["native_pid"]) != config["windows_session"]:
        raise RuntimeError("consumer and native source must share the explicit Windows session")
    bounded = config["runtime_policy"] == "BOUNDED"
    parent_deadline = None
    if bounded:
        remaining = min(config["max_seconds"], config["source_deadline_unix"] - time.time())
        if remaining <= 2 * config["timeout_seconds"] + 2:
            raise RuntimeError("insufficient source/runner margin for two handshakes and exit; no connections created")
        parent_deadline = min(config["source_deadline_unix"], time.time() + config["max_seconds"])
    redis_client, proxy, clients, supervisor_gate, rotation_channel = None, None, [], None, None
    rotation_driver = None
    errors = []
    result = dict(status="FAILED", account_id=config["account_id"], environment=config["environment"], automatic_retry=False,
                  allow_order_methods=config["allow_order_methods"], runtime_policy=config["runtime_policy"],
                  start_write_blocked=config["start_write_blocked"],
                  receive_policy=config["receive_policy"],
                  live_write_authorized=config["live_write_authorized"],
                  writes_blocked=config["start_write_blocked"], write_ready=False,
                  write_block_reason="OPERATOR_WRITE_DISABLED" if config["start_write_blocked"] else "",
                  processed_requests=0, received_requests=0, full_duration_acceptance_proved=False)
    began = time.monotonic()
    phase = "REDIS_CLIENT_CREATE"
    try:
        redis_client = (redis_factory or create_redis_client)(config["redis"])
        transport = RedisTransport(redis_client, account_id=config["account_id"])
        factory = shm_factory or ShmReadOnlyClient
        phase = "SHM_READ_CLIENT_CREATE"
        clients.append(factory(config["read_name"], config["account_id"]))
        phase = "SHM_EXECUTION_CLIENT_CREATE"
        clients.append(factory(config["execution_name"], config["account_id"]))
        phase = "PROXY_CREATE"
        if config["execution_cursor_guard_target"] is not None and config["allow_order_methods"]:
            try:
                supervisor_gate = InheritedSupervisorWriteGate.from_environment()
            except Exception:
                # 来源仍可只读诊断；缺少可信继承句柄绝不放开写入。
                supervisor_gate = None
        if (supervisor_gate is not None and
                config["execution_cursor_guard_target"] == {"gateway_id": "qmt-vm-001", "profile_id": "live-stock"}):
            try:
                rotation_channel = InheritedRotationChannel.from_environment()
            except Exception:
                # 无私有授权管道时正常只读可继续，换代入口仍禁用。
                rotation_channel = None
        proxy = RedisShmReadOnlyProxy(
            redis_transport=transport, readonly_client=clients[0], execution_client=clients[1],
            account_id=config["account_id"], environment=config["environment"],
            live_write_authorized=config["live_write_authorized"],
            execution_cursor_guard_target=config["execution_cursor_guard_target"],
            supervisor_write_gate=supervisor_gate,
            supervisor_rotation_channel=rotation_channel,
            receive_policy=config["receive_policy"],
            start_write_blocked=config["start_write_blocked"], expected_readonly_session_id=config["read_session_id"],
            expected_execution_session_id=config["execution_session_id"],
            expected_native_pid=config["native_pid"], timeout_seconds=config["timeout_seconds"],
            max_requests=config["max_requests"], deadline_unix=parent_deadline,
            runtime_policy=config["runtime_policy"], allow_order_methods=config["allow_order_methods"],
            read_cache_capacity=config["read_cache_capacity"], write_ledger_capacity=config["write_ledger_capacity"],
            expected_windows_session_id=config["windows_session"],
        )
        if config["start_write_blocked"]:
            proxy._block_writes("OPERATOR_WRITE_DISABLED")
        if (config["execution_cursor_guard_target"] == {"gateway_id": "qmt-vm-001", "profile_id": "live-stock"}
                and config["allow_order_methods"] and supervisor_gate is not None):
            # manager 已先监听；只尝试一次，失败或超时不进入业务循环。
            phase = "NATIVE_SUPERVISOR_ATTACH"
            proxy.attach_native_supervisor()
        phase = "IDENTITY_VERIFY"
        proxy.verify_identity()
        if (config["execution_cursor_guard_target"] == {"gateway_id": "qmt-vm-001", "profile_id": "live-stock"}
                and supervisor_gate is not None and rotation_channel is not None):
            rotation_driver = ReadonlyRotationDriver(proxy)
        evidence = dict(proxy.identity_snapshot(), status="READY", automatic_retry=False)
        phase = "READY_OBSERVER"
        (ready_observer or _emit_ready)(evidence)
        stop_requested = False
        reported_error_at = None
        reported_rotation_block = False
        while not proxy.stopped:
            if stop_event is not None and stop_event.is_set():
                stop_requested = True
                break
            if bounded:
                remaining = min(config["max_seconds"] - (time.monotonic() - began),
                                config["source_deadline_unix"] - time.time())
                if (remaining <= config["timeout_seconds"] + 1
                        or proxy.processed_count >= config["max_requests"]
                        or result["received_requests"] >= config["max_requests"]):
                    break
            phase = "RECEIVE_LOOP"
            response = proxy.poll_once()
            if rotation_driver is not None:
                rotation_driver.step()
                if rotation_driver.blocked and not reported_rotation_block:
                    reported_rotation_block = True
                    print(json.dumps(dict(event="READ_ROTATION_DRIVER_BLOCKED", phase=rotation_driver.stage,
                                          error_type=rotation_driver.error_type, automatic_retry=False)), flush=True)
            diagnostic = proxy.diagnostic_snapshot()
            latest = diagnostic["last_error"]
            if latest and latest["error_at_unix"] != reported_error_at:
                reported_error_at = latest["error_at_unix"]
                print(json.dumps({"event": "PROXY_TRANSPORT_ERROR", **latest,
                    "error_message": _safe_message(latest["error_message"], config["redis"])}), flush=True)
            if response is None:
                time.sleep(min(0.02, remaining) if bounded else 0.02)
            else:
                result["received_requests"] += 1
        result.update(status="PROXY_STOPPED" if proxy.stopped else ("STOP_REQUESTED" if stop_requested else "BOUNDED_STOP"),
                      identity_verified=proxy.verified, processed_requests=proxy.processed_count,
                      stop_reason=proxy.stop_reason)
    except KeyboardInterrupt:
        result.update(status="STOP_REQUESTED", identity_verified=bool(proxy and proxy.verified),
                      processed_requests=proxy.processed_count if proxy else 0,
                      stop_reason="KEYBOARD_INTERRUPT")
    except Exception as exc:
        diagnostics = proxy.diagnostic_snapshot() if proxy is not None else {}
        result.update(status="FAILED", error_type=type(exc).__name__,
                      error_phase=diagnostics.get("operation_phase", phase) if phase in ("RECEIVE_LOOP", "IDENTITY_VERIFY") else phase,
                      error_message=_safe_message(exc, config["redis"]),
                      error_at_unix=time.time(), stop_reason=proxy.stop_reason if proxy else "",
                      identity_verified=bool(proxy and proxy.verified))
        if proxy is not None:
            result["processed_requests"] = proxy.processed_count
    finally:
        if proxy is not None:
            diagnostics = proxy.diagnostic_snapshot()
            for name in ("first_error", "last_error"):
                if diagnostics[name]:
                    diagnostics[name]["error_message"] = _safe_message(diagnostics[name]["error_message"], config["redis"])
            result["diagnostics"] = diagnostics
            if proxy.verified:
                snapshot = proxy.identity_snapshot()
                result.update({field: snapshot[field]
                               for field in ("writes_blocked", "write_ready", "write_block_reason")})
            elif config["execution_cursor_guard_target"] is not None and config["allow_order_methods"]:
                result.update(writes_blocked=True, write_ready=False,
                              write_block_reason=proxy._write_block_reason or "SOURCE_IDENTITY_UNVERIFIED")
            try:
                proxy.close()
            except Exception as exc:
                errors.append(type(exc).__name__)
        else:
            if config["execution_cursor_guard_target"] is not None and config["allow_order_methods"]:
                result.update(writes_blocked=True, write_ready=False, write_block_reason="SOURCE_IDENTITY_UNVERIFIED")
            for resource in (rotation_channel, supervisor_gate):
                if resource is not None:
                    try:
                        resource.close()
                    except Exception as exc:
                        errors.append(type(exc).__name__)
        for client in clients:
            try:
                client.close()
            except Exception as exc:
                errors.append(type(exc).__name__)
        if redis_client is not None:
            try:
                redis_client.connection_pool.disconnect()
            except Exception as exc:
                errors.append(type(exc).__name__)
        result.update(cleanup_errors=errors, resources_closed=not errors, runtime_running=False,
                      elapsed_seconds=round(time.monotonic() - began, 3))
        if errors:
            result["status"] = "UNKNOWN"
    return result


def validate_reconciliation_plan(value, *, apply=False):
    """只接受数据文件；原 UNKNOWN 原文由预览返回，禁止猜测或自动补写。"""
    allowed = {"request_id", "order_sys_id", "order_identity", "expected_record", "operator", "stopped_proxy_pid",
               "reason", "source_transition"}
    if not isinstance(value, dict) or set(value) - allowed:
        raise ValueError("an exact single-RID reconciliation JSON plan is required")
    RedisShmReadOnlyProxy._request_id(value)
    identity = value.get("order_identity")
    if (not isinstance(value.get("order_sys_id"), str) or not value["order_sys_id"]
            or not isinstance(identity, dict) or set(identity) != {
                "user_order_id", "stock_code", "action", "volume", "price"}
            or not isinstance(identity.get("user_order_id"), str) or not identity["user_order_id"]
            or not isinstance(identity.get("stock_code"), str) or not identity["stock_code"]
            or identity.get("action") not in ("BUY", "SELL")
            or type(identity.get("volume")) is not int or identity["volume"] <= 0
            or type(identity.get("price")) not in (int, float)
            or not math.isfinite(identity["price"]) or identity["price"] <= 0):
        raise ValueError("complete explicit original order identity is required")
    if apply and (not isinstance(value.get("expected_record"), str) or not value["expected_record"]
                  or not isinstance(value.get("operator"), str) or not value["operator"].strip()
                  or len(value["operator"]) > 128):
        raise ValueError("apply requires the exact previewed record and an explicit operator")
    if "reason" in value and (not isinstance(value["reason"], str) or not value["reason"].strip()
                              or len(value["reason"]) > 1024):
        raise ValueError("reconciliation reason must be explicit and within 1024 characters")
    if "source_transition" in value:
        transition = value["source_transition"]
        if not isinstance(transition, dict):
            raise ValueError("explicit original/evidence source transition required")
        RedisShmReadOnlyProxy._validate_source_transition(transition, transition.get("original_binding"))
        if apply and "reason" not in value:
            raise ValueError("cross-source apply requires an explicit operator reason")
    if "stopped_proxy_pid" in value:
        _integer(value["stopped_proxy_pid"], "stopped_proxy_pid", 1, 0xFFFFFFFF)
    return dict(value)


def confirmed_proxy_stopped(pid):
    """不停止进程；只核对执行清单中已确认的唯一旧代理 PID 已退出。"""
    if os.name != "nt":
        raise RuntimeError("proxy-exit confirmation requires Windows")
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.OpenProcess.argtypes = [ctypes.c_uint32, ctypes.c_int, ctypes.c_uint32]
    kernel.OpenProcess.restype = ctypes.c_void_p
    kernel.GetExitCodeProcess.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_uint32)]
    kernel.CloseHandle.argtypes = [ctypes.c_void_p]
    handle = kernel.OpenProcess(0x1000, False, pid)
    if not handle:
        if ctypes.get_last_error() == 87:
            return True
        raise RuntimeError("the explicitly identified proxy exit could not be confirmed")
    try:
        status = ctypes.c_uint32()
        if not kernel.GetExitCodeProcess(handle, ctypes.byref(status)):
            raise RuntimeError("the explicitly identified proxy exit could not be read")
        return status.value != 259
    finally:
        kernel.CloseHandle(handle)


def run_reconciliation(value, plan, *, apply=False, redis_factory=None, shm_factory=None):
    """一次有界只读核对及可选单记录 CAS；不启动接收循环或发单。"""
    config = validate_config(value)
    plan = validate_reconciliation_plan(plan, apply=apply)
    if config["runtime_policy"] not in WRITE_POLICIES:
        raise ValueError("reconciliation requires the existing persistent account read/write sources")
    if "source_transition" in plan:
        if config["environment"] != "LIVE":
            raise ValueError("cross-source reconciliation requires the existing LIVE policy")
        expected = dict(account_id=config["account_id"], environment=config["environment"],
                        runtime_policy=config["runtime_policy"], allow_order_methods=config["allow_order_methods"],
                        read_session_id=config["read_session_id"], execution_session_id=config["execution_session_id"],
                        native_pid=config["native_pid"], windows_session_id=config["windows_session"])
        RedisShmReadOnlyProxy._validate_source_transition(plan["source_transition"],
            plan["source_transition"]["original_binding"], expected)
    if windows_session_id(os.getpid()) != config["windows_session"] or windows_session_id(config["native_pid"]) != config["windows_session"]:
        raise RuntimeError("consumer and native source must share the explicit Windows session")
    old_proxy_pid = _integer(plan.get("stopped_proxy_pid"), "stopped_proxy_pid", 1, 0xFFFFFFFF)
    if old_proxy_pid in (os.getpid(), config["native_pid"]) or not confirmed_proxy_stopped(old_proxy_pid):
        raise RuntimeError("the independently identified unique old proxy must be stopped before mailbox inspection")
    redis_client, proxy, clients, errors = None, None, [], []
    result = dict(status="FAILED", account_id=config["account_id"], environment=config["environment"], request_id=plan["request_id"],
                  automatic_retry=False, native_write_calls=0, ledger_records_written=0)
    try:
        redis_client = (redis_factory or create_redis_client)(config["redis"])
        transport = RedisTransport(redis_client, account_id=config["account_id"])
        factory = shm_factory or ShmReadOnlyClient
        clients.append(factory(config["read_name"], config["account_id"]))
        clients.append(factory(config["execution_name"], config["account_id"]))
        proxy = RedisShmReadOnlyProxy(transport, clients[0], clients[1],
            account_id=config["account_id"], environment=config["environment"], start_write_blocked=config["start_write_blocked"],
            live_write_authorized=config["live_write_authorized"],
            execution_cursor_guard_target=config["execution_cursor_guard_target"],
            receive_policy=config["receive_policy"],
            expected_readonly_session_id=config["read_session_id"],
            expected_execution_session_id=config["execution_session_id"],
            expected_native_pid=config["native_pid"], timeout_seconds=config["timeout_seconds"],
            max_requests=0, runtime_policy=config["runtime_policy"], allow_order_methods=True,
            read_cache_capacity=config["read_cache_capacity"], write_ledger_capacity=config["write_ledger_capacity"],
            expected_windows_session_id=config["windows_session"])
        if config["start_write_blocked"]:
            proxy._block_writes("OPERATOR_WRITE_DISABLED")
        proxy.verify_identity()
        result.update(proxy.reconcile_unknown_write(plan["request_id"], plan["order_sys_id"], plan["order_identity"],
                      expected_record=plan.get("expected_record"), operator=plan.get("operator"),
                      reason=plan.get("reason"), source_transition=plan.get("source_transition"), apply=apply))
        if not apply:
            result["apply_plan"] = dict(plan, expected_record=result["expected_record"], operator="")
    except Exception as exc:
        result.update(status="FAILED", error_type=type(exc).__name__,
                      error_message=_safe_message(exc, config["redis"]))
    finally:
        if proxy is not None:
            try:
                proxy.close()
            except Exception as exc:
                errors.append(type(exc).__name__)
        for client in clients:
            try:
                client.close()
            except Exception as exc:
                errors.append(type(exc).__name__)
        if redis_client is not None:
            try:
                redis_client.connection_pool.disconnect()
            except Exception as exc:
                errors.append(type(exc).__name__)
        result.update(cleanup_errors=errors, resources_closed=not errors, runtime_running=False)
        if errors:
            result["status"] = "UNKNOWN"
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True, help="当次双SHM来源与Redis目标JSON，不执行配置代码")
    action = parser.add_mutually_exclusive_group()
    action.add_argument("--run", action="store_true", help="仅在另获实机授权后显式运行；默认dry-run")
    action.add_argument("--inspect-reconciliation", action="store_true", help="已核实唯一旧代理退出后，只读核对精确RID，不更改账本")
    action.add_argument("--apply-reconciliation", action="store_true", help="仅显式CAS已预览的精确UNKNOWN记录，不重发订单")
    parser.add_argument("--reconcile-plan", type=Path, help="精确RID/合同/原订单身份及可选预览原记录的JSON")
    args = parser.parse_args(argv)
    config = {}
    try:
        config = validate_config(json.loads(args.config.read_text(encoding="utf-8-sig")))
        plan = (validate_reconciliation_plan(json.loads(args.reconcile_plan.read_text(encoding="utf-8-sig")),
                    apply=args.apply_reconciliation) if args.reconcile_plan is not None else None)
        if (args.inspect_reconciliation or args.apply_reconciliation) and plan is None:
            raise ValueError("explicit reconciliation JSON plan required")
        if args.run and plan is not None:
            raise ValueError("normal queue runner cannot be combined with reconciliation")
        if args.inspect_reconciliation or args.apply_reconciliation:
            result = run_reconciliation(config, plan, apply=args.apply_reconciliation)
        elif args.run:
            result = run_candidate(config)
        else:
            result = dict(status="DRY_RUN", account_id=config["account_id"], environment=config["environment"], allow_order_methods=config["allow_order_methods"],
                          runtime_policy=config["runtime_policy"],
                          start_write_blocked=config["start_write_blocked"],
                          receive_policy=config["receive_policy"],
                          live_write_authorized=config["live_write_authorized"],
                          native_pid=config["native_pid"], windows_session=config["windows_session"],
                          max_seconds=config["max_seconds"], max_requests=config["max_requests"],
                          connections_created=0, full_duration_acceptance_proved=False,
                          reconciliation_plan_validated=plan is not None, runtime_preflight_verified=False)
    except Exception as exc:
        result = dict(status="FAILED", error_type=type(exc).__name__, automatic_retry=False,
                      error_message=_safe_message(exc, config.get("redis", {})))
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0 if result["status"] in ("DRY_RUN", "BOUNDED_STOP", "STOP_REQUESTED", "RECONCILIATION_PREVIEW", "RECONCILED") else 1


if __name__ == "__main__":
    raise SystemExit(main())
