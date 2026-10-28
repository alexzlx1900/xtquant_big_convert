# coding: utf-8
"""构建固定账户双SHM入口；默认模拟禁写，实盘须显式政策，不在QMT内初始化Redis。"""

import argparse
import ast
import base64
import importlib.util
import math
import ntpath
import os
from pathlib import Path
import re


ROOT = Path(__file__).resolve().parent.parent
ACCOUNT = "55013746"
SIM_NATIVE_EXECUTABLE = r"C:\国金QMT交易端模拟\bin.x64\XtItClient.exe"


def _load_builder(filename):
    spec = importlib.util.spec_from_file_location("dual_" + filename[:-3], ROOT / "tools" / filename)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


READ_BUILDER = _load_builder("build_shm_readonly_single_file.py")
EXECUTION_BUILDER = _load_builder("build_shm_rpc_single_file.py")


def _configure_source(source, settings):
    lines = source.splitlines(keepends=True)
    found = set()
    for node in reversed(ast.parse(source).body):
        if isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
            name = node.targets[0].id
            if name in settings:
                found.add(name)
                lines[node.lineno - 1:node.end_lineno] = ["%s = %r\n" % (name, settings[name])]
    if found != set(settings):
        raise ValueError("single-source configuration fields missing: %s" % sorted(set(settings) - found))
    return "".join(lines)


def build_source(*, native_pid, windows_session, hard_deadline_unix, read_name,
                 execution_name, symbols, account_id=ACCOUNT, max_seconds=900,
                 max_requests=1000, enable_market_rpc=False, persistent_readonly=False,
                 persistent_simulation_read_write=False, environment="SIM", persistent_live_read_write=False,
                 live_account_query_login=False, native_executable=None, runtime_launcher=None,
                 source_identity_path=None, execution_event_guard=False, read_fault_isolation=False):
    if environment not in ("SIM", "LIVE"):
        raise ValueError("explicit SIM/LIVE environment required")
    if type(live_account_query_login) is not bool or (live_account_query_login and environment != "LIVE"):
        raise ValueError("ACCOUNT query login authority requires explicit LIVE selection")
    if environment == "LIVE":
        if (persistent_live_read_write is not True or not isinstance(account_id, str) or not account_id
                or len(account_id) > 64 or account_id == ACCOUNT
                or not all("0" <= char <= "9" for char in account_id)):
            raise ValueError("LIVE build requires explicit persistent LIVE policy and live numeric account")
    elif persistent_live_read_write:
        raise ValueError("persistent LIVE policy requires LIVE environment")
    elif account_id != ACCOUNT:
        raise ValueError("this candidate is limited to simulation account 55013746")
    resident_identity = native_pid == 0 and type(native_pid) is int
    if resident_identity:
        if (not isinstance(native_executable, str) or not ntpath.isabs(native_executable)
                or ntpath.basename(native_executable).lower() != "xtitclient.exe"
                or ntpath.normpath(native_executable) != native_executable):
            raise ValueError("resident identity requires explicit QMT executable absolute path")
        if environment == "SIM" and (
                not ((persistent_readonly is True and persistent_simulation_read_write is False)
                    or (persistent_readonly is False and persistent_simulation_read_write is True))
                or enable_market_rpc is not False or runtime_launcher is not None
                or ntpath.normcase(native_executable) != ntpath.normcase(SIM_NATIVE_EXECUTABLE)):
            raise ValueError("SIM resident identity requires the fixed simulation executable and exclusive persistent policy without launcher")
    elif native_executable is not None:
        raise ValueError("native executable binding requires explicit native_pid=0 resident mode")
    if resident_identity and (not isinstance(source_identity_path, str) or not ntpath.isabs(source_identity_path)):
        raise ValueError("resident identity requires explicit absolute source identity path")
    if not resident_identity and source_identity_path is not None:
        raise ValueError("source identity publication requires resident mode")
    if runtime_launcher is not None:
        if (not resident_identity or not isinstance(runtime_launcher, dict)
                or set(runtime_launcher) != {"python", "script", "config"}
                or any(not isinstance(v, str) or not ntpath.isabs(v) for v in runtime_launcher.values())
                or ntpath.basename(runtime_launcher["script"]) != "supervise_qmt_runtime.py"):
            raise ValueError("resident runtime launcher requires three explicit absolute paths")
    if type(native_pid) is not int or native_pid < 0 or type(windows_session) is not int or windows_session < 0:
        raise ValueError("explicit native PID and Windows session required")
    if (type(persistent_readonly) is not bool or type(persistent_simulation_read_write) is not bool
            or type(persistent_live_read_write) is not bool):
        raise ValueError("persistent readonly selection must be explicit boolean")
    if sum((persistent_readonly, persistent_simulation_read_write, persistent_live_read_write)) > 1:
        raise ValueError("choose exactly one persistent runtime policy")
    read_write = persistent_simulation_read_write or persistent_live_read_write
    if (type(read_fault_isolation) is not bool or read_fault_isolation and
            not ((environment == "SIM" and account_id == ACCOUNT and (persistent_readonly is True or persistent_simulation_read_write is True))
                 or (environment == "LIVE" and account_id == "8882152350" and persistent_live_read_write is True))):
        raise ValueError("read fault isolation requires an explicit target account and persistent policy")
    if (type(execution_event_guard) is not bool or execution_event_guard and
            not ((environment == "SIM" and account_id == ACCOUNT and persistent_simulation_read_write is True)
                 or (environment == "LIVE" and account_id == "8882152350" and persistent_live_read_write is True))):
        raise ValueError("execution event guard requires an explicit target account and persistent write policy")
    persistent = persistent_readonly or read_write
    if persistent_readonly:
        if (hard_deadline_unix is not None or type(max_seconds) is not int or max_seconds != 0
                or type(max_requests) is not int or max_requests != 0 or enable_market_rpc is not False):
            raise ValueError("persistent readonly requires no deadline, explicit zero budgets and market disabled")
    elif read_write:
        if (hard_deadline_unix is not None or type(max_seconds) is not int or max_seconds != 0
                or type(max_requests) is not int or max_requests != 0):
            raise ValueError("persistent read-write requires no deadline and explicit zero budgets")
    else:
        if isinstance(hard_deadline_unix, bool) or not isinstance(hard_deadline_unix, (int, float)) or not math.isfinite(hard_deadline_unix) or hard_deadline_unix <= 0:
            raise ValueError("finite positive absolute deadline required")
        if type(max_seconds) is not int or not 1 <= max_seconds <= 900 or type(max_requests) is not int or not 1 <= max_requests <= 1000:
            raise ValueError("seconds [1,900] and per-source requests [1,1000] required")
    if type(enable_market_rpc) is not bool:
        raise ValueError("market RPC selection must be boolean")
    if not isinstance(read_name, str) or not re.fullmatch(r"Local\\AQP_SHM_READONLY_[A-Za-z0-9_-]{1,58}", read_name):
        raise ValueError("independent readonly mailbox name required")
    if not isinstance(execution_name, str) or not re.fullmatch(r"Local\\AQP_SHM_TRADE_[A-Za-z0-9_-]{1,58}", execution_name):
        raise ValueError("independent execution mailbox name required")
    if resident_identity and max(len(read_name), len(execution_name)) + 17 > len("Local\\AQP_SHM_") + 64:
        raise ValueError("resident mailbox base requires room for independent instance suffix")
    symbol_limit = 1024 if environment == "LIVE" else 64
    if not isinstance(symbols, (list, tuple)) or not 1 <= len(symbols) <= symbol_limit or any(not isinstance(code, str) or not re.fullmatch(r"[0-9]{6}\.(SH|SZ|BJ)", code) for code in symbols) or len(set(symbols)) != len(symbols):
        raise ValueError("1-%d unique confirmed SH/SZ/BJ symbols required" % symbol_limit)
    read = _configure_source(READ_BUILDER.build_source(), {
        "BIGQMT_ACCOUNT_ID": account_id, "BIGQMT_ENVIRONMENT": environment, "READONLY_SHM_NAME": read_name,
        "READONLY_SYMBOLS": list(symbols), "READONLY_MAX_SECONDS": max_seconds,
        "READONLY_MAX_REQUESTS": max_requests, "READONLY_ENABLE_MARKET_RPC": enable_market_rpc,
        "READONLY_PERSISTENT_READONLY": persistent,
        "READONLY_PERSISTENT_MARKET_RPC": read_write and enable_market_rpc,
        "READONLY_PERSISTENT_LIVE_READ_WRITE": persistent_live_read_write,
        "READONLY_PRESERVE_FAILED_MAILBOX": read_fault_isolation,
    })
    execution = EXECUTION_BUILDER.build_source(account_id=account_id, name=execution_name,
        native_stock_gateway=read_write, environment=environment,
        persistent_live_read_write=persistent_live_read_write,
        max_seconds=0 if persistent_live_read_write else None,
        max_requests=0 if persistent_live_read_write else None)
    execution = _configure_source(execution, {
        "SHM_RPC_BIND_ACCOUNT_CALLBACKS": False, "SHM_RPC_ALLOW_ORDER_METHODS": read_write,
        "SHM_RPC_ORDER_GATEWAY": None,
        "SHM_RPC_MAX_SECONDS": max_seconds, "SHM_RPC_MAX_REQUESTS": max_requests,
        "SHM_RPC_PERSISTENT_READONLY": persistent_readonly,
        "SHM_RPC_PERSISTENT_SIMULATION_READ_WRITE": persistent_simulation_read_write,
        "SHM_RPC_PERSISTENT_LIVE_READ_WRITE": persistent_live_read_write,
        "SHM_RPC_ACCOUNT_ALREADY_BOUND": read_write,
        "SHM_RPC_LIVE_ACCOUNT_QUERY_LOGIN": live_account_query_login,
        "SHM_RPC_EXECUTION_EVENT_GUARD": execution_event_guard,
    })
    guard_source = (ROOT / "src/bigqmt_signal_trader/native_supervisor_guard.py").read_text(encoding="utf-8")
    sources = {"read": read, "execution": execution, "native_guard": guard_source}
    for key, source in sources.items():
        ast.parse(source, filename=key, feature_version=(3, 6))
    payloads = {key: base64.b64encode(source.encode("utf-8")).decode("ascii") for key, source in sources.items()}
    settings = dict(account_id=account_id, environment=environment, native_pid=native_pid, windows_session=windows_session,
        hard_deadline_unix=None if persistent else float(hard_deadline_unix), max_seconds=max_seconds,
        max_requests_per_source=max_requests, market_rpc_enabled=enable_market_rpc,
        persistent_readonly=persistent_readonly, persistent_simulation_read_write=persistent_simulation_read_write,
        persistent_live_read_write=persistent_live_read_write, live_account_query_login=live_account_query_login,
        native_executable=native_executable, runtime_launcher=runtime_launcher, execution_event_guard=execution_event_guard,
        read_fault_isolation=read_fault_isolation,
        source_identity_path=source_identity_path)
    output = TEMPLATE.replace("__DUAL_SETTINGS__", repr(settings)).replace("__DUAL_PAYLOADS__", repr(payloads))
    output = output.replace("__SIM_NATIVE_EXECUTABLE__", repr(SIM_NATIVE_EXECUTABLE))
    output = output.encode("ascii", "backslashreplace").decode("ascii")
    ast.parse(output, feature_version=(3, 6))
    return output


TEMPLATE = '''#coding:gbk
"""Fixed account/environment, one native binding and two private SHM sources."""
import base64
from collections import deque
import ctypes
import json
import ntpath
import os
import subprocess
import threading
import time
import uuid

DUAL_SHM_SETTINGS = __DUAL_SETTINGS__
_SIM_NATIVE_EXECUTABLE = __SIM_NATIVE_EXECUTABLE__
_SOURCE_PAYLOADS = __DUAL_PAYLOADS__
_RUNTIMES = {}
_INIT_ATTEMPTED = False
_CLOSED = False
_OUTER_STOP_PENDING = threading.Event()
_STARTED_AT = None
_STARTED_AT_UNIX = None
_SOURCE_DEADLINE = None
_BOUND_NATIVE_PID = None
_BOUND_NATIVE_CREATION_FILETIME = None
_READ_FAULT = None
_SOURCE_IDENTITY = None
_READ_ROTATION = None
# 已关代的旧映射与请求证据只增不删；不得在换代过程中关闭或覆写。
_RETIRED_READ_ROTATIONS = []
_SUPERVISOR_PROCESS = None
_SUPERVISOR_IDENTITY = None
_SUPERVISOR_ATTACH_ATTEMPTED = False
_SUPERVISOR_ATTACH_STATE = None
_SUPERVISOR_ATTACH_RESULTS = deque()
_SUPERVISOR_ATTACH_STOP = threading.Event()
_SUPERVISOR_ATTACH_THREAD = None
_ROTATION_AUTH_THREAD = None
_ROTATION_CAPABILITY_THREAD = None
_SUPERVISOR_CLEANUP_STATUS = None
_SUPERVISOR_PIPE = None
_SUPERVISOR_GATE = None
_ROTATION_AUTH_ATTEMPTS = set()
_ROTATION_CONTROL_JOBS = {}
_ROTATION_CONTROL_RESULTS = deque()
_ROTATION_CAPABILITY = None
_ROTATION_CAPABILITY_RESULTS = deque()
_ROTATION_PINNED_TRANSITION = None
# 没有原生可独立核验的监督授权时，控制入口始终拒绝；不能用随机 token 替代授权。
_READ_ROTATION_AUTHORIZER = None
_READ_CAPABILITIES = ["ping", "get_asset", "get_positions", "get_full_tick", "get_ticks", "query_orders", "query_trades"]

for _key in ("read", "execution"):
    _namespace = {"__name__": "__aqp_dual_shm_" + _key + "__"}
    exec(compile(base64.b64decode(_SOURCE_PAYLOADS[_key]), "<dual-shm-" + _key + ">", "exec"), _namespace)
    _RUNTIMES[_key] = _namespace
_NATIVE_GUARD = {"__name__": "__aqp_native_supervisor_guard__"}
exec(compile(base64.b64decode(_SOURCE_PAYLOADS["native_guard"]), "<native-supervisor-guard>", "exec"), _NATIVE_GUARD)
_BASE_READ_NAME = _RUNTIMES["read"]["READONLY_SHM_NAME"]


def _verify_identity():
    configured = DUAL_SHM_SETTINGS["native_pid"]
    if configured == 0:
        if DUAL_SHM_SETTINGS["environment"] == "SIM" and (
                DUAL_SHM_SETTINGS["account_id"] != "55013746"
                or type(configured) is not int
                or type(DUAL_SHM_SETTINGS["windows_session"]) is not int
                or DUAL_SHM_SETTINGS["windows_session"] < 0
                or not ((DUAL_SHM_SETTINGS["persistent_readonly"] is True
                        and DUAL_SHM_SETTINGS["persistent_simulation_read_write"] is False)
                    or (DUAL_SHM_SETTINGS["persistent_readonly"] is False
                        and DUAL_SHM_SETTINGS["persistent_simulation_read_write"] is True))
                or DUAL_SHM_SETTINGS["persistent_live_read_write"] is not False
                or DUAL_SHM_SETTINGS["market_rpc_enabled"] is not False
                or DUAL_SHM_SETTINGS["runtime_launcher"] is not None
                or not isinstance(DUAL_SHM_SETTINGS["native_executable"], str)
                or ntpath.normcase(DUAL_SHM_SETTINGS["native_executable"]) != ntpath.normcase(_SIM_NATIVE_EXECUTABLE)
                or not isinstance(DUAL_SHM_SETTINGS["source_identity_path"], str)
                or not ntpath.isabs(DUAL_SHM_SETTINGS["source_identity_path"])):
            raise RuntimeError("SIM resident identity policy changed")
        path = ctypes.create_unicode_buffer(32768)
        length = ctypes.windll.kernel32.GetModuleFileNameW(None, path, len(path))
        expected = DUAL_SHM_SETTINGS["native_executable"]
        if (not length or length >= len(path) or not expected
                or ntpath.normcase(path.value) != ntpath.normcase(expected)):
            raise RuntimeError("native executable mismatch")
    elif os.getpid() != configured:
        raise RuntimeError("native PID mismatch")
    session = ctypes.c_uint()
    if not ctypes.windll.kernel32.ProcessIdToSessionId(os.getpid(), ctypes.byref(session)) or session.value != DUAL_SHM_SETTINGS["windows_session"]:
        raise RuntimeError("Windows session mismatch")
    return os.getpid()


def _process_creation_filetime(handle, kernel=None):
    try:
        if kernel is None:
            if os.name != "nt":
                return None
            kernel = ctypes.windll.kernel32
        class FileTime(ctypes.Structure):
            _fields_ = [("low", ctypes.c_uint32), ("high", ctypes.c_uint32)]
        get_times = kernel.GetProcessTimes
        get_times.argtypes = [ctypes.c_void_p, ctypes.POINTER(FileTime), ctypes.POINTER(FileTime),
                              ctypes.POINTER(FileTime), ctypes.POINTER(FileTime)]
        get_times.restype = ctypes.c_int
        created, exited, kernel_time, user_time = FileTime(), FileTime(), FileTime(), FileTime()
        if not get_times(ctypes.c_void_p(handle), ctypes.byref(created), ctypes.byref(exited),
                         ctypes.byref(kernel_time), ctypes.byref(user_time)):
            return None
        return (created.high << 32) | created.low
    except Exception:
        return None


def _native_creation_filetime():
    return _process_creation_filetime(-1)


def _supervisor_identity(process, kernel=None):
    try:
        if kernel is None:
            if os.name != "nt":
                return None
            kernel = ctypes.windll.kernel32
        if process.poll() is not None or type(process.pid) is not int or process.pid <= 0:
            return None
        handle = int(process._handle)
        get_pid = kernel.GetProcessId
        get_pid.argtypes, get_pid.restype = [ctypes.c_void_p], ctypes.c_uint32
        if get_pid(ctypes.c_void_p(handle)) != process.pid:
            return None
        created = _process_creation_filetime(handle, kernel)
        session = ctypes.c_uint()
        if (type(created) is not int or created <= 0
                or not kernel.ProcessIdToSessionId(process.pid, ctypes.byref(session))
                or session.value != DUAL_SHM_SETTINGS["windows_session"]):
            return None
        return dict(pid=process.pid, creation_filetime=created, windows_session_id=session.value)
    except Exception:
        return None


def _dispose_supervisor_attach(pipe, gate):
    success = True
    for resource in (gate, pipe):
        if resource is not None:
            try:
                resource.close()
            except BaseException:
                success = False
    return success


def _supervisor_attach_worker(request, identity):
    # 本线程只访问本机内核对象和监督器 pipe，不访问 QMT API 或 SHM 邮箱。
    pipe, gate = None, None
    try:
        kernel = _NATIVE_GUARD["WindowsNativeKernel"]()
        retained = lambda: _supervisor_identity(_SUPERVISOR_PROCESS)
        pipe = _NATIVE_GUARD["NativeControlPipe"](kernel, identity, retained)
        pipe.connect()
        response = pipe.exchange(request)
        expected = dict(request, status="HANDLE_OFFER")
        if (set(response) != set(expected) | {"guard_handles"}
                or any(response.get(key) != value or type(response.get(key)) is not type(value)
                       for key, value in expected.items())):
            raise RuntimeError("supervisor attachment receipt identity mismatch")
        gate = _NATIVE_GUARD["NativeWriteGate"](kernel, response["guard_handles"], identity, retained)
        result = (pipe, gate, None)
    except BaseException as exc:
        _dispose_supervisor_attach(pipe, gate)
        result = (None, None, type(exc).__name__)
    _SUPERVISOR_ATTACH_RESULTS.append(result)
    if _SUPERVISOR_ATTACH_STOP.is_set():
        try:
            stopped_pipe, stopped_gate, _ = _SUPERVISOR_ATTACH_RESULTS.popleft()
        except IndexError:
            return
        _dispose_supervisor_attach(stopped_pipe, stopped_gate)


def _attach_supervisor(command):
    global _SUPERVISOR_ATTACH_ATTEMPTED, _SUPERVISOR_ATTACH_STATE, _SUPERVISOR_ATTACH_THREAD
    attach_id = command.get("attach_id") if isinstance(command, dict) else None
    if (not isinstance(command, dict) or set(command) != {"contract", "attach_id"}
            or command.get("contract") != "QMT_NATIVE_SUPERVISOR_ATTACH_V1"
            or not isinstance(attach_id, str) or len(attach_id) != 32
            or any(char not in "0123456789abcdef" for char in attach_id) or _SUPERVISOR_ATTACH_ATTEMPTED):
        raise RuntimeError("supervisor attachment invalid or already attempted")
    _SUPERVISOR_ATTACH_ATTEMPTED = True
    execution = _RUNTIMES["execution"]["_SERVER"]
    if (DUAL_SHM_SETTINGS["environment"] != "LIVE" or DUAL_SHM_SETTINGS["account_id"] != "8882152350"
            or DUAL_SHM_SETTINGS["native_pid"] != 0 or DUAL_SHM_SETTINGS["execution_event_guard"] is not True
            or DUAL_SHM_SETTINGS["read_fault_isolation"] is not True or _SUPERVISOR_PROCESS is None
            or not isinstance(_SUPERVISOR_IDENTITY, dict) or execution is None or execution.closed
            or _native_creation_filetime() != _BOUND_NATIVE_CREATION_FILETIME
            or _supervisor_identity(_SUPERVISOR_PROCESS) != _SUPERVISOR_IDENTITY):
        raise RuntimeError("supervisor or native identity is unavailable")
    execution._thread()
    bridge = execution.bridge
    if not bridge.execution_event_guard_required or bridge.native_write_runtime_guard is not None:
        raise RuntimeError("native write gate is not available")
    request = dict(contract="QMT_NATIVE_ROTATION_PIPE_V1", action="ATTACH", native_pid=_BOUND_NATIVE_PID,
                   native_creation_filetime=_BOUND_NATIVE_CREATION_FILETIME,
                   windows_session_id=DUAL_SHM_SETTINGS["windows_session"], supervisor_pid=_SUPERVISOR_IDENTITY["pid"],
                   supervisor_creation_filetime=_SUPERVISOR_IDENTITY["creation_filetime"])
    _SUPERVISOR_ATTACH_STATE = dict(contract="QMT_NATIVE_SUPERVISOR_ATTACH_V1", attach_id=attach_id, status="PENDING",
        native_pid=_BOUND_NATIVE_PID, native_creation_filetime=_BOUND_NATIVE_CREATION_FILETIME,
        windows_session_id=DUAL_SHM_SETTINGS["windows_session"], execution_session_id=bridge.mailbox.session_id,
        supervisor_pid=_SUPERVISOR_IDENTITY["pid"], supervisor_creation_filetime=_SUPERVISOR_IDENTITY["creation_filetime"],
        automatic_retry=False)
    try:
        worker = threading.Thread(target=_supervisor_attach_worker, args=(request, dict(_SUPERVISOR_IDENTITY)),
                                  name="aqp-supervisor-attach", daemon=True)
        _SUPERVISOR_ATTACH_THREAD = worker
        worker.start()
    except BaseException:
        _SUPERVISOR_ATTACH_STATE["status"] = "FAILED"
        raise
    return dict(_SUPERVISOR_ATTACH_STATE)


def _supervisor_attach_status(command):
    if (not isinstance(command, dict) or set(command) != {"contract", "attach_id"}
            or command.get("contract") != "QMT_NATIVE_SUPERVISOR_ATTACH_V1"
            or _SUPERVISOR_ATTACH_STATE is None or command.get("attach_id") != _SUPERVISOR_ATTACH_STATE["attach_id"]):
        raise RuntimeError("supervisor attachment status identity mismatch")
    return dict(_SUPERVISOR_ATTACH_STATE)


def _advance_supervisor_attach():
    global _SUPERVISOR_PIPE, _SUPERVISOR_GATE
    if _SUPERVISOR_ATTACH_STATE is None or _SUPERVISOR_ATTACH_STATE["status"] != "PENDING":
        return
    try:
        pipe, gate, error = _SUPERVISOR_ATTACH_RESULTS.popleft()
    except IndexError:
        return
    execution = _RUNTIMES["execution"]["_SERVER"]
    try:
        if (error is not None or pipe is None or gate is None or execution is None or execution.closed
                or _SUPERVISOR_ATTACH_STOP.is_set() or _supervisor_identity(_SUPERVISOR_PROCESS) != _SUPERVISOR_IDENTITY
                or _verify_identity() != _BOUND_NATIVE_PID or _native_creation_filetime() != _BOUND_NATIVE_CREATION_FILETIME):
            raise RuntimeError("supervisor attachment result unavailable")
        execution._thread()
        bridge = execution.bridge
        if not bridge.execution_event_guard_required or bridge.native_write_runtime_guard is not None:
            raise RuntimeError("native write gate is not available")
        # 写包装已在原生 runtime 创建时安装；原回调线程安装句柄后发内核 ACK。
        bridge.native_write_runtime_guard = lambda: gate
        gate.acknowledge()
        if _supervisor_identity(_SUPERVISOR_PROCESS) != _SUPERVISOR_IDENTITY:
            raise RuntimeError("supervisor attachment identity changed")
        bridge.runtime_metadata.update(native_write_runtime_guard_attached=True,
                                       native_write_runtime_guard_ready=False,
                                       native_callback_threading_verified=False,
                                       supervisor_write_gate_contract="INHERITED_SUPERVISOR_GATE_V1",
                                       supervisor_gate_epoch_contract="SUPERVISOR_GATE_EPOCH_V1")
        _SUPERVISOR_PIPE, _SUPERVISOR_GATE = pipe, gate
        _SUPERVISOR_ATTACH_STATE["status"] = "ATTACHED"
    except BaseException:
        if execution is not None:
            execution.bridge.native_write_runtime_guard = None
        _SUPERVISOR_ATTACH_STATE["status"] = "FAILED"
        _dispose_supervisor_attach(pipe, gate)


def _execution_continuity_clear():
    execution_server = _RUNTIMES["execution"]["_SERVER"]
    if execution_server is None or execution_server.closed:
        return False
    bridge = execution_server.bridge
    return (not bridge.execution_event_guard_required or
            (bridge._lost_sequence == 0 and not bridge._off_thread_event_loss.is_set()))


def _rotated_read_continuity_clear():
    if _READ_ROTATION is None and not _RETIRED_READ_ROTATIONS:
        return True
    return _execution_continuity_clear()


def _read_rotation_evidence():
    if (_READ_FAULT is not None or type(_BOUND_NATIVE_CREATION_FILETIME) is not int
            or _BOUND_NATIVE_CREATION_FILETIME <= 0 or _native_creation_filetime() != _BOUND_NATIVE_CREATION_FILETIME):
        return None
    read_server = _RUNTIMES["read"]["_SERVER"]
    execution_server = _RUNTIMES["execution"]["_SERVER"]
    if read_server is None or execution_server is None or execution_server.closed:
        return None
    if not _execution_continuity_clear():
        return None
    if _READ_ROTATION is not None and _READ_ROTATION.get("old_mailbox") is not None:
        return read_server.read_rotation_evidence(execution_server.bridge.mailbox.session_id,
            _BOUND_NATIVE_CREATION_FILETIME, mailbox=_READ_ROTATION["old_mailbox"],
            completed=_READ_ROTATION["old_completed"])
    return read_server.read_rotation_evidence(execution_server.bridge.mailbox.session_id,
                                               _BOUND_NATIVE_CREATION_FILETIME)


class _NativeCapabilityBusy(RuntimeError):
    pass


def _capability_mailbox_header(mailbox):
    if not mailbox.region.acquire(0):
        raise _NativeCapabilityBusy("native capability mailbox busy")
    try:
        header = mailbox._header()
        ready = _RUNTIMES["read"]["_load_embedded"]("aqp_readonly_transport").READY
        if header[2] != ready or header[3] != _BOUND_NATIVE_PID or header[9].hex() != mailbox.session_id:
            raise RuntimeError("native capability mailbox identity changed")
    finally:
        mailbox.region.release()


def _native_capability_request(nonce):
    execution = _RUNTIMES["execution"]["_SERVER"]
    read = _RUNTIMES["read"]["_SERVER"]
    if (not isinstance(nonce, str) or len(nonce) != 32 or any(char not in "0123456789abcdef" for char in nonce)
            or DUAL_SHM_SETTINGS["environment"] != "LIVE" or DUAL_SHM_SETTINGS["account_id"] != "8882152350"
            or DUAL_SHM_SETTINGS["native_pid"] != 0 or DUAL_SHM_SETTINGS["execution_event_guard"] is not True
            or DUAL_SHM_SETTINGS["read_fault_isolation"] is not True or _CLOSED or _READ_FAULT is not None
            or execution is None or execution.closed or read is None or read.closed or read.failed
            or _SUPERVISOR_ATTACH_STATE is None or _SUPERVISOR_ATTACH_STATE.get("status") != "ATTACHED"
            or _SUPERVISOR_PIPE is None or _SUPERVISOR_PIPE.poisoned or _SUPERVISOR_PIPE.handle is None
            or _SUPERVISOR_GATE is None or _SUPERVISOR_GATE.failed or _SUPERVISOR_PROCESS is None
            or not isinstance(_SUPERVISOR_IDENTITY, dict)
            or _supervisor_identity(_SUPERVISOR_PROCESS) != _SUPERVISOR_IDENTITY
            or _verify_identity() != _BOUND_NATIVE_PID or type(_BOUND_NATIVE_CREATION_FILETIME) is not int
            or _BOUND_NATIVE_CREATION_FILETIME <= 0 or _native_creation_filetime() != _BOUND_NATIVE_CREATION_FILETIME
            or not _execution_continuity_clear() or not isinstance(_SOURCE_IDENTITY, dict)):
        raise RuntimeError("native read rotation capability identity unavailable")
    execution._thread()
    old = read.mailbox if _READ_ROTATION is None else _READ_ROTATION.get("old_mailbox")
    if (old is None or old.server_pid != _BOUND_NATIVE_PID or execution.bridge.mailbox.server_pid != _BOUND_NATIVE_PID
            or old.session_id == execution.bridge.mailbox.session_id
            or read.account_id != DUAL_SHM_SETTINGS["account_id"] or read.environment != DUAL_SHM_SETTINGS["environment"]
            or execution.bridge.account_id != DUAL_SHM_SETTINGS["account_id"]
            or execution.bridge.environment != DUAL_SHM_SETTINGS["environment"]
            or read.windows_session_id != DUAL_SHM_SETTINGS["windows_session"]
            or execution.bridge.runtime_metadata.get("windows_session_id") != DUAL_SHM_SETTINGS["windows_session"]
            or _SOURCE_IDENTITY.get("account_id") != DUAL_SHM_SETTINGS["account_id"]
            or _SOURCE_IDENTITY.get("environment") != DUAL_SHM_SETTINGS["environment"]
            or _SOURCE_IDENTITY.get("native_pid") != _BOUND_NATIVE_PID
            or _SOURCE_IDENTITY.get("windows_session") != DUAL_SHM_SETTINGS["windows_session"]
            or _SOURCE_IDENTITY.get("read_name") != old.name
            or _SOURCE_IDENTITY.get("execution_name") != execution.bridge.mailbox.name
            or _SOURCE_IDENTITY.get("read_session_id") != old.session_id
            or _SOURCE_IDENTITY.get("execution_session_id") != execution.bridge.mailbox.session_id):
        raise RuntimeError("native read rotation capability source changed")
    _capability_mailbox_header(old)
    _capability_mailbox_header(execution.bridge.mailbox)
    return dict(contract="QMT_NATIVE_ROTATION_PIPE_V1", action="CAPABILITY_PROBE", nonce=nonce,
                account_id=DUAL_SHM_SETTINGS["account_id"], environment=DUAL_SHM_SETTINGS["environment"],
                native_pid=_BOUND_NATIVE_PID, native_creation_filetime=_BOUND_NATIVE_CREATION_FILETIME,
                windows_session_id=DUAL_SHM_SETTINGS["windows_session"],
                execution_session_id=execution.bridge.mailbox.session_id, read_session_id=old.session_id,
                supervisor_pid=_SUPERVISOR_IDENTITY["pid"],
                supervisor_creation_filetime=_SUPERVISOR_IDENTITY["creation_filetime"])


def _rotation_capability_worker(request, pipe):
    try:
        response, error = pipe.exchange(request), None
    except BaseException as exc:
        response, error = None, type(exc).__name__
    _ROTATION_CAPABILITY_RESULTS.append((response, error))
    if _SUPERVISOR_ATTACH_STOP.is_set():
        try:
            _ROTATION_CAPABILITY_RESULTS.popleft()
        except IndexError:
            return
        _dispose_supervisor_attach(pipe, None)


def _start_read_rotation_capability(command):
    global _ROTATION_CAPABILITY, _ROTATION_CAPABILITY_THREAD
    if (not isinstance(command, dict) or set(command) != {"contract", "nonce"}
            or command.get("contract") != "READ_ROTATION_CAPABILITY_V1" or _ROTATION_CAPABILITY is not None
            or _READ_ROTATION is not None or any(job["status"] in ("AUTH_PENDING", "ACTIVATION_PENDING", "READ_RELEASE_PENDING", "RETIRE_ACK_PENDING")
                                                 for job in _ROTATION_CONTROL_JOBS.values())):
        raise RuntimeError("native read rotation capability is invalid or already attempted")
    request = _native_capability_request(command.get("nonce"))
    _ROTATION_CAPABILITY = dict(nonce=command["nonce"], request=request, status="PENDING", receipt=None)
    try:
        worker = threading.Thread(target=_rotation_capability_worker, args=(request, _SUPERVISOR_PIPE),
                                  name="aqp-read-rotation-capability", daemon=True)
        _ROTATION_CAPABILITY_THREAD = worker
        worker.start()
    except BaseException:
        _ROTATION_CAPABILITY.update(status="UNKNOWN", request=None)
        raise
    return dict(contract="READ_ROTATION_CAPABILITY_V1", nonce=command["nonce"], status="PENDING", automatic_retry=False)


def _read_rotation_capability_status(command):
    if (not isinstance(command, dict) or set(command) != {"contract", "nonce"}
            or command.get("contract") != "READ_ROTATION_CAPABILITY_V1" or _ROTATION_CAPABILITY is None
            or command.get("nonce") != _ROTATION_CAPABILITY["nonce"]):
        raise RuntimeError("native read rotation capability status identity mismatch")
    receipt = _ROTATION_CAPABILITY["receipt"]
    return dict(contract="READ_ROTATION_CAPABILITY_V1", nonce=command["nonce"],
                status=_ROTATION_CAPABILITY["status"], automatic_retry=False,
                receipt=None if receipt is None else dict(receipt))


def _advance_read_rotation_capability():
    if _ROTATION_CAPABILITY is None or _ROTATION_CAPABILITY["status"] != "PENDING":
        return
    try:
        response, error = _ROTATION_CAPABILITY_RESULTS.popleft()
    except IndexError:
        return
    try:
        request = _ROTATION_CAPABILITY["request"]
        expected = dict(request, status="READY")
        if (error is not None or _SUPERVISOR_ATTACH_STOP.is_set() or not isinstance(response, dict)
                or set(response) != set(expected)
                or any(response.get(field) != value or type(response.get(field)) is not type(value)
                       for field, value in expected.items()) or _native_capability_request(request["nonce"]) != request):
            raise RuntimeError("native read rotation capability proof is unconfirmed")
        _ROTATION_CAPABILITY.update(status="READY", receipt=dict(response))
        # READY 仅表示双通道挑战成功；实机全链验收前不能开放生产换代能力。
        execution = _RUNTIMES["execution"]["_SERVER"]
        execution.bridge.runtime_metadata["read_rotation_capability_contract"] = "READ_ROTATION_CAPABILITY_V1"
        execution.bridge.runtime_metadata["read_rotation_authorization_ready"] = False
    except BaseException:
        _ROTATION_CAPABILITY.update(status="UNKNOWN", receipt=None)


def _refresh_read_rotation_capability():
    if _ROTATION_CAPABILITY is None or _ROTATION_CAPABILITY["status"] != "READY":
        return
    try:
        request = _ROTATION_CAPABILITY["request"]
        if _native_capability_request(request["nonce"]) == request:
            return
    except _NativeCapabilityBusy:
        return
    except Exception:
        pass
    reason = "COMMITTED_READ_SID" if _committed_capability_transition() else "IDENTITY_OR_CONTINUITY_UNKNOWN"
    _ROTATION_CAPABILITY.update(status="REVOKED", reason=reason,
                                prior_receipt=_ROTATION_CAPABILITY["receipt"], receipt=None)
    execution = _RUNTIMES["execution"]["_SERVER"]
    if execution is not None:
        execution.bridge.runtime_metadata["read_rotation_authorization_ready"] = False


def _committed_capability_transition(expected_state="COMMITTED"):
    capability, rotation = _ROTATION_CAPABILITY, _READ_ROTATION
    if not isinstance(capability, dict) or not isinstance(rotation, dict) or _ROTATION_PINNED_TRANSITION is None:
        return False
    request = capability.get("request")
    execution = _RUNTIMES["execution"]["_SERVER"]
    return bool(isinstance(request, dict) and isinstance(_SOURCE_IDENTITY, dict) and execution is not None and not execution.closed
        and rotation.get("state") == expected_state and rotation.get("transition_id") == _ROTATION_PINNED_TRANSITION
        and rotation.get("old_evidence", {}).get("read_session_id") == request.get("read_session_id")
        and _SOURCE_IDENTITY.get("read_session_id") == rotation.get("candidate_read_session_id")
        and _SOURCE_IDENTITY.get("execution_session_id") == request.get("execution_session_id")
        and execution.bridge.mailbox.session_id == request.get("execution_session_id")
        and _BOUND_NATIVE_PID == request.get("native_pid") and _BOUND_NATIVE_CREATION_FILETIME == request.get("native_creation_filetime")
        and _native_creation_filetime() == _BOUND_NATIVE_CREATION_FILETIME and _execution_continuity_clear()
        and _SUPERVISOR_PIPE is not None and not _SUPERVISOR_PIPE.poisoned and _SUPERVISOR_GATE is not None
        and not _SUPERVISOR_GATE.failed and _SUPERVISOR_PROCESS is not None
        and _supervisor_identity(_SUPERVISOR_PROCESS) == _SUPERVISOR_IDENTITY)


def _capability_allows_activation(command):
    return bool(isinstance(command, dict) and command.get("action") == "ACTIVATE"
        and command.get("transition_id") == _ROTATION_PINNED_TRANSITION
        and isinstance(_ROTATION_CAPABILITY, dict) and _ROTATION_CAPABILITY.get("status") == "REVOKED"
        and _ROTATION_CAPABILITY.get("reason") == "COMMITTED_READ_SID" and _committed_capability_transition())


def _capability_allows_retirement(command):
    key = (_ROTATION_PINNED_TRANSITION, "ACTIVATE")
    job = _ROTATION_CONTROL_JOBS.get(key)
    return bool(isinstance(command, dict) and command.get("action") == "CLOSE"
        and command.get("transition_id") == _ROTATION_PINNED_TRANSITION
        and isinstance(_ROTATION_CAPABILITY, dict) and _ROTATION_CAPABILITY.get("status") == "REVOKED"
        and _ROTATION_CAPABILITY.get("reason") == "COMMITTED_READ_SID"
        and isinstance(job, dict) and job.get("status") == "ACTIVATED" and isinstance(job.get("receipt"), dict)
        and _committed_capability_transition("ACTIVATED"))


def _prepare_read_rotation_authorization(command):
    """从原生已核实状态构造监督器许可请求；不信任调用方自报的来源字段。"""
    if (not isinstance(command, dict) or command.get("contract") != "READ_ROTATION_CONTROL_V1"
            or command.get("action") not in ("PREPARE", "COMMIT", "ACTIVATE", "CLOSE")):
        raise RuntimeError("read rotation authorization command invalid")
    action = command["action"]
    required = ({"contract", "action", "transition_id", "nonce", "capability", "old_evidence"} if action == "PREPARE"
                else {"contract", "action", "transition_id", "nonce", "capability", "token", "probe_request_id", "probe_sequence"}
                if action == "COMMIT" else {"contract", "action", "transition_id", "nonce", "capability", "token"}
                if action == "ACTIVATE" else {"contract", "action", "transition_id", "nonce", "capability", "gateway_verified_sha256"})
    if set(command) != required:
        raise RuntimeError("read rotation authorization fields invalid")
    for field, size in (("transition_id", 32), ("nonce", 32), ("capability", 64)):
        value = command.get(field)
        if not isinstance(value, str) or len(value) != size or any(char not in "0123456789abcdef" for char in value):
            raise RuntimeError("read rotation authorization identity invalid")
    if action == "CLOSE":
        digest = command.get("gateway_verified_sha256")
        if not isinstance(digest, str) or len(digest) != 64 or any(char not in "0123456789abcdef" for char in digest):
            raise RuntimeError("read rotation gateway verification digest invalid")
    execution = _RUNTIMES["execution"]["_SERVER"]
    if (execution is None or execution.closed or _CLOSED or _SUPERVISOR_PIPE is None
            or _SUPERVISOR_PIPE.poisoned or _SUPERVISOR_GATE is None or _SUPERVISOR_GATE.failed
            or _SUPERVISOR_PROCESS is None or _supervisor_identity(_SUPERVISOR_PROCESS) != _SUPERVISOR_IDENTITY
            or _verify_identity() != _BOUND_NATIVE_PID or _native_creation_filetime() != _BOUND_NATIVE_CREATION_FILETIME):
        raise RuntimeError("read rotation supervisor or native identity unavailable")
    execution._thread()
    evidence = _read_rotation_evidence()
    if not isinstance(evidence, dict) or evidence.get("execution_session_id") != execution.bridge.mailbox.session_id:
        raise RuntimeError("read rotation old evidence unavailable")
    if action == "PREPARE":
        if _READ_ROTATION is not None or command["old_evidence"] != evidence:
            raise RuntimeError("read rotation old evidence changed")
        candidate, probe, probe_sequence = None, None, None
    else:
        expected_state = "PREPARED" if action == "COMMIT" else "COMMITTED" if action == "ACTIVATE" else "ACTIVATED"
        if (_READ_ROTATION is None or _READ_ROTATION.get("state") != expected_state
                or _READ_ROTATION.get("transition_id") != command["transition_id"]
                or (action != "CLOSE" and _READ_ROTATION.get("token") != command["token"])
                or _READ_ROTATION.get("old_evidence") != evidence):
            raise RuntimeError("read rotation native state changed")
        if action == "CLOSE":
            activated = _ROTATION_CONTROL_JOBS.get((command["transition_id"], "ACTIVATE"))
            if not isinstance(activated, dict) or activated.get("status") != "ACTIVATED" or not isinstance(activated.get("receipt"), dict):
                raise RuntimeError("read rotation activation receipt is unconfirmed")
        candidate = _READ_ROTATION["candidate_read_session_id"]
        probe, probe_sequence = _READ_ROTATION["probe_request_id"], 1
        if action == "COMMIT" and (command["probe_request_id"] != probe or type(command["probe_sequence"]) is not int
                                   or command["probe_sequence"] != probe_sequence):
            raise RuntimeError("read rotation candidate probe changed")
    key = (command["transition_id"], action)
    if key in _ROTATION_AUTH_ATTEMPTS:
        raise RuntimeError("read rotation authorization is not repeatable")
    request = dict(contract="QMT_NATIVE_ROTATION_PIPE_V1", action="CLOSE_PERMIT" if action == "CLOSE" else action,
                   transition_id=command["transition_id"], nonce=command["nonce"], capability=command["capability"],
                   account_id=DUAL_SHM_SETTINGS["account_id"], environment=DUAL_SHM_SETTINGS["environment"],
                   native_pid=_BOUND_NATIVE_PID, native_creation_filetime=_BOUND_NATIVE_CREATION_FILETIME,
                   windows_session_id=DUAL_SHM_SETTINGS["windows_session"],
                   execution_session_id=execution.bridge.mailbox.session_id,
                   read_session_id=evidence["read_session_id"], old_request_id=evidence["request_id"],
                   old_sequence=evidence["sequence"], candidate_read_session_id=candidate,
                   new_probe_request_id=probe, new_probe_sequence=probe_sequence)
    if action == "CLOSE":
        request["gateway_verified_sha256"] = command["gateway_verified_sha256"]
    # 即使监督器已消费许可但应答丢失，也不得再次使用该阶段。
    _ROTATION_AUTH_ATTEMPTS.add(key)
    return request, evidence


def _verify_read_rotation_authorization(request, evidence, response):
    expected = dict(request, status="ALLOW")
    if (set(response) != set(expected) or any(response.get(field) != value or type(response.get(field)) is not type(value)
                                              for field, value in expected.items())
            or _read_rotation_evidence() != evidence or _supervisor_identity(_SUPERVISOR_PROCESS) != _SUPERVISOR_IDENTITY):
        raise RuntimeError("read rotation authorization receipt unverified")
    return True


def _authorize_read_rotation(command):
    # 只供隔离协议测试；真实回调不得同步等待 pipe。
    request, evidence = _prepare_read_rotation_authorization(command)
    response = _SUPERVISOR_PIPE.exchange(request)
    return _verify_read_rotation_authorization(request, evidence, response)


def _rotation_authorization_worker(key, request, pipe, phase="AUTH"):
    # 只做 pipe I/O；所有 QMT 身份、旧帧与状态切换由原回调线程检查。
    try:
        response, error = pipe.exchange(request), None
    except BaseException as exc:
        response, error = None, type(exc).__name__
    _ROTATION_CONTROL_RESULTS.append((key, response, error, phase))
    if _SUPERVISOR_ATTACH_STOP.is_set():
        try:
            _ROTATION_CONTROL_RESULTS.popleft()
        except IndexError:
            return
        _dispose_supervisor_attach(pipe, None)


def _start_read_rotation_control(command):
    global _ROTATION_AUTH_THREAD, _ROTATION_PINNED_TRANSITION
    execution = _RUNTIMES["execution"]["_SERVER"]
    continuation = _capability_allows_activation(command) or _capability_allows_retirement(command)
    if (execution is None or (execution.bridge.runtime_metadata.get("read_rotation_authorization_ready") is not True
                              and not continuation)
            or (_ROTATION_CAPABILITY is not None and _ROTATION_CAPABILITY["status"] != "READY" and not continuation)
            or (_ROTATION_CAPABILITY is not None and _ROTATION_CAPABILITY["status"] == "PENDING")
            or any(job["status"] in ("AUTH_PENDING", "ACTIVATION_PENDING", "READ_RELEASE_PENDING", "RETIRE_ACK_PENDING", "UNKNOWN")
                   for job in _ROTATION_CONTROL_JOBS.values())):
        raise RuntimeError("read rotation independent authorization is unavailable")
    request, evidence = _prepare_read_rotation_authorization(command)
    key = (request["transition_id"], command["action"])
    if command["action"] == "PREPARE":
        _ROTATION_PINNED_TRANSITION = request["transition_id"]
    if key in _ROTATION_CONTROL_JOBS:
        raise RuntimeError("read rotation control is not repeatable")
    saved = dict(command)
    if isinstance(saved.get("old_evidence"), dict):
        saved["old_evidence"] = dict(saved["old_evidence"])
    _ROTATION_CONTROL_JOBS[key] = dict(command=saved, request=request, evidence=dict(evidence),
                                       status="AUTH_PENDING", receipt=None)
    try:
        worker = threading.Thread(target=_rotation_authorization_worker, args=(key, request, _SUPERVISOR_PIPE),
                                  name="aqp-rotation-authorize", daemon=True)
        _ROTATION_AUTH_THREAD = worker
        worker.start()
    except BaseException:
        job = _ROTATION_CONTROL_JOBS[key]
        job.update(command=None, request=None, status="UNKNOWN")
        raise
    return dict(contract="READ_ROTATION_CONTROL_V1", action=key[1], transition_id=key[0],
                status="AUTH_PENDING", automatic_retry=False)


def _read_rotation_control_status(command):
    if (not isinstance(command, dict) or set(command) != {"contract", "action", "transition_id"}
            or command.get("contract") != "READ_ROTATION_CONTROL_V1"):
        raise RuntimeError("read rotation control status invalid")
    key = (command.get("transition_id"), command.get("action"))
    job = _ROTATION_CONTROL_JOBS.get(key)
    if job is None:
        raise RuntimeError("read rotation control status unavailable")
    _record_rotation_continuity_loss()
    return dict(contract="READ_ROTATION_CONTROL_V1", action=key[1], transition_id=key[0],
                status=job["status"], automatic_retry=False,
                receipt=None if job["receipt"] is None else dict(job["receipt"]))


def _dispatch_read_rotation_control(command):
    execution = _RUNTIMES["execution"]["_SERVER"]
    if execution is not None and (execution.bridge.runtime_metadata.get("read_rotation_authorization_ready") is True
                                  or _capability_allows_activation(command) or _capability_allows_retirement(command)):
        return _start_read_rotation_control(command)
    return _read_rotation_control(command)


def _native_activation_receipt(action):
    rotation = _READ_ROTATION
    execution = _RUNTIMES["execution"]["_SERVER"]
    read = _RUNTIMES["read"]["_SERVER"]
    if action not in ("ACTIVATION_RECEIPT", "READ_RELEASED_RECEIPT"):
        raise RuntimeError("native activation receipt action invalid")
    expected_state = "ACTIVATED_RESTRICTED" if action == "ACTIVATION_RECEIPT" else "ACTIVATED"
    if (not isinstance(rotation, dict) or rotation.get("state") != expected_state
            or execution is None or execution.closed or read is None or read.closed
            or _SOURCE_IDENTITY is None or _SUPERVISOR_PIPE is None or _SUPERVISOR_PIPE.poisoned
            or _SUPERVISOR_PROCESS is None or _supervisor_identity(_SUPERVISOR_PROCESS) != _SUPERVISOR_IDENTITY
            or _verify_identity() != _BOUND_NATIVE_PID or _native_creation_filetime() != _BOUND_NATIVE_CREATION_FILETIME
            or _read_rotation_evidence() != rotation.get("old_evidence")
            or _SOURCE_IDENTITY.get("read_session_id") != rotation.get("candidate_read_session_id")
            or _SOURCE_IDENTITY.get("execution_session_id") != execution.bridge.mailbox.session_id
            or read.mailbox is not rotation.get("candidate")
            or read.rotation_probe_request_id != (rotation.get("probe_request_id")
                                                  if action == "ACTIVATION_RECEIPT" else None)):
        raise RuntimeError("native activation receipt identity unavailable")
    candidate = rotation["candidate"]
    if not candidate.region.acquire(0):
        raise RuntimeError("candidate frame unavailable for activation receipt")
    try:
        if not _rotation_probe_locked(read, candidate, rotation["probe_request_id"], 1):
            raise RuntimeError("candidate frame changed before activation receipt")
    finally:
        candidate.region.release()
    evidence = rotation["old_evidence"]
    return dict(contract="QMT_NATIVE_ROTATION_PIPE_V1", action=action,
                transition_id=rotation["transition_id"], account_id=DUAL_SHM_SETTINGS["account_id"],
                environment=DUAL_SHM_SETTINGS["environment"], native_pid=_BOUND_NATIVE_PID,
                native_creation_filetime=_BOUND_NATIVE_CREATION_FILETIME,
                windows_session_id=DUAL_SHM_SETTINGS["windows_session"],
                execution_session_id=execution.bridge.mailbox.session_id,
                old_read_session_id=evidence["read_session_id"], candidate_read_session_id=candidate.session_id,
                old_request_id=evidence["request_id"], old_sequence=evidence["sequence"],
                probe_request_id=rotation["probe_request_id"], probe_sequence=1)


def _native_retirement_receipt():
    rotation = _READ_ROTATION
    execution = _RUNTIMES["execution"]["_SERVER"]
    read = _RUNTIMES["read"]["_SERVER"]
    if (not isinstance(rotation, dict) or rotation.get("state") != "RETIRED_RESTRICTED"
            or execution is None or execution.closed or read is None or read.closed
            or _SOURCE_IDENTITY is None or _SUPERVISOR_PIPE is None or _SUPERVISOR_PIPE.poisoned
            or _SUPERVISOR_PROCESS is None or _supervisor_identity(_SUPERVISOR_PROCESS) != _SUPERVISOR_IDENTITY
            or _verify_identity() != _BOUND_NATIVE_PID or _native_creation_filetime() != _BOUND_NATIVE_CREATION_FILETIME
            or _read_rotation_evidence() != rotation.get("old_evidence")
            or _SOURCE_IDENTITY.get("read_session_id") != rotation.get("candidate_read_session_id")
            or _SOURCE_IDENTITY.get("execution_session_id") != execution.bridge.mailbox.session_id
            or read.mailbox is not rotation.get("candidate") or read.rotation_probe_request_id is not None
            or not _execution_continuity_clear()):
        raise RuntimeError("native retirement receipt identity unavailable")
    with open(DUAL_SHM_SETTINGS["source_identity_path"], "r") as stream:
        if json.load(stream) != _SOURCE_IDENTITY:
            raise RuntimeError("published source identity changed during retirement")
    evidence = rotation["old_evidence"]
    return dict(contract="QMT_NATIVE_ROTATION_PIPE_V1", action="GENERATION_RETIRED_RECEIPT",
                transition_id=rotation["transition_id"], account_id=DUAL_SHM_SETTINGS["account_id"],
                environment=DUAL_SHM_SETTINGS["environment"], native_pid=_BOUND_NATIVE_PID,
                native_creation_filetime=_BOUND_NATIVE_CREATION_FILETIME,
                windows_session_id=DUAL_SHM_SETTINGS["windows_session"],
                execution_session_id=execution.bridge.mailbox.session_id,
                old_read_session_id=evidence["read_session_id"],
                candidate_read_session_id=rotation["candidate_read_session_id"],
                old_request_id=evidence["request_id"], old_sequence=evidence["sequence"],
                probe_request_id=rotation["probe_request_id"], probe_sequence=1,
                gateway_verified_sha256=rotation["gateway_verified_sha256"])


def _retirement_unknown(job):
    if job.get("pending_receipt") is not None:
        job["prior_receipt"] = job["pending_receipt"]
    job.update(receipt=None, pending_receipt=None, activation_request=None, status="UNKNOWN")
    if _READ_ROTATION is not None:
        _rotation_state("CLOSE_UNKNOWN")


def _activation_receipt_unknown(job):
    if job.get("pending_receipt") is not None:
        job["prior_receipt"] = job["pending_receipt"]
    job.update(receipt=None, pending_receipt=None, activation_request=None, status="UNKNOWN")
    read = _RUNTIMES["read"]["_SERVER"]
    if _READ_ROTATION is not None:
        if read is not None:
            read.rotation_probe_request_id = _READ_ROTATION["probe_request_id"]
        _rotation_state("ACTIVATE_UNKNOWN")


def _advance_read_rotation_control():
    global _ROTATION_AUTH_THREAD, _READ_ROTATION, _ROTATION_CAPABILITY, _ROTATION_PINNED_TRANSITION
    try:
        key, response, error, phase = _ROTATION_CONTROL_RESULTS.popleft()
    except IndexError:
        return
    job = _ROTATION_CONTROL_JOBS.get(key)
    if job is None:
        return
    if phase == "GENERATION_RETIRED_RECEIPT":
        if job["status"] != "RETIRE_ACK_PENDING":
            return
        try:
            expected = dict(job["activation_request"], status="ACK")
            if (error is not None or _SUPERVISOR_ATTACH_STOP.is_set() or not isinstance(response, dict)
                    or set(response) != set(expected)
                    or any(response.get(field) != value or type(response.get(field)) is not type(value)
                           for field, value in expected.items())
                    or _native_retirement_receipt() != job["activation_request"]):
                raise RuntimeError("native retirement receipt is unconfirmed")
            rotation = _READ_ROTATION
            if not _execution_continuity_clear():
                raise RuntimeError("execution continuity changed before retirement")
            rotation["retired_capability"] = _ROTATION_CAPABILITY
            _rotation_state("CLOSED")
            _RETIRED_READ_ROTATIONS.append(rotation)  # 旧 mailbox 与原帧继续持有，不 ACK、不清理。
            if not _execution_continuity_clear():
                raise RuntimeError("execution continuity changed during retirement")
            job.update(receipt=job["pending_receipt"], pending_receipt=None, activation_request=None, status="CLOSED")
            _READ_ROTATION, _ROTATION_CAPABILITY, _ROTATION_PINNED_TRANSITION = None, None, None
            execution = _RUNTIMES["execution"]["_SERVER"]
            execution.bridge.runtime_metadata["read_rotation_authorization_ready"] = False
        except BaseException:
            _retirement_unknown(job)
        return
    if phase in ("ACTIVATION_RECEIPT", "READ_RELEASED_RECEIPT"):
        expected_job = "ACTIVATION_PENDING" if phase == "ACTIVATION_RECEIPT" else "READ_RELEASE_PENDING"
        if job["status"] != expected_job:
            return
        try:
            expected = dict(job["activation_request"], status="ACK")
            if (error is not None or _SUPERVISOR_ATTACH_STOP.is_set() or not isinstance(response, dict)
                    or set(response) != set(expected)
                    or any(response.get(field) != value or type(response.get(field)) is not type(value)
                           for field, value in expected.items())
                    or _native_activation_receipt(phase) != job["activation_request"]):
                raise RuntimeError("native activation receipt is unconfirmed")
            if phase == "ACTIVATION_RECEIPT":
                read = _RUNTIMES["read"]["_SERVER"]
                read.rotation_probe_request_id = None
                if not _execution_continuity_clear():
                    raise RuntimeError("execution continuity changed before activation publication")
                _rotation_state("ACTIVATED")
                if not _execution_continuity_clear():
                    raise RuntimeError("execution continuity changed during activation publication")
                frame = _native_activation_receipt("READ_RELEASED_RECEIPT")
                job.update(status="READ_RELEASE_PENDING", activation_request=frame)
                worker = threading.Thread(target=_rotation_authorization_worker,
                    args=(key, frame, _SUPERVISOR_PIPE, "READ_RELEASED_RECEIPT"),
                    name="aqp-rotation-release-receipt", daemon=True)
                _ROTATION_AUTH_THREAD = worker
                worker.start()
            else:
                job.update(receipt=job["pending_receipt"], pending_receipt=None, activation_request=None, status="ACTIVATED")
        except BaseException:
            _activation_receipt_unknown(job)
        return
    if phase != "AUTH" or job["status"] != "AUTH_PENDING":
        return
    try:
        if error is not None or _SUPERVISOR_ATTACH_STOP.is_set():
            raise RuntimeError("read rotation supervisor authorization unknown")
        _verify_read_rotation_authorization(job["request"], job["evidence"], response)
        receipt = _read_rotation_control(job["command"], preauthorized=True)
        expected = {"PREPARE": "PREPARED", "COMMIT": "COMMITTED", "ACTIVATE": "ACTIVATED", "CLOSE": "CLOSED"}[key[1]]
        if not isinstance(receipt, dict) or receipt.get("status") != expected:
            raise RuntimeError("read rotation control outcome unknown")
        if not _execution_continuity_clear():
            job["prior_receipt"] = receipt
            raise RuntimeError("read rotation execution continuity became unknown")
        if key[1] == "ACTIVATE":
            frame = _native_activation_receipt("ACTIVATION_RECEIPT")
            job.update(status="ACTIVATION_PENDING", pending_receipt=receipt, activation_request=frame)
            worker = threading.Thread(target=_rotation_authorization_worker,
                args=(key, frame, _SUPERVISOR_PIPE, "ACTIVATION_RECEIPT"),
                name="aqp-rotation-activation-receipt", daemon=True)
            _ROTATION_AUTH_THREAD = worker
            worker.start()
        elif key[1] == "CLOSE":
            frame = _native_retirement_receipt()
            job.update(status="RETIRE_ACK_PENDING", pending_receipt=receipt, activation_request=frame)
            worker = threading.Thread(target=_rotation_authorization_worker,
                args=(key, frame, _SUPERVISOR_PIPE, "GENERATION_RETIRED_RECEIPT"),
                name="aqp-rotation-retirement-receipt", daemon=True)
            _ROTATION_AUTH_THREAD = worker
            worker.start()
        else:
            job.update(receipt=receipt, status=expected)
    except BaseException:
        if key[1] == "CLOSE":
            _retirement_unknown(job)
        elif key[1] == "ACTIVATE" and _READ_ROTATION is not None and _READ_ROTATION.get("state") in ("ACTIVATED_RESTRICTED", "ACTIVATED"):
            _activation_receipt_unknown(job)
        else:
            job.update(receipt=None, status="UNKNOWN")
    finally:
        # capability 仅用于本次管道帧，不进入状态回执或持久存储。
        job.update(command=None, request=None)


def _rotation_state(state):
    _READ_ROTATION["state"] = state
    execution = _RUNTIMES["execution"]["_SERVER"]
    if execution is not None:
        execution.bridge.runtime_metadata["read_rotation_status"] = dict(
            contract="READ_ROTATION_CONTROL_V1", state=state,
            transition_id=_READ_ROTATION["transition_id"],
            old_read_session_id=_READ_ROTATION["old_evidence"]["read_session_id"],
            candidate_read_session_id=_READ_ROTATION.get("candidate_read_session_id"))


def _record_rotation_continuity_loss():
    if (_READ_ROTATION is None and not _RETIRED_READ_ROTATIONS) or _execution_continuity_clear():
        return
    execution = _RUNTIMES["execution"]["_SERVER"]
    if execution is None:
        return
    bridge = execution.bridge
    bridge.runtime_metadata["read_rotation_continuity"] = "UNKNOWN"
    if bridge._off_thread_event_loss.is_set():
        bridge._flush_off_thread_event_loss()
    if _READ_ROTATION is None:
        retired = _RETIRED_READ_ROTATIONS[-1]
        read = _RUNTIMES["read"]["_SERVER"]
        if read is not None and read.mailbox is retired.get("candidate"):
            read.rotation_probe_request_id = retired["probe_request_id"]
        execution.bridge.runtime_metadata["read_rotation_status"] = dict(
            contract="READ_ROTATION_CONTROL_V1", state="CONTINUITY_UNKNOWN",
            transition_id=retired["transition_id"], old_read_session_id=retired["old_evidence"]["read_session_id"],
            candidate_read_session_id=retired["candidate_read_session_id"])
    elif _READ_ROTATION.get("state") in ("ACTIVATED_RESTRICTED", "ACTIVATED", "RETIRED_RESTRICTED", "CLOSE_UNKNOWN"):
        read = _RUNTIMES["read"]["_SERVER"]
        if read is not None:
            read.rotation_probe_request_id = _READ_ROTATION["probe_request_id"]
        _rotation_state("CONTINUITY_UNKNOWN")
    for job in _ROTATION_CONTROL_JOBS.values():
        if job["status"] in ("AUTH_PENDING", "ACTIVATION_PENDING", "READ_RELEASE_PENDING", "RETIRE_ACK_PENDING", "ACTIVATED", "CLOSED"):
            if job["receipt"] is not None:
                job["prior_receipt"] = job["receipt"]
            if job.get("pending_receipt") is not None:
                job["prior_receipt"] = job["pending_receipt"]
            job.update(status="UNKNOWN", receipt=None, pending_receipt=None, activation_request=None,
                       command=None, request=None)


def _rotation_probe_locked(read_server, candidate, probe_id, probe_sequence):
    transport = _RUNTIMES["read"]["_load_embedded"]("aqp_readonly_transport")
    header = candidate._header()
    if (probe_sequence != 1 or header[2] != transport.READY or header[4:7] != [1, 1, 1]
            or not 0 < header[7] <= transport.REQUEST_CAPACITY
            or not 0 < header[8] <= transport.RESPONSE_CAPACITY or read_server.pending is not None):
        return False
    request_raw = candidate.region.read(transport.REQUEST_OFFSET, header[7])
    response_raw = candidate.region.read(transport.RESPONSE_OFFSET, header[8])
    if candidate.last_request_frame != (1, request_raw) or candidate.last_response_frame != (1, response_raw):
        return False
    request = transport.decode_payload(request_raw)
    response = transport.decode_payload(response_raw)
    if (not isinstance(request, dict) or request.get("request_id") != probe_id or request.get("method") != "ping"
            or request.get("account_id") != DUAL_SHM_SETTINGS["account_id"]
            or request.get("environment", None if DUAL_SHM_SETTINGS["environment"] == "LIVE" else "SIM") != DUAL_SHM_SETTINGS["environment"]
            or request.get("session_id") != candidate.session_id or request.get("sequence") != 1):
        return False
    transport.ShmReadOnlyClient._validate_response(None, request, response)
    data = response.get("data")
    return (response.get("ok") is True and isinstance(data, dict) and data.get("pong") is True
            and data.get("account_id") == DUAL_SHM_SETTINGS["account_id"]
            and data.get("environment") == DUAL_SHM_SETTINGS["environment"]
            and data.get("session_id") == candidate.session_id and data.get("native_pid") == _BOUND_NATIVE_PID
            and data.get("windows_session_id") == DUAL_SHM_SETTINGS["windows_session"]
            and data.get("native_creation_filetime") == _BOUND_NATIVE_CREATION_FILETIME
            and data.get("read_fault_isolation_contract") == "READ_FAILURE_ISOLATION_V1"
            and data.get("allow_order_methods") is False and read_server.rotation_probe_used)


def _read_rotation_control(command, preauthorized=False):
    global _READ_ROTATION, _SOURCE_IDENTITY
    if (not isinstance(command, dict) or command.get("contract") != "READ_ROTATION_CONTROL_V1"
            or command.get("action") not in ("PREPARE", "COMMIT", "ACTIVATE", "CLOSE")):
        raise ValueError("explicit read rotation control required")
    action = command["action"]
    transition = command.get("transition_id")
    if not isinstance(transition, str) or len(transition) != 32 or any(c not in "0123456789abcdef" for c in transition):
        raise ValueError("explicit unique read transition id required")
    nonce, capability = command.get("nonce"), command.get("capability")
    if (not isinstance(nonce, str) or len(nonce) != 32 or any(c not in "0123456789abcdef" for c in nonce)
            or not isinstance(capability, str) or len(capability) != 64
            or any(c not in "0123456789abcdef" for c in capability)):
        raise ValueError("explicit per-stage nonce and capability required")
    if preauthorized is not True and (not callable(_READ_ROTATION_AUTHORIZER) or _READ_ROTATION_AUTHORIZER(command) is not True):
        raise RuntimeError("read rotation independent authorization is unavailable")
    if action == "CLOSE" and preauthorized is not True:
        raise RuntimeError("read rotation retirement requires independent supervisor authorization")
    read = _RUNTIMES["read"]["_SERVER"]
    execution = _RUNTIMES["execution"]["_SERVER"]
    if (read is None or execution is None or execution.closed or _CLOSED or _READ_FAULT is not None
            or DUAL_SHM_SETTINGS["native_pid"] != 0 or _SOURCE_IDENTITY is None
            or DUAL_SHM_SETTINGS["source_identity_path"] is None
            or not (DUAL_SHM_SETTINGS["persistent_readonly"] or DUAL_SHM_SETTINGS["persistent_simulation_read_write"]
                    or DUAL_SHM_SETTINGS["persistent_live_read_write"])
            or _verify_identity() != _BOUND_NATIVE_PID or type(_BOUND_NATIVE_CREATION_FILETIME) is not int
            or _BOUND_NATIVE_CREATION_FILETIME <= 0 or _native_creation_filetime() != _BOUND_NATIVE_CREATION_FILETIME):
        raise RuntimeError("read rotation native identity is unverified")
    identity_path = DUAL_SHM_SETTINGS["source_identity_path"]
    if action == "CLOSE":
        expected = {"contract", "action", "transition_id", "nonce", "capability", "gateway_verified_sha256"}
        digest = command.get("gateway_verified_sha256")
        activated = _ROTATION_CONTROL_JOBS.get((transition, "ACTIVATE"))
        rotation = _READ_ROTATION
        if (set(command) != expected or not isinstance(digest, str) or len(digest) != 64
                or any(c not in "0123456789abcdef" for c in digest)
                or not isinstance(rotation, dict) or rotation.get("state") != "ACTIVATED"
                or rotation.get("transition_id") != transition or not isinstance(activated, dict)
                or activated.get("status") != "ACTIVATED" or not isinstance(activated.get("receipt"), dict)
                or read.mailbox is not rotation.get("candidate") or read.rotation_probe_request_id is not None
                or _SOURCE_IDENTITY.get("read_session_id") != read.mailbox.session_id
                or _SOURCE_IDENTITY.get("execution_session_id") != execution.bridge.mailbox.session_id
                or not _execution_continuity_clear()):
            raise RuntimeError("read rotation retirement identity unavailable")
        old = rotation["old_mailbox"]
        if not old.region.acquire(0):
            raise RuntimeError("old read mailbox unavailable for retirement")
        try:
            evidence = read.read_rotation_evidence(execution.bridge.mailbox.session_id,
                _BOUND_NATIVE_CREATION_FILETIME, mailbox=old, completed=rotation["old_completed"], mailbox_locked=True)
            if evidence != rotation["old_evidence"] or not _execution_continuity_clear():
                raise RuntimeError("old read retirement evidence changed")
            with open(identity_path, "r") as stream:
                if json.load(stream) != _SOURCE_IDENTITY:
                    raise RuntimeError("published source identity changed")
            rotation["gateway_verified_sha256"] = digest
            _rotation_state("RETIRED_RESTRICTED")
            if not _execution_continuity_clear():
                _rotation_state("CLOSE_UNKNOWN")
                raise RuntimeError("execution continuity changed during retirement")
            return dict(contract="READ_ROTATION_CONTROL_V1", action="CLOSE", status="CLOSED",
                transition_id=transition, account_id=DUAL_SHM_SETTINGS["account_id"],
                environment=DUAL_SHM_SETTINGS["environment"], native_pid=_BOUND_NATIVE_PID,
                native_creation_filetime=_BOUND_NATIVE_CREATION_FILETIME,
                windows_session_id=DUAL_SHM_SETTINGS["windows_session"],
                old_read_session_id=evidence["read_session_id"], candidate_read_session_id=read.mailbox.session_id,
                execution_session_id=execution.bridge.mailbox.session_id, old_request_id=evidence["request_id"],
                old_sequence=evidence["sequence"], probe_request_id=rotation["probe_request_id"],
                probe_sequence=1, gateway_verified_sha256=digest)
        finally:
            old.region.release()
    if action == "PREPARE":
        if set(command) != {"contract", "action", "transition_id", "nonce", "capability", "old_evidence"} or _READ_ROTATION is not None:
            raise RuntimeError("read rotation prepare is not available or already attempted")
        evidence = _read_rotation_evidence()
        if not isinstance(evidence, dict) or evidence != command["old_evidence"]:
            raise RuntimeError("old read completion evidence changed")
        with open(identity_path, "r") as stream:
            if json.load(stream) != _SOURCE_IDENTITY:
                raise RuntimeError("published source identity changed")
        old_name = _RUNTIMES["read"]["READONLY_SHM_NAME"]
        if _SOURCE_IDENTITY.get("read_name") != old_name:
            raise RuntimeError("published old read name changed")
        old_mailbox = read.mailbox
        _READ_ROTATION = dict(transition_id=transition, old_evidence=dict(evidence), old_read_name=old_name,
                              old_mailbox=old_mailbox, old_completed=dict(read.last_completed_request))
        _rotation_state("PREPARE_PENDING")
        candidate, transport = None, None
        try:
            name = _BASE_READ_NAME + "_" + uuid.uuid4().hex[:16]
            transport = _RUNTIMES["read"]["_load_embedded"]("aqp_readonly_transport")
            candidate = transport.ReadOnlyMailbox(name, create=True)
            candidate.retain_frame_evidence = True
            candidate.mark_ready()
            if not old_mailbox.region.acquire(0):
                raise RuntimeError("old read mailbox lock unavailable during prepare")
            try:
                checked = read.read_rotation_evidence(execution.bridge.mailbox.session_id,
                    _BOUND_NATIVE_CREATION_FILETIME, mailbox=old_mailbox,
                    completed=_READ_ROTATION["old_completed"], mailbox_locked=True)
                previous_sessions = {item["old_evidence"]["read_session_id"] for item in _RETIRED_READ_ROTATIONS}
                previous_sessions.update(item["candidate_read_session_id"] for item in _RETIRED_READ_ROTATIONS)
                previous_sessions.update((old_mailbox.session_id, execution.bridge.mailbox.session_id))
                if (candidate.server_pid != _BOUND_NATIVE_PID or candidate.session_id in previous_sessions or checked != evidence
                        or not _execution_continuity_clear()):
                    raise RuntimeError("candidate or old source changed during prepare")
                probe_id = "read-rotation-" + transition
                token = uuid.uuid4().hex
                read.retired_request_ids.update(read.seen_request_ids)
                read.retired_request_ids.add(evidence["request_id"])
                read.seen_request_ids = {}
                read.last_completed_request = None
                read.rotation_probe_request_id = probe_id
                read.rotation_probe_used = False
                read.mailbox = candidate
                _READ_ROTATION.update(candidate=candidate, candidate_read_name=name,
                                      candidate_read_session_id=candidate.session_id,
                                      probe_request_id=probe_id, token=token)
                if not _execution_continuity_clear():
                    raise RuntimeError("execution continuity changed during prepare")
                _rotation_state("PREPARED")
            finally:
                old_mailbox.region.release()
            return dict(contract="READ_ROTATION_CONTROL_V1", action=action, status="PREPARED",
                        transition_id=transition, old_read_name=old_name, old_evidence=dict(evidence),
                        candidate_read_name=name, candidate_read_session_id=candidate.session_id,
                        probe_request_id=probe_id, token=token)
        except Exception as exc:
            if candidate is not None and read.mailbox is not candidate:
                try:
                    candidate.close(failed=True)
                except Exception:
                    pass
            capacity = None
            if (candidate is None and transport is not None and isinstance(exc, transport.ShmNativeError)
                    and getattr(exc, "native_operation", None) == "CreateFileMappingW"
                    and type(getattr(exc, "native_winerror", None)) is int
                    and exc.native_winerror in (8, 14, 1455) and read.mailbox is old_mailbox):
                try:
                    if old_mailbox.region.acquire(0):
                        try:
                            checked = read.read_rotation_evidence(execution.bridge.mailbox.session_id,
                                _BOUND_NATIVE_CREATION_FILETIME, mailbox=old_mailbox,
                                completed=_READ_ROTATION["old_completed"], mailbox_locked=True)
                            with open(identity_path, "r") as stream:
                                published = json.load(stream)
                            if (checked == evidence and published == _SOURCE_IDENTITY
                                    and _verify_identity() == _BOUND_NATIVE_PID
                                    and _native_creation_filetime() == _BOUND_NATIVE_CREATION_FILETIME
                                    and _execution_continuity_clear()):
                                capacity = dict(contract="READ_ROTATION_CAPACITY_V1",
                                    status="READ_ROTATION_CAPACITY_BLOCKED", native_winerror=exc.native_winerror,
                                    native_operation="CreateFileMappingW", transition_id=transition,
                                    account_id=DUAL_SHM_SETTINGS["account_id"], environment=DUAL_SHM_SETTINGS["environment"],
                                    native_pid=_BOUND_NATIVE_PID, native_creation_filetime=_BOUND_NATIVE_CREATION_FILETIME,
                                    windows_session_id=DUAL_SHM_SETTINGS["windows_session"],
                                    old_read_name=old_name, old_read_session_id=evidence["read_session_id"],
                                    old_request_id=evidence["request_id"], old_sequence=evidence["sequence"],
                                    execution_session_id=execution.bridge.mailbox.session_id,
                                    old_evidence=dict(evidence), old_frame_unchanged=True, automatic_retry=False)
                        finally:
                            old_mailbox.region.release()
                except Exception:
                    capacity = None
            if capacity is not None:
                execution.bridge.runtime_metadata["read_rotation_capacity_evidence"] = capacity
            _rotation_state("PREPARE_CAPACITY_BLOCKED" if capacity is not None else "PREPARE_UNKNOWN")
            raise
    required = ({"contract", "action", "transition_id", "nonce", "capability", "token", "probe_request_id", "probe_sequence"}
                if action == "COMMIT" else {"contract", "action", "transition_id", "nonce", "capability", "token"})
    if (set(command) != required or _READ_ROTATION is None or transition != _READ_ROTATION["transition_id"]
            or command.get("token") != _READ_ROTATION.get("token")
            or _READ_ROTATION["state"] != ("PREPARED" if action == "COMMIT" else "COMMITTED")):
        raise RuntimeError("read rotation state or token mismatch; do not retry")
    probe_id = _READ_ROTATION["probe_request_id"]
    probe_sequence = command.get("probe_sequence", 1)
    if action == "COMMIT" and (command.get("probe_request_id") != probe_id or type(probe_sequence) is not int or probe_sequence != 1):
        raise RuntimeError("candidate probe identity mismatch")
    old, candidate = _READ_ROTATION["old_mailbox"], _READ_ROTATION["candidate"]
    _rotation_state("COMMIT_PENDING" if action == "COMMIT" else "ACTIVATE_PENDING")
    if read.mailbox is not candidate or not old.region.acquire(0):
        raise RuntimeError("old read mailbox lock unavailable")
    try:
        if not candidate.region.acquire(0):
            raise RuntimeError("candidate mailbox lock unavailable")
        try:
            evidence = read.read_rotation_evidence(execution.bridge.mailbox.session_id,
                _BOUND_NATIVE_CREATION_FILETIME, mailbox=old, completed=_READ_ROTATION["old_completed"], mailbox_locked=True)
            if (evidence != _READ_ROTATION["old_evidence"]
                    or not _rotation_probe_locked(read, candidate, probe_id, probe_sequence)
                    or not _execution_continuity_clear()):
                raise RuntimeError("old read or candidate probe evidence changed")
            with open(identity_path, "r") as stream:
                if json.load(stream) != _SOURCE_IDENTITY:
                    raise RuntimeError("published source identity changed")
            if action == "COMMIT":
                old_evidence = _READ_ROTATION["old_evidence"]
                identity = dict(_SOURCE_IDENTITY, read_name=_READ_ROTATION["candidate_read_name"],
                                read_session_id=candidate.session_id,
                                read_rotation=dict(contract="READ_ROTATION_COMMIT_V1", state="COMMITTED",
                                    transition_id=transition, old_read_name=_READ_ROTATION["old_read_name"],
                                    old_read_session_id=old.session_id, old_request_id=old_evidence["request_id"],
                                    old_method=old_evidence["method"], old_sequence=old_evidence["sequence"],
                                    old_expires_at_unix=old_evidence["read_expires_at_unix"],
                                    new_probe_request_id=probe_id, new_probe_sequence=1,
                                    native_creation_filetime=_BOUND_NATIVE_CREATION_FILETIME))
                try:
                    temporary = identity_path + ".rotation." + transition + ".new"
                    with open(temporary, "x") as stream:
                        json.dump(identity, stream, sort_keys=True)
                    if not _execution_continuity_clear():
                        raise RuntimeError("execution continuity changed before publication")
                    os.replace(temporary, identity_path)
                    if not _execution_continuity_clear():
                        raise RuntimeError("execution continuity changed during publication")
                except Exception:
                    _rotation_state("COMMIT_UNKNOWN")
                    raise
                _SOURCE_IDENTITY = identity
                _RUNTIMES["read"]["READONLY_SHM_NAME"] = _READ_ROTATION["candidate_read_name"]
                execution.bridge.runtime_metadata["native_read_session_id"] = candidate.session_id
                execution.bridge.runtime_metadata["native_read_source_state"] = "UNKNOWN"
                _rotation_state("COMMITTED")
                _refresh_read_rotation_capability()
            else:
                if preauthorized is True:
                    # 候选 ping 原帧必须保留到监督器独立确认；后台回执前不接业务请求。
                    _rotation_state("ACTIVATED_RESTRICTED")
                else:
                    # 仅用于隔离模型注入的同步授权，真实生成源默认不开放此路径。
                    read.rotation_probe_request_id = None
                    if not _execution_continuity_clear():
                        read.rotation_probe_request_id = probe_id
                        _rotation_state("ACTIVATE_UNKNOWN")
                        raise RuntimeError("execution continuity changed during activation")
                    _rotation_state("ACTIVATED")
            return dict(contract="READ_ROTATION_CONTROL_V1", action=action,
                        status="ACTIVATED" if action == "ACTIVATE" and preauthorized is True else _READ_ROTATION["state"],
                        transition_id=transition,
                        candidate_read_name=_READ_ROTATION["candidate_read_name"],
                        candidate_read_session_id=candidate.session_id,
                        execution_session_id=execution.bridge.mailbox.session_id)
        finally:
            candidate.region.release()
    finally:
        old.region.release()


def init(ContextInfo):
    global _INIT_ATTEMPTED, _STARTED_AT, _STARTED_AT_UNIX, _SOURCE_DEADLINE, _BOUND_NATIVE_PID, _BOUND_NATIVE_CREATION_FILETIME, _SOURCE_IDENTITY, _SUPERVISOR_PROCESS, _SUPERVISOR_IDENTITY
    if _INIT_ATTEMPTED or _CLOSED:
        raise RuntimeError("init may only be attempted once; build a fresh source")
    _INIT_ATTEMPTED = True
    try:
        _BOUND_NATIVE_PID = _verify_identity()
        _BOUND_NATIVE_CREATION_FILETIME = (_native_creation_filetime() if DUAL_SHM_SETTINGS["read_fault_isolation"] else None)
        if DUAL_SHM_SETTINGS["native_pid"] == 0:
            # 新实例使用独立邮箱；旧消费者即使尚未退出，也不能阻止新来源初始化或覆盖旧请求。
            suffix = "_" + uuid.uuid4().hex[:16]
            for key, field in (("read", "READONLY_SHM_NAME"), ("execution", "SHM_RPC_NAME")):
                _RUNTIMES[key][field] = _RUNTIMES[key][field] + suffix
        _STARTED_AT = time.monotonic()
        _STARTED_AT_UNIX = time.time()
        live_write = DUAL_SHM_SETTINGS["persistent_live_read_write"]
        read_write = DUAL_SHM_SETTINGS["persistent_simulation_read_write"] or live_write
        environment = DUAL_SHM_SETTINGS["environment"]
        account = DUAL_SHM_SETTINGS["account_id"]
        if (environment not in ("SIM", "LIVE") or type(live_write) is not bool
                or type(DUAL_SHM_SETTINGS["read_fault_isolation"]) is not bool
                or DUAL_SHM_SETTINGS["read_fault_isolation"] and not (
                    environment == "SIM" and account == "55013746" and
                    (DUAL_SHM_SETTINGS["persistent_readonly"] is True or DUAL_SHM_SETTINGS["persistent_simulation_read_write"] is True)
                    or environment == "LIVE" and account == "8882152350" and live_write is True)
                or type(DUAL_SHM_SETTINGS["execution_event_guard"]) is not bool
                or DUAL_SHM_SETTINGS["execution_event_guard"] and not (
                    environment == "SIM" and account == "55013746" and DUAL_SHM_SETTINGS["persistent_simulation_read_write"] is True
                    or environment == "LIVE" and account == "8882152350" and live_write is True)
                or sum((DUAL_SHM_SETTINGS["persistent_readonly"], DUAL_SHM_SETTINGS["persistent_simulation_read_write"], live_write)) > 1
                or environment == "LIVE" and (not live_write or not isinstance(account, str) or not account
                    or len(account) > 64 or account == "55013746" or not all("0" <= char <= "9" for char in account))
                or environment == "SIM" and (live_write or account != "55013746")):
            raise RuntimeError("fixed account/environment policy changed")
        persistent = DUAL_SHM_SETTINGS["persistent_readonly"] or read_write
        if persistent:
            if (DUAL_SHM_SETTINGS["hard_deadline_unix"] is not None
                    or DUAL_SHM_SETTINGS["max_seconds"] != 0 or DUAL_SHM_SETTINGS["max_requests_per_source"] != 0
                    or DUAL_SHM_SETTINGS["persistent_readonly"] and DUAL_SHM_SETTINGS["market_rpc_enabled"] is not False):
                raise RuntimeError("persistent runtime policy changed")
            _SOURCE_DEADLINE = None
            remaining = 0
        else:
            _SOURCE_DEADLINE = min(DUAL_SHM_SETTINGS["hard_deadline_unix"], _STARTED_AT_UNIX + DUAL_SHM_SETTINGS["max_seconds"])
            remaining = int(_SOURCE_DEADLINE - time.time())
            if remaining < 1:
                raise RuntimeError("source deadline expired before initialization")
        query = globals().get("get_trade_detail_data")
        if not callable(query) or not callable(getattr(ContextInfo, "set_account", None)) or not callable(getattr(ContextInfo, "run_time", None)):
            raise RuntimeError("native account/query/timer APIs required")
        execution = _RUNTIMES["execution"]
        expected_factory = execution["create_native_stock_order_gateway"] if read_write else None
        if (execution["SHM_RPC_ALLOW_ORDER_METHODS"] is not read_write or execution["SHM_RPC_BIND_ACCOUNT_CALLBACKS"] is not False
                or execution["SHM_RPC_ORDER_GATEWAY"] is not None or execution["SHM_RPC_ORDER_GATEWAY_FACTORY"] is not expected_factory):
            raise RuntimeError("readonly execution source gate changed")
        if (execution["SHM_RPC_ACCOUNT_ALREADY_BOUND"] is not read_write
                or execution["SHM_RPC_PERSISTENT_SIMULATION_READ_WRITE"] is not DUAL_SHM_SETTINGS["persistent_simulation_read_write"]
                or execution["SHM_RPC_PERSISTENT_LIVE_READ_WRITE"] is not live_write
                or execution["SHM_RPC_EXECUTION_EVENT_GUARD"] is not DUAL_SHM_SETTINGS["execution_event_guard"]
                or execution["BIGQMT_ENVIRONMENT"] != environment or execution["BIGQMT_ACCOUNT_ID"] != account
                or execution["SHM_RPC_PERSISTENT_READONLY"] is not DUAL_SHM_SETTINGS["persistent_readonly"]):
            raise RuntimeError("execution source runtime policy changed")
        read = _RUNTIMES["read"]
        if (read["READONLY_PERSISTENT_READONLY"] is not persistent or read["READONLY_ENABLE_MARKET_RPC"] is not DUAL_SHM_SETTINGS["market_rpc_enabled"]
                or read["READONLY_PERSISTENT_MARKET_RPC"] is not (read_write and DUAL_SHM_SETTINGS["market_rpc_enabled"])
                or read["READONLY_PERSISTENT_LIVE_READ_WRITE"] is not live_write
                or read["READONLY_PRESERVE_FAILED_MAILBOX"] is not DUAL_SHM_SETTINGS["read_fault_isolation"]
                or read["BIGQMT_ENVIRONMENT"] != environment or read["BIGQMT_ACCOUNT_ID"] != account):
            raise RuntimeError("readonly source runtime policy changed")
        if read_write and (not callable(globals().get("passorder")) or not callable(globals().get("cancel"))):
            raise RuntimeError("explicit native %s order/cancel functions required" % ("LIVE" if live_write else "simulation"))
        ContextInfo.set_account(DUAL_SHM_SETTINGS["account_id"])
        _RUNTIMES["read"]["READONLY_MAX_SECONDS"] = remaining
        execution["SHM_RPC_MAX_SECONDS"] = remaining
        for key in ("read", "execution"):
            _RUNTIMES[key]["get_trade_detail_data"] = query
            if key == "read":
                _RUNTIMES[key]["download_history_data"] = globals().get("download_history_data")
            if key == "execution" and read_write:
                _RUNTIMES[key]["passorder"] = globals().get("passorder")
                _RUNTIMES[key]["cancel"] = globals().get("cancel")
            _RUNTIMES[key]["init"](ContextInfo)
        read_server = _RUNTIMES["read"]["_SERVER"]
        execution_server = execution["_SERVER"]
        if read_server.mailbox.server_pid != _BOUND_NATIVE_PID or execution_server.bridge.mailbox.server_pid != _BOUND_NATIVE_PID or read_server.mailbox.session_id == execution_server.bridge.mailbox.session_id:
            raise RuntimeError("dual source PID/session identity mismatch")
        if (read_server.windows_session_id != DUAL_SHM_SETTINGS["windows_session"]
                or execution_server.bridge.runtime_metadata["windows_session_id"] != DUAL_SHM_SETTINGS["windows_session"]):
            raise RuntimeError("dual source actual Windows session mismatch")
        if DUAL_SHM_SETTINGS["read_fault_isolation"]:
            execution_server.bridge.runtime_metadata.update(
                read_fault_isolation_contract="READ_FAILURE_ISOLATION_V1",
                native_read_source_state="UNKNOWN", native_read_session_id=read_server.mailbox.session_id,
                native_creation_filetime=_BOUND_NATIVE_CREATION_FILETIME,
                read_rotation_authorization_ready=False)
            read_server.native_creation_filetime = _BOUND_NATIVE_CREATION_FILETIME
            if DUAL_SHM_SETTINGS["execution_event_guard"]:
                read_server.rotation_continuity_guard = _rotated_read_continuity_clear
                read_server.rotation_continuity_lock = execution_server.bridge._read_publication_lock
            execution_server.bridge.read_rotation_evidence_reader = _read_rotation_evidence
            execution_server.bridge.read_rotation_control = _dispatch_read_rotation_control
            execution_server.bridge.read_rotation_control_status_reader = _read_rotation_control_status
            execution_server.bridge.read_rotation_capability_control = _start_read_rotation_capability
            execution_server.bridge.read_rotation_capability_status_reader = _read_rotation_capability_status
            execution_server.bridge.supervisor_attach_control = _attach_supervisor
            execution_server.bridge.supervisor_attach_status_reader = _supervisor_attach_status
        read_capabilities = list(_READ_CAPABILITIES)
        if read_server.market_enabled:
            read_capabilities += ["get_local_data", "get_market_data_ex", "subscribe_quote", "poll_quote_events", "unsubscribe_quote"]
            if callable(read_server.history_downloader):
                read_capabilities += ["download_history_data"]
        rpc_transport = execution["_load_embedded"]("bigqmt_signal_trader.transports.shm_rpc")
        execution_capabilities = sorted(rpc_transport.READ_METHODS | (rpc_transport.ALL_WRITE_METHODS if read_write else frozenset()))
        identity = dict(
            account_id=DUAL_SHM_SETTINGS["account_id"], environment=environment, native_pid=_BOUND_NATIVE_PID,
            windows_session=DUAL_SHM_SETTINGS["windows_session"],
            read_name=_RUNTIMES["read"]["READONLY_SHM_NAME"], execution_name=execution["SHM_RPC_NAME"],
            read_session_id=read_server.mailbox.session_id, execution_session_id=execution_server.bridge.mailbox.session_id,
            allow_order_methods=read_write, source_deadline_unix=_SOURCE_DEADLINE,
            runtime_policy="PERSISTENT_LIVE_READ_WRITE" if live_write else "PERSISTENT_SIMULATION_READ_WRITE" if read_write else "PERSISTENT_READONLY" if persistent else "BOUNDED_READONLY",
            started_at_unix=_STARTED_AT_UNIX, account_bound_once=True,
            account_callback_route="outer_to_execution", execution_bind_account_callbacks=False,
            max_seconds=DUAL_SHM_SETTINGS["max_seconds"], max_requests_per_source=DUAL_SHM_SETTINGS["max_requests_per_source"],
            market_rpc_enabled=read_server.market_enabled, read_capabilities=read_capabilities,
            quote_initial_symbol_count=len(read_server.initial_quote_symbols),
            quote_symbol_scope=read_server.quote_symbol_scope,
            quote_request_symbol_limit=read_server.quote_symbol_limit,
            execution_capabilities=execution_capabilities)
        if read_write:
            identity["async_submit_contract"] = rpc_transport.ASYNC_SUBMIT_CONTRACT
        if DUAL_SHM_SETTINGS["read_fault_isolation"]:
            identity["native_creation_filetime"] = _BOUND_NATIVE_CREATION_FILETIME
        identity_path = DUAL_SHM_SETTINGS["source_identity_path"]
        _SOURCE_IDENTITY = dict(identity, status="READY")
        if identity_path is not None:
            temporary = identity_path + ".new"
            with open(temporary, "w") as stream:
                json.dump(_SOURCE_IDENTITY, stream, sort_keys=True)
            os.replace(temporary, identity_path)
        print("AQP_DUAL_SHM_READY " + json.dumps(identity, sort_keys=True))
        launcher = DUAL_SHM_SETTINGS["runtime_launcher"]
        if launcher is not None:
            args = [launcher["python"], launcher["script"], "--config", launcher["config"]]
            if DUAL_SHM_SETTINGS["read_fault_isolation"]:
                # Python 3.6 可在不重定向标准句柄时禁止所有句柄继承。
                process = subprocess.Popen(args, close_fds=True, creationflags=0x00000200 | 0x00000008)
            else:
                process = subprocess.Popen(args, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL, close_fds=False, creationflags=0x00000200 | 0x00000008)
            if DUAL_SHM_SETTINGS["read_fault_isolation"]:
                _SUPERVISOR_PROCESS = process
                _SUPERVISOR_IDENTITY = _supervisor_identity(process)
        return None
    except BaseException:
        stop(ContextInfo)
        raise


def _within_budget(ContextInfo):
    if _CLOSED or _STARTED_AT is None:
        return False
    execution_server = _RUNTIMES["execution"]["_SERVER"]
    if (DUAL_SHM_SETTINGS["execution_event_guard"] and execution_server is not None
            and threading.get_ident() != execution_server.thread_id):
        # 进入 budget/read 状态及 stop 之前，异线程只能设置单调故障标记。
        execution_server.bridge.flag_off_thread_execution_event()
        return False
    if _OUTER_STOP_PENDING.is_set():
        stop(ContextInfo)
        return False
    _record_rotation_continuity_loss()
    _refresh_read_rotation_capability()
    if not (DUAL_SHM_SETTINGS["persistent_readonly"] or DUAL_SHM_SETTINGS["persistent_simulation_read_write"] or DUAL_SHM_SETTINGS["persistent_live_read_write"]) and (time.time() >= _SOURCE_DEADLINE or time.monotonic() - _STARTED_AT >= DUAL_SHM_SETTINGS["max_seconds"]):
        stop(ContextInfo)
        return False
    if (execution_server is not None and execution_server.closed or
            not DUAL_SHM_SETTINGS["read_fault_isolation"] and any(
                runtime["_SERVER"] is not None and runtime["_SERVER"].closed for runtime in _RUNTIMES.values())):
        stop(ContextInfo)
        return False
    read_server = _RUNTIMES["read"]["_SERVER"]
    if (DUAL_SHM_SETTINGS["read_fault_isolation"] and execution_server is not None and read_server is not None
            and (read_server.closed or read_server.failed)):
        execution_server.bridge.runtime_metadata["native_read_source_state"] = "RESULT_UNKNOWN"
    return True


def aqp_shm_readonly_adjust(ContextInfo):
    global _READ_FAULT
    if not DUAL_SHM_SETTINGS["read_fault_isolation"]:
        if _within_budget(ContextInfo):
            return _RUNTIMES["read"]["aqp_shm_readonly_adjust"](ContextInfo)
        return None
    if _within_budget(ContextInfo) and _READ_FAULT is None:
        try:
            return _RUNTIMES["read"]["aqp_shm_readonly_adjust"](ContextInfo)
        except Exception as exc:
            # Do not restart the read server or stop the execution callback route.
            # A read failure is not evidence of an execution channel or event gap.
            _READ_FAULT = {"status": "FAILED", "error_type": type(exc).__name__, "error": str(exc)}
            execution_server = _RUNTIMES["execution"]["_SERVER"]
            if execution_server is not None:
                execution_server.bridge.runtime_metadata["native_read_source_state"] = "RESULT_UNKNOWN"
            print("AQP_DUAL_SHM_READONLY_FAILED " + json.dumps(_READ_FAULT, sort_keys=True))


def aqp_shm_rpc_adjust(ContextInfo):
    if _within_budget(ContextInfo):
        _advance_supervisor_attach()
        _advance_read_rotation_capability()
        _advance_read_rotation_control()
        return _RUNTIMES["execution"]["aqp_shm_rpc_adjust"](ContextInfo)


def handlebar(ContextInfo):
    return None


def account_callback(ContextInfo, accountInfo):
    if _within_budget(ContextInfo):
        return _RUNTIMES["execution"]["account_callback"](ContextInfo, accountInfo)


def order_callback(ContextInfo, orderInfo):
    if _within_budget(ContextInfo):
        return _RUNTIMES["execution"]["order_callback"](ContextInfo, orderInfo)


def deal_callback(ContextInfo, dealInfo):
    if _within_budget(ContextInfo):
        return _RUNTIMES["execution"]["deal_callback"](ContextInfo, dealInfo)


def orderError_callback(ContextInfo, orderArgs, errMsg):
    if _within_budget(ContextInfo):
        return _RUNTIMES["execution"]["orderError_callback"](ContextInfo, orderArgs, errMsg)


def stop(ContextInfo):
    global _CLOSED, _SUPERVISOR_CLEANUP_STATUS
    if _CLOSED:
        return
    execution_server = _RUNTIMES["execution"]["_SERVER"]
    if (DUAL_SHM_SETTINGS["execution_event_guard"] and execution_server is not None
            and threading.get_ident() != execution_server.thread_id):
        execution_server.bridge.flag_off_thread_execution_event()
        _OUTER_STOP_PENDING.set()
        return
    _CLOSED = True
    errors = []
    _SUPERVISOR_ATTACH_STOP.set()
    try:
        pending_pipe, pending_gate, pending_error = _SUPERVISOR_ATTACH_RESULTS.popleft()
        if pending_error is not None:
            errors.append("supervisor-pending-attach:UNKNOWN")
        if not _dispose_supervisor_attach(pending_pipe, pending_gate):
            errors.append("supervisor-pending-cleanup:UNKNOWN")
    except IndexError:
        pass
    pending_auth = _ROTATION_AUTH_THREAD is not None and _ROTATION_AUTH_THREAD.is_alive()
    if pending_auth:
        try:
            _ROTATION_CONTROL_RESULTS.popleft()
            pending_auth = False
        except IndexError:
            pass
    pending_capability = _ROTATION_CAPABILITY_THREAD is not None and _ROTATION_CAPABILITY_THREAD.is_alive()
    if pending_capability:
        try:
            _ROTATION_CAPABILITY_RESULTS.popleft()
            pending_capability = False
        except IndexError:
            pass
    for job in _ROTATION_CONTROL_JOBS.values():
        job.update(command=None, request=None)
    resources = (("supervisor-pipe", None if pending_auth or pending_capability else _SUPERVISOR_PIPE),
                 ("supervisor-gate", _SUPERVISOR_GATE))
    for label, resource in resources:
        if resource is not None:
            try:
                resource.close()
            except BaseException as exc:
                errors.append(label + ":" + type(exc).__name__)
    for key in ("execution", "read"):
        try:
            _RUNTIMES[key]["stop"](ContextInfo)
        except BaseException as exc:
            errors.append(key + ":" + type(exc).__name__)
    if _READ_ROTATION is not None and _READ_ROTATION.get("old_mailbox") is not None:
        try:
            _READ_ROTATION["old_mailbox"].region.close()
        except BaseException as exc:
            errors.append("retired-read:" + type(exc).__name__)
    for rotation in _RETIRED_READ_ROTATIONS:
        if _READ_ROTATION is rotation:
            continue  # 关代中回报丢失时，活动代与已保留证据可能同一对象。
        try:
            rotation["old_mailbox"].region.close()
        except BaseException as exc:
            errors.append("retired-read:" + type(exc).__name__)
    pending_cleanup = any(worker is not None and worker.is_alive()
                          for worker in (_SUPERVISOR_ATTACH_THREAD, _ROTATION_AUTH_THREAD, _ROTATION_CAPABILITY_THREAD))
    status = ("UNKNOWN" if errors or (_SUPERVISOR_ATTACH_STATE is not None and
              _SUPERVISOR_ATTACH_STATE["status"] == "FAILED") else "PENDING" if pending_cleanup else "CLOSED")
    _SUPERVISOR_CLEANUP_STATUS = dict(contract="QMT_SUPERVISOR_PIPE_CLEANUP_V1", status=status,
                                      background_cleanup_pending=pending_cleanup, automatic_retry=False)
    if DUAL_SHM_SETTINGS["read_fault_isolation"]:
        print("AQP_SUPERVISOR_PIPE_CLEANUP " + json.dumps(_SUPERVISOR_CLEANUP_STATUS, sort_keys=True))
    if errors:
        raise RuntimeError("dual source cleanup incomplete: " + ",".join(errors))
'''


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--account-id", default=ACCOUNT)
    parser.add_argument("--environment", choices=("SIM", "LIVE"), default="SIM")
    parser.add_argument("--native-pid", type=int, required=True)
    parser.add_argument("--native-executable", help="native-pid=0 时固定实际QMT可执行文件，允许重启后重新核验PID")
    parser.add_argument("--source-identity-path", help="常驻来源原子发布实际PID、邮箱及双SID的绝对路径")
    parser.add_argument("--windows-session", type=int, required=True)
    parser.add_argument("--hard-deadline-unix", type=float)
    parser.add_argument("--read-name", required=True)
    parser.add_argument("--execution-name", required=True)
    parser.add_argument("--symbols", required=True, help="已确认白名单代码，逗号分隔")
    parser.add_argument("--max-seconds", type=int, default=900)
    parser.add_argument("--max-requests", type=int, default=1000)
    parser.add_argument("--enable-market-rpc", action="store_true", help="复用已有市场读能力；默认关闭")
    parser.add_argument("--persistent-readonly", action="store_true", help="仅55013746禁写驻留，必须显式max-seconds=0/max-requests=0且无deadline")
    parser.add_argument("--persistent-simulation-read-write", action="store_true", help="显式55013746模拟读写驻留；读源禁写，执行源启用普通股票限价gateway；必须零预算无deadline")
    parser.add_argument("--persistent-live-read-write", action="store_true", help="显式实盘常驻读写候选；仅构建不启动，必须零预算无deadline")
    parser.add_argument("--execution-event-guard", action="store_true", help="仅显式目标账户启用回报游标写门")
    parser.add_argument("--read-fault-isolation", action="store_true", help="仅显式目标账户启用只读故障隔离")
    args = parser.parse_args()
    source = build_source(account_id=args.account_id, native_pid=args.native_pid,
        windows_session=args.windows_session, hard_deadline_unix=args.hard_deadline_unix,
        read_name=args.read_name, execution_name=args.execution_name, symbols=args.symbols.split(","),
        max_seconds=args.max_seconds, max_requests=args.max_requests, enable_market_rpc=args.enable_market_rpc,
        persistent_readonly=args.persistent_readonly,
        persistent_simulation_read_write=args.persistent_simulation_read_write,
        environment=args.environment, persistent_live_read_write=args.persistent_live_read_write,
        native_executable=args.native_executable, source_identity_path=args.source_identity_path,
        execution_event_guard=args.execution_event_guard, read_fault_isolation=args.read_fault_isolation)
    descriptor = os.open(str(args.output), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as stream:
        stream.write(source)
    print("built dual SHM source: %s (%d bytes); no native process started" % (args.output, len(source.encode("utf-8"))))


if __name__ == "__main__":
    main()
