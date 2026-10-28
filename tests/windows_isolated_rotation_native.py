# coding: utf-8
"""独立 Python 3.6 驱动生成源；所有行情/账户/交易入口均为本地替身。"""

import builtins
import ctypes
import io
import json
import ntpath
import os
from pathlib import Path
import re
import socket
import subprocess
import sys
import threading
import time


def _resp(sock, parts):
    encoded = [part.encode("utf-8") for part in parts]
    raw = ("*%d\r\n" % len(encoded)).encode("ascii")
    for part in encoded:
        raw += ("$%d\r\n" % len(part)).encode("ascii") + part + b"\r\n"
    sock.sendall(raw)
    stream = sock.makefile("rb")
    line = stream.readline()
    if not line:
        raise RuntimeError("isolated Redis reply unavailable")
    if line[:1] == b"+":
        return line[1:-2]
    if line[:1] == b"$":
        size = int(line[1:-2])
        return None if size < 0 else stream.read(size + 2)[:-2]
    raise RuntimeError("isolated Redis reply invalid")


def verify_environment():
    env = os.environ
    run_id = env.get("AQP_ISOLATED_RUN_ID", "")
    root = Path(env.get("AQP_ISOLATED_ROOT", ""))
    expected_root = Path(env.get("TEMP", "")) / ("aqp-rotation-" + run_id)
    source = Path(env.get("AQP_ISOLATED_SOURCE_PATH", ""))
    native_source = Path(env.get("AQP_ISOLATED_NATIVE_SOURCE", ""))
    port = int(env.get("AQP_ISOLATED_REDIS_PORT", "0"))
    db = int(env.get("AQP_ISOLATED_REDIS_DB", "0"))
    container = env.get("AQP_ISOLATED_REDIS_CONTAINER_ID", "")
    if (os.name != "nt" or sys.version_info[:2] != (3, 6) or ctypes.sizeof(ctypes.c_void_p) != 8
            or re.fullmatch(r"[0-9a-f]{32}", run_id) is None or re.fullmatch(r"[0-9a-f]{64}", container) is None
            or root.resolve() != expected_root.resolve() or source.parent.resolve() != root.resolve()
            or native_source.parent.resolve() != root.resolve() or port <= 0 or port == 6379 or not 1 <= db <= 15
            or ntpath.normcase(sys.executable) != ntpath.normcase(env.get("AQP_ISOLATED_NATIVE_EXE", ""))):
        raise RuntimeError("isolated native environment mismatch")
    password = env.get("AQP_ISOLATED_REDIS_PASSWORD", "")
    if not password:
        raise RuntimeError("isolated Redis authentication unavailable")
    sock = socket.create_connection(("127.0.0.1", port), timeout=3)
    try:
        if (_resp(sock, ["AUTH", password]) != b"OK" or _resp(sock, ["SELECT", str(db)]) != b"OK"
                or _resp(sock, ["GET", "aqp:isolated:" + run_id + ":owner"]) != run_id.encode("ascii")):
            raise RuntimeError("isolated Redis owner proof unavailable")
    finally:
        sock.close()
    return run_id, root, source, native_source


class SyntheticContext:
    def __init__(self, account, scenario):
        self.account, self.quote_calls, self.financial_calls = account, 0, 0
        self.delayed_quote_calls = (1, 3) if scenario == "two_generations" else (1,)

    def set_account(self, account):
        if account != self.account:
            raise RuntimeError("isolated account identity mismatch")

    def run_time(self, *args):
        return None

    def get_full_tick(self, codes):
        self.quote_calls += 1
        if self.quote_calls in self.delayed_quote_calls:
            time.sleep(1.0)  # 在独立测试进程中制造有界迟到，不调用 QMT。
        now = int(time.time() * 1000)
        return {code: {"lastPrice": 10.0, "time": now} for code in codes}

    def forbidden_write(self, *args, **kwargs):
        self.financial_calls += 1
        raise RuntimeError("financial API forbidden in isolated test")


def _query(account, account_type, kind, strategy_name=""):
    if account != "8882152350" or account_type != "STOCK":
        raise RuntimeError("isolated query identity mismatch")
    return []


def _isolated_identity():
    path = ctypes.create_unicode_buffer(32768)
    length = ctypes.windll.kernel32.GetModuleFileNameW(None, path, len(path))
    session = ctypes.c_uint32()
    if (not length or length >= len(path) or ntpath.normcase(path.value) != ntpath.normcase(os.environ["AQP_ISOLATED_NATIVE_EXE"])
            or not ctypes.windll.kernel32.ProcessIdToSessionId(os.getpid(), ctypes.byref(session))
            or session.value != int(os.environ["AQP_ISOLATED_WINDOWS_SESSION"])):
        raise RuntimeError("isolated native process identity mismatch")
    return os.getpid()


def inject_gap_after_completed_read(namespace, context, expected_rid):
    """仅隔离测试：原读已结束后，在旧帧锁内制造异线程回报缺口。"""
    if namespace.get("_READ_ROTATION") is not None:
        raise RuntimeError("isolated gap injection started after rotation")
    evidence = namespace["_read_rotation_evidence"]()
    if evidence is None:
        return None
    if evidence.get("request_id") != expected_rid or evidence.get("method") not in ("get_ticks", "get_full_tick"):
        raise RuntimeError("isolated old read evidence mismatch")
    read = namespace["_RUNTIMES"]["read"]["_SERVER"]
    execution = namespace["_RUNTIMES"]["execution"]["_SERVER"]
    mailbox = read.mailbox
    if not mailbox.region.acquire(0):
        return None
    try:
        checked = read.read_rotation_evidence(execution.bridge.mailbox.session_id,
            namespace["_BOUND_NATIVE_CREATION_FILETIME"], mailbox=mailbox, mailbox_locked=True)
        if checked != evidence:
            raise RuntimeError("isolated old read changed before gap")
        header = mailbox._header()
        transport = namespace["_RUNTIMES"]["read"]["_load_embedded"]("aqp_readonly_transport")
        before = mailbox.region.read(0, transport.RESPONSE_OFFSET + header[8])
        worker = threading.Thread(target=namespace["deal_callback"], args=(context, {}))
        worker.start()
        worker.join(1)
        if worker.is_alive():
            raise RuntimeError("isolated gap callback did not finish")
        # 在原始 callback 线程上将单调 off-thread 标记转成明确 lost_sequence。
        namespace["aqp_shm_rpc_adjust"](context)
        bridge = execution.bridge
        if (not bridge._off_thread_event_loss.is_set() or type(bridge._lost_sequence) is not int
                or bridge._lost_sequence <= 0 or not bridge._async_write_block_reason
                or namespace["_read_rotation_evidence"]() is not None
                or namespace.get("_READ_ROTATION") is not None or mailbox._header() != header
                or mailbox.region.read(0, transport.RESPONSE_OFFSET + header[8]) != before):
            raise RuntimeError("isolated gap or old frame proof unavailable")
        return bridge._lost_sequence
    finally:
        mailbox.region.release()


def main():
    run_id, root, source_path, native_source = verify_environment()
    # pythonw 无交互终端；不把测试期协议、密钥或原始帧写日志。
    sys.stdout, sys.stderr = io.StringIO(), io.StringIO()
    context, namespace = SyntheticContext("8882152350", os.environ.get("AQP_ISOLATED_SCENARIO", "")), {}
    original_import = builtins.__import__

    def restricted_import(name, *args, **kwargs):
        if name.split(".")[0] in ("xtquant", "redis", "zmq"):
            raise ImportError("external native dependency forbidden in isolated test")
        return original_import(name, *args, **kwargs)

    builtins.__import__ = restricted_import
    manager = None
    started = False
    error_type = ""
    try:
        with native_source.open("rb") as stream:
            code = compile(stream.read(), str(native_source), "exec")
        exec(code, namespace)
        namespace["_verify_identity"] = _isolated_identity  # 仅本测试：独立 pythonw 而非 QMT 可执行文件。
        namespace.update(get_trade_detail_data=_query, passorder=context.forbidden_write, cancel=context.forbidden_write)
        namespace["init"](context)
        started = True
        manager_script, manager_python = os.environ["AQP_ISOLATED_MANAGER_SCRIPT"], os.environ["AQP_ISOLATED_QMT310_PYTHON"]
        manager = subprocess.Popen([manager_python, "-B", manager_script], close_fds=True,
                                   creationflags=0x00000200 | 0x00000008, env=dict(os.environ))
        namespace["_SUPERVISOR_PROCESS"] = manager
        namespace["_SUPERVISOR_IDENTITY"] = namespace["_supervisor_identity"](manager)
        if namespace["_SUPERVISOR_IDENTITY"] is None:
            raise RuntimeError("isolated supervisor identity unavailable")
        marker = root / "capability-verified.marker"
        gap_marker = root / "inject-gap.marker"
        injected_gap, released, gap_sequence, capability_injections = False, False, 0, 0
        budget = 270 if os.environ.get("AQP_ISOLATED_SCENARIO") == "two_generations" else 150
        deadline = time.monotonic() + budget
        while manager.poll() is None and time.monotonic() < deadline:
            capability = namespace.get("_ROTATION_CAPABILITY")
            next_marker = (marker if capability_injections == 0 else
                           root / "capability-two-verified.marker" if capability_injections == 1
                           and os.environ.get("AQP_ISOLATED_SCENARIO") == "two_generations" else None)
            if next_marker is not None and next_marker.is_file() and next_marker.read_text(encoding="ascii").strip() == run_id:
                if not isinstance(capability, dict) or capability.get("status") != "READY":
                    raise RuntimeError("isolated capability marker lacks native proof")
                try:
                    proof = namespace["_native_capability_request"](capability["nonce"])
                except namespace["_NativeCapabilityBusy"]:
                    proof = None  # 仅当前只读复核锁忙；不再次发送挑战或控制帧。
                if proof is not None:
                    if proof != capability["request"]:
                        raise RuntimeError("isolated native capability identity changed")
                    # 明确的测试驱动注入：仅演练独立 OS/SHM/Redis，不改变生成源生产默认硬门。
                    bridge = namespace["_RUNTIMES"]["execution"]["_SERVER"].bridge
                    bridge.runtime_metadata["read_rotation_authorization_ready"] = True
                    injected_marker = root / ("capability-injected.marker" if capability_injections == 0
                                              else "capability-two-injected.marker")
                    with injected_marker.open("x", encoding="ascii") as stream:
                        stream.write(run_id)
                    released = True
                    capability_injections += 1
            if not injected_gap and gap_marker.is_file() and gap_marker.read_text(encoding="ascii").strip() == run_id:
                gap_sequence = inject_gap_after_completed_read(namespace, context, os.environ["AQP_ISOLATED_OLD_READ_RID"])
                if gap_sequence is not None:
                    injected_gap = True
                    with (root / "gap-injected.marker").open("x", encoding="ascii") as stream:
                        stream.write(run_id)
            namespace["aqp_shm_readonly_adjust"](context)
            namespace["aqp_shm_rpc_adjust"](context)
            time.sleep(0.005)
        if manager.poll() is None:
            raise RuntimeError("isolated supervisor timeout")
        if manager.returncode != 0 or context.financial_calls:
            raise RuntimeError("isolated supervisor or financial guard failed")
    except BaseException as exc:
        error_type = type(exc).__name__
    finally:
        server = namespace.get("_RUNTIMES", {}).get("execution", {}).get("_SERVER")
        bridge = None if server is None else server.bridge
        if manager is not None and manager.poll() is None:
            # 只终止本次亲启的 manager 及其测试后代，避免留下代理或网关子进程。
            subprocess.run(["taskkill", "/PID", str(manager.pid), "/T", "/F"],
                           stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=5)
            manager.wait(timeout=5)
        if started:
            try:
                namespace["stop"](context)
            except BaseException:
                error_type = error_type or "NativeCleanupUnknown"
        builtins.__import__ = original_import
        result = dict(status="PASS" if not error_type else "UNKNOWN", error_type=error_type,
                      financial_calls=context.financial_calls, synthetic_quote_calls=context.quote_calls,
                      supervisor_returncode=None if manager is None else manager.returncode,
                      read_rotation_ready_test_injected=bool(locals().get("released", False)),
                      capability_injection_count=int(locals().get("capability_injections", 0)),
                      gap_injected=bool(locals().get("injected_gap", False)),
                      native_lost_sequence=None if bridge is None else bridge._lost_sequence,
                      native_writes_blocked=None if bridge is None else bool(bridge._async_write_block_reason), run_id=run_id)
        with (root / "native-result.json").open("w", encoding="utf-8") as stream:
            json.dump(result, stream, sort_keys=True)
    return 0 if not error_type else 1


if __name__ == "__main__":
    sys.exit(main())
