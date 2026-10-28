"""独立 Windows 进程、随机测试 SHM 与专用 Redis 的只读换代驱动。"""

import ctypes
import importlib.util
import json
import ntpath
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import time
import uuid

import redis
from redis.backoff import NoBackoff
from redis.retry import Retry


def emit(step, status, run_id=None, error_type=None):
    value = dict(step=step, status=status)
    if run_id is not None:
        value["run_id"] = run_id
    if error_type is not None:
        value["error_type"] = error_type
    print(json.dumps(value, sort_keys=True), flush=True)


def under(path, root):
    try:
        if not ntpath.isabs(str(path)) or not ntpath.isabs(str(root)):
            return False
        # 在实际 Windows 上解析 junction/symlink，避免指定候选目录却加载别处源码。
        resolved_path, resolved_root = str(Path(path).resolve(strict=True)), str(Path(root).resolve(strict=True))
        return ntpath.normcase(ntpath.commonpath((resolved_path, resolved_root))) == ntpath.normcase(resolved_root)
    except (OSError, RuntimeError, ValueError):
        return False


def allocate_database(port, password, run_id):
    script = "if redis.call('DBSIZE')~=0 then return 0 end " \
             "return redis.call('SET',KEYS[1],ARGV[1],'NX') and 1 or 0"
    for db in range(1, 16):
        client = redis.Redis(host="127.0.0.1", port=port, db=db, password=password,
                             socket_connect_timeout=3, socket_timeout=3, retry=Retry(NoBackoff(), 0))
        try:
            key = "aqp:isolated:" + run_id + ":owner"
            if client.eval(script, 1, key, run_id) == 1:
                if client.get(key) != run_id.encode("ascii"):
                    raise RuntimeError("isolated Redis owner readback failed")
                return db
        finally:
            client.close()
    raise RuntimeError("isolated Redis databases unavailable")


def main():
    run_id = uuid.uuid4().hex
    phase = "INITIALIZE"
    native = None
    try:
        env = dict(os.environ)
        converter = env["AQP_ISOLATED_CONVERTER_ROOT"]
        aqp = env["AQP_ISOLATED_AQP_ROOT"]
        paths = ((__file__, converter), (str(Path(converter) / "tools/build_dual_shm_single_file.py"), converter),
                 (env["AQP_ISOLATED_NATIVE_DRIVER"], converter), (env["AQP_ISOLATED_PROXY_SCRIPT"], converter),
                 (env["AQP_ISOLATED_MANAGER_SCRIPT"], aqp), (env["AQP_ISOLATED_GATEWAY_SCRIPT"], aqp))
        port = int(env["AQP_ISOLATED_REDIS_PORT"])
        scenario = env.get("AQP_ISOLATED_SCENARIO")
        if (os.name != "nt" or sys.version_info[:2] != (3, 10) or not 1 <= port <= 65535 or port == 6379
                or re.fullmatch(r"[0-9a-f]{64}", env["AQP_ISOLATED_REDIS_CONTAINER_ID"]) is None
                or scenario not in ("success", "gap", "two_generations")
                or not env.get("AQP_ISOLATED_REDIS_PASSWORD") or not all(under(path, root) for path, root in paths)
                or not all(Path(path).is_file() for path, _ in paths)):
            raise RuntimeError("isolated candidate or Redis contract unavailable")
        phase = "REDIS_OWNER"
        db = allocate_database(port, env["AQP_ISOLATED_REDIS_PASSWORD"], run_id)
        emit(phase, "PASS", run_id)
        root = Path(tempfile.gettempdir()) / ("aqp-rotation-" + run_id)
        root.mkdir()  # 唯一专属目录；不覆盖或清理现有用户文件。
        source = root / "source.json"
        generated = root / "native_source.py"
        session = ctypes.c_uint32()
        if not ctypes.windll.kernel32.ProcessIdToSessionId(os.getpid(), ctypes.byref(session)):
            raise RuntimeError("isolated Windows session unavailable")
        read_base = "Local\\AQP_SHM_READONLY_ISO_" + run_id
        execution_base = "Local\\AQP_SHM_TRADE_ISO_" + run_id
        builder_path = Path(converter) / "tools/build_dual_shm_single_file.py"
        spec = importlib.util.spec_from_file_location("isolated_dual_builder", str(builder_path))
        builder = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(builder)
        generated.write_text(builder.build_source(native_pid=0, windows_session=session.value,
            hard_deadline_unix=None, read_name=read_base, execution_name=execution_base, symbols=["000001.SZ"],
            account_id="8882152350", environment="LIVE", max_seconds=0, max_requests=0,
            persistent_live_read_write=True, native_executable=r"C:\isolated\XtItClient.exe",
            source_identity_path=str(source), read_fault_isolation=True, execution_event_guard=True), encoding="ascii")
        first_rid, second_rid = uuid.uuid4().hex, uuid.uuid4().hex
        if first_rid == second_rid:
            raise RuntimeError("isolated request identities collided")
        env.update(AQP_ISOLATED_RUN_ID=run_id, AQP_ISOLATED_ROOT=str(root), AQP_ISOLATED_SOURCE_PATH=str(source),
                   AQP_ISOLATED_NATIVE_SOURCE=str(generated), AQP_ISOLATED_REDIS_DB=str(db),
                   AQP_ISOLATED_WINDOWS_SESSION=str(session.value), AQP_ISOLATED_READ_BASE=read_base,
                   AQP_ISOLATED_EXECUTION_BASE=execution_base, AQP_ISOLATED_OLD_READ_RID=first_rid,
                   AQP_ISOLATED_SECOND_READ_RID=second_rid,
                   AQP_ISOLATED_GATEWAY_ROOT=str(Path(aqp) / "windows_gateway"))
        phase = "NATIVE_START"
        native = subprocess.Popen([env["AQP_ISOLATED_NATIVE_EXE"], "-I", "-S", "-B",
                                   env["AQP_ISOLATED_NATIVE_DRIVER"]], env=env, close_fds=True)
        emit(phase, "PASS", run_id)
        deadline = time.monotonic() + (270 if scenario == "two_generations" else 150)
        while native.poll() is None and time.monotonic() < deadline:
            time.sleep(0.1)
        if native.poll() is None:
            raise RuntimeError("isolated native test timeout")
        phase = "FINAL_READBACK"
        native_result = json.loads((root / "native-result.json").read_text(encoding="utf-8"))
        manager_result = json.loads((root / "manager-result.json").read_text(encoding="utf-8"))
        if (native.returncode != 0 or native_result.get("status") != "PASS"
                or native_result.get("run_id") != run_id or type(native_result.get("financial_calls")) is not int
                or native_result.get("financial_calls") != 0
                or native_result.get("read_rotation_ready_test_injected") is not True
                or type(native_result.get("synthetic_quote_calls")) is not int
                or manager_result.get("run_id") != run_id or manager_result.get("actual_qmt_connected") is not False
                or type(manager_result.get("financial_actions")) is not int or manager_result.get("financial_actions") != 0):
            raise RuntimeError("isolated native completion unconfirmed")
        if scenario == "success":
            if (native_result["synthetic_quote_calls"] != 2 or native_result.get("gap_injected") is not False
                    or native_result.get("native_lost_sequence") != 0
                    or native_result.get("capability_injection_count") != 1
                    or manager_result.get("status") != "READ_ROTATION_ACCEPTED"):
                raise RuntimeError("isolated read rotation acceptance unconfirmed")
        elif scenario == "gap":
            if (native_result["synthetic_quote_calls"] != 1 or native_result.get("gap_injected") is not True
                    or native_result.get("capability_injection_count") != 1
                    or type(native_result.get("native_lost_sequence")) is not int or native_result["native_lost_sequence"] <= 0
                    or native_result.get("native_writes_blocked") is not True
                    or manager_result.get("status") != "GAP_FAIL_CLOSED"
                    or any(manager_result.get(key) is not True for key in ("old_frame_unchanged", "execution_gap_verified",
                                                                           "prepare_never_authorized", "transition_claim_absent"))):
                raise RuntimeError("isolated gap rejection unconfirmed")
        elif scenario == "two_generations":
            if (native_result["synthetic_quote_calls"] != 4 or native_result.get("gap_injected") is not False
                    or native_result.get("native_lost_sequence") != 0 or native_result.get("capability_injection_count") != 2
                    or manager_result.get("status") != "TWO_ROTATIONS_CLOSED" or manager_result.get("generation_count") != 2
                    or any(manager_result.get(key) is not True for key in ("old_frames_unchanged", "execution_sid_unchanged",
                              "execution_cursor_unchanged", "serial_close_verified"))):
                raise RuntimeError("isolated serial rotation acceptance unconfirmed")
            gateway = json.loads((root / "gateway-complete.marker").read_text(encoding="utf-8"))
            transitions, old_rids, sessions = (gateway.get("transition_ids"), gateway.get("old_read_request_ids"),
                                               gateway.get("read_session_ids"))
            if (gateway.get("status") != "TWO_READ_ROTATIONS_COMPLETE" or gateway.get("run_id") != run_id
                    or old_rids != [first_rid, second_rid] or not isinstance(transitions, list) or len(transitions) != 2
                    or not isinstance(sessions, list) or len(sessions) != 3 or len(set(transitions)) != 2
                    or len(set(sessions)) != 3 or any(not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{32}", value) is None
                                                      for value in transitions + sessions)
                    or any(gateway.get(key) is not True for key in ("old_read_unknown_preserved", "execution_sid_unchanged",
                                                                     "execution_cursor_unchanged"))
                    or any(manager_result.get(key) != gateway.get(key) for key in
                           ("transition_ids", "old_read_request_ids", "read_session_ids"))
                    or not isinstance(manager_result.get("execution_session_id"), str)
                    or re.fullmatch(r"[0-9a-f]{32}", manager_result["execution_session_id"]) is None
                    or manager_result["execution_session_id"] in sessions
                    or any(type(gateway.get(key)) is not int or gateway[key] != 2 for key in
                           ("gateway_open_events_confirmed", "gateway_next_ready_events_confirmed", "synthetic_quote_reads"))
                    or type(gateway.get("financial_calls")) is not int or gateway["financial_calls"] != 0
                    or gateway.get("production_database") is not False):
                raise RuntimeError("isolated serial Gateway acceptance unconfirmed")
        else:
            raise RuntimeError("isolated scenario has no verified outcome contract")
        emit(phase, "PASS", run_id)
        return 0
    except BaseException as exc:
        emit(phase, "UNKNOWN", run_id, type(exc).__name__)
        return 1
    finally:
        if native is not None and native.poll() is None:
            # 只结束本轮亲启测试 native 及其测试后代。
            subprocess.run(["taskkill", "/PID", str(native.pid), "/T", "/F"],
                           stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=5)
            native.wait(timeout=5)


if __name__ == "__main__":
    sys.exit(main())
