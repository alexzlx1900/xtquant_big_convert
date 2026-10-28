# coding: utf-8
"""仅供隔离 Windows 进程联测的只读代理；不得连接现有 Redis 或 QMT。"""

import json
import math
import os
from pathlib import Path
import re
import sys
import tempfile
import time
import traceback
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "src"))

from bigqmt_signal_trader.redis_shm_proxy import (
    InheritedSupervisorWriteGate, PURE_READ_METHODS, RedisShmReadOnlyProxy,
    SupervisorWriteGateError, _windows_native_process_identity,
)
from bigqmt_signal_trader.transports.readonly_shm import (
    REQUEST_CAPACITY, REQUEST_OFFSET, ShmProtocolError, ShmReadOnlyClient, decode_payload,
)
from bigqmt_signal_trader.transports.redis_transport import RedisTransport
from tools.run_external_redis_shm_proxy import (
    InheritedRotationChannel, ReadonlyRotationDriver, create_redis_client, windows_session_id,
)


ACCOUNT = "8882152350"
TARGET = {"gateway_id": "qmt-vm-001", "profile_id": "live-stock"}
SAFE_SHM_FAILURES = {
    "native read rotation authorization is not verified": "ROTATION_CAPABILITY_UNVERIFIED",
    "old read completion evidence is incomplete or mismatched": "ROTATION_OLD_EVIDENCE_MISMATCH",
    "old read completion sequence is not confirmed as pending": "ROTATION_OLD_SEQUENCE_MISMATCH",
    "rotation transition changed before registration": "ROTATION_REGISTER_RECORD_CHANGED",
    "supervisor rotation registration response is unconfirmed": "ROTATION_REGISTER_UNCONFIRMED",
    "rotation control did not acknowledge pending authorization": "ROTATION_CONTROL_UNCONFIRMED",
    "rotation authorization outcome is unconfirmed": "ROTATION_CONTROL_UNKNOWN",
    "rotation authorization status identity is invalid": "ROTATION_STATUS_MISMATCH",
    "candidate readonly source identity changed": "ROTATION_CANDIDATE_IDENTITY_CHANGED",
    "candidate probe frame is unavailable or changed": "ROTATION_CANDIDATE_FRAME_CHANGED",
    "execution mailbox has an unconfirmed request": "ROTATION_EXECUTION_UNCONFIRMED",
}


class IsolatedReadonlyProxy(RedisShmReadOnlyProxy):
    """测试入口在正常校验前显式拒绝所有非只读方法。"""

    def _validate_request(self, request):
        if request.get("method") not in PURE_READ_METHODS:
            raise ShmProtocolError("isolated harness allows readonly methods only")
        return super()._validate_request(request)

    def process_request(self, request):
        """只为隔离 ping 留存一条受限观测，不记录参数、凭据或原始响应。"""
        rid = request.get("request_id") if isinstance(request, dict) else None
        if (not isinstance(request, dict) or request.get("method") != "ping"
                or not isinstance(rid, str) or re.fullmatch(r"[0-9a-f]{32}", rid) is None):
            return super().process_request(request)
        deadline = request.get("read_expires_at_unix")
        observation = dict(event="ISOLATED_PING_OBSERVATION", request_id=rid, proxy_received_at_unix=time.time(),
            read_expires_at_unix=deadline if type(deadline) in (int, float) and math.isfinite(deadline) else None)
        try:
            response = super().process_request(request)
        except Exception as exc:
            observation.update(outcome="EXCEPTION", error_type=type(exc).__name__)
            emit(observation)
            raise
        if isinstance(response, dict):
            for field, allowed in (("status", {"UNKNOWN", "REJECTED", "OK"}),
                    ("server_error", {"SHM_NOT_SENT", "SHM_RESULT_UNCONFIRMED", "SHM_IDENTITY_OR_PROTOCOL_ERROR"}),
                    ("publication_state", {"NOT_SENT", "UNKNOWN"}), ("publication_scope", {"SHM"})):
                value = response.get(field)
                if isinstance(value, str) and value in allowed:
                    observation[field] = value
            for field in ("ok", "automatic_retry", "previous_unconfirmed"):
                if type(response.get(field)) is bool:
                    observation[field] = response[field]
            if type(response.get("sequence")) is int and response["sequence"] > 0:
                observation["sequence"] = response["sequence"]
        emit(observation)
        return response


def _read_source(path):
    if path.stat().st_size > 65536:
        raise ValueError("isolated source identity is too large")
    with path.open(encoding="utf-8-sig") as stream:
        value = json.load(stream)
    if not isinstance(value, dict):
        raise ValueError("isolated source identity is invalid")
    return value


def isolated_config(environ):
    """在任何网络或 SHM 打开前验证测试专用根、名称、账户和端口。"""
    run_id = environ.get("AQP_ISOLATED_RUN_ID", "")
    if re.fullmatch(r"[0-9a-f]{32}", run_id) is None:
        raise ValueError("a random isolated run identity is required")
    root = Path(environ.get("AQP_ISOLATED_ROOT", ""))
    source_path = Path(environ.get("AQP_ISOLATED_SOURCE_PATH", ""))
    if (not root.is_absolute() or not source_path.is_absolute() or not root.is_dir()
            or root.resolve().parent != Path(tempfile.gettempdir()).resolve()
            or root.name != "aqp-rotation-" + run_id or source_path.resolve() != root.resolve() / "source.json"):
        raise ValueError("source must use the new isolated temporary root")
    read_base = r"Local\AQP_SHM_READONLY_ISO_" + run_id
    execution_base = r"Local\AQP_SHM_TRADE_ISO_" + run_id
    if (environ.get("AQP_ISOLATED_READ_BASE") != read_base
            or environ.get("AQP_ISOLATED_EXECUTION_BASE") != execution_base):
        raise ValueError("isolated SHM names must bind the run identity")
    source = _read_source(source_path)
    if (source.get("account_id") != ACCOUNT or source.get("environment") != "LIVE"
            or source.get("runtime_policy") != "PERSISTENT_LIVE_READ_WRITE"
            or not re.fullmatch(re.escape(read_base) + r"_[0-9a-f]{16}", str(source.get("read_name", "")))
            or not re.fullmatch(re.escape(execution_base) + r"_[0-9a-f]{16}", str(source.get("execution_name", "")))
            or any(re.fullmatch(r"[0-9a-f]{32}", str(source.get(key, ""))) is None
                   for key in ("read_session_id", "execution_session_id"))
            or source["read_session_id"] == source["execution_session_id"]
            or type(source.get("native_pid")) is not int or source["native_pid"] <= 0
            or type(source.get("native_creation_filetime")) is not int or source["native_creation_filetime"] <= 0
            or type(source.get("windows_session")) is not int or source["windows_session"] < 0):
        raise ValueError("isolated source binding is invalid")
    try:
        port = int(environ["AQP_ISOLATED_REDIS_PORT"])
        database = int(environ["AQP_ISOLATED_REDIS_DB"])
        session = int(environ["AQP_ISOLATED_WINDOWS_SESSION"])
    except (KeyError, ValueError):
        raise ValueError("isolated Redis and Windows session are required") from None
    password = environ.get("AQP_ISOLATED_REDIS_PASSWORD")
    old_rid = environ.get("AQP_ISOLATED_OLD_READ_RID", "")
    native_exe = environ.get("AQP_ISOLATED_NATIVE_EXE", "")
    scenario = environ.get("AQP_ISOLATED_SCENARIO", "success")
    second_rid = environ.get("AQP_ISOLATED_SECOND_READ_RID", "")
    if (not 1024 <= port <= 65535 or port == 6379 or not 1 <= database <= 15 or session != source["windows_session"]
            or not isinstance(password, str) or len(password) < 16
            or not old_rid or len(old_rid) > 128 or any(ord(c) < 33 or ord(c) > 126 for c in old_rid)
            or not os.path.isabs(native_exe) or Path(native_exe).name.lower() not in ("python.exe", "pythonw.exe")
            or scenario not in ("success", "gap", "two_generations")
            or scenario == "two_generations" and (re.fullmatch(r"[0-9a-f]{32}", old_rid) is None
                or re.fullmatch(r"[0-9a-f]{32}", second_rid) is None or old_rid == second_rid)):
        raise ValueError("isolated Redis, request or process identity is invalid")
    return dict(run_id=run_id, root=root, source=source, native_exe=os.path.normcase(os.path.realpath(native_exe)),
                old_rid=old_rid, second_rid=second_rid, scenario=scenario,
                redis=dict(host="127.0.0.1", port=port, db=database, password=password))


def verify_isolated_redis(client, run_id):
    key = "aqp:isolated:%s:owner" % run_id
    value = client.get(key)
    if isinstance(value, bytes):
        value = value.decode("ascii", errors="strict")
    if value != run_id or client.ttl(key) != -1:
        raise RuntimeError("isolated Redis ownership marker is not confirmed")


def old_frame_completed(proxy, expected_rid):
    fault = proxy._route_faults["readonly"]
    if fault is None:
        return False
    if (fault.get("state") != "RESULT_UNKNOWN" or fault.get("trigger_request_id") != expected_rid
            or type(fault.get("sequence")) is not int):
        raise ShmProtocolError("isolated read fault does not match the original request")
    mailbox = proxy._clients["readonly"].mailbox
    if not mailbox.region.acquire(0):
        return False
    try:
        header = mailbox._header()
        if header[4] != header[5] or header[4] != fault["sequence"] or header[6] >= header[4]:
            return False
        if not 0 < header[7] <= REQUEST_CAPACITY:
            raise ShmProtocolError("old isolated request frame length is invalid")
        request = decode_payload(mailbox.region.read(REQUEST_OFFSET, header[7]))
        if (not isinstance(request, dict) or request.get("request_id") != expected_rid
                or request.get("sequence") != fault["sequence"]):
            raise ShmProtocolError("old isolated request frame changed")
        return time.time() >= request.get("read_expires_at_unix", float("inf"))
    finally:
        mailbox.region.release()


def emit(event):
    print(json.dumps(event, ensure_ascii=False, sort_keys=True), flush=True)


def wait_marker(config, name, deadline):
    path = config["root"] / name
    while time.monotonic() < deadline:
        if path.is_file():
            if path.read_text(encoding="ascii").strip() != config["run_id"]:
                raise ShmProtocolError("isolated synchronization marker does not match this run")
            return
        time.sleep(0.05)
    raise ShmProtocolError("isolated synchronization marker timed out")


def write_marker(config, name):
    with (config["root"] / name).open("x", encoding="ascii") as stream:
        stream.write(config["run_id"])


def gateway_read_marker_matches(proxy, config, name):
    path = config["root"] / name
    if not path.is_file():
        return False
    if path.stat().st_size > 4096:
        raise ShmProtocolError("isolated Gateway read marker is invalid")
    with path.open(encoding="utf-8") as stream:
        value = json.load(stream)
    expected = dict(run_id=config["run_id"], transition_id=proxy._accepted_rotation_record["transition_id"],
                    read_session_id=proxy._sessions["readonly"])
    if value != expected:
        raise ShmProtocolError("isolated Gateway read marker identity changed")
    return True


def observe_execution_cursor(proxy, config, kind, deadline):
    """保留一次真实 poll 原帧，直至 manager 独立捕获。"""
    request = dict(schema_version=1, request_id=uuid.uuid4().hex, account_id=ACCOUNT, environment="LIVE",
                   method="poll_execution_events", params={"after_sequence": 0},
                   expected_source_binding=proxy.identity_snapshot()["source_binding"])
    response = proxy.process_request(request)
    data = response.get("data") if isinstance(response, dict) else None
    if (response.get("ok") is not True or not isinstance(data, dict)
            or proxy._execution_event_state(data, request) != "HEALTHY"
            or data.get("latest_sequence") != 0 or data.get("lost_sequence") != 0 or data.get("events") != []):
        raise ShmProtocolError("isolated execution cursor is unconfirmed")
    write_marker(config, "execution-cursor-" + kind + ".marker")
    wait_marker(config, "execution-cursor-" + kind + "-captured.marker", deadline)


def observe_callback_gap(proxy, config, deadline):
    """最终保留一次实际 poll 回报帧；信号只控制测试时序。"""
    proxy.timeout_seconds = 2.0
    observation = proxy._execution_control_ping()["data"]
    if (observation.get("writes_blocked") is not True
            or observation.get("write_block_reason") != "EXECUTION_EVENT_LOSS; outcome=UNKNOWN"
            or observation.get("read_rotation_evidence") is not None
            or observation.get("native_creation_filetime") != config["source"]["native_creation_filetime"]):
        raise ShmProtocolError("isolated native callback gap is not confirmed")
    if proxy.transport.redis.hlen(proxy._read_rotation_key) or proxy._rotation_control_attempted:
        raise ShmProtocolError("isolated gap unexpectedly contains an earlier rotation")
    try:
        proxy.prepare_readonly_rotation()
    except ShmProtocolError:
        pass
    else:
        raise ShmProtocolError("isolated callback gap unexpectedly allowed rotation")
    if proxy.transport.redis.hlen(proxy._read_rotation_key) or proxy._rotation_control_attempted:
        raise ShmProtocolError("isolated gap crossed a rotation control boundary")
    request = dict(schema_version=1, request_id=uuid.uuid4().hex, account_id=ACCOUNT, environment="LIVE",
                   method="poll_execution_events", params={"after_sequence": 0})
    response = proxy.process_request(request)
    data = response.get("data") if isinstance(response, dict) else None
    if (response.get("ok") is not True or not isinstance(data, dict) or data.get("gap") is not True
            or type(data.get("lost_sequence")) is not int or data["lost_sequence"] <= 0
            or data.get("automatic_retry") is not False or "EXECUTION_EVENT_GAP" not in proxy._write_block_reasons
            or not any(isinstance(item, dict) and isinstance(item.get("event"), dict)
                       and item["event"].get("event_type") == "data_loss"
                       and item["event"].get("data_loss") is True for item in data.get("events", []))
            or config["old_rid"] not in proxy._unconfirmed_read_ids["readonly"]):
        raise ShmProtocolError("isolated callback gap poll is unconfirmed")
    with (config["root"] / "gap-observed.marker").open("x", encoding="ascii") as stream:
        stream.write(config["run_id"])
    # 不再调用任何 execution SHM；等待 manager 用该实际原帧做独立只读核验。
    while time.monotonic() < deadline:
        for name in ("gap-verified.marker", "stop.marker"):
            path = config["root"] / name
            if path.is_file() and path.read_text(encoding="ascii").strip() == config["run_id"]:
                return {"status": "GAP_VERIFIED" if name == "gap-verified.marker" else "STOPPED",
                        "read_rotation_accepted": False}
        time.sleep(0.05)
    return {"status": "GAP_VERIFICATION_TIMEOUT", "read_rotation_accepted": False}


def run_two_generations_with_driver(proxy, config, deadline):
    """测试只同步证据采集；换代阶段由真实入口的调度器自行推进。"""
    driver = ReadonlyRotationDriver(proxy)
    generation, current_rid, initial_cursor_done = 1, config["old_rid"], False
    while time.monotonic() < deadline and not proxy.stopped:
        stop = config["root"] / "stop.marker"
        if stop.is_file() and stop.read_text(encoding="ascii").strip() == config["run_id"]:
            return {"status": "STOPPED", "read_rotation_accepted": False}
        proxy.poll_once()
        if not initial_cursor_done:
            if (config["root"] / "gateway-initial-pings-complete.marker").is_file():
                wait_marker(config, "gateway-initial-pings-complete.marker", deadline)
                observe_execution_cursor(proxy, config, "initial", deadline)
                initial_cursor_done = True
            time.sleep(0.01)
            continue
        if driver.stage == "IDLE":
            if not old_frame_completed(proxy, current_rid):
                time.sleep(0.01)
                continue
            unknown = config["root"] / ("gateway-read-unknown.marker" if generation == 1
                                        else "gateway-read-unknown-two.marker")
            if not unknown.is_file():
                time.sleep(0.01)
                continue
            with unknown.open(encoding="utf-8") as stream:
                observed = json.load(stream)
            if observed != {"run_id": config["run_id"], "request_id": current_rid}:
                raise ShmProtocolError("isolated original read marker identity changed")
            name = "one" if generation == 1 else "two"
            ready = config["root"] / ("old-frame-" + name + "-ready.marker")
            if not ready.exists():
                write_marker(config, "old-frame-" + name + "-ready.marker")
            wait_marker(config, "old-frame-" + name + "-captured.marker", deadline)
            proxy.timeout_seconds = 2.0
        if driver.stage == "GATEWAY_VERIFIED":
            name = "first" if generation == 1 else "second"
            if not gateway_read_marker_matches(proxy, config, "gateway-" + name + "-read.marker"):
                time.sleep(0.01)
                continue
        prior = driver.stage
        driver.step()
        if driver.blocked:
            emit(dict(event="ISOLATED_DRIVER_BLOCKED", stage=driver.stage,
                      error_type=driver.error_type, automatic_retry=False))
            raise ShmProtocolError("isolated automatic rotation driver blocked")
        if driver.stage != prior:
            emit(dict(event="ISOLATED_DRIVER_STAGE", generation=generation,
                      stage=driver.stage, automatic_retry=False))
        if prior == "GATEWAY_VERIFIED" and driver.stage == "IDLE":
            if generation == 1:
                wait_marker(config, "generation-one-ready.marker", deadline)
                generation, current_rid = 2, config["second_rid"]
                proxy.timeout_seconds = 0.2
            else:
                wait_marker(config, "gateway-final-next-ready.marker", deadline)
                observe_execution_cursor(proxy, config, "final", deadline)
                wait_marker(config, "manager-complete.marker", deadline)
                return {"status": "TWO_GENERATIONS_CLOSED", "read_rotation_accepted": True,
                        "automatic_driver": True, "automatic_retry": False}
        time.sleep(0.01)
    return {"status": "TIMEOUT_OR_STOPPED", "read_rotation_accepted": False,
            "automatic_driver": True, "automatic_retry": False}


def run_isolated(environ):
    if os.name != "nt":
        raise RuntimeError("the isolated process harness requires Windows")
    config = isolated_config(environ)
    source = config["source"]
    if (windows_session_id(os.getpid()) != source["windows_session"]
            or windows_session_id(source["native_pid"]) != source["windows_session"]):
        raise RuntimeError("isolated processes do not share the Windows session")
    actual = _windows_native_process_identity(source["native_pid"])
    if (actual["native_creation_filetime"] != source["native_creation_filetime"]
            or os.path.normcase(os.path.realpath(actual["executable"])) != config["native_exe"]):
        raise RuntimeError("isolated native process identity differs")
    redis_client = proxy = gate = channel = None
    clients = []
    phase = "REDIS"
    try:
        redis_client = create_redis_client(config["redis"])
        verify_isolated_redis(redis_client, config["run_id"])
        phase = "INHERITED_GATE"
        gate = InheritedSupervisorWriteGate.from_environment()
        phase = "PRIVATE_CHANNEL"
        channel = InheritedRotationChannel.from_environment()
        phase = "SHM"
        clients = [ShmReadOnlyClient(source["read_name"], ACCOUNT),
                   ShmReadOnlyClient(source["execution_name"], ACCOUNT)]
        proxy = IsolatedReadonlyProxy(
            RedisTransport(redis_client, account_id=ACCOUNT), clients[0], clients[1], account_id=ACCOUNT,
            environment="LIVE", runtime_policy="PERSISTENT_LIVE_READ_WRITE", allow_order_methods=True,
            live_write_authorized=False, start_write_blocked=True, receive_policy="QUEUE_ONLY",
            execution_cursor_guard_target=TARGET, supervisor_write_gate=gate, supervisor_rotation_channel=channel,
            expected_readonly_session_id=source["read_session_id"], expected_execution_session_id=source["execution_session_id"],
            expected_native_pid=source["native_pid"], expected_windows_session_id=source["windows_session"],
            timeout_seconds=0.2, max_requests=0)
        proxy._block_writes("ISOLATED_TEST_FINANCIAL_METHODS_DISABLED")
        phase = "ATTACH"
        proxy.attach_native_supervisor()
        phase = "IDENTITY"
        proxy.verify_identity()
        if proxy._verified_process_identity != actual or proxy.identity_snapshot()["write_ready"] is not False:
            raise RuntimeError("isolated source or write isolation is unconfirmed")
        emit(dict(proxy.identity_snapshot(), status="READY", automatic_retry=False))
        phase = "CAPABILITY"
        until = time.monotonic() + 180
        if config["scenario"] == "two_generations":
            phase = "AUTOMATIC_DRIVER"
            return run_two_generations_with_driver(proxy, config, until)
        capability = False
        attempted_prepare = False
        completed_at = None
        accepted = False
        current_rid = config["old_rid"]
        while time.monotonic() < until and not proxy.stopped:
            stop = config["root"] / "stop.marker"
            if stop.is_file() and stop.read_text(encoding="ascii").strip() == config["run_id"]:
                return {"status": "STOPPED", "read_rotation_accepted": accepted}
            if not capability:
                proof = proxy.attest_read_rotation_capability()
                if proof is not None:
                    capability = True
                    emit({"event": "ISOLATED_CAPABILITY_VERIFIED", **proof})
                else:
                    time.sleep(0.1)
            else:
                phase = "RECEIVE"
                proxy.poll_once()
                if not attempted_prepare and old_frame_completed(proxy, current_rid):
                    # 给旧回调返回留一个观察周期，再执行需要独立正证据的 PREPARE。
                    if completed_at is None:
                        completed_at = time.monotonic()
                    elif time.monotonic() - completed_at >= 0.1:
                        if config["scenario"] == "gap":
                            injected = config["root"] / "gap-injected.marker"
                            if not injected.is_file() or injected.read_text(encoding="ascii").strip() != config["run_id"]:
                                time.sleep(0.01)
                                continue
                            phase = "GAP_EVIDENCE"
                            return observe_callback_gap(proxy, config, until)
                        attempted_prepare = True
                        proxy.timeout_seconds = 2.0
                        phase = "PREPARE"
                        emit({"event": "ISOLATED_PREPARE_RESULT", **proxy.prepare_readonly_rotation()})
                if proxy._read_rotation_stage == "COMMITTED":
                    phase = "ACTIVATE"
                    result = proxy.activate_readonly_rotation()
                    if result is not None:
                        emit({"event": "ISOLATED_ACTIVATE_RESULT", **result})
                if proxy._read_rotation_stage == "ACTIVATED":
                    phase = "GATEWAY_ACCEPTANCE"
                    result = proxy.accept_gateway_rotation()
                    if result is not None:
                        emit({"event": "ISOLATED_GATEWAY_RESULT", **result})
                        accepted = result["status"] == "READ_ROTATION_ACCEPTED"
                        if not accepted:
                            return {"status": "UNKNOWN", "read_rotation_accepted": False}
                time.sleep(0.01)
        return {"status": "TIMEOUT_OR_STOPPED", "read_rotation_accepted": accepted}
    except Exception as exc:
        # 不输出原异常：私有管道和 Redis 凭据不应进入日志。
        failure = {"event": "ISOLATED_PROXY_FAILED", "phase": phase, "error_type": type(exc).__name__}
        if isinstance(exc, SupervisorWriteGateError) and exc.reason in {
                "SUPERVISOR_GATE_UNAVAILABLE", "SUPERVISOR_HANDLE_TYPE_OR_ACCESS_INVALID",
                "SUPERVISOR_HANDLE_INSPECTION_FAILED", "SUPERVISOR_PARENT_UNVERIFIED", "SUPERVISOR_EPOCH_UNAVAILABLE",
                "SUPERVISOR_PARENT_UNAVAILABLE", "SUPERVISOR_WRITE_BLOCKED"}:
            failure["reason"] = exc.reason
        if isinstance(exc, ShmProtocolError):
            if str(exc) in SAFE_SHM_FAILURES:
                failure["error_code"] = SAFE_SHM_FAILURES[str(exc)]
            for frame in reversed(traceback.extract_tb(exc.__traceback__)):
                name = Path(frame.filename).name
                if name in ("redis_shm_proxy.py", "readonly_shm.py") and re.fullmatch(r"[A-Za-z0-9_]{1,80}", frame.name):
                    failure["error_site"] = "%s:%s:%d" % (name, frame.name, frame.lineno)
                    break
        emit(failure)
        raise
    finally:
        if proxy is not None:
            proxy.close()
        else:
            for resource in clients + [channel, gate]:
                if resource is not None:
                    resource.close()
        if redis_client is not None:
            redis_client.connection_pool.disconnect()


if __name__ == "__main__":
    try:
        emit(run_isolated(os.environ))
    except Exception:
        sys.exit(1)
