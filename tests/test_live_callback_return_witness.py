# coding: utf-8
"""生成的 LIVE 模型与真实 Proxy 的 Python 回调返回见证；仅进程内替身。"""

import os
import struct
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

import test_dual_shm_single_file as DUAL
import test_live_redis_shm_proxy as LIVE
import bigqmt_signal_trader.redis_shm_proxy as proxy_module
from bigqmt_signal_trader.redis_shm_proxy import RedisShmReadOnlyProxy
from bigqmt_signal_trader.transports.readonly_shm import REQUEST_OFFSET, RESPONSE_OFFSET
from bigqmt_signal_trader.transports.readonly_shm import ShmBusyError
from bigqmt_signal_trader.transports.redis_transport import RedisTransport


CREATION = 134567890123456789
CONTRACT = "READ_PY_CALLBACK_RETURNED_V1"


@pytest.fixture
def model(monkeypatch):
    native = DUAL.Fixture(monkeypatch, **DUAL.live_settings(
        account_id=LIVE.ACCOUNT, read_fault_isolation=True, execution_event_guard=True))
    native.namespace["_native_creation_filetime"] = lambda: CREATION
    native.start()
    identity = dict(native_pid=os.getpid(), native_creation_filetime=CREATION,
                    windows_session_id=1, executable="fixture-native")
    monkeypatch.setattr(proxy_module, "_windows_native_process_identity", lambda pid: dict(identity))
    read, execution = native.clients["read"], native.clients["execution"]
    redis = LIVE.FIXTURE.FakeRedis()
    proxy = RedisShmReadOnlyProxy(
        RedisTransport(redis, account_id=LIVE.ACCOUNT), read, execution,
        account_id=LIVE.ACCOUNT, environment="LIVE", runtime_policy=LIVE.POLICY,
        allow_order_methods=True, start_write_blocked=True,
        expected_readonly_session_id=read.mailbox.session_id,
        expected_execution_session_id=execution.mailbox.session_id,
        expected_native_pid=os.getpid(), expected_windows_session_id=1,
        timeout_seconds=0.1, max_requests=0,
        execution_cursor_guard_target={"gateway_id": "qmt-vm-001", "profile_id": "live-stock"},
        supervisor_write_gate=LIVE.FIXTURE.FakeSupervisorWriteGate())
    proxy._supervisor_attach_verified = True  # 仅本地替身，不代表实机监督授权。

    def drive(function, *, drive_read=True):
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(function)
            until = time.monotonic() + 3
            while not future.done() and time.monotonic() < until:
                if drive_read:
                    native.namespace["aqp_shm_readonly_adjust"](native.context)
                native.namespace["aqp_shm_rpc_adjust"](native.context)
                time.sleep(0.001)
            return future.result(timeout=0.2)

    try:
        drive(proxy.verify_identity)
        assert proxy._source_identity["readonly"]["read_py_callback_returned_contract"] == CONTRACT
        assert proxy._source_identity["execution"]["read_py_callback_returned_contract"] == CONTRACT
        yield native, proxy, drive
    finally:
        proxy.close()
        native.close()


def late_read_with_nested_execution(native, proxy, *, force_zero_depth=False):
    read_server = native.namespace["_RUNTIMES"]["read"]["_SERVER"]
    runtime = native.namespace["_RUNTIMES"]["read"]
    callback = runtime["aqp_shm_readonly_adjust"]
    original_query = read_server.positions.get_trade_detail_data
    observations = {}
    original = LIVE.request("get_positions", rid="old-position", position_read_route_contract="POSITION_READ_ROUTE_V1")

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(proxy.process_request, original)

        def stalled_query(*args, **kwargs):
            # 原生位置查询已开始；等待 Proxy 真实超时，不依赖固定 sleep 判断退出。
            observations["old_result"] = future.result(timeout=2)
            return original_query(*args, **kwargs)

        read_server.positions.get_trade_detail_data = stalled_query

        def hold_outer_tail(context):
            callback(context)
            header = read_server.mailbox._header()
            observations["head"] = read_server.mailbox.region.read(0, 56)
            observations["request"] = read_server.mailbox.region.read(REQUEST_OFFSET, header[7])
            observations["response"] = read_server.mailbox.region.read(RESPONSE_OFFSET, header[8])
            observations["depth"] = native.namespace["_READ_CALLBACK_DEPTH"]
            # 同线程同步重入执行定时回调仍在旧读包装的栈内，不能出证。
            if force_zero_depth:
                native.namespace["_READ_CALLBACK_DEPTH"] = 0
            try:
                native.namespace["aqp_shm_rpc_adjust"](context)
            finally:
                if force_zero_depth:
                    native.namespace["_READ_CALLBACK_DEPTH"] = observations["depth"]
            observations["witness_during_tail"] = read_server.mailbox.region.read(56, 8)
            observations["finished_during_tail"] = proxy._readonly_callback_finished()
            observations["ping_during_tail"] = proxy.process_request(LIVE.request("ping", rid="nested-ping"))

        runtime["aqp_shm_readonly_adjust"] = hold_outer_tail
        until = time.monotonic() + 2
        while read_server.mailbox._header()[4] == read_server.mailbox._header()[5] and time.monotonic() < until:
            time.sleep(0.001)
        assert read_server.mailbox._header()[4] > read_server.mailbox._header()[5]
        native.namespace["aqp_shm_readonly_adjust"](native.context)
        assert future.done()
    runtime["aqp_shm_readonly_adjust"] = callback
    return original, observations


def test_published_response_tail_reentry_is_blocked_then_later_callback_recovers(model):
    native, proxy, drive = model
    original, result = late_read_with_nested_execution(native, proxy)
    assert result["old_result"]["status"] == "UNKNOWN"
    assert result["old_result"]["automatic_retry"] is False
    assert result["depth"] == 1 and result["witness_during_tail"] == b"\0" * 8
    assert result["finished_during_tail"] is False
    assert result["ping_during_tail"]["server_error"] == "SHM_NOT_SENT"
    assert result["ping_during_tail"]["publication_state"] == "NOT_SENT"
    assert proxy.identity_snapshot()["execution_source_state"] == "HEALTHY"
    assert proxy._readonly_callback_finished() is False
    native.namespace["aqp_shm_rpc_adjust"](native.context)
    assert proxy._readonly_callback_finished() is True
    response = drive(lambda: proxy.process_request(LIVE.request("ping", rid="after-callback-return")), drive_read=False)
    assert response["ok"] is True and response["data"]["execution_source_state"] == "HEALTHY"
    region = native.clients["read"].mailbox.region
    header = native.clients["read"].mailbox._header()
    assert header[4] == header[5] and header[6] == header[4] - 1
    assert region.read(0, 56) == result["head"]
    assert region.read(REQUEST_OFFSET, header[7]) == result["request"]
    assert region.read(RESPONSE_OFFSET, header[8]) == result["response"]
    assert proxy.process_request(original) == result["old_result"]
    assert proxy.identity_snapshot()["write_ready"] is False
    native.writes.assert_not_called()


def test_later_read_callback_revokes_witness_before_stalling(model):
    native, proxy, drive = model
    original, old = late_read_with_nested_execution(native, proxy)
    native.namespace["aqp_shm_rpc_adjust"](native.context)
    assert proxy._readonly_callback_finished() is True
    read = native.clients["read"].mailbox
    execution = native.clients["execution"].mailbox
    before_execution = execution._header()[4:7]
    runtime = native.namespace["_RUNTIMES"]["read"]
    read_server = runtime["_SERVER"]
    callback = runtime["aqp_shm_readonly_adjust"]
    observed = {}

    def stall_before_outer_return(context):
        observed["witness"] = read.region.read(56, 8)
        observed["finished"] = proxy._readonly_callback_finished()
        observed["ping"] = proxy.process_request(LIVE.request("ping", rid="during-later-read"))
        observed["head"] = read.region.read(0, 56)

    # 已发布、尚待清除的 native pending 代表读调用仍有工作，不能复用旧见证。
    read_server.pending = (read._header()[4], {})
    runtime["aqp_shm_readonly_adjust"] = stall_before_outer_return
    try:
        native.namespace["aqp_shm_readonly_adjust"](native.context)
        assert read.region.read(56, 8) == b"\0" * 8 and observed == {}
        native.namespace["aqp_shm_readonly_adjust"](native.context)
    finally:
        runtime["aqp_shm_readonly_adjust"] = callback
        read_server.pending = None
    assert observed["witness"] == b"\0" * 8 and observed["finished"] is False
    assert observed["ping"]["server_error"] == "SHM_NOT_SENT"
    assert observed["ping"]["publication_state"] == "NOT_SENT"
    assert observed["head"] == old["head"] and execution._header()[4:7] == before_execution
    header = read._header()
    assert read.region.read(REQUEST_OFFSET, header[7]) == old["request"]
    assert read.region.read(RESPONSE_OFFSET, header[8]) == old["response"]
    assert header[6] == header[4] - 1
    assert proxy.process_request(original) == old["old_result"]
    assert proxy.identity_snapshot()["execution_source_state"] == "HEALTHY"
    assert proxy._readonly_callback_finished() is False
    native.namespace["aqp_shm_rpc_adjust"](native.context)
    assert proxy._readonly_callback_finished() is True
    response = drive(lambda: proxy.process_request(LIVE.request("ping", rid="after-later-read")), drive_read=False)
    assert response["ok"] is True and response["data"]["execution_source_state"] == "HEALTHY"
    native.writes.assert_not_called()


def test_execution_then_frozen_read_same_phase_preserves_witness_and_proxy_progress(model):
    native, proxy, drive = model
    original, old = late_read_with_nested_execution(native, proxy)
    read = native.clients["read"].mailbox
    runtime = native.namespace["_RUNTIMES"]["read"]
    callback = runtime["aqp_shm_readonly_adjust"]
    calls = []
    runtime["aqp_shm_readonly_adjust"] = lambda context: calls.append(context)
    try:
        for _ in range(10):
            native.namespace["aqp_shm_rpc_adjust"](native.context)
            witness = read.region.read(56, 8)
            assert witness != b"\0" * 8 and proxy._readonly_callback_finished() is True
            native.namespace["aqp_shm_readonly_adjust"](native.context)
            assert read.region.read(56, 8) == witness
            assert proxy._readonly_callback_finished() is True
        assert calls == []
    finally:
        runtime["aqp_shm_readonly_adjust"] = callback
    result = drive(lambda: proxy.process_request(LIVE.request("ping", rid="same-phase-progress")), drive_read=False)
    assert result["ok"] is True and result["data"]["execution_source_state"] == "HEALTHY"
    header = read._header()
    assert read.region.read(0, 56) == old["head"] and header[6] == header[4] - 1
    assert read.region.read(REQUEST_OFFSET, header[7]) == old["request"]
    assert read.region.read(RESPONSE_OFFSET, header[8]) == old["response"]
    assert proxy.process_request(original) == old["old_result"]
    native.writes.assert_not_called()


@pytest.mark.parametrize("change", ["ack", "request_frame", "response_frame", "source_identity", "expired", "reentry"])
def test_only_complete_matching_idle_frame_can_preserve_witness(model, change):
    native, proxy, _ = model
    late_read_with_nested_execution(native, proxy)
    native.namespace["aqp_shm_rpc_adjust"](native.context)
    assert proxy._readonly_callback_finished() is True
    read = native.clients["read"].mailbox
    region = read.region
    header = read._header()
    witness = region.read(56, 8)
    runtime = native.namespace["_RUNTIMES"]["read"]
    callback = runtime["aqp_shm_readonly_adjust"]
    calls = []
    runtime["aqp_shm_readonly_adjust"] = lambda context: calls.append(context)
    if change == "ack":
        header[6] = header[4]
        read._write_header(header)
        region.write(56, witness)
    elif change == "request_frame":
        raw = region.read(REQUEST_OFFSET, header[7])
        altered = raw.replace(b"old-position", b"bad-position")
        assert altered != raw and len(altered) == len(raw)
        region.write(REQUEST_OFFSET, altered)
    elif change == "response_frame":
        raw = region.read(RESPONSE_OFFSET, header[8])
        region.write(RESPONSE_OFFSET, b" " + raw[1:])
    elif change == "source_identity":
        native.namespace["_SOURCE_IDENTITY"]["read_session_id"] = "f" * 32
    elif change == "expired":
        region.write(56, struct.pack("<II", header[4], int(time.time()) - 5))
    else:
        native.namespace["_READ_CALLBACK_DEPTH"] = 1
    try:
        native.namespace["aqp_shm_readonly_adjust"](native.context)
        assert calls == []
        assert region.read(56, 8) == b"\0" * 8
        assert proxy._readonly_callback_finished() is False
        native.namespace["aqp_shm_readonly_adjust"](native.context)
        assert calls == [native.context]
    finally:
        if change == "reentry":
            native.namespace["_READ_CALLBACK_DEPTH"] = 0
        runtime["aqp_shm_readonly_adjust"] = callback
    native.writes.assert_not_called()


def test_read_callback_skips_processing_when_old_mailbox_mutex_is_held(model):
    native, proxy, _ = model
    late_read_with_nested_execution(native, proxy)
    native.namespace["aqp_shm_rpc_adjust"](native.context)
    assert proxy._readonly_callback_finished() is True
    read = native.clients["read"].mailbox
    runtime = native.namespace["_RUNTIMES"]["read"]
    callback = runtime["aqp_shm_readonly_adjust"]
    calls = []
    runtime["aqp_shm_readonly_adjust"] = lambda context: calls.append(context)
    assert read.region.acquire(0)
    try:
        head, witness = read.region.read(0, 56), read.region.read(56, 8)
        assert native.namespace["aqp_shm_readonly_adjust"](native.context) is None
        assert calls == [] and native.namespace["_READ_CALLBACK_DEPTH"] == 0
        assert read.region.read(0, 56) == head and read.region.read(56, 8) == witness
    finally:
        read.region.release()
        runtime["aqp_shm_readonly_adjust"] = callback
    assert proxy._readonly_callback_finished() is True
    native.writes.assert_not_called()


def test_read_callback_skips_during_actual_guarded_execution_request(model):
    native, proxy, _ = model
    late_read_with_nested_execution(native, proxy)
    native.namespace["aqp_shm_rpc_adjust"](native.context)
    assert proxy._readonly_callback_finished() is True
    proxy.timeout_seconds = 1.0
    read = native.clients["read"].mailbox
    execution = native.clients["execution"]
    original_send = execution.send_request
    entered, resume = threading.Event(), threading.Event()
    calls = []
    runtime = native.namespace["_RUNTIMES"]["read"]
    original_callback = runtime["aqp_shm_readonly_adjust"]

    def held_execution_send(request, timeout_seconds, **kwargs):
        entered.set()
        assert resume.wait(timeout=1)
        return original_send(request, timeout_seconds, **kwargs)

    execution.send_request = held_execution_send
    runtime["aqp_shm_readonly_adjust"] = lambda context: calls.append(context)
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(proxy.process_request, LIVE.request("ping", rid="read-timer-during-execution"))
            try:
                assert entered.wait(timeout=1)
                head, witness = read.region.read(0, 56), read.region.read(56, 8)
                native.namespace["aqp_shm_readonly_adjust"](native.context)
                assert calls == [] and read.region.read(0, 56) == head and read.region.read(56, 8) == witness
            finally:
                resume.set()
            until = time.monotonic() + 2
            while not future.done() and time.monotonic() < until:
                native.namespace["aqp_shm_rpc_adjust"](native.context)
                time.sleep(0.001)
            response = future.result(timeout=0.2)
        assert response["ok"] is True and response["data"]["execution_source_state"] == "HEALTHY"
    finally:
        runtime["aqp_shm_readonly_adjust"] = original_callback
        execution.send_request = original_send
    native.writes.assert_not_called()


def test_witness_absent_expired_changed_identity_and_legacy_handshake_fail_closed(model, monkeypatch):
    native, proxy, _ = model
    _, result = late_read_with_nested_execution(native, proxy)
    mailbox = native.clients["read"].mailbox
    region = mailbox.region
    sequence = mailbox._header()[4]
    native.namespace["aqp_shm_rpc_adjust"](native.context)
    assert proxy._readonly_callback_finished() is True
    good = region.read(56, 8)
    for raw in (b"\0" * 8, struct.pack("<II", sequence, int(time.time()) - 5),
                struct.pack("<II", sequence, int(time.time()) + 5),
                struct.pack("<II", sequence + 1, int(time.time()))):
        region.write(56, raw)
        assert proxy._readonly_callback_finished() is False
    region.write(56, good)
    for route in ("readonly", "execution"):
        identity = proxy._source_identity[route]
        saved = identity.pop("read_py_callback_returned_contract")
        assert proxy._readonly_callback_finished() is False
        identity["read_py_callback_returned_contract"] = saved
    old_sid = proxy._sessions["readonly"]
    proxy._sessions["readonly"] = "f" * 32
    assert proxy._readonly_callback_finished() is False
    proxy._sessions["readonly"] = old_sid
    old_execution_sid = proxy._sessions["execution"]
    proxy._sessions["execution"] = "e" * 32
    assert proxy._readonly_callback_finished() is False
    proxy._sessions["execution"] = old_execution_sid
    header = mailbox._header()
    raw_request = region.read(REQUEST_OFFSET, header[7])
    corrupted_request = raw_request.replace(b"old-position", b"bad-position")
    assert corrupted_request != raw_request and len(corrupted_request) == len(raw_request)
    region.write(REQUEST_OFFSET, corrupted_request)
    assert proxy._readonly_callback_finished() is False
    region.write(REQUEST_OFFSET, raw_request)
    original_identity = proxy._verified_process_identity
    monkeypatch.setattr(proxy_module, "_windows_native_process_identity",
                        lambda pid: dict(original_identity, native_creation_filetime=CREATION + 1))
    assert proxy._readonly_callback_finished() is False
    assert region.read(0, 56) == result["head"]
    native.writes.assert_not_called()


def test_stack_thread_depth_and_ack_are_independent_requirements(model):
    native, proxy, _ = model
    _, result = late_read_with_nested_execution(native, proxy, force_zero_depth=True)
    assert result["witness_during_tail"] == b"\0" * 8 and result["finished_during_tail"] is False
    region = native.clients["read"].mailbox.region
    with ThreadPoolExecutor(max_workers=1) as pool:
        pool.submit(native.namespace["_publish_read_callback_return_witness"]).result(timeout=1)
    assert region.read(56, 8) == b"\0" * 8
    native.namespace["_READ_CALLBACK_DEPTH"] = 1
    native.namespace["aqp_shm_rpc_adjust"](native.context)
    native.namespace["_READ_CALLBACK_DEPTH"] = 0
    assert region.read(56, 8) == b"\0" * 8
    native.namespace["aqp_shm_rpc_adjust"](native.context)
    assert proxy._readonly_callback_finished() is True
    mailbox = native.clients["read"].mailbox
    # 原协议头写清除保留位后，必须等待新的同线程见证。
    assert region.acquire(0)
    try:
        header = mailbox._header()
        mailbox._write_header(header)
    finally:
        region.release()
    assert region.read(56, 8) == b"\0" * 8 and proxy._readonly_callback_finished() is False
    native.namespace["aqp_shm_rpc_adjust"](native.context)
    assert proxy._readonly_callback_finished() is True
    assert region.acquire(0)
    try:
        header = mailbox._header()
        header[6] = header[4]
        mailbox._write_header(header)
        assert region.read(56, 8) == b"\0" * 8
    finally:
        region.release()
    native.namespace["aqp_shm_rpc_adjust"](native.context)
    assert region.read(56, 8) == b"\0" * 8
    # 即使人为重填见证，已 ACK 仍不得放行。
    region.write(56, struct.pack("<II", header[4], int(time.time())))
    assert proxy._readonly_callback_finished() is False
    native.writes.assert_not_called()


def test_old_read_mutex_blocks_competing_ack_until_execution_response(model):
    native, proxy, drive = model
    _, result = late_read_with_nested_execution(native, proxy)
    native.namespace["aqp_shm_rpc_adjust"](native.context)
    assert proxy._readonly_callback_finished() is True
    read = native.clients["read"].mailbox
    execution = native.clients["execution"]
    original_send = execution.send_request
    attempted = []

    def send_while_guarded(request, timeout_seconds, **kwargs):
        def competing_ack():
            if not read.region.acquire(0):
                return False
            try:
                header = read._header()
                header[6] = header[4]
                read._write_header(header)
                return True
            finally:
                read.region.release()

        with ThreadPoolExecutor(max_workers=1) as pool:
            attempted.append(pool.submit(competing_ack).result(timeout=1))
        return original_send(request, timeout_seconds, **kwargs)

    execution.send_request = send_while_guarded
    response = drive(lambda: proxy.process_request(LIVE.request("ping", rid="guarded-execution")), drive_read=False)
    assert response["ok"] is True and attempted == [False]
    assert read._header()[6] == read._header()[4] - 1
    assert read.region.read(0, 56) == result["head"]
    native.writes.assert_not_called()


def test_busy_old_read_mutex_defers_execution_without_publication(model):
    native, proxy, _ = model
    late_read_with_nested_execution(native, proxy)
    native.namespace["aqp_shm_rpc_adjust"](native.context)
    read = native.clients["read"].mailbox
    before = native.clients["execution"].mailbox._header()[4:7]
    assert read.region.acquire(0)
    try:
        deferred = proxy.process_request(LIVE.request("ping", rid="busy-read-lock"))
        assert deferred["server_error"] == "SHM_NOT_SENT"
        assert deferred["publication_state"] == "NOT_SENT"
    finally:
        read.region.release()
    assert native.clients["execution"].mailbox._header()[4:7] == before
    assert proxy.identity_snapshot()["execution_source_state"] == "HEALTHY"
    native.writes.assert_not_called()


def test_internal_rotation_control_is_not_sent_when_read_fault_exists(model):
    native, proxy, _ = model
    late_read_with_nested_execution(native, proxy)
    before = native.clients["execution"].mailbox._header()[4:7]
    with pytest.raises(ShmBusyError, match="rotation control"):
        proxy._execution_control_ping()
    native.namespace["aqp_shm_rpc_adjust"](native.context)
    assert proxy._readonly_callback_finished() is True
    with pytest.raises(ShmBusyError, match="rotation control"):
        proxy._execution_control_ping()
    assert native.clients["execution"].mailbox._header()[4:7] == before
    assert proxy.identity_snapshot()["execution_source_state"] == "HEALTHY"
    native.writes.assert_not_called()


def test_witness_does_not_turn_later_execution_timeout_into_not_sent_or_retry(model):
    native, proxy, _ = model
    _, result = late_read_with_nested_execution(native, proxy)
    native.namespace["aqp_shm_rpc_adjust"](native.context)
    assert proxy._readonly_callback_finished() is True
    request = LIVE.request("ping", rid="execution-timeout-after-witness")
    # 见证后不再驱动执行定时回调，模拟请求已发出但执行侧未响应。
    timed_out = proxy.process_request(request)
    assert timed_out["status"] == "UNKNOWN" and timed_out["publication_state"] == "UNKNOWN"
    assert timed_out["automatic_retry"] is False
    assert proxy.identity_snapshot()["execution_source_state"] == "RESULT_UNKNOWN"
    assert proxy.identity_snapshot()["write_ready"] is False
    header = native.clients["execution"].mailbox._header()
    assert header[4] == header[5] + 1
    assert proxy.process_request(request) == timed_out
    assert native.clients["execution"].mailbox._header()[4:7] == header[4:7]
    assert result["old_result"]["status"] == "UNKNOWN"
    native.writes.assert_not_called()
