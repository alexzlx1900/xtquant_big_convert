"""无需真实 QMT 的监督器句柄、单次控制帧与写门故障模型。"""

import json
import struct

import pytest

from bigqmt_signal_trader import native_supervisor_guard as native


IDENTITY = dict(pid=123, creation_filetime=0x1234567890ABCDEF, windows_session_id=1)
HANDLES = {name: number for number, name in enumerate(native._HANDLE_CONTRACT, 10)}


class Kernel:
    def __init__(self):
        self.peer, self.parent, self.parent_state = 123, dict(IDENTITY), native.WAIT_TIMEOUT
        self.types = {HANDLES[name]: value for name, value in native._HANDLE_CONTRACT.items()}
        self.states = {HANDLES["AQP_WRITE_BLOCK_EVENT_HANDLE"]: native.WAIT_TIMEOUT,
                       HANDLES["AQP_IDENTITY_READY_EVENT_HANDLE"]: native.WAIT_TIMEOUT,
                       HANDLES["AQP_WRITE_GATE_MUTEX_HANDLE"]: native.WAIT_OBJECT_0}
        self.epoch, self.response, self.writes = 1, None, []
        self.ack, self.releases, self.closed = [], [], []
        self.short_write = False

    def open_pipe(self, name):
        assert name == r"\\.\pipe\AQP_VM_GUARD_123_1234567890abcdef"
        return 99

    def peer_pid(self, handle):
        return self.peer

    def nonblocking_message_mode(self, handle):
        assert handle == 99

    def write(self, handle, frame):
        self.writes.append(frame)
        return 0 if self.short_write else len(frame)

    def read(self, handle):
        return self.response

    def process_identity(self, handle):
        return dict(self.parent)

    def object_identity(self, handle):
        return self.types[handle]

    def wait(self, handle, timeout):
        if handle == HANDLES["AQP_SUPERVISOR_PROCESS_HANDLE"]:
            return self.parent_state
        return self.states[handle]

    def release_mutex(self, handle):
        self.releases.append(handle)

    def set_event(self, handle):
        self.ack.append(handle)

    def map_epoch(self, handle):
        return 0x1000

    def read_epoch(self, address):
        return self.epoch

    def unmap_epoch(self, address):
        pass

    def close(self, handle):
        self.closed.append(handle)


def packet(value):
    raw = json.dumps(value, separators=(",", ":")).encode()
    return struct.pack("<I", len(raw)) + raw


def test_message_pipe_checks_peer_and_retained_process_before_and_after_each_frame():
    kernel = Kernel()
    pipe = native.NativeControlPipe(kernel, IDENTITY, lambda: dict(IDENTITY))
    pipe.connect()
    kernel.response = packet(dict(status="HANDLE_OFFER"))
    assert pipe.exchange(dict(action="ATTACH")) == {"status": "HANDLE_OFFER"}
    frame = kernel.writes[0]
    assert struct.unpack("<I", frame[:4])[0] == len(frame) - 4
    assert json.loads(frame[4:].decode()) == {"action": "ATTACH"}
    kernel.peer = 124
    with pytest.raises(native.NativeGuardError, match="IDENTITY"):
        pipe.exchange(dict(action="PREPARE"))
    assert len(kernel.writes) == 1
    with pytest.raises(native.NativeGuardError, match="POISONED"):
        pipe.exchange(dict(action="PREPARE"))


@pytest.mark.parametrize("failure", ["short_write", "missing_response", "malformed_response", "changed_parent"])
def test_unknown_pipe_result_never_resends(failure):
    kernel = Kernel()
    retained = dict(IDENTITY)
    pipe = native.NativeControlPipe(kernel, IDENTITY, lambda: retained, timeout=0.01)
    pipe.connect()
    if failure == "short_write":
        kernel.short_write = True
    elif failure == "malformed_response":
        kernel.response = b"\x01\x00\x00\x00{"  # Invalid JSON.
    elif failure == "changed_parent":
        retained["creation_filetime"] += 1
    with pytest.raises((native.NativeGuardError, ValueError)):
        pipe.exchange(dict(action="ATTACH"))
    count = len(kernel.writes)
    with pytest.raises(native.NativeGuardError, match="POISONED"):
        pipe.exchange(dict(action="ATTACH"))
    assert len(kernel.writes) == count
    assert count == (0 if failure == "changed_parent" else 1)


@pytest.mark.parametrize("field", list(native._HANDLE_CONTRACT))
def test_all_offered_handles_must_have_exact_type_and_minimum_rights(field):
    kernel = Kernel()
    expected_type, expected_rights = kernel.types[HANDLES[field]]
    kernel.types[HANDLES[field]] = (expected_type, expected_rights | 0x80)
    with pytest.raises(native.NativeGuardError, match="RIGHTS_OR_TYPE"):
        native.NativeWriteGate(kernel, HANDLES, IDENTITY, lambda: dict(IDENTITY))
    assert kernel.ack == []


def test_gate_acknowledges_only_after_verifying_all_handles_and_parent():
    kernel = Kernel()
    gate = native.NativeWriteGate(kernel, HANDLES, IDENTITY, lambda: dict(IDENTITY))
    assert kernel.ack == []
    gate.acknowledge()
    assert kernel.ack == [HANDLES["AQP_GUARD_ATTACH_ACK_HANDLE"]]
    kernel.states[HANDLES["AQP_IDENTITY_READY_EVENT_HANDLE"]] = native.WAIT_OBJECT_0
    with gate as lease:
        assert lease.ensure_ready() is True
        assert lease.read_epoch() == 1
        kernel.epoch = 2
        assert lease.read_epoch() == 2
    assert len(kernel.releases) == 2  # initial check and actual lease


@pytest.mark.parametrize("failure", ["parent_exited", "parent_reused", "block", "abandoned", "epoch_zero"])
def test_gate_never_allows_write_on_unknown_or_stale_kernel_evidence(failure):
    kernel = Kernel()
    gate = native.NativeWriteGate(kernel, HANDLES, IDENTITY, lambda: dict(IDENTITY))
    kernel.states[HANDLES["AQP_IDENTITY_READY_EVENT_HANDLE"]] = native.WAIT_OBJECT_0
    if failure == "parent_exited":
        kernel.parent_state = native.WAIT_OBJECT_0
    elif failure == "parent_reused":
        kernel.parent["creation_filetime"] += 1
    elif failure == "block":
        kernel.states[HANDLES["AQP_WRITE_BLOCK_EVENT_HANDLE"]] = native.WAIT_OBJECT_0
    elif failure == "abandoned":
        kernel.states[HANDLES["AQP_WRITE_GATE_MUTEX_HANDLE"]] = native.WAIT_ABANDONED
    elif failure == "epoch_zero":
        kernel.epoch = 0
    called = []
    with pytest.raises(native.NativeGuardError):
        with gate as lease:
            lease.ensure_ready()
            lease.read_epoch()
            called.append(True)
    assert called == []
    assert gate.failed is True


def test_temporary_ready_reset_and_mutex_busy_do_not_reuse_previous_epoch():
    kernel = Kernel()
    gate = native.NativeWriteGate(kernel, HANDLES, IDENTITY, lambda: dict(IDENTITY))
    with gate as lease:
        assert lease.ensure_ready() is False
        assert lease.read_epoch() == 1
    assert gate.failed is False
    kernel.states[HANDLES["AQP_WRITE_GATE_MUTEX_HANDLE"]] = native.WAIT_TIMEOUT
    with pytest.raises(native.NativeGuardError, match="MUTEX_BUSY"):
        with gate:
            pytest.fail("busy mutex must not allow a write")
    assert gate.failed is False
    kernel.states[HANDLES["AQP_WRITE_GATE_MUTEX_HANDLE"]] = native.WAIT_OBJECT_0
    kernel.states[HANDLES["AQP_IDENTITY_READY_EVENT_HANDLE"]] = native.WAIT_OBJECT_0
    kernel.epoch = 2
    with gate as lease:
        assert lease.ensure_ready() is True
        assert lease.read_epoch() == 2  # A request bound to the old epoch 1 must still be rejected.
