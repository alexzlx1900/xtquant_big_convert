# coding: utf-8
"""QMT 侧受限监督器管道和写门；缺少内核证据时始终拒绝。"""

import ctypes
from ctypes import wintypes
import json
import os
import struct
import time


WAIT_OBJECT_0 = 0
WAIT_ABANDONED = 128
WAIT_TIMEOUT = 258
MAX_FRAME = 32768
_HANDLE_CONTRACT = {
    "AQP_WRITE_BLOCK_EVENT_HANDLE": ("Event", 0x00100000),
    "AQP_IDENTITY_READY_EVENT_HANDLE": ("Event", 0x00100000),
    "AQP_WRITE_GATE_MUTEX_HANDLE": ("Mutant", 0x00100001),
    "AQP_SUPERVISOR_PROCESS_HANDLE": ("Process", 0x00101000),
    "AQP_IDENTITY_EPOCH_MAPPING_HANDLE": ("Section", 0x00000004),
    "AQP_GUARD_ATTACH_ACK_HANDLE": ("Event", 0x00000002),
}


class NativeGuardError(RuntimeError):
    pass


def _unique_pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise NativeGuardError("DUPLICATE_CONTROL_FIELD")
        result[key] = value
    return result


def _reject_constant(value):
    raise NativeGuardError("NONFINITE_CONTROL_FIELD")


class WindowsNativeKernel:
    """真实 Windows API；所有句柄宽度和实际授权均由系统读取。"""

    def __init__(self):
        if os.name != "nt":
            raise NativeGuardError("WINDOWS_REQUIRED")
        self.k = ctypes.WinDLL("kernel32", use_last_error=True)
        self.nt = ctypes.WinDLL("ntdll", use_last_error=True)
        functions = {
            "CreateFileW": ((wintypes.LPCWSTR, ctypes.c_uint32, ctypes.c_uint32, ctypes.c_void_p,
                              ctypes.c_uint32, ctypes.c_uint32, ctypes.c_void_p), ctypes.c_void_p),
            "GetNamedPipeServerProcessId": ((ctypes.c_void_p, ctypes.POINTER(ctypes.c_uint32)), ctypes.c_int),
            "SetNamedPipeHandleState": ((ctypes.c_void_p, ctypes.POINTER(ctypes.c_uint32), ctypes.c_void_p,
                                         ctypes.c_void_p), ctypes.c_int),
            "WriteFile": ((ctypes.c_void_p, ctypes.c_void_p, ctypes.c_uint32,
                           ctypes.POINTER(ctypes.c_uint32), ctypes.c_void_p), ctypes.c_int),
            "ReadFile": ((ctypes.c_void_p, ctypes.c_void_p, ctypes.c_uint32,
                          ctypes.POINTER(ctypes.c_uint32), ctypes.c_void_p), ctypes.c_int),
            "GetProcessId": ((ctypes.c_void_p,), ctypes.c_uint32),
            "GetProcessTimes": ((ctypes.c_void_p, ctypes.POINTER(wintypes.FILETIME),
                                 ctypes.POINTER(wintypes.FILETIME), ctypes.POINTER(wintypes.FILETIME),
                                 ctypes.POINTER(wintypes.FILETIME)), ctypes.c_int),
            "ProcessIdToSessionId": ((ctypes.c_uint32, ctypes.POINTER(ctypes.c_uint32)), ctypes.c_int),
            "WaitForSingleObject": ((ctypes.c_void_p, ctypes.c_uint32), ctypes.c_uint32),
            "ReleaseMutex": ((ctypes.c_void_p,), ctypes.c_int),
            "SetEvent": ((ctypes.c_void_p,), ctypes.c_int),
            "MapViewOfFile": ((ctypes.c_void_p, ctypes.c_uint32, ctypes.c_uint32,
                               ctypes.c_uint32, ctypes.c_size_t), ctypes.c_void_p),
            "UnmapViewOfFile": ((ctypes.c_void_p,), ctypes.c_int),
            "CloseHandle": ((ctypes.c_void_p,), ctypes.c_int),
        }
        for name, (arguments, result) in functions.items():
            function = getattr(self.k, name)
            function.argtypes, function.restype = arguments, result
        self.nt.NtQueryObject.argtypes = (ctypes.c_void_p, ctypes.c_int, ctypes.c_void_p,
                                           ctypes.c_uint32, ctypes.POINTER(ctypes.c_uint32))
        self.nt.NtQueryObject.restype = ctypes.c_int32

    def open_pipe(self, name):
        value = self.k.CreateFileW(name, 0xC0000000, 0, None, 3, 0, None)
        if not value or value == ctypes.c_void_p(-1).value:
            raise NativeGuardError("SUPERVISOR_PIPE_UNAVAILABLE")
        return int(value)

    def peer_pid(self, handle):
        value = ctypes.c_uint32()
        if not self.k.GetNamedPipeServerProcessId(handle, ctypes.byref(value)) or not value.value:
            raise NativeGuardError("SUPERVISOR_PIPE_PEER_UNVERIFIED")
        return int(value.value)

    def nonblocking_message_mode(self, handle):
        mode = ctypes.c_uint32(0x00000002 | 0x00000001)
        if not self.k.SetNamedPipeHandleState(handle, ctypes.byref(mode), None, None):
            raise NativeGuardError("SUPERVISOR_PIPE_MODE_UNAVAILABLE")

    def write(self, handle, frame):
        written = ctypes.c_uint32()
        buffer = ctypes.create_string_buffer(frame)
        if not self.k.WriteFile(handle, buffer, len(frame), ctypes.byref(written), None):
            raise NativeGuardError("SUPERVISOR_PIPE_WRITE_UNKNOWN")
        return int(written.value)

    def read(self, handle):
        buffer = ctypes.create_string_buffer(MAX_FRAME + 4)
        count = ctypes.c_uint32()
        if not self.k.ReadFile(handle, buffer, len(buffer), ctypes.byref(count), None):
            error = ctypes.get_last_error()
            if error in (232, 536):  # No message yet; a read has no external side effect.
                return None
            raise NativeGuardError("SUPERVISOR_PIPE_READ_UNKNOWN")
        if not count.value:
            return None
        return bytes(buffer.raw[:count.value])

    def process_identity(self, handle):
        pid = int(self.k.GetProcessId(handle))
        created, exited, kernel, user = (wintypes.FILETIME() for _ in range(4))
        session = ctypes.c_uint32()
        if (not pid or not self.k.GetProcessTimes(handle, ctypes.byref(created), ctypes.byref(exited),
                                                  ctypes.byref(kernel), ctypes.byref(user))
                or not self.k.ProcessIdToSessionId(pid, ctypes.byref(session))):
            raise NativeGuardError("SUPERVISOR_PROCESS_IDENTITY_UNVERIFIED")
        return dict(pid=pid, creation_filetime=(created.dwHighDateTime << 32) | created.dwLowDateTime,
                    windows_session_id=int(session.value))

    def object_identity(self, handle):
        class Basic(ctypes.Structure):
            _fields_ = [("attributes", ctypes.c_uint32), ("access", ctypes.c_uint32),
                        ("handle_count", ctypes.c_uint32), ("pointer_count", ctypes.c_uint32),
                        ("reserved", ctypes.c_uint32 * 10)]

        class UnicodeString(ctypes.Structure):
            _fields_ = [("length", ctypes.c_uint16), ("maximum", ctypes.c_uint16),
                        ("buffer", ctypes.c_void_p)]

        basic, size = Basic(), ctypes.c_uint32()
        if self.nt.NtQueryObject(handle, 0, ctypes.byref(basic), ctypes.sizeof(basic), ctypes.byref(size)) != 0:
            raise NativeGuardError("GUARD_ACCESS_UNVERIFIED")
        buffer = ctypes.create_string_buffer(8192)
        if self.nt.NtQueryObject(handle, 2, buffer, len(buffer), ctypes.byref(size)) != 0:
            raise NativeGuardError("GUARD_TYPE_UNVERIFIED")
        name = UnicodeString.from_buffer(buffer)
        start, end = ctypes.addressof(buffer), ctypes.addressof(buffer) + len(buffer)
        if (not name.buffer or name.length == 0 or name.length % 2 or name.length > name.maximum
                or name.buffer < start or name.buffer + name.length > end):
            raise NativeGuardError("GUARD_TYPE_UNVERIFIED")
        return ctypes.wstring_at(name.buffer, name.length // 2), int(basic.access)

    def wait(self, handle, milliseconds):
        return int(self.k.WaitForSingleObject(handle, milliseconds))

    def release_mutex(self, handle):
        if not self.k.ReleaseMutex(handle):
            raise NativeGuardError("GUARD_MUTEX_RELEASE_FAILED")

    def set_event(self, handle):
        if not self.k.SetEvent(handle):
            raise NativeGuardError("GUARD_ATTACH_ACK_UNKNOWN")

    def map_epoch(self, handle):
        address = self.k.MapViewOfFile(handle, 0x0004, 0, 0, 8)
        if not address or int(address) % 8:
            if address:
                self.k.UnmapViewOfFile(address)
            raise NativeGuardError("GUARD_EPOCH_MAPPING_UNAVAILABLE")
        return int(address)

    def read_epoch(self, address):
        return int(ctypes.c_uint64.from_address(address).value)

    def unmap_epoch(self, address):
        if not self.k.UnmapViewOfFile(address):
            raise NativeGuardError("GUARD_EPOCH_UNMAP_FAILED")

    def close(self, handle):
        if not self.k.CloseHandle(handle):
            raise NativeGuardError("GUARD_HANDLE_CLOSE_FAILED")


class NativeControlPipe:
    """同一认证连接；写入或回复不明会永久污染，绝不自动重发。"""

    def __init__(self, kernel, supervisor_identity, retained_identity, timeout=2.0):
        if not 0 < timeout <= 10:
            raise ValueError("bounded pipe timeout required")
        self.kernel, self.identity, self.retained_identity = kernel, dict(supervisor_identity), retained_identity
        self.timeout, self.handle, self.poisoned = timeout, None, False

    def _verify(self):
        if (self.retained_identity() != self.identity or self.handle is None
                or self.kernel.peer_pid(self.handle) != self.identity["pid"]):
            raise NativeGuardError("SUPERVISOR_PIPE_IDENTITY_MISMATCH")

    def connect(self):
        if self.handle is not None or self.poisoned or self.retained_identity() != self.identity:
            raise NativeGuardError("SUPERVISOR_PIPE_NOT_REPEATABLE")
        name = r"\\.\pipe\AQP_VM_GUARD_%d_%016x" % (self.identity["pid"], self.identity["creation_filetime"])
        self.handle = self.kernel.open_pipe(name)
        try:
            self._verify()
            self.kernel.nonblocking_message_mode(self.handle)
            self._verify()
        except BaseException:
            self.poisoned = True
            raise

    def exchange(self, request):
        if self.poisoned:
            raise NativeGuardError("SUPERVISOR_PIPE_POISONED")
        try:
            self._verify()
            raw = json.dumps(request, ensure_ascii=True, allow_nan=False, separators=(",", ":")).encode("utf-8")
            if not 0 < len(raw) <= MAX_FRAME:
                raise NativeGuardError("SUPERVISOR_FRAME_SIZE_INVALID")
            frame = struct.pack("<I", len(raw)) + raw
            # Even an exception or zero-byte write can mean publication; never repeat it.
            if self.kernel.write(self.handle, frame) != len(frame):
                raise NativeGuardError("SUPERVISOR_PIPE_WRITE_UNKNOWN")
            deadline = time.monotonic() + self.timeout
            while True:
                if time.monotonic() >= deadline:
                    raise NativeGuardError("SUPERVISOR_PIPE_READ_UNKNOWN")
                self._verify()
                response = self.kernel.read(self.handle)
                if response is None:
                    time.sleep(0.005)
                    continue
                if len(response) < 4:
                    raise NativeGuardError("SUPERVISOR_FRAME_INVALID")
                length = struct.unpack("<I", response[:4])[0]
                if not 0 < length <= MAX_FRAME or len(response) != length + 4:
                    raise NativeGuardError("SUPERVISOR_FRAME_INVALID")
                decoded = json.loads(response[4:].decode("utf-8"), object_pairs_hook=_unique_pairs,
                                     parse_constant=_reject_constant)
                if not isinstance(decoded, dict):
                    raise NativeGuardError("SUPERVISOR_FRAME_INVALID")
                self._verify()
                return decoded
        except BaseException:
            self.poisoned = True
            raise

    def close(self):
        if self.handle is not None:
            handle, self.handle = self.handle, None
            self.kernel.close(handle)
        self.poisoned = True


class NativeWriteGate:
    """执行函数的同一回调线程持 mutex 校验事件、父进程与只读 epoch。"""

    def __init__(self, kernel, handles, supervisor_identity, retained_identity):
        if not isinstance(handles, dict) or set(handles) != set(_HANDLE_CONTRACT):
            raise NativeGuardError("GUARD_HANDLE_OFFER_INVALID")
        if (any(type(value) is not int or not 0 < value < (1 << (ctypes.sizeof(ctypes.c_void_p) * 8)) - 1
                for value in handles.values()) or len(set(handles.values())) != len(handles)):
            raise NativeGuardError("GUARD_HANDLE_OFFER_INVALID")
        self.kernel, self.handles = kernel, dict(handles)
        self.identity, self.retained_identity = dict(supervisor_identity), retained_identity
        self.address, self.held, self.failed = None, False, False
        for field, expected in _HANDLE_CONTRACT.items():
            if kernel.object_identity(handles[field]) != expected:
                raise NativeGuardError("GUARD_HANDLE_RIGHTS_OR_TYPE_MISMATCH")
        self._parent()
        block, ready = handles["AQP_WRITE_BLOCK_EVENT_HANDLE"], handles["AQP_IDENTITY_READY_EVENT_HANDLE"]
        if kernel.wait(block, 0) != WAIT_TIMEOUT or kernel.wait(ready, 0) != WAIT_TIMEOUT:
            raise NativeGuardError("GUARD_INITIAL_EVENT_STATE_INVALID")
        self.address = kernel.map_epoch(handles["AQP_IDENTITY_EPOCH_MAPPING_HANDLE"])
        try:
            with self:
                if self.read_epoch() < 1:
                    raise NativeGuardError("GUARD_EPOCH_INVALID")
        except BaseException:
            kernel.unmap_epoch(self.address)
            self.address = None
            raise

    def _parent(self):
        handle = self.handles["AQP_SUPERVISOR_PROCESS_HANDLE"]
        if (self.retained_identity() != self.identity or self.kernel.process_identity(handle) != self.identity
                or self.kernel.wait(handle, 0) != WAIT_TIMEOUT):
            self.failed = True
            raise NativeGuardError("GUARD_SUPERVISOR_IDENTITY_UNVERIFIED")

    def __enter__(self):
        if self.failed or self.held:
            raise NativeGuardError("GUARD_UNAVAILABLE")
        # QMT 策略共用回调线程；不能在该线程等待其他进程释放锁。
        result = self.kernel.wait(self.handles["AQP_WRITE_GATE_MUTEX_HANDLE"], 0)
        if result == WAIT_ABANDONED:
            self.failed = True
            self.kernel.release_mutex(self.handles["AQP_WRITE_GATE_MUTEX_HANDLE"])
            raise NativeGuardError("GUARD_MUTEX_ABANDONED")
        if result == WAIT_TIMEOUT:
            # 观察/发布窗口占锁只拒绝本次，后续仍须重新核对 READY 和代次。
            raise NativeGuardError("GUARD_MUTEX_BUSY")
        if result != WAIT_OBJECT_0:
            self.failed = True
            raise NativeGuardError("GUARD_MUTEX_UNAVAILABLE")
        self.held = True
        return self

    def __exit__(self, exc_type, exc, traceback):
        self.held = False
        try:
            self.kernel.release_mutex(self.handles["AQP_WRITE_GATE_MUTEX_HANDLE"])
        except BaseException:
            self.failed = True
            raise
        return False

    def read_epoch(self):
        if self.failed or not self.held or self.address is None:
            raise NativeGuardError("GUARD_EPOCH_UNAVAILABLE")
        epoch = self.kernel.read_epoch(self.address)
        if type(epoch) is not int or not 1 <= epoch <= 0xFFFFFFFFFFFFFFFF:
            self.failed = True
            raise NativeGuardError("GUARD_EPOCH_INVALID")
        return epoch

    def ensure_ready(self):
        if self.failed or not self.held:
            raise NativeGuardError("GUARD_UNAVAILABLE")
        self._parent()
        block = self.kernel.wait(self.handles["AQP_WRITE_BLOCK_EVENT_HANDLE"], 0)
        ready = self.kernel.wait(self.handles["AQP_IDENTITY_READY_EVENT_HANDLE"], 0)
        if block != WAIT_TIMEOUT or ready not in (WAIT_OBJECT_0, WAIT_TIMEOUT):
            self.failed = True
            raise NativeGuardError("GUARD_NOT_READY")
        return ready == WAIT_OBJECT_0

    def acknowledge(self):
        self._parent()
        self.kernel.set_event(self.handles["AQP_GUARD_ATTACH_ACK_HANDLE"])

    def close(self):
        self.failed = True
        if self.address is not None:
            address, self.address = self.address, None
            self.kernel.unmap_epoch(address)
        for handle in self.handles.values():
            self.kernel.close(handle)
