# coding: utf-8
"""独立只读邮箱；Win32 后端不使用业务文件、网络或后台线程。"""

import ctypes
import json
import math
import os
import re
import struct
import time
from ctypes import wintypes


MAGIC = b"AQPSHM2\x00"
PROTOCOL_VERSION = 2
HEADER_SIZE = 64
REQUEST_CAPACITY = 8192
RESPONSE_CAPACITY = 8 * 1024 * 1024
REQUEST_OFFSET = HEADER_SIZE
RESPONSE_OFFSET = REQUEST_OFFSET + REQUEST_CAPACITY
MAP_SIZE = RESPONSE_OFFSET + RESPONSE_CAPACITY
INITIALIZING, READY, CLOSED, FAILED = 0, 1, 2, 3
_NAME_PATTERN = re.compile(r"Local\\AQP_SHM_[A-Za-z0-9_-]{1,64}\Z")
_HEADER = struct.Struct("<8sIIIIIIII16s8x")


class ShmProtocolError(RuntimeError):
    pass


class ShmBusyError(RuntimeError):
    pass


class ShmTimeoutError(TimeoutError):
    pass


class ShmCapacityError(ValueError):
    pass


class ShmNativeError(OSError):
    pass


def validate_name(name):
    if not isinstance(name, str) or not _NAME_PATTERN.fullmatch(name):
        raise ValueError("mailbox name must be Local\\AQP_SHM_ plus 1-64 ASCII identifier characters")


def _reject_constant(value):
    raise ShmProtocolError("non-finite JSON number: %s" % value)


def _finite_float(value):
    number = float(value)
    if not math.isfinite(number):
        raise ShmProtocolError("non-finite JSON number: %s" % value)
    return number


def _unique_pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ShmProtocolError("duplicate JSON key: %s" % key)
        result[key] = value
    return result


def encode_payload(payload, capacity):
    raw = json.dumps(payload, ensure_ascii=True, allow_nan=False, separators=(",", ":")).encode("ascii")
    if not 0 < len(raw) <= capacity:
        raise ShmCapacityError("payload exceeds %s-byte capacity" % capacity)
    return raw


def decode_payload(raw):
    try:
        return json.loads(raw.decode("ascii"), parse_constant=_reject_constant, parse_float=_finite_float, object_pairs_hook=_unique_pairs)
    except (ValueError, UnicodeError) as exc:
        raise ShmProtocolError("invalid JSON payload: %s" % exc) from exc


class Win32Region:
    """命名对象的唯一所有者或只打开客户端，句柄在部分初始化失败时也释放。"""

    def __init__(self, name, create=False, kernel=None):
        validate_name(name)
        self.mapping = None
        self.mutex = None
        self.address = None
        self.closed = False
        self.cleanup_errors = []
        if kernel is None:
            if os.name != "nt":
                raise ShmNativeError("Win32 shared memory requires Windows")
            kernel = ctypes.WinDLL("kernel32", use_last_error=True)
        self.kernel = kernel
        self._bind()
        self.pid = int(kernel.GetCurrentProcessId())
        try:
            if create:
                ctypes.set_last_error(0)
                self.mapping = kernel.CreateFileMappingW(ctypes.c_void_p(-1), None, 4, 0, MAP_SIZE, name + "_map")
                if self.mapping and ctypes.get_last_error() == 183:
                    raise ShmNativeError("mailbox mapping already exists; refusing to attach or overwrite")
            else:
                self.mapping = kernel.OpenFileMappingW(6, False, name + "_map")
            if not self.mapping:
                raise self._error("mapping open/create")
            self.address = kernel.MapViewOfFile(self.mapping, 6, 0, 0, MAP_SIZE)
            if not self.address:
                raise self._error("MapViewOfFile")
            if create:
                ctypes.set_last_error(0)
                self.mutex = kernel.CreateMutexW(None, False, name + "_mutex")
                if self.mutex and ctypes.get_last_error() == 183:
                    raise ShmNativeError("mailbox mutex already exists; refusing to attach")
            else:
                self.mutex = kernel.OpenMutexW(0x00100001, False, name + "_mutex")
            if not self.mutex:
                raise self._error("mutex open/create")
        except Exception as exc:
            try:
                self.close()
            except Exception as cleanup:
                raise ShmNativeError("%s; cleanup failed: %s" % (exc, cleanup)) from exc
            raise

    def _bind(self):
        signatures = {
            "CreateFileMappingW": ([wintypes.HANDLE, wintypes.LPVOID, wintypes.DWORD, wintypes.DWORD, wintypes.DWORD, wintypes.LPCWSTR], wintypes.HANDLE),
            "OpenFileMappingW": ([wintypes.DWORD, wintypes.BOOL, wintypes.LPCWSTR], wintypes.HANDLE),
            "MapViewOfFile": ([wintypes.HANDLE, wintypes.DWORD, wintypes.DWORD, wintypes.DWORD, ctypes.c_size_t], ctypes.c_void_p),
            "UnmapViewOfFile": ([ctypes.c_void_p], wintypes.BOOL),
            "CreateMutexW": ([wintypes.LPVOID, wintypes.BOOL, wintypes.LPCWSTR], wintypes.HANDLE),
            "OpenMutexW": ([wintypes.DWORD, wintypes.BOOL, wintypes.LPCWSTR], wintypes.HANDLE),
            "WaitForSingleObject": ([wintypes.HANDLE, wintypes.DWORD], wintypes.DWORD),
            "ReleaseMutex": ([wintypes.HANDLE], wintypes.BOOL),
            "CloseHandle": ([wintypes.HANDLE], wintypes.BOOL),
            "GetCurrentProcessId": ([], wintypes.DWORD),
        }
        for name, (arguments, result) in signatures.items():
            function = getattr(self.kernel, name)
            function.argtypes, function.restype = arguments, result

    def _error(self, label):
        return ShmNativeError("%s failed: winerror=%s" % (label, ctypes.get_last_error()))

    def acquire(self, milliseconds):
        if self.closed:
            raise ShmNativeError("mailbox region is closed")
        result = self.kernel.WaitForSingleObject(self.mutex, milliseconds)
        if result == 0:
            return True
        if result == 258:
            return False
        if result == 128:
            self.release()
            raise ShmProtocolError("abandoned mailbox mutex; session cannot be trusted")
        raise self._error("WaitForSingleObject")

    def release(self):
        if not self.kernel.ReleaseMutex(self.mutex):
            raise self._error("ReleaseMutex")

    def read(self, offset, length):
        self._bounds(offset, length)
        return ctypes.string_at(self.address + offset, length)

    def write(self, offset, raw):
        self._bounds(offset, len(raw))
        ctypes.memmove(self.address + offset, raw, len(raw))

    def _bounds(self, offset, length):
        if self.closed or not self.address:
            raise ShmNativeError("mailbox view is closed")
        if type(offset) is not int or type(length) is not int or offset < 0 or length < 0 or offset + length > MAP_SIZE:
            raise ShmProtocolError("memory access is outside the fixed mailbox")

    def close(self):
        if self.closed:
            return
        self.closed = True
        errors = []
        for attribute, operation in (("address", "UnmapViewOfFile"), ("mutex", "CloseHandle"), ("mapping", "CloseHandle")):
            value = getattr(self, attribute)
            if value:
                try:
                    if not getattr(self.kernel, operation)(value):
                        raise self._error(operation)
                except Exception as exc:
                    errors.append(str(exc))
                setattr(self, attribute, None)
        self.cleanup_errors = errors
        if errors:
            raise ShmNativeError("native cleanup incomplete: " + "; ".join(errors))


class ReadOnlyMailbox:
    """确认序号保护唯一在途请求；业务查询在互斥量之外执行。"""

    def __init__(self, name, create=False, region=None):
        validate_name(name)
        self.owner = bool(create)
        self.closed = False
        self.session_id = None
        self.server_pid = None
        self.region = region if region is not None else Win32Region(name, create=create)
        try:
            if not self.region.acquire(500):
                raise ShmBusyError("initial mailbox lock is busy")
            try:
                if create:
                    token = os.urandom(16)
                    self.region.write(0, _HEADER.pack(MAGIC, PROTOCOL_VERSION, INITIALIZING, self.region.pid, 0, 0, 0, 0, 0, token))
                header = self._header(check_session=False)
                self.session_id = header[9].hex()
                self.server_pid = header[3]
                if not create and header[2] != READY:
                    raise ShmProtocolError("server is not ready")
            finally:
                self.region.release()
        except Exception as exc:
            self.closed = True
            try:
                self.region.close()
            except Exception as cleanup:
                raise ShmNativeError("%s; cleanup failed: %s" % (exc, cleanup)) from exc
            raise

    def _header(self, check_session=True):
        values = list(_HEADER.unpack(self.region.read(0, HEADER_SIZE)))
        if values[0] != MAGIC or values[1] != PROTOCOL_VERSION or values[2] not in (INITIALIZING, READY, CLOSED, FAILED) or not values[3]:
            raise ShmProtocolError("mailbox header/version is invalid")
        request, response, confirmed = values[4:7]
        if not 0 <= confirmed <= response <= request <= confirmed + 1:
            raise ShmProtocolError("mailbox sequence state is invalid")
        if check_session and (values[9].hex() != self.session_id or values[3] != self.server_pid):
            raise ShmProtocolError("mailbox session identity changed")
        return values

    def _write_header(self, header):
        self.region.write(0, _HEADER.pack(*header))

    def mark_ready(self):
        if not self.owner or self.closed:
            raise ShmProtocolError("only an open server can become ready")
        if not self.region.acquire(500):
            raise ShmBusyError("ready lock is busy")
        try:
            header = self._header()
            if header[2] != INITIALIZING:
                raise ShmProtocolError("server is already initialized")
            header[2] = READY
            self._write_header(header)
        finally:
            self.region.release()

    def read_request(self):
        if not self.owner or self.closed:
            raise ShmProtocolError("request polling requires an open server")
        if not self.region.acquire(0):
            return None
        try:
            header = self._header()
            if header[2] != READY:
                raise ShmProtocolError("server is not ready")
            if header[4] == header[5]:
                return None
            length = header[7]
            if not 0 < length <= REQUEST_CAPACITY:
                raise ShmProtocolError("invalid request payload length")
            return header[4], decode_payload(self.region.read(REQUEST_OFFSET, length))
        finally:
            self.region.release()

    def publish_response(self, sequence, response):
        if not self.owner or self.closed:
            raise ShmProtocolError("response publishing requires an open server")
        try:
            raw = encode_payload(response, RESPONSE_CAPACITY)
        except (ValueError, TypeError, OverflowError) as exc:
            response = dict(response)
            response.update(ok=False, data=None, error="%s: %s" % (type(exc).__name__, exc), server_error="RESPONSE_TOO_LARGE" if isinstance(exc, ShmCapacityError) else "SERIALIZATION_FAILED")
            raw = encode_payload(response, RESPONSE_CAPACITY)
        if not self.region.acquire(0):
            return False
        try:
            header = self._header()
            if header[2] != READY or header[4] != sequence or header[5] >= sequence:
                raise ShmProtocolError("response sequence is no longer pending")
            self.region.write(RESPONSE_OFFSET, raw)
            header[8], header[5] = len(raw), sequence
            self._write_header(header)
            return True
        finally:
            self.region.release()

    def close(self, failed=False):
        if self.closed:
            return
        self.closed = True
        errors = []
        if self.owner:
            try:
                if not self.region.acquire(50):
                    raise ShmBusyError("closed state could not be published")
                try:
                    header = self._header()
                    header[2] = FAILED if failed else CLOSED
                    self._write_header(header)
                finally:
                    self.region.release()
            except Exception as exc:
                errors.append(str(exc))
        try:
            self.region.close()
        except Exception as exc:
            errors.append(str(exc))
        if errors:
            raise ShmNativeError("mailbox cleanup incomplete: " + "; ".join(errors))


class ShmReadOnlyClient:
    """外部客户端；超时后不重发，不复用未确认邮箱。"""

    def __init__(self, name, account_id, region=None):
        if not isinstance(account_id, str) or not account_id or len(account_id) > 64:
            raise ValueError("a fixed account_id is required")
        self.account_id = account_id
        self.mailbox = ReadOnlyMailbox(name, region=region)
        self.poisoned = False

    def send_request(self, request, timeout_seconds=5.0):
        if self.poisoned or self.mailbox.closed:
            raise ShmProtocolError("client is closed or has an unconfirmed result")
        if not isinstance(request, dict):
            raise ValueError("request must be an object")
        if isinstance(timeout_seconds, bool) or not isinstance(timeout_seconds, (int, float)) or not math.isfinite(timeout_seconds) or not 0 < timeout_seconds <= 60:
            raise ValueError("timeout must be finite and within (0, 60] seconds")
        payload = dict(request)
        request_id = payload.get("request_id")
        if not isinstance(request_id, str) or not request_id or len(request_id) > 128 or any(ord(char) < 33 or ord(char) > 126 for char in request_id):
            raise ValueError("request_id requires 1-128 printable ASCII characters without spaces")
        if not isinstance(payload.get("method"), str) or not payload["method"] or len(payload["method"]) > 64:
            raise ValueError("method requires a nonempty string of at most 64 characters")
        if payload.get("account_id") != self.account_id:
            raise ShmProtocolError("request account_id mismatch")
        if payload.get("session_id", self.mailbox.session_id) != self.mailbox.session_id:
            raise ShmProtocolError("request session_id mismatch")
        expires_at = payload.get("read_expires_at_unix", time.time() + timeout_seconds)
        if isinstance(expires_at, bool) or not isinstance(expires_at, (int, float)) or not math.isfinite(expires_at) or expires_at <= time.time():
            raise ValueError("read deadline must be a finite future timestamp")
        payload.update(schema_version=payload.get("schema_version", 1), session_id=self.mailbox.session_id, read_expires_at_unix=min(expires_at, time.time() + timeout_seconds))
        read_timing = ({"shm_client_started_at_unix": time.time(),
                        "shm_deadline_at_unix": payload["read_expires_at_unix"]}
                       if payload.get("method") == "get_positions" else None)
        deadline = time.monotonic() + timeout_seconds
        region = self.mailbox.region
        remaining = min(deadline - time.monotonic(), payload["read_expires_at_unix"] - time.time())
        if remaining <= 0 or not region.acquire(max(1, int(math.ceil(remaining * 1000)))):
            error = ShmTimeoutError("mutex deadline expired before publication; request not sent")
            if read_timing is not None:
                error.read_timing = dict(read_timing)
            raise error
        try:
            if read_timing is not None:
                read_timing["shm_lock_acquired_at_unix"] = time.time()
            if time.monotonic() >= deadline or time.time() >= payload["read_expires_at_unix"]:
                raise ShmTimeoutError("mutex deadline expired before publication; request not sent")
            header = self.mailbox._header()
            if header[2] != READY:
                raise ShmProtocolError("server is not ready")
            if header[4] != header[6]:
                raise ShmBusyError("mailbox has an unconfirmed request")
            sequence = header[4] + 1
            if sequence > 0xFFFFFFFF:
                raise ShmProtocolError("request sequence exhausted")
            if "sequence" in payload and (type(payload["sequence"]) is not int or payload["sequence"] != sequence):
                raise ShmProtocolError("request sequence mismatch")
            payload["sequence"] = sequence
            raw = encode_payload(payload, REQUEST_CAPACITY)
            region.write(REQUEST_OFFSET, raw)
            header[7], header[4] = len(raw), sequence
            self.mailbox._write_header(header)
            if read_timing is not None:
                read_timing["shm_published_at_unix"] = time.time()
        except Exception as exc:
            if read_timing is not None:
                exc.read_timing = dict(read_timing)
            raise
        finally:
            region.release()
        try:
            while time.monotonic() < deadline:
                if region.acquire(0):
                    try:
                        header = self.mailbox._header()
                        if header[5] == sequence:
                            if read_timing is not None:
                                read_timing["shm_response_observed_at_unix"] = time.time()
                            if not 0 < header[8] <= RESPONSE_CAPACITY:
                                raise ShmProtocolError("invalid response payload length")
                            response = decode_payload(region.read(RESPONSE_OFFSET, header[8]))
                            self._validate_response(payload, response)
                            if read_timing is not None:
                                read_timing["shm_response_validated_at_unix"] = time.time()
                                native_timing = response.get("read_timing")
                                if isinstance(native_timing, dict):
                                    for field in ("native_started_at_unix", "native_finished_at_unix"):
                                        value = native_timing.get(field)
                                        if type(value) in (int, float) and 0 < value < 1e12:
                                            read_timing[field] = value
                            if time.monotonic() >= deadline or time.time() >= payload["read_expires_at_unix"]:
                                raise ShmTimeoutError("response arrived after its deadline; result unconfirmed")
                            header[6] = sequence
                            self.mailbox._write_header(header)
                            if read_timing is not None:
                                read_timing["shm_ack_at_unix"] = time.time()
                                response["read_timing"] = dict(read_timing)
                            return response
                        if header[2] in (CLOSED, FAILED):
                            raise ShmProtocolError("server closed before replying")
                    finally:
                        region.release()
                time.sleep(min(0.01, max(0, deadline - time.monotonic())))
            raise ShmTimeoutError("shared memory response timeout; result unconfirmed, do not resubmit")
        except Exception as exc:
            self.poisoned = True
            if read_timing is not None:
                exc.read_timing = dict(read_timing)
            raise

    def _validate_response(self, request, response):
        if not isinstance(response, dict) or type(response.get("ok")) is not bool or type(response.get("schema_version")) is not int or response["schema_version"] != 1 or type(response.get("sequence")) is not int:
            raise ShmProtocolError("invalid response envelope")
        if not all(field in response for field in ("data", "error", "server_error", "handled_at")) or not all(isinstance(response[field], str) for field in ("error", "server_error", "handled_at")):
            raise ShmProtocolError("response is missing typed result/error fields")
        if response["ok"] and (response["data"] is None or response["error"] or response["server_error"]):
            raise ShmProtocolError("successful response has missing data or an error")
        if not response["ok"] and (not response["error"] or not response["server_error"]):
            raise ShmProtocolError("failed response has no explicit error")
        for field in ("request_id", "account_id", "method", "session_id", "sequence"):
            if response.get(field) != request.get(field):
                raise ShmProtocolError("response %s mismatch" % field)

    def close(self):
        self.mailbox.close()
