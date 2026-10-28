"""仅供隔离验证：通过 Parallels stdio 连接两端 127.0.0.1，不访问现有 Redis。"""

import argparse
import base64
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


GUEST_PYTHON = r"C:\Users\zhanglongxiang\miniconda3\envs\qmt_310\python.exe"
MAX_CHUNK = 16384


class FramedOutput:
    def __init__(self, stream):
        self.stream, self.lock = stream, threading.Lock()

    def send(self, value):
        line = json.dumps(value, ensure_ascii=True, separators=(",", ":")) + "\n"
        with self.lock:
            self.stream.write(line)
            self.stream.flush()


class Connections:
    def __init__(self):
        self.items, self.lock = {}, threading.Lock()

    def add(self, key, connection):
        with self.lock:
            if key in self.items:
                raise RuntimeError("duplicate relay connection")
            self.items[key] = connection

    def get(self, key):
        with self.lock:
            return self.items.get(key)

    def close(self, key):
        with self.lock:
            connection = self.items.pop(key, None)
        if connection is not None:
            try:
                connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            connection.close()

    def close_all(self):
        with self.lock:
            keys = list(self.items)
        for key in keys:
            self.close(key)


def forward_reader(connection, key, output, connections, stopped):
    try:
        while not stopped.is_set():
            try:
                data = connection.recv(MAX_CHUNK)
            except socket.timeout:
                continue
            if not data:
                break
            output.send(dict(op="DATA", id=key, data=base64.b64encode(data).decode("ascii")))
    except (OSError, ValueError, BrokenPipeError):
        pass
    finally:
        connections.close(key)
        try:
            output.send(dict(op="CLOSE", id=key))
        except (OSError, ValueError, BrokenPipeError):
            pass


def accept_guest(listener, output, connections, stopped):
    next_id = 0
    while not stopped.is_set():
        try:
            connection, address = listener.accept()
        except socket.timeout:
            continue
        except OSError:
            return
        if address[0] != "127.0.0.1":
            connection.close()
            continue
        next_id += 1
        connection.settimeout(0.2)
        connections.add(next_id, connection)
        output.send(dict(op="OPEN", id=next_id))
        threading.Thread(target=forward_reader, args=(connection, next_id, output, connections, stopped), daemon=True).start()


def relay_evidence(stream, output):
    for line in stream:
        try:
            value = json.loads(line)
            if (not isinstance(value, dict) or set(value) - {"step", "status", "count", "run_id", "error_type"}
                    or not isinstance(value.get("step"), str) or not re.fullmatch(r"[A-Z0-9_]{1,64}", value["step"])
                    or not isinstance(value.get("status"), str) or not re.fullmatch(r"[A-Z0-9_]{1,32}", value["status"])
                    or "count" in value and type(value["count"]) is not int
                    or "run_id" in value and (not isinstance(value["run_id"], str)
                                              or not re.fullmatch(r"[0-9a-f]{32}", value["run_id"]))
                    or "error_type" in value and (not isinstance(value["error_type"], str)
                                                  or not re.fullmatch(r"[A-Za-z0-9_]{1,64}", value["error_type"]))):
                continue
            output.send(dict(op="EVIDENCE", value=value))
        except (ValueError, OSError, BrokenPipeError):
            continue


def receive_control(stream, output, connections, stopped, start):
    try:
        for line in stream:
            message = json.loads(line)
            op, key = message.get("op"), message.get("id")
            if op == "START":
                start(message)
            elif op == "DATA":
                connection = connections.get(key)
                if connection is None:
                    continue
                payload = base64.b64decode(message["data"], validate=True)
                if len(payload) > MAX_CHUNK:
                    raise RuntimeError("relay frame exceeds bound")
                connection.sendall(payload)
            elif op == "CLOSE":
                connections.close(key)
            elif op == "STOP":
                return
            else:
                raise RuntimeError("unknown relay control")
    except (OSError, ValueError, KeyError, RuntimeError):
        pass
    finally:
        stopped.set()
        connections.close_all()


def isolated_child_environment(values, executable, port, source):
    if (not isinstance(values, dict) or any(not isinstance(key, str) or not key.startswith("AQP_ISOLATED_")
            or not isinstance(value, str) for key, value in values.items())):
        raise RuntimeError("relay test environment is invalid")
    allowed = ("SystemRoot", "WINDIR", "COMSPEC", "PATHEXT", "TEMP", "TMP")
    env = {key: source[key] for key in allowed if key in source}
    system_root = env.get("SystemRoot") or env.get("WINDIR")
    if not system_root:
        raise RuntimeError("isolated Windows system root unavailable")
    runtime_root = ntpath.dirname(executable)
    env["PATH"] = ";".join((runtime_root, ntpath.join(runtime_root, "Library", "bin"),
                             ntpath.join(system_root, "System32"), system_root))
    env["PYTHONNOUSERSITE"] = "1"
    # 在任何 Converter 导入前关闭文件日志，防止继承进程创建或清理用户日志。
    env["BIGQMT_LOG_ENABLED"] = "0"
    env.update(values, AQP_ISOLATED_REDIS_PORT=str(port))
    return env


def resolved_under(path, root):
    try:
        resolved_path, resolved_root = Path(path).resolve(strict=True), Path(root).resolve(strict=True)
        return resolved_path == resolved_root or resolved_root in resolved_path.parents
    except (OSError, RuntimeError, ValueError):
        return False


def guest(timeout):
    if os.name != "nt":
        raise RuntimeError("guest relay is only for isolated Windows tests")
    stopped, started = threading.Event(), threading.Event()
    output, connections = FramedOutput(sys.stdout), Connections()
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(("127.0.0.1", 0))
    listener.listen(16)
    listener.settimeout(0.2)
    state = {"child": None}

    def start(message):
        if started.is_set() or set(message) != {"op", "argv", "env"}:
            raise RuntimeError("relay test is not repeatable")
        argv, values = message["argv"], message["env"]
        if (not isinstance(argv, list) or not argv or not all(isinstance(item, str) for item in argv)
                or not isinstance(values, dict) or any(not isinstance(key, str) or not key.startswith("AQP_ISOLATED_")
                or not isinstance(value, str) for key, value in values.items())):
            raise RuntimeError("relay test command is invalid")
        converter_root = values.get("AQP_ISOLATED_CONVERTER_ROOT")
        if (not isinstance(converter_root, str) or len(argv) != 3
                or not resolved_under(__file__, converter_root) or not resolved_under(argv[2], converter_root)):
            raise RuntimeError("resolved relay and test driver must belong to the explicit candidate")
        env = isolated_child_environment(values, argv[0], listener.getsockname()[1], os.environ)
        state["child"] = subprocess.Popen(argv, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                           stderr=subprocess.DEVNULL, env=env, close_fds=True,
                                           text=True, bufsize=1)
        threading.Thread(target=relay_evidence, args=(state["child"].stdout, output), daemon=True).start()
        started.set()

    threading.Thread(target=accept_guest, args=(listener, output, connections, stopped), daemon=True).start()
    threading.Thread(target=receive_control, args=(sys.stdin, output, connections, stopped, start), daemon=True).start()
    output.send(dict(op="READY", port=listener.getsockname()[1]))
    deadline = time.monotonic() + timeout
    try:
        while not stopped.is_set() and time.monotonic() < deadline:
            child = state["child"]
            if child is not None and child.poll() is not None:
                output.send(dict(op="DONE", returncode=child.returncode))
                stopped.wait(2)
                return child.returncode
            time.sleep(0.05)
        output.send(dict(op="ERROR", reason="TIMEOUT_OR_PARENT_CLOSED"))
        return 1
    finally:
        stopped.set()
        listener.close()
        connections.close_all()
        child = state["child"]
        if child is not None and child.poll() is None:
            # 仅终止本次创建的测试子进程及其后代，不匹配进程名。
            subprocess.run(["taskkill", "/PID", str(child.pid), "/T", "/F"],
                           stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=5)
            child.wait(timeout=5)


def verify_container(container_id, volume_id, redis_port, password):
    output = subprocess.run(["docker", "inspect", container_id], check=True, stdout=subprocess.PIPE,
                            stderr=subprocess.DEVNULL, timeout=5).stdout
    items = json.loads(output)
    if not isinstance(items, list) or len(items) != 1:
        raise RuntimeError("isolated Redis container identity unavailable")
    item = items[0]
    command = item["Config"].get("Cmd") or []

    def option(name):
        if command.count(name) != 1 or command.index(name) + 1 >= len(command):
            return None
        return command[command.index(name) + 1]

    bindings = item["NetworkSettings"].get("Ports") or {}
    expected_binding = [{"HostIp": "127.0.0.1", "HostPort": str(redis_port)}]
    mounts = item.get("Mounts") or []
    if (item.get("Id") != container_id or item["State"].get("Running") is not True
            or item["Config"].get("Image") != "redis:7" or bindings != {"6379/tcp": expected_binding}
            or option("--save") != "" or option("--appendonly") != "no" or option("--requirepass") != password
            or len(mounts) != 1 or mounts[0].get("Type") != "volume"
            or mounts[0].get("Name") != volume_id or mounts[0].get("Destination") != "/data"):
        raise RuntimeError("isolated Redis container contract mismatch")


def under_windows_root(path, root):
    if not isinstance(path, str) or not isinstance(root, str) or not ntpath.isabs(path) or not ntpath.isabs(root):
        return False
    try:
        normalized_path, normalized_root = ntpath.normpath(path), ntpath.normpath(root)
        return ntpath.normcase(ntpath.commonpath((normalized_path, normalized_root))) == ntpath.normcase(normalized_root)
    except ValueError:
        return False


def prlctl_failure_detail(returncode, stderr):
    # prlctl stderr 不原样显示，避免意外输出子进程环境或认证信息。
    message = stderr.lower()
    categories = (("VM_NOT_RUNNING", ("not running", "is stopped")),
                  ("VM_BUSY", ("already in progress", "virtual machine is busy")),
                  ("AUTHENTICATION_FAILED", ("authentication failed", "permission denied")),
                  ("PRLCTL_JOB_INVALID_ARGUMENT", ("prljob_getresult: invalid argument",)),
                  ("TRANSPORT_UNAVAILABLE", ("unable to connect", "connection failed", "connection lost")))
    category = next((name for name, patterns in categories if any(pattern in message for pattern in patterns)), "UNCLASSIFIED")
    return "prlctl_returncode=%s stderr_category=%s stderr_bytes=%s" % (returncode, category, len(stderr.encode("utf-8")))


def host(args):
    if os.name == "nt" or type(args.redis_port) is not int or not 1 <= args.redis_port <= 65535:
        raise RuntimeError("isolated host Redis port required")
    paths = (args.guest_relay, args.guest_script, args.guest_native_driver, args.guest_proxy_harness)
    other_paths = (args.guest_manager_harness, args.guest_gateway_harness)
    if (not isinstance(args.container_id, str) or len(args.container_id) != 64
            or not isinstance(args.volume_id, str) or len(args.volume_id) != 64
            or not all(under_windows_root(path, args.guest_converter_root) for path in paths)
            or not all(under_windows_root(path, args.guest_aqp_root) for path in other_paths)
            or not isinstance(args.guest_python, str) or not ntpath.isabs(args.guest_python)
            or not isinstance(args.guest_native_python, str) or not ntpath.isabs(args.guest_native_python)):
        raise RuntimeError("explicit absolute candidate paths required")
    password = sys.stdin.readline().rstrip("\r\n")
    if not password:
        raise RuntimeError("isolated Redis password required on stdin")
    verify_container(args.container_id, args.volume_id, args.redis_port, password)
    cmd = ["prlctl", "exec", "Windows 11", "--current-user", args.guest_python, "-u", args.guest_relay,
           "--guest", "--timeout", str(args.timeout)]
    process = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                               text=True, bufsize=1)
    output, connections, stopped = FramedOutput(process.stdin), Connections(), threading.Event()
    lines, diagnostic = [], [""]

    def read_diagnostic():
        for chunk in iter(lambda: process.stderr.read(1024), ""):
            if len(diagnostic[0]) < 8192:
                diagnostic[0] = (diagnostic[0] + chunk)[:8192]

    def reader():
        try:
            for line in process.stdout:
                lines.append(json.loads(line))
        except (OSError, ValueError):
            lines.append(dict(op="ERROR", reason="RELAY_OUTPUT_INVALID"))
        finally:
            stopped.set()

    threading.Thread(target=reader, daemon=True).start()
    diagnostic_thread = threading.Thread(target=read_diagnostic, daemon=True)
    diagnostic_thread.start()
    started = False
    deadline = time.monotonic() + args.timeout
    try:
        while time.monotonic() < deadline:
            if not lines:
                if stopped.is_set() or process.poll() is not None:
                    diagnostic_thread.join(timeout=0.5)
                    raise RuntimeError("guest relay ended before READY: " +
                        prlctl_failure_detail(process.poll(), diagnostic[0]))
                time.sleep(0.01)
                continue
            message = lines.pop(0)
            op, key = message.get("op"), message.get("id")
            if op == "READY" and not started:
                env = dict(AQP_ISOLATED_REDIS_PASSWORD=password, AQP_ISOLATED_REDIS_CONTAINER_ID=args.container_id,
                    AQP_ISOLATED_CONVERTER_ROOT=args.guest_converter_root, AQP_ISOLATED_AQP_ROOT=args.guest_aqp_root,
                    AQP_ISOLATED_NATIVE_DRIVER=args.guest_native_driver, AQP_ISOLATED_NATIVE_EXE=args.guest_native_python,
                    AQP_ISOLATED_MANAGER_SCRIPT=args.guest_manager_harness, AQP_ISOLATED_PROXY_SCRIPT=args.guest_proxy_harness,
                    AQP_ISOLATED_GATEWAY_SCRIPT=args.guest_gateway_harness, AQP_ISOLATED_QMT310_PYTHON=args.guest_python,
                    AQP_ISOLATED_SCENARIO=args.scenario)
                output.send(dict(op="START", argv=[args.guest_python, "-B", args.guest_script], env=env))
                started = True
            elif op == "OPEN" and started:
                if type(key) is not int or key <= 0:
                    raise RuntimeError("guest relay id invalid")
                connection = socket.create_connection(("127.0.0.1", args.redis_port), timeout=2)
                connection.settimeout(0.2)
                connections.add(key, connection)
                threading.Thread(target=forward_reader, args=(connection, key, output, connections, stopped), daemon=True).start()
            elif op == "DATA":
                connection = connections.get(key)
                if connection is not None:
                    payload = base64.b64decode(message["data"], validate=True)
                    if len(payload) > MAX_CHUNK:
                        raise RuntimeError("relay frame exceeds bound")
                    connection.sendall(payload)
            elif op == "CLOSE":
                connections.close(key)
            elif op == "EVIDENCE" and started:
                print(json.dumps(message["value"], sort_keys=True))
            elif op == "DONE" and started:
                if message.get("returncode") != 0:
                    raise RuntimeError("isolated guest test failed")
                print("isolated Windows loopback Redis test passed")
                return 0
            else:
                raise RuntimeError("guest relay protocol failed")
        raise RuntimeError("isolated relay deadline exceeded")
    finally:
        stopped.set()
        connections.close_all()
        try:
            output.send(dict(op="STOP"))
        except (OSError, ValueError, BrokenPipeError):
            pass
        try:
            process.wait(timeout=8)
        except subprocess.TimeoutExpired:
            process.terminate()
            process.wait(timeout=5)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--guest", action="store_true")
    parser.add_argument("--redis-port", type=int)
    parser.add_argument("--container-id")
    parser.add_argument("--volume-id")
    parser.add_argument("--guest-relay")
    parser.add_argument("--guest-script")
    parser.add_argument("--guest-converter-root")
    parser.add_argument("--guest-aqp-root")
    parser.add_argument("--guest-native-driver")
    parser.add_argument("--guest-native-python")
    parser.add_argument("--guest-manager-harness")
    parser.add_argument("--guest-proxy-harness")
    parser.add_argument("--guest-gateway-harness")
    parser.add_argument("--guest-python", default=GUEST_PYTHON)
    parser.add_argument("--scenario", choices=("success", "gap", "two_generations"), default="success")
    parser.add_argument("--timeout", type=int, default=90)
    args = parser.parse_args()
    if not 1 <= args.timeout <= 300:
        raise ValueError("bounded isolated test timeout required")
    return guest(args.timeout) if args.guest else host(args)


if __name__ == "__main__":
    sys.exit(main())
