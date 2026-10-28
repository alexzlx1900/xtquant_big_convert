# coding: utf-8
"""执行双源生成物和既有协议；仅 Win32 区域与原生输入使用 fixture。"""

import ast
import base64
import builtins
from concurrent.futures import ThreadPoolExecutor
import ctypes
import importlib.util
import json
import os
from pathlib import Path
import struct
import subprocess
import sys
import threading
import time
import types
from unittest import mock

import pytest


ROOT = Path(__file__).resolve().parent.parent
SPEC = importlib.util.spec_from_file_location("dual_source_builder_fixture", ROOT / "tools/build_dual_shm_single_file.py")
BUILDER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(BUILDER)
NATIVE_TEST_SPEC = importlib.util.spec_from_file_location("isolated_rotation_native_fixture", ROOT / "tests/windows_isolated_rotation_native.py")
NATIVE_TEST_DRIVER = importlib.util.module_from_spec(NATIVE_TEST_SPEC)
NATIVE_TEST_SPEC.loader.exec_module(NATIVE_TEST_DRIVER)
ACCOUNT = "55013746"
CODE = "000001.SZ"
# 生产保存批次17166的67个非零持仓代码；报价与Win32/QMT均仍是本地fixture。
LIVE_SAVED_POSITION_SYMBOLS_20261020 = """
000929.SZ 001336.SZ 002008.SZ 002012.SZ 002098.SZ 002188.SZ 002247.SZ
002274.SZ 002278.SZ 002316.SZ 002344.SZ 002381.SZ 002409.SZ 002427.SZ
002482.SZ 002494.SZ 002529.SZ 002629.SZ 002652.SZ 002671.SZ 002715.SZ
002760.SZ 002778.SZ 002843.SZ 002861.SZ 003015.SZ 003016.SZ 003032.SZ
300220.SZ 300405.SZ 300417.SZ 300500.SZ 300535.SZ 300605.SZ 300614.SZ
300621.SZ 300635.SZ 301167.SZ 600137.SH 600149.SH 600455.SH 600493.SH
600561.SH 600697.SH 600768.SH 600778.SH 600847.SH 600883.SH 603022.SH
603117.SH 603214.SH 603717.SH 603860.SH 603879.SH 603908.SH 605567.SH
688021.SH 688067.SH 688092.SH 688217.SH 688565.SH 688573.SH 688659.SH
688681.SH 688695.SH 688701.SH 688793.SH
""".split()


class State:
    def __init__(self):
        self.data = bytearray(64 + 8192 + 8 * 1024 * 1024)
        self.lock = threading.Lock()
        self.regions = []


class Region:
    def __init__(self, state):
        self.state, self.pid, self.closed = state, os.getpid(), False
        state.regions.append(self)

    def acquire(self, milliseconds):
        if self.closed:
            raise RuntimeError("fixture region closed")
        return self.state.lock.acquire(False) if not milliseconds else self.state.lock.acquire(timeout=milliseconds / 1000)

    def release(self):
        self.state.lock.release()

    def read(self, offset, length):
        return bytes(self.state.data[offset:offset + length])

    def write(self, offset, value):
        self.state.data[offset:offset + len(value)] = value

    def close(self):
        self.closed = True


class FixtureRuntimeWriteLease:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def read_epoch(self):
        return 1

    def ensure_ready(self):
        return True


class Context:
    def __init__(self, fail_timer=None):
        self.accounts, self.timers, self.unsubscribed = [], [], []
        self.fail_timer, self.callback = fail_timer, None

    def set_account(self, account):
        self.accounts.append(account)

    def run_time(self, name, period, start):
        self.timers.append((name, period, start))
        if name == self.fail_timer:
            raise RuntimeError("fixture second timer failure")

    def get_full_tick(self, codes):
        return {code: dict(lastPrice=11.47, time=1792110000000) for code in codes}

    def subscribe_quote(self, code, **kwargs):
        self.callback = kwargs["callback"]
        return 410

    def unsubscribe_quote(self, handle):
        self.unsubscribed.append(handle)
        return True


def settings(**overrides):
    values = dict(native_pid=os.getpid(), windows_session=1, hard_deadline_unix=time.time() + 600,
        read_name="Local\\AQP_SHM_READONLY_DUAL_FIXTURE", execution_name="Local\\AQP_SHM_TRADE_DUAL_FIXTURE",
        symbols=[CODE], max_seconds=120, max_requests=50)
    return dict(values, **overrides)


@pytest.mark.parametrize("actual,expected", [
    (r"C:\QMT\bin.x64\XtItClient.exe", True),
    (r"C:\QMT-SIM\bin.x64\XtItClient.exe", False),
])
def test_resident_identity_verifies_executable_and_publishes_actual_pid(monkeypatch, capsys, actual, expected):
    h = Fixture(monkeypatch, native_pid=0, native_executable=r"C:\QMT\bin.x64\XtItClient.exe",
        source_identity_path=r"C:\QMT\source.json",
        account_id="8882152350", environment="LIVE", persistent_live_read_write=True,
        max_seconds=0, max_requests=0, hard_deadline_unix=None)

    def module_path(module, buffer, size):
        buffer.value = actual
        return len(actual)

    monkeypatch.setattr(ctypes.windll.kernel32, "GetModuleFileNameW", module_path, raising=False)
    opened = mock.mock_open()
    h.namespace["open"] = opened
    monkeypatch.setattr(os, "replace", mock.Mock())
    if not expected:
        with pytest.raises(RuntimeError, match="native executable mismatch"):
            h.start()
        assert h.context.accounts == []
        return
    h.start()
    ready = next(line for line in capsys.readouterr().out.splitlines() if line.startswith("AQP_DUAL_SHM_READY "))
    identity = json.loads(ready.split(" ", 1)[1])
    assert identity["native_pid"] == os.getpid()
    assert identity["read_name"].startswith(h.settings["read_name"] + "_")
    assert identity["execution_name"].startswith(h.settings["execution_name"] + "_")
    published = json.loads("".join(call.args[0] for call in opened().write.call_args_list))
    assert published == dict(identity, status="READY")
    os.replace.assert_called_once_with(r"C:\QMT\source.json.new", r"C:\QMT\source.json")
    h.close()


@pytest.mark.parametrize("overrides", [
    dict(native_pid=0), dict(native_pid=0, native_executable="XtItClient.exe"),
    dict(native_pid=0, native_executable=r"C:\QMT\python.exe"),
    dict(native_executable=r"C:\QMT\XtItClient.exe"),
])
def test_resident_mode_never_implicitly_weakens_pid_binding(overrides):
    with pytest.raises(ValueError):
        BUILDER.build_source(**settings(**overrides))


@pytest.mark.parametrize("isolate,account", [(True, "8882152350"), (False, "8765432100")])
def test_resident_model_launches_only_explicit_runtime_after_verified_sources(monkeypatch, isolate, account):
    launcher = dict(python=r"C:\Python\python.exe", script=r"C:\AQP\supervise_qmt_runtime.py", config=r"C:\AQP\runtime.json")
    h = Fixture(monkeypatch, native_pid=0, native_executable=r"C:\QMT\XtItClient.exe",
        source_identity_path=r"C:\AQP\source.json", runtime_launcher=launcher,
        account_id=account, environment="LIVE", persistent_live_read_write=True,
        max_seconds=0, max_requests=0, hard_deadline_unix=None, read_fault_isolation=isolate)

    def module_path(module, buffer, size):
        buffer.value = r"C:\QMT\XtItClient.exe"
        return len(buffer.value)

    monkeypatch.setattr(ctypes.windll.kernel32, "GetModuleFileNameW", module_path, raising=False)
    h.namespace["open"] = mock.mock_open()
    monkeypatch.setattr(os, "replace", mock.Mock())
    launched = mock.Mock()
    monkeypatch.setattr(subprocess, "Popen", launched)
    h.start()
    assert all(runtime["_SERVER"] is not None for runtime in h.namespace["_RUNTIMES"].values())
    args = [launcher["python"], launcher["script"], "--config", launcher["config"]]
    if isolate:
        launched.assert_called_once_with(args, close_fds=True, creationflags=0x208)
        assert h.namespace["_SUPERVISOR_PROCESS"] is launched.return_value
    else:
        launched.assert_called_once_with(args, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                                         stderr=subprocess.DEVNULL, close_fds=False, creationflags=0x208)
        assert h.namespace["_SUPERVISOR_PROCESS"] is None
    assert h.namespace["_SUPERVISOR_IDENTITY"] is None
    h.close()


def test_supervisor_identity_uses_retained_handle_and_exact_os_creation_time(monkeypatch):
    h = Fixture(monkeypatch)
    process = types.SimpleNamespace(pid=4321, _handle=987, poll=lambda: None)

    def get_times(handle, created, exited, kernel_time, user_time):
        assert handle.value == 987
        created._obj.low = 123
        created._obj.high = 7
        return True

    def get_session(pid, pointer):
        assert pid == 4321
        pointer._obj.value = 1
        return True

    kernel = types.SimpleNamespace(GetProcessId=mock.Mock(return_value=4321),
        GetProcessTimes=mock.Mock(side_effect=get_times), ProcessIdToSessionId=mock.Mock(side_effect=get_session))
    identity = h.namespace["_supervisor_identity"](process, kernel)
    assert identity == dict(pid=4321, creation_filetime=(7 << 32) | 123, windows_session_id=1)
    kernel.GetProcessId.return_value = 4322
    assert h.namespace["_supervisor_identity"](process, kernel) is None
    kernel.GetProcessId.return_value = 4321
    process.poll = lambda: 0
    assert h.namespace["_supervisor_identity"](process, kernel) is None


def sim_resident_settings(**overrides):
    values = dict(native_pid=0, native_executable=BUILDER.SIM_NATIVE_EXECUTABLE,
        source_identity_path=r"C:\Users\zhanglongxiang\AppData\Local\AQP\qmt-sim\source.identity.json",
        read_name=r"Local\AQP_SHM_READONLY_VM_SIM_RESIDENT",
        execution_name=r"Local\AQP_SHM_TRADE_VM_SIM_RESIDENT",
        persistent_readonly=True, max_seconds=0, max_requests=0, hard_deadline_unix=None)
    return dict(values, **overrides)


def test_sim_resident_same_source_rebinds_actual_pid_and_publishes_new_private_identity(monkeypatch, capsys):
    identities, sources = [], []
    for pid in (2988, 23572):
        with mock.patch.object(os, "getpid", return_value=pid):
            h = Fixture(monkeypatch, **sim_resident_settings())

            def module_path(module, buffer, size):
                buffer.value = BUILDER.SIM_NATIVE_EXECUTABLE
                return len(buffer.value)

            monkeypatch.setattr(ctypes.windll.kernel32, "GetModuleFileNameW", module_path, raising=False)
            opened = mock.mock_open()
            h.namespace["open"] = opened
            replaced, launched = mock.Mock(), mock.Mock()
            monkeypatch.setattr(os, "replace", replaced)
            monkeypatch.setattr(subprocess, "Popen", launched)
            h.start()
            identity = json.loads(next(line.split(" ", 1)[1] for line in capsys.readouterr().out.splitlines()
                if line.startswith("AQP_DUAL_SHM_READY ")))
            assert identity["native_pid"] == pid and identity["windows_session"] == 1
            assert identity["account_id"] == ACCOUNT and identity["environment"] == "SIM"
            assert identity["runtime_policy"] == "PERSISTENT_READONLY"
            assert identity["allow_order_methods"] is False and identity["market_rpc_enabled"] is False
            assert identity["account_bound_once"] is True and identity["account_callback_route"] == "outer_to_execution"
            assert identity["execution_bind_account_callbacks"] is False
            assert identity["read_name"].startswith(h.settings["read_name"] + "_")
            assert identity["execution_name"].startswith(h.settings["execution_name"] + "_")
            assert len(identity["read_name"]) == len(h.settings["read_name"]) + 17
            assert len(identity["execution_name"]) == len(h.settings["execution_name"]) + 17
            assert identity["source_deadline_unix"] is None
            assert identity["max_seconds"] == identity["max_requests_per_source"] == 0
            published = json.loads("".join(call.args[0] for call in opened().write.call_args_list))
            assert published == dict(identity, status="READY")
            path = h.settings["source_identity_path"]
            replaced.assert_called_once_with(path + ".new", path)
            launched.assert_not_called()
            assert h.context.accounts == [ACCOUNT]
            for key in ("read", "execution"):
                ping = h.call(key, "ping")["data"]
                assert ping["native_pid"] == pid and ping["allow_order_methods"] is False
                for method in ("submit_order", "cancel_order"):
                    assert h.call(key, method)["ok"] is False
            sources.append(h.source)
            identities.append(identity)
            h.close()
    assert sources[0] == sources[1]
    assert len({value[field] for value in identities for field in ("read_session_id", "execution_session_id")}) == 4
    for field in ("read_name", "execution_name"):
        assert identities[0][field] != identities[1][field]


@pytest.mark.parametrize("overrides", [
    dict(account_id="8882152350"),
    dict(environment="LIVE"),
    dict(native_executable=r"C:\国金证券QMT交易端\bin.x64\XtItClient.exe"),
    dict(native_executable=r"C:\Other-SIM\bin.x64\XtItClient.exe"),
    dict(persistent_readonly=False),
    dict(persistent_simulation_read_write=True),
    dict(persistent_live_read_write=True),
    dict(enable_market_rpc=True),
    dict(runtime_launcher=dict(python=r"C:\Python\python.exe", script=r"C:\AQP\supervise_qmt_runtime.py", config=r"C:\AQP\runtime.json")),
    dict(source_identity_path=None),
])
def test_sim_resident_builder_rejects_any_other_account_executable_or_policy(overrides):
    with pytest.raises(ValueError):
        BUILDER.build_source(**settings(**sim_resident_settings(**overrides)))


@pytest.mark.parametrize("field,value", [
    ("account_id", "8882152350"),
    ("environment", "LIVE"),
    ("persistent_readonly", False),
    ("persistent_simulation_read_write", True),
    ("persistent_live_read_write", True),
    ("market_rpc_enabled", True),
    ("runtime_launcher", dict(python=r"C:\Python\python.exe", script=r"C:\AQP\supervise_qmt_runtime.py", config=r"C:\AQP\runtime.json")),
    ("native_executable", r"C:\国金证券QMT交易端\bin.x64\XtItClient.exe"),
    ("source_identity_path", "source.json"),
    ("windows_session", 2),
    ("windows_session", True),
])
def test_sim_resident_rejects_changed_identity_before_account_binding(monkeypatch, field, value):
    h = Fixture(monkeypatch, **sim_resident_settings())

    def module_path(module, buffer, size):
        buffer.value = BUILDER.SIM_NATIVE_EXECUTABLE
        return len(buffer.value)

    monkeypatch.setattr(ctypes.windll.kernel32, "GetModuleFileNameW", module_path, raising=False)
    h.namespace["DUAL_SHM_SETTINGS"][field] = value
    with pytest.raises(RuntimeError):
        h.start()
    assert h.context.accounts == [] and h.query_calls == []
    assert all(runtime["_SERVER"] is None for runtime in h.namespace["_RUNTIMES"].values())
    h.writes.assert_not_called()


def test_sim_resident_wrong_actual_executable_rejected_before_account_binding(monkeypatch):
    h = Fixture(monkeypatch, **sim_resident_settings())

    def module_path(module, buffer, size):
        buffer.value = r"C:\国金证券QMT交易端\bin.x64\XtItClient.exe"
        return len(buffer.value)

    monkeypatch.setattr(ctypes.windll.kernel32, "GetModuleFileNameW", module_path, raising=False)
    with pytest.raises(RuntimeError, match="native executable mismatch"):
        h.start()
    assert h.context.accounts == [] and h.query_calls == []
    assert all(runtime["_SERVER"] is None for runtime in h.namespace["_RUNTIMES"].values())


def test_sim_resident_rw_same_source_rebinds_pid_and_keeps_native_write_contract(monkeypatch, capsys):
    identities, sources = [], []
    for pid in (2988, 23572):
        with mock.patch.object(os, "getpid", return_value=pid):
            h = Fixture(monkeypatch, **sim_resident_settings(
                persistent_readonly=False, persistent_simulation_read_write=True))
            _, submit, cancel = _enable_fixture_simulation_orders(h)

            def module_path(module, buffer, size):
                buffer.value = BUILDER.SIM_NATIVE_EXECUTABLE
                return len(buffer.value)

            monkeypatch.setattr(ctypes.windll.kernel32, "GetModuleFileNameW", module_path, raising=False)
            h.namespace["open"] = mock.mock_open()
            replaced, launched = mock.Mock(), mock.Mock()
            monkeypatch.setattr(os, "replace", replaced)
            monkeypatch.setattr(subprocess, "Popen", launched)
            h.start()
            identity = json.loads(next(line.split(" ", 1)[1] for line in capsys.readouterr().out.splitlines()
                if line.startswith("AQP_DUAL_SHM_READY ")))
            assert identity["native_pid"] == pid and identity["windows_session"] == 1
            assert identity["account_id"] == ACCOUNT and identity["environment"] == "SIM"
            assert identity["runtime_policy"] == "PERSISTENT_SIMULATION_READ_WRITE"
            assert identity["allow_order_methods"] is True and identity["market_rpc_enabled"] is False
            assert identity["account_bound_once"] is True and identity["account_callback_route"] == "outer_to_execution"
            assert identity["execution_bind_account_callbacks"] is False
            assert {"submit_order", "submit_order_async", "cancel_order"} <= set(identity["execution_capabilities"])
            assert identity["async_submit_contract"] == "NATIVE_CALL_RETURNED_V1"
            assert identity["read_name"].startswith(h.settings["read_name"] + "_")
            assert identity["execution_name"].startswith(h.settings["execution_name"] + "_")
            path = h.settings["source_identity_path"]
            replaced.assert_called_once_with(path + ".new", path)
            launched.assert_not_called()
            assert h.context.accounts == [ACCOUNT]
            read_ping, execution_ping = h.call("read", "ping")["data"], h.call("execution", "ping")["data"]
            assert read_ping["allow_order_methods"] is False and execution_ping["allow_order_methods"] is True
            assert execution_ping["native_pid"] == pid
            assert execution_ping["session_id"] == identity["execution_session_id"]
            for method in ("submit_order", "submit_order_async", "cancel_order"):
                assert h.call("read", method)["ok"] is False
            params = dict(stock_code=CODE, action="BUY", volume=100, price=10.5, remark="resident-sim-order")
            blocked = h.call("execution", "submit_order", params)
            assert blocked["data"]["status"] == "BLOCKED"
            submit.assert_not_called()
            _fixture_native_login(h)
            submitted = h.call("execution", "submit_order", params, request_id="resident-sim-submit")
            assert submitted["data"]["status"] == "CONFIRMED"
            assert h.call("execution", "submit_order", params, request_id="resident-sim-submit")["data"] == submitted["data"]
            assert h.call("execution", "submit_order", dict(params, volume=200), request_id="resident-sim-submit")["ok"] is False
            assert submit.call_count == 1
            h.namespace["_RUNTIMES"]["execution"]["_SERVER"].bridge.handlers.order_settle_timeout_seconds = 0.005
            refs = dict(order_sys_id="SIM-FIXTURE-001", user_order_id="resident-sim-order")
            cancelled = h.call("execution", "cancel_order", refs, request_id="resident-sim-cancel")
            assert cancelled["data"]["status"] == "UNKNOWN" and cancelled["data"]["automatic_retry"] is False
            assert h.call("execution", "cancel_order", refs, request_id="resident-sim-cancel")["data"] == cancelled["data"]
            cancel.assert_called_once_with("SIM-FIXTURE-001", ACCOUNT, "STOCK", h.context)
            sources.append(h.source)
            identities.append(identity)
            h.close()
    assert sources[0] == sources[1]
    assert len({value[field] for value in identities for field in ("read_session_id", "execution_session_id")}) == 4


@pytest.mark.parametrize("field,value", [
    ("account_id", "8882152350"), ("environment", "LIVE"),
    ("persistent_readonly", True), ("persistent_simulation_read_write", False),
    ("persistent_simulation_read_write", 1), ("persistent_live_read_write", True),
    ("market_rpc_enabled", True), ("runtime_launcher", {}),
    ("native_executable", r"C:\国金证券QMT交易端\bin.x64\XtItClient.exe"),
    ("source_identity_path", "source.json"), ("windows_session", 2),
])
def test_sim_resident_rw_rejects_changed_identity_before_native_calls(monkeypatch, field, value):
    h = Fixture(monkeypatch, **sim_resident_settings(
        persistent_readonly=False, persistent_simulation_read_write=True))

    def module_path(module, buffer, size):
        buffer.value = BUILDER.SIM_NATIVE_EXECUTABLE
        return len(buffer.value)

    monkeypatch.setattr(ctypes.windll.kernel32, "GetModuleFileNameW", module_path, raising=False)
    h.namespace["DUAL_SHM_SETTINGS"][field] = value
    with pytest.raises(RuntimeError):
        h.start()
    assert h.context.accounts == [] and h.query_calls == []
    assert all(runtime["_SERVER"] is None for runtime in h.namespace["_RUNTIMES"].values())
    h.writes.assert_not_called()


class Fixture:
    def __init__(self, monkeypatch, **overrides):
        self.settings = settings(**overrides)
        self.account = self.settings.get("account_id", ACCOUNT)
        self.source = BUILDER.build_source(**self.settings)
        self.namespace = {}
        original_import = builtins.__import__

        def imports(name, *args, **kwargs):
            if name.split(".")[0] in ("redis", "socket", "zmq", "xtquant"):
                raise AssertionError("forbidden native dependency: " + name)
            return original_import(name, *args, **kwargs)

        with mock.patch("builtins.__import__", side_effect=imports):
            exec(compile(self.source, "<actual-dual-source>", "exec"), self.namespace)
        self.context = Context()
        self.query_calls, self.states, self.transports, self.clients = [], {}, {}, {}
        self.fixture_guards = {}
        self.namespace["get_trade_detail_data"] = self.query
        self.writes = mock.Mock(side_effect=AssertionError("native write forbidden"))
        self.namespace.update(passorder=self.writes, cancel=self.writes)

        def session(pid, pointer):
            pointer._obj.value = 1
            return True

        monkeypatch.setattr(ctypes, "windll", types.SimpleNamespace(kernel32=types.SimpleNamespace(ProcessIdToSessionId=session)), raising=False)
        for key, transport_name in (("read", "aqp_readonly_transport"), ("execution", "bigqmt_signal_trader.transports.readonly_shm")):
            runtime = self.namespace["_RUNTIMES"][key]
            transport = runtime["_load_embedded"](transport_name)
            state = State()
            self.states[key], self.transports[key] = state, transport
            transport.Win32Region = lambda *args, state=state, **kwargs: Region(state)

    def query(self, account, account_type, kind, strategy_name=""):
        self.query_calls.append((account, account_type, kind, strategy_name))
        assert (account, account_type) == (self.account, "STOCK")
        if kind == "POSITION":
            return [types.SimpleNamespace(m_strInstrumentID="000001", m_strExchangeID="SZ", m_nVolume=100,
                m_nCanUseVolume=100, m_dOpenPrice=10.0, m_dMarketValue=1147.0, m_dLastPrice=11.47)]
        if kind == "ACCOUNT":
            row = types.SimpleNamespace(m_strAccountID=self.account, m_strStatus="登录成功", m_dAvailable=1000.0, m_dBalance=2147.0,
                m_dFrozenCash=0.0, m_dInstrumentValue=1147.0)
            if self.settings.get("environment") == "LIVE":
                row.m_nBrokerType, row.m_strAccountKey = 2, "LIVE-STOCK-FIXTURE-ONLY"
            return [row]
        return []

    def start(self, enable_fixture_write_gate=True):
        assert self.namespace["init"](self.context) is None
        bridge = self.namespace["_RUNTIMES"]["execution"]["_SERVER"].bridge
        if bridge.execution_event_guard_required and enable_fixture_write_gate:
            # 仅验证游标/回调模型；真实生成源不会凭此测试注入声明运行门已就绪。
            bridge.runtime_metadata.update(native_write_runtime_guard_ready=True,
                                           native_write_runtime_guard_attached=True,
                                           native_callback_threading_verified=True,
                                           supervisor_write_gate_contract="INHERITED_SUPERVISOR_GATE_V1",
                                           supervisor_gate_epoch_contract="SUPERVISOR_GATE_EPOCH_V1")
            bridge.native_write_runtime_guard = FixtureRuntimeWriteLease
        for key, transport in self.transports.items():
            name = self.namespace["_RUNTIMES"][key]["READONLY_SHM_NAME" if key == "read" else "SHM_RPC_NAME"]
            self.clients[key] = transport.ShmReadOnlyClient(name, self.account)

    def call(self, key, method, params=None, **overrides):
        fixture_guard = overrides.pop("_fixture_guard", True)
        request = dict(schema_version=1, request_id="dual-%s-%s" % (key, time.monotonic_ns()),
            account_id=self.account, method=method, params=dict(params or {}))
        if self.settings.get("environment") == "LIVE":
            request["environment"] = "LIVE"
        request.update(overrides)
        if request.get("environment") is None:
            request.pop("environment", None)
        if (key == "execution" and method in ("submit_order", "submit_order_async", "cancel_order") and fixture_guard
                and self.namespace["_RUNTIMES"]["execution"]["_SERVER"].bridge.execution_event_guard_required):
            bridge = self.namespace["_RUNTIMES"]["execution"]["_SERVER"].bridge
            guard = self.fixture_guards.get(request["request_id"])
            if guard is None:
                cursor = bridge._event_sequence
                if not bridge._event_cursor_observed or bridge._event_observed_cursor < cursor:
                    self.call("execution", "poll_execution_events", {"after_sequence": cursor})
                guard = dict(contract="EXECUTION_CURSOR_WRITE_GATE_V1", execution_session_id=bridge.mailbox.session_id,
                             native_pid=bridge.mailbox.server_pid, sequence=cursor)
                self.fixture_guards[request["request_id"]] = guard
            request["params"].setdefault("execution_event_guard", guard)
            request["params"].setdefault("supervisor_gate_epoch", dict(contract="SUPERVISOR_GATE_EPOCH_V1", epoch=1))
        callback = self.namespace["aqp_shm_readonly_adjust" if key == "read" else "aqp_shm_rpc_adjust"]
        with ThreadPoolExecutor(1) as pool:
            future = pool.submit(self.clients[key].send_request, request, 1.0)
            until = time.monotonic() + 2
            while not future.done() and time.monotonic() < until:
                callback(self.context)
                time.sleep(0.001)
            return future.result(timeout=0.2)

    def close(self):
        for client in self.clients.values():
            client.close()
        self.namespace["stop"](self.context)
        self.namespace["stop"](self.context)
        assert all(region.closed for state in self.states.values() for region in state.regions)
        self.writes.assert_not_called()


def test_actual_generated_two_protocols_readonly_identity_caps_and_isolation(monkeypatch, capsys):
    foreign = types.ModuleType("bigqmt_signal_trader.models")
    monkeypatch.setitem(sys.modules, foreign.__name__, foreign)
    original_switch = sys.getswitchinterval()
    h = Fixture(monkeypatch)
    h.start()
    assert h.context.accounts == [ACCOUNT]
    assert [timer[:2] for timer in h.context.timers] == [("aqp_shm_readonly_adjust", "100nMilliSecond"), ("aqp_shm_rpc_adjust", "100nMilliSecond")]
    read, execution = h.namespace["_RUNTIMES"]["read"], h.namespace["_RUNTIMES"]["execution"]
    assert read["_MODULE_CACHE"] is not execution["_MODULE_CACHE"]
    assert read["_load_embedded"]("bigqmt_signal_trader.models") is not foreign
    assert sys.modules[foreign.__name__] is foreign and sys.getswitchinterval() == original_switch
    ready_line = next(line for line in capsys.readouterr().out.splitlines() if line.startswith("AQP_DUAL_SHM_READY "))
    ready = json.loads(ready_line.split(" ", 1)[1])
    assert ready["account_id"] == ACCOUNT and ready["native_pid"] == os.getpid()
    assert ready["read_session_id"] != ready["execution_session_id"] and ready["market_rpc_enabled"] is False
    assert ready["max_requests_per_source"] == 50
    assert "subscribe_quote" not in ready["read_capabilities"]
    assert not {"submit_order", "cancel_order", "submit_order_async"} & set(ready["execution_capabilities"])
    for key in ("read", "execution"):
        ping = h.call(key, "ping")
        assert ping["ok"] and ping["data"]["allow_order_methods"] is False
        assert ping["data"]["session_id"] == ready["read_session_id" if key == "read" else "execution_session_id"]
        assert h.call(key, "get_positions")["data"][CODE]["volume"] == 100
        for method in ("submit_order", "cancel_order", "submit_order_async"):
            assert h.call(key, method)["ok"] is False
    assert h.call("read", "get_full_tick", {"codes": [CODE]})["data"][CODE]["lastPrice"] == 11.47
    assert h.call("execution", "get_asset")["data"]["source_evidence"]["complete"] is True
    with pytest.raises(RuntimeError, match="only be attempted once"):
        h.namespace["init"](h.context)
    h.close()


def test_sid_mismatch_rejects_before_business_and_new_source_gets_new_sid(monkeypatch):
    h = Fixture(monkeypatch)
    h.start()
    old_sids = [h.clients[key].mailbox.session_id for key in ("read", "execution")]
    with pytest.raises(Exception, match="session_id mismatch"):
        h.call("read", "get_positions", session_id="foreign-sid")
    assert not h.query_calls
    h.close()
    second = Fixture(monkeypatch)
    second.start()
    new_sids = [second.clients[key].mailbox.session_id for key in ("read", "execution")]
    assert len(set(old_sids + new_sids)) == 4
    second.close()


def test_partial_second_timer_failure_releases_both_mapping_owners(monkeypatch):
    h = Fixture(monkeypatch)
    h.context.fail_timer = "aqp_shm_rpc_adjust"
    with pytest.raises(RuntimeError, match="second timer failure"):
        h.namespace["init"](h.context)
    assert h.namespace["_CLOSED"]
    assert all(region.closed for state in h.states.values() for region in state.regions)
    assert h.context.accounts == [ACCOUNT]
    h.namespace["stop"](h.context)
    h.writes.assert_not_called()


def test_explicit_market_subscription_is_cancelled_on_owner_stop(monkeypatch):
    h = Fixture(monkeypatch, enable_market_rpc=True)
    h.start()
    subscribed = h.call("read", "subscribe_quote", {"code": CODE})
    assert subscribed["ok"] and subscribed["data"]["native_handle"] == 410
    h.close()
    assert h.context.unsubscribed == [410]
    h.context.callback({CODE: dict(lastPrice=11.47, time=1792110000000)})
    assert h.namespace["_RUNTIMES"]["read"]["_SERVER"].events == []


def test_absolute_deadline_closes_both_sources(monkeypatch):
    h = Fixture(monkeypatch)
    h.start()
    h.namespace["_SOURCE_DEADLINE"] = time.time() - 1
    h.namespace["aqp_shm_readonly_adjust"](h.context)
    assert h.namespace["_CLOSED"]
    h.close()


def test_read_failure_preserves_execution_sid_and_callbacks(monkeypatch, capsys):
    h = Fixture(monkeypatch, **persistent_settings())
    h.start()
    read = h.namespace["_RUNTIMES"]["read"]["_SERVER"]
    execution = h.namespace["_RUNTIMES"]["execution"]["_SERVER"]
    execution_sid = execution.bridge.mailbox.session_id
    read_ping = h.call("read", "ping")["data"]
    assert read_ping["read_fault_isolation_contract"] == "READ_FAILURE_ISOLATION_V1"
    initial_ping = h.call("execution", "ping")["data"]
    assert initial_ping["read_fault_isolation_contract"] == "READ_FAILURE_ISOLATION_V1"
    assert initial_ping["read_rotation_authorization_ready"] is False
    assert initial_ping["native_read_session_id"] == read.mailbox.session_id
    assert initial_ping["native_read_source_state"] == "UNKNOWN"
    order = dict(m_strAccountID=ACCOUNT, m_strOrderSysID="fixture-order", m_nOrderStatus=50)
    trade = dict(m_strAccountID=ACCOUNT, m_strOrderSysID="fixture-order", m_strTradeID="fixture-trade")
    h.namespace["account_callback"](h.context, dict(m_strAccountID=ACCOUNT, m_strStatus="登录成功", m_nStatus=0))
    h.namespace["order_callback"](h.context, order)
    with mock.patch.object(read.mailbox, "read_request", side_effect=RuntimeError("fixture read failure")):
        h.namespace["aqp_shm_readonly_adjust"](h.context)
    assert read.failed and not read.closed and h.namespace["_READ_FAULT"]["status"] == "FAILED"
    assert not execution.closed and not h.namespace["_CLOSED"]
    assert execution.bridge.mailbox.session_id == execution_sid
    assert h.context.accounts == [ACCOUNT]
    h.namespace["deal_callback"](h.context, trade)
    result = h.call("execution", "poll_execution_events", {"after_sequence": 0})["data"]
    assert result["session_id"] == execution_sid and result["gap"] is False
    assert [(entry["sequence"], entry["event"]["event_type"]) for entry in result["events"]] == [
        (1, "order"), (2, "trade")]
    assert execution.trade_login.event is not None
    failure_ping = h.call("execution", "ping")["data"]
    assert failure_ping["session_id"] == execution_sid
    assert failure_ping["native_read_session_id"] == read.mailbox.session_id
    assert failure_ping["native_read_source_state"] == "RESULT_UNKNOWN"
    assert "AQP_DUAL_SHM_READONLY_FAILED" in capsys.readouterr().out
    h.close()


def test_read_failure_preserves_unconfirmed_frame_and_quote_callback(monkeypatch):
    h = Fixture(monkeypatch, **read_write_settings(enable_market_rpc=True))
    h.start()
    read = h.namespace["_RUNTIMES"]["read"]["_SERVER"]
    subscription = h.call("read", "subscribe_quote", {"code": CODE})["data"]
    h.context.callback({CODE: dict(lastPrice=11.47, time=1792110000000)})
    client = h.clients["read"]
    request = dict(schema_version=1, request_id="late-read", account_id=ACCOUNT,
                   method="get_positions", params={})
    with pytest.raises(h.transports["read"].ShmTimeoutError):
        client.send_request(request, 0.01)
    h.namespace["aqp_shm_readonly_adjust"](h.context)
    header = read.mailbox._header()
    assert header[4:7] == [2, 2, 1]
    before = bytes(h.states["read"].data)
    assert h.call("execution", "ping")["data"]["read_rotation_authorization_ready"] is False
    with mock.patch.object(read.mailbox, "read_request", side_effect=RuntimeError("fixture mailbox failure")) as poll:
        h.namespace["aqp_shm_readonly_adjust"](h.context)
        h.namespace["aqp_shm_readonly_adjust"](h.context)
        poll.assert_called_once()
    assert read.failed and not read.closed and not read.mailbox.region.closed
    assert bytes(h.states["read"].data) == before
    assert h.context.unsubscribed == []
    h.context.callback({CODE: dict(lastPrice=11.48, time=1792110000001)})
    assert [event["sequence"] for event in read.events] == [1, 2]
    assert all(event["subscription_id"] == subscription["subscription_id"] for event in read.events)
    assert h.call("execution", "ping")["ok"] is True
    h.close()


def test_rotation_evidence_requires_returned_expired_matching_old_request(monkeypatch):
    h = Fixture(monkeypatch, **persistent_settings())
    h.namespace["_native_creation_filetime"] = lambda: 134567890123456789
    h.start()
    read = h.namespace["_RUNTIMES"]["read"]["_SERVER"]
    execution = h.namespace["_RUNTIMES"]["execution"]["_SERVER"]
    request = dict(schema_version=1, request_id="rotation-old-rid", account_id=ACCOUNT,
                   method="get_positions", params={})
    with pytest.raises(h.transports["read"].ShmTimeoutError):
        h.clients["read"].send_request(request, 0.01)
    assert h.call("execution", "ping")["data"]["read_rotation_evidence"] is None
    h.namespace["aqp_shm_readonly_adjust"](h.context)
    before = bytes(h.states["read"].data)
    evidence = h.call("execution", "ping")["data"]["read_rotation_evidence"]
    assert evidence["contract"] == "READ_ROTATION_EVIDENCE_V1" and evidence["status"] == "ELIGIBLE"
    assert evidence["account_id"] == ACCOUNT and evidence["environment"] == "SIM"
    assert evidence["native_pid"] == os.getpid() and evidence["native_creation_filetime"] == 134567890123456789
    assert evidence["windows_session_id"] == 1 and evidence["read_session_id"] == read.mailbox.session_id
    assert evidence["execution_session_id"] == execution.bridge.mailbox.session_id
    assert evidence["request_id"] == "rotation-old-rid" and evidence["method"] == "get_positions"
    assert (evidence["request_sequence"], evidence["response_sequence"], evidence["confirmed_sequence"]) == (1, 1, 0)
    assert evidence["confirmed_sequence"] == evidence["sequence"] - 1
    assert evidence["read_expires_at_unix"] <= evidence["native_started_at_unix"] <= evidence["native_finished_at_unix"]
    assert evidence["automatic_retry"] is False
    assert bytes(h.states["read"].data) == before and not h.query_calls
    header = read.mailbox._header()
    transport = h.transports["read"]
    original_request = read.mailbox.region.read(transport.REQUEST_OFFSET, header[7])
    changed_request = original_request.replace(b'"params":{}', b'"params":[]')
    assert changed_request != original_request and len(changed_request) == len(original_request)
    read.mailbox.region.write(transport.REQUEST_OFFSET, changed_request)
    assert h.call("execution", "ping")["data"]["read_rotation_evidence"] is None
    read.mailbox.region.write(transport.REQUEST_OFFSET, original_request)
    read.last_completed_request["request_id"] = "different-rid"
    assert h.call("execution", "ping")["data"]["read_rotation_evidence"] is None
    assert bytes(h.states["read"].data) == before
    h.close()


def test_rotation_evidence_rejects_missing_creation_time_and_subscription(monkeypatch):
    h = Fixture(monkeypatch, **read_write_settings(enable_market_rpc=True))
    h.start()
    request = dict(schema_version=1, request_id="no-creation", account_id=ACCOUNT,
                   method="get_positions", params={})
    with pytest.raises(h.transports["read"].ShmTimeoutError):
        h.clients["read"].send_request(request, 0.01)
    h.namespace["aqp_shm_readonly_adjust"](h.context)
    assert h.call("execution", "ping")["data"]["read_rotation_evidence"] is None
    h.namespace["_BOUND_NATIVE_CREATION_FILETIME"] = 123
    h.namespace["_native_creation_filetime"] = lambda: 123
    read = h.namespace["_RUNTIMES"]["read"]["_SERVER"]
    assert h.call("execution", "ping")["data"]["read_rotation_evidence"]["status"] == "ELIGIBLE"
    read.subscription = {"active": True}
    assert h.call("execution", "ping")["data"]["read_rotation_evidence"] is None
    read.subscription = None
    h.close()


@pytest.mark.parametrize("loss", ["recorded_gap", "off_thread_callback"])
def test_rotation_evidence_rejects_known_execution_event_loss(monkeypatch, loss):
    h = Fixture(monkeypatch, **read_write_settings())
    h.namespace["_native_creation_filetime"] = lambda: 134567890123456789
    h.start()
    read = h.namespace["_RUNTIMES"]["read"]["_SERVER"]
    execution = h.namespace["_RUNTIMES"]["execution"]["_SERVER"]
    request = dict(schema_version=1, request_id="old-read-before-gap", account_id=ACCOUNT,
                   method="get_positions", params={})
    with pytest.raises(h.transports["read"].ShmTimeoutError):
        h.clients["read"].send_request(request, 0.01)
    h.namespace["aqp_shm_readonly_adjust"](h.context)
    old_frame = bytes(h.states["read"].data)
    execution_sid = execution.bridge.mailbox.session_id
    assert h.namespace["_read_rotation_evidence"]()["request_id"] == "old-read-before-gap"
    if loss == "recorded_gap":
        execution.bridge.record_execution_event_loss("order", "fixture callback lost")
    else:
        with ThreadPoolExecutor(1) as pool:
            pool.submit(h.namespace["deal_callback"], h.context, {}).result(timeout=1)
    assert h.namespace["_read_rotation_evidence"]() is None
    assert bytes(h.states["read"].data) == old_frame and read.mailbox._header()[4:7] == [1, 1, 0]
    assert execution.bridge.mailbox.session_id == execution_sid
    h.close()


def test_rotation_evidence_keeps_a_native_result_that_arrived_after_timeout(monkeypatch):
    h = Fixture(monkeypatch, **persistent_settings())
    h.namespace["_native_creation_filetime"] = lambda: 134567890123456789
    h.start()
    read = h.namespace["_RUNTIMES"]["read"]["_SERVER"]
    ordinary_quote = h.context.get_full_tick

    def slow_quote(codes):
        time.sleep(0.04)
        return ordinary_quote(codes)

    h.context.get_full_tick = mock.Mock(side_effect=slow_quote)
    request = dict(schema_version=1, request_id="late-native", account_id=ACCOUNT,
                   method="get_full_tick", params={"codes": [CODE]})
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(h.clients["read"].send_request, request, 0.02)
        until = time.monotonic() + 1
        while read.mailbox._header()[4] != 1 and time.monotonic() < until:
            time.sleep(0.001)
        assert read.mailbox._header()[4] == 1
        h.namespace["aqp_shm_readonly_adjust"](h.context)
        with pytest.raises(h.transports["read"].ShmTimeoutError):
            future.result(timeout=0.2)
    before = bytes(h.states["read"].data)
    evidence = h.call("execution", "ping")["data"]["read_rotation_evidence"]
    assert evidence["request_id"] == "late-native" and evidence["method"] == "get_full_tick"
    assert evidence["native_started_at_unix"] < evidence["read_expires_at_unix"] < evidence["native_finished_at_unix"]
    assert read.mailbox._header()[4:7] == [1, 1, 0]
    assert bytes(h.states["read"].data) == before
    h.context.get_full_tick.assert_called_once_with([CODE])
    h.close()


def test_isolated_gap_injection_requires_callback_completion_and_preserves_old_frame(monkeypatch):
    h = Fixture(monkeypatch, **read_write_settings())
    h.namespace["_native_creation_filetime"] = lambda: 134567890123456789
    h.start()
    read = h.namespace["_RUNTIMES"]["read"]["_SERVER"]
    original = h.context.get_full_tick

    def delayed_quote(codes):
        time.sleep(0.04)
        return original(codes)

    h.context.get_full_tick = mock.Mock(side_effect=delayed_quote)
    request = dict(schema_version=1, request_id="isolated-gap-old", account_id=ACCOUNT,
                   method="get_full_tick", params={"codes": [CODE]})
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(h.clients["read"].send_request, request, 0.02)
        deadline = time.monotonic() + 1
        while read.mailbox._header()[4] != 1 and time.monotonic() < deadline:
            time.sleep(0.001)
        assert NATIVE_TEST_DRIVER.inject_gap_after_completed_read(h.namespace, h.context, "isolated-gap-old") is None
        h.namespace["aqp_shm_readonly_adjust"](h.context)
        with pytest.raises(h.transports["read"].ShmTimeoutError):
            future.result(timeout=0.2)
    before = bytes(h.states["read"].data)
    assert NATIVE_TEST_DRIVER.inject_gap_after_completed_read(h.namespace, h.context, "isolated-gap-old") == 1
    assert bytes(h.states["read"].data) == before and read.mailbox._header()[4:7] == [1, 1, 0]
    data = h.call("execution", "poll_execution_events", {"after_sequence": 0})["data"]
    assert data["gap"] is True and data["lost_sequence"] == 1
    assert data["events"][0]["event"]["event_type"] == "data_loss"
    assert h.namespace["_read_rotation_evidence"]() is None and h.namespace["_READ_ROTATION"] is None
    h.close()


def test_isolated_two_generation_quotes_delay_only_the_two_old_requests():
    context = NATIVE_TEST_DRIVER.SyntheticContext(ACCOUNT, "two_generations")
    with mock.patch.object(NATIVE_TEST_DRIVER.time, "sleep") as sleep:
        results = [context.get_full_tick([CODE]) for _ in range(4)]
    assert sleep.call_args_list == [mock.call(1.0), mock.call(1.0)]
    assert all(result[CODE]["lastPrice"] > 0 and result[CODE]["time"] > 0 for result in results)
    assert context.quote_calls == 4 and context.financial_calls == 0


def test_rotation_evidence_does_not_accept_native_query_exception(monkeypatch):
    h = Fixture(monkeypatch, **persistent_settings())
    h.namespace["_native_creation_filetime"] = lambda: 134567890123456789
    h.start()
    read = h.namespace["_RUNTIMES"]["read"]["_SERVER"]

    def failed_query(codes):
        time.sleep(0.04)
        raise OSError("fixture native failure")

    h.context.get_full_tick = mock.Mock(side_effect=failed_query)
    request = dict(schema_version=1, request_id="native-error", account_id=ACCOUNT,
                   method="get_full_tick", params={"codes": [CODE]})
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(h.clients["read"].send_request, request, 0.02)
        until = time.monotonic() + 1
        while read.mailbox._header()[4] != 1 and time.monotonic() < until:
            time.sleep(0.001)
        assert read.mailbox._header()[4] == 1
        h.namespace["aqp_shm_readonly_adjust"](h.context)
        with pytest.raises(h.transports["read"].ShmTimeoutError):
            future.result(timeout=0.2)
    assert read.last_completed_request["server_error"] == "QMT_QUERY_FAILED"
    assert h.call("execution", "ping")["data"]["read_rotation_evidence"] is None
    assert read.mailbox._header()[4:7] == [1, 1, 0]
    h.close()


@pytest.mark.parametrize("operation,code,alter_old,expected", [
    ("CreateFileMappingW", 8, False, True), ("CreateFileMappingW", 14, False, True),
    ("CreateFileMappingW", 1455, False, True), ("CreateFileMappingW", 5, False, False),
    (None, 1455, False, False), ("OpenFileMappingW", 1455, False, False),
    ("CreateFileMappingW", 1455, True, False),
])
def test_rotation_capacity_requires_direct_win32_error_and_unchanged_old_frame(monkeypatch, tmp_path, operation, code, alter_old, expected):
    h = Fixture(monkeypatch, **read_write_settings())
    h.namespace["_native_creation_filetime"] = lambda: 134567890123456789
    h.start()
    read = h.namespace["_RUNTIMES"]["read"]["_SERVER"]
    execution = h.namespace["_RUNTIMES"]["execution"]["_SERVER"]
    identity_path = tmp_path / "source.identity.json"
    identity_path.write_text(json.dumps(h.namespace["_SOURCE_IDENTITY"]), encoding="utf-8")
    h.namespace["DUAL_SHM_SETTINGS"].update(native_pid=0, source_identity_path=str(identity_path))
    h.namespace["_verify_identity"] = lambda: os.getpid()
    h.namespace["_READ_ROTATION_AUTHORIZER"] = lambda command: True  # 仅隔离状态机模型。
    request = dict(schema_version=1, request_id="capacity-old", account_id=ACCOUNT, method="get_positions", params={})
    with pytest.raises(h.transports["read"].ShmTimeoutError):
        h.clients["read"].send_request(request, 0.01)
    h.namespace["aqp_shm_readonly_adjust"](h.context)
    before = bytes(h.states["read"].data)
    evidence = h.call("execution", "ping")["data"]["read_rotation_evidence"]

    def fail_allocation(name, create=False):
        assert create is True
        if alter_old:
            h.states["read"].data[h.transports["read"].REQUEST_OFFSET] ^= 1
        raise h.transports["read"].ShmNativeError("fixture allocation failed", native_winerror=code,
                                                   native_operation=operation)

    h.transports["read"].Win32Region = fail_allocation
    command = dict(contract="READ_ROTATION_CONTROL_V1", action="PREPARE", transition_id="c" * 32,
                   nonce="d" * 32, capability="e" * 64, old_evidence=evidence)
    assert h.call("execution", "ping", {"read_rotation": command})["ok"] is False
    data = h.call("execution", "ping")["data"]
    proof = data.get("read_rotation_capacity_evidence")
    if expected:
        assert proof == dict(contract="READ_ROTATION_CAPACITY_V1", status="READ_ROTATION_CAPACITY_BLOCKED",
            native_winerror=code, native_operation="CreateFileMappingW", transition_id="c" * 32,
            account_id=ACCOUNT, environment="SIM", native_pid=os.getpid(), native_creation_filetime=134567890123456789,
            windows_session_id=1, old_read_name=read.mailbox.name, old_read_session_id=read.mailbox.session_id,
            old_request_id="capacity-old", old_sequence=1, execution_session_id=execution.bridge.mailbox.session_id,
            old_evidence=evidence, old_frame_unchanged=True, automatic_retry=False)
        assert h.namespace["_READ_ROTATION"]["state"] == "PREPARE_CAPACITY_BLOCKED"
        assert bytes(h.states["read"].data) == before
    else:
        assert proof is None and h.namespace["_READ_ROTATION"]["state"] == "PREPARE_UNKNOWN"
    assert read.mailbox._header()[4:7] == [1, 1, 0]
    assert execution.bridge.mailbox.session_id == evidence["execution_session_id"]
    assert h.call("execution", "ping", {"read_rotation": command})["ok"] is False
    h.close()


def test_async_rotation_capacity_reports_evidence_but_keeps_control_unknown(monkeypatch, tmp_path):
    h = Fixture(monkeypatch, **read_write_settings())
    h.namespace["_native_creation_filetime"] = lambda: 134567890123456789
    h.start()
    execution = h.namespace["_RUNTIMES"]["execution"]["_SERVER"]
    identity_path = tmp_path / "source.identity.json"
    identity_path.write_text(json.dumps(h.namespace["_SOURCE_IDENTITY"]), encoding="utf-8")
    h.namespace["DUAL_SHM_SETTINGS"].update(native_pid=0, source_identity_path=str(identity_path))
    h.namespace["_verify_identity"] = lambda: os.getpid()
    pipe = FixtureSupervisorAuthorizationPipe()
    pipe.release.set()
    h.namespace["_SUPERVISOR_PROCESS"] = object()
    h.namespace["_SUPERVISOR_IDENTITY"] = {"pid": 222}
    h.namespace["_supervisor_identity"] = lambda process: {"pid": 222}
    h.namespace["_SUPERVISOR_PIPE"] = pipe
    h.namespace["_SUPERVISOR_GATE"] = types.SimpleNamespace(failed=False, close=lambda: None)
    execution.bridge.runtime_metadata["read_rotation_authorization_ready"] = True  # 仅模型授权。
    old = dict(schema_version=1, request_id="capacity-async-old", account_id=ACCOUNT, method="get_positions", params={})
    with pytest.raises(h.transports["read"].ShmTimeoutError):
        h.clients["read"].send_request(old, 0.01)
    h.namespace["aqp_shm_readonly_adjust"](h.context)
    before = bytes(h.states["read"].data)
    evidence = h.call("execution", "ping")["data"]["read_rotation_evidence"]

    def fail_allocation(name, create=False):
        raise h.transports["read"].ShmNativeError("fixture system memory unavailable",
            native_winerror=1455, native_operation="CreateFileMappingW")

    h.transports["read"].Win32Region = fail_allocation
    command = dict(contract="READ_ROTATION_CONTROL_V1", action="PREPARE", transition_id="c" * 32,
                   nonce="d" * 32, capability="e" * 64, old_evidence=evidence)
    assert h.call("execution", "ping", {"read_rotation": command})["data"]["read_rotation_control"]["status"] == "AUTH_PENDING"
    status = _rotation_control_status(h, "PREPARE", "c" * 32)
    assert status["status"] == "UNKNOWN" and status["receipt"] is None and status["automatic_retry"] is False
    data = h.call("execution", "ping")["data"]
    assert data["read_rotation_capacity_evidence"]["status"] == "READ_ROTATION_CAPACITY_BLOCKED"
    assert data["read_rotation_capacity_evidence"]["old_evidence"] == evidence
    assert bytes(h.states["read"].data) == before and h.call("execution", "ping", {"read_rotation": command})["ok"] is False
    assert [frame["action"] for frame in pipe.requests] == ["PREPARE"]
    h.close()


def _rotation_control_status(h, action, transition):
    command = dict(contract="READ_ROTATION_CONTROL_V1", action=action, transition_id=transition)
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline:
        result = h.call("execution", "ping", {"read_rotation_control_status": command})
        assert result["ok"] is True, result
        data = result["data"]["read_rotation_control_status"]
        if data["status"] not in ("AUTH_PENDING", "ACTIVATION_PENDING", "READ_RELEASE_PENDING", "RETIRE_ACK_PENDING"):
            return data
        time.sleep(0.005)
    raise AssertionError("fixture authorization did not complete")


def _prepared_read_rotation(monkeypatch, tmp_path, async_pipe=None, target_guard=False,
                            old_method="get_positions", old_params=None):
    h = Fixture(monkeypatch, **(read_write_settings() if target_guard else persistent_settings()))
    creation = 134567890123456789
    h.namespace["_native_creation_filetime"] = lambda: creation
    h.start()
    read = h.namespace["_RUNTIMES"]["read"]["_SERVER"]
    execution = h.namespace["_RUNTIMES"]["execution"]["_SERVER"]
    identity_path = tmp_path / "source.identity.json"
    identity_path.write_text(json.dumps(h.namespace["_SOURCE_IDENTITY"]), encoding="utf-8")
    h.namespace["DUAL_SHM_SETTINGS"].update(native_pid=0, source_identity_path=str(identity_path))
    h.namespace["_verify_identity"] = lambda: os.getpid()
    # 仅验证控制模型；实际生成源没有可用的独立授权器，默认不能执行换代。
    if async_pipe is None:
        h.namespace["_READ_ROTATION_AUTHORIZER"] = lambda command: True
    else:
        h.namespace["_SUPERVISOR_PROCESS"] = object()
        h.namespace["_SUPERVISOR_IDENTITY"] = {"pid": 222}
        h.namespace["_supervisor_identity"] = lambda process: {"pid": 222}
        h.namespace["_SUPERVISOR_PIPE"] = async_pipe
        h.namespace["_SUPERVISOR_GATE"] = types.SimpleNamespace(failed=False, close=lambda: None)
        execution.bridge.runtime_metadata["read_rotation_authorization_ready"] = True
    old_name = read.mailbox.name
    old_client = h.clients["read"]
    old_state = h.states["read"]
    extra_states = {}

    def named_region(name, create=False):
        if name == old_name:
            return Region(old_state)
        if name not in extra_states:
            assert create
            extra_states[name] = State()
        return Region(extra_states[name])

    h.transports["read"].Win32Region = named_region
    request = dict(schema_version=1, request_id="rotation-old", account_id=ACCOUNT,
                   method=old_method, params=dict(old_params or {}))
    with pytest.raises(h.transports["read"].ShmTimeoutError):
        old_client.send_request(request, 0.01)
    h.namespace["aqp_shm_readonly_adjust"](h.context)
    old_frame = bytes(old_state.data)
    evidence = h.call("execution", "ping")["data"]["read_rotation_evidence"]
    transition = "a" * 32
    prepare = dict(contract="READ_ROTATION_CONTROL_V1", action="PREPARE", transition_id=transition,
                   nonce="1" * 32, capability="1" * 64,
                   old_evidence=evidence)
    result = h.call("execution", "ping", {"read_rotation": prepare})
    assert result["ok"] is True, result
    receipt = result["data"]["read_rotation_control"]
    if async_pipe is not None:
        assert receipt["status"] == "AUTH_PENDING" and receipt["automatic_retry"] is False
        pending = h.call("execution", "ping", {"read_rotation_control_status": dict(
            contract="READ_ROTATION_CONTROL_V1", action="PREPARE", transition_id=transition)})
        assert pending["data"]["read_rotation_control_status"]["status"] == "AUTH_PENDING"
        assert h.namespace["_READ_ROTATION"] is None and bytes(old_state.data) == old_frame
        async_pipe.release.set()
        completion = _rotation_control_status(h, "PREPARE", transition)
        assert completion["status"] == "PREPARED" and completion["automatic_retry"] is False
        receipt = completion["receipt"]
    assert receipt["status"] == "PREPARED" and receipt["old_evidence"] == evidence
    assert bytes(old_state.data) == old_frame and identity_path.read_text() == json.dumps(h.namespace["_SOURCE_IDENTITY"])
    assert read.mailbox.session_id == receipt["candidate_read_session_id"]
    assert execution.bridge.mailbox.session_id == evidence["execution_session_id"]
    h.clients["read"] = h.transports["read"].ShmReadOnlyClient(receipt["candidate_read_name"], ACCOUNT)
    return h, read, execution, old_client, old_state, old_frame, identity_path, extra_states, receipt


def test_direct_native_rotation_control_is_disabled_without_independent_authorization(monkeypatch):
    h = Fixture(monkeypatch, **persistent_settings())
    h.start()
    read = h.namespace["_RUNTIMES"]["read"]["_SERVER"]
    execution = h.namespace["_RUNTIMES"]["execution"]["_SERVER"]
    before = bytes(h.states["read"].data)
    for action in ("PREPARE", "COMMIT", "ACTIVATE", "CLOSE"):
        command = dict(contract="READ_ROTATION_CONTROL_V1", action=action,
                       transition_id="a" * 32, nonce="1" * 32, capability="1" * 64,
                       token="b" * 32, old_evidence={}, gateway_verified_sha256="f" * 64)
        result = h.call("execution", "ping", {"read_rotation": command})
        assert not result["ok"] and "independent authorization is unavailable" in result["error"]
    assert h.namespace["_READ_ROTATION"] is None
    assert bytes(h.states["read"].data) == before
    assert not read.closed and not execution.closed and not h.namespace["_CLOSED"]
    h.close()


@pytest.mark.parametrize("action", ["PREPARE", "COMMIT", "ACTIVATE"])
def test_rotation_authorizer_builds_full_identity_from_native_state_and_consumes_once(monkeypatch, action):
    h = Fixture(monkeypatch, **persistent_settings())
    h.namespace["_native_creation_filetime"] = lambda: 12345
    h.start()
    execution = h.namespace["_RUNTIMES"]["execution"]["_SERVER"]
    evidence = dict(read_session_id="a" * 32, execution_session_id=execution.bridge.mailbox.session_id,
                    request_id="old-rid", sequence=3)
    h.namespace["_read_rotation_evidence"] = lambda: dict(evidence)
    h.namespace["_verify_identity"] = lambda: os.getpid()
    h.namespace["_SUPERVISOR_PROCESS"] = object()
    h.namespace["_SUPERVISOR_IDENTITY"] = {"pid": 222}
    h.namespace["_supervisor_identity"] = lambda process: {"pid": 222}
    h.namespace["_SUPERVISOR_GATE"] = types.SimpleNamespace(failed=False)
    requests = []

    def exchange(request):
        requests.append(dict(request))
        return dict(request, status="ALLOW")

    h.namespace["_SUPERVISOR_PIPE"] = types.SimpleNamespace(poisoned=False, exchange=exchange, close=lambda: None)
    command = dict(contract="READ_ROTATION_CONTROL_V1", action=action, transition_id="b" * 32,
                   nonce="c" * 32, capability="d" * 64)
    if action == "PREPARE":
        command["old_evidence"] = dict(evidence)
    else:
        h.namespace["_READ_ROTATION"] = dict(state="PREPARED" if action == "COMMIT" else "COMMITTED",
            transition_id="b" * 32, token="e" * 32, old_evidence=dict(evidence),
            candidate_read_session_id="f" * 32, probe_request_id="probe-rid")
        command["token"] = "e" * 32
        if action == "COMMIT":
            command.update(probe_request_id="probe-rid", probe_sequence=1)
    assert h.namespace["_authorize_read_rotation"](command) is True
    assert requests == [dict(contract="QMT_NATIVE_ROTATION_PIPE_V1", action=action,
        transition_id="b" * 32, nonce="c" * 32, capability="d" * 64, account_id=ACCOUNT, environment="SIM",
        native_pid=os.getpid(), native_creation_filetime=12345, windows_session_id=1,
        execution_session_id=execution.bridge.mailbox.session_id, read_session_id="a" * 32,
        old_request_id="old-rid", old_sequence=3, candidate_read_session_id=None if action == "PREPARE" else "f" * 32,
        new_probe_request_id=None if action == "PREPARE" else "probe-rid",
        new_probe_sequence=None if action == "PREPARE" else 1)]
    with pytest.raises(RuntimeError, match="not repeatable"):
        h.namespace["_authorize_read_rotation"](command)
    assert len(requests) == 1
    h.namespace["_READ_ROTATION"] = None
    h.namespace["_SUPERVISOR_GATE"] = None
    h.close()


@pytest.mark.parametrize("failure", ["unmatched_evidence", "bad_receipt", "lost_reply"])
def test_rotation_authorization_never_retries_untrusted_or_unknown_result(monkeypatch, failure):
    h = Fixture(monkeypatch, **persistent_settings())
    h.namespace["_native_creation_filetime"] = lambda: 12345
    h.start()
    execution = h.namespace["_RUNTIMES"]["execution"]["_SERVER"]
    evidence = dict(read_session_id="a" * 32, execution_session_id=execution.bridge.mailbox.session_id,
                    request_id="old-rid", sequence=3)
    h.namespace["_read_rotation_evidence"] = lambda: dict(evidence)
    h.namespace["_verify_identity"] = lambda: os.getpid()
    h.namespace["_SUPERVISOR_PROCESS"] = object()
    h.namespace["_SUPERVISOR_IDENTITY"] = {"pid": 222}
    h.namespace["_supervisor_identity"] = lambda process: {"pid": 222}
    h.namespace["_SUPERVISOR_GATE"] = types.SimpleNamespace(failed=False)
    requests = []

    def exchange(request):
        requests.append(dict(request))
        if failure == "lost_reply":
            raise OSError("supervisor response unknown")
        return dict(request, status="ALLOW", old_sequence=4)

    h.namespace["_SUPERVISOR_PIPE"] = types.SimpleNamespace(poisoned=False, exchange=exchange, close=lambda: None)
    supplied = dict(evidence)
    if failure == "unmatched_evidence":
        supplied["sequence"] += 1
    command = dict(contract="READ_ROTATION_CONTROL_V1", action="PREPARE", transition_id="b" * 32,
                   nonce="c" * 32, capability="d" * 64, old_evidence=supplied)
    with pytest.raises((RuntimeError, OSError)):
        h.namespace["_authorize_read_rotation"](command)
    assert len(requests) == (0 if failure == "unmatched_evidence" else 1)
    if failure != "unmatched_evidence":
        with pytest.raises(RuntimeError, match="not repeatable"):
            h.namespace["_authorize_read_rotation"](command)
        assert len(requests) == 1
    h.namespace["_SUPERVISOR_GATE"] = None
    h.close()


def test_read_rotation_prepares_commits_and_activates_without_reloading_execution(monkeypatch, tmp_path):
    h, read, execution, old_client, old_state, old_frame, identity_path, extra_states, receipt = _prepared_read_rotation(monkeypatch, tmp_path)
    sid = execution.bridge.mailbox.session_id
    h.namespace["order_callback"](h.context, dict(m_strAccountID=ACCOUNT, m_strOrderSysID="before-rotation"))
    probe = h.call("read", "ping", request_id=receipt["probe_request_id"])
    assert probe["ok"] and probe["sequence"] == 1
    assert probe["data"]["session_id"] == receipt["candidate_read_session_id"]
    assert probe["data"]["native_creation_filetime"] == 134567890123456789
    candidate_state = extra_states[receipt["candidate_read_name"]]
    candidate_probe_frame = bytes(candidate_state.data)
    commit = dict(contract="READ_ROTATION_CONTROL_V1", action="COMMIT", transition_id=receipt["transition_id"],
                  nonce="2" * 32, capability="2" * 64,
                  token=receipt["token"], probe_request_id=receipt["probe_request_id"], probe_sequence=1)
    result = h.call("execution", "ping", {"read_rotation": commit})
    assert result["ok"] and result["data"]["read_rotation_control"]["status"] == "COMMITTED"
    identity = json.loads(identity_path.read_text())
    assert identity["read_name"] == receipt["candidate_read_name"]
    assert identity["read_session_id"] == receipt["candidate_read_session_id"]
    assert identity["execution_session_id"] == sid
    assert identity["read_rotation"]["old_request_id"] == "rotation-old"
    assert identity["read_rotation"]["new_probe_sequence"] == 1
    assert bytes(old_state.data) == old_frame and bytes(candidate_state.data) == candidate_probe_frame
    activate = dict(contract="READ_ROTATION_CONTROL_V1", action="ACTIVATE", transition_id=receipt["transition_id"],
                    nonce="3" * 32, capability="3" * 64,
                    token=receipt["token"])
    result = h.call("execution", "ping", {"read_rotation": activate})
    assert result["ok"] and result["data"]["read_rotation_control"]["status"] == "ACTIVATED"
    assert h.call("read", "get_full_tick", {"codes": [CODE]})["ok"] is True
    duplicate = h.call("read", "get_full_tick", {"codes": [CODE]}, request_id="rotation-old")
    assert duplicate["server_error"] == "DUPLICATE_REQUEST"
    h.namespace["deal_callback"](h.context, dict(m_strAccountID=ACCOUNT, m_strOrderSysID="before-rotation", m_strTradeID="after-rotation"))
    events = h.call("execution", "poll_execution_events", {"after_sequence": 0})["data"]
    assert events["session_id"] == sid and events["gap"] is False
    assert [item["sequence"] for item in events["events"]] == [1, 2]
    assert bytes(old_state.data) == old_frame and h.context.accounts == [ACCOUNT]
    old_client.close()
    h.close()
    assert all(region.closed for state in extra_states.values() for region in state.regions)


def test_read_rotation_candidate_refuses_business_requests_before_activation(monkeypatch, tmp_path):
    h, read, execution, old_client, old_state, old_frame, identity_path, extra_states, receipt = _prepared_read_rotation(monkeypatch, tmp_path)
    denied = h.call("read", "get_full_tick", {"codes": [CODE]})
    assert not denied["ok"] and denied["server_error"] == "ROTATION_PREPARED_ONLY"
    assert not h.context.unsubscribed and not h.query_calls
    assert bytes(old_state.data) == old_frame and not execution.closed
    commit = dict(contract="READ_ROTATION_CONTROL_V1", action="COMMIT", transition_id=receipt["transition_id"],
                  nonce="2" * 32, capability="2" * 64,
                  token=receipt["token"], probe_request_id=receipt["probe_request_id"], probe_sequence=1)
    assert not h.call("execution", "ping", {"read_rotation": commit})["ok"]
    assert json.loads(identity_path.read_text())["read_session_id"] == receipt["old_evidence"]["read_session_id"]
    old_client.close()
    h.close()


class FixtureSupervisorAuthorizationPipe:
    def __init__(self):
        self.poisoned, self.fail = False, False
        self.release, self.closed = threading.Event(), threading.Event()
        self.activation_release, self.fail_activation = threading.Event(), False
        self.read_release, self.fail_read_release = threading.Event(), False
        self.retire_release, self.fail_retire = threading.Event(), False
        self.activation_release.set()
        self.read_release.set()
        self.retire_release.set()
        self.requests = []

    def exchange(self, request):
        self.requests.append(dict(request))
        if not self.release.wait(1.5):
            raise TimeoutError("fixture supervisor authorization timeout")
        if self.fail:
            raise OSError("fixture supervisor reply unknown")
        if request.get("action") == "ACTIVATION_RECEIPT":
            if not self.activation_release.wait(1.5) or self.fail_activation:
                raise OSError("fixture activation receipt unknown")
        if request.get("action") == "READ_RELEASED_RECEIPT":
            if not self.read_release.wait(1.5) or self.fail_read_release:
                raise OSError("fixture read release receipt unknown")
        if request.get("action") == "GENERATION_RETIRED_RECEIPT":
            if not self.retire_release.wait(1.5) or self.fail_retire:
                raise OSError("fixture retired generation receipt unknown")
        return dict(request, status="ACK" if request.get("action") in
                    ("ACTIVATION_RECEIPT", "READ_RELEASED_RECEIPT", "GENERATION_RETIRED_RECEIPT") else "ALLOW")

    def close(self):
        self.closed.set()


def test_async_rotation_control_preserves_old_frame_and_exec_session_through_three_stages(monkeypatch, tmp_path):
    pipe = FixtureSupervisorAuthorizationPipe()
    h, read, execution, old_client, old_state, old_frame, identity_path, extra_states, receipt = _prepared_read_rotation(
        monkeypatch, tmp_path, async_pipe=pipe)
    execution_sid = execution.bridge.mailbox.session_id
    assert h.namespace["_READ_ROTATION_AUTHORIZER"] is None  # Only a test-injected capability is active.
    assert len(pipe.requests) == 1 and pipe.requests[0]["action"] == "PREPARE"
    probe = h.call("read", "ping", request_id=receipt["probe_request_id"])
    assert probe["ok"] and probe["sequence"] == 1
    candidate_frame = bytes(extra_states[receipt["candidate_read_name"]].data)
    commit = dict(contract="READ_ROTATION_CONTROL_V1", action="COMMIT", transition_id=receipt["transition_id"],
                  nonce="2" * 32, capability="2" * 64, token=receipt["token"],
                  probe_request_id=receipt["probe_request_id"], probe_sequence=1)
    initial = h.call("execution", "ping", {"read_rotation": commit})["data"]["read_rotation_control"]
    assert initial["status"] == "AUTH_PENDING" and initial["automatic_retry"] is False
    completed = _rotation_control_status(h, "COMMIT", receipt["transition_id"])
    assert completed["status"] == "COMMITTED" and completed["receipt"]["status"] == "COMMITTED"
    assert bytes(old_state.data) == old_frame and bytes(extra_states[receipt["candidate_read_name"]].data) == candidate_frame
    assert json.loads(identity_path.read_text())["execution_session_id"] == execution_sid
    activate = dict(contract="READ_ROTATION_CONTROL_V1", action="ACTIVATE", transition_id=receipt["transition_id"],
                    nonce="3" * 32, capability="3" * 64, token=receipt["token"])
    initial = h.call("execution", "ping", {"read_rotation": activate})["data"]["read_rotation_control"]
    assert initial["status"] == "AUTH_PENDING"
    completed = _rotation_control_status(h, "ACTIVATE", receipt["transition_id"])
    assert completed["status"] == "ACTIVATED" and completed["receipt"]["status"] == "ACTIVATED"
    assert [request["action"] for request in pipe.requests] == ["PREPARE", "COMMIT", "ACTIVATE", "ACTIVATION_RECEIPT", "READ_RELEASED_RECEIPT"]
    assert pipe.requests[-2] == dict(contract="QMT_NATIVE_ROTATION_PIPE_V1", action="ACTIVATION_RECEIPT",
        transition_id=receipt["transition_id"], account_id=ACCOUNT, environment="SIM", native_pid=os.getpid(),
        native_creation_filetime=134567890123456789, windows_session_id=1, execution_session_id=execution_sid,
        old_read_session_id=receipt["old_evidence"]["read_session_id"],
        candidate_read_session_id=receipt["candidate_read_session_id"], old_request_id="rotation-old",
        old_sequence=1, probe_request_id=receipt["probe_request_id"], probe_sequence=1)
    assert pipe.requests[-1] == dict(pipe.requests[-2], action="READ_RELEASED_RECEIPT")
    assert h.call("read", "get_full_tick", {"codes": [CODE]})["ok"] is True
    assert bytes(old_state.data) == old_frame and execution.bridge.mailbox.session_id == execution_sid
    assert all(job["request"] is None and job["command"] is None for job in h.namespace["_ROTATION_CONTROL_JOBS"].values())
    old_client.close()
    h.close()


def test_async_rotation_unknown_is_sticky_and_never_republishes_or_retries(monkeypatch, tmp_path):
    pipe = FixtureSupervisorAuthorizationPipe()
    h, read, execution, old_client, old_state, old_frame, identity_path, extra_states, receipt = _prepared_read_rotation(
        monkeypatch, tmp_path, async_pipe=pipe)
    assert h.call("read", "ping", request_id=receipt["probe_request_id"])["ok"] is True
    candidate_frame = bytes(extra_states[receipt["candidate_read_name"]].data)
    original_identity = identity_path.read_text()
    pipe.fail = True
    commit = dict(contract="READ_ROTATION_CONTROL_V1", action="COMMIT", transition_id=receipt["transition_id"],
                  nonce="2" * 32, capability="2" * 64, token=receipt["token"],
                  probe_request_id=receipt["probe_request_id"], probe_sequence=1)
    assert h.call("execution", "ping", {"read_rotation": commit})["data"]["read_rotation_control"]["status"] == "AUTH_PENDING"
    completed = _rotation_control_status(h, "COMMIT", receipt["transition_id"])
    assert completed["status"] == "UNKNOWN" and completed["receipt"] is None
    assert completed["automatic_retry"] is False and "2" * 64 not in json.dumps(completed)
    for retried in (commit, dict(commit, transition_id="f" * 32)):
        assert h.call("execution", "ping", {"read_rotation": retried})["ok"] is False
    assert len(pipe.requests) == 2
    assert bytes(old_state.data) == old_frame and bytes(extra_states[receipt["candidate_read_name"]].data) == candidate_frame
    assert identity_path.read_text() == original_identity and read.mailbox.session_id == receipt["candidate_read_session_id"]
    old_client.close()
    h.close()


def test_activation_ack_loss_is_unknown_and_candidate_returns_to_probe_only(monkeypatch, tmp_path):
    pipe = FixtureSupervisorAuthorizationPipe()
    h, read, execution, old_client, old_state, old_frame, identity_path, extra_states, receipt = _prepared_read_rotation(
        monkeypatch, tmp_path, async_pipe=pipe, target_guard=True)
    assert h.call("read", "ping", request_id=receipt["probe_request_id"])["ok"] is True
    candidate_frame = bytes(extra_states[receipt["candidate_read_name"]].data)
    commit = dict(contract="READ_ROTATION_CONTROL_V1", action="COMMIT", transition_id=receipt["transition_id"],
                  nonce="2" * 32, capability="2" * 64, token=receipt["token"],
                  probe_request_id=receipt["probe_request_id"], probe_sequence=1)
    assert h.call("execution", "ping", {"read_rotation": commit})["data"]["read_rotation_control"]["status"] == "AUTH_PENDING"
    assert _rotation_control_status(h, "COMMIT", receipt["transition_id"])["status"] == "COMMITTED"
    activate = dict(contract="READ_ROTATION_CONTROL_V1", action="ACTIVATE", transition_id=receipt["transition_id"],
                    nonce="3" * 32, capability="3" * 64, token=receipt["token"])
    pipe.activation_release.clear()
    pipe.fail_activation = True
    assert h.call("execution", "ping", {"read_rotation": activate})["data"]["read_rotation_control"]["status"] == "AUTH_PENDING"
    status_command = dict(contract="READ_ROTATION_CONTROL_V1", action="ACTIVATE", transition_id=receipt["transition_id"])
    deadline = time.monotonic() + 1
    while time.monotonic() < deadline:
        pending = h.call("execution", "ping", {"read_rotation_control_status": status_command})["data"]["read_rotation_control_status"]
        if pending["status"] == "ACTIVATION_PENDING":
            break
    assert pending["status"] == "ACTIVATION_PENDING" and pending["receipt"] is None
    assert h.namespace["_READ_ROTATION"]["state"] == "ACTIVATED_RESTRICTED"
    assert read.rotation_probe_request_id == receipt["probe_request_id"]
    assert bytes(extra_states[receipt["candidate_read_name"]].data) == candidate_frame
    pipe.activation_release.set()
    result = _rotation_control_status(h, "ACTIVATE", receipt["transition_id"])
    assert result["status"] == "UNKNOWN" and result["receipt"] is None
    assert h.namespace["_READ_ROTATION"]["state"] == "ACTIVATE_UNKNOWN"
    assert read.rotation_probe_request_id == receipt["probe_request_id"]
    assert bytes(old_state.data) == old_frame
    assert h.call("execution", "ping", {"read_rotation": activate})["ok"] is False
    assert [row["action"] for row in pipe.requests].count("ACTIVATION_RECEIPT") == 1
    old_client.close()
    h.close()


@pytest.mark.parametrize("fault", ["ack_lost", "callback_gap"])
def test_read_release_unconfirmed_never_publishes_activation(monkeypatch, tmp_path, fault):
    pipe = FixtureSupervisorAuthorizationPipe()
    h, read, execution, old_client, old_state, old_frame, identity_path, extra_states, receipt = _prepared_read_rotation(
        monkeypatch, tmp_path, async_pipe=pipe, target_guard=True)
    assert h.call("read", "ping", request_id=receipt["probe_request_id"])["ok"] is True
    candidate_frame = bytes(extra_states[receipt["candidate_read_name"]].data)
    commit = dict(contract="READ_ROTATION_CONTROL_V1", action="COMMIT", transition_id=receipt["transition_id"],
                  nonce="2" * 32, capability="2" * 64, token=receipt["token"],
                  probe_request_id=receipt["probe_request_id"], probe_sequence=1)
    assert h.call("execution", "ping", {"read_rotation": commit})["data"]["read_rotation_control"]["status"] == "AUTH_PENDING"
    assert _rotation_control_status(h, "COMMIT", receipt["transition_id"])["status"] == "COMMITTED"
    activate = dict(contract="READ_ROTATION_CONTROL_V1", action="ACTIVATE", transition_id=receipt["transition_id"],
                    nonce="3" * 32, capability="3" * 64, token=receipt["token"])
    pipe.read_release.clear()
    pipe.fail_read_release = fault == "ack_lost"
    assert h.call("execution", "ping", {"read_rotation": activate})["data"]["read_rotation_control"]["status"] == "AUTH_PENDING"
    status_command = dict(contract="READ_ROTATION_CONTROL_V1", action="ACTIVATE", transition_id=receipt["transition_id"])
    deadline = time.monotonic() + 1
    while time.monotonic() < deadline:
        pending = h.call("execution", "ping", {"read_rotation_control_status": status_command})["data"]["read_rotation_control_status"]
        if pending["status"] == "READ_RELEASE_PENDING":
            break
    assert pending["status"] == "READ_RELEASE_PENDING" and pending["receipt"] is None
    assert h.namespace["_READ_ROTATION"]["state"] == "ACTIVATED"
    assert bytes(extra_states[receipt["candidate_read_name"]].data) == candidate_frame
    if fault == "callback_gap":
        with ThreadPoolExecutor(1) as pool:
            pool.submit(h.namespace["deal_callback"], h.context, {}).result(timeout=1)
        pending = h.call("execution", "ping", {"read_rotation_control_status": status_command})["data"]["read_rotation_control_status"]
        assert pending["status"] == "UNKNOWN" and pending["receipt"] is None
        assert execution.bridge._lost_sequence > 0
    pipe.read_release.set()
    result = _rotation_control_status(h, "ACTIVATE", receipt["transition_id"])
    assert result["status"] == "UNKNOWN" and result["receipt"] is None and result["automatic_retry"] is False
    assert read.rotation_probe_request_id == receipt["probe_request_id"]
    assert bytes(old_state.data) == old_frame and bytes(extra_states[receipt["candidate_read_name"]].data) == candidate_frame
    assert h.call("execution", "ping", {"read_rotation": activate})["ok"] is False
    assert [row["action"] for row in pipe.requests].count("READ_RELEASED_RECEIPT") == 1
    old_client.close()
    h.close()


@pytest.mark.parametrize("fault", [None, "ack_lost", "callback_gap", "retirement_race"])
def test_retirement_requires_manager_ack_and_preserves_old_frame(monkeypatch, tmp_path, fault):
    pipe = FixtureSupervisorAuthorizationPipe()
    h, read, execution, old_client, old_state, old_frame, identity_path, extra_states, receipt = _prepared_read_rotation(
        monkeypatch, tmp_path, async_pipe=pipe, target_guard=True)
    assert h.call("read", "ping", request_id=receipt["probe_request_id"])["ok"] is True
    commit = dict(contract="READ_ROTATION_CONTROL_V1", action="COMMIT", transition_id=receipt["transition_id"],
                  nonce="2" * 32, capability="2" * 64, token=receipt["token"],
                  probe_request_id=receipt["probe_request_id"], probe_sequence=1)
    assert h.call("execution", "ping", {"read_rotation": commit})["data"]["read_rotation_control"]["status"] == "AUTH_PENDING"
    assert _rotation_control_status(h, "COMMIT", receipt["transition_id"])["status"] == "COMMITTED"
    activate = dict(contract="READ_ROTATION_CONTROL_V1", action="ACTIVATE", transition_id=receipt["transition_id"],
                    nonce="3" * 32, capability="3" * 64, token=receipt["token"])
    assert h.call("execution", "ping", {"read_rotation": activate})["data"]["read_rotation_control"]["status"] == "AUTH_PENDING"
    assert _rotation_control_status(h, "ACTIVATE", receipt["transition_id"])["status"] == "ACTIVATED"
    # 开放业务读后候选 ping 帧允许自然覆盖；旧邮箱的迟到帧必须不变。
    assert h.call("read", "get_full_tick", {"codes": [CODE]})["ok"] is True
    # 模拟已验证 capability 因本代 COMMIT 撤销；只有同 transition 的收尾可用。
    h.namespace["_ROTATION_CAPABILITY"] = dict(status="REVOKED", reason="COMMITTED_READ_SID", request=dict(
        read_session_id=receipt["old_evidence"]["read_session_id"],
        execution_session_id=execution.bridge.mailbox.session_id, native_pid=os.getpid(),
        native_creation_filetime=134567890123456789))
    execution.bridge.runtime_metadata["read_rotation_authorization_ready"] = False
    close = dict(contract="READ_ROTATION_CONTROL_V1", action="CLOSE", transition_id=receipt["transition_id"],
                 nonce="4" * 32, capability="4" * 64, gateway_verified_sha256="f" * 64)
    pipe.retire_release.clear()
    pipe.fail_retire = fault == "ack_lost"
    assert h.call("execution", "ping", {"read_rotation": close})["data"]["read_rotation_control"]["status"] == "AUTH_PENDING"
    status_command = dict(contract="READ_ROTATION_CONTROL_V1", action="CLOSE", transition_id=receipt["transition_id"])
    deadline = time.monotonic() + 1
    while time.monotonic() < deadline:
        pending = h.call("execution", "ping", {"read_rotation_control_status": status_command})["data"]["read_rotation_control_status"]
        if pending["status"] == "RETIRE_ACK_PENDING":
            break
    assert pending["status"] == "RETIRE_ACK_PENDING" and pending["receipt"] is None
    assert h.namespace["_READ_ROTATION"]["state"] == "RETIRED_RESTRICTED"
    assert not h.namespace["_RETIRED_READ_ROTATIONS"] and bytes(old_state.data) == old_frame
    if fault == "callback_gap":
        with ThreadPoolExecutor(1) as pool:
            pool.submit(h.namespace["deal_callback"], h.context, {}).result(timeout=1)
        pending = h.call("execution", "ping", {"read_rotation_control_status": status_command})["data"]["read_rotation_control_status"]
        assert pending["status"] == "UNKNOWN" and pending["receipt"] is None
    elif fault == "retirement_race":
        check = h.namespace["_execution_continuity_clear"]

        def gap_after_retention():
            if h.namespace["_RETIRED_READ_ROTATIONS"] and not execution.bridge._off_thread_event_loss.is_set():
                with ThreadPoolExecutor(1) as pool:
                    pool.submit(h.namespace["deal_callback"], h.context, {}).result(timeout=1)
            return check()

        h.namespace["_execution_continuity_clear"] = gap_after_retention
    pipe.retire_release.set()
    result = _rotation_control_status(h, "CLOSE", receipt["transition_id"])
    assert result["automatic_retry"] is False and bytes(old_state.data) == old_frame
    assert execution.bridge.mailbox.session_id == receipt["old_evidence"]["execution_session_id"]
    assert [row["action"] for row in pipe.requests].count("CLOSE_PERMIT") == 1
    assert [row["action"] for row in pipe.requests].count("GENERATION_RETIRED_RECEIPT") == 1
    permit, retired_frame = pipe.requests[-2:]
    assert permit == dict(contract="QMT_NATIVE_ROTATION_PIPE_V1", action="CLOSE_PERMIT",
        transition_id=receipt["transition_id"], nonce="4" * 32, capability="4" * 64,
        account_id=ACCOUNT, environment="SIM", native_pid=os.getpid(), native_creation_filetime=134567890123456789,
        windows_session_id=1, execution_session_id=execution.bridge.mailbox.session_id,
        read_session_id=receipt["old_evidence"]["read_session_id"], old_request_id="rotation-old", old_sequence=1,
        candidate_read_session_id=receipt["candidate_read_session_id"], new_probe_request_id=receipt["probe_request_id"],
        new_probe_sequence=1, gateway_verified_sha256="f" * 64)
    assert retired_frame == dict(contract="QMT_NATIVE_ROTATION_PIPE_V1", action="GENERATION_RETIRED_RECEIPT",
        transition_id=receipt["transition_id"], account_id=ACCOUNT, environment="SIM", native_pid=os.getpid(),
        native_creation_filetime=134567890123456789, windows_session_id=1,
        execution_session_id=execution.bridge.mailbox.session_id, old_read_session_id=receipt["old_evidence"]["read_session_id"],
        candidate_read_session_id=receipt["candidate_read_session_id"], old_request_id="rotation-old", old_sequence=1,
        probe_request_id=receipt["probe_request_id"], probe_sequence=1, gateway_verified_sha256="f" * 64)
    if fault is None:
        assert result["status"] == "CLOSED" and result["receipt"]["gateway_verified_sha256"] == "f" * 64
        assert h.namespace["_READ_ROTATION"] is None and len(h.namespace["_RETIRED_READ_ROTATIONS"]) == 1
        retained = h.namespace["_RETIRED_READ_ROTATIONS"][0]["old_mailbox"]
        assert (retained.name, retained.session_id) == (old_client.mailbox.name, old_client.mailbox.session_id)
        assert execution.bridge.runtime_metadata["read_rotation_authorization_ready"] is False
        # 新代没有新的独立 capability 时仍不允许 PREPARE。
        next_client = h.clients["read"]
        next_request = dict(schema_version=1, request_id="rotation-old-second", account_id=ACCOUNT,
                            method="get_positions", params={})
        with pytest.raises(h.transports["read"].ShmTimeoutError):
            next_client.send_request(next_request, 0.01)
        h.namespace["aqp_shm_readonly_adjust"](h.context)
        second_frame = bytes(extra_states[receipt["candidate_read_name"]].data)
        second_evidence = h.call("execution", "ping")["data"]["read_rotation_evidence"]
        assert second_evidence["request_id"] == "rotation-old-second"
        second_prepare = dict(contract="READ_ROTATION_CONTROL_V1", action="PREPARE", transition_id="b" * 32,
                              nonce="5" * 32, capability="5" * 64, old_evidence=second_evidence)
        assert h.call("execution", "ping", {"read_rotation": second_prepare})["ok"] is False
        # 仅模型中模拟第二代独立 challenge 已验证；生成源真实生产能力仍默认为 False。
        execution.bridge.runtime_metadata["read_rotation_authorization_ready"] = True
        assert h.call("execution", "ping", {"read_rotation": second_prepare})["data"]["read_rotation_control"]["status"] == "AUTH_PENDING"
        second = _rotation_control_status(h, "PREPARE", "b" * 32)
        assert second["status"] == "PREPARED" and second["receipt"]["old_evidence"] == second_evidence
        assert second["receipt"]["candidate_read_session_id"] not in (
            receipt["candidate_read_session_id"], receipt["old_evidence"]["read_session_id"], execution.bridge.mailbox.session_id)
        assert bytes(old_state.data) == old_frame and bytes(extra_states[receipt["candidate_read_name"]].data) == second_frame
        second = second["receipt"]
        h.clients["read"] = h.transports["read"].ShmReadOnlyClient(second["candidate_read_name"], ACCOUNT)
        assert h.call("read", "ping", request_id=second["probe_request_id"])["ok"] is True
        second_commit = dict(contract="READ_ROTATION_CONTROL_V1", action="COMMIT", transition_id="b" * 32,
            nonce="6" * 32, capability="6" * 64, token=second["token"], probe_request_id=second["probe_request_id"], probe_sequence=1)
        assert h.call("execution", "ping", {"read_rotation": second_commit})["data"]["read_rotation_control"]["status"] == "AUTH_PENDING"
        assert _rotation_control_status(h, "COMMIT", "b" * 32)["status"] == "COMMITTED"
        second_activate = dict(contract="READ_ROTATION_CONTROL_V1", action="ACTIVATE", transition_id="b" * 32,
                               nonce="7" * 32, capability="7" * 64, token=second["token"])
        assert h.call("execution", "ping", {"read_rotation": second_activate})["data"]["read_rotation_control"]["status"] == "AUTH_PENDING"
        assert _rotation_control_status(h, "ACTIVATE", "b" * 32)["status"] == "ACTIVATED"
        second_close = dict(contract="READ_ROTATION_CONTROL_V1", action="CLOSE", transition_id="b" * 32,
                            nonce="8" * 32, capability="8" * 64, gateway_verified_sha256="e" * 64)
        assert h.call("execution", "ping", {"read_rotation": second_close})["data"]["read_rotation_control"]["status"] == "AUTH_PENDING"
        assert _rotation_control_status(h, "CLOSE", "b" * 32)["status"] == "CLOSED"
        assert len(h.namespace["_RETIRED_READ_ROTATIONS"]) == 2 and h.namespace["_READ_ROTATION"] is None
        assert bytes(old_state.data) == old_frame and bytes(extra_states[receipt["candidate_read_name"]].data) == second_frame
        assert execution.bridge.mailbox.session_id == receipt["old_evidence"]["execution_session_id"]
        # 关代后仍不可把跨线程回报丢失当作连续；旧收据与帧保留且写被阻断。
        with ThreadPoolExecutor(1) as pool:
            pool.submit(h.namespace["deal_callback"], h.context, {}).result(timeout=1)
        h.call("execution", "ping")
        assert execution.bridge.runtime_metadata["read_rotation_continuity"] == "UNKNOWN"
        assert execution.bridge._lost_sequence > 0 and execution.bridge._async_write_block_reason
        assert h.namespace["_ROTATION_CONTROL_JOBS"][("b" * 32, "CLOSE")]["prior_receipt"]["status"] == "CLOSED"
        assert bytes(old_state.data) == old_frame and bytes(extra_states[receipt["candidate_read_name"]].data) == second_frame
    else:
        assert result["status"] == "UNKNOWN" and result["receipt"] is None
        assert h.namespace["_READ_ROTATION"] is not None
        assert len(h.namespace["_RETIRED_READ_ROTATIONS"]) == (1 if fault == "retirement_race" else 0)
        assert h.call("execution", "ping", {"read_rotation": close})["ok"] is False
    old_client.close()
    h.close()


def test_stop_during_rotation_authorization_never_applies_late_permit(monkeypatch, tmp_path):
    pipe = FixtureSupervisorAuthorizationPipe()
    h, read, execution, old_client, old_state, old_frame, identity_path, extra_states, receipt = _prepared_read_rotation(
        monkeypatch, tmp_path, async_pipe=pipe)
    assert h.call("read", "ping", request_id=receipt["probe_request_id"])["ok"] is True
    original_identity = identity_path.read_text()
    pipe.release.clear()
    commit = dict(contract="READ_ROTATION_CONTROL_V1", action="COMMIT", transition_id=receipt["transition_id"],
                  nonce="2" * 32, capability="2" * 64, token=receipt["token"],
                  probe_request_id=receipt["probe_request_id"], probe_sequence=1)
    assert h.call("execution", "ping", {"read_rotation": commit})["data"]["read_rotation_control"]["status"] == "AUTH_PENDING"
    old_client.close()
    h.close()
    assert h.namespace["_SUPERVISOR_CLEANUP_STATUS"]["status"] == "PENDING"
    assert not pipe.closed.is_set()  # I/O still in flight; stop must not pretend it ended.
    pipe.release.set()
    assert pipe.closed.wait(2)
    assert h.namespace["_READ_ROTATION"]["state"] == "PREPARED"
    assert identity_path.read_text() == original_identity and bytes(old_state.data) == old_frame


@pytest.mark.parametrize("action", ["COMMIT", "ACTIVATE"])
def test_gap_injected_after_locked_evidence_never_completes_rotation(monkeypatch, tmp_path, action):
    h, read, execution, old_client, old_state, old_frame, identity_path, extra_states, receipt = _prepared_read_rotation(
        monkeypatch, tmp_path, target_guard=True)
    assert h.call("read", "ping", request_id=receipt["probe_request_id"])["ok"] is True
    commit = dict(contract="READ_ROTATION_CONTROL_V1", action="COMMIT", transition_id=receipt["transition_id"],
                  nonce="2" * 32, capability="2" * 64, token=receipt["token"],
                  probe_request_id=receipt["probe_request_id"], probe_sequence=1)
    if action == "ACTIVATE":
        assert h.call("execution", "ping", {"read_rotation": commit})["ok"] is True
    before_identity = identity_path.read_text()
    candidate_frame = bytes(extra_states[receipt["candidate_read_name"]].data)
    original = read.read_rotation_evidence

    def evidence_with_concurrent_callback(*args, **kwargs):
        value = original(*args, **kwargs)
        if kwargs.get("mailbox_locked"):
            with ThreadPoolExecutor(1) as pool:
                pool.submit(h.namespace["deal_callback"], h.context, {}).result(timeout=1)
        return value

    with mock.patch.object(read, "read_rotation_evidence", side_effect=evidence_with_concurrent_callback):
        command = (commit if action == "COMMIT" else
                   dict(contract="READ_ROTATION_CONTROL_V1", action="ACTIVATE", transition_id=receipt["transition_id"],
                        nonce="3" * 32, capability="3" * 64, token=receipt["token"]))
        assert h.call("execution", "ping", {"read_rotation": command})["ok"] is False
    assert execution.bridge._off_thread_event_loss.is_set()
    assert h.namespace["_READ_ROTATION"]["state"] == action + "_PENDING"
    assert identity_path.read_text() == before_identity
    assert bytes(old_state.data) == old_frame and bytes(extra_states[receipt["candidate_read_name"]].data) == candidate_frame
    assert read.rotation_probe_request_id == receipt["probe_request_id"]
    assert h.namespace["_read_rotation_evidence"]() is None
    assert execution.bridge.mailbox.session_id == receipt["old_evidence"]["execution_session_id"]
    old_client.close()
    h.close()


def test_gap_during_publication_is_unknown_and_preserves_both_generations(monkeypatch, tmp_path):
    h, read, execution, old_client, old_state, old_frame, identity_path, extra_states, receipt = _prepared_read_rotation(
        monkeypatch, tmp_path, target_guard=True)
    assert h.call("read", "ping", request_id=receipt["probe_request_id"])["ok"] is True
    candidate_frame = bytes(extra_states[receipt["candidate_read_name"]].data)
    original_replace = os.replace

    def replace_then_callback(source, target):
        original_replace(source, target)
        with ThreadPoolExecutor(1) as pool:
            pool.submit(h.namespace["deal_callback"], h.context, {}).result(timeout=1)

    commit = dict(contract="READ_ROTATION_CONTROL_V1", action="COMMIT", transition_id=receipt["transition_id"],
                  nonce="2" * 32, capability="2" * 64, token=receipt["token"],
                  probe_request_id=receipt["probe_request_id"], probe_sequence=1)
    with mock.patch.object(os, "replace", side_effect=replace_then_callback):
        assert h.call("execution", "ping", {"read_rotation": commit})["ok"] is False
    assert h.namespace["_READ_ROTATION"]["state"] == "COMMIT_UNKNOWN"
    assert execution.bridge._off_thread_event_loss.is_set()
    assert json.loads(identity_path.read_text())["read_session_id"] == receipt["candidate_read_session_id"]
    assert bytes(old_state.data) == old_frame and bytes(extra_states[receipt["candidate_read_name"]].data) == candidate_frame
    assert read.rotation_probe_request_id == receipt["probe_request_id"]
    assert h.namespace["_read_rotation_evidence"]() is None
    old_client.close()
    h.close()


def test_gap_during_activation_restores_probe_only_and_returns_unknown(monkeypatch, tmp_path):
    h, read, execution, old_client, old_state, old_frame, identity_path, extra_states, receipt = _prepared_read_rotation(
        monkeypatch, tmp_path, target_guard=True)
    assert h.call("read", "ping", request_id=receipt["probe_request_id"])["ok"] is True
    commit = dict(contract="READ_ROTATION_CONTROL_V1", action="COMMIT", transition_id=receipt["transition_id"],
                  nonce="2" * 32, capability="2" * 64, token=receipt["token"],
                  probe_request_id=receipt["probe_request_id"], probe_sequence=1)
    assert h.call("execution", "ping", {"read_rotation": commit})["ok"] is True
    original_check = h.namespace["_execution_continuity_clear"]
    checks = []

    def fail_after_activation():
        checks.append(True)
        if read.rotation_probe_request_id is None:
            with ThreadPoolExecutor(1) as pool:
                pool.submit(h.namespace["deal_callback"], h.context, {}).result(timeout=1)
        return original_check()

    h.namespace["_execution_continuity_clear"] = fail_after_activation
    activate = dict(contract="READ_ROTATION_CONTROL_V1", action="ACTIVATE", transition_id=receipt["transition_id"],
                    nonce="3" * 32, capability="3" * 64, token=receipt["token"])
    assert h.call("execution", "ping", {"read_rotation": activate})["ok"] is False
    assert h.namespace["_READ_ROTATION"]["state"] == "ACTIVATE_UNKNOWN"
    assert read.rotation_probe_request_id == receipt["probe_request_id"]
    assert execution.bridge._off_thread_event_loss.is_set() and bytes(old_state.data) == old_frame
    old_client.close()
    h.close()


def test_gap_after_activation_revokes_read_availability_and_preserves_original_receipt(monkeypatch, tmp_path):
    pipe = FixtureSupervisorAuthorizationPipe()
    h, read, execution, old_client, old_state, old_frame, identity_path, extra_states, receipt = _prepared_read_rotation(
        monkeypatch, tmp_path, async_pipe=pipe, target_guard=True)
    assert h.call("read", "ping", request_id=receipt["probe_request_id"])["ok"] is True
    commit = dict(contract="READ_ROTATION_CONTROL_V1", action="COMMIT", transition_id=receipt["transition_id"],
                  nonce="2" * 32, capability="2" * 64, token=receipt["token"],
                  probe_request_id=receipt["probe_request_id"], probe_sequence=1)
    assert h.call("execution", "ping", {"read_rotation": commit})["data"]["read_rotation_control"]["status"] == "AUTH_PENDING"
    assert _rotation_control_status(h, "COMMIT", receipt["transition_id"])["status"] == "COMMITTED"
    activate = dict(contract="READ_ROTATION_CONTROL_V1", action="ACTIVATE", transition_id=receipt["transition_id"],
                    nonce="3" * 32, capability="3" * 64, token=receipt["token"])
    assert h.call("execution", "ping", {"read_rotation": activate})["data"]["read_rotation_control"]["status"] == "AUTH_PENDING"
    assert _rotation_control_status(h, "ACTIVATE", receipt["transition_id"])["status"] == "ACTIVATED"
    with ThreadPoolExecutor(1) as pool:
        pool.submit(h.namespace["deal_callback"], h.context, {}).result(timeout=1)
    outcome = _rotation_control_status(h, "ACTIVATE", receipt["transition_id"])
    assert outcome["status"] == "UNKNOWN" and outcome["receipt"] is None and outcome["automatic_retry"] is False
    job = h.namespace["_ROTATION_CONTROL_JOBS"][(receipt["transition_id"], "ACTIVATE")]
    assert job["prior_receipt"]["status"] == "ACTIVATED"
    assert h.namespace["_READ_ROTATION"]["state"] == "CONTINUITY_UNKNOWN"
    assert read.rotation_probe_request_id == receipt["probe_request_id"]
    assert execution.bridge.runtime_metadata["read_rotation_continuity"] == "UNKNOWN"
    assert execution.bridge._lost_sequence > 0 and execution.bridge._async_write_block_reason
    assert bytes(old_state.data) == old_frame
    assert execution.bridge.mailbox.session_id == receipt["old_evidence"]["execution_session_id"]
    old_client.close()
    h.close()


def _activated_rotation_for_read_guard(monkeypatch, tmp_path):
    pipe = FixtureSupervisorAuthorizationPipe()
    h, read, execution, old_client, old_state, old_frame, _, _, receipt = _prepared_read_rotation(
        monkeypatch, tmp_path, async_pipe=pipe, target_guard=True)
    bridge = execution.bridge
    # 此测试只注入换代授权，不能把写门或真实回调线程验证标为已就绪。
    bridge.runtime_metadata.update(native_write_runtime_guard_ready=False, native_callback_threading_verified=False)
    assert h.call("read", "ping", request_id=receipt["probe_request_id"])["ok"] is True
    commit = dict(contract="READ_ROTATION_CONTROL_V1", action="COMMIT", transition_id=receipt["transition_id"],
                  nonce="2" * 32, capability="2" * 64, token=receipt["token"],
                  probe_request_id=receipt["probe_request_id"], probe_sequence=1)
    assert h.call("execution", "ping", {"read_rotation": commit})["ok"] is True
    assert _rotation_control_status(h, "COMMIT", receipt["transition_id"])["status"] == "COMMITTED"
    activate = dict(contract="READ_ROTATION_CONTROL_V1", action="ACTIVATE", transition_id=receipt["transition_id"],
                    nonce="3" * 32, capability="3" * 64, token=receipt["token"])
    assert h.call("execution", "ping", {"read_rotation": activate})["ok"] is True
    assert _rotation_control_status(h, "ACTIVATE", receipt["transition_id"])["status"] == "ACTIVATED"
    return h, read, bridge, old_client, old_state, old_frame


@pytest.mark.parametrize("moment", ["after_budget", "during_native_read", "before_response_write",
                                    "after_response_write", "restore_failure"])
def test_rotated_business_read_never_reports_success_when_callback_gap_precedes_publication(monkeypatch, tmp_path, moment):
    h, read, bridge, old_client, old_state, old_frame = _activated_rotation_for_read_guard(monkeypatch, tmp_path)
    execution_sid = bridge.mailbox.session_id
    previous_response = read.mailbox.last_response_frame[1]
    injected, api_calls = [], []

    def inject_gap():
        assert not injected
        injected.append(True)
        with ThreadPoolExecutor(1) as pool:
            assert pool.submit(h.namespace["deal_callback"], h.context, {}).result(timeout=1) is None
        assert bridge._off_thread_event_loss.is_set()

    original_budget = h.namespace["_within_budget"]

    def budget(context):
        allowed = original_budget(context)
        if moment == "after_budget" and allowed and not injected:
            inject_gap()
        return allowed

    h.namespace["_within_budget"] = budget
    original_quote = h.context.get_full_tick

    def quote(codes):
        api_calls.append(list(codes))
        if moment == "during_native_read":
            inject_gap()
        return original_quote(codes)

    h.context.get_full_tick = quote
    original_write = read.mailbox.region.write

    def write(offset, raw):
        is_response = offset == h.transports["read"].RESPONSE_OFFSET
        if is_response and moment == "restore_failure" and injected:
            raise OSError("fixture response restore failed")
        if is_response and moment == "before_response_write" and not injected:
            inject_gap()
        result = original_write(offset, raw)
        if is_response and moment in ("after_response_write", "restore_failure") and not injected:
            inject_gap()
        return result

    read.mailbox.region.write = write
    with pytest.raises(h.transports["read"].ShmTimeoutError):
        h.call("read", "get_full_tick", {"codes": [CODE]}, request_id="gap-business-read")
    assert injected == [True]
    assert api_calls == ([] if moment == "after_budget" else [[CODE]])
    assert read.mailbox._header()[4:7] == [2, 1, 1]
    if moment != "restore_failure":
        assert read.mailbox.region.read(h.transports["read"].RESPONSE_OFFSET, len(previous_response)) == previous_response
    assert read.failed and h.clients["read"].poisoned
    if moment != "after_budget":
        assert read.pending[1]["ok"] is True  # 原生调用已经返回，不伪称该调用被取消。
    with pytest.raises(h.transports["read"].ShmProtocolError):
        h.clients["read"].send_request(dict(schema_version=1, request_id="retry-forbidden", account_id=ACCOUNT,
                                          method="get_full_tick", params={"codes": [CODE]}), 0.01)
    h.namespace["aqp_shm_rpc_adjust"](h.context)
    assert bridge._lost_sequence > 0 and bridge._async_write_block_reason
    assert bytes(old_state.data) == old_frame and bridge.mailbox.session_id == execution_sid
    assert bridge.runtime_metadata["native_write_runtime_guard_ready"] is False
    assert bridge.runtime_metadata["native_callback_threading_verified"] is False
    old_client.close()
    h.close()


@pytest.mark.parametrize("moment", ["same_thread_reentry", "during_header_publication", "after_publication"])
def test_rotated_read_reentry_and_later_gap_have_explicit_order(monkeypatch, tmp_path, moment):
    h, read, bridge, old_client, old_state, old_frame = _activated_rotation_for_read_guard(monkeypatch, tmp_path)
    calls = []
    original_quote = h.context.get_full_tick
    original_header_write = read.mailbox._write_header
    callback_started = threading.Event()
    callback_pool = ThreadPoolExecutor(1)
    callbacks = []

    def quote(codes):
        calls.append(list(codes))
        if moment == "same_thread_reentry":
            h.namespace["deal_callback"](h.context, dict(m_strAccountID=ACCOUNT,
                m_strOrderSysID="FIXTURE-ONLY", m_strTradeID="same-thread"))
        return original_quote(codes)

    def concurrent_callback():
        callback_started.set()
        return h.namespace["deal_callback"](h.context, {})

    def write_header(header):
        result = original_header_write(header)
        if moment == "during_header_publication" and header[4:7] == [2, 2, 1] and not callbacks:
            callbacks.append(callback_pool.submit(concurrent_callback))
            assert callback_started.wait(1)
            # 回调入口已到达，但短临界区尚未提交；不在发布线程等待回调结束。
            assert not callbacks[0].done() and not bridge._off_thread_event_loss.is_set()
        return result

    h.context.get_full_tick = quote
    read.mailbox._write_header = write_header
    result = h.call("read", "get_full_tick", {"codes": [CODE]}, request_id="ordered-read")
    assert result["ok"] is True and calls == [[CODE]]
    if moment == "same_thread_reentry":
        assert bridge._event_sequence == 1 and bridge._lost_sequence == 0
        assert not bridge._off_thread_event_loss.is_set()
    else:
        if moment == "during_header_publication":
            assert len(callbacks) == 1 and callbacks[0].result(timeout=1) is None
        else:
            assert callback_pool.submit(concurrent_callback).result(timeout=1) is None
        assert bridge._off_thread_event_loss.is_set()
        h.namespace["aqp_shm_rpc_adjust"](h.context)
        with pytest.raises(h.transports["read"].ShmTimeoutError):
            h.call("read", "get_full_tick", {"codes": [CODE]}, request_id="later-read")
        assert calls == [[CODE]] and bridge._lost_sequence > 0
    assert bytes(old_state.data) == old_frame
    assert bridge.runtime_metadata["native_write_runtime_guard_ready"] is False
    assert bridge.runtime_metadata["native_callback_threading_verified"] is False
    callback_pool.shutdown(wait=True)
    old_client.close()
    h.close()


def test_rotated_read_same_thread_reentrant_event_loss_never_confirms(monkeypatch, tmp_path):
    h, read, bridge, old_client, old_state, old_frame = _activated_rotation_for_read_guard(monkeypatch, tmp_path)
    original_quote, calls = h.context.get_full_tick, []

    def quote(codes):
        calls.append(list(codes))
        h.namespace["deal_callback"](h.context, dict(m_strAccountID="wrong-account",
            m_strOrderSysID="FIXTURE-ONLY", m_strTradeID="same-thread-loss"))
        assert bridge._lost_sequence > 0
        return original_quote(codes)

    h.context.get_full_tick = quote
    with pytest.raises(h.transports["read"].ShmTimeoutError):
        h.call("read", "get_full_tick", {"codes": [CODE]}, request_id="reentrant-gap")
    assert calls == [[CODE]] and read.pending[1]["ok"] is True and read.failed
    assert read.mailbox._header()[4:7] == [2, 1, 1] and bytes(old_state.data) == old_frame
    assert bridge.runtime_metadata["native_write_runtime_guard_ready"] is False
    assert bridge.runtime_metadata["native_callback_threading_verified"] is False
    old_client.close()
    h.close()


@pytest.mark.parametrize("written", [False, True])
def test_rotated_read_header_write_exception_preserves_ambiguous_outcome(monkeypatch, tmp_path, written):
    h, read, bridge, old_client, old_state, old_frame = _activated_rotation_for_read_guard(monkeypatch, tmp_path)
    original_header_write = read.mailbox._write_header
    attempts = []

    def write_header(header):
        if header[4:7] == [2, 2, 1]:
            attempts.append(True)
            if written:
                original_header_write(header)
            raise OSError("fixture header write outcome unknown")
        return original_header_write(header)

    read.mailbox._write_header = write_header
    if written:
        # 写入后抛错的替身证明调用方仍可能看到成功，不能宣称一定未发布。
        assert h.call("read", "get_full_tick", {"codes": [CODE]}, request_id="header-unknown")["ok"] is True
    else:
        with pytest.raises(h.transports["read"].ShmTimeoutError):
            h.call("read", "get_full_tick", {"codes": [CODE]}, request_id="header-unknown")
    assert attempts == [True] and read.failed
    assert h.namespace["_READ_FAULT"]["status"] == "FAILED" and read.pending[1]["ok"] is True
    assert bytes(old_state.data) == old_frame and not bridge._off_thread_event_loss.is_set()
    old_client.close()
    h.close()


def test_read_rotation_commit_unknown_keeps_both_mailboxes_and_never_retries(monkeypatch, tmp_path):
    h, read, execution, old_client, old_state, old_frame, identity_path, extra_states, receipt = _prepared_read_rotation(monkeypatch, tmp_path)
    probe = h.call("read", "ping", request_id=receipt["probe_request_id"])
    assert probe["ok"] and probe["sequence"] == 1
    commit = dict(contract="READ_ROTATION_CONTROL_V1", action="COMMIT", transition_id=receipt["transition_id"],
                  nonce="2" * 32, capability="2" * 64,
                  token=receipt["token"], probe_request_id=receipt["probe_request_id"], probe_sequence=1)
    replace = os.replace

    def ambiguous_replace(source, destination):
        replace(source, destination)
        raise OSError("fixture commit result unknown")

    with mock.patch.object(os, "replace", side_effect=ambiguous_replace) as replace_call:
        result = h.call("execution", "ping", {"read_rotation": commit})
        assert not result["ok"]
        assert h.namespace["_READ_ROTATION"]["state"] == "COMMIT_UNKNOWN"
        assert not h.call("execution", "ping", {"read_rotation": commit})["ok"]
        replace_call.assert_called_once()
    assert json.loads(identity_path.read_text())["read_session_id"] == receipt["candidate_read_session_id"]
    assert bytes(old_state.data) == old_frame and not old_state.regions[0].closed
    assert read.rotation_probe_request_id == receipt["probe_request_id"]
    assert not execution.closed and not h.namespace["_CLOSED"]
    old_client.close()
    h.close()


def test_read_rotation_commit_lock_failure_stays_pending_and_is_not_retried(monkeypatch, tmp_path):
    h, read, execution, old_client, old_state, old_frame, identity_path, extra_states, receipt = _prepared_read_rotation(monkeypatch, tmp_path)
    assert h.call("read", "ping", request_id=receipt["probe_request_id"])["ok"]
    commit = dict(contract="READ_ROTATION_CONTROL_V1", action="COMMIT", transition_id=receipt["transition_id"],
                  nonce="2" * 32, capability="2" * 64,
                  token=receipt["token"], probe_request_id=receipt["probe_request_id"], probe_sequence=1)
    old_state.lock.acquire()
    try:
        assert not h.call("execution", "ping", {"read_rotation": commit})["ok"]
    finally:
        old_state.lock.release()
    assert h.namespace["_READ_ROTATION"]["state"] == "COMMIT_PENDING"
    assert not h.call("execution", "ping", {"read_rotation": commit})["ok"]
    assert json.loads(identity_path.read_text())["read_session_id"] == receipt["old_evidence"]["read_session_id"]
    assert bytes(old_state.data) == old_frame and not execution.closed
    old_client.close()
    h.close()


def test_failed_response_publication_keeps_request_and_pending_result(monkeypatch):
    h = Fixture(monkeypatch, **persistent_settings())
    h.start()
    read = h.namespace["_RUNTIMES"]["read"]["_SERVER"]
    request = dict(schema_version=1, request_id="late-unpublished", account_id=ACCOUNT,
                   method="get_positions", params={})
    with pytest.raises(h.transports["read"].ShmTimeoutError):
        h.clients["read"].send_request(request, 0.01)
    before = bytes(h.states["read"].data)
    with mock.patch.object(read.mailbox, "publish_response", side_effect=OSError("fixture publish failure")) as publish:
        h.namespace["aqp_shm_readonly_adjust"](h.context)
        h.namespace["aqp_shm_readonly_adjust"](h.context)
        publish.assert_called_once()
    assert bytes(h.states["read"].data) == before
    assert read.mailbox._header()[4:7] == [1, 0, 0]
    assert read.pending[0] == 1 and read.pending[1]["request_id"] == "late-unpublished"
    assert read.pending[1]["server_error"] == "READ_EXPIRED"
    assert not h.query_calls and not read.closed
    assert h.call("execution", "ping")["ok"] is True
    h.close()


def test_execution_failure_remains_a_dual_source_stop(monkeypatch):
    h = Fixture(monkeypatch, **persistent_settings())
    h.start()
    execution = h.namespace["_RUNTIMES"]["execution"]["_SERVER"]
    read = h.namespace["_RUNTIMES"]["read"]["_SERVER"]
    execution.close("fixture execution failure")
    h.namespace["aqp_shm_rpc_adjust"](h.context)
    assert h.namespace["_CLOSED"] and read.closed
    h.close()


def test_closed_read_source_does_not_stop_execution_callbacks(monkeypatch):
    h = Fixture(monkeypatch, **persistent_settings())
    h.start()
    read = h.namespace["_RUNTIMES"]["read"]["_SERVER"]
    execution = h.namespace["_RUNTIMES"]["execution"]["_SERVER"]
    sid = execution.bridge.mailbox.session_id
    read.close("fixture read closure")
    h.namespace["order_callback"](h.context, dict(m_strAccountID=ACCOUNT, m_strOrderSysID="after-read-close"))
    result = h.call("execution", "poll_execution_events", {"after_sequence": 0})["data"]
    assert not h.namespace["_CLOSED"] and not execution.closed
    assert result["session_id"] == sid and result["gap"] is False
    assert len(result["events"]) == 1 and result["events"][0]["event"]["order_sys_id"] == "after-read-close"
    assert h.call("execution", "ping")["data"]["native_read_source_state"] == "RESULT_UNKNOWN"
    h.close()


def test_dual_source_refuses_disabled_failure_evidence_before_binding_account(monkeypatch):
    h = Fixture(monkeypatch, **persistent_settings())
    h.namespace["_RUNTIMES"]["read"]["READONLY_PRESERVE_FAILED_MAILBOX"] = False
    with pytest.raises(RuntimeError, match="readonly source runtime policy changed"):
        h.start()
    assert h.context.accounts == []
    assert h.namespace["_CLOSED"]
    h.writes.assert_not_called()


def test_unexpected_read_callback_failure_is_not_retried_or_promoted(monkeypatch):
    h = Fixture(monkeypatch, **persistent_settings())
    h.start()
    runtime = h.namespace["_RUNTIMES"]["read"]
    callback = mock.Mock(side_effect=RuntimeError("fixture callback failure"))
    runtime["aqp_shm_readonly_adjust"] = callback
    h.namespace["aqp_shm_readonly_adjust"](h.context)
    h.namespace["aqp_shm_readonly_adjust"](h.context)
    callback.assert_called_once()
    assert h.namespace["_READ_FAULT"]["status"] == "FAILED"
    assert not h.namespace["_CLOSED"]
    assert h.call("execution", "ping")["ok"] is True
    h.close()


def test_generated_python36_ascii_gbk_and_no_native_redis_or_write_bindings():
    source = BUILDER.build_source(**settings())
    tree = ast.parse(source, feature_version=(3, 6))
    source.encode("ascii").decode("gbk")
    payload_node = next(node for node in tree.body if isinstance(node, ast.Assign) and isinstance(node.targets[0], ast.Name) and node.targets[0].id == "_SOURCE_PAYLOADS")
    payloads = ast.literal_eval(payload_node.value)
    assert set(payloads) == {"read", "execution", "native_guard"}
    for payload in payloads.values():
        inner = base64.b64decode(payload).decode("utf-8")
        ast.parse(inner, feature_version=(3, 6))
    namespace = {}
    exec(compile(source, "<default-dual-source>", "exec"), namespace)
    execution = namespace["_RUNTIMES"]["execution"]
    assert execution["SHM_RPC_ALLOW_ORDER_METHODS"] is False
    assert execution["SHM_RPC_ORDER_GATEWAY"] is execution["SHM_RPC_ORDER_GATEWAY_FACTORY"] is None
    assert namespace["DUAL_SHM_SETTINGS"]["persistent_simulation_read_write"] is False


@pytest.mark.parametrize("changes", [
    {"account_id": "8882152350"}, {"native_pid": True}, {"windows_session": -1},
    {"hard_deadline_unix": float("nan")}, {"max_seconds": 901}, {"max_requests": 1001},
    {"enable_market_rpc": 1}, {"symbols": [CODE, CODE]}, {"read_name": "Global\\arbitrary"},
])
def test_builder_rejects_unauthorized_or_unbounded_inputs(changes):
    with pytest.raises(ValueError):
        BUILDER.build_source(**settings(**changes))


def test_cli_only_writes_new_private_candidate_without_starting_source(tmp_path):
    path = tmp_path / "candidate.py"
    args = [sys.executable, str(ROOT / "tools/build_dual_shm_single_file.py"), "--output", str(path),
        "--native-pid", str(os.getpid()), "--windows-session", "1", "--hard-deadline-unix", str(time.time()+600),
        "--read-name", "Local\\AQP_SHM_READONLY_CLI_FIXTURE", "--execution-name", "Local\\AQP_SHM_TRADE_CLI_FIXTURE", "--symbols", CODE]
    first = subprocess.run(args, text=True, capture_output=True, timeout=15)
    assert first.returncode == 0 and "no native process started" in first.stdout
    assert path.stat().st_mode & 0o777 == 0o600
    second = subprocess.run(args, text=True, capture_output=True, timeout=15)
    assert second.returncode != 0 and "FileExistsError" in second.stderr


def persistent_settings(**changes):
    values = dict(persistent_readonly=True, hard_deadline_unix=None, max_seconds=0, max_requests=0,
                  read_fault_isolation=True)
    values.update(changes)
    return values


def test_persistent_generated_native_runtime_has_truthful_policy_and_binds_only_once(monkeypatch, capsys):
    h = Fixture(monkeypatch, **persistent_settings())
    h.start()
    lines = capsys.readouterr().out.splitlines()
    read_ready = json.loads(next(line.split(" ", 1)[1] for line in lines if line.startswith("AQP_SHM_READONLY_READY ")))
    execution_ready = json.loads(next(line.split(" ", 1)[1] for line in lines if line.startswith("AQP_SHM_RPC_READY ")))
    combined = json.loads(next(line.split(" ", 1)[1] for line in lines if line.startswith("AQP_DUAL_SHM_READY ")))
    for proof in (read_ready, execution_ready, combined):
        assert proof["runtime_policy"] == "PERSISTENT_READONLY"
        assert proof["started_at_unix"] > 0
        assert proof["source_deadline_unix"] is None
        assert proof["max_seconds"] == 0
    assert read_ready["max_requests"] == execution_ready["max_requests"] == combined["max_requests_per_source"] == 0
    assert combined["account_bound_once"] is True and execution_ready["bind_account_callbacks"] is False
    assert combined["account_callback_route"] == "outer_to_execution" and combined["execution_bind_account_callbacks"] is False
    assert combined["allow_order_methods"] is execution_ready["allow_order_methods"] is read_ready["allow_order_methods"] is False
    assert h.context.accounts == [ACCOUNT]
    for key in ("read", "execution"):
        assert h.call(key, "ping")["data"]["allow_order_methods"] is False
        assert h.call(key, "get_positions")["data"][CODE]["volume"] == 100
    h.close()


def test_persistent_runtime_continues_after_old_total_time_and_request_limits(monkeypatch):
    h = Fixture(monkeypatch, **persistent_settings())
    h.start()
    read = h.namespace["_RUNTIMES"]["read"]["_SERVER"]
    execution = h.namespace["_RUNTIMES"]["execution"]["_SERVER"]
    read.created_at = execution.created_at = time.monotonic() - 100000
    read.request_count = execution.requests = 5000
    h.namespace["_STARTED_AT"] = time.monotonic() - 100000
    for key in ("read", "execution"):
        assert h.call(key, "get_positions")["ok"] is True
        assert h.call(key, "submit_order")["ok"] is False
    assert not h.namespace["_CLOSED"] and not read.closed and not execution.closed
    h.writes.assert_not_called()
    h.close()


def test_persistent_read_keeps_original_per_request_deadline_and_identity_gates(monkeypatch):
    h = Fixture(monkeypatch, **persistent_settings())
    h.start()
    read = h.namespace["_RUNTIMES"]["read"]["_SERVER"]
    request = dict(schema_version=1, request_id="expired-persistent-read", account_id=ACCOUNT,
                   method="get_positions", params={}, sequence=1, session_id=read.mailbox.session_id,
                   read_expires_at_unix=time.time() - 1)
    response = read.process_request(request, sequence=1)
    assert response["ok"] is False and response["server_error"] == "READ_EXPIRED"
    assert not h.query_calls and not read.closed
    with pytest.raises(Exception, match="session_id mismatch"):
        h.call("execution", "get_positions", session_id="old-closed-sid")
    assert not h.query_calls
    assert h.call("execution", "get_asset")["ok"] is True
    h.close()


@pytest.mark.parametrize("changes", [
    {"hard_deadline_unix": 9999999999}, {"max_seconds": 900}, {"max_requests": 1000},
    {"enable_market_rpc": True}, {"persistent_readonly": 1}, {"max_seconds": False},
    {"max_requests": False}, {"account_id": "8882152350"},
])
def test_persistent_builder_rejects_inferred_budgets_market_and_other_accounts(changes):
    values = persistent_settings()
    values.update(changes)
    with pytest.raises(ValueError):
        BUILDER.build_source(**settings(**values))


def test_persistent_runtime_tampered_gateway_factory_refused_before_account_binding(monkeypatch):
    h = Fixture(monkeypatch, **persistent_settings())
    factory = mock.Mock(side_effect=AssertionError("must not construct a write gateway"))
    h.namespace["_RUNTIMES"]["execution"]["SHM_RPC_ORDER_GATEWAY_FACTORY"] = factory
    with pytest.raises(RuntimeError, match="readonly execution source gate changed"):
        h.namespace["init"](h.context)
    assert h.context.accounts == []
    factory.assert_not_called()
    h.writes.assert_not_called()


def test_explicit_persistent_cli_requires_zero_budgets_and_generates_no_fake_deadline(tmp_path):
    path = tmp_path / "persistent.py"
    args = [sys.executable, str(ROOT / "tools/build_dual_shm_single_file.py"), "--output", str(path),
            "--native-pid", str(os.getpid()), "--windows-session", "1", "--read-name", "Local\\AQP_SHM_READONLY_RESIDENT_TEST",
            "--execution-name", "Local\\AQP_SHM_TRADE_RESIDENT_TEST", "--symbols", CODE,
            "--persistent-readonly", "--max-seconds", "0", "--max-requests", "0"]
    result = subprocess.run(args, text=True, capture_output=True, timeout=15)
    assert result.returncode == 0, result.stderr
    tree = ast.parse(path.read_text(encoding="utf-8"), feature_version=(3, 6))
    node = next(node for node in tree.body if isinstance(node, ast.Assign) and node.targets[0].id == "DUAL_SHM_SETTINGS")
    policy = ast.literal_eval(node.value)
    assert policy["persistent_readonly"] is True and policy["hard_deadline_unix"] is None
    assert policy["max_seconds"] == policy["max_requests_per_source"] == 0


def test_persistent_actual_sources_accept_more_than_1024_reads_and_expire_only_confirmed_ids(monkeypatch):
    h = Fixture(monkeypatch, **persistent_settings())
    h.start()
    clock = [time.time()]
    with mock.patch("time.time", side_effect=lambda: clock[0]):
        for index in range(1026):
            for key in ("read", "execution"):
                assert h.call(key, "get_positions")["ok"] is True
            clock[0] += 0.1
        read = h.namespace["_RUNTIMES"]["read"]["_SERVER"]
        execution = h.namespace["_RUNTIMES"]["execution"]["_SERVER"]
        assert read.request_count == execution.requests == 1026
        assert len(read.seen_request_ids) <= 12
        assert len(execution.bridge._replays) <= 12
        assert not read.closed and not execution.closed and not h.namespace["_CLOSED"]
        clock[0] += 2
        h.namespace["aqp_shm_readonly_adjust"](h.context)
        h.namespace["aqp_shm_rpc_adjust"](h.context)
        assert read.seen_request_ids == {} and execution.bridge._replays == {}
        assert execution.bridge._read_replay_expiries == {}
    h.close()


def test_persistent_unconfirmed_or_pending_replay_is_not_expired_or_executed_again(monkeypatch):
    h = Fixture(monkeypatch, **persistent_settings())
    h.start()
    read = h.namespace["_RUNTIMES"]["read"]["_SERVER"]
    bridge = h.namespace["_RUNTIMES"]["execution"]["_SERVER"].bridge
    read.seen_request_ids["unconfirmed"] = (time.time() - 1, 1)
    bridge._replays["unconfirmed"] = ("known", {"ok": True})
    bridge._read_replay_expiries["unconfirmed"] = (time.time() - 1, 1)
    # 两邮箱初始confirmed=0；未确认sequence1不能因时间过去被回收。
    h.namespace["aqp_shm_readonly_adjust"](h.context)
    bridge._expire_confirmed_read_replays()
    assert "unconfirmed" in read.seen_request_ids and "unconfirmed" in bridge._replays
    pending_request = {"request_id": "pending"}
    bridge._pending = (1, pending_request, {}, "known", None)
    bridge._replays["pending"] = ("known", {"ok": True})
    bridge._read_replay_expiries["pending"] = (time.time() - 1, 0)
    bridge._expire_confirmed_read_replays()
    assert "pending" in bridge._replays
    assert h.query_calls == []
    bridge._pending = None
    h.close()


def test_default_execution_replay_capacity_and_read_seen_set_remain_unchanged(monkeypatch):
    h = Fixture(monkeypatch)
    h.start()
    read = h.namespace["_RUNTIMES"]["read"]["_SERVER"]
    bridge = h.namespace["_RUNTIMES"]["execution"]["_SERVER"].bridge
    assert isinstance(read.seen_request_ids, set)
    assert bridge.persistent_readonly is False and bridge._replay_capacity == 1024
    bridge._replays["old"] = ("known", {"ok": True})
    bridge._read_replay_expiries["old"] = (time.time() - 1, 0)
    bridge._expire_confirmed_read_replays()
    assert "old" in bridge._replays
    h.close()


@pytest.mark.parametrize("account,allow", [(ACCOUNT, True), ("8882152350", False)])
def test_shared_bridge_persistent_mode_rejects_writes_and_foreign_accounts_before_mailbox(monkeypatch, account, allow):
    h = Fixture(monkeypatch, **persistent_settings())
    bridge_class = h.namespace["_RUNTIMES"]["execution"]["_load_embedded"]("bigqmt_signal_trader.shm_rpc_bridge").ShmRpcBridge
    handlers = types.SimpleNamespace(account_id=account, allow_order_methods=allow, settle_orders_inline=False)
    with pytest.raises(ValueError, match="fixed simulation and writes disabled"):
        bridge_class(name="Local\\AQP_SHM_TRADE_FORBIDDEN", account_id=account, handlers=handlers,
                     allow_order_methods=allow, persistent_readonly=True)
    assert all(not state.regions for state in h.states.values())


def read_write_settings(**changes):
    values = dict(persistent_simulation_read_write=True, hard_deadline_unix=None,
                  max_seconds=0, max_requests=0, execution_event_guard=True, read_fault_isolation=True)
    values.update(changes)
    return values


def _enable_fixture_simulation_orders(h, fail_submit=False):
    orders = []
    ordinary_query = h.query

    def query(account, account_type, kind, strategy_name=""):
        if kind == "ORDER":
            h.query_calls.append((account, account_type, kind, strategy_name))
            assert (account, account_type) == (h.account, "STOCK")
            return orders
        return ordinary_query(account, account_type, kind, strategy_name)

    def submit(action, order_type, account, code, price_type, price, volume, strategy, quick, remark, context):
        assert (action, order_type, account, code, price_type, volume, quick, context) == (
            23, 1101, h.account, CODE, 11, 100, 2, h.context)
        if fail_submit:
            raise OSError("fixture native result unconfirmed after call")
        orders.append(types.SimpleNamespace(m_strAccountID=h.account, m_strInstrumentID="000001",
            m_strExchangeID="SZ", m_nVolumeTotalOriginal=volume, m_nVolumeTraded=0,
            m_nOrderStatus=50, m_nOffsetFlag=48, m_dLimitPrice=price, m_strOrderSysID="SIM-FIXTURE-001",
            m_strRemark=remark, m_strStrategyName=strategy, m_nOpType=23, m_nOrderPriceType=50,
            m_dTradedPrice=0.0, m_strInsertDate="20261016", m_strInsertTime="093105"))

    submit_mock, cancel_mock = mock.Mock(side_effect=submit), mock.Mock(return_value=True)
    h.namespace.update(get_trade_detail_data=query, passorder=submit_mock, cancel=cancel_mock)
    return orders, submit_mock, cancel_mock


def _fixture_native_login(h):
    # 只注入原生回调输入，执行实际生成的登录解析与写门禁；非真实QMT登录证明。
    row = dict(m_strAccountID=h.account, m_strStatus="登录成功", m_nStatus=0)
    if h.settings.get("environment") == "LIVE":
        row.update(m_nBrokerType=2, m_strAccountKey="LIVE-STOCK-FIXTURE-ONLY")
    h.namespace["account_callback"](h.context, row)


def test_write_guard_requires_explicit_zero_cursor_poll_before_native_write(monkeypatch):
    h = Fixture(monkeypatch, **read_write_settings())
    _, submit, cancel = _enable_fixture_simulation_orders(h)
    h.start()
    _fixture_native_login(h)
    bridge = h.namespace["_RUNTIMES"]["execution"]["_SERVER"].bridge
    ping = h.call("execution", "ping")["data"]
    assert ping["execution_event_guard_contract"] == "EXECUTION_CURSOR_WRITE_GATE_V1"
    guard = dict(contract="EXECUTION_CURSOR_WRITE_GATE_V1", execution_session_id=bridge.mailbox.session_id,
                 native_pid=bridge.mailbox.server_pid, sequence=0)
    params = dict(stock_code=CODE, action="BUY", volume=100, price=10.5, client_order_id="no-zero-poll",
                  execution_event_guard=guard)
    result = h.call("execution", "submit_order_async", params, request_id="no-zero-poll", _fixture_guard=False)
    data = result["data"]
    assert data["status"] == "BLOCKED" and data["native_write_executed"] is False
    assert data["no_write_contract"] == "EXECUTION_CURSOR_NO_WRITE_V1"
    assert data["execution_event_guard"] == guard and data["native_event_sequence"] == data["native_lost_sequence"] == 0
    assert data["request_id"] == "no-zero-poll" and data["automatic_retry"] is False
    submit.assert_not_called()
    cancel.assert_not_called()
    h.close()


def test_target_native_denies_direct_write_without_verified_supervisor_gate(monkeypatch):
    h = Fixture(monkeypatch, **read_write_settings())
    _, submit, cancel = _enable_fixture_simulation_orders(h)
    h.start(enable_fixture_write_gate=False)
    _fixture_native_login(h)
    bridge = h.namespace["_RUNTIMES"]["execution"]["_SERVER"].bridge
    ping = h.call("execution", "ping")["data"]
    assert ping["native_write_runtime_guard_ready"] is False
    assert "supervisor_write_gate_contract" not in ping
    assert h.call("execution", "poll_execution_events", {"after_sequence": 0})["data"]["gap"] is False
    guard = dict(contract="EXECUTION_CURSOR_WRITE_GATE_V1", execution_session_id=bridge.mailbox.session_id,
                 native_pid=bridge.mailbox.server_pid, sequence=0)
    params = dict(stock_code=CODE, action="BUY", volume=100, price=10.5,
                  client_order_id="no-supervisor-gate", execution_event_guard=guard)
    data = h.call("execution", "submit_order_async", params, request_id="no-supervisor-gate", _fixture_guard=False)["data"]
    assert data["status"] == "BLOCKED" and data["native_write_executed"] is False
    assert data["error"] == "SUPERVISOR_WRITE_GATE_UNAVAILABLE" and data["writes_blocked"] is True
    assert data["automatic_retry"] is False
    submit.assert_not_called()
    cancel.assert_not_called()
    h.close()


@pytest.mark.parametrize("field", ["native_write_runtime_guard_attached", "native_callback_threading_verified",
                                  "supervisor_write_gate_contract", "supervisor_gate_epoch_contract"])
def test_native_ready_bit_alone_cannot_bypass_callback_or_gate_contract(monkeypatch, field):
    h = Fixture(monkeypatch, **read_write_settings())
    _, submit, cancel = _enable_fixture_simulation_orders(h)
    h.start()  # Only this test injects a complete model attestation.
    bridge = h.namespace["_RUNTIMES"]["execution"]["_SERVER"].bridge
    bridge.runtime_metadata[field] = False
    assert h.call("execution", "poll_execution_events", {"after_sequence": 0})["data"]["gap"] is False
    guard = dict(contract="EXECUTION_CURSOR_WRITE_GATE_V1", execution_session_id=bridge.mailbox.session_id,
                 native_pid=bridge.mailbox.server_pid, sequence=0)
    params = dict(stock_code=CODE, action="BUY", volume=100, price=10.5, client_order_id="bad-attestation",
                  execution_event_guard=guard,
                  supervisor_gate_epoch=dict(contract="SUPERVISOR_GATE_EPOCH_V1", epoch=1))
    data = h.call("execution", "submit_order_async", params, request_id="bad-attestation", _fixture_guard=False)["data"]
    assert data["status"] == "BLOCKED" and data["native_write_executed"] is False
    assert data["error"] == "SUPERVISOR_WRITE_GATE_UNAVAILABLE"
    submit.assert_not_called()
    cancel.assert_not_called()
    h.close()


@pytest.mark.parametrize("failure", [None, "slow", "stop_pending", "peer_pid", "wrong_handle_rights", "wrong_receipt",
                                     "capability_bad_receipt"])
def test_generated_live_attach_checks_real_protocol_and_never_retries_unknown(monkeypatch, failure):
    h = Fixture(monkeypatch, **live_settings(account_id="8882152350", execution_event_guard=True,
                                             read_fault_isolation=True))
    h.start(enable_fixture_write_gate=False)
    identity = dict(pid=123, creation_filetime=456, windows_session_id=1)
    h.namespace["DUAL_SHM_SETTINGS"]["native_pid"] = 0
    h.namespace["_BOUND_NATIVE_CREATION_FILETIME"] = 789
    h.namespace["_native_creation_filetime"] = lambda: 789
    h.namespace["_verify_identity"] = lambda: os.getpid()
    h.namespace["_SUPERVISOR_PROCESS"] = object()
    h.namespace["_SUPERVISOR_IDENTITY"] = identity
    h.namespace["_supervisor_identity"] = lambda process: dict(identity)
    native = h.namespace["_NATIVE_GUARD"]
    response_enabled = threading.Event()

    class Kernel:
        def __init__(self):
            self.handles = {key: index + 10 for index, key in enumerate(native["_HANDLE_CONTRACT"])}
            self.types = {self.handles[key]: value for key, value in native["_HANDLE_CONTRACT"].items()}
            self.ack, self.writes, self.response = [], [], None
            self.closed, self.pipe_closed = [], threading.Event()
            if failure == "wrong_handle_rights":
                self.types[self.handles["AQP_WRITE_BLOCK_EVENT_HANDLE"]] = ("Event", 0x100002)

        def open_pipe(self, name):
            return 99

        def peer_pid(self, handle):
            return 124 if failure == "peer_pid" else 123

        def nonblocking_message_mode(self, handle):
            pass

        def write(self, handle, frame):
            self.writes.append(frame)
            request = json.loads(frame[4:].decode())
            if request.get("action") == "CAPABILITY_PROBE":
                response = dict(request, status="READY")
                if failure == "capability_bad_receipt":
                    response["read_session_id"] = "f" * 32
            else:
                response = dict(request, status="HANDLE_OFFER", guard_handles=self.handles)
                if failure == "wrong_receipt":
                    response["supervisor_creation_filetime"] += 1
            payload = json.dumps(response).encode()
            self.response = struct.pack("<I", len(payload)) + payload
            return len(frame)

        def read(self, handle):
            if failure in ("slow", "stop_pending") and not response_enabled.is_set():
                return None
            return self.response

        def process_identity(self, handle):
            return dict(identity)

        def object_identity(self, handle):
            return self.types[handle]

        def wait(self, handle, timeout):
            return 0 if handle == self.handles["AQP_WRITE_GATE_MUTEX_HANDLE"] else 258

        def release_mutex(self, handle):
            pass

        def set_event(self, handle):
            self.ack.append(handle)

        def map_epoch(self, handle):
            return 100

        def read_epoch(self, address):
            return 1

        def unmap_epoch(self, address):
            pass

        def close(self, handle):
            self.closed.append(handle)
            if handle == 99:
                self.pipe_closed.set()

    kernel = Kernel()
    native["WindowsNativeKernel"] = lambda: kernel
    command = {"contract": "QMT_NATIVE_SUPERVISOR_ATTACH_V1", "attach_id": "1" * 32}
    started = time.monotonic()
    response = h.call("execution", "ping", {"supervisor_attach": command}, request_id="attach-once")
    assert response["ok"] is True
    assert response["data"]["supervisor_attach"]["status"] == "PENDING"
    assert response["data"]["supervisor_attach"]["attach_id"] == command["attach_id"]
    assert time.monotonic() - started < 1
    if failure in ("slow", "stop_pending"):
        assert kernel.ack == []
        pending = h.call("execution", "ping", {"supervisor_attach_status": command})
        assert pending["data"]["supervisor_attach_status"]["status"] == "PENDING"
        count = len(kernel.writes)
        other = dict(command, attach_id="2" * 32)
        assert h.call("execution", "ping", {"supervisor_attach_status": other})["ok"] is False
        assert len(kernel.writes) == count and kernel.ack == []
        if failure == "stop_pending":
            h.close()
            assert h.namespace["_SUPERVISOR_CLEANUP_STATUS"]["status"] == "PENDING"
            assert h.namespace["_SUPERVISOR_CLEANUP_STATUS"]["background_cleanup_pending"] is True
            response_enabled.set()
            assert kernel.pipe_closed.wait(2)
            assert kernel.ack == [] and h.namespace["_SUPERVISOR_GATE"] is None
            assert set(kernel.closed) == set(kernel.handles.values()) | {99}
            return
        response_enabled.set()
    deadline = time.monotonic() + 2
    status = None
    while time.monotonic() < deadline:
        response = h.call("execution", "ping", {"supervisor_attach_status": command})
        status = response["data"]["supervisor_attach_status"]["status"]
        if status != "PENDING":
            break
        time.sleep(0.005)
    bridge = h.namespace["_RUNTIMES"]["execution"]["_SERVER"].bridge
    if failure in (None, "slow", "capability_bad_receipt"):
        assert status == "ATTACHED"
        assert response["ok"] is True
        data = response["data"]
        assert data["native_write_runtime_guard_attached"] is True
        assert data["native_write_runtime_guard_ready"] is False
        assert data["native_callback_threading_verified"] is False
        assert data["supervisor_write_gate_contract"] == "INHERITED_SUPERVISOR_GATE_V1"
        assert data["supervisor_gate_epoch_contract"] == "SUPERVISOR_GATE_EPOCH_V1"
        assert data["read_rotation_authorization_ready"] is False
        assert data["supervisor_attach_status"] == dict(contract="QMT_NATIVE_SUPERVISOR_ATTACH_V1", status="ATTACHED",
            attach_id=command["attach_id"], automatic_retry=False,
            native_pid=os.getpid(), native_creation_filetime=789, windows_session_id=1,
            execution_session_id=bridge.mailbox.session_id, supervisor_pid=123, supervisor_creation_filetime=456)
        assert kernel.ack == [kernel.handles["AQP_GUARD_ATTACH_ACK_HANDLE"]]
        capability = dict(contract="READ_ROTATION_CAPABILITY_V1", nonce="a" * 32)
        initial_capability = h.call("execution", "ping", {"read_rotation_capability": capability})["data"]["read_rotation_capability"]
        assert initial_capability == dict(capability, status="PENDING", automatic_retry=False)
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            observed = h.call("execution", "ping", {"read_rotation_capability_status": capability})["data"]["read_rotation_capability_status"]
            if observed["status"] != "PENDING":
                break
        if failure == "capability_bad_receipt":
            assert observed["status"] == "UNKNOWN" and observed["receipt"] is None
        else:
            assert observed["status"] == "READY" and observed["automatic_retry"] is False
            assert observed["receipt"] == dict(contract="QMT_NATIVE_ROTATION_PIPE_V1", action="CAPABILITY_PROBE",
                nonce=capability["nonce"], account_id=h.account, environment="LIVE", native_pid=os.getpid(),
                native_creation_filetime=789, windows_session_id=1, execution_session_id=bridge.mailbox.session_id,
                read_session_id=h.namespace["_RUNTIMES"]["read"]["_SERVER"].mailbox.session_id,
                supervisor_pid=123, supervisor_creation_filetime=456, status="READY")
        assert bridge.runtime_metadata["read_rotation_authorization_ready"] is False
        count = len(kernel.writes)
        assert h.call("execution", "ping", {"read_rotation_capability": capability})["ok"] is False
        assert len(kernel.writes) == count
        assert h.call("execution", "poll_execution_events", {"after_sequence": 0})["data"]["gap"] is False
        guard = dict(contract="EXECUTION_CURSOR_WRITE_GATE_V1", execution_session_id=bridge.mailbox.session_id,
                     native_pid=bridge.mailbox.server_pid, sequence=0)
        params = dict(stock_code=CODE, action="BUY", volume=100, price=10.5,
                      client_order_id="not-yet-attested", execution_event_guard=guard,
                      supervisor_gate_epoch=dict(contract="SUPERVISOR_GATE_EPOCH_V1", epoch=1))
        write = h.call("execution", "submit_order_async", params, request_id="not-yet-attested", _fixture_guard=False)
        assert write["data"]["status"] == "BLOCKED" and write["data"]["native_write_executed"] is False
        assert write["data"]["error"] == "SUPERVISOR_WRITE_GATE_UNAVAILABLE"
        h.writes.assert_not_called()
        if failure == "slow":
            with ThreadPoolExecutor(1) as pool:
                pool.submit(h.namespace["deal_callback"], h.context, {}).result(timeout=1)
            revoked = h.call("execution", "ping", {"read_rotation_capability_status": capability})["data"]["read_rotation_capability_status"]
            assert revoked["status"] == "REVOKED" and revoked["receipt"] is None
            assert bridge.runtime_metadata["read_rotation_authorization_ready"] is False
    else:
        assert status == "FAILED"
        assert bridge.runtime_metadata["native_write_runtime_guard_ready"] is False
        assert kernel.ack == []
    count = len(kernel.writes)
    second = h.call("execution", "ping", {"supervisor_attach": command}, request_id="attach-retry")
    assert second["ok"] is False and len(kernel.writes) == count
    assert h.namespace["_READ_ROTATION_AUTHORIZER"] is None
    h.close()


def test_stale_supervisor_epoch_blocks_before_native_write(monkeypatch):
    h = Fixture(monkeypatch, **read_write_settings())
    _, submit, cancel = _enable_fixture_simulation_orders(h)
    h.start()
    _fixture_native_login(h)
    bridge = h.namespace["_RUNTIMES"]["execution"]["_SERVER"].bridge
    assert h.call("execution", "poll_execution_events", {"after_sequence": 0})["data"]["gap"] is False
    guard = dict(contract="EXECUTION_CURSOR_WRITE_GATE_V1", execution_session_id=bridge.mailbox.session_id,
                 native_pid=bridge.mailbox.server_pid, sequence=0)

    class NewEpochLease(FixtureRuntimeWriteLease):
        def read_epoch(self):
            return 2

    bridge.native_write_runtime_guard = NewEpochLease
    epoch = dict(contract="SUPERVISOR_GATE_EPOCH_V1", epoch=1)
    params = dict(stock_code=CODE, action="BUY", volume=100, price=10.5,
                  client_order_id="old-epoch", execution_event_guard=guard, supervisor_gate_epoch=epoch)
    data = h.call("execution", "submit_order_async", params, request_id="old-epoch", _fixture_guard=False)["data"]
    assert data["status"] == "BLOCKED" and data["native_write_executed"] is False
    assert data["no_write_contract"] == "SUPERVISOR_GATE_NO_WRITE_V1"
    assert data["error"] == "SUPERVISOR_GATE_EPOCH_MISMATCH"
    assert data["supervisor_gate_epoch"] == epoch and data["native_supervisor_gate_epoch"] == 2
    assert data["request_id"] == "old-epoch" and data["account_id"] == ACCOUNT
    assert data["session_id"] == bridge.mailbox.session_id and data["native_pid"] == bridge.mailbox.server_pid
    assert data["automatic_retry"] is False
    submit.assert_not_called()
    cancel.assert_not_called()
    h.close()


def test_callback_during_supervisor_gate_check_is_seen_before_native_write(monkeypatch):
    h = Fixture(monkeypatch, **read_write_settings())
    _, submit, cancel = _enable_fixture_simulation_orders(h)
    h.start()
    _fixture_native_login(h)
    bridge = h.namespace["_RUNTIMES"]["execution"]["_SERVER"].bridge
    assert h.call("execution", "poll_execution_events", {"after_sequence": 0})["data"]["gap"] is False

    class CallbackDuringGate(FixtureRuntimeWriteLease):
        def ensure_ready(self):
            worker = threading.Thread(target=h.namespace["deal_callback"],
                args=(h.context, dict(m_strAccountID=ACCOUNT, m_strOrderSysID="EXISTING",
                                      m_strTradeID="BEFORE-WRITE")))
            worker.start()
            worker.join()
            return True

    bridge.native_write_runtime_guard = CallbackDuringGate
    guard = dict(contract="EXECUTION_CURSOR_WRITE_GATE_V1", execution_session_id=bridge.mailbox.session_id,
                 native_pid=bridge.mailbox.server_pid, sequence=0)
    params = dict(stock_code=CODE, action="BUY", volume=100, price=10.5, client_order_id="gate-callback",
                  execution_event_guard=guard,
                  supervisor_gate_epoch=dict(contract="SUPERVISOR_GATE_EPOCH_V1", epoch=1))
    data = h.call("execution", "submit_order_async", params, request_id="gate-callback", _fixture_guard=False)["data"]
    assert data["status"] == "BLOCKED" and data["no_write_contract"] == "EXECUTION_CURSOR_NO_WRITE_V1"
    assert data["native_lost_sequence"] == data["native_event_sequence"] == 1
    assert data["native_write_executed"] is False and data["automatic_retry"] is False
    submit.assert_not_called()
    cancel.assert_not_called()
    h.close()


@pytest.mark.parametrize("callback", ["account_callback", "order_callback", "deal_callback", "orderError_callback",
                                     "aqp_shm_readonly_adjust", "aqp_shm_rpc_adjust"])
@pytest.mark.parametrize("closed_source", ["read", "execution"])
def test_outer_wrong_thread_never_updates_metadata_or_stops_sources(monkeypatch, callback, closed_source):
    h = Fixture(monkeypatch, **read_write_settings())
    h.start()
    read = h.namespace["_RUNTIMES"]["read"]["_SERVER"]
    execution = h.namespace["_RUNTIMES"]["execution"]["_SERVER"]
    if closed_source == "read":
        read.failed = True
    else:
        execution.closed = True
    original_metadata = dict(execution.bridge.runtime_metadata)
    args = ((h.context,) if callback.startswith("aqp_") else
            (h.context, {}, "error") if callback == "orderError_callback" else (h.context, {}))
    with ThreadPoolExecutor(1) as pool:
        assert pool.submit(h.namespace[callback], *args).result(timeout=1) is None
    assert execution.bridge._off_thread_event_loss.is_set()
    assert execution.bridge.runtime_metadata == original_metadata
    assert h.namespace["_CLOSED"] is False and not read.closed
    # 让原线程自行记录 gap 和只读故障；fixture 不依赖模拟 closed 状态关闭句柄。
    if closed_source == "execution":
        execution.closed = False
    h.namespace["aqp_shm_rpc_adjust"](h.context)
    assert execution.bridge._lost_sequence == 1 and execution.bridge._event_sequence == 1
    if closed_source == "read":
        assert execution.bridge.runtime_metadata["native_read_source_state"] == "RESULT_UNKNOWN"
    h.close()


def test_outer_wrong_thread_stop_is_deferred_to_original_callback(monkeypatch):
    h = Fixture(monkeypatch, **read_write_settings())
    h.start()
    execution = h.namespace["_RUNTIMES"]["execution"]["_SERVER"]
    with ThreadPoolExecutor(1) as pool:
        assert pool.submit(h.namespace["stop"], h.context).result(timeout=1) is None
    assert execution.bridge._off_thread_event_loss.is_set()
    assert h.namespace["_OUTER_STOP_PENDING"].is_set() and h.namespace["_CLOSED"] is False
    assert not execution.closed
    h.namespace["aqp_shm_rpc_adjust"](h.context)
    assert h.namespace["_CLOSED"] is True and execution.closed
    h.close()


def test_same_account_unscoped_native_keeps_legacy_write_contract(monkeypatch):
    h = Fixture(monkeypatch, **read_write_settings(execution_event_guard=False, read_fault_isolation=False))
    _, submit, cancel = _enable_fixture_simulation_orders(h)
    h.start()
    _fixture_native_login(h)
    ping = h.call("execution", "ping")["data"]
    assert "execution_event_guard_contract" not in ping and "read_fault_isolation_contract" not in ping
    assert "native_creation_filetime" not in h.namespace["_SOURCE_IDENTITY"]
    assert h.namespace["_RUNTIMES"]["read"]["_SERVER"].mailbox.retain_frame_evidence is False
    params = dict(stock_code=CODE, action="BUY", volume=100, price=10.5, remark="legacy-unchanged")
    result = h.call("execution", "submit_order", params, request_id="legacy-unchanged", _fixture_guard=False)
    assert result["ok"] and result["data"]["status"] == "CONFIRMED"
    submit.assert_called_once()
    cancel.assert_not_called()
    h.namespace["_RUNTIMES"]["read"]["_SERVER"].close("fixture legacy closure")
    h.namespace["aqp_shm_rpc_adjust"](h.context)
    assert h.namespace["_CLOSED"] and h.namespace["_RUNTIMES"]["execution"]["_SERVER"].closed
    h.close()


def test_unrelated_live_account_cannot_enable_vm_execution_guard():
    with pytest.raises(ValueError, match="explicit target account"):
        BUILDER.build_source(**settings(**live_settings(execution_event_guard=True)))


def test_unrelated_live_account_keeps_legacy_capabilities_and_write_contract(monkeypatch):
    h = Fixture(monkeypatch, **live_settings())
    _, submit, cancel = _enable_fixture_simulation_orders(h)
    h.start()
    _fixture_native_login(h)
    ping = h.call("execution", "ping")["data"]
    assert "execution_event_guard_contract" not in ping and "read_fault_isolation_contract" not in ping
    result = h.call("execution", "submit_order", dict(stock_code=CODE, action="BUY", volume=100,
        price=10.5, remark="unrelated-live"), request_id="unrelated-live", _fixture_guard=False)
    assert result["ok"] and result["data"]["status"] == "CONFIRMED"
    submit.assert_called_once()
    cancel.assert_not_called()
    h.close()


def test_target_live_execution_guard_is_explicit_and_requires_cursor(monkeypatch):
    h = Fixture(monkeypatch, **live_settings(account_id="8882152350", execution_event_guard=True, read_fault_isolation=True))
    _, submit, cancel = _enable_fixture_simulation_orders(h)
    h.start()
    _fixture_native_login(h)
    assert h.call("execution", "ping")["data"]["execution_event_guard_contract"] == "EXECUTION_CURSOR_WRITE_GATE_V1"
    params = dict(stock_code=CODE, action="BUY", volume=100, price=10.5, remark="live-guarded")
    result = h.call("execution", "submit_order", params, request_id="live-no-guard", _fixture_guard=False)
    assert result["data"]["status"] == "BLOCKED" and result["data"]["native_write_executed"] is False
    submit.assert_not_called()
    cancel.assert_not_called()
    h.close()


def test_reentrant_event_during_final_login_blocks_async_before_passorder(monkeypatch):
    h = Fixture(monkeypatch, **read_write_settings())
    _, submit, cancel = _enable_fixture_simulation_orders(h)
    h.start()
    _fixture_native_login(h)
    execution = h.namespace["_RUNTIMES"]["execution"]["_SERVER"]
    bridge = execution.bridge
    assert h.call("execution", "poll_execution_events", {"after_sequence": 0})["data"]["gap"] is False
    guard = dict(contract="EXECUTION_CURSOR_WRITE_GATE_V1", execution_session_id=bridge.mailbox.session_id,
                 native_pid=bridge.mailbox.server_pid, sequence=0)
    original_reader = bridge.trade_login_reader
    calls = []

    def login_with_callback():
        result = original_reader()
        calls.append(True)
        if len(calls) == 2:
            h.namespace["order_callback"](h.context, dict(m_strAccountID=ACCOUNT,
                m_strOrderSysID="unexpected-during-login", m_nOrderStatus=50))
        return result

    bridge.trade_login_reader = login_with_callback
    params = dict(stock_code=CODE, action="BUY", volume=100, price=10.5,
                  client_order_id="guarded-async", execution_event_guard=guard,
                  supervisor_gate_epoch=dict(contract="SUPERVISOR_GATE_EPOCH_V1", epoch=1))
    data = h.call("execution", "submit_order_async", params, request_id="guarded-async", _fixture_guard=False)["data"]
    assert data["status"] == "BLOCKED" and data["native_write_executed"] is False
    assert data["error"] == "EXECUTION_EVENT_CURSOR_NOT_CURRENT" and data["no_write_contract"] == "EXECUTION_CURSOR_NO_WRITE_V1"
    assert data["execution_event_guard"] == guard and data["native_event_sequence"] == 1 and data["native_lost_sequence"] == 0
    assert data["request_id"] == "guarded-async" and data["client_order_id"] == "guarded-async"
    assert data["session_id"] == bridge.mailbox.session_id and data["native_pid"] == bridge.mailbox.server_pid
    assert data["automatic_retry"] is False and data["writes_blocked"] is False
    submit.assert_not_called()
    cancel.assert_not_called()
    events = h.call("execution", "poll_execution_events", {"after_sequence": 0})["data"]
    assert not events["gap"] and [event["sequence"] for event in events["events"]] == [1]
    h.namespace["order_callback"](h.context, dict(m_strAccountID=ACCOUNT, m_strRemark="guarded-async",
        m_strOrderSysID="unexpected-same-cid", m_nOrderStatus=50))
    assert h.call("execution", "ping")["data"]["writes_blocked"] is True
    assert data["status"] == "BLOCKED" and data["native_write_executed"] is False
    h.close()


def test_reentrant_event_during_final_login_blocks_cancel_before_native_call(monkeypatch):
    h = Fixture(monkeypatch, **read_write_settings())
    orders, submit, cancel = _enable_fixture_simulation_orders(h)
    h.start()
    _fixture_native_login(h)
    placed = h.call("execution", "submit_order", dict(stock_code=CODE, action="BUY", volume=100,
        price=10.5, remark="cancel-guard"), request_id="cancel-guard-submit")
    assert placed["data"]["status"] == "CONFIRMED" and len(orders) == 1
    h.namespace["order_callback"](h.context, orders[0])
    assert h.call("execution", "poll_execution_events", {"after_sequence": 0})["data"]["latest_sequence"] == 1
    assert h.call("execution", "poll_execution_events", {"after_sequence": 1})["data"]["gap"] is False
    bridge = h.namespace["_RUNTIMES"]["execution"]["_SERVER"].bridge
    guard = dict(contract="EXECUTION_CURSOR_WRITE_GATE_V1", execution_session_id=bridge.mailbox.session_id,
                 native_pid=bridge.mailbox.server_pid, sequence=1)
    original_reader = bridge.trade_login_reader
    calls = []

    def login_with_callback():
        result = original_reader()
        calls.append(True)
        if len(calls) == 2:
            h.namespace["deal_callback"](h.context, dict(m_strAccountID=ACCOUNT,
                m_strOrderSysID="SIM-FIXTURE-001", m_strTradeID="late-fill"))
        return result

    bridge.trade_login_reader = login_with_callback
    params = dict(order_sys_id="SIM-FIXTURE-001", user_order_id="cancel-guard", execution_event_guard=guard,
                  supervisor_gate_epoch=dict(contract="SUPERVISOR_GATE_EPOCH_V1", epoch=1))
    data = h.call("execution", "cancel_order", params, request_id="guarded-cancel", _fixture_guard=False)["data"]
    assert data["status"] == "BLOCKED" and data["native_write_executed"] is False
    assert data["no_write_contract"] == "EXECUTION_CURSOR_NO_WRITE_V1" and data["native_event_sequence"] == 2
    assert data["execution_event_guard"] == guard and data["request_id"] == "guarded-cancel"
    assert data["session_id"] == bridge.mailbox.session_id and data["automatic_retry"] is False
    submit.assert_called_once()
    cancel.assert_not_called()
    h.close()


@pytest.mark.parametrize("event_type,normalizer", [
    ("order", "normalize_order_event"), ("trade", "normalize_trade_event"),
    ("order_error", "normalize_order_error_event"), ("cancel_error", "normalize_cancel_error_event"),
])
def test_failed_callback_normalization_records_gap_and_permanently_blocks_writes(monkeypatch, event_type, normalizer):
    h = Fixture(monkeypatch, **read_write_settings())
    _, submit, cancel = _enable_fixture_simulation_orders(h)
    h.start()
    _fixture_native_login(h)
    runtime = h.namespace["_RUNTIMES"]["execution"]
    execution = runtime["_SERVER"]
    bridge = execution.bridge
    bridge_module = runtime["_load_embedded"]("bigqmt_signal_trader.shm_rpc_bridge")
    monkeypatch.setattr(bridge_module, normalizer, mock.Mock(side_effect=ValueError("bad callback")))
    assert bridge.record_execution_event(event_type, {"m_strAccountID": ACCOUNT}) == 1
    events = h.call("execution", "poll_execution_events", {"after_sequence": 0})["data"]
    assert events["gap"] and events["lost_sequence"] == 1
    assert events["events"][0]["event"]["event_type"] == "data_loss"
    assert events["events"][0]["event"]["source_event_type"] == event_type
    assert h.call("execution", "ping")["data"]["writes_blocked"] is True
    submit.assert_not_called()
    cancel.assert_not_called()
    h.close()


def test_order_error_preprocessing_failure_is_recorded_as_gap(monkeypatch):
    h = Fixture(monkeypatch, **read_write_settings())
    _, submit, cancel = _enable_fixture_simulation_orders(h)
    h.start()
    _fixture_native_login(h)

    class InvalidNativeObject:
        @property
        def m_strRemark(self):
            raise RuntimeError("fixture unavailable callback field")

    h.namespace["orderError_callback"](h.context, InvalidNativeObject(), "fixture error")
    events = h.call("execution", "poll_execution_events", {"after_sequence": 0})["data"]
    assert events["gap"] and events["lost_sequence"] == 1
    assert events["events"][0]["event"]["source_event_type"] == "order_error"
    assert h.call("execution", "ping")["data"]["writes_blocked"] is True
    submit.assert_not_called()
    cancel.assert_not_called()
    h.close()


@pytest.mark.parametrize("event_type", ["account", "order", "trade", "order_error", "cancel_error"])
def test_off_thread_execution_callback_latches_gap_before_later_writes(monkeypatch, event_type):
    h = Fixture(monkeypatch, **read_write_settings())
    _, submit, cancel = _enable_fixture_simulation_orders(h)
    h.start()
    _fixture_native_login(h)
    execution = h.namespace["_RUNTIMES"]["execution"]["_SERVER"]
    bridge = execution.bridge
    with ThreadPoolExecutor(max_workers=1) as pool:
        if event_type == "account":
            pool.submit(h.namespace["account_callback"], h.context,
                        {"m_strAccountID": ACCOUNT, "m_strStatus": "尚未登录"}).result()
        elif event_type == "order":
            pool.submit(h.namespace["order_callback"], h.context, {"m_strAccountID": ACCOUNT}).result()
        elif event_type == "trade":
            pool.submit(h.namespace["deal_callback"], h.context, {"m_strAccountID": ACCOUNT}).result()
        elif event_type == "order_error":
            pool.submit(h.namespace["orderError_callback"], h.context, {"m_strAccountID": ACCOUNT}, "fixture").result()
        else:
            pool.submit(h.namespace["_RUNTIMES"]["execution"]["aqp_shm_rpc_cancel_error"],
                        h.context, {"m_strAccountID": ACCOUNT}).result()
    assert bridge._off_thread_event_loss.is_set() and bridge._event_sequence == 0
    result = h.call("execution", "poll_execution_events", {"after_sequence": 0})["data"]
    assert result["gap"] and result["lost_sequence"] == result["latest_sequence"] == 1
    assert result["events"][0]["event"]["event_type"] == "data_loss"
    assert result["events"][0]["event"]["source_event_type"] == "UNKNOWN"
    assert h.call("execution", "ping")["data"]["writes_blocked"] is True
    login = h.call("execution", "get_trade_login_state")["data"]
    assert login["status"] == "UNKNOWN" and login["reason"] == "NATIVE_CALLBACK_CONTINUITY_UNVERIFIED"
    guard = dict(contract="EXECUTION_CURSOR_WRITE_GATE_V1", execution_session_id=bridge.mailbox.session_id,
                 native_pid=bridge.mailbox.server_pid, sequence=0)
    data = h.call("execution", "submit_order_async", dict(stock_code=CODE, action="BUY", volume=100,
        price=10.5, client_order_id="lost-event-write", execution_event_guard=guard),
        request_id="lost-event-write", _fixture_guard=False)["data"]
    assert data["status"] == "BLOCKED" and data["native_write_executed"] is False
    assert data["native_lost_sequence"] == 1 and data["writes_blocked"] is True
    submit.assert_not_called()
    cancel.assert_not_called()
    h.close()


def test_invalid_account_callback_marks_login_unknown_and_blocks_writes(monkeypatch):
    h = Fixture(monkeypatch, **read_write_settings())
    _, submit, cancel = _enable_fixture_simulation_orders(h)
    h.start()
    _fixture_native_login(h)

    class InvalidAccount:
        @property
        def m_strAccountID(self):
            raise RuntimeError("fixture unavailable account identity")

    h.namespace["account_callback"](h.context, InvalidAccount())
    events = h.call("execution", "poll_execution_events", {"after_sequence": 0})["data"]
    assert events["gap"] and events["events"][0]["event"]["source_event_type"] == "account"
    login = h.call("execution", "get_trade_login_state")["data"]
    assert login["status"] == "UNKNOWN" and login["reason"] == "NATIVE_CALLBACK_CONTINUITY_UNVERIFIED"
    assert h.call("execution", "ping")["data"]["writes_blocked"] is True
    submit.assert_not_called()
    cancel.assert_not_called()
    h.close()


def test_off_thread_event_during_login_blocks_before_passorder(monkeypatch):
    h = Fixture(monkeypatch, **read_write_settings())
    _, submit, cancel = _enable_fixture_simulation_orders(h)
    h.start()
    _fixture_native_login(h)
    bridge = h.namespace["_RUNTIMES"]["execution"]["_SERVER"].bridge
    assert h.call("execution", "poll_execution_events", {"after_sequence": 0})["data"]["gap"] is False
    guard = dict(contract="EXECUTION_CURSOR_WRITE_GATE_V1", execution_session_id=bridge.mailbox.session_id,
                 native_pid=bridge.mailbox.server_pid, sequence=0)
    original_reader = bridge.trade_login_reader
    calls = []

    def login_with_foreign_callback():
        result = original_reader()
        calls.append(True)
        if len(calls) == 2:
            with ThreadPoolExecutor(max_workers=1) as pool:
                pool.submit(h.namespace["deal_callback"], h.context, {"m_strAccountID": ACCOUNT}).result()
        return result

    bridge.trade_login_reader = login_with_foreign_callback
    params = dict(stock_code=CODE, action="BUY", volume=100, price=10.5,
                  client_order_id="off-thread", execution_event_guard=guard,
                  supervisor_gate_epoch=dict(contract="SUPERVISOR_GATE_EPOCH_V1", epoch=1))
    data = h.call("execution", "submit_order_async", params, request_id="off-thread", _fixture_guard=False)["data"]
    assert data["status"] == "BLOCKED" and data["native_write_executed"] is False
    assert data["native_lost_sequence"] == data["native_event_sequence"] == 1
    assert data["writes_blocked"] is True and data["automatic_retry"] is False
    submit.assert_not_called()
    cancel.assert_not_called()
    h.close()


def test_generated_persistent_simulation_read_write_has_real_separate_handshakes_and_single_binding(monkeypatch, capsys):
    h = Fixture(monkeypatch, **read_write_settings())
    _, submit, cancel = _enable_fixture_simulation_orders(h)
    h.start()
    lines = capsys.readouterr().out.splitlines()
    combined = json.loads(next(line.split(" ", 1)[1] for line in lines if line.startswith("AQP_DUAL_SHM_READY ")))
    assert combined["runtime_policy"] == "PERSISTENT_SIMULATION_READ_WRITE" and combined["allow_order_methods"] is True
    assert h.context.accounts == [ACCOUNT]
    assert h.namespace["handlebar"](h.context) is None
    assert [timer[0] for timer in h.context.timers] == ["aqp_shm_readonly_adjust", "aqp_shm_rpc_adjust"]
    for key, policy, allowed in (("read", "PERSISTENT_READONLY", False), ("execution", "PERSISTENT_SIMULATION_READ_WRITE", True)):
        ping = h.call(key, "ping")["data"]
        assert ping["runtime_policy"] == policy and ping["allow_order_methods"] is allowed
        assert ping["native_pid"] == os.getpid() and ping["windows_session_id"] == 1
        assert ping["session_id"] == combined[key + "_session_id"]
        assert ping["source_deadline_unix"] is None and ping["max_seconds"] == ping["max_requests"] == 0
        assert ping["started_at_unix"] > 0 and ping["rpc_revision"]
        assert ({"submit_order", "cancel_order"} <= set(ping["capabilities"])) is allowed
        positions = h.call(key, "get_positions", {"include_source_evidence": True})["data"]
        assert positions["source_evidence"]["complete"] is True
        assert positions["source_evidence"]["session_id"] == ping["session_id"]
        assert positions["source_evidence"]["windows_session_id"] == 1
    assert h.call("read", "submit_order")["ok"] is False
    blocked = h.call("execution", "submit_order", {"stock_code": CODE, "action": "BUY", "volume": 100, "price": 10.5})
    assert blocked["data"]["status"] == "BLOCKED" and blocked["data"]["native_write_executed"] is False
    submit.assert_not_called()
    cancel.assert_not_called()
    h.close()


def test_actual_generated_write_submit_exact_cancel_events_and_source_evidence(monkeypatch):
    h = Fixture(monkeypatch, **read_write_settings())
    orders, submit, cancel = _enable_fixture_simulation_orders(h)
    h.start()
    _fixture_native_login(h)
    params = dict(stock_code=CODE, action="BUY", volume=100, price=10.5, remark="fixed-local-sim")
    response = h.call("execution", "submit_order", params, request_id="fixed-local-submit")
    assert response["ok"] and response["data"]["status"] == "CONFIRMED", response
    assert response["data"]["order_sys_id"] == "SIM-FIXTURE-001"
    replay = h.call("execution", "submit_order", params, request_id="fixed-local-submit")
    assert replay["data"] == response["data"] and submit.call_count == 1
    assert h.call("execution", "submit_order", dict(params, volume=200), request_id="fixed-local-submit")["ok"] is False
    h.namespace["order_callback"](h.context, orders[0])
    events = h.call("execution", "poll_execution_events", {"after_sequence": 0})["data"]
    assert events["events"][0]["event"]["order_sys_id"] == "SIM-FIXTURE-001"
    refs = dict(order_sys_id="SIM-FIXTURE-001", user_order_id="fixed-local-sim")
    h.namespace["_RUNTIMES"]["execution"]["_SERVER"].bridge.handlers.order_settle_timeout_seconds = 0.005
    cancelled = h.call("execution", "cancel_order", refs, request_id="fixed-local-cancel")
    assert cancelled["data"]["status"] == "UNKNOWN" and cancelled["data"]["automatic_retry"] is False
    assert h.call("execution", "cancel_order", refs, request_id="fixed-local-cancel")["data"] == cancelled["data"]
    assert cancel.call_count == 1
    cancel.assert_called_once_with("SIM-FIXTURE-001", ACCOUNT, "STOCK", h.context)
    query = h.call("execution", "query_orders", {"include_source_evidence": True})["data"]
    assert query["source_evidence"]["complete"] and query["source_evidence"]["raw_row_count"] == 1
    snapshot = h.call("execution", "query_execution_snapshot", {"order_strategy_name": ""})["data"]
    assert snapshot["source_evidence"]["complete"] is True
    assert snapshot["orders_source_evidence"]["session_id"] == h.clients["execution"].mailbox.session_id
    assert snapshot["trades_source_evidence"]["returned_row_count"] == 0
    assert snapshot["source_evidence"]["raw_row_count"] == 1
    h.close()


def test_persistent_simulation_write_unknown_survives_read_expiry_and_changed_rid_conflict(monkeypatch):
    h = Fixture(monkeypatch, **read_write_settings())
    _, submit, cancel = _enable_fixture_simulation_orders(h, fail_submit=True)
    h.start()
    _fixture_native_login(h)
    params = dict(stock_code=CODE, action="BUY", volume=100, price=10.5, remark="unknown-local-sim")
    unknown = h.call("execution", "submit_order", params, request_id="unknown-write")
    assert unknown["data"]["status"] == "UNKNOWN" and submit.call_count == 1
    bridge = h.namespace["_RUNTIMES"]["execution"]["_SERVER"].bridge
    assert "unknown-write" not in bridge._read_replay_expiries
    clock = [time.time()]
    with mock.patch("time.time", side_effect=lambda: clock[0]):
        for _ in range(1026):
            assert h.call("execution", "get_positions")["ok"] is True
            clock[0] += 0.1
        assert len(bridge._replays) <= 13 and "unknown-write" in bridge._replays
        replay = h.call("execution", "submit_order", params, request_id="unknown-write")
        assert replay["data"] == unknown["data"] and submit.call_count == 1
        assert h.call("execution", "submit_order", dict(params, price=10.6), request_id="unknown-write")["ok"] is False
        assert "unknown-write" in bridge._replays
    cancel.assert_not_called()
    h.close()


def test_persistent_simulation_market_history_uses_actual_read_source_and_releases_subscription(monkeypatch):
    h = Fixture(monkeypatch, **read_write_settings(enable_market_rpc=True))
    _, submit, cancel = _enable_fixture_simulation_orders(h)
    native_history = mock.Mock(return_value={CODE: {"20261016093100": dict(open=10.5, high=11.0, low=10.4, close=10.8, volume=100, amount=1080)}})
    h.context.get_market_data_ex = native_history
    h.start()
    ping = h.call("read", "ping")["data"]
    assert {"get_local_data", "get_market_data_ex", "subscribe_quote", "poll_quote_events", "unsubscribe_quote"} <= set(ping["capabilities"])
    params = dict(code=CODE, period="1m", start_time="20261016093000", end_time="20261016094000", count=1,
                  dividend_type="none", fill_data=False, subscribe=False)
    history = h.call("read", "get_market_data_ex", params)
    assert history["ok"] and history["data"]["status"] == "OK" and len(history["data"]["rows"]) == 1
    assert history["data"]["source_evidence"]["session_id"] == ping["session_id"]
    subscribed = h.call("read", "subscribe_quote", {"code": CODE})["data"]
    h.context.callback({CODE: dict(lastPrice=10.8, time=1792110000000)})
    events = h.call("read", "poll_quote_events", {"after_sequence": subscribed["initial_sequence"]})["data"]
    assert len(events["events"]) == 1
    assert h.call("read", "unsubscribe_quote", {"subscription_id": subscribed["subscription_id"]})["data"]["cancelled"] is True
    submit.assert_not_called()
    cancel.assert_not_called()
    h.close()
    assert h.context.unsubscribed == [410]


@pytest.mark.parametrize("changes", [
    {"account_id": "8882152350"}, {"persistent_readonly": True}, {"max_seconds": 1}, {"max_requests": 1},
    {"hard_deadline_unix": 9999999999}, {"persistent_simulation_read_write": 1},
])
def test_persistent_simulation_builder_rejects_mixed_or_unconfirmed_policy(changes):
    values = read_write_settings()
    values.update(changes)
    with pytest.raises(ValueError):
        BUILDER.build_source(**settings(**values))


def test_persistent_simulation_generated_native_gate_refuses_missing_functions_before_account_binding(monkeypatch):
    h = Fixture(monkeypatch, **read_write_settings())
    h.namespace["passorder"] = None
    with pytest.raises(RuntimeError, match="native simulation order/cancel"):
        h.namespace["init"](h.context)
    assert h.context.accounts == [] and all(not state.regions for state in h.states.values())
    h.writes.assert_not_called()


def test_actual_generated_record_and_snapshot_evidence_keeps_incomplete_native_rows(monkeypatch):
    h = Fixture(monkeypatch, **read_write_settings())
    orders, submit, cancel = _enable_fixture_simulation_orders(h)
    orders.append(types.SimpleNamespace(m_strAccountID=ACCOUNT, m_strInstrumentID="000001", m_strExchangeID="SZ"))
    h.start()
    query = h.call("execution", "query_orders", {"include_source_evidence": True})["data"]
    assert query["source_evidence"]["complete"] is False
    assert query["source_evidence"]["raw_row_count"] == 1
    assert query["source_evidence"]["returned_row_count"] == 0
    assert query["source_evidence"]["skipped_row_count"] == 1
    snapshot = h.call("execution", "query_execution_snapshot", {"order_strategy_name": ""})["data"]
    assert snapshot["source_evidence"]["complete"] is False and snapshot["source_evidence"]["status"] == "PARTIAL"
    assert snapshot["source_evidence"]["error_code"] == "EXECUTION_SNAPSHOT_INCOMPLETE"
    assert snapshot["orders_source_evidence"]["raw_row_count"] == 1
    submit.assert_not_called()
    cancel.assert_not_called()
    h.close()


def test_actual_generated_windows_session_disagreement_closes_both_native_sources(monkeypatch):
    h = Fixture(monkeypatch, **read_write_settings())
    _, submit, cancel = _enable_fixture_simulation_orders(h)
    call_count = [0]

    def drifting_session(pid, pointer):
        call_count[0] += 1
        pointer._obj.value = 1 if call_count[0] == 1 else 2
        return True

    ctypes.windll.kernel32.ProcessIdToSessionId = drifting_session
    with pytest.raises(RuntimeError, match="actual Windows session mismatch"):
        h.namespace["init"](h.context)
    assert all(region.closed for state in h.states.values() for region in state.regions)
    assert h.context.accounts == [ACCOUNT]
    submit.assert_not_called()
    cancel.assert_not_called()
    h.writes.assert_not_called()


def test_explicit_persistent_simulation_cli_generates_reviewable_read_write_candidate(tmp_path):
    path = tmp_path / "persistent-simulation-rw.py"
    args = [sys.executable, str(ROOT / "tools/build_dual_shm_single_file.py"), "--output", str(path),
        "--native-pid", str(os.getpid()), "--windows-session", "1", "--read-name", "Local\\AQP_SHM_READONLY_RW_CLI",
        "--execution-name", "Local\\AQP_SHM_TRADE_RW_CLI", "--symbols", CODE,
        "--persistent-simulation-read-write", "--max-seconds", "0", "--max-requests", "0", "--enable-market-rpc"]
    result = subprocess.run(args, text=True, capture_output=True, timeout=15)
    assert result.returncode == 0, result.stderr
    assert "no native process started" in result.stdout and path.stat().st_mode & 0o777 == 0o600
    source = path.read_text(encoding="utf-8")
    ast.parse(source, feature_version=(3, 6))
    source.encode("ascii").decode("gbk")
    namespace = {}
    exec(compile(source, "<reviewable-read-write-cli>", "exec"), namespace)
    assert namespace["DUAL_SHM_SETTINGS"]["persistent_simulation_read_write"] is True
    read, execution = namespace["_RUNTIMES"]["read"], namespace["_RUNTIMES"]["execution"]
    assert read["READONLY_PERSISTENT_READONLY"] is True and read["READONLY_ENABLE_MARKET_RPC"] is True
    assert execution["SHM_RPC_PERSISTENT_SIMULATION_READ_WRITE"] is True
    assert execution["SHM_RPC_ALLOW_ORDER_METHODS"] is True
    assert execution["SHM_RPC_ORDER_GATEWAY_FACTORY"] is execution["create_native_stock_order_gateway"]
    assert read["_SERVER"] is execution["_SERVER"] is None
    for payload in namespace["_SOURCE_PAYLOADS"].values():
        ast.parse(base64.b64decode(payload).decode("utf-8"), feature_version=(3, 6))


@pytest.mark.parametrize("native_available", [False, True])
def test_dual_generated_history_download_capability_follows_actual_native_injection(monkeypatch, native_available):
    h = Fixture(monkeypatch, **read_write_settings(enable_market_rpc=True))
    _, submit, cancel = _enable_fixture_simulation_orders(h)
    downloader = mock.Mock(return_value=None)
    if native_available:
        h.namespace["download_history_data"] = downloader
    h.start()
    ping = h.call("read", "ping")["data"]
    assert ("download_history_data" in ping["capabilities"]) is native_available
    response = h.call("read", "download_history_data", dict(code="000002.SZ", period="tick",
        start_time="20261016", end_time="20261016", incrementally=None))
    if native_available:
        assert response["ok"] and response["data"]["status"] == "REQUESTED"
        assert response["data"]["completion_confirmed"] is False
        assert response["data"]["session_id"] == ping["session_id"]
        downloader.assert_called_once_with("000002.SZ", "tick", "20261016", "20261016")
    else:
        assert response["ok"] is False and response["server_error"] == "CAPABILITY_MISSING"
        downloader.assert_not_called()
    submit.assert_not_called()
    cancel.assert_not_called()
    h.close()


def live_settings(**changes):
    values = dict(account_id="8765432100", environment="LIVE", persistent_live_read_write=True,
                  hard_deadline_unix=None, max_seconds=0, max_requests=0, enable_market_rpc=True)
    values.update(changes)
    return values


@pytest.mark.parametrize("changes", [
    {"environment": "SIM"}, {"environment": "live"}, {"persistent_live_read_write": False},
    {"persistent_live_read_write": 1}, {"account_id": ACCOUNT}, {"account_id": "LIVE_TEST"},
    {"account_id": "１２３４"}, {"account_id": ""}, {"account_id": "1" * 65},
    {"persistent_simulation_read_write": True}, {"persistent_readonly": True},
    {"max_seconds": 1}, {"max_requests": 1}, {"hard_deadline_unix": 9999999999},
])
def test_live_dual_builder_rejects_implicit_mixed_simulation_and_bounded_policies(changes):
    with pytest.raises(ValueError):
        BUILDER.build_source(**settings(**live_settings(**changes)))


def test_live_generated_handshakes_and_account_evidence_preserve_live_environment(monkeypatch, capsys):
    h = Fixture(monkeypatch, **live_settings())
    _, submit, cancel = _enable_fixture_simulation_orders(h)
    h.start()
    ready = json.loads(next(line.split(" ", 1)[1] for line in capsys.readouterr().out.splitlines()
                            if line.startswith("AQP_DUAL_SHM_READY ")))
    assert ready["environment"] == "LIVE" and ready["account_id"] == h.account
    assert ready["runtime_policy"] == "PERSISTENT_LIVE_READ_WRITE"
    assert ready["read_session_id"] != ready["execution_session_id"]
    assert h.context.accounts == [h.account] and h.namespace["handlebar"](h.context) is None
    for key, policy, write_allowed in (("read", "PERSISTENT_READONLY", False),
                                       ("execution", "PERSISTENT_LIVE_READ_WRITE", True)):
        response = h.call(key, "ping")
        assert response["environment"] == "LIVE" and response["data"]["environment"] == "LIVE"
        assert response["data"]["runtime_policy"] == policy
        assert response["data"]["allow_order_methods"] is write_allowed
        assert response["data"]["source_deadline_unix"] is None
        assert response["data"]["max_seconds"] == response["data"]["max_requests"] == 0
        for method in ("get_asset", "get_positions", "query_orders", "query_trades"):
            response = h.call(key, method, {"include_source_evidence": True})
            assert response["ok"] and response["environment"] == "LIVE", response
            evidence = response["data"]["source_evidence"]
            assert evidence["environment"] == "LIVE" and evidence["account_id"] == h.account
            assert evidence["native_pid"] == os.getpid() and evidence["windows_session_id"] == 1
    snapshot = h.call("execution", "query_execution_snapshot", {"order_strategy_name": ""})["data"]
    for field in ("source_evidence", "orders_source_evidence", "trades_source_evidence"):
        assert snapshot[field]["environment"] == "LIVE"
    assert h.call("read", "submit_order")["ok"] is False
    blocked = h.call("execution", "submit_order", dict(stock_code=CODE, action="BUY", volume=100, price=10.5))
    assert blocked["data"]["status"] == "BLOCKED" and blocked["data"]["native_write_executed"] is False
    submit.assert_not_called()
    cancel.assert_not_called()
    h.close()


@pytest.mark.parametrize("environment", [None, "SIM", "live"])
@pytest.mark.parametrize("key", ["read", "execution"])
def test_live_generated_native_rejects_missing_or_mismatched_environment_before_business(monkeypatch, key, environment):
    h = Fixture(monkeypatch, **live_settings())
    _, submit, cancel = _enable_fixture_simulation_orders(h)
    h.start()
    response = h.call(key, "get_asset", environment=environment)
    assert response["ok"] is False and "environment mismatch" in response["error"]
    assert h.query_calls == []
    submit.assert_not_called()
    cancel.assert_not_called()
    h.close()


def test_live_generated_unknown_write_is_retained_and_not_resent(monkeypatch):
    h = Fixture(monkeypatch, **live_settings())
    _, submit, cancel = _enable_fixture_simulation_orders(h, fail_submit=True)
    h.start()
    _fixture_native_login(h)
    params = dict(stock_code=CODE, action="BUY", volume=100, price=10.5, remark="local-live-fixture")
    original = h.call("execution", "submit_order", params, request_id="live-local-unknown")
    assert original["environment"] == "LIVE" and original["data"]["status"] == "UNKNOWN"
    assert original["data"]["automatic_retry"] is False and submit.call_count == 1
    repeated = h.call("execution", "submit_order", params, request_id="live-local-unknown")
    assert repeated["data"] == original["data"] and submit.call_count == 1
    conflict = h.call("execution", "submit_order", dict(params, volume=200), request_id="live-local-unknown")
    assert conflict["ok"] is False and submit.call_count == 1
    cancel.assert_not_called()
    h.close()


@pytest.mark.parametrize("field,value", [("BIGQMT_ENVIRONMENT", "SIM"), ("BIGQMT_ACCOUNT_ID", ACCOUNT),
                                        ("SHM_RPC_PERSISTENT_LIVE_READ_WRITE", False)])
def test_live_generated_payload_tampering_rejected_before_account_binding(monkeypatch, field, value):
    h = Fixture(monkeypatch, **live_settings())
    h.namespace["_RUNTIMES"]["execution"][field] = value
    with pytest.raises(RuntimeError, match="runtime policy changed"):
        h.namespace["init"](h.context)
    assert h.context.accounts == [] and all(not state.regions for state in h.states.values())
    h.writes.assert_not_called()


def test_live_cli_generates_only_reviewable_candidate_without_native_execution(tmp_path):
    path = tmp_path / "AQP_LIVE_DUAL_SOURCE.py"
    args = [sys.executable, str(ROOT / "tools/build_dual_shm_single_file.py"), "--output", str(path),
        "--account-id", "8765432100", "--environment", "LIVE", "--persistent-live-read-write",
        "--native-pid", str(os.getpid()), "--windows-session", "1",
        "--read-name", "Local\\AQP_SHM_READONLY_LIVE_FIXTURE",
        "--execution-name", "Local\\AQP_SHM_TRADE_LIVE_FIXTURE", "--symbols", CODE,
        "--max-seconds", "0", "--max-requests", "0", "--enable-market-rpc"]
    result = subprocess.run(args, text=True, capture_output=True, timeout=15)
    assert result.returncode == 0, result.stderr
    assert "no native process started" in result.stdout
    source = path.read_text(encoding="utf-8")
    ast.parse(source, feature_version=(3, 6))
    source.encode("ascii").decode("gbk")
    namespace = {}
    exec(compile(source, str(path), "exec"), namespace)
    assert namespace["DUAL_SHM_SETTINGS"]["environment"] == "LIVE"
    assert namespace["DUAL_SHM_SETTINGS"]["persistent_live_read_write"] is True
    assert namespace["_RUNTIMES"]["execution"]["_SERVER"] is None


@pytest.mark.parametrize("kind", ["read", "execution"])
def test_live_single_source_builders_require_explicit_account_environment_policy_and_mailbox(kind):
    builder = BUILDER.READ_BUILDER if kind == "read" else BUILDER.EXECUTION_BUILDER
    kwargs = dict(account_id="8765432100", environment="LIVE", persistent_live_read_write=True,
                  name="Local\\AQP_SHM_%s_LIVE_SINGLE_FIXTURE" % ("READONLY" if kind == "read" else "TRADE"))
    if kind == "execution":
        kwargs.update(native_stock_gateway=True, max_seconds=0, max_requests=0)
    source = builder.build_source(**kwargs)
    ast.parse(source, feature_version=(3, 6))
    namespace = {}
    exec(compile(source, "<single-live-source>", "exec"), namespace)
    assert namespace["BIGQMT_ENVIRONMENT"] == "LIVE" and namespace["_SERVER"] is None
    for changes in ({"environment": "SIM"}, {"persistent_live_read_write": False},
                    {"account_id": ACCOUNT}, {"name": None}):
        with pytest.raises(ValueError):
            builder.build_source(**dict(kwargs, **changes))


def test_live_generated_successful_native_submission_keeps_account_and_cancel_identity(monkeypatch):
    h = Fixture(monkeypatch, **live_settings())
    orders, submit, cancel = _enable_fixture_simulation_orders(h)
    h.start()
    _fixture_native_login(h)
    response = h.call("execution", "submit_order", dict(stock_code=CODE, action="BUY", volume=100,
        price=10.5, remark="local-live-confirmed"), request_id="live-local-submit")
    assert response["ok"] and response["environment"] == "LIVE"
    assert response["data"]["status"] == "CONFIRMED" and submit.call_count == 1
    h.namespace["order_callback"](h.context, orders[0])
    event = h.call("execution", "poll_execution_events", {"after_sequence": 0})
    assert event["environment"] == "LIVE" and event["data"]["events"][0]["account_id"] == h.account
    h.namespace["_RUNTIMES"]["execution"]["_SERVER"].bridge.handlers.order_settle_timeout_seconds = 0.005
    cancelled = h.call("execution", "cancel_order", dict(order_sys_id="SIM-FIXTURE-001",
        user_order_id="local-live-confirmed"), request_id="live-local-cancel")
    assert cancelled["data"]["status"] == "UNKNOWN" and cancelled["data"]["automatic_retry"] is False
    cancel.assert_called_once_with("SIM-FIXTURE-001", h.account, "STOCK", h.context)
    h.close()


def test_live_partial_quote_source_evidence_keeps_environment_and_raw_failure(monkeypatch):
    h = Fixture(monkeypatch, **live_settings())
    h.start()
    h.context.get_full_tick = lambda codes: {CODE: dict(lastPrice=0.0, time=1792110000000)}
    response = h.call("read", "get_full_tick", {"codes": [CODE]})
    assert response["ok"] is False and response["environment"] == "LIVE"
    assert response["data"]["source_evidence"]["environment"] == "LIVE"
    assert response["data"]["source_evidence"]["complete"] is False
    assert response["data"]["invalid_symbols"] == [CODE]
    h.close()


@pytest.mark.parametrize("method", ["get_ticks", "get_full_tick"])
def test_live_generated_reads_all_67_saved_position_symbols_in_existing_mailbox(monkeypatch, method, record_property):
    codes = LIVE_SAVED_POSITION_SYMBOLS_20261020
    assert len(codes) == len(set(codes)) == 67
    h = Fixture(monkeypatch, **live_settings(symbols=codes))
    h.context.get_full_tick = lambda requested: {code: dict(lastPrice=11.47, time=1792110000000,
        lastClose=11.4, open=11.4, high=11.5, low=11.3, amount=1000000.0, volume=1000,
        askPrice=[11.48, 11.49, 11.50, 11.51, 11.52], bidPrice=[11.47, 11.46, 11.45, 11.44, 11.43],
        askVol=[100] * 5, bidVol=[100] * 5) for code in requested}
    h.start()
    response = h.call("read", method, {"codes": codes})
    assert response["ok"] and response["environment"] == "LIVE" and set(response["data"]) == set(codes)
    transport = h.transports["read"]
    assert transport.REQUEST_CAPACITY == 8192 and transport.RESPONSE_CAPACITY == 8 * 1024 * 1024
    raw = transport.encode_payload(response, transport.RESPONSE_CAPACITY)
    record_property("live_67_%s_response_bytes" % method, len(raw))
    assert len(raw) < 8 * 1024 * 1024
    h.close()


@pytest.mark.parametrize("environment,count,allowed", [("SIM", 65, False), ("LIVE", 128, True), ("LIVE", 1025, False)])
def test_generated_quote_symbol_capacity_is_live_specific(monkeypatch, environment, count, allowed):
    codes = ["%06d.SZ" % number for number in range(count)]
    values = live_settings(symbols=codes) if environment == "LIVE" else dict(symbols=codes)
    if not allowed:
        with pytest.raises(ValueError, match="1-%d unique" % (1024 if environment == "LIVE" else 64)):
            BUILDER.build_source(**settings(**values))
        return
    h = Fixture(monkeypatch, **values)
    h.start()
    response = h.call("read", "get_ticks", {"codes": codes})
    assert response["ok"] and len(response["data"]) == 128
    assert h.call("read", "get_ticks", {"codes": codes + ["999999.SZ"]})["server_error"] == "INVALID_PARAMS"
    h.close()


@pytest.mark.parametrize("codes", [[], [CODE, CODE], ["bad.SZ"], ["000001.XX"]])
def test_live_large_whitelist_still_rejects_empty_duplicate_and_invalid_symbols(codes):
    with pytest.raises(ValueError):
        BUILDER.build_source(**settings(**live_settings(symbols=codes)))


def test_sim_generated_request_limit_remains_64_and_live_reads_outside_initial_symbols(monkeypatch):
    sim_codes = ["%06d.SZ" % number for number in range(64)]
    sim = Fixture(monkeypatch, symbols=sim_codes)
    sim.start()
    assert sim.call("read", "get_ticks", {"codes": sim_codes + ["999999.SZ"]})["server_error"] == "INVALID_PARAMS"
    sim.close()
    live = Fixture(monkeypatch, **live_settings(symbols=LIVE_SAVED_POSITION_SYMBOLS_20261020))
    live.start()
    assert live.call("read", "get_ticks", {"codes": ["999999.SZ"]})["ok"] is True
    live.close()
