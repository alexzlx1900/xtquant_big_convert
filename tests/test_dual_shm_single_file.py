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


def test_resident_model_launches_only_explicit_runtime_after_verified_sources(monkeypatch):
    launcher = dict(python=r"C:\Python\python.exe", script=r"C:\AQP\supervise_qmt_runtime.py", config=r"C:\AQP\runtime.json")
    h = Fixture(monkeypatch, native_pid=0, native_executable=r"C:\QMT\XtItClient.exe",
        source_identity_path=r"C:\AQP\source.json", runtime_launcher=launcher,
        account_id="8882152350", environment="LIVE", persistent_live_read_write=True,
        max_seconds=0, max_requests=0, hard_deadline_unix=None)

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
    launched.assert_called_once_with([launcher["python"], launcher["script"], "--config", launcher["config"]],
        stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        close_fds=False, creationflags=0x208)
    h.close()


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
    dict(persistent_readonly=False, persistent_simulation_read_write=True),
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

    def start(self):
        assert self.namespace["init"](self.context) is None
        for key, transport in self.transports.items():
            name = self.namespace["_RUNTIMES"][key]["READONLY_SHM_NAME" if key == "read" else "SHM_RPC_NAME"]
            self.clients[key] = transport.ShmReadOnlyClient(name, self.account)

    def call(self, key, method, params=None, **overrides):
        request = dict(schema_version=1, request_id="dual-%s-%s" % (key, time.monotonic_ns()),
            account_id=self.account, method=method, params=params or {})
        if self.settings.get("environment") == "LIVE":
            request["environment"] = "LIVE"
        request.update(overrides)
        if request.get("environment") is None:
            request.pop("environment", None)
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
    assert not {"submit_order", "cancel_order"} & set(ready["execution_capabilities"])
    for key in ("read", "execution"):
        ping = h.call(key, "ping")
        assert ping["ok"] and ping["data"]["allow_order_methods"] is False
        assert ping["data"]["session_id"] == ready["read_session_id" if key == "read" else "execution_session_id"]
        assert h.call(key, "get_positions")["data"][CODE]["volume"] == 100
        for method in ("submit_order", "cancel_order"):
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


def test_generated_python36_ascii_gbk_and_no_native_redis_or_write_bindings():
    source = BUILDER.build_source(**settings())
    tree = ast.parse(source, feature_version=(3, 6))
    source.encode("ascii").decode("gbk")
    payload_node = next(node for node in tree.body if isinstance(node, ast.Assign) and isinstance(node.targets[0], ast.Name) and node.targets[0].id == "_SOURCE_PAYLOADS")
    payloads = ast.literal_eval(payload_node.value)
    assert set(payloads) == {"read", "execution"}
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
    return dict(persistent_readonly=True, hard_deadline_unix=None, max_seconds=0, max_requests=0, **changes)


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
    return dict(persistent_simulation_read_write=True, hard_deadline_unix=None,
                max_seconds=0, max_requests=0, **changes)


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


@pytest.mark.parametrize("environment,count,allowed", [("SIM", 65, False), ("LIVE", 128, True), ("LIVE", 129, False)])
def test_generated_quote_symbol_capacity_is_live_specific(monkeypatch, environment, count, allowed):
    codes = ["%06d.SZ" % number for number in range(count)]
    values = live_settings(symbols=codes) if environment == "LIVE" else dict(symbols=codes)
    if not allowed:
        with pytest.raises(ValueError, match="1-%d unique" % (128 if environment == "LIVE" else 64)):
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


def test_sim_generated_request_limit_remains_64_and_live_still_enforces_exact_whitelist(monkeypatch):
    sim_codes = ["%06d.SZ" % number for number in range(64)]
    sim = Fixture(monkeypatch, symbols=sim_codes)
    sim.start()
    assert sim.call("read", "get_ticks", {"codes": sim_codes + ["999999.SZ"]})["server_error"] == "INVALID_PARAMS"
    sim.close()
    live = Fixture(monkeypatch, **live_settings(symbols=LIVE_SAVED_POSITION_SYMBOLS_20261020))
    live.start()
    assert live.call("read", "get_ticks", {"codes": ["999999.SZ"]})["server_error"] == "SYMBOL_DENIED"
    live.close()
