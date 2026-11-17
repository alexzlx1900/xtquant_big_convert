# coding: utf-8
"""实际生成并执行三运行体；仅 Redis/Win32 区域和原生输入使用替身。"""

import ast
import builtins
from concurrent.futures import ThreadPoolExecutor
import ctypes
import importlib.util
import io
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import threading
import time
import types
import unittest
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
ACCOUNT = "55013746"
CODE = "000001.SZ"


def load_file(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


P1_FIXTURE = load_file("original_p1_fixture", ROOT / "tests/test_p1_embedded_runtime.py")
BUILDER = load_file("p1_coexist_builder_fixture", ROOT / "tools/build_p1_coexist_single_file.py")


class State:
    def __init__(self):
        self.data = bytearray(270400)
        self.lock = threading.Lock()
        self.regions = []


class Region:
    def __init__(self, state):
        self.state, self.pid, self.closed = state, os.getpid(), False
        state.regions.append(self)

    def acquire(self, milliseconds):
        if self.closed:
            raise RuntimeError("closed fixture region")
        return self.state.lock.acquire(False) if not milliseconds else self.state.lock.acquire(timeout=milliseconds / 1000.0)

    def release(self):
        self.state.lock.release()

    def read(self, offset, length):
        return bytes(self.state.data[offset:offset + length])

    def write(self, offset, value):
        self.state.data[offset:offset + len(value)] = value

    def close(self):
        self.closed = True


class Context(P1_FIXTURE.FakeContext):
    def __init__(self, fail_timer=None):
        super().__init__()
        self.fail_timer = fail_timer
        self.financial_calls = 0

    def run_time(self, *args):
        super().run_time(*args)
        if args[0] == self.fail_timer:
            raise RuntimeError("fixture timer initialization failure")

    def get_financial_data(self, *args, **kwargs):
        self.financial_calls += 1
        raise AssertionError("warmup financial query forbidden")


def _child(scenario):
    sys.path.insert(0, str(ROOT / "src"))
    os.environ.update(BIGQMT_LOG_ENABLED="0", BIGQMT_LOG_TO_STDOUT="0")
    for name in ("xtquant.xtconstant", "xtquant.xttype", "base64", "datetime", "logging.handlers", "threading", "queue", "traceback", "inspect", "math", "re", "uuid", "typing", "hashlib", "dataclasses", "enum", "collections", "functools", "copy", "weakref", "abc"):
        __import__(name)
    redis_module = types.ModuleType("redis")
    class OwnedRedis(P1_FIXTURE.FakeRedis):
        instances = []
        shared_pool = types.SimpleNamespace(disconnect=mock.Mock())

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.connection_pool = self.shared_pool if scenario == "pool_shared" else types.SimpleNamespace(disconnect=mock.Mock())
            self.instances.append(self)

    redis_module.Redis = OwnedRedis
    sys.modules["redis"] = redis_module
    foreign = types.ModuleType("bigqmt_signal_trader_strategy")
    foreign.reset_app = mock.Mock()
    foreign_pool = types.SimpleNamespace(disconnect=mock.Mock())
    foreign.redis = types.SimpleNamespace(connection_pool=foreign_pool)
    old_config = types.ModuleType("bigqmt_signal_trader_local_config")
    old_config.BIGQMT_ACCOUNT_ID = "FOREIGN"
    old_config.BIGQMT_REDIS_CONFIG = {"rpc_allow_order_methods": True}
    sys.modules[foreign.__name__] = foreign
    sys.modules[old_config.__name__] = old_config
    global_modules = {name: module for name, module in sys.modules.items() if name.startswith("bigqmt_signal_trader")}
    original_import_module, original_reload = importlib.import_module, importlib.reload
    original_switchinterval = sys.getswitchinterval()
    original_thread = threading.Thread
    thread_names = []

    def observed_thread(*args, **kwargs):
        thread_names.append(kwargs.get("name", ""))
        if kwargs.get("name") == "bigqmt-context-warmup":
            raise AssertionError("warmup thread forbidden")
        return original_thread(*args, **kwargs)

    threading.Thread = observed_thread
    sys.setswitchinterval = mock.Mock(side_effect=AssertionError("global GIL tuning forbidden"))
    writes, queries = [], []

    def forbidden_write(*args, **kwargs):
        writes.append((args, kwargs))
        raise AssertionError("native trading forbidden")

    def detail(account, account_type, kind, *args):
        queries.append((account, account_type, kind))
        assert (account, account_type) == (ACCOUNT, "STOCK")
        if kind == "POSITION":
            return [types.SimpleNamespace(m_strInstrumentID="000001", m_strExchangeID="SZ", m_nVolume=100, m_nCanUseVolume=100, m_dOpenPrice=10.0, m_dMarketValue=1147.0, m_dLastPrice=11.47)]
        if kind == "ACCOUNT":
            return [types.SimpleNamespace(m_strAccountID=ACCOUNT, m_dAvailable=1000.0, m_dBalance=2147.0, m_dFrozenCash=0.0, m_dInstrumentValue=1147.0)]
        return []

    def current_session(pid, pointer):
        pointer._obj.value = 2
        return True

    def build(suffix):
        return BUILDER.build_source(P1_FIXTURE.CONFIG, os.getpid(), 2, time.time() + 60,
            "Local\\AQP_SHM_READONLY_P1_TEST_" + suffix,
            "Local\\AQP_SHM_TRADE_P1_TEST_" + suffix, [CODE], max_seconds=60, max_requests=1000)

    original_open, original_io_open = builtins.open, io.open
    original_exists, original_isfile = os.path.exists, os.path.isfile

    def guard(function):
        def guarded(path, *args, **kwargs):
            if "bigqmt_signal_trader" in str(path):
                raise AssertionError("custom module or config disk access forbidden")
            return function(path, *args, **kwargs)
        return guarded

    # Build before filesystem/network guards: only runtime custom file I/O is forbidden.
    first_source = build("A")
    second_source = build("B")
    ast.parse(first_source, feature_version=(3, 6))
    first_source.encode("ascii").decode("gbk")
    builtins.open, io.open = guard(original_open), guard(original_io_open)
    os.path.exists, os.path.isfile = guard(original_exists), guard(original_isfile)
    socket.socket = mock.Mock(side_effect=AssertionError("network forbidden"))
    socket.create_connection = mock.Mock(side_effect=AssertionError("network forbidden"))
    ctypes.windll = types.SimpleNamespace(kernel32=types.SimpleNamespace(ProcessIdToSessionId=current_session))

    def namespace_from(source):
        namespace = dict(__name__="p1_coexist_fixture", __file__="/fixture/P1.py", get_trade_detail_data=detail,
            passorder=forbidden_write, cancel=forbidden_write)
        for name in ("get_history_trade_detail_data", "get_value_by_order_id", "get_last_order_id",
                     "download_history_data", "download_history_data2", "down_history_data"):
            namespace[name] = mock.Mock(return_value=[])
        exec(compile(source, "<generated-p1-coexist>", "exec"), namespace)
        return namespace

    ns = namespace_from(first_source)
    redis_ns, read_ns, exec_ns = [ns["_RUNTIMES"][key] for key in ("redis", "read", "execution")]
    assert redis_ns["_ISOLATED_EMBEDDED"] is True
    assert redis_ns["BIGQMT_REDIS_CONFIG"] == dict(P1_FIXTURE.CONFIG["BIGQMT_REDIS_CONFIG"], rpc_allow_order_methods=False)
    assert importlib.import_module is original_import_module and importlib.reload is original_reload
    assert {name: module for name, module in sys.modules.items() if name.startswith("bigqmt_signal_trader")} == global_modules
    assert redis_ns["_load_local_module"]("bigqmt_signal_trader_local_config") is not old_config
    assert redis_ns["_strategy"] is not foreign
    for name in ns["_NATIVE_BINDINGS"]:
        if name in ns:
            assert redis_ns["_strategy"]._qmt_api[name] is ns[name]
    foreign.reset_app.assert_not_called()
    states = {}
    transports = {}
    for key, runtime, transport_name in (("read", read_ns, "aqp_readonly_transport"), ("execution", exec_ns, "bigqmt_signal_trader.transports.readonly_shm")):
        transport = runtime["_load_embedded"](transport_name)
        transports[key] = transport
        states[key] = State()
        transport.Win32Region = lambda *args, state=states[key], **kwargs: Region(state)
    context = Context("aqp_shm_rpc_adjust" if scenario == "rollback" else None)
    if scenario == "identity":
        ns["P1_SETTINGS"]["windows_session"] = 3
        try:
            ns["init"](context)
        except RuntimeError as exc:
            assert "session mismatch" in str(exc)
        else:
            raise AssertionError("wrong Windows session accepted")
        assert context.accounts == [] and not states["read"].regions
    elif scenario == "rollback":
        try:
            ns["init"](context)
        except RuntimeError as exc:
            assert "timer initialization failure" in str(exc)
        else:
            raise AssertionError("partial init was accepted")
        assert redis_ns["_strategy"]._rpc_service is None
        assert read_ns["_SERVER"].closed and exec_ns["_SERVER"].closed
        assert all(region.closed for state in states.values() for region in state.regions)
        ns["stop"](context)
    elif scenario == "pool_shared":
        ns["init"](context)
        service = redis_ns["_strategy"]._rpc_service
        assert len(OwnedRedis.instances) == 3
        assert len({id(client.connection_pool) for client in OwnedRedis.instances}) == 1
        ns["stop"](context)
        ns["stop"](context)
        assert "process_request" not in service.__dict__
        assert service.redis is None and service.listen_redis is None
        assert service._transport.on_raw_payload is None
    elif scenario == "budget":
        ns["P1_SETTINGS"]["max_requests"] = 1
        ns["init"](context)
        service = redis_ns["_strategy"]._rpc_service
        rpc = redis_ns["_load_local_module"]("bigqmt_signal_trader.redis_rpc")
        for index in range(2):
            rid = "budget-%s" % index
            service.listen_redis.rpush(service.request_queue, rpc.encode_rpc_request_payload(dict(
                request_id=rid, account_id=ACCOUNT, method="ping", params={}, reply_key=rid)))
        ns["adjust"](context)
        assert service._processed_count == 1
        assert service.redis.get("budget-0") is not None and service.redis.get("budget-1") is None
        ns["aqp_shm_readonly_adjust"](context)
        assert ns["_CLOSED"] and not service._running.is_set()
        assert all(region.closed for state in states.values() for region in state.regions)
    elif scenario == "deadline":
        ns["init"](context)
        service = redis_ns["_strategy"]._rpc_service
        ns["_ACTIVE_DEADLINE"] = time.time() - 1
        ns["adjust"](context)
        assert ns["_CLOSED"] and not service._running.is_set()
        assert all(region.closed for state in states.values() for region in state.regions)
    else:
        native_init_result = ns["init"](context)
        runner = redis_ns["_load_local_module"]("bigqmt_signal_trader.runner")
        app = runner.get_app()
        service = redis_ns["_strategy"]._rpc_service
        assert native_init_result is None
        assert app is not None and app.account_id == ACCOUNT and service._running.is_set()
        assert app.position_sync_sink.redis in ns["_OWN_REDIS_CLIENTS"]
        assert service.handlers.allow_order_methods is False
        assert redis_ns["_strategy"]._build_config()["run_app_tick"] is False
        assert [timer[0] for timer in context.timers] == ["adjust", "aqp_shm_readonly_adjust", "aqp_shm_rpc_adjust"]
        assert context.accounts == [ACCOUNT, ACCOUNT]
        try:
            ns["init"](context)
        except RuntimeError:
            pass
        else:
            raise AssertionError("duplicate init accepted")
        assert service._running.is_set()
        read_position_module = read_ns["_load_embedded"]("bigqmt_signal_trader.adapters.position_bigqmt")
        assert read_position_module is not exec_ns["_load_embedded"]("bigqmt_signal_trader.adapters.position_bigqmt")
        assert read_position_module is not redis_ns["_load_local_module"]("bigqmt_signal_trader.adapters.position_bigqmt")
        # Loading another candidate must not stop or replace the original private service.
        second = namespace_from(second_source)
        assert service._running.is_set() and redis_ns["_strategy"]._rpc_service is service
        clients = {key: transport.ShmReadOnlyClient(
            read_ns["READONLY_SHM_NAME"] if key == "read" else exec_ns["SHM_RPC_NAME"], ACCOUNT,
            region=Region(states[key])) for key, transport in transports.items()}
        rpc = redis_ns["_load_local_module"]("bigqmt_signal_trader.redis_rpc")

        def redis_request(method, index):
            rid = "redis-%s-%s" % (method, index)
            wire = rpc.encode_rpc_request_payload(dict(request_id=rid, account_id=ACCOUNT, method=method, params={}, reply_key=rid))
            assert wire.startswith("b64s:")
            service.listen_redis.rpush(service.request_queue, wire)
            ns["adjust"](context)
            value = json.loads(service.redis.get(rid))
            assert value["request_id"] == rid and value["account_id"] == ACCOUNT
            return value

        def shm_request(key, method, index, params=None):
            request = dict(schema_version=1, request_id="%s-%s-%s" % (key, method, index), account_id=ACCOUNT,
                method=method, params=params or {}, read_expires_at_unix=time.time() + 5)
            callback = ns["aqp_shm_readonly_adjust" if key == "read" else "aqp_shm_rpc_adjust"]
            with ThreadPoolExecutor(1) as pool:
                future = pool.submit(clients[key].send_request, request, 1.0)
                deadline = time.monotonic() + 2
                while not future.done() and time.monotonic() < deadline:
                    callback(context)
                    time.sleep(0.001)
                return future.result(timeout=0.2)

        for index in range(10):
            assert redis_request("ping", index)["data"]["pong"] is True
            read = shm_request("read", "ping", index)
            execution = shm_request("execution", "ping", index)
            assert read["ok"] and execution["ok"]
            assert read["data"]["session_id"] != execution["data"]["session_id"]
            assert read["data"]["native_pid"] == execution["data"]["native_pid"] == os.getpid()
        assert redis_request("get_positions", 0)["data"][CODE]["volume"] == 100
        assert shm_request("read", "get_positions", 0)["data"][CODE]["volume"] == 100
        assert shm_request("execution", "get_positions", 0)["data"][CODE]["volume"] == 100
        quote = shm_request("read", "get_full_tick", 0, {"codes": ["600000.SH"]})
        assert quote["ok"] and quote["data"]["600000.SH"]["lastPrice"] == 11.47
        assert context.tick_requests[-1] == ["600000.SH"]
        for method in ("submit_order", "cancel_order"):
            assert redis_request(method, 0)["ok"] is False
            assert shm_request("read", method, 0)["ok"] is False
            assert shm_request("execution", method, 0)["ok"] is False
        old_sids = (read_ns["_SERVER"].mailbox.session_id, exec_ns["_SERVER"].bridge.mailbox.session_id)
        for client in clients.values():
            client.close()
        ns["stop"](context)
        ns["stop"](context)
        assert not service._running.is_set() and redis_ns["_strategy"]._rpc_service is None
        assert runner.get_app() is None and app.position_sync_sink.redis is None
        assert all(region.closed for state in states.values() for region in state.regions)
        new_states = {}
        for key, transport_name in (("read", "aqp_readonly_transport"), ("execution", "bigqmt_signal_trader.transports.readonly_shm")):
            new_states[key] = State()
            transport = second["_RUNTIMES"][key]["_load_embedded"](transport_name)
            transport.Win32Region = lambda *args, state=new_states[key], **kwargs: Region(state)
        new_context = Context()
        second["init"](new_context)
        new_sids = (second["_RUNTIMES"]["read"]["_SERVER"].mailbox.session_id,
                    second["_RUNTIMES"]["execution"]["_SERVER"].bridge.mailbox.session_id)
        assert len(set(old_sids + new_sids)) == 4
        second["stop"](new_context)
        assert all(region.closed for state in new_states.values() for region in state.regions)
    assert ns["_CLOSED"] and not writes
    assert context.financial_calls == 0 and "bigqmt-context-warmup" not in thread_names
    assert sys.getswitchinterval() == original_switchinterval
    sys.setswitchinterval.assert_not_called()
    foreign_pool.disconnect.assert_not_called()
    for client in OwnedRedis.instances:
        client.connection_pool.disconnect.assert_called_once_with()
    assert not ns["_OWN_REDIS_CLIENTS"]
    foreign.reset_app.assert_not_called()
    assert sys.modules[foreign.__name__] is foreign and sys.modules[old_config.__name__] is old_config
    assert importlib.import_module is original_import_module and importlib.reload is original_reload
    print("P1_COEXIST_%s_PASS" % scenario.upper())


class CoexistRuntimeTest(unittest.TestCase):
    def scenario(self, name):
        result = subprocess.run([sys.executable, str(Path(__file__).resolve()), name], cwd=str(ROOT),
            env=dict(os.environ, BIGQMT_LOG_ENABLED="0", BIGQMT_LOG_TO_STDOUT="0"),
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=40)
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertIn("P1_COEXIST_%s_PASS" % name.upper(), result.stdout)

    def test_actual_three_protocols_alternate_ten_rounds_isolation_cleanup_new_sid(self):
        self.scenario("reads")

    def test_partial_init_failure_closes_original_redis_and_both_mailboxes(self):
        self.scenario("rollback")

    def test_identity_mismatch_fails_before_native_initialization(self):
        self.scenario("identity")

    def test_request_budget_does_not_execute_an_excess_redis_request(self):
        self.scenario("budget")

    def test_hard_deadline_closes_all_three_sources(self):
        self.scenario("deadline")

    def test_owned_shared_connection_pool_disconnects_once_and_references_clear(self):
        self.scenario("pool_shared")

    def test_builder_rejects_unbounded_or_different_account(self):
        settings = dict(config_values=P1_FIXTURE.CONFIG, native_pid=1, windows_session=2,
            hard_deadline_unix=1.0, read_name="Local\\AQP_SHM_READONLY_TEST", execution_name="Local\\AQP_SHM_TRADE_TEST", symbols=[CODE])
        for changes in ({"max_seconds": 901}, {"max_requests": 1001}, {"native_pid": True},
                        {"hard_deadline_unix": float("nan")}, {"symbols": [CODE, CODE]},
                        {"config_values": dict(P1_FIXTURE.CONFIG, BIGQMT_ACCOUNT_ID="8882152350")}):
            with self.assertRaises(ValueError):
                BUILDER.build_source(**dict(settings, **changes))

    def test_private_output_is_restricted_at_creation_and_never_overwrites(self):
        with tempfile.TemporaryDirectory(prefix="p1_coexist_output_") as directory:
            path = Path(directory) / "candidate.py"
            with BUILDER.open_private(path) as stream:
                stream.write("fixture candidate\n")
                if os.name == "posix":
                    self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            with self.assertRaises(FileExistsError):
                BUILDER.open_private(path)
            self.assertEqual(path.read_text(), "fixture candidate\n")


if __name__ == "__main__":
    _child(sys.argv[1])
