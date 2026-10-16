"""隔离进程验证原 Redis 模型内嵌候选，不连接任何外部服务。"""

import ast
import builtins
import importlib.util
import io
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import types
import unittest


ROOT = Path(__file__).resolve().parents[1]
ACCOUNT = "55013746"
CODE = "000001.SZ"
CONFIG = {
    "BIGQMT_ACCOUNT_ID": ACCOUNT,
    "BIGQMT_ACCOUNT_TYPE": "STOCK",
    "BIGQMT_REDIS_CONFIG": {
        "host": "fixture.invalid", "port": 16379, "db": 9,
        "username": "fixture-user", "password": "fixture-password",
        "rpc_allow_order_methods": True,
        "rpc_background_threads": False,
        "rpc_process_in_listener": True,
        "rpc_listener_methods": ("*",),
        "schedule_adjust": True,
        "schedule_adjust_interval": "100nMilliSecond",
        "full_tick_cache_enabled": False,
        "download_jobs_enabled": False,
        "exec_events_enabled": False,
        "fixture_retained_nested": {"values": ["preserved", 7]},
        "fixture_retained_tuple": ("metadata", 3),
    },
}


class FakeRedis:
    kv = {}
    lists = {}
    configurations = []
    published = []

    def __init__(self, *args, **kwargs):
        self.configurations.append(dict(kwargs))

    def ping(self):
        return True

    def get(self, key):
        return self.kv.get(key)

    def set(self, key, value, **kwargs):
        self.kv[key] = value
        return True

    def setex(self, key, seconds, value):
        return self.set(key, value)

    def rpush(self, key, value):
        self.lists.setdefault(key, []).append(value)
        return len(self.lists[key])

    def lpop(self, key):
        values = self.lists.get(key, [])
        return values.pop(0) if values else None

    def expire(self, key, seconds):
        return True

    def publish(self, channel, value):
        self.published.append((channel, value))
        return 1

    def delete(self, *keys):
        for key in keys:
            self.kv.pop(key, None)
            self.lists.pop(key, None)
        return len(keys)

    def hgetall(self, key):
        return {}


class FakeContext:
    def __init__(self):
        self.accounts = []
        self.timers = []
        self.tick_requests = []

    def set_account(self, account_id):
        self.accounts.append(account_id)

    def run_time(self, *args):
        self.timers.append(args)

    def get_full_tick(self, codes):
        self.tick_requests.append(list(codes))
        return {code: {"lastPrice": 11.47, "time": 1792110000000,
                       "askPrice": [11.48], "bidPrice": [11.46]} for code in codes}

    def get_financial_data(self, *args, **kwargs):
        return {}


def _load_builder():
    spec = importlib.util.spec_from_file_location("p1_builder_fixture", ROOT / "tools/build_single_file.py")
    builder = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(builder)
    return builder


def _build_source(config_values=None):
    return _load_builder().build_source(config_values=config_values)


def _child(scenario):
    if scenario == "snapshot":
        builder = _load_builder()
        with tempfile.TemporaryDirectory(prefix="p1_snapshot_fixture_") as directory:
            path = Path(directory) / "literal_snapshot.py"
            initial = dict(CONFIG["BIGQMT_REDIS_CONFIG"], rpc_allow_order_methods=False)
            text = "\n".join(("BIGQMT_ACCOUNT_ID = %r" % ACCOUNT,
                "BIGQMT_ACCOUNT_TYPE = 'STOCK'", "BIGQMT_REDIS_CONFIG = %r" % initial,
                "BIGQMT_REDIS_CONFIG['rpc_allow_order_methods'] = True"))
            path.write_text(text, encoding="utf-8")
            parsed = builder.load_config_snapshot(path)
            assert parsed == CONFIG
            assert parsed["BIGQMT_REDIS_CONFIG"]["fixture_retained_tuple"] == ("metadata", 3)
            for unsafe in ("import os", "exec('unsafe')", "BIGQMT_ACCOUNT_ID = str(123)",
                           "BIGQMT_REDIS_CONFIG['rpc_allow_order_methods'] = bool(1)"):
                path.write_text(unsafe, encoding="utf-8")
                try:
                    builder.load_config_snapshot(path)
                except (ValueError, TypeError):
                    pass
                else:
                    raise AssertionError("dynamic snapshot source was accepted")
        print("P1_SNAPSHOT_PASS")
        return
    source = _build_source(None if scenario == "default" else CONFIG)
    assert isinstance(source, str)
    ast.parse(source, feature_version=(3, 6))
    source.encode("ascii")
    source.encode("gbk")
    if scenario == "default":
        assert 'BIGQMT_ACCOUNT_ID = "YOUR_ACCOUNT_ID"' in source
        assert '"rpc_allow_order_methods": False,' in source
        assert '"rpc_allow_order_methods": True,' not in source
        print("P1_DEFAULT_PASS")
        return

    os.environ["BIGQMT_LOG_ENABLED"] = "0"
    os.environ["BIGQMT_LOG_TO_STDOUT"] = "0"
    # Only existing SDK constant/type definitions are loaded from disk. Custom
    # bridge modules must subsequently resolve through the embedded loader.
    sys.path.insert(0, str(ROOT / "src"))
    for name in ("xtquant.xtconstant", "xtquant.xttype", "base64", "datetime",
                 "logging.handlers", "threading", "queue", "traceback", "inspect",
                 "math", "re", "uuid", "typing", "hashlib", "dataclasses",
                 "enum", "collections", "functools", "copy", "weakref", "abc"):
        importlib.import_module(name)
    redis_module = types.ModuleType("redis")
    redis_module.Redis = FakeRedis
    sys.modules["redis"] = redis_module
    old_config = types.ModuleType("bigqmt_signal_trader_local_config")
    old_config.BIGQMT_ACCOUNT_ID = "STALE_ACCOUNT"
    old_config.BIGQMT_ACCOUNT_TYPE = "CREDIT"
    old_config.BIGQMT_REDIS_CONFIG = {"rpc_allow_order_methods": True, "host": "stale.invalid"}
    sys.modules[old_config.__name__] = old_config

    blocked_accesses = []
    original_open, original_io_open = builtins.open, io.open
    original_exists, original_isfile = os.path.exists, os.path.isfile

    def custom_path(path):
        return "bigqmt_signal_trader" in str(path).replace("\\", "/")

    def guard(function):
        def checked(path, *args, **kwargs):
            if custom_path(path):
                blocked_accesses.append(str(path))
                raise AssertionError("custom module/config disk access forbidden: %s" % path)
            return function(path, *args, **kwargs)
        return checked

    def no_network(*args, **kwargs):
        raise AssertionError("network access forbidden in embedded fixture")

    builtins.open = guard(original_open)
    io.open = guard(original_io_open)
    os.path.exists = guard(original_exists)
    os.path.isfile = guard(original_isfile)
    socket.socket = no_network
    socket.create_connection = no_network

    native_writes, detail_requests, history_calls = [], [], []

    def write_probe(*args, **kwargs):
        native_writes.append((args, kwargs))
        raise AssertionError("native order/cancel must never run")

    def detail(account_id, account_type, kind, *args):
        detail_requests.append((account_id, account_type, kind))
        if kind == "POSITION":
            return [types.SimpleNamespace(m_strInstrumentID="000001", m_strExchangeID="SZ",
                m_strInstrumentName="fixture", m_nVolume=100, m_nCanUseVolume=100,
                m_dOpenPrice=10.0, m_dMarketValue=1147.0, m_dLastPrice=11.47)]
        return []

    def history(name):
        def read_only(*args, **kwargs):
            history_calls.append((name, args, kwargs))
            return [] if name == "get_history_trade_detail_data" else "FIXTURE-ORDER-ID"
        return read_only

    native_history = {name: history(name) for name in (
        "get_history_trade_detail_data", "get_value_by_order_id", "get_last_order_id",
        "download_history_data", "download_history_data2", "down_history_data")}
    namespace = dict(__name__="p1_embedded_fixture", __file__="/fixture/EMBEDDED.py",
        passorder=write_probe, cancel=write_probe, get_trade_detail_data=detail, **native_history)
    exec(compile(source, "/fixture/EMBEDDED.py", "exec"), namespace)
    config = namespace["_load_local_module"]("bigqmt_signal_trader_local_config")
    expected = dict(CONFIG["BIGQMT_REDIS_CONFIG"], rpc_allow_order_methods=False)
    assert config is not old_config
    assert config.BIGQMT_ACCOUNT_ID == ACCOUNT and config.BIGQMT_ACCOUNT_TYPE == "STOCK"
    assert config.BIGQMT_REDIS_CONFIG == expected
    assert namespace["BIGQMT_REDIS_CONFIG"] == expected
    assert CONFIG["BIGQMT_REDIS_CONFIG"]["rpc_allow_order_methods"] is True
    assert namespace["_runtime"].RPC_ALLOW_ORDER_METHODS is False
    for export in ("init", "handlebar", "adjust", "order_callback", "deal_callback"):
        assert namespace[export] is getattr(namespace["_runtime"], export)
    strategy = namespace["_load_local_module"]("bigqmt_signal_trader_strategy")
    for name, function in native_history.items():
        assert strategy._qmt_api[name] is function
    assert not blocked_accesses

    if scenario == "foreign":
        foreign = types.ModuleType(config.__name__)
        foreign.BIGQMT_REDIS_CONFIG = {"rpc_allow_order_methods": True}
        sys.modules[config.__name__] = foreign
        try:
            namespace["_load_local_module"](config.__name__)
        except ImportError:
            pass
        else:
            raise AssertionError("foreign same-named config module was reused")
        assert not native_writes and not blocked_accesses
        print("P1_FOREIGN_PASS")
        return

    context = FakeContext()
    app = namespace["init"](context)
    service = strategy._rpc_service
    assert app is not None
    assert service is not None and service._running.is_set()
    assert service._transport._running
    assert service.handlers.allow_order_methods is False
    for name, function in native_history.items():
        assert service.handlers.qmt_api[name] is function
    assert service.background_threads is False
    assert service._thread is None and service._queue_thread is None
    assert context.accounts == [ACCOUNT]
    assert any(timer[0] == "adjust" for timer in context.timers)
    assert all(client.get("host") == expected["host"] for client in FakeRedis.configurations)
    rpc = namespace["_load_local_module"]("bigqmt_signal_trader.redis_rpc")

    def request(method, params=None):
        request_id = "p1-fixture-" + method
        reply_key = "fixture:reply:" + request_id
        wire = rpc.encode_rpc_request_payload(dict(
            request_id=request_id, account_id=ACCOUNT, method=method, params=params or {}, reply_key=reply_key))
        assert wire.startswith("b64s:")
        service.listen_redis.rpush(service.request_queue, wire)
        assert service.redis.get(reply_key) is None
        namespace["adjust"](context)
        response = json.loads(service.redis.get(reply_key))
        assert response["request_id"] == request_id and response["account_id"] == ACCOUNT
        assert response["method"] == method
        return response

    try:
        if scenario == "reads":
            ping = request("ping")
            assert ping["ok"] and ping["data"]["pong"] is True
            assert ping["data"]["allow_order_methods"] is False
            positions = request("get_positions")
            assert positions["ok"]
            assert positions["data"][CODE]["volume"] == 100
            assert positions["data"][CODE]["available"] == 100
            assert (ACCOUNT, "STOCK", "POSITION") in detail_requests
            tick = request("get_full_tick", {"codes": [CODE]})
            assert tick["ok"] and tick["data"][CODE]["lastPrice"] == 11.47
            assert tick["data"][CODE]["time"] == 1792110000000
            assert [CODE] in context.tick_requests
        elif scenario == "orders":
            write_methods = set(rpc.ORDER_METHODS)
            write_methods.update(alias for alias, target in rpc.METHOD_ALIASES.items() if target in rpc.ORDER_METHODS)
            for method in sorted(write_methods):
                response = request(method)
                assert response["ok"] is False
                assert response["data"] is None
                assert service.handlers._canonical_method(method) not in service.handlers.allowed_methods
                assert response["error"] == "ValueError: rpc method is not allowed: %s" % method
        else:
            raise AssertionError("unknown scenario")
        assert not native_writes and not history_calls and not blocked_accesses
    finally:
        strategy.reset_app()
    assert not service._running.is_set()
    print("P1_%s_PASS" % scenario.upper())


class EmbeddedRuntimeTest(unittest.TestCase):
    def run_scenario(self, scenario):
        result = subprocess.run([sys.executable, str(Path(__file__).resolve()), scenario],
            cwd=str(ROOT), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
            timeout=30, env=dict(os.environ, BIGQMT_LOG_ENABLED="0", BIGQMT_LOG_TO_STDOUT="0"))
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertIn("P1_%s_PASS" % scenario.upper(), result.stdout)

    def test_default_placeholder_and_python36_ascii_gbk_contract(self):
        self.run_scenario("default")

    def test_actual_init_b64s_adjust_reads_and_history_exports(self):
        self.run_scenario("reads")

    def test_all_write_methods_and_aliases_are_rejected_without_native_calls(self):
        self.run_scenario("orders")

    def test_foreign_same_named_config_is_not_reused(self):
        self.run_scenario("foreign")

    def test_literal_snapshot_preserves_final_true_and_tuple_but_rejects_code(self):
        self.run_scenario("snapshot")


if __name__ == "__main__":
    _child(sys.argv[1])
