# coding: utf-8
"""实际执行生成bootstrap，只有内存region/原生数据fixture替代，无交易调用。"""

import ast
import base64
import builtins
import importlib.util
import json
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest


ROOT = Path(__file__).resolve().parent.parent
SPEC = importlib.util.spec_from_file_location("shm_rpc_builder", ROOT / "tools/build_shm_rpc_single_file.py")
BUILDER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(BUILDER)
SOURCE = BUILDER.build_source()
ACCOUNT = "SIM_BOOTSTRAP"


class State:
    def __init__(self):
        self.data, self.lock, self.regions = bytearray(64 + 8192 + 8 * 1024 * 1024), threading.Lock(), []


class Region:
    def __init__(self, state):
        self.state, self.pid, self.closed = state, 777, False
        state.regions.append(self)

    def acquire(self, milliseconds):
        if self.closed:
            raise RuntimeError("closed region")
        return self.state.lock.acquire(False) if not milliseconds else self.state.lock.acquire(timeout=milliseconds / 1000)

    def release(self):
        self.state.lock.release()

    def read(self, offset, length):
        return bytes(self.state.data[offset:offset + length])

    def write(self, offset, raw):
        self.state.data[offset:offset + len(raw)] = raw

    def close(self):
        self.closed = True


class Context:
    def __init__(self, timer_error=False):
        self.calls, self.timer_error = [], timer_error

    def run_time(self, name, period, start):
        self.calls.append((name, period, start))
        if self.timer_error:
            raise RuntimeError("timer unavailable")


class Fixture:
    def __init__(self, source=SOURCE, native_gateway=False, submit=None, cancel=None, account=ACCOUNT,
                 verified_login_fixture=True):
        self.state, self.context, self.query_calls = State(), Context(), []
        self.account, self.orders, self.trades = account, [], []
        self.namespace = {}
        # 禁止引入任何业务网络依赖；直接运行完整生成的外壳和内嵌处理器。
        original = builtins.__import__
        def guarded(name, *args, **kwargs):
            if name.split(".")[0] in ("redis", "socket", "zmq", "xtquant"):
                raise AssertionError("forbidden runtime import: %s" % name)
            return original(name, *args, **kwargs)
        with mock.patch("builtins.__import__", side_effect=guarded):
            exec(compile(source, "<generated-shm-rpc>", "exec"), self.namespace)
        self.namespace["BIGQMT_ACCOUNT_ID"] = account
        self.namespace["get_trade_detail_data"] = self.query
        if native_gateway or self.namespace["SHM_RPC_BIND_ACCOUNT_CALLBACKS"]:
            self.context.set_account = mock.Mock()
        if native_gateway:
            self.namespace.update(SHM_RPC_ALLOW_ORDER_METHODS=True,
                                  SHM_RPC_ORDER_GATEWAY_FACTORY=self.namespace["create_native_stock_order_gateway"],
                                  passorder=submit or mock.Mock(), cancel=cancel or mock.Mock(return_value=True))
        transport = self.namespace["_load_embedded"]("bigqmt_signal_trader.transports.readonly_shm")
        self.transport = transport
        transport.Win32Region = lambda *args, **kwargs: Region(self.state)
        self.namespace["init"](self.context)
        self.server = self.namespace["_SERVER"]
        if native_gateway and verified_login_fixture:
            # 单独的已验证登录替身只服务原订单合同测试；缺源门禁测试显式关闭它。
            self.server.bridge.trade_login_reader = lambda: dict(status="LOGGED_IN", account_id=account,
                session_id=self.server.bridge.mailbox.session_id, native_pid=777, source="native_account_callback", fixture_only=True,
                raw_status="FIXTURE_ONLY", event_observed_at_utc="2026-10-15T00:00:00Z",
                age_seconds=0.0, fresh=True, positive_contract_verified=True, reason="")
        self.client = transport.ShmReadOnlyClient(self.namespace["SHM_RPC_NAME"], account)

    def query(self, account, account_type, detail_type, strategy_name=""):
        self.query_calls.append((account, account_type, detail_type, strategy_name))
        assert account == self.account and account_type == "STOCK"
        if detail_type == "ACCOUNT":
            return [SimpleNamespace(m_strAccountID=self.account, m_dAvailable=1000.0, m_dBalance=3000.0,
                                    m_dFrozenCash=0.0, m_dInstrumentValue=2000.0)]
        if detail_type == "POSITION":
            return [SimpleNamespace(m_strInstrumentID="000001", m_strExchangeID="SZ", m_nVolume=100,
                                    m_nCanUseVolume=100, m_dOpenPrice=10, m_dMarketValue=2000, m_dLastPrice=20)]
        if detail_type == "ORDER":
            return self.orders
        if detail_type == "DEAL":
            return self.trades
        return []

    def call(self, method, params=None, request_id=None):
        request = dict(schema_version=1, request_id=request_id or "request-%d" % time.monotonic_ns(),
                       account_id=self.account, method=method, params=params or {})
        with ThreadPoolExecutor(1) as pool:
            future = pool.submit(self.client.send_request, request)
            deadline = time.monotonic() + 2
            while not future.done() and time.monotonic() < deadline:
                self.namespace["aqp_shm_rpc_adjust"](self.context)
                time.sleep(0.001)
            return future.result(timeout=1)

    def close(self):
        self.client.close()
        self.namespace["stop"](self.context)


def test_output_ascii_gbk_python36_and_derived_handler_without_runtime_broker():
    assert SOURCE.encode("ascii").decode("gbk") == SOURCE
    ast.parse(SOURCE, feature_version=(3, 6))
    env = {}
    exec(compile(SOURCE, "<generated>", "exec"), env)
    for name, raw in env["_EMBEDDED_SOURCES"].items():
        ast.parse(base64.b64decode(raw).decode("utf-8"), feature_version=(3, 6))
    rpc = env["_load_embedded"]("bigqmt_signal_trader.redis_rpc")
    assert hasattr(rpc, "BigQmtRpcHandlers") and not hasattr(rpc, "RedisPubSubRpcService")
    assert not hasattr(rpc, "_rpc_logger")
    with pytest.raises(ImportError):
        env["_bundle_import"]("socket")
    with pytest.raises(ImportError):
        env["_bundle_import"]("redis")


def test_actual_bootstrap_default_deny_query_and_session_identity():
    h = Fixture()
    assert h.namespace["SHM_RPC_ORDER_GATEWAY"] is None
    ping = h.call("ping")["data"]
    assert ping["allow_order_methods"] is False and ping["transport"] == "shm_rpc"
    assert "submit_order" not in ping["capabilities"] and "cancel_order" not in ping["capabilities"]
    assert ping["session_id"] == h.server.bridge.mailbox.session_id and ping["native_pid"] == 777
    assert h.context.calls[0][0:2] == ("aqp_shm_rpc_adjust", "100nMilliSecond")
    assert h.namespace["handlebar"](h.context) is None
    for method in ("submit_order", "cancel_order"):
        value = h.call(method, {"stock_code": "000001.SZ", "action": "BUY", "volume": 100, "price": 20})
        assert value["ok"] is False and value["server_error"] == "PermissionError"
    assert not h.query_calls
    asset = h.call("get_asset")["data"]
    assert asset["asset"]["cash"] == 1000.0 and asset["source_evidence"]["complete"] is True
    positions = h.call("get_positions")["data"]
    assert positions["000001.SZ"]["volume"] == 100
    assert h.call("query_orders")["data"] == [] and h.call("query_trades")["data"] == []
    snapshot = h.call("query_execution_snapshot")["data"]
    assert snapshot["orders"] == [] and snapshot["trades"] == []
    h.close()
    assert all(region.closed for region in h.state.regions)


def test_native_callbacks_preserve_identity_and_derive_rejection_event():
    h = Fixture()
    h.namespace["order_callback"](h.context, SimpleNamespace(m_strAccountID=ACCOUNT, m_strOrderSysID="REAL-1", m_nOrderStatus=57, m_strRemark="own-order"))
    h.namespace["deal_callback"](h.context, SimpleNamespace(m_strAccountID=ACCOUNT, order_id=123, seq=456, m_strRemark="own-order"))
    h.namespace["aqp_shm_rpc_cancel_error"](h.context, SimpleNamespace(m_strAccountID=ACCOUNT, m_strOrderSysID="REAL-1", error_id=10))
    value = h.call("poll_execution_events", {"after_sequence": 0})["data"]
    assert [event["event"]["event_type"] for event in value["events"]] == ["order", "order_error", "trade", "cancel_error"]
    assert value["events"][0]["event"]["identity_status"] == "KNOWN"
    assert value["events"][2]["event"]["identity_status"] == "UNKNOWN"
    assert not value["events"][2]["event"]["order_sys_id"]
    h.close()
    h.namespace["order_callback"](h.context, SimpleNamespace(m_strAccountID=ACCOUNT))
    assert h.server.bridge._event_sequence == 4


@pytest.mark.parametrize("callback,date_field,time_field,result_field", [
    ("order_callback", "m_strInsertDate", "m_strInsertTime", "order_time"),
    ("deal_callback", "m_strTradeDate", "m_strTradeTime", "traded_time"),
])
def test_generated_callbacks_parse_native_dates_through_lazy_standard_library_import(callback, date_field, time_field, result_field):
    h = Fixture()
    native = dict(m_strAccountID=ACCOUNT, m_strOrderSysID="REAL-DATED-1", m_strRemark="dated-short",
                  m_strInstrumentID="000001", m_strExchangeID="SZ", m_nOrderStatus=50,
                  m_nOffsetFlag=48, m_nVolumeTotalOriginal=100, m_nVolume=100)
    native.update({date_field: "20261015", time_field: "09:30:05"})
    try:
        h.namespace[callback](h.context, SimpleNamespace(**native))
        events = h.call("poll_execution_events", {"after_sequence": 0})["data"]["events"]
        assert len(events) == 1
        event = events[0]["event"]
        assert event[result_field] == int(time.mktime((2026, 10, 15, 9, 30, 5, 0, 0, -1)))
        assert event["identity_status"] == "KNOWN" and event["order_sys_id"] == "REAL-DATED-1"
        assert event["stock_code"] == "000001.SZ" and event["user_order_id"] == "dated-short"
    finally:
        h.close()


def test_timer_failure_releases_created_mailbox_and_unbound_write_optin_refuses():
    h = Fixture()
    h.close()
    h.context = Context(timer_error=True)
    with pytest.raises(RuntimeError, match="timer unavailable"):
        h.namespace["init"](h.context)
    assert all(region.closed for region in h.state.regions)
    h.namespace["SHM_RPC_ALLOW_ORDER_METHODS"] = True
    with pytest.raises(RuntimeError, match="authorized order_gateway"):
        h.namespace["init"](Context())
    assert all(region.closed for region in h.state.regions)
    h.namespace["SHM_RPC_ALLOW_ORDER_METHODS"] = False
    h.namespace["SHM_RPC_ORDER_GATEWAY"] = object()
    with pytest.raises(RuntimeError, match="readonly bootstrap must not bind order_gateway"):
        h.namespace["init"](Context())
    assert all(region.closed for region in h.state.regions)


def test_authorized_write_binds_fixed_account_before_mailbox_ready(capsys):
    env, state, calls = {}, State(), []
    exec(compile(SOURCE, "<generated>", "exec"), env)
    env.update(BIGQMT_ACCOUNT_ID=ACCOUNT, get_trade_detail_data=lambda *args: [],
               SHM_RPC_ALLOW_ORDER_METHODS=True, SHM_RPC_ORDER_GATEWAY=object())
    transport = env["_load_embedded"]("bigqmt_signal_trader.transports.readonly_shm")
    transport.Win32Region = lambda *args, **kwargs: Region(state)
    mark_ready = transport.ReadOnlyMailbox.mark_ready
    def observed_ready(mailbox):
        assert calls == [("set_account", ACCOUNT)]
        calls.append(("mailbox_ready",))
        return mark_ready(mailbox)
    context = SimpleNamespace(set_account=lambda account: calls.append(("set_account", account)),
                              run_time=lambda *args: calls.append(("timer",)))
    with mock.patch.object(transport.ReadOnlyMailbox, "mark_ready", observed_ready):
        env["init"](context)
    assert calls == [("set_account", ACCOUNT), ("mailbox_ready",), ("timer",)]
    assert '"allow_order_methods": true' in capsys.readouterr().out
    env["stop"](context)
    assert state.regions and all(region.closed for region in state.regions)


@pytest.mark.parametrize("binding", ["missing", "raises"])
def test_authorized_write_account_binding_failure_never_creates_ready_mailbox(binding, capsys):
    env, state, query = {}, State(), mock.Mock(return_value=[])
    exec(compile(SOURCE, "<generated>", "exec"), env)
    env.update(BIGQMT_ACCOUNT_ID=ACCOUNT, get_trade_detail_data=query,
               SHM_RPC_ALLOW_ORDER_METHODS=True, SHM_RPC_ORDER_GATEWAY=object())
    transport = env["_load_embedded"]("bigqmt_signal_trader.transports.readonly_shm")
    transport.Win32Region = lambda *args, **kwargs: Region(state)
    context = Context()
    if binding == "raises":
        context.set_account = mock.Mock(side_effect=RuntimeError("account binding failed"))
    with pytest.raises(RuntimeError, match="set_account|account binding failed"):
        env["init"](context)
    assert env["_SERVER"] is None and not state.regions and not context.calls
    assert "AQP_SHM_RPC_READY" not in capsys.readouterr().out
    query.assert_not_called()
    if binding == "raises":
        context.set_account.assert_called_once_with(ACCOUNT)


def test_authorized_write_timer_failure_closes_mailbox_after_account_binding(capsys):
    env, state = {}, State()
    exec(compile(SOURCE, "<generated>", "exec"), env)
    env.update(BIGQMT_ACCOUNT_ID=ACCOUNT, get_trade_detail_data=lambda *args: [],
               SHM_RPC_ALLOW_ORDER_METHODS=True, SHM_RPC_ORDER_GATEWAY=object())
    transport = env["_load_embedded"]("bigqmt_signal_trader.transports.readonly_shm")
    transport.Win32Region = lambda *args, **kwargs: Region(state)
    context = Context(timer_error=True)
    context.set_account = mock.Mock()
    with pytest.raises(RuntimeError, match="timer unavailable"):
        env["init"](context)
    context.set_account.assert_called_once_with(ACCOUNT)
    assert state.regions and all(region.closed for region in state.regions)
    assert env["_SERVER"].closed
    assert "AQP_SHM_RPC_READY" not in capsys.readouterr().out


@pytest.mark.parametrize("as_dict", [False, True])
@pytest.mark.parametrize("contract", [None, "REAL-ERROR-1"])
def test_native_three_argument_order_error_preserves_target_remark_message_and_real_contract(as_dict, contract):
    h = Fixture()
    fields = dict(accountID=ACCOUNT, orderCode="SZ000001", strategyName="sample_&&&_aqp-short",
                  modelVolume=100.0, modelPrice=11.93, opType=23, order_id=123, seq=456)
    if contract is not None:
        fields["m_strOrderSysID"] = contract
    native_args = fields if as_dict else SimpleNamespace(**fields)
    h.namespace["orderError_callback"](h.context, native_args, "native limit price rejected")
    value = h.call("poll_execution_events", {"after_sequence": 0})["data"]
    assert len(value["events"]) == 1 and value["gap"] is False
    event = value["events"][0]["event"]
    assert event["event_type"] == "order_error" and event["account_id"] == ACCOUNT
    assert event["stock_code"] == "000001.SZ"
    assert event["order_remark"] == event["user_order_id"] == "aqp-short"
    assert event["error_msg"] == "native limit price rejected"
    assert event["native_order_id"] == 123 and event["native_sequence"] == 456
    assert event["order_sys_id"] == (contract or "")
    assert event["identity_status"] == ("KNOWN" if contract else "UNKNOWN")
    h.close()
    h.namespace["orderError_callback"](h.context, native_args, "after close")
    assert h.server.bridge._event_sequence == 1


def test_native_order_error_explicit_remark_wins_and_foreign_account_refuses():
    h = Fixture()
    args = dict(accountID=ACCOUNT, orderCode="SZ000001", strategyName="sample_&&&_from-strategy",
                remark="explicit-short", error_msg="stale message", order_id=999)
    h.namespace["orderError_callback"](h.context, args, "third-argument-error")
    event = h.call("poll_execution_events", {"after_sequence": 0})["data"]["events"][0]["event"]
    assert event["order_remark"] == "explicit-short" and event["error_msg"] == "third-argument-error"
    assert event["identity_status"] == "UNKNOWN" and event["order_sys_id"] == ""
    args["accountID"] = "FOREIGN"
    with pytest.raises(h.transport.ShmProtocolError, match="event account mismatch"):
        h.namespace["orderError_callback"](h.context, args, "foreign account")
    assert h.server.bridge._event_sequence == 1
    h.close()


def test_budget_and_wrong_thread_close_or_refuse_without_query():
    h = Fixture()
    with ThreadPoolExecutor(1) as pool:
        with pytest.raises(RuntimeError, match="original callback thread"):
            pool.submit(h.namespace["aqp_shm_rpc_adjust"], h.context).result()
    h.server.created_at -= h.namespace["SHM_RPC_MAX_SECONDS"] + 1
    h.namespace["aqp_shm_rpc_adjust"](h.context)
    assert h.server.closed and h.server.bridge.mailbox.region.closed
    assert not h.query_calls
    h.client.close()


def test_generated_mark_ready_failure_closes_owned_handles_and_preserves_original_error():
    env, state = {}, State()
    exec(compile(SOURCE, "<generated>", "exec"), env)
    env["BIGQMT_ACCOUNT_ID"], env["get_trade_detail_data"] = ACCOUNT, lambda *args: []
    transport = env["_load_embedded"]("bigqmt_signal_trader.transports.readonly_shm")
    transport.Win32Region = lambda *args, **kwargs: Region(state)
    original = RuntimeError("initial ready publication failed")
    with mock.patch.object(transport.ReadOnlyMailbox, "mark_ready", side_effect=original):
        with pytest.raises(RuntimeError, match="initial ready publication failed") as caught:
            env["init"](Context())
    assert caught.value is original and all(region.closed for region in state.regions)
    assert env["_SERVER"] is None


def test_generated_cleanup_error_cannot_replace_initial_ready_error():
    env, state = {}, State()
    exec(compile(SOURCE, "<generated>", "exec"), env)
    env["BIGQMT_ACCOUNT_ID"], env["get_trade_detail_data"] = ACCOUNT, lambda *args: []
    transport = env["_load_embedded"]("bigqmt_signal_trader.transports.readonly_shm")
    class CloseErrorRegion(Region):
        def close(self):
            self.closed = True
            raise OSError("cleanup error preserved")
    transport.Win32Region = lambda *args, **kwargs: CloseErrorRegion(state)
    original = RuntimeError("initial ready publication failed")
    with mock.patch.object(transport.ReadOnlyMailbox, "mark_ready", side_effect=original):
        with pytest.raises(RuntimeError) as caught:
            env["init"](Context())
    assert caught.value is original and "cleanup error preserved" in original.cleanup_error
    assert all(region.closed for region in state.regions)


def test_builder_writes_only_explicit_external_artifact_and_refuses_overwrite(tmp_path):
    output = tmp_path / "SHM_RPC_BOOTSTRAP.py"
    with mock.patch("sys.argv", ["build_shm_rpc_single_file.py", "--output", str(output)]):
        BUILDER.main()
    assert output.read_text(encoding="ascii") == SOURCE
    with mock.patch("sys.argv", ["build_shm_rpc_single_file.py", "--output", str(output)]):
        with pytest.raises(FileExistsError):
            BUILDER.main()


def test_generated_default_name_matches_gateway_trade_mailbox_contract():
    env = {}
    exec(compile(SOURCE, "<generated>", "exec"), env)
    # 与AQP gateway_config._parse_bigqmt_rpc的shm_rpc命名约束一致。
    assert env["SHM_RPC_NAME"] == "Local\\AQP_SHM_TRADE_SIM"
    assert re.fullmatch(r"Local\\AQP_SHM_TRADE_[A-Za-z0-9_-]{1,58}", env["SHM_RPC_NAME"])
    assert env["SHM_RPC_ALLOW_ORDER_METHODS"] is False


def _native_order_row(account=ACCOUNT, status=50, traded=0, volume=100, contract="7620"):
    # 保留合同7620的真实字段形状；status/traded可切换用于本地边界fixture。
    return SimpleNamespace(m_strAccountID=account, m_strOrderSysID=contract,
                           m_strInstrumentID="000001", m_strExchangeID="SZ", m_strRemark="native-short",
                           m_strStrategyName="", m_nOffsetFlag=48, m_nOpType=23,
                           m_nOrderStatus=status, m_nVolumeTotalOriginal=volume, m_nVolumeTraded=traded,
                           m_dLimitPrice=11.42, m_dTradedPrice=11.41 if traded else 0.0,
                           m_nOrderPriceType=50, m_strInsertDate="20261015", m_strInsertTime="154422")


def _native_limit_params():
    return dict(stock_code="000001.SZ", action="BUY", volume=100, price=11.42,
                price_type="LIMIT", remark="native-short", strategy_name="local-test", signal_id="local-test")


def test_generated_native_factory_candidate_is_configured_but_stays_readonly():
    source = BUILDER.build_source(account_id="55013746", name="Local\\AQP_SHM_TRADE_CORE_20261015",
                                  max_seconds=600, max_requests=1000, native_stock_gateway=True)
    h = Fixture(source=source, account="55013746")
    assert h.namespace["SHM_RPC_ALLOW_ORDER_METHODS"] is False
    assert h.namespace["SHM_RPC_ORDER_GATEWAY"] is None
    assert h.namespace["SHM_RPC_ORDER_GATEWAY_FACTORY"] is h.namespace["create_native_stock_order_gateway"]
    assert h.namespace["SHM_RPC_MAX_SECONDS"] == 600 and h.namespace["SHM_RPC_MAX_REQUESTS"] == 1000
    assert not isinstance(h.server.bridge.handlers.order_gateway, h.namespace["NativeStockOrderGateway"])
    assert "passorder" not in h.namespace and "cancel" not in h.namespace
    assert h.call("submit_order", _native_limit_params())["ok"] is False
    h.close()


def test_generated_readonly_callback_candidate_binds_account_without_native_order_functions(capsys):
    source = BUILDER.build_source(account_id="55013746", name="Local\\AQP_SHM_TRADE_CORE_20261015",
                                  max_seconds=900, max_requests=1000, native_stock_gateway=True,
                                  bind_account_callbacks=True)
    assert source.encode("ascii").decode("gbk") == source
    ast.parse(source, feature_version=(3, 6))
    h = Fixture(source=source, account="55013746")
    ready = json.loads(capsys.readouterr().out.strip().split("AQP_SHM_RPC_READY ", 1)[1])
    assert ready["source_deadline_unix"] == h.server.created_at_unix + 900
    assert ready["max_seconds"] == 900 and ready["max_requests"] == 1000
    assert ready["bind_account_callbacks"] is True and ready["allow_order_methods"] is False
    h.context.set_account.assert_called_once_with("55013746")
    assert h.namespace["SHM_RPC_ALLOW_ORDER_METHODS"] is False
    assert "passorder" not in h.namespace and "cancel" not in h.namespace
    h.namespace["order_callback"](h.context, _native_order_row(account="55013746", status=56, traded=100))
    event = h.call("poll_execution_events", {"after_sequence": 0})["data"]["events"][0]["event"]
    assert event["order_sys_id"] == "7620" and event["traded_volume"] == 100
    assert event["strategy_name"] == ""
    h.close()


def test_generated_explicit_readonly_callback_binding_requires_native_account_api():
    env = {}
    exec(compile(BUILDER.build_source(bind_account_callbacks=True), "<generated>", "exec"), env)
    env.update(BIGQMT_ACCOUNT_ID=ACCOUNT, get_trade_detail_data=lambda *args: [])
    with pytest.raises(RuntimeError, match="set_account API required"):
        env["init"](Context())
    assert env["_SERVER"] is None


def test_generated_native_gateway_uses_existing_signature_real_contract_and_empty_native_strategy():
    calls = []
    def native_submit(*args):
        calls.append(args)
        h.orders.append(_native_order_row())
        h.namespace["order_callback"](h.context, h.orders[0])
        return "THIS-IS-NOT-A-CONTRACT"
    h = Fixture(native_gateway=True, submit=native_submit)
    params = _native_limit_params()
    result = h.call("submit_order", params, request_id="native-submit")["data"]
    assert len(calls) == 1 and calls[0] == (23, 1101, ACCOUNT, "000001.SZ", 11, 11.42, 100,
                                          "local-test", 2, "native-short", h.context)
    assert result["status"] == "CONFIRMED" and result["order_sys_id"] == "7620"
    assert result["automatic_retry"] is False
    assert h.call("submit_order", params, request_id="native-submit")["data"] == result
    assert len(calls) == 1
    rows = h.call("query_orders")["data"]
    assert rows[0]["strategy_name"] == "" and rows[0]["remark"] == "native-short"
    events = h.call("poll_execution_events", {"after_sequence": 0})["data"]["events"]
    assert events[0]["event"]["order_sys_id"] == "7620" and events[0]["event"]["strategy_name"] == ""
    h.context.set_account.assert_called_once_with(ACCOUNT)
    h.close()


def test_generated_native_gateway_contract_1258_float_price_replay():
    # 2026-10-19真实回读字段；仅重放fixture，不调用QMT或外部交易接口。
    remark = "CO-f3941c6e25c14a23a5ae66f176f3600a"

    def native_submit(*args):
        row = _native_order_row(account="55013746", contract="1258")
        row.m_strRemark, row.m_dLimitPrice = remark, 11.120000000000001
        row.m_strInsertDate, row.m_strInsertTime = "20261019", "104410"
        h.orders.append(row)
        return None

    h = Fixture(native_gateway=True, submit=native_submit, account="55013746")
    try:
        result = h.call("submit_order", dict(_native_limit_params(), price=11.12, remark=remark))["data"]
        assert result["status"] == "CONFIRMED" and result["order_sys_id"] == "1258"
        assert result["automatic_retry"] is False
        assert h.query_calls[-1] == ("55013746", "STOCK", "ORDER", "")
    finally:
        h.close()


@pytest.mark.parametrize("native_price", [11.13, 11.12001, 11.12000002, float("nan"), float("inf"), -float("inf")])
def test_generated_native_gateway_price_identity_still_rejects_real_difference_or_nonfinite(native_price):
    h = Fixture(native_gateway=True)
    try:
        gateway = h.server.bridge.handlers.order_gateway
        request = h.namespace["_load_embedded"]("bigqmt_signal_trader.models").OrderRequest(
            signal_id="fixture", account_id=ACCOUNT, action="BUY", stock_code="000001.SZ",
            volume=100, price=11.12, price_type="LIMIT", strategy_name="local-test", remark="native-short")
        gateway._submitted[request.remark] = request
        row = _native_order_row()
        row.m_dLimitPrice = native_price
        h.orders.append(row)
        with pytest.raises((ValueError, RuntimeError)):
            gateway.query_orders(ACCOUNT, "")
    finally:
        h.close()


def test_generated_native_gateway_unknown_submit_never_repeats_attempt_for_same_remark():
    submit = mock.Mock(side_effect=RuntimeError("native outcome unknown"))
    h = Fixture(native_gateway=True, submit=submit)
    params = _native_limit_params()
    first = h.call("submit_order", params, request_id="unknown-one")["data"]
    assert first["status"] == "UNKNOWN" and first["automatic_retry"] is False
    second = h.call("submit_order", params, request_id="unknown-two")["data"]
    assert second["status"] == "UNKNOWN" and "attempt already consumed" in second["error"]
    assert submit.call_count == 1
    h.close()


def test_generated_native_gateway_does_not_treat_raw_submit_return_as_contract():
    submit = mock.Mock(return_value="7620")
    h = Fixture(native_gateway=True, submit=submit)
    result = h.call("submit_order", dict(_native_limit_params(), wait_settlement=False))["data"]
    assert result["status"] == "UNKNOWN" and result["order_sys_id"] == ""
    assert result["result"]["order_sys_id"] is None and submit.call_count == 1
    h.close()


def test_generated_native_gateway_cancel_requires_unique_remark_remaining_volume_and_never_retries():
    cancel = mock.Mock(side_effect=RuntimeError("native cancel outcome unknown"))
    h = Fixture(native_gateway=True, cancel=cancel)
    h.orders.append(_native_order_row())
    params = dict(order_sys_id="7620", user_order_id="native-short")
    wrong = h.call("cancel_order", dict(params, user_order_id="foreign"))["data"]
    assert wrong["status"] == "UNKNOWN" and cancel.call_count == 0
    first = h.call("cancel_order", params, request_id="cancel-one")["data"]
    second = h.call("cancel_order", params, request_id="cancel-two")["data"]
    assert first["status"] == second["status"] == "UNKNOWN"
    assert first["automatic_retry"] is False and "attempt already consumed" in second["error"]
    cancel.assert_called_once_with("7620", ACCOUNT, "STOCK", h.context)
    h.close()


@pytest.mark.parametrize("status,traded", [(54, 0), (53, 40)])
def test_generated_native_cancel_confirms_only_fresh_exact_cancelled_contract(status, traded):
    def native_cancel(*args):
        h.orders[0].m_nOrderStatus, h.orders[0].m_nVolumeTraded = status, traded
        return False  # A native synchronous return never proves the final outcome.

    h = Fixture(native_gateway=True, cancel=native_cancel)
    try:
        h.orders.append(_native_order_row())
        params = dict(order_sys_id="7620", user_order_id="native-short")
        result = h.call("cancel_order", params, request_id="cancel-terminal")['data']
        assert result["status"] == "CONFIRMED" and result["order_sys_id"] == "7620"
        assert result["automatic_retry"] is False and not result["error"]
        evidence = result["result"]
        assert evidence["cancellation_confirmed"] is True and evidence["order_terminal"] is True
        assert evidence["confirmation_source"] == "native_query_orders_strict"
        assert evidence["native_order_status"] == str(status) and evidence["traded_volume"] == traded
        assert h.call("cancel_order", params, request_id="cancel-terminal")["data"] == result
        assert h.call("cancel_order", params, request_id="cancel-again")["data"]["status"] == "UNKNOWN"
    finally:
        h.close()


def test_generated_native_cancel_queries_later_callbacks_without_repeating_native_cancel():
    cancel = mock.Mock(return_value=True)
    h = Fixture(native_gateway=True, cancel=cancel)
    try:
        h.orders.append(_native_order_row())
        gateway = h.server.bridge.handlers.order_gateway
        original, calls = gateway.query_cancel_confirmation, []

        def delayed_confirmation(result):
            calls.append(True)
            if len(calls) == 3:
                h.orders[0].m_nOrderStatus = 54
            return original(result)

        gateway.query_cancel_confirmation = delayed_confirmation
        result = h.call("cancel_order", dict(order_sys_id="7620", user_order_id="native-short"))["data"]
        assert result["status"] == "CONFIRMED" and len(calls) == 3
        assert cancel.call_count == 1
    finally:
        h.close()


def test_generated_native_cancel_true_return_without_terminal_query_stays_unknown():
    cancel = mock.Mock(return_value=True)
    h = Fixture(native_gateway=True, cancel=cancel)
    try:
        h.server.bridge.handlers.order_settle_timeout_seconds = 0.005
        h.orders.append(_native_order_row())
        result = h.call("cancel_order", dict(order_sys_id="7620", user_order_id="native-short"))["data"]
        assert result["status"] == "UNKNOWN" and result["result"]["cancellation_confirmed"] is False
        assert result["error"] == "CANCEL_CONFIRMATION_DEADLINE; outcome=UNKNOWN"
        assert cancel.call_count == 1
    finally:
        h.close()


@pytest.mark.parametrize("status,traded", [(56, 100), (57, 0)])
def test_generated_native_cancel_does_not_confuse_other_order_terminal_with_cancellation(status, traded):
    def native_cancel(*args):
        h.orders[0].m_nOrderStatus, h.orders[0].m_nVolumeTraded = status, traded
        return True

    h = Fixture(native_gateway=True, cancel=native_cancel)
    try:
        h.orders.append(_native_order_row())
        result = h.call("cancel_order", dict(order_sys_id="7620", user_order_id="native-short"))["data"]
        assert result["status"] == "UNKNOWN" and result["result"]["cancellation_confirmed"] is False
        assert result["result"]["order_terminal"] is True
        assert result["error"] == "CANCELLATION_NOT_CONFIRMED_ORDER_ALREADY_TERMINAL"
    finally:
        h.close()


@pytest.mark.parametrize("changes", [
    {"m_strRemark": "foreign"}, {"m_strInstrumentID": "000002"}, {"m_nOffsetFlag": 49, "m_nOpType": 24},
    {"m_nVolumeTotalOriginal": 200}, {"m_dLimitPrice": 11.43},
    {"m_nOrderStatus": 54, "m_nVolumeTraded": 20}, {"m_nOrderStatus": 53, "m_nVolumeTraded": 0},
    {"m_strAccountID": "OTHER"},
])
def test_generated_native_cancel_rejects_changed_identity_or_terminal_quantities(changes):
    def native_cancel(*args):
        h.orders[0].m_nOrderStatus = 54
        for key, value in changes.items():
            setattr(h.orders[0], key, value)
        return True

    h = Fixture(native_gateway=True, cancel=native_cancel)
    try:
        h.server.bridge.handlers.order_settle_timeout_seconds = 0.005
        h.orders.append(_native_order_row())
        result = h.call("cancel_order", dict(order_sys_id="7620", user_order_id="native-short"))["data"]
        assert result["status"] == "UNKNOWN" and result["result"]["cancellation_confirmed"] is False
        assert result["error"].startswith("CANCEL_CONFIRMATION_LOOKUP_FAILED:")
    finally:
        h.close()


def test_generated_cancel_lookup_failure_is_explicit_redacted_and_not_retried_as_a_write():
    def native_cancel(*args):
        h.server.bridge.handlers.order_gateway.query_orders = mock.Mock(
            side_effect=RuntimeError("redis://operator:secret@host token=private"))
        return True

    cancel = mock.Mock(side_effect=native_cancel)
    h = Fixture(native_gateway=True, cancel=cancel)
    try:
        h.server.bridge.handlers.order_settle_timeout_seconds = 0.005
        h.orders.append(_native_order_row())
        result = h.call("cancel_order", dict(order_sys_id="7620", user_order_id="native-short"))["data"]
        assert result["status"] == "UNKNOWN" and result["automatic_retry"] is False
        assert result["error"] == "CANCEL_CONFIRMATION_LOOKUP_FAILED:RuntimeError; outcome=UNKNOWN"
        assert "secret" not in json.dumps(result) and "private" not in json.dumps(result)
        assert cancel.call_count == 1
    finally:
        h.close()


def test_generated_native_cancel_rejects_traded_volume_regression():
    def native_cancel(*args):
        h.orders[0].m_nOrderStatus, h.orders[0].m_nVolumeTraded = 53, 10
        return True

    h = Fixture(native_gateway=True, cancel=native_cancel)
    try:
        h.server.bridge.handlers.order_settle_timeout_seconds = 0.005
        h.orders.append(_native_order_row(status=55, traded=20))
        result = h.call("cancel_order", dict(order_sys_id="7620", user_order_id="native-short"))["data"]
        assert result["status"] == "UNKNOWN" and result["result"]["cancellation_confirmed"] is False
        assert result["error"].startswith("CANCEL_CONFIRMATION_LOOKUP_FAILED:")
    finally:
        h.close()


@pytest.mark.parametrize("changes", [{"order_type": 27}, {"price_type": "LATEST_PRICE"},
                                      {"stock_code": "IF2401.IF"}, {"price": 0.0}])
def test_generated_native_gateway_rejects_outside_ordinary_stock_limit_interface(changes):
    submit = mock.Mock()
    h = Fixture(native_gateway=True, submit=submit)
    result = h.call("submit_order", dict(_native_limit_params(), **changes))["data"]
    assert result["status"] == "UNKNOWN" and result["order_sys_id"] == ""
    submit.assert_not_called()
    h.close()


@pytest.mark.parametrize("settings", [{"max_seconds": 1801}, {"max_requests": 1001},
                                       {"name": "Local\\OTHER"}, {"account_id": ""}])
def test_builder_rejects_unbounded_or_invalid_candidate_settings(settings):
    with pytest.raises(ValueError):
        BUILDER.build_source(**settings)
