# coding: utf-8
"""独立交易登录门禁；真实已保存回调重放与工程正向替身分别注明来源。"""

import importlib.util
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest


ROOT = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("trade_login_protocol_fixture", ROOT / "test_shm_rpc.py")
PROTOCOL = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PROTOCOL)
NATIVE_SPEC = importlib.util.spec_from_file_location("trade_login_native_fixture", ROOT / "test_shm_rpc_single_file.py")
NATIVE = importlib.util.module_from_spec(NATIVE_SPEC)
NATIVE_SPEC.loader.exec_module(NATIVE)
DUAL_SPEC = importlib.util.spec_from_file_location("trade_login_dual_fixture", ROOT / "test_dual_shm_single_file.py")
DUAL = importlib.util.module_from_spec(DUAL_SPEC)
DUAL_SPEC.loader.exec_module(DUAL)

# 已保存实机字段的本地重放；不读取VM、不制造新登录事件，不视为当前登录。
# source: ~/.codex/tmp/aqp-qmt-core-20261015-2118/runtime/core-observation-result.json
# exact JSON path: health_before.order_provider_health.trade_login_state
REAL_NATIVE_LOGIN = dict(raw_account_id="55013746", raw_status="登录成功", raw_numeric_status=None,
    source="native_account_callback", event_observed_at_utc="2026-10-15T13:44:43.989912Z",
    age_seconds=28.10899999999674, session_id="6018c77267c706812b271cffdf69afb9", native_pid=12692)


class OpaqueAccountMeta(type):
    def __getattribute__(cls, name):
        if name == "__module__":
            raise AttributeError("native type has no module metadata")
        return super().__getattribute__(name)


class OpaqueAccount(metaclass=OpaqueAccountMeta):
    def __init__(self, values):
        self.__dict__.update(values)


class OpaqueTypeInfoMeta(type):
    def __getattribute__(cls, name):
        if name in ("__module__", "__name__"):
            raise RuntimeError("native type metadata unavailable")
        return super().__getattribute__(name)


class OpaqueTypeInfoAccount(metaclass=OpaqueTypeInfoMeta):
    def __init__(self, values):
        self.__dict__.update(values)


class UnreadableIdentityAccount(OpaqueAccount):
    @property
    def m_strAccountKey(self):
        raise RuntimeError("native account key unavailable")


@pytest.fixture
def live_native(monkeypatch):
    # 真实生成物+本地Win32/QMT替身；人工fixture key不代表任何实盘身份。
    h = DUAL.Fixture(monkeypatch, **DUAL.live_settings())
    _, submit, cancel = DUAL._enable_fixture_simulation_orders(h)
    h.start()
    try:
        yield h, submit, cancel
    finally:
        h.close()


def _live_account_row(h, **changes):
    return dict(dict(m_strAccountID=h.account, m_strStatus="登录成功", m_nBrokerType=2,
                     m_strAccountKey="LIVE-STOCK-FIXTURE-ONLY"), **changes)


def _live_login_state(h):
    return h.call("execution", "get_trade_login_state")["data"]


def _assert_live_login_blocks_both_writes(h, submit, cancel):
    for method, params in (("submit_order", PROTOCOL.ORDER), ("cancel_order", {"order_sys_id": "FIXTURE-SYSID"})):
        result = h.call("execution", method, params)["data"]
        assert result["status"] == "BLOCKED" and result["native_write_executed"] is False
        assert result["automatic_retry"] is False
    submit.assert_not_called()
    cancel.assert_not_called()


def test_missing_login_blocks_both_writes_and_keeps_reads_available():
    h = PROTOCOL.Harness(True)
    h.bridge.trade_login_reader = None
    try:
        h.handshake()
        for method, params in (("submit_order", PROTOCOL.ORDER), ("cancel_order", {"order_sys_id": "REAL-SYSID"})):
            value = h.execute(lambda: h.client.call(method, params))
            assert value["status"] == "BLOCKED" and value["native_write_executed"] is False
            assert value["automatic_retry"] is False and value["order_sys_id"] == ""
        assert h.gateway.submits == [] and h.gateway.cancels == []
        state = h.execute(h.client.get_trade_login_state)
        assert state["status"] == "UNKNOWN" and state["reason"] == "TRADE_LOGIN_SOURCE_UNAVAILABLE"
        assert h.execute(lambda: h.client.call("query_orders")) == []
        assert h.execute(h.client.poll_execution_events)["events"] == []
        assert h.execute(lambda: h.client.call("ping"))["pong"] is True
        assert h.client._closed is False and h.client._uncertain is False
    finally:
        h.close()


@pytest.mark.parametrize("change", [
    {"status": "UNKNOWN"}, {"status": "NOT_LOGGED_IN"}, {"age_seconds": 15.001},
    {"age_seconds": -0.1}, {"fresh": False}, {"positive_contract_verified": False},
    {"account_id": "OTHER"}, {"session_id": "OLD"}, {"native_pid": 1},
    {"source": "TASK"}, {"source": "query_ACCOUNT"}, {"source": "synthetic_ONLINE"},
    {"source": "arbitrary_nonblacklisted_source"}, {"event_observed_at_utc": None},
])
def test_unknown_or_unverified_login_has_zero_native_writes(change):
    h = PROTOCOL.Harness(True)
    reader = h.bridge.trade_login_reader
    h.bridge.trade_login_reader = lambda: dict(reader(), **change)
    try:
        h.handshake()
        for method, params in (("submit_order", PROTOCOL.ORDER), ("cancel_order", {"order_sys_id": "REAL-SYSID"})):
            assert h.execute(lambda: h.client.call(method, params))["status"] == "BLOCKED"
        assert not h.gateway.submits and not h.gateway.cancels
    finally:
        h.close()


def test_blocked_request_is_never_later_executed_and_next_write_rechecks_login():
    h = PROTOCOL.Harness(True)
    positive_fixture = h.bridge.trade_login_reader
    try:
        h.handshake()
        h.bridge.trade_login_reader = None
        blocked = h.execute(lambda: h.client.call("submit_order", PROTOCOL.ORDER, request_id="once-blocked"))
        h.bridge.trade_login_reader = positive_fixture
        assert h.execute(lambda: h.client.call("submit_order", PROTOCOL.ORDER, request_id="once-blocked")) == blocked
        assert not h.gateway.submits
        assert h.execute(lambda: h.client.call("submit_order", PROTOCOL.ORDER, request_id="new-fixture"))["status"] == "CONFIRMED"
        h.bridge.trade_login_reader = lambda: dict(positive_fixture(), status="NOT_LOGGED_IN")
        assert h.execute(lambda: h.client.call("cancel_order", {"order_sys_id": "REAL-SYSID"}))["status"] == "BLOCKED"
        assert len(h.gateway.submits) == 1 and not h.gateway.cancels
    finally:
        h.close()


def test_generated_native_assets_and_bind_do_not_prove_login_and_cache_does_not_refresh_event():
    h = NATIVE.Fixture(native_gateway=True, verified_login_fixture=False)
    try:
        h.context.set_account.assert_called_once_with(NATIVE.ACCOUNT)
        assert h.call("get_asset")["data"]["asset"]["cash"] == 1000
        state = h.call("get_trade_login_state")["data"]
        assert state["status"] == "UNKNOWN" and state["event_observed_at_utc"] is None
        assert state["query_diagnostic"]["proves_login"] is False
        h.namespace["account_callback"](h.context, SimpleNamespace(m_strAccountID=NATIVE.ACCOUNT, m_strStatus="准备登录"))
        before = h.server.trade_login.readback()
        assert before["status"] == "NOT_LOGGED_IN"
        h.server.trade_login.query = mock.Mock(return_value=[SimpleNamespace(m_strAccountID=NATIVE.ACCOUNT, m_strStatus="登录成功", m_nStatus=0)])
        after = h.call("get_trade_login_state")["data"]
        assert after["status"] == "NOT_LOGGED_IN" and after["event_observed_at_utc"] == before["event_observed_at_utc"]
        assert after["query_diagnostic"]["raw_status"] == "登录成功" and after["query_diagnostic"]["raw_numeric_status"] == 0
        assert after["query_diagnostic"]["proves_login"] is False
        for method, params in (("submit_order", PROTOCOL.ORDER), ("cancel_order", {"order_sys_id": "REAL-SYSID"})):
            assert h.call(method, params)["data"]["status"] == "BLOCKED"
        assert h.namespace["passorder"].call_count == 0 and h.namespace["cancel"].call_count == 0
    finally:
        h.close()


@pytest.mark.parametrize("raw,account,reason", [
    ("正常", NATIVE.ACCOUNT, "TRADE_LOGIN_POSITIVE_STATUS_CONTRACT_UNVERIFIED"),
    ("ONLINE", NATIVE.ACCOUNT, "TRADE_LOGIN_POSITIVE_STATUS_CONTRACT_UNVERIFIED"),
    (0, NATIVE.ACCOUNT, "TRADE_LOGIN_POSITIVE_STATUS_CONTRACT_UNVERIFIED"),
    (" 登录成功", NATIVE.ACCOUNT, "TRADE_LOGIN_POSITIVE_STATUS_CONTRACT_UNVERIFIED"),
    (None, NATIVE.ACCOUNT, "TRADE_LOGIN_POSITIVE_STATUS_CONTRACT_UNVERIFIED"),
    ("准备登录", "OTHER", "TRADE_LOGIN_CALLBACK_ACCOUNT_MISSING_OR_MISMATCH"),
    ("准备登录", None, "TRADE_LOGIN_CALLBACK_ACCOUNT_MISSING_OR_MISMATCH"),
])
def test_generated_raw_status_has_no_unverified_positive_mapping(raw, account, reason):
    h = NATIVE.Fixture()
    try:
        h.namespace["account_callback"](h.context, SimpleNamespace(m_strAccountID=account, m_strStatus=raw, m_nStatus=0))
        state = h.server.trade_login.readback()
        assert state["status"] == "UNKNOWN" and state["reason"] == reason
        assert state["raw_status"] == raw and state["raw_numeric_status"] == 0
        assert state["positive_contract_verified"] is False
    finally:
        h.close()


def test_real_saved_success_text_replay_maps_only_fresh_matching_native_callback():
    h = NATIVE.Fixture(source=NATIVE.BUILDER.build_source(bind_account_callbacks=True),
        account=REAL_NATIVE_LOGIN["raw_account_id"])
    try:
        assert h.namespace["SHM_RPC_ALLOW_ORDER_METHODS"] is False
        assert h.namespace["SHM_RPC_ORDER_GATEWAY"] is None
        h.namespace["account_callback"](h.context, dict(m_strAccountID=REAL_NATIVE_LOGIN["raw_account_id"],
            m_strStatus=REAL_NATIVE_LOGIN["raw_status"], m_nStatus=REAL_NATIVE_LOGIN["raw_numeric_status"]))
        event = h.server.trade_login.event
        event["observed_at_utc"] = REAL_NATIVE_LOGIN["event_observed_at_utc"]
        # 明确是保存字段的时钟fixture；只有<=15s的边界表示工程正向，非当前实机登录。
        with mock.patch.object(h.namespace["time"], "monotonic", return_value=event["observed_monotonic"] + 15):
            fresh = h.server.trade_login.readback()
        assert fresh["status"] == "LOGGED_IN" and fresh["positive_contract_verified"] is True
        assert fresh["raw_status"] == REAL_NATIVE_LOGIN["raw_status"] and fresh["source"] == REAL_NATIVE_LOGIN["source"]
        assert fresh["fresh"] is True and fresh["age_seconds"] == 15
        event["raw_account_id"] = "OTHER"
        assert h.server.trade_login.readback()["status"] == "UNKNOWN"
        event["raw_account_id"] = REAL_NATIVE_LOGIN["raw_account_id"]
        event["identity"] = ("OLD_SESSION", 1)
        assert h.server.trade_login.readback()["reason"] == "TRADE_LOGIN_SESSION_CHANGED"
    finally:
        h.close()


def test_real_saved_success_callback_age_is_preserved_with_one_current_consistent_sample():
    h = NATIVE.Fixture(native_gateway=True, verified_login_fixture=False,
        account=REAL_NATIVE_LOGIN["raw_account_id"])
    try:
        h.namespace["account_callback"](h.context, dict(m_strAccountID=REAL_NATIVE_LOGIN["raw_account_id"],
            m_strStatus=REAL_NATIVE_LOGIN["raw_status"]))
        event = h.server.trade_login.event
        event["observed_monotonic"] -= REAL_NATIVE_LOGIN["age_seconds"]
        event["observed_at_utc"] = REAL_NATIVE_LOGIN["event_observed_at_utc"]
        h.server.trade_login.query = mock.Mock(return_value=[dict(m_strAccountID=REAL_NATIVE_LOGIN["raw_account_id"],
            m_strStatus=REAL_NATIVE_LOGIN["raw_status"])])
        state = h.call("get_trade_login_state")["data"]
        assert state["status"] == "LOGGED_IN" and state["login_evidence_version"] == 2
        assert state["age_seconds"] >= REAL_NATIVE_LOGIN["age_seconds"]
        assert state["state_event_age_seconds"] == state["age_seconds"]
        assert state["fresh"] is True and 0 <= state["sample_age_seconds"] <= 15
        assert state["event_observed_at_utc"] == REAL_NATIVE_LOGIN["event_observed_at_utc"]
        assert state["query_diagnostic"]["proves_login"] is False and state["query_diagnostic"]["raw_status"] == "登录成功"
        h.server.trade_login.query.assert_called_once_with(REAL_NATIVE_LOGIN["raw_account_id"], "STOCK", "ACCOUNT")
        assert h.namespace["passorder"].call_count == 0 and h.namespace["cancel"].call_count == 0
    finally:
        h.close()


def test_generated_negative_event_does_not_age_into_success_and_new_session_clears_evidence():
    h = NATIVE.Fixture()
    try:
        h.namespace["account_callback"](h.context, {"m_strAccountID": NATIVE.ACCOUNT, "m_strStatus": "准备登录"})
        h.server.trade_login.event["observed_monotonic"] -= 16
        state = h.server.trade_login.readback()
        assert state["status"] == "NOT_LOGGED_IN" and state["reason"] == "TRADE_LOGIN_PREPARING_LOGIN"
        h.server.trade_login.bind_identity("new-session", 888)
        state = h.server.trade_login.readback()
        assert state["status"] == "UNKNOWN" and state["event_observed_at_utc"] is None
        assert state["session_id"] == "new-session" and state["native_pid"] == 888
    finally:
        h.close()


def test_generated_v2_old_state_event_with_fresh_sample_permits_only_exact_session_order():
    def submitted(*args):
        h.orders.append(NATIVE._native_order_row())

    h = NATIVE.Fixture(native_gateway=True, verified_login_fixture=False, submit=submitted)
    try:
        h.namespace["account_callback"](h.context, dict(m_strAccountID=NATIVE.ACCOUNT, m_strStatus="登录成功"))
        event = h.server.trade_login.event
        event["observed_monotonic"] -= 120
        original_at = event["observed_at_utc"]
        state = h.call("get_trade_login_state")["data"]
        assert state["status"] == "LOGGED_IN" and state["age_seconds"] >= 120
        assert state["event_observed_at_utc"] == original_at
        assert state["freshness_basis"] == "native_callback_state_with_current_account_sample"
        result = h.call("submit_order", NATIVE._native_limit_params())["data"]
        assert result["status"] == "CONFIRMED" and result["order_sys_id"] == "7620"
        h.namespace["account_callback"](h.context, dict(m_strAccountID=NATIVE.ACCOUNT, m_strStatus="准备登录"))
        blocked = h.call("cancel_order", dict(order_sys_id="7620", user_order_id="native-short"))["data"]
        assert blocked["status"] == "BLOCKED" and h.namespace["cancel"].call_count == 0
    finally:
        h.close()


@pytest.mark.parametrize("rows", [[], None, [dict(m_strAccountID=NATIVE.ACCOUNT, m_strStatus="登录成功")] * 2,
    [dict(m_strAccountID="OTHER", m_strStatus="登录成功")], [dict(m_strAccountID=NATIVE.ACCOUNT)],
    [dict(m_strAccountID=NATIVE.ACCOUNT, m_strStatus="准备登录")],
    [dict(m_strAccountID=NATIVE.ACCOUNT, m_strStatus="LOGGED_OUT")],
    [dict(m_strAccountID=NATIVE.ACCOUNT, m_strStatus="UNKNOWN")]])
def test_generated_v2_negative_missing_or_conflicting_sample_blocks_both_native_writes(rows):
    h = NATIVE.Fixture(native_gateway=True, verified_login_fixture=False)
    try:
        h.namespace["account_callback"](h.context, dict(m_strAccountID=NATIVE.ACCOUNT, m_strStatus="登录成功"))
        h.server.trade_login.query = mock.Mock(return_value=rows)
        for method, params in (("submit_order", PROTOCOL.ORDER), ("cancel_order", {"order_sys_id": "REAL-SYSID"})):
            result = h.call(method, params)["data"]
            assert result["status"] == "BLOCKED" and result["native_write_executed"] is False
        assert h.namespace["passorder"].call_count == h.namespace["cancel"].call_count == 0
    finally:
        h.close()


def test_generated_v2_query_error_is_redacted_and_fresh_cache_alone_never_proves_login():
    h = NATIVE.Fixture(native_gateway=True, verified_login_fixture=False)
    try:
        state = h.call("get_trade_login_state")["data"]
        assert state["status"] == "UNKNOWN" and state["query_diagnostic"]["raw_status"] == "登录成功"
        h.namespace["account_callback"](h.context, dict(m_strAccountID=NATIVE.ACCOUNT, m_strStatus="登录成功"))
        h.server.trade_login.query = mock.Mock(side_effect=RuntimeError("password=private"))
        state = h.call("get_trade_login_state")["data"]
        assert state["status"] == "UNKNOWN" and state["reason"] == "TRADE_LOGIN_ACCOUNT_SAMPLE_FAILED"
        assert state["query_diagnostic"]["exception_type"] == "RuntimeError"
        assert "private" not in str(state)
        assert h.call("submit_order", PROTOCOL.ORDER)["data"]["status"] == "BLOCKED"
        assert h.namespace["passorder"].call_count == 0
    finally:
        h.close()


@pytest.mark.parametrize("change", [dict(login_evidence_version=3), dict(login_evidence_version=True),
    dict(freshness_basis="cache_only"), dict(state_event_age_seconds=0), dict(sample_age_seconds=15.001),
    dict(sample_age_seconds=float("nan")), dict(sample_age_seconds=-1), dict(sample_observed_at_utc=None),
    dict(sample_observed_at_utc="not-a-date"), dict(event_observed_at_utc="not-a-date"),
    dict(raw_account_id="OTHER"), dict(raw_status="UNKNOWN"), dict(query_diagnostic={})])
def test_bridge_rejects_malformed_v2_proof_with_zero_native_writes(change):
    h = NATIVE.Fixture(native_gateway=True, verified_login_fixture=False)
    try:
        h.namespace["account_callback"](h.context, dict(m_strAccountID=NATIVE.ACCOUNT, m_strStatus="登录成功"))
        h.server.trade_login.event["observed_monotonic"] -= 120
        state = h.server.trade_login.readback()
        h.server.bridge.trade_login_reader = lambda: dict(state, **change)
        assert h.call("submit_order", PROTOCOL.ORDER)["data"]["status"] == "BLOCKED"
        assert h.namespace["passorder"].call_count == 0
    finally:
        h.close()


def test_generated_stop_clears_positive_callback_evidence():
    h = NATIVE.Fixture(native_gateway=True, verified_login_fixture=False)
    try:
        h.namespace["account_callback"](h.context, dict(m_strAccountID=NATIVE.ACCOUNT, m_strStatus="登录成功"))
        assert h.server.trade_login.readback()["status"] == "LOGGED_IN"
        h.namespace["stop"](h.context)
        assert h.server.trade_login.event is None
        assert h.server.trade_login.readback()["status"] == "UNKNOWN"
    finally:
        h.client.close()


def test_generated_v2_slow_account_sample_is_stale_and_cannot_authorize():
    h = NATIVE.Fixture(native_gateway=True, verified_login_fixture=False)
    try:
        h.namespace["account_callback"](h.context, dict(m_strAccountID=NATIVE.ACCOUNT, m_strStatus="登录成功"))
        start = h.server.trade_login.event["observed_monotonic"] + 100
        with mock.patch.object(h.namespace["time"], "monotonic", side_effect=[start, start + 16, start + 16]):
            state = h.server.trade_login.readback()
        assert state["fresh"] is False and state["status"] == "UNKNOWN"
        assert state["reason"] == "TRADE_LOGIN_ACCOUNT_SAMPLE_STALE"
        h.server.bridge.trade_login_reader = lambda: state
        assert h.call("submit_order", PROTOCOL.ORDER)["data"]["status"] == "BLOCKED"
        assert h.namespace["passorder"].call_count == 0
    finally:
        h.close()


def test_bridge_cannot_reuse_old_sample_by_freezing_its_reported_age_at_zero():
    h = NATIVE.Fixture(native_gateway=True, verified_login_fixture=False)
    try:
        h.namespace["account_callback"](h.context, dict(m_strAccountID=NATIVE.ACCOUNT, m_strStatus="登录成功"))
        state = h.server.trade_login.readback()
        state["sample_observed_at_utc"] = "2001-01-01T00:00:00Z"
        state["query_diagnostic"]["query_observed_at_utc"] = state["sample_observed_at_utc"]
        state["sample_age_seconds"] = 0
        h.server.bridge.trade_login_reader = lambda: state
        assert h.call("submit_order", PROTOCOL.ORDER)["data"]["status"] == "BLOCKED"
        assert h.namespace["passorder"].call_count == 0
    finally:
        h.close()


def test_native_sample_cannot_rebind_new_event_into_previous_session_response():
    h = NATIVE.Fixture(native_gateway=True, verified_login_fixture=False)
    try:
        state = h.server.trade_login

        def changed_during_query(*args):
            state.bind_identity("new-session", 888)
            state.record_account_callback(dict(m_strAccountID=NATIVE.ACCOUNT, m_strStatus="登录成功"))
            return [dict(m_strAccountID=NATIVE.ACCOUNT, m_strStatus="登录成功")]

        state.query = changed_during_query
        result = h.call("submit_order", PROTOCOL.ORDER)["data"]
        assert result["status"] == "BLOCKED"
        assert result["error"] == "TRADE_LOGIN_SESSION_CHANGED"
        assert h.namespace["passorder"].call_count == 0
    finally:
        h.close()


def test_final_native_function_boundary_rechecks_login_even_after_positive_gate():
    h = NATIVE.Fixture(native_gateway=True)
    try:
        assert h.server.bridge._trade_login()["status"] == "LOGGED_IN"
        h.server.bridge.trade_login_reader = None
        gateway = h.server.bridge.handlers.order_gateway
        with pytest.raises(PermissionError, match="TRADE_LOGIN_SOURCE_UNAVAILABLE"):
            gateway.passorder(23, 1101, NATIVE.ACCOUNT, "000001.SZ", 11, 10, 100, "", 2, "fixture", h.context)
        with pytest.raises(PermissionError, match="TRADE_LOGIN_SOURCE_UNAVAILABLE"):
            gateway.cancel_func("REAL-SYSID", NATIVE.ACCOUNT, "STOCK", h.context)
        assert h.namespace["passorder"].call_count == 0 and h.namespace["cancel"].call_count == 0
    finally:
        h.close()


def test_live_stock_identity_success_and_same_fund_credit_callback_does_not_overwrite(live_native):
    h, submit, cancel = live_native
    h.namespace["account_callback"](h.context, _live_account_row(h))
    before = _live_login_state(h)
    assert before["status"] == "LOGGED_IN" and before["positive_contract_verified"] is True
    assert before["raw_broker_type"] == before["query_diagnostic"]["raw_broker_type"] == 2
    assert before["raw_account_key"] == before["bound_account_key"] == before["query_diagnostic"]["raw_account_key"]
    assert before["raw_object_type"] == "builtins.dict"
    assert before["query_diagnostic"]["raw_object_type"] == "types.SimpleNamespace"
    h.namespace["account_callback"](h.context, _live_account_row(h, m_nBrokerType=3,
        m_strAccountKey="LIVE-CREDIT-FIXTURE-ONLY", m_strStatus="尚未登录"))
    after = _live_login_state(h)
    assert after["status"] == "LOGGED_IN" and after["event_observed_at_utc"] == before["event_observed_at_utc"]
    ignored = after["account_instance_diagnostics"][-1]
    assert ignored["reason"] == "NON_STOCK_CALLBACK_IGNORED" and ignored["raw_broker_type"] == 3
    assert ignored["raw_status"] == "尚未登录" and ignored["raw_account_key"] == "LIVE-CREDIT-FIXTURE-ONLY"
    assert ignored["session_id"] == after["session_id"] and ignored["native_pid"] == after["native_pid"]
    submit.assert_not_called()
    cancel.assert_not_called()


def test_live_cache_and_only_foreign_callback_never_prove_target_login(live_native):
    h, submit, cancel = live_native
    h.namespace["account_callback"](h.context, _live_account_row(h, m_nBrokerType=3,
        m_strAccountKey="LIVE-CREDIT-FIXTURE-ONLY", m_strStatus="尚未登录"))
    state = _live_login_state(h)
    assert state["status"] == "UNKNOWN" and state["reason"] == "TRADE_LOGIN_CALLBACK_NOT_OBSERVED"
    assert state["query_diagnostic"]["raw_status"] == "登录成功"
    assert state["query_diagnostic"]["proves_login"] is False
    assert state["bound_account_key"] == "LIVE-STOCK-FIXTURE-ONLY"
    _assert_live_login_blocks_both_writes(h, submit, cancel)


def test_live_same_type_different_instance_key_stays_unknown(live_native):
    h, submit, cancel = live_native
    h.namespace["account_callback"](h.context, _live_account_row(h))
    assert _live_login_state(h)["status"] == "LOGGED_IN"
    h.namespace["account_callback"](h.context, _live_account_row(h, m_strAccountKey="DIFFERENT-STOCK-INSTANCE"))
    state = _live_login_state(h)
    assert state["status"] == "UNKNOWN" and state["reason"] == "TRADE_LOGIN_CALLBACK_ACCOUNT_KEY_MISMATCH"
    assert state["raw_account_key"] != state["bound_account_key"]
    assert state["account_instance_diagnostics"][-1]["reason"] == "CALLBACK_INSTANCE_KEY_CONFLICT"
    _assert_live_login_blocks_both_writes(h, submit, cancel)


@pytest.mark.parametrize("changes", [dict(m_nBrokerType=None), dict(m_nBrokerType=True),
    dict(m_nBrokerType=0), dict(m_nBrokerType=999), dict(m_nBrokerType="2"),
    dict(m_strAccountKey=None), dict(m_strAccountKey=""), dict(m_strAccountKey=" "),
    dict(m_strAccountKey=2), dict(m_strAccountKey="x" * 257)])
def test_live_missing_or_unverified_callback_identity_cannot_keep_old_success(live_native, changes):
    h, submit, cancel = live_native
    h.namespace["account_callback"](h.context, _live_account_row(h))
    assert _live_login_state(h)["status"] == "LOGGED_IN"
    h.namespace["account_callback"](h.context, _live_account_row(h, **changes))
    state = _live_login_state(h)
    assert state["status"] == "UNKNOWN" and state["positive_contract_verified"] is False
    assert state["account_instance_diagnostics"][-1]["reason"] == "CALLBACK_INSTANCE_IDENTITY_MISSING"
    _assert_live_login_blocks_both_writes(h, submit, cancel)


@pytest.mark.parametrize("status,reason", [("尚未登录", "TRADE_LOGIN_NOT_LOGGED_IN"),
                                         ("准备登录", "TRADE_LOGIN_PREPARING_LOGIN")])
def test_live_true_target_negative_overrides_success_despite_positive_query(live_native, status, reason):
    h, submit, cancel = live_native
    h.namespace["account_callback"](h.context, _live_account_row(h))
    assert _live_login_state(h)["status"] == "LOGGED_IN"
    h.namespace["account_callback"](h.context, _live_account_row(h, m_strStatus=status))
    state = _live_login_state(h)
    assert state["status"] == "NOT_LOGGED_IN" and state["reason"] == reason
    assert state["query_diagnostic"]["raw_status"] == "登录成功"
    assert state["positive_contract_verified"] is False
    _assert_live_login_blocks_both_writes(h, submit, cancel)


@pytest.mark.parametrize("changes", [dict(m_nBrokerType=None), dict(m_nBrokerType=3),
    dict(m_strAccountKey=None), dict(m_strAccountKey="DIFFERENT-STOCK-INSTANCE"), dict(m_strAccountID="OTHER")])
def test_live_current_account_sample_identity_vetoes_success(live_native, changes):
    h, submit, cancel = live_native
    h.namespace["account_callback"](h.context, _live_account_row(h))
    assert _live_login_state(h)["status"] == "LOGGED_IN"
    h.namespace["_RUNTIMES"]["execution"]["_SERVER"].trade_login.query = mock.Mock(
        return_value=[_live_account_row(h, **changes)])
    state = _live_login_state(h)
    assert state["status"] == "UNKNOWN" and state["positive_contract_verified"] is False
    assert state["query_diagnostic"]["error"] and state["query_diagnostic"]["proves_login"] is False
    _assert_live_login_blocks_both_writes(h, submit, cancel)


def test_live_same_type_new_instance_cannot_rebind_inside_one_sid(live_native):
    h, submit, cancel = live_native
    h.namespace["account_callback"](h.context, _live_account_row(h))
    first = _live_login_state(h)
    native = h.namespace["_RUNTIMES"]["execution"]["_SERVER"].trade_login
    native.query = mock.Mock(return_value=[_live_account_row(h, m_strAccountKey="NEW-STOCK-INSTANCE")])
    h.namespace["account_callback"](h.context, _live_account_row(h, m_strAccountKey="NEW-STOCK-INSTANCE"))
    state = _live_login_state(h)
    assert state["status"] == "UNKNOWN" and state["reason"] == "TRADE_LOGIN_ACCOUNT_SAMPLE_KEY_CHANGED"
    assert state["bound_account_key"] == first["bound_account_key"]
    _assert_live_login_blocks_both_writes(h, submit, cancel)


def test_live_current_sample_negative_vetoes_positive_callback(live_native):
    h, submit, cancel = live_native
    h.namespace["account_callback"](h.context, _live_account_row(h))
    native = h.namespace["_RUNTIMES"]["execution"]["_SERVER"].trade_login
    native.query = mock.Mock(return_value=[_live_account_row(h, m_strStatus="尚未登录")])
    state = _live_login_state(h)
    assert state["status"] == "NOT_LOGGED_IN" and state["reason"] == "TRADE_LOGIN_ACCOUNT_SAMPLE_NOT_LOGGED_IN"
    _assert_live_login_blocks_both_writes(h, submit, cancel)


def test_live_instance_diagnostics_bounded_and_new_identity_clears_everything(live_native):
    h, submit, cancel = live_native
    h.namespace["account_callback"](h.context, _live_account_row(h))
    assert _live_login_state(h)["status"] == "LOGGED_IN"
    for number in range(5):
        h.namespace["account_callback"](h.context, _live_account_row(h, m_nBrokerType=3,
            m_strAccountKey="CREDIT-FIXTURE-%d" % number, m_strStatus="尚未登录"))
    state = _live_login_state(h)
    assert len(state["account_instance_diagnostics"]) == 3 and state["status"] == "LOGGED_IN"
    assert [row["raw_account_key"] for row in state["account_instance_diagnostics"]] == [
        "CREDIT-FIXTURE-2", "CREDIT-FIXTURE-3", "CREDIT-FIXTURE-4"]
    native = h.namespace["_RUNTIMES"]["execution"]["_SERVER"].trade_login
    native.bind_identity("new-fixture-sid", 888)
    assert native.event is None and native.account_key is None and native.instance_diagnostics == []
    assert native.readback()["status"] == "UNKNOWN"
    submit.assert_not_called()
    cancel.assert_not_called()


def test_sim_account_login_contract_is_unchanged_by_live_identity_fields():
    h = NATIVE.Fixture(native_gateway=True, verified_login_fixture=False)
    try:
        h.namespace["account_callback"](h.context, dict(m_strAccountID=NATIVE.ACCOUNT, m_strStatus="登录成功"))
        state = h.call("get_trade_login_state")["data"]
        assert state["status"] == "LOGGED_IN" and "account_instance_contract" not in state
        h.namespace["account_callback"](h.context, dict(m_strAccountID=NATIVE.ACCOUNT, m_strStatus="尚未登录"))
        state = h.call("get_trade_login_state")["data"]
        assert state["status"] == "UNKNOWN" and state["reason"] == "TRADE_LOGIN_POSITIVE_STATUS_CONTRACT_UNVERIFIED"
    finally:
        h.close()


def test_live_stopped_identity_cannot_reacquire_instance_anchor_from_query(live_native):
    h, submit, cancel = live_native
    h.namespace["account_callback"](h.context, _live_account_row(h))
    assert _live_login_state(h)["status"] == "LOGGED_IN"
    native = h.namespace["_RUNTIMES"]["execution"]["_SERVER"].trade_login
    native.bind_identity(None, None)
    assert native.readback()["status"] == "UNKNOWN"
    assert native.event is None and native.account_key is None and native.instance_diagnostics == []
    submit.assert_not_called()
    cancel.assert_not_called()


@pytest.mark.parametrize("factory,expected_type", [(OpaqueAccount, "OpaqueAccount"),
                                                  (OpaqueTypeInfoAccount, "UNAVAILABLE")],
                         ids=["missing-module", "unavailable-module-and-name"])
def test_live_opaque_type_metadata_cannot_destroy_valid_instance_evidence(live_native, factory, expected_type):
    h, submit, cancel = live_native
    native = h.namespace["_RUNTIMES"]["execution"]["_SERVER"].trade_login
    native.query = mock.Mock(return_value=[factory(_live_account_row(h))])
    before = _live_login_state(h)
    assert before["status"] == "UNKNOWN" and before["reason"] == "TRADE_LOGIN_CALLBACK_NOT_OBSERVED"
    assert "error" not in before["query_diagnostic"]
    assert before["query_diagnostic"]["raw_broker_type"] == 2
    assert before["query_diagnostic"]["raw_account_key"] == "LIVE-STOCK-FIXTURE-ONLY"
    assert before["query_diagnostic"]["raw_object_type"] == expected_type
    _assert_live_login_blocks_both_writes(h, submit, cancel)
    h.namespace["account_callback"](h.context, factory(_live_account_row(h)))
    after = _live_login_state(h)
    assert after["status"] == "LOGGED_IN" and after["positive_contract_verified"] is True
    assert after["raw_object_type"] == expected_type
    assert after["raw_broker_type"] == after["query_diagnostic"]["raw_broker_type"] == 2
    assert after["raw_account_key"] == after["query_diagnostic"]["raw_account_key"]
    submit.assert_not_called()
    cancel.assert_not_called()


def test_live_opaque_type_with_missing_identity_stays_unknown(live_native):
    h, submit, cancel = live_native
    native = h.namespace["_RUNTIMES"]["execution"]["_SERVER"].trade_login
    missing = _live_account_row(h)
    missing.pop("m_strAccountKey")
    native.query = mock.Mock(return_value=[OpaqueAccount(missing)])
    h.namespace["account_callback"](h.context, OpaqueAccount(missing))
    state = _live_login_state(h)
    assert state["status"] == "UNKNOWN" and state["positive_contract_verified"] is False
    assert state["query_diagnostic"]["raw_broker_type"] == 2
    assert state["query_diagnostic"]["raw_account_key"] is None
    assert state["query_diagnostic"]["error"] == "TRADE_LOGIN_ACCOUNT_SAMPLE_KEY_MISSING"
    _assert_live_login_blocks_both_writes(h, submit, cancel)


def test_live_opaque_identity_accessor_failure_is_missing_identity_not_query_failure(live_native):
    h, submit, cancel = live_native
    native = h.namespace["_RUNTIMES"]["execution"]["_SERVER"].trade_login
    row = UnreadableIdentityAccount(_live_account_row(h))
    native.query = mock.Mock(return_value=[row])
    h.namespace["account_callback"](h.context, row)
    state = _live_login_state(h)
    assert state["status"] == "UNKNOWN" and state["positive_contract_verified"] is False
    assert state["query_diagnostic"]["raw_broker_type"] == 2
    assert state["query_diagnostic"]["raw_account_key"] is None
    assert state["query_diagnostic"]["error"] == "TRADE_LOGIN_ACCOUNT_SAMPLE_KEY_MISSING"
    assert state["reason"] == "TRADE_LOGIN_CALLBACK_KEY_MISSING"
    _assert_live_login_blocks_both_writes(h, submit, cancel)
