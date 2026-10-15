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

# 已保存实机字段的本地重放；不读取VM、不制造新登录事件，不视为当前登录。
# source: ~/.codex/tmp/aqp-qmt-core-20261015-2118/runtime/core-observation-result.json
# exact JSON path: health_before.order_provider_health.trade_login_state
REAL_NATIVE_LOGIN = dict(raw_account_id="55013746", raw_status="登录成功", raw_numeric_status=None,
    source="native_account_callback", event_observed_at_utc="2026-10-15T13:44:43.989912Z",
    age_seconds=28.10899999999674, session_id="6018c77267c706812b271cffdf69afb9", native_pid=12692)


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


def test_real_saved_success_callback_stays_stale_after_cached_query_and_blocks_writes():
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
        assert state["status"] == "UNKNOWN" and state["reason"] == "TRADE_LOGIN_CALLBACK_STALE"
        assert state["event_observed_at_utc"] == REAL_NATIVE_LOGIN["event_observed_at_utc"]
        assert state["query_diagnostic"]["proves_login"] is False and state["query_diagnostic"]["raw_status"] == "登录成功"
        for method, params in (("submit_order", PROTOCOL.ORDER), ("cancel_order", {"order_sys_id": "REAL-SYSID"})):
            assert h.call(method, params)["data"]["status"] == "BLOCKED"
        assert h.namespace["passorder"].call_count == 0 and h.namespace["cancel"].call_count == 0
    finally:
        h.close()


def test_generated_login_staleness_and_new_session_clear_evidence():
    h = NATIVE.Fixture()
    try:
        h.namespace["account_callback"](h.context, {"m_strAccountID": NATIVE.ACCOUNT, "m_strStatus": "准备登录"})
        h.server.trade_login.event["observed_monotonic"] -= 16
        state = h.server.trade_login.readback()
        assert state["status"] == "UNKNOWN" and state["reason"] == "TRADE_LOGIN_CALLBACK_STALE" and state["fresh"] is False
        h.server.trade_login.bind_identity("new-session", 888)
        state = h.server.trade_login.readback()
        assert state["status"] == "UNKNOWN" and state["event_observed_at_utc"] is None
        assert state["session_id"] == "new-session" and state["native_pid"] == 888
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
