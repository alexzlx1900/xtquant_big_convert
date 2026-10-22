"""显式LIVE当前账户登录合同；真实生成物配合本地QMT/Win32替身，无实机交易。"""
import importlib.util
from pathlib import Path
from unittest import mock

import pytest


SPEC = importlib.util.spec_from_file_location("live_query_login_fixture", Path(__file__).with_name("test_shm_trade_login.py"))
BASE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(BASE)


@pytest.fixture
def native(monkeypatch):
    h = BASE.DUAL.Fixture(monkeypatch, **dict(BASE.DUAL.live_settings(), live_account_query_login=True))
    _, submit, cancel = BASE.DUAL._enable_fixture_simulation_orders(h)
    h.start()
    try:
        yield h, submit, cancel
    finally:
        h.close()


def test_current_account_query_proves_login_without_inventing_callback(native):
    h, submit, cancel = native
    state = BASE._live_login_state(h)
    assert state["status"] == "LOGGED_IN" and state["login_evidence_version"] == 3
    assert state["source"] == "native_account_query" and state["positive_contract_verified"] is True
    assert state["event_observed_at_utc"] is None and state["callback_diagnostic"] == {}
    assert state["query_diagnostic"]["proves_login"] is True
    assert h.namespace["_RUNTIMES"]["execution"]["_SERVER"].bridge._trade_login()["status"] == "LOGGED_IN"
    submit.assert_not_called()
    cancel.assert_not_called()


def test_current_query_is_authority_and_conflicting_callback_is_preserved(native):
    h, submit, cancel = native
    h.namespace["account_callback"](h.context, BASE._live_account_row(h, m_strStatus="尚未登录"))
    state = BASE._live_login_state(h)
    assert state["status"] == "LOGGED_IN" and state["raw_status"] == "登录成功"
    assert state["callback_status_conflict"] is True
    assert state["callback_diagnostic"]["raw_status"] == "尚未登录"
    assert state["event_observed_at_utc"] == state["callback_diagnostic"]["observed_at_utc"]
    submit.assert_not_called()
    cancel.assert_not_called()


@pytest.mark.parametrize("status", ["准备登录", "尚未登录", "登录失败", "已断开", "正常", None, " 登录成功"])
def test_negative_or_unknown_current_sample_blocks_both_writes(native, status):
    h, submit, cancel = native
    h.namespace["account_callback"](h.context, BASE._live_account_row(h))
    login = h.namespace["_RUNTIMES"]["execution"]["_SERVER"].trade_login
    login.query = mock.Mock(return_value=[BASE._live_account_row(h, m_strStatus=status)])
    assert BASE._live_login_state(h)["status"] != "LOGGED_IN"
    BASE._assert_live_login_blocks_both_writes(h, submit, cancel)


@pytest.mark.parametrize("change", [dict(m_strAccountID="OTHER"), dict(m_nBrokerType=3),
    dict(m_nBrokerType=True), dict(m_strAccountKey=None), dict(m_strAccountKey="NEW-INSTANCE")])
def test_current_query_identity_cannot_change_inside_one_session(native, change):
    h, submit, cancel = native
    assert BASE._live_login_state(h)["status"] == "LOGGED_IN"
    login = h.namespace["_RUNTIMES"]["execution"]["_SERVER"].trade_login
    login.query = mock.Mock(return_value=[BASE._live_account_row(h, **change)])
    assert BASE._live_login_state(h)["status"] == "UNKNOWN"
    BASE._assert_live_login_blocks_both_writes(h, submit, cancel)


@pytest.mark.parametrize("change", [dict(sample_age_seconds=16), dict(sample_age_seconds=float("nan")),
    dict(sample_observed_at_utc="2001-01-01T00:00:00Z"), dict(source="native_account_callback"),
    dict(bound_account_key="OTHER"), dict(raw_broker_type=True), dict(query_diagnostic={})])
def test_native_write_boundary_rejects_malformed_v3_proof(native, change):
    h, submit, cancel = native
    server = h.namespace["_RUNTIMES"]["execution"]["_SERVER"]
    state = dict(server.trade_login.readback(), **change)
    server.bridge.trade_login_reader = lambda: state
    BASE._assert_live_login_blocks_both_writes(h, submit, cancel)


def test_query_failure_and_inflight_session_change_do_not_reuse_success(native):
    h, submit, cancel = native
    login = h.namespace["_RUNTIMES"]["execution"]["_SERVER"].trade_login
    assert BASE._live_login_state(h)["status"] == "LOGGED_IN"
    login.query = mock.Mock(side_effect=RuntimeError("fixture query failure"))
    assert BASE._live_login_state(h)["status"] == "UNKNOWN"
    BASE._assert_live_login_blocks_both_writes(h, submit, cancel)


def test_simulation_cannot_enable_live_query_authority():
    with pytest.raises(ValueError, match="LIVE"):
        BASE.DUAL.BUILDER.build_source(**dict(BASE.DUAL.settings(), live_account_query_login=True))
