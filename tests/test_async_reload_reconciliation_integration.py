# coding: utf-8
"""实际源重载、SHM读取和单RID终态核对；全程仅内存/native API替身。"""

from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
from types import SimpleNamespace

import pytest

from bigqmt_signal_trader.transports.readonly_shm import ShmProtocolError
from test_async_external_native_path import NativePath
from test_async_write_reconciliation import ReconciliationRedis
from test_live_redis_shm_proxy import ACCOUNT, LEDGER, request


RID, CID, CONTRACT = "reload-unknown-rid", "reload-unknown-cid", "765120"
IDENTITY = dict(user_order_id=CID, stock_code="000001.SZ", action="BUY", volume=100, price=10.0)


def native_query(book, original, account, kind, detail, strategy_name=""):
    if detail == "ORDER":
        return book
    if detail == "DEAL":
        return []
    return original(account, kind, detail, strategy_name)


def terminal_row(record, status):
    stamp = datetime.fromtimestamp(record["claimed_at_unix"], timezone(timedelta(hours=8)))
    return SimpleNamespace(
        m_strAccountID=ACCOUNT, m_strOrderSysID=CONTRACT, m_strRemark=CID,
        m_strInstrumentID="000001", m_strExchangeID="SZ", m_nOpType=23,
        m_nOffsetFlag=48, m_nOrderStatus=status, m_nVolumeTotalOriginal=100,
        m_nVolumeTraded=0, m_dLimitPrice=10.0, m_dTradedPrice=0.0,
        m_nOrderPriceType=50, m_strStrategyName="async-local",
        m_strInsertDate=stamp.strftime("%Y%m%d"), m_strInsertTime=stamp.strftime("%H%M%S"),
    )


@pytest.mark.parametrize("terminal", [False, True])
def test_two_new_sids_restore_actual_reads_and_reconcile_only_exact_old_unknown(monkeypatch, terminal):
    provider, book = ReconciliationRedis(), []
    query = lambda *args, **kwargs: native_query(book, *args, **kwargs)
    original = NativePath(monkeypatch, redis_provider=provider, native_query=query, fail_native=True)
    first = original.send(RID, CID)
    assert first["status"] == "UNKNOWN" and len(original.native_calls) == 1
    encoded = provider.hget(LEDGER, RID)
    record = json.loads(encoded)
    old_binding = deepcopy(record["binding"])
    cid_claim = provider.hget(LEDGER + ":cid", CID)
    original.close()
    book.append(terminal_row(record, 54 if terminal else 50))

    current = NativePath(monkeypatch, redis_provider=provider, native_query=query, start_write_blocked=True)
    try:
        new_binding = current.proxy._write_binding()
        assert new_binding["read_session_id"] != old_binding["read_session_id"]
        assert new_binding["execution_session_id"] != old_binding["execution_session_id"]
        assert new_binding["account_id"] == old_binding["account_id"] == ACCOUNT
        asset = current.execute(current.proxy.process_request, request("get_asset", rid="after-reload-read"))
        assert asset["ok"] is True and asset["data"]["cash"] == 1000
        assert current.proxy.identity_snapshot()["writes_blocked"] is True
        before = deepcopy(provider.hashes)
        transition = dict(original_binding=old_binding, evidence_binding=new_binding)
        operation_start = len(provider.operations)
        preview_call = lambda: current.proxy.reconcile_unknown_write(
            RID, CONTRACT, IDENTITY, source_transition=transition)
        if not terminal:
            with pytest.raises(ShmProtocolError):
                current.execute(preview_call)
            assert provider.hashes == before and current.native_calls == []
            return

        preview = current.execute(preview_call)
        assert preview["ledger_records_written"] == 0 and preview["native_write_calls"] == 0
        assert preview["expected_record"] == encoded and provider.hashes == before
        applied = current.execute(lambda: current.proxy.reconcile_unknown_write(
            RID, CONTRACT, IDENTITY, source_transition=transition, expected_record=encoded,
            operator="local-contract-test", reason="exact old CID terminal proof after two-source reload", apply=True))
        assert applied["status"] == "RECONCILED" and applied["ledger_records_written"] == 1
        saved = json.loads(provider.hget(LEDGER, RID))
        assert saved["binding"] == old_binding
        for key in ("response", "fingerprint", "token", "claimed_at_unix"):
            assert saved[key] == record[key]
        assert saved["reconciliation"]["source_transition"] == transition
        assert provider.hget(LEDGER + ":cid", CID) == cid_claim == RID
        assert set(provider.hgetall(LEDGER)) == {RID}
        assert not any(row[0] in {"setex", "rpush", "publish", "expire"}
                       for row in provider.operations[operation_start:])
        assert current.proxy.identity_snapshot()["writes_blocked"] is True
        assert current.native_calls == []
    finally:
        current.close()

    # 已核对的历史事实在后续新源启动时仍可校验，原UNKNOWN和原SID不被改写。
    restarted = NativePath(monkeypatch, redis_provider=provider, native_query=query, start_write_blocked=True)
    try:
        assert restarted.proxy.identity_snapshot()["writes_blocked"] is True
        readback = restarted.execute(restarted.proxy.process_request, request("get_asset", rid="reconciled-restart-read"))
        assert readback["ok"] is True
        duplicate = restarted.send("new-rid-same-old-cid", CID)
        assert duplicate["ok"] is False and restarted.native_calls == []
        assert provider.hget(LEDGER + ":cid", CID) == RID
        assert json.loads(provider.hget(LEDGER, RID))["response"] == record["response"]
    finally:
        restarted.close()
