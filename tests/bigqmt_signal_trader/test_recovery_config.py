"""真实 RPC 入口的持仓事件关闭开关与成交时间来源。"""

import ast
import datetime
import os
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "src"))

from bigqmt_signal_trader.adapter_factory import build_app
from bigqmt_signal_trader.models import AccountSnapshot, AssetSnapshot
from bigqmt_signal_trader.xtquant_compat import BigQmtXtTrader
from test_redis_adapters import FakeRedis


class RecoveryConfigTest(unittest.TestCase):
    def test_runtime_passes_false_to_sink_without_disabling_position_snapshot(self):
        path = os.path.join(ROOT, "src", "bigqmt_signal_trader_redis_rpc_runtime.py")
        with open(path, encoding="utf-8") as handle:
            tree = ast.parse(handle.read())
        apply_config = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "_apply_config")
        captured = {}
        names = {n.id for n in ast.walk(apply_config) if isinstance(n, ast.Name)}
        namespace = {name: False for name in names if name.isupper()}
        namespace.update(str=str, configure=lambda **kwargs: captured.update(kwargs), set_account_id=lambda value: None,
                         ACCOUNT_TYPE="STOCK", RPC_TRANSPORT="redis", BIGQMT_REDIS_CONFIG={"position_publish_events": False})
        module = ast.Module(body=[apply_config], type_ignores=[])
        exec(compile(module, path, "exec"), namespace)
        namespace["_apply_config"]("acct")
        self.assertIs(captured["run_app_tick"], False)
        r = FakeRedis()
        app = build_app(config={"mode": "dryrun", "account_id": "acct", "position_sync_type": "redis",
                         "redis": captured["redis"], "redis_client": r})
        app.position_sync_sink.publish(AccountSnapshot(
            account_id="acct", asset=AssetSnapshot(account_id="acct", cash=100, total_asset=100),
            positions={}, reason="tick", updated_at=datetime.datetime(2026, 9, 7, 10)))
        self.assertIn("bigqmt:positions:acct", r.kv)
        self.assertNotIn("bigqmt:position_events:acct", r.streams)

    def test_callback_time_is_not_labelled_as_broker_execution_time(self):
        trader = BigQmtXtTrader(account_id="acct")
        row = trader._trade_from_dict("acct", {"traded_time": 0, "created_at_ts": 1788563086.5, "traded_at": "2727"})
        self.assertEqual(row.trade_time_source, "callback_received_at")
        self.assertEqual(row.broker_trade_time, 0)
        row = trader._trade_from_dict("acct", {"traded_time": 2727})
        self.assertEqual(row.trade_time_source, "unknown")
        row = trader._trade_from_dict("acct", {"traded_time": 1788563086, "created_at_ts": 1788563090})
        self.assertEqual(row.trade_time_source, "broker")
        self.assertEqual(row.broker_trade_time, 1788563086)
