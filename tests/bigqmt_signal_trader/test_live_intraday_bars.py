import datetime as dt
import os
import sys
import unittest
from unittest.mock import Mock

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "src"))

from bigqmt_signal_trader.adapters.market_bigqmt import BigQmtMarketDataProvider


class MinuteContext:
    def __init__(self):
        self.subscriptions = {7: {"func": "subscribe_quote", "stock_code": "600000.SH", "period": "1d"}}
        self.cancelled = []
        self.cleanup_fails = False

    def get_all_subscription(self):
        return dict(self.subscriptions)

    def subscribe_quote(self, code, **kwargs):
        self.subscriptions[8] = {"func": "subscribe_quote", "stock_code": code, "period": kwargs["period"]}
        return 8

    def unsubscribe_quote(self, handle):
        self.cancelled.append(handle)
        if not self.cleanup_fails:
            self.subscriptions.pop(handle, None)


class LiveMinuteTests(unittest.TestCase):
    def setUp(self):
        self.context = MinuteContext()
        self.provider = BigQmtMarketDataProvider(self.context)
        self.provider.get_market_data_ex = Mock(return_value={"688670.SH": []})
        self.today = dt.datetime.now().strftime("%Y%m%d")

    def read(self, **kwargs):
        return self.provider.get_intraday_bars("688670.SH", self.today, **kwargs)

    def test_dynamic_read_releases_own_subscription_and_preserves_existing(self):
        result = self.read()
        self.assertEqual("CANCELLED", result["cleanup_status"])
        self.assertEqual(1, result["subscriptions_before"])
        self.assertEqual(1, result["subscriptions_after"])
        self.assertEqual([8], self.context.cancelled)
        call = self.provider.get_market_data_ex.call_args.kwargs
        self.assertIs(False, call["subscribe"])
        self.assertIs(False, call["fill_data"])
        self.assertEqual(["688670.SH"], call["stock_list"])
        self.assertEqual(self.today + "093000", call["start_time"])

    def test_native_read_exception_still_cancels(self):
        self.provider.get_market_data_ex.side_effect = ValueError("native read failed")
        with self.assertRaisesRegex(ValueError, "native read failed"):
            self.read()
        self.assertEqual([8], self.context.cancelled)
        self.assertEqual({7}, set(self.context.subscriptions))

    def test_failed_cancellation_never_returns_normal_data(self):
        self.context.cleanup_fails = True
        with self.assertRaisesRegex(RuntimeError, "CLEANUP_UNCONFIRMED"):
            self.read()

    def test_native_cancel_failure_is_not_hidden_by_local_table_removal(self):
        def failed_cancel(handle):
            self.context.cancelled.append(handle)
            self.context.subscriptions.pop(handle, None)
            return False
        self.context.unsubscribe_quote = failed_cancel
        with self.assertRaisesRegex(RuntimeError, "CLEANUP_UNCONFIRMED"):
            self.read()
        self.assertEqual({7}, set(self.context.subscriptions))

    def test_zero_native_handle_never_reads_or_reports_cleanup(self):
        self.context.subscribe_quote = Mock(return_value=0)
        with self.assertRaisesRegex(RuntimeError, "SUBSCRIPTION_UNCONFIRMED"):
            self.read()
        self.provider.get_market_data_ex.assert_not_called()
        self.assertEqual([], self.context.cancelled)

    def test_additional_target_subscription_is_not_assumed_to_be_ours(self):
        def dynamic(**kwargs):
            self.context.subscriptions[9] = {"func": "subscribe_quote", "stock_code": "688670.SH", "period": "1m"}
            return {"688670.SH": []}
        self.provider.get_market_data_ex.side_effect = dynamic
        with self.assertRaisesRegex(RuntimeError, "CLEANUP_UNCONFIRMED"):
            self.read()
        self.assertEqual([8], self.context.cancelled)
        self.assertEqual({7, 9}, set(self.context.subscriptions))

    def test_unrelated_subscription_is_never_cancelled(self):
        def dynamic(**kwargs):
            self.context.subscriptions[9] = {"func": "subscribe_quote", "stock_code": "000001.SZ", "period": "1m"}
            return {"688670.SH": []}
        self.provider.get_market_data_ex.side_effect = dynamic
        with self.assertRaisesRegex(RuntimeError, "CLEANUP_UNCONFIRMED"):
            self.read()
        self.assertEqual([8], self.context.cancelled)

    def test_invalid_window_does_not_subscribe(self):
        with self.assertRaises(ValueError):
            self.provider.get_intraday_bars("688670.SH", "20200101")
        self.assertEqual({7}, set(self.context.subscriptions))
        for count in (0, 2001, True):
            with self.assertRaises(ValueError):
                self.read(count=count)
        self.assertEqual([], self.context.cancelled)


if __name__ == "__main__":
    unittest.main()
