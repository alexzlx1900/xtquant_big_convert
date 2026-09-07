import os
import sys
import unittest


ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

from bigqmt_signal_trader.adapters.market_bigqmt import BigQmtMarketDataProvider


class RawMarketContext:
    def __init__(self, payload=None):
        self.payload = payload or {}
        self.calls = []

    def get_market_data_ex_ori(
        self,
        fields=None,
        stock_code=None,
        period="1d",
        start_time="",
        end_time="",
        count=-1,
        dividend_type="none",
        fill_data=True,
    ):
        self.calls.append(
            {
                "fields": fields,
                "stock_code": stock_code,
                "period": period,
                "start_time": start_time,
                "end_time": end_time,
                "count": count,
                "dividend_type": dividend_type,
                "fill_data": fill_data,
            }
        )
        return self.payload

    def get_market_data_ex(self, *args, **kwargs):
        raise AssertionError("DataFrame-producing QMT API must not be called")


class BigQmtRawMarketBridgeTest(unittest.TestCase):
    def test_raw_market_data_preserves_explicit_no_fill(self):
        context = RawMarketContext()
        provider = BigQmtMarketDataProvider(context)
        provider.get_market_data_ex(
            field_list=["close"], stock_list=["002144.SZ"],
            period="1m", start_time="20260904093000",
            end_time="20260904150100", fill_data=False,
        )
        self.assertIs(False, context.calls[0]["fill_data"])

    def test_market_data_shapes_never_drop_extended_fill_flag(self):
        provider = BigQmtMarketDataProvider(RawMarketContext())
        for name in ("get_market_data_ex", "get_market_data_ex_ori"):
            for _, args, kwargs in provider._market_data_shapes(name, fill_data=False):
                if len(args) == 8:
                    self.assertIs(False, args[7])
                else:
                    self.assertIs(False, kwargs["fill_data"])

    def test_market_data_ex_uses_raw_context_api(self):
        rows = [[1784014200000, 55.1], [1784014260000, 55.2]]
        context = RawMarketContext({"600276.SH": rows})
        provider = BigQmtMarketDataProvider(context)

        data = provider.get_market_data_ex(
            field_list=["close"], stock_list=["600276.SH"], period="1m", count=2
        )

        self.assertEqual("DataFrame", data["600276.SH"]["__bigqmt_type__"])
        self.assertEqual(["stime", "close"], data["600276.SH"]["columns"])
        self.assertEqual(rows, data["600276.SH"]["records"])
        self.assertEqual(["close"], context.calls[0]["fields"])
        self.assertEqual(["600276.SH"], context.calls[0]["stock_code"])

    def test_market_data_ex_returns_empty_frame_for_requested_symbol(self):
        context = RawMarketContext({})
        provider = BigQmtMarketDataProvider(context)

        data = provider.get_market_data_ex(
            field_list=["close"], stock_list=["600276.SH"], period="1m", count=2
        )

        self.assertEqual([], data["600276.SH"]["records"])
        self.assertEqual(["stime", "close"], data["600276.SH"]["columns"])

    def test_trading_date_context_fallback_maps_market_to_representative_stock(self):
        """ContextInfo 路径必须把市场代码转换为其要求的证券代码。"""
        class CalendarContext:
            def __init__(self):
                self.calls = []

            def get_trading_dates(self, stock_code, start_time, end_time, count):
                self.calls.append((stock_code, start_time, end_time, count))
                return ["20260826"]

        class UnavailableNativeXtData:
            def get_trading_dates(self, *_args):
                raise RuntimeError("native quote service unavailable")

        context = CalendarContext()
        provider = BigQmtMarketDataProvider(context, native_xtdata=UnavailableNativeXtData())

        for market, expected_stock in (("SH", "000001.SH"), ("sz", "399001.SZ"), ("000300.SH", "000300.SH")):
            with self.subTest(market=market):
                self.assertEqual(["20260826"], provider.get_trading_dates(market, "20260801", "20260826", 3))
                self.assertEqual((expected_stock, "20260801", "20260826", 3), context.calls[-1])


if __name__ == "__main__":
    unittest.main()
