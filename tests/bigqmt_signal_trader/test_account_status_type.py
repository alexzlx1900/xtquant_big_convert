"""on_account_status must report the account type, not a constant (#103).

fengzhizialex ran a credit deployment and got:

    on_account_status: 8886... stock 1

after setting the type to CREDIT. The field was hardcoded "STOCK".

The server is the authority here, and that is the part worth being careful
about: the client's ``StockAccount(id, "CREDIT")`` never travels to the QMT
side. The deployment trades as whatever BIGQMT_ACCOUNT_TYPE says, so a client
declaring CREDIT against a STOCK deployment is not merely mislabelled -- its
queries answer as STOCK, and a credit account read that way returns an
all-zero asset row with no error. That was issue #92.

So the status reports what the server says, falls back to what the caller
declared, and the two disagreeing is now a warning instead of silence.
"""

import logging
import os
import sys
import unittest


ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "src"))

from bigqmt_signal_trader.redis_rpc import BigQmtRpcHandlers

from bigqmt_signal_trader import xtquant_compat as compat
from bigqmt_signal_trader.xtquant_compat import BigQmtXtTrader, StockAccount


ACCOUNT = "8886800503"


class Recorder(object):
    def __init__(self):
        self.statuses = []

    def on_account_status(self, status):
        self.statuses.append(status)


class FakeClient(object):
    def __init__(self, account_type="CREDIT"):
        self.account_id = ACCOUNT
        self.local_cache_config = {}
        self.full_tick_cache_config = {}
        self._account_type = account_type

    def call(self, method, params=None, account_id=None, timeout_seconds=None):
        if method == "ping":
            reply = {"pong": True, "account_id": ACCOUNT, "version": "9.9.9"}
            if self._account_type is not None:
                reply["account_type"] = self._account_type
            return reply
        return {}

    def _redis(self):
        raise AssertionError("redis not expected here")


def _trader(server_type="CREDIT"):
    trader = BigQmtXtTrader(account_id=ACCOUNT)
    trader.client = FakeClient(server_type)
    recorder = Recorder()
    trader.register_callback(recorder)
    return trader, recorder


class ReportsTheServerTypeTest(unittest.TestCase):
    def test_a_credit_deployment_reports_credit(self):
        trader, recorder = _trader("CREDIT")

        trader.connect()

        self.assertEqual(recorder.statuses[-1].account_type, "CREDIT")

    def test_a_stock_deployment_reports_stock(self):
        trader, recorder = _trader("STOCK")

        trader.connect()

        self.assertEqual(recorder.statuses[-1].account_type, "STOCK")

    def test_the_account_id_still_rides_along(self):
        trader, recorder = _trader("CREDIT")

        trader.connect()

        self.assertEqual(recorder.statuses[-1].account_id, ACCOUNT)
        self.assertEqual(recorder.statuses[-1].status, 1)


class FallbackTest(unittest.TestCase):
    """An older deployment does not report the field at all."""

    def test_it_falls_back_to_what_the_caller_declared(self):
        trader, recorder = _trader(server_type=None)

        trader.subscribe(StockAccount(ACCOUNT, "CREDIT"))

        self.assertEqual(recorder.statuses[-1].account_type, "CREDIT")

    def test_with_neither_it_stays_stock(self):
        trader, recorder = _trader(server_type=None)

        trader.subscribe(StockAccount(ACCOUNT))

        self.assertEqual(recorder.statuses[-1].account_type, "STOCK")

    def test_the_server_wins_over_the_declaration(self):
        """The client's type never reaches QMT, so the server is the truth."""
        trader, recorder = _trader("STOCK")
        trader.connect()

        trader.subscribe(StockAccount(ACCOUNT, "CREDIT"))

        self.assertEqual(recorder.statuses[-1].account_type, "STOCK")


class MismatchIsAnnouncedTest(unittest.TestCase):
    """Silence here is what made #92 expensive to diagnose."""

    def test_a_disagreement_warns(self):
        trader, _recorder = _trader("STOCK")
        trader.connect()

        with self.assertLogs(compat.log, level="WARNING") as logs:
            trader.subscribe(StockAccount(ACCOUNT, "CREDIT"))

        joined = "\n".join(logs.output)
        self.assertIn("account_type mismatch", joined)

    def test_the_warning_says_where_to_fix_it(self):
        trader, _recorder = _trader("STOCK")
        trader.connect()

        with self.assertLogs(compat.log, level="WARNING") as logs:
            trader.subscribe(StockAccount(ACCOUNT, "CREDIT"))

        joined = "\n".join(logs.output)
        self.assertIn("BIGQMT_ACCOUNT_TYPE", joined)
        self.assertIn("does NOT travel", joined)

    def test_the_warning_names_the_symptom(self):
        """"all-zero asset row" is what the reporter actually saw."""
        trader, _recorder = _trader("STOCK")
        trader.connect()

        with self.assertLogs(compat.log, level="WARNING") as logs:
            trader.subscribe(StockAccount(ACCOUNT, "CREDIT"))

        self.assertIn("all-zero", "\n".join(logs.output))

    def test_agreement_is_quiet(self):
        trader, _recorder = _trader("CREDIT")
        trader.connect()

        logger = logging.getLogger(compat.log.name)
        with self.assertLogs(logger, level="DEBUG") as logs:
            logger.debug("marker")           # so assertLogs has something
            trader.subscribe(StockAccount(ACCOUNT, "CREDIT"))

        self.assertEqual([l for l in logs.output if "mismatch" in l], [])

    def test_a_silent_deployment_does_not_warn(self):
        """No reported type means nothing to disagree with."""
        trader, _recorder = _trader(server_type=None)
        trader.connect()

        logger = logging.getLogger(compat.log.name)
        with self.assertLogs(logger, level="DEBUG") as logs:
            logger.debug("marker")
            trader.subscribe(StockAccount(ACCOUNT, "CREDIT"))

        self.assertEqual([l for l in logs.output if "mismatch" in l], [])


class ServerSideTest(unittest.TestCase):
    def test_ping_carries_the_configured_type(self):

        handlers = BigQmtRpcHandlers.__new__(BigQmtRpcHandlers)

        class Gateway(object):
            account_type = "credit"

        handlers.order_gateway = Gateway()

        self.assertEqual(handlers._reported_account_type(), "CREDIT")

    def test_no_gateway_reports_nothing_rather_than_guessing(self):

        handlers = BigQmtRpcHandlers.__new__(BigQmtRpcHandlers)
        handlers.order_gateway = None

        self.assertEqual(handlers._reported_account_type(), "")


if __name__ == "__main__":
    unittest.main()
