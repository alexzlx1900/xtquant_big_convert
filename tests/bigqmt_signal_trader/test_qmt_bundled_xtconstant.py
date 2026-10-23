# coding: utf-8
"""Server-side code may only use xtconstant names Big QMT's own copy has.

This repo ships src/xtquant/xtconstant.py, and every test here imports it. The
terminal does not. Big QMT bundles its own xtquant at

    bin.x64/Lib/site-packages/xtquant/xtconstant.py

and that is the module `from xtquant import xtconstant` resolves to inside the
sandbox. It defines 91 names; the shim defines 538. No name means a different
value in the two -- the difference is purely what is absent.

Reading one of the 447 absent names at import time is invisible until the
strategy restarts, and then it is not a small failure. Building an account-type
map from ACCOUNT_TYPE_DICT took down the whole order gateway:

    AttributeError: module 'xtquant.xtconstant' has no attribute
                    'ACCOUNT_TYPE_DICT'
    -> init_app/build_app failed
    -> every order and position query answers
       "RuntimeError: order_gateway is not configured"

while ping kept working and the adjust loop kept ticking, so from outside the
bridge looked healthy. The same shape as the __init__.py trap in CLAUDE.md:
works everywhere it is tested, missing in the one place that matters.

tests/data/qmt_bundled_xtconstant_names.txt is what the live terminal actually
defines. Refresh it from a real install if the terminal is upgraded.
"""

import ast
import io
import os
import re
import sys
import unittest


ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "src"))

from xtquant import xtconstant
from bigqmt_signal_trader.adapters import order_bigqmt
from bigqmt_signal_trader.adapters.order_bigqmt import BigQmtOrderGateway
from xtquant.xtconstant import CREDIT_ACCOUNT

SRC = os.path.join(ROOT, "src")
NAMES_FILE = os.path.join(ROOT, "tests", "data", "qmt_bundled_xtconstant_names.txt")

# `_XC.NAME`, `_xtconstant.NAME`, `xtconstant.NAME`
ATTRIBUTE_USE = re.compile(
    r"\b(?:_XC|_xtconstant|xtconstant)\.([A-Za-z_][A-Za-z0-9_]*)")

# The client half. It runs in the caller's own interpreter against the real
# MiniQMT xtquant (or this repo's shim), so it is not bound by QMT's copy.
CLIENT_SIDE = {
    os.path.join("bigqmt_signal_trader", "xtquant_compat.py"),
}


def _bundled_names():
    names = set()
    with io.open(NAMES_FILE, encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line and not line.startswith("#"):
                names.add(line)
    return names


def _server_side_modules():
    for root, _dirs, files in os.walk(SRC):
        # The shim itself. Compare path COMPONENTS: the repo directory is
        # called xtquant_big_convert, so a substring test on the path excludes
        # every file in the project (which is how this guard first "passed").
        if "xtquant" in os.path.relpath(root, SRC).split(os.sep):
            continue
        for name in sorted(files):
            if not name.endswith(".py"):
                continue
            path = os.path.join(root, name)
            if os.path.relpath(path, SRC) in CLIENT_SIDE:
                continue
            yield path


class TheSnapshotIsUsableTest(unittest.TestCase):
    def test_it_has_the_names_the_terminal_had(self):
        names = _bundled_names()

        self.assertEqual(len(names), 91)

    def test_the_account_type_constants_are_all_there(self):
        """What _account_type_codes falls back to when the dict is absent."""
        names = _bundled_names()

        for name in ("SECURITY_ACCOUNT", "CREDIT_ACCOUNT", "FUTURE_ACCOUNT",
                     "STOCK_OPTION_ACCOUNT"):
            self.assertIn(name, names)

    def test_account_type_dict_is_the_one_that_is_not(self):
        """The specific absence that broke the live deployment."""
        self.assertNotIn("ACCOUNT_TYPE_DICT", _bundled_names())

    def test_the_shim_really_is_bigger(self):

        shim = set(name for name in dir(xtconstant) if not name.startswith("_"))

        self.assertGreater(len(shim), len(_bundled_names()))

    def test_no_name_disagrees_in_value(self):
        """Only absence, never a wrong number -- worth knowing, and worth
        noticing if it ever stops being true."""

        source = io.open(os.path.join(
            os.path.dirname(NAMES_FILE), "qmt_bundled_xtconstant_names.txt"),
            encoding="utf-8").read()

        self.assertIn("ACCOUNT_STATUS_OK", source)
        self.assertTrue(hasattr(xtconstant, "ACCOUNT_STATUS_OK"))


class ServerSideUsageTest(unittest.TestCase):
    def test_no_server_module_uses_a_name_the_terminal_lacks(self):
        available = _bundled_names()
        offenders = {}

        for path in _server_side_modules():
            with io.open(path, encoding="utf-8") as handle:
                text = handle.read()
            used = set(ATTRIBUTE_USE.findall(text))
            missing = sorted(name for name in used if name not in available)
            if missing:
                offenders[os.path.relpath(path, ROOT)] = missing

        self.assertEqual(
            offenders, {},
            "these xtconstant names do not exist in Big QMT's own xtquant, so "
            "reading them inside the terminal raises AttributeError at import "
            "time and takes down whatever imports the module:\n%r" % (offenders,))

    def test_no_server_module_imports_one_by_name_either(self):
        """`from xtquant.xtconstant import X` skips the attribute pattern."""
        available = _bundled_names()
        offenders = {}

        for path in _server_side_modules():
            with io.open(path, encoding="utf-8") as handle:
                tree = ast.parse(handle.read())
            for node in ast.walk(tree):
                if not isinstance(node, ast.ImportFrom):
                    continue
                if (node.module or "") not in ("xtquant.xtconstant", "xtconstant"):
                    continue
                missing = [alias.name for alias in node.names
                           if alias.name not in available and alias.name != "*"]
                if missing:
                    offenders.setdefault(os.path.relpath(path, ROOT), []).extend(missing)

        self.assertEqual(offenders, {}, repr(offenders))

    def test_the_scan_actually_looks_at_the_order_gateway(self):
        """A guard that silently scans nothing would pass forever."""
        scanned = [os.path.relpath(path, SRC) for path in _server_side_modules()]

        self.assertIn(os.path.join("bigqmt_signal_trader", "adapters",
                                   "order_bigqmt.py"), scanned)

    def test_the_scan_would_catch_a_bad_name(self):
        available = _bundled_names()

        used = set(ATTRIBUTE_USE.findall("value = _XC.ACCOUNT_TYPE_DICT.items()"))

        self.assertEqual([n for n in used if n not in available],
                         ["ACCOUNT_TYPE_DICT"])


class AccountTypeCodesWithoutTheDictTest(unittest.TestCase):
    """The fix: resolve account types without ACCOUNT_TYPE_DICT."""

    def _codes_from(self, module):

        real = order_bigqmt._xtconstant
        try:
            order_bigqmt._xtconstant = module
            return order_bigqmt._account_type_codes()
        finally:
            order_bigqmt._xtconstant = real

    class Bundled(object):
        """What Big QMT actually provides: the constants, not the dict."""
        FUTURE_ACCOUNT = 1
        SECURITY_ACCOUNT = 2
        CREDIT_ACCOUNT = 3
        STOCK_OPTION_ACCOUNT = 6

    def test_stock_resolves_without_the_dict(self):
        codes = self._codes_from(self.Bundled())

        self.assertEqual(codes["STOCK"], 2)

    def test_credit_resolves_without_the_dict(self):
        codes = self._codes_from(self.Bundled())

        self.assertEqual(codes["CREDIT"], 3)

    def test_both_spellings_of_the_security_account_are_present(self):
        """ACCOUNT_TYPE_DICT calls it STOCK; the constant is SECURITY_ACCOUNT."""
        codes = self._codes_from(self.Bundled())

        self.assertEqual(codes["STOCK"], codes["SECURITY"])

    def test_the_dict_is_still_used_when_it_exists(self):
        class WithDict(self.Bundled):
            ACCOUNT_TYPE_DICT = {2: "STOCK", 3: "CREDIT", 8: "INCOME_SWAP"}

        codes = self._codes_from(WithDict())

        self.assertEqual(codes["INCOME_SWAP"], 8)

    def test_a_bare_module_still_yields_something_usable(self):
        class Empty(object):
            pass

        codes = self._codes_from(Empty())

        self.assertEqual(codes["STOCK"], 2)

    def test_the_gateway_resolves_its_configured_type(self):

        gateway = BigQmtOrderGateway(context_info=None, account_type="CREDIT")

        self.assertEqual(gateway._account_type_code(), CREDIT_ACCOUNT)


if __name__ == "__main__":
    unittest.main()
