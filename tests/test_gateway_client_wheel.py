"""The Gateway wheel must coexist with a broker-native ``xtquant`` install."""

import os
import subprocess
import sys
import tempfile
import unittest
import zipfile


ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BUILDER = os.path.join(ROOT, "tools", "build_gateway_client_wheel.py")


class GatewayClientWheelTest(unittest.TestCase):
    def test_build_uses_committed_package_and_excludes_xtquant_shim(self):
        with tempfile.TemporaryDirectory() as output_dir:
            result = subprocess.run(
                [
                    sys.executable,
                    BUILDER,
                    "--source-ref",
                    "HEAD",
                    "--output-dir",
                    output_dir,
                ],
                cwd=ROOT,
                capture_output=True,
                text=True,
            )
            self.assertEqual(result.returncode, 0, result.stderr or result.stdout)
            wheels = [
                os.path.join(output_dir, name)
                for name in os.listdir(output_dir)
                if name.endswith(".whl")
            ]
            self.assertEqual(len(wheels), 1, wheels)
            with zipfile.ZipFile(wheels[0]) as archive:
                names = archive.namelist()
                archived_adapter = archive.read(
                    "bigqmt_signal_trader/adapters/order_bigqmt.py"
                )

            self.assertIn("bigqmt_signal_trader/xtquant_compat.py", names)
            self.assertFalse(any(name.startswith("xtquant/") for name in names))
            self.assertFalse(any(name.startswith("bigqmt_backtest/") for name in names))

            committed_adapter = subprocess.run(
                [
                    "git",
                    "show",
                    "HEAD:src/bigqmt_signal_trader/adapters/order_bigqmt.py",
                ],
                cwd=ROOT,
                check=True,
                capture_output=True,
            ).stdout
            self.assertEqual(archived_adapter, committed_adapter)


if __name__ == "__main__":
    unittest.main()
