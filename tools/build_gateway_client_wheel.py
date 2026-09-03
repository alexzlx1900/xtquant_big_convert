#!/usr/bin/env python3
"""Build a Gateway client wheel without the top-level ``xtquant`` shim."""

import argparse
import hashlib
import io
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
import zipfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TEMPLATE_DIR = ROOT / "packaging" / "gateway_client"
PACKAGE_PREFIX = "src/bigqmt_signal_trader"
REQUIRED_WHEEL_MEMBER = "bigqmt_signal_trader/xtquant_compat.py"


def _run(*args, **kwargs):
    return subprocess.run(args, check=True, **kwargs)


def _resolve_commit(source_ref):
    result = _run(
        "git",
        "rev-parse",
        "%s^{commit}" % source_ref,
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def _extract_package(source_ref, project_dir):
    archive = _run(
        "git",
        "archive",
        "--format=tar",
        source_ref,
        PACKAGE_PREFIX,
        cwd=ROOT,
        capture_output=True,
    ).stdout
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:") as handle:
        handle.extractall(project_dir)


def _read_version(project_dir):
    version_path = project_dir / PACKAGE_PREFIX / "version.py"
    match = re.search(
        r'^__version__\s*=\s*"([^"]+)"',
        version_path.read_text(encoding="utf-8"),
        re.MULTILINE,
    )
    if not match:
        raise RuntimeError("cannot read __version__ from archived package")
    return match.group(1)


def _prepare_project(source_ref, project_dir):
    _extract_package(source_ref, project_dir)
    version = _read_version(project_dir)
    template = (TEMPLATE_DIR / "pyproject.toml.in").read_text(encoding="utf-8")
    (project_dir / "pyproject.toml").write_text(
        template.replace("@VERSION@", version),
        encoding="utf-8",
    )
    shutil.copy2(TEMPLATE_DIR / "README.md", project_dir / "README.md")
    return version


def _verify_wheel(wheel_path, expected_version):
    with zipfile.ZipFile(wheel_path) as archive:
        names = set(archive.namelist())
        if REQUIRED_WHEEL_MEMBER not in names:
            raise RuntimeError("client wheel is missing %s" % REQUIRED_WHEEL_MEMBER)
        forbidden = sorted(
            name
            for name in names
            if name.startswith("xtquant/") or name.startswith("bigqmt_backtest/")
        )
        if forbidden:
            raise RuntimeError("client wheel contains forbidden packages: %s" % forbidden)
        metadata_names = sorted(name for name in names if name.endswith(".dist-info/METADATA"))
        if len(metadata_names) != 1:
            raise RuntimeError("client wheel must contain exactly one METADATA file")
        metadata = archive.read(metadata_names[0]).decode("utf-8", "replace")
    if "Name: xtquant-big-convert-client\n" not in metadata:
        raise RuntimeError("client wheel distribution name is incorrect")
    if "Version: %s\n" % expected_version not in metadata:
        raise RuntimeError("client wheel version is incorrect")


def build(source_ref, output_dir):
    commit = _resolve_commit(source_ref)
    output_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="bigqmt-gateway-client-") as temp:
        temp_root = Path(temp)
        project_dir = temp_root / "project"
        wheel_dir = temp_root / "wheel"
        project_dir.mkdir()
        wheel_dir.mkdir()
        version = _prepare_project(commit, project_dir)
        _run(
            sys.executable,
            "-m",
            "pip",
            "wheel",
            "--no-deps",
            "--no-build-isolation",
            "--wheel-dir",
            str(wheel_dir),
            str(project_dir),
        )
        wheels = sorted(wheel_dir.glob("*.whl"))
        if len(wheels) != 1:
            raise RuntimeError("expected one client wheel, found %d" % len(wheels))
        _verify_wheel(wheels[0], version)
        destination = output_dir / wheels[0].name
        shutil.copy2(wheels[0], destination)
    digest = hashlib.sha256(destination.read_bytes()).hexdigest()
    print("SOURCE_COMMIT=%s" % commit)
    print("WHEEL=%s" % destination)
    print("SHA256=%s" % digest)
    return destination


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-ref", default="HEAD")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=ROOT / "dist" / "gateway-client",
    )
    args = parser.parse_args(argv)
    build(args.source_ref, args.output_dir.resolve())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
