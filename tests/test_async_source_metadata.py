# coding: utf-8
"""实际生成源码发布与双源ping能力一致；仅内存原生API/Win32替身。"""

import ctypes
import json
import os
from unittest import mock

import pytest

from test_dual_shm_single_file import BUILDER, Fixture, sim_resident_settings


@pytest.mark.parametrize("mode", [
    "live_write_resident", "sim_write", "bounded_readonly",
    "persistent_readonly", "sim_readonly_resident",
])
def test_published_capabilities_match_actual_ping_without_changing_authority(monkeypatch, capsys, mode):
    settings = {}
    executable = None
    writable = mode in {"live_write_resident", "sim_write"}
    if mode == "live_write_resident":
        executable = r"C:\QMT\XtItClient.exe"
        settings = dict(
            account_id="8882152350", environment="LIVE", persistent_live_read_write=True,
            native_pid=0, native_executable=executable,
            source_identity_path=r"C:\AQP\win.source.identity.json",
            hard_deadline_unix=None, max_seconds=0, max_requests=0,
        )
    elif mode == "sim_write":
        settings = dict(persistent_simulation_read_write=True,
                        hard_deadline_unix=None, max_seconds=0, max_requests=0)
    elif mode == "persistent_readonly":
        settings = dict(persistent_readonly=True,
                        hard_deadline_unix=None, max_seconds=0, max_requests=0)
    elif mode == "sim_readonly_resident":
        executable = BUILDER.SIM_NATIVE_EXECUTABLE
        settings = sim_resident_settings()

    source = Fixture(monkeypatch, **settings)
    opened = None
    replace = None
    if executable is not None:
        def module_path(module, buffer, size):
            buffer.value = executable
            return len(executable)

        monkeypatch.setattr(ctypes.windll.kernel32, "GetModuleFileNameW", module_path, raising=False)
        opened, replace = mock.mock_open(), mock.Mock()
        source.namespace["open"] = opened
        monkeypatch.setattr(os, "replace", replace)

    try:
        source.start()
        ready = json.loads(next(line.split(" ", 1)[1] for line in capsys.readouterr().out.splitlines()
                                if line.startswith("AQP_DUAL_SHM_READY ")))
        if opened is not None:
            published = json.loads("".join(call.args[0] for call in opened().write.call_args_list))
            assert published == dict(ready, status="READY")
            path = source.settings["source_identity_path"]
            replace.assert_called_once_with(path + ".new", path)

        read_ping = source.call("read", "ping")["data"]
        execution_ping = source.call("execution", "ping")["data"]
        assert set(ready["read_capabilities"]) == set(read_ping["capabilities"])
        assert ready["execution_capabilities"] == sorted(execution_ping["capabilities"])
        assert len(ready["execution_capabilities"]) == len(set(ready["execution_capabilities"]))
        assert ready["allow_order_methods"] is writable
        for route, ping in (("read", read_ping), ("execution", execution_ping)):
            assert ready["account_id"] == ping["account_id"] == source.account
            assert ready["environment"] == ping["environment"] == source.settings.get("environment", "SIM")
            assert ready[route + "_session_id"] == ping["session_id"]
            assert ready["native_pid"] == ping["native_pid"] == os.getpid()
            assert ready["windows_session"] == ping["windows_session_id"] == 1
            assert ping["allow_order_methods"] is (writable and route == "execution")
        assert ready["read_session_id"] != ready["execution_session_id"]
        assert "submit_order_async" not in ready["read_capabilities"]
        if writable:
            assert "submit_order_async" in ready["execution_capabilities"]
            assert ready["async_submit_contract"] == execution_ping["async_submit_contract"] == "NATIVE_CALL_RETURNED_V1"
        else:
            assert not {"submit_order", "cancel_order", "submit_order_async"} & set(ready["execution_capabilities"])
            assert "async_submit_contract" not in ready
        # 来源初始化/能力发布不代表账户已登录、可写或已获柜台受理。
        assert not {"write_ready", "broker_accepted", "qmt_async_seq"} & ready.keys()
        assert source.context.accounts == [source.account]
        source.writes.assert_not_called()
    finally:
        source.close()
