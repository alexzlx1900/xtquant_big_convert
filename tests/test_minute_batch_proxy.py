"""批量分钟的 Redis 代理路径：固定读源、有限预算和显式版本。"""

import importlib.util
import time
from pathlib import Path

import pytest


SPEC = importlib.util.spec_from_file_location("minute_batch_proxy_fixture",
    Path(__file__).parent / "bigqmt_signal_trader/test_redis_shm_proxy.py")
FIXTURE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(FIXTURE)
CONTRACT = "SCOPED_MINUTE_HISTORY_BATCH_V1"
METHODS = ["get_market_data_ex_batch", "download_history_data_batch"]


def parameters(method):
    result = {"symbols": ["000001.SZ", "301718.SZ"], "period": "1m"}
    if method == "get_market_data_ex_batch":
        return {**result, "start_time": "20261022000000", "end_time": "20261022235959",
            "count": 301, "dividend_type": "none", "fill_data": False, "subscribe": False}
    return {**result, "start_time": "20261022", "end_time": "20261022", "incrementally": None}


def make_proxy(advertise=True):
    proxy, redis, readonly, execution = FIXTURE.RedisShmReadOnlyProxyTest().make_proxy(verify=False, market=True)
    original = readonly.send_request
    def send(request, timeout_seconds):
        response = original(request, timeout_seconds)
        if advertise and request["method"] == "ping":
            response["data"].update(minute_history_batch_contract=CONTRACT, minute_history_batch_max_symbols=50)
        return response
    readonly.send_request = send
    readonly.result = {"status": "REQUESTED", "completion_confirmed": False}
    proxy.verify_identity()
    return proxy, redis, readonly, execution


@pytest.mark.parametrize("method,budget", [(METHODS[0], 15), (METHODS[1], 60)])
def test_batch_is_forwarded_once_to_readonly_with_its_scoped_deadline(method, budget):
    proxy, _redis, readonly, execution = make_proxy()
    request = FIXTURE.request(method=method, params=parameters(method))
    request["read_expires_at_unix"] = time.time() + 60
    result = proxy.process_request(request)
    assert result["ok"] is True and result["session_id"] == "readonly-sid"
    assert result["data"]["completion_confirmed"] is False
    assert len(readonly.calls) == 2 and len(execution.calls) == 1
    assert budget - 1 < readonly.calls[-1][1] <= budget
    assert readonly.calls[-1][0]["method"] == method
    assert proxy.identity_snapshot()["read_source"]["minute_history_batch_contract"] == CONTRACT


@pytest.mark.parametrize("method", METHODS)
def test_batch_capability_without_version_is_rejected_before_native_forward(method):
    proxy, _redis, readonly, execution = make_proxy(advertise=False)
    request = FIXTURE.request(method=method, params=parameters(method))
    result = proxy.process_request(request)
    assert result["ok"] is False and result["server_error"] == "PROXY_REQUEST_REJECTED"
    assert "contract" in result["error"]
    assert len(readonly.calls) == len(execution.calls) == 1


def test_download_batch_keeps_shorter_original_deadline_instead_of_resetting_budget():
    proxy, _redis, readonly, _execution = make_proxy()
    request = FIXTURE.request(method=METHODS[1], params=parameters(METHODS[1]))
    request["read_expires_at_unix"] = time.time() + 2
    result = proxy.process_request(request)
    assert result["ok"] is True
    assert 1 < readonly.calls[-1][1] <= 2
    assert readonly.calls[-1][0]["read_expires_at_unix"] == request["read_expires_at_unix"]
