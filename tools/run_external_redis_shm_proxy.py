# coding: utf-8
"""外置 Redis→双 SHM 候选入口；默认只校验配置，不建立连接。"""

import argparse
import ctypes
import inspect
import json
import math
import os
from pathlib import Path
import re
import sys
import time

try:
    import redis
except ImportError:
    redis = None

try:
    from redis.backoff import NoBackoff
    from redis.retry import Retry
except ImportError:
    NoBackoff = None
    Retry = None


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
os.environ.setdefault("BIGQMT_LOG_ENABLED", "0")

from bigqmt_signal_trader.redis_shm_proxy import RUNTIME_POLICIES, RedisShmReadOnlyProxy
from bigqmt_signal_trader.transports.readonly_shm import ShmReadOnlyClient, validate_name
from bigqmt_signal_trader.transports.redis_transport import RedisTransport


ACCOUNT = "55013746"


def _integer(value, name, minimum, maximum):
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValueError("%s must be an integer in [%s, %s]" % (name, minimum, maximum))
    return value


def _finite(value, name, minimum, maximum=None):
    if type(value) not in (int, float) or not math.isfinite(value) or value <= minimum or (maximum is not None and value > maximum):
        raise ValueError("%s must be finite and within its bounded range" % name)
    return float(value)


def validate_config(value, now_unix=None):
    """检查当次来源及本地 Redis 目标，不读取客户端或运行状态。"""
    if not isinstance(value, dict) or value.get("account_id") != ACCOUNT:
        raise ValueError("simulation account 55013746 is required")
    policy = value.get("runtime_policy", "BOUNDED")
    if policy not in RUNTIME_POLICIES:
        raise ValueError("explicit supported runtime_policy required")
    if type(value.get("allow_order_methods")) is not bool or value["allow_order_methods"] is not (policy == "PERSISTENT_SIMULATION_READ_WRITE"):
        raise ValueError("allow_order_methods must explicitly match simulation runtime policy")
    result = dict(value)
    result["runtime_policy"] = policy
    result["native_pid"] = _integer(value.get("native_pid"), "native_pid", 1, 0xFFFFFFFF)
    result["windows_session"] = _integer(value.get("windows_session"), "windows_session", 0, 0xFFFFFFFF)
    for field, prefix in (("read_name", "Local\\AQP_SHM_READONLY_"), ("execution_name", "Local\\AQP_SHM_TRADE_")):
        name = value.get(field)
        validate_name(name)
        if not name.startswith(prefix):
            raise ValueError("%s must use its independent mailbox prefix" % field)
    for field in ("read_session_id", "execution_session_id"):
        if not isinstance(value.get(field), str) or not re.fullmatch(r"[0-9a-f]{32}", value[field]):
            raise ValueError("%s must be an explicit native session ID" % field)
    if value["read_session_id"] == value["execution_session_id"]:
        raise ValueError("the two native sessions must be distinct")
    now = time.time() if now_unix is None else now_unix
    if policy == "BOUNDED":
        result["source_deadline_unix"] = _finite(value.get("source_deadline_unix"), "source_deadline_unix", now)
        result["max_seconds"] = _integer(value.get("max_seconds", 60), "max_seconds", 1, 900)
        result["max_requests"] = _integer(value.get("max_requests", 100), "max_requests", 1, 1000)
    else:
        if value.get("source_deadline_unix", "missing") is not None:
            raise ValueError("persistent source deadline must explicitly be null")
        result["source_deadline_unix"] = None
        result["max_seconds"] = _integer(value.get("max_seconds"), "max_seconds", 0, 0)
        result["max_requests"] = _integer(value.get("max_requests"), "max_requests", 0, 0)
    result["read_cache_capacity"] = _integer(value.get("read_cache_capacity", 1000), "read_cache_capacity", 1, 1000)
    result["write_ledger_capacity"] = _integer(value.get("write_ledger_capacity", 1000), "write_ledger_capacity", 1, 1000)
    result["timeout_seconds"] = _finite(value.get("timeout_seconds", 2.0), "timeout_seconds", 0, 60)
    target = value.get("redis")
    if not isinstance(target, dict) or set(target) - {"host", "port", "db", "username", "password"}:
        raise ValueError("an explicit bounded Redis target is required")
    if target.get("host") not in ("127.0.0.1", "localhost", "::1"):
        raise ValueError("the candidate only connects to the local Redis provider")
    target = dict(target)
    target["port"] = _integer(target.get("port"), "redis.port", 1, 65535)
    target["db"] = _integer(target.get("db"), "redis.db", 0, 15)
    for field in ("username", "password"):
        if target.get(field) is not None and not isinstance(target[field], str):
            raise ValueError("redis.%s must be a string or null" % field)
    result["redis"] = target
    return result


def windows_session_id(pid):
    if os.name != "nt":
        raise RuntimeError("explicit run requires Windows; dry-run works locally")
    session = ctypes.c_uint()
    if not ctypes.windll.kernel32.ProcessIdToSessionId(pid, ctypes.byref(session)):
        raise RuntimeError("Windows process session could not be confirmed")
    return session.value


def create_redis_client(target):
    if redis is None or Retry is None or NoBackoff is None:
        raise RuntimeError("the existing environment needs the project Redis optional dependency")
    # Direct constructor keeps this exact local target independent of URL/env fallbacks.
    kwargs = dict(target, socket_connect_timeout=1.0, socket_timeout=1.0,
                  retry=Retry(NoBackoff(), 0), retry_on_timeout=False)
    if "protocol" in inspect.signature(redis.Redis.__init__).parameters:
        kwargs["protocol"] = 2
    return redis.Redis(**kwargs)


def _emit_ready(evidence):
    print(json.dumps(evidence, ensure_ascii=False, sort_keys=True), flush=True)


def run_candidate(value, redis_factory=None, shm_factory=None, *, stop_event=None, ready_observer=None):
    """显式运行政策；停止事件或中断释放自有连接，不重连、不重发、不删键。"""
    config = validate_config(value)
    if windows_session_id(os.getpid()) != config["windows_session"] or windows_session_id(config["native_pid"]) != config["windows_session"]:
        raise RuntimeError("consumer and native source must share the explicit Windows session")
    bounded = config["runtime_policy"] == "BOUNDED"
    parent_deadline = None
    if bounded:
        remaining = min(config["max_seconds"], config["source_deadline_unix"] - time.time())
        if remaining <= 2 * config["timeout_seconds"] + 2:
            raise RuntimeError("insufficient source/runner margin for two handshakes and exit; no connections created")
        parent_deadline = min(config["source_deadline_unix"], time.time() + config["max_seconds"])
    redis_client, proxy, clients = None, None, []
    errors = []
    result = dict(status="FAILED", account_id=ACCOUNT, automatic_retry=False,
                  allow_order_methods=config["allow_order_methods"], runtime_policy=config["runtime_policy"],
                  processed_requests=0, received_requests=0, full_duration_acceptance_proved=False)
    began = time.monotonic()
    try:
        redis_client = (redis_factory or create_redis_client)(config["redis"])
        transport = RedisTransport(redis_client, account_id=ACCOUNT)
        factory = shm_factory or ShmReadOnlyClient
        clients.append(factory(config["read_name"], ACCOUNT))
        clients.append(factory(config["execution_name"], ACCOUNT))
        proxy = RedisShmReadOnlyProxy(
            redis_transport=transport, readonly_client=clients[0], execution_client=clients[1],
            account_id=ACCOUNT, expected_readonly_session_id=config["read_session_id"],
            expected_execution_session_id=config["execution_session_id"],
            expected_native_pid=config["native_pid"], timeout_seconds=config["timeout_seconds"],
            max_requests=config["max_requests"], deadline_unix=parent_deadline,
            runtime_policy=config["runtime_policy"], allow_order_methods=config["allow_order_methods"],
            read_cache_capacity=config["read_cache_capacity"], write_ledger_capacity=config["write_ledger_capacity"],
            expected_windows_session_id=config["windows_session"],
        )
        proxy.verify_identity()
        evidence = dict(proxy.identity_snapshot(), status="READY", automatic_retry=False)
        (ready_observer or _emit_ready)(evidence)
        stop_requested = False
        while not proxy.stopped:
            if stop_event is not None and stop_event.is_set():
                stop_requested = True
                break
            if bounded:
                remaining = min(config["max_seconds"] - (time.monotonic() - began),
                                config["source_deadline_unix"] - time.time())
                if (remaining <= config["timeout_seconds"] + 1
                        or proxy.processed_count >= config["max_requests"]
                        or result["received_requests"] >= config["max_requests"]):
                    break
            response = proxy.poll_once()
            if response is None:
                time.sleep(min(0.02, remaining) if bounded else 0.02)
            else:
                result["received_requests"] += 1
        result.update(status="PROXY_STOPPED" if proxy.stopped else ("STOP_REQUESTED" if stop_requested else "BOUNDED_STOP"),
                      identity_verified=proxy.verified, processed_requests=proxy.processed_count,
                      stop_reason=proxy.stop_reason)
    except KeyboardInterrupt:
        result.update(status="STOP_REQUESTED", identity_verified=bool(proxy and proxy.verified),
                      processed_requests=proxy.processed_count if proxy else 0,
                      stop_reason="KEYBOARD_INTERRUPT")
    except Exception as exc:
        result.update(status="FAILED", error_type=type(exc).__name__)
        if proxy is not None:
            result["processed_requests"] = proxy.processed_count
    finally:
        if proxy is not None:
            try:
                proxy.close()
            except Exception as exc:
                errors.append(type(exc).__name__)
        for client in clients:
            try:
                client.close()
            except Exception as exc:
                errors.append(type(exc).__name__)
        if redis_client is not None:
            try:
                redis_client.connection_pool.disconnect()
            except Exception as exc:
                errors.append(type(exc).__name__)
        result.update(cleanup_errors=errors, resources_closed=not errors,
                      elapsed_seconds=round(time.monotonic() - began, 3))
        if errors:
            result["status"] = "UNKNOWN"
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True, help="当次双SHM来源与Redis目标JSON，不执行配置代码")
    parser.add_argument("--run", action="store_true", help="仅在另获实机授权后显式运行；默认dry-run")
    args = parser.parse_args(argv)
    try:
        config = validate_config(json.loads(args.config.read_text(encoding="utf-8-sig")))
        if args.run:
            result = run_candidate(config)
        else:
            result = dict(status="DRY_RUN", account_id=ACCOUNT, allow_order_methods=config["allow_order_methods"],
                          runtime_policy=config["runtime_policy"],
                          native_pid=config["native_pid"], windows_session=config["windows_session"],
                          max_seconds=config["max_seconds"], max_requests=config["max_requests"],
                          connections_created=0, full_duration_acceptance_proved=False)
    except Exception as exc:
        result = dict(status="FAILED", error_type=type(exc).__name__, automatic_retry=False)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0 if result["status"] in ("DRY_RUN", "BOUNDED_STOP", "STOP_REQUESTED") else 1


if __name__ == "__main__":
    raise SystemExit(main())
