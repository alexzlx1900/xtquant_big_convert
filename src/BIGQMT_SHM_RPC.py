# coding: utf-8
"""独立SHM RPC原生入口；默认只查询，不绑定原生下单/撤单函数。"""

import base64
import builtins
import ctypes
import datetime as _dt
import json
import math
import re
import threading
import time
import types


BIGQMT_ACCOUNT_ID = "YOUR_ACCOUNT_ID"
BIGQMT_ENVIRONMENT = "SIM"
SHM_RPC_NAME = "Local\\AQP_SHM_TRADE_SIM"
SHM_RPC_ALLOW_ORDER_METHODS = False
SHM_RPC_BIND_ACCOUNT_CALLBACKS = False  # 显式只读回报候选可绑定账户；不授予订单权限。
SHM_RPC_MAX_SECONDS = 60
SHM_RPC_MAX_REQUESTS = 180
SHM_RPC_PERSISTENT_READONLY = False  # 显式55013746驻留禁写；默认仍有界。
SHM_RPC_PERSISTENT_SIMULATION_READ_WRITE = False  # 显式模拟读写驻留；不启用策略tick。
SHM_RPC_PERSISTENT_LIVE_READ_WRITE = False  # 仅显式实盘账户常驻；不启用策略tick。
SHM_RPC_ACCOUNT_ALREADY_BOUND = False  # 仅显式双源外壳已完成唯一账户绑定时使用。
SHM_RPC_LIVE_ACCOUNT_QUERY_LOGIN = False  # 仅显式LIVE候选使用当前ACCOUNT登录状态。
SHM_RPC_ORDER_GATEWAY = None  # 禁写默认不绑定；另行授权后才可显式注入既有gateway。
SHM_RPC_ORDER_GATEWAY_FACTORY = None  # 显式候选可注入factory；禁写时不会构造。
_EMBEDDED_SOURCES = {}  # BUILD_SHM_RPC_EMBEDDED
_MODULE_CACHE = {}
_ORIGINAL_IMPORT = builtins.__import__
_PACKAGES = ("bigqmt_signal_trader", "bigqmt_signal_trader.adapters", "bigqmt_signal_trader.transports")
_STANDARD_IMPORTS = frozenset(("_strptime", "base64", "builtins", "collections", "ctypes", "datetime", "enum", "json", "math", "os", "re", "struct", "threading", "time", "types", "typing", "uuid"))
_SERVER = None


def _native_windows_session(native_pid):
    kernel = getattr(getattr(ctypes, "windll", None), "kernel32", None)
    if kernel is None:
        return None  # 非Windowsfixture保留缺失，不把配置值当原生证据。
    session = ctypes.c_uint()
    if not kernel.ProcessIdToSessionId(native_pid, ctypes.byref(session)):
        raise RuntimeError("native Windows session lookup failed")
    return session.value


def _bundle_import(name, module_globals=None, module_locals=None, fromlist=(), level=0):
    if level:
        parts = (module_globals or {}).get("__package__", "").split(".")
        name = ".".join(parts[:len(parts) - level + 1] + ([name] if name else []))
    if name in _EMBEDDED_SOURCES or name in _PACKAGES:
        module = _load_embedded(name)
        return module if fromlist else _load_embedded(name.split(".")[0])
    if name.split(".")[0] not in _STANDARD_IMPORTS:
        raise ImportError("module is outside SHM RPC embedded allowlist: %s" % name)
    return _ORIGINAL_IMPORT(name, module_globals, module_locals, fromlist, 0)


def _load_embedded(name):
    if name in _MODULE_CACHE:
        return _MODULE_CACHE[name]
    if name not in _PACKAGES and name not in _EMBEDDED_SOURCES:
        raise ImportError("embedded SHM RPC module missing: %s" % name)
    module = types.ModuleType(name)
    package = name in _PACKAGES
    module.__package__ = name if package else name.rpartition(".")[0]
    module.__file__ = "<embedded:%s>" % name
    if package:
        module.__path__ = []
    module_builtins = dict(vars(builtins))
    module_builtins["__import__"] = _bundle_import
    module.__dict__["__builtins__"] = module_builtins
    _MODULE_CACHE[name] = module
    try:
        if not package:
            source = base64.b64decode(_EMBEDDED_SOURCES[name]).decode("utf-8")
            exec(compile(source, module.__file__, "exec"), module.__dict__)
        if "." in name:
            parent, _, child = name.rpartition(".")
            setattr(_load_embedded(parent), child, module)
    except Exception:
        _MODULE_CACHE.pop(name, None)
        raise
    return module


class NativeReadOnlyProvider:
    """把已有严格只读转换交给既有handlers；不创建另一个交易gateway。"""
    def __init__(self, query, account_id):
        self.account_id = account_id
        self.positions = _load_embedded("bigqmt_signal_trader.adapters.position_bigqmt").BigQmtPositionProvider(query, account_type="STOCK")
        self.accounts = _load_embedded("bigqmt_signal_trader.adapters.account_readonly").ReadOnlyAccountProvider(query)

    def _target(self, account_id):
        if account_id != self.account_id:
            raise ValueError("native provider account mismatch")

    def get_asset(self, account_id):
        self._target(account_id)
        return self.accounts.get_asset(account_id)

    def get_positions(self, account_id):
        self._target(account_id)
        evidence = self.positions.get_positions_with_evidence(account_id)
        if not evidence["source_evidence"]["complete"]:
            raise RuntimeError("native position evidence incomplete: %s" % evidence["source_evidence"]["error_code"])
        return evidence["positions"]

    def get_positions_with_evidence(self, account_id):
        self._target(account_id)
        return self.positions.get_positions_with_evidence(account_id)

    def _records(self, account_id, detail_type, strategy_name):
        self._target(account_id)
        evidence = self.accounts.query_records(account_id, detail_type, strategy_name)
        if not evidence["source_evidence"]["complete"]:
            raise RuntimeError("native %s evidence incomplete: %s" % (detail_type, evidence["source_evidence"]["error_code"]))
        return evidence["orders" if detail_type == "ORDER" else "trades"]

    def query_orders(self, account_id, strategy_name=""):
        return self._records(account_id, "ORDER", strategy_name)

    def query_trades(self, account_id, strategy_name=""):
        return self._records(account_id, "DEAL", strategy_name)

    def get_orders_with_evidence(self, account_id, strategy_name=""):
        self._target(account_id)
        return self.accounts.query_records(account_id, "ORDER", strategy_name)

    def get_trades_with_evidence(self, account_id, strategy_name=""):
        self._target(account_id)
        return self.accounts.query_records(account_id, "DEAL", strategy_name)

    def get_execution_snapshot_with_evidence(self, account_id, order_strategy_name, trade_strategy_name):
        orders = self.get_orders_with_evidence(account_id, order_strategy_name)
        trades = self.get_trades_with_evidence(account_id, trade_strategy_name)
        order_evidence, trade_evidence = orders["source_evidence"], trades["source_evidence"]
        complete = order_evidence["complete"] and trade_evidence["complete"]
        raw_counts = (order_evidence["raw_row_count"], trade_evidence["raw_row_count"])
        returned_counts = (order_evidence["returned_row_count"], trade_evidence["returned_row_count"])
        return dict(account_id=account_id, server_time=_dt.datetime.now(),
                    rpc_revision=_load_embedded("bigqmt_signal_trader.redis_rpc").RPC_REVISION,
                    orders=orders["orders"], trades=trades["trades"],
                    orders_source_evidence=order_evidence, trades_source_evidence=trade_evidence,
                    source_evidence=dict(contract_version=1, account_id=account_id, complete=complete,
                        status="OK" if complete else "PARTIAL", source="native_ORDER_DEAL_queries",
                        queried_at=trade_evidence["queried_at"],
                        raw_row_count=sum(raw_counts) if all(type(value) is int for value in raw_counts) else None,
                        returned_row_count=sum(returned_counts) if all(type(value) is int for value in returned_counts) else None,
                        error_code="" if complete else "EXECUTION_SNAPSHOT_INCOMPLETE"))


class NativeStockOrderGateway(NativeReadOnlyProvider):
    """普通股票限价原生接口；复用既有订单模型和严格查询，不推断合同号。"""
    def __init__(self, context, account_id, query, passorder_func, cancel_func):
        if not callable(passorder_func) or not callable(cancel_func):
            raise RuntimeError("explicit native passorder/cancel functions required")
        super().__init__(query, account_id)
        self.context, self.passorder, self.cancel_func = context, passorder_func, cancel_func
        self.models = _load_embedded("bigqmt_signal_trader.models")
        self._submitted, self._cancel_attempted, self._cancel_snapshots = {}, set(), {}

    def validate_submit_params(self, params):
        # handler信用类型转换依赖xtquant；受限普通股票入口不可丢弃显式类型。
        if params.get("order_type") not in (None, ""):
            raise ValueError("native stock gateway requires BUY/SELL action without order_type")

    def submit(self, request):
        self._target(request.account_id)
        code = _load_embedded("bigqmt_signal_trader.code_utils").normalize_stock_code(request.stock_code)
        if (not re.fullmatch(r"[0-9]{6}\.(?:SH|SZ|BJ)", code)
                or request.action not in ("BUY", "SELL") or request.order_type is not None
                or request.price_type not in ("LIMIT", "FIX_PRICE", 11)
                or type(request.volume) is not int or request.volume <= 0
                or type(request.price) not in (int, float) or not math.isfinite(request.price) or request.price <= 0
                or not isinstance(request.remark, str) or not request.remark.strip()):
            raise ValueError("native gateway requires explicit ordinary-stock limit order")
        tag = request.remark.strip()
        if tag in self._submitted:
            raise RuntimeError("native submission attempt already consumed for this remark")
        orders, trades = self.query_submission_identities_strict(self.account_id, "")
        if any(row.user_order_id == tag for row in orders + trades):
            raise RuntimeError("remark already exists in native orders/trades")
        self._submitted[tag] = request
        self.passorder(23 if request.action == "BUY" else 24, 1101, self.account_id, code, 11,
                       float(request.price), request.volume, request.strategy_name, 2, tag, self.context)
        return self.models.OrderSubmitResult(status="SUBMITTED", user_order_id=tag, order_sys_id=None,
                                            message="native call returned; contract requires strict query/callback")

    def query_orders(self, account_id, strategy_name=""):
        rows = super().query_orders(account_id, strategy_name)
        for tag, request in self._submitted.items():
            matched = [row for row in rows if row.user_order_id == tag]
            if len(matched) > 1:
                raise RuntimeError("native remark maps to multiple orders")
            if matched:
                row = matched[0]
                code = _load_embedded("bigqmt_signal_trader.code_utils").normalize_stock_code(request.stock_code)
                if (row.stock_code != code or row.action != request.action or row.volume != request.volume
                        or not math.isfinite(row.price) or not math.isfinite(request.price)
                        or not math.isclose(row.price, request.price, rel_tol=0.0, abs_tol=1e-8)
                        or row.order_sys_id.strip().upper() in ("", "0", "-1", "0.0", "-1.0", "NONE", "NULL", "UNKNOWN")):
                    raise RuntimeError("native submitted-order identity mismatch")
        return rows

    def query_orders_strict(self, account_id, strategy_name=""):
        return self.query_orders(account_id, strategy_name)

    def query_trades_strict(self, account_id, strategy_name=""):
        return self.query_trades(account_id, strategy_name)

    def query_submission_identities_strict(self, account_id, strategy_name=""):
        return self.query_orders(account_id, strategy_name), self.query_trades(account_id, strategy_name)

    def cancel(self, order_ref):
        contract = str(order_ref.order_sys_id or "").strip()
        if not contract or contract.upper() in ("0", "-1", "0.0", "-1.0", "NONE", "NULL", "UNKNOWN"):
            raise ValueError("real native contract required for cancellation")
        if contract in self._cancel_attempted:
            raise RuntimeError("native cancel attempt already consumed for this contract")
        rows = [row for row in self.query_orders(self.account_id) if row.order_sys_id == contract]
        if (len(rows) != 1 or not order_ref.user_order_id or rows[0].user_order_id != order_ref.user_order_id
                or rows[0].status not in ("50", "55") or rows[0].traded_volume >= rows[0].volume):
            raise RuntimeError("no unique matching cancelable native order with remaining volume")
        before = rows[0]
        if (before.action not in ("BUY", "SELL") or before.volume <= 0
                or not math.isfinite(before.price) or before.price <= 0):
            raise RuntimeError("native cancel identity incomplete")
        self._cancel_attempted.add(contract)
        self._cancel_snapshots[contract] = (before.user_order_id, before.stock_code, before.action,
                                            before.volume, before.price, before.traded_volume)
        self.cancel_func(contract, self.account_id, "STOCK", self.context)
        result = self.models.CancelResult(success=False, message="native cancel returned; strict query required")
        result.order_sys_id, result.user_order_id = contract, before.user_order_id
        result.confirmation_pending, result.cancellation_confirmed = True, False
        result.order_terminal, result.native_write_executed = False, True
        return result

    def query_cancel_confirmation(self, result):
        """Only a fresh exact native contract query can confirm cancellation."""
        contract = str(result.order_sys_id or "")
        before = self._cancel_snapshots.get(contract)
        if before is None or before[0] != result.user_order_id:
            raise RuntimeError("native cancel confirmation attempt identity missing")
        rows = [row for row in self.query_orders(self.account_id) if row.order_sys_id == contract]
        if not rows:
            return result
        if len(rows) != 1:
            raise RuntimeError("native cancel contract maps to multiple orders")
        row = rows[0]
        if ((row.user_order_id, row.stock_code, row.action, row.volume) != before[:4]
                or not math.isfinite(row.price) or not math.isclose(row.price, before[4], rel_tol=0.0, abs_tol=1e-8)
                or row.traded_volume < before[5]):
            raise RuntimeError("native cancel confirmation identity mismatch")
        result.native_order_status, result.traded_volume = row.status, row.traded_volume
        result.volume, result.confirmation_source = row.volume, "native_query_orders_strict"
        if row.status in ("53", "54"):
            if ((row.status == "53" and not 0 < row.traded_volume < row.volume)
                    or (row.status == "54" and row.traded_volume != 0)):
                raise RuntimeError("native cancellation terminal quantities inconsistent")
            result.success, result.cancellation_confirmed = True, True
            result.confirmation_pending, result.order_terminal = False, True
            result.message = "native original contract cancellation confirmed by strict query"
        elif row.status in ("56", "57"):
            if ((row.status == "56" and row.traded_volume != row.volume)
                    or (row.status == "57" and row.traded_volume != 0)):
                raise RuntimeError("native order terminal quantities inconsistent")
            result.confirmation_pending, result.order_terminal = False, True
            result.confirmation_error = "CANCELLATION_NOT_CONFIRMED_ORDER_ALREADY_TERMINAL"
            result.message = result.confirmation_error
        return result


def create_native_stock_order_gateway(context, account_id, query, passorder_func, cancel_func):
    return NativeStockOrderGateway(context, account_id, query, passorder_func, cancel_func)


class NativeTradeLoginState:
    """会话内原生状态事件与本次账户采样分开计时，不把状态变化当心跳。"""
    def __init__(self, account_id, query, environment="SIM", query_login_authority=False):
        if environment not in ("SIM", "LIVE"):
            raise ValueError("explicit SIM/LIVE login environment required")
        if type(query_login_authority) is not bool or (query_login_authority and environment != "LIVE"):
            raise ValueError("current ACCOUNT login authority requires explicit LIVE selection")
        self.account_id, self.query = account_id, query
        self.strict_stock_instance = environment == "LIVE"
        self.query_login_authority = query_login_authority
        self.identity, self.event = None, None
        self.account_key, self.instance_diagnostics = None, []

    def bind_identity(self, session_id, native_pid):
        self.identity, self.event = (session_id, native_pid), None
        self.account_key, self.instance_diagnostics = None, []

    @staticmethod
    def _instance_fields(account_info):
        def field(obj, name):
            try:
                return obj.get(name) if isinstance(obj, dict) else getattr(obj, name, None)
            except Exception:
                return None  # C扩展字段不可读按身份缺失处理，不让诊断异常掩盖已有样本。
        object_type = type(account_info)
        parts = (field(object_type, "__module__"), field(object_type, "__name__"))
        return dict(raw_broker_type=field(account_info, "m_nBrokerType"),
                    raw_account_key=field(account_info, "m_strAccountKey"),
                    raw_object_type=".".join(part for part in parts if isinstance(part, str) and part) or "UNAVAILABLE")

    @staticmethod
    def _valid_account_key(key):
        return isinstance(key, str) and 0 < len(key) <= 256 and bool(key.strip())

    def record_account_callback(self, account_info):
        field = lambda name: account_info.get(name) if isinstance(account_info, dict) else getattr(account_info, name, None)
        event = dict(raw_account_id=field("m_strAccountID"), raw_status=field("m_strStatus"),
                     raw_numeric_status=field("m_nStatus"), identity=self.identity,
                     observed_monotonic=time.monotonic(), observed_at_utc=_dt.datetime.utcnow().isoformat() + "Z")
        if self.strict_stock_instance:
            event.update(self._instance_fields(account_info))
            broker_type = event["raw_broker_type"]
            foreign_type = (event["raw_account_id"] == self.account_id
                            and type(broker_type) is int and broker_type in (1, 3, 5, 6, 7, 11))
            reason = "NON_STOCK_CALLBACK_IGNORED" if foreign_type else "STOCK_CALLBACK_OBSERVED"
            if not foreign_type and (type(broker_type) is not int or broker_type != 2
                                     or not self._valid_account_key(event["raw_account_key"])):
                reason = "CALLBACK_INSTANCE_IDENTITY_MISSING"
            elif not foreign_type and self.account_key is not None and event["raw_account_key"] != self.account_key:
                reason = "CALLBACK_INSTANCE_KEY_CONFLICT"
            diagnostic = {key: value for key, value in event.items() if key not in ("identity", "observed_monotonic")}
            sid, pid = self.identity or (None, None)
            diagnostic.update(reason=reason, session_id=sid, native_pid=pid)
            self.instance_diagnostics = (self.instance_diagnostics + [diagnostic])[-3:]
            if foreign_type:
                return
        self.event = event

    def _account_query_readback(self, value, diagnostic, sample_error):
        """显式LIVE v3：查询状态为依据，原资金回调只保留事实与身份核验。"""
        event = self.event
        callback = {key: item for key, item in (event or {}).items()
                    if key not in ("identity", "observed_monotonic")}
        value.update(login_evidence_version=3, source="native_account_query",
                     freshness_basis="current_native_account_query",
                     account_instance_contract="LIVE_STOCK_ACCOUNT_QUERY_V1",
                     callback_diagnostic=callback,
                     callback_status_conflict=bool(event and event["raw_status"] != diagnostic["raw_status"]),
                     contract_source="https://dict.thinktrader.net/innerApi/trading_function.html",
                     reason=sample_error or "TRADE_LOGIN_ACCOUNT_SAMPLE_STATUS_UNVERIFIED")
        diagnostic.update(source="native_get_trade_detail_data_ACCOUNT", proves_login=True,
                          used_as_consistency_veto=False)
        for key in ("raw_account_id", "raw_status", "raw_numeric_status", "raw_broker_type", "raw_account_key", "raw_object_type"):
            value[key] = diagnostic.get(key)
        if event:
            age = time.monotonic() - event["observed_monotonic"]
            value.update(event_observed_at_utc=event["observed_at_utc"], age_seconds=age,
                         state_event_age_seconds=age)
            if (event["identity"] != self.identity or event["raw_account_id"] != self.account_id
                    or type(event.get("raw_broker_type")) is not int or event["raw_broker_type"] != 2
                    or event.get("raw_account_key") != self.account_key):
                value.update(fresh=False, reason="TRADE_LOGIN_CALLBACK_INSTANCE_CONFLICT")
                return value
        if sample_error:
            return value
        if not value["fresh"]:
            value["reason"] = "TRADE_LOGIN_ACCOUNT_SAMPLE_STALE"
        elif diagnostic["raw_status"] == "\u767b\u5f55\u6210\u529f":
            value.update(status="LOGGED_IN", positive_contract_verified=True, reason="")
        elif diagnostic["raw_status"] in ("\u51c6\u5907\u767b\u5f55", "\u5c1a\u672a\u767b\u5f55", "\u767b\u5f55\u5931\u8d25", "\u5df2\u65ad\u5f00"):
            value.update(status="NOT_LOGGED_IN", reason="TRADE_LOGIN_ACCOUNT_SAMPLE_NOT_LOGGED_IN")
        return value

    def readback(self):
        session_id, pid = self.identity or (None, None)
        sample_started = time.monotonic()
        sample_at = _dt.datetime.utcnow().isoformat() + "Z"
        diagnostic = dict(source="native_query_ACCOUNT_cache", query_observed_at_utc=sample_at,
                          proves_login=False, used_as_consistency_veto=True, raw_status=None, raw_numeric_status=None)
        sample_error = ""
        try:
            rows = self.query(self.account_id, "STOCK", "ACCOUNT")
            if not isinstance(rows, (list, tuple)) or len(rows) != 1:
                raise ValueError("TRADE_LOGIN_ACCOUNT_SAMPLE_NOT_UNIQUE")
            row = rows[0]
            field = lambda name: row.get(name) if isinstance(row, dict) else getattr(row, name, None)
            diagnostic.update(raw_account_id=field("m_strAccountID"), raw_status=field("m_strStatus"),
                              raw_numeric_status=field("m_nStatus"))
            if self.strict_stock_instance:
                diagnostic.update(self._instance_fields(row))
            if diagnostic["raw_account_id"] != self.account_id:
                raise ValueError("TRADE_LOGIN_ACCOUNT_SAMPLE_IDENTITY_MISMATCH")
            if self.strict_stock_instance:
                if type(diagnostic["raw_broker_type"]) is not int or diagnostic["raw_broker_type"] != 2:
                    raise ValueError("TRADE_LOGIN_ACCOUNT_SAMPLE_TYPE_MISSING_OR_MISMATCH")
                if not self._valid_account_key(diagnostic["raw_account_key"]):
                    raise ValueError("TRADE_LOGIN_ACCOUNT_SAMPLE_KEY_MISSING")
                if (not isinstance(session_id, str) or not session_id or type(pid) is not int or pid <= 0
                        or self.identity != (session_id, pid)):
                    raise ValueError("TRADE_LOGIN_SESSION_CHANGED")
                # 样本只固定本SID内的账户实例，不构成登录正向证据；新SID才能重置。
                if self.account_key is None:
                    self.account_key = diagnostic["raw_account_key"]
                elif self.account_key != diagnostic["raw_account_key"]:
                    raise ValueError("TRADE_LOGIN_ACCOUNT_SAMPLE_KEY_CHANGED")
            if not isinstance(diagnostic["raw_status"], str) or not diagnostic["raw_status"]:
                raise ValueError("TRADE_LOGIN_ACCOUNT_SAMPLE_STATUS_UNKNOWN")
        except Exception as exc:
            known = ("TRADE_LOGIN_ACCOUNT_SAMPLE_NOT_UNIQUE", "TRADE_LOGIN_ACCOUNT_SAMPLE_IDENTITY_MISMATCH",
                     "TRADE_LOGIN_ACCOUNT_SAMPLE_STATUS_UNKNOWN", "TRADE_LOGIN_ACCOUNT_SAMPLE_TYPE_MISSING_OR_MISMATCH",
                     "TRADE_LOGIN_ACCOUNT_SAMPLE_KEY_MISSING", "TRADE_LOGIN_ACCOUNT_SAMPLE_KEY_CHANGED", "TRADE_LOGIN_SESSION_CHANGED")
            sample_error = str(exc) if isinstance(exc, ValueError) and str(exc) in known else "TRADE_LOGIN_ACCOUNT_SAMPLE_FAILED"
            diagnostic.update(error=sample_error, exception_type=type(exc).__name__)
        sample_age = time.monotonic() - sample_started
        value = dict(status="UNKNOWN", account_id=self.account_id, session_id=session_id, native_pid=pid,
                     source="native_account_callback", raw_status=None, raw_numeric_status=None,
                     login_evidence_version=2,
                     freshness_basis="native_callback_state_with_current_account_sample",
                     event_observed_at_utc=None, age_seconds=None, state_event_age_seconds=None,
                     sample_observed_at_utc=sample_at, sample_age_seconds=sample_age,
                     fresh=not sample_error and 0 <= sample_age <= 15,
                     positive_contract_verified=False, reason="TRADE_LOGIN_CALLBACK_NOT_OBSERVED",
                     query_diagnostic=diagnostic,
                     contract_source="https://dict.thinktrader.net/innerApi/data_structure.html")
        if self.strict_stock_instance:
            value.update(account_instance_contract="LIVE_STOCK_ACCOUNT_KEY_V1", expected_broker_type=2,
                         bound_account_key=self.account_key,
                         account_instance_diagnostics=[dict(item) for item in self.instance_diagnostics])
        if self.query_login_authority:
            return self._account_query_readback(value, diagnostic, sample_error)
        if self.event is not None:
            event = self.event
            age = time.monotonic() - event["observed_monotonic"]
            value.update(raw_status=event["raw_status"], raw_numeric_status=event["raw_numeric_status"],
                         raw_account_id=event["raw_account_id"], event_observed_at_utc=event["observed_at_utc"],
                         age_seconds=age, state_event_age_seconds=age)
            if self.strict_stock_instance:
                value.update({key: event.get(key) for key in ("raw_broker_type", "raw_account_key", "raw_object_type")})
            if self.identity != (session_id, pid) or event["identity"] != (session_id, pid):
                value["reason"] = "TRADE_LOGIN_SESSION_CHANGED"
            elif event["raw_account_id"] != self.account_id:
                value["reason"] = "TRADE_LOGIN_CALLBACK_ACCOUNT_MISSING_OR_MISMATCH"
            elif self.strict_stock_instance and (type(event.get("raw_broker_type")) is not int
                                                or event["raw_broker_type"] != 2):
                value["reason"] = "TRADE_LOGIN_CALLBACK_TYPE_MISSING_OR_MISMATCH"
            elif self.strict_stock_instance and not self._valid_account_key(event.get("raw_account_key")):
                value["reason"] = "TRADE_LOGIN_CALLBACK_KEY_MISSING"
            elif self.strict_stock_instance and sample_error:
                value["reason"] = sample_error
            elif self.strict_stock_instance and event["raw_account_key"] != self.account_key:
                value["reason"] = "TRADE_LOGIN_CALLBACK_ACCOUNT_KEY_MISMATCH"
            elif not math.isfinite(age) or age < 0:
                value["reason"] = "TRADE_LOGIN_EVENT_TIME_INVALID"
            elif event["raw_status"] == "\u51c6\u5907\u767b\u5f55":
                value.update(status="NOT_LOGGED_IN", reason="TRADE_LOGIN_PREPARING_LOGIN")
            elif self.strict_stock_instance and event["raw_status"] == "\u5c1a\u672a\u767b\u5f55":
                value.update(status="NOT_LOGGED_IN", reason="TRADE_LOGIN_NOT_LOGGED_IN")
            elif event["raw_status"] == "\u767b\u5f55\u6210\u529f":
                if sample_error:
                    value["reason"] = sample_error
                elif not value["fresh"]:
                    value["reason"] = "TRADE_LOGIN_ACCOUNT_SAMPLE_STALE"
                elif diagnostic["raw_status"] == "\u51c6\u5907\u767b\u5f55":
                    value.update(status="NOT_LOGGED_IN", reason="TRADE_LOGIN_ACCOUNT_SAMPLE_PREPARING_LOGIN")
                elif self.strict_stock_instance and diagnostic["raw_status"] == "\u5c1a\u672a\u767b\u5f55":
                    value.update(status="NOT_LOGGED_IN", reason="TRADE_LOGIN_ACCOUNT_SAMPLE_NOT_LOGGED_IN")
                elif diagnostic["raw_status"] != "\u767b\u5f55\u6210\u529f":
                    value["reason"] = "TRADE_LOGIN_ACCOUNT_SAMPLE_STATUS_UNVERIFIED"
                else:
                    value.update(status="LOGGED_IN", positive_contract_verified=True, reason="")
            else:
                value["reason"] = "TRADE_LOGIN_POSITIVE_STATUS_CONTRACT_UNVERIFIED"
        return value

    def diagnostic_readback(self):
        # 复用readback的一次采样；缓存不能建立回调状态，也不改事件时间。
        return self.readback()


class NativeShmRpcRuntime:
    def __init__(self, context, query):
        if not isinstance(BIGQMT_ACCOUNT_ID, str) or not BIGQMT_ACCOUNT_ID or BIGQMT_ACCOUNT_ID == "YOUR_ACCOUNT_ID" or len(BIGQMT_ACCOUNT_ID) > 64:
            raise ValueError("confirmed fixed account required")
        if type(SHM_RPC_ALLOW_ORDER_METHODS) is not bool:
            raise ValueError("explicit boolean order permission required")
        if type(SHM_RPC_BIND_ACCOUNT_CALLBACKS) is not bool:
            raise ValueError("explicit boolean callback binding required")
        if (type(SHM_RPC_PERSISTENT_READONLY) is not bool or type(SHM_RPC_PERSISTENT_SIMULATION_READ_WRITE) is not bool
                or type(SHM_RPC_PERSISTENT_LIVE_READ_WRITE) is not bool
                or type(SHM_RPC_ACCOUNT_ALREADY_BOUND) is not bool):
            raise ValueError("explicit boolean persistent readonly selection required")
        if sum((SHM_RPC_PERSISTENT_READONLY, SHM_RPC_PERSISTENT_SIMULATION_READ_WRITE, SHM_RPC_PERSISTENT_LIVE_READ_WRITE)) > 1:
            raise ValueError("choose one persistent runtime policy")
        if BIGQMT_ENVIRONMENT not in ("SIM", "LIVE"):
            raise ValueError("explicit SIM/LIVE environment required")
        if BIGQMT_ENVIRONMENT == "LIVE":
            if (not SHM_RPC_PERSISTENT_LIVE_READ_WRITE or BIGQMT_ACCOUNT_ID == "55013746"
                    or not all("0" <= char <= "9" for char in BIGQMT_ACCOUNT_ID)):
                raise ValueError("LIVE requires explicit persistent LIVE policy and live numeric account")
        elif SHM_RPC_PERSISTENT_LIVE_READ_WRITE:
            raise ValueError("persistent LIVE policy requires LIVE environment")
        if SHM_RPC_ACCOUNT_ALREADY_BOUND and not (SHM_RPC_PERSISTENT_SIMULATION_READ_WRITE or SHM_RPC_PERSISTENT_LIVE_READ_WRITE):
            raise ValueError("prebound account only belongs to explicit persistent read-write source")
        if SHM_RPC_PERSISTENT_READONLY:
            if (BIGQMT_ACCOUNT_ID != "55013746" or SHM_RPC_ALLOW_ORDER_METHODS is not False
                    or SHM_RPC_BIND_ACCOUNT_CALLBACKS is not False or SHM_RPC_ORDER_GATEWAY is not None
                    or SHM_RPC_ORDER_GATEWAY_FACTORY is not None or type(SHM_RPC_MAX_SECONDS) is not int
                    or SHM_RPC_MAX_SECONDS != 0 or type(SHM_RPC_MAX_REQUESTS) is not int or SHM_RPC_MAX_REQUESTS != 0):
                raise ValueError("persistent mode requires fixed simulation, no write gateway/binding and zero budgets")
        elif SHM_RPC_PERSISTENT_SIMULATION_READ_WRITE:
            if (BIGQMT_ACCOUNT_ID != "55013746" or SHM_RPC_ALLOW_ORDER_METHODS is not True
                    or SHM_RPC_BIND_ACCOUNT_CALLBACKS is not False or type(SHM_RPC_MAX_SECONDS) is not int
                    or SHM_RPC_MAX_SECONDS != 0 or type(SHM_RPC_MAX_REQUESTS) is not int or SHM_RPC_MAX_REQUESTS != 0):
                raise ValueError("persistent read-write requires explicit fixed simulation writes and zero budgets")
        elif SHM_RPC_PERSISTENT_LIVE_READ_WRITE:
            if (SHM_RPC_ALLOW_ORDER_METHODS is not True or SHM_RPC_BIND_ACCOUNT_CALLBACKS is not False
                    or type(SHM_RPC_MAX_SECONDS) is not int or SHM_RPC_MAX_SECONDS != 0
                    or type(SHM_RPC_MAX_REQUESTS) is not int or SHM_RPC_MAX_REQUESTS != 0):
                raise ValueError("persistent LIVE read-write requires explicit writes and zero budgets")
        elif type(SHM_RPC_MAX_SECONDS) is not int or not 1 <= SHM_RPC_MAX_SECONDS <= 1800 or type(SHM_RPC_MAX_REQUESTS) is not int or not 1 <= SHM_RPC_MAX_REQUESTS <= 1000:
            raise ValueError("bounded runtime budgets required")
        if not callable(query) or not callable(getattr(context, "run_time", None)):
            raise RuntimeError("native query/timer API unavailable")
        if SHM_RPC_ALLOW_ORDER_METHODS and SHM_RPC_ORDER_GATEWAY is None and not callable(SHM_RPC_ORDER_GATEWAY_FACTORY):
            raise RuntimeError("write enabled without explicit authorized order_gateway")
        if SHM_RPC_ALLOW_ORDER_METHODS and SHM_RPC_ORDER_GATEWAY is not None and SHM_RPC_ORDER_GATEWAY_FACTORY is not None:
            raise RuntimeError("choose one explicit order_gateway or factory")
        if not SHM_RPC_ALLOW_ORDER_METHODS and SHM_RPC_ORDER_GATEWAY is not None:
            raise RuntimeError("default readonly bootstrap must not bind order_gateway")
        if (SHM_RPC_ALLOW_ORDER_METHODS or SHM_RPC_BIND_ACCOUNT_CALLBACKS) and not SHM_RPC_ACCOUNT_ALREADY_BOUND:
            set_account = getattr(context, "set_account", None)
            if not callable(set_account):
                raise RuntimeError("native set_account API required for order callbacks")
            set_account(BIGQMT_ACCOUNT_ID)
        self.context, self.thread_id = context, threading.get_ident()
        self.created_at, self.requests, self.closed = time.monotonic(), 0, False
        self.created_at_unix = time.time()
        self.persistent_readonly = SHM_RPC_PERSISTENT_READONLY
        self.persistent_simulation_read_write = SHM_RPC_PERSISTENT_SIMULATION_READ_WRITE
        self.persistent_live_read_write = SHM_RPC_PERSISTENT_LIVE_READ_WRITE
        self.persistent = self.persistent_readonly or self.persistent_simulation_read_write or self.persistent_live_read_write
        self.runtime_policy = ("PERSISTENT_LIVE_READ_WRITE" if self.persistent_live_read_write
                               else "PERSISTENT_SIMULATION_READ_WRITE" if self.persistent_simulation_read_write
                               else "PERSISTENT_READONLY" if self.persistent_readonly else "BOUNDED")
        self.trade_login = NativeTradeLoginState(BIGQMT_ACCOUNT_ID, query, environment=BIGQMT_ENVIRONMENT,
            query_login_authority=SHM_RPC_LIVE_ACCOUNT_QUERY_LOGIN)
        provider = NativeReadOnlyProvider(query, BIGQMT_ACCOUNT_ID)
        rpc = _load_embedded("bigqmt_signal_trader.redis_rpc")
        transport = _load_embedded("bigqmt_signal_trader.transports.shm_rpc")
        allowed = set(transport.READ_METHODS) - {"poll_execution_events"}
        if SHM_RPC_ALLOW_ORDER_METHODS:
            allowed.update(transport.WRITE_METHODS)
        gateway = provider
        if SHM_RPC_ALLOW_ORDER_METHODS:
            gateway = SHM_RPC_ORDER_GATEWAY
            if gateway is None:
                gateway = SHM_RPC_ORDER_GATEWAY_FACTORY(context, BIGQMT_ACCOUNT_ID, query,
                                                       globals().get("passorder"), globals().get("cancel"))
            if gateway is None:
                raise RuntimeError("authorized order_gateway factory returned no gateway")
        handlers = rpc.BigQmtRpcHandlers(BIGQMT_ACCOUNT_ID, None, provider, gateway,
                                        allow_order_methods=SHM_RPC_ALLOW_ORDER_METHODS,
                                        allowed_methods=allowed, settle_orders_inline=False)
        self.bridge = _load_embedded("bigqmt_signal_trader.shm_rpc_bridge").ShmRpcBridge(
            name=SHM_RPC_NAME, account_id=BIGQMT_ACCOUNT_ID, handlers=handlers,
            allow_order_methods=SHM_RPC_ALLOW_ORDER_METHODS, trade_login_reader=self.trade_login.readback,
            persistent_readonly=self.persistent_readonly,
            persistent_simulation_read_write=self.persistent_simulation_read_write,
            environment=BIGQMT_ENVIRONMENT, persistent_live_read_write=self.persistent_live_read_write)
        self.bridge.runtime_metadata = dict(runtime_policy=self.runtime_policy, environment=BIGQMT_ENVIRONMENT,
            started_at_unix=self.created_at_unix, source_deadline_unix=None if self.persistent else self.created_at_unix + SHM_RPC_MAX_SECONDS,
            max_seconds=SHM_RPC_MAX_SECONDS, max_requests=SHM_RPC_MAX_REQUESTS,
            rpc_revision="shm-rpc-single-file-v1")
        self.trade_login.bind_identity(self.bridge.mailbox.session_id, self.bridge.mailbox.server_pid)
        self.bridge.trade_login_diagnostic_reader = self.trade_login.diagnostic_readback
        if SHM_RPC_ALLOW_ORDER_METHODS:
            for field in ("passorder", "cancel_func"):
                native_function = getattr(gateway, field, None)
                if callable(native_function):
                    setattr(gateway, field, self._guard_native_write(native_function))

    def _guard_native_write(self, function):
        def guarded(*args, **kwargs):
            self._thread()
            login = self.bridge._trade_login()
            if login.get("status") != "LOGGED_IN":
                raise PermissionError(login.get("reason") or "TRADE_LOGIN_NOT_CONFIRMED")
            return function(*args, **kwargs)
        return guarded

    def _thread(self):
        if threading.get_ident() != self.thread_id:
            raise RuntimeError("native original callback thread required")

    def poll(self):
        self._thread()
        if self.closed:
            return
        if not self.persistent and (time.monotonic() - self.created_at >= SHM_RPC_MAX_SECONDS or self.requests >= SHM_RPC_MAX_REQUESTS):
            self.close("budget")
            return
        try:
            if self.bridge.poll_once():
                self.requests += 1
        except Exception:
            self.close("callback_error")
            raise

    def event(self, event_type, native_object):
        self._thread()
        if self.closed:
            return
        self.bridge.record_execution_event(event_type, native_object)

    def close(self, reason="stop"):
        self._thread()
        if self.closed:
            return
        self.closed = True
        self.trade_login.bind_identity(None, None)
        self.bridge.close()
        print("AQP_SHM_RPC_CLOSED " + json.dumps(dict(reason=reason, requests=self.requests,
                                                      native_handles_closed=True), sort_keys=True))


def init(ContextInfo):
    global _SERVER
    if _SERVER is not None:
        _SERVER.close("reload")
        _SERVER = None
    if not _EMBEDDED_SOURCES:
        raise RuntimeError("build tools/build_shm_rpc_single_file.py before native use")
    try:
        _SERVER = NativeShmRpcRuntime(ContextInfo, globals().get("get_trade_detail_data"))
        _SERVER.bridge.runtime_metadata["windows_session_id"] = _native_windows_session(_SERVER.bridge.mailbox.server_pid)
        start_at = (_dt.datetime.now() + _dt.timedelta(seconds=1)).strftime("%Y-%m-%d %H:%M:%S")
        ContextInfo.run_time("aqp_shm_rpc_adjust", "100nMilliSecond", start_at)
    except Exception:
        if _SERVER is not None:
            _SERVER.close("init_failed")
        raise
    print("AQP_SHM_RPC_READY " + json.dumps(dict(account_id=BIGQMT_ACCOUNT_ID, environment=BIGQMT_ENVIRONMENT, name=SHM_RPC_NAME,
          session_id=_SERVER.bridge.mailbox.session_id, native_pid=_SERVER.bridge.mailbox.server_pid,
          windows_session_id=_SERVER.bridge.runtime_metadata["windows_session_id"],
          allow_order_methods=SHM_RPC_ALLOW_ORDER_METHODS, bind_account_callbacks=SHM_RPC_BIND_ACCOUNT_CALLBACKS,
          max_seconds=SHM_RPC_MAX_SECONDS, max_requests=SHM_RPC_MAX_REQUESTS,
          runtime_policy=_SERVER.runtime_policy,
          started_at_unix=_SERVER.created_at_unix,
          source_deadline_unix=None if _SERVER.persistent else _SERVER.created_at_unix + SHM_RPC_MAX_SECONDS,
          background_threads=False), sort_keys=True))


def aqp_shm_rpc_adjust(ContextInfo):
    if _SERVER is not None:
        _SERVER.poll()


def handlebar(ContextInfo):
    return None


def account_callback(ContextInfo, accountInfo):
    if _SERVER is not None and not _SERVER.closed:
        _SERVER._thread()
        _SERVER.trade_login.record_account_callback(accountInfo)


def order_callback(ContextInfo, orderInfo):
    if _SERVER is not None:
        _SERVER.event("order", orderInfo)
        status = orderInfo.get("m_nOrderStatus", orderInfo.get("status")) if isinstance(orderInfo, dict) else getattr(orderInfo, "m_nOrderStatus", getattr(orderInfo, "status", None))
        try:
            rejected = int(status) == 57
        except (TypeError, ValueError):
            rejected = False
        if rejected:
            _SERVER.event("order_error", orderInfo)


def deal_callback(ContextInfo, dealInfo):
    if _SERVER is not None:
        _SERVER.event("trade", dealInfo)


def aqp_shm_rpc_order_error(ContextInfo, orderErrorInfo):
    if _SERVER is not None:
        _SERVER.event("order_error", orderErrorInfo)


def orderError_callback(ContextInfo, orderArgs, errMsg):
    if _SERVER is None or _SERVER.closed:
        return
    events = _load_embedded("bigqmt_signal_trader.exec_events")
    remark = events._attr(orderArgs, ["m_strRemark", "order_remark", "remark", "user_order_id"], "")
    if not remark:
        strategy_name = str(events._attr(orderArgs, ["strategyName"], "") or "")
        _, separator, native_remark = strategy_name.partition("_&&&_")
        remark = native_remark if separator else ""
    stock_code = events._attr(orderArgs, ["orderCode", "stock_code", "m_strInstrumentID"], "")
    try:
        stock_code = _load_embedded("bigqmt_signal_trader.code_utils").normalize_stock_code(stock_code)
    except ValueError:
        stock_code = str(stock_code or "")
    payload = {field: events._attr(orderArgs, [field]) for field in
               ("m_strOrderSysID", "order_sys_id", "order_sysid", "order_id", "seq", "request_id",
                "m_nErrorID", "error_id", "m_nOrderStatus", "status")}
    payload.update(account_id=events._attr(orderArgs, ["m_strAccountID", "accountID", "account_id"], BIGQMT_ACCOUNT_ID),
                   stock_code=stock_code, remark=remark, error_msg=str(errMsg or ""))
    _SERVER.event("order_error", payload)


def aqp_shm_rpc_cancel_error(ContextInfo, cancelErrorInfo):
    if _SERVER is not None:
        _SERVER.event("cancel_error", cancelErrorInfo)


def stop(ContextInfo):
    if _SERVER is not None:
        _SERVER.close()
