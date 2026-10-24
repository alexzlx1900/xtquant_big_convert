# coding: utf-8
"""独立只读账户转换；来源失败、空记录及字段未知分别保留。"""

import datetime as _dt
import math
import re

from ..models import AssetSnapshot, OrderSnapshot, TradeSnapshot
from .position_bigqmt import _attr, _full_code


_ASSET_FIELDS = {
    "cash": ("m_dAvailable", "m_dAvailableCash", "available_cash", "cash"),
    "total_asset": ("m_dBalance", "total_asset", "asset"),
    "frozen_cash": ("m_dFrozenCash", "frozen_cash"),
    "market_value": ("m_dInstrumentValue", "m_dMarketValue", "market_value"),
}
_SHANGHAI = _dt.timezone(_dt.timedelta(hours=8))


def _number(value, integer=False):
    if value is None or isinstance(value, bool):
        raise ValueError("NUMBER_UNAVAILABLE")
    result = float(value)
    if not math.isfinite(result) or result < 0 or (integer and result != int(result)):
        raise ValueError("NUMBER_INVALID")
    return int(result) if integer else result


def _text(row, names, required=False):
    value = _attr(row, names)
    if value is None:
        if required:
            raise ValueError("TEXT_UNAVAILABLE")
        return ""
    if not isinstance(value, str):
        raise ValueError("TEXT_INVALID")
    if required and not value.strip():
        raise ValueError("TEXT_UNAVAILABLE")
    return value


def _timestamp(date_value, time_value):
    if not isinstance(date_value, str) or not isinstance(time_value, str):
        return 0
    date_text = date_value.replace("-", "")
    time_text = time_value.replace(":", "")
    if not re.fullmatch(r"[0-9]{8}", date_text) or not re.fullmatch(r"[0-9]{6}(?:\.[0-9]+)?", time_text):
        return 0
    try:
        value = _dt.datetime(int(date_text[:4]), int(date_text[4:6]), int(date_text[6:8]),
                             int(time_text[:2]), int(time_text[2:4]), int(time_text[4:6]), tzinfo=_SHANGHAI)
        return int(value.timestamp())
    except ValueError:
        return 0


def _account_matches(row, account_id):
    if _text(row, ("m_strAccountID", "account_id"), required=True) != account_id:
        raise ValueError("ROW_ACCOUNT_MISMATCH")


def _stock_code(row):
    value = _full_code(_attr(row, ("m_strInstrumentID", "instrument_id", "stock_code")),
                       _attr(row, ("m_strExchangeID", "exchange_id", "market")))
    if not re.fullmatch(r"[0-9]{6}\.(?:SH|SZ|BJ)", value):
        raise ValueError("STOCK_CODE_INVALID")
    return value


def _action(row, unknown):
    value = _attr(row, ("m_nOffsetFlag", "offset_flag"))
    if type(value) is int and value in (48, 49):
        return "BUY" if value == 48 else "SELL"
    unknown.append("action")
    return "UNKNOWN"


def _optional_number(row, names, field, unknown, integer=False):
    value = _attr(row, names)
    if value is None:
        unknown.append(field)
        return None
    try:
        return _number(value, integer=integer)
    except (TypeError, ValueError, OverflowError):
        unknown.append(field)
        return None


def _commission(row):
    # 两种原生拼写可同时出现；优先非零有效值，缺失或无效仍为未知。
    result = None
    for name in ("m_dComssion", "m_dCommission", "commission"):
        value = _optional_number(row, (name,), "commission", [])
        if value:
            return value
        if value == 0:
            result = value
    return result


def _convert_order(row, account_id, strategy_name):
    _account_matches(row, account_id)
    unknown = []
    action = _action(row, unknown)
    volume = _number(_attr(row, ("m_nVolumeTotalOriginal", "volume")), integer=True)
    traded = _number(_attr(row, ("m_nVolumeTraded", "traded_volume")), integer=True)
    if traded > volume:
        raise ValueError("TRADED_VOLUME_EXCEEDS_ORDER")
    status_raw = _attr(row, ("m_nOrderStatus", "status"))
    if type(status_raw) is not int or status_raw < 0 or status_raw == 2147483647:
        status = "UNKNOWN"
        unknown.append("status")
    else:
        status = str(status_raw)
    when = _timestamp(_attr(row, ("m_strInsertDate", "m_strOrderDate", "insert_date", "order_date")),
                      _attr(row, ("m_strInsertTime", "m_strOrderTime", "insert_time", "order_time")))
    if not when:
        unknown.append("order_time")
    op_type = _optional_number(row, ("m_nOpType", "op_type"), "op_type", [], integer=True)
    order_type = op_type if op_type in (23, 24) else None
    if op_type == 2147483647 and action in ("BUY", "SELL"):
        order_type = 23 if action == "BUY" else 24
    if op_type in (23, 24) and action in ("BUY", "SELL") and (op_type == 23) != (action == "BUY"):
        unknown.append("order_type_direction_conflict")
        order_type = None
    native_strategy = _text(row, ("m_strStrategyName", "strategy_name"))
    if strategy_name and native_strategy and native_strategy != strategy_name:
        raise ValueError("ROW_STRATEGY_MISMATCH")
    remark = _text(row, ("m_strRemark", "user_order_id", "remark"))
    status_msg = ""
    for field in ("m_strCancelInfo", "m_strErrorMsg", "m_strStatusMsg", "status_msg", "error_msg"):
        message = _text(row, (field,)).strip()
        if message:
            status_msg = message
            break
    result = OrderSnapshot(
        order_sys_id=_text(row, ("m_strOrderSysID", "order_sys_id"), required=True),
        user_order_id=remark, stock_code=_stock_code(row), action=action,
        volume=volume, traded_volume=traded, status=status,
        price=_number(_attr(row, ("m_dLimitPrice", "m_dPrice", "price"))),
        strategy_name=native_strategy or strategy_name, remark=remark, order_time=when,
        status_msg=status_msg,
        traded_price=_optional_number(row, ("m_dTradedPrice", "traded_price", "avg_traded_price"), "traded_price", unknown),
        price_type=_optional_number(row, ("m_nOrderPriceType", "price_type"), "price_type", unknown, integer=True),
        op_type=op_type, order_type=order_type,
        account_type=2,  # 本转换器的实际查询固定为 STOCK；不依赖原生 xtquant。
        instrument_name=_text(row, ("m_strInstrumentName", "instrument_name")),
        secu_account=_text(row, ("m_strShareholderID", "m_strSecuAccount", "secu_account")),
        offset_flag=_optional_number(row, ("m_nOffsetFlag", "offset_flag"), "offset_flag", [], integer=True),
        direction=_optional_number(row, ("m_nDirection", "direction"), "direction", [], integer=True),
    )
    return result, (result.stock_code, result.order_sys_id), unknown


def _convert_trade(row, account_id, strategy_name):
    _account_matches(row, account_id)
    unknown = []
    trade_date = _attr(row, ("m_strTradeDate", "trade_date", "m_strDealDate"))
    traded_at = _text(row, ("m_strTradeTime", "trade_time", "traded_at"))
    # 原生 DEAL 上午可返回 HMMSS；只补小时前导零，保留原值和严格日期校验。
    parsed_at = "0" + traded_at if re.fullmatch(r"[0-9]{5}", traded_at) else traded_at
    when = _timestamp(trade_date, parsed_at)
    if not when:
        unknown.append("traded_time")
    native_strategy = _text(row, ("m_strStrategyName", "strategy_name"))
    if strategy_name and native_strategy and native_strategy != strategy_name:
        raise ValueError("ROW_STRATEGY_MISMATCH")
    result = TradeSnapshot(
        trade_id=_text(row, ("m_strTradeID", "trade_id"), required=True),
        order_sys_id=_text(row, ("m_strOrderSysID", "order_sys_id"), required=True),
        stock_code=_stock_code(row), action=_action(row, unknown),
        volume=_number(_attr(row, ("m_nVolume", "volume")), integer=True),
        price=_number(_attr(row, ("m_dPrice", "m_dTradePrice", "price"))),
        traded_at=traded_at, user_order_id=_text(row, ("m_strRemark", "user_order_id", "remark")),
        amount=_optional_number(row, ("m_dTradeAmount", "amount"), "amount", unknown),
        strategy_name=native_strategy or strategy_name, traded_time=when,
        account_type=2,
        instrument_name=_text(row, ("m_strInstrumentName", "instrument_name")),
        secu_account=_text(row, ("m_strShareholderID", "m_strSecuAccount", "secu_account")),
        commission=_commission(row),
        offset_flag=_optional_number(row, ("m_nOffsetFlag", "offset_flag"), "offset_flag", [], integer=True),
        direction=_optional_number(row, ("m_nDirection", "direction"), "direction", [], integer=True),
    )
    return result, (result.stock_code, result.trade_id, str(trade_date or "")), unknown


class ReadOnlyAccountProvider:
    def __init__(self, query):
        self.query = query

    def _read(self, account_id, detail_type, result_key, strategy_name=""):
        evidence = {
            "contract_version": 1, "account_id": account_id, "account_type": "STOCK",
            "source": "native_get_trade_detail_data", "detail_type": detail_type,
            "queried_at": _dt.datetime.now(_dt.timezone.utc).isoformat(),
            "strategy_name": strategy_name, "raw_row_count": None,
            "converted_row_count": None, "returned_row_count": None,
            "skipped_row_count": None, "duplicate_row_count": None,
            "unknown_fields": [], "row_errors": [], "complete": False,
            "status": "FAILED", "error_code": None,
        }
        result = {result_key: None, "source_evidence": evidence}
        try:
            rows = self.query(account_id, "STOCK", detail_type, strategy_name) if detail_type in ("ORDER", "DEAL") else self.query(account_id, "STOCK", detail_type)
        except Exception as error:
            evidence.update(error_code=detail_type + "_QUERY_FAILED", exception_type=type(error).__name__)
            return result, None
        if rows is None or not isinstance(rows, (list, tuple)):
            evidence["error_code"] = detail_type + ("_ROWS_UNAVAILABLE" if rows is None else "_ROWS_INVALID")
            return result, None
        evidence.update(raw_row_count=len(rows), converted_row_count=0, returned_row_count=0,
                        skipped_row_count=0, duplicate_row_count=0)
        return result, rows

    def get_asset(self, account_id):
        result, rows = self._read(account_id, "ACCOUNT", "asset")
        evidence = result["source_evidence"]
        if rows is None:
            return result
        if len(rows) != 1:
            evidence["error_code"] = "ACCOUNT_ROWS_UNAVAILABLE" if not rows else "ACCOUNT_ROWS_AMBIGUOUS"
            return result
        try:
            _account_matches(rows[0], account_id)
        except Exception as error:
            evidence["error_code"] = str(error) if isinstance(error, ValueError) else "ACCOUNT_CONVERSION_FAILED"
            evidence["exception_type"] = type(error).__name__
            return result
        values = {}
        evidence["field_sources"] = {}
        for field, names in _ASSET_FIELDS.items():
            values[field] = None
            for name in names:
                raw = _attr(rows[0], (name,))
                if raw is None:
                    continue
                try:
                    values[field] = _number(raw)
                    evidence["field_sources"][field] = name
                    break
                except (TypeError, ValueError, OverflowError):
                    continue
            if values[field] is None:
                evidence["unknown_fields"].append(field)
        result["asset"] = AssetSnapshot(account_id=account_id, **values)
        evidence.update(converted_row_count=1, returned_row_count=1)
        complete = not evidence["unknown_fields"]
        evidence.update(complete=complete, status="OK" if complete else "PARTIAL",
                        error_code=None if complete else "ACCOUNT_CONVERSION_INCOMPLETE")
        if complete:
            delta = values["total_asset"] - values["cash"] - values["frozen_cash"] - values["market_value"]
            evidence["reconciliation"] = {
                "status": "MATCH" if abs(delta) <= 0.02 else "MISMATCH",
                "total_minus_components": delta, "tolerance": 0.02,
            }
            # 保留原生原值，但零总资产与非零分项冲突时不能作为完整资产。
            if values["total_asset"] == 0 and delta < -0.02:
                evidence.update(complete=False, status="PARTIAL",
                                error_code="ACCOUNT_TOTAL_ASSET_ZERO_CONFLICT")
        else:
            evidence["reconciliation"] = {"status": "UNKNOWN", "total_minus_components": None, "tolerance": 0.02}
        return result

    def query_records(self, account_id, detail_type, strategy_name=""):
        if detail_type not in ("ORDER", "DEAL"):
            raise ValueError("unsupported readonly detail type")
        key = "orders" if detail_type == "ORDER" else "trades"
        result, rows = self._read(account_id, detail_type, key, strategy_name)
        evidence = result["source_evidence"]
        if rows is None:
            return result
        result[key] = []
        seen = set()
        converter = _convert_order if detail_type == "ORDER" else _convert_trade
        for index, row in enumerate(rows):
            try:
                value, identity, unknown = converter(row, account_id, strategy_name)
                evidence["converted_row_count"] += 1
                if identity in seen:
                    evidence["duplicate_row_count"] += 1
                    continue
                seen.add(identity)
                result[key].append(value)
                if unknown:
                    evidence["unknown_fields"].append({"row_index": index, "fields": unknown})
            except Exception as error:
                evidence["skipped_row_count"] += 1
                evidence["row_errors"].append({"row_index": index, "error_code": str(error)[:120], "exception_type": type(error).__name__})
        complete = not evidence["skipped_row_count"] and not evidence["duplicate_row_count"] and not evidence["unknown_fields"]
        evidence.update(returned_row_count=len(result[key]), complete=complete,
                        status="EMPTY" if complete and not rows else ("OK" if complete else "PARTIAL"),
                        error_code=None if complete else detail_type + "_CONVERSION_INCOMPLETE")
        return result
