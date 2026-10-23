"""Big QMT order gateway.

The passorder signature follows src/api/qmt_jq_trade.
"""

import hashlib

from xtquant import xtconstant as _xtconstant

from ..code_utils import normalize_stock_code
from ..exec_events import date_time_seconds
from ..models import CancelResult, OrderSnapshot, OrderSubmitResult, SignalAction, TradeSnapshot
from .position_bigqmt import _attr, _full_code, skip_unparsable_row


PRICE_TYPE_ALIASES = {
    "LIMIT": 11,
    "FIX_PRICE": 11,
    "LATEST_PRICE": 5,
    "MARKET_PEER_PRICE_FIRST": 44,
    "MARKET_SH_CONVERT_5_LIMIT": 43,
    "MARKET_SZ_CONVERT_5_CANCEL": 47,
}


# MiniQMT's order_type (xtconstant) and passorder's opType are two different
# numberings. They agree on 27-32 and diverge on the special-margin family:
#
#   meaning            xtconstant           passorder opType (API ref 10.1)
#   融资买入 .. 直接还款   27-32                27-32      same
#   担保品买入 / 卖出      -- (CREDIT_BUY/SELL)  33 / 34
#   专项两融              40-45                70-75      DIFFERENT
#
# Sending 40 through unchanged would reach passorder as "期货组合开多", so the
# translation is not optional. Values are taken from xtconstant by NAME: PR #88
# asserted them as literals and encoded the same mistake in its tests, which is
# why they passed while the mapping was wrong.
_XC = _xtconstant

def _account_type_codes():
    """Name -> xtconstant account-type code.

    Read off xtconstant rather than written out (PR #88 is the standing
    reminder about literals), but NOT off ACCOUNT_TYPE_DICT: the xtquant that
    wins inside Big QMT is the terminal's own bundled copy at
    bin.x64/Lib/site-packages/xtquant, not this repo's shim, and it carries 91
    names where the shim has 538. ACCOUNT_TYPE_DICT is one of the 447 it does
    not have -- reading it at import time took the whole order gateway down
    with "module 'xtquant.xtconstant' has no attribute ACCOUNT_TYPE_DICT",
    which surfaces to clients as "order_gateway is not configured".

    So: the dict when it exists, and the individual *_ACCOUNT constants
    otherwise. Those 8 are in both copies.
    """
    codes = {}
    table = getattr(_xtconstant, "ACCOUNT_TYPE_DICT", None)
    if isinstance(table, dict):
        for code, name in table.items():
            try:
                codes[str(name).strip().upper()] = int(code)
            except (TypeError, ValueError):
                continue
    for attribute in dir(_xtconstant):
        if not attribute.endswith("_ACCOUNT"):
            continue
        value = getattr(_xtconstant, attribute)
        if isinstance(value, int) and not isinstance(value, bool):
            codes.setdefault(attribute[:-len("_ACCOUNT")], value)
    # ACCOUNT_TYPE_DICT names SECURITY_ACCOUNT "STOCK"; the attribute scan
    # yields "SECURITY". Both spellings reach callers, so keep both.
    security = getattr(_xtconstant, "SECURITY_ACCOUNT", 2)
    codes.setdefault("STOCK", security)
    codes.setdefault("SECURITY", security)
    return codes


ACCOUNT_TYPE_CODES = _account_type_codes()


def _first_nonzero(row, names, default=0.0):
    """First candidate attribute with a non-zero numeric value."""
    for name in names:
        value = getattr(row, name, None)
        if value is None:
            continue
        try:
            number = float(value)
        except (TypeError, ValueError):
            continue
        if number:
            return number
    return default





CREDIT_OPTYPE_BY_ORDER_TYPE = {
    _XC.CREDIT_FIN_BUY: 27,                    # 融资买入
    _XC.CREDIT_SLO_SELL: 28,                   # 融券卖出
    _XC.CREDIT_BUY_SECU_REPAY: 29,             # 买券还券
    _XC.CREDIT_DIRECT_SECU_REPAY: 30,          # 直接还券
    _XC.CREDIT_SELL_SECU_REPAY: 31,            # 卖券还款
    _XC.CREDIT_DIRECT_CASH_REPAY: 32,          # 直接还款
    _XC.CREDIT_FIN_BUY_SPECIAL: 70,            # 专项融资买入
    _XC.CREDIT_SLO_SELL_SPECIAL: 71,           # 专项融券卖出
    _XC.CREDIT_BUY_SECU_REPAY_SPECIAL: 72,     # 专项买券还券
    _XC.CREDIT_DIRECT_SECU_REPAY_SPECIAL: 73,  # 专项直接还券
    _XC.CREDIT_SELL_SECU_REPAY_SPECIAL: 74,    # 专项卖券还款
    _XC.CREDIT_DIRECT_CASH_REPAY_SPECIAL: 75,  # 专项直接还款
}

ORDER_TYPE_BY_CREDIT_OPTYPE = dict(
    (op_type, order_type)
    for order_type, op_type in CREDIT_OPTYPE_BY_ORDER_TYPE.items()
)
ORDER_TYPE_BY_OPTYPE = dict(ORDER_TYPE_BY_CREDIT_OPTYPE)
ORDER_TYPE_BY_OPTYPE.update({
    23: _XC.STOCK_BUY,
    24: _XC.STOCK_SELL,
})

# Which side of the book each one is, for bookkeeping only -- the opType above
# is what actually goes to passorder. Repayment operations that move securities
# are classified by what they do to the holding.
_CREDIT_BUY_SIDE = frozenset({
    _XC.CREDIT_FIN_BUY, _XC.CREDIT_BUY_SECU_REPAY,
    _XC.CREDIT_FIN_BUY_SPECIAL, _XC.CREDIT_BUY_SECU_REPAY_SPECIAL,
})
_CREDIT_SELL_SIDE = frozenset({
    _XC.CREDIT_SLO_SELL, _XC.CREDIT_SELL_SECU_REPAY,
    _XC.CREDIT_DIRECT_SECU_REPAY,
    _XC.CREDIT_SLO_SELL_SPECIAL, _XC.CREDIT_SELL_SECU_REPAY_SPECIAL,
    _XC.CREDIT_DIRECT_SECU_REPAY_SPECIAL,
})


def credit_action_of(order_type):
    """BUY / SELL for a credit order_type, or None if it is not one.

    直接还款 (32 / 45) moves cash rather than securities, so it has no side;
    callers must pass an action for it explicitly.
    """
    try:
        value = int(order_type)
    except (TypeError, ValueError):
        return None
    if value in _CREDIT_BUY_SIDE:
        return SignalAction.BUY.value
    if value in _CREDIT_SELL_SIDE:
        return SignalAction.SELL.value
    return None


def credit_optype_of(order_type):
    """passorder opType for a MiniQMT credit order_type, or None."""
    try:
        return CREDIT_OPTYPE_BY_ORDER_TYPE.get(int(order_type))
    except (TypeError, ValueError):
        return None


def order_type_of_optype(op_type):
    """Translate passorder opType back to the MiniQMT order_type contract."""
    try:
        return ORDER_TYPE_BY_OPTYPE.get(int(op_type))
    except (TypeError, ValueError):
        return None


def _query_order_type(account_type, op_type, action):
    """Return MiniQMT order_type for a queried Big QMT order.

    The live stock-account ORDER object can expose ``m_nOpType`` as the C-int
    sentinel 2147483647 even though ``m_nOffsetFlag`` still carries a verified
    BUY/SELL direction. Only that exact stock-account case is losslessly
    recoverable. Missing values and non-stock accounts stay unknown so credit,
    futures and option operations are never collapsed to plain stock trades.
    """
    mapped = order_type_of_optype(op_type)
    if mapped is not None:
        return mapped
    try:
        invalid_sentinel = int(op_type) == 2147483647
    except (TypeError, ValueError):
        invalid_sentinel = False
    if not invalid_sentinel or str(account_type or "").upper() != "STOCK":
        return None
    if action == SignalAction.BUY.value:
        return _XC.STOCK_BUY
    if action == SignalAction.SELL.value:
        return _XC.STOCK_SELL
    return None


def _action_from_offset_flag(offset_flag):
    return SignalAction.BUY.value if int(offset_flag or 0) == 48 else SignalAction.SELL.value


# 报单时间。大 QMT 的 ORDER 行把日期和时间分成两个字段, MiniQMT 的
# XtOrder.order_time 是 Unix 秒, 所以要拼接后转换。成交那条路径早就读了
# m_strTradeTime, 委托这边一直漏掉 (issue #48)。
_ORDER_DATE_FIELDS = ("m_strInsertDate", "m_strOrderDate", "insert_date", "order_date")
_ORDER_TIME_FIELDS = ("m_strInsertTime", "m_strOrderTime", "insert_time", "order_time")

# 取不到时打印该行实际有哪些 m_*, 每进程一次。字段名无法离线核实,
# 猜一个然后静默返回 0 正是订单方向那个 bug 的成因。
_missing_order_time_reported = []


def _report_missing_order_time(row):
    if _missing_order_time_reported:
        return
    _missing_order_time_reported.append(True)
    try:
        available = sorted(n for n in dir(row) if n.startswith("m_"))
    except Exception:
        available = []
    print(
        "[bigqmt_order] order_time not found (tried %s / %s); ORDER row exposes: %s"
        % (", ".join(_ORDER_DATE_FIELDS), ", ".join(_ORDER_TIME_FIELDS),
           ", ".join(available) or "<none>")
    )


# 委托状态描述。官方字段表 (docs/BIGQMT_INNER_PYTHON_API_REFERENCE.md):
#   m_strCancelInfo  废单原因      <- 状态 57 时柜台的拒单理由在这里
#   m_strErrorMsg    状态信息
# 柜台消息形如 "[COUNTER] 资金可用余额不足，尚需[4789.630]"; 两个字段都空过,
# 客户端就只能看到一个没有原因的失败 (issue #60)。废单原因优先, 它更具体。
_STATUS_MSG_FIELDS = (
    "m_strCancelInfo",
    "m_strErrorMsg",
    "m_strStatusMsg",
    "status_msg",
    "error_msg",
)


def _status_message(row):
    for name in _STATUS_MSG_FIELDS:
        value = _attr(row, (name,))
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _order_time_seconds(row):
    """把 ORDER 行的报单日期+时间转成 Unix 秒, 拿不到返回 0。

    容忍几种实际会遇到的写法: 日期 '20260819' 或 '2026-08-19',
    时间 '093015'、'09:30:15' 或 '09:30:15.123'。已经是数字时间戳的直接用
    (毫秒会被归一到秒)。
    """
    raw_time = _attr(row, _ORDER_TIME_FIELDS)
    raw_date = _attr(row, _ORDER_DATE_FIELDS)
    if raw_time is None and raw_date is None:
        _report_missing_order_time(row)
        return 0

    # 已是数字: 当成时间戳 (>1e11 视为毫秒)。
    if isinstance(raw_time, (int, float)) and not isinstance(raw_time, bool):
        value = float(raw_time)
        if value > 1e11:
            value /= 1000.0
        if value > 1e8:      # 像时间戳而不是 093015 这种时分秒
            return int(value)

    date_text = "".join(ch for ch in str(raw_date or "") if ch.isdigit())
    time_text = "".join(ch for ch in str(raw_time or "") if ch.isdigit())
    if not date_text or len(date_text) < 8:
        return 0
    time_text = (time_text + "000000")[:6]   # 补齐到 HHMMSS, 丢掉毫秒
    try:
        import time as _time

        parsed = _time.strptime(date_text[:8] + time_text, "%Y%m%d%H%M%S")
        return int(_time.mktime(parsed))
    except Exception:
        return 0


def _price_type_value(value, default):
    if value is None or value == "":
        return int(default)
    try:
        return int(value)
    except (TypeError, ValueError):
        text = str(value).strip().upper()
        return int(PRICE_TYPE_ALIASES.get(text, default))


class BigQmtOrderGateway:
    def __init__(
        self,
        context_info,
        account_id="",
        passorder_func=None,
        cancel_func=None,
        get_trade_detail_data_func=None,
        account_type="STOCK",
        combo_type=1101,
        price_type=11,
        quick_trade=2,
    ):
        self.context_info = context_info
        self.account_id = account_id
        self.passorder = passorder_func
        self.cancel_func = cancel_func
        self.get_trade_detail_data = get_trade_detail_data_func
        self.account_type = account_type
        self.combo_type = combo_type
        self.price_type = price_type
        self.quick_trade = quick_trade

    def _require_passorder(self):
        if self.passorder is None:
            raise RuntimeError("passorder is not available in Big QMT runtime")
        return self.passorder

    def _require_cancel(self):
        if self.cancel_func is None:
            raise RuntimeError("cancel is not available in Big QMT runtime")
        return self.cancel_func

    def _account_type_code(self):
        """The account type as MiniQMT reports it: an xtconstant int.

        xttype.XtOrder/XtTrade both carry account_type, and real MiniQMT fills
        it with SECURITY_ACCOUNT unconditionally. This deployment knows better
        -- it is configured with the type it actually trades as -- and #92
        showed what silence costs: a credit account read as STOCK returns an
        all-zero asset row with no error. So report the configured type and
        fall back to SECURITY_ACCOUNT only when there is nothing to report.
        """
        text = str(self.account_type or "").strip().upper()
        if text in ACCOUNT_TYPE_CODES:
            return ACCOUNT_TYPE_CODES[text]
        try:
            # Already a code -- some configs set the number directly.
            return int(text)
        except (TypeError, ValueError):
            return getattr(_xtconstant, "SECURITY_ACCOUNT", 2)

    def _instrument_name(self, row, stock_code, cache):
        """证券名称 —— from the row if QMT put it there, else ContextInfo.

        Position rows carry m_strInstrumentName (position_bigqmt reads it), so
        order/deal rows plausibly do too; whether they actually do is a
        question about the terminal, not about us, hence the fallback. The
        cache is per query call: get_stock_name is an in-process ContextInfo
        call, but a day of orders can repeat the same code many times.
        """
        name = str(_attr(row, ("m_strInstrumentName", "instrument_name",
                               "stock_name"), "") or "")
        if name:
            return name
        if stock_code in cache:
            return cache[stock_code]
        resolved = ""
        getter = getattr(self.context_info, "get_stock_name", None)
        if getter is not None:
            try:
                resolved = str(getter(stock_code) or "")
            except Exception:
                resolved = ""
        cache[stock_code] = resolved
        return resolved

    def _require_query_func(self):
        if self.get_trade_detail_data is None:
            raise RuntimeError("get_trade_detail_data is not available in Big QMT runtime")
        return self.get_trade_detail_data

    @staticmethod
    def build_user_order_id(signal_id):
        text = str(signal_id or "")
        digest = hashlib.sha1(text.encode("utf-8")).hexdigest()[:10]
        return "bq:%s:%s" % (digest, text[:30])

    def submit(self, request):
        passorder = self._require_passorder()
        action = str(request.action).upper()
        credit_optype = credit_optype_of(getattr(request, "order_type", None))
        if credit_optype is not None:
            # A credit operation carries more than a side: mapping it back to
            # BUY/SELL would turn 融资买入 into an ordinary buy, which is the
            # bug behind issue #103 -- worse than the rejection it replaced,
            # because it places a real but different order.
            op_type = credit_optype
        elif action == SignalAction.BUY.value:
            op_type = 23
        elif action == SignalAction.SELL.value:
            op_type = 24
        else:
            raise ValueError("unsupported order action: %s" % request.action)

        user_order_id = str(request.remark or "").strip() or self.build_user_order_id(request.signal_id)
        account_id = request.account_id or self.account_id
        passorder(
            op_type,
            self.combo_type,
            account_id,
            normalize_stock_code(request.stock_code),
            _price_type_value(request.price_type, self.price_type),
            float(request.price),
            int(request.volume),
            request.strategy_name,
            self.quick_trade,
            user_order_id,
            self.context_info,
        )
        return OrderSubmitResult(
            status="SUBMITTED",
            user_order_id=user_order_id,
            order_sys_id=None,
            message="passorder submitted",
        )

    def cancel(self, order_ref):
        cancel_func = self._require_cancel()
        ok = cancel_func(order_ref.order_sys_id, self.account_id, self.account_type, self.context_info)
        return CancelResult(success=bool(ok), message="" if ok else "cancel returned false")

    def query_orders(self, account_id, strategy_name):
        try:
            return self.query_orders_strict(account_id, strategy_name)
        except Exception:
            return []

    def query_orders_strict(self, account_id, strategy_name):
        query = self._require_query_func()
        rows = query(account_id, self.account_type, "ORDER", strategy_name) or []
        result = []
        account_type_code = self._account_type_code()
        name_cache = {}
        for row in rows:
            try:
                stock_code = _full_code(
                    _attr(row, ("m_strInstrumentID", "instrument_id", "stock_code")),
                    _attr(row, ("m_strExchangeID", "exchange_id", "market")),
                )
            except Exception as exc:
                skip_unparsable_row("ORDER", row, exc)
                continue
            op_type = _attr(row, ("m_nOpType", "op_type", "order_type"))
            action = _action_from_offset_flag(_attr(row, ("m_nOffsetFlag", "offset_flag"), 0))
            result.append(
                OrderSnapshot(
                    order_sys_id=str(_attr(row, ("m_strOrderSysID", "order_sys_id"), "") or ""),
                    user_order_id=str(_attr(row, ("m_strRemark", "user_order_id", "remark"), "") or ""),
                    stock_code=stock_code,
                    action=action,
                    volume=int(_attr(row, ("m_nVolumeTotalOriginal", "volume"), 0) or 0),
                    traded_volume=int(_attr(row, ("m_nVolumeTraded", "traded_volume"), 0) or 0),
                    status=str(_attr(row, ("m_nOrderStatus", "status"), "") or ""),
                    price=float(_attr(row, ("m_dLimitPrice", "m_dPrice", "price"), 0.0) or 0.0),
                    strategy_name=str(_attr(row, ("m_strStrategyName", "strategy_name"), "")
                                      or strategy_name or ""),
                    remark=str(_attr(row, ("m_strRemark", "remark"), "") or ""),
                    order_time=_order_time_seconds(row),
                    status_msg=_status_message(row),
                    price_type=_attr(row, ("m_nOrderPriceType", "price_type")),
                    traded_price=float(
                        _attr(row, ("m_dTradedPrice", "traded_price", "avg_traded_price"), 0.0) or 0.0
                    ),
                    op_type=op_type,
                    order_type=_query_order_type(self.account_type, op_type, action),
                    # MiniQMT XtOrder carries these and this bridge never sent
                    # them, so every client saw AttributeError (issue #133).
                    account_type=account_type_code,
                    instrument_name=self._instrument_name(row, stock_code, name_cache),
                    # 股东代码. MiniQMT's XtOrder has it; the ORDER rows this
                    # terminal returns do not -- none of their 120 attributes
                    # is a shareholder id. Kept so the field exists (a caller
                    # reading it gets "" rather than AttributeError) and so a
                    # broker whose QMT does supply it is picked up.
                    secu_account=str(_attr(row, ("m_strShareholderID", "m_strSecuAccount",
                                                 "secu_account"), "") or ""),
                    offset_flag=_attr(row, ("m_nOffsetFlag", "offset_flag")),
                    direction=_attr(row, ("m_nDirection", "direction")),
                )
            )
        return result

    def query_trades(self, account_id, strategy_name):
        try:
            return self.query_trades_strict(account_id, strategy_name)
        except Exception:
            return []

    def query_trades_strict(self, account_id, strategy_name):
        query = self._require_query_func()
        rows = []
        last_error = None
        for detail_type in ("DEAL", "TRADE"):
            try:
                if str(strategy_name or "").strip():
                    rows = query(account_id, self.account_type, detail_type, strategy_name) or []
                else:
                    rows = query(account_id, self.account_type, detail_type) or []
                if rows:
                    break
            except Exception as exc:
                last_error = exc
        if not rows and last_error is not None:
            raise last_error
        result = []
        account_type_code = self._account_type_code()
        name_cache = {}
        for row in rows:
            traded_at_raw = _attr(row, ("m_strTradeTime", "trade_time", "traded_at"), "")
            try:
                stock_code = _full_code(
                    _attr(row, ("m_strInstrumentID", "instrument_id", "stock_code")),
                    _attr(row, ("m_strExchangeID", "exchange_id", "market")),
                )
            except Exception as exc:
                skip_unparsable_row("DEAL", row, exc)
                continue
            result.append(
                TradeSnapshot(
                    trade_id=str(_attr(row, ("m_strTradeID", "trade_id"), "") or ""),
                    order_sys_id=str(_attr(row, ("m_strOrderSysID", "order_sys_id"), "") or ""),
                    stock_code=stock_code,
                    action=_action_from_offset_flag(_attr(row, ("m_nOffsetFlag", "offset_flag"), 0)),
                    volume=int(_attr(row, ("m_nVolume", "volume"), 0) or 0),
                    price=float(_attr(row, ("m_dPrice", "m_dTradePrice", "price"), 0.0) or 0.0),
                    traded_at=str(traded_at_raw or ""),
                    user_order_id=str(_attr(row, ("m_strRemark", "user_order_id", "remark"), "") or ""),
                    # 官方 Deal 字段: m_dTradeAmount 成交额; m_strTradeDate+
                    # m_strTradeTime 合成 Unix 秒。
                    amount=float(_attr(row, ("m_dTradeAmount", "amount"), 0.0) or 0.0),
                    # The row's own strategy name first -- though on this
                    # terminal there is none: neither ORDER nor DEAL rows carry
                    # m_strStrategyName. QMT filters by strategy without ever
                    # reporting it, which is why the field read "" for
                    # everything (issue #133). The filter is the fallback: when
                    # one IS given every row belongs to it by construction. For
                    # orders this bridge submitted, the RPC layer puts the real
                    # name back from the identity store.
                    strategy_name=str(
                        _attr(row, ("m_strStrategyName", "strategy_name"), "")
                        or strategy_name or ""
                    ),
                    traded_time=date_time_seconds(
                        _attr(row, ("m_strTradeDate", "trade_date", "m_strDealDate")),
                        traded_at_raw,
                    ),
                    account_type=account_type_code,
                    instrument_name=self._instrument_name(row, stock_code, name_cache),
                    secu_account=str(_attr(row, ("m_strShareholderID", "m_strSecuAccount",
                                                 "secu_account"), "") or ""),
                    # XtTrade.commission 手续费. A live DEAL row carries BOTH
                    # m_dComssion (QMT's own misspelling) and m_dCommission, so
                    # first-non-None would stop at whichever comes first even
                    # when it is the 0.0 one. Take the first non-zero instead.
                    commission=_first_nonzero(
                        row, ("m_dComssion", "m_dCommission", "commission")),
                    offset_flag=_attr(row, ("m_nOffsetFlag", "offset_flag")),
                    direction=_attr(row, ("m_nDirection", "direction")),
                )
            )
        return result


    def query_submission_identities_strict(self, account_id, strategy_name):
        orders = self.query_orders_strict(account_id, strategy_name)
        trades = self.query_trades_strict(account_id, strategy_name)
        return orders, trades
