# 显式实盘当前账户登录依据

`tools/build_dual_shm_single_file.py` 的 `build_source(live_account_query_login=True, ...)` 仅允许配合显式 LIVE 常驻政策；默认 False，旧模型和模拟模型不变。

QMT 官方交易示例使用当前 `get_trade_detail_data(account, 'STOCK', 'ACCOUNT')` 判断登录：<https://dict.thinktrader.net/innerApi/trading_function.html>。该候选输出 v3、`source=native_account_query`，只接受唯一且当前 SID/PID、账户、股票类型和固定实例 key 均匹配的“登录成功”。查询超过 15 秒、失败、未知、身份改变或明确未登录均阻断；每次写入前继续复核，不从资产金额或已保存快照推断登录。

资金回调状态可能与查询不一致；原文及时间独立保留在 `callback_diagnostic`，冲突在 `callback_status_conflict` 记录，不制造成功回调。回调身份冲突仍阻断。原生写入边界和外部代理识别新合同；Gateway 也必须同步验证 v3，否则不会就绪。

本地测试使用生成物和 QMT/Win32 替身，不发实机订单。实机只应用 qmt-vm-001 的显式候选；账户 8882152350 原 Redis 模型、WIN、SIM、审批和风控不修改。提交、推送、包验证、实机加载和交易效果分别计量。
