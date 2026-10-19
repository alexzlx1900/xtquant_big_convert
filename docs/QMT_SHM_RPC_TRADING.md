# 显式 SHM 交易传输本地合同

目标：独立 `shm_rpc`，复用固定邮箱协议、现有 `BigQmtRpcHandlers` 与 `exec_events`；不修改 `shm_readonly` 的安全门。默认客户端与服务端均关闭订单方法；开启仍须固定账户与显式会话/PID握手。

## 2026-10-15 独立交易登录门禁

股票 Account 对象的原生字段为 `m_strAccountID`、`m_strStatus`，依据是[官方数据结构](https://dict.thinktrader.net/innerApi/data_structure.html)和[账户回调](https://dict.thinktrader.net/innerApi/callback_function.html)。不移植 MiniQMT 或信用账户的数值状态，也不把 TASK、资产查询、set_account 或合成 ONLINE 当作登录证明。

真实独立 `account_callback` 已取得账户 55013746 的精确文本“登录成功”，保留唯一正向文本映射。旧版证据合同要求回调年龄不超过15秒；2026-10-19 新版 `login_evidence_version=2` 修正为会话内状态事件与本次账户采样分开计时。官方回调是“资金账号状态变化”，没有15秒心跳承诺，不能仅因状态事件变老而判定登出。新版当前仅为本地实现，应用实机须另以固定候选及真实路径验收。

v2 的 `freshness_basis=native_callback_state_with_current_account_sample`、`source=native_account_callback`。只有当前 SID/PID 下已收到精确账户“登录成功”事件，并且本次原生线程中唯一 `ACCOUNT` 采样的 `m_strAccountID/m_strStatus` 与该事件一致，才为 `LOGGED_IN`。`event_observed_at_utc`、`age_seconds` 保留真实事件时间与年龄，`state_event_age_seconds` 等于该年龄；新增 `sample_observed_at_utc/sample_age_seconds` 表示本次采样起点及单调耗时，`fresh` 只表示该采样有效且年龄0至15秒。采样异常、空/多行、错账户、未知或负状态均否决；负回调优先覆盖旧成功，未知字符串仍 UNKNOWN，新 SID 和停止清空事件。

每次 `readback` 仅查询一次 `ACCOUNT`，`diagnostic_readback` 复用同一结果；`query_diagnostic.proves_login=False`、`used_as_consistency_veto=True`，缓存不能单独建立正状态，不能刷新原事件时间。Bridge 在每次写入及最终原生函数边界重新读取；v2 严格核对版本、来源/身份、事件非负有限年龄、相同事件年龄字段、本次采样0至15秒及诊断一致性，未知版本拒绝。无版本旧合同继续原15秒约束，不自动放宽。该合同证明当前 QMT 客户端报告的状态，不宣称具有普通股票柜台强查询API；本地手册6.8明确 `ACCOUNT` 为客户端缓存。

新增只读 `get_trade_login_state`，返回原始状态、回调观测时间、年龄、来源和拒绝原因。`query_diagnostic` 保存 ACCOUNT 缓存的原始字段，明确 `proves_login=False`。每份新的 submit/cancel 在最终原生调用前重新检查；未确认登录返回 `BLOCKED`、`native_write_executed=False`，阻断结果缓存，登录恢复不能借同一 request_id 重发。

v2本地验收：`tests/test_shm_trade_login.py tests/test_shm_rpc_single_file.py tests/test_shm_rpc.py tests/test_dual_shm_single_file.py` 共230项通过；最后补充查询期间会话切换防护后，登录定向55项通过，两组重叠不相加。覆盖实际生成bundle、超过120秒正事件配合当前一致采样、事件时间保真、单次ACCOUNT读取、负事件覆盖、负/缺/多行/错账户/异常样本、样本超龄和冻结旧样本年龄攻击、未知版本、停止/新SID及无登录来源零写。生成外壳继续通过ASCII/GBK/Python3.6检查。此记录仅证明本地工程合同，未加载新实机源、未请求交易、未推送或部署。

历史首轮回调时间 `2026-10-15T13:44:43.989912Z`，读时约28.109秒；恢复回调 `2026-10-15T14:04:44.175167Z`，读时约95.125秒，按当时v1合同均为 `STALE/UNKNOWN`，不能把这一工程判定写成柜台已登出的事实。2026-10-19 12:11三次保存查询中来源及RPC可用、写账本未阻断，仅事件年龄28.906/30.203/31.515秒触发旧门禁，缓存状态仍为“登录成功”，由此定位上述状态事件与心跳混用问题。未知登录仍保留强阻断，不以只读连接代替登录。

首批 Converter 119 项及 Gateway 36 项测试通过；取得真实字段后 63 项受影响/生成包检查通过，集合重叠不求和。真实恢复候选默认禁写，本轮零下单、零撤单、零卖出；未证明新单柜台接受或撤单终态。Converter `alex-quant` 工作区未提交，无关 `position_bigqmt.py` 改动保持。

`ShmRpcClient.call` 仅支持查询、`submit_order`、`cancel_order` 和事件读取。单在途；发送后超时或连接错误返回 `UNKNOWN`、`automatic_retry=false` 并禁止复用。request_id 在桥会话内不重复执行；缓存有上限且满后拒绝新请求，不能淘汰后重放写请求。会话重启不提供持久去重，不允许以新会话重发未确认订单。

桥必须在构造时的 QMT callback 线程调用 `poll_once`；不启动线程、不用 socket/Redis/业务文件轮询。注入既有处理器；异步合同号核对使用既有 settlement 在后续 callback 完成；无合同号或异常仍为 UNKNOWN。取消 RPC 仅证明调用返回，真实撤单由回报与查询确认。

回报复用既有归一化，内存队列有界。`poll_execution_events(after_sequence)` 返回独立事件序号、会话身份、溢出缺批标记；序号不是订单合同号。字段缺失保存 UNKNOWN，不伪造身份。关闭释放邮箱句柄。

验收：本地真实邮箱协议+内存 region +既有业务处理器；默认拒绝、账户/会话错配、重放、超时、迟到、缺订单身份及事件缺批。仅本地实现与合同验证，不构成 QMT 原生启动、真实订单或现金/持仓闭环验收。原生 bootstrap 本轮不新增打包框架，部署须单独取得运行授权。

## 本轮结果

新增 `transports/shm_rpc.py` 的客户端与 `shm_rpc_bridge.py` callback 桥；接口：

- `ShmRpcClient(name=..., account_id=..., allow_order_methods=False, timeout_seconds=5.0)`。
- 写入前显式 `verify_identity(expected_session_id=..., expected_native_pid=...)`；`call(method, params, request_id=...)` 与读取调用形式一致。
- 写调用返回 `{status, result, order_sys_id, automatic_retry, error}`。`CONFIRMED` 仅指已有合同号，不能代表成交或现金闭环；撤单返回始终保留 UNKNOWN，须查回报。
- `ShmRpcBridge(name=..., account_id=..., handlers=..., allow_order_methods=False).poll_once()` 必须在原 callback 线程执行；`record_execution_event(event_type, native_object)` 由原 order/deal/error callback 注入。
- `poll_execution_events(after_sequence)` 返回队列、会话/PID、latest/oldest、gap、lost_sequence。单事件超出1536字节或不可编码时输出明确损失记录，避免无界负载。

`PYTHONPATH=src conda run --no-capture-output -n alex_quant_platform_dev python -m pytest tests/test_shm_rpc.py -q`：12项通过（默认两端关闭、固定账户/握手、处理器异步合同号、request_id重放/冲突、缺身份、撤单未知、发送超时/迟到、目标漂移、事件溢出/负载损失、callback线程、连接中断及句柄释放）。本轮未提交converter，未推送/部署/实际交易。

原生 bootstrap、真实订单/回报，以及真实现金与持仓对账仍未验收。跨进程重启没有持久request_id账本，未确认写请求禁止自动重发。

### 最终审查修复：回报合同身份

独立审查发现旧normalizer允许把native局部 `order_id` 回退为合同号。仅本桥覆盖该兼容结果：真实合同只从原对象的 `m_strOrderSysID`、`order_sys_id`、`order_sysid` 读取，空值、0、-1等无效值保持 UNKNOWN；`request_id`、`native_order_id`、`native_sequence` 独立保留用于后续归属，不填合同号。全局 `exec_events.py` 未修改。

新增 order/trade/order_error/cancel_error 全四类回报的局部ID-only、无效合同及真实合同字段回归检查（同时覆盖dict和native对象）。同一针对性测试命令：60项通过，0.39秒；仍为本地合同证据。

## P2 原生单文件入口（本地实现）

新增 `src/BIGQMT_SHM_RPC.py` 与 `tools/build_shm_rpc_single_file.py`，沿既有base64内嵌惯例构建；外壳ASCII/GBK兼容，外壳及直接内嵌依赖通过Python 3.6 AST检查。复用现有 `BigQmtRpcHandlers` 与转换代码；只从 `redis_rpc.py` 提取业务handler、settlement、序列化和必要辅助/常量，不包含Redis服务、客户端或文件logger。新入口和构建器的import位于文件头；所提取既有handler内部兼容import保留原行为，默认禁写路径不依赖Redis/socket/xtquant。

原生 `init` 核对固定账户及native query/timer，建立邮箱并注册 `run_time("aqp_shm_rpc_adjust", "100nMilliSecond", start_at)`；`handlebar`不重复执行；timer/stop遵循构造callback线程，60秒/180请求默认预算，异常与停止释放邮箱。默认 `SHM_RPC_ALLOW_ORDER_METHODS=False`，不绑定passorder/cancel，写方法不出现在ping capabilities；显式开启而未注入另行授权的既有order_gateway，初始化直接拒绝。文件自身不会发起订单。

资产、持仓、委托、成交查询使用真实handler和既有严格native转换。异常/缺字段保留UNKNOWN或明确失败，不用零值代替。原生 `order_callback`、`deal_callback` 接入队列；照既有strategy，order status=57派生 `order_error`。撤单错误保留显式 `aqp_shm_rpc_cancel_error` 入口；未取得broker原生撤单错误callback签名证据，不能声明该自动回报已接通。

初始化mark_ready失败新增最小资源清理；清理异常作为原异常的cleanup_error证据保留，不覆盖首个初始化错误。生成文件只写调用者指定的新文件，拒绝覆盖；本轮构建测试的产物位于仓库外临时目录，未部署到QMT。

验收：实际exec生成外壳和内嵌handler，仅Win32 Region与native查询数据替换为本地fixture；禁写、固定身份/capabilities、资产/持仓/委托/成交查询、order/deal+拒单派生/显式错误队列、stop、timer注册失败/mark_ready失败及清理失败、预算、原线程、ASCII/GBK/3.6、明确输出与拒绝覆盖均验证。该证据不构成真实QMT原生启动或订单/现金闭环验收。

P2本轮针对性命令：`PYTHONPATH=src conda run --no-capture-output -n alex_quant_platform_dev python -m pytest tests/test_shm_rpc_single_file.py tests/test_shm_rpc.py -q`，68项通过（0.77秒）；`git diff --check`通过。仅本地未提交converter，未推送、未操作VM、未部署、未启用实际交易。

### P2接入审查修复：默认邮箱名称

默认原生 `SHM_RPC_NAME` 已改为 `Local\AQP_SHM_TRADE_SIM`，满足现有AQP `gateway_config._parse_bigqmt_rpc` 的 `Local\AQP_SHM_TRADE_[A-Za-z0-9_-]{1,58}` 独立交易邮箱约束，避免bootstrap READY后Gateway配置仍被拒绝。生成测试新增对应约束检查，受影响的single-file测试9项通过（0.44秒）；另以临时只读脚本实际导入AQP现有配置解析器，解析生成外壳的默认名称成功，`allow_order_methods=False` 保持。未改AQP配置文件、未部署、未开启交易。

### 模拟单笔验收前的原生回调修补

本次只修 `BIGQMT_SHM_RPC.py` 直接入口、生成脚本测试及本说明，服务于用户已确认的模拟账户 `55013746`、`000001.SZ` 买入 `100` 股且最多一笔的实机验收。产品默认 `SHM_RPC_ALLOW_ORDER_METHODS=False`、`SHM_RPC_ORDER_GATEWAY=None` 保持；该修补自身不提交订单，测试使用内存邮箱和原生对象 fixture，未操作 VM。

显式开启订单且已注入授权 gateway 时，初始化先执行 `ContextInfo.set_account(BIGQMT_ACCOUNT_ID)`，再创建邮箱和发布 READY。缺少该 API 或绑定抛错立即终止，不发布 READY、不创建邮箱；只读默认入口不要求此 API。后续 timer 注册失败仍关闭已创建邮箱，不用初始化错误代替成功状态。

补充原生三参 `orderError_callback(ContextInfo, orderArgs, errMsg)`。按[迅投原生回调文档](https://dict.thinktrader.net/innerApi/callback_function.html)将 `accountID`、`orderCode` 和第三参错误文本局部适配到既有 `order_error` 归一化；优先保留显式 remark，只有原生 `strategyName` 含 `_&&&_` 分隔符时才提取其后备注，无分隔符不把策略名当备注。`SZ000001` 经既有代码转换为 `000001.SZ`；无法识别的代码保留原值，不伪造目标。真实合同仍只从显式 sysid 字段读取，native 局部订单 ID、序号与 request_id 只保留关联证据，缺合同号继续 `UNKNOWN`。全局 `exec_events.py` 未修改。

官方回调要求在策略交易的实盘运行模式下先绑定账户；模拟验收必须额外核对终端路径、账户和模拟柜台，编辑器内部撮合结果不能代替柜台委托、回报或现金持仓证据。撤单错误仍仅有既有显式入口，未增加未经证实的原生签名。

验收覆盖实际生成 bundle 的账户绑定早于 mailbox READY、缺 API/绑定失败、绑定后 timer 失败清理、默认禁写与禁止注入 gateway、三参错误回调的 dict/native 对象、短备注/目标/错误消息、局部 ID 不冒充合同号、真实合同和账户错配。先运行 `tests/test_shm_rpc_single_file.py`，18 项通过（0.63 秒）；再运行 `PYTHONPATH=src conda run --no-capture-output -n alex_quant_platform_dev python -m pytest tests/test_shm_rpc_single_file.py tests/test_shm_rpc.py -q`，受影响范围共 78 项通过（1.04 秒），两次测试存在重叠，不相加。`git diff --check` 与三个未跟踪目标文件的 `--no-index --check` 均通过。仅证明本地工程合同，原生订阅注册效果及模拟订单、成交、现金持仓闭环须由主代理另行实机核验。converter 保持本地未提交，未推送、部署或发布。

### 协议 fixture 暴露的日期解析阻断

临时单笔协议 fixture 在提交期间同步注入带 `m_strInsertDate`、`m_strInsertTime` 的原生委托回报，实际经过生成 bundle 的 `normalize_order_event -> date_time_seconds -> time.strptime`；标准库惰性导入 `_strptime` 被入口 allowlist 拒绝，提交结果因此保留 `UNKNOWN`。该问题来自本地真实协议/内存 region 验证，尚未执行实机订单。

最小修复仅在 `BIGQMT_SHM_RPC.py` 允许标准库 `_strptime`，保持其余导入限制、默认禁写、gateway 注入规则及回报时间语义。新增实际生成 bundle 的委托/成交日期字段回归，核对解析后的时间、目标、短备注与真实合同身份；修复前两项均复现上述 ImportError，修复后两项通过（0.12 秒），没有提前加载模块绕过问题。受影响 `tests/test_shm_rpc_single_file.py tests/test_shm_rpc.py` 共 80 项通过（1.01 秒）；该次数覆盖前述 78 项，不相加。临时单笔协议完整复验和原生实机订单证据由主代理负责，converter 仍未提交或部署。

## 核心迁移的可运行原生候选

直接缺口是生成入口只有外部 gateway 对象注入，真实模拟下单曾依赖仓库外单笔适配器。既有 `BigQmtOrderGateway` 包含 `xtquant` 和信用交易常量，不能直接加载进受限单文件。入口现提供 `NativeStockOrderGateway` 与显式 `create_native_stock_order_gateway` factory：沿既有 `OrderRequest`、`OrderSubmitResult`、`CancelResult` 和 `BigQmtRpcHandlers` 接口，查询复用严格 `account_readonly`，不增加网络、线程、文件日志或另一套订单协议。

最小写接口限定普通股票、显式 BUY/SELL、正整数数量与正有限限价；不接信用、期货、市场价或批量方法。提交沿既有 `passorder(23/24,1101,account,code,11,price,volume,strategy,2,remark,ContextInfo)`，返回值不作为合同号。`submit(request)`、`cancel(OrderRef)`、严格委托/成交与提交身份查询由该 gateway 提供。相同 remark 的原生提交尝试、相同合同的撤单尝试均在调用前消费，异常或 UNKNOWN 后不能以新 request_id 再执行 native 操作；进程重启仍没有持久尝试账本，未知请求不能跨 session 重发。

提交前拒绝已存在的 remark，提交后查询检查唯一 remark、代码、方向、数量、价格与真实 sysid。原生空策略名保留为空。撤单必须具有唯一真实合同、精确 user_order_id、可撤状态 `50/55` 和未成交余量，接口返回仍为 UNKNOWN，须查回报。桥仅对注入了 `validate_submit_params` 的 gateway 调用该提交前钩子；native stock gateway 拒绝显式 `order_type`，避免 bundle 缺 `xtquant` 时旧 handler 将信用类型丢弃并变成普通股票 BUY/SELL。全局 `redis_rpc.py`、`exec_events.py`、既有 `order_bigqmt.py` 未修改。

`SHM_RPC_ORDER_GATEWAY_FACTORY=None` 与原对象注入互斥；只有显式开启订单时才构造 factory，并要求可调用的原生 passorder/cancel。`SHM_RPC_BIND_ACCOUNT_CALLBACKS=False` 是独立显式选项，只读回报候选可开启它并在 READY 前绑定账户；不授予订单权限。旧默认只读入口不要求 set_account。产品 `SHM_RPC_ALLOW_ORDER_METHODS=False`、`SHM_RPC_ORDER_GATEWAY=None` 保持，构建器没有自动启用写权限的选项。

构建固定账户、独立邮箱和预算的只读回报候选：

```bash
conda run --no-capture-output -n alex_quant_platform_dev python tools/build_shm_rpc_single_file.py \
  --output /absolute/new/QMT_CORE_SHM_RPC_READONLY.py \
  --account-id 55013746 --name 'Local\AQP_SHM_TRADE_CORE_20261015' \
  --max-seconds 900 --max-requests 1000 \
  --native-stock-gateway --bind-account-callbacks
```

产物只写指定新文件，拒绝覆盖。该候选可在授权模拟策略入口中启动；运行前仍须核对模拟客户端路径、账户、PID/session 与柜台模式，READY 后重新握手。当前候选禁写，显式 factory 不被实例化。READY 包含 `source_deadline_unix`、`max_seconds`、`max_requests` 和 `bind_account_callbacks`；deadline 是原生预算起点的 Unix 时间投影，实际停止仍按 monotonic 计时。timer 达到 900 秒或 1000 请求时关闭自有邮箱，`stop(ContextInfo)` 可提前关闭；600 秒观察窗口开始前须确认原生剩余预算不少于 660 秒，预留启动收尾余量。不启动原模型，不修改其文件或共享配置。停止后核对临时模型、mapping/mutex 和保护文件，再回退到原停止状态。订单参数尚未在本轮冻结，不能靠该构建命令或旧单笔预算启用写权限。

已保存真实模拟订单合同 `7620` 的目标、空策略名、限价 11.42、成交价 11.41 和日期字段用于核对 fixture 形状；不沿用该报价、合同、100 股数量或 remark 发新单，也不把本地回放写成实机结果。生成 bundle 回归覆盖默认禁写、factory 延迟构造、显式只读账户绑定、原生提交签名、真实合同查询、空策略名保留、request_id 重放、UNKNOWN 后同 remark/合同不再调用、非普通股票接口拒绝、实际日期解析及构建预算边界。先跑 single-file 定向测试，再跑 `tests/test_shm_rpc_single_file.py tests/test_shm_rpc.py`，共 95 项通过（1.67 秒）；新增候选通过 ASCII/GBK/Python 3.6 检查。实机订单/撤单、柜台回报、长期恢复及正式 OMS 页面链路仍由主代理验收。converter 保持本地未提交，未部署或发布。

随后仅补 READY 的期限元数据，最新 single-file 35 项通过（1.24 秒），不与 95 项相加。实际生成的最终候选是 `/Users/zhanglongxiang/.codex/tmp/aqp-qmt-core-native-candidate-20261015-pnIKoz/QMT_CORE_SHM_RPC_READONLY_900_v2.py`；此前同目录的 600 秒与无期限字段的 900 秒候选保留作准备证据，不用于最终模型。该文件未在 VM 中执行，两来源组合需使用独立 namespace，分别保存 `_SERVER`、`_MODULE_CACHE`、timer wrapper 与关闭路径，不能把两个 bundle 的全局状态混在一起。
