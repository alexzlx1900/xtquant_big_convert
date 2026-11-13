# WIN 请求级恢复补验

用户授权复用已有 WIN 源码和模型基线推进本地 P0/P1，不重复全量采集。当前 Converter 实现提交 `283ad1ee85df9612e5d616d1bba08aa2d255e1e3` 已对账户 `8882152350 / LIVE / PERSISTENT_LIVE_READ_WRITE` 提供请求级恢复；本轮不重复实现底层逻辑，补齐精确 WIN ASYNC 的跨仓库验收。

`tests/test_external_redis_gateway_readwrite_integration.py` 将 WIN 的 Gateway 身份与 VM 专属 `previous_source_unknown_target` 区分，保留各自现行配置合同。`tests/test_vm_request_scoped_gateway_integration.py` 使用 WIN `ASYNC`、生成的原生源码、本地 Redis、正式 Gateway 客户端和账户适配器验证：

- 原单 UNKNOWN 后，独立新单和撤单到达原生；同 CID 换 RID 不产生第二次调用，原账本保持。
- 原生迟到完成只释放原帧；不重放原业务。
- 原生调用后的回执发布失败保留 UNKNOWN，后续独立业务继续。
- 读通道在途/超时不冻结执行通道，读帧完成后恢复读取。

模型关联复用现有 `runtime_update_identity=True`、`QMT_RUNTIME_UPDATE_MODEL_V1`、`runtime_update_model_id`，不引入第二套模型标识。该标识须在将来的受控首次引导中随原生模型加载；仅有本地生成物或历史包不能证明 WIN 已加载。旧模型不声明身份时，上层记录明确 UNKNOWN，不因此封锁交易。默认构建仍保留旧模型的重建合同。

上层实现与机器可读基线在 AQP 仓库的 `docs/live-trading/qmt_win_request_scoped_fault_recovery_20261113.md`、`qmt_win_runtime_baseline_20261113.json`。本轮不改 WIN 文件、配置或进程，不发报撤单，不推送或部署。

## 验证

最终六个受影响文件 212 项通过，无失败或跳过（37.71 秒）；不与初轮 209 项累加。包含上述四种 WIN ASYNC 贯通故障，以及模型身份随 WIN 监督配置变化而变化的生成物验证。AQP 的 13 个文件 820 项通过。使用 `alex_quant_platform_dev`、本地替身及独立测试 Redis，没有连接生产或 QMT。

JUnit 和最终汇总保存在 `/Users/zhanglongxiang/.codex/tmp/aqp-win-p0p1-local-20261113-S1Je9T/`。本轮仅修改三个测试文件及本说明，不重复改动已经具备请求级恢复的生产源码；其他任务的未提交文件保持原样。
