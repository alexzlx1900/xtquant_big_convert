# Gateway client-only 制品

这个制品只向外部 Gateway 安装 `bigqmt_signal_trader` RPC 客户端，不包含顶层
`xtquant` 兼容包。它用于已经安装券商官方 MiniQMT `xtquant` 的 Python 环境，避免
兼容包覆盖原生 `xtquant.xtdata`、`xtquant.xttrader` 等模块。

不要把这个制品部署到大 QMT 策略目录；大 QMT 服务端仍使用项目原有部署方式。

从仓库根目录构建：

```powershell
python tools/build_gateway_client_wheel.py --source-ref HEAD --output-dir dist/gateway-client
$wheel = Get-ChildItem dist/gateway-client/xtquant_big_convert_client-*.whl | Select-Object -First 1
python -m pip install ($wheel.FullName + "[redis]")
```

构建器只从指定 Git 提交读取 `src/bigqmt_signal_trader`，不会把该目录的工作区未提交
改动混入 wheel。构建结束前还会检查 wheel 中不存在任何 `xtquant/` 文件。
