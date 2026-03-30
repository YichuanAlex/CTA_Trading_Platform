策略实现手册：test_sample_strategy

1. 策略原理
- 多因子截面调仓：在同一时点对候选期货品种进行横截面评分，选择分数最高的若干品种做多、最低的若干品种做空，等权配置。
- 因子组合：
  - 动量因子：窗口期收益率 `R = Close_t / Close_{t-window} - 1`
  - 期限结构因子：近远月价差比 `TS = Near / Far - 1`，Backwardation 为正、Contango 为负
  - 持仓量变化因子：`OIΔ = (OI_t - OI_{t-window}) / |OI_{t-window}|`
- 因子标准化与加权：对三类因子分别做截面 `z-score`，按权重加权求和得到总分 `Score = w_m*Z(R) + w_ts*Z(TS) + w_oi*Z(OIΔ)`，排序选取多空组合。

2. 设计决策
2.1 为什么选择这些因子？
- 动量：在期货强趋势阶段具备延续性，截面上能捕捉强弱分化。
- 期限结构：反映供需与库存压力，Backwardation 常对应现货偏紧、做多更有优势；Contango 多对应顺价、做空更有优势。
- 持仓量变化：捕捉主力参与度与资金流向，OI 增量与价格同向时更可信。
2.2 参数设置依据
- 窗口期 `factor_window=20`：对 1m 数据约一个交易日的长度，兼顾稳定性与响应速度，可按需求调整。
- 调仓间隔 `rebalance_interval=5`：避免过度换手与滑点，日内/隔日均可适配。
- 多空数量：`long_count=2, short_count=2`，确保组合足够分散。
- 权重：`momentum_weight=0.5, term_structure_weight=0.3, oi_weight=0.2`，动量为主、结构与OI做校验。
2.3 与apple示例的差异
- apple 示例偏单品种通道突破与复权换月；本策略为多品种截面选股（选期）模型，强调因子评分与等权多空组合。
- 接口保持一致（`init`, `calculate_target_position`），内部增加 `on_cycle/generate_signals/adjust_positions` 以便扩展。

3. 代码结构说明
3.1 配置JSON字段解析
| 字段 | 含义 |
| --- | --- |
| `class_name` | 策略类名（需与代码一致） |
| `subscribe_symbol` | 订阅品种根列表（如 `AP.CZCE`） |
| `start_date` / `end_date` | 回测区间 |
| `acct_balance_ratio` | 资金分配比例（与平台聚合逻辑一致） |
| `params.rebalance_interval` | 调仓间隔（单位：日） |
| `params.factor_window` | 因子窗口长度 |
| `params.long_count` / `short_count` | 多/空持仓品种数 |
| `params.open_allowed_after` | 允许开仓的最早时间（如 `09:10`） |
| `params.cancel_cutoff_time` | 撤单/禁新单时间（如 `14:57`） |
| `params.max_deal_pct` | 最大成交比例限制 |
| `params.momentum_weight` | 动量权重 |
| `params.term_structure_weight` | 期限结构权重 |
| `params.oi_weight` | OI变化权重 |
| `factors.momentum.window` | 动量窗口 |
| `factors.term_structure.kind` | 期限结构计算方式 |
| `factors.open_interest_change.window` | OI变化窗口 |

3.2 Python类方法职责
- `__init__`: 设置默认参数与初始化状态容器
- `init`: 接收平台 `api`，从 JSON/kwargs 注入参数，调用 `on_init`
- `on_init`: 清理状态、准备运行环境
- `on_bar`: 可选的单bar更新（历史缓存）
- `on_cycle`: 截面周期入口；汇总行情、节流调仓、生成目标
- `generate_signals`: 计算三类因子、z-score、加权、排序，输出多空列表
- `adjust_positions`: 等权生成目标手数，返回 `{symbol: net_qty}`
- `calculate_target_position`: 平台适配入口，调用 `on_cycle`

4. 运行与验证
4.1 启动命令
```bash
python cta_platform.py test_config.json
```
4.2 预期输出
- 日志出现初始化参数打印与 `[signals] long=[...] short=[...]` 提示
- 每次达到 `rebalance_interval` 后生成新的目标仓位字典
- 回测结束后在 `result/<strategy>_<begin>_<end>/` 下生成净值、交易明细与评估指标
4.3 常见问题排查
- 无行情或订阅为空：检查 `strategy/config/test/test_sample_strategy.json` 的 `subscribe_symbol` 与日期区间
- JSON解析失败：确认无尾随逗号、时间字段为 `HH:MM` 格式
- 没有进行期限结构因子计算：确保 `is_main_symbol_only=false` 或当日同根有近远月至少两张合约
- OI字段为零或缺失：因子会弱化，检查数据源是否包含 `open_interest`
- 未触发调仓：检查 `rebalance_interval` 与 `factor_window` 是否过大

5. 风险提示
- 市场适用性：在趋势明显、结构分化的环境下更有效；震荡或流动性不足时可能失效。
- 交易成本与滑点：截面调仓有换手率，需合理设置 `max_deal_pct` 与开仓/撤单时间窗口。
- 合约换月：若仅订阅主力合约，期限结构因子可能弱化；需关注开仓后近月临近交割的风险。

