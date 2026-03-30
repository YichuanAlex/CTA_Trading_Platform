在运行python cta_platform.py test_config.json时，发现数据下载行为不符合预期。当前系统会从回测日期的开头重新下载所有数据，我的数据格式是这样的：data\\1m\\CZCE\\AP.CZCE\\2020-01-02\\AP310.CZCE，需要实现以下改进：

1. 数据下载逻辑优化：
   - 首先检查data\\1m目录下是否已存在config文件中指定的所有期权产品数据
   - 对于已存在的数据文件，验证其时间范围是否完整覆盖回测周期，检查一头一尾的日期文件是否存在，如果存在则跳过，说明此期权产品的1分钟数据已完整下载
   - 如果缺少某些日期文件，则仅并行下载缺失时间段的数据（增量下载模式）

2. 特殊情况处理：
   - 当服务器上确实不存在某些时间段的数据时：
     * 记录详细的缺失数据日志
     * 跳过这些时间段继续执行
     * 在回测报告中明确标注数据缺失情况

3. 配置要求：
   - 在test_config.json中增加配置项控制下载行为：
     * "download_mode": "incremental"（增量）/"full"（全量）
     * "skip_missing_data": true/false

4. 实现验证：
   - 添加数据完整性检查函数
   - 编写测试用例验证各种数据存在/缺失场景
   - 确保日志系统能清晰记录数据下载和处理过程

请你看看我的代码，为什么我设置交易区间从2024年9月1日到2025年1月31日时，交易系统从12月25日之后就再也不交易了，交易情况如图所示，请你帮我仔细检查交易平台代码 cta_0 调用 test_sample_strategy.py 策略的逻辑和交易策略中可能存在的问题，仔细修改代码，你不用测试我会把测试的结果粘贴给你

请你继续修改我的代码，我观察到pnl净值在2024-12-17这一天有一个跳跃，应该是这一天或者前一天的切换就开始有问题了，如第一张图所示；另外，你在给目标仓位的时候，需要 价格*数量*乘数 这个是市值，记得乘以合约乘数；最后我发现交易的过程中第二张图中这个AU的size很大，应该是有问题的，需要乘以合约乘数计算总市值得出目标仓位 test_sample_strategy.py

你这版修改的代码中netvalue一直没有变化过，请你再次修改代码解决这个问题，就在让你修改这个任务之后就任何交易的内容都没了：请你继续修改我的代码，我观察到pnl净值在2024-12-17这一天有一个跳跃，应该是这一天或者前一天的切换就开始有问题了，如第一张图所示；另外，你在给目标仓位的时候，需要 价格*数量*乘数 这个是市值，记得乘以合约乘数；最后我发现交易的过程中第二张图中这个AU的size很大，应该是有问题的，需要乘以合约乘数计算总市值得出目标仓位 `c:\Users\xlzc\Desktop\cta_0\strategy\source_code\test\test_sample_strategy.py`

你做的很好，整个交易能正常的运行起来，但是我发现代码从某个时间段之后就再也没有交易过了，netvalue一直保持为某日结束后的值，请你检查这是怎么回事，然后给我重新修改代码，保证代码在所有时间区间上都能正常的运行起来，而不会因为某种原因而不交易了

你修改了这个之后，交易进行到某个时间段一直在亏损，base从1000000一直亏损到cur只有400000，请你检擦代码这是怎么回事？依旧还是会有这个问题，请你继续仔细检检查，有没有可能是这个问题：你这版修改的代码中netvalue一直没有变化过，请你再次修改代码解决这个问题，就在让你修改这个任务之后就任何交易的内容都没了：请你继续修改我的代码，我观察到pnl净值在2024-12-17这一天有一个跳跃，应该是这一天或者前一天的切换就开始有问题了，如第一张图所示；另外，你在给目标仓位的时候，需要 价格*数量*乘数 这个是市值，记得乘以合约乘数；最后我发现交易的过程中第二张图中这个AU的size很大，应该是有问题的，需要乘以合约乘数计算总市值得出目标仓位 `c:\Users\xlzc\Desktop\cta_0\strategy\source_code\test\test_sample_strategy.py`

我发现代码运行起来有个问题：如果我把 test_config.json 48-48 设置为2025年1月1日到2025年1月31日，交易都很正常，但是如果我设置2024年12月1日到2025年1月31日，我会发现到2024年12月31日都没问题，但是到了交易日隔日的25年1月2日，直接就avail=586921.73，cur=42308，nav=0.420309了，情况如我上传的图所示，这是怎么回事，而且trading_order.csv和net_value.csv也灭有记录下来25年的所有数据？只有log和pnl.png是正常的，这是因为什么期权产品到期了还是因为跨年处理有问题？请你修改代码解决这个问题

你做的很好，但是我发现你的代码在24年11月到25年1月的时间区间的运行的都很正常，但是我要是把时间区间 `c:\Users\xlzc\Desktop\cta_0\test_config.json#L48-48` 调大到从20年1月开始时，我发现让代码从20年1月跑到5月其netvalue中的指标一直都不变，从进行过交易，请你看看怎么回事，然后接着上面的任务内容给我修改代码

你说得对：你运行的是 python cta_platform.py test_config.json，这条线上回测路径不会用到 offline_test_sample_strategy.py。线上回测的核心流程是：

- cta_platform.py 读取 test_config.json，初始化 client_api、strategy_base、strategy_manager
- strategy_manager 加载策略代码 test_sample_strategy.py ，并在每个截面周期调用 calculate_target_position
- client_api 负责交易日日历、逐日数据加载与回放、下单和账户撮合
之前我修改 offline_test_sample_strategy.py 是为了让离线回测的适配器也健壮，但这不影响你现在的线上回测流程。真正导致你把区间调到 2020-01~2020-05 后“没有交易、净值不变”的根因在于线上策略会话的门控。

问题根因

- 线上门控在 strategy_manager.on_section_bar 中生效，只有当 “当前交易日处于策略会话区间 begin_date~end_date” 时才会计算目标仓位： strategy_manager.py:on_section_bar
- 这个会话区间来自策略配置文件 test_sample_strategy.json ，当前写死为:
  - start_date: 2024-08-31
  - end_date:   2025-08-31
- 当你把 test_config.json 的 backtest_period 扩展到 2020-01~2020-05 时，strategy_manager 会认为这些日子“在策略会话之外”，因此完全不执行 calculate_target_position，自然不会产生订单，净值也就不变。
修改内容

- 我已把策略会话的 begin_date/end_date 在运行时覆盖为 test_config.json 的 backtest_period，这样策略在整个你设定的回测区间都参与交易。
- 具体修改在 cta_platform.py ：
  - 读取 backtest_period 后，调用 set_date_section(begin_date, end_date)
  - 立即将所有已加载策略的会话区间覆盖为该 begin/end
代码参考

- 覆盖策略会话区间逻辑： cta_platform.py
- 策略会话门控（问题发生点）： strategy_manager.py:on_section_bar
- 你的策略配置当前区间（与策略门控绑定）： test_sample_strategy.json
- 策略实现入口（线上）： test_sample_strategy.py
你可以直接保留原有策略配置文件不改；程序会在启动时将策略会话自动对齐到 test_config.json 的 backtest_period。这样当你把 test_config.json 的回测时间改到 2020-01~2020-05 时，策略就会参与交易。

验证建议

- 按你现有方式运行：
  - python cta_platform.py test_config.json
- 观察日志提示（我保留了原有的净值不变提醒），如果仍然显示“净值未发生变化，可能未产生交易”，通常还有两类客观因素：
  - 远端数据不可用：若登录失败，client_api 会进入纯本地模式且没有数据文件时，确实无法产生交易。你在真实环境中是“登录成功”，这点应不影响。
  - 本地数据集为空：如果你想在离线环境也跑通旧区间，可以先用 dataset.py 批量下载缓存，然后再跑线上平台（这是可选项，线上远端可用时不需要）。
补充说明

- 我之前对 offline_test_sample_strategy.py 的改动仅用于离线适配器的健壮性（比如 5m 回退与空目录过滤），不会影响你现在线上运行的路径。如果你只跑线上平台，不需要关心这个文件。
- 线上路径的关键数据加载/兜底逻辑都在 client_api.py 中，包含：
  - 交易日区间与日历构造：set_date_section
  - 当日合约枚举与 dataset 目录兜底：get_daily_symbols
  - 日内数据加载与 5m 合成： load_data
- 我没有改动你的策略逻辑 test_sample_strategy.py；这次修复只解决“策略在目标区间不参与交易”的门控问题。
完成的任务

- 覆盖策略会话区间为测试区间以启用2020交易
如果你希望策略会话始终跟随 test_config.json，而不是每次运行时覆盖，也可以把 test_sample_strategy.json 的 start_date/end_date 改成更宽的范围。不过当前的运行时覆盖方案已经满足“改 test_config.json 即生效”的需求，不需要额外改文件。

现在你修改的代码我指定 `c:\Users\xlzc\Desktop\cta_0\test_config.json#L48-48` 为从24年8月到25年8月会在某个时间后就再也不交易了，情况如图所示。请你修改代码有的问题的地方，保证整个代码在我任意指定的所有时间段上 `c:\Users\xlzc\Desktop\cta_0\test_config.json#L48-48` 都能正常执行交易，而不会发生上面你所说的问题，也不会因为非下面的一些原因而不能正确执行交易代码：问题根因 
 
 - 线上门控在 strategy_manager.on_section_bar 中生效，只有当 “当前交易日处于策略会话区间 begin_date~end_date” 时才会计算目标仓位： strategy_manager.py:on_section_bar 
 - 这个会话区间来自策略配置文件 test_sample_strategy.json ，当前写死为: 
   - start_date: 2024-08-31 
   - end_date:   2025-08-31 
 - 当你把 test_config.json 的 backtest_period 扩展到 2020-01~2020-05 时，strategy_manager 会认为这些日子“在策略会话之外”，因此完全不执行 calculate_target_position，自然不会产生订单，净值也就不变。

 还是不对，现在的代码从24年8月31日运行到25年8月31日依旧是运行到一阵时间后到24年12月11日后就不执行交易量，trade_order.csv如图所示。请你找到这个问题的根因，浏览一遍所有的代码找到为什么一阵子后就不交易量了，分析所有潜在的可能并修复