# CTA Trading Platform | 商品交易顾问交易系统

<div align="center">

**Quantitative Trading System | Futures & Options | 量化交易系统 | 期货期权**

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://www.python.org/)
[![Trading](https://img.shields.io/badge/Trading-CTA%20Strategy-green.svg)]()
[![Backtesting](https://img.shields.io/badge/Backtesting-Event--Driven-orange.svg)]()

**Author | 作者**: YichuanAlex (Zixi Jiang)  
**Email | 邮箱**: jiangzixi1527435659@gmail.com  
**Last Updated | 最后更新**: 2026-03-30

</div>

---

## 目录 | Table of Contents

- [项目概述](#项目概述)
- [研究背景](#研究背景)
- [研究目标](#研究目标)
- [系统架构](#系统架构)
- [核心功能](#核心功能)
- [方法论](#方法论)
- [项目结构](#项目结构)
- [安装与配置](#安装与配置)
- [使用指南](#使用指南)
- [策略开发](#策略开发)
- [数据管理](#数据管理)
- [回测引擎](#回测引擎)
- [实盘交易](#实盘交易)
- [绩效评估](#绩效评估)
- [风险管理](#风险管理)
- [常见问题](#常见问题)
- [引用建议](#引用建议)
- [许可证](#许可证)
- [联系方式](#联系方式)

---

## 项目概述

**English:**  
This is a comprehensive CTA (Commodity Trading Advisor) trading platform supporting futures and options trading in the Chinese market. The system features a complete trading infrastructure including data download, strategy management, order execution, position management, and performance evaluation. It supports both offline backtesting and online live trading with real-time data replay capabilities.

**中文:**  
这是一个综合性的 CTA（商品交易顾问）交易平台，支持中国期货和期权市场的交易。系统包含完整的交易基础设施，包括数据下载、策略管理、订单执行、仓位管理和绩效评估。支持离线回测和在线实盘交易，具备实时数据回放功能。

---

## 研究背景

**English:**  
Quantitative trading systems require robust infrastructure for data management, strategy execution, and risk control. The Chinese futures and options markets present unique challenges including:

1. **Market Microstructure**: Multiple exchanges (SHFE, CZCE, DCE, CFFEX, INE) with different trading rules
2. **Contract Rollover**: Monthly contract expiration requiring systematic rollover strategies
3. **Trading Hours**: Day and night trading sessions with different liquidity patterns
4. **Margin Requirements**: Dynamic margin rates based on contract and market conditions
5. **Data Quality**: Handling missing data, price limits, and settlement procedures

This platform addresses these challenges through a modular, event-driven architecture that separates data, strategy, execution, and risk management layers.

**中文:**  
量化交易系统需要强大的基础设施来进行数据管理、策略执行和风险控制。中国期货和期权市场面临独特的挑战：

1. **市场微观结构**: 多个交易所（上期所、郑商所、大商所、中金所、能源中心）具有不同的交易规则
2. **合约展期**: 月度合约到期需要系统的展期策略
3. **交易时间**: 日盘和夜盘交易时段具有不同的流动性模式
4. **保证金要求**: 基于合约和市场条件的动态保证金率
5. **数据质量**: 处理缺失数据、涨跌停板和结算程序

本平台通过模块化、事件驱动的架构来应对这些挑战，将数据、策略、执行和风险管理层分离。

---

## 研究目标

**English:**
1. Build a complete CTA trading platform with data, strategy, and execution modules
2. Implement robust backtesting engine with accurate simulation of trading mechanics
3. Support incremental data download and validation for multiple exchanges
4. Enable seamless transition from backtesting to live trading
5. Provide comprehensive performance metrics and risk management tools
6. Support multi-strategy portfolio management with capital allocation
7. Handle complex scenarios: multi-contract trading, rollover, night sessions

**中文:**
1. 构建包含数据、策略和执行模块的完整 CTA 交易平台
2. 实现稳健的回测引擎，准确模拟交易机制
3. 支持多交易所的增量数据下载和验证
4. 实现从回测到实盘的无缝切换
5. 提供全面的绩效指标和风险管理工具
6. 支持多策略组合管理和资金分配
7. 处理复杂场景：多合约交易、展期、夜盘交易

---

## 系统架构

```
┌─────────────────────────────────────────────────────────────┐
│                     Application Layer                       │
│                    应用层                                   │
│  ┌─────────────────┐  ┌─────────────────┐                   │
│  │  Backtesting    │  │  Live Trading   │                   │
│  │  回测模式       │  │  实盘模式       │                   │
│  └─────────────────┘  └─────────────────┘                   │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│                    Strategy Layer                           │
│                    策略层                                   │
│  ┌─────────────────────────────────────────────────────┐    │
│  │  Strategy Manager | 策略管理器                      │    │
│  │  • Strategy Loading (Python/Pyd)                    │    │
│  │  • Multi-Strategy Portfolio                         │    │
│  │  • Position Aggregation                             │    │
│  │  • Capital Allocation                               │    │
│  └─────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│                   Execution Layer                           │
│                   执行层                                    │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐       │
│  │ Order Mgmt   │  │ Position Mgmt│  │ Match Engine │       │
│  │ 订单管理     │  │ 仓位管理     │  │ 撮合引擎     │       │
│  └──────────────┘  └──────────────┘  └──────────────┘       │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│                     Data Layer                              │
│                     数据层                                  │
│  ┌─────────────────────────────────────────────────────┐    │
│  │  Market Data API | 市场数据 API                     │    │
│  │  • Real-time Quotes                                 │    │
│  │  • Historical Bars (1m, 5m, 1d)                     │    │
│  │  • Contract Information                             │    │
│  │  • Settlement Data                                  │    │
│  └─────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│                  Infrastructure Layer                       │
│                  基础设施层                                 │
│  • Database (SQLite) | 数据库                              │
│  • Log System | 日志系统                                    │
│  • Configuration | 配置管理                                 │
│  • Utilities (DateTime, Multi-processing)                  │
└─────────────────────────────────────────────────────────────┘
```

---

## 核心功能

**English:**
1. **Data Management**
   - Incremental download from multiple exchanges
   - Data validation and integrity checks
   - Multi-timeframe support (1-minute, 5-minute aggregated, daily)
   - Main contract identification and rollover handling

2. **Strategy Framework**
   - Base strategy class with standard interface
   - Event-driven callbacks (on_bar, on_order, on_trade, on_bod, on_eod)
   - Support for Python and compiled Pyd formats
   - Multi-strategy portfolio with capital allocation

3. **Backtesting Engine**
   - Accurate simulation of trading mechanics
   - Configurable slippage, fees, and margin rates
   - Volume-based execution constraints
   - Night session support

4. **Order Management**
   - Order lifecycle tracking (pending, filled, cancelled)
   - Position target calculation
   - Real-time risk monitoring
   - Maximum deal percentage constraints

5. **Position Management**
   - Real-time position tracking
   - PnL calculation (realized and unrealized)
   - Margin requirement monitoring
   - Contract multiplier adjustment

6. **Performance Evaluation**
   - Comprehensive metrics: Annualized return, volatility, Sharpe, Sortino, Calmar
   - Maximum drawdown analysis
   - Win rate and profit-loss ratio
   - Monthly and daily statistics

7. **Risk Management**
   - Real-time net value monitoring
   - Drawdown limits
   - Position limits per contract and portfolio
   - Trading hour restrictions

**中文:**
1. **数据管理**
   - 多交易所增量下载
   - 数据验证和完整性检查
   - 多时间框架支持（1 分钟、5 分钟聚合、日线）
   - 主力合约识别和展期处理

2. **策略框架**
   - 标准接口的策略基类
   - 事件驱动回调（on_bar、on_order、on_trade、on_bod、on_eod）
   - 支持 Python 和编译后的 Pyd 格式
   - 多策略组合与资金分配

3. **回测引擎**
   - 准确模拟交易机制
   - 可配置的滑点、手续费和保证金率
   - 基于成交量的执行约束
   - 夜盘支持

4. **订单管理**
   - 订单生命周期跟踪（待成交、已成交、已撤销）
   - 目标仓位计算
   - 实时风险监控
   - 最大成交比例限制

5. **仓位管理**
   - 实时仓位跟踪
   - 盈亏计算（已实现和未实现）
   - 保证金要求监控
   - 合约乘数调整

6. **绩效评估**
   - 综合指标：年化收益、波动率、夏普比率、索提诺比率、卡玛比率
   - 最大回撤分析
   - 胜率和盈亏比
   - 月度 and 日度统计

7. **风险管理**
   - 实时净值监控
   - 回撤限制
   - 单个合约和组合的仓位限制
   - 交易时段限制

---

## 方法论

### 1. 数据下载与处理

**English:**
- **Incremental Download**: Only fetch missing data based on existing records
- **Validation**: Check for gaps, duplicates, and data quality issues
- **Main Contract Identification**: Use open_interest to identify the most liquid contract
- **Rollover**: Automatic switching 60 days before expiration
- **Multi-Timeframe**: Aggregate 1-minute bars to 5-minute for strategy flexibility

**中文:**
- **增量下载**: 基于现有记录仅获取缺失数据
- **数据验证**: 检查缺口、重复和数据质量问题
- **主力合约识别**: 使用持仓量识别流动性最高的合约
- **自动展期**: 到期前 60 天自动切换
- **多时间框架**: 将 1 分钟 K 线聚合为 5 分钟用于策略灵活性

### 2. 策略框架

**English:**
```python
class StrategyBase:
    def init(self, api, user_name, strat_name, kwargs):
        """Initialize strategy with API and parameters"""
        
    def on_bar(self, symbol, timestamp, bar):
        """Called on each new bar"""
        
    def on_order(self, order):
        """Called on order status change"""
        
    def on_trade(self, trade):
        """Called on trade execution"""
        
    def calculate_target_position(self, symbol, timestamp, bar):
        """Calculate target position for given symbol"""
        return target_qty
```

**中文:**
```python
class StrategyBase:
    def init(self, api, user_name, strat_name, kwargs):
        """使用 API 和参数初始化策略"""
        
    def on_bar(self, symbol, timestamp, bar):
        """每个新 K 线时调用"""
        
    def on_order(self, order):
        """订单状态变化时调用"""
        
    def on_trade(self, trade):
        """成交执行时调用"""
        
    def calculate_target_position(self, symbol, timestamp, bar):
        """计算给定合约的目标仓位"""
        return target_qty
```

### 3. 仓位计算

**English:**
- **Position Target**: Calculated based on strategy signals and contract multiplier
- **Aggregation**: Multiple strategies' positions are aggregated with capital weights
- **Constraints**: Limited by maximum deal percentage and market volume
- **Rebalancing**: Periodic or signal-driven rebalancing

**中文:**
- **目标仓位**: 基于策略信号和合约乘数计算
- **聚合**: 多个策略的仓位按资金权重聚合
- **约束**: 受最大成交比例和市场成交量限制
- **再平衡**: 定期或信号驱动的再平衡

### 4. 绩效评估

**English:**
```python
# Key Performance Metrics
annualized_return = (final_net_value / initial_net_value) ** (252 / trading_days) - 1
annualized_volatility = daily_return_std * np.sqrt(252)
sharpe_ratio = (annualized_return - risk_free_rate) / annualized_volatility
max_drawdown = max((peak - trough) / peak for peak, trough in running_max)
calmar_ratio = annualized_return / max_drawdown
win_rate = winning_trades / total_trades
profit_loss_ratio = avg_winning_trade / avg_losing_trade
```

**中文:**
```python
# 关键绩效指标
年化收益率 = (最终净值 / 初始净值) ** (252 / 交易天数) - 1
年化波动率 = 日收益标准差 * np.sqrt(252)
夏普比率 = (年化收益率 - 无风险利率) / 年化波动率
最大回撤 = max((峰值 - 谷值) / 峰值 for 峰值，谷值 in 运行最大值)
卡玛比率 = 年化收益率 / 最大回撤
胜率 = 盈利交易数 / 总交易数
盈亏比 = 平均盈利 / 平均亏损
```

---

## 项目结构

```
mos/
│
├── README.md                              # 项目文档（本文件）
├── requirements.txt                       # Python 依赖包
├── setup.py                               # 安装配置脚本
├── 数据使用规则.md                        # 数据使用规则说明
├── 策略实现手册_test_sample_strategy.md   # 策略实现示例
├── 策略编写注意事项.txt                   # 策略编写指南
├── prompt.md                              # AI 辅助开发提示
│
├── cta_platform.py                        # 主平台入口脚本
│   • 策略加载与执行                       │
│   • 日志管理                             │
│   • 结果汇总                             │
│
├── cta_platform_api.py                    # 平台 API 封装
├── client_api.py                          # 客户端 API 接口
├── back_test.py                           # 回测入口脚本
│
├── strategy_base.py                       # 策略基类
│   • init()                               │
│   • on_bar()                             │
│   • on_order()                           │
│   • on_trade()                           │
│   • calculate_target_position()          │
│
├── strategy_manager.py                    # 策略管理器
│   • 策略加载（本地/数据库）              │
│   • 多策略管理                           │
│   • 仓位聚合                             │
│   • 资金分配                             │
│
├── order_manager.py                       # 订单管理器
├── position_manager.py                    # 仓位管理器
├── match_engine.py                        # 撮合引擎
│
├── market_data_type.py                    # 市场数据类型定义
├── md_data_struct.py                      # 市场数据结构
├── md_minute_bar_api.py                   # 分钟线 API
├── download_data.py                       # 数据下载脚本
├── download_data_parallelism.py           # 并行数据下载
├── mock_data_generator.py                 # 模拟数据生成
│
├── order.py                               # 订单数据结构
├── order_event.py                         # 订单事件
├── position.py                            # 仓位数据结构
├── protocol.py                            # 通信协议
│
├── account_manager.py                     # 账户管理器
├── dataset.py                             # 数据集管理
├── date_time_util.py                      # 日期时间工具
├── evaluating_indicator.py                # 评估指标计算
├── gate_test.py                           # 网关测试
├── importlib_local.py                     # 本地导入工具
├── multi_process_exec.py                  # 多进程执行
├── return_split.py                        # 收益拆分
│
├── strategy/                              # 策略目录
│   ├── config/                            # 策略配置文件
│   │   └── test/                          # 测试配置
│   │       ├── test_sample_strategy.json  # 测试策略配置
│   │       └── test_sample_strategy_apple.json
│   │
│   └── source_code/                       # 策略源代码
│       └── test/                          # 测试策略
│           ├── test_sample_strategy.py    # 多因子截面策略
│           ├── test_sample_strategy_apple.py
│           ├── test_sample_strategy_lite.py
│           ├── test_sample_strategy_origin.py
│           └── test_sample_strategy_杠杆.py
│
├── test_config.json                       # 主配置文件
├── test_config_apple.json                 # Apple 策略配置
├── test_config copy.json                  # 配置备份
│
├── test.py                                # 测试脚本
├── offline_test_sample_strategy.py        # 离线测试策略
├── offline_test_sample_strategy_old.py    # 旧版离线测试
└── offline_test_sample_strategy_无交易 but 健壮.py

# 运行时生成的目录
├── db/                                    # 数据库目录
│   └── user_data.db                       # 用户策略数据库
│
├── log/                                   # 日志目录
│   └── {strategy}_{begin_date}_{end_date}.log
│
├── result/                                # 回测结果目录
│   ├── net_value.csv                      # 净值曲线
│   ├── trade_order.csv                    # 交易明细
│   └── evaluating_indicator.txt           # 评估指标
│
└── data/                                  # 数据目录（下载的数据）
    └── {exchange}/{symbol}/{timeframe}/
```

---

## 安装与配置

### 前置条件

**English:**
- Python 3.8 or higher
- pip package manager
- Git (for cloning the repository)
- Database: SQLite (included with Python)

**中文:**
- Python 3.8 或更高版本
- pip 包管理器
- Git（用于克隆仓库）
- 数据库：SQLite（Python 自带）

### 安装步骤

**English:**
```bash
# 1. Clone the repository
git clone https://github.com/YichuanAlex/CTA_Trading_Platform.git

# 2. Navigate to project directory
cd mos

# 3. Create virtual environment (recommended)
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# 4. Install dependencies
pip install -r requirements.txt
```

**中文:**
```bash
# 1. 克隆仓库
git clone https://github.com/YichuanAlex/CTA_Trading_Platform.git

# 2. 进入项目目录
cd mos

# 3. 创建虚拟环境（推荐）
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# 4. 安装依赖
pip install -r requirements.txt
```

### 依赖包

**English:**
```txt
numpy==2.3.5             # Numerical computing | 数值计算
pandas==2.3.3            # Data manipulation | 数据处理
matplotlib==3.10.7       # Visualization | 可视化
scipy                    # Scientific computing | 科学计算
pyzmq==27.1.0            # ZeroMQ messaging | 消息队列
redis==7.1.0             # Cache and messaging | 缓存和消息
```

**中文:**
```txt
numpy==2.3.5             # 数值计算
pandas==2.3.3            # 数据处理
matplotlib==3.10.7       # 可视化
scipy                    # 科学计算
pyzmq==27.1.0            # ZeroMQ 消息队列
redis==7.1.0             # 缓存和消息
```

### 配置文件

**English:**
Create a `config.json` file with the following structure:

```json
{
  "server_addr": "tcp://localhost:5555",
  "user_name": "test_user",
  "user_passwd": "password123",
  "subscribe_symbol": ["rb.SHFE", "au.SHFE", "cu.SHFE"],
  "start_date": "20240101",
  "end_date": "20241231",
  "data_type": "1m",
  "init_money": 1000000,
  "riskless_rate": 0.03,
  "margin_rate": 0.10,
  "slippage_type": "fixed",
  "slippage_value": 1,
  "buy_fee_type": "ratio",
  "buy_fee_value": 0.0001,
  "sell_fee_type": "ratio",
  "sell_fee_value": 0.0001,
  "deal_type": "close",
  "max_deal_pct": 0.10,
  "is_check_market_volume": true,
  "night_trade": true
}
```

**中文:**
创建 `config.json` 文件，结构如下：

```json
{
  "server_addr": "tcp://localhost:5555",
  "user_name": "test_user",
  "user_passwd": "password123",
  "subscribe_symbol": ["rb.SHFE", "au.SHFE", "cu.SHFE"],
  "start_date": "20240101",
  "end_date": "20241231",
  "data_type": "1m",
  "init_money": 1000000,
  "riskless_rate": 0.03,
  "margin_rate": 0.10,
  "slippage_type": "fixed",
  "slippage_value": 1,
  "buy_fee_type": "ratio",
  "buy_fee_value": 0.0001,
  "sell_fee_type": "ratio",
  "sell_fee_value": 0.0001,
  "deal_type": "close",
  "max_deal_pct": 0.10,
  "is_check_market_volume": true,
  "night_trade": true
}
```

---

## 使用指南

### 快速开始

**English:**
```bash
# 1. Activate virtual environment
source venv/bin/activate  # Windows: venv\Scripts\activate

# 2. Download market data
python download_data.py

# 3. Run backtest with test strategy
python cta_platform.py test_config.json

# 4. View results in result/ directory
ls result/
cat result/evaluating_indicator.txt
```

**中文:**
```bash
# 1. 激活虚拟环境
source venv/bin/activate  # Windows: venv\Scripts\activate

# 2. 下载市场数据
python download_data.py

# 3. 使用测试策略运行回测
python cta_platform.py test_config.json

# 4. 在 result/ 目录查看结果
ls result/
cat result/evaluating_indicator.txt
```

### 回测模式

**English:**
```bash
# Run backtest with custom config
python back_test.py

# Or use the platform script
python cta_platform.py test_config.json
```

**中文:**
```bash
# 使用自定义配置运行回测
python back_test.py

# 或使用平台脚本
python cta_platform.py test_config.json
```

### 实盘模式

**English:**
```bash
# Switch to live trading mode
# Modify config.json: set is_live_mode = true
python cta_platform.py live_config.json
```

**中文:**
```bash
# 切换到实盘模式
# 修改 config.json: 设置 is_live_mode = true
python cta_platform.py live_config.json
```

### 多策略组合

**English:**
```python
# In strategy_manager.py, configure multiple strategies
dict_user_name = {
    "user1": ["strategy1.py", "strategy2.py"],
    "user2": ["strategy3.py"]
}

acct_balance_ratio = {
    "user1": 0.6,
    "user2": 0.4
}
```

**中文:**
```python
# 在 strategy_manager.py 中配置多个策略
dict_user_name = {
    "user1": ["strategy1.py", "strategy2.py"],
    "user2": ["strategy3.py"]
}

acct_balance_ratio = {
    "user1": 0.6,
    "user2": 0.4
}
```

---

## 策略开发

### 策略模板

**English:**
```python
from strategy_base import strategy_base

class MyStrategy(strategy_base):
    def __init__(self):
        super().__init__()
        self.params = {
            'lookback': 20,
            'threshold': 0.02
        }
    
    def init(self, api, user_name, strat_name, kwargs):
        super().init(api, user_name, strat_name, kwargs)
        # Initialize strategy parameters
        self.api = api
        self.user_name = user_name
        self.strat_name = strat_name
        
    def on_bar(self, symbol, timestamp, bar):
        """Called on each new bar"""
        # Update indicators
        pass
    
    def on_order(self, order):
        """Called on order status change"""
        print(f"Order update: {order}")
    
    def on_trade(self, trade):
        """Called on trade execution"""
        print(f"Trade executed: {trade}")
    
    def calculate_target_position(self, symbol, timestamp, bar):
        """Calculate target position"""
        # Your trading logic here
        if bar['close'] > bar['open'] * (1 + self.params['threshold']):
            target_qty = 10  # Long
        elif bar['close'] < bar['open'] * (1 - self.params['threshold']):
            target_qty = -10  # Short
        else:
            target_qty = 0  # Close
        
        return target_qty
```

**中文:**
```python
from strategy_base import strategy_base

class MyStrategy(strategy_base):
    def __init__(self):
        super().__init__()
        self.params = {
            'lookback': 20,
            'threshold': 0.02
        }
    
    def init(self, api, user_name, strat_name, kwargs):
        super().init(api, user_name, strat_name, kwargs)
        # 初始化策略参数
        self.api = api
        self.user_name = user_name
        self.strat_name = strat_name
        
    def on_bar(self, symbol, timestamp, bar):
        """每个新 K 线时调用"""
        # 更新指标
        pass
    
    def on_order(self, order):
        """订单状态变化时调用"""
        print(f"订单更新：{order}")
    
    def on_trade(self, trade):
        """成交执行时调用"""
        print(f"成交执行：{trade}")
    
    def calculate_target_position(self, symbol, timestamp, bar):
        """计算目标仓位"""
        # 在这里编写交易逻辑
        if bar['close'] > bar['open'] * (1 + self.params['threshold']):
            target_qty = 10  # 做多
        elif bar['close'] < bar['open'] * (1 - self.params['threshold']):
            target_qty = -10  # 做空
        else:
            target_qty = 0  # 平仓
        
        return target_qty
```

### 示例策略：多因子截面策略

**English:**
See [策略实现手册_test_sample_strategy.md](策略实现手册_test_sample_strategy.md) for a complete example of a multi-factor cross-sectional strategy using momentum, term structure, and open interest change factors.

**中文:**
完整的示例请参考 [策略实现手册_test_sample_strategy.md](策略实现手册_test_sample_strategy.md)，该策略使用动量、期限结构和持仓量变化三个因子进行截面选股。

---

## 数据管理

### 数据下载

**English:**
```bash
# Download all data
python download_data.py

# Download with parallelism
python download_data_parallelism.py

# Download specific symbols
python -c "from download_data import download_symbols; download_symbols(['rb.SHFE', 'au.SHFE'])"
```

**中文:**
```bash
# 下载所有数据
python download_data.py

# 并行下载
python download_data_parallelism.py

# 下载特定品种
python -c "from download_data import download_symbols; download_symbols(['rb.SHFE', 'au.SHFE'])"
```

### 数据规则

**English:**
- **Timeframe**: 1-minute bars (can be aggregated to 5-minute)
- **Main Contract**: Identified by highest open_interest
- **Rollover**: 60 days before expiration
- **Trading Hours**: Day session (9:00-15:00) and Night session (21:00-2:30)
- **Quality Checks**: Gap detection, duplicate removal, price limit validation

**中文:**
- **时间框架**: 1 分钟 K 线（可聚合为 5 分钟）
- **主力合约**: 由最高持仓量识别
- **展期**: 到期前 60 天
- **交易时间**: 日盘（9:00-15:00）和夜盘（21:00-2:30）
- **质量检查**: 缺口检测、重复删除、涨跌停板验证

---

## 回测引擎

### 回测配置

**English:**
```json
{
  "start_date": "20240101",
  "end_date": "20241231",
  "init_money": 1000000,
  "riskless_rate": 0.03,
  "margin_rate": 0.10,
  "slippage_type": "fixed",
  "slippage_value": 1,
  "buy_fee_type": "ratio",
  "buy_fee_value": 0.0001,
  "sell_fee_type": "ratio",
  "sell_fee_value": 0.0001,
  "deal_type": "close",
  "max_deal_pct": 0.10,
  "is_check_market_volume": true,
  "night_trade": true
}
```

**中文:**
```json
{
  "start_date": "20240101",
  "end_date": "20241231",
  "init_money": 1000000,
  "riskless_rate": 0.03,
  "margin_rate": 0.10,
  "slippage_type": "fixed",
  "slippage_value": 1,
  "buy_fee_type": "ratio",
  "buy_fee_value": 0.0001,
  "sell_fee_type": "ratio",
  "sell_fee_value": 0.0001,
  "deal_type": "close",
  "max_deal_pct": 0.10,
  "is_check_market_volume": true,
  "night_trade": true
}
```

### 回测执行

**English:**
```bash
# Run backtest
python cta_platform.py test_config.json

# The system will:
# 1. Load strategy from config
# 2. Initialize market data
# 3. Run event-driven simulation
# 4. Generate results in result/ directory
```

**中文:**
```bash
# 运行回测
python cta_platform.py test_config.json

# 系统将：
# 1. 从配置加载策略
# 2. 初始化市场数据
# 3. 运行事件驱动模拟
# 4. 在 result/ 目录生成结果
```

---

## 实盘交易

### 实盘配置

**English:**
```json
{
  "server_addr": "tcp://your-broker.com:5555",
  "user_name": "your_account",
  "user_passwd": "your_password",
  "is_live_mode": true,
  "night_trade": true,
  "max_deal_pct": 0.05
}
```

**中文:**
```json
{
  "server_addr": "tcp://your-broker.com:5555",
  "user_name": "your_account",
  "user_passwd": "your_password",
  "is_live_mode": true,
  "night_trade": true,
  "max_deal_pct": 0.05
}
```

### 实盘注意事项

**English:**
1. **Risk Control**: Set conservative position limits and drawdown thresholds
2. **Monitoring**: Real-time monitoring of net value and positions
3. **Emergency Stop**: Implement circuit breakers for abnormal market conditions
4. **Logging**: Comprehensive logging for audit and debugging
5. **Backup**: Redundant systems and data backup

**中文:**
1. **风险控制**: 设置保守的仓位限制和回撤阈值
2. **监控**: 实时监控净值和仓位
3. **紧急停止**: 为异常市场情况实施断路器
4. **日志**: 全面的日志用于审计和调试
5. **备份**: 冗余系统和数据备份

---

## 绩效评估

### 关键指标

**English:**

| 指标 | 公式 | 说明 |
|------|------|------|
| **年化收益率** | `(FV/IV)^(252/n) - 1` | 年化后的总收益率 |
| **年化波动率** | `σ_daily × √252` | 收益率的年化标准差 |
| **夏普比率** | `(R_p - R_f) / σ_p` | 风险调整后的收益 |
| **最大回撤** | `max(1 - C_t/max(C_s))` | 最大累计损失 |
| **卡玛比率** | `年化收益 / 最大回撤` | 收益回撤比 |
| **胜率** | `盈利交易数 / 总交易数` | 盈利交易比例 |
| **盈亏比** | `平均盈利 / 平均亏损` | 盈利与亏损的比率 |

**中文:**

| 指标 | 公式 | 说明 |
|------|------|------|
| **年化收益率** | `(FV/IV)^(252/n) - 1` | 年化后的总收益率 |
| **年化波动率** | `σ_daily × √252` | 收益率的年化标准差 |
| **夏普比率** | `(R_p - R_f) / σ_p` | 风险调整后的收益 |
| **最大回撤** | `max(1 - C_t/max(C_s))` | 最大累计损失 |
| **卡玛比率** | `年化收益 / 最大回撤` | 收益回撤比 |
| **胜率** | `盈利交易数 / 总交易数` | 盈利交易比例 |
| **盈亏比** | `平均盈利 / 平均亏损` | 盈利与亏损的比率 |

### 结果查看

**English:**
```bash
# View performance metrics
cat result/evaluating_indicator.txt

# View net value curve
cat result/net_value.csv

# View trade details
cat result/trade_order.csv
```

**中文:**
```bash
# 查看绩效指标
cat result/evaluating_indicator.txt

# 查看净值曲线
cat result/net_value.csv

# 查看交易明细
cat result/trade_order.csv
```

---

## 风险管理

### 风险控制措施

**English:**
1. **Position Limits**: Maximum position per contract and total portfolio
2. **Drawdown Control**: Stop trading if drawdown exceeds threshold
3. **Volume Constraints**: Limit execution to percentage of market volume
4. **Trading Hours**: Restrict trading to liquid periods
5. **Margin Monitoring**: Real-time margin requirement tracking
6. **Emergency Stop**: Manual and automatic stop mechanisms

**中文:**
1. **仓位限制**: 单个合约和总组合的最大仓位
2. **回撤控制**: 回撤超过阈值时停止交易
3. **成交量约束**: 限制执行为市场成交量的百分比
4. **交易时间**: 限制交易在流动性高的时段
5. **保证金监控**: 实时保证金要求跟踪
6. **紧急停止**: 手动和自动停止机制

---

## 常见问题

### Q1: 数据下载失败

**English:**
Check network connection and API credentials. Verify that the server address is correct in the configuration file.

**中文:**
检查网络连接和 API 凭证。验证配置文件中的服务器地址是否正确。

### Q2: 回测结果为空

**English:**
Ensure that the strategy is generating signals and that the date range has valid market data. Check logs for errors.

**中文:**
确保策略正在生成信号，并且日期范围内有有效的市场数据。检查日志是否有错误。

### Q3: 实盘交易不执行

**English:**
Verify account credentials, available balance, and trading permissions. Check if the market is open and if trading hours are correctly configured.

**中文:**
验证账户凭证、可用余额和交易权限。检查市场是否开市以及交易时间是否正确配置。

### Q4: 策略加载失败

**English:**
Ensure the strategy file is in the correct location and follows the naming convention. Check for syntax errors in the strategy code.

**中文:**
确保策略文件在正确的位置并遵循命名约定。检查策略代码中的语法错误。

---

## 引用建议

**English:**
```bibtex
@misc{jiang2026cta,
  title={CTA Trading Platform: A Comprehensive System for Futures and Options Trading},
  author={Jiang, Zixi},
  year={2026},
  howpublished={\url{https://github.com/YichuanAlex/CTA_Trading_Platform}},
  note={GitHub Repository}
}
```

**中文:**
```
江子曦。(2026). CTA 交易平台：期货和期权交易的综合系统 [GitHub 仓库]. 
https://github.com/YichuanAlex/CTA_Trading_Platform
```

---

## 许可证

**English:**
This project is licensed under the MIT License. You are free to use, modify, and distribute this work for academic and non-commercial purposes. Please cite the original author when using this research.

**中文:**
本项目采用 MIT 许可证。您可以自由地使用、修改和分发本作品用于学术和非商业目的。使用本研究时请注明原作者。

---

## 联系方式

**English:**
For questions, suggestions, or collaborations, please contact:

- **Author**: Zixi Jiang (YichuanAlex)
- **Email**: jiangzixi1527435659@gmail.com
- **GitHub**: https://github.com/YichuanAlex
- **Location**: Shanghai, Shanghai, China

**中文:**
如有问题、建议或合作意向，请联系：

- **作者**: 江子曦 (YichuanAlex)
- **邮箱**: jiangzixi1527435659@gmail.com
- **GitHub**: https://github.com/YichuanAlex
- **地点**: 中国上海

---

<div align="center">

**📈 量化交易 · CTA 策略 · 期货期权 📊**

**Quantitative Trading · CTA Strategy · Futures & Options**

[![System Reliability](https://img.shields.io/badge/Reliability-Production%20Ready-blue.svg)]()
[![Backtesting](https://img.shields.io/badge/Backtesting-Event--Driven-orange.svg)]()

**感谢使用本交易平台！**

**Thank you for using this trading platform!**

</div>
