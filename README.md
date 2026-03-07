# DEX_total

覆盖 **12 个加密货币永续合约交易所**的多交易所数据采集与跨交易所分析系统。通过置信度评分生成套利信号 — **仅生成信号，不执行交易**。

## 系统架构

```
                    ┌─────────────┐
                    │   main.py   │  编排器 (Orchestrator)
                    │   编排器     │  - 7 个并发采集/分析循环
                    └──────┬──────┘  - 生命周期管理
                           │         - 信号路由
            ┌──────────────┼──────────────┐
            ▼              ▼              ▼
    ┌──────────────┐ ┌──────────┐ ┌──────────────┐
    │  采集器       │ │  分析层   │ │   监控层      │
    │  (12 交易所)  │ │          │ │              │
    │              │ │ 价差矩阵  │ │ 终端仪表盘    │
    │ Tier 1: CEX  │ │ 相关性   │ │ Telegram     │
    │ Tier 2: DEX  │ │ 领先滞后  │ │ CSV 日志     │
    │ Tier 3: Beta │ │ 回测引擎  │ │              │
    │ Tier 4: Stub │ │          │ │              │
    └──────┬───────┘ └────┬─────┘ └──────────────┘
           │              │
           ▼              ▼
    ┌──────────────┐ ┌──────────────┐
    │  CSV 存储     │ │   交易信号    │
    │ data/YYYYMMDD │ │  (不执行交易)  │
    └──────────────┘ └──────────────┘
```

### 数据流

```
BBO 循环 (1s)       ─→ SpreadAnalyzer.update_bbo()
                       ─→ compute_matrix() → NxN 价差矩阵
                       ─→ SignalGenerator.evaluate() → 置信度评分
                       ─→ CSV + Telegram

交易记录循环 (5s)    ─→ get_recent_trades() → CSV

资金费率循环 (60s)   ─→ get_funding_rate() → SignalGenerator + CSV

持仓量循环 (30s)     ─→ get_open_interest() → SignalGenerator + CSV

分析循环 (120s)      ─→ CorrelationAnalyzer.compute_correlation_matrix()
                       ─→ LeadLagAnalyzer.identify_leaders()
                       ─→ SignalGenerator.set_leader()
```

## 快速开始

### 1. 安装依赖
```bash
pip install -r requirements.txt
```

### 2. 配置 API 密钥
```bash
cp .env.example .env
# 编辑 .env 填入你的 API 密钥（可选 — 公开数据不需要密钥即可运行）
```

### 3. 运行
```bash
python main.py
```

## 交易所覆盖

| 等级 | 交易所 | 类型 | 连接方式 | 数据源 | 状态 |
|------|--------|------|---------|--------|------|
| 1 | Binance | CEX | ccxt 统一接口 | 订单簿, 交易, 资金费率, 持仓量, 清算 | 生产就绪 |
| 1 | OKX | CEX | ccxt 统一接口 | 订单簿, 交易, 资金费率, 持仓量 | 生产就绪 |
| 1 | Bitget | CEX | ccxt 统一接口 | 订单簿, 交易, 资金费率, 持仓量 | 生产就绪 |
| 2 | Lighter | DEX | REST + WS | 订单簿, 交易, 资金费率 | 生产就绪 |
| 2 | GRVT | DEX | REST (全部 POST) | 订单簿, 交易, 资金费率 | 生产就绪 |
| 2 | Hyperliquid | DEX | REST + WS | 订单簿, 交易, 资金费率, 持仓量 | 生产就绪 |
| 2 | Nado | DEX | REST V1/V2 + WS | 订单簿, 交易, 资金费率, 持仓量 | 生产就绪 |
| 3 | Paradex | DEX | 仅 REST | 订单簿, 交易, 资金费率 | Beta |
| 3 | Aster | DEX | 仅 REST | 订单簿, 交易, 资金费率 | Beta |
| 3 | EdgeX | DEX | 仅 REST | 订单簿, 交易 | Beta |
| 3 | Variational | DEX | 仅 REST (公开后端) | BBO (无深度), 标记价格 | Beta |
| 3 | 01.xyz | DEX | 仅 REST | 订单簿 | Beta |

### 分层设计

- **Tier 1 (CEX)**: 通过 ccxt 统一 API 获取完整数据。`CcxtCollector` 基类实现约 70% 代码复用。
- **Tier 2 (DEX)**: 自定义 REST + WebSocket。WS 提供实时 BBO，内置指数退避重连机制。包括 Lighter、GRVT、Hyperliquid 和 Nado。
- **Tier 3 (Beta)**: 仅 REST，优雅降级（404 → NotImplementedError，不会崩溃）。包括 Paradex、Aster、EdgeX、Variational 和 01.xyz。

## 配置说明

### config/exchanges.yaml
控制启用哪些交易所及其连接参数。API 密钥通过环境变量引用（永远不会硬编码）。

```yaml
exchanges:
  binance:
    tier: 1
    type: ccxt
    enabled: true
    api_key_env: BINANCE_API_KEY        # 从 .env 文件解析
    api_secret_env: BINANCE_API_SECRET
  lighter:
    tier: 2
    type: custom
    enabled: true
    rest_url: https://mainnet.lighter.xyz
    ws_url: wss://mainnet.lighter.xyz/ws
```

### config/collection.yaml
控制数据采集间隔、订单簿深度和每个交易所的交易对映射。

```yaml
intervals:
  orderbook: 1.0      # 每 1 秒采集一次 BBO 快照
  trades: 5.0          # 每 5 秒采集最近交易
  funding: 60.0        # 每 60 秒采集资金费率
  open_interest: 30.0  # 每 30 秒采集持仓量

symbol_map:
  binance:
    BTC-PERP: BTC/USDT:USDT
  lighter:
    BTC-PERP: BTCUSDC
  hyperliquid:
    BTC-PERP: BTC
```

### config/strategy.yaml
控制信号生成的权重和阈值。

```yaml
weights:
  spread_magnitude: 0.30     # 价差幅度
  spread_persistence: 0.20   # 价差持续时间
  volume_confirmation: 0.15  # 成交量确认
  funding_alignment: 0.10    # 资金费率对齐
  lead_lag_signal: 0.15      # 领先-滞后信号
  oi_divergence: 0.10        # 持仓量背离

signal:
  min_confidence: 0.3    # 仅输出高于此阈值的信号
  cooldown_s: 5.0         # 信号间最小冷却时间（秒）
  warmup_ticks: 30        # 预热期 tick 数（预热期内不生成信号）
```

## 数据输出

### CSV 目录结构
```
data/
  20260307/
    bbo_binance.csv          # BBO 快照（1 秒间隔）
    bbo_okx.csv
    bbo_lighter.csv
    trades_binance.csv       # 最近交易记录
    funding_binance.csv      # 资金费率
    oi_binance.csv           # 持仓量
    spreads_matrix.csv       # 跨交易所价差矩阵
    signals_signals.csv      # 生成的信号（含置信度）
```

### CSV 数据格式

**BBO 数据** (`bbo_{exchange}.csv`)
| 字段 | 类型 | 说明 |
|------|------|------|
| timestamp | ISO 8601 | UTC 时间戳 |
| exchange | string | 交易所名称 |
| symbol | string | 交易对 |
| bid | decimal | 最优买价 |
| ask | decimal | 最优卖价 |
| bid_size | decimal | 最优买量 |
| ask_size | decimal | 最优卖量 |

**交易记录** (`trades_{exchange}.csv`)
| 字段 | 类型 | 说明 |
|------|------|------|
| timestamp | ISO 8601 | UTC 时间戳 |
| exchange | string | 交易所名称 |
| symbol | string | 交易对 |
| side | string | 买/卖方向 |
| price | decimal | 成交价格 |
| size | decimal | 成交数量 |
| trade_id | string | 交易所交易 ID |

**信号数据** (`signals_signals.csv`)
| 字段 | 类型 | 说明 |
|------|------|------|
| timestamp | ISO 8601 | UTC 时间戳 |
| buy_exchange | string | 买方交易所 |
| sell_exchange | string | 卖方交易所 |
| symbol | string | 交易对 |
| direction | string | 如 buy@binance_sell@okx |
| confidence | float | 0-1 置信度评分 |
| spread_bps | float | 净价差（基点） |
| components | string | 各因子得分 JSON |

## 分析工具

### 价差分析器 (`analysis/spread_analyzer.py`)
每次 BBO tick 时计算 NxN 跨交易所价差矩阵。

```
对于每个 (买方交易所, 卖方交易所) 组合：
  原始价差   = 卖方最优买价 - 买方最优卖价
  价差基点   = 原始价差 / 中间价 * 10000
  手续费成本 = 2 * 预估单边费率（往返）
  自然价差   = avg(买方自然价差, 卖方自然价差)
  净价差     = 价差基点 - 手续费成本 - 自然价差
```

核心特性：
- 过期数据过滤（>30 秒 → 排除）
- 每个交易所的滚动自然价差计算（可配置窗口）
- 每对交易所的价差持续性追踪（用于信号评分）

### 相关性分析器 (`analysis/correlation.py`)
使用 numpy.corrcoef 在滚动窗口内计算交易所间价格的 Pearson 相关系数。识别哪些交易所价格同步移动，哪些出现背离。

```python
from analysis.correlation import CorrelationAnalyzer
corr = CorrelationAnalyzer(window_size=300)
# ... 持续喂入价格数据 ...
matrix = corr.compute_correlation_matrix()
# → {('binance', 'okx'): 0.9987, ('lighter', 'grvt'): 0.9912, ...}
```

### 领先-滞后分析器 (`analysis/lead_lag.py`)
使用 scipy 互相关检测交易所间的领先-追随关系。识别哪个交易所最先变动（通常是 Binance），以及滞后的毫秒数。

```python
from analysis.lead_lag import LeadLagAnalyzer
ll = LeadLagAnalyzer(window_s=300)
# ... 持续喂入价格数据 ...
leaders = ll.identify_leaders()
# → [{'exchange': 'binance', 'lead_count': 5, 'avg_lag_ms': 150.0}, ...]
```

编排器每 120 秒运行一次此分析，并将识别出的领先交易所反馈给 SignalGenerator。

### 回测引擎 (`analysis/backtest_engine.py`)
将历史 BBO 数据通过真实管线重放（SpreadAnalyzer → SignalGenerator）。

```python
from analysis.backtest_engine import BacktestEngine
from analysis.spread_analyzer import SpreadAnalyzer
from strategy.signal_generator import SignalGenerator

engine = BacktestEngine(data_dir="data")
data = engine.load_bbo_data("20260307", ["binance", "okx", "lighter"])

# 方法一：自定义策略函数
result = engine.replay(data, my_strategy_fn)

# 方法二：真实管线回放（推荐 — 与实盘路径完全一致）
sa = SpreadAnalyzer(fee_estimate_bps=5.0)
sg = SignalGenerator(min_confidence=0.3)
result = engine.replay_with_pipeline(data, sa, sg, symbol="BTC-PERP")

print(engine.generate_report(result["stats"]))
```

`replay_with_pipeline()` 修复了关键的 P0 问题：
- 注入历史时间戳（避免 time.time() 坍缩导致所有 tick 同一时间）
- 按时间间隔分批（防止前瞻偏差 look-ahead bias）
- 走与实盘完全相同的代码路径

## 信号生成

信号采用 **加权多因子置信度评分**（0-1），而非硬门控过滤。这避免了二元过滤导致的"0 笔交易"问题。

| 因子 | 权重 | 范围 | 说明 |
|------|------|------|------|
| 价差幅度 | 30% | 0bps→0, 20+bps→1.0 | 净价差的大小 |
| 价差持续性 | 20% | 10 秒窗口内 0-1 | 价差持续存在的时间 |
| 成交量确认 | 15% | 0.5（占位符） | 成交量是否支持方向 |
| 资金费率对齐 | 10% | 费率差 → 0-1 | 资金费率是否确认套利方向 |
| 领先-滞后信号 | 15% | 基于领先交易所 0.5/0.8 | 领先交易所是否先行变动 |
| 持仓量背离 | 10% | OI 不平衡 → 0.5-1.0 | 持仓量是否出现跨交易所背离 |

置信度高于 `min_confidence`（默认 0.3）的信号会输出到 CSV 和 Telegram。

**重要提示**：本系统仅生成信号。交易执行由各交易所专属 bot 负责（如 grvt_lighter），需遵循 21 条安全规则。

## 监控

### 终端仪表盘
基于 Rich 的实时终端仪表盘，显示：
- 交易所连接状态（在线/离线/WS）及等级分类
- 所有交易所的实时 BBO
- 按净基点排序的最优价差
- 最近的信号及置信度评分
- 系统统计（运行时间、tick 计数、信号计数）

### Telegram 告警
通过 `TELEGRAM_BOT_TOKEN` 和 `TELEGRAM_CHAT_ID` 环境变量配置。
- 启动/停止通知（附交易所列表）
- 信号告警（附价差和置信度详情）
- 定期心跳（每 5 分钟）
- 错误/警告通知
- WS 数据过期检测告警

## 安全规则

源自生产环境套利交易经验的关键规则：

1. **API 失败必须抛异常** — 永远不返回 0 或默认值
2. **全程 Decimal(str(value))** — 价格/数量永远不能用 float 污染
3. **WS 数据过期检测** — 30 秒阈值，超时自动降级为 REST
4. **认证失败快速失败** — 立即 raise，不静默继续
5. **永远不对仓位 size 用 abs()** — 正负号 = 多空方向信息
6. **disconnect() 清理所有资源** — 关闭全部 aiohttp session 和 WS 连接

## Rust 迁移准备

代码库已为未来将热路径模块迁移到 Rust 做好结构准备：

### 数据类型 (`models/data_types.py`)
所有热路径数据结构使用 `@dataclass`，字段扁平化：
- `BBOState`、`SpreadEntry`、`SignalOutput`、`TradeEntry`、`FundingSnapshot`、`OISnapshot`
- 每个 dataclass 直接映射到 Rust struct
- 通过 `dataclasses.asdict()` 可序列化用于 IPC

### IPC 桥接 (`ipc/bridge.py`)
抽象 `IPCBridge` 接口，三种实现：
- `InProcessBridge` — 当前默认，零开销，进程内直接调用
- `UDSBridge` — Unix Domain Socket 占位（用于 Rust 进程通信）
- `SharedMemoryBridge` — 共享内存占位（用于零拷贝高频数据传输）

### 独立 WS 解析器 (`collectors/ws_parsers.py`)
WS 消息解析提取为纯函数（无 `self` 依赖，可直接移植到 Rust）：
- `parse_lighter_orderbook(msg)` → `(best_bid, best_ask)`
- `parse_hyperliquid_l2book(msg)` → `(best_bid, best_ask)`
- `parse_generic_orderbook(msg)` → `(best_bid, best_ask)`
- `parse_nado_bbo(msg)` → `(best_bid, best_ask)`

对应 Rust 函数签名：`fn parse_lighter_orderbook(msg: &Value) -> Option<(Decimal, Decimal)>`

## 项目结构

```
DEX_total/
├── config/                  # YAML 配置 + 加载器
│   ├── exchanges.yaml       # 交易所配置（API 密钥通过环境变量引用）
│   ├── collection.yaml      # 采集间隔、深度、交易对映射
│   ├── strategy.yaml        # 信号权重和阈值
│   └── loader.py            # YAML 加载 + 环境变量解析
├── collectors/              # 12 个交易所采集器
│   ├── base_collector.py    # 抽象基类 (ABC)
│   ├── ccxt_collector.py    # CEX 统一基类 (Binance/OKX/Bitget)
│   ├── binance_collector.py # Binance 特有（清算数据、标记价格）
│   ├── okx_collector.py     # OKX 特有（passphrase）
│   ├── bitget_collector.py  # Bitget 特有（产品类型）
│   ├── lighter_collector.py # Lighter DEX — REST + WS
│   ├── grvt_collector.py    # GRVT DEX — 仅 REST（全部 POST）
│   ├── hyperliquid_collector.py  # Hyperliquid — REST + WS
│   ├── paradex_collector.py # Paradex — REST (Tier 3)
│   ├── aster_collector.py   # Aster — REST (Tier 3, beta)
│   ├── edgex_collector.py   # EdgeX — REST (Tier 3, beta)
│   ├── variational_collector.py  # Variational — REST (Tier 3, 公开后端)
│   ├── nado_collector.py    # Nado — REST V1/V2 + WS (Tier 2)
│   ├── o1_collector.py      # 01.xyz — REST (Tier 3)
│   ├── ws_parsers.py        # 独立 WS 解析函数（Rust 就绪）
│   └── stubs/               # 旧占位符（已废弃，仅供参考）
│       ├── variational_stub.py  # → 已由 variational_collector.py 替代
│       ├── nado_stub.py         # → 已由 nado_collector.py 替代
│       └── o1_stub.py           # → 已由 o1_collector.py 替代
├── analysis/                # 分析工具
│   ├── spread_analyzer.py   # NxN 跨交易所价差矩阵
│   ├── correlation.py       # 滚动价格相关性 (numpy)
│   ├── lead_lag.py          # 领先-追随检测 (scipy)
│   └── backtest_engine.py   # 历史 BBO 回放
├── strategy/                # 信号生成
│   ├── signal_generator.py  # 多因子置信度评分
│   └── execution_engine.py  # 占位 — 不执行实际交易
├── models/                  # Rust 迁移就绪的数据类型
│   └── data_types.py        # 所有热路径结构的 @dataclass
├── ipc/                     # 进程间通信桥接
│   └── bridge.py            # InProcess / UDS / SharedMemory 三种实现
├── monitoring/              # 仪表盘 + 告警
│   ├── dashboard.py         # Rich 终端实时 UI
│   ├── alerts.py            # Telegram 通知（6 种类型）
│   └── data_logger.py       # CSV 写入（按日轮转、flush 安全）
├── tests/                   # 测试套件
│   ├── test_base_collector.py
│   ├── test_spread_analyzer.py
│   └── test_ccxt_collector.py
├── data/                    # CSV 输出（自动创建、按日轮转）
├── logs/                    # 日志文件（自动创建）
├── main.py                  # 入口 — 编排器
├── requirements.txt         # Python 依赖
├── .env.example             # API 密钥模板
├── .gitignore
└── CLAUDE.md                # Claude Code 项目指令
```

**总量**: 40 个 Python 文件，约 6,500 行代码

## 开发指南

### 运行测试
```bash
pytest tests/ -v
```

### 语法验证（全部文件）
```bash
python -c "
import ast, os
for r, d, fs in os.walk('.'):
    for f in fs:
        if f.endswith('.py'):
            ast.parse(open(os.path.join(r,f), encoding='utf-8').read())
print('All files valid')
"
```

### 添加新交易所
1. 创建 `collectors/新交易所_collector.py`
2. 继承 `BaseCollector`
3. 实现 `connect()`、`disconnect()`、`get_orderbook()`、`get_ticker()`
4. 可选实现 `get_recent_trades()`、`get_funding_rate()`、`get_open_interest()`
5. 在 `config/exchanges.yaml` 添加配置
6. 在 `config/collection.yaml` 添加交易对映射
7. 在 `main.py` 的 `COLLECTOR_MAP` 中添加工厂 lambda
8. 运行 `ast.parse()` 验证通过

### 存储空间估算
- BBO (1 秒/次): 约 5 KB/交易所/小时 → 约 120 KB/天/交易所
- 12 个活跃交易所 + 交易记录 + 信号: 约 140 MB/天
- 月度: 约 3.2 GB（CSV 完全可以胜任）

## 设计决策

| 决策 | 原因 |
|------|------|
| 信号与执行分离 | 每个交易所有独特的安全规则（21 条黄金规则）。通用执行引擎是危险的。 |
| 置信度评分而非硬门控 | 二元过滤会导致"0 笔交易"问题。加权评分可调可控。 |
| CSV 存储 | 简单、可 grep、pandas 直接读取。研究系统不需要数据库开销。 |
| ccxt 统一 CEX 基座 | Binance/OKX/Bitget 代码复用约 70%。只覆写交易所特有差异。 |
| 12 交易所全部实现 | Nado (Tier 2 REST+WS)、Variational 和 01.xyz (Tier 3 REST) 均已实现完整采集器。旧 stub 文件保留仅供参考。 |
| @dataclass 数据类型 | 扁平 FFI 友好的布局。可直接映射为 Rust struct，为未来迁移做准备。 |
