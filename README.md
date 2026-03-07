# DEX_total

Multi-exchange data collection and cross-exchange analysis system covering **12 cryptocurrency perpetual futures exchanges**. Generates arbitrage signals with confidence scoring — **signal only, no execution**.

## Architecture

```
                    ┌─────────────┐
                    │   main.py   │  Orchestrator
                    │ Orchestrator│  - 7 concurrent loops
                    └──────┬──────┘  - Lifecycle management
                           │         - Signal routing
            ┌──────────────┼──────────────┐
            ▼              ▼              ▼
    ┌──────────────┐ ┌──────────┐ ┌──────────────┐
    │  Collectors   │ │ Analysis │ │  Monitoring   │
    │  (12 exch.)   │ │          │ │              │
    │              │ │ Spread   │ │ Dashboard    │
    │ Tier 1: CEX  │ │ Correl.  │ │ Telegram     │
    │ Tier 2: DEX  │ │ Lead-Lag │ │ DataLogger   │
    │ Tier 3: Beta │ │ Backtest │ │              │
    │ Tier 4: Stub │ │          │ │              │
    └──────┬───────┘ └────┬─────┘ └──────────────┘
           │              │
           ▼              ▼
    ┌──────────────┐ ┌──────────────┐
    │  CSV Storage  │ │   Signals    │
    │ data/YYYYMMDD │ │ (no execut.) │
    └──────────────┘ └──────────────┘
```

### Data Flow

```
BBO Loop (1s)    ─→ SpreadAnalyzer.update_bbo()
                    ─→ compute_matrix() → NxN spread matrix
                    ─→ SignalGenerator.evaluate() → confidence scores
                    ─→ CSV + Telegram

Trades Loop (5s) ─→ get_recent_trades() → CSV

Funding Loop (60s) ─→ get_funding_rate() → SignalGenerator + CSV

OI Loop (30s)    ─→ get_open_interest() → SignalGenerator + CSV

Analysis (120s)  ─→ CorrelationAnalyzer.compute_correlation_matrix()
                    ─→ LeadLagAnalyzer.identify_leaders()
                    ─→ SignalGenerator.set_leader()
```

## Quick Start

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Configure API Keys
```bash
cp .env.example .env
# Edit .env with your API keys (optional — public data works without keys)
```

### 3. Run
```bash
python main.py
```

## Exchange Coverage

| Tier | Exchange | Type | Connection | Data Sources | Status |
|------|----------|------|-----------|-------------|--------|
| 1 | Binance | CEX | ccxt unified | OB, Trades, Funding, OI, Liquidations | Production |
| 1 | OKX | CEX | ccxt unified | OB, Trades, Funding, OI | Production |
| 1 | Bitget | CEX | ccxt unified | OB, Trades, Funding, OI | Production |
| 2 | Lighter | DEX | REST + WS | OB, Trades, Funding | Production |
| 2 | GRVT | DEX | REST (all POST) | OB, Trades, Funding | Production |
| 2 | Hyperliquid | DEX | REST + WS | OB, Trades, Funding, OI | Production |
| 3 | Paradex | DEX | REST only | OB, Trades, Funding | Beta |
| 3 | Aster | DEX | REST only | OB, Trades, Funding | Beta |
| 3 | EdgeX | DEX | REST only | OB, Trades | Beta |
| 4 | Variational | DEX | — | — | Stub (no API) |
| 4 | Nado | DEX | — | — | Stub (no SDK) |
| 4 | 01.xyz | DEX | — | — | Stub (migrating) |

### Tier Design

- **Tier 1 (CEX)**: Full data via ccxt unified API. ~70% code reuse through `CcxtCollector` base class.
- **Tier 2 (DEX)**: Custom REST + WebSocket. WS provides real-time BBO with exponential backoff reconnect.
- **Tier 3 (Beta)**: REST only with graceful degradation (404 → NotImplementedError, not crash).
- **Tier 4 (Stub)**: Placeholder — always unhealthy, all methods raise NotImplementedError.

## Configuration

### config/exchanges.yaml
Controls which exchanges are enabled and their connection parameters.
API keys are referenced via environment variables (never hardcoded).

```yaml
exchanges:
  binance:
    tier: 1
    type: ccxt
    enabled: true
    api_key_env: BINANCE_API_KEY        # resolved from .env
    api_secret_env: BINANCE_API_SECRET
  lighter:
    tier: 2
    type: custom
    enabled: true
    rest_url: https://mainnet.lighter.xyz
    ws_url: wss://mainnet.lighter.xyz/ws
```

### config/collection.yaml
Controls data collection intervals, orderbook depth, and symbol mappings per exchange.

```yaml
intervals:
  orderbook: 1.0      # BBO snapshot every 1s
  trades: 5.0          # recent trades every 5s
  funding: 60.0        # funding rate every 60s
  open_interest: 30.0  # OI every 30s

symbol_map:
  binance:
    BTC-PERP: BTC/USDT:USDT
  lighter:
    BTC-PERP: BTCUSDC
  hyperliquid:
    BTC-PERP: BTC
```

### config/strategy.yaml
Controls signal generation weights and thresholds.

```yaml
weights:
  spread_magnitude: 0.30
  spread_persistence: 0.20
  volume_confirmation: 0.15
  funding_alignment: 0.10
  lead_lag_signal: 0.15
  oi_divergence: 0.10

signal:
  min_confidence: 0.3    # only emit signals above this threshold
  cooldown_s: 5.0         # minimum seconds between signals
  warmup_ticks: 30        # ticks before generating signals
```

## Data Output

### CSV Structure
```
data/
  20260307/
    bbo_binance.csv          # BBO snapshots (1s intervals)
    bbo_okx.csv
    bbo_lighter.csv
    trades_binance.csv       # Recent trades
    funding_binance.csv      # Funding rates
    oi_binance.csv           # Open interest
    spreads_matrix.csv       # Cross-exchange spread matrix
    signals_signals.csv      # Generated signals with confidence
```

### CSV Schemas

**BBO** (`bbo_{exchange}.csv`)
| Column | Type | Description |
|--------|------|-------------|
| timestamp | ISO 8601 | UTC timestamp |
| exchange | string | Exchange name |
| symbol | string | Trading pair |
| bid | decimal | Best bid price |
| ask | decimal | Best ask price |
| bid_size | decimal | Best bid size |
| ask_size | decimal | Best ask size |

**Trades** (`trades_{exchange}.csv`)
| Column | Type | Description |
|--------|------|-------------|
| timestamp | ISO 8601 | UTC timestamp |
| exchange | string | Exchange name |
| symbol | string | Trading pair |
| side | string | buy/sell |
| price | decimal | Trade price |
| size | decimal | Trade size |
| trade_id | string | Exchange trade ID |

**Signals** (`signals_signals.csv`)
| Column | Type | Description |
|--------|------|-------------|
| timestamp | ISO 8601 | UTC timestamp |
| buy_exchange | string | Buy side exchange |
| sell_exchange | string | Sell side exchange |
| symbol | string | Trading pair |
| direction | string | e.g. buy@binance_sell@okx |
| confidence | float | 0-1 confidence score |
| spread_bps | float | Net spread in basis points |
| components | string | JSON of component scores |

## Analysis Tools

### Spread Analyzer (`analysis/spread_analyzer.py`)
Computes NxN cross-exchange spread matrix every BBO tick.

```
For each (buy_exchange, sell_exchange) pair:
  raw_spread   = sell_bid - buy_ask
  spread_bps   = raw_spread / midprice * 10000
  fee_cost     = 2 * fee_estimate_bps (round trip)
  nat_spread   = avg(natural_spread_buy, natural_spread_sell)
  net_spread   = spread_bps - fee_cost - nat_spread
```

Key features:
- Stale data filtering (>30s → excluded)
- Rolling natural spread per exchange (configurable window)
- Persistence tracking per pair (for signal scoring)

### Correlation Analyzer (`analysis/correlation.py`)
Rolling window cross-correlation between exchange prices using numpy.corrcoef.
Identifies which exchanges move together and which diverge.

```python
from analysis.correlation import CorrelationAnalyzer
corr = CorrelationAnalyzer(window_size=300)
# ... feed prices ...
matrix = corr.compute_correlation_matrix()
# → {('binance', 'okx'): 0.9987, ('lighter', 'grvt'): 0.9912, ...}
```

### Lead-Lag Analyzer (`analysis/lead_lag.py`)
Detects leader-follower relationships using scipy cross-correlation.
Identifies which exchanges move first (typically Binance) and the lag in ms.

```python
from analysis.lead_lag import LeadLagAnalyzer
ll = LeadLagAnalyzer(window_s=300)
# ... feed prices ...
leaders = ll.identify_leaders()
# → [{'exchange': 'binance', 'lead_count': 5, 'avg_lag_ms': 150.0}, ...]
```

The orchestrator runs this every 120s and feeds the leader into SignalGenerator.

### Backtest Engine (`analysis/backtest_engine.py`)
Replays historical BBO data through the real pipeline (SpreadAnalyzer → SignalGenerator).

```python
from analysis.backtest_engine import BacktestEngine
from analysis.spread_analyzer import SpreadAnalyzer
from strategy.signal_generator import SignalGenerator

engine = BacktestEngine(data_dir="data")
data = engine.load_bbo_data("20260307", ["binance", "okx", "lighter"])

# Method 1: Custom strategy function
result = engine.replay(data, my_strategy_fn)

# Method 2: Real pipeline (recommended — matches live exactly)
sa = SpreadAnalyzer(fee_estimate_bps=5.0)
sg = SignalGenerator(min_confidence=0.3)
result = engine.replay_with_pipeline(data, sa, sg, symbol="BTC-PERP")

print(engine.generate_report(result["stats"]))
```

`replay_with_pipeline()` fixes critical P0 issues vs raw `replay()`:
- Injects historical timestamps (no time.time() collapse)
- Batches ticks by interval (prevents look-ahead bias)
- Routes through the same code path as live

## Signal Generation

Signals use **weighted multi-factor confidence scoring** (0-1) instead of hard gates.
This avoids the "0 trades" problem of binary filtering.

| Factor | Weight | Range | Description |
|--------|--------|-------|-------------|
| Spread magnitude | 30% | 0 at 0bps, 1.0 at 20+bps | How large the net spread is |
| Spread persistence | 20% | 0-1 over 10s window | How long the spread has persisted |
| Volume confirmation | 15% | 0.5 (placeholder) | Volume supports direction |
| Funding alignment | 10% | 0-1 from rate differential | Funding rate confirms thesis |
| Lead-lag signal | 15% | 0.5/0.8 based on leader | Leader exchange moved first |
| OI divergence | 10% | 0.5-1.0 from OI imbalance | Open interest imbalance |

Signals above `min_confidence` (default: 0.3) are emitted to CSV + Telegram.

**Important**: This system generates signals only. Trade execution is handled by
exchange-specific bots (e.g., grvt_lighter) with proper safety rules (21 golden rules).

## Monitoring

### Terminal Dashboard
Rich-based live dashboard showing:
- Exchange connection status (online/offline/WS) with tier classification
- Real-time BBO across all exchanges
- Top spreads by net basis points
- Recent signals with confidence scores
- System stats (runtime, tick count, signal count)

### Telegram Alerts
Configured via `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` environment variables.
- Start/stop notifications with exchange list
- Signal alerts with spread and confidence details
- Periodic heartbeat (every 5 min)
- Error/warning notifications
- WS stale detection alerts

## Safety Rules

Critical rules inherited from production arbitrage experience:

1. **API failures MUST raise** — never return 0 or default values
2. **Decimal(str(value)) everywhere** — never float contamination in prices
3. **WS stale detection** — 30s threshold, fall back to REST automatically
4. **Auth failures fail-fast** — raise immediately, don't silently continue
5. **Never abs() on positions** — sign = direction information
6. **Session cleanup in disconnect()** — close all aiohttp sessions and WS

## Rust Migration Readiness

The codebase is structured for future Rust migration of hot-path modules:

### Data Types (`models/data_types.py`)
All hot-path data structures use `@dataclass` with flat fields:
- `BBOState`, `SpreadEntry`, `SignalOutput`, `TradeEntry`, `FundingSnapshot`, `OISnapshot`
- Each maps directly to a Rust struct
- Serializable via `dataclasses.asdict()` for IPC

### IPC Bridge (`ipc/bridge.py`)
Abstract `IPCBridge` with three implementations:
- `InProcessBridge` — current default, zero overhead
- `UDSBridge` — placeholder for Unix domain socket IPC to Rust
- `SharedMemoryBridge` — placeholder for zero-copy shared memory

### Free WS Parsers (`collectors/ws_parsers.py`)
WS message parsing extracted to pure functions (no `self` dependency):
- `parse_lighter_orderbook(msg)` → `(best_bid, best_ask)`
- `parse_hyperliquid_l2book(msg)` → `(best_bid, best_ask)`
- `parse_generic_orderbook(msg)` → `(best_bid, best_ask)`

These map directly to Rust functions: `fn parse_lighter_orderbook(msg: &Value) -> Option<(Decimal, Decimal)>`

## Project Structure

```
DEX_total/
├── config/                  # YAML configs + loader
│   ├── exchanges.yaml       # Exchange configs (API keys via env vars)
│   ├── collection.yaml      # Intervals, depth, symbol mappings
│   ├── strategy.yaml        # Signal weights and thresholds
│   └── loader.py            # YAML loading + env var resolution
├── collectors/              # 12 exchange collectors
│   ├── base_collector.py    # Abstract base class (ABC)
│   ├── ccxt_collector.py    # Unified CEX base (Binance/OKX/Bitget)
│   ├── binance_collector.py # Binance-specific (liquidations, mark price)
│   ├── okx_collector.py     # OKX-specific (passphrase)
│   ├── bitget_collector.py  # Bitget-specific (product type)
│   ├── lighter_collector.py # Lighter DEX — REST + WS
│   ├── grvt_collector.py    # GRVT DEX — REST only (all POST)
│   ├── hyperliquid_collector.py  # Hyperliquid — REST + WS
│   ├── paradex_collector.py # Paradex — REST (Tier 3)
│   ├── aster_collector.py   # Aster — REST (Tier 3, beta)
│   ├── edgex_collector.py   # EdgeX — REST (Tier 3, beta)
│   ├── ws_parsers.py        # Free WS parsing functions (Rust-ready)
│   └── stubs/               # Tier 4 placeholders
│       ├── variational_stub.py
│       ├── nado_stub.py
│       └── o1_stub.py
├── analysis/                # Analysis tools
│   ├── spread_analyzer.py   # NxN cross-exchange spread matrix
│   ├── correlation.py       # Rolling price correlation (numpy)
│   ├── lead_lag.py          # Leader-follower detection (scipy)
│   └── backtest_engine.py   # Historical BBO replay
├── strategy/                # Signal generation
│   ├── signal_generator.py  # Multi-factor confidence scoring
│   └── execution_engine.py  # STUB — no real execution
├── models/                  # Rust-migration-ready data types
│   └── data_types.py        # @dataclass for all hot-path structs
├── ipc/                     # Inter-process communication bridge
│   └── bridge.py            # InProcess / UDS / SharedMemory bridges
├── monitoring/              # Dashboard + alerts
│   ├── dashboard.py         # Rich terminal live UI
│   ├── alerts.py            # Telegram notifications (6 types)
│   └── data_logger.py       # CSV writer (daily rotation, flush-safe)
├── tests/                   # Test suite
│   ├── test_base_collector.py
│   ├── test_spread_analyzer.py
│   └── test_ccxt_collector.py
├── data/                    # CSV output (auto-created, daily rotation)
├── logs/                    # Log files (auto-created)
├── main.py                  # Entry point — Orchestrator
├── requirements.txt         # Python dependencies
├── .env.example             # Template for API keys
├── .gitignore
└── CLAUDE.md                # Claude Code project instructions
```

**Total**: ~5,500 lines across 40 Python files

## Development

### Run Tests
```bash
pytest tests/ -v
```

### Validate All Syntax
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

### Add a New Exchange
1. Create `collectors/new_exchange_collector.py`
2. Inherit from `BaseCollector`
3. Implement `connect()`, `disconnect()`, `get_orderbook()`, `get_ticker()`
4. Optionally implement `get_recent_trades()`, `get_funding_rate()`, `get_open_interest()`
5. Add config entry in `config/exchanges.yaml`
6. Add symbol mapping in `config/collection.yaml`
7. Add factory lambda in `main.py` `COLLECTOR_MAP`
8. Run `ast.parse()` validation

### Storage Estimate
- BBO (1s): ~5 KB/exchange/hour → ~120 KB/day/exchange
- With 9 active exchanges + trades + signals: ~107 MB/day
- Monthly: ~3.2 GB (easily manageable with CSV)

## Design Decisions

| Decision | Rationale |
|----------|-----------|
| Signal != Execution | Each exchange has unique safety rules (21 golden rules). Generic execution is dangerous. |
| Confidence scoring, not hard gates | Binary filtering causes "0 trades" problem. Weighted scoring is tunable. |
| CSV storage | Simple, grep-able, pandas-readable. No DB overhead for research system. |
| ccxt base for CEX | 70% code reuse across Binance/OKX/Bitget. Only exchange-specific quirks overridden. |
| Stubs for Tier 4 | Variational/Nado/01.xyz have no usable API. Stubs prevent crashes without fake data. |
| Dataclasses for data types | Flat FFI-friendly layout. Maps directly to Rust structs for future migration. |
