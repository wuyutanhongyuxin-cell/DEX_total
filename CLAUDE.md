# DEX_total - Claude Code Project Instructions

## Project Overview
Multi-exchange data collection and cross-exchange analysis system covering 12 exchanges.
**Signal generation only** — execution is delegated to exchange-specific bots.

## Architecture
```
Collectors (12) → DataLogger (CSV) → Analysis (spread/corr/lead-lag) → SignalGenerator → Telegram
                                                                        ↓
                                                                   ExecutionEngine (STUB)
```

## Critical Safety Rules (from 21 Golden Rules)

### Universal (apply to ALL collectors)
1. **API failures MUST raise exceptions** — never return 0 or default values
2. **All prices/sizes use Decimal(str(value))** — never float contamination
3. **WS stale detection** — 30s threshold, fall back to REST
4. **Auth failures must fail-fast** — raise, not log
5. **Never use abs() on position sizes** — sign = direction info
6. **Session cleanup in disconnect()** — close all aiohttp sessions, WS connections

### Exchange-Specific
- **GRVT**: All endpoints are POST, not GET. Prices must align to tick_size.
- **Lighter**: Position has separate `sign` field. IOC = ORDER_TYPE_MARKET(1).
- **Hyperliquid**: Single POST endpoint for all queries. Use `type` field to select.
- **Tier 3 (Paradex/Aster/EdgeX)**: Graceful degradation — log + re-raise, don't crash.
- **Tier 4 (Variational/Nado/01)**: Stubs only, always return NotImplementedError.

## Key File Paths

### P0 (Critical Path)
- `collectors/base_collector.py` — abstract interface, all collectors inherit
- `collectors/ccxt_collector.py` — unified CEX base (Binance/OKX/Bitget)
- `analysis/spread_analyzer.py` — NxN spread matrix, core analysis
- `strategy/signal_generator.py` — confidence scoring, signal output
- `main.py` — orchestrator, collection loops

### P1 (Important)
- `monitoring/alerts.py` — Telegram notifications
- `monitoring/dashboard.py` — Rich terminal UI
- `monitoring/data_logger.py` — CSV output
- `config/loader.py` — YAML config + env var resolution

### P1.5 (Rust Migration Prep)
- `models/data_types.py` — @dataclass for all hot-path structs (FFI-ready)
- `ipc/bridge.py` — IPC bridge abstraction (InProcess / UDS / SharedMemory)
- `collectors/ws_parsers.py` — Free WS parsing functions (no self dependency)

### P2 (Individual Collectors)
- `collectors/{exchange}_collector.py` — one per exchange
- `collectors/stubs/` — Tier 4 placeholders

## Data Schema

### CSV Output Structure
```
data/YYYYMMDD/
  bbo_{exchange}.csv         # timestamp, exchange, symbol, bid, ask, bid_size, ask_size
  trades_{exchange}.csv      # timestamp, exchange, symbol, side, price, size, trade_id
  funding_{exchange}.csv     # timestamp, exchange, symbol, rate, next_funding_time
  oi_{exchange}.csv          # timestamp, exchange, symbol, open_interest, oi_value
  spreads_matrix.csv         # timestamp, buy_ex, sell_ex, symbol, spread_bps, net_bps, ...
  signals_signals.csv        # timestamp, buy_ex, sell_ex, symbol, direction, confidence, ...
```

## Config Files
- `config/exchanges.yaml` — API keys (via env vars), endpoints, tier classification
- `config/collection.yaml` — intervals, depth, symbol mapping per exchange
- `config/strategy.yaml` — signal weights, thresholds, monitoring settings

## Exchange Tiers
| Tier | Exchanges | Connection | Status |
|------|-----------|------------|--------|
| 1 | Binance, OKX, Bitget | ccxt unified | Production |
| 2 | Lighter, GRVT, Hyperliquid | Custom SDK/REST+WS | Production |
| 3 | Paradex, Aster, EdgeX | REST only | Beta |
| 4 | Variational, Nado, 01.xyz | Stub | No API |

## Development Commands
```bash
# Run
python main.py

# Test
pytest tests/ -v

# Validate syntax
python -c "import ast,os; [ast.parse(open(os.path.join(r,f),encoding='utf-8').read()) for r,d,fs in os.walk('.') for f in fs if f.endswith('.py')]"
```

## Design Decisions
1. **Signal != Execution** — This system generates signals only. Execution needs exchange-specific safety rules (21 golden rules).
2. **Confidence scoring, not hard gates** — Avoids "0 trades" problem from binary filtering.
3. **CSV storage** — Simple, grep-able, pandas-readable. No database overhead.
4. **ccxt base for CEX** — 70% code reuse across Binance/OKX/Bitget.
