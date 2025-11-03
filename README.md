# Martingale Multi-Symbol Trading Bot

This repository contains a modular martingale trading framework targeting Binance spot markets. It provides:

- Backtesting against historical CSV data via Backtrader-style loops.
- Live execution on the Binance spot **testnet**, with optional Telegram notifications.
- A reusable architecture that separates strategy logic, portfolio/risk management, exchange access, and notification delivery.

> **Security notice:** `config/testnet.yaml` currently holds plain-text API credentials. Replace them with environment-variable references (e.g. `${BINANCE_TEST_KEY}`) before committing or sharing the repository.

## Project Structure

- `config/` – Environment-specific YAML configs (`backtest.yaml`, `testnet.yaml`).
- `core/` – Engine loop, configuration helpers, and portfolio management.
- `strategies/` – Strategy definitions (martingale with MACD/Stoch RSI/EMA triggers).
- `services/` – Exchange client integrations (Binance REST/WebSocket).
- `notifiers/` – Notification adapters (stdout, Telegram).
- `backtests/` – CSV-based backtest runner that mirrors live execution flow.

## Getting Started

1. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

2. **Prepare data for backtests**  
   Place historical candles in `data/BTCUSDT_1h.csv` (columns: `datetime,open,high,low,close,volume`).

3. **Run a backtest**
   ```bash
   python run_backtest.py -c config/backtest.yaml
   ```
   This outputs summary metrics and saves `equity_martingale.png`.

4. **Configure testnet credentials**  
   Export your Binance spot testnet keys (recommended) or edit `config/testnet.yaml`:
   ```powershell
   $env:BINANCE_TEST_KEY  = "your_testnet_key"
   $env:BINANCE_TEST_SECRET = "your_testnet_secret"
   ```

5. **Launch live testnet session**  
   ```bash
   python run_testnet.py -c config/testnet.yaml
   ```
   The runner subscribes to multi-symbol klines, processes signals per bar, places testnet orders, and emits log snapshots + notifications.

## Configuration Highlights

- **Multi-symbol** support via `exchange.symbols` (each symbol uses its own portfolio instance and cash budget).
- **Risk controls** (`risk.max_notional`, `cooldown_minutes`) limit martingale exposure.
- **Notifications** – Enable Telegram by setting `notifications.telegram.enabled` to `true` and providing `bot_token` & `chat_id`.

## Development Notes

- Logs: strategy-level indicator snapshots are emitted once per completed bar (e.g. every minute if `interval: 1m`).
- Testing: you can quickly sanity-check indicator logic with local CSVs using the provided backtest runner.
- Secrets: store API keys outside of version control (environment variables, secret managers). Update configs accordingly before publishing.

## Suggested Next Steps

1. Replace hard-coded credentials with `${ENV_VAR}` placeholders and document required environment variables.
2. Add unit tests covering `PortfolioManager.process_signal` and strategy edge cases.
3. Extend `notifications/` with additional channels (e.g., email, webhook) if needed.

