Martingale Trading Framework
============================

Structure
---------
- `config/`: Environment-specific YAML settings for backtest and Binance testnet.
- `core/`: Shared engine, portfolio, configuration, and type definitions.
- `strategies/`: Strategy implementations, including the martingale entry logic.
- `services/`: Integrations such as the Binance exchange client.
- `notifiers/`: Notification adapters (Telegram, console).
- `backtests/`: Vectorised backtest runner that replays CSV data.

Usage
-----
1. Create a virtual environment and install requirements: `pip install -r requirements.txt`.
2. Drop historical bars into `data/BTCUSDT_1h.csv` with columns `datetime,open,high,low,close,volume`.
3. Run a backtest: `python run_backtest.py -c config/backtest.yaml`.
4. Prepare Binance testnet API keys and Telegram credentials, export them as environment variables, then launch: `python run_testnet.py -c config/testnet.yaml`.

Notes
-----
- The portfolio manager enforces martingale position scaling and stop conditions.
- Notifications default to stdout and can be extended by adding modules in `notifiers/`.
