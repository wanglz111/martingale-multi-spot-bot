#!/usr/bin/env sh
set -e

MODE="${MODE:-backtest}"
CONFIG_FILE="${CONFIG_FILE:-config/backtest.yaml}"

echo "Running martingale bot | mode=${MODE} | config=${CONFIG_FILE}"

case "${MODE}" in
  backtest)
    exec python run_backtest.py -c "${CONFIG_FILE}"
    ;;
  testnet)
    exec python run_testnet.py -c "${CONFIG_FILE}"
    ;;
  *)
    echo "Unsupported MODE: ${MODE}. Use 'backtest' or 'testnet'."
    exit 1
    ;;
esac
