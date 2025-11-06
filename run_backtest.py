import argparse

from backtests.runner import run_backtest


def main() -> None:
    parser = argparse.ArgumentParser(description="Run martingale backtest.")
    parser.add_argument(
        "-c",
        "--config",
        default="config/backtest.yaml",
        help="Path to backtest configuration file.",
    )
    args = parser.parse_args()
    run_backtest(args.config)


if __name__ == "__main__":
    main()
