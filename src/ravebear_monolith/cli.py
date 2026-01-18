"""CLI entry points for Ravebear Monolith."""

import argparse
import sys
from pathlib import Path

from ravebear_monolith.runtime.main import run


def cli_main() -> int:
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        prog="monolith",
        description="Ravebear Monolith - Unified Trading Intelligence System",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # run command
    run_parser = subparsers.add_parser("run", help="Run the monolith")
    run_parser.add_argument(
        "--config",
        type=Path,
        default=Path("config/settings.yaml"),
        help="Path to configuration file",
    )

    args = parser.parse_args()

    if args.command == "run":
        return run(config_path=args.config)

    return 0


def main() -> None:
    """Entry point for poetry scripts."""
    sys.exit(cli_main())
