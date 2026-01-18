"""CLI entrypoint for the Query API server."""

from pathlib import Path

import uvicorn

from ravebear_monolith.api.app import create_app
from ravebear_monolith.foundation.config import load_config


def main() -> None:
    """Run the Query API server."""
    config_path = Path("config/settings.yaml")

    if not config_path.exists():
        print(f"Config file not found: {config_path}")
        print("Creating default config...")
        config_path.parent.mkdir(parents=True, exist_ok=True)
        config_path.write_text("# RAVEBEAR config\n", encoding="utf-8")

    config = load_config(config_path)
    app = create_app(config)

    uvicorn.run(
        app,
        host="127.0.0.1",
        port=8000,
        log_level="info",
    )


if __name__ == "__main__":
    main()
