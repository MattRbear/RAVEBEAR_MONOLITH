"""
Start the multi-venue candle collector - CLEAN OUTPUT VERSION.
"""
import argparse
import asyncio
import logging
import signal
import sys
from pathlib import Path

# Add parent to path
sys.path.insert(0, str(Path(__file__).parent))

from collector.config import load_config
from collector.runtime_multi_fixed import MultiVenueRuntime


def setup_logging():
    """Setup minimal logging - errors only."""
    # Only show warnings and errors in console
    logging.basicConfig(
        level=logging.WARNING,
        format='%(levelname)s: %(message)s',
        handlers=[logging.StreamHandler()]
    )
    
    # File handler for debug info
    file_handler = logging.FileHandler('logs/collector.log')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(
        '%(asctime)s %(levelname)s %(name)s: %(message)s'
    ))
    logging.getLogger().addHandler(file_handler)
    
    # Suppress noisy loggers
    logging.getLogger('aiohttp').setLevel(logging.ERROR)
    logging.getLogger('asyncio').setLevel(logging.ERROR)
    logging.getLogger('urllib3').setLevel(logging.ERROR)


def main():
    parser = argparse.ArgumentParser(description='Multi-venue candle collector')
    parser.add_argument('--config', '-c', default='config.yaml', help='Config file path')
    args = parser.parse_args()
    
    # Create logs directory
    Path('logs').mkdir(exist_ok=True)
    
    setup_logging()
    
    # Load config
    config = load_config(args.config)
    
    # Create runtime
    runtime = MultiVenueRuntime(config)
    stop_event = asyncio.Event()
    
    # Signal handlers
    def handle_signal(sig, frame):
        print("\n\n  Ctrl+C received, stopping...")
        stop_event.set()
    
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)
    
    # Run
    try:
        asyncio.run(runtime.run(stop_event))
    except KeyboardInterrupt:
        pass
    
    print("  Done.\n")


if __name__ == '__main__':
    main()
