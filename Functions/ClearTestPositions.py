from alpaca.trading.client import TradingClient
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from Config import get_alpaca_credentials

credentials = get_alpaca_credentials()
if not credentials.paper:
    raise RuntimeError("ClearTestPositions.py only supports ALPACA_ENV=paper.")

trading_client = TradingClient(credentials.key, credentials.secret, paper=True)

trading_client.close_all_positions(cancel_orders=True)

print("All positions closed")
