from alpaca.trading.client import TradingClient
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import Keys

trading_client = TradingClient(Keys.Key_Test, Keys.Secret_Test, paper=True)

trading_client.close_all_positions(cancel_orders=True)

print("All positions closed")