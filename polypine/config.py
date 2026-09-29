"""Static configuration: which Polymarket series map to which asset/timeframe."""

from dataclasses import dataclass

ASSETS = ("BTC", "ETH", "SOL", "DOGE", "XRP", "HYPE")
TIMEFRAMES = ("5m", "15m", "1h", "1d")

TF_SECONDS = {"5m": 300, "15m": 900, "1h": 3600, "1d": 86400}

# Gamma series slugs. 5m/15m use short tickers; hourly/daily use a mix of names.
_SERIES_NAME = {
    "5m": {"BTC": "btc", "ETH": "eth", "SOL": "sol", "DOGE": "doge", "XRP": "xrp", "HYPE": "hype"},
    "15m": {"BTC": "btc", "ETH": "eth", "SOL": "sol", "DOGE": "doge", "XRP": "xrp", "HYPE": "hype"},
    "1h": {"BTC": "btc", "ETH": "eth", "SOL": "solana", "DOGE": "doge", "XRP": "xrp", "HYPE": "hype"},
    "1d": {"BTC": "btc", "ETH": "eth", "SOL": "solana", "DOGE": "dogecoin", "XRP": "xrp", "HYPE": "hype"},
}
_SERIES_SUFFIX = {"5m": "5m", "15m": "15m", "1h": "hourly", "1d": "daily"}

# Underlying price feed per asset: (Binance market, symbol). It is the 1h/1d resolution source (spot for
# most coins; HYPE's hourly/daily markets settle on the USD-M perpetual, whose spot book is thin) and a
# proxy for 5m/15m, which resolve on Chainlink.
UNDERLYING = {
    "BTC": ("spot", "BTCUSDT"), "ETH": ("spot", "ETHUSDT"), "SOL": ("spot", "SOLUSDT"),
    "DOGE": ("spot", "DOGEUSDT"), "XRP": ("spot", "XRPUSDT"), "HYPE": ("futures", "HYPEUSDT"),
}
BINANCE_SYMBOL = {a: sym for a, (_, sym) in UNDERLYING.items()}

# Polymarket's public real-time data socket: Chainlink prices used to resolve 5m/15m markets.
RTDS_URL = "wss://ws-live-data.polymarket.com"
CHAINLINK_SYMBOL = {"BTC": "btc/usd", "ETH": "eth/usd", "SOL": "sol/usd", "DOGE": "doge/usd", "XRP": "xrp/usd",
                    "HYPE": "hype/usd"}

GAMMA_URL = "https://gamma-api.polymarket.com"
CLOB_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
BINANCE_URL = "https://data-api.binance.vision"
BINANCE_FUTURES_URL = "https://fapi.binance.com"


@dataclass(frozen=True)
class Series:
    asset: str
    tf: str

    @property
    def slug(self) -> str:
        return f"{_SERIES_NAME[self.tf][self.asset]}-up-or-down-{_SERIES_SUFFIX[self.tf]}"


def all_series(assets=ASSETS, timeframes=TIMEFRAMES) -> list[Series]:
    return [Series(a, tf) for a in assets for tf in timeframes]
