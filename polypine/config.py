"""Static configuration: which Polymarket series map to which asset/timeframe."""

from dataclasses import dataclass

ASSETS = ("BTC", "ETH", "SOL", "DOGE", "XRP")
TIMEFRAMES = ("5m", "15m", "1h", "1d")

TF_SECONDS = {"5m": 300, "15m": 900, "1h": 3600, "1d": 86400}

# Gamma series slugs. 5m/15m use short tickers; hourly/daily use a mix of names.
_SERIES_NAME = {
    "5m": {"BTC": "btc", "ETH": "eth", "SOL": "sol", "DOGE": "doge", "XRP": "xrp"},
    "15m": {"BTC": "btc", "ETH": "eth", "SOL": "sol", "DOGE": "doge", "XRP": "xrp"},
    "1h": {"BTC": "btc", "ETH": "eth", "SOL": "solana", "DOGE": "doge", "XRP": "xrp"},
    "1d": {"BTC": "btc", "ETH": "eth", "SOL": "solana", "DOGE": "dogecoin", "XRP": "xrp"},
}
_SERIES_SUFFIX = {"5m": "5m", "15m": "15m", "1h": "hourly", "1d": "daily"}

# Binance spot symbols used as the underlying proxy (markets resolve on Chainlink).
BINANCE_SYMBOL = {"BTC": "BTCUSDT", "ETH": "ETHUSDT", "SOL": "SOLUSDT", "DOGE": "DOGEUSDT", "XRP": "XRPUSDT"}

# Polymarket's public real-time data socket: Chainlink prices used to resolve 5m/15m markets.
RTDS_URL = "wss://ws-live-data.polymarket.com"
CHAINLINK_SYMBOL = {"BTC": "btc/usd", "ETH": "eth/usd", "SOL": "sol/usd", "DOGE": "doge/usd", "XRP": "xrp/usd"}

GAMMA_URL = "https://gamma-api.polymarket.com"
CLOB_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
BINANCE_URL = "https://data-api.binance.vision"


@dataclass(frozen=True)
class Series:
    asset: str
    tf: str

    @property
    def slug(self) -> str:
        return f"{_SERIES_NAME[self.tf][self.asset]}-up-or-down-{_SERIES_SUFFIX[self.tf]}"


def all_series(assets=ASSETS, timeframes=TIMEFRAMES) -> list[Series]:
    return [Series(a, tf) for a in assets for tf in timeframes]
