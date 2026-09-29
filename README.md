# polypine

A TradingView-style backtester for Polymarket crypto **Up or Down** markets (BTC, ETH, SOL, DOGE, XRP, HYPE ×
5m, 15m, 1h, 1d). Write strategies in Pine-style Python (PyneCore), run them on the underlying coin's
price, and trade the signals as binary contracts using recorded order books, real fees and oracle outcomes.

## Quick start

```bash
uv sync

# 1. Historical data: public 5m dataset (Mar 24 – May 18 2026) + Binance 1s underlying for the same dates
mkdir -p data/raw/kacho && for c in btc eth sol doge xrp hype; do for k in markets ticks; do
  curl -L -o data/raw/kacho/${c}_$k.parquet \
    https://huggingface.co/datasets/kachoio/polymarket-5-minute-crypto-up-down-markets/resolve/main/${c}_$k.parquet
done; done                                          # ~600 MB
uv run polypine import-kacho
uv run polypine import-binance --start 2026-03-24 --end 2026-05-18

# 2. Live recording runs on the VPS (see "Running the collector on a VPS"); pull its data here:
scripts/sync-data.sh user@your-vps

# 3. Backtest from the CLI ...
uv run polypine backtest strategies/late_momentum.py --asset BTC --bar 5s --start 2026-04-06 --end 2026-04-20
uv run polypine backtest strategies/ema_cross.py --input Fast=5 --input Slow=20 --start 2026-04-06 --end 2026-04-13

# ... or in the browser: chart + editor + results
uv run polypine serve        # http://127.0.0.1:8765

uv run polypine status       # row counts per table
uv run pytest
```

## Running the collector on a VPS

Order books can't be backfilled, so the collector has to run continuously. Any small Linux VPS will do:
1 vCPU and 1 GB RAM is enough, and disk grows by a few hundred MB per day before compaction. A good
network helps with the `1013 slow consumer` drops on the busiest market.

```bash
# on the VPS (Debian/Ubuntu, as root), from a clone of this repo
sudo ./deploy/install.sh              # /opt/polypine, `polypine` user, systemd units, uv + Python 3.12
journalctl -u polypine-collector -f   # stats line every minute: live markets, reconnects, slow_consumer
sudo -u polypine /opt/polypine/.venv/bin/polypine --data /opt/polypine/data status

# after `git pull`, re-run install.sh; it restarts nothing by itself, so:
sudo systemctl restart polypine-collector
```

- `polypine-collector.service` restarts on failure and stops with SIGINT, so buffered rows are flushed.
- `polypine-compact.timer` merges the per-minute part files daily at 00:20 UTC.
- On your laptop, `scripts/sync-data.sh user@vps` mirrors `/opt/polypine/data` into `./data`. It uses
  `rsync --delete` because compaction replaces files, but it protects the locally imported
  `kacho_*` / `binance_*` history. Then backtest locally with `polypine serve` or `polypine backtest`.

## Writing strategies

Strategies are PyneCore scripts ("Pyne": Python with Pine's bar-by-bar semantics); see
`strategies/*.py`. A `.pine` file also works if `PYNESYS_API_KEY` is set: it's compiled through the
paid pynesys.io compiler (3 free conversions).

**Inputs:** a script's `input.*()` defaults are its baseline. Override them per run from the UI ("Inputs override")
or the CLI (`--input min_bps=10`), keyed by argument name or title. Overrides apply to that run only; an unknown
key is an error. Each run re-imports the script, so edits take effect immediately, and PyneCore's per-script
`.toml` files aren't written.

The script runs on **underlying bars** (`--bar`, 1s…1d). Each bar also carries the state of the
**Polymarket market live at the bar's close** (`--tf`) as extra fields:

```python
from pynecore.lib import extra_fields
up_ask: Series[float] = extra_fields["up_ask"]
```

| field | meaning |
|---|---|
| `up_bid`, `up_ask`, `down_bid`, `down_ask` | top of book (na when no quote in the last 5s) |
| `secs_left`, `secs_in` | seconds until the window ends / since it started |
| `price_to_beat` | the window's opening reference price (Chainlink for 5m/15m) |
| `window_start` | window start, ms; changes when a new market begins |
| `oracle_price` | latest recorded Chainlink price (the 5m/15m settlement oracle); na without Chainlink data |
| `oracle_twap60` | mean Chainlink price over the last 60s, which 5m/15m settle on at the window end |

**Measure distance with the oracle, not `close`.** `close` is Binance, which can sit several bps from
Chainlink (4–5 bps on BTC in late September 2026). With a threshold of a few bps, a Binance-based signal
calls the wrong side and buys 4–10¢ longshots; a few of those paying 20× can make a losing strategy look
very profitable. See `strategies/late_momentum.py`, and check "Net excl. best trade" in the results.

How Pine orders turn into contract trades (`polypine/engine.py`):

- `strategy.long` buys the **Up** token and `strategy.short` buys **Down**, in the window live at the fill.
- Pine fills at the next bar's open. The simulator then waits `--latency-ms` (default 1000) and walks the
  recorded 1s book: the best level's size at the ask, then the rest of the depth within 5¢, assumed evenly
  spread over the next five ticks. Anything beyond that depth is left unfilled.
- If Pine exits before the window ends, the position is sold at the best bid. Otherwise it settles at
  $1/$0 on the oracle outcome.
- Fees follow [Polymarket's fee page](https://docs.polymarket.com/trading/fees): every taker match pays
  `shares × rate × (p(1−p))^exponent` USDC, rounded to 5 decimals, with `rate`/`exponent` read from each
  market's Gamma `feeSchedule` (crypto: 0.07 and 1; 0 when `feesEnabled` is false). A fill that spans two
  price levels pays each at its own price. Makers pay no fee; the 20% maker rebate isn't modelled, because
  the simulator only takes liquidity. `tests/test_engine.py` checks the published fee table.
- `--roll`: while Pine stays in position, re-enter every new window. `--min-secs-left` skips late entries.
- Stats: net PnL after fees, ROI on stake, win rate, `edge_vs_implied` (settled payout minus price paid),
  max drawdown, and the t-stat of per-trade PnL.

## What the data says (sanity checks, Apr 6–13 2026, 5m)

Buying one side blindly at a fixed time in each window, with no signal:

| entry price | win rate | avg price | fees / stake | ROI |
|---|---|---|---|---|
| < 0.10 | 2.7% | 0.045 | 6.7% | **−67%** |
| 0.10–0.30 | 16.9% | 0.19 | 5.7% | −24% |
| 0.30–0.70 | 48.3% | 0.50 | 3.5% | −6.9% |
| 0.70–0.90 | 81.5% | 0.81 | 1.3% | +0.5% |
| > 0.90 | 96.3% | 0.96 | 0.3% | +0.4% |

The market is well calibrated around 50%. Fees plus the spread cost about 5–7% of stake near 50¢, and
longshots are heavily overpriced. Any strategy has to clear those costs. Both example strategies roughly
break even or lose after fees; they demonstrate the tool and are not an edge.

## Underlying price per coin

| coin | source | why |
|---|---|---|
| BTC, ETH, SOL, DOGE, XRP | Binance **spot** 1s klines | their 1h/1d markets settle on Binance spot |
| HYPE | Binance **USD-M perpetual**, 1s bars built from aggTrades | HYPE 1h/1d settle on the perpetual; spot is ~30x thinner |

Futures has no 1s klines, so `polypine/underlying.py` aggregates trades into 1s bars and fills quiet seconds
flat at the last close (like spot 1s klines). Configure per coin in `config.UNDERLYING`. Against Chainlink's
price to beat at window start, spot sits a median of about 2.3 bps away and the HYPE perpetual about 4.9 bps
(13.7 bps at p99), so Binance is a weaker 5m/15m proxy for HYPE.

HYPE's 5m books are also much wider: a median spread of 9¢ (BTC 1¢, DOGE 6¢) with about 15 shares at the
best ask, so buying at the ask costs about 5–6¢ over mid before fees.

## Historical data

- **Kacho CC0 dataset** (Hugging Face `kachoio/polymarket-5-minute-crypto-up-down-markets`): per-second
  top of book for about 65k 5m markets on our 5 coins. Download `<coin>_markets.parquet` and
  `<coin>_ticks.parquet` into `data/raw/kacho/`. `import-kacho` takes the ticks from it but re-fetches
  market metadata and **oracle outcomes from Gamma** (100 slugs per request). The dataset's own outcome
  is missing for about 10% of markets and wrong for another 0.2%.
- **Binance** daily 1s kline archives (data.binance.vision). Their timestamps changed from ms to µs in 2025,
  and the importer converts them. At window start, Binance is a median of 2.3 bps away from Chainlink's
  `priceToBeat`, which matters for near-the-money 5m trades.

## Data layout

`data/<table>/asset=BTC/tf=5m/date=YYYY-MM-DD/part-*.parquet` (zstd). `underlying_1s` has no `tf=` level.

| table | grain | notes |
|---|---|---|
| `markets` | 1 row per window | token ids, `start_ms`/`end_ms`, tick size, fee schedule, TWAP lookback, `price_to_beat` |
| `resolutions` | 1 row per window | `outcome` UP/DOWN, `price_to_beat`, `final_price` |
| `book_ticks` | 1s per token | best bid/ask, sizes, resting depth within 5¢ |
| `book_l2` | 10s per token | top 10 levels per side (for depth-aware fills) |
| `trades` | every print | price, size, taker side, tx hash |
| `underlying_1s` | 1s per asset | Binance 1s bars: the exact oracle for 1h/1d, a proxy for 5m/15m (see below) |
| `chainlink_1s` | ~1s per asset | Chainlink oracle prices (Polymarket real-time socket): the 5m/15m resolution source |

Imported tables live in the same layout (`kacho_*.parquet`, `binance_*.parquet` files). `polypine/datafeed.py` reads everything through DuckDB and deduplicates markets and resolutions.

A restart can write a `markets` row twice and overlap `underlying_1s` by 10 minutes, so dedupe in queries:

```sql
select * from read_parquet('data/markets/**/*.parquet', hive_partitioning=true)
qualify row_number() over (partition by condition_id) = 1;
```

## How the markets work (verified live, Sep 2026)

- **Discovery**: Gamma `/events?series_slug=<slug>&closed=false&end_date_min=<now>`. The series slugs are in
  `polypine/config.py`. 5m and 15m event slugs are `<asset>-updown-<tf>-<window start epoch>`.
- **Resolution oracle depends on the timeframe**:
  - **5m / 15m**: Chainlink BTC/USD **60s TWAP** at the end of the window, compared with the price at the
    start (`cryptoMarketConfig.twapLookbackSeconds = 60`). Binance 1s data is only a proxy here.
  - **1h**: the **Binance** `<ASSET>/USDT` 1h candle, Up if close ≥ open.
  - **1d**: the **Binance** 1m candle close at 12:00 ET, compared with the previous day's 12:00 ET close.
  - So `underlying_1s` reproduces 1h and 1d outcomes exactly.
- **Resolution timing**: 5m and 15m close about 1 minute after the end, and 1h and 1d take 11–33 minutes.
  `eventMetadata.finalPrice` can lag the outcome by several more minutes (market N's `finalPrice` equals
  market N+1's `priceToBeat`). The collector waits up to 30 minutes for it.
- **Fees**: taker only, in USDC: `fee = shares × 0.07 × p × (1 − p)`, so at p = 0.5 it's 1.75¢ per share, which is 3.5% of the stake.
- **Order book**: the WS `book` event lists bids ascending and asks descending. A `price_change` gives the new
  absolute size (0 removes the level). One busy 5m market sends about 650 messages per second.
  The Up and Down books mirror each other (Up bid = 1 − Down ask), which the recorded data confirms.

## Known limits

- Polymarket drops the busiest connections (BTC 5m/15m) with `1013 slow consumer` about 2–3 times per
  10 minutes, at any point in the window, while the process stays far from CPU limits. On the VPS
  this cost about 0.3% of BTC 5m book seconds, in 1–2s gaps, and no other coin was affected. The backtester
  accepts quotes up to 5s old, so these gaps don't cause skipped entries. The collector reconnects and takes
  a fresh snapshot. The flush line reports the worst event-loop stall and the number of stalls over 250ms
  for the last interval, plus cumulative `reconnects` / `slow_consumer` counts.
- `underlying_1s` starts 10 minutes before the collector does. Use `backfill-underlying` for more history.
- Backtests on 5s bars run at about 4k bars/s (PyneCore's bar-by-bar interpreter). Two weeks of 5s bars
  takes about a minute; 1m bars are near-instant.
- The fill model only knows the best level and the total depth within 5¢ (plus 10s L2 snapshots from
  the live recorder, not yet used), so large stakes are modelled approximately.
- Only 5m markets have historical order books (from the dataset). 15m/1h/1d become backtestable as the
  live collector builds their history.
