#!/usr/bin/env bash
# Sweep late_momentum on the latest 24h of BTC 5m data in ./data.
#   scripts/sweep_late_momentum.sh                      # full grid, ranked
#   scripts/sweep_late_momentum.sh --until-profitable   # stop at the first robust profitable combination
# Extra flags pass through to `polypine sweep` (e.g. --hours 48, --asset ETH, --min-trades 30).
set -euo pipefail
cd "$(dirname "$0")/.."
exec uv run --quiet polypine sweep strategies/late_momentum.py \
  --grid window_secs=200:30:-10 \
  --grid min_bps=0:10:1 \
  --grid max_price=0.50:0.96:0.02 \
  --out "sweeps/late_momentum_$(date -u +%Y%m%dT%H%M).csv" \
  "$@"
