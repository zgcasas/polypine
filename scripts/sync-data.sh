#!/usr/bin/env bash
# Pull the VPS recordings into ./data for local backtesting.
#   scripts/sync-data.sh user@vps [remote_data_dir]
# --delete mirrors the VPS (compaction replaces part files, and stale local copies would duplicate rows),
# but the protect filters keep locally imported history (import-kacho / import-binance output).
# "not empty, cannot delete" warnings are expected: those folders hold only the protected imports.
set -euo pipefail
HOST="${1:?usage: $0 user@vps [remote_data_dir]}"
REMOTE="${2:-/opt/polypine/data}"
rsync -az --stats --delete \
  --filter='P kacho_*' --filter='P binance_*' --filter='P raw/' \
  --exclude 'raw/' --exclude '*.tmp' \
  "$HOST:$REMOTE/" "$(dirname "$0")/../data/"
