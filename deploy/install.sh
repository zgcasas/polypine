#!/usr/bin/env bash
# Run on the VPS as root, from a checkout of this repo:  sudo ./deploy/install.sh
# Installs to /opt/polypine, creates a `polypine` user, and enables the collector + daily compaction.
set -euo pipefail
SRC="$(cd "$(dirname "$0")/.." && pwd)"
DEST=/opt/polypine

id polypine &>/dev/null || useradd --system --create-home --home-dir /var/lib/polypine --shell /usr/sbin/nologin polypine
mkdir -p "$DEST/data"
rsync -a --delete --exclude .venv --exclude data --exclude logs --exclude .git "$SRC/" "$DEST/"
chown -R polypine:polypine "$DEST"

# uv + a Python 3.12 venv owned by the service user
if ! sudo -u polypine bash -lc 'command -v uv' &>/dev/null; then
  sudo -u polypine bash -lc 'curl -LsSf https://astral.sh/uv/install.sh | sh'
fi
sudo -u polypine bash -lc "cd $DEST && ~/.local/bin/uv sync --no-dev --python 3.12"

cp "$DEST"/deploy/polypine-collector.service "$DEST"/deploy/polypine-compact.{service,timer} /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now polypine-collector.service polypine-compact.timer
systemctl --no-pager status polypine-collector.service | head -5
echo "Logs: journalctl -u polypine-collector -f"
