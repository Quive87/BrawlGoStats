#!/bin/bash
# Run this script on the VPS (e.g. after git pull or rsync) to build and restart services.
# Usage: ./scripts/deploy-vps.sh   (from repo root) or from /var/www/BrawlGoStats

set -e
REPO_ROOT="${REPO_ROOT:-/var/www/BrawlGoStats}"
cd "$REPO_ROOT"

echo "=== Building Go worker ==="
cd worker
go build -o worker .
cd ..

echo "=== Restarting services ==="
systemctl restart brawl-discovery
systemctl restart brawl-worker

echo "=== Status ==="
systemctl is-active brawl-discovery brawl-worker

echo "Done. Discovery (Python) and worker (Go) restarted."
