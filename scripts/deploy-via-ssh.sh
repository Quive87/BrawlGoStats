#!/bin/bash
# Deploy to VPS via SSH: sync files, build Go worker, restart services.
# Usage: DEPLOY_SSH=root@api.corestats.pro ./scripts/deploy-via-ssh.sh
#    or: ./scripts/deploy-via-ssh.sh root@api.corestats.pro

set -e
DEPLOY_SSH="${DEPLOY_SSH:-$1}"
REPO_ROOT="${REPO_ROOT:-/var/www/BrawlGoStats}"

if [ -z "$DEPLOY_SSH" ]; then
  echo "Usage: DEPLOY_SSH=user@host $0   OR   $0 user@host"
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "=== Syncing repo to $DEPLOY_SSH:$REPO_ROOT ==="
rsync -avz --exclude '.git' --exclude 'venv' --exclude '__pycache__' --exclude '*.pyc' \
  "$PROJECT_ROOT/" "$DEPLOY_SSH:$REPO_ROOT/"

echo "=== Building and restarting on VPS ==="
ssh "$DEPLOY_SSH" "cd $REPO_ROOT/worker && go build -o worker . && cd $REPO_ROOT && systemctl restart brawl-discovery brawl-worker && systemctl is-active brawl-discovery brawl-worker && echo Done."
