#!/bin/sh
set -eu
cd "$(dirname "$0")"
replicas="${1:-1}"
case "$replicas" in 1|2|3|4) ;; *) echo 'Usage: sh start.sh [1|2|3|4]' >&2; exit 2 ;; esac
if [ ! -f .env ]; then
 python3 -c "import secrets,pathlib; pathlib.Path('.env').write_text('POSTGRES_PASSWORD='+secrets.token_hex(24)+'\nPORT=18081\nPROMETHEUS_PORT=19090\nWEB_CPUS=1.5\n')"
fi
docker compose up -d --build --wait --scale "web=$replicas"
docker compose restart lb
