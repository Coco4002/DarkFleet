#!/usr/bin/env bash
# Nightly backup of the DarkFleet database (run from the repository root, e.g. by cron on the server):
#   0 3 * * * cd /opt/darkfleet && deploy/backup/backup.sh >> backups/backup.log 2>&1
#
# Writes backups/darkfleet-<UTC time>.dump (pg_dump custom format: compressed, restorable table by table)
# and keeps the newest $KEEP. Set BACKUP_REMOTE (an rsync destination such as user@host:/srv/backups)
# to also copy each dump off the machine; a backup that only lives on the server it protects is not one.
#
# Restore into an empty database:
#   docker compose exec -T postgres pg_restore -U darkfleet -d darkfleet --clean --if-exists < backups/<file>.dump
set -euo pipefail

KEEP="${KEEP:-14}"
DIR="${BACKUP_DIR:-backups}"
mkdir -p "$DIR"

file="$DIR/darkfleet-$(date -u +%Y%m%dT%H%M%SZ).dump"
tmp="$file.partial"

docker compose exec -T postgres pg_dump -U darkfleet -d darkfleet --format=custom --compress=6 > "$tmp"
# a dump pg_restore can't list is broken; never let it replace a good one
docker compose exec -T postgres pg_restore --list < "$tmp" > /dev/null
mv "$tmp" "$file"
echo "$(date -u +%FT%TZ) wrote $file ($(du -h "$file" | cut -f1))"

if [ -n "${BACKUP_REMOTE:-}" ]; then
  rsync -a "$file" "$BACKUP_REMOTE/"
  echo "$(date -u +%FT%TZ) copied to $BACKUP_REMOTE"
fi

ls -1t "$DIR"/darkfleet-*.dump | tail -n +"$((KEEP + 1))" | xargs -r rm --
