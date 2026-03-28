#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

SMTP_HOST="${SMTP_HOST:-smtp.gmail.com}"
SMTP_PORT="${SMTP_PORT:-587}"
SMTP_STARTTLS="${SMTP_STARTTLS:-1}"
SMTP_SSL="${SMTP_SSL:-0}"

SMTP_USER="${SMTP_USER:-}"
if [[ -z "$SMTP_USER" ]]; then
  read -r -p "Gmail address: " SMTP_USER
fi

SMTP_PASSWORD="${SMTP_PASSWORD:-}"
if [[ -z "$SMTP_PASSWORD" ]]; then
  read -r -s -p "Gmail app password (16 chars): " SMTP_PASSWORD
  echo
fi

if [[ -z "$SMTP_USER" || -z "$SMTP_PASSWORD" ]]; then
  echo "SMTP_USER and SMTP_PASSWORD are required." >&2
  exit 1
fi

SMTP_FROM="${SMTP_FROM:-$SMTP_USER}"
RECIPIENT="${1:-$SMTP_USER}"

export SMTP_HOST
export SMTP_PORT
export SMTP_STARTTLS
export SMTP_SSL
export SMTP_USER
export SMTP_PASSWORD
export SMTP_FROM

exec "$ROOT_DIR/scripts/test_smtp_send.sh" "$RECIPIENT"
