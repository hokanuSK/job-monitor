#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-$ROOT_DIR/.venv/bin/python}"
SMTP_HOST="${SMTP_HOST:-127.0.0.1}"
SMTP_PORT="${SMTP_PORT:-1025}"

if [[ ! -x "$PYTHON_BIN" ]]; then
  PYTHON_BIN="$(command -v python3 || true)"
fi

if [[ -z "${PYTHON_BIN:-}" ]]; then
  echo "No Python interpreter found. Set PYTHON_BIN or create .venv." >&2
  exit 1
fi

echo "Starting local SMTP debug server on ${SMTP_HOST}:${SMTP_PORT}"
echo "Press Ctrl+C to stop."
exec "$PYTHON_BIN" -m smtpd -c DebuggingServer -n "${SMTP_HOST}:${SMTP_PORT}"
