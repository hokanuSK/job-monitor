#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-$ROOT_DIR/.venv/bin/python}"

if [[ ! -x "$PYTHON_BIN" ]]; then
  PYTHON_BIN="$(command -v python3 || true)"
fi

if [[ -z "${PYTHON_BIN:-}" ]]; then
  echo "No Python interpreter found. Set PYTHON_BIN or create .venv." >&2
  exit 1
fi

RECIPIENT="${1:-recipient@example.com}"

export SMTP_HOST="${SMTP_HOST:-127.0.0.1}"
export SMTP_PORT="${SMTP_PORT:-1025}"
export SMTP_STARTTLS="${SMTP_STARTTLS:-0}"
export SMTP_SSL="${SMTP_SSL:-0}"
export SMTP_FROM="${SMTP_FROM:-test@local}"
export SMTP_USER="${SMTP_USER:-}"
export SMTP_PASSWORD="${SMTP_PASSWORD:-}"
export TEST_RECIPIENT="$RECIPIENT"
export MAX_AGE_HOURS="${MAX_AGE_HOURS:-24}"

cd "$ROOT_DIR"

"$PYTHON_BIN" - <<'PY'
import os
import pandas as pd

from web_app import send_jobs_email

recipient = os.environ["TEST_RECIPIENT"]
max_age_hours = float(os.environ["MAX_AGE_HOURS"])

jobs_df = pd.DataFrame(
    [
        {
            "index": 1,
            "title": "SMTP local test",
            "company": "Local",
            "location": "127.0.0.1",
            "date_posted": "dnes",
            "salary": "",
            "url": "http://localhost/test",
        }
    ]
)

ok, message = send_jobs_email(recipient, max_age_hours, jobs_df)
print(f"ok={ok}")
print(message)

if not ok:
    raise SystemExit(1)
PY
