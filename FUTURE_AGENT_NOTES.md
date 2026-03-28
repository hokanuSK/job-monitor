# Future Agent Notes (Job Monitor)

## Snapshot
- Last updated: 2026-03-28.
- App is a local Flask UI with MySQL-backed jobs, offline filtering, and SMTP notifications.
- Job descriptions are now scraped and stored; filters can target specific requirement sections from job detail text.
- Notification emails now send as multipart digests with styled HTML + plain-text fallback.

## Main Files
- `src/web_app.py`: Flask routes, updater thread, listing scrape, detail description extraction, section filters, notification send.
- `src/mysql_store.py`: MySQL schema, upsert/load, URL normalization, missing-description lookup + update helpers.
- `templates/index.html`: UI filter form (including section-based requirement filters) and send notifications form.
- `src/job_monitor.py`: Scrapy spider for CSV/XLSX export, now also visits detail pages to populate `description`.
- `app_settings.json`: persisted `recipient_email` and `notification_max_age_hours`.
- `docker-compose.yml`: optional MySQL service (may conflict with existing host MySQL on `3306`).

## Current Behavior
- `GET /`
  - Starts updater thread once.
  - Loads all jobs from MySQL.
  - Applies filters offline and renders table (`limit` 1-1000).
- `POST /apply`
  - Applies filters only (no one-shot full-site crawl).
  - Redirects to `GET /` with filter params.
  - Flash text: `Filters applied. Notifications use these currently applied filters.`
- `POST /send-mails`
  - Uses currently applied filters from hidden fields.
  - Applies max-age cutoff in hours.
  - Sends one multipart email with matched jobs via SMTP (plain text + HTML alternative).

## Description + Section Filtering
- Listing updater (`scrape_first_page_jobs`) now fetches each job detail page and stores `description`.
- Description extraction priority:
  1. `application/ld+json` `JobPosting.description`
  2. Fallback to visible page text between known section markers.
- Cached in-memory by URL (`_description_cache`) to reduce repeated parsing.
- Background backfill updates old DB rows missing descriptions each updater cycle:
  - Env var `DESCRIPTION_BACKFILL_BATCH` (default `1`).

### Section-based filters (from bold headings in job ads)
All fields are "contains all terms" with comma/semicolon/newline tokenization.
- `section_education`: `Pozícii vyhovujú uchádzači so vzdelaním`
- `section_education_field`: `Vzdelanie v odbore`
- `section_languages`: `Jazykové znalosti`
- `section_other_knowledge`: `Ostatné znalosti`
- `section_practice_area`: `Prax na pozícii/v oblasti`
- `section_years_experience`: `Počet rokov praxe`
- `section_personal_skills`: `Osobnostné predpoklady a zručnosti`
- `section_job_tasks`: `Čo budeš robiť`

Notes:
- Matching is case-insensitive and diacritics-insensitive for section heading detection.
- Global `search` still searches the full description text (plus title/company/location/date/salary).

## Data Notes
- Job URL normalization strips query strings before upsert (`normalize_job_url`), reducing duplicates.
- `jobs.url` has unique index; upsert updates existing rows and `last_seen_at`.
- Upsert behavior preserves existing non-empty description if incoming row has empty description.

## Environment Variables

### MySQL
- `MYSQL_HOST` default `127.0.0.1`
- `MYSQL_PORT` default `3306`
- `MYSQL_USER` default `jobs_user`
- `MYSQL_PASSWORD` default `jobs_pass`
- `MYSQL_DATABASE` default `jobs_db`

### Flask / Updater
- `FLASK_SECRET_KEY` default `change-me`
- `UPDATE_INTERVAL_SEC` default `1`
- `DESCRIPTION_BACKFILL_BATCH` default `1`

### Notification Defaults
- `NOTIFY_TO_EMAIL` optional default recipient
- `NOTIFY_MAX_AGE_HOURS` default `24`

### SMTP
- `SMTP_HOST` required for send (else flash: `SMTP_HOST is not configured.`)
- `SMTP_PORT` default `587`
- `SMTP_USER` optional
- `SMTP_PASSWORD` optional
- `SMTP_FROM` optional fallback to `SMTP_USER`
- `SMTP_STARTTLS` default `1` (`0/false/no` disables TLS)
- `SMTP_SSL` optional (`1` uses SSL transport, must not be combined with STARTTLS)
- `SMTP_TIMEOUT` default `30` seconds

## Email Digest Notes (2026-03-28)
- Rendering split into dedicated helpers in `web_app.py`:
  - `build_jobs_email_text(...)`
  - `build_jobs_email_html(...)`
- HTML digest includes:
  - Branded header block
  - Summary cards (criteria window + matched count)
  - Styled per-job rows with direct listing links
  - Truncation hint if digest exceeds max listed rows
- Defensive sanitization for digest fields:
  - Text fields collapsed and normalized via `email_safe_text(...)`
  - URL links restricted to `http://` or `https://` via `email_safe_url(...)`
- `send_jobs_email(...)` now calls:
  - `msg.set_content(text_body)`
  - `msg.add_alternative(html_body, subtype="html")`
- Unit test coverage:
  - Existing SMTP tests still pass.
  - Added assertions in `tests/test_web_filters.py` to verify multipart content type and HTML body markers.
  - Last run: `.venv/bin/python -m unittest tests/test_web_filters.py` -> `Ran 18 tests ... OK`.

## Local SMTP Testing
- Port `1025` was free when checked; no local SMTP server was running.
- Quick local test server:
  - `python -m aiosmtpd -n -l 127.0.0.1:1025`
- Suggested env for local test:
  - `SMTP_HOST=127.0.0.1`
  - `SMTP_PORT=1025`
  - `SMTP_STARTTLS=0`
  - `SMTP_FROM=test@local`

## Known Issues / Tradeoffs
1. Updater is first-page-only for listings and does not crawl all pagination.
2. Description parsing depends on current Profesia page structure; section extraction may miss atypical ad layouts.
3. No auth on send endpoint; local/dev assumption.

## Suggested Next Work
1. Add optional manual "full crawl" mode (pagination with throttling) for broader coverage.
2. Add explicit UI empty-state row/message when filtered result set is zero.
3. Add SMTP test endpoint/button (send test mail without job digest).
4. Consider persisting parsed sections in DB for faster section filtering at scale.
5. Add optional digest theme presets (compact, detailed, dark-light neutral) configurable from UI.
6. Add optional include/exclude fields for digest rows (salary, location, posted age, link-only mode).

## Quick Run
```bash
cd /Users/admin/job-monitor
source .venv/bin/activate
python -m src.web_app
```
- App URL: `http://127.0.0.1:5000`

## Recent Changes (2026-03-28)
- Initialized git repository and pushed `main`.
- Expanded `README.md` with overview, features, architecture, and quick-start instructions.
- Renamed GitHub repository from `CVsender` to `job-monitor` (private).
- Renamed local folder from `/Users/admin/CVsender` to `/Users/admin/job-monitor`.
- Updated project branding from `CVsender` to `Job Monitor`.

## UI Notes (2026-03-28)
- Header/hero at top was redesigned:
  - Main centered title: `Job Monitor`.
  - Quote moved under title with emphasized lead words `Kto` and `Pre`.
  - Color theme changed to blue gradient to align with primary button color (`--accent: #1462ff`).
- Title quote was replaced with a longer multi-line Slovak verse and later corrected (`toho` -> `tomu`).
- Final sentence in the quote is forced to one row on desktop via `.fancy-line-one-row` with mobile wrap fallback.
- `Send Notifications` form was moved below the jobs table (bottom of page).
- Runtime compatibility fix: created symlink `/Users/admin/CVsender -> /Users/admin/job-monitor` to avoid `TemplateNotFound: index.html` when old path is used.
- README screenshot still uses `assets/job-monitor.png`; image was refreshed from a local screenshot.
