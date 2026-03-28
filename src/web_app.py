import json
import os
import re
import smtplib
import threading
import unicodedata
from datetime import datetime
from email.message import EmailMessage
from email.utils import parseaddr
from functools import lru_cache
from html import escape, unescape
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
from flask import Flask, flash, redirect, render_template, request, url_for
from parsel import Selector

try:
    from .mysql_store import MySQLJobStore, normalize_job_url
except ImportError:
    # Allow direct execution (e.g. `python src/web_app.py`) without package context.
    from mysql_store import MySQLJobStore, normalize_job_url


BASE_DIR = Path(__file__).resolve().parent.parent
JOBS_PATH = BASE_DIR / "jobs.csv"
SETTINGS_PATH = BASE_DIR / "app_settings.json"
LISTING_URL = "https://www.profesia.sk/praca/"

DISPLAY_COLUMNS = [
    "index",
    "title",
    "company",
    "location",
    "date_posted",
    "salary",
    "url",
]

REMOTE_PATTERN = r"práca z domu|praca z domu|home office|remote"
UPDATE_INTERVAL_SEC = max(1, int(os.environ.get("UPDATE_INTERVAL_SEC", "1")))
SCRAPER_HEADERS = {"User-Agent": "Mozilla/5.0 (JobMonitor Local Updater)"}
DESCRIPTION_START_MARKERS = (
    "čo budeš robiť",
    "co budes robit",
    "požiadavky na zamestnanca",
    "pozadavky na zamestnanca",
    "informácie o pracovnom mieste",
    "informacie o pracovnom mieste",
    "job description",
)
DESCRIPTION_END_MARKERS = (
    "inzerujúca spoločnosť",
    "inzerujuca spolocnost",
    "stručná charakteristika spoločnosti",
    "strucna charakteristika spolocnosti",
    "company profile",
    "kontakt",
    "contact",
)
DESCRIPTION_CACHE_LIMIT = 5000
DESCRIPTION_BACKFILL_BATCH = max(0, int(os.environ.get("DESCRIPTION_BACKFILL_BATCH", "1")))
DESCRIPTION_SECTION_MARKERS = {
    "job_tasks": (
        "co budes robit",
        "what will you do",
    ),
    "education": (
        "pozicii vyhovuju uchadzaci so vzdelanim",
        "education",
    ),
    "education_field": (
        "vzdelanie v odbore",
        "field of study",
    ),
    "languages": (
        "jazykove znalosti",
        "language skills",
    ),
    "other_knowledge": (
        "ostatne znalosti",
        "other knowledge",
    ),
    "practice_area": (
        "prax na pozicii/v oblasti",
        "prax na pozicii / v oblasti",
        "experience in position/area",
    ),
    "years_experience": (
        "pocet rokov praxe",
        "years of experience",
    ),
    "personal_skills": (
        "osobnostne predpoklady a zrucnosti",
        "personality requirements and skills",
    ),
}
SECTION_FILTER_TO_SECTION_KEY = {
    "section_education": "education",
    "section_education_field": "education_field",
    "section_languages": "languages",
    "section_other_knowledge": "other_knowledge",
    "section_practice_area": "practice_area",
    "section_years_experience": "years_experience",
    "section_personal_skills": "personal_skills",
    "section_job_tasks": "job_tasks",
}

store = MySQLJobStore.from_env()
_description_cache: Dict[str, str] = {}

_db_lock = threading.Lock()
_db_ready = False
_db_error = ""

_updater_lock = threading.Lock()
_updater_started = False
_updater_stop_event = threading.Event()
_updater_status = "Updater has not run yet."
_updater_error = ""


app = Flask(__name__, template_folder=str(BASE_DIR / "templates"))
app.config["SECRET_KEY"] = os.environ.get("FLASK_SECRET_KEY", "change-me")

SMTP_SETTING_KEYS = (
    "smtp_host",
    "smtp_port",
    "smtp_user",
    "smtp_password",
    "smtp_from",
    "smtp_starttls",
    "smtp_ssl",
    "smtp_timeout",
)


def env_flag(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    normalized = raw.strip().lower()
    if not normalized:
        return default
    return normalized not in {"0", "false", "no", "off"}


def parse_flag_value(raw_value, default: bool) -> bool:
    if raw_value is None:
        return default
    if isinstance(raw_value, bool):
        return raw_value
    normalized = str(raw_value).strip().lower()
    if not normalized:
        return default
    return normalized not in {"0", "false", "no", "off"}


def default_smtp_settings() -> dict:
    smtp_ssl = env_flag("SMTP_SSL", False)
    return {
        "smtp_host": os.environ.get("SMTP_HOST", "").strip(),
        "smtp_port": os.environ.get("SMTP_PORT", "587").strip(),
        "smtp_user": os.environ.get("SMTP_USER", "").strip(),
        "smtp_password": os.environ.get("SMTP_PASSWORD", "").strip(),
        "smtp_from": os.environ.get("SMTP_FROM", "").strip(),
        "smtp_starttls": "1" if env_flag("SMTP_STARTTLS", not smtp_ssl) else "0",
        "smtp_ssl": "1" if smtp_ssl else "0",
        "smtp_timeout": os.environ.get("SMTP_TIMEOUT", "30").strip(),
    }


def normalize_smtp_settings(values: Optional[dict], fallback: Optional[dict] = None) -> dict:
    base = default_smtp_settings()
    if isinstance(fallback, dict):
        for key in SMTP_SETTING_KEYS:
            if key in fallback:
                base[key] = str(fallback.get(key, "")).strip()

    source = values if isinstance(values, dict) else {}
    smtp_ssl = parse_flag_value(source.get("smtp_ssl", base["smtp_ssl"]), False)
    smtp_starttls = parse_flag_value(
        source.get("smtp_starttls", base["smtp_starttls"]),
        not smtp_ssl,
    )

    return {
        "smtp_host": str(source.get("smtp_host", base["smtp_host"])).strip(),
        "smtp_port": str(source.get("smtp_port", base["smtp_port"])).strip(),
        "smtp_user": str(source.get("smtp_user", base["smtp_user"])).strip(),
        "smtp_password": str(source.get("smtp_password", base["smtp_password"])).strip(),
        "smtp_from": str(source.get("smtp_from", base["smtp_from"])).strip(),
        "smtp_starttls": "1" if smtp_starttls else "0",
        "smtp_ssl": "1" if smtp_ssl else "0",
        "smtp_timeout": str(source.get("smtp_timeout", base["smtp_timeout"])).strip(),
    }


def load_settings() -> dict:
    settings = {
        "recipient_email": os.environ.get("NOTIFY_TO_EMAIL", "").strip(),
        "notification_max_age_hours": str(
            os.environ.get("NOTIFY_MAX_AGE_HOURS", "24")
        ).strip(),
    }
    settings.update(default_smtp_settings())

    if not SETTINGS_PATH.exists():
        return settings

    try:
        data = json.loads(SETTINGS_PATH.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return settings

    if isinstance(data, dict):
        settings["recipient_email"] = str(data.get("recipient_email", "")).strip()
        settings["notification_max_age_hours"] = str(
            data.get(
                "notification_max_age_hours", settings["notification_max_age_hours"]
            )
        ).strip()
        settings.update(normalize_smtp_settings(data, settings))
    return settings


def save_settings(
    recipient_email: str,
    notification_max_age_hours: str,
    smtp_settings: Optional[dict] = None,
) -> dict:
    existing_settings = load_settings()
    settings = {
        "recipient_email": recipient_email.strip(),
        "notification_max_age_hours": str(notification_max_age_hours).strip(),
    }
    settings.update(normalize_smtp_settings(smtp_settings, existing_settings))
    SETTINGS_PATH.write_text(json.dumps(settings, indent=2), encoding="utf-8")
    return settings


def is_valid_email(email_value: str) -> bool:
    _, parsed = parseaddr(email_value)
    if not parsed:
        return False
    if "@" not in parsed:
        return False
    domain = parsed.rsplit("@", 1)[1]
    return "." in domain


def parse_numeric_filter(value: str) -> Optional[float]:
    if not value:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def parse_positive_hours(value: str) -> Optional[float]:
    parsed = parse_numeric_filter(value)
    if parsed is None or parsed <= 0:
        return None
    return parsed


def parse_positive_int(value: str) -> Optional[int]:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    if parsed <= 0:
        return None
    return parsed


def collapse_spaces(value: str) -> str:
    return re.sub(r"\s+", " ", unescape(value or "")).strip()


def parse_filter_terms(value: str) -> List[str]:
    terms: List[str] = []
    seen = set()
    for raw in re.split(r"[,\n;]+", value or ""):
        term = collapse_spaces(raw.lower())
        if not term or term in seen:
            continue
        seen.add(term)
        terms.append(term)
    return terms


def normalize_description_lines(lines: List[str]) -> str:
    cleaned: List[str] = []
    seen = set()
    for raw in lines:
        line = collapse_spaces(raw)
        if not line:
            continue
        key = line.lower()
        if key in seen:
            continue
        seen.add(key)
        cleaned.append(line)
    text = "\n".join(cleaned)
    if len(text) > 12000:
        return text[:12000].rstrip()
    return text


def normalize_for_match(value: str) -> str:
    collapsed = collapse_spaces(value).lower().strip()
    normalized = unicodedata.normalize("NFKD", collapsed)
    without_diacritics = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    return re.sub(r"\s+", " ", without_diacritics).strip(" :-")


def match_description_section(line: str) -> Tuple[Optional[str], bool]:
    normalized_line = normalize_for_match(line)
    if not normalized_line:
        return None, False

    for section_key, markers in DESCRIPTION_SECTION_MARKERS.items():
        for marker in markers:
            if normalized_line == marker:
                return section_key, False
            if normalized_line.startswith(f"{marker}:") or normalized_line.startswith(f"{marker} "):
                return section_key, True
    return None, False


@lru_cache(maxsize=20000)
def parse_description_sections_cached(description: str) -> Dict[str, str]:
    sections: Dict[str, List[str]] = {key: [] for key in DESCRIPTION_SECTION_MARKERS}
    current_section: Optional[str] = None

    for raw_line in (description or "").splitlines():
        line = collapse_spaces(raw_line)
        if not line:
            continue

        matched_section, heading_has_inline_content = match_description_section(line)
        if matched_section:
            current_section = matched_section
            if heading_has_inline_content:
                sections[current_section].append(line)
            continue

        if current_section:
            sections[current_section].append(line)

    return {
        key: " ".join(values).strip().lower()
        for key, values in sections.items()
    }


def iter_json_nodes(value):
    if isinstance(value, dict):
        yield value
        for nested in value.values():
            yield from iter_json_nodes(nested)
    elif isinstance(value, list):
        for nested in value:
            yield from iter_json_nodes(nested)


def extract_jobposting_description_from_ld_json(selector: Selector) -> str:
    script_nodes = selector.css("script[type='application/ld+json']::text").getall()
    for script_text in script_nodes:
        payload = (script_text or "").strip()
        if not payload:
            continue
        try:
            parsed = json.loads(payload)
        except json.JSONDecodeError:
            continue
        for node in iter_json_nodes(parsed):
            if not isinstance(node, dict):
                continue
            node_type = node.get("@type", "")
            if isinstance(node_type, list):
                normalized_types = {str(value).lower() for value in node_type}
            else:
                normalized_types = {str(node_type).lower()}
            if "jobposting" not in normalized_types:
                continue
            description = str(node.get("description", "") or "")
            if not description:
                continue
            description_selector = Selector(text=f"<div>{description}</div>")
            text_nodes = description_selector.css("div ::text").getall()
            extracted = normalize_description_lines(text_nodes)
            if extracted:
                return extracted
    return ""


def extract_job_description_from_html(html: str) -> str:
    selector = Selector(text=html)
    ld_json_text = extract_jobposting_description_from_ld_json(selector)
    if ld_json_text:
        return ld_json_text

    main = selector.css("main")
    text_nodes = main.css("::text").getall() if main else selector.css("body ::text").getall()
    lines: List[str] = []
    for node in text_nodes:
        normalized = collapse_spaces(node)
        if normalized:
            lines.append(normalized)
    if not lines:
        return ""

    lower_lines = [line.lower() for line in lines]
    start_idx = None
    for idx, line in enumerate(lower_lines):
        if any(marker in line for marker in DESCRIPTION_START_MARKERS):
            start_idx = idx
            break

    if start_idx is not None:
        lines = lines[start_idx:]
        lower_lines = lower_lines[start_idx:]
        for idx, line in enumerate(lower_lines):
            if idx == 0:
                continue
            if any(marker in line for marker in DESCRIPTION_END_MARKERS):
                lines = lines[:idx]
                break

    return normalize_description_lines(lines)


def fetch_job_description(session: requests.Session, job_url: str) -> str:
    cached = _description_cache.get(job_url)
    if cached:
        return cached

    try:
        response = session.get(job_url, timeout=20)
        response.raise_for_status()
    except Exception:
        return ""

    description = extract_job_description_from_html(response.text)
    if description:
        if len(_description_cache) >= DESCRIPTION_CACHE_LIMIT:
            _description_cache.pop(next(iter(_description_cache)))
        _description_cache[job_url] = description
    return description


def parse_posted_age_hours(date_posted: str) -> Optional[float]:
    text = (date_posted or "").strip().lower()
    if not text:
        return None

    if "dnes" in text or "today" in text:
        return 0.0
    if "včera" in text or "vcera" in text or "yesterday" in text:
        return 24.0

    date_match = re.search(r"(\d{1,2})\.(\d{1,2})\.(\d{4})", text)
    if date_match:
        try:
            posted_at = datetime.strptime(date_match.group(0), "%d.%m.%Y")
            now = datetime.now()
            delta_hours = (now - posted_at).total_seconds() / 3600.0
            return max(0.0, delta_hours)
        except ValueError:
            pass

    num_match = re.search(r"(\d+)", text)
    amount = int(num_match.group(1)) if num_match else 1

    if "min" in text:
        return amount / 60.0
    if "hod" in text or "hour" in text:
        return float(amount)
    if "dň" in text or "dni" in text or "dnom" in text or "day" in text:
        return float(amount * 24)
    if "týž" in text or "tyz" in text or "week" in text:
        return float(amount * 24 * 7)
    if "mesiac" in text or "month" in text:
        return float(amount * 24 * 30)

    return None


def filter_jobs_by_post_age(jobs_df: pd.DataFrame, max_age_hours: float) -> pd.DataFrame:
    if jobs_df.empty:
        return jobs_df

    filtered = jobs_df.copy()
    filtered["posted_age_hours"] = filtered["date_posted"].map(parse_posted_age_hours)
    filtered = filtered[
        (filtered["posted_age_hours"].notna()) & (filtered["posted_age_hours"] <= max_age_hours)
    ]
    filtered = filtered.sort_values(by=["posted_age_hours", "title"], na_position="last")
    return filtered


def email_safe_text(value: object, fallback: str = "N/A") -> str:
    text = collapse_spaces(str(value if value is not None else ""))
    return text if text else fallback


def email_safe_url(value: object) -> str:
    url = str(value if value is not None else "").strip()
    if url.startswith(("http://", "https://")):
        return url
    return ""


def build_jobs_email_text(
    listed_jobs: pd.DataFrame,
    total_jobs: int,
    max_age_hours: float,
    sent_at: str,
    max_list: int,
) -> str:
    lines = [
        "Job Monitor Notification",
        f"Sent at: {sent_at}",
        f"Criteria: jobs posted within last {max_age_hours:g} hours",
        f"Matched jobs: {total_jobs}",
        "",
    ]

    if listed_jobs.empty:
        lines.append("No jobs matched the selected age threshold.")
        return "\n".join(lines)

    for position, row in enumerate(listed_jobs.itertuples(index=False), start=1):
        title = email_safe_text(getattr(row, "title", ""), "Untitled role")
        company = email_safe_text(getattr(row, "company", ""))
        location = email_safe_text(getattr(row, "location", ""))
        posted = email_safe_text(getattr(row, "date_posted", ""), "Unknown")
        salary = email_safe_text(getattr(row, "salary", ""))
        url = email_safe_url(getattr(row, "url", ""))

        lines.append(f"{position}. {title}")
        lines.append(f"   Company: {company}")
        lines.append(f"   Location: {location}")
        lines.append(f"   Posted: {posted}")
        lines.append(f"   Salary: {salary}")
        if url:
            lines.append(f"   URL: {url}")
        lines.append("")

    if total_jobs > max_list:
        lines.append(f"...and {total_jobs - max_list} more jobs.")

    return "\n".join(lines)


def build_jobs_email_html(
    listed_jobs: pd.DataFrame,
    total_jobs: int,
    max_age_hours: float,
    sent_at: str,
    max_list: int,
) -> str:
    if listed_jobs.empty:
        rows_html = (
            '<tr><td style="padding:16px;color:#334155;font-size:14px;">'
            "No jobs matched the selected age threshold."
            "</td></tr>"
        )
    else:
        rows = []
        for position, row in enumerate(listed_jobs.itertuples(index=False), start=1):
            title = escape(email_safe_text(getattr(row, "title", ""), "Untitled role"))
            company = escape(email_safe_text(getattr(row, "company", "")))
            location = escape(email_safe_text(getattr(row, "location", "")))
            posted = escape(email_safe_text(getattr(row, "date_posted", ""), "Unknown"))
            salary = escape(email_safe_text(getattr(row, "salary", "")))
            url = email_safe_url(getattr(row, "url", ""))
            if url:
                action_html = (
                    f'<a href="{escape(url, quote=True)}" '
                    'style="color:#0f62fe;text-decoration:none;font-weight:600;">Open listing</a>'
                )
            else:
                action_html = '<span style="color:#64748b;">URL not available</span>'

            rows.append(
                "<tr>"
                '<td style="padding:14px 16px;border-bottom:1px solid #e4eaf3;">'
                f'<div style="font-size:15px;font-weight:700;color:#0f172a;">{position}. {title}</div>'
                f'<div style="margin-top:5px;font-size:13px;color:#334155;">{company} | {location}</div>'
                f'<div style="margin-top:5px;font-size:12px;color:#64748b;">Posted: {posted} | Salary: {salary}</div>'
                f'<div style="margin-top:8px;font-size:12px;">{action_html}</div>'
                "</td>"
                "</tr>"
            )
        rows_html = "".join(rows)

    extra_html = ""
    if total_jobs > max_list:
        extra_html = (
            f'<p style="margin:16px 0 0;color:#92400e;font-size:12px;">'
            f"...and {total_jobs - max_list} more jobs not shown in this digest."
            "</p>"
        )

    return (
        "<!doctype html>"
        '<html><body style="margin:0;padding:0;background:#eef3f9;font-family:Segoe UI,Arial,sans-serif;">'
        '<table role="presentation" width="100%" cellpadding="0" cellspacing="0" '
        'style="background:#eef3f9;padding:24px 12px;">'
        "<tr><td align=\"center\">"
        '<table role="presentation" width="680" cellpadding="0" cellspacing="0" '
        'style="max-width:680px;width:100%;background:#ffffff;border-radius:14px;overflow:hidden;'
        'border:1px solid #d9e2ef;">'
        '<tr><td style="padding:24px;background:linear-gradient(120deg,#0f172a,#1d4ed8);">'
        '<h1 style="margin:0;color:#ffffff;font-size:22px;">Job Monitor Notification</h1>'
        f'<p style="margin:8px 0 0;color:#dbeafe;font-size:13px;">Sent at: {escape(sent_at)}</p>'
        "</td></tr>"
        '<tr><td style="padding:18px 24px 12px 24px;">'
        '<table role="presentation" width="100%" cellpadding="0" cellspacing="0">'
        "<tr>"
        '<td style="padding:12px;background:#f8fafc;border:1px solid #e2e8f0;border-radius:10px;">'
        f'<div style="font-size:11px;color:#64748b;text-transform:uppercase;">Criteria</div>'
        f'<div style="margin-top:4px;font-size:14px;color:#0f172a;font-weight:600;">'
        f"Last {max_age_hours:g} hours</div>"
        "</td>"
        '<td style="width:12px;"></td>'
        '<td style="padding:12px;background:#f8fafc;border:1px solid #e2e8f0;border-radius:10px;">'
        f'<div style="font-size:11px;color:#64748b;text-transform:uppercase;">Matched Jobs</div>'
        f'<div style="margin-top:4px;font-size:14px;color:#0f172a;font-weight:600;">{total_jobs}</div>'
        "</td>"
        "</tr>"
        "</table>"
        "</td></tr>"
        '<tr><td style="padding:0 24px 24px 24px;">'
        '<table role="presentation" width="100%" cellpadding="0" cellspacing="0" '
        'style="border:1px solid #e2e8f0;border-radius:10px;overflow:hidden;background:#ffffff;">'
        f"{rows_html}"
        "</table>"
        f"{extra_html}"
        '<p style="margin:16px 0 0;color:#64748b;font-size:11px;">'
        "This digest reflects your currently applied filters in Job Monitor."
        "</p>"
        "</td></tr>"
        "</table>"
        "</td></tr>"
        "</table>"
        "</body></html>"
    )

def send_jobs_email(
    recipient_email: str,
    max_age_hours: float,
    jobs_df: pd.DataFrame,
    smtp_config: Optional[dict] = None,
) -> tuple:
    smtp_settings = normalize_smtp_settings(smtp_config, default_smtp_settings())
    smtp_host = smtp_settings["smtp_host"]
    smtp_port_text = smtp_settings["smtp_port"]
    smtp_user = smtp_settings["smtp_user"]
    smtp_password = smtp_settings["smtp_password"]
    smtp_from = smtp_settings["smtp_from"] or smtp_user
    smtp_ssl = parse_flag_value(smtp_settings["smtp_ssl"], False)
    smtp_starttls = parse_flag_value(smtp_settings["smtp_starttls"], not smtp_ssl)
    smtp_timeout = parse_positive_int(smtp_settings["smtp_timeout"])

    if not smtp_host:
        return False, "SMTP_HOST is not configured."
    smtp_port = parse_positive_int(smtp_port_text)
    if smtp_port is None:
        return False, f"Invalid SMTP_PORT value: {smtp_port_text!r}. Use a positive integer."
    if not smtp_from:
        return False, "SMTP_FROM or SMTP_USER must be configured."
    if smtp_ssl and smtp_starttls:
        return False, "SMTP_SSL and SMTP_STARTTLS cannot both be enabled."
    if smtp_timeout is None:
        return False, "SMTP_TIMEOUT must be a positive integer."
    if (smtp_user and not smtp_password) or (smtp_password and not smtp_user):
        return False, "Configure both SMTP_USER and SMTP_PASSWORD, or neither."

    max_list = 200
    listed_jobs = jobs_df.head(max_list)
    sent_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    text_body = build_jobs_email_text(
        listed_jobs=listed_jobs,
        total_jobs=len(jobs_df),
        max_age_hours=max_age_hours,
        sent_at=sent_at,
        max_list=max_list,
    )
    html_body = build_jobs_email_html(
        listed_jobs=listed_jobs,
        total_jobs=len(jobs_df),
        max_age_hours=max_age_hours,
        sent_at=sent_at,
        max_list=max_list,
    )

    msg = EmailMessage()
    msg["Subject"] = f"Job Monitor: {len(jobs_df)} jobs within {max_age_hours:g}h"
    msg["From"] = smtp_from
    msg["To"] = recipient_email
    msg.set_content(text_body)
    msg.add_alternative(html_body, subtype="html")

    try:
        smtp_cls = smtplib.SMTP_SSL if smtp_ssl else smtplib.SMTP
        with smtp_cls(host=smtp_host, port=smtp_port, timeout=smtp_timeout) as smtp:
            smtp.ehlo()
            if smtp_starttls:
                if not smtp.has_extn("starttls"):
                    return (
                        False,
                        "SMTP server does not advertise STARTTLS. Set SMTP_STARTTLS=0 "
                        "or use SMTP_SSL=1.",
                    )
                smtp.starttls()
                smtp.ehlo()
            if smtp_user and smtp_password:
                smtp.login(smtp_user, smtp_password)
            smtp.send_message(msg)
    except Exception as exc:
        return False, f"Failed to send email: {exc}"

    return (
        True,
        f"Notification sent to {recipient_email} for currently applied filters "
        f"({len(jobs_df)} jobs, max age {max_age_hours:g}h).",
    )


def read_filters(values) -> dict:
    filters = {
        "search": values.get("search", "").strip(),
        "title": values.get("title", "").strip(),
        "company": values.get("company", "").strip(),
        "location": values.get("location", "").strip(),
        "date_posted": values.get("date_posted", "").strip(),
        "salary_min": values.get("salary_min", "").strip(),
        "salary_max": values.get("salary_max", "").strip(),
        "section_education": values.get("section_education", "").strip(),
        "section_education_field": values.get("section_education_field", "").strip(),
        "section_languages": values.get("section_languages", "").strip(),
        "section_other_knowledge": values.get("section_other_knowledge", "").strip(),
        "section_practice_area": values.get("section_practice_area", "").strip(),
        "section_years_experience": values.get("section_years_experience", "").strip(),
        "section_personal_skills": values.get("section_personal_skills", "").strip(),
        "section_job_tasks": values.get("section_job_tasks", "").strip(),
        "remote_only": values.get("remote_only", "").strip() == "1",
    }
    try:
        limit = int(values.get("limit", "50"))
    except ValueError:
        limit = 50
    filters["limit"] = max(1, min(limit, 1000))
    return filters


def read_smtp_settings(values, existing_settings: dict) -> dict:
    submitted_password = str(values.get("smtp_password", ""))
    if submitted_password.strip():
        smtp_password = submitted_password
    else:
        smtp_password = str(existing_settings.get("smtp_password", ""))

    smtp_values = {
        "smtp_host": values.get("smtp_host", existing_settings.get("smtp_host", "")),
        "smtp_port": values.get("smtp_port", existing_settings.get("smtp_port", "587")),
        "smtp_user": values.get("smtp_user", existing_settings.get("smtp_user", "")),
        "smtp_password": smtp_password,
        "smtp_from": values.get("smtp_from", existing_settings.get("smtp_from", "")),
        "smtp_starttls": values.get("smtp_starttls", "0"),
        "smtp_ssl": values.get("smtp_ssl", "0"),
        "smtp_timeout": values.get("smtp_timeout", existing_settings.get("smtp_timeout", "30")),
    }
    return normalize_smtp_settings(smtp_values, existing_settings)


def filters_to_query(filters: dict) -> dict:
    query = {
        "search": filters["search"],
        "title": filters["title"],
        "company": filters["company"],
        "location": filters["location"],
        "date_posted": filters["date_posted"],
        "salary_min": filters["salary_min"],
        "salary_max": filters["salary_max"],
        "section_education": filters["section_education"],
        "section_education_field": filters["section_education_field"],
        "section_languages": filters["section_languages"],
        "section_other_knowledge": filters["section_other_knowledge"],
        "section_practice_area": filters["section_practice_area"],
        "section_years_experience": filters["section_years_experience"],
        "section_personal_skills": filters["section_personal_skills"],
        "section_job_tasks": filters["section_job_tasks"],
        "limit": str(filters["limit"]),
    }
    query = {k: v for k, v in query.items() if v}
    if filters["remote_only"]:
        query["remote_only"] = "1"
    return query


def scrape_first_page_jobs() -> list:
    with requests.Session() as session:
        session.headers.update(SCRAPER_HEADERS)
        response = session.get(LISTING_URL, timeout=20)
        response.raise_for_status()

        selector = Selector(text=response.text)
        jobs = []
        row_index = 0

        for row in selector.css("ul.list li.list-row"):
            href = row.css("h2 a::attr(href)").get()
            if not href:
                continue
            url = normalize_job_url(requests.compat.urljoin(LISTING_URL, href))
            if not url or "/praca/" not in url or "/O" not in url:
                continue

            row_index += 1

            salary_parts = row.css(
                'span.label-group a[data-dimension7="Salary label"] span.label::text'
            ).getall()
            salary = "".join(salary_parts).strip()
            description = fetch_job_description(session, url)

            jobs.append(
                {
                    "index": row_index,
                    "title": row.css("span.title::text").get(default="").strip(),
                    "company": row.css("span.employer::text").get(default="").strip(),
                    "location": row.css("span.job-location::text").get(default="").strip(),
                    "date_posted": row.css("div.list-footer span.info strong::text")
                    .get(default="")
                    .strip(),
                    "url": url,
                    "salary": salary,
                    "description": description,
                }
            )

        return jobs


def backfill_missing_descriptions(limit: int) -> int:
    if limit <= 0:
        return 0

    urls = store.list_urls_missing_description(limit=limit)
    if not urls:
        return 0

    updated = 0
    with requests.Session() as session:
        session.headers.update(SCRAPER_HEADERS)
        for url in urls:
            description = fetch_job_description(session, url)
            if not description:
                continue
            if store.update_job_description(url, description):
                updated += 1
    return updated


def ensure_database_ready(force: bool = False) -> bool:
    global _db_ready, _db_error, _updater_status

    with _db_lock:
        if _db_ready and not force:
            return True

        try:
            store.ensure_database_and_schema()
            existing = store.count_jobs()
            if existing == 0 and JOBS_PATH.exists() and JOBS_PATH.stat().st_size > 0:
                imported = store.import_from_csv(JOBS_PATH)
                _updater_status = f"Seeded MySQL with {imported} rows from jobs.csv."
            _db_ready = True
            _db_error = ""
            return True
        except Exception as exc:
            _db_ready = False
            _db_error = str(exc)
            return False


def load_jobs_from_database() -> pd.DataFrame:
    if not ensure_database_ready():
        cols = DISPLAY_COLUMNS + ["salary_low", "salary_high", "description"]
        return pd.DataFrame(columns=cols)

    df = store.load_jobs_dataframe().fillna("")

    for col in DISPLAY_COLUMNS:
        if col not in df.columns:
            df[col] = ""
    if "salary_low" not in df.columns:
        df["salary_low"] = ""
    if "salary_high" not in df.columns:
        df["salary_high"] = ""
    if "description" not in df.columns:
        df["description"] = ""

    df["salary_low"] = pd.to_numeric(df["salary_low"], errors="coerce")
    df["salary_high"] = pd.to_numeric(df["salary_high"], errors="coerce")

    df["index_num"] = pd.to_numeric(df["index"], errors="coerce")
    df = df.sort_values(by=["index_num", "title"], na_position="last").drop(columns=["index_num"])
    return df


def updater_loop() -> None:
    global _updater_status, _updater_error

    while not _updater_stop_event.is_set():
        try:
            if ensure_database_ready():
                latest_jobs = scrape_first_page_jobs()
                processed = store.upsert_jobs(latest_jobs)
                backfilled = backfill_missing_descriptions(DESCRIPTION_BACKFILL_BATCH)
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                _updater_status = (
                    f"Last update {timestamp}: processed {processed} listing rows "
                    f"(description backfill +{backfilled}, interval {UPDATE_INTERVAL_SEC}s)."
                )
                _updater_error = ""
        except Exception as exc:
            _updater_error = f"{type(exc).__name__}: {exc}"

        _updater_stop_event.wait(UPDATE_INTERVAL_SEC)


def ensure_updater_started() -> None:
    global _updater_started

    if _updater_started:
        return

    with _updater_lock:
        if _updater_started:
            return

        thread = threading.Thread(target=updater_loop, name="profesia-updater", daemon=True)
        thread.start()
        _updater_started = True


def build_filtered_jobs_df(filters: dict) -> pd.DataFrame:
    jobs_df = load_jobs_from_database()
    if jobs_df.empty:
        return jobs_df

    search = filters["search"]
    title = filters["title"]
    company = filters["company"]
    location = filters["location"]
    date_posted = filters["date_posted"]
    salary_min_text = filters["salary_min"]
    salary_max_text = filters["salary_max"]
    remote_only = filters["remote_only"]

    if search:
        query = search.lower()
        searchable = (
            jobs_df["title"].str.lower()
            + " "
            + jobs_df["company"].str.lower()
            + " "
            + jobs_df["location"].str.lower()
            + " "
            + jobs_df["date_posted"].str.lower()
            + " "
            + jobs_df["salary"].str.lower()
            + " "
            + jobs_df["description"].astype(str).str.lower()
        )
        jobs_df = jobs_df[searchable.str.contains(query, na=False, regex=False)]

    if title:
        jobs_df = jobs_df[
            jobs_df["title"].str.lower().str.contains(title.lower(), na=False, regex=False)
        ]
    if company:
        jobs_df = jobs_df[
            jobs_df["company"].str.lower().str.contains(company.lower(), na=False, regex=False)
        ]
    if location:
        jobs_df = jobs_df[
            jobs_df["location"].str.lower().str.contains(location.lower(), na=False, regex=False)
        ]
    if date_posted:
        jobs_df = jobs_df[
            jobs_df["date_posted"].str.lower().str.contains(date_posted.lower(), na=False, regex=False)
        ]
    if remote_only:
        jobs_df = jobs_df[
            jobs_df["location"].str.lower().str.contains(REMOTE_PATTERN, na=False, regex=True)
        ]

    salary_min = parse_numeric_filter(salary_min_text)
    salary_max = parse_numeric_filter(salary_max_text)
    if salary_min is not None and salary_min > 0:
        jobs_df = jobs_df[(jobs_df["salary_high"].notna()) & (jobs_df["salary_high"] >= salary_min)]
    if salary_max is not None and salary_max > 0:
        jobs_df = jobs_df[(jobs_df["salary_low"].notna()) & (jobs_df["salary_low"] <= salary_max)]

    section_filters_active = any(filters[key] for key in SECTION_FILTER_TO_SECTION_KEY)
    if section_filters_active:
        section_map_series = jobs_df["description"].astype(str).map(parse_description_sections_cached)
        for filter_key, section_key in SECTION_FILTER_TO_SECTION_KEY.items():
            terms = parse_filter_terms(filters[filter_key])
            if not terms:
                continue

            section_text_series = section_map_series.map(
                lambda section_map, key=section_key: section_map.get(key, "")
            )
            mask = pd.Series(True, index=jobs_df.index)
            for term in terms:
                mask = mask & section_text_series.str.contains(term, na=False, regex=False)

            jobs_df = jobs_df[mask]
            section_map_series = section_map_series.loc[jobs_df.index]

    return jobs_df


@app.route("/", methods=["GET"])
def index():
    ensure_updater_started()

    settings = load_settings()
    smtp_settings = normalize_smtp_settings(settings, settings)
    filters = read_filters(request.args)
    limit = filters["limit"]

    all_jobs_df = load_jobs_from_database()
    all_jobs_count = len(all_jobs_df)

    jobs_df = build_filtered_jobs_df(filters)
    visible_jobs_df = jobs_df.head(limit).copy() if not jobs_df.empty else jobs_df
    total_jobs = len(jobs_df)
    unique_companies = (
        int(
            jobs_df["company"]
            .astype(str)
            .str.strip()
            .replace("", pd.NA)
            .nunique(dropna=True)
        )
        if not jobs_df.empty
        else 0
    )
    filtered_jobs_last_hour = (
        int(jobs_df["date_posted"].map(parse_posted_age_hours).le(1.0).sum())
        if not jobs_df.empty
        else 0
    )

    jobs = (
        visible_jobs_df[DISPLAY_COLUMNS].to_dict(orient="records")
        if not visible_jobs_df.empty
        else []
    )

    return render_template(
        "index.html",
        jobs=jobs,
        total_jobs=total_jobs,
        unique_companies=unique_companies,
        filtered_jobs_last_hour=filtered_jobs_last_hour,
        all_jobs_count=all_jobs_count,
        recipient_email=str(settings.get("recipient_email", "")).strip(),
        notification_max_age_hours=str(settings.get("notification_max_age_hours", "24")).strip(),
        smtp_host=smtp_settings["smtp_host"],
        smtp_port=smtp_settings["smtp_port"],
        smtp_user=smtp_settings["smtp_user"],
        smtp_from=smtp_settings["smtp_from"],
        smtp_timeout=smtp_settings["smtp_timeout"],
        smtp_starttls=parse_flag_value(smtp_settings["smtp_starttls"], True),
        smtp_ssl=parse_flag_value(smtp_settings["smtp_ssl"], False),
        search=filters["search"],
        title=filters["title"],
        company=filters["company"],
        location=filters["location"],
        date_posted=filters["date_posted"],
        salary_min=filters["salary_min"],
        salary_max=filters["salary_max"],
        section_education=filters["section_education"],
        section_education_field=filters["section_education_field"],
        section_languages=filters["section_languages"],
        section_other_knowledge=filters["section_other_knowledge"],
        section_practice_area=filters["section_practice_area"],
        section_years_experience=filters["section_years_experience"],
        section_personal_skills=filters["section_personal_skills"],
        section_job_tasks=filters["section_job_tasks"],
        remote_only=filters["remote_only"],
        limit=limit,
        jobs_file_exists=JOBS_PATH.exists(),
        database_ready=_db_ready,
        database_error=_db_error,
        updater_status=_updater_status,
        updater_error=_updater_error,
    )


@app.route("/apply", methods=["POST"])
def apply_filters():
    ensure_updater_started()
    filters = read_filters(request.form)
    flash("Filters applied. Notifications use these currently applied filters.")
    return redirect(url_for("index", **filters_to_query(filters)))


@app.route("/save-smtp", methods=["POST"])
def save_smtp():
    ensure_updater_started()
    filters = read_filters(request.form)
    existing_settings = load_settings()
    smtp_settings = read_smtp_settings(request.form, existing_settings)
    recipient_email = str(existing_settings.get("recipient_email", "")).strip()
    max_age_text = str(existing_settings.get("notification_max_age_hours", "24")).strip()

    save_settings(recipient_email, max_age_text, smtp_settings)
    flash("SMTP configuration saved.")
    return redirect(url_for("index", **filters_to_query(filters)))


@app.route("/send-mails", methods=["POST"])
def send_mails():
    ensure_updater_started()
    filters = read_filters(request.form)
    existing_settings = load_settings()
    recipient_email = request.form.get("recipient_email", "").strip()
    max_age_text = request.form.get("notification_max_age_hours", "").strip()
    smtp_settings = normalize_smtp_settings(existing_settings, existing_settings)

    if not is_valid_email(recipient_email):
        flash("Invalid email format. Please provide a valid recipient email.")
        return redirect(url_for("index", **filters_to_query(filters)))

    max_age_hours = parse_positive_hours(max_age_text)
    if max_age_hours is None:
        flash("Invalid max age. Enter a positive number of hours.")
        return redirect(url_for("index", **filters_to_query(filters)))

    save_settings(recipient_email, max_age_text, smtp_settings)

    jobs_df = build_filtered_jobs_df(filters)
    jobs_df = filter_jobs_by_post_age(jobs_df, max_age_hours)

    ok, message = send_jobs_email(recipient_email, max_age_hours, jobs_df, smtp_settings)
    flash(message)
    return redirect(url_for("index", **filters_to_query(filters)))



if __name__ == "__main__":
    app.run(debug=True, host="127.0.0.1", port=5000)
