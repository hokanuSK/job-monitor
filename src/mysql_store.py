import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlsplit, urlunsplit

import pandas as pd
import pymysql
from pymysql.cursors import DictCursor


def extract_salary_bounds(salary_text: str) -> Tuple[Optional[float], Optional[float]]:
    if not salary_text:
        return None, None

    numbers: List[float] = []
    current = []
    for ch in salary_text:
        if ch.isdigit() or ch == " ":
            current.append(ch)
        elif current:
            token = "".join(current).replace(" ", "").strip()
            current = []
            if token.isdigit():
                value = float(token)
                if value > 0:
                    numbers.append(value)
    if current:
        token = "".join(current).replace(" ", "").strip()
        if token.isdigit():
            value = float(token)
            if value > 0:
                numbers.append(value)

    if not numbers:
        return None, None
    return min(numbers), max(numbers)


def normalize_job_url(url: str) -> str:
    value = (url or "").strip()
    if not value:
        return ""
    parts = urlsplit(value)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))


class MySQLJobStore:
    def __init__(self, host: str, port: int, user: str, password: str, database: str):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database

    @classmethod
    def from_env(cls) -> "MySQLJobStore":
        return cls(
            host=os.environ.get("MYSQL_HOST", "127.0.0.1"),
            port=int(os.environ.get("MYSQL_PORT", "3306")),
            user=os.environ.get("MYSQL_USER", "jobs_user"),
            password=os.environ.get("MYSQL_PASSWORD", "jobs_pass"),
            database=os.environ.get("MYSQL_DATABASE", "jobs_db"),
        )

    def _connect_server(self):
        return pymysql.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            charset="utf8mb4",
            cursorclass=DictCursor,
            autocommit=True,
        )

    def _connect_db(self):
        return pymysql.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database,
            charset="utf8mb4",
            cursorclass=DictCursor,
            autocommit=False,
        )

    def ensure_database_and_schema(self) -> None:
        with self._connect_server() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"CREATE DATABASE IF NOT EXISTS `{self.database}` "
                    "CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
                )

        with self._connect_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS jobs (
                        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
                        source_index INT NULL,
                        title VARCHAR(512) NOT NULL,
                        company VARCHAR(512) NOT NULL,
                        location VARCHAR(512) NOT NULL,
                        date_posted VARCHAR(128) NOT NULL,
                        url VARCHAR(700) NOT NULL,
                        salary VARCHAR(128) NOT NULL,
                        salary_low DOUBLE NULL,
                        salary_high DOUBLE NULL,
                        description MEDIUMTEXT NULL,
                        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        last_seen_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (id),
                        UNIQUE KEY uniq_jobs_url (url),
                        KEY idx_jobs_updated_at (updated_at),
                        KEY idx_jobs_company (company),
                        KEY idx_jobs_title (title)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                    """
                )
            conn.commit()

    def ping(self) -> bool:
        try:
            with self._connect_db() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
            return True
        except Exception:
            return False

    def upsert_jobs(self, jobs: List[Dict]) -> int:
        if not jobs:
            return 0

        rows = []
        for job in jobs:
            salary_text = str(job.get("salary", "") or "")
            salary_low, salary_high = extract_salary_bounds(salary_text)
            source_index = job.get("index")
            try:
                source_index = int(source_index) if str(source_index).strip() else None
            except ValueError:
                source_index = None

            rows.append(
                (
                    source_index,
                    str(job.get("title", "") or "")[:512],
                    str(job.get("company", "") or "")[:512],
                    str(job.get("location", "") or "")[:512],
                    str(job.get("date_posted", "") or "")[:128],
                    normalize_job_url(str(job.get("url", "") or ""))[:700],
                    salary_text[:128],
                    salary_low,
                    salary_high,
                    str(job.get("description", "") or ""),
                )
            )

        query = """
            INSERT INTO jobs (
                source_index, title, company, location, date_posted, url, salary,
                salary_low, salary_high, description, last_seen_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON DUPLICATE KEY UPDATE
                source_index = VALUES(source_index),
                title = VALUES(title),
                company = VALUES(company),
                location = VALUES(location),
                date_posted = VALUES(date_posted),
                salary = VALUES(salary),
                salary_low = VALUES(salary_low),
                salary_high = VALUES(salary_high),
                description = CASE
                    WHEN COALESCE(VALUES(description), '') <> '' THEN VALUES(description)
                    ELSE description
                END,
                last_seen_at = NOW()
        """

        with self._connect_db() as conn:
            with conn.cursor() as cur:
                cur.executemany(query, rows)
            conn.commit()
        return len(rows)

    def import_from_csv(self, csv_path: Path) -> int:
        if not csv_path.exists() or csv_path.stat().st_size == 0:
            return 0
        df = pd.read_csv(csv_path, dtype=str).fillna("")
        if df.empty:
            return 0
        jobs = df.to_dict(orient="records")
        return self.upsert_jobs(jobs)

    def load_jobs_dataframe(self) -> pd.DataFrame:
        query = """
            SELECT
                COALESCE(CAST(source_index AS CHAR), '') AS `index`,
                title,
                company,
                location,
                date_posted,
                salary,
                url,
                salary_low,
                salary_high,
                COALESCE(description, '') AS description
            FROM jobs
        """
        with self._connect_db() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                rows = cur.fetchall()
        if not rows:
            return pd.DataFrame(
                columns=[
                    "index",
                    "title",
                    "company",
                    "location",
                    "date_posted",
                    "salary",
                    "url",
                    "salary_low",
                    "salary_high",
                    "description",
                ]
            )
        return pd.DataFrame(rows).fillna("")

    def list_urls_missing_description(self, limit: int = 20) -> List[str]:
        if limit <= 0:
            return []

        query = """
            SELECT url
            FROM jobs
            WHERE description IS NULL OR TRIM(description) = ''
            ORDER BY RAND()
            LIMIT %s
        """
        with self._connect_db() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (int(limit),))
                rows = cur.fetchall()
        return [str(row.get("url", "")).strip() for row in rows if str(row.get("url", "")).strip()]

    def update_job_description(self, url: str, description: str) -> bool:
        normalized_url = normalize_job_url(url)
        normalized_description = str(description or "").strip()
        if not normalized_url or not normalized_description:
            return False

        query = "UPDATE jobs SET description = %s WHERE url = %s"
        with self._connect_db() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (normalized_description, normalized_url))
            conn.commit()
            return bool(cur.rowcount)

    def count_jobs(self) -> int:
        with self._connect_db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) AS cnt FROM jobs")
                row = cur.fetchone()
                return int(row["cnt"]) if row else 0
