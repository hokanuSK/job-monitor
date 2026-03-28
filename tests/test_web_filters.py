import unittest
from unittest.mock import MagicMock, patch
from urllib.parse import parse_qs, urlparse

import pandas as pd

import web_app


def make_filters(**overrides):
    filters = {
        "search": "",
        "title": "",
        "company": "",
        "location": "",
        "date_posted": "",
        "salary_min": "",
        "salary_max": "",
        "section_education": "",
        "section_education_field": "",
        "section_languages": "",
        "section_other_knowledge": "",
        "section_practice_area": "",
        "section_years_experience": "",
        "section_personal_skills": "",
        "section_job_tasks": "",
        "remote_only": False,
        "limit": 50,
    }
    filters.update(overrides)
    return filters


def sample_jobs_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "index": "1",
                "title": "Python Backend Engineer",
                "company": "Acme",
                "location": "Bratislava - home office",
                "date_posted": "pred 2 hod",
                "salary": "2 000 - 3 500 EUR",
                "url": "https://example.com/O1",
                "salary_low": 2000.0,
                "salary_high": 3500.0,
                "description": (
                    "What will you do\n"
                    "Build backend APIs\n"
                    "Language skills\n"
                    "English - B2\n"
                    "Other knowledge\n"
                    "Python, AWS, Terraform\n"
                    "Education\n"
                    "University\n"
                    "Experience in position/area\n"
                    "DevOps\n"
                    "Years of experience\n"
                    "2"
                ),
            },
            {
                "index": "2",
                "title": "Java Developer",
                "company": "Beta",
                "location": "Bratislava",
                "date_posted": "pred 1 dnom",
                "salary": "1 800 - 2 200 EUR",
                "url": "https://example.com/O2",
                "salary_low": 1800.0,
                "salary_high": 2200.0,
                "description": (
                    "What will you do\n"
                    "Maintain Java services\n"
                    "Language skills\n"
                    "Slovak\n"
                    "Other knowledge\n"
                    "Java, Spring\n"
                    "Education\n"
                    "High school"
                ),
            },
            {
                "index": "3",
                "title": "Data Engineer",
                "company": "Gamma",
                "location": "Kosice - praca z domu",
                "date_posted": "dnes",
                "salary": "2 300 - 3 000 EUR",
                "url": "https://example.com/O3",
                "salary_low": 2300.0,
                "salary_high": 3000.0,
                "description": (
                    "What will you do\n"
                    "Design data pipelines airflow\n"
                    "Language skills\n"
                    "English\n"
                    "Other knowledge\n"
                    "Python, GCP\n"
                    "Field of study\n"
                    "Computer Science"
                ),
            },
            {
                "index": "4",
                "title": "QA Engineer",
                "company": "Delta",
                "location": "Trnava",
                "date_posted": "pred 3 dnom",
                "salary": "1 500 - 1 900 EUR",
                "url": "https://example.com/O4",
                "salary_low": 1500.0,
                "salary_high": 1900.0,
                "description": (
                    "What will you do\n"
                    "Manual QA testing\n"
                    "Language skills\n"
                    "English\n"
                    "Other knowledge\n"
                    "AWS, Selenium, PHP\n"
                    "Personality requirements and skills\n"
                    "Communication"
                ),
            },
        ]
    )


ALL_SAMPLE_TITLES = [
    "Python Backend Engineer",
    "Java Developer",
    "Data Engineer",
    "QA Engineer",
]


def email_jobs_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "title": "Python Backend Engineer",
                "company": "Acme",
                "location": "Bratislava - home office",
                "date_posted": "pred 2 hod",
                "salary": "2 000 - 3 500 EUR",
                "url": "https://example.com/O1",
            },
            {
                "title": "Data Engineer",
                "company": "Gamma",
                "location": "Kosice - praca z domu",
                "date_posted": "dnes",
                "salary": "2 300 - 3 000 EUR",
                "url": "https://example.com/O3",
            },
        ]
    )


class WebFilterUnitTests(unittest.TestCase):
    def build_filtered(self, **filter_overrides):
        filters = make_filters(**filter_overrides)
        with patch("web_app.load_jobs_from_database", return_value=sample_jobs_df()):
            return web_app.build_filtered_jobs_df(filters)

    def test_parse_filter_terms_normalizes_and_deduplicates(self):
        terms = web_app.parse_filter_terms(" AWS;kubernetes,\naws, terraform ")
        self.assertEqual(terms, ["aws", "kubernetes", "terraform"])

    def test_read_filters_clamps_limit_and_parses_remote_flag(self):
        filters = web_app.read_filters({"title": "python", "limit": "5001", "remote_only": "1"})
        self.assertEqual(filters["title"], "python")
        self.assertEqual(filters["limit"], 1000)
        self.assertTrue(filters["remote_only"])

        fallback = web_app.read_filters({"limit": "bad"})
        self.assertEqual(fallback["limit"], 50)

    def test_filters_to_query_keeps_only_active_values(self):
        query = web_app.filters_to_query(make_filters(title="python", remote_only=True, limit=20))
        self.assertEqual(
            query,
            {
                "title": "python",
                "limit": "20",
                "remote_only": "1",
            },
        )

    def test_build_filtered_jobs_df_applies_remote_and_search(self):
        filtered = self.build_filtered(search="airflow", remote_only=True)
        self.assertEqual(filtered["title"].tolist(), ["Data Engineer"])

    def test_build_filtered_jobs_df_applies_section_filters(self):
        filtered = self.build_filtered(
            section_languages="english",
            section_other_knowledge="terraform",
        )
        self.assertEqual(filtered["title"].tolist(), ["Python Backend Engineer"])

    def test_build_filtered_jobs_df_applies_salary_filters(self):
        min_filtered = self.build_filtered(salary_min="3000")
        self.assertEqual(min_filtered["title"].tolist(), ["Python Backend Engineer", "Data Engineer"])

        max_filtered = self.build_filtered(salary_max="2000")
        self.assertEqual(
            max_filtered["title"].tolist(),
            ["Python Backend Engineer", "Java Developer", "QA Engineer"],
        )


class WebFilterRouteTests(unittest.TestCase):
    @staticmethod
    def _render_index(query: dict) -> str:
        client = web_app.app.test_client()
        with (
            patch("web_app.ensure_updater_started"),
            patch(
                "web_app.load_settings",
                return_value={"recipient_email": "", "notification_max_age_hours": "24"},
            ),
            patch("web_app.load_jobs_from_database", return_value=sample_jobs_df()),
        ):
            response = client.get("/", query_string=query)

        if response.status_code != 200:
            raise AssertionError(f"Expected HTTP 200, got {response.status_code}")
        return response.get_data(as_text=True)

    def assert_visible_titles(self, html: str, expected_titles):
        expected = set(expected_titles)
        for title in ALL_SAMPLE_TITLES:
            if title in expected:
                self.assertIn(title, html)
            else:
                self.assertNotIn(title, html)

    def test_apply_route_redirects_with_filter_query(self):
        client = web_app.app.test_client()
        with patch("web_app.ensure_updater_started"):
            response = client.post(
                "/apply",
                data={"title": "Python", "limit": "20", "remote_only": "1"},
            )

        self.assertEqual(response.status_code, 302)
        query = parse_qs(urlparse(response.headers["Location"]).query)
        self.assertEqual(query["title"], ["Python"])
        self.assertEqual(query["limit"], ["20"])
        self.assertEqual(query["remote_only"], ["1"])

    def test_apply_route_preserves_all_user_filter_inputs(self):
        client = web_app.app.test_client()
        payload = {
            "search": "airflow",
            "title": "Engineer",
            "company": "Gamma",
            "location": "kosice",
            "date_posted": "dnes",
            "salary_min": "2000",
            "salary_max": "3000",
            "section_education": "university",
            "section_education_field": "computer science",
            "section_languages": "english",
            "section_other_knowledge": "python,gcp",
            "section_practice_area": "devops",
            "section_years_experience": "2",
            "section_personal_skills": "communication",
            "section_job_tasks": "pipelines",
            "remote_only": "1",
            "limit": "9999",
        }
        with patch("web_app.ensure_updater_started"):
            response = client.post("/apply", data=payload)

        self.assertEqual(response.status_code, 302)
        query = parse_qs(urlparse(response.headers["Location"]).query)
        for key, value in payload.items():
            if key == "limit":
                self.assertEqual(query[key], ["1000"])
            else:
                self.assertEqual(query[key], [value])

    def test_index_route_renders_filtered_rows(self):
        html = self._render_index({"title": "Python", "limit": "10"})
        self.assertIn("Python Backend Engineer", html)
        self.assertNotIn("Java Developer", html)

    def test_index_route_supports_user_input_for_each_filter_field(self):
        cases = [
            ({"search": "AIRFLOW"}, ["Data Engineer"]),
            ({"title": "backend"}, ["Python Backend Engineer"]),
            ({"company": "beta"}, ["Java Developer"]),
            ({"location": "praca z domu"}, ["Data Engineer"]),
            ({"date_posted": "1 dnom"}, ["Java Developer"]),
            ({"salary_min": "3000"}, ["Python Backend Engineer", "Data Engineer"]),
            ({"salary_max": "1900"}, ["Java Developer", "QA Engineer"]),
            ({"section_education": "university"}, ["Python Backend Engineer"]),
            ({"section_education_field": "computer science"}, ["Data Engineer"]),
            ({"section_languages": "slovak"}, ["Java Developer"]),
            ({"section_other_knowledge": "selenium"}, ["QA Engineer"]),
            ({"section_practice_area": "devops"}, ["Python Backend Engineer"]),
            ({"section_years_experience": "2"}, ["Python Backend Engineer"]),
            ({"section_personal_skills": "communication"}, ["QA Engineer"]),
            ({"section_job_tasks": "backend apis"}, ["Python Backend Engineer"]),
            ({"remote_only": "1"}, ["Python Backend Engineer", "Data Engineer"]),
        ]

        for query, expected_titles in cases:
            with self.subTest(query=query):
                html = self._render_index({**query, "limit": "10"})
                self.assert_visible_titles(html, expected_titles)

    def test_index_route_accepts_multi_term_section_input(self):
        html = self._render_index({"section_other_knowledge": "python;terraform", "limit": "10"})
        self.assert_visible_titles(html, ["Python Backend Engineer"])

    def test_index_route_enforces_limit_and_handles_invalid_limit(self):
        limited_html = self._render_index({"title": "engineer", "limit": "1"})
        self.assert_visible_titles(limited_html, ["Python Backend Engineer"])

        invalid_limit_html = self._render_index({"title": "engineer", "limit": "bad"})
        self.assert_visible_titles(
            invalid_limit_html,
            ["Python Backend Engineer", "Data Engineer", "QA Engineer"],
        )


class SmtpUnitTests(unittest.TestCase):
    def test_read_smtp_settings_keeps_existing_password_when_blank(self):
        existing = {
            "smtp_host": "smtp.gmail.com",
            "smtp_port": "587",
            "smtp_user": "user@gmail.com",
            "smtp_password": "saved-app-password",
            "smtp_from": "user@gmail.com",
            "smtp_starttls": "1",
            "smtp_ssl": "0",
            "smtp_timeout": "30",
        }
        form_values = {
            "smtp_host": "smtp.gmail.com",
            "smtp_port": "587",
            "smtp_user": "user@gmail.com",
            "smtp_password": "",
            "smtp_from": "user@gmail.com",
            "smtp_timeout": "45",
            "smtp_starttls": "1",
        }

        parsed = web_app.read_smtp_settings(form_values, existing)
        self.assertEqual(parsed["smtp_password"], "saved-app-password")
        self.assertEqual(parsed["smtp_timeout"], "45")
        self.assertEqual(parsed["smtp_starttls"], "1")
        self.assertEqual(parsed["smtp_ssl"], "0")

    def test_send_jobs_email_supports_form_smtp_config(self):
        smtp_cls = MagicMock()
        smtp_client = MagicMock()
        smtp_client.has_extn.return_value = True
        smtp_cls.return_value.__enter__.return_value = smtp_client

        smtp_config = {
            "smtp_host": "smtp.example.com",
            "smtp_port": "587",
            "smtp_user": "smtp-user",
            "smtp_password": "smtp-pass",
            "smtp_from": "sender@example.com",
            "smtp_starttls": "1",
            "smtp_ssl": "0",
            "smtp_timeout": "30",
        }

        with (
            patch.dict("os.environ", {}, clear=True),
            patch("web_app.smtplib.SMTP", smtp_cls),
        ):
            ok, message = web_app.send_jobs_email(
                "receiver@example.com",
                24,
                email_jobs_df(),
                smtp_config=smtp_config,
            )

        self.assertTrue(ok)
        self.assertIn("Notification sent to receiver@example.com", message)
        smtp_cls.assert_called_once_with(host="smtp.example.com", port=587, timeout=30)
        smtp_client.starttls.assert_called_once()
        smtp_client.login.assert_called_once_with("smtp-user", "smtp-pass")

    def test_send_jobs_email_rejects_missing_host(self):
        with patch.dict("os.environ", {"SMTP_FROM": "sender@example.com"}, clear=True):
            ok, message = web_app.send_jobs_email(
                "receiver@example.com",
                24,
                email_jobs_df(),
            )

        self.assertFalse(ok)
        self.assertEqual(message, "SMTP_HOST is not configured.")

    def test_send_jobs_email_fails_when_starttls_not_supported(self):
        smtp_cls = MagicMock()
        smtp_client = MagicMock()
        smtp_client.has_extn.return_value = False
        smtp_cls.return_value.__enter__.return_value = smtp_client

        with (
            patch.dict(
                "os.environ",
                {
                    "SMTP_HOST": "smtp.example.com",
                    "SMTP_PORT": "587",
                    "SMTP_FROM": "sender@example.com",
                    "SMTP_STARTTLS": "1",
                    "SMTP_SSL": "0",
                },
                clear=True,
            ),
            patch("web_app.smtplib.SMTP", smtp_cls),
        ):
            ok, message = web_app.send_jobs_email("receiver@example.com", 24, email_jobs_df())

        self.assertFalse(ok)
        self.assertIn("does not advertise STARTTLS", message)
        smtp_client.starttls.assert_not_called()

    def test_send_jobs_email_sends_with_starttls_and_auth(self):
        smtp_cls = MagicMock()
        smtp_client = MagicMock()
        smtp_client.has_extn.return_value = True
        smtp_cls.return_value.__enter__.return_value = smtp_client

        with (
            patch.dict(
                "os.environ",
                {
                    "SMTP_HOST": "smtp.example.com",
                    "SMTP_PORT": "587",
                    "SMTP_USER": "smtp-user",
                    "SMTP_PASSWORD": "smtp-pass",
                    "SMTP_FROM": "sender@example.com",
                    "SMTP_STARTTLS": "1",
                    "SMTP_SSL": "0",
                },
                clear=True,
            ),
            patch("web_app.smtplib.SMTP", smtp_cls),
        ):
            ok, message = web_app.send_jobs_email("receiver@example.com", 24, email_jobs_df())

        self.assertTrue(ok)
        self.assertIn("Notification sent to receiver@example.com", message)
        smtp_cls.assert_called_once_with(host="smtp.example.com", port=587, timeout=30)
        smtp_client.ehlo.assert_called()
        smtp_client.starttls.assert_called_once()
        smtp_client.login.assert_called_once_with("smtp-user", "smtp-pass")
        smtp_client.send_message.assert_called_once()

        sent_message = smtp_client.send_message.call_args[0][0]
        self.assertEqual(sent_message["To"], "receiver@example.com")
        self.assertEqual(sent_message["From"], "sender@example.com")
        self.assertIn("2 jobs within 24h", sent_message["Subject"])

    def test_send_jobs_email_uses_ssl_transport(self):
        smtp_ssl_cls = MagicMock()
        smtp_ssl_client = MagicMock()
        smtp_ssl_cls.return_value.__enter__.return_value = smtp_ssl_client

        with (
            patch.dict(
                "os.environ",
                {
                    "SMTP_HOST": "smtp.example.com",
                    "SMTP_PORT": "465",
                    "SMTP_FROM": "sender@example.com",
                    "SMTP_SSL": "1",
                },
                clear=True,
            ),
            patch("web_app.smtplib.SMTP_SSL", smtp_ssl_cls),
            patch("web_app.smtplib.SMTP", MagicMock()) as smtp_plain_cls,
        ):
            ok, _ = web_app.send_jobs_email("receiver@example.com", 24, email_jobs_df())

        self.assertTrue(ok)
        smtp_ssl_cls.assert_called_once_with(host="smtp.example.com", port=465, timeout=30)
        smtp_plain_cls.assert_not_called()
        smtp_ssl_client.starttls.assert_not_called()


if __name__ == "__main__":
    unittest.main()
