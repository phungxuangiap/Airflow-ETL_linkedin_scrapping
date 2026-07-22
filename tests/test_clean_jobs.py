import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import duckdb

from src.jobs.silver import clean_jobs


class LocalDuckDBClient:
    """DuckDB client used by tests without loading remote/S3 extensions."""

    def __enter__(self):
        self.connection = duckdb.connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.connection.close()

    def fetch_arrow_table(self, query):
        return self.connection.execute(query).fetch_arrow_table()


class CleanScrappedSourceJobsTest(unittest.TestCase):
    def setUp(self):
        self.temp_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_directory.cleanup)
        self.bronze_path = Path(self.temp_directory.name)

    def run_cleaner(self, load_date, content):
        partition = self.bronze_path / f"load_date={load_date}"
        partition.mkdir()
        (partition / "jobs.jsonl").write_text(content, encoding="utf-8")

        with (
            patch.object(clean_jobs, "DuckDBClient", LocalDuckDBClient),
            patch.object(clean_jobs, "BRONZE_CRAWLER_DATA_PATH", str(self.bronze_path)),
        ):
            return clean_jobs.clean_scrapped_source_jobs(load_date)

    def test_empty_jsonl_returns_empty_tables(self):
        result = self.run_cleaner("empty", "")

        self.assertEqual(result["jobs"].num_rows, 0)
        self.assertEqual(result["companies"].num_rows, 0)

    def test_missing_title_uses_unknown_fallback(self):
        content = json.dumps(
            {
                "company": "Acme",
                "company_location": "Hanoi",
                "source_name": "test",
            }
        )
        result = self.run_cleaner("missing-title", content + "\n")

        self.assertEqual(result["jobs"].column("title").to_pylist(), ["Unknown"])
        self.assertEqual(result["companies"].column("name").to_pylist(), ["Acme"])


if __name__ == "__main__":
    unittest.main()
