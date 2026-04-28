import unittest
from pathlib import Path
import sys


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from sql_lineage_audit import extract_edges_from_sql, extract_sources, scan_sql_root, strip_comments


class SqlLineageAuditTests(unittest.TestCase):
    def test_strip_comments_removes_block_and_line_comments(self):
        sql = "SELECT * FROM a.table_one -- ignore b.table_two\n/* FROM c.table_three */"

        clean = strip_comments(sql)

        self.assertIn("a.table_one", clean)
        self.assertNotIn("b.table_two", clean)
        self.assertNotIn("c.table_three", clean)

    def test_extract_sources_ignores_ctes(self):
        sql = """
        CREATE TABLE mart.output_table AS
        WITH staged AS (
            SELECT * FROM raw.input_table
        )
        SELECT * FROM staged JOIN reference.lookup_table ON 1 = 1
        """

        self.assertEqual(extract_sources(sql), {"raw.input_table", "reference.lookup_table"})

    def test_extract_edges_from_insert_statement(self):
        sql = """
        INSERT INTO mart.output_table
        SELECT * FROM work.staged_table JOIN reference.lookup_table ON 1 = 1
        """

        edges = extract_edges_from_sql(sql, "publish.sql")

        self.assertEqual(
            {(edge.source_table, edge.target_table) for edge in edges},
            {
                ("work.staged_table", "mart.output_table"),
                ("reference.lookup_table", "mart.output_table"),
            },
        )

    def test_scan_sql_root_scans_multiple_files(self):
        edges = scan_sql_root(PROJECT_ROOT / "sample_sql")

        self.assertEqual(len(edges), 3)
        self.assertEqual(
            {(edge.source_table, edge.target_table) for edge in edges},
            {
                ("raw.entity_events", "work.entity_metrics_daily"),
                ("reference.entity_status", "work.entity_metrics_daily"),
                ("work.entity_metrics_daily", "mart.entity_metrics_daily"),
            },
        )


if __name__ == "__main__":
    unittest.main()
