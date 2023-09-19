import src.objects.models as models
import write as dbwrite
import read as dbread
import sqlite3
import unittest

test_db = "tests/test.db"


def run_schema_migration(conn: sqlite3.Connection):
    cursor = conn.cursor()
    with open('db/migrations/database_schema.sql', 'r') as file:
        sql = file.read()
        cursor.executescript(sql)
        conn.commit()


class TestHelper(unittest.TestCase):
    def test_dblock_from_rows(self):
        row_list = [{
            "data_id": 1,
            "type": "timeseries",
            "source": "Kolada",
            "source_id": "A343434",
            "tags": "A;B",
            "title": "a",
            "description": "test_description",
            "integration_id": 1,
            "var_labels": "Kön",
            "geo_groups": "C",
            "meta": {"a": "b"}}]
        got = dbread.dblock_from_rows(row_list)
        want = [models.NormalisedDataBlock(**{
            "data_id": 1,
            "type": "timeseries",
            "source": "Kolada",
            "source_id": "A343434",
            "tags": "A;B",
            "title": "a",
            "description": "test_description",
            "integration_id": 1,
            "var_labels": "Kön",
            "geo_groups": "C",
            "meta": {"a": "b"}
        })]
        self.assertEqual(got, want)

    def test_apply_filters(self):
        q = "SELECT * FROM table WHERE"
        filters = {"source": "Kolada", "a": "b", "tags": "test"}
        want = "SELECT * FROM table WHERE source = 'Kolada' AND a = 'b' AND tags LIKE '%test%'"
        got = dbread.apply_filters(q=q, **filters)
        self.assertEqual(got, want)


class TestDbRead(unittest.TestCase):
    def test_get_all_tags(self):
        # Prepare
        run_schema_migration(sqlite3.connect(test_db))

        conn = dbwrite.Writer(test_db)
        blocks = [models.SourceDataBlock(**{
            "type": "timeseries",
            "source": "Kolada",
            "source_id": "A343434",
            "tags": "A;B",
            "title": "a",
            "description": "test_description",
            "integration_id": 1,
            "geo_groups": "C",
            "var_labels": "Kön"})]
        conn.upsert_datablocks(blocks)

        conn = dbread.Reader(test_db)
        # Do
        got = conn.get_all_tags()

        # Check
        want = {"A": 1, "B": 1}
        self.assertEqual(got, want)

    def test_datablocks_by_search(self):
        run_schema_migration(sqlite3.connect(test_db))
        conn = dbwrite.Writer(test_db)
        blocks = [models.SourceDataBlock(**{
            "type": "timeseries",
            "source": "Kolada",
            "source_id": "A343434",
            "tags": "A;B",
            "title": "a",
            "description": "test_description",
            "integration_id": 1,
            "geo_groups": "C",
            "var_labels": "Kön",
            "meta": None})]
        conn.upsert_datablocks(blocks)

        conn = dbread.Reader(test_db)
        got = conn.get_datablocks_by_search(term="a", filters={"geo_groups": "C", "source": "Kolada"})
        want = [models.NormalisedDataBlock(**{
            "type": "timeseries",
            "source": "Kolada",
            "source_id": "A343434",
            "tags": "A;B",
            "title": "a",
            "description": "test_description",
            "integration_id": 1,
            "geo_groups": "C",
            "data_id": 1,
            "var_labels": "Kön",
            "meta": None})]
        self.assertEqual(got, want)


if __name__ == '__main__':
    unittest.main()
