import src.models.models as models
import src.db.write as dbwrite
import src.db.read as dbread
import src.db.scripts.run_migration as mig
import os
import sqlite3
import unittest

test_db = "test.db"

def delete_db():
    if os.path.exists("test.db"):
        os.remove("test.db")

def delete_db():
    if os.path.exists("test.db"):
        os.remove("test.db")

class TestHelper(unittest.TestCase):
    def test_dblock_from_row_list(self):
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
            "meta": '{"a": "b"}'}]
        got = dbread.dblock_from_row_list(row_list)
        want = [models.NormalisedDataBlock(**{
            "data_id": 1,
            "type": "timeseries",
            "source": "Kolada",
            "source_id": "A343434",
            "tags": ['A', 'B'],
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
    def setUp(self):
        mig.run_schema_migration(sqlite3.connect(test_db))

    def tearDown(self):
        delete_db()


    def test_get_all_tags(self):
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
        got = conn.get_all_tags()
        want = {"A": 1, "B": 1}
        self.assertEqual(got, want)
        
    def test_datablocks_by_search(self):
        writer = dbwrite.Writer(test_db)
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
        writer.upsert_datablocks(blocks)

        reader = dbread.Reader(test_db)
        got = reader.get_datablocks_by_search(term="a", geo_groups='C', source='Kolada')
        want = [models.NormalisedDataBlock(**{
            "type": "timeseries",
            "source": "Kolada",
            "source_id": "A343434",
            "tags": ['A', 'B'],
            "title": "a",
            "description": "test_description",
            "integration_id": 1,
            "geo_groups": "C",
            "data_id": 1,
            "var_labels": "Kön",
            "meta": {}})]
        self.assertEqual(got, want)
"""

if __name__ == '__main__':
    unittest.main()
