from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from database import crud, schemas
from models import models
from database.scripts import scripts
import unittest
import pandas as pd
from pandas.testing import assert_frame_equal
import logging
import sys



def setup_test_db() -> scoped_session:
    # Create session
    engine = create_engine("sqlite:///:memory:")
    session_factory = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    session = scoped_session(session_factory)
    # Create tables
    schemas.metadata.create_all(bind=engine)
    # Upload geo
    df = scripts.get_geo_df()
    df.to_sql('geo_unit', engine, if_exists='replace', index=False)
    return session


class TestDbRead(unittest.TestCase):
    def setUp(self):
        self.session = setup_test_db()
        logging.basicConfig(filename='test_log.log', level=logging.DEBUG)

    def tearDown(self):
        # Remove all data from the test database and close the session
        self.session.remove()

    def test_get_all_tags(self):
        block_list = [
            models.DataBlockBase(
                **{
                    "type": "timeseries",
                    "source": "Kolada",
                    "source_id": "A343434",
                    "tags": "A;B",
                    "title": "a",
                    "description": "test_description",
                    "integration_id": 1,
                    "geo_groups": "C",
                    "var_labels": "Kön",
                }
            )
        ]
        crud.upsert_datablocks(self.session(), block_list)
        got = crud.get_tags(self.session())
        want = {"A": 1, "B": 1}
        self.assertEqual(got, want)

    def test_datablocks_by_search(self):
        block_list = [
            models.DataBlockBase(
                **{
                    "type": "timeseries",
                    "source": "Kolada",
                    "source_id": "A343434",
                    "tags": "A;B",
                    "title": "a",
                    "description": "test_description",
                    "integration_id": 1,
                    "geo_groups": "C",
                    "var_labels": "Kön",
                }
            )
        ]
        crud.upsert_datablocks(self.session(), block_list)
        
        got = crud.get_datablocks(db=self.session(), search_term="a")
        want = [
            models.DataBlock(
                **{
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
                    "meta": {},
                }
            )
        ]
        self.assertEqual(got, want)

    def test_datablocks_by_search_filters(self):
        block_list = [
            models.DataBlockBase(
                **{
                    "type": "timeseries",
                    "source": "Kolada",
                    "source_id": "A343434",
                    "tags": "A;B",
                    "title": "a",
                    "description": "test_description",
                    "integration_id": 1,
                    "geo_groups": "C",
                    "var_labels": "Kön",
                }
            )
        ]

        crud.upsert_datablocks(self.session(), block_list)

        got = crud.get_datablocks(
            db=self.session(), search_term="a", source="Kolada", type="timeseries"
        )
        want = [
            models.DataBlock(
                **{
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
                    "meta": {},
                }
            )
        ]
        self.assertEqual(got, want)

    def test_get_timeseries(self):
        crud.insert_timeseries(self.session(), pd.DataFrame({
            "data_id": [1],
            "value": [123.456],
            "date": ["2023-01-01"],
            "variable": ["TestVar"],
            "geo_id": ["TestGeo"]
        }))
        
        df, missing_geo_ids = crud.get_timeseries(db=self.session(), data_id=1, geo_list=["TestGeo"])
        #logging.info(f"df: {df}")

        expected_df = pd.DataFrame({
            "ts_id": [1],
            "data_id": [1],
            "value": [123.456],
            "date": ["2023-01-01"],
            "variable": ["TestVar"],
            "geo_id": ["TestGeo"]
        })

        #logging.info(f"expected_df: {expected_df}")

        assert_frame_equal(df, expected_df)
        self.assertEqual(len(missing_geo_ids), 0)


if __name__ == "__main__":
    unittest.main()
