import unittest
import pandas as pd
from pandas.testing import assert_frame_equal

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session

from database import crud, schemas
from database.scripts import scripts

from models import models


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
        self.block_list = [
            models.DataBlockBase(
                type="timeseries", source="Kolada", source_id="A343434",
                tags="A;B;C;Pear", title="Lund", description="test_description",
                integration_id=1, geo_groups="C", var_labels="Kön",
            ),
            models.DataBlockBase(
                type="timeseries", source="Kolada", source_id="B343434",
                tags="A;C", title="Malmö", description="test_description",
                integration_id=1, geo_groups="A", var_labels="Kön",
            ),
            models.DataBlockBase(
                type="map", source="SEB", source_id="C343434",
                tags="C;D;Pear;Apple", title="Göteborg", description="test_description",
                integration_id=1, geo_groups="B", var_labels="Man",
            ),
            models.DataBlockBase(
                type="timeseries", source="SEB", source_id="D343434",
                tags="X;Y;Z", title="Stockholm", description="custom_description",
                integration_id=1, geo_groups="D", var_labels="Kvinna",
            ),
            models.DataBlockBase(
                type="map", source="Kolada", source_id="E343434",
                tags="M;N;O;P", title="Västerås", description="another_description",
                integration_id=2, geo_groups="A", var_labels="Kvinna",
            ),
        ]

    def tearDown(self):
        # Remove all data from the test database and close the session
        self.session.remove()

    def test_get_no_tags(self):
        got = crud.get_tags(self.session())
        self.assertEqual(got, {})

    def test_get_all_tags(self):
        crud.upsert_datablocks(self.session(), self.block_list)
        got = crud.get_tags(self.session())
        want = {"A": 2, "B": 1, "C": 3, "D": 1, "Pear": 2, "Apple": 1,
                "X": 1, "Y": 1, "Z": 1, "M": 1, "N": 1, "O": 1, "P": 1}
        self.assertEqual(got, want)

    def test_datablocks_by_search(self):
        crud.upsert_datablocks(self.session(), self.block_list)
        got = crud.get_datablocks(db=self.session(), search_term="Pear")

        want = [
            models.DataBlock(
                type="timeseries", source="Kolada", source_id="A343434",
                tags="A;B;C;Pear", title="Lund", description="test_description",
                integration_id=1, geo_groups="C", var_labels="Kön", data_id=1, meta={}
            ),
            models.DataBlock(
                type="map", source="SEB", source_id="C343434",
                tags="C;D;Pear;Apple", title="Göteborg", description="test_description",
                integration_id=1, geo_groups="B", var_labels="Man", data_id=3, meta={}
            ),
        ]
        self.assertEqual(got, want)

    def test_datablocks_by_search_filters(self):
        crud.upsert_datablocks(self.session(), self.block_list)

        got = crud.get_datablocks(
            db=self.session(), search_term="Pear", source="Kolada", type="timeseries"
        )

        want = [
            models.DataBlock(
                **{
                    "type": "timeseries",
                    "source": "Kolada",
                    "source_id": "A343434",
                    "tags": "A;B;C;Pear",
                    "title": "Lund",
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
        df, missing_geo_ids = crud.get_timeseries(db=self.session(),
                                                  data_id=1,
                                                  geo_list=["TestGeo"])

        expected_df = pd.DataFrame({
            "ts_id": [1],
            "data_id": [1],
            "value": [123.456],
            "date": ["2023-01-01"],
            "variable": ["TestVar"],
            "geo_id": ["TestGeo"]
        })

        assert_frame_equal(df, expected_df)
        self.assertEqual(len(missing_geo_ids), 0)

    def test_large_data_insert(self):
        num_rows = 1000
        large_block_list = []
        for i in range(num_rows):  # Higher values can be used later
            block = models.DataBlockBase(
                type="timeseries", source="Kolada", source_id=f"X{i}",
                tags="A;B;C", title=f"a_{i}", description="test_description",
                integration_id=1, geo_groups="C", var_labels="Kön",
            )
            large_block_list.append(block)

        crud.upsert_datablocks(self.session(), large_block_list)
        got = crud.get_datablocks(db=self.session())
        self.assertEqual(len(got), num_rows)


if __name__ == "__main__":
    unittest.main()
