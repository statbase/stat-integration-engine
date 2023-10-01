from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from database import crud, schemas
from models import models
from database.scripts import scripts
import unittest


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
        # Testar
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


if __name__ == "__main__":
    unittest.main()
