import unittest
import api.routes as routes
from fastapi import FastAPI
from fastapi.testclient import TestClient
from unittest.mock import patch

app = FastAPI()
app.include_router(routes.router, prefix="")


class TestHelpers(unittest.TestCase):
    def test_parse_filter(self):
        filter = "source=Kolada,tags=Test"
        got = routes.parse_datablock_filter(filter)
        want = {"source": "Kolada", "tags": "Test"}
        self.assertEqual(got, want)

    def test_parse_geo_ids(self):
        filter = "1401,1402"
        got = routes.parse_geo_ids(filter)
        want = ['1401', '1402']
        self.assertEqual(got, want)

    def test_parse_geo_ids_valueerr(self):
        filter = "1401,1233242"
        with self.assertRaises(ValueError):
            routes.parse_geo_ids(filter)


class TestRoutes(unittest.TestCase):
    @patch('api.routes.db_read')
    def test_get_tags(self, mock_db_read):
        mock_db_read.return_value = {"Kvalitet och resultat": 2383}
        client = TestClient(app)

        res = client.get("/tags")
        self.assertEqual(res.status_code, 200)

if __name__ == '__main__':
    unittest.main()
