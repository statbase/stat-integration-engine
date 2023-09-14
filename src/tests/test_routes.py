import sys
import os
sys.path.append(os.getcwd()) #Seems super hacky but import wont work otherwise. Damnit Python!
import unittest
import api.routes as routes

class TestHelpers(unittest.TestCase):
    def test_parse_filter(self):
        filter = "source=Kolada,tags=Test"
        got = routes.parse_filter(filter)
        want = {"source":"Kolada", "tags":"Test"}
        self.assertEqual(got, want)

if __name__ == '__main__':
    unittest.main()