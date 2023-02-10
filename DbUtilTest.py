import os
import unittest
from Util.dbUtil import DbUtil

class TestDbUtil(unittest.TestCase):
    TEST_DB = 'test.db'

    def setUp(self):
        if os.path.exists(self.TEST_DB):
            os.remove(self.TEST_DB)
        self.util = DbUtil(self.TEST_DB)

    def tearDown(self):
        if os.path.exists(self.TEST_DB):
            os.remove(self.TEST_DB)

    def test_create_table(self):
        self.util.create_table("test_table", "level1 TEXT", "level2 TEXT")
        result = self.util.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='test_table'").fetchall()
        self.assertTrue(len(result) == 1)

    def test_insert(self):
        self.util.create_table("test_table", "level1 TEXT", "level2 TEXT")
        self.util.insert("test_table", ["level1", "level2"], [("product1", "year1"), ("product1", "year2")])
        result = self.util.filter("test_table", "level2", level1="product1")
        self.assertEqual(result, ["year1", "year2"])

    def test_filter(self):
        self.util.create_table("test_table", "level1 TEXT", "level2 TEXT")
        self.util.insert("test_table", ["level1", "level2"], [("product1", "year1"), ("product1", "year2")])
        result = self.util.filter("test_table", "level1", **{})
        self.assertEqual(result, ["product1"])

if __name__ == '__main__':
    unittest.main()
