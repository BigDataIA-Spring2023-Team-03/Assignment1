import unittest
from SEVIRDataFetcher import filename_url_producer
import pandas as pd


class TestSevirDataFetcher(unittest.TestCase):

    def setUp(self):
        self.df = pd.read_excel('data/goes_classsheet.xlsx', sheet_name = ['GEOS - Datasets','NEXRAD-Datasets'])

    def test_geos_filename_producer(self):
        bucket_name = 'noaa-goes18'
        df = self.df.get('GEOS - Datasets')
        actual = df[['File name']][1:13].values.tolist()

        expected = df[['Full file name']][1:13].values.tolist()

        for i in range(len(actual)):
            actual_url = filename_url_producer(bucket_name, actual[i][0])
            self.assertEqual(actual_url, expected[i][0])

    def test_nexrad_filename_producer(self):
        bucket_name = 'noaa-nexrad-level2'
        df = self.df.get('NEXRAD-Datasets')
        actual = df[['File name']][1:13].values.tolist()
        expected = df[['Full file name']][1:13].values.tolist()
        for i in range(len(actual)):
            actual_url = filename_url_producer(bucket_name, actual[i][0])
            self.assertEqual(actual_url, expected[i][0])

if __name__ == '__main__':
    unittest.main()
