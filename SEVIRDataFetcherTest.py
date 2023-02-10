import unittest
from SEVIRDataFetcher import filename_url_producer


class TestSevirDataFetcher(unittest.TestCase):

    def test_geos_filename_producer(self):
        bucket_name = 'noaa-goes18'
        filename = 'OR_ABI-L1b-RadC-M6C01_G18_s20230020101172_e20230020103548_c20230020103594.nc'
        expected = 'https://noaa-goes18.s3.amazonaws.com/ABI-L1b-RadC/2023/002/01/OR_ABI-L1b-RadC' \
                   '-M6C01_G18_s20230020101172_e20230020103548_c20230020103594.nc'

        actual = filename_url_producer(bucket_name, filename)
        self.assertEqual(expected, actual)

    def test_nexrad_filename_producer(self):
        bucket_name = 'nexrad-level2'
        filename = 'KABR20150820_222521_V06'
        expected = 'https://noaa-nexrad-level2.s3.amazonaws.com/2015/08/20/KABR/KABR20150820_222521_V06'

        actual = filename_url_producer(bucket_name, filename)
        self.assertEqual(expected, actual)


if __name__ == '__main__':
    unittest.main()
