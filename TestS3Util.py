import unittest
from Util.S3Util import S3Util

class TestS3Util(unittest.TestCase):

    def test_get_url(self):
        resource = 's3'
        bucket_name = 'noaa-goes18'
        prefix = ['ABI-L1b-RadC','2023', '005', '02', 'OR_ABI-L1b-RadC-M6C01_G18_s20230050201176_e20230050203554_c20230050203587.nc']
        actual = 'https://noaa-goes18.s3.amazonaws.com/ABI-L1b-RadC/2023/005/02/OR_ABI-L1b-RadC-M6C01_G18_s20230050201176_e20230050203554_c20230050203587.nc'
        util = S3Util(resource, bucket_name)
        expected = util.get_url(*prefix)

        self.assertEqual(actual, expected)

if __name__ == '__main__':
    unittest.main()