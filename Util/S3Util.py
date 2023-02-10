import boto3
from botocore import UNSIGNED
from botocore.config import Config


class S3Util:
    def __init__(self, resource, bucket_name):
        self.s3 = boto3.client(resource, config=Config(signature_version=UNSIGNED))
        self.bucket_name = bucket_name

    def get_pages(self, prefix=''):
        paginator = self.s3.get_paginator('list_objects')
        return paginator.paginate(Bucket=self.bucket_name, Prefix=prefix)

    def get_url(self, *args):
        url = self.s3.generate_presigned_url(
            ClientMethod='get_object',
            Params={
                'Bucket': self.bucket_name,
                'Key': '/'.join(args)
            }
        )
        return url

if __name__ == '__main__':
    util = S3Util('s3','noaa-goes18')
    util.get_url('ABI-L1b-RadC','2023', '005', '02', 'OR_ABI-L1b-RadC-M6C01_G18_s20230050201176_e20230050203554_c20230050203587.nc')

