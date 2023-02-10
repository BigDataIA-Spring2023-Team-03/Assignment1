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

