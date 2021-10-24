from typing import Optional

from .aws import is_current as aws_current
from .aws import Storage as S3Storage
from .types import *


def new_storage(location_env_key: CLOUD_STORE,
                assume_aws: str = None) -> Optional[CloudStoreBase]:

    if assume_aws or aws_current():
        if aws_current():
            print("Running within AWS Lambda")
        print("Using AWS S3 storage")
        return S3Storage(location_env_key=location_env_key["s3"])


def in_cloud():
    return aws_current()


default_assume = "assume_aws"
