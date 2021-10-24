import os
from dataclasses import dataclass, field

from .types import *

available = True
try:
    import boto3
except ModuleNotFoundError as args:
    available = False


def is_current() -> bool:
    return bool(os.environ.get("AWS_LAMBDA_FUNCTION_VERSION"))


@dataclass
class Storage:
    location_env_key: str
    bucket: str = field(default="")
    s3: Any = field(init=False)

    def __post_init__(self):
        assert available, "Please install boto3 for AWS: `python -m pip install boto3`"
        if not self.bucket:
            try:
                self.bucket = os.environ[self.location_env_key]
            except KeyError:
                self.bucket = self.location_env_key
        self.s3 = boto3.resource("s3").Bucket(self.bucket)

    def downloader(self, Key: str, Filename: str):
        self.s3.download_file(Key=Key, Filename=Filename)

    def uploader(self, Key: str, Filename: str):
        self.s3.upload_file(Key=Key, Filename=Filename)
