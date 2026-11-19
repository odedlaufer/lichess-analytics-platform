"""S3 client for the object store (SeaweedFS), configured from the environment."""

import os

import boto3
from botocore.client import Config
from mypy_boto3_s3 import S3Client


def make_s3_client(
    access_key: str | None = None,
    secret_key: str | None = None,
) -> S3Client:
    return boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT"],
        aws_access_key_id=access_key or os.environ["S3_ACCESS_KEY"],
        aws_secret_access_key=secret_key or os.environ["S3_SECRET_KEY"],
        region_name="us-east-1",
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )
