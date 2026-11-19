"""Object store (SeaweedFS S3 API) smoke tests. Requires `make up PROFILES=lake`."""

import os
import uuid
from collections.abc import Iterator

import boto3
import pytest
from botocore.client import Config
from botocore.exceptions import ClientError
from mypy_boto3_s3 import S3Client

pytestmark = pytest.mark.integration


def _client(access_key: str, secret_key: str) -> S3Client:
    return boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT"],
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


@pytest.fixture
def s3() -> S3Client:
    return _client(os.environ["S3_ACCESS_KEY"], os.environ["S3_SECRET_KEY"])


@pytest.fixture
def bucket() -> str:
    return os.environ["LAKE_BUCKET"]


@pytest.fixture
def key(s3: S3Client, bucket: str) -> Iterator[str]:
    k = f"_smoke/{uuid.uuid4()}.txt"
    yield k
    s3.delete_object(Bucket=bucket, Key=k)


def test_lake_bucket_exists(s3: S3Client, bucket: str) -> None:
    names = [b["Name"] for b in s3.list_buckets()["Buckets"]]
    assert bucket in names


def test_put_get_list_roundtrip(s3: S3Client, bucket: str, key: str) -> None:
    body = b"1. e4 e5 2. Nf3 Nc6"
    s3.put_object(Bucket=bucket, Key=key, Body=body)

    assert s3.get_object(Bucket=bucket, Key=key)["Body"].read() == body
    listed = s3.list_objects_v2(Bucket=bucket, Prefix="_smoke/")
    assert key in [o["Key"] for o in listed.get("Contents", [])]


def test_wrong_credentials_rejected(bucket: str) -> None:
    bad = _client(os.environ["S3_ACCESS_KEY"], "wrong-secret")
    with pytest.raises(ClientError):
        bad.list_objects_v2(Bucket=bucket)
