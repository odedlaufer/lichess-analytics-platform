"""Object store (SeaweedFS S3 API) smoke tests. Requires `make up`."""

import os
import uuid
from collections.abc import Iterator

import pytest
from botocore.exceptions import ClientError
from mypy_boto3_s3 import S3Client

from ingest.s3 import make_s3_client
from tools.bootstrap_lake import DIR_PLACEHOLDERS

pytestmark = pytest.mark.integration


@pytest.fixture
def s3() -> S3Client:
    return make_s3_client()


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


def test_dir_placeholders_exist(s3: S3Client, bucket: str) -> None:
    for placeholder in DIR_PLACEHOLDERS:
        s3.head_object(Bucket=bucket, Key=placeholder)


def test_put_get_list_roundtrip(s3: S3Client, bucket: str, key: str) -> None:
    body = b"1. e4 e5 2. Nf3 Nc6"
    s3.put_object(Bucket=bucket, Key=key, Body=body)

    assert s3.get_object(Bucket=bucket, Key=key)["Body"].read() == body
    listed = s3.list_objects_v2(Bucket=bucket, Prefix="_smoke/")
    assert key in [o["Key"] for o in listed.get("Contents", [])]


def test_wrong_credentials_rejected(bucket: str) -> None:
    bad = make_s3_client(secret_key="wrong-secret")
    with pytest.raises(ClientError):
        bad.list_objects_v2(Bucket=bucket)
