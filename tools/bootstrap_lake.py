"""Create the lake bucket and required directory markers. Idempotent."""

import os

from botocore.exceptions import ClientError

from ingest.s3 import make_s3_client

# S3A needs these directories to exist before Spark starts. An empty "dir/" key is not
# enough on SeaweedFS (S3A probes directories by listing), so each gets a hidden placeholder
# file. Spark's History Server ignores files starting with ".".
DIR_PLACEHOLDERS = ("_spark-events/.keep",)


def main() -> None:
    s3 = make_s3_client()
    bucket = os.environ["LAKE_BUCKET"]

    try:
        s3.head_bucket(Bucket=bucket)
        print(f"bucket {bucket}: exists")
    except ClientError as err:
        if err.response.get("Error", {}).get("Code") not in ("404", "NoSuchBucket"):
            raise
        s3.create_bucket(Bucket=bucket)
        print(f"bucket {bucket}: created")

    for placeholder in DIR_PLACEHOLDERS:
        s3.put_object(Bucket=bucket, Key=placeholder, Body=b"")
        print(f"placeholder {bucket}/{placeholder}: ok")


if __name__ == "__main__":
    main()
