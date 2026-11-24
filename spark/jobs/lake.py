"""Lake layout and helpers shared by Spark jobs."""

import json
from typing import Any

from pyspark.sql import SparkSession

DEFAULT_LAKE_ROOT = "s3a://lake"


def table_path(root: str, layer: str, name: str) -> str:
    return f"{root}/{layer}/{name}"


def read_text(spark: SparkSession, path: str) -> str:
    """Read a small file through the Hadoop FS API.

    Spark's file sources skip names starting with ``_`` (e.g. ``_manifest.json``).
    """
    jvm = spark._jvm
    assert jvm is not None
    hpath = jvm.org.apache.hadoop.fs.Path(path)
    fs = hpath.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    stream = fs.open(hpath)
    try:
        return str(jvm.org.apache.commons.io.IOUtils.toString(stream, "UTF-8"))
    finally:
        stream.close()


def read_manifest(spark: SparkSession, root: str, month: str) -> dict[str, Any]:
    manifest: dict[str, Any] = json.loads(read_text(spark, f"{root}/raw/{month}/_manifest.json"))
    if manifest.get("month") != month:
        raise ValueError(f"manifest month {manifest.get('month')!r} != {month!r}")
    return manifest
