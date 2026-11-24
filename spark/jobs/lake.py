"""Lake layout and helpers shared by Spark jobs."""

import json
import urllib.request
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


def stage_metrics(spark: SparkSession) -> dict[str, int]:
    """Sum shuffle and spill bytes over all stages of this app (Spark REST API on the driver)."""
    sc = spark.sparkContext
    if not sc.uiWebUrl:
        return {}
    url = f"{sc.uiWebUrl}/api/v1/applications/{sc.applicationId}/stages"
    try:
        with urllib.request.urlopen(url, timeout=10) as resp:
            stages = json.loads(resp.read())
    except OSError:
        return {}
    return {
        "shuffle_read_bytes": sum(s.get("shuffleReadBytes", 0) for s in stages),
        "shuffle_write_bytes": sum(s.get("shuffleWriteBytes", 0) for s in stages),
        "spill_bytes": sum(
            s.get("memoryBytesSpilled", 0) + s.get("diskBytesSpilled", 0) for s in stages
        ),
    }
