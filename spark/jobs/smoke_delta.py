"""Phase 0.3 smoke test, run on the cluster via `make spark-smoke`.

Checks: both workers registered, Python 3.12 on driver and executors,
Delta write/append/read/history on S3A, then cleans up.
"""

import os
import sys
import time
import uuid

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

EXPECTED_EXECUTORS = 2
EXPECTED_PYTHON = (3, 12)
ROWS = 100_000


def wait_for_executors(spark: SparkSession, expected: int, timeout_s: int = 60) -> int:
    jsc = spark.sparkContext._jsc
    deadline = time.monotonic() + timeout_s
    count = 0
    while time.monotonic() < deadline:
        count = int(jsc.sc().getExecutorMemoryStatus().size()) - 1  # minus the driver
        if count >= expected:
            break
        time.sleep(1)
    return count


def main() -> None:
    spark = SparkSession.builder.appName("smoke-delta").getOrCreate()
    sc = spark.sparkContext
    path = f"s3a://{os.environ['LAKE_BUCKET']}/_smoke/delta_{uuid.uuid4().hex}"

    executors = wait_for_executors(spark, EXPECTED_EXECUTORS)
    assert executors == EXPECTED_EXECUTORS, f"executors={executors}"

    driver_py = sys.version_info[:2]
    executor_py = set(
        sc.parallelize(range(8), 8).map(lambda _: tuple(sys.version_info[:2])).collect()
    )
    assert driver_py == EXPECTED_PYTHON, f"driver python={driver_py}"
    assert executor_py == {EXPECTED_PYTHON}, f"executor python={executor_py}"

    df = spark.range(ROWS).withColumn("bucket", F.col("id") % 10)
    df.write.format("delta").partitionBy("bucket").save(path)
    df.limit(10).write.format("delta").mode("append").save(path)

    rows = spark.read.format("delta").load(path).count()
    versions = spark.sql(f"DESCRIBE HISTORY delta.`{path}`").count()
    assert rows == ROWS + 10, f"rows={rows}"
    assert versions == 2, f"versions={versions}"

    jvm = spark._jvm
    assert jvm is not None
    hpath = jvm.org.apache.hadoop.fs.Path(path)
    hpath.getFileSystem(sc._jsc.hadoopConfiguration()).delete(hpath, True)

    print(f"SMOKE executors={executors}")
    print(f"SMOKE python driver={driver_py} executors={sorted(executor_py)}")
    print(f"SMOKE delta rows={rows} versions={versions}")
    print("SMOKE OK")
    spark.stop()


if __name__ == "__main__":
    main()
