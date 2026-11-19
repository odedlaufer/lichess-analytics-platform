"""Phase 0 gate: Spark writes a small Delta table that ClickHouse must read back.

The path is wiped first so the table has exactly one version, which keeps a plain
Parquet glob over its data files equal to the Delta snapshot.
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

GATE_PREFIX = "_gate/spark_delta"
ROWS = 1_000


def main() -> None:
    spark = SparkSession.builder.appName("phase0-gate-write").getOrCreate()
    sc = spark.sparkContext
    path = f"s3a://{os.environ['LAKE_BUCKET']}/{GATE_PREFIX}"

    jvm = spark._jvm
    assert jvm is not None
    hpath = jvm.org.apache.hadoop.fs.Path(path)
    hpath.getFileSystem(sc._jsc.hadoopConfiguration()).delete(hpath, True)

    df = (
        spark.range(ROWS)
        .withColumn("value", F.col("id") * 2)
        .withColumn("label", F.concat(F.lit("g"), F.col("id").cast("string")))
    )
    df.coalesce(2).write.format("delta").save(path)

    stored = spark.read.format("delta").load(path)
    rows = stored.count()
    sum_id = stored.agg(F.sum("id")).first()[0]  # type: ignore[index]
    assert rows == ROWS, f"rows={rows}"

    print(f"GATE path={path}")
    print(f"GATE rows={rows} sum_id={sum_id}")
    spark.stop()


if __name__ == "__main__":
    main()
