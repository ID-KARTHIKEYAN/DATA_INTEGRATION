from __future__ import annotations

from pyspark.sql import SparkSession


def get_spark() -> SparkSession:
    spark = SparkSession.getActiveSession()
    if spark is None:
        spark = SparkSession.builder.getOrCreate()
    return spark
