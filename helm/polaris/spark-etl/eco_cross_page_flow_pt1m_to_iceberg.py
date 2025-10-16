# -*- coding: utf-8 -*-
"""
Load Parquet files from a user-provided GCS path and write into an Iceberg table (cross_page_flow).
Usage:
  pyspark <spark-options> -- /path/to/script.py \
    --path gs://bucket/prefix/2025/09/17/09/*/*.parquet \
    --table dpi_catalog.default.eco_cross_page_flow_pt1m_dist
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, to_timestamp
import argparse

def repartition_and_sort(df: DataFrame) -> DataFrame:
    """Repartition by customerId and sort within each partition."""
    return (
        df
        .repartitionByRange("customerId","timestampMs")
        # .sortWithinPartitions("timestampMs", "country", "city", "flowId")
    )

def create_spark_session(app_name: str = "ParquetToIceberg-CROSS") -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()

def read_parquet(spark: SparkSession, path: str) -> DataFrame:
    try:
        return spark.read.parquet(path)
    except Exception as e:
        raise RuntimeError(f"Error reading Parquet files from {path}: {e}")

def convert_timestamp_columns(df: DataFrame) -> DataFrame:
    try:
        return (
            df.withColumn("timestampMs", (col("timestampMs") / 1000).cast("timestamp"))
            .withColumn("flowStartTimeMs", (col("flowStartTimeMs") / 1000).cast("timestamp"))
            .withColumn("watermarkMs", to_timestamp(col("watermarkMs")))
            .withColumn("retentionDate", (col("retentionDate") / 1000).cast("timestamp"))
        )
    except Exception as e:
        raise ValueError(f"Error processing timestamp columns: {e}")

def select_columns(df: DataFrame, columns):
    missing = [c for c in columns if c not in df.columns]
    if missing:
        raise KeyError(f"Missing columns in DataFrame: {missing}")
    return df.select(*columns)

def write_to_iceberg(df: DataFrame, target_table: str):
    try:
        df.writeTo(target_table).append()
    except Exception as e:
        raise RuntimeError(f"Error writing to Iceberg table {target_table}: {e}")

def parse_args():
    p = argparse.ArgumentParser(description="Load Parquet into an Iceberg table (cross_page_flow).")
    p.add_argument("--path", type=str, required=True)
    p.add_argument("--table", type=str, default="dpi_catalog.default.eco_cross_page_flow_pt1m_dist")
    return p.parse_args()

def main():
    args = parse_args()
    input_path = args.path
    iceberg_table = args.table

    columns_to_write = [
        "timestampMs","flowId","flowStartTimeMs","customerId","clientId","sessionId","inSession","userSessionId",
        "inUserSession","platform","platformSubcategory","appName","appBuild","appVersion","browserName","browserVersion",
        "userId","deviceManufacturer","deviceMarketingName","deviceModel","deviceHardwareType","deviceName","deviceCategory",
        "deviceOperatingSystem","deviceOperatingSystemVersion","deviceOperatingSystemFamily","country","state","city",
        "countryIso","sub1Iso","sub2Iso","cityGid","dma","postalCode","isp","netSpeed","sensorVersion","appType","asn",
        "timezoneOffsetMins","connType","watermarkMs","partitionId","retentionDate","tagGroup1","tagGroup2","tagGroup3",
        "tagGroup4","tagGroup5","tagGroup6","tagGroup7","tagGroup8","tagGroup9","tagGroup10","tagGroup11","tagGroup12",
        "tagGroup13","tagGroup14","tagGroup15","metricIntGroup1","metricIntGroup2","metricIntGroup3","metricIntGroup4",
        "metricIntGroup5","metricIntGroup6","metricIntGroup7","metricIntGroup8","metricIntGroup9","metricIntGroup10",
        "metricIntGroup11","metricIntGroup12","metricIntGroup13","metricIntGroup14","metricIntGroup15",
        "metricFloatGroup1","metricFloatGroup2","metricFloatGroup3","metricFloatGroup4","metricFloatGroup5",
        "metricFloatGroup6","metricFloatGroup7","metricFloatGroup8","metricFloatGroup9","metricFloatGroup10",
        "metricFloatGroup11","metricFloatGroup12","metricFloatGroup13","metricFloatGroup14","metricFloatGroup15"
    ]

    spark = create_spark_session()
    print(f"[INFO] Reading from: {input_path}")
    print(f"[INFO] Writing to Iceberg table: {iceberg_table}")

    try:
        df = read_parquet(spark, input_path)
        df = convert_timestamp_columns(df)
        df = select_columns(df, columns_to_write)
        df = repartition_and_sort(df)
        write_to_iceberg(df, iceberg_table)
        print("[INFO] Data successfully written to Iceberg.")
    except Exception as e:
        print(f"[ERROR] Pipeline failed: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
