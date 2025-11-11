# -*- coding: utf-8 -*-
"""
Load Parquet files and write into an Iceberg table (eco_events_dist).

Usage:
  pyspark <spark-options> -- /path/to/script.py \
    --path "hdfs://nameservice-aa/odp_control_plane/semantic_analysis/2025/11/11/09/*/*/events_output_filtered/*.parquet" \
    --table "polaris.default.eco_events_dist"
"""

import argparse
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, to_timestamp, when, coalesce, lit, transform_values
)

# --------------------------- Spark helpers ---------------------------

def create_spark_session(app_name: str = "ParquetToIceberg-ECO_EVENTS") -> SparkSession:
    """Create a SparkSession with a descriptive app name."""
    return SparkSession.builder.appName(app_name).getOrCreate()

def read_parquet(spark: SparkSession, path: str) -> DataFrame:
    """Read Parquet with schema merge enabled (handles per-file schema drift)."""
    try:
        return spark.read.option("mergeSchema", "true").parquet(path)
    except Exception as e:
        raise RuntimeError(f"Error reading Parquet files from {path}: {e}")

# --------------------------- Transformations ---------------------------

def convert_timestamp_columns(df: DataFrame) -> DataFrame:
    """
    Convert time-related columns to TIMESTAMP without deriving from other columns.
    - sessionStartMs: epoch millis -> timestamp (if present)
    - watermarkMs: numeric millis or parseable string -> timestamp (if present)
    - eventTimeMs:
        * if column exists:
            - numeric (epoch millis) -> timestamp
            - else try parse as timestamp string
          then fill remaining NULLs to '2099-01-01 00:00:00'
        * if column does not exist:
          create it and set to '2099-01-01 00:00:00'
    """
    d = df

    # sessionStartMs (if present)
    if "sessionStartMs" in d.columns:
        d = d.withColumn("sessionStartMs", (col("sessionStartMs") / 1000).cast("timestamp"))

    # watermarkMs (if present)
    if "watermarkMs" in d.columns:
        d = d.withColumn(
            "watermarkMs",
            when(col("watermarkMs").cast("string").rlike(r'^\d+$'),
                 (col("watermarkMs").cast("double") / 1000).cast("timestamp")
                 ).otherwise(to_timestamp(col("watermarkMs")))
        )

    # eventTimeMs (no derivation from other columns)
    if "eventTimeMs" in d.columns:
        d = d.withColumn(
            "eventTimeMs",
            when(col("eventTimeMs").cast("string").rlike(r'^[+-]?\d+$'),
                 (col("eventTimeMs").cast("double") / 1000).cast("timestamp")
                 ).otherwise(to_timestamp(col("eventTimeMs")))
        )
        d = d.withColumn("eventTimeMs", coalesce(col("eventTimeMs"), to_timestamp(lit("2099-01-01 00:00:00"))))
    else:
        d = d.withColumn("eventTimeMs", to_timestamp(lit("2099-01-01 00:00:00")))

    return d

def derive_and_alias_columns(df: DataFrame) -> DataFrame:
    """
    Derive/alias required columns:
    - sessionId: strictly set from sessionIdNew (no fallback). Fail fast if sessionIdNew is missing.
    """
    if "sessionIdNew" not in df.columns:
        raise KeyError("Missing required column 'sessionIdNew' in input data (needed for Iceberg 'sessionId').")
    return df.withColumn("sessionId", col("sessionIdNew"))

def harmonize_types(df: DataFrame) -> DataFrame:
    """
    Align Spark DF types to Iceberg schema (polaris.default.eco_events_dist).
    NOTE: Do NOT change appInstanceId (STRING by design) or sessionId (STRING).
    """
    d = df

    # ints
    for c in ["customerId","cityGid","asn","isp","connType","timezoneOffsetMins","partitionId"]:
        if c in d.columns:
            d = d.withColumn(c, col(c).cast("int"))

    # smallint
    if "dma" in d.columns:
        d = d.withColumn("dma", col("dma").cast("smallint"))

    # longs
    for c in ["country","state","city","userSessionId"]:
        if c in d.columns:
            d = d.withColumn(c, col(c).cast("long"))

    # doubles
    if "networkRequestDurationMs" in d.columns:
        d = d.withColumn("networkRequestDurationMs", col("networkRequestDurationMs").cast("double"))

    # maps: tagGroup* -> MAP<STRING, STRING> (coalesce null values to "")
    for i in range(1, 16):
        k = f"tagGroup{i}"
        if k in d.columns:
            d = d.withColumn(k, transform_values(col(k), lambda kk, vv: coalesce(vv, lit("")).cast("string")))

    # maps: metricIntGroup* -> MAP<STRING, LONG>
    for i in range(1, 16):
        k = f"metricIntGroup{i}"
        if k in d.columns:
            d = d.withColumn(k, transform_values(col(k), lambda kk, vv: vv.cast("long")))

    # maps: metricFloatGroup* -> MAP<STRING, DOUBLE>
    for i in range(1, 16):
        k = f"metricFloatGroup{i}"
        if k in d.columns:
            d = d.withColumn(k, transform_values(col(k), lambda kk, vv: vv.cast("double")))

    # customs
    if "customStrFloat32MapGroup1" in d.columns:
        d = d.withColumn("customStrFloat32MapGroup1",
                         transform_values(col("customStrFloat32MapGroup1"), lambda kk, vv: vv.cast("double")))
    if "customStrInt32MapGroup1" in d.columns:
        d = d.withColumn("customStrInt32MapGroup1",
                         transform_values(col("customStrInt32MapGroup1"), lambda kk, vv: vv.cast("int")))

    return d

# For missing columns, add NULLs cast to target types, then select in exact order
ICEBERG_SCHEMA_TYPES = {
    # timestamps & ids
    "eventTimeMs": "timestamp", "customerId": "int", "clientId": "string", "appInstanceId": "string",
    "sessionId": "string", "sessionStartMs": "timestamp", "inSession": "boolean",
    "userSessionId": "long", "inUserSession": "boolean",

    # app/platform/device/browser
    "platform": "string", "platformSubcategory": "string", "appName": "string", "appBuild": "string",
    "appVersion": "string", "sensorVersion": "string",
    "referrerHost": "string", "host": "string", "path": "string", "query": "string",
    "referrer": "string", "title": "string", "url": "string",
    "deviceName": "string", "deviceCategory": "string", "deviceHardwareType": "string",
    "deviceManufacturer": "string", "deviceMarketingName": "string",
    "deviceOperatingSystem": "string", "deviceOperatingSystemVersion": "string",
    "deviceOperatingSystemFamily": "string", "deviceModel": "string", "deviceVendor": "string",
    "browserName": "string", "browserVersion": "string",
    "playerFrameworkName": "string", "playerFrameworkVersion": "string",

    # geo/network/user
    "country": "long", "state": "long", "city": "long", "countryIso": "string", "sub1Iso": "string",
    "sub2Iso": "string", "cityGid": "int", "dma": "smallint", "postalCode": "string",
    "timezoneOffsetMins": "int", "ipV4": "string", "ipV6": "string", "userId": "string",
    "asn": "int", "isp": "int", "domain": "string", "connType": "int", "netSpeed": "string",

    # event & metrics
    "eventVendor": "string", "eventVersion": "string", "eventCategory": "string", "eventName": "string",
    "networkRequestDurationMs": "double",
    "watermarkMs": "timestamp", "partitionId": "int",

    # tags
    **{f"tagGroup{i}": "map<string,string>" for i in range(1, 16)},

    # customs
    "customStrFloat32MapGroup1": "map<string,double>",
    "customStrInt32MapGroup1": "map<string,int>",

    # metric int maps
    **{f"metricIntGroup{i}": "map<string,long>" for i in range(1, 16)},

    # metric float maps
    **{f"metricFloatGroup{i}": "map<string,double>" for i in range(1, 16)},
}

def ensure_columns(df: DataFrame, schema_types: dict) -> DataFrame:
    """Add missing columns as NULLs cast to target types so the write matches table schema."""
    d = df
    for c, t in schema_types.items():
        if c not in d.columns:
            d = d.withColumn(c, lit(None).cast(t))
    return d

def select_columns(df: DataFrame, columns):
    """Project DataFrame to the exact ordered list of columns (existing after ensure_columns)."""
    return df.select(*columns)

def repartition_and_sort(df: DataFrame) -> DataFrame:
    """Repartition by customerId and eventTimeMs; optional sort can be enabled if needed."""
    return df.repartitionByRange("customerId", "eventTimeMs")
    # .sortWithinPartitions("eventTimeMs", "country", "city", "eventName")

# --------------------------- Write ---------------------------

def write_to_iceberg(df: DataFrame, target_table: str):
    """Append DataFrame into the target Iceberg table."""
    try:
        df.writeTo(target_table).append()
    except Exception as e:
        raise RuntimeError(f"Error writing to Iceberg table {target_table}: {e}")

# --------------------------- CLI & main ---------------------------

def parse_args():
    p = argparse.ArgumentParser(description="Load Parquet into an Iceberg table (eco_events_dist).")
    p.add_argument("--path", type=str, required=True)
    p.add_argument("--table", type=str, default="polaris.default.eco_events_dist")
    return p.parse_args()

def main():
    args = parse_args()
    input_path = args.path
    iceberg_table = args.table

    # Exact column order matching Iceberg table
    columns_to_write = [
        # timestamps & ids
        "eventTimeMs","customerId","clientId","appInstanceId","sessionId","sessionStartMs",
        "inSession","userSessionId","inUserSession",

        # app/platform/device/browser
        "platform","platformSubcategory","appName","appBuild","appVersion","sensorVersion",
        "referrerHost","host","path","query","referrer","title","url",
        "deviceName","deviceCategory","deviceHardwareType","deviceManufacturer","deviceMarketingName",
        "deviceOperatingSystem","deviceOperatingSystemVersion","deviceOperatingSystemFamily",
        "deviceModel","deviceVendor","browserName","browserVersion","playerFrameworkName","playerFrameworkVersion",

        # geo/network/user
        "country","state","city","countryIso","sub1Iso","sub2Iso","cityGid","dma","postalCode",
        "timezoneOffsetMins","ipV4","ipV6","userId","asn","isp","domain","connType","netSpeed",

        # event & metrics
        "eventVendor","eventVersion","eventCategory","eventName","networkRequestDurationMs",
        "watermarkMs","partitionId",

        # tags
        "tagGroup1","tagGroup2","tagGroup3","tagGroup4","tagGroup5","tagGroup6","tagGroup7","tagGroup8",
        "tagGroup9","tagGroup10","tagGroup11","tagGroup12","tagGroup13","tagGroup14","tagGroup15",

        # customs
        "customStrFloat32MapGroup1","customStrInt32MapGroup1",

        # metric int maps
        "metricIntGroup1","metricIntGroup2","metricIntGroup3","metricIntGroup4","metricIntGroup5",
        "metricIntGroup6","metricIntGroup7","metricIntGroup8","metricIntGroup9","metricIntGroup10",
        "metricIntGroup11","metricIntGroup12","metricIntGroup13","metricIntGroup14","metricIntGroup15",

        # metric float maps
        "metricFloatGroup1","metricFloatGroup2","metricFloatGroup3","metricFloatGroup4","metricFloatGroup5",
        "metricFloatGroup6","metricFloatGroup7","metricFloatGroup8","metricFloatGroup9","metricFloatGroup10",
        "metricFloatGroup11","metricFloatGroup12","metricFloatGroup13","metricFloatGroup14","metricFloatGroup15"
    ]

    spark = create_spark_session()
    print(f"[INFO] Reading from: {input_path}")
    print(f"[INFO] Writing to Iceberg table: {iceberg_table}")

    try:
        df = read_parquet(spark, input_path)
        df = convert_timestamp_columns(df)       # only fill NULL eventTimeMs to 2099-01-01
        df = derive_and_alias_columns(df)        # sessionId <- sessionIdNew (strict)
        df = harmonize_types(df)                 # light type alignment
        df = ensure_columns(df, ICEBERG_SCHEMA_TYPES)
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
