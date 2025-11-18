# -*- coding: utf-8 -*-
"""
Load Parquet files (eco schema) and write into an Iceberg table (eco_events_dist).

Usage:
  pyspark <spark-options> -- /path/to/load_eco_to_iceberg.py \
    --path "hdfs://nameservice-aa/odp_control_plane/semantic_analysis/2025/11/11/09/*/*/events_output_filtered/*.parquet" \
    --table "polaris.default.eco_events_dist"
"""

import argparse
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, to_timestamp, when, coalesce, lit, transform_values, length, trim
)
from pyspark.sql.types import MapType, StringType

# --------------------------- Spark helpers ---------------------------

def create_spark_session(app_name: str = "ParquetToIceberg-ECO_EVENTS") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        # 如需 Iceberg catalog，请在 spark-submit 传入 catalog 配置；这里保持简洁
        .getOrCreate()
    )

def read_parquet(spark: SparkSession, path: str) -> DataFrame:
    try:
        return spark.read.option("mergeSchema", "true").parquet(path)
    except Exception as e:
        raise RuntimeError(f"Error reading Parquet files from {path}: {e}")

# --------------------------- Transformations ---------------------------

def to_ts_maybe(d: DataFrame, name: str) -> DataFrame:
    """把列统一为 timestamp：数值当作 epochMillis，字符串用 to_timestamp 解析。若不存在则忽略。"""
    if name not in d.columns:
        return d
    return d.withColumn(
        name,
        when(col(name).cast("string").rlike(r'^[+-]?\d+$'),
             (col(name).cast("double") / 1000).cast("timestamp")
             ).otherwise(to_timestamp(col(name)))
    )

def convert_core_columns(df: DataFrame) -> DataFrame:
    """统一关键列类型与语义，尽量不派生新字段。"""
    d = df

    # 统一 timestamp 列：输入 eco.parquet 已是 DateTime64(3,'UTC')，这里仍做保险转换
    for c in ["eventTimeMs", "sessionStartMs", "watermarkMs"]:
        d = to_ts_maybe(d, c)

    # 布尔列（输入是 UInt8）：转 boolean
    for c in ["inSession", "inUserSession"]:
        if c in d.columns:
            d = d.withColumn(c, col(c).cast("boolean"))

    # 直接沿用 sessionId（输入为 FixedString(32)）：转 string 去除可能的填充
    if "sessionId" in d.columns:
        d = d.withColumn("sessionId", trim(col("sessionId").cast("string")))

    # appInstanceId（输入 Nullable(Int32)）：转 string
    if "appInstanceId" in d.columns:
        d = d.withColumn("appInstanceId", col("appInstanceId").cast("string"))

    # clientId 统一为非空白字符串（供后续 NOT NULL 过滤判断）
    if "clientId" in d.columns:
        d = d.withColumn("clientId", trim(col("clientId").cast("string")))

    return d

def harmonize_scalar_types(df: DataFrame) -> DataFrame:
    """按 Iceberg 目标类型对标量列进行轻量 cast。"""
    d = df

    # int
    for c in ["customerId", "cityGid", "asn", "isp", "connType", "timezoneOffsetMins", "partitionId"]:
        if c in d.columns:
            d = d.withColumn(c, col(c).cast("int"))

    # smallint
    if "dma" in d.columns:
        d = d.withColumn("dma", col("dma").cast("smallint"))

    # long
    for c in ["country", "state", "city", "userSessionId"]:
        if c in d.columns:
            d = d.withColumn(c, col(c).cast("long"))

    # double
    if "networkRequestDurationMs" in d.columns:
        d = d.withColumn("networkRequestDurationMs", col("networkRequestDurationMs").cast("double"))

    return d

def cast_map_values(df: DataFrame) -> DataFrame:
    """把所有 map 列按目标 schema 的 value 类型统一（保留 key 为 string）。"""
    d = df

    # tagGroup*: map<string,string> （把 NULL 值归一为空串，避免写入复杂的可空 map value）
    for i in range(1, 16):
        k = f"tagGroup{i}"
        if k in d.columns:
            d = d.withColumn(
                k,
                transform_values(col(k), lambda kk, vv: coalesce(vv.cast("string"), lit("")))
                .cast(MapType(StringType(), StringType(), valueContainsNull=False))
            )

    # metricIntGroup*: map<string,long>
    for i in range(1, 16):
        k = f"metricIntGroup{i}"
        if k in d.columns:
            d = d.withColumn(k, transform_values(col(k), lambda kk, vv: vv.cast("long")))

    # metricFloatGroup*: map<string,double>
    for i in range(1, 15 + 1):
        k = f"metricFloatGroup{i}"
        if k in d.columns:
            d = d.withColumn(k, transform_values(col(k), lambda kk, vv: vv.cast("double")))

    # customs
    if "customStrFloat32MapGroup1" in d.columns:
        d = d.withColumn(
            "customStrFloat32MapGroup1",
            transform_values(col("customStrFloat32MapGroup1"), lambda kk, vv: vv.cast("double"))
        )
    if "customStrInt32MapGroup1" in d.columns:
        d = d.withColumn(
            "customStrInt32MapGroup1",
            transform_values(col("customStrInt32MapGroup1"), lambda kk, vv: vv.cast("int"))
        )

    return d

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
    d = df
    for c, t in schema_types.items():
        if c not in d.columns:
            d = d.withColumn(c, lit(None).cast(t))
    return d

def select_columns(df: DataFrame, columns):
    return df.select(*columns)

def repartition_and_sort(df: DataFrame) -> DataFrame:
    return df.repartitionByRange("customerId", "eventTimeMs")

def drop_rows_violating_not_null(df: DataFrame) -> DataFrame:
    """Iceberg 表中 NOT NULL 的列：eventTimeMs, customerId, clientId。"""
    return df.where(
        col("eventTimeMs").isNotNull()
        & col("customerId").isNotNull()
        & (col("clientId").isNotNull() & (length(col("clientId")) > 0))
    )

# --------------------------- Write ---------------------------

def write_to_iceberg(df: DataFrame, target_table: str):
    try:
        df.writeTo(target_table).append()
    except Exception as e:
        raise RuntimeError(f"Error writing to Iceberg table {target_table}: {e}")

# --------------------------- CLI & main ---------------------------

def parse_args():
    p = argparse.ArgumentParser(description="Load Parquet (eco schema) into Iceberg table (eco_events_dist).")
    p.add_argument("--path", type=str, required=True, help="Parquet glob/path")
    p.add_argument("--table", type=str, default="polaris.default.eco_events_dist")
    return p.parse_args()

def main():
    args = parse_args()
    input_path = args.path
    iceberg_table = args.table

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
        df = convert_core_columns(df)
        df = harmonize_scalar_types(df)
        df = cast_map_values(df)
        df = ensure_columns(df, ICEBERG_SCHEMA_TYPES)
        df = select_columns(df, columns_to_write)
        df = drop_rows_violating_not_null(df)    # 严格满足 Iceberg NOT NULL
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
