# -*- coding: utf-8 -*-
# Ingest moneta Avro -> Iceberg (polaris.default.snowplow_events)
# - Read paths from --path (comma-separated), supports file:/// and gs:// (runtime configs via spark-submit)
# - Robust Avro union handling for key/value and headers.value
# - Flatten Kafka headers into 9 string columns
# - Parse Snowplow enriched TSV: only 7 timestamp columns are cast to TIMESTAMP
# - Write to Iceberg table provided by --table

import argparse
from pyspark.sql import SparkSession, functions as F, types as T

# -----------------------
# CLI arguments
# -----------------------
parser = argparse.ArgumentParser()
parser.add_argument(
    "--path",
    required=True,
    help="Input path(s), comma-separated. Supports file:/// and gs:// with glob "
         "(e.g. 'gs://bucket/dir/*.avro,file:///.../moneta/*.avro'). "
         "All storage-specific configs must be passed via spark-submit --conf."
)
parser.add_argument(
    "--table",
    default="polaris.default.snowplow_events",
    help="Target Iceberg table (default: polaris.default.snowplow_events)"
)
args = parser.parse_args()

spark = (SparkSession.builder
         .appName("ingest-moneta-to-snowplow-events")
         .getOrCreate())

# -----------------------
# Resolve input paths (comma-separated)
# -----------------------
raw_paths = [p.strip() for p in args.path.split(",") if p.strip()]
if not raw_paths:
    raise ValueError("No valid paths provided via --path")

print("📥 Loading Avro from:")
for p in raw_paths:
    print("   -", p)

# -----------------------
# Helpers
# -----------------------
def empty_to_null(c):
    """Turn empty string into NULL."""
    return F.when((c.isNull()) | (c == F.lit("")), F.lit(None)).otherwise(c)

def to_ts(c):
    """Parse 'yyyy-MM-dd HH:mm:ss.SSS' to TIMESTAMP; empty->NULL."""
    return F.to_timestamp(empty_to_null(c), "yyyy-MM-dd HH:mm:ss.SSS")

def resolve_union_binary(df, field_name: str):
    """
    Extract binary from an Avro union field:
      - If already BinaryType: return it directly
      - If it's a Struct (union expanded), prefer the member with BinaryType
      - Otherwise coalesce members casted to binary
    """
    dt = df.schema[field_name].dataType
    if isinstance(dt, T.BinaryType):
        return F.col(field_name)
    if isinstance(dt, T.StructType):
        for f in dt.fields:
            if isinstance(f.dataType, T.BinaryType):
                return F.col(f"{field_name}.{f.name}")
        return F.coalesce(*[F.col(f"{field_name}.{f.name}").cast("binary") for f in dt.fields])
    return F.col(field_name).cast("binary")

def make_union_to_string(hdr_value_dtype: T.StructType):
    """
    Convert Avro union-expanded struct to STRING in a robust way:
      - Prefer StringType members
      - Then BinaryType (base64)
      - Then numeric/boolean cast to string
      - Else NULL
    """
    string_members = [f.name for f in hdr_value_dtype.fields if isinstance(f.dataType, T.StringType)]
    binary_members = [f.name for f in hdr_value_dtype.fields if isinstance(f.dataType, T.BinaryType)]
    numeric_bool_members = [
        f.name for f in hdr_value_dtype.fields
        if isinstance(f.dataType, (T.BooleanType, T.ByteType, T.ShortType, T.IntegerType,
                                   T.LongType, T.FloatType, T.DoubleType, T.DecimalType))
    ]

    def builder(u_col):
        exprs = []
        for name in string_members:
            exprs.append(u_col.getField(name).cast("string"))
        for name in binary_members:
            exprs.append(F.base64(u_col.getField(name)))
        for name in numeric_bool_members:
            exprs.append(u_col.getField(name).cast("string"))
        if exprs:
            return F.coalesce(*exprs)
        return F.lit(None).cast("string")

    return builder

# -----------------------
# 1) Load Avro
# -----------------------
df0 = spark.read.format("avro").load(raw_paths)

# key/value -> UTF-8 text
key_bin   = resolve_union_binary(df0, "key")
value_bin = resolve_union_binary(df0, "value")
key_text  = F.decode(key_bin, "UTF-8").alias("key_text")
value_str = F.decode(value_bin, "UTF-8")

# Split TSV into 131 columns; keep empty fields
parts = F.split(F.coalesce(value_str, F.lit("")), "\t", -1)
p = lambda i: parts.getItem(i)

# -----------------------
# 2) Flatten headers -> 9 columns (string)
# -----------------------
headers_type = df0.schema["headers"].dataType  # ArrayType(StructType(...))
elem_type: T.StructType = headers_type.elementType
value_field = next(f for f in elem_type.fields if f.name == "value")
hdr_value_struct: T.StructType = value_field.dataType

union_to_string = make_union_to_string(hdr_value_struct)

empty_map = F.map_from_arrays(F.array().cast("array<string>"), F.array().cast("array<string>"))
hdr_map = F.when(
    F.col("headers").isNull(), empty_map
).otherwise(
    F.map_from_entries(
        F.transform(
            F.col("headers"),
            lambda h: F.struct(
                h.getField("key"),
                union_to_string(h.getField("value"))
            )
        )
    )
).alias("hdr")

hdr_keys = {
    "header_ck": "ck",
    "header_clid": "clid",
    "header_gt": "gt",
    "header_ip": "ip",
    "header_offset": "offset",
    "header_tp": "tp",
    "header_trace_id": "trace_id",
    "header_uid": "uid",
    "header_v": "v",
    "header_conid":    "conid",
    "header_st":       "st",
}
hdr_selects = [F.col("hdr").getItem(k).alias(alias) for alias, k in hdr_keys.items()]

# -----------------------
# 3) Snowplow TSV mapping (only 7 TIMESTAMP columns)
# -----------------------
cols = [
    ("app_id", p(0)),
    ("platform", p(1)),
    ("etl_tstamp", to_ts(p(2))),
    ("collector_tstamp", to_ts(p(3))),
    ("dvce_created_tstamp", to_ts(p(4))),
    ("event", p(5)),
    ("event_id", p(6)),
    ("txn_id", p(7)),
    ("name_tracker", p(8)),
    ("v_tracker", p(9)),
    ("v_collector", p(10)),
    ("v_etl", p(11)),
    ("user_id", p(12)),
    ("user_ipaddress", p(13)),
    ("user_fingerprint", p(14)),
    ("domain_userid", p(15)),
    ("domain_sessionidx", p(16)),
    ("network_userid", p(17)),
    ("geo_country", p(18)),
    ("geo_region", p(19)),
    ("geo_city", p(20)),
    ("geo_zipcode", p(21)),
    ("geo_latitude", p(22)),
    ("geo_longitude", p(23)),
    ("geo_region_name", p(24)),
    ("ip_isp", p(25)),
    ("ip_organization", p(26)),
    ("ip_domain", p(27)),
    ("ip_netspeed", p(28)),
    ("page_url", p(29)),
    ("page_title", p(30)),
    ("page_referrer", p(31)),
    ("page_urlscheme", p(32)),
    ("page_urlhost", p(33)),
    ("page_urlport", p(34)),
    ("page_urlpath", p(35)),
    ("page_urlquery", p(36)),
    ("page_urlfragment", p(37)),
    ("refr_urlscheme", p(38)),
    ("refr_urlhost", p(39)),
    ("refr_urlport", p(40)),
    ("refr_urlpath", p(41)),
    ("refr_urlquery", p(42)),
    ("refr_urlfragment", p(43)),
    ("refr_medium", p(44)),
    ("refr_source", p(45)),
    ("refr_term", p(46)),
    ("mkt_medium", p(47)),
    ("mkt_source", p(48)),
    ("mkt_term", p(49)),
    ("mkt_content", p(50)),
    ("mkt_campaign", p(51)),
    ("contexts", p(52)),
    ("se_category", p(53)),
    ("se_action", p(54)),
    ("se_label", p(55)),
    ("se_property", p(56)),
    ("se_value", p(57)),
    ("unstruct_event", p(58)),
    ("tr_orderid", p(59)),
    ("tr_affiliation", p(60)),
    ("tr_total", p(61)),
    ("tr_tax", p(62)),
    ("tr_shipping", p(63)),
    ("tr_city", p(64)),
    ("tr_state", p(65)),
    ("tr_country", p(66)),
    ("ti_orderid", p(67)),
    ("ti_sku", p(68)),
    ("ti_name", p(69)),
    ("ti_category", p(70)),
    ("ti_price", p(71)),
    ("ti_quantity", p(72)),
    ("pp_xoffset_min", p(73)),
    ("pp_xoffset_max", p(74)),
    ("pp_yoffset_min", p(75)),
    ("pp_yoffset_max", p(76)),
    ("useragent", p(77)),
    ("br_name", p(78)),
    ("br_family", p(79)),
    ("br_version", p(80)),
    ("br_type", p(81)),
    ("br_renderengine", p(82)),
    ("br_lang", p(83)),
    ("br_features_pdf", p(84)),
    ("br_features_flash", p(85)),
    ("br_features_java", p(86)),
    ("br_features_director", p(87)),
    ("br_features_quicktime", p(88)),
    ("br_features_realplayer", p(89)),
    ("br_features_windowsmedia", p(90)),
    ("br_features_gears", p(91)),
    ("br_features_silverlight", p(92)),
    ("br_cookies", p(93)),
    ("br_colordepth", p(94)),
    ("br_viewwidth", p(95)),
    ("br_viewheight", p(96)),
    ("os_name", p(97)),
    ("os_family", p(98)),
    ("os_manufacturer", p(99)),
    ("os_timezone", p(100)),
    ("dvce_type", p(101)),
    ("dvce_ismobile", p(102)),
    ("dvce_screenwidth", p(103)),
    ("dvce_screenheight", p(104)),
    ("doc_charset", p(105)),
    ("doc_width", p(106)),
    ("doc_height", p(107)),
    ("tr_currency", p(108)),
    ("tr_total_base", p(109)),
    ("tr_tax_base", p(110)),
    ("tr_shipping_base", p(111)),
    ("ti_currency", p(112)),
    ("ti_price_base", p(113)),
    ("base_currency", p(114)),
    ("geo_timezone", p(115)),
    ("mkt_clickid", p(116)),
    ("mkt_network", p(117)),
    ("etl_tags", p(118)),
    ("dvce_sent_tstamp", to_ts(p(119))),
    ("refr_domain_userid", p(120)),
    ("refr_device_tstamp", to_ts(p(121))),
    ("derived_contexts", p(122)),
    ("domain_sessionid", p(123)),
    ("derived_tstamp", to_ts(p(124))),
    ("event_vendor", p(125)),
    ("event_name", p(126)),
    ("event_format", p(127)),
    ("event_version", p(128)),
    ("event_fingerprint", p(129)),
    ("true_tstamp", to_ts(p(130))),
]

# -----------------------
# 4) Build final DataFrame (order matches DDL)
# -----------------------
final_cols = [key_text, hdr_map] + hdr_selects + [c.alias(name) for name, c in cols]
df = df0.select(*final_cols).drop("hdr")

# -----------------------
# 5) Write to Iceberg
# -----------------------
print(f"📝 Writing to Iceberg table: {args.table}")
(df.write
 .format("iceberg")
 .mode("append")
 .save(args.table))

print(f"✅ Ingest completed -> {args.table}")
