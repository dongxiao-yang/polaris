# -*- coding: utf-8 -*-
# Ingest moneta Avro -> Iceberg (polaris.default.snowplow_events)
# - Read paths from --path (comma-separated), supports file:/// and gs:// (runtime configs via spark-submit)
# - Robust Avro union handling for key/value and headers.value
# - Flatten Kafka headers into 11 decoded string columns (header_*)
# - Decode rules:
#     header_ck/ip/trace_id/uid/conid/st -> Base64 -> UTF-8 text (fallback: original string)
#     header_clid -> Base64 -> 16 bytes -> 4×int32 (big-endian) -> "a.b.c.d" (fallback: original string)
#     header_gt/header_offset -> Base64 -> int64 (big-endian) -> decimal string
#     header_tp -> Base64 -> int32 (big-endian) -> decimal string
#     header_v -> Base64 -> 1..8-byte unsigned big-endian -> decimal string
# - Parse Snowplow enriched TSV: only 7 timestamp columns are cast to TIMESTAMP
# - Write to Iceberg table provided by --table (all header_* remain STRING per your DDL)

import argparse
import struct
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

# (Optional) Avoid super long plan logs being truncated
# spark.conf.set("spark.sql.debug.maxToStringFields", 2000)

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
    Returns a callable that accepts a Column (the union struct).
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
            # keep binary as base64 text (so we can unbase64 later when decoding per-key)
            exprs.append(F.base64(u_col.getField(name)))
        for name in numeric_bool_members:
            exprs.append(u_col.getField(name).cast("string"))
        if exprs:
            return F.coalesce(*exprs)
        return F.lit(None).cast("string")

    return builder

# -----------------------
# UDFs for header decoding (outputs are STRING to match your DDL)
# -----------------------
def _decode_clid_bytes(b: bytes) -> str:
    """
    header_clid: base64 -> 16 bytes -> 4 × int32 (big-endian) -> 'a.b.c.d'.
    If not 16 bytes or any error, return best-effort UTF-8 (ignore invalid).
    """
    if b is None:
        return None
    try:
        if len(b) == 16:
            a, b1, c, d = struct.unpack(">iiii", b)  # big-endian int32 x4
            return f"{a}.{b1}.{c}.{d}"
        return b.decode("utf-8", errors="ignore")
    except Exception:
        return b.decode("utf-8", errors="ignore")

def _be64_to_str(b: bytes) -> str:
    """8-byte big-endian signed long -> decimal string (e.g., millis since epoch)."""
    if b is None or len(b) != 8:
        return None
    return str(struct.unpack(">q", b)[0])

def _be32_to_str(b: bytes) -> str:
    """4-byte big-endian signed int -> decimal string."""
    if b is None or len(b) != 4:
        return None
    return str(struct.unpack(">i", b)[0])

def _var_be_uint_to_str(b: bytes) -> str:
    """1..8 byte big-endian unsigned int -> decimal string; else NULL."""
    if b is None:
        return None
    n = len(b)
    if n < 1 or n > 8:
        return None
    return str(int.from_bytes(b, byteorder="big", signed=False))

decode_clid_udf     = F.udf(_decode_clid_bytes,     T.StringType())
be64_to_str_udf     = F.udf(_be64_to_str,           T.StringType())
be32_to_str_udf     = F.udf(_be32_to_str,           T.StringType())
var_be_uint_str_udf = F.udf(_var_be_uint_to_str,    T.StringType())

# -----------------------
# 1) Load Avro
# -----------------------
df0 = spark.read.format("avro").load(raw_paths)

# key/value -> UTF-8 text (keep empty fields)
key_bin   = resolve_union_binary(df0, "key")
value_bin = resolve_union_binary(df0, "value")
key_text  = F.decode(key_bin, "UTF-8").alias("key_text")
value_str = F.decode(value_bin, "UTF-8")

# Split TSV into 131 columns; keep empty fields
parts = F.split(F.coalesce(value_str, F.lit("")), "\t", -1)
p = lambda i: parts.getItem(i)

# -----------------------
# 2) Flatten headers -> map<string,string> ('hdr'), then decode per header_*
# -----------------------
# headers: Array<Struct<key: string, value: union>>
headers_type = df0.schema["headers"].dataType            # ArrayType(StructType(...))
elem_type: T.StructType = headers_type.elementType
value_field = next(f for f in elem_type.fields if f.name == "value")
hdr_value_struct: T.StructType = value_field.dataType    # union-expanded struct

union_to_string = make_union_to_string(hdr_value_struct)

empty_map = F.map_from_arrays(F.array().cast("array<string>"), F.array().cast("array<string>"))
hdr_map = F.when(
    F.col("headers").isNull(), empty_map
).otherwise(
    F.map_from_entries(
        F.transform(
            F.col("headers"),
            lambda h: F.struct(
                h.getField("key").alias("key"),
                union_to_string(h.getField("value")).alias("value")
            )
        )
    )
).alias("hdr")

# IMPORTANT: materialize 'hdr' as a real column before referencing hdr[...]
df1 = df0.withColumn("hdr", hdr_map)

# Map alias -> key name inside 'hdr'
hdr_keys = {
    "header_ck":       "ck",
    "header_clid":     "clid",
    "header_gt":       "gt",
    "header_ip":       "ip",
    "header_offset":   "offset",
    "header_tp":       "tp",
    "header_trace_id": "trace_id",
    "header_uid":      "uid",
    "header_v":        "v",
    "header_conid":    "conid",
    "header_st":       "st",
}

# Helpers to access header values and decode base64
def H(alias: str):
    """Get header value as STRING from the 'hdr' map (may be base64 if original was binary)."""
    return F.col("hdr").getItem(hdr_keys[alias])

def B64(alias: str):
    """Try base64-decode a header value; invalid base64 yields NULL."""
    return F.unbase64(H(alias))

# Order must match your DDL (11 headers in this exact order)
decoded_headers = [
    # 1) header_ck: text
    F.when(B64("header_ck").isNotNull(), F.decode(B64("header_ck"), "UTF-8")).otherwise(H("header_ck")).alias("header_ck"),
    # 2) header_clid: 16 bytes -> 4×int32 BE -> "a.b.c.d"
    F.when(B64("header_clid").isNotNull(), decode_clid_udf(B64("header_clid"))).otherwise(H("header_clid")).alias("header_clid"),
    # 3) header_gt: 8-byte BE long (millis) -> decimal string
    F.when(B64("header_gt").isNotNull(), be64_to_str_udf(B64("header_gt"))).otherwise(H("header_gt")).alias("header_gt"),
    # 4) header_ip: text
    F.when(B64("header_ip").isNotNull(), F.decode(B64("header_ip"), "UTF-8")).otherwise(H("header_ip")).alias("header_ip"),
    # 5) header_offset: 8-byte BE long -> decimal string
    F.when(B64("header_offset").isNotNull(), be64_to_str_udf(B64("header_offset"))).otherwise(H("header_offset")).alias("header_offset"),
    # 6) header_tp: 4-byte BE int -> decimal string
    F.when(B64("header_tp").isNotNull(), be32_to_str_udf(B64("header_tp"))).otherwise(H("header_tp")).alias("header_tp"),
    # 7) header_trace_id: text
    F.when(B64("header_trace_id").isNotNull(), F.decode(B64("header_trace_id"), "UTF-8")).otherwise(H("header_trace_id")).alias("header_trace_id"),
    # 8) header_uid: text
    F.when(B64("header_uid").isNotNull(), F.decode(B64("header_uid"), "UTF-8")).otherwise(H("header_uid")).alias("header_uid"),
    # 9) header_v: 1..8-byte unsigned BE -> decimal string
    F.when(B64("header_v").isNotNull(), var_be_uint_str_udf(B64("header_v"))).otherwise(H("header_v")).alias("header_v"),
    # 10) header_conid: text
    F.when(B64("header_conid").isNotNull(), F.decode(B64("header_conid"), "UTF-8")).otherwise(H("header_conid")).alias("header_conid"),
    # 11) header_st: text
    F.when(B64("header_st").isNotNull(), F.decode(B64("header_st"), "UTF-8")).otherwise(H("header_st")).alias("header_st"),
]

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
# 4) Build final DataFrame (order matches your DDL)
# -----------------------
# NOTE: We do not include the 'hdr' map column in the final output,
#       only the decoded header_* columns in the exact DDL order.
final_cols = [key_text] + decoded_headers + [c.alias(name) for name, c in cols]
df = df1.select(*final_cols)

# -----------------------
# 5) Write to Iceberg
# -----------------------
print(f"📝 Writing to Iceberg table: {args.table}")
(df.write
 .format("iceberg")
 .mode("append")
 .save(args.table))

print(f"✅ Ingest completed -> {args.table}")
