package io.github.bpdbi.pg.impl.codec;

/** Postgres type OIDs used for binary encoding/decoding. */
final class PgOIDs {

  static final int BOOL = 16;
  static final int BYTEA = 17;
  static final int CHAR = 18;
  static final int NAME = 19;
  static final int INT8 = 20;
  static final int INT2 = 21;
  static final int INT4 = 23;
  static final int TEXT = 25;
  static final int JSON = 114;
  static final int XML = 142;
  static final int FLOAT4 = 700;
  static final int FLOAT8 = 701;
  static final int MONEY = 790;
  static final int MACADDR = 829;
  static final int INET = 869;
  static final int CIDR = 650;
  static final int MACADDR8 = 774;
  static final int BPCHAR = 1042;
  static final int VARCHAR = 1043;
  static final int DATE = 1082;
  static final int TIME = 1083;
  static final int TIMESTAMP = 1114;
  static final int TIMESTAMPTZ = 1184;
  static final int INTERVAL = 1186;
  static final int TIMETZ = 1266;
  static final int BIT = 1560;
  static final int VARBIT = 1562;
  static final int NUMERIC = 1700;
  static final int UUID = 2950;
  static final int POINT = 600;
  static final int LINE = 628;
  static final int LSEG = 601;
  static final int BOX = 603;
  static final int PATH = 602;
  static final int POLYGON = 604;
  static final int CIRCLE = 718;
  static final int JSONB = 3802;
  static final int TSVECTOR = 3614;
  static final int TSQUERY = 3615;

  // Array types
  static final int BOOL_ARRAY = 1000;
  static final int BYTEA_ARRAY = 1001;
  static final int INT2_ARRAY = 1005;
  static final int INT4_ARRAY = 1007;
  static final int TEXT_ARRAY = 1009;
  static final int INT8_ARRAY = 1016;
  static final int FLOAT4_ARRAY = 1021;
  static final int FLOAT8_ARRAY = 1022;
  static final int UUID_ARRAY = 2951;
  static final int DATE_ARRAY = 1182;
  static final int TIME_ARRAY = 1183;
  static final int TIMESTAMP_ARRAY = 1115;
  static final int TIMESTAMPTZ_ARRAY = 1185;
  static final int NUMERIC_ARRAY = 1231;
  static final int TIMETZ_ARRAY = 1270;

  private PgOIDs() {}
}
