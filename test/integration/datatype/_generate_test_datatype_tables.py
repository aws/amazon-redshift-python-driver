import os
import pathlib
import typing
from datetime import date, datetime, time, timezone
from decimal import Decimal
from enum import Enum, auto

if typing.TYPE_CHECKING:
    from redshift_connector import Connection

SCHEMA_NAME: str = "datatype_integration"
CREATE_FILE_PATH: str = "{}/datatype_test_stmts.sql".format(pathlib.Path().absolute())
TEARDOWN_FILE_PATH: str = "{}/datatype_teardown_stmts.sql".format(pathlib.Path().absolute())
"""
This file generates a sql file that creates db resources used for testing datatype support.
The file generated is run directly with psql to bypass redshift_connector.
"""


class RedshiftDatatypes(Enum):
    """
    All redshift specific datatypes are defined here
    """

    super = auto()
    geometry = auto()
    varbyte = auto()

    @classmethod
    def list(cls) -> typing.List["RedshiftDatatypes"]:
        return list(map(lambda p: p, cls))  # type: ignore


redshift_test_data: typing.Dict[
    str, typing.Union[typing.Tuple[typing.Tuple[str, str], ...], typing.List[typing.Tuple[str, ...]]]
] = {
    RedshiftDatatypes.geometry.name: (
        (
            "ST_GeomFromText('LINESTRING(1 2,3 4,5 6,7 8,9 10,11 12,13 14,15 16,17 18,19 20)')",
            (
                "01020000000a000000000000000000f03f000000000000004000000000000008400000000000001040000000000000144000000000"
                "000018400000000000001c400000000000002040000000000000224000000000000024400000000000002640000000000000284000"
                "00000000002a400000000000002c400000000000002e40000000000000304000000000000031400000000000003240000000000000"
                "33400000000000003440"
            ),
        ),
        (
            "ST_GeomFromText('POLYGON((0 0,0 1,1 1,1 0,0 0))',4326)",
            (
                "0103000020e61000000100000005000000000000000000000000000000000000000000000000000000000000000000f03f00000000"
                "0000f03f000000000000f03f000000000000f03f000000000000000000000000000000000000000000000000"
            ),
        ),
        (
            (
                "ST_GeomFromEWKB('0103000020E61000000100000005000000000000000000000000000000000000000000000000000000000000000000F03F000000"
                "000000F03F000000000000F03F000000000000F03F000000000000000000000000000000000000000000000000')"
            ),
            (
                "0103000020e61000000100000005000000000000000000000000000000000000000000000000000000000000000000f03f000000"
                "000000f03f000000000000f03f000000000000f03f000000000000000000000000000000000000000000000000"
            ),
        ),
        (
            (
                "ST_GeomFromEWKB('01020000000A000000000000000000F03F0000000000000040000000000000084000000000000010400000000000001440000000"
                "00000018400000000000001C4000000000000020400000000000002240000000000000244000000000000026400000000000002840"
                "0000000000002A400000000000002C400000000000002E400000000000003040000000000000314000000000000032400000000000"
                "0033400000000000003440')"
            ),
            (
                "01020000000a000000000000000000f03f0000000000000040000000000000084000000000000010400000000000001440000000"
                "00000018400000000000001c4000000000000020400000000000002240000000000000244000000000000026400000000000002840"
                "0000000000002a400000000000002c400000000000002e400000000000003040000000000000314000000000000032400000000000"
                "0033400000000000003440"
            ),
        ),
        (
            (
                "ST_GeomFromWKB('01030000000100000005000000000000000000000000000000000000000000000000000000000000000000F03"
                "F000000000000F03F000000000000F03F000000000000F03F000000000000000000000000000000000000000000000000')"
            ),
            (
                "01030000000100000005000000000000000000000000000000000000000000000000000000000000000000f03"
                "f000000000000f03f000000000000f03f000000000000f03f000000000000000000000000000000000000000000000000"
            ),
        ),
    ),
    RedshiftDatatypes.varbyte.name: [
        ("'{}'::varbyte".format(x), x)
        for x in (
            "00",
            "1111",
            "11",
            "222222",
            "22",
            "33333333",
            "33",
            "44444444",
            "44",
            "5555555555",
            "55",
            "666666666666",
            "66",
            "77777777777777",
            "77",
            "8888888888888888",
            "88",
            "999999999999999999",
            "99",
            "aa",
            "aa",
            "bbbb",
            "bb",
            "cccccc",
            "cc",
            "dddddddd",
            "dd",
            "eeeeeeeeee",
            "ee",
            "ffffffffffff",
            "ff",
            "aa00",
            "00",
            "bb1111",
            "1111",
            "cc222222",
            "222222",
            "dd33333333",
            "33333333",
            "ee4444444444",
            "4444444444",
            "ff555555555555",
            "555555555555",
            "0066666666666666",
            "66666666666666",
            "117777777777777777",
            "7777777777777777",
            "22888888888888888888",
            "888888888888888888",
            "3399",
            "99",
            "44aaaa",
            "aaaa",
            "55bbbbbb",
            "bbbbbb",
            "66cccccccc",
            "cccccccc",
            "77dddddddddd",
            "dddddddddd",
            "88eeeeeeeeeeee",
            "eeeeeeeeeeee",
            "99ffffffffffffff",
            "ffffffffffffff",
        )
    ],
}


class Datatypes(Enum):
    """
    All non-redshift specific datatypes are defined here, so a test table can be created for each
    """

    int2 = auto()
    int4 = auto()
    int8 = auto()
    numeric = "(18, 8)"  # toggle precision between 18 and 38 to test 8 & 16  byte. scale must be >= 8
    float4 = auto()
    float8 = auto()
    bool = auto()
    char = auto()
    varchar = auto()
    date = auto()
    timestamp = auto()
    timestamptz = auto()
    time = auto()
    timetz = auto()

    @classmethod
    def list(cls) -> typing.List["Datatypes"]:
        return list(map(lambda p: p, cls))  # type: ignore


FLOAT_DATATYPES: typing.Tuple[Datatypes, ...] = (Datatypes.float4, Datatypes.float8, Datatypes.numeric)

DATATYPES_WITH_MS: typing.Tuple[Datatypes, ...] = (Datatypes.timetz, Datatypes.timestamptz)

# test_data is structured as follows.
# 1) a description of the test row.
# 2) the test value.
# 3) (Optional) the Python value we expect to receive. If this field is missing,
# we expect to receive the test value back directly.

test_data: typing.Dict[str, typing.Tuple[typing.Tuple[typing.Any, ...], ...]] = {
    Datatypes.int2.name: (  # smallint
        ("-32768", -32768),  # signed 2 byte int min
        ("-128", -128),
        ("-1", -1),
        ("0", 0),
        ("1", 1),
        ("2", 2),
        ("123", 123),
        ("127", 127),
        ("32767", 32767),  # signed 2 byte int max
    ),
    Datatypes.int4.name: (  # integer
        ("-2147483648", -2147483648),  # signed 4 byte int min
        ("-32768", -32768),  # signed 2 byte int min
        ("-128", -128),
        ("-1", -1),
        ("0", 0),
        ("1", 1),
        ("2", 2),
        ("123", 123),
        ("127", 127),
        ("32767", 32767),  # signed 2 byte int max
        ("2147483647", 2147483647),  # signed 4 byte int max
    ),
    Datatypes.int8.name: (  # bigint
        ("-9223372036854775808", -9223372036854775808),  # signed 8 byte int min
        ("-2147483648", -2147483648),  # signed 4 byte int min
        ("-32768", -32768),  # signed 2 byte int min
        ("-128", -128),
        ("-1", -1),
        ("0", 0),
        ("1", 1),
        ("2", 2),
        ("123", 123),
        ("127", 127),
        ("32767", 32767),  # signed 2 byte int max
        ("2147483647", 2147483647),  # signed 4 byte int max
        ("9223372036854775807", 9223372036854775807),  # signed 8 byte int max
    ),
    Datatypes.numeric.name: (
        ("-2147483648", -2147483648, Decimal(-2147483648)),  # signed 4 byte int min
        ("-32768", -32768, Decimal(-32768)),  # signed 2 byte int min
        ("-12345.67891", -12345.67891, Decimal("-12345.67891")),
        ("-128", -128, Decimal(-128)),
        ("-1", -1, Decimal(-1)),
        ("-0.11", -0.11, Decimal(-0.11)),
        ("0", 0, Decimal(0)),
        ("0.00012345", 0.00012345, Decimal(0.00012345)),
        ("0.1", 0.2, Decimal(0.2)),
        ("1", 1, Decimal(1)),
        ("1.5", 1.5, Decimal(1.5)),
        ("2", 2, Decimal(2)),
        ("123", 123, Decimal(123)),
        ("127", 127, Decimal(127)),
        ("12345.67891", 12345.67891, Decimal("12345.67891")),
        ("32767", 32767, Decimal(32767)),  # signed 2 byte int max
        ("2147483647", 2147483647, Decimal(2147483647)),  # signed 4 byte int max
    ),
    Datatypes.float4.name: (  # real
        ("-2147483648.0001", -2147483648.0001),
        ("-2147483648", -2147483648),  # signed 4 byte int min
        ("-32768", -32768),  # signed 2 byte int min
        ("-32767.0", -32767.0),
        ("-128.497839", -128.497839),
        ("-128", -128),
        ("-1.000000000001", -1.000000000001),
        ("-1", -1),
        ("-0.465890", -0.465890),
        ("9e-6", 9e-6),
        ("0", 0),
        ("1", 1),
        ("1.9", 1.9),
        ("1.0", 1.0),
        ("2", 2),
        ("123", 123),
        ("123.456", 123.456),
        ("127", 127),
        ("127.890", 127.890),
        ("32767", 32767),  # signed 2 byte int max
        ("12345678.901234", 12345678.901234),
        ("2147483647", 2147483647),  # signed 4 byte int max,
    ),
    Datatypes.float8.name: (  # double precision
        ("-2147483648.0001", -2147483648.0001),
        ("-2147483648", -2147483648),  # signed 4 byte int min
        ("-12345678.123456789123456", 12345678.132456789123456),
        ("-32768", -32768),  # signed 2 byte int min
        ("-32767.0", -32767.0),
        ("-128.497839", -128.497839),
        ("-128", -128),
        ("-1.000000000001", -1.000000000001),
        ("-1", -1),
        ("-0.465890", -0.465890),
        ("9e-6", 9e-6),
        ("0", 0),
        ("0.00000006733", 0.00000006733),
        ("1", 1),
        ("1.9", 1.9),
        ("1.0", 1.0),
        ("2", 2),
        ("123", 123),
        ("123.456", 123.456),
        ("127", 127),
        ("127.890", 127.890),
        ("32767", 32767),  # signed 2 byte int max
        ("12345678.123456789123456", 12345678.132456789123456),
        ("12345678.901234", 12345678.901234),
        ("2147483647", 2147483647),  # signed 4 byte int max,
    ),
    Datatypes.bool.name: (
        ("TRUE", "TRUE", True),
        ("t", "t", True),
        ("true", "true", True),
        ("y", "y", True),
        ("yes", "yes", True),
        ("1", "1", True),
        ("FALSE", "FALSE", False),
        ("f", "f", False),
        ("false", "false", False),
        ("n", "n", False),
        ("no", "no", False),
        ("0", "0", False),
    ),
    Datatypes.char.name: tuple(
        ("chr({})".format(i), chr(i))
        for i in list(range(32, 39)) + list(range(40, 92)) + list(range(93, 128))
        # skip ' \ some control chars
        # ref: https://www.utf8-chartable.de/unicode-utf8-table.pl?utf8=dec
    ),
    Datatypes.varchar.name: (
        ("empty", ""),
        ("negative one", "-1"),
        ("zero", "0"),
        ("one", "1"),
        ("special characters", "~!@#$%^&*()_+{}|:<>?"),
        ("uuid", "123e4567-e89b-12d3-a456-426614174000"),
        (
            "bin",
            "01100100 01101111 01101111 01100110 00100000 01100100 01101111 01101111 01100110 00100000 01110101 01101110 01110100 01111010 00100000 01110101 01101110 01110100 01111010 ",
        ),
        ("hex", "646f6f6620646f6f6620756e747a20756e747a"),
        ("oct", "144 157 157 146 040 144 157 157 146 040 165 156 164 172 040 165 156 164 172"),
        ("ascii", "[)00|= [)00|= (_)|V72 (_)|V72"),
        ("euro", "€€€€"),
        ("string", "The quick brown fox jumps over the lazy dog"),
        (
            "string with trailing spaces",
            "The quick brown fox jumps over the lazy dog                                                                                               ",
        ),
    ),
    Datatypes.date.name: (
        ("julian date", "4713-01-12", date(year=4713, month=1, day=12)),
        ("mm/dd/yyy", "01-06-2020", date(year=2020, month=1, day=6)),
        ("yyyy-mm-dd", "2020-01-06", date(year=2020, month=1, day=6)),
        ("mm.dd.yyyy", "01.20.2020", date(year=2020, month=1, day=20)),
        ("max date", "294276-01-02", date.max),  # too big for datetime.date
        ("some day", "01-01-1900", date(year=1900, month=1, day=1)),
        ("feb 29 2020", "02-29-2020", date(year=2020, month=2, day=29)),
    ),
    Datatypes.timestamp.name: (
        ("julian date", "4713-01-12 00:00:00", datetime(year=4713, month=1, day=12, hour=0, minute=0, second=0)),
        ("jun 1 2008", "Jun 1,2008  09:59:59", datetime(year=2008, month=6, day=1, hour=9, minute=59, second=59)),
        ("dec 31 2008", "Dec 31,2008 18:20", datetime(year=2008, month=12, day=31, hour=18, minute=20, second=0)),
        ("feb 29, 2020", "02-29-2020 00:00:00", datetime(year=2020, month=2, day=29, hour=0, minute=0, second=0)),
        ("max date", "294276-01-02 23:59:59", datetime.max),  # too big for datetime.datetime
    ),
    Datatypes.timestamptz.name: (
        (
            "julian date",
            "4713-01-12 00:00:00 UTC",
            datetime(year=4713, month=1, day=12, hour=0, minute=0, second=0, tzinfo=timezone.utc),
        ),
        (
            "jun 1 2008",
            "Jun 1,2008  09:59:59 EST",
            datetime(year=2008, month=6, day=1, hour=14, minute=59, second=59, tzinfo=timezone.utc),
        ),
        (
            "dec 31 2008",
            "Dec 31,2008 18:20 US/Pacific",
            datetime(year=2009, month=1, day=1, hour=2, minute=20, second=0, tzinfo=timezone.utc),
        ),
        (
            "feb 29, 2020",
            "02-29-2020 00:00:00 UTC",
            datetime(year=2020, month=2, day=29, hour=0, minute=0, second=0, tzinfo=timezone.utc),
        ),
        ("max date", "294276-01-02 23:59:59 UTC", datetime.max),  # too big for datetime.datetime
    ),
    Datatypes.time.name: (
        ("early", "00:00:00", time(hour=0, minute=0, second=0)),
        ("noon", "12:30:10", time(hour=12, minute=30, second=10)),
        ("evening", "18:42:22", time(hour=18, minute=42, second=22)),
        ("night", "22:44:54", time(hour=22, minute=44, second=54)),
        ("end", "24:00:00", time(hour=0, minute=0)),
    ),
    Datatypes.timetz.name: (
        ("early", "00:00:00 EST", time(hour=5, minute=0, second=0, tzinfo=timezone.utc)),
        ("noon", "12:30:10 WDT", time(hour=3, minute=30, second=10, tzinfo=timezone.utc)),
        ("evening", "18:42:22 GMT", time(hour=18, minute=42, second=22, tzinfo=timezone.utc)),
        ("night", "22:44:54 CET", time(hour=21, minute=44, second=54, tzinfo=timezone.utc)),
        (
            "with micro1",
            "22:44:54.189717 CET",
            time(hour=21, minute=44, second=54, microsecond=189717, tzinfo=timezone.utc),
        ),
        ("with micro2", "22:44:54.18 CET", time(hour=21, minute=44, second=54, microsecond=18, tzinfo=timezone.utc)),
        ("end", "24:00:00 WET", time(hour=0, minute=0, second=0, tzinfo=timezone.utc)),
    ),
}


def get_table_name(dt: Datatypes) -> str:
    return "{schema}.test_{datatype}".format(schema=SCHEMA_NAME, datatype=dt.name)


def _make_data_str(dt: Datatypes) -> str:
    datas: typing.List[str] = []

    for row in test_data[dt.name]:
        # if the column storing test data is a string in test_data, insert it as a string
        test_col: str = "'{val}'".format(val=row[1]) if isinstance(row[1], str) else row[1]
        datas.append("('{c1_val}', {c2_val})".format(c1_val=row[0], c2_val=test_col))

    return ",".join(datas)


def _build_table_stmts(dt: Datatypes) -> None:
    drop_stmt: str = "drop table if exists {schema}.test_{datatype};".format(schema=SCHEMA_NAME, datatype=dt.name)

    col_type: str = dt.name
    if dt.name == Datatypes.numeric.name:
        col_type += dt.value

    create_stmt: str = "create table {schema}.test_{datatype} (c1 varchar, c2 {col_type});".format(
        schema=SCHEMA_NAME, datatype=dt.name, col_type=col_type
    )
    insert_stmt: str = "insert into {schema}.test_{datatype}(c1, c2) values{data};".format(
        schema=SCHEMA_NAME, datatype=dt.name, data=_make_data_str(dt)
    )

    with open(CREATE_FILE_PATH, "a") as f:
        f.write(drop_stmt + "\n")
        f.write(create_stmt + "\n")
        f.write(insert_stmt + "\n")


def _build_schema_stmts() -> None:
    drop_stmt: str = "drop schema if exists {name} cascade;".format(name=SCHEMA_NAME)
    create_stmt: str = "create schema {name};".format(name=SCHEMA_NAME)

    with open(CREATE_FILE_PATH, "a+") as f:
        f.write(drop_stmt + "\n")
        f.write(create_stmt + "\n")


def datatype_test_setup(conf) -> None:
    try:  # remove test sql file if exists
        os.remove(CREATE_FILE_PATH)
    except OSError:
        pass

    # build test sql file
    _build_schema_stmts()
    for dt in Datatypes:
        _build_table_stmts(dt)
    # execute test sql file
    os.system(
        "PGPASSWORD={password} psql --host={host} --port 5439 --user={user} --dbname={db} -f {file}".format(
            password=conf.get("ci-cluster", "test_password"),
            host=conf.get("ci-cluster", "host"),
            user=conf.get("ci-cluster", "test_user"),
            db=conf.get("ci-cluster", "database"),
            file=CREATE_FILE_PATH,
        )
    )


def datatype_test_teardown(conf) -> None:
    try:  # remove test sql file if exists
        os.remove(TEARDOWN_FILE_PATH)
    except OSError:
        pass
    with open(TEARDOWN_FILE_PATH, "a+") as f:
        f.write("drop schema if exists {name} cascade;".format(name=SCHEMA_NAME))

    os.system(
        "PGPASSWORD={password} psql --host={host} --port 5439 --user={user} --dbname={db} -f {file}".format(
            password=conf.get("ci-cluster", "test_password"),
            host=conf.get("ci-cluster", "host"),
            user=conf.get("ci-cluster", "test_user"),
            db=conf.get("ci-cluster", "database"),
            file=TEARDOWN_FILE_PATH,
        )
    )
