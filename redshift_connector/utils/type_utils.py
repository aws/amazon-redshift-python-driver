import enum
import typing
from collections import defaultdict
from datetime import date
from datetime import datetime as Datetime
from datetime import time
from datetime import timedelta as Timedelta
from datetime import timezone as Timezone
from decimal import Decimal
from enum import Enum
from json import loads
from struct import Struct

from redshift_connector.config import (
    EPOCH,
    EPOCH_SECONDS,
    EPOCH_TZ,
    FC_BINARY,
    FC_TEXT,
    _client_encoding,
    timegm,
)
from redshift_connector.pg_types import (
    PGEnum,
    PGJson,
    PGJsonb,
    PGText,
    PGTsvector,
    PGVarchar,
)

ANY_ARRAY = 2277
BIGINT = 20
BIGINT_ARRAY = 1016
BOOLEAN = 16
BOOLEAN_ARRAY = 1000
BYTES = 17
BYTES_ARRAY = 1001
CHAR = 1042
CHAR_ARRAY = 1014
CIDR = 650
CIDR_ARRAY = 651
CSTRING = 2275
CSTRING_ARRAY = 1263
DATE = 1082
DATE_ARRAY = 1182
FLOAT = 701
FLOAT_ARRAY = 1022
GEOMETRY = 3000
INET = 869
INET_ARRAY = 1041
INT2VECTOR = 22
INTEGER = 23
INTEGER_ARRAY = 1007
INTERVAL = 1186
INTERVAL_ARRAY = 1187
OID = 26
JSON = 114
JSON_ARRAY = 199
JSONB = 3802
JSONB_ARRAY = 3807
MACADDR = 829
MONEY = 790
MONEY_ARRAY = 791
NAME = 19
NAME_ARRAY = 1003
NUMERIC = 1700
NUMERIC_ARRAY = 1231
NULLTYPE = -1
OID = 26
POINT = 600
REAL = 700
REAL_ARRAY = 1021
SMALLINT = 21
SMALLINT_ARRAY = 1005
SMALLINT_VECTOR = 22
STRING = 1043
SUPER = 4000
TEXT = 25
TEXT_ARRAY = 1009
TIME = 1083
TIME_ARRAY = 1183
TIMESTAMP = 1114
TIMESTAMP_ARRAY = 1115
TIMESTAMPTZ = 1184
TIMESTAMPTZ_ARRAY = 1185
TIMETZ = 1266
UNKNOWN = 705
UUID_TYPE = 2950
UUID_ARRAY = 2951
VARCHAR = 1043
VARCHAR_ARRAY = 1015
XID = 28

BIGINTEGER = BIGINT
DATETIME = TIMESTAMP
NUMBER = DECIMAL = NUMERIC
DECIMAL_ARRAY = NUMERIC_ARRAY
ROWID = OID


def pack_funcs(fmt: str) -> typing.Tuple[typing.Callable, typing.Callable]:
    struc: Struct = Struct("!" + fmt)
    return struc.pack, struc.unpack_from


i_pack, i_unpack = pack_funcs("i")
I_pack, I_unpack = pack_funcs("I")
h_pack, h_unpack = pack_funcs("h")
q_pack, q_unpack = pack_funcs("q")
d_pack, d_unpack = pack_funcs("d")
f_pack, f_unpack = pack_funcs("f")
iii_pack, iii_unpack = pack_funcs("iii")
ii_pack, ii_unpack = pack_funcs("ii")
qii_pack, qii_unpack = pack_funcs("qii")
dii_pack, dii_unpack = pack_funcs("dii")
ihihih_pack, ihihih_unpack = pack_funcs("ihihih")
ci_pack, ci_unpack = pack_funcs("ci")
bh_pack, bh_unpack = pack_funcs("bh")
cccc_pack, cccc_unpack = pack_funcs("cccc")
qq_pack, qq_unpack = pack_funcs("qq")


def text_recv(data: bytes, offset: int, length: int) -> str:
    return str(data[offset : offset + length], _client_encoding)


def bool_recv(data: bytes, offset: int, length: int) -> bool:
    return data[offset] == 1


# bytea
# def bytea_recv(data: bytearray, offset: int, length: int) -> bytearray:
#     return data[offset:offset + length]


def int8_recv(data: bytes, offset: int, length: int) -> int:
    return q_unpack(data, offset)[0]


def int2_recv(data: bytes, offset: int, length: int) -> int:
    return h_unpack(data, offset)[0]


def vector_in(data: bytes, idx: int, length: int) -> typing.List:
    return eval("[" + data[idx : idx + length].decode(_client_encoding).replace(" ", ",") + "]")


def int4_recv(data: bytes, offset: int, length: int) -> int:
    return i_unpack(data, offset)[0]


def int_in(data: bytes, offset: int, length: int) -> int:
    return int(data[offset : offset + length])


def oid_recv(data: bytes, offset: int, length: int) -> int:
    return I_unpack(data, offset)[0]


def json_in(data: bytes, offset: int, length: int) -> typing.Dict[str, typing.Any]:
    return loads(str(data[offset : offset + length], _client_encoding))


def float4_recv(data: bytes, offset: int, length: int) -> float:
    return f_unpack(data, offset)[0]


def float8_recv(data: bytes, offset: int, length: int) -> float:
    return d_unpack(data, offset)[0]


# def bytea_send(v: bytearray) -> bytearray:
#     return v


# def uuid_send(v: UUID) -> bytes:
#     return v.bytes


def bool_send(v: bool) -> bytes:
    return b"\x01" if v else b"\x00"


NULL: bytes = i_pack(-1)
NULL_BYTE: bytes = b"\x00"


def null_send(v) -> bytes:
    return NULL


# data is 64-bit integer representing microseconds since 2000-01-01
def timestamp_recv_integer(data: bytes, offset: int, length: int) -> typing.Union[Datetime, str, float]:
    micros: float = q_unpack(data, offset)[0]
    try:
        return EPOCH + Timedelta(microseconds=micros)
    except OverflowError:
        epoch_delta: Timedelta = Timedelta(seconds=EPOCH_SECONDS)
        d_delta: Timedelta = Timedelta(microseconds=micros)
        if d_delta < epoch_delta:
            return Datetime.min
        else:
            return Datetime.max


# data is 64-bit integer representing microseconds since 2000-01-01
def timestamp_send_integer(v: Datetime) -> bytes:
    return q_pack(int((timegm(v.timetuple()) - EPOCH_SECONDS) * 1e6) + v.microsecond)


def timestamptz_send_integer(v: Datetime) -> bytes:
    # timestamps should be sent as UTC.  If they have zone info,
    # convert them.
    return timestamp_send_integer(v.astimezone(Timezone.utc).replace(tzinfo=None))


# return a timezone-aware datetime instance if we're reading from a
# "timestamp with timezone" type.  The timezone returned will always be
# UTC, but providing that additional information can permit conversion
# to local.
def timestamptz_recv_integer(data: bytes, offset: int, length: int) -> typing.Union[str, Datetime, int]:
    micros: int = q_unpack(data, offset)[0]
    try:
        return EPOCH_TZ + Timedelta(microseconds=micros)
    except OverflowError:
        epoch_delta: Timedelta = Timedelta(seconds=EPOCH_SECONDS)
        d_delta: Timedelta = Timedelta(microseconds=micros)
        if d_delta < epoch_delta:
            return Datetime.min
        else:
            return Datetime.max


# def interval_send_integer(v: typing.Union[Interval, Timedelta]) -> bytes:
#     microseconds: int = v.microseconds
#     try:
#         microseconds += int(v.seconds * 1e6)  # type: ignore
#     except AttributeError:
#         pass
#
#     try:
#         months = v.months  # type: ignore
#     except AttributeError:
#         months = 0
#
#     return typing.cast(bytes, qii_pack(microseconds, v.days, months))


glbls: typing.Dict[str, type] = {"Decimal": Decimal}
trans_tab = dict(zip(map(ord, "{}"), "[]"))


# def array_in(data: bytes, idx: int, length: int) -> typing.List:
#     arr: typing.List[str] = []
#     prev_c = None
#     for c in data[idx:idx + length].decode(
#             _client_encoding).translate(
#         trans_tab).replace('NULL', 'None'):
#         if c not in ('[', ']', ',', 'N') and prev_c in ('[', ','):
#             arr.extend("Decimal('")
#         elif c in (']', ',') and prev_c not in ('[', ']', ',', 'e'):
#             arr.extend("')")
#
#         arr.append(c)
#         prev_c = c
#     return typing.cast(typing.List, eval(''.join(arr), glbls))


def numeric_in_binary(data: bytes, offset: int, length: int, scale: int) -> Decimal:
    raw_value: int

    if length == 8:
        raw_value = q_unpack(data, offset)[0]
    elif length == 16:
        temp: typing.Tuple[int, int] = qq_unpack(data, offset)
        raw_value = (temp[0] << 64) | temp[1]
    else:
        raise Exception("Malformed column value of type numeric received")

    return Decimal(raw_value).scaleb(-1 * scale)


def numeric_in(data: bytes, offset: int, length: int) -> Decimal:
    return Decimal(data[offset : offset + length].decode(_client_encoding))


# def uuid_recv(data: bytes, offset: int, length: int) -> UUID:
#     return UUID(bytes=data[offset:offset+length])


# def interval_recv_integer(data: bytes, offset: int, length: int) -> typing.Union[Timedelta, Interval]:
#     microseconds, days, months = typing.cast(
#         typing.Tuple[int, ...], qii_unpack(data, offset)
#     )
#     if months == 0:
#         seconds, micros = divmod(microseconds, 1e6)
#         return Timedelta(days, seconds, micros)
#     else:
#         return Interval(microseconds, days, months)


def timetz_recv_binary(data: bytes, offset: int, length: int) -> time:
    return time_recv_binary(data, offset, length).replace(tzinfo=Timezone.utc)


# data is 64-bit integer representing microseconds
def time_recv_binary(data: bytes, offset: int, length: int) -> time:
    millis: float = q_unpack(data, offset)[0] / 1000

    if length == 12:
        time_offset: int = i_unpack(data, offset + 8)[0]  # tz lives after time
        time_offset *= -1000
        millis -= time_offset

    q, r = divmod(millis, 1000)
    micros: float = r * 1000  # maximum of six digits of precision for fractional seconds.
    q, r = divmod(q, 60)
    seconds: float = r
    q, r = divmod(q, 60)
    minutes: float = r
    hours: float = q
    return time(hour=int(hours), minute=int(minutes), second=int(seconds), microsecond=int(micros))


def time_in(data: bytes, offset: int, length: int) -> time:
    hour: int = int(data[offset : offset + 2])
    minute: int = int(data[offset + 3 : offset + 5])
    sec: Decimal = Decimal(data[offset + 6 : offset + length].decode(_client_encoding))
    return time(hour, minute, int(sec), int((sec - int(sec)) * 1000000))


def timetz_in(data: bytes, offset: int, length: int) -> time:
    hour: int = int(data[offset : offset + 2])
    minute: int = int(data[offset + 3 : offset + 5])
    sec: Decimal = Decimal(data[offset + 6 : offset + 8].decode(_client_encoding))
    microsec: int = int((sec - int(sec)) * 1000000)

    if length != 8:
        idx_tz: int = offset + 8
        # if microsec present, they start with '.'
        if data[idx_tz : idx_tz + 1] == b".":
            end_microseconds: int = length + offset
            for idx in range(idx_tz + 1, len(data)):
                if data[idx] == 43 or data[idx] == 45:  # +/- char indicates start of tz offset
                    end_microseconds = idx
                    break

            microsec += int(data[idx_tz + 1 : end_microseconds])
    return time(hour, minute, int(sec), microsec, tzinfo=Timezone.utc)


def date_recv_binary(data: bytes, offset: int, length: int) -> date:
    # 86400 seconds per day
    seconds: float = i_unpack(data, offset)[0] * 86400

    # Julian/Gregorian calendar cutoff point
    if seconds < -12219292800:  # October 4, 1582 -> October 15, 1582
        seconds += 864000  # add 10 days worth of seconds
        if seconds < -14825808000:  # 1500-02-28 -> 1500-03-01
            extraLeaps: float = (seconds + 14825808000) / 3155760000
            extraLeaps -= 1
            extraLeaps -= extraLeaps / 4
            seconds += extraLeaps * 86400

    microseconds: float = seconds * 1e6

    try:
        return (EPOCH + Timedelta(microseconds=microseconds)).date()
    except OverflowError:
        if Timedelta(microseconds=microseconds) < Timedelta(seconds=EPOCH_SECONDS):
            return date.min
        else:
            return date.max
    except Exception as e:
        raise e


def date_in(data: bytes, offset: int, length: int) -> date:
    d: str = data[offset : offset + length].decode(_client_encoding)

    # datetime module does not support BC dates, so return min date
    if d[-1] == "C":
        return date.min
    try:
        return date(int(d[:4]), int(d[5:7]), int(d[8:10]))
    except ValueError:
        # likely occurs if a date > datetime.datetime.max
        return date.max


class ArrayState(Enum):
    InString = 1
    InEscape = 2
    InValue = 3
    Out = 4


# parses an array received in text format. currently all elements are returned as strings.


def _parse_array(adapter: typing.Optional[typing.Callable], data: bytes, offset: int, length: int) -> typing.List:
    state: ArrayState = ArrayState.Out
    stack: typing.List = [[]]
    val: typing.List[str] = []
    str_data: str = text_recv(data, offset, length)

    for c in str_data:
        if state == ArrayState.InValue:
            if c in ("}", ","):
                value: typing.Optional[str] = "".join(val)
                if value == "NULL":
                    value = None
                elif adapter is not None:
                    value = adapter(value)
                stack[-1].append(value)
                state = ArrayState.Out
            else:
                val.append(c)

        if state == ArrayState.Out:
            if c == "{":
                a: typing.List = []
                stack[-1].append(a)
                stack.append(a)
            elif c == "}":
                stack.pop()
            elif c == ",":
                pass
            elif c == '"':
                val = []
                state = ArrayState.InString
            else:
                val = [c]
                state = ArrayState.InValue

        elif state == ArrayState.InString:
            if c == '"':
                value = "".join(val)
                if adapter is not None:
                    value = adapter(value)  # type: ignore
                stack[-1].append(value)
                state = ArrayState.Out
            elif c == "\\":
                state = ArrayState.InEscape
            else:
                val.append(c)
        elif state == ArrayState.InEscape:
            val.append(c)
            state = ArrayState.InString

    return stack[0][0]


def _array_in(adapter: typing.Optional[typing.Callable] = None):
    def f(data: bytes, offset: int, length: int):
        return _parse_array(adapter, data, offset, length)

    return f


array_recv_text: typing.Callable = _array_in()
int_array_recv: typing.Callable = _array_in(lambda data: int(data))
float_array_recv: typing.Callable = _array_in(lambda data: float(data))


def array_recv_binary(data: bytes, idx: int, length: int) -> typing.List:
    final_idx: int = idx + length
    dim, hasnull, typeoid = iii_unpack(data, idx)
    idx += 12

    # get type conversion method for typeoid
    conversion: typing.Callable = pg_types[typeoid][1]

    # Read dimension info
    dim_lengths: typing.List = []
    for i in range(dim):
        dim_lengths.append(ii_unpack(data, idx)[0])
        idx += 8

    # Read all array values
    values: typing.List = []
    while idx < final_idx:
        (element_len,) = i_unpack(data, idx)
        idx += 4
        if element_len == -1:
            values.append(None)
        else:
            values.append(conversion(data, idx, element_len))
            idx += element_len

    # at this point, {{1,2,3},{4,5,6}}::int[][] looks like
    # [1,2,3,4,5,6]. go through the dimensions and fix up the array
    # contents to match expected dimensions
    for length in reversed(dim_lengths[1:]):
        values = list(map(list, zip(*[iter(values)] * length)))
    return values


# def inet_in(data: bytes, offset: int, length: int) -> typing.Union[IPv4Address, IPv6Address, IPv4Network, IPv6Network]:
#     inet_str: str = data[offset: offset + length].decode(
#         _client_encoding)
#     if '/' in inet_str:
#         return typing.cast(typing.Union[IPv4Network, IPv6Network], ip_network(inet_str, False))
#     else:
#         return typing.cast(typing.Union[IPv4Address, IPv6Address], ip_address(inet_str))


pg_types: typing.DefaultDict[int, typing.Tuple[int, typing.Callable]] = defaultdict(
    lambda: (FC_TEXT, text_recv),
    {
        BOOLEAN: (FC_BINARY, bool_recv),  # boolean
        # 17: (FC_BINARY, bytea_recv),  # bytea
        NAME: (FC_BINARY, text_recv),  # name type
        BIGINT: (FC_BINARY, int8_recv),  # int8
        SMALLINT: (FC_BINARY, int2_recv),  # int2
        SMALLINT_VECTOR: (FC_TEXT, vector_in),  # int2vector
        INTEGER: (FC_BINARY, int4_recv),  # int4
        24: (FC_BINARY, oid_recv),  # regproc
        TEXT: (FC_BINARY, text_recv),  # TEXT type
        OID: (FC_BINARY, oid_recv),  # oid
        XID: (FC_TEXT, int_in),  # xid
        JSON: (FC_TEXT, json_in),  # json
        REAL: (FC_BINARY, float4_recv),  # float4
        FLOAT: (FC_BINARY, float8_recv),  # float8
        UNKNOWN: (FC_BINARY, text_recv),  # unknown
        # 829: (FC_TEXT, text_recv),  # MACADDR type
        # 869: (FC_TEXT, inet_in),  # inet
        # 1000: (FC_BINARY, array_recv),  # BOOL[]
        # 1003: (FC_BINARY, array_recv),  # NAME[]
        SMALLINT_ARRAY: (FC_BINARY, array_recv_binary),  # INT2[]
        INTEGER_ARRAY: (FC_BINARY, array_recv_binary),  # INT4[]
        TEXT_ARRAY: (FC_BINARY, array_recv_binary),  # TEXT[]
        1002: (FC_BINARY, array_recv_binary),  # CHAR[]
        # 1014: (FC_BINARY, array_recv_text),  # BPCHAR[]
        1028: (FC_BINARY, int_array_recv),  # OID[]
        1033: (FC_BINARY, text_recv),  # ACLITEM
        1034: (FC_BINARY, array_recv_binary),  # ACLITEM[]
        VARCHAR_ARRAY: (FC_BINARY, array_recv_binary),  # VARCHAR[]
        # 1016: (FC_BINARY, array_recv),  # INT8[]
        REAL_ARRAY: (FC_BINARY, array_recv_binary),  # FLOAT4[]
        # 1022: (FC_BINARY, array_recv),  # FLOAT8[]
        CHAR: (FC_BINARY, text_recv),  # CHAR type
        STRING: (FC_BINARY, text_recv),  # VARCHAR type
        DATE: (FC_BINARY, date_recv_binary),  # date
        TIME: (FC_BINARY, time_recv_binary),  # time
        TIMESTAMP: (FC_BINARY, timestamp_recv_integer),  # timestamp
        TIMESTAMPTZ: (FC_BINARY, timestamptz_recv_integer),  # timestamptz
        TIMETZ: (FC_BINARY, timetz_recv_binary),  # timetz
        # 1186: (FC_BINARY, interval_recv_integer),
        # 1231: (FC_TEXT, array_in),  # NUMERIC[]
        # 1263: (FC_BINARY, array_recv),  # cstring[]
        NUMERIC: (FC_BINARY, numeric_in_binary),  # NUMERIC
        # 2275: (FC_BINARY, text_recv),  # cstring
        # 2950: (FC_BINARY, uuid_recv),  # uuid
        GEOMETRY: (FC_TEXT, text_recv),  # GEOMETRY
        # 3802: (FC_TEXT, json_in),  # jsonb
        SUPER: (FC_TEXT, text_recv),  # SUPER
    },
)


def text_out(v: typing.Union[PGText, PGVarchar, PGJson, PGJsonb, PGTsvector, str]) -> bytes:
    return v.encode(_client_encoding)


def enum_out(v: typing.Union[PGEnum, enum.Enum]) -> bytes:
    return str(v.value).encode(_client_encoding)


def time_out(v: time) -> bytes:
    return v.isoformat().encode(_client_encoding)


def date_out(v: date) -> bytes:
    return v.isoformat().encode(_client_encoding)


def unknown_out(v) -> bytes:
    return str(v).encode(_client_encoding)


def numeric_out(d: Decimal) -> bytes:
    return str(d).encode(_client_encoding)


# def inet_out(v: typing.Union[IPv4Address, IPv6Address, IPv4Network, IPv6Network]) -> bytes:
#     return str(v).encode(_client_encoding)


py_types: typing.Dict[typing.Union[type, int], typing.Tuple[int, int, typing.Callable]] = {
    type(None): (-1, FC_BINARY, null_send),  # null
    bool: (16, FC_BINARY, bool_send),
    # bytearray: (17, FC_BINARY, bytea_send),  # bytea
    BIGINT: (BIGINT, FC_BINARY, q_pack),  # int8
    SMALLINT: (SMALLINT, FC_BINARY, h_pack),  # int2
    INTEGER: (INTEGER, FC_BINARY, i_pack),  # int4
    PGText: (TEXT, FC_TEXT, text_out),  # text
    float: (FLOAT, FC_BINARY, d_pack),  # float8
    PGEnum: (UNKNOWN, FC_TEXT, enum_out),
    date: (DATE, FC_TEXT, date_out),  # date
    time: (TIME, FC_TEXT, time_out),  # time
    TIMESTAMP: (TIMESTAMP, FC_BINARY, timestamp_send_integer),  # timestamp
    # timestamp w/ tz
    PGVarchar: (STRING, FC_TEXT, text_out),  # varchar
    TIMESTAMPTZ: (TIMESTAMPTZ, FC_BINARY, timestamptz_send_integer),
    PGJson: (JSON, FC_TEXT, text_out),
    # PGJsonb: (3802, FC_TEXT, text_out),
    # Timedelta: (1186, FC_BINARY, interval_send_integer),
    # Interval: (1186, FC_BINARY, interval_send_integer),
    Decimal: (NUMERIC, FC_TEXT, numeric_out),  # Decimal
    PGTsvector: (3614, FC_TEXT, text_out),
    # UUID: (2950, FC_BINARY, uuid_send),  # uuid
    # bytes: (17, FC_BINARY, bytea_send),  # bytea
    str: (UNKNOWN, FC_TEXT, text_out),  # unknown
    enum.Enum: (UNKNOWN, FC_TEXT, enum_out),
    # IPv4Address: (869, FC_TEXT, inet_out),  # inet
    # IPv6Address: (869, FC_TEXT, inet_out),  # inet
    # IPv4Network: (869, FC_TEXT, inet_out),  # inet
    # IPv6Network: (869, FC_TEXT, inet_out)  # inet
}
