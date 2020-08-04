import enum
from collections import defaultdict
from datetime import date
from datetime import datetime as Datetime
from datetime import time
from datetime import timedelta as Timedelta
from datetime import timezone as Timezone
from decimal import Decimal
from ipaddress import (
    IPv4Address,
    IPv4Network,
    IPv6Address,
    IPv6Network,
    ip_address,
    ip_network,
)
from json import loads
from struct import Struct
from uuid import UUID

from redshift_connector.config import (
    EPOCH,
    EPOCH_SECONDS,
    EPOCH_TZ,
    FC_BINARY,
    FC_TEXT,
    INFINITY_MICROSECONDS,
    MINUS_INFINITY_MICROSECONDS,
    _client_encoding,
    timegm,
)
from redshift_connector.interval import Interval
from redshift_connector.pg_types import (
    PGEnum,
    PGJson,
    PGJsonb,
    PGText,
    PGTsvector,
    PGVarchar,
)


def pack_funcs(fmt):
    struc = Struct('!' + fmt)
    return struc.pack, struc.unpack_from


i_pack, i_unpack = pack_funcs('i')
h_pack, h_unpack = pack_funcs('h')
q_pack, q_unpack = pack_funcs('q')
d_pack, d_unpack = pack_funcs('d')
f_pack, f_unpack = pack_funcs('f')
iii_pack, iii_unpack = pack_funcs('iii')
ii_pack, ii_unpack = pack_funcs('ii')
qii_pack, qii_unpack = pack_funcs('qii')
dii_pack, dii_unpack = pack_funcs('dii')
ihihih_pack, ihihih_unpack = pack_funcs('ihihih')
ci_pack, ci_unpack = pack_funcs('ci')
bh_pack, bh_unpack = pack_funcs('bh')
cccc_pack, cccc_unpack = pack_funcs('cccc')


def text_recv(data, offset, length):
    return str(data[offset: offset + length], _client_encoding)


def bool_recv(data, offset, length):
    return data[offset] == 1


# bytea
def bytea_recv(data, offset, length):
    return data[offset:offset + length]


def int8_recv(data, offset, length):
    return q_unpack(data, offset)[0]


def int2_recv(data, offset, length):
    return h_unpack(data, offset)[0]


def vector_in(data, idx, length):
    return eval('[' + data[idx:idx + length].decode(
        _client_encoding).replace(' ', ',') + ']')


def int4_recv(data, offset, length):
    return i_unpack(data, offset)[0]


def int_in(data, offset, length):
    return int(data[offset: offset + length])


def json_in(data, offset, length):
    return loads(
        str(data[offset: offset + length], _client_encoding))


def float4_recv(data, offset, length):
    return f_unpack(data, offset)[0]


def float8_recv(data, offset, length):
    return d_unpack(data, offset)[0]


def bytea_send(v):
    return v


def uuid_send(v):
    return v.bytes


def bool_send(v):
    return b"\x01" if v else b"\x00"


NULL = i_pack(-1)
NULL_BYTE = b'\x00'


def null_send(v):
    return NULL


# data is double-precision float representing seconds since 2000-01-01
def timestamp_recv_float(data, offset, length):
    return Datetime.utcfromtimestamp(EPOCH_SECONDS + d_unpack(data, offset)[0])


def timestamptz_recv_float(data, offset, length):
    return timestamp_recv_float(data, offset, length).replace(
        tzinfo=Timezone.utc)


# data is 64-bit integer representing microseconds since 2000-01-01
def timestamp_recv_integer(data, offset, length):
    micros = q_unpack(data, offset)[0]
    try:
        return EPOCH + Timedelta(microseconds=micros)
    except OverflowError:
        if micros == INFINITY_MICROSECONDS:
            return 'infinity'
        elif micros == MINUS_INFINITY_MICROSECONDS:
            return '-infinity'
        else:
            return micros


# data is 64-bit integer representing microseconds since 2000-01-01
def timestamp_send_integer(v):
    return q_pack(
        int((timegm(v.timetuple()) - EPOCH_SECONDS) * 1e6) + v.microsecond)


# data is double-precision float representing seconds since 2000-01-01
def timestamp_send_float(v):
    return d_pack(timegm(v.timetuple()) + v.microsecond / 1e6 - EPOCH_SECONDS)


def timestamptz_send_integer(v):
    # timestamps should be sent as UTC.  If they have zone info,
    # convert them.
    return timestamp_send_integer(
        v.astimezone(Timezone.utc).replace(tzinfo=None))


def timestamptz_send_float(v):
    # timestamps should be sent as UTC.  If they have zone info,
    # convert them.
    return timestamp_send_float(
        v.astimezone(Timezone.utc).replace(tzinfo=None))


# return a timezone-aware datetime instance if we're reading from a
# "timestamp with timezone" type.  The timezone returned will always be
# UTC, but providing that additional information can permit conversion
# to local.
def timestamptz_recv_integer(data, offset, length):
    micros = q_unpack(data, offset)[0]
    try:
        return EPOCH_TZ + Timedelta(microseconds=micros)
    except OverflowError:
        if micros == INFINITY_MICROSECONDS:
            return 'infinity'
        elif micros == MINUS_INFINITY_MICROSECONDS:
            return '-infinity'
        else:
            return micros


def interval_send_integer(v):
    microseconds = v.microseconds
    try:
        microseconds += int(v.seconds * 1e6)
    except AttributeError:
        pass

    try:
        months = v.months
    except AttributeError:
        months = 0

    return qii_pack(microseconds, v.days, months)


def interval_send_float(v):
    seconds = v.microseconds / 1000.0 / 1000.0
    try:
        seconds += v.seconds
    except AttributeError:
        pass

    try:
        months = v.months
    except AttributeError:
        months = 0

    return dii_pack(seconds, v.days, months)


glbls = {'Decimal': Decimal}
trans_tab = dict(zip(map(ord, '{}'), '[]'))


def array_in(data, idx, length):
    arr = []
    prev_c = None
    for c in data[idx:idx + length].decode(
            _client_encoding).translate(
        trans_tab).replace('NULL', 'None'):
        if c not in ('[', ']', ',', 'N') and prev_c in ('[', ','):
            arr.extend("Decimal('")
        elif c in (']', ',') and prev_c not in ('[', ']', ',', 'e'):
            arr.extend("')")

        arr.append(c)
        prev_c = c
    return eval(''.join(arr), glbls)


def numeric_in(data, offset, length):
    return Decimal(
        data[offset: offset + length].decode(_client_encoding))


def uuid_recv(data, offset, length):
    return UUID(bytes=data[offset:offset+length])


def interval_recv_float(data, offset, length):
    seconds, days, months = dii_unpack(data, offset)
    if months == 0:
        secs, microseconds = divmod(seconds, 1e6)
        return Timedelta(days, secs, microseconds)
    else:
        return Interval(int(seconds * 1000 * 1000), days, months)


def interval_recv_integer(data, offset, length):
    microseconds, days, months = qii_unpack(data, offset)
    if months == 0:
        seconds, micros = divmod(microseconds, 1e6)
        return Timedelta(days, seconds, micros)
    else:
        return Interval(microseconds, days, months)


def time_in(data, offset, length):
    hour = int(data[offset:offset + 2])
    minute = int(data[offset + 3:offset + 5])
    sec = Decimal(
        data[offset + 6:offset + length].decode(_client_encoding))
    return time(
        hour, minute, int(sec), int((sec - int(sec)) * 1000000))


def date_in(data, offset, length):
    d = data[offset:offset + length].decode(_client_encoding)
    try:
        return date(int(d[:4]), int(d[5:7]), int(d[8:10]))
    except ValueError:
        return d


def array_recv(data, idx, length):
    final_idx = idx + length
    dim, hasnull, typeoid = iii_unpack(data, idx)
    idx += 12

    # get type conversion method for typeoid
    conversion = pg_types[typeoid][1]

    # Read dimension info
    dim_lengths = []
    for i in range(dim):
        dim_lengths.append(ii_unpack(data, idx)[0])
        idx += 8

    # Read all array values
    values = []
    while idx < final_idx:
        element_len, = i_unpack(data, idx)
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


def inet_in(data, offset, length):
    inet_str = data[offset: offset + length].decode(
        _client_encoding)
    if '/' in inet_str:
        return ip_network(inet_str, False)
    else:
        return ip_address(inet_str)


pg_types = defaultdict(
            lambda: (FC_TEXT, text_recv), {
                16: (FC_BINARY, bool_recv),  # boolean
                17: (FC_BINARY, bytea_recv),  # bytea
                19: (FC_BINARY, text_recv),  # name type
                20: (FC_BINARY, int8_recv),  # int8
                21: (FC_BINARY, int2_recv),  # int2
                22: (FC_TEXT, vector_in),  # int2vector
                23: (FC_BINARY, int4_recv),  # int4
                25: (FC_BINARY, text_recv),  # TEXT type
                26: (FC_TEXT, int_in),  # oid
                28: (FC_TEXT, int_in),  # xid
                114: (FC_TEXT, json_in),  # json
                700: (FC_BINARY, float4_recv),  # float4
                701: (FC_BINARY, float8_recv),  # float8
                705: (FC_BINARY, text_recv),  # unknown
                829: (FC_TEXT, text_recv),  # MACADDR type
                869: (FC_TEXT, inet_in),  # inet
                1000: (FC_BINARY, array_recv),  # BOOL[]
                1003: (FC_BINARY, array_recv),  # NAME[]
                1005: (FC_BINARY, array_recv),  # INT2[]
                1007: (FC_BINARY, array_recv),  # INT4[]
                1009: (FC_BINARY, array_recv),  # TEXT[]
                1014: (FC_BINARY, array_recv),  # CHAR[]
                1015: (FC_BINARY, array_recv),  # VARCHAR[]
                1016: (FC_BINARY, array_recv),  # INT8[]
                1021: (FC_BINARY, array_recv),  # FLOAT4[]
                1022: (FC_BINARY, array_recv),  # FLOAT8[]
                1042: (FC_BINARY, text_recv),  # CHAR type
                1043: (FC_BINARY, text_recv),  # VARCHAR type
                1082: (FC_TEXT, date_in),  # date
                1083: (FC_TEXT, time_in),
                1114: (FC_BINARY, timestamp_recv_float),  # timestamp w/ tz
                1184: (FC_BINARY, timestamptz_recv_float),
                1186: (FC_BINARY, interval_recv_integer),
                1231: (FC_TEXT, array_in),  # NUMERIC[]
                1263: (FC_BINARY, array_recv),  # cstring[]
                1700: (FC_TEXT, numeric_in),  # NUMERIC
                2275: (FC_BINARY, text_recv),  # cstring
                2950: (FC_BINARY, uuid_recv),  # uuid
                3802: (FC_TEXT, json_in),  # jsonb
            }
        )


def text_out(v):
    return v.encode(_client_encoding)


def enum_out(v):
    return str(v.value).encode(_client_encoding)


def time_out(v):
    return v.isoformat().encode(_client_encoding)


def date_out(v):
    return v.isoformat().encode(_client_encoding)


def unknown_out(v):
    return str(v).encode(_client_encoding)


def numeric_out(d):
    return str(d).encode(_client_encoding)


def inet_out(v):
    return str(v).encode(_client_encoding)


py_types = {
            type(None): (-1, FC_BINARY, null_send),  # null
            bool: (16, FC_BINARY, bool_send),
            bytearray: (17, FC_BINARY, bytea_send),  # bytea
            20: (20, FC_BINARY, q_pack),  # int8
            21: (21, FC_BINARY, h_pack),  # int2
            23: (23, FC_BINARY, i_pack),  # int4
            PGText: (25, FC_TEXT, text_out),  # text
            float: (701, FC_BINARY, d_pack),  # float8
            PGEnum: (705, FC_TEXT, enum_out),
            date: (1082, FC_TEXT, date_out),  # date
            time: (1083, FC_TEXT, time_out),  # time
            1114: (1114, FC_BINARY, timestamp_send_integer),  # timestamp
            # timestamp w/ tz
            PGVarchar: (1043, FC_TEXT, text_out),  # varchar
            1184: (1184, FC_BINARY, timestamptz_send_integer),
            PGJson: (114, FC_TEXT, text_out),
            PGJsonb: (3802, FC_TEXT, text_out),
            Timedelta: (1186, FC_BINARY, interval_send_integer),
            Interval: (1186, FC_BINARY, interval_send_integer),
            Decimal: (1700, FC_TEXT, numeric_out),  # Decimal
            PGTsvector: (3614, FC_TEXT, text_out),
            UUID: (2950, FC_BINARY, uuid_send),  # uuid
            bytes: (17, FC_BINARY, bytea_send),  # bytea
            str: (705, FC_TEXT, text_out),  # unknown
            enum.Enum: (705, FC_TEXT, enum_out),
            IPv4Address: (869, FC_TEXT, inet_out),  # inet
            IPv6Address: (869, FC_TEXT, inet_out),  # inet
            IPv4Network: (869, FC_TEXT, inet_out),  # inet
            IPv6Network: (869, FC_TEXT, inet_out)  # inet
        }
