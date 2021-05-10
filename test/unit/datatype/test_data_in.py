import typing
from datetime import date, datetime, time, timezone
from decimal import Decimal
from enum import Enum, auto
from math import isclose

import pytest  # type: ignore

import redshift_connector
from redshift_connector.utils import type_utils


class Datatypes(Enum):
    text = type_utils.text_recv
    bool = type_utils.bool_recv
    int8 = type_utils.int8_recv
    int2 = type_utils.int2_recv
    int4 = type_utils.int4_recv
    float4 = type_utils.float4_recv
    float8 = type_utils.float8_recv
    timestamp = type_utils.timestamp_recv_integer
    numeric_binary = type_utils.numeric_in_binary
    numeric = type_utils.numeric_in
    timetz_binary = type_utils.timetz_recv_binary
    time_binary = type_utils.time_recv_binary
    time = type_utils.time_in
    timetz = type_utils.timetz_in
    date_binary = type_utils.date_recv_binary
    date = type_utils.date_in
    aclitem_array = type_utils.array_recv_text
    aclitem_array_binary = type_utils.array_recv_binary
    char_array = type_utils.array_recv_text
    char_array_binary = type_utils.array_recv_binary
    oid_array = type_utils.int_array_recv
    oid_array_binary = type_utils.array_recv_binary
    text_array = type_utils.array_recv_text
    text_array_binary = type_utils.array_recv_binary
    geometry = type_utils.text_recv


test_data: typing.Dict[Datatypes, typing.List[typing.Tuple]] = {
    Datatypes.text: [
        (
            b"\x00\x05\x00\x00\x00\x10123456789abcdef \x00\x00\x00\x10123456789abcdef \x00\x00\x00\x10123456789abcdef \x00\x00\x00\x10123456789abcdef \x00\x00\x00\x10123456789abcdef ",
            6,
            16,
            "123456789abcdef ",
        ),
    ],
    Datatypes.bool: [
        (
            b"\x00\x05\x00\x00\x00\x01\x01\x00\x00\x00\x01\x00\x00\x00\x00\x01\x01\x00\x00\x00\x01\x00\x00\x00\x00\x01\x01",
            6,
            1,
            True,
        ),
        (
            b"\x00\x05\x00\x00\x00\x01\x01\x00\x00\x00\x01\x00\x00\x00\x00\x01\x01\x00\x00\x00\x01\x00\x00\x00\x00\x01\x01",
            11,
            1,
            False,
        ),
    ],
    Datatypes.int8: [
        (
            b'\x00\x05\x00\x00\x00\x08\x02\xfa\xf0\x80\x8f\xee"\xcf\x00\x00\x00\x08\x02\xfa\xf0\x80\x8f\xee"\xcf\x00\x00\x00\x08\x02\xfa\xf0\x80\x8f\xee"\xcf\x00\x00\x00\x08\x02\xfa\xf0\x80\x8f\xee"\xcf\x00\x00\x00\x08\x02\xfa\xf0\x80\x8f\xee"\xcf',
            6,
            8,
            214748367214748367,
        ),
    ],
    Datatypes.int2: [
        (
            b"\x00\x05\x00\x00\x00\x02\x7f\xff\x00\x00\x00\x02\x7f\xff\x00\x00\x00\x02\x7f\xff\x00\x00\x00\x02\x7f\xff\x00\x00\x00\x02\x7f\xff",
            6,
            2,
            32767,
        )
    ],
    Datatypes.int4: [
        (
            b"\x00\x05\x00\x00\x00\x04\x0c\xcc\xcc\xcf\x00\x00\x00\x04\x0c\xcc\xcc\xcf\x00\x00\x00\x04\x0c\xcc\xcc\xcf\x00\x00\x00\x04\x0c\xcc\xcc\xcf\x00\x00\x00\x04\x0c\xcc\xcc\xcf",
            6,
            4,
            214748367,
        )
    ],
    Datatypes.float4: [
        (
            b"\x00\x05\x00\x00\x00\x04B\xf6>\xfa\x00\x00\x00\x04B\xf6>\xfa\x00\x00\x00\x04B\xf6>\xfa\x00\x00\x00\x04B\xf6>\xfa\x00\x00\x00\x04B\xf6>\xfa",
            6,
            4,
            123.12300109863281,
        )
    ],
    Datatypes.float8: [
        (
            b"\x00\x05\x00\x00\x00\x08A2\xd6\x87\x1f\x9a\xdb\xb9\x00\x00\x00\x08A2\xd6\x87\x1f\x9a\xdb\xb9\x00\x00\x00\x08A2\xd6\x87\x1f\x9a\xdb\xb9\x00\x00\x00\x08A2\xd6\x87\x1f\x9a\xdb\xb9\x00\x00\x00\x08A2\xd6\x87\x1f\x9a\xdb\xb9",
            6,
            8,
            1234567.1234567,
        )
    ],
    Datatypes.timestamp: [
        (
            b"\x00\x05\x00\x00\x00\x08\x00\x00\xf1\x96\xb5\xe3\xe5\xc0\x00\x00\x00\x08\x00\x00\xf1\x96\xb5\xe3\xe5\xc0\x00\x00\x00\x08\x00\x00\xf1\x96\xb5\xe3\xe5\xc0\x00\x00\x00\x08\x00\x00\xf1\x96\xb5\xe3\xe5\xc0\x00\x00\x00\x08\x00\x00\xf1\x96\xb5\xe3\xe5\xc0",
            6,
            8,
            datetime(year=2008, month=6, day=1, hour=9, minute=59, second=59),
        )
    ],
    Datatypes.numeric_binary: [
        # 8
        (
            b"\x00\x05\x00\x00\x00\x08\x01\xb6\x9bK\xac\xd0_\x15\x00\x00\x00\x08\x01\xb6\x9bK\xac\xd0_\x15\x00\x00\x00\x08\x01\xb6\x9bK\xac\xd0_\x15\x00\x00\x00\x08\x01\xb6\x9bK\xac\xd0_\x15\x00\x00\x00\x08\x01\xb6\x9bK\xac\xd0_\x15",
            6,
            8,
            0,
            Decimal(123456789123456789),
        ),
        (
            b"\x00\x05\x00\x00\x00\x08\x01\xb6\x9bK\xac\xd0_\x15\x00\x00\x00\x08\x01\xb6\x9bK\xac\xd0_\x15\x00\x00\x00\x08\x01\xb6\x9bK\xac\xd0_\x15\x00\x00\x00\x08\x01\xb6\x9bK\xac\xd0_\x15\x00\x00\x00\x08\x01\xb6\x9bK\xac\xd0_\x15",
            6,
            8,
            9,
            Decimal(123456789.12345679104328155517578125),
        ),
        (
            b"\x00\x02\x00\x00\x00\x0c-12345.67891\x00\x00\x00\x08\xff\xff\xfe\xe0\x8e\x04\xf7\xc8",
            22,
            8,
            8,
            Decimal(-12345.67891),
        ),
        (
            b"\x00\x02\x00\x00\x00\n0.00012345\x00\x00\x00\x08\x00\x00\x00\x00\x00\x0009",
            20,
            8,
            8,
            Decimal(0.00012345),
        ),
        (
            b"\x00\x02\x00\x00\x00\x0b12345.67891\x00\x00\x00\x08\x00\x00\x01\x1fq\xfb\x088",
            21,
            8,
            8,
            Decimal(12345.67891),
        ),
        # 16
        (
            b"\x00\x05\x00\x00\x00\x10\tI\xb0\xf7\x13\xe9\x18_~\x8f\x1a\x99\xa9\x9b\xb6\xdb\x00\x00\x00\x10\tI\xb0\xf7\x13\xe9\x18_~\x8f\x1a\x99\xa9\x9b\xb6\xdb\x00\x00\x00\x10\tI\xb0\xf7\x13\xe9\x18_~\x8f\x1a\x99\xa9\x9b\xb6\xdb\x00\x00\x00\x10\tI\xb0\xf7\x13\xe9\x18_~\x8f\x1a\x99\xa9\x9b\xb6\xdb\x00\x00\x00\x10\tI\xb0\xf7\x13\xe9\x18_~\x8f\x1a\x99\xa9\x9b\xb6\xdb",
            6,
            16,
            0,
            Decimal(12345678912345678991234567891234567899),
        ),
        (
            b"\x00\x02\x00\x00\x00\x0b12345.67891\x00\x00\x00\x10\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00I\x96\x02\xd3",
            21,
            16,
            5,
            Decimal(12345.67891),
        ),
        (
            b"\x00\x02\x00\x00\x00\x06-32768\x00\x00\x00\x10\xff\xff\xff\xff\xff\xff\xff\xff\xff\xfe\xd5\xfa\x0e\x00\x00\x00",
            16,
            16,
            10,
            Decimal(-32768),
        ),
        (
            b"\x00\x02\x00\x00\x00\x0c-12345.67891\x00\x00\x00\x10\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x8f\xb7y\xf0\xca ",
            22,
            16,
            10,
            Decimal(-12345.67891),
        ),
        (
            b"\x00\x02\x00\x00\x00\x04-0.11\x00\x00\x00\x10\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xbeoU\x00",
            15,
            16,
            10,
            Decimal(-0.11),
        ),
        (
            b"\x00\x02\x00\x00\x00\x010\x00\x00\x00\x10\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
            11,
            16,
            10,
            Decimal(0),
        ),
        (
            b"\x00\x02\x00\x00\x00\x0c0.0000012345\x00\x00\x00\x10\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x0009",
            22,
            16,
            10,
            Decimal(0.0000012345),
        ),
    ],
    Datatypes.numeric: [
        # 8
        (
            b"\x00\x05\x00\x00\x00\x13123456789.123456789\x00\x00\x00\x13123456789.123456789\x00\x00\x00\x13123456789.123456789\x00\x00\x00\x13123456789.123456789\x00\x00\x00\x13123456789.123456789",
            6,
            19,
            Decimal(123456789.12345679104328155517578125),
        ),
        # 16
        (
            b"\x00\x05\x00\x00\x00'1234567891234567899.1234567891234567899\x00\x00\x00'1234567891234567899.1234567891234567899\x00\x00\x00'1234567891234567899.1234567891234567899\x00\x00\x00'1234567891234567899.1234567891234567899\x00\x00\x00'1234567891234567899.1234567891234567899",
            6,
            39,
            Decimal(1.234567891234568e18),
        ),
    ],
    Datatypes.time: [
        (b"02:04:06", 0, 8, time(hour=2, minute=4, second=6)),
        (b"22:24:56", 0, 8, time(hour=22, minute=24, second=56)),
    ],
    Datatypes.timetz: [
        (b"12:34:56+00", 0, 8, time(hour=12, minute=34, second=56, tzinfo=timezone.utc)),
        (b"12:34:56.789", 0, 12, time(hour=12, minute=34, second=56, microsecond=789, tzinfo=timezone.utc)),
        (b"08:12:16.202224+0", 0, 17, time(hour=8, minute=12, second=16, microsecond=202224, tzinfo=timezone.utc)),
    ],
    Datatypes.date: [
        (
            b"\x00\x05\x00\x00\x00\n2020-03-10\x00\x00\x00\n2020-03-10\x00\x00\x00\n2020-03-10\x00\x00\x00\n2020-03-10\x00\x00\x00\n2020-03-10",
            6,
            10,
            date(year=2020, day=10, month=3),
        ),
        (b"0010-01-01 BC", 0, 13, date.min),
        (b"999999-01-01", 0, 11, date.max),
    ],
    Datatypes.aclitem_array: [
        (
            b"\x00\x02\x00\x00\x00\x0epg_default_acl\x00\x00\x00\x1e{rdsdb=arwdRxt/rdsdb,=r/rdsdb}",
            24,
            30,
            ["rdsdb=arwdRxt/rdsdb", "=r/rdsdb"],
        ),
        (b"\x00\x01\x00\x00\x00\x1e{rdsdb=arwdRxt/rdsdb,=r/rdsdb}", 6, 30, ["rdsdb=arwdRxt/rdsdb", "=r/rdsdb"]),
    ],
    Datatypes.aclitem_array_binary: [
        (
            b"\x00\x02\x00\x00\x00\x0epg_default_acl\x00\x00\x007\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x04\t\x00\x00\x00\x02\x00\x00\x00\x01\x00\x00\x00\x13rdsdb=arwdRxt/rdsdb\x00\x00\x00\x08=r/rdsdb",
            24,
            55,
            ["rdsdb=arwdRxt/rdsdb", "=r/rdsdb"],
        ),
        (
            b"\x00\x01\x00\x00\x007\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x04\t\x00\x00\x00\x02\x00\x00\x00\x01\x00\x00\x00\x13rdsdb=arwdRxt/rdsdb\x00\x00\x00\x08=r/rdsdb",
            6,
            55,
            ["rdsdb=arwdRxt/rdsdb", "=r/rdsdb"],
        ),
    ],
    Datatypes.char_array: [
        (
            b"\x00\x01\x00\x00\x00\x03{o}",
            6,
            3,
            ["o"],
        ),
        (b"\x00\x01\x00\x00\x00\x07{i,b,o}", 6, 7, ["i", "b", "o"]),
    ],
    Datatypes.char_array_binary: [
        (
            b"\x00\x01\x00\x00\x00\x19\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x12\x00\x00\x00\x01\x00\x00\x00\x01\x00\x00\x00\x01o",
            6,
            25,
            ["o"],
        ),
        (
            b"\x00\x01\x00\x00\x00#\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x12\x00\x00\x00\x03\x00\x00\x00\x01\x00\x00\x00\x01i\x00\x00\x00\x01b\x00\x00\x00\x01o",
            6,
            35,
            ["i", "b", "o"],
        ),
    ],
    Datatypes.oid_array: [
        (
            b"\x00\x01\x00\x00\x00\x06{1700}",
            6,
            6,
            [1700],
        ),
        (
            b"\x00\x01\x00\x00\x00\x0e{23,1043,1043}",
            6,
            14,
            [23, 1043, 1043],
        ),
    ],
    Datatypes.oid_array_binary: [
        (
            b"\x00\x01\x00\x00\x00\x1c\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x1a\x00\x00\x00\x01\x00\x00\x00\x01\x00\x00\x00\x04\x00\x00\x06\xa4",
            6,
            28,
            [1700],
        ),
        (
            b"\x00\x01\x00\x00\x00,\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x1a\x00\x00\x00\x03\x00\x00\x00\x01\x00\x00\x00\x04\x00\x00\x00\x17\x00\x00\x00\x04\x00\x00\x04\x13\x00\x00\x00\x04\x00\x00\x04\x13",
            6,
            44,
            [23, 1043, 1043],
        ),
    ],
    Datatypes.text_array: [(b"\x00\x01\x00\x00\x00\x0e{typid,typmod}", 6, 14, ["typid", "typmod"])],
    Datatypes.text_array_binary: [
        (
            b"\x00\x01\x00\x00\x00'\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x19\x00\x00\x00\x02\x00\x00\x00\x01\x00\x00\x00\x05typid\x00\x00\x00\x06typmod",
            6,
            39,
            ["typid", "typmod"],
        )
    ],
    Datatypes.geometry: [
        (
            b"\x00\x01\x00\x00\x01R01020000000A000000000000000000F03F000000000000004000000000000008400000000000001040000000000000144000000000000018400000000000001C40000000000000204000000000000022400000000000002440000000000000264000000000000028400000000000002A400000000000002C400000000000002E4000000000000030400000000000003140000000000000324000000000000033400000000000003440",
            6,
            338,
            "01020000000A000000000000000000F03F000000000000004000000000000008400000000000001040000000000000144000000000000018400000000000001C40000000000000204000000000000022400000000000002440000000000000264000000000000028400000000000002A400000000000002C400000000000002E4000000000000030400000000000003140000000000000324000000000000033400000000000003440",
        ),
        (
            b"\x00\x01\x00\x00\x00\xc20103000020E61000000100000005000000000000000000000000000000000000000000000000000000000000000000F03F000000000000F03F000000000000F03F000000000000F03F000000000000000000000000000000000000000000000000",
            6,
            194,
            "0103000020E61000000100000005000000000000000000000000000000000000000000000000000000000000000000F03F000000000000F03F000000000000F03F000000000000F03F000000000000000000000000000000000000000000000000",
        ),
    ],
}


def get_test_cases() -> typing.Generator:
    for key, value in test_data.items():
        for test_case in value:
            yield key, test_case


@pytest.mark.parametrize("_input", get_test_cases(), ids=[k.__name__ for k, v in get_test_cases()])
def test_datatype_recv(_input):
    test_func, test_args = _input
    if len(test_args) == 5:  # numeric_in_binary
        _data, _offset, _length, scale, exp_result = test_args
        assert isclose(test_func(_data, _offset, _length, scale), exp_result, rel_tol=1e-6)
    else:
        _data, _offset, _length, exp_result = test_args
        if test_func == type_utils.numeric_in:
            assert isclose(test_func(_data, _offset, _length), exp_result, rel_tol=1e-6)
        else:
            assert test_func(_data, _offset, _length) == exp_result
