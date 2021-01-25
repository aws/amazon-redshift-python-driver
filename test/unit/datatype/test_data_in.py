import typing
from datetime import date, datetime, time, timezone
from decimal import Decimal
from enum import Enum, auto
from math import isclose

import pytest

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
    numeric = type_utils.numeric_in
    time = type_utils.time_in
    timetz = type_utils.timetz_in
    date = type_utils.date_in
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
    _data, _offset, _length, exp_result = test_args
    if test_func == type_utils.numeric_in:
        assert isclose(test_func(_data, _offset, _length), exp_result, rel_tol=1e-6)
    else:
        assert test_func(_data, _offset, _length) == exp_result
