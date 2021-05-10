from datetime import date, datetime, time
from decimal import Decimal
from enum import Enum

import pytest  # type: ignore

from redshift_connector.config import (
    EPOCH,
    INFINITY_MICROSECONDS,
    MINUS_INFINITY_MICROSECONDS,
)
from redshift_connector.utils import type_utils


@pytest.mark.parametrize("_input", [(True, b"\x01"), (False, b"\x00")])
def test_bool_send(_input):
    in_val, exp_val = _input
    assert type_utils.bool_send(in_val) == exp_val


@pytest.mark.parametrize("_input", [None, 1])
def test_null_send(_input):
    assert type_utils.null_send(_input) == type_utils.NULL


class Apple(Enum):
    macintosh = 1
    granny_smith = 2
    ambrosia = 3


class Orange(Enum):
    navel = 1
    blood = 2
    cara_cara = 3


@pytest.mark.parametrize("_input", [(Apple.macintosh, b"1"), (Orange.cara_cara, b"3")])
def test_enum_out(_input):
    in_val, exp_val = _input
    assert type_utils.enum_out(in_val) == exp_val


@pytest.mark.parametrize(
    "_input", [(time(hour=0, minute=0, second=0), b"00:00:00"), (time(hour=12, minute=34, second=56), b"12:34:56")]
)
def test_time_out(_input):
    in_val, exp_val = _input
    assert type_utils.time_out(in_val) == exp_val


@pytest.mark.parametrize(
    "_input", [(date(month=1, day=1, year=1), b"0001-01-01"), (date(month=1, day=31, year=2020), b"2020-01-31")]
)
def test_date_out(_input):
    in_val, exp_val = _input
    assert type_utils.date_out(in_val) == exp_val


@pytest.mark.parametrize(
    "_input",
    [
        (Decimal(123.45678), b"123.4567799999999948568074614740908145904541015625"),
        (Decimal(123456789.012345), b"123456789.01234500110149383544921875"),
    ],
)
def test_numeric_out(_input):
    in_val, exp_val = _input
    assert type_utils.numeric_out(in_val) == exp_val


timestamp_send_integer_data = [
    (b"00000000", datetime.max),
    (b"12345678", datetime.max),
    (INFINITY_MICROSECONDS.to_bytes(length=8, byteorder="big"), datetime.max),
    (MINUS_INFINITY_MICROSECONDS.to_bytes(signed=True, length=8, byteorder="big"), datetime.min),
]


@pytest.mark.parametrize("_input", timestamp_send_integer_data)
def test_timestamp_recv_integer(_input):
    in_val, exp_val = _input
    print(type_utils.timestamp_recv_integer(in_val, 0, 0))
    print(EPOCH.timestamp() * 1000)
    assert type_utils.timestamp_recv_integer(in_val, 0, 0) == exp_val
