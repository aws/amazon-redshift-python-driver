import typing
import unittest

import pytest

from redshift_connector.interval import Interval


@pytest.mark.parametrize(
    "kwargs, exp_months, exp_days, exp_microseconds",
    (
        (
            {"months": 1},
            1,
            0,
            0,
        ),
        ({"days": 1}, 0, 1, 0),
        ({"microseconds": 1}, 0, 0, 1),
        ({"months": 1, "days": 2, "microseconds": 3}, 1, 2, 3),
    ),
)
def test_interval_constructor(kwargs, exp_months, exp_days, exp_microseconds):
    i = Interval(**kwargs)
    assert i.months == exp_months
    assert i.days == exp_days
    assert i.microseconds == exp_microseconds


def test_default_constructor():
    i: Interval = Interval()
    assert i.months == 0
    assert i.days == 0
    assert i.microseconds == 0


def interval_range_test(parameter, in_range, out_of_range):
    for v in out_of_range:
        try:
            Interval(**{parameter: v})
            pytest.fail("expected OverflowError")
        except OverflowError:
            pass
    for v in in_range:
        Interval(**{parameter: v})


def test_interval_days_range():
    out_of_range_days = (
        -2147483648,
        +2147483648,
    )
    in_range_days = (
        -2147483647,
        +2147483647,
    )
    interval_range_test("days", in_range_days, out_of_range_days)


def test_interval_months_range():
    out_of_range_months = (
        -2147483648,
        +2147483648,
    )
    in_range_months = (
        -2147483647,
        +2147483647,
    )
    interval_range_test("months", in_range_months, out_of_range_months)


def test_interval_microseconds_range():
    out_of_range_microseconds = (
        -9223372036854775808,
        +9223372036854775808,
    )
    in_range_microseconds = (
        -9223372036854775807,
        +9223372036854775807,
    )
    interval_range_test("microseconds", in_range_microseconds, out_of_range_microseconds)


@pytest.mark.parametrize(
    "kwargs, exp_total_seconds",
    (
        ({"months": 1}, 0),
        ({"days": 1}, 86400),
        ({"microseconds": 1}, 1e-6),
        ({"months": 1, "days": 2, "microseconds": 3}, 172800.000003),
    ),
)
def test_total_seconds(kwargs, exp_total_seconds):
    i: Interval = Interval(**kwargs)
    assert i.total_seconds() == exp_total_seconds


def test_set_months_raises_type_error():
    with pytest.raises(TypeError):
        Interval(months="foobar")


def test_set_days_raises_type_error():
    with pytest.raises(TypeError):
        Interval(days="foobar")


def test_set_microseconds_raises_type_error():
    with pytest.raises(TypeError):
        Interval(microseconds="foobar")


interval_equality_test_vals: typing.Tuple[
    typing.Tuple[typing.Optional[Interval], typing.Optional[Interval], bool], ...
] = (
    (Interval(months=1), Interval(months=1), True),
    (Interval(months=1), Interval(), False),
    (Interval(months=1), Interval(months=2), False),
    (Interval(days=1), Interval(days=1), True),
    (Interval(days=1), Interval(), False),
    (Interval(days=1), Interval(days=2), False),
    (Interval(microseconds=1), Interval(microseconds=1), True),
    (Interval(microseconds=1), Interval(), False),
    (Interval(microseconds=1), Interval(microseconds=2), False),
    (Interval(), Interval(), True),
)


@pytest.mark.parametrize("i1, i2, exp_eq", interval_equality_test_vals)
def test__eq__(i1, i2, exp_eq):
    actual_eq = i1.__eq__(i2)
    assert actual_eq == exp_eq


@pytest.mark.parametrize("i1, i2, exp_eq", interval_equality_test_vals)
def test__neq__(i1, i2, exp_eq):
    exp_neq = not exp_eq
    actual_neq = i1.__neq__(i2)
    assert actual_neq == exp_neq
