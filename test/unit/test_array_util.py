import typing

import pytest  # type: ignore

from redshift_connector.utils import array_util

walk_array_data: typing.List = [
    (
        [10, 9, 8, 7, 6],
        [
            ([10, 9, 8, 7, 6], 0, 10),
            ([10, 9, 8, 7, 6], 1, 9),
            ([10, 9, 8, 7, 6], 2, 8),
            ([10, 9, 8, 7, 6], 3, 7),
            ([10, 9, 8, 7, 6], 4, 6),
        ],
    ),
    (
        [1, 2, [3, 4, 5], 6],
        [
            ([1, 2, [3, 4, 5], 6], 0, 1),
            ([1, 2, [3, 4, 5], 6], 1, 2),
            ([3, 4, 5], 0, 3),
            ([3, 4, 5], 1, 4),
            ([3, 4, 5], 2, 5),
            ([1, 2, [3, 4, 5], 6], 3, 6),
        ],
    ),
]


@pytest.mark.parametrize("_input", walk_array_data)
def test_walk_array(_input):
    in_val, exp_vals = _input
    x = array_util.walk_array(in_val)
    idx = 0
    for a, b, c in x:
        assert a == exp_vals[idx][0]
        assert b == exp_vals[idx][1]
        assert c == exp_vals[idx][2]
        idx += 1


array_flatten_data: typing.List = [
    ([1, 2, 3, 4], [1, 2, 3, 4]),
    ([1, [2], 3, 4], [1, 2, 3, 4]),
    ([1, [2, [3]], 4, [5, [6]]], [1, 2, 3, 4, 5, 6]),
    ([[1]], [1]),
]


@pytest.mark.parametrize("_input", array_flatten_data)
def test_array_flatten(_input):
    in_val, exp_val = _input
    assert 1 == 1
    assert list(array_util.array_flatten(in_val)) == exp_val


array_find_first_element_data: typing.List = [
    ([1], 1),
    ([None, None, [None, None, 1], 2], 1),
    ([[[1]]], 1),
    ([None, None, [None]], None),
]


@pytest.mark.parametrize("_input", array_find_first_element_data)
def test_array_find_first_element(_input):
    in_val, exp_val = _input
    assert array_util.array_find_first_element(in_val) == exp_val


array_has_null_data: typing.List = [
    ([None], True),
    ([1, 2, 3, 4, [[[None]]]], True),
    ([1, 2, 3, 4], False),
]


@pytest.mark.parametrize("_input", array_has_null_data)
def test_array_has_null(_input):
    in_val, exp_val = _input
    assert array_util.array_has_null(in_val) is exp_val
