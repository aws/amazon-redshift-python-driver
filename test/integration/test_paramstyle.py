import typing

import pytest  # type: ignore


@pytest.mark.parametrize(
    "parameters",
    [
        ({"c1": "abc", "c2": "defg", "c3": "hijkl"}, ["abc", "defg", "hijkl"]),
        ({"c1": "a", "c2": "b", "c3": "c"}, ["a", "b", "c"]),
    ],
)
def test_pyformat(cursor, parameters) -> None:
    cursor.paramstyle = "pyformat"
    data, exp_result = parameters
    cursor.execute("create temporary table test_pyformat(c1 varchar, c2 varchar, c3 varchar)")
    cursor.execute("insert into test_pyformat(c1, c2, c3) values(%(c1)s, %(c2)s, %(c3)s)", data)
    cursor.execute("select * from test_pyformat")
    res: typing.Tuple[typing.List[str], ...] = cursor.fetchall()
    assert len(res) == 1
    assert len(res[0]) == len(data)
    assert res[0] == exp_result


@pytest.mark.parametrize(
    "parameters",
    [
        (
            ({"c1": "abc", "c2": "defg", "c3": "hijkl"}, {"c1": "a", "c2": "b", "c3": "c"}),
            [["a", "b", "c"], ["abc", "defg", "hijkl"]],
        ),
    ],
)
def test_pyformat_multiple_insert(cursor, parameters) -> None:
    cursor.paramstyle = "pyformat"
    data, exp_result = parameters
    cursor.execute("create temporary table test_pyformat(c1 varchar, c2 varchar, c3 varchar)")
    cursor.executemany("insert into test_pyformat(c1, c2, c3) values(%(c1)s, %(c2)s, %(c3)s)", data)
    cursor.execute("select * from test_pyformat order by c1")
    res: typing.Tuple[typing.List[str], ...] = cursor.fetchall()
    assert len(res) == len(exp_result)
    for idx, row in enumerate(res):
        assert len(row) == len(exp_result[idx])
        assert row == exp_result[idx]


@pytest.mark.parametrize(
    "parameters", [(["abc", "defg", "hijkl"], ["abc", "defg", "hijkl"]), (["a", "b", "c"], ["a", "b", "c"])]
)
def test_qmark(cursor, parameters) -> None:
    cursor.paramstyle = "qmark"
    data, exp_result = parameters
    cursor.execute("create temporary table test_qmark(c1 varchar, c2 varchar, c3 varchar)")
    cursor.execute("insert into test_qmark(c1, c2, c3) values(?, ?, ?)", data)
    cursor.execute("select * from test_qmark")
    res: typing.Tuple[typing.List[str], ...] = cursor.fetchall()
    assert len(res) == 1
    assert len(res[0]) == len(data)
    assert res[0] == exp_result


@pytest.mark.parametrize(
    "parameters", [(["abc", "defg", "hijkl"], ["abc", "defg", "hijkl"]), (["a", "b", "c"], ["a", "b", "c"])]
)
def test_numeric(cursor, parameters) -> None:
    cursor.paramstyle = "numeric"
    data, exp_result = parameters
    cursor.execute("create temporary table test_numeric(c1 varchar, c2 varchar, c3 varchar)")
    cursor.execute("insert into test_numeric(c1, c2, c3) values(:1, :2, :3)", data)
    cursor.execute("select * from test_numeric")
    res: typing.Tuple[typing.List[str], ...] = cursor.fetchall()
    assert len(res) == 1
    assert len(res[0]) == len(data)
    assert res[0] == exp_result


@pytest.mark.parametrize(
    "parameters",
    [
        ({"parameter1": "abc", "parameter2": "defg", "parameter3": "hijkl"}, ["abc", "defg", "hijkl"]),
        ({"parameter1": "a", "parameter2": "b", "parameter3": "c"}, ["a", "b", "c"]),
    ],
)
def test_named(cursor, parameters) -> None:
    cursor.paramstyle = "named"
    data, exp_result = parameters
    cursor.execute("create temporary table test_named(c1 varchar, c2 varchar, c3 varchar)")
    cursor.execute("insert into test_named(c1, c2, c3) values(:parameter1, :parameter2, :parameter3)", data)
    cursor.execute("select * from test_named")
    res: typing.Tuple[typing.List[str], ...] = cursor.fetchall()
    assert len(res) == 1
    assert len(res[0]) == len(data)
    assert res[0] == exp_result


@pytest.mark.parametrize(
    "parameters", [(["abc", "defg", "hijkl"], ["abc", "defg", "hijkl"]), (["a", "b", "c"], ["a", "b", "c"])]
)
def test_format(cursor, parameters) -> None:
    cursor.paramstyle = "format"
    data, exp_result = parameters
    cursor.execute("create temporary table test_format(c1 varchar, c2 varchar, c3 varchar)")
    cursor.execute("insert into test_format(c1, c2, c3) values(%s, %s, %s)", data)
    cursor.execute("select * from test_format")
    res: typing.Tuple[typing.List[str], ...] = cursor.fetchall()
    assert len(res) == 1
    assert len(res[0]) == len(data)
    assert res[0] == exp_result


@pytest.mark.parametrize(
    "parameters",
    [
        ([["abc", "defg", "hijkl"], ["a", "b", "c"]], [["a", "b", "c"], ["abc", "defg", "hijkl"]]),
    ],
)
def test_format_multiple(cursor, parameters) -> None:
    cursor.paramstyle = "format"
    data, exp_result = parameters
    cursor.execute("create temporary table test_format(c1 varchar, c2 varchar, c3 varchar)")
    cursor.executemany("insert into test_format(c1, c2, c3) values(%s, %s, %s)", data)
    cursor.execute("select * from test_format order by c1")
    res: typing.Tuple[typing.List[str], ...] = cursor.fetchall()
    assert len(res) == len(exp_result)
    for idx, row in enumerate(res):
        assert len(row) == len(exp_result[idx])
        assert row == exp_result[idx]
