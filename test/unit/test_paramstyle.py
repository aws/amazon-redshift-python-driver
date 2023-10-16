import typing

import pytest

from redshift_connector.config import DbApiParamstyle
from redshift_connector.core import convert_paramstyle as convert

# Tests of the convert_paramstyle function.


@pytest.mark.parametrize(
    "in_statement,out_statement,args",
    [
        (
            'SELECT ?, ?, "field_?" FROM t ' "WHERE a='say ''what?''' AND b=? AND c=E'?\\'test\\'?'",
            'SELECT $1, $2, "field_?" FROM t WHERE ' "a='say ''what?''' AND b=$3 AND c=E'?\\'test\\'?'",
            (1, 2, 3),
        ),
        (
            "SELECT ?, ?, * FROM t WHERE a=? AND b='are you ''sure?'",
            "SELECT $1, $2, * FROM t WHERE a=$3 AND b='are you ''sure?'",
            (1, 2, 3),
        ),
    ],
)
def test_qmark(in_statement, out_statement, args) -> None:
    new_query, make_args = convert(DbApiParamstyle.QMARK.value, in_statement)
    assert new_query == out_statement
    assert make_args(args) == args


def test_numeric() -> None:
    new_query, make_args = convert(
        DbApiParamstyle.NUMERIC.value, "SELECT sum(x)::decimal(5, 2) :2, :1, * FROM t WHERE a=:3"
    )
    expected: str = "SELECT sum(x)::decimal(5, 2) $2, $1, * FROM t WHERE a=$3"
    assert new_query == expected
    assert make_args((1, 2, 3)) == (1, 2, 3)


def test_numeric_default_parameter() -> None:
    new_query, make_args = convert(DbApiParamstyle.NUMERIC.value, "make_interval(days := 10)")

    assert new_query == "make_interval(days := 10)"
    assert make_args((1, 2, 3)) == (1, 2, 3)


def test_named() -> None:
    new_query, make_args = convert(
        DbApiParamstyle.NAMED.value, "SELECT sum(x)::decimal(5, 2) :f_2, :f1 FROM t WHERE a=:f_2"
    )
    expected: str = "SELECT sum(x)::decimal(5, 2) $1, $2 FROM t WHERE a=$1"
    assert new_query == expected
    assert make_args({"f_2": 1, "f1": 2}) == (1, 2)


def test_format() -> None:
    new_query, make_args = convert(
        DbApiParamstyle.FORMAT.value,
        "SELECT %s, %s, \"f1_%%\", E'txt_%%' FROM t WHERE a=%s AND b='75%%' AND c = '%' -- Comment with %",
    )
    expected: str = (
        "SELECT $1, $2, \"f1_%%\", E'txt_%%' FROM t WHERE a=$3 AND " "b='75%%' AND c = '%' -- Comment with %"
    )
    assert new_query == expected
    assert make_args((1, 2, 3)) == (1, 2, 3)


def test_format_multiline() -> None:
    new_query, make_args = convert(DbApiParamstyle.FORMAT.value, "SELECT -- Comment\n%s FROM t")
    assert new_query == "SELECT -- Comment\n$1 FROM t"


@pytest.mark.parametrize("paramstyle", DbApiParamstyle.list())
@pytest.mark.parametrize(
    "statement",
    (
        """
        EXPLAIN
        /* blabla
           something 100% with percent
        */
        SELECT {}
    """,
        """
        EXPLAIN
        /* blabla
           %% %s :blah %sbooze $1 %%s
        */
        SELECT {}
    """,
        """/* multiple line  comment here */""",
        """
    /* this is my multi-line sql comment  */
    select
        pk_id,
        {},
        -- shared_id, disabled until 12/12/2020
        order_date
    from my_table
    """,
        """/**/select {}""",
        """select {}
    /*\n
    some comments about the logic
    */ -- redo later""",
        r"""COMMENT ON TABLE test_schema.comment_test """ r"""IS 'the test % '' " \ table comment'""",
    ),
)
def test_multiline_single_parameter(paramstyle, statement) -> None:
    in_statement = statement
    format_char = None
    expected = statement.format("$1")

    if paramstyle == DbApiParamstyle.FORMAT.value:
        format_char = "%s"
    elif paramstyle == DbApiParamstyle.PYFORMAT.value:
        format_char = "%(f1)s"
    elif paramstyle == DbApiParamstyle.NAMED.value:
        format_char = ":beer"
    elif paramstyle == DbApiParamstyle.NUMERIC.value:
        format_char = ":1"
    elif paramstyle == DbApiParamstyle.QMARK.value:
        format_char = "?"
    in_statement = in_statement.format(format_char)

    new_query, make_args = convert(paramstyle, in_statement)
    assert new_query == expected


def test_py_format() -> None:
    new_query, make_args = convert(
        DbApiParamstyle.PYFORMAT.value,
        "SELECT %(f2)s, %(f1)s, \"f1_%%\", E'txt_%%' " "FROM t WHERE a=%(f2)s AND b='75%%'",
    )
    expected: str = "SELECT $1, $2, \"f1_%%\", E'txt_%%' FROM t WHERE a=$1 AND " "b='75%%'"
    assert new_query == expected
    assert make_args({"f2": 1, "f1": 2, "f3": 3}) == (1, 2)

    # pyformat should support %s and an array, too:
    new_query, make_args = convert(
        DbApiParamstyle.PYFORMAT.value, "SELECT %s, %s, \"f1_%%\", E'txt_%%' " "FROM t WHERE a=%s AND b='75%%'"
    )
    expected = "SELECT $1, $2, \"f1_%%\", E'txt_%%' FROM t WHERE a=$3 AND " "b='75%%'"
    assert new_query, expected
    assert make_args((1, 2, 3)) == (1, 2, 3)
