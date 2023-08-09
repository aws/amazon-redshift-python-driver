from io import StringIO
from unittest.mock import mock_open, patch

import pytest  # type: ignore

import redshift_connector
from redshift_connector import InterfaceError, DataError


@pytest.mark.parametrize("col_name", (("apples", "apples"), ("author‎ ", "author\u200e")))
def test_get_description(db_kwargs, col_name):
    given_col_name, exp_col_name = col_name
    with redshift_connector.connect(**db_kwargs) as conn:
        with conn.cursor() as cursor:
            cursor.execute("create temp table tmptbl({} int)".format(given_col_name))
            cursor.execute("select * from tmptbl")
            assert cursor.description is not None
            assert cursor.description[0][0] == exp_col_name


@pytest.mark.parametrize(
    "col_names",
    (
            ("(c1 int, c2 int, c3 int)", ("c1", "c2", "c3")),
            (
                    "(áppleṣ int, orañges int, passion⁘fruit int, papaya  int, bañanaș int)",
                    ("áppleṣ", "orañges", "passion⁘fruit", "papaya\u205f", "bañanaș"),
            ),
    ),
)
def test_get_description_multiple_column_names(db_kwargs, col_names):
    given_col_names, exp_col_names = col_names
    with redshift_connector.connect(**db_kwargs) as conn:
        with conn.cursor() as cursor:
            cursor.execute("create temp table tmptbl {}".format(given_col_names))
            cursor.execute("select * from tmptbl")
            assert cursor.description is not None

            for cidx, column in enumerate(cursor.description):
                assert column[0] == exp_col_names[cidx]


@patch("builtins.open", new_callable=mock_open)
def test_insert_data_invalid_column_raises(mocked_csv, db_kwargs):
    indexes, names, exp_execute_args = (
        [0],
        ["col1"],
        ("INSERT INTO githubissue161 (col1) VALUES (%s), (%s), (%s);", ["1", "2", "-1"]),
    )

    mocked_csv.side_effect = [StringIO("""\col1,col2,col3\n1,3,foo\n2,5,bar\n-1,7,baz""")]

    with redshift_connector.connect(**db_kwargs) as conn:
        with conn.cursor() as cursor:
            cursor.execute("create temporary table githubissue161 (id int)")

            with pytest.raises(
                    InterfaceError,
                    match="Invalid column name. No results were returned when performing column name validity check.",
            ):
                cursor.insert_data_bulk(
                    filename="mocked_csv",
                    table_name="githubissue161",
                    parameter_indices=indexes,
                    column_names=["IncorrectColumnName"],
                    delimiter=",",
                    batch_size=3,
                )


def test_insert_data_raises_too_many_params(db_kwargs):
    max_params = 32767
    prepared_stmt = "INSERT INTO githubissue165 (col1) VALUES " + "(%s), " * max_params + "(%s);"
    params = [1 for _ in range(max_params + 1)]

    with redshift_connector.connect(**db_kwargs) as conn:
        with conn.cursor() as cursor:
            cursor.execute("create temporary table githubissue165 (col1 int)")

            with pytest.raises(
                    DataError,
                    match="Prepared statement exceeds bind parameter limit 32767.",
            ):
                cursor.execute(prepared_stmt, params)


@patch("builtins.open", new_callable=mock_open)
def test_insert_data_bulk_raises_too_many_params(mocked_csv, db_kwargs):
    max_params = 32767
    indexes, names = (
        [0],
        ["col1"],
    )
    csv_str = "\col1\n" + "1\n" * max_params + "1"  # 32768 rows
    mocked_csv.side_effect = [StringIO(csv_str)]

    with redshift_connector.connect(**db_kwargs) as conn:
        with conn.cursor() as cursor:
            cursor.execute("create temporary table githubissue165 (col1 int)")
            with pytest.raises(DataError, match="Prepared statement exceeds bind parameter limit 32767."):
                cursor.insert_data_bulk(
                    filename="mocked_csv",
                    table_name="githubissue165",
                    parameter_indices=indexes,
                    column_names=["col1"],
                    delimiter=",",
                    batch_size=max_params + 1,
                )
