import pytest  # type: ignore

import redshift_connector


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
