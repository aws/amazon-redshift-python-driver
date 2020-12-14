from test.utils import numpy_only, pandas_only
from warnings import filterwarnings

import pytest  # type: ignore

import redshift_connector

# Tests relating to the pandas and numpy operation of the database driver
# redshift_connector custom interface.


@pytest.fixture
def db_table(request, con):
    filterwarnings("ignore", "DB-API extension cursor.next()")
    filterwarnings("ignore", "DB-API extension cursor.__iter__()")
    con.paramstyle = "format"
    with con.cursor() as cursor:
        cursor.execute("drop table if exists book")
        cursor.execute("create Temp table book(bookname varchar,author‎ varchar)")

    def fin():
        try:
            with con.cursor() as cursor:
                cursor.execute("drop table if exists book")
        except redshift_connector.ProgrammingError:
            pass

    request.addfinalizer(fin)
    return con


@pandas_only
def test_fetch_dataframe(db_table):
    import numpy as np  # type: ignore
    import pandas as pd  # type: ignore

    df = pd.DataFrame(
        np.array(
            [
                ["One Hundred Years of Solitude", "Gabriel García Márquez"],
                ["A Brief History of Time", "Stephen Hawking"],
            ]
        ),
        columns=["bookname", "author‎"],
    )
    with db_table.cursor() as cursor:
        cursor.executemany(
            "insert into book (bookname, author‎) values (%s, %s)",
            [
                ("One Hundred Years of Solitude", "Gabriel García Márquez"),
                ("A Brief History of Time", "Stephen Hawking"),
            ],
        )
        cursor.execute("select * from book; ")
        result = cursor.fetch_dataframe()
        assert result.columns[0] == "bookname"


@pandas_only
def test_write_dataframe(db_table):
    import numpy as np
    import pandas as pd

    df = pd.DataFrame(
        np.array(
            [
                ["One Hundred Years of Solitude", "Gabriel García Márquez"],
                ["A Brief History of Time", "Stephen Hawking"],
            ]
        ),
        columns=["bookname", "author‎"],
    )
    with db_table.cursor() as cursor:
        cursor.write_dataframe(df, "book")
        cursor.execute("select * from book; ")
        result = cursor.fetchall()
        assert len(np.array(result)) == 2


@numpy_only
def test_fetch_numpyarray(db_table):
    with db_table.cursor() as cursor:
        cursor.executemany(
            "insert into book (bookname, author‎) values (%s, %s)",
            [
                ("One Hundred Years of Solitude", "Gabriel García Márquez"),
                ("A Brief History of Time", "Stephen Hawking"),
            ],
        )
        cursor.execute("select * from book; ")
        result = cursor.fetch_numpy_array()
        assert len(result) == 2
