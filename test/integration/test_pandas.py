from test.utils import numpy_only, pandas_only
from warnings import filterwarnings

import pytest  # type: ignore

import redshift_connector
from redshift_connector.config import DbApiParamstyle

# Tests relating to the pandas and numpy operation of the database driver
# redshift_connector custom interface.


@pytest.fixture
def db_table(request, con: redshift_connector.Connection) -> redshift_connector.Connection:
    filterwarnings("ignore", "DB-API extension cursor.next()")
    filterwarnings("ignore", "DB-API extension cursor.__iter__()")
    con.paramstyle = "format"  # type: ignore
    with con.cursor() as cursor:
        cursor.execute("drop table if exists book")
        cursor.execute("create Temp table book(bookname varchar,author‎ varchar)")

    def fin() -> None:
        try:
            with con.cursor() as cursor:
                cursor.execute("drop table if exists book")
        except redshift_connector.ProgrammingError:
            pass

    request.addfinalizer(fin)
    return con


@pandas_only
def test_fetch_dataframe(db_table) -> None:
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
        assert result.columns[1] == "author\u200e"


@pandas_only
@pytest.mark.parametrize("paramstyle", DbApiParamstyle.list())
def test_write_dataframe(db_table, paramstyle) -> None:
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
    db_table.paramstyle = paramstyle

    with db_table.cursor() as cursor:
        cursor.write_dataframe(df, "book")
        cursor.execute("select * from book; ")
        result = cursor.fetchall()
        assert len(np.array(result)) == 2

    assert db_table.paramstyle == paramstyle


@numpy_only
def test_fetch_numpyarray(db_table) -> None:
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
