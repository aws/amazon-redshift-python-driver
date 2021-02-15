import configparser
import os
from datetime import datetime as Datetime
from datetime import timezone as Timezone
from warnings import filterwarnings

import pytest  # type: ignore

import redshift_connector

conf = configparser.ConfigParser()
root_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
conf.read(root_path + "/config.ini")


# Tests relating to the basic operation of the database driver, driven by the
# redshift_connector custom interface.


@pytest.fixture
def db_table(request, con):
    filterwarnings("ignore", "DB-API extension cursor.next()")
    filterwarnings("ignore", "DB-API extension cursor.__iter__()")
    con.paramstyle = "format"
    with con.cursor() as cursor:
        cursor.execute("DROP TABLE IF EXISTS t1")
        cursor.execute("CREATE TEMPORARY TABLE t1 (f1 int primary key, " "f2 bigint not null, f3 varchar(50) null) ")

    def fin():
        try:
            with con.cursor() as cursor:
                cursor.execute("drop table if exists t1")
        except redshift_connector.ProgrammingError:
            pass

    request.addfinalizer(fin)
    return con


def test_database_error(cursor):
    with pytest.raises(redshift_connector.ProgrammingError):
        cursor.execute("INSERT INTO t99 VALUES (1, 2, 3)")


def test_parallel_queries(db_table):
    with db_table.cursor() as cursor:
        cursor.execute("INSERT INTO t1 (f1, f2, f3) VALUES (%s, %s, %s)", (1, 1, None))
        cursor.execute("INSERT INTO t1 (f1, f2, f3) VALUES (%s, %s, %s)", (2, 10, None))
        cursor.execute("INSERT INTO t1 (f1, f2, f3) VALUES (%s, %s, %s)", (3, 100, None))
        cursor.execute("INSERT INTO t1 (f1, f2, f3) VALUES (%s, %s, %s)", (4, 1000, None))
        cursor.execute("INSERT INTO t1 (f1, f2, f3) VALUES (%s, %s, %s)", (5, 10000, None))
        with db_table.cursor() as c1, db_table.cursor() as c2:
            c1.execute("SELECT f1, f2, f3 FROM t1")
            for row in c1:
                f1, f2, f3 = row
                c2.execute("SELECT f1, f2, f3 FROM t1 WHERE f1 > %s", (f1,))
                for row in c2:
                    f1, f2, f3 = row


def test_parallel_open_portals(con):
    with con.cursor() as c1, con.cursor() as c2:
        c1count, c2count = 0, 0
        q = "select * from generate_series(1, %s)"
        params = (100,)
        c1.execute(q, params)
        c2.execute(q, params)
        for c2row in c2:
            c2count += 1
        for c1row in c1:
            c1count += 1

    assert c1count == c2count


# Run a query on a table, alter the structure of the table, then run the
# original query again.


def test_alter(db_table):
    with db_table.cursor() as cursor:
        cursor.execute("select * from t1")
        cursor.execute("alter table t1 drop column f3")
        cursor.execute("select * from t1")


# Run a query on a table, drop then re-create the table, then run the
# original query again.


def test_create(db_table):
    with db_table.cursor() as cursor:
        cursor.execute("select * from t1")
        cursor.execute("drop table t1")
        cursor.execute("create temporary table t1 (f1 int primary key)")
        cursor.execute("select * from t1")


def test_insert_returning(db_table):
    with db_table.cursor() as cursor:
        cursor.execute("CREATE TABLE t2 (id int, data varchar(20))")
        row_id = 1
        # Test INSERT ... RETURNING with one row...
        cursor.execute("INSERT INTO t2 VALUES (%s, %s)", (row_id, "test1"))

        assert cursor.rowcount == 1

        cursor.execute("SELECT data FROM t2 WHERE id = %s", (row_id,))
        assert "test1" == cursor.fetchone()[0]

        # Test with multiple rows...
        cursor.execute("INSERT INTO t2 VALUES (2, 'test2'), (3, 'test3'), (4,'test4') ")
        assert cursor.rowcount == 3
        cursor.execute("SELECT * FROM t2")
        ids = cursor.fetchall()
        assert len(ids) == 4


# why the expected count = -1?
# because the protocol version of redshift does not
# support the row_count when execute 'SELECT' and 'COPY'
def test_row_count(db_table):
    with db_table.cursor() as cursor:
        expected_count = 57
        cursor.executemany(
            "INSERT INTO t1 (f1, f2, f3) VALUES (%s, %s, %s)", tuple((i, i, None) for i in range(expected_count))
        )

        # Check rowcount after executemany
        assert expected_count == cursor.rowcount

        cursor.execute("SELECT * FROM t1")

        # Check row_count without doing any reading first...
        assert -1 == cursor.rowcount

        # Check rowcount after reading some rows, make sure it still
        # works...
        for i in range(expected_count // 2):
            cursor.fetchone()
        assert -1 == cursor.rowcount

    with db_table.cursor() as cursor:
        # Restart the cursor, read a few rows, and then check rowcount
        # again...
        cursor.execute("SELECT * FROM t1")
        for i in range(expected_count // 3):
            cursor.fetchone()
        assert -1 == cursor.rowcount

        # Should be -1 for a command with no results
        cursor.execute("DROP TABLE t1")
        assert -1 == cursor.rowcount


def test_row_count_fetch(db_table):
    with db_table.cursor() as cursor:
        cursor.execute("INSERT INTO t1 (f1, f2, f3) VALUES (%s, %s, %s)", (1, 1, None))
        cursor.execute("SELECT * FROM t1")
        cursor.fetchall()
        assert -1 == cursor.rowcount


def test_row_count_update(db_table):
    with db_table.cursor() as cursor:
        cursor.execute("INSERT INTO t1 (f1, f2, f3) VALUES (%s, %s, %s)", (1, 1, None))
        cursor.execute("INSERT INTO t1 (f1, f2, f3) VALUES (%s, %s, %s)", (2, 10, None))
        cursor.execute("INSERT INTO t1 (f1, f2, f3) VALUES (%s, %s, %s)", (3, 100, None))
        cursor.execute("INSERT INTO t1 (f1, f2, f3) VALUES (%s, %s, %s)", (4, 1000, None))
        cursor.execute("INSERT INTO t1 (f1, f2, f3) VALUES (%s, %s, %s)", (5, 10000, None))
        cursor.execute("UPDATE t1 SET f3 = %s WHERE f2 > 101", ("Hello!",))
        assert cursor.rowcount == 2


def test_int_oid(cursor):
    cursor.execute("SELECT typname FROM pg_type WHERE oid = %s", (100,))


def test_unicode_query(cursor):
    cursor.execute(
        "CREATE TEMPORARY TABLE \u043c\u0435\u0441\u0442\u043e "
        "(\u0438\u043c\u044f VARCHAR(50), "
        "\u0430\u0434\u0440\u0435\u0441 VARCHAR(250))"
    )


def test_executemany(db_table):
    with db_table.cursor() as cursor:
        cursor.executemany("INSERT INTO t1 (f1, f2, f3) VALUES (%s, %s, %s)", ((1, 1, "Avast ye!"), (2, 1, None)))

        cursor.executemany("select %s", ((Datetime(2014, 5, 7, tzinfo=Timezone.utc),), (Datetime(2014, 5, 7),)))


# Check that autocommit stays off
# We keep track of whether we're in a transaction or not by using the
# READY_FOR_QUERY message.
def test_transactions(db_table):
    with db_table.cursor() as cursor:
        cursor.execute("commit")
        cursor.execute("INSERT INTO t1 (f1, f2, f3) VALUES (%s, %s, %s)", (1, 1, "Zombie"))
        assert cursor.rowcount == 1
        cursor.execute("rollback")
        cursor.execute("select * from t1")

        assert len(cursor.fetchall()) == 0


def test_in(cursor):
    cursor.execute("SELECT typname FROM pg_type WHERE oid = any(%s)", ([16, 23],))
    ret = cursor.fetchall()
    assert ret[0][0] == "bool"


def test_no_previous_tpc(con):
    con.tpc_begin("Stacey")
    with con.cursor() as cursor:
        cursor.execute("SELECT * FROM pg_type")
        con.tpc_commit()


# Check that tpc_recover() doesn't start a transaction
def test_tpc_recover(con):
    con.tpc_recover()
    with con.cursor() as cursor:
        con.autocommit = True

        # If tpc_recover() has started a transaction, this will fail
        cursor.execute("VACUUM")


# An empty query should raise a ProgrammingError
def test_empty_query(cursor):
    with pytest.raises(redshift_connector.ProgrammingError):
        cursor.execute("")


def test_context_manager_class(con):
    assert "__enter__" in redshift_connector.core.Cursor.__dict__
    assert "__exit__" in redshift_connector.core.Cursor.__dict__

    with con.cursor() as cursor:
        cursor.execute("select 1")


def test_get_procedures(con):
    with con.cursor() as cursor:
        cursor.execute(
            "CREATE OR REPLACE PROCEDURE test_sp1(f1 int, f2 varchar(20))"
            " AS $$"
            " DECLARE"
            "   min_val int;"
            " BEGIN"
            "   DROP TABLE IF EXISTS tmp_tbl;"
            "   CREATE TEMP TABLE tmp_tbl(id int);"
            "   INSERT INTO tmp_tbl values (f1),(10001),(10002);"
            "   SELECT INTO min_val MIN(id) FROM tmp_tbl;"
            "   RAISE INFO 'min_val = %, f2 = %', min_val, f2;"
            " END;"
            " $$ LANGUAGE plpgsql;"
        )
        res = cursor.get_procedures()
        assert len(res) > 0


def test_get_schemas(con):
    with con.cursor() as cursor:
        cursor.execute(
            "create schema IF NOT EXISTS schema_test1 authorization {awsuser}".format(
                awsuser=conf.get("ci-cluster", "test_user")
            )
        )
        res = cursor.get_schemas(schema_pattern="schema_test1")
        assert res[0][0] == "schema_test1"


def test_get_primary_keys(con):
    with con.cursor() as cursor:
        cursor.execute("CREATE TABLE table_primary_key (f1 int primary key, f3 varchar(20) null) ")
        key = cursor.get_primary_keys(table="table_primary_key")
        assert key[0][3] == "f1"

        cursor.execute(
            "create schema IF NOT EXISTS schema_test1 authorization {awsuser}".format(
                awsuser=conf.get("ci-cluster", "test_user")
            )
        )
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS {database}.schema_test1.test_primary (f1 int primary key, f3 varchar(50) null)".format(
                database=conf.get("ci-cluster", "database")
            )
        )
        key = cursor.get_primary_keys(schema="schema_test1", table="test_primary")
        assert key[0][3] == "f1"


def test_get_columns(con):
    with con.cursor() as cursor:
        cursor.execute("create table book (bookname varchar, author varchar, price int)")
        columns = cursor.get_columns(tablename_pattern="book")
        assert len(columns) == 3

        cursor.execute(
            "create schema IF NOT EXISTS schema_test1 authorization {awsuser}".format(
                awsuser=conf.get("ci-cluster", "test_user")
            )
        )
        cursor.execute(
            "create table IF NOT EXISTS {database}.schema_test1.table_columns (bookname varchar, author varchar)".format(
                database=conf.get("ci-cluster", "database")
            )
        )
        columns = cursor.get_columns(schema_pattern="schema_test1", tablename_pattern="table_columns")
        assert len(columns) == 2


def test_get_tables(con):
    with con.cursor() as cursor:
        num = len(cursor.get_tables(types=["TABLE"]))
        cursor.execute("create table test_exist (f1 varchar)")
        new_num = len(cursor.get_tables(types=["TABLE"]))
        assert new_num - num == 1

        cursor.execute(
            "create schema IF NOT EXISTS schema_test1 authorization {awsuser}".format(
                awsuser=conf.get("ci-cluster", "test_user")
            )
        )
        num = len(cursor.get_tables(schema_pattern="schema_test1", types=["TABLE"]))
        cursor.execute(
            "create table IF NOT EXISTS {database}.schema_test1.book (bookname varchar, author varchar)".format(
                database=conf.get("ci-cluster", "database")
            )
        )
        new_num = len(cursor.get_tables(schema_pattern="schema_test1", types=["TABLE"]))
        assert new_num - num == 1


def test_merge_read(con):
    with con.cursor() as cursor:
        cursor.execute("create temp table m1(c1 integer);")
        sqls = [
            "insert into m1 values(1);",
            "insert into m1 values(2);",
            "insert into m1 values(3);",
            "insert into m1 values(4);",
            "select count(*) from m1;",
        ]
        for sql in sqls:
            cursor.execute(sql)
        rows = cursor.fetchall()
        for val in [True, False]:
            cursor.execute("select * from m1", merge_socket_read=val)
            res = cursor.fetchall()
            assert rows[0][0] == len(res)


def test_handle_COMMAND_COMPLETE_closed_ps(con, mocker):
    with con.cursor() as cursor:
        cursor.execute("drop table if exists t1")

        spy = mocker.spy(con, "close_prepared_statement")
        cursor.execute("create table t1 (a int primary key)")

        assert len(con._caches) == 1
        cache_iter = next(iter(con._caches.values()))  # get first transaction
        assert len(next(iter(cache_iter.values()))["statement"]) == 3  # should be 3 ps in this transaction
        # begin transaction, drop table t1, create table t1
        assert spy.called
        assert spy.call_count == 3


@pytest.mark.parametrize("_input", ["NO_SCHEMA_UNIVERSAL_QUERY", "EXTERNAL_SCHEMA_QUERY", "LOCAL_SCHEMA_QUERY"])
def test___get_table_filter_clause_throws_for_bad_type(con, _input):
    with con.cursor() as cursor:
        with pytest.raises(redshift_connector.InterfaceError):
            cursor.get_tables(schema_pattern=_input, types=["garbage"])
