import warnings

import pytest  # type: ignore

import redshift_connector

driver = redshift_connector
table_prefix = "dbapi20test_"  # If you need to specify a prefix for tables

ddl1 = "create table %sbooze (name varchar(20))" % table_prefix
ddl2 = "create table %sbarflys (name varchar(20))" % table_prefix
xddl1 = "drop table if exists %sbooze" % table_prefix
xddl2 = "drop table if exists %sbarflys" % table_prefix

# Name of stored procedure to convert
# string->lowercase
lowerfunc = "lower"


# Some drivers may need to override these helpers, for example adding
# a 'commit' after the execute.
def executeDDL1(cursor):
    cursor.execute(xddl1)
    cursor.execute(ddl1)


def executeDDL2(cursor):
    cursor.execute(xddl2)
    cursor.execute(ddl2)


@pytest.fixture
def db(request, con):
    def fin():
        with con.cursor() as cur:
            for ddl in (xddl1, xddl2):
                # try:
                cur.execute(ddl)
                con.commit()
                # except driver.Error:
                #     # Assume table didn't exist. Other tests will check if
                #     # execute is busted.
                #     pass

    request.addfinalizer(fin)
    return con


def test_ExceptionsAsConnectionAttributes(con):
    # OPTIONAL EXTENSION
    # Test for the optional DB API 2.0 extension, where the exceptions
    # are exposed as attributes on the Connection object
    # I figure this optional extension will be implemented by any
    # driver author who is using this test suite, so it is enabled
    # by default.
    warnings.simplefilter("ignore")
    drv = driver
    assert con.Warning is drv.Warning
    assert con.Error is drv.Error
    assert con.InterfaceError is drv.InterfaceError
    assert con.DatabaseError is drv.DatabaseError
    assert con.OperationalError is drv.OperationalError
    assert con.IntegrityError is drv.IntegrityError
    assert con.InternalError is drv.InternalError
    assert con.ProgrammingError is drv.ProgrammingError
    assert con.NotSupportedError is drv.NotSupportedError
    warnings.resetwarnings()


def test_commit(con):
    # Commit must work, even if it doesn't do anything
    con.commit()


def test_rollback(con):
    # If rollback is defined, it should either work or throw
    # the documented exception
    if hasattr(con, "rollback"):
        try:
            con.rollback()
        except driver.NotSupportedError:
            pass


def test_cursor(con):
    con.cursor()


def test_cursor_isolation(con):
    # Make sure cursors created from the same connection have
    # the documented transaction isolation level
    cur1 = con.cursor()
    cur2 = con.cursor()
    executeDDL1(cur1)
    cur1.execute("insert into %sbooze values ('Victoria Bitter')" % (table_prefix))
    cur2.execute("select name from %sbooze" % table_prefix)
    booze = cur2.fetchall()
    assert len(booze) == 1
    assert len(booze[0]) == 1
    assert booze[0][0] == "Victoria Bitter"


def test_description(con):
    cur = con.cursor()
    executeDDL1(cur)
    assert cur.description is None, (
        "cursor.description should be none after executing a " "statement that can return no rows (such as DDL)"
    )
    cur.execute("select name from %sbooze" % table_prefix)
    assert len(cur.description) == 1, "cursor.description describes too many columns"
    assert len(cur.description[0]) == 7, "cursor.description[x] tuples must have 7 elements"
    assert cur.description[0][0].lower() == b"name", "cursor.description[x][0] must return column name"
    assert cur.description[0][1] == driver.STRING, (
        "cursor.description[x][1] must return column type. Got %r" % cur.description[0][1]
    )

    # Make sure self.description gets reset
    executeDDL2(cur)
    assert cur.description is None, (
        "cursor.description not being set to None when executing " "no-result statements (eg. DDL)"
    )


def test_rowcount(cursor):
    executeDDL1(cursor)
    assert cursor.rowcount == -1, "cursor.rowcount should be -1 after executing no-result " "statements"
    cursor.execute("insert into %sbooze values ('Victoria Bitter')" % (table_prefix))
    assert cursor.rowcount in (-1, 1), (
        "cursor.rowcount should == number or rows inserted, or " "set to -1 after executing an insert statement"
    )
    cursor.execute("select name from %sbooze" % table_prefix)
    assert cursor.rowcount in (-1, 1), (
        "cursor.rowcount should == number of rows returned, or " "set to -1 after executing a select statement"
    )
    executeDDL2(cursor)
    assert cursor.rowcount == -1, "cursor.rowcount not being reset to -1 after executing " "no-result statements"


def test_callproc(cursor):
    cursor.execute(
        """
CREATE PROCEDURE echo(INOUT val text)
  LANGUAGE plpgsql AS
$proc$
BEGIN
END
$proc$;
"""
    )

    cursor.callproc("echo", ["hello"])
    assert cursor.fetchall() == (["hello"],)


def test_close(con):
    cur = con.cursor()
    con.close()

    # cursor.execute should raise an Error if called after connection
    # closed
    with pytest.raises(driver.Error):
        executeDDL1(cur)

    # connection.commit should raise an Error if called after connection'
    # closed.'
    with pytest.raises(driver.Error):
        con.commit()

    # connection.close should raise an Error if called more than once
    with pytest.raises(driver.Error):
        con.close()


def test_execute(con):
    cur = con.cursor()
    _paraminsert(cur)


def _paraminsert(cur):
    executeDDL1(cur)
    cur.execute("insert into %sbooze values ('Victoria Bitter')" % (table_prefix))
    assert cur.rowcount in (-1, 1)

    if driver.paramstyle == "qmark":
        cur.execute("insert into %sbooze values (?)" % table_prefix, ("Cooper's",))
    elif driver.paramstyle == "numeric":
        cur.execute("insert into %sbooze values (:1)" % table_prefix, ("Cooper's",))
    elif driver.paramstyle == "named":
        cur.execute("insert into %sbooze values (:beer)" % table_prefix, {"beer": "Cooper's"})
    elif driver.paramstyle == "format":
        cur.execute("insert into %sbooze values (%%s)" % table_prefix, ("Cooper's",))
    elif driver.paramstyle == "pyformat":
        cur.execute("insert into %sbooze values (%%(beer)s)" % table_prefix, {"beer": "Cooper's"})
    else:
        assert False, "Invalid paramstyle"

    assert cur.rowcount in (-1, 1)

    cur.execute("select name from %sbooze" % table_prefix)
    res = cur.fetchall()
    assert len(res) == 2, "cursor.fetchall returned too few rows"
    beers = [res[0][0], res[1][0]]
    beers.sort()
    assert beers[0] == "Cooper's", "cursor.fetchall retrieved incorrect data, or data inserted " "incorrectly"
    assert beers[1] == "Victoria Bitter", "cursor.fetchall retrieved incorrect data, or data inserted " "incorrectly"


def test_executemany(cursor):
    executeDDL1(cursor)
    largs = [("Cooper's",), ("Boag's",)]
    margs = [{"beer": "Cooper's"}, {"beer": "Boag's"}]
    if driver.paramstyle == "qmark":
        cursor.executemany("insert into %sbooze values (?)" % table_prefix, largs)
    elif driver.paramstyle == "numeric":
        cursor.executemany("insert into %sbooze values (:1)" % table_prefix, largs)
    elif driver.paramstyle == "named":
        cursor.executemany("insert into %sbooze values (:beer)" % table_prefix, margs)
    elif driver.paramstyle == "format":
        cursor.executemany("insert into %sbooze values (%%s)" % table_prefix, largs)
    elif driver.paramstyle == "pyformat":
        cursor.executemany("insert into %sbooze values (%%(beer)s)" % (table_prefix), margs)
    else:
        assert False, "Unknown paramstyle"

    assert cursor.rowcount in (-1, 2), (
        "insert using cursor.executemany set cursor.rowcount to " "incorrect value %r" % cursor.rowcount
    )

    cursor.execute("select name from %sbooze" % table_prefix)
    res = cursor.fetchall()
    assert len(res) == 2, "cursor.fetchall retrieved incorrect number of rows"
    beers = [res[0][0], res[1][0]]
    beers.sort()
    assert beers[0] == "Boag's", "incorrect data retrieved"
    assert beers[1] == "Cooper's", "incorrect data retrieved"


def test_fetchone(cursor):
    # cursor.fetchone should raise an Error if called before
    # executing a select-type query
    with pytest.raises(driver.Error):
        cursor.fetchone()

    # cursor.fetchone should raise an Error if called after
    # executing a query that cannnot return rows
    executeDDL1(cursor)
    with pytest.raises(driver.Error):
        cursor.fetchone()

    cursor.execute("select name from %sbooze" % table_prefix)
    assert cursor.fetchone() is None, "cursor.fetchone should return None if a query retrieves " "no rows"
    assert cursor.rowcount in (-1, 0)

    # cursor.fetchone should raise an Error if called after
    # executing a query that cannnot return rows
    cursor.execute("insert into %sbooze values ('Victoria Bitter')" % (table_prefix))
    with pytest.raises(driver.Error):
        cursor.fetchone()

    cursor.execute("select name from %sbooze" % table_prefix)
    r = cursor.fetchone()
    assert len(r) == 1, "cursor.fetchone should have retrieved a single row"
    assert r[0] == "Victoria Bitter", "cursor.fetchone retrieved incorrect data"
    assert cursor.fetchone() is None, "cursor.fetchone should return None if no more rows available"
    assert cursor.rowcount in (-1, 1)


samples = ["Carlton Cold", "Carlton Draft", "Mountain Goat", "Redback", "Victoria Bitter", "XXXX"]


def _populate():
    """Return a list of sql commands to setup the DB for the fetch
    tests.
    """
    populate = ["insert into %sbooze values ('%s')" % (table_prefix, s) for s in samples]
    return populate


def test_fetchmany(cursor):
    # cursor.fetchmany should raise an Error if called without
    # issuing a query
    with pytest.raises(driver.Error):
        cursor.fetchmany(4)

    executeDDL1(cursor)
    for sql in _populate():
        cursor.execute(sql)

    cursor.execute("select name from %sbooze" % table_prefix)
    r = cursor.fetchmany()
    assert len(r) == 1, "cursor.fetchmany retrieved incorrect number of rows, " "default of arraysize is one."
    cursor.arraysize = 10
    r = cursor.fetchmany(3)  # Should get 3 rows
    assert len(r) == 3, "cursor.fetchmany retrieved incorrect number of rows"
    r = cursor.fetchmany(4)  # Should get 2 more
    assert len(r) == 2, "cursor.fetchmany retrieved incorrect number of rows"
    r = cursor.fetchmany(4)  # Should be an empty sequence
    assert len(r) == 0, "cursor.fetchmany should return an empty sequence after " "results are exhausted"
    assert cursor.rowcount in (-1, 6)

    # Same as above, using cursor.arraysize
    cursor.arraysize = 4
    cursor.execute("select name from %sbooze" % table_prefix)
    r = cursor.fetchmany()  # Should get 4 rows
    assert len(r) == 4, "cursor.arraysize not being honoured by fetchmany"
    r = cursor.fetchmany()  # Should get 2 more
    assert len(r) == 2
    r = cursor.fetchmany()  # Should be an empty sequence
    assert len(r) == 0
    assert cursor.rowcount in (-1, 6)

    cursor.arraysize = 6
    cursor.execute("select name from %sbooze" % table_prefix)
    rows = cursor.fetchmany()  # Should get all rows
    assert cursor.rowcount in (-1, 6)
    assert len(rows) == 6
    assert len(rows) == 6
    rows = [row[0] for row in rows]
    rows.sort()

    # Make sure we get the right data back out
    for i in range(0, 6):
        assert rows[i] == samples[i], "incorrect data retrieved by cursor.fetchmany"

    rows = cursor.fetchmany()  # Should return an empty list
    assert len(rows) == 0, (
        "cursor.fetchmany should return an empty sequence if " "called after the whole result set has been fetched"
    )
    assert cursor.rowcount in (-1, 6)

    executeDDL2(cursor)
    cursor.execute("select name from %sbarflys" % table_prefix)
    r = cursor.fetchmany()  # Should get empty sequence
    assert len(r) == 0, "cursor.fetchmany should return an empty sequence if " "query retrieved no rows"
    assert cursor.rowcount in (-1, 0)


def test_fetchall(cursor):
    # cursor.fetchall should raise an Error if called
    # without executing a query that may return rows (such
    # as a select)
    with pytest.raises(driver.Error):
        cursor.fetchall()

    executeDDL1(cursor)
    for sql in _populate():
        cursor.execute(sql)

    # cursor.fetchall should raise an Error if called
    # after executing a a statement that cannot return rows
    with pytest.raises(driver.Error):
        cursor.fetchall()

    cursor.execute("select name from %sbooze" % table_prefix)
    rows = cursor.fetchall()
    assert cursor.rowcount in (-1, len(samples))
    assert len(rows) == len(samples), "cursor.fetchall did not retrieve all rows"
    rows = [r[0] for r in rows]
    rows.sort()
    for i in range(0, len(samples)):
        assert rows[i] == samples[i], "cursor.fetchall retrieved incorrect rows"
    rows = cursor.fetchall()
    assert len(rows) == 0, (
        "cursor.fetchall should return an empty list if called " "after the whole result set has been fetched"
    )
    assert cursor.rowcount in (-1, len(samples))

    executeDDL2(cursor)
    cursor.execute("select name from %sbarflys" % table_prefix)
    rows = cursor.fetchall()
    assert cursor.rowcount in (-1, 0)
    assert len(rows) == 0, "cursor.fetchall should return an empty list if " "a select query returns no rows"


def test_mixedfetch(cursor):
    executeDDL1(cursor)
    for sql in _populate():
        cursor.execute(sql)

    cursor.execute("select name from %sbooze" % table_prefix)
    rows1 = cursor.fetchone()
    rows23 = cursor.fetchmany(2)
    rows4 = cursor.fetchone()
    rows56 = cursor.fetchall()
    assert cursor.rowcount in (-1, 6)
    assert len(rows23) == 2, "fetchmany returned incorrect number of rows"
    assert len(rows56) == 2, "fetchall returned incorrect number of rows"

    rows = [rows1[0]]
    rows.extend([rows23[0][0], rows23[1][0]])
    rows.append(rows4[0])
    rows.extend([rows56[0][0], rows56[1][0]])
    rows.sort()
    for i in range(0, len(samples)):
        assert rows[i] == samples[i], "incorrect data retrieved or inserted"


def help_nextset_setUp(cur):
    """Should create a procedure called deleteme
    that returns two result sets, first the
    number of rows in booze then "name from booze"
    """
    raise NotImplementedError("Helper not implemented")


def help_nextset_tearDown(cur):
    "If cleaning up is needed after nextSetTest"
    raise NotImplementedError("Helper not implemented")


def test_nextset(cursor):
    if not hasattr(cursor, "nextset"):
        return

    try:
        executeDDL1(cursor)
        sql = _populate()
        for sql in _populate():
            cursor.execute(sql)

        help_nextset_setUp(cursor)

        cursor.callproc("deleteme")
        numberofrows = cursor.fetchone()
        assert numberofrows[0] == len(samples)
        assert cursor.nextset()
        names = cursor.fetchall()
        assert len(names) == len(samples)
        s = cursor.nextset()
        assert s is None, "No more return sets, should return None"
    finally:
        help_nextset_tearDown(cursor)


def test_arraysize(cursor):
    # Not much here - rest of the tests for this are in test_fetchmany
    assert hasattr(cursor, "arraysize"), "cursor.arraysize must be defined"


def test_setinputsizes(cursor):
    cursor.setinputsizes((25,))
    _paraminsert(cursor)  # Make sure cursor still works


def test_setoutputsize_basic(cursor):
    # Basic test is to make sure setoutputsize doesn't blow up
    cursor.setoutputsize(1000)
    cursor.setoutputsize(2000, 0)
    _paraminsert(cursor)  # Make sure the cursor still works


def test_None(cursor):
    executeDDL1(cursor)
    cursor.execute("insert into %sbooze values (NULL)" % table_prefix)
    cursor.execute("select name from %sbooze" % table_prefix)
    r = cursor.fetchall()
    assert len(r) == 1
    assert len(r[0]) == 1
    assert r[0][0] is None, "NULL value not returned as None"
