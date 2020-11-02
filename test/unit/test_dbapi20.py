import time

import redshift_connector

driver = redshift_connector


def test_apilevel():
    # Must exist
    apilevel = driver.apilevel

    # Must equal 2.0
    assert apilevel == "2.0"


def test_threadsafety():
    try:
        # Must exist
        threadsafety = driver.threadsafety
        # Must be a valid value
        assert threadsafety in (0, 1, 2, 3)
    except AttributeError:
        assert False, "Driver doesn't define threadsafety"


def test_paramstyle():
    try:
        # Must exist
        paramstyle = driver.paramstyle
        # Must be a valid value
        assert paramstyle in ("qmark", "numeric", "named", "format", "pyformat")
    except AttributeError:
        assert False, "Driver doesn't define paramstyle"


def test_Exceptions():
    # Make sure required exceptions exist, and are in the
    # defined heirarchy.
    assert issubclass(driver.Warning, Exception)
    assert issubclass(driver.Error, Exception)
    assert issubclass(driver.InterfaceError, driver.Error)
    assert issubclass(driver.DatabaseError, driver.Error)
    assert issubclass(driver.OperationalError, driver.Error)
    assert issubclass(driver.IntegrityError, driver.Error)
    assert issubclass(driver.InternalError, driver.Error)
    assert issubclass(driver.ProgrammingError, driver.Error)
    assert issubclass(driver.NotSupportedError, driver.Error)


def test_Date():
    driver.Date(2002, 12, 25)
    driver.DateFromTicks(time.mktime((2002, 12, 25, 0, 0, 0, 0, 0, 0)))


def test_Time():
    driver.Time(13, 45, 30)
    driver.TimeFromTicks(time.mktime((2001, 1, 1, 13, 45, 30, 0, 0, 0)))


def test_Timestamp():
    driver.Timestamp(2002, 12, 25, 13, 45, 30)
    driver.TimestampFromTicks(time.mktime((2002, 12, 25, 13, 45, 30, 0, 0, 0)))


def test_Binary():
    driver.Binary(b"Something")
    driver.Binary(b"")


def test_STRING():
    assert hasattr(driver, "STRING"), "module.STRING must be defined"


def test_BINARY():
    assert hasattr(driver, "BINARY"), "module.BINARY must be defined."


def test_NUMBER():
    assert hasattr(driver, "NUMBER"), "module.NUMBER must be defined."


def test_DATETIME():
    assert hasattr(driver, "DATETIME"), "module.DATETIME must be defined."


def test_ROWID():
    assert hasattr(driver, "ROWID"), "module.ROWID must be defined."
