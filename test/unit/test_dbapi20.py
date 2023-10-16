import time

import redshift_connector

driver = redshift_connector


def test_apilevel() -> None:
    # Must exist
    apilevel: str = driver.apilevel

    # Must equal 2.0
    assert apilevel == "2.0"


def test_threadsafety() -> None:
    try:
        # Must exist
        threadsafety: int = driver.threadsafety
        # Must be a valid value
        assert threadsafety in (0, 1, 2, 3)
    except AttributeError:
        assert False, "Driver doesn't define threadsafety"


def test_paramstyle() -> None:
    from redshift_connector.config import DbApiParamstyle

    try:
        # Must exist
        paramstyle: str = driver.paramstyle
        # Must be a valid value
        assert paramstyle in DbApiParamstyle.list()
    except AttributeError:
        assert False, "Driver doesn't define paramstyle"


def test_Exceptions() -> None:
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


def test_Date() -> None:
    driver.Date(2002, 12, 25)
    driver.DateFromTicks(time.mktime((2002, 12, 25, 0, 0, 0, 0, 0, 0)))


def test_Time() -> None:
    driver.Time(13, 45, 30)
    driver.TimeFromTicks(time.mktime((2001, 1, 1, 13, 45, 30, 0, 0, 0)))


def test_Timestamp() -> None:
    driver.Timestamp(2002, 12, 25, 13, 45, 30)
    driver.TimestampFromTicks(time.mktime((2002, 12, 25, 13, 45, 30, 0, 0, 0)))


def test_Binary() -> None:
    driver.Binary(b"Something")
    driver.Binary(b"")


def test_STRING() -> None:
    assert hasattr(driver, "STRING"), "module.STRING must be defined"


def test_BINARY() -> None:
    assert hasattr(driver, "BINARY"), "module.BINARY must be defined."


def test_NUMBER() -> None:
    assert hasattr(driver, "NUMBER"), "module.NUMBER must be defined."


def test_DATETIME() -> None:
    assert hasattr(driver, "DATETIME"), "module.DATETIME must be defined."


def test_ROWID() -> None:
    assert hasattr(driver, "ROWID"), "module.ROWID must be defined."
