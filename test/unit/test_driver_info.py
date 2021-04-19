import re

import pytest

from redshift_connector.utils import DriverInfo


def test_version_is_not_none():
    assert DriverInfo.version() is not None


def test_version_is_str():
    assert isinstance(DriverInfo.version(), str)


def test_version_proper_format():
    version_regex = re.compile(r"^\d+(\.\d+){2,3}$")
    assert version_regex.match(DriverInfo.version())


def test_driver_name_is_not_none():
    assert DriverInfo.driver_name() is not None


def test_driver_short_name_is_not_none():
    assert DriverInfo.driver_short_name() is not None


def test_driver_full_name_is_not_none():
    assert DriverInfo.driver_full_name() is not None


def test_driver_full_name_contains_name():
    assert DriverInfo.driver_name() in DriverInfo.driver_full_name()


def test_driver_full_name_contains_version():
    assert DriverInfo.version() in DriverInfo.driver_full_name()
