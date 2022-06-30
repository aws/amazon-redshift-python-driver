import pytest

import redshift_connector
from redshift_connector.utils.oids import RedshiftOID


def test_errors_available_on_module():
    assert redshift_connector.Warning
    assert redshift_connector.Error
    assert redshift_connector.InterfaceError
    assert redshift_connector.DatabaseError
    assert redshift_connector.DataError
    assert redshift_connector.OperationalError
    assert redshift_connector.IntegrityError
    assert redshift_connector.InternalError
    assert redshift_connector.ProgrammingError
    assert redshift_connector.NotSupportedError
    assert redshift_connector.ArrayContentNotSupportedError
    assert redshift_connector.ArrayContentNotHomogenousError
    assert redshift_connector.ArrayDimensionsNotConsistentError


def test_cursor_on_module():
    assert redshift_connector.Cursor


def test_connection_on_module():
    assert redshift_connector.Connection


def test_version_on_module():
    assert redshift_connector.__version__


@pytest.mark.parametrize("datatype", [d.name for d in RedshiftOID])
def test_datatypes_on_module(datatype):
    assert datatype in redshift_connector.__all__
