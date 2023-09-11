import pytest

import redshift_connector
from redshift_connector.utils.oids import RedshiftOID


@pytest.mark.parametrize(
    "error_class",
    (
        "Warning",
        "Error",
        "InterfaceError",
        "DatabaseError",
        "OperationalError",
        "IntegrityError",
        "InternalError",
        "ProgrammingError",
        "NotSupportedError",
        "ArrayContentNotSupportedError",
        "ArrayContentNotHomogenousError",
        "ArrayDimensionsNotConsistentError",
    ),
)
def test_errors_available_on_module(error_class):
    import importlib

    getattr(importlib.import_module("redshift_connector"), error_class)


def test_cursor_on_module():
    import importlib

    getattr(importlib.import_module("redshift_connector"), "Cursor")


def test_connection_on_module():
    import importlib

    getattr(importlib.import_module("redshift_connector"), "Connection")


def test_version_on_module():
    import importlib

    getattr(importlib.import_module("redshift_connector"), "__version__")


@pytest.mark.parametrize("datatype", [d.name for d in RedshiftOID])
def test_datatypes_on_module(datatype):
    assert datatype in redshift_connector.__all__
