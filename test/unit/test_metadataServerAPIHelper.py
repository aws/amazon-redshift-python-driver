import typing
import pytest  # type: ignore
from collections import deque

from redshift_connector import Connection, Cursor, DataError, InterfaceError
from redshift_connector.metadataServerAPIHelper import MetadataServerAPIHelper


def test_get_catalog_server_api(mocker) -> None:
    mocker.patch("redshift_connector.Cursor.execute", return_value=None)
    mocker.patch("redshift_connector.Cursor.fetchall", return_value=None)

    mock_cursor: Cursor = Cursor.__new__(Cursor)
    mock_cursor.paramstyle = "mocked"
    mock_connection: Connection = Connection.__new__(Connection)
    mock_connection.parameter_statuses = deque(maxlen=100)
    mock_connection.parameter_statuses.append((b"show_discovery", 0))
    mock_cursor._c = mock_connection
    mock_cursor.ps = {}
    mock_cursor._SHOW_DATABASES_Col_index = {}

    mock_metadataServerAPIHelper: MetadataServerAPIHelper = MetadataServerAPIHelper(mock_cursor)

    spy = mocker.spy(mock_cursor, "execute")

    mock_metadataServerAPIHelper.get_catalog_server_api()

    assert spy.called
    assert spy.call_count == 1
    assert "SHOW DATABASES" in spy.call_args[0][0]


def test_get_schema_server_api(mocker) -> None:
    mocker.patch("redshift_connector.Cursor.execute", return_value=None)
    mocker.patch("redshift_connector.Cursor.fetchall", return_value=None)

    mock_cursor: Cursor = Cursor.__new__(Cursor)
    mock_cursor.paramstyle = "mocked"
    mock_connection: Connection = Connection.__new__(Connection)
    mock_connection.parameter_statuses = deque(maxlen=100)
    mock_connection.parameter_statuses.append((b"show_discovery", 0))
    mock_cursor._c = mock_connection
    mock_cursor.ps = {}
    mock_cursor._SHOW_SCHEMAS_Col_index = {}

    mock_metadataServerAPIHelper: MetadataServerAPIHelper = MetadataServerAPIHelper(mock_cursor)

    spy = mocker.spy(mock_cursor, "execute")

    mock_metadataServerAPIHelper.get_schema_server_api("test_catalog", "test_schema")

    assert spy.called
    assert spy.call_count == 1
    assert "SHOW SCHEMAS" in spy.call_args[0][0]

def test_get_table_server_api(mocker) -> None:
    mocker.patch("redshift_connector.Cursor.execute", return_value=None)
    mocker.patch("redshift_connector.Cursor.fetchall", return_value=None)

    mock_cursor: Cursor = Cursor.__new__(Cursor)
    mock_cursor.paramstyle = "mocked"
    mock_connection: Connection = Connection.__new__(Connection)
    mock_connection.parameter_statuses = deque(maxlen=100)
    mock_connection.parameter_statuses.append((b"show_discovery", 0))
    mock_cursor._c = mock_connection
    mock_cursor.ps = {}
    mock_cursor._SHOW_TABLES_Col_index = {}

    mock_metadataServerAPIHelper: MetadataServerAPIHelper = MetadataServerAPIHelper(mock_cursor)

    spy = mocker.spy(mock_cursor, "execute")

    mock_metadataServerAPIHelper.get_table_server_api("test_catalog", "test_schema", "test_table")

    assert spy.called
    assert spy.call_count == 1
    assert "SHOW TABLES" in spy.call_args[0][0]

def test_get_table_server_api(mocker) -> None:
    mocker.patch("redshift_connector.Cursor.execute", return_value=None)
    mocker.patch("redshift_connector.Cursor.fetchall", return_value=None)

    mock_cursor: Cursor = Cursor.__new__(Cursor)
    mock_cursor.paramstyle = "mocked"
    mock_connection: Connection = Connection.__new__(Connection)
    mock_connection.parameter_statuses = deque(maxlen=100)
    mock_connection.parameter_statuses.append((b"show_discovery", 0))
    mock_cursor._c = mock_connection
    mock_cursor.ps = {}
    mock_cursor._SHOW_COLUMNS_Col_index = {}

    mock_metadataServerAPIHelper: MetadataServerAPIHelper = MetadataServerAPIHelper(mock_cursor)

    spy = mocker.spy(mock_cursor, "execute")

    mock_metadataServerAPIHelper.get_column_server_api("test_catalog", "test_schema", "test_table", "test_column")

    assert spy.called
    assert spy.call_count == 1
    assert "SHOW COLUMNS" in spy.call_args[0][0]