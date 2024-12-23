import typing
from unittest import TestCase

import pytest  # type: ignore
from collections import deque

from redshift_connector import Connection, Cursor, DataError, InterfaceError
from redshift_connector.metadataServerAPIHelper import MetadataServerAPIHelper
from unittest.mock import Mock, PropertyMock, mock_open, patch, MagicMock, call



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
    with patch("redshift_connector.metadataServerAPIHelper.MetadataServerAPIHelper.call_get_catalog_list", new_callable=PropertyMock()) as mock_call_get_catalog_list,\
        patch("redshift_connector.metadataServerAPIHelper.MetadataServerAPIHelper.quote_ident", new_callable=PropertyMock()) as mock_quote_ident,\
        patch("redshift_connector.metadataServerAPIHelper.MetadataServerAPIHelper.quote_literal", new_callable=PropertyMock()) as mock_quote_literal:

        mock_call_get_catalog_list.return_value = ["testCatalog"]
        mock_quote_ident.return_value = "testIdent"
        mock_quote_literal.return_value = "testLiteral"

        spy = mocker.spy(mock_cursor, "execute")

        mock_metadataServerAPIHelper.get_schema_server_api("testCatalog", "testSchema")

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
    mock_cursor._SHOW_SCHEMAS_Col_index = {"schema_name" : 0}
    mock_cursor._SHOW_TABLES_Col_index = {}

    mock_metadataServerAPIHelper: MetadataServerAPIHelper = MetadataServerAPIHelper(mock_cursor)
    with patch("redshift_connector.metadataServerAPIHelper.MetadataServerAPIHelper.call_get_catalog_list", new_callable=PropertyMock()) as mock_call_get_catalog_list,\
        patch("redshift_connector.metadataServerAPIHelper.MetadataServerAPIHelper.call_show_schema", new_callable=PropertyMock()) as mock_call_show_schema,\
        patch("redshift_connector.metadataServerAPIHelper.MetadataServerAPIHelper.quote_ident", new_callable=PropertyMock()) as mock_quote_ident,\
        patch("redshift_connector.metadataServerAPIHelper.MetadataServerAPIHelper.quote_literal", new_callable=PropertyMock()) as mock_quote_literal:

        mock_call_get_catalog_list.return_value = ["testCatalog"]
        mock_call_show_schema.return_value = (["testSchema"])
        mock_quote_ident.return_value = "testIdent"
        mock_quote_literal.return_value = "testLiteral"

        spy = mocker.spy(mock_cursor, "execute")

        mock_metadataServerAPIHelper.get_table_server_api("testCatalog", "testSchema", "testTable")

        assert spy.called
        assert spy.call_count == 1
        assert "SHOW TABLES" in spy.call_args[0][0]

def test_get_column_server_api(mocker) -> None:
    mocker.patch("redshift_connector.Cursor.execute", return_value=None)
    mocker.patch("redshift_connector.Cursor.fetchall", return_value=None)

    mock_cursor: Cursor = Cursor.__new__(Cursor)
    mock_cursor.paramstyle = "mocked"
    mock_connection: Connection = Connection.__new__(Connection)
    mock_connection.parameter_statuses = deque(maxlen=100)
    mock_connection.parameter_statuses.append((b"show_discovery", 0))
    mock_cursor._c = mock_connection
    mock_cursor.ps = {}
    mock_cursor._SHOW_SCHEMAS_Col_index = {"schema_name": 0}
    mock_cursor._SHOW_TABLES_Col_index = {"table_name": 0}
    mock_cursor._SHOW_COLUMNS_Col_index = {}

    mock_metadataServerAPIHelper: MetadataServerAPIHelper = MetadataServerAPIHelper(mock_cursor)

    with patch("redshift_connector.metadataServerAPIHelper.MetadataServerAPIHelper.call_get_catalog_list", new_callable=PropertyMock()) as mock_call_get_catalog_list,\
        patch("redshift_connector.metadataServerAPIHelper.MetadataServerAPIHelper.call_show_schema", new_callable=PropertyMock()) as mock_call_show_schema, \
        patch("redshift_connector.metadataServerAPIHelper.MetadataServerAPIHelper.call_show_table", new_callable=PropertyMock()) as mock_call_show_table, \
        patch("redshift_connector.metadataServerAPIHelper.MetadataServerAPIHelper.quote_ident", new_callable=PropertyMock()) as mock_quote_ident,\
        patch("redshift_connector.metadataServerAPIHelper.MetadataServerAPIHelper.quote_literal", new_callable=PropertyMock()) as mock_quote_literal:

        mock_call_get_catalog_list.return_value = ["testCatalog"]
        mock_call_show_schema.return_value = (["testSchema"])
        mock_call_show_table.return_value = (["testTable"])
        mock_quote_ident.return_value = "testIdent"
        mock_quote_literal.return_value = "testLiteral"

        spy = mocker.spy(mock_cursor, "execute")

        mock_metadataServerAPIHelper.get_column_server_api("testCatalog", "testSchema", "testTable", "testColumn")

        assert spy.called
        assert spy.call_count == 1
        assert "SHOW COLUMNS" in spy.call_args[0][0]