import typing
from unittest import TestCase

import pytest  # type: ignore
from collections import deque

from redshift_connector import Connection, Cursor, DataError, InterfaceError
from redshift_connector.metadataServerProxy import MetadataServerProxy
from unittest.mock import Mock, PropertyMock, mock_open, patch, MagicMock, call



def test_get_catalog_server_proxy(mocker) -> None:
    mocker.patch("redshift_connector.Cursor.execute", return_value=None)
    mocker.patch("redshift_connector.Cursor.fetchall", return_value=None)

    mock_connection: Connection = Connection.__new__(Connection)
    mock_connection.parameter_statuses = deque(maxlen=100)
    mock_connection.parameter_statuses.append((b"show_discovery", 0))
    mock_cursor: Cursor = Cursor(mock_connection)
    mock_cursor.paramstyle = "mocked"
    mock_cursor._c = mock_connection
    mock_cursor.ps = {}
    mock_cursor._SHOW_DATABASES_Col_index = {}

    mock_metadataServerProxy: MetadataServerProxy = MetadataServerProxy(mock_cursor)

    spy = mocker.spy(mock_cursor, "execute")

    mock_metadataServerProxy.get_catalogs()

    assert spy.called
    assert spy.call_count == 1
    assert "SHOW DATABASES;" == spy.call_args[0][0]


def test_get_schema_server_proxy(mocker) -> None:
    mocker.patch("redshift_connector.Cursor.execute", return_value=None)
    mocker.patch("redshift_connector.Cursor.fetchall", return_value=None)

    mock_connection: Connection = Connection.__new__(Connection)
    mock_connection.parameter_statuses = deque(maxlen=100)
    mock_connection.parameter_statuses.append((b"show_discovery", 0))
    mock_cursor: Cursor = Cursor(mock_connection)
    mock_cursor.paramstyle = "mocked"
    mock_cursor._c = mock_connection
    mock_cursor.ps = {}
    mock_cursor._SHOW_SCHEMAS_Col_index = {}

    mock_metadataServerProxy: MetadataServerProxy = MetadataServerProxy(mock_cursor)
    with patch("redshift_connector.metadataServerProxy.MetadataServerProxy.get_catalog_list", new_callable=PropertyMock()) as mock_get_catalog_list:

        mock_get_catalog_list.return_value = ["testCatalog"]

        spy = mocker.spy(mock_cursor, "execute")

        mock_metadataServerProxy.get_schemas("testCatalog", "testSchema")

        assert spy.called
        assert spy.call_count == 1
        assert "SHOW SCHEMAS FROM DATABASE" in spy.call_args[0][0]

def test_get_table_server_proxy(mocker) -> None:
    mocker.patch("redshift_connector.Cursor.execute", return_value=None)
    mocker.patch("redshift_connector.Cursor.fetchall", return_value=None)

    mock_connection: Connection = Connection.__new__(Connection)
    mock_connection.parameter_statuses = deque(maxlen=100)
    mock_connection.parameter_statuses.append((b"show_discovery", 0))
    mock_cursor: Cursor = Cursor(mock_connection)
    mock_cursor.paramstyle = "mocked"
    mock_cursor._c = mock_connection
    mock_cursor.ps = {}
    mock_cursor._SHOW_SCHEMAS_Col_index = {"schema_name" : 0}
    mock_cursor._SHOW_TABLES_Col_index = {}

    mock_metadataServerProxy: MetadataServerProxy = MetadataServerProxy(mock_cursor)
    with patch("redshift_connector.metadataServerProxy.MetadataServerProxy.get_catalog_list", new_callable=PropertyMock()) as mock_get_catalog_list:

        def mock_get_schema_list(catalog, pattern):
            schema_list: typing.List = []
            schema_list.append("testSchema")
            return schema_list

        mocker.patch.object(
            mock_metadataServerProxy,
            'get_schema_list',
            side_effect=mock_get_schema_list
        )

        mock_get_catalog_list.return_value = ["testCatalog"]

        spy = mocker.spy(mock_cursor, "execute")

        mock_metadataServerProxy.get_tables("testCatalog", "testSchema", "testTable")

        assert spy.called
        assert spy.call_count == 1
        assert "SHOW TABLES FROM SCHEMA" in spy.call_args[0][0]

def test_get_column_server_proxy(mocker) -> None:
    mocker.patch("redshift_connector.Cursor.execute", return_value=None)
    mocker.patch("redshift_connector.Cursor.fetchall", return_value=None)

    mock_connection: Connection = Connection.__new__(Connection)
    mock_connection.parameter_statuses = deque(maxlen=100)
    mock_connection.parameter_statuses.append((b"show_discovery", 0))
    mock_cursor: Cursor = Cursor(mock_connection)
    mock_cursor.paramstyle = "mocked"
    mock_cursor._c = mock_connection
    mock_cursor.ps = {}
    mock_cursor._SHOW_SCHEMAS_Col_index = {"schema_name": 0}
    mock_cursor._SHOW_TABLES_Col_index = {"table_name": 0}
    mock_cursor._SHOW_COLUMNS_Col_index = {}

    mock_metadataServerProxy: MetadataServerProxy = MetadataServerProxy(mock_cursor)

    with patch("redshift_connector.metadataServerProxy.MetadataServerProxy.get_catalog_list", new_callable=PropertyMock()) as mock_get_catalog_list:
        def mock_get_schema_list(catalog, pattern):
            schema_list: typing.List = []
            schema_list.append("testSchema")
            return schema_list

        mocker.patch.object(
            mock_metadataServerProxy,
            'get_schema_list',
            side_effect=mock_get_schema_list
        )

        def mock_get_table_list(catalog, schema, table):
            table_list: typing.List = []
            table_list.append("testTable")
            return table_list

        mocker.patch.object(
            mock_metadataServerProxy,
            'get_table_list',
            side_effect=mock_get_table_list
        )

        mock_get_catalog_list.return_value = ["testCatalog"]

        spy = mocker.spy(mock_cursor, "execute")

        mock_metadataServerProxy.get_columns("testCatalog", "testSchema", "testTable", "testColumn")

        assert spy.called
        assert spy.call_count == 1
        assert "SHOW COLUMNS FROM TABLE" in spy.call_args[0][0]


def test_get_primary_keys_server_proxy(mocker) -> None:
    # Mock the cursor
    mocker.patch("redshift_connector.Cursor.execute", return_value=None)
    mocker.patch("redshift_connector.Cursor.fetchall", return_value=None)

    # Setup mock cursor and connection
    mock_connection: Connection = Connection.__new__(Connection)
    mock_connection.parameter_statuses = deque(maxlen=100)
    mock_connection.parameter_statuses.append((b"show_discovery", 0))
    mock_cursor: Cursor = Cursor(mock_connection)
    mock_cursor.paramstyle = "mocked"
    mock_cursor._c = mock_connection
    mock_cursor.ps = {}
    mock_cursor._SHOW_SCHEMAS_Col_index = {"schema_name": 0}
    mock_cursor._SHOW_TABLES_Col_index = {"table_name": 0}
    mock_cursor._SHOW_CONSTRAINTS_PK_Col_index = {}

    mock_metadataServerProxy: MetadataServerProxy = MetadataServerProxy(mock_cursor)

    with patch("redshift_connector.metadataServerProxy.MetadataServerProxy.get_catalog_list", new_callable=PropertyMock()) as mock_get_catalog_list:
        mock_get_catalog_list.return_value = ["testCatalog"]

        spy = mocker.spy(mock_cursor, "execute")

        mock_metadataServerProxy.get_primary_keys("testCatalog", "testSchema", "testTable")

        assert spy.called
        assert spy.call_count == 1
        assert "SHOW CONSTRAINTS PRIMARY KEYS" in spy.call_args[0][0]

def test_get_imported_keys_server_proxy(mocker) -> None:
    # Mock the cursor
    mocker.patch("redshift_connector.Cursor.execute", return_value=None)
    mocker.patch("redshift_connector.Cursor.fetchall", return_value=None)

    # Setup mock cursor and connection
    mock_connection: Connection = Connection.__new__(Connection)
    mock_connection.parameter_statuses = deque(maxlen=100)
    mock_connection.parameter_statuses.append((b"show_discovery", 0))
    mock_cursor: Cursor = Cursor(mock_connection)
    mock_cursor.paramstyle = "mocked"
    mock_cursor._c = mock_connection
    mock_cursor.ps = {}
    mock_cursor._SHOW_SCHEMAS_Col_index = {"schema_name": 0}
    mock_cursor._SHOW_TABLES_Col_index = {"table_name": 0}
    mock_cursor._SHOW_CONSTRAINTS_FK_Col_index = {}

    mock_metadataServerProxy: MetadataServerProxy = MetadataServerProxy(mock_cursor)

    with patch("redshift_connector.metadataServerProxy.MetadataServerProxy.get_catalog_list", new_callable=PropertyMock()) as mock_get_catalog_list:
        mock_get_catalog_list.return_value = ["testCatalog"]

        spy = mocker.spy(mock_cursor, "execute")

        mock_metadataServerProxy.get_foreign_keys("testCatalog", "testSchema", "testTable", True)

        assert spy.called
        assert spy.call_count == 1
        assert "SHOW CONSTRAINTS FOREIGN KEYS" in spy.call_args[0][0]

def test_get_exported_keys_server_proxy(mocker) -> None:
    # Mock the cursor
    mocker.patch("redshift_connector.Cursor.execute", return_value=None)
    mocker.patch("redshift_connector.Cursor.fetchall", return_value=None)

    # Setup mock cursor and connection
    mock_connection: Connection = Connection.__new__(Connection)
    mock_connection.parameter_statuses = deque(maxlen=100)
    mock_connection.parameter_statuses.append((b"show_discovery", 0))
    mock_cursor: Cursor = Cursor(mock_connection)
    mock_cursor.paramstyle = "mocked"
    mock_cursor._c = mock_connection
    mock_cursor.ps = {}
    mock_cursor._SHOW_SCHEMAS_Col_index = {"schema_name": 0}
    mock_cursor._SHOW_TABLES_Col_index = {"table_name": 0}
    mock_cursor._SHOW_CONSTRAINTS_FK_Col_index = {}

    mock_metadataServerProxy: MetadataServerProxy = MetadataServerProxy(mock_cursor)

    with patch("redshift_connector.metadataServerProxy.MetadataServerProxy.get_catalog_list", new_callable=PropertyMock()) as mock_get_catalog_list:
        mock_get_catalog_list.return_value = ["testCatalog"]

        spy = mocker.spy(mock_cursor, "execute")

        mock_metadataServerProxy.get_foreign_keys("testCatalog", "testSchema", "testTable", True, False)

        assert spy.called
        assert spy.call_count == 1
        assert "SHOW CONSTRAINTS FOREIGN KEYS EXPORTED" in spy.call_args[0][0]

def test_get_best_row_identifier_server_proxy(mocker) -> None:
    # Mock the cursor
    mocker.patch("redshift_connector.Cursor.execute", return_value=None)
    mocker.patch("redshift_connector.Cursor.fetchall", return_value=["testColumn"])

    # Setup mock cursor and connection
    mock_connection: Connection = Connection.__new__(Connection)
    mock_connection.parameter_statuses = deque(maxlen=100)
    mock_connection.parameter_statuses.append((b"show_discovery", 0))
    mock_cursor: Cursor = Cursor(mock_connection)
    mock_cursor.paramstyle = "mocked"
    mock_cursor._c = mock_connection
    mock_cursor.ps = {}
    mock_cursor._SHOW_SCHEMAS_Col_index = {"schema_name": 0}
    mock_cursor._SHOW_TABLES_Col_index = {"table_name": 0}
    mock_cursor._SHOW_CONSTRAINTS_PK_Col_index = {"column_name": 0}
    mock_cursor._SHOW_COLUMNS_Col_index = {"column_name": 0}

    mock_metadataServerProxy: MetadataServerProxy = MetadataServerProxy(mock_cursor)

    with patch("redshift_connector.metadataServerProxy.MetadataServerProxy.get_catalog_list", new_callable=PropertyMock()) as mock_get_catalog_list:
        mock_get_catalog_list.return_value = ["testCatalog"]

        spy = mocker.spy(mock_cursor, "execute")

        mock_metadataServerProxy.get_best_row_identifier("testCatalog", "testSchema", "testTable")

        assert spy.called
        assert spy.call_count == 2
        assert "SHOW COLUMNS" in spy.call_args[0][0]

def test_get_column_privileges_server_proxy(mocker) -> None:
    # Mock the cursor
    mocker.patch("redshift_connector.Cursor.execute", return_value=None)
    mocker.patch("redshift_connector.Cursor.fetchall", return_value=["testColumn"])

    # Setup mock cursor and connection
    mock_connection: Connection = Connection.__new__(Connection)
    mock_connection.parameter_statuses = deque(maxlen=100)
    mock_connection.parameter_statuses.append((b"show_discovery", 0))
    mock_cursor: Cursor = Cursor(mock_connection)
    mock_cursor.paramstyle = "mocked"
    mock_cursor._c = mock_connection
    mock_cursor.ps = {}
    mock_cursor._SHOW_SCHEMAS_Col_index = {"schema_name": 0}
    mock_cursor._SHOW_TABLES_Col_index = {"table_name": 0}
    mock_cursor._SHOW_GRANTS_COLUMN_Col_index = {"column_name": 0, "privilege_type": 1}


    mock_metadataServerProxy: MetadataServerProxy = MetadataServerProxy(mock_cursor)

    with patch("redshift_connector.metadataServerProxy.MetadataServerProxy.get_catalog_list", new_callable=PropertyMock()) as mock_get_catalog_list:
        mock_get_catalog_list.return_value = ["testCatalog"]

        spy = mocker.spy(mock_cursor, "execute")

        mock_metadataServerProxy.get_column_privileges("testCatalog", "testSchema", "testTable", "testColumn")

        assert spy.called
        assert spy.call_count == 1
        assert "SHOW COLUMN GRANTS" in spy.call_args[0][0]

def test_get_table_privileges_server_proxy(mocker) -> None:
    # Mock the cursor
    mocker.patch("redshift_connector.Cursor.execute", return_value=None)
    mocker.patch("redshift_connector.Cursor.fetchall", return_value=(["SELECT"]))

    # Setup mock cursor and connection
    mock_connection: Connection = Connection.__new__(Connection)
    mock_connection.parameter_statuses = deque(maxlen=100)
    mock_connection.parameter_statuses.append((b"show_discovery", 0))
    mock_cursor: Cursor = Cursor(mock_connection)
    mock_cursor.paramstyle = "mocked"
    mock_cursor._c = mock_connection
    mock_cursor.ps = {}
    mock_cursor._SHOW_SCHEMAS_Col_index = {"schema_name": 0}
    mock_cursor._SHOW_TABLES_Col_index = {"table_name": 0}
    mock_cursor._SHOW_GRANTS_TABLE_Col_index = {"privilege_type": 0}


    mock_metadataServerProxy: MetadataServerProxy = MetadataServerProxy(mock_cursor)

    with patch("redshift_connector.metadataServerProxy.MetadataServerProxy.get_catalog_list", new_callable=PropertyMock()) as mock_get_catalog_list:
        mock_get_catalog_list.return_value = ["testCatalog"]

        def mock_get_schema_list(catalog, pattern):
            schema_list: typing.List = []
            schema_list.append("testSchema")
            return schema_list

        mocker.patch.object(
            mock_metadataServerProxy,
            'get_schema_list',
            side_effect=mock_get_schema_list
        )

        def mock_get_table_list(catalog, schema, table):
            table_list: typing.List = []
            table_list.append("testTable")
            return table_list

        mocker.patch.object(
            mock_metadataServerProxy,
            'get_table_list',
            side_effect=mock_get_table_list
        )

        spy = mocker.spy(mock_cursor, "execute")

        mock_metadataServerProxy.get_table_privileges("testCatalog", "testSchema", "testTable")

        assert spy.called
        assert spy.call_count == 1
        assert "SHOW GRANTS ON TABLE" in spy.call_args[0][0]

def test_get_procedures_server_proxy(mocker) -> None:
    # Mock the cursor
    mocker.patch("redshift_connector.Cursor.execute", return_value=None)
    mocker.patch("redshift_connector.Cursor.fetchall", return_value=None)

    # Setup mock cursor and connection
    mock_connection: Connection = Connection.__new__(Connection)
    mock_connection.parameter_statuses = deque(maxlen=100)
    mock_connection.parameter_statuses.append((b"show_discovery", 0))
    mock_cursor: Cursor = Cursor(mock_connection)
    mock_cursor.paramstyle = "mocked"
    mock_cursor._c = mock_connection
    mock_cursor.ps = {}
    mock_cursor._SHOW_SCHEMAS_Col_index = {"schema_name": 0}
    mock_cursor._SHOW_PROCEDURES_Col_index = {}


    mock_metadataServerProxy: MetadataServerProxy = MetadataServerProxy(mock_cursor)

    with patch("redshift_connector.metadataServerProxy.MetadataServerProxy.get_catalog_list", new_callable=PropertyMock()) as mock_get_catalog_list:
        mock_get_catalog_list.return_value = ["testCatalog"]

        def mock_get_schema_list(catalog, pattern):
            schema_list: typing.List = []
            schema_list.append("testSchema")
            return schema_list

        mocker.patch.object(
            mock_metadataServerProxy,
            'get_schema_list',
            side_effect=mock_get_schema_list
        )

        spy = mocker.spy(mock_cursor, "execute")

        mock_metadataServerProxy.get_procedures("testCatalog", "testSchema", "testProcedures")

        assert spy.called
        assert spy.call_count == 1
        assert "SHOW PROCEDURES" in spy.call_args[0][0]

def test_get_procedure_columns_server_proxy(mocker) -> None:
    # Mock the cursor
    mocker.patch("redshift_connector.Cursor.execute", return_value=None)
    mocker.patch("redshift_connector.Cursor.fetchall",
                 side_effect=[
                     [["testProcedure", "integer"]],
                     [["testCatalog", "testSchema", "testProcedure", "testColumn"]]
                 ])

    # Setup mock cursor and connection
    mock_connection: Connection = Connection.__new__(Connection)
    mock_connection.parameter_statuses = deque(maxlen=100)
    mock_connection.parameter_statuses.append((b"show_discovery", 0))
    mock_cursor: Cursor = Cursor(mock_connection)
    mock_cursor.paramstyle = "mocked"
    mock_cursor._c = mock_connection
    mock_cursor.ps = {}
    mock_cursor._SHOW_SCHEMAS_Col_index = {"schema_name": 0}
    mock_cursor._SHOW_PROCEDURES_Col_index = {"procedure_name": 0, "argument_list": 1}
    mock_cursor._SHOW_PARAMETERS_PRO_Col_index = {}


    mock_metadataServerProxy: MetadataServerProxy = MetadataServerProxy(mock_cursor)

    with patch("redshift_connector.metadataServerProxy.MetadataServerProxy.get_catalog_list", new_callable=PropertyMock()) as mock_get_catalog_list:
        mock_get_catalog_list.return_value = ["testCatalog"]

        def mock_get_schema_list(catalog, pattern):
            schema_list: typing.List = []
            schema_list.append("testSchema")
            return schema_list

        mocker.patch.object(
            mock_metadataServerProxy,
            'get_schema_list',
            side_effect=mock_get_schema_list
        )

        spy = mocker.spy(mock_cursor, "execute")

        mock_metadataServerProxy.get_procedure_columns("testCatalog", "testSchema", "testProcedures", "testColumns")

        assert spy.called
        assert spy.call_count == 2
        assert "SHOW PARAMETERS OF PROCEDURE" in spy.call_args[0][0]

def test_get_functions_server_proxy(mocker) -> None:
    # Mock the cursor
    mocker.patch("redshift_connector.Cursor.execute", return_value=None)
    mocker.patch("redshift_connector.Cursor.fetchall", return_value=None)

    # Setup mock cursor and connection
    mock_connection: Connection = Connection.__new__(Connection)
    mock_connection.parameter_statuses = deque(maxlen=100)
    mock_connection.parameter_statuses.append((b"show_discovery", 0))
    mock_cursor: Cursor = Cursor(mock_connection)
    mock_cursor.paramstyle = "mocked"
    mock_cursor._c = mock_connection
    mock_cursor.ps = {}
    mock_cursor._SHOW_SCHEMAS_Col_index = {"schema_name": 0}
    mock_cursor._SHOW_FUNCTIONS_Col_index = {}


    mock_metadataServerProxy: MetadataServerProxy = MetadataServerProxy(mock_cursor)

    with patch("redshift_connector.metadataServerProxy.MetadataServerProxy.get_catalog_list", new_callable=PropertyMock()) as mock_get_catalog_list:
        mock_get_catalog_list.return_value = ["testCatalog"]

        def mock_get_schema_list(catalog, pattern):
            schema_list: typing.List = []
            schema_list.append("testSchema")
            return schema_list

        mocker.patch.object(
            mock_metadataServerProxy,
            'get_schema_list',
            side_effect=mock_get_schema_list
        )

        spy = mocker.spy(mock_cursor, "execute")

        mock_metadataServerProxy.get_functions("testCatalog", "testSchema", "testFunctions")

        assert spy.called
        assert spy.call_count == 1
        assert "SHOW FUNCTIONS" in spy.call_args[0][0]

def test_get_function_columns_server_proxy(mocker) -> None:
    # Mock the cursor
    mocker.patch("redshift_connector.Cursor.execute", return_value=None)
    mocker.patch("redshift_connector.Cursor.fetchall",
                 side_effect=[
                     [["testFunction", "integer"]],
                     [["testCatalog", "testSchema", "testFunction", "testColumn"]]
                 ])

    # Setup mock cursor and connection
    mock_connection: Connection = Connection.__new__(Connection)
    mock_connection.parameter_statuses = deque(maxlen=100)
    mock_connection.parameter_statuses.append((b"show_discovery", 0))
    mock_cursor: Cursor = Cursor(mock_connection)
    mock_cursor.paramstyle = "mocked"
    mock_cursor._c = mock_connection
    mock_cursor.ps = {}
    mock_cursor._SHOW_SCHEMAS_Col_index = {"schema_name": 0}
    mock_cursor._SHOW_FUNCTIONS_Col_index = {"function_name": 0, "argument_list": 1}
    mock_cursor._SHOW_PARAMETERS_FUNC_Col_index = {}


    mock_metadataServerProxy: MetadataServerProxy = MetadataServerProxy(mock_cursor)

    with patch("redshift_connector.metadataServerProxy.MetadataServerProxy.get_catalog_list", new_callable=PropertyMock()) as mock_get_catalog_list:
        mock_get_catalog_list.return_value = ["testCatalog"]

        def mock_get_schema_list(catalog, pattern):
            schema_list: typing.List = []
            schema_list.append("testSchema")
            return schema_list

        mocker.patch.object(
            mock_metadataServerProxy,
            'get_schema_list',
            side_effect=mock_get_schema_list
        )

        spy = mocker.spy(mock_cursor, "execute")

        mock_metadataServerProxy.get_function_columns("testCatalog", "testSchema", "testFunction", "testColumn")

        assert spy.called
        assert spy.call_count == 2
        assert "SHOW PARAMETERS OF FUNCTION" in spy.call_args[0][0]
