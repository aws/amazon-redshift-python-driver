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

def test_get_tables_v5_server_proxy(mocker) -> None:
    mocker.patch("redshift_connector.Cursor.execute", return_value=None)
    mocker.patch("redshift_connector.Cursor.fetchall", return_value=None)

    mock_connection: Connection = Connection.__new__(Connection)
    mock_connection.parameter_statuses = deque(maxlen=100)
    mock_connection.parameter_statuses.append((b"show_discovery", 0))
    mock_cursor: Cursor = Cursor(mock_connection)
    mock_cursor.paramstyle = "mocked"
    mock_cursor._c = mock_connection
    mock_cursor.ps = {}
    mock_cursor._SHOW_TABLES_Col_index = {}

    mock_metadataServerProxy: MetadataServerProxy = MetadataServerProxy(mock_cursor)
    with patch("redshift_connector.metadataServerProxy.MetadataServerProxy.get_catalog_list", new_callable=PropertyMock()) as mock_get_catalog_list:

        mock_get_catalog_list.return_value = ["testCatalog"]

        spy = mocker.spy(mock_cursor, "execute")

        mock_metadataServerProxy.get_tables_v5("testCatalog", "testSchema", "testTable")

        assert spy.called
        assert spy.call_count == 1
        assert "SHOW TABLES FROM DATABASE %s" in spy.call_args[0][0]
        assert "SCHEMA_NAME LIKE %s" in spy.call_args[0][0]
        assert "TABLE_NAME LIKE %s" in spy.call_args[0][0]
        assert spy.call_args[0][1] == ["testCatalog", "testSchema", "testTable"]


def test_get_tables_v5_no_filters_server_proxy(mocker) -> None:
    mocker.patch("redshift_connector.Cursor.execute", return_value=None)
    mocker.patch("redshift_connector.Cursor.fetchall", return_value=None)

    mock_connection: Connection = Connection.__new__(Connection)
    mock_connection.parameter_statuses = deque(maxlen=100)
    mock_connection.parameter_statuses.append((b"show_discovery", 0))
    mock_cursor: Cursor = Cursor(mock_connection)
    mock_cursor.paramstyle = "mocked"
    mock_cursor._c = mock_connection
    mock_cursor.ps = {}
    mock_cursor._SHOW_TABLES_Col_index = {}

    mock_metadataServerProxy: MetadataServerProxy = MetadataServerProxy(mock_cursor)
    with patch("redshift_connector.metadataServerProxy.MetadataServerProxy.get_catalog_list", new_callable=PropertyMock()) as mock_get_catalog_list:

        mock_get_catalog_list.return_value = ["testCatalog"]

        spy = mocker.spy(mock_cursor, "execute")

        mock_metadataServerProxy.get_tables_v5("testCatalog", None, None)

        assert spy.called
        assert spy.call_count == 1
        assert "SHOW TABLES FROM DATABASE %s;" in spy.call_args[0][0]
        assert "WHERE" not in spy.call_args[0][0]
        assert spy.call_args[0][1] == ["testCatalog"]


def test_get_tables_v5_multi_catalog_server_proxy(mocker) -> None:
    mocker.patch("redshift_connector.Cursor.execute", return_value=None)
    mocker.patch("redshift_connector.Cursor.fetchall", return_value=None)

    mock_connection: Connection = Connection.__new__(Connection)
    mock_connection.parameter_statuses = deque(maxlen=100)
    mock_connection.parameter_statuses.append((b"show_discovery", 0))
    mock_cursor: Cursor = Cursor(mock_connection)
    mock_cursor.paramstyle = "mocked"
    mock_cursor._c = mock_connection
    mock_cursor.ps = {}
    mock_cursor._SHOW_TABLES_Col_index = {}

    mock_metadataServerProxy: MetadataServerProxy = MetadataServerProxy(mock_cursor)
    with patch("redshift_connector.metadataServerProxy.MetadataServerProxy.get_catalog_list", new_callable=PropertyMock()) as mock_get_catalog_list:

        mock_get_catalog_list.return_value = ["catalog1", "catalog2"]

        spy = mocker.spy(mock_cursor, "execute")

        mock_metadataServerProxy.get_tables_v5(None, "schema%", "table%", is_single_database_metadata=False)

        assert spy.called
        assert spy.call_count == 2
        # Verify each catalog is bound as the first parameter
        assert spy.call_args_list[0][0][1][0] == "catalog1"
        assert spy.call_args_list[1][0][1][0] == "catalog2"


def test_get_columns_v5_server_proxy(mocker) -> None:
    mocker.patch("redshift_connector.Cursor.execute", return_value=None)
    mocker.patch("redshift_connector.Cursor.fetchall", return_value=None)

    mock_connection: Connection = Connection.__new__(Connection)
    mock_connection.parameter_statuses = deque(maxlen=100)
    mock_connection.parameter_statuses.append((b"show_discovery", 0))
    mock_cursor: Cursor = Cursor(mock_connection)
    mock_cursor.paramstyle = "mocked"
    mock_cursor._c = mock_connection
    mock_cursor.ps = {}
    mock_cursor._SHOW_COLUMNS_Col_index = {}

    mock_metadataServerProxy: MetadataServerProxy = MetadataServerProxy(mock_cursor)
    with patch("redshift_connector.metadataServerProxy.MetadataServerProxy.get_catalog_list", new_callable=PropertyMock()) as mock_get_catalog_list:

        mock_get_catalog_list.return_value = ["testCatalog"]

        spy = mocker.spy(mock_cursor, "execute")

        mock_metadataServerProxy.get_columns_v5("testCatalog", "testSchema", "testTable", "testColumn")

        assert spy.called
        assert spy.call_count == 1
        assert spy.call_args[0][0] == "SHOW COLUMNS FROM DATABASE %s WHERE SCHEMA_NAME LIKE %s AND TABLE_NAME LIKE %s AND COLUMN_NAME LIKE %s;"
        assert spy.call_args[0][1] == ["testCatalog", "testSchema", "testTable", "testColumn"]


def test_get_columns_v5_no_filters_server_proxy(mocker) -> None:
    mocker.patch("redshift_connector.Cursor.execute", return_value=None)
    mocker.patch("redshift_connector.Cursor.fetchall", return_value=None)

    mock_connection: Connection = Connection.__new__(Connection)
    mock_connection.parameter_statuses = deque(maxlen=100)
    mock_connection.parameter_statuses.append((b"show_discovery", 0))
    mock_cursor: Cursor = Cursor(mock_connection)
    mock_cursor.paramstyle = "mocked"
    mock_cursor._c = mock_connection
    mock_cursor.ps = {}
    mock_cursor._SHOW_COLUMNS_Col_index = {}

    mock_metadataServerProxy: MetadataServerProxy = MetadataServerProxy(mock_cursor)
    with patch("redshift_connector.metadataServerProxy.MetadataServerProxy.get_catalog_list", new_callable=PropertyMock()) as mock_get_catalog_list:

        mock_get_catalog_list.return_value = ["testCatalog"]

        spy = mocker.spy(mock_cursor, "execute")

        mock_metadataServerProxy.get_columns_v5("testCatalog", None, None, None)

        assert spy.called
        assert spy.call_count == 1
        assert "SHOW COLUMNS FROM DATABASE %s;" in spy.call_args[0][0]
        assert "WHERE" not in spy.call_args[0][0]
        assert spy.call_args[0][1] == ["testCatalog"]


def test_get_tables_v5_match_all_pattern_dropped_server_proxy(mocker) -> None:
    # A match-all pattern ("%", "%%", ...) is semantically equivalent to no filter,
    # so the driver must omit the redundant WHERE ... LIKE clause.
    mocker.patch("redshift_connector.Cursor.execute", return_value=None)
    mocker.patch("redshift_connector.Cursor.fetchall", return_value=None)

    mock_connection: Connection = Connection.__new__(Connection)
    mock_connection.parameter_statuses = deque(maxlen=100)
    mock_connection.parameter_statuses.append((b"show_discovery", 0))
    mock_cursor: Cursor = Cursor(mock_connection)
    mock_cursor.paramstyle = "mocked"
    mock_cursor._c = mock_connection
    mock_cursor.ps = {}
    mock_cursor._SHOW_TABLES_Col_index = {}

    mock_metadataServerProxy: MetadataServerProxy = MetadataServerProxy(mock_cursor)
    with patch("redshift_connector.metadataServerProxy.MetadataServerProxy.get_catalog_list", new_callable=PropertyMock()) as mock_get_catalog_list:

        mock_get_catalog_list.return_value = ["testCatalog"]

        spy = mocker.spy(mock_cursor, "execute")

        mock_metadataServerProxy.get_tables_v5("testCatalog", "%", "%%")

        assert spy.called
        assert spy.call_count == 1
        assert "SHOW TABLES FROM DATABASE %s" in spy.call_args[0][0]
        assert "WHERE" not in spy.call_args[0][0]
        assert "LIKE" not in spy.call_args[0][0]
        assert spy.call_args[0][1] == ["testCatalog"]


def test_get_tables_v5_escaped_percent_kept_server_proxy(mocker) -> None:
    # An escaped literal percent means NOT match-all. "%%\%" matches anything ending in a
    # literal '%', so it is a real filter and the LIKE clause must be preserved.
    mocker.patch("redshift_connector.Cursor.execute", return_value=None)
    mocker.patch("redshift_connector.Cursor.fetchall", return_value=None)

    mock_connection: Connection = Connection.__new__(Connection)
    mock_connection.parameter_statuses = deque(maxlen=100)
    mock_connection.parameter_statuses.append((b"show_discovery", 0))
    mock_cursor: Cursor = Cursor(mock_connection)
    mock_cursor.paramstyle = "mocked"
    mock_cursor._c = mock_connection
    mock_cursor.ps = {}
    mock_cursor._SHOW_TABLES_Col_index = {}

    mock_metadataServerProxy: MetadataServerProxy = MetadataServerProxy(mock_cursor)
    with patch("redshift_connector.metadataServerProxy.MetadataServerProxy.get_catalog_list", new_callable=PropertyMock()) as mock_get_catalog_list:

        mock_get_catalog_list.return_value = ["testCatalog"]

        spy = mocker.spy(mock_cursor, "execute")

        # schema = "%%\%" (a real filter, not match-all); table = None
        mock_metadataServerProxy.get_tables_v5("testCatalog", "%%\\%", None)

        assert spy.called
        assert "SCHEMA_NAME LIKE %s" in spy.call_args[0][0]
        assert "TABLE_NAME LIKE" not in spy.call_args[0][0]
        assert spy.call_args[0][1] == ["testCatalog", "%%\\%"]


def test_get_table_privileges_v5_match_all_pattern_dropped_server_proxy(mocker) -> None:
    # Same match-all handling for getTablePrivileges: "%"/"%%" omit the WHERE ... LIKE clause.
    mocker.patch("redshift_connector.Cursor.execute", return_value=None)
    mocker.patch("redshift_connector.Cursor.fetchall", return_value=None)

    mock_connection: Connection = Connection.__new__(Connection)
    mock_connection.parameter_statuses = deque(maxlen=100)
    mock_connection.parameter_statuses.append((b"show_discovery", 0))
    mock_cursor: Cursor = Cursor(mock_connection)
    mock_cursor.paramstyle = "mocked"
    mock_cursor._c = mock_connection
    mock_cursor.ps = {}
    mock_cursor._SHOW_GRANTS_TABLE_Col_index = {}

    mock_metadataServerProxy: MetadataServerProxy = MetadataServerProxy(mock_cursor)
    with patch("redshift_connector.metadataServerProxy.MetadataServerProxy.get_catalog_list", new_callable=PropertyMock()) as mock_get_catalog_list:

        mock_get_catalog_list.return_value = ["testCatalog"]

        spy = mocker.spy(mock_cursor, "execute")

        mock_metadataServerProxy.get_table_privileges_v5("testCatalog", "%", "%%")

        assert spy.called
        assert spy.call_count == 1
        assert "SHOW GRANTS ON TABLES FROM DATABASE %s" in spy.call_args[0][0]
        assert "WHERE" not in spy.call_args[0][0]
        assert "LIKE" not in spy.call_args[0][0]
        assert spy.call_args[0][1] == ["testCatalog"]


def test_get_columns_v5_match_all_pattern_dropped_server_proxy(mocker) -> None:
    # Mixed: a real schema prefix is kept, but match-all table/column patterns are dropped.
    mocker.patch("redshift_connector.Cursor.execute", return_value=None)
    mocker.patch("redshift_connector.Cursor.fetchall", return_value=None)

    mock_connection: Connection = Connection.__new__(Connection)
    mock_connection.parameter_statuses = deque(maxlen=100)
    mock_connection.parameter_statuses.append((b"show_discovery", 0))
    mock_cursor: Cursor = Cursor(mock_connection)
    mock_cursor.paramstyle = "mocked"
    mock_cursor._c = mock_connection
    mock_cursor.ps = {}
    mock_cursor._SHOW_COLUMNS_Col_index = {}

    mock_metadataServerProxy: MetadataServerProxy = MetadataServerProxy(mock_cursor)
    with patch("redshift_connector.metadataServerProxy.MetadataServerProxy.get_catalog_list", new_callable=PropertyMock()) as mock_get_catalog_list:

        mock_get_catalog_list.return_value = ["testCatalog"]

        spy = mocker.spy(mock_cursor, "execute")

        mock_metadataServerProxy.get_columns_v5("testCatalog", "testSchema", "%", "%")

        assert spy.called
        assert spy.call_count == 1
        assert spy.call_args[0][0] == "SHOW COLUMNS FROM DATABASE %s WHERE SCHEMA_NAME LIKE %s;"
        assert spy.call_args[0][1] == ["testCatalog", "testSchema"]


def test_get_columns_v5_multi_catalog_server_proxy(mocker) -> None:
    mocker.patch("redshift_connector.Cursor.execute", return_value=None)
    mocker.patch("redshift_connector.Cursor.fetchall", return_value=None)

    mock_connection: Connection = Connection.__new__(Connection)
    mock_connection.parameter_statuses = deque(maxlen=100)
    mock_connection.parameter_statuses.append((b"show_discovery", 0))
    mock_cursor: Cursor = Cursor(mock_connection)
    mock_cursor.paramstyle = "mocked"
    mock_cursor._c = mock_connection
    mock_cursor.ps = {}
    mock_cursor._SHOW_COLUMNS_Col_index = {}

    mock_metadataServerProxy: MetadataServerProxy = MetadataServerProxy(mock_cursor)
    with patch("redshift_connector.metadataServerProxy.MetadataServerProxy.get_catalog_list", new_callable=PropertyMock()) as mock_get_catalog_list:

        mock_get_catalog_list.return_value = ["catalog1", "catalog2"]

        spy = mocker.spy(mock_cursor, "execute")

        mock_metadataServerProxy.get_columns_v5(None, "schema%", "table%", None, is_single_database_metadata=False)

        assert spy.called
        assert spy.call_count == 2
        # Verify each catalog is bound with the correct params
        assert spy.call_args_list[0][0][1] == ["catalog1", "schema%", "table%"]
        assert spy.call_args_list[1][0][1] == ["catalog2", "schema%", "table%"]


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

def test_get_table_privileges_v5_server_proxy(mocker) -> None:
    mocker.patch("redshift_connector.Cursor.execute", return_value=None)
    mocker.patch("redshift_connector.Cursor.fetchall", return_value=None)

    mock_connection: Connection = Connection.__new__(Connection)
    mock_connection.parameter_statuses = deque(maxlen=100)
    mock_connection.parameter_statuses.append((b"show_discovery", 0))
    mock_cursor: Cursor = Cursor(mock_connection)
    mock_cursor.paramstyle = "mocked"
    mock_cursor._c = mock_connection
    mock_cursor.ps = {}
    mock_cursor._SHOW_GRANTS_TABLE_Col_index = {}

    mock_metadataServerProxy: MetadataServerProxy = MetadataServerProxy(mock_cursor)
    with patch("redshift_connector.metadataServerProxy.MetadataServerProxy.get_catalog_list", new_callable=PropertyMock()) as mock_get_catalog_list:

        mock_get_catalog_list.return_value = ["testCatalog"]

        spy = mocker.spy(mock_cursor, "execute")

        mock_metadataServerProxy.get_table_privileges_v5("testCatalog", "testSchema", "testTable")

        assert spy.called
        assert spy.call_count == 1
        assert spy.call_args[0][0] == "SHOW GRANTS ON TABLES FROM DATABASE %s WHERE SCHEMA_NAME LIKE %s AND TABLE_NAME LIKE %s;"
        assert spy.call_args[0][1] == ["testCatalog", "testSchema", "testTable"]


def test_get_table_privileges_v5_no_filters_server_proxy(mocker) -> None:
    mocker.patch("redshift_connector.Cursor.execute", return_value=None)
    mocker.patch("redshift_connector.Cursor.fetchall", return_value=None)

    mock_connection: Connection = Connection.__new__(Connection)
    mock_connection.parameter_statuses = deque(maxlen=100)
    mock_connection.parameter_statuses.append((b"show_discovery", 0))
    mock_cursor: Cursor = Cursor(mock_connection)
    mock_cursor.paramstyle = "mocked"
    mock_cursor._c = mock_connection
    mock_cursor.ps = {}
    mock_cursor._SHOW_GRANTS_TABLE_Col_index = {}

    mock_metadataServerProxy: MetadataServerProxy = MetadataServerProxy(mock_cursor)
    with patch("redshift_connector.metadataServerProxy.MetadataServerProxy.get_catalog_list", new_callable=PropertyMock()) as mock_get_catalog_list:

        mock_get_catalog_list.return_value = ["testCatalog"]

        spy = mocker.spy(mock_cursor, "execute")

        mock_metadataServerProxy.get_table_privileges_v5("testCatalog", None, None)

        assert spy.called
        assert spy.call_count == 1
        assert spy.call_args[0][0] == "SHOW GRANTS ON TABLES FROM DATABASE %s;"
        assert spy.call_args[0][1] == ["testCatalog"]


def test_get_table_privileges_v5_multi_catalog_server_proxy(mocker) -> None:
    mocker.patch("redshift_connector.Cursor.execute", return_value=None)
    mocker.patch("redshift_connector.Cursor.fetchall", return_value=None)

    mock_connection: Connection = Connection.__new__(Connection)
    mock_connection.parameter_statuses = deque(maxlen=100)
    mock_connection.parameter_statuses.append((b"show_discovery", 0))
    mock_cursor: Cursor = Cursor(mock_connection)
    mock_cursor.paramstyle = "mocked"
    mock_cursor._c = mock_connection
    mock_cursor.ps = {}
    mock_cursor._SHOW_GRANTS_TABLE_Col_index = {}

    mock_metadataServerProxy: MetadataServerProxy = MetadataServerProxy(mock_cursor)
    with patch("redshift_connector.metadataServerProxy.MetadataServerProxy.get_catalog_list", new_callable=PropertyMock()) as mock_get_catalog_list:

        mock_get_catalog_list.return_value = ["catalog1", "catalog2"]

        spy = mocker.spy(mock_cursor, "execute")

        mock_metadataServerProxy.get_table_privileges_v5(None, "schema%", "table%", is_single_database_metadata=False)

        assert spy.called
        assert spy.call_count == 2
        assert spy.call_args_list[0][0][1] == ["catalog1", "schema%", "table%"]
        assert spy.call_args_list[1][0][1] == ["catalog2", "schema%", "table%"]


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


def test_get_table_privileges_v5_dispatch(mocker) -> None:
    """Verify cursor dispatches to get_table_privileges_v5 when show_discovery >= 5,
    and to get_table_privileges otherwise."""
    from redshift_connector.metadataAPIPostProcessor import MetadataAPIPostProcessor

    mock_connection: Connection = Connection.__new__(Connection)
    mock_connection.parameter_statuses = deque(maxlen=100)
    mock_connection.parameter_statuses.append((b"show_discovery", b"5"))
    mock_cursor: Cursor = Cursor.__new__(Cursor)
    mock_cursor.paramstyle = "mocked"
    mock_cursor._c = mock_connection
    mock_cursor.ps = {}
    mock_cursor._MIN_SHOW_DISCOVERY_VERSION_V4 = 4
    mock_cursor._MIN_SHOW_DISCOVERY_VERSION_V5 = 5

    mock_proxy = MagicMock()
    mock_proxy.get_table_privileges_v5.return_value = []
    mock_proxy.get_table_privileges.return_value = []
    # Server sent a usable token (or none at all), so the batch path is available.
    mock_proxy.has_malformed_driver_token.return_value = False
    mock_cursor._metadataServerProxy = mock_proxy

    mock_post_processor = MagicMock()
    mock_post_processor.get_table_privileges_post_processing.return_value = ()
    mock_cursor._metadataAPIPostProcessor = mock_post_processor

    with patch("redshift_connector.Connection.is_single_database_metadata", new_callable=PropertyMock()) as mock_single_db:
        mock_single_db.__get__ = Mock(return_value=True)
        mock_cursor.get_table_privileges("catalog", "schema", "table")

    # V5 path should be chosen
    mock_proxy.get_table_privileges_v5.assert_called_once()
    mock_proxy.get_table_privileges.assert_not_called()


def test_get_table_privileges_v4_dispatch(mocker) -> None:
    """Verify cursor dispatches to get_table_privileges (V4) when show_discovery < 5."""
    from redshift_connector.metadataAPIPostProcessor import MetadataAPIPostProcessor

    mock_connection: Connection = Connection.__new__(Connection)
    mock_connection.parameter_statuses = deque(maxlen=100)
    mock_connection.parameter_statuses.append((b"show_discovery", b"4"))
    mock_cursor: Cursor = Cursor.__new__(Cursor)
    mock_cursor.paramstyle = "mocked"
    mock_cursor._c = mock_connection
    mock_cursor.ps = {}
    mock_cursor._MIN_SHOW_DISCOVERY_VERSION_V4 = 4
    mock_cursor._MIN_SHOW_DISCOVERY_VERSION_V5 = 5

    mock_proxy = MagicMock()
    mock_proxy.get_table_privileges_v5.return_value = []
    mock_proxy.get_table_privileges.return_value = []
    mock_cursor._metadataServerProxy = mock_proxy

    mock_post_processor = MagicMock()
    mock_post_processor.get_table_privileges_post_processing.return_value = ()
    mock_cursor._metadataAPIPostProcessor = mock_post_processor

    with patch("redshift_connector.Connection.is_single_database_metadata", new_callable=PropertyMock()) as mock_single_db:
        mock_single_db.__get__ = Mock(return_value=True)
        mock_cursor.get_table_privileges("catalog", "schema", "table")

    # V4 path should be chosen
    mock_proxy.get_table_privileges.assert_called_once()
    mock_proxy.get_table_privileges_v5.assert_not_called()


def _capture_batch_show_sql_for_token(mocker, driver_token: typing.Optional[bytes], schema_pattern=None) -> str:
    """
    Runs get_tables_v5 with the given driver_token reported by the server and returns the SQL
    passed to cursor.execute. Pass driver_token=None to omit the parameter status entirely.
    """
    mocker.patch("redshift_connector.Cursor.execute", return_value=None)
    mocker.patch("redshift_connector.Cursor.fetchall", return_value=None)

    mock_connection: Connection = Connection.__new__(Connection)
    mock_connection.parameter_statuses = deque(maxlen=100)
    mock_connection.parameter_statuses.append((b"show_discovery", b"5"))
    if driver_token is not None:
        mock_connection.parameter_statuses.append((b"driver_token", driver_token))
    mock_cursor: Cursor = Cursor(mock_connection)
    mock_cursor.paramstyle = "mocked"
    mock_cursor._c = mock_connection
    mock_cursor.ps = {}
    mock_cursor._SHOW_TABLES_Col_index = {}

    mock_metadataServerProxy: MetadataServerProxy = MetadataServerProxy(mock_cursor)
    with patch("redshift_connector.metadataServerProxy.MetadataServerProxy.get_catalog_list", new_callable=PropertyMock()) as mock_get_catalog_list:

        mock_get_catalog_list.return_value = ["testCatalog"]

        spy = mocker.spy(mock_cursor, "execute")

        mock_metadataServerProxy.get_tables_v5("testCatalog", schema_pattern, None)

        assert spy.called
        return spy.call_args[0][0]


def _proxy_for_token(driver_token: typing.Optional[bytes]) -> MetadataServerProxy:
    """Builds a MetadataServerProxy whose connection reports the given driver_token."""
    mock_connection: Connection = Connection.__new__(Connection)
    mock_connection.parameter_statuses = deque(maxlen=100)
    mock_connection.parameter_statuses.append((b"show_discovery", b"5"))
    if driver_token is not None:
        mock_connection.parameter_statuses.append((b"driver_token", driver_token))
    mock_cursor: Cursor = Cursor(mock_connection)
    mock_cursor._c = mock_connection
    return MetadataServerProxy(mock_cursor)


# A server that sends no driver token is not gating batch SHOW, so the batch path stays available and
# only the DRIVER_TOKEN clause is dropped. Distinguishing "absent" from "malformed" here is what lets
# the batch path keep working unchanged if the server later stops issuing tokens.
@pytest.mark.parametrize("driver_token", [None, b""])
def test_has_malformed_driver_token_false_when_token_absent(driver_token) -> None:
    assert _proxy_for_token(driver_token).has_malformed_driver_token() is False


def test_has_malformed_driver_token_false_when_token_valid() -> None:
    assert _proxy_for_token(b"aaaaaaaa-bbbb-7ccc-8ddd-eeeeeeeeeeea").has_malformed_driver_token() is False


@pytest.mark.parametrize(
    "driver_token",
    [
        b"not-a-uuid",
        b"   ",
        b"aaaaaaaa-bbbb-7ccc-8ddd-eeeeeeeeeee",
        b"aaaaaaaa-bbbb-7ccc-8ddd-eeeeeeeeeeeaa",
        b"aaaaaaaa-bbbb-7ccc-8ddd-eeeeeeeeeeea' OR '1'='1",
        b"aaaaaaaa-bbbb-7ccc-8ddd-eeeeeeeeeeea\n",
        b"AAAAAAAA-BBBB-7CCC-8DDD-EEEEEEEEEEEA",
    ],
)
def test_has_malformed_driver_token_true_when_token_present_but_invalid(driver_token) -> None:
    assert _proxy_for_token(driver_token).has_malformed_driver_token() is True


def test_build_batch_show_sql_includes_driver_token(mocker) -> None:
    """Verify DRIVER_TOKEN is included in batch SHOW SQL when available."""
    sql = _capture_batch_show_sql_for_token(mocker, b"aaaaaaaa-bbbb-7ccc-8ddd-eeeeeeeeeeea", schema_pattern="testSchema")

    assert "DRIVER_TOKEN 'aaaaaaaa-bbbb-7ccc-8ddd-eeeeeeeeeeea'" in sql
    assert sql.index("DRIVER_TOKEN") < sql.index("WHERE")


def test_build_batch_show_sql_omits_driver_token_when_absent(mocker) -> None:
    """Verify DRIVER_TOKEN is omitted when not in parameter statuses."""
    sql = _capture_batch_show_sql_for_token(mocker, None)

    assert "DRIVER_TOKEN" not in sql


# The driver_token arrives over the wire in a ParameterStatus message and is embedded as a SQL
# string literal, because the server grammar does not accept a bind parameter in that position.
# The UUID format check is therefore the control that keeps the token from escaping the literal,
# so every one of these values must be dropped rather than concatenated. Covers the absent cases
# (parameter status missing, and empty) alongside the malformed ones, since all of them must leave
# the clause off.
@pytest.mark.parametrize(
    "malformed_token",
    [
        # Parameter status absent entirely
        None,
        # Injection attempts that break out of the quoted literal
        b"invalid'token;DROP TABLE x--",
        b"' OR '1'='1",
        b"aaaaaaaa-bbbb-7ccc-8ddd-eeeeeeeeeeea' OR '1'='1",
        b"aaaaaaaa-bbbb-7ccc-8ddd-eeeeeeeeeeea'; DROP TABLE x; --",
        b"aaaaaaaa-bbbb-7ccc-8ddd-eeeeeeeeeeea''",
        # Trailing/leading content: verifies the pattern is anchored at both ends
        b"aaaaaaaa-bbbb-7ccc-8ddd-eeeeeeeeeeeexx",
        b"xxaaaaaaaa-bbbb-7ccc-8ddd-eeeeeeeeeeee",
        b"aaaaaaaa-bbbb-7ccc-8ddd-eeeeeeeeeeea aaaaaaaa-bbbb-7ccc-8ddd-eeeeeeeeeeea",
        # Newline-separated: guards against a multiline-permissive matcher
        b"aaaaaaaa-bbbb-7ccc-8ddd-eeeeeeeeeeea\n' OR '1'='1",
        # Trailing newline only: "$" matches before a final newline, so this requires fullmatch()
        b"aaaaaaaa-bbbb-7ccc-8ddd-eeeeeeeeeeea\n",
        # Structurally wrong: length, grouping, and character-set violations
        b"aaaaaaaa-bbbb-7ccc-8ddd-eeeeeeeeeee",
        b"aaaaaaaa-bbbb-7ccc-8ddd-eeeeeeeeeeeef",
        b"aaaaaaaabbbb7ccc8dddeeeeeeeeeeee",
        b"aaaaaaaa_bbbb_7ccc_8ddd_eeeeeeeeeeee",
        b"aaaaaaaa-bbbb-7ccc-8ddd-eeeeeeeeeeZZ",
        b"AAAAAAAA-BBBB-7CCC-8DDD-EEEEEEEEEEEE",
        b"",
        b"   ",
    ],
)
def test_build_batch_show_sql_rejects_malformed_token(mocker, malformed_token) -> None:
    """Verify a token that is not a well-formed UUID is dropped instead of being embedded."""
    sql = _capture_batch_show_sql_for_token(mocker, malformed_token)

    # An unusable token is dropped entirely — no clause, and no fragment of the value leaks in.
    assert "DRIVER_TOKEN" not in sql, "Unusable token must not produce a DRIVER_TOKEN clause. SQL was: " + sql
    assert (
        "SHOW TABLES FROM DATABASE" in sql
    ), "Rejecting the token must not prevent the batch SHOW from being issued. SQL was: " + sql


def test_build_batch_show_sql_rejected_token_leaves_balanced_quotes(mocker) -> None:
    """Verify a rejected token leaves no stray quote that would corrupt the rest of the statement."""
    sql = _capture_batch_show_sql_for_token(mocker, b"aaaaaaaa-bbbb-7ccc-8ddd-eeeeeeeeeeea'; DROP TABLE x; --")

    assert "DROP TABLE" not in sql
    assert sql.count("'") == 0, "Rejected token must leave no quote characters in the SQL. SQL was: " + sql


def test_build_batch_show_sql_accepts_server_token_format(mocker) -> None:
    """Verify a token shaped like the UUID v7 the server sends is accepted."""
    # Dummy value with the UUID v7 version nibble (7) and variant nibble (8).
    sql = _capture_batch_show_sql_for_token(mocker, b"11111111-2222-7333-8444-555555555555")

    assert "DRIVER_TOKEN '11111111-2222-7333-8444-555555555555'" in sql


def test_redact_driver_token_masks_value() -> None:
    """Verify redactDriverToken masks the token value."""
    sql = "SHOW TABLES FROM DATABASE %s DRIVER_TOKEN 'dummy-token-value' WHERE SCHEMA_NAME LIKE %s;"
    result = MetadataServerProxy._redact_driver_token(sql)
    assert "dummy-token-value" not in result
    assert "DRIVER_TOKEN '[REDACTED]'" in result
    assert "WHERE SCHEMA_NAME LIKE %s;" in result


def test_redact_driver_token_no_clause_unchanged() -> None:
    """Verify SQL without DRIVER_TOKEN is returned unchanged."""
    sql = "SHOW COLUMNS FROM DATABASE %s WHERE SCHEMA_NAME LIKE %s;"
    assert MetadataServerProxy._redact_driver_token(sql) == sql


def test_redact_driver_token_handles_escaped_quotes() -> None:
    """Verify redaction handles doubled quotes inside token."""
    sql = "SHOW TABLES FROM DATABASE %s DRIVER_TOKEN 'a''b' WHERE X LIKE %s;"
    result = MetadataServerProxy._redact_driver_token(sql)
    assert "DRIVER_TOKEN '[REDACTED]'" in result
    assert "a''b" not in result


def _cursor_with_malformed_token_proxy(api: str) -> typing.Tuple[Cursor, MagicMock]:
    """
    Builds a cursor reporting show_discovery=5 whose proxy reports a malformed driver token, so the
    batch path must not be chosen.
    """
    mock_connection: Connection = Connection.__new__(Connection)
    mock_connection.parameter_statuses = deque(maxlen=100)
    mock_connection.parameter_statuses.append((b"show_discovery", b"5"))
    mock_cursor: Cursor = Cursor.__new__(Cursor)
    mock_cursor.paramstyle = "mocked"
    mock_cursor._c = mock_connection
    mock_cursor.ps = {}
    mock_cursor._MIN_SHOW_DISCOVERY_VERSION_V4 = 4
    mock_cursor._MIN_SHOW_DISCOVERY_VERSION_V5 = 5

    mock_proxy = MagicMock()
    getattr(mock_proxy, api).return_value = []
    getattr(mock_proxy, api + "_v5").return_value = []
    mock_proxy.has_malformed_driver_token.return_value = True
    mock_cursor._metadataServerProxy = mock_proxy

    mock_post_processor = MagicMock()
    getattr(mock_post_processor, api + "_post_processing").return_value = ()
    mock_cursor._metadataAPIPostProcessor = mock_post_processor

    return mock_cursor, mock_proxy


# A server that sends a driver token gates batch SHOW on it and rejects the command when the
# DRIVER_TOKEN clause carries a token it does not recognize (SQLState 42501). When the token we received
# fails validation we cannot send a usable clause, so rather than surfacing that error the driver falls
# back to the loop-based SHOW commands, which still return correct metadata. A server that sends no token
# at all is not gating batch SHOW; see test_has_malformed_driver_token_false_when_token_absent.
def test_get_tables_falls_back_to_loop_when_malformed_driver_token() -> None:
    mock_cursor, mock_proxy = _cursor_with_malformed_token_proxy("get_tables")

    with patch("redshift_connector.Connection.is_single_database_metadata", new_callable=PropertyMock()) as mock_single_db:
        mock_single_db.__get__ = Mock(return_value=True)
        mock_cursor.get_tables("catalog", "schema", "table")

    mock_proxy.get_tables.assert_called_once()
    mock_proxy.get_tables_v5.assert_not_called()


def test_get_columns_falls_back_to_loop_when_malformed_driver_token() -> None:
    mock_cursor, mock_proxy = _cursor_with_malformed_token_proxy("get_columns")

    with patch("redshift_connector.Connection.is_single_database_metadata", new_callable=PropertyMock()) as mock_single_db:
        mock_single_db.__get__ = Mock(return_value=True)
        mock_cursor.get_columns("catalog", "schema", "table", "column")

    mock_proxy.get_columns.assert_called_once()
    mock_proxy.get_columns_v5.assert_not_called()


def test_get_table_privileges_falls_back_to_loop_when_malformed_driver_token() -> None:
    mock_cursor, mock_proxy = _cursor_with_malformed_token_proxy("get_table_privileges")

    with patch("redshift_connector.Connection.is_single_database_metadata", new_callable=PropertyMock()) as mock_single_db:
        mock_single_db.__get__ = Mock(return_value=True)
        mock_cursor.get_table_privileges("catalog", "schema", "table")

    mock_proxy.get_table_privileges.assert_called_once()
    mock_proxy.get_table_privileges_v5.assert_not_called()
