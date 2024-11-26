import datetime
import typing
from collections import deque
from io import StringIO
from math import ceil

from test.utils import pandas_only
from unittest.mock import Mock, PropertyMock, mock_open, patch, MagicMock

import pytest  # type: ignore

from redshift_connector import Connection, Cursor, DataError, InterfaceError

IS_SINGLE_DATABASE_METADATA_TOGGLE: typing.List[bool] = [True, False]


description_warn_response_data: typing.List[typing.Tuple[bytes, str]] = [
    (b"ab\xffcd", "failed to decode column name"),
]

def mock_sanitizer(input: str = "") -> str:
    return str(input)

@pytest.mark.parametrize("_input", description_warn_response_data)
def test_get_description_warns_user(_input) -> None:
    data, exp_warning_msg = _input
    mock_cursor: Cursor = Cursor.__new__(Cursor)
    mock_cursor.__setattr__("ps", {"row_desc": [{"type_oid": 1043, "label": data, "column_name": b"c1"}]})
    with pytest.warns(UserWarning, match=exp_warning_msg):
        mock_cursor.description


fetch_df_warn_response_data: typing.List[typing.Tuple[typing.Optional[typing.List[bytes]], str]] = [
    (None, "No row description was found. pandas dataframe will be missing column labels."),
]


@pandas_only
@pytest.mark.parametrize("_input", fetch_df_warn_response_data)
def test_fetch_dataframe_warns_user(_input, mocker) -> None:
    data, exp_warning_msg = _input
    mock_cursor: Cursor = Cursor.__new__(Cursor)
    mocker.patch("redshift_connector.Cursor._getDescription", return_value=[data])
    mocker.patch("redshift_connector.Cursor.__next__", return_value=["blah"])
    with pytest.warns(UserWarning, match=exp_warning_msg):
        mock_cursor.fetch_dataframe(1)


@pandas_only
def test_fetch_dataframe_no_results(mocker) -> None:
    mock_cursor: Cursor = Cursor.__new__(Cursor)
    mocker.patch("redshift_connector.Cursor._getDescription", return_value=["test"])
    mocker.patch("redshift_connector.Cursor.__next__", side_effect=StopIteration("mocked end"))

    assert mock_cursor.fetch_dataframe(1).size == 0


@pandas_only
def test_fetch_dataframe_respects_case_sensitivity(mocker) -> None:
    import pandas as pd

    mock_cursor: Cursor = Cursor.__new__(Cursor)
    mocker.patch(
        "redshift_connector.Cursor._getDescription",
        return_value=[("C", 23, None, None, None, None, None)],
    )
    mocker.patch("redshift_connector.Cursor.__next__", side_effect=StopIteration("mocked end"))

    df = mock_cursor.fetch_dataframe()

    assert df.size == 0
    assert df.columns.to_list() == ["C"]


def test_raw_connection_property_warns() -> None:
    mock_cursor: Cursor = Cursor.__new__(Cursor)
    mock_cursor._c = Connection.__new__(Connection)

    with pytest.warns(UserWarning, match="DB-API extension cursor.connection used"):
        mock_cursor.connection


def test_get_description_no_ps() -> None:
    mock_cursor: Cursor = Cursor.__new__(Cursor)
    mock_cursor.ps = None
    assert mock_cursor._getDescription() is None


def test_execute_no_connection_raises_interface_error() -> None:
    mock_cursor: Cursor = Cursor.__new__(Cursor)
    mock_cursor._c = None

    with pytest.raises(InterfaceError, match="Cursor closed"):
        mock_cursor.execute("blah")


get_procedure_arg_data: typing.List[typing.Tuple[typing.Optional[str], ...]] = [
    ("apples", "blueberries", "oranges"),
    (None, "laffytaffy", "gobstoppers"),
    ("chocolate", None, "coffee"),
    ("pumpkin", "spaghetti_squash", None),
    (None, "a%", None),
    (None, "_b_", None),
]


@pytest.mark.parametrize("_input", get_procedure_arg_data)
def test_get_procedures_considers_args(_input, mocker) -> None:
    catalog, schema_pattern, procedure_name_pattern = _input
    mocker.patch("redshift_connector.Cursor.execute", return_value=None)
    mocker.patch("redshift_connector.Cursor.fetchall", return_value=None)
    mocker.patch("redshift_connector.Connection.is_single_database_metadata", return_value=True)

    mock_cursor: Cursor = Cursor.__new__(Cursor)
    mock_connection: Connection = Connection.__new__(Connection)
    mock_cursor._c = mock_connection

    mock_cursor.paramstyle = "mocked_val"
    spy = mocker.spy(mock_cursor, "execute")

    mock_cursor.get_procedures(catalog, schema_pattern, procedure_name_pattern)
    assert spy.called
    assert spy.call_count == 1
    assert catalog not in spy.call_args[0][1]
    for arg in (schema_pattern, procedure_name_pattern):
        if arg is not None:
            assert arg in spy.call_args[0][1]


catalog_filter_conditions_data: typing.List[typing.Tuple[typing.Optional[str], bool, typing.Optional[str]]] = [
    ("apples", True, "oranges"),
    ("peanuts", False, "walnuts"),
    (None, True, "pecans"),
    (None, False, "pistachios"),
    ("blue", True, None),
    ("green", False, None),
    (None, True, None),
    (None, False, None),
]


@pytest.mark.parametrize("is_single_database_metadata_val", IS_SINGLE_DATABASE_METADATA_TOGGLE)
@pytest.mark.parametrize("_input", catalog_filter_conditions_data)
def test__get_catalog_filter_conditions_considers_args(_input, is_single_database_metadata_val) -> None:
    catalog, api_supported_only_for_connected_database, database_col_name = _input

    mock_cursor: Cursor = Cursor.__new__(Cursor)
    mock_connection: Connection = Connection.__new__(Connection)
    mock_cursor._c = mock_connection

    with patch(
        "redshift_connector.Connection.is_single_database_metadata", new_callable=PropertyMock()
    ) as mock_is_single_database_metadata:
        mock_is_single_database_metadata.__get__ = Mock(return_value=is_single_database_metadata_val)
        result: str = mock_cursor._get_catalog_filter_conditions(
            catalog, api_supported_only_for_connected_database, database_col_name
        )

    if catalog is not None:
        assert catalog in result
        if is_single_database_metadata_val or api_supported_only_for_connected_database:
            assert "current_database()" in result
            assert catalog in result
        elif database_col_name is None:
            assert "database_name" in result
        else:
            assert database_col_name in result
    else:
        assert result == ""


get_schemas_arg_data: typing.List[typing.Tuple[typing.Optional[str], ...]] = [
    ("lipbalm", "cherry"),
    ("lavender", "chamomille"),
    ("pumpkin", None),
    (None, "volcano"),
    (None, None),
]


@pytest.mark.parametrize("is_single_database_metadata_val", IS_SINGLE_DATABASE_METADATA_TOGGLE)
@pytest.mark.parametrize("_input", get_schemas_arg_data)
def test_get_schemas_considers_args(_input, is_single_database_metadata_val, mocker) -> None:
    catalog, schema_pattern = _input
    mocker.patch("redshift_connector.Cursor.execute", return_value=None)
    mocker.patch("redshift_connector.Cursor.fetchall", return_value=None)
    mocker.patch(
        "redshift_connector.Cursor.sanitize_parameter",
        side_effect=mock_sanitizer
    )

    mock_cursor: Cursor = Cursor.__new__(Cursor)
    mock_cursor.paramstyle = "mocked"
    mock_connection: Connection = Connection.__new__(Connection)
    mock_connection.parameter_statuses = deque(maxlen=100)
    mock_connection.parameter_statuses.append((b'show_discovery', b'0'))
    mock_cursor._c = mock_connection
    spy = mocker.spy(mock_cursor, "execute")

    with patch(
        "redshift_connector.Connection.is_single_database_metadata", new_callable=PropertyMock()
    ) as mock_is_single_database_metadata:
        mock_is_single_database_metadata.__get__ = Mock(return_value=is_single_database_metadata_val)
        mock_cursor.get_schemas(catalog, schema_pattern)

    assert spy.called
    assert spy.call_count == 1

    if schema_pattern is not None:  # should be in parameterized portion
        assert schema_pattern in spy.call_args[0][1]

    if catalog is not None:
        assert catalog in spy.call_args[0][0]


@pytest.mark.parametrize("is_single_database_metadata_val", IS_SINGLE_DATABASE_METADATA_TOGGLE)
def test_get_schemas_show_discovery(is_single_database_metadata_val, mocker) -> None:
    mocker.patch(
        "redshift_connector.Cursor.sanitize_parameter",
        return_value="mock"
    )

    mock_cursor: Cursor = Cursor.__new__(Cursor)
    mock_cursor.paramstyle = "mocked"

    mock_connection: Connection = Connection.__new__(Connection)
    mock_cursor._c = mock_connection

    mock_connection.parameter_statuses = deque(maxlen=100)
    mock_connection.parameter_statuses.append((b'show_discovery', b'1'))

    from redshift_connector.metadataServerAPIHelper import MetadataServerAPIHelper
    mock_metadataServerAPIHelper = mocker.patch.object(MetadataServerAPIHelper, 'get_schema_server_api')
    mock_metadataServerAPIHelper.return_value = "mock"
    mock_cursor._metadataServerAPIHelper = mock_metadataServerAPIHelper

    spy_metadataServerAPIHelper = mocker.spy(mock_cursor._metadataServerAPIHelper, "get_schema_server_api")

    from redshift_connector.metadataAPIPostProcessing import MetadataAPIPostProcessing
    mock_metadataAPIPostProcessing = mocker.patch.object(MetadataAPIPostProcessing, 'get_schema_post_processing')
    mock_metadataAPIPostProcessing.return_value = "mock"
    mock_cursor._metadataAPIPostProcessing = mock_metadataAPIPostProcessing

    spy_metadataAPIPostProcessing = mocker.spy(mock_cursor._metadataAPIPostProcessing, "get_schema_post_processing")

    with patch(
            "redshift_connector.Connection.is_single_database_metadata", new_callable=PropertyMock()
    ) as mock_is_single_database_metadata:
        mock_is_single_database_metadata.__get__ = Mock(return_value=is_single_database_metadata_val)
        mock_cursor.get_schemas("apples", "oranges")

    assert spy_metadataServerAPIHelper.called
    assert spy_metadataServerAPIHelper.call_count == 1

    assert spy_metadataAPIPostProcessing.called
    assert spy_metadataAPIPostProcessing.call_count == 1

@pytest.mark.parametrize("is_single_database_metadata_val", IS_SINGLE_DATABASE_METADATA_TOGGLE)
def test_get_catalogs_considers_args(is_single_database_metadata_val, mocker) -> None:
    mocker.patch("redshift_connector.Cursor.execute", return_value=None)
    mocker.patch("redshift_connector.Cursor.fetchall", return_value=None)

    mock_cursor: Cursor = Cursor.__new__(Cursor)
    mock_cursor.paramstyle = "mocked"
    mock_connection: Connection = Connection.__new__(Connection)
    mock_connection.parameter_statuses = deque(maxlen=100)
    mock_connection.parameter_statuses.append((b'show_discovery', b'0'))
    mock_cursor._c = mock_connection
    spy = mocker.spy(mock_cursor, "execute")

    with patch(
        "redshift_connector.Connection.is_single_database_metadata", new_callable=PropertyMock()
    ) as mock_is_single_database_metadata:
        mock_is_single_database_metadata.__get__ = Mock(return_value=is_single_database_metadata_val)
        mock_cursor.get_catalogs()

    assert spy.called
    assert spy.call_count == 1

    if is_single_database_metadata_val:
        assert "select current_database as TABLE_CAT FROM current_database()" in spy.call_args[0][0]
    else:
        assert (
            "SELECT CAST(database_name AS varchar(124)) AS TABLE_CAT FROM PG_CATALOG.SVV_REDSHIFT_DATABASES "
            in spy.call_args[0][0]
        )

@pytest.mark.parametrize("is_single_database_metadata_val", IS_SINGLE_DATABASE_METADATA_TOGGLE)
def test_get_catalogs_show_discovery(is_single_database_metadata_val, mocker) -> None:
    mocker.patch("redshift_connector.Cursor.execute", return_value=None)
    mocker.patch("redshift_connector.Cursor.fetchall", return_value=None)

    mock_cursor: Cursor = Cursor.__new__(Cursor)
    mock_cursor.paramstyle = "mocked"

    mock_connection: Connection = Connection.__new__(Connection)
    mock_cursor._c = mock_connection
    spy_execute = mocker.spy(mock_cursor, "execute")

    mock_connection.parameter_statuses = deque(maxlen=100)
    mock_connection.parameter_statuses.append((b'show_discovery', b'1'))

    from redshift_connector.metadataServerAPIHelper import MetadataServerAPIHelper
    mock_metadataServerAPIHelper = mocker.patch.object(MetadataServerAPIHelper, 'get_catalog_server_api')
    mock_metadataServerAPIHelper.return_value = "mock"
    mock_cursor._metadataServerAPIHelper = mock_metadataServerAPIHelper

    spy_metadataServerAPIHelper = mocker.spy(mock_cursor._metadataServerAPIHelper, "get_catalog_server_api")

    from redshift_connector.metadataAPIPostProcessing import MetadataAPIPostProcessing
    mock_metadataAPIPostProcessing = mocker.patch.object(MetadataAPIPostProcessing, 'get_catalog_post_processing')
    mock_metadataAPIPostProcessing.return_value = "mock"
    mock_cursor._metadataAPIPostProcessing = mock_metadataAPIPostProcessing

    spy_metadataAPIPostProcessing = mocker.spy(mock_cursor._metadataAPIPostProcessing, "get_catalog_post_processing")

    with patch(
            "redshift_connector.Connection.is_single_database_metadata", new_callable=PropertyMock()
    ) as mock_is_single_database_metadata:
        mock_is_single_database_metadata.__get__ = Mock(return_value=is_single_database_metadata_val)
        mock_cursor.get_catalogs()

    if is_single_database_metadata_val:
        assert spy_execute.called
        assert spy_execute.call_count == 1

        assert "select current_database as TABLE_CAT FROM current_database()" in spy_execute.call_args[0][0]
    else:
        assert spy_metadataServerAPIHelper.called
        assert spy_metadataServerAPIHelper.call_count == 1

        assert spy_metadataAPIPostProcessing.called
        assert spy_metadataAPIPostProcessing.call_count == 1


get_tables_arg_data: typing.List[typing.Tuple[typing.Optional[str], ...]] = [
    ("apples", "oranges", "peaches"),
    (None, "blocks", "legos"),
    ("trains", None, "planes"),
    ("lions", "tigers", None),
    (None, None, None),
]

# "NO_SCHEMA_UNIVERSAL_QUERY" is excluded, as that case is hit in __schema_pattern_match when schema_pattern is None


@pytest.mark.parametrize("schema_pattern_type", ["EXTERNAL_SCHEMA_QUERY", "LOCAL_SCHEMA_QUERY"])
@pytest.mark.parametrize("is_single_database_metadata_val", IS_SINGLE_DATABASE_METADATA_TOGGLE)
@pytest.mark.parametrize("_input", get_tables_arg_data)
def test_get_tables_considers_args(is_single_database_metadata_val, _input, schema_pattern_type, mocker) -> None:
    catalog, schema_pattern, table_name_pattern = _input
    mocker.patch("redshift_connector.Cursor.execute", return_value=None)
    # mock the return value from __schema_pattern_match as it's return value is used in get_tables()
    # the other potential call to this method in get_tables() result is simply returned, so at this time
    # it has no impact
    mocker.patch(
        "redshift_connector.Cursor.fetchall",
        return_value=None if schema_pattern_type == "EXTERNAL_SCHEMA_QUERY" else tuple("mock"),
    )
    mocker.patch(
        "redshift_connector.Cursor.sanitize_parameter",
        side_effect=mock_sanitizer
    )

    mock_cursor: Cursor = Cursor.__new__(Cursor)
    mock_cursor.paramstyle = "mocked"
    mock_connection: Connection = Connection.__new__(Connection)
    mock_connection.parameter_statuses = deque(maxlen=100)
    mock_connection.parameter_statuses.append((b'show_discovery', b'0'))
    mock_cursor._c = mock_connection
    spy = mocker.spy(mock_cursor, "execute")

    with patch(
        "redshift_connector.Connection.is_single_database_metadata", new_callable=PropertyMock()
    ) as mock_is_single_database_metadata:
        mock_is_single_database_metadata.__get__ = Mock(return_value=is_single_database_metadata_val)
        mock_cursor.get_tables(catalog, schema_pattern, table_name_pattern)

    assert spy.called

    if is_single_database_metadata_val:
        assert spy.call_count == 2  # call in __schema_pattern_match(), get_tables()
    else:
        assert spy.call_count == 1

    if catalog is not None:
        assert catalog in spy.call_args[0][0]

    for arg in (schema_pattern, table_name_pattern):
        if arg is not None:
            assert arg in spy.call_args[0][1]

@pytest.mark.parametrize("is_single_database_metadata_val", IS_SINGLE_DATABASE_METADATA_TOGGLE)
def test_get_tables_show_discovery(is_single_database_metadata_val,mocker) -> None:
    mocker.patch(
        "redshift_connector.Cursor.sanitize_parameter",
        return_value="mock"
    )

    mock_cursor: Cursor = Cursor.__new__(Cursor)
    mock_cursor.paramstyle = "mocked"

    mock_connection: Connection = Connection.__new__(Connection)
    mock_cursor._c = mock_connection

    mock_connection.parameter_statuses = deque(maxlen=100)
    mock_connection.parameter_statuses.append((b'show_discovery', b'1'))

    from redshift_connector.metadataServerAPIHelper import MetadataServerAPIHelper
    mock_metadataServerAPIHelper = mocker.patch.object(MetadataServerAPIHelper, 'get_table_server_api')
    mock_metadataServerAPIHelper.return_value = "mock"
    mock_cursor._metadataServerAPIHelper = mock_metadataServerAPIHelper

    spy_metadataServerAPIHelper = mocker.spy(mock_cursor._metadataServerAPIHelper, "get_table_server_api")

    from redshift_connector.metadataAPIPostProcessing import MetadataAPIPostProcessing
    mock_metadataAPIPostProcessing = mocker.patch.object(MetadataAPIPostProcessing,'get_table_post_processing')
    mock_metadataAPIPostProcessing.return_value = "mock"
    mock_cursor._metadataAPIPostProcessing = mock_metadataAPIPostProcessing

    spy_metadataAPIPostProcessing = mocker.spy(mock_cursor._metadataAPIPostProcessing, "get_table_post_processing")


    with patch(
        "redshift_connector.Connection.is_single_database_metadata", new_callable=PropertyMock()
    ) as mock_is_single_database_metadata:
        mock_is_single_database_metadata.__get__ = Mock(return_value=is_single_database_metadata_val)
        mock_cursor.get_tables("apples", "oranges", "peaches")


    assert spy_metadataServerAPIHelper.called
    assert spy_metadataServerAPIHelper.call_count == 1

    assert spy_metadataAPIPostProcessing.called
    assert spy_metadataAPIPostProcessing.call_count == 1


@pytest.mark.parametrize("is_single_database_metadata_val", IS_SINGLE_DATABASE_METADATA_TOGGLE)
def test_get_columns_show_discovery(is_single_database_metadata_val,mocker) -> None:
    mocker.patch(
        "redshift_connector.Cursor.sanitize_parameter",
        return_value="mock"
    )

    mock_cursor: Cursor = Cursor.__new__(Cursor)
    mock_cursor.paramstyle = "mocked"

    mock_connection: Connection = Connection.__new__(Connection)
    mock_cursor._c = mock_connection

    mock_connection.parameter_statuses = deque(maxlen=100)
    mock_connection.parameter_statuses.append((b'show_discovery', b'1'))

    from redshift_connector.metadataServerAPIHelper import MetadataServerAPIHelper
    mock_metadataServerAPIHelper = mocker.patch.object(MetadataServerAPIHelper, 'get_column_server_api')
    mock_metadataServerAPIHelper.return_value = "mock"
    mock_cursor._metadataServerAPIHelper = mock_metadataServerAPIHelper

    spy_metadataServerAPIHelper = mocker.spy(mock_cursor._metadataServerAPIHelper, "get_column_server_api")

    from redshift_connector.metadataAPIPostProcessing import MetadataAPIPostProcessing
    mock_metadataAPIPostProcessing = mocker.patch.object(MetadataAPIPostProcessing,'get_column_post_processing')
    mock_metadataAPIPostProcessing.return_value = "mock"
    mock_cursor._metadataAPIPostProcessing = mock_metadataAPIPostProcessing

    spy_metadataAPIPostProcessing = mocker.spy(mock_cursor._metadataAPIPostProcessing, "get_column_post_processing")


    with patch(
        "redshift_connector.Connection.is_single_database_metadata", new_callable=PropertyMock()
    ) as mock_is_single_database_metadata:
        mock_is_single_database_metadata.__get__ = Mock(return_value=is_single_database_metadata_val)
        mock_cursor.get_columns("apples", "oranges", "peaches","banana")


    assert spy_metadataServerAPIHelper.called
    assert spy_metadataServerAPIHelper.call_count == 1

    assert spy_metadataAPIPostProcessing.called
    assert spy_metadataAPIPostProcessing.call_count == 1

@pytest.mark.parametrize("input_str", ["user' OR '1'='1","test;","tes   t","tes\t"])
def test_sanitize_parameter_invalid(input_str) -> None:
    mock_cursor: Cursor = Cursor(None)

    with pytest.raises(Exception, match="Invalid parameter input:"):
        mock_cursor.sanitize_parameter(input_str)

def test_sanitize_parameter_valid() -> None:
    mock_cursor: Cursor = Cursor(None)

    assert mock_cursor.sanitize_parameter("test")


@pytest.mark.parametrize("indexes, names", [([1], []), ([], ["c1"])])
def test_insert_data_column_names_indexes_mismatch_raises(indexes, names, mocker) -> None:
    # mock fetchone to return "True" to ensure the table_name and column_name
    # validation steps pass
    mocker.patch("redshift_connector.Cursor.fetchone", return_value=[1])

    mock_cursor: Cursor = Cursor.__new__(Cursor)
    # mock out the connection
    mock_cursor._c = Mock()
    mock_cursor.paramstyle = "qmark"

    with pytest.raises(InterfaceError, match="Column names and parameter indexes must be the same length"):
        mock_cursor.insert_data_bulk(
            filename="test_file",
            table_name="test_table",
            parameter_indices=indexes,
            column_names=names,
            delimiter=",",
        )


insert_bulk_data = [
    (
        [0],
        ["col1"],
        ("INSERT INTO  test_table (col1) VALUES (%s), (%s), (%s);", ["1", "2", "-1"]),
    ),
    (
        [1],
        ["col2"],
        ("INSERT INTO  test_table (col2) VALUES (%s), (%s), (%s);", ["3", "5", "7"]),
    ),
    (
        [2],
        ["col3"],
        (
            "INSERT INTO  test_table (col3) VALUES (%s), (%s), (%s);",
            ["foo", "bar", "baz"],
        ),
    ),
    (
        [0, 1],
        ["col1", "col2"],
        (
            "INSERT INTO  test_table (col1, col2) VALUES (%s, %s), (%s, %s), (%s, %s);",
            ["1", "3", "2", "5", "-1", "7"],
        ),
    ),
    (
        [0, 2],
        ["col1", "col3"],
        (
            "INSERT INTO  test_table (col1, col3) VALUES (%s, %s), (%s, %s), (%s, %s);",
            ["1", "foo", "2", "bar", "-1", "baz"],
        ),
    ),
    (
        [1, 2],
        ["col2", "col3"],
        (
            "INSERT INTO  test_table (col2, col3) VALUES (%s, %s), (%s, %s), (%s, %s);",
            ["3", "foo", "5", "bar", "7", "baz"],
        ),
    ),
    (
        [0, 1, 2],
        ["col1", "col2", "col3"],
        (
            "INSERT INTO  test_table (col1, col2, col3) VALUES (%s, %s, %s), (%s, %s, %s), (%s, %s, %s);",
            ["1", "3", "foo", "2", "5", "bar", "-1", "7", "baz"],
        ),
    ),
]


@patch("builtins.open", new_callable=mock_open)
@pytest.mark.parametrize("indexes,names,exp_execute_args", insert_bulk_data)
def test_insert_data_column_stmt(mocked_csv, indexes, names, exp_execute_args, mocker) -> None:
    # mock fetchone to return "True" to ensure the table_name and column_name
    # validation steps pass
    mocker.patch("redshift_connector.Cursor.fetchone", return_value=[1])
    mock_cursor: Cursor = Cursor.__new__(Cursor)

    # spy on the execute method, so we can check value of sql_query
    spy = mocker.spy(mock_cursor, "execute")

    # mock out the connection
    mock_cursor._c = Mock()
    mock_cursor.paramstyle = "qmark"

    mocked_csv.side_effect = [StringIO("""\col1,col2,col3\n1,3,foo\n2,5,bar\n-1,7,baz""")]

    mock_cursor.insert_data_bulk(
        filename="mocked_csv",
        table_name="test_table",
        parameter_indices=indexes,
        column_names=names,
        delimiter=",",
        batch_size=3,
    )

    assert spy.called is True
    assert spy.call_args[0][0] == exp_execute_args[0]
    assert spy.call_args[0][1] == exp_execute_args[1]


@pytest.mark.parametrize("batch_size", [1, 2, 3, 4])
@patch("builtins.open", new_callable=mock_open)
def test_insert_data_uses_batch_size(mocked_csv, batch_size, mocker) -> None:
    # mock fetchone to return "True" to ensure the table_name and column_name
    # validation steps pass
    mocker.patch("redshift_connector.Cursor.fetchone", return_value=[1])
    mock_cursor: Cursor = Cursor.__new__(Cursor)

    # spy on the execute method, so we can check value of sql_query
    spy = mocker.spy(mock_cursor, "execute")

    # mock out the connection
    mock_cursor._c = Mock()
    mock_cursor.paramstyle = "qmark"

    mocked_csv.side_effect = [StringIO("""\col1,col2,col3\n1,3,foo\n2,5,bar\n-1,7,baz""")]

    mock_cursor.insert_data_bulk(
        filename="mocked_csv",
        table_name="test_table",
        parameter_indices=[0, 1, 2],
        column_names=["col1", "col2", "col3"],
        delimiter=",",
        batch_size=batch_size,
    )

    assert spy.called is True
    actual_insert_stmts_executed = 0

    for call in spy.mock_calls:
        if len(call[1]) == 2 and "INSERT INTO" in call[1][0]:
            actual_insert_stmts_executed += 1

    assert actual_insert_stmts_executed == ceil(3 / batch_size)


max_params = 32767


@patch("builtins.open", new_callable=mock_open)
def test_insert_data_bulk_raises_too_many_parameters(mocked_csv, mocker) -> None:
    # mock fetchone to return "True" to ensure the table_name and column_name
    # validation steps pass
    mocker.patch("redshift_connector.Cursor.fetchone", return_value=[1])

    mock_cursor: Cursor = Cursor.__new__(Cursor)

    # mock out the connection to raise DataError.
    mock_cursor._c = Mock()
    mocker.patch.object(
        mock_cursor._c, "execute", side_effect=DataError("Prepared statement exceeds bind parameter " "limit 32767.")
    )
    mock_cursor.paramstyle = "mocked"

    indexes, names = (
        [0],
        ["col1"],
    )

    csv_str = "\col1\n" + "1\n" * max_params + "1"  # 32768 rows
    mocked_csv.side_effect = [StringIO(csv_str)]

    with pytest.raises(DataError, match="Prepared statement exceeds bind parameter limit 32767."):
        mock_cursor.insert_data_bulk(
            filename="mocked_csv",
            table_name="githubissue165",
            parameter_indices=indexes,
            column_names=["col1"],
            delimiter=",",
            batch_size=max_params + 1,
        )


@patch("builtins.open", new_callable=mock_open)
def test_insert_data_raises_too_many_parameters(mocker) -> None:
    mock_cursor: Cursor = Cursor.__new__(Cursor)

    # mock out the connection to raise DataError.
    mock_cursor._c = Mock()
    mock_cursor._c.execute.side_effect = DataError("Prepared statement exceeds bind " "parameter limit 32767.")
    mock_cursor.paramstyle = "mocked"

    prepared_stmt = "INSERT INTO githubissue165 (col1) VALUES " + "(%s), " * max_params + "(%s);"
    params = [1 for _ in range(max_params + 1)]

    with pytest.raises(DataError, match="Prepared statement exceeds bind parameter limit 32767."):
        mock_cursor.execute(prepared_stmt, params)


@pandas_only
def test_write_dataframe_handles_npdtyes(mocker):
    import numpy as np
    import pandas as pd

    mocker.patch("redshift_connector.Cursor.execute", return_value=None)
    mocker.patch("redshift_connector.Cursor.fetchone", return_value=[1])
    mock_cursor: Cursor = Cursor.__new__(Cursor)
    mock_connection: Connection = Connection.__new__(Connection)
    mock_cursor._c = mock_connection

    mock_cursor.paramstyle = "mocked_val"
    for datatype, data in (
        ("int8_col", np.array([1], dtype=np.int8)),
        ("int16_col", np.array([1], dtype=np.int16)),
        ("int32_col", np.array([1], dtype=np.int32)),
        ("int64_col", np.array([1], dtype=np.int64)),
        ("uint8_col", np.array([1], dtype=np.uint8)),
        ("uint16_col", np.array([1], dtype=np.uint16)),
        ("uint32_col", np.array([1], dtype=np.uint32)),
        ("uint64_col", np.array([1], dtype=np.uint64)),
        ("float16_col", np.array([1.0], dtype=np.float16)),
        ("float32_col", np.array([1.0], dtype=np.float32)),
        ("float64_col", np.array([1.0], dtype=np.float64)),
        ("complex64_col", np.array([1 + 1j], dtype=np.complex64)),
        ("complex128_col", np.array([1 + 1j], dtype=np.complex128)),
        ("bool_col", np.array([True], dtype=np.bool_)),
        ("string_col", np.array(["hello"], dtype="U")),
        ("object_col", np.array([{"key", "value"}], dtype=object)),
    ):
        spy = mocker.spy(mock_cursor, "execute")
        dataframe = pd.DataFrame(data)
        mock_cursor.write_dataframe(df=dataframe, table=datatype)

        assert spy.called
        assert spy.call_count == 2  # once for __is_valid_table, once for write_dataframe
        assert not isinstance(spy.mock_calls[1].args[1], np.ndarray)
        assert isinstance(spy.mock_calls[1].args[1], list)
        assert len(spy.mock_calls[1].args[1]) == 1
        # bind parameter list should not contain numpy objects
        assert not isinstance(spy.mock_calls[1].args[1][0], np.generic)


@pandas_only
def test_write_dataframe_handles_pandas_types(mocker):
    import pandas as pd

    mocker.patch("redshift_connector.Cursor.execute", return_value=None)
    mocker.patch("redshift_connector.Cursor.fetchone", return_value=[1])
    mock_cursor: Cursor = Cursor.__new__(Cursor)
    mock_connection: Connection = Connection.__new__(Connection)
    mock_cursor._c = mock_connection

    mock_cursor.paramstyle = "mocked_val"

    for datatype, data, _type in (
        ("int64", pd.Series([42]), int),
        ("float64", pd.Series([3.14]), float),
        ("object", pd.Series(["Hello, Pandas!"]), str),
        ("bool", pd.Series([True]), bool),
        ("datetime64", pd.Series([pd.Timestamp("2022-01-01")]), int),
        ("timedelta64", pd.Series([pd.Timedelta(days=5)]), int),
    ):
        spy = mocker.spy(mock_cursor, "execute")
        dataframe = pd.DataFrame(data)
        mock_cursor.write_dataframe(df=dataframe, table=datatype)

        assert spy.called
        assert spy.call_count == 2  # once for __is_valid_table, once for write_dataframe
        assert not isinstance(spy.mock_calls[1].args[1], pd.core.base.PandasObject)
        assert isinstance(spy.mock_calls[1].args[1], list)
        assert len(spy.mock_calls[1].args[1]) == 1
        # bind parameter list should not contain numpy objects
        assert isinstance(spy.mock_calls[1].args[1][0], _type)


@pandas_only
@pytest.mark.parametrize(
    "datatype,data,_type",
    (
        ("int", 42, int),
        ("float", 3.14, float),
        ("str", "H", str),
        ("bool", True, bool),
        ("list", [1, 2, 3], list),
        ("tuple", (4, 5, 6), tuple),
        ("set", {1, 2, 3}, set),
        ("datetime", datetime.datetime.now(datetime.timezone.utc), datetime.datetime),
    ),
)
def test_write_dataframe_handles_python_types(mocker, datatype, data, _type):
    import datetime

    import pandas as pd

    mocker.patch("redshift_connector.Cursor.execute", return_value=None)
    mocker.patch("redshift_connector.Cursor.fetchone", return_value=[1])
    mock_cursor: Cursor = Cursor.__new__(Cursor)
    mock_connection: Connection = Connection.__new__(Connection)
    mock_cursor._c = mock_connection

    mock_cursor.paramstyle = "mocked_val"

    spy = mocker.spy(mock_cursor, "execute")
    dataframe = pd.DataFrame({col: [data] * 1 for col in (datatype,)})
    mock_cursor.write_dataframe(df=dataframe, table=datatype)

    assert spy.called
    assert spy.call_count == 2  # once for __is_valid_table, once for write_dataframe
    assert not isinstance(spy.mock_calls[1].args[1], pd.core.base.PandasObject)
    assert isinstance(spy.mock_calls[1].args[1], list)
    assert len(spy.mock_calls[1].args[1]) == 1
    assert isinstance((spy.mock_calls[1].args[1][0]), _type)
