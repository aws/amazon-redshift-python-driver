import typing
from collections import deque
from test.utils import pandas_only
from unittest.mock import Mock, PropertyMock, patch

import pytest  # type: ignore
from pytest_mock import mocker

from redshift_connector import Connection, Cursor, InterfaceError, ProgrammingError

IS_SINGLE_DATABASE_METADATA_TOGGLE: typing.List[bool] = [True, False]


test_warn_response_data: typing.List[typing.Tuple[typing.Optional[typing.List[bytes]], str]] = [
    ([b"ab\xffcd"], "Unable to decode column names. Byte values will be used for pandas dataframe column labels."),
    (None, "No row description was found. pandas dataframe will be missing column labels."),
]


@pandas_only
@pytest.mark.parametrize("_input", test_warn_response_data)
def test_fetch_dataframe_warns_user(_input, mocker):
    data, exp_warning_msg = _input
    mock_cursor: Cursor = Cursor.__new__(Cursor)
    mocker.patch("redshift_connector.Cursor._getDescription", return_value=[data])
    mocker.patch("redshift_connector.Cursor.__next__", return_value=["blah"])
    with pytest.warns(UserWarning, match=exp_warning_msg):
        mock_cursor.fetch_dataframe(1)


@pandas_only
def test_fetch_dataframe_no_results(mocker):
    mock_cursor: Cursor = Cursor.__new__(Cursor)
    mocker.patch("redshift_connector.Cursor._getDescription", return_value=["test"])
    mocker.patch("redshift_connector.Cursor.__next__", side_effect=StopIteration("mocked end"))

    assert mock_cursor.fetch_dataframe(1) is None


def test_raw_connection_property_warns():
    mock_cursor: Cursor = Cursor.__new__(Cursor)
    mock_cursor._c = Connection.__new__(Connection)

    with pytest.warns(UserWarning, match="DB-API extension cursor.connection used"):
        mock_cursor.connection


def test_get_description_no_ps():
    mock_cursor: Cursor = Cursor.__new__(Cursor)
    mock_cursor.ps = None
    assert mock_cursor._getDescription() is None


def test_execute_no_connection_raises_interface_error():
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
def test_get_procedures_considers_args(_input, mocker):
    catalog, schema_pattern, procedure_name_pattern = _input
    mocker.patch("redshift_connector.Cursor.execute", return_value=None)
    mocker.patch("redshift_connector.Cursor.fetchall", return_value=None)
    mocker.patch("redshift_connector.Connection.is_single_database_metadata", return_value=True)

    mock_cursor: Cursor = Cursor.__new__(Cursor)
    mock_connection: Cursor = Connection.__new__(Connection)
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
def test__get_catalog_filter_conditions_considers_args(_input, is_single_database_metadata_val):
    catalog, api_supported_only_for_connected_database, database_col_name = _input

    mock_cursor: Cursor = Cursor.__new__(Cursor)
    mock_connection: Cursor = Connection.__new__(Connection)
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
def test_get_schemas_considers_args(_input, is_single_database_metadata_val, mocker):
    catalog, schema_pattern = _input
    mocker.patch("redshift_connector.Cursor.execute", return_value=None)
    mocker.patch("redshift_connector.Cursor.fetchall", return_value=None)

    mock_cursor: Cursor = Cursor.__new__(Cursor)
    mock_cursor.paramstyle = "mocked"
    mock_connection: Cursor = Connection.__new__(Connection)
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
def test_get_catalogs_considers_args(is_single_database_metadata_val, mocker):
    mocker.patch("redshift_connector.Cursor.execute", return_value=None)
    mocker.patch("redshift_connector.Cursor.fetchall", return_value=None)

    mock_cursor: Cursor = Cursor.__new__(Cursor)
    mock_cursor.paramstyle = "mocked"
    mock_connection: Cursor = Connection.__new__(Connection)
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
def test_get_tables_considers_args(is_single_database_metadata_val, _input, schema_pattern_type, mocker):
    catalog, schema_pattern, table_name_pattern = _input
    mocker.patch("redshift_connector.Cursor.execute", return_value=None)
    # mock the return value from __schema_pattern_match as it's return value is used in get_tables()
    # the other potential call to this method in get_tables() result is simply returned, so at this time
    # it has no impact
    mocker.patch(
        "redshift_connector.Cursor.fetchall",
        return_value=None if schema_pattern_type == "EXTERNAL_SCHEMA_QUERY" else tuple("mock"),
    )

    mock_cursor: Cursor = Cursor.__new__(Cursor)
    mock_cursor.paramstyle = "mocked"
    mock_connection: Cursor = Connection.__new__(Connection)
    mock_cursor._c = mock_connection
    spy = mocker.spy(mock_cursor, "execute")

    with patch(
        "redshift_connector.Connection.is_single_database_metadata", new_callable=PropertyMock()
    ) as mock_is_single_database_metadata:
        mock_is_single_database_metadata.__get__ = Mock(return_value=is_single_database_metadata_val)
        mock_cursor.get_tables(catalog, schema_pattern, table_name_pattern)

    assert spy.called

    if schema_pattern is not None and is_single_database_metadata_val:
        assert spy.call_count == 2  # call in __schema_pattern_match(), get_tables()
    else:
        assert spy.call_count == 1

    if catalog is not None:
        assert catalog in spy.call_args[0][0]

    for arg in (schema_pattern, table_name_pattern):
        if arg is not None:
            assert arg in spy.call_args[0][1]
