import configparser
import os
import typing

import pytest  # type: ignore

import redshift_connector

# Pre-reqs for testing datasharing
# These tests require the following:
# - A single Redshift instance OR two Redshift instances configured for datasharing as consumer and producer.
# - Config.ini entry for [ci-cluster] file populated with the host and port of the single Redshift instance or the
#   producer instance if you want to test datasharing metadata methods
#
# How to use this test file
# The included tests can be run to ensure the expected Redshift system tables are queried. Running this test via CLI
# and using tee can enable to you compare and contrast result sets returned by metadata APIs on multiple clusters with
# different patch versions. Please note the result set output is only useful if the same data is accessible by the
# database user in all test situations.


conf: configparser.ConfigParser = configparser.ConfigParser()
root_path: str = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
conf.read(root_path + "/config.ini")

cross_table_name: str = "alldt_test"
create_table_stmt: str = (
    "CREATE TABLE {} ("
    "c_bool boolean,"
    " c_smallint smallint,"
    " c_int int,"
    " c_bigint bigint,"
    " c_double double precision,"
    " c_decimal decimal(6,3),"
    " c_char char(10),"
    " c_varchar varchar(10),"
    " c_date date,"
    " c_timetz timetz,"
    " c_timestp timestamp,"
    " c_super super,"
    " c_geometry geometry"
    " );"
).format(cross_table_name)

startup_stmts: typing.Tuple[str, ...] = (
    "DROP TABLE IF EXISTS {}".format(cross_table_name),
    create_table_stmt,
    "DROP SCHEMA IF EXISTS ext_sales",
    "CREATE EXTERNAL SCHEMA ext_sales FROM Redshift DATABASE 'dev2' schema 'public_2'",
)

shutdown_stmts: typing.Tuple[str, ...] = (
    "DROP TABLE IF EXISTS {}".format(cross_table_name),
    "DROP SCHEMA IF EXISTS ext_sales",
)


# @pytest.fixture(scope="session", autouse=True)
# def list_catalog_config(request, db_kwargs):
#     filterwarnings("ignore", "DB-API extension cursor.next()")
#     filterwarnings("ignore", "DB-API extension cursor.__iter__()")
#     with redshift_connector.connect(**db_kwargs) as con:
#         con.paramstyle = "format"
#         with con.cursor() as cursor:
#             for stmt in startup_stmts:
#                 cursor.execute(stmt)
#
#     def fin():
#         try:
#             with redshift_connector.connect(**db_kwargs) as con:
#                 with con.cursor() as cursor:
#                     for stmt in shutdown_stmts:
#                         cursor.execute(stmt)
#         except redshift_connector.ProgrammingError:
#             pass
#
#     request.addfinalizer(fin)


class TestListCatalog:
    database: str = "dev"

    test_cross_db: str = "dev2"
    test_cross_db_schema: str = "public"
    test_cross_db_table: str = cross_table_name
    test_cross_db_column: str = "c_int"
    test_cross_db_schema_pattern: str = "p%"
    test_cross_db_table_pattern: str = "a%"
    test_cross_db_column_pattern: str = "c%"
    test_table_types: typing.Tuple[str, ...] = ("TABLE", "SHARED TABLE", "VIEW", "EXTERNAL TABLE")

    test_ext_schema: str = "ext_sales"
    test_ext_schema_table: str = "test"
    test_ext_schema_table_column: str = "id"
    test_ext_schema_pattern: str = "e%"
    test_ext_schema_table_pattern: str = "t%"
    test_ext_schema_table_column_pattern: str = "i%"

    test_db: str = ""
    test_schema: str = ""
    test_table: str = ""
    test_column: str = ""
    test_schema_pattern: str = ""
    test_table_pattern: str = ""
    test_column_pattern: str = ""

    @classmethod
    def config_class_consts(cls, enabled: bool) -> None:
        # configures test values based on if datashare is enabled in application
        TestListCatalog.test_db = TestListCatalog.database if enabled is True else TestListCatalog.test_cross_db

        if enabled is True:
            TestListCatalog.test_db = TestListCatalog.database
            TestListCatalog.test_schema = TestListCatalog.test_ext_schema
            TestListCatalog.test_table = TestListCatalog.test_ext_schema_table
            TestListCatalog.test_column = TestListCatalog.test_ext_schema_table_column
            TestListCatalog.test_schema_pattern = TestListCatalog.test_ext_schema_pattern
            TestListCatalog.test_table_pattern = TestListCatalog.test_ext_schema_table_pattern
            TestListCatalog.test_column_pattern = TestListCatalog.test_ext_schema_table_column_pattern
        else:
            TestListCatalog.test_schema = TestListCatalog.test_cross_db_schema
            TestListCatalog.test_table = TestListCatalog.test_cross_db_table
            TestListCatalog.test_column = TestListCatalog.test_cross_db_column
            TestListCatalog.test_schema_pattern = TestListCatalog.test_cross_db_schema_pattern
            TestListCatalog.test_table_pattern = TestListCatalog.test_cross_db_table_pattern
            TestListCatalog.test_column_pattern = TestListCatalog.test_cross_db_column_pattern


def get_schemas_test_data() -> typing.List[typing.Tuple[bool, typing.Dict[str, typing.Optional[str]]]]:
    result: typing.List[typing.Tuple[bool, typing.Dict[str, typing.Optional[str]]]] = []
    for flip in (True, False):
        TestListCatalog.config_class_consts(flip)
        arg_data: typing.List[typing.Dict[str, typing.Optional[str]]] = [
            {
                "catalog": None,
                "schema_pattern": None,
            },
            {"catalog": TestListCatalog.test_db, "schema_pattern": TestListCatalog.test_schema},
            {
                "catalog": None,
                "schema_pattern": TestListCatalog.test_schema,
            },
            {"catalog": TestListCatalog.test_db, "schema_pattern": None},
            {"catalog": TestListCatalog.test_db, "schema_pattern": TestListCatalog.test_schema_pattern},
            {"catalog": None, "schema_pattern": TestListCatalog.test_schema_pattern},
        ]
        for test_case in arg_data:
            result.append((flip, test_case))
    return result


@pytest.mark.parametrize("_input", get_schemas_test_data())
def test_get_schemas(mocker, _input, db_kwargs) -> None:
    database_metadata_current_db_only_val, _args = _input
    db_kwargs["database_metadata_current_db_only"] = database_metadata_current_db_only_val
    with redshift_connector.connect(**db_kwargs) as conn:
        with conn.cursor() as cursor:
            spy = mocker.spy(cursor, "execute")
            result: typing.Tuple = cursor.get_schemas(**_args)
            print(result)
            # ensure execute was called
            assert spy.called
            assert spy.call_count == 1  # call in get_schemas()

            # ensure execute was called with the catalog value in the prepared statement
            if _args["catalog"] is not None:
                assert _args["catalog"] in spy.call_args[0][0]

            # ensure execute was called with below bind parameters
            if _args["schema_pattern"] is not None:
                assert _args["schema_pattern"] in spy.call_args[0][1]

            # assert query text executed contains the target table name
            if conn.is_single_database_metadata:
                assert "FROM pg_catalog.pg_namespace" in spy.call_args[0][0]
            else:
                assert "FROM PG_CATALOG.SVV_ALL_SCHEMAS" in spy.call_args[0][0]


def get_tables_test_data() -> typing.List[typing.Optional[typing.Tuple[bool, typing.Dict[str, typing.Any]]]]:
    result: typing.List[typing.Optional[typing.Tuple[bool, typing.Dict[str, typing.Any]]]] = []
    for flip in (True, False):
        TestListCatalog.config_class_consts(flip)
        arg_data: typing.List[typing.Dict[str, typing.Any]] = [
            {"catalog": None, "schema_pattern": None, "table_name_pattern": None, "types": tuple()},
            {
                "catalog": TestListCatalog.test_db,
                "schema_pattern": TestListCatalog.test_schema,
                "table_name_pattern": TestListCatalog.test_table,
                "types": TestListCatalog.test_table_types,
            },
            {
                "catalog": None,
                "schema_pattern": TestListCatalog.test_schema,
                "table_name_pattern": TestListCatalog.test_table,
                "types": TestListCatalog.test_table_types,
            },
            {
                "catalog": TestListCatalog.test_db,
                "schema_pattern": None,
                "table_name_pattern": TestListCatalog.test_table,
                "types": TestListCatalog.test_table_types,
            },
            {
                "catalog": TestListCatalog.test_db,
                "schema_pattern": TestListCatalog.test_schema,
                "table_name_pattern": None,
                "types": TestListCatalog.test_table_types,
            },
            {
                "catalog": TestListCatalog.test_db,
                "schema_pattern": TestListCatalog.test_schema,
                "table_name_pattern": TestListCatalog.test_table,
                "types": tuple(),
            },
            {
                "catalog": None,
                "schema_pattern": None,
                "table_name_pattern": TestListCatalog.test_table,
                "types": TestListCatalog.test_table_types,
            },
            {
                "catalog": TestListCatalog.test_db,
                "schema_pattern": None,
                "table_name_pattern": None,
                "types": TestListCatalog.test_table_types,
            },
            {
                "catalog": TestListCatalog.test_db,
                "schema_pattern": TestListCatalog.test_schema,
                "table_name_pattern": None,
                "types": tuple(),
            },
            {
                "catalog": None,
                "schema_pattern": TestListCatalog.test_schema,
                "table_name_pattern": TestListCatalog.test_table,
                "types": tuple(),
            },
            {
                "catalog": None,
                "schema_pattern": None,
                "table_name_pattern": TestListCatalog.test_table,
                "types": tuple(),
            },
            {
                "catalog": None,
                "schema_pattern": None,
                "table_name_pattern": None,
                "types": TestListCatalog.test_table_types,
            },
            {
                "catalog": TestListCatalog.test_db,
                "schema_pattern": TestListCatalog.test_schema,
                "table_name_pattern": TestListCatalog.test_table_pattern,
                "types": TestListCatalog.test_table_types,
            },
            {
                "catalog": TestListCatalog.test_db,
                "schema_pattern": TestListCatalog.test_schema_pattern,
                "table_name_pattern": TestListCatalog.test_table,
                "types": TestListCatalog.test_table_types,
            },
            {
                "catalog": TestListCatalog.test_db,
                "schema_pattern": TestListCatalog.test_schema_pattern,
                "table_name_pattern": TestListCatalog.test_table_pattern,
                "types": TestListCatalog.test_table_types,
            },
            {
                "catalog": None,
                "schema_pattern": TestListCatalog.test_schema_pattern,
                "table_name_pattern": TestListCatalog.test_table_pattern,
                "types": tuple(),
            },
        ]
        for test_case in arg_data:
            result.append((flip, test_case))
    return result


@pytest.mark.parametrize("_input", get_tables_test_data())
def test_get_tables(mocker, _input, db_kwargs) -> None:
    database_metadata_current_db_only_val, _args = _input
    db_kwargs["database_metadata_current_db_only"] = database_metadata_current_db_only_val
    with redshift_connector.connect(**db_kwargs) as conn:
        with conn.cursor() as cursor:
            spy = mocker.spy(cursor, "execute")
            result: typing.Tuple = cursor.get_tables(**_args)
            print(result)
            # ensure execute was called
            assert spy.called

            if conn.is_single_database_metadata:
                assert spy.call_count == 2  # call in __schema_pattern_match(), get_tables()
            else:
                assert spy.call_count == 1  # call in get_tables()

            # ensure execute was called with the catalog value in the prepared statement
            if _args["catalog"] is not None:
                assert _args["catalog"] in spy.call_args[0][0]

            # ensure execute was called with below bind parameters
            for arg in (_args["schema_pattern"], _args["table_name_pattern"]):
                if arg is not None:
                    assert arg in spy.call_args[0][1]

            # we cannot easily know what schema pattern matches in Python driver, so
            # we check table is one of a few options based on whether is_single_database_metadata
            # is true or false

            possible_not_ds_tables = (
                "FROM svv_tables",  # universal
                "FROM pg_catalog.pg_namespace n, pg_catalog.pg_class",  # local
                "FROM svv_external_tables",  # external
            )
            possible_ds_tables = (
                "FROM PG_CATALOG.SVV_ALL_TABLES",  # universal
                "FROM pg_catalog.pg_namespace n, pg_catalog.pg_class",  # local
                "FROM svv_external_tables",  # external
            )

            if conn.is_single_database_metadata:
                for table in possible_not_ds_tables:
                    if table in spy.call_args[0][0]:
                        assert 1 == 1
                        return
                assert 1 == 0, spy.call_args[0][0]
            else:
                for table in possible_ds_tables:
                    if table in spy.call_args[0][0]:
                        assert 1 == 1
                        return
                assert 1 == 0, spy.call_args[0][0]


def get_columns_test_data() -> typing.List[typing.Tuple[bool, typing.Dict[str, typing.Optional[str]]]]:
    result: typing.List[typing.Tuple[bool, typing.Dict[str, typing.Optional[str]]]] = []
    for flip in (True, False):
        TestListCatalog.config_class_consts(flip)
        arg_data: typing.List[typing.Dict[str, typing.Optional[str]]] = [
            {
                "catalog": None,
                "schema_pattern": None,
                "tablename_pattern": None,
                "columnname_pattern": None,
            },
            {
                "catalog": TestListCatalog.test_db,
                "schema_pattern": TestListCatalog.test_schema,
                "tablename_pattern": TestListCatalog.test_table,
                "columnname_pattern": TestListCatalog.test_column,
            },
            {
                "catalog": None,
                "schema_pattern": TestListCatalog.test_schema,
                "tablename_pattern": TestListCatalog.test_table,
                "columnname_pattern": TestListCatalog.test_column,
            },
            {
                "catalog": TestListCatalog.test_db,
                "schema_pattern": None,
                "tablename_pattern": TestListCatalog.test_table,
                "columnname_pattern": TestListCatalog.test_column,
            },
            {
                "catalog": TestListCatalog.test_db,
                "schema_pattern": TestListCatalog.test_schema,
                "tablename_pattern": None,
                "columnname_pattern": TestListCatalog.test_column,
            },
            {
                "catalog": TestListCatalog.test_db,
                "schema_pattern": TestListCatalog.test_schema,
                "tablename_pattern": TestListCatalog.test_table,
                "columnname_pattern": None,
            },
            {
                "catalog": None,
                "schema_pattern": None,
                "tablename_pattern": TestListCatalog.test_table,
                "columnname_pattern": TestListCatalog.test_column,
            },
            {
                "catalog": TestListCatalog.test_db,
                "schema_pattern": None,
                "tablename_pattern": None,
                "columnname_pattern": TestListCatalog.test_column,
            },
            {
                "catalog": TestListCatalog.test_db,
                "schema_pattern": TestListCatalog.test_schema,
                "tablename_pattern": None,
                "columnname_pattern": None,
            },
            {
                "catalog": None,
                "schema_pattern": TestListCatalog.test_schema,
                "tablename_pattern": TestListCatalog.test_table,
                "columnname_pattern": None,
            },
            {
                "catalog": None,
                "schema_pattern": None,
                "tablename_pattern": None,
                "columnname_pattern": TestListCatalog.test_column,
            },
            {
                "catalog": TestListCatalog.test_db,
                "schema_pattern": None,
                "tablename_pattern": None,
                "columnname_pattern": None,
            },
            {
                "catalog": None,
                "schema_pattern": TestListCatalog.test_schema,
                "tablename_pattern": None,
                "columnname_pattern": None,
            },
            {
                "catalog": None,
                "schema_pattern": None,
                "tablename_pattern": TestListCatalog.test_table,
                "columnname_pattern": None,
            },
            {
                "catalog": TestListCatalog.test_db,
                "schema_pattern": TestListCatalog.test_schema,
                "tablename_pattern": TestListCatalog.test_table_pattern,
                "columnname_pattern": TestListCatalog.test_column,
            },
            {
                "catalog": TestListCatalog.test_db,
                "schema_pattern": TestListCatalog.test_schema_pattern,
                "tablename_pattern": TestListCatalog.test_table,
                "columnname_pattern": TestListCatalog.test_column,
            },
            {
                "catalog": TestListCatalog.test_db,
                "schema_pattern": TestListCatalog.test_schema_pattern,
                "tablename_pattern": TestListCatalog.test_table_pattern,
                "columnname_pattern": TestListCatalog.test_column,
            },
            {
                "catalog": None,
                "schema_pattern": TestListCatalog.test_schema_pattern,
                "tablename_pattern": TestListCatalog.test_table_pattern,
                "columnname_pattern": TestListCatalog.test_column,
            },
            {
                "catalog": TestListCatalog.test_db,
                "schema_pattern": TestListCatalog.test_schema_pattern,
                "tablename_pattern": TestListCatalog.test_table_pattern,
                "columnname_pattern": None,
            },
            {
                "catalog": None,
                "schema_pattern": TestListCatalog.test_schema_pattern,
                "tablename_pattern": TestListCatalog.test_table_pattern,
                "columnname_pattern": None,
            },
            {
                "catalog": TestListCatalog.test_db,
                "schema_pattern": TestListCatalog.test_schema,
                "tablename_pattern": TestListCatalog.test_table_pattern,
                "columnname_pattern": TestListCatalog.test_column_pattern,
            },
            {
                "catalog": TestListCatalog.test_db,
                "schema_pattern": TestListCatalog.test_schema,
                "tablename_pattern": TestListCatalog.test_table,
                "columnname_pattern": TestListCatalog.test_column_pattern,
            },
            {
                "catalog": TestListCatalog.test_db,
                "schema_pattern": TestListCatalog.test_schema_pattern,
                "tablename_pattern": TestListCatalog.test_table_pattern,
                "columnname_pattern": TestListCatalog.test_column_pattern,
            },
            {
                "catalog": TestListCatalog.test_db,
                "schema_pattern": TestListCatalog.test_schema,
                "tablename_pattern": None,
                "columnname_pattern": TestListCatalog.test_column_pattern,
            },
            {
                "catalog": TestListCatalog.test_db,
                "schema_pattern": None,
                "tablename_pattern": TestListCatalog.test_table_pattern,
                "columnname_pattern": TestListCatalog.test_column_pattern,
            },
            {
                "catalog": None,
                "schema_pattern": None,
                "tablename_pattern": TestListCatalog.test_table_pattern,
                "columnname_pattern": TestListCatalog.test_column_pattern,
            },
            {
                "catalog": None,
                "schema_pattern": None,
                "tablename_pattern": None,
                "columnname_pattern": TestListCatalog.test_column_pattern,
            },
        ]
        for test_case in arg_data:
            result.append((flip, test_case))
    return result


@pytest.mark.parametrize("_input", get_columns_test_data())
def test_get_columns(mocker, _input, db_kwargs) -> None:
    database_metadata_current_db_only_val, _args = _input
    db_kwargs["database_metadata_current_db_only"] = database_metadata_current_db_only_val
    with redshift_connector.connect(**db_kwargs) as conn:
        with conn.cursor() as cursor:
            spy = mocker.spy(cursor, "execute")
            result: typing.Tuple = cursor.get_columns(**_args)
            print(result)
            # ensure execute was called
            assert spy.called

            if conn.is_single_database_metadata:
                assert spy.call_count == 2  # call in __schema_pattern_match(), get_columns()
            else:
                assert spy.call_count == 1  # call in get_columns()

            # ensure execute was called with below bind parameters
            for arg in (
                _args["catalog"],
                _args["schema_pattern"],
                _args["tablename_pattern"],
                _args["columnname_pattern"],
            ):
                if arg is not None:
                    assert arg in spy.call_args[0][0]

            # we cannot easily know what schema pattern matches in Python driver, so
            # we check table is one of a few options based on whether is_single_database_metadata
            # is true or false

            possible_not_ds_tables = (
                "FROM svv_columns",  # universal
                "FROM pg_catalog.pg_namespace",  # local
                "FROM svv_external_columns",  # external
            )
            possible_ds_tables = (
                "FROM PG_CATALOG.svv_all_columns",  # universal
                "FROM pg_catalog.pg_namespace",  # local
                "FROM svv_external_columns",  # external
            )

            if conn.is_single_database_metadata:
                for table in possible_not_ds_tables:
                    if table in spy.call_args[0][0]:
                        assert 1 == 1
                        return
                assert 1 == 0, spy.call_args[0][0]
            else:
                for table in possible_ds_tables:
                    if table in spy.call_args[0][0]:
                        assert 1 == 1
                        return
                assert 1 == 0, spy.call_args[0][0]


def get_catalogs_test_data() -> typing.List[bool]:
    return [True, False]


@pytest.mark.parametrize("_input", get_catalogs_test_data())
def test_get_catalogs(mocker, _input, db_kwargs) -> None:
    database_metadata_current_db_only_val = _input
    db_kwargs["database_metadata_current_db_only"] = database_metadata_current_db_only_val
    with redshift_connector.connect(**db_kwargs) as conn:
        with conn.cursor() as cursor:
            spy = mocker.spy(cursor, "execute")
            result: typing.Tuple = cursor.get_catalogs()
            print(result)
            # ensure execute was called
            assert spy.called
            assert spy.call_count == 1  # call in get_catalogs()

            if conn.is_single_database_metadata:
                assert "current_database()" in spy.call_args[0][0]
            else:
                assert "PG_CATALOG.SVV_REDSHIFT_DATABASES" in spy.call_args[0][0]
