import configparser
import os
import typing
from warnings import filterwarnings

import pytest  # type: ignore
from pytest_mock import mocker

import redshift_connector

conf = configparser.ConfigParser()
root_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
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
    def config_class_consts(cls, enabled: bool):
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


@pytest.mark.skip(reason="manual")
@pytest.mark.parametrize("_input", get_schemas_test_data())
def test_get_schemas(mocker, _input, db_kwargs):
    database_metadata_current_db_only_val, _args = _input
    db_kwargs["database_metadata_current_db_only"] = database_metadata_current_db_only_val
    with redshift_connector.connect(**db_kwargs) as conn:
        assert conn.is_single_database_metadata is database_metadata_current_db_only_val

        with conn.cursor() as cursor:
            spy = mocker.spy(cursor, "execute")
            result: typing.Tuple = cursor.get_schemas(**_args)
            # ensure query was executed with arguments passed to get_schemas
            assert spy.called
            assert spy.call_count == 1

            if _args["schema_pattern"] is not None:  # should be in parameterized portion
                assert _args["schema_pattern"] in spy.call_args[0][1]

            if _args["catalog"] is not None:
                assert _args["catalog"] in spy.call_args[0][0]

            assert len(result) > 0, print(spy.call_args, "\n", result)
            assert len(result[0]) == 2


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


@pytest.mark.skip(reason="manual")
@pytest.mark.parametrize("_input", get_tables_test_data())
def test_get_tables(mocker, _input, db_kwargs):
    database_metadata_current_db_only_val, _args = _input
    db_kwargs["database_metadata_current_db_only"] = database_metadata_current_db_only_val
    with redshift_connector.connect(**db_kwargs) as conn:
        assert conn.is_single_database_metadata is database_metadata_current_db_only_val

        with conn.cursor() as cursor:
            spy = mocker.spy(cursor, "execute")
            result: typing.Tuple = cursor.get_tables(**_args)
            print(result)
            # ensure query was executed with arguments passed to get_schemas
            assert spy.called

            if _args["schema_pattern"] is not None and database_metadata_current_db_only_val:
                assert spy.call_count == 2  # call in __schema_pattern_match(), get_tables()
            else:
                assert spy.call_count == 1

            if _args["schema_pattern"] is not None:  # should be in parameterized portion
                print(spy.call_args)
                print(spy.call_args[0][1])
                print(_args, database_metadata_current_db_only_val, TestListCatalog.test_schema)
                assert _args["schema_pattern"] in spy.call_args[0][1], print(spy.call_args)

            if _args["catalog"] is not None:
                assert _args["catalog"] in spy.call_args[0][0]

            for arg in (_args["schema_pattern"], _args["table_name_pattern"]):
                if arg is not None:
                    assert arg in spy.call_args[0][1]

            assert len(result) > 0, print(spy.call_args, "\n", result)
            assert len(result[0]) == 10


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


@pytest.mark.skip(reason="manual")
@pytest.mark.parametrize("_input", get_columns_test_data())
def test_get_columns(mocker, _input, db_kwargs):
    database_metadata_current_db_only_val, _args = _input
    db_kwargs["database_metadata_current_db_only"] = database_metadata_current_db_only_val
    with redshift_connector.connect(**db_kwargs) as conn:
        assert conn.is_single_database_metadata is database_metadata_current_db_only_val

        with conn.cursor() as cursor:
            spy = mocker.spy(cursor, "execute")
            result: typing.Tuple = cursor.get_columns(**_args)
            # ensure query was executed with arguments passed to get_schemas
            assert spy.called

            if _args["schema_pattern"] is not None and database_metadata_current_db_only_val:
                assert spy.call_count == 2  # call in __schema_pattern_match(), get_columns()
            else:
                assert spy.call_count == 1

            for arg in (
                _args["catalog"],
                _args["schema_pattern"],
                _args["tablename_pattern"],
                _args["columnname_pattern"],
            ):
                if arg is not None:
                    assert arg in spy.call_args[0][0]

            assert len(result) > 0, print(spy.call_args, "\n", result)
            assert len(result[0]) == 24
