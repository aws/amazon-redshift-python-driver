import configparser
import os
import typing
import pytest  # type: ignore

import redshift_connector
from redshift_connector.metadataAPIHelper import MetadataAPIHelper

conf: configparser.ConfigParser = configparser.ConfigParser()
root_path: str = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
conf.read(root_path + "/config.ini")

# Manually set the following flag to False if we want to test cross-database metadata api call
disable_cross_database_testing: bool = True

catalogs_sql_name: str = "təst_cat好log$123`~!#$%^&*()_+-={}[]:\"\",./<>?'' delimited"
catalog_name: str = "təst_cat好log$123`~!#$%^&*()_+-={}[]:\",./<>?'' delimited"
catalog_identifier: str = "təst_cat好log$123`~!#$%^&*()_+-={}[]:\",./<>?'' delimited"

object_sql_name: str = "təst_n𝒜m好e$123\\n\\t\\r\\0`~!@#$%^&*()_+-={}[]:\"\";,./<>?\\'' delimited"
object_name: str = "təst_n𝒜m好e$123\\n\\t\\r\\0`~!@#$%^&*()_+-={}[]:\";,./<>?\\'' delimited"
object_identifier: str = "təst_n𝒜m好e$123\\\\n\\\\t\\\\r\\\\0`~!@#$%^&*()_+-={}[]:\";,./<>?\\\\'' delimited"
object_identifier_mixed_case: str = "təst_N𝒜m好e$123\\\\n\\\\t\\\\r\\\\0`~!@#$%^&*()_+-={}[]:\";,./<>?\\\\'' Delimited"

startup_stmts: typing.Tuple[str, ...] = (
    "DROP SCHEMA IF EXISTS \"{}\" CASCADE;".format(object_sql_name),
    "CREATE SCHEMA \"{}\";".format(object_sql_name),
    "create table \"{}\".\"{}\" (\"{}\" INT);".format(object_sql_name, object_sql_name, object_sql_name),
)

test_cases = [
    # Delimited identifier with lower case
    ([catalog_identifier, object_identifier], 1,
     [object_name]),

    # Delimited identifier with mixed case
    ([catalog_identifier, object_identifier_mixed_case], 0,
     []),

    # Delimited identifier with lower case + non-printable UTF8 (control character)
    ([catalog_identifier, "təst_n𝒜m好e$123\n\t\r\0`~!@#$%^&*()_+-={}[]:\";,./<>?\\\\'' delimited"], 0,
     []),
]

@pytest.fixture(scope="class", autouse=True)
def test_metadataAPI_config(request, db_kwargs):
    global cur_db_kwargs
    cur_db_kwargs = dict(db_kwargs)
    print(cur_db_kwargs)

    with redshift_connector.connect(**cur_db_kwargs) as con:
        con.paramstyle = "format"
        con.autocommit = True
        with con.cursor() as cursor:
            try:
                cursor.execute("drop database \"{}\";".format(catalogs_sql_name))
            except redshift_connector.ProgrammingError:
                pass
            cursor.execute("create database \"{}\";".format(catalogs_sql_name))
    cur_db_kwargs["database"] = catalog_name
    with redshift_connector.connect(**cur_db_kwargs) as con:
        con.paramstyle = "format"
        with con.cursor() as cursor:
            for stmt in startup_stmts:
                cursor.execute(stmt)

            con.commit()
    def fin():
        try:
            with redshift_connector.connect(**db_kwargs) as con:
                con.autocommit = True
                with con.cursor() as cursor:
                    cursor.execute("drop database \"{}\";".format(catalogs_sql_name))
                    cursor.execute("select 1;")
        except redshift_connector.ProgrammingError:
            pass

    request.addfinalizer(fin)

@pytest.mark.parametrize("test_case, expected_row_count, expected_result", test_cases)
def test_get_schemas_special_character(db_kwargs, test_case, expected_row_count, expected_result) -> None:
    global cur_db_kwargs
    if disable_cross_database_testing:
        test_db_kwargs = dict(cur_db_kwargs)
    else:
        test_db_kwargs = dict(db_kwargs)
        test_db_kwargs["database_metadata_current_db_only"] = False

    with redshift_connector.connect(**test_db_kwargs) as conn:
        with conn.cursor() as cursor:
            result: tuple = cursor.get_schemas(test_case[0], test_case[1])

            assert len(result) == expected_row_count

            for actual_row, expected_schema in zip(result, expected_result):
                assert actual_row[0] == expected_schema
                assert actual_row[1] == catalog_name

@pytest.mark.parametrize("test_case, expected_row_count, expected_result", test_cases)
def test_get_tables_special_character(db_kwargs, test_case, expected_row_count, expected_result) -> None:
    global cur_db_kwargs
    if disable_cross_database_testing:
        test_db_kwargs = dict(cur_db_kwargs)
    else:
        test_db_kwargs = dict(db_kwargs)
        test_db_kwargs["database_metadata_current_db_only"] = False

    with redshift_connector.connect(**test_db_kwargs) as conn:
        with conn.cursor() as cursor:
            result: tuple = cursor.get_tables(test_case[0], test_case[1], test_case[1], None)
            assert len(result) == expected_row_count
            for actual_row, expected_name in zip(result, expected_result):
                assert actual_row[0] == catalog_name
                assert actual_row[1] == expected_name
                assert actual_row[2] == expected_name

@pytest.mark.parametrize("test_case, expected_row_count, expected_result", test_cases)
def test_get_columns_special_character(db_kwargs, test_case, expected_row_count, expected_result) -> None:
    global cur_db_kwargs
    if disable_cross_database_testing:
        test_db_kwargs = dict(cur_db_kwargs)
    else:
        test_db_kwargs = dict(db_kwargs)
        test_db_kwargs["database_metadata_current_db_only"] = False

    with redshift_connector.connect(**test_db_kwargs) as conn:
        with conn.cursor() as cursor:
            result: tuple = cursor.get_columns(test_case[0], test_case[1], test_case[1], test_case[1])
            assert len(result) == expected_row_count
            for actual_row, expected_name in zip(result, expected_result):
                assert actual_row[0] == catalog_name
                assert actual_row[1] == expected_name
                assert actual_row[2] == expected_name
                assert actual_row[3] == expected_name

