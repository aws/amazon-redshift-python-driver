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

catalog_name: str = "test_sql_injection -- comment"

schema_name: str = "test_schema; create table pwn1(i int);--"
table_name: str = "test_table; create table pwn2(i int);--"
col_name: str = "col"

startup_stmts: typing.Tuple[str, ...] = (
    "DROP SCHEMA IF EXISTS \"{}\" CASCADE;".format(schema_name),
    "CREATE SCHEMA \"{}\";".format(schema_name),
    "create table \"{}\".\"{}\" (col INT);".format(schema_name, table_name),
)

test_cases = [
    "' OR '1'='1",
    "' OR 1=1 --",
    "' OR '1'='1' --",
    "') OR 1=1 --",
    "') OR '1'='1' --",
    "' OR 1=1;--",
    "' OR 1=1 LIMIT 1;--",
    "' UNION SELECT null --",
    "' UNION SELECT null, null --",
    "' UNION SELECT 1, 'username', 'password' FROM users --",
    "' UNION SELECT * FROM users --",
    "') AND 1=CAST((SELECT current_database()) AS INT) --",
    "' AND 1=1 --",
    "' AND 1=2 --",
    "'; DROP TABLE test_table_100; --",
    "\"; DROP TABLE test_table_100; --\"",
    "; DROP TABLE test_table_100; --"
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
                cursor.execute("drop database \"{}\";".format(catalog_name))
            except redshift_connector.ProgrammingError:
                pass
            cursor.execute("create database \"{}\";".format(catalog_name))
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
                    cursor.execute("drop database \"{}\";".format(catalog_name))
                    cursor.execute("select 1;")
        except redshift_connector.ProgrammingError:
            pass

    request.addfinalizer(fin)

@pytest.mark.parametrize("test_input", test_cases)
def test_input_parameter_sql_injection(db_kwargs, test_input) -> None:
    with redshift_connector.connect(**db_kwargs) as conn:
        with conn.cursor() as cursor:
            try:
                result: tuple = cursor.get_schemas(test_input, None)
            except Exception as e:
                pytest.fail(f"Unexpected exception raised: {e}")

def test_get_schemas_sql_injection(db_kwargs) -> None:
    global cur_db_kwargs
    if disable_cross_database_testing:
        test_db_kwargs = dict(cur_db_kwargs)
    else:
        test_db_kwargs = dict(db_kwargs)
        test_db_kwargs["database_metadata_current_db_only"] = False

    with redshift_connector.connect(**test_db_kwargs) as conn:
        with conn.cursor() as cursor:
            result: tuple = cursor.get_schemas(catalog_name, None)

            assert len(result) > 0

            found_expected_row: bool = False
            for actual_row in result:
                if actual_row[0] == schema_name and actual_row[1] == catalog_name:
                    found_expected_row = True

            assert found_expected_row

def test_get_tables_sql_injection(db_kwargs) -> None:
    global cur_db_kwargs
    if disable_cross_database_testing:
        test_db_kwargs = dict(cur_db_kwargs)
    else:
        test_db_kwargs = dict(db_kwargs)
        test_db_kwargs["database_metadata_current_db_only"] = False

    with redshift_connector.connect(**test_db_kwargs) as conn:
        with conn.cursor() as cursor:
            result: tuple = cursor.get_tables(catalog_name, None, None)

            assert len(result) > 0

            found_expected_row: bool = False
            for actual_row in result:
                if actual_row[0] == catalog_name and actual_row[1] == schema_name and actual_row[2] == table_name:
                    found_expected_row = True

            assert found_expected_row

def test_get_columns_sql_injection(db_kwargs) -> None:
    global cur_db_kwargs
    if disable_cross_database_testing:
        test_db_kwargs = dict(cur_db_kwargs)
    else:
        test_db_kwargs = dict(db_kwargs)
        test_db_kwargs["database_metadata_current_db_only"] = False

    with redshift_connector.connect(**test_db_kwargs) as conn:
        with conn.cursor() as cursor:
            result: tuple = cursor.get_columns(catalog_name, None, None, None)

            assert len(result) > 0

            found_expected_row: bool = False
            for actual_row in result:
                if actual_row[0] == catalog_name and actual_row[1] == schema_name and actual_row[2] == table_name and actual_row[3] == col_name:
                    found_expected_row = True

            assert found_expected_row

