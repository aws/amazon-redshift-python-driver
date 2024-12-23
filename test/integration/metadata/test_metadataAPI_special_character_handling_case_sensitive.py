import configparser
import os
import typing
import pytest  # type: ignore

import redshift_connector
from redshift_connector.metadataAPIHelper import MetadataAPIHelper

conf: configparser.ConfigParser = configparser.ConfigParser()
root_path: str = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
conf.read(root_path + "/config.ini")

current_catalog: str = "dev"

schema_name: str = "\"sChema`~!@#$%^&*()_+-={}|[]:\"\";,./<>?'\\\""

enable_case_sensitive: str = "SET enable_case_sensitive_identifier TO true;"
disable_case_sensitive: str = "SET enable_case_sensitive_identifier TO false;"

startup_stmts: typing.Tuple[str, ...] = (
    enable_case_sensitive,
    "DROP SCHEMA IF EXISTS {} CASCADE;".format(schema_name),
    "CREATE SCHEMA {};".format(schema_name),
    "create table {}.{} (\"%cOl1\" INT);".format(schema_name, "\"test\\_Table\\_1\""),
    "create table {}.{} (\"%cOl1\" INT, \"%cOl2\" INT);".format(schema_name, "\"test_Table_2\""),
    "create table {}.{} (\"%cOl1\" INT, \"%cOl2\" INT, \"%cOl3\" INT);".format(schema_name, "\"test_/Table_/3\""),
    "create table {}.{} (\"%cOl1\" INT, \"%cOl2\" INT, \"%cOl3\" INT, \"%cOl4\" INT);".format(schema_name, "\"test_%Table_%4\""),
    "create table {}.{} (\"%cOl1\" INT, \"%cOl2\" INT, \"%cOl3\" INT, \"%cOl4\" INT, \"%cOl5\" INT);".format(schema_name, "\"test_ Table_ 5\""),
    "create table {}.{} (\"%cOl1\" INT);".format(schema_name, "\"ÖhNo\""),
    "create table {}.{} (\"%cOl1\" INT);".format(schema_name, "\"OhNÖ\""),
    "create table {}.{} (\"%cOl1\" INT);".format(schema_name, "\"OhÖNo\""),
    "create table {}.{} (\"%cOl1\" INT);".format(schema_name, "\"\"\"heLlo\""),
    "create table {}.{} (\"%cOl1\" INT);".format(schema_name, "\"heLlo\"\"\""),
    "create table {}.{} (\"%cOl1\" INT);".format(schema_name, "\"he\"\"Llo\""),
    "create table {}.{} (\"%cOl1\" INT);".format(schema_name, "\"\"\"heLlo\"\"\""),
    "create table {}.{} (\"%cOl1\" INT);".format(schema_name, "\"\"\"gRoup\"\"\""),
)

shutdown_stmts: typing.Tuple[str, ...] = (
    enable_case_sensitive,
    "DROP SCHEMA IF EXISTS {} CASCADE;".format(schema_name),
    disable_case_sensitive,
)

test_cases = [
    (current_catalog, "schema`~!@#$%^&*()_+-={}|[]:\";,./<>?'\\\\", "test_Table_1", "%", 0, 0),
    (current_catalog, "schema`~!@#$%^&*()_+-={}|[]:\";,./<>?'\\\\", "test\\_Table\\_1", "%", 0, 0),
    (current_catalog, "schema`~!@#$%^&*()_+-={}|[]:\";,./<>?'\\\\", "test\\\\_Table\\\\_1", "%", 0, 0),
    (current_catalog, "sChema`~!@#$%^&*()_+-={}|[]:\";,./<>?'\\\\", "test_Table_1", "%", 0, 0),
    (current_catalog, "sChema`~!@#$%^&*()_+-={}|[]:\";,./<>?'\\\\", "test\\_Table\\_1", "%", 0, 0),
    (current_catalog, "sChema`~!@#$%^&*()_+-={}|[]:\";,./<>?'\\\\", "test\\\\_Table\\\\_1", "%", 1, 1),

    (current_catalog, "schema`~!@#$%^&*()_+-={}|[]:\";,./<>?'\\\\", "test_Table_2", "%", 0, 0),
    (current_catalog, "schema`~!@#$%^&*()_+-={}|[]:\";,./<>?'\\\\", "test\\_Table\\_2", "%", 0, 0),
    (current_catalog, "sChema`~!@#$%^&*()_+-={}|[]:\";,./<>?'\\\\", "test_Table_2", "%", 1, 2),
    (current_catalog, "sChema`~!@#$%^&*()_+-={}|[]:\";,./<>?'\\\\", "test\\_Table\\_2", "%", 1, 2),

    (current_catalog, "schema`~!@#$%^&*()_+-={}|[]:\";,./<>?'\\\\", "test_/Table_/3", "%", 0, 0),
    (current_catalog, "schema`~!@#$%^&*()_+-={}|[]:\";,./<>?'\\\\", "test\\_/Table\\_/3", "%", 0, 0),
    (current_catalog, "sChema`~!@#$%^&*()_+-={}|[]:\";,./<>?'\\\\", "test_/Table_/3", "%", 1, 3),
    (current_catalog, "sChema`~!@#$%^&*()_+-={}|[]:\";,./<>?'\\\\", "test\\_/Table\\_/3", "%", 1, 3),

    (current_catalog, "schema`~!@#$%^&*()_+-={}|[]:\";,./<>?'\\\\", "test_%Table_%4", "%", 0, 0),
    (current_catalog, "schema`~!@#$%^&*()_+-={}|[]:\";,./<>?'\\\\", "test\\_%Table\\_%4", "%", 0, 0),
    (current_catalog, "sChema`~!@#$%^&*()_+-={}|[]:\";,./<>?'\\\\", "test_%Table_%4", "%", 1, 4),
    (current_catalog, "sChema`~!@#$%^&*()_+-={}|[]:\";,./<>?'\\\\", "test\\_%Table\\_%4", "%", 1, 4),

    (current_catalog, "schema`~!@#$%^&*()_+-={}|[]:\";,./<>?'\\\\", "test_ Table_ 5", "%", 0, 0),
    (current_catalog, "schema`~!@#$%^&*()_+-={}|[]:\";,./<>?'\\\\", "test\\_ Table\\_ 5", "%", 0, 0),
    (current_catalog, "sChema`~!@#$%^&*()_+-={}|[]:\";,./<>?'\\\\", "test_ Table_ 5", "%", 1, 5),
    (current_catalog, "sChema`~!@#$%^&*()_+-={}|[]:\";,./<>?'\\\\", "test\\_ Table\\_ 5", "%", 1, 5),

    (current_catalog, "schema`~!@#$%^&*()_+-={}|[]:\";,./<>?'\\\\", "ÖhNo", "%", 0, 0),
    (current_catalog, "schema`~!@#$%^&*()_+-={}|[]:\";,./<>?'\\\\", "OhNÖ", "%", 0, 0),
    (current_catalog, "schema`~!@#$%^&*()_+-={}|[]:\";,./<>?'\\\\", "ohNÖ", "%", 0, 0),
    (current_catalog, "schema`~!@#$%^&*()_+-={}|[]:\";,./<>?'\\\\", "OhÖNo", "%", 0, 0),
    (current_catalog, "schema`~!@#$%^&*()_+-={}|[]:\";,./<>?'\\\\", "OhÖno", "%", 0, 0),
    (current_catalog, "sChema`~!@#$%^&*()_+-={}|[]:\";,./<>?'\\\\", "ÖhNo", "%", 1, 1),
    (current_catalog, "sChema`~!@#$%^&*()_+-={}|[]:\";,./<>?'\\\\", "OhNÖ", "%", 1, 1),
    (current_catalog, "sChema`~!@#$%^&*()_+-={}|[]:\";,./<>?'\\\\", "ohNÖ", "%", 0, 0),
    (current_catalog, "sChema`~!@#$%^&*()_+-={}|[]:\";,./<>?'\\\\", "OhÖNo", "%", 1, 1),
    (current_catalog, "sChema`~!@#$%^&*()_+-={}|[]:\";,./<>?'\\\\", "OhÖno", "%", 0, 0),

    (current_catalog, "schema`~!@#$%^&*()_+-={}|[]:\";,./<>?'\\\\", "\"heLlo", "%", 0, 0),
    (current_catalog, "schema`~!@#$%^&*()_+-={}|[]:\";,./<>?'\\\\", "\"hello", "%", 0, 0),
    (current_catalog, "schema`~!@#$%^&*()_+-={}|[]:\";,./<>?'\\\\", "heLlo\"", "%", 0, 0),
    (current_catalog, "schema`~!@#$%^&*()_+-={}|[]:\";,./<>?'\\\\", "hello\"", "%", 0, 0),
    (current_catalog, "schema`~!@#$%^&*()_+-={}|[]:\";,./<>?'\\\\", "he\"Llo", "%", 0, 0),
    (current_catalog, "schema`~!@#$%^&*()_+-={}|[]:\";,./<>?'\\\\", "he\"llo", "%", 0, 0),
    (current_catalog, "sChema`~!@#$%^&*()_+-={}|[]:\";,./<>?'\\\\", "\"heLlo", "%", 1, 1),
    (current_catalog, "sChema`~!@#$%^&*()_+-={}|[]:\";,./<>?'\\\\", "\"hello", "%", 0, 0),
    (current_catalog, "sChema`~!@#$%^&*()_+-={}|[]:\";,./<>?'\\\\", "heLlo\"", "%", 1, 1),
    (current_catalog, "sChema`~!@#$%^&*()_+-={}|[]:\";,./<>?'\\\\", "hello\"", "%", 0, 0),
    (current_catalog, "sChema`~!@#$%^&*()_+-={}|[]:\";,./<>?'\\\\", "he\"Llo", "%", 1, 1),
    (current_catalog, "sChema`~!@#$%^&*()_+-={}|[]:\";,./<>?'\\\\", "he\"llo", "%", 0, 0),

    (current_catalog, "schema`~!@#$%^&*()_+-={}|[]:\";,./<>?'\\\\", "\"heLlo\"", "%", 0, 0),
    (current_catalog, "schema`~!@#$%^&*()_+-={}|[]:\";,./<>?'\\\\", "\"hello\"", "%", 0, 0),
    (current_catalog, "schema`~!@#$%^&*()_+-={}|[]:\";,./<>?'\\\\", "\"gRoup\"", "%", 0, 0),
    (current_catalog, "sChema`~!@#$%^&*()_+-={}|[]:\";,./<>?'\\\\", "\"heLlo\"", "%", 1, 1),
    (current_catalog, "sChema`~!@#$%^&*()_+-={}|[]:\";,./<>?'\\\\", "\"hello\"", "%", 0, 0),
    (current_catalog, "sChema`~!@#$%^&*()_+-={}|[]:\";,./<>?'\\\\", "\"gRoup\"", "%", 1, 1),
]

@pytest.fixture(scope="class", autouse=True)
def test_metadataAPI_config(request, db_kwargs):
    with redshift_connector.connect(**db_kwargs) as con:
        con.paramstyle = "format"
        with con.cursor() as cursor:
            for stmt in startup_stmts:
                cursor.execute(stmt)

            con.commit()
    def fin():
        try:
            with redshift_connector.connect(**db_kwargs) as con:
                with con.cursor() as cursor:
                    for stmt in shutdown_stmts:
                        cursor.execute(stmt)
                con.commit()
        except redshift_connector.ProgrammingError:
            pass

    request.addfinalizer(fin)

@pytest.mark.parametrize("catalog, schema, table_name, column_name, expected_tables_count, expected_columns_count", test_cases)
def test_get_tables_special_character(db_kwargs, catalog, schema, table_name, column_name, expected_tables_count, expected_columns_count) -> None:
    with redshift_connector.connect(**db_kwargs) as conn:
        with conn.cursor() as cursor:
            cursor.execute(enable_case_sensitive)
            result: tuple = cursor.get_tables(catalog, schema, table_name, None)
            assert len(result) == expected_tables_count


@pytest.mark.parametrize("catalog, schema, table_name, column_name, expected_tables_count, expected_columns_count", test_cases)
def test_get_columns_special_character(db_kwargs, catalog, schema, table_name, column_name, expected_tables_count, expected_columns_count) -> None:
    with redshift_connector.connect(**db_kwargs) as conn:
        with conn.cursor() as cursor:
            cursor.execute(enable_case_sensitive)
            result: tuple = cursor.get_columns(catalog, schema, table_name, column_name)
            assert len(result) == expected_columns_count

