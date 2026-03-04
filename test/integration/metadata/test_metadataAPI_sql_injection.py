import configparser
import os
import typing

import pytest  # type: ignore

import redshift_connector
from redshift_connector.metadataAPIHelper import MetadataAPIHelper
from _password_generator import generate_password
from metadata_test_utils import setup_metadata_test_env, teardown_metadata_test_env, get_connection_properties


# Define constants
is_single_database_metadata_case: typing.List[bool] = [True, False]

catalog_name: str = "test_sql_injection -- comment"

schema_name: str = "test_schema; create table pwn1(i int);--"
table_name: str = "test_table; create table pwn2(i int);--"
table_name_child: str = "test_table; create table pwn2(i int);-- child"
col_name: str = "test_column; create table pwn3(i int);--"
user: str = "test_user"
user2: str = "test_user_2"
proc_name: str = "test_procedure; create table pwn4(i int);--"
proc_col_name: str = "test_procedure_col; create table pwn6(i int);--"
func_name: str = "test_function; create table pwn5(i int);--"
func_col_name: str = "test_function_col"
password: str = generate_password()

startup_stmts: typing.Tuple[str, ...] = (
    "DROP SCHEMA IF EXISTS \"{}\" CASCADE;".format(schema_name),
    "DROP USER IF EXISTS \"{}\";".format(user),
    "DROP USER IF EXISTS \"{}\";".format(user2),
    "CREATE SCHEMA \"{}\";".format(schema_name),
    "CREATE TABLE \"{}\".\"{}\" (\"{}\" INTEGER PRIMARY KEY);".format(schema_name, table_name, col_name),
    "CREATE TABLE \"{}\".\"{}\"(\"{}\" INTEGER, FOREIGN KEY (\"{}\") REFERENCES \"{}\".\"{}\"(\"{}\"))".format(schema_name, table_name_child, col_name, col_name, schema_name, table_name, col_name),
    "CREATE USER {} PASSWORD '{}';".format(user, password),
    "CREATE USER {} PASSWORD '{}';".format(user2, password),
    "GRANT SELECT ON \"{}\".\"{}\" TO {};".format(schema_name, table_name, user),
    "GRANT SELECT (\"{}\") ON \"{}\".\"{}\" TO {};".format(col_name, schema_name, table_name, user2),
    "CREATE OR REPLACE PROCEDURE \"{}\".\"{}\"(\"{}\" OUT int) AS $$ BEGIN NULL; END; $$ LANGUAGE plpgsql;".format(schema_name, proc_name, proc_col_name),
    "CREATE OR REPLACE FUNCTION \"{}\".\"{}\"(\"{}\" int) RETURNS int stable AS $$ select 1 $$ LANGUAGE sql;".format(schema_name, func_name, func_col_name),
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
    '"; DROP TABLE test_table_100; --"',
    "; DROP TABLE test_table_100; --",
]
class TestSQLInjection:
    @pytest.fixture(scope="class", autouse=True)
    def test_metadataAPI_config(self, request, db_kwargs):
        setup_metadata_test_env(db_kwargs, catalog_name, startup_stmts, True, False, catalog_name)

        def fin():
            teardown_metadata_test_env(db_kwargs, catalog_name, True)

        request.addfinalizer(fin)

    @pytest.mark.parametrize("test_input", test_cases)
    @pytest.mark.parametrize("is_single_database_metadata", is_single_database_metadata_case)
    def test_input_parameter_sql_injection(self, db_kwargs, test_input, is_single_database_metadata) -> None:
        test_db_kwargs = get_connection_properties(db_kwargs, catalog_name, is_single_database_metadata)
        with redshift_connector.connect(**test_db_kwargs) as conn:
            with conn.cursor() as cursor:
                try:
                    result: tuple = cursor.get_schemas(test_input, None)
                except Exception as e:
                    pytest.fail(f"Unexpected exception raised: {e}")

    @pytest.mark.parametrize(
        "test_case",
        [
            {
                "method_name": "get_schemas",
                "method_args": (catalog_name, schema_name),
                "expected_indices": {"schema": 0, "catalog": 1},
                "expected_values": {
                    "schema": schema_name,
                    "catalog": catalog_name
                }
            },
            {
                "method_name": "get_tables",
                "method_args": (catalog_name, schema_name, table_name),
                "expected_indices": {"catalog": 0, "schema": 1, "table": 2},
                "expected_values": {
                    "catalog": catalog_name,
                    "schema": schema_name,
                    "table": table_name
                }
            },
            {
                "method_name": "get_columns",
                "method_args": (catalog_name, schema_name, table_name, col_name),
                "expected_indices": {
                    "catalog": 0,
                    "schema": 1,
                    "table": 2,
                    "column": 3
                },
                "expected_values": {
                    "catalog": catalog_name,
                    "schema": schema_name,
                    "table": table_name,
                    "column": col_name
                }
            },
            {
                "method_name": "get_primary_keys",
                "method_args": (catalog_name, schema_name, table_name),
                "expected_indices": {
                    "catalog": 0,
                    "schema": 1,
                    "table": 2,
                    "column": 3
                },
                "expected_values": {
                    "catalog": catalog_name,
                    "schema": schema_name,
                    "table": table_name,
                    "column": col_name
                }
            },
            {
                "method_name": "get_imported_keys",
                "method_args": (catalog_name, schema_name, table_name_child),
                "expected_indices": {
                    "pk_catalog": 0,
                    "pk_schema": 1,
                    "pk_table": 2,
                    "pk_column": 3,
                    "fk_catalog": 4,
                    "fk_schema": 5,
                    "fk_table": 6,
                    "fk_column": 7
                },
                "expected_values": {
                    "pk_catalog": catalog_name,
                    "pk_schema": schema_name,
                    "pk_table": table_name,
                    "pk_column": col_name,
                    "fk_catalog": catalog_name,
                    "fk_schema": schema_name,
                    "fk_table": table_name_child,
                    "fk_column": col_name
                }
            },
            {
                "method_name": "get_exported_keys",
                "method_args": (catalog_name, schema_name, table_name),
                "expected_indices": {
                    "pk_catalog": 0,
                    "pk_schema": 1,
                    "pk_table": 2,
                    "pk_column": 3,
                    "fk_catalog": 4,
                    "fk_schema": 5,
                    "fk_table": 6,
                    "fk_column": 7
                },
                "expected_values": {
                    "pk_catalog": catalog_name,
                    "pk_schema": schema_name,
                    "pk_table": table_name,
                    "pk_column": col_name,
                    "fk_catalog": catalog_name,
                    "fk_schema": schema_name,
                    "fk_table": table_name_child,
                    "fk_column": col_name
                }
            },
            {
                "method_name": "get_best_row_identifier",
                "method_args": (catalog_name, schema_name, table_name, 1, True),
                "expected_indices": {
                    "scope": 0,
                    "column_name": 1
                },
                "expected_values": {
                    "scope": 1,
                    "column_name": col_name
                }
            },
            {
                "method_name": "get_column_privileges",
                "method_args": (catalog_name, schema_name, table_name, col_name),
                "expected_indices": {
                    "catalog": 0,
                    "schema": 1,
                    "table": 2,
                    "column": 3,
                    "grantor": 4,
                    "grantee": 5
                },
                "expected_values": {
                    "catalog": catalog_name,
                    "schema": schema_name,
                    "table": table_name,
                    "column": col_name,
                    "grantor": "awsuser",
                    "grantee": user2
                }
            },
            {
                "method_name": "get_table_privileges",
                "method_args": (catalog_name, schema_name, table_name),
                "expected_indices": {
                    "catalog": 0,
                    "schema": 1,
                    "table": 2,
                    "grantor": 3,
                    "grantee": 4
                },
                "expected_values": {
                    "catalog": catalog_name,
                    "schema": schema_name,
                    "table": table_name,
                    "grantor": "awsuser",
                    "grantee": user
                }
            },
            {
                "method_name": "get_procedures",
                "method_args": (catalog_name, schema_name, proc_name),
                "expected_indices": {
                    "catalog": 0,
                    "schema": 1,
                    "procedure": 2
                },
                "expected_values": {
                    "catalog": catalog_name,
                    "schema": schema_name,
                    "procedure": proc_name
                }
            },
            {
                "method_name": "get_procedure_columns",
                "method_args": (catalog_name, schema_name, proc_name, proc_col_name),
                "expected_indices": {
                    "catalog": 0,
                    "schema": 1,
                    "procedure": 2,
                    "column": 3
                },
                "expected_values": {
                    "catalog": catalog_name,
                    "schema": schema_name,
                    "procedure": proc_name,
                    "column": proc_col_name
                }
            },
            {
                "method_name": "get_functions",
                "method_args": (catalog_name, schema_name, func_name),
                "expected_indices": {
                    "catalog": 0,
                    "schema": 1,
                    "function": 2
                },
                "expected_values": {
                    "catalog": catalog_name,
                    "schema": schema_name,
                    "function": func_name
                }
            },
            {
                "method_name": "get_function_columns",
                "method_args": (catalog_name, schema_name, func_name, func_col_name),
                "expected_indices": {
                    "catalog": 0,
                    "schema": 1,
                    "function": 2,
                    "column": 3
                },
                "expected_values": {
                    "catalog": catalog_name,
                    "schema": schema_name,
                    "function": func_name,
                    "column": func_col_name
                }
            }
        ]
    )
    @pytest.mark.parametrize("is_single_database_metadata", is_single_database_metadata_case)
    def test_get_metadata_sql_injection(self, db_kwargs, test_case, is_single_database_metadata) -> None:
        test_db_kwargs = get_connection_properties(db_kwargs, catalog_name, is_single_database_metadata)

        with redshift_connector.connect(**test_db_kwargs) as conn:
            with conn.cursor() as cursor:
                try:
                    cursor_method = getattr(cursor, test_case["method_name"])
                    result: tuple = cursor_method(*test_case["method_args"])

                    assert len(result) > 0

                    found_expected_row: bool = False
                    for row in result:
                        matches = all(
                            row[idx] == test_case["expected_values"][col_name]
                            for col_name, idx in test_case["expected_indices"].items()
                        )
                        if matches:
                            found_expected_row = True
                            break

                    assert found_expected_row, f"Expected row not found for {test_case['method_name']}"
                except Exception as e:
                    pytest.fail(f"Unexpected exception raised in {test_case['method_name']}: {e}")
