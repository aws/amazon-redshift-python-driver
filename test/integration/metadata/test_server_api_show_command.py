import configparser
import os
import typing
import pytest  # type: ignore

import redshift_connector
from redshift_connector.metadataAPIHelper import MetadataAPIHelper

conf: configparser.ConfigParser = configparser.ConfigParser()
root_path: str = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
conf.read(root_path + "/config.ini")

@pytest.mark.parametrize("test_case", [
    {
        "query": "SHOW DATABASES;",
        "show_discovery_version": 2,
        "expected_columns": [
            "_SHOW_DATABASES_database_name"
        ]
    },
    {
        "query": "SHOW SCHEMAS FROM DATABASE test_catalog;",
        "show_discovery_version": 2,
        "expected_columns": [
            "_SHOW_SCHEMA_database_name",
            "_SHOW_SCHEMA_schema_name"
        ]
    },
    {
        "query": "SHOW TABLES FROM SCHEMA test_catalog.test_schema;",
        "show_discovery_version": 2,
        "expected_columns": [
            "_SHOW_TABLES_database_name",
            "_SHOW_TABLES_schema_name",
            "_SHOW_TABLES_table_name",
            "_SHOW_TABLES_table_type",
            "_SHOW_TABLES_remarks"
        ]
    },
    {
        "query": "SHOW COLUMNS FROM TABLE test_catalog.test_schema.test_table;",
        "show_discovery_version": 2,
        "expected_columns": [
            "_SHOW_COLUMNS_database_name",
            "_SHOW_COLUMNS_schema_name",
            "_SHOW_COLUMNS_table_name",
            "_SHOW_COLUMNS_column_name",
            "_SHOW_COLUMNS_ordinal_position",
            "_SHOW_COLUMNS_column_default",
            "_SHOW_COLUMNS_is_nullable",
            "_SHOW_COLUMNS_data_type",
            "_SHOW_COLUMNS_character_maximum_length",
            "_SHOW_COLUMNS_numeric_precision",
            "_SHOW_COLUMNS_numeric_scale",
            "_SHOW_COLUMNS_remarks"
        ]
    },
    {
        "query": "SHOW CONSTRAINTS PRIMARY KEYS FROM TABLE test_catalog.test_schema.test_table;",
        "show_discovery_version": 3,
        "expected_columns": [
            "_SHOW_CONSTRAINTS_PK_database_name",
            "_SHOW_CONSTRAINTS_PK_schema_name",
            "_SHOW_CONSTRAINTS_PK_table_name",
            "_SHOW_CONSTRAINTS_PK_column_name",
            "_SHOW_CONSTRAINTS_PK_key_seq",
            "_SHOW_CONSTRAINTS_PK_pk_name"
        ]
    },
    {
        "query": "SHOW CONSTRAINTS FOREIGN KEYS FROM TABLE test_catalog.test_schema.test_table;",
        "show_discovery_version": 3,
        "expected_columns": [
            "_SHOW_CONSTRAINTS_FK_pk_database_name",
            "_SHOW_CONSTRAINTS_FK_pk_schema_name",
            "_SHOW_CONSTRAINTS_FK_pk_table_name",
            "_SHOW_CONSTRAINTS_FK_pk_column_name",
            "_SHOW_CONSTRAINTS_FK_fk_database_name",
            "_SHOW_CONSTRAINTS_FK_fk_schema_name",
            "_SHOW_CONSTRAINTS_FK_fk_table_name",
            "_SHOW_CONSTRAINTS_FK_fk_column_name",
            "_SHOW_CONSTRAINTS_FK_key_seq",
            "_SHOW_CONSTRAINTS_FK_update_rule",
            "_SHOW_CONSTRAINTS_FK_delete_rule",
            "_SHOW_CONSTRAINTS_FK_fk_name",
            "_SHOW_CONSTRAINTS_FK_pk_name",
            "_SHOW_CONSTRAINTS_FK_deferrability"
        ]
    },
    {
        "query": "SHOW CONSTRAINTS FOREIGN KEYS EXPORTED FROM TABLE test_catalog.test_schema.test_table;",
        "show_discovery_version": 3,
        "expected_columns": [
            "_SHOW_CONSTRAINTS_FK_pk_database_name",
            "_SHOW_CONSTRAINTS_FK_pk_schema_name",
            "_SHOW_CONSTRAINTS_FK_pk_table_name",
            "_SHOW_CONSTRAINTS_FK_pk_column_name",
            "_SHOW_CONSTRAINTS_FK_fk_database_name",
            "_SHOW_CONSTRAINTS_FK_fk_schema_name",
            "_SHOW_CONSTRAINTS_FK_fk_table_name",
            "_SHOW_CONSTRAINTS_FK_fk_column_name",
            "_SHOW_CONSTRAINTS_FK_key_seq",
            "_SHOW_CONSTRAINTS_FK_update_rule",
            "_SHOW_CONSTRAINTS_FK_delete_rule",
            "_SHOW_CONSTRAINTS_FK_fk_name",
            "_SHOW_CONSTRAINTS_FK_pk_name",
            "_SHOW_CONSTRAINTS_FK_deferrability"
        ]
    },
    {
        "query": "SHOW COLUMN GRANTS ON TABLE test_catalog.test_schema.test_table;",
        "show_discovery_version": 3,
        "expected_columns": [
            "_SHOW_GRANT_database_name",
            "_SHOW_GRANT_schema_name",
            "_SHOW_GRANT_table_name",
            "_SHOW_GRANT_column_name",
            "_SHOW_GRANT_grantor_name",
            "_SHOW_GRANT_identity_name",
            "_SHOW_GRANT_privilege_type",
            "_SHOW_GRANT_admin_option"
        ]
    },
    {
        "query": "SHOW GRANTS ON TABLE test_catalog.test_schema.test_table;",
        "show_discovery_version": 3,
        "expected_columns": [
            "_SHOW_GRANT_database_name",
            "_SHOW_GRANT_schema_name",
            "_SHOW_GRANT_object_name",
            "_SHOW_GRANT_grantor_name",
            "_SHOW_GRANT_identity_name",
            "_SHOW_GRANT_privilege_type",
            "_SHOW_GRANT_admin_option"
        ]
    },
    {
        "query": "SHOW PROCEDURES FROM SCHEMA test_catalog.test_schema;",
        "show_discovery_version": 3,
        "expected_columns": [
            "_SHOW_PROCEDURES_database_name",
            "_SHOW_PROCEDURES_schema_name",
            "_SHOW_PROCEDURES_procedure_name",
            "_SHOW_PROCEDURES_return_type",
            "_SHOW_PROCEDURES_argument_list"
        ]
    },
    {
        "query": "SHOW PARAMETERS OF PROCEDURE test_catalog.test_schema.test_procedure();",
        "show_discovery_version": 3,
        "expected_columns": [
            "_SHOW_PARAMETERS_database_name",
            "_SHOW_PARAMETERS_schema_name",
            "_SHOW_PARAMETERS_procedure_name",
            "_SHOW_PARAMETERS_parameter_name",
            "_SHOW_PARAMETERS_parameter_type",
            "_SHOW_PARAMETERS_ordinal_position",
            "_SHOW_PARAMETERS_data_type",
            "_SHOW_PARAMETERS_character_maximum_length",
            "_SHOW_PARAMETERS_numeric_precision",
            "_SHOW_PARAMETERS_numeric_scale",
        ]
    },
    {
        "query": "SHOW FUNCTIONS FROM SCHEMA test_catalog.test_schema;",
        "show_discovery_version": 3,
        "expected_columns": [
            "_SHOW_FUNCTIONS_database_name",
            "_SHOW_FUNCTIONS_schema_name",
            "_SHOW_FUNCTIONS_function_name",
            "_SHOW_FUNCTIONS_return_type",
            "_SHOW_FUNCTIONS_argument_list"
        ]
    },
    {
        "query": "SHOW PARAMETERS OF FUNCTION test_catalog.test_schema.test_function();",
        "show_discovery_version": 3,
        "expected_columns": [
            "_SHOW_PARAMETERS_database_name",
            "_SHOW_PARAMETERS_schema_name",
            "_SHOW_PARAMETERS_function_name",
            "_SHOW_PARAMETERS_parameter_name",
            "_SHOW_PARAMETERS_parameter_type",
            "_SHOW_PARAMETERS_ordinal_position",
            "_SHOW_PARAMETERS_character_maximum_length",
            "_SHOW_PARAMETERS_data_type",
            "_SHOW_PARAMETERS_character_maximum_length",
            "_SHOW_PARAMETERS_numeric_precision",
            "_SHOW_PARAMETERS_numeric_scale",
        ]
    }
])
def test_show_metadata_columns(db_kwargs, test_case) -> None:
    with redshift_connector.connect(**db_kwargs) as conn:
        with conn.cursor() as cursor:
            if cursor.get_show_discovery_version() >= test_case["show_discovery_version"]:
                cursor.execute(test_case["query"])
                column = cursor.description

                col_set: typing.Set = set()
                for col in column:
                    col_set.add(col[0])

                mock_metadataAPIHelper: MetadataAPIHelper = MetadataAPIHelper()

                # Verify all expected columns are present
                for expected_column in test_case["expected_columns"]:
                    assert getattr(mock_metadataAPIHelper, expected_column) in col_set
