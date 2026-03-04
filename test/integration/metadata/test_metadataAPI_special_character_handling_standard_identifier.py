import typing

import pytest  # type: ignore

import redshift_connector
from _password_generator import generate_password
from metadata_test_utils import setup_metadata_test_env, teardown_metadata_test_env, get_connection_properties, ProcedureType, FunctionType


# Define constants
is_single_database_metadata_case: typing.List[bool] = [True, False]

current_catalog: str = "təst_cata好log$123standard"

object_name: str = "təst_n𝒜m好e$123standard"
function_col_name: str = "test_name123standard" # Hit error with unicode / $

# Use different name for primary key / foreign key test due to unicode issue in Redshift
# SIM: https://issues.amazon.com/issues/RedCat-2279
tem_object_name: str = "təst_nam好e$123standard"

user1_name: str = "təst_nam好e$123standard_user1"
user2_name: str = "təst_nam好e$123standard_user2"
password: str = generate_password()

startup_stmts: typing.Tuple[str, ...] = (
    "DROP SCHEMA IF EXISTS {} CASCADE;".format(object_name),
    "DROP USER IF EXISTS {};".format(user1_name),
    "DROP USER IF EXISTS {};".format(user2_name),
    "CREATE SCHEMA {};".format(object_name),
    "create table {}.{} ({} INT);".format(object_name, object_name, object_name),
    "CREATE SCHEMA {};".format(tem_object_name),
    "create table {}.{} ({} INT PRIMARY KEY );".format(tem_object_name, tem_object_name+"_parent", tem_object_name),
    "create table {}.{} ({} INT, FOREIGN KEY ({}) REFERENCES {}.{}({}));".format(tem_object_name,tem_object_name+"_child",tem_object_name,tem_object_name,tem_object_name,tem_object_name+"_parent",tem_object_name),
    "CREATE USER {} PASSWORD '{}';".format(user1_name, password),
    "CREATE USER {} PASSWORD '{}';".format(user2_name, password),
    "GRANT SELECT ({}) ON {}.{} TO {};".format(tem_object_name, tem_object_name, tem_object_name+"_parent", user1_name),
    "GRANT SELECT ON {}.{} TO {};".format(tem_object_name, tem_object_name+"_child", user2_name),
    "CREATE OR REPLACE PROCEDURE {}.{}({} OUT int) AS $$ BEGIN {} := 1; END; $$ LANGUAGE plpgsql;".format(tem_object_name, tem_object_name, tem_object_name, tem_object_name),
    # Hit error when create function with special character
    "CREATE OR REPLACE FUNCTION {}.{}({} int) RETURNS int stable AS $$ select 1 $$ LANGUAGE sql;".format(tem_object_name, tem_object_name, function_col_name)
)


class TestStandardIdentifier:
    @pytest.fixture(scope="class", autouse=True)
    def test_metadataAPI_config(self, request, db_kwargs):
        setup_metadata_test_env(db_kwargs, current_catalog, startup_stmts)

        def fin():
            teardown_metadata_test_env(db_kwargs, current_catalog)

        request.addfinalizer(fin)

    test_cases = [
        # Standard identifier with lower case
        ([current_catalog, object_name], 1,
         [object_name]),

        # Standard identifier with mixed case
        ([current_catalog, "təst_N𝒜m好e$123Standard"], 0,
         []),

        # Standard identifier with lower case + illegal special character
        ([current_catalog, "təst_n𝒜m好e$123!standard"], 0,
         []),
    ]

    @pytest.mark.parametrize("test_case, expected_row_count, expected_result", test_cases)
    @pytest.mark.parametrize("is_single_database_metadata", is_single_database_metadata_case)
    def test_get_schemas_special_character(self, db_kwargs, test_case, expected_row_count, expected_result, is_single_database_metadata) -> None:
        test_db_kwargs = get_connection_properties(db_kwargs, current_catalog, is_single_database_metadata)

        with redshift_connector.connect(**test_db_kwargs) as conn:
            with conn.cursor() as cursor:
                result: tuple = cursor.get_schemas(test_case[0], test_case[1])
                assert len(result) == expected_row_count
                for actual_row, expected_schema in zip(result, expected_result):
                    assert actual_row[0] == expected_schema
                    assert actual_row[1] == current_catalog

    @pytest.mark.parametrize("test_case, expected_row_count, expected_result", test_cases)
    @pytest.mark.parametrize("is_single_database_metadata", is_single_database_metadata_case)
    def test_get_tables_special_character(self, db_kwargs, test_case, expected_row_count, expected_result, is_single_database_metadata) -> None:
        test_db_kwargs = get_connection_properties(db_kwargs, current_catalog, is_single_database_metadata)

        with redshift_connector.connect(**test_db_kwargs) as conn:
            with conn.cursor() as cursor:
                result: tuple = cursor.get_tables(test_case[0], test_case[1], test_case[1], None)
                assert len(result) == expected_row_count
                for actual_row, expected_name in zip(result, expected_result):
                    assert actual_row[0] == current_catalog
                    assert actual_row[1] == expected_name
                    assert actual_row[2] == expected_name

    @pytest.mark.parametrize("test_case, expected_row_count, expected_result", test_cases)
    @pytest.mark.parametrize("is_single_database_metadata", is_single_database_metadata_case)
    def test_get_columns_special_character(self, db_kwargs, test_case, expected_row_count, expected_result, is_single_database_metadata) -> None:
        test_db_kwargs = get_connection_properties(db_kwargs, current_catalog, is_single_database_metadata)

        with redshift_connector.connect(**test_db_kwargs) as conn:
            with conn.cursor() as cursor:
                result: tuple = cursor.get_columns(test_case[0], test_case[1], test_case[1], test_case[1])
                assert len(result) == expected_row_count
                for actual_row, expected_name in zip(result, expected_result):
                    assert actual_row[0] == current_catalog
                    assert actual_row[1] == expected_name
                    assert actual_row[2] == expected_name
                    assert actual_row[3] == expected_name

    primary_key_test_cases = [
        # Standard identifier with lower case
        ([current_catalog, tem_object_name, tem_object_name+"_parent"], 1,
         [
             [current_catalog, tem_object_name, tem_object_name+"_parent", tem_object_name]
         ]),

        # Standard identifier with mixed case
        ([current_catalog, "təst_Nam好e$123Standard", tem_object_name+"_parent"], 0,
         []),

        # Standard identifier with lower case + illegal special character
        ([current_catalog, "təst_nam好e$123!standard", tem_object_name+"_parent"], 0,
         []),
    ]
    @pytest.mark.parametrize("test_case, expected_row_count, expected_result", primary_key_test_cases)
    @pytest.mark.parametrize("is_single_database_metadata", is_single_database_metadata_case)
    def test_get_primary_keys_special_character(self, db_kwargs, test_case, expected_row_count, expected_result, is_single_database_metadata) -> None:
        test_db_kwargs = get_connection_properties(db_kwargs, current_catalog, is_single_database_metadata)

        with redshift_connector.connect(**test_db_kwargs) as conn:
            with conn.cursor() as cursor:
                result: tuple = cursor.get_primary_keys(test_case[0], test_case[1], test_case[2])
                # Check the number of rows
                assert len(result) == expected_row_count

                # Validate the actual value in each rows
                for i in range(expected_row_count):
                    for j in range(len(expected_result[i])):
                        assert result[i][j] == expected_result[i][j]

    imported_key_test_cases = [
        # Standard identifier with lower case
        ([current_catalog, tem_object_name, tem_object_name+"_child"], 1,
         [
             [current_catalog, tem_object_name, tem_object_name+"_parent", tem_object_name, current_catalog, tem_object_name, tem_object_name+"_child", tem_object_name]
         ]),

        # Standard identifier with mixed case
        ([current_catalog, "təst_Nam好e$123Standard", tem_object_name+"_child"], 0,
         []),

        # Standard identifier with lower case + illegal special character
        ([current_catalog, "təst_nam好e$123!standard", tem_object_name+"_child"], 0,
         []),
    ]
    @pytest.mark.parametrize("test_case, expected_row_count, expected_result", imported_key_test_cases)
    @pytest.mark.parametrize("is_single_database_metadata", is_single_database_metadata_case)
    def test_get_imported_keys_special_character(self, db_kwargs, test_case, expected_row_count, expected_result, is_single_database_metadata) -> None:
        test_db_kwargs = get_connection_properties(db_kwargs, current_catalog, is_single_database_metadata)

        with redshift_connector.connect(**test_db_kwargs) as conn:
            with conn.cursor() as cursor:
                if cursor.get_show_discovery_version() >= 3:
                    result: tuple = cursor.get_imported_keys(test_case[0], test_case[1], test_case[2])
                    # Check the number of rows
                    assert len(result) == expected_row_count

                    # Validate the actual value in each rows
                    for i in range(expected_row_count):
                        for j in range(len(expected_result[i])):
                            assert result[i][j] == expected_result[i][j]
                else:
                    with pytest.raises(redshift_connector.InterfaceError,
                                       match="Current cluster doesn't support required SHOW command"):
                        cursor.get_imported_keys(test_case[0], test_case[1], test_case[2])

    exported_key_test_cases = [
        # Standard identifier with lower case
        ([current_catalog, tem_object_name, tem_object_name+"_parent"], 1,
         [
             [current_catalog, tem_object_name, tem_object_name+"_parent", tem_object_name, current_catalog, tem_object_name, tem_object_name+"_child", tem_object_name]
         ]),

        # Standard identifier with mixed case
        ([current_catalog, "təst_Nam好e$123Standard", tem_object_name+"_parent"], 0,
         []),

        # Standard identifier with lower case + illegal special character
        ([current_catalog, "təst_nam好e$123!standard", tem_object_name+"_parent"], 0,
         []),
    ]
    @pytest.mark.parametrize("test_case, expected_row_count, expected_result", exported_key_test_cases)
    @pytest.mark.parametrize("is_single_database_metadata", is_single_database_metadata_case)
    def test_get_exported_keys_special_character(self, db_kwargs, test_case, expected_row_count, expected_result, is_single_database_metadata) -> None:
        test_db_kwargs = get_connection_properties(db_kwargs, current_catalog, is_single_database_metadata)

        with redshift_connector.connect(**test_db_kwargs) as conn:
            with conn.cursor() as cursor:
                if cursor.get_show_discovery_version() >= 3:
                    result: tuple = cursor.get_exported_keys(test_case[0], test_case[1], test_case[2])
                    # Check the number of rows
                    assert len(result) == expected_row_count

                    # Validate the actual value in each rows
                    for i in range(expected_row_count):
                        for j in range(len(expected_result[i])):
                            assert result[i][j] == expected_result[i][j]
                else:
                    with pytest.raises(redshift_connector.InterfaceError,
                                       match="Current cluster doesn't support required SHOW command"):
                        cursor.get_exported_keys(test_case[0], test_case[1], test_case[2])

    best_row_identifier_test_cases = [
        # Standard identifier with lower case
        ([current_catalog, tem_object_name, tem_object_name+"_parent", 1, False], 1,
         [
             [1, tem_object_name, 4, 'int4', 10, None, 0, 1]
         ]),

        # Standard identifier with mixed case
        ([current_catalog, "təst_Nam好e$123Standard", tem_object_name+"_parent", 1, False], 0,
         []),

        # Standard identifier with lower case + illegal special character
        ([current_catalog, "təst_nam好e$123!standard", tem_object_name+"_parent", 1, False], 0,
         []),
    ]
    @pytest.mark.parametrize("test_case, expected_row_count, expected_result", best_row_identifier_test_cases)
    @pytest.mark.parametrize("is_single_database_metadata", is_single_database_metadata_case)
    def test_get_best_row_identifier_special_character(self, db_kwargs, test_case, expected_row_count, expected_result, is_single_database_metadata) -> None:
        test_db_kwargs = get_connection_properties(db_kwargs, current_catalog, is_single_database_metadata)

        with redshift_connector.connect(**test_db_kwargs) as conn:
            with conn.cursor() as cursor:
                if cursor.get_show_discovery_version() >= 3:
                    result: tuple = cursor.get_best_row_identifier(test_case[0], test_case[1], test_case[2], test_case[3], test_case[4])
                    # Check the number of rows
                    assert len(result) == expected_row_count

                    # Validate the actual value in each rows
                    for i in range(expected_row_count):
                        for j in range(len(expected_result[i])):
                            assert result[i][j] == expected_result[i][j]
                else:
                    with pytest.raises(redshift_connector.InterfaceError,
                                       match="Current cluster doesn't support required SHOW command"):
                        cursor.get_best_row_identifier(test_case[0], test_case[1], test_case[2], test_case[3], test_case[4])

    column_privileges_test_cases = [
        # Standard identifier with lower case
        ([current_catalog, tem_object_name, tem_object_name + "_parent", tem_object_name], 1,
         [
             [current_catalog, tem_object_name, tem_object_name + "_parent", tem_object_name, "awsuser", user1_name,
              "SELECT", "NO"],
         ]),

        # Standard identifier with mixed case
        ([current_catalog, "təst_Nam好e$123Standard", tem_object_name + "_parent", tem_object_name], 0,
         []),

        # Standard identifier with lower case + illegal special character
        ([current_catalog, "təst_nam好e$123!standard", tem_object_name + "_parent", tem_object_name], 0,
         []),

        # Standard identifier with lower case
        ([current_catalog, tem_object_name, tem_object_name + "_child", tem_object_name], 0,
         []),

        # Standard identifier with mixed case
        ([current_catalog, "təst_Nam好e$123Standard", tem_object_name + "_child", tem_object_name], 0,
         []),

        # Standard identifier with lower case + illegal special character
        ([current_catalog, "təst_nam好e$123!standard", tem_object_name + "_child", tem_object_name], 0,
         []),
    ]
    @pytest.mark.parametrize("test_case, expected_row_count, expected_result", column_privileges_test_cases)
    @pytest.mark.parametrize("is_single_database_metadata", is_single_database_metadata_case)
    def test_get_column_privileges_special_character(self, db_kwargs, test_case, expected_row_count, expected_result, is_single_database_metadata) -> None:
        test_db_kwargs = get_connection_properties(db_kwargs, current_catalog, is_single_database_metadata)

        with redshift_connector.connect(**test_db_kwargs) as conn:
            with conn.cursor() as cursor:
                if cursor.get_show_discovery_version() >= 3:
                    result: tuple = cursor.get_column_privileges(test_case[0], test_case[1], test_case[2], test_case[3])
                    # Check the number of rows
                    assert len(result) == expected_row_count

                    # Validate the actual value in each rows
                    for i in range(expected_row_count):
                        for j in range(len(expected_result[i])):
                            assert result[i][j] == expected_result[i][j]
                else:
                    with pytest.raises(redshift_connector.InterfaceError,
                                       match="Current cluster doesn't support required SHOW command"):
                        cursor.get_column_privileges(test_case[0], test_case[1], test_case[2], test_case[3])

    table_privileges_test_cases = [
        # Standard identifier with lower case
        ([current_catalog, tem_object_name, tem_object_name+"_parent"], 0,
         []),

        # Standard identifier with mixed case
        ([current_catalog, "təst_Nam好e$123Standard", tem_object_name+"_parent"], 0,
         []),

        # Standard identifier with lower case + illegal special character
        ([current_catalog, "təst_nam好e$123!standard", tem_object_name+"_parent"], 0,
         []),

        # Standard identifier with lower case
        ([current_catalog, tem_object_name, tem_object_name+"_child"], 1,
         [
            [current_catalog, tem_object_name, tem_object_name+"_child", "awsuser", user2_name, "SELECT", "NO"]
         ]),

        # Standard identifier with mixed case
        ([current_catalog, "təst_Nam好e$123Standard", tem_object_name+"_child"], 0,
         []),

        # Standard identifier with lower case + illegal special character
        ([current_catalog, "təst_nam好e$123!standard", tem_object_name+"_child"], 0,
         []),
    ]
    @pytest.mark.parametrize("test_case, expected_row_count, expected_result", table_privileges_test_cases)
    @pytest.mark.parametrize("is_single_database_metadata", is_single_database_metadata_case)
    def test_get_table_privileges_special_character(self, db_kwargs, test_case, expected_row_count, expected_result, is_single_database_metadata) -> None:
        test_db_kwargs = get_connection_properties(db_kwargs, current_catalog, is_single_database_metadata)

        with redshift_connector.connect(**test_db_kwargs) as conn:
            with conn.cursor() as cursor:
                if cursor.get_show_discovery_version() >= 3:
                    result: tuple = cursor.get_table_privileges(test_case[0], test_case[1], test_case[2])
                    # Check the number of rows
                    assert len(result) == expected_row_count

                    # Validate the actual value in each rows
                    for i in range(expected_row_count):
                        for j in range(len(expected_result[i])):
                            assert result[i][j] == expected_result[i][j]
                else:
                    with pytest.raises(redshift_connector.InterfaceError,
                                       match="Current cluster doesn't support required SHOW command"):
                        cursor.get_table_privileges(test_case[0], test_case[1], test_case[2])

    procedures_test_cases = [
        # Standard identifier with lower case
        ([current_catalog, tem_object_name, tem_object_name], 1,
         [
             [current_catalog, tem_object_name, tem_object_name, None, None, None, '', ProcedureType.RETURNS_RESULT]
         ]),

        # Standard identifier with mixed case
        ([current_catalog, "təst_Nam好e$123Standard", tem_object_name], 0,
         []),

        # Standard identifier with lower case + illegal special character
        ([current_catalog, "təst_nam好e$123!standard", tem_object_name], 0,
         []),
    ]
    @pytest.mark.parametrize("test_case, expected_row_count, expected_result", procedures_test_cases)
    @pytest.mark.parametrize("is_single_database_metadata", is_single_database_metadata_case)
    def test_get_procedures_special_character(self, db_kwargs, test_case, expected_row_count, expected_result, is_single_database_metadata) -> None:
        test_db_kwargs = get_connection_properties(db_kwargs, current_catalog, is_single_database_metadata)

        with redshift_connector.connect(**test_db_kwargs) as conn:
            with conn.cursor() as cursor:
                if cursor.get_show_discovery_version() >= 3:
                    result: tuple = cursor.get_procedures(test_case[0], test_case[1], test_case[2])
                    # Check the number of rows
                    assert len(result) == expected_row_count

                    # Validate the actual value in each rows
                    for i in range(expected_row_count):
                        for j in range(len(expected_result[i])):
                            assert result[i][j] == expected_result[i][j]
                else:
                    with pytest.raises(redshift_connector.InterfaceError,
                                       match="Current cluster doesn't support required SHOW command"):
                        cursor.get_procedures(test_case[0], test_case[1], test_case[2])

    procedure_columns_test_cases = [
        # Standard identifier with lower case
        ([current_catalog, tem_object_name, tem_object_name, tem_object_name], 1,
         [
             [current_catalog, tem_object_name, tem_object_name, tem_object_name],
         ]),

        # Standard identifier with mixed case
        ([current_catalog, "təst_Nam好e$123Standard", tem_object_name, tem_object_name], 0,
         []),

        # Standard identifier with lower case + illegal special character
        ([current_catalog, "təst_nam好e$123!standard", tem_object_name, tem_object_name], 0,
         []),
    ]
    @pytest.mark.parametrize("test_case, expected_row_count, expected_result", procedure_columns_test_cases)
    @pytest.mark.parametrize("is_single_database_metadata", is_single_database_metadata_case)
    def test_get_procedure_columns_special_character(self, db_kwargs, test_case, expected_row_count, expected_result, is_single_database_metadata) -> None:
        test_db_kwargs = get_connection_properties(db_kwargs, current_catalog, is_single_database_metadata)

        with redshift_connector.connect(**test_db_kwargs) as conn:
            with conn.cursor() as cursor:
                if cursor.get_show_discovery_version() >= 3:
                    result: tuple = cursor.get_procedure_columns(test_case[0], test_case[1], test_case[2], test_case[3])
                    # Check the number of rows
                    assert len(result) == expected_row_count

                    # Validate the actual value in each rows
                    for i in range(expected_row_count):
                        for j in range(len(expected_result[i])):
                            assert result[i][j] == expected_result[i][j]
                else:
                    with pytest.raises(redshift_connector.InterfaceError,
                                       match="Current cluster doesn't support required SHOW command"):
                        cursor.get_procedure_columns(test_case[0], test_case[1], test_case[2], test_case[3])

    functions_test_cases = [
        # Standard identifier with lower case
        ([current_catalog, tem_object_name, tem_object_name], 1,
         [
             [current_catalog, tem_object_name, tem_object_name, "", FunctionType.NOTABLE]
         ]),

        # Standard identifier with mixed case
        ([current_catalog, "təst_Nam好e$123Standard", tem_object_name], 0,
         []),

        # Standard identifier with lower case + illegal special character
        ([current_catalog, "təst_nam好e$123!standard", tem_object_name], 0,
         []),
    ]
    @pytest.mark.parametrize("test_case, expected_row_count, expected_result", functions_test_cases)
    @pytest.mark.parametrize("is_single_database_metadata", is_single_database_metadata_case)
    def test_get_functions_special_character(self, db_kwargs, test_case, expected_row_count, expected_result, is_single_database_metadata) -> None:
        test_db_kwargs = get_connection_properties(db_kwargs, current_catalog, is_single_database_metadata)

        with redshift_connector.connect(**test_db_kwargs) as conn:
            with conn.cursor() as cursor:
                if cursor.get_show_discovery_version() >= 3:
                    result: tuple = cursor.get_functions(test_case[0], test_case[1], test_case[2])
                    # Check the number of rows
                    assert len(result) == expected_row_count

                    # Validate the actual value in each rows
                    for i in range(expected_row_count):
                        for j in range(len(expected_result[i])):
                            assert result[i][j] == expected_result[i][j]
                else:
                    with pytest.raises(redshift_connector.InterfaceError,
                                       match="Current cluster doesn't support required SHOW command"):
                        cursor.get_functions(test_case[0], test_case[1], test_case[2])

    function_columns_test_cases = [
        # Standard identifier with lower case
        ([current_catalog, tem_object_name, tem_object_name, function_col_name], 1,
         [
             [current_catalog, tem_object_name, tem_object_name, function_col_name]
         ]),

        # Standard identifier with mixed case
        ([current_catalog, "təst_Nam好e$123Standard", tem_object_name, function_col_name], 0,
         []),

        # Standard identifier with lower case + illegal special character
        ([current_catalog, "təst_nam好e$123!standard", tem_object_name, function_col_name], 0,
         []),
    ]
    @pytest.mark.parametrize("test_case, expected_row_count, expected_result", function_columns_test_cases)
    @pytest.mark.parametrize("is_single_database_metadata", is_single_database_metadata_case)
    def test_get_function_columns_special_character(self, db_kwargs, test_case, expected_row_count, expected_result, is_single_database_metadata) -> None:
        test_db_kwargs = get_connection_properties(db_kwargs, current_catalog, is_single_database_metadata)

        with redshift_connector.connect(**test_db_kwargs) as conn:
            with conn.cursor() as cursor:
                if cursor.get_show_discovery_version() >= 3:
                    result: tuple = cursor.get_function_columns(test_case[0], test_case[1], test_case[2], test_case[3])
                    # Check the number of rows
                    assert len(result) == expected_row_count

                    # Validate the actual value in each rows
                    for i in range(expected_row_count):
                        for j in range(len(expected_result[i])):
                            assert result[i][j] == expected_result[i][j]
                else:
                    with pytest.raises(redshift_connector.InterfaceError,
                                       match="Current cluster doesn't support required SHOW command"):
                        cursor.get_function_columns(test_case[0], test_case[1], test_case[2], test_case[3])
