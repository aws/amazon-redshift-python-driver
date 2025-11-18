import typing

import pytest  # type: ignore

from redshift_connector.metadataAPIHelper import (
    FunctionColumnType,
    FunctionType,
    MetadataAPIHelper,
    ProcedureColumnType,
    ProcedureType,
)
from redshift_connector.utils.oids import RedshiftOID
from redshift_connector.error import (
    ProgrammingError
)

@pytest.fixture
def metadata_helper():
    return MetadataAPIHelper()

@pytest.mark.parametrize(
    "test_name, metadata_attribute, expected_columns, expected_types",
    [
        (
            "catalogs",
            "_get_catalogs_result_metadata",
            ["TABLE_CAT"],
            [RedshiftOID.VARCHAR]
        ),
        (
            "schemas",
            "_get_schemas_result_metadata",
            ["TABLE_SCHEM", "TABLE_CATALOG"],
            [RedshiftOID.VARCHAR, RedshiftOID.VARCHAR]
        ),
        (
            "tables",
            "_get_tables_result_metadata",
            ["TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE", "REMARKS",
             "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "SELF_REFERENCING_COL_NAME",
             "REF_GENERATION", "OWNER", "LAST_ALTERED_TIME", "LAST_MODIFIED_TIME",
             "DIST_STYLE", "TABLE_SUBTYPE"],
            [RedshiftOID.VARCHAR, RedshiftOID.VARCHAR, RedshiftOID.VARCHAR,
             RedshiftOID.VARCHAR, RedshiftOID.VARCHAR, RedshiftOID.VARCHAR,
             RedshiftOID.VARCHAR, RedshiftOID.VARCHAR, RedshiftOID.VARCHAR,
             RedshiftOID.VARCHAR, RedshiftOID.VARCHAR, RedshiftOID.TIMESTAMP,
             RedshiftOID.TIMESTAMP, RedshiftOID.VARCHAR, RedshiftOID.VARCHAR]
        ),
        (
            "columns",
            "_get_columns_result_metadata",
            ["TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE",
             "TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS",
             "NUM_PREC_RADIX", "NULLABLE", "REMARKS", "COLUMN_DEF", "SQL_DATA_TYPE",
             "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH", "ORDINAL_POSITION",
             "IS_NULLABLE", "SCOPE_CATALOG", "SCOPE_SCHEMA", "SCOPE_TABLE",
             "SOURCE_DATA_TYPE", "IS_AUTOINCREMENT", "IS_GENERATEDCOLUMN",
             "SORT_KEY_TYPE", "SORT_KEY", "DIST_KEY", "ENCODING", "COLLATION"],
            [RedshiftOID.VARCHAR, RedshiftOID.VARCHAR, RedshiftOID.VARCHAR,
             RedshiftOID.VARCHAR, RedshiftOID.INTEGER, RedshiftOID.VARCHAR,
             RedshiftOID.INTEGER, RedshiftOID.INTEGER, RedshiftOID.INTEGER,
             RedshiftOID.INTEGER, RedshiftOID.INTEGER, RedshiftOID.VARCHAR,
             RedshiftOID.VARCHAR, RedshiftOID.INTEGER, RedshiftOID.INTEGER,
             RedshiftOID.INTEGER, RedshiftOID.INTEGER, RedshiftOID.VARCHAR,
             RedshiftOID.VARCHAR, RedshiftOID.VARCHAR, RedshiftOID.VARCHAR,
             RedshiftOID.SMALLINT, RedshiftOID.VARCHAR, RedshiftOID.VARCHAR,
             RedshiftOID.VARCHAR, RedshiftOID.INTEGER, RedshiftOID.INTEGER,
             RedshiftOID.VARCHAR, RedshiftOID.VARCHAR]
        ),
        (
            "primary_keys",
            "_get_primary_keys_result_metadata",
            ["TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "KEY_SEQ",
             "PK_NAME"],
            [RedshiftOID.VARCHAR, RedshiftOID.VARCHAR, RedshiftOID.VARCHAR,
             RedshiftOID.VARCHAR, RedshiftOID.SMALLINT, RedshiftOID.VARCHAR]
        ),
        (
            "foreign_keys",
            "_get_foreign_keys_result_metadata",
            ["PKTABLE_CAT", "PKTABLE_SCHEM", "PKTABLE_NAME", "PKCOLUMN_NAME",
             "FKTABLE_CAT", "FKTABLE_SCHEM", "FKTABLE_NAME", "FKCOLUMN_NAME",
             "KEY_SEQ", "UPDATE_RULE", "DELETE_RULE", "FK_NAME", "PK_NAME",
             "DEFERRABILITY"],
            [RedshiftOID.VARCHAR, RedshiftOID.VARCHAR, RedshiftOID.VARCHAR,
             RedshiftOID.VARCHAR, RedshiftOID.VARCHAR, RedshiftOID.VARCHAR,
             RedshiftOID.VARCHAR, RedshiftOID.VARCHAR, RedshiftOID.SMALLINT,
             RedshiftOID.SMALLINT, RedshiftOID.SMALLINT, RedshiftOID.VARCHAR,
             RedshiftOID.VARCHAR, RedshiftOID.SMALLINT]
        ),
        (
            "best_row_identifier",
            "_get_best_row_identifier_result_metadata",
            ["SCOPE", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME", "COLUMN_SIZE",
             "BUFFER_LENGTH", "DECIMAL_DIGITS", "PSEUDO_COLUMN"],
            [RedshiftOID.SMALLINT, RedshiftOID.VARCHAR, RedshiftOID.INTEGER,
             RedshiftOID.VARCHAR, RedshiftOID.INTEGER, RedshiftOID.INTEGER,
             RedshiftOID.SMALLINT, RedshiftOID.SMALLINT]
        ),
        (
            "column_privileges",
            "_get_column_privileges_result_metadata",
            ["TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "GRANTOR",
             "GRANTEE", "PRIVILEGE", "IS_GRANTABLE"],
            [RedshiftOID.VARCHAR] * 8
        ),
        (
            "table_privileges",
            "_get_table_privileges_result_metadata",
            ["TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "GRANTOR", "GRANTEE",
             "PRIVILEGE", "IS_GRANTABLE"],
            [RedshiftOID.VARCHAR] * 7
        ),
        (
            "procedures",
            "_get_procedures_result_metadata",
            ["PROCEDURE_CAT", "PROCEDURE_SCHEM", "PROCEDURE_NAME", "RESERVE1",
             "RESERVE2", "RESERVE3", "REMARKS", "PROCEDURE_TYPE", "SPECIFIC_NAME"],
            [RedshiftOID.VARCHAR, RedshiftOID.VARCHAR, RedshiftOID.VARCHAR,
             RedshiftOID.VARCHAR, RedshiftOID.VARCHAR, RedshiftOID.VARCHAR,
             RedshiftOID.VARCHAR, RedshiftOID.SMALLINT, RedshiftOID.VARCHAR]
        ),
        (
            "procedure_columns",
            "_get_procedure_columns_result_metadata",
            ["PROCEDURE_CAT", "PROCEDURE_SCHEM", "PROCEDURE_NAME", "COLUMN_NAME",
             "COLUMN_TYPE", "DATA_TYPE", "TYPE_NAME", "PRECISION", "LENGTH",
             "SCALE", "RADIX", "NULLABLE", "REMARKS", "COLUMN_DEF",
             "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH",
             "ORDINAL_POSITION", "IS_NULLABLE", "SPECIFIC_NAME"],
            [RedshiftOID.VARCHAR, RedshiftOID.VARCHAR, RedshiftOID.VARCHAR,
             RedshiftOID.VARCHAR, RedshiftOID.SMALLINT, RedshiftOID.INTEGER,
             RedshiftOID.VARCHAR, RedshiftOID.INTEGER, RedshiftOID.INTEGER,
             RedshiftOID.SMALLINT, RedshiftOID.SMALLINT, RedshiftOID.SMALLINT,
             RedshiftOID.VARCHAR, RedshiftOID.VARCHAR, RedshiftOID.INTEGER,
             RedshiftOID.INTEGER, RedshiftOID.INTEGER, RedshiftOID.INTEGER,
             RedshiftOID.VARCHAR, RedshiftOID.VARCHAR]
        ),
        (
            "functions",
            "_get_functions_result_metadata",
            ["FUNCTION_CAT", "FUNCTION_SCHEM", "FUNCTION_NAME", "REMARKS",
             "FUNCTION_TYPE", "SPECIFIC_NAME"],
            [RedshiftOID.VARCHAR, RedshiftOID.VARCHAR, RedshiftOID.VARCHAR,
             RedshiftOID.VARCHAR, RedshiftOID.SMALLINT, RedshiftOID.VARCHAR]
        ),
        (
            "function_columns",
            "_get_function_columns_result_metadata",
            ["FUNCTION_CAT", "FUNCTION_SCHEM", "FUNCTION_NAME", "COLUMN_NAME",
             "COLUMN_TYPE", "DATA_TYPE", "TYPE_NAME", "PRECISION", "LENGTH",
             "SCALE", "RADIX", "NULLABLE", "REMARKS", "CHAR_OCTET_LENGTH",
             "ORDINAL_POSITION", "IS_NULLABLE", "SPECIFIC_NAME"],
            [RedshiftOID.VARCHAR, RedshiftOID.VARCHAR, RedshiftOID.VARCHAR,
             RedshiftOID.VARCHAR, RedshiftOID.SMALLINT, RedshiftOID.INTEGER,
             RedshiftOID.VARCHAR, RedshiftOID.INTEGER, RedshiftOID.INTEGER,
             RedshiftOID.SMALLINT, RedshiftOID.SMALLINT, RedshiftOID.SMALLINT,
             RedshiftOID.VARCHAR, RedshiftOID.INTEGER, RedshiftOID.INTEGER,
             RedshiftOID.VARCHAR, RedshiftOID.VARCHAR]
        ),
        (
            "table_types",
            "_get_table_type_result_metadata",
            ["TABLE_TYPE"],
            [RedshiftOID.VARCHAR]
        )
    ],
    ids=["get_catalogs", "get_schemas", "get_tables", "get_columns", "get_primary_keys", "foreign_keys",
         "get_best_row_identifier", "get_column_privileges", "get_table_privileges",
         "get_procedures", "get_procedure_columns", "get_functions", "get_function_columns",
         "get_table_types"]
)
def test_metadata_api_columns(metadata_helper,
    test_name: str,
    metadata_attribute: str,
    expected_columns: list,
    expected_types: list) -> None:

    metadata_cols: typing.Dict = getattr(metadata_helper, metadata_attribute).columns
    metadata_column_count: int = getattr(metadata_helper, metadata_attribute).column_count

    assert len(metadata_cols) == metadata_column_count
    assert len(metadata_cols) == len(expected_columns)
    assert len(metadata_cols) == len(expected_types)

    for col_name, expected_type in zip(expected_columns, expected_types):
        assert metadata_cols[col_name] == expected_type


@pytest.mark.parametrize(
    "data_type, rs_type, cus_precision, precisions",
    list(
        zip(
            [
                "time without time zone",
                "time with time zone (3)",
                "timestamp without time zone (4)",
                "timestamp with time zone (5)",
                "interval day to second",
                "interval day to second (4)",
            ],
            ["time", "timetz", "timestamp", "timestamptz", "intervald2s", "intervald2s"],
            [False, True, True, True, False, True],
            [6, 3, 4, 5, 6, 4],
        )
    ),
)
def test_get_second_fraction(data_type, rs_type, cus_precision, precisions) -> None:
    mock_metadataAPIHelper: MetadataAPIHelper = MetadataAPIHelper()
    test_rs_type, test_cus_precision, test_precisions = mock_metadataAPIHelper.get_second_fraction(data_type)
    assert test_rs_type == rs_type
    assert test_cus_precision is cus_precision
    assert test_precisions == precisions


def test_get_nullable() -> None:
    mock_metadataAPIHelper: MetadataAPIHelper = MetadataAPIHelper.__new__(MetadataAPIHelper)

    assert 1 == mock_metadataAPIHelper.get_nullable("YES")
    assert 0 == mock_metadataAPIHelper.get_nullable("NO")
    assert 2 == mock_metadataAPIHelper.get_nullable("Cannot determine")


def test_get_auto_increment() -> None:
    mock_metadataAPIHelper: MetadataAPIHelper = MetadataAPIHelper.__new__(MetadataAPIHelper)

    assert "YES" == mock_metadataAPIHelper.get_auto_increment("\"identity\"(211928, 0, '1,1'::text)")
    assert "YES" == mock_metadataAPIHelper.get_auto_increment("default_identity")
    assert "NO" == mock_metadataAPIHelper.get_auto_increment("")
    assert "NO" == mock_metadataAPIHelper.get_auto_increment(None)


def test_is_none_or_empty() -> None:
    mock_metadataAPIHelper: MetadataAPIHelper = MetadataAPIHelper.__new__(MetadataAPIHelper)

    assert True == mock_metadataAPIHelper.is_none_or_empty(None)
    assert True == mock_metadataAPIHelper.is_none_or_empty("")

    assert False == mock_metadataAPIHelper.is_none_or_empty(" ")
    assert False == mock_metadataAPIHelper.is_none_or_empty("%")
    assert False == mock_metadataAPIHelper.is_none_or_empty("_")
    assert False == mock_metadataAPIHelper.is_none_or_empty("exactName")
    assert False == mock_metadataAPIHelper.is_none_or_empty("pattern_name")
    assert False == mock_metadataAPIHelper.is_none_or_empty("patternName%")


def test_get_column_size() -> None:
    mock_metadataAPIHelper: MetadataAPIHelper = MetadataAPIHelper.__new__(MetadataAPIHelper)

    numeric_precision: int = 10
    character_maximum_length: int = 200
    assert numeric_precision == mock_metadataAPIHelper.get_column_size(
        "numeric", numeric_precision, character_maximum_length
    )
    assert character_maximum_length == mock_metadataAPIHelper.get_column_size(
        "varchar", numeric_precision, character_maximum_length
    )

    assert None == mock_metadataAPIHelper.get_column_size("super", numeric_precision, character_maximum_length)
    assert None == mock_metadataAPIHelper.get_column_size("geometry", numeric_precision, character_maximum_length)
    assert None == mock_metadataAPIHelper.get_column_size("geography", numeric_precision, character_maximum_length)
    assert None == mock_metadataAPIHelper.get_column_size("varbyte", numeric_precision, character_maximum_length)


def test_get_decimal_digits() -> None:
    mock_metadataAPIHelper: MetadataAPIHelper = MetadataAPIHelper.__new__(MetadataAPIHelper)

    numeric_scale: int = 6
    precision: int = 4
    customizePrecision: bool = True

    assert 8 == mock_metadataAPIHelper.get_decimal_digits("float4", numeric_scale, precision, customizePrecision)
    assert 17 == mock_metadataAPIHelper.get_decimal_digits("float8", numeric_scale, precision, customizePrecision)
    assert numeric_scale == mock_metadataAPIHelper.get_decimal_digits(
        "numeric", numeric_scale, precision, customizePrecision
    )
    assert precision == mock_metadataAPIHelper.get_decimal_digits(
        "intervald2s", numeric_scale, precision, customizePrecision
    )

    assert None == mock_metadataAPIHelper.get_decimal_digits("super", numeric_scale, precision, customizePrecision)
    assert None == mock_metadataAPIHelper.get_decimal_digits("geometry", numeric_scale, precision, customizePrecision)
    assert None == mock_metadataAPIHelper.get_decimal_digits("geography", numeric_scale, precision, customizePrecision)
    assert None == mock_metadataAPIHelper.get_decimal_digits("varbyte", numeric_scale, precision, customizePrecision)

    assert 0 == mock_metadataAPIHelper.get_decimal_digits(
        "other_datatype", numeric_scale, precision, customizePrecision
    )


def test_get_num_prefix_radix() -> None:
    mock_metadataAPIHelper: MetadataAPIHelper = MetadataAPIHelper.__new__(MetadataAPIHelper)
    binary_datatype_radix: int = 2
    default_radix: int = 10

    assert binary_datatype_radix == mock_metadataAPIHelper.get_num_prefix_radix("geography")
    assert binary_datatype_radix == mock_metadataAPIHelper.get_num_prefix_radix("varbyte")

    assert default_radix == mock_metadataAPIHelper.get_num_prefix_radix("other_datatype")


@pytest.mark.parametrize("name, argument_list, expected_result, should_raise", [
    ("proc_name", "arg1, arg2", "proc_name(arg1, arg2)", False),
    ("func_name", "", "func_name()", False),
    ("test_proc", None, "test_proc()", False),
    ("", "arg1, arg2", None, True),
    (None, "arg1, arg2", None, True),
])
def test_get_specific_name(name: str, argument_list: str,
                           expected_result: str, should_raise: bool):
    """
    Test get_specific_name with various inputs

    Args:
        name: Function/procedure name
        argument_list: List of arguments
        expected_result: Expected output string
        should_raise: Whether the input should raise an exception
    """
    if should_raise:
        with pytest.raises(ProgrammingError):
            MetadataAPIHelper.get_specific_name(name, argument_list)
    else:
        result = MetadataAPIHelper.get_specific_name(name, argument_list)
        assert result == expected_result


@pytest.mark.parametrize("argument_list, sql_base, column_name_pattern, expected_result", [
    (
        "integer, varchar, float",
        "SHOW PARAMETERS OF PROCEDURE %s.%s.%s",
        None,
        ("SHOW PARAMETERS OF PROCEDURE %s.%s.%s(%s, %s, %s);", ["integer", "varchar", "float"])
    ),
    (
        "integer",
        "SHOW PARAMETERS OF PROCEDURE %s.%s.%s",
        "test%",
        ("SHOW PARAMETERS OF PROCEDURE %s.%s.%s(%s) LIKE %s;", ["integer"])
    ),
    (
        "",
        "SHOW PARAMETERS OF PROCEDURE %s.%s.%s",
        None,
        ("SHOW PARAMETERS OF PROCEDURE %s.%s.%s();", [])
    ),
    (
        None,
        "SHOW PARAMETERS OF PROCEDURE %s.%s.%s",
        None,
        ("SHOW PARAMETERS OF PROCEDURE %s.%s.%s();", [])
    ),
    (
        "int, float, character varying, timestamp with time zone",  # Test whitespace handling
        "SHOW PARAMETERS OF PROCEDURE %s.%s.%s",
        None,
        ("SHOW PARAMETERS OF PROCEDURE %s.%s.%s(%s, %s, %s, %s);", ["int", "float", "character varying", "timestamp with time zone"])
    ),
])
def test_create_parameterized_query_string(metadata_helper,
                                       argument_list: str,
                                       sql_base: str,
                                       column_name_pattern: str,
                                       expected_result: typing.Tuple[str, typing.List[str]]):
    """
    Test create_parameterized_query_string with various inputs

    Args:
        argument_list: Comma-separated argument list
        sql_base: Base SQL query
        column_name_pattern: Optional pattern for LIKE clause
        expected_result: Tuple of (expected SQL string, expected argument list)
    """
    result_sql, result_args = metadata_helper.create_parameterized_query_string(
        argument_list, sql_base, column_name_pattern
    )
    expected_sql, expected_args = expected_result
    assert result_sql == expected_sql
    assert result_args == expected_args


@pytest.mark.parametrize("argument_list, sql_base, column_name_pattern, expected_error", [
    # Invalid data types
    (
            "invalid_type, varchar",
            "SHOW PARAMETERS OF PROCEDURE %s.%s.%s",
            None,
            ValueError("Invalid data type(s) in argument list: invalid_type")
    ),
    (
            "integer, wrong_type, varchar, bad_type",
            "SHOW PARAMETERS OF PROCEDURE %s.%s.%s",
            None,
            ValueError("Invalid data type(s) in argument list: wrong_type, bad_type")
    ),

    # Malformed argument lists
    (
            "integer,,varchar",
            "SHOW PARAMETERS OF PROCEDURE %s.%s.%s",
            None,
            ValueError("Invalid data type(s) in argument list: ")
    ),
    (
            "integer, , varchar",
            "SHOW PARAMETERS OF PROCEDURE %s.%s.%s",
            None,
            ValueError("Empty argument found in argument list")
    ),
    (
            ",integer",
            "SHOW PARAMETERS OF PROCEDURE %s.%s.%s",
            None,
            ValueError("Invalid data type(s) in argument list: ")
    ),
    (
            "integer,",
            "SHOW PARAMETERS OF PROCEDURE %s.%s.%s",
            None,
            ValueError("Invalid data type(s) in argument list: ")
    )
])
def test_create_parameterized_query_string_invalid(
        metadata_helper,
        argument_list: str,
        sql_base: str,
        column_name_pattern: str,
        expected_error: Exception
):
    """
    Test create_parameterized_query_string with invalid inputs

    Args:
        argument_list: Comma-separated argument list
        sql_base: Base SQL query
        column_name_pattern: Optional pattern for LIKE clause
        expected_error: Expected exception to be raised
    """
    with pytest.raises(type(expected_error)) as exc_info:
        metadata_helper.create_parameterized_query_string(
            argument_list, sql_base, column_name_pattern
        )
    assert str(expected_error) in str(exc_info.value)


@pytest.mark.parametrize("return_type, expected_type", [
    ("varchar", ProcedureType.RETURNS_RESULT),
    ("integer", ProcedureType.RETURNS_RESULT),
    (None, ProcedureType.NO_RESULT),
    ("", ProcedureType.NO_RESULT)
])
def test_get_procedure_type(return_type: typing.Optional[str],
                            expected_type: ProcedureType):
    """
    Test get_procedure_type with various return types

    Args:
        return_type: Return type of the procedure
        expected_type: Expected ProcedureType enum value
    """
    mock_metadataAPIHelper: MetadataAPIHelper = MetadataAPIHelper.__new__(MetadataAPIHelper)
    result = mock_metadataAPIHelper.get_procedure_type(return_type)
    assert result == expected_type


@pytest.mark.parametrize("return_type, expected_type", [
    ("record", FunctionType.RETURNS_TABLE),
    ("varchar", FunctionType.NOTABLE),
    ("integer", FunctionType.NOTABLE),
    (None, FunctionType.NOTABLE),
    ("", FunctionType.NOTABLE),
])
def test_get_function_type(return_type: typing.Optional[str],
                           expected_type: FunctionType):
    """
    Test get_function_type with various return types

    Args:
        return_type: Return type of the function
        expected_type: Expected FunctionType enum value
    """
    mock_metadataAPIHelper: MetadataAPIHelper = MetadataAPIHelper.__new__(MetadataAPIHelper)
    result = mock_metadataAPIHelper.get_function_type(return_type)
    assert result == expected_type


@pytest.mark.parametrize("parameter_type, expected_type", [
    ("IN", ProcedureColumnType.IN),
    ("in", ProcedureColumnType.IN),
    ("INOUT", ProcedureColumnType.IN_OUT),
    ("inout", ProcedureColumnType.IN_OUT),
    ("OUT", ProcedureColumnType.OUT),
    ("out", ProcedureColumnType.OUT),
    ("TABLE", ProcedureColumnType.RESULT),
    ("table", ProcedureColumnType.RESULT),
    ("RETURN", ProcedureColumnType.RETURN),
    ("return", ProcedureColumnType.RETURN),
    (None, ProcedureColumnType.UNKNOWN),
    ("", ProcedureColumnType.UNKNOWN),
    ("integer", ProcedureColumnType.UNKNOWN),
])
def test_get_procedure_column_type(metadata_helper,
                                   parameter_type: typing.Optional[str],
                                   expected_type: ProcedureColumnType):
    """
    Test get_procedure_column_type with various parameter types

    Args:
        parameter_type: Input parameter type
        expected_type: Expected ProcedureColumnType enum value
    """
    result = metadata_helper.get_procedure_column_type(parameter_type)
    assert result == expected_type


@pytest.mark.parametrize("parameter_type, expected_type", [
    ("IN", FunctionColumnType.IN),
    ("in", FunctionColumnType.IN),
    ("INOUT", FunctionColumnType.IN_OUT),
    ("inout", FunctionColumnType.IN_OUT),
    ("OUT", FunctionColumnType.OUT),
    ("out", FunctionColumnType.OUT),
    ("TABLE", FunctionColumnType.RESULT),
    ("table", FunctionColumnType.RESULT),
    ("RETURN", FunctionColumnType.RETURN),
    ("return", FunctionColumnType.RETURN),
    (None, FunctionColumnType.UNKNOWN),
    ("", FunctionColumnType.UNKNOWN),
    ("integer", FunctionColumnType.UNKNOWN),  # Test case sensitivity
])
def test_get_function_column_type(metadata_helper,
                                  parameter_type: typing.Optional[str],
                                  expected_type: FunctionColumnType):
    """
    Test get_function_column_type with various parameter types

    Args:
        parameter_type: Input parameter type
        expected_type: Expected FunctionColumnType enum value
    """
    result = metadata_helper.get_function_column_type(parameter_type)
    assert result == expected_type

TEST_CASES = [
    # Empty pattern tests
    ("", "", True),
    ("hello", "", True),
    ("123", "", True),

    # Pattern with only % tests
    ("", "%", True),
    ("hello", "%", True),
    ("hello", "%%%", True),
    ("123!@#", "%%%%", True),

    # Empty string tests with non-empty patterns
    ("", "a", False),
    ("", "_", False),
    ("", "a%", False),
    ("", "%a", False),

    # Single character tests
    ("a", "a", True),
    ("a", "b", False),
    ("a", "_", True),
    ("a", "%", True),

    # Underscore tests
    ("hello", "_ello", True),
    ("hello", "h_llo", True),
    ("hello", "he_lo", True),
    ("hello", "hel_o", True),
    ("hello", "hell_", True),
    ("hello", "_____", True),
    ("hello", "____", False),
    ("hello", "______", False),

    # Percent tests
    ("hello", "h%", True),
    ("hello", "%o", True),
    ("hello", "h%o", True),
    ("hello", "%l%", True),
    ("hello", "h%l%o", True),
    ("hello", "%hello%", True),
    ("hello", "%hel%", True),
    ("hello", "%h%l%o%", True),

    # Mixed underscore and percent tests
    ("hello", "h_%o", True),
    ("hello", "h_%", True),
    ("hello", "%_l%", True),
    ("hello", "_el%", True),
    ("hello", "%l_o", True),

    # Negative test cases
    ("hello", "world", False),
    ("hello", "hell", False),
    ("hello", "hello!", False),
    ("hello", "h%x", False),
    ("hello", "h_x", False),

    # Special characters tests
    ("h.llo", "h.llo", True),
    ("h*llo", "h*llo", True),
    ("h[llo", "h[llo", True),
    ("h#llo", "h#llo", True),
    ("h@llo", "_@_lo", True),

    # Case sensitivity tests
    ("Hello", "hello", False),
    ("HELLO", "hello", False),
    ("Hello", "H%o", True),
    ("HELLO", "H_LLO", True),

    # Number tests
    ("123", "123", True),
    ("123", "1%3", True),
    ("123", "1_3", True),
    ("123", "%2%", True),

    # Long string tests
    ("HelloWorld123", "Hello%123", True),
    ("HelloWorld123", "Hello_%_123", True),
    ("HelloWorld123", "%World%", True),
    ("HelloWorld123", "HelloWorld124", False),

    # === ADDITIONAL EDGE CASES ===

    # Multiple consecutive wildcards
    ("abc", "a%%b%c", True),
    ("abc", "a___%c", False),
    ("abc", "%_%_%", True),
    ("a", "%%_%", True),
    ("", "%%", True),

    # Pattern longer than string
    ("hi", "hello", False),
    ("hi", "h%ello", False),
    ("hi", "h_llo", False),
    ("a", "ab", False),

    # String longer than pattern
    ("hello", "hi", False),
    ("hello", "h", False),
    ("hello", "he", False),

    # Patterns starting/ending with wildcards
    ("%hello", "%hello", True),
    ("hello%", "hello%", True),
    ("hello", "%hello", True),
    ("hello", "hello%", True),
    ("hello", "%ello", True),
    ("hello", "hell%", True),

    # Complex mixed patterns
    ("database_table_name", "database_%_name", True),
    ("database_table_name", "database_%name", True),
    ("database_table_name", "data%table%", True),
    ("database_table_name", "data%_table_%", True),
    ("database_table_name", "_atabase_table_nam_", True),

    # Whitespace and special characters
    ("hello world", "hello%world", True),
    ("hello world", "hello_world", True),
    ("hello  world", "hello__world", True),
    ("hello\tworld", "hello_world", True),
    ("hello\nworld", "hello_world", True),

    # Unicode and special characters
    ("café", "caf_", True),
    ("naïve", "na_ve", True),
    ("résumé", "r_sum_", True),
    ("test@email.com", "test%email%", True),
    ("file.txt", "%.txt", True),
    ("file.txt", "file.%", True),

    # SQL injection-like patterns (should be handled safely)
    ("'; DROP TABLE", "%DROP%", True),
    ("'; DROP TABLE", "_%TABLE", True),
    ("admin'--", "admin%", True),

    # Numeric patterns
    ("12345", "1%5", True),
    ("12345", "1_3_5", True),
    ("12345", "%3%", True),
    ("12.34", "%.%", True),
    ("12.34", "12._4", True),

    # Repeated characters
    ("aaaa", "a%", True),
    ("aaaa", "a%a", True),
    ("aaaa", "aa%", True),
    ("aaaa", "%aa", True),
    ("aaaa", "a_a_", True),
    ("aaaa", "____", True),
    ("aaaa", "_____", False),

    # Edge cases with single wildcards
    ("a", "_%", True),
    ("a", "%_", True),
    ("ab", "_%", True),
    ("ab", "%_", True),
    ("ab", "_%_", True),

    # Patterns that should fail
    ("hello", "h%x%o", False),
    ("hello", "h_x_o", False),
    ("hello", "%x%", False),
    ("hello", "_x_", False),

    # Very specific patterns
    ("column_name", "column%", True),
    ("column_name", "%name", True),
    ("column_name", "column_name", True),
    ("column_name", "column_%", True),
    ("column_name", "%_name", True),
    ("column_name", "col%nam%", True),

    # Boundary conditions
    ("x", "x%", True),
    ("x", "%x", True),
    ("x", "_", True),
    ("xy", "x_", True),
    ("xy", "_y", True),

    # Multiple underscores with percent
    ("hello", "h_%_%o", True),
    ("hello", "h%_%o", True),
    ("hello", "%_%_%", True),
    ("he", "%_%", True),
    ("h", "%_%", True),

    # Patterns with literal % and _ (escaped)
    # Note: These test the unescape_backslash functionality
    ("hello%world", "hello\\%world", True),
    ("hello_world", "hello\\_world", True),
    ("test\\file", "test\\\\file", True),
]


@pytest.mark.parametrize("test_str,pattern,expected", TEST_CASES)
def test_pattern_match(metadata_helper, test_str: str, pattern: str, expected: bool):
    """
    Parameterized test for pattern matching function
    """
    result = metadata_helper.pattern_match(test_str, pattern)
    assert result == expected, (
        f"Failed for string: '{test_str}' with pattern: '{pattern}'. "
        f"Expected: {expected}, Got: {result}"
    )


def test_very_long_string(metadata_helper):
    """
    Test pattern matching with very long strings
    """
    long_str = "a" * 1000  # String of 1000 'a's

    assert metadata_helper.pattern_match(long_str, "%")
    assert metadata_helper.pattern_match(long_str, "a%a")
    assert metadata_helper.pattern_match(long_str, "a" * 1000)
    assert not metadata_helper.pattern_match(long_str, "a" * 1001)


def test_consecutive_wildcards(metadata_helper):
    """
    Test pattern matching with consecutive wildcard characters
    """
    assert metadata_helper.pattern_match("hello", "h%_%o")
    assert metadata_helper.pattern_match("hello", "h_%_%o")
    assert metadata_helper.pattern_match("hello", "h%_%_%o")
