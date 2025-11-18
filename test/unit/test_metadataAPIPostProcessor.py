import pytest  # type: ignore
import typing
from collections import deque
from redshift_connector.utils.oids import RedshiftOID
from redshift_connector.metadataAPIPostProcessor import MetadataAPIPostProcessor
from redshift_connector.metadataAPIHelper import (
    ProcedureColumnType,
    FunctionColumnType
)
from redshift_connector import Connection, Cursor, DataError, InterfaceError
from enum import IntEnum


# Define constant
class MetadataAPI(IntEnum):
    get_catalogs = 0
    get_schemas = 1
    get_tables = 2
    get_columns = 3
    get_primary_keys = 4
    get_foreign_keys = 5
    get_best_row_identifier = 6
    get_column_privileges = 7
    get_table_privileges = 8
    get_procedures = 9
    get_procedure_columns = 10
    get_functions = 11
    get_function_columns = 12
    get_table_types = 13


colName: typing.List[typing.List[str]] = [["TABLE_CAT"],
           ["TABLE_SCHEM", "TABLE_CATALOG"],
           ["TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE", "REMARKS",
            "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "SELF_REFERENCING_COL_NAME", "REF_GENERATION",
            "OWNER", "LAST_ALTERED_TIME", "LAST_MODIFIED_TIME", "DIST_STYLE", "TABLE_SUBTYPE"],
           ["TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE",
	        "TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS", "NUM_PREFIX_RADIX",
	        "NULLABLE", "REMARKS", "COLUMN_DEF", "SQL_DATA_TYPE", "SQL_DATETIME_SUB",
	        "CHAR_OCTET_LENGTH", "ORDINAL_POSITION", "IS_NULLABLE", "SCOPE_CATALOG", "SCOPE_SCHEMA",
	        "SCOPE_TABLE", "SOURCE_DATA_TYPE", "IS_AUTOINCREMENT", "IS_GENERATEDCOLUMN",
            "SORT_KEY_TYPE", "SORT_KEY", "DIST_KEY", "ENCODING", "COLLATION"],
           ["TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "KEY_SEQ", "PK_NAME"],
           ["PKTABLE_CAT", "PKTABLE_SCHEM", "PKTABLE_NAME", "PKCOLUMN_NAME",
            "FKTABLE_CAT", "FKTABLE_SCHEM", "FKTABLE_NAME", "FKCOLUMN_NAME",
            "KEY_SEQ", "UPDATE_RULE", "DELETE_RULE", "FK_NAME", "PK_NAME",
            "DEFERRABILITY"],
           ["SCOPE", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME", "COLUMN_SIZE",
             "BUFFER_LENGTH", "DECIMAL_DIGITS", "PSEUDO_COLUMN"],
           ["TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "GRANTOR",
             "GRANTEE", "PRIVILEGE", "IS_GRANTABLE"],
           ["TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "GRANTOR", "GRANTEE",
             "PRIVILEGE", "IS_GRANTABLE"],
           ["PROCEDURE_CAT", "PROCEDURE_SCHEM", "PROCEDURE_NAME", "RESERVE1",
             "RESERVE2", "RESERVE3", "REMARKS", "PROCEDURE_TYPE", "SPECIFIC_NAME"],
           ["PROCEDURE_CAT", "PROCEDURE_SCHEM", "PROCEDURE_NAME", "COLUMN_NAME",
             "COLUMN_TYPE", "DATA_TYPE", "TYPE_NAME", "PRECISION", "LENGTH",
             "SCALE", "RADIX", "NULLABLE", "REMARKS", "COLUMN_DEF",
             "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH",
             "ORDINAL_POSITION", "IS_NULLABLE", "SPECIFIC_NAME"],
           ["FUNCTION_CAT", "FUNCTION_SCHEM", "FUNCTION_NAME", "REMARKS",
             "FUNCTION_TYPE", "SPECIFIC_NAME"],
           ["FUNCTION_CAT", "FUNCTION_SCHEM", "FUNCTION_NAME", "COLUMN_NAME",
             "COLUMN_TYPE", "DATA_TYPE", "TYPE_NAME", "PRECISION", "LENGTH",
             "SCALE", "RADIX", "NULLABLE", "REMARKS", "CHAR_OCTET_LENGTH",
             "ORDINAL_POSITION", "IS_NULLABLE", "SPECIFIC_NAME"],
           ["TABLE_TYPE"]
           ]

colType = [[RedshiftOID.VARCHAR],
           [RedshiftOID.VARCHAR, RedshiftOID.VARCHAR],
           [RedshiftOID.VARCHAR, RedshiftOID.VARCHAR, RedshiftOID.VARCHAR, RedshiftOID.VARCHAR, RedshiftOID.VARCHAR,
            RedshiftOID.VARCHAR, RedshiftOID.VARCHAR, RedshiftOID.VARCHAR, RedshiftOID.VARCHAR, RedshiftOID.VARCHAR,
            RedshiftOID.VARCHAR, RedshiftOID.TIMESTAMP, RedshiftOID.TIMESTAMP, RedshiftOID.VARCHAR, RedshiftOID.VARCHAR],
           [RedshiftOID.VARCHAR, RedshiftOID.VARCHAR, RedshiftOID.VARCHAR, RedshiftOID.VARCHAR, RedshiftOID.INTEGER,
            RedshiftOID.VARCHAR, RedshiftOID.INTEGER, RedshiftOID.INTEGER, RedshiftOID.INTEGER, RedshiftOID.INTEGER,
	        RedshiftOID.INTEGER, RedshiftOID.VARCHAR, RedshiftOID.VARCHAR, RedshiftOID.INTEGER, RedshiftOID.INTEGER,
            RedshiftOID.INTEGER, RedshiftOID.INTEGER, RedshiftOID.VARCHAR, RedshiftOID.VARCHAR, RedshiftOID.VARCHAR,
	        RedshiftOID.VARCHAR, RedshiftOID.SMALLINT, RedshiftOID.VARCHAR, RedshiftOID.VARCHAR, RedshiftOID.VARCHAR,
            RedshiftOID.INTEGER, RedshiftOID.INTEGER, RedshiftOID.VARCHAR, RedshiftOID.VARCHAR],
           [RedshiftOID.VARCHAR, RedshiftOID.VARCHAR, RedshiftOID.VARCHAR,
            RedshiftOID.VARCHAR, RedshiftOID.SMALLINT, RedshiftOID.VARCHAR],
           [RedshiftOID.VARCHAR, RedshiftOID.VARCHAR, RedshiftOID.VARCHAR,
             RedshiftOID.VARCHAR, RedshiftOID.VARCHAR, RedshiftOID.VARCHAR,
             RedshiftOID.VARCHAR, RedshiftOID.VARCHAR, RedshiftOID.SMALLINT,
             RedshiftOID.SMALLINT, RedshiftOID.SMALLINT, RedshiftOID.VARCHAR,
             RedshiftOID.VARCHAR, RedshiftOID.SMALLINT],
           [RedshiftOID.SMALLINT, RedshiftOID.VARCHAR, RedshiftOID.INTEGER,
             RedshiftOID.VARCHAR, RedshiftOID.INTEGER, RedshiftOID.INTEGER,
             RedshiftOID.SMALLINT, RedshiftOID.SMALLINT],
           [RedshiftOID.VARCHAR] * 8,
           [RedshiftOID.VARCHAR] * 7,
           [RedshiftOID.VARCHAR, RedshiftOID.VARCHAR, RedshiftOID.VARCHAR,
             RedshiftOID.VARCHAR, RedshiftOID.VARCHAR, RedshiftOID.VARCHAR,
             RedshiftOID.VARCHAR, RedshiftOID.SMALLINT, RedshiftOID.VARCHAR],
           [RedshiftOID.VARCHAR, RedshiftOID.VARCHAR, RedshiftOID.VARCHAR,
             RedshiftOID.VARCHAR, RedshiftOID.SMALLINT, RedshiftOID.INTEGER,
             RedshiftOID.VARCHAR, RedshiftOID.INTEGER, RedshiftOID.INTEGER,
             RedshiftOID.SMALLINT, RedshiftOID.SMALLINT, RedshiftOID.SMALLINT,
             RedshiftOID.VARCHAR, RedshiftOID.VARCHAR, RedshiftOID.INTEGER,
             RedshiftOID.INTEGER, RedshiftOID.INTEGER, RedshiftOID.INTEGER,
             RedshiftOID.VARCHAR, RedshiftOID.VARCHAR],
           [RedshiftOID.VARCHAR, RedshiftOID.VARCHAR, RedshiftOID.VARCHAR,
             RedshiftOID.VARCHAR, RedshiftOID.SMALLINT, RedshiftOID.VARCHAR],
           [RedshiftOID.VARCHAR, RedshiftOID.VARCHAR, RedshiftOID.VARCHAR,
             RedshiftOID.VARCHAR, RedshiftOID.SMALLINT, RedshiftOID.INTEGER,
             RedshiftOID.VARCHAR, RedshiftOID.INTEGER, RedshiftOID.INTEGER,
             RedshiftOID.SMALLINT, RedshiftOID.SMALLINT, RedshiftOID.SMALLINT,
             RedshiftOID.VARCHAR, RedshiftOID.INTEGER, RedshiftOID.INTEGER,
             RedshiftOID.VARCHAR, RedshiftOID.VARCHAR],
           [RedshiftOID.VARCHAR]
           ]


expected_result_get_columns = [
    ["testCatalog", "testSchema", "testTable", "testColumnNameSmallint", 5, "int2", 5, None, 0, 10, 1, "testRemarks", "testColumnDefault", 5, None, 5, 1, "YES", None, None, None, None, "NO", "NO"],
    ["testCatalog", "testSchema", "testTable", "testColumnNameInteger", 4, "int4", 10, None, 0, 10, 0, "testRemarks", "\"identity\"(211928, 0, '1,1'::text)", 4, None, 10, 2, "NO", None, None, None, None, "YES", "YES"],
    ["testCatalog", "testSchema", "testTable", "testColumnNameBigint", -5, "int8", 19, None, 0, 10, 1, "testRemarks", "testColumnDefault", -5, None, 19, 3, "YES", None, None, None, None, "NO", "NO"],
    ["testCatalog", "testSchema", "testTable", "testColumnNameNumeric", 2, "numeric", 10, None, 5, 10, 1, "testRemarks", None, 2, None, 10, 4, "YES", None, None, None, None, "NO", "NO"],
    ["testCatalog", "testSchema", "testTable", "testColumnNameReal", 7, "float4", 8, None, 8, 10, 1, "testRemarks", "testColumnDefault", 7, None, 8, 5, "YES", None, None, None, None, "NO", "NO"],
    ["testCatalog", "testSchema", "testTable", "testColumnNameDouble", 8, "float8", 17, None, 17, 10, 1, "testRemarks", "testColumnDefault", 8, None, 17, 6, "YES", None, None, None, None, "NO", "NO"],
    ["testCatalog", "testSchema", "testTable", "testColumnNameBoolean", -7, "bool", 1, None, 0, 10, 1, "testRemarks", "testColumnDefault", -7, None, 1, 7, "YES", None, None, None, None, "NO", "NO"],
    ["testCatalog", "testSchema", "testTable", "testColumnNameChar", 1, "char", 20, None, 0, 10, 1, "testRemarks", "testColumnDefault", 1, None, 20, 8, "YES", None, None, None, None, "NO", "NO"],
    ["testCatalog", "testSchema", "testTable", "testColumnNameVarchar", 12, "varchar", 256, None, 0, 10, 1, "testRemarks", "testColumnDefault", 12, None, 256, 9, "YES", None, None, None, None, "NO", "NO"],
    ["testCatalog", "testSchema", "testTable", "testColumnNameDate", 91, "date", 13, None, 0, 10, 1, "testRemarks", "testColumnDefault", 91, None, 13, 10, "YES", None, None, None, None, "NO", "NO"],
    ["testCatalog", "testSchema", "testTable", "testColumnNameTime", 92, "time", 15, None, 1, 10, 1, "testRemarks", "testColumnDefault", 92, None, 15, 11, "YES", None, None, None, None, "NO", "NO"],
    ["testCatalog", "testSchema", "testTable", "testColumnNameTimetz", 2013, "timetz", 21, None, 2, 10, 1, "testRemarks", "testColumnDefault", 2013, None, 21, 12, "YES", None, None, None, None, "NO", "NO"],
    ["testCatalog", "testSchema", "testTable", "testColumnNameTimestamp", 93, "timestamp", 29, None, 3, 10, 1, "testRemarks", "testColumnDefault", 93, None, 29, 13, "YES", None, None, None, None, "NO", "NO"],
    ["testCatalog", "testSchema", "testTable", "testColumnNameTimestamptz", 2014, "timestamptz", 35, None, 6, 10, 1, "testRemarks", "testColumnDefault", 2014, None, 35, 14, "YES", None, None, None, None, "NO", "NO"],
    ["testCatalog", "testSchema", "testTable", "testColumnNameIntervaly2m", 1111, "intervaly2m", 32, None, 0, 10, 1, "testRemarks", "testColumnDefault", 1111, None, 32, 15, "YES", None, None, None, None, "NO", "NO"],
    ["testCatalog", "testSchema", "testTable", "testColumnNameIntervald2s", 1111, "intervald2s", 64, None, 4, 10, 1, "testRemarks", "testColumnDefault", 1111, None, 64, 16, "YES", None, None, None, None, "NO", "NO"]
]

def show_databases_column() -> typing.Dict:
    return {'database_name': int(RedshiftOID.VARCHAR),
            'database_owner': int(RedshiftOID.INTEGER),
            'database_type': int(RedshiftOID.VARCHAR),
            'database_acl': int(RedshiftOID.VARCHAR),
            'parameters': int(RedshiftOID.VARCHAR),
            'database_isolation_level': int(RedshiftOID.VARCHAR)}

def show_databases_column_extra() -> typing.Dict:
    return {'database_name': int(RedshiftOID.VARCHAR),
            'database_owner': int(RedshiftOID.INTEGER),
            'database_type': int(RedshiftOID.VARCHAR),
            'database_acl': int(RedshiftOID.VARCHAR),
            'parameters': int(RedshiftOID.VARCHAR),
            'database_isolation_level': int(RedshiftOID.VARCHAR),
            'extra_1': int(RedshiftOID.VARCHAR),
            'extra_2': int(RedshiftOID.VARCHAR),
            'extra_3': int(RedshiftOID.INTEGER),
            'extra_4': int(RedshiftOID.INTEGER)}

def show_databases_column_diff_order() -> typing.Dict:
    return {'database_isolation_level': int(RedshiftOID.VARCHAR),
            'database_name': int(RedshiftOID.VARCHAR),
            'database_owner': int(RedshiftOID.INTEGER),
            'database_type': int(RedshiftOID.VARCHAR),
            'database_acl': int(RedshiftOID.VARCHAR),
            'parameters': int(RedshiftOID.VARCHAR)}


SHOW_DATABASES_COL: typing.List[typing.Dict] = [show_databases_column(), show_databases_column_extra(), show_databases_column_diff_order()]
SHOW_DATABASES_RES: typing.List[typing.List] = [['testCatalog','1','share',None,None,None], ['testCatalog','1','share',None,None,None,'extra_1','extra_2','1','2'], [None,'testCatalog','1','share',None,None,]]

@pytest.mark.parametrize("show_databases_col, show_databases_res", list(zip(SHOW_DATABASES_COL, SHOW_DATABASES_RES)))
def test_get_catalogs_post_processing(show_databases_col, show_databases_res) -> None:
    mock_cursor: Cursor = Cursor.__new__(Cursor)
    mock_cursor.paramstyle = "mocked"
    mock_connection: Connection = Connection.__new__(Connection)
    mock_connection.parameter_statuses = deque(maxlen=100)
    mock_connection.parameter_statuses.append((b"show_discovery", 0))
    mock_cursor._c = mock_connection
    mock_cursor.ps = {}
    mock_cursor._SHOW_DATABASES_Col_index = {}
    mock_metadataAPIPostProcessor: MetadataAPIPostProcessor = MetadataAPIPostProcessor(mock_cursor)

    # Create Result set + column description to mock the Result return from Server API call
    mock_metadataAPIPostProcessor.set_row_description(show_databases_col) # Create the column description

    intermediate_rs: typing.Tuple = ()                                      # Create result set
    intermediate_row: typing.List = show_databases_res                      # Create result row
    intermediate_rs += (intermediate_row,)

    intermediate_column = mock_cursor.description                           # Create the column name/index mapping
    for col, i in zip(intermediate_column, range(len(intermediate_column))):
        mock_cursor._SHOW_DATABASES_Col_index[col[0]] = int(i)

    # Apply post-processing
    final_rs: typing.Tuple = mock_metadataAPIPostProcessor.get_catalogs_post_processing(intermediate_rs)

    # Verify the final result
    final_column = mock_cursor.description
    assert len(final_column) == len(colName[MetadataAPI.get_catalogs])
    assert len(final_column) == len(colType[MetadataAPI.get_catalogs])

    expected_result = ["testCatalog"]
    for row in final_rs:
        for res1, res2 in zip(row, expected_result):
            assert res1 == res2


def show_schemas_column() -> typing.Dict:
    return {'database_name': int(RedshiftOID.VARCHAR),
            'schema_name': int(RedshiftOID.VARCHAR),
            'schema_owner': int(RedshiftOID.INTEGER),
            'schema_type': int(RedshiftOID.VARCHAR),
            'schema_acl': int(RedshiftOID.VARCHAR),
            'source_database': int(RedshiftOID.VARCHAR),
            'schema_option': int(RedshiftOID.VARCHAR)}

def show_schemas_column_extra() -> typing.Dict:
    return {'database_name': int(RedshiftOID.VARCHAR),
            'schema_name': int(RedshiftOID.VARCHAR),
            'schema_owner': int(RedshiftOID.INTEGER),
            'schema_type': int(RedshiftOID.VARCHAR),
            'schema_acl': int(RedshiftOID.VARCHAR),
            'source_database': int(RedshiftOID.VARCHAR),
            'schema_option': int(RedshiftOID.VARCHAR),
            'extra_1': int(RedshiftOID.VARCHAR),
            'extra_2': int(RedshiftOID.INTEGER),
            'extra_3': int(RedshiftOID.INTEGER)}

def show_schemas_column_diff_order() -> typing.Dict:
    return {'schema_option': int(RedshiftOID.VARCHAR),
            'database_name': int(RedshiftOID.VARCHAR),
            'schema_name': int(RedshiftOID.VARCHAR),
            'schema_owner': int(RedshiftOID.INTEGER),
            'schema_type': int(RedshiftOID.VARCHAR),
            'schema_acl': int(RedshiftOID.VARCHAR),
            'source_database': int(RedshiftOID.VARCHAR)}


SHOW_SCHEMAS_COL: typing.List[typing.Dict] = [show_schemas_column(), show_schemas_column_extra(), show_schemas_column_diff_order()]
SHOW_SCHEMAS_RES: typing.List[typing.List] = [['testCatalog','testSchema','100','share',None,None,None], ['testCatalog','testSchema','100','share',None,None,None,'extra_1','1','2'], [None,'testCatalog','testSchema','100','share',None,None]]

@pytest.mark.parametrize("show_schemas_col, show_schemas_res", list(zip(SHOW_SCHEMAS_COL, SHOW_SCHEMAS_RES)))
def test_get_schemas_post_processing(show_schemas_col, show_schemas_res) -> None:
    mock_cursor: Cursor = Cursor.__new__(Cursor)
    mock_cursor.paramstyle = "mocked"
    mock_connection: Connection = Connection.__new__(Connection)
    mock_connection.parameter_statuses = deque(maxlen=100)
    mock_connection.parameter_statuses.append((b"show_discovery", 0))
    mock_cursor._c = mock_connection
    mock_cursor.ps = {}
    mock_cursor._SHOW_SCHEMAS_Col_index = {}
    mock_metadataAPIPostProcessor: MetadataAPIPostProcessor = MetadataAPIPostProcessor(mock_cursor)

    # Create Result set + column description to mock the Result return from Server API call
    mock_metadataAPIPostProcessor.set_row_description(show_schemas_col) # Create the column description

    intermediate_rs: typing.Tuple = ()                                      # Create result set
    intermediate_row: typing.List = show_schemas_res                        # Create result row
    intermediate_rs += (intermediate_row,)
    intermediate_rs_list:typing.List  = []                                  # Create result set list
    intermediate_rs_list.append(intermediate_rs)

    intermediate_column = mock_cursor.description                           # Create the column name/index mapping
    for col, i in zip(intermediate_column, range(len(intermediate_column))):
        mock_cursor._SHOW_SCHEMAS_Col_index[col[0]] = int(i)

    # Apply post-processing
    final_rs: typing.Tuple = mock_metadataAPIPostProcessor.get_schemas_post_processing(intermediate_rs_list)

    # Verify the final result
    final_column = mock_cursor.description
    assert len(final_column) == len(colName[MetadataAPI.get_schemas])
    assert len(final_column) == len(colType[MetadataAPI.get_schemas])

    expected_result = ["testSchema","testCatalog"]
    for row in final_rs:
        for res1, res2 in zip(row, expected_result):
            assert res1 == res2


def show_tables_column() -> typing.Dict:
    return {'database_name': int(RedshiftOID.VARCHAR),
            'schema_name': int(RedshiftOID.VARCHAR),
            'table_name': int(RedshiftOID.VARCHAR),
            'table_type': int(RedshiftOID.VARCHAR),
            'table_acl': int(RedshiftOID.VARCHAR),
            'remarks': int(RedshiftOID.VARCHAR),
            'owner': int(RedshiftOID.VARCHAR),
            'last_altered_time': int(RedshiftOID.TIMESTAMP),
            'last_modified_time': int(RedshiftOID.TIMESTAMP),
            'dist_style': int(RedshiftOID.VARCHAR),
            'table_subtype': int(RedshiftOID.VARCHAR)}

def show_tables_column_extra() -> typing.Dict:
    return {'database_name': int(RedshiftOID.VARCHAR),
            'schema_name': int(RedshiftOID.VARCHAR),
            'table_name': int(RedshiftOID.VARCHAR),
            'table_type': int(RedshiftOID.VARCHAR),
            'table_acl': int(RedshiftOID.VARCHAR),
            'remarks': int(RedshiftOID.VARCHAR),
            'owner': int(RedshiftOID.VARCHAR),
            'last_altered_time': int(RedshiftOID.TIMESTAMP),
            'last_modified_time': int(RedshiftOID.TIMESTAMP),
            'dist_style': int(RedshiftOID.VARCHAR),
            'table_subtype': int(RedshiftOID.VARCHAR),
            'extra_1': int(RedshiftOID.VARCHAR),
            'extra_2': int(RedshiftOID.VARCHAR),
            'extra_3': int(RedshiftOID.INTEGER),
            'extra_4': int(RedshiftOID.INTEGER)}

def show_tables_column_diff_order() -> typing.Dict:
    return {'remarks': int(RedshiftOID.VARCHAR),
            'database_name': int(RedshiftOID.VARCHAR),
            'schema_name': int(RedshiftOID.VARCHAR),
            'table_name': int(RedshiftOID.VARCHAR),
            'table_type': int(RedshiftOID.VARCHAR),
            'table_acl': int(RedshiftOID.VARCHAR),
            'owner': int(RedshiftOID.VARCHAR),
            'last_altered_time': int(RedshiftOID.TIMESTAMP),
            'last_modified_time': int(RedshiftOID.TIMESTAMP),
            'dist_style': int(RedshiftOID.VARCHAR),
            'table_subtype': int(RedshiftOID.VARCHAR)}


SHOW_TABLES_COL: typing.List[typing.Dict] = [show_tables_column(), show_tables_column_extra(), show_tables_column_diff_order()]
SHOW_TABLES_RES: typing.List[typing.List] = [
    ['testCatalog','testSchema','testTable','testTableType','testAcl','testRemarks', 'testOwner', '2012-01-01 01:01:01.123456', '2012-01-01 01:01:01.123456', 'testDistStyle', 'testSubType'],
    ['testCatalog','testSchema','testTable','testTableType','testAcl','testRemarks', 'testOwner', '2012-01-01 01:01:01.123456', '2012-01-01 01:01:01.123456', 'testDistStyle', 'testSubType', 'extra_1','extra_2','1','2'],
    ['testRemarks','testCatalog','testSchema','testTable','testTableType','testAcl', 'testOwner', '2012-01-01 01:01:01.123456', '2012-01-01 01:01:01.123456', 'testDistStyle', 'testSubType']
]
@pytest.mark.parametrize("show_tables_col, show_tables_res", list(zip(SHOW_TABLES_COL, SHOW_TABLES_RES)))
def test_get_tables_post_processing(show_tables_col, show_tables_res) -> None:
    mock_cursor: Cursor = Cursor.__new__(Cursor)
    mock_cursor.paramstyle = "mocked"
    mock_connection: Connection = Connection.__new__(Connection)
    mock_connection.parameter_statuses = deque(maxlen=100)
    mock_connection.parameter_statuses.append((b"show_discovery", 0))
    mock_cursor._c = mock_connection
    mock_cursor.ps = {}
    mock_cursor._SHOW_TABLES_Col_index = {}
    mock_metadataAPIPostProcessor: MetadataAPIPostProcessor = MetadataAPIPostProcessor(mock_cursor)

    # Create Result set + column description to mock the Result return from Server API call
    mock_metadataAPIPostProcessor.set_row_description(show_tables_col) # Create the column description

    intermediate_rs: typing.Tuple = ()                                      # Create result set
    intermediate_row: typing.List = show_tables_res                         # Create result row
    intermediate_rs += (intermediate_row,)
    intermediate_rs_list:typing.List  = []                                  # Create result set list
    intermediate_rs_list.append(intermediate_rs)

    intermediate_column = mock_cursor.description                           # Create the column name/index mapping
    for col, i in zip(intermediate_column, range(len(intermediate_column))):
        mock_cursor._SHOW_TABLES_Col_index[col[0]] = int(i)

    # Apply post-processing
    final_rs: typing.Tuple = mock_metadataAPIPostProcessor.get_tables_post_processing(intermediate_rs_list, ['testTableType'])

    # Verify the final result
    final_column = mock_cursor.description
    assert len(final_column) == len(colName[MetadataAPI.get_tables])
    assert len(final_column) == len(colType[MetadataAPI.get_tables])

    expected_result = ["testCatalog", "testSchema","testTable","testTableType","testRemarks","","","","","", 'testOwner', '2012-01-01 01:01:01.123456', '2012-01-01 01:01:01.123456', 'testDistStyle', 'testSubType']
    for row in final_rs:
        for res1, res2 in zip(row, expected_result):
            assert res1 == res2


def show_columns_column() -> typing.Dict:
    return {'database_name': int(RedshiftOID.VARCHAR),
            'schema_name': int(RedshiftOID.VARCHAR),
            'table_name': int(RedshiftOID.VARCHAR),
            'column_name': int(RedshiftOID.VARCHAR),
            'ordinal_position': int(RedshiftOID.INTEGER),
            'column_default': int(RedshiftOID.VARCHAR),
            'is_nullable': int(RedshiftOID.VARCHAR),
            'data_type': int(RedshiftOID.VARCHAR),
            'character_maximum_length': int(RedshiftOID.INTEGER),
            'numeric_precision': int(RedshiftOID.INTEGER),
            'numeric_scale': int(RedshiftOID.INTEGER),
            'remarks': int(RedshiftOID.VARCHAR),
            'sort_key_type': int(RedshiftOID.VARCHAR),
            'sort_key': int(RedshiftOID.INTEGER),
            'dist_key': int(RedshiftOID.INTEGER),
            'encoding': int(RedshiftOID.VARCHAR),
            'collation': int(RedshiftOID.VARCHAR)}

def show_columns_column_extra() -> typing.Dict:
    return {'database_name': int(RedshiftOID.VARCHAR),
            'schema_name': int(RedshiftOID.VARCHAR),
            'table_name': int(RedshiftOID.VARCHAR),
            'column_name': int(RedshiftOID.VARCHAR),
            'ordinal_position': int(RedshiftOID.INTEGER),
            'column_default': int(RedshiftOID.VARCHAR),
            'is_nullable': int(RedshiftOID.VARCHAR),
            'data_type': int(RedshiftOID.VARCHAR),
            'character_maximum_length': int(RedshiftOID.INTEGER),
            'numeric_precision': int(RedshiftOID.INTEGER),
            'numeric_scale': int(RedshiftOID.INTEGER),
            'remarks': int(RedshiftOID.VARCHAR),
            'sort_key_type': int(RedshiftOID.VARCHAR),
            'sort_key': int(RedshiftOID.INTEGER),
            'dist_key': int(RedshiftOID.INTEGER),
            'encoding': int(RedshiftOID.VARCHAR),
            'collation': int(RedshiftOID.VARCHAR),
            'extra_1': int(RedshiftOID.VARCHAR),
            'extra_2': int(RedshiftOID.INTEGER),
            'extra_3': int(RedshiftOID.INTEGER)}

def show_columns_column_diff_order() -> typing.Dict:
    return {'remarks': int(RedshiftOID.VARCHAR),
            'database_name': int(RedshiftOID.VARCHAR),
            'schema_name': int(RedshiftOID.VARCHAR),
            'table_name': int(RedshiftOID.VARCHAR),
            'column_name': int(RedshiftOID.VARCHAR),
            'ordinal_position': int(RedshiftOID.INTEGER),
            'column_default': int(RedshiftOID.VARCHAR),
            'is_nullable': int(RedshiftOID.VARCHAR),
            'data_type': int(RedshiftOID.VARCHAR),
            'character_maximum_length': int(RedshiftOID.INTEGER),
            'numeric_precision': int(RedshiftOID.INTEGER),
            'numeric_scale': int(RedshiftOID.INTEGER),
            'sort_key_type': int(RedshiftOID.VARCHAR),
            'sort_key': int(RedshiftOID.INTEGER),
            'dist_key': int(RedshiftOID.INTEGER),
            'encoding': int(RedshiftOID.VARCHAR),
            'collation': int(RedshiftOID.VARCHAR)}

col_name_list = ["testColumnNameSmallint","testColumnNameInteger","testColumnNameBigint","testColumnNameNumeric","testColumnNameReal",
                 "testColumnNameDouble","testColumnNameBoolean","testColumnNameChar","testColumnNameVarchar","testColumnNameDate",
                 "testColumnNameTime","testColumnNameTimetz","testColumnNameTimestamp","testColumnNameTimestamptz","testColumnNameIntervaly2m",
                 "testColumnNameIntervald2s"]
col_def_list = ["testColumnDefault","\"identity\"(211928, 0, '1,1'::text)","testColumnDefault",None,"testColumnDefault",
                "testColumnDefault","testColumnDefault","testColumnDefault","testColumnDefault","testColumnDefault",
                "testColumnDefault","testColumnDefault","testColumnDefault","testColumnDefault","testColumnDefault",
                "testColumnDefault"]
col_nullable_list = ["YES","NO","YES","YES","YES",
                     "YES","YES","YES","YES","YES",
                     "YES","YES","YES","YES","YES",
                     "YES"]
data_type_list = ["smallint", "integer", "bigint", "numeric", "real",
                  "double precision", "boolean", "character", "character varying", "date",
                  "time(1) without time zone", "time(2) with time zone", "timestamp(3) without time zone", "timestamp with time zone", "interval year to month",
                  "interval day to second (4)"]
character_maximum_length_list = [None, None, None, None, None, None, None, "20", "256", None, None, None, None, None, None, None]
numeric_precision_list = ["16", "32", "64", "10", None, None, None, None, None, None, None, None, None, None, None, None]
numeric_scale_list = [None, None, None, "5", None, None, None, None, None, None, None, None, None, None, None, None]

SHOW_COLUMNS_COL: typing.List[typing.Dict] = [show_columns_column(), show_columns_column_extra(), show_columns_column_diff_order()]

@pytest.mark.parametrize("show_columns_col, diff_order", list(zip(SHOW_COLUMNS_COL, [False,False,True])))
def test_get_columns_post_processing(show_columns_col,diff_order) -> None:
    mock_cursor: Cursor = Cursor.__new__(Cursor)
    mock_cursor.paramstyle = "mocked"
    mock_connection: Connection = Connection.__new__(Connection)
    mock_connection.parameter_statuses = deque(maxlen=100)
    mock_connection.parameter_statuses.append((b"show_discovery", 0))
    mock_cursor._c = mock_connection
    mock_cursor.ps = {}
    mock_cursor._SHOW_COLUMNS_Col_index = {}
    mock_metadataAPIPostProcessor: MetadataAPIPostProcessor = MetadataAPIPostProcessor(mock_cursor)

    # Create Result set + column description to mock the Result return from Server API call
    mock_metadataAPIPostProcessor.set_row_description(show_columns_col)        # Create the column description

    intermediate_rs: typing.Tuple = ()                                          # Create result set
    for index in range(len(col_name_list)):
        intermediate_row: typing.List = []                                      # Create result row
        if diff_order:
            intermediate_row.append("testRemarks")
        intermediate_row.append("testCatalog")
        intermediate_row.append("testSchema")
        intermediate_row.append("testTable")
        intermediate_row.append(col_name_list[index])
        intermediate_row.append(index+1)
        intermediate_row.append(col_def_list[index])
        intermediate_row.append(col_nullable_list[index])
        intermediate_row.append(data_type_list[index])
        intermediate_row.append(character_maximum_length_list[index])
        intermediate_row.append(numeric_precision_list[index])
        intermediate_row.append(numeric_scale_list[index])
        if not diff_order:
            intermediate_row.append("testRemarks")
        intermediate_row.append("AUTO")
        intermediate_row.append(0)
        intermediate_row.append(0)
        intermediate_row.append("testEncode")
        intermediate_row.append("testCollation")
        intermediate_rs += (intermediate_row,)
    intermediate_rs_list:typing.List  = []                                      # Create result set list
    intermediate_rs_list.append(intermediate_rs)

    intermediate_column = mock_cursor.description                               # Create the column name/index mapping
    for col, i in zip(intermediate_column, range(len(intermediate_column))):
        mock_cursor._SHOW_COLUMNS_Col_index[col[0]] = int(i)

    # Apply post-processing
    final_rs: typing.Tuple = mock_metadataAPIPostProcessor.get_columns_post_processing(intermediate_rs_list)

    # Verify the final result
    final_column = mock_cursor.description
    assert len(final_column) == len(colName[MetadataAPI.get_columns])
    assert len(final_column) == len(colType[MetadataAPI.get_columns])
    for row1, row2 in zip(final_rs, expected_result_get_columns):
        for res1, res2 in zip(row1, row2):
            assert res1 == res2

def show_constraints_pk_column() -> typing.Dict:
    return {'database_name': int(RedshiftOID.VARCHAR),
            'schema_name': int(RedshiftOID.VARCHAR),
            'table_name': int(RedshiftOID.VARCHAR),
            'column_name': int(RedshiftOID.VARCHAR),
            'key_seq': int(RedshiftOID.INTEGER),
            'pk_name': int(RedshiftOID.VARCHAR)}

def show_constraints_pk_column_extra() -> typing.Dict:
    return {'database_name': int(RedshiftOID.VARCHAR),
            'schema_name': int(RedshiftOID.VARCHAR),
            'table_name': int(RedshiftOID.VARCHAR),
            'column_name': int(RedshiftOID.VARCHAR),
            'key_seq': int(RedshiftOID.INTEGER),
            'pk_name': int(RedshiftOID.VARCHAR),
            'extra_1': int(RedshiftOID.VARCHAR),
            'extra_2': int(RedshiftOID.INTEGER),
            'extra_3': int(RedshiftOID.INTEGER)}

def show_constraints_pk_column_diff_order() -> typing.Dict:
    return {'pk_name': int(RedshiftOID.VARCHAR),
            'database_name': int(RedshiftOID.VARCHAR),
            'schema_name': int(RedshiftOID.VARCHAR),
            'table_name': int(RedshiftOID.VARCHAR),
            'column_name': int(RedshiftOID.VARCHAR),
            'key_seq': int(RedshiftOID.INTEGER)}


SHOW_CONSTRAINTS_PK_COL: typing.List[typing.Dict] = [show_constraints_pk_column(), show_constraints_pk_column_extra(), show_constraints_pk_column_diff_order()]
SHOW_CONSTRAINTS_PK_RES: typing.List[typing.List] = [
    ['testCatalog','testSchema','testTable','testColumn','1','pkName'],
    ['testCatalog','testSchema','testTable','testColumn','1','pkName','extra_1','1','2'],
    ['pkName','testCatalog','testSchema','testTable','testColumn','1']]

@pytest.mark.parametrize("show_constraints_pk_col, show_constraints_pk_res", list(zip(SHOW_CONSTRAINTS_PK_COL, SHOW_CONSTRAINTS_PK_RES)))
def test_get_primary_keys_post_processing(show_constraints_pk_col, show_constraints_pk_res) -> None:
    mock_cursor: Cursor = Cursor.__new__(Cursor)
    mock_cursor.paramstyle = "mocked"
    mock_connection: Connection = Connection.__new__(Connection)
    mock_connection.parameter_statuses = deque(maxlen=100)
    mock_connection.parameter_statuses.append((b"show_discovery", 0))
    mock_cursor._c = mock_connection
    mock_cursor.ps = {}
    mock_cursor._SHOW_CONSTRAINTS_PK_Col_index = {}
    mock_metadataAPIPostProcessor: MetadataAPIPostProcessor = MetadataAPIPostProcessor(mock_cursor)

    # Create Result set + column description to mock the Result return from Server API call
    mock_metadataAPIPostProcessor.set_row_description(show_constraints_pk_col) # Create the column description

    intermediate_rs: typing.Tuple = ()                                      # Create result set
    intermediate_row: typing.List = show_constraints_pk_res                 # Create result row
    intermediate_rs += (intermediate_row,)
    intermediate_rs_list:typing.List  = []                                  # Create result set list
    intermediate_rs_list.append(intermediate_rs)

    intermediate_column = mock_cursor.description                           # Create the column name/index mapping
    for col, i in zip(intermediate_column, range(len(intermediate_column))):
        mock_cursor._SHOW_CONSTRAINTS_PK_Col_index[col[0]] = int(i)

    # Apply post-processing
    final_rs: typing.Tuple = mock_metadataAPIPostProcessor.get_primary_keys_post_processing(intermediate_rs_list)

    # Verify the final result
    final_column = mock_cursor.description
    assert len(final_column) == len(colName[MetadataAPI.get_primary_keys])
    assert len(final_column) == len(colType[MetadataAPI.get_primary_keys])

    expected_result = ['testCatalog','testSchema','testTable','testColumn','1','pkName']
    for row in final_rs:
        for res1, res2 in zip(row, expected_result):
            assert res1 == res2

def show_constraints_fk_column() -> typing.Dict:
    return {'pk_database_name': int(RedshiftOID.VARCHAR),
            'pk_schema_name': int(RedshiftOID.VARCHAR),
            'pk_table_name': int(RedshiftOID.VARCHAR),
            'pk_column_name': int(RedshiftOID.VARCHAR),
            'fk_database_name': int(RedshiftOID.VARCHAR),
            'fk_schema_name': int(RedshiftOID.VARCHAR),
            'fk_table_name': int(RedshiftOID.VARCHAR),
            'fk_column_name': int(RedshiftOID.VARCHAR),
            'key_seq': int(RedshiftOID.INTEGER),
            'update_rule': int(RedshiftOID.INTEGER),
            'delete_rule': int(RedshiftOID.INTEGER),
            'fk_name': int(RedshiftOID.VARCHAR),
            'pk_name': int(RedshiftOID.VARCHAR),
            'deferrability': int(RedshiftOID.INTEGER)}

def show_constraints_fk_column_extra() -> typing.Dict:
    return {'pk_database_name': int(RedshiftOID.VARCHAR),
            'pk_schema_name': int(RedshiftOID.VARCHAR),
            'pk_table_name': int(RedshiftOID.VARCHAR),
            'pk_column_name': int(RedshiftOID.VARCHAR),
            'fk_database_name': int(RedshiftOID.VARCHAR),
            'fk_schema_name': int(RedshiftOID.VARCHAR),
            'fk_table_name': int(RedshiftOID.VARCHAR),
            'fk_column_name': int(RedshiftOID.VARCHAR),
            'key_seq': int(RedshiftOID.INTEGER),
            'update_rule': int(RedshiftOID.INTEGER),
            'delete_rule': int(RedshiftOID.INTEGER),
            'fk_name': int(RedshiftOID.VARCHAR),
            'pk_name': int(RedshiftOID.VARCHAR),
            'deferrability': int(RedshiftOID.INTEGER),
            'extra_1': int(RedshiftOID.VARCHAR),
            'extra_2': int(RedshiftOID.INTEGER),
            'extra_3': int(RedshiftOID.INTEGER)}

def show_constraints_fk_column_diff_order() -> typing.Dict:
    return {'deferrability': int(RedshiftOID.INTEGER),
            'pk_database_name': int(RedshiftOID.VARCHAR),
            'pk_schema_name': int(RedshiftOID.VARCHAR),
            'pk_table_name': int(RedshiftOID.VARCHAR),
            'pk_column_name': int(RedshiftOID.VARCHAR),
            'fk_database_name': int(RedshiftOID.VARCHAR),
            'fk_schema_name': int(RedshiftOID.VARCHAR),
            'fk_table_name': int(RedshiftOID.VARCHAR),
            'fk_column_name': int(RedshiftOID.VARCHAR),
            'key_seq': int(RedshiftOID.INTEGER),
            'update_rule': int(RedshiftOID.INTEGER),
            'delete_rule': int(RedshiftOID.INTEGER),
            'fk_name': int(RedshiftOID.VARCHAR),
            'pk_name': int(RedshiftOID.VARCHAR)}


SHOW_CONSTRAINTS_FK_COL: typing.List[typing.Dict] = [show_constraints_fk_column(), show_constraints_fk_column_extra(), show_constraints_fk_column_diff_order()]
SHOW_CONSTRAINTS_FK_RES: typing.List[typing.List] = [
    [
        ['testCatalogPk','testSchemaPk','testTablePk_1','testColumnPk','testCatalogFk','testSchemaFk','testTableFk_3','testColumnFk','1','1','1','fkName','pkName','1'],
        ['testCatalogPk','testSchemaPk','testTablePk_1','testColumnPk','testCatalogFk','testSchemaFk','testTableFk_3','testColumnFk','2','1','1','fkName','pkName','1'],
        ['testCatalogPk','testSchemaPk','testTablePk_3','testColumnPk','testCatalogFk','testSchemaFk','testTableFk_1','testColumnFk','1','1','1','fkName','pkName','1'],
        ['testCatalogPk','testSchemaPk','testTablePk_3','testColumnPk','testCatalogFk','testSchemaFk','testTableFk_1','testColumnFk','2','1','1','fkName','pkName','1'],
        ['testCatalogPk','testSchemaPk','testTablePk_2','testColumnPk','testCatalogFk','testSchemaFk','testTableFk_2','testColumnFk','1','1','1','fkName','pkName','1'],
        ['testCatalogPk','testSchemaPk','testTablePk_2','testColumnPk','testCatalogFk','testSchemaFk','testTableFk_2','testColumnFk','2','1','1','fkName','pkName','1']
    ],
    [
        ['testCatalogPk','testSchemaPk','testTablePk_1','testColumnPk','testCatalogFk','testSchemaFk','testTableFk_3','testColumnFk','1','1','1','fkName','pkName','1','extra_1','1','2'],
        ['testCatalogPk','testSchemaPk','testTablePk_1','testColumnPk','testCatalogFk','testSchemaFk','testTableFk_3','testColumnFk','2','1','1','fkName','pkName','1','extra_1','1','2'],
        ['testCatalogPk','testSchemaPk','testTablePk_3','testColumnPk','testCatalogFk','testSchemaFk','testTableFk_1','testColumnFk','1','1','1','fkName','pkName','1','extra_1','1','2'],
        ['testCatalogPk','testSchemaPk','testTablePk_3','testColumnPk','testCatalogFk','testSchemaFk','testTableFk_1','testColumnFk','2','1','1','fkName','pkName','1','extra_1','1','2'],
        ['testCatalogPk','testSchemaPk','testTablePk_2','testColumnPk','testCatalogFk','testSchemaFk','testTableFk_2','testColumnFk','1','1','1','fkName','pkName','1','extra_1','1','2'],
        ['testCatalogPk','testSchemaPk','testTablePk_2','testColumnPk','testCatalogFk','testSchemaFk','testTableFk_2','testColumnFk','2','1','1','fkName','pkName','1','extra_1','1','2']
    ],
    [
        ['1','testCatalogPk','testSchemaPk','testTablePk_1','testColumnPk','testCatalogFk','testSchemaFk','testTableFk_3','testColumnFk','1','1','1','fkName','pkName'],
        ['1','testCatalogPk','testSchemaPk','testTablePk_1','testColumnPk','testCatalogFk','testSchemaFk','testTableFk_3','testColumnFk','2','1','1','fkName','pkName'],
        ['1','testCatalogPk','testSchemaPk','testTablePk_3','testColumnPk','testCatalogFk','testSchemaFk','testTableFk_1','testColumnFk','1','1','1','fkName','pkName'],
        ['1','testCatalogPk','testSchemaPk','testTablePk_3','testColumnPk','testCatalogFk','testSchemaFk','testTableFk_1','testColumnFk','2','1','1','fkName','pkName'],
        ['1','testCatalogPk','testSchemaPk','testTablePk_2','testColumnPk','testCatalogFk','testSchemaFk','testTableFk_2','testColumnFk','1','1','1','fkName','pkName'],
        ['1','testCatalogPk','testSchemaPk','testTablePk_2','testColumnPk','testCatalogFk','testSchemaFk','testTableFk_2','testColumnFk','2','1','1','fkName','pkName']
    ]
]

get_imported_keys_expected_result = (
        ['testCatalogPk', 'testSchemaPk', 'testTablePk_1', 'testColumnPk', 'testCatalogFk', 'testSchemaFk', 'testTableFk_3', 'testColumnFk', '1', '1', '1', 'fkName', 'pkName', '1'],
        ['testCatalogPk', 'testSchemaPk', 'testTablePk_1', 'testColumnPk', 'testCatalogFk', 'testSchemaFk', 'testTableFk_3', 'testColumnFk', '2', '1', '1', 'fkName', 'pkName', '1'],
        ['testCatalogPk', 'testSchemaPk', 'testTablePk_2', 'testColumnPk', 'testCatalogFk', 'testSchemaFk', 'testTableFk_2', 'testColumnFk', '1', '1', '1', 'fkName', 'pkName', '1'],
        ['testCatalogPk', 'testSchemaPk', 'testTablePk_2', 'testColumnPk', 'testCatalogFk', 'testSchemaFk', 'testTableFk_2', 'testColumnFk', '2', '1', '1', 'fkName', 'pkName', '1'],
        ['testCatalogPk', 'testSchemaPk', 'testTablePk_3', 'testColumnPk', 'testCatalogFk', 'testSchemaFk', 'testTableFk_1', 'testColumnFk', '1', '1', '1', 'fkName', 'pkName', '1'],
        ['testCatalogPk', 'testSchemaPk', 'testTablePk_3', 'testColumnPk', 'testCatalogFk', 'testSchemaFk', 'testTableFk_1', 'testColumnFk', '2', '1', '1', 'fkName', 'pkName', '1']
)

get_exported_keys_expected_result = (
        ['testCatalogPk', 'testSchemaPk', 'testTablePk_3', 'testColumnPk', 'testCatalogFk', 'testSchemaFk', 'testTableFk_1', 'testColumnFk', '1', '1', '1', 'fkName', 'pkName', '1'],
        ['testCatalogPk', 'testSchemaPk', 'testTablePk_3', 'testColumnPk', 'testCatalogFk', 'testSchemaFk', 'testTableFk_1', 'testColumnFk', '2', '1', '1', 'fkName', 'pkName', '1'],
        ['testCatalogPk', 'testSchemaPk', 'testTablePk_2', 'testColumnPk', 'testCatalogFk', 'testSchemaFk', 'testTableFk_2', 'testColumnFk', '1', '1', '1', 'fkName', 'pkName', '1'],
        ['testCatalogPk', 'testSchemaPk', 'testTablePk_2', 'testColumnPk', 'testCatalogFk', 'testSchemaFk', 'testTableFk_2', 'testColumnFk', '2', '1', '1', 'fkName', 'pkName', '1'],
        ['testCatalogPk', 'testSchemaPk', 'testTablePk_1', 'testColumnPk', 'testCatalogFk', 'testSchemaFk', 'testTableFk_3', 'testColumnFk', '1', '1', '1', 'fkName', 'pkName', '1'],
        ['testCatalogPk', 'testSchemaPk', 'testTablePk_1', 'testColumnPk', 'testCatalogFk', 'testSchemaFk', 'testTableFk_3', 'testColumnFk', '2', '1', '1', 'fkName', 'pkName', '1']
)

@pytest.mark.parametrize("show_constraints_fk_col, show_constraints_fk_res", list(zip(SHOW_CONSTRAINTS_FK_COL, SHOW_CONSTRAINTS_FK_RES)))
def test_get_foreign_keys_post_processing(show_constraints_fk_col, show_constraints_fk_res) -> None:
    mock_cursor: Cursor = Cursor.__new__(Cursor)
    mock_cursor.paramstyle = "mocked"
    mock_connection: Connection = Connection.__new__(Connection)
    mock_connection.parameter_statuses = deque(maxlen=100)
    mock_connection.parameter_statuses.append((b"show_discovery", 0))
    mock_cursor._c = mock_connection
    mock_cursor.ps = {}
    mock_cursor._SHOW_CONSTRAINTS_FK_Col_index = {}
    mock_metadataAPIPostProcessor: MetadataAPIPostProcessor = MetadataAPIPostProcessor(mock_cursor)

    # Create Result set + column description to mock the Result return from Server API call
    mock_metadataAPIPostProcessor.set_row_description(show_constraints_fk_col) # Create the column description

    intermediate_rs: typing.Tuple = ()                                      # Create result set
    intermediate_row: typing.List = show_constraints_fk_res                 # Create result row
    for row in intermediate_row:
        intermediate_rs += (row,)
    intermediate_rs_list:typing.List  = []                                  # Create result set list
    intermediate_rs_list.append(intermediate_rs)

    intermediate_column = mock_cursor.description                           # Create the column name/index mapping
    for col, i in zip(intermediate_column, range(len(intermediate_column))):
        mock_cursor._SHOW_CONSTRAINTS_FK_Col_index[col[0]] = int(i)

    is_imported_list = [True, False]
    for is_imported in is_imported_list:
        # Apply post-processing
        final_rs: typing.Tuple = mock_metadataAPIPostProcessor.get_foreign_keys_post_processing(intermediate_rs_list, is_imported)

        # Verify the final result
        final_column = mock_cursor.description
        assert len(final_column) == len(colName[MetadataAPI.get_foreign_keys])
        assert len(final_column) == len(colType[MetadataAPI.get_foreign_keys])

        expected_result = get_imported_keys_expected_result if is_imported else get_exported_keys_expected_result
        for actual_row, expected_row in zip(final_rs, expected_result):
            for res1, res2 in zip(actual_row, expected_row):
                assert res1 == res2

@pytest.mark.parametrize("show_columns_col, diff_order", list(zip(SHOW_COLUMNS_COL, [False,False,True])))
def test_get_best_row_identifier_post_processing(show_columns_col,diff_order) -> None:
    mock_cursor: Cursor = Cursor.__new__(Cursor)
    mock_cursor.paramstyle = "mocked"
    mock_connection: Connection = Connection.__new__(Connection)
    mock_connection.parameter_statuses = deque(maxlen=100)
    mock_connection.parameter_statuses.append((b"show_discovery", 0))
    mock_cursor._c = mock_connection
    mock_cursor.ps = {}
    mock_cursor._SHOW_COLUMNS_Col_index = {}
    mock_metadataAPIPostProcessor: MetadataAPIPostProcessor = MetadataAPIPostProcessor(mock_cursor)

    # Create Result set + column description to mock the Result return from Server API call
    mock_metadataAPIPostProcessor.set_row_description(show_columns_col)        # Create the column description

    intermediate_rs: typing.Tuple = ()                                          # Create result set
    for index in range(len(col_name_list)):
        intermediate_row: typing.List = []                                      # Create result row
        if diff_order:
            intermediate_row.append("testRemarks")
        intermediate_row.append("testCatalog")
        intermediate_row.append("testSchema")
        intermediate_row.append("testTable")
        intermediate_row.append(col_name_list[index])
        intermediate_row.append(index+1)
        intermediate_row.append(col_def_list[index])
        intermediate_row.append(col_nullable_list[index])
        intermediate_row.append(data_type_list[index])
        intermediate_row.append(character_maximum_length_list[index])
        intermediate_row.append(numeric_precision_list[index])
        intermediate_row.append(numeric_scale_list[index])
        if not diff_order:
            intermediate_row.append("testRemarks")
        intermediate_rs += (intermediate_row,)
    intermediate_rs_list:typing.List  = []                                      # Create result set list
    intermediate_rs_list.append(intermediate_rs)

    intermediate_column = mock_cursor.description                               # Create the column name/index mapping
    for col, i in zip(intermediate_column, range(len(intermediate_column))):
        mock_cursor._SHOW_COLUMNS_Col_index[col[0]] = int(i)

    # Apply post-processing
    final_rs: typing.Tuple = mock_metadataAPIPostProcessor.get_best_row_identifier_post_processing(intermediate_rs_list, 0)

    # Verify the final result
    final_column = mock_cursor.description
    assert len(final_column) == len(colName[MetadataAPI.get_best_row_identifier])
    assert len(final_column) == len(colType[MetadataAPI.get_best_row_identifier])

    expected_result = [
        [0, "testColumnNameSmallint", 5, "int2", 5, None, 0, 1],
        [0, "testColumnNameInteger", 4, "int4", 10, None, 0, 1],
        [0, "testColumnNameBigint", -5, "int8", 19, None, 0, 1],
        [0, "testColumnNameNumeric", 2, "numeric", 10, None, 5, 1],
        [0, "testColumnNameReal", 7, "float4", 8, None, 8, 1],
        [0, "testColumnNameDouble", 8, "float8", 17, None, 17, 1],
        [0, "testColumnNameBoolean", -7, "bool", 1, None, 0, 1],
        [0, "testColumnNameChar", 1, "char", 20, None, 0, 1],
        [0, "testColumnNameVarchar", 12, "varchar", 256, None, 0, 1],
        [0, "testColumnNameDate", 91, "date", 13, None, 0, 1],
        [0, "testColumnNameTime", 92, "time", 15, None, 1, 1],
        [0, "testColumnNameTimetz", 2013, "timetz", 21, None, 2, 1],
        [0, "testColumnNameTimestamp", 93, "timestamp", 29, None, 3, 1],
        [0, "testColumnNameTimestamptz", 2014, "timestamptz", 35, None, 6, 1],
        [0, "testColumnNameIntervaly2m", 1111, "intervaly2m", 32, None, 0, 1],
        [0, "testColumnNameIntervald2s", 1111, "intervald2s", 64, None, 4, 1]
    ]
    for row1, row2 in zip(final_rs, expected_result):
        for res1, res2 in zip(row1, row2):
            assert res1 == res2

def show_grant_column_column() -> typing.Dict:
    return {'database_name': int(RedshiftOID.VARCHAR),
            'schema_name': int(RedshiftOID.VARCHAR),
            'table_name': int(RedshiftOID.VARCHAR),
            'column_name': int(RedshiftOID.VARCHAR),
            'grantor_name': int(RedshiftOID.VARCHAR),
            'identity_name': int(RedshiftOID.VARCHAR),
            'privilege_type': int(RedshiftOID.VARCHAR),
            'admin_option': int(RedshiftOID.VARCHAR)}

def show_grant_column_column_extra() -> typing.Dict:
    return {'database_name': int(RedshiftOID.VARCHAR),
            'schema_name': int(RedshiftOID.VARCHAR),
            'table_name': int(RedshiftOID.VARCHAR),
            'column_name': int(RedshiftOID.VARCHAR),
            'grantor_name': int(RedshiftOID.VARCHAR),
            'identity_name': int(RedshiftOID.VARCHAR),
            'privilege_type': int(RedshiftOID.VARCHAR),
            'admin_option': int(RedshiftOID.VARCHAR),
            'extra_1': int(RedshiftOID.VARCHAR),
            'extra_2': int(RedshiftOID.INTEGER),
            'extra_3': int(RedshiftOID.INTEGER)}

def show_grant_column_column_diff_order() -> typing.Dict:
    return {'admin_option': int(RedshiftOID.VARCHAR),
            'database_name': int(RedshiftOID.VARCHAR),
            'schema_name': int(RedshiftOID.VARCHAR),
            'table_name': int(RedshiftOID.VARCHAR),
            'column_name': int(RedshiftOID.VARCHAR),
            'grantor_name': int(RedshiftOID.VARCHAR),
            'identity_name': int(RedshiftOID.VARCHAR),
            'privilege_type': int(RedshiftOID.VARCHAR)}


SHOW_GRANT_COLUMN_COL: typing.List[typing.Dict] = [show_grant_column_column(), show_grant_column_column_extra(), show_grant_column_column_diff_order()]
SHOW_GRANT_COLUMN_RES: typing.List[typing.List] = [
    ['testCatalog','testSchema','testTable','testColumn','grantor','grantee','select','YES'],
    ['testCatalog','testSchema','testTable','testColumn','grantor','grantee','select','YES','extra_1','1','2'],
    ['YES','testCatalog','testSchema','testTable','testColumn','grantor','grantee','select']]

@pytest.mark.parametrize("show_grant_column_col, show_grant_column_res", list(zip(SHOW_GRANT_COLUMN_COL, SHOW_GRANT_COLUMN_RES)))
def test_get_column_privileges_post_processing(show_grant_column_col, show_grant_column_res) -> None:
    mock_cursor: Cursor = Cursor.__new__(Cursor)
    mock_cursor.paramstyle = "mocked"
    mock_connection: Connection = Connection.__new__(Connection)
    mock_connection.parameter_statuses = deque(maxlen=100)
    mock_connection.parameter_statuses.append((b"show_discovery", 0))
    mock_cursor._c = mock_connection
    mock_cursor.ps = {}
    mock_cursor._SHOW_GRANTS_COLUMN_Col_index = {}
    mock_metadataAPIPostProcessor: MetadataAPIPostProcessor = MetadataAPIPostProcessor(mock_cursor)

    # Create Result set + column description to mock the Result return from Server API call
    mock_metadataAPIPostProcessor.set_row_description(show_grant_column_col) # Create the column description

    intermediate_rs: typing.Tuple = ()                                      # Create result set
    intermediate_row: typing.List = show_grant_column_res                  # Create result row
    intermediate_rs += (intermediate_row,)
    intermediate_rs_list:typing.List  = []                                  # Create result set list
    intermediate_rs_list.append(intermediate_rs)

    intermediate_column = mock_cursor.description                           # Create the column name/index mapping
    for col, i in zip(intermediate_column, range(len(intermediate_column))):
        mock_cursor._SHOW_GRANTS_COLUMN_Col_index[col[0]] = int(i)

    # Apply post-processing
    final_rs: typing.Tuple = mock_metadataAPIPostProcessor.get_column_privileges_post_processing(intermediate_rs_list)

    # Verify the final result
    final_column = mock_cursor.description
    assert len(final_column) == len(colName[MetadataAPI.get_column_privileges])
    assert len(final_column) == len(colType[MetadataAPI.get_column_privileges])

    expected_result = ['testCatalog','testSchema','testTable','testColumn','grantor','grantee','select','YES']
    for row in final_rs:
        for res1, res2 in zip(row, expected_result):
            assert res1 == res2

def show_grant_table_column() -> typing.Dict:
    return {'database_name': int(RedshiftOID.VARCHAR),
            'schema_name': int(RedshiftOID.VARCHAR),
            'object_name': int(RedshiftOID.VARCHAR),
            'grantor_name': int(RedshiftOID.VARCHAR),
            'identity_name': int(RedshiftOID.VARCHAR),
            'privilege_type': int(RedshiftOID.VARCHAR),
            'admin_option': int(RedshiftOID.VARCHAR)}

def show_grant_table_column_extra() -> typing.Dict:
    return {'database_name': int(RedshiftOID.VARCHAR),
            'schema_name': int(RedshiftOID.VARCHAR),
            'object_name': int(RedshiftOID.VARCHAR),
            'grantor_name': int(RedshiftOID.VARCHAR),
            'identity_name': int(RedshiftOID.VARCHAR),
            'privilege_type': int(RedshiftOID.VARCHAR),
            'admin_option': int(RedshiftOID.VARCHAR),
            'extra_1': int(RedshiftOID.VARCHAR),
            'extra_2': int(RedshiftOID.INTEGER),
            'extra_3': int(RedshiftOID.INTEGER)}

def show_grant_table_column_diff_order() -> typing.Dict:
    return {'admin_option': int(RedshiftOID.VARCHAR),
            'database_name': int(RedshiftOID.VARCHAR),
            'schema_name': int(RedshiftOID.VARCHAR),
            'object_name': int(RedshiftOID.VARCHAR),
            'grantor_name': int(RedshiftOID.VARCHAR),
            'identity_name': int(RedshiftOID.VARCHAR),
            'privilege_type': int(RedshiftOID.VARCHAR)}


SHOW_GRANT_TABLE_COL: typing.List[typing.Dict] = [show_grant_table_column(), show_grant_table_column_extra(), show_grant_table_column_diff_order()]
SHOW_GRANT_TABLE_RES: typing.List[typing.List] = [
    ['testCatalog','testSchema','testTable','grantor','grantee','select','YES'],
    ['testCatalog','testSchema','testTable','grantor','grantee','select','YES','extra_1','1','2'],
    ['YES','testCatalog','testSchema','testTable','grantor','grantee','select']]

@pytest.mark.parametrize("show_grant_table_col, show_grant_table_res", list(zip(SHOW_GRANT_TABLE_COL, SHOW_GRANT_TABLE_RES)))
def test_get_table_privileges_post_processing(show_grant_table_col, show_grant_table_res) -> None:
    mock_cursor: Cursor = Cursor.__new__(Cursor)
    mock_cursor.paramstyle = "mocked"
    mock_connection: Connection = Connection.__new__(Connection)
    mock_connection.parameter_statuses = deque(maxlen=100)
    mock_connection.parameter_statuses.append((b"show_discovery", 0))
    mock_cursor._c = mock_connection
    mock_cursor.ps = {}
    mock_cursor._SHOW_GRANTS_TABLE_Col_index = {}
    mock_metadataAPIPostProcessor: MetadataAPIPostProcessor = MetadataAPIPostProcessor(mock_cursor)

    # Create Result set + column description to mock the Result return from Server API call
    mock_metadataAPIPostProcessor.set_row_description(show_grant_table_col) # Create the column description

    intermediate_rs: typing.Tuple = ()                                      # Create result set
    intermediate_row: typing.List = show_grant_table_res                  # Create result row
    intermediate_rs += (intermediate_row,)
    intermediate_rs_list:typing.List  = []                                  # Create result set list
    intermediate_rs_list.append(intermediate_rs)

    intermediate_column = mock_cursor.description                           # Create the column name/index mapping
    for col, i in zip(intermediate_column, range(len(intermediate_column))):
        mock_cursor._SHOW_GRANTS_TABLE_Col_index[col[0]] = int(i)

    # Apply post-processing
    final_rs: typing.Tuple = mock_metadataAPIPostProcessor.get_table_privileges_post_processing(intermediate_rs_list)

    # Verify the final result
    final_column = mock_cursor.description
    assert len(final_column) == len(colName[MetadataAPI.get_table_privileges])
    assert len(final_column) == len(colType[MetadataAPI.get_table_privileges])

    expected_result = ['testCatalog','testSchema','testTable','grantor','grantee','select','YES']
    for row in final_rs:
        for res1, res2 in zip(row, expected_result):
            assert res1 == res2

def show_procedures_column() -> typing.Dict:
    return {'database_name': int(RedshiftOID.VARCHAR),
            'schema_name': int(RedshiftOID.VARCHAR),
            'procedure_name': int(RedshiftOID.VARCHAR),
            'number_of_argument': int(RedshiftOID.INTEGER),
            'argument_list': int(RedshiftOID.VARCHAR),
            'return_type': int(RedshiftOID.VARCHAR)}

def show_procedures_column_extra() -> typing.Dict:
    return {'database_name': int(RedshiftOID.VARCHAR),
            'schema_name': int(RedshiftOID.VARCHAR),
            'procedure_name': int(RedshiftOID.VARCHAR),
            'number_of_argument': int(RedshiftOID.INTEGER),
            'argument_list': int(RedshiftOID.VARCHAR),
            'return_type': int(RedshiftOID.VARCHAR),
            'extra_1': int(RedshiftOID.VARCHAR),
            'extra_2': int(RedshiftOID.INTEGER),
            'extra_3': int(RedshiftOID.INTEGER)}

def show_procedures_column_diff_order() -> typing.Dict:
    return {'return_type': int(RedshiftOID.VARCHAR),
            'database_name': int(RedshiftOID.VARCHAR),
            'schema_name': int(RedshiftOID.VARCHAR),
            'procedure_name': int(RedshiftOID.VARCHAR),
            'number_of_argument': int(RedshiftOID.INTEGER),
            'argument_list': int(RedshiftOID.VARCHAR)}


SHOW_PROCEDURES_COL: typing.List[typing.Dict] = [show_procedures_column(), show_procedures_column_extra(), show_procedures_column_diff_order()]
SHOW_PROCEDURES_RES: typing.List[typing.List] = [
    ['testCatalog','testSchema','testProcedure','4','smallint, integer, bigint, numeric', 'integer'],
    ['testCatalog','testSchema','testProcedure','4','smallint, integer, bigint, numeric', 'integer','extra_1','1','2'],
    ['integer','testCatalog','testSchema','testProcedure','4', 'smallint, integer, bigint, numeric']]

@pytest.mark.parametrize("show_procedures_col, show_procedures_res", list(zip(SHOW_PROCEDURES_COL, SHOW_PROCEDURES_RES)))
def test_get_procedures_post_processing(show_procedures_col, show_procedures_res) -> None:
    mock_cursor: Cursor = Cursor.__new__(Cursor)
    mock_cursor.paramstyle = "mocked"
    mock_connection: Connection = Connection.__new__(Connection)
    mock_connection.parameter_statuses = deque(maxlen=100)
    mock_connection.parameter_statuses.append((b"show_discovery", 0))
    mock_cursor._c = mock_connection
    mock_cursor.ps = {}
    mock_cursor._SHOW_PROCEDURES_Col_index = {}
    mock_metadataAPIPostProcessor: MetadataAPIPostProcessor = MetadataAPIPostProcessor(mock_cursor)

    # Create Result set + column description to mock the Result return from Server API call
    mock_metadataAPIPostProcessor.set_row_description(show_procedures_col) # Create the column description

    intermediate_rs: typing.Tuple = ()                                      # Create result set
    intermediate_row: typing.List = show_procedures_res                  # Create result row
    intermediate_rs += (intermediate_row,)
    intermediate_rs_list:typing.List  = []                                  # Create result set list
    intermediate_rs_list.append(intermediate_rs)

    intermediate_column = mock_cursor.description                           # Create the column name/index mapping
    for col, i in zip(intermediate_column, range(len(intermediate_column))):
        mock_cursor._SHOW_PROCEDURES_Col_index[col[0]] = int(i)

    # Apply post-processing
    final_rs: typing.Tuple = mock_metadataAPIPostProcessor.get_procedures_post_processing(intermediate_rs_list)

    # Verify the final result
    final_column = mock_cursor.description
    assert len(final_column) == len(colName[MetadataAPI.get_procedures])
    assert len(final_column) == len(colType[MetadataAPI.get_procedures])

    expected_result = ['testCatalog','testSchema','testProcedure', None, None, None, '', 2, 'testProcedure(smallint, integer, bigint, numeric)']
    for row in final_rs:
        for res1, res2 in zip(row, expected_result):
            assert res1 == res2


def show_parameters_procedure_column() -> typing.Dict:
    return {'database_name': int(RedshiftOID.VARCHAR),
            'schema_name': int(RedshiftOID.VARCHAR),
            'procedure_name': int(RedshiftOID.VARCHAR),
            'parameter_name': int(RedshiftOID.VARCHAR),
            'ordinal_position': int(RedshiftOID.INTEGER),
            'parameter_type': int(RedshiftOID.VARCHAR),
            'data_type': int(RedshiftOID.VARCHAR),
            'character_maximum_length': int(RedshiftOID.INTEGER),
            'numeric_precision': int(RedshiftOID.INTEGER),
            'numeric_scale': int(RedshiftOID.INTEGER),
            'specific_name': int(RedshiftOID.VARCHAR)}

def show_parameters_procedure_column_extra() -> typing.Dict:
    return {'database_name': int(RedshiftOID.VARCHAR),
            'schema_name': int(RedshiftOID.VARCHAR),
            'procedure_name': int(RedshiftOID.VARCHAR),
            'parameter_name': int(RedshiftOID.VARCHAR),
            'ordinal_position': int(RedshiftOID.INTEGER),
            'parameter_type': int(RedshiftOID.VARCHAR),
            'data_type': int(RedshiftOID.VARCHAR),
            'character_maximum_length': int(RedshiftOID.INTEGER),
            'numeric_precision': int(RedshiftOID.INTEGER),
            'numeric_scale': int(RedshiftOID.INTEGER),
            'specific_name': int(RedshiftOID.VARCHAR),
            'extra_1': int(RedshiftOID.VARCHAR),
            'extra_2': int(RedshiftOID.INTEGER),
            'extra_3': int(RedshiftOID.INTEGER)}

def show_parameters_procedure_column_diff_order() -> typing.Dict:
    return {'numeric_scale': int(RedshiftOID.INTEGER),
            'database_name': int(RedshiftOID.VARCHAR),
            'schema_name': int(RedshiftOID.VARCHAR),
            'procedure_name': int(RedshiftOID.VARCHAR),
            'parameter_name': int(RedshiftOID.VARCHAR),
            'ordinal_position': int(RedshiftOID.INTEGER),
            'parameter_type': int(RedshiftOID.VARCHAR),
            'data_type': int(RedshiftOID.VARCHAR),
            'character_maximum_length': int(RedshiftOID.INTEGER),
            'numeric_precision': int(RedshiftOID.INTEGER),
            'specific_name': int(RedshiftOID.VARCHAR)}
SHOW_COLUMNS_PRO_COL: typing.List[typing.Dict] = [show_parameters_procedure_column(), show_parameters_procedure_column_extra(), show_parameters_procedure_column_diff_order()]
@pytest.mark.parametrize("show_parameters_col, diff_order", list(zip(SHOW_COLUMNS_PRO_COL, [False,False,True])))
@pytest.mark.parametrize("parameter_type, column_type", [
    ("IN", ProcedureColumnType.IN),
    ("OUT", ProcedureColumnType.OUT),
    ("INOUT", ProcedureColumnType.IN_OUT),
    ("TABLE", ProcedureColumnType.RESULT),
    ("RETURN", ProcedureColumnType.RETURN),
    ("", ProcedureColumnType.UNKNOWN),
    (None, ProcedureColumnType.UNKNOWN),
])
def test_get_procedure_columns_post_processing(show_parameters_col,diff_order, parameter_type, column_type) -> None:
    mock_cursor: Cursor = Cursor.__new__(Cursor)
    mock_cursor.paramstyle = "mocked"
    mock_connection: Connection = Connection.__new__(Connection)
    mock_connection.parameter_statuses = deque(maxlen=100)
    mock_connection.parameter_statuses.append((b"show_discovery", 0))
    mock_cursor._c = mock_connection
    mock_cursor.ps = {}
    mock_cursor._SHOW_PARAMETERS_PRO_Col_index = {}
    mock_metadataAPIPostProcessor: MetadataAPIPostProcessor = MetadataAPIPostProcessor(mock_cursor)

    # Create Result set + column description to mock the Result return from Server API call
    mock_metadataAPIPostProcessor.set_row_description(show_parameters_col)        # Create the column description

    intermediate_rs: typing.Tuple = ()                                          # Create result set
    for index in range(len(col_name_list)):
        intermediate_row: typing.List = []                                      # Create result row
        if diff_order:
            intermediate_row.append(numeric_scale_list[index])
        intermediate_row.append("testCatalog")
        intermediate_row.append("testSchema")
        intermediate_row.append("testProcedure")
        intermediate_row.append(col_name_list[index])
        intermediate_row.append(index+1)
        intermediate_row.append(parameter_type)
        intermediate_row.append(data_type_list[index])
        intermediate_row.append(character_maximum_length_list[index])
        intermediate_row.append(numeric_precision_list[index])
        if not diff_order:
            intermediate_row.append(numeric_scale_list[index])
        intermediate_row.append("specificName")
        intermediate_rs += (intermediate_row,)
    intermediate_rs_list:typing.List  = []                                      # Create result set list
    intermediate_rs_list.append(intermediate_rs)

    intermediate_column = mock_cursor.description                               # Create the column name/index mapping
    for col, i in zip(intermediate_column, range(len(intermediate_column))):
        mock_cursor._SHOW_PARAMETERS_PRO_Col_index[col[0]] = int(i)

    # Apply post-processing
    final_rs: typing.Tuple = mock_metadataAPIPostProcessor.get_procedure_columns_post_processing(intermediate_rs_list)

    # Verify the final result
    final_column = mock_cursor.description
    assert len(final_column) == len(colName[MetadataAPI.get_procedure_columns])
    assert len(final_column) == len(colType[MetadataAPI.get_procedure_columns])

    expected_result = [
        ["testCatalog", "testSchema", "testProcedure", "testColumnNameSmallint", column_type, 5, "int2", 5, 2, 0, 10, 2, "", None, 5, None, None, 1, "", "specificName"],
        ["testCatalog", "testSchema", "testProcedure", "testColumnNameInteger", column_type, 4, "int4", 10, 4, 0, 10, 2, "", None, 4, None, None, 2, "", "specificName"],
        ["testCatalog", "testSchema", "testProcedure", "testColumnNameBigint", column_type, -5, "int8", 19, 20, 0, 10, 2, "", None, -5, None, None, 3, "", "specificName"],
        ["testCatalog", "testSchema", "testProcedure", "testColumnNameNumeric", column_type, 2, "numeric", 10, None, 5, 10, 2, "", None, 2, None, None, 4, "", "specificName"],
        ["testCatalog", "testSchema", "testProcedure", "testColumnNameReal", column_type, 7, "float4", 8, 4, 8, 10, 2, "", None, 7, None, None, 5, "", "specificName"],
        ["testCatalog", "testSchema", "testProcedure", "testColumnNameDouble", column_type, 8, "float8", 17, 8, 17, 10, 2, "", None, 8, None, None, 6, "", "specificName"],
        ["testCatalog", "testSchema", "testProcedure", "testColumnNameBoolean", column_type, -7, "bool", 1, 1, 0, 10, 2, "", None, -7, None, None, 7, "", "specificName"],
        ["testCatalog", "testSchema", "testProcedure", "testColumnNameChar", column_type, 1, "char", 20, None, 0, 10, 2, "", None, 1, None, None, 8, "", "specificName"],
        ["testCatalog", "testSchema", "testProcedure", "testColumnNameVarchar", column_type, 12, "varchar", 256, None, 0, 10, 2, "", None, 12, None, None, 9, "", "specificName"],
        ["testCatalog", "testSchema", "testProcedure", "testColumnNameDate", column_type, 91, "date", 13, 6, 0, 10, 2, "", None, 91, None, None, 10, "", "specificName"],
        ["testCatalog", "testSchema", "testProcedure", "testColumnNameTime", column_type, 92, "time", 15, 15, 1, 10, 2, "", None, 92, None, None, 11, "",  "specificName"],
        ["testCatalog", "testSchema", "testProcedure", "testColumnNameTimetz", column_type, 2013, "timetz", 21, 21, 2, 10, 2, "", None, 2013, None, None, 12, "", "specificName"],
        ["testCatalog", "testSchema", "testProcedure", "testColumnNameTimestamp", column_type, 93, "timestamp", 29, 6, 3, 10, 2, "", None, 93, None, None, 13, "", "specificName"],
        ["testCatalog", "testSchema", "testProcedure", "testColumnNameTimestamptz", column_type, 2014, "timestamptz", 35, 35, 6, 10, 2, "", None, 2014, None, None, 14, "", "specificName"],
        ["testCatalog", "testSchema", "testProcedure", "testColumnNameIntervaly2m", column_type, 1111, "intervaly2m", 32, 4, 0, 10, 2, "", None, 1111, None, None, 15, "", "specificName"],
        ["testCatalog", "testSchema", "testProcedure", "testColumnNameIntervald2s", column_type, 1111, "intervald2s", 64, 8, 4, 10, 2, "", None, 1111, None, None, 16, "",  "specificName"]
    ]
    for row1, row2 in zip(final_rs, expected_result):
        for res1, res2 in zip(row1, row2):
            assert res1 == res2

def show_functions_column() -> typing.Dict:
    return {'database_name': int(RedshiftOID.VARCHAR),
            'schema_name': int(RedshiftOID.VARCHAR),
            'function_name': int(RedshiftOID.VARCHAR),
            'return_type': int(RedshiftOID.INTEGER),
            'argument_list': int(RedshiftOID.VARCHAR)}

def show_functions_column_extra() -> typing.Dict:
    return {'database_name': int(RedshiftOID.VARCHAR),
            'schema_name': int(RedshiftOID.VARCHAR),
            'function_name': int(RedshiftOID.VARCHAR),
            'return_type': int(RedshiftOID.INTEGER),
            'argument_list': int(RedshiftOID.VARCHAR),
            'extra_1': int(RedshiftOID.VARCHAR),
            'extra_2': int(RedshiftOID.INTEGER),
            'extra_3': int(RedshiftOID.INTEGER)}

def show_functions_column_diff_order() -> typing.Dict:
    return {'argument_list': int(RedshiftOID.VARCHAR),
            'database_name': int(RedshiftOID.VARCHAR),
            'schema_name': int(RedshiftOID.VARCHAR),
            'function_name': int(RedshiftOID.VARCHAR),
            'return_type': int(RedshiftOID.INTEGER)}


SHOW_FUNCTIONS_COL: typing.List[typing.Dict] = [show_functions_column(), show_functions_column_extra(), show_functions_column_diff_order()]
SHOW_FUNCTIONS_RES: typing.List[typing.List] = [
    ['testCatalog','testSchema','testFunction','1','smallint, integer, bigint, numeric'],
    ['testCatalog','testSchema','testFunction','1','smallint, integer, bigint, numeric','extra_1','1','2'],
    ['smallint, integer, bigint, numeric','testCatalog','testSchema','testFunction','1']]

@pytest.mark.parametrize("show_functions_col, show_functions_res", list(zip(SHOW_FUNCTIONS_COL, SHOW_FUNCTIONS_RES)))
def test_get_functions_post_processing(show_functions_col, show_functions_res) -> None:
    mock_cursor: Cursor = Cursor.__new__(Cursor)
    mock_cursor.paramstyle = "mocked"
    mock_connection: Connection = Connection.__new__(Connection)
    mock_connection.parameter_statuses = deque(maxlen=100)
    mock_connection.parameter_statuses.append((b"show_discovery", 0))
    mock_cursor._c = mock_connection
    mock_cursor.ps = {}
    mock_cursor._SHOW_FUNCTIONS_Col_index = {}
    mock_metadataAPIPostProcessor: MetadataAPIPostProcessor = MetadataAPIPostProcessor(mock_cursor)

    # Create Result set + column description to mock the Result return from Server API call
    mock_metadataAPIPostProcessor.set_row_description(show_functions_col) # Create the column description

    intermediate_rs: typing.Tuple = ()                                      # Create result set
    intermediate_row: typing.List = show_functions_res                  # Create result row
    intermediate_rs += (intermediate_row,)
    intermediate_rs_list:typing.List  = []                                  # Create result set list
    intermediate_rs_list.append(intermediate_rs)

    intermediate_column = mock_cursor.description                           # Create the column name/index mapping
    for col, i in zip(intermediate_column, range(len(intermediate_column))):
        mock_cursor._SHOW_FUNCTIONS_Col_index[col[0]] = int(i)

    # Apply post-processing
    final_rs: typing.Tuple = mock_metadataAPIPostProcessor.get_functions_post_processing(intermediate_rs_list)

    # Verify the final result
    final_column = mock_cursor.description
    assert len(final_column) == len(colName[MetadataAPI.get_functions])
    assert len(final_column) == len(colType[MetadataAPI.get_functions])

    expected_result = ['testCatalog','testSchema','testFunction', '', 1,'testFunction(smallint, integer, bigint, numeric)']
    for row in final_rs:
        for res1, res2 in zip(row, expected_result):
            assert res1 == res2

def show_parameters_function_column() -> typing.Dict:
    return {'database_name': int(RedshiftOID.VARCHAR),
            'schema_name': int(RedshiftOID.VARCHAR),
            'function_name': int(RedshiftOID.VARCHAR),
            'parameter_name': int(RedshiftOID.VARCHAR),
            'ordinal_position': int(RedshiftOID.INTEGER),
            'parameter_type': int(RedshiftOID.VARCHAR),
            'data_type': int(RedshiftOID.VARCHAR),
            'character_maximum_length': int(RedshiftOID.INTEGER),
            'numeric_precision': int(RedshiftOID.INTEGER),
            'numeric_scale': int(RedshiftOID.INTEGER),
            'specific_name': int(RedshiftOID.VARCHAR)}

def show_parameters_function_column_extra() -> typing.Dict:
    return {'database_name': int(RedshiftOID.VARCHAR),
            'schema_name': int(RedshiftOID.VARCHAR),
            'function_name': int(RedshiftOID.VARCHAR),
            'parameter_name': int(RedshiftOID.VARCHAR),
            'ordinal_position': int(RedshiftOID.INTEGER),
            'parameter_type': int(RedshiftOID.VARCHAR),
            'data_type': int(RedshiftOID.VARCHAR),
            'character_maximum_length': int(RedshiftOID.INTEGER),
            'numeric_precision': int(RedshiftOID.INTEGER),
            'numeric_scale': int(RedshiftOID.INTEGER),
            'specific_name': int(RedshiftOID.VARCHAR),
            'extra_1': int(RedshiftOID.VARCHAR),
            'extra_2': int(RedshiftOID.INTEGER),
            'extra_3': int(RedshiftOID.INTEGER)}

def show_parameters_function_column_diff_order() -> typing.Dict:
    return {'numeric_scale': int(RedshiftOID.INTEGER),
            'database_name': int(RedshiftOID.VARCHAR),
            'schema_name': int(RedshiftOID.VARCHAR),
            'function_name': int(RedshiftOID.VARCHAR),
            'parameter_name': int(RedshiftOID.VARCHAR),
            'ordinal_position': int(RedshiftOID.INTEGER),
            'parameter_type': int(RedshiftOID.VARCHAR),
            'data_type': int(RedshiftOID.VARCHAR),
            'character_maximum_length': int(RedshiftOID.INTEGER),
            'numeric_precision': int(RedshiftOID.INTEGER),
            'specific_name': int(RedshiftOID.VARCHAR)}
SHOW_COLUMNS_FUNC_COL: typing.List[typing.Dict] = [show_parameters_function_column(), show_parameters_function_column_extra(), show_parameters_function_column_diff_order()]
@pytest.mark.parametrize("show_parameters_col, diff_order", list(zip(SHOW_COLUMNS_FUNC_COL, [False,False,True])))
@pytest.mark.parametrize("parameter_type, column_type", [
    ("IN", FunctionColumnType.IN),
    ("OUT", FunctionColumnType.OUT),
    ("INOUT", FunctionColumnType.IN_OUT),
    ("TABLE", FunctionColumnType.RESULT),
    ("RETURN", FunctionColumnType.RETURN),
    ("", FunctionColumnType.UNKNOWN),
    (None, FunctionColumnType.UNKNOWN),
])
def test_get_function_columns_post_processing(show_parameters_col,diff_order, parameter_type, column_type) -> None:
    mock_cursor: Cursor = Cursor.__new__(Cursor)
    mock_cursor.paramstyle = "mocked"
    mock_connection: Connection = Connection.__new__(Connection)
    mock_connection.parameter_statuses = deque(maxlen=100)
    mock_connection.parameter_statuses.append((b"show_discovery", 0))
    mock_cursor._c = mock_connection
    mock_cursor.ps = {}
    mock_cursor._SHOW_PARAMETERS_FUNC_Col_index = {}
    mock_metadataAPIPostProcessor: MetadataAPIPostProcessor = MetadataAPIPostProcessor(mock_cursor)

    # Create Result set + column description to mock the Result return from Server API call
    mock_metadataAPIPostProcessor.set_row_description(show_parameters_col)        # Create the column description

    intermediate_rs: typing.Tuple = ()                                          # Create result set
    for index in range(len(col_name_list)):
        intermediate_row: typing.List = []                                      # Create result row
        if diff_order:
            intermediate_row.append(numeric_scale_list[index])
        intermediate_row.append("testCatalog")
        intermediate_row.append("testSchema")
        intermediate_row.append("testFunction")
        intermediate_row.append(col_name_list[index])
        intermediate_row.append(index+1)
        intermediate_row.append(parameter_type)
        intermediate_row.append(data_type_list[index])
        intermediate_row.append(character_maximum_length_list[index])
        intermediate_row.append(numeric_precision_list[index])
        if not diff_order:
            intermediate_row.append(numeric_scale_list[index])
        intermediate_row.append("specificName")
        intermediate_rs += (intermediate_row,)
    intermediate_rs_list:typing.List  = []                                      # Create result set list
    intermediate_rs_list.append(intermediate_rs)

    intermediate_column = mock_cursor.description                               # Create the column name/index mapping
    for col, i in zip(intermediate_column, range(len(intermediate_column))):
        mock_cursor._SHOW_PARAMETERS_FUNC_Col_index[col[0]] = int(i)

    # Apply post-processing
    final_rs: typing.Tuple = mock_metadataAPIPostProcessor.get_function_columns_post_processing(intermediate_rs_list)

    # Verify the final result
    final_column = mock_cursor.description
    assert len(final_column) == len(colName[MetadataAPI.get_function_columns])
    assert len(final_column) == len(colType[MetadataAPI.get_function_columns])

    expected_result = [
        ["testCatalog", "testSchema", "testFunction", "testColumnNameSmallint", column_type, 5, "int2", 5, 2, 0, 10, 2, "", None, 1, "", "specificName"],
        ["testCatalog", "testSchema", "testFunction", "testColumnNameInteger", column_type, 4, "int4", 10, 4, 0, 10, 2, "", None, 2, "", "specificName"],
        ["testCatalog", "testSchema", "testFunction", "testColumnNameBigint", column_type, -5, "int8", 19, 20, 0, 10, 2, "", None, 3, "", "specificName"],
        ["testCatalog", "testSchema", "testFunction", "testColumnNameNumeric", column_type, 2, "numeric", 10, None, 5, 10, 2, "", None, 4, "", "specificName"],
        ["testCatalog", "testSchema", "testFunction", "testColumnNameReal", column_type, 7, "float4", 8, 4, 8, 10, 2, "", None, 5, "", "specificName"],
        ["testCatalog", "testSchema", "testFunction", "testColumnNameDouble", column_type, 8, "float8", 17, 8, 17, 10, 2, "", None, 6, "", "specificName"],
        ["testCatalog", "testSchema", "testFunction", "testColumnNameBoolean", column_type, -7, "bool", 1, 1, 0, 10, 2, "", None, 7, "", "specificName"],
        ["testCatalog", "testSchema", "testFunction", "testColumnNameChar", column_type, 1, "char", 20, None, 0, 10, 2, "", None, 8, "", "specificName"],
        ["testCatalog", "testSchema", "testFunction", "testColumnNameVarchar", column_type, 12, "varchar", 256, None, 0, 10, 2, "", None, 9, "", "specificName"],
        ["testCatalog", "testSchema", "testFunction", "testColumnNameDate", column_type, 91, "date", 13, 6, 0, 10, 2, "", None, 10, "", "specificName"],
        ["testCatalog", "testSchema", "testFunction", "testColumnNameTime", column_type, 92, "time", 15, 15, 1, 10, 2, "", None, 11, "",  "specificName"],
        ["testCatalog", "testSchema", "testFunction", "testColumnNameTimetz", column_type, 2013, "timetz", 21, 21, 2, 10, 2, "", None, 12, "", "specificName"],
        ["testCatalog", "testSchema", "testFunction", "testColumnNameTimestamp", column_type, 93, "timestamp", 29, 6, 3, 10, 2, "", None, 13, "", "specificName"],
        ["testCatalog", "testSchema", "testFunction", "testColumnNameTimestamptz", column_type, 2014, "timestamptz", 35, 35, 6, 10, 2, "", None, 14, "", "specificName"],
        ["testCatalog", "testSchema", "testFunction", "testColumnNameIntervaly2m", column_type, 1111, "intervaly2m", 32, 4, 0, 10, 2, "", None, 15, "", "specificName"],
        ["testCatalog", "testSchema", "testFunction", "testColumnNameIntervald2s", column_type, 1111, "intervald2s", 64, 8, 4, 10, 2, "", None, 16, "",  "specificName"]
    ]
    for row1, row2 in zip(final_rs, expected_result):
        for res1, res2 in zip(row1, row2):
            assert res1 == res2

def test_get_table_types_post_processing() -> None:
    mock_cursor: Cursor = Cursor.__new__(Cursor)
    mock_cursor.paramstyle = "mocked"
    mock_connection: Connection = Connection.__new__(Connection)
    mock_connection.parameter_statuses = deque(maxlen=100)
    mock_connection.parameter_statuses.append((b"show_discovery", 0))
    mock_cursor._c = mock_connection
    mock_cursor.ps = {}
    mock_metadataAPIPostProcessor: MetadataAPIPostProcessor = MetadataAPIPostProcessor(mock_cursor)

    # Create Result set + column description to mock the Result return from Server API call
    mock_metadataAPIPostProcessor.set_row_description({"TABLE_TYPE": int(RedshiftOID.VARCHAR)}) # Create the column description

    intermediate_rs: typing.List = [("EXTERNAL TABLE",), ("EXTERNAL VIEW",), ("LOCAL TEMPORARY",), ("TABLE",), ("VIEW",)]

    # Apply post-processing
    final_rs: typing.Tuple = mock_metadataAPIPostProcessor.get_table_types_post_processing(intermediate_rs)

    # Verify the final result
    final_column = mock_cursor.description
    assert len(final_column) == len(colName[MetadataAPI.get_table_types])
    assert len(final_column) == len(colType[MetadataAPI.get_table_types])

    expected_result = (["EXTERNAL TABLE"], ["EXTERNAL VIEW"], ["LOCAL TEMPORARY"], ["TABLE"], ["VIEW"])
    assert final_rs == expected_result

