import pytest  # type: ignore
import typing
from collections import deque
from redshift_connector.utils.oids import RedshiftOID
from redshift_connector.metadataAPIPostProcessing import MetadataAPIPostProcessing
from redshift_connector import Connection, Cursor, DataError, InterfaceError

colName = [["TABLE_CAT"],
           ["TABLE_SCHEM", "TABLE_CATALOG"],
           ["TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE", "REMARKS",
            "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "SELF_REFERENCING_COL_NAME", "REF_GENERATION"],
           ["TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE",
	        "TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS", "NUM_PREFIX_RADIX",
	        "NULLABLE", "REMARKS", "COLUMN_DEF", "SQL_DATA_TYPE", "SQL_DATETIME_SUB",
	        "CHAR_OCTET_LENGTH", "ORDINAL_POSITION", "IS_NULLABLE", "SCOPE_CATALOG", "SCOPE_SCHEMA",
	        "SCOPE_TABLE", "SOURCE_DATA_TYPE", "IS_AUTOINCREMENT", "IS_GENERATEDCOLUMN"]]

colType = [[RedshiftOID.VARCHAR],
           [RedshiftOID.VARCHAR, RedshiftOID.VARCHAR],
           [RedshiftOID.VARCHAR, RedshiftOID.VARCHAR, RedshiftOID.VARCHAR, RedshiftOID.VARCHAR, RedshiftOID.VARCHAR,
            RedshiftOID.VARCHAR, RedshiftOID.VARCHAR, RedshiftOID.VARCHAR, RedshiftOID.VARCHAR, RedshiftOID.VARCHAR],
           [RedshiftOID.VARCHAR, RedshiftOID.VARCHAR, RedshiftOID.VARCHAR, RedshiftOID.VARCHAR, RedshiftOID.INTEGER,
            RedshiftOID.VARCHAR, RedshiftOID.INTEGER, RedshiftOID.INTEGER, RedshiftOID.INTEGER, RedshiftOID.INTEGER,
	        RedshiftOID.INTEGER, RedshiftOID.VARCHAR, RedshiftOID.VARCHAR, RedshiftOID.INTEGER, RedshiftOID.INTEGER,
            RedshiftOID.INTEGER, RedshiftOID.INTEGER, RedshiftOID.VARCHAR, RedshiftOID.VARCHAR, RedshiftOID.VARCHAR,
	        RedshiftOID.VARCHAR, RedshiftOID.SMALLINT, RedshiftOID.VARCHAR, RedshiftOID.VARCHAR]]

expected_result_set = [["testCatalog"],
          ["testSchema","testCatalog"],
          ["testCatalog", "testSchema","testTable","testTableType","testRemarks","","","","",""]]

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
    mock_metadataAPIPostProcessing: MetadataAPIPostProcessing = MetadataAPIPostProcessing(mock_cursor)

    # Create Result set + column description to mock the Result return from Server API call
    mock_metadataAPIPostProcessing.set_row_description(show_databases_col) # Create the column description

    intermediate_rs: typing.Tuple = ()                                      # Create result set
    intermediate_row: typing.List = show_databases_res                      # Create result row
    intermediate_rs += (intermediate_row,)

    intermediate_column = mock_cursor.description                           # Create the column name/index mapping
    for col, i in zip(intermediate_column, range(len(intermediate_column))):
        mock_cursor._SHOW_DATABASES_Col_index[col[0]] = int(i)

    # Apply post-processing
    final_rs: typing.Tuple = mock_metadataAPIPostProcessing.get_catalog_post_processing(intermediate_rs)

    # Verify the final result
    final_column = mock_cursor.description
    assert len(final_column) == len(colName[0])
    assert len(final_column) == len(colType[0])

    for row in final_rs:
        for res1, res2 in zip(row, expected_result_set[0]):
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
    mock_metadataAPIPostProcessing: MetadataAPIPostProcessing = MetadataAPIPostProcessing(mock_cursor)

    # Create Result set + column description to mock the Result return from Server API call
    mock_metadataAPIPostProcessing.set_row_description(show_schemas_col) # Create the column description

    intermediate_rs: typing.Tuple = ()                                      # Create result set
    intermediate_row: typing.List = show_schemas_res                        # Create result row
    intermediate_rs += (intermediate_row,)
    intermediate_rs_list:typing.List  = []                                  # Create result set list
    intermediate_rs_list.append(intermediate_rs)

    intermediate_column = mock_cursor.description                           # Create the column name/index mapping
    for col, i in zip(intermediate_column, range(len(intermediate_column))):
        mock_cursor._SHOW_SCHEMAS_Col_index[col[0]] = int(i)

    # Apply post-processing
    final_rs: typing.Tuple = mock_metadataAPIPostProcessing.get_schema_post_processing(intermediate_rs_list)

    # Verify the final result
    final_column = mock_cursor.description
    assert len(final_column) == len(colName[1])
    assert len(final_column) == len(colType[1])

    for row in final_rs:
        for res1, res2 in zip(row, expected_result_set[1]):
            assert res1 == res2


def show_tables_column() -> typing.Dict:
    return {'database_name': int(RedshiftOID.VARCHAR),
            'schema_name': int(RedshiftOID.VARCHAR),
            'table_name': int(RedshiftOID.VARCHAR),
            'table_type': int(RedshiftOID.VARCHAR),
            'table_acl': int(RedshiftOID.VARCHAR),
            'remarks': int(RedshiftOID.VARCHAR)}

def show_tables_column_extra() -> typing.Dict:
    return {'database_name': int(RedshiftOID.VARCHAR),
            'schema_name': int(RedshiftOID.VARCHAR),
            'table_name': int(RedshiftOID.VARCHAR),
            'table_type': int(RedshiftOID.VARCHAR),
            'table_acl': int(RedshiftOID.VARCHAR),
            'remarks': int(RedshiftOID.VARCHAR),
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
            'table_acl': int(RedshiftOID.VARCHAR)}


SHOW_TABLES_COL: typing.List[typing.Dict] = [show_tables_column(), show_tables_column_extra(), show_tables_column_diff_order()]
SHOW_TABLES_RES: typing.List[typing.List] = [['testCatalog','testSchema','testTable','testTableType','testAcl','testRemarks'], ['testCatalog','testSchema','testTable','testTableType','testAcl','testRemarks','extra_1','extra_2','1','2'], ['testRemarks','testCatalog','testSchema','testTable','testTableType','testAcl']]

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
    mock_metadataAPIPostProcessing: MetadataAPIPostProcessing = MetadataAPIPostProcessing(mock_cursor)

    # Create Result set + column description to mock the Result return from Server API call
    mock_metadataAPIPostProcessing.set_row_description(show_tables_col) # Create the column description

    intermediate_rs: typing.Tuple = ()                                      # Create result set
    intermediate_row: typing.List = show_tables_res                         # Create result row
    intermediate_rs += (intermediate_row,)
    intermediate_rs_list:typing.List  = []                                  # Create result set list
    intermediate_rs_list.append(intermediate_rs)

    intermediate_column = mock_cursor.description                           # Create the column name/index mapping
    for col, i in zip(intermediate_column, range(len(intermediate_column))):
        mock_cursor._SHOW_TABLES_Col_index[col[0]] = int(i)

    # Apply post-processing
    final_rs: typing.Tuple = mock_metadataAPIPostProcessing.get_table_post_processing(intermediate_rs_list, ['testTableType'])

    # Verify the final result
    final_column = mock_cursor.description
    assert len(final_column) == len(colName[2])
    assert len(final_column) == len(colType[2])

    for row in final_rs:
        for res1, res2 in zip(row, expected_result_set[2]):
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
            'remarks': int(RedshiftOID.VARCHAR)}

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
            'numeric_scale': int(RedshiftOID.INTEGER)}

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
    mock_metadataAPIPostProcessing: MetadataAPIPostProcessing = MetadataAPIPostProcessing(mock_cursor)

    # Create Result set + column description to mock the Result return from Server API call
    mock_metadataAPIPostProcessing.set_row_description(show_columns_col)        # Create the column description

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
    final_rs: typing.Tuple = mock_metadataAPIPostProcessing.get_column_post_processing(intermediate_rs_list)

    # Verify the final result
    final_column = mock_cursor.description
    assert len(final_column) == len(colName[3])
    assert len(final_column) == len(colType[3])
    for row1, row2 in zip(final_rs, expected_result_get_columns):
        for res1, res2 in zip(row1, row2):
            assert res1 == res2


