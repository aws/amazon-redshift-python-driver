import typing
import pytest  # type: ignore

from redshift_connector.metadataAPIHelper import MetadataAPIHelper
from redshift_connector.utils.oids import RedshiftOID



colName = [["TABLE_CAT"],
           ["TABLE_SCHEM", "TABLE_CATALOG"],
           ["TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE", "REMARKS",
            "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "SELF_REFERENCING_COL_NAME", "REF_GENERATION"],
           ["TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE",
	        "TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS", "NUM_PREC_RADIX",
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

def test_get_catalogs_column() -> None:
    mock_metadataAPIHelper: MetadataAPIHelper = MetadataAPIHelper()

    catalogs_col: typing.Dict = mock_metadataAPIHelper._get_catalogs_col

    assert len(catalogs_col) == len(colName[0])
    assert len(catalogs_col) == len(colType[0])

    for i in range(len(catalogs_col)):
        assert catalogs_col[colName[0][i]] == colType[0][i]

def test_get_schemas_column() -> None:
    mock_metadataAPIHelper: MetadataAPIHelper = MetadataAPIHelper()

    schemas_col: typing.Dict = mock_metadataAPIHelper._get_schemas_col

    assert len(schemas_col) == len(colName[1])
    assert len(schemas_col) == len(colType[1])

    for i in range(len(schemas_col)):
        assert schemas_col[colName[1][i]] == colType[1][i]

def test_get_tables_column() -> None:
    mock_metadataAPIHelper: MetadataAPIHelper = MetadataAPIHelper()

    tables_col: typing.Dict = mock_metadataAPIHelper._get_tables_col

    assert len(tables_col) == len(colName[2])
    assert len(tables_col) == len(colType[2])

    for i in range(len(tables_col)):
        assert tables_col[colName[2][i]] == colType[2][i]

def test_get_columns_column() -> None:
    mock_metadataAPIHelper: MetadataAPIHelper = MetadataAPIHelper()

    columns_col: typing.Dict = mock_metadataAPIHelper._get_columns_col

    assert len(columns_col) == len(colName[3])
    assert len(columns_col) == len(colType[3])

    for i in range(len(columns_col)):
        assert columns_col[colName[3][i]] == colType[3][i]

@pytest.mark.parametrize("data_type, rs_type, cus_precision, precisions", list(zip(['time without time zone','time with time zone (3)','timestamp without time zone (4)','timestamp with time zone (5)','interval day to second','interval day to second (4)'],
                                                                                   ['time','timetz','timestamp','timestamptz','intervald2s','intervald2s'],
                                                                                   [False,True,True,True,False,True],
                                                                                   [6,3,4,5,6,4])))
def test_get_second_fraction(data_type, rs_type, cus_precision, precisions) -> None:
    mock_metadataAPIHelper: MetadataAPIHelper = MetadataAPIHelper()
    test_rs_type, test_cus_precision, test_precisions = mock_metadataAPIHelper.get_second_fraction(data_type)
    assert test_rs_type == rs_type
    assert test_cus_precision is cus_precision
    assert test_precisions == precisions

def test_get_nullable() -> None:
    mock_metadataAPIHelper: MetadataAPIHelper = MetadataAPIHelper.__new__(MetadataAPIHelper)

    assert 1 == mock_metadataAPIHelper.get_nullable('YES')
    assert 0 == mock_metadataAPIHelper.get_nullable('NO')
    assert 2 == mock_metadataAPIHelper.get_nullable('Cannot determine')

def test_get_auto_increment() -> None:
    mock_metadataAPIHelper: MetadataAPIHelper = MetadataAPIHelper.__new__(MetadataAPIHelper)

    assert 'YES' == mock_metadataAPIHelper.get_auto_increment("\"identity\"(211928, 0, '1,1'::text)")
    assert 'YES' == mock_metadataAPIHelper.get_auto_increment('default_identity')
    assert 'NO' == mock_metadataAPIHelper.get_auto_increment('')
    assert 'NO' == mock_metadataAPIHelper.get_auto_increment(None)

def test_check_name_is_not_pattern() -> None:
    mock_metadataAPIHelper: MetadataAPIHelper = MetadataAPIHelper.__new__(MetadataAPIHelper)

    assert True == mock_metadataAPIHelper.check_name_is_not_pattern(None)
    assert True == mock_metadataAPIHelper.check_name_is_not_pattern("")
    assert True == mock_metadataAPIHelper.check_name_is_not_pattern("%")

    assert False == mock_metadataAPIHelper.check_name_is_not_pattern("exact_name")
    assert False == mock_metadataAPIHelper.check_name_is_not_pattern("pattern_name%")


def test_check_name_is_exact_name() -> None:
    mock_metadataAPIHelper: MetadataAPIHelper = MetadataAPIHelper.__new__(MetadataAPIHelper)

    assert False == mock_metadataAPIHelper.check_name_is_exact_name(None)
    assert False == mock_metadataAPIHelper.check_name_is_exact_name("")
    assert False == mock_metadataAPIHelper.check_name_is_exact_name("%")

    assert True == mock_metadataAPIHelper.check_name_is_exact_name("exact_name")
    assert False == mock_metadataAPIHelper.check_name_is_exact_name("pattern_name%")

def test_get_column_size() -> None:
    mock_metadataAPIHelper: MetadataAPIHelper = MetadataAPIHelper.__new__(MetadataAPIHelper)

    numeric_precision: int = 10
    character_maximum_length: int = 200
    assert numeric_precision == mock_metadataAPIHelper.get_column_size("numeric", numeric_precision, character_maximum_length)
    assert character_maximum_length == mock_metadataAPIHelper.get_column_size("varchar", numeric_precision, character_maximum_length)

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
    assert numeric_scale == mock_metadataAPIHelper.get_decimal_digits("numeric", numeric_scale, precision, customizePrecision)
    assert precision == mock_metadataAPIHelper.get_decimal_digits("intervald2s", numeric_scale, precision, customizePrecision)

    assert None == mock_metadataAPIHelper.get_decimal_digits("super", numeric_scale, precision, customizePrecision)
    assert None == mock_metadataAPIHelper.get_decimal_digits("geometry", numeric_scale, precision, customizePrecision)
    assert None == mock_metadataAPIHelper.get_decimal_digits("geography", numeric_scale, precision, customizePrecision)
    assert None == mock_metadataAPIHelper.get_decimal_digits("varbyte", numeric_scale, precision, customizePrecision)

    assert 0 == mock_metadataAPIHelper.get_decimal_digits("other_datatype", numeric_scale, precision, customizePrecision)

def test_get_num_prefix_radix() -> None:
    mock_metadataAPIHelper: MetadataAPIHelper = MetadataAPIHelper.__new__(MetadataAPIHelper)

    assert 2 == mock_metadataAPIHelper.get_num_prefix_radix("geography")
    assert 2 == mock_metadataAPIHelper.get_num_prefix_radix("varbyte")

    assert 10 == mock_metadataAPIHelper.get_num_prefix_radix("other_datatype")