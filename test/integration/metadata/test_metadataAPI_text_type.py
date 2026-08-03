import typing

import pytest  # type: ignore

import redshift_connector
from metadata_test_utils import (
    get_connection_properties,
    setup_metadata_test_env,
    teardown_metadata_test_env,
)

# A view column whose server type resolves to text must report a bounded
# character type through get_columns, never SQL_OTHER with the unknown size
# sentinel. When the server reports the column as text, the driver maps it to
# SQL_VARCHAR named varchar and sized at 256, matching the documented Redshift
# conversion of TEXT to VARCHAR(256). The view uses a bare literal CASE
# expression, which the server types as text with no length. Expression type
# resolution varies across server versions (text on some, sized varchar on
# others), so the assertions accept either resolution while rejecting the
# broken fallback in both.

catalog: str = "text_type_test_db"
schema: str = "public"
view: str = "v_text256_check"

setup_statements: typing.Tuple[str, ...] = (
    "create view {}.{} as select case when 1 = 1 then 'yes' else 'no' end as case_text_col".format(schema, view),
)

SQL_VARCHAR: int = 12
SQL_OTHER: int = 1111
UNKNOWN_SIZE_SENTINEL: int = 2147483647

DATA_TYPE_IDX: int = 4
TYPE_NAME_IDX: int = 5
COLUMN_SIZE_IDX: int = 6
CHAR_OCTET_LENGTH_IDX: int = 15


@pytest.fixture(scope="class")
def text_type_test_env(db_kwargs) -> typing.Generator:
    setup_metadata_test_env(db_kwargs, catalog, setup_statements)
    yield
    teardown_metadata_test_env(db_kwargs, catalog)


@pytest.mark.usefixtures("text_type_test_env")
class TestGetColumnsTextType:
    @pytest.mark.parametrize("is_single_database_metadata", [True, False])
    def test_get_columns_text_column_reports_bounded_varchar(self, db_kwargs, is_single_database_metadata) -> None:
        cur_db_kwargs = get_connection_properties(db_kwargs, catalog, is_single_database_metadata)

        with redshift_connector.connect(**cur_db_kwargs) as conn:
            with conn.cursor() as cursor:
                if cursor.get_show_discovery_version() < 2:
                    pytest.skip("Server does not support SHOW discovery")

                result: typing.Tuple = cursor.get_columns(catalog, schema, view, "case_text_col")

                assert len(result) == 1
                row = result[0]

                data_type = int(row[DATA_TYPE_IDX])
                type_name = row[TYPE_NAME_IDX]
                column_size = int(row[COLUMN_SIZE_IDX])
                char_octet_length = int(row[CHAR_OCTET_LENGTH_IDX])

                # Never the broken fallback type or the unknown size sentinel
                assert data_type != SQL_OTHER
                assert column_size != UNKNOWN_SIZE_SENTINEL

                # A bounded character type in both server resolutions
                assert data_type == SQL_VARCHAR
                assert type_name == "varchar"
                assert 0 < column_size <= 256

                # When the server reports the column as text (no length), the
                # driver assigns exactly 256
                if column_size == 256:
                    assert char_octet_length == 256
