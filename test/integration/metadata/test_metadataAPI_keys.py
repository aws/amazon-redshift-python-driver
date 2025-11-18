import typing
import pytest  # type: ignore
import redshift_connector
from dataclasses import dataclass
from enum import IntEnum
from metadata_test_utils import setup_metadata_test_env, teardown_metadata_test_env, exact_matches, run_metadata_test


# Define constant
is_single_database_metadata_case: typing.List[bool] = [True, False]
current_catalog: str = "test_keys_catalog"
test_schema: str = "test_keys_schema"
scope: int = 1


class UpdateDeleteRule(IntEnum):
    CASCADE = 0
    RESTRICT = 1
    SET_NULL = 2
    NO_ACTION = 3
    SET_DEFAULT = 4


class Deferrability(IntEnum):
    INITIALLY_DEFERRED = 5
    INITIALLY_IMMEDIATE = 6
    NOT_DEFERRABLE = 7


class PseudoColumn(IntEnum):
    UNKNOWN = 0
    NOT_PSEUDO = 1
    PSEUDO = 2

# Define column constant for get_primary_keys
get_primary_keys_col_name: typing.List[str] = [
    "TABLE_CAT",
    "TABLE_SCHEM",
    "TABLE_NAME",
    "COLUMN_NAME",
    "KEY_SEQ",
    "PK_NAME"
]


# Define result structure for get_primary_keys
@dataclass
class PrimaryKeysInfo:
    table_cat: str
    table_schem: str
    table_name: str
    column_name: str
    key_seq: int
    pk_name: str


# Define expected returned result for get_primary_keys
get_primary_keys_result: typing.List[PrimaryKeysInfo] = [
    PrimaryKeysInfo(current_catalog, test_schema, "child_composite_fk", "child_id", 1, "child_composite_fk_pkey"),
    PrimaryKeysInfo(current_catalog, test_schema, "child_multiple_fk", "child_id", 1, "child_multiple_fk_pkey"),
    PrimaryKeysInfo(current_catalog, test_schema, "child_single_fk", "child_id", 1, "child_single_fk_pkey"),
    PrimaryKeysInfo(current_catalog, test_schema, "parent1", "parent_id", 1, "parent1_pkey"),
    PrimaryKeysInfo(current_catalog, test_schema, "parent2", "p_id1", 1, "parent2_pkey"),
    PrimaryKeysInfo(current_catalog, test_schema, "parent2", "p_id2", 2, "parent2_pkey"),
]


# Define column constant for get_imported_keys and get_exported_keys
get_foreign_keys_col_name: typing.List[str] = [
    "PKTABLE_CAT",
    "PKTABLE_SCHEM",
    "PKTABLE_NAME",
    "PKCOLUMN_NAME",
    "FKTABLE_CAT",
    "FKTABLE_SCHEM",
    "FKTABLE_NAME",
    "FKCOLUMN_NAME",
    "KEY_SEQ",
    "UPDATE_RULE",
    "DELETE_RULE",
    "FK_NAME",
    "PK_NAME",
    "DEFERRABILITY"
]


# Define result structure for get_imported_keys and get_exported_keys
@dataclass
class ForeignKeysInfo:
    pktable_cat: str
    pktable_schem: str
    pktable_name: str
    pkcolumn_name: str
    fktable_cat: str
    fktable_schem: str
    fktable_name: str
    fkcolum_name: str
    key_seq: int
    update_rule: int
    delete_rule: int
    fk_name: str
    pk_name: str
    deferrability: int


# Define expected returned result for get_imported_keys
get_imported_keys_result: typing.List[ForeignKeysInfo] = [
    ForeignKeysInfo(current_catalog, test_schema, "parent1", "parent_id", current_catalog, test_schema, "child_multiple_fk", "parent1_id", 1, UpdateDeleteRule.NO_ACTION, UpdateDeleteRule.NO_ACTION, "child_multiple_fk_parent1_id_fkey", "parent1_pkey", Deferrability.NOT_DEFERRABLE),
    ForeignKeysInfo(current_catalog, test_schema, "parent1", "parent_id", current_catalog, test_schema, "child_single_fk", "parent_id", 1, UpdateDeleteRule.NO_ACTION, UpdateDeleteRule.NO_ACTION, "child_single_fk_parent_id_fkey", "parent1_pkey", Deferrability.NOT_DEFERRABLE),
    ForeignKeysInfo(current_catalog, test_schema, "parent2", "p_id1", current_catalog, test_schema, "child_composite_fk", "p_id1", 1, UpdateDeleteRule.NO_ACTION, UpdateDeleteRule.NO_ACTION, "child_composite_fk_p_id1_fkey", "parent2_pkey", Deferrability.NOT_DEFERRABLE),
    ForeignKeysInfo(current_catalog, test_schema, "parent2", "p_id1", current_catalog, test_schema, "child_multiple_fk", "p_id1", 1, UpdateDeleteRule.NO_ACTION, UpdateDeleteRule.NO_ACTION, "child_multiple_fk_p_id1_fkey", "parent2_pkey", Deferrability.NOT_DEFERRABLE),
    ForeignKeysInfo(current_catalog, test_schema, "parent2", "p_id2", current_catalog, test_schema, "child_composite_fk", "p_id2", 2, UpdateDeleteRule.NO_ACTION, UpdateDeleteRule.NO_ACTION, "child_composite_fk_p_id1_fkey", "parent2_pkey", Deferrability.NOT_DEFERRABLE),
    ForeignKeysInfo(current_catalog, test_schema, "parent2", "p_id2", current_catalog, test_schema, "child_multiple_fk", "p_id2", 2, UpdateDeleteRule.NO_ACTION, UpdateDeleteRule.NO_ACTION, "child_multiple_fk_p_id1_fkey", "parent2_pkey", Deferrability.NOT_DEFERRABLE)
]

# Define expected returned result for get_exported_keys
get_exported_keys_result: typing.List[ForeignKeysInfo] = [
    ForeignKeysInfo(current_catalog, test_schema, "parent2", "p_id1", current_catalog, test_schema, "child_composite_fk", "p_id1", 1, UpdateDeleteRule.NO_ACTION, UpdateDeleteRule.NO_ACTION, "child_composite_fk_p_id1_fkey", "parent2_pkey", Deferrability.NOT_DEFERRABLE),
    ForeignKeysInfo(current_catalog, test_schema, "parent2", "p_id2", current_catalog, test_schema, "child_composite_fk", "p_id2", 2, UpdateDeleteRule.NO_ACTION, UpdateDeleteRule.NO_ACTION, "child_composite_fk_p_id1_fkey", "parent2_pkey", Deferrability.NOT_DEFERRABLE),
    ForeignKeysInfo(current_catalog, test_schema, "parent1", "parent_id", current_catalog, test_schema, "child_multiple_fk", "parent1_id", 1, UpdateDeleteRule.NO_ACTION, UpdateDeleteRule.NO_ACTION, "child_multiple_fk_parent1_id_fkey", "parent1_pkey", Deferrability.NOT_DEFERRABLE),
    ForeignKeysInfo(current_catalog, test_schema, "parent2", "p_id1", current_catalog, test_schema, "child_multiple_fk", "p_id1", 1, UpdateDeleteRule.NO_ACTION, UpdateDeleteRule.NO_ACTION, "child_multiple_fk_p_id1_fkey", "parent2_pkey", Deferrability.NOT_DEFERRABLE),
    ForeignKeysInfo(current_catalog, test_schema, "parent2", "p_id2", current_catalog, test_schema, "child_multiple_fk", "p_id2", 2, UpdateDeleteRule.NO_ACTION, UpdateDeleteRule.NO_ACTION, "child_multiple_fk_p_id1_fkey", "parent2_pkey", Deferrability.NOT_DEFERRABLE),
    ForeignKeysInfo(current_catalog, test_schema, "parent1", "parent_id", current_catalog, test_schema, "child_single_fk", "parent_id", 1, UpdateDeleteRule.NO_ACTION, UpdateDeleteRule.NO_ACTION, "child_single_fk_parent_id_fkey", "parent1_pkey", Deferrability.NOT_DEFERRABLE)
]

# Define column constant for get_best_row_identifier
get_best_row_identifier_col_name: typing.List[str] = [
    "SCOPE",
    "COLUMN_NAME",
    "DATA_TYPE",
    "TYPE_NAME",
    "COLUMN_SIZE",
    "BUFFER_LENGTH",
    "DECIMAL_DIGITS",
    "PSEUDO_COLUMN"
]


# Define result structure for get_imported_keys and get_best_row_identifier
@dataclass
class BestRowIdentifierInfo:
    scope: int
    column_name: str
    data_type: int
    type_name: str
    column_size: int
    buffer_length: typing.Optional[int]
    decimal_digits: int
    pseudo_column: int


startup_stmts: typing.Tuple[str, ...] = (
    "DROP SCHEMA IF EXISTS {} cascade;".format(test_schema),
    "CREATE SCHEMA {}".format(test_schema),
    "CREATE TABLE {}.{}(parent_id INTEGER PRIMARY KEY,parent_name VARCHAR(50))".format(test_schema,"parent1"),
    "CREATE TABLE {}.{}(p_id1 INTEGER,p_id2 VARCHAR(20),parent_desc TEXT,PRIMARY KEY (p_id1, p_id2))".format(test_schema,"parent2"),
    "CREATE TABLE {}.{}(col1 INTEGER,col2 VARCHAR(50))".format(test_schema,"no_pk"),
    "CREATE TABLE {}.{}(child_id INTEGER PRIMARY KEY,parent_id INTEGER,child_data VARCHAR(50),FOREIGN KEY (parent_id) REFERENCES test_keys_schema.parent1(parent_id))".format(test_schema,"child_single_fk"),
    "CREATE TABLE {}.{}(child_id INTEGER PRIMARY KEY,p_id1 INTEGER,p_id2 VARCHAR(20),child_data VARCHAR(50),FOREIGN KEY (p_id1, p_id2) REFERENCES test_keys_schema.parent2(p_id1, p_id2))".format(test_schema,"child_composite_fk"),
    "CREATE TABLE {}.{}(child_id INTEGER PRIMARY KEY,parent1_id INTEGER,p_id1 INTEGER,p_id2 VARCHAR(20),child_data VARCHAR(50),FOREIGN KEY (parent1_id) REFERENCES test_keys_schema.parent1(parent_id),FOREIGN KEY (p_id1, p_id2) REFERENCES test_keys_schema.parent2(p_id1, p_id2))".format(test_schema,"child_multiple_fk")
)


catalogs_case = [None, "", current_catalog, current_catalog+"%", "wrong_database"]
schemas_case = [test_schema, test_schema+"%", "wrong_schema"]
primary_keys_tables_case = ["", "parent1", "parent2", "no_pk", "child_single_fk", "child_multiple_fk", "wrong_table"]
imported_keys_tables_case = ["", "child_single_fk", "child_multiple_fk", "wrong_table"]
exported_keys_tables_case = ["", "parent1", "parent2", "wrong_table"]


class TestMetadataAPIKeys:
    @pytest.fixture(scope="class", autouse=True)
    def test_metadataAPI_keys_config(self, request, db_kwargs):
        setup_metadata_test_env(db_kwargs, current_catalog, startup_stmts)

        def fin():
            teardown_metadata_test_env(db_kwargs, current_catalog)

        request.addfinalizer(fin)

    @pytest.mark.parametrize("catalog", catalogs_case)
    @pytest.mark.parametrize("schema", schemas_case)
    @pytest.mark.parametrize("table", primary_keys_tables_case)
    @pytest.mark.parametrize("is_single_database_metadata", is_single_database_metadata_case)
    def test_get_primary_keys(self, db_kwargs, catalog, schema, table, is_single_database_metadata) -> None:
        expected_result: typing.List[PrimaryKeysInfo] = self.get_primary_keys_matches(catalog, schema, table)

        run_metadata_test(
            db_kwargs=db_kwargs,
            current_catalog=current_catalog,
            is_single_database_metadata=is_single_database_metadata,
            min_show_discovery_version=3,
            method_name="get_primary_keys",
            method_args=(catalog, schema, table),
            expected_col_name=get_primary_keys_col_name,
            expected_result=expected_result
        )

    @pytest.mark.parametrize("catalog", catalogs_case)
    @pytest.mark.parametrize("schema", schemas_case)
    @pytest.mark.parametrize("table", imported_keys_tables_case)
    @pytest.mark.parametrize("is_single_database_metadata", is_single_database_metadata_case)
    def test_get_imported_keys(self, db_kwargs, catalog, schema, table, is_single_database_metadata) -> None:
        expected_result: typing.List[ForeignKeysInfo] = self.get_imported_keys_matches(catalog, schema, table)

        run_metadata_test(
            db_kwargs=db_kwargs,
            current_catalog=current_catalog,
            is_single_database_metadata=is_single_database_metadata,
            min_show_discovery_version=3,
            method_name="get_imported_keys",
            method_args=(catalog, schema, table),
            expected_col_name=get_foreign_keys_col_name,
            expected_result=expected_result
        )

    @pytest.mark.parametrize("catalog", catalogs_case)
    @pytest.mark.parametrize("schema", schemas_case)
    @pytest.mark.parametrize("table", exported_keys_tables_case)
    @pytest.mark.parametrize("is_single_database_metadata", is_single_database_metadata_case)
    def test_get_exported_keys(self, db_kwargs, catalog, schema, table, is_single_database_metadata) -> None:
        expected_result: typing.List[ForeignKeysInfo] = self.get_exported_keys_matches(catalog, schema, table)

        run_metadata_test(
            db_kwargs=db_kwargs,
            current_catalog=current_catalog,
            is_single_database_metadata=is_single_database_metadata,
            min_show_discovery_version=3,
            method_name="get_exported_keys",
            method_args=(catalog, schema, table),
            expected_col_name=get_foreign_keys_col_name,
            expected_result=expected_result
        )

    @pytest.mark.parametrize(
        "test_input, expected_result",
        [
            (
                ["test_keys_catalog", "test_keys_schema","parent1", scope, False],
                [
                    BestRowIdentifierInfo(scope, "parent_id", 4, "int4", 10, None, 0, PseudoColumn.NOT_PSEUDO)
                ]
            ),
            (
                ["test_keys_catalog", "test_keys_schema","parent2", scope, False],
                [
                    BestRowIdentifierInfo(scope, "p_id1", 4, "int4", 10, None, 0, PseudoColumn.NOT_PSEUDO),
                    BestRowIdentifierInfo(scope, "p_id2", 12, "varchar", 20, None, 0, PseudoColumn.NOT_PSEUDO),
                ]
            ),
            (
                ["test_keys_catalog", "test_keys_schema","child_single_fk", scope, False],
                [
                    BestRowIdentifierInfo(scope, "child_id", 4, "int4", 10, None, 0, PseudoColumn.NOT_PSEUDO)
                ]
            ),
            (
                ["test_keys_catalog", "test_keys_schema","child_composite_fk", scope, False],
                [
                    BestRowIdentifierInfo(scope, "child_id", 4, "int4", 10, None, 0, PseudoColumn.NOT_PSEUDO)
                ]
            )
        ]
    )
    @pytest.mark.parametrize("is_single_database_metadata", is_single_database_metadata_case)
    def test_get_best_row_identifier(self, db_kwargs, test_input: list, expected_result: typing.List[BestRowIdentifierInfo], is_single_database_metadata) -> None:
        run_metadata_test(
            db_kwargs=db_kwargs,
            current_catalog=current_catalog,
            is_single_database_metadata=is_single_database_metadata,
            min_show_discovery_version=3,
            method_name="get_best_row_identifier",
            method_args=(test_input[0], test_input[1], test_input[2], scope, False),
            expected_col_name=get_best_row_identifier_col_name,
            expected_result=expected_result
        )

    @staticmethod
    def get_primary_keys_matches(catalog, schema, table) -> typing.List[PrimaryKeysInfo]:
        result: typing.List[PrimaryKeysInfo] = []
        for row_info in get_primary_keys_result:
            if (exact_matches(catalog, row_info.table_cat) and
                    exact_matches(schema, row_info.table_schem) and
                    exact_matches(table, row_info.table_name)):
                result.append(row_info)
        return result

    @staticmethod
    def get_imported_keys_matches(catalog, schema, table) -> typing.List[ForeignKeysInfo]:
        result: typing.List[ForeignKeysInfo] = []
        for row_info in get_imported_keys_result:
            if (exact_matches(catalog, row_info.fktable_cat) and
                    exact_matches(schema, row_info.fktable_schem) and
                    exact_matches(table, row_info.fktable_name)):
                result.append(row_info)
        return result

    @staticmethod
    def get_exported_keys_matches(catalog, schema, table) -> typing.List[ForeignKeysInfo]:
        result: typing.List[ForeignKeysInfo] = []
        for row_info in get_exported_keys_result:
            if (exact_matches(catalog, row_info.pktable_cat) and
                    exact_matches(schema, row_info.pktable_schem) and
                    exact_matches(table, row_info.pktable_name)):
                result.append(row_info)
        return result
