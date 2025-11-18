import typing
import pytest  # type: ignore
from _password_generator import generate_password
import redshift_connector
from dataclasses import dataclass
from metadata_test_utils import setup_metadata_test_env, teardown_metadata_test_env, run_metadata_test


# Define constants
is_single_database_metadata_case: typing.List[bool] = [True, False]
current_catalog: str = "test_privileges_catalog"
current_schema1: str = "test_privileges_schema1"
current_schema2: str = "test_privileges_schema2"
current_schema3: str = "test_privileges_schema3"
current_schema4: str = "test_privileges_schema4"
current_table1: str = "test_privileges_table1_grant_user_table_privilege"
current_table2: str = "test_privileges_table2_grant_role_table_privilege"
current_table3: str = "test_privileges_table3_admin_with_grant_option_table_privilege"
current_table4: str = "test_privileges_table4_public_table_privilege"
current_table5: str = "test_privileges_table5_grant_user_table_privilege"
current_table6: str = "test_privileges_table6_grant_role_table_privilege"
# Redshift doesn't support column level grant with option:
# https://docs.aws.amazon.com/redshift/latest/dg/r_GRANT-usage-notes.html#r_GRANT-usage-notes-clp
current_table7: str = "test_privileges_table7_grant_user_table_privilege"
current_table8: str = "test_privileges_table8_public_table_privilege"
test_role: str = "test_role"
admin_role: str = "admin_role"
user1: str = "test_user1"
user2: str = "test_user2"
admin_user: str = "test_admin"
password: str = generate_password()


# Define column constant for get_column_privileges
get_column_privileges_col_name: typing.List[str] = [
    "TABLE_CAT",
    "TABLE_SCHEM",
    "TABLE_NAME",
    "COLUMN_NAME",
    "GRANTOR",
    "GRANTEE",
    "PRIVILEGE",
    "IS_GRANTABLE"
]


# Define result structure for get_column_privileges
@dataclass
class ColumnPrivilegeInfo:
    table_cat: str
    table_schem: str
    table_name: str
    column_name: str
    grantor: str
    grantee: str
    privilege: str
    is_grantable: str


# Define column constant for get_table_privileges
get_table_privileges_col_name: typing.List[str] = [
    "TABLE_CAT",
    "TABLE_SCHEM",
    "TABLE_NAME",
    "GRANTOR",
    "GRANTEE",
    "PRIVILEGE",
    "IS_GRANTABLE"
]


# Define result structure for get_table_privileges
@dataclass
class TablePrivilegeInfo:
    table_cat: str
    table_schem: str
    table_name: str
    grantor: str
    grantee: str
    privilege: str
    is_grantable: str


# Drop user/role query
drop_stmts: typing.Tuple[str, ...] = (
    "DROP USER IF EXISTS {};".format(user1),
    "DROP USER IF EXISTS {};".format(user2),
    "DROP USER IF EXISTS {};".format(admin_user),
    "DROP ROLE {};".format(test_role),
    "DROP ROLE {};".format(admin_role),
)

# Test environment setup Query
startup_stmts: typing.Tuple[str, ...] = (
    "DROP SCHEMA IF EXISTS {} cascade;".format(current_schema1),
    "DROP SCHEMA IF EXISTS {} cascade;".format(current_schema2),
    "DROP SCHEMA IF EXISTS {} cascade;".format(current_schema3),
    "DROP SCHEMA IF EXISTS {} cascade;".format(current_schema4),
    "CREATE SCHEMA {}".format(current_schema1),
    "CREATE SCHEMA {}".format(current_schema2),
    "CREATE SCHEMA {}".format(current_schema3),
    "CREATE SCHEMA {}".format(current_schema4),
    "CREATE TABLE {}.{}(id INTEGER PRIMARY KEY)".format(current_schema1,current_table1),
    "CREATE TABLE {}.{}(id INTEGER PRIMARY KEY)".format(current_schema1,current_table2),
    "CREATE TABLE {}.{}(id INTEGER PRIMARY KEY)".format(current_schema2,current_table3),
    "CREATE TABLE {}.{}(id INTEGER PRIMARY KEY)".format(current_schema2,current_table4),
    "CREATE ROLE {};".format(test_role),
    "CREATE ROLE {};".format(admin_role),
    "CREATE USER {} PASSWORD '{}';".format(user1, password),
    "CREATE USER {} PASSWORD '{}';".format(user2, password),
    "CREATE USER {} PASSWORD '{}';".format(admin_user, password),
    "GRANT ROLE {} TO {};".format(admin_role, admin_user),
    "CREATE TABLE {}.{}(id INTEGER PRIMARY KEY,sensitive_col VARCHAR(50),public_col VARCHAR(50))".format(current_schema3,current_table5),
    "CREATE TABLE {}.{}(id INTEGER PRIMARY KEY,sensitive_col VARCHAR(50),public_col VARCHAR(50))".format(current_schema3,current_table6),
    "CREATE TABLE {}.{}(id INTEGER PRIMARY KEY,sensitive_col VARCHAR(50),public_col VARCHAR(50))".format(current_schema4,current_table7),
    "CREATE TABLE {}.{}(id INTEGER PRIMARY KEY,sensitive_col VARCHAR(50),public_col VARCHAR(50))".format(current_schema4,current_table8),
    "GRANT SELECT ON {}.{} TO {};".format(current_schema1, current_table1, user1),
    "GRANT INSERT ON {}.{} TO {};".format(current_schema1, current_table2, user1),
    "GRANT UPDATE ON {}.{} TO {};".format(current_schema2, current_table3, user1),
    "GRANT SELECT, INSERT, UPDATE, DELETE ON {}.{} TO {};".format(current_schema1, current_table1, user2),
    "GRANT SELECT, INSERT, UPDATE, DELETE ON {}.{} TO ROLE {};".format(current_schema1, current_table2, test_role),
    "GRANT USAGE, CREATE ON SCHEMA {} TO {};".format(current_schema2, admin_user),
    "GRANT ALL ON {}.{} TO {} WITH GRANT OPTION;".format(current_schema2, current_table3, admin_user),
    "SET SESSION AUTHORIZATION {};".format(admin_user),
    "GRANT SELECT ON {}.{} TO {};".format(current_schema2, current_table3, user2),
    "RESET SESSION AUTHORIZATION;",
    "GRANT SELECT ON {}.{} TO PUBLIC;".format(current_schema2, current_table4),
    "GRANT SELECT (sensitive_col) ON {}.{} TO {};".format(current_schema3, current_table5, user1),
    "GRANT SELECT (sensitive_col) ON {}.{} TO {};".format(current_schema3, current_table6, user1),
    "GRANT UPDATE (sensitive_col) ON {}.{} TO {};".format(current_schema4, current_table7, user1),
    "GRANT SELECT (sensitive_col), UPDATE (sensitive_col) ON {}.{} TO {};".format(current_schema3, current_table5, user2),
    "GRANT SELECT (sensitive_col), UPDATE (sensitive_col) ON {}.{} TO ROLE {};".format(current_schema3, current_table6, test_role),
    "GRANT ROLE {} TO {};".format(test_role, user2),
    "GRANT SELECT (public_col) ON {}.{} TO PUBLIC;".format(current_schema4, current_table8)
)


class TestMetadataAPIPrivileges:
    @pytest.fixture(scope="class", autouse=True)
    def test_metadataAPI_privileges_config(self, request, db_kwargs):
        with redshift_connector.connect(**db_kwargs) as connection:
            connection.autocommit = True
            with connection.cursor() as cursor:
                for stmt in drop_stmts:
                    try:
                        cursor.execute(stmt)
                    except Exception as e:
                        pass # ignore the error when user/role doesn't exist during drop

        setup_metadata_test_env(db_kwargs, current_catalog, startup_stmts)

        def fin():
            teardown_metadata_test_env(db_kwargs, current_catalog)

        request.addfinalizer(fin)


    @pytest.mark.parametrize(
        "test_input, expected_result",
        [
            (
                # Table level grant should not show up
                [current_catalog, current_schema1,current_table1, ""],
                []
            ),
            (
                # Table level grant should not show up
                [current_catalog, current_schema1,current_table2, ""],
                []
            ),
            (
                # Table level grant should not show up
                [current_catalog, current_schema2,current_table3, ""],
                []
            ),
            (
                # Table level grant should not show up
                [current_catalog, current_schema2,current_table4, ""],
                []
            ),
            (
                # Column level user grant
                [current_catalog, current_schema3,current_table5, ""],
                [
                    ColumnPrivilegeInfo(current_catalog, current_schema3, current_table5, "sensitive_col", "awsuser", user1, "SELECT", "NO"),
                    ColumnPrivilegeInfo(current_catalog, current_schema3, current_table5, "sensitive_col", "awsuser", user2, "SELECT", "NO"),
                    ColumnPrivilegeInfo(current_catalog, current_schema3, current_table5, "sensitive_col", "awsuser", user2, "UPDATE", "NO"),
                ]
            ),
            (
                [current_catalog, current_schema3,current_table5, "id"],
                []
            ),
            (
                [current_catalog, current_schema3,current_table5, "public_col"],
                []
            ),
            (
                [current_catalog, current_schema3,current_table5, "sensitive%"],
                [
                    ColumnPrivilegeInfo(current_catalog, current_schema3, current_table5, "sensitive_col", "awsuser", user1, "SELECT", "NO"),
                    ColumnPrivilegeInfo(current_catalog, current_schema3, current_table5, "sensitive_col", "awsuser", user2, "SELECT", "NO"),
                    ColumnPrivilegeInfo(current_catalog, current_schema3, current_table5, "sensitive_col", "awsuser", user2, "UPDATE", "NO"),
                ]
            ),
            (
                # Column level role grant
                [current_catalog, current_schema3,current_table6, ""],
                [
                    ColumnPrivilegeInfo(current_catalog, current_schema3, current_table6, "sensitive_col", "awsuser", user1, "SELECT", "NO"),
                    ColumnPrivilegeInfo(current_catalog, current_schema3, current_table6, "sensitive_col", "awsuser", test_role, "SELECT", "NO"),
                    ColumnPrivilegeInfo(current_catalog, current_schema3, current_table6, "sensitive_col", "awsuser", test_role, "UPDATE", "NO"),
                ]
            ),
            (
                [current_catalog, current_schema3,current_table6, "sensitive%"],
                [
                    ColumnPrivilegeInfo(current_catalog, current_schema3, current_table6, "sensitive_col", "awsuser", user1, "SELECT", "NO"),
                    ColumnPrivilegeInfo(current_catalog, current_schema3, current_table6, "sensitive_col", "awsuser", test_role, "SELECT", "NO"),
                    ColumnPrivilegeInfo(current_catalog, current_schema3, current_table6, "sensitive_col", "awsuser", test_role, "UPDATE", "NO"),
                ]
            ),
            (
                [current_catalog, current_schema4,current_table7, ""],
                [
                    # Column level user grant
                    ColumnPrivilegeInfo(current_catalog, current_schema4, current_table7, "sensitive_col", "awsuser", user1, "UPDATE", "NO"),
                ]
            ),
            (
                [current_catalog, current_schema4,current_table7, "sensitive%"],
                [
                    ColumnPrivilegeInfo(current_catalog, current_schema4, current_table7, "sensitive_col", "awsuser", user1, "UPDATE", "NO"),
                ]
            ),
            (
                [current_catalog, current_schema4,current_table8, ""],
                [
                    # Column level public grant
                    ColumnPrivilegeInfo(current_catalog, current_schema4, current_table8, "public_col", "awsuser", "public", "SELECT", "NO"),
                ]
            )
        ]
    )
    @pytest.mark.parametrize("is_single_database_metadata", is_single_database_metadata_case)
    def test_get_column_privileges(self, db_kwargs, test_input: list, expected_result: typing.List[ColumnPrivilegeInfo], is_single_database_metadata) -> None:
        run_metadata_test(
            db_kwargs=db_kwargs,
            current_catalog=current_catalog,
            is_single_database_metadata=is_single_database_metadata,
            min_show_discovery_version=3,
            method_name="get_column_privileges",
            method_args=(test_input[0], test_input[1], test_input[2], test_input[3]),
            expected_col_name=get_column_privileges_col_name,
            expected_result=expected_result
        )

    @pytest.mark.parametrize(
        "test_input, expected_result",
        [
            (
                [current_catalog, current_schema1,current_table1],
                [
                    TablePrivilegeInfo(current_catalog, current_schema1, current_table1, "awsuser", user2, "DELETE", "NO"),
                    TablePrivilegeInfo(current_catalog, current_schema1, current_table1, "awsuser", user2, "INSERT", "NO"),
                    TablePrivilegeInfo(current_catalog, current_schema1, current_table1, "awsuser", user1, "SELECT", "NO"),
                    TablePrivilegeInfo(current_catalog, current_schema1, current_table1, "awsuser", user2, "SELECT", "NO"),
                    TablePrivilegeInfo(current_catalog, current_schema1, current_table1, "awsuser", user2, "UPDATE", "NO"),
                ]
            ),
            (
                [current_catalog, current_schema1,current_table2],
                [
                    TablePrivilegeInfo(current_catalog, current_schema1, current_table2, "awsuser", test_role, "DELETE", "NO"), # New return from show grants
                    TablePrivilegeInfo(current_catalog, current_schema1, current_table2, "awsuser", user1, "INSERT", "NO"),
                    TablePrivilegeInfo(current_catalog, current_schema1, current_table2, "awsuser", test_role, "INSERT", "NO"), # New return from show grants
                    TablePrivilegeInfo(current_catalog, current_schema1, current_table2, "awsuser", test_role, "SELECT", "NO"), # New return from show grants
                    TablePrivilegeInfo(current_catalog, current_schema1, current_table2, "awsuser", test_role, "UPDATE", "NO"), # New return from show grants
                ]
            ),
            (
                [current_catalog, current_schema2,current_table3],
                [
                    TablePrivilegeInfo(current_catalog, current_schema2, current_table3, "awsuser", admin_user, "ALTER", "YES"),
                    TablePrivilegeInfo(current_catalog, current_schema2, current_table3, "awsuser", admin_user, "DELETE", "YES"),
                    TablePrivilegeInfo(current_catalog, current_schema2, current_table3, "awsuser", admin_user, "DROP", "YES"),
                    TablePrivilegeInfo(current_catalog, current_schema2, current_table3, "awsuser", admin_user, "INSERT", "YES"),
                    TablePrivilegeInfo(current_catalog, current_schema2, current_table3, "awsuser", admin_user, "REFERENCES", "YES"),
                    TablePrivilegeInfo(current_catalog, current_schema2, current_table3, "awsuser", admin_user, "RULE", "YES"),
                    TablePrivilegeInfo(current_catalog, current_schema2, current_table3, admin_user, user2, "SELECT", "NO"),
                    TablePrivilegeInfo(current_catalog, current_schema2, current_table3, "awsuser", admin_user, "SELECT", "YES"),
                    TablePrivilegeInfo(current_catalog, current_schema2, current_table3, "awsuser", admin_user, "TRIGGER", "YES"),
                    TablePrivilegeInfo(current_catalog, current_schema2, current_table3, "awsuser", admin_user, "TRUNCATE", "YES"),
                    TablePrivilegeInfo(current_catalog, current_schema2, current_table3, "awsuser", user1, "UPDATE", "NO"),
                    TablePrivilegeInfo(current_catalog, current_schema2, current_table3, "awsuser", admin_user, "UPDATE", "YES"),
                ]
            ),
            (
                [current_catalog, current_schema2,current_table4],
                [
                    TablePrivilegeInfo(current_catalog, current_schema2, current_table4, "awsuser", "public", "SELECT", "NO"),
                ]
            ),
            (
                [current_catalog, current_schema2,"test_privileges_table%"],
                [
                    TablePrivilegeInfo(current_catalog, current_schema2, current_table3, "awsuser", admin_user, "ALTER", "YES"),
                    TablePrivilegeInfo(current_catalog, current_schema2, current_table3, "awsuser", admin_user, "DELETE", "YES"),
                    TablePrivilegeInfo(current_catalog, current_schema2, current_table3, "awsuser", admin_user, "DROP", "YES"),
                    TablePrivilegeInfo(current_catalog, current_schema2, current_table3, "awsuser", admin_user, "INSERT", "YES"),
                    TablePrivilegeInfo(current_catalog, current_schema2, current_table3, "awsuser", admin_user, "REFERENCES", "YES"),
                    TablePrivilegeInfo(current_catalog, current_schema2, current_table3, "awsuser", admin_user, "RULE", "YES"),
                    TablePrivilegeInfo(current_catalog, current_schema2, current_table3, admin_user, user2, "SELECT", "NO"),
                    TablePrivilegeInfo(current_catalog, current_schema2, current_table3, "awsuser", admin_user, "SELECT", "YES"),
                    TablePrivilegeInfo(current_catalog, current_schema2, current_table3, "awsuser", admin_user, "TRIGGER", "YES"),
                    TablePrivilegeInfo(current_catalog, current_schema2, current_table3, "awsuser", admin_user, "TRUNCATE", "YES"),
                    TablePrivilegeInfo(current_catalog, current_schema2, current_table3, "awsuser", user1, "UPDATE", "NO"),
                    TablePrivilegeInfo(current_catalog, current_schema2, current_table3, "awsuser", admin_user, "UPDATE", "YES"),
                    TablePrivilegeInfo(current_catalog, current_schema2, current_table4, "awsuser", "public", "SELECT", "NO"),
                ]
            )
        ]
    )
    @pytest.mark.parametrize("is_single_database_metadata", is_single_database_metadata_case)
    def test_get_table_privileges(self, db_kwargs, test_input: list, expected_result: typing.List[TablePrivilegeInfo], is_single_database_metadata) -> None:
        run_metadata_test(
            db_kwargs=db_kwargs,
            current_catalog=current_catalog,
            is_single_database_metadata=is_single_database_metadata,
            min_show_discovery_version=3,
            method_name="get_table_privileges",
            method_args=(test_input[0], test_input[1], test_input[2]),
            expected_col_name=get_table_privileges_col_name,
            expected_result=expected_result
        )
