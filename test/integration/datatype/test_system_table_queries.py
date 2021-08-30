import typing
from warnings import warn

import pytest

import redshift_connector
from redshift_connector.config import ClientProtocolVersion

system_tables: typing.List[str] = [
    "pg_aggregate",
    "pg_am",
    "pg_amop",
    "pg_amproc",
    "pg_attrdef",
    "pg_attribute",
    "pg_cast",
    "pg_class",
    "pg_constraint",
    "pg_conversion",
    "pg_database",
    "pg_default_acl",
    "pg_depend",
    "pg_description",
    # "pg_index",  # has unsupported type oids 22, 30
    "pg_inherits",
    "pg_language",
    "pg_largeobject",
    "pg_namespace",
    "pg_opclass",
    "pg_operator",
    "pg_proc",
    "pg_rewrite",
    "pg_shdepend",
    "pg_statistic",
    "pg_tablespace",
    # "pg_trigger",  # has unsupported type oid 22
    "pg_type",
    "pg_group",
    "pg_indexes",
    "pg_locks",
    "pg_rules",
    "pg_settings",
    "pg_stats",
    "pg_tables",
    # "pg_user",  # has unsupported type oid 702
    "pg_views",
    # "pg_authid",
    # "pg_auth_members",
    # "pg_collation",
    # "pg_db_role_setting",
    # "pg_enum",
    # "pg_extension",
    # "pg_foreign_data_wrapper",
    # "pg_foreign_server",
    # "pg_foreign_table",
    # "pg_largeobject_metadata",
    # "pg_opfamily",
    # "pg_pltemplate",
    # "pg_seclabel",
    # "pg_shdescription",
    # "pg_ts_config",
    # "pg_ts_config_map",
    # "pg_ts_dict",
    # "pg_ts_parser",
    # "pg_ts_template",
    # "pg_user_mapping",
    # "pg_available_extensions",
    # "pg_available_extension_versions",
    # "pg_cursors",
    # "pg_prepared_statements",
    # "pg_prepared_xacts",
    # "pg_roles",
    # "pg_seclabels",
    # "pg_shadow",
    # "pg_timezone_abbrevs",
    # "pg_timezone_names",
    # "pg_user_mappings",
]

# this test ensures system tables can be queried without datatype
# conversion issue. no validation of result set occurs.


@pytest.mark.skip(reason="manual")
@pytest.mark.parametrize("client_protocol", ClientProtocolVersion.list())
@pytest.mark.parametrize("table_name", system_tables)
def test_process_system_table_datatypes(db_kwargs, client_protocol, table_name):
    db_kwargs["client_protocol_version"] = client_protocol

    with redshift_connector.connect(**db_kwargs) as conn:
        if conn._client_protocol_version != client_protocol:
            warn(
                "Requested client_protocol_version was not satisfied. Requested {} Got {}".format(
                    client_protocol, conn._client_protocol_version
                )
            )
        with conn.cursor() as cursor:
            cursor.execute("select * from {}".format(table_name))
