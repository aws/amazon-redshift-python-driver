import typing

import pytest  # type: ignore

import redshift_connector
from metadata_test_utils import setup_metadata_test_env, teardown_metadata_test_env

"""
Integration tests for the ``enable_table_types`` connection option on the
server SHOW path. With enable_table_types=False the driver collapses TABLE_TYPE
into the generic TABLE/VIEW buckets (in get_tables and get_table_types); with
the default (True) it reports the detailed list. Only meaningful on a SHOW
capable cluster (show_discovery >= 4); skipped otherwise.
"""

SCHEMA: str = "enable_table_types_it_schema"
TABLE: str = "ett_table"
VIEW: str = "ett_view"
# A real EXTERNAL TABLE is produced with a throwaway cross-database external
# schema (no Glue/S3/IAM needed): a source database holds a plain table, and the
# current database references it through an external schema. Tables reached via
# that external schema report TABLE_TYPE = EXTERNAL TABLE.
SRC_DB: str = "ett_src_db"
SRC_SCHEMA: str = "ett_src_schema"
SRC_TABLE: str = "ett_src_table"
EXT_SCHEMA: str = "ett_ext_schema"
MIN_SHOW_DISCOVERY_VERSION_V4: int = 4

# get_tables result column index for TABLE_TYPE.
TABLE_TYPE_IDX: int = 3


def _show_capable(con) -> bool:
    with con.cursor() as cur:
        return cur.get_show_discovery_version() >= MIN_SHOW_DISCOVERY_VERSION_V4


@pytest.fixture
def setup_objects(db_kwargs) -> bool:
    # Function scope: db_kwargs is class-scoped, so a broader (module) scope here
    # would raise a pytest ScopeMismatch. Function scope can depend on it safely.
    con = redshift_connector.connect(**db_kwargs)
    con.autocommit = True
    capable = _show_capable(con)
    try:
        if capable:
            with con.cursor() as cur:
                cur.execute("DROP SCHEMA IF EXISTS {} CASCADE".format(SCHEMA))
                cur.execute("CREATE SCHEMA {}".format(SCHEMA))
                cur.execute(
                    "CREATE TABLE {}.{} (id int, name varchar(32))".format(SCHEMA, TABLE)
                )
                cur.execute(
                    "CREATE VIEW {}.{} AS SELECT id, name FROM {}.{}".format(
                        SCHEMA, VIEW, SCHEMA, TABLE
                    )
                )
                cur.execute("DROP SCHEMA IF EXISTS {} CASCADE".format(EXT_SCHEMA))
            # Build a throwaway source database with a plain table, then expose it
            # through an external schema in the current database so the table is
            # reported as EXTERNAL TABLE. setup_metadata_test_env creates (and
            # first drops) SRC_DB and runs the DDL inside it.
            setup_metadata_test_env(
                db_kwargs,
                SRC_DB,
                (
                    "CREATE SCHEMA {}".format(SRC_SCHEMA),
                    "CREATE TABLE {}.{} (id int, name varchar(32))".format(
                        SRC_SCHEMA, SRC_TABLE
                    ),
                ),
            )
            with con.cursor() as cur:
                cur.execute(
                    "CREATE EXTERNAL SCHEMA {} FROM REDSHIFT DATABASE '{}' SCHEMA '{}'".format(
                        EXT_SCHEMA, SRC_DB, SRC_SCHEMA
                    )
                )
        yield capable
    finally:
        if capable:
            with con.cursor() as cur:
                cur.execute("DROP SCHEMA IF EXISTS {} CASCADE".format(EXT_SCHEMA))
                cur.execute("DROP SCHEMA IF EXISTS {} CASCADE".format(SCHEMA))
            teardown_metadata_test_env(db_kwargs, SRC_DB)
        con.close()


def _table_types(con) -> typing.Set[str]:
    with con.cursor() as cur:
        return {row[0] for row in cur.get_table_types()}


def _table_type_of(
    db_kwargs, enable: bool, name: str, types: list, schema: str = SCHEMA
) -> typing.Optional[str]:
    # Use a fresh connection per get_tables call. Issuing multiple get_tables
    # calls on a single reused connection trips a pre-existing streaming
    # row-description defect (cursor truncated_row_desc IndexError) that is
    # unrelated to enable_table_types; a new connection sidesteps it.
    con = _connect(db_kwargs, enable)
    try:
        with con.cursor() as cur:
            for row in cur.get_tables(None, schema, name, types):
                return row[TABLE_TYPE_IDX]
        return None
    finally:
        con.close()


def _connect(db_kwargs, enable: bool):
    kwargs = dict(db_kwargs)
    kwargs["enable_table_types"] = enable
    return redshift_connector.connect(**kwargs)


def test_get_table_types_off_returns_only_table_and_view(db_kwargs, setup_objects) -> None:
    if not setup_objects:
        pytest.skip("Cluster does not support SHOW discovery (>=4)")
    con = _connect(db_kwargs, False)
    try:
        assert _table_types(con) == {"TABLE", "VIEW"}
    finally:
        con.close()


def test_get_table_types_on_returns_detailed_list(db_kwargs, setup_objects) -> None:
    if not setup_objects:
        pytest.skip("Cluster does not support SHOW discovery (>=4)")
    con = _connect(db_kwargs, True)
    try:
        types = _table_types(con)
        assert len(types) > 2
        assert "TABLE" in types
        assert "VIEW" in types
    finally:
        con.close()


def test_get_tables_off_table_and_view_retain_generic_types(db_kwargs, setup_objects) -> None:
    if not setup_objects:
        pytest.skip("Cluster does not support SHOW discovery (>=4)")
    assert _table_type_of(db_kwargs, False, TABLE, ["TABLE"]) == "TABLE"
    assert _table_type_of(db_kwargs, False, VIEW, ["VIEW"]) == "VIEW"
    # A TABLE-only filter must not return the view.
    assert _table_type_of(db_kwargs, False, VIEW, ["TABLE"]) is None


def test_get_tables_on_table_and_view_reported(db_kwargs, setup_objects) -> None:
    if not setup_objects:
        pytest.skip("Cluster does not support SHOW discovery (>=4)")
    assert _table_type_of(db_kwargs, True, TABLE, ["TABLE"]) == "TABLE"
    assert _table_type_of(db_kwargs, True, VIEW, ["VIEW"]) == "VIEW"


def test_get_tables_off_external_table_relabeled_to_table(db_kwargs, setup_objects) -> None:
    if not setup_objects:
        pytest.skip("Cluster does not support SHOW discovery (>=4)")
    # With the option disabled, an EXTERNAL TABLE collapses to TABLE and matches
    # a TABLE-only filter (generalization happens before the requested-type filter).
    assert _table_type_of(db_kwargs, False, SRC_TABLE, None, schema=EXT_SCHEMA) == "TABLE"
    assert _table_type_of(db_kwargs, False, SRC_TABLE, ["TABLE"], schema=EXT_SCHEMA) == "TABLE"
    # It must not match an EXTERNAL TABLE filter once generalized.
    assert _table_type_of(db_kwargs, False, SRC_TABLE, ["EXTERNAL TABLE"], schema=EXT_SCHEMA) is None


def test_get_tables_on_external_table_reported(db_kwargs, setup_objects) -> None:
    if not setup_objects:
        pytest.skip("Cluster does not support SHOW discovery (>=4)")
    # With the default (enabled), the detailed EXTERNAL TABLE type is preserved.
    assert _table_type_of(db_kwargs, True, SRC_TABLE, None, schema=EXT_SCHEMA) == "EXTERNAL TABLE"
    assert _table_type_of(db_kwargs, True, SRC_TABLE, ["EXTERNAL TABLE"], schema=EXT_SCHEMA) == "EXTERNAL TABLE"
