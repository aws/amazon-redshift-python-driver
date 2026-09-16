"""
Integration test verifying the driver token works across multiple
metadata APIs on one connection, and that each connection negotiates
its own token. This mirrors how BI tools open many connections and
issue metadata calls per session.

Driver tokens only apply to the V5 batch SHOW path. On a server that
reports `show_discovery < 5` the driver falls back to the per-object
loop, which does not attach a DRIVER_TOKEN clause and does not
exercise anything this test is meant to cover; the test skips there.
The CI pipeline provisions a plain provisioned cluster on the current
maintenance track (currently V4), so this test is expected to skip in
CI until the pipeline runs against a V5-capable cluster.

Uses pg_catalog.pg_class because it exists on every cluster, so no
fixture setup is required.
"""
import pytest
import redshift_connector


class TestDriverTokenSessionReuse:

    def test_driver_token_across_apis(self, db_kwargs) -> None:
        """All 3 V5 batch SHOW APIs succeed on a single connection."""
        with redshift_connector.connect(**db_kwargs) as conn:
            cursor = conn.cursor()

            show_discovery = cursor.get_show_discovery_version()
            if show_discovery < 5:
                pytest.skip(
                    "server does not support the V5 batch SHOW path "
                    "(show_discovery={}); driver_token gating is not "
                    "exercised in the loop path.".format(show_discovery)
                )

            result = cursor.get_tables(catalog=None, schema_pattern="pg_catalog", table_name_pattern="pg_class")
            assert len(result) > 0

            result = cursor.get_columns(catalog=None, schema_pattern="pg_catalog", tablename_pattern="pg_class")
            assert len(result) > 0

            result = cursor.get_table_privileges(catalog=None, schema_pattern="pg_catalog", table_name_pattern="pg_class")
            # May be 0 rows depending on grants, but should not error

    def test_driver_token_independent_connections(self, db_kwargs) -> None:
        """A second connection negotiates its own token and succeeds."""
        with redshift_connector.connect(**db_kwargs) as conn1:
            cursor1 = conn1.cursor()

            if cursor1.get_show_discovery_version() < 5:
                pytest.skip(
                    "server does not support the V5 batch SHOW path; "
                    "driver_token gating is not exercised in the loop path."
                )

            result1 = cursor1.get_tables(catalog=None, schema_pattern="pg_catalog", table_name_pattern="pg_class")
            assert len(result1) > 0

        with redshift_connector.connect(**db_kwargs) as conn2:
            cursor2 = conn2.cursor()
            result2 = cursor2.get_tables(catalog=None, schema_pattern="pg_catalog", table_name_pattern="pg_class")
            assert len(result2) > 0
