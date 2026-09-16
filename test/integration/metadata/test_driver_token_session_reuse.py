"""
Integration test verifying the driver token works across multiple
metadata APIs on one connection, and that each connection negotiates
its own token. This mirrors how BI tools open many connections and
issue metadata calls per session.

Uses pg_catalog.pg_class which exists on every cluster, so no fixture
setup is needed. Succeeds on both V5 (gated) and V4 (ungated) servers.
"""
import pytest
import redshift_connector


class TestDriverTokenSessionReuse:

    def test_driver_token_across_apis(self, db_kwargs) -> None:
        """All 3 V5 batch SHOW APIs succeed on a single connection."""
        with redshift_connector.connect(**db_kwargs) as conn:
            cursor = conn.cursor()

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
            result1 = cursor1.get_tables(catalog=None, schema_pattern="pg_catalog", table_name_pattern="pg_class")
            assert len(result1) > 0

        with redshift_connector.connect(**db_kwargs) as conn2:
            cursor2 = conn2.cursor()
            result2 = cursor2.get_tables(catalog=None, schema_pattern="pg_catalog", table_name_pattern="pg_class")
            assert len(result2) > 0
