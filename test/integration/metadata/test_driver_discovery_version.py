"""
Integration test for driver_discovery_version startup parameter.

When the driver sends driver_discovery_version=1 in the startup packet,
the server enables SHOW-based metadata discovery. This test verifies
that behavior by checking that get_schemas() returns pg_catalog in the
result set, which indicates the server recognized the parameter and
responded with the expected schema list.
"""

import pytest
import typing
import redshift_connector

@pytest.mark.skip(reason="Requires server with driver_discovery_version support. RedshiftDP-123568")
class TestDriverDiscoveryVersion:
    """
    Verifies that driver_discovery_version=1 is correctly sent to the server
    by checking that pg_catalog appears in the schemas returned by get_schemas().
    """

    def test_get_schemas_returns_pg_catalog(self, db_kwargs: typing.Dict[str, typing.Union[str, bool, int]]) -> None:
        """
        When driver_discovery_version=1 is sent to the server,
        get_schemas() should return pg_catalog in the result set.
        """
        with redshift_connector.connect(**db_kwargs) as conn:
            with conn.cursor() as cursor:
                result: tuple = cursor.get_schemas(None, None)

                schema_names = {row[0] for row in result}

                assert "pg_catalog" in schema_names, (
                    f"get_schemas() should return pg_catalog when "
                    f"driver_discovery_version=1 is sent to server. "
                    f"Schemas found: {schema_names}"
                )

    def test_get_schemas_with_pg_catalog_pattern(self, db_kwargs: typing.Dict[str, typing.Union[str, bool, int]]) -> None:
        """
        When driver_discovery_version=1 is sent to the server,
        get_schemas(None, 'pg_catalog') should return pg_catalog.
        """
        with redshift_connector.connect(**db_kwargs) as conn:
            with conn.cursor() as cursor:
                result: tuple = cursor.get_schemas(None, "pg_catalog")

                found = any(row[0] == "pg_catalog" for row in result)

                assert found, (
                    "get_schemas(None, 'pg_catalog') should return pg_catalog "
                    "when driver_discovery_version=1 is sent to server"
                )

    def test_get_schemas_single_db_mode_returns_pg_catalog(self, db_kwargs: typing.Dict[str, typing.Union[str, bool, int]]) -> None:
        """
        When driver_discovery_version=1 is sent to the server and
        database_metadata_current_db_only=True, get_schemas() should
        still return pg_catalog.
        """
        single_db_kwargs = db_kwargs.copy()
        single_db_kwargs["database_metadata_current_db_only"] = True

        with redshift_connector.connect(**single_db_kwargs) as conn:
            with conn.cursor() as cursor:
                result: tuple = cursor.get_schemas(None, None)

                schema_names = {row[0] for row in result}

                assert "pg_catalog" in schema_names, (
                    f"get_schemas() should return pg_catalog in single-db mode "
                    f"when driver_discovery_version=1 is sent to server. "
                    f"Schemas found: {schema_names}"
                )
