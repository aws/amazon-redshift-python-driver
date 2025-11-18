"""Shared utilities for Azure credential providers."""
import typing

from redshift_connector.error import InterfaceError


def validate_idp_partition(idp_partition: typing.Optional[str]) -> typing.Optional[str]:
    """Validate idp_partition parameter and return normalized value."""
    if idp_partition is not None:
        if not isinstance(idp_partition, str):
            raise InterfaceError("idp_partition must be a string")
        # Validate against allowed values
        valid_partitions = ["", "commercial", "us-gov", "cn"]
        normalized = idp_partition.strip().lower()
        if normalized not in valid_partitions:
            raise InterfaceError(
                f"idp_partition must be one of: {', '.join(repr(p) if p else 'empty string' for p in valid_partitions)}"
            )
    return idp_partition
