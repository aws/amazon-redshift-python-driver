"""Common test utilities for Azure credential providers."""
import pytest

from redshift_connector import InterfaceError
from redshift_connector.plugin.azure_utils import validate_idp_partition
from redshift_connector.plugin.plugin_utils import get_microsoft_idp_host


@pytest.mark.parametrize("partition", ["", "    ", None])
def test_get_microsoft_idp_host_empty_partition_returns_commercial_host(partition) -> None:
    """Test that empty/None partitions return commercial host."""
    assert get_microsoft_idp_host(partition) == "login.microsoftonline.com"


@pytest.mark.parametrize("partition", ["commercial", "COMMERCIAL", "Commercial   "])
def test_get_microsoft_idp_host_commercial_partition_returns_commercial_host(partition) -> None:
    """Test that 'commercial' partitions return commercial host."""
    assert get_microsoft_idp_host(partition) == "login.microsoftonline.com"


@pytest.mark.parametrize("partition", ["us-gov", "US-GOV", "Us-gov   "])
def test_get_microsoft_idp_host_us_gov_partition(partition) -> None:
    """Test that us-gov partitions return US government host."""
    assert get_microsoft_idp_host(partition) == "login.microsoftonline.us"


@pytest.mark.parametrize("partition", ["cn", "CN", "Cn   "])
def test_get_microsoft_idp_host_china_partition(partition) -> None:
    """Test that cn partitions return China host."""
    assert get_microsoft_idp_host(partition) == "login.chinacloudapi.cn"


def test_get_microsoft_idp_host_invalid_partition_throws_error() -> None:
    """Test that invalid partitions raise InterfaceError."""
    with pytest.raises(InterfaceError, match="idp_partition must be one of"):
        get_microsoft_idp_host("random_partition")


@pytest.mark.parametrize("partition", ["", "commercial", "us-gov", "cn", None])
def test_validate_idp_partition_valid_values(partition) -> None:
    """Test that valid partition values are accepted."""
    result = validate_idp_partition(partition)
    assert result == partition


def test_validate_idp_partition_invalid_type() -> None:
    """Test that non-string partition values raise InterfaceError."""
    with pytest.raises(InterfaceError, match="idp_partition must be a string"):
        validate_idp_partition(123)  # type: ignore


def test_validate_idp_partition_invalid_value() -> None:
    """Test that invalid partition values raise InterfaceError."""
    with pytest.raises(InterfaceError, match="idp_partition must be one of"):
        validate_idp_partition("invalid_partition")
