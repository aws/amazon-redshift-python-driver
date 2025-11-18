import typing

import pytest
from pytest_mock import mocker  # type: ignore

from redshift_connector.error import InterfaceError
from redshift_connector.plugin.browser_azure_oauth2_credentials_provider import (
    BrowserAzureOAuth2CredentialsProvider,
)
from redshift_connector.plugin.plugin_utils import get_microsoft_idp_host
from redshift_connector.redshift_property import RedshiftProperty

# Import common Azure tests
from test.unit.plugin.test_azure_common import (
    test_get_microsoft_idp_host_empty_partition_returns_commercial_host,
    test_get_microsoft_idp_host_us_gov_partition,
    test_get_microsoft_idp_host_china_partition,
    test_get_microsoft_idp_host_invalid_partition_throws_error
)


def make_valid_azure_oauth2_provider() -> typing.Tuple[BrowserAzureOAuth2CredentialsProvider, RedshiftProperty]:
    rp: RedshiftProperty = RedshiftProperty()
    rp.idp_tenant = "my_idp_tenant"
    rp.client_id = "my_client_id"
    rp.scope = "my_scope"
    rp.idp_response_timeout = 900
    rp.listen_port = 1099
    cp: BrowserAzureOAuth2CredentialsProvider = BrowserAzureOAuth2CredentialsProvider()
    cp.add_parameter(rp)
    return cp, rp

def test_default_parameters_azure_oauth2_specific() -> None:
    acp, _ = make_valid_azure_oauth2_provider()
    assert acp.ssl_insecure == False
    assert acp.do_verify_ssl_cert() == True


def test_add_parameter_sets_azure_oauth2_specific() -> None:
    acp, rp = make_valid_azure_oauth2_provider()
    assert acp.idp_tenant == rp.idp_tenant
    assert acp.client_id == rp.client_id
    assert acp.scope == rp.scope
    assert acp.idp_response_timeout == rp.idp_response_timeout
    assert acp.listen_port == rp.listen_port


@pytest.mark.parametrize("partition, expected_host", [
    (None, "login.microsoftonline.com"),
    ("", "login.microsoftonline.com"),
    ("us-gov", "login.microsoftonline.us"),
    ("cn", "login.chinacloudapi.cn")
])
def test_fetch_jwt_response_uses_correct_idp_host(mocker, partition, expected_host) -> None:
    """Test that fetch_jwt_response uses the correct IdP host based on partition"""
    from urllib.parse import urlparse
    
    acp, _ = make_valid_azure_oauth2_provider()
    acp.idp_partition = partition
    acp.redirectUri = "http://localhost:7890/redshift/"  # Set required attribute
    
    # Mock the requests.post call
    mock_response = mocker.Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"access_token": "test_token"}
    mock_post = mocker.patch("requests.post", return_value=mock_response)
    
    # Call the method
    acp.fetch_jwt_response("test_auth_code")
    
    # Parse the URL and verify its components
    call_args = mock_post.call_args
    url = call_args[0][0]  # First positional argument is the URL
    parsed_url = urlparse(url)
    assert parsed_url.netloc == expected_host
    assert parsed_url.scheme == 'https'
    assert '/oauth2/' in parsed_url.path


def test_invalid_partition_handling() -> None:
    """Test error handling when an invalid partition is provided"""
    acp, _ = make_valid_azure_oauth2_provider()
    acp.idp_partition = "invalid-partition"
    
    with pytest.raises(InterfaceError, match="idp_partition must be one of"):
        acp.fetch_jwt_response("test_auth_code")


@pytest.mark.parametrize("value", [None, ""])
def test_check_required_parameters_raises_if_idp_tenant_missing_or_too_small(value) -> None:
    acp, _ = make_valid_azure_oauth2_provider()
    acp.idp_tenant = value

    with pytest.raises(InterfaceError, match="Missing required connection property: idp_tenant"):
        acp.get_jwt_assertion()


@pytest.mark.parametrize("value", [None, ""])
def test_check_required_parameters_raises_if_client_id_missing(value) -> None:
    acp, _ = make_valid_azure_oauth2_provider()
    acp.client_id = value

    with pytest.raises(InterfaceError, match="Missing required connection property: client_id"):
        acp.get_jwt_assertion()


@pytest.mark.parametrize("value", [None, ""])
def test_check_required_parameters_raises_if_idp_response_timeout_missing(value) -> None:
    acp, _ = make_valid_azure_oauth2_provider()
    acp.idp_response_timeout = value

    with pytest.raises(
        InterfaceError,
        match="Invalid value specified for connection property: idp_response_timeout. Must be 10 seconds or greater",
    ):
        acp.get_jwt_assertion()


def test_get_jwt_assertion_fetches_and_extracts(mocker) -> None:
    mock_token: str = "mock_token"
    mock_content: str = "mock_content"
    mock_jwt_assertion: str = "mock_jwt_assertion"
    mocker.patch(
        "redshift_connector.plugin.browser_azure_oauth2_credentials_provider."
        "BrowserAzureOAuth2CredentialsProvider.fetch_authorization_token",
        return_value=mock_token,
    )
    mocker.patch(
        "redshift_connector.plugin.browser_azure_oauth2_credentials_provider."
        "BrowserAzureOAuth2CredentialsProvider.fetch_jwt_response",
        return_value=mock_content,
    )
    mocker.patch(
        "redshift_connector.plugin.browser_azure_oauth2_credentials_provider."
        "BrowserAzureOAuth2CredentialsProvider.extract_jwt_assertion",
        return_value=mock_jwt_assertion,
    )
    acp, rp = make_valid_azure_oauth2_provider()

    fetch_token_spy = mocker.spy(acp, "fetch_authorization_token")
    fetch_jwt_spy = mocker.spy(acp, "fetch_jwt_response")
    extract_jwt_spy = mocker.spy(acp, "extract_jwt_assertion")

    jwt_assertion: str = acp.get_jwt_assertion()

    assert fetch_token_spy.called is True
    assert fetch_token_spy.call_count == 1

    assert fetch_jwt_spy.called is True
    assert fetch_jwt_spy.call_count == 1
    assert fetch_jwt_spy.call_args[0][0] == mock_token

    assert extract_jwt_spy.called is True
    assert extract_jwt_spy.call_count == 1
    assert extract_jwt_spy.call_args[0][0] == mock_content

    assert jwt_assertion == mock_jwt_assertion
