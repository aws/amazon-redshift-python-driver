import typing

import pytest
from pytest_mock import mocker  # type: ignore

from redshift_connector.error import InterfaceError
from redshift_connector.plugin.browser_azure_oauth2_credentials_provider import (
    BrowserAzureOAuth2CredentialsProvider,
)
from redshift_connector.redshift_property import RedshiftProperty


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


def test_add_parameter_sets_azure_oauth2_specific():
    acp, rp = make_valid_azure_oauth2_provider()
    assert acp.idp_tenant == rp.idp_tenant
    assert acp.client_id == rp.client_id
    assert acp.scope == rp.scope
    assert acp.idp_response_timeout == rp.idp_response_timeout
    assert acp.listen_port == rp.listen_port


@pytest.mark.parametrize("value", [None, ""])
def test_check_required_parameters_raises_if_idp_tenant_missing_or_too_small(value):
    acp, _ = make_valid_azure_oauth2_provider()
    acp.idp_tenant = value

    with pytest.raises(InterfaceError, match="Missing required connection property: idp_tenant"):
        acp.get_jwt_assertion()


@pytest.mark.parametrize("value", [None, ""])
def test_check_required_parameters_raises_if_client_id_missing(value):
    acp, _ = make_valid_azure_oauth2_provider()
    acp.client_id = value

    with pytest.raises(InterfaceError, match="Missing required connection property: client_id"):
        acp.get_jwt_assertion()


@pytest.mark.parametrize("value", [None, ""])
def test_check_required_parameters_raises_if_idp_response_timeout_missing(value):
    acp, _ = make_valid_azure_oauth2_provider()
    acp.idp_response_timeout = value

    with pytest.raises(
        InterfaceError,
        match="Invalid value specified for connection property: idp_response_timeout. Must be 10 seconds or greater",
    ):
        acp.get_jwt_assertion()


def test_get_jwt_assertion_fetches_and_extracts(mocker):
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
