from unittest.mock import MagicMock, Mock, patch

import pytest  # type: ignore
import requests

from redshift_connector import InterfaceError, RedshiftProperty
from redshift_connector.plugin import AzureCredentialsProvider
from redshift_connector.plugin.credential_provider_constants import azure_headers


def make_valid_azure_credentials_provider():
    rp: RedshiftProperty = RedshiftProperty()
    rp.user_name = "AzureDiamond"
    rp.password = "hunter2"
    acp: AzureCredentialsProvider = AzureCredentialsProvider()

    rp.idp_tenant = "example.com"
    rp.client_secret = "peanut butter"
    rp.client_id = "1234"
    acp.add_parameter(rp)
    return acp, rp


def test_add_parameter_sets_azure_specific():
    acp, rp = make_valid_azure_credentials_provider()

    assert acp.idp_tenant == rp.idp_tenant
    assert acp.client_secret == rp.client_secret
    assert acp.client_id == rp.client_id


@pytest.mark.parametrize("value", [None, ""])
def test_get_saml_assertion_missing_user_name_should_fail(value):
    acp, _ = make_valid_azure_credentials_provider()
    acp.user_name = value

    with pytest.raises(InterfaceError, match="Missing required property: user_name"):
        acp.get_saml_assertion()


@pytest.mark.parametrize("value", [None, ""])
def test_get_saml_assertion_missing_password_should_fail(value):
    acp, _ = make_valid_azure_credentials_provider()
    acp.password = value

    with pytest.raises(InterfaceError, match="Missing required property: password"):
        acp.get_saml_assertion()


@pytest.mark.parametrize("value", [None, ""])
def test_get_saml_assertion_missing_idp_tenant_should_fail(value):
    acp, _ = make_valid_azure_credentials_provider()
    acp.idp_tenant = value

    with pytest.raises(InterfaceError, match="Missing required property: idp_tenant"):
        acp.get_saml_assertion()


@pytest.mark.parametrize("value", [None, ""])
def test_get_saml_assertion_missing_client_secret_should_fail(value):
    acp, _ = make_valid_azure_credentials_provider()
    acp.client_secret = value

    with pytest.raises(InterfaceError, match="Missing required property: client_secret"):
        acp.get_saml_assertion()


@pytest.mark.parametrize("value", [None, ""])
def test_get_saml_assertion_missing_client_id_should_fail(value):
    acp, _ = make_valid_azure_credentials_provider()
    acp.client_id = value

    with pytest.raises(InterfaceError, match="Missing required property: client_id"):
        acp.get_saml_assertion()


def test_get_saml_assertion_should_call_azure_oauth_based_authentication(mocker):
    acp, _ = make_valid_azure_credentials_provider()
    mocked_saml_response: str = "my_first_saml_response"
    mocker.patch(
        "redshift_connector.plugin.AzureCredentialsProvider.azure_oauth_based_authentication",
        return_value=mocked_saml_response,
    )
    spy = mocker.spy(AzureCredentialsProvider, "azure_oauth_based_authentication")

    assert acp.get_saml_assertion() == mocked_saml_response
    assert spy.called
    assert spy.call_count == 1


def test_azure_oauth_based_authentication_payload_is_correct(mocker):
    acp, rp = make_valid_azure_credentials_provider()
    MockRequest: MagicMock = MagicMock()
    MockRequest.raise_for_status.return_value = None
    MockRequest.json.return_value = {"access_token": "mocked_token"}

    mocker.patch("requests.post", return_value=MockRequest)
    spy = mocker.spy(requests, "post")

    acp.azure_oauth_based_authentication()
    assert spy.called
    assert spy.call_count == 1
    assert spy.call_args[0][0] == "https://login.microsoftonline.com/{tenant}/oauth2/token".format(
        tenant=acp.idp_tenant
    )
    assert spy.call_args[1]["data"]["username"] == acp.user_name
    assert spy.call_args[1]["data"]["password"] == acp.password
    assert spy.call_args[1]["data"]["client_secret"] == acp.client_secret
    assert spy.call_args[1]["data"]["client_id"] == acp.client_id
    assert spy.call_args[1]["data"]["resource"] == acp.client_id
    assert spy.call_args[1]["headers"] == azure_headers


@pytest.mark.parametrize(
    "error",
    [
        requests.exceptions.HTTPError,
        requests.exceptions.Timeout,
        requests.exceptions.TooManyRedirects,
        requests.exceptions.RequestException,
    ],
)
def test_azure_oauth_based_authentication_request_fails_should_fail(mocker, error):
    acp, _ = make_valid_azure_credentials_provider()

    with patch("requests.post") as mock_request:
        mock_request.side_effect = error
        with pytest.raises(InterfaceError):
            acp.azure_oauth_based_authentication()


def test_azure_oauth_based_authentication_response_missing_access_token_should_fail(mocker):
    acp, rp = make_valid_azure_credentials_provider()
    MockRequest: MagicMock = MagicMock()
    MockRequest.raise_for_status.return_value = None
    MockRequest.json.return_value = {"animal": "raccoon"}

    mocker.patch("requests.post", return_value=MockRequest)

    with pytest.raises(InterfaceError) as e:
        acp.azure_oauth_based_authentication()
    assert "access_token" in str(e.value)


def test_azure_oauth_based_authentication_response_empty_access_token_should_fail(mocker):
    acp, rp = make_valid_azure_credentials_provider()
    MockRequest: MagicMock = MagicMock()
    MockRequest.raise_for_status.return_value = None
    MockRequest.json.return_value = {"access_token": ""}

    mocker.patch("requests.post", return_value=MockRequest)

    with pytest.raises(InterfaceError) as e:
        acp.azure_oauth_based_authentication()
    assert "Azure access_token is empty" in str(e.value)
