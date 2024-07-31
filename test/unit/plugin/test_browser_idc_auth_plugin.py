import socket
import time
import typing
from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError
from pytest_mock import mocker

from redshift_connector.error import InterfaceError
from redshift_connector.plugin.browser_idc_auth_plugin import BrowserIdcAuthPlugin
from redshift_connector.redshift_property import RedshiftProperty


def make_valid_browser_idc_provider() -> typing.Tuple[BrowserIdcAuthPlugin, RedshiftProperty]:
    rp: RedshiftProperty = RedshiftProperty()
    rp.idc_region = "some_region"
    rp.issuer_url = "some_url"
    rp.idp_response_timeout = 100
    rp.listen_port = 8000
    cp: BrowserIdcAuthPlugin = BrowserIdcAuthPlugin()
    cp.add_parameter(rp)
    return cp, rp


def valid_browser_without_optional_parameter() -> typing.Tuple[BrowserIdcAuthPlugin, RedshiftProperty]:
    rp: RedshiftProperty = RedshiftProperty()
    rp.idc_region = "some_region"
    rp.issuer_url = "some_url"
    cp: BrowserIdcAuthPlugin = BrowserIdcAuthPlugin()
    cp.add_parameter(rp)
    return cp, rp


def test_add_parameter_sets_browser_idc_specific():
    idc_credentials_provider, rp = make_valid_browser_idc_provider()
    assert idc_credentials_provider.idc_region == rp.idc_region
    assert idc_credentials_provider.issuer_url == rp.issuer_url
    assert idc_credentials_provider.idp_response_timeout == rp.idp_response_timeout
    assert idc_credentials_provider.listen_port == rp.listen_port


def test_add_parameter_sets_default():
    idc_credentials_provider, rp = valid_browser_without_optional_parameter()
    assert idc_credentials_provider.idp_response_timeout == 120
    assert idc_credentials_provider.listen_port == 7890
    assert idc_credentials_provider.idc_client_display_name == "Amazon Redshift Python connector"


@pytest.mark.parametrize("value", [None, ""])
def test_check_required_parameters_raises_if_issuer_url_missing(value):
    idc_credentials_provider, _ = make_valid_browser_idc_provider()
    idc_credentials_provider.issuer_url = value

    with pytest.raises(
        InterfaceError, match="IdC authentication failed: The issuer_url must be included in the connection parameters."
    ):
        idc_credentials_provider.get_auth_token()


@pytest.mark.parametrize("value", [None, ""])
def test_check_required_parameters_raises_if_idc_region_missing(value):
    idc_credentials_provider, _ = make_valid_browser_idc_provider()
    idc_credentials_provider.idc_region = value

    with pytest.raises(
        InterfaceError, match="IdC authentication failed: The idc_region must be included in the connection parameters."
    ):
        idc_credentials_provider.get_auth_token()


def test_valid_register_client():
    idc_credentials_provider, rp = make_valid_browser_idc_provider()
    mocked_register_client_result: typing.Dict[str, typing.Any] = {
        "clientId": "mockedClientId",
        "clientSecret": "mockedClientSecret",
        "clientSecretExpiresAt": time.time() + 60,
    }

    mocked_boto_client = MagicMock()
    mocked_boto_client.register_client.return_value = mocked_register_client_result

    idc_credentials_provider.sso_oidc_client = mocked_boto_client

    register_client_result = idc_credentials_provider.register_client()

    assert register_client_result == mocked_register_client_result


def test_register_client_interface_exception():
    idc_credentials_provider, rp = make_valid_browser_idc_provider()

    mocked_boto_client = MagicMock()
    error_response = {
        "Error": {"Code": "400", "Message": "IdC authentication failed : Error registering client with IdC."}
    }

    operation_name = "RegisterClient"
    mocked_boto_client.register_client.side_effect = ClientError(error_response, operation_name)
    idc_credentials_provider.sso_oidc_client = mocked_boto_client

    with pytest.raises(InterfaceError, match="IdC authentication failed : Error registering client with IdC."):
        idc_credentials_provider.register_client()


def test_register_client_cache():
    idc_cred, rp = make_valid_browser_idc_provider()
    mocked_register_client_result: typing.Dict[str, typing.Any] = {
        "clientId": "mockedClientId",
        "clientSecret": "mockedClientSecret",
        "clientSecretExpiresAt": time.time() + 60,
    }
    mocked_cache_key: str = f"{idc_cred.idc_client_display_name}:{idc_cred.idc_region}:{idc_cred.listen_port}"
    mocked_cache: typing.Dict[str, dict] = {
        mocked_cache_key: mocked_register_client_result,
    }

    idc_cred.register_client_cache = mocked_cache

    register_client_result = idc_cred.register_client()

    assert register_client_result == mocked_register_client_result


def test_register_client_cache_expired():
    idc_cred, rp = make_valid_browser_idc_provider()
    mocked_client_expired_result: typing.Dict[str, typing.Any] = {
        "clientId": "expiredClientId",
        "clientSecret": "expiredClientSecret",
        "clientSecretExpiresAt": time.time(),
    }
    mocked_cache_key: str = f"{idc_cred.idc_client_display_name}:{idc_cred.idc_region}:{idc_cred.listen_port}"
    mocked_cache: typing.Dict[str, dict] = {
        mocked_cache_key: mocked_client_expired_result,
    }
    idc_cred.register_client_cache = mocked_cache

    mocked_client_result: typing.Dict[str, typing.Any] = {
        "clientId": "mockedClientId",
        "clientSecret": "mockedClientSecret",
        "clientSecretExpiresAt": time.time() + 60,
    }

    mocked_boto_client = MagicMock()
    mocked_boto_client.register_client.return_value = mocked_client_result
    idc_cred.sso_oidc_client = mocked_boto_client
    register_client_result = idc_cred.register_client()

    assert register_client_result["clientId"] == "mockedClientId"


def test_register_client_exception_handling(mocker):
    idc_credentials_provider, rp = make_valid_browser_idc_provider()

    mocker.patch.object(idc_credentials_provider, "register_client", side_effect=Exception("Some error"))

    with pytest.raises(InterfaceError):
        idc_credentials_provider.get_auth_token()


def test_fetch_authorization_code_exception_handling(mocker):
    idc_credentials_provider, rp = make_valid_browser_idc_provider()
    mocked_register_client_result: typing.Dict[str, typing.Any] = {
        "clientId": "mockedClientId",
        "clientSecret": "mockedClientSecret",
    }

    mocker.patch.object(idc_credentials_provider, "register_client", return_value=mocked_register_client_result)
    mocker.patch.object(idc_credentials_provider, "fetch_authorization_code", side_effect=Exception("Some error"))

    with pytest.raises(InterfaceError):
        idc_credentials_provider.get_auth_token()


def test_fetch_authorization_code_exception(mocker):
    idc_credentials_provider, rp = make_valid_browser_idc_provider()
    mocked_register_client_result: typing.Dict[str, typing.Any] = {
        "clientId": "mockedClientId",
        "clientSecret": "mockedClientSecret",
    }

    mocker.patch.object(idc_credentials_provider, "register_client", return_value=mocked_register_client_result)
    mocker.patch.object(idc_credentials_provider, "fetch_authorization_code", side_effect=Exception("Some error"))

    with pytest.raises(Exception):
        idc_credentials_provider.fetch_authorization_code()


def test_valid_fetch_access_token():
    idc_credentials_provider, rp = make_valid_browser_idc_provider()
    idc_credentials_provider.redirect_uri = "http://127.0.0.1:8000"
    mocked_register_client_result: typing.Dict[str, typing.Any] = {
        "clientId": "mockedClientId",
        "clientSecret": "mockedClientSecret",
    }
    mocked_access_token_result: typing.Dict[str, typing.Any] = {
        "accessToken": "validAccessToken",
    }
    mocked_auth_code: str = "validAuthCode"
    mocked_verifier: str = "validVerifier"
    expected_access_token = "validAccessToken"

    mocked_boto_client = MagicMock()
    mocked_boto_client.create_token.return_value = mocked_access_token_result

    idc_credentials_provider.sso_oidc_client = mocked_boto_client

    accessToken = idc_credentials_provider.fetch_access_token(
        mocked_register_client_result, mocked_auth_code, mocked_verifier
    )

    assert accessToken == expected_access_token


def test_fetch_access_token_exception_handling(mocker):
    idc_credentials_provider, rp = make_valid_browser_idc_provider()
    mocked_register_client_result: typing.Dict[str, typing.Any] = {
        "clientId": "mockedClientId",
        "clientSecret": "mockedClientSecret",
    }
    mocked_fetch_authorization_code_result: str = {"mockedAuthCode"}

    mocker.patch.object(idc_credentials_provider, "register_client", return_value=mocked_register_client_result)
    mocker.patch.object(
        idc_credentials_provider, "fetch_authorization_code", return_value=mocked_fetch_authorization_code_result
    )
    mocker.patch.object(idc_credentials_provider, "fetch_access_token", side_effect=Exception("Unexpected error"))

    with pytest.raises(InterfaceError):
        idc_credentials_provider.get_auth_token()


def test_get_auth_token_fetches_idc_token(mocker):
    # Mock the dependencies and their return values
    idc_credentials_provider, rp = make_valid_browser_idc_provider()

    mocked_register_client_result: typing.Dict[str, typing.Any] = {
        "clientId": "mockedClientId",
        "clientSecret": "mockedClientSecret",
    }
    mocked_fetch_authorization_code_result: str = {"mockedAuthCode"}
    expected_idc_token: str = "mockedAccessToken"

    mocker.patch("boto3.client")  # Mocking boto3.client

    # Mocking the response of internal methods
    mocker.patch.object(idc_credentials_provider, "register_client", return_value=mocked_register_client_result)
    mocker.patch.object(
        idc_credentials_provider, "fetch_authorization_code", return_value=mocked_fetch_authorization_code_result
    )
    mocker.patch.object(idc_credentials_provider, "fetch_access_token", return_value=expected_idc_token)

    # Call the method under test
    test_result_idc_token: str = idc_credentials_provider.get_auth_token()

    assert test_result_idc_token == expected_idc_token


def test_authorization_token_url():
    idc_credentials_provider, rp = make_valid_browser_idc_provider()
    mocked_state: str = "mockedState"
    mocked_client_id: str = "mockedClientId"
    mocked_code_challenge: str = "mockedCodeChallenge"
    expected_url = "https://oidc.some_region.amazonaws.com/authorize?response_type=code&client_id=mockedClientId&redirect_uri=None&state=mockedState&scopes=redshift%3Aconnect&code_challenge=mockedCodeChallenge&code_challenge_method=S256"

    url: str = idc_credentials_provider.get_authorization_token_url(
        mocked_state, mocked_client_id, mocked_code_challenge
    )
    assert url == expected_url


def test_generate_random_state():
    idc_credentials_provider, rp = make_valid_browser_idc_provider()
    state: str = idc_credentials_provider.generate_random_state()

    assert len(state) == 14


def test_get_listen_socket():
    idc_credentials_provider, rp = make_valid_browser_idc_provider()
    mocked_port: str = 8000
    expected_socket = "('127.0.0.1', 8000)"

    listen_socket: socket.socket = idc_credentials_provider.get_listen_socket(mocked_port)
    assert str(listen_socket.getsockname()) == expected_socket


def test_open_browser():
    idc_credentials_provider, rp = make_valid_browser_idc_provider()
    mocked_port: str = 8000
    expected_socket = "('127.0.0.1', 8000)"

    listen_socket: socket.socket = idc_credentials_provider.get_listen_socket(mocked_port)
    assert str(listen_socket.getsockname()) == expected_socket
