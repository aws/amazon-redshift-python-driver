import typing
import webbrowser
from test.unit.mocks.mock_socket import MockSocket
from test.unit.plugin.data import browser_azure_data
from unittest.mock import Mock, patch

import pytest  # type: ignore
import requests
from pytest_mock import mocker

from redshift_connector import RedshiftProperty
from redshift_connector.error import InterfaceError
from redshift_connector.plugin.browser_azure_credentials_provider import (
    BrowserAzureCredentialsProvider,
)

if typing.TYPE_CHECKING:
    import socket


@pytest.fixture(autouse=True)
def cleanup_mock_socket():
    # cleans up class attribute that mocks data the socket receives
    MockSocket.mocked_data = None


def make_valid_browser_azure_credential_provider() -> BrowserAzureCredentialsProvider:
    properties: RedshiftProperty = RedshiftProperty()
    properties.user_name = ""
    properties.password = ""
    bacp: BrowserAzureCredentialsProvider = BrowserAzureCredentialsProvider()
    bacp.add_parameter(properties)

    # browser azure specific values
    bacp.idp_tenant = "abcdefghijklmnopqrstuvwxyz"
    bacp.client_secret = "happy"
    bacp.client_id = "123455678"
    return bacp


invalid_idp_tenants: typing.List[typing.Optional[str]] = ["", None]


@pytest.mark.parametrize("idp_tenant_value", invalid_idp_tenants)
def test_get_saml_assertion_invalid_idp_tenant_should_fail(idp_tenant_value):
    bacp: BrowserAzureCredentialsProvider = make_valid_browser_azure_credential_provider()
    bacp.idp_tenant = idp_tenant_value

    with pytest.raises(InterfaceError) as ex:
        bacp.get_saml_assertion()
    assert "Missing required property: idp_tenant" in str(ex.value)


invalid_client_id: typing.List[typing.Optional[str]] = ["", None]


@pytest.mark.parametrize("idp_tenant_value", invalid_client_id)
def test_get_saml_assertion_invalid_client_id_should_fail(idp_tenant_value):
    bacp: BrowserAzureCredentialsProvider = make_valid_browser_azure_credential_provider()
    bacp.client_id = idp_tenant_value

    with pytest.raises(InterfaceError) as ex:
        bacp.get_saml_assertion()
    assert "Missing required property: client_id" in str(ex.value)


invalid_idp_response_timeouts: typing.List[typing.Optional[int]] = [-1, 0, 1, 9]


@pytest.mark.parametrize("idp_response_timeout_value", invalid_idp_response_timeouts)
def test_get_saml_assertion_invalid_idp_response_timeout_should_fail(idp_response_timeout_value):
    bacp: BrowserAzureCredentialsProvider = make_valid_browser_azure_credential_provider()
    bacp.idp_response_timeout = idp_response_timeout_value

    with pytest.raises(InterfaceError) as ex:
        bacp.get_saml_assertion()
    assert "idp_response_timeout should be 10 seconds or greater." in str(ex.value)


def get_listen_socket_chooses_free_socket():
    bacp: BrowserAzureCredentialsProvider = make_valid_browser_azure_credential_provider()
    ports: typing.Set[int] = set()
    sockets: typing.List["socket.socket"] = []

    for _ in range(5):  # open up 10 listen sockets
        _socket: "socket.socket" = bacp.get_listen_socket()
        sockets.append(_socket)

        assert _socket is not None
        assert _socket.getsockname()[0] == "127.0.0.1"
        listen_port = _socket.getsockname()[1]

        if listen_port in ports:
            raise pytest.fail("listen port collision")
        ports.add(listen_port)

    # clean up sockets
    for s in sockets:
        s.close()


def test_run_server():
    bacp: BrowserAzureCredentialsProvider = make_valid_browser_azure_credential_provider()
    MockSocket.mocked_data = browser_azure_data.valid_response

    result: str = bacp.run_server(listen_socket=MockSocket(), idp_response_timeout=10, state=browser_azure_data.state)
    assert result == browser_azure_data.code


invalid_datas = [
    (browser_azure_data.missing_code_response, "No code found"),
    (browser_azure_data.empty_code_response, "No valid code found"),
    (
        browser_azure_data.mismatched_state_response,
        "Incoming state {} does not match the outgoing state {}".format(
            browser_azure_data.state[::-1], browser_azure_data.state
        ),
    ),
]


@pytest.mark.parametrize("data", invalid_datas)
def test_run_server_invalid_data(data):
    data, expected_exception = data
    bacp: BrowserAzureCredentialsProvider = make_valid_browser_azure_credential_provider()
    MockSocket.mocked_data = data

    with pytest.raises(InterfaceError) as ex:
        bacp.run_server(listen_socket=MockSocket(), idp_response_timeout=10, state=browser_azure_data.state)
    assert expected_exception in str(ex.value)


request_errors: typing.List[typing.Callable] = [
    requests.exceptions.HTTPError,
    requests.exceptions.Timeout,
    requests.exceptions.TooManyRedirects,
    requests.exceptions.RequestException,
]


@patch("requests.post")
@pytest.mark.parametrize("error", request_errors)
def test_fetch_saml_response_error_should_fail(mocked_post, error):
    bacp: BrowserAzureCredentialsProvider = make_valid_browser_azure_credential_provider()
    mocked_post.side_effect = error

    with pytest.raises(InterfaceError) as ex:
        bacp.fetch_saml_response(token="blah")


@patch("requests.post")
def test_fetch_saml_response(mocked_post):
    bacp: BrowserAzureCredentialsProvider = make_valid_browser_azure_credential_provider()

    def mock_get_resp() -> requests.Response:
        r: requests.Response = requests.Response()
        r.status_code = 200

        def mock_get_json() -> typing.Dict:
            return browser_azure_data.valid_json_response

        r.json = mock_get_json  # type: ignore
        return r

    mocked_post.return_value = mock_get_resp()

    saml_assertion: str = bacp.fetch_saml_response(token="blah")
    assert str(browser_azure_data.saml_response) == saml_assertion


malformed_json_responses: typing.List[typing.Tuple[typing.Optional[typing.Dict], str]] = [
    (browser_azure_data.json_response_no_access_token, "access_token"),
    (browser_azure_data.json_response_empty_access_token, "Azure access_token is empty"),
]


@patch("requests.post")
@pytest.mark.parametrize("datas", malformed_json_responses)
def test_fetch_saml_response_malformed_should_fail(mocked_post, datas):
    data, expected_error = datas
    bacp: BrowserAzureCredentialsProvider = make_valid_browser_azure_credential_provider()

    def mock_get_resp() -> requests.Response:
        r: requests.Response = requests.Response()
        r.status_code = 200

        def mock_get_json() -> typing.Dict:
            return data

        r.json = mock_get_json  # type: ignore
        return r

    mocked_post.return_value = mock_get_resp()

    with pytest.raises(InterfaceError) as ex:
        bacp.fetch_saml_response(token="blah")

    assert expected_error in str(ex.value)


def test_open_browser(mocker):
    bacp: BrowserAzureCredentialsProvider = make_valid_browser_azure_credential_provider()
    expected_url: str = (
        "https://login.microsoftonline.com/{tenant}"
        "/oauth2/authorize"
        "?scope=openid"
        "&response_type=code"
        "&response_mode=form_post"
        "&client_id={id}"
        "&redirect_uri={uri}"
        "&state={state}".format(
            tenant=bacp.idp_tenant, id=bacp.client_id, uri=bacp.redirectUri, state=browser_azure_data.state
        )
    )
    mocker.patch("webbrowser.open", returnValue=None)
    spy = mocker.spy(webbrowser, "open")
    bacp.open_browser(state=browser_azure_data.state)

    assert spy.called
    assert spy.call_count == 1
    assert isinstance(spy.call_args[0][0], str) is True
    assert spy.call_args[0][0] == expected_url
