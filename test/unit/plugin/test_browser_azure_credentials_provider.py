import typing
import webbrowser
from test.unit.mocks.mock_socket import MockSocket
from test.unit.plugin.data import browser_azure_data
from unittest.mock import MagicMock, Mock, patch

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


def test_get_saml_assertion_uses_listen_port(mocker):
    bacp: BrowserAzureCredentialsProvider = make_valid_browser_azure_credential_provider()
    mocked_token: str = "test_token"
    mocked_saml_assertion: str = "test_saml_assertion"
    mocked_socket = MockSocket()
    mocker.patch(
        "redshift_connector.plugin.BrowserAzureCredentialsProvider.get_listen_socket", return_value=mocked_socket
    )
    mocker.patch(
        "redshift_connector.plugin.BrowserAzureCredentialsProvider.fetch_authorization_token", return_value=mocked_token
    )
    mocker.patch(
        "redshift_connector.plugin.BrowserAzureCredentialsProvider.fetch_saml_response",
        return_value=mocked_saml_assertion,
    )
    mocker.patch(
        "redshift_connector.plugin.BrowserAzureCredentialsProvider.wrap_and_encode_assertion", return_value=None
    )

    get_listen_socket_spy = mocker.spy(BrowserAzureCredentialsProvider, "get_listen_socket")
    fetch_auth_spy = mocker.spy(BrowserAzureCredentialsProvider, "fetch_authorization_token")
    fetch_saml_spy = mocker.spy(BrowserAzureCredentialsProvider, "fetch_saml_response")
    wrap_and_encode_spy = mocker.spy(BrowserAzureCredentialsProvider, "wrap_and_encode_assertion")

    bacp.get_saml_assertion()

    assert bacp.redirectUri == "http://localhost:{port}/redshift/".format(port=bacp.listen_port)

    assert get_listen_socket_spy.called
    assert get_listen_socket_spy.call_count == 1

    assert fetch_auth_spy.called
    assert fetch_auth_spy.call_count == 1
    assert fetch_auth_spy.call_args[0][0] == mocked_socket

    assert fetch_saml_spy.called
    assert fetch_saml_spy.call_count == 1
    assert fetch_saml_spy.call_args[0][0] == mocked_token

    assert wrap_and_encode_spy.called
    assert wrap_and_encode_spy.call_count == 1
    assert wrap_and_encode_spy.call_args[0][0] == mocked_saml_assertion


def test_get_listen_socket_chooses_free_socket():
    bacp: BrowserAzureCredentialsProvider = make_valid_browser_azure_credential_provider()
    ports: typing.Set[int] = set()
    sockets: typing.List["socket.socket"] = []

    for _ in range(5):  # open up 10 listen sockets
        _socket: "socket.socket" = bacp.get_listen_socket()
        assert bacp.listen_port == _socket.getsockname()[1]
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


def test_fetch_authorization_token_returns_authorization_token(mocker):
    bacp: BrowserAzureCredentialsProvider = make_valid_browser_azure_credential_provider()
    mock_authorization_token: str = "my_authorization_token"

    mocker.patch("redshift_connector.plugin.BrowserAzureCredentialsProvider.open_browser", return_value=None)
    mocker.patch(
        "redshift_connector.plugin.BrowserAzureCredentialsProvider.run_server", return_value=mock_authorization_token
    )

    assert bacp.fetch_authorization_token(listen_socket=MockSocket()) == mock_authorization_token


def test_fetch_authorization_errors_should_fail(mocker):
    bacp: BrowserAzureCredentialsProvider = make_valid_browser_azure_credential_provider()

    mocker.patch("redshift_connector.plugin.BrowserAzureCredentialsProvider.open_browser", return_value=None)
    with patch("redshift_connector.plugin.BrowserAzureCredentialsProvider.run_server") as mocked_server:
        mocked_server.side_effect = Exception("bad mistake")

        with pytest.raises(Exception, match="bad mistake"):
            bacp.fetch_authorization_token(listen_socket=MockSocket())


def test_run_server():
    bacp: BrowserAzureCredentialsProvider = make_valid_browser_azure_credential_provider()
    MockSocket.mocked_data = browser_azure_data.valid_response

    result: str = bacp.run_server(listen_socket=MockSocket(), idp_response_timeout=10, state=browser_azure_data.state)
    assert result == browser_azure_data.code


def test_run_server_calls_get_success_response_http_msg(mocker):
    bacp: BrowserAzureCredentialsProvider = make_valid_browser_azure_credential_provider()
    MockSocket.mocked_data = browser_azure_data.valid_response
    listen_socket: MockSocket = MockSocket()
    spy = mocker.spy(bacp, "close_window_http_resp")

    result: str = bacp.run_server(listen_socket=listen_socket, idp_response_timeout=10, state=browser_azure_data.state)
    assert spy.called is True
    assert spy.call_count == 1


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
