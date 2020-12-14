import typing
from test.unit.mocks.mock_socket import MockSocket
from test.unit.plugin.data import saml_response_data
from unittest.mock import Mock, patch

import pytest  # type: ignore

from redshift_connector.error import InterfaceError
from redshift_connector.plugin.browser_saml_credentials_provider import (
    BrowserSamlCredentialsProvider,
)

http_response_datas: typing.List[bytes] = [
    saml_response_data.valid_http_response_with_header_equal_delim,
    saml_response_data.valid_http_response_with_header_colon_delim,
    saml_response_data.valid_http_response_no_header,
]


@pytest.fixture(autouse=True)
def cleanup_mock_socket():
    # cleans up class attribute that mocks data the socket receives
    MockSocket.mocked_data = None


@pytest.mark.parametrize("http_response", http_response_datas)
def test_run_server_parses_saml_response(http_response):
    MockSocket.mocked_data = http_response
    with patch("socket.socket", return_value=MockSocket()):
        browser_saml_credentials: BrowserSamlCredentialsProvider = BrowserSamlCredentialsProvider()
        parsed_saml_response: str = browser_saml_credentials.run_server(listen_port=0, idp_response_timeout=5)
        assert parsed_saml_response == saml_response_data.encoded_saml_response.decode("utf-8")


invalid_login_url_vals: typing.List[typing.Optional[str]] = ["", None]


@pytest.mark.parametrize("login_url", invalid_login_url_vals)
def test_get_saml_assertion_no_login_url_should_fail(login_url):
    browser_saml_credentials: BrowserSamlCredentialsProvider = BrowserSamlCredentialsProvider()
    browser_saml_credentials.login_url = login_url

    with pytest.raises(InterfaceError) as ex:
        browser_saml_credentials.get_saml_assertion()
    assert "Missing required property: login_url" in str(ex.value)


def test_get_saml_assertion_low_idp_response_timeout_should_fail():
    browser_saml_credentials: BrowserSamlCredentialsProvider = BrowserSamlCredentialsProvider()
    browser_saml_credentials.login_url = "https://www.example.com"
    browser_saml_credentials.idp_response_timeout = -1

    with pytest.raises(InterfaceError) as ex:
        browser_saml_credentials.get_saml_assertion()
    assert "idp_response_timeout should be 10 seconds or greater" in str(ex.value)


invalid_listen_port_vals: typing.List[int] = [-1, 0, 65536]


@pytest.mark.parametrize("listen_port", invalid_listen_port_vals)
def test_get_saml_assertion_invalid_listen_port_should_fail(listen_port):
    browser_saml_credentials: BrowserSamlCredentialsProvider = BrowserSamlCredentialsProvider()
    browser_saml_credentials.login_url = "https://www.example.com"
    browser_saml_credentials.idp_response_timeout = 11
    browser_saml_credentials.listen_port = listen_port

    with pytest.raises(InterfaceError) as ex:
        browser_saml_credentials.get_saml_assertion()
    assert "Invalid property value: listen_port" in str(ex.value)


def test_authenticate_returns_authorization_token(mocker):
    browser_saml_credentials: BrowserSamlCredentialsProvider = BrowserSamlCredentialsProvider()
    mock_authorization_token: str = "my_authorization_token"

    mocker.patch("redshift_connector.plugin.BrowserSamlCredentialsProvider.open_browser", return_value=None)
    mocker.patch(
        "redshift_connector.plugin.BrowserSamlCredentialsProvider.run_server", return_value=mock_authorization_token
    )

    assert browser_saml_credentials.authenticate() == mock_authorization_token


def test_authenticate_errors_should_fail(mocker):
    browser_saml_credentials: BrowserSamlCredentialsProvider = BrowserSamlCredentialsProvider()

    mocker.patch("redshift_connector.plugin.BrowserSamlCredentialsProvider.open_browser", return_value=None)
    with patch("redshift_connector.plugin.BrowserSamlCredentialsProvider.run_server") as mocked_server:
        mocked_server.side_effect = Exception("bad mistake")

        with pytest.raises(Exception, match="bad mistake"):
            browser_saml_credentials.authenticate()


def test_open_browser_no_url_should_fail():
    browser_saml_credentials: BrowserSamlCredentialsProvider = BrowserSamlCredentialsProvider()

    with pytest.raises(InterfaceError) as ex:
        browser_saml_credentials.open_browser()
    assert "the login_url could not be empty" in str(ex.value)
