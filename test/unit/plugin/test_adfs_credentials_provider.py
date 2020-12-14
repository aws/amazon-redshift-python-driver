from unittest.mock import MagicMock, Mock, patch

import pytest  # type: ignore
import requests

from redshift_connector import InterfaceError, RedshiftProperty
from redshift_connector.plugin import AdfsCredentialsProvider


def make_valid_adfs_credentials_provider():
    rp: RedshiftProperty = RedshiftProperty()
    rp.user_name = "AzureDiamond"
    rp.password = "hunter2"
    acp: AdfsCredentialsProvider = AdfsCredentialsProvider()

    rp.idp_host = "example.com"
    acp.add_parameter(rp)
    return acp, rp


@pytest.mark.parametrize("value", [None, ""])
def test_get_saml_assertion_invalid_idp_host_should_fail(value):
    acp, _ = make_valid_adfs_credentials_provider()
    acp.idp_host = value

    with pytest.raises(InterfaceError, match="Missing required property: idp_host"):
        acp.get_saml_assertion()


@pytest.mark.parametrize("value", [None, ""])
def test_get_saml_assertion_missing_user_name_should_prompt_windows_integrated_auth(value, mocker):
    acp, _ = make_valid_adfs_credentials_provider()
    acp.user_name = value

    mocker.patch(
        "redshift_connector.plugin.AdfsCredentialsProvider.windows_integrated_authentication", return_value=None
    )
    mocker.patch("redshift_connector.plugin.AdfsCredentialsProvider.form_based_authentication", return_value=None)
    spy = mocker.spy(AdfsCredentialsProvider, "windows_integrated_authentication")

    acp.get_saml_assertion()

    assert spy.called
    assert spy.call_count == 1


@pytest.mark.parametrize("value", [None, ""])
def test_get_saml_assertion_missing_password_should_prompt_windows_integrated_auth(value, mocker):
    acp, _ = make_valid_adfs_credentials_provider()
    acp.password = value

    mocker.patch(
        "redshift_connector.plugin.AdfsCredentialsProvider.windows_integrated_authentication", return_value=None
    )
    mocker.patch("redshift_connector.plugin.AdfsCredentialsProvider.form_based_authentication", return_value=None)
    spy = mocker.spy(AdfsCredentialsProvider, "windows_integrated_authentication")

    acp.get_saml_assertion()

    assert spy.called
    assert spy.call_count == 1


@pytest.mark.parametrize(
    "error",
    [
        requests.exceptions.HTTPError,
        requests.exceptions.Timeout,
        requests.exceptions.TooManyRedirects,
        requests.exceptions.RequestException,
    ],
)
def test_form_based_authentication_request_error_should_fail(error):
    acp, _ = make_valid_adfs_credentials_provider()

    with patch("requests.get") as mock_request:
        mock_request.side_effect = error

        with pytest.raises(InterfaceError) as e:
            acp.form_based_authentication()


def test_form_based_authentication_payload_is_correct(mocker):
    acp, _ = make_valid_adfs_credentials_provider()
    mock_auth_form = MagicMock()
    mock_auth_form.text = open("test/unit/plugin/data/mock_adfs_sign_in.html").read()  # mocked auth form

    mock_saml_response = MagicMock()
    mock_saml_response.text = open(
        "test/unit/plugin/data/mock_adfs_saml_response.html"
    ).read()  # mocked HTML response with SAMLResponse

    mocker.patch("requests.get", return_value=mock_auth_form)
    mocker.patch("requests.post", return_value=mock_saml_response)

    form_request_spy = mocker.spy(requests, "get")
    auth_request_spy = mocker.spy(requests, "post")

    assert acp.form_based_authentication() is not None

    assert form_request_spy.called
    assert form_request_spy.call_count == 1
    assert form_request_spy.call_args[0][0] == (
        "https://{host}:{port}/adfs/ls/IdpInitiatedSignOn.aspx?loginToRp=urn:amazon:webservices".format(
            host=acp.idp_host, port=str(acp.idpPort)
        )
    )

    assert auth_request_spy.called
    assert auth_request_spy.call_count == 1
    assert (
        auth_request_spy.call_args[0][0]
        == "https://example.com:443/adfs/ls/IdpInitiatedSignOn.aspx?loginToRp=urn:amazon:webservices3&client-request-id=1234"
    )
    assert auth_request_spy.call_args[1]["data"]["UserName"] == acp.user_name
    assert auth_request_spy.call_args[1]["data"]["Password"] == acp.password
    assert auth_request_spy.call_args[1]["data"]["Kmsi"] == "true"


@pytest.mark.parametrize(
    "error",
    [
        requests.exceptions.HTTPError,
        requests.exceptions.Timeout,
        requests.exceptions.TooManyRedirects,
        requests.exceptions.RequestException,
    ],
)
def test_form_based_authentication_login_fails_should_fail(error, mocker):
    acp, _ = make_valid_adfs_credentials_provider()
    mock_auth_form = MagicMock()
    mock_auth_form.text = open("test/unit/plugin/data/mock_adfs_sign_in.html").read()  # mocked auth form

    mocker.patch("requests.get", return_value=mock_auth_form)
    form_request_spy = mocker.spy(requests, "get")

    with patch("requests.post") as mock_login_request:
        mock_login_request.side_effect = error

        with pytest.raises(InterfaceError):
            acp.form_based_authentication()

    assert form_request_spy.called
    assert form_request_spy.call_count == 1


def test_form_based_authentication_saml_response_parse_fail_should_fail(mocker):
    acp, _ = make_valid_adfs_credentials_provider()
    mock_auth_form = MagicMock()
    mock_auth_form.text = open("test/unit/plugin/data/mock_adfs_sign_in.html").read()  # mocked auth form

    mocker.patch("requests.get", return_value=mock_auth_form)
    mocker.patch("requests.post", return_value=MagicMock())

    with patch("bs4.BeautifulSoup") as mock_xml_parser:
        mock_xml_parser.side_effect = Exception

        with pytest.raises(InterfaceError):
            acp.form_based_authentication()


def test_form_based_authentication_empty_saml_response_should_fail(mocker):
    acp, _ = make_valid_adfs_credentials_provider()
    mock_auth_form = MagicMock()
    mock_auth_form.text = open("test/unit/plugin/data/mock_adfs_sign_in.html").read()  # mocked auth form

    mocker.patch("requests.get", return_value=mock_auth_form)
    mocker.patch("requests.post", return_value=MagicMock())
    mock_soup = MagicMock()
    mock_soup.find_all.return_value = iter([])
    mocker.patch("bs4.BeautifulSoup", return_value=mock_soup)

    with pytest.raises(InterfaceError, match="Failed to find Adfs access_token"):
        acp.form_based_authentication()
