import json
import typing
from unittest.mock import MagicMock, Mock, patch

import pytest  # type: ignore
import requests

from redshift_connector import InterfaceError, RedshiftProperty
from redshift_connector.plugin import OktaCredentialsProvider
from redshift_connector.plugin.credential_provider_constants import okta_headers


def make_valid_okta_credentials_provider():
    rp: RedshiftProperty = RedshiftProperty()
    rp.user_name = "AzureDiamond"
    rp.password = "hunter2"
    rp.idp_host = "test_idp_host"
    rp.app_id = "test_app_id"
    rp.app_name = "test_app_name"
    ocp: OktaCredentialsProvider = OktaCredentialsProvider()
    ocp.add_parameter(rp)
    return ocp, rp


def test_add_parameter_sets_okta_specific():
    ocp, rp = make_valid_okta_credentials_provider()

    assert ocp.app_id == rp.app_id
    assert ocp.app_name == rp.app_name


@pytest.mark.parametrize("value", [None, ""])
def test_get_saml_assertion_missing_app_id_should_fail(value):
    ocp, _ = make_valid_okta_credentials_provider()
    ocp.app_id = value

    with pytest.raises(InterfaceError, match="Missing required property: app_id"):
        ocp.get_saml_assertion()


def test_get_saml_assertion_should_call_okta_authentication(mocker):
    ocp, _ = make_valid_okta_credentials_provider()
    mocked_session_token: str = "my_first_session_token"
    mocker.patch(
        "redshift_connector.plugin.OktaCredentialsProvider.okta_authentication",
        return_value=mocked_session_token,
    )
    mocker.patch(
        "redshift_connector.plugin.OktaCredentialsProvider.handle_saml_assertion",
        return_value=None,
    )

    spy_okta_authentication = mocker.spy(OktaCredentialsProvider, "okta_authentication")
    spy_handle_saml_assertion = mocker.spy(OktaCredentialsProvider, "handle_saml_assertion")

    ocp.get_saml_assertion()

    assert spy_okta_authentication.called
    assert spy_okta_authentication.call_count == 1

    assert spy_handle_saml_assertion.called
    assert spy_handle_saml_assertion.call_count == 1
    assert spy_handle_saml_assertion.call_args[0][0] == mocked_session_token


@pytest.mark.parametrize(
    "error",
    [
        requests.exceptions.HTTPError,
        requests.exceptions.Timeout,
        requests.exceptions.TooManyRedirects,
        requests.exceptions.RequestException,
    ],
)
def test_okta_authentication_request_fails_should_fail(mocker, error):
    ocp, _ = make_valid_okta_credentials_provider()

    with patch("requests.post") as mock_request:
        mock_request.side_effect = error
        with pytest.raises(InterfaceError):
            ocp.okta_authentication()


def test_okta_authentication_no_status_in_response_should_fail(mocker):
    ocp, _ = make_valid_okta_credentials_provider()
    MockRequest: MagicMock = MagicMock()
    MockRequest.raise_for_status.return_value = None
    MockRequest.json.return_value = {"animal": "raccoon"}

    mocker.patch("requests.post", return_value=MockRequest)

    with pytest.raises(InterfaceError) as e:
        ocp.okta_authentication()
    assert "Request for authentication retrieved malformed payload" in str(e.value)


def test_okta_authentication_not_success_status_in_response_should_fail(mocker):
    ocp, _ = make_valid_okta_credentials_provider()
    MockRequest: MagicMock = MagicMock()
    MockRequest.raise_for_status.return_value = None
    MockRequest.json.return_value = {"status": "FAILURE"}

    mocker.patch("requests.post", return_value=MockRequest)

    with pytest.raises(InterfaceError) as e:
        ocp.okta_authentication()
    assert "Request for authentication received non success response" in str(e.value)


def test_okta_authentication_should_return_session_token(mocker):
    ocp, _ = make_valid_okta_credentials_provider()
    MockRequest: MagicMock = MagicMock()
    MockRequest.raise_for_status.return_value = None
    mocked_session_token: str = "my_first_session_token"
    MockRequest.json.return_value = {"status": "SUCCESS", "sessionToken": mocked_session_token}

    mocker.patch("requests.post", return_value=MockRequest)

    assert ocp.okta_authentication() == mocked_session_token


def test_okta_authentication_payload_is_correct(mocker):
    ocp, _ = make_valid_okta_credentials_provider()
    MockRequest: MagicMock = MagicMock()
    MockRequest.raise_for_status.return_value = None

    mocked_session_token: str = "my_first_session_token"
    MockRequest.json.return_value = {"status": "SUCCESS", "sessionToken": mocked_session_token}

    mocker.patch("requests.post", return_value=MockRequest)
    spy = mocker.spy(requests, "post")

    ocp.okta_authentication()
    assert spy.called
    assert spy.call_count == 1

    assert spy.call_args[0][0] == "https://{host}/api/v1/authn".format(host=ocp.idp_host)

    payload: typing.Dict = json.loads(spy.call_args[1]["data"])

    assert payload["username"] == ocp.user_name
    assert payload["password"] == ocp.password
    assert spy.call_args[1]["headers"] == okta_headers


@pytest.mark.parametrize(
    "error",
    [
        requests.exceptions.HTTPError,
        requests.exceptions.Timeout,
        requests.exceptions.TooManyRedirects,
        requests.exceptions.RequestException,
    ],
)
def test_handle_saml_assertion_request_fails_should_fail(mocker, error):
    ocp, _ = make_valid_okta_credentials_provider()

    with patch("requests.get") as mock_request:
        mock_request.side_effect = error
        with pytest.raises(InterfaceError):
            ocp.handle_saml_assertion("test")


def test_handle_saml_assertion_valid_html_response_should_success(mocker):
    ocp, _ = make_valid_okta_credentials_provider()
    MockRequest: MagicMock = MagicMock()
    MockRequest.raise_for_status.return_value = None
    mocked_saml_response: str = "my_first_saml_response"
    MockRequest.text = '<html><body><input name="SAMLResponse" type="hidden" value="{}"/><input name="RelayState" type="hidden" value=""/></body></html>'.format(
        mocked_saml_response
    )
    mocker.patch("requests.get", return_value=MockRequest)

    assert ocp.handle_saml_assertion("test") == mocked_saml_response


@pytest.mark.parametrize(
    "data",
    ["<html></html>", None, "", '<html><body><input name="RelayState" type="hidden" value=""/></body></html>'],
)
def test_handle_saml_assertion_invalid_html_response_should_fail(mocker, data):
    ocp, _ = make_valid_okta_credentials_provider()
    MockRequest: MagicMock = MagicMock()
    MockRequest.raise_for_status.return_value = None
    MockRequest.text = data
    mocker.patch("requests.get", return_value=MockRequest)

    with pytest.raises(InterfaceError):
        ocp.handle_saml_assertion("test")
