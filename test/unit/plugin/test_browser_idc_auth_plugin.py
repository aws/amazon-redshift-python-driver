import typing

import pytest
from pytest_mock import mocker  # type: ignore

from redshift_connector.error import InterfaceError
from redshift_connector.plugin.browser_idc_auth_plugin import BrowserIdcAuthPlugin
from redshift_connector.redshift_property import RedshiftProperty


def make_valid_browser_idc_provider() -> typing.Tuple[BrowserIdcAuthPlugin, RedshiftProperty]:
    rp: RedshiftProperty = RedshiftProperty()
    rp.idc_region = "some_region"
    rp.start_url = "some_url"
    rp.idc_response_timeout = 120
    cp: BrowserIdcAuthPlugin = BrowserIdcAuthPlugin()
    cp.add_parameter(rp)
    return cp, rp


def test_add_parameter_sets_browser_idc_specific():
    idc_credentials_provider, rp = make_valid_browser_idc_provider()
    assert idc_credentials_provider.idc_region == rp.idc_region
    assert idc_credentials_provider.start_url == rp.start_url
    assert idc_credentials_provider.idc_response_timeout == rp.idc_response_timeout


@pytest.mark.parametrize("value", [None, ""])
def test_check_required_parameters_raises_if_start_url_missing(value):
    idc_credentials_provider, _ = make_valid_browser_idc_provider()
    idc_credentials_provider.start_url = value

    with pytest.raises(
        InterfaceError, match="IdC authentication failed: The start URL must be included in the connection parameters."
    ):
        idc_credentials_provider.get_auth_token()


@pytest.mark.parametrize("value", [None, ""])
def test_check_required_parameters_raises_if_idc_region_missing(value):
    idc_credentials_provider, _ = make_valid_browser_idc_provider()
    idc_credentials_provider.idc_region = value

    with pytest.raises(
        InterfaceError, match="IdC authentication failed: The IdC region must be included in the connection parameters."
    ):
        idc_credentials_provider.get_auth_token()


def test_get_auth_token_fetches_idc_token(mocker):
    # Mock the dependencies and their return values
    idc_credentials_provider, rp = make_valid_browser_idc_provider()

    test_register_client_cache_key: str = (
        f"{idc_credentials_provider.idc_client_display_name}:{idc_credentials_provider.idc_region}"
    )
    mocked_register_client_result: typing.Dict[str, typing.Any] = {
        "clientId": "mockedClientId",
        "clientSecret": "mockedClientSecret",
    }
    mocked_start_device_auth_result: typing.Dict[str, typing.Any] = {
        "verificationUriComplete": "http://mockedVerificationUriComplete"
    }
    expected_idc_token: str = "mockedAccessToken"

    mocker.patch("boto3.client")  # Mocking boto3.client

    # Mocking the response of internal methods
    mocker.patch.object(idc_credentials_provider, "register_client", return_value=mocked_register_client_result)
    mocker.patch.object(
        idc_credentials_provider, "start_device_authorization", return_value=mocked_start_device_auth_result
    )
    mocker.patch("redshift_connector.plugin.browser_idc_auth_plugin.BrowserIdcAuthPlugin.open_browser")
    mocker.patch.object(idc_credentials_provider, "poll_for_create_token", return_value=expected_idc_token)

    # Call the method under test
    test_result_idc_token: str = idc_credentials_provider.get_auth_token()

    # Assertions
    idc_credentials_provider.register_client.assert_called_once_with(
        test_register_client_cache_key,
        idc_credentials_provider.idc_client_display_name,
        idc_credentials_provider.CLIENT_TYPE,
        idc_credentials_provider.IDC_SCOPE,
    )
    idc_credentials_provider.start_device_authorization.assert_called_once_with(
        mocked_register_client_result["clientId"],
        mocked_register_client_result["clientSecret"],
        idc_credentials_provider.start_url,
    )
    idc_credentials_provider.open_browser.assert_called_once_with(
        mocked_start_device_auth_result["verificationUriComplete"]
    )
    idc_credentials_provider.poll_for_create_token.assert_called_once_with(
        mocked_register_client_result, mocked_start_device_auth_result, idc_credentials_provider.GRANT_TYPE
    )

    assert test_result_idc_token == expected_idc_token


def test_register_client_exception_handling(mocker):
    idc_credentials_provider, rp = make_valid_browser_idc_provider()

    mocker.patch.object(idc_credentials_provider, "register_client", side_effect=Exception("Some error"))

    with pytest.raises(InterfaceError):
        idc_credentials_provider.get_auth_token()


def test_start_device_authorization_exception_handling(mocker):
    idc_credentials_provider, rp = make_valid_browser_idc_provider()
    mocked_register_client_result: typing.Dict[str, typing.Any] = {
        "clientId": "mockedClientId",
        "clientSecret": "mockedClientSecret",
    }

    mocker.patch.object(idc_credentials_provider, "register_client", return_value=mocked_register_client_result)
    mocker.patch.object(idc_credentials_provider, "start_device_authorization", side_effect=Exception("Some error"))

    with pytest.raises(InterfaceError):
        idc_credentials_provider.get_auth_token()


def test_poll_for_create_token_exception_handling(mocker):
    idc_credentials_provider, rp = make_valid_browser_idc_provider()
    mocked_register_client_result: typing.Dict[str, typing.Any] = {
        "clientId": "mockedClientId",
        "clientSecret": "mockedClientSecret",
    }
    mocked_start_device_auth_result: typing.Dict[str, typing.Any] = {
        "verificationUriComplete": "http://mockedVerificationUriComplete"
    }

    mocker.patch.object(idc_credentials_provider, "register_client", return_value=mocked_register_client_result)
    mocker.patch.object(
        idc_credentials_provider, "start_device_authorization", return_value=mocked_start_device_auth_result
    )
    mocker.patch.object(idc_credentials_provider, "poll_for_create_token", side_effect=Exception("Unexpected error"))

    with pytest.raises(InterfaceError):
        idc_credentials_provider.get_auth_token()
