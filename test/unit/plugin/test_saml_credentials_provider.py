import base64
import typing
from test.unit.plugin.data import saml_response_data
from unittest.mock import MagicMock, patch

import pytest  # type: ignore

from redshift_connector import InterfaceError, RedshiftProperty
from redshift_connector.credentials_holder import CredentialsHolder
from redshift_connector.plugin import SamlCredentialsProvider


@patch.multiple(SamlCredentialsProvider, __abstractmethods__=set())
def make_valid_saml_credentials_provider():
    rp: RedshiftProperty = RedshiftProperty()
    rp.user_name = "AzureDiamond"
    rp.password = "hunter2"
    scp: SamlCredentialsProvider = SamlCredentialsProvider()
    scp.add_parameter(rp)
    return scp, rp


def test_get_cache_key_format_as_expected():
    scp, _ = make_valid_saml_credentials_provider()
    expected_cache_key: str = "{username}{password}{idp_host}{idp_port}{duration}{preferred_role}".format(
        username=scp.user_name,
        password=scp.password,
        idp_host=scp.idp_host,
        idp_port=scp.idpPort,
        duration=scp.duration,
        preferred_role=scp.preferred_role,
    )
    assert scp.get_cache_key() == expected_cache_key


def test_get_credentials_uses_cache_when_exists(mocker):
    scp, _ = make_valid_saml_credentials_provider()
    mock_credentials = MagicMock()
    mock_credentials.is_expired.return_value = False
    scp.cache[scp.get_cache_key()] = mock_credentials

    spy = mocker.spy(SamlCredentialsProvider, "refresh")

    assert scp.get_credentials() == mock_credentials
    assert spy.called is False


def test_get_credentials_calls_refresh_when_cache_expired(mocker):
    scp, _ = make_valid_saml_credentials_provider()
    mock_credentials = MagicMock()
    mock_credentials.is_expired.return_value = True
    scp.cache[scp.get_cache_key()] = mock_credentials

    mocker.patch("redshift_connector.plugin.SamlCredentialsProvider.refresh", return_value=None)
    spy = mocker.spy(SamlCredentialsProvider, "refresh")

    scp.get_credentials()

    assert spy.called
    assert spy.call_count == 1


def test_get_credentials_sets_db_user_when_present(mocker):
    scp, _ = make_valid_saml_credentials_provider()
    mocked_db_user: str = "test_db_user"
    scp.db_user = mocked_db_user
    mock_credentials = MagicMock()
    mock_credentials.is_expired.return_value = True
    mock_credentials.metadata = MagicMock()
    scp.cache[scp.get_cache_key()] = mock_credentials

    mocker.patch("redshift_connector.plugin.SamlCredentialsProvider.refresh", return_value=None)
    spy = mocker.spy(mock_credentials.metadata, "set_db_user")

    scp.get_credentials()

    assert spy.called
    assert spy.call_count == 1
    assert spy.call_args[0][0] == mocked_db_user


def test_refresh_get_saml_assertion_fails(mocker):
    scp, _ = make_valid_saml_credentials_provider()

    with patch("redshift_connector.plugin.SamlCredentialsProvider.get_saml_assertion") as mocked_get_saml_assertion:
        mocked_get_saml_assertion.side_effect = Exception("bad robot")

        with pytest.raises(InterfaceError, match="bad robot"):
            scp.refresh()


def test_refresh_saml_assertion_missing_role_should_fail(mocker):
    scp, _ = make_valid_saml_credentials_provider()
    mocked_data: str = "test"
    mocker.patch("redshift_connector.plugin.SamlCredentialsProvider.get_saml_assertion", return_value=mocked_data)

    with pytest.raises(InterfaceError, match="No role found in SamlAssertion"):
        scp.refresh()


def test_refresh_saml_assertion_passed_to_boto(mocker):
    scp, _ = make_valid_saml_credentials_provider()
    mocker.patch(
        "redshift_connector.plugin.SamlCredentialsProvider.get_saml_assertion",
        return_value=base64.b64encode(saml_response_data.saml_response),
    )

    mocked_response: typing.Dict[str, typing.Any] = {
        "Credentials": {"Expiration": "test_expiry", "Things": "much data", "Other Things": "more data"}
    }

    mocked_boto = MagicMock()
    mocked_boto_response = MagicMock()
    mocked_boto_response.return_value = mocked_response
    mocked_boto.assume_role_with_saml = mocked_boto_response

    credential_holder_spy = mocker.spy(CredentialsHolder, "__init__")

    mocker.patch("boto3.client", return_value=mocked_boto)

    scp.refresh()
    assert credential_holder_spy.called
    assert credential_holder_spy.call_count == 1
    assert credential_holder_spy.call_args[0][1] == mocked_response["Credentials"]
