import pytest

from redshift_connector import IamHelper, InterfaceError, RedshiftProperty
from redshift_connector.plugin import IdpTokenAuthPlugin
from redshift_connector.plugin.native_token_holder import NativeTokenHolder


def test_should_fail_without_token():
    itap: IdpTokenAuthPlugin = IdpTokenAuthPlugin()
    itap.token_type = "blah"

    with pytest.raises(
        InterfaceError, match="IdC authentication failed: The token must be included in the connection parameters."
    ):
        itap.check_required_parameters()


def test_should_fail_without_token_type():
    itap: IdpTokenAuthPlugin = IdpTokenAuthPlugin()
    itap.token = "blah"

    with pytest.raises(
        InterfaceError, match="IdC authentication failed: The token type must be included in the connection parameters."
    ):
        itap.check_required_parameters()


def test_get_auth_token_calls_check_required_parameters(mocker):
    spy = mocker.spy(IdpTokenAuthPlugin, "check_required_parameters")
    itap: IdpTokenAuthPlugin = IdpTokenAuthPlugin()
    itap.token = "my_token"
    itap.token_type = "testing_token"

    itap.get_auth_token()
    assert spy.called
    assert spy.call_count == 1


def test_get_auth_token_returns_token():
    itap: IdpTokenAuthPlugin = IdpTokenAuthPlugin()
    itap.token = "my_token"
    itap.token_type = "testing_token"

    result = itap.get_auth_token()
    assert result == "my_token"


def test_add_parameter_sets_token():
    itap: IdpTokenAuthPlugin = IdpTokenAuthPlugin()
    rp: RedshiftProperty = RedshiftProperty()
    token_value: str = "a token of appreciation"
    rp.token = token_value
    itap.add_parameter(rp)
    assert itap.token == token_value


def test_add_parameter_sets_token_type():
    itap: IdpTokenAuthPlugin = IdpTokenAuthPlugin()
    rp: RedshiftProperty = RedshiftProperty()
    token_type_value: str = "appreciative token"
    rp.token_type = token_type_value
    itap.add_parameter(rp)
    assert itap.token_type == token_type_value


def test_get_sub_type_is_idc():
    itap: IdpTokenAuthPlugin = IdpTokenAuthPlugin()
    assert itap.get_sub_type() == IamHelper.IDC_PLUGIN


def test_cache_disabled_by_default():
    itap: IdpTokenAuthPlugin = IdpTokenAuthPlugin()
    rp: RedshiftProperty = RedshiftProperty()
    rp.token_type = "happy token"
    rp.token = "hello world"
    itap.add_parameter(rp)
    assert itap.disable_cache == True


def test_get_credentials_calls_refresh(mocker):
    itap: IdpTokenAuthPlugin = IdpTokenAuthPlugin()
    rp: RedshiftProperty = RedshiftProperty()
    rp.token_type = "happy token"
    rp.token = "hello world"
    itap.add_parameter(rp)
    mocker.patch("redshift_connector.plugin.IdpTokenAuthPlugin.refresh", return_value=None)
    spy = mocker.spy(IdpTokenAuthPlugin, "refresh")
    itap.get_credentials()
    assert spy.called
    assert spy.call_count == 1


def test_refresh_sets_credential():
    itap: IdpTokenAuthPlugin = IdpTokenAuthPlugin()
    rp: RedshiftProperty = RedshiftProperty()
    rp.token_type = "happy token"
    rp.token = "hello world"
    itap.add_parameter(rp)

    itap.refresh()
    result: NativeTokenHolder = itap.last_refreshed_credentials
    assert result is not None
    assert isinstance(result, NativeTokenHolder)
    assert result.access_token == rp.token


def test_get_credentials_returns_credential():
    itap: IdpTokenAuthPlugin = IdpTokenAuthPlugin()
    rp: RedshiftProperty = RedshiftProperty()
    rp.token_type = "happy token"
    rp.token = "hello world"
    itap.add_parameter(rp)

    result: NativeTokenHolder = itap.get_credentials()
    assert result is not None
    assert isinstance(result, NativeTokenHolder)
    assert result.access_token == rp.token
