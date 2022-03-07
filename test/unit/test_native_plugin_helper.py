from unittest.mock import MagicMock

from pytest_mock import mocker  # type: ignore

from redshift_connector.iam_helper import IamHelper, IdpAuthHelper
from redshift_connector.native_plugin_helper import NativeAuthPluginHelper


def test_set_native_auth_plugin_properties_gets_idp_token_when_credentials_provider(mocker):
    mocked_idp_token: str = "my_idp_token"
    mocker.patch("redshift_connector.iam_helper.IdpAuthHelper.set_auth_properties", return_value=None)
    mocker.patch(
        "redshift_connector.native_plugin_helper.NativeAuthPluginHelper.get_native_auth_plugin_credentials",
        return_value=mocked_idp_token,
    )
    spy = mocker.spy(NativeAuthPluginHelper, "get_native_auth_plugin_credentials")
    mock_rp: MagicMock = MagicMock()

    NativeAuthPluginHelper.set_native_auth_plugin_properties(mock_rp)

    assert spy.called is True
    assert spy.call_count == 1
    assert spy.call_args[0][0] == mock_rp
    assert mock_rp.method_calls[0][0] == "put"
    assert mock_rp.method_calls[0][1] == ("web_identity_token", mocked_idp_token)
