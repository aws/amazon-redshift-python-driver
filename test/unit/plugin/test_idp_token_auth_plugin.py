import typing
from unittest.mock import MagicMock

import pytest

from redshift_connector import IamHelper, InterfaceError, RedshiftProperty
from redshift_connector.plugin import IdpTokenAuthPlugin
from redshift_connector.plugin.native_token_holder import NativeTokenHolder


def test_should_fail_without_token():
    """Verify that check_required_parameters fails when only token_type is provided (no valid flow)"""
    itap: IdpTokenAuthPlugin = IdpTokenAuthPlugin()
    itap.token_type = "blah"

    with pytest.raises(
        InterfaceError, match="IdC authentication failed: Either token/token_type or AccessKeyID/SecretAccessKey/SessionToken must be provided."
    ):
        itap.check_required_parameters()


def test_should_fail_without_token_type():
    """Verify that check_required_parameters fails when only token is provided (no valid flow)"""
    itap: IdpTokenAuthPlugin = IdpTokenAuthPlugin()
    itap.token = "blah"

    with pytest.raises(
        InterfaceError, match="IdC authentication failed: Either token/token_type or AccessKeyID/SecretAccessKey/SessionToken must be provided."
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


def test_add_parameter_sets_identity_enhanced_credentials():
    itap: IdpTokenAuthPlugin = IdpTokenAuthPlugin()
    rp: RedshiftProperty = RedshiftProperty()
    rp.access_key_id = "dummy_access_key_id"
    rp.secret_access_key = "dummy_secret_access_key"
    rp.session_token = "dummy_session_token"
    itap.add_parameter(rp)

    assert itap.access_key_id == rp.access_key_id
    assert itap.secret_access_key == rp.secret_access_key
    assert itap.session_token == rp.session_token


def test_add_parameter_stores_redshift_property_reference():
    """Verify that add_parameter stores a reference to the RedshiftProperty object"""
    itap: IdpTokenAuthPlugin = IdpTokenAuthPlugin()
    rp: RedshiftProperty = RedshiftProperty()
    rp.host = "cluster.abc123.us-east-1.redshift.amazonaws.com"
    itap.add_parameter(rp)

    assert itap.redshift_property is rp


def test_init_redshift_property_is_none():
    """Verify that redshift_property is None after __init__()"""
    itap: IdpTokenAuthPlugin = IdpTokenAuthPlugin()
    
    assert itap.redshift_property is None


def test_add_parameter_sets_optional_parameters():
    itap: IdpTokenAuthPlugin = IdpTokenAuthPlugin()
    rp: RedshiftProperty = RedshiftProperty()
    rp.host = "cluster.abc123.us-east-1.redshift.amazonaws.com"
    rp.endpoint_url = "https://redshift.us-east-1.amazonaws.com"
    rp.region = "us-east-1"
    itap.add_parameter(rp)

    assert itap.redshift_property == rp
    assert itap.endpoint_url == rp.endpoint_url
    assert itap.region == rp.region


def test_add_parameter_maintains_backward_compatibility():
    """Verify that direct token flow still works when identity-enhanced credentials are not provided"""
    itap: IdpTokenAuthPlugin = IdpTokenAuthPlugin()
    rp: RedshiftProperty = RedshiftProperty()
    rp.token = "my_token"
    rp.token_type = "ACCESS_TOKEN"
    itap.add_parameter(rp)

    assert itap.token == "my_token"
    assert itap.token_type == "ACCESS_TOKEN"
    assert itap.access_key_id is None
    assert itap.secret_access_key is None
    assert itap.session_token is None


def test_check_required_parameters_with_identity_enhanced_credentials():
    """Verify that check_required_parameters accepts valid identity-enhanced credentials"""
    itap: IdpTokenAuthPlugin = IdpTokenAuthPlugin()
    itap.access_key_id = "dummy_access_key_id"
    itap.secret_access_key = "dummy_secret_access_key"
    itap.session_token = "dummy_session_token"
      
    # Should not raise an error
    itap.check_required_parameters()


def test_check_required_parameters_rejects_incomplete_identity_enhanced_credentials():
    """Verify that check_required_parameters rejects incomplete identity-enhanced credentials"""
    itap: IdpTokenAuthPlugin = IdpTokenAuthPlugin()
    itap.access_key_id = "dummy_access_key_id"
    itap.secret_access_key = "dummy_secret_access_key"
    # Missing session_token - partial IAM
    
    with pytest.raises(
        InterfaceError, match=r"IdC authentication failed: Either token/token_type or AccessKeyID/SecretAccessKey/SessionToken must be provided."
    ):
        itap.check_required_parameters()


def test_check_required_parameters_accepts_identity_enhanced_without_host():
    """Verify that check_required_parameters accepts identity-enhanced credentials without Host
    (host validation is now handled by RedshiftProperty, not the plugin)"""
    itap: IdpTokenAuthPlugin = IdpTokenAuthPlugin()
    itap.access_key_id = "dummy_access_key_id"
    itap.secret_access_key = "dummy_secret_access_key"
    itap.session_token = "dummy_session_token"
    # Missing host - should not raise error since host validation is handled elsewhere
    
    # Should not raise an error
    itap.check_required_parameters()


def test_check_required_parameters_rejects_conflicting_parameters():
    """Verify that check_required_parameters rejects both direct token and identity-enhanced parameters"""
    itap: IdpTokenAuthPlugin = IdpTokenAuthPlugin()
    # Direct token parameters
    itap.token = "my_token"
    itap.token_type = "ACCESS_TOKEN"
    # Identity-enhanced credentials parameters
    itap.access_key_id = "dummy_access_key_id"
    itap.secret_access_key = "dummy_secret_access_key"
    itap.session_token = "dummy_session_token"
    
    with pytest.raises(
        InterfaceError, match=r"IdC authentication failed: Cannot provide both direct token parameters \(token, token_type\) and \(AccessKeyID, SecretAccessKey, SessionToken\)."
    ):
        itap.check_required_parameters()


def test_check_required_parameters_accepts_no_parameters():
    """Verify that check_required_parameters passes when no parameters are provided (default credentials flow)"""
    itap: IdpTokenAuthPlugin = IdpTokenAuthPlugin()
    
    # Should not raise - falls through to default credentials flow
    itap.check_required_parameters()


def test_is_using_identity_enhanced_credentials_returns_true_with_credentials():
    """Verify that _is_using_identity_enhanced_credentials returns True when credentials are provided"""
    itap: IdpTokenAuthPlugin = IdpTokenAuthPlugin()
    itap.access_key_id = "dummy_access_key_id"
    itap.secret_access_key = "dummy_secret_access_key"
    itap.session_token = "dummy_session_token"
    
    assert itap._is_using_identity_enhanced_credentials() is True


def test_is_using_identity_enhanced_credentials_returns_false_with_partial_credentials():
    """Verify that _is_using_identity_enhanced_credentials returns False with partial credentials (only access_key_id)"""
    itap: IdpTokenAuthPlugin = IdpTokenAuthPlugin()
    itap.access_key_id = "dummy_access_key_id"
    # Missing secret_access_key and session_token
    
    assert itap._is_using_identity_enhanced_credentials() is False


def test_is_using_identity_enhanced_credentials_returns_false_with_two_credentials():
    """Verify that _is_using_identity_enhanced_credentials returns False with only two credentials"""
    itap: IdpTokenAuthPlugin = IdpTokenAuthPlugin()
    itap.access_key_id = "dummy_access_key_id"
    itap.secret_access_key = "dummy_secret_access_key"
    # Missing session_token
    
    assert itap._is_using_identity_enhanced_credentials() is False


def test_is_using_identity_enhanced_credentials_returns_false_without_credentials():
    """Verify that _is_using_identity_enhanced_credentials returns False when no credentials are provided"""
    itap: IdpTokenAuthPlugin = IdpTokenAuthPlugin()
    
    assert itap._is_using_identity_enhanced_credentials() is False


def test_is_using_identity_enhanced_credentials_returns_false_with_direct_token():
    """Verify that _is_using_identity_enhanced_credentials returns False with direct token flow"""
    itap: IdpTokenAuthPlugin = IdpTokenAuthPlugin()
    itap.token = "my_token"
    itap.token_type = "ACCESS_TOKEN"
    
    assert itap._is_using_identity_enhanced_credentials() is False


def test_create_aws_credentials_with_valid_parameters():
    """Verify that _create_aws_credentials creates valid credentials"""
    itap: IdpTokenAuthPlugin = IdpTokenAuthPlugin()
    itap.access_key_id = "dummy_access_key_id"
    itap.secret_access_key = "dummy_secret_access_key"
    itap.session_token = "dummy_session_token"
    
    credentials = itap._create_aws_credentials()
    
    assert credentials is not None
    assert credentials.access_key == "dummy_access_key_id"
    assert credentials.secret_key == "dummy_secret_access_key"
    assert credentials.token == "dummy_session_token"


def test_get_subject_token_provisioned_cluster(mocker):
    """Verify that _get_subject_token successfully handles provisioned cluster flow using RedshiftProperty"""
    itap: IdpTokenAuthPlugin = IdpTokenAuthPlugin()
    rp: RedshiftProperty = RedshiftProperty()
    rp.access_key_id = "dummy_access_key_id"
    rp.secret_access_key = "dummy_secret_access_key"
    rp.session_token = "dummy_session_token"
    rp.region = "us-east-1"
    rp.cluster_identifier = "my-cluster"
    rp.is_serverless = False
    itap.add_parameter(rp)
    
    # Mock the _get_provisioned_auth_token method
    mocker.patch.object(
        itap,
        "_get_provisioned_auth_token",
        return_value="provisioned_subject_token"
    )
    
    # Call the method
    token = itap._get_subject_token()
    
    assert token == "provisioned_subject_token"
    itap._get_provisioned_auth_token.assert_called_once()


def test_get_subject_token_serverless_workgroup(mocker):
    """Verify that _get_subject_token successfully handles serverless workgroup flow using RedshiftProperty"""
    itap: IdpTokenAuthPlugin = IdpTokenAuthPlugin()
    rp: RedshiftProperty = RedshiftProperty()
    rp.access_key_id = "dummy_access_key_id"
    rp.secret_access_key = "dummy_secret_access_key"
    rp.session_token = "dummy_session_token"
    rp.region = "us-west-2"
    rp.serverless_work_group = "default"
    rp.is_serverless = True
    itap.add_parameter(rp)
    
    # Mock the _get_serverless_auth_token method
    mocker.patch.object(
        itap,
        "_get_serverless_auth_token",
        return_value="serverless_subject_token"
    )
    
    # Call the method
    token = itap._get_subject_token()
    
    assert token == "serverless_subject_token"
    itap._get_serverless_auth_token.assert_called_once()


def test_get_subject_token_with_region_override(mocker):
    """Verify that _get_subject_token uses explicit region override over RedshiftProperty.region"""
    itap: IdpTokenAuthPlugin = IdpTokenAuthPlugin()
    rp: RedshiftProperty = RedshiftProperty()
    rp.access_key_id = "dummy_access_key_id"
    rp.secret_access_key = "dummy_secret_access_key"
    rp.session_token = "dummy_session_token"
    rp.region = "us-east-1"  # RedshiftProperty region
    rp.cluster_identifier = "my-cluster"
    rp.is_serverless = False
    itap.add_parameter(rp)
    itap.region = "eu-west-1"  # Explicit region override
    
    # Mock the _get_provisioned_auth_token method
    mock_get_token = mocker.patch.object(
        itap,
        "_get_provisioned_auth_token",
        return_value="provisioned_subject_token"
    )
    
    # Call the method
    token = itap._get_subject_token()
    
    assert token == "provisioned_subject_token"
    # Verify that the explicit region override was used
    call_args = mock_get_token.call_args
    assert call_args[1]["region"] == "eu-west-1"


def test_get_subject_token_uses_redshift_property_is_serverless(mocker):
    """Verify that _get_subject_token uses _is_serverless from RedshiftProperty to determine cluster type"""
    itap: IdpTokenAuthPlugin = IdpTokenAuthPlugin()
    rp: RedshiftProperty = RedshiftProperty()
    rp.access_key_id = "dummy_access_key_id"
    rp.secret_access_key = "dummy_secret_access_key"
    rp.session_token = "dummy_session_token"
    rp.region = "us-west-2"
    rp.serverless_work_group = "my-workgroup"
    rp.is_serverless = True  # This should trigger serverless flow
    itap.add_parameter(rp)
    
    # Mock the _get_serverless_auth_token method
    mock_serverless = mocker.patch.object(
        itap,
        "_get_serverless_auth_token",
        return_value="serverless_token"
    )
    mock_provisioned = mocker.patch.object(
        itap,
        "_get_provisioned_auth_token",
        return_value="provisioned_token"
    )
    
    # Call the method
    token = itap._get_subject_token()
    
    # Verify serverless flow was used
    assert token == "serverless_token"
    mock_serverless.assert_called_once()
    mock_provisioned.assert_not_called()


def test_get_subject_token_provisioned_uses_cluster_identifier(mocker):
    """Verify that _get_subject_token uses cluster_identifier from RedshiftProperty for provisioned clusters"""
    itap: IdpTokenAuthPlugin = IdpTokenAuthPlugin()
    rp: RedshiftProperty = RedshiftProperty()
    rp.access_key_id = "dummy_access_key_id"
    rp.secret_access_key = "dummy_secret_access_key"
    rp.session_token = "dummy_session_token"
    rp.region = "us-east-1"
    rp.cluster_identifier = "my-specific-cluster"
    rp.is_serverless = False
    itap.add_parameter(rp)
    
    # Mock the _get_provisioned_auth_token method
    mock_get_token = mocker.patch.object(
        itap,
        "_get_provisioned_auth_token",
        return_value="provisioned_subject_token"
    )
    
    # Call the method
    token = itap._get_subject_token()
    
    assert token == "provisioned_subject_token"
    # Verify that cluster_identifier from RedshiftProperty was used
    call_args = mock_get_token.call_args
    assert call_args[1]["cluster_id"] == "my-specific-cluster"


def test_get_subject_token_serverless_uses_serverless_work_group(mocker):
    """Verify that _get_subject_token uses serverless_work_group from RedshiftProperty for serverless clusters"""
    itap: IdpTokenAuthPlugin = IdpTokenAuthPlugin()
    rp: RedshiftProperty = RedshiftProperty()
    rp.access_key_id = "dummy_access_key_id"
    rp.secret_access_key = "dummy_secret_access_key"
    rp.session_token = "dummy_session_token"
    rp.region = "us-west-2"
    rp.serverless_work_group = "my-specific-workgroup"
    rp.is_serverless = True
    itap.add_parameter(rp)
    
    # Mock the _get_serverless_auth_token method
    mock_get_token = mocker.patch.object(
        itap,
        "_get_serverless_auth_token",
        return_value="serverless_subject_token"
    )
    
    # Call the method
    token = itap._get_subject_token()
    
    assert token == "serverless_subject_token"
    # Verify that serverless_work_group from RedshiftProperty was used
    call_args = mock_get_token.call_args
    assert call_args[1]["workgroup_id"] == "my-specific-workgroup"


def test_get_subject_token_explicit_region_overrides_redshift_property(mocker):
    """Verify that explicit region parameter takes precedence over RedshiftProperty.region"""
    itap: IdpTokenAuthPlugin = IdpTokenAuthPlugin()
    rp: RedshiftProperty = RedshiftProperty()
    rp.access_key_id = "dummy_access_key_id"
    rp.secret_access_key = "dummy_secret_access_key"
    rp.session_token = "dummy_session_token"
    rp.region = "us-east-1"  # RedshiftProperty region
    rp.cluster_identifier = "my-cluster"
    rp.is_serverless = False
    itap.add_parameter(rp)
    itap.region = "ap-southeast-1"  # Explicit override
    
    # Mock the _get_provisioned_auth_token method
    mock_get_token = mocker.patch.object(
        itap,
        "_get_provisioned_auth_token",
        return_value="provisioned_subject_token"
    )
    
    # Call the method
    itap._get_subject_token()
    
    # Verify that explicit region was used
    call_args = mock_get_token.call_args
    assert call_args[1]["region"] == "ap-southeast-1"


def test_get_subject_token_falls_back_to_redshift_property_region(mocker):
    """Verify that RedshiftProperty.region is used when no explicit region is provided"""
    itap: IdpTokenAuthPlugin = IdpTokenAuthPlugin()
    rp: RedshiftProperty = RedshiftProperty()
    rp.access_key_id = "dummy_access_key_id"
    rp.secret_access_key = "dummy_secret_access_key"
    rp.session_token = "dummy_session_token"
    rp.region = "eu-central-1"  # RedshiftProperty region
    rp.cluster_identifier = "my-cluster"
    rp.is_serverless = False
    itap.add_parameter(rp)
    # No explicit region set (itap.region is None)
    
    # Mock the _get_provisioned_auth_token method
    mock_get_token = mocker.patch.object(
        itap,
        "_get_provisioned_auth_token",
        return_value="provisioned_subject_token"
    )
    
    # Call the method
    itap._get_subject_token()
    
    # Verify that RedshiftProperty.region was used
    call_args = mock_get_token.call_args
    assert call_args[1]["region"] == "eu-central-1"


def test_get_subject_token_raises_error_when_region_unavailable():
    """Verify that _get_subject_token raises InterfaceError when neither explicit region nor RedshiftProperty.region is available"""
    itap: IdpTokenAuthPlugin = IdpTokenAuthPlugin()
    rp: RedshiftProperty = RedshiftProperty()
    rp.access_key_id = "dummy_access_key_id"
    rp.secret_access_key = "dummy_secret_access_key"
    rp.session_token = "dummy_session_token"
    rp.region = None  # No region in RedshiftProperty
    rp.cluster_identifier = "my-cluster"
    rp.is_serverless = False
    itap.add_parameter(rp)
    # No explicit region set (itap.region is None)
    
    with pytest.raises(
        InterfaceError, match="Region must be provided or resolvable from hostname"
    ):
        itap._get_subject_token()


def test_get_subject_token_raises_error_when_cluster_identifier_missing():
    """Verify that _get_subject_token raises InterfaceError when cluster_identifier is missing for provisioned cluster"""
    itap: IdpTokenAuthPlugin = IdpTokenAuthPlugin()
    rp: RedshiftProperty = RedshiftProperty()
    rp.access_key_id = "dummy_access_key_id"
    rp.secret_access_key = "dummy_secret_access_key"
    rp.session_token = "dummy_session_token"
    rp.region = "us-east-1"
    rp.cluster_identifier = None  # Missing cluster_identifier
    rp.is_serverless = False
    itap.add_parameter(rp)
    
    with pytest.raises(
        InterfaceError, match="Cluster identifier must be provided"
    ):
        itap._get_subject_token()


def test_get_subject_token_raises_error_when_serverless_work_group_missing():
    """Verify that _get_subject_token raises InterfaceError when serverless_work_group is missing for serverless cluster"""
    itap: IdpTokenAuthPlugin = IdpTokenAuthPlugin()
    rp: RedshiftProperty = RedshiftProperty()
    rp.access_key_id = "dummy_access_key_id"
    rp.secret_access_key = "dummy_secret_access_key"
    rp.session_token = "dummy_session_token"
    rp.region = "us-west-2"
    rp.serverless_work_group = None  # Missing serverless_work_group
    rp.is_serverless = True
    itap.add_parameter(rp)
    
    with pytest.raises(
        InterfaceError, match="Serverless workgroup must be provided or resolvable from hostname"
    ):
        itap._get_subject_token()


def test_get_auth_token_with_identity_enhanced_credentials(mocker):
    """Verify that get_auth_token uses identity-enhanced flow when credentials are provided"""
    itap: IdpTokenAuthPlugin = IdpTokenAuthPlugin()
    rp: RedshiftProperty = RedshiftProperty()
    rp.access_key_id = "dummy_access_key_id"
    rp.secret_access_key = "dummy_secret_access_key"
    rp.session_token = "dummy_session_token"
    rp.region = "us-east-1"
    rp.cluster_identifier = "my-cluster"
    rp.is_serverless = False
    itap.add_parameter(rp)
    
    # Mock the _get_subject_token method
    mocker.patch.object(
        itap,
        "_get_subject_token",
        return_value="subject_token_from_api"
    )
    
    # Call get_auth_token
    token = itap.get_auth_token()
    
    assert token == "subject_token_from_api"
    itap._get_subject_token.assert_called_once()


def test_get_auth_token_with_direct_token(mocker):
    """Verify that get_auth_token uses direct token flow when token is provided"""
    itap: IdpTokenAuthPlugin = IdpTokenAuthPlugin()
    itap.token = "my_direct_token"
    itap.token_type = "ACCESS_TOKEN"
    
    # Mock the _get_subject_token method to ensure it's not called
    mocker.patch.object(
        itap,
        "_get_subject_token",
        return_value="should_not_be_called"
    )
    
    # Call get_auth_token
    token = itap.get_auth_token()
    
    assert token == "my_direct_token"
    # Verify that _get_subject_token was not called
    itap._get_subject_token.assert_not_called()


def test_get_auth_token_calls_check_required_parameters_with_identity_enhanced(mocker):
    """Verify that get_auth_token calls check_required_parameters for identity-enhanced flow"""
    itap: IdpTokenAuthPlugin = IdpTokenAuthPlugin()
    rp: RedshiftProperty = RedshiftProperty()
    rp.access_key_id = "dummy_access_key_id"
    rp.secret_access_key = "dummy_secret_access_key"
    rp.session_token = "dummy_session_token"
    rp.region = "us-east-1"
    rp.cluster_identifier = "my-cluster"
    rp.is_serverless = False
    itap.add_parameter(rp)
    
    # Mock the _get_subject_token method
    mocker.patch.object(
        itap,
        "_get_subject_token",
        return_value="subject_token"
    )
    
    # Spy on check_required_parameters
    spy = mocker.spy(IdpTokenAuthPlugin, "check_required_parameters")
    
    # Call get_auth_token
    itap.get_auth_token()
    
    # Verify that check_required_parameters was called
    assert spy.called
# Default Credentials Fallback Tests

def _make_plugin(**kwargs) -> IdpTokenAuthPlugin:
    """Create an IdpTokenAuthPlugin with specified attributes."""
    plugin = IdpTokenAuthPlugin()
    for k, v in kwargs.items():
        setattr(plugin, k, v)
    return plugin


def _make_plugin_with_rp(**rp_kwargs) -> IdpTokenAuthPlugin:
    """Create an IdpTokenAuthPlugin with a RedshiftProperty configured."""
    plugin = IdpTokenAuthPlugin()
    rp = RedshiftProperty()
    for k, v in rp_kwargs.items():
        setattr(rp, k, v)
    plugin.add_parameter(rp)
    return plugin


class TestValidCombinations:
    def test_valid_direct_token_flow(self):
        plugin = _make_plugin(token="my_token", token_type="ACCESS_TOKEN")
        assert plugin.get_auth_token() == "my_token"

    def test_valid_identity_enhanced_flow(self):
        plugin = _make_plugin_with_rp(
            access_key_id="AKIA123", secret_access_key="secret", session_token="sesstoken",
            host="mycluster.abc123.us-east-1.redshift.amazonaws.com",
            region="us-east-1", cluster_identifier="mycluster", is_serverless=False,
        )
        plugin.check_required_parameters()

    def test_valid_default_credentials_flow(self):
        plugin = _make_plugin_with_rp(
            host="mycluster.abc123.us-east-1.redshift.amazonaws.com")
        plugin.check_required_parameters()


class TestPartialTokenValidation:
    def test_token_without_token_type_raises(self):
        plugin = _make_plugin(token="my_token")
        with pytest.raises(InterfaceError, match="IdC authentication failed: Either token/token_type or AccessKeyID/SecretAccessKey/SessionToken must be provided."):
            plugin.check_required_parameters()

    def test_token_type_without_token_raises(self):
        plugin = _make_plugin(token_type="ACCESS_TOKEN")
        with pytest.raises(InterfaceError, match="IdC authentication failed: Either token/token_type or AccessKeyID/SecretAccessKey/SessionToken must be provided."):
            plugin.check_required_parameters()


class TestPartialAndConflictingCredentials:
    def test_partial_iam_access_key_only(self):
        plugin = _make_plugin(access_key_id="AKIA123")
        with pytest.raises(InterfaceError, match=r"IdC authentication failed: Either token/token_type or AccessKeyID/SecretAccessKey/SessionToken must be provided."):
            plugin.check_required_parameters()

    def test_partial_iam_access_key_and_secret(self):
        plugin = _make_plugin(access_key_id="AKIA123",
                              secret_access_key="secret")
        with pytest.raises(InterfaceError, match=r"IdC authentication failed: Either token/token_type or AccessKeyID/SecretAccessKey/SessionToken must be provided."):
            plugin.check_required_parameters()

    def test_partial_iam_access_key_and_session_token(self):
        plugin = _make_plugin(access_key_id="AKIA123", session_token="token")
        with pytest.raises(InterfaceError, match=r"IdC authentication failed: Either token/token_type or AccessKeyID/SecretAccessKey/SessionToken must be provided."):
            plugin.check_required_parameters()

    def test_partial_iam_secret_and_session_token(self):
        plugin = _make_plugin(secret_access_key="secret",
                              session_token="token")
        with pytest.raises(InterfaceError, match=r"IdC authentication failed: Either token/token_type or AccessKeyID/SecretAccessKey/SessionToken must be provided."):
            plugin.check_required_parameters()

    def test_conflicting_complete_token_and_complete_iam(self):
        plugin = _make_plugin(
            token="tok", token_type="ACCESS_TOKEN",
            access_key_id="AKIA", secret_access_key="secret", session_token="sess",
        )
        with pytest.raises(InterfaceError, match=r"IdC authentication failed: Cannot provide both direct token parameters \(token, token_type\) and \(AccessKeyID, SecretAccessKey, SessionToken\)."):
            plugin.check_required_parameters()

    def test_conflicting_complete_token_and_partial_iam(self):
        """Complete token + partial IAM -> direct token flow is fully valid, passes validation.
        The extra partial IAM param is ignored at runtime."""
        plugin = _make_plugin(
            token="tok", token_type="ACCESS_TOKEN", access_key_id="AKIA")
        # Should not raise - direct token flow is complete
        plugin.check_required_parameters()

    def test_partial_token_and_partial_iam_raises(self):
        """Partial token + partial IAM -> neither flow is fully valid, raises 'either X or Y'."""
        plugin = _make_plugin(token="tok", access_key_id="AKIA")
        with pytest.raises(InterfaceError, match=r"IdC authentication failed: Either token/token_type or AccessKeyID/SecretAccessKey/SessionToken must be provided."):
            plugin.check_required_parameters()

    def test_iam_credentials_without_host_raises_at_auth_time(self):
        plugin = _make_plugin(access_key_id="AKIA",
                              secret_access_key="secret", session_token="sess")
        rp = RedshiftProperty()
        rp.host = ""
        plugin.redshift_property = rp
        with pytest.raises(InterfaceError):
            plugin.get_auth_token()


class TestIsUsingDefaultCredentials:
    def test_true_when_only_host_set(self):
        plugin = _make_plugin_with_rp(
            host="mycluster.abc123.us-east-1.redshift.amazonaws.com")
        assert plugin.is_using_default_credentials() is True

    def test_true_when_no_params(self):
        assert IdpTokenAuthPlugin().is_using_default_credentials() is True

    def test_false_when_token_params_present(self):
        plugin = _make_plugin(token="tok", token_type="ACCESS_TOKEN")
        assert plugin.is_using_default_credentials() is False

    def test_false_when_iam_params_present(self):
        plugin = _make_plugin(access_key_id="AKIA",
                              secret_access_key="secret", session_token="sess")
        assert plugin.is_using_default_credentials() is False


class TestIsUsingIdentityEnhancedCredentialsDefault:
    def test_true_when_all_three_iam_provided(self):
        plugin = _make_plugin(access_key_id="AKIA",
                              secret_access_key="secret", session_token="sess")
        assert plugin._is_using_identity_enhanced_credentials() is True

    def test_false_when_access_key_missing(self):
        plugin = _make_plugin(secret_access_key="secret", session_token="sess")
        assert plugin._is_using_identity_enhanced_credentials() is False

    def test_false_when_secret_missing(self):
        plugin = _make_plugin(access_key_id="AKIA", session_token="sess")
        assert plugin._is_using_identity_enhanced_credentials() is False

    def test_false_when_session_token_missing(self):
        plugin = _make_plugin(access_key_id="AKIA", secret_access_key="secret")
        assert plugin._is_using_identity_enhanced_credentials() is False

    def test_false_with_direct_token(self):
        plugin = _make_plugin(token="tok", token_type="ACCESS_TOKEN")
        assert plugin._is_using_identity_enhanced_credentials() is False

    def test_false_with_no_params(self):
        assert IdpTokenAuthPlugin()._is_using_identity_enhanced_credentials() is False


class TestRegionAndIdentifierResolution:
    def test_provisioned_hostname_resolves_cluster_and_region(self, mocker):
        plugin = _make_plugin_with_rp(
            host="mycluster.abc123.us-east-1.redshift.amazonaws.com",
            region="us-east-1", cluster_identifier="mycluster", is_serverless=False,
        )
        mock_session = MagicMock()
        mock_client = MagicMock()
        mock_client.get_identity_center_auth_token.return_value = {
            "Token": "subject_tok"}
        mock_session.client.return_value = mock_client
        mocker.patch.object(
            plugin, "_create_default_credentials_provider", return_value=mock_session)
        assert plugin._get_default_credentials_auth_token() == "subject_tok"
        mock_session.client.assert_called_once_with(
            "redshift", region_name="us-east-1", endpoint_url=None)

    def test_serverless_hostname_resolves_workgroup_and_region(self, mocker):
        plugin = _make_plugin_with_rp(
            host="default.123456789012.us-west-2.redshift-serverless.amazonaws.com",
            region="us-west-2", serverless_work_group="default", is_serverless=True,
        )
        mock_session = MagicMock()
        mock_client = MagicMock()
        mock_client.get_identity_center_auth_token.return_value = {
            "token": "serverless_tok"}
        mock_session.client.return_value = mock_client
        mocker.patch.object(
            plugin, "_create_default_credentials_provider", return_value=mock_session)
        assert plugin._get_default_credentials_auth_token() == "serverless_tok"
        mock_session.client.assert_called_once_with(
            "redshift-serverless", region_name="us-west-2", endpoint_url=None)

    def test_explicit_region_overrides_hostname_region(self, mocker):
        plugin = _make_plugin_with_rp(
            host="mycluster.abc123.us-east-1.redshift.amazonaws.com",
            region="us-east-1", cluster_identifier="mycluster", is_serverless=False,
        )
        plugin.region = "eu-west-1"
        mock_session = MagicMock()
        mock_client = MagicMock()
        mock_client.get_identity_center_auth_token.return_value = {
            "Token": "tok"}
        mock_session.client.return_value = mock_client
        mocker.patch.object(
            plugin, "_create_default_credentials_provider", return_value=mock_session)
        plugin._get_default_credentials_auth_token()
        mock_session.client.assert_called_once_with(
            "redshift", region_name="eu-west-1", endpoint_url=None)

    def test_missing_cluster_identifier_raises(self):
        plugin = _make_plugin_with_rp(
            host="mycluster.abc123.us-east-1.redshift.amazonaws.com",
            region="us-east-1", cluster_identifier=None, is_serverless=False,
        )
        with pytest.raises(InterfaceError, match="Unable to determine cluster identifier"):
            plugin._get_default_credentials_auth_token()

    def test_missing_region_raises(self):
        plugin = _make_plugin_with_rp(
            host="mycluster.abc123.us-east-1.redshift.amazonaws.com",
            region=None, cluster_identifier="mycluster", is_serverless=False,
        )
        plugin.region = None
        with pytest.raises(InterfaceError, match="Unable to determine AWS region"):
            plugin._get_default_credentials_auth_token()


class TestProvisionedAuthTokenDefault:
    def test_success_returns_token(self):
        plugin = IdpTokenAuthPlugin()
        mock_session = MagicMock()
        mock_client = MagicMock()
        mock_client.get_identity_center_auth_token.return_value = {
            "Token": "provisioned_tok"}
        mock_session.client.return_value = mock_client
        plugin._default_credentials_provider = mock_session
        assert plugin._get_provisioned_auth_token_default(
            "us-east-1", "mycluster") == "provisioned_tok"
        mock_client.get_identity_center_auth_token.assert_called_once_with(ClusterIds=[
                                                                           "mycluster"])

    def test_api_failure_raises_interface_error(self):
        plugin = IdpTokenAuthPlugin()
        mock_session = MagicMock()
        mock_client = MagicMock()
        mock_client.get_identity_center_auth_token.side_effect = Exception(
            "API timeout")
        mock_session.client.return_value = mock_client
        plugin._default_credentials_provider = mock_session
        with pytest.raises(InterfaceError, match="Failed to obtain subject token.*provisioned cluster.*API timeout"):
            plugin._get_provisioned_auth_token_default(
                "us-east-1", "mycluster")

    def test_endpoint_url_passed_to_client(self):
        plugin = IdpTokenAuthPlugin()
        plugin.endpoint_url = "https://custom-endpoint.example.com"
        mock_session = MagicMock()
        mock_client = MagicMock()
        mock_client.get_identity_center_auth_token.return_value = {
            "Token": "tok"}
        mock_session.client.return_value = mock_client
        plugin._default_credentials_provider = mock_session
        plugin._get_provisioned_auth_token_default("us-east-1", "mycluster")
        mock_session.client.assert_called_once_with(
            "redshift", region_name="us-east-1", endpoint_url="https://custom-endpoint.example.com",
        )


class TestServerlessAuthTokenDefault:
    def test_success_returns_token(self):
        plugin = IdpTokenAuthPlugin()
        mock_session = MagicMock()
        mock_client = MagicMock()
        mock_client.get_identity_center_auth_token.return_value = {
            "token": "serverless_tok"}
        mock_session.client.return_value = mock_client
        plugin._default_credentials_provider = mock_session
        assert plugin._get_serverless_auth_token_default(
            "us-west-2", "my-workgroup") == "serverless_tok"
        mock_client.get_identity_center_auth_token.assert_called_once_with(
            workgroupNames=["my-workgroup"])

    def test_api_failure_raises_interface_error(self):
        plugin = IdpTokenAuthPlugin()
        mock_session = MagicMock()
        mock_client = MagicMock()
        mock_client.get_identity_center_auth_token.side_effect = Exception(
            "Connection refused")
        mock_session.client.return_value = mock_client
        plugin._default_credentials_provider = mock_session
        with pytest.raises(InterfaceError, match="Failed to obtain subject token.*serverless workgroup.*Connection refused"):
            plugin._get_serverless_auth_token_default(
                "us-west-2", "my-workgroup")

    def test_endpoint_url_passed_to_client(self):
        plugin = IdpTokenAuthPlugin()
        plugin.endpoint_url = "https://custom-serverless.example.com"
        mock_session = MagicMock()
        mock_client = MagicMock()
        mock_client.get_identity_center_auth_token.return_value = {
            "token": "tok"}
        mock_session.client.return_value = mock_client
        plugin._default_credentials_provider = mock_session
        plugin._get_serverless_auth_token_default("us-west-2", "wg")
        mock_session.client.assert_called_once_with(
            "redshift-serverless", region_name="us-west-2", endpoint_url="https://custom-serverless.example.com",
        )


class TestDefaultCredentialsOrchestration:
    def test_provisioned_end_to_end(self, mocker):
        plugin = _make_plugin_with_rp(
            host="mycluster.abc123.us-east-1.redshift.amazonaws.com",
            region="us-east-1", cluster_identifier="mycluster", is_serverless=False,
        )
        mock_session = MagicMock()
        mock_client = MagicMock()
        mock_client.get_identity_center_auth_token.return_value = {
            "Token": "e2e_tok"}
        mock_session.client.return_value = mock_client
        mocker.patch.object(
            plugin, "_create_default_credentials_provider", return_value=mock_session)
        assert plugin._get_default_credentials_auth_token() == "e2e_tok"

    def test_serverless_end_to_end(self, mocker):
        plugin = _make_plugin_with_rp(
            host="default.123456789012.us-west-2.redshift-serverless.amazonaws.com",
            region="us-west-2", serverless_work_group="default", is_serverless=True,
        )
        mock_session = MagicMock()
        mock_client = MagicMock()
        mock_client.get_identity_center_auth_token.return_value = {
            "token": "serverless_e2e"}
        mock_session.client.return_value = mock_client
        mocker.patch.object(
            plugin, "_create_default_credentials_provider", return_value=mock_session)
        assert plugin._get_default_credentials_auth_token() == "serverless_e2e"

    def test_empty_token_provisioned_raises(self, mocker):
        plugin = _make_plugin_with_rp(
            host="mycluster.abc123.us-east-1.redshift.amazonaws.com",
            region="us-east-1", cluster_identifier="mycluster", is_serverless=False,
        )
        mock_session = MagicMock()
        mock_client = MagicMock()
        mock_client.get_identity_center_auth_token.return_value = {"Token": ""}
        mock_session.client.return_value = mock_client
        mocker.patch.object(
            plugin, "_create_default_credentials_provider", return_value=mock_session)
        with pytest.raises(InterfaceError, match="empty response for provisioned cluster"):
            plugin._get_default_credentials_auth_token()

    def test_empty_token_serverless_raises(self, mocker):
        plugin = _make_plugin_with_rp(
            host="default.123456789012.us-west-2.redshift-serverless.amazonaws.com",
            region="us-west-2", serverless_work_group="default", is_serverless=True,
        )
        mock_session = MagicMock()
        mock_client = MagicMock()
        mock_client.get_identity_center_auth_token.return_value = {"token": ""}
        mock_session.client.return_value = mock_client
        mocker.patch.object(
            plugin, "_create_default_credentials_provider", return_value=mock_session)
        with pytest.raises(InterfaceError, match="empty response for serverless workgroup"):
            plugin._get_default_credentials_auth_token()

    def test_cluster_resolution_failure_raises(self):
        plugin = _make_plugin_with_rp(
            host="mycluster.abc123.us-east-1.redshift.amazonaws.com",
            region="us-east-1", cluster_identifier=None, is_serverless=False,
        )
        with pytest.raises(InterfaceError, match="Unable to determine cluster identifier"):
            plugin._get_default_credentials_auth_token()


class TestDefaultCredentialsProviderSpecific:
    def test_is_using_default_true_only_host(self):
        plugin = _make_plugin_with_rp(
            host="mycluster.abc123.us-east-1.redshift.amazonaws.com")
        assert plugin.is_using_default_credentials() is True

    def test_is_using_default_true_no_params(self):
        assert IdpTokenAuthPlugin().is_using_default_credentials() is True

    def test_provisioned_success_with_mock_provider(self, mocker):
        plugin = _make_plugin_with_rp(
            host="mycluster.abc123.us-east-1.redshift.amazonaws.com",
            region="us-east-1", cluster_identifier="mycluster", is_serverless=False,
        )
        mock_session = MagicMock()
        mock_client = MagicMock()
        mock_client.get_identity_center_auth_token.return_value = {
            "Token": "prov_tok"}
        mock_session.client.return_value = mock_client
        mocker.patch.object(
            plugin, "_create_default_credentials_provider", return_value=mock_session)
        assert plugin.get_auth_token() == "prov_tok"

    def test_serverless_success_with_mock_provider(self, mocker):
        plugin = _make_plugin_with_rp(
            host="default.123456789012.us-west-2.redshift-serverless.amazonaws.com",
            region="us-west-2", serverless_work_group="default", is_serverless=True,
        )
        mock_session = MagicMock()
        mock_client = MagicMock()
        mock_client.get_identity_center_auth_token.return_value = {
            "token": "srv_tok"}
        mock_session.client.return_value = mock_client
        mocker.patch.object(
            plugin, "_create_default_credentials_provider", return_value=mock_session)
        assert plugin.get_auth_token() == "srv_tok"

    def test_no_host_raises(self):
        plugin = IdpTokenAuthPlugin()
        plugin.redshift_property = RedshiftProperty()
        plugin.redshift_property.host = ""
        with pytest.raises(InterfaceError, match="Host URL must be provided"):
            plugin._get_default_credentials_auth_token()

    def test_no_redshift_property_raises(self):
        plugin = IdpTokenAuthPlugin()
        plugin.redshift_property = None
        with pytest.raises(InterfaceError, match="Host URL must be provided"):
            plugin._get_default_credentials_auth_token()

    def test_credentials_unavailable_raises(self, mocker):
        from botocore.exceptions import NoCredentialsError
        plugin = _make_plugin_with_rp(
            host="mycluster.abc123.us-east-1.redshift.amazonaws.com",
            region="us-east-1", cluster_identifier="mycluster", is_serverless=False,
        )
        mock_session = MagicMock()
        mock_session.client.side_effect = NoCredentialsError()
        mocker.patch.object(
            plugin, "_create_default_credentials_provider", return_value=mock_session)
        with pytest.raises(InterfaceError, match="Failed to obtain subject token.*provisioned cluster"):
            plugin._get_default_credentials_auth_token()

    def test_invalid_credentials_api_403_raises(self, mocker):
        from botocore.exceptions import ClientError
        plugin = _make_plugin_with_rp(
            host="mycluster.abc123.us-east-1.redshift.amazonaws.com",
            region="us-east-1", cluster_identifier="mycluster", is_serverless=False,
        )
        mock_session = MagicMock()
        mock_client = MagicMock()
        error_response = {"Error": {"Code": "403", "Message": "Access Denied"}}
        mock_client.get_identity_center_auth_token.side_effect = ClientError(
            error_response, "GetIdentityCenterAuthToken")
        mock_session.client.return_value = mock_client
        mocker.patch.object(
            plugin, "_create_default_credentials_provider", return_value=mock_session)
        with pytest.raises(InterfaceError, match="Failed to obtain subject token.*provisioned cluster"):
            plugin._get_default_credentials_auth_token()



class TestIsUsingDefaultCredentialsBoundary:
    """Verify is_using_default_credentials across all parameter combinations."""

    @pytest.mark.parametrize("token,token_type,access_key_id,secret_access_key,session_token,expected", [
        # No params at all -> True
        (None, None, None, None, None, True),
        # Any token param set -> False
        ("t", None, None, None, None, False),
        (None, "tt", None, None, None, False),
        # Any IAM param set -> False
        (None, None, "a", None, None, False),
        (None, None, None, "s", None, False),
        (None, None, None, None, "st", False),
        (None, None, "a", "s", None, False),
        (None, None, "a", None, "st", False),
        (None, None, None, "s", "st", False),
        # Complete token pair -> False
        ("t", "tt", None, None, None, False),
        ("t", "tt", "a", None, None, False),
        ("t", "tt", "a", "s", "st", False),
        # Complete IAM triple -> False
        (None, None, "a", "s", "st", False),
        ("t", None, "a", "s", "st", False),
    ])
    def test_is_using_default_credentials(self, token, token_type, access_key_id, secret_access_key, session_token, expected):
        plugin = _make_plugin(
            token=token, token_type=token_type,
            access_key_id=access_key_id, secret_access_key=secret_access_key, session_token=session_token,
        )
        assert plugin.is_using_default_credentials() is expected


class TestCheckRequiredParametersBoundary:
    """Verify check_required_parameters rejects conflicting and incomplete credentials.

    Logic:
    1. All params None -> default credentials (valid, early return)
    2. Both direct token AND identity-enhanced fully provided -> conflict error
    3. Otherwise (partial or single-side only) -> "either X or Y must be provided"
    """

    def test_conflicting_full_token_and_full_iam(self):
        """Both complete direct token and complete IAM raises conflict error."""
        plugin = _make_plugin(
            token="t", token_type="tt",
            access_key_id="a", secret_access_key="s", session_token="st",
        )
        with pytest.raises(InterfaceError, match=r"IdC authentication failed: Cannot provide both direct token parameters \(token, token_type\) and \(AccessKeyID, SecretAccessKey, SessionToken\)."):
            plugin.check_required_parameters()

    @pytest.mark.parametrize("token,token_type,access_key_id,secret_access_key,session_token", [
        # Partial token only (no IAM) - not default (has a param), not direct_token, not identity_enhanced
        ("t", None, None, None, None),
        (None, "tt", None, None, None),
        # Partial IAM only (no token) - not default, not direct_token, not identity_enhanced
        (None, None, "a", None, None),
        (None, None, None, "s", None),
        (None, None, None, None, "st"),
        (None, None, "a", "s", None),
        (None, None, "a", None, "st"),
        (None, None, None, "s", "st"),
        # Partial token + partial IAM - neither flow fully valid
        ("t", None, "a", None, None),
        ("t", None, None, "s", None),
        ("t", None, None, None, "st"),
        (None, "tt", "a", None, None),
        (None, "tt", None, "s", None),
    ])
    def test_incomplete_params_raises(self, token, token_type, access_key_id, secret_access_key, session_token):
        """Any incomplete combination (not default, not both-full) raises 'either X or Y'."""
        plugin = _make_plugin(
            token=token, token_type=token_type,
            access_key_id=access_key_id, secret_access_key=secret_access_key, session_token=session_token,
        )
        with pytest.raises(InterfaceError, match=r"IdC authentication failed: Either token/token_type or AccessKeyID/SecretAccessKey/SessionToken must be provided."):
            plugin.check_required_parameters()

    @pytest.mark.parametrize("token,token_type,access_key_id,secret_access_key,session_token", [
        # Complete direct token + partial IAM -> direct token flow wins, validation passes
        ("t", "tt", "a", None, None),
        ("t", "tt", None, "s", None),
        ("t", "tt", "a", "s", None),
        # Partial token + complete IAM -> identity-enhanced flow wins, validation passes
        ("t", None, "a", "s", "st"),
        (None, "tt", "a", "s", "st"),
    ])
    def test_one_complete_flow_with_extra_params_passes(self, token, token_type, access_key_id, secret_access_key, session_token):
        """When one flow is fully specified, extra partial params from the other don't block validation."""
        plugin = _make_plugin(
            token=token, token_type=token_type,
            access_key_id=access_key_id, secret_access_key=secret_access_key, session_token=session_token,
        )
        # Should not raise - at least one flow is fully valid
        plugin.check_required_parameters()


class TestLazyInitialization:
    def test_provider_none_at_construction(self):
        assert IdpTokenAuthPlugin()._default_credentials_provider is None

    def test_first_call_creates_provider(self, mocker):
        plugin = _make_plugin_with_rp(
            host="mycluster.abc123.us-east-1.redshift.amazonaws.com",
            region="us-east-1", cluster_identifier="mycluster", is_serverless=False,
        )
        mock_session = MagicMock()
        mock_client = MagicMock()
        mock_client.get_identity_center_auth_token.return_value = {
            "Token": "tok"}
        mock_session.client.return_value = mock_client
        create_spy = mocker.patch.object(
            plugin, "_create_default_credentials_provider", return_value=mock_session)
        plugin._get_default_credentials_auth_token()
        create_spy.assert_called_once()
        assert plugin._default_credentials_provider is mock_session

    def test_subsequent_calls_reuse_provider(self, mocker):
        plugin = _make_plugin_with_rp(
            host="mycluster.abc123.us-east-1.redshift.amazonaws.com",
            region="us-east-1", cluster_identifier="mycluster", is_serverless=False,
        )
        mock_session = MagicMock()
        mock_client = MagicMock()
        mock_client.get_identity_center_auth_token.return_value = {
            "Token": "tok"}
        mock_session.client.return_value = mock_client
        create_spy = mocker.patch.object(
            plugin, "_create_default_credentials_provider", return_value=mock_session)
        plugin._get_default_credentials_auth_token()
        plugin._get_default_credentials_auth_token()
        create_spy.assert_called_once()

    def test_boto3_client_created_without_explicit_credentials(self, mocker):
        plugin = _make_plugin_with_rp(
            host="mycluster.abc123.us-east-1.redshift.amazonaws.com",
            region="us-east-1", cluster_identifier="mycluster", is_serverless=False,
        )
        mock_session = MagicMock()
        mock_client = MagicMock()
        mock_client.get_identity_center_auth_token.return_value = {
            "Token": "tok"}
        mock_session.client.return_value = mock_client
        mocker.patch.object(
            plugin, "_create_default_credentials_provider", return_value=mock_session)
        plugin._get_default_credentials_auth_token()
        call_kwargs = mock_session.client.call_args[1]
        assert "aws_access_key_id" not in call_kwargs
        assert "aws_secret_access_key" not in call_kwargs
        assert "aws_session_token" not in call_kwargs
