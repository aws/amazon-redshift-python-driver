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
    """Verify that check_required_parameters rejects incomplete identity-enhanced credentials (treated as no valid flow)"""
    itap: IdpTokenAuthPlugin = IdpTokenAuthPlugin()
    itap.access_key_id = "dummy_access_key_id"
    itap.secret_access_key = "dummy_secret_access_key"
    # Missing session_token - this means _is_using_identity_enhanced_credentials returns False
    # and since there's no direct token either, it should fail with "no parameters" error
    
    with pytest.raises(
        InterfaceError, match="IdC authentication failed: Either token/token_type or AccessKeyID/SecretAccessKey/SessionToken must be provided."
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
    itap.host = "cluster.abc123.us-east-1.redshift.amazonaws.com"
    
    with pytest.raises(
        InterfaceError, match="Cannot provide both direct token parameters"
    ):
        itap.check_required_parameters()


def test_check_required_parameters_rejects_no_parameters():
    """Verify that check_required_parameters rejects when no parameters are provided"""
    itap: IdpTokenAuthPlugin = IdpTokenAuthPlugin()
    
    with pytest.raises(
        InterfaceError, match="IdC authentication failed: Either token/token_type or AccessKeyID/SecretAccessKey/SessionToken must be provided."
    ):
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
