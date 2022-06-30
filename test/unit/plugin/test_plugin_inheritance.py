import pytest

from redshift_connector import plugin
from redshift_connector.plugin.i_native_plugin import INativePlugin
from redshift_connector.plugin.i_plugin import IPlugin
from redshift_connector.plugin.idp_credentials_provider import IdpCredentialsProvider


def test_i_native_plugin_inherits_from_i_plugin():
    assert issubclass(INativePlugin, IPlugin)


def test_idp_credentials_provider_inherits_from_i_plugin():
    assert issubclass(IdpCredentialsProvider, IPlugin)


def test_saml_provider_plugin_inherit_from_idp_credentials_provider():
    assert issubclass(plugin.SamlCredentialsProvider, IdpCredentialsProvider)


def test_jwt_abc_inherit_from_idp_credentials_provider():
    assert issubclass(plugin.JwtCredentialsProvider, IdpCredentialsProvider)


saml_provider_plugins = (
    plugin.BrowserSamlCredentialsProvider,
    plugin.OktaCredentialsProvider,
    plugin.AdfsCredentialsProvider,
    plugin.AzureCredentialsProvider,
    plugin.BrowserSamlCredentialsProvider,
    plugin.BrowserAzureCredentialsProvider,
)


@pytest.mark.parametrize("saml_plugin", saml_provider_plugins)
def test_saml_provider_plugins_inherit_from_saml_credentials_provider(saml_plugin):
    assert issubclass(saml_plugin, plugin.SamlCredentialsProvider)


jwt_plugins = (plugin.BrowserAzureOAuth2CredentialsProvider, plugin.BasicJwtCredentialsProvider)


@pytest.mark.parametrize("jwt_plugin", jwt_plugins)
def test_jwt_plugins_inherit_from_jwt_abc(jwt_plugin):
    assert issubclass(jwt_plugin, plugin.JwtCredentialsProvider)
