from redshift_connector.plugin import SamlCredentialsProvider


class MockCredentialsProvider(SamlCredentialsProvider):
    def get_saml_assertion(self: "SamlCredentialsProvider"):
        return "mocked"
