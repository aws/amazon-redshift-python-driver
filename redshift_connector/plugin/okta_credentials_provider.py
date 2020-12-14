import json
import logging
import typing

from redshift_connector.error import InterfaceError
from redshift_connector.plugin.credential_provider_constants import okta_headers
from redshift_connector.plugin.saml_credentials_provider import SamlCredentialsProvider
from redshift_connector.redshift_property import RedshiftProperty

_logger: logging.Logger = logging.getLogger(__name__)


# Class to get SAML Response from Okta
class OktaCredentialsProvider(SamlCredentialsProvider):
    def __init__(self: "OktaCredentialsProvider") -> None:
        super().__init__()
        self.app_id: typing.Optional[str] = None
        self.app_name: typing.Optional[str] = None

    def add_parameter(self: "OktaCredentialsProvider", info: RedshiftProperty) -> None:
        super().add_parameter(info)
        self.app_id = info.app_id
        self.app_name = info.app_name

    def get_saml_assertion(self: "OktaCredentialsProvider") -> str:
        self.check_required_parameters()
        if self.app_id == "" or self.app_id is None:
            raise InterfaceError("Missing required property: app_id")

        okta_session_token: str = self.okta_authentication()
        return self.handle_saml_assertion(okta_session_token)

    # Authenticates users credentials via Okta, return Okta session token.
    def okta_authentication(self: "OktaCredentialsProvider") -> str:
        import requests

        # HTTP Post request to Okta API for session token
        url: str = "https://{host}/api/v1/authn".format(host=self.idp_host)
        headers: typing.Dict[str, str] = okta_headers
        payload: typing.Dict[str, typing.Optional[str]] = {"username": self.user_name, "password": self.password}
        try:
            response: "requests.Response" = requests.post(
                url, data=json.dumps(payload), headers=headers, verify=self.do_verify_ssl_cert()
            )
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            _logger.error("Request for authentication from Okta was unsuccessful. {}".format(str(e)))
            raise InterfaceError(e)
        except requests.exceptions.Timeout as e:
            _logger.error("A timeout occurred when requesting authentication from Okta")
            raise InterfaceError(e)
        except requests.exceptions.TooManyRedirects as e:
            _logger.error(
                "A error occurred when requesting authentication from Okta. Verify RedshiftProperties are correct"
            )
            raise InterfaceError(e)
        except requests.exceptions.RequestException as e:
            _logger.error("A unknown error occurred when requesting authentication from Okta")
            raise InterfaceError(e)

        # Retrieve and parse the Okta response for session token
        if response is None:
            raise InterfaceError("Request for authentication returned empty payload")
        response_payload: typing.Dict[str, typing.Any] = response.json()
        if "status" not in response_payload:
            raise InterfaceError("Request for authentication retrieved malformed payload.")
        elif response_payload["status"] != "SUCCESS":
            raise InterfaceError("Request for authentication received non success response.")
        else:
            return str(response_payload["sessionToken"])

    # Retrieves SAML assertion from Okta containing AWS roles.
    def handle_saml_assertion(self: "OktaCredentialsProvider", okta_session_token: str) -> str:
        import bs4  # type: ignore
        import requests

        url: str = "https://{host}/home/{app_name}/{app_id}?onetimetoken={session_token}".format(
            host=self.idp_host, app_name=self.app_name, app_id=self.app_id, session_token=okta_session_token
        )
        try:
            response: "requests.Response" = requests.get(url, verify=self.do_verify_ssl_cert())
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            _logger.error("Request for SAML assertion from Okta was unsuccessful. {}".format(str(e)))
            raise InterfaceError(e)
        except requests.exceptions.Timeout as e:
            _logger.error("A timeout occurred when requesting SAML assertion from Okta")
            raise InterfaceError(e)
        except requests.exceptions.TooManyRedirects as e:
            _logger.error(
                "A error occurred when requesting SAML assertion from Okta. Verify RedshiftProperties are correct"
            )
            raise InterfaceError(e)
        except requests.exceptions.RequestException as e:
            _logger.error("A unknown error occurred when requesting SAML assertion from Okta")
            raise InterfaceError(e)

        text: str = response.text

        try:
            soup = bs4.BeautifulSoup(text, "html.parser")
            saml_response: str = soup.find("input", {"name": "SAMLResponse"})["value"]
            return saml_response
        except Exception as e:
            _logger.error("An error occurred while parsing SAML response: {}".format(str(e)))
            raise InterfaceError(e)
