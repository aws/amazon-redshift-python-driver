import logging
import re
import typing

from redshift_connector.error import InterfaceError
from redshift_connector.plugin.saml_credentials_provider import SamlCredentialsProvider

_logger: logging.Logger = logging.getLogger(__name__)


class AdfsCredentialsProvider(SamlCredentialsProvider):
    # Required method to grab the SAML Response. Used in base class to refresh temporary credentials.
    def get_saml_assertion(self: "AdfsCredentialsProvider") -> typing.Optional[str]:
        if self.idp_host == "" or self.idp_host is None:
            raise InterfaceError("Missing required property: idp_host")

        if self.user_name == "" or self.user_name is None or self.password == "" or self.password is None:
            return self.windows_integrated_authentication()

        return self.form_based_authentication()

    def windows_integrated_authentication(self: "AdfsCredentialsProvider"):
        pass

    def form_based_authentication(self: "AdfsCredentialsProvider") -> str:
        import bs4  # type: ignore
        import requests

        url: str = "https://{host}:{port}/adfs/ls/IdpInitiatedSignOn.aspx?loginToRp=urn:amazon:webservices".format(
            host=self.idp_host, port=str(self.idpPort)
        )
        try:
            response: "requests.Response" = requests.get(url, verify=self.do_verify_ssl_cert())
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            _logger.error("Request for SAML assertion when refreshing credentials was unsuccessful. {}".format(str(e)))
            raise InterfaceError(e)
        except requests.exceptions.Timeout as e:
            _logger.error("A timeout occurred when requesting SAML assertion")
            raise InterfaceError(e)
        except requests.exceptions.TooManyRedirects as e:
            _logger.error(
                "A error occurred when requesting SAML assertion to refresh credentials. "
                "Verify RedshiftProperties are correct"
            )
            raise InterfaceError(e)
        except requests.exceptions.RequestException as e:
            _logger.error("A unknown error occurred when requesting SAML assertion to refresh credentials")
            raise InterfaceError(e)

        try:
            soup = bs4.BeautifulSoup(response.text)
        except Exception as e:
            _logger.error("An error occurred while parsing response: {}".format(str(e)))
            raise InterfaceError(e)

        payload: typing.Dict[str, typing.Optional[str]] = {}

        for inputtag in soup.find_all(re.compile("(INPUT|input)")):
            name: str = inputtag.get("name", "")
            value: str = inputtag.get("value", "")
            if "username" in name.lower():
                payload[name] = self.user_name
            elif "authmethod" in name.lower():
                payload[name] = value
            elif "password" in name.lower():
                payload[name] = self.password
            elif name != "":
                payload[name] = value

        action: typing.Optional[str] = self.get_form_action(soup)
        if action and action.startswith("/"):
            url = "https://{host}:{port}{action}".format(host=self.idp_host, port=str(self.idpPort), action=action)

        try:
            response = requests.post(url, data=payload, verify=self.do_verify_ssl_cert())
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            _logger.error("Request to refresh credentials was unsuccessful. {}".format(str(e)))
            raise InterfaceError(e)
        except requests.exceptions.Timeout as e:
            _logger.error("A timeout occurred when attempting to refresh credentials")
            raise InterfaceError(e)
        except requests.exceptions.TooManyRedirects as e:
            _logger.error("A error occurred when refreshing credentials. Verify RedshiftProperties are correct")
            raise InterfaceError(e)
        except requests.exceptions.RequestException as e:
            _logger.error("A unknown error occurred when refreshing credentials")
            raise InterfaceError(e)

        try:
            soup = bs4.BeautifulSoup(response.text)
        except Exception as e:
            _logger.error("An error occurred while parsing response: {}".format(str(e)))
            raise InterfaceError(e)
        assertion: str = ""

        for inputtag in soup.find_all("input"):
            if inputtag.get("name") == "SAMLResponse":
                assertion = inputtag.get("value")

        if assertion == "":
            raise InterfaceError("Failed to find Adfs access_token")

        return assertion
