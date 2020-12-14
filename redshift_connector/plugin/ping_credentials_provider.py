import logging
import re
import typing

from redshift_connector.error import InterfaceError
from redshift_connector.plugin.saml_credentials_provider import SamlCredentialsProvider
from redshift_connector.redshift_property import RedshiftProperty

_logger: logging.Logger = logging.getLogger(__name__)


class PingCredentialsProvider(SamlCredentialsProvider):
    def __init__(self: "PingCredentialsProvider") -> None:
        super().__init__()
        self.partner_sp_id: typing.Optional[str] = None

    def add_parameter(self: "PingCredentialsProvider", info: RedshiftProperty) -> None:
        super().add_parameter(info)
        self.partner_sp_id = info.partner_sp_id

    # Required method to grab the SAML Response. Used in base class to refresh temporary credentials.
    def get_saml_assertion(self: "PingCredentialsProvider") -> str:
        import bs4  # type: ignore
        import requests

        self.check_required_parameters()

        if self.partner_sp_id is None:
            self.partner_sp_id = "urn%3Aamazon%3Awebservices"

        url: str = "https://{host}:{port}/idp/startSSO.ping?PartnerSpId={sp_id}".format(
            host=self.idp_host, port=str(self.idpPort), sp_id=self.partner_sp_id
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
        username: bool = False
        pwd: bool = False

        for inputtag in soup.find_all(re.compile("(INPUT|input)")):
            name: str = inputtag.get("name", "")
            id: str = inputtag.get("id", "")
            value: str = inputtag.get("value", "")

            if username is False and self.is_text(inputtag) and id == "username":
                payload[name] = self.user_name
                username = True
            elif self.is_password(inputtag) and ("pass" in name):
                if pwd is True:
                    raise InterfaceError("Duplicate password fields on login page.")
                payload[name] = self.password
                pwd = True
            elif name != "":
                payload[name] = value

        if username is False:
            for inputtag in soup.find_all(re.compile("(INPUT|input)")):
                name = inputtag.get("name", "")
                if self.is_text(inputtag) and ("user" in name or "email" in name):
                    payload[name] = self.user_name
                    username = True

        if (username is False) or (pwd is False):
            raise InterfaceError("Failed to parse login form.")

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
            _logger.error("An error occurred while parsing SAML response: {}".format(str(e)))
            raise InterfaceError(e)

        assertion: str = ""
        for inputtag in soup.find_all("input"):
            if inputtag.get("name") == "SAMLResponse":
                assertion = inputtag.get("value")

        if assertion == "":
            raise InterfaceError("Failed to retrieve SAMLAssertion.")

        return assertion
