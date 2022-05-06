import typing
from abc import ABC, abstractmethod

from redshift_connector.error import InterfaceError
from redshift_connector.redshift_property import RedshiftProperty

if typing.TYPE_CHECKING:
    from redshift_connector.credentials_holder import ABCCredentialsHolder
    from redshift_connector.plugin.native_token_holder import NativeTokenHolder


class IdpCredentialsProvider(ABC):
    """
    Abstract base class for authentication plugins.
    """

    def __init__(self: "IdpCredentialsProvider") -> None:
        self.cache: typing.Dict[str, typing.Union[NativeTokenHolder, ABCCredentialsHolder]] = {}

    @staticmethod
    def close_window_http_resp() -> bytes:
        """
        Builds the client facing HTML contents notifying that the authentication window may be closed.
        """
        return str.encode(
            "HTTP/1.1 200 OK\nContent-Type: text/html\n\n"
            + "<html><body>Thank you for using Amazon Redshift! You can now close this window.</body></html>\n"
        )

    @abstractmethod
    def check_required_parameters(self: "IdpCredentialsProvider") -> None:
        """
        Performs validation on client provided parameters used by the IdP.
        """
        pass  # pragma: no cover

    @abstractmethod
    def add_parameter(self: "IdpCredentialsProvider", info: RedshiftProperty) -> None:
        """
        Defines instance attributes from the :class:RedshiftProperty object associated with the current authentication session.
        """
        pass  # pragma: no cover

    @classmethod
    def validate_url(cls, uri: str) -> None:
        if not uri.startswith("https"):
            raise InterfaceError("URI: {} is an invalid web address".format(uri))
