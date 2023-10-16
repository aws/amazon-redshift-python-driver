import logging
import time
import typing
import webbrowser

import boto3
from botocore.exceptions import ClientError

from redshift_connector.error import InterfaceError
from redshift_connector.plugin.common_credentials_provider import (
    CommonCredentialsProvider,
)
from redshift_connector.redshift_property import RedshiftProperty

logging.getLogger(__name__).addHandler(logging.NullHandler())
_logger: logging.Logger = logging.getLogger(__name__)


class BrowserIdcAuthPlugin(CommonCredentialsProvider):
    """
    Class to get IdC Token using SSO OIDC APIs
    """

    DEFAULT_IDC_CLIENT_DISPLAY_NAME = "Amazon Redshift Python connector"
    CLIENT_TYPE = "public"
    GRANT_TYPE = "urn:ietf:params:oauth:grant-type:device_code"
    IDC_SCOPE = "redshift:connect"
    DEFAULT_BROWSER_AUTH_VERIFY_TIMEOUT_IN_SEC = 120
    DEFAULT_CREATE_TOKEN_INTERVAL_IN_SEC = 1

    def __init__(self: "BrowserIdcAuthPlugin") -> None:
        super().__init__()
        self.idc_response_timeout: int = self.DEFAULT_BROWSER_AUTH_VERIFY_TIMEOUT_IN_SEC
        self.idc_client_display_name: str = self.DEFAULT_IDC_CLIENT_DISPLAY_NAME
        self.register_client_cache: typing.Dict[str, dict] = {}
        self.start_url: typing.Optional[str] = None
        self.idc_region: typing.Optional[str] = None
        self.sso_oidc_client: boto3.client = None

    def add_parameter(
        self: "BrowserIdcAuthPlugin",
        info: RedshiftProperty,
    ) -> None:
        """
        Adds parameters to the BrowserIdcAuthPlugin
        :param info: RedshiftProperty object containing the parameters to be added to the BrowserIdcAuthPlugin.
        :return: None.
        """
        super().add_parameter(info)
        self.start_url = info.start_url
        _logger.debug("Setting start_url = {}".format(self.start_url))
        self.idc_region = info.idc_region
        _logger.debug("Setting idc_region = {}".format(self.idc_region))
        if info.idc_response_timeout and info.idc_response_timeout > 10:
            self.idc_response_timeout = info.idc_response_timeout
        _logger.debug("Setting idc_response_timeout = {}".format(self.idc_response_timeout))
        if info.idc_client_display_name:
            self.idc_client_display_name = info.idc_client_display_name
        _logger.debug("Setting idc_client_display_name = {}".format(self.idc_client_display_name))

    def check_required_parameters(self: "BrowserIdcAuthPlugin") -> None:
        """
        Checks if the required parameters are set.
        :return: None.
        :raises InterfaceError: Raised when the parameters are not valid.
        """
        super().check_required_parameters()
        if not self.start_url:
            _logger.error("IdC authentication failed: start_url needs to be provided in connection params")
            raise InterfaceError(
                "IdC authentication failed: The start URL must be included in the connection parameters."
            )
        if not self.idc_region:
            _logger.error("IdC authentication failed: idc_region needs to be provided in connection params")
            raise InterfaceError(
                "IdC authentication failed: The IdC region must be included in the connection parameters."
            )

    def get_cache_key(self: "BrowserIdcAuthPlugin") -> str:
        """
        Returns the cache key for the BrowserIdcAuthPlugin.
        :return: str.
        """
        return "{}".format(self.start_url if self.start_url else "")

    def get_auth_token(self: "BrowserIdcAuthPlugin") -> str:
        """
        Returns the auth token as per plugin specific implementation.
        :return: str.
        """
        return self.get_idc_token()

    def get_idc_token(self: "BrowserIdcAuthPlugin") -> str:
        """
        Returns the IdC token using SSO OIDC APIs.
        :return: str.
        """
        try:
            self.check_required_parameters()

            self.sso_oidc_client = boto3.client("sso-oidc", region_name=self.idc_region)
            register_client_cache_key: str = f"{self.idc_client_display_name}:{self.idc_region}"

            register_client_result: typing.Dict[str, typing.Any] = self.register_client(
                register_client_cache_key,
                self.idc_client_display_name,
                typing.cast(str, self.CLIENT_TYPE),
                self.IDC_SCOPE,
            )
            start_device_auth_result: typing.Dict[str, typing.Any] = self.start_device_authorization(
                register_client_result["clientId"],
                register_client_result["clientSecret"],
                typing.cast(str, self.start_url),
            )
            self.open_browser(start_device_auth_result["verificationUriComplete"])

            return self.poll_for_create_token(register_client_result, start_device_auth_result, self.GRANT_TYPE)
        except InterfaceError as e:
            raise
        except Exception as e:
            _logger.debug("An error occurred while trying to obtain an IdC token : {}".format(str(e)))
            raise InterfaceError("There was an error during authentication.")

    def register_client(  # type: ignore
        self: "BrowserIdcAuthPlugin", register_client_cache_key: str, client_name: str, client_type: str, scope: str
    ) -> typing.Dict[str, typing.Any]:
        """
        Registers the client with IdC.
        :param register_client_cache_key: str
            The cache key used for storing register client result.
        :param client_name: str
            The client name to be used for registering the client.
        :param client_type: str
            The client type to be used for registering the client.
        :param scope: str
            The scope to be used for registering the client.
        :return: dict
            The register client result from IdC
        """
        if (
            register_client_cache_key in self.register_client_cache
            and self.register_client_cache[register_client_cache_key]["clientSecretExpiresAt"] > time.time()
        ):
            _logger.debug("Valid registerClient result found from cache")
            return self.register_client_cache[register_client_cache_key]

        try:
            register_client_result: typing.Dict[str, typing.Any] = self.sso_oidc_client.register_client(
                clientName=client_name, clientType=client_type, scopes=[scope]
            )
            self.register_client_cache[register_client_cache_key] = register_client_result
            return register_client_result
        except ClientError as e:
            self.handle_error(e, "registering client with IdC")

    def start_device_authorization(  # type: ignore
        self: "BrowserIdcAuthPlugin", client_id: str, client_secret: str, start_url: str
    ) -> typing.Dict[str, typing.Any]:
        """
        Starts device authorization flow with IdC.
        :param client_id: str
            The client id to be used for starting device authorization.
        :param client_secret: str
            The client secret to be used for starting device authorization.
        :param start_url: str
            The portal start url to be used for starting device authorization.
        :return: dict
            The start device authorization result from IdC.
        """
        try:
            response: typing.Dict[str, typing.Any] = self.sso_oidc_client.start_device_authorization(
                clientId=client_id, clientSecret=client_secret, startUrl=start_url
            )
            return response
        except ClientError as e:
            self.handle_error(e, "starting device authorization with IdC")

    def open_browser(self: "BrowserIdcAuthPlugin", url: str) -> None:
        """
        Opens the default browser with this url to allow user authentication with the IdC
        :param url: str
            The verification uri obtained from start device auth response
        :return: None.
        """
        _logger.debug("Opening browser with url: {}".format(url))
        self.validate_url(url)
        webbrowser.open(url)

    def poll_for_create_token(
        self: "BrowserIdcAuthPlugin",
        register_client_result: typing.Dict[str, typing.Any],
        start_device_auth_result: typing.Dict[str, typing.Any],
        grant_type: str,
    ) -> str:
        """
        Polls for IdC access token using SSO OIDC APIs.
        :param register_client_result: dict
            The register client result from IdC.
        :param start_device_auth_result: dict
            The start device auth result from IdC.
        :param grant_type: str
            The grant type to be used for polling for IdC access token.
        :return: str
            The IdC access token obtained from polling for IdC access token.
        :raises InterfaceError: Raised when the IdC access token is not fetched successfully.
        """
        polling_end_time: float = time.time() + self.idc_response_timeout

        polling_interval_in_sec: int = self.DEFAULT_CREATE_TOKEN_INTERVAL_IN_SEC
        if start_device_auth_result["interval"]:
            polling_interval_in_sec = start_device_auth_result["interval"]

        while time.time() < polling_end_time:
            try:
                response: typing.Dict[str, typing.Any] = self.sso_oidc_client.create_token(
                    clientId=register_client_result["clientId"],
                    clientSecret=register_client_result["clientSecret"],
                    grantType=grant_type,
                    deviceCode=start_device_auth_result["deviceCode"],
                )
                if not response["accessToken"]:
                    raise InterfaceError("IdC authentication failed : The credential token couldn't be created.")
                return response["accessToken"]
            except ClientError as e:
                if e.response["Error"]["Code"] == "AuthorizationPendingException":
                    _logger.debug("Browser authorization pending from user")
                    time.sleep(polling_interval_in_sec)
                else:
                    self.handle_error(e, "polling for an IdC access token")

        raise InterfaceError("IdC authentication failed : The request timed out. Authentication wasn't completed.")

    def handle_error(self: "BrowserIdcAuthPlugin", e: ClientError, operation: str) -> None:
        """
        Handles the client error from SSO OIDC APIs.
        :param e: ClientError
            The error from SSO OIDC API.
        :param operation: str
            The operation for which error was encountered.
        :return: None.
        :raises InterfaceError: A client error to be returned to the user with appropriate error message
        """
        _logger.debug("Error response = {} ".format(e.response))
        error_message = e.response["Error"]["Message"]
        if not error_message:
            error_message = (
                e.response["error_description"] if e.response["error_description"] else "Something unexpected happened"
            )
        error_code = e.response["Error"]["Code"]
        _logger.debug("An error occurred while {}: ClientError = {} - {}".format(operation, error_code, error_message))
        raise InterfaceError("IdC authentication failed : An error occurred during the request.")
