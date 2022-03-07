import logging
import typing
from enum import Enum

from redshift_connector.error import InterfaceError, ProgrammingError
from redshift_connector.redshift_property import RedshiftProperty

logging.getLogger(__name__).addHandler(logging.NullHandler())
_logger: logging.Logger = logging.getLogger(__name__)


class SupportedSSLMode(Enum):
    """
    Definition of SSL modes supported by Amazon Redshift.
    """

    VERIFY_CA: str = "verify-ca"
    VERIFY_FULL: str = "verify-full"

    @staticmethod
    def default() -> str:
        return SupportedSSLMode.VERIFY_CA.value

    @staticmethod
    def list() -> typing.List[str]:
        return list(map(lambda mode: mode.value, SupportedSSLMode))


class IdpAuthHelper:
    # Subtype of plugin
    SAML_PLUGIN: int = 1
    JWT_PLUGIN: int = 2

    @staticmethod
    def set_auth_properties(info: RedshiftProperty):
        """
        Helper function to handle IAM and Native Auth connection properties and ensure required parameters are specified.
        Parameters
        """
        import pkg_resources
        from packaging.version import Version

        if info is None:
            raise InterfaceError("Invalid connection property setting. info must be specified")

        # IAM requires an SSL connection to work.
        # Make sure that is set to SSL level VERIFY_CA or higher.
        if info.ssl is True:
            if info.sslmode not in SupportedSSLMode.list():
                info.put("sslmode", SupportedSSLMode.default())
                _logger.debug(
                    "A non-supported value: {} was provides for sslmode. Falling back to default value: {}".format(
                        info.sslmode, SupportedSSLMode.default()
                    )
                )
        else:
            info.put("sslmode", "")

        # elif (info.iam is False) and any(
        #     (info.credentials_provider, info.access_key_id, info.secret_access_key, info.session_token, info.profile)
        # ):
        #     raise InterfaceError(
        #         "Invalid connection property setting. IAM must be enabled when using credential_provider, "
        #         "AWS credentials, Amazon Redshift authentication profile, or AWS profile"
        #     )
        if info.iam is True:
            _logger.debug("boto3 version: {}".format(Version(pkg_resources.get_distribution("boto3").version)))
            _logger.debug("botocore version: {}".format(Version(pkg_resources.get_distribution("botocore").version)))

            if info.cluster_identifier is None and not info.is_serverless_host:
                raise InterfaceError(
                    "Invalid connection property setting. cluster_identifier must be provided when IAM is enabled"
                )

            if info.credentials_provider is not None:
                if info.auth_profile is None and any(
                    (info.access_key_id, info.secret_access_key, info.session_token, info.profile)
                ):
                    raise InterfaceError(
                        "Invalid connection property setting. It is not valid to provide both Credentials provider and "
                        "AWS credentials or AWS profile"
                    )
                elif not isinstance(info.credentials_provider, str):
                    raise InterfaceError(
                        "Invalid connection property setting. It is not valid to provide a non-string value to "
                        "credentials_provider."
                    )
            elif info.profile is not None:
                if info.auth_profile is None and any((info.access_key_id, info.secret_access_key, info.session_token)):
                    raise InterfaceError(
                        "Invalid connection property setting. It is not valid to provide any of access_key_id, "
                        "secret_access_key, or session_token when profile is provided"
                    )
            elif info.access_key_id is not None:

                if info.secret_access_key is not None:
                    pass
                elif info.password != "":
                    info.put("secret_access_key", info.password)
                    _logger.debug("Value of password will be used for secret_access_key")
                else:
                    raise InterfaceError(
                        "Invalid connection property setting. "
                        "secret access key must be provided in either secret_access_key or password field"
                    )

                _logger.debug(
                    "AWS Credentials access_key_id: {} secret_access_key: {} session_token: {}".format(
                        bool(info.access_key_id), bool(info.secret_access_key), bool(info.session_token)
                    )
                )
            elif info.secret_access_key is not None:
                raise InterfaceError(
                    "Invalid connection property setting. access_key_id is required when secret_access_key is "
                    "provided"
                )
            elif info.session_token is not None:
                raise InterfaceError(
                    "Invalid connection property setting. access_key_id and secret_access_key are  required when "
                    "session_token is provided"
                )

        if info.db_groups and info.force_lowercase:
            info.put("db_groups", [group.lower() for group in info.db_groups])

        # Check for IAM keys and AuthProfile first
        if info.auth_profile is not None:
            if Version(pkg_resources.get_distribution("boto3").version) < Version("1.17.111"):
                raise pkg_resources.VersionConflict(
                    "boto3 >= 1.17.111 required for authentication via Amazon Redshift authentication profile. "
                    "Please upgrade the installed version of boto3 to use this functionality."
                )

            if not all((info.access_key_id, info.secret_access_key, info.region)):
                raise InterfaceError(
                    "Invalid connection property setting. access_key_id, secret_access_key, and region are required "
                    "for authentication via Redshift auth_profile"
                )
            else:
                # info.put("region", info.region)
                # info.put("endpoint_url", info.endpoint_url)

                resp = IdpAuthHelper.read_auth_profile(
                    auth_profile=typing.cast(str, info.auth_profile),
                    iam_access_key_id=typing.cast(str, info.access_key_id),
                    iam_secret_key=typing.cast(str, info.secret_access_key),
                    iam_session_token=info.session_token,
                    info=info,
                )
                info.put_all(resp)

    @staticmethod
    def read_auth_profile(
        auth_profile: str,
        iam_access_key_id: str,
        iam_secret_key: str,
        iam_session_token: typing.Optional[str],
        info: RedshiftProperty,
    ) -> RedshiftProperty:
        import json

        import boto3
        from botocore.exceptions import ClientError

        # 1st phase - authenticate with boto3 client for Amazon Redshift via IAM
        # credentials provided by end user
        creds: typing.Dict[str, str] = {
            "aws_access_key_id": iam_access_key_id,
            "aws_secret_access_key": iam_secret_key,
            "region_name": typing.cast(str, info.region),
        }

        for opt_key, opt_val in (
            ("aws_session_token", iam_session_token),
            ("endpoint_url", info.endpoint_url),
        ):
            if opt_val is not None and opt_val != "":
                creds[opt_key] = opt_val

        try:
            _logger.debug("Initial authentication with boto3...")
            client = boto3.client(service_name="redshift", **creds)
            _logger.debug("Requesting authentication profiles")
            # 2nd phase - request Amazon Redshift authentication profiles and record contents for retrieving
            # temporary credentials for the Amazon Redshift cluster specified by end user
            response = client.describe_authentication_profiles(AuthenticationProfileName=auth_profile)
        except ClientError:
            raise InterfaceError("Unable to retrieve contents of Redshift authentication profile from server")

        _logger.debug("Received {} authentication profiles".format(len(response["AuthenticationProfiles"])))
        # the first matching authentication profile will be used
        profile_content: typing.Union[str] = response["AuthenticationProfiles"][0]["AuthenticationProfileContent"]

        try:
            profile_content_dict: typing.Dict = json.loads(profile_content)
            return RedshiftProperty(**profile_content_dict)
        except ValueError:
            raise ProgrammingError(
                "Unable to decode the JSON content of the Redshift authentication profile: {}".format(auth_profile)
            )


def dynamic_plugin_import(name: str):
    components = name.split(".")
    mod = __import__(components[0])
    for comp in components[1:]:
        mod = getattr(mod, comp)
    return mod
