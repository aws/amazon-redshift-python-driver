import datetime
import logging
import typing
from enum import Enum

from dateutil.tz import tzutc

from redshift_connector.auth.aws_credentials_provider import AWSCredentialsProvider
from redshift_connector.config import ClientProtocolVersion
from redshift_connector.credentials_holder import (
    ABCAWSCredentialsHolder,
    AWSDirectCredentialsHolder,
    AWSProfileCredentialsHolder,
    CredentialsHolder,
)
from redshift_connector.error import InterfaceError, ProgrammingError
from redshift_connector.plugin import SamlCredentialsProvider
from redshift_connector.redshift_property import RedshiftProperty

_logger: logging.Logger = logging.getLogger(__name__)


class SupportedSSLMode(Enum):
    VERIFY_CA: str = "verify-ca"
    VERIFY_FULL: str = "verify-full"

    @staticmethod
    def default() -> str:
        return SupportedSSLMode.VERIFY_CA.value

    @staticmethod
    def list() -> typing.List[str]:
        return list(map(lambda mode: mode.value, SupportedSSLMode))


def dynamic_plugin_import(name: str):
    components = name.split(".")
    mod = __import__(components[0])
    for comp in components[1:]:
        mod = getattr(mod, comp)
    return mod


class IamHelper:

    credentials_cache: typing.Dict[str, dict] = {}

    @staticmethod
    def set_iam_properties(info: RedshiftProperty) -> RedshiftProperty:
        """
        Helper function to handle IAM connection properties and ensure required parameters are specified.
        Parameters
        """
        if info is None:
            raise InterfaceError("Invalid connection property setting. info must be specified")

        # Check for IAM keys and AuthProfile first
        if info.auth_profile is not None:
            import pkg_resources
            from packaging.version import Version

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

                resp = IamHelper.read_auth_profile(
                    auth_profile=typing.cast(str, info.auth_profile),
                    iam_access_key_id=typing.cast(str, info.access_key_id),
                    iam_secret_key=typing.cast(str, info.secret_access_key),
                    iam_session_token=info.session_token,
                    info=info,
                )
                info.put_all(resp)

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

        if (info.ssl is False) and (info.iam is True):
            raise InterfaceError("Invalid connection property setting. SSL must be enabled when using IAM")

        if (info.iam is False) and (info.ssl_insecure is False):
            raise InterfaceError("Invalid connection property setting. IAM must be enabled when using ssl_insecure")
        elif (info.iam is False) and any(
            (info.credentials_provider, info.access_key_id, info.secret_access_key, info.session_token, info.profile)
        ):
            raise InterfaceError(
                "Invalid connection property setting. IAM must be enabled when using credential_provider, "
                "AWS credentials, Amazon Redshift authentication profile, or AWS profile"
            )
        elif info.iam is True:

            if not any(
                (
                    info.credentials_provider,
                    info.access_key_id,
                    info.secret_access_key,
                    info.session_token,
                    info.profile,
                    info.auth_profile,
                )
            ):
                raise InterfaceError(
                    "Invalid connection property setting. Credentials provider, AWS credentials, Redshift auth profile "
                    "or AWS profile must be provided when IAM is enabled"
                )

            if info.cluster_identifier is None:
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

        if info.client_protocol_version not in ClientProtocolVersion.list():
            raise InterfaceError(
                "Invalid connection property setting. client_protocol_version must be in: {}".format(
                    ClientProtocolVersion.list()
                )
            )

        if info.db_groups and info.force_lowercase:
            info.put("db_groups", [group.lower() for group in info.db_groups])

        if info.iam is True:
            if info.cluster_identifier is None:
                raise InterfaceError(
                    "Invalid connection property setting. cluster_identifier must be provided when IAM is enabled"
                )
            IamHelper.set_iam_credentials(info)
        return info

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

    @staticmethod
    def set_iam_credentials(info: RedshiftProperty) -> None:
        """
        Helper function to create the appropriate credential providers.
        """
        klass: typing.Optional[SamlCredentialsProvider] = None
        provider: typing.Union[SamlCredentialsProvider, AWSCredentialsProvider]

        if info.credentials_provider is not None:
            try:
                klass = dynamic_plugin_import(info.credentials_provider)
                provider = klass()  # type: ignore
                provider.add_parameter(info)  # type: ignore
            except (AttributeError, ModuleNotFoundError):
                _logger.debug("Failed to load user defined plugin: {}".format(info.credentials_provider))
                try:
                    predefined_idp: str = "redshift_connector.plugin.{}".format(info.credentials_provider)
                    klass = dynamic_plugin_import(predefined_idp)
                    provider = klass()  # type: ignore
                    provider.add_parameter(info)  # type: ignore
                    info.put("credentials_provider", predefined_idp)
                except (AttributeError, ModuleNotFoundError):
                    _logger.debug(
                        "Failed to load pre-defined IdP plugin from redshift_connector.plugin: {}".format(
                            info.credentials_provider
                        )
                    )
                    raise InterfaceError("Invalid credentials provider " + info.credentials_provider)
        else:  # indicates AWS Credentials will be used
            _logger.debug("AWS Credentials provider will be used for authentication")
            provider = AWSCredentialsProvider()
            provider.add_parameter(info)

        if isinstance(provider, SamlCredentialsProvider):
            credentials: CredentialsHolder = provider.get_credentials()
            metadata: CredentialsHolder.IamMetadata = credentials.get_metadata()
            if metadata is not None:
                auto_create: bool = metadata.get_auto_create()
                db_user: typing.Optional[str] = metadata.get_db_user()
                saml_db_user: typing.Optional[str] = metadata.get_saml_db_user()
                profile_db_user: typing.Optional[str] = metadata.get_profile_db_user()
                db_groups: typing.List[str] = metadata.get_db_groups()
                force_lowercase: bool = metadata.get_force_lowercase()
                allow_db_user_override: bool = metadata.get_allow_db_user_override()
                if auto_create is True:
                    info.put("auto_create", auto_create)

                if force_lowercase is True:
                    info.put("force_lowercase", force_lowercase)

                if allow_db_user_override is True:
                    if saml_db_user is not None:
                        info.put("db_user", saml_db_user)
                    elif db_user is not None:
                        info.put("db_user", db_user)
                    elif profile_db_user is not None:
                        info.put("db_user", profile_db_user)
                else:
                    if db_user is not None:
                        info.put("db_user", db_user)
                    elif profile_db_user is not None:
                        info.put("db_user", profile_db_user)
                    elif saml_db_user is not None:
                        info.put("db_user", saml_db_user)

                if (len(info.db_groups) == 0) and (len(db_groups) > 0):
                    if force_lowercase:
                        info.db_groups = [group.lower() for group in db_groups]
                    else:
                        info.db_groups = db_groups

        IamHelper.set_cluster_credentials(provider, info)

    @staticmethod
    def get_credentials_cache_key(info: RedshiftProperty):
        db_groups: str = ""

        if len(info.db_groups) > 0:
            info.put("db_groups", sorted(info.db_groups))
            db_groups = ",".join(info.db_groups)

        return ";".join(
            (
                typing.cast(str, info.db_user),
                info.db_name,
                db_groups,
                typing.cast(str, info.cluster_identifier),
                str(info.auto_create),
            )
        )

    @staticmethod
    def set_cluster_credentials(
        cred_provider: typing.Union[SamlCredentialsProvider, AWSCredentialsProvider], info: RedshiftProperty
    ) -> None:
        """
        Calls the AWS SDK methods to return temporary credentials.
        The expiration date is returned as the local time set by the client machines OS.
        """
        import boto3  # type: ignore
        import botocore  # type: ignore

        try:
            credentials_holder: typing.Union[
                CredentialsHolder, AWSDirectCredentialsHolder, AWSProfileCredentialsHolder
            ] = cred_provider.get_credentials()
            session_credentials: typing.Dict[str, str] = credentials_holder.get_session_credentials()

            for opt_key, opt_val in (("region_name", info.region), ("endpoint_url", info.endpoint_url)):
                if opt_val is not None:
                    session_credentials[opt_key] = opt_val

            # if AWS credentials were used to create a boto3.Session object, use it
            if credentials_holder.has_associated_session:
                cached_session: boto3.Session = typing.cast(
                    ABCAWSCredentialsHolder, credentials_holder
                ).get_boto_session()
                client = cached_session.client(service_name="redshift", region_name=info.region)
            else:
                client = boto3.client(service_name="redshift", **session_credentials)

            if info.host is None or info.host == "" or info.port is None or info.port == "":
                response = client.describe_clusters(ClusterIdentifier=info.cluster_identifier)

                info.put("host", response["Clusters"][0]["Endpoint"]["Address"])
                info.put("port", response["Clusters"][0]["Endpoint"]["Port"])

            cred: typing.Optional[typing.Dict[str, typing.Union[str, datetime.datetime]]] = None

            if info.iam_disable_cache is False:
                # temporary credentials are cached by redshift_connector and will be used if they have not expired
                cache_key: str = IamHelper.get_credentials_cache_key(info)
                cred = IamHelper.credentials_cache.get(cache_key, None)

                _logger.debug(
                    "Searching credential cache for temporary AWS credentials. Found: {} Expiration: {}".format(
                        bool(cache_key in IamHelper.credentials_cache),
                        cred["Expiration"] if cred is not None else "N/A",
                    )
                )

            if cred is None or typing.cast(datetime.datetime, cred["Expiration"]) < datetime.datetime.now(tz=tzutc()):
                # retries will occur by default ref:
                # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/retries.html#legacy-retry-mode
                cred = typing.cast(
                    typing.Dict[str, typing.Union[str, datetime.datetime]],
                    client.get_cluster_credentials(
                        DbUser=info.db_user,
                        DbName=info.db_name,
                        DbGroups=info.db_groups,
                        ClusterIdentifier=info.cluster_identifier,
                        AutoCreate=info.auto_create,
                    ),
                )

                if info.iam_disable_cache is False:
                    IamHelper.credentials_cache[cache_key] = typing.cast(
                        typing.Dict[str, typing.Union[str, datetime.datetime]], cred
                    )

            info.put("user_name", typing.cast(str, cred["DbUser"]))
            info.put("password", typing.cast(str, cred["DbPassword"]))

            _logger.debug("Using temporary aws credentials with expiration: {}".format(cred["Expiration"]))

        except botocore.exceptions.ClientError as e:
            _logger.error("ClientError: %s", e)
            raise e
        except Exception as e:
            _logger.error("other Exception: %s", e)
            raise e
