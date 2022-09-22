import datetime
import enum
import logging
import typing

from dateutil.tz import tzutc
from packaging.version import Version

from redshift_connector.auth.aws_credentials_provider import AWSCredentialsProvider
from redshift_connector.credentials_holder import (
    ABCAWSCredentialsHolder,
    AWSDirectCredentialsHolder,
    AWSProfileCredentialsHolder,
    CredentialsHolder,
)
from redshift_connector.error import InterfaceError, ProgrammingError
from redshift_connector.idp_auth_helper import IdpAuthHelper
from redshift_connector.native_plugin_helper import NativeAuthPluginHelper
from redshift_connector.plugin.i_native_plugin import INativePlugin
from redshift_connector.plugin.i_plugin import IPlugin
from redshift_connector.plugin.saml_credentials_provider import SamlCredentialsProvider
from redshift_connector.redshift_property import RedshiftProperty

_logger: logging.Logger = logging.getLogger(__name__)


class IamHelper(IdpAuthHelper):
    class IAMAuthenticationType(enum.Enum):
        """
        Defines authentication types supported by redshift-connector
        """

        NONE = enum.auto()
        PROFILE = enum.auto()
        IAM_KEYS_WITH_SESSION = enum.auto()
        IAM_KEYS = enum.auto()
        PLUGIN = enum.auto()

    class GetClusterCredentialsAPIType(enum.Enum):
        """
        Defines supported Python SDK methods used for Redshift credential retrieval
        """

        SERVERLESS_V1 = "get_credentials()"
        IAM_V1 = "get_cluster_credentials()"
        IAM_V2 = "get_cluster_credentials_with_iam()"

        @staticmethod
        def can_support_v2(provider_type: "IamHelper.IAMAuthenticationType") -> bool:
            """
            Determines if user provided connection options and boto3 version support group federation.
            """
            return (
                provider_type
                in (
                    IamHelper.IAMAuthenticationType.PROFILE,
                    IamHelper.IAMAuthenticationType.IAM_KEYS,
                    IamHelper.IAMAuthenticationType.IAM_KEYS_WITH_SESSION,
                )
            ) and IdpAuthHelper.get_pkg_version("boto3") >= Version("1.24.5")

    credentials_cache: typing.Dict[str, dict] = {}

    @staticmethod
    def get_cluster_credentials_api_type(
        info: RedshiftProperty, provider_type: "IamHelper.IAMAuthenticationType"
    ) -> GetClusterCredentialsAPIType:
        """
        Returns an enum representing the Python SDK method to use for getting temporary IAM credentials.
        """
        if not info._is_serverless:
            if not info.group_federation:
                return IamHelper.GetClusterCredentialsAPIType.IAM_V1
            elif IamHelper.GetClusterCredentialsAPIType.can_support_v2(provider_type):
                return IamHelper.GetClusterCredentialsAPIType.IAM_V2
            else:
                raise InterfaceError("Authentication with plugin is not supported for group federation")
        elif not info.group_federation:
            return IamHelper.GetClusterCredentialsAPIType.SERVERLESS_V1
        elif IamHelper.GetClusterCredentialsAPIType.can_support_v2(provider_type):
            return IamHelper.GetClusterCredentialsAPIType.IAM_V2
        else:
            raise InterfaceError("Authentication with plugin is not supported for group federation")

    @staticmethod
    def set_iam_properties(info: RedshiftProperty) -> RedshiftProperty:
        """
        Helper function to handle connection properties and ensure required parameters are specified.
        Parameters
        """
        provider_type: IamHelper.IAMAuthenticationType = IamHelper.IAMAuthenticationType.NONE
        # set properties present for both IAM, Native authentication
        IamHelper.set_auth_properties(info)

        if info._is_serverless and info.iam:
            if IdpAuthHelper.get_pkg_version("boto3") < Version("1.24.11"):
                raise ModuleNotFoundError(
                    "boto3 >= 1.24.11 required for authentication with Amazon Redshift serverless. "
                    "Please upgrade the installed version of boto3 to use this functionality."
                )

        if info.is_serverless_host:
            # consider overridden connection parameters
            if not info.region:
                info.set_region_from_host()
            if not info.serverless_acct_id:
                info.set_serverless_acct_id()
            if not info.serverless_work_group:
                info.set_serverless_work_group_from_host()

        if info.iam is True:
            if info.cluster_identifier is None and not info._is_serverless:
                raise InterfaceError(
                    "Invalid connection property setting. cluster_identifier must be provided when IAM is enabled"
                )
            IamHelper.set_iam_credentials(info)
        # Check for Browser based OAuth Native authentication
        NativeAuthPluginHelper.set_native_auth_plugin_properties(info)
        return info

    @staticmethod
    def set_iam_credentials(info: RedshiftProperty) -> None:
        """
        Helper function to create the appropriate credential providers.
        """
        klass: typing.Optional[IPlugin] = None
        provider: typing.Union[IPlugin, AWSCredentialsProvider]

        if info.credentials_provider is not None:
            provider = IdpAuthHelper.load_credentials_provider(info)

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

        if not isinstance(provider, INativePlugin):
            IamHelper.set_cluster_credentials(provider, info)

    @staticmethod
    def get_credentials_cache_key(info: RedshiftProperty, cred_provider: typing.Union[IPlugin, AWSCredentialsProvider]):
        db_groups: str = ""

        if len(info.db_groups) > 0:
            info.put("db_groups", sorted(info.db_groups))
            db_groups = ",".join(info.db_groups)

        cred_key: str = ""

        if cred_provider:
            cred_key = str(cred_provider.get_cache_key())

        return ";".join(
            filter(
                None,
                (
                    cred_key,
                    typing.cast(str, info.db_user if info.db_user else info.user_name),
                    info.db_name,
                    db_groups,
                    typing.cast(str, info.serverless_acct_id if info._is_serverless else info.cluster_identifier),
                    typing.cast(
                        str, info.serverless_work_group if info._is_serverless and info.serverless_work_group else ""
                    ),
                    str(info.auto_create),
                    str(info.duration),
                    # v2 api parameters
                    info.preferred_role,
                    info.web_identity_token,
                    info.role_arn,
                    info.role_session_name,
                    # providers
                    info.profile,
                    info.access_key_id,
                    info.secret_access_key,
                    info.session_token,
                ),
            )
        )

    @staticmethod
    def get_authentication_type(
        provider: typing.Union[IPlugin, AWSCredentialsProvider]
    ) -> "IamHelper.IAMAuthenticationType":
        """
        Returns an enum representing the type of authentication the user is requesting based on connection parameters.
        """
        provider_type: IamHelper.IAMAuthenticationType = IamHelper.IAMAuthenticationType.NONE
        if isinstance(provider, IPlugin):
            provider_type = IamHelper.IAMAuthenticationType.PLUGIN
        elif isinstance(provider, AWSCredentialsProvider):
            if provider.profile is not None:
                provider_type = IamHelper.IAMAuthenticationType.PROFILE
            elif provider.session_token is not None:
                provider_type = IamHelper.IAMAuthenticationType.IAM_KEYS_WITH_SESSION
            else:
                provider_type = IamHelper.IAMAuthenticationType.IAM_KEYS

        return provider_type

    @staticmethod
    def set_cluster_credentials(
        cred_provider: typing.Union[IPlugin, AWSCredentialsProvider], info: RedshiftProperty
    ) -> None:
        """
        Calls the AWS SDK methods to return temporary credentials.
        The expiration date is returned as the local time set by the client machines OS.
        """
        import boto3  # type: ignore
        import botocore  # type: ignore

        try:
            credentials_holder: typing.Union[
                CredentialsHolder, ABCAWSCredentialsHolder
            ] = cred_provider.get_credentials()  # type: ignore
            session_credentials: typing.Dict[str, str] = credentials_holder.get_session_credentials()

            redshift_client: str = "redshift-serverless" if info._is_serverless else "redshift"
            _logger.debug("boto3.client(service_name={}) being used for IAM auth".format(redshift_client))

            for opt_key, opt_val in (("region_name", info.region), ("endpoint_url", info.endpoint_url)):
                if opt_val is not None:
                    session_credentials[opt_key] = opt_val

            # if AWS credentials were used to create a boto3.Session object, use it
            if credentials_holder.has_associated_session:
                cached_session: boto3.Session = typing.cast(
                    ABCAWSCredentialsHolder, credentials_holder
                ).get_boto_session()
                client = cached_session.client(service_name=redshift_client, region_name=info.region)
            else:
                client = boto3.client(service_name=redshift_client, **session_credentials)

            if info.host is None or info.host == "" or info.port is None or info.port == "":
                response: dict

                if info._is_serverless:
                    if not info.serverless_work_group:
                        raise InterfaceError("Serverless workgroup is not set.")
                    response = client.get_workgroup(workgroupName=info.serverless_work_group)
                    info.put("host", response["workgroup"]["endpoint"]["address"])
                    info.put("port", response["workgroup"]["endpoint"]["port"])
                else:
                    response = client.describe_clusters(ClusterIdentifier=info.cluster_identifier)
                    info.put("host", response["Clusters"][0]["Endpoint"]["Address"])
                    info.put("port", response["Clusters"][0]["Endpoint"]["Port"])

            cred: typing.Optional[typing.Dict[str, typing.Union[str, datetime.datetime]]] = None

            if info.iam_disable_cache is False:
                _logger.debug("iam_disable_cache=False")
                # temporary credentials are cached by redshift_connector and will be used if they have not expired
                cache_key: str = IamHelper.get_credentials_cache_key(info, cred_provider)
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
                _logger.debug("Credentials expired or not found...requesting from boto")
                provider_type: IamHelper.IAMAuthenticationType = IamHelper.get_authentication_type(cred_provider)
                get_creds_api_version: IamHelper.GetClusterCredentialsAPIType = (
                    IamHelper.get_cluster_credentials_api_type(info, provider_type)
                )
                _logger.debug("boto3 get_credentials api version: {} will be used".format(get_creds_api_version.value))

                if get_creds_api_version == IamHelper.GetClusterCredentialsAPIType.SERVERLESS_V1:
                    get_cred_args: typing.Dict[str, str] = {"dbName": info.db_name}
                    if info.serverless_work_group:
                        get_cred_args["workgroupName"] = info.serverless_work_group

                    cred = typing.cast(
                        typing.Dict[str, typing.Union[str, datetime.datetime]],
                        client.get_credentials(**get_cred_args),
                    )
                    # re-map expiration for compatibility with redshift credential response
                    cred["Expiration"] = cred["expiration"]
                    del cred["expiration"]
                elif get_creds_api_version == IamHelper.GetClusterCredentialsAPIType.IAM_V2:
                    cred = typing.cast(
                        typing.Dict[str, typing.Union[str, datetime.datetime]],
                        client.get_cluster_credentials_with_iam(
                            DbName=info.db_name,
                            ClusterIdentifier=info.cluster_identifier,
                            DurationSeconds=info.duration,
                        ),
                    )
                else:
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
            # redshift-serverless api json response payload slightly differs
            if info._is_serverless:
                info.put("user_name", typing.cast(str, cred["dbUser"]))
                info.put("password", typing.cast(str, cred["dbPassword"]))
            else:
                info.put("user_name", typing.cast(str, cred["DbUser"]))
                info.put("password", typing.cast(str, cred["DbPassword"]))

            _logger.debug("Using temporary aws credentials with expiration: {}".format(cred.get("Expiration")))

        except botocore.exceptions.ClientError as e:
            _logger.error("ClientError: %s", e)
            raise e
        except Exception as e:
            _logger.error("other Exception: %s", e)
            raise e
