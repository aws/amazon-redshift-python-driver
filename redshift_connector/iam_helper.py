import datetime
import logging
import typing

from dateutil.tz import tzutc

from redshift_connector.auth.aws_credentials_provider import AWSCredentialsProvider
from redshift_connector.credentials_holder import (
    ABCAWSCredentialsHolder,
    AWSDirectCredentialsHolder,
    AWSProfileCredentialsHolder,
    CredentialsHolder,
)
from redshift_connector.error import InterfaceError, ProgrammingError
from redshift_connector.idp_auth_helper import IdpAuthHelper, dynamic_plugin_import
from redshift_connector.native_plugin_helper import NativeAuthPluginHelper
from redshift_connector.plugin.i_native_plugin import INativePlugin
from redshift_connector.plugin.saml_credentials_provider import SamlCredentialsProvider
from redshift_connector.redshift_property import RedshiftProperty

_logger: logging.Logger = logging.getLogger(__name__)


class IamHelper(IdpAuthHelper):

    credentials_cache: typing.Dict[str, dict] = {}

    @staticmethod
    def set_iam_properties(info: RedshiftProperty) -> RedshiftProperty:
        """
        Helper function to handle connection properties and ensure required parameters are specified.
        Parameters
        """
        # set properties present for both IAM, Native authentication
        IamHelper.set_auth_properties(info)

        if info.is_serverless_host and info.iam:
            raise ProgrammingError("This feature is not yet available")
            # if Version(pkg_resources.get_distribution("boto3").version) <= Version("1.20.22"):
            #     raise pkg_resources.VersionConflict(
            #         "boto3 >= XXX required for authentication with Amazon Redshift serverless. "
            #         "Please upgrade the installed version of boto3 to use this functionality."
            #     )

        if info.is_serverless_host:
            info.set_account_id_from_host()
            info.set_region_from_host()

        if info.iam is True:
            if info.cluster_identifier is None and not info.is_serverless_host:
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

        if not isinstance(provider, INativePlugin):
            IamHelper.set_cluster_credentials(provider, info)

    @staticmethod
    def get_credentials_cache_key(
        info: RedshiftProperty, cred_provider: typing.Union[SamlCredentialsProvider, AWSCredentialsProvider]
    ):
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
                    typing.cast(str, info.account_id if info.is_serverless_host else info.cluster_identifier),
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

            redshift_client: str = "redshift-serverless" if info.is_serverless_host else "redshift"
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

                if info.is_serverless_host:
                    response = client.describe_configuration()
                    info.put("host", response["endpoint"]["address"])
                    info.put("port", response["endpoint"]["port"])
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
                if info.is_serverless_host:
                    cred = typing.cast(
                        typing.Dict[str, typing.Union[str, datetime.datetime]],
                        client.get_credentials(
                            dbName=info.db_name,
                        ),
                    )
                    # re-map expiration for compatibility with redshift credential response
                    cred["Expiration"] = cred["expiration"]
                    del cred["expiration"]
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
            if info.is_serverless_host:
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
