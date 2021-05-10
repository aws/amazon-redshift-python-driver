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
from redshift_connector.error import InterfaceError
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
    def set_iam_properties(
        info: RedshiftProperty,
        user: str,
        host: str,
        database: str,
        port: int,
        password: str,
        source_address: typing.Optional[str],
        unix_sock: typing.Optional[str],
        ssl: bool,
        sslmode: str,
        timeout: typing.Optional[int],
        max_prepared_statements: int,
        tcp_keepalive: bool,
        application_name: typing.Optional[str],
        replication: typing.Optional[str],
        idp_host: typing.Optional[str],
        db_user: typing.Optional[str],
        iam: bool,
        app_id: typing.Optional[str],
        app_name: str,
        preferred_role: typing.Optional[str],
        principal_arn: typing.Optional[str],
        access_key_id: typing.Optional[str],
        secret_access_key: typing.Optional[str],
        session_token: typing.Optional[str],
        profile: typing.Optional[str],
        credentials_provider: typing.Optional[str],
        region: typing.Optional[str],
        cluster_identifier: typing.Optional[str],
        client_id: typing.Optional[str],
        idp_tenant: typing.Optional[str],
        client_secret: typing.Optional[str],
        partner_sp_id: typing.Optional[str],
        idp_response_timeout: int,
        listen_port: int,
        login_url: typing.Optional[str],
        auto_create: bool,
        db_groups: typing.List[str],
        force_lowercase: bool,
        allow_db_user_override: bool,
        client_protocol_version: int,
        database_metadata_current_db_only: bool,
        ssl_insecure: typing.Optional[bool],
        web_identity_token: typing.Optional[str],
        role_session_name: typing.Optional[str],
        role_arn: typing.Optional[str],
    ) -> None:
        """
        Helper function to handle IAM connection properties and ensure required parameters are specified.
        Parameters
        """
        if info is None:
            raise InterfaceError("Invalid connection property setting. info must be specified")
        # IAM requires an SSL connection to work.
        # Make sure that is set to SSL level VERIFY_CA or higher.
        info.ssl = ssl
        if info.ssl is True:
            if sslmode not in SupportedSSLMode.list():
                info.sslmode = SupportedSSLMode.default()
                _logger.debug(
                    "A non-supported value: {} was provides for sslmode. Falling back to default value: {}".format(
                        sslmode, SupportedSSLMode.default()
                    )
                )
            else:
                info.sslmode = sslmode
        else:
            info.sslmode = ""

        if (info.ssl is False) and (iam is True):
            raise InterfaceError("Invalid connection property setting. SSL must be enabled when using IAM")
        else:
            info.iam = iam

        if (info.iam is False) and (ssl_insecure is not None):
            raise InterfaceError("Invalid connection property setting. IAM must be enabled when using ssl_insecure")
        elif (info.iam is False) and any(
            (credentials_provider, access_key_id, secret_access_key, session_token, profile)
        ):
            raise InterfaceError(
                "Invalid connection property setting. IAM must be enabled when using credential_provider, "
                "AWS credentials, or AWS profile"
            )
        elif info.iam is True:
            if cluster_identifier is None:
                raise InterfaceError(
                    "Invalid connection property setting. cluster_identifier must be provided when IAM is enabled"
                )
            if not any((credentials_provider, access_key_id, secret_access_key, session_token, profile)):
                raise InterfaceError(
                    "Invalid connection property setting. Credentials provider, AWS credentials, or AWS profile must be"
                    " provided when IAM is enabled"
                )
            elif credentials_provider is not None:
                if any((access_key_id, secret_access_key, session_token, profile)):
                    raise InterfaceError(
                        "Invalid connection property setting. It is not valid to provide both Credentials provider and "
                        "AWS credentials or AWS profile"
                    )
                elif not isinstance(credentials_provider, str):
                    raise InterfaceError(
                        "Invalid connection property setting. It is not valid to provide a non-string value to "
                        "credentials_provider."
                    )
                else:
                    info.credentials_provider = credentials_provider
            elif profile is not None:
                if any((access_key_id, secret_access_key, session_token)):
                    raise InterfaceError(
                        "Invalid connection property setting. It is not valid to provide any of access_key_id, "
                        "secret_access_key, or session_token when profile is provided"
                    )
                else:
                    info.profile = profile
            elif access_key_id is not None:
                info.access_key_id = access_key_id

                if secret_access_key is not None:
                    info.secret_access_key = secret_access_key
                # secret_access_key can be provided in the password field so it is hidden from applications such as
                # SQL Workbench.
                elif password != "":
                    info.secret_access_key = password
                else:
                    raise InterfaceError(
                        "Invalid connection property setting. "
                        "secret access key must be provided in either secret_access_key or password field"
                    )

                if session_token is not None:
                    info.session_token = session_token
            elif secret_access_key is not None:
                raise InterfaceError(
                    "Invalid connection property setting. access_key_id is required when secret_access_key is "
                    "provided"
                )
            elif session_token is not None:
                raise InterfaceError(
                    "Invalid connection property setting. access_key_id and secret_access_key are  required when "
                    "session_token is provided"
                )

        if not all((user, host, database, port, password)):
            if not any((profile, access_key_id, credentials_provider)):
                raise InterfaceError(
                    "Invalid connection property setting. "
                    "user, password, host, database and port are required "
                    "when not using AWS credentials and AWS profile"
                )

        if client_protocol_version not in ClientProtocolVersion.list():
            raise InterfaceError(
                "Invalid connection property setting. client_protocol_version must be in: {}".format(
                    ClientProtocolVersion.list()
                )
            )

        # basic driver parameters
        info.user_name = user
        info.host = host
        info.db_name = database
        info.port = port
        info.password = password
        info.source_address = source_address
        info.unix_sock = unix_sock
        info.timeout = timeout
        info.max_prepared_statements = max_prepared_statements
        info.tcp_keepalive = tcp_keepalive
        info.application_name = application_name
        info.replication = replication
        info.client_protocol_version = client_protocol_version
        info.database_metadata_current_db_only = database_metadata_current_db_only

        # Idp parameters
        info.idp_host = idp_host
        info.db_user = db_user
        info.app_id = app_id
        info.app_name = app_name
        info.preferred_role = preferred_role
        info.principal = principal_arn
        # Regions.fromName(string) requires the string to be lower case and in this format:
        # E.g. "us-west-2"
        info.region = region
        # cluster_identifier parameter is required
        info.cluster_identifier = cluster_identifier
        info.auto_create = auto_create
        info.db_groups = db_groups
        info.force_lowercase = force_lowercase
        info.allow_db_user_override = allow_db_user_override

        if ssl_insecure is not None:
            info.sslInsecure = ssl_insecure

        # Azure specified parameters
        info.client_id = client_id
        info.idp_tenant = idp_tenant
        info.client_secret = client_secret

        # Browser idp parameters
        info.idp_response_timeout = idp_response_timeout
        info.listen_port = listen_port
        info.login_url = login_url
        info.partner_sp_id = partner_sp_id

        # Jwt idp parameters
        info.web_identity_token = web_identity_token
        info.role_session_name = role_session_name
        info.role_arn = role_arn

        if info.iam is True:
            IamHelper.set_iam_credentials(info)
        else:
            return

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
                    info.credentials_provider = predefined_idp
                except (AttributeError, ModuleNotFoundError):
                    _logger.debug(
                        "Failed to load pre-defined IdP plugin from redshift_connector.plugin: {}".format(
                            info.credentials_provider
                        )
                    )
                    raise InterfaceError("Invalid credentials provider " + info.credentials_provider)
        else:  # indicates AWS Credentials will be used
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
                    info.auto_create = auto_create

                if force_lowercase is True:
                    info.force_lowercase = force_lowercase

                if allow_db_user_override is True:
                    if saml_db_user is not None:
                        info.db_user = saml_db_user
                    if db_user is not None:
                        info.db_user = db_user
                    if profile_db_user is not None:
                        info.db_user = profile_db_user
                else:
                    if db_user is not None:
                        info.db_user = db_user
                    if profile_db_user is not None:
                        info.db_user = profile_db_user
                    if saml_db_user is not None:
                        info.db_user = saml_db_user

                if (len(info.db_groups) == 0) and (len(db_groups) > 0):
                    info.db_groups = db_groups

        IamHelper.set_cluster_credentials(provider, info)

    @staticmethod
    def get_credentials_cache_key(info: RedshiftProperty):
        db_groups: str = ""

        if len(info.db_groups) > 0:
            info.db_groups = sorted(info.db_groups)
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

            if info.region is not None:
                session_credentials["region_name"] = info.region

            # if AWS credentials were used to create a boto3.Session object, use it
            if credentials_holder.has_associated_session:
                cached_session: boto3.Session = typing.cast(
                    ABCAWSCredentialsHolder, credentials_holder
                ).get_boto_session()
                client = cached_session.client(service_name="redshift", region_name=info.region)
            else:
                client = boto3.client(service_name="redshift", **session_credentials)

            response = client.describe_clusters(ClusterIdentifier=info.cluster_identifier)
            info.host = response["Clusters"][0]["Endpoint"]["Address"]
            info.port = response["Clusters"][0]["Endpoint"]["Port"]

            # temporary credentials are cached by redshift_connector and will be used if they have not expired
            cache_key: str = IamHelper.get_credentials_cache_key(info)
            cred: typing.Optional[
                typing.Dict[str, typing.Union[str, datetime.datetime]]
            ] = IamHelper.credentials_cache.get(cache_key, None)

            _logger.debug(
                "Searching credential cache for temporary AWS credentials. Found: {} Expiration: {}".format(
                    bool(cache_key in IamHelper.credentials_cache), cred["Expiration"] if cred is not None else "N/A"
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

                IamHelper.credentials_cache[cache_key] = typing.cast(
                    typing.Dict[str, typing.Union[str, datetime.datetime]], cred
                )

            info.user_name = typing.cast(str, cred["DbUser"])
            info.password = typing.cast(str, cred["DbPassword"])

            _logger.debug("Using temporary aws credentials with expiration: {}".format(cred["Expiration"]))

        except botocore.exceptions.ClientError as e:
            _logger.error("ClientError: %s", e)
            raise e
        except client.exceptions.ClusterNotFoundFault as e:
            _logger.error("ClusterNotFoundFault: %s", e)
            raise e
        except client.exceptions.UnsupportedOperationFault as e:
            _logger.error("UnsupportedOperationFault: %s", e)
            raise e
        except botocore.exceptions.EndpointConnectionError as e:
            _logger.error("EndpointConnectionError: %s", e)
            raise e
        except Exception as e:
            _logger.error("other Exception: %s", e)
            raise e
