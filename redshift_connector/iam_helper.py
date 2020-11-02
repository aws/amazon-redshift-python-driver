import logging
import typing
from enum import Enum

import boto3  # type: ignore
import botocore  # type: ignore

from redshift_connector.credentials_holder import CredentialsHolder
from redshift_connector.error import InterfaceError
from redshift_connector.plugin import (
    AdfsCredentialsProvider,
    AzureCredentialsProvider,
    BrowserAzureCredentialsProvider,
    BrowserSamlCredentialsProvider,
    OktaCredentialsProvider,
    PingCredentialsProvider,
    SamlCredentialsProvider,
)
from redshift_connector.redshift_property import RedshiftProperty

logger = logging.getLogger(__name__)


class SSLMode(Enum):
    VERIFY_CA: str = "verify-ca"
    VERIFY_FULL: str = "verify-full"


# Helper function to handle IAM connection properties. If any IAM related connection property
# is specified, all other <b>required</b> IAM properties must be specified
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
    db_groups: typing.Optional[typing.List[str]],
    force_lowercase: bool,
    allow_db_user_override: bool,
) -> None:
    if info is None:
        raise InterfaceError("Invalid connection property setting. info must be specified")
    # IAM requires an SSL connection to work.
    # Make sure that is set to SSL level VERIFY_CA or higher.
    info.ssl = ssl
    if info.ssl is True:
        if sslmode == SSLMode.VERIFY_CA.value:
            info.sslmode = SSLMode.VERIFY_CA.value
        elif sslmode == SSLMode.VERIFY_FULL.value:
            info.sslmode = SSLMode.VERIFY_FULL.value
        else:
            info.sslmode = SSLMode.VERIFY_CA.value
    else:
        info.sslmode = ""

    if (info.ssl is False) and (iam is True):
        raise InterfaceError("Invalid connection property setting. SSL must be enabled when using IAM")
    else:
        info.iam = iam

    if (info.iam is False) and (credentials_provider is not None):
        raise InterfaceError(
            "Invalid connection property setting. IAM must be enabled when using credentials " "via identity provider"
        )
    elif (info.iam is True) and (credentials_provider is None):
        raise InterfaceError(
            "Invalid connection property setting. " "Credentials provider cannot be None when IAM is enabled"
        )
    else:
        info.credentials_provider = credentials_provider

    if user is None:
        raise InterfaceError("Invalid connection property setting. user must be specified")
    if host is None:
        raise InterfaceError("Invalid connection property setting. host must be specified")
    if database is None:
        raise InterfaceError("Invalid connection property setting. database must be specified")
    if port is None:
        raise InterfaceError("Invalid connection property setting. port must be specified")
    if password is None:
        raise InterfaceError("Invalid connection property setting. password must be specified")

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

    # Azure specified parameters
    info.client_id = client_id
    info.idp_tenant = idp_tenant
    info.client_secret = client_secret

    # Browser idp parameters
    info.idp_response_timeout = idp_response_timeout
    info.listen_port = listen_port
    info.login_url = login_url
    info.partner_sp_id = partner_sp_id

    if info.iam is True:
        set_iam_credentials(info)
    else:
        return


# Helper function to create the appropriate credential providers.
def set_iam_credentials(info: RedshiftProperty) -> None:
    provider: typing.Optional[SamlCredentialsProvider] = None
    # case insensitive comparison
    if info.credentials_provider is None:
        return
    elif info.credentials_provider.lower() == "OktaCredentialsProvider".lower():
        provider = OktaCredentialsProvider()
        provider.add_parameter(info)
    elif info.credentials_provider.lower() == "AzureCredentialsProvider".lower():
        provider = AzureCredentialsProvider()
        provider.add_parameter(info)
    elif info.credentials_provider.lower() == "BrowserAzureCredentialsProvider".lower():
        provider = BrowserAzureCredentialsProvider()
        provider.add_parameter(info)
    elif info.credentials_provider.lower() == "PingCredentialsProvider".lower():
        provider = PingCredentialsProvider()
        provider.add_parameter(info)
    elif info.credentials_provider.lower() == "BrowserSamlCredentialsProvider".lower():
        provider = BrowserSamlCredentialsProvider()
        provider.add_parameter(info)
    elif info.credentials_provider.lower() == "AdfsCredentialsProvider".lower():
        provider = AdfsCredentialsProvider()
        provider.add_parameter(info)
    else:
        raise InterfaceError("Invalid credentials provider" + info.credentials_provider)

    credentials: CredentialsHolder = provider.get_credentials()
    metadata: CredentialsHolder.IamMetadata = credentials.get_metadata()
    if metadata is not None:
        auto_create: bool = metadata.get_auto_create()
        db_user: typing.Optional[str] = metadata.get_db_user()
        saml_db_user: typing.Optional[str] = metadata.get_saml_db_user()
        profile_db_user: typing.Optional[str] = metadata.get_profile_db_user()
        db_groups: typing.Optional[str] = metadata.get_db_groups()
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

        if (info.db_groups is None) and (db_groups is not None):
            tmp: typing.List[str] = db_groups.split(",")
            info.db_groups = [group.lower() for group in tmp]

    set_cluster_credentials(provider, info)


# Calls the AWS SDK methods to return temporary credentials.
# The expiration date is returned as the local time set by the client machines OS.
def set_cluster_credentials(cred_provider: SamlCredentialsProvider, info: RedshiftProperty) -> None:
    try:
        credentials: CredentialsHolder = cred_provider.get_credentials()
        client = boto3.client(
            "redshift",
            region_name=info.region,
            aws_access_key_id=credentials.get_aws_access_key_id(),
            aws_secret_access_key=credentials.get_aws_secret_key(),
            aws_session_token=credentials.get_session_token(),
        )
        info.host, info.port = client.describe_clusters(ClusterIdentifier=info.cluster_identifier)["Clusters"][0][
            "Endpoint"
        ].values()

        cred: dict = client.get_cluster_credentials(
            DbUser=info.db_user,
            DbName=info.db_name,
            ClusterIdentifier=info.cluster_identifier,
            AutoCreate=info.auto_create,
        )
        info.user_name = cred["DbUser"]
        info.password = cred["DbPassword"]
    except botocore.exceptions.ClientError as e:
        logger.error("ClientError: %s", e)
        raise e
    except client.exceptions.ClusterNotFoundFault as e:
        logger.error("ClusterNotFoundFault: %s", e)
        raise e
    except client.exceptions.UnsupportedOperationFault as e:
        logger.error("UnsupportedOperationFault: %s", e)
        raise e
    except botocore.exceptions.EndpointConnectionError as e:
        logger.error("EndpointConnectionError: %s", e)
        raise e
    except Exception as e:
        logger.error("other Exception: %s", e)
        raise e
