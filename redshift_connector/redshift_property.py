import typing
from typing import TYPE_CHECKING

from redshift_connector.config import DEFAULT_PROTOCOL_VERSION

if TYPE_CHECKING:
    from redshift_connector.iam_helper import SSLMode


class RedshiftProperty:
    iam: bool = False
    # The IdP (identity provider) host you are using to authenticate into Redshift.
    idp_host: typing.Optional[str] = None
    # The port used by an IdP (identity provider).
    idpPort: int = 443
    # The length of time, in seconds
    duration: int = 900
    # The IAM role you want to assume during the connection to Redshift.
    preferred_role: typing.Optional[str] = None
    # The Amazon Resource Name (ARN) of the SAML provider in IAM that describes the IdP.
    principal: typing.Optional[str] = None
    # This property indicates whether the IDP hosts server certificate should be verified.
    sslInsecure: bool = True
    # The Okta-provided unique ID associated with your Redshift application.
    app_id: typing.Optional[str] = None
    # The name of the Okta application that you use to authenticate the connection to Redshift.
    app_name: str = "amazon_aws_redshift"
    # The access key for the IAM role or IAM user configured for IAM database authentication
    access_key_id: typing.Optional[str] = None
    # The secret access key for the IAM role or IAM user configured for IAM database authentication
    secret_access_key: typing.Optional[str] = None
    # session_token is required only for an IAM role with temporary credentials.
    # session_token is not used for an IAM user.
    session_token: typing.Optional[str] = None
    # The name of a profile in a AWS credentials or config file that contains values for connection options
    profile: typing.Optional[str] = None
    # The class path to a specific credentials provider plugin class.
    credentials_provider: typing.Optional[str] = None
    # A comma-separated list of existing database group names that the DbUser joins for the current session.
    # If not specified, defaults to PUBLIC.
    db_groups: typing.List[str] = list()
    # Forces the database group names to be lower case.
    force_lowercase: bool = False
    # This option specifies whether the driver uses the DbUser value from the SAML assertion
    # or the value that is specified in the DbUser connection property in the connection URL.
    allow_db_user_override: bool = False
    # Indicates whether the user should be created if not exists.
    auto_create: bool = False
    # The AWS region where the cluster specified by m_cluster_identifier is located.
    region: typing.Optional[str] = None
    # The name of the Redshift Cluster to use.
    cluster_identifier: typing.Optional[str] = None
    # The client ID associated with the user name in the Azure AD portal. Only used for Azure AD.
    client_id: typing.Optional[str] = None
    # The Azure AD tenant ID for your Redshift application.Only used for Azure AD.
    idp_tenant: typing.Optional[str] = None
    # The client secret as associated with the client ID in the AzureAD portal. Only used for Azure AD.
    client_secret: typing.Optional[str] = None
    # The user ID to use with your Redshift account.
    db_user: typing.Optional[str] = None
    # The host to connect to.
    host: str = "localhost"
    # database name
    db_name: str
    # The port to connect to.
    port: int = 5439
    # The user name.
    user_name: str
    # The password.
    password: str
    # The source IP address which initiates the connection to the PostgreSQL server.
    source_address: typing.Optional[str] = None
    # The path to the UNIX socket to access the database through
    unix_sock: typing.Optional[str] = None
    # if use ssl authentication
    ssl: bool = True
    # ssl mode: verify-ca or verify-full.
    sslmode: str = "verify-ca"
    # This is the time in seconds before the connection to the server will time out.
    timeout: typing.Optional[int] = None
    # max number of prepared statements
    max_prepared_statements: int = 1000
    # Use this property to enable or disable TCP keepalives. The following values are possible:
    tcp_keepalive: bool = True
    # client's requested transfer protocol version. See config.py for supported protocols
    client_protocol_version: int = DEFAULT_PROTOCOL_VERSION
    # Boolean indicating if application supports multidatabase datashare catalogs. Default value of True indicates the
    # application is does not support multidatabase datashare catalogs for backwards compatibility.
    database_metadata_current_db_only: bool = True
    # application name
    application_name: typing.Optional[str] = None
    # Used to run in streaming replication mode. If your server character encoding is not ascii or utf8,
    # then you need to provide values as bytes
    replication: typing.Optional[str] = None
    # parameter for PingIdentity
    partner_sp_id: typing.Optional[str] = None
    # parameters for BrowserIDP
    idp_response_timeout: int = 120
    listen_port: int = 7890
    login_url: typing.Optional[str] = None
