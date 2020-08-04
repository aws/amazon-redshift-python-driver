from typing import List, Optional


class RedshiftProperty:
    iam: bool = False
    # The IdP (identity provider) host you are using to authenticate into Redshift.
    idp_host: Optional[str] = None
    # The port used by an IdP (identity provider).
    idpPort: int = 443
    # The length of time, in seconds
    duration: int = 900
    # The IAM role you want to assume during the connection to Redshift.
    preferred_role: Optional[str] = None
    # The Amazon Resource Name (ARN) of the SAML provider in IAM that describes the IdP.
    principal: Optional[str] = None
    # This property indicates whether the IDP hosts server certificate should be verified.
    sslInsecure: bool = True
    # The Okta-provided unique ID associated with your Redshift application.
    app_id: Optional[str] = None
    # The name of the Okta application that you use to authenticate the connection to Redshift.
    app_name: str = 'amazon_aws_redshift'
    # The class path to a specific credentials provider plugin class.
    credentials_provider: Optional[str] = None
    # A comma-separated list of existing database group names that the DbUser joins for the current session.
    # If not specified, defaults to PUBLIC.
    db_groups: Optional[List[str]] = None
    # Forces the database group names to be lower case.
    force_lowercase: bool = False
    # This option specifies whether the driver uses the DbUser value from the SAML assertion
    # or the value that is specified in the DbUser connection property in the connection URL.
    allow_db_user_override: bool = False
    # Indicates whether the user should be created if not exists.
    auto_create: bool = False
    # The AWS region where the cluster specified by m_cluster_identifier is located.
    region: Optional[str] = None
    # The name of the Redshift Cluster to use.
    cluster_identifier: Optional[str] = None
    # The client ID associated with the user name in the Azure AD portal. Only used for Azure AD.
    client_id: Optional[str] = None
    # The Azure AD tenant ID for your Redshift application.Only used for Azure AD.
    idp_tenant: Optional[str] = None
    # The client secret as sociated with the client ID in the AzureAD portal. Only used for Azure AD.
    client_secret: Optional[str] = None
    # The user ID to use with your Redshift account.
    db_user: Optional[str] = None
    # The host to connect to.
    host: str = 'localhost'
    # database name
    db_name: Optional[str] = None
    # The port to connect to.
    port: int = 5432
    # The user name.
    user_name: Optional[str] = None
    # The password.
    password: Optional[str] = None
    # The source IP address which initiates the connection to the PostgreSQL server.
    source_address: Optional[str] = None
    # The path to the UNIX socket to access the database through
    unix_sock: Optional[str] = None
    # if use ssl authentication
    ssl: bool = True
    # ssl mode: verify-ca or verfy-full.
    sslmode: str = "verify-ca"
    # This is the time in seconds before the connection to the server will time out.
    timeout: Optional[int] = None
    # max number of prepared statements
    max_prepared_statements: int = 1000
    # Use this property to enable or disable TCP keepalives. The following values are possible:
    tcp_keepalive: bool = True
    # application name
    application_name: Optional[str] = None
    # Used to run in streaming replication mode. If your server character encoding is not ascii or utf8,
    # then you need to provide values as bytes
    replication: Optional[str] = None
    # parameter for PingIdentity
    partner_sp_id: Optional[str] = None
    # parameters for BrowserIDP
    idp_response_timeout: int = 120
    listen_port: int = 7890
    login_url: Optional[str] = None
