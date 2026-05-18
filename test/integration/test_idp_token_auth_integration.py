"""
Integration tests for IdpTokenAuthPlugin with identity enhanced credentials. In order to get the credentials and setup
for these tests, we perform the following:
1. Python Driver Prod account tod role has permissions to assume EspressoRedshiftRole in drivers testing account
   EspressoRedshiftRole has all of the necessary permissions to perform the actions for this test
2. Using EspressoRedshiftRole's credentials, we will get a JWT token, use CreateTokenWIthIAM to get identity context, and then assume
   role with the identity context in order to get the identity enhanced AWS credentials that we can use to run these integration tests

In order to get the identity enhanced credentials, we use the Azure TTI set up with IdC for the drivers testing account to get a JWT token
, which we then call CreateTokenWithIAM with to get the associated identity context. We then call assume role with the identity context
in order to get the identity enhanced credentials.

Cluster endpoints are stored in the secret manager and have been onboarded to an IdC application with the EspressoRedshiftRole created in
Python Driver Prod account. We have also created test_user_azure user in identity center for positive test cases authentication.
"""

import json
from typing import Any, Dict, Optional
import pytest
import requests
import boto3
from botocore.exceptions import ClientError

import redshift_connector


class AzureAuthenticator:
    """Handles Azure AD authentication and AWS credential exchange using boto3.

    Uses boto3 SDK for all AWS API calls, eliminating subprocess/CLI dependency.
    """

    # Secret name in AWS Secrets Manager
    PROD_SECRET_NAME = "IdpToken-Python-Testing-Credentials"
    SECRET_NAME = "test/smus-tip-credentials"
    SECRET_REGION = "us-east-1"

    def __init__(self):
        # Fields for IDC
        self.iam_role = self._get_assume_role_arn()
        self.region = "us-east-1"

        # boto3 clients with assumed role credentials
        self._sts_client = None
        self._secretsmanager_client = None
        self._sso_oidc_client = None

        # Step 1: Assume base role to get credentials for subsequent AWS API calls
        self._assume_base_role()

        # Step 2: Fetch secrets using assumed role credentials from testing account
        secrets = self._get_secrets()

        self.idc_app_arn = secrets['idc_app_arn']
        self.TEST_AZURE_TENANT_ID = secrets['azure_tenant_id']
        self.TEST_AZURE_APP_ID = secrets['azure_app_id']
        self.TEST_AZURE_SCOPE = secrets['azure_scope']
        self.TEST_AZURE_CLIENT_SECRET = secrets['azure_client_secret']
        self.username = secrets['username']
        self.user_password = secrets['password']

        # Redshift endpoints from secrets
        self.provisioned_endpoint = secrets['provisioned_endpoint']
        self.serverless_endpoint = secrets['serverless_endpoint']

    def _get_assume_role_arn(self) -> str:
        """Fetch the role arn from secret manager"""
        session = boto3.session.Session()
        client = session.client(service_name="secretsmanager", region_name=self.SECRET_REGION)
        try:
            response = client.get_secret_value(SecretId=self.PROD_SECRET_NAME)
        except ClientError as e:
            raise e
        return json.loads(response['SecretString'])["role_arn"]

    def _assume_base_role(self) -> None:
        """Assume EspressoRedshiftRole to get base credentials for AWS API calls.

        Uses boto3 default credential chain, then creates new clients
        with the assumed role credentials.
        """
        try:
            # Use default credential chain for initial STS client
            sts = boto3.client('sts', region_name=self.region)

            response = sts.assume_role(
                RoleArn=self.iam_role,
                RoleSessionName='integration-test-session'
            )
            creds = response['Credentials']

            # Create boto3 clients with assumed role credentials
            self._sts_client = boto3.client(
                'sts',
                region_name=self.region,
                aws_access_key_id=creds['AccessKeyId'],
                aws_secret_access_key=creds['SecretAccessKey'],
                aws_session_token=creds['SessionToken']
            )

            self._secretsmanager_client = boto3.client(
                'secretsmanager',
                region_name=self.SECRET_REGION,
                aws_access_key_id=creds['AccessKeyId'],
                aws_secret_access_key=creds['SecretAccessKey'],
                aws_session_token=creds['SessionToken']
            )

            self._sso_oidc_client = boto3.client(
                'sso-oidc',
                region_name=self.region,
                aws_access_key_id=creds['AccessKeyId'],
                aws_secret_access_key=creds['SecretAccessKey'],
                aws_session_token=creds['SessionToken']
            )

            print(f"✓ Assumed base role (expires: {creds['Expiration']})")

        except ClientError as e:
            print(f"Failed to assume base role: {e}")
            raise

    def _get_secrets(self) -> Dict[str, str]:
        """Fetch Azure credentials from AWS Secrets Manager using boto3."""
        try:
            response = self._secretsmanager_client.get_secret_value(
                SecretId=self.SECRET_NAME
            )
            return json.loads(response['SecretString'])
        except ClientError as e:
            print(f"Failed to fetch secrets from Secrets Manager: {e}")
            raise

    def get_azure_token(self):
        try:
            url = f'https://login.microsoftonline.com/{self.TEST_AZURE_TENANT_ID}/oauth2/v2.0/token'

            data_payload = {
                'client_id': self.TEST_AZURE_APP_ID,
                'scope': self.TEST_AZURE_SCOPE,
                'grant_type': 'password',
                'username': self.username,
                'password': self.user_password,
                'client_secret': self.TEST_AZURE_CLIENT_SECRET
            }

            resp = requests.post(url, data=data_payload)
            resp.raise_for_status()
            resp_json = json.loads(resp.text)
            return resp_json['access_token']

        except requests.exceptions.RequestException as e:
            print(f"HTTP Request failed: {e}")
            raise
        except json.JSONDecodeError as e:
            print(f"Failed to parse JSON response: {e}")
            raise
        except KeyError as e:
            print(f"Access token not found in response: {e}")
            raise

    def assume_role_with_context(self, context_assertion: str) -> Dict[str, Any]:
        """Assume IAM role with identity context to get identity-enhanced credentials."""
        try:
            response = self._sts_client.assume_role(
                RoleArn=self.iam_role,
                RoleSessionName='integration-test-session',
                ProvidedContexts=[{
                    'ProviderArn': 'arn:aws:iam::aws:contextProvider/IdentityCenter',
                    'ContextAssertion': context_assertion
                }]
            )
            return response
        except ClientError as e:
            print(f"AWS STS assume-role with context failed: {e}")
            raise

    def create_aws_token(self, azure_token: str) -> Dict[str, Any]:
        """Exchange Azure token for AWS token with identity context using boto3."""
        try:
            response = self._sso_oidc_client.create_token_with_iam(
                clientId=self.idc_app_arn,
                grantType='urn:ietf:params:oauth:grant-type:jwt-bearer',
                assertion=azure_token
            )
            return response
        except ClientError as e:
            print(f"AWS SSO-OIDC create_token_with_iam failed: {e}")
            raise


class TestIdpTokenAuthPlugin:
    """Test class for IdpTokenAuthPlugin integration tests."""

    # Cluster configuration constants
    DATABASE = "dev"
    REGION = "us-east-1"

    # Cached credentials and endpoints (class-level to avoid re-fetching)
    _credentials: Optional[Dict[str, str]] = None
    _provisioned_host: Optional[str] = None
    _serverless_host: Optional[str] = None

    @classmethod
    def setup_credentials(cls):
        """Fetch and cache AWS credentials using Azure authentication flow.

        Credentials are cached at class level to avoid re-fetching during test session.
        """
        # Check if credentials are already cached
        if cls._credentials is not None:
            print("✓ Using cached credentials")
            return

        authenticator = AzureAuthenticator()

        # Cache endpoints from secrets
        cls._provisioned_host = authenticator.provisioned_endpoint
        cls._serverless_host = authenticator.serverless_endpoint

        # Step 1: Get Azure token
        azure_token = authenticator.get_azure_token()
        print("✓ Obtained Azure token")

        # Step 2: Exchange Azure token for AWS token with identity context
        aws_response = authenticator.create_aws_token(azure_token)
        print("✓ Got AWS token with identity context")

        # Step 3: Assume role with context to get temporary credentials
        context_assertion = aws_response['awsAdditionalDetails']['identityContext']
        credentials_response = authenticator.assume_role_with_context(context_assertion)
        print("✓ Assumed role successfully")

        # Extract and cache credentials
        creds = credentials_response['Credentials']
        cls._credentials = {
            'access_key_id': creds['AccessKeyId'],
            'secret_access_key': creds['SecretAccessKey'],
            'session_token': creds['SessionToken'],
            'expiration': creds['Expiration']
        }

        print(f"✓ Credentials cached (expires: {creds['Expiration']})")

    def get_connection_params(self, host: str = None):
        """Build connection parameters for the given host.

        Args:
            host: The Redshift host to connect to. If None, defaults to provisioned host.

        Returns:
            dict: Connection parameters for redshift_connector.connect()
        """
        # Ensure credentials are fetched and cached
        if self._credentials is None:
            self.setup_credentials()

        # Default to provisioned host if not specified
        if host is None:
            host = self._provisioned_host

        return {
            "host": host,
            "database": self.DATABASE,
            "credentials_provider": "IdpTokenAuthPlugin",
            "access_key_id": self._credentials['access_key_id'],
            "secret_access_key": self._credentials['secret_access_key'],
            "session_token": self._credentials['session_token'],
        }

    def _get_host_for_type(self, host_type: str) -> str:
        """Get the host URL for the given cluster type.

        Args:
            host_type: Either "provisioned" or "serverless"

        Returns:
            str: The host URL for the specified cluster type
        """
        # Ensure credentials/endpoints are fetched
        if self._credentials is None:
            self.setup_credentials()

        if host_type == "provisioned":
            return self._provisioned_host
        elif host_type == "serverless":
            return self._serverless_host
        else:
            raise ValueError(f"Unknown host_type: {host_type}")

    @pytest.mark.parametrize("host_type", ["provisioned", "serverless"])
    def test_connect_and_execute_select_one(self, host_type: str):
        """Test connecting to Redshift and executing SELECT 1.

        This test validates that IdpTokenAuthPlugin successfully connects to both
        provisioned clusters and serverless workgroups using identity-enhanced credentials.

        """
        host = self._get_host_for_type(host_type)
        conn = redshift_connector.connect(**self.get_connection_params(host))

        try:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            result = cursor.fetchone()

            assert result[0] == 1, f"Expected SELECT 1 to return 1, got {result[0]}"
            print(f"✓ Successfully connected to {host_type} cluster and executed SELECT 1")

            cursor.close()
        finally:
            conn.close()

    @pytest.mark.parametrize("host_type", ["provisioned", "serverless"])
    def test_connect_and_query_current_user(self, host_type: str):
        """Test connecting to Redshift and querying current user.

        This test validates that after successful authentication, the connection
        has a valid user context and can retrieve the current user information.

        """
        host = self._get_host_for_type(host_type)
        conn = redshift_connector.connect(**self.get_connection_params(host))

        try:
            cursor = conn.cursor()
            cursor.execute("SELECT current_user")
            result = cursor.fetchone()

            assert result is not None, "Expected current_user query to return a result"
            assert result[0] is not None, "Expected current_user to not be None"
            assert len(result[0]) > 0, "Expected current_user to not be empty"
            print(f"✓ Successfully connected to {host_type} cluster, current_user: {result[0]}")

            cursor.close()
        finally:
            conn.close()

    @pytest.mark.parametrize("host_type", ["provisioned", "serverless"])
    def test_connect_and_execute_multiple_queries(self, host_type: str):
        """Test executing multiple queries on the same cursor.

        This test validates that a connection established via IdpTokenAuthPlugin
        can execute multiple queries sequentially on the same cursor without issues.

        """
        host = self._get_host_for_type(host_type)
        conn = redshift_connector.connect(**self.get_connection_params(host))

        try:
            cursor = conn.cursor()

            # Execute first query
            cursor.execute("SELECT 1")
            result1 = cursor.fetchone()
            assert result1[0] == 1, f"First query failed: expected 1, got {result1[0]}"

            # Execute second query
            cursor.execute("SELECT 2 + 2")
            result2 = cursor.fetchone()
            assert result2[0] == 4, f"Second query failed: expected 4, got {result2[0]}"

            # Execute third query
            cursor.execute("SELECT current_database()")
            result3 = cursor.fetchone()
            assert result3[0] is not None, "Third query failed: expected database name"

            print(f"✓ Successfully executed 3 queries on {host_type} cluster")

            cursor.close()
        finally:
            conn.close()

    # ==================== Invalid Credentials Failure Tests ====================

    def test_invalid_access_key_id_fails(self):
        """Test that connection fails when AccessKeyId is invalid.

        This test validates that IdpTokenAuthPlugin properly fails when provided
        with an invalid AccessKeyId. The error originates from the AWS API call
        (GetIdentityCenterAuthToken) and is wrapped in an InterfaceError.

        """
        # Get valid connection params and replace AccessKeyId with invalid value
        params = self.get_connection_params(self._provisioned_host)
        params['access_key_id'] = 'INVALID_ACCESS_KEY_ID_12345'

        with pytest.raises(
            redshift_connector.InterfaceError,
            match="IdC authentication failed: Failed to obtain token from GetIdentityCenterAuthToken"
        ):
            conn = redshift_connector.connect(**params)
            # If connection somehow succeeds, try to execute a query to trigger auth failure
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            conn.close()

    def test_invalid_secret_access_key_fails(self):
        """Test that connection fails when SecretAccessKey is invalid.

        This test validates that IdpTokenAuthPlugin properly fails when provided
        with an invalid SecretAccessKey. The error originates from the AWS API call
        (GetIdentityCenterAuthToken) and is wrapped in an InterfaceError.

        """
        # Get valid connection params and replace SecretAccessKey with invalid value
        params = self.get_connection_params(self._provisioned_host)
        params['secret_access_key'] = 'INVALID_SECRET_ACCESS_KEY_12345'

        with pytest.raises(
            redshift_connector.InterfaceError,
            match="IdC authentication failed: Failed to obtain token from GetIdentityCenterAuthToken"
        ):
            conn = redshift_connector.connect(**params)
            # If connection somehow succeeds, try to execute a query to trigger auth failure
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            conn.close()

    def test_invalid_session_token_fails(self):
        """Test that connection fails when SessionToken is invalid.

        This test validates that IdpTokenAuthPlugin properly fails when provided
        with an invalid SessionToken. The error originates from the AWS API call
        (GetIdentityCenterAuthToken) and is wrapped in an InterfaceError.

        """
        # Get valid connection params and replace SessionToken with invalid value
        params = self.get_connection_params(self._provisioned_host)
        params['session_token'] = 'INVALID_SESSION_TOKEN_12345'

        with pytest.raises(
            redshift_connector.InterfaceError,
            match="IdC authentication failed: Failed to obtain token from GetIdentityCenterAuthToken"
        ):
            conn = redshift_connector.connect(**params)
            # If connection somehow succeeds, try to execute a query to trigger auth failure
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            conn.close()

    # ==================== Missing Parameters Failure Tests ====================

    def test_missing_access_key_id_fails(self):
        """Test that connection fails when AccessKeyId is missing.

        This test validates that IdpTokenAuthPlugin properly fails when
        AccessKeyId is omitted from connection parameters. Without all three
        identity-enhanced credential params, and without direct token params,
        the plugin raises an error indicating neither auth flow is satisfied.

        """
        # Build params without access_key_id - use dummy values for other params
        # since validation happens before any AWS API calls
        params = {
            "host": "dummy-host.redshift.amazonaws.com",
            "database": self.DATABASE,
            "credentials_provider": "IdpTokenAuthPlugin",
            "secret_access_key": "dummy_secret_access_key",
            "session_token": "dummy_session_token",
        }

        with pytest.raises(
            redshift_connector.InterfaceError,
            match="Either token/token_type or AccessKeyID/SecretAccessKey/SessionToken must be provided"
        ):
            redshift_connector.connect(**params)

    def test_missing_secret_access_key_fails(self):
        """Test that connection fails when SecretAccessKey is missing.

        This test validates that IdpTokenAuthPlugin properly fails when
        SecretAccessKey is omitted from connection parameters. Without all three
        identity-enhanced credential params, and without direct token params,
        the plugin raises an error indicating neither auth flow is satisfied.

        """
        # Build params without secret_access_key - use dummy values for other params
        # since validation happens before any AWS API calls
        params = {
            "host": "dummy-host.redshift.amazonaws.com",
            "database": self.DATABASE,
            "credentials_provider": "IdpTokenAuthPlugin",
            "access_key_id": "dummy_access_key_id",
            "session_token": "dummy_session_token",
        }

        with pytest.raises(
            redshift_connector.InterfaceError,
            match="Either token/token_type or AccessKeyID/SecretAccessKey/SessionToken must be provided"
        ):
            redshift_connector.connect(**params)

    def test_missing_session_token_fails(self):
        """Test that connection fails when SessionToken is missing.

        This test validates that IdpTokenAuthPlugin properly fails when
        SessionToken is omitted from connection parameters. Without all three
        identity-enhanced credential params, and without direct token params,
        the plugin raises an error indicating neither auth flow is satisfied.

        """
        # Build params without session_token - use dummy values for other params
        # since validation happens before any AWS API calls
        params = {
            "host": "dummy-host.redshift.amazonaws.com",
            "database": self.DATABASE,
            "credentials_provider": "IdpTokenAuthPlugin",
            "access_key_id": "dummy_access_key_id",
            "secret_access_key": "dummy_secret_access_key",
        }

        with pytest.raises(
            redshift_connector.InterfaceError,
            match="Either token/token_type or AccessKeyID/SecretAccessKey/SessionToken must be provided"
        ):
            redshift_connector.connect(**params)

    # ==================== Conflicting Parameters Failure Tests ====================

    def test_conflicting_token_and_credentials_params_fails(self):
        """Test that connection fails when both token/token_type AND identity-enhanced credentials are provided.

        This test validates that IdpTokenAuthPlugin properly fails when conflicting
        authentication parameters are provided. The plugin should detect that both
        direct token parameters (token, token_type) and identity-enhanced credentials
        (access_key_id, secret_access_key, session_token) are provided and raise an error.

        The validation happens locally before any AWS API calls, so no real credentials needed.

        """
        # Build params with BOTH token/token_type AND identity-enhanced credentials
        # This is a conflicting configuration that should be rejected
        params = {
            "host": "dummy-host.redshift.amazonaws.com",
            "database": self.DATABASE,
            "credentials_provider": "IdpTokenAuthPlugin",
            # Direct token parameters
            "token": "dummy_direct_token",
            "token_type": "ACCESS_TOKEN",
            # Identity-enhanced credentials parameters
            "access_key_id": "dummy_access_key_id",
            "secret_access_key": "dummy_secret_access_key",
            "session_token": "dummy_session_token",
        }

        with pytest.raises(
            redshift_connector.InterfaceError,
            match="Cannot provide both direct token parameters \\(token, token_type\\) and \\(AccessKeyID, SecretAccessKey, SessionToken\\)"
        ):
            redshift_connector.connect(**params)
