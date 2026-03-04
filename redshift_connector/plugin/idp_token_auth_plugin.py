import logging
import typing

import boto3
from botocore.credentials import Credentials

from redshift_connector.error import InterfaceError
from redshift_connector.plugin.common_credentials_provider import (
    CommonCredentialsProvider,
)
from redshift_connector.redshift_property import RedshiftProperty

logging.getLogger(__name__).addHandler(logging.NullHandler())
_logger: logging.Logger = logging.getLogger(__name__)

class IdpTokenAuthPlugin(CommonCredentialsProvider):
    """
    A basic IdP Token auth plugin class. This plugin class allows clients to directly provide any auth token that is handled by Redshift.
    It also supports identity-enhanced credentials flow where AWS credentials are exchanged for a subject token via GetIdentityCenterAuthToken.
    """

    def __init__(self: "IdpTokenAuthPlugin") -> None:
        super().__init__()
        # Direct token flow parameters
        self.token: typing.Optional[str] = None
        self.token_type: typing.Optional[str] = None

        # Identity-enhanced credentials parameters
        self.access_key_id: typing.Optional[str] = None
        self.secret_access_key: typing.Optional[str] = None
        self.session_token: typing.Optional[str] = None

        # Explicit override parameters
        self.endpoint_url: typing.Optional[str] = None
        self.region: typing.Optional[str] = None

        # Reference to RedshiftProperty for cluster info
        self.redshift_property: typing.Optional[RedshiftProperty] = None

    def add_parameter(
        self: "IdpTokenAuthPlugin",
        info: RedshiftProperty,
    ) -> None:
        super().add_parameter(info)

        # Store reference to RedshiftProperty for cluster info access
        self.redshift_property = info

        # Only direct token flow will have token parameter
        self.token = info.token
        # This will be user inputted token type for direct token flow. For
        # identity-enhanced flow where customer gives identity enhanced
        # credentials, token_type will be set to SUBJECT_TOKEN
        self.token_type = info.token_type
        _logger.debug("Setting token_type = {}".format(self.token_type))

        # Identity-enhanced credentials parameters
        self.access_key_id = info.access_key_id
        self.secret_access_key = info.secret_access_key
        self.session_token = info.session_token
        _logger.debug("Setting identity-enhanced credentials parameters")

        # Explicit override parameters
        self.endpoint_url = info.endpoint_url
        self.region = info.region
        _logger.debug("Setting explicit override parameters: endpoint_url={}, region={}".format(
            self.endpoint_url, self.region
        ))

    def check_required_parameters(self: "IdpTokenAuthPlugin") -> None:
        super().check_required_parameters()

        # Determine which flow is being used
        has_direct_token_params = self.token is not None and self.token_type is not None
        has_identity_enhanced_params = self._is_using_identity_enhanced_credentials()

        # Check for conflicting parameters
        if has_direct_token_params and has_identity_enhanced_params:
            _logger.error("IdC authentication failed: conflicting parameters provided")
            raise InterfaceError(
                "IdC authentication failed: Cannot provide both direct token parameters (token, token_type) "
                "and (AccessKeyID, SecretAccessKey, SessionToken)."
            )

        # If neither flow is provided, raise error
        elif not has_direct_token_params and not has_identity_enhanced_params:
            _logger.error("IdC authentication failed: no authentication parameters provided")
            raise InterfaceError(
                "IdC authentication failed: Either token/token_type or AccessKeyID/SecretAccessKey/SessionToken must be provided."
            )

    def _is_using_identity_enhanced_credentials(self: "IdpTokenAuthPlugin") -> bool:
        """
        Determine if the plugin is using identity-enhanced credentials flow.

        Returns True if identity-enhanced credentials parameters are provided,
        False if using direct token flow.
        """
        return (
            self.access_key_id is not None
            and self.secret_access_key is not None
            and self.session_token is not None
        )

    def _create_aws_credentials(self: "IdpTokenAuthPlugin") -> Credentials:
        """
        Create AWS credentials from AccessKeyID, SecretAccessKey, and SessionToken.

        Returns:
            botocore.credentials.Credentials object with the provided credentials

        Raises:
            InterfaceError: If credential creation fails
        """
        try:
            _logger.debug("Creating AWS credentials from provided parameters")
            credentials = Credentials(
                access_key=self.access_key_id,
                secret_key=self.secret_access_key,
                token=self.session_token,
            )
            _logger.debug("Successfully created AWS credentials")
            return credentials
        except Exception as e:
            _logger.error(f"Failed to create AWS credentials: {str(e)}")
            raise InterfaceError(
                f"Failed to create AWS credentials: {str(e)}"
            )

    def _get_provisioned_auth_token(
        self: "IdpTokenAuthPlugin",
        credentials: Credentials,
        region: str,
        cluster_id: str,
    ) -> str:
        """
        Call GetIdentityCenterAuthToken for provisioned cluster to obtain subject token.

        Args:
            credentials: AWS credentials to use for API call
            region: AWS region for the cluster
            cluster_id: Cluster identifier

        Returns:
            Subject token string from the API response

        Raises:
            InterfaceError: If API call fails or returns empty token
        """
        try:
            _logger.debug(f"Making GetIdentityCenterAuthToken call for provisioned cluster: cluster_id={cluster_id}, region={region}")

            # Create RedshiftClient with provided credentials
            redshift_client = boto3.client(
                "redshift",
                region_name=region,
                aws_access_key_id=credentials.access_key,
                aws_secret_access_key=credentials.secret_key,
                aws_session_token=credentials.token,
                endpoint_url=self.endpoint_url,
            )

            # Call get_identity_center_auth_token API
            response = redshift_client.get_identity_center_auth_token(
                ClusterIds=[cluster_id]
            )

            # Extract subject token from response
            subject_token = response.get("Token")

            if not subject_token:
                _logger.error("GetIdentityCenterAuthToken returned empty token for provisioned workgroup")
                raise InterfaceError(
                    "IdC authentication failed: GetIdentityCenterAuthToken returned an empty token. "
                    "Please verify your credentials and cluster configuration."
                )

            _logger.debug("Successfully obtained subject token from GetIdentityCenterAuthToken for provisioned cluster")
            return subject_token

        except InterfaceError:
            # Re-raise InterfaceError as-is
            raise
        except Exception as e:
            _logger.error(f"GetIdentityCenterAuthToken call failed for provisioned cluster: {str(e)}")
            raise InterfaceError(
                f"IdC authentication failed: Failed to obtain token from GetIdentityCenterAuthToken: {str(e)}"
            )

    def _get_serverless_auth_token(
        self: "IdpTokenAuthPlugin",
        credentials: Credentials,
        region: str,
        workgroup_id: str,
    ) -> str:
        """
        Call GetIdentityCenterAuthToken for serverless workgroup to obtain subject token.

        Args:
            credentials: AWS credentials to use for API call
            region: AWS region for the workgroup
            workgroup_id: Workgroup identifier

        Returns:
            Subject token string from the API response

        Raises:
            InterfaceError: If API call fails or returns empty token
        """
        try:
            _logger.debug(f"Making GetIdentityCenterAuthToken call for serverless workgroup: workgroup_id={workgroup_id}, region={region}")

            # Create RedshiftServerlessClient with provided credentials
            redshift_serverless_client = boto3.client(
                "redshift-serverless",
                region_name=region,
                aws_access_key_id=credentials.access_key,
                aws_secret_access_key=credentials.secret_key,
                aws_session_token=credentials.token,
                endpoint_url=self.endpoint_url,
            )

            # Call get_identity_center_auth_token API
            response = redshift_serverless_client.get_identity_center_auth_token(
                workgroupNames=[workgroup_id]
            )

            # Extract subject token from response
            subject_token = response.get("token")

            if not subject_token:
                _logger.error("GetIdentityCenterAuthToken returned empty token for serverless workgroup")
                raise InterfaceError(
                    "IdC authentication failed: GetIdentityCenterAuthToken returned an empty token. "
                    "Please verify your credentials and workgroup configuration."
                )

            _logger.debug("Successfully obtained subject token from GetIdentityCenterAuthToken for serverless workgroup")
            return subject_token

        except InterfaceError:
            # Re-raise InterfaceError as-is
            raise
        except Exception as e:
            _logger.error(f"GetIdentityCenterAuthToken call failed for serverless workgroup: {str(e)}")
            raise InterfaceError(
                f"IdC authentication failed: Failed to obtain token from GetIdentityCenterAuthToken: {str(e)}"
            )

    def _get_subject_token(self: "IdpTokenAuthPlugin") -> str:
        """
        Identity-enhanced credentials flow - obtain subject token from GetIdentityCenterAuthToken.
        Uses cluster info from RedshiftProperty (already resolved by IamHelper.set_iam_properties).

        Returns:
            Subject token string from the GetIdentityCenterAuthToken

        Raises:
            InterfaceError: If any step fails
        """
        try:
            _logger.debug("Starting identity-enhanced credentials flow")

            # Step 1: Determine region - explicit override takes precedence
            region = self.region or (self.redshift_property.region if self.redshift_property else None)
            if not region:
                _logger.error("Region resolution failed: neither explicit region nor RedshiftProperty.region available")
                raise InterfaceError(
                    "Region must be provided or resolvable from hostname"
                )
            _logger.debug(f"Using region: {region}")

            # Step 2: Create AWS credentials from parameters
            credentials = self._create_aws_credentials()

            # Step 3: Route to appropriate API call based on cluster type from RedshiftProperty
            if self.redshift_property._is_serverless:
                workgroup_id = self.redshift_property.serverless_work_group
                if not workgroup_id:
                    _logger.error("Serverless workgroup resolution failed: serverless_work_group not available in RedshiftProperty")
                    raise InterfaceError(
                        "Serverless workgroup must be provided or resolvable from hostname"
                    )
                _logger.debug(f"Routing to serverless API call for workgroup: {workgroup_id}")
                subject_token = self._get_serverless_auth_token(
                    credentials=credentials,
                    region=region,
                    workgroup_id=workgroup_id,
                )
            else:
                cluster_id = self.redshift_property.cluster_identifier
                if not cluster_id:
                    _logger.error("Cluster identifier resolution failed: cluster_identifier not available in RedshiftProperty")
                    raise InterfaceError(
                        "Cluster identifier must be provided"
                    )
                _logger.debug(f"Routing to provisioned API call for cluster: {cluster_id}")
                subject_token = self._get_provisioned_auth_token(
                    credentials=credentials,
                    region=region,
                    cluster_id=cluster_id,
                )

            _logger.debug("Successfully obtained subject token from identity-enhanced credentials flow")
            return subject_token

        except InterfaceError:
            # Re-raise InterfaceError as-is
            raise
        except Exception as e:
            _logger.error(f"Identity-enhanced credentials flow failed: {str(e)}")
            raise InterfaceError(
                f"IdC authentication failed when retrieving subject token: {str(e)}"
            )

    def get_cache_key(self: "IdpTokenAuthPlugin") -> str:  # type: ignore
        pass

    def get_auth_token(self: "IdpTokenAuthPlugin") -> str:
        self.check_required_parameters()

        # If using identity-enhanced credentials flow, get subject token from GetIdentityCenterAuthToken
        if self._is_using_identity_enhanced_credentials():
            _logger.debug("Using identity-enhanced credentials flow")
            return self._get_subject_token()

        # Otherwise, return direct token
        _logger.debug("Using direct token flow")
        return typing.cast(str, self.token)
