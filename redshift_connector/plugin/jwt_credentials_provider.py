import base64
import json
import logging
import re
import typing
from abc import ABC, abstractmethod

from redshift_connector.credentials_holder import CredentialsHolder
from redshift_connector.error import InterfaceError
from redshift_connector.plugin.saml_credentials_provider import SamlCredentialsProvider
from redshift_connector.redshift_property import RedshiftProperty

_logger: logging.Logger = logging.getLogger(__name__)


class JwtCredentialsProvider(SamlCredentialsProvider, ABC):
    KEY_ROLE_ARN: str = "role_arn"
    KEY_WEB_IDENTITY_TOKEN: str = "web_identity_token"
    KEY_DURATION: str = "duration"
    KEY_ROLE_SESSION_NAME: str = "role_session_name"
    DEFAULT_ROLE_SESSION_NAME: str = "jwt_redshift_session"

    def __init__(self: "JwtCredentialsProvider"):
        super().__init__()
        # required params
        self.role_arn: typing.Optional[str] = None
        self.jwt: typing.Optional[str] = None

        # optional params
        self.role_session_name = JwtCredentialsProvider.DEFAULT_ROLE_SESSION_NAME
        self.duration: typing.Optional[int] = None
        self.db_user: typing.Optional[str] = None
        # self.db_groups: typing.Optional[str] = None
        # self.db_groups_filter: typing.Optional[str] = None
        # self.force_lowercase: typing.Optional[bool] = None
        # self.auto_create: typing.Optional[bool] = None
        self.sts_endpoint: typing.Optional[str] = None
        self.region: typing.Optional[str] = None

    @abstractmethod
    def process_jwt(self: "JwtCredentialsProvider", jwt: str) -> str:
        pass  # pragma: no cover

    def add_parameter(
        self: "JwtCredentialsProvider",
        info: RedshiftProperty,
    ) -> None:
        self.role_arn = info.role_arn
        self.jwt = info.web_identity_token
        self.duration = info.duration
        # Do not read dbUser from connection, as it derives from token.
        self.region = info.region

        if info.role_session_name is not None:
            self.role_session_name = info.role_session_name

    def get_cache_key(self: "JwtCredentialsProvider") -> str:
        return "{}{}{}{}".format(self.role_arn, self.jwt, self.role_session_name, self.duration)

    def get_credentials(self: "SamlCredentialsProvider") -> CredentialsHolder:
        key: str = self.get_cache_key()
        if key not in self.cache or self.cache[key].is_expired():
            try:
                self.refresh()
            except Exception as e:
                _logger.error("refresh failed: {}".format(str(e)))
                raise InterfaceError(e)

        if key not in self.cache or self.cache[key] is None:
            raise InterfaceError("Unable to load AWS credentials from IDP")

        return self.cache[key]

    def refresh(self: "JwtCredentialsProvider") -> None:
        import boto3  # type: ignore

        client = boto3.client("sts")

        try:
            _logger.debug("JWT: {}".format(self.jwt))
            if self.jwt is None:
                raise InterfaceError("Unable to refresh, no jwt provided")

            jwt: str = self.process_jwt(self.jwt)
            decoded_jwt: typing.Optional[typing.List[typing.Union[str, bytes]]] = self.decode_jwt(self.jwt)

            self.db_user = self.derive_database_user(decoded_jwt)

            response = client.assume_role_with_web_identity(
                RoleArn=self.role_arn,
                RoleSessionName=self.role_session_name,
                WebIdentityToken=jwt,
                DurationSeconds=self.duration if (self.duration is not None) and (self.duration > 0) else None,
            )

            stscred: typing.Dict[str, typing.Any] = response["Credentials"]
            credentials: CredentialsHolder = CredentialsHolder(stscred)
            credentials.set_metadata(self.read_metadata())
            key: str = self.get_cache_key()
            self.cache[key] = credentials

        except client.exceptions.MalformedPolicyDocumentException as e:
            _logger.error("MalformedPolicyDocumentException: %s", e)
            raise e
        except client.exceptions.PackedPolicyTooLargeException as e:
            _logger.error("PackedPolicyTooLargeException: %s", e)
            raise e
        except client.exceptions.IDPRejectedClaimException as e:
            _logger.error("IDPRejectedClaimException: %s", e)
            raise e
        except client.exceptions.InvalidIdentityTokenException as e:
            _logger.error("InvalidIdentityTokenException: %s", e)
            raise e
        except client.exceptions.ExpiredTokenException as e:
            _logger.error("ExpiredTokenException: %s", e)
            raise e
        except client.exceptions.RegionDisabledException as e:
            _logger.error("RegionDisabledException: %s", e)
            raise e
        except Exception as e:
            _logger.error("other Exception: %s", e)
            raise e

    def check_required_parameters(self: "JwtCredentialsProvider") -> None:
        if self.role_arn is None or self.role_arn == "":
            raise InterfaceError("Missing required property: {}".format(JwtCredentialsProvider.KEY_ROLE_ARN))
        elif self.jwt is None or self.jwt == "":
            raise InterfaceError("Missing required property: {}".format(JwtCredentialsProvider.KEY_WEB_IDENTITY_TOKEN))

    def decode_jwt(
        self: "JwtCredentialsProvider", jwt: typing.Optional[str]
    ) -> typing.Optional[typing.List[typing.Union[str, bytes]]]:
        if jwt is None:
            return None

        # base64(JOSE header).base64(payload).base64(signature)
        header_payload_sig: typing.List[str] = jwt.split(".")

        _logger.debug("Encoded JWT Elements: {}".format(header_payload_sig))

        if len(header_payload_sig) == 3:
            decoded_jwt: typing.List[typing.Union[bytes, str]] = []
            # decode the header and payload
            for i in range(2):
                decoded_jwt.append(base64.b64decode(header_payload_sig[i] + "==="))

            decoded_jwt.append(header_payload_sig[2])
            _logger.debug("Decoded JWT Elements: {}".format(header_payload_sig))
            return decoded_jwt
        else:
            return None

    def derive_database_user(
        self: "JwtCredentialsProvider", decoded_jwt: typing.Optional[typing.List[typing.Union[str, bytes]]]
    ) -> str:
        database_user: typing.Optional[str] = None

        if decoded_jwt is not None and len(decoded_jwt) == 3:
            payload: str = typing.cast(str, decoded_jwt[1])
            claims: typing.Tuple[str, ...] = ("DbUser", "upn", "preferred_username", "email")

            entity_json: typing.Dict = json.loads(payload)
            user_token_field: typing.Dict = {}

            for claim in claims:
                user_token_field = entity_json.get(claim, None)

                if user_token_field is not None:
                    database_user = typing.cast(str, user_token_field)

                    if database_user is not None and database_user != "":
                        _logger.debug(
                            "JWT claim: {claim} as database user {user}".format(claim=claim, user=database_user)
                        )
                        break

            if database_user is None or database_user == "":
                raise InterfaceError("No database user claim found in JWT")
            return database_user
        else:
            raise InterfaceError("JWT decoding error")

    def get_saml_assertion(self: "SamlCredentialsProvider"):
        raise NotImplementedError

    def do_verify_ssl_cert(self: "SamlCredentialsProvider") -> bool:
        raise NotImplementedError

    def get_form_action(self: "SamlCredentialsProvider", soup) -> typing.Optional[str]:
        raise NotImplementedError

    def read_metadata(self: "SamlCredentialsProvider", doc: bytes = b"") -> CredentialsHolder.IamMetadata:
        metadata: CredentialsHolder.IamMetadata = CredentialsHolder.IamMetadata()
        metadata.set_db_user(typing.cast(str, self.db_user))
        metadata.set_auto_create("true")
        return metadata


class BasicJwtCredentialsProvider(JwtCredentialsProvider):
    """
    A basic JWT Credential provider class that can be changed and implemented to work with any desired JWT service provider.
    """

    def process_jwt(self: "JwtCredentialsProvider", jwt: str) -> str:
        self.check_required_parameters()
        return self.jwt  # type: ignore
