import datetime
import typing


# credentials class used to store credentials
# and metadata from SAML assertion
class CredentialsHolder:
    def __init__(self: "CredentialsHolder", credentials: typing.Dict[str, typing.Any]) -> None:
        self.metadata: "CredentialsHolder.IamMetadata" = CredentialsHolder.IamMetadata()
        self.credentials: typing.Dict[str, typing.Any] = credentials
        self.expiration: "datetime.datetime" = credentials["Expiration"]

    def set_metadata(self: "CredentialsHolder", metadata: "IamMetadata") -> None:
        self.metadata = metadata

    def get_metadata(self: "CredentialsHolder") -> "CredentialsHolder.IamMetadata":
        return self.metadata

    # The AWS Access Key ID for this credentials object.
    def get_aws_access_key_id(self: "CredentialsHolder") -> str:
        return typing.cast(str, self.credentials["AccessKeyId"])

    # The AWS secret access key that can be used to sign requests.
    def get_aws_secret_key(self: "CredentialsHolder") -> str:
        return typing.cast(str, self.credentials["SecretAccessKey"])

    # The token that users must pass to the service API to use the temporary credentials.
    def get_session_token(self: "CredentialsHolder") -> str:
        return typing.cast(str, self.credentials["SessionToken"])

    # The date on which the current credentials expire.
    def get_expiration(self: "CredentialsHolder") -> datetime.datetime:
        return self.expiration

    def is_expired(self: "CredentialsHolder") -> bool:
        return datetime.datetime.now() > self.expiration.replace(tzinfo=None)

    # metadata used to store information from SAML assertion
    class IamMetadata:
        def __init__(self: "CredentialsHolder.IamMetadata") -> None:
            self.auto_create: bool = False
            self.db_user: typing.Optional[str] = None
            self.saml_db_user: typing.Optional[str] = None
            self.profile_db_user: typing.Optional[str] = None
            self.db_groups: typing.List[str] = list()
            self.allow_db_user_override: bool = False
            self.force_lowercase: bool = False

        def get_auto_create(self: "CredentialsHolder.IamMetadata") -> bool:
            return self.auto_create

        def set_auto_create(self: "CredentialsHolder.IamMetadata", auto_create: str) -> None:
            if auto_create.lower() == "true":
                self.auto_create = True
            else:
                self.auto_create = False

        def get_db_user(self: "CredentialsHolder.IamMetadata") -> typing.Optional[str]:
            return self.db_user

        def set_db_user(self: "CredentialsHolder.IamMetadata", db_user: str) -> None:
            self.db_user = db_user

        def get_saml_db_user(self: "CredentialsHolder.IamMetadata") -> typing.Optional[str]:
            return self.saml_db_user

        def set_saml_db_user(self: "CredentialsHolder.IamMetadata", saml_db_user: str) -> None:
            self.saml_db_user = saml_db_user

        def get_profile_db_user(self: "CredentialsHolder.IamMetadata") -> typing.Optional[str]:
            return self.profile_db_user

        def set_profile_db_user(self: "CredentialsHolder.IamMetadata", profile_db_user: str) -> None:
            self.profile_db_user = profile_db_user

        def get_db_groups(self: "CredentialsHolder.IamMetadata") -> typing.List[str]:
            return self.db_groups

        def set_db_groups(self: "CredentialsHolder.IamMetadata", db_groups: typing.List[str]) -> None:
            self.db_groups = db_groups

        def get_allow_db_user_override(self: "CredentialsHolder.IamMetadata") -> bool:
            return self.allow_db_user_override

        def set_allow_db_user_override(self: "CredentialsHolder.IamMetadata", allow_db_user_override: str) -> None:
            if allow_db_user_override.lower() == "true":
                self.allow_db_user_override = True
            else:
                self.allow_db_user_override = False

        def get_force_lowercase(self: "CredentialsHolder.IamMetadata") -> bool:
            return self.force_lowercase

        def set_force_lowercase(self: "CredentialsHolder.IamMetadata", force_lowercase: str) -> None:
            if force_lowercase.lower() == "true":
                self.force_lowercase = True
            else:
                self.force_lowercase = False
