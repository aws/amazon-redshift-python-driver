import datetime
from typing import Any, Dict, Optional, Union


# credentials class used to store credentials
# and metadata from SAML assertion
class CredentialsHolder:
    def __init__(self, credentials: Dict[str, Any]) -> None:
        self.metadata: CredentialsHolder.IamMetadata = CredentialsHolder.IamMetadata()
        self.credentials: Dict[str, Any] = credentials
        self.expiration: datetime.datetime = credentials['Expiration']

    def set_metadata(self, metadata):
        self.metadata = metadata

    def get_metadata(self):
        return self.metadata

    # The AWS Access Key ID for this credentials object.
    def get_aws_access_key_id(self) -> str:
        return self.credentials['AccessKeyId']

    # The AWS secret access key that can be used to sign requests.
    def get_aws_secret_key(self) -> str:
        return self.credentials['SecretAccessKey']

    # The token that users must pass to the service API to use the temporary credentials.
    def get_session_token(self) -> str:
        return self.credentials['SessionToken']

    # The date on which the current credentials expire.
    def get_expiration(self) -> datetime.datetime:
        return self.expiration

    def is_expired(self) -> bool:
        return datetime.datetime.now() > self.expiration.replace(tzinfo=None)

    # metadata used to store information from SAML assertion
    class IamMetadata:
        def __init__(self) -> None:
            self.auto_create: bool = False
            self.db_user: Optional[str] = None
            self.saml_db_user: Optional[str] = None
            self.profile_db_user: Optional[str] = None
            self.db_groups: Optional[str] = None
            self.allow_db_user_override: bool = False
            self.force_lowercase: bool = False

        def get_auto_create(self) -> bool:
            return self.auto_create

        def set_auto_create(self, auto_create: str) -> None:
            if auto_create.lower() == 'true':
                self.auto_create = True
            else:
                self.auto_create = False

        def get_db_user(self) -> Optional[str]:
            return self.db_user

        def set_db_user(self, db_user: str) -> None:
            self.db_user = db_user

        def get_saml_db_user(self) -> Optional[str]:
            return self.saml_db_user

        def set_saml_db_user(self, saml_db_user: str) -> None:
            self.saml_db_user = saml_db_user

        def get_profile_db_user(self) -> Optional[str]:
            return self.profile_db_user

        def set_profile_db_user(self, profile_db_user: str) -> None:
            self.profile_db_user = profile_db_user

        def get_db_groups(self) -> Optional[str]:
            return self.db_groups

        def set_db_groups(self, db_groups: str) -> None:
            self.db_groups = db_groups

        def get_allow_db_user_override(self) -> bool:
            return self.allow_db_user_override

        def set_allow_db_user_override(self, allow_db_user_override: str) -> None:
            if allow_db_user_override.lower() == 'true':
                self.allow_db_user_override = True
            else:
                self.allow_db_user_override = False

        def get_force_lowercase(self) -> bool:
            return self.force_lowercase

        def set_force_lowercase(self, force_lowercase: str) -> None:
            if force_lowercase.lower() == 'true':
                self.force_lowercase = True
            else:
                self.force_lowercase = False
