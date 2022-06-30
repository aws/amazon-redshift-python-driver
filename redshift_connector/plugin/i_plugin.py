import typing
from abc import ABC, abstractmethod

if typing.TYPE_CHECKING:
    from redshift_connector.credentials_holder import CredentialsHolder
    from redshift_connector.plugin.native_token_holder import NativeTokenHolder
    from redshift_connector.redshift_property import RedshiftProperty


class IPlugin(ABC):
    @abstractmethod
    def add_parameter(self: "IPlugin", info: "RedshiftProperty"):
        """
        Defines instance attributes from the :class:RedshiftProperty object associated with the current authentication session.
        """
        pass

    @abstractmethod
    def get_cache_key(self: "IPlugin") -> str:
        pass

    @abstractmethod
    def get_credentials(self: "IPlugin") -> typing.Union["NativeTokenHolder", "CredentialsHolder"]:
        """
        Returns a :class:NativeTokenHolder object associated with the current plugin.
        """
        pass  # pragma: no cover

    @abstractmethod
    def get_sub_type(self: "IPlugin") -> int:
        """
        Returns a type code indicating the type of the current plugin.

        See :class:IdpAuthHelper for possible return values

        """
        pass  # pragma: no cover

    @abstractmethod
    def refresh(self: "IPlugin") -> None:
        """
        Refreshes the credentials, stored in :class:NativeTokenHolder, for the current plugin.
        """
        pass  # pragma: no cover

    @abstractmethod
    def set_group_federation(self: "IPlugin", group_federation: bool):
        pass
