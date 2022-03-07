from abc import ABC, abstractmethod

from redshift_connector.plugin.native_token_holder import NativeTokenHolder


class INativePlugin(ABC):
    """
    Abstract base class for authentication plugins using Redshift Native authentication
    """

    @abstractmethod
    def get_idp_token(self: "INativePlugin") -> str:
        """
        Returns the IdP token retrieved after authenticating with the plugin.
        """
        pass  # pragma: no cover

    @abstractmethod
    def get_cache_key(self: "INativePlugin") -> str:
        """
        Returns the cache key used for storing credentials provided by the plugin.
        """
        pass  # pragma: no cover

    @abstractmethod
    def get_sub_type(self: "INativePlugin") -> int:
        """
        Returns a type code indicating the type of the current plugin.

        See :class:IdpAuthHelper for possible return values

        """
        pass  # pragma: no cover

    @abstractmethod
    def get_credentials(self: "INativePlugin") -> NativeTokenHolder:
        """
        Returns a :class:NativeTokenHolder object associated with the current plugin.
        """
        pass  # pragma: no cover

    @abstractmethod
    def refresh(self: "INativePlugin") -> None:
        """
        Refreshes the credentials, stored in :class:NativeTokenHolder, for the current plugin.
        """
        pass  # pragma: no cover
