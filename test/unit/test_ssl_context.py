import ssl
import typing
import warnings
from unittest import mock

import pytest

from redshift_connector.core import Connection


class _StopAtTLS(Exception):
    """Stop before authentication, after inspecting the real SSL context."""


@pytest.mark.parametrize(
    "sslmode,sslmode_defaulted,check_hostname",
    [
        ("verify-ca", False, False),
        ("verify-full", True, False),
        ("verify-full", False, True),
    ],
)
def test_tls_context_uses_client_protocol_without_deprecated_defaults(
    sslmode: str, sslmode_defaulted: bool, check_hostname: bool
) -> None:
    sock = mock.MagicMock()
    sock.recv.return_value = b"S"
    contexts: typing.List[ssl.SSLContext] = []

    def stop_at_tls(context, raw_socket, **kwargs):
        contexts.append(context)
        raise _StopAtTLS()

    with warnings.catch_warnings(), mock.patch(
        "redshift_connector.core.socket.socket", return_value=sock
    ), mock.patch.object(ssl.SSLContext, "wrap_socket", stop_at_tls):
        warnings.simplefilter("error", DeprecationWarning)
        with pytest.raises(_StopAtTLS):
            Connection(
                user="u",
                password="p",
                host="localhost",
                port=5439,
                database="dev",
                ssl=True,
                sslmode=sslmode,
                sslmode_defaulted=sslmode_defaulted,
            )

    assert len(contexts) == 1
    assert contexts[0].protocol == ssl.PROTOCOL_TLS_CLIENT
    assert contexts[0].verify_mode == ssl.CERT_REQUIRED
    assert contexts[0].check_hostname is check_hostname
