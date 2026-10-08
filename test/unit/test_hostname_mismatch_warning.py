import logging
import typing

import pytest


def _mismatch_warnings(caplog) -> typing.List[str]:
    """The tolerated-mismatch lines the driver logged.

    The log is the driver's only channel for this, so every case here reads it
    rather than stderr -- and asserts stderr stayed empty, because a connection
    that succeeded must not put text in front of the application's user.
    """
    return [record.message for record in caplog.records if "does not name host" in record.message]

from redshift_connector import core
from redshift_connector.core import Connection
from redshift_connector.error import InterfaceError


def test_every_tolerated_mismatch_is_reported(caplog, capsys) -> None:
    # The warning is the only signal a caller gets while the transitional
    # fallback exists, and the count is what says how many connections rely on
    # it, so every occurrence is reported -- repeats on one host included.
    with caplog.at_level(logging.WARNING, logger="redshift_connector.core"):
        core._warn_hostname_mismatch("first.example")
        core._warn_hostname_mismatch("first.example")
        core._warn_hostname_mismatch("second.example")

    warnings: typing.List[str] = _mismatch_warnings(caplog)
    assert len(warnings) == 3, f"Expected 3 warnings, got: {warnings}"
    assert any("first.example" in warning for warning in warnings), "the first host must be named"
    assert any(
        "second.example" in warning for warning in warnings
    ), "a second host must not be silently tolerated"
    assert capsys.readouterr().err == "", "the warning must not reach stderr"


def test_warning_names_the_host_and_the_remedy(caplog, capsys) -> None:
    with caplog.at_level(logging.WARNING, logger="redshift_connector.core"):
        core._warn_hostname_mismatch("cluster.example")

    warnings: typing.List[str] = _mismatch_warnings(caplog)
    assert warnings, "the tolerated mismatch must be recorded"
    assert "cluster.example" in warnings[0], f"the host must be named, got: {warnings[0]}"
    assert "sslmode" in warnings[0], "the warning must point at the remedy"
    assert capsys.readouterr().err == "", "the warning must not reach stderr"


def test_warning_is_recorded_at_warning_level(caplog) -> None:
    # The level is what decides whether a customer who turns logging on at the
    # usual verbosity sees this at all, so it is asserted rather than assumed.
    with caplog.at_level(logging.WARNING, logger="redshift_connector.core"):
        core._warn_hostname_mismatch("logged.example")

    records = [record for record in caplog.records if "does not name host" in record.message]
    assert records, f"Expected the warning in the log, got {[rec.message for rec in caplog.records]}"
    assert records[0].levelno == logging.WARNING, f"Expected WARNING, got {records[0].levelname}"
    assert "logged.example" in records[0].message, "the log line must name the host"


class _StopAfterNameCheck(Exception):
    """Stands in for the startup handshake, ending the attempt once the name
    check has had its say.

    Deliberately not an ``OSError``, so ``Connection.__init__``'s ``socket.error``
    handler cannot swallow it and disguise where execution actually got to.
    """


def _drive_name_check(sslmode_defaulted: bool):
    """Drive ``Connection.__init__`` to the verify-full name check against a
    certificate that names another host.

    ``wrap_socket`` *succeeds* here: a chain failure raises before the branch the
    transitional allowance lives in, so mocking one can never tell the two modes
    apart. The name check is failed instead, which is the mismatch this change is
    about, and ``makefile`` then raises so a tolerated connection stops right
    after the decision rather than going on to the startup message.

    Returns the connection, whatever propagated, whether ``makefile`` was reached
    at all -- which is how a tolerated mismatch is told from a rejected one,
    independently of the message -- plus the ``check_hostname`` the driver asked
    for and whether the local matcher was consulted. Those last two are what
    distinguish the two paths: a caller-named mode leaves OpenSSL as the gate.
    """
    import ssl as ssl_module
    from unittest import mock

    conn: Connection = object.__new__(Connection)
    raw_socket = mock.MagicMock()
    raw_socket.recv.return_value = b"S"  # the server agrees to TLS
    wrapped = mock.MagicMock()
    wrapped.makefile.side_effect = _StopAfterNameCheck()

    seen_check_hostname: typing.List[bool] = []

    def record_wrap(self, sock, **kwargs):
        # OpenSSL enforces the name during the handshake when check_hostname is
        # set, so capture what it was asked to do and stand in for the result.
        seen_check_hostname.append(self.check_hostname)
        if self.check_hostname:
            raise ssl_module.SSLCertVerificationError(
                "hostname 'localhost' doesn't match 'other.example'"
            )
        return wrapped

    raised: typing.Optional[BaseException] = None
    with mock.patch("redshift_connector.core.socket.socket", return_value=raw_socket), mock.patch.object(
        ssl_module.SSLContext, "wrap_socket", record_wrap
    ), mock.patch("redshift_connector.core.match_hostname", return_value=False) as matcher:
        try:
            Connection.__init__(
                conn,
                user="u",
                password="p",
                # Resolvable without a network: the address lookup ahead of the
                # TLS handshake is the real one, only the socket is a stand-in.
                host="localhost",
                port=5439,
                database="dev",
                ssl=True,
                sslmode="verify-full",
                sslmode_defaulted=sslmode_defaulted,
            )
        except BaseException as e:  # noqa: BLE001 - the outcome is the assertion
            raised = e

    asked_for = seen_check_hostname[0] if seen_check_hostname else None
    return conn, raised, wrapped.makefile.called, asked_for, matcher.called


def test_a_driver_chosen_mode_lets_a_mismatched_name_through_with_a_warning(caplog, capsys) -> None:
    with caplog.at_level(logging.WARNING, logger="redshift_connector.core"):
        conn, raised, reached_handshake, check_hostname, matcher_used = _drive_name_check(
            sslmode_defaulted=True
        )

    assert check_hostname is False, (
        "the defaulted path must check the name after the handshake, so a mismatch can "
        "be a warning rather than a handshake failure"
    )
    assert matcher_used, "the local matcher is what decides whether to warn on this path"
    assert reached_handshake, f"the connection should have continued past the name check, got {raised!r}"
    assert isinstance(raised, _StopAfterNameCheck), f"Expected to stop at the handshake, got {raised!r}"

    warnings: typing.List[str] = _mismatch_warnings(caplog)
    assert warnings, f"the tolerated mismatch must warn, got: {[rec.message for rec in caplog.records]}"
    assert "localhost" in warnings[0], f"the warning must name the host, got: {warnings[0]}"
    assert capsys.readouterr().err == "", "a tolerated mismatch must stay out of stderr"


def test_a_caller_named_mode_leaves_openssl_as_the_gate(caplog) -> None:
    """A caller who asked for verify-full keeps the behaviour they had before this
    change: OpenSSL enforces the name during the handshake, so a bug in the local
    matcher cannot admit a certificate for them."""
    import ssl as ssl_module

    with caplog.at_level(logging.WARNING, logger="redshift_connector.core"):
        conn, raised, reached_handshake, check_hostname, matcher_used = _drive_name_check(
            sslmode_defaulted=False
        )

    assert check_hostname is True, "OpenSSL must be told to enforce the name for an explicit mode"
    assert not matcher_used, "the local matcher must not gate accept/reject for an explicit mode"
    assert not reached_handshake, "a rejected name must not reach the startup handshake"
    # Connection.__init__ wraps a handshake failure as InterfaceError("communication
    # error", <original>), so assert on what OpenSSL contributed rather than the
    # wrapper: the rejection must come from the handshake, not from our matcher.
    assert isinstance(raised, InterfaceError), f"Expected the connection to fail, got {raised!r}"
    assert any(
        isinstance(arg, ssl_module.SSLCertVerificationError) for arg in raised.args
    ), f"OpenSSL must be what rejected the name, got {raised!r}"
    assert not _mismatch_warnings(caplog), "a caller who chose the mode must not be warned"


@pytest.mark.parametrize(
    "sslmode_defaulted",
    [
        # The mode the driver chose, so the transitional allowance is in play.
        True,
        # A mode the caller named, so it is not.
        False,
    ],
)
def test_untrusted_chain_is_rejected_whichever_way_the_mode_was_chosen(sslmode_defaulted: bool) -> None:
    """The allowance relaxes the name check only, never the chain.

    So a bad chain must fail the same way whoever chose the mode. Unlike the cases
    above this one does fail inside ``wrap_socket``, which is the point: the
    failure must land before the name check can offer any allowance at all.
    """
    import ssl as ssl_module
    from unittest import mock

    conn: Connection = object.__new__(Connection)
    sock = mock.MagicMock()
    sock.recv.return_value = b"S"
    conn._usock = sock

    chain_error = ssl_module.SSLCertVerificationError("certificate verify failed")

    with mock.patch("redshift_connector.core.socket.socket", return_value=sock), mock.patch.object(
        ssl_module.SSLContext, "wrap_socket", side_effect=chain_error
    ), mock.patch("redshift_connector.core.match_hostname", return_value=True) as matched:
        with pytest.raises((InterfaceError, ssl_module.SSLError)) as excinfo:
            Connection.__init__(
                conn,
                user="u",
                password="p",
                host="localhost",
                port=5439,
                database="dev",
                ssl=True,
                sslmode="verify-full",
                sslmode_defaulted=sslmode_defaulted,
            )

    message: str = str(excinfo.value)
    assert "certificate verify failed" in message, f"Expected the chain failure, got {message}"
    assert not matched.called, "the chain must be rejected before the name is ever considered"
