import logging
import typing

import pytest

import redshift_connector

"""
Host-name verification against a real cluster -- the positive direction only.

A real endpoint is the right place to prove that a correct certificate is
accepted: the chain, the CA, the wildcard SAN and the endpoint naming are all
genuine, and none of that can be reproduced by a certificate the test mints
itself.

The negative direction deliberately lives elsewhere. A real cluster cannot be
asked to present a certificate naming the wrong host, and forcing one by dialling
its IP would make the test depend on Redshift certificates continuing to omit an
``iPAddress`` SAN -- so the case would start failing if AWS changed its
certificates, while the driver was still correct. Mismatch and untrusted-chain
cases are covered where they can be constructed deterministically:

* ``test/unit/test_hostname_check.py`` -- the matcher itself, including wildcards,
  partial wildcards, public suffixes and address literals.
* ``test/unit/test_hostname_mismatch_warning.py`` -- that a tolerated mismatch is
  reported once per occurrence and never reaches stderr.
"""


@pytest.fixture
def defaulted_ssl_kwargs(db_kwargs) -> typing.Dict[str, typing.Any]:
    """Connection args with no sslmode, so the driver supplies its own default."""
    args = dict(db_kwargs)
    args["ssl"] = True
    args.pop("sslmode", None)
    return args


def test_defaulted_mode_connects_and_is_silent(defaulted_ssl_kwargs, caplog) -> None:
    """The default is verify-full, and a real endpoint satisfies it quietly.

    This is the case that would break for every customer if the new default did
    not match real certificates, so it is the one worth running against a cluster.
    """
    with caplog.at_level(logging.WARNING, logger="redshift_connector"):
        with redshift_connector.connect(**defaulted_ssl_kwargs) as conn:
            with conn.cursor() as cursor:
                cursor.execute("select 1")
                assert cursor.fetchone()[0] == 1

    assert "does not name host" not in caplog.text, (
        "a certificate that names the dialled host must not warn; log was:\n"
        + caplog.text
    )


def test_explicit_verify_full_connects(db_kwargs) -> None:
    """An explicitly strict caller reaches the same endpoint, so the flip to a
    verify-full default cannot be stricter than what verify-full already allowed."""
    args = dict(db_kwargs)
    args["ssl"] = True
    args["sslmode"] = "verify-full"

    with redshift_connector.connect(**args) as conn:
        with conn.cursor() as cursor:
            cursor.execute("select 1")
            assert cursor.fetchone()[0] == 1

# Note: the three sites that carry the default are pinned to each other by
# test/unit/test_redshift_property.py, which does not need a cluster. The
# connection object exposes no resolved-mode attribute, so there is nothing for an
# integration test to assert on beyond the behaviour above.
