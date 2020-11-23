import typing

import pytest  # type: ignore
from pytest_mock import mocker

from redshift_connector import Cursor

test_warn_response_data: typing.List[typing.Tuple[typing.Optional[typing.List[bytes]], str]] = [
    ([b"ab\xffcd"], "Unable to decode column names. Byte values will be used for pandas dataframe column labels."),
    (None, "No row description was found. pandas dataframe will be missing column labels."),
]


@pytest.mark.parametrize("_input", test_warn_response_data)
def test_fetch_dataframe_warns_user(_input, mocker):
    data, exp_warning_msg = _input
    mock_cursor: Cursor = Cursor.__new__(Cursor)
    mocker.patch("redshift_connector.Cursor._getDescription", return_value=[data])
    mocker.patch("redshift_connector.Cursor.__next__", return_value=["blah"])
    with pytest.warns(UserWarning, match=exp_warning_msg):
        mock_cursor.fetch_dataframe(1)


def test_fetch_dataframe_no_results(mocker):
    mock_cursor: Cursor = Cursor.__new__(Cursor)
    mocker.patch("redshift_connector.Cursor._getDescription", return_value=["test"])
    mocker.patch("redshift_connector.Cursor.__next__", side_effect=StopIteration("mocked end"))

    assert mock_cursor.fetch_dataframe(1) is None
