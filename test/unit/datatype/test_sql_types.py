import typing

import pytest

from redshift_connector.utils.sql_types import SQLType, get_sql_type_name

all_oids: typing.List[int] = [d for d in SQLType]
all_datatypes: typing.List[typing.Tuple[int, str]] = [(d, d.name) for d in SQLType]


@pytest.mark.parametrize("oid", all_oids)
def test_sql_type_has_type_int(oid):
    assert isinstance(oid, int)


@pytest.mark.parametrize("oid, datatype", all_datatypes)
def test_get_sql_type_name(oid, datatype):
    assert get_sql_type_name(oid) == datatype


def test_get_datatype_name_invalid_oid_raises() -> None:
    with pytest.raises(ValueError, match="not a valid SQLType"):
        get_sql_type_name(3000)

def test_modify_sql_type() -> None:
    with pytest.raises(AttributeError, match="Cannot modify SQL type constant 'SQL_VARCHAR'"):
        SQLType.SQL_VARCHAR = 0