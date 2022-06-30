import pytest

from redshift_connector.utils.oids import RedshiftOID, get_datatype_name

all_oids: list[int] = [d for d in RedshiftOID]
all_datatypes: list[tuple[int, str]] = [(d, d.name) for d in RedshiftOID]


@pytest.mark.parametrize("oid", all_oids)
def test_RedshiftOID_has_type_int(oid):
    assert isinstance(oid, int)


@pytest.mark.parametrize("oid, datatype", all_datatypes)
def test_get_datatype_name(oid, datatype):
    assert get_datatype_name(oid) == datatype


def test_get_datatype_name_invalid_oid_raises():
    with pytest.raises(ValueError, match="not a valid RedshiftOID"):
        get_datatype_name(-9)
