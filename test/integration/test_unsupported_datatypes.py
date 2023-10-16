import configparser
import os
import typing
from warnings import filterwarnings

import pytest  # type: ignore

import redshift_connector

if typing.TYPE_CHECKING:
    from redshift_connector import Connection

conf: configparser.ConfigParser = configparser.ConfigParser()
root_path: str = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
conf.read(root_path + "/config.ini")


@pytest.fixture
def db_table(request, con: redshift_connector.Connection) -> redshift_connector.Connection:
    filterwarnings("ignore", "DB-API extension cursor.next()")
    filterwarnings("ignore", "DB-API extension cursor.__iter__()")
    con.paramstyle = "format"  # type: ignore

    def fin() -> None:
        try:
            with con.cursor() as cursor:
                cursor.execute("drop table if exists t1")
        except redshift_connector.ProgrammingError:
            pass

    request.addfinalizer(fin)
    return con


# https://docs.aws.amazon.com/redshift/latest/dg/c_unsupported-postgresql-datatypes.html
unsupported_datatypes: typing.List[str] = [
    "bytea",
    "interval",
    "bit",
    "bit varying",
    "hstore",
    "json",
    "serial",
    "bigserial",
    "smallserial",
    "money",
    "txid_snapshot",
    "uuid",
    "xml",
    "inet",
    "cidr",
    "macaddr",
    "oid",
    "regproc",
    "regprocedure",
    "regoper",
    "regoperator",
    "regclass",
    "regtype",
    "regrole",
    "regnamespace",
    "regconfig",
    "regdictionary",
    "any",
    "anyelement",
    "anyarray",
    "anynonarray",
    "anyenum",
    "anyrange",
    "cstring",
    "internal",
    "language_handler",
    "fdw_handler",
    "tsm_handler",
    "record",
    "trigger",
    "event_trigger",
    "pg_ddl_command",
    "void",
    "opaque",
    "int4range",
    "int8range",
    "numrange",
    "tsrange",
    "tstzrange",
    "daterange",
    "tsvector",
    "tsquery",
]


@pytest.mark.unsupported_datatype
class TestUnsupportedDataTypes:
    @pytest.mark.parametrize("datatype", unsupported_datatypes)
    def test_create_table_with_unsupported_datatype_fails(self, db_table: "Connection", datatype: str) -> None:
        with db_table.cursor() as cursor:
            with pytest.raises(Exception) as exception:
                cursor.execute("CREATE TEMPORARY TABLE t1 (a {})".format(datatype))
            assert exception.type == redshift_connector.ProgrammingError
            assert (
                'Column "t1.a" has unsupported type' in exception.__str__()
                or 'type "{}" does not exist'.format(datatype) in exception.__str__()
                or 'syntax error at or near "{}"'.format(datatype) in exception.__str__()
            )

    @pytest.mark.parametrize("datatype", ["int[]", "int[][]"])
    def test_create_table_with_array_datatype_fails(self, db_table: "Connection", datatype: str) -> None:
        with db_table.cursor() as cursor:
            with pytest.raises(Exception) as exception:
                cursor.execute("CREATE TEMPORARY TABLE t1 (a {})".format(datatype))
            assert exception.type == redshift_connector.ProgrammingError
            assert (
                'Column "t1.a" has unsupported type' in exception.__str__()
                or 'type "{}" does not exist'.format(datatype) in exception.__str__()
            )
