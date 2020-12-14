import configparser
import os
import typing
from math import isclose
from test.integration.datatype._generate_test_datatype_tables import (
    DATATYPES_WITH_MS,
    FLOAT_DATATYPES,
    Datatypes,
    datatype_test_setup,
    datatype_test_teardown,
    get_table_name,
    test_data,
)

import pytest

import redshift_connector

conf = configparser.ConfigParser()
root_path = os.path.dirname(os.path.dirname(os.path.abspath(os.path.join(__file__, os.pardir))))
conf.read(root_path + "/config.ini")


@pytest.fixture(scope="session", autouse=True)
def create_datatype_test_resources(request):
    """
    Creates datatype test schema, tables, and inserts test data.
    """
    datatype_test_setup(conf)

    def fin():
        datatype_test_teardown(conf)

    request.addfinalizer(fin)


@pytest.mark.parametrize("datatype", Datatypes.list())
def test_datatype_recv_support(db_kwargs, datatype):
    table_name: str = get_table_name(datatype)
    exp_results: typing.Tuple[typing.Tuple[str, ...], ...] = test_data[datatype.name]

    with redshift_connector.connect(**db_kwargs) as con:
        with con.cursor() as cursor:
            cursor.execute("select * from {}".format(table_name))
            results = cursor.fetchall()

            assert results is not None
            assert len(results) == len(exp_results)

            for ridx, exp_row in enumerate(exp_results):
                assert results[ridx][0] == exp_row[0]

                # the expected Python value is stored in the last index of the tuple
                if datatype in FLOAT_DATATYPES:
                    assert isclose(
                        typing.cast(float, results[ridx][1]),
                        typing.cast(float, exp_row[-1]),
                        rel_tol=1e-05,
                        abs_tol=1e-08,
                    )

                elif datatype in DATATYPES_WITH_MS:
                    assert results[ridx][1].replace(microsecond=0) == exp_row[-1].replace(microsecond=0)
                    assert isclose(results[ridx][1].microsecond, exp_row[-1].microsecond, rel_tol=1e1)
                else:
                    assert results[ridx][1] == exp_row[-1]
