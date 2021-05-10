import statistics
import time as timer
import typing
from datetime import date, datetime, time, timezone
from enum import Enum
from math import isclose

import redshift_connector
from redshift_connector.config import ClientProtocolVersion

"""
THIS IS A MANUAL TEST REQUIRING LEADER NODE ACCESS ON A REDSHIFT CLUSTER
1) Execute protocol_perf_test.sql, using a tool like psql, on your Redshift cluster
2) Install Python>=3.5 on the leader node of your Redshift cluster and install redshift_connector
3) Hardcode user, password, password arguments for connect(...). Ensure host is set to 'localhost' and SSL is
   disabled.
4) Execute this script from the leader node, the script will output performance results
"""


# maps type names to test table names


class PerformanceTestDatatypes(Enum):
    int2 = "perf_integer2"
    int4 = "perf_integer"
    int8 = "perf_biginteger8"
    float4 = "perf_real4"
    float8 = "perf_double"
    date = "perf_date"
    timestamp = "perf_timestamp"
    decimal8 = "perf_numeric8"
    decimal16 = "perf_numeric16"
    bool = "perf_bool"
    char = "perf_char16"
    varchar = "perf_varchar"
    time = "perf_time"
    timetz = "perf_timetz"
    timestamptz = "perf_timestamptz"

    @classmethod
    def list(cls) -> typing.List["PerformanceTestDatatypes"]:
        return list(map(lambda p: p, cls))  # type: ignore


PROTOCOLS: typing.Tuple[ClientProtocolVersion, ...] = (
    ClientProtocolVersion.EXTENDED_RESULT_METADATA,
    ClientProtocolVersion.BINARY,
)
QUERY_ROW_LIMIT: typing.Dict[PerformanceTestDatatypes, typing.Tuple[float, ...]] = {
    PerformanceTestDatatypes.int2: (1e6, 2e6, 4e6),
    PerformanceTestDatatypes.int4: (1e6, 2e6, 4e6),
    PerformanceTestDatatypes.int8: (1e6, 2e6, 3e6),
    PerformanceTestDatatypes.float4: (1e6, 2e6, 3e6),
    PerformanceTestDatatypes.float8: (1e6, 2e6, 3e6),
    PerformanceTestDatatypes.date: (1e6, 2e6, 3e6),
    PerformanceTestDatatypes.timestamp: (1e6, 2e6, 3e6),
    PerformanceTestDatatypes.decimal8: (1e6, 2e6),
    PerformanceTestDatatypes.decimal16: (1e6, 2e6),
    PerformanceTestDatatypes.bool: (1e6, 2e6, 3e6),
    PerformanceTestDatatypes.char: (1e6, 2e6),
    PerformanceTestDatatypes.varchar: (1e5, 2e5),
    PerformanceTestDatatypes.time: (1e5, 2e5),
    PerformanceTestDatatypes.timetz: (1e5, 2e5),
    PerformanceTestDatatypes.timestamptz: (1e5, 2e5),
}
NUM_COLUMNS: int = 5

# each table has 5 columns, all holding the same datatype
# each column holds the same value, except for PerformanceTestDatatypes.bool
# which flips between True/False

expected_results: typing.Dict[
    PerformanceTestDatatypes, typing.Optional[typing.Union[int, float, date, datetime, time, str]]
] = {
    PerformanceTestDatatypes.int2: 32767,
    PerformanceTestDatatypes.int4: 214748367,
    PerformanceTestDatatypes.int8: 214748367214748367,
    PerformanceTestDatatypes.float4: 123.123,
    PerformanceTestDatatypes.float8: 1234567.1234567,
    PerformanceTestDatatypes.date: date(year=2020, month=3, day=10),
    PerformanceTestDatatypes.timestamp: datetime(year=2008, month=6, day=1, hour=9, minute=59, second=59),
    PerformanceTestDatatypes.decimal8: 123456789.123456789,
    PerformanceTestDatatypes.decimal16: 1234567891234567899.1234567891234567899,
    PerformanceTestDatatypes.bool: None,
    PerformanceTestDatatypes.char: "123456789abcdef ",
    PerformanceTestDatatypes.varchar: "abcd¬µ3kt¿abcdÆgda123~Øasd",
    PerformanceTestDatatypes.time: time(hour=12, minute=13, second=14),
    PerformanceTestDatatypes.timetz: time(hour=20, minute=13, second=14, microsecond=123456, tzinfo=timezone.utc),
    PerformanceTestDatatypes.timestamptz: datetime(
        year=1997, month=10, day=11, hour=7, minute=37, second=16, tzinfo=timezone.utc
    ),
}
execution_times: typing.Dict[
    PerformanceTestDatatypes, typing.Dict[ClientProtocolVersion, typing.Dict[float, typing.List]]
] = {}

for datatype in PerformanceTestDatatypes.list():
    execution_times[datatype] = {}
    for protocol in PROTOCOLS:
        execution_times[datatype][protocol] = {}
        for row_limit in QUERY_ROW_LIMIT[datatype]:
            # store all execution results so we can produce avg and best performance numbers
            execution_times[datatype][protocol][row_limit] = []


def validate_results(test_datatype: PerformanceTestDatatypes, results: typing.Tuple, exp_num_rows: float):
    if len(results) != int(exp_num_rows):
        print(test_datatype, len(results), len(expected_results))
        raise Exception

    for ret_row in results:
        assert len(ret_row) == NUM_COLUMNS

        if test_datatype in (
            PerformanceTestDatatypes.float4,
            PerformanceTestDatatypes.float8,
            PerformanceTestDatatypes.decimal8,
            PerformanceTestDatatypes.decimal16,
        ):
            for ret_col in ret_row:
                assert isclose(ret_col, expected_results[test_datatype], rel_tol=1e-3)  # type: ignore
        else:
            for ret_col in ret_row:
                assert ret_col == expected_results[test_datatype], ret_row


def run_performance_test(
    test_datatype: PerformanceTestDatatypes, limit: float, test_protocol: ClientProtocolVersion, is_warmup=False
):
    with redshift_connector.connect(
        ssl=False,
        sslmode="disable",
        host="localhost",
        database="",
        user="",
        password="",
        client_protocol_version=test_protocol.value,
    ) as conn:
        assert conn._client_protocol_version == test_protocol.value
        with conn.cursor() as cursor:
            start_ms: float = timer.time() * 1000
            cursor.execute(
                "select * from {table_name} limit {limit}".format(table_name=test_datatype.value, limit=limit)
            )
            end_ms: float = timer.time() * 1000
            total_ms: float = end_ms - start_ms
            if not is_warmup:
                print(
                    "datatype: {}\tprotocol: {}\tfetch {:.0f}: {:.2f}ms".format(
                        test_datatype.name, test_protocol.name, limit, total_ms
                    )
                )
            results = cursor.fetchall()
            validate_results(test_datatype, results, limit)

        if not is_warmup:
            # total execution time is in msec and represents time required to
            # fetch data from server and copy to client deque
            execution_times[test_datatype][test_protocol][int(limit)].append(end_ms - start_ms)


def perf_test_driver(repeat=1):
    for test_datatype in PerformanceTestDatatypes.list():
        for test_protocol in PROTOCOLS:
            for test_num_rows in QUERY_ROW_LIMIT[test_datatype]:
                # the warm up run is to fill cache, and results are not considered in report. They are
                # not output to console either
                run_performance_test(test_datatype, test_num_rows, test_protocol, is_warmup=True)

                for _ in range(repeat):
                    run_performance_test(test_datatype, test_num_rows, test_protocol, is_warmup=False)


perf_test_driver(repeat=5)
print("\n\nSummarized Performance Results\n")
for datatype in PerformanceTestDatatypes.list():
    print(datatype.name)
    for num_rows in QUERY_ROW_LIMIT[datatype]:
        text_avg: float = statistics.mean(
            execution_times[datatype][ClientProtocolVersion.EXTENDED_RESULT_METADATA][num_rows]
        )
        binary_avg: float = statistics.mean(execution_times[datatype][ClientProtocolVersion.BINARY][num_rows])

        print("protocol: {}\tavg fetch {:.0f}: {:.2f}ms".format("text", num_rows, text_avg))
        print("protocol: {}\tavg fetch {:.0f}: {:.2f}ms".format("binary", num_rows, binary_avg))
        print("avg difference: {:.2f}%\n".format(100 * (1 - binary_avg / text_avg)))
