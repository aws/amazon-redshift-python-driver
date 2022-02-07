import configparser
import csv
import os
import time
import typing

import redshift_connector

conf: configparser.ConfigParser = configparser.ConfigParser()
root_path: str = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
conf.read(root_path + "/config.ini")

root_path = os.path.dirname(os.path.abspath(__file__))


def perf_conn():
    return redshift_connector.connect(
        database=conf.get("ci-cluster", "database"),
        host=conf.get("ci-cluster", "host"),
        port=conf.getint("default-test", "port"),
        user=conf.get("ci-cluster", "test_user"),
        password=conf.get("ci-cluster", "test_password"),
        ssl=True,
        sslmode=conf.get("default-test", "sslmode"),
        iam=False,
    )


print("Reading data from csv file")
with open(root_path + "/bulk_insert_data.csv", "r", encoding="utf8") as csv_data:
    reader = csv.reader(csv_data, delimiter="\t")
    next(reader)
    data = []
    for row in reader:
        data.append(row)
print("Inserting {} rows having {} columns".format(len(data), len(data[0])))

print("\nCursor.insert_data_bulk()..")
for batch_size in [1e2, 1e3, 1e4]:
    with perf_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("drop table if exists bulk_insert_perf;")
            cursor.execute("create table bulk_insert_perf (c1 int, c2 int, c3 int, c4 int, c5 int);")
            start_time: float = time.time()
            cursor.insert_data_bulk(
                filename=root_path + "/bulk_insert_data.csv",
                table_name="bulk_insert_perf",
                parameter_indices=[0, 1, 2, 3, 4],
                column_names=["c1", "c2", "c3", "c4", "c5"],
                delimiter="\t",
                batch_size=batch_size,
            )

            print("batch_size={0} {1} seconds.".format(batch_size, time.time() - start_time))

print("Cursor.executemany()")
with perf_conn() as conn:
    with conn.cursor() as cursor:
        cursor.execute("drop table if exists bulk_insert_perf;")
        cursor.execute("create table bulk_insert_perf (c1 int, c2 int, c3 int, c4 int, c5 int);")
        start_time = time.time()
        cursor.executemany("insert into bulk_insert_perf(c1, c2, c3, c4, c5) values(%s, %s, %s, %s, %s)", data)
        print("{0} seconds.".format(time.time() - start_time))
