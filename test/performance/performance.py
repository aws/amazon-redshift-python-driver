import configparser
import os
import time
import typing

import redshift_connector

conf = configparser.ConfigParser()
root_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
conf.read(root_path + "/config.ini")

root_path = os.path.dirname(os.path.abspath(__file__))
sql: typing.TextIO = open(root_path + "/test.sql", "r", encoding="utf8")
sqls: typing.List[str] = sql.readlines()
sqls = [_sql.replace("\n", "") for _sql in sqls]
sql.close()

conn: redshift_connector.Connection = redshift_connector.connect(
    database=conf.get("ci-cluster", "database"),
    host=conf.get("ci-cluster", "host"),
    port=conf.getint("default-test", "port"),
    user=conf.get("ci-cluster", "test_user"),
    password=conf.get("ci-cluster", "test_password"),
    ssl=True,
    sslmode=conf.get("default-test", "sslmode"),
    iam=False,
)

cursor: redshift_connector.Cursor = conn.cursor()
for _sql in sqls:
    cursor.execute(_sql)

result: typing.Tuple = cursor.fetchall()
print("fetch {result} rows".format(result=result))

print("start calculate fetch time")
for val in [True, False]:
    print("merge_socket_read={val}".format(val=val))
    start_time = time.time()
    cursor.execute("select * from performance", merge_socket_read=val)
    results = cursor.fetchall()
    print("Took {0} seconds.".format(time.time() - start_time))
    print("fetch {result} rows".format(result=len(results)))

cursor.close()
conn.commit()
