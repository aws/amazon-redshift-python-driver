import configparser
import os
import time

import redshift_connector

conf = configparser.ConfigParser()
root_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
conf.read(root_path + "/config.ini")

root_path = os.path.dirname(os.path.abspath(__file__))
sql = open(root_path + "/test.sql", "r", encoding="utf8")
sqls = sql.readlines()
sqls = [sql.replace("\n", "") for sql in sqls]
sql.close()

conn = redshift_connector.connect(
    database=conf.get("database", "database"),
    host=conf.get("database", "host"),
    port=conf.getint("database", "port"),
    user=conf.get("database", "user"),
    password=conf.get("database", "password"),
    ssl=True,
    sslmode=conf.get("database", "sslmode"),
    iam=False,
)

cursor = conn.cursor()
for sql in sqls:
    cursor.execute(sql)

result = cursor.fetchall()
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
