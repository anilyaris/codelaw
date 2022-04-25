import pymongo
import psycopg2

with open("/cluster/home/ayaris/mongohost", 'r') as f:
    mongo_host = f.read()[:-1]
with open("/cluster/home/ayaris/logfile", 'r') as f:
    psql_host = f.readlines()[0][:-1]

mongo_client = pymongo.MongoClient(mongo_host)
psql_cursor = psycopg2.connect(host=psql_host, database="ghtorrent")