import pymongo
import psycopg2

with open("/cluster/home/ayaris/mongohost", 'r') as f:
    mongo_host = f.read()[:-1]
with open("/cluster/home/ayaris/logfile", 'r') as f:
    psql_host = f.readlines()[0][:-1]

try:
    mongo_client = pymongo.MongoClient(mongo_host)
except Exception as e:
    mongo_client = None

try:
    psql_conn = psycopg2.connect(host=psql_host, database="ghtorrent", user="ghtorrent")
except Exception as e:
    psql_conn = None