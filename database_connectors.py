import pymongo
import psycopg2

class Conntectors:
    def __init__(self):
        with open("/cluster/home/ayaris/mongohost", 'r') as f:
            mongo_host = f.read()[:-1]
        with open("/cluster/home/ayaris/logfile", 'r') as f:
            psql_host = f.readlines()[0][:-1]

        try:
            self.mongo_client = pymongo.MongoClient(mongo_host)
        except Exception as e:
            self.mongo_client = None

        try:
            self.psql_conn = psycopg2.connect(host=psql_host, database="ghtorrent", user="ghtorrent")
        except Exception as e:
            self.psql_conn = None

    def close(self):
        self.mongo_client.close()
        self.psql_conn.close()