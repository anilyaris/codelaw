import os
import sys
import tarfile

import pymongo
import psycopg2
from database_connectors import mongo_client, psql_cursor

from date import Date
from aggregations import pipelines

def run():
    if os.path.exists("stop.txt"):
        os.remove("stop.txt")

    if os.path.exists("start_date.txt"):
        with open("start_date.txt", 'r') as f:
            dump_date = Date(int(f.readline()[:-1]), int(f.readline()[:-1]), int(f.readline())) 
    else:
        dump_date = Date(1, 12, 2015)

    if os.path.exists("past_end_date.txt"):
        with open("past_end_date.txt", 'r') as f:
            past_last_dump_date = Date(int(f.readline()[:-1]), int(f.readline()[:-1]), int(f.readline())) 
    else:
        past_last_dump_date = Date(25, 5, 2021)

    while dump_date != past_last_dump_date:

        if os.path.exists("stop.txt"):
            break

        dump_file_name = "mongo-dump-" + str(dump_date) + ".tar.gz"
        dump_file_url = "http://ghtorrent-downloads.ewi.tudelft.nl/mongo-daily/" + dump_file_name
        dump_file_path = "/cluster/scratch/ayaris/" + dump_file_name
        extraction_directory = "/cluster/scratch/ayaris/dump/github/"
        
        print("Downloading", dump_file_name)
        os.system("wget -q -O %s %s" % (dump_file_path, dump_file_url))

        print ("Extracting", dump_file_name)
        extraction_success = True
        try:
            with tarfile.open(dump_file_path) as f:
                f.extractall(extraction_directory + "../..")
        except Exception as e:
                print("Cannot open/extract archive, saving to retry.txt")
                with open("retry.txt", 'a' if os.path.exists("retry.txt") else 'w') as f:
                    f.writelines([dump_file_name, ": ", str(e), "\n"])
                extraction_success = False
        
        print("Removing archive")
        os.remove(dump_file_path)

        if extraction_success:
            print("Restoring dump")
            os.system("/cluster/home/ayaris/mongodb-linux-x86_64-rhel70-5.0.7/bin/mongorestore --quiet --host=%s -d ghtorrent /cluster/scratch/ayaris/dump/github/" % mongo_client.address[0])

        print("Clearing extraction directory")
        for i in os.listdir(extraction_directory):
            os.remove(extraction_directory + i)

        if extraction_success:
            base = mongo_client.ghtorrent
            stripped = mongo_client.ghtorrent_stripped
            for table_name in pipelines:
                print("Processing", table_name)
                stripped[table_name].insert_many(base[table_name].aggregate(pipelines[table_name]))

            mongo_client.drop_database("ghtorrent")

        dump_date = dump_date + 1

        with open("start_date.txt", 'w') as f:
            f.write("%d\n%d\n%d" % (dump_date.d, dump_date.m, dump_date.y))

if __name__ == "__main__":
    run()