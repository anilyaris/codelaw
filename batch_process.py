import os
import sys
import tarfile
from urllib.request import urlopen

import pymongo
import psycopg2
from database_connectors import Conntectors

from date import Date
import aggregations
from multiprocessing import Process

def run():
    connectors = Conntectors()
    data_transfer_functions = aggregations.__dict__
    table_names = list(aggregations.pipelines.keys())
    processes = [None] * len(table_names)
 
    if os.path.exists("stop.txt"):
        os.remove("stop.txt")    

    if os.path.exists("start_line.txt"):
        with open("start_line.txt", 'r') as f:
            line = int(f.read())
    else:
        line = 5

    if os.path.exists("past_end_date.txt"):
        with open("past_end_date.txt", 'r') as f:
            past_last_dump_date = Date(int(f.readline()[:-1]), int(f.readline()[:-1]), int(f.readline())) 
    else:
        past_last_dump_date = Date(7, 3, 2021)

    main_page_url = "http://ghtorrent-downloads.ewi.tudelft.nl/mongo-daily/"
    with urlopen(main_page_url) as f:
        dump_files = f.read().decode("utf8").splitlines()

    while line < len(dump_files): #dump_date != past_last_dump_date:

        if os.path.exists("stop.txt"):
            break

        dump_file_name = dump_files[line][9:37] #"mongo-dump-" + str(dump_date) + ".tar.gz"
        dump_file_url = main_page_url + dump_file_name
        dump_file_path = "/cluster/scratch/ayaris/" + dump_file_name
        extraction_directory = "/cluster/scratch/ayaris/dump/github/"            
        extraction_success = True

        if "ghtorrent" not in connectors.mongo_client.list_database_names():

            print("Downloading", dump_file_name)
            os.system("wget -q -O %s %s" % (dump_file_path, dump_file_url))

            print("Extracting", dump_file_name)
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
                os.system("/cluster/home/ayaris/mongodb-linux-x86_64-rhel70-5.0.7/bin/mongorestore --quiet --host=%s -d ghtorrent /cluster/scratch/ayaris/dump/github/" % connectors.mongo_client.address[0])

            print("Clearing extraction directory")
            for i in os.listdir(extraction_directory):
                os.remove(extraction_directory + i)

        if extraction_success:
            for index in range(len(table_names)):
                table_name = table_names[index]
                print("Processing", table_name)

                processes[index] = Process(target=data_transfer_functions["process_" + table_name])
                processes[index].start()

            for index in range(len(table_names)):
                processes[index].join()

            connectors.mongo_client.drop_database("ghtorrent")

        line += 1

        with open("start_line.txt", 'w') as f:
            f.write("%d" % line)

    connectors.close()

if __name__ == "__main__":
    run()
