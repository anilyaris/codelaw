import os
import sys
import tarfile
from urllib.request import urlopen

import pymongo
import psycopg2
from database_connectors import Conntectors

from date import Date
import aggregations
from multiprocessing import Process, Manager

from itertools import chain
from collections import deque
try:
    from reprlib import repr
except ImportError:
    pass

def total_size(o, handlers={}, verbose=False):
    """ Returns the approximate memory footprint an object and all of its contents.

    Automatically finds the contents of the following builtin containers and
    their subclasses:  tuple, list, deque, dict, set and frozenset.
    To search other containers, add handlers to iterate over their contents:

        handlers = {SomeContainerClass: iter,
                    OtherContainerClass: OtherContainerClass.get_elements}

    """
    dict_handler = lambda d: chain.from_iterable(d.items())
    all_handlers = {tuple: iter,
                    list: iter,
                    deque: iter,
                    dict: dict_handler,
                    set: iter,
                    frozenset: iter,
                   }
    all_handlers.update(handlers)     # user handlers take precedence
    seen = set()                      # track which object id's have already been seen
    default_size = sys.getsizeof(0)       # estimate sizeof object without __sizeof__

    def sizeof(o):
        if id(o) in seen:       # do not double count the same object
            return 0
        seen.add(id(o))
        s = sys.getsizeof(o, default_size)

        if verbose:
            print(s, type(o), repr(o))

        for typ, handler in all_handlers.items():
            if isinstance(o, typ):
                s += sum(map(sizeof, handler(o)))
                break
        return s

    return sizeof(o)

def run():
    connectors = Conntectors()
    manager = Manager()
    data_transfer_functions = aggregations.__dict__
    table_names = list(aggregations.pipelines.keys())
    table_batches = {name: [] for name in table_names}
    current_batch = [None] * len(table_names)
    batch_size = 0
    commits_index = table_names.index("commits")
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

    while line < len(dump_files) - 2: #dump_date != past_last_dump_date:

        stop = os.path.exists("stop.txt")
        if not stop:

            dump_file_name = dump_files[line][9:37] #"mongo-dump-" + str(dump_date) + ".tar.gz"
            dump_file_url = main_page_url + dump_file_name
            dump_file_path = "/cluster/scratch/ayaris/" + dump_file_name
            extraction_directory = "/cluster/scratch/ayaris/dump/github/"            
            extraction_success = True
            restore = False

            if processes[0] is not None or "ghtorrent" not in connectors.mongo_client.list_database_names():

                print("Downloading", dump_file_name)
                os.system("wget -q -O %s %s" % (dump_file_path, dump_file_url))

                print("Extracting", dump_file_name)
                try:
                    with tarfile.open(dump_file_path) as f:
                        f.extractall(extraction_directory + "../..")
                        restore = True
                except Exception as e:
                        print("Cannot open/extract archive, saving to retry.txt")
                        with open("retry.txt", 'a' if os.path.exists("retry.txt") else 'w') as f:
                            f.writelines([dump_file_name, ": ", str(e), "\n"])
                        extraction_success = False
                
                print("Removing archive")
                os.remove(dump_file_path)

        if extraction_success or stop:
            drop = False
            for index in range(len(table_names)):
                if processes[index] is not None:
                    processes[index].join()
                    processes[index] = None
                    drop = True

            if drop:
                current_batch_size = total_size(current_batch, handlers={type(current_batch[0]): iter})
                for index in range(len(table_names)):
                    table_batches[table_names[index]] += current_batch[index]
                    current_batch[index] = None
                connectors.mongo_client.drop_database("ghtorrent")
                
                batch_size += current_batch_size
                if stop or batch_size > 16 * 1024 * 1024 * 1024:
                    for index in range(len(table_names)):                
                        table_name = table_names[index]
                        processes[index] = Process(data_transfer_functions("process_" + table_name), args=(table_batches[table_name], ))
                        processes[index].start()

                    for index in range(len(table_names)):
                        if processes[index] is not None:                
                            table_name = table_names[index]
                            processes[index].join()
                            table_batches[table_name] = []
                            processes[index] = None
                    

        if stop:
            break

        if restore:
            print("Removing unneccessary databases")
            for i in os.listdir(extraction_directory):
                if os.path.splitext(os.path.splitext(i)[0])[0] not in table_names:
                    os.remove(extraction_directory + i)

            print("Restoring dump")
            os.system("/cluster/home/ayaris/mongodb-linux-x86_64-rhel70-5.0.7/bin/mongorestore --quiet --host=%s -d ghtorrent /cluster/scratch/ayaris/dump/github/" % connectors.mongo_client.address[0])

        print("Clearing extraction directory")
        for i in os.listdir(extraction_directory):
            os.remove(extraction_directory + i)        

        if extraction_success:
            for index in range(len(table_names)):                
                table_name = table_names[index]
                current_batch[index] = manager.list()
                processes[index] = Process(target=aggregations.fetchall, args=(table_name, current_batch[index]))
                processes[index].start()            

        line += 1

        #with open("start_line.txt", 'w') as f:
        #    f.write("%d" % line)

    connectors.close()

if __name__ == "__main__":
    run()
