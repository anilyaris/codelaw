import os
import sys
import tarfile
from urllib.request import urlopen

import pymongo
import psycopg2
from database_connectors import Conntectors

import aggregations
from multiprocessing import Process, Manager
from filelock import FileLock

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
    default_size = sys.getsizeof(0)       # estimate sizeof object without __sizeof__

    def sizeof(o):
        s = sys.getsizeof(o, default_size)

        if verbose:
            print(s, type(o), repr(o))

        for typ, handler in all_handlers.items():
            if isinstance(o, typ):
                s += sum(map(sizeof, handler(o)))
                break
        return s

    return sizeof(o)

def write_batch(batch, file):
    lock = FileLock(file + ".lock")
    with lock: 
        with open(file, 'a' if os.path.exists(file) else 'w') as f:
            f.writelines(batch)

def run():
    connectors = Conntectors()
    manager = Manager()
    table_names = list(aggregations.pipelines.keys())
    table_batches = {name: [] for name in table_names}
    current_batch = [None] * len(table_names)
    batch_size = 0
    processes = [None] * len(table_names)
    
    direction_oldtonew = True
    if len(sys.argv) > 1:
        d = sys.argv[1][0] 
        direction_oldtonew = d == 'o' or d == '0'

    main_page_url = "http://ghtorrent-downloads.ewi.tudelft.nl/mongo-daily/"
    with urlopen(main_page_url) as f:
        dump_files = f.read().decode("utf8").splitlines()
 
    if os.path.exists("stop.txt"):
        os.remove("stop.txt")    

    if direction_oldtonew:
        if os.path.exists("start_line.txt"):
            with open("start_line.txt", 'r') as f:
                line = int(f.read())
        else:
            line = 5

    else:
        if os.path.exists("end_line.txt"):
            with open("end_line.txt", 'r') as f:
                line = int(f.read())
        else:
            line = len(dump_files) - 3

    database_name = "ghtorrent"
    if direction_oldtonew:
        database_name += "2"
    extraction_directory = "/cluster/scratch/ayaris/%s/dump/github/" % database_name

    cutoff = len(dump_files) // 2 + 1
    while (direction_oldtonew and line <= cutoff) or (not direction_oldtonew and line > cutoff):

        stop = os.path.exists("stop.txt")
        if not stop:

            dump_file_name = dump_files[line][9:37] #"mongo-dump-" + str(dump_date) + ".tar.gz"
            dump_file_url = main_page_url + dump_file_name
            dump_file_path = "/cluster/scratch/ayaris/" + dump_file_name            
            extraction_success = True
            restore = False

            if processes[0] is not None or database_name not in connectors.mongo_client.list_database_names():

                print("Downloading", dump_file_name)
                os.system("wget -q -O %s %s" % (dump_file_path, dump_file_url))

                print("Extracting", dump_file_name)
                try:
                    with tarfile.open(dump_file_path) as f:
                        f.extractall(extraction_directory + "../..")
                        restore = True
                except Exception as e:
                        print("Cannot open/extract archive, saving to retry.txt")
                        lock = FileLock("retry.lock")
                        with lock: 
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
                connectors.mongo_client.drop_database(database_name)
                
                batch_size += current_batch_size
                if stop or batch_size > 0 * 16 * 1024 * 1024 * 1024:
                    for index in range(len(table_names)):                
                        table_name = table_names[index]
                        processes[index] = Process(target=write_batch, args=(table_batches[table_name], "/cluster/scratch/ayaris/csvs/%s.csv" % table_name))
                        processes[index].start()

                    for index in range(len(table_names)):
                        if processes[index] is not None:                
                            table_name = table_names[index]
                            processes[index].join()
                            table_batches[table_name] = []
                            processes[index] = None

                    with open("start_line.txt" if direction_oldtonew else "end_line.txt", 'w') as f:
                        f.write("%d" % line)
                    
        if stop:
            break

        if restore:
            print("Removing unneccessary databases")
            for i in os.listdir(extraction_directory):
                if os.path.splitext(os.path.splitext(i)[0])[0] not in table_names:
                    os.remove(extraction_directory + i)

            print("Restoring dump")
            os.system("/cluster/home/ayaris/mongodb-linux-x86_64-rhel70-5.0.7/bin/mongorestore --quiet --host=%s -d %s %s" % (connectors.mongo_client.address[0], database_name, extraction_directory))

        print("Clearing extraction directory")
        for i in os.listdir(extraction_directory):
            os.remove(extraction_directory + i)        

        if extraction_success:
            for index in range(len(table_names)):                
                table_name = table_names[index]
                current_batch[index] = manager.list()
                processes[index] = Process(target=aggregations.fetchall, args=(database_name, table_name, current_batch[index]))
                processes[index].start()            

        if direction_oldtonew:
            line += 1
        else:
            line -= 1

    connectors.close()

if __name__ == "__main__":
    run()
