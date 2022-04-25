import json
import pymongo
import psycopg2
from database_connectors import mongo_client, psql_cursor

def combine_fields(documents, combined = {}):
	for document in documents:
		for field in document:

			value = document[field]
			t = type(value)
			if field not in combined:
				combined[field] = []
			types = combined[field]

			if t == dict:
				i = 0
				while i < len(types):
					if type(types[i]) == dict:
						break
				if i == len(types):
					types.append({})
					
				types[i] = combine_fields([value], types[i])

			elif t not in types:
				types.append(t)

	return combined

def dump_type(o):
    return str(o)

def run():
    db = mongo_client.ghtorrent_stripped
    for name in db.list_collection_names():
        print("Analyzing", name)
        fields = combine_fields(db[name].find())
        with open(name + ".json", 'w') as f:
            json.dump(f, default=dump_type, indent=2)

if __name__ == "__main__":
    run()