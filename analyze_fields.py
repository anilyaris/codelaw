import os
import json
import pymongo
import psycopg2
from database_connectors import mongo_client, psql_cursor

SAMPLE_SIZE = 5000000

def combine_fields(documents, combined = {}, document_count = -1, collection_name = ""):
        
	document_index = 1
	for document in documents:
		if document_count >= 0:
			with open("log.txt", 'w') as f:
				f.write((collection_name + ": " if collection_name else "") + "Document %d of %d" % (document_index, document_count))
			document_index += 1

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
					i += 1
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
		if not os.path.exists(name + '.json'):
			cursor = db[name].aggregate([ { "$sample": { "size": SAMPLE_SIZE } } ])
			count = min(db[name].count_documents({}), SAMPLE_SIZE)
			fields = combine_fields(cursor, document_count=count, collection_name=name)
			with open(name + ".json", 'w') as f:
				json.dump(fields, fp=f, default=dump_type, indent=2)

if __name__ == "__main__":
    run()