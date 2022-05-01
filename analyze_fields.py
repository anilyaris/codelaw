import os
import sys
import json
import pymongo
import psycopg2
from database_connectors import mongo_client, psql_cursor

document_counts = {
	"issue_comments": 13285419,
	"commits": 110788421,
	"issue_events": 5392129,
	"issues": 7038929,
	"pull_requests": 5049699,
	"repos": 29975338
}

def combine_fields(documents, combined = {}, document_count = -1, collection_name = ""):
	if document_count >= 0 and os.path.exists(collection_name + ".txt"):
		os.remove(collection_name + ".txt")

	document_index = 1
	for document in documents:
		if document_count >= 0:
			with open(collection_name + ".txt", 'w') as f:
				f.write((collection_name + ": " if collection_name else "") + "Document %d of %d" % (document_index, document_count))
			document_index += 1

			if document_index % 20000 != 1:
				continue

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
	names = sys.argv[1:] if len(sys.argv) > 1 else document_counts.keys()
	for name in names:
		if not os.path.exists(name + '.json'):
			print("Analyzing", name)
			cursor = db[name].find()
			count = document_counts[name]
			fields = combine_fields(cursor, {}, document_count=count, collection_name=name)
			with open(name + ".json", 'w') as f:
				json.dump(fields, fp=f, default=dump_type, indent=2)

if __name__ == "__main__":
    run()