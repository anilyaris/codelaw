import pymongo
import psycopg2
import time
import keywords
from datetime import datetime
from multiprocessing import Pool
from database_connectors import Conntectors

pipelines = {
    "issue_comments": [ {"$project": {"id": 1, "updated_at": 1, "body": 1, "owner": 1, "repo": 1, "issue_id": 1} }],
    "commits": [ {"$project": {"sha": 1, "message": "$commit.message", "files.filename": 1, "files.patch": 1} }],
    #"issue_events": [ {"$project": {"url": 0, "actor": 0, "event": 0, "commit_id": 0, "commit_url": 0, "created_at": 0, "owner": 0, "repo": 0, "issue_id": 0} }, {"$redact": {"$cond": {"if": {"$gt": [{ "$size": { "$objectToArray": "$$CURRENT" } }, 2]}, "then": "$$KEEP", "else": "$$PRUNE" } } }],
    "issues": [ {"$project": {"number": 1, "title": 1, "state": 1, "updated_at": 1, "closed_at": 1, "body": 1, "closed_by.login": 1, "repo": 1, "owner": 1} }],
    "pull_requests": [ {"$project": {"number": 1, "merged_at": 1, "merged": 1, "merged_by.login": 1, "repo": 1, "owner": 1} }],
    "repos": [ {"$project": {"name": 1, "owner.login": 1, "private": 1, "pushed_at": 1, "homepage": 1, "has_wiki": 1, "has_pages": 1} }],
}

def sanitize(text):
    if text:
        text = text.replace('\x00', '')
    return text

def label_body(d):
    body_keyword_labels = keywords.generate_labels()
    keywords.match_keywords(d['body'], body_keyword_labels)
    d.pop('body')

    d['labels'] = list(body_keyword_labels.values())

def label_title_body(d):
    title_keyword_labels = keywords.generate_labels()
    keywords.match_keywords(d['title'], title_keyword_labels)
    title_keyword_flag = any(title_keyword_labels.values())
    d.pop('title')

    body_keyword_labels = keywords.generate_labels()
    keywords.match_keywords(d['body'], body_keyword_labels)
    d.pop('body')

    d['labels'] = [title_keyword_flag] + list(body_keyword_labels.values())

def label_commit(d):
    message_keyword_labels = keywords.generate_labels()
    keywords.match_keywords(d['message'], message_keyword_labels)
    message_keyword_flag = any(message_keyword_labels.values())
    d.pop('message')

    filename_keyword_flag = False
    filename_keyword_labels = keywords.generate_labels()
    patch_keyword_labels = keywords.generate_labels(quantifiers=["_added", "_removed"])

    if 'files' in d and d['files']:
        for f in d['files']:
            if not filename_keyword_flag:
                keywords.match_keywords(f['filename'], filename_keyword_labels)
                filename_keyword_flag = any(filename_keyword_labels.values())

            if 'patch' in f and f['patch']:
                patch_keyword_labels = keywords.generate_labels(f['patch'], patch_keyword_labels)

        d.pop('files')

    d['labels'] = [message_keyword_flag, filename_keyword_flag] + list(patch_keyword_labels.values())
    return d

def stringify(d, n, i=None, c=0, t=None):
    if n == "issue_comments":
        label_body(d)
        s = ','.join([
                        d['repo'], d['owner'], str(d['issue_id']), str(d['id']), 
                        "" if d['updated_at'] is None else d['updated_at'][:10] + ' ' + d['updated_at'][11:-1]]
                        + ['t' if l else 'f' for l in d['labels']
                    ])

    elif n == "commits":
        s = ','.join([
                        d['sha']] + ['t' if l else 'f' for l in d['labels']
                    ])

    elif n == "issues":
        label_title_body(d)
        s = ','.join([
                        d['repo'], d['owner'], str(d['number']), 't' if d['state'] == 'open' else 'f',
                        d['updated_at'][:10] + ' ' + d['updated_at'][11:-1], 
                        "" if d['closed_at'] is None else d['closed_at'][:10] + ' ' + d['closed_at'][11:-1],
                        d['closed_by']['login'] if 'closed_by' in d and d['closed_by'] else ""]
                        + ['t' if l else 'f' for l in d['labels']
                    ])

    elif n == "pull_requests":
        s = ','.join([
                        d['repo'], d['owner'], str(d['number']),
                        "" if d['merged_at'] is None else d['merged_at'][:10] + ' ' + d['merged_at'][11:-1], 't' if d['merged'] else 'f',
                        d['merged_by']['login'] if 'merged_by' in d and d['merged_by'] else ""
                    ])

    elif n == "repos":
        s = ','.join([
                        d['name'], d['owner']['login'], 't' if d['private'] else 'f',
                        "" if d['pushed_at'] is None else d['pushed_at'][:10] + ' ' + d['pushed_at'][11:-1],
                        't' if bool(d['homepage']) else 'f', 't' if d['has_wiki'] else 'f', 't' if d['has_pages'] else 'f'
                    ])

    if i:
        with open(n + ".txt", 'w') as f:
            f.write(n + ": Document %d of %d" % (i[0], c))
            if t:
                f.write("\nelapsed time: ")
                f.write(str(time.time() - t)) 
        i[0] += 1

    return s + '\n'

def fetchall(database_name, table_name, return_list):
    connectors = Conntectors()
    mongo_client = connectors.mongo_client

    #document_index = [1]
    #document_count = mongo_client[database_name][table_name].count_documents({})

    if table_name != "commits":
        return_list[:] = [stringify(d, table_name) for d in mongo_client[database_name][table_name].aggregate(pipelines[table_name])]
    
    else:
        pool = Pool(processes=10)
        return_list[:] = [stringify(d, table_name) for d in pool.imap_unordered(label_commit, mongo_client[database_name].commits.aggregate(pipelines["commits"]), 20000)]

        pool.close()
        pool.join()

    connectors.close()