import pymongo
import psycopg2
import keywords
from datetime import datetime
from database_connectors import Conntectors

pipelines = {
    "issue_comments": [ {"$project": {"id": 1, "updated_at": 1, "body": 1, "owner": 1, "repo": 1, "issue_id": 1} }],
    "commits": [ {"$project": {"sha": 1, "message": "$commit.message", "files.sha": 1, "files.filename": 1, "files.status": 1, "files.patch": 1} }],
    "issue_events": [ {"$project": {"url": 0, "actor": 0, "event": 0, "commit_id": 0, "commit_url": 0, "created_at": 0, "owner": 0, "repo": 0, "issue_id": 0} }, {"$redact": {"$cond": {"if": {"$gt": [{ "$size": { "$objectToArray": "$$CURRENT" } }, 2]}, "then": "$$KEEP", "else": "$$PRUNE" } } }],
    "issues": [ {"$project": {"id": 1, "number": 1, "title": 1, "labels.name": 1, "state": 1, "locked": 1, "milestone": 1, "updated_at": 1, "closed_at": 1, "body": 1, "closed_by.login": 1, "closed_by.id": 1, "repo": 1, "owner": 1} }],
    "pull_requests": [ {"$project": {"id": 1, "number": 1, "state": 1, "locked": 1, "merged_at": 1, "merged": 1, "merged_by.id": 1, "merged_by.login": 1, "repo": 1, "owner": 1} }],
    "repos": [ {"$project": {"id": 1, "name": 1, "owner.login": 1, "owner.id": 1, "private": 1, "pushed_at": 1, "homepage": 1, "has_wiki": 1, "has_pages": 1} }],
}

def sanitize(text):
    if text:
        text = text.replace('\x00', '')
    return text

def process_issue_comments():
    
    connectors = Conntectors()
    psql_conn = connectors.psql_conn
    mongo_client = connectors.mongo_client

    cursor = psql_conn.cursor()
    document_count = mongo_client.ghtorrent.issue_comments.count_documents({})
    document_index = 1

    for d in mongo_client.ghtorrent.issue_comments.aggregate(pipelines["issue_comments"]):
        with open("issue_comments.txt", 'w') as f:
            f.write("issue_comments: Document %d of %d" % (document_index, document_count))
        document_index += 1
        
        cursor.execute("INSERT INTO mongo_issue_comments VALUES ( \
            (%s), (%s), (%s), (%s), \
            (%s), (%s) \
            )",
            (
                d['repo'], d['owner'], d['issue_id'], str(d['id']), 
                None if d['updated_at'] is None else datetime.strptime(d['updated_at'], "%Y-%m-%dT%H:%M:%SZ"), sanitize(d['body'])
            )
        )

    psql_conn.commit()
    cursor.close()
    mongo_client.ghtorrent.issue_comments.drop()
    connectors.close()

def process_commits():

    connectors = Conntectors()
    psql_conn = connectors.psql_conn
    mongo_client = connectors.mongo_client
    
    cursor = psql_conn.cursor()
    document_count = mongo_client.ghtorrent.commits.count_documents({})
    document_index = 1

    for d in mongo_client.ghtorrent.commits.aggregate(pipelines["commits"]):
        with open("commits.txt", 'w') as f:
            f.write("commits: Document %d of %d" % (document_index, document_count))
        document_index += 1

        filename_keyword_flag = False
        patch_keyword_labels = keywords.generate_labels("")

        if 'files' in d and d['files']:
            for f in d['files']:
                for k in keywords.keywords:
                    filename_keyword_flag = filename_keyword_flag or keywords.match_keyword(f['filename'], k)

                if 'patch' in f and f['patch']:
                    patch_keyword_labels = keywords.generate_labels(f['patch'], patch_keyword_labels)

        cursor.execute("INSERT INTO mongo_commits VALUES ( \
            (%s), (%s), (%s), \
            (%s), (%s), \
            (%s), (%s), \
            (%s), (%s), \
            (%s), (%s), \
            (%s), (%s), \
            (%s), (%s), \
            (%s), (%s), \
            (%s), (%s), \
            (%s), (%s), \
            (%s), (%s), \
            (%s), (%s), \
            (%s), (%s), \
            (%s), (%s) \
            )", 
            (
                d['sha'], sanitize(d['message']), filename_keyword_flag, 
                patch_keyword_labels['gdpr_added'], patch_keyword_labels['gdpr_removed'],
                patch_keyword_labels['rgpd_added'], patch_keyword_labels['rgpd_removed'], 
                patch_keyword_labels['dsgvo_added'], patch_keyword_labels['dsgvo_removed'],
                patch_keyword_labels['ccpa_added'], patch_keyword_labels['ccpa_removed'],
                patch_keyword_labels['cpra_added'], patch_keyword_labels['cpra_removed'],
                patch_keyword_labels['privacy_added'], patch_keyword_labels['privacy_removed'],
                patch_keyword_labels['data_protection_added'], patch_keyword_labels['data_protection_removed'],
                patch_keyword_labels['compliance_added'], patch_keyword_labels['compliance_removed'],
                patch_keyword_labels['legal_added'], patch_keyword_labels['legal_removed'],
                patch_keyword_labels['consent_added'], patch_keyword_labels['consent_removed'],
                patch_keyword_labels['law_added'], patch_keyword_labels['law_removed'],
                patch_keyword_labels['statute_added'], patch_keyword_labels['statute_removed'],
                patch_keyword_labels['personal_data_added'], patch_keyword_labels['personal_data_removed']                
            )
        )

    psql_conn.commit()
    cursor.close()
    mongo_client.ghtorrent.commits.drop()
    connectors.close()

def process_issue_events():
    
    connectors = Conntectors()
    psql_conn = connectors.psql_conn
    mongo_client = connectors.mongo_client

    mongo_client.ghtorrent.issue_events.drop()
    connectors.close()

def process_issues():
    
    connectors = Conntectors()
    psql_conn = connectors.psql_conn
    mongo_client = connectors.mongo_client

    cursor = psql_conn.cursor()
    document_count = mongo_client.ghtorrent.issues.count_documents({})
    document_index = 1

    for d in mongo_client.ghtorrent.issues.aggregate(pipelines["issues"]):
        with open("issues.txt", 'w') as f:
            f.write("issues: Document %d of %d" % (document_index, document_count))
        document_index += 1
        
        cursor.execute("INSERT INTO mongo_issues VALUES ( \
            (%s), (%s), (%s), \
            (%s), (%s), NULL, \
            (%s), (%s), \
            (%s), (%s) \
            )",
            (
                d['repo'], d['owner'], d['number'], 
                sanitize(d['title']), d['state'] == 'open', 
                datetime.strptime(d['updated_at'], "%Y-%m-%dT%H:%M:%SZ"), None if d['closed_at'] is None else datetime.strptime(d['closed_at'], "%Y-%m-%dT%H:%M:%SZ"), 
                sanitize(d['body']), d['closed_by']['login'] if 'closed_by'in d and d['closed_by'] else None             
            )
        )

    psql_conn.commit()
    cursor.close()
    mongo_client.ghtorrent.issues.drop()
    connectors.close()

def process_pull_requests():

    connectors = Conntectors()
    psql_conn = connectors.psql_conn
    mongo_client = connectors.mongo_client
    
    cursor = psql_conn.cursor()
    document_count = mongo_client.ghtorrent.pull_requests.count_documents({})
    document_index = 1

    for d in mongo_client.ghtorrent.pull_requests.aggregate(pipelines["pull_requests"]):
        with open("pull_requests.txt", 'w') as f:
            f.write("pull_requests: Document %d of %d" % (document_index, document_count))
        document_index += 1
        
        cursor.execute("INSERT INTO mongo_pull_requests VALUES ( \
            (%s), (%s), (%s), \
            (%s), (%s), \
            (%s) \
            )",
            (
                d['repo'], d['owner'], d['number'], 
                None if d['merged_at'] is None else datetime.strptime(d['merged_at'], "%Y-%m-%dT%H:%M:%SZ"), d['merged'],
                d['merged_by']['login'] if 'merged_by'in d and d['merged_by'] else None
            )
        )

    psql_conn.commit()
    cursor.close()
    mongo_client.ghtorrent.pull_requests.drop()
    connectors.close()

def process_repos():

    connectors = Conntectors()
    psql_conn = connectors.psql_conn
    mongo_client = connectors.mongo_client
    
    cursor = psql_conn.cursor()
    document_count = mongo_client.ghtorrent.repos.count_documents({})
    document_index = 1

    for d in mongo_client.ghtorrent.repos.aggregate(pipelines["repos"]):
        with open("repos.txt", 'w') as f:
            f.write("repos: Document %d of %d" % (document_index, document_count))
        document_index += 1
        
        cursor.execute("INSERT INTO mongo_projects VALUES ( \
            (%s), (%s), \
            (%s), (%s), \
            (%s), (%s), (%s) \
            )",
            (
                d['name'], d['owner']['login'],
                d['private'], None if d['pushed_at'] is None else datetime.strptime(d['pushed_at'], "%Y-%m-%dT%H:%M:%SZ"), 
                bool(d['homepage']), d['has_wiki'], d['has_pages']                
            )
        )

    psql_conn.commit()
    cursor.close()
    mongo_client.ghtorrent.repos.drop()
    connectors.close()
