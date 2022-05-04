import pymongo
import psycopg2
import keywords
from datetime import datetime
from database_connectors import mongo_client, psql_conn

pipelines = {
    "issue_comments": [ {"$project": {"id": 1, "body": 1} }],
    "commits": [ {"$project": {"sha": 1, "message": "$commit.message", "files.sha": 1, "files.filename": 1, "files.status": 1, "files.patch": 1} }],
    "issue_events": [ {"$project": {"url": 0, "actor": 0, "event": 0, "commit_id": 0, "commit_url": 0, "created_at": 0, "owner": 0, "repo": 0, "issue_id": 0} }, {"$redact": {"$cond": {"if": {"$gt": [{ "$size": { "$objectToArray": "$$CURRENT" } }, 2]}, "then": "$$KEEP", "else": "$$PRUNE" } } }],
    "issues": [ {"$project": {"id": 1, "number": 1, "title": 1, "labels.name": 1, "state": 1, "locked": 1, "milestone": 1, "updated_at": 1, "closed_at": 1, "body": 1, "closed_by.login": 1, "closed_by.id": 1, "repo": 1, "owner": 1} }],
    "pull_requests": [ {"$project": {"id": 1, "number": 1, "state": 1, "locked": 1, "merged_at": 1, "merged": 1, "merged_by.id": 1, "merged_by.login": 1, "repo": 1, "owner": 1} }],
    "repos": [ {"$project": {"id": 1, "name": 1, "owner.login": 1, "owner.id": 1, "private": 1, "pushed_at": 1, "homepage": 1, "has_wiki": 1, "has_pages": 1} }],
}

def process_issue_comments():
    
    cursor = psql_conn.cursor()
    document_count = mongo_client.ghtorrent.issue_comments.count_documents({})
    document_index = 1

    for d in mongo_client.ghtorrent.issue_comments.aggregate(pipelines["issue_comments"]):
        with open("issue_comments.txt", 'w') as f:
            f.write("issue_comments: Document %d of %d" % (document_index, document_count))
        document_index += 1
        
        cursor.execute("UPDATE issue_comments \
            SET body = (%s) \
            WHERE comment_id = (%s)",
            (
                d['body'], 
                str(d['id'])
            )
        )

    psql_conn.commit()
    cursor.close()

def process_commits():
    
    cursor = psql_conn.cursor()
    document_count = mongo_client.ghtorrent.commits.count_documents({})
    document_index = 1

    for d in mongo_client.ghtorrent.commits.aggregate(pipelines["commits"]):
        with open("commits.txt", 'w') as f:
            f.write("commits: Document %d of %d" % (document_index, document_count))
        document_index += 1

        filename_keyword_flag = False
        patch_keyword_labels = {}

        for f in d['files']:
            for k in keywords.keywords:
                filename_keyword_flag = filename_keyword_flag or keywords.match_keyword(f['filename'], k)

            patch_keyword_labels = keywords.generate_labels(f['patch'], patch_keyword_labels)

        cursor.execute("UPDATE commits \
            SET message = (%s), any_filename_contains_any_keyword = (%s), \
            gdpr_added = (%s), gdpr_removed = (%s), \
            rgpd_added = (%s), rgpd_removed = (%s), \
            dsgvo_added = (%s), dsgvo_removed = (%s), \
            ccpa_added = (%s), ccpa_removed = (%s), \
            cpra_added = (%s), cpra_removed = (%s), \
            privacy_added = (%s), privacy_removed = (%s), \
            data_protection_added = (%s), data_protection_removed = (%s), \
            compliance_added = (%s), compliance_removed = (%s), \
            legal_added = (%s), legal_removed = (%s), \
            consent_added = (%s), consent_removed = (%s), \
            law_added = (%s), law_removed = (%s), \
            statute_added = (%s), statute_removed = (%s), \
            personal_data_added = (%s), personal_data_removed = (%s) \
            WHERE sha = (%s)", 
            (
                d['message'], filename_keyword_flag, 
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
                patch_keyword_labels['personal_data_added'], patch_keyword_labels['personal_data_removed'],
                d['sha']
            )
        )

    psql_conn.commit()
    cursor.close()

def process_issue_events():
    return

def process_issues():
    
    cursor = psql_conn.cursor()
    document_count = mongo_client.ghtorrent.issues.count_documents({})
    document_index = 1

    for d in mongo_client.ghtorrent.issues.aggregate(pipelines["issues"]):
        with open("issues.txt", 'w') as f:
            f.write("issues: Document %d of %d" % (document_index, document_count))
        document_index += 1
        
        query = "WITH r AS ( \
            SELECT p.id FROM users u, projects p \
            WHERE p.name = (%s) AND u.login = (%s) AND p.owner_id = u.id \
            )" + (", extant AS ( \
            SELECT id FROM repo_milestones WHERE repo_id = r.id AND name = (%s) \
            ), inserted AS ( \
            INSERT INTO repo_milestones(repo_id, name, description) \
            SELECT r.id, (%s), (%s) FROM r \
            WHERE NOT EXISTS (SELECT NULL FROM extant)  \
            RETURNING id \
            )" if 'milestone' in d and d['milestone'] else "") + " UPDATE issues i \
            SET title = (%s), open = (%s), " + ("milestone_id = m.id, " if 'milestone' in d and d['milestone'] else "") + "updated_at = (%s), closed_at = (%s), body = (%s)" + (", closer_id = u.id" if 'closed_by' in d and d['closed_by'] else "") + " \
            FROM r" + (", users u" if 'closed_by' in d and d['closed_by'] else "") + (", (SELECT id FROM inserted UNION ALL SELECT id FROM extant) AS m" if 'milestone' in d and d['milestone'] else "") + " \
            WHERE i.repo_id = r.id AND i.issue_id = (%s)" + (" AND u.login = (%s)" if 'closed_by' in d and d['closed_by'] else "")
            
        vars = [
            d['repo'], d['owner']] + ([
            d['milestone']['name'],
            d['milestone']['name'], d['milestone']['description']] if 'milestone' in d and d['milestone'] else []) + [
            d['title'], d['state'] == 'open', datetime.strptime(d['updated_at'], "%Y-%m-%dT%H:%M:%SZ"), None if d['closed_at'] is None else datetime.strptime(d['closed_at'], "%Y-%m-%dT%H:%M:%SZ"), d['body'],
            d['number']
        ] + ([d['closed_by']['login']] if 'closed_by'in d and d['closed_by'] else [])

        cursor.execute(query, vars)

    psql_conn.commit()
    cursor.close()

def process_pull_requests():

    cursor = psql_conn.cursor()
    document_count = mongo_client.ghtorrent.pull_requests.count_documents({})
    document_index = 1

    for d in mongo_client.ghtorrent.pull_requests.aggregate(pipelines["pull_requests"]):
        with open("pull_requests.txt", 'w') as f:
            f.write("pull_requests: Document %d of %d" % (document_index, document_count))
        document_index += 1
        
        query = "WITH r AS ( \
            SELECT p.id FROM users u, projects p \
            WHERE p.name = (%s) AND u.login = (%s) AND p.owner_id = u.id \
            ) UPDATE pull_requests i \
            SET merged_at = (%s), merged = (%s)" + (", merger_id = u.id" if 'merged_by' in d and d['merged_by'] else "") + " \
            FROM r" + (", users u, " if 'merged_by' in d and d['merged_by'] else "") + " \
            WHERE i.base_repo_id = r.id AND i.pullreq_id = (%s)" + (" AND u.login = (%s)" if 'merged_by' in d and d['merged_by'] else "")
            
        vars = [
                d['repo'], d['owner'], 
                None if d['merged_at'] is None else datetime.strptime(d['merged_at'], "%Y-%m-%dT%H:%M:%SZ"), d['merged'],
                d['number'] 
            ] + ([d['merged_by']['login']] if 'merged_by' in d and d['merged_by'] else [])

        cursor.execute(query, vars)

    psql_conn.commit()
    cursor.close()

def process_repos():
    
    cursor = psql_conn.cursor()
    document_count = mongo_client.ghtorrent.repos.count_documents({})
    document_index = 1

    for d in mongo_client.ghtorrent.repos.aggregate(pipelines["repos"]):
        with open("repos.txt", 'w') as f:
            f.write("repos: Document %d of %d" % (document_index, document_count))
        document_index += 1
        
        cursor.execute("UPDATE projects p \
            SET private = (%s), pushed_at = (%s), has_homepage = (%s), has_wiki = (%s), has_pages = (%s) \
            FROM users u \
            WHERE p.name = (%s) AND u.login = (%s) AND p.owner_id = u.id",
            (
                d['private'], None if d['pushed_at'] is None else datetime.strptime(d['pushed_at'], "%Y-%m-%dT%H:%M:%SZ"), bool(d['homepage']), d['has_wiki'], d['has_pages'],  
                d['name'], d['owner']['login']
            )
        )

    psql_conn.commit()
    cursor.close()
