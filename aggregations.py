pipelines = {
    "issue_comments": [ {"$project": {"id": 1, "body": 1} }],
    "commits": [ {"$project": {"sha": 1, "message": "$commit.message", "files.sha": 1, "files.filename": 1, "files.status": 1, "files.patch": 1} }],
    "issue_events": [ {"$project": {"url": 0, "actor": 0, "event": 0, "commit_id": 0, "commit_url": 0, "created_at": 0, "owner": 0, "repo": 0, "issue_id": 0} }, {"$redact": {"$cond": {"if": {"$gt": [{ "$size": { "$objectToArray": "$$CURRENT" } }, 2]}, "then": "$$KEEP", "else": "$$PRUNE" } } }],
    "issues": [ {"$project": {"id": 1, "title": 1, "labels.name": 1, "state": 1, "locked": 1, "milestone": 1, "updated_at": 1, "closed_at": 1, "body": 1, "closed_by.id": 1} }],
    "pull_requests": [ {"$project": {"id": 1, "number": 1, "state": 1, "locked": 1, "title": 1, "body": 1, "created_at": 1, "updated_at": 1, "closed_at": 1, "merged_at": 1, "assignee.login": 1, "assignee.id": 1, "milestone": 1, "base.user.id": 1, "base.user.login": 1, "base.repo.id": 1, "base.repo.name": 1, "merged": 1, "merged_by.id": 1, "merged_by.login": 1} }],
    "repos": [ {"$project": {"id": 1, "name": 1, "owner.login": 1, "owner.id": 1, "private": 1, "pushed_at": 1, "homepage": 1, "has_wiki": 1, "has_pages": 1} }],
}