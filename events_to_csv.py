import json
import os
import sys
from filelock import FileLock
from multiprocessing import Process

def process_events(filename):
	new_issues = []
	new_comments = []
	extended_issues = []
	extended_comments = []

	with open(filename, 'r') as f:
		events = json.load(f)
		for event in events:
			if event['type'] == 'IssueCommentEvent':
				comment = event['payload']['comment']
				url = comment['html_url']
				index = url.rfind('-')
				comment_id = url[index + 1:]
				index = url.rfind('#', 0, index)
				issue_id = url[url.rfind('/', 0, index) + 1:index]
				index = url.rfind('/', 0, index)
				index = url.rfind('/', 0, index)
				repo = url[url.rfind('/', 0, index) + 1:index]
				index = url.rfind('/', 0, index)
				owner = url[url.rfind('/', 0, index) + 1:index]

				created_at = comment['created_at'] if 'created_at' in comment else ""
				if created_at:
					created_at = created_at[:10] + ' ' + created_at[11:-1]

				updated_at = comment['updated_at'] if 'updated_at' in comment else ""
				if updated_at:
					updated_at = updated_at[:10] + ' ' + updated_at[11:-1]

				s = ','.join([
					repo, owner, issue_id, comment_id, updated_at]
					+ ['t' if l else 'f' for l in event['body_labels'].values()
				])
				new_comments.append(s +'\n')

				s = ','.join([
						repo, owner, issue_id, comment_id, created_at
					])
				extended_comments.append(s +'\n')

			else:
				issue = event['payload']['issue']
				url = issue['html_url']
				index = url.rfind('/')
				issue_id = url[index + 1:]
				index = url.rfind('/', 0, index)
				repo = url[url.rfind('/', 0, index) + 1:index]
				index = url.rfind('/', 0, index)
				owner = url[url.rfind('/', 0, index) + 1:index]

				created_at = issue['created_at'] if 'created_at' in issue else ""
				if created_at:
					created_at = created_at[:10] + ' ' + created_at[11:-1]

				updated_at = issue['updated_at'] if 'updated_at' in issue else ""
				if updated_at:
					updated_at = updated_at[:10] + ' ' + updated_at[11:-1]

				is_open = 't'
				closed_at = ''
				closed_by = ''
				title_flag = 't' if any(event['title_labels'].values()) else 'f'

				s = ','.join([
					repo, owner, issue_id, is_open, updated_at, closed_at, closed_by, title_flag]
					+ ['t' if l else 'f' for l in event['body_labels'].values()
				])
				new_issues.append(s +'\n')

				s = ','.join([
						repo, owner, issue_id, created_at]
						+ ['t' if l else 'f' for l in event['title_labels'].values()
					])
				extended_issues.append(s +'\n')
	
	file = "/cluster/scratch/ayaris/gharchive/new_issues.csv"
	lock = FileLock("new_issues.lock")
	with lock: 
		with open(file, 'a' if os.path.exists(file) else 'w') as f:
			f.writelines(new_issues)
	
	file = "/cluster/scratch/ayaris/gharchive/extended_issues.csv"
	lock = FileLock("extended_issues.lock")
	with lock: 
		with open(file, 'a' if os.path.exists(file) else 'w') as f:
			f.writelines(extended_issues)

	file = "/cluster/scratch/ayaris/gharchive/new_comments.csv"
	lock = FileLock("new_comments.lock")
	with lock:
		with open(file, 'a' if os.path.exists(file) else 'w') as f:
			f.writelines(new_comments)

			
	file = "/cluster/scratch/ayaris/gharchive/extended_comments.csv"
	lock = FileLock("extended_comments.lock")
	with lock:
		with open(file, 'a' if os.path.exists(file) else 'w') as f:
			f.writelines(extended_comments)

if __name__ == '__main__':
	processes = [None] * 14
	for i in range(14):
		processes[i] = Process(target=process_events, args=("/cluster/scratch/ayaris/gharchive/events/%d.json" % i,))
		processes[i].start()
	for i in range(14):
		processes[i].join()
