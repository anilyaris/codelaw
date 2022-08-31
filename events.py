import os
import json
import keywords
from multiprocessing import Process, Manager

def process_month(month, events, PM):
	dir = "/cluster/scratch/ayaris/gharchive/"
	url = "https://data.gharchive.org/%s.json.gz"
	date = "2020-" if month == 1 else "2019-"
	date += ("%d-" % month).zfill(3)
	
	for day in range(1, 32):
		ymd = date + ("%d-" % day).zfill(3)

		for hour in range(PM * 12, PM * 12 + 12):
			ymdh = ymd + str(hour)

			os.system("wget -q %s -P %s" % (url % ymdh, dir))
			filepath = dir + ymdh + ".json"

			if os.path.exists(filepath + ".gz"):
				os.system("gunzip " + filepath + ".gz")

				if os.path.exists(filepath):                    
					with open(filepath, "r") as f:
						for line in f:
							e = json.loads(line)

							try:
								if e['type'] == 'IssuesEvent' and e['payload']['action'] == 'opened':
									issue = e['payload']['issue']
									title = keywords.generate_labels()
									body = keywords.generate_labels()
									keywords.match_keywords(issue['title'], title)
									keywords.match_keywords(issue['body'], body)

									e['title_labels'] = title
									e['body_labels'] = body
									events.append(e)

								elif e['type'] == 'IssueCommentEvent' and e['payload']['action'] == 'created':
									comment = e['payload']['comment']
									body = keywords.generate_labels()
									keywords.match_keywords(comment['body'], body)

									e['body_labels'] = body
									events.append(e)
								
							except Exception as err:
								print(err)
								print(e)

					os.remove(filepath)

def run():
	manager = Manager()
	lists = [None] * 14
	processes = [None] * 14
	for i in range(14):
		month = i//2 + 7
		if month == 13:
			month = 1

		lists[i] = manager.list()
		processes[i] = Process(target=process_month, args=(month, lists[i], i % 2))
		processes[i].start()

	for i in range(14):
		processes[i].join()
		with open("/cluster/scratch/ayaris/gharchive/events/%d.json" % i, "w") as f:
			json.dump(list(lists[i]), f)

if __name__ == "__main__":
	run()