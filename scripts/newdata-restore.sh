#!/usr/bin/env bash

host="localhost"
data=""

while getopts "h:d:" o
do
  case $o in
  h)  host=$OPTARG ;;
  d)  data=$OPTARG ;;
  esac
done

psql -d ghtorrent -U ghtorrent -h $host -c "COPY mongo_issue_comments FROM $data/issue_comments.csv WITH (FORMAT 'csv')"
psql -d ghtorrent -U ghtorrent -h $host -c "COPY mongo_commits FROM $data/commits.csv WITH (FORMAT 'csv')"
psql -d ghtorrent -U ghtorrent -h $host -c "COPY mongo_issues FROM $data/issues.csv WITH (FORMAT 'csv')"
psql -d ghtorrent -U ghtorrent -h $host -c "COPY mongo_pull_requests FROM $data/pull_requests.csv WITH (FORMAT 'csv')"
psql -d ghtorrent -U ghtorrent -h $host -c "COPY mongo_projects FROM $data/repos.csv WITH (FORMAT 'csv')"

psql -d ghtorrent -U ghtorrent -h $host -c "COPY mongo_issue_comments FROM $data/new_comments.csv WITH (FORMAT 'csv')"
psql -d ghtorrent -U ghtorrent -h $host -c "COPY mongo_issues FROM $data/new_issues.csv WITH (FORMAT 'csv')"

#: ft=bash
