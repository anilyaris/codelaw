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

psql -d ghtorrent -U ghtorrent -h $host -c "COPY mongo_issue_comments_extended FROM $data/issue_comments_extended.csv WITH (FORMAT 'csv')"
psql -d ghtorrent -U ghtorrent -h $host -c "COPY mongo_commits_extended FROM $data/commits_extended.csv WITH (FORMAT 'csv')"
psql -d ghtorrent -U ghtorrent -h $host -c "COPY mongo_issues_extended FROM $data/issues_extended.csv WITH (FORMAT 'csv')"
psql -d ghtorrent -U ghtorrent -h $host -c "COPY mongo_projects_extended FROM $data/repos_extended.csv WITH (FORMAT 'csv')"

psql -d ghtorrent -U ghtorrent -h $host -c "COPY mongo_issue_comments_extended FROM $data/extended_comments.csv WITH (FORMAT 'csv')"
psql -d ghtorrent -U ghtorrent -h $host -c "COPY mongo_issues_extended FROM $data/extended_issues.csv WITH (FORMAT 'csv')"

#: ft=bash
