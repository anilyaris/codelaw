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

psql -d postgres -h $host -c "COPY mongo_issue_comments FROM $data/issue_comments.csv WITH (FORMAT 'csv')"
psql -d postgres -h $host -c "COPY mongo_commits FROM $data/commits.csv WITH (FORMAT 'csv')"
psql -d postgres -h $host -c "COPY mongo_issues FROM $data/issues.csv WITH (FORMAT 'csv')"
psql -d postgres -h $host -c "COPY mongo_projecs FROM $data/repos.csv WITH (FORMAT 'csv')"

#: ft=bash
