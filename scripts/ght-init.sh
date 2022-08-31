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

cp sql/*.conf $data
psql -d postgres -h $host -f ./sql/userdb.sql

#: ft=bash
