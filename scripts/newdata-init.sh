#!/usr/bin/env bash

host="localhost"

while getopts "h:" o
do
  case $o in
  h)  host=$OPTARG ;;
  esac
done

psql -d ghtorrent -U ghtorrent -h $host -f ./sql/new_tables.sql

#: ft=bash
