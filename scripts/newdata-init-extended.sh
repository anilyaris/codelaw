#!/usr/bin/env bash

host="localhost"

while getopts "h:" o
do
  case $o in
  h)  host=$OPTARG ;;
  esac
done

psql -d ghtorrent -U ghtorrent -h $host -f ./sql/extended_tables.sql

#: ft=bash
