#!/usr/bin/env bash

# defaults
user="ghtorrent"
passwd="ghtorrent"
host="localhost"
db="ghtorrent"

usage()
{
  echo "Usage: $0 [-u dbuser ] [-p dbpasswd ] [-h dbhost] [-d database ] dump_dir"
  echo
  echo "Restore a database from CSV and SQL files in dump_dir"
  echo "    -u database user (default: $user)"
  echo "    -p database passwd (default: $passwd)"
  echo "    -h database host (default: $host)"
  echo "    -d database to restore to. Must exist. (default: $db)"
}

if [ -z $1 ]
then
  usage
  exit 1
fi

while getopts "u:p:h:d:" o
do
  case $o in
  u)  user=$OPTARG ;;
  p)  passwd=$OPTARG ;;
  h)  host=$OPTARG ;;
  d)  db=$OPTARG ;;
  \?)     echo "Invalid option: -$OPTARG" >&2
    usage
    exit 1
    ;;
  esac
done

# Setup PostgreSQL command line
if ! [ -z $passwd ]; then
  export PGPASSWORD=$passwd
fi

# Run psql with the specified arguments exitting on first error
# If no arguments are specified, then input is set to come from stdin
run_psql()
{
  stdin=''
  if [ -z "$1" ] ; then
    stdin='-f -'
  fi
  psql -U $user -t -q -h $host -d $db -c '\set ON_ERROR_STOP 1' $stdin "$@"
}

shift $(expr $OPTIND - 1)
dumpDir=$1
if [ -z "$1" ] ; then
  dumpDir='dump'
fi

if [ ! -e $dumpDir ]; then
  echo "Cannot find directory to restore from"
  exit 1
fi

# Convert to full path
dumpDir="`pwd`/$dumpDir"
sqlDir="`pwd`/sql"

if [ ! -e $sqlDir/pg_schema.sql ]; then
  echo "Cannot find $sqlDir/pg_schema.sql to create DB schema"
  exit 1
fi

# 1. Create db schema
echo "`date` Creating the DB schema"
run_psql <$sqlDir/pg_schema.sql || exit 1

# 2. Restore CSV files with disabled FK checks
cat $sqlDir/ORDER | while read table; do
  f="${dumpDir}/${table}.csv"
  echo "`date` Restoring table $table"
  if [ $table = commits ] ; then
    # For commits.csv convert the mySQL null dates to NULL records
    sed 's/"0000-00-00 00:00:00"/\\N/g' $f |
      run_psql -c "COPY commits FROM STDIN WITH (FORMAT 'csv', QUOTE E'\\\"', ESCAPE '\\', NULL '\\N');" || exit 1
  else
    # For the other tables have the server import directly from the local
    # file, without having the data travel through the client
    echo "COPY $table FROM '$f' WITH (FORMAT 'csv', QUOTE E'\"', ESCAPE '\', NULL '\N');" | run_psql || exit 1
  fi
done

# 3. Reset sequences
if [ ! -e $sqlDir/pg_reset_sequences.sql ]; then
  echo "Cannot find $sqlDir/pg_reset_sequences.sql to create DB indexes and foreign keys"
  exit 1
fi

echo "`date` Resetting sequences"
cat $sqlDir/pg_reset_sequences.sql |
while read idx; do
  echo "`date` $idx"
  echo $idx | run_psql || exit 1
done

# 4. Create indexes
if [ ! -e $sqlDir/pg_indexes_and_foreign_keys.sql ]; then
  echo "Cannot find $sqlDir/pg_indexes_and_foreign_keys.sql to create DB indexes and foreign keys"
  exit 1
fi

echo "`date` Creating indexes"
cat $sqlDir/pg_indexes_and_foreign_keys.sql |
while read idx; do
  echo "`date` $idx"
  echo $idx | run_psql || exit 1
done

#: ft=bash
