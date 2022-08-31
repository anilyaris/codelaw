#!/usr/bin/env bash

if [ ! -z "$1" ] ; then
  cd $1
fi

wget http://ghtorrent-downloads.ewi.tudelft.nl/mysql/mysql-2021-03-06.tar.gz
tar -xzvf mysql-2021-03-06.tar.gz

#: ft=bash
