#!/usr/bin/env bash

mysql -e "CREATE DATABASE IF NOT EXISTS jellyfish_merge"

extension="h38au.bowtie2-e2e.unmapped.13mers.jellyfish2.csv"

for f in /Users/jakewendt/20200211-20191008_Stanford71/??.${extension} ; do
	base=$( basename $f .${extension} )
	echo $base $f

	echo "Creating table"
	time mysql jellyfish_merge -e "CREATE TABLE IF NOT EXISTS c${base}( mer VARCHAR(20) NOT NULL, c${base} INT, UNIQUE (mer) );"

	echo "Loading table (took about 1m30s for 13mer / canonical)"
	time mysql jellyfish_merge -e "LOAD DATA INFILE '${f}' INTO TABLE c${base} COLUMNS TERMINATED BY ' ';"

done

