
-- .separator " "

CREATE DATABASE IF NOT EXISTS mergetest;
USE mergetest;

-- These csv files do not have a header line, so need to create table first

CREATE TABLE IF NOT EXISTS t1( mer VARCHAR(20) NOT NULL, hairpin INT, UNIQUE (mer) );
CREATE TABLE IF NOT EXISTS t2( mer VARCHAR(20) NOT NULL,  mature INT, UNIQUE (mer) );

-- Import each CSV into a separate table
LOAD DATA INFILE '/Users/jakewendt/github/ucsffrancislab/table_merging/hairpin.csv'
	INTO TABLE t1 COLUMNS TERMINATED BY ' ';
LOAD DATA INFILE '/Users/jakewendt/github/ucsffrancislab/table_merging/mature.csv'
	INTO TABLE t2 COLUMNS TERMINATED BY ' ';
 
