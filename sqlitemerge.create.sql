
.separator " "

-- These csv files do not have a header line, so need to create table first
create table t1 ( mer, hairpin );
create table t2 ( mer, mature );

-- Import each CSV into a separate table
.import hairpin.csv t1
.import mature.csv  t2
 
