#!/usr/bin/env bash


#max=77
max=10


query="SELECT x.mer "

#	I deleted the files to free up limited disk space so 
#, COALESCE (mature, 0) AS mature

for i in $( seq -w 1 ${max} ) ; do
	query=${query}", COALESCE (c${i}, 0) AS c${i}"
done
query=${query}"	FROM ( SELECT mer FROM c01"
for i in $( seq -w 2 ${max} ) ; do
	query=${query}" UNION SELECT mer FROM c${i}"
done
query=${query}") AS x"
for i in $( seq -w 1 ${max} ) ; do
	query=${query}" LEFT JOIN c${i} ON x.mer = c${i}.mer"
done
query=${query}" ORDER BY x.mer;"


echo ${query}
time mysql jellyfish_merge -e "${query}"


#	ERROR 1116 (HY000) at line 1: Too many tables; MariaDB can only use 61 tables in a join


