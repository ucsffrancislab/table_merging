-- Set CSV output mode, with headers
.mode csv
.headers on
.output result.csv
 
-- Now JOIN them all together
SELECT * FROM t1
	JOIN t2 USING (mer)
	ORDER BY mer;

-- Really need a FULL OUTER JOIN, which sqlite does not support so, waste of time
