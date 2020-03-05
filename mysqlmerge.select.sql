-- Set CSV output mode, with headers
--.mode csv
--.headers on
--.output merged_jellyfish_sqlite.csv
 
-- Now JOIN them all together
--	SELECT * FROM t1
--		JOIN t2 USING (mer)
--		ORDER BY mer;

--	SELECT * FROM t1
--	LEFT JOIN t2 ON t1.mer = t2.mer
--	UNION
--	SELECT * FROM t1
--	RIGHT JOIN t2 ON t1.mer = t2.mer

-- SELECT mer FROM t1

SELECT x.mer, COALESCE (mature, 0) AS mature, COALESCE (hairpin, 0) AS hairpin
	FROM (
		SELECT mer FROM t2 UNION SELECT mer FROM t1
	) AS x
	LEFT JOIN t1 ON x.mer = t1.mer
	LEFT JOIN t2 ON x.mer = t2.mer
ORDER BY x.mer;


-- Really need a FULL OUTER JOIN, which sqlite does not support so, waste of time
-- Neither does mysql, without some help
