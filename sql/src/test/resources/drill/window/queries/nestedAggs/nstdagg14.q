SELECT c2, MAX(AVG(c1)) OVER() FROM "tblWnulls.parquet" GROUP BY c2
