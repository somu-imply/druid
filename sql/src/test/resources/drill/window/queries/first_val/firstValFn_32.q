SELECT * FROM (SELECT c1, c2, lead(c2) OVER ( PARTITION BY c2 ORDER BY c1) lead_c2, lag(c2) OVER ( PARTITION BY c2 ORDER BY c1) lag_c2, ntile(3) OVER ( PARTITION BY c2 ORDER BY c1) tile, first_value(c2) OVER ( PARTITION BY c2 ORDER BY c1) firstVal_c2, last_value(c2) OVER ( PARTITION BY c2 ORDER BY c1) lastVal_c2 FROM "tblWnulls.parquet") sub_query where firstVal_c2 = "e" ORDER BY tile, c1
