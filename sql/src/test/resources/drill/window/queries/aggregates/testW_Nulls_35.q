SELECT c1, cume_dst FROM ( SELECT c1, cume_dISt() OVER ( PARTITION BY c2 ORDER BY c1 ASC nulls last ) cume_dst FROM "tblWnulls.parquet") sub_query WHERE cume_dst > 0.5