SELECT c1 , LAG(c1) OVER( PARTITION BY c2 ORDER BY c1 ) LAG_c1 FROM ( SELECT col1 c1, col2 c2 FROM "fewRowsAllData.parquet") sub_query