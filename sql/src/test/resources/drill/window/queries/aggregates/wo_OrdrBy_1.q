SELECT MAX(col_int) OVER(PARTITION BY col_vchar_52) max_int, col_vchar_52, col_int FROM "smlTbl.parquet" WHERE col_vchar_52 = 'AXXXXXXXXXXXXXXXXXXXXXXXXXCXXXXXXXXXXXXXXXXXXXXXXXXB'