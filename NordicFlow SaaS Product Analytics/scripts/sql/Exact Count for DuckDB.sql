SELECT 
    table_name,
    -- Exact row count for DuckDB
    estimated_size AS estimated_rows,
    -- Column count from the columns metadata
    (SELECT COUNT(*) 
     FROM duckdb_columns() c 
     WHERE c.table_name = t.table_name AND c.schema_name = t.schema_name) AS column_count
FROM duckdb_tables() t
WHERE schema_name = 'main'
  AND table_name LIKE 'raw__%'
ORDER BY table_name;