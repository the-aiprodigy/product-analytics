import duckdb

# Connect to the example DuckDB file
con = duckdb.connect("setup_example.duckdb")

# Run a simple query to confirm the connection works
result = con.sql("SELECT COUNT(*) AS total_customers FROM raw_customers").fetchdf()

print("Connection successful!")
print(result)

con.close()
