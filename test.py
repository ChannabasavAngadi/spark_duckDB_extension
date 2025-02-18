from pyspark.sql import SparkSession
from duckdb_extension.duckdb_extension import register_duckdb_extension

spark = SparkSession.builder.appName("DuckDB Example").getOrCreate()

# Register the DuckDB extension
register_duckdb_extension(spark)

# Create a DataFrame to write
data = [(1, "Alice"), (2, "Bob"), (3, "Charlie")]
df = spark.createDataFrame(data, ["id", "name"])

# Use the custom extension to write the DataFrame to DuckDB and specify the table name
df.write.duckdb_extension("./my_db.duckdb", "newtable" , mode="append")