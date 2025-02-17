from pyspark.sql import SparkSession
from duckdb_extension import register_duckdb_extension

# Initialize Spark
spark = SparkSession.builder.appName("DuckDB Extension Example").getOrCreate()

# Register the extension
register_duckdb_extension(spark)

# Create a DataFrame
data = [(1, "Alice"), (2, "Bob"), (3, "Charlie")]
df = spark.createDataFrame(data, ["id", "name"])
df.show()
# Use the custom DuckDB format
df.write.duckdb_extension("my_db.duckdb")

print("Data written to DuckDB successfully!")
