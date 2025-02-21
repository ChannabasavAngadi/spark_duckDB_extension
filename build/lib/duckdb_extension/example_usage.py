from pyspark.sql import SparkSession
from duckdb_extension import register_duckdb_extension

spark = SparkSession.builder.appName("DuckDB Example").getOrCreate()

# Register the DuckDB extension
register_duckdb_extension(spark)

df=spark.read.csv("DME_LIST.csv",header=True)

# Use the custom extension to write the DataFrame to DuckDB and specify the table name
df.write.duckdb_extension(database="./my_db.duckdb", table_name="newtable", mode="append")
