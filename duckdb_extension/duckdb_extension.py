import os
import duckdb
from pyspark.sql import DataFrame

class DuckDBWriter:
    """
    A custom PySpark writer that first writes data to Parquet,
    then loads it into DuckDB.
    """
    def __init__(self, duckdb_path: str, temp_parquet_path: str):
        self.duckdb_path = duckdb_path
        self.temp_parquet_path = temp_parquet_path

    def save(self, df: DataFrame):
        """
        Writes the DataFrame to Parquet first, then inserts it into DuckDB.
        """
        # Ensure temp directory exists
        os.makedirs(self.temp_parquet_path, exist_ok=True)

        print(f"Attempting to write Parquet to: {self.temp_parquet_path}")
        try:
            # Here, we use the DataFrame's write method correctly:
            df.write.format("parquet").mode("overwrite").save(self.temp_parquet_path)
            print(f"Successfully wrote Parquet to: {self.temp_parquet_path}")
        except Exception as e:
            print(f"Error writing Parquet: {e}")
            raise e

        # Insert data into DuckDB
        conn = duckdb.connect(self.duckdb_path)
        # For this example, we create or replace a table named 'my_table'
        conn.execute(
            f"CREATE TABLE IF NOT EXISTS my_table AS SELECT * FROM parquet_scan('{self.temp_parquet_path}/*.parquet')"
        )
        conn.close()

def register_duckdb_extension(spark):
    """
    Registers a custom method 'duckdb_extension' on the DataFrameWriter.
    This allows you to call:
    
        df.write.duckdb_extension("my_db.duckdb")
    
    It uses the underlying DataFrame (accessed via the private _df attribute).
    """
    from pyspark.sql.readwriter import DataFrameWriter

    def duckdb_writer(self, path):
        # 'self' here is the DataFrameWriter.
        # We access the underlying DataFrame via self._df (a private attribute).
        writer = DuckDBWriter(path, "temp_parquet_storage/")
        writer.save(self._df)

    # Monkey-patch DataFrameWriter to add our custom method.
    DataFrameWriter.duckdb_extension = duckdb_writer
