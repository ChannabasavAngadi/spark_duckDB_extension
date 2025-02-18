import os
import duckdb
from pyspark.sql import DataFrame
import shutil

class DuckDBWriter:
    """
    A custom PySpark writer that first writes data to Parquet,
    then loads it into DuckDB.
    """
    def __init__(self, duckdb_path: str, temp_parquet_path: str, table_name: str):
        self.duckdb_path = duckdb_path
        self.temp_parquet_path = temp_parquet_path
        self.table_name = table_name  # Add the table_name as an argument

    def save(self, df: DataFrame ,mode = "overwrite"):
        """
        Writes the DataFrame to Parquet first, then inserts it into DuckDB.
        """
        # Ensure temp directory exists
        os.makedirs(self.temp_parquet_path, exist_ok=True)
        try:
            # Write the DataFrame to Parquet first
            df.write.format("parquet").mode("overwrite").save(self.temp_parquet_path)
        except Exception as e:
            print(f"Error writing Parquet: {e}")
            raise e

        # Insert data into DuckDB
        conn = duckdb.connect(self.duckdb_path)
        
        # Check if table exists
        table_exists = conn.execute(
            f"SELECT COUNT(*) FROM duckdb_tables() WHERE table_name='{self.table_name}'"
        ).fetchone()[0] > 0

        if mode == "overwrite":
            print(f"Overwriting table: {self.table_name}")
            conn.execute(f"DROP TABLE IF EXISTS {self.table_name}")
            conn.execute(f"CREATE TABLE {self.table_name} AS SELECT * FROM parquet_scan('{self.temp_parquet_path}/*.parquet')")
        
        elif mode == "append":
            if not table_exists:
                print(f"Table {self.table_name} does not exist, creating new table and Inserting the Data.")
                conn.execute(f"CREATE TABLE {self.table_name} AS SELECT * FROM parquet_scan('{self.temp_parquet_path}/*.parquet')")
            else:
                print(f"Appending data to table: {self.table_name}")
                conn.execute(f"INSERT INTO {self.table_name} SELECT * FROM parquet_scan('{self.temp_parquet_path}/*.parquet')")

        conn.close()
        # After writing to DuckDB, clean up temp files
        shutil.rmtree(self.temp_parquet_path, ignore_errors=True)


def register_duckdb_extension(spark):
    """
    Registers a custom method 'duckdb_extension' on the DataFrameWriter.
    This allows you to call:
    
        df.write.duckdb_extension("my_db.duckdb", "target_table")
    
    It uses the underlying DataFrame (accessed via the private _df attribute).
    """
    from pyspark.sql.readwriter import DataFrameWriter

    def duckdb_writer(self, duckdb_path, table_name , mode="overwrite"):
        # 'self' here is the DataFrameWriter.
        # We access the underlying DataFrame via self._df (a private attribute).
        writer = DuckDBWriter(duckdb_path, "temp_parquet_storage/", table_name)
        writer.save(self._df, mode)

    # Monkey-patch DataFrameWriter to add our custom method.
    DataFrameWriter.duckdb_extension = duckdb_writer
