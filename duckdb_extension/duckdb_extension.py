import os
import shutil
import duckdb
from pyspark.sql import DataFrame

class ParquetWriter:
    """
    Handles writing a PySpark DataFrame to Parquet format.
    """
    def __init__(self, temp_parquet_path: str):
        self.temp_parquet_path = temp_parquet_path
        os.makedirs(self.temp_parquet_path, exist_ok=True)

    def write(self, df: DataFrame):
        try:
            df.write.format("parquet").mode("overwrite").save(self.temp_parquet_path)
        except Exception as e:
            print(f"Error writing Parquet: {e}")
            raise e

    def cleanup(self):
        shutil.rmtree(self.temp_parquet_path, ignore_errors=True)


class DuckDBManager:
    """
    Handles interactions with DuckDB, including table creation and data insertion.
    """
    def __init__(self, duckdb_path: str, table_name: str):
        self.duckdb_path = duckdb_path
        self.table_name = table_name
        self.conn = duckdb.connect(self.duckdb_path)

    def table_exists(self) -> bool:
        return self.conn.execute(
            f"""SELECT COUNT(*) FROM duckdb_tables() WHERE table_name='{self.table_name}'"""
        ).fetchone()[0] > 0

    def create_table_from_parquet(self, parquet_path: str):
        self.conn.execute(
            f"CREATE TABLE {self.table_name} AS SELECT * FROM parquet_scan('{parquet_path}/*.parquet')"
        )

    def get_existing_columns(self):
        return [row[0] for row in self.conn.execute(f"DESCRIBE {self.table_name}").fetchall()]

    def get_parquet_columns(self, parquet_path: str):
        return [row[0] for row in self.conn.execute(f"DESCRIBE SELECT * FROM parquet_scan('{parquet_path}/*.parquet')").fetchall()]

    def add_missing_columns(self, new_columns, existing_columns):
        for col in new_columns:
            if col not in existing_columns:
                print(f"Adding column: {col}")
                self.conn.execute(f"ALTER TABLE {self.table_name} ADD COLUMN {col} STRING")  # Default type STRING

    def insert_data(self, parquet_path: str, new_columns, existing_columns):
        all_columns = list(set(existing_columns + new_columns))
        column_selection = ", ".join([
            f"{col}" if col in new_columns else f"NULL AS {col}" for col in all_columns
        ])
        self.conn.execute(
            f"INSERT INTO {self.table_name} ({', '.join(all_columns)}) SELECT {column_selection} FROM parquet_scan('{parquet_path}/*.parquet')"
        )

    def close(self):
        self.conn.close()


class DuckDBWriter:
    """
    Orchestrates writing a DataFrame to DuckDB via Parquet.
    """
    def __init__(self, duckdb_path: str, temp_parquet_path: str, table_name: str):
        self.parquet_writer = ParquetWriter(temp_parquet_path)
        self.duckdb_manager = DuckDBManager(duckdb_path, table_name)
        self.temp_parquet_path = temp_parquet_path

    def save(self, df: DataFrame, mode="overwrite"):
        self.parquet_writer.write(df)

        if mode == "overwrite":
            print(f"Overwriting table: {self.duckdb_manager.table_name}")
            self.duckdb_manager.conn.execute(f"DROP TABLE IF EXISTS {self.duckdb_manager.table_name}")
            self.duckdb_manager.create_table_from_parquet(self.temp_parquet_path)

        elif mode == "append":
            if not self.duckdb_manager.table_exists():
                print(f"Table {self.duckdb_manager.table_name} does not exist, creating new table.")
                self.duckdb_manager.create_table_from_parquet(self.temp_parquet_path)
            else:
                print(f"Appending data to table: {self.duckdb_manager.table_name}")
                existing_columns = self.duckdb_manager.get_existing_columns()
                new_columns = self.duckdb_manager.get_parquet_columns(self.temp_parquet_path)
                self.duckdb_manager.add_missing_columns(new_columns, existing_columns)
                self.duckdb_manager.insert_data(self.temp_parquet_path, new_columns, existing_columns)

        self.duckdb_manager.close()
        self.parquet_writer.cleanup()


def register_duckdb_extension(spark):
    from pyspark.sql.readwriter import DataFrameWriter

    def duckdb_writer(self, duckdb_path, table_name, mode="overwrite"):
        writer = DuckDBWriter(duckdb_path, "temp_parquet_storage/", table_name)
        writer.save(self._df, mode)

    DataFrameWriter.duckdb_extension = duckdb_writer