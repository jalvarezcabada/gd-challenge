from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


def read_parquet_file(spark: SparkSession, path: str) -> DataFrame:
    "Reads a Parquet file into a DataFrame"
    return spark.read.parquet(path)


def read_csv_file(spark: SparkSession, path: str) -> DataFrame:
    "Reads a CSV file into a DataFrame"
    return spark.read.option("header", "true").csv(path)


def write_parquet_file(
    dataframe: DataFrame, path: str, mode: str, compression: str
) -> None:
    "Writes a DataFrame to a Parquet file"
    dataframe.write.mode(mode).option("compression", compression).parquet(path)
