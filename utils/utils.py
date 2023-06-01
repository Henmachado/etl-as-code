import json

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

from typing import List


def get_local_spark_session() -> SparkSession:
    """creates a local spark session"""
    return SparkSession.builder.getOrCreate()


def read_csv(spark_session, path: str) -> DataFrame:
    """loads csv data from a file path inferring the schema"""
    return (
        spark_session.read.format("com.databricks.spark.csv")
        .option("header", "true")
        .option("treatEmptyValuesAsNulls", "true")
        .option("inferSchema", "true")
        .load(f"Tables/{path}")
    )


def create_local_warehouse(spark_session: SparkSession, tables: List[str], table_names: List[str]) -> None:
    """creates a local spark-warehouse"""
    for table in tables:
        _df = read_csv(spark_session, table)  # ToDo: Refactor this
        _df.createOrReplaceTempView(table)


def read_json_from_file(file_path: str) -> List:
    """reads raw json from a file path"""
    with open(file_path, 'r') as f:
        json_data = f.read()
        data = json.loads(json_data)
    return data


def write_json_to_file(data: List[str], file_path) -> None:
    """write the json back as one object per line"""
    with open(file_path, 'w') as file:
        for item in data:
            json.dump(item, file)
            file.write('\n')


def read_json_output(spark_session: SparkSession, output_file: str, schema: str) -> DataFrame:
    """reads the processed json on spark"""
    return spark_session.read.schema(schema).json(output_file)
