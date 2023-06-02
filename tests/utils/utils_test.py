import pytest
from utils.utils import get_local_spark_session, read_json_from_file
from unittest.mock import patch
from pyspark.sql import SparkSession


@pytest.fixture
def spark_session() -> SparkSession:
    return SparkSession.builder.getOrCreate()


def test_get_local_spark_session(spark_session: SparkSession) -> None:
    spark = get_local_spark_session()
    assert isinstance(spark, SparkSession)


def test_read_json_from_file() -> None:
    file_path = "test.json"
    expected_data = [{"key": "value"}]
    with patch("builtins.open") as mock_open:
        mock_file = mock_open.return_value.__enter__.return_value
        mock_file.read.return_value = '[{"key": "value"}]'
        data = read_json_from_file(file_path)
    mock_open.assert_called_once_with(file_path, 'r')
    assert data == expected_data
