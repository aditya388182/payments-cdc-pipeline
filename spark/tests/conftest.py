import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    builder = (
        SparkSession.builder.appName("tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "1")
    )
    # Configure delta if available in environment
    try:
        from delta import configure_spark_with_delta_pip
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
    except ImportError:
        spark = builder.getOrCreate()
    yield spark
    spark.stop()

@pytest.fixture(scope="session")
def merchant_ids():
    return {f"MERCH_{str(i).zfill(3)}" for i in range(1, 21)}