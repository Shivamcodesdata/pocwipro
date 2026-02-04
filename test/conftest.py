import pytest
import json
import os
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("unit-tests")
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture
def temp_config(tmp_path):
    config = {
        "layer_catalog_map": {
            "bronze": "bronze",
            "silver": "silver",
            "gold": "gold"
        }
    }
    env_dir = tmp_path / "dev"
    env_dir.mkdir()
    (env_dir / "config.json").write_text(json.dumps(config))
    return str(tmp_path)
