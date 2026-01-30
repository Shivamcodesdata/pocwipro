# ---------------- 1️⃣ Import Libraries & Helper Scripts ----------------
import argparse
from app_ddl_creation.lib.scripts.helper import (
    load_config,
    find_sql_file,
    check_table_exists,
    parse_columns_from_sql,
    compare_schema_and_alter,
    read_sql_file,
)
from pyspark.sql import SparkSession

# Spark session (Databricks already has 'spark')
spark = SparkSession.builder.enableHiveSupport().getOrCreate()


# ---------------- 2️⃣ Argument Parser ----------------
def parse_args():
    parser = argparse.ArgumentParser(description="Databricks DDL Runner")
    parser.add_argument("--env", required=True, help="Environment: dev / uat / prd")
    parser.add_argument("--layer", required=True, help="Layer: bronze / silver / gold")
    parser.add_argument("--table_name", required=True, help="Target table name")
    return parser.parse_args()


# ---------------- 3️⃣ Main Function ----------------
def run_driver(env: str, layer: str, table_name: str):
    print(f"📝 Inputs received → env: {env}, layer: {layer}, table: {table_name}")

    # Step 1: Load base path and layer_catalog_map from config
    config = load_config(env)  # Now returns a dict with 'base_path' and 'layer_catalog_map'
    base_path = config.get("base_path")
    layer_catalog_map = config.get("layer_catalog_map")

    if not base_path:
        raise ValueError(f"'base_path' missing in {env} config")
    if not layer_catalog_map:
        raise ValueError(f"'layer_catalog_map' missing in {env} config")

    print(f"📂 Base path from config: {base_path}")
    print(f"🗂 Layer → Catalog map from config: {layer_catalog_map}")

    # Step 2: Locate SQL file
    sql_file = find_sql_file(base_path, layer, table_name)

    # Step 3: Switch to correct catalog based on layer
    layer_lower = layer.lower()
    if layer_lower not in layer_catalog_map:
        raise ValueError(f"Invalid layer '{layer}'. Must be one of {list(layer_catalog_map.keys())}")

    catalog_name = layer_catalog_map[layer_lower]
    spark.sql(f"USE CATALOG {catalog_name}")
    print(f"🔄 Using catalog: {catalog_name}")

    # Step 4: Check if table exists in the catalog
    exists = check_table_exists(table_name)

    if exists:
        # Table exists → parse SQL and validate schema
        print(f"⚠️ Table '{table_name}' exists. Validating schema...")
        sql_cols = parse_columns_from_sql(sql_file)
        compare_schema_and_alter(table_name, sql_cols)
    else:
        # Table does not exist → execute SQL to create it
        print(f"🚀 Table '{table_name}' does not exist. Creating in catalog '{catalog_name}'...")
        sql_content = read_sql_file(sql_file)

        # Ensure 'CREATE TABLE IF NOT EXISTS' to avoid accidental overwrite
        sql_content = sql_content.replace("create table", "create table if not exists")

        try:
            spark.sql(sql_content)
            print(f"✅ Table '{table_name}' created successfully in catalog '{catalog_name}'.")
        except Exception as e:
            raise RuntimeError(f"Failed to create table '{table_name}' in catalog '{catalog_name}': {e}")

    print("🔎 Pre-check and execution completed successfully.")



# ---------------- 4️⃣ Example CLI Usage ----------------
# args = parse_args()
# run_driver(env=args.env, layer=args.layer, table_name=args.table_name)

# ---------------- 5️⃣ Example Notebook Usage ----------------
# from main.driver import run_driver
# run_driver(env="dev", layer="bronze", table_name="sample")
