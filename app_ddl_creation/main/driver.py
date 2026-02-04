# ---------------- 1️⃣ Import Libraries & Helper Scripts ----------------
import argparse
import re
import sys
from app_ddl_creation.lib.scripts.helper import (
    load_config,
    find_sql_file,
    check_table_exists,
    parse_columns_from_sql,
    get_table_schema,
    detect_schema_drift,
    generate_alter_statements,
    apply_schema_changes,
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
    print(sys.argv)
    return parser.parse_args()
    


# ---------------- 3️⃣ Main Function ----------------
def run_driver(env: str, layer: str, table_name: str):
    print(f"📝 Inputs received → env: {env}, layer: {layer}, table: {table_name}")

    # -----------------------------
    # Step 1: Load config
    # -----------------------------
    config = load_config(env)

    base_path = config.get("base_path")
    catalog_name = config.get("catalog")
    layer_schema_map = config.get("layer_schema_map")
    layer = layer.lower()
    table_name = table_name.lower()

    if not base_path:
        raise ValueError(f"'base_path' missing in {env} config")
    if not catalog_name:
        raise ValueError(f"'catalog' missing in {env} config")
    if not layer_schema_map:
        raise ValueError(f"'layer_schema_map' missing in {env} config")

    print(f"📂 Base path from config: {base_path}")
    print(f"🗂 Layer → Schema map from config: {layer_schema_map}")


    # -----------------------------
    # Step 2: Locate SQL file
    # -----------------------------
    sql_file = find_sql_file(base_path, layer, table_name)

    # -----------------------------
    # Step 3: Resolve catalog
    # -----------------------------
    layer_lower = layer.lower()

    if layer_lower not in layer_schema_map:
        raise ValueError(
            f"Invalid layer '{layer}'. Must be one of {list(layer_schema_map.keys())}"
        )

    schema_name = layer_schema_map[layer_lower]

    # -----------------------------
    # Step 4: Build fully qualified table name (OPTION 1)
    # -----------------------------
    full_table_name = f"{catalog_name}.{schema_name}.{table_name}"
    print(f"📛 Fully qualified table name: {full_table_name}")

    # -----------------------------
    # Step 5: Check if table exists
    # -----------------------------
    exists = check_table_exists(
    spark,
    catalog_name,
    schema_name,
    table_name
)
    if exists:
        print(f"⚠️ Table '{full_table_name}' exists. Validating schema...")

        # -----------------------------
        # Step 6: Parse SQL schema
        # -----------------------------
        sql_columns = parse_columns_from_sql(sql_file)

        # -----------------------------
        # Step 7: Read existing table schema
        # -----------------------------
        table_schema = get_table_schema(spark, full_table_name)

        # -----------------------------
        # Step 8: Detect schema drift
        # -----------------------------
        drift = detect_schema_drift(sql_columns, table_schema)

        # -----------------------------
        # Step 9: Generate ALTER statements
        # -----------------------------
        alter_statements = generate_alter_statements(
            full_table_name,
            drift
        )

        # -----------------------------
        # Step 10: Apply schema changes (env-controlled)
        # -----------------------------
        apply_schema_changes(
            spark=spark,
            table_name=full_table_name,
            alter_statements=alter_statements,
            env=env,
            drift=drift
        )

    else:
        print(f"🚀 Table '{full_table_name}' does not exist. Creating it...")

        sql_content = read_sql_file(sql_file)

        # Safety: enforce IF NOT EXISTS
        sql_content = re.sub(
            r"create\s+table\s+(if\s+not\s+exists\s+)?\w+",
            f"CREATE TABLE IF NOT EXISTS {full_table_name}",
            sql_content,
            flags=re.IGNORECASE
        )

        try:
            spark.sql(sql_content)
            print(f"✅ Table '{full_table_name}' created successfully.")
        except Exception as e:
            raise RuntimeError(
                f"Failed to create table '{full_table_name}': {e}"
            )

    print("🔎 Pre-check and execution completed successfully.")

    

def main():
    args = parse_args()
    run_driver(
        env=args.env,
        layer=args.layer,
        table_name=args.table_name
    )


if __name__ == "__main__":
    main()
