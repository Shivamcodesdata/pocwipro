# ---------------- 1️⃣ Import Libraries & Helper Scripts ----------------
import argparse
import re
import sys
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

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

# -----------------------------
# Unity Catalog Detection
# -----------------------------
def is_unity_catalog_enabled(spark):
    catalogs = [c.catalog for c in spark.sql("SHOW CATALOGS").collect()]
    return len(catalogs) > 1 or catalogs[0] != "spark_catalog"

# ---------------- 2️⃣ Argument Parser ----------------
def parse_args():
    parser = argparse.ArgumentParser(description="Databricks DDL Runner")
    parser.add_argument("--env", required=True, help="Environment: dev / uat / prd")
    parser.add_argument("--layer", required=True, help="Layer: bronze / silver / gold")
    parser.add_argument(
    "--table_name",
    required=True,
    help="Comma separated table names OR 'all'"
)
    return parser.parse_args()


# ---------------- 3️⃣ Single Table Executor ----------------
def process_table(
    env,
    layer,
    table_name,
    base_path,
    catalog_name,
    layer_schema_map
):
    """
    Handles execution for ONE table.
    Used for bulk + parallel execution.
    """

    try:
        print(f"\n🚀 Processing table → {table_name}")

        # -----------------------------
        # Locate SQL
        # -----------------------------
        sql_file = find_sql_file(base_path, layer, table_name)

        # -----------------------------
        # Resolve schema
        # -----------------------------
        schema_name = layer_schema_map[layer]

        if is_unity_catalog_enabled(spark):
            full_table_name = f"{catalog_name}.{schema_name}.{table_name}"
            print("🧭 Unity Catalog detected → Using 3-part naming")
        else:
            full_table_name = f"{schema_name}.{table_name}"
            print("🧭 Hive Metastore detected → Using 2-part naming")

        print(f"📛 Fully qualified table name: {full_table_name}")

        # -----------------------------
        # Check existence
        # -----------------------------
        exists = check_table_exists(
            spark,
            catalog_name,
            schema_name,
            table_name
        )

        # =============================
        # TABLE EXISTS → DRIFT
        # =============================
        if exists:

            print(f"⚠️ Table '{full_table_name}' exists. Validating schema...")

            sql_columns = parse_columns_from_sql(sql_file)
            table_schema = get_table_schema(spark, full_table_name)

            drift = detect_schema_drift(sql_columns, table_schema)

            alter_statements = generate_alter_statements(
                full_table_name,
                drift
            )

            apply_schema_changes(
                spark=spark,
                table_name=full_table_name,
                alter_statements=alter_statements,
                env=env,
                drift=drift
            )

            # -----------------------------
            # ✅ Outcome classification
            # -----------------------------
            if not alter_statements:
                return f"SUCCESS → No Change → {table_name}"
            else:
                return f"SUCCESS → Drift Fixed → {table_name}"

        # =============================
        # TABLE NOT EXISTS → CREATE
        # =============================
        else:

            print(f"🚀 Creating table '{full_table_name}'")

            sql_content = read_sql_file(sql_file)

            sql_content = re.sub(
                r"create\s+table\s+(if\s+not\s+exists\s+)?\w+",
                f"CREATE TABLE IF NOT EXISTS {full_table_name}",
                sql_content,
                flags=re.IGNORECASE
            )

            spark.sql(sql_content)

            return f"SUCCESS → Created → {table_name}"

    except Exception as e:
        return f"FAILED → {table_name} → {str(e)}"


# -----------------------------
# Discover all tables in layer
# -----------------------------
def get_all_tables(base_path, layer):
    """
    Reads SQL directory and extracts all table names.
    """

    layer_path = os.path.join(base_path, "Layer", layer)

    if not os.path.exists(layer_path):
        raise FileNotFoundError(
            f"Layer path not found → {layer_path}"
        )

    tables = []

    for file in os.listdir(layer_path):
        if file.endswith(".sql"):
            table = file.replace(".sql", "").lower()
            tables.append(table)

    if not tables:
        raise ValueError(
            f"No SQL files found in layer → {layer}"
        )

    return sorted(tables)

# ---------------- 4️⃣ Main Function ----------------
def run_driver(env: str, layer: str, table_name: str):

    print(f"📝 Inputs received → env: {env}, layer: {layer}, tables: {table_name}")

    # -----------------------------
    # Step 1: Load config
    # -----------------------------
    config = load_config(env)

    base_path = config.get("base_path")
    catalog_name = config.get("catalog")
    layer_schema_map = config.get("layer_schema_map")
    layer = layer.lower()
    
    if not base_path:
        raise ValueError(f"'base_path' missing in {env} config")
    if not catalog_name:
        raise ValueError(f"'catalog' missing in {env} config")
    if not layer_schema_map:
        raise ValueError(f"'layer_schema_map' missing in {env} config")

    if layer not in layer_schema_map:
        raise ValueError(
            f"Invalid layer '{layer}'. Must be one of {list(layer_schema_map.keys())}"
        )

    print(f"📂 Base path from config: {base_path}")
    print(f"🗂 Layer → Schema map from config: {layer_schema_map}")
    # -----------------------------
    # Step 2: BULK TABLE LIST
    # -----------------------------
    if table_name.lower() == "all":

        print("📂 'ALL' detected → Discovering all tables in layer")

        table_list = get_all_tables(base_path, layer)

    else:

        table_list = [
            t.strip().lower()
            for t in table_name.split(",")
            if t.strip()
        ]

    print(f"\n📦 Total tables to process → {len(table_list)}")
    print(f"📋 Tables → {table_list}")

    # -----------------------------
    # Step 3: Parallel Execution
    # -----------------------------
    results = []

    for table in table_list:

        result = process_table(
            env,
            layer,
            table,
            base_path,
            catalog_name,
            layer_schema_map
        )

        results.append(result)

    # -----------------------------
    # Step 4: Summary
    # -----------------------------
    print("\n" + "=" * 60)
    print("📊 BULK EXECUTION SUMMARY")
    print("=" * 60)

    success = [r for r in results if r.startswith("SUCCESS")]
    failed = [r for r in results if r.startswith("FAILED")]

    for r in success:
        print(f"✅ {r}")

    for r in failed:
        print(f"❌ {r}")

    print("\nTotals:")
    print(f"   Success : {len(success)}")
    print(f"   Failed  : {len(failed)}")

    print("\n🔎 Execution completed.")


# ---------------- 5️⃣ Main Entry ----------------
def main():
    args = parse_args()
    run_driver(
        env=args.env,
        layer=args.layer,
        table_name=args.table_name
    )


if __name__ == "__main__":
    main()
