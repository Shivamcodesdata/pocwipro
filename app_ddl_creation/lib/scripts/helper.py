# ---------------- 1️⃣ Import Libraries ----------------
import os
import json
import re
from pyspark.sql import SparkSession

# Initialize Spark session (Databricks already provides 'spark', but safe for standalone)
spark = SparkSession.builder.enableHiveSupport().getOrCreate()


# ---------------- 2️⃣ Load Config ----------------
def load_config(env: str) -> dict:
    # path of current file (driver.py)
    current_dir = os.path.dirname(os.path.abspath(__file__))

    # move up to app_ddl_creation/
    project_root = os.path.abspath(os.path.join(current_dir, "..", ".."))

    config_path = os.path.join(
        project_root, "env_config", env, "config.json"
    )

    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config not found: {config_path}")

    with open(config_path, "r") as f:
        config = json.load(f)

    return config


# ---------------- 3️⃣ Locate SQL File ----------------
def find_sql_file(base_path: str, layer: str, table_name: str) -> str:
    """
    Locate SQL file for a given table in a specific layer.
    """
    file_path = os.path.join(
        base_path,
        "Layer",      # important
        layer.lower(),
        f"{table_name}.sql"
    )

    print(file_path)

    if os.path.exists(file_path):
        print(f"✅ SQL file found: {file_path}")
        return file_path

    raise FileNotFoundError(
        f"SQL file not found: layer={layer}, table={table_name}"
    )


# ---------------- 4️⃣ Check if Table Exists ----------------
def check_table_exists(spark, catalog: str, schema: str, table: str) -> bool:
    """
    Unity Catalog safe existence check.
    Verifies both metadata AND queryability.
    """
    table = table.lower()

    result = spark.sql(
        f"SHOW TABLES IN {catalog}.{schema} LIKE '{table}'"
    ).collect()

    if not result:
        print(f"✅ Table '{catalog}.{schema}.{table}' does not exist.")
        return False

    # Extra safety: try a lightweight DESCRIBE
    try:
        spark.sql(f"DESCRIBE TABLE {catalog}.{schema}.{table}")
        print(f"⚠️ Table '{catalog}.{schema}.{table}' exists.")
        return True
    except Exception:
        print(f"⚠️ Metadata found but table not accessible: {catalog}.{schema}.{table}")
        return False


# ---------------- 5️⃣ Read SQL File ----------------
def read_sql_file(file_path: str) -> str:
    """
    Reads SQL file and returns its content as a string.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"SQL file not found: {file_path}")
    with open(file_path, "r") as f:
        return f.read()


# ---------------- 6️⃣ Parse Columns from SQL ----------------
def parse_columns_from_sql(sql_file: str) -> dict:
    """
    Parse SQL file and return a dictionary of columns with datatypes.
    Column names are converted to lowercase for comparison.
    """
    content = read_sql_file(sql_file)
    content_clean = " ".join(content.replace("\n", " ").split())

    match = re.search(
    r"create\s+table\s+(?:if\s+not\s+exists\s+)?[\w\.]+\s*\((.*?)\)\s*using",
    content_clean,
    re.IGNORECASE | re.DOTALL
)
    if not match:
        raise ValueError(f"Cannot parse columns from SQL file: {sql_file}")

    columns_str = match.group(1)
    columns = columns_str.split(",")
    col_dict = {}

    for col in columns:
        parts = col.strip().split()
        if len(parts) >= 2:
            col_name = parts[0].lower()
            col_type = normalize_datatype(parts[1])
            col_dict[col_name] = col_type

    return col_dict


# ---------------- 7️⃣ Get Table Schema ----------------
def get_table_schema(spark, table_name: str) -> dict:
    """
    Returns Spark table schema as {column_name: datatype}.
    Column names are lowercase and datatypes normalized.
    """
    df = spark.table(table_name)
    return {
        f.name.lower(): normalize_datatype(f.dataType.simpleString())
        for f in df.schema.fields
    }


# ---------------- 8️⃣ Normalize Datatype ----------------
def normalize_datatype(datatype: str) -> str:
    """
    Normalize datatypes for comparison.
    Converts to uppercase and handles common Spark type aliases.
    """
    dt = datatype.upper()
    if dt in ["STRING", "VARCHAR", "CHAR"]:
        return "STRING"
    if dt in ["INT", "INTEGER", "BIGINT", "LONG"]:
        return "INT"
    if dt in ["DOUBLE", "FLOAT", "DECIMAL"]:
        return "DOUBLE"
    # Add more rules if needed
    return dt



def detect_schema_drift(sql_columns: dict, table_schema: dict) -> dict:
    return {
        "missing_in_table": {
            col: sql_columns[col]
            for col in sql_columns
            if col not in table_schema
        },
        "type_mismatch": {
            col: {
                "sql": sql_columns[col],
                "table": table_schema[col]
            }
            for col in sql_columns
            if col in table_schema and sql_columns[col] != table_schema[col]
        },
        "extra_in_table": {
            col: table_schema[col]
            for col in table_schema
            if col not in sql_columns
        }
    }


def generate_alter_statements(full_table_name: str, drift: dict) -> list:
    statements = []

    # ADD COLUMN
    for col, dtype in drift.get("missing_in_table", {}).items():
        statements.append(
            f"ALTER TABLE {full_table_name} ADD COLUMNS ({col} {dtype})"
        )

    # TYPE MISMATCH
    for col, info in drift.get("type_mismatch", {}).items():
        statements.append(
            f"ALTER TABLE {full_table_name} ALTER COLUMN {col} TYPE {info['sql']}"
        )

    # -------- Improved messaging --------
    if not statements:
        if drift.get("extra_in_table"):
            print(
                "ℹ️ Schema drift detected: target table has extra columns "
                "not present in source SQL. No action taken (DROP not allowed in v1.0)."
            )
        else:
            print("✅ Source and target schemas are aligned. No changes required.")

    return statements

# ---------------- 9️⃣ Compare Schema and Alter Table ----------------
def compare_schema_and_alter(table_name: str, sql_columns: dict) -> list:
    """
    Compares source SQL schema with existing table schema and applies
    allowed schema changes (ADD / TYPE ALTER).

    DROP operations are intentionally not supported in v1.0.
    """
    executed_alters = []

    # -----------------------------
    # Step 1: Read existing schema
    # -----------------------------
    table_schema = get_table_schema(table_name)

    # -----------------------------
    # Step 2: Detect schema drift
    # -----------------------------
    drift = detect_schema_drift(sql_columns, table_schema)

    # -----------------------------
    # Step 3: Inform about extra columns (NO DROP)
    # -----------------------------
    if drift.get("extra_in_table"):
        print(
            "ℹ️ Schema drift detected: target table has extra columns "
            "not present in source SQL. No action taken (DROP not supported in v1.0)."
        )

    # -----------------------------
    # Step 4: Generate ALTER statements
    # -----------------------------
    alter_stmts = generate_alter_statements(table_name, drift)

    # -----------------------------
    # Step 5: Apply ALTERs
    # -----------------------------
    for stmt in alter_stmts:
        print(f"🔧 Executing: {stmt}")
        spark.sql(stmt)
        executed_alters.append(stmt)

    # -----------------------------
    # Step 6: Final messaging
    # -----------------------------
    if not executed_alters and not drift.get("extra_in_table"):
        print("✅ Source and target schemas are aligned. No changes required.")

    return executed_alters

def apply_schema_changes(
    spark,
    table_name: str,
    alter_statements: list,
    env: str,
    drift: dict | None = None
):
    """
    Applies schema changes based on generated ALTER statements.
    PROD environment is protected by default.
    """

    # -----------------------------
    # No ALTER statements
    # -----------------------------
    if not alter_statements:
        if drift and drift.get("extra_in_table"):
            print(
                "ℹ️ Schema drift detected: target table has extra columns. "
                "No action taken (DROP not supported in v1.0)."
            )
        else:
            print("✅ Source and target schemas are aligned. No changes required.")
        return

    # -----------------------------
    # Drift detected
    # -----------------------------
    print("⚠️ Schema drift detected. Applying allowed changes...")

    # -----------------------------
    # PROD safety guard
    # -----------------------------
    if env.lower() == "prod":
        raise RuntimeError(
            "🚫 Schema drift detected in PROD. "
            "Automatic schema changes are disabled. Manual approval required."
        )

    # -----------------------------
    # Execute ALTER statements
    # -----------------------------
    for stmt in alter_statements:
        print(f"🔧 Executing: {stmt}")
        spark.sql(stmt)

    print("✅ Schema changes applied successfully.")
