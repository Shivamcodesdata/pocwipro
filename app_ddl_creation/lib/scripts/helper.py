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
def check_table_exists(table_name: str) -> bool:
    """
    Check if a table already exists in Spark catalog.
    Returns True if exists, False otherwise.
    """
    tables = [t.name.lower() for t in spark.catalog.listTables()]
    if table_name.lower() in tables:
        print(f"⚠️ Table '{table_name}' already exists in Spark catalog.")
        return True
    else:
        print(f"✅ Table '{table_name}' does not exist in Spark catalog.")
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
def get_table_schema(table_name: str) -> dict:
    """
    Returns Spark table schema as {column_name: datatype}.
    Column names are lowercase and datatypes normalized.
    """
    df = spark.table(table_name)
    schema_dict = {f.name.lower(): normalize_datatype(f.dataType.simpleString()) for f in df.schema.fields}
    return schema_dict


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


# ---------------- 9️⃣ Compare Schema and Alter Table ----------------
def compare_schema_and_alter(table_name: str, sql_columns: dict) -> list:
    """
    Compare SQL columns with table schema and:
    1. Raise exception if SQL has fewer columns
    2. ALTER TABLE to add extra columns
    3. Raise exception if datatype mismatch
    Returns: list of executed ALTER statements for logging
    """
    executed_alters = []
    existing_schema = get_table_schema(table_name)

    existing_cols_set = set(existing_schema.keys())
    sql_cols_set = set(sql_columns.keys())

    # Case 1: SQL has fewer columns → Exception
    if len(sql_cols_set) < len(existing_cols_set):
        raise Exception(f"SQL file has fewer columns ({len(sql_cols_set)}) than existing table ({len(existing_cols_set)})")

    # Case 2: SQL has extra columns → ALTER TABLE
    extra_cols = sql_cols_set - existing_cols_set
    for col in extra_cols:
        datatype = sql_columns[col]
        alter_query = f"ALTER TABLE {table_name} ADD COLUMNS ({col} {datatype})"
        print(f"🔧 Executing: {alter_query}")
        spark.sql(alter_query)
        executed_alters.append(alter_query)

    # Case 3: Check datatype for common columns
    common_cols = sql_cols_set & existing_cols_set
    for col in common_cols:
        sql_type = sql_columns[col]
        table_type = existing_schema[col]
        if sql_type != table_type:
            raise Exception(f"Datatype mismatch for column '{col}': SQL={sql_type}, Table={table_type}")

    if executed_alters:
        print(f"✅ Executed ALTER statements: {executed_alters}")
    else:
        print("✅ No ALTER needed. Table schema matches SQL.")

    return executed_alters
