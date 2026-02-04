from app_ddl_creation.lib.scripts.helper import check_table_exists

def check_table_exists(spark, database: str, table: str) -> bool:
    try:
        spark.sql(f"DESCRIBE TABLE {database}.{table}")
        return True
    except Exception:
        return False
