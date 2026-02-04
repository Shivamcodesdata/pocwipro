from app_ddl_creation.lib.scripts.helper import (
    detect_schema_drift,
    generate_alter_statements
)

# -----------------------------
# 1️⃣ No drift
# -----------------------------
def test_detect_schema_drift_no_change():
    sql_schema = {
        "id": "INT",
        "name": "STRING"
    }

    table_schema = {
        "id": "INT",
        "name": "STRING"
    }

    drift = detect_schema_drift(sql_schema, table_schema)

    assert drift["missing_in_table"] == {}
    assert drift["type_mismatch"] == {}
    assert drift["extra_in_table"] == {}


# -----------------------------
# 2️⃣ Missing column → ADD COLUMN
# -----------------------------
def test_detect_schema_drift_missing_column():
    sql_schema = {
        "id": "INT",
        "name": "STRING",
        "amount": "DOUBLE"
    }

    table_schema = {
        "id": "INT",
        "name": "STRING"
    }

    drift = detect_schema_drift(sql_schema, table_schema)

    assert drift["missing_in_table"] == {
        "amount": "DOUBLE"
    }
    assert drift["type_mismatch"] == {}


# -----------------------------
# 3️⃣ Datatype mismatch
# -----------------------------
def test_detect_schema_drift_type_mismatch():
    sql_schema = {
        "id": "INT",
        "price": "DOUBLE"
    }

    table_schema = {
        "id": "INT",
        "price": "INT"
    }

    drift = detect_schema_drift(sql_schema, table_schema)

    assert "price" in drift["type_mismatch"]
    assert drift["type_mismatch"]["price"]["sql"] == "DOUBLE"
    assert drift["type_mismatch"]["price"]["table"] == "INT"


# -----------------------------
# 4️⃣ Extra column in table (no DROP)
# -----------------------------
def test_detect_schema_drift_extra_column():
    sql_schema = {
        "id": "INT"
    }

    table_schema = {
        "id": "INT",
        "legacy_col": "STRING"
    }

    drift = detect_schema_drift(sql_schema, table_schema)

    assert drift["extra_in_table"] == {
        "legacy_col": "STRING"
    }


# -----------------------------
# 5️⃣ ALTER statement generation
# -----------------------------
def test_generate_alter_statements():
    drift = {
        "missing_in_table": {"amount": "DOUBLE"},
        "type_mismatch": {
            "price": {"sql": "DOUBLE", "table": "INT"}
        },
        "extra_in_table": {}
    }

    stmts = generate_alter_statements("gold.sales", drift)

    assert "ALTER TABLE gold.sales ADD COLUMNS (amount DOUBLE)" in stmts
    assert "ALTER TABLE gold.sales ALTER COLUMN price TYPE DOUBLE" in stmts
