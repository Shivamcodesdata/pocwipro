# DDL Automation Framework (Databricks)

## 1. Executive Summary
This project is a **DDL Automation Framework** built on **Databricks + PySpark**. It enables controlled, repeatable, and environment-aware creation and evolution of database tables using SQL files packaged inside a Python wheel.

The framework is designed for **enterprise data platforms** where schema changes must be:
- Version-controlled
- Environment-specific (dev / qa / prod)
- Auditable
- Safe to run multiple times

---

## 2. Business Problem Statement
In large data platforms, table creation and schema evolution often suffer from:
- Manual execution of SQL scripts
- Environment-specific inconsistencies
- Missing governance and repeatability
- High risk during schema changes

This framework solves these problems by providing:
- A single CLI-driven entry point
- Layer-based table management (bronze / silver / gold)
- Automated validation and execution
- Testable and CI/CD-friendly architecture

---

## 3. High-Level Architecture

### Components
- **Python Wheel Package** – Core logic
- **SQL Files** – Table definitions per layer
- **Databricks Job / CLI** – Execution layer
- **Pytest** – Local and CI validation

### Logical Flow
1. User triggers the framework via CLI or Databricks Job
2. Environment configuration is loaded
3. SQL file is discovered based on layer and table name
4. Target catalog/schema is resolved
5. SQL is executed safely using Spark

---

## 4. Project Structure

```
app_ddl_creation/
├── main/
│   └── driver.py            # Entry point
│
├── lib/scripts/
│   └── helper.py            # Core reusable logic
│
├── Layer/
│   ├── bronze/
│   ├── silver/
│   └── gold/
│       └── sampleB.sql
│
├── env_config/
│   └── dev/
│       └── config.json
│
├── tests/
│   ├── test_driver.py
│   ├── test_helper_sql.py
│   └── test_helper_config.py
│
└── pyproject.toml
```

---

## 5. Configuration Management

Each environment has its own configuration file:

**Example: `env_config/dev/config.json`**
```json
{
  "layer_catalog_map": {
    "bronze": "bronze",
    "silver": "silver",
    "gold": "gold"
  }
}
```

### Benefits
- Environment isolation
- No hardcoded catalogs or schemas
- Easy promotion from dev → prod

---

## 6. SQL Management Strategy

### Layer-Based Design
- **Bronze** – Raw ingestion tables
- **Silver** – Cleaned / conformed tables
- **Gold** – Business-ready tables

Each table has exactly **one SQL file**:
```
Layer/<layer>/<table_name>.sql
```

### Why SQL Inside the Wheel
- No dependency on Databricks Workspace paths
- Immutable deployments
- Compatible with CI/CD

---

## 7. Execution Flow (Technical)

### Entry Point
The framework is executed via a registered CLI entry point:

```bash
app_ddl_creation --env=dev --layer=gold --table_name=sampleB
```

### Runtime Steps
1. Parse input arguments
2. Load environment configuration
3. Locate SQL file using package resources
4. Resolve catalog/schema
5. Execute SQL via Spark

---

## 8. Unit Testing Strategy

### Why Unit Testing
- Catch errors before Databricks execution
- Prevent missing SQL or config issues
- Enable safe refactoring

### What Is Tested
| Area | Covered |
|-----|--------|
Config loading | Yes |
SQL discovery | Yes |
SQL parsing | Yes |
Table existence checks | Yes |

### Tooling
- **pytest**
- Local SparkSession for validation

---

## 9. CI/CD Readiness

The framework is designed to plug into CI/CD pipelines:

1. Run pytest on every commit
2. Build Python wheel
3. Deploy wheel to Databricks
4. Trigger jobs using parameters

This ensures:
- Zero manual SQL execution
- Controlled schema changes
- Audit-ready deployments

---

## 10. Security & Governance

- No hardcoded credentials
- No workspace file dependencies
- Version-controlled SQL and logic
- Deterministic deployments

---

## 11. Key Advantages for Client

- ✅ Standardized DDL execution
- ✅ Reduced production risk
- ✅ Faster onboarding of new tables
- ✅ CI/CD compatible
- ✅ Databricks best practices aligned

---

## 12. Future Enhancements

- ALTER TABLE automation
- Schema drift detection
- Dry-run mode
- Audit logging
- Multi-catalog support

---

## 13. Conclusion

This DDL Automation Framework provides a **scalable, testable, and enterprise-ready** solution for managing table definitions in Databricks. It replaces manual processes with a governed, automated, and auditable approach suitable for modern data platforms.

