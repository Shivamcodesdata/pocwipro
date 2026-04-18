1. Executive Summary

This project delivers a DDL Automation & Schema Governance Framework built on Databricks, PySpark, and Unity Catalog.

The framework enables controlled, repeatable, and environment-aware table creation and schema evolution using source-controlled SQL files packaged as a Python wheel.

It is specifically designed for enterprise and regulated data platforms where schema changes must be:

Version-controlled

Environment-aware (dev / qa / prod)

Safe to re-run

Auditable

Governed (no destructive operations)

The solution replaces ad-hoc notebook-based DDL execution with a standardized, automation-first approach.

2. Business Problem Statement

In large Databricks environments, schema management typically suffers from:

Manual execution of SQL via notebooks

Inconsistent schemas across environments

Lack of clear ownership and governance

High production risk during schema changes

Limited CI/CD and auditability

These issues frequently lead to:

Pipeline failures

Production incidents

Delayed releases

Compliance concerns

This framework addresses these challenges by introducing centralized, automated, and governed DDL execution.

3. Solution Overview

The DDL Automation Framework provides:

A single CLI / job-driven entry point

Layer-based table management (bronze / silver / gold)

Fully qualified table resolution (catalog.schema.table)

Automated schema validation and drift detection

Strong production safety controls

CI/CD-ready testing and packaging

All table definitions are treated as code, not manual operations.

4. High-Level Architecture
Core Components

Python Wheel Package
Contains all execution logic and utilities

SQL Definitions (Git-managed)
One SQL file per table, per layer

Databricks Job / CLI Execution
Parameter-driven execution

Pytest Test Suite
Validation before deployment

Logical Flow
SQL (Git Repo)
     ↓
DDL Automation Framework (Wheel)
     ↓
Schema Validation & Governance
     ↓
Databricks / Unity Catalog
workspace.<layer>.<table>

5. Project Structure
app_ddl_creation/
├── main/
│   └── driver.py              # CLI / Job entry point
│
├── lib/scripts/
│   └── helper.py              # Reusable core logic
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


This structure enforces clear separation of concerns and supports scalable growth.

6. Configuration Management

Each environment has its own configuration file.

Example (env_config/dev/config.json)
{
  "base_path": "/Workspace/Poc/app_ddl_creation",
  "catalog": "workspace",
  "layer_schema_map": {
    "bronze": "bronze",
    "silver": "silver",
    "gold": "gold"
  }
}

Key Benefits

No hardcoded catalog or schema names

Clean environment isolation

Simple promotion from dev → qa → prod

Supports enterprise multi-environment standards

7. SQL Management Strategy
Layer-Based Design

Bronze – Raw ingestion tables

Silver – Cleaned and standardized tables

Gold – Business-ready, consumption tables

Each table has exactly one SQL file:

Layer/<layer>/<table_name>.sql

Why SQL Inside the Wheel

No dependency on Databricks Workspace paths

Immutable, versioned deployments

CI/CD compatible

Eliminates manual file handling

8. Execution Flow (Runtime)
Entry Point
app_ddl_creation --env=dev --layer=gold --table_name=sampleB

Runtime Steps

Parse input arguments

Load environment configuration

Locate SQL file

Resolve fully qualified table name
workspace.gold.sampleb

Check table existence

Validate schema

Apply safe schema changes (if allowed)

Execute SQL via Spark

9. Schema Drift Detection & Governance

Before applying any change, the framework compares:

SQL-defined schema

Existing table schema

Drift Scenarios Handled
Scenario	Framework Behavior
New column in SQL	ADD COLUMN
Datatype mismatch	Block execution
Column removed in SQL	Block execution
Target has extra columns	Block execution
PROD drift detected	Manual approval required

DROP operations are intentionally not supported

This design prevents accidental data loss.

10. Clear Runtime Feedback

The framework provides explicit, user-friendly messages, for example:

❌ Schema drift detected
Target table has extra column: legacy_flag

DROP operations are not supported.
No changes were applied.


This ensures users always understand:

What was detected

Why execution stopped

What action is required

11. Unit Testing & Quality Assurance
Why Testing Matters

Catch issues before Databricks execution

Prevent broken SQL or config deployments

Enable safe refactoring

Covered Areas
Component	Tested
Config loading	✅
SQL discovery	✅
SQL parsing	✅
Schema validation	✅
Driver flow	✅

Tests run locally and in CI pipelines using pytest.

12. CI/CD Readiness

The framework is designed for automated delivery:

Run pytest on commit

Build Python wheel

Deploy wheel to Databricks

Trigger jobs with parameters

This enables:

Zero manual DDL execution

Consistent deployments

Full audit trail

13. Security & Governance

No credentials stored in code

No workspace file dependencies

SQL and logic fully version-controlled

Deterministic, repeatable execution

Production safety guards built-in

14. Key Client Benefits

✅ Reduced production risk

✅ Standardized schema management

✅ Faster table onboarding

✅ CI/CD aligned

✅ Enterprise & compliance ready

15. Version Status & Roadmap
Version 1.0 (Locked)

Included

Table creation

Schema drift detection

Safe ALTER support

Environment awareness

Explicitly Excluded

DROP operations

Automatic PROD schema changes

Future Enhancements

Approval workflow for PROD

Schema version history

Dry-run mode

Audit logging

Multi-catalog expansion

16. Conclusion

This framework transforms schema management from a manual, high-risk activity into a governed, automated, and auditable engineering process, aligned with Databricks and enterprise best practices.