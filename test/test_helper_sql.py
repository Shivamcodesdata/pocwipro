import os
import re

from app_ddl_creation.lib.scripts.helper import (
    find_sql_file,
    read_sql_file,
    parse_columns_from_sql
)

def test_find_sql_file(tmp_path):
    sql_dir = tmp_path / "Layer" / "bronze"
    sql_dir.mkdir(parents=True)

    file = sql_dir / "sample.sql"
    file.write_text("select * from test")

    result = find_sql_file(str(tmp_path), "bronze", "sample")
    assert result.endswith("sample.sql")


def test_read_sql_file(tmp_path):
    sql_file = tmp_path / "test.sql"
    sql_file.write_text("select id, name from table")

    content = read_sql_file(str(sql_file))
    assert "select id" in content


def parse_columns_from_sql(sql_or_path: str) -> list:
    if os.path.exists(sql_or_path):
        content = read_sql_file(sql_or_path)
    else:
        content = sql_or_path

    pattern = re.compile(r"\s*(\w+)\s+\w+", re.IGNORECASE)
    matches = pattern.findall(content)

    return matches
