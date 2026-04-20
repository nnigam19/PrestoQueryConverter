from src.utils.helper_functions import ensure_regexp_replacement, force_aliases_pre_parse, safe_split_sql

def test_ensure_regexp_replacement_adds_empty_rep():
    presto = "SELECT regexp_replace(col, '\\\\D') FROM t"
    fixed = ensure_regexp_replacement(presto)
    assert "regexp_replace(col, '\\\\D', '')" in fixed


def test_force_aliases_does_not_eat_union_after_as_n():
    sql = "select a as n union all select b as m from t"
    assert force_aliases_pre_parse(sql) == sql


def test_force_aliases_rewrites_multiword_unquoted_alias():
    sql = "select col as my project name from t"
    out = force_aliases_pre_parse(sql)
    assert "as my_project_name" in out.lower()


def test_safe_split_sql_handles_line_comments_with_quotes():
    sql = "SELECT 1 -- it's a comment\n; SELECT 2;"
    result = safe_split_sql(sql)
    assert len(result) == 2


def test_safe_split_sql_handles_block_comments_with_quotes():
    sql = "SELECT 1 /* it's a 'test */ ; SELECT 2;"
    result = safe_split_sql(sql)
    assert len(result) == 2


def test_safe_split_sql_no_split_inside_block_comment():
    sql = "SELECT 1 /* ; */ FROM t;"
    result = safe_split_sql(sql)
    assert len(result) == 1


def test_safe_split_sql_no_split_inside_line_comment():
    sql = "SELECT 1 -- ;\nFROM t;"
    result = safe_split_sql(sql)
    assert len(result) == 1


def test_safe_split_sql_strips_trailing_semicolon():
    sql = "SELECT 'Tellius' as n\n) n\n) where cost is not null\n);"
    result = safe_split_sql(sql)
    assert len(result) == 1
    assert not result[0].endswith(";")