from src.utils.helper_functions import ensure_regexp_replacement

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