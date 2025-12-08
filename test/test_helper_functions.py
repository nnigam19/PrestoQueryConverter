from src.utils.helper_functions import ensure_regexp_replacement

def test_ensure_regexp_replacement_adds_empty_rep():
    presto = "SELECT regexp_replace(col, '\\\\D') FROM t"
    fixed = ensure_regexp_replacement(presto)
    assert "regexp_replace(col, '\\\\D', '')" in fixed