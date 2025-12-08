from converter import convert_blob, convert_full

def test_convert_blob_regexp_replace_two_args_to_three():
    presto = "SELECT regexp_replace(col, '\\\\D') FROM t"
    dbsql, err = convert_blob(presto)
    assert err == ""
    assert "regexp_replace(col, '\\\\D', '')" in dbsql

def test_convert_full_splits_multiple_queries():
    sql = "SELECT 1; SELECT 2;"
    converted, errors, compatible = convert_full(sql)
    assert errors == ""
    assert converted.count("-- QUERY ") + compatible.count("-- QUERY ") == 2