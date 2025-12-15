import sqlglot
from src.utils.helper_functions import (
    strip_ansi,
    find_quoted_content,
    unescape_wrapped_sql_content,
    normalize_identifiers,
    force_aliases_pre_parse,
    ensure_regexp_replacement,
    convert_trim_syntax,
    repair_common_trailing_mistakes,
    ast_fix_regexp_nodes,
    safe_split_sql,
    balance_single_quotes,
    quoted_identifier_set,
    is_semantically_same,
    remove_ansi_and_control_from_file,
)

# ----------------------------------------------------------------------
# EXECUTE ... USING extractor (unwraps embedded SQL)
# ----------------------------------------------------------------------

def extract_inner_from_execute(blob: str) -> str:
    blob = strip_ansi(blob)
    import re
    m = re.search(r"\bUSING\b", blob, flags=re.IGNORECASE)
    if m:
        first, pos1 = find_quoted_content(blob, m.end(), "'")
        if pos1 != -1:
            comma = blob.find(",", pos1)
            if comma != -1:
                second, pos2 = find_quoted_content(blob, comma + 1, "'")
                if pos2 != -1:
                    return second
        first_any, _ = find_quoted_content(blob, 0, "'")
        if first_any and first_any.strip().upper().startswith("SELECT"):
            return first_any
    return blob

# ----------------------------------------------------------------------
# PREPARE ... FROM extractor (unwraps embedded SQL)
# ----------------------------------------------------------------------

def extract_inner_from_prepare(blob: str) -> str:
    """Extract the SQL query from PREPARE stmt FROM <query> statements."""
    blob = strip_ansi(blob)
    import re
    # Match PREPARE ... FROM pattern (case insensitive)
    m = re.search(r"\bPREPARE\s+\w+\s+FROM\s+", blob, flags=re.IGNORECASE)
    if m:
        # Extract everything after "FROM"
        sql_part = blob[m.end():].strip()
        # Remove trailing semicolon if present
        sql_part = sql_part.rstrip(';').strip()
        return sql_part
    return blob

# ----------------------------------------------------------------------
# MAIN BLOB CONVERTER
# ----------------------------------------------------------------------

def convert_blob(blob: str):
    try:
        blob = strip_ansi(blob)

        # First check for PREPARE statements, then EXECUTE statements
        inner = extract_inner_from_prepare(blob)
        if inner == blob:  # No PREPARE found, check for EXECUTE
            inner = extract_inner_from_execute(blob)
        
        inner = unescape_wrapped_sql_content(inner)
        inner = normalize_identifiers(inner)
        inner = force_aliases_pre_parse(inner)
        inner = ensure_regexp_replacement(inner)
        inner = convert_trim_syntax(inner)
        inner = repair_common_trailing_mistakes(inner)
        inner = balance_single_quotes(inner)
        # Strip any ANSI codes that may have been introduced
        inner = strip_ansi(inner)

        parsed = sqlglot.parse_one(inner, read="presto")
        parsed = ast_fix_regexp_nodes(parsed)
        dbsql = parsed.sql(dialect="databricks")
        dbsql = strip_ansi(dbsql)
        return dbsql, ""

    except Exception as e:
        cleaned = inner if 'inner' in locals() else blob
        cleaned = strip_ansi(cleaned)
        cleaned = repair_common_trailing_mistakes(cleaned)
        cleaned = balance_single_quotes(cleaned)
        # Strip ANSI codes from error message as well
        error_msg = strip_ansi(str(e))
        return "", f"{error_msg}\n-- CLEANED_CANDIDATE:\n{cleaned}"

# ----------------------------------------------------------------------
# Unified converter (classification: converted / compatible / errors)
# ----------------------------------------------------------------------

def convert_full(sql_text: str):
    tokens = safe_split_sql(sql_text)

    converted_arr = []
    compatible_arr = []
    errors_arr = []

    for idx, t in enumerate(tokens, start=1):
        conv, err = convert_blob(t)

        if err:
            errors_arr.append(f"-- QUERY {idx}\n-- ERROR:\n{err}\n")
            continue

        # Determine classification: Compatible vs Converted
        orig_ast = normalize_identifiers(t).strip().rstrip(";")
        conv_ast = conv.strip().rstrip(";")

        same_ast = is_semantically_same(orig_ast, conv_ast)
        orig_q = quoted_identifier_set(t)
        conv_q = quoted_identifier_set(conv)

        if same_ast and orig_q == conv_q:
            # Already compatible
            compatible_arr.append(f"-- QUERY {idx}\n{t.strip()};\n")
        else:
            # Converted
            converted_arr.append(f"-- QUERY {idx}\n{conv.strip()};\n")

    return (
        "\n".join(converted_arr),
        "\n".join(errors_arr),
        "\n".join(compatible_arr),
    )

# ----------------------------------------------------------
# FINAL: Write converted / errors / already compatible
# ----------------------------------------------------------
def process_file(input_path, output_converted, output_errors, output_compatible,
                 remove_ansi_first=True):

    if remove_ansi_first:
        remove_ansi_and_control_from_file(input_path)

    with open(input_path, "r", encoding="utf-8") as f:
        content = f.read()

    tokens = safe_split_sql(content)

    conv_out = []
    err_out = []
    compat_out = []

    for idx, t in enumerate(tokens, start=1):
        print(f"Processing query {idx}...")
        converted, err = convert_blob(t)

        if err:
            err_out.append(f"-- QUERY {idx}\n-- ERROR:\n{err}\n")
        else:
            orig_for_ast = normalize_identifiers(t).strip().rstrip(";")
            conv_for_ast = converted.strip().rstrip(";")

            same_ast = is_semantically_same(orig_for_ast, conv_for_ast)
            orig_quoted = quoted_identifier_set(t)
            conv_quoted = quoted_identifier_set(converted)
            if same_ast and orig_quoted == conv_quoted:
                compat_out.append(f"-- QUERY {idx}\n{t.strip()};\n")
            else:
                conv_out.append(f"-- QUERY {idx}\n{converted.strip()};\n")

    with open(output_converted, "w", encoding="utf-8") as f:
        f.write("\n".join(conv_out))

    with open(output_errors, "w", encoding="utf-8") as f:
        f.write("\n".join(err_out))

    with open(output_compatible, "w", encoding="utf-8") as f:
        f.write("\n".join(compat_out))

    print("Process completed")