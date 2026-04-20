import re
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
    validate_sql,
    format_validation_issues,
)
from src.utils.presto_functions import convert_presto_functions

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

def _preprocess(blob: str) -> str:
    """Run all pre-parse transformations on a SQL string."""
    inner = extract_inner_from_prepare(blob)
    was_wrapped = inner != blob
    if not was_wrapped:
        inner = extract_inner_from_execute(blob)
        was_wrapped = inner != blob

    if was_wrapped:
        inner = unescape_wrapped_sql_content(inner)
    inner = normalize_identifiers(inner)
    inner = force_aliases_pre_parse(inner)
    inner = ensure_regexp_replacement(inner)
    inner = convert_trim_syntax(inner)
    inner = convert_presto_functions(inner)
    inner = repair_common_trailing_mistakes(inner)
    inner = balance_single_quotes(inner)
    inner = strip_ansi(inner)
    inner = inner.rstrip().rstrip(";").rstrip()
    return inner


def _split_top_level_union(sql: str):
    """Split a SQL string on top-level UNION ALL, respecting quotes, parens, and comments."""
    parts = []
    cur = []
    depth = 0
    in_sq = in_dq = False
    in_line_comment = False
    in_block_comment = False
    i, n = 0, len(sql)

    while i < n:
        ch = sql[i]

        if in_line_comment:
            cur.append(ch)
            if ch == '\n':
                in_line_comment = False
            i += 1
            continue

        if in_block_comment:
            cur.append(ch)
            if ch == '*' and i + 1 < n and sql[i + 1] == '/':
                cur.append('/')
                i += 2
                in_block_comment = False
            else:
                i += 1
            continue

        if not in_sq and not in_dq:
            if ch == '-' and i + 1 < n and sql[i + 1] == '-':
                cur.append('-'); cur.append('-')
                i += 2
                in_line_comment = True
                continue
            if ch == '/' and i + 1 < n and sql[i + 1] == '*':
                cur.append('/'); cur.append('*')
                i += 2
                in_block_comment = True
                continue

        if ch == "'" and not in_dq:
            cur.append(ch)
            if i + 1 < n and sql[i + 1] == "'":
                cur.append("'"); i += 2
            else:
                in_sq = not in_sq; i += 1
            continue
        if ch == '"' and not in_sq:
            cur.append(ch)
            in_dq = not in_dq
            i += 1
            continue

        if not in_sq and not in_dq:
            if ch == '(':
                depth += 1
            elif ch == ')':
                depth -= 1

            if depth == 0:
                tail = sql[i:]
                m = re.match(r'\bUNION\s+ALL\b', tail, re.IGNORECASE)
                if m:
                    parts.append("".join(cur).strip())
                    cur = []
                    i += m.end()
                    continue

        cur.append(ch)
        i += 1

    leftover = "".join(cur).strip()
    if leftover:
        parts.append(leftover)
    return parts


def convert_blob(blob: str):
    blob = strip_ansi(blob)

    try:
        inner = _preprocess(blob)
    except Exception as e:
        error_msg = strip_ansi(str(e))
        return "", f"{error_msg}\n-- CLEANED_CANDIDATE:\n{strip_ansi(blob)}"

    parse_err = None
    try:
        parsed = sqlglot.parse_one(inner, read="presto")
        parsed = ast_fix_regexp_nodes(parsed)
        dbsql = parsed.sql(dialect="databricks")
        return strip_ansi(dbsql), ""
    except Exception as e:
        parse_err = e

    try:
        parts = _split_top_level_union(inner)
        if len(parts) > 1:
            converted_parts = []
            for part in parts:
                part = part.rstrip().rstrip(";").rstrip()
                parsed = sqlglot.parse_one(part, read="presto")
                parsed = ast_fix_regexp_nodes(parsed)
                converted_parts.append(parsed.sql(dialect="databricks"))
            dbsql = "\nUNION ALL\n".join(converted_parts)
            return strip_ansi(dbsql), ""
    except Exception:
        pass

    cleaned = strip_ansi(inner)
    cleaned = repair_common_trailing_mistakes(cleaned)
    cleaned = balance_single_quotes(cleaned)
    error_msg = strip_ansi(str(parse_err))
    return "", f"{error_msg}\n-- CLEANED_CANDIDATE:\n{cleaned}"

# ----------------------------------------------------------------------
# Unified converter (classification: converted / compatible / errors)
# ----------------------------------------------------------------------

def convert_full(sql_text: str):
    # --- Pre-conversion validation ---
    issues = validate_sql(sql_text)
    blocking = [i for i in issues if i['severity'] == 'ERROR']

    if blocking:
        formatted = format_validation_issues(issues)
        return ("", f"-- SQL VALIDATION FAILED\n{formatted}", "")

    tokens = safe_split_sql(sql_text)

    converted_arr = []
    compatible_arr = []
    errors_arr = []
    warnings_text = ""

    non_blocking = [i for i in issues if i['severity'] == 'WARNING']
    if non_blocking:
        warnings_text = format_validation_issues(non_blocking)

    for idx, t in enumerate(tokens, start=1):
        conv, err = convert_blob(t)

        if err:
            errors_arr.append(f"-- QUERY {idx}\n-- ERROR:\n{err}\n")
            continue

        orig_ast = normalize_identifiers(t).strip().rstrip(";")
        conv_ast = conv.strip().rstrip(";")

        same_ast = is_semantically_same(orig_ast, conv_ast)
        orig_q = quoted_identifier_set(t)
        conv_q = quoted_identifier_set(conv)

        if same_ast and orig_q == conv_q:
            compatible_arr.append(f"-- QUERY {idx}\n{t.strip()};\n")
        else:
            converted_arr.append(f"-- QUERY {idx}\n{conv.strip()};\n")

    errors_output = "\n".join(errors_arr)
    if warnings_text and errors_output:
        errors_output = f"-- WARNINGS\n{warnings_text}\n\n{errors_output}"
    elif warnings_text:
        errors_output = f"-- WARNINGS\n{warnings_text}"

    return (
        "\n".join(converted_arr),
        errors_output,
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