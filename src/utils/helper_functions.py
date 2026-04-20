import re
import sqlglot
from sqlglot import expressions as exp

# ----------------------------------------------------------------------
# CONFIG / PATTERNS
# ----------------------------------------------------------------------

_ANSI_RE = re.compile(r'\x1B\[[0-?]*[ -/]*[@-~]')
_DOUBLE_QUAL_QUOTE = re.compile(r'(\b\w+\b)\.\s*"([^"]+)"')
_QUOTED_IDENT = re.compile(r'"([^"]+)"')
_BACKTICK_IDENT = re.compile(r'`([^`]+)`')

_AS_DOUBLE_QUOTED_ALIAS = re.compile(r'AS\s+"([^"]+)"', flags=re.IGNORECASE)
_AS_SINGLE_QUOTED_ALIAS = re.compile(r"AS\s+'([^']+)'", flags=re.IGNORECASE)

_KEYWORDS_IN_UNQUOTED_ALIAS = frozenset(
    {
        "all",
        "and",
        "any",
        "as",
        "between",
        "both",
        "by",
        "case",
        "cross",
        "distinct",
        "else",
        "end",
        "except",
        "exists",
        "false",
        "from",
        "full",
        "group",
        "having",
        "ilike",
        "in",
        "inner",
        "intersect",
        "is",
        "join",
        "lateral",
        "leading",
        "left",
        "like",
        "limit",
        "natural",
        "not",
        "null",
        "offset",
        "on",
        "or",
        "order",
        "outer",
        "recursive",
        "right",
        "select",
        "similar",
        "some",
        "then",
        "to",
        "trailing",
        "true",
        "union",
        "using",
        "when",
        "where",
        "with",
    }
)

_REGEXP_REPLACE_2ARGS = re.compile(
    r"(regexp_replace)\(\s*([^\),]+?)\s*,\s*('(?:[^']|''|\\')*')\s*\)",
    flags=re.IGNORECASE,
)

# ----------------------------------------------------------------------
# FILE-LEVEL ANSI + CONTROL CLEAN
# ----------------------------------------------------------------------

def remove_ansi_and_control_from_file(path: str):
    with open(path, "r", encoding="utf-8") as f:
        raw = f.read()
    cleaned = _ANSI_RE.sub("", raw)
    cleaned = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]', '', cleaned)
    with open(path, "w", encoding="utf-8") as f:
        f.write(cleaned)
    print("Removed ANSI/control chars from:", path)

def strip_ansi(text: str) -> str:
    return _ANSI_RE.sub("", text) if text else text

# ----------------------------------------------------------------------
# Unescape double-single-quotes in wrapped SQL
# ----------------------------------------------------------------------

def unescape_wrapped_sql_content(s: str) -> str:
    if not s:
        return s
    out, i, n = [], 0, len(s)
    while i < n:
        # Skip -- line comments (quotes inside must not be interpreted)
        if s[i] == '-' and i + 1 < n and s[i + 1] == '-':
            while i < n and s[i] != '\n':
                out.append(s[i]); i += 1
            continue
        # Skip /* */ block comments
        if s[i] == '/' and i + 1 < n and s[i + 1] == '*':
            out.append(s[i]); i += 1   # /
            out.append(s[i]); i += 1   # *
            while i < n:
                if s[i] == '*' and i + 1 < n and s[i + 1] == '/':
                    out.append(s[i]); i += 1
                    out.append(s[i]); i += 1
                    break
                out.append(s[i]); i += 1
            continue
        if s[i] == "'":
            i += 1
            lit = []
            while i < n:
                if s[i] == "'":
                    if i+1 < n and s[i+1] == "'":
                        lit.append("'"); i += 2
                    else:
                        i += 1; break
                else:
                    lit.append(s[i]); i += 1
            out.append("'" + "".join(lit) + "'")
        else:
            out.append(s[i]); i += 1
    return "".join(out)

# ----------------------------------------------------------------------
# IDENTIFIER NORMALIZATION (no structural edits)
# ----------------------------------------------------------------------

def normalize_identifiers(sql: str) -> str:
    if not sql:
        return sql
    sql = sql.replace('""', '"')
    sql = strip_ansi(sql)
    return sql

# ----------------------------------------------------------------------
# STRICT PRE-PARSING ALIAS NORMALIZATION (fixes crash)
# ----------------------------------------------------------------------

def force_aliases_pre_parse(sql: str) -> str:
    def _clean(alias: str) -> str:
        alias_clean = re.sub(r"\s+", "_", alias.strip())
        alias_clean = re.sub(r"[^\w_]", "_", alias_clean)
        return alias_clean

    def repl_single(m):
        alias = m.group(1)
        return f"AS {_clean(alias)}"

    def repl_double(m):
        alias = m.group(1)
        return f'AS "{alias}"'

    sql = _AS_SINGLE_QUOTED_ALIAS.sub(repl_single, sql)
    sql = _AS_DOUBLE_QUOTED_ALIAS.sub(repl_double, sql)

    _MULTIWORD_UNQUOTED_ALIAS = re.compile(
        r"\bAS\s+((?:[A-Za-z_][A-Za-z0-9_]*)(?:\s+[A-Za-z_][A-Za-z0-9_]*)+?)"
        r"(?=\s*,|\s+FROM\b|\s*\)|\s*$)",
        re.IGNORECASE,
    )

    def repl_unquoted(m):
        alias = m.group(1).strip()
        parts = re.split(r"\s+", alias)
        if len(parts) < 2:
            return m.group(0)
        if any(p.lower() in _KEYWORDS_IN_UNQUOTED_ALIAS for p in parts):
            return m.group(0)
        if re.search(r"[\s\-]", alias):
            alias_clean = re.sub(r"\s+", "_", alias)
            alias_clean = re.sub(r"[^\w_]", "_", alias_clean)
            return f"AS {alias_clean}"
        return m.group(0)

    sql = _MULTIWORD_UNQUOTED_ALIAS.sub(repl_unquoted, sql)
    
    return sql

# ----------------------------------------------------------------------
# Ensures regexp_replace(expr, pattern) become regexp_replace(expr, pattern, '')
# ----------------------------------------------------------------------

def ensure_regexp_replacement(sql: str):
    return _REGEXP_REPLACE_2ARGS.sub(lambda m: f"{m.group(1)}({m.group(2)}, {m.group(3)}, '')", sql)

# ----------------------------------------------------------------------
# Convert TRIM(LEADING/TRAILING/BOTH ... FROM ...) to LTRIM/RTRIM/TRIM
# ----------------------------------------------------------------------

def convert_trim_syntax(sql: str) -> str:
    """
    Convert Presto TRIM syntax to Databricks-compatible syntax.
    TRIM(LEADING 'x' FROM col) -> LTRIM(col, 'x')
    TRIM(TRAILING 'x' FROM col) -> RTRIM(col, 'x')
    TRIM(BOTH 'x' FROM col) -> TRIM(col, 'x')
    TRIM('x' FROM col) -> TRIM(col, 'x')  # defaults to BOTH
    
    This function properly handles nested parentheses in the column expression.
    """
    
    def find_matching_paren(text, start_pos):
        depth = 1
        i = start_pos
        in_single_quote = False
        in_double_quote = False
        
        while i < len(text) and depth > 0:
            ch = text[i]
            
            if ch == "'" and not in_double_quote:
                if i > 0 and text[i-1] == '\\':
                    pass  
                else:
                    in_single_quote = not in_single_quote
            elif ch == '"' and not in_single_quote:
                if i > 0 and text[i-1] == '\\':
                    pass  
                else:
                    in_double_quote = not in_double_quote
            elif not in_single_quote and not in_double_quote:
                if ch == '(':
                    depth += 1
                elif ch == ')':
                    depth -= 1
            
            i += 1
        
        return i if depth == 0 else -1
    
    result = []
    i = 0
    
    while i < len(sql):
        trim_match = re.match(r'\bTRIM\s*\(', sql[i:], re.IGNORECASE)
        
        if trim_match:
            trim_start = i
            paren_start = i + trim_match.end() - 1 
            
            paren_end = find_matching_paren(sql, paren_start + 1)
            
            if paren_end == -1:
                result.append(sql[i])
                i += 1
                continue
            
            content = sql[paren_start + 1:paren_end - 1].strip()
            

            trim_type = None
            remaining = content.strip()
            
            trim_keyword_match = re.match(r'^\s*(LEADING|TRAILING|BOTH)\s+', remaining, re.IGNORECASE)
            if trim_keyword_match:
                trim_type = trim_keyword_match.group(1)
                remaining = remaining[trim_keyword_match.end():]
            

            quote_char = None
            trim_char = None
            
            if remaining.startswith("'"):
                quote_char = "'"
                pos = 1
                trim_chars = []
                while pos < len(remaining):
                    if remaining[pos] == "'":
                        if pos + 1 < len(remaining) and remaining[pos + 1] == "'":
                            trim_chars.append("'")
                            pos += 2
                        else:
                            trim_char = ''.join(trim_chars)
                            remaining = remaining[pos + 1:].strip()
                            break
                    else:
                        trim_chars.append(remaining[pos])
                        pos += 1
            elif remaining.startswith('"'):
                quote_char = '"'
                pos = 1
                trim_chars = []
                while pos < len(remaining):
                    if remaining[pos] == '"':
                        if pos + 1 < len(remaining) and remaining[pos + 1] == '"':
                            trim_chars.append('"')
                            pos += 2
                        else:
                            trim_char = ''.join(trim_chars)
                            remaining = remaining[pos + 1:].strip()
                            break
                    else:
                        trim_chars.append(remaining[pos])
                        pos += 1
            
            from_match = re.match(r'^\s*FROM\s+(.+)$', remaining, re.IGNORECASE | re.DOTALL)
            
            if quote_char and trim_char is not None and from_match:
                column_expr = from_match.group(1).strip()
                
                if trim_type:
                    trim_type_upper = trim_type.upper()
                    if trim_type_upper == 'LEADING':
                        func_name = 'LTRIM'
                    elif trim_type_upper == 'TRAILING':
                        func_name = 'RTRIM'
                    else:  # BOTH
                        func_name = 'TRIM'
                else:
                    func_name = 'TRIM'
                
                
                if quote_char == "'" and "'" in trim_char:
                    escaped_trim_char = trim_char.replace("'", "''")
                    replacement = f"{func_name}({column_expr}, '{escaped_trim_char}')"
                elif quote_char == '"' and '"' in trim_char:
                    escaped_trim_char = trim_char.replace('"', '""')
                    replacement = f'{func_name}({column_expr}, "{escaped_trim_char}")'
                else:
                    replacement = f"{func_name}({column_expr}, {quote_char}{trim_char}{quote_char})"
                
                result.append(replacement)
                i = paren_end
            else:
                result.append(sql[trim_start:paren_end])
                i = paren_end
        else:
            result.append(sql[i])
            i += 1
    
    return ''.join(result)

# ----------------------------------------------------------------------
# Small trailing repairs
# ----------------------------------------------------------------------

def repair_common_trailing_mistakes(sql: str) -> str:
    sql = re.sub(r',\s*\)', r", '')", sql)
    sql = sql.replace(", '') )", ", '')")
    return sql

# ----------------------------------------------------------------------
# AST fix for RegexpReplace missing expression
# ----------------------------------------------------------------------

def ast_fix_regexp_nodes(expr):
    def _fix(node):
        if isinstance(node, exp.RegexpReplace):
            if getattr(node, "expression", None) is None and getattr(node, "this", None) is not None:
                node.set("expression", node.this)
        return node
    return expr.transform(_fix)

# ----------------------------------------------------------------------
# Safe semicolon splitter (comment-aware)
# ----------------------------------------------------------------------

def safe_split_sql(text: str):
    queries, cur = [], []
    in_sq = in_dq = False
    in_line_comment = False
    in_block_comment = False
    i, n = 0, len(text)
    while i < n:
        ch = text[i]
        cur.append(ch)

        if in_line_comment:
            if ch == '\n':
                in_line_comment = False
            i += 1
            continue

        if in_block_comment:
            if ch == '*' and i + 1 < n and text[i + 1] == '/':
                cur.append('/'); i += 2
                in_block_comment = False
            else:
                i += 1
            continue

        if not in_sq and not in_dq:
            if ch == '-' and i + 1 < n and text[i + 1] == '-':
                cur.append('-'); i += 2
                in_line_comment = True
                continue
            if ch == '/' and i + 1 < n and text[i + 1] == '*':
                cur.append('*'); i += 2
                in_block_comment = True
                continue

        if ch == "'" and not in_dq:
            if i+1 < n and text[i+1] == "'":
                cur.append(text[i+1]); i += 1
            else:
                in_sq = not in_sq
        elif ch == '"' and not in_sq:
            in_dq = not in_dq
        elif ch == ";" and not in_sq and not in_dq:
            token = "".join(cur[:-1]).strip()
            if token:
                queries.append(token)
            cur = []
        i += 1
    if cur:
        token = "".join(cur).strip()
        if token:
            queries.append(token)
    return queries

# ----------------------------------------------------------------------
# SQL Validation — detect common syntax bugs before conversion
# ----------------------------------------------------------------------

_SQL_CLAUSE_KW = re.compile(
    r'\b(SELECT|FROM|WHERE|GROUP\s+BY|ORDER\s+BY|HAVING|UNION|JOIN|'
    r'LEFT\s+JOIN|RIGHT\s+JOIN|INNER\s+JOIN|OUTER\s+JOIN|CROSS\s+JOIN|'
    r'ON|INSERT|UPDATE|DELETE|CREATE|DROP|ALTER|INTO|VALUES|SET|'
    r'LIMIT|OFFSET)\b',
    re.IGNORECASE,
)


def validate_sql(sql: str) -> list:
    """
    Validate SQL for common syntax bugs *before* conversion.

    Returns a list of issue dicts::

        {'line': int, 'column': int, 'severity': str,
         'message': str, 'suggestion': str}

    ``severity`` is ``'ERROR'`` (blocks conversion) or ``'WARNING'``.
    An empty list means no issues detected.
    """
    issues: list = []
    if not sql or not sql.strip():
        return issues

    n = len(sql)

    def _pos_lc(pos):
        line = sql[:pos].count('\n') + 1
        last_nl = sql.rfind('\n', 0, pos)
        col = (pos - last_nl) if last_nl != -1 else pos + 1
        return line, col

    # ---- Pass 1: state-machine walk ----
    in_sq = in_dq = False
    in_line_comment = in_block_comment = False
    paren_depth = 0
    sq_start = dq_start = bc_start = 0
    paren_stack: list = []
    string_literals: list = []          # (start_pos, end_pos, content)
    cur_str_start = 0
    cur_str_chars: list = []

    i = 0
    while i < n:
        ch = sql[i]

        # --- line comment ---
        if in_line_comment:
            if ch == '\n':
                in_line_comment = False
            i += 1
            continue

        # --- block comment ---
        if in_block_comment:
            if ch == '*' and i + 1 < n and sql[i + 1] == '/':
                in_block_comment = False
                i += 2
            else:
                i += 1
            continue

        # --- comment start (outside strings) ---
        if not in_sq and not in_dq:
            if ch == '-' and i + 1 < n and sql[i + 1] == '-':
                in_line_comment = True
                i += 2
                continue
            if ch == '/' and i + 1 < n and sql[i + 1] == '*':
                in_block_comment = True
                bc_start = i
                i += 2
                continue

        # --- single quote ---
        if ch == "'" and not in_dq:
            if in_sq:
                if i + 1 < n and sql[i + 1] == "'":
                    cur_str_chars.append("'")
                    i += 2
                    continue
                content = "".join(cur_str_chars)
                string_literals.append((cur_str_start, i, content))
                in_sq = False
                i += 1
                continue
            in_sq = True
            sq_start = i
            cur_str_start = i
            cur_str_chars = []
            i += 1
            continue

        # --- double quote ---
        if ch == '"' and not in_sq:
            if in_dq:
                in_dq = False
            else:
                in_dq = True
                dq_start = i
            i += 1
            continue

        # --- collect string content ---
        if in_sq:
            cur_str_chars.append(ch)
            i += 1
            continue

        # --- parentheses ---
        if not in_sq and not in_dq:
            if ch == '(':
                paren_depth += 1
                paren_stack.append(i)
            elif ch == ')':
                if paren_depth > 0:
                    paren_depth -= 1
                    paren_stack.pop()
                else:
                    ln, co = _pos_lc(i)
                    issues.append({
                        'line': ln, 'column': co, 'severity': 'ERROR',
                        'message': (
                            f'Unmatched closing parenthesis `)` at line {ln}, '
                            f'column {co}.'
                        ),
                        'suggestion': (
                            'Remove this extra `)` or add a matching `(` '
                            'earlier in the query.'
                        ),
                    })
        i += 1

    # ---- end-of-input structural checks ----
    if in_sq:
        ln, co = _pos_lc(sq_start)
        issues.append({
            'line': ln, 'column': co, 'severity': 'ERROR',
            'message': (
                f'Unclosed single quote starting at line {ln}, column {co}. '
                f'The string literal is never terminated.'
            ),
            'suggestion': (
                "Add a closing `'` to terminate the string, or change to "
                "`''` (two single quotes) if you intended an empty string."
            ),
        })

    if in_dq:
        ln, co = _pos_lc(dq_start)
        issues.append({
            'line': ln, 'column': co, 'severity': 'ERROR',
            'message': (
                f'Unclosed double quote starting at line {ln}, column {co}. '
                f'The quoted identifier is never terminated.'
            ),
            'suggestion': 'Add a closing `"` to properly close the quoted identifier.',
        })

    if in_block_comment:
        ln, co = _pos_lc(bc_start)
        issues.append({
            'line': ln, 'column': co, 'severity': 'ERROR',
            'message': (
                f'Unclosed block comment `/*` starting at line {ln}, '
                f'column {co}.'
            ),
            'suggestion': 'Add `*/` to close the block comment.',
        })

    if paren_depth > 0:
        for pos in paren_stack:
            ln, co = _pos_lc(pos)
            issues.append({
                'line': ln, 'column': co, 'severity': 'ERROR',
                'message': (
                    f'Unmatched opening parenthesis `(` at line {ln}, '
                    f'column {co}.'
                ),
                'suggestion': 'Add a matching `)` or remove this extra `(`.',
            })

    # ---- Heuristic: suspicious string literals ----
    for s_pos, e_pos, content in string_literals:
        # High-confidence bug: string starts with whitespace + AS <ident>,
        # e.g. ' as clustername, ' — almost certainly a missing quote
        # where the user intended '' (empty string).
        if (content and content[0] in (' ', '\t')
                and re.match(r'\s+AS\s+\w+', content, re.IGNORECASE)):
            s_ln, s_co = _pos_lc(s_pos)
            issues.append({
                'line': s_ln, 'column': s_co, 'severity': 'ERROR',
                'message': (
                    f"String literal at line {s_ln}, column {s_co} "
                    f"begins with SQL alias syntax "
                    f"(e.g., ' AS column_name'). This almost certainly "
                    f"means a `'` should be `''` (empty string)."
                ),
                'suggestion': (
                    f"Change the `'` at line {s_ln}, column {s_co} "
                    f"to `''` (two single quotes for an empty string)."
                ),
            })
            continue

        # Multi-line string containing SQL clause keywords
        if '\n' in content:
            kw_hits = _SQL_CLAUSE_KW.findall(content)
            unique_kws = list(dict.fromkeys(
                k.strip().upper() for k in kw_hits
            ))
            if len(unique_kws) >= 2:
                s_ln, s_co = _pos_lc(s_pos)
                e_ln = sql[:e_pos].count('\n') + 1
                kw_display = ", ".join(unique_kws[:5])
                issues.append({
                    'line': s_ln, 'column': s_co, 'severity': 'ERROR',
                    'message': (
                        f"Suspicious string literal from line {s_ln} to "
                        f"line {e_ln} contains SQL keywords: {kw_display}."
                        f" This almost certainly means a `'` should be "
                        f"`''` (empty string)."
                    ),
                    'suggestion': (
                        f"Change the `'` at line {s_ln}, column {s_co} "
                        f"to `''` (two single quotes for an empty string)."
                    ),
                })

    # ---- Build code-only version ----
    # Strings/identifiers → '_' (non-whitespace) to prevent false
    # regex matches; comments → spaces.
    _Q = '_'   # placeholder for quoted content
    code_only: list = []
    in_sq = in_dq = in_line_comment = in_block_comment = False
    i = 0
    while i < n:
        ch = sql[i]
        if in_line_comment:
            code_only.append('\n' if ch == '\n' else ' ')
            if ch == '\n':
                in_line_comment = False
            i += 1
            continue
        if in_block_comment:
            code_only.append('\n' if ch == '\n' else ' ')
            if ch == '*' and i + 1 < n and sql[i + 1] == '/':
                code_only.append(' ')
                in_block_comment = False
                i += 2
            else:
                i += 1
            continue
        if not in_sq and not in_dq:
            if ch == '-' and i + 1 < n and sql[i + 1] == '-':
                code_only.append(' '); code_only.append(' ')
                in_line_comment = True
                i += 2
                continue
            if ch == '/' and i + 1 < n and sql[i + 1] == '*':
                code_only.append(' '); code_only.append(' ')
                in_block_comment = True
                i += 2
                continue
        if ch == "'" and not in_dq:
            code_only.append(_Q)
            if in_sq:
                if i + 1 < n and sql[i + 1] == "'":
                    code_only.append(_Q)
                    i += 2
                    continue
                in_sq = False
            else:
                in_sq = True
            i += 1
            continue
        if ch == '"' and not in_sq:
            code_only.append(_Q)
            in_dq = not in_dq
            i += 1
            continue
        if in_sq or in_dq:
            code_only.append('\n' if ch == '\n' else _Q)
        else:
            code_only.append(ch)
        i += 1

    code_str = "".join(code_only)

    # ---- Trailing comma before clause keyword ----
    for m in re.finditer(
        r',\s*\b(FROM|WHERE|GROUP\s+BY|ORDER\s+BY|HAVING|LIMIT|'
        r'UNION|EXCEPT|INTERSECT)\b',
        code_str, re.IGNORECASE,
    ):
        ln, co = _pos_lc(m.start())
        kw = m.group(1).strip().upper()
        issues.append({
            'line': ln, 'column': co, 'severity': 'ERROR',
            'message': (
                f'Trailing comma before `{kw}` at line {ln}, column {co}.'
            ),
            'suggestion': (
                f'Remove the extra comma before `{kw}`, or add the missing '
                f'column/expression after it.'
            ),
        })

    # ---- Double commas ----
    for m in re.finditer(r',\s*,', code_str):
        ln, co = _pos_lc(m.start())
        issues.append({
            'line': ln, 'column': co, 'severity': 'ERROR',
            'message': f'Double comma at line {ln}, column {co}.',
            'suggestion': (
                'Remove one comma or add the missing expression between them.'
            ),
        })

    # ---- Empty IN() clause ----
    for m in re.finditer(r'\bIN\s*\(\s*\)', code_str, re.IGNORECASE):
        ln, co = _pos_lc(m.start())
        issues.append({
            'line': ln, 'column': co, 'severity': 'WARNING',
            'message': f'Empty `IN()` clause at line {ln}, column {co}.',
            'suggestion': 'Add values inside `IN(...)` or remove the clause.',
        })

    # ---- Adjacent string literals without separator ----
    for j in range(len(string_literals) - 1):
        _, end1, _ = string_literals[j]
        start2, _, _ = string_literals[j + 1]
        between = code_str[end1 + 1:start2]
        if between.replace(_Q, '').strip() == '':
            ln, co = _pos_lc(end1 + 1)
            issues.append({
                'line': ln, 'column': co, 'severity': 'ERROR',
                'message': (
                    f'Adjacent string literals without a separator (e.g., '
                    f'missing comma) near line {ln}, column {co}.'
                ),
                'suggestion': (
                    'Add a comma `,` between the two string literals, '
                    'or merge them into one.'
                ),
            })

    # ---- = NULL / != NULL / <> NULL ----
    for m in re.finditer(r'(?<![!<>=])=\s*NULL\b', code_str, re.IGNORECASE):
        ln, co = _pos_lc(m.start())
        issues.append({
            'line': ln, 'column': co, 'severity': 'WARNING',
            'message': (
                f'`= NULL` at line {ln}, column {co}. '
                f'In SQL, use `IS NULL` instead; `= NULL` always yields NULL.'
            ),
            'suggestion': 'Change `= NULL` to `IS NULL`.',
        })

    for m in re.finditer(r'(?:!=|<>)\s*NULL\b', code_str, re.IGNORECASE):
        ln, co = _pos_lc(m.start())
        issues.append({
            'line': ln, 'column': co, 'severity': 'WARNING',
            'message': (
                f'`!= NULL` or `<> NULL` at line {ln}, column {co}. '
                f'Use `IS NOT NULL` instead.'
            ),
            'suggestion': 'Change to `IS NOT NULL`.',
        })

    return issues


def format_validation_issues(issues: list) -> str:
    """Format validation issues into a readable multi-line string."""
    if not issues:
        return ""

    errors = [i for i in issues if i['severity'] == 'ERROR']
    warnings = [i for i in issues if i['severity'] == 'WARNING']

    lines: list = []
    lines.append(
        f"SQL Validation: {len(errors)} error(s), "
        f"{len(warnings)} warning(s)\n"
    )

    for idx, iss in enumerate(errors + warnings, 1):
        tag = "ERROR" if iss['severity'] == 'ERROR' else "WARNING"
        lines.append(
            f"  {idx}. [{tag}] Line {iss['line']}, Col {iss['column']}"
        )
        lines.append(f"     {iss['message']}")
        lines.append(f"     -> Fix: {iss['suggestion']}")
        lines.append("")

    if errors:
        lines.append("Please fix the ERROR(s) above before conversion.")

    return "\n".join(lines)


# ----------------------------------------------------------------------
# Balance stray single quotes
# ----------------------------------------------------------------------

def _count_code_single_quotes(sql: str) -> int:
    """Count single quotes that are in actual SQL code, ignoring those inside comments."""
    count = 0
    i, n = 0, len(sql)
    while i < n:
        if sql[i] == '-' and i + 1 < n and sql[i + 1] == '-':
            while i < n and sql[i] != '\n':
                i += 1
            continue
        if sql[i] == '/' and i + 1 < n and sql[i + 1] == '*':
            i += 2
            while i < n:
                if sql[i] == '*' and i + 1 < n and sql[i + 1] == '/':
                    i += 2
                    break
                i += 1
            continue
        if sql[i] == "'":
            count += 1
        i += 1
    return count


def balance_single_quotes(sql: str):
    if _count_code_single_quotes(sql) % 2 == 1:
        sql = sql.rstrip()
        if sql.endswith("'"):
            sql = sql[:-1]
        else:
            sql = sql + "'"
    return sql

# --- helper: semantic AST equality using sqlglot
def is_semantically_same(original_sql: str, converted_sql: str) -> bool:
    try:
        ast_original = sqlglot.parse_one(original_sql, read="presto")
        ast_converted = sqlglot.parse_one(converted_sql, read="databricks")
        return ast_original == ast_converted
    except Exception:
        return False

# --- helper: extract inner names of double-quoted and backtick identifiers
def quoted_identifier_set(sql: str):
    """
    Returns a dictionary mapping identifier names to their quote styles.
    This allows us to detect when quote styles change (e.g., "alias" -> `alias`).
    
    Returns:
        dict: {identifier_name: quote_char} where quote_char is '"' or '`'
    """
    if not sql:
        return {}
    
    result = {}
    for m in re.finditer(r'"([^"]+)"', sql):
        result[m.group(1)] = '"'
    
    for m in re.finditer(r'`([^`]+)`', sql):
        result[m.group(1)] = '`'
    
    return result

# ----------------------------------------------------------------------
# EXECUTE ... USING extractor (unwraps embedded SQL)
# ----------------------------------------------------------------------

def find_quoted_content(text: str, start_idx: int = 0, quote_char: str = "'"):
    n = len(text)
    i = start_idx
    while i < n and text[i] != quote_char:
        i += 1
    if i >= n:
        return None, -1
    i += 1
    parts = []
    while i < n:
        if text[i] == quote_char:
            if i + 1 < n and text[i+1] == quote_char:
                parts.append(quote_char)
                i += 2
            else:
                return "".join(parts), i + 1
        else:
            parts.append(text[i]); i += 1
    return None, -1