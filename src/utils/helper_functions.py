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
    
    unquoted_alias_pattern = re.compile(r'\bAS\s+([A-Za-z_][A-Za-z0-9_\s]+?)(?=\s*,\s*[A-Za-z_]|\s+FROM|\s*$)', re.IGNORECASE)
    def repl_unquoted(m):
        alias = m.group(1).strip()
        if re.search(r'[\s\-]', alias):
            alias_clean = re.sub(r"\s+", "_", alias)
            alias_clean = re.sub(r"[^\w_]", "_", alias_clean)
            return f"AS {alias_clean}"
        return m.group(0)  
    
    sql = unquoted_alias_pattern.sub(repl_unquoted, sql)
    
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
# Safe semicolon splitter
# ----------------------------------------------------------------------

def safe_split_sql(text: str):
    queries, cur = [], []
    in_sq = in_dq = False
    i, n = 0, len(text)
    while i < n:
        ch = text[i]
        cur.append(ch)
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
# Balance stray single quotes
# ----------------------------------------------------------------------

def balance_single_quotes(sql: str):
    if sql.count("'") % 2 == 1:
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