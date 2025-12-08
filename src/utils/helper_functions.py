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
    sql = _DOUBLE_QUAL_QUOTE.sub(lambda m: f"{m.group(1)}.{m.group(2).replace('/', '_')}", sql)
    sql = _QUOTED_IDENT.sub(lambda m: m.group(1).replace("/", "_"), sql)
    sql = _BACKTICK_IDENT.sub(lambda m: m.group(1).replace("/", "_"), sql)
    return sql

# ----------------------------------------------------------------------
# STRICT PRE-PARSING ALIAS NORMALIZATION (fixes crash)
# ----------------------------------------------------------------------

def force_aliases_pre_parse(sql: str) -> str:
    def repl(m):
        alias = m.group(1).strip()
        alias_clean = re.sub(r"\s+", "_", alias)
        alias_clean = re.sub(r"[^\w_]", "_", alias_clean)
        return f"AS {alias_clean}"
    sql = _AS_DOUBLE_QUOTED_ALIAS.sub(repl, sql)
    sql = _AS_SINGLE_QUOTED_ALIAS.sub(repl, sql)
    return sql

# ----------------------------------------------------------------------
# Ensures regexp_replace(expr, pattern) become regexp_replace(expr, pattern, '')
# ----------------------------------------------------------------------

def ensure_regexp_replacement(sql: str):
    return _REGEXP_REPLACE_2ARGS.sub(lambda m: f"{m.group(1)}({m.group(2)}, {m.group(3)}, '')", sql)

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
    if not sql:
        return set()
    dq = {m.group(1) for m in re.finditer(r'"([^"]+)"', sql)}
    bt = {m.group(1) for m in re.finditer(r'`([^`]+)`', sql)}
    return dq.union(bt)

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