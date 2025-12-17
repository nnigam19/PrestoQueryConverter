"""
Presto-specific function conversions to Databricks equivalents.

This module handles Presto functions that sqlglot doesn't automatically convert.
"""

import re


def find_matching_paren(text: str, start_pos: int) -> int:
    """
    Find the position of the matching closing parenthesis.
    Handles nested parentheses and respects quoted strings.
    
    Args:
        text: The text to search in
        start_pos: Position after the opening parenthesis
    
    Returns:
        Position of the matching closing parenthesis, or -1 if not found
    """
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


def convert_presto_functions(sql: str) -> str:
    """
    Convert Presto-specific functions that sqlglot doesn't handle automatically
    to their Databricks equivalents.
    
    Handles:
    - AT_TIMEZONE(timestamp, timezone) -> FROM_UTC_TIMESTAMP(timestamp, timezone)
    - DATE_PARSE(string, format) -> TO_TIMESTAMP(string, format)
    - FROM_ISO8601_TIMESTAMP(string) -> TO_TIMESTAMP(string)
    - TO_UNIXTIME(timestamp) -> UNIX_TIMESTAMP(timestamp)
    - NOW() -> CURRENT_TIMESTAMP()
    - DATE_ADD(unit, value, date) -> DATEADD(unit, value, date)
    - CARDINALITY(array) -> SIZE(array)
    - FORMAT_DATETIME(timestamp, format) -> DATE_FORMAT(timestamp, format)
    - ARBITRARY(x) -> FIRST(x, true) [ignoring nulls]
    - REGEXP_LIKE(str, pattern) -> RLIKE(str, pattern)
    
    Note: The following are handled correctly by sqlglot and don't need pre-processing:
    - DATE_DIFF, STRPOS, SUBSTR, ELEMENT_AT, CONCAT_WS, FROM_UNIXTIME
    """
    if not sql:
        return sql
    
    # 1. Convert AT_TIMEZONE to FROM_UTC_TIMESTAMP
    sql = _convert_at_timezone(sql)
    
    # 2. Convert DATE_PARSE to TO_TIMESTAMP
    sql = re.sub(
        r'\bDATE_PARSE\s*\(',
        'TO_TIMESTAMP(',
        sql,
        flags=re.IGNORECASE
    )
    
    # 3. Convert FROM_ISO8601_TIMESTAMP to TO_TIMESTAMP
    sql = _convert_from_iso8601_timestamp(sql)
    
    # 4. Convert TO_UNIXTIME to UNIX_TIMESTAMP
    sql = re.sub(
        r'\bTO_UNIXTIME\s*\(',
        'UNIX_TIMESTAMP(',
        sql,
        flags=re.IGNORECASE
    )
    
    # 5. Convert NOW() to CURRENT_TIMESTAMP()
    sql = re.sub(
        r'\bNOW\s*\(\s*\)',
        'CURRENT_TIMESTAMP()',
        sql,
        flags=re.IGNORECASE
    )
    
    # 6. Convert DATE_ADD with Presto syntax to DATEADD with Databricks syntax
    sql = _convert_date_add(sql)
    
    # Note: DATE_DIFF, STRPOS, and SUBSTR are handled correctly by sqlglot
    # and don't need pre-processing conversion
    
    # 10. Convert CARDINALITY to SIZE
    sql = re.sub(
        r'\bCARDINALITY\s*\(',
        'SIZE(',
        sql,
        flags=re.IGNORECASE
    )
    
    # 11. Convert FORMAT_DATETIME to DATE_FORMAT
    sql = re.sub(
        r'\bFORMAT_DATETIME\s*\(',
        'DATE_FORMAT(',
        sql,
        flags=re.IGNORECASE
    )
    
    # 12. Convert ARBITRARY to FIRST with ignore nulls
    sql = _convert_arbitrary(sql)
    
    # 13. Convert ARRAY_SORT to ARRAY_SORT (Databricks uses different syntax for comparator)
    # Note: This is complex and may need manual review if custom comparators are used
    
    # 14. Convert REGEXP_LIKE to RLIKE (simpler alternative)
    sql = re.sub(
        r'\bREGEXP_LIKE\s*\(',
        'RLIKE(',
        sql,
        flags=re.IGNORECASE
    )
    
    return sql


def _convert_at_timezone(sql: str) -> str:
    """
    Convert AT_TIMEZONE(timestamp, timezone) to FROM_UTC_TIMESTAMP(timestamp, timezone).
    """
    result = []
    i = 0
    n = len(sql)
    
    while i < n:
        match = re.match(r'AT_TIMEZONE\s*\(', sql[i:], re.IGNORECASE)
        if match:
            func_start = i
            paren_start = i + match.end() - 1
            paren_end = find_matching_paren(sql, paren_start + 1)
            
            if paren_end != -1:
                args_str = sql[paren_start + 1:paren_end - 1]
                replacement = f"FROM_UTC_TIMESTAMP({args_str})"
                result.append(replacement)
                i = paren_end
            else:
                result.append(sql[i])
                i += 1
        else:
            result.append(sql[i])
            i += 1
    
    return ''.join(result)


def _convert_from_iso8601_timestamp(sql: str) -> str:
    """
    Convert FROM_ISO8601_TIMESTAMP(string) to TO_TIMESTAMP(string).
    """
    result = []
    i = 0
    n = len(sql)
    
    while i < n:
        match = re.match(r'FROM_ISO8601_TIMESTAMP\s*\(', sql[i:], re.IGNORECASE)
        if match:
            func_start = i
            paren_start = i + match.end() - 1
            paren_end = find_matching_paren(sql, paren_start + 1)
            
            if paren_end != -1:
                arg_str = sql[paren_start + 1:paren_end - 1].strip()
                replacement = f"TO_TIMESTAMP({arg_str})"
                result.append(replacement)
                i = paren_end
            else:
                result.append(sql[i])
                i += 1
        else:
            result.append(sql[i])
            i += 1
    
    return ''.join(result)


def _convert_date_add(sql: str) -> str:
    """
    Convert Presto DATE_ADD('unit', value, date) to Databricks DATEADD(unit, value, date).
    """
    result = []
    i = 0
    n = len(sql)
    
    while i < n:
        match = re.match(r'DATE_ADD\s*\(', sql[i:], re.IGNORECASE)
        if match:
            func_start = i
            paren_start = i + match.end() - 1
            paren_end = find_matching_paren(sql, paren_start + 1)
            
            if paren_end != -1:
                args_str = sql[paren_start + 1:paren_end - 1].strip()
                args = _parse_function_args(args_str)
                
                if len(args) == 3:
                    unit = args[0].strip().strip("'\"")
                    value = args[1].strip()
                    date_expr = args[2].strip()
                    replacement = f"DATEADD({unit}, {value}, {date_expr})"
                    result.append(replacement)
                    i = paren_end
                else:
                    result.append(sql[func_start:paren_end])
                    i = paren_end
            else:
                result.append(sql[i])
                i += 1
        else:
            result.append(sql[i])
            i += 1
    
    return ''.join(result)


def _convert_date_diff(sql: str) -> str:
    """
    Convert Presto DATE_DIFF('unit', start, end) to Databricks DATEDIFF(unit, start, end).
    Note: Argument order is the same, just remove quotes from unit.
    """
    result = []
    i = 0
    n = len(sql)
    
    while i < n:
        match = re.match(r'DATE_DIFF\s*\(', sql[i:], re.IGNORECASE)
        if match:
            func_start = i
            paren_start = i + match.end() - 1
            paren_end = find_matching_paren(sql, paren_start + 1)
            
            if paren_end != -1:
                args_str = sql[paren_start + 1:paren_end - 1].strip()
                args = _parse_function_args(args_str)
                
                if len(args) == 3:
                    unit = args[0].strip().strip("'\"")
                    start_date = args[1].strip()
                    end_date = args[2].strip()
                    replacement = f"DATEDIFF({unit}, {start_date}, {end_date})"
                    result.append(replacement)
                    i = paren_end
                else:
                    result.append(sql[func_start:paren_end])
                    i = paren_end
            else:
                result.append(sql[i])
                i += 1
        else:
            result.append(sql[i])
            i += 1
    
    return ''.join(result)


def _convert_strpos(sql: str) -> str:
    """
    Convert Presto STRPOS(string, substring) to Databricks LOCATE(substring, string).
    Note: Argument order is REVERSED!
    Presto: STRPOS(string, substring) - returns position of substring in string
    Databricks: LOCATE(substring, string) - returns position of substring in string
    """
    result = []
    i = 0
    n = len(sql)
    
    while i < n:
        match = re.match(r'STRPOS\s*\(', sql[i:], re.IGNORECASE)
        if match:
            func_start = i
            paren_start = i + match.end() - 1
            paren_end = find_matching_paren(sql, paren_start + 1)
            
            if paren_end != -1:
                args_str = sql[paren_start + 1:paren_end - 1].strip()
                args = _parse_function_args(args_str)
                
                if len(args) == 2:
                    string_expr = args[0].strip()
                    substring_expr = args[1].strip()
                    # Reverse the argument order for LOCATE
                    replacement = f"LOCATE({substring_expr}, {string_expr})"
                    result.append(replacement)
                    i = paren_end
                elif len(args) == 3:
                    # STRPOS with 3 args: STRPOS(string, substring, start_pos)
                    # LOCATE with 3 args: LOCATE(substring, string, start_pos)
                    string_expr = args[0].strip()
                    substring_expr = args[1].strip()
                    start_pos = args[2].strip()
                    replacement = f"LOCATE({substring_expr}, {string_expr}, {start_pos})"
                    result.append(replacement)
                    i = paren_end
                else:
                    result.append(sql[func_start:paren_end])
                    i = paren_end
            else:
                result.append(sql[i])
                i += 1
        else:
            result.append(sql[i])
            i += 1
    
    return ''.join(result)


def _convert_arbitrary(sql: str) -> str:
    """
    Convert Presto ARBITRARY(x) to Databricks FIRST(x, true).
    ARBITRARY returns an arbitrary non-null value, similar to FIRST with ignore nulls.
    """
    result = []
    i = 0
    n = len(sql)
    
    while i < n:
        match = re.match(r'ARBITRARY\s*\(', sql[i:], re.IGNORECASE)
        if match:
            func_start = i
            paren_start = i + match.end() - 1
            paren_end = find_matching_paren(sql, paren_start + 1)
            
            if paren_end != -1:
                args_str = sql[paren_start + 1:paren_end - 1].strip()
                # ARBITRARY takes one argument, convert to FIRST(arg, true)
                replacement = f"FIRST({args_str}, true)"
                result.append(replacement)
                i = paren_end
            else:
                result.append(sql[i])
                i += 1
        else:
            result.append(sql[i])
            i += 1
    
    return ''.join(result)


def _parse_function_args(args_str: str) -> list:
    """
    Parse comma-separated function arguments, respecting nested parentheses and quotes.
    """
    args = []
    current_arg = []
    depth = 0
    in_single_quote = False
    in_double_quote = False
    i = 0
    n = len(args_str)
    
    while i < n:
        char = args_str[i]
        
        if char == "'" and not in_double_quote:
            if i + 1 < n and args_str[i + 1] == "'":
                current_arg.append("''")
                i += 2
                continue
            else:
                in_single_quote = not in_single_quote
                current_arg.append(char)
                i += 1
                continue
        
        if char == '"' and not in_single_quote:
            if i + 1 < n and args_str[i + 1] == '"':
                current_arg.append('""')
                i += 2
                continue
            else:
                in_double_quote = not in_double_quote
                current_arg.append(char)
                i += 1
                continue
        
        if not in_single_quote and not in_double_quote:
            if char == '(':
                depth += 1
            elif char == ')':
                depth -= 1
            elif char == ',' and depth == 0:
                args.append(''.join(current_arg))
                current_arg = []
                i += 1
                continue
        
        current_arg.append(char)
        i += 1
    
    if current_arg:
        args.append(''.join(current_arg))
    
    return args

