"""
Presto-specific function conversions to Databricks equivalents.

This module handles Presto functions that sqlglot doesn't automatically convert.
"""

import re


def convert_date_format_pattern(pattern: str) -> str:
    """
    Convert legacy date format patterns (Spark 2.x / Presto style with % prefix)
    to Spark 3.0+ Java DateTimeFormatter patterns.
    
    Args:
        pattern: Date format pattern string (e.g., '%Y-%m-%d' or 'yyyy-MM-dd')
    
    Returns:
        Converted pattern compatible with Spark 3.0+ (e.g., 'yyyy-MM-dd')
    
    Examples:
        '%Y-%m-%d' -> 'yyyy-MM-dd'
        '%m/%d/%Y' -> 'MM/dd/yyyy'
        '%Y-%m-%d %H:%i:%s' -> 'yyyy-MM-dd HH:mm:ss'
    """
    if '%' not in pattern:
        return pattern
    
    # Mapping from legacy patterns to Spark 3.0+ patterns
    conversions = [
        ('%Y', 'yyyy'),      # 4-digit year
        ('%y', 'yy'),        # 2-digit year
        ('%m', 'MM'),        # 2-digit month
        ('%d', 'dd'),        # 2-digit day
        ('%H', 'HH'),        # 2-digit hour (24-hour)
        ('%h', 'hh'),        # 2-digit hour (12-hour)
        ('%i', 'mm'),        # 2-digit minute (Presto uses %i)
        ('%M', 'mm'),        # 2-digit minute (some systems use %M)
        ('%s', 'ss'),        # 2-digit second
        ('%S', 'ss'),        # 2-digit second (alternative)
        ('%p', 'a'),         # AM/PM marker
        ('%W', 'EEEE'),      # Full weekday name
        ('%w', 'e'),         # Day of week (1-7)
        ('%b', 'MMM'),       # Abbreviated month name
        ('%B', 'MMMM'),      # Full month name
        ('%j', 'DDD'),       # Day of year
    ]
    
    result = pattern
    for old, new in conversions:
        result = result.replace(old, new)
    
    return result


def find_matching_paren(text: str, start_pos: int) -> int:
    """
    Find the position of the matching closing parenthesis.
    Handles nested parentheses and respects quoted strings.
    Uses SQL's doubled-quote escaping ('' or "") not backslash escaping.
    
    Args:
        text: The text to search in
        start_pos: Position after the opening parenthesis
    
    Returns:
        Position of the matching closing parenthesis (the position of ')'), or -1 if not found
    """
    depth = 1
    i = start_pos
    in_single_quote = False
    in_double_quote = False
    
    while i < len(text) and depth > 0:
        ch = text[i]
        
        if ch == "'" and not in_double_quote:
            if in_single_quote and i + 1 < len(text) and text[i + 1] == "'":
                i += 2
                continue
            else:
                in_single_quote = not in_single_quote
        elif ch == '"' and not in_single_quote:
            if in_double_quote and i + 1 < len(text) and text[i + 1] == '"':
                i += 2
                continue
            else:
                in_double_quote = not in_double_quote
        elif not in_single_quote and not in_double_quote:
            if ch == '(':
                depth += 1
            elif ch == ')':
                depth -= 1
                if depth == 0:
                    return i  
        
        i += 1
    
    return -1 


def _replace_outside_strings(sql: str, pattern: str, replacement: str, flags=0) -> str:
    """
    Replace pattern with replacement, but only outside of string literals.
    Respects both single-quoted and double-quoted strings with SQL escaped quotes.
    
    Args:
        sql: The SQL string to process
        pattern: Regex pattern to match
        replacement: Replacement string
        flags: Regex flags (e.g., re.IGNORECASE)
    
    Returns:
        SQL with replacements applied only outside string literals
    """
    result = []
    i = 0
    n = len(sql)
    compiled_pattern = re.compile(pattern, flags)
    
    while i < n:
        if sql[i] in ('"', "'"):
            quote_char = sql[i]
            result.append(quote_char)
            i += 1
            
            while i < n:
                if sql[i] == quote_char:
                    if i + 1 < n and sql[i + 1] == quote_char:
                        result.append(quote_char)
                        result.append(quote_char)
                        i += 2
                    else:
                        result.append(quote_char)
                        i += 1
                        break
                else:
                    result.append(sql[i])
                    i += 1
        else:
            match = compiled_pattern.match(sql[i:])
            if match:
                result.append(replacement)
                i += len(match.group(0))
            else:
                result.append(sql[i])
                i += 1
    
    return ''.join(result)


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
    - Legacy date format patterns (%Y-%m-%d) -> Spark 3.0+ patterns (yyyy-MM-dd)
    
    Note: The following are handled correctly by sqlglot and don't need pre-processing:
    - DATE_DIFF, STRPOS, SUBSTR, ELEMENT_AT, CONCAT_WS, FROM_UNIXTIME
    
    All conversions respect string literal boundaries and only apply to actual SQL code.
    """
    if not sql:
        return sql
    
    sql = _convert_date_format_patterns_in_sql(sql)
    
    sql = _convert_at_timezone(sql)
    
    sql = _replace_outside_strings(
        sql,
        r'\bDATE_PARSE\s*\(',
        'TO_TIMESTAMP(',
        re.IGNORECASE
    )
    
    sql = _convert_from_iso8601_timestamp(sql)
    
    sql = _replace_outside_strings(
        sql,
        r'\bTO_UNIXTIME\s*\(',
        'UNIX_TIMESTAMP(',
        re.IGNORECASE
    )
    
    sql = _replace_outside_strings(
        sql,
        r'\bNOW\s*\(\s*\)',
        'CURRENT_TIMESTAMP()',
        re.IGNORECASE
    )
    
    sql = _convert_date_add(sql)
    
    sql = _replace_outside_strings(
        sql,
        r'\bCARDINALITY\s*\(',
        'SIZE(',
        re.IGNORECASE
    )
    
    sql = _replace_outside_strings(
        sql,
        r'\bFORMAT_DATETIME\s*\(',
        'DATE_FORMAT(',
        re.IGNORECASE
    )
    
    sql = _convert_arbitrary(sql)
    
    sql = _replace_outside_strings(
        sql,
        r'\bREGEXP_LIKE\s*\(',
        'RLIKE(',
        re.IGNORECASE
    )
    
    return sql


def _convert_date_format_patterns_in_sql(sql: str) -> str:
    """
    Find and convert legacy date format patterns in SQL functions like:
    - TO_TIMESTAMP(col, '%Y-%m-%d')
    - DATE_FORMAT(col, '%Y-%m-%d')
    - DATE_PARSE(col, '%Y-%m-%d')
    - TO_DATE(col, '%Y-%m-%d')
    
    Converts them to Spark 3.0+ compatible patterns.
    Respects string literal boundaries - patterns inside strings are not converted.
    """
    date_functions = [
        'TO_TIMESTAMP',
        'DATE_FORMAT',
        'DATE_PARSE',
        'TO_DATE',
        'FROM_UNIXTIME',
        'UNIX_TIMESTAMP',
    ]
    
    result = []
    i = 0
    n = len(sql)
    
    while i < n:
        if sql[i] in ('"', "'"):
            quote_char = sql[i]
            result.append(quote_char)
            i += 1
            
            while i < n:
                if sql[i] == quote_char:
                    if i + 1 < n and sql[i + 1] == quote_char:
                        result.append(quote_char)
                        result.append(quote_char)
                        i += 2
                    else:
                        # End of string
                        result.append(quote_char)
                        i += 1
                        break
                else:
                    result.append(sql[i])
                    i += 1
            continue
        
        matched_func = None
        for func in date_functions:
            pattern = re.compile(rf'\b{func}\s*\(', re.IGNORECASE)
            match = pattern.match(sql[i:])
            if match:
                matched_func = func
                break
        
        if matched_func:
            func_start = i
            paren_start = i + len(match.group(0)) - 1
            paren_end = find_matching_paren(sql, paren_start + 1)
            
            if paren_end != -1:
                args_str = sql[paren_start + 1:paren_end]
                
                converted_args = _convert_format_in_args(args_str)
                
                result.append(sql[func_start:paren_start + 1])
                result.append(converted_args)
                result.append(')')
                i = paren_end + 1  
            else:
                result.append(sql[i])
                i += 1
        else:
            result.append(sql[i])
            i += 1
    
    return ''.join(result)


def _convert_format_in_args(args_str: str) -> str:
    """
    Convert date format patterns within function arguments.
    Handles quoted strings that contain % patterns.
    Uses SQL's doubled-quote escaping ('' or "") not backslash escaping.
    """
    result = []
    i = 0
    n = len(args_str)
    
    while i < n:
        if args_str[i] in ('"', "'"):
            quote_char = args_str[i]
            result.append(quote_char)  
            i += 1
            
            string_content = []
            while i < n:
                if args_str[i] == quote_char:
                    if i + 1 < n and args_str[i + 1] == quote_char:
                        string_content.append(quote_char)
                        string_content.append(quote_char)
                        i += 2
                    else:
                        break
                else:
                    string_content.append(args_str[i])
                    i += 1
            
            content = ''.join(string_content)
            if '%' in content:
                converted_content = convert_date_format_pattern(content)
                result.append(converted_content)
            else:
                result.append(content)
            
            if i < n and args_str[i] == quote_char:
                result.append(quote_char)
                i += 1
        else:
            result.append(args_str[i])
            i += 1
    
    return ''.join(result)


def _convert_at_timezone(sql: str) -> str:
    """
    Convert AT_TIMEZONE(timestamp, timezone) to FROM_UTC_TIMESTAMP(timestamp, timezone).
    Only converts outside of string literals.
    """
    result = []
    i = 0
    n = len(sql)
    
    while i < n:
        if sql[i] in ('"', "'"):
            quote_char = sql[i]
            result.append(quote_char)
            i += 1
            
            while i < n:
                if sql[i] == quote_char:
                    if i + 1 < n and sql[i + 1] == quote_char:
                        result.append(quote_char)
                        result.append(quote_char)
                        i += 2
                    else:
                        result.append(quote_char)
                        i += 1
                        break
                else:
                    result.append(sql[i])
                    i += 1
        else:
            match = re.match(r'AT_TIMEZONE\s*\(', sql[i:], re.IGNORECASE)
            if match:
                func_start = i
                paren_start = i + match.end() - 1
                paren_end = find_matching_paren(sql, paren_start + 1)
                
                if paren_end != -1:
                    args_str = sql[paren_start + 1:paren_end]  
                    replacement = f"FROM_UTC_TIMESTAMP({args_str})"
                    result.append(replacement)
                    i = paren_end + 1  
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
    Only converts outside of string literals.
    """
    result = []
    i = 0
    n = len(sql)
    
    while i < n:
        if sql[i] in ('"', "'"):
            quote_char = sql[i]
            result.append(quote_char)
            i += 1
            
            while i < n:
                if sql[i] == quote_char:
                    if i + 1 < n and sql[i + 1] == quote_char:
                        result.append(quote_char)
                        result.append(quote_char)
                        i += 2
                    else:
                        result.append(quote_char)
                        i += 1
                        break
                else:
                    result.append(sql[i])
                    i += 1
        else:
            match = re.match(r'FROM_ISO8601_TIMESTAMP\s*\(', sql[i:], re.IGNORECASE)
            if match:
                func_start = i
                paren_start = i + match.end() - 1
                paren_end = find_matching_paren(sql, paren_start + 1)
                
                if paren_end != -1:
                    arg_str = sql[paren_start + 1:paren_end].strip()  
                    replacement = f"TO_TIMESTAMP({arg_str})"
                    result.append(replacement)
                    i = paren_end + 1 
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
    Only converts outside of string literals.
    """
    result = []
    i = 0
    n = len(sql)
    
    while i < n:
        if sql[i] in ('"', "'"):
            quote_char = sql[i]
            result.append(quote_char)
            i += 1
            
            while i < n:
                if sql[i] == quote_char:
                    if i + 1 < n and sql[i + 1] == quote_char:
                        result.append(quote_char)
                        result.append(quote_char)
                        i += 2
                    else:
                        result.append(quote_char)
                        i += 1
                        break
                else:
                    result.append(sql[i])
                    i += 1
        else:
            match = re.match(r'DATE_ADD\s*\(', sql[i:], re.IGNORECASE)
            if match:
                func_start = i
                paren_start = i + match.end() - 1
                paren_end = find_matching_paren(sql, paren_start + 1)
                
                if paren_end != -1:
                    args_str = sql[paren_start + 1:paren_end].strip()  
                    args = _parse_function_args(args_str)
                    
                    if len(args) == 3:
                        unit = args[0].strip().strip("'\"")
                        value = args[1].strip()
                        date_expr = args[2].strip()
                        replacement = f"DATEADD({unit}, {value}, {date_expr})"
                        result.append(replacement)
                        i = paren_end + 1  
                    else:
                        result.append(sql[func_start:paren_end + 1])
                        i = paren_end + 1
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
                    replacement = f"LOCATE({substring_expr}, {string_expr})"
                    result.append(replacement)
                    i = paren_end
                elif len(args) == 3:
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
    Only converts outside of string literals.
    """
    result = []
    i = 0
    n = len(sql)
    
    while i < n:
        if sql[i] in ('"', "'"):
            quote_char = sql[i]
            result.append(quote_char)
            i += 1
            
            while i < n:
                if sql[i] == quote_char:
                    if i + 1 < n and sql[i + 1] == quote_char:
                        result.append(quote_char)
                        result.append(quote_char)
                        i += 2
                    else:
                        result.append(quote_char)
                        i += 1
                        break
                else:
                    result.append(sql[i])
                    i += 1
        else:
            match = re.match(r'ARBITRARY\s*\(', sql[i:], re.IGNORECASE)
            if match:
                func_start = i
                paren_start = i + match.end() - 1
                paren_end = find_matching_paren(sql, paren_start + 1)
                
                if paren_end != -1:
                    args_str = sql[paren_start + 1:paren_end].strip() 
                    replacement = f"FIRST({args_str}, TRUE)"
                    result.append(replacement)
                    i = paren_end + 1  
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

