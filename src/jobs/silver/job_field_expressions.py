"""DuckDB SQL expression builders for silver job fields."""


def build_keyword_list_filter_sql(keyword_list, text_expression: str) -> str:
    """
    Build a DuckDB list_filter expression that matches canonical keywords against text.

    The matcher keeps the original keyword as the output value while matching both:
    - boundary-aware regex form, allowing separators between multi-word terms
    - compact form, removing separators to catch variants like front-end/frontend or node.js/nodejs
    """
    return f"""
                list_filter(
                    {keyword_list},
                    x ->
                        regexp_matches(
                            lower(COALESCE({text_expression}, '')),
                            '(^|[^[:alnum:]_])' ||
                            regexp_replace(x, '\\\\s+', '[\\\\s._/+:-]+', 'g') ||
                            '([^[:alnum:]_]|$)'
                        )
                        OR (
                            length(regexp_replace(x, '[^[:alnum:]#+]+', '', 'g')) > 4
                            AND strpos(
                                regexp_replace(lower(COALESCE({text_expression}, '')), '[^[:alnum:]#+]+', '', 'g'),
                                regexp_replace(x, '[^[:alnum:]#+]+', '', 'g')
                            ) > 0
                        )
                )
    """


def build_first_keyword_match_sql(keyword_list, text_expression: str, default: str = "Unknown") -> str:
    """Build a DuckDB expression returning the first canonical keyword matched in text."""
    list_filter_expression = build_keyword_list_filter_sql(keyword_list, text_expression)
    return f"COALESCE(NULLIF(({list_filter_expression})[1], ''), '{default}')"


def build_employment_type_match_sql(keyword_list, text_expression: str, default: str = "Unknown") -> str:
    """Build a DuckDB expression returning the first canonical employment type matched in text."""
    normalized_text = f"lower(COALESCE({text_expression}, ''))"
    return f"""
                COALESCE(
                    NULLIF(
                        (
                            list_filter(
                                {keyword_list},
                                x ->
                                    CASE
                                        WHEN x = 'FULL_TIME' THEN regexp_matches(
                                            {normalized_text},
                                            '(^|[^[:alnum:]_])(full[\\s._/+:-]*time|permanent|toan[\\s._/+:-]*thoi[\\s._/+:-]*gian|toàn[\\s._/+:-]*thời[\\s._/+:-]*gian)([^[:alnum:]_]|$)'
                                        )
                                        WHEN x = 'PART_TIME' THEN regexp_matches(
                                            {normalized_text},
                                            '(^|[^[:alnum:]_])(part[\\s._/+:-]*time|ban[\\s._/+:-]*thoi[\\s._/+:-]*gian|bán[\\s._/+:-]*thời[\\s._/+:-]*gian)([^[:alnum:]_]|$)'
                                        )
                                        WHEN x = 'CONTRACT' THEN regexp_matches(
                                            {normalized_text},
                                            '(^|[^[:alnum:]_])(contract|contractor|freelance|hop[\\s._/+:-]*dong|hợp[\\s._/+:-]*đồng)([^[:alnum:]_]|$)'
                                        )
                                        WHEN x = 'TEMPORARY' THEN regexp_matches(
                                            {normalized_text},
                                            '(^|[^[:alnum:]_])(temporary|temp|seasonal|thoi[\\s._/+:-]*vu|thời[\\s._/+:-]*vụ|tam[\\s._/+:-]*thoi|tạm[\\s._/+:-]*thời)([^[:alnum:]_]|$)'
                                        )
                                        WHEN x = 'INTERNSHIP' THEN regexp_matches(
                                            {normalized_text},
                                            '(^|[^[:alnum:]_])(internship|intern|trainee|thuc[\\s._/+:-]*tap|thực[\\s._/+:-]*tập)([^[:alnum:]_]|$)'
                                        )
                                        WHEN x = 'VOLUNTEER' THEN regexp_matches(
                                            {normalized_text},
                                            '(^|[^[:alnum:]_])(volunteer|voluntary|tinh[\\s._/+:-]*nguyen|tình[\\s._/+:-]*nguyện)([^[:alnum:]_]|$)'
                                        )
                                        ELSE regexp_matches(
                                            {normalized_text},
                                            '(^|[^[:alnum:]_])' ||
                                            regexp_replace(lower(x), '_+', '[\\s._/+:-]+', 'g') ||
                                            '([^[:alnum:]_]|$)'
                                        )
                                    END
                            )
                        )[1],
                        ''
                    ),
                    '{default}'
                )
    """


def build_working_type_fallback_sql(working_type_expression: str, fallback_expression: str) -> str:
    """Prefer raw working type, but fallback when it is blank or already unknown-like."""
    return f"""
                COALESCE(
                    CASE
                        WHEN lower(trim(COALESCE({working_type_expression}, ''))) IN ('', 'unknown', 'n/a', 'na', 'none', 'null')
                            THEN NULL
                        ELSE trim({working_type_expression})
                    END,
                    {fallback_expression}
                )
    """


def build_location_type_match_sql(keyword_list, text_expression: str, default: str = "N/A") -> str:
    """Build a DuckDB expression returning the first canonical location type matched in text."""
    normalized_text = f"lower(COALESCE({text_expression}, ''))"
    return f"""
                COALESCE(
                    NULLIF(
                        (
                            list_filter(
                                {keyword_list},
                                x ->
                                    CASE
                                        WHEN x = 'REMOTE' THEN regexp_matches(
                                            {normalized_text},
                                            '(^|[^[:alnum:]_])(remote|work[\\s._/+:-]*from[\\s._/+:-]*home|wfh|lam[\\s._/+:-]*viec[\\s._/+:-]*tu[\\s._/+:-]*xa|làm[\\s._/+:-]*việc[\\s._/+:-]*từ[\\s._/+:-]*xa)([^[:alnum:]_]|$)'
                                        )
                                        WHEN x = 'HYBRID' THEN regexp_matches(
                                            {normalized_text},
                                            '(^|[^[:alnum:]_])(hybrid|remote[\\s._/+:-]*and[\\s._/+:-]*office|office[\\s._/+:-]*and[\\s._/+:-]*remote|linh[\\s._/+:-]*hoat|linh[\\s._/+:-]*hoạt)([^[:alnum:]_]|$)'
                                        )
                                        WHEN x = 'ON_SITE' THEN regexp_matches(
                                            {normalized_text},
                                            '(^|[^[:alnum:]_])(on[\\s._/+:-]*site|onsite|in[\\s._/+:-]*office|at[\\s._/+:-]*office|office|van[\\s._/+:-]*phong|văn[\\s._/+:-]*phòng)([^[:alnum:]_]|$)'
                                        )
                                        ELSE false
                                    END
                            )
                        )[1],
                        ''
                    ),
                    '{default}'
                )
    """


def build_seniority_level_match_sql(keyword_list, text_expression: str, default: str = "Unknown") -> str:
    """Build a DuckDB expression returning the first canonical seniority level matched in text."""
    normalized_text = f"lower(COALESCE({text_expression}, ''))"
    return f"""
                COALESCE(
                    NULLIF(
                        (
                            list_filter(
                                {keyword_list},
                                x ->
                                    CASE
                                        WHEN x = 'Entry' THEN regexp_matches(
                                            {normalized_text},
                                            '(^|[^[:alnum:]_])(entry|entry[\\s._/+:-]*level|fresher|fresh[\\s._/+:-]*graduate|graduate|new[\\s._/+:-]*grad|moi[\\s._/+:-]*ra[\\s._/+:-]*truong|mới[\\s._/+:-]*ra[\\s._/+:-]*trường)([^[:alnum:]_]|$)'
                                        )
                                        WHEN x = 'Junior' THEN regexp_matches(
                                            {normalized_text},
                                            '(^|[^[:alnum:]_])(junior|jr\\.?|associate|0[\\s._/+:-]*1[\\s._/+:-]*years?|1[\\s._/+:-]*year|duoi[\\s._/+:-]*1[\\s._/+:-]*nam|dưới[\\s._/+:-]*1[\\s._/+:-]*năm)([^[:alnum:]_]|$)'
                                        )
                                        WHEN x = 'Mid-Level' THEN regexp_matches(
                                            {normalized_text},
                                            '(^|[^[:alnum:]_])(mid|middle|mid[\\s._/+:-]*level|intermediate|2[\\s._/+:-]*4[\\s._/+:-]*years?|2\\+[\\s._/+:-]*years?|3\\+[\\s._/+:-]*years?)([^[:alnum:]_]|$)'
                                        )
                                        WHEN x = 'Senior' THEN regexp_matches(
                                            {normalized_text},
                                            '(^|[^[:alnum:]_])(senior|sr\\.?|experienced|5\\+[\\s._/+:-]*years?|5[\\s._/+:-]*years?|trên[\\s._/+:-]*5[\\s._/+:-]*năm|tren[\\s._/+:-]*5[\\s._/+:-]*nam)([^[:alnum:]_]|$)'
                                        )
                                        WHEN x = 'Lead' THEN regexp_matches(
                                            {normalized_text},
                                            '(^|[^[:alnum:]_])(lead|team[\\s._/+:-]*lead|tech[\\s._/+:-]*lead|technical[\\s._/+:-]*lead|leader|truong[\\s._/+:-]*nhom|trưởng[\\s._/+:-]*nhóm)([^[:alnum:]_]|$)'
                                        )
                                        WHEN x = 'Principal' THEN regexp_matches(
                                            {normalized_text},
                                            '(^|[^[:alnum:]_])(principal|principal[\\s._/+:-]*engineer|architect)([^[:alnum:]_]|$)'
                                        )
                                        WHEN x = 'Staff' THEN regexp_matches(
                                            {normalized_text},
                                            '(^|[^[:alnum:]_])(staff|staff[\\s._/+:-]*engineer)([^[:alnum:]_]|$)'
                                        )
                                        WHEN x = 'Manager' THEN regexp_matches(
                                            {normalized_text},
                                            '(^|[^[:alnum:]_])(manager|engineering[\\s._/+:-]*manager|head[\\s._/+:-]*of|quan[\\s._/+:-]*ly|quản[\\s._/+:-]*lý)([^[:alnum:]_]|$)'
                                        )
                                        WHEN x = 'Director' THEN regexp_matches(
                                            {normalized_text},
                                            '(^|[^[:alnum:]_])(director|director[\\s._/+:-]*of|giam[\\s._/+:-]*doc|giám[\\s._/+:-]*đốc)([^[:alnum:]_]|$)'
                                        )
                                        WHEN x = 'VP' THEN regexp_matches(
                                            {normalized_text},
                                            '(^|[^[:alnum:]_])(vp|vice[\\s._/+:-]*president)([^[:alnum:]_]|$)'
                                        )
                                        WHEN x = 'C-Level' THEN regexp_matches(
                                            {normalized_text},
                                            '(^|[^[:alnum:]_])(c[\\s._/+:-]*level|cto|cio|ceo|coo|chief[\\s._/+:-]*(technology|information|executive|operating)[\\s._/+:-]*officer)([^[:alnum:]_]|$)'
                                        )
                                        ELSE false
                                    END
                            )
                        )[1],
                        ''
                    ),
                    '{default}'
                )
    """


def build_experience_year_level_match_sql(text_expression: str, default: str = "Unknown") -> str:
    """Build a DuckDB expression mapping years of experience in text to a canonical seniority level."""
    normalized_text = f"lower(COALESCE({text_expression}, ''))"
    experience_context = (
        "(years?|yrs?|nam|năm)[\\s._/+:-]*(of[\\s._/+:-]*)?"
        "(experience|exp|kinh[\\s._/+:-]*nghiem|kinh[\\s._/+:-]*nghiệm)"
        "|(experience|exp|kinh[\\s._/+:-]*nghiem|kinh[\\s._/+:-]*nghiệm)"
    )
    return f"""
                CASE
                    WHEN regexp_matches(
                        {normalized_text},
                        '(^|[^[:alnum:]_])(0[\\s._/+:-]*(-|~|to|den|đến)[\\s._/+:-]*1[\\s._/+:-]*({experience_context}))([^[:alnum:]_]|$)'
                    )
                        THEN 'Entry'
                    WHEN regexp_matches(
                        {normalized_text},
                        '(^|[^[:alnum:]_])(1[\\s._/+:-]*(-|~|to|den|đến)[\\s._/+:-]*2[\\s._/+:-]*({experience_context}))([^[:alnum:]_]|$)'
                    )
                        THEN 'Junior'
                    WHEN regexp_matches(
                        {normalized_text},
                        '(^|[^[:alnum:]_])(2[\\s._/+:-]*(-|~|to|den|đến)[\\s._/+:-]*4[\\s._/+:-]*({experience_context}))([^[:alnum:]_]|$)'
                    )
                        THEN 'Mid-Level'
                    WHEN regexp_matches(
                        {normalized_text},
                        '(^|[^[:alnum:]_])((4|5|6|7|8|9|[1-9][0-9])[\\s._/+:-]*(\\+|plus)?[\\s._/+:-]*({experience_context})|({experience_context})[\\s._/+:-]*(4|5|6|7|8|9|[1-9][0-9])[\\s._/+:-]*(\\+|plus)?)([^[:alnum:]_]|$)'
                    )
                        THEN 'Senior'
                    WHEN regexp_matches(
                        {normalized_text},
                        '(^|[^[:alnum:]_])((2|3)[\\s._/+:-]*(\\+|plus)?[\\s._/+:-]*({experience_context})|({experience_context})[\\s._/+:-]*(2|3)[\\s._/+:-]*(\\+|plus)?)([^[:alnum:]_]|$)'
                    )
                        THEN 'Mid-Level'
                    WHEN regexp_matches(
                        {normalized_text},
                        '(^|[^[:alnum:]_])((1)[\\s._/+:-]*(\\+|plus)?[\\s._/+:-]*({experience_context})|({experience_context})[\\s._/+:-]*(1)[\\s._/+:-]*(\\+|plus)?)([^[:alnum:]_]|$)'
                    )
                        THEN 'Junior'
                    WHEN regexp_matches(
                        {normalized_text},
                        '(^|[^[:alnum:]_])((0)[\\s._/+:-]*(\\+|plus)?[\\s._/+:-]*({experience_context})|({experience_context})[\\s._/+:-]*(0)[\\s._/+:-]*(\\+|plus)?)([^[:alnum:]_]|$)'
                    )
                        THEN 'Entry'
                    ELSE '{default}'
                END
    """


def build_unknown_fallback_sql(primary_expression: str, fallback_expression: str) -> str:
    """Use fallback when the primary expression returns an unknown-like value."""
    return f"""
                COALESCE(
                    NULLIF(
                        CASE
                            WHEN lower(trim(COALESCE({primary_expression}, ''))) IN ('', 'unknown', 'n/a', 'na', 'none', 'null')
                                THEN NULL
                            ELSE {primary_expression}
                        END,
                        ''
                    ),
                    {fallback_expression}
                )
    """


def build_canonical_field_fallback_sql(raw_expression: str, raw_match_expression: str, fallback_expression: str) -> str:
    """Prefer a canonicalized raw field, then fallback to description-derived value."""
    return f"""
                COALESCE(
                    CASE
                        WHEN lower(trim(COALESCE({raw_expression}, ''))) IN ('', 'unknown', 'n/a', 'na', 'none', 'null')
                            THEN NULL
                        ELSE NULLIF({raw_match_expression}, '')
                    END,
                    {fallback_expression}
                )
    """


def build_salary_match_sql(text_expression: str, default: str = "Unknown") -> str:
    """Build a DuckDB expression returning the first salary-like phrase matched in text."""
    normalized_text = f"lower(COALESCE({text_expression}, ''))"
    amount = "[0-9]+([.,][0-9]+)?"
    prefix = "(up[\\s._/+:-]*to|upto|from|to|tu|từ|den|đến)"
    currency_prefix = "[$€£]"
    range_separator = "(to|den|đến)"
    salary_unit = "(usd|vnd|vnđ|trieu|triệu|million)"
    salary_pattern = (
        f"(({prefix}[\\s._/+:-]*({currency_prefix}[\\s._/+:-]*)?{amount}"
        f"([\\s._/+:-]*{range_separator}[\\s._/+:-]*({currency_prefix}[\\s._/+:-]*)?{amount})?"
        f"([\\s._/+:-]*{salary_unit})?"
        f"|{currency_prefix}[\\s._/+:-]*{amount}"
        f"([\\s._/+:-]*{range_separator}[\\s._/+:-]*({currency_prefix}[\\s._/+:-]*)?{amount})?"
        f"([\\s._/+:-]*{salary_unit})?"
        f"|{amount}[\\s._/+:-]*{salary_unit}"
        f"([\\s._/+:-]*{range_separator}[\\s._/+:-]*({currency_prefix}[\\s._/+:-]*)?{amount}[\\s._/+:-]*{salary_unit}?)?"
        f"|{amount}[\\s._/+:-]*{range_separator}[\\s._/+:-]*({currency_prefix}[\\s._/+:-]*)?{amount}"
        f"([\\s._/+:-]*{salary_unit})?"
        "|negotiable|competitive|thoa[\\s._/+:-]*thuan|thỏa[\\s._/+:-]*thuận)"
    )
    return f"""
                COALESCE(
                    NULLIF(regexp_extract({normalized_text}, '{salary_pattern}', 0), ''),
                    '{default}'
                )
    """


def build_date_posted_match_sql(date_expression: str, processed_at_expression: str) -> str:
    """Build a DuckDB expression normalizing absolute and relative posted dates to dd-mm-yyyy."""
    normalized_text = f"lower(trim(COALESCE({date_expression}, '')))"
    minute_pattern = "([0-9]+)[\\s._/+:-]*(minutes?|mins?|min|phut|phút)[\\s._/+:-]*(ago|truoc|trước)"
    hour_pattern = "([0-9]+)[\\s._/+:-]*(hours?|hrs?|hr|gio|giờ)[\\s._/+:-]*(ago|truoc|trước)"
    day_pattern = "([0-9]+)[\\s._/+:-]*(days?|ngay|ngày)[\\s._/+:-]*(ago|truoc|trước)"
    today_pattern = "(^|[^[:alnum:]_])(today|just[\\s._/+:-]*now|new|hom[\\s._/+:-]*nay|hôm[\\s._/+:-]*nay|vua[\\s._/+:-]*xong|vừa[\\s._/+:-]*xong|moi[\\s._/+:-]*dang|mới[\\s._/+:-]*đăng)([^[:alnum:]_]|$)"
    yesterday_pattern = "(^|[^[:alnum:]_])(yesterday|hom[\\s._/+:-]*qua|hôm[\\s._/+:-]*qua)([^[:alnum:]_]|$)"
    return f"""
                STRFTIME(
                    COALESCE(
                        TRY_CAST(NULLIF(TRIM({date_expression}), '') AS DATE),
                        CAST(TRY_STRPTIME(NULLIF(TRIM({date_expression}), ''), '%d-%m-%Y') AS DATE),
                        CAST(TRY_STRPTIME(NULLIF(TRIM({date_expression}), ''), '%d/%m/%Y') AS DATE),
                        CAST(TRY_STRPTIME(NULLIF(TRIM({date_expression}), ''), '%Y-%m-%d') AS DATE),
                        CAST(TRY_STRPTIME(NULLIF(TRIM({date_expression}), ''), '%Y/%m/%d') AS DATE),
                        CAST(
                            CASE
                                WHEN regexp_matches({normalized_text}, '{minute_pattern}')
                                    THEN {processed_at_expression}
                                        - TRY_CAST(regexp_extract({normalized_text}, '{minute_pattern}', 1) AS INTEGER) * INTERVAL '1 minute'
                                WHEN regexp_matches({normalized_text}, '{hour_pattern}')
                                    THEN {processed_at_expression}
                                        - TRY_CAST(regexp_extract({normalized_text}, '{hour_pattern}', 1) AS INTEGER) * INTERVAL '1 hour'
                                WHEN regexp_matches({normalized_text}, '{day_pattern}')
                                    THEN {processed_at_expression}
                                        - TRY_CAST(regexp_extract({normalized_text}, '{day_pattern}', 1) AS INTEGER) * INTERVAL '1 day'
                                WHEN regexp_matches({normalized_text}, '{yesterday_pattern}')
                                    THEN {processed_at_expression} - INTERVAL '1 day'
                                WHEN regexp_matches({normalized_text}, '{today_pattern}')
                                    THEN {processed_at_expression}
                                ELSE {processed_at_expression}
                            END AS DATE
                        )
                    ),
                    '%d-%m-%Y'
                )
    """


def build_text_field_fallback_sql(raw_expression: str, fallback_expression: str) -> str:
    """Prefer raw text field, then fallback when it is blank or already unknown-like."""
    return f"""
                COALESCE(
                    CASE
                        WHEN lower(trim(COALESCE({raw_expression}, ''))) IN ('', 'unknown', 'n/a', 'na', 'none', 'null')
                            THEN NULL
                        ELSE trim({raw_expression})
                    END,
                    {fallback_expression}
                )
    """
