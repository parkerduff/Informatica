"""Row count / schema / null validations used by every job."""
import logging
from typing import List, Optional

logger = logging.getLogger(__name__)


class ValidationError(Exception):
    pass


def validate_row_count(df, expected_min: int = 1, expected_max: Optional[int] = None,
                       context: str = "") -> int:
    count = df.count()
    if count < expected_min:
        raise ValidationError(f"{context}: row count {count} < expected minimum {expected_min}")
    if expected_max is not None and count > expected_max:
        raise ValidationError(f"{context}: row count {count} > expected maximum {expected_max}")
    logger.info("%s: row count %d OK", context or "validate_row_count", count)
    return count


def validate_schema(df, expected_columns: List[str], context: str = "") -> None:
    actual = set(df.columns)
    missing = [c for c in expected_columns if c not in actual]
    if missing:
        raise ValidationError(f"{context}: missing columns {missing}; actual={sorted(actual)}")


def validate_no_nulls(df, columns: List[str], context: str = "") -> None:
    from pyspark.sql import functions as F

    for col in columns:
        n = df.filter(F.col(col).isNull()).count()
        if n > 0:
            raise ValidationError(f"{context}: column {col} has {n} null values")


def log_row_count(conn, table: str, process_name: str, count: int, pay_period: dict) -> None:
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO COUNTER_TBL
            (RUN_DATE, PROCESS_NAME, COUNTER_DESCRIPTION, COUNTER_VALUE,
             PP_END_YEAR, PP_NUM, CYCLE_ID)
        VALUES (CAST(GETDATE() AS DATE), ?, ?, ?, ?, ?, ?)
        """,
        process_name,
        f"Rows loaded into {table}",
        count,
        pay_period.get("pp_end_year"),
        pay_period.get("pp_num"),
        pay_period.get("cycle_id", 1),
    )
