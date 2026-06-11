"""Shared CPM logic: payroll schema access, fixed-width rendering and the
header/trailer generation used by every agency extract."""
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from utils import schemas  # noqa: E402
from utils.db import get_current_pay_period, pyodbc_connection, read_table  # noqa: E402

logger = logging.getLogger("cpm.common")

NEWPAY_TABLE = "CPM_NEWPAY_TBL"

# Identity / ordering columns used by every agency extract. The full 501-field
# payroll schema is available via newpay_columns().
KEY_COLUMNS = ["EMPLOYEE_SSN_ID", "PP_NUM", "PP_END_YEAR"]


def newpay_fields():
    return schemas.get_table_fields("CPM_NIH", NEWPAY_TABLE, "sources")


def newpay_columns():
    return [f["name"] for f in newpay_fields()]


def agency_layout(module: str, definition: str, kind: str = "targets"):
    """Fixed-width layout (name/offset/length) for an agency flat-file record."""
    return schemas.fixed_width_layout(module, definition, kind)


def record_length(layout) -> int:
    return max((f["offset"] + f["length"]) for f in layout) if layout else 0


def render_fixed_width(row: dict, layout, signed_decimal_fields=None) -> str:
    """Render one record dict into a fixed-width line per the XML layout."""
    signed_decimal_fields = signed_decimal_fields or {}
    buf = [" "] * record_length(layout)
    for f in layout:
        name, offset, length = f["name"], f["offset"], f["length"]
        raw = row.get(name)
        if raw is None:
            text = " " * length
        elif name in signed_decimal_fields:
            scale = signed_decimal_fields[name]
            value = int(round(float(raw) * (10 ** scale)))
            sign = "-" if value < 0 else "+"
            text = f"{abs(value):0{length - 1}d}{sign}"
        else:
            text = str(raw)
        text = text[:length].ljust(length)
        buf[offset:offset + length] = list(text)
    return "".join(buf)


def build_header(pay_period: dict, agency: str) -> str:
    return (
        f"H{agency:<4}{pay_period['pp_end_year']:04d}{pay_period['pp_num']:02d}"
    )


def build_trailer(record_count: int, agency: str) -> str:
    return f"T{agency:<4}{record_count:09d}"


def get_pay_period(spark, config, secret) -> dict:
    return get_current_pay_period(spark, config, secret)


def read_newpay(spark, config, secret, columns=None):
    return read_table(spark, NEWPAY_TABLE, config, secret, columns=columns or newpay_columns())


def load_staging_table(config, secret, table: str, lines, record_types):
    """Replace agency staging table contents with the rendered file lines."""
    with pyodbc_connection(secret, config) as conn:
        cur = conn.cursor()
        cur.execute(f"DELETE FROM [dbo].[{table}]")
        cur.fast_executemany = True
        rows = [
            (i + 1, rt, line)
            for i, (rt, line) in enumerate(zip(record_types, lines))
        ]
        if rows:
            cur.executemany(
                f"INSERT INTO [dbo].[{table}] (RECORD_NUM, RECORD_TYPE, RECORD_DATA, LOAD_DATE) "
                "VALUES (?, ?, ?, GETDATE())",
                rows,
            )
        conn.commit()
    return len(rows)
