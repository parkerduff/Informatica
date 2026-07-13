#!/usr/bin/env python3
"""
infa_compat -- Informatica PowerCenter compatibility library.

Pure-Python, dependency-free implementations of the Informatica semantics used
across the eight EHRP->BIIS PowerCenter folders.  The same primitives are used
by:
  * the Glue PySpark jobs (called inside UDFs / mapPartitions),
  * the Glue Python Shell jobs (called directly),
  * the spec-derived golden baseline,
so there is a single, unit-tested source of truth for transformation semantics.

Everything here is intentionally faithful to PowerCenter behaviour (NULL
propagation, DECODE(TRUE, ...), overpunch trailing-sign decoding, IS_DATE
validation, fixed-width slicing by PHYSICALOFFSET/PHYSICALLENGTH, codepage
decoding, etc.).  See migration/spec/classification.md and the migration report
for how each is mapped from the XML expressions.
"""
from __future__ import annotations

import datetime as _dt
import re
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

# ---------------------------------------------------------------------------
# Sentinels / errors
# ---------------------------------------------------------------------------


class TransformationError(Exception):
    """Raised by the ERROR() Informatica function -- routed to ERROR_TBL."""

    def __init__(self, message: str = "transformation error"):
        super().__init__(message)
        self.message = message


# ---------------------------------------------------------------------------
# Codepage decoding.  Sources declare Latin1 / MS1252; never assume UTF-8.
# ---------------------------------------------------------------------------

_CODEPAGE_ALIASES = {
    "MS1252": "cp1252",
    "LATIN1": "latin-1",
    "ISO-8859-1": "latin-1",
    "UTF-8": "utf-8",
    "UTF8": "utf-8",
}


def resolve_codepage(codepage: Optional[str]) -> str:
    if not codepage:
        return "latin-1"
    return _CODEPAGE_ALIASES.get(codepage.upper(), "latin-1")


def decode_bytes(raw: bytes, codepage: Optional[str]) -> str:
    return raw.decode(resolve_codepage(codepage), errors="replace")


def encode_str(text: str, codepage: Optional[str]) -> bytes:
    return text.encode(resolve_codepage(codepage), errors="replace")


# ---------------------------------------------------------------------------
# Fixed-width slicing by PHYSICALOFFSET / PHYSICALLENGTH (includes FILLER_*).
# ---------------------------------------------------------------------------


def slice_fixed(line: str, offset: int, length: int) -> str:
    """Return the fixed-width slice; short lines are right-padded (like PMCMD)."""
    if line is None:
        return ""
    end = offset + length
    chunk = line[offset:end]
    if len(chunk) < length:
        chunk = chunk + (" " * (length - len(chunk)))
    return chunk


def parse_fixed_record(line: str, fields: Sequence[Dict[str, Any]]) -> Dict[str, str]:
    """Slice a fixed-width line into a dict keyed by field name.

    ``fields`` items must carry ``name``, ``physicaloffset`` and
    ``physicallength`` (as produced by the parser).  FILLER_* fields are kept so
    downstream expressions (e.g. SUBSTR(FILLER_6, 145, 5)) resolve correctly.
    """
    row: Dict[str, str] = {}
    for f in fields:
        off = f.get("physicaloffset")
        ln = f.get("physicallength")
        if off is None or ln is None:
            continue
        row[f["name"]] = slice_fixed(line, off, ln)
    return row


# ---------------------------------------------------------------------------
# Signed / overpunch trailing-sign numeric decoding.
#
# In these feeds the sign is stored as a trailing character ('+' or '-') and the
# implied decimal is inserted by SUBSTR concatenation, e.g. Pseudossn
# v_UNIF_ALLOW_AMT / o_UNIF_ALLOW_AMT:
#     v_sign = SUBSTR(in_amt, 6, 1)
#     v_amt  = SUBSTR(in_amt,1,3) || '.' || SUBSTR(in_amt,4,2)
#     DECODE(sign,'+', x, '-', x*-1)
# ---------------------------------------------------------------------------

# Classic IBM zoned-decimal overpunch table (kept for completeness / other feeds)
_OVERPUNCH_POS = {"{": "0", "A": "1", "B": "2", "C": "3", "D": "4",
                  "E": "5", "F": "6", "G": "7", "H": "8", "I": "9"}
_OVERPUNCH_NEG = {"}": "0", "J": "1", "K": "2", "L": "3", "M": "4",
                  "N": "5", "O": "6", "P": "7", "Q": "8", "R": "9"}


def decode_trailing_sign(digits: str, sign_char: str, scale: int) -> Optional[Decimal]:
    """Decode a trailing-sign numeric: digits + explicit '+'/'-' sign char."""
    s = (digits or "").strip()
    if s == "" or not is_number(insert_decimal(s, scale)):
        return None
    value = to_decimal(insert_decimal(s, scale), scale)
    if value is None:
        return None
    if sign_char == "-":
        return -value
    return value


def insert_decimal(intlike: str, scale: int) -> str:
    """Insert an implied decimal point ``scale`` digits from the right."""
    s = (intlike or "").strip()
    if scale <= 0 or s == "":
        return s
    neg = s.startswith("-")
    if neg:
        s = s[1:]
    s = s.rjust(scale + 1, "0")
    out = s[:-scale] + "." + s[-scale:]
    return ("-" + out) if neg else out


def decode_zoned_overpunch(field: str, scale: int = 0) -> Optional[Decimal]:
    """Decode IBM zoned-decimal overpunch (sign encoded in the last digit)."""
    if not field:
        return None
    f = field.strip()
    if not f:
        return None
    last = f[-1]
    body = f[:-1]
    sign = 1
    digit = last
    if last in _OVERPUNCH_POS:
        digit = _OVERPUNCH_POS[last]
    elif last in _OVERPUNCH_NEG:
        digit = _OVERPUNCH_NEG[last]
        sign = -1
    raw = body + digit
    if not raw.isdigit():
        return None
    val = to_decimal(insert_decimal(raw, scale), scale)
    return val if val is None else val * sign


# ---------------------------------------------------------------------------
# Informatica function equivalents (NULL-aware).
# ---------------------------------------------------------------------------


def iif(cond: Any, t: Any, f: Any = None) -> Any:
    return t if _truthy(cond) else f


def decode(value: Any, *args: Any) -> Any:
    """DECODE(value, s1, r1, s2, r2, ..., [default]).

    Matches PowerCenter: first equal search value wins; trailing odd arg is the
    default.  Used both as DECODE(port, ...) and DECODE(TRUE, cond, res, ...).
    """
    pairs = list(args)
    default = None
    if len(pairs) % 2 == 1:
        default = pairs[-1]
        pairs = pairs[:-1]
    for i in range(0, len(pairs), 2):
        search, result = pairs[i], pairs[i + 1]
        if _eq(value, search):
            return result
    return default


def substr(s: Any, start: int, length: Optional[int] = None) -> Optional[str]:
    """Informatica SUBSTR: 1-based; negative start counts from the end."""
    if s is None:
        return None
    text = _to_str(s)
    if start == 0:
        start = 1
    if start < 0:
        idx = len(text) + start
        if idx < 0:
            idx = 0
    else:
        idx = start - 1
    if length is None:
        return text[idx:]
    if length < 0:
        return ""
    return text[idx: idx + length]


def ltrim(s: Any, chars: str = " ") -> Optional[str]:
    if s is None:
        return None
    return _to_str(s).lstrip(chars)


def rtrim(s: Any, chars: str = " ") -> Optional[str]:
    if s is None:
        return None
    return _to_str(s).rstrip(chars)


def infa_length(s: Any) -> Optional[int]:
    if s is None:
        return None
    return len(_to_str(s))


def upper(s: Any) -> Optional[str]:
    return None if s is None else _to_str(s).upper()


def lower(s: Any) -> Optional[str]:
    return None if s is None else _to_str(s).lower()


def in_list(value: Any, *candidates: Any) -> bool:
    return any(_eq(value, c) for c in candidates)


def isnull(value: Any) -> bool:
    return value is None or (isinstance(value, str) and value == "")


def is_number(s: Any) -> bool:
    if s is None:
        return False
    if isinstance(s, (int, float, Decimal)):
        return True
    t = _to_str(s).strip()
    if t == "":
        return False
    try:
        Decimal(t)
        return True
    except InvalidOperation:
        return False


_DATE_FORMATS = {
    "MM/DD/YYYY": "%m/%d/%Y",
    "YYYY-MM-DD": "%Y-%m-%d",
    "YYYYMMDD": "%Y%m%d",
    "MMDDYYYY": "%m%d%Y",
    "YYYYDDMM": "%Y%d%m",
    "MM/DD/YYYY HH24:MI:SS": "%m/%d/%Y %H:%M:%S",
    "YYYY-MM-DD HH24:MI:SS": "%Y-%m-%d %H:%M:%S",
    "MM-DD-YYYY": "%m-%d-%Y",
}


def _fmt(fmt: str) -> str:
    key = (fmt or "").strip().upper()
    return _DATE_FORMATS.get(key, key)


def is_date(s: Any, fmt: str = "MM/DD/YYYY") -> bool:
    if s is None:
        return False
    t = _to_str(s).strip()
    if t == "":
        return False
    try:
        _dt.datetime.strptime(t, _fmt(fmt))
        return True
    except (ValueError, TypeError):
        return False


def to_date(s: Any, fmt: str = "MM/DD/YYYY") -> Optional[_dt.datetime]:
    if s is None:
        return None
    t = _to_str(s).strip()
    if t == "":
        return None
    try:
        return _dt.datetime.strptime(t, _fmt(fmt))
    except (ValueError, TypeError):
        return None


def to_char(value: Any, fmt: str = "MM/DD/YYYY HH24:MI:SS") -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, (_dt.datetime, _dt.date)):
        pyfmt = _fmt(fmt)
        if isinstance(value, _dt.date) and not isinstance(value, _dt.datetime):
            value = _dt.datetime(value.year, value.month, value.day)
        return value.strftime(pyfmt)
    return _to_str(value)


def to_decimal(s: Any, scale: int = 0) -> Optional[Decimal]:
    if s is None:
        return None
    if isinstance(s, Decimal):
        val = s
    else:
        t = _to_str(s).strip()
        if t == "":
            return None
        try:
            val = Decimal(t)
        except InvalidOperation:
            return None
    if not val.is_finite():          # NaN / Infinity -> NULL, like Informatica
        return None
    q = Decimal(1).scaleb(-scale) if scale > 0 else Decimal(1)
    try:
        return val.quantize(q, rounding=ROUND_HALF_UP)
    except InvalidOperation:
        return val


def to_integer(s: Any) -> Optional[int]:
    d = to_decimal(s, 0)
    return None if d is None else int(d)


def error(message: str = "transformation error") -> None:
    raise TransformationError(message)


def sessstarttime() -> _dt.datetime:
    """Session start time.  Overridable via set_sessstarttime for determinism."""
    return _SESSION_START[0]


_SESSION_START = [_dt.datetime.now()]


def set_sessstarttime(ts: _dt.datetime) -> None:
    _SESSION_START[0] = ts


# ---------------------------------------------------------------------------
# Record-type flagging (Header / Trailer / Detail) and trailer count checks.
# ---------------------------------------------------------------------------


def record_type_flag(first_field: str,
                     header_token: str = "HEADER",
                     trailer_token: str = "TRAILER") -> str:
    """Replicates v_RECORD_TYPE_FLAG DECODE(TRUE, SUBSTR(..)= 'HEADER' ...)."""
    s = first_field or ""
    if substr(s, 1, len(header_token)) == header_token:
        return "H"
    if substr(s, 1, len(trailer_token)) == trailer_token:
        return "T"
    return "D"


def detail_or_reject(record_flag: str, key_value: Any) -> str:
    """o_RECORD_TYPE_FLAG: detail with data -> 'D', else 'R' (reject)."""
    if record_flag == "D" and infa_length(ltrim(rtrim(key_value))) and \
            infa_length(ltrim(rtrim(key_value))) > 0:
        return record_flag
    return "R"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _to_str(v: Any) -> str:
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, Decimal):
        return format(v, "f")
    return str(v)


def _truthy(v: Any) -> bool:
    if v is None:
        return False
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float, Decimal)):
        return v != 0
    if isinstance(v, str):
        return v not in ("", "0", "FALSE", "false")
    return True


def _eq(a: Any, b: Any) -> bool:
    if a is None or b is None:
        return a is None and b is None
    if isinstance(a, bool) or isinstance(b, bool):
        return _truthy(a) == _truthy(b)
    if isinstance(a, (int, float, Decimal)) and isinstance(b, (int, float, Decimal)):
        return Decimal(str(a)) == Decimal(str(b))
    # numeric-vs-string comparison, PowerCenter compares as values when possible
    if is_number(a) and is_number(b) and not (isinstance(a, str) and isinstance(b, str)):
        try:
            return Decimal(_to_str(a)) == Decimal(_to_str(b))
        except InvalidOperation:
            pass
    return _to_str(a) == _to_str(b)


# ---------------------------------------------------------------------------
# Additional PowerCenter scalar functions (present in the eight exports)
# ---------------------------------------------------------------------------


def infa_abs(x: Any) -> Optional[Decimal]:
    d = to_decimal(x, 10)
    return None if d is None else abs(d)


def sign(x: Any) -> Optional[int]:
    d = to_decimal(x, 10)
    if d is None:
        return None
    return (d > 0) - (d < 0)


def chr_(n: Any) -> Optional[str]:
    i = to_integer(n)
    return None if i is None else chr(int(i))


def is_spaces(s: Any) -> bool:
    if s is None:
        return False
    return len(_to_str(s)) > 0 and _to_str(s).strip(" ") == ""


def lpad(s: Any, length: Any, pad: str = " ") -> Optional[str]:
    if s is None:
        return None
    n = to_integer(length) or 0
    text = _to_str(s)
    pad = _to_str(pad) if pad else " "
    if len(text) >= n:
        return text[:n]
    if not pad:
        return text
    need = n - len(text)
    fill = (pad * (need // len(pad) + 1))[:need]
    return fill + text


def rpad(s: Any, length: Any, pad: str = " ") -> Optional[str]:
    if s is None:
        return None
    n = to_integer(length) or 0
    text = _to_str(s)
    pad = _to_str(pad) if pad else " "
    if len(text) >= n:
        return text[:n]
    if not pad:
        return text
    need = n - len(text)
    fill = (pad * (need // len(pad) + 1))[:need]
    return text + fill


def instr(s: Any, sub: Any, start: Any = 1, occ: Any = 1) -> int:
    if s is None or sub is None:
        return 0
    text = _to_str(s)
    needle = _to_str(sub)
    st = to_integer(start) or 1
    n = to_integer(occ) or 1
    idx = (st - 1) if st > 0 else 0
    found = 0
    pos = idx
    while True:
        p = text.find(needle, pos)
        if p < 0:
            return 0
        found += 1
        if found == n:
            return p + 1
        pos = p + 1


def replacechr(case_flag: Any, s: Any, old_chars: Any, new_char: Any) -> Optional[str]:
    if s is None:
        return None
    text = _to_str(s)
    if old_chars is None:
        return text
    olds = _to_str(old_chars)
    new = "" if new_char is None else _to_str(new_char)
    new = new[:1]
    cs = _truthy(case_flag)
    if cs:
        table = {c: new for c in olds}
        return "".join(table.get(ch, ch) for ch in text)
    olds_l = olds.lower()
    return "".join(new if ch.lower() in olds_l else ch for ch in text)


def replacestr(case_flag: Any, s: Any, *args: Any) -> Optional[str]:
    if s is None:
        return None
    text = _to_str(s)
    if len(args) < 1:
        return text
    new = "" if args[-1] is None else _to_str(args[-1])
    olds = [(_to_str(a)) for a in args[:-1] if a is not None]
    cs = _truthy(case_flag)
    for old in olds:
        if not old:
            continue
        if cs:
            text = text.replace(old, new)
        else:
            out = []
            i = 0
            low = text.lower()
            ol = old.lower()
            while i < len(text):
                if low.startswith(ol, i):
                    out.append(new)
                    i += len(old)
                else:
                    out.append(text[i])
                    i += 1
            text = "".join(out)
            low = text.lower()
    return text


_DATE_PARTS = {"YYYY": "year", "YY": "year", "MM": "month", "MON": "month",
               "DD": "day", "DDD": "day", "HH": "hour", "MI": "minute",
               "SS": "second", "D": "day", "Y": "year"}


def _coerce_date(d: Any) -> Optional[_dt.datetime]:
    if d is None:
        return None
    if isinstance(d, _dt.datetime):
        return d
    if isinstance(d, _dt.date):
        return _dt.datetime(d.year, d.month, d.day)
    return to_date(_to_str(d))


def get_date_part(d: Any, part: Any) -> Optional[int]:
    dtv = _coerce_date(d)
    if dtv is None:
        return None
    p = _DATE_PARTS.get(_to_str(part).upper().strip("'\""), "day")
    parts = {"year": dtv.year, "month": dtv.month, "day": dtv.day,
             "hour": dtv.hour, "minute": dtv.minute, "second": dtv.second}
    return parts.get(p, dtv.day)


def add_to_date(d: Any, part: Any, amount: Any) -> Optional[_dt.datetime]:
    dtv = _coerce_date(d)
    if dtv is None:
        return None
    amt = to_integer(amount) or 0
    p = _DATE_PARTS.get(_to_str(part).upper().strip("'\""), "day")
    if p == "day":
        return dtv + _dt.timedelta(days=amt)
    if p == "hour":
        return dtv + _dt.timedelta(hours=amt)
    if p == "minute":
        return dtv + _dt.timedelta(minutes=amt)
    if p == "second":
        return dtv + _dt.timedelta(seconds=amt)
    if p == "month":
        m = dtv.month - 1 + amt
        year = dtv.year + m // 12
        month = m % 12 + 1
        return dtv.replace(year=year, month=month, day=min(dtv.day, 28))
    if p == "year":
        return dtv.replace(year=dtv.year + amt)
    return dtv


def trunc(x: Any, digits: Any = 0) -> Any:
    if isinstance(x, (_dt.date, _dt.datetime)):
        dtv = _coerce_date(x)
        return _dt.datetime(dtv.year, dtv.month, dtv.day) if dtv else None
    d = to_decimal(x, 10)
    if d is None:
        return None
    n = to_integer(digits) or 0
    q = Decimal(10) ** -n
    return (d / q).to_integral_value(rounding="ROUND_DOWN") * q


def setvariable(_var: Any, value: Any = None) -> Any:
    return value


def agg_identity(x: Any = None, *_: Any) -> Any:
    return x


# ---------------------------------------------------------------------------
# Function registry used by the expression evaluator.
# ---------------------------------------------------------------------------

FUNCTIONS: Dict[str, Callable[..., Any]] = {
    "IIF": iif,
    "DECODE": decode,
    "SUBSTR": substr,
    "SUBSTRING": substr,
    "LTRIM": ltrim,
    "RTRIM": rtrim,
    "TRIM": lambda s: None if s is None else _to_str(s).strip(),
    "LENGTH": infa_length,
    "UPPER": upper,
    "LOWER": lower,
    "IN": in_list,
    "ISNULL": isnull,
    "IS_NUMBER": is_number,
    "IS_DATE": is_date,
    "TO_DATE": to_date,
    "TO_CHAR": to_char,
    "TO_DECIMAL": to_decimal,
    "TO_INTEGER": to_integer,
    "TO_FLOAT": lambda s: None if to_decimal(s, 10) is None else float(to_decimal(s, 10)),
    "ERROR": error,
    "ABORT": error,
    "SESSSTARTTIME": sessstarttime,
    "TRUE": lambda: True,
    "FALSE": lambda: False,
    "ABS": infa_abs,
    "SIGN": sign,
    "CHR": chr_,
    "IS_SPACES": is_spaces,
    "LPAD": lpad,
    "RPAD": rpad,
    "INSTR": instr,
    "REPLACECHR": replacechr,
    "REPLACESTR": replacestr,
    "GET_DATE_PART": get_date_part,
    "ADD_TO_DATE": add_to_date,
    "TRUNC": trunc,
    "SETVARIABLE": setvariable,
    "SETMAXVARIABLE": setvariable,
    "SETMINVARIABLE": setvariable,
    "SETCOUNTVARIABLE": setvariable,
    "COUNT": agg_identity,
    "SUM": agg_identity,
    "MAX": agg_identity,
    "MIN": agg_identity,
    "FIRST": agg_identity,
    "LAST": agg_identity,
}
