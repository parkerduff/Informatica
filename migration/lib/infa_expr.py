#!/usr/bin/env python3
"""
infa_expr -- a small, faithful evaluator for the Informatica PowerCenter
expression language used by the EHRP->BIIS mappings.

Supports the subset present across the eight folders:
  * string / numeric / TRUE / FALSE / NULL literals
  * column (port) references
  * function calls dispatched to ``infa_compat.FUNCTIONS``
  * string concatenation with ``||``
  * arithmetic  + - * /
  * comparisons  =  <>  !=  >  <  >=  <=
  * logical  AND  OR  NOT
  * parentheses
  * ``--`` line comments (stripped)

An expression string compiles to a Python callable ``f(row: dict) -> value``.
The same compiled expressions are executed by the PySpark jobs (inside
mapPartitions), the Python Shell jobs and the golden baseline, so transformation
semantics come from one unit-tested implementation.
"""
from __future__ import annotations

import re
from decimal import Decimal
from typing import Any, Callable, Dict, List, Optional, Tuple

from . import infa_compat as C

# ---------------------------------------------------------------------------
# Tokenizer
# ---------------------------------------------------------------------------

_TOKEN_RE = re.compile(
    r"""
    \s+                         |   # whitespace (skipped)
    '(?:[^']|'')*'              |   # single-quoted string ('' escapes ')
    \d+\.\d+|\.\d+|\d+          |   # number
    <>|!=|>=|<=|\|\||=|>|<      |   # operators
    [()+\-*/,]                  |   # punctuation
    [A-Za-z_][A-Za-z0-9_$]*        # identifier
    """,
    re.VERBOSE,
)

_KEYWORDS = {"AND", "OR", "NOT", "TRUE", "FALSE", "NULL"}


def _strip_comments(expr: str) -> str:
    out_lines = []
    for line in expr.splitlines():
        idx = line.find("--")
        if idx >= 0:
            line = line[:idx]
        out_lines.append(line)
    return "\n".join(out_lines)


def tokenize(expr: str) -> List[Tuple[str, Any]]:
    expr = _strip_comments(expr)
    tokens: List[Tuple[str, Any]] = []
    pos = 0
    n = len(expr)
    while pos < n:
        m = _TOKEN_RE.match(expr, pos)
        if not m:
            raise SyntaxError(f"cannot tokenize near: {expr[pos:pos + 20]!r}")
        text = m.group(0)
        pos = m.end()
        if text.isspace():
            continue
        if text.startswith("'"):
            tokens.append(("STR", text[1:-1].replace("''", "'")))
        elif re.fullmatch(r"\d+\.\d+|\.\d+|\d+", text):
            tokens.append(("NUM", Decimal(text)))
        elif text in ("<>", "!=", ">=", "<=", "=", ">", "<"):
            tokens.append(("CMP", text))
        elif text == "||":
            tokens.append(("CONCAT", text))
        elif text in "+-*/":
            tokens.append(("OP", text))
        elif text in "(),":
            tokens.append((text, text))
        else:
            up = text.upper()
            if up in _KEYWORDS:
                tokens.append((up, up))
            else:
                tokens.append(("IDENT", text))
    tokens.append(("EOF", None))
    return tokens


# ---------------------------------------------------------------------------
# Parser -> AST (nested tuples).  Grammar (lowest to highest precedence):
#   or_expr   := and_expr (OR and_expr)*
#   and_expr  := not_expr (AND not_expr)*
#   not_expr  := NOT not_expr | cmp_expr
#   cmp_expr  := concat (CMP concat)*
#   concat    := add (|| add)*
#   add       := mul ((+|-) mul)*
#   mul       := unary ((*|/) unary)*
#   unary     := (-)? primary
#   primary   := NUM | STR | TRUE | FALSE | NULL | IDENT
#              | IDENT '(' args ')' | '(' or_expr ')'
# ---------------------------------------------------------------------------


class _Parser:
    def __init__(self, tokens: List[Tuple[str, Any]]):
        self.toks = tokens
        self.i = 0

    def peek(self) -> Tuple[str, Any]:
        return self.toks[self.i]

    def next(self) -> Tuple[str, Any]:
        t = self.toks[self.i]
        self.i += 1
        return t

    def expect(self, kind: str) -> Tuple[str, Any]:
        t = self.next()
        if t[0] != kind:
            raise SyntaxError(f"expected {kind}, got {t}")
        return t

    def parse(self):
        node = self.or_expr()
        if self.peek()[0] != "EOF":
            raise SyntaxError(f"trailing tokens: {self.peek()}")
        return node

    def or_expr(self):
        node = self.and_expr()
        while self.peek()[0] == "OR":
            self.next()
            node = ("or", node, self.and_expr())
        return node

    def and_expr(self):
        node = self.not_expr()
        while self.peek()[0] == "AND":
            self.next()
            node = ("and", node, self.not_expr())
        return node

    def not_expr(self):
        if self.peek()[0] == "NOT":
            self.next()
            return ("not", self.not_expr())
        return self.cmp_expr()

    def cmp_expr(self):
        node = self.concat()
        while self.peek()[0] == "CMP":
            op = self.next()[1]
            node = ("cmp", op, node, self.concat())
        return node

    def concat(self):
        node = self.add()
        while self.peek()[0] == "CONCAT":
            self.next()
            node = ("concat", node, self.add())
        return node

    def add(self):
        node = self.mul()
        while self.peek()[0] == "OP" and self.peek()[1] in "+-":
            op = self.next()[1]
            node = ("arith", op, node, self.mul())
        return node

    def mul(self):
        node = self.unary()
        while self.peek()[0] == "OP" and self.peek()[1] in "*/":
            op = self.next()[1]
            node = ("arith", op, node, self.unary())
        return node

    def unary(self):
        if self.peek()[0] == "OP" and self.peek()[1] == "-":
            self.next()
            return ("neg", self.unary())
        return self.primary()

    def primary(self):
        kind, val = self.peek()
        if kind == "NUM":
            self.next()
            return ("num", val)
        if kind == "STR":
            self.next()
            return ("str", val)
        if kind in ("TRUE", "FALSE"):
            self.next()
            return ("bool", kind == "TRUE")
        if kind == "NULL":
            self.next()
            return ("null",)
        if kind == "(":
            self.next()
            node = self.or_expr()
            self.expect(")")
            return node
        if kind == "IDENT":
            self.next()
            if self.peek()[0] == "(":
                self.next()
                args = []
                if self.peek()[0] != ")":
                    args.append(self.or_expr())
                    while self.peek()[0] == ",":
                        self.next()
                        args.append(self.or_expr())
                self.expect(")")
                return ("call", val.upper(), args)
            return ("col", val)
        raise SyntaxError(f"unexpected token {self.peek()}")


def parse(expr: str):
    return _Parser(tokenize(expr)).parse()


# ---------------------------------------------------------------------------
# Evaluator
# ---------------------------------------------------------------------------

_NUMERIC = (int, float, Decimal)


def _num(v: Any) -> Optional[Decimal]:
    if v is None:
        return None
    if isinstance(v, bool):
        return Decimal(1) if v else Decimal(0)
    if isinstance(v, Decimal):
        return v
    if isinstance(v, (int, float)):
        return Decimal(str(v))
    try:
        return Decimal(str(v).strip())
    except Exception:
        return None


def _eval(node, row: Dict[str, Any]) -> Any:
    tag = node[0]
    if tag == "num":
        return node[1]
    if tag == "str":
        return node[1]
    if tag == "bool":
        return node[1]
    if tag == "null":
        return None
    if tag == "col":
        return row.get(node[1])
    if tag == "neg":
        v = _num(_eval(node[1], row))
        return None if v is None else -v
    if tag == "concat":
        a = _eval(node[1], row)
        b = _eval(node[2], row)
        if a is None or b is None:
            # PowerCenter treats NULL in concat as empty string
            a = "" if a is None else C._to_str(a)
            b = "" if b is None else C._to_str(b)
        return C._to_str(a) + C._to_str(b)
    if tag == "arith":
        a = _num(_eval(node[2], row))
        b = _num(_eval(node[3], row))
        if a is None or b is None:
            return None
        op = node[1]
        if op == "+":
            return a + b
        if op == "-":
            return a - b
        if op == "*":
            return a * b
        if op == "/":
            return None if b == 0 else a / b
    if tag == "cmp":
        op = node[1]
        a = _eval(node[2], row)
        b = _eval(node[3], row)
        return _compare(op, a, b)
    if tag == "and":
        return C._truthy(_eval(node[1], row)) and C._truthy(_eval(node[2], row))
    if tag == "or":
        return C._truthy(_eval(node[1], row)) or C._truthy(_eval(node[2], row))
    if tag == "not":
        return not C._truthy(_eval(node[1], row))
    if tag == "call":
        return _eval_call(node[1], node[2], row)
    raise ValueError(f"unknown node {node}")


def _compare(op: str, a: Any, b: Any) -> Optional[bool]:
    if op in ("=",):
        return C._eq(a, b)
    if op in ("<>", "!="):
        return not C._eq(a, b)
    if a is None or b is None:
        return None
    na, nb = _num(a), _num(b)
    if na is not None and nb is not None and not (isinstance(a, str) and isinstance(b, str)):
        left, right = na, nb
    else:
        left, right = C._to_str(a), C._to_str(b)
    if op == ">":
        return left > right
    if op == "<":
        return left < right
    if op == ">=":
        return left >= right
    if op == "<=":
        return left <= right
    raise ValueError(op)


def _eval_call(name: str, arg_nodes, row: Dict[str, Any]) -> Any:
    fn = C.FUNCTIONS.get(name)
    if fn is None:
        raise ValueError(f"unsupported Informatica function: {name}")
    # IIF short-circuits so ERROR()/side-effecting branches only run when taken.
    if name == "IIF":
        cond = _eval(arg_nodes[0], row)
        if C._truthy(cond):
            return _eval(arg_nodes[1], row)
        return _eval(arg_nodes[2], row) if len(arg_nodes) > 2 else None
    if name == "DECODE":
        value = _eval(arg_nodes[0], row)
        rest = arg_nodes[1:]
        i = 0
        while i + 1 < len(rest):
            search = _eval(rest[i], row)
            if C._eq(value, search):
                return _eval(rest[i + 1], row)
            i += 2
        if len(rest) % 2 == 1:  # default
            return _eval(rest[-1], row)
        return None
    args = [_eval(a, row) for a in arg_nodes]
    # numeric literal args to SUBSTR need to be ints
    if name in ("SUBSTR", "SUBSTRING"):
        args = [args[0]] + [int(x) if isinstance(x, Decimal) else x for x in args[1:]]
    return fn(*args)


def compile_expr(expr: str) -> Callable[[Dict[str, Any]], Any]:
    """Compile an Informatica expression to ``f(row) -> value``."""
    ast = parse(expr)
    return lambda row: _eval(ast, row)


def evaluate(expr: str, row: Dict[str, Any]) -> Any:
    return _eval(parse(expr), row)
