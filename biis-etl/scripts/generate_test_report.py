"""Render a single self-contained HTML test report.

Aggregates the artifacts produced by the local validation harness into one
downloadable, dependency-free HTML file:

* ``reports/junit.xml``                 -> per-test results (unit / functional /
                                           regression / performance suites)
* ``reports/benchmark.json``            -> performance timings vs Informatica
                                           baselines
* ``reports/reconciliation/summary.json`` -> the 12 source-target reconciliation
                                           targets (zero-deprecation gate)

Each test's "validation" column is derived from the test's own docstring (first
line) when present, otherwise from a humanised form of the test name.

Usage::

    python scripts/generate_test_report.py \
        --reports-dir reports --out reports/test_report.html
"""
from __future__ import annotations

import argparse
import ast
import datetime as dt
import html
import json
import os
import sys
import xml.etree.ElementTree as ET
from typing import Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

SUITE_LABELS = {
    "unit": "Unit tests (transformation logic, mocked dependencies)",
    "functional": "Functional tests (end-to-end job runs against the DB)",
    "regression": "Regression tests (golden-dataset zero-diff comparison)",
    "performance": "Performance tests (timings vs Informatica baselines)",
}

# Informatica baselines (seconds) mirrored from tests/performance/test_benchmarks.py
BASELINES = {
    "pay_calendar": 30,
    "comptime": 60,
    "pseudossn": 120,
    "fda_leave": 180,
    "ehrp2biis": 600,
    "cpm_nih": 900,
}


def _humanise(name: str) -> str:
    text = name[len("test_"):] if name.startswith("test_") else name
    # drop pytest parametrisation suffix for the prose description
    base = text.split("[", 1)[0]
    return base.replace("_", " ").strip().capitalize()


def _collect_docstrings() -> Dict[str, str]:
    """Map ``test_function_name`` -> first docstring line across the test tree."""
    docs: Dict[str, str] = {}
    tests_dir = os.path.join(_ROOT, "tests")
    for root, _dirs, files in os.walk(tests_dir):
        for fn in files:
            if not (fn.startswith("test_") and fn.endswith(".py")):
                continue
            try:
                tree = ast.parse(open(os.path.join(root, fn)).read())
            except (OSError, SyntaxError):
                continue
            for node in ast.walk(tree):
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) \
                        and node.name.startswith("test_"):
                    doc = ast.get_docstring(node)
                    if doc:
                        docs[node.name] = doc.strip().splitlines()[0].strip()
    return docs


class Case:
    def __init__(self, suite: str, file: str, name: str, time: float,
                 status: str, message: str):
        self.suite = suite
        self.file = file
        self.name = name
        self.time = time
        self.status = status
        self.message = message


def _suite_of(location: str) -> str:
    # location may be a path ("tests/unit/test_x.py") or dotted classname
    # ("tests.unit.test_x"); normalise both to slash-delimited tokens.
    norm = location.replace(".", "/")
    tokens = norm.split("/")
    for key in SUITE_LABELS:
        if key in tokens:
            return key
    return "other"


def parse_junit(path: str, docs: Dict[str, str]) -> List[Case]:
    tree = ET.parse(path)
    root = tree.getroot()
    suites = root.findall(".//testsuite") or [root]
    cases: List[Case] = []
    for ts in suites:
        for tc in ts.findall("testcase"):
            name = tc.get("name", "")
            file = tc.get("file") or tc.get("classname", "") or name
            time = float(tc.get("time") or 0.0)
            status, message = "passed", ""
            failure = tc.find("failure")
            error = tc.find("error")
            skipped = tc.find("skipped")
            if failure is not None:
                status = "failed"
                message = failure.get("message", "") or (failure.text or "")
            elif error is not None:
                status = "error"
                message = error.get("message", "") or (error.text or "")
            elif skipped is not None:
                status = "skipped"
                message = skipped.get("message", "") or (skipped.text or "")
            cases.append(Case(_suite_of(file), file, name, time, status, message))
    return cases


def load_reconciliation(path: str) -> List[dict]:
    if not os.path.exists(path):
        return []
    with open(path) as fh:
        return json.load(fh)


def load_benchmarks(path: str) -> List[dict]:
    if not os.path.exists(path):
        return []
    with open(path) as fh:
        data = json.load(fh)
    out = []
    for b in data.get("benchmarks", []):
        params = b.get("params") or {}
        module = params.get("module") or b.get("name", "")
        mean = b.get("stats", {}).get("mean")
        baseline = BASELINES.get(module)
        out.append({"module": module, "mean": mean, "baseline": baseline})
    return out


# --------------------------------------------------------------------------- #
# Rendering
# --------------------------------------------------------------------------- #
STYLE = """
body{font-family:-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;
  margin:0;background:#f6f8fa;color:#1f2328}
header{background:#0d1117;color:#fff;padding:28px 40px}
header h1{margin:0 0 4px;font-size:24px}
header .meta{color:#9da7b3;font-size:13px}
main{max-width:1180px;margin:0 auto;padding:24px 40px 64px}
.cards{display:flex;gap:16px;flex-wrap:wrap;margin:24px 0}
.card{flex:1;min-width:150px;background:#fff;border:1px solid #d0d7de;border-radius:8px;
  padding:16px 18px}
.card .n{font-size:30px;font-weight:700}
.card .l{font-size:12px;color:#636c76;text-transform:uppercase;letter-spacing:.04em}
.card.pass .n{color:#1a7f37}.card.fail .n{color:#cf222e}.card.skip .n{color:#9a6700}
h2{margin:34px 0 6px;font-size:18px;border-bottom:1px solid #d0d7de;padding-bottom:6px}
.sub{color:#636c76;font-size:13px;margin:0 0 12px}
table{width:100%;border-collapse:collapse;background:#fff;border:1px solid #d0d7de;
  border-radius:8px;overflow:hidden;font-size:13px}
th,td{text-align:left;padding:9px 12px;border-bottom:1px solid #eaeef2;vertical-align:top}
th{background:#f6f8fa;font-weight:600;color:#424a53}
tr:last-child td{border-bottom:none}
.badge{display:inline-block;padding:2px 9px;border-radius:20px;font-size:11px;font-weight:600}
.b-pass{background:#dafbe1;color:#1a7f37}.b-fail{background:#ffebe9;color:#cf222e}
.b-skip{background:#fff8c5;color:#9a6700}
.mono{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:12px;color:#57606a}
.right{text-align:right}
.msg{color:#cf222e;font-family:ui-monospace,monospace;font-size:11px;white-space:pre-wrap}
footer{color:#8c959f;font-size:12px;text-align:center;padding:24px}
"""


def _badge(status: str) -> str:
    cls = {"passed": "b-pass", "failed": "b-fail", "error": "b-fail",
           "skipped": "b-skip"}.get(status, "b-skip")
    return f'<span class="badge {cls}">{status.upper()}</span>'


def render(cases: List[Case], recon: List[dict], benches: List[dict],
           docs: Dict[str, str]) -> str:
    total = len(cases)
    passed = sum(c.status == "passed" for c in cases)
    failed = sum(c.status in ("failed", "error") for c in cases)
    skipped = sum(c.status == "skipped" for c in cases)
    duration = sum(c.time for c in cases)
    now = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    recon_pass = sum(r["verdict"] == "PASS" for r in recon)
    recon_total = len(recon)

    parts: List[str] = []
    parts.append(f"<!doctype html><html><head><meta charset='utf-8'>"
                 f"<title>BIIS ETL Test Report</title><style>{STYLE}</style></head><body>")
    parts.append("<header><h1>BIIS ETL Migration &mdash; Test &amp; Validation Report</h1>"
                 f"<div class='meta'>Generated {html.escape(now)} &nbsp;&bull;&nbsp; "
                 "PySpark migration of Informatica PowerCenter workflows &nbsp;&bull;&nbsp; "
                 "backend: SQLite (test)</div></header><main>")

    # summary cards
    parts.append("<div class='cards'>")
    parts.append(f"<div class='card'><div class='n'>{total}</div><div class='l'>Total tests</div></div>")
    parts.append(f"<div class='card pass'><div class='n'>{passed}</div><div class='l'>Passed</div></div>")
    parts.append(f"<div class='card fail'><div class='n'>{failed}</div><div class='l'>Failed</div></div>")
    parts.append(f"<div class='card skip'><div class='n'>{skipped}</div><div class='l'>Skipped</div></div>")
    parts.append(f"<div class='card'><div class='n'>{duration:.1f}s</div><div class='l'>Duration</div></div>")
    parts.append(f"<div class='card pass'><div class='n'>{recon_pass}/{recon_total}</div>"
                 "<div class='l'>Recon zero-diff</div></div>")
    parts.append("</div>")

    # reconciliation
    if recon:
        parts.append("<h2>Source &rarr; Target Reconciliation</h2>")
        parts.append("<p class='sub'>Definitive zero-deprecation gate: PySpark output compared "
                     "row-by-row and column-by-column against the golden Informatica baseline.</p>")
        parts.append("<table><tr><th>Module</th><th>Target table</th><th class='right'>Expected</th>"
                     "<th class='right'>Actual</th><th class='right'>Diffs</th><th>Verdict</th></tr>")
        for r in recon:
            v = "passed" if r["verdict"] == "PASS" else "failed"
            parts.append(f"<tr><td>{html.escape(r['module'])}</td>"
                         f"<td class='mono'>{html.escape(r['table'])}</td>"
                         f"<td class='right'>{r['expected']}</td>"
                         f"<td class='right'>{r['actual']}</td>"
                         f"<td class='right'>{r['diffs']}</td><td>{_badge(v)}</td></tr>")
        parts.append("</table>")

    # test suites
    for suite, label in SUITE_LABELS.items():
        suite_cases = [c for c in cases if c.suite == suite]
        if not suite_cases:
            continue
        sp = sum(c.status == "passed" for c in suite_cases)
        parts.append(f"<h2>{html.escape(label)}</h2>")
        parts.append(f"<p class='sub'>{sp}/{len(suite_cases)} passed</p>")
        if suite == "performance":
            parts.append("<table><tr><th>Test case</th><th>Validation</th>"
                         "<th class='right'>Mean (s)</th><th class='right'>Baseline (s)</th>"
                         "<th class='right'>Headroom</th><th>Result</th></tr>")
            bench_by_mod = {b["module"]: b for b in benches}
            for c in suite_cases:
                module = c.name.split("[", 1)[1].rstrip("]").split("-")[0] \
                    if "[" in c.name else c.name
                b = bench_by_mod.get(module)
                mean = f"{b['mean']:.3f}" if b and b.get("mean") is not None else "&ndash;"
                base = b["baseline"] if b and b.get("baseline") is not None else "&ndash;"
                headroom = "&ndash;"
                if b and b.get("mean") and b.get("baseline"):
                    headroom = f"{(b['baseline'] * 2) / b['mean']:.1f}&times;"
                desc = docs.get(c.name.split("[", 1)[0], "Runs within 2&times; the Informatica baseline")
                parts.append(f"<tr><td class='mono'>{html.escape(c.name)}</td>"
                             f"<td>{desc}</td><td class='right'>{mean}</td>"
                             f"<td class='right'>{base}</td><td class='right'>{headroom}</td>"
                             f"<td>{_badge(c.status)}</td></tr>")
            parts.append("</table>")
            continue

        parts.append("<table><tr><th>Test case</th><th>Validation</th>"
                     "<th class='right'>Time (s)</th><th>Result</th></tr>")
        for c in suite_cases:
            doc = docs.get(c.name.split("[", 1)[0]) or _humanise(c.name)
            row = (f"<tr><td class='mono'>{html.escape(c.name)}</td>"
                   f"<td>{html.escape(doc)}</td>"
                   f"<td class='right'>{c.time:.3f}</td><td>{_badge(c.status)}</td></tr>")
            parts.append(row)
            if c.message:
                parts.append(f"<tr><td colspan='4' class='msg'>{html.escape(c.message[:1200])}</td></tr>")
        parts.append("</table>")

    parts.append("<footer>BIIS ETL local execution &amp; validation harness "
                 "&mdash; auto-generated from junit.xml, benchmark.json and reconciliation summary."
                 "</footer>")
    parts.append("</main></body></html>")
    return "".join(parts)


def main(argv: Optional[List[str]] = None) -> None:
    p = argparse.ArgumentParser(description="Generate a self-contained HTML test report")
    p.add_argument("--reports-dir", default="reports")
    p.add_argument("--out", default="reports/test_report.html")
    args = p.parse_args(argv)

    junit = os.path.join(args.reports_dir, "junit.xml")
    if not os.path.exists(junit):
        print(f"ERROR: {junit} not found. Run the test suite first "
              "(make test / pytest --junitxml).", file=sys.stderr)
        sys.exit(1)

    docs = _collect_docstrings()
    cases = parse_junit(junit, docs)
    recon = load_reconciliation(os.path.join(args.reports_dir, "reconciliation", "summary.json"))
    benches = load_benchmarks(os.path.join(args.reports_dir, "benchmark.json"))

    html_text = render(cases, recon, benches, docs)
    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    with open(args.out, "w") as fh:
        fh.write(html_text)
    print(f"Wrote test report: {args.out} "
          f"({len(cases)} tests, {len(recon)} reconciliation targets)")


if __name__ == "__main__":
    main()
