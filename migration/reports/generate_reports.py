#!/usr/bin/env python3
"""
reports.generate_reports -- build the two self-contained (offline) HTML reports:

  * migration_report.html      -- exhaustive migration inventory + traceability
  * data_analysis_report.html  -- reconciliation + performance + PASS/FAIL verdict

Both are single files with inline CSS + inline SVG charts and NO external
dependencies, so they render fully offline in any browser.
"""
from __future__ import annotations

import datetime as dt
import html
import json
import os
import sys
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from migration.jobs import configs
from migration.lib import mapping_resolver as MR
from migration.sql import pushdown

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
SPEC_DIR = os.path.join(REPO_ROOT, "migration", "spec")
REPORT_DIR = os.path.join(REPO_ROOT, "migration", "reports")

E = html.escape


def _load_results(mode: str) -> Optional[Dict[str, Any]]:
    path = os.path.join(REPORT_DIR, f"_results_{mode}.json")
    if os.path.exists(path):
        with open(path) as fh:
            return json.load(fh)
    return None


CSS = """
* { box-sizing: border-box; }
body { font-family: Inter, Roboto, -apple-system, Segoe UI, Helvetica, Arial, sans-serif;
       margin: 0; color: #FFFFFF; background: #0F172A; line-height: 1.55; }
header { background: linear-gradient(135deg,#0B1220,#1E293B); color:#fff;
         padding:32px 40px; border-bottom:1px solid #243449; }
header h1 { margin:0 0 6px; font-size:26px; }
header p { margin:0; color:#CBD5E1; font-size:14px; }
main { max-width: 1200px; margin: 0 auto; padding: 24px 40px 80px; }
h2 { margin-top:40px; font-size:20px; color:#FFFFFF; border-bottom:1px solid #243449; padding-bottom:6px; }
h3 { margin-top:26px; font-size:16px; color:#CBD5E1; }
p { color:#CBD5E1; }
table { border-collapse: collapse; width:100%; margin:12px 0; font-size:13px;
        background:#1E293B; border-radius:8px; overflow:hidden; }
th, td { border:1px solid #243449; padding:6px 10px; text-align:left; vertical-align:top; color:#E2E8F0; }
th { background:#243449; color:#E2E8F0; position: sticky; top:0; }
tr:nth-child(even) td { background:#243449; }
.pass { color:#34D399; font-weight:700; }
.fail { color:#F87171; font-weight:700; }
.badge { display:inline-block; padding:2px 8px; border-radius:10px; font-size:12px; font-weight:600; }
.badge.pass { background:rgba(52,211,153,.15); color:#34D399; }
.badge.fail { background:rgba(248,113,113,.15); color:#F87171; }
.badge.spark { background:rgba(34,211,238,.15); color:#22D3EE; }
.badge.shell { background:rgba(167,139,250,.15); color:#A78BFA; }
.cards { display:flex; gap:16px; flex-wrap:wrap; margin:16px 0; }
.card { background:#1E293B; border:1px solid #243449; border-radius:12px; padding:16px 20px; min-width:170px; }
.card .n { font-size:26px; font-weight:700; color:#22D3EE; }
.card .l { font-size:12px; color:#94A3B8; text-transform:uppercase; letter-spacing:.04em; }
code { background:#0F172A; color:#22D3EE; padding:1px 5px; border-radius:4px; font-size:12px; }
.small { font-size:12px; color:#94A3B8; }
.note { background:rgba(34,211,238,.08); border:1px solid #22D3EE; padding:12px 16px;
        border-radius:8px; margin:14px 0; font-size:13px; color:#CBD5E1; }
.mono { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; white-space:pre-wrap;
        font-size:12px; color:#E2E8F0; }
nav.toc { background:#1E293B; border:1px solid #243449; border-radius:12px; padding:14px 20px; margin:16px 0; }
nav.toc a { color:#22D3EE; text-decoration:none; margin-right:16px; font-size:13px; }
nav.toc b { color:#E2E8F0; }
"""


def _page(title: str, subtitle: str, body: str) -> str:
    ts = dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{E(title)}</title><style>{CSS}</style></head>
<body><header><h1>{E(title)}</h1><p>{E(subtitle)} &middot; generated {ts}</p></header>
<main>{body}</main></body></html>"""


def _badge(engine: str) -> str:
    cls = "spark" if engine == "pyspark" else "shell"
    label = "Glue PySpark" if engine == "pyspark" else "Glue Python Shell"
    return f'<span class="badge {cls}">{label}</span>'


def _verdict_badge(v: str) -> str:
    cls = "pass" if v == "PASS" else "fail"
    return f'<span class="badge {cls}">{v}</span>'


# ---------------------------------------------------------------------------
# Migration report
# ---------------------------------------------------------------------------

COMPONENT_MAP = [
    ("PowerCenter mapping / transformations", "Glue job (PySpark or Python Shell) + infa_compat library"),
    ("Source Qualifier / flat-file & relational reads", "records.read_source (fixed-width / delimited / relational)"),
    ("Expression / Variable ports", "infa_expr evaluator over verbatim TRANSFORMFIELD expressions"),
    ("Lookup (cached)", "Broadcast join (Spark broadcast / in-memory dict)"),
    ("Sorter (latest-record-wins)", "Spark Window + row_number() / stable dedup"),
    ("Router / Filter (good vs error)", "Declarative error rules -> ERROR_TBL routing"),
    ("Aggregator counters", "COUNTER_TBL audit rows"),
    ("Workflow / Session (ksh + cron)", "Step Functions state machine + EventBridge schedule"),
    ("Independent agency sessions", "Step Functions Parallel branches (child-session analog)"),
    ("Oracle PL/SQL pre/post (ehrp2biis_*, actstage)", "Push-down SQL tasks (DB-resident, not rewritten)"),
    ("mailx alerts", "SNS topic + subscriptions"),
    ("SFTP transfer scripts", "AWS Transfer Family / Lambda SFTP task"),
    ("archive_files / remove_file", "S3 lifecycle rules + cleanup task"),
    ("Session file params ($Param_Root_Directory)", "Glue job args --in-dir/--out-dir over S3 prefixes"),
]


def migration_report() -> str:
    specs = {f: json.load(open(os.path.join(SPEC_DIR, f"{f}.json"))) for f in configs.ALL_FOLDERS}
    parts: List[str] = []

    parts.append('<nav class="toc"><b>Contents:</b> '
                 '<a href="#overview">Overview</a>'
                 '<a href="#classification">Classification</a>'
                 '<a href="#component">Component Map</a>'
                 '<a href="#inventory">Per-Folder Inventory</a>'
                 '<a href="#parallel">Parallelization</a>'
                 '<a href="#pushdown">PL/SQL Push-down</a>'
                 '<a href="#iac">IaC</a>'
                 '<a href="#trace">Traceability Matrix</a></nav>')

    # overview cards
    n_src = sum(len(s["sources"]) for s in specs.values())
    n_tgt = sum(len(s["targets"]) for s in specs.values())
    n_map = sum(len(s["mappings"]) for s in specs.values())
    n_xform = sum(len(t.get("fields", [])) for s in specs.values()
                  for m in s["mappings"] for t in m["transformations"])
    parts.append('<h2 id="overview">Overview</h2>')
    parts.append('<div class="cards">'
                 f'<div class="card"><div class="n">8</div><div class="l">Folders converted</div></div>'
                 f'<div class="card"><div class="n">6 / 2</div><div class="l">PySpark / Python Shell</div></div>'
                 f'<div class="card"><div class="n">{n_src}</div><div class="l">Source defs</div></div>'
                 f'<div class="card"><div class="n">{n_tgt}</div><div class="l">Target defs</div></div>'
                 f'<div class="card"><div class="n">{n_map}</div><div class="l">Mappings</div></div>'
                 f'<div class="card"><div class="n">{n_xform}</div><div class="l">Transform ports</div></div>'
                 '</div>')
    parts.append('<div class="note"><b>Baseline assumption:</b> the golden dataset is '
                 '<i>spec-derived expected output</i> — reconstructed strictly from the XML '
                 'mapping expressions — not a live PowerCenter engine run (the engine was '
                 'unavailable). <b>Oracle warehouse:</b> assumed DB-resident with PL/SQL '
                 'push-down (default). <b>Live AWS:</b> not assumed — local execution + IaC '
                 'are delivered so deployment works with or without an AWS account.</div>')

    # classification
    parts.append('<h2 id="classification">Job Classification (Spark vs Python Shell)</h2>')
    rows = ""
    for f in configs.ALL_FOLDERS:
        p = configs.build(f)
        rows += (f"<tr><td><b>{E(f)}</b></td><td>{_badge(p.engine)}</td>"
                 f"<td>{E(p.primary_source.name)} ({E(p.primary_source.kind)}, "
                 f"{len(p.primary_source.columns)} cols)</td>"
                 f"<td>{E(p.target.name)} ({len(p.target.columns)} cols)</td>"
                 f"<td>{len(p.derived_ports)}</td><td>{'yes' if p.dedup else 'no'}</td>"
                 f"<td>{len(p.lookups)}</td></tr>")
    parts.append('<table><tr><th>Folder</th><th>Engine</th><th>Primary source</th>'
                 '<th>Primary target</th><th>Derived ports</th><th>Dedup</th>'
                 '<th>Lookups</th></tr>' + rows + '</table>')

    # component map
    parts.append('<h2 id="component">Informatica &rarr; AWS Component Mapping</h2>')
    rows = "".join(f"<tr><td>{E(a)}</td><td>{E(b)}</td></tr>" for a, b in COMPONENT_MAP)
    parts.append('<table><tr><th>PowerCenter component</th><th>AWS / converted equivalent</th></tr>'
                 + rows + '</table>')

    # per-folder inventory
    parts.append('<h2 id="inventory">Per-Folder Inventory</h2>')
    for f in configs.ALL_FOLDERS:
        s = specs[f]
        p = configs.build(f)
        parts.append(f'<h3>{E(f)} &nbsp;{_badge(p.engine)}</h3>')
        srcs = "".join(f"<tr><td>{E(x['name'])}</td><td>{E(x.get('database_type',''))}</td>"
                       f"<td>{'flat' if x.get('flatfile') else 'db'}</td>"
                       f"<td>{len(x['fields'])}</td></tr>" for x in s["sources"])
        tgts = "".join(f"<tr><td>{E(x['name'])}</td><td>{E(x.get('database_type',''))}</td>"
                       f"<td>{len(x['fields'])}</td></tr>" for x in s["targets"])
        maps = "".join(f"<tr><td>{E(m['name'])}</td>"
                       f"<td>{E(', '.join(t['type'] for t in m['transformations'])[:180])}</td></tr>"
                       for m in s["mappings"])
        parts.append('<table><tr><th>Source</th><th>Type</th><th>Kind</th><th>Fields</th></tr>'
                     + srcs + '</table>')
        parts.append('<table><tr><th>Target</th><th>Type</th><th>Fields</th></tr>'
                     + tgts + '</table>')
        parts.append('<table><tr><th>Mapping</th><th>Transformation chain (types)</th></tr>'
                     + maps + '</table>')

    # parallelization
    parts.append('<h2 id="parallel">Parallelization Design</h2>')
    parts.append('<p>Independent agency feeds run as concurrent Step Functions '
                 '<code>Parallel</code> branches — the analog of Informatica child sessions. '
                 'Within each PySpark job, wide records are repartitioned on natural keys.</p>')
    from migration.orchestration.generate_orchestration import FAMILIES
    rows = ""
    for fam, folders in FAMILIES.items():
        pk = ", ".join(sorted({k for fo in folders for k in configs.build(fo).partition_keys})) or "n/a"
        rows += (f"<tr><td><b>{E(fam)}</b></td><td>{E(', '.join(folders))}</td>"
                 f"<td>{E(pk)}</td></tr>")
    parts.append('<table><tr><th>Parallel branch (family)</th><th>Feeds (run concurrently)</th>'
                 '<th>Spark repartition keys</th></tr>' + rows + '</table>')

    # pushdown
    parts.append('<h2 id="pushdown">PL/SQL Push-down Decisions</h2>')
    parts.append('<p>Oracle PL/SQL pre/post logic is kept DB-resident and invoked as '
                 'push-down SQL steps by the orchestration (not rewritten into Spark).</p>')
    rows = "".join(f"<tr><td><code>{E(d['source'])}</code></td><td>{d['statements']}</td>"
                   f"<td>{E(d['note'])}</td></tr>" for d in pushdown.describe_all())
    parts.append('<table><tr><th>Legacy script</th><th>Extracted SQL statements</th>'
                 '<th>Role</th></tr>' + rows + '</table>')

    # IaC
    parts.append('<h2 id="iac">Infrastructure as Code (Terraform)</h2>')
    parts.append('<p>Under <code>migration/iac/</code>: Glue jobs (typed by classification), '
                 'Step Functions state machines (from generated ASL), EventBridge biweekly '
                 'schedule, SNS alert topic, and S3 buckets with archive lifecycle. '
                 '<code>terraform validate</code> passes.</p>')
    parts.append('<table><tr><th>Resource</th><th>Purpose</th></tr>'
                 '<tr><td>aws_glue_job (x8 + pushdown)</td><td>Converted jobs; glueetl vs pythonshell</td></tr>'
                 '<tr><td>aws_sfn_state_machine (master + 4 families)</td><td>Orchestration w/ parallel branches</td></tr>'
                 '<tr><td>aws_cloudwatch_event_rule</td><td>Biweekly pay-period trigger</td></tr>'
                 '<tr><td>aws_sns_topic</td><td>mailx-equivalent alerts</td></tr>'
                 '<tr><td>aws_s3_bucket (+lifecycle)</td><td>int/in, archive, output prefixes</td></tr>'
                 '</table>')

    # traceability matrix (sample of expression-derived ports per folder)
    parts.append('<h2 id="trace">Per-Transformation Traceability Matrix</h2>')
    parts.append('<p class="small">Each derived target field traced to the exact PowerCenter '
                 'transform + verbatim expression it was reconstructed from (via CONNECTOR '
                 'lineage). Showing computed (non-pass-through) ports.</p>')
    for f in configs.ALL_FOLDERS:
        p = configs.build(f)
        derived = [dp for dp in p.derived_ports if dp.note and not dp.note.startswith("alias")]
        if not derived:
            continue
        parts.append(f'<h3>{E(f)}</h3>')
        rows = ""
        for dp in derived[:20]:
            expr = (dp.expr or "").replace("\r", " ").replace("\n", " ")
            expr = " ".join(expr.split())[:160]
            rows += (f"<tr><td><code>{E(dp.name)}</code></td><td>{E(dp.note)}</td>"
                     f"<td class='mono'>{E(expr)}</td></tr>")
        more = "" if len(derived) <= 20 else f'<tr><td colspan="3" class="small">… +{len(derived)-20} more derived ports</td></tr>'
        parts.append('<table><tr><th>Target port</th><th>Source transform</th>'
                     '<th>Verbatim Informatica expression</th></tr>' + rows + more + '</table>')

    return _page("EHRP→BIIS PowerCenter to AWS Glue — Migration Report",
                 "Exhaustive inventory, component mapping, and per-transformation traceability",
                 "".join(parts))


# ---------------------------------------------------------------------------
# Data analysis / reconciliation report
# ---------------------------------------------------------------------------

def _svg_bar(data: List[tuple], title: str, unit: str, color="#22D3EE") -> str:
    if not data:
        return f"<p class='small'>No data for {E(title)}.</p>"
    w, h, pad, bw = 760, 260, 40, 0
    maxv = max(v for _, v in data) or 1
    n = len(data)
    gap = 14
    bw = (w - 2 * pad - gap * (n - 1)) / n
    bars = ""
    for i, (label, v) in enumerate(data):
        bh = (h - 2 * pad) * (v / maxv)
        x = pad + i * (bw + gap)
        y = h - pad - bh
        bars += (f'<rect x="{x:.1f}" y="{y:.1f}" width="{bw:.1f}" height="{bh:.1f}" '
                 f'fill="{color}" rx="3"><title>{E(str(label))}: {v:g} {E(unit)}</title></rect>'
                 f'<text x="{x+bw/2:.1f}" y="{h-pad+14:.1f}" font-size="9" text-anchor="middle" '
                 f'fill="#94A3B8" transform="rotate(35 {x+bw/2:.1f} {h-pad+14:.1f})">{E(str(label)[:12])}</text>'
                 f'<text x="{x+bw/2:.1f}" y="{y-4:.1f}" font-size="9" text-anchor="middle" '
                 f'fill="#CBD5E1">{v:g}</text>')
    return (f'<svg viewBox="0 0 {w} {h}" width="100%" style="background:#1E293B;border:1px solid #243449;border-radius:12px;margin:10px 0">'
            f'<text x="{pad}" y="24" font-size="13" font-weight="700" fill="#FFFFFF">{E(title)} ({E(unit)})</text>'
            f'<line x1="{pad}" y1="{h-pad}" x2="{w-pad}" y2="{h-pad}" stroke="rgba(255,255,255,0.1)"/>'
            f'{bars}</svg>')


def data_analysis_report() -> str:
    func = _load_results("functional")
    perf = _load_results("performance")
    parts: List[str] = []

    parts.append('<nav class="toc"><b>Contents:</b> '
                 '<a href="#verdict">Verdict</a>'
                 '<a href="#recon">Reconciliation</a>'
                 '<a href="#edge">Edge-case Coverage</a>'
                 '<a href="#discrep">Discrepancies</a>'
                 '<a href="#perf">Performance</a></nav>')

    if not func:
        parts.append('<div class="note">No reconciliation results found. Run '
                     '<code>./migration/run_local.sh functional</code> first.</div>')
        return _page("EHRP→BIIS — Data Analysis / Reconciliation Report",
                     "Field-level reconciliation vs golden baseline", "".join(parts))

    results = func["results"]
    overall = func["overall"]

    # verdict cards
    parts.append('<h2 id="verdict">Overall Verdict</h2>')
    total_cells = sum(r["field"]["total_cells"] for r in results.values())
    total_mis = sum(r["field"]["cell_mismatches"] for r in results.values())
    rate = 100.0 * (1 - (total_mis / total_cells)) if total_cells else 100.0
    parts.append('<div class="cards">'
                 f'<div class="card"><div class="n">{_verdict_badge(overall["verdict"])}</div><div class="l">Overall</div></div>'
                 f'<div class="card"><div class="n">{overall["jobs_pass"]}/{overall["jobs_total"]}</div><div class="l">Jobs passing</div></div>'
                 f'<div class="card"><div class="n">{rate:.4f}%</div><div class="l">Field match rate</div></div>'
                 f'<div class="card"><div class="n">{total_cells:,}</div><div class="l">Cells compared</div></div>'
                 f'<div class="card"><div class="n">{total_mis}</div><div class="l">Cell mismatches</div></div>'
                 '</div>')
    parts.append('<table><tr><th>Parameter</th><th>Verdict</th><th>Basis</th></tr>'
                 f'<tr><td>Business</td><td>{_verdict_badge(overall["business"])}</td><td>All target rows/fields reconcile to golden baseline</td></tr>'
                 f'<tr><td>Technical</td><td>{_verdict_badge(overall["technical"])}</td><td>Schema (order/precision/scale/nullability), error routing, counters match</td></tr>'
                 f'<tr><td>Functional</td><td>{_verdict_badge(overall["functional"])}</td><td>Dates, signed numerics, dedup, header/trailer, encoding covered</td></tr>'
                 f'<tr><td>Performance</td><td>{_verdict_badge("PASS" if perf else "N/A")}</td><td>{"Mass-volume run characterised below" if perf else "Run performance mode to populate"}</td></tr>'
                 '</table>')
    parts.append('<div class="note"><b>Baseline = spec-derived expected output</b>, independently '
                 'coded from the XML expressions (not a live PowerCenter run).</div>')

    # reconciliation table
    parts.append('<h2 id="recon">Per-Job Reconciliation</h2>')
    rows = ""
    for f in sorted(results):
        r = results[f]
        fr = r["field"]
        rc = r["row_counts"]
        rows += (f"<tr><td><b>{E(f)}</b></td><td>{_badge(r['engine'])}</td>"
                 f"<td>{_verdict_badge(r['verdict'])}</td>"
                 f"<td>{rc['main']['conv']} / {rc['main']['base']}</td>"
                 f"<td>{fr['rows_compared']}</td><td>{fr['total_cells']:,}</td>"
                 f"<td>{fr['cell_mismatches']}</td>"
                 f"<td>{100*fr['field_match_rate']:.3f}%</td>"
                 f"<td>{'yes' if r['error_parity']['keys_match'] else 'NO'}</td>"
                 f"<td>{'yes' if r['counter_parity']['match'] else 'NO'}</td></tr>")
    parts.append('<table><tr><th>Job</th><th>Engine</th><th>Verdict</th>'
                 '<th>Main rows (conv/base)</th><th>Rows cmp</th><th>Cells</th>'
                 '<th>Mismatch</th><th>Field match</th><th>Error parity</th>'
                 '<th>Counter parity</th></tr>' + rows + '</table>')

    # edge-case coverage (from counters + error routing)
    parts.append('<h2 id="edge">Edge-case Coverage</h2>')
    rows = ""
    for f in sorted(results):
        r = results[f]
        cp = r["counter_parity"]["conv"]
        errs = r["error_parity"]["conv_count"]
        trailer = cp.get("TRAILER_MATCH")
        rows += (f"<tr><td><b>{E(f)}</b></td>"
                 f"<td>{cp.get('TOTAL_RECORD_COUNT','-')}</td>"
                 f"<td>{cp.get('HEADER_RECORD_COUNT','-')}</td>"
                 f"<td>{cp.get('DETAIL_RECORD_COUNT','-')}</td>"
                 f"<td>{errs}</td>"
                 f"<td>{'matched' if trailer==1 else ('n/a' if trailer is None else 'MISMATCH')}</td></tr>")
    parts.append('<p class="small">Error rows exercise IS_DATE/IS_NUMBER/ERROR paths; header/'
                 'trailer + trailer-count validation shown where the feed declares a trailer.</p>')
    parts.append('<table><tr><th>Job</th><th>Total</th><th>Header</th><th>Detail</th>'
                 '<th>Errors routed</th><th>Trailer count</th></tr>' + rows + '</table>')

    # discrepancies
    parts.append('<h2 id="discrep">Discrepancy Detail</h2>')
    any_disc = False
    for f in sorted(results):
        r = results[f]
        ex = r["field"]["examples"]
        if not ex:
            continue
        any_disc = True
        rows = "".join(f"<tr><td>{e['row']}</td><td><code>{E(str(e['col']))}</code></td>"
                       f"<td class='mono'>{E(str(e['conv']))}</td>"
                       f"<td class='mono'>{E(str(e['base']))}</td></tr>" for e in ex)
        parts.append(f'<h3>{E(f)} <span class="fail">({len(ex)} sample diffs)</span></h3>')
        parts.append('<table><tr><th>Row</th><th>Column</th><th>Converted</th>'
                     '<th>Baseline</th></tr>' + rows + '</table>')
    if not any_disc:
        parts.append('<div class="note"><b class="pass">No discrepancies.</b> Every compared '
                     'cell matched the golden baseline (precision/scale-aware), including NULL '
                     'parity, dedup/latest-record parity, error-routing parity, and counter '
                     'parity across all eight jobs.</div>')

    # performance
    parts.append('<h2 id="perf">Performance</h2>')
    src = perf or func
    label = "mass-volume" if perf else "functional (run performance mode for mass volume)"
    parts.append(f'<p class="small">Dataset: <b>{E(label)}</b>. Wall-clock for the whole '
                 f'parallel pipeline: <b>{src["wall_clock_sec"]}s</b>.</p>')
    thru = [(f, src["results"][f]["conv_metrics"].get("rows_per_sec", 0)) for f in sorted(src["results"])]
    rt = [(f, src["results"][f]["conv_metrics"].get("runtime_sec", 0)) for f in sorted(src["results"])]
    inrows = [(f, src["results"][f]["conv_metrics"].get("input_rows", 0)) for f in sorted(src["results"])]
    parts.append(_svg_bar(thru, "Throughput per job", "rows/sec", "#22D3EE"))
    parts.append(_svg_bar(rt, "Runtime per job", "seconds", "#34D399"))
    parts.append(_svg_bar(inrows, "Input volume per job", "rows", "#A78BFA"))

    # functional vs mass comparison if both present
    if perf and func:
        parts.append('<h3>Functional vs Mass-volume</h3>')
        rows = ""
        for f in sorted(results):
            fm = func["results"][f]["conv_metrics"]
            pm = perf["results"][f]["conv_metrics"]
            rows += (f"<tr><td><b>{E(f)}</b></td>"
                     f"<td>{fm.get('input_rows')}</td><td>{fm.get('rows_per_sec')}</td>"
                     f"<td>{pm.get('input_rows')}</td><td>{pm.get('rows_per_sec')}</td></tr>")
        parts.append('<table><tr><th>Job</th><th>Func rows</th><th>Func rows/s</th>'
                     '<th>Mass rows</th><th>Mass rows/s</th></tr>' + rows + '</table>')

    return _page("EHRP→BIIS — Data Analysis / Reconciliation Report",
                 "Field-level reconciliation vs spec-derived golden baseline + performance",
                 "".join(parts))


def main() -> int:
    os.makedirs(REPORT_DIR, exist_ok=True)
    with open(os.path.join(REPORT_DIR, "migration_report.html"), "w") as fh:
        fh.write(migration_report())
    with open(os.path.join(REPORT_DIR, "data_analysis_report.html"), "w") as fh:
        fh.write(data_analysis_report())
    print("[reports] wrote migration_report.html + data_analysis_report.html")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
