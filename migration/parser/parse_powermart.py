#!/usr/bin/env python3
"""
Informatica PowerCenter (.XML / <POWERMART>) metadata parser.

Parses each ``XML/<FOLDER>`` PowerMart export and emits a machine-readable
``migration/spec/<folder>.json`` capturing SOURCE / TARGET / MAPPING /
TRANSFORMATION / WORKFLOW / SESSION metadata.

These JSON specs are the single source of truth that drives:
  * code generation (migration/jobs)
  * the spec-derived golden baseline (migration/baseline)
  * the synthetic data generator (migration/synth)
  * the reconciliation contract (migration/recon)
  * the migration report (migration/reports)

The parser does NOT modify any source artifact -- the XML exports remain the
authoritative spec.
"""
from __future__ import annotations

import json
import os
import sys
import xml.etree.ElementTree as ET
from typing import Any, Dict, List, Optional

# The eight folders in scope for the migration.
FOLDERS = [
    "COMPTIME",
    "CPM_CDC",
    "CPM_NIH",
    "CPM_OIG",
    "EHRP2BIIS_UPDATE",
    "FDA_Leave",
    "Pay_Calendar",
    "Pseudossn",
]

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
XML_DIR = os.path.join(REPO_ROOT, "XML")
SPEC_DIR = os.path.join(REPO_ROOT, "migration", "spec")


def _attrs(el: ET.Element) -> Dict[str, str]:
    """Return element attributes with whitespace-normalised keys."""
    return {k.strip(): v for k, v in el.attrib.items()}


def _field(el: ET.Element) -> Dict[str, Any]:
    a = _attrs(el)
    return {
        "name": a.get("NAME"),
        "datatype": a.get("DATATYPE"),
        "precision": _to_int(a.get("PRECISION")),
        "scale": _to_int(a.get("SCALE")),
        "length": _to_int(a.get("LENGTH")),
        "physicaloffset": _to_int(a.get("PHYSICALOFFSET")),
        "physicallength": _to_int(a.get("PHYSICALLENGTH")),
        "nullable": a.get("NULLABLE"),
        "keytype": a.get("KEYTYPE"),
        "fieldnumber": _to_int(a.get("FIELDNUMBER")),
        "picturetext": a.get("PICTURETEXT"),
        "usage_flags": a.get("USAGE_FLAGS"),
    }


def _to_int(v: Optional[str]) -> Optional[int]:
    if v is None or v == "":
        return None
    try:
        return int(v)
    except ValueError:
        try:
            return int(float(v))
        except ValueError:
            return None


def _flatfile(source_or_target: ET.Element) -> Optional[Dict[str, Any]]:
    ff = source_or_target.find("FLATFILE")
    if ff is None:
        return None
    a = _attrs(ff)
    return {
        "codepage": a.get("CODEPAGE"),
        "delimited": a.get("DELIMITED") == "YES",
        "delimiters": a.get("DELIMITERS"),
        "quote_character": a.get("QUOTE_CHARACTER"),
        "null_character": a.get("NULL_CHARACTER"),
        "nullchartype": a.get("NULLCHARTYPE"),
        "escape_character": a.get("ESCAPE_CHARACTER"),
        "linesequential": a.get("LINESEQUENTIAL") == "YES",
        "skiprows": _to_int(a.get("SKIPROWS")),
        "striptrailingblanks": a.get("STRIPTRAILINGBLANKS") == "YES",
        "rowdelimiter": a.get("ROWDELIMITER"),
        "padbytes": _to_int(a.get("PADBYTES")),
    }


def parse_source(el: ET.Element) -> Dict[str, Any]:
    a = _attrs(el)
    return {
        "name": a.get("NAME"),
        "database_type": a.get("DATABASETYPE"),
        "dbdname": a.get("DBDNAME"),
        "ownername": a.get("OWNERNAME"),
        "flatfile": _flatfile(el),
        "fields": [_field(f) for f in el.findall("SOURCEFIELD")],
    }


def parse_target(el: ET.Element) -> Dict[str, Any]:
    a = _attrs(el)
    return {
        "name": a.get("NAME"),
        "database_type": a.get("DATABASETYPE"),
        "flatfile": _flatfile(el),
        "fields": [_field(f) for f in el.findall("TARGETFIELD")],
    }


def parse_transformation(el: ET.Element) -> Dict[str, Any]:
    a = _attrs(el)
    fields = []
    for tf in el.findall("TRANSFORMFIELD"):
        ta = _attrs(tf)
        fields.append(
            {
                "name": ta.get("NAME"),
                "porttype": ta.get("PORTTYPE"),
                "datatype": ta.get("DATATYPE"),
                "precision": _to_int(ta.get("PRECISION")),
                "scale": _to_int(ta.get("SCALE")),
                "expression": ta.get("EXPRESSION"),
                "expressiontype": ta.get("EXPRESSIONTYPE"),
                "default_value": ta.get("DEFAULTVALUE"),
            }
        )
    table_attrs = {
        _attrs(t).get("NAME"): _attrs(t).get("VALUE")
        for t in el.findall("TABLEATTRIBUTE")
    }
    return {
        "name": a.get("NAME"),
        "type": a.get("TYPE"),
        "reusable": a.get("REUSABLE"),
        "fields": fields,
        "table_attributes": table_attrs,
    }


def parse_mapping(el: ET.Element) -> Dict[str, Any]:
    a = _attrs(el)
    instances = []
    for ins in el.findall("INSTANCE"):
        ia = _attrs(ins)
        instances.append(
            {
                "name": ia.get("NAME"),
                "type": ia.get("TYPE"),
                "transformation_type": ia.get("TRANSFORMATION_TYPE"),
                "transformation_name": ia.get("TRANSFORMATION_NAME"),
                "dbdname": ia.get("DBDNAME"),
            }
        )
    connectors = []
    for c in el.findall("CONNECTOR"):
        ca = _attrs(c)
        connectors.append(
            {
                "from_instance": ca.get("FROMINSTANCE"),
                "from_field": ca.get("FROMFIELD"),
                "to_instance": ca.get("TOINSTANCE"),
                "to_field": ca.get("TOFIELD"),
                "from_instancetype": ca.get("FROMINSTANCETYPE"),
                "to_instancetype": ca.get("TOINSTANCETYPE"),
            }
        )
    transformations = [parse_transformation(t) for t in el.findall("TRANSFORMATION")]
    return {
        "name": a.get("NAME"),
        "instances": instances,
        "connectors": connectors,
        "transformations": transformations,
    }


def _session_component_commands(sess: ET.Element) -> Dict[str, List[str]]:
    """Extract pre/post session commands from SESSTRANSFORMATIONINST / COMPONENT."""
    pre_post: Dict[str, List[str]] = {}
    for comp in sess.iter():
        if comp.tag not in ("COMPONENT", "SESSCOMPONENT"):
            continue
        ca = _attrs(comp)
        reftype = ca.get("REFOBJECTTYPE") or ca.get("TYPE") or comp.tag
        val = ca.get("VALUE")
        if val:
            pre_post.setdefault(reftype, []).append(val)
    return pre_post


def parse_session(sess: ET.Element) -> Dict[str, Any]:
    a = _attrs(sess)
    directories: Dict[str, str] = {}
    filenames: Dict[str, str] = {}
    misc_attrs: Dict[str, str] = {}
    commands: List[str] = []
    for attr in sess.iter("ATTRIBUTE"):
        aa = _attrs(attr)
        n, v = aa.get("NAME"), aa.get("VALUE")
        if not n or not v:
            continue
        low = n.lower()
        if "directory" in low:
            directories[n] = v
        elif "file name" in low:
            filenames[n] = v
        elif "command" in low and v not in ("NO", "YES", ""):
            commands.append(f"{n}: {v}")
        else:
            misc_attrs[n] = v
    # session extensions carry per-connection file names / connection refs
    extensions = []
    for se in sess.iter("SESSIONEXTENSION"):
        sea = _attrs(se)
        ext_attrs = {}
        for attr in se.findall("ATTRIBUTE"):
            aa = _attrs(attr)
            if aa.get("NAME") and aa.get("VALUE"):
                ext_attrs[aa["NAME"]] = aa["VALUE"]
        connections = []
        for cr in se.findall("CONNECTIONREFERENCE"):
            connections.append(_attrs(cr))
        extensions.append(
            {
                "instance_name": sea.get("SINSTANCENAME"),
                "dsq_instance": sea.get("DSQINSTNAME"),
                "type": sea.get("TYPE"),
                "subtype": sea.get("SUBTYPE"),
                "attributes": ext_attrs,
                "connections": connections,
            }
        )
    return {
        "name": a.get("NAME"),
        "mapping_name": a.get("MAPPINGNAME"),
        "directories": directories,
        "filenames": filenames,
        "commands": commands,
        "attributes": misc_attrs,
        "extensions": extensions,
    }


def parse_workflow(wf: ET.Element) -> Dict[str, Any]:
    a = _attrs(wf)
    tasks = []
    for ti in wf.findall("TASKINSTANCE"):
        tasks.append(_attrs(ti))
    links = []
    for wl in wf.findall("WORKFLOWLINK"):
        la = _attrs(wl)
        links.append(
            {
                "from": la.get("FROMTASK"),
                "to": la.get("TOTASK"),
                "condition": la.get("CONDITION"),
            }
        )
    return {
        "name": a.get("NAME"),
        "tasks": tasks,
        "links": links,
    }


def parse_folder(folder: str) -> Dict[str, Any]:
    path = os.path.join(XML_DIR, folder)
    tree = ET.parse(path)
    root = tree.getroot()
    ra = _attrs(root)
    repo_el = root.find("REPOSITORY")
    repo = _attrs(repo_el) if repo_el is not None else {}
    folder_el = repo_el.find("FOLDER") if repo_el is not None else None
    fa = _attrs(folder_el) if folder_el is not None else {}

    scope = folder_el if folder_el is not None else root

    sources = [parse_source(s) for s in scope.findall("SOURCE")]
    targets = [parse_target(t) for t in scope.findall("TARGET")]
    mappings = [parse_mapping(m) for m in scope.findall("MAPPING")]

    workflows = []
    for wf in scope.iter("WORKFLOW"):
        w = parse_workflow(wf)
        w["sessions"] = [parse_session(s) for s in wf.iter("SESSION")]
        workflows.append(w)
    # sessions can also live directly under the folder
    folder_sessions = [
        parse_session(s)
        for s in scope.findall("SESSION")
    ]

    return {
        "folder": folder,
        "creation_date": ra.get("CREATION_DATE"),
        "repository": {
            "name": repo.get("NAME"),
            "codepage": repo.get("CODEPAGE"),
            "database_type": repo.get("DATABASETYPE"),
            "version": repo.get("VERSION"),
        },
        "folder_name": fa.get("NAME"),
        "folder_description": fa.get("DESCRIPTION"),
        "counts": {
            "sources": len(sources),
            "targets": len(targets),
            "mappings": len(mappings),
            "transformations": sum(len(m["transformations"]) for m in mappings),
            "workflows": len(workflows),
        },
        "sources": sources,
        "targets": targets,
        "mappings": mappings,
        "workflows": workflows,
        "folder_sessions": folder_sessions,
    }


def main(argv: List[str]) -> int:
    os.makedirs(SPEC_DIR, exist_ok=True)
    only = argv[1:] if len(argv) > 1 else FOLDERS
    summary = []
    for folder in only:
        spec = parse_folder(folder)
        out = os.path.join(SPEC_DIR, f"{folder}.json")
        with open(out, "w", encoding="utf-8") as fh:
            json.dump(spec, fh, indent=2, ensure_ascii=False)
        c = spec["counts"]
        summary.append((folder, c))
        print(
            f"[parse] {folder:18s} sources={c['sources']} targets={c['targets']} "
            f"mappings={c['mappings']} transforms={c['transformations']} -> {out}"
        )
    # index file
    with open(os.path.join(SPEC_DIR, "index.json"), "w", encoding="utf-8") as fh:
        json.dump(
            {"folders": [f for f, _ in summary], "counts": {f: c for f, c in summary}},
            fh,
            indent=2,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
