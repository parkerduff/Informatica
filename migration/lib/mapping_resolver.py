#!/usr/bin/env python3
"""
mapping_resolver -- reconstruct a mapping's data lineage from CONNECTOR edges.

PowerCenter renames ports between transformation instances via CONNECTORs
(e.g. SQ.PSEUDO_SSN -> exp.PSEUDOSSN).  To convert a mapping faithfully we
trace every TARGET field backwards through the connector graph to either:

  * a SOURCE / Source Qualifier field  -> a pass-through column, or
  * a non-trivial Expression output port -> a derived transformation (whose
    verbatim Informatica expression we carry forward for codegen / traceability).

This makes the converted jobs and the golden baseline share one, automatically
derived field map instead of hundreds of hand-written mappings.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple


def _instance_index(mapping: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    return {i["name"]: i for i in mapping["instances"]}


def _transform_index(mapping: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    return {t["name"]: t for t in mapping["transformations"]}


def _incoming_index(mapping: Dict[str, Any]) -> Dict[Tuple[str, str], Tuple[str, str]]:
    """(to_instance, to_field) -> (from_instance, from_field)."""
    idx: Dict[Tuple[str, str], Tuple[str, str]] = {}
    for c in mapping["connectors"]:
        idx[(c["to_instance"], c["to_field"])] = (c["from_instance"], c["from_field"])
    return idx


def _port(transform: Dict[str, Any], name: str) -> Optional[Dict[str, Any]]:
    for f in transform["fields"]:
        if f["name"] == name:
            return f
    return None


def _is_passthrough(expr: Optional[str], name: str) -> bool:
    if expr is None:
        return True
    e = expr.strip()
    return e == "" or e == name or e.isidentifier()


TERMINAL_TYPES = {"Source Qualifier", "Source Definition", "Normalizer", "Joiner"}


def _base_name(name: str) -> str:
    import re
    return re.sub(r"\d+$", "", name)


def _name_variants(name: str):
    import re
    seen = []
    base = re.sub(r"\d+$", "", name)
    if base != name:
        seen.append(base)
    for pref in ("in_", "i_", "o_", "v_"):
        if name.startswith(pref):
            seen.append(name[len(pref):])
    return seen


def resolve_target(mapping: Dict[str, Any], target_instance: str) -> Dict[str, Dict[str, Any]]:
    """Return {target_field: lineage} for one target instance.

    lineage = {'kind': 'source'|'expr'|'unresolved',
               'source_field': str,               # for kind == 'source'
               'transform': str, 'port': str,      # for kind == 'expr'
               'expression': str}
    """
    instances = _instance_index(mapping)
    transforms = _transform_index(mapping)
    incoming = _incoming_index(mapping)

    result: Dict[str, Dict[str, Any]] = {}
    for (to_inst, to_field), (from_inst, from_field) in incoming.items():
        if to_inst != target_instance:
            continue
        result[to_field] = _trace(from_inst, from_field, instances, transforms, incoming)
    return result


def _trace(inst_name, field_name, instances, transforms, incoming, depth=0):
    if depth > 200:
        return {"kind": "unresolved", "note": "cycle/limit"}
    inst = instances.get(inst_name)
    ttype = (inst or {}).get("transformation_type") or ""
    itype = (inst or {}).get("type") or ""

    # reached a source-side instance -> pass-through column
    if ttype in TERMINAL_TYPES or itype == "SOURCE":
        return {"kind": "source", "source_field": field_name,
                "via": inst_name}

    transform = transforms.get(inst_name)
    if transform is not None:
        port = _port(transform, field_name)
        expr = port.get("expression") if port else None
        if port is not None and not _is_passthrough(expr, field_name):
            return {"kind": "expr", "transform": inst_name, "port": field_name,
                    "expression": expr,
                    "transform_type": transform.get("type")}
        # pass-through: follow the input feeding this port
        nxt = incoming.get((inst_name, field_name))
        if nxt is not None:
            return _trace(nxt[0], nxt[1], instances, transforms, incoming, depth + 1)
        # port with an expression referencing a differently-named single input
        if port is not None and expr and expr.strip().isidentifier() and expr.strip() != field_name:
            nxt = incoming.get((inst_name, expr.strip()))
            if nxt is not None:
                return _trace(nxt[0], nxt[1], instances, transforms, incoming, depth + 1)
        # Router / Filter / Update Strategy rename output ports with a group
        # suffix (PSEUDO_SSN -> PSEUDO_SSN1); strip it and follow the input.
        for alt in _name_variants(field_name):
            nxt = incoming.get((inst_name, alt))
            if nxt is not None:
                return _trace(nxt[0], nxt[1], instances, transforms, incoming, depth + 1)
        return {"kind": "source", "source_field": _base_name(field_name),
                "via": inst_name, "note": "leaf"}

    # unknown instance, follow incoming if present
    nxt = incoming.get((inst_name, field_name))
    if nxt is not None:
        return _trace(nxt[0], nxt[1], instances, transforms, incoming, depth + 1)
    return {"kind": "source", "source_field": field_name, "via": inst_name}


def input_aliases(mapping: Dict[str, Any], transform_name: str) -> Dict[str, Dict[str, Any]]:
    """For every INPUT port of ``transform_name``, resolve the upstream lineage.

    Returns {input_port: lineage} so an expression referencing a renamed input
    port (e.g. exp reads ``PSEUDOSSN`` fed from source ``PSEUDO_SSN``) can be
    aliased back to a source column / prior derived port.
    """
    instances = _instance_index(mapping)
    transforms = _transform_index(mapping)
    incoming = _incoming_index(mapping)
    transform = transforms.get(transform_name)
    aliases: Dict[str, Dict[str, Any]] = {}
    if transform is None:
        return aliases
    for f in transform["fields"]:
        pt = (f.get("porttype") or "").upper()
        if "INPUT" not in pt:
            continue
        nxt = incoming.get((transform_name, f["name"]))
        if nxt is not None:
            aliases[f["name"]] = _trace(nxt[0], nxt[1], instances, transforms, incoming)
    return aliases


def derived_ports(mapping: Dict[str, Any], transform_name: str):
    """Ordered (name, expression) for VARIABLE + OUTPUT ports of a transform.

    Pure pass-through ports are skipped (handled by the field map); variable and
    computed output ports are emitted verbatim for evaluation.
    """
    transforms = _transform_index(mapping)
    transform = transforms.get(transform_name)
    out = []
    if transform is None:
        return out
    for f in transform["fields"]:
        pt = (f.get("porttype") or "").upper()
        expr = f.get("expression")
        name = f["name"]
        if "VARIABLE" in pt or (not _is_passthrough(expr, name) and "OUTPUT" in pt):
            out.append((name, expr))
    return out


def topo_order(mapping: Dict[str, Any]) -> Dict[str, int]:
    """Approximate topological rank of each instance from CONNECTOR edges."""
    succ: Dict[str, set] = {}
    indeg: Dict[str, int] = {}
    nodes = {i["name"] for i in mapping["instances"]}
    for n in nodes:
        succ.setdefault(n, set())
        indeg.setdefault(n, 0)
    for c in mapping["connectors"]:
        a, b = c["from_instance"], c["to_instance"]
        if a == b or a not in nodes or b not in nodes:
            continue
        if b not in succ[a]:
            succ[a].add(b)
            indeg[b] += 1
    order: Dict[str, int] = {}
    queue = sorted([n for n in nodes if indeg[n] == 0])
    rank = 0
    while queue:
        nxt = []
        for n in queue:
            order[n] = rank
            for m in succ[n]:
                indeg[m] -= 1
                if indeg[m] == 0:
                    nxt.append(m)
        queue = sorted(nxt)
        rank += 1
    for n in nodes:  # cycles / leftovers
        order.setdefault(n, rank)
    return order


def field_map(mapping: Dict[str, Any], target_instance: str) -> Dict[str, str]:
    """Simplified {target_field: source_field} for pass-through lineage."""
    out: Dict[str, str] = {}
    for tf, lin in resolve_target(mapping, target_instance).items():
        if lin["kind"] == "source":
            out[tf] = lin["source_field"]
        elif lin["kind"] == "expr":
            out[tf] = lin["port"]
    return out
