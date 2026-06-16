"""Reusable transformation building blocks shared by the PySpark jobs.

Every transform is implemented twice:

* a pure-Python function operating on a single ``str`` value, so the parsing
  rules can be unit-tested without a Spark session, and
* a thin PySpark ``Column`` helper (suffixed ``_col``) that wraps the Python
  function in a UDF for use inside DataFrame pipelines.

The semantics mirror the Informatica expression transformations exactly,
including the "return NULL / 0 on bad input" behaviour.
"""
