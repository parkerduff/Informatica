"""Schema primitives shared across the generated schema modules.

These lightweight dataclasses describe the field layouts taken verbatim from the
Informatica PowerCenter source/target definitions. They are deliberately free of
any PySpark import so they can be consumed both by Spark jobs and by plain unit
tests without a Spark session.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class FixedField:
    """A single field in a fixed-width (VSAM-style) flat file.

    ``offset`` and ``length`` are the 0-indexed PHYSICALOFFSET / PHYSICALLENGTH
    from the Informatica source definition. PySpark ``substring`` is 1-indexed,
    so use :meth:`spark_start` when building ``F.substring`` calls.
    """

    name: str
    offset: int
    length: int
    datatype: str = "string"
    precision: int = 0
    scale: int = 0

    @property
    def spark_start(self) -> int:
        return self.offset + 1


@dataclass(frozen=True)
class Column:
    """A relational column in an Oracle source/target table."""

    name: str
    datatype: str = "varchar2"
    precision: int = 0
    scale: int = 0
    nullable: str = "NULL"


def to_spark_schema(fields):
    """Build a PySpark ``StructType`` of all-string fields for a fixed-width layout.

    Imported lazily so this module stays Spark-free for unit tests.
    """
    from pyspark.sql.types import StringType, StructField, StructType

    return StructType([StructField(f.name, StringType(), True) for f in fields])
