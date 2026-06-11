"""Helpers for constructing a SparkSession and parsing common CLI args."""
from __future__ import annotations

import argparse
from typing import TYPE_CHECKING, Optional, Sequence

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import SparkSession


def get_spark(app_name: str = "biis-etl") -> "SparkSession":
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.master("local[*]")
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def base_arg_parser(description: str) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--env", default="test", choices=["test", "prod"])
    parser.add_argument("--run-date", default=None, help="YYYY-MM-DD run date")
    parser.add_argument("--file-path", default=None, help="Input file path")
    return parser


def parse_args(description: str, argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    return base_arg_parser(description).parse_args(argv)
