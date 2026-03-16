"""
File Utilities

Fixed-width file parser and CSV parser for flat file ingestion.
Replaces all Informatica flat file Source Qualifiers.
"""

import logging

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

logger = logging.getLogger(__name__)


def parse_fixed_width(spark, file_path, field_specs):
    """
    Read a fixed-width text file and extract fields using substring positions.

    Replaces Informatica flat file Source Qualifiers that read COBOL/mainframe
    fixed-width files (e.g., Pseudossn SDA file, LES EMP_REC_TYPE files,
    CPM VSAM files).

    Parameters
    ----------
    spark : SparkSession
    file_path : str
        Path to the fixed-width text file.
    field_specs : list of tuple
        Each tuple is (field_name, start_pos, length, data_type) where:
        - field_name: column name
        - start_pos: 1-based start position in the line
        - length: number of characters
        - data_type: 'string', 'integer', 'long', 'decimal(p,s)', or 'date'

    Returns
    -------
    DataFrame
        DataFrame with extracted and typed columns.
    """
    logger.info("Parsing fixed-width file: %s (%d fields)", file_path, len(field_specs))

    raw_df = spark.read.text(file_path)

    for field_name, start_pos, length, data_type in field_specs:
        raw_df = raw_df.withColumn(
            field_name,
            F.trim(F.substring(F.col("value"), start_pos, length)),
        )

    result_df = raw_df.drop("value")

    for field_name, _, _, data_type in field_specs:
        if data_type == "integer":
            result_df = result_df.withColumn(
                field_name,
                F.col(field_name).cast(IntegerType()),
            )
        elif data_type == "long":
            result_df = result_df.withColumn(
                field_name,
                F.col(field_name).cast(LongType()),
            )
        elif data_type.startswith("decimal"):
            parts = data_type.replace("decimal(", "").replace(")", "").split(",")
            precision = int(parts[0])
            scale = int(parts[1]) if len(parts) > 1 else 0
            result_df = result_df.withColumn(
                field_name,
                F.col(field_name).cast(DecimalType(precision, scale)),
            )

    return result_df


def parse_csv(spark, file_path, schema=None, header=False, delimiter=",",
              quote='"'):
    """
    Read a CSV file with explicit schema.

    Replaces Informatica delimited flat file Source Qualifiers
    (e.g., COMPTIME U0287D01 file from XML/COMPTIME lines 6-27).

    Parameters
    ----------
    spark : SparkSession
    file_path : str
    schema : StructType, optional
        Explicit schema. If None, schema is inferred.
    header : bool
        Whether file has header row.
    delimiter : str
    quote : str

    Returns
    -------
    DataFrame
    """
    logger.info("Parsing CSV file: %s", file_path)

    reader = (
        spark.read.format("csv")
        .option("delimiter", delimiter)
        .option("quote", quote)
        .option("header", str(header).lower())
    )

    if schema is not None:
        reader = reader.schema(schema)
    else:
        reader = reader.option("inferSchema", "true")

    return reader.load(file_path)


def build_csv_schema(field_definitions):
    """
    Build a StructType schema from a list of field definitions.

    Parameters
    ----------
    field_definitions : list of tuple
        Each tuple is (field_name, data_type_str) where data_type_str is
        'string', 'integer', 'long', 'decimal(p,s)', or 'date'.

    Returns
    -------
    StructType
    """
    type_map = {
        "string": StringType(),
        "integer": IntegerType(),
        "long": LongType(),
        "date": DateType(),
    }

    fields = []
    for name, dtype in field_definitions:
        if dtype in type_map:
            spark_type = type_map[dtype]
        elif dtype.startswith("decimal"):
            parts = dtype.replace("decimal(", "").replace(")", "").split(",")
            precision = int(parts[0])
            scale = int(parts[1]) if len(parts) > 1 else 0
            spark_type = DecimalType(precision, scale)
        else:
            spark_type = StringType()
        fields.append(StructField(name, spark_type, nullable=True))

    return StructType(fields)
