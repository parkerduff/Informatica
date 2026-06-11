from setuptools import find_packages, setup

setup(
    name="biis-etl",
    version="1.0.0",
    description="BIIS ETL — Informatica PowerCenter to PySpark migration",
    packages=find_packages(include=["jobs*", "utils*", "transfers*"]),
    python_requires=">=3.10",
)
