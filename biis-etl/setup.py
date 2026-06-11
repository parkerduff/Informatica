"""Editable install for the BIIS ETL migration package.

Allows ``from jobs...`` / ``from utils...`` / ``from transfers...`` imports
from anywhere (tests, spark-submit, scripts) after ``pip install -e .``.
"""
from setuptools import setup, find_packages

setup(
    name="biis-etl",
    version="0.1.0",
    description="PySpark migration of the EHRP/BIIS Informatica PowerCenter workflows",
    packages=find_packages(include=["jobs*", "utils*", "transfers*"]),
    include_package_data=True,
    package_data={"jobs.cpm": ["*.json"]},
    python_requires=">=3.11",
)
