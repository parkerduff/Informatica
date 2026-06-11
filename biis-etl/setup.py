from setuptools import find_packages, setup

setup(
    name="biis-etl",
    version="1.0.0",
    description="BIIS ETL - PySpark migration of Informatica PowerCenter jobs",
    packages=find_packages(
        include=["jobs", "jobs.*", "utils", "utils.*", "transfers", "transfers.*"]
    ),
    python_requires=">=3.9",
)
