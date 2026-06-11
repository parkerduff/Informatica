from setuptools import setup, find_packages

setup(
    name="biis-etl",
    version="1.0.0",
    description="PySpark migration of BIIS Informatica PowerCenter workflows (Pay Calendar, COMPTIME)",
    packages=find_packages(exclude=["tests", "tests.*"]),
    python_requires=">=3.9",
    install_requires=[
        "pyspark>=3.4.0",
        "pyodbc>=5.0",
        "pyyaml>=6.0",
        "Jinja2>=3.1",
        "tabulate>=0.9",
    ],
)
