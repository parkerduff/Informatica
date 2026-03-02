"""
Informatica PowerCenter to PySpark Migration Package.

Migrates 6 HHS ETL workflows from Informatica PowerCenter 9.6.1 to PySpark:
  Job 1: Pay_Calendar - Pay period management
  Job 2: COMPTIME - Compensatory time processing
  Job 3: Pseudossn - PseudoSSN management from SDA files
  Job 4: FDA_Leave - FDA leave validation
  Job 5: CPM_NIH / CPM_CDC - Agency payroll extracts
  Job 6: EHRP2BIIS_UPDATE - Core HR integration pipeline
"""

__version__ = "1.0.0"
