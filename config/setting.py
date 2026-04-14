"""
config/settings.py
Environment-level configuration for the CMHC card extraction pipeline.
Override these values for different environments (dev/staging/prod).
"""

# Azure Data Lake Storage output path template.
# {username} is substituted at runtime by run_extraction().
ADLS_BASE_PATH = "abfss://sandbox@eppd1stcdp01.dfs.core.windows.net/users/{username}/Extract/"

# Hive source tables
HIVE_CARD_TABLE      = "abc.card"
HIVE_STRUCTURE_TABLE = "abc.structure"
HIVE_PROVINCE_TABLE  = "dfs.provinceorstate"

# Spark Excel library format string
SPARK_EXCEL_FORMAT = "com.crealytics.spark.excel"
