# Databricks notebook source
#installation
%pip install databricks-labs-dqx

# COMMAND ----------

#restart python
dbutils.library.restartPython()

# COMMAND ----------

# 04_bronze_to_silver
# DQX profiling + validation + cleaning
# Writes clean data to Silver and failed records to quarantine

from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx import check_funcs
from databricks.labs.dqx.rule import DQRowRule
from databricks.sdk import WorkspaceClient
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from datetime import datetime

dbutils.widgets.text("catalog_name", "workspace")
dbutils.widgets.text("bronze_schema", "bronze")
dbutils.widgets.text("silver_schema", "silver")
dbutils.widgets.text("metadata_schema", "metadata")
dbutils.widgets.text("table_name", "")

catalog_name    = dbutils.widgets.get("catalog_name")
bronze_schema   = dbutils.widgets.get("bronze_schema")
silver_schema   = dbutils.widgets.get("silver_schema")
metadata_schema = dbutils.widgets.get("metadata_schema")
table_name      = dbutils.widgets.get("table_name")

print(f"Processing: {table_name}")

# COMMAND ----------

#Step 1: Read from Bronze

bronze_path = f"{catalog_name}.{bronze_schema}.{table_name.lower()}"
df = spark.read.table(bronze_path)
total_count = df.count()

print(f"Read {total_count} rows from Bronze: {bronze_path}")

# COMMAND ----------

#Step 2: Data Profiling
# Analyse Bronze for nulls, dupes, type issues

from pyspark.sql import functions as F

print(f"\n DATA PROFILING REPORT — {table_name}")
print("=" * 50)

# Null counts per column
print("\n Null Value Analysis:")
for col in df.columns:
    null_count = df.filter(F.col(col).isNull()).count()
    pct = round((null_count / total_count) * 100, 2) if total_count > 0 else 0
    print(f"  {col}: {null_count} nulls ({pct}%)")

# Duplicate records
dupe_count = total_count - df.dropDuplicates().count()
print(f"\n Duplicate Records: {dupe_count}")

# Column uniqueness (first column assumed primary key)
pk_col = df.columns[0]
unique_count = df.select(pk_col).distinct().count()
print(f"\n Unique values in {pk_col}: {unique_count} / {total_count}")

print("\n Profiling complete")

# COMMAND ----------

from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx import check_funcs
from databricks.labs.dqx.rule import DQRowRule

rules_map = {
    "Album": [
        DQRowRule(criticality="error", check_func=check_funcs.is_not_null, column="AlbumId"),
        DQRowRule(criticality="error", check_func=check_funcs.is_not_null, column="Title"),
        DQRowRule(criticality="error", check_func=check_funcs.is_not_null, column="ArtistId")
    ],
    "Artist": [
        DQRowRule(criticality="error", check_func=check_funcs.is_not_null, column="ArtistId"),
        DQRowRule(criticality="error", check_func=check_funcs.is_not_null, column="Name")
    ],
    "Customer": [
        DQRowRule(criticality="error", check_func=check_funcs.is_not_null, column="CustomerId"),
        DQRowRule(criticality="error", check_func=check_funcs.is_not_null, column="FirstName"),
        DQRowRule(criticality="error", check_func=check_funcs.is_not_null, column="LastName"),
        DQRowRule(criticality="error", check_func=check_funcs.is_not_null, column="Email")
    ],
    "Employee": [
        DQRowRule(criticality="error", check_func=check_funcs.is_not_null, column="EmployeeId"),
        DQRowRule(criticality="error", check_func=check_funcs.is_not_null, column="FirstName"),
        DQRowRule(criticality="error", check_func=check_funcs.is_not_null, column="LastName")
    ],
    "Genre": [
        DQRowRule(criticality="error", check_func=check_funcs.is_not_null, column="GenreId"),
        DQRowRule(criticality="error", check_func=check_funcs.is_not_null, column="Name")
    ],
    "Invoice": [
        DQRowRule(criticality="error", check_func=check_funcs.is_not_null, column="InvoiceId"),
        DQRowRule(criticality="error", check_func=check_funcs.is_not_null, column="CustomerId"),
        DQRowRule(criticality="error", check_func=check_funcs.is_not_null, column="InvoiceDate"),
        DQRowRule(criticality="error", check_func=check_funcs.is_not_null, column="Total")
    ],
    "InvoiceLine": [
        DQRowRule(criticality="error", check_func=check_funcs.is_not_null, column="InvoiceLineId"),
        DQRowRule(criticality="error", check_func=check_funcs.is_not_null, column="InvoiceId"),
        DQRowRule(criticality="error", check_func=check_funcs.is_not_null, column="TrackId"),
        DQRowRule(criticality="error", check_func=check_funcs.is_not_less_than, column="Quantity", check_func_kwargs={"limit": 1}),
        DQRowRule(criticality="error", check_func=check_funcs.is_not_less_than, column="UnitPrice", check_func_kwargs={"limit": 0})
    ],
    "MediaType": [
        DQRowRule(criticality="error", check_func=check_funcs.is_not_null, column="MediaTypeId"),
        DQRowRule(criticality="error", check_func=check_funcs.is_not_null, column="Name")
    ],
    "Playlist": [
        DQRowRule(criticality="error", check_func=check_funcs.is_not_null, column="PlaylistId"),
        DQRowRule(criticality="error", check_func=check_funcs.is_not_null, column="Name")
    ],
    "PlaylistTrack": [
        DQRowRule(criticality="error", check_func=check_funcs.is_not_null, column="PlaylistId"),
        DQRowRule(criticality="error", check_func=check_funcs.is_not_null, column="TrackId")
    ],
    "Track": [
        DQRowRule(criticality="error", check_func=check_funcs.is_not_null, column="TrackId"),
        DQRowRule(criticality="error", check_func=check_funcs.is_not_null, column="Name"),
        DQRowRule(criticality="error", check_func=check_funcs.is_not_null, column="AlbumId"),
        DQRowRule(criticality="error", check_func=check_funcs.is_not_null, column="MediaTypeId"),
        DQRowRule(criticality="error", check_func=check_funcs.is_not_less_than, column="Milliseconds", check_func_kwargs={"limit": 0}),
        DQRowRule(criticality="error", check_func=check_funcs.is_not_less_than, column="UnitPrice", check_func_kwargs={"limit": 0})
    ]
}

rules = rules_map.get(table_name, [])
print(f"Loaded {len(rules)} DQX rules for {table_name}")

# COMMAND ----------

#Step 4: Run DQX Validation

from datetime import datetime

from databricks.sdk import WorkspaceClient

ws = WorkspaceClient()
dqx = DQEngine(workspace_client=ws, spark=spark)

# apply_checks_and_split returns (valid_df, invalid_df)
passed_df, failed_df = dqx.apply_checks_and_split(df, rules)

passed_count = passed_df.count()
failed_count  = failed_df.count()

print(f"\n DQX Validation Results — {table_name}")
print(f"Total: {total_count}")
print(f"Passed: {passed_count}")
print(f"Failed: {failed_count}")

# COMMAND ----------

#Step 5: Write failed records to quarantine
# Failed records stored for review and not loaded to Silver

if failed_count > 0:
    quarantine_path = f"{catalog_name}.{metadata_schema}.quarantine_{table_name.lower()}"
    
    failed_df.withColumn("quarantine_time", F.lit(datetime.now())) \
             .write.format("delta") \
             .mode("append") \
             .option("overwriteSchema", "true") \
             .saveAsTable(quarantine_path)
    
    print(f" {failed_count} failed records written to {quarantine_path}")
else:
    print(" No failed records — quarantine not needed")

# COMMAND ----------

#Step 6: Log DQX execution metrics

from pyspark.sql import Row

dqx_log = spark.createDataFrame([Row(
    table_name      = table_name,
    execution_time  = datetime.now(),
    total_records   = total_count,
    passed_records  = passed_count,
    failed_records  = failed_count,
    created_date    = datetime.now().date()
)])

dqx_log.write.format("delta") \
    .mode("append") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.{metadata_schema}.dqx_execution_log")

print(f"DQX metrics logged")

# COMMAND ----------

#Step 7: Apply cleaning to passed records
# Trim strings, handle nulls, standardize

from pyspark.sql.types import StringType

# Trim all string columns
string_cols = [f.name for f in passed_df.schema.fields if isinstance(f.dataType, StringType)]
cleaned_df  = passed_df

for col in string_cols:
    cleaned_df = cleaned_df.withColumn(col, F.trim(F.col(col)))

# Replace empty strings with null
for col in string_cols:
    cleaned_df = cleaned_df.withColumn(
        col, F.when(F.col(col) == "", None).otherwise(F.col(col))
    )

print(f"Cleaning applied to {len(string_cols)} string columns")

# COMMAND ----------

#Step 8: Write cleaned data to Silver

silver_path = f"{catalog_name}.{silver_schema}.{table_name.lower()}"

cleaned_df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(silver_path)

silver_count = spark.read.table(silver_path).count()
print(f"Written {silver_count} rows to Silver: {silver_path}")

# COMMAND ----------

#Step 9: Fail if too many records rejected
# Stop job if >5% records failed DQX

fail_pct = (failed_count / total_count * 100) if total_count > 0 else 0

if fail_pct > 5:
    raise Exception(
        f"DQX failure rate {round(fail_pct, 2)}% exceeds 5% threshold for {table_name}. Check quarantine table."
    )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS workspace.metadata.dqx_execution_log (
# MAGIC     table_name      STRING,
# MAGIC     execution_time  TIMESTAMP,
# MAGIC     total_records   BIGINT,
# MAGIC     passed_records  BIGINT,
# MAGIC     failed_records  BIGINT,
# MAGIC     created_date    DATE
# MAGIC );

# COMMAND ----------

#verification

print(f" Silver table preview — {table_name}:")
display(spark.sql(f"SELECT * FROM {catalog_name}.{silver_schema}.{table_name.lower()} LIMIT 5"))

print(f"\nDQX Execution Log:")
display(spark.sql(f"SELECT * FROM {catalog_name}.{metadata_schema}.dqx_execution_log ORDER BY execution_time DESC"))

print(f"\nAll Silver tables:")
display(spark.sql(f"SHOW TABLES IN {catalog_name}.{silver_schema}"))
