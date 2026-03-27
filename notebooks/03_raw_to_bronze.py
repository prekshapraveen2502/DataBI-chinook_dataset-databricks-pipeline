# Databricks notebook source
# 03_raw_to_bronze
# Reads latest Parquet from Raw volume using execution_log
# Writes to Bronze Delta tables the current day snapshot
# No transformations and exact copy of Raw

dbutils.widgets.text("catalog_name", "workspace")
dbutils.widgets.text("metadata_schema", "metadata")
dbutils.widgets.text("bronze_schema", "bronze")
dbutils.widgets.text("table_name", "")

catalog_name    = dbutils.widgets.get("catalog_name")
metadata_schema = dbutils.widgets.get("metadata_schema")
bronze_schema   = dbutils.widgets.get("bronze_schema")
table_name      = dbutils.widgets.get("table_name")

print(f"Processing: {table_name}")

# COMMAND ----------

# Step 1: Get latest file_location from execution_log
# Always read from the most recent successful run

latest_log = spark.sql(f"""
    SELECT file_location
    FROM {catalog_name}.{metadata_schema}.execution_log
    WHERE table_name = '{table_name}'
    AND status = 'SUCCESS'
    ORDER BY execution_time DESC
    LIMIT 1
""")

if latest_log.count() == 0:
    raise Exception(f"No successful raw file found for {table_name} in execution_log")

file_location = latest_log.collect()[0]["file_location"]
print(f"Reading from: {file_location}")

# COMMAND ----------

#Step 2: Read Parquet from Raw volume (chinook)

df = spark.read.parquet(file_location)
raw_count = df.count()

print(f"Read {raw_count} rows from Raw")

# COMMAND ----------

#Step 3: Write to Bronze as Delta
# Overwrite mode: Bronze always holds current day's snapshot
# Zero transformations with exact copy of Raw

bronze_path = f"{catalog_name}.{bronze_schema}.{table_name.lower()}"

df.write.format("delta") \
  .mode("overwrite") \
  .option("overwriteSchema", "true") \
  .saveAsTable(bronze_path)

print(f"Written to Bronze: {bronze_path}")

# COMMAND ----------

#Step 4: Validate row counts

bronze_count = spark.read.table(bronze_path).count()

if raw_count == bronze_count:
    print(f"Row count validated: {raw_count} rows")
else:
    raise Exception(f"Mismatch! Raw: {raw_count} | Bronze: {bronze_count}")

# COMMAND ----------

#verification

print(f"Bronze table preview — {table_name}:")
display(spark.sql(f"SELECT * FROM {catalog_name}.{bronze_schema}.{table_name.lower()} LIMIT 5"))

print(f"\nBronze row count: {bronze_count}")

print(f"\nAll Bronze tables:")
display(spark.sql(f"SHOW TABLES IN {catalog_name}.{bronze_schema}"))
