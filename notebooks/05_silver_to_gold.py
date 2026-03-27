# Databricks notebook source
#installation
%pip install databricks-labs-dqx

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

#Widgets + Imports
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from datetime import datetime

dbutils.widgets.text("catalog_name", "workspace")
dbutils.widgets.text("silver_schema", "silver")
dbutils.widgets.text("gold_schema", "gold")

catalog_name  = dbutils.widgets.get("catalog_name")
silver_schema = dbutils.widgets.get("silver_schema")
gold_schema   = dbutils.widgets.get("gold_schema")

print(f"Building Gold layer")

# COMMAND ----------

#dim_artist
artist_df = spark.read.table(f"{catalog_name}.{silver_schema}.artist")

dim_artist = artist_df.select(
    F.col("ArtistId").alias("artist_id"),
    F.col("Name").alias("artist_name")
).withColumn("artist_key", F.monotonically_increasing_id())

dim_artist.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.{gold_schema}.dim_artist")

print(f"dim_artist: {dim_artist.count()} rows")

# COMMAND ----------

# dim_genre
genre_df = spark.read.table(f"{catalog_name}.{silver_schema}.genre")

dim_genre = genre_df.select(
    F.col("GenreId").alias("genre_id"),
    F.col("Name").alias("genre_name")
).withColumn("genre_key", F.monotonically_increasing_id())

dim_genre.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.{gold_schema}.dim_genre")

print(f"dim_genre: {dim_genre.count()} rows")

# COMMAND ----------

#dim_mediatype
mediatype_df = spark.read.table(f"{catalog_name}.{silver_schema}.mediatype")

dim_mediatype = mediatype_df.select(
    F.col("MediaTypeId").alias("media_type_id"),
    F.col("Name").alias("media_type_name")
).withColumn("media_type_key", F.monotonically_increasing_id())

dim_mediatype.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.{gold_schema}.dim_mediatype")

print(f"dim_mediatype: {dim_mediatype.count()} rows")

# COMMAND ----------

# dim_employee
employee_df = spark.read.table(f"{catalog_name}.{silver_schema}.employee")

dim_employee = employee_df.select(
    F.col("EmployeeId").alias("employee_id"),
    F.col("FirstName").alias("first_name"),
    F.col("LastName").alias("last_name"),
    F.col("Title").alias("title"),
    F.col("ReportsTo").alias("reports_to"),
    F.col("City").alias("city"),
    F.col("Country").alias("country"),
    F.col("Email").alias("email")
).withColumn("employee_key", F.monotonically_increasing_id())

dim_employee.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.{gold_schema}.dim_employee")

print(f"dim_employee: {dim_employee.count()} rows")

# COMMAND ----------

#dim_customer (SCD Type 2)
from delta.tables import DeltaTable

customer_df = spark.read.table(f"{catalog_name}.{silver_schema}.customer")

incoming = customer_df.select(
    F.col("CustomerId").alias("customer_id"),
    F.col("FirstName").alias("first_name"),
    F.col("LastName").alias("last_name"),
    F.col("Company").alias("company"),
    F.col("Address").alias("address"),
    F.col("City").alias("city"),
    F.col("State").alias("state"),
    F.col("Country").alias("country"),
    F.col("PostalCode").alias("postal_code"),
    F.col("Phone").alias("phone"),
    F.col("Email").alias("email"),
    F.col("SupportRepId").alias("support_rep_id")
)

gold_table_path = f"{catalog_name}.{gold_schema}.dim_customer"
table_exists = spark.catalog.tableExists(gold_table_path)

if not table_exists:
    dim_customer_init = incoming \
        .withColumn("customer_key", F.monotonically_increasing_id()) \
        .withColumn("effective_start_date", F.current_date()) \
        .withColumn("effective_end_date", F.lit(None).cast("date")) \
        .withColumn("is_current", F.lit(True))

    dim_customer_init.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(gold_table_path)

    print(f"dim_customer created (first run): {dim_customer_init.count()} rows")

else:
    existing = DeltaTable.forName(spark, gold_table_path)
    existing_df = spark.read.table(gold_table_path).filter(F.col("is_current") == True)

    changed = incoming.alias("new").join(
        existing_df.alias("old"), on="customer_id", how="inner"
    ).filter(
        (F.col("new.first_name")  != F.col("old.first_name"))  |
        (F.col("new.last_name")   != F.col("old.last_name"))   |
        (F.col("new.email")       != F.col("old.email"))       |
        (F.col("new.address")     != F.col("old.address"))     |
        (F.col("new.city")        != F.col("old.city"))        |
        (F.col("new.country")     != F.col("old.country"))
    ).select("new.customer_id")

    changed_ids = [row.customer_id for row in changed.collect()]
    print(f"{len(changed_ids)} customers changed")

    existing.update(
        condition=F.col("customer_id").isin(changed_ids) & (F.col("is_current") == True),
        set={"is_current": F.lit(False), "effective_end_date": F.current_date()}
    )

    new_records = incoming.filter(F.col("customer_id").isin(changed_ids)) \
        .withColumn("customer_key", F.monotonically_increasing_id()) \
        .withColumn("effective_start_date", F.current_date()) \
        .withColumn("effective_end_date", F.lit(None).cast("date")) \
        .withColumn("is_current", F.lit(True))

    new_records.write.format("delta").mode("append").saveAsTable(gold_table_path)

    new_customers = incoming.join(existing_df, on="customer_id", how="left_anti") \
        .withColumn("customer_key", F.monotonically_increasing_id()) \
        .withColumn("effective_start_date", F.current_date()) \
        .withColumn("effective_end_date", F.lit(None).cast("date")) \
        .withColumn("is_current", F.lit(True))

    new_customers.write.format("delta").mode("append").saveAsTable(gold_table_path)

    total = spark.read.table(gold_table_path).count()
    print(f"dim_customer updated: {total} total rows")

# COMMAND ----------

#fact_sales
invoice_df     = spark.read.table(f"{catalog_name}.{silver_schema}.invoice")
invoiceline_df = spark.read.table(f"{catalog_name}.{silver_schema}.invoiceline")
track_df       = spark.read.table(f"{catalog_name}.{silver_schema}.track")
dim_customer_df = spark.read.table(f"{catalog_name}.{gold_schema}.dim_customer") \
                      .filter(F.col("is_current") == True)

fact_sales = invoiceline_df.alias("il") \
    .join(invoice_df.alias("i"), "InvoiceId", "inner") \
    .join(track_df.alias("t"), "TrackId", "inner") \
    .join(dim_customer_df.alias("dc"),
          F.col("i.CustomerId") == F.col("dc.customer_id"), "inner") \
    .select(
        F.col("il.InvoiceLineId").alias("invoice_line_id"),
        F.col("i.InvoiceId").alias("invoice_id"),
        F.col("dc.customer_key"),
        F.col("i.CustomerId").alias("customer_id"),
        F.col("il.TrackId").alias("track_id"),
        F.col("t.AlbumId").alias("album_id"),
        F.col("t.GenreId").alias("genre_id"),
        F.col("t.MediaTypeId").alias("media_type_id"),
        F.col("i.InvoiceDate").alias("invoice_date"),
        F.col("il.UnitPrice").alias("unit_price"),
        F.col("il.Quantity").alias("quantity"),
        (F.col("il.UnitPrice") * F.col("il.Quantity")).alias("total_amount"),
        F.col("i.BillingCountry").alias("billing_country"),
        F.col("i.BillingCity").alias("billing_city")
    )

fact_sales.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.{gold_schema}.fact_sales")

print(f"fact_sales: {fact_sales.count()} rows")

# COMMAND ----------

#fact_sales_customer_agg (built from fact_sales only)
fact_sales_df = spark.read.table(f"{catalog_name}.{gold_schema}.fact_sales")

fact_sales_customer_agg = fact_sales_df.groupBy("customer_key", "customer_id") \
    .agg(
        F.count("invoice_line_id").alias("total_purchases"),
        F.sum("total_amount").alias("total_spend"),
        F.avg("total_amount").alias("avg_order_value"),
        F.min("invoice_date").alias("first_purchase_date"),
        F.max("invoice_date").alias("last_purchase_date"),
        F.countDistinct("invoice_id").alias("total_invoices")
    )

fact_sales_customer_agg.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.{gold_schema}.fact_sales_customer_agg")

print(f"fact_sales_customer_agg: {fact_sales_customer_agg.count()} rows")

# COMMAND ----------

# Final verification: all gold tables
gold_tables = [
    "dim_artist", "dim_genre", "dim_mediatype",
    "dim_employee", "dim_customer",
    "fact_sales", "fact_sales_customer_agg"
]

print("\nGold Layer Summary:")
for table in gold_tables:
    count = spark.read.table(f"{catalog_name}.{gold_schema}.{table}").count()
    print(f"{table}: {count} rows")

print("\nAll Gold tables:")
display(spark.sql(f"SHOW TABLES IN {catalog_name}.{gold_schema}"))

print("\ndim_customer (SCD Type 2 — check is_current):")
display(spark.sql(f"SELECT * FROM {catalog_name}.{gold_schema}.dim_customer LIMIT 10"))

print("\nfact_sales preview:")
display(spark.sql(f"SELECT * FROM {catalog_name}.{gold_schema}.fact_sales LIMIT 10"))

print("\nfact_sales_customer_agg preview:")
display(spark.sql(f"SELECT * FROM {catalog_name}.{gold_schema}.fact_sales_customer_agg LIMIT 10"))

print("\ndim_artist preview:")
display(spark.sql(f"SELECT * FROM {catalog_name}.{gold_schema}.dim_artist LIMIT 5"))

print("\ndim_employee preview:")
display(spark.sql(f"SELECT * FROM {catalog_name}.{gold_schema}.dim_employee LIMIT 5"))