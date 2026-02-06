# Databricks notebook source
from pyspark.sql.functions import current_timestamp, col

# COMMAND ----------

base_batch_path = "/Volumes/workspace/ecomschema/ecom/bronze/batch"
base_delta_path = "/Volumes/workspace/ecomschema/ecom/bronze/delta"

# COMMAND ----------

tables = [f.name.rstrip("/") for f in dbutils.fs.ls(base_batch_path)]

print("Discovered tables:")
for t in tables:
    print(t)


# COMMAND ----------

# DBTITLE 1,Untitled
for table_name in tables:
    print(f"\n===== Processing table: {table_name} =====")

    source_path = f"{base_batch_path}/{table_name}/*.csv"
    delta_path = f"{base_delta_path}/{table_name}"

    df = (
        spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(source_path)
            .withColumn("ingestion_ts", current_timestamp())
            .withColumn("source_file", col("_metadata.file_path"))

    )

    df.write \
        .format("delta") \
        .mode("append") \
        .save(delta_path)

    print(f"Completed table: {table_name}")
