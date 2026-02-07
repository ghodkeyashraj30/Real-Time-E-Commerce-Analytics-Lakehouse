# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.functions import current_timestamp, col
from delta.tables import DeltaTable

# COMMAND ----------

# Paths
base_batch_path = "/Volumes/workspace/ecomschema/ecom/bronze/batch"
base_delta_path = "/Volumes/workspace/ecomschema/ecom/bronze/delta"
checkpoint_base_path = "/Volumes/workspace/ecomschema/ecom/bronze/_checkpoints"

# COMMAND ----------

# Discover tables dynamically
tables = [f.name.rstrip("/") for f in dbutils.fs.ls(base_batch_path)]

print("Discovered tables:")
for t in tables:
    print(t)

# COMMAND ----------

# DBTITLE 1,Untitled
for table_name in tables:
    print(f"\n===== Auto Loader processing table: {table_name} =====")

    source_path = f"{base_batch_path}/{table_name}"
    target_path = f"{base_delta_path}/{table_name}"
    checkpoint_path = f"{checkpoint_base_path}/{table_name}"

    # Auto Loader read (incremental)
    stream_df = (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.schemaLocation", checkpoint_path + "/schema")
            .option("header", "true")
            .option("inferSchema", "true")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .load(source_path)
            .withColumn("ingestion_ts", current_timestamp())
            .withColumn("source_file", col("_metadata.file_path"))
    )

    def merge_to_bronze(microbatch_df, batch_id):

        if not DeltaTable.isDeltaTable(spark, target_path):
            (
                microbatch_df
                .write
                .format("delta")
                .mode("append")
                .save(target_path)
            )
            return

        target = DeltaTable.forPath(spark, target_path)

        (
            target.alias("t")
            .merge(
                microbatch_df.alias("s"),
                "t.source_file = s.source_file"
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

    (
        stream_df
        .writeStream
        .foreachBatch(merge_to_bronze)
        .option("checkpointLocation", checkpoint_path + "/checkpoint")
        .trigger(availableNow=True)
        .start()
    )
