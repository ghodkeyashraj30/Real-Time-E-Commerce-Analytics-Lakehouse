# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.functions import col, trim, current_timestamp
from delta.tables import DeltaTable


# COMMAND ----------

silver_reviews_path = "/Volumes/workspace/ecomschema/ecom/silver/order_reviews"
silver_orders_path = "/Volumes/workspace/ecomschema/ecom/silver/orders"
gold_fact_reviews_path = "/Volumes/workspace/ecomschema/ecom/gold/fact_reviews"

order_reviews_silver = spark.read.format("delta").load(silver_reviews_path)
orders_silver = spark.read.format("delta").load(silver_orders_path)


# COMMAND ----------

reviews_clean = (
    order_reviews_silver

    # mandatory keys
    .filter(col("review_id").isNotNull())
    .filter(col("order_id").isNotNull())

    # review score validation
    .withColumn("review_score", col("review_score").cast("int"))
    .filter(col("review_score").between(1, 5))

    # text cleanup
    .withColumn("review_comment_title", trim(col("review_comment_title")))
    .withColumn("review_comment_message", trim(col("review_comment_message")))

    # SAFE timestamp parsing (no job failure)
    .withColumn(
        "review_creation_date",
        F.try_to_timestamp(col("review_creation_date"))
    )
    .withColumn(
        "review_answer_timestamp",
        F.try_to_timestamp(col("review_answer_timestamp"))
    )

    # drop corrupted timestamps
    .filter(col("review_creation_date").isNotNull())

    # gold metadata
    .withColumn("gold_processed_ts", current_timestamp())
)


# COMMAND ----------

reviews_enriched = (
    reviews_clean.alias("r")
    .join(
        orders_silver.alias("o"),
        on="order_id",
        how="left"
    )
    .select(
        col("r.review_id"),
        col("r.order_id"),
        col("o.customer_id"),
        col("r.review_score"),
        col("r.review_comment_title"),
        col("r.review_comment_message"),
        col("r.review_creation_date"),
        col("r.review_answer_timestamp"),
        col("o.order_status"),
        col("o.order_purchase_timestamp"),
        col("r.gold_processed_ts")
    )
)


# COMMAND ----------

from pyspark.sql.window import Window

w = Window.partitionBy("review_id").orderBy(
    F.col("gold_processed_ts").desc()
)

reviews_final = (
    reviews_enriched
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)


# COMMAND ----------

# Create table if not exists
if not DeltaTable.isDeltaTable(spark, gold_fact_reviews_path):
    (
        reviews_enriched
        .write
        .format("delta")
        .mode("overwrite")
        .save(gold_fact_reviews_path)
    )

gold_table = DeltaTable.forPath(spark, gold_fact_reviews_path)

(
    gold_table.alias("target")
    .merge(
        reviews_final.alias("source"),
        "target.review_id = source.review_id"
    )
    .whenMatchedUpdate(set={
        "order_id": "source.order_id",
        "customer_id": "source.customer_id",
        "review_score": "source.review_score",
        "review_comment_title": "source.review_comment_title",
        "review_comment_message": "source.review_comment_message",
        "review_creation_date": "source.review_creation_date",
        "review_answer_timestamp": "source.review_answer_timestamp",
        "order_status": "source.order_status",
        "order_purchase_timestamp": "source.order_purchase_timestamp",
        "gold_processed_ts": "source.gold_processed_ts"
    })
    .whenNotMatchedInsertAll()
    .execute()
)


# COMMAND ----------

display(
    spark.read.format("delta").load(gold_fact_reviews_path)
)
