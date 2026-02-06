# Databricks notebook source
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp, col

# COMMAND ----------

# Paths to silver tables
silver_orders_path = "/Volumes/workspace/ecomschema/ecom/silver/orders"
silver_payments_path = "/Volumes/workspace/ecomschema/ecom/silver/order_payments"
gold_fact_payments_path = "/Volumes/workspace/ecomschema/ecom/gold/fact_payments"

# Load Silver tables
orders_silver = spark.read.format("delta").load(silver_orders_path)
order_payments_silver = spark.read.format("delta").load(silver_payments_path)


# COMMAND ----------

# Clean payments: mandatory keys + valid amount
order_payments_clean = (
    order_payments_silver
    .filter(col("order_id").isNotNull())
    .filter(col("payment_value").isNotNull())
    .withColumn("payment_value", col("payment_value").cast("double"))
    # Metadata
    .withColumn("silver_processed_ts", current_timestamp())
)

# Aggregate per order
payments_agg = (
    order_payments_clean
    .groupBy("order_id")
    .agg(
        F.count("*").alias("payment_count"),
        F.sum("payment_value").alias("total_payment_value")
    )
)


# COMMAND ----------

payments_enriched = (
    payments_agg.alias("p")
    .join(orders_silver.alias("o"), "order_id", "left")
    .select(
        F.col("p.order_id"),
        F.col("o.customer_id"),
        F.col("total_payment_value"),
        F.col("payment_count"),
        F.col("o.order_purchase_timestamp"),
        F.col("o.order_status"),
        F.current_timestamp().alias("gold_processed_ts")
    )
)


# COMMAND ----------

# Create table if not exists
if not DeltaTable.isDeltaTable(spark, gold_fact_payments_path):
    payments_enriched.write.format("delta").mode("overwrite").save(gold_fact_payments_path)

# Merge incremental updates
gold_table = DeltaTable.forPath(spark, gold_fact_payments_path)

(
    gold_table.alias("target")
    .merge(
        payments_enriched.alias("source"),
        "target.order_id = source.order_id"
    )
    .whenMatchedUpdate(
        set={
            "customer_id": "source.customer_id",
            "total_payment_value": "source.total_payment_value",
            "payment_count": "source.payment_count",
            "order_status": "source.order_status",
            "order_purchase_timestamp": "source.order_purchase_timestamp",
            "gold_processed_ts": "source.gold_processed_ts"
        }
    )
    .whenNotMatchedInsertAll()
    .execute()
)


# COMMAND ----------

display(
    spark.read.format("delta").load(gold_fact_payments_path)
)
