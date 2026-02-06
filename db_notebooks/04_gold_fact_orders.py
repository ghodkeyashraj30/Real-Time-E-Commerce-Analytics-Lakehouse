# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable



# COMMAND ----------

silver_base = "/Volumes/workspace/ecomschema/ecom/silver"
gold_base = "/Volumes/workspace/ecomschema/ecom/gold"

orders_silver_path = f"{silver_base}/orders"
order_items_silver_path = f"{silver_base}/order_items"
customers_silver_path = f"{silver_base}/customer"

gold_fact_orders_path = f"{gold_base}/fact_orders"


# COMMAND ----------

orders_silver = (
    spark.read.format("delta")
    .load(orders_silver_path)
    .withColumnRenamed("silver_processed_ts", "orders_silver_ts")
)

order_items_silver = (
    spark.read.format("delta")
    .load(order_items_silver_path)
)

customers_silver = (
    spark.read.format("delta")
    .load(customers_silver_path)
)


# COMMAND ----------

order_items_agg = (
    order_items_silver
    .groupBy("order_id")
    .agg(
        F.count("*").alias("order_item_count"),
        F.sum("price").alias("total_order_value"),
        F.sum("freight_value").alias("total_freight_value")
    )
)


# COMMAND ----------

orders_enriched = (
    orders_silver.alias("o")
    .join(customers_silver.alias("c"), "customer_id", "left")
    .join(order_items_agg.alias("oi"), "order_id", "left")
)


# COMMAND ----------

fact_orders_final = (
    orders_enriched
    .select(
        F.col("o.order_id"),
        F.col("o.customer_id"),
        F.col("o.order_status"),
        F.col("o.order_purchase_timestamp"),
        F.col("o.order_approved_at"),
        F.col("o.order_delivered_customer_date"),
        F.col("o.order_estimated_delivery_date"),

        F.col("oi.order_item_count"),
        F.col("oi.total_order_value"),
        F.col("oi.total_freight_value"),

        F.datediff("order_approved_at", "order_purchase_timestamp")
            .alias("order_to_approval_days"),

        F.datediff("order_delivered_customer_date", "order_purchase_timestamp")
            .alias("order_to_delivery_days"),

        F.datediff("order_delivered_customer_date", "order_estimated_delivery_date")
            .alias("delivery_delay_days"),

        F.col("o.orders_silver_ts"),
        F.current_timestamp().alias("gold_processed_ts")
    )
)


# COMMAND ----------

if not DeltaTable.isDeltaTable(spark, gold_fact_orders_path):
    (
        fact_orders_final
        .write
        .format("delta")
        .mode("overwrite")
        .save(gold_fact_orders_path)
    )


# COMMAND ----------

gold_table = DeltaTable.forPath(spark, gold_fact_orders_path)

(
    gold_table.alias("target")
    .merge(
        fact_orders_final.alias("source"),
        "target.order_id = source.order_id"
    )
    .whenMatchedUpdate(
        condition="source.orders_silver_ts > target.orders_silver_ts",
        set={
            "order_status": "source.order_status",
            "order_purchase_timestamp": "source.order_purchase_timestamp",
            "order_approved_at": "source.order_approved_at",
            "order_delivered_customer_date": "source.order_delivered_customer_date",
            "order_estimated_delivery_date": "source.order_estimated_delivery_date",

            "order_item_count": "source.order_item_count",
            "total_order_value": "source.total_order_value",
            "total_freight_value": "source.total_freight_value",

            "order_to_approval_days": "source.order_to_approval_days",
            "order_to_delivery_days": "source.order_to_delivery_days",
            "delivery_delay_days": "source.delivery_delay_days",

            "orders_silver_ts": "source.orders_silver_ts",
            "gold_processed_ts": "source.gold_processed_ts"
        }
    )
    .whenNotMatchedInsertAll()
    .execute()
)


# COMMAND ----------

display(
    spark.read.format("delta").load(gold_fact_orders_path)
)
