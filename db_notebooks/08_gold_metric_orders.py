# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.functions import col, to_date, datediff


# COMMAND ----------

fact_orders_path = "/Volumes/workspace/ecomschema/ecom/gold/fact_orders"
dim_customers_path = "/Volumes/workspace/ecomschema/ecom/gold/dim_customers"
metrics_orders_path = "/Volumes/workspace/ecomschema/ecom/gold/metrics/metrics_orders"


# COMMAND ----------

fact_orders = spark.read.format("delta").load(fact_orders_path)
dim_customers = spark.read.format("delta").load(dim_customers_path)


# COMMAND ----------

orders_base = (
    fact_orders.alias("o")
    .join(
        dim_customers.alias("c"),
        on="customer_id",
        how="left"
    )
    .select(
        to_date(col("o.order_purchase_timestamp")).alias("order_date"),
        col("c.customer_state"),
        col("o.order_id"),
        col("o.order_status"),
        col("o.order_purchase_timestamp"),
        col("o.order_delivered_customer_date")
    )
)


# COMMAND ----------

metrics_orders = (
    orders_base
    .groupBy("order_date", "customer_state")
    .agg(
        # Volume metrics
        F.countDistinct("order_id").alias("total_orders"),

        F.sum(F.when(col("order_status") == "delivered", 1).otherwise(0))
            .alias("delivered_orders"),

        F.sum(F.when(col("order_status") == "canceled", 1).otherwise(0))
            .alias("cancelled_orders"),

        F.sum(F.when(col("order_status") == "shipped", 1).otherwise(0))
            .alias("shipped_orders"),

        F.sum(F.when(col("order_status") == "processing", 1).otherwise(0))
            .alias("processing_orders"),

        # Lifecycle metrics
        F.avg(
            datediff(
                col("order_delivered_customer_date"),
                col("order_purchase_timestamp")
            )
        ).alias("avg_delivery_days")
    )
)


# COMMAND ----------

(
    metrics_orders
    .write
    .format("delta")
    .mode("overwrite")   # FULL REFRESH (INTENTIONAL)
    .save(metrics_orders_path)
)


# COMMAND ----------

display(
    spark.read.format("delta").load(metrics_orders_path)
)
