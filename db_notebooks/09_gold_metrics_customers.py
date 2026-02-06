# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.functions import col, to_date


# COMMAND ----------

dim_customers_path = "/Volumes/workspace/ecomschema/ecom/gold/dim_customers"
fact_orders_path   = "/Volumes/workspace/ecomschema/ecom/gold/fact_orders"
metrics_customers_path = "/Volumes/workspace/ecomschema/ecom/gold/metrics/metrics_customers"


# COMMAND ----------

dim_customers = spark.read.format("delta").load(dim_customers_path)
fact_orders   = spark.read.format("delta").load(fact_orders_path)


# COMMAND ----------

customer_orders_base = (
    dim_customers.alias("c")
    .join(
        fact_orders.alias("o"),
        on="customer_id",
        how="left"
    )
    .select(
        col("c.customer_id"),
        to_date(col("c.gold_created_ts")).alias("customer_signup_date"),
        col("c.customer_state"),
        col("o.order_id"),
        col("o.order_purchase_timestamp"),
        col("o.order_status")
    )
)


# COMMAND ----------

metrics_customers = (
    customer_orders_base
    .groupBy("customer_signup_date", "customer_state")
    .agg(
        F.countDistinct("customer_id").alias("total_customers"),

        F.countDistinct("order_id").alias("total_orders"),

        F.countDistinct(
            F.when(col("order_status") == "delivered", col("order_id"))
        ).alias("delivered_orders"),

        F.countDistinct(
            F.when(col("order_id").isNotNull(), col("customer_id"))
        ).alias("active_customers"),

        F.sum(
            F.when(col("order_id").isNotNull(), 1).otherwise(0)
        ).alias("orders_per_customer_proxy")
    )
)


# COMMAND ----------

(
    metrics_customers
    .write
    .format("delta")
    .mode("overwrite")   # FULL REFRESH
    .save(metrics_customers_path)
)


# COMMAND ----------

display(
    spark.read.format("delta").load(metrics_customers_path)
)
