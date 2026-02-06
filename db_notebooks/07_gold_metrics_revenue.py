# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.functions import col, to_date

# COMMAND ----------

# Paths
fact_orders_path = "/Volumes/workspace/ecomschema/ecom/gold/fact_orders"
fact_payments_path = "/Volumes/workspace/ecomschema/ecom/gold/fact_payments"
dim_customers_path = "/Volumes/workspace/ecomschema/ecom/gold/dim_customers"
metrics_revenue_path = "/Volumes/workspace/ecomschema/ecom/gold/metrics/metrics_revenue"

# Read tables
fact_orders = spark.read.format("delta").load(fact_orders_path)
fact_payments = spark.read.format("delta").load(fact_payments_path)
dim_customers = spark.read.format("delta").load(dim_customers_path)




# COMMAND ----------

# =========================
# Revenue base dataset
# =========================
revenue_base = (
    fact_orders.alias("o")
    .join(
        fact_payments.alias("p"),
        on="order_id",
        how="inner"   # only paid orders contribute to revenue
    )
    .join(
        dim_customers.alias("c"),
        on="customer_id",
        how="left"
    )
    .select(
        to_date(col("o.order_purchase_timestamp")).alias("order_date"),
        col("c.customer_state"),
        col("o.order_status"),
        col("o.order_id"),
        col("p.total_payment_value")
    )
)



# COMMAND ----------

# =========================
# Aggregated revenue metrics
# =========================
metrics_revenue = (
    revenue_base
    .groupBy(
        "order_date",
        "customer_state",
        "order_status"
    )
    .agg(
        F.sum("total_payment_value").alias("total_revenue"),
        F.countDistinct("order_id").alias("total_orders")
    )
)



# COMMAND ----------


(
    metrics_revenue
    .write
    .format("delta")
    .mode("overwrite")   # FULL REFRESH (INTENTIONAL)
    .save(metrics_revenue_path)
)


# COMMAND ----------

display(
    spark.read.format("delta").load(metrics_revenue_path)
)
