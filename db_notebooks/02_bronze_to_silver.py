# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------

# Bronze customers delta table path
bronze_customers_path = "/Volumes/workspace/ecomschema/ecom/bronze/delta/customer"

# Silver customers delta table path
silver_customers_path = "/Volumes/workspace/ecomschema/ecom/silver/customer"


# COMMAND ----------

bronze_df = spark.read.format("delta").load(bronze_customers_path)

display(bronze_df)


# COMMAND ----------

window_spec = Window.partitionBy("customer_id").orderBy(F.col("ingestion_ts").desc())

customers_dedup = (
    bronze_df
    .withColumn("row_num", F.row_number().over(window_spec))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)


# COMMAND ----------

customers_clean = (
    customers_dedup
    # Drop invalid customers
    .filter(F.col("customer_id").isNotNull())
    .filter(F.col("customer_unique_id").isNotNull())

    # Standardize text columns
    .withColumn("customer_city", F.upper(F.trim(F.col("customer_city"))))
    .withColumn("customer_state", F.upper(F.trim(F.col("customer_state"))))

    # Enforce data types
    .withColumn("customer_zip_code_prefix", F.col("customer_zip_code_prefix").cast("string"))

    # Audit column
    .withColumn("silver_processed_ts", F.current_timestamp())
)


# COMMAND ----------

if not DeltaTable.isDeltaTable(spark, silver_customers_path):
    (
        customers_clean
        .write
        .format("delta")
        .mode("overwrite")
        .save(silver_customers_path)
    )


# COMMAND ----------

silver_table = DeltaTable.forPath(spark, silver_customers_path)

(
    silver_table.alias("target")
    .merge(
        customers_clean.alias("source"),
        "target.customer_id = source.customer_id"
    )
    .whenMatchedUpdate(condition="source.ingestion_ts > target.ingestion_ts", set={
        "customer_unique_id": "source.customer_unique_id",
        "customer_zip_code_prefix": "source.customer_zip_code_prefix",
        "customer_city": "source.customer_city",
        "customer_state": "source.customer_state",
        "ingestion_ts": "source.ingestion_ts",
        "source_file": "source.source_file",
        "silver_processed_ts": "source.silver_processed_ts"
    })
    .whenNotMatchedInsertAll()
    .execute()
)


# COMMAND ----------

# MAGIC %md
# MAGIC #Geolocation

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

bronze_geolocation_path = "/Volumes/workspace/ecomschema/ecom/bronze/delta/geolocation"
silver_geolocation_path = "/Volumes/workspace/ecomschema/ecom/silver/geolocation"


# COMMAND ----------

geo_bronze_df = (
    spark.read
    .format("delta")
    .load(bronze_geolocation_path)
)


# COMMAND ----------

geo_clean_df = (
    geo_bronze_df
    .select(
        F.col("geolocation_zip_code_prefix").alias("zip_code_prefix"),
        F.col("geolocation_lat").alias("latitude"),
        F.col("geolocation_lng").alias("longitude"),
        F.lower(F.trim(F.col("geolocation_city"))).alias("city"),
        F.upper(F.trim(F.col("geolocation_state"))).alias("state"),
        F.col("ingestion_ts"),
        F.col("_metadata.file_path").alias("source_file")
    )
    .filter(
        F.col("zip_code_prefix").isNotNull() &
        F.col("latitude").isNotNull() &
        F.col("longitude").isNotNull()
    )
)


# COMMAND ----------

geo_lat_lng_agg = (
    geo_clean_df
    .groupBy("zip_code_prefix")
    .agg(
        F.avg("latitude").alias("latitude"),
        F.avg("longitude").alias("longitude"),
        F.min("ingestion_ts").alias("ingestion_ts")
    )
)


# COMMAND ----------

city_state_count = (
    geo_clean_df
    .groupBy("zip_code_prefix", "city", "state")
    .count()
)

window_spec = Window.partitionBy("zip_code_prefix").orderBy(F.desc("count"))

city_state_ranked = (
    city_state_count
    .withColumn("rn", F.row_number().over(window_spec))
    .filter(F.col("rn") == 1)
    .select("zip_code_prefix", "city", "state")
)


# COMMAND ----------

source_file_df = (
    geo_clean_df
    .groupBy("zip_code_prefix")
    .agg(
        F.first("source_file").alias("source_file")
    )
)


# COMMAND ----------

geo_silver_df = (
    geo_lat_lng_agg
    .join(city_state_ranked, "zip_code_prefix", "left")
    .join(source_file_df, "zip_code_prefix", "left")
    .withColumn("silver_processed_ts", F.current_timestamp())
)


# COMMAND ----------

(
    geo_silver_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(silver_geolocation_path)
)


# COMMAND ----------

# MAGIC %md
# MAGIC Order_items

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable


# COMMAND ----------

bronze_order_items_path = "/Volumes/workspace/ecomschema/ecom/bronze/delta/order_items"
silver_order_items_path = "/Volumes/workspace/ecomschema/ecom/silver/order_items"

# COMMAND ----------

order_items_bronze_df = (
    spark.read
    .format("delta")
    .load(bronze_order_items_path)
)


# COMMAND ----------

order_items_clean_df = (
    order_items_bronze_df
    .select(
        F.col("order_id"),
        F.col("order_item_id").cast("int"),
        F.col("product_id"),
        F.col("seller_id"),
        F.to_timestamp("shipping_limit_date").alias("shipping_limit_date"),
        F.col("price").cast("double"),
        F.col("freight_value").cast("double"),
        F.col("ingestion_ts"),
        F.col("_metadata.file_path").alias("source_file")
    )
    .filter(
        F.col("order_id").isNotNull() &
        F.col("order_item_id").isNotNull()
    )
)


# COMMAND ----------

order_items_dedup_df = (
    order_items_clean_df
    .dropDuplicates(["order_id", "order_item_id"])
)


# COMMAND ----------

order_items_enriched_df = (
    order_items_dedup_df
    .withColumn(
        "item_total_value",
        F.col("price") + F.col("freight_value")
    )
    .withColumn("silver_processed_ts", F.current_timestamp())
)


# COMMAND ----------

(
    order_items_enriched_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(silver_order_items_path)
)


# COMMAND ----------

# MAGIC %md
# MAGIC Order_Payments

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable


# COMMAND ----------

bronze_order_payments_path = "/Volumes/workspace/ecomschema/ecom/bronze/delta/order_payments"
silver_order_payments_path = "/Volumes/workspace/ecomschema/ecom/silver/order_payments"


# COMMAND ----------

order_payments_bronze = (
    spark.read
    .format("delta")
    .load(bronze_order_payments_path)
)


# COMMAND ----------

order_payments_clean = (
    order_payments_bronze
    # mandatory keys
    .filter(col("order_id").isNotNull())
    .filter(col("payment_sequential").isNotNull())
    
    # numeric validations
    .filter(col("payment_value") >= 0)
    
    # normalize payment_type
    .withColumn(
        "payment_type",
        lower(trim(col("payment_type")))
    )
    
    # handle installments
    .withColumn(
        "payment_installments",
        when(col("payment_installments").isNull(), lit(1))
        .otherwise(col("payment_installments"))
    )
    
    # metadata
    .withColumn("silver_processed_ts", current_timestamp())
)


# COMMAND ----------

(
    order_payments_clean
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(silver_order_payments_path)
)


# COMMAND ----------

from pyspark.sql.functions import *
from delta.tables import DeltaTable
from pyspark.sql.functions import col

# COMMAND ----------

bronze_order_reviews_path = "/Volumes/workspace/ecomschema/ecom/bronze/delta/order_reviews"
silver_order_reviews_path = "/Volumes/workspace/ecomschema/ecom/silver/order_reviews"

# COMMAND ----------

order_reviews_bronze = (
    spark.read
    .format("delta")
    .load(bronze_order_reviews_path)
)

# COMMAND ----------

# DBTITLE 1,Untitled
from pyspark.sql.functions import *

order_reviews_clean = (
    order_reviews_bronze

    # -----------------------------
    # Mandatory keys
    # -----------------------------
    .filter(col("review_id").isNotNull())
    .filter(col("order_id").isNotNull())

    # -----------------------------
    # SAFE review_score handling
    # -----------------------------
    .withColumn(
        "review_score",
        expr("try_cast(review_score as int)")
    )
    .filter(col("review_score").between(1, 5))

    # -----------------------------
    # Text cleanup
    # -----------------------------
    .withColumn("review_comment_title", trim(col("review_comment_title")))
    .withColumn("review_comment_message", trim(col("review_comment_message")))

    # -----------------------------
    # SAFE timestamp parsing (NO FAILURES)
    # -----------------------------
    .withColumn(
        "review_creation_date",
        expr("try_to_timestamp(review_creation_date, 'yyyy-MM-dd HH:mm:ss')")
    )
    .withColumn(
        "review_answer_timestamp",
        expr("try_to_timestamp(review_answer_timestamp, 'yyyy-MM-dd HH:mm:ss')")
    )

    # -----------------------------
    # Drop corrupted timestamp rows
    # -----------------------------
    .filter(col("review_creation_date").isNotNull())

    # -----------------------------
    # Silver metadata
    # -----------------------------
    .withColumn("silver_processed_ts", current_timestamp())
)


# COMMAND ----------

# DBTITLE 1,Untitled
(
    order_reviews_clean
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(silver_order_reviews_path)
)


# COMMAND ----------

# MAGIC %md
# MAGIC Orders

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp, current_timestamp
from delta.tables import DeltaTable


# COMMAND ----------

bronze_orders_path = "/Volumes/workspace/ecomschema/ecom/bronze/delta/orders"
silver_orders_path = "/Volumes/workspace/ecomschema/ecom/silver/orders"


# COMMAND ----------

orders_bronze = (
    spark
    .read
    .format("delta")
    .load(bronze_orders_path)
)


# COMMAND ----------

valid_status = [
    "created",
    "approved",
    "invoiced",
    "processing",
    "shipped",
    "delivered",
    "canceled",
    "unavailable"
]


# COMMAND ----------

orders_clean = (
    orders_bronze
    
    # Mandatory columns
    .filter(col("order_id").isNotNull())
    .filter(col("customer_id").isNotNull())
    .filter(col("order_status").isNotNull())
    
    # Business rule
    .filter(col("order_status").isin(valid_status))
    
    # Timestamp standardization (SAFE parsing)
    .withColumn(
        "order_purchase_timestamp",
        to_timestamp(col("order_purchase_timestamp"), "yyyy-MM-dd HH:mm:ss")
    )
    .withColumn(
        "order_approved_at",
        to_timestamp(col("order_approved_at"), "yyyy-MM-dd HH:mm:ss")
    )
    .withColumn(
        "order_delivered_carrier_date",
        to_timestamp(col("order_delivered_carrier_date"), "yyyy-MM-dd HH:mm:ss")
    )
    .withColumn(
        "order_delivered_customer_date",
        to_timestamp(col("order_delivered_customer_date"), "yyyy-MM-dd HH:mm:ss")
    )
    .withColumn(
        "order_estimated_delivery_date",
        to_timestamp(col("order_estimated_delivery_date"), "yyyy-MM-dd HH:mm:ss")
    )
    
    # Silver metadata ONLY
    .withColumn("silver_processed_ts", current_timestamp())
)


# COMMAND ----------

if not DeltaTable.isDeltaTable(spark, silver_orders_path):
    (
        orders_clean
        .write
        .format("delta")
        .mode("overwrite")
        .save(silver_orders_path)
    )


# COMMAND ----------

silver_orders = DeltaTable.forPath(spark, silver_orders_path)


# COMMAND ----------

(
    silver_orders.alias("target")
    .merge(
        orders_clean.alias("source"),
        "target.order_id = source.order_id"
    )
    .whenMatchedUpdate(
        condition="source.ingestion_ts > target.ingestion_ts",
        set={
            "customer_id": "source.customer_id",
            "order_status": "source.order_status",
            "order_purchase_timestamp": "source.order_purchase_timestamp",
            "order_approved_at": "source.order_approved_at",
            "order_delivered_carrier_date": "source.order_delivered_carrier_date",
            "order_delivered_customer_date": "source.order_delivered_customer_date",
            "order_estimated_delivery_date": "source.order_estimated_delivery_date",
            "ingestion_ts": "source.ingestion_ts",
            "source_file": "source.source_file",
            "silver_processed_ts": "source.silver_processed_ts"
        }
    )
    .whenNotMatchedInsertAll()
    .execute()
)


# COMMAND ----------

# MAGIC %md
# MAGIC products

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp, current_timestamp,lower, trim
from delta.tables import DeltaTable


# COMMAND ----------

bronze_products_path = "/Volumes/workspace/ecomschema/ecom/bronze/delta/products"
silver_products_path = "/Volumes/workspace/ecomschema/ecom/silver/products"


# COMMAND ----------

products_bronze = (
    spark
    .read
    .format("delta")
    .load(bronze_products_path)
)


# COMMAND ----------

products_clean = (
    products_bronze
    # mandatory key
    .filter(col("product_id").isNotNull())

    # clean category
    .withColumn(
        "product_category_name",
        lower(trim(col("product_category_name")))
    )

    # cast numeric columns safely
    .withColumn("product_name_lenght", col("product_name_lenght").cast("int"))
    .withColumn("product_description_lenght", col("product_description_lenght").cast("int"))
    .withColumn("product_photos_qty", col("product_photos_qty").cast("int"))

    .withColumn("product_weight_g", col("product_weight_g").cast("int"))
    .withColumn("product_length_cm", col("product_length_cm").cast("int"))
    .withColumn("product_height_cm", col("product_height_cm").cast("int"))
    .withColumn("product_width_cm", col("product_width_cm").cast("int"))

    # silver metadata
    .withColumn("silver_processed_ts", current_timestamp())
)


# COMMAND ----------

if not DeltaTable.isDeltaTable(spark, silver_products_path):
    (
        products_clean
        .write
        .format("delta")
        .mode("overwrite")
        .save(silver_products_path)
    )


# COMMAND ----------

silver_products = DeltaTable.forPath(spark, silver_products_path)

(
    silver_products.alias("target")
    .merge(
        products_clean.alias("source"),
        "target.product_id = source.product_id"
    )
    .whenMatchedUpdate(
        condition="source.ingestion_ts > target.ingestion_ts",
        set={
            "product_category_name": "source.product_category_name",
            "product_name_lenght": "source.product_name_lenght",
            "product_description_lenght": "source.product_description_lenght",
            "product_photos_qty": "source.product_photos_qty",
            "product_weight_g": "source.product_weight_g",
            "product_length_cm": "source.product_length_cm",
            "product_height_cm": "source.product_height_cm",
            "product_width_cm": "source.product_width_cm",
            "ingestion_ts": "source.ingestion_ts",
            "source_file": "source.source_file",
            "silver_processed_ts": "source.silver_processed_ts"
        }
    )
    .whenNotMatchedInsertAll()
    .execute()
)


# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp, current_timestamp,lower, trim, initcap, upper
from delta.tables import DeltaTable


# COMMAND ----------

bronze_sellers_path = "/Volumes/workspace/ecomschema/ecom/bronze/delta/sellers"
silver_sellers_path = "/Volumes/workspace/ecomschema/ecom/silver/sellers"


# COMMAND ----------

sellers_bronze = (
    spark
    .read
    .format("delta")
    .load(bronze_sellers_path)
)


# COMMAND ----------

sellers_clean = (
    sellers_bronze
    # mandatory key
    .filter(col("seller_id").isNotNull())

    # standardize zip
    .withColumn(
        "seller_zip_code_prefix",
        col("seller_zip_code_prefix").cast("int")
    )

    # clean text fields
    .withColumn("seller_city", initcap(trim(col("seller_city"))))
    .withColumn("seller_state", upper(trim(col("seller_state"))))

    # deduplicate sellers
    .dropDuplicates(["seller_id"])

    # silver metadata
    .withColumn("silver_processed_ts", current_timestamp())
)


# COMMAND ----------

if not DeltaTable.isDeltaTable(spark, silver_sellers_path):
    (
        sellers_clean
        .write
        .format("delta")
        .mode("overwrite")
        .save(silver_sellers_path)
    )


# COMMAND ----------

silver_sellers = DeltaTable.forPath(spark, silver_sellers_path)

(
    silver_sellers.alias("target")
    .merge(
        sellers_clean.alias("source"),
        "target.seller_id = source.seller_id"
    )
    .whenMatchedUpdate(
        condition="source.ingestion_ts > target.ingestion_ts",
        set={
            "seller_zip_code_prefix": "source.seller_zip_code_prefix",
            "seller_city": "source.seller_city",
            "seller_state": "source.seller_state",
            "ingestion_ts": "source.ingestion_ts",
            "source_file": "source.source_file",
            "silver_processed_ts": "source.silver_processed_ts"
        }
    )
    .whenNotMatchedInsertAll()
    .execute()
)
