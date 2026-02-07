# Databricks notebook source
# MAGIC %md
# MAGIC #Customer

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------

bronze_customers_path = "/Volumes/workspace/ecomschema/ecom/bronze/delta/customer"
silver_customers_path = "/Volumes/workspace/ecomschema/ecom/silver/customer"
checkpoint_path = "/Volumes/workspace/ecomschema/ecom/silver/_checkpoint/customer"


# COMMAND ----------

bronze_stream = (
    spark.readStream
        .format("delta")
        .load(bronze_customers_path)
)

# COMMAND ----------

def process_customers_batch(batch_df, batch_id):

    if batch_df.isEmpty():
        return

    # Deduplicate inside batch
    window_spec = Window.partitionBy("customer_id").orderBy(F.col("ingestion_ts").desc())

    customers_dedup = (
        batch_df
        .withColumn("row_num", F.row_number().over(window_spec))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )

    # Clean & standardize
    customers_clean = (
        customers_dedup
        .filter(F.col("customer_id").isNotNull())
        .filter(F.col("customer_unique_id").isNotNull())
        .withColumn("customer_city", F.upper(F.trim(F.col("customer_city"))))
        .withColumn("customer_state", F.upper(F.trim(F.col("customer_state"))))
        .withColumn("customer_zip_code_prefix", F.col("customer_zip_code_prefix").cast("string"))
        .withColumn("silver_processed_ts", F.current_timestamp())
    )

    # Initial table creation
    if not DeltaTable.isDeltaTable(spark, silver_customers_path):
        (
            customers_clean
            .write
            .format("delta")
            .mode("overwrite")
            .save(silver_customers_path)
        )
        return

    silver_table = DeltaTable.forPath(spark, silver_customers_path)

    (
        silver_table.alias("target")
        .merge(
            customers_clean.alias("source"),
            "target.customer_id = source.customer_id"
        )
        .whenMatchedUpdate(
            condition="source.ingestion_ts > target.ingestion_ts",
            set={
                "customer_unique_id": "source.customer_unique_id",
                "customer_zip_code_prefix": "source.customer_zip_code_prefix",
                "customer_city": "source.customer_city",
                "customer_state": "source.customer_state",
                "ingestion_ts": "source.ingestion_ts",
                "source_file": "source.source_file",
                "silver_processed_ts": "source.silver_processed_ts"
            }
        )
        .whenNotMatchedInsertAll()
        .execute()
    )


# COMMAND ----------

query = (
    bronze_stream
    .writeStream
    .foreachBatch(process_customers_batch)
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)   # Batch-style execution
    .start()
)

query.awaitTermination()


# COMMAND ----------

# MAGIC %md
# MAGIC #Geolocation

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

bronze_geolocation_path = "/Volumes/workspace/ecomschema/ecom/bronze/delta/geolocation"
silver_geolocation_path = "/Volumes/workspace/ecomschema/ecom/silver/geolocation"
checkpoint_path = "/Volumes/workspace/ecomschema/ecom/silver/_checkpoint/geolocation"


# COMMAND ----------

bronze_stream = (
    spark.readStream
        .format("delta")
        .load(bronze_geolocation_path)
)


# COMMAND ----------

def process_geolocation_batch(batch_df, batch_id):

    if batch_df.isEmpty():
        return

    geo_clean_df = (
        batch_df
        .select(
            F.col("geolocation_zip_code_prefix").alias("zip_code_prefix"),
            F.col("geolocation_lat").alias("latitude"),
            F.col("geolocation_lng").alias("longitude"),
            F.lower(F.trim(F.col("geolocation_city"))).alias("city"),
            F.upper(F.trim(F.col("geolocation_state"))).alias("state"),
            F.col("ingestion_ts"),
            F.col("source_file")
        )
        .filter(
            F.col("zip_code_prefix").isNotNull() &
            F.col("latitude").isNotNull() &
            F.col("longitude").isNotNull()
        )
    )

    # Aggregate lat/long
    geo_lat_lng_agg = (
        geo_clean_df
        .groupBy("zip_code_prefix")
        .agg(
            F.avg("latitude").alias("latitude"),
            F.avg("longitude").alias("longitude"),
            F.max("ingestion_ts").alias("ingestion_ts")
        )
    )

    # Most frequent city/state per ZIP
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

    geo_final = (
        geo_lat_lng_agg
        .join(city_state_ranked, "zip_code_prefix", "left")
        .withColumn("silver_processed_ts", F.current_timestamp())
    )

    # Create Silver table if missing
    if not DeltaTable.isDeltaTable(spark, silver_geolocation_path):
        (
            geo_final
            .write
            .format("delta")
            .mode("overwrite")
            .save(silver_geolocation_path)
        )
        return

    # MERGE (IDEMPOTENT)
    silver_table = DeltaTable.forPath(spark, silver_geolocation_path)

    (
        silver_table.alias("t")
        .merge(
            geo_final.alias("s"),
            "t.zip_code_prefix = s.zip_code_prefix"
        )
        .whenMatchedUpdate(
            condition="s.ingestion_ts > t.ingestion_ts",
            set={
                "latitude": "s.latitude",
                "longitude": "s.longitude",
                "city": "s.city",
                "state": "s.state",
                "ingestion_ts": "s.ingestion_ts",
                "silver_processed_ts": "s.silver_processed_ts"
            }
        )
        .whenNotMatchedInsertAll()
        .execute()
    )


# COMMAND ----------

(
    bronze_stream
    .writeStream
    .foreachBatch(process_geolocation_batch)
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .start()
)


# COMMAND ----------

# MAGIC %md
# MAGIC #Order_items

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

bronze_order_items_path = "/Volumes/workspace/ecomschema/ecom/bronze/delta/order_items"
silver_order_items_path = "/Volumes/workspace/ecomschema/ecom/silver/order_items"
checkpoint_path = "/Volumes/workspace/ecomschema/ecom/silver/_checkpoint/order_items"


# COMMAND ----------

bronze_stream = (
    spark.readStream
        .format("delta")
        .load(bronze_order_items_path)
)


# COMMAND ----------

def process_order_items_batch(batch_df, batch_id):

    if batch_df.isEmpty():
        return

    # ------------------------
    # Clean & type enforcement
    # ------------------------
    order_items_clean_df = (
        batch_df
        .select(
            F.col("order_id"),
            F.col("order_item_id").cast("int"),
            F.col("product_id"),
            F.col("seller_id"),
            F.to_timestamp("shipping_limit_date").alias("shipping_limit_date"),
            F.col("price").cast("double"),
            F.col("freight_value").cast("double"),
            F.col("ingestion_ts"),
            F.col("source_file")
        )
        .filter(
            F.col("order_id").isNotNull() &
            F.col("order_item_id").isNotNull()
        )
    )

    # ------------------------
    # Deterministic dedup
    # ------------------------
    window_spec = (
        Window
        .partitionBy("order_id", "order_item_id")
        .orderBy(F.col("ingestion_ts").desc())
    )

    order_items_dedup_df = (
        order_items_clean_df
        .withColumn("rn", F.row_number().over(window_spec))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    # ------------------------
    # Enrichment
    # ------------------------
    order_items_final_df = (
        order_items_dedup_df
        .withColumn(
            "item_total_value",
            F.col("price") + F.col("freight_value")
        )
        .withColumn("silver_processed_ts", F.current_timestamp())
    )

    # ------------------------
    # Create Silver if missing
    # ------------------------
    if not DeltaTable.isDeltaTable(spark, silver_order_items_path):
        (
            order_items_final_df
            .write
            .format("delta")
            .mode("overwrite")
            .save(silver_order_items_path)
        )
        return

    # ------------------------
    # MERGE (Idempotent)
    # ------------------------
    silver_table = DeltaTable.forPath(spark, silver_order_items_path)

    (
        silver_table.alias("t")
        .merge(
            order_items_final_df.alias("s"),
            """
            t.order_id = s.order_id
            AND t.order_item_id = s.order_item_id
            """
        )
        .whenMatchedUpdate(
            condition="s.ingestion_ts > t.ingestion_ts",
            set={
                "product_id": "s.product_id",
                "seller_id": "s.seller_id",
                "shipping_limit_date": "s.shipping_limit_date",
                "price": "s.price",
                "freight_value": "s.freight_value",
                "item_total_value": "s.item_total_value",
                "ingestion_ts": "s.ingestion_ts",
                "source_file": "s.source_file",
                "silver_processed_ts": "s.silver_processed_ts"
            }
        )
        .whenNotMatchedInsertAll()
        .execute()
    )


# COMMAND ----------

(
    bronze_stream
    .writeStream
    .foreachBatch(process_order_items_batch)
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .start()
)


# COMMAND ----------

# MAGIC %md
# MAGIC #Order_Payments

# COMMAND ----------

from pyspark.sql.functions import col, expr, current_timestamp, lower, trim, when, lit
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

    # SAFE numeric casting
    .withColumn(
        "payment_value",
        expr("try_cast(payment_value as decimal(10,2))")
    )
    .filter(col("payment_value").isNotNull())

    .withColumn(
        "payment_sequential",
        expr("try_cast(payment_sequential as int)")
    )

    .withColumn(
        "payment_installments",
        expr("try_cast(payment_installments as int)")
    )

    # normalize text
    .withColumn(
        "payment_type",
        lower(trim(col("payment_type")))
    )

    # default installments
    .withColumn(
        "payment_installments",
        when(col("payment_installments").isNull(), lit(1))
        .otherwise(col("payment_installments"))
    )

    # metadata
    .withColumn("silver_processed_ts", current_timestamp())
)


# COMMAND ----------

if not DeltaTable.isDeltaTable(spark, silver_order_payments_path):
    (
        order_payments_clean
        .write
        .format("delta")
        .mode("overwrite")
        .save(silver_order_payments_path)
    )


# COMMAND ----------

silver_order_payments = DeltaTable.forPath(
    spark, silver_order_payments_path
)

(
    silver_order_payments.alias("t")
    .merge(
        order_payments_clean.alias("s"),
        """
        t.order_id = s.order_id
        AND t.payment_sequential = s.payment_sequential
        """
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)


# COMMAND ----------

# MAGIC %md
# MAGIC #Order Reviews

# COMMAND ----------

from pyspark.sql.functions import *
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
order_reviews_clean = (
    order_reviews_bronze

    # Mandatory keys
    .filter(col("review_id").isNotNull())
    .filter(col("order_id").isNotNull())

    # Safe review_score
    .withColumn(
        "review_score",
        expr("try_cast(review_score as int)")
    )
    .filter(col("review_score").between(1, 5))

    # Text cleanup
    .withColumn("review_comment_title", trim(col("review_comment_title")))
    .withColumn("review_comment_message", trim(col("review_comment_message")))

    # Safe timestamp parsing
    .withColumn(
        "review_creation_date",
        expr("try_to_timestamp(review_creation_date, 'yyyy-MM-dd HH:mm:ss')")
    )
    .withColumn(
        "review_answer_timestamp",
        expr("try_to_timestamp(review_answer_timestamp, 'yyyy-MM-dd HH:mm:ss')")
    )

    # Drop corrupted timestamps
    .filter(col("review_creation_date").isNotNull())

    # Silver metadata
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
# MAGIC #Orders

# COMMAND ----------

from pyspark.sql.functions import *
from delta.tables import DeltaTable


# COMMAND ----------

bronze_orders_path = "/Volumes/workspace/ecomschema/ecom/bronze/delta/orders"
silver_orders_path = "/Volumes/workspace/ecomschema/ecom/silver/orders"

# COMMAND ----------

orders_bronze = (
    spark.read
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

    # SAFE timestamp parsing
    .withColumn(
        "order_purchase_timestamp",
        expr("try_to_timestamp(order_purchase_timestamp, 'yyyy-MM-dd HH:mm:ss')")
    )
    .withColumn(
        "order_approved_at",
        expr("try_to_timestamp(order_approved_at, 'yyyy-MM-dd HH:mm:ss')")
    )
    .withColumn(
        "order_delivered_carrier_date",
        expr("try_to_timestamp(order_delivered_carrier_date, 'yyyy-MM-dd HH:mm:ss')")
    )
    .withColumn(
        "order_delivered_customer_date",
        expr("try_to_timestamp(order_delivered_customer_date, 'yyyy-MM-dd HH:mm:ss')")
    )
    .withColumn(
        "order_estimated_delivery_date",
        expr("try_to_timestamp(order_estimated_delivery_date, 'yyyy-MM-dd HH:mm:ss')")
    )

    # Silver metadata
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
# MAGIC #Products

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lower, trim, expr
from delta.tables import DeltaTable


# COMMAND ----------

bronze_products_path = "/Volumes/workspace/ecomschema/ecom/bronze/delta/products"
silver_products_path = "/Volumes/workspace/ecomschema/ecom/silver/products"

# COMMAND ----------

products_bronze = (
    spark.read
    .format("delta")
    .load(bronze_products_path)
)

# COMMAND ----------

products_clean = (
    products_bronze

    # Mandatory key
    .filter(col("product_id").isNotNull())

    # Normalize category
    .withColumn(
        "product_category_name",
        lower(trim(col("product_category_name")))
    )

    # SAFE numeric casting (no failures)
    .withColumn("product_name_lenght", expr("try_cast(product_name_lenght as int)"))
    .withColumn("product_description_lenght", expr("try_cast(product_description_lenght as int)"))
    .withColumn("product_photos_qty", expr("try_cast(product_photos_qty as int)"))

    .withColumn("product_weight_g", expr("try_cast(product_weight_g as int)"))
    .withColumn("product_length_cm", expr("try_cast(product_length_cm as int)"))
    .withColumn("product_height_cm", expr("try_cast(product_height_cm as int)"))
    .withColumn("product_width_cm", expr("try_cast(product_width_cm as int)"))

    # Silver metadata
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

# MAGIC %md
# MAGIC #Sellers

# COMMAND ----------

from pyspark.sql.functions import (
    col,
    current_timestamp,
    trim,
    initcap,
    upper,
    expr
)
from delta.tables import DeltaTable


# COMMAND ----------

bronze_sellers_path = "/Volumes/workspace/ecomschema/ecom/bronze/delta/sellers"
silver_sellers_path = "/Volumes/workspace/ecomschema/ecom/silver/sellers"

# COMMAND ----------

sellers_bronze = (
    spark.read
    .format("delta")
    .load(bronze_sellers_path)
)


# COMMAND ----------

sellers_clean = (
    sellers_bronze

    # Mandatory key
    .filter(col("seller_id").isNotNull())

    # SAFE zip casting
    .withColumn(
        "seller_zip_code_prefix",
        expr("try_cast(seller_zip_code_prefix as int)")
    )

    # Clean text fields
    .withColumn("seller_city", initcap(trim(col("seller_city"))))
    .withColumn("seller_state", upper(trim(col("seller_state"))))

    # Silver metadata
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