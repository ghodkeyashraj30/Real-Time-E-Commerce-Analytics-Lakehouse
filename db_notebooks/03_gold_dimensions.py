# Databricks notebook source
from pyspark.sql import functions as F
from delta.tables import DeltaTable


# COMMAND ----------

silver_customers_path = "/Volumes/workspace/ecomschema/ecom/silver/customer"
gold_customers_path   = "/Volumes/workspace/ecomschema/ecom/gold/dim_customers"


# COMMAND ----------

customers_silver = (
    spark.read
         .format("delta")
         .load(silver_customers_path)
)


# COMMAND ----------

from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("ingestion_ts").desc())

customers_dedup = (
    customers_silver
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)


# COMMAND ----------

dim_customers_df = (
    customers_dedup
    .select(
        F.col("customer_id"),
        F.col("customer_unique_id"),
        F.col("customer_zip_code_prefix").cast("int"),
        F.lower(F.trim(F.col("customer_city"))).alias("customer_city"),
        F.upper(F.trim(F.col("customer_state"))).alias("customer_state"),
        F.current_timestamp().alias("gold_created_ts")
    )
)


# COMMAND ----------

if not DeltaTable.isDeltaTable(spark, gold_customers_path):
    (
        dim_customers_df
        .write
        .format("delta")
        .mode("overwrite")
        .save(gold_customers_path)
    )


# COMMAND ----------

gold_table = DeltaTable.forPath(spark, gold_customers_path)

(
    gold_table.alias("target")
    .merge(
        dim_customers_df.alias("source"),
        "target.customer_id = source.customer_id"
    )
    .whenMatchedUpdate(set={
        "customer_unique_id": "source.customer_unique_id",
        "customer_zip_code_prefix": "source.customer_zip_code_prefix",
        "customer_city": "source.customer_city",
        "customer_state": "source.customer_state",
        "gold_created_ts": "source.gold_created_ts"
    })
    .whenNotMatchedInsertAll()
    .execute()
)


# COMMAND ----------

display(
    spark.read
         .format("delta")
         .load(gold_customers_path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Products Dimension

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable


# COMMAND ----------

silver_products_path = "/Volumes/workspace/ecomschema/ecom/silver/products"
gold_products_path   = "/Volumes/workspace/ecomschema/ecom/gold/dim_products"


# COMMAND ----------

products_silver = spark.read.format("delta").load(silver_products_path)

# COMMAND ----------

w = Window.partitionBy("product_id").orderBy(F.col("ingestion_ts").desc())

products_latest = (
    products_silver
    .filter(F.col("product_id").isNotNull())
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

# COMMAND ----------

dim_products = (
    products_latest
    .withColumn(
        "product_volume_cm3",
        F.col("product_length_cm") *
        F.col("product_height_cm") *
        F.col("product_width_cm")
    )
    .withColumn("gold_processed_ts", F.current_timestamp())
)


# COMMAND ----------

dim_products_final = dim_products.withColumn(
    "product_sk",
    F.monotonically_increasing_id()
)


# COMMAND ----------

if not DeltaTable.isDeltaTable(spark, gold_products_path):
    (
        dim_products_final
        .write
        .format("delta")
        .mode("overwrite")
        .save(gold_products_path)
    )
else:
    gold_table = DeltaTable.forPath(spark, gold_products_path)

    (
        gold_table.alias("target")
        .merge(
            dim_products_final.alias("source"),
            "target.product_id = source.product_id"
        )
        .whenMatchedUpdate(set={
            "product_category_name": "source.product_category_name",
            "product_weight_g": "source.product_weight_g",
            "product_length_cm": "source.product_length_cm",
            "product_height_cm": "source.product_height_cm",
            "product_width_cm": "source.product_width_cm",
            "product_volume_cm3": "source.product_volume_cm3",
            "silver_processed_ts": "source.silver_processed_ts",
            "gold_processed_ts": "source.gold_processed_ts"
        })
        .whenNotMatchedInsertAll()
        .execute()
    )


# COMMAND ----------

display(
    spark.read
         .format("delta")
         .load(gold_products_path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Sellers Dimension

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window


# COMMAND ----------

# Paths
silver_sellers_path = "/Volumes/workspace/ecomschema/ecom/silver/sellers"
gold_dim_sellers_path = "/Volumes/workspace/ecomschema/ecom/gold/dim_sellers"


# COMMAND ----------

# Read silver sellers
sellers_silver = spark.read.format("delta").load(silver_sellers_path)

# COMMAND ----------

# Window for deduplication
w = Window.partitionBy("seller_id").orderBy(F.col("ingestion_ts").desc())

# Deduplicate & clean
dim_sellers = (
    sellers_silver
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
    
    # text standardization
    .withColumn("seller_city", F.upper(F.trim(F.col("seller_city"))))
    .withColumn("seller_state", F.upper(F.trim(F.col("seller_state"))))
    
    # surrogate key
    .withColumn("seller_sk", F.monotonically_increasing_id())
    
    # gold metadata
    .withColumn("gold_processed_ts", F.current_timestamp())
)

# COMMAND ----------

# Write Gold Dimension
(
    dim_sellers
    .write
    .format("delta")
    .mode("overwrite")
    .save(gold_dim_sellers_path)
)

# COMMAND ----------

display(
    spark.read
         .format("delta")
         .load(gold_dim_sellers_path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Geolocation Dimension

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window


# COMMAND ----------

# Paths
silver_geolocation_path = "/Volumes/workspace/ecomschema/ecom/silver/geolocation"
gold_dim_geolocation_path = "/Volumes/workspace/ecomschema/ecom/gold/dim_geolocation"


# COMMAND ----------

# Read silver geolocation
geo_silver = spark.read.format("delta").load(silver_geolocation_path)


# COMMAND ----------

geo_clean = (
    geo_silver
    .withColumn("city", F.upper(F.trim(F.col("city"))))
    .withColumn("state", F.upper(F.trim(F.col("state"))))
)

# COMMAND ----------


# Find most frequent city/state per ZIP

city_state_count = (
    geo_clean
    .groupBy(
        "zip_code_prefix",
        "city",
        "state"
    )
    .count()
)

w = Window.partitionBy("zip_code_prefix").orderBy(F.col("count").desc())

city_state_ranked = (
    city_state_count
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn", "count")
)


# COMMAND ----------

# Aggregate lat / long per ZIP

lat_long_agg = (
    geo_clean
    .groupBy("zip_code_prefix")
    .agg(
        F.avg("latitude").alias("avg_lat"),
        F.avg("longitude").alias("avg_lng")
    )
)


# COMMAND ----------

#Final dimension

dim_geolocation = (
    lat_long_agg
    .join(
        city_state_ranked,
        on="zip_code_prefix",
        how="left"
    )
    .withColumn("geolocation_sk", F.monotonically_increasing_id())
    .withColumn("gold_processed_ts", F.current_timestamp())
)

# COMMAND ----------

(
    dim_geolocation
    .write
    .format("delta")
    .mode("overwrite")
    .save(gold_dim_geolocation_path)
)

# COMMAND ----------

display(
    spark.read
         .format("delta")
         .load(gold_dim_geolocation_path)
)