# Databricks notebook source
# ============================================================================
# MEDALLION ARCHITECTURE: BRONZE → SILVER
# ============================================================================
# Purpose: Clean, validate, and standardize raw data from Bronze layer
# Output: Parquet files in Silver layer
# ============================================================================

from pyspark.sql.functions import (
    col, trim, lower, when, to_timestamp, 
    countDistinct, isnan, isnull, coalesce, current_timestamp
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
import datetime

# COMMAND ----------

# Configuration
STORAGE_ACCOUNT = "saleslake"  # Replace with your Azure storage account
CONTAINER = "lakehouse"  # Replace with your container name
ABFSS_BASE = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net"

# For local testing (without Azure), use local paths:
# ABFSS_BASE = "/home/ubuntu/sales_pipeline"

# COMMAND ----------

# ============================================================================
# 1. LOAD BRONZE DATA (RAW CSVs)
# ============================================================================

print("=" * 80)
print("LOADING BRONZE DATA")
print("=" * 80)

# Load leads
leads_bronze_path = f"{ABFSS_BASE}/bronze/leads"
leads_raw = spark.read.option("header", True).csv(leads_bronze_path)
print(f"\n✓ Leads loaded: {leads_raw.count()} records")
print(f"  Schema: {leads_raw.columns}")

# Load customers
customers_bronze_path = f"{ABFSS_BASE}/bronze/customers"
customers_raw = spark.read.option("header", True).csv(customers_bronze_path)
print(f"\n✓ Customers loaded: {customers_raw.count()} records")
print(f"  Schema: {customers_raw.columns}")

# Load interactions
interactions_bronze_path = f"{ABFSS_BASE}/bronze/interactions"
interactions_raw = spark.read.option("header", True).csv(interactions_bronze_path)
print(f"\n✓ Interactions loaded: {interactions_raw.count()} records")
print(f"  Schema: {interactions_raw.columns}")

# Load products
products_bronze_path = f"{ABFSS_BASE}/bronze/products"
products_raw = spark.read.option("header", True).csv(products_bronze_path)
print(f"\n✓ Products loaded: {products_raw.count()} records")
print(f"  Schema: {products_raw.columns}")

# COMMAND ----------

# ============================================================================
# 2. CLEAN LEADS TABLE
# ============================================================================

print("\n" + "=" * 80)
print("CLEANING LEADS TABLE")
print("=" * 80)

leads_silver = (
    leads_raw
    # Trim and lowercase text fields
    .withColumn("email", trim(lower(col("email"))))
    .withColumn("phone", trim(col("phone")))
    .withColumn("company", trim(col("company")))
    .withColumn("lead_name", trim(col("lead_name")))
    .withColumn("source", trim(col("source")))
    .withColumn("status", trim(lower(col("status"))))
    
    # Convert numeric fields
    .withColumn("deal_value", col("deal_value").cast("double"))
    .withColumn("lead_id", col("lead_id").cast("string"))
    .withColumn("customer_id", col("customer_id").cast("string"))
    
    # Convert timestamps
    .withColumn("created_at", to_timestamp(col("created_at"), "yyyy-MM-dd"))
    .withColumn("updated_at", to_timestamp(col("updated_at"), "yyyy-MM-dd"))
    
    # Handle nulls - mark contactability
    .withColumn(
        "is_contactable",
        when((col("email").isNull()) & (col("phone").isNull()), 0).otherwise(1)
    )
    
    # Handle null deal values
    .withColumn("deal_value", coalesce(col("deal_value"), 0.0))
    
    # Remove duplicates (keep first occurrence by lead_id)
    .dropDuplicates(["lead_id"])
    
    # Add processing timestamp
    .withColumn("processed_at", current_timestamp())
)

# Show data quality metrics
print(f"\n✓ Leads cleaned: {leads_silver.count()} records (removed {leads_raw.count() - leads_silver.count()} duplicates)")
print(f"\n  Data Quality Metrics:")
print(f"  - Null emails: {leads_silver.filter(col('email').isNull()).count()}")
print(f"  - Null phones: {leads_silver.filter(col('phone').isNull()).count()}")
print(f"  - Contactable records: {leads_silver.filter(col('is_contactable') == 1).count()}")
print(f"  - Deal value range: ${leads_silver.agg({'deal_value': 'min'}).collect()[0][0]:.0f} - ${leads_silver.agg({'deal_value': 'max'}).collect()[0][0]:.0f}")

# COMMAND ----------

# ============================================================================
# 3. CLEAN CUSTOMERS TABLE
# ============================================================================

print("\n" + "=" * 80)
print("CLEANING CUSTOMERS TABLE")
print("=" * 80)

customers_silver = (
    customers_raw
    # Trim and lowercase text fields
    .withColumn("customer_name", trim(col("customer_name")))
    .withColumn("industry", trim(col("industry")))
    .withColumn("country", trim(col("country")))
    .withColumn("city", trim(col("city")))
    .withColumn("account_manager", trim(col("account_manager")))
    
    # Convert numeric fields
    .withColumn("employee_count", col("employee_count").cast("integer"))
    .withColumn("annual_revenue", col("annual_revenue").cast("double"))
    .withColumn("customer_id", col("customer_id").cast("string"))
    
    # Convert timestamps
    .withColumn("created_at", to_timestamp(col("created_at"), "yyyy-MM-dd"))
    
    # Handle nulls
    .withColumn("employee_count", coalesce(col("employee_count"), 0))
    .withColumn("annual_revenue", coalesce(col("annual_revenue"), 0.0))
    
    # Remove duplicates
    .dropDuplicates(["customer_id"])
    
    # Add processing timestamp
    .withColumn("processed_at", current_timestamp())
)

print(f"\n✓ Customers cleaned: {customers_silver.count()} records")
print(f"\n  Data Quality Metrics:")
print(f"  - Null account managers: {customers_silver.filter(col('account_manager').isNull()).count()}")
print(f"  - Employee count range: {customers_silver.agg({'employee_count': 'min'}).collect()[0][0]} - {customers_silver.agg({'employee_count': 'max'}).collect()[0][0]}")
print(f"  - Revenue range: ${customers_silver.agg({'annual_revenue': 'min'}).collect()[0][0]:.0f} - ${customers_silver.agg({'annual_revenue': 'max'}).collect()[0][0]:.0f}")

# COMMAND ----------

# ============================================================================
# 4. CLEAN INTERACTIONS TABLE
# ============================================================================

print("\n" + "=" * 80)
print("CLEANING INTERACTIONS TABLE")
print("=" * 80)

interactions_silver = (
    interactions_raw
    # Trim text fields
    .withColumn("channel", trim(lower(col("channel"))))
    .withColumn("outcome", trim(lower(col("outcome"))))
    .withColumn("notes", trim(col("notes")))
    
    # Convert numeric fields
    .withColumn("interaction_time_minutes", col("interaction_time_minutes").cast("integer"))
    .withColumn("interaction_id", col("interaction_id").cast("string"))
    .withColumn("customer_id", col("customer_id").cast("string"))
    
    # Convert timestamps
    .withColumn("interaction_date", to_timestamp(col("interaction_date"), "yyyy-MM-dd"))
    
    # Handle nulls
    .withColumn("interaction_time_minutes", coalesce(col("interaction_time_minutes"), 0))
    .withColumn("notes", coalesce(col("notes"), ""))
    
    # Standardize channel values
    .withColumn(
        "channel_standardized",
        when(col("channel").isin("call", "phone"), "call")
        .when(col("channel").isin("whatsapp", "whatsapp message"), "whatsapp")
        .when(col("channel").isin("email", "e-mail"), "email")
        .otherwise(col("channel"))
    )
    
    # Remove duplicates
    .dropDuplicates(["interaction_id"])
    
    # Add processing timestamp
    .withColumn("processed_at", current_timestamp())
)

print(f"\n✓ Interactions cleaned: {interactions_silver.count()} records")
print(f"\n  Data Quality Metrics:")
print(f"  - Channel distribution:")
for channel in interactions_silver.select("channel_standardized").distinct().collect():
    count = interactions_silver.filter(col("channel_standardized") == channel[0]).count()
    print(f"    • {channel[0]}: {count}")
print(f"  - Outcome distribution:")
for outcome in interactions_silver.select("outcome").distinct().collect():
    count = interactions_silver.filter(col("outcome") == outcome[0]).count()
    print(f"    • {outcome[0]}: {count}")

# COMMAND ----------

# ============================================================================
# 5. CLEAN PRODUCTS TABLE
# ============================================================================

print("\n" + "=" * 80)
print("CLEANING PRODUCTS TABLE")
print("=" * 80)

products_silver = (
    products_raw
    # Trim text fields
    .withColumn("product_name", trim(col("product_name")))
    .withColumn("category", trim(col("category")))
    .withColumn("currency", trim(col("currency")))
    
    # Convert numeric fields
    .withColumn("base_price", col("base_price").cast("double"))
    .withColumn("product_id", col("product_id").cast("string"))
    
    # Convert timestamps
    .withColumn("launch_date", to_timestamp(col("launch_date"), "yyyy-MM-dd"))
    
    # Handle nulls
    .withColumn("base_price", coalesce(col("base_price"), 0.0))
    
    # Remove duplicates
    .dropDuplicates(["product_id"])
    
    # Add processing timestamp
    .withColumn("processed_at", current_timestamp())
)

print(f"\n✓ Products cleaned: {products_silver.count()} records")
print(f"\n  Data Quality Metrics:")
print(f"  - Category distribution:")
for category in products_silver.select("category").distinct().collect():
    count = products_silver.filter(col("category") == category[0]).count()
    print(f"    • {category[0]}: {count}")
print(f"  - Price range: ${products_silver.agg({'base_price': 'min'}).collect()[0][0]:.2f} - ${products_silver.agg({'base_price': 'max'}).collect()[0][0]:.2f}")

# COMMAND ----------

# ============================================================================
# 6. SAVE TO SILVER LAYER (PARQUET)
# ============================================================================

print("\n" + "=" * 80)
print("SAVING TO SILVER LAYER")
print("=" * 80)

# Save leads
leads_silver_path = f"{ABFSS_BASE}/silver/leads_clean"
leads_silver.write.mode("overwrite").parquet(leads_silver_path)
print(f"\n✓ Leads saved to: {leads_silver_path}")

# Save customers
customers_silver_path = f"{ABFSS_BASE}/silver/customers_clean"
customers_silver.write.mode("overwrite").parquet(customers_silver_path)
print(f"✓ Customers saved to: {customers_silver_path}")

# Save interactions
interactions_silver_path = f"{ABFSS_BASE}/silver/interactions_clean"
interactions_silver.write.mode("overwrite").parquet(interactions_silver_path)
print(f"✓ Interactions saved to: {interactions_silver_path}")

# Save products
products_silver_path = f"{ABFSS_BASE}/silver/products_clean"
products_silver.write.mode("overwrite").parquet(products_silver_path)
print(f"✓ Products saved to: {products_silver_path}")

print("\n" + "=" * 80)
print("BRONZE → SILVER TRANSFORMATION COMPLETE")
print("=" * 80)
