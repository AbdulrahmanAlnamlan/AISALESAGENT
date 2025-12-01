# Databricks notebook source
# ============================================================================
# MEDALLION ARCHITECTURE: SILVER → GOLD
# ============================================================================
# Purpose: Feature engineering, aggregations, and business-level curations
# Output: Parquet files optimized for AI agent and Power BI consumption
# ============================================================================

from pyspark.sql.functions import (
    col, count, sum as _sum, avg as _avg, max as _max, min as _min,
    datediff, current_date, when, round as _round, 
    row_number, dense_rank, lag, lead, coalesce,
    date_trunc, date_add, date_sub, current_timestamp
)
from pyspark.sql.window import Window
from datetime import datetime, timedelta

# COMMAND ----------

# Configuration
STORAGE_ACCOUNT = "saleslake"  # Replace with your Azure storage account
CONTAINER = "lakehouse"  # Replace with your container name
ABFSS_BASE = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net"

# For local testing (without Azure), use local paths:
# ABFSS_BASE = "/home/ubuntu/sales_pipeline"

# COMMAND ----------

# ============================================================================
# 1. LOAD SILVER DATA
# ============================================================================

print("=" * 80)
print("LOADING SILVER DATA")
print("=" * 80)

leads_silver = spark.read.parquet(f"{ABFSS_BASE}/silver/leads_clean")
customers_silver = spark.read.parquet(f"{ABFSS_BASE}/silver/customers_clean")
interactions_silver = spark.read.parquet(f"{ABFSS_BASE}/silver/interactions_clean")
products_silver = spark.read.parquet(f"{ABFSS_BASE}/silver/products_clean")

print(f"\n✓ Leads: {leads_silver.count()} records")
print(f"✓ Customers: {customers_silver.count()} records")
print(f"✓ Interactions: {interactions_silver.count()} records")
print(f"✓ Products: {products_silver.count()} records")

# COMMAND ----------

# ============================================================================
# 2. CREATE INTERACTION AGGREGATIONS
# ============================================================================

print("\n" + "=" * 80)
print("CREATING INTERACTION AGGREGATIONS")
print("=" * 80)

# Aggregate interactions by customer
interaction_agg = (
    interactions_silver
    .groupBy("customer_id")
    .agg(
        count("*").alias("total_interactions"),
        _sum(when(col("channel_standardized") == "call", 1).otherwise(0)).alias("num_calls"),
        _sum(when(col("channel_standardized") == "whatsapp", 1).otherwise(0)).alias("num_whatsapp"),
        _sum(when(col("channel_standardized") == "email", 1).otherwise(0)).alias("num_emails"),
        _sum(when(col("outcome") == "positive", 1).otherwise(0)).alias("num_positive_outcomes"),
        _sum(when(col("outcome") == "negative", 1).otherwise(0)).alias("num_negative_outcomes"),
        _sum(when(col("outcome") == "neutral", 1).otherwise(0)).alias("num_neutral_outcomes"),
        _avg("interaction_time_minutes").alias("avg_interaction_duration_minutes"),
        _max("interaction_date").alias("last_interaction_date"),
        _min("interaction_date").alias("first_interaction_date")
    )
    # Calculate days since last interaction
    .withColumn(
        "days_since_last_interaction",
        datediff(current_date(), col("last_interaction_date"))
    )
    # Calculate interaction frequency (interactions per day)
    .withColumn(
        "interaction_frequency_per_day",
        _round(
            col("total_interactions") / 
            (datediff(current_date(), col("first_interaction_date")) + 1),
            4
        )
    )
    # Determine preferred channel
    .withColumn(
        "preferred_channel",
        when(col("num_calls") >= col("num_whatsapp"), 
             when(col("num_calls") >= col("num_emails"), "call").otherwise("email")
        ).otherwise(
            when(col("num_whatsapp") >= col("num_emails"), "whatsapp").otherwise("email")
        )
    )
    # Calculate engagement score (0-100)
    .withColumn(
        "engagement_score",
        _round(
            (col("num_positive_outcomes") * 10 + 
             col("total_interactions") * 2 - 
             col("num_negative_outcomes") * 5) / 
            (col("total_interactions") + 1),
            2
        )
    )
)

print(f"\n✓ Interaction aggregations created: {interaction_agg.count()} customers")
print(f"  - Avg interactions per customer: {interaction_agg.agg({'total_interactions': 'avg'}).collect()[0][0]:.1f}")
print(f"  - Avg engagement score: {interaction_agg.agg({'engagement_score': 'avg'}).collect()[0][0]:.1f}")

# COMMAND ----------

# ============================================================================
# 3. CREATE DEAL/REVENUE AGGREGATIONS
# ============================================================================

print("\n" + "=" * 80)
print("CREATING DEAL AND REVENUE AGGREGATIONS")
print("=" * 80)

# Aggregate deals by customer
deal_agg = (
    leads_silver
    .groupBy("customer_id")
    .agg(
        _sum(when(col("status") == "closed_won", col("deal_value")).otherwise(0)).alias("total_revenue"),
        _sum(when(col("status") == "closed_won", 1).otherwise(0)).alias("num_closed_won"),
        _sum(when(col("status") == "closed_lost", 1).otherwise(0)).alias("num_closed_lost"),
        _sum(when(col("status") == "open", 1).otherwise(0)).alias("num_open_deals"),
        _sum(col("deal_value")).alias("total_deal_value"),
        _avg(when(col("status") == "closed_won", col("deal_value")).otherwise(None)).alias("avg_won_deal_value"),
        _max(col("deal_value")).alias("max_deal_value"),
        count("*").alias("total_deals")
    )
    # Calculate conversion rate
    .withColumn(
        "conversion_rate",
        _round(
            col("num_closed_won") / 
            (col("num_closed_won") + col("num_closed_lost") + 0.001),
            4
        )
    )
    # Calculate win rate
    .withColumn(
        "win_rate",
        _round(
            col("num_closed_won") / (col("total_deals") + 0.001),
            4
        )
    )
    # Calculate average deal value
    .withColumn(
        "avg_deal_value",
        _round(col("total_deal_value") / (col("total_deals") + 0.001), 2)
    )
)

print(f"\n✓ Deal aggregations created: {deal_agg.count()} customers")
print(f"  - Total revenue: ${deal_agg.agg({'total_revenue': 'sum'}).collect()[0][0]:,.0f}")
print(f"  - Avg conversion rate: {deal_agg.agg({'conversion_rate': 'avg'}).collect()[0][0]:.2%}")
print(f"  - Avg deal value: ${deal_agg.agg({'avg_deal_value': 'avg'}).collect()[0][0]:,.0f}")

# COMMAND ----------

# ============================================================================
# 4. CREATE CUSTOMER FEATURES TABLE (MAIN GOLD TABLE)
# ============================================================================

print("\n" + "=" * 80)
print("CREATING CUSTOMER FEATURES TABLE")
print("=" * 80)

# Join all aggregations with customer base data
customer_features = (
    customers_silver.alias("c")
    .join(interaction_agg.alias("i"), "customer_id", "left")
    .join(deal_agg.alias("d"), "customer_id", "left")
    # Fill nulls for customers with no interactions or deals
    .withColumn("total_interactions", coalesce(col("i.total_interactions"), 0))
    .withColumn("num_calls", coalesce(col("i.num_calls"), 0))
    .withColumn("num_whatsapp", coalesce(col("i.num_whatsapp"), 0))
    .withColumn("num_emails", coalesce(col("i.num_emails"), 0))
    .withColumn("num_positive_outcomes", coalesce(col("i.num_positive_outcomes"), 0))
    .withColumn("num_negative_outcomes", coalesce(col("i.num_negative_outcomes"), 0))
    .withColumn("engagement_score", coalesce(col("i.engagement_score"), 0.0))
    .withColumn("preferred_channel", coalesce(col("i.preferred_channel"), "email"))
    .withColumn("days_since_last_interaction", coalesce(col("i.days_since_last_interaction"), 999))
    .withColumn("total_revenue", coalesce(col("d.total_revenue"), 0.0))
    .withColumn("num_closed_won", coalesce(col("d.num_closed_won"), 0))
    .withColumn("num_closed_lost", coalesce(col("d.num_closed_lost"), 0))
    .withColumn("num_open_deals", coalesce(col("d.num_open_deals"), 0))
    .withColumn("conversion_rate", coalesce(col("d.conversion_rate"), 0.0))
    .withColumn("win_rate", coalesce(col("d.win_rate"), 0.0))
    .withColumn("avg_deal_value", coalesce(col("d.avg_deal_value"), 0.0))
    .select(
        "c.customer_id",
        "c.customer_name",
        "c.industry",
        "c.country",
        "c.city",
        "c.employee_count",
        "c.annual_revenue",
        "c.account_manager",
        "c.created_at",
        "total_interactions",
        "num_calls",
        "num_whatsapp",
        "num_emails",
        "num_positive_outcomes",
        "num_negative_outcomes",
        "engagement_score",
        "preferred_channel",
        "days_since_last_interaction",
        "total_revenue",
        "num_closed_won",
        "num_closed_lost",
        "num_open_deals",
        "conversion_rate",
        "win_rate",
        "avg_deal_value"
    )
)

# COMMAND ----------

# ============================================================================
# 5. CREATE LEAD SCORING (AI AGENT READY)
# ============================================================================

print("\n" + "=" * 80)
print("CREATING LEAD SCORING")
print("=" * 80)

# Rule-based lead scoring (0-100 scale)
customer_features = (
    customer_features
    .withColumn(
        "lead_score",
        _round(
            # Revenue component (0-30 points)
            (col("total_revenue") / 100000 * 30) +
            # Interaction component (0-25 points)
            (col("total_interactions") * 2) +
            # Engagement component (0-25 points)
            (col("engagement_score")) +
            # Win rate component (0-20 points)
            (col("win_rate") * 20),
            2
        )
    )
    # Cap lead score at 100
    .withColumn(
        "lead_score",
        when(col("lead_score") > 100, 100).otherwise(col("lead_score"))
    )
    # Categorize lead quality
    .withColumn(
        "lead_quality",
        when(col("lead_score") >= 80, "hot")
        .when(col("lead_score") >= 60, "warm")
        .when(col("lead_score") >= 40, "cool")
        .otherwise("cold")
    )
    # Probability to convert (simple heuristic)
    .withColumn(
        "probability_to_convert",
        _round(
            (col("engagement_score") / 100 * 0.4 + 
             col("conversion_rate") * 0.6),
            4
        )
    )
    # Add processing timestamp
    .withColumn("processed_at", current_timestamp())
)

print(f"\n✓ Lead scoring created")
print(f"  - Lead quality distribution:")
for quality in customer_features.select("lead_quality").distinct().collect():
    count = customer_features.filter(col("lead_quality") == quality[0]).count()
    pct = (count / customer_features.count()) * 100
    print(f"    • {quality[0].upper()}: {count} customers ({pct:.1f}%)")

# COMMAND ----------

# ============================================================================
# 6. CREATE SALES SUMMARY TABLE (DAILY AGGREGATIONS)
# ============================================================================

print("\n" + "=" * 80)
print("CREATING SALES SUMMARY TABLE")
print("=" * 80)

# Daily sales metrics
sales_summary = (
    leads_silver
    .withColumn("deal_date", col("created_at"))
    .groupBy("deal_date")
    .agg(
        count("*").alias("total_deals"),
        _sum(when(col("status") == "closed_won", 1).otherwise(0)).alias("deals_won"),
        _sum(when(col("status") == "closed_lost", 1).otherwise(0)).alias("deals_lost"),
        _sum(when(col("status") == "open", 1).otherwise(0)).alias("deals_open"),
        _sum(when(col("status") == "closed_won", col("deal_value")).otherwise(0)).alias("revenue_won"),
        _sum(col("deal_value")).alias("total_deal_value"),
        countDistinct("customer_id").alias("unique_customers")
    )
    .withColumn(
        "win_rate_daily",
        _round(col("deals_won") / (col("deals_won") + col("deals_lost") + 0.001), 4)
    )
    .withColumn("processed_at", current_timestamp())
    .orderBy(col("deal_date").desc())
)

print(f"\n✓ Sales summary created: {sales_summary.count()} days")
print(f"  - Date range: {sales_summary.agg({'deal_date': 'min'}).collect()[0][0]} to {sales_summary.agg({'deal_date': 'max'}).collect()[0][0]}")

# COMMAND ----------

# ============================================================================
# 7. CREATE CHANNEL PERFORMANCE TABLE
# ============================================================================

print("\n" + "=" * 80)
print("CREATING CHANNEL PERFORMANCE TABLE")
print("=" * 80)

# Channel performance metrics
channel_performance = (
    interactions_silver
    .groupBy("channel_standardized")
    .agg(
        count("*").alias("total_interactions"),
        _sum(when(col("outcome") == "positive", 1).otherwise(0)).alias("positive_outcomes"),
        _sum(when(col("outcome") == "negative", 1).otherwise(0)).alias("negative_outcomes"),
        _sum(when(col("outcome") == "neutral", 1).otherwise(0)).alias("neutral_outcomes"),
        _avg("interaction_time_minutes").alias("avg_duration_minutes"),
        countDistinct("customer_id").alias("unique_customers")
    )
    .withColumn(
        "positive_rate",
        _round(col("positive_outcomes") / (col("total_interactions") + 0.001), 4)
    )
    .withColumn(
        "interactions_per_customer",
        _round(col("total_interactions") / (col("unique_customers") + 0.001), 2)
    )
    .withColumn("processed_at", current_timestamp())
    .orderBy(col("total_interactions").desc())
)

print(f"\n✓ Channel performance created: {channel_performance.count()} channels")
channel_performance.show()

# COMMAND ----------

# ============================================================================
# 8. CREATE TOP CUSTOMERS TABLE (FOR POWER BI)
# ============================================================================

print("\n" + "=" * 80)
print("CREATING TOP CUSTOMERS TABLE")
print("=" * 80)

# Top customers by revenue and engagement
top_customers = (
    customer_features
    .select(
        "customer_id",
        "customer_name",
        "industry",
        "annual_revenue",
        "total_revenue",
        "num_closed_won",
        "total_interactions",
        "engagement_score",
        "lead_score",
        "lead_quality",
        "account_manager"
    )
    .orderBy(col("total_revenue").desc())
    .limit(50)
    .withColumn("processed_at", current_timestamp())
)

print(f"\n✓ Top customers table created: {top_customers.count()} records")

# COMMAND ----------

# ============================================================================
# 9. SAVE ALL GOLD TABLES
# ============================================================================

print("\n" + "=" * 80)
print("SAVING GOLD LAYER TABLES")
print("=" * 80)

# Save customer features (main table for AI agent)
customer_features_path = f"{ABFSS_BASE}/gold/customer_features"
customer_features.write.mode("overwrite").parquet(customer_features_path)
print(f"\n✓ Customer Features saved: {customer_features_path}")

# Save sales summary
sales_summary_path = f"{ABFSS_BASE}/gold/sales_summary"
sales_summary.write.mode("overwrite").parquet(sales_summary_path)
print(f"✓ Sales Summary saved: {sales_summary_path}")

# Save channel performance
channel_performance_path = f"{ABFSS_BASE}/gold/channel_performance"
channel_performance.write.mode("overwrite").parquet(channel_performance_path)
print(f"✓ Channel Performance saved: {channel_performance_path}")

# Save top customers
top_customers_path = f"{ABFSS_BASE}/gold/top_customers"
top_customers.write.mode("overwrite").parquet(top_customers_path)
print(f"✓ Top Customers saved: {top_customers_path}")

print("\n" + "=" * 80)
print("SILVER → GOLD TRANSFORMATION COMPLETE")
print("=" * 80)
print("\n✓ All Gold tables ready for AI agent and Power BI consumption!")
