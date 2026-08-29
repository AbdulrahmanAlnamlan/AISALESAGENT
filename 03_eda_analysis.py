#!/usr/bin/env python3
# ============================================================================
# EXPLORATORY DATA ANALYSIS (EDA)
# ============================================================================
# Purpose: Analyze and visualize Gold layer data
# Can run locally with Pandas or in Databricks with PySpark
# ============================================================================

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import warnings

warnings.filterwarnings('ignore')

# Set style for better visualizations
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (14, 8)
plt.rcParams['font.size'] = 10

# ============================================================================
# CONFIGURATION
# ============================================================================

# Base path for data
BASE_PATH = Path("/home/ubuntu/sales_pipeline")
DATA_PATH = BASE_PATH / "data"
OUTPUT_PATH = BASE_PATH / "analysis_output"

# Create output directory
OUTPUT_PATH.mkdir(exist_ok=True)

print("=" * 80)
print("EXPLORATORY DATA ANALYSIS (EDA)")
print("=" * 80)

# ============================================================================
# 1. LOAD DATA
# ============================================================================

print("\n[1/8] Loading data...")

leads_df = pd.read_csv(DATA_PATH / "leads.csv")
customers_df = pd.read_csv(DATA_PATH / "customers.csv")
interactions_df = pd.read_csv(DATA_PATH / "interactions.csv")
products_df = pd.read_csv(DATA_PATH / "products.csv")

print(f"✓ Leads: {len(leads_df)} records")
print(f"✓ Customers: {len(customers_df)} records")
print(f"✓ Interactions: {len(interactions_df)} records")
print(f"✓ Products: {len(products_df)} records")

# ============================================================================
# 2. DATA QUALITY ASSESSMENT
# ============================================================================

print("\n[2/8] Data Quality Assessment...")

# Leads quality
leads_nulls = leads_df.isnull().sum()
print(f"\n  Leads - Missing Values:")
for col, count in leads_nulls[leads_nulls > 0].items():
    pct = (count / len(leads_df)) * 100
    print(f"    • {col}: {count} ({pct:.1f}%)")

# Customers quality
customers_nulls = customers_df.isnull().sum()
print(f"\n  Customers - Missing Values:")
for col, count in customers_nulls[customers_nulls > 0].items():
    pct = (count / len(customers_df)) * 100
    print(f"    • {col}: {count} ({pct:.1f}%)")

# Interactions quality
interactions_nulls = interactions_df.isnull().sum()
print(f"\n  Interactions - Missing Values:")
for col, count in interactions_nulls[interactions_nulls > 0].items():
    pct = (count / len(interactions_df)) * 100
    print(f"    • {col}: {count} ({pct:.1f}%)")

# ============================================================================
# 3. LEADS ANALYSIS
# ============================================================================

print("\n[3/8] Leads Analysis...")

# Deal value statistics
print(f"\n  Deal Value Statistics:")
print(f"    • Total: ${leads_df['deal_value'].sum():,.0f}")
print(f"    • Average: ${leads_df['deal_value'].mean():,.0f}")
print(f"    • Median: ${leads_df['deal_value'].median():,.0f}")
print(f"    • Min: ${leads_df['deal_value'].min():,.0f}")
print(f"    • Max: ${leads_df['deal_value'].max():,.0f}")

# Deal status distribution
print(f"\n  Deal Status Distribution:")
status_counts = leads_df['status'].value_counts()
for status, count in status_counts.items():
    pct = (count / len(leads_df)) * 100
    print(f"    • {status}: {count} ({pct:.1f}%)")

# Revenue by status
print(f"\n  Revenue by Deal Status:")
revenue_by_status = leads_df.groupby('status')['deal_value'].sum()
for status, revenue in revenue_by_status.items():
    print(f"    • {status}: ${revenue:,.0f}")

# Lead source distribution
print(f"\n  Lead Source Distribution:")
source_counts = leads_df['source'].value_counts()
for source, count in source_counts.items():
    pct = (count / len(leads_df)) * 100
    print(f"    • {source}: {count} ({pct:.1f}%)")

# ============================================================================
# 4. CUSTOMER ANALYSIS
# ============================================================================

print("\n[4/8] Customer Analysis...")

# Industry distribution
print(f"\n  Industry Distribution:")
industry_counts = customers_df['industry'].value_counts()
for industry, count in industry_counts.items():
    pct = (count / len(customers_df)) * 100
    print(f"    • {industry}: {count} ({pct:.1f}%)")

# Company size analysis
print(f"\n  Company Size (Employee Count):")
print(f"    • Average: {customers_df['employee_count'].mean():,.0f}")
print(f"    • Median: {customers_df['employee_count'].median():,.0f}")
print(f"    • Min: {customers_df['employee_count'].min():,.0f}")
print(f"    • Max: {customers_df['employee_count'].max():,.0f}")

# Revenue analysis
print(f"\n  Annual Revenue Statistics:")
print(f"    • Total: ${customers_df['annual_revenue'].sum():,.0f}")
print(f"    • Average: ${customers_df['annual_revenue'].mean():,.0f}")
print(f"    • Median: ${customers_df['annual_revenue'].median():,.0f}")

# ============================================================================
# 5. INTERACTION ANALYSIS
# ============================================================================

print("\n[5/8] Interaction Analysis...")

# Channel distribution
print(f"\n  Interaction Channel Distribution:")
channel_counts = interactions_df['channel'].value_counts()
for channel, count in channel_counts.items():
    pct = (count / len(interactions_df)) * 100
    print(f"    • {channel}: {count} ({pct:.1f}%)")

# Outcome distribution
print(f"\n  Interaction Outcome Distribution:")
outcome_counts = interactions_df['outcome'].value_counts()
for outcome, count in outcome_counts.items():
    pct = (count / len(interactions_df)) * 100
    print(f"    • {outcome}: {count} ({pct:.1f}%)")

# Duration analysis
print(f"\n  Interaction Duration Statistics:")
print(f"    • Average: {interactions_df['interaction_time_minutes'].mean():.1f} minutes")
print(f"    • Median: {interactions_df['interaction_time_minutes'].median():.1f} minutes")
print(f"    • Max: {interactions_df['interaction_time_minutes'].max():.1f} minutes")

# ============================================================================
# 6. FEATURE ENGINEERING (SIMULATE GOLD LAYER)
# ============================================================================

print("\n[6/8] Creating Customer Features (Gold Layer Simulation)...")

# Merge data to create customer features
customer_interactions = interactions_df.groupby('customer_id').agg({
    'interaction_id': 'count',
    'interaction_time_minutes': 'mean',
    'interaction_date': 'max'
}).rename(columns={
    'interaction_id': 'total_interactions',
    'interaction_time_minutes': 'avg_interaction_duration',
    'interaction_date': 'last_interaction_date'
})

# Channel breakdown
channel_pivot = interactions_df.groupby(['customer_id', 'channel']).size().unstack(fill_value=0)
channel_pivot.columns = [f'num_{col}' for col in channel_pivot.columns]

# Outcome breakdown
outcome_pivot = interactions_df.groupby(['customer_id', 'outcome']).size().unstack(fill_value=0)
outcome_pivot.columns = [f'num_{col}_outcomes' for col in outcome_pivot.columns]

# Deal metrics
customer_deals = leads_df.groupby('customer_id').agg({
    'deal_value': ['sum', 'count', 'mean'],
    'status': lambda x: (x == 'closed_won').sum()
}).reset_index()

customer_deals.columns = ['customer_id', 'total_revenue', 'total_deals', 'avg_deal_value', 'num_closed_won']
customer_deals = customer_deals.set_index('customer_id')

# Combine all features
customer_features = customers_df.set_index('customer_id').copy()
customer_features = customer_features.join(customer_interactions, how='left')
customer_features = customer_features.join(channel_pivot, how='left')
customer_features = customer_features.join(outcome_pivot, how='left')
customer_features = customer_features.join(customer_deals, how='left')

# Fill NaNs
customer_features = customer_features.fillna(0)

# Calculate engagement score
customer_features['engagement_score'] = (
    customer_features.get('num_positive_outcomes', 0) * 10 +
    customer_features.get('total_interactions', 0) * 2 -
    customer_features.get('num_negative_outcomes', 0) * 5
) / (customer_features.get('total_interactions', 1) + 1)

# Calculate lead score
customer_features['lead_score'] = (
    (customer_features.get('total_revenue', 0) / 100000 * 30) +
    (customer_features.get('total_interactions', 0) * 2) +
    (customer_features.get('engagement_score', 0) * 25) +
    ((customer_features.get('num_closed_won', 0) / (customer_features.get('total_deals', 1) + 0.001)) * 20)
).clip(0, 100)

# Lead quality categorization
customer_features['lead_quality'] = pd.cut(
    customer_features['lead_score'],
    bins=[0, 40, 60, 80, 100],
    labels=['cold', 'cool', 'warm', 'hot']
)

print(f"✓ Customer features created: {len(customer_features)} records")
print(f"\n  Lead Quality Distribution:")
for quality in ['hot', 'warm', 'cool', 'cold']:
    count = (customer_features['lead_quality'] == quality).sum()
    pct = (count / len(customer_features)) * 100
    print(f"    • {quality.upper()}: {count} ({pct:.1f}%)")

# ============================================================================
# 7. CREATE VISUALIZATIONS
# ============================================================================

print("\n[7/8] Creating Visualizations...")

# 1. Deal Value Distribution
fig, axes = plt.subplots(2, 2, figsize=(16, 12))

# Deal value by status
ax1 = axes[0, 0]
deal_by_status = leads_df.groupby('status')['deal_value'].sum().sort_values(ascending=False)
colors = ['#2ecc71', '#e74c3c', '#3498db']
deal_by_status.plot(kind='bar', ax=ax1, color=colors)
ax1.set_title('Total Revenue by Deal Status', fontsize=14, fontweight='bold')
ax1.set_ylabel('Revenue ($)')
ax1.set_xlabel('Status')
ax1.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x/1e6:.1f}M'))
plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45)

# Deal count by source
ax2 = axes[0, 1]
source_counts = leads_df['source'].value_counts()
ax2.pie(source_counts.values, labels=source_counts.index, autopct='%1.1f%%', startangle=90)
ax2.set_title('Leads by Source', fontsize=14, fontweight='bold')

# Interaction channel distribution
ax3 = axes[1, 0]
channel_counts = interactions_df['channel'].value_counts()
channel_counts.plot(kind='barh', ax=ax3, color='#3498db')
ax3.set_title('Interactions by Channel', fontsize=14, fontweight='bold')
ax3.set_xlabel('Count')

# Interaction outcome distribution
ax4 = axes[1, 1]
outcome_counts = interactions_df['outcome'].value_counts()
colors_outcome = ['#2ecc71', '#e74c3c', '#95a5a6']
outcome_counts.plot(kind='bar', ax=ax4, color=colors_outcome)
ax4.set_title('Interaction Outcomes', fontsize=14, fontweight='bold')
ax4.set_ylabel('Count')
plt.setp(ax4.xaxis.get_majorticklabels(), rotation=45)

plt.tight_layout()
plt.savefig(OUTPUT_PATH / '01_deals_and_interactions.png', dpi=300, bbox_inches='tight')
print(f"  ✓ Saved: 01_deals_and_interactions.png")
plt.close()

# 2. Customer Features Analysis
fig, axes = plt.subplots(2, 2, figsize=(16, 12))

# Lead score distribution
ax1 = axes[0, 0]
ax1.hist(customer_features['lead_score'], bins=15, color='#3498db', edgecolor='black')
ax1.set_title('Lead Score Distribution', fontsize=14, fontweight='bold')
ax1.set_xlabel('Lead Score')
ax1.set_ylabel('Number of Customers')
ax1.axvline(customer_features['lead_score'].mean(), color='red', linestyle='--', label=f"Mean: {customer_features['lead_score'].mean():.1f}")
ax1.legend()

# Lead quality pie chart
ax2 = axes[0, 1]
quality_counts = customer_features['lead_quality'].value_counts()
colors_quality = {'hot': '#e74c3c', 'warm': '#f39c12', 'cool': '#3498db', 'cold': '#95a5a6'}
colors_list = [colors_quality.get(q, '#95a5a6') for q in quality_counts.index]
ax2.pie(quality_counts.values, labels=quality_counts.index, autopct='%1.1f%%', colors=colors_list, startangle=90)
ax2.set_title('Customer Distribution by Lead Quality', fontsize=14, fontweight='bold')

# Revenue vs Engagement Score
ax3 = axes[1, 0]
scatter = ax3.scatter(
    customer_features.get('total_interactions', 0),
    customer_features.get('total_revenue', 0),
    c=customer_features['lead_score'],
    s=100,
    cmap='RdYlGn',
    alpha=0.6,
    edgecolors='black'
)
ax3.set_title('Total Revenue vs Total Interactions', fontsize=14, fontweight='bold')
ax3.set_xlabel('Total Interactions')
ax3.set_ylabel('Total Revenue ($)')
ax3.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x/1e6:.1f}M'))
cbar = plt.colorbar(scatter, ax=ax3)
cbar.set_label('Lead Score')

# Top 10 customers by revenue
ax4 = axes[1, 1]
if 'total_revenue' in customer_features.columns:
    top_customers = customer_features.nlargest(10, 'total_revenue')[['customer_name', 'total_revenue']].sort_values('total_revenue')
    ax4.barh(range(len(top_customers)), top_customers['total_revenue'].values, color='#2ecc71')
    ax4.set_yticks(range(len(top_customers)))
    ax4.set_yticklabels(top_customers['customer_name'].values, fontsize=9)
    ax4.set_title('Top 10 Customers by Revenue', fontsize=14, fontweight='bold')
    ax4.set_xlabel('Revenue ($)')
    ax4.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x/1e6:.1f}M'))
else:
    ax4.text(0.5, 0.5, 'Revenue data not available', ha='center', va='center')
    ax4.set_title('Top 10 Customers by Revenue', fontsize=14, fontweight='bold')

plt.tight_layout()
plt.savefig(OUTPUT_PATH / '02_customer_features.png', dpi=300, bbox_inches='tight')
print(f"  ✓ Saved: 02_customer_features.png")
plt.close()

# 3. Industry and Company Size Analysis
fig, axes = plt.subplots(1, 2, figsize=(16, 6))

# Industry distribution
ax1 = axes[0]
industry_revenue = customers_df.groupby('industry')['annual_revenue'].sum().sort_values(ascending=False)
industry_revenue.plot(kind='barh', ax=ax1, color='#9b59b6')
ax1.set_title('Annual Revenue by Industry', fontsize=14, fontweight='bold')
ax1.set_xlabel('Annual Revenue ($)')
ax1.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x/1e9:.1f}B'))

# Company size vs revenue
ax2 = axes[1]
scatter = ax2.scatter(
    customers_df['employee_count'],
    customers_df['annual_revenue'],
    s=150,
    alpha=0.6,
    c=customers_df['annual_revenue'],
    cmap='viridis',
    edgecolors='black'
)
ax2.set_title('Company Size vs Annual Revenue', fontsize=14, fontweight='bold')
ax2.set_xlabel('Employee Count')
ax2.set_ylabel('Annual Revenue ($)')
ax2.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x/1e9:.1f}B'))
cbar = plt.colorbar(scatter, ax=ax2)
cbar.set_label('Revenue ($)')

plt.tight_layout()
plt.savefig(OUTPUT_PATH / '03_industry_analysis.png', dpi=300, bbox_inches='tight')
print(f"  ✓ Saved: 03_industry_analysis.png")
plt.close()

# ============================================================================
# 8. SAVE SUMMARY REPORT
# ============================================================================

print("\n[8/8] Generating Summary Report...")

# Save customer features to CSV
customer_features.to_csv(OUTPUT_PATH / 'customer_features.csv')
print(f"  ✓ Saved: customer_features.csv")

# Create summary report
summary_report = f"""
{'='*80}
MEDALLION ARCHITECTURE - EXPLORATORY DATA ANALYSIS REPORT
{'='*80}

EXECUTIVE SUMMARY
{'-'*80}
Total Customers: {len(customers_df)}
Total Leads: {len(leads_df)}
Total Interactions: {len(interactions_df)}
Total Products: {len(products_df)}

LEADS METRICS
{'-'*80}
Total Deal Value: ${leads_df['deal_value'].sum():,.0f}
Average Deal Value: ${leads_df['deal_value'].mean():,.0f}
Median Deal Value: ${leads_df['deal_value'].median():,.0f}

Deal Status Breakdown:
"""

for status, count in leads_df['status'].value_counts().items():
    pct = (count / len(leads_df)) * 100
    revenue = leads_df[leads_df['status'] == status]['deal_value'].sum()
    summary_report += f"  • {status.upper()}: {count} deals (${revenue:,.0f}) - {pct:.1f}%\n"

summary_report += f"""
Lead Source Breakdown:
"""

for source, count in leads_df['source'].value_counts().items():
    pct = (count / len(leads_df)) * 100
    summary_report += f"  • {source}: {count} leads - {pct:.1f}%\n"

summary_report += f"""
CUSTOMER METRICS
{'-'*80}
Total Annual Revenue: ${customers_df['annual_revenue'].sum():,.0f}
Average Company Size: {customers_df['employee_count'].mean():,.0f} employees
Largest Company: {customers_df['employee_count'].max():,.0f} employees
Smallest Company: {customers_df['employee_count'].min():,.0f} employees

Top 5 Industries:
"""

for industry, count in customers_df['industry'].value_counts().head(5).items():
    pct = (count / len(customers_df)) * 100
    summary_report += f"  • {industry}: {count} customers - {pct:.1f}%\n"

summary_report += f"""
INTERACTION METRICS
{'-'*80}
Total Interactions: {len(interactions_df)}
Average Interaction Duration: {interactions_df['interaction_time_minutes'].mean():.1f} minutes

Channel Distribution:
"""

for channel, count in interactions_df['channel'].value_counts().items():
    pct = (count / len(interactions_df)) * 100
    summary_report += f"  • {channel}: {count} interactions - {pct:.1f}%\n"

summary_report += f"""
Outcome Distribution:
"""

for outcome, count in interactions_df['outcome'].value_counts().items():
    pct = (count / len(interactions_df)) * 100
    summary_report += f"  • {outcome}: {count} interactions - {pct:.1f}%\n"

summary_report += f"""
GOLD LAYER - CUSTOMER FEATURES
{'-'*80}
Average Lead Score: {customer_features['lead_score'].mean():.1f}
Median Lead Score: {customer_features['lead_score'].median():.1f}
Max Lead Score: {customer_features['lead_score'].max():.1f}

Lead Quality Distribution:
"""

for quality in ['hot', 'warm', 'cool', 'cold']:
    count = (customer_features['lead_quality'] == quality).sum()
    pct = (count / len(customer_features)) * 100
    summary_report += f"  • {quality.upper()}: {count} customers - {pct:.1f}%\n"

summary_report += f"""
Average Engagement Score: {customer_features['engagement_score'].mean():.1f}
Average Total Interactions per Customer: {customer_features['total_interactions'].mean():.1f}
Average Total Revenue per Customer: ${customer_features.get('total_revenue', pd.Series([0])).mean():,.0f}

TOP 10 CUSTOMERS BY LEAD SCORE
{'-'*80}
"""

top_10 = customer_features.nlargest(10, 'lead_score')[['customer_name', 'lead_score', 'total_revenue', 'lead_quality']]
for idx, (cust_id, row) in enumerate(top_10.iterrows(), 1):
    summary_report += f"{idx:2d}. {row['customer_name']:30s} | Score: {row['lead_score']:6.1f} | Revenue: ${row['total_revenue']:>12,.0f} | Quality: {str(row['lead_quality']).upper()}\n"

summary_report += f"""
{'='*80}
VISUALIZATIONS GENERATED
{'-'*80}
✓ 01_deals_and_interactions.png - Overview of deals and interactions
✓ 02_customer_features.png - Customer features and lead scoring analysis
✓ 03_industry_analysis.png - Industry and company size analysis
✓ customer_features.csv - Full customer features dataset (Gold layer)

{'='*80}
NEXT STEPS
{'-'*80}
1. Import customer_features.csv into Power BI
2. Create dashboards for:
   - Lead scoring and quality distribution
   - Revenue by industry and customer
   - Interaction channel effectiveness
   - Sales pipeline metrics
3. Use customer features in AI sales agent for:
   - Lead prioritization
   - Personalized outreach
   - Channel recommendation
   - Conversion prediction

{'='*80}
"""

# Save report
with open(OUTPUT_PATH / 'EDA_REPORT.txt', 'w') as f:
    f.write(summary_report)

print(f"  ✓ Saved: EDA_REPORT.txt")
print(summary_report)

print("\n" + "=" * 80)
print("EDA COMPLETE - All outputs saved to:", OUTPUT_PATH)
print("=" * 80)
