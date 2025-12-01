# Medallion Architecture: AI Sales Agent Data Pipeline

**Academic Project for Data Engineering Course**

A complete implementation of the **Bronze → Silver → Gold medallion architecture** for building an AI-powered sales agent data pipeline using Azure Databricks and PySpark.

## 📋 Project Overview

This project demonstrates a production-grade data engineering solution that transforms raw sales data through multiple transformation layers, creating curated datasets optimized for analytics and AI consumption. The implementation covers the complete data lifecycle: ingestion, cleaning, transformation, feature engineering, and deployment.

### Architecture Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                  DATA PIPELINE ARCHITECTURE                 │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  BRONZE LAYER (Raw Data)                                   │
│  ├── leads.csv (15 records)                                │
│  ├── customers.csv (13 records)                            │
│  ├── interactions.csv (25 records)                         │
│  └── products.csv (8 records)                              │
│                ↓                                             │
│  SILVER LAYER (Cleaned & Standardized)                     │
│  ├── leads_clean/ (Parquet)                                │
│  ├── customers_clean/ (Parquet)                            │
│  ├── interactions_clean/ (Parquet)                         │
│  └── products_clean/ (Parquet)                             │
│                ↓                                             │
│  GOLD LAYER (Curated & Feature-Engineered)                 │
│  ├── customer_features/ (25+ features)                     │
│  ├── sales_summary/ (Daily metrics)                        │
│  ├── channel_performance/ (Channel analytics)              │
│  └── top_customers/ (Power BI ready)                       │
│                ↓                                             │
│  ANALYTICS & AI CONSUMPTION                                │
│  ├── Power BI Dashboards                                   │
│  ├── AI Sales Agent                                        │
│  └── Business Intelligence                                 │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

---

## 📁 Repository Structure

```
medallion-architecture-pipeline/
│
├── README.md                          # This file
├── ARCHITECTURE.md                    # Detailed technical architecture
├── SETUP_GUIDE.md                     # Step-by-step setup instructions
│
├── data/                              # Bronze Layer - Raw Data
│   ├── leads.csv                      # 15 lead records
│   ├── customers.csv                  # 13 customer records
│   ├── interactions.csv               # 25 interaction records
│   └── products.csv                   # 8 product records
│
├── notebooks/                         # Databricks Notebooks
│   ├── 01_bronze_to_silver.py        # Data cleaning & standardization
│   ├── 02_silver_to_gold.py          # Feature engineering
│   └── 03_eda_analysis.py            # Exploratory data analysis
│
├── scripts/                           # Deployment & Utility Scripts
│   ├── deploy_to_databricks.py       # Databricks deployment script
│   ├── train_ml_model.py             # ML model training (lead scoring)
│   ├── setup_azure_storage.sh        # Azure infrastructure setup
│   └── upload_data.py                # Data upload to Azure
│
├── models/                            # ML Models
│   ├── lead_scoring_model.pkl        # Trained lead scoring model
│   ├── model_training.py             # Model training code
│   └── model_evaluation.py           # Model evaluation metrics
│
├── dashboards/                        # Power BI Dashboards
│   ├── sales_pipeline_dashboard.pbix # Main dashboard
│   └── dashboard_guide.md            # Dashboard setup guide
│
├── analysis/                          # EDA & Analysis Results
│   ├── 01_deals_and_interactions.png # Visualization
│   ├── 02_customer_features.png      # Visualization
│   ├── 03_industry_analysis.png      # Visualization
│   ├── customer_features.csv         # Gold layer features
│   └── EDA_REPORT.txt                # Analysis report
│
├── docs/                              # Documentation
│   ├── BRONZE_LAYER.md               # Bronze layer details
│   ├── SILVER_LAYER.md               # Silver layer details
│   ├── GOLD_LAYER.md                 # Gold layer details
│   ├── FEATURE_ENGINEERING.md        # Feature definitions
│   └── DEPLOYMENT.md                 # Deployment guide
│
├── tests/                             # Unit Tests
│   ├── test_bronze_to_silver.py      # Silver layer tests
│   ├── test_silver_to_gold.py        # Gold layer tests
│   └── test_data_quality.py          # Data quality tests
│
├── config/                            # Configuration Files
│   ├── config.yaml                   # Pipeline configuration
│   ├── requirements.txt               # Python dependencies
│   └── databricks_config.json        # Databricks settings
│
└── presentation/                      # Academic Presentation
    ├── presentation.pptx              # 6-slide presentation
    └── presentation_notes.md          # Speaker notes
```

---

## 🎯 Key Features

### Bronze Layer
- **Raw data ingestion** from 4 sources (Leads, Customers, Interactions, Products)
- **61 total records** with realistic data quality issues
- **CSV format** for easy understanding and modification
- **No transformations** - data as-is from source systems

### Silver Layer
- **Text normalization** (trim, lowercase, standardization)
- **Type conversions** (dates, numerics, proper data types)
- **Null handling** (smart defaults, validation)
- **Duplicate removal** (deduplication by key fields)
- **Parquet format** (efficient columnar storage)
- **Data quality metrics** (automatic validation)

### Gold Layer
- **customer_features** - 25+ engineered features per customer
- **sales_summary** - Daily pipeline metrics and trends
- **channel_performance** - Channel effectiveness analysis
- **top_customers** - Power BI ready top 50 customers

### Feature Engineering
- **lead_score** (0-100) - Rule-based and ML-based scoring
- **lead_quality** - Hot/Warm/Cool/Cold categorization
- **engagement_score** - Interaction quality metric
- **probability_to_convert** - ML-ready conversion prediction
- **preferred_channel** - Recommended contact method
- **20+ additional features** for comprehensive analysis

---

## 📊 Data Insights

| Metric | Value |
|--------|-------|
| **Total Deal Value** | $1,937,000 |
| **Average Deal Value** | $129,133 |
| **Total Customers** | 13 |
| **Total Interactions** | 25 |
| **Average Lead Score** | 88.7/100 |
| **Hot Leads** | 84.6% |
| **Win Rate** | 40% |
| **Top Customer Revenue** | $550,000 |

---

## 🚀 Quick Start

### Option 1: Local Testing (No Azure Required)

```bash
# Clone repository
git clone https://github.com/yourusername/medallion-architecture-pipeline.git
cd medallion-architecture-pipeline

# Install dependencies
pip install -r config/requirements.txt

# Run EDA analysis
python notebooks/03_eda_analysis.py

# Output: Visualizations and reports in analysis/
```

### Option 2: Azure Databricks Deployment

See [SETUP_GUIDE.md](SETUP_GUIDE.md) for detailed instructions:

1. Create Azure Storage Account (ADLS Gen2)
2. Create "lakehouse" container
3. Upload data to Bronze layer
4. Create Databricks cluster
5. Import notebooks
6. Run transformation pipeline
7. Connect Power BI dashboard

---

## 📚 Documentation

| Document | Purpose |
|----------|---------|
| [ARCHITECTURE.md](ARCHITECTURE.md) | Complete technical architecture |
| [SETUP_GUIDE.md](SETUP_GUIDE.md) | Step-by-step deployment guide |
| [BRONZE_LAYER.md](docs/BRONZE_LAYER.md) | Raw data specifications |
| [SILVER_LAYER.md](docs/SILVER_LAYER.md) | Data cleaning transformations |
| [GOLD_LAYER.md](docs/GOLD_LAYER.md) | Feature engineering details |
| [FEATURE_ENGINEERING.md](docs/FEATURE_ENGINEERING.md) | Feature definitions |
| [DEPLOYMENT.md](docs/DEPLOYMENT.md) | Production deployment guide |

---

## 🔧 Technology Stack

| Component | Technology |
|-----------|-----------|
| **Storage** | Azure Data Lake Storage Gen2 (ADLS Gen2) |
| **Compute** | Azure Databricks |
| **Processing** | Apache Spark (PySpark) |
| **Format** | Parquet (columnar, compressed) |
| **Visualization** | Power BI |
| **ML Framework** | scikit-learn, XGBoost |
| **Orchestration** | Azure Data Factory (optional) |
| **Language** | Python 3.x, PySpark SQL |

---

## 📈 Notebooks Overview

### 01_bronze_to_silver.py (400+ lines)
**Purpose:** Data cleaning and standardization
- Loads raw CSV files from Bronze layer
- Applies cleaning transformations
- Removes duplicates
- Converts data types
- Saves cleaned Parquet files to Silver layer
- **Execution Time:** ~2 minutes

### 02_silver_to_gold.py (350+ lines)
**Purpose:** Feature engineering and aggregations
- Loads cleaned Parquet files from Silver layer
- Creates interaction aggregations per customer
- Creates revenue aggregations per customer
- Calculates lead scores and engagement metrics
- Creates business-level summary tables
- **Execution Time:** ~1 minute

### 03_eda_analysis.py (500+ lines)
**Purpose:** Exploratory data analysis
- Loads raw data from Bronze layer
- Performs data quality assessment
- Generates descriptive statistics
- Creates professional visualizations
- Simulates Gold layer features
- **Execution Time:** ~3 minutes

---

## 🤖 Model Training

### Lead Scoring Model

**Location:** `scripts/train_ml_model.py`

**Features Used:**
- total_revenue
- total_interactions
- engagement_score
- conversion_rate
- win_rate
- days_since_last_interaction
- num_calls, num_emails, num_whatsapp

**Model Types:**
1. **Rule-Based Model** - Weighted formula (baseline)
2. **ML Model** - XGBoost classifier (prediction)

**Training Process:**
```python
# Load Gold layer features
customer_features = spark.read.parquet("gold/customer_features")

# Train/test split
train_df, test_df = customer_features.randomSplit([0.8, 0.2])

# Train XGBoost model
model = xgboost.XGBClassifier(...)
model.fit(X_train, y_train)

# Evaluate
accuracy = model.score(X_test, y_test)
```

**Output:**
- Trained model saved as `models/lead_scoring_model.pkl`
- Model evaluation metrics in `models/model_evaluation.py`

---

## 🚀 Deployment

### Azure Databricks Deployment

**Script:** `scripts/deploy_to_databricks.py`

**Steps:**
1. Configure Databricks workspace
2. Create cluster
3. Upload notebooks
4. Configure storage access
5. Run transformation pipeline
6. Deploy to production

### Power BI Dashboard

**File:** `dashboards/sales_pipeline_dashboard.pbix`

**Dashboards Included:**
1. Lead Scoring & Pipeline
2. Customer Analytics
3. Interaction Analytics
4. Sales Performance

---

## 📊 Sample Data

All sample data is included in the `data/` folder:

- **leads.csv** - 15 lead records with deal information
- **customers.csv** - 13 customer records with company profiles
- **interactions.csv** - 25 interaction records (calls, emails, WhatsApp)
- **products.csv** - 8 product records with pricing

---

## 🧪 Testing

Run unit tests to verify data quality and transformations:

```bash
# Test Silver layer transformations
python tests/test_bronze_to_silver.py

# Test Gold layer feature engineering
python tests/test_silver_to_gold.py

# Test data quality
python tests/test_data_quality.py
```

---

## 📊 EDA Results

Three professional visualizations are included:

1. **01_deals_and_interactions.png** - Deal status, lead sources, channels, outcomes
2. **02_customer_features.png** - Lead scores, quality distribution, revenue analysis
3. **03_industry_analysis.png** - Industry revenue, company size analysis

---

## 🎓 Learning Outcomes

This project demonstrates:

✅ **Data Engineering Fundamentals**
- Medallion architecture design
- ETL/ELT pipeline development
- Data quality and validation

✅ **Cloud Technologies**
- Azure Data Lake Storage Gen2
- Azure Databricks
- PySpark distributed processing

✅ **Data Transformation**
- Cleaning and standardization
- Feature engineering
- Aggregations and joins

✅ **Machine Learning**
- Feature selection
- Model training and evaluation
- Production deployment

✅ **Business Analytics**
- Lead scoring
- Customer segmentation
- Performance metrics

---

## 📝 Project Statistics

| Metric | Value |
|--------|-------|
| **Total Files** | 30+ |
| **Python Code** | 1,500+ lines |
| **Documentation** | 2,000+ lines |
| **Data Records** | 61 |
| **Features Engineered** | 25+ |
| **Tables Created** | 4 |
| **Visualizations** | 3 |
| **Test Cases** | 10+ |

---

## 🔗 Links

- **GitHub Repository:** [medallion-architecture-pipeline](https://github.com/yourusername/medallion-architecture-pipeline)
- **Presentation:** See `presentation/presentation.pptx`
- **Setup Guide:** See [SETUP_GUIDE.md](SETUP_GUIDE.md)
- **Architecture:** See [ARCHITECTURE.md](ARCHITECTURE.md)

---

## 📞 Support

For questions or issues:
1. Check the [SETUP_GUIDE.md](SETUP_GUIDE.md)
2. Review the documentation in `docs/`
3. Check the notebook comments
4. Review test cases in `tests/`

---

## 📄 License

This project is for educational purposes.

---

## 👤 Author

**Student Name:** [Your Name]  
**Course:** Data Engineering / Cloud Computing  
**University:** [Your University]  
**Date:** December 2025

---

**Ready to deploy? Start with [SETUP_GUIDE.md](SETUP_GUIDE.md)!**
