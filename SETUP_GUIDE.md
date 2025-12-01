# Setup Guide: Medallion Architecture Pipeline

Complete step-by-step guide to deploy the medallion architecture pipeline on Azure Databricks.

## 📋 Prerequisites

- Azure subscription
- Databricks workspace
- Python 3.8+
- Git
- Azure CLI (optional)

---

## 🔧 Step 1: Azure Infrastructure Setup

### 1.1 Create Storage Account

```bash
# Using Azure CLI
az storage account create \
  --name saleslake60313569 \
  --resource-group your-resource-group \
  --location eastus \
  --sku Standard_LRS \
  --kind StorageV2 \
  --hierarchical-namespace true
```

Or use Azure Portal:
1. Create Storage Account
2. Enable "Hierarchical namespace"
3. Note the storage account name and key

### 1.2 Create Container

```bash
# Create "lakehouse" container
az storage container create \
  --account-name saleslake60313569 \
  --name lakehouse
```

### 1.3 Get Storage Account Key

```bash
# Get account key
az storage account keys list \
  --account-name saleslake60313569 \
  --query "[0].value" -o tsv
```

Save this key - you'll need it for Databricks configuration.

---

## 📤 Step 2: Upload Raw Data

### 2.1 Using Azure CLI

```bash
# Create bronze folder structure
az storage blob upload-batch \
  --account-name saleslake60313569 \
  --destination lakehouse/bronze/leads \
  --source data/leads.csv

az storage blob upload-batch \
  --account-name saleslake60313569 \
  --destination lakehouse/bronze/customers \
  --source data/customers.csv

az storage blob upload-batch \
  --account-name saleslake60313569 \
  --destination lakehouse/bronze/interactions \
  --source data/interactions.csv

az storage blob upload-batch \
  --account-name saleslake60313569 \
  --destination lakehouse/bronze/products \
  --source data/products.csv
```

### 2.2 Using Python Script

```bash
# Use provided upload script
python scripts/upload_data.py \
  --storage-account saleslake60313569 \
  --storage-key <YOUR-ACCOUNT-KEY> \
  --container lakehouse \
  --data-path data/
```

### 2.3 Using Azure Storage Explorer

1. Download Azure Storage Explorer
2. Connect to your storage account
3. Create folder: `lakehouse/bronze/`
4. Upload CSV files to respective folders

---

## 🏗️ Step 3: Databricks Setup

### 3.1 Create Databricks Workspace

1. Go to Azure Portal
2. Create new Databricks Workspace
3. Select region close to storage account
4. Create workspace

### 3.2 Create Cluster

1. In Databricks workspace, click "Compute"
2. Click "Create Cluster"
3. Configure:
   ```
   Cluster name: sales-pipeline
   Databricks Runtime: 12.x or 13.x
   Worker type: Standard (4GB RAM)
   Number of workers: 2-4
   Driver type: Same as workers
   ```
4. Click "Create Cluster"
5. Wait for cluster to start

### 3.3 Create Notebooks

1. In Databricks, click "Workspace"
2. Create folder: `/Workspace/sales_pipeline/`
3. Import notebooks:
   - `notebooks/01_bronze_to_silver.py`
   - `notebooks/02_silver_to_gold.py`
   - `notebooks/03_eda_analysis.py`

---

## 🔐 Step 4: Configure Storage Access

### 4.1 Using Account Key (Simple)

Add this to the first cell of each notebook:

```python
# Configure ADLS Gen2 access
storage_account = "saleslake60313569"  # Replace with YOUR account name
container = "lakehouse"
abfss_base = f"abfss://{container}@{storage_account}.dfs.core.windows.net"

# Configure storage access
spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    "<YOUR-ACCOUNT-KEY>"
)

# Test connection
dbutils.fs.ls(f"{abfss_base}/bronze/")
```

### 4.2 Using Service Principal (Recommended)

```python
# Configure with service principal
spark.conf.set("fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", 
               "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", 
               "<CLIENT-ID>")
spark.conf.set("fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", 
               "<CLIENT-SECRET>")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", 
               "https://login.microsoftonline.com/<TENANT-ID>/oauth2/v2.0/token")
```

---

## ▶️ Step 5: Run Pipeline

### 5.1 Run Bronze to Silver

1. Open notebook: `01_bronze_to_silver.py`
2. Update storage account name
3. Click "Run All"
4. Wait for completion (~2 minutes)
5. Verify Silver layer created in ADLS Gen2

### 5.2 Run Silver to Gold

1. Open notebook: `02_silver_to_gold.py`
2. Update storage account name
3. Click "Run All"
4. Wait for completion (~1 minute)
5. Verify Gold layer created in ADLS Gen2

### 5.3 Run EDA Analysis

1. Open notebook: `03_eda_analysis.py`
2. Click "Run All"
3. Review visualizations and statistics

---

## 📊 Step 6: Verify Results

### 6.1 Check ADLS Gen2 Folders

```
lakehouse/
├── bronze/
│   ├── leads/leads.csv
│   ├── customers/customers.csv
│   ├── interactions/interactions.csv
│   └── products/products.csv
├── silver/
│   ├── leads_clean/
│   ├── customers_clean/
│   ├── interactions_clean/
│   └── products_clean/
└── gold/
    ├── customer_features/
    ├── sales_summary/
    ├── channel_performance/
    └── top_customers/
```

### 6.2 Query Gold Layer

```python
# In Databricks notebook
customer_features = spark.read.parquet("abfss://lakehouse@saleslake60313569.dfs.core.windows.net/gold/customer_features/")
customer_features.display()

# Show statistics
customer_features.describe().show()
```

---

## 📈 Step 7: Connect Power BI (Optional)

### 7.1 Create SQL Endpoint

1. In Databricks, click "SQL"
2. Click "Create SQL Endpoint"
3. Configure:
   ```
   Name: sales-pipeline-endpoint
   Cluster size: Small
   ```
4. Wait for endpoint to start

### 7.2 Connect Power BI

1. Open Power BI Desktop
2. Get Data → Azure Databricks
3. Enter SQL endpoint URL
4. Authenticate with token
5. Load Gold layer tables
6. Create visualizations

---

## 🤖 Step 8: Train ML Model (Optional)

### 8.1 Run Model Training

```bash
# Train lead scoring model
python scripts/train_ml_model.py \
  --data-path analysis/customer_features.csv \
  --model-output models/lead_scoring_model.pkl
```

### 8.2 Evaluate Model

```bash
# Evaluate model performance
python scripts/model_evaluation.py \
  --model-path models/lead_scoring_model.pkl \
  --test-data analysis/customer_features.csv
```

---

## 🧪 Step 9: Run Tests

```bash
# Install test dependencies
pip install pytest

# Run all tests
pytest tests/

# Run specific test
pytest tests/test_bronze_to_silver.py -v
```

---

## 🚀 Step 10: Production Deployment

### 10.1 Using Deployment Script

```bash
# Deploy to production
python scripts/deploy_to_databricks.py \
  --workspace-url https://your-workspace.cloud.databricks.com \
  --token <DATABRICKS-TOKEN> \
  --cluster-id <CLUSTER-ID>
```

### 10.2 Schedule Pipeline

1. In Azure Data Factory, create pipeline
2. Add activities:
   - Run 01_bronze_to_silver.py (2:00 AM)
   - Run 02_silver_to_gold.py (3:00 AM)
3. Set daily trigger
4. Configure alerts

---

## 📋 Checklist

- [ ] Azure Storage Account created
- [ ] ADLS Gen2 container "lakehouse" created
- [ ] Raw data uploaded to Bronze layer
- [ ] Databricks workspace created
- [ ] Databricks cluster created
- [ ] Notebooks imported
- [ ] Storage access configured
- [ ] 01_bronze_to_silver.py executed
- [ ] 02_silver_to_gold.py executed
- [ ] 03_eda_analysis.py executed
- [ ] Gold layer verified
- [ ] Power BI connected (optional)
- [ ] ML model trained (optional)
- [ ] Tests passed
- [ ] Production deployment completed

---

## 🔧 Troubleshooting

### Issue: Storage account not found
**Solution:** Update storage account name in notebook configuration

### Issue: Parquet file not found
**Solution:** Ensure Bronze → Silver completed successfully

### Issue: Power BI connection timeout
**Solution:** Check Databricks SQL endpoint is running

### Issue: Lead score is null
**Solution:** Verify Silver layer tables were created

### Issue: Out of memory
**Solution:** Increase cluster size or partition data

---

## 📞 Support

For issues or questions:
1. Check this guide
2. Review notebook comments
3. Check Azure Databricks documentation
4. Review test cases

---

**Next Steps:**
1. Follow steps 1-7 for basic setup
2. Follow step 8 for ML model training
3. Follow step 9 for testing
4. Follow step 10 for production deployment

**Happy deploying!** 🚀
