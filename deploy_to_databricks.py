#!/usr/bin/env python3
# ============================================================================
# DATABRICKS DEPLOYMENT SCRIPT
# ============================================================================
# Purpose: Deploy pipeline notebooks to Azure Databricks
# Usage: python deploy_to_databricks.py --workspace-url <URL> --token <TOKEN>
# ============================================================================

import requests
import json
import argparse
from pathlib import Path
import sys

# ============================================================================
# CONFIGURATION
# ============================================================================

NOTEBOOKS = [
    {
        'local_path': 'notebooks/01_bronze_to_silver.py',
        'remote_path': '/Workspace/sales_pipeline/01_bronze_to_silver',
        'language': 'PYTHON'
    },
    {
        'local_path': 'notebooks/02_silver_to_gold.py',
        'remote_path': '/Workspace/sales_pipeline/02_silver_to_gold',
        'language': 'PYTHON'
    },
    {
        'local_path': 'notebooks/03_eda_analysis.py',
        'remote_path': '/Workspace/sales_pipeline/03_eda_analysis',
        'language': 'PYTHON'
    }
]

# ============================================================================
# DATABRICKS API FUNCTIONS
# ============================================================================

class DatabricksClient:
    """Client for Databricks API"""
    
    def __init__(self, workspace_url, token):
        """Initialize Databricks client"""
        self.workspace_url = workspace_url.rstrip('/')
        self.token = token
        self.headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }
    
    def create_directory(self, path):
        """Create directory in workspace"""
        url = f"{self.workspace_url}/api/2.0/workspace/mkdirs"
        data = {'path': path}
        
        response = requests.post(url, json=data, headers=self.headers)
        
        if response.status_code not in [200, 400]:  # 400 if already exists
            raise Exception(f"Failed to create directory: {response.text}")
        
        return True
    
    def upload_notebook(self, local_path, remote_path, language='PYTHON'):
        """Upload notebook to workspace"""
        
        # Read notebook content
        with open(local_path, 'r') as f:
            content = f.read()
        
        # Prepare upload request
        url = f"{self.workspace_url}/api/2.0/workspace/import"
        data = {
            'path': remote_path,
            'format': 'SOURCE',
            'language': language,
            'overwrite': True,
            'content': content
        }
        
        response = requests.post(url, json=data, headers=self.headers)
        
        if response.status_code != 200:
            raise Exception(f"Failed to upload notebook: {response.text}")
        
        return True
    
    def create_job(self, job_name, notebook_path, cluster_id, schedule=None):
        """Create scheduled job"""
        
        url = f"{self.workspace_url}/api/2.1/jobs/create"
        
        data = {
            'name': job_name,
            'tasks': [
                {
                    'task_key': 'run_notebook',
                    'notebook_task': {
                        'notebook_path': notebook_path
                    },
                    'existing_cluster_id': cluster_id
                }
            ]
        }
        
        # Add schedule if provided
        if schedule:
            data['schedule'] = schedule
        
        response = requests.post(url, json=data, headers=self.headers)
        
        if response.status_code != 200:
            raise Exception(f"Failed to create job: {response.text}")
        
        job_id = response.json()['job_id']
        return job_id
    
    def test_connection(self):
        """Test connection to Databricks"""
        url = f"{self.workspace_url}/api/2.0/workspace/get-status"
        data = {'path': '/'}
        
        response = requests.get(url, json=data, headers=self.headers)
        
        return response.status_code == 200

# ============================================================================
# DEPLOYMENT FUNCTIONS
# ============================================================================

def deploy_notebooks(client, notebooks):
    """Deploy all notebooks to Databricks"""
    
    print("=" * 80)
    print("DEPLOYING NOTEBOOKS")
    print("=" * 80)
    
    # Create workspace directory
    print("\n1. Creating workspace directory...")
    try:
        client.create_directory('/Workspace/sales_pipeline')
        print("   ✓ Directory created")
    except Exception as e:
        print(f"   ✗ Error: {e}")
        return False
    
    # Upload notebooks
    print("\n2. Uploading notebooks...")
    for notebook in notebooks:
        local_path = notebook['local_path']
        remote_path = notebook['remote_path']
        language = notebook['language']
        
        if not Path(local_path).exists():
            print(f"   ✗ File not found: {local_path}")
            continue
        
        try:
            client.upload_notebook(local_path, remote_path, language)
            print(f"   ✓ Uploaded: {local_path}")
        except Exception as e:
            print(f"   ✗ Failed to upload {local_path}: {e}")
            return False
    
    return True

def create_jobs(client, cluster_id, notebooks):
    """Create scheduled jobs for pipeline"""
    
    print("\n" + "=" * 80)
    print("CREATING SCHEDULED JOBS")
    print("=" * 80)
    
    jobs = []
    
    # Job 1: Bronze to Silver (2:00 AM)
    print("\n1. Creating Bronze to Silver job...")
    try:
        job_id = client.create_job(
            job_name='bronze_to_silver_daily',
            notebook_path='/Workspace/sales_pipeline/01_bronze_to_silver',
            cluster_id=cluster_id,
            schedule={
                'quartz_cron_expression': '0 0 2 * * ?',  # 2:00 AM daily
                'timezone_id': 'UTC'
            }
        )
        print(f"   ✓ Job created (ID: {job_id})")
        jobs.append(job_id)
    except Exception as e:
        print(f"   ✗ Error: {e}")
    
    # Job 2: Silver to Gold (3:00 AM)
    print("\n2. Creating Silver to Gold job...")
    try:
        job_id = client.create_job(
            job_name='silver_to_gold_daily',
            notebook_path='/Workspace/sales_pipeline/02_silver_to_gold',
            cluster_id=cluster_id,
            schedule={
                'quartz_cron_expression': '0 0 3 * * ?',  # 3:00 AM daily
                'timezone_id': 'UTC'
            }
        )
        print(f"   ✓ Job created (ID: {job_id})")
        jobs.append(job_id)
    except Exception as e:
        print(f"   ✗ Error: {e}")
    
    return jobs

def main():
    """Main deployment function"""
    
    parser = argparse.ArgumentParser(description='Deploy pipeline to Databricks')
    parser.add_argument('--workspace-url', required=True,
                       help='Databricks workspace URL (e.g., https://your-workspace.cloud.databricks.com)')
    parser.add_argument('--token', required=True,
                       help='Databricks personal access token')
    parser.add_argument('--cluster-id', 
                       help='Cluster ID for scheduled jobs (optional)')
    
    args = parser.parse_args()
    
    # Initialize client
    print("=" * 80)
    print("DATABRICKS DEPLOYMENT")
    print("=" * 80)
    
    print(f"\nWorkspace URL: {args.workspace_url}")
    
    client = DatabricksClient(args.workspace_url, args.token)
    
    # Test connection
    print("\nTesting connection...")
    if not client.test_connection():
        print("✗ Failed to connect to Databricks")
        print("  Check workspace URL and token")
        sys.exit(1)
    
    print("✓ Connected to Databricks")
    
    # Deploy notebooks
    if not deploy_notebooks(client, NOTEBOOKS):
        print("\n✗ Deployment failed")
        sys.exit(1)
    
    # Create jobs if cluster ID provided
    if args.cluster_id:
        jobs = create_jobs(client, args.cluster_id, NOTEBOOKS)
        
        print("\n" + "=" * 80)
        print("DEPLOYMENT SUMMARY")
        print("=" * 80)
        print(f"\n✓ Notebooks deployed: {len(NOTEBOOKS)}")
        print(f"✓ Scheduled jobs created: {len(jobs)}")
        print("\nSchedules:")
        print("  • Bronze to Silver: 2:00 AM UTC daily")
        print("  • Silver to Gold: 3:00 AM UTC daily")
    else:
        print("\n" + "=" * 80)
        print("DEPLOYMENT SUMMARY")
        print("=" * 80)
        print(f"\n✓ Notebooks deployed: {len(NOTEBOOKS)}")
        print("⚠ No scheduled jobs created (provide --cluster-id to enable)")
    
    print("\n" + "=" * 80)
    print("NEXT STEPS")
    print("=" * 80)
    print("""
1. Go to your Databricks workspace
2. Open the notebooks in /Workspace/sales_pipeline/
3. Update storage account name and key
4. Run notebooks manually or wait for scheduled execution
5. Monitor job runs in the Jobs section
    """)

if __name__ == "__main__":
    main()
