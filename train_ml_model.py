#!/usr/bin/env python3
# ============================================================================
# LEAD SCORING MODEL TRAINING
# ============================================================================
# Purpose: Train ML model to predict lead conversion probability
# Output: Trained model saved as pickle file
# ============================================================================

import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestClassifier
from xgboost import XGBClassifier
from sklearn.metrics import (
    accuracy_score, precision_score, recall_score, f1_score,
    roc_auc_score, confusion_matrix, classification_report
)
import pickle
import argparse
from pathlib import Path
import warnings

warnings.filterwarnings('ignore')

# ============================================================================
# CONFIGURATION
# ============================================================================

MODEL_OUTPUT_PATH = Path("models/lead_scoring_model.pkl")
MODEL_OUTPUT_PATH.parent.mkdir(exist_ok=True)

FEATURES = [
    'total_interactions',
    'num_calls',
    'num_whatsapp',
    'num_emails',
    'num_positive_outcomes',
    'num_negative_outcomes',
    'engagement_score',
    'total_revenue',
    'num_closed_won',
    'num_closed_lost',
    'conversion_rate',
    'win_rate',
    'avg_deal_value',
    'employee_count',
    'annual_revenue'
]

TARGET = 'is_hot_lead'  # 1 if lead_score >= 80, 0 otherwise

# ============================================================================
# MAIN TRAINING FUNCTION
# ============================================================================

def load_data(data_path):
    """Load customer features data"""
    print("=" * 80)
    print("LOADING DATA")
    print("=" * 80)
    
    df = pd.read_csv(data_path)
    print(f"\n✓ Data loaded: {len(df)} records")
    print(f"  Columns: {df.columns.tolist()}")
    
    return df

def prepare_data(df):
    """Prepare data for model training"""
    print("\n" + "=" * 80)
    print("PREPARING DATA")
    print("=" * 80)
    
    # Create target variable (hot lead if score >= 80)
    if 'lead_score' in df.columns:
        df[TARGET] = (df['lead_score'] >= 80).astype(int)
    else:
        # If lead_score not available, use heuristic
        df[TARGET] = (
            (df.get('total_revenue', 0) > df.get('total_revenue', 0).median()) &
            (df.get('total_interactions', 0) > df.get('total_interactions', 0).median())
        ).astype(int)
    
    print(f"\n✓ Target variable created")
    print(f"  Hot leads: {(df[TARGET] == 1).sum()} ({(df[TARGET] == 1).sum() / len(df) * 100:.1f}%)")
    print(f"  Cold leads: {(df[TARGET] == 0).sum()} ({(df[TARGET] == 0).sum() / len(df) * 100:.1f}%)")
    
    # Select features that exist in data
    available_features = [f for f in FEATURES if f in df.columns]
    print(f"\n✓ Using {len(available_features)} features:")
    for f in available_features:
        print(f"    • {f}")
    
    # Fill missing values
    X = df[available_features].fillna(0)
    y = df[TARGET]
    
    print(f"\n✓ Data prepared: {X.shape[0]} samples, {X.shape[1]} features")
    
    return X, y, available_features

def train_models(X, y):
    """Train multiple models"""
    print("\n" + "=" * 80)
    print("TRAINING MODELS")
    print("=" * 80)
    
    # Split data
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )
    
    print(f"\n✓ Train/test split: {len(X_train)} / {len(X_test)}")
    
    # Scale features
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)
    
    print(f"✓ Features scaled")
    
    # Train Random Forest
    print("\n  Training Random Forest...")
    rf_model = RandomForestClassifier(
        n_estimators=100,
        max_depth=10,
        random_state=42,
        n_jobs=-1
    )
    rf_model.fit(X_train, y_train)
    rf_pred = rf_model.predict(X_test)
    rf_pred_proba = rf_model.predict_proba(X_test)[:, 1]
    
    print(f"    ✓ Random Forest trained")
    
    # Train XGBoost
    print("  Training XGBoost...")
    xgb_model = XGBClassifier(
        n_estimators=100,
        max_depth=5,
        learning_rate=0.1,
        random_state=42,
        n_jobs=-1
    )
    xgb_model.fit(X_train, y_train)
    xgb_pred = xgb_model.predict(X_test)
    xgb_pred_proba = xgb_model.predict_proba(X_test)[:, 1]
    
    print(f"    ✓ XGBoost trained")
    
    # Evaluate models
    print("\n" + "=" * 80)
    print("MODEL EVALUATION")
    print("=" * 80)
    
    print("\nRANDOM FOREST:")
    print_metrics(y_test, rf_pred, rf_pred_proba)
    
    print("\nXGBOOST:")
    print_metrics(y_test, xgb_pred, xgb_pred_proba)
    
    # Select best model (XGBoost typically performs better)
    best_model = xgb_model
    print("\n✓ Selected model: XGBoost")
    
    return best_model, scaler, X_test, y_test, xgb_pred, xgb_pred_proba

def print_metrics(y_true, y_pred, y_pred_proba):
    """Print model evaluation metrics"""
    accuracy = accuracy_score(y_true, y_pred)
    precision = precision_score(y_true, y_pred, zero_division=0)
    recall = recall_score(y_true, y_pred, zero_division=0)
    f1 = f1_score(y_true, y_pred, zero_division=0)
    roc_auc = roc_auc_score(y_true, y_pred_proba)
    
    print(f"  Accuracy:  {accuracy:.4f}")
    print(f"  Precision: {precision:.4f}")
    print(f"  Recall:    {recall:.4f}")
    print(f"  F1-Score:  {f1:.4f}")
    print(f"  ROC-AUC:   {roc_auc:.4f}")
    
    print(f"\n  Confusion Matrix:")
    cm = confusion_matrix(y_true, y_pred)
    print(f"    TN: {cm[0,0]}, FP: {cm[0,1]}")
    print(f"    FN: {cm[1,0]}, TP: {cm[1,1]}")
    
    print(f"\n  Classification Report:")
    print(classification_report(y_true, y_pred, zero_division=0))

def save_model(model, scaler, features, output_path):
    """Save trained model and scaler"""
    print("\n" + "=" * 80)
    print("SAVING MODEL")
    print("=" * 80)
    
    model_data = {
        'model': model,
        'scaler': scaler,
        'features': features,
        'model_type': 'XGBoost'
    }
    
    with open(output_path, 'wb') as f:
        pickle.dump(model_data, f)
    
    print(f"\n✓ Model saved to: {output_path}")
    print(f"  Model type: XGBoost")
    print(f"  Features: {len(features)}")
    print(f"  File size: {output_path.stat().st_size / 1024:.2f} KB")

def load_and_test_model(model_path, X_test, y_test):
    """Load model and test on new data"""
    print("\n" + "=" * 80)
    print("TESTING LOADED MODEL")
    print("=" * 80)
    
    with open(model_path, 'rb') as f:
        model_data = pickle.load(f)
    
    model = model_data['model']
    scaler = model_data['scaler']
    
    print(f"\n✓ Model loaded from: {model_path}")
    
    # Make predictions
    y_pred = model.predict(X_test)
    y_pred_proba = model.predict_proba(X_test)[:, 1]
    
    print(f"\n✓ Predictions made on {len(X_test)} test samples")
    print_metrics(y_test, y_pred, y_pred_proba)

def main():
    """Main training pipeline"""
    parser = argparse.ArgumentParser(description='Train lead scoring model')
    parser.add_argument('--data-path', default='analysis/customer_features.csv',
                       help='Path to customer features CSV')
    parser.add_argument('--model-output', default='models/lead_scoring_model.pkl',
                       help='Output path for trained model')
    
    args = parser.parse_args()
    
    # Load data
    df = load_data(args.data_path)
    
    # Prepare data
    X, y, features = prepare_data(df)
    
    # Train models
    model, scaler, X_test, y_test, y_pred, y_pred_proba = train_models(X, y)
    
    # Save model
    save_model(model, scaler, features, Path(args.model_output))
    
    # Test loaded model
    load_and_test_model(Path(args.model_output), X_test, y_test)
    
    print("\n" + "=" * 80)
    print("MODEL TRAINING COMPLETE")
    print("=" * 80)
    print(f"\n✓ Model ready for deployment: {args.model_output}")

if __name__ == "__main__":
    main()
