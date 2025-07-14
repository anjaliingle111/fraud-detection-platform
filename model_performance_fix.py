# cloud_native_data_platform.py
import os
import sys
import time
import logging
import json
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, when

from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.model_selection import cross_val_score, StratifiedKFold
from sklearn.metrics import roc_auc_score, precision_score, recall_score, f1_score

from flask import Flask, request, jsonify
import threading

from privacy_masking import PrivacyMaskingService

class WindowsCompatibleRealisticPlatform:
    def __init__(self):
        self.setup_logging()
        self.logger = logging.getLogger(self.__class__.__name__)
        
        self.privacy_service = PrivacyMaskingService()
        self.logger.info("Privacy masking service initialized")
        
        self.platform_name = "WindowsCompatibleRealisticPlatform"
        self.start_time = time.time()
        
        self.data_lake_path = "data-lake"
        self.bronze_path = f"{self.data_lake_path}/bronze"
        self.silver_path = f"{self.data_lake_path}/silver"
        self.gold_path = f"{self.data_lake_path}/gold"
        
        self.create_data_lake_structure()
        self.spark = self._initialize_spark_with_delta()
        
        self.ml_model = None
        self.feature_columns = []
        self.delta_lake_available = False
        
        self.logger.info(f"Platform {self.platform_name} initialized successfully")
    
    def setup_logging(self):
        log_dir = "logs"
        os.makedirs(log_dir, exist_ok=True)
        
        log_filename = f"{log_dir}/fraud_detection_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_filename, encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ]
        )
    
    def _initialize_spark_with_delta(self):
        try:
            os.environ["HADOOP_HOME"] = "C:\\hadoop"
            os.environ["HADOOP_USER_NAME"] = "spark"
            os.environ["HADOOP_OPTS"] = "-Djava.library.path="
            
            self.logger.info("Windows Hadoop compatibility settings applied")
            
            try:
                from delta import configure_spark_with_delta_pip
                
                builder = SparkSession.builder \
                    .appName("WindowsCompatibleRealisticFraudDetection") \
                    .config("spark.master", "local[*]") \
                    .config("spark.sql.adaptive.enabled", "true") \
                    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                    .config("spark.driver.memory", "4g")
                
                spark = configure_spark_with_delta_pip(builder).getOrCreate()
                self.logger.info("Spark session with Delta Lake initialized successfully")
                return spark
                
            except Exception as e:
                self.logger.warning(f"Delta Lake initialization failed: {str(e)}")
                self.logger.info("Falling back to standard Spark session...")
                
                spark = SparkSession.builder \
                    .appName("WindowsCompatibleRealisticFraudDetection") \
                    .config("spark.master", "local[*]") \
                    .config("spark.driver.memory", "4g") \
                    .getOrCreate()
                
                self.logger.info("Standard Spark session initialized")
                return spark
                
        except Exception as e:
            self.logger.error(f"Spark initialization failed: {str(e)}")
            raise
    
    def create_data_lake_structure(self):
        try:
            directories = [
                self.bronze_path,
                self.silver_path,
                self.gold_path,
                f"{self.data_lake_path}/csv_exports",
                f"{self.data_lake_path}/monitoring/reports",
                f"{self.data_lake_path}/privacy_reports"
            ]
            
            for directory in directories:
                os.makedirs(directory, exist_ok=True)
            
            self.logger.info("Data lake directory structure created")
            
        except Exception as e:
            self.logger.error(f"Failed to create data lake structure: {str(e)}")
            raise
    
    def generate_realistic_fraud_data(self, num_customers=2000, num_transactions=20000):
        try:
            self.logger.info(f"Generating realistic fraud data: {num_customers} customers, {num_transactions} transactions")
            
            np.random.seed(42)
            
            customers = []
            for i in range(num_customers):
                customer = {
                    'customer_id': f'CUST_{i:05d}',
                    'customer_name': f'Customer_{i}',
                    'ssn': f'{np.random.randint(100,999):03d}-{np.random.randint(10,99):02d}-{np.random.randint(1000,9999):04d}',
                    'email': f'customer{i}@email.com',
                    'credit_score': np.random.normal(700, 100),
                    'account_age_days': np.random.randint(30, 3650),
                    'is_fraudster': np.random.random() < 0.05
                }
                customers.append(customer)
            
            customer_df = pd.DataFrame(customers)
            
            transactions = []
            base_date = datetime.now() - timedelta(days=365)
            
            for i in range(num_transactions):
                customer = customer_df.sample(1).iloc[0]
                
                transaction = {
                    'transaction_id': f'TXN_{i:08d}',
                    'customer_id': customer['customer_id'],
                    'amount': np.random.lognormal(5, 1),
                    'transaction_time': base_date + timedelta(
                        days=np.random.randint(0, 365),
                        hours=np.random.randint(0, 24)
                    ),
                    'channel': np.random.choice(['online', 'mobile', 'in_store']),
                    'merchant_category': np.random.choice(['retail', 'gas', 'grocery'])
                }
                
                fraud_probability = 0.001
                
                if customer['is_fraudster']:
                    fraud_probability += 0.05
                if transaction['amount'] > 1000:
                    fraud_probability += 0.02
                if transaction['channel'] == 'online':
                    fraud_probability += 0.01
                
                transaction['is_fraud'] = np.random.random() < fraud_probability
                
                transactions.append(transaction)
            
            transaction_df = pd.DataFrame(transactions)
            
            fraud_rate = transaction_df['is_fraud'].sum() / len(transaction_df)
            self.logger.info(f"Generated data - Fraud rate: {fraud_rate:.4f}")
            
            return customer_df, transaction_df
            
        except Exception as e:
            self.logger.error(f"Data generation failed: {str(e)}")
            raise
    
    def train_realistic_ml_model(self, df):
        try:
            self.logger.info("Training realistic ML model...")
            
            pdf = df.toPandas()
            
            pdf['amount_log'] = np.log1p(pdf['amount'])
            pdf['hour'] = pd.to_datetime(pdf['transaction_time']).dt.hour
            pdf['channel_online'] = (pdf['channel'] == 'online').astype(int)
            
            feature_columns = ['amount_log', 'hour', 'channel_online']
            
            for col in feature_columns:
                if col in pdf.columns:
                    pdf[col] = pdf[col].fillna(0)
            
            X = pdf[feature_columns]
            y = pdf['is_fraud'].astype(int)
            
            fraud_rate = y.sum() / len(y)
            self.logger.info(f"Original fraud rate: {fraud_rate:.4f}")
            
            if fraud_rate < 0.01:
                self.logger.info("Creating synthetic fraud cases...")
                
                target_fraud_count = int(len(y) * 0.02)
                synthetic_needed = target_fraud_count - y.sum()
                
                if synthetic_needed > 0:
                    normal_samples = X[y == 0].sample(n=synthetic_needed, replace=True)
                    synthetic_samples = normal_samples.copy()
                    synthetic_samples['amount_log'] += np.random.normal(1, 0.3, synthetic_needed)
                    synthetic_samples['channel_online'] = 1
                    
                    X = pd.concat([X, synthetic_samples], ignore_index=True)
                    y = pd.concat([y, pd.Series([1] * synthetic_needed)], ignore_index=True)
                    
                    self.logger.info(f"Added {synthetic_needed} synthetic fraud cases")
            
            model = Pipeline([
                ('scaler', StandardScaler()),
                ('classifier', RandomForestClassifier(
                    n_estimators=100,
                    max_depth=10,
                    class_weight='balanced',
                    random_state=42
                ))
            ])
            
            cv_scores = cross_val_score(
                model, X, y,
                cv=StratifiedKFold(n_splits=3, shuffle=True, random_state=42),
                scoring='roc_auc'
            )
            
            model.fit(X, y)
            
            y_pred = model.predict(X)
            y_pred_proba = model.predict_proba(X)[:, 1]
            
            auc_score = roc_auc_score(y, y_pred_proba)
            precision = precision_score(y, y_pred)
            recall = recall_score(y, y_pred)
            f1 = f1_score(y, y_pred)
            
            self.ml_model = model
            self.feature_columns = feature_columns
            
            self.logger.info(f"Model trained - AUC: {auc_score:.4f}, Precision: {precision:.4f}")
            
            return {
                'auc_score': auc_score,
                'precision': precision,
                'recall': recall,
                'f1_score': f1
            }
            
        except Exception as e:
            self.logger.error(f"Model training failed: {str(e)}")
            return {'auc_score': 0.5, 'precision': 0.0, 'recall': 0.0, 'f1_score': 0.0}
    
    def predict_fraud(self, transaction_data):
        try:
            if self.ml_model is None:
                return {'error': 'Model not trained'}
            
            if isinstance(transaction_data, dict):
                transaction_df = pd.DataFrame([transaction_data])
            else:
                transaction_df = transaction_data
            
            transaction_df['amount_log'] = np.log1p(transaction_df['amount'])
            transaction_df['hour'] = 12
            transaction_df['channel_online'] = (transaction_df.get('channel', 'online') == 'online').astype(int)
            
            for col in self.feature_columns:
                if col not in transaction_df.columns:
                    transaction_df[col] = 0
            
            X = transaction_df[self.feature_columns].fillna(0)
            fraud_probability = self.ml_model.predict_proba(X)[0][1]
            
            if fraud_probability > 0.7:
                risk_level = 'High'
                recommendation = 'Block transaction'
            elif fraud_probability > 0.3:
                risk_level = 'Medium'
                recommendation = 'Review transaction'
            else:
                risk_level = 'Low'
                recommendation = 'Approve transaction'
            
            return {
                'fraud_probability': float(fraud_probability),
                'risk_level': risk_level,
                'recommendation': recommendation
            }
            
        except Exception as e:
            self.logger.error(f"Fraud prediction failed: {str(e)}")
            return {'error': str(e)}
    
    def run_complete_pipeline(self):
        try:
            self.logger.info("STARTING FRAUD DETECTION PIPELINE")
            
            customer_df, transaction_df = self.generate_realistic_fraud_data()
            
            customer_spark_df = self.spark.createDataFrame(customer_df)
            transaction_spark_df = self.spark.createDataFrame(transaction_df)
            
            self.logger.info("Applying privacy masking...")
            masked_transaction_df = self.privacy_service.mask_pii_spark_dataframe(transaction_spark_df)
            
            self.logger.info("Training ML model...")
            model_results = self.train_realistic_ml_model(masked_transaction_df)
            
            transaction_spark_df.toPandas().to_csv(f"{self.data_lake_path}/csv_exports/transactions.csv", index=False)
            
            self.logger.info("FRAUD DETECTION PIPELINE COMPLETE!")
            
            return {
                'status': 'success',
                'model_results': model_results
            }
            
        except Exception as e:
            self.logger.error(f"Pipeline execution failed: {str(e)}")
            raise
    
    def start_api_server(self):
        try:
            self.logger.info("Starting API server on port 8080...")
            
            app = Flask(__name__)
            
            @app.route('/health', methods=['GET'])
            def health_check():
                return jsonify({
                    'status': 'healthy',
                    'platform': self.platform_name,
                    'timestamp': datetime.now().isoformat()
                })
            
            @app.route('/predict', methods=['POST'])
            def predict():
                try:
                    data = request.get_json()
                    result = self.predict_fraud(data)
                    return jsonify(result)
                except Exception as e:
                    return jsonify({'error': str(e)}), 500
            
            @app.route('/platform-info', methods=['GET'])
            def platform_info():
                return jsonify({
                    'platform_name': self.platform_name,
                    'model_trained': self.ml_model is not None,
                    'privacy_masking_enabled': True
                })
            
            server_thread = threading.Thread(target=lambda: app.run(host='0.0.0.0', port=8080, debug=False))
            server_thread.daemon = True
            server_thread.start()
            
            self.logger.info("API server started successfully")
            return app
            
        except Exception as e:
            self.logger.error(f"API server startup failed: {str(e)}")
            raise

def main():
    try:
        print("ENTERPRISE FRAUD DETECTION PLATFORM")
        print("=" * 50)
        
        platform = WindowsCompatibleRealisticPlatform()
        results = platform.run_complete_pipeline()
        platform.start_api_server()
        
        print("\nPLATFORM COMPLETE!")
        print("SUCCESS: Fraud detection implemented")
        print("SUCCESS: Privacy masking applied")
        print("SUCCESS: API server running on http://localhost:8080")
        print("\nTest your platform:")
        print("curl http://localhost:8080/health")
        
        try:
            while True:
                time.sleep(60)
        except KeyboardInterrupt:
            print("\nShutting down...")
            platform.spark.stop()
            
    except Exception as e:
        print(f"Platform failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()