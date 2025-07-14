"""
PRODUCTION DATA ENGINEERING PLATFORM - COMPLETELY FIXED
========================================================
Real-world data engineering with PySpark + Parquet
- Late-arriving data handling
- Schema evolution tracking
- Data quality monitoring
- SCD Type 2 implementation
- ML feature engineering
- Production monitoring
"""

import os
import sys
import json
import time
import uuid
import hashlib
import random
import math
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union
import logging
from dataclasses import dataclass, asdict
from enum import Enum
import warnings
warnings.filterwarnings('ignore')

# Core libraries
import pandas as pd
import numpy as np
from faker import Faker

# PySpark imports
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window


class DataQualityRule(Enum):
    """Data quality rule types"""
    NOT_NULL = "not_null"
    UNIQUE = "unique"
    RANGE = "range"
    PATTERN = "pattern"
    FOREIGN_KEY = "foreign_key"
    CUSTOM = "custom"


@dataclass
class DataQualityCheck:
    """Data quality check definition"""
    rule_type: DataQualityRule
    column: str
    threshold: float = 0.95
    parameters: Dict = None
    description: str = ""


@dataclass
class SchemaEvolutionEvent:
    """Schema evolution tracking"""
    table_name: str
    event_type: str
    old_schema: Dict
    new_schema: Dict
    timestamp: datetime
    impact_score: float


class ProductionDataPlatform:
    """
    Production-Grade Data Engineering Platform
    
    Features:
    - Messy data with quality issues
    - Late-arriving data processing
    - Schema evolution management
    - SCD Type 2 implementation
    - Advanced Spark optimizations
    - Production monitoring
    """
    
    def __init__(self, 
                 app_name: str = "ProductionDataPlatform",
                 data_lake_path: str = "data-lake",
                 log_level: str = "INFO"):
        
        self.app_name = app_name
        self.data_lake_path = Path(data_lake_path)
        self.setup_logging(log_level)
        
        # Initialize pure Spark session
        self.spark = self.initialize_spark_session()
        
        # Data quality thresholds
        self.data_quality_thresholds = {
            'completeness': 0.95,
            'uniqueness': 0.98,
            'validity': 0.90,
            'consistency': 0.85,
            'timeliness': 0.80
        }
        
        # Schema registry
        self.schema_registry = {}
        self.schema_evolution_log = []
        
        # Monitoring metrics
        self.metrics = {
            'records_processed': 0,
            'data_quality_violations': 0,
            'schema_evolution_events': 0,
            'late_arriving_records': 0,
            'duplicate_records': 0,
            'processing_time_ms': 0
        }
        
        # Create directory structure
        self.setup_directory_structure()
        
        self.logger.info(f"Platform {self.app_name} initialized successfully")
        
    def setup_logging(self, log_level: str):
        """Setup logging - Windows compatible"""
        logging.basicConfig(
            level=getattr(logging, log_level),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('platform.log', encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(self.app_name)
        
    def initialize_spark_session(self) -> SparkSession:
        """Initialize pure Spark session"""
        try:
            spark = SparkSession.builder \
                .appName(self.app_name) \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.sql.adaptive.skewJoin.enabled", "true") \
                .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
                .config("spark.sql.parquet.compression.codec", "snappy") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .getOrCreate()
            
            spark.sparkContext.setLogLevel("ERROR")
            self.logger.info("Spark session initialized successfully")
            return spark
            
        except Exception as e:
            self.logger.error(f"Failed to initialize Spark: {str(e)}")
            raise
    
    def setup_directory_structure(self):
        """Create directory structure"""
        directories = [
            "bronze/raw", "bronze/failed", "bronze/quarantine",
            "silver/validated", "silver/enriched", "silver/scd",
            "gold/marts", "gold/aggregates", "gold/ml_features",
            "metadata/schemas", "metadata/lineage", "metadata/quality",
            "monitoring/metrics", "monitoring/alerts"
        ]
        
        for directory in directories:
            (self.data_lake_path / directory).mkdir(parents=True, exist_ok=True)
    
    def safe_round(self, value: float, decimals: int = 2) -> float:
        """Safe rounding function to avoid PySpark conflicts"""
        if value is None:
            return None
        return float(round(value, decimals))
            
    def generate_realistic_data(self, num_records: int = 10000) -> Dict[str, pd.DataFrame]:
        """Generate realistic messy data with quality issues"""
        fake = Faker()
        self.logger.info(f"Generating {num_records} realistic records...")
        
        # Generate customers
        customers = []
        customer_ids = set()
        
        for i in range(num_records // 10):
            customer_id = str(uuid.uuid4())
            customer_ids.add(customer_id)
            
            customer = {
                'customer_id': customer_id,
                'first_name': fake.first_name() if random.random() > 0.02 else None,
                'last_name': fake.last_name() if random.random() > 0.01 else None,
                'email': fake.email() if random.random() > 0.03 else None,
                'phone': fake.phone_number() if random.random() > 0.05 else None,
                'date_of_birth': fake.date_of_birth(minimum_age=18, maximum_age=80) if random.random() > 0.08 else None,
                'address': fake.address() if random.random() > 0.10 else None,
                'city': fake.city() if random.random() > 0.04 else None,
                'state': fake.state() if random.random() > 0.03 else None,
                'country': fake.country() if random.random() > 0.02 else None,
                'postal_code': fake.postcode() if random.random() > 0.06 else None,
                'registration_date': fake.date_between(start_date='-2y', end_date='today'),
                'is_active': random.choice([True, False, None]) if random.random() > 0.01 else None,
                'customer_tier': random.choice(['Bronze', 'Silver', 'Gold', 'Platinum', None]) if random.random() > 0.05 else None,
                'lifetime_value': self.safe_round(random.uniform(0, 50000)) if random.random() > 0.07 else None,
                'created_at': fake.date_time_between(start_date='-2y', end_date='now'),
                'updated_at': fake.date_time_between(start_date='-1y', end_date='now')
            }
            
            # Schema evolution: 30% have new fields
            if random.random() > 0.7:
                customer['preferred_language'] = fake.language_code()
                customer['marketing_consent'] = random.choice([True, False])
                customer['social_media_handle'] = f"@{fake.user_name()}"
                
            customers.append(customer)
            
        # Introduce duplicates (5%)
        duplicate_count = int(len(customers) * 0.05)
        for _ in range(duplicate_count):
            duplicate_customer = customers[random.randint(0, len(customers)-1)].copy()
            duplicate_customer['customer_id'] = str(uuid.uuid4())
            customers.append(duplicate_customer)
            
        # Generate transactions
        transactions = []
        
        for i in range(num_records):
            transaction_id = str(uuid.uuid4())
            customer_id = random.choice(list(customer_ids))
            
            # Simulate late-arriving data
            base_date = fake.date_time_between(start_date='-1y', end_date='now')
            if random.random() > 0.9:  # 10% arrive late
                arrival_delay = timedelta(hours=random.randint(1, 72))
                processing_date = base_date + arrival_delay
            else:
                processing_date = base_date
                
            amount_value = self.safe_round(random.uniform(1, 5000)) if random.random() > 0.01 else None
            fraud_score_value = self.safe_round(random.uniform(0, 1), 3) if random.random() > 0.10 else None
            
            transaction = {
                'transaction_id': transaction_id,
                'customer_id': customer_id if random.random() > 0.02 else None,
                'merchant_id': f"merchant_{random.randint(1, 100)}",
                'transaction_date': base_date,
                'processing_date': processing_date,
                'amount': amount_value,
                'currency': random.choice(['USD', 'EUR', 'GBP', 'CAD', None]) if random.random() > 0.02 else None,
                'transaction_type': random.choice(['purchase', 'refund', 'adjustment', 'fee', None]) if random.random() > 0.01 else None,
                'channel': random.choice(['online', 'mobile', 'in-store', 'phone', None]) if random.random() > 0.03 else None,
                'payment_method': random.choice(['credit_card', 'debit_card', 'paypal', 'bank_transfer', 'cash', None]) if random.random() > 0.02 else None,
                'is_fraud': random.choice([True, False]) if random.random() > 0.05 else None,
                'fraud_score': fraud_score_value,
                'merchant_category': random.choice(['grocery', 'gas', 'restaurant', 'retail', 'online', None]) if random.random() > 0.08 else None,
                'location_lat': self.safe_round(random.uniform(25, 50), 6) if random.random() > 0.15 else None,
                'location_lon': self.safe_round(random.uniform(-125, -70), 6) if random.random() > 0.15 else None,
                'device_fingerprint': fake.sha256() if random.random() > 0.20 else None,
                'user_agent': fake.user_agent() if random.random() > 0.30 else None,
                'ip_address': fake.ipv4() if random.random() > 0.12 else None,
                'session_id': str(uuid.uuid4()) if random.random() > 0.18 else None,
                'created_at': processing_date,
                'updated_at': processing_date
            }
            
            # Schema evolution: 20% have new fields
            if random.random() > 0.8:
                transaction['risk_level'] = random.choice(['low', 'medium', 'high'])
                transaction['merchant_rating'] = self.safe_round(random.uniform(1, 5), 1)
                transaction['cashback_amount'] = self.safe_round(random.uniform(0, 50))
                
            # Data corruption: 5% have invalid data
            if random.random() > 0.95:
                transaction['amount'] = random.choice([-999, 999999, 0])
                transaction['currency'] = random.choice(['XXX', '123', 'INVALID'])
                
            transactions.append(transaction)
            
        # Generate merchants
        merchants = []
        for i in range(100):
            merchant_id = f"merchant_{i+1}"
            merchant = {
                'merchant_id': merchant_id,
                'merchant_name': fake.company() if random.random() > 0.05 else None,
                'merchant_category': random.choice(['grocery', 'gas', 'restaurant', 'retail', 'online']),
                'address': fake.address() if random.random() > 0.15 else None,
                'city': fake.city() if random.random() > 0.08 else None,
                'state': fake.state() if random.random() > 0.06 else None,
                'country': fake.country() if random.random() > 0.03 else None,
                'postal_code': fake.postcode() if random.random() > 0.12 else None,
                'phone': fake.phone_number() if random.random() > 0.20 else None,
                'email': fake.email() if random.random() > 0.25 else None,
                'website': fake.url() if random.random() > 0.40 else None,
                'registration_date': fake.date_between(start_date='-5y', end_date='-1y'),
                'is_active': random.choice([True, False]) if random.random() > 0.02 else None,
                'risk_rating': random.choice(['low', 'medium', 'high']) if random.random() > 0.10 else None,
                'average_ticket_size': self.safe_round(random.uniform(10, 500)) if random.random() > 0.15 else None,
                'monthly_volume': self.safe_round(random.uniform(1000, 100000)) if random.random() > 0.20 else None,
                'created_at': fake.date_time_between(start_date='-5y', end_date='-1y'),
                'updated_at': fake.date_time_between(start_date='-1y', end_date='now')
            }
            merchants.append(merchant)
            
        # Account changes for SCD Type 2
        account_changes = []
        for customer_id in list(customer_ids)[:100]:
            account_changes.append({
                'customer_id': customer_id,
                'account_status': 'ACTIVE',
                'credit_limit': self.safe_round(random.uniform(1000, 10000)),
                'risk_score': self.safe_round(random.uniform(0, 1), 3),
                'tier': random.choice(['Bronze', 'Silver', 'Gold']),
                'effective_date': fake.date_between(start_date='-2y', end_date='-1y'),
                'end_date': None,
                'is_current': True,
                'created_at': fake.date_time_between(start_date='-2y', end_date='-1y')
            })
            
            # Some customers have changes
            if random.random() > 0.7:
                account_changes[-1]['end_date'] = fake.date_between(start_date='-1y', end_date='-6M')
                account_changes[-1]['is_current'] = False
                
                account_changes.append({
                    'customer_id': customer_id,
                    'account_status': random.choice(['ACTIVE', 'SUSPENDED', 'CLOSED']),
                    'credit_limit': self.safe_round(random.uniform(1000, 15000)),
                    'risk_score': self.safe_round(random.uniform(0, 1), 3),
                    'tier': random.choice(['Silver', 'Gold', 'Platinum']),
                    'effective_date': account_changes[-1]['end_date'],
                    'end_date': None,
                    'is_current': True,
                    'created_at': fake.date_time_between(start_date='-6M', end_date='now')
                })
                
        # Events for streaming
        events = []
        for i in range(num_records // 2):
            event = {
                'event_id': str(uuid.uuid4()),
                'customer_id': random.choice(list(customer_ids)),
                'event_type': random.choice(['login', 'logout', 'page_view', 'click', 'purchase', 'search']),
                'event_timestamp': fake.date_time_between(start_date='-7d', end_date='now'),
                'session_id': str(uuid.uuid4()),
                'page_url': fake.url() if random.random() > 0.20 else None,
                'referrer': fake.url() if random.random() > 0.60 else None,
                'user_agent': fake.user_agent() if random.random() > 0.30 else None,
                'ip_address': fake.ipv4() if random.random() > 0.15 else None,
                'device_type': random.choice(['desktop', 'mobile', 'tablet']) if random.random() > 0.10 else None,
                'browser': random.choice(['chrome', 'firefox', 'safari', 'edge']) if random.random() > 0.20 else None,
                'platform': random.choice(['windows', 'macos', 'linux', 'ios', 'android']) if random.random() > 0.25 else None,
                'created_at': fake.date_time_between(start_date='-7d', end_date='now')
            }
            events.append(event)
            
        # Convert to DataFrames
        customers_df = pd.DataFrame(customers)
        transactions_df = pd.DataFrame(transactions)
        merchants_df = pd.DataFrame(merchants)
        account_changes_df = pd.DataFrame(account_changes)
        events_df = pd.DataFrame(events)
        
        # Quality issues summary
        quality_issues = {
            'customers': {
                'total_records': len(customers_df),
                'missing_first_name': customers_df['first_name'].isna().sum(),
                'missing_email': customers_df['email'].isna().sum(),
                'duplicates': duplicate_count,
                'schema_variations': sum(1 for c in customers if 'preferred_language' in c)
            },
            'transactions': {
                'total_records': len(transactions_df),
                'missing_amount': transactions_df['amount'].isna().sum(),
                'orphaned_transactions': transactions_df['customer_id'].isna().sum(),
                'late_arriving': sum(1 for t in transactions if t.get('processing_date') != t.get('transaction_date')),
                'data_corruption': sum(1 for t in transactions if t.get('amount') in [-999, 999999, 0])
            }
        }
        
        self.logger.info(f"Data quality issues: {quality_issues}")
        
        return {
            'customers': customers_df,
            'transactions': transactions_df,
            'merchants': merchants_df,
            'account_changes': account_changes_df,
            'events': events_df,
            'quality_issues': quality_issues
        }
    
    def create_bronze_layer(self, raw_data: Dict[str, pd.DataFrame]):
        """Bronze Layer: Raw data ingestion with audit trail"""
        self.logger.info("Creating Bronze Layer - Raw Data Ingestion")
        
        bronze_tables = {}
        
        for table_name, df in raw_data.items():
            if table_name == 'quality_issues':
                continue
                
            start_time = time.time()
            
            # Convert to Spark DataFrame
            spark_df = self.spark.createDataFrame(df)
            
            # Add bronze metadata
            bronze_df = spark_df.select(
                "*",
                F.lit(str(uuid.uuid4())).alias("_bronze_id"),
                F.current_timestamp().alias("_ingestion_timestamp"),
                F.lit("batch_ingestion").alias("_ingestion_method"),
                F.lit("v1.0").alias("_schema_version"),
                F.lit(table_name).alias("_source_table"),
                F.lit("file_system").alias("_source_system"),
                F.lit(len(df)).alias("_source_record_count"),
                F.lit(str(uuid.uuid4())).alias("_batch_id"),
                F.sha2(F.concat_ws("|", *[F.col(c) for c in spark_df.columns]), 256).alias("_row_hash")
            )
            
            # Schema evolution tracking
            self.register_schema_evolution(table_name, bronze_df.schema)
            
            # Write to bronze layer
            bronze_path = str(self.data_lake_path / f"bronze/raw/{table_name}")
            
            try:
                bronze_df.write.mode("overwrite").parquet(bronze_path)
                self.logger.info(f"Bronze table created: {table_name} ({len(df)} records)")
                
            except Exception as e:
                self.logger.error(f"Failed to write {table_name}: {str(e)}")
                raise
                
            bronze_tables[table_name] = bronze_df
            
            # Track processing time
            processing_time = (time.time() - start_time) * 1000
            self.metrics['processing_time_ms'] += processing_time
            
        return bronze_tables
    
    def register_schema_evolution(self, table_name: str, schema: StructType):
        """Register schema changes"""
        schema_dict = self.schema_to_dict(schema)
        
        if table_name in self.schema_registry:
            old_schema = self.schema_registry[table_name]
            changes = self.detect_schema_changes(old_schema, schema_dict)
            
            if changes:
                evolution_event = SchemaEvolutionEvent(
                    table_name=table_name,
                    event_type=changes['type'],
                    old_schema=old_schema,
                    new_schema=schema_dict,
                    timestamp=datetime.now(),
                    impact_score=changes['impact_score']
                )
                
                self.schema_evolution_log.append(evolution_event)
                self.metrics['schema_evolution_events'] += 1
                
                self.logger.warning(f"Schema evolution detected in {table_name}: {changes['type']}")
                
        self.schema_registry[table_name] = schema_dict
        
    def schema_to_dict(self, schema: StructType) -> Dict:
        """Convert Spark schema to dictionary"""
        return {field.name: str(field.dataType) for field in schema.fields}
        
    def detect_schema_changes(self, old_schema: Dict, new_schema: Dict) -> Dict:
        """Detect schema changes"""
        old_fields = set(old_schema.keys())
        new_fields = set(new_schema.keys())
        
        added_fields = new_fields - old_fields
        removed_fields = old_fields - new_fields
        
        type_changes = {
            field: (old_schema[field], new_schema[field])
            for field in old_fields & new_fields
            if old_schema[field] != new_schema[field]
        }
        
        if added_fields:
            return {'type': 'ADD_COLUMN', 'details': list(added_fields), 'impact_score': 0.3}
        elif removed_fields:
            return {'type': 'DROP_COLUMN', 'details': list(removed_fields), 'impact_score': 0.8}
        elif type_changes:
            return {'type': 'CHANGE_TYPE', 'details': type_changes, 'impact_score': 0.6}
            
        return {}
    
    def create_silver_layer(self, bronze_tables: Dict[str, DataFrame]):
        """Silver Layer: Data validation and cleaning"""
        self.logger.info("Creating Silver Layer - Data Validation & Cleaning")
        
        silver_tables = {}
        
        for table_name, bronze_df in bronze_tables.items():
            self.logger.info(f"Processing silver table: {table_name}")
            
            # Data quality checks
            quality_results = self.apply_data_quality_checks(bronze_df, table_name)
            
            # Late-arriving data handling
            if table_name == 'transactions':
                silver_df = self.handle_late_arriving_data(bronze_df)
            else:
                silver_df = bronze_df
                
            # Deduplication
            silver_df = self.deduplicate_records(silver_df, table_name)
            
            # Business rules
            silver_df = self.apply_business_rules(silver_df, table_name)
            
            # Add silver metadata
            silver_df = silver_df.select(
                "*",
                F.lit(str(uuid.uuid4())).alias("_silver_id"),
                F.current_timestamp().alias("_silver_timestamp"),
                F.lit(quality_results['overall_score']).alias("_quality_score"),
                F.lit(quality_results['completeness']).alias("_completeness_score"),
                F.lit(quality_results['validity']).alias("_validity_score"),
                F.lit("silver_v1.0").alias("_processing_version")
            )
            
            # Write to silver layer
            silver_path = str(self.data_lake_path / f"silver/validated/{table_name}")
            
            try:
                silver_df.write.mode("overwrite").parquet(silver_path)
            except Exception as e:
                self.logger.error(f"Failed to write silver {table_name}: {str(e)}")
                raise
                
            silver_tables[table_name] = silver_df
            
            self.logger.info(f"Silver table created: {table_name} (Quality: {quality_results['overall_score']:.3f})")
            
        return silver_tables
    
    def apply_data_quality_checks(self, df: DataFrame, table_name: str) -> Dict:
        """Apply data quality checks"""
        quality_rules = {
            'customers': [
                DataQualityCheck(DataQualityRule.NOT_NULL, 'customer_id', 1.0),
                DataQualityCheck(DataQualityRule.UNIQUE, 'customer_id', 1.0),
                DataQualityCheck(DataQualityRule.NOT_NULL, 'first_name', 0.95),
            ],
            'transactions': [
                DataQualityCheck(DataQualityRule.NOT_NULL, 'transaction_id', 1.0),
                DataQualityCheck(DataQualityRule.UNIQUE, 'transaction_id', 1.0),
                DataQualityCheck(DataQualityRule.NOT_NULL, 'amount', 0.95),
            ]
        }
        
        rules = quality_rules.get(table_name, [])
        quality_scores = {}
        
        total_records = df.count()
        
        for rule in rules:
            if rule.rule_type == DataQualityRule.NOT_NULL:
                non_null_count = df.filter(F.col(rule.column).isNotNull()).count()
                score = non_null_count / total_records if total_records > 0 else 0
                quality_scores[f"{rule.column}_completeness"] = score
                
                if score < rule.threshold:
                    self.metrics['data_quality_violations'] += 1
                    self.logger.warning(f"Data quality violation: {rule.column} completeness {score:.3f} < {rule.threshold}")
                    
            elif rule.rule_type == DataQualityRule.UNIQUE:
                unique_count = df.select(rule.column).distinct().count()
                score = unique_count / total_records if total_records > 0 else 0
                quality_scores[f"{rule.column}_uniqueness"] = score
        
        completeness_scores = [v for k, v in quality_scores.items() if 'completeness' in k]
        validity_scores = [v for k, v in quality_scores.items() if 'validity' in k]
        uniqueness_scores = [v for k, v in quality_scores.items() if 'uniqueness' in k]
        
        return {
            'overall_score': np.mean(list(quality_scores.values())) if quality_scores else 0,
            'completeness': np.mean(completeness_scores) if completeness_scores else 1,
            'validity': np.mean(validity_scores) if validity_scores else 1,
            'uniqueness': np.mean(uniqueness_scores) if uniqueness_scores else 1,
            'details': quality_scores
        }
    
    def handle_late_arriving_data(self, df: DataFrame) -> DataFrame:
        """Handle late-arriving data"""
        self.logger.info("Handling late-arriving data...")
        
        late_arriving_df = df.filter(
            F.col("processing_date") > F.col("transaction_date") + F.expr("INTERVAL 1 DAY")
        )
        
        late_count = late_arriving_df.count()
        if late_count > 0:
            self.metrics['late_arriving_records'] += late_count
            self.logger.warning(f"Found {late_count} late-arriving records")
            
            df = df.withColumn(
                "_is_late_arriving",
                F.when(F.col("processing_date") > F.col("transaction_date") + F.expr("INTERVAL 1 DAY"), True)
                .otherwise(False)
            )
            
            df = df.withColumn(
                "_lateness_hours",
                F.when(F.col("_is_late_arriving"), 
                     (F.unix_timestamp(F.col("processing_date")) - F.unix_timestamp(F.col("transaction_date"))) / 3600)
                .otherwise(0)
            )
            
        return df
    
    def deduplicate_records(self, df: DataFrame, table_name: str) -> DataFrame:
        """Deduplicate records"""
        self.logger.info(f"Deduplicating {table_name}...")
        
        if table_name == 'customers':
            window_spec = Window.partitionBy(
                F.col("first_name"), F.col("last_name"), F.col("email")
            ).orderBy(F.col("created_at").desc())
            
            dedup_df = df.withColumn("_row_number", F.row_number().over(window_spec)) \
                        .filter(F.col("_row_number") == 1) \
                        .drop("_row_number")
                        
        elif table_name == 'transactions':
            window_spec = Window.partitionBy("transaction_id").orderBy(F.col("created_at").desc())
            
            dedup_df = df.withColumn("_row_number", F.row_number().over(window_spec)) \
                        .filter(F.col("_row_number") == 1) \
                        .drop("_row_number")
        else:
            dedup_df = df.dropDuplicates()
            
        original_count = df.count()
        dedup_count = dedup_df.count()
        duplicates_removed = original_count - dedup_count
        
        if duplicates_removed > 0:
            self.metrics['duplicate_records'] += duplicates_removed
            self.logger.info(f"Removed {duplicates_removed} duplicate records from {table_name}")
            
        return dedup_df
    
    def apply_business_rules(self, df: DataFrame, table_name: str) -> DataFrame:
        """Apply business rules"""
        if table_name == 'customers':
            df = df.withColumn("first_name", F.initcap(F.col("first_name"))) \
                   .withColumn("last_name", F.initcap(F.col("last_name"))) \
                   .withColumn("email", F.lower(F.col("email"))) \
                   .withColumn("customer_tier", 
                             F.when(F.col("customer_tier").isNull(), "Bronze")
                             .otherwise(F.col("customer_tier")))
                             
        elif table_name == 'transactions':
            df = df.withColumn("is_high_value", F.col("amount") > 1000) \
                   .withColumn("is_weekend", F.dayofweek(F.col("transaction_date")).isin([1, 7])) \
                   .withColumn("transaction_hour", F.hour(F.col("transaction_date"))) \
                   .withColumn("is_business_hours", F.col("transaction_hour").between(9, 17))
                             
        return df
    
    def create_gold_layer(self, silver_tables: Dict[str, DataFrame]):
        """Gold Layer: Business analytics and ML features"""
        self.logger.info("Creating Gold Layer - Business Analytics")
        
        gold_tables = {}
        
        # Customer 360 with SCD Type 2
        if 'customers' in silver_tables and 'account_changes' in silver_tables:
            customer_360 = self.create_customer_360_scd(
                silver_tables['customers'], 
                silver_tables['account_changes']
            )
            gold_tables['customer_360'] = customer_360
            
        # Transaction analytics
        if 'transactions' in silver_tables:
            transaction_analytics = self.create_transaction_analytics(silver_tables['transactions'])
            gold_tables['transaction_analytics'] = transaction_analytics
            
        # ML features
        if 'customers' in silver_tables and 'transactions' in silver_tables:
            ml_features = self.create_ml_feature_store(
                silver_tables['customers'],
                silver_tables['transactions']
            )
            gold_tables['ml_features'] = ml_features
            
        # Real-time metrics
        if 'events' in silver_tables:
            real_time_metrics = self.create_real_time_metrics(silver_tables['events'])
            gold_tables['real_time_metrics'] = real_time_metrics
            
        return gold_tables
    
    def create_customer_360_scd(self, customers_df: DataFrame, account_changes_df: DataFrame) -> DataFrame:
        """Create Customer 360 with SCD Type 2"""
        self.logger.info("Creating Customer 360 with SCD Type 2...")
        
        customer_360 = customers_df.alias("c").join(
            account_changes_df.alias("a"),
            F.col("c.customer_id") == F.col("a.customer_id"),
            "left"
        ).select(
            F.col("c.customer_id"),
            F.col("c.first_name"),
            F.col("c.last_name"),
            F.col("c.email"),
            F.col("c.customer_tier"),
            F.col("c.lifetime_value"),
            F.col("c.registration_date"),
            F.col("a.account_status"),
            F.col("a.credit_limit"),
            F.col("a.risk_score"),
            F.col("a.tier").alias("current_tier"),
            F.col("a.effective_date"),
            F.col("a.end_date"),
            F.col("a.is_current"),
            F.when(F.col("a.is_current") == True, F.current_timestamp()).otherwise(F.col("a.end_date")).alias("valid_to"),
            F.col("a.effective_date").alias("valid_from"),
            F.col("a.is_current").alias("is_active_record"),
            F.sha2(F.concat_ws("|", F.col("a.account_status"), F.col("a.credit_limit"), F.col("a.risk_score")), 256).alias("business_key_hash")
        )
        
        customer_360 = customer_360.withColumn(
            "days_since_last_change",
            F.datediff(F.current_date(), F.col("effective_date"))
        )
        
        gold_path = str(self.data_lake_path / "gold/marts/customer_360")
        
        try:
            customer_360.write.mode("overwrite").parquet(gold_path)
        except Exception as e:
            self.logger.error(f"Failed to write customer_360: {str(e)}")
            raise
            
        self.logger.info(f"Customer 360 created with {customer_360.count()} records")
        return customer_360
    
    def create_transaction_analytics(self, transactions_df: DataFrame) -> DataFrame:
        """Create transaction analytics"""
        self.logger.info("Creating transaction analytics...")
        
        daily_summary = transactions_df.groupBy(
            F.date_trunc("day", F.col("transaction_date")).alias("transaction_date"),
            "channel",
            "payment_method"
        ).agg(
            F.count("*").alias("transaction_count"),
            F.sum("amount").alias("total_amount"),
            F.avg("amount").alias("avg_amount"),
            F.min("amount").alias("min_amount"),
            F.max("amount").alias("max_amount"),
            F.stddev("amount").alias("stddev_amount"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.sum(F.when(F.col("is_fraud") == True, 1).otherwise(0)).alias("fraud_count"),
            F.sum(F.when(F.col("is_fraud") == True, F.col("amount")).otherwise(0)).alias("fraud_amount")
        ).withColumn(
            "fraud_rate",
            F.col("fraud_count") / F.col("transaction_count")
        ).withColumn(
            "avg_fraud_amount",
            F.col("fraud_amount") / F.col("fraud_count")
        )
        
        daily_summary = daily_summary.withColumn(
            "is_weekend", F.dayofweek(F.col("transaction_date")).isin([1, 7])
        ).withColumn(
            "day_of_week", F.dayofweek(F.col("transaction_date"))
        ).withColumn(
            "month", F.month(F.col("transaction_date"))
        ).withColumn(
            "quarter", F.quarter(F.col("transaction_date"))
        )
        
        gold_path = str(self.data_lake_path / "gold/aggregates/transaction_analytics")
        
        try:
            daily_summary.write.mode("overwrite").partitionBy("transaction_date").parquet(gold_path)
        except Exception as e:
            self.logger.error(f"Failed to write transaction_analytics: {str(e)}")
            raise
            
        self.logger.info(f"Transaction analytics created with {daily_summary.count()} records")
        return daily_summary
    
    def create_ml_feature_store(self, customers_df: DataFrame, transactions_df: DataFrame) -> DataFrame:
        """Create ML feature store"""
        self.logger.info("Creating ML feature store...")
        
        customer_features = customers_df.select(
            "customer_id", "customer_tier", "lifetime_value",
            F.datediff(F.current_date(), F.col("registration_date")).alias("days_since_registration"),
            F.when(F.col("is_active") == True, 1).otherwise(0).alias("is_active_customer")
        )
        
        transaction_features = transactions_df.groupBy("customer_id").agg(
            F.count("*").alias("total_transactions"),
            F.sum("amount").alias("total_amount"),
            F.avg("amount").alias("avg_amount"),
            F.stddev("amount").alias("stddev_amount"),
            F.countDistinct("merchant_id").alias("unique_merchants"),
            F.countDistinct("channel").alias("unique_channels"),
            F.sum(F.when(F.col("is_fraud") == True, 1).otherwise(0)).alias("fraud_count"),
            F.sum(F.when(F.col("is_high_value") == True, 1).otherwise(0)).alias("high_value_count"),
            F.sum(F.when(F.col("is_weekend") == True, 1).otherwise(0)).alias("weekend_transactions"),
            F.sum(F.when(F.col("is_business_hours") == False, 1).otherwise(0)).alias("off_hours_transactions")
        )
        
        transaction_features = transaction_features.withColumn(
            "fraud_rate", F.col("fraud_count") / F.col("total_transactions")
        ).withColumn(
            "high_value_rate", F.col("high_value_count") / F.col("total_transactions")
        ).withColumn(
            "weekend_rate", F.col("weekend_transactions") / F.col("total_transactions")
        ).withColumn(
            "off_hours_rate", F.col("off_hours_transactions") / F.col("total_transactions")
        )
        
        recent_cutoff = F.current_date() - F.expr("INTERVAL 30 DAYS")
        recent_features = transactions_df.filter(F.col("transaction_date") >= recent_cutoff) \
            .groupBy("customer_id").agg(
                F.count("*").alias("recent_transactions"),
                F.sum("amount").alias("recent_amount"),
                F.avg("amount").alias("recent_avg_amount"),
                F.countDistinct("merchant_id").alias("recent_unique_merchants")
            )
        
        ml_features = customer_features.join(transaction_features, "customer_id", "left") \
                                     .join(recent_features, "customer_id", "left") \
                                     .fillna(0)
        
        ml_features = ml_features.withColumn(
            "amount_per_transaction", F.col("total_amount") / F.col("total_transactions")
        ).withColumn(
            "recent_activity_ratio", F.col("recent_transactions") / F.col("total_transactions")
        ).withColumn(
            "merchant_diversity", F.col("unique_merchants") / F.col("total_transactions")
        ).withColumn(
            "feature_timestamp", F.current_timestamp()
        )
        
        gold_path = str(self.data_lake_path / "gold/ml_features/fraud_detection")
        
        try:
            ml_features.write.mode("overwrite").parquet(gold_path)
        except Exception as e:
            self.logger.error(f"Failed to write ml_features: {str(e)}")
            raise
            
        self.logger.info(f"ML feature store created with {ml_features.count()} records")
        return ml_features
    
    def create_real_time_metrics(self, events_df: DataFrame) -> DataFrame:
        """Create real-time metrics"""
        self.logger.info("Creating real-time metrics...")
        
        hourly_metrics = events_df.groupBy(
            F.date_trunc("hour", F.col("event_timestamp")).alias("event_hour"),
            "event_type", "device_type"
        ).agg(
            F.count("*").alias("event_count"),
            F.countDistinct("customer_id").alias("unique_users"),
            F.countDistinct("session_id").alias("unique_sessions"),
            F.avg(F.unix_timestamp(F.col("event_timestamp"))).alias("avg_timestamp")
        )
        
        hourly_metrics = hourly_metrics.withColumn(
            "hour_of_day", F.hour(F.col("event_hour"))
        ).withColumn(
            "day_of_week", F.dayofweek(F.col("event_hour"))
        ).withColumn(
            "is_business_hour", F.col("hour_of_day").between(9, 17)
        )
        
        gold_path = str(self.data_lake_path / "gold/aggregates/real_time_metrics")
        
        try:
            hourly_metrics.write.mode("overwrite").partitionBy("event_hour").parquet(gold_path)
        except Exception as e:
            self.logger.error(f"Failed to write real_time_metrics: {str(e)}")
            raise
            
        self.logger.info(f"Real-time metrics created with {hourly_metrics.count()} records")
        return hourly_metrics
    
    def run_data_quality_monitoring(self):
        """Run data quality monitoring"""
        self.logger.info("Running data quality monitoring...")
        
        silver_path = self.data_lake_path / "silver/validated"
        
        quality_report = {
            'timestamp': datetime.now().isoformat(),
            'tables': {},
            'overall_health': 'healthy',
            'alerts': []
        }
        
        for table_path in silver_path.glob("*"):
            if table_path.is_dir():
                table_name = table_path.name
                
                try:
                    df = self.spark.read.parquet(str(table_path))
                    total_records = df.count()
                    null_counts = {}
                    
                    for column in df.columns:
                        if not column.startswith('_'):
                            null_count = df.filter(F.col(column).isNull()).count()
                            null_counts[column] = null_count
                    
                    completeness = 1 - (sum(null_counts.values()) / (total_records * len(null_counts)))
                    quality_score = df.agg(F.avg(F.col('_quality_score'))).collect()[0][0] if '_quality_score' in df.columns else 0
                    
                    table_quality = {
                        'record_count': total_records,
                        'completeness': completeness,
                        'quality_score': quality_score,
                        'null_counts': null_counts,
                        'status': 'healthy' if completeness >= 0.95 else 'warning'
                    }
                    
                    quality_report['tables'][table_name] = table_quality
                    
                    if completeness < self.data_quality_thresholds['completeness']:
                        quality_report['alerts'].append({
                            'table': table_name,
                            'type': 'completeness',
                            'severity': 'high',
                            'value': completeness,
                            'threshold': self.data_quality_thresholds['completeness']
                        })
                        
                except Exception as e:
                    quality_report['tables'][table_name] = {
                        'status': 'error',
                        'error': str(e)
                    }
        
        if quality_report['alerts']:
            quality_report['overall_health'] = 'warning' if len(quality_report['alerts']) < 3 else 'critical'
            
        quality_path = self.data_lake_path / "metadata/quality/quality_report.json"
        with open(quality_path, 'w') as f:
            json.dump(quality_report, f, indent=2)
            
        self.logger.info(f"Data quality report generated: {quality_report['overall_health']}")
        return quality_report
    
    def generate_lineage_report(self):
        """Generate data lineage report"""
        self.logger.info("Generating data lineage report...")
        
        lineage_report = {
            'timestamp': datetime.now().isoformat(),
            'pipeline_version': '1.0',
            'storage_format': 'parquet',
            'layers': {
                'bronze': {
                    'purpose': 'Raw data ingestion with audit trail',
                    'tables': list(self.schema_registry.keys()),
                    'transformations': ['add_metadata', 'schema_validation']
                },
                'silver': {
                    'purpose': 'Data validation, cleaning, standardization',
                    'tables': ['customers', 'transactions', 'merchants', 'account_changes', 'events'],
                    'transformations': ['data_quality_checks', 'deduplication', 'business_rules', 'late_data_handling']
                },
                'gold': {
                    'purpose': 'Business analytics and ML features',
                    'tables': ['customer_360', 'transaction_analytics', 'ml_features', 'real_time_metrics'],
                    'transformations': ['scd_type_2', 'aggregations', 'feature_engineering']
                }
            },
            'schema_evolution': [asdict(event) for event in self.schema_evolution_log],
            'metrics': self.metrics
        }
        
        lineage_path = self.data_lake_path / "metadata/lineage/lineage_report.json"
        with open(lineage_path, 'w') as f:
            json.dump(lineage_report, f, indent=2, default=str)
            
        return lineage_report
    
    def run_full_pipeline(self):
        """Execute complete data engineering pipeline"""
        self.logger.info("EXECUTING PRODUCTION DATA ENGINEERING PIPELINE")
        self.logger.info("=" * 60)
        
        start_time = time.time()
        
        try:
            # Step 1: Generate data
            self.logger.info("STEP 1: Generating realistic messy data...")
            raw_data = self.generate_realistic_data(10000)
            
            # Step 2: Bronze Layer
            self.logger.info("STEP 2: Creating Bronze Layer...")
            bronze_tables = self.create_bronze_layer(raw_data)
            
            # Step 3: Silver Layer
            self.logger.info("STEP 3: Creating Silver Layer...")
            silver_tables = self.create_silver_layer(bronze_tables)
            
            # Step 4: Gold Layer
            self.logger.info("STEP 4: Creating Gold Layer...")
            gold_tables = self.create_gold_layer(silver_tables)
            
            # Step 5: Quality Monitoring
            self.logger.info("STEP 5: Running Data Quality Monitoring...")
            quality_report = self.run_data_quality_monitoring()
            
            # Step 6: Lineage Report
            self.logger.info("STEP 6: Generating Data Lineage Report...")
            lineage_report = self.generate_lineage_report()
            
            # Calculate metrics
            execution_time = time.time() - start_time
            self.metrics['records_processed'] = sum(df.count() for df in bronze_tables.values())
            self.metrics['processing_time_ms'] = execution_time * 1000
            
            # Print summary
            self.print_pipeline_summary(quality_report, lineage_report, execution_time)
            
            return {
                'bronze_tables': bronze_tables,
                'silver_tables': silver_tables,
                'gold_tables': gold_tables,
                'quality_report': quality_report,
                'lineage_report': lineage_report,
                'metrics': self.metrics
            }
            
        except Exception as e:
            self.logger.error(f"Pipeline execution failed: {str(e)}")
            raise
    
    def print_pipeline_summary(self, quality_report: Dict, lineage_report: Dict, execution_time: float):
        """Print pipeline summary"""
        print("\n" + "="*60)
        print("PRODUCTION DATA ENGINEERING PIPELINE COMPLETE!")
        print("="*60)
        
        print(f"\nEXECUTION SUMMARY:")
        print(f"   Total execution time: {execution_time:.2f} seconds")
        print(f"   Records processed: {self.metrics['records_processed']:,}")
        print(f"   Data quality violations: {self.metrics['data_quality_violations']}")
        print(f"   Schema evolution events: {self.metrics['schema_evolution_events']}")
        print(f"   Late arriving records: {self.metrics['late_arriving_records']}")
        print(f"   Duplicate records removed: {self.metrics['duplicate_records']}")
        
        print(f"\nARCHITECTURE SUMMARY:")
        print(f"   Storage Format: Parquet (Production Standard)")
        print(f"   Bronze Layer: {len(lineage_report['layers']['bronze']['tables'])} tables")
        print(f"   Silver Layer: {len(lineage_report['layers']['silver']['tables'])} tables")
        print(f"   Gold Layer: {len(lineage_report['layers']['gold']['tables'])} tables")
        
        print(f"\nDATA QUALITY SUMMARY:")
        print(f"   Overall health: {quality_report['overall_health'].upper()}")
        print(f"   Active alerts: {len(quality_report['alerts'])}")
        
        if quality_report['alerts']:
            print(f"\nDATA QUALITY ALERTS:")
            for alert in quality_report['alerts']:
                print(f"   * {alert['table']}: {alert['type']} = {alert['value']:.3f} (threshold: {alert['threshold']})")
        
        print(f"\nSCHEMA EVOLUTION:")
        if self.schema_evolution_log:
            for event in self.schema_evolution_log:
                print(f"   * {event.table_name}: {event.event_type} (impact: {event.impact_score:.1f})")
        else:
            print(f"   * No schema changes detected")
        
        print(f"\nREAL-WORLD CHALLENGES ADDRESSED:")
        print(f"   * Late-arriving data handling")
        print(f"   * Schema evolution management")
        print(f"   * Data quality monitoring")
        print(f"   * Duplicate detection & removal")
        print(f"   * SCD Type 2 implementation")
        print(f"   * ML feature engineering")
        print(f"   * Production monitoring")
        print(f"   * Data lineage tracking")
        
        print(f"\nNEXT STEPS:")
        print(f"   1. Set up streaming ingestion")
        print(f"   2. Implement real-time fraud detection")
        print(f"   3. Deploy to cloud platform")
        print(f"   4. Add orchestration with Airflow")
        print(f"   5. Implement data governance")
        
        print("\n" + "="*60)


def main():
    """Main execution function"""
    print("PRODUCTION DATA ENGINEERING PLATFORM")
    print("Industry-Standard PySpark + Parquet")
    print("=" * 60)
    
    # Initialize platform
    platform = ProductionDataPlatform()
    
    # Run complete pipeline
    results = platform.run_full_pipeline()
    
    print("\nPRODUCTION DATA ENGINEERING COMPLETE!")
    print("   Your production-grade data platform is ready!")
    print("   Check the data-lake/ directory for all tables and metadata.")
    print("   Using Parquet format - industry standard!")


if __name__ == "__main__":
    main()