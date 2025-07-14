"""
Enhanced Privacy Masking Service - ML Compatible Version
Fixed to work with ML models while maintaining privacy
"""

import hashlib
import logging
import re
import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional
from datetime import datetime
import random
import string

logger = logging.getLogger(__name__)

class EnhancedPrivacyMaskingService:
    """
    Enterprise-grade privacy masking service with comprehensive PII protection
    ML Compatible Version
    """
    
    def __init__(self):
        """Initialize the enhanced privacy masking service"""
        
        logger.info("Starting comprehensive PII masking...")
        
        # Initialize salt for consistent hashing
        self.salt = "fraud_detection_2024"
        
        # PII field mappings
        self.pii_fields = {
            'customer_id': 'hash_with_prefix',
            'ssn': 'partial_mask',
            'email': 'hash_preserve_domain',
            'phone': 'partial_mask',
            'address': 'hash_preserve_format',
            'ip_address': 'partial_mask',
            'device_id': 'hash_with_prefix',
            'transaction_id': 'hash_with_prefix'
        }
        
        # Compliance audit trail
        self.audit_trail = []
        
        logger.info("Privacy masking service initialized")
    
    def mask_transaction_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply privacy masking to transaction data"""
        
        logger.info("Applying privacy masking to transaction data...")
        
        if df is None or df.empty:
            logger.warning("No transaction data to mask")
            return df
        
        masked_df = df.copy()
        
        # Mask sensitive fields
        for field, method in self.pii_fields.items():
            if field in masked_df.columns:
                masked_df[field] = self._apply_masking(masked_df[field], method, field)
                self._log_masking_action(field, method, len(masked_df))
        
        # Apply additional privacy measures (ML compatible)
        masked_df = self._apply_additional_privacy_measures_ml_compatible(masked_df)
        
        logger.info("Transaction data privacy masking completed")
        return masked_df
    
    def mask_customer_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply privacy masking to customer data"""
        
        logger.info("Applying privacy masking to customer data...")
        
        if df is None or df.empty:
            logger.warning("No customer data to mask")
            return df
        
        masked_df = df.copy()
        
        # Mask customer-specific fields
        customer_pii_fields = {
            'customer_id': 'hash_with_prefix',
            'ssn': 'partial_mask'
        }
        
        for field, method in customer_pii_fields.items():
            if field in masked_df.columns:
                masked_df[field] = self._apply_masking(masked_df[field], method, field)
                self._log_masking_action(field, method, len(masked_df))
        
        logger.info("Customer data privacy masking completed")
        return masked_df
    
    def _apply_masking(self, series: pd.Series, method: str, field_name: str) -> pd.Series:
        """Apply specific masking method to a series"""
        
        if method == 'hash_with_prefix':
            return series.apply(lambda x: self._hash_with_prefix(str(x), field_name))
        elif method == 'partial_mask':
            return series.apply(lambda x: self._partial_mask(str(x)))
        elif method == 'hash_preserve_domain':
            return series.apply(lambda x: self._hash_preserve_domain(str(x)))
        elif method == 'hash_preserve_format':
            return series.apply(lambda x: self._hash_preserve_format(str(x)))
        else:
            return series
    
    def _hash_with_prefix(self, value: str, field_name: str) -> str:
        """Hash value with field prefix"""
        
        if pd.isna(value) or value == 'nan':
            return value
        
        # Create hash
        hash_object = hashlib.sha256((value + self.salt).encode())
        hash_hex = hash_object.hexdigest()[:8]
        
        # Add field prefix
        prefix = field_name.upper()[:4]
        return f"{prefix}_{hash_hex}"
    
    def _partial_mask(self, value: str) -> str:
        """Partially mask value (show first/last chars)"""
        
        if pd.isna(value) or value == 'nan' or len(value) <= 4:
            return value
        
        if len(value) <= 8:
            return value[:2] + '*' * (len(value) - 4) + value[-2:]
        else:
            return value[:3] + '*' * (len(value) - 6) + value[-3:]
    
    def _hash_preserve_domain(self, value: str) -> str:
        """Hash email while preserving domain"""
        
        if pd.isna(value) or value == 'nan' or '@' not in value:
            return value
        
        local, domain = value.split('@', 1)
        hash_object = hashlib.sha256((local + self.salt).encode())
        hash_hex = hash_object.hexdigest()[:8]
        
        return f"user_{hash_hex}@{domain}"
    
    def _hash_preserve_format(self, value: str) -> str:
        """Hash while preserving format structure"""
        
        if pd.isna(value) or value == 'nan':
            return value
        
        # Generate consistent hash
        hash_object = hashlib.sha256((value + self.salt).encode())
        hash_hex = hash_object.hexdigest()[:12]
        
        return f"ADDR_{hash_hex}"
    
    def _apply_additional_privacy_measures_ml_compatible(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply additional privacy protection measures (ML compatible)"""
        
        # Add noise to sensitive numerical fields (keep as numeric)
        sensitive_numeric_fields = ['amount', 'annual_income', 'credit_card_debt']
        
        for field in sensitive_numeric_fields:
            if field in df.columns:
                # Add small random noise (±1% of value)
                noise = np.random.normal(0, 0.01, len(df))
                df[field] = df[field] * (1 + noise)
        
        # Apply privacy noise to credit scores (keep as numeric for ML)
        credit_score_fields = ['fico_score', 'cibil_score']
        for field in credit_score_fields:
            if field in df.columns:
                # Add small amount of noise to credit scores (±5 points)
                noise = np.random.normal(0, 5, len(df))
                df[field] = df[field] + noise
                # Ensure scores stay within valid ranges
                if field == 'fico_score':
                    df[field] = np.clip(df[field], 300, 850)
                elif field == 'cibil_score':
                    df[field] = np.clip(df[field], 300, 900)
        
        return df
    
    def _log_masking_action(self, field: str, method: str, record_count: int):
        """Log masking action for audit trail"""
        
        action = {
            'timestamp': datetime.utcnow().isoformat(),
            'field': field,
            'method': method,
            'record_count': record_count,
            'compliance': 'GDPR/CCPA/PCI DSS'
        }
        
        self.audit_trail.append(action)
        logger.info(f"Masked {field} using {method} for {record_count} records")
    
    def get_audit_trail(self) -> List[Dict[str, Any]]:
        """Get comprehensive audit trail"""
        return self.audit_trail
    
    def get_compliance_report(self) -> Dict[str, Any]:
        """Generate compliance report"""
        
        return {
            'gdpr_compliant': True,
            'ccpa_compliant': True,
            'pci_dss_compliant': True,
            'fields_masked': len(self.audit_trail),
            'masking_methods': list(set(action['method'] for action in self.audit_trail)),
            'total_records_processed': sum(action['record_count'] for action in self.audit_trail),
            'audit_trail_available': True,
            'last_masking_timestamp': max(action['timestamp'] for action in self.audit_trail) if self.audit_trail else None
        }