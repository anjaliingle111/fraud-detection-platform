"""
Enterprise Data Quality Engine
Handles messy data, validation, cleaning, and quality scoring
"""

import pandas as pd
import numpy as np
import logging
from typing import Dict, List, Tuple, Optional, Any
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum
import re
import json
from sklearn.impute import SimpleImputer, KNNImputer
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import IsolationForest
from sklearn.cluster import DBSCAN
import warnings

warnings.filterwarnings('ignore')

logger = logging.getLogger(__name__)

class DataQualityIssue(Enum):
    """Types of data quality issues"""
    MISSING_VALUE = "missing_value"
    OUTLIER = "outlier"
    INVALID_FORMAT = "invalid_format"
    INCONSISTENT_DATA = "inconsistent_data"
    DUPLICATE_RECORD = "duplicate_record"
    CONSTRAINT_VIOLATION = "constraint_violation"
    SUSPICIOUS_PATTERN = "suspicious_pattern"

@dataclass
class DataQualityReport:
    """Data quality assessment report"""
    overall_score: float
    total_records: int
    clean_records: int
    issues_found: Dict[DataQualityIssue, int]
    field_quality_scores: Dict[str, float]
    recommendations: List[str]
    auto_repairs: List[str]

class EnterpriseDataQualityEngine:
    """
    Comprehensive data quality engine for enterprise fraud detection
    """
    
    def __init__(self):
        self.quality_rules = self._initialize_quality_rules()
        self.repair_strategies = self._initialize_repair_strategies()
        self.validation_cache = {}
        self.quality_history = []
        
    def _initialize_quality_rules(self) -> Dict[str, Dict]:
        """Initialize data quality validation rules"""
        return {
            'ssn': {
                'type': 'string',
                'pattern': r'^\d{9}$',
                'required': True,
                'max_length': 9,
                'min_length': 9,
                'invalid_values': ['000000000', '111111111', '222222222', '333333333', 
                                 '444444444', '555555555', '666666666', '777777777', 
                                 '888888888', '999999999']
            },
            'fico_score': {
                'type': 'numeric',
                'min_value': 300,
                'max_value': 850,
                'required': False,
                'outlier_method': 'iqr',
                'outlier_threshold': 3.0
            },
            'cibil_score': {
                'type': 'numeric',
                'min_value': 300,
                'max_value': 900,
                'required': False,
                'outlier_method': 'iqr',
                'outlier_threshold': 3.0
            },
            'annual_income': {
                'type': 'numeric',
                'min_value': 0,
                'max_value': 10000000,
                'required': True,
                'outlier_method': 'isolation_forest',
                'outlier_threshold': 0.1
            },
            'credit_card_debt': {
                'type': 'numeric',
                'min_value': 0,
                'max_value': 1000000,
                'required': True,
                'outlier_method': 'isolation_forest'
            },
            'amount': {
                'type': 'numeric',
                'min_value': 0,
                'max_value': 100000,
                'required': True,
                'outlier_method': 'isolation_forest'
            },
            'chargeback_ratio': {
                'type': 'numeric',
                'min_value': 0,
                'max_value': 1,
                'required': True
            },
            'return_ratio': {
                'type': 'numeric',
                'min_value': 0,
                'max_value': 1,
                'required': True
            },
            'account_age_days': {
                'type': 'numeric',
                'min_value': 0,
                'max_value': 36500,  # 100 years
                'required': True
            },
            'email': {
                'type': 'string',
                'pattern': r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$',
                'required': False,
                'max_length': 254
            },
            'phone': {
                'type': 'string',
                'pattern': r'^\(\d{3}\) \d{3}-\d{4}$',
                'required': False
            },
            'ip_address': {
                'type': 'string',
                'pattern': r'^(?:[0-9]{1,3}\.){3}[0-9]{1,3}$',
                'required': False
            },
            'channel': {
                'type': 'categorical',
                'valid_values': ['online', 'mobile', 'atm', 'pos', 'phone'],
                'required': True
            },
            'employment_status': {
                'type': 'categorical',
                'valid_values': ['EMPLOYED', 'UNEMPLOYED', 'SELF_EMPLOYED', 'STUDENT', 'RETIRED'],
                'required': False
            }
        }
    
    def _initialize_repair_strategies(self) -> Dict[str, str]:
        """Initialize automated data repair strategies"""
        return {
            'missing_value': 'impute',
            'outlier': 'cap_or_impute',
            'invalid_format': 'standardize',
            'inconsistent_data': 'normalize',
            'duplicate_record': 'deduplicate',
            'constraint_violation': 'correct_or_flag',
            'suspicious_pattern': 'flag_for_review'
        }
    
    def assess_data_quality(self, df: pd.DataFrame) -> DataQualityReport:
        """Comprehensive data quality assessment"""
        logger.info("🔍 Starting comprehensive data quality assessment...")
        
        issues_found = {issue: 0 for issue in DataQualityIssue}
        field_quality_scores = {}
        recommendations = []
        auto_repairs = []
        
        total_records = len(df)
        clean_records = 0
        
        # Assess each field
        for column in df.columns:
            if column in self.quality_rules:
                field_issues, field_score = self._assess_field_quality(df[column], column)
                field_quality_scores[column] = field_score
                
                # Update issue counts
                for issue_type, count in field_issues.items():
                    issues_found[issue_type] += count
            else:
                # Basic assessment for unspecified fields
                field_score = self._basic_field_assessment(df[column])
                field_quality_scores[column] = field_score
        
        # Count clean records
        clean_records = self._count_clean_records(df, issues_found)
        
        # Calculate overall score
        overall_score = self._calculate_overall_score(field_quality_scores, issues_found, total_records)
        
        # Generate recommendations
        recommendations = self._generate_recommendations(issues_found, field_quality_scores)
        
        # Generate auto-repair suggestions
        auto_repairs = self._generate_auto_repairs(issues_found)
        
        report = DataQualityReport(
            overall_score=overall_score,
            total_records=total_records,
            clean_records=clean_records,
            issues_found=issues_found,
            field_quality_scores=field_quality_scores,
            recommendations=recommendations,
            auto_repairs=auto_repairs
        )
        
        logger.info(f"✅ Data quality assessment complete. Overall score: {overall_score:.3f}")
        return report
    
    def _assess_field_quality(self, series: pd.Series, field_name: str) -> Tuple[Dict[DataQualityIssue, int], float]:
        """Assess quality of individual field"""
        rules = self.quality_rules[field_name]
        issues = {issue: 0 for issue in DataQualityIssue}
        
        total_values = len(series)
        valid_values = 0
        
        for value in series:
            is_valid = True
            
            # Check for missing values
            if pd.isna(value) or value is None:
                if rules.get('required', False):
                    issues[DataQualityIssue.MISSING_VALUE] += 1
                    is_valid = False
                continue
            
            # Type-specific validation
            if rules['type'] == 'numeric':
                is_valid = self._validate_numeric_field(value, rules, issues)
            elif rules['type'] == 'string':
                is_valid = self._validate_string_field(value, rules, issues)
            elif rules['type'] == 'categorical':
                is_valid = self._validate_categorical_field(value, rules, issues)
            
            if is_valid:
                valid_values += 1
        
        # Check for outliers
        if rules['type'] == 'numeric' and 'outlier_method' in rules:
            outlier_count = self._detect_outliers(series, rules)
            issues[DataQualityIssue.OUTLIER] += outlier_count
        
        # Check for suspicious patterns
        suspicious_count = self._detect_suspicious_patterns(series, field_name)
        issues[DataQualityIssue.SUSPICIOUS_PATTERN] += suspicious_count
        
        # Calculate field quality score
        field_score = valid_values / total_values if total_values > 0 else 0
        
        return issues, field_score
    
    def _validate_numeric_field(self, value: Any, rules: Dict, issues: Dict) -> bool:
        """Validate numeric field"""
        try:
            numeric_value = float(value)
            
            # Check range constraints
            if 'min_value' in rules and numeric_value < rules['min_value']:
                issues[DataQualityIssue.CONSTRAINT_VIOLATION] += 1
                return False
            
            if 'max_value' in rules and numeric_value > rules['max_value']:
                issues[DataQualityIssue.CONSTRAINT_VIOLATION] += 1
                return False
            
            return True
            
        except (ValueError, TypeError):
            issues[DataQualityIssue.INVALID_FORMAT] += 1
            return False
    
    def _validate_string_field(self, value: Any, rules: Dict, issues: Dict) -> bool:
        """Validate string field"""
        try:
            str_value = str(value)
            
            # Check length constraints
            if 'max_length' in rules and len(str_value) > rules['max_length']:
                issues[DataQualityIssue.CONSTRAINT_VIOLATION] += 1
                return False
            
            if 'min_length' in rules and len(str_value) < rules['min_length']:
                issues[DataQualityIssue.CONSTRAINT_VIOLATION] += 1
                return False
            
            # Check pattern
            if 'pattern' in rules and not re.match(rules['pattern'], str_value):
                issues[DataQualityIssue.INVALID_FORMAT] += 1
                return False
            
            # Check invalid values
            if 'invalid_values' in rules and str_value in rules['invalid_values']:
                issues[DataQualityIssue.CONSTRAINT_VIOLATION] += 1
                return False
            
            return True
            
        except Exception:
            issues[DataQualityIssue.INVALID_FORMAT] += 1
            return False
    
    def _validate_categorical_field(self, value: Any, rules: Dict, issues: Dict) -> bool:
        """Validate categorical field"""
        try:
            str_value = str(value)
            
            if 'valid_values' in rules and str_value not in rules['valid_values']:
                issues[DataQualityIssue.CONSTRAINT_VIOLATION] += 1
                return False
            
            return True
            
        except Exception:
            issues[DataQualityIssue.INVALID_FORMAT] += 1
            return False
    
    def _detect_outliers(self, series: pd.Series, rules: Dict) -> int:
        """Detect outliers in numeric series"""
        if series.empty or series.isna().all():
            return 0
        
        clean_series = series.dropna()
        if len(clean_series) < 10:  # Need minimum samples for outlier detection
            return 0
        
        method = rules.get('outlier_method', 'iqr')
        outlier_count = 0
        
        if method == 'iqr':
            Q1 = clean_series.quantile(0.25)
            Q3 = clean_series.quantile(0.75)
            IQR = Q3 - Q1
            threshold = rules.get('outlier_threshold', 1.5)
            
            lower_bound = Q1 - threshold * IQR
            upper_bound = Q3 + threshold * IQR
            
            outliers = clean_series[(clean_series < lower_bound) | (clean_series > upper_bound)]
            outlier_count = len(outliers)
        
        elif method == 'isolation_forest':
            try:
                contamination = rules.get('outlier_threshold', 0.1)
                isolation_forest = IsolationForest(contamination=contamination, random_state=42)
                outlier_predictions = isolation_forest.fit_predict(clean_series.values.reshape(-1, 1))
                outlier_count = np.sum(outlier_predictions == -1)
            except:
                outlier_count = 0
        
        return outlier_count
    
    def _detect_suspicious_patterns(self, series: pd.Series, field_name: str) -> int:
        """Detect suspicious patterns in data"""
        if series.empty:
            return 0
        
        suspicious_count = 0
        
        # Check for excessive duplicates
        if field_name in ['ssn', 'email', 'phone']:
            value_counts = series.value_counts()
            # Flag values that appear more than 5 times
            suspicious_duplicates = value_counts[value_counts > 5]
            suspicious_count += len(suspicious_duplicates)
        
        # Check for sequential patterns
        if field_name == 'ssn':
            for value in series.dropna():
                if isinstance(value, str) and len(value) == 9:
                    # Check for sequential digits
                    if value in ['123456789', '987654321']:
                        suspicious_count += 1
        
        # Check for test patterns
        if field_name == 'email':
            test_patterns = ['test@', 'fake@', 'example@', 'dummy@']
            for value in series.dropna():
                if any(pattern in str(value).lower() for pattern in test_patterns):
                    suspicious_count += 1
        
        return suspicious_count
    
    def _basic_field_assessment(self, series: pd.Series) -> float:
        """Basic quality assessment for unspecified fields"""
        if series.empty:
            return 0.0
        
        # Calculate completeness
        completeness = 1 - (series.isna().sum() / len(series))
        
        # Check for variance (not all same value)
        if series.nunique() <= 1:
            variance_score = 0.5
        else:
            variance_score = 1.0
        
        # Combine scores
        return (completeness + variance_score) / 2
    
    def _count_clean_records(self, df: pd.DataFrame, issues: Dict) -> int:
        """Count records with no data quality issues"""
        total_issues = sum(issues.values())
        total_records = len(df)
        
        # Estimate clean records (simplified)
        estimated_clean = max(0, total_records - total_issues)
        return estimated_clean
    
    def _calculate_overall_score(self, field_scores: Dict, issues: Dict, total_records: int) -> float:
        """Calculate overall data quality score"""
        # Average field scores
        avg_field_score = np.mean(list(field_scores.values())) if field_scores else 0
        
        # Issue penalty
        total_issues = sum(issues.values())
        issue_penalty = min(0.5, total_issues / total_records) if total_records > 0 else 0
        
        # Combine scores
        overall_score = max(0, avg_field_score - issue_penalty)
        
        return overall_score
    
    def _generate_recommendations(self, issues: Dict, field_scores: Dict) -> List[str]:
        """Generate data quality improvement recommendations"""
        recommendations = []
        
        # High-priority issues
        if issues[DataQualityIssue.MISSING_VALUE] > 0:
            recommendations.append("Implement data validation at source to reduce missing values")
        
        if issues[DataQualityIssue.CONSTRAINT_VIOLATION] > 0:
            recommendations.append("Add input validation to prevent constraint violations")
        
        if issues[DataQualityIssue.INVALID_FORMAT] > 0:
            recommendations.append("Standardize data formats and implement format validation")
        
        if issues[DataQualityIssue.OUTLIER] > 0:
            recommendations.append("Review outlier detection rules and consider business context")
        
        # Field-specific recommendations
        low_quality_fields = [field for field, score in field_scores.items() if score < 0.8]
        if low_quality_fields:
            recommendations.append(f"Focus on improving data quality for: {', '.join(low_quality_fields)}")
        
        return recommendations
    
    def _generate_auto_repairs(self, issues: Dict) -> List[str]:
        """Generate automated repair suggestions"""
        repairs = []
        
        if issues[DataQualityIssue.MISSING_VALUE] > 0:
            repairs.append("Apply KNN imputation for missing values")
        
        if issues[DataQualityIssue.OUTLIER] > 0:
            repairs.append("Cap outliers at 95th percentile")
        
        if issues[DataQualityIssue.INVALID_FORMAT] > 0:
            repairs.append("Apply format standardization rules")
        
        if issues[DataQualityIssue.DUPLICATE_RECORD] > 0:
            repairs.append("Remove duplicate records based on key fields")
        
        return repairs
    
    def clean_data(self, df: pd.DataFrame, apply_auto_repairs: bool = True) -> pd.DataFrame:
        """Clean data using automated repair strategies"""
        logger.info("🧹 Starting automated data cleaning...")
        
        cleaned_df = df.copy()
        
        if apply_auto_repairs:
            # Apply missing value imputation
            cleaned_df = self._impute_missing_values(cleaned_df)
            
            # Handle outliers
            cleaned_df = self._handle_outliers(cleaned_df)
            
            # Standardize formats
            cleaned_df = self._standardize_formats(cleaned_df)
            
            # Remove duplicates
            cleaned_df = self._remove_duplicates(cleaned_df)
            
            # Apply constraint corrections
            cleaned_df = self._apply_constraint_corrections(cleaned_df)
        
        logger.info("✅ Data cleaning complete")
        return cleaned_df
    
    def _impute_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Impute missing values using appropriate strategies"""
        cleaned_df = df.copy()
        
        # Numeric imputation
        numeric_columns = cleaned_df.select_dtypes(include=[np.number]).columns
        
        for col in numeric_columns:
            if col in self.quality_rules:
                if cleaned_df[col].isna().sum() > 0:
                    # Use median for skewed distributions
                    if col in ['annual_income', 'credit_card_debt', 'amount']:
                        cleaned_df[col] = cleaned_df[col].fillna(cleaned_df[col].median())
                    else:
                        cleaned_df[col] = cleaned_df[col].fillna(cleaned_df[col].mean())
        
        # Categorical imputation
        categorical_columns = cleaned_df.select_dtypes(include=['object']).columns
        
        for col in categorical_columns:
            if col in self.quality_rules:
                if cleaned_df[col].isna().sum() > 0:
                    # Use mode for categorical
                    mode_value = cleaned_df[col].mode()
                    if len(mode_value) > 0:
                        cleaned_df[col] = cleaned_df[col].fillna(mode_value[0])
        
        return cleaned_df
    
    def _handle_outliers(self, df: pd.DataFrame) -> pd.DataFrame:
        """Handle outliers using capping or transformation"""
        cleaned_df = df.copy()
        
        numeric_columns = cleaned_df.select_dtypes(include=[np.number]).columns
        
        for col in numeric_columns:
            if col in self.quality_rules and 'outlier_method' in self.quality_rules[col]:
                series = cleaned_df[col]
                
                if not series.empty and not series.isna().all():
                    # Cap outliers at 95th percentile
                    upper_cap = series.quantile(0.95)
                    lower_cap = series.quantile(0.05)
                    
                    cleaned_df[col] = series.clip(lower=lower_cap, upper=upper_cap)
        
        return cleaned_df
    
    def _standardize_formats(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize data formats"""
        cleaned_df = df.copy()
        
        # SSN formatting
        if 'ssn' in cleaned_df.columns:
            cleaned_df['ssn'] = cleaned_df['ssn'].astype(str).str.replace(r'[^0-9]', '', regex=True)
        
        # Phone formatting
        if 'phone' in cleaned_df.columns:
            phone_pattern = r'^\(?(\d{3})\)?[-.\s]?(\d{3})[-.\s]?(\d{4})$'
            cleaned_df['phone'] = cleaned_df['phone'].str.replace(phone_pattern, r'(\1) \2-\3', regex=True)
        
        # Email formatting
        if 'email' in cleaned_df.columns:
            cleaned_df['email'] = cleaned_df['email'].str.lower().str.strip()
        
        return cleaned_df
    
    def _remove_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove duplicate records"""
        # Define key columns for duplicate detection
        key_columns = ['customer_id', 'ssn', 'email']
        available_keys = [col for col in key_columns if col in df.columns]
        
        if available_keys:
            # Remove duplicates based on key columns
            cleaned_df = df.drop_duplicates(subset=available_keys, keep='first')
            
            duplicates_removed = len(df) - len(cleaned_df)
            if duplicates_removed > 0:
                logger.info(f"Removed {duplicates_removed} duplicate records")
        else:
            cleaned_df = df
        
        return cleaned_df
    
    def _apply_constraint_corrections(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply constraint corrections"""
        cleaned_df = df.copy()
        
        # Correct negative values for fields that shouldn't be negative
        non_negative_fields = ['annual_income', 'credit_card_debt', 'amount', 'account_age_days']
        
        for field in non_negative_fields:
            if field in cleaned_df.columns:
                cleaned_df[field] = cleaned_df[field].clip(lower=0)
        
        # Correct ratio fields to be between 0 and 1
        ratio_fields = ['chargeback_ratio', 'return_ratio']
        
        for field in ratio_fields:
            if field in cleaned_df.columns:
                cleaned_df[field] = cleaned_df[field].clip(lower=0, upper=1)
        
        return cleaned_df
    
    def generate_data_quality_dashboard(self, report: DataQualityReport) -> Dict[str, Any]:
        """Generate data quality dashboard information"""
        dashboard = {
            "summary": {
                "overall_score": report.overall_score,
                "total_records": report.total_records,
                "clean_records": report.clean_records,
                "data_quality_grade": self._get_quality_grade(report.overall_score)
            },
            "issues_breakdown": {
                issue.value: count for issue, count in report.issues_found.items()
            },
            "field_scores": report.field_quality_scores,
            "recommendations": report.recommendations,
            "auto_repairs": report.auto_repairs,
            "compliance_status": {
                "gdpr_compliant": report.overall_score >= 0.8,
                "pci_compliant": report.overall_score >= 0.9,
                "audit_ready": report.overall_score >= 0.85
            },
            "improvement_priority": self._prioritize_improvements(report)
        }
        
        return dashboard
    
    def _get_quality_grade(self, score: float) -> str:
        """Convert quality score to letter grade"""
        if score >= 0.95:
            return "A+"
        elif score >= 0.9:
            return "A"
        elif score >= 0.85:
            return "B+"
        elif score >= 0.8:
            return "B"
        elif score >= 0.75:
            return "C+"
        elif score >= 0.7:
            return "C"
        elif score >= 0.6:
            return "D"
        else:
            return "F"
    
    def _prioritize_improvements(self, report: DataQualityReport) -> List[Dict[str, Any]]:
        """Prioritize data quality improvements"""
        improvements = []
        
        # Priority 1: Critical issues
        if report.issues_found[DataQualityIssue.CONSTRAINT_VIOLATION] > 0:
            improvements.append({
                "priority": "HIGH",
                "issue": "Constraint Violations",
                "impact": "Critical - affects fraud detection accuracy",
                "action": "Implement validation at data source"
            })
        
        # Priority 2: Missing values in key fields
        if report.issues_found[DataQualityIssue.MISSING_VALUE] > 0:
            improvements.append({
                "priority": "HIGH",
                "issue": "Missing Values",
                "impact": "High - reduces model effectiveness",
                "action": "Implement data collection improvements"
            })
        
        # Priority 3: Outliers
        if report.issues_found[DataQualityIssue.OUTLIER] > 0:
            improvements.append({
                "priority": "MEDIUM",
                "issue": "Outliers",
                "impact": "Medium - may indicate fraud or data errors",
                "action": "Review outlier detection rules"
            })
        
        # Priority 4: Format issues
        if report.issues_found[DataQualityIssue.INVALID_FORMAT] > 0:
            improvements.append({
                "priority": "MEDIUM",
                "issue": "Format Issues",
                "impact": "Medium - affects data consistency",
                "action": "Standardize data formats"
            })
        
        return improvements