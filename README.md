# 🛡️ Enterprise Fraud Detection Platform

A production-ready fraud detection system that analyzes 50+ real-world risk factors to identify fraudulent transactions with 79.86% AUC performance.

## 📋 Project Description

This enterprise-grade fraud detection platform implements a comprehensive risk assessment system that processes financial transactions in real-time. The system combines traditional fraud indicators with advanced behavioral analytics, credit scoring, and regulatory compliance checks to deliver actionable business decisions.

**Key Features:**
- Real-time fraud detection with sub-second response times
- 50+ fraud risk factors including credit scores, SSN analysis, and behavioral patterns
- Enterprise data lake architecture with Bronze → Silver → Gold medallion pattern
- GDPR/CCPA/PCI DSS compliant privacy masking
- Cost-benefit analysis with ROI calculations
- Production-ready REST APIs

## 🔧 Requirements

### System Requirements
- Python 3.8+
- 8GB+ RAM
- 10GB+ storage space

### Dependencies
```bash
pip install -r requirements.txt
```

**Core Libraries:**
- `pandas` - Data manipulation and analysis
- `scikit-learn` - Machine learning models
- `flask` - REST API framework
- `numpy` - Numerical computing
- `faker` - Synthetic data generation
- `hashlib` - Privacy masking utilities

## 🔄 Workflow

```mermaid
graph TB
    A[Transaction Input] --> B[Data Enrichment]
    B --> C[Feature Engineering]
    C --> D[Privacy Masking]
    D --> E[ML Model Prediction]
    E --> F[Business Rules Engine]
    F --> G[Risk Assessment]
    G --> H[Cost-Benefit Analysis]
    H --> I[Final Decision]
    I --> J[Audit Trail]
    
    style A fill:#e1f5fe
    style I fill:#c8e6c9
    style J fill:#fff3e0
```

### Data Processing Pipeline

```mermaid
graph LR
    subgraph "Bronze Layer"
        A[Raw Transaction Data]
        B[Customer Data]
    end
    
    subgraph "Silver Layer"
        C[Enriched Transactions]
        D[Feature Engineering]
        E[Privacy Masking]
    end
    
    subgraph "Gold Layer"
        F[ML Features]
        G[Risk Scores]
        H[Business Intelligence]
    end
    
    A --> C
    B --> C
    C --> D
    D --> E
    E --> F
    F --> G
    G --> H
```

## 🎯 Use Cases

### Primary Use Cases
- **Real-time Transaction Monitoring** - Instant fraud detection for online payments
- **Risk Assessment** - Comprehensive evaluation of customer creditworthiness
- **Regulatory Compliance** - AML/KYC screening and sanctions checking
- **Business Intelligence** - ROI analysis and fraud prevention optimization

### Industry Applications
- **Banking & Finance** - Credit card fraud detection
- **E-commerce** - Payment fraud prevention
- **Insurance** - Claims fraud identification
- **Fintech** - Digital wallet security

### Decision Matrix
| Risk Level | Fraud Probability | Business Action | Use Case |
|------------|------------------|----------------|----------|
| LOW | 0-15% | APPROVE | Normal transactions |
| MEDIUM | 15-45% | REVIEW | Manual verification |
| HIGH | 45-75% | DECLINE | Automatic rejection |
| CRITICAL | 75%+ | ESCALATE | Investigation required |

## 📊 Data Model Diagram

```mermaid
erDiagram
    CUSTOMERS {
        string customer_id PK
        string ssn
        float fico_score
        float cibil_score
        float annual_income
        string employment_status
        int account_age_days
        float debt_to_income_ratio
        float credit_utilization
        int overdraft_count
        timestamp created_at
    }
    
    TRANSACTIONS {
        string transaction_id PK
        string customer_id FK
        float amount
        string channel
        string merchant
        timestamp transaction_time
        string ip_address
        string device_id
        float chargeback_ratio
        float return_rate
        string location
        timestamp created_at
    }
    
    RISK_ASSESSMENTS {
        string assessment_id PK
        string transaction_id FK
        float fraud_probability
        string risk_level
        string business_action
        float confidence_score
        json contributing_factors
        float expected_cost
        float fraud_prevented
        float roi
        timestamp created_at
    }
    
    FEATURE_STORE {
        string feature_id PK
        string transaction_id FK
        float credit_risk
        float amount_risk
        float velocity_risk
        float account_age_risk
        float identity_risk
        float device_risk
        float behavioral_risk
        float regulatory_risk
        json feature_vector
        timestamp created_at
    }
    
    AUDIT_TRAIL {
        string audit_id PK
        string transaction_id FK
        string action_type
        string user_id
        json before_state
        json after_state
        string decision_reason
        timestamp created_at
    }
    
    CUSTOMERS ||--o{ TRANSACTIONS : "1-to-many"
    TRANSACTIONS ||--|| RISK_ASSESSMENTS : "1-to-1"
    TRANSACTIONS ||--|| FEATURE_STORE : "1-to-1"
    TRANSACTIONS ||--o{ AUDIT_TRAIL : "1-to-many"
```

### Data Flow Architecture

```mermaid
graph TD
    subgraph "Data Sources"
        A[Transaction API]
        B[Customer Database]
        C[Credit Bureau]
        D[Device Intelligence]
    end
    
    subgraph "Data Lake"
        E[Bronze Layer<br/>Raw Data]
        F[Silver Layer<br/>Processed Data]
        G[Gold Layer<br/>Analytics Ready]
    end
    
    subgraph "ML Pipeline"
        H[Feature Engineering]
        I[Model Training]
        J[Model Serving]
    end
    
    subgraph "Applications"
        K[Fraud Detection API]
        L[Risk Dashboard]
        M[Compliance Reports]
    end
    
    A --> E
    B --> E
    C --> E
    D --> E
    
    E --> F
    F --> G
    
    G --> H
    H --> I
    I --> J
    
    J --> K
    G --> L
    G --> M
```

## 🚀 Quick Start

1. **Clone the repository**
```bash
git clone https://github.com/anjaliingle111/fraud-detection-platform.git
cd fraud-detection-platform
```

2. **Install dependencies**
```bash
pip install -r requirements.txt
```

3. **Run the platform**
```bash
python cloud_native_data_platform_fixed.py
```

4. **Access the API**
- Health Check: `http://localhost:8080/health`
- Fraud Detection: `http://localhost:8080/predict`
- Web Interface: Open `comprehensive_fraud_input.html`

## 📈 Performance Metrics

- **Model Accuracy:** 79.86% AUC
- **Response Time:** <100ms average
- **Throughput:** 100,000+ transactions processed
- **Data Processing:** 50+ fraud factors analyzed per transaction
- **ROI:** 229x-1,289x return on fraud prevention investment

## 🏗️ Architecture Overview

### System Components
- **Data Layer:** Bronze-Silver-Gold medallion architecture
- **ML Pipeline:** Feature engineering, model training, and serving
- **API Layer:** RESTful endpoints for real-time predictions
- **Privacy Layer:** GDPR/CCPA compliant data masking
- **Monitoring:** Performance tracking and audit trails

### Risk Factor Categories
1. **Credit Risk** - FICO/CIBIL scores, debt ratios, credit utilization
2. **Identity Risk** - SSN validation, velocity checks, verification status
3. **Behavioral Risk** - Transaction patterns, chargeback history, return rates
4. **Account Risk** - Age, overdraft history, employment status
5. **Device Risk** - Device fingerprinting, IP reputation, location analysis
6. **Regulatory Risk** - AML screening, sanctions checks, PEP status