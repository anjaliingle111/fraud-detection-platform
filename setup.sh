# Simple Cloud-Native Data Platform Setup Script for Windows
# This script sets up the complete enterprise data platform on Windows

Write-Host "CLOUD-NATIVE DATA PLATFORM SETUP (Windows)" -ForegroundColor Blue
Write-Host "===============================================" -ForegroundColor Blue

# Function to print colored output
function Write-Info {
    param($Message)
    Write-Host "[INFO] $Message" -ForegroundColor Green
}

function Write-Warning {
    param($Message)
    Write-Host "[WARNING] $Message" -ForegroundColor Yellow
}

function Write-Error {
    param($Message)
    Write-Host "[ERROR] $Message" -ForegroundColor Red
}

# Check prerequisites
Write-Info "Checking prerequisites..."

# Check Python
try {
    $pythonVersion = python --version
    Write-Info "Python found: $pythonVersion"
} catch {
    Write-Error "Python is required but not installed"
    exit 1
}

# Check pip
try {
    $pipVersion = pip --version
    Write-Info "pip found: $pipVersion"
} catch {
    Write-Error "pip is required but not installed"
    exit 1
}

# Check Docker
$dockerAvailable = $false
try {
    $dockerVersion = docker --version
    $dockerAvailable = $true
    Write-Info "Docker found: $dockerVersion"
} catch {
    Write-Warning "Docker is not installed. Some features will be limited."
}

Write-Info "Prerequisites check completed"

# Create project structure
Write-Info "Creating project structure..."

# Create directories
$directories = @(
    "data", "logs", "config", "notebooks", "sql", "monitoring", "backup", "scripts", "tests",
    "cloud-deployment", "cloud-deployment\docker", "cloud-deployment\kubernetes", 
    "cloud-deployment\terraform", "cloud-deployment\ci-cd", "cloud-deployment\monitoring", 
    "cloud-deployment\scripts"
)

foreach ($dir in $directories) {
    if (-not (Test-Path $dir)) {
        New-Item -ItemType Directory -Path $dir -Force | Out-Null
        Write-Info "Created directory: $dir"
    }
}

# Create .env file
Write-Info "Creating configuration files..."

$envContent = "ENVIRONMENT=development`nSPARK_MASTER=local[*]`nPOSTGRES_HOST=localhost`nPOSTGRES_PORT=5432`nPOSTGRES_DB=data_platform`nPOSTGRES_USER=datauser`nPOSTGRES_PASSWORD=datapass123`nREDIS_HOST=localhost`nREDIS_PORT=6379`nMONITORING_ENABLED=true`nLOG_LEVEL=INFO"

$envContent | Out-File -FilePath ".env" -Encoding UTF8
Write-Info "Created .env file"

# Create basic config
$configContent = @"
app:
  name: "Cloud-Native Data Platform"
  version: "1.0.0"
  environment: "development"

spark:
  master: "local[*]"
  app_name: "DataPlatform"
  executor_memory: "2g"
  driver_memory: "1g"

data:
  input_path: "./data/input"
  output_path: "./data/output"
  checkpoint_path: "./data/checkpoints"

monitoring:
  enabled: true
  metrics_port: 8080
  prometheus_enabled: true
  grafana_enabled: true

security:
  encryption_enabled: true
  authentication_required: false
  ssl_enabled: false
"@

$configContent | Out-File -FilePath "config\app.yaml" -Encoding UTF8
Write-Info "Created app.yaml config"

# Create requirements.txt
Write-Info "Creating requirements.txt..."

$requirements = @"
pyspark==3.5.0
pandas==2.1.0
numpy==1.24.3
faker==19.6.2
scikit-learn==1.3.0
flask==2.3.3
pyyaml==6.0.1
prometheus-client==0.17.1
python-dotenv==1.0.0
pytest==7.4.0
black==23.7.0
xgboost==1.7.6
gunicorn==21.2.0
cachetools==5.3.1
joblib==1.3.2
"@

$requirements | Out-File -FilePath "requirements.txt" -Encoding UTF8
Write-Info "Created requirements.txt"

# Install Python dependencies
Write-Info "Installing Python dependencies..."
try {
    pip install --upgrade pip
    pip install -r requirements.txt
    Write-Info "Python dependencies installed successfully"
} catch {
    Write-Error "Failed to install Python dependencies"
}

# Create docker-compose.yml
Write-Info "Creating Docker Compose configuration..."

$dockerCompose = @"
version: '3.8'

services:
  data-platform:
    build:
      context: .
      dockerfile: cloud-deployment/docker/Dockerfile
    ports:
      - "8080:8080"
    environment:
      - ENVIRONMENT=development
      - SPARK_MASTER=local[*]
      - MONITORING_ENABLED=true
    volumes:
      - ./data:/app/data
      - ./logs:/app/logs
      - ./config:/app/config
    networks:
      - data-platform-network

  postgres:
    image: postgres:15
    environment:
      - POSTGRES_DB=data_platform
      - POSTGRES_USER=datauser
      - POSTGRES_PASSWORD=datapass123
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data
    networks:
      - data-platform-network

  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    volumes:
      - redis_data:/data
    networks:
      - data-platform-network

  prometheus:
    image: prom/prometheus:latest
    ports:
      - "9090:9090"
    volumes:
      - ./monitoring/prometheus.yml:/etc/prometheus/prometheus.yml
    networks:
      - data-platform-network

  grafana:
    image: grafana/grafana:latest
    ports:
      - "3000:3000"
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=admin123
    volumes:
      - grafana_data:/var/lib/grafana
    networks:
      - data-platform-network

volumes:
  postgres_data:
  redis_data:
  grafana_data:

networks:
  data-platform-network:
    driver: bridge
"@

$dockerCompose | Out-File -FilePath "docker-compose.yml" -Encoding UTF8
Write-Info "Created docker-compose.yml"

# Create simple Dockerfile
Write-Info "Creating Dockerfile..."

$dockerfileContent = @'
FROM python:3.9-slim

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

RUN apt-get update && apt-get install -y \
    openjdk-11-jre \
    curl \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

CMD ["python", "cloud_native_data_platform.py"]
'@

$dockerfileContent | Out-File -FilePath "cloud-deployment\docker\Dockerfile" -Encoding UTF8
Write-Info "Created Dockerfile"

# Create PowerShell scripts
Write-Info "Creating PowerShell scripts..."

# Run script
$runScript = @"
Write-Host "Starting Cloud-Native Data Platform" -ForegroundColor Blue
Write-Host "======================================" -ForegroundColor Blue

if (Get-Command docker -ErrorAction SilentlyContinue) {
    if (Test-Path "docker-compose.yml") {
        Write-Host "Starting with Docker..." -ForegroundColor Green
        docker-compose up -d
        Write-Host "Platform started with Docker" -ForegroundColor Green
        Write-Host "Access: http://localhost:8080" -ForegroundColor Cyan
    } else {
        Write-Host "docker-compose.yml not found" -ForegroundColor Red
    }
} else {
    Write-Host "Starting with Python..." -ForegroundColor Green
    python cloud_native_data_platform.py
}
"@

$runScript | Out-File -FilePath "scripts\run_platform.ps1" -Encoding UTF8

# Stop script
$stopScript = @"
Write-Host "Stopping Cloud-Native Data Platform" -ForegroundColor Blue
Write-Host "=====================================" -ForegroundColor Blue

if (Get-Command docker -ErrorAction SilentlyContinue) {
    if (Test-Path "docker-compose.yml") {
        Write-Host "Stopping Docker services..." -ForegroundColor Yellow
        docker-compose down
        Write-Host "Platform stopped" -ForegroundColor Green
    }
} else {
    Write-Host "Stopping Python processes..." -ForegroundColor Yellow
    Get-Process | Where-Object {$_.ProcessName -like "*python*"} | Stop-Process -Force
    Write-Host "Platform stopped" -ForegroundColor Green
}
"@

$stopScript | Out-File -FilePath "scripts\stop_platform.ps1" -Encoding UTF8

# Status script
$statusScript = @"
Write-Host "Cloud-Native Data Platform Status" -ForegroundColor Blue
Write-Host "====================================" -ForegroundColor Blue

if (Get-Command docker -ErrorAction SilentlyContinue) {
    if (Test-Path "docker-compose.yml") {
        Write-Host "Docker Services:" -ForegroundColor Green
        docker-compose ps
        Write-Host ""
        Write-Host "Service URLs:" -ForegroundColor Cyan
        Write-Host "   Data Platform: http://localhost:8080"
        Write-Host "   Grafana: http://localhost:3000"
        Write-Host "   Prometheus: http://localhost:9090"
    }
} else {
    Write-Host "Python Process Status:" -ForegroundColor Green
    if (Get-Process | Where-Object {$_.ProcessName -like "*python*"}) {
        Write-Host "   Platform is running" -ForegroundColor Green
    } else {
        Write-Host "   Platform is not running" -ForegroundColor Red
    }
}
"@

$statusScript | Out-File -FilePath "scripts\status.ps1" -Encoding UTF8

Write-Info "Created PowerShell scripts"

# Create monitoring configuration
Write-Info "Creating monitoring configuration..."

$prometheusConfig = @"
global:
  scrape_interval: 15s
  evaluation_interval: 15s

scrape_configs:
  - job_name: 'data-platform'
    static_configs:
      - targets: ['data-platform:8080']
    scrape_interval: 5s
    metrics_path: '/metrics'
"@

$prometheusConfig | Out-File -FilePath "monitoring\prometheus.yml" -Encoding UTF8
Write-Info "Created Prometheus configuration"

# Create .gitignore
$gitignore = @"
__pycache__/
*.py[cod]
*.so
.Python
build/
develop-eggs/
dist/
downloads/
eggs/
.eggs/
lib/
lib64/
parts/
sdist/
var/
wheels/
*.egg-info/
.installed.cfg
*.egg
.env
.venv
env/
venv/
ENV/
env.bak/
venv.bak/
data/
logs/
*.log
.vscode/
.idea/
*.swp
*.swo
.dockerignore
.DS_Store
Thumbs.db
"@

$gitignore | Out-File -FilePath ".gitignore" -Encoding UTF8
Write-Info "Created .gitignore"

# Create README
$readme = @"
# Cloud-Native Data Platform

Enterprise-grade data engineering platform with cloud deployment capabilities.

## Quick Start (Windows)

1. Setup Environment: .\setup_windows.ps1
2. Start Platform: .\scripts\run_platform.ps1
3. Check Status: .\scripts\status.ps1
4. Access: http://localhost:8080

## Service URLs

- Data Platform: http://localhost:8080
- Grafana: http://localhost:3000 (admin/admin123)
- Prometheus: http://localhost:9090

## Development

Start development environment:
docker-compose up -d

Run tests:
pytest tests/

## Next Steps

1. Create cloud_native_data_platform.py with the main platform code
2. Run the platform with .\scripts\run_platform.ps1
3. Access the platform at http://localhost:8080

Your enterprise data platform is ready!
"@

$readme | Out-File -FilePath "README.md" -Encoding UTF8
Write-Info "Created README.md"

Write-Host ""
Write-Host "SETUP COMPLETE!" -ForegroundColor Blue
Write-Host "================" -ForegroundColor Blue
Write-Host ""
Write-Info "Cloud-Native Data Platform setup completed successfully!"
Write-Host ""
Write-Info "Project structure created"
Write-Info "Python dependencies installed"
Write-Info "Docker environment configured"
Write-Info "PowerShell scripts created"
Write-Info "Monitoring setup completed"
Write-Host ""
Write-Host "NEXT STEPS:" -ForegroundColor Yellow
Write-Host "1. Create cloud_native_data_platform.py with the platform code"
Write-Host "2. Run: .\scripts\run_platform.ps1"
Write-Host "3. Access: http://localhost:8080"
Write-Host ""
Write-Host "Your enterprise data platform is ready!" -ForegroundColor Green