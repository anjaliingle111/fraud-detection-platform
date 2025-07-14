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
