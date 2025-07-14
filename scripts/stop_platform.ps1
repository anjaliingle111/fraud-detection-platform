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
    Get-Process | Where-Object {.ProcessName -like "*python*"} | Stop-Process -Force
    Write-Host "Platform stopped" -ForegroundColor Green
}
