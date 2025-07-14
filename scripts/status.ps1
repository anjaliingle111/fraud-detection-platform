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
    if (Get-Process | Where-Object {.ProcessName -like "*python*"}) {
        Write-Host "   Platform is running" -ForegroundColor Green
    } else {
        Write-Host "   Platform is not running" -ForegroundColor Red
    }
}
