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
