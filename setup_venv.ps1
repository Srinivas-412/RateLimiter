# setup_venv.ps1
# Creates a virtual environment, installs dependencies, and prints usage hints.
# Run from the repo root:  .\setup_venv.ps1

$ErrorActionPreference = "Stop"
$VenvDir = ".venv"

Write-Host "`n=== Rate Limiter — Virtual Environment Setup ===" -ForegroundColor Cyan

# 1. Create venv
if (-not (Test-Path $VenvDir)) {
    Write-Host "Creating virtual environment in '$VenvDir'..." -ForegroundColor Yellow
    python -m venv $VenvDir
} else {
    Write-Host "Virtual environment '$VenvDir' already exists — skipping creation." -ForegroundColor Green
}

# 2. Activate
$ActivateScript = Join-Path $VenvDir "Scripts\Activate.ps1"
Write-Host "Activating virtual environment..." -ForegroundColor Yellow
& $ActivateScript

# 3. Upgrade pip silently
python -m pip install --upgrade pip --quiet

# 4. Install dependencies
Write-Host "Installing dependencies from requirements.txt..." -ForegroundColor Yellow
pip install -r requirements.txt

Write-Host "`n=== Setup complete! ===" -ForegroundColor Green
Write-Host ""
Write-Host "Quick-start commands:" -ForegroundColor Cyan
Write-Host "  Run demo scenario   :  python demo/scenario.py"
Write-Host "  Run tests           :  pytest tests/ -v"
Write-Host "  Start API server    :  uvicorn api.app:app --reload"
Write-Host "  Try the API (curl)  :  curl 'http://localhost:8000/api/data?client_id=alice'"
Write-Host ""
