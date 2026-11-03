$ErrorActionPreference='Stop'
Set-Location $PSScriptRoot
if (-not (Test-Path .venv\Scripts\python.exe)) {
    python -m venv .venv
    if ($LASTEXITCODE -ne 0) { throw 'Python virtual environment creation failed' }
}
& .\.venv\Scripts\python.exe -m pip install -r requirements-lab.txt
if ($LASTEXITCODE -ne 0) { throw 'Dependency installation failed' }
& .\.venv\Scripts\python.exe scripts/experiment.py
if ($LASTEXITCODE -ne 0) { throw 'Experiment incomplete. Read the error above; existing evidence is preserved.' }
