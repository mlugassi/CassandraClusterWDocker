# Start Cassandra Docker Cluster (Windows PowerShell script)

[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

$targetDir = Split-Path -Parent $MyInvocation.MyCommand.Definition

function Confirm-Step($message) {
    Read-Host "$message (Press ENTER to continue)"
}

function Check-Dependency($cmd, $name, $url) {
    if (-not (Get-Command $cmd -ErrorAction SilentlyContinue)) {
        Write-Warning "$name not found. Please install it from: $url"
        Start-Process $url
        Read-Host "After installing $name, press ENTER to exit and rerun the script."
        exit 1
    } else {
        Write-Host "$name found."
    }
}

function Start-Docker {
    Write-Host "Launching Docker Desktop..."
    Start-Process "C:\Program Files\Docker\Docker\Docker Desktop.exe" -ErrorAction SilentlyContinue
    Confirm-Step "Wait until Docker is fully started"
}

function Wait-For-Docker {
    Write-Host "Waiting for Docker to be ready..."
    while (-not (docker info -ErrorAction SilentlyContinue)) {
        Write-Host "Docker not ready... retrying in 5 seconds"
        Start-Sleep -Seconds 5
    }
    Write-Host "Docker is ready."
}

function Docker-Up {
    Set-Location $targetDir
    Confirm-Step "About to run: docker compose up -d"
    docker compose up -d
    if ($LASTEXITCODE -eq 0) {
        Write-Host "Cassandra containers are running."
    } else {
        Write-Error "Docker Compose failed. Please check your Docker status."
    }
}

# Start process
Write-Host ""
Write-Host "Starting Cassandra Docker Cluster Setup"

Check-Dependency "docker" "Docker Desktop" "https://www.docker.com/products/docker-desktop/"

Start-Docker
Wait-For-Docker
Docker-Up

Write-Host ""
Write-Host "Setup complete."
