@echo off
:: Runs the PowerShell script to start the Cassandra Docker cluster

echo Starting Cassandra cluster using Docker Compose...
PowerShell -NoProfile -ExecutionPolicy Bypass -File "%~dp0start-cassandra-cluster.ps1"
pause
