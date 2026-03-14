# Deploy to VPS via SSH: sync files, build Go worker, restart services.
# Usage: .\scripts\deploy-via-ssh.ps1 -DeploySsh "root@api.corestats.pro"
#        .\scripts\deploy-via-ssh.ps1 -DeploySsh "root@1.2.3.4" -SshPort 22
#    or: $env:DEPLOY_SSH = "root@your-vps"; .\scripts\deploy-via-ssh.ps1

param(
    [string]$DeploySsh = $env:DEPLOY_SSH,
    [string]$RepoRoot = "/var/www/BrawlGoStats",
    [int]$SshPort = 0
)

if (-not $DeploySsh) {
    Write-Error "Usage: .\deploy-via-ssh.ps1 -DeploySsh user@host   OR set `$env:DEPLOY_SSH"
    exit 1
}

$ProjectRoot = Split-Path $PSScriptRoot -Parent
if (-not (Test-Path "$ProjectRoot\worker\main.go")) { $ProjectRoot = (Get-Location).Path }

$sshOpts = @()
$scpOpts = @()
if ($SshPort -gt 0) {
    $sshOpts = @("-p", $SshPort)
    $scpOpts = @("-P", $SshPort)
}

Write-Host "=== Deploying to ${DeploySsh}:${RepoRoot} (SSH port: $(if($SshPort){"$SshPort"}else{"22"})) ==="

# Option A: rsync if available
$rsync = Get-Command rsync -ErrorAction SilentlyContinue
if ($rsync) {
    $excludeArgs = "--exclude=.git --exclude=venv --exclude=__pycache__ --exclude=*.pyc"
    if ($SshPort -gt 0) { $excludeArgs += " -e `"ssh -p $SshPort`"" }
    Invoke-Expression "rsync -avz $excludeArgs `"$ProjectRoot/`" `"${DeploySsh}:${RepoRoot}/`""
} else {
    # Option B: scp key files then run git pull + build on server
    Write-Host "rsync not found. Copying worker and discovery_scraper, then remote build..."
    scp @scpOpts "$ProjectRoot\worker\main.go" "${DeploySsh}:${RepoRoot}/worker/"
    scp @scpOpts "$ProjectRoot\discovery_scraper.py" "${DeploySsh}:${RepoRoot}/"
    Write-Host "Running remote build and restart..."
}

$remoteScript = @"
set -e
cd $RepoRoot && (git pull 2>/dev/null || true)
cd $RepoRoot/worker && go build -o worker .
systemctl restart brawl-discovery brawl-worker
systemctl is-active brawl-discovery brawl-worker
echo Done.
"@
if ($SshPort -gt 0) {
    $remoteScript | ssh -p $SshPort $DeploySsh "bash -s"
} else {
    $remoteScript | ssh $DeploySsh "bash -s"
}
