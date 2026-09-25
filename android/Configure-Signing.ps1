# Run from the PRIVATE signing backup folder, using your existing GitHub CLI login.
$ErrorActionPreference = 'Stop'
$repo = 'gio9772rm/weather-app'
$key = Join-Path $PSScriptRoot 'meteo-pro.jks'
$passwordFile = Join-Path $PSScriptRoot 'password.txt'
if (!(Test-Path $key) -or !(Test-Path $passwordFile)) { throw 'Esegui questo script dalla cartella del backup privato della firma.' }
gh auth status
if ($LASTEXITCODE -ne 0) { throw 'Prima esegui gh auth login e accedi al tuo account GitHub.' }
[Convert]::ToBase64String([IO.File]::ReadAllBytes($key)) | gh secret set ANDROID_KEYSTORE_B64 --repo $repo
if ($LASTEXITCODE -ne 0) { throw 'Salvataggio del keystore non riuscito.' }
[IO.File]::ReadAllText($passwordFile).Trim() | gh secret set ANDROID_KEYSTORE_PASSWORD --repo $repo
if ($LASTEXITCODE -ne 0) { throw 'Salvataggio della password non riuscito.' }
gh workflow run android_release.yml --repo $repo --ref main
if ($LASTEXITCODE -ne 0) { throw 'Segreti salvati; avvia Android OTA - Release da GitHub Actions.' }
Write-Host 'Firma automatica configurata. Conserva una copia privata del backup; non caricarlo nel repository.'
