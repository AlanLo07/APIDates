# deploy.ps1 — Script de despliegue mejorado
param(
    [string]$Environment = "dev",
    [string]$Region = "us-east-2"
)

$StackName = "citas-app-$Environment"

Write-Host "🔨 Building..." -ForegroundColor Cyan
sam build --use-container  # Usa contenedor para asegurar dependencias correctas en arm64

if ($LASTEXITCODE -ne 0) {
    Write-Error "Build fallido"
    exit 1
}

Write-Host "🚀 Deploying stack: $StackName to $Region..." -ForegroundColor Cyan
sam deploy `
    --stack-name $StackName `
    --region $Region `
    --capabilities CAPABILITY_IAM `
    --resolve-s3 `
    --no-confirm-changeset `
    --parameter-overrides "Environment=$Environment"

if ($LASTEXITCODE -ne 0) {
    Write-Error "Deploy fallido"
    exit 1
}

Write-Host "✅ Despliegue exitoso!" -ForegroundColor Green

# Obtener URL de la API
$ApiUrl = aws cloudformation describe-stacks `
    --stack-name $StackName `
    --region $Region `
    --query "Stacks[0].Outputs[?OutputKey=='ApiUrl'].OutputValue" `
    --output text

Write-Host "🌐 API URL: $ApiUrl" -ForegroundColor Yellow
Write-Host ""
Write-Host "Endpoints disponibles:"
Write-Host "  GET    $ApiUrl/planes          — Listar planes"
Write-Host "  POST   $ApiUrl/planes          — Crear plan"
Write-Host "  PUT    $ApiUrl/planes/{id}     — Actualizar plan"
Write-Host "  DELETE $ApiUrl/planes/{id}     — Eliminar plan"
Write-Host "  GET    $ApiUrl/citas           — Listar citas"
Write-Host "  POST   $ApiUrl/citas           — Crear cita"
Write-Host "  PUT    $ApiUrl/citas/{id}      — Actualizar cita"
Write-Host "  DELETE $ApiUrl/citas/{id}      — Eliminar cita"
Write-Host "  GET    $ApiUrl/random          — Plan aleatorio"
Write-Host "  GET    $ApiUrl/random?tipo=restaurante&soloNuevos=true"