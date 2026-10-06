# End-to-end test in both directions through the Kerberos-authenticated bridge (PowerShell).
#   Kafka  -> Solace : produce to KAFKA_TOPIC_IN, expect it on Solace queue SOLACE_QUEUE_FROM_KAFKA
#   Solace -> Kafka  : publish to solace/orders/..., expect it on Kafka topic KAFKA_TOPIC_OUT
$ErrorActionPreference = "Continue"
Set-Location (Join-Path $PSScriptRoot "..")

# load .env
$env_ = @{}
Get-Content .env | Where-Object { $_ -match '^\s*[^#].*=' } | ForEach-Object {
    $k, $v = $_ -split '=', 2; $env_[$k.Trim()] = $v.Trim()
}
$topicIn  = $env_['KAFKA_TOPIC_IN'];  $topicOut = $env_['KAFKA_TOPIC_OUT']
$qFrom    = $env_['SOLACE_QUEUE_FROM_KAFKA']; $qTo = $env_['SOLACE_QUEUE_TO_KAFKA']
$stamp    = "smoke-" + (Get-Date -Format "yyyyMMddTHHmmss")
$pass = 0; $fail = 0

Write-Host "=============================================================="
Write-Host " 1) Kafka -> Solace   ($topicIn -> kafka/$topicIn -> $qFrom)"
Write-Host "=============================================================="
$before = [int]((docker compose run --rm -T solace-tools qcount $qFrom 2>$null | Select-Object -Last 1))
Write-Host "  queue $qFrom spooled before: $before"

docker compose exec -T kafka-client produce $topicIn "$stamp hello-from-kafka"
Start-Sleep -Seconds 6
$after = [int]((docker compose run --rm -T solace-tools qcount $qFrom 2>$null | Select-Object -Last 1))
Write-Host "  queue $qFrom spooled after:  $after"
if ($after -gt $before) { Write-Host "  PASS  message spooled on $qFrom (+$($after-$before))"; $pass++ }
else { Write-Host "  FAIL  queue count did not increase - run: docker compose run --rm solace-tools status"; $fail++ }

Write-Host ""
Write-Host "=============================================================="
Write-Host " 2) Solace -> Kafka   (solace/orders/smoke -> $qTo -> $topicOut)"
Write-Host "=============================================================="
docker compose run --rm -T solace-tools publish "solace/orders/smoke" "$stamp hello-from-solace"
Start-Sleep -Seconds 4
$consumed = docker compose exec -T kafka-client consume $topicOut 15 2>$null
$hit = $consumed | Select-String -SimpleMatch $stamp | Select-Object -First 1
if ($hit) { Write-Host "  PASS  record received on Kafka topic ${topicOut}: $hit"; $pass++ }
else { Write-Host "  FAIL  record not found on $topicOut"; $consumed | Select-Object -Last 5; $fail++ }

Write-Host ""
Write-Host "=============================================================="
Write-Host " Result: $pass passed, $fail failed"
Write-Host "=============================================================="
exit $fail
