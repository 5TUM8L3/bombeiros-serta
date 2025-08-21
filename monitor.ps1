Param(
  [string]$Municipio = "Sertã",
  [int]$PollSeconds = 30,
  [string]$Topic = "bombeiros-serta",
  [string]$NtfyUrl = "https://ntfy.sh",
  [string]$StateFile = "last_ids.json"
)

$env:MUNICIPIO = $Municipio
$env:POLL_SECONDS = [string]$PollSeconds
$env:NTFY_TOPIC = $Topic
$env:NTFY_URL = $NtfyUrl
$env:STATE_FILE = $StateFile

Write-Host "Iniciando monitor para '$Municipio' a cada $PollSeconds s (tópico: $Topic)"
bun run start
