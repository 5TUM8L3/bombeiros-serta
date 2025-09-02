# Bombeiros-Serta (Go)

Monitor de incidentes ativos (GeoJSON) de Fogos.pt filtrado por municípios (Sertã e vizinhos). Envia alertas opcionais via ntfy.

## Requisitos

- Go 1.22+
- Windows (funciona também em Linux/macOS)

## Build rápido

PowerShell:

```
Set-Location E:\bombeiros-serta
go build -o bin/monitor.exe ./cmd/monitor
```

CMD:

```
cd /d E:\bombeiros-serta
go build -o bin\monitor.exe .\cmd\monitor
```

## Execução

- Uma só vez (sem polling):

PowerShell:

```
$env:POLL_SECONDS = '0'
& .\bin\monitor.exe
```

CMD:

```
set POLL_SECONDS=0 && bin\monitor.exe
```

- Contínuo (padrão 30s):

PowerShell:

```
$env:NTFY_TOPIC = 'bombeiros-serta'
$env:POLL_SECONDS = '60'
& .\bin\monitor.exe
```

CMD:

```
set NTFY_TOPIC=bombeiros-serta && set POLL_SECONDS=60 && bin\monitor.exe
```

Ou use os scripts:

- `monitor.ps1` (PowerShell)
- `monitor.bat` (CMD)

## Variáveis de ambiente

- `MUNICIPIOS` lista separada por vírgula ou ponto-e-vírgula. Ex.:
  - PowerShell: `$env:MUNICIPIOS = 'Sertã,Oleiros,Castanheira de Pera,Proença-a-Nova'`
  - CMD: `set MUNICIPIOS=Sertã,Oleiros,Castanheira de Pera,Proença-a-Nova`
- `POLL_SECONDS` intervalo em segundos (0 executa só uma vez)
- `NTFY_URL` URL base do ntfy (def: `https://ntfy.sh`)
- `NTFY_TOPIC` tópico do ntfy (def: `bombeiros-serta`)
- `NTFY_PRIORITY` prioridade ntfy 1–5 (def: `5`)
- `NTFY_TAGS` tags/emojis (def: `fire,rotating_light`)
- `FOGOS_URL` endpoint de incidents ativos (def: `https://api.fogos.pt/v2/incidents/active?geojson=true`)
- `FOGOS_FALLBACK_URLS` lista de endpoints fallback separados por vírgula/; (opcional)
- `FOGOS_API_KEY` token opcional (se necessário pelo endpoint)
- `STATE_FILE` caminho do ficheiro de estado (def: `last_ids.json`)
- `NTFY_TEST` se definido, envia uma notificação de teste no arranque

Notas

- O programa fecha graciosamente com Ctrl+C (SIGINT/SIGTERM) após concluir o ciclo corrente.
- Resposta vazia do endpoint (0 incidentes) é aceite sem erro.
- `NTFY_TOPIC` tópico para notificação (opcional)
- `NTFY_URL` servidor do ntfy (default `https://ntfy.sh`)
- `STATE_FILE` caminho do ficheiro de estado (default `last_ids.json`)
- `FOGOS_URL` endpoint principal (default API v2 de Fogos)
- `FOGOS_FALLBACK_URLS` endereços alternativos, separados por vírgula/espaço/`;`
- `FOGOS_API_KEY` token opcional para Authorization: Bearer

## Estado

O ficheiro `last_ids.json` mantém, por município normalizado, os IDs já notificados, evitando alertas repetidos entre reinícios.

## Notas

- Normalização de municípios remove acentos e espaços para equivalência, com alguns sinónimos comuns.
- Cabeçalhos "amigáveis" para evitar bloqueios de WAF/CDN.
- Binário único, leve e adequado a correr 24/7 como Task agendada ou Serviço.
