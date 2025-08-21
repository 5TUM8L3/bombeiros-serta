@echo off
chcp 65001 >nul
setlocal enabledelayedexpansion
pushd %~dp0

REM Usage: monitor.bat [Municipios] [PollSeconds] [Topic] [NtfyUrl] [StateFile] [FogosApiKey]
REM  - MUNICIPIOS pode ser uma lista separada por virgula ou ponto-e-virgula
REM Defaults: MUNICIPIOS="Sertã,Oleiros,Castanheira de Pera,Proença-a-Nova,Vila de Rei,Vila Velha de Ródão,Sardoal,Figueiró dos Vinhos,Pedrógão Grande,Pampilhosa da Serra,Ferreira do Zêzere,Fundão,Castelo Branco,Idanha-a-Nova,Penamacor,Belmonte,Covilhã"
REM           PollSeconds=30, Topic=bombeiros-serta, NtfyUrl=https://ntfy.sh, StateFile=last_ids.json
REM           FogosApiKey=(opcional)

set "MUNICIPIOS=%~1"
REM Se vazio, nao definir; o default é aplicado no código (evita problemas de acentos no .bat)
if "%MUNICIPIOS%"=="" set "MUNICIPIOS="

set "POLL_SECONDS=%~2"
if "%POLL_SECONDS%"=="" set "POLL_SECONDS=30"

set "NTFY_TOPIC=%~3"
if "%NTFY_TOPIC%"=="" set "NTFY_TOPIC=bombeiros-serta"

set "NTFY_URL=%~4"
if "%NTFY_URL%"=="" set "NTFY_URL=https://ntfy.sh"

set "STATE_FILE=%~5"
if "%STATE_FILE%"=="" set "STATE_FILE=last_ids.json"

set "FOGOS_API_KEY=%~6"
if "%FOGOS_API_KEY%"=="" set "FOGOS_API_KEY="

where bun >nul 2>nul
if errorlevel 1 (
  echo ERRO: Bun nao encontrado no PATH.
  echo Instale o Bun em https://bun.sh e tente novamente.
  popd
  exit /b 1
)

if "%MUNICIPIOS%"=="" (
  echo Iniciando monitor com municipios padrao a cada %POLL_SECONDS%s (topico: %NTFY_TOPIC%)
 ) else (
  echo Iniciando monitor para "%MUNICIPIOS%" a cada %POLL_SECONDS%s (topico: %NTFY_TOPIC%)
 )

REM Environment vars for the Bun process
if not "%MUNICIPIOS%"=="" set "MUNICIPIOS=%MUNICIPIOS%"
set "POLL_SECONDS=%POLL_SECONDS%"
set "NTFY_TOPIC=%NTFY_TOPIC%"
set "NTFY_URL=%NTFY_URL%"
set "STATE_FILE=%STATE_FILE%"
if not "%FOGOS_API_KEY%"=="" set "FOGOS_API_KEY=%FOGOS_API_KEY%"

bun run start
set EXITCODE=%ERRORLEVEL%
popd
exit /b %EXITCODE%
