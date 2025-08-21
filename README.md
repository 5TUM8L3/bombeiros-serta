# David-Bombeiros

Script simples em Bun que consulta os incidentes ativos (GeoJSON) de Fogos.pt e filtra apenas o concelho "Sertã". Opcionalmente envia notificação para ntfy.sh.

## Pré-requisitos

- [Bun](https://bun.sh/) instalado e no PATH

## Como executar

### CMD (Prompt de Comando)

```
cd /d E:\David-Bombeiros
bun install
bun run start
```

### PowerShell

```
Set-Location E:\David-Bombeiros
bun install
bun run start
```

## Notificação opcional (ntfy)

Para receber notificações push via [ntfy.sh](https://ntfy.sh/):

1. Escolha um tópico (ex.: `bombeiros-serta`).
2. Na app ntfy do telemóvel, subscreva o tópico (server: https://ntfy.sh).
3. Execute com o tópico:

CMD:

```
cd /d E:\David-Bombeiros
set NTFY_TOPIC=bombeiros-serta && bun run start
```

PowerShell:

```
Set-Location E:\David-Bombeiros
$env:NTFY_TOPIC = 'bombeiros-serta'
bun run start
```

Servidor personalizado (opcional):

```
set NTFY_URL=https://ntfy.sh
```

Teste rápido de notificação (sem consultar API):

CMD:

```
cd /d E:\David-Bombeiros
set NTFY_TOPIC=bombeiros-serta && set NTFY_TEST=1 && bun run start
```

PowerShell:

```
Set-Location E:\David-Bombeiros
$env:NTFY_TOPIC = 'bombeiros-serta'
$env:NTFY_TEST = '1'
bun run start
```

## Saída

O script imprime um JSON com:

- `count`: número de ocorrências em Sertã
- `features`: array GeoJSON de ocorrências filtradas

## Vários municípios

Pode monitorizar vários municípios de uma só vez. Defina a variável `MUNICIPIOS` como lista separada por vírgulas ou ponto-e-vírgula, por exemplo:

PowerShell:

```
Set-Location E:\David-Bombeiros
$env:MUNICIPIOS = 'Sertã,Oleiros,Castanheira de Pera,Proença-a-Nova,Vila de Rei,Vila Velha de Ródão,Sardoal,Figueiró dos Vinhos,Pampilhosa da Serra'
$env:NTFY_TOPIC = 'bombeiros-serta'
bun run start
```

CMD:

```
cd /d E:\David-Bombeiros
set MUNICIPIOS=Sertã,Oleiros,Castanheira de Pera,Proença-a-Nova,Vila de Rei,Vila Velha de Ródão,Sardoal,Figueiró dos Vinhos,Pampilhosa da Serra
set NTFY_TOPIC=bombeiros-serta && bun run start
```

Se `MUNICIPIOS` não for definido, o padrão já inclui estes municípios.

## Polling contínuo

Para monitorizar automaticamente (por omissão a cada 30s) e enviar alerta quando houver alterações:

CMD:

```
cd /d E:\David-Bombeiros
set NTFY_TOPIC=bombeiros-serta && set POLL_SECONDS=60 && bun run start
```

PowerShell:

```
Set-Location E:\David-Bombeiros
$env:NTFY_TOPIC = 'bombeiros-serta'
$env:POLL_SECONDS = '60'
bun run start
```

Ou use o script pronto `monitor.ps1` (mantém estado entre reinícios):

```
Set-Location E:\David-Bombeiros
./monitor.ps1 -Municipio 'Sertã' -PollSeconds 30 -Topic 'bombeiros-serta'  # (opcional: usa 1 município)

Ou, no CMD, use `monitor.bat` (também com 30s por omissão):

```

cd /d E:\David-Bombeiros
monitor.bat "Sertã,Oleiros,Castanheira de Pera,Proença-a-Nova,Vila de Rei,Vila Velha de Ródão,Sardoal,Figueiró dos Vinhos,Pampilhosa da Serra" 30 bombeiros-serta

```

```

Para manter sempre ativo após reiniciar o Windows, use o Agendador de Tarefas:

1. Abra o Agendador de Tarefas > Criar Tarefa.
2. Em Disparadores, "Ao iniciar sessão" ou "Ao iniciar".
3. Em Ações, pode escolher uma destas opções:
   - Usando PowerShell:
     - Programa/script: `powershell.exe`
     - Argumentos: `-ExecutionPolicy Bypass -NoProfile -File E:\David-Bombeiros\monitor.ps1 -Municipio 'Sertã' -PollSeconds 30 -Topic 'bombeiros-serta'`
     - Iniciar em: `E:\David-Bombeiros`
   - Usando BAT (CMD):
     - Programa/script: `E:\David-Bombeiros\monitor.bat`
     - Iniciar em: `E:\David-Bombeiros`
4. Marque "Executar com privilégios mais elevados" se necessário e guarde.

PowerShell:

```
Set-Location E:\David-Bombeiros
$env:NTFY_TOPIC = 'bombeiros-serta'
$env:POLL_SECONDS = '60'
bun run start
```
