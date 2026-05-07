# sync_logs — Sincronização Log Base × Análises de Pagamentos

Módulo blueprint do app Flask Render que substitui IMPORTRANGE da aba `Log` das
Análises de Pagamentos por sync via Render.

## Visão geral

```
LOG BASE (Registros de SP, 1lrP1...)
  ├── A:AE: dados
  ├── AF:   ARRAYFORMULA do usuário (intacta)
  └── AG:   "Atualizado em" — timestamp ISO 8601 (NOVO)
        ↑
        │ scripts/Make/Flask preenchem AG ao escrever
        │
   Render (este módulo, /api/sync_logs)
   ├── /incremental  — a cada 30s: linhas com AG > last_sync
   └── /reset        — 3h da manhã + botão de menu: snapshot completo
        │
        ▼
   ANÁLISES DE PAGAMENTOS (1em1Q..., +4 outras)
        ├── A:AE: dados (escritos pelo Render)
        └── AF+:  fórmulas do usuário (intactas)
```

## Estrutura do pacote

```
app/apps/sync_logs/         ← copiar pra dentro do projeto
├── __init__.py
├── config.py               ← constantes (IDs, ranges, fuso)
├── utils.py                ← cliente gspread, helpers de data, chave A+G
├── core.py                 ← lógica de incremental e reset
└── routes.py               ← rotas Flask

util_carimbo.gs             ← cole no Apps Script de QUEM ESCREVE na Log Base
SyncLog.gs                  ← cole no Apps Script de cada Análise (botão menu)
patch_main_py.md            ← 2 linhas pra adicionar em app/main.py
README.md                   ← este arquivo
```

## Variáveis de ambiente novas no Render

| Variável | Descrição | Exemplo |
|---|---|---|
| `SYNC_LOGS_SECRET` | Secret pro auth das rotas (padrão dos outros módulos) | `openssl rand -hex 32` |
| `SYNC_LOGS_ANALISES_IDS` | JSON array dos IDs das Análises | `["1em1QlCKx1Mele..."]` |

> `GOOGLE_CREDENTIALS_BASE64` já existe e é reutilizada (igual aos outros módulos).

---

## Implantação — passo a passo

### Fase 1: Adicionar coluna AG na Log Base (5 min, uma vez)

1. Abra a Log Base (`1lrP1HOvwqyXi...`).
2. Confirme que a aba é `Log`.
3. Na célula **AG1**, escreva `Atualizado em`.
4. Pronto. Vai ser preenchida pelos scripts ao longo do tempo.

### Fase 2: Adicionar o módulo sync_logs ao projeto Flask

1. Copie a pasta `app/apps/sync_logs/` para o seu monorepo (mantém os 4 arquivos:
   `__init__.py`, `config.py`, `utils.py`, `core.py`, `routes.py`).
2. Aplique o patch em `app/main.py` (ver `patch_main_py.md` — 2 linhas).
3. Configure as 2 envvars novas no Render:
   - `SYNC_LOGS_SECRET` — gere com `openssl rand -hex 32`.
   - `SYNC_LOGS_ANALISES_IDS` — comece com `["1em1QlCKx1MeleAUqUi3hbpH2Z69p2wyVlNlHMg76-N0"]`.
4. Confirme que a service account já tem permissão de **edição** na Log Base e
   na Análise (ela já deve ter, dado que `atualizaspbotao` funciona).
5. Faça o deploy.
6. Teste rápido (curl):
   ```bash
   curl -X POST https://SEU-APP.onrender.com/api/sync_logs/incremental \
        -H "Content-Type: application/json" \
        -d '{"secret":"SEU_SECRET"}'
   ```
   Deve retornar `{"ok": true, "linhas_alteradas": 0, ...}` (zero porque
   ninguém escreveu AG ainda).

### Fase 3: Configurar cron-job.org (gratuito)

Em [cron-job.org](https://cron-job.org), crie 2 jobs:

**Job 1 — Incremental (a cada 30s)**
- URL: `https://SEU-APP.onrender.com/api/sync_logs/incremental`
- Method: POST
- Schedule: a cada 30s (ou 1 min se o plano free não permitir 30s)
- Headers: `Content-Type: application/json`
- Body: `{"secret":"SEU_SYNC_LOGS_SECRET"}`
- Notifications: On failure

**Job 2 — Reset noturno (3h da manhã)**
- URL: `https://SEU-APP.onrender.com/api/sync_logs/reset`
- Method: POST
- Schedule: `0 3 * * *`
- Headers: `Content-Type: application/json`
- Body: `{"secret":"SEU_SYNC_LOGS_SECRET"}`  (sem `analise_id` = todas)
- Notifications: On failure

> ⚠️ **Não ative os crons ainda.** Continue a implantação e ative no final.

### Fase 4: Botão "Atualizar Log" na sua Análise (testes)

1. Abra a Análise (`1em1Q...`) → Extensões → Apps Script.
2. Crie arquivo `SyncLog.gs` e cole o conteúdo.
3. Engrenagem → Propriedades do script → adicione:
   - `SYNC_LOGS_URL` = `https://SEU-APP.onrender.com/api/sync_logs/reset`
   - `SYNC_LOGS_SECRET` = mesmo valor configurado no Render
4. No `onOpen` da planilha, adicione no menu:
   ```js
   .addItem('🔄 Atualizar Log', 'atualizarLogDoRender')
   ```
5. Salve, recarregue a planilha.

### Fase 5: Migração inicial da sua Análise

**ANTES de ligar os crons**, popule a Análise pela primeira vez:

1. Na sua Análise, aba `Log`:
   - Selecione `A2:AE` até a última linha.
   - Apague tudo (`Delete`) — isso remove todos os IMPORTRANGE.
   - Não toque na linha 1 (cabeçalho) nem em colunas AF+.
2. Confirme via `Ctrl+H` → marca "Pesquisar dentro de fórmulas" → procura
   `IMPORTRANGE` em A:AE: deve dar zero ocorrências.
3. Clique no botão **🔄 Atualizar Log** do menu.
4. Em ~10s deve aparecer toast `✅ Log atualizada: NNNNN linhas (2025/2026)`.
5. Confira aba Log: dados a partir da linha 2, sem fórmulas em A:AE,
   colunas AF+ continuam funcionando normalmente.

### Fase 6: Adaptar pontos de escrita pra preencher AG

Pontos identificados que escrevem na aba `Log` da Log Base e precisam preencher
AG:

| Origem | Como adaptar |
|---|---|
| `AlteraStatus.gs` (na Análise) | Após escrever, chame `carimbarLinha(abaLog, N)` ou inclua AG no array do setValues |
| Outros scripts da Análise (`AppWeb`, `BeeValeLinhaErro`, `CopiaColunaF`) | Idem — usar `util_carimbo.gs` |
| `atualizaspbotao` (Flask) | Adicionar AG no payload escrito pra Log |
| Make.com | Mapping da coluna AG: `{{formatDate(now; "YYYY-MM-DD[T]HH:mm:ss"; "America/Fortaleza")}}` |

⚠️ **Pontos NÃO adaptados não são fatal.** A linha que não tem AG preenchido é
ignorada pelo sync incremental, mas aparece na Análise no reset noturno (ou
quando alguém clicar no botão). Adapte o que conseguir; o resto eventualmente
sincroniza.

> **Util_carimbo.gs:** copie pra cada projeto Apps Script que escreve na Log
> Base. Centraliza a função `carimboAgora()` e os helpers.

### Fase 7: Ligar os crons

Volte ao cron-job.org e **ative** os 2 jobs.

A partir desse momento:
- Toda escrita na Log Base com AG preenchido propaga pra Análise em ≤30s.
- Às 3h da manhã, Análise é reescrita do zero (rede de proteção).
- Botão "🔄 Atualizar Log" disponível pra forçar reset manualmente.

### Fase 8: Adicionar as outras 4 Análises (depois)

Quando estiver confiante:

1. Adicione os IDs em `SYNC_LOGS_ANALISES_IDS`:
   ```json
   ["1em1QlCKx1Mele...","ID2","ID3","ID4","ID5"]
   ```
2. Em cada uma das outras 4: cole `SyncLog.gs`, configure Properties, adicione
   item no menu, faça migração inicial (Fase 5).
3. Próximo sync incremental e reset noturno já incluem as 5.

---

## Decisões arquiteturais (resumo)

- **Chave única lógica:** `A + "||" + G` (mesma chave do `limparLog` noturno do
  usuário).
- **Detecção de mudança:** coluna AG (timestamp ISO) > last_sync.
- **last_sync persistido:** `/tmp/sync_logs_last_sync.txt` (Render pago não zera
  entre requests).
- **Filtro de ano no reset:** linhas com coluna B (data) em ano corrente +
  anterior.
- **Estratégia de escrita no incremental:** updates por linha (batch_update
  agrupando) + appends pra linhas novas.
- **Estratégia do reset:** `batch_clear` em A2:AE até o fim + 1 update.

---

## Troubleshooting

| Sintoma | Causa provável | Solução |
|---|---|---|
| Botão → "SYNC_LOGS_URL não configurado" | Properties do script vazias | Configurar conforme Fase 4 |
| Toast: "Render respondeu 400 / Secret inválido" | `SYNC_LOGS_SECRET` divergente | Conferir env do Render vs Properties do Apps Script |
| Toast: "Render respondeu 500 / SYNC_LOGS_ANALISES_IDS" | envvar não configurada | Adicionar envvar no Render |
| Sync incremental sempre 0 linhas | Scripts não estão preenchendo AG | Conferir patch dos scripts |
| Reset traz 0 linhas | Coluna B (data) sem formato esperado | Conferir valores de B na Log Base |
| Erro 403 da Sheets API | Service account sem edição em alguma planilha | Compartilhar com email da SA |

---

## Rollback

Se algo der MUITO errado:

1. Desative os 2 crons no cron-job.org.
2. Re-adicione os IMPORTRANGE na aba Log da Análise.
3. Sistema volta ao estado anterior.

> Antes da Fase 5, faça backup dos IMPORTRANGE atuais (cópia da fórmula em txt
> ou print). Não custa nada e te salva se precisar.
