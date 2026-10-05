# Mensageria — chatbot, Telegram, gateway de WhatsApp e o notificador

Esta pasta é **só memória**: aqui não há código. O código da área vive em
quatro lugares, e esta é a primeira coisa a saber antes de mexer:

| Peça | Onde está | O que faz |
|---|---|---|
| **Chatbot** | `app/apps/chatbot/` | Assistente de **WhatsApp** (via Z-API) que entrega o contracheque a quem informa o CPF. Guarda também o "cérebro" reaproveitado pelo Telegram: sessão, validação de CPF × telefone, planilha de colaboradores, recorte do PDF da folha e Dropbox |
| **Telegram** | `app/apps/telegram/` | Bot do Telegram: autocadastro (aba `TelegramID`), o mesmo assistente de contracheque, e a porta de **envio** `/telegram/enviar` usada pelo Make e pela emissão de NFS-e |
| **Gateway de WhatsApp** | `app/apps/whatsapp_gateway/` | Fachada que **imita o Z-API** por fora e fala com a **Evolution API** por dentro, para o Make trocar só o domínio da URL. Nenhum módulo Python usa o gateway — todos falam com o Z-API direto |
| **Notificador** | `app/apps/notificador.py` | Função comum `notificar()` / `enviar_telegram()` usada pelo ERP, ponto, Análise de SPs, BaixaBradesco, ProcessarNovaSP e ValidaSP. Telegram por chamada interna; WhatsApp por Z-API direto |

> **Pegando este trabalho agora?** Leia antes o `HISTORICO.md` ao lado. Ele
> diz em que pé a área está, o que o dono quer dela, as perguntas que só ele
> responde e a análise feita em 05/10/2026 sobre juntar ou não as três
> aplicações. Aqui está **como a coisa é**; lá está **por que é assim**.

A área nasceu em 05/10/2026 pelo pedido do dono de trazer essas aplicações
para o mesmo jeito de trabalhar das outras (um chat por área, memória no
repositório). Este arquivo foi escrito **a partir do código, lido de ponta a
ponta** — tudo aqui foi conferido nele. O que depende do mundo fora do
repositório (quais variáveis estão no Render, se a Evolution foi instalada, se
o Z-API continua contratado) **não foi possível conferir** e está marcado como
pergunta no `HISTORICO.md`.

---

## O mapa em uma figura

```
                      ┌─────────────────────────┐
  Colaborador ──WA──► │  Z-API (serviço pago)   │ ──webhook──► /api/chatbot/webhook  (chatbot)
                      └─────────────────────────┘ ◄──send-text── chatbot/zapi_sender.py
                                   ▲                         ◄── notificador.py (WhatsApp)
                                   │                         ◄── validasp/zapi.py
                                   │                         ◄── baixabradesco/zapi.py
                                   │                         ◄── emissaonf/zapi.py
                                   │                         ◄── telegram_bot (aviso "cadastre-se")
                                   │
  Make.com ──────────────────────► │  (cenários do Make falam com o Z-API direto HOJE;
                                   │   o gateway existe para receber essas URLs no futuro)
                                   │
                      ┌─────────────────────────┐
  Make.com ─────────► │ /instances/<id>/token/  │ ──► Evolution API (2º serviço no Render,
                      │  <tk>/send-*  (gateway) │     instalação NÃO confirmada)
                      └─────────────────────────┘ ◄── /api/whatsapp_gateway/webhook/<inst> ◄── Evolution
                                                       └──► traduz p/ formato Z-API ──► webhook do Make

  Colaborador ──TG──► Telegram ──webhook──► /telegram/webhook (autocadastro + contracheque)
  Make / emissaonf ──HTTP──► /telegram/enviar ──► Telegram sendMessage/sendDocument
  ERP, ponto, analisesps, baixabradesco, processarnovasp, validasp ──import──► notificador.py
        notificador ──chamada interna──► funções do telegram_bot   (canal Telegram)
        notificador ──HTTP──► api.z-api.io                          (canal WhatsApp)
```

Três coisas que a figura mostra e que surpreendem quem chega:

1. **O chatbot é de WhatsApp, não de Telegram.** O que existe no Telegram é uma
   **cópia** do fluxo de contracheque, escrita dentro do `telegram_bot.py`,
   que importa os módulos do chatbot (sessão, validação, planilha, PDF, Dropbox)
   e troca só o canal de envio.
2. **O gateway está isolado.** Nenhum módulo Python chama `/instances/...`.
   Ele só serve ao Make, e só terá efeito quando a Evolution estiver no ar e os
   cenários do Make apontarem para ele. Hoje, se a Evolution não existir, o
   gateway é código parado.
3. **Existem SEIS pontos que falam com o WhatsApp**, cada um com o seu próprio
   código de envio: `chatbot/zapi_sender.py`, `notificador.py`,
   `telegram_bot._wa_aviso_cadastro`, `validasp/zapi.py`,
   `baixabradesco/zapi.py` e `emissaonf/zapi.py`. Mexer no "WhatsApp da
   empresa" é mexer em seis lugares.

---

## Rotas

Registradas no `app/main.py`. O chatbot leva prefixo `/api/chatbot`; os outros
dois já trazem o prefixo embutido e são registrados **sem** `url_prefix`.

| Método | Rota | Quem chama | Autenticação |
|---|---|---|---|
| POST | `/api/chatbot/webhook` | Z-API (mensagem recebida no WhatsApp) | `X-Webhook-Secret` ou `?secret=` = `CHATBOT_WEBHOOK_SECRET`. ⚠️ **Se a variável estiver vazia, aceita qualquer um** |
| GET | `/api/chatbot/status` | diagnóstico | nenhuma (mostra só se o cache carregou e quantas sessões há) |
| POST | `/api/chatbot/cache/invalidar` | manual | mesmo secret do webhook |
| POST | `/telegram/webhook` | Telegram (BotFather `setWebhook`) | header `X-Telegram-Bot-Api-Secret-Token` = `TELEGRAM_SECRET_TOKEN` |
| POST | `/telegram/enviar` | Make, `emissaonf` (por HTTP público) | header `X-Api-Key` = `TELEGRAM_SECRET_TOKEN`. **Falha fechado**: sem a variável, recusa tudo |
| GET | `/telegram/health` | diagnóstico | nenhuma (diz se o token está configurado) |
| POST | `/instances/<id>/token/<tk>/send-text` · `send-image` · `send-document/<ext>` · `send-audio` · `send-link` | Make (URLs iguais às do Z-API) | o par `id`/`tk` precisa estar em `WHATSAPP_GATEWAY_INSTANCES`; `Client-Token` opcional |
| POST | `/api/whatsapp_gateway/webhook/<instancia_evolution>` | Evolution API | `?secret=` = `WHATSAPP_GATEWAY_WEBHOOK_SECRET` |
| GET | `/api/whatsapp_gateway/health` | diagnóstico | nenhuma (lista os ids de instância configurados, sem tokens) |

Respostas seguem `{'ok': True/False}` — **exceto** as rotas `/instances/...`,
que devolvem o formato do Z-API (`{zaapId, messageId, id}` ou `{error}`) de
propósito, para o Make não precisar mudar nada além do domínio.

---

## Como cada peça funciona

### Chatbot (WhatsApp, via Z-API)

**Fluxo:** mensagem chega → responde na hora `200` e processa numa thread, com
um cadeado por telefone (duas mensagens da mesma pessoa não se atropelam) e
deduplicação por id da mensagem por 60 s (o Z-API reenvia).

**Estados da conversa** (em memória, 30 min de inatividade e some):

- `AGUARDANDO_CPF` → pede o CPF. Confere na planilha **Dados Documentos**
  (`1fqi4QUOVGUd1_4Gg4vK5qP_IMOSgFaw8DD9MDgmM3vo`, colunas A=CPF, E=nome,
  W=telefone, AX=status) que o CPF existe, não está em "Colaboradores
  Desligados" e que o **telefone de quem escreve bate com o da planilha**
  (tolera o nono dígito). O telefone `CHATBOT_MASTER_PHONE` (padrão
  `5585987846225`) pode pedir o contracheque de **qualquer** CPF.
  Cinco erros de CPF → bloqueio de 1 hora, **sem resposta** a cada nova
  tentativa (para não gastar cota nem entrar em eco).
- `MENU_PRINCIPAL` → só há uma opção: `1` / "contracheque".
- `AGUARDANDO_COMPETENCIA` → `MM/AAAA`. Baixa do Dropbox o PDF da folha
  (`/BWS DP/FOLHA DE PAGAMENTO/CONTRACHEQUES/<competência>.pdf`), recorta a
  página do CPF (dois contracheques por página) e manda o PDF individual.

`sair` / `cancelar` / `reiniciar` encerram a qualquer momento. A planilha fica
em cache por 30 min; `/cache/invalidar` força reler.

**Memória:** sessões, bloqueios e cache moram **no processo**. É um dos
motivos de o gunicorn rodar com `--workers 1` (ver `CLAUDE.md` › Gunicorn).

### Telegram

Um arquivo só, `telegram_bot.py` (1.300 linhas), com três papéis:

**1. Autocadastro.** `/start` → botão "Compartilhar meu número" (o Telegram
garante que é o número da própria pessoa). O número é procurado em duas bases
**só de leitura** — a aba *Dados Documentos* da planilha acima e a aba
*Colaborador (Cartões)* da planilha `1C7MWQmr5uFGWuJ18osUNDapiojVXzQ_GxMMDQqxPsBk`.
Achou → grava na aba **`TelegramID`** (criada sozinha se faltar) da segunda
planilha: data, CPF, telefone, id do chat, nome, origem, observação — **uma
linha por pessoa** (troca de conta substitui a linha). Não achou → pede o CPF;
achou por CPF → cadastra com observação de telefone divergente e pede para
confirmar o número pelo botão. Tudo que falha vai para a aba
**`Pendências Telegram`**. Opção `2` troca o número.

**2. Assistente de contracheque.** O mesmo fluxo do chatbot, sem pedir CPF —
a identidade já é a do cadastro. Reusa `chatbot.session`, `auth`,
`sheets_cache`, `paystub` e `dropbox_client`. O telefone master ganha o
estado extra `AGUARDANDO_CPF_MASTER`. A sessão é a **mesma estrutura em
memória do chatbot**, com o id do chat no lugar do telefone.

**3. Envio (`/telegram/enviar`).** Destinatário por `chat_id`, `telefone` ou
`cpf` (procura na `TelegramID`, com cache de 2 min; se o telefone mudou na
base do RH, cai para CPF). Conteúdo: `mensagem`, `arquivo_url` (o servidor
baixa, com teto de 48 MB — necessário para Drive/Dropbox), `arquivo_base64`
ou upload multipart. Aceita JSON **e** formulário (o Make quebrava JSON com
aspas). **Se a pessoa não tem cadastro no Telegram**, responde `200` com
`erro: nao_cadastrado` (para o cenário do Make não parar) e dispara **um
aviso pelo WhatsApp** (Z-API) pedindo o cadastro, no máximo um a cada 6 h por
telefone — a menos que o chamador mande `avisar_whatsapp=0`, como o
`emissaonf` faz, porque ele já entrega a mesma mensagem por WhatsApp.

Links vão **sem preview**: o robô do Telegram fazia um GET na URL para montar
o preview e isso **disparava links de ação de um clique** antes de a pessoa
clicar (incidente de 16/07/2026 — está no comentário do código, não no
`CONTEXTO.md`).

Markdown desbalanceado numa mensagem não derruba o envio: reenvia como texto
puro. Erros `429` respeitam o `retry_after`.

### Gateway de WhatsApp (Evolution API)

Decisão de 10/07/2026 (`CONTEXTO.md` §9): sair do Z-API (pago) para a
**Evolution API** (código aberto, instalada como **segundo serviço** no
Render, com um Postgres externo só dela). A estratégia foi não mexer no Make:
o gateway recebe as URLs **no mesmo formato do Z-API** e traduz.

- `config.py` lê `WHATSAPP_GATEWAY_INSTANCES` (JSON, **a cada chamada**, sem
  cache — para trocar o mapeamento editando a variável no Render):
  `id do Z-API → {token, evolution_instance, evolution_apikey, make_webhook_url}`.
- `evolution.py` chama `sendText`, `sendMedia`, `sendWhatsAppAudio` da
  Evolution v2 com `apikey` por instância.
- `webhook.py` traduz `messages.upsert` da Evolution para o `ReceivedCallback`
  do Z-API (texto, imagem, áudio, documento) e `routes.py` repassa ao
  `make_webhook_url` da instância. **Não repassa ao chatbot**: se o chatbot
  um dia for para a Evolution, isso precisa ser escrito.
- `send-link` vira texto com preview; a miniatura personalizada do Z-API não
  é replicada.

Sem `EVOLUTION_BASE_URL` e `WHATSAPP_GATEWAY_INSTANCES`, toda rota
`/instances/...` responde `500 "Gateway mal configurado"` e o `health` lista
zero instâncias — foi o que aconteceu ao subir aqui sem variáveis.

### Notificador (`app/apps/notificador.py`)

`notificar(telefone|cpf|chat_id, mensagem, arquivo_url|arquivo_base64, canais=,
politica=)`. Canais `telegram` e `whatsapp`; política `ambos` (padrão) ou
`fallback` (o primeiro que der certo encerra). `enviar_telegram(...)` é o
atalho só-Telegram. Canal Telegram é chamada **interna** às funções do
`telegram_bot` (mesmo processo); canal WhatsApp é HTTP direto ao Z-API.

Quem usa, e com que canais (conferido no código em 05/10/2026):

| Quem | Canais | Para quê |
|---|---|---|
| ERP (`core/notificacoes.py`, `agente`, `encaminhar`, `perguntas/agendadas`, `ia_custo`, `suprimentos/insumos`) | **só Telegram** (`enviar_telegram`), exceto `agente` e `encaminhar`, que chamam `notificar()` com o padrão (ambos) | título pago com comprovante, encaminhamentos, respostas agendadas, teto de IA, insumos |
| Ponto (`core/envios.py`, `acesso.py`, `rotina.py`) | **só WhatsApp** | QR Code de batida, código do PIN, rotina diária |
| Análise de SPs (`avisos_ponto.py`) | WhatsApp, Telegram de reserva | avisos de ponto |
| BaixaBradesco (`avisos.py`, `zapi.py`) | envio Z-API próprio; Telegram de espelho; notificador de reserva | comprovante ao responsável, falhas do lote |
| ProcessarNovaSP (`notify.py`) | padrão (ambos) | aviso ao master |
| ValidaSP (`zapi.py`) | envio Z-API próprio; Telegram de espelho | validação de SP |
| Emissão de NFS-e (`zapi.py`) | envio Z-API próprio; Telegram por **HTTP** a `/telegram/enviar` no domínio público | nota emitida |

---

## Variáveis de ambiente

| Variável | Quem lê | Observação |
|---|---|---|
| `ZAPI_INSTANCE_ID` | todos os que falam com o Z-API | |
| `ZAPI_API_TOKEN` | `chatbot`, `validasp`, `baixabradesco`, `emissaonf` (via aba Credenciais), docs | ⚠️ **um nome…** |
| `ZAPI_INSTANCE_TOKEN` | `notificador.py`, `telegram_bot` (aviso de cadastro), `ponto` | ⚠️ **…e outro nome para a MESMA coisa.** O que estiver faltando no Render deixa metade dos envios de WhatsApp falhando em silêncio ("whatsapp_nao_configurado"). Já estava anotado no `baixabradesco/HISTORICO.md` |
| `ZAPI_CLIENT_TOKEN` | todos | header `Client-Token` |
| `NOTIFICAR_WHATSAPP` / `NOTIFICAR_TELEGRAM` | `notificador`, `validasp`, `baixabradesco`, `emissaonf`, `ponto` | `"0"` desliga o canal. **É o "botão que desativa o WhatsApp"** de que o dono lembra. ⚠️ O chatbot (respostas no WhatsApp) e o aviso de cadastro do Telegram **não olham** esse botão |
| `CHATBOT_WEBHOOK_SECRET` | chatbot | vazio = webhook aberto |
| `CHATBOT_MASTER_PHONE` | chatbot, telegram, processarnovasp, baixabradesco | padrão `5585987846225` |
| `TELEGRAM_BOT_TOKEN`, `TELEGRAM_SECRET_TOKEN`, `TELEGRAM_BOT_LINK` | telegram, notificador | link padrão `t.me/bwsconstrucoesbotbot` |
| `TELEGRAM_ENVIAR_URL` | emissaonf | padrão `https://aplicacoes.bwsconstrucoes.com.br/telegram/enviar` |
| `EVOLUTION_BASE_URL`, `WHATSAPP_GATEWAY_INSTANCES`, `WHATSAPP_GATEWAY_WEBHOOK_SECRET`, `WHATSAPP_GATEWAY_CLIENT_TOKEN` | gateway | |
| `GOOGLE_CREDENTIALS_BASE64`, `DROPBOX_APP_KEY/SECRET/REFRESH_TOKEN` | chatbot, telegram | as mesmas do monorepo |

---

## Testes

**Não há teste automatizado de nenhuma das três aplicações.** A suíte só toca
o `notificador` para **dublá-lo** (garantir que nada sai do contêiner). O
`CONTEXTO.md` registra um smoke test manual 17/17 do gateway em 10/07/2026 —
não está versionado.

O que dá para conferir sem credenciais: `python app/main.py` sobe os 19
blueprints, e `/telegram/health`, `/api/whatsapp_gateway/health` e
`/api/chatbot/status` respondem `200` (feito em 05/10/2026).

---

## Riscos e limites conhecidos

- **Estado em memória** (sessões do chatbot/Telegram, dedup, cache da
  `TelegramID`, throttle do aviso de WhatsApp). Reinício do serviço — que
  acontece a cada ~1000 acessos — zera tudo. Uma conversa de contracheque no
  meio de um reinício volta ao começo.
- **`emissaonf` chama o próprio servidor por HTTP** para espelhar no Telegram.
  Com 1 worker e 4 threads, uma thread fica presa esperando outra, por até
  120 s. Funciona; não é bonito.
- **Chave de escopo misturada:** o `chatbot.session` normaliza a chave como
  telefone (acrescenta `55`); o Telegram passa o id do chat pela mesma
  função. Como entrada e saída passam pelo mesmo lugar, funciona — mas um id
  de chat vira "55123456789" por dentro. Não "arrume" sem olhar os dois lados.
- **`health` do gateway é público** e lista os ids de instância do Z-API (sem
  token). Baixo risco; vale saber.
- **Logs do Telegram usam `print`**, não `logging` — aparecem no Render, mas
  sem nível nem nome de módulo.
- **Planilhas e o telefone master estão no código**, não em variável. Trocar
  a planilha é publicar.
