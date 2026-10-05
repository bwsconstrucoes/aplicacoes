# Mensageria — chatbot, Telegram e o notificador

Esta pasta é **só memória**: aqui não há código. O código da área vive em
três lugares, e esta é a primeira coisa a saber antes de mexer:

| Peça | Onde está | O que faz |
|---|---|---|
| **Chatbot** | `app/apps/chatbot/` | Assistente de **WhatsApp** (via Z-API) que entrega o contracheque a quem informa o CPF. Guarda também o "cérebro" reaproveitado pelo Telegram: sessão, validação de CPF × telefone, planilha de colaboradores, recorte do PDF da folha e Dropbox |
| **Telegram** | `app/apps/telegram/` | Bot do Telegram: autocadastro (aba `TelegramID`), o mesmo assistente de contracheque, e a porta de **envio** `/telegram/enviar` usada pelo Make e pela emissão de NFS-e |
| **Notificador** | `app/apps/notificador.py` | Função comum `notificar()` / `enviar_telegram()` / `enviar_whatsapp()` usada pelo ERP, ponto, Análise de SPs, BaixaBradesco, ProcessarNovaSP, ValidaSP e pelo bot do Telegram. Telegram por chamada interna; WhatsApp por Z-API. **É onde mora o liga/desliga por canal e por finalidade** |

(Havia uma quarta peça, o `whatsapp_gateway/` — fachada para a Evolution API, uma alternativa ao Z-API que ficou só no plano. Removida em 05/10/2026; ver `HISTORICO.md`.)

> **Pegando este trabalho agora?** Leia antes o `HISTORICO.md` ao lado. Ele
> diz em que pé a área está, o que o dono quer dela, o que falta ele fazer no
> Render e a análise feita em 05/10/2026 sobre juntar ou não as aplicações. Aqui está **como a coisa é**; lá está **por que é assim**.

A área nasceu em 05/10/2026 pelo pedido do dono de trazer essas aplicações
para o mesmo jeito de trabalhar das outras (um chat por área, memória no
repositório). Este arquivo foi escrito **a partir do código, lido de ponta a
ponta** — tudo aqui foi conferido nele. O que depende do mundo fora do
repositório foi respondido pelo dono no mesmo dia e está no `HISTORICO.md`:
Z-API ativo, `NOTIFICAR_WHATSAPP=0` em produção, Evolution nunca instalada.

---

## O mapa em uma figura

```
  Colaborador ──WA──► Z-API ──webhook──► /api/chatbot/webhook  (chatbot: contracheque)
                        ▲ ◄── chatbot/zapi_sender.py          (respostas do chatbot)
                        │ ◄── notificador.py                  (ERP, ponto, analisesps, processarnovasp,
                        │                                       telegram "cadastre-se")
                        │ ◄── validasp/zapi.py, baixabradesco/zapi.py, emissaonf/zapi.py  (envio próprio)
  Make.com ─────────────┘  (poucos cenários; falam com o Z-API direto, fora deste repositório)

  Colaborador ──TG──► Telegram ──webhook──► /telegram/webhook (autocadastro + contracheque)
  Make / emissaonf ──HTTP──► /telegram/enviar ──► Telegram sendMessage/sendDocument
  ERP, ponto, analisesps, baixabradesco, processarnovasp, validasp ──import──► notificador.py
        notificador ──chamada interna──► funções do telegram_bot   (canal Telegram)
        notificador ──HTTP──► api.z-api.io                          (canal WhatsApp)
        notificador decide: NOTIFICAR_<CANAL>_<FINALIDADE> › NOTIFICAR_<CANAL>
```

Três coisas que a figura mostra e que surpreendem quem chega:

1. **O chatbot é de WhatsApp, não de Telegram.** O que existe no Telegram é uma
   **cópia** do fluxo de contracheque, escrita dentro do `telegram_bot.py`,
   que importa os módulos do chatbot (sessão, validação, planilha, PDF, Dropbox)
   e troca só o canal de envio.
2. **O Z-API bloqueia quando o volume sobe.** Foi por isso que os avisos
   migraram para o Telegram e `NOTIFICAR_WHATSAPP` está em `0` em produção.
   WhatsApp é para o que precisa chegar a quem não tem Telegram, com ritmo
   (a fila do ponto já nasce com ritmo: 30–90 s entre mensagens, 40/hora,
   200/dia).
3. **Ainda existem CINCO pontos que falam com o WhatsApp**: `notificador.py`
   (o certo, e o único com liga/desliga por finalidade), `chatbot/zapi_sender.py`,
   `validasp/zapi.py`, `baixabradesco/zapi.py` e `emissaonf/zapi.py`. Os
   quatro últimos têm envio próprio; unificá-los no notificador é o próximo
   passo da área.

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

Respostas seguem `{'ok': True/False}`.

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

### Notificador (`app/apps/notificador.py`)

`notificar(telefone|cpf|chat_id, mensagem, arquivo_url|arquivo_base64, canais=,
politica=, finalidade=)`. Canais `telegram` e `whatsapp`; política `ambos`
(padrão) ou `fallback` (o primeiro que der certo encerra). `enviar_telegram(...)`
e `enviar_whatsapp(...)` são os atalhos de um canal só. Canal Telegram é chamada
**interna** às funções do `telegram_bot` (mesmo processo); canal WhatsApp é HTTP
ao Z-API, com as credenciais lidas **a cada chamada** (trocar no Render vale na
hora) e o token aceito pelos dois nomes (`ZAPI_API_TOKEN`, que é o do Render, ou
`ZAPI_INSTANCE_TOKEN`).

**O liga/desliga (desde 05/10/2026):** `NOTIFICAR_WHATSAPP` e
`NOTIFICAR_TELEGRAM` são o padrão de tudo. Quando o chamador diz a
**finalidade** (`finalidade="ponto"`), a variável
`NOTIFICAR_<CANAL>_<FINALIDADE>` (ex.: `NOTIFICAR_WHATSAPP_PONTO`) **manda
sozinha** para aquele uso, ligando ou desligando; se não existir ou estiver
vazia, vale a geral. Finalidades em uso: `ponto` (QR, PIN e resumo do dia) e
`aviso_cadastro_telegram` (o aviso por WhatsApp a quem ainda não tem Telegram).
Uso novo ganha a sua finalidade e uma linha nesta lista. Teste:
`tests/test_notificador.py`.

Quem usa, e com que canais (conferido no código em 05/10/2026):

| Quem | Canais | Para quê |
|---|---|---|
| ERP (`core/notificacoes.py`, `agente`, `encaminhar`, `perguntas/agendadas`, `ia_custo`, `suprimentos/insumos`) | **só Telegram** (`enviar_telegram`), exceto `agente` e `encaminhar`, que chamam `notificar()` com o padrão (ambos) | título pago com comprovante, encaminhamentos, respostas agendadas, teto de IA, insumos |
| Ponto (`core/envios.py`, `acesso.py`, `rotina.py`) | **só WhatsApp**, finalidade `ponto` | QR Code de batida, código do PIN, resumo do dia. A fila só liga se `whatsapp_pronto()` — credenciais presentes e finalidade ligada |
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
| `ZAPI_API_TOKEN` | todos (o notificador aceita também o apelido `ZAPI_INSTANCE_TOKEN`, que foi o nome lido por ele, pelo ponto e pelo aviso do Telegram até 05/10/2026 — e que **não existe no Render**) | é o nome que existe no Render |
| `ZAPI_CLIENT_TOKEN` | todos | header `Client-Token` |
| `NOTIFICAR_WHATSAPP` / `NOTIFICAR_TELEGRAM` | `notificador` (e por ele ERP, ponto, analisesps, processarnovasp, telegram), `validasp`, `baixabradesco`, `emissaonf` | `"0"` desliga o canal. **`NOTIFICAR_WHATSAPP=0` em produção.** As respostas do chatbot no WhatsApp **não olham** esse botão (quem escreve recebe resposta) |
| `NOTIFICAR_WHATSAPP_PONTO`, `NOTIFICAR_WHATSAPP_AVISO_CADASTRO_TELEGRAM` (e qualquer `NOTIFICAR_<CANAL>_<FINALIDADE>`) | `notificador` | manda sobre a geral para aquela finalidade. **Para o ponto mandar WhatsApp com a geral em `0`: `NOTIFICAR_WHATSAPP_PONTO=1`** |
| `CHATBOT_WEBHOOK_SECRET` | chatbot | vazio = webhook aberto |
| `CHATBOT_MASTER_PHONE` | chatbot, telegram, processarnovasp, baixabradesco | padrão `5585987846225` |
| `TELEGRAM_BOT_TOKEN`, `TELEGRAM_SECRET_TOKEN`, `TELEGRAM_BOT_LINK` | telegram, notificador | link padrão `t.me/bwsconstrucoesbotbot` |
| `TELEGRAM_ENVIAR_URL` | emissaonf | padrão `https://aplicacoes.bwsconstrucoes.com.br/telegram/enviar` |
| `GOOGLE_CREDENTIALS_BASE64`, `DROPBOX_APP_KEY/SECRET/REFRESH_TOKEN` | chatbot, telegram | as mesmas do monorepo |

---

## Testes

`tests/test_notificador.py` (desde 05/10/2026): credenciais pelos dois nomes,
liga/desliga por finalidade nos dois sentidos, o ponto seguindo o notificador e
o aviso do Telegram calando com o botão desligado. O POST ao Z-API é dublado.
**O chatbot e o bot do Telegram em si continuam sem teste** — a conversa, o
cadastro e o `/telegram/enviar` só são exercitados em produção.

O que dá para conferir sem credenciais: `python app/main.py` sobe os 18
blueprints, e `/telegram/health` e `/api/chatbot/status` respondem `200`.

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
- **Logs do Telegram usam `print`**, não `logging` — aparecem no Render, mas
  sem nível nem nome de módulo.
- **Planilhas e o telefone master estão no código**, não em variável. Trocar
  a planilha é publicar.
