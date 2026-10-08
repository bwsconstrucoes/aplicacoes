# Mensageria — por onde cada mensagem sai, com registro

Área do monorepo desde 05/10/2026. Cobre **quem decide por onde um aviso sai**
(Telegram ou WhatsApp), **quem manda** e **o que foi mandado**. O código vive
em quatro lugares, e esta é a primeira coisa a saber antes de mexer:

| Peça | Onde está | O que faz |
|---|---|---|
| **Mensageria** (esta pasta) | `app/apps/mensageria/` | `core.py`: o catálogo de tipos de mensagem, a política por tipo, a chave geral e o teto do WhatsApp, o registro de envios. `gestao.py`: a tela **ERP › Mensagens**, pendurada no blueprint do ERP como o ponto. Schema `mensageria` no banco do ERP (migração **083** do ERP) |
| **Notificador** | `app/apps/notificador.py` | Quem **manda**: `notificar()`, `enviar_telegram()`, `enviar_whatsapp()`. Pergunta à mensageria por onde sair, manda, registra. Usado pelo ERP, ponto, Análise de SPs, BaixaBradesco, ProcessarNovaSP, ValidaSP e pelo bot do Telegram |
| **Telegram** | `app/apps/telegram/` | O bot: autocadastro (aba `TelegramID` em planilha), assistente de contracheque, e a porta de envio `/telegram/enviar` usada pelo Make e pela emissão de NFS-e. O notificador chama as funções dele por dentro. Desde 08/10/2026 também recebe, de quem ligou a conversa a um usuário do Análise de SPs, as mensagens de pedido de pagamento e põe as SPs no lote dele (`analisesps/telegram_lote.py`) |
| **Chatbot** | `app/apps/chatbot/` | Assistente de **WhatsApp** (Z-API) que entrega o contracheque a quem informa o CPF. Guarda o "cérebro" reaproveitado pelo Telegram (sessão, validação, planilha, PDF, Dropbox). Não passa pela mensageria |

> **Pegando este trabalho agora?** Leia antes o `HISTORICO.md` ao lado: estado,
> decisões do dono, o que falta ele fazer e o que ficou de fora. Aqui está
> **como a coisa é**; lá está **por que é assim**.

**O que o dono decidiu, e que molda tudo (05/10/2026):** o Z-API (WhatsApp)
está ativo, mas **bloqueia quando o volume sobe** — por isso os avisos
migraram para o Telegram. **Prioridade é Telegram; por enquanto só o ponto
usa WhatsApp.** O Análise de SPs, o ValidaSP, a baixa Bradesco e o Make vão
sumir; tudo se concentra no ERP. A Evolution (alternativa ao Z-API) nunca foi
instalada e foi removida.

---

## O mapa

```
  quem pede o aviso                    a decisão                      quem manda
  ─────────────────                    ─────────                      ──────────
  ERP (título pago, encaminhamento,
      agente, perguntas, teto IA,      notificador.notificar(         Telegram: funções do telegram_bot
      suprimentos)         ──────►        finalidade="erp.x") ──────►   (mesmo processo; chat_id pela
  Ponto (QR, PIN, mosaico,                   │                           aba TelegramID da planilha)
      aviso de foto, resumo) ─────►          ▼                         WhatsApp: HTTP ao Z-API
  Telegram (convite p/ cadastro) ──►  mensageria.core.decidir(tipo)
                                        política do tipo (tela)
                                        × chave geral do WhatsApp
                                        × teto por hora/dia      ──────► mensageria.envios (registro, 90 dias)
  Módulos antigos (sem tipo):
      analisesps, baixabradesco,
      processarnovasp, validasp ──►  sem finalidade: valem as variáveis NOTIFICAR_*; o envio
                                     ainda assim fica no registro como "sem_tipo"
```

**Duas camadas de liga/desliga, e a ordem importa:**

1. **A tela ERP › Mensagens** (mensageria): quando o chamador diz o **tipo**
   (`finalidade="ponto.qr"`) e as tabelas existem, é a política da tela que
   decide os canais, mais a chave geral "WhatsApp ligado" e o teto. Os
   `canais=` que o chamador pediu são ignorados.
2. **As variáveis de ambiente** (`NOTIFICAR_WHATSAPP`, `NOTIFICAR_TELEGRAM`,
   `NOTIFICAR_<CANAL>_<TIPO>`): valem quando a mensageria não alcança — sem
   tipo, sem banco, antes da migração 083. Rede de segurança, não gestão.

---

## A tela ERP › Mensagens

Duas abas. Quem vê: `ver_mensagens` (Administrador, Diretor financeiro,
Departamento pessoal). Quem mexe: `configurar_mensagens` (Administrador).
Seção `adm_mensagens` no cadastro de perfis.

**Tipos e canais**

- **WhatsApp** — a chave geral (nasce **desligada**; ligar é um clique do
  dono, com confirmação), o teto por hora e por dia (padrão 40 e 200), e os
  números de hoje. Se faltar credencial do Z-API ou o token do Telegram no
  servidor, a tela diz.
- **Tipos de mensagem** — uma linha por tipo, agrupada por módulo, com a
  política: **Só Telegram** · **Telegram; WhatsApp para quem não tem
  Telegram** · **Só WhatsApp** · **Desligado**. Tipo novo que o código
  declarar aparece sozinho, nascendo "Só Telegram".
- **Mandar uma mensagem de teste** — telefone e canal; passa pela chave geral
  e pelo teto; fica no registro como `mensageria.teste`.

**Enviadas** — o registro dos últimos 90 dias, com filtros por período, canal,
situação (enviada, falhou, sem destinatário, tipo desligado, teto), tipo e
busca por telefone, CPF ou trecho do texto.

Antes da migração 083 as duas abas abrem e explicam o que falta, em vez de
quebrar.

### Rotas

| Método | Rota | Ação |
|---|---|---|
| GET | `/erp/mensagens` · `/erp/mensagens/enviadas` | `ver_mensagens` |
| GET | `/erp/api/mensagens/tipos` · `/erp/api/mensagens/enviadas` | `ver_mensagens` |
| POST | `/erp/api/mensagens/politica` `{chave, politica}` | `configurar_mensagens` |
| POST | `/erp/api/mensagens/parametros` `{whatsapp_ligado, por_hora, por_dia}` | `configurar_mensagens` |
| POST | `/erp/api/mensagens/teste` `{telefone, canal}` | `configurar_mensagens` |

---

## O catálogo (tipos que o código declara hoje)

| Chave | Nome | Nasce |
|---|---|---|
| `ponto.qr` · `ponto.codigo_acesso` · `ponto.mosaico` · `ponto.aviso_foto` · `ponto.resumo_dia` | QR Code, código do Meu ponto, mosaico, aviso sem foto, resumo do dia | **Só WhatsApp** |
| `erp.titulo_pago` · `erp.encaminhamento` · `erp.agente_cobranca` · `erp.pergunta_agendada` · `erp.teto_ia` · `erp.insumos` | os avisos do ERP | Só Telegram |
| `mensageria.limite` · `mensageria.teste` | aviso de teto aos ADMIN; teste da tela | Telegram; teste: Telegram ou WhatsApp |
| `telegram.aviso_cadastro` | convite por WhatsApp a quem não tem Telegram | Desligado |

Tipo novo: uma linha em `core.CATALOGO` (chave, nome em português, módulo,
descrição, política de nascimento) e `finalidade="<chave>"` na chamada ao
notificador. Nada mais.

**O teto.** Conta só WhatsApp **enviado**, na última hora e desde a meia-noite
de Fortaleza. Estourou: a mensagem não sai, fica no registro como `LIMITE`, e
os ADMIN do ERP (com telefone ou CPF) recebem **um** aviso por Telegram por
hora dizendo qual tipo mais consumiu. A fila do ponto tenta de novo depois
(10 min × tentativas, como já fazia).

---

## Como o ponto usa

A fila do ponto (`ponto/core/envios.py`) só liga se `whatsapp_pronto()`:
credenciais do Z-API presentes **e** a mensageria liberando `ponto.qr` por
WhatsApp (política + chave geral). Cada item da fila diz o seu tipo
(`ponto.qr`, `ponto.mosaico`, `ponto.aviso_foto`); o código do PIN é
`ponto.codigo_acesso`; o resumo do dia, `ponto.resumo_dia`. O ritmo (30–90 s
entre mensagens, 40/h, 200/dia, seg.–sáb. 7h30–17h30) continua sendo do ponto;
o teto da mensageria é a segunda cerca, para a empresa inteira.

---

## Variáveis de ambiente

| Variável | Para quê |
|---|---|
| `ZAPI_INSTANCE_ID`, `ZAPI_API_TOKEN`, `ZAPI_CLIENT_TOKEN` | o Z-API. `ZAPI_INSTANCE_TOKEN` é aceito como apelido do token (nome que o notificador, o ponto e o aviso do Telegram liam até 05/10/2026 e que **não existe no Render**) |
| `TELEGRAM_BOT_TOKEN`, `TELEGRAM_SECRET_TOKEN`, `TELEGRAM_BOT_LINK` | o bot; o link padrão é `t.me/bwsconstrucoesbotbot` |
| `NOTIFICAR_WHATSAPP`, `NOTIFICAR_TELEGRAM`, `NOTIFICAR_<CANAL>_<TIPO>` | a camada 2 (só sem mensageria). **Em produção `NOTIFICAR_WHATSAPP=0`.** O nome do tipo vira maiúsculo com `_`: `ponto.qr` → `NOTIFICAR_WHATSAPP_PONTO_QR` |
| `CHATBOT_WEBHOOK_SECRET`, `CHATBOT_MASTER_PHONE` | o chatbot (vazio = webhook aberto; master padrão `5585987846225`) |
| `TELEGRAM_ENVIAR_URL` | a emissão de NFS-e chama `/telegram/enviar` por HTTP público |
| `GOOGLE_CREDENTIALS_BASE64`, `DROPBOX_*` | chatbot e Telegram (planilhas e a folha em PDF) |

---

## O bot do Telegram e o chatbot, em resumo

**Telegram.** `/start` → botão "Compartilhar meu número" → procura o número em
duas abas de planilha só de leitura (*Dados Documentos* e *Colaborador
(Cartões)*) → grava na aba **`TelegramID`** (uma linha por pessoa). Não achou
→ pede CPF. Falhas vão para a aba *Pendências Telegram*. Depois de cadastrado,
`1`/"contracheque" abre o assistente (reusa os módulos do chatbot).
`/telegram/enviar` manda texto ou arquivo por `chat_id`, `telefone` ou `cpf`
(procura na `TelegramID`, cache de 2 min); quem não tem cadastro gera o tipo
`telegram.aviso_cadastro` (hoje desligado). Links vão **sem preview** — o
robô do Telegram fazia GET na URL e disparava links de ação (16/07/2026).

**Chatbot (WhatsApp).** Z-API → `/api/chatbot/webhook` → pede CPF, confere
telefone × planilha (tolera o nono dígito), bloqueia 1 h depois de 5 erros,
baixa do Dropbox o PDF da folha e manda o contracheque recortado. Sessões em
memória (30 min) — um dos motivos do `--workers 1`. **Não passa pela
mensageria**: responde a quem escreveu, e só.

---

## Testes

- `tests/test_mensageria.py` — a decisão (política × chave geral), o
  notificador obedecendo à tela, o teto segurando e avisando, o registro, o
  encaixe no ERP (ações, seções, menu, padrão NEGAR, telas abrindo).
- `tests/test_mensageria_banco.py` (`@pytest.mark.banco`) — a migração 083, a
  semeadura do catálogo, a contagem do teto, a limpeza dos 90 dias.
- `tests/test_notificador.py` — a camada das variáveis (sem mensageria).
- **O bot do Telegram e o chatbot continuam sem teste.**

---

## Riscos e limites conhecidos

- **O cadastro do Telegram ainda é planilha.** Quem tem Telegram está na aba
  `TelegramID`; o ERP não sabe. "Só Telegram" para quem não se cadastrou vira
  "sem destinatário" no registro. Trazer o cadastro para o ERP é o próximo
  passo da área (ver `HISTORICO.md`).
- **Estado em memória**: sessões do chatbot/Telegram, cache da `TelegramID`,
  a trava "já avisei nesta hora" do teto fica no banco, mas a faxina diária
  do registro usa um relógio em memória (reinício só adianta a faxina).
- **`emissaonf` chama o próprio servidor por HTTP** para espelhar no Telegram.
- **Planilhas e o telefone master estão no código** do chatbot/Telegram.
