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

## O painel (14/11/2026)

`painel.py`; aba **Painel** (a primeira do módulo). Tudo do registro
`mensageria.envios`, no período escolhido (até 90 dias), com hora e dia de
Fortaleza. Filtros na barra da esquerda: período (hoje, 7, 30, 90 dias ou datas),
canal, de onde vem (módulo), tipo, situação (entregue, não chegou, barrada) e
pessoa (nome, CPF ou telefone). Mostra: os números do topo; entregues por dia
(Telegram × WhatsApp); não chegaram por dia; entregues por hora do dia e por dia
da semana; os tipos de mensagem (Telegram, WhatsApp, não chegou); as pessoas
(quantas, por onde, quais tipos, a última); e as últimas 200 mensagens com o
texto. Clicar no nome filtra a pessoa. A **pessoa** de cada linha é achada pelo
CPF, pelo telefone (com/sem 55 e nono dígito) ou pelo id do chat, contra
colaboradores, operadores e o cadastro do Telegram; sem casar, aparece como
"Fora do cadastro (telefone)". Gráficos em SVG desenhado no próprio arquivo,
sem biblioteca: azul = Telegram, laranja = WhatsApp, vermelho = não chegou.

**Na ficha do colaborador** (Pessoal › Colaboradores, e a ficha única do
ponto): aba **Telegram e mensagens**, com a situação do Telegram, o último
convite, as contagens de 30 dias e as últimas 15 mensagens, e o link para o
painel já filtrado. O bloco é um só, `mensagensDaPessoa` no `erp_base.html`;
lê `/erp/api/mensagens/pessoa/<id>` (`ver_mensagens`).

## O cadastro do Telegram e o convite (14/11/2026)

Código em `telegram_cadastro.py`; tela na aba **Cadastro do Telegram**; tabelas
`mensageria.telegram_cadastros` e `mensageria.convites` (migração **103** do ERP).

**Quem pode se cadastrar:** colaborador do ERP que não está desligado e tem
telefone, e operador do ERP ativo com telefone (decisão do dono).

**REGRA RÍGIDA (dono, 14/11/2026):** o número que o Telegram confirma (botão
"Compartilhar meu número") tem de bater com o telefone do cadastro do ERP — com
ou sem 55, com ou sem o nono dígito. Não bateu: **não cadastra**; o robô manda a
pessoa pedir ao DP para atualizar o telefone, e a tentativa entra na **fila do
DP** (`mensageria.telegram_tentativas`, aba Cadastro do Telegram). **CPF digitado
não cadastra** (só entra na fila, para o DP saber quem é). Trocar de número vale
a mesma regra.

**As planilhas morrem — e a chave é o botão "Migrar da planilha".** Antes dele,
tudo segue como era (o robô lê as abas "Dados Documentos", "Colaborador
(Cartões)" e "TelegramID" e escreve nesta e na "Pendências Telegram"). O botão
copia a aba TelegramID para `mensageria.telegram_cadastros` **cruzando cada linha
com o ERP**: confere (entra), telefone diferente (entra, e o DP acerta), fora do
ERP (não entra). Daí em diante **nenhuma planilha é lida nem escrita**: o robô
reconhece só pelo ERP, grava só no ERP, a busca do chat dos avisos olha só o ERP,
o contracheque pelo Telegram e pelo WhatsApp (chatbot) leem o colaborador do ERP,
e a fila do DP substitui a "Pendências Telegram". O relatório do cruzamento fica
guardado e aparece na tela.

**O convite:** WhatsApp com um **link pessoal** (`t.me/<robô>?start=c_<código>`,
vale 30 dias, uma vez). Quem toca no link é saudado pelo nome e confirma o
número — que precisa bater, como qualquer outro. O link só facilita.

- **Automático** — na **primeira** mensagem do Telegram que não chega por falta
  de cadastro (pelo notificador ou pelo `/telegram/enviar`), se a pessoa é
  elegível. O próximo só sai depois de **mais N mensagens que não chegaram**
  (padrão 5) **e** de **N dias** (padrão 7), e no máximo **3 automáticos por
  pessoa**. Pedido do dono: *"não a cada mensagem que der errada (…) senão a
  gente pode correr o risco de ser banido de novo"*. Os três números se mudam na
  tela. Roda em segundo plano: quem mandou a mensagem original não espera.
- **Pela tela** — um a um (resposta na hora) ou até 50 marcados (saem um por vez,
  com 20–45 s entre eles; o lote para se bater no teto). No máximo um por dia por
  pessoa; não conta para o máximo automático.
- Os dois passam pelo tipo `telegram.aviso_cadastro` (**Só WhatsApp** desde a
  103), pela chave geral do WhatsApp e pelo teto — e ficam na aba Enviadas.
  Convite que não saiu (WhatsApp desligado, teto) não conta como convite.

**O texto** é editável na tela (`{nome}` = primeiro nome, `{link}` = link
pessoal, obrigatório); o sugerido está em `TEXTO_PADRAO`. Pela tela, a frase
"tentou te mandar uma mensagem" sai do texto sugerido, porque não houve
mensagem perdida.

**O registro ganhou o Make:** o que passa por `/telegram/enviar` é anotado como
"Envio externo pelo Telegram (Make, emissão de nota)" — só anotação, a resposta
ao Make não mudou.

## O bot do Telegram e o chatbot, em resumo

**Telegram.** `/start` → botão "Compartilhar meu número" → procura o número em
duas abas de planilha só de leitura (*Dados Documentos* e *Colaborador
(Cartões)*) → grava na aba **`TelegramID`** (uma linha por pessoa). Não achou
→ pede CPF. Falhas vão para a aba *Pendências Telegram*. Depois de cadastrado,
`1`/"contracheque" abre o assistente (reusa os módulos do chatbot).
`/telegram/enviar` manda texto ou arquivo por `chat_id`, `telefone` ou `cpf`
(procura no cadastro do ERP e depois na `TelegramID`); quem não tem cadastro
recebe o convite da seção acima. Links vão **sem preview** — o
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
- `tests/test_mensageria_telegram.py` e `tests/test_mensageria_telegram_banco.py`
  — quem pode, a busca do chat, a troca de conta, as três travas do convite, o
  link pessoal, a cópia da planilha, o painel, o robô (link do convite antes do
  lote do Análise de SPs) e a tela.
- **O bot do Telegram e o chatbot continuam sem teste.**

---

## Riscos e limites conhecidos

- **Make lendo a aba TelegramID direto:** se algum cenário do Make lê a planilha
  sem passar pelo `/telegram/enviar`, depois da migração ele deixa de ver os
  cadastros novos. O código do repositório não lê mais a aba; o Make não dá para
  conferir daqui.
- **Quem estava na planilha com o telefone diferente** foi migrado ATIVO (para
  não cortar quem já recebe) e aparece com aviso "DP acertar". A regra rígida vale
  para cadastro novo e para troca de número.
- **Estado em memória**: sessões do chatbot/Telegram, cache da `TelegramID`,
  a trava "já avisei nesta hora" do teto fica no banco, mas a faxina diária
  do registro usa um relógio em memória (reinício só adianta a faxina).
- **`emissaonf` chama o próprio servidor por HTTP** para espelhar no Telegram.
- **Planilhas e o telefone master estão no código** do chatbot/Telegram.
