# Mensageria — decisões, estado e o que falta

Este arquivo existe para uma sessão nova pegar o trabalho sem repetir o que já
foi lido e discutido. O `README.md` ao lado explica **como** o chatbot, o bot
do Telegram e o notificador funcionam. Este aqui explica
**em que pé estão**, o que o dono quer deles e o que ainda depende dele.

---

## Como esta memória nasceu — 05/10/2026

O dono abriu um chat para estas aplicações e pediu duas coisas:

1. **Trazê-las para o mesmo jeito de trabalhar das outras áreas** — um chat
   por área, memória no repositório, evolução pelo Claude Code.
2. **Uma análise, com cautela, sobre juntar as três numa só** ("dentro do
   chatbot ter o gateway do Telegram e o gateway do WhatsApp"), sem quebrar
   nada.

E deixou claro o que quer no fim: **usar o WhatsApp para algumas coisas e
manter no Telegram o chatbot e os envios que já existem lá.**

Os dois arquivos foram escritos **a partir do código, lido de ponta a ponta**
(cerca de 3.300 linhas nas três pastas, mais o notificador e os seis módulos
que falam com o WhatsApp). O que está no mundo fora do repositório — o que há
nas variáveis do Render, se a Evolution foi instalada, se o Z-API continua
contratado, para onde o Make aponta — **não foi possível conferir daqui**, e
virou pergunta (abaixo).

**Antes desta sessão a área não tinha memória nenhuma:** o `CONTEXTO.md` dizia
"ainda não documentado aqui" para o `telegram` desde julho, e "detalhar quando
precisar mexer" para o `chatbot`. O histórico do git também não ajuda: as três
pastas entraram no repositório num commit só (29/09/2026), então **não há
registro de quando cada coisa foi escrita nem por quê** além dos comentários
no próprio código.

---

## 14/11/2026 (tarde) — REGRA RÍGIDA, FIM DAS PLANILHAS, PAINEL E A FICHA DA PESSOA

O dono perguntou se o número do Telegram era conferido com o do cadastro. Era,
mas com duas brechas: o CPF digitado cadastrava mesmo sem o número bater, e o
link do convite que eu tinha acabado de fazer aceitava outro chip. Decisão dele:
*"ser rígido. Se a pessoa não tiver o telefone batendo com o do cadastro, ele
tem que atualizar o telefone do cadastro. E isso tem que solicitar o DP.
Eliminar essas planilhas (…) deixar tudo funcionando com os dados que nós temos
dos colaboradores do DP (…) embutir o ID do Telegram no cadastro de cada um (…)
uma espécie de dashboard com monitoramento, com o tipo de mensagem, para quem e
o que, com filtragens na sidebar (…) com gráficos (…) por dia, por período, por
horário."* E: *"você já cruza as informações, vê se tem alguém que está faltando.
Acredito que você consegue acessar a base de dados."*

**PUBLICADO em 14/11/2026, com o "pode" do dono** ("estamos migrando do Make e
Pipefy para o ERP; se der problema depois resolveremos"). Falta ele apertar
"Aplicar atualizações do banco" (103) e "Migrar da planilha agora".

**Feito:**
1. **Regra rígida no robô**: número diferente do cadastro não cadastra; CPF
   digitado não cadastra; troca de número só se o novo estiver no cadastro. A
   pessoa é mandada ao DP e a tentativa entra na **fila do DP** (tabela nova
   `mensageria.telegram_tentativas`, na própria migração 103).
2. **Fim das planilhas**, com uma chave só: o botão **Migrar da planilha**.
   Ele copia a aba TelegramID cruzando com o ERP (confere / telefone diferente /
   fora do ERP) e guarda o relatório. Depois dele: o robô só reconhece e grava
   pelo ERP; a busca do chat dos avisos só olha o ERP; o contracheque do Telegram
   e o do chatbot de WhatsApp leem o colaborador do ERP; "Pendências Telegram"
   virou a fila do DP. Antes dele, tudo segue como era — publicar não muda nada
   até alguém apertar.
3. **Painel** (primeira aba do módulo): filtros na barra da esquerda, números do
   topo, gráficos por dia, hora e dia da semana, tipos, pessoas e as últimas
   mensagens com o texto.
4. **A ficha do colaborador** ganhou a aba **Telegram e mensagens** (Pessoal ›
   Colaboradores e a ficha única do ponto).
5. Na aba Cadastro do Telegram: a fila do DP, quem está sem telefone, o aviso
   de Telegram com número diferente, e o relatório da migração.

**O cruzamento que o dono pediu NÃO foi feito por mim:** este contêiner não
alcança o banco de produção nem a planilha (sem credenciais, e não deve ter). Ele
roda NO SERVIDOR, quando alguém aperta "Migrar da planilha", e o resultado fica
na tela. É o mesmo cruzamento, com os dados de verdade.

**Decisões minhas, com o motivo:**
- Quem estava na planilha com o telefone diferente do ERP **entra ativo** e vai
  para a lista do DP acertar — cortar quem já recebe não era o objetivo. A regra
  rígida vale para cadastro novo e troca de número. Se o dono quiser rígido
  também para esses, é desligar na tela, um a um, ou eu mudo a migração.
- Quem estava na planilha e não é mais colaborador ativo **não entra**.
- O "ID do Telegram" não virou coluna nova na tabela de colaboradores: fica na
  tabela do cadastro do Telegram, ligada ao colaborador, e APARECE na ficha dele.
  Assim o ERP não ganha coluna que só a mensageria usa, e o operador (quem entra
  no sistema) também pode ter Telegram.
- O painel é a primeira aba do módulo, porque é o que mais se abre.

**Verificado:** suíte inteira com banco de verdade; fluxo com banco: migração
(1 confere, 1 telefone diferente, 1 fora), número errado recusado e na fila do
DP, CPF recusado, DP acerta o telefone e o cadastro passa, contracheque pelo ERP,
e depois da migração nenhuma planilha foi tocada (o teste falharia). Telas
fotografadas (painel com 420 mensagens de duas semanas, cadastro, ficha).

### O que está pendente AGORA (pelo dono) — substitui a lista abaixo
1. "Pode" para publicar; **apertar "Aplicar atualizações do banco"** (103) no
   mesmo momento. Publicar sozinho não muda o robô: ele continua nas planilhas.
2. **ERP › Mensagens › Cadastro do Telegram › "Migrar da planilha agora"**, e ler
   o relatório: quem tem telefone diferente (o DP acerta) e quem ficou de fora.
3. Ver a lista "Colaboradores sem telefone no cadastro" — essas pessoas não
   conseguem se cadastrar até o DP preencher.
4. Dizer se algum cenário do Make lê a aba TelegramID direto (sem chamar o
   sistema): se sim, ele precisa passar a usar o `/telegram/enviar`.

## 14/11/2026 — O CADASTRO DO TELEGRAM VEIO PARA O ERP, e quem não recebe é convidado pelo WhatsApp

**O pedido**, nas palavras do dono: *"tem muita gente fazendo solicitações, mas
não está recebendo mensagens porque não tem cadastro no Telegram (…) o
mensageria identifica que a mensagem não foi entregue, aí ele manda uma
mensagem no WhatsApp explicando (…) que ela precisa fazer o cadastro (…) essa
questão dessa planilha, trazer para dentro do sistema também, e a gente
visualizar esse cadastramento dentro do mensageria."* Quem pode: *"os
colaboradores (…) que têm telefone"*. E, no meio do trabalho: *"na primeira
mensagem que não dá certo, a gente encaminha o WhatsApp. Mas não a cada
mensagem que der errada (…) depois de algumas outras mensagens, aí manda mais
uma (…) senão a gente pode correr o risco de ser banido de novo."*

**Feito no ramo `claude/amazing-wright-f8g0bm`** (não publicado):

1. **Aba Cadastro do Telegram** em ERP › Mensagens: quem pode se cadastrar,
   quem tem e quem não tem, quantas mensagens não chegaram em 30 dias, o último
   convite (automático ou pela tela, se abriu o link, se se cadastrou), convidar
   um ou até 50, desligar um Telegram, e "Cadastros a conferir" (Telegram que
   não bate com ninguém do ERP). Configuração do convite na mesma aba.
2. **Migração 103 do ERP**: `mensageria.telegram_cadastros` e
   `mensageria.convites`; o tipo `telegram.aviso_cadastro` passa de Desligado
   para Só WhatsApp (só se ninguém mexeu nele na tela).
3. **O convite automático** (primeira falha; depois só após mais 5 falhas E 7
   dias; no máximo 3 por pessoa) pelo notificador e pelo `/telegram/enviar`.
4. **O robô**: acha a pessoa no ERP antes das planilhas; grava no banco e na
   planilha; entende o link pessoal (`/start c_<código>`) antes do lote do
   Análise de SPs; aceita o número de outro chip quando veio de um convite;
   a planilha fora do ar não derruba mais um cadastro já gravado no ERP.
5. **O registro** passou a anotar o que chega pelo `/telegram/enviar`.
6. As perguntas que isto responde entraram no `erp/PERGUNTAS.md` (3z-quater).

**Verificado:** suíte inteira com banco de verdade (Postgres local descartável
neste contêiner), e o fluxo de ponta a ponta com banco: mensagem perdida → um
convite (a segunda perdida logo depois não gera outro) → link → número de outro
chip → cadastrado → a mensagem seguinte chega pelo Telegram. Tela fotografada.
**Não verificado:** envio real pelo Z-API e pelo Telegram (sem credenciais aqui).

**Decisões tomadas por mim, com o motivo** (o dono pode trocar):
- Números das travas: 5 mensagens, 7 dias, 3 automáticos por pessoa. Pela
  tela: um por dia por pessoa, até 50 por vez com 20–45 s entre eles.
- Convidar e trazer a planilha é do Administrador (`configurar_mensagens`);
  Diretor financeiro e DP só veem. Se o RH for convidar, é dar a seção a ele.
- Link pessoal com o número ainda confirmado no Telegram: o link facilita, o
  número prova. Vale 30 dias e é de uso único.

### O que está pendente AGORA (pelo dono)
1. "Pode" para publicar; ao juntar, **apertar "Aplicar atualizações do banco"**
   (a 103) no mesmo momento.
2. Em ERP › Mensagens › Cadastro do Telegram, apertar **Trazer da planilha
   TelegramID** uma vez. Até lá, quem já tinha Telegram aparece como "não tem" —
   e o convite automático NÃO vai para essas pessoas, porque o robô ainda acha o
   cadastro delas na planilha.
3. Conferir que a chave geral do WhatsApp está ligada (aba Tipos e canais) e ler
   o texto do convite.

## 31/10/2026 — uma situação nova no registro, vinda do chat do Ponto: "Barrado na origem"

O dono, no chat do Ponto: *"preciso que essas mensagens mesmo que bloqueadas apareçam no mensageria, pq assim tenho
como resolver a pane e visualizar a tentativa"* (uma pessoa pedia o PIN e não recebia, e o teste da tela Mensagens ia).
- O **ponto** passou a anotar no registro (`mensageria.envios`, por `notificador._registrar`, `origem = "ponto"`) o que
  ele mesmo segura antes do WhatsApp: **"Sem destinatário"** (SEM_DESTINO, já existente) quando não há celular com DDD
  no cadastro; **"Barrado na origem"** (BARRADO, novo) quando o CPF não foi achado, a pessoa está desligada, ou passou
  do teto de pedidos (código de acesso: 3 por hora; QR: 3 por dia). O detalhe diz o motivo.
- Aqui, só a tela: a situação "Barrado na origem" no filtro e na etiqueta (`templates/mensagens.html`). Nada mudou na
  regra, nos tetos nem na política.
- O que é decidido aqui já era anotado (enviada, falhou, tipo desligado, teto do WhatsApp).

## 29/10/2026 — um tipo novo no catálogo, vindo do chat do Análise de SPs: "Encaminhar SP"

- O dono pediu que a janela Encaminhar (Solicitações e Lote) ENVIE a mensagem, em
  vez de abrir o WhatsApp de quem clica. O envio passa pelo `notificador` com a
  finalidade `analisesps.encaminhar` (módulo "Análise de SPs", política
  **Só WhatsApp**): obedece à chave geral do WhatsApp e ao teto da tela, e fica no
  registro. Com a chave geral desligada, NÃO sai — a janela manda ligar aqui.
- Só a linha no `CATALOGO` (`core.py`) e o teste do catálogo; nada mais desta área
  mudou. Detalhes: `app/apps/analisesps/HISTORICO.md`, leva 243.

## 28/10/2026 — um tipo novo no catálogo, vindo do chat do Ponto: "Pedido do ponto negado"

Mudança pequena, feita pelo chat do Ponto e registrada aqui porque o catálogo é desta área: a
linha `ponto.pedido_negado` em `mensageria/core.py` (política "Só WhatsApp", como todo tipo do
ponto, pela decisão de 05/10/2026). É o aviso à pessoa — e a quem pediu por ela — de que o
ajuste, atestado ou licença foi negado, com o motivo (`ponto/core/negativas.py`). Sai pela fila
do ponto, no ritmo dela; a mensageria decide o canal como para os outros tipos. Sem o WhatsApp
ligado na tela Mensagens, não sai — e o aviso no aplicativo continua valendo. O teste
`test_a_fila_do_ponto_diz_o_tipo_de_cada_mensagem` passou a conferir também que todo tipo da
fila do ponto está no catálogo.

## 08/10/2026 — o robô do Telegram passou a alimentar o lote do Análise de SPs

Mudança feita pelo chat do Análise de SPs, com o "pode" do dono para mexer no
robô. O que mudou AQUI: o webhook (`telegram_bot.telegram_webhook`) pergunta
primeiro ao `analisesps.telegram_lote.receber` se a mensagem é dele —
`/start lote_<código>` (o link de ligar, gerado na tela Lote) ou texto com
número de SP (10 dígitos) vindo de conversa ligada. Se for, responde e para;
se não, o robô segue o caminho de sempre (cadastro, contracheque). Um erro do
Análise de SPs vira uma frase ao usuário e nunca derruba o robô
(`_lote_analisesps`). CPF (11 dígitos) não é confundido com SP. O motivo, o
desenho e os testes estão no `analisesps/HISTORICO.md` (leva 200).

## Onde o trabalho está

**05/10/2026, noite — a TELA MENSAGENS está PUBLICADA** (junção `25cf488`, com o
"pode" do dono; suíte verde local e no GitHub Actions com banco). O dono disse "pode fazer assim, vamos criar essa tela de
mensageria, vamos para frente", com três condições: não mexer em nada que
impacte o Análise de SPs e o Make; começar usando o WhatsApp pelo ponto; e
testar. O que foi feito:

1. **Módulo `mensageria` com código** (`core.py`, `gestao.py`, `db.py`, tela
   `templates/mensagens.html`), pendurado no ERP como o ponto: menu
   **Mensagens**, abas *Tipos e canais* e *Enviadas*. Ações `ver_mensagens`
   (Administrador, Diretor financeiro, DP) e `configurar_mensagens`
   (Administrador); seção `adm_mensagens`; **migração 083 do ERP** (schema
   `mensageria`: `tipos`, `envios`, `parametros`, e as seções nos perfis).
2. **Política por tipo** (Só Telegram · Telegram, WhatsApp para quem não tem ·
   Só WhatsApp · Desligado), **chave geral do WhatsApp** (nasce desligada) e
   **teto** por hora/dia para a empresa inteira, com aviso aos ADMIN por
   Telegram uma vez por hora. **Registro** de 90 dias de tudo que o
   notificador tentou, inclusive o que não saiu e por quê.
3. **O notificador obedece à tela.** Com `finalidade=` e a mensageria no
   banco, a política manda; sem ela (antes da 083, sem banco, módulos
   antigos sem tipo), valem as variáveis como antes — nada muda para o
   Análise de SPs, a baixa Bradesco, o ValidaSP, o ProcessarNovaSP e o Make,
   que não foram tocados. Os envios deles ainda assim aparecem no registro
   como "sem_tipo".
4. **O ponto e o ERP dizem o tipo de cada mensagem** (`ponto.qr`,
   `ponto.codigo_acesso`, `ponto.mosaico`, `ponto.aviso_foto`,
   `ponto.resumo_dia`; `erp.titulo_pago`, `erp.encaminhamento`,
   `erp.agente_cobranca`, `erp.pergunta_agendada`, `erp.teto_ia`,
   `erp.insumos`). A fila do ponto só liga se a mensageria liberar o QR por
   WhatsApp. Mudança de UMA linha em cada chamada do ERP.
5. **A Evolution saiu** (detalhe mais abaixo).
6. **Testes**: `tests/test_mensageria.py` (sem banco) e
   `tests/test_mensageria_banco.py` (com banco; roda no GitHub Actions — este
   contêiner não tem Postgres). A suíte inteira rodou e o monorepo sobe.

**A tela foi testada aqui só por código** (as páginas abrem com sessão dublada,
as rotas negam quem não pode, a decisão e o teto têm teste). **Não foi aberta
num navegador com banco de verdade** — isso é o passo 3 do dono, abaixo.

### O que está pendente AGORA

**Pelo dono, nesta ordem:**

1. ~~Dizer "pode"~~ — feito; publicado em 05/10/2026.
2. **Agora**, ERP › Configurações › "Aplicar atualizações
   do banco" (a **083**). Até apertar, a tela Mensagens abre e explica o que
   falta, e os envios seguem as variáveis (WhatsApp desligado).
3. Abrir **ERP › Mensagens**: conferir que os tipos aparecem, que o ponto está
   "Só WhatsApp", **ligar a chave geral do WhatsApp** e mandar uma mensagem de
   teste para o próprio celular, pelos dois canais. Depois pedir um QR Code ou
   o código do PIN pela tela do ponto e conferir na aba *Enviadas*.
4. A variável `NOTIFICAR_WHATSAPP_PONTO` que foi sugerida de manhã **não é
   mais necessária** — a tela substitui. `NOTIFICAR_WHATSAPP=0` pode ficar: só
   vale quando a mensageria não alcança.

**Próximos passos de código (ordem sugerida):**

- **O cadastro do Telegram no ERP** (entrega 2 do plano): o bot reconhecer a
  pessoa pelo cadastro do ERP e gravar o id do chat lá, com a planilha
  `TelegramID` como reserva na transição. Hoje "Só Telegram" para quem não se
  cadastrou vira "sem destinatário" e o ERP não sabe quem falta.
- Quando os módulos antigos forem desligados, a camada das variáveis e os
  envios próprios de Z-API deles somem junto.

---

## Como a tarde começou — 05/10/2026, tarde: o dono respondeu, e a área ganhou código

**O dono respondeu, e a área ganhou código.** As respostas,
nas palavras dele: o Z-API está ativo; `NOTIFICAR_WHATSAPP` está em `0`; a
Evolution "eu nem sei o que é isso (…) vamos excluir"; o Z-API bloqueava
porque eram muitas mensagens, por isso os avisos migraram para o Telegram e o
WhatsApp ficou em poucos cenários do Make; e o ponto precisa de WhatsApp, mas
no teste dele "não chegou nada". As variáveis que existem no Render (só os
nomes): `NOTIFICAR_TELEGRAM`, `NOTIFICAR_WHATSAPP`, `TELEGRAM_BOT_TOKEN`,
`TELEGRAM_SECRET_TOKEN`, `ZAPI_API_TOKEN`, `ZAPI_CLIENT_TOKEN`,
`ZAPI_INSTANCE_ID`, `CHATBOT_MASTER_PHONE`, `CHATBOT_WEBHOOK_SECRET`.
**Não existe** `ZAPI_INSTANCE_TOKEN`, nem variável da Evolution, nem
`TELEGRAM_BOT_LINK` (vale o padrão `t.me/bwsconstrucoesbotbot`).

O que foi feito, no ramo `claude/amazing-wright-f8g0bm` (não publicado):

1. **A Evolution saiu.** A pasta `whatsapp_gateway/` foi apagada e o
   `app/main.py` deixou de registrá-la (18 blueprints). Era isto: uma
   alternativa ao Z-API, de código aberto, que rodaria como um segundo
   serviço no Render; o gateway era a peça que faria o Make falar com ela sem
   mudar de endereço. Nunca saiu do plano, ninguém a usava, nenhuma variável
   dela existia. Código morto com cinco rotas públicas só confunde.
2. **O WhatsApp do ponto não chegava por DOIS motivos, e os dois estão
   tratados.** O ponto conferia o token pelo nome `ZAPI_INSTANCE_TOKEN`, que
   não existe no Render — então a fila de QR Code nem ligava, e o código do
   PIN saía com "não configurado". E, mesmo que ligasse, `NOTIFICAR_WHATSAPP=0`
   calaria tudo. Agora o notificador aceita os dois nomes de token, lê as
   credenciais a cada chamada, e o ponto pergunta a ele.
3. **Liga/desliga por finalidade.** `NOTIFICAR_WHATSAPP_PONTO=1` liga o
   WhatsApp só para o ponto, com o geral em `0`. Vale para qualquer canal e
   qualquer finalidade (`NOTIFICAR_<CANAL>_<FINALIDADE>`); a decisão fica num
   lugar só, o `notificador.py`. O aviso "cadastre-se no Telegram" que saía
   por WhatsApp sem olhar botão nenhum passou a obedecer também
   (`AVISO_CADASTRO_TELEGRAM`).
4. **Primeiros testes da área**, `tests/test_notificador.py` (12 casos). A
   suíte inteira passou (o `main.py` mudou, então rodou tudo), e o monorepo
   sobe.

### O que está pendente AGORA

**No Render, pelo dono (nada disso dá para fazer daqui):**

1. Criar a variável **`NOTIFICAR_WHATSAPP_PONTO` = `1`**. Sem ela, mesmo
   depois de publicar, o ponto continua calado — e é o certo: a geral está em
   `0` de propósito.
2. Publicar o ramo (juntar na `main`), com o "pode" dele. Não há migração de
   banco.
3. **Testar de novo o envio do ponto** — pedir um QR Code ou o código do PIN
   pela tela do ponto. Se não chegar, os lugares para olhar, nesta ordem: a
   tela de gestão do ponto diz "WhatsApp configurado"? (é o `whatsapp_pronto()`),
   o `resultado` da fila `ponto.envios` (mostra a resposta do Z-API), e a cota
   do Z-API.

**Cuidado que continua valendo, dito pelo dono:** o Z-API bloqueia pelo
volume. A fila do ponto já tem ritmo (30–90 s entre mensagens, 40/hora,
200/dia, seg.–sáb. 7h30–17h30). Ligar outras finalidades no WhatsApp é
decisão caso a caso, e cada uma ganha a sua variável.

**Próximo passo de código (decisão tomada, execução pendente):** unificar no
notificador os envios próprios de Z-API do `validasp`, `baixabradesco`,
`emissaonf` e `chatbot`, dando a cada um a sua finalidade. Hoje eles obedecem
só à variável geral e cada um tem a sua cópia do envio. É mudança em quatro
áreas de uma vez; vale um pedido próprio, com os testes de cada módulo.

---

## Como a sessão começou — a leitura (05/10/2026, manhã)

O que o dono acreditava sobre as aplicações e o que o código dizia divergiam
em três pontos, e isso foi corrigido antes de decidir:

| O dono lembra | O que o código mostra |
|---|---|
| "O chatbot usa o Telegram" | O **chatbot é de WhatsApp** (Z-API). O Telegram tem uma **cópia** do fluxo de contracheque dentro do `telegram_bot.py`, que importa os módulos do chatbot e troca só o envio |
| "Tem algo em algum canto que desativa o WhatsApp, e acho que está desativado" | O botão é a variável **`NOTIFICAR_WHATSAPP=0`** no Render. Ela desliga o WhatsApp no notificador, no ValidaSP, no BaixaBradesco, na emissão de NFS-e e no ponto. **Não** desliga as respostas do chatbot nem o aviso "cadastre-se no Telegram" — esses dois não olham a variável. Se está em `0` hoje, só o Render diz |
| "São três aplicações separadas que trabalham juntas" | São **quatro peças**, e a quarta é a mais usada: o `notificador.py`, importado por seis módulos. E o gateway **não trabalha com ninguém** por enquanto: nenhum módulo Python o chama, e ele só tem efeito se a Evolution estiver no ar e o Make apontar para ele |

### As perguntas feitas ao dono (respondidas no mesmo dia — ver acima)

1. **O Z-API continua contratado e ativo?** Todo envio de WhatsApp do
   repositório passa por ele. Se foi cancelado, "usar o WhatsApp" começa por
   escolher outro provedor (ver análise).
2. **Qual destas duas variáveis existe no Render: `ZAPI_API_TOKEN`,
   `ZAPI_INSTANCE_TOKEN`, ou as duas?** São dois nomes para o mesmo token.
   O chatbot, o ValidaSP, o BaixaBradesco e a emissão de NFS-e leem a
   primeira; o notificador, o ponto e o aviso do Telegram leem a segunda.
   **Se só uma existir, metade dos envios de WhatsApp falha em silêncio** —
   e o ponto (QR Code, PIN) está nessa metade. A resposta é olhar a tela de
   variáveis do Render; o nome basta, **o valor não entra no chat**.
3. **`NOTIFICAR_WHATSAPP` está em `0` hoje?** Mesmo lugar.
4. **A Evolution API foi instalada como segundo serviço no Render?**
   `WHATSAPP_GATEWAY_INSTANCES` foi preenchida? Algum cenário do Make já
   aponta para `aplicacoes.bwsconstrucoes.com.br/instances/...`? Em julho isso
   estava "pendente"; não há registro posterior.
5. **O webhook do Z-API ainda aponta para `/api/chatbot/webhook`?** Ou seja:
   alguém ainda pede contracheque pelo WhatsApp, ou isso já migrou de vez para
   o Telegram? Decide se o chatbot de WhatsApp é algo a manter ou a aposentar.
6. **Quais coisas vão para o WhatsApp?** Hoje a escolha é por módulo, no
   código (tabela do `README.md`), e o botão `NOTIFICAR_WHATSAPP` é **um só
   para tudo**. "Algumas coisas pelo WhatsApp" exige dizer quais: QR e PIN do
   ponto (já são só WhatsApp), aviso de título pago (hoje só Telegram),
   falhas da baixa, nota emitida, validação de SP…

---

## A análise: juntar as três numa só?

Pedida pelo dono em 05/10/2026. Resposta curta: **juntar as pastas é possível
e não exige mudar nenhum endereço externo — mas é a mudança que menos resolve
o que ele quer.** O que desorganiza a área não é haver três pastas; é haver
**seis códigos de envio de WhatsApp**, **dois nomes para o mesmo token**, **um
botão só** para ligar e desligar tudo, e **um gateway sem ninguém do lado de
dentro usando**.

### O que um "juntar" físico mudaria, e o que não mudaria

- **Endereços externos não precisam mudar.** O Z-API, o BotFather, o Make e a
  Evolution apontam para rotas (`/api/chatbot/webhook`, `/telegram/webhook`,
  `/telegram/enviar`, `/instances/...`). Um blueprint pode mudar de pasta e
  manter as rotas iguais. Então mover código **não** obriga a reconfigurar
  nada fora — a preocupação do dono com "alterar redirecionamentos" não se
  confirma, desde que as rotas sejam preservadas à risca.
- **O que quebra ao mover são as importações internas.** O notificador importa
  funções privadas do `telegram_bot` (com underline, isto é, não feitas para
  serem importadas); o Telegram importa cinco módulos do chatbot; seis
  módulos importam o notificador; e a emissão de NFS-e chama `/telegram/enviar`
  por HTTP com a URL **gravada no código**. É tudo rastreável e a suíte não
  cobre nada disso — um nome errado só aparece em produção, na hora de
  enviar.
- **O ganho é só de arrumação.** Uma pasta em vez de três. Nenhum
  comportamento muda, nenhuma pergunta do dono é respondida.

### O que realmente organiza (recomendação)

A separação que faz sentido não é por aplicativo, é por **papel**:

| Papel | Hoje | Como deveria ser |
|---|---|---|
| **Canal** — como se fala com o WhatsApp e com o Telegram | WhatsApp em 6 lugares; Telegram em 1 (bem feito) | **Um** lugar para cada canal. Todo mundo chama "mande isto para este telefone" e não sabe se por trás é Z-API, Evolution ou outro |
| **Conversa** — o que o bot diz e pergunta | O "cérebro" no `chatbot/`, uma cópia do fluxo dentro do `telegram_bot.py` | Um fluxo só, que recebe "chegou esta mensagem de fulano por este canal" e responde pelo mesmo canal |
| **Avisos** — quem recebe o quê e por onde | Decidido no código de cada módulo + um botão global | Uma regra por **tipo de aviso** (QR do ponto, título pago, nota emitida…) dizendo o canal e se está ligado — e aí sim "WhatsApp para algumas coisas" vira configuração, não publicação |

Isso se faz **em fases, cada uma segura sozinha, sem mexer em pasta**:

1. **Primeiro, as respostas do dono** (lista acima). Sem saber o que está no
   Render e se a Evolution existe, qualquer mudança no envio é chute.
2. **Um envio de WhatsApp só**, dentro do `notificador.py`, aceitando os dois
   nomes de token e respeitando o botão; os seis módulos passam a chamar ele.
   É mudança de código em seis áreas — pequena em cada uma, mas **atravessa
   áreas** e precisa do teste de cada módulo. Ganha teste próprio (hoje não há
   nenhum). ⚠️ Efeito colateral a avisar: se hoje só um dos nomes de token
   existe no Render, metade dos envios está falhando em silêncio; aceitar os
   dois nomes **faz esses envios passarem a sair**. É bom — mas é mensagem
   chegando no celular de colaborador que hoje não chega, e o dono precisa
   saber antes.
3. **Botão por tipo de aviso** em vez de um global — o que o dono pediu de
   fato.
4. **Decidir o provedor de WhatsApp.** Se a Evolution está no ar, o gateway
   ganha um segundo cliente (o próprio notificador, além do Make) e o Z-API
   pode ser aposentado como estava planejado. Se não está, o gateway é código
   parado e a decisão é instalá-la ou removê-lo.
5. **Só no fim, e só se o dono quiser a arrumação:** mover as três pastas para
   uma (`app/apps/mensageria/`, esta mesma, que por enquanto só guarda a
   memória), mantendo todas as rotas. A essa altura as importações já
   estariam concentradas, e mover seria barato.

### O que ficou de fora de propósito

- Não se mexeu em código, nem nos consertos pequenos e seguros (aceitar os
  dois nomes de token, fazer o chatbot respeitar o botão): cada um deles **muda
  o que chega no celular de alguém**, e isso depende das respostas acima.
- Não se tentou falar com Z-API, Telegram, Evolution, Google ou Dropbox: a
  sessão não tem credenciais, e não deve ter.

---

## Decisões registradas

- **13/10/2026 — dois tipos novos no catálogo, do ponto (chat do Ponto):**
  `ponto.ronda` ("Ronda surpresa do vigia") e `ponto.ronda_perdida` ("Aviso de
  ronda perdida"), os dois nascendo "Só WhatsApp". São os primeiros que saem de
  MADRUGADA de propósito — a ronda surpresa do vigia noturno; poucas por noite,
  fora da fila com ritmo do QR. Só a linha do catálogo foi mexida aqui.

- **05/10/2026 — A área ganhou memória antes de ganhar código.** Mesma regra
  da emissão de NFS-e (21/09/2026): área sem `README.md`/`HISTORICO.md` não
  entra na tabela do `CLAUDE.md` apontando para arquivos que não existem. A
  memória vive numa pasta própria, `app/apps/mensageria/`, porque cobre
  quatro peças em quatro lugares — e porque, se um dia as pastas forem
  unidas, é para cá que vêm.
- **05/10/2026 — Recomendação: não juntar as pastas agora.** Motivos na
  análise acima. A decisão é do dono.
- **05/10/2026 — A Evolution foi removida, não guardada.** O dono não a
  reconheceu e pediu para excluir. Fica no git se um dia o Z-API bloquear de
  vez; o `CONTEXTO.md` §9 (10/07/2026) guarda o porquê da escolha original.
- **05/10/2026 — Liga/desliga por finalidade, não por módulo.** A variável
  leva o nome do uso (`PONTO`), não do módulo que envia, porque é assim que o
  dono pensa ("o ponto precisa de WhatsApp"). E fica no notificador, para
  que todo canal e todo uso obedeça à mesma regra. Variável ausente ou vazia
  = segue a geral, para não mudar comportamento de quem não pediu.
- **05/10/2026 — O token do Z-API tem um nome, `ZAPI_API_TOKEN`.** O outro é
  apelido aceito, não documentado como opção. Novo código lê o nome do Render.
- **05/10/2026 — A gestão é uma TELA, não variável.** Variável no Render é
  invisível: não se vê a lista, não se vê o que saiu. A tela mostra os tipos,
  decide por tipo, e o registro mostra o que aconteceu. As variáveis viram
  rede de segurança.
- **05/10/2026 — A mensageria mora no ERP, não numa função solta.** Porque é
  no ERP que tudo vai se concentrar (palavras do dono: "vamos concentrar e
  focar tudo no ERP"); os módulos antigos vão sumir e não ganharam adaptação.
- **05/10/2026 — Política por tipo, com a opção "Telegram; WhatsApp para quem
  não tem".** É a que gasta WhatsApp só com quem ainda não se cadastrou — o
  jeito de alcançar todo mundo sem alimentar o bloqueio do Z-API.
- **05/10/2026 — A chave geral do WhatsApp nasce DESLIGADA.** Mandar WhatsApp
  é escrever em sistema de terceiro, e o Z-API bloqueia; quem liga é o dono,
  na tela, com confirmação.
- **05/10/2026 — O aviso de teto vai aos ADMIN do ERP, não ao telefone master
  da variável.** O papel já existe no cadastro; e vai direto pelo Telegram,
  sem passar pela política, para nunca cair no próprio teto.
- **05/10/2026 — Os módulos antigos não foram tocados** (condição do dono:
  "não quero mexer em absolutamente nada que impacte no Análise de SPs, tudo
  que está no Make"). Eles continuam decidindo pelas variáveis; só o registro
  passou a vê-los.

---

## Incidentes conhecidos (do próprio código — sem data no `CONTEXTO.md`)

- **16/07/2026 — preview de link do Telegram disparava ações.** O robô do
  Telegram fazia um GET na URL para montar o preview e isso **executava links
  de ação de um clique** (validação/anuência de SP) antes de a pessoa clicar.
  Conserto: todo envio vai com preview desligado (`_tg_enviar`). Está só no
  comentário do código; registrado aqui para não se perder.
- **Loop de eco no bloqueio do chatbot.** Telefone bloqueado por CPF errado
  recebia resposta a cada nova mensagem, inclusive a callbacks de status do
  Z-API reentrando no webhook — gastava cota e girava em círculo. Conserto:
  bloqueado não recebe resposta nenhuma. Sem data.
- **Token do Dropbox guardado para sempre.** O chatbot usava um cache sem
  validade e, depois de ~4 h de processo de pé, todo download falhava com
  `401 expired_access_token`. Conserto: cache com expiração. Sem data.
- **Make quebrava JSON com aspas** na mensagem do `/telegram/enviar`.
  Conserto: a rota aceita formulário e upload, além de JSON, e destrava `\n`
  literal. Sem data.
