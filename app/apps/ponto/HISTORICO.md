# Ponto eletrônico — HISTÓRICO

Memória da área. Escrito para quem nunca viu a conversa. Ler antes de mexer,
junto com o `README.md` e o `PLANO.md`.

## Pendente AGORA

0. **O WhatsApp do ponto passou a ser decidido em ERP › Mensagens (05/10/2026,
   chat da Mensageria, ramo não publicado).** Por que "não chegou nada" no teste
   do dono: a fila conferia o token do Z-API por um nome de variável que não
   existe no Render (`ZAPI_INSTANCE_TOKEN`), e a chave geral `NOTIFICAR_WHATSAPP`
   está em `0`. Agora `whatsapp_pronto()` pergunta à mensageria: credenciais
   presentes **e** o tipo "QR Code do ponto" liberado por WhatsApp (política
   "Só WhatsApp", que é como nasce) **e** a chave geral do WhatsApp **ligada na
   tela** (nasce desligada). Cada mensagem da fila diz o seu tipo (`ponto.qr`,
   `ponto.mosaico`, `ponto.aviso_foto`); o código do PIN é `ponto.codigo_acesso`
   e o resumo do dia `ponto.resumo_dia`. O ritmo da fila continua aqui; a
   mensageria é a segunda cerca (teto da empresa inteira) e o registro. Para
   voltar a enviar: publicar, aplicar a 083 do ERP, ligar o WhatsApp em
   ERP › Mensagens, e a chave "Enviar e trocar o QR automaticamente" aqui.

1. **Fase 2 e QR/mosaico PUBLICADOS em 04/10/2026** (com o "pode" do dono).
   Falta o dono apertar, se ainda não apertou, **os dois botões de banco**:
   - ERP › Configurações › "Aplicar atualizações do banco" (a **082**, que dá as
     seções do ponto aos perfis). Sem ela, quem tem perfil cadastrado não vê o
     menu Ponto;
   - ERP › Ponto › Configuração › "Aplicar atualizações do ponto" (a 001, se
     ainda não rodou, a **002** e a **003**). Sem ela, as telas do ponto
     respondem "aplique as atualizações" em vez de quebrar, e o tablet aceita
     só CPF (o QR do WhatsApp responde "ainda não foi ativado").
1b. **Três chaves que nascem DESLIGADAS e são decisão do dono** (Ponto ›
   Configuração): "Enviar e trocar o QR Code automaticamente" (manda WhatsApp
   para todo mundo com telefone, espalhado em dias); "Avisar a pessoa quando a
   batida ficar sem foto"; e quais obras têm o **mosaico obrigatório**, com que
   responsável. Sugestão: ligar o QR só depois do piloto numa obra.
1d. **Quem valida cada pedido** (Ponto › Configuração): nasce tudo no DP. Se
   algo deve ir para o encarregado (ajuste de batida, por exemplo), trocar lá.
1c. **Coordenadas das obras na coluna AM ("Coordenadas Geográficas") da aba
   C. Diários**, no formato `-3.731862, -38.526670` (o que o Google Maps copia).
   A cerca BLOQUEIA e a obra é detectada pela localização; obra ativa sem
   coordenada não detecta (vai para conferência). Depois de preencher: Ponto ›
   Configuração › Base de obras › "Ler a planilha agora" e conferir a tabela
   "Coordenadas que não deu para ler".
1o. **PUBLICADO em 10/10/2026, noite** (com o "pode" do dono, `main` em
   `248c90d`): documento com um botão só, o "esqueci de bater" no ponto da obra
   abrindo o dia com os horários da escala e a hora digitada, e a **migração
   010** (licenças da lei atualizadas). Confirmar com o dono que apertou
   "Aplicar atualizações do ponto" — sem a 010 nada quebra, só o pré-natal fica
   com a regra antiga. E que o botão do documento abriu o menu do iPhone.
   **Janeiro de 2027: subir a licença-paternidade de 5 para 10 dias** em
   ERP › Ponto › Configuração › Jornada e licenças (Lei 15.371/2026).
1n. **PUBLICADO em 10/10/2026, segunda leva** (com o "pode" do dono, sem
   migração nova): no ponto da obra todo mundo entra primeiro (CPF e PIN), a
   consulta não depende da localização, a batida não desloga e sair dela pede o
   PIN. Confirmar com o dono: o PIN para sair da batida ficou bom, ou prefere
   sem? O responsável de cada ponto da obra já tem PIN criado?
1m. **PUBLICADO em 10/10/2026** (com o "pode" do dono, sem migração nova): o
   ponto da obra como aparelho (consulta pelo CPF sem PIN, QR por WhatsApp na
   tela inicial, sem caixa de obra, opções que não mudam com quem entrou) e o
   aplicativo que não se registra só por abrir. Confirmar com o dono: as obras
   que vão usar o ponto da obra já têm coordenada na coluna AM? O risco da
   consulta só pelo CPF ficou aceito, ou quer a data de nascimento junto?
1l. **PUBLICADO em 09/10/2026, terceira leva** (com o "pode" do dono, sem
   migração nova): o ponto da obra só pela localização (sem caixa de obra; sem
   localização ou fora da área, não bate), o celular da pessoa explicando sem
   localização, e o QR que sai no primeiro pedido. Confirmar com o dono: o QR
   chegou no primeiro pedido? O ponto da obra mostrou a obra certa no alto?
1k. **PUBLICADO em 09/10/2026, segunda leva** (com o "pode" do dono, sem
   migração nova): fora da obra só explicando (vai para conferência), nada
   sem localização no celular, ⏳ na batida em conferência, e o aparelho que
   chega com o nome de quem entrou. Confirmar com o dono: aprovou o celular
   novo da pessoa que tinha apagado os dados do navegador?
1j. **PUBLICADO em 09/10/2026** (com o "pode" do dono): os seis ajustes de
   09/10 e a correção do fuso na apuração (seção de 09/10), com a migração
   **009**. Confirmar com o dono: apertou "Aplicar atualizações do ponto"?
   Marcou em Cerca das obras as obras de só entrada e saída? Conferiu a
   coordenada da obra de teste (a recusa a 16 mil km)?
1i. **PUBLICADO em 08/10/2026** (com o "pode" do dono, `main` em `29bd1f0`): as
   correções do cadastro de aparelhos (seção de 07/10) e a TELA INICIAL com a
   consulta pela cerca, o administrativo de obra, os ícones ✏️/🗓️ e a regra
   do computador (seção de 08/10), com a migração **008**. Confirmar com o dono:
   apertou "Aplicar atualizações do ponto"? Sem ela, ninguém é administrativo e
   o pedido pelo celular segue a regra de 06/10. Depois: marcar os
   administrativos de obra (Pessoas › "Acesso no aplicativo") e testar a tela
   inicial num celular de verdade (câmera traseira e zoom ainda não conferidos).
1h. **PUBLICADO em 06/10/2026** (com o "pode" do dono): os pedidos de 06/10
   (itens 1 a 10 abaixo), com as migrações **006 e 007**. Confirmar com o dono:
   apertou "Aplicar atualizações do ponto"? escolheu a **escala padrão da
   empresa** (Configuração › Jornada)? Para a conferência do rosto: criou a
   conta AWS, desligou o uso para treino (opt-out), criou o usuário só com
   `rekognition:CompareFaces`, pôs AWS_ACCESS_KEY_ID e AWS_SECRET_ACCESS_KEY no
   Render e ligou a chave em Configuração › Fotos? Regra configurável ("só a
   primeira batida do dia" ou todas, exclusões, teto).
1g. **Três decisões em aberto com o dono (06/10/2026, "vou pensar")**:
   (a) prazo para entregar atestado (hoje: qualquer tempo, com o mês aberto;
   sugerido 48 h); (b) atestado de HORAS / declaração de comparecimento (hoje
   só dias inteiros); (c) documento obrigatório na licença (hoje opcional).
   Também publicado em 06/10: "Alterar"/"Reativar" aparelho aprovado ou
   bloqueado (aprovar de novo; grupo mantido se não redigitado), CPF do dono
   já preenchido ao aprovar o celular de quem entrou nele, e o código de 6
   letras visível depois de entrar. Sem migração nova.
   ⚠️ Fora do ponto: `test_painel_cenario.py::test_quem_esta_preso_nao_abre_nem_grava_os_parametros`
   falha quando roda depois de `test_painel_projeto_das_obras.py` (dado que
   sobra entre testes) — avisado ao dono para levar ao chat do painel.
1f. **Modo de teste PUBLICADO em 06/10/2026** (com o "pode" do dono). Traz a
   migração **005**: confirmar com o dono se ele apertou "Aplicar atualizações
   do ponto". Combinado: no dia de começar o ponto com a equipe, apagar tudo do
   teste (obra TESTE-PONTO, pessoa e aparelhos de teste, batidas) e recomeçar a
   numeração das batidas do 1 — só com o "pode" dele nesse dia.
1e. **PUBLICADO em 05/10/2026** (com o "pode" do dono, `main` em `efd509b`):
   base de pessoas em dia sozinha, período de contrato, base de obras da
   C. Diários, forma de bater por exceção e as travas do aparelho da obra.
   Confirmar com o dono se ele apertou "Aplicar atualizações do ponto" (a
   **004**) e se a primeira leitura da C. Diários deu certo (Configuração ›
   Base de obras). Até o botão, vale o comportamento antigo (obras do ERP;
   celular aprovado bate).
2. **A migração 001 em produção**: na publicação da fase 1 ficou com o dono
   (Shell do Render). Se ainda não rodou, o botão do item 1 aplica as duas.
3. **Configurar para começar a usar**: cadastrar as escalas reais e atribuir a
   cada pessoa (sem escala, o espelho não julga falta); conferir os feriados
   estaduais e municipais das obras; marcar o regime de banco de horas de quem
   tem (com o acordo anexado); pôr os telefones do resumo diário; marcar "todas
   as obras" no cadastro do DP no ERP.
4. **Escolher a obra do piloto** (Mobponto e ponto novo juntos por um mês).
5. **Contabilidade/advogado**: registro do REP-P, termo de responsabilidade,
   convenção coletiva da construção (pode mudar tolerância, banco e intervalo).
6. **Fase 3**: AFD/AEJ, iDFace, a folha da Análise de SPs lendo daqui, expurgo
   de fotos por prazo, desligar o Mobponto.

## 10/10/2026 (noite) — Documento com um botão, ajuste de batida sem roleta, e as licenças conferidas na lei

O dono: *"no iPhone, escolher arquivo já oferece fototeca, tirar foto ou escolher
arquivo (…) é melhor deixar só esse botão"*; e o ajuste de quem esqueceu de
bater: *"vai ter que colocar um por um (…) o horário rola para cima e para
baixo, muito lento (…) tem que buscar do cadastro (7, meio-dia, 13, 17; sexta
16)"*. Depois: *"da onde você tirou esses tipos de licença? (…) é da lei? A
gente tem como editar, adicionar, remover?"*

- **Documento: um botão só** ("📎 Anexar o documento — foto ou arquivo"), que
  abre as opções do próprio celular (câmera, fotos, arquivo). O "Fotografar com
  a câmera" do ponto da obra saiu. A foto é reduzida no aparelho (lado maior
  1800 px, JPEG) antes de subir — a de 12 MP passava do limite; imagem aceita
  até 25 MB na escolha, PDF até 10 MB.
- **O ajuste abre o dia.** No ponto da obra, escolhido o dia, a tela traz o
  que a pessoa já bateu, a escala e **uma linha por horário que falta, já
  marcada e com o horário da escala** (sexta com 16:00). Desmarca o que não
  quer, corrige o que for diferente. A hora é **digitada** (teclado de
  números; "1705" vira 17:05), nada de roleta. Vários horários de um dia num
  envio só (intercalado também), e depois **"Pedir outro dia"**, que abre no
  dia anterior, sem se identificar de novo. Sem escala, aparecem as linhas em
  branco (entrada, saídas e voltas). Rotas novas: `/app/api/tablet/dia` e
  `/app/api/tablet/ajuste-do-dia` (o mesmo bilhete de 10 min do pedido).
  O "Corrigir" do celular e da consulta usa as mesmas linhas, e o erro de
  horário aparece dentro da janela, sem fechá-la.
- Pedido entregue no ponto da obra **não volta mais sozinho para a tela
  inicial**: continua no modo de entrega para a próxima pessoa.
- **Licenças: a lista é a da lei** (CLT art. 473 e leis próprias, migração
  006), editável em ERP › Ponto › Configuração › Jornada e licenças: "ajustar"
  (dias, vezes por ano, documento, obrigatório, situação) e "+ Licença da
  convenção". Remover é pôr em "Desligada" — não apaga, para os pedidos antigos
  manterem o nome. Na conferência de hoje, dois itens estavam atrás da lei, e a
  **010** os corrige (só se ninguém os ajustou pela tela): pré-natal do pai de
  2 para **até 6 consultas ou exames** (Lei 14.457/2022); paternidade continua
  5 dias em 2026, com a base legal dizendo a escala da Lei 15.371/2026 (10 em
  2027, 15 em 2028, 20 em 2029) — **subir em janeiro de 2027 pela tela**.
  Fica de fora: o inciso IX do art. 473 (representante sindical em reunião
  internacional), raro demais para estar na lista.

## 10/10/2026 (tarde) — No ponto da obra, todo mundo entra primeiro

O dono, testando a versão da manhã: *"Todo mundo tem que logar (…) se eu for
logar, vai entender quem é você — a pessoa que tem esse modo de bater ponto em
todo mundo tem acesso a todas essas telas (…) como eu já loguei, por que preciso
logar de novo?"* E: "consultar o ponto de uma pessoa já não está selecionável —
tem a ver com a geolocalização? Não deveria".

- **O ponto da obra abre na entrada com CPF e PIN** (só o responsável pelo
  aparelho e o administrativo entram nele). Entrou, tem as cinco opções — bater
  para todos, atestados, consultar pelo CPF, QR por WhatsApp e o "Meu ponto"
  dele, sem entrar de novo. **"Sair"** no alto e no fim da tela inicial.
- **A consulta não depende da localização.** O aparelho alcança a obra em que
  está (se houver localização), as obras fixadas nele na aprovação, e as obras
  em que ELE registrou batida nos últimos 30 dias (`papeis.DIAS_DO_APARELHO`).
- **A batida não desloga quem entrou** — fica aberta o dia todo, sem sair
  sozinha. Para sair dela ("‹ Início") pede o PIN de quem entrou (só o PIN):
  ela fica na frente de todo mundo, e o início abre a consulta. Escolha minha,
  dita ao dono. Reiniciado na batida, o aparelho volta para a batida; o início
  dali leva à entrada.
- Fora da batida, 3 minutos sem uso saem sozinhos (volta para a entrada).

## 10/10/2026 — O ponto da obra é do aparelho: consulta pelo CPF, QR na tela inicial, nada de caixa de obra

O dono testou de novo e listou:

1. **"Só de abrir uma aba anônima já pede para cadastrar o aparelho."** O
   aplicativo se registrava como aparelho novo ao abrir — cada navegador limpo
   virava um pedido de aprovação. Agora NÃO se registra sozinho: quem bate no
   próprio celular registra ao entrar (como já era, com o nome); o ponto da
   obra, pelo botão "Este aparelho vai ser o ponto da obra? Pedir a aprovação"
   da tela de entrada, que mostra o código.
2. **A caixa de obra continuava no ponto da obra** — era a escolha entre obras
   SEM coordenada (o aparelho sem lista de obras vale para todas, e muitas da
   C. Diários ainda não têm coordenada). Saiu: o ponto da obra só bate na obra
   que a localização identifica; obra sem coordenada, ele recusa (o servidor
   também). **Para o ponto da obra funcionar numa obra, ela precisa da
   coordenada na coluna AM.**
3. **O aviso não quebrava a linha** — corrigido.
4. **"Esqueci meu QR Code" saiu da tela de batida** ("aqui é para sair batendo o
   povo") e virou opção da tela inicial do ponto da obra: "Receber o QR Code no
   WhatsApp — o seu ou o de outra pessoa, pelo CPF". No celular da pessoa, a
   tela inicial ganhou "Meu QR Code".
5. **Consulta no ponto da obra é PELO CPF, sem PIN** ("que PIN é esse? (…) é para
   colocar só o CPF; se tiver alguma relação com aquela pessoa (…) consegue
   visualizar"). O aparelho, por ele mesmo, alcança quem bateu na obra em que
   ele está (ou é dela no cadastro) — `papeis.alcance_do_aparelho`. Digita o CPF
   (`POST /ponto/app/api/equipe/cpf`), abre a folha, e no dia corrige e lança
   (origem `APARELHO`, "no aparelho …"). Nunca uma lista de nomes, e o número
   da pessoa sozinho não abre nada: a folha só abre para o CPF digitado ali nos
   últimos 10 minutos. Some sozinho para o início em 3 minutos sem uso. O
   **risco, dito ao dono**: quem estiver na frente do aparelho e souber o CPF de
   um colega vê o mês dele e pode lançar pedido em nome dele (o pedido vai para
   validação e fica registrado como feito no aparelho).
6. **"Ele está meio que misturando os modos"**: no ponto da obra, as opções da
   tela inicial passaram a ser do APARELHO e não mudam com quem entrou; quem
   entra (responsável ou administrativo) só ganha o "Meu ponto — Fulano".

## 09/10/2026 (tarde) — O ponto da obra só pela localização, e o QR que não chegava

1. **No ponto da obra, a caixa de obra saiu** (o dono: "se já identificou a
   obra, não tem que selecionar mais nada"). A obra é a da localização, escrita
   no alto. **Sem localização ou fora da área, o ponto da obra não bate** ("o
   celular ponto de obra (…) só é para bater com a geolocalização") — mudou o
   que valia desde 03/10 (sem localização ia para conferência). A tela avisa em
   vermelho, relê a localização a cada 30 s, e uma leitura que falha não apaga a
   boa de até meia hora. Única escolha que sobra: entre obras do aparelho SEM
   coordenada cadastrada (não há como detectar; a batida vai para conferência).
2. **No celular da pessoa, sem localização também se bate** escolhendo a obra e
   explicando — vai para conferência ("o individual pode permitir que você bata
   sem a localização ou fora da localização, contanto que selecione a obra e
   justifique"). Isso DESFAZ o "não devíamos permitir o ponto sem localização"
   do mesmo dia, de manhã — avisado ao dono. A caixa de obra só aparece quando
   a obra não foi identificada.
3. **"Precisei pedir o QR umas três vezes para chegar"**: a fila de WhatsApp é
   acordada pelas requisições do ponto (o serviço não tem relógio), no COMEÇO
   de cada uma e no máximo uma vez por minuto — ANTES de o pedido ser gravado.
   Olhava, não achava nada, e só voltava a olhar um minuto depois, se viesse
   outra requisição; o pedido feito pela tela do ERP nem acordava. Agora o
   pedido acorda a fila logo depois de gravado (`envios.enviar_agora`), nos três
   lugares: "Esqueci meu QR" no ponto da obra, "Receber no WhatsApp" no Meu ponto
   e o botão do ERP. Teste: `test_pedir_o_qr_uma_vez_basta`.

## 09/10/2026 — Bater fora da obra (Portaria 671), nada sem localização, e o aparelho com nome

Pergunta do dono: pela Portaria 671, a batida tem de ser aceita fora da obra?
Resposta dada: a Portaria proíbe restringir HORÁRIO, marcação automática,
autorização prévia de hora extra e alterar o registrado — restringir LUGAR não
está na lista; o risco é a jornada fora da obra ficar sem registro num processo.
Primeiro propus uma marcação por pessoa ("pode bater fora da área"); o dono
resumiu a regra para TODOS, e a marcação foi desfeita antes de publicar:

> *"Não tá na obra. Alerta! E se a pessoa ainda for bater, explicar o motivo e o
> ponto ir para conferência e o sistema avisar isso. E a pessoa precisa poder
> visualizar isso no meu dia, que aquela batida ainda não está válida."*

- **Fora da obra, no celular:** a tela avisa "⚠️ Você NÃO está na obra" (com a
  distância); para bater mesmo assim, a pessoa escolhe a obra e EXPLICA o motivo
  — sem explicação, o servidor recusa. Com ela, a batida entra EM CONFERÊNCIA,
  com a localização gravada e o motivo ("bateu explicando o motivo" + a
  explicação), e vai para a fila de Validações da administração da obra.
- **Sem localização, o celular nunca bate** — nem na obra que aceita sem
  explicação. O **ponto da obra** sem localização continua aceitando, para
  conferência: recusar pararia a equipe inteira por causa do GPS de um aparelho
  já preso à obra (escolha minha, dita ao dono). O ponto da obra FORA da cerca
  (o aparelho saiu da obra) continua recusado.
- **A batida em conferência aparece no dia**: ⏳ antes do horário, no "Hoje" e
  no "Meu mês", com "ainda não vale até a administração da obra validar".
- A escolha por obra em Cerca das obras passou a se chamar "Só com explicação
  da pessoa" (o antigo BLOQUEAR, padrão) e "Aceitar sem explicação" (ANALISAR).

**O aparelho chegava "sem nome" na aprovação.** Desde a tela inicial (08/10), o
login não passava mais pelo trecho que dava ao celular o nome de quem entrou
(ficou só na tela "Hoje"). Agora o servidor faz isso NA ENTRADA com CPF e PIN (e
na criação do PIN): o pedido chega "Celular de Fulano (CPF final 123)". Os
celulares de quem não bate no celular nem entram na fila (regra de 05/10).
**A pessoa que já tinha celular aprovado e pediu de novo:** quase certamente
apagou os dados do navegador (ou reinstalou o aplicativo) ao mexer na
localização — o identificador do aparelho mora ali, e o celular vira "novo". O
pedido agora diz "já tem um celular aprovado desde … — provavelmente trocou de
celular ou apagou os dados do navegador; aprovar este bloqueia o anterior". Não
se aprova sozinho: com o PIN de alguém, outro celular bateria por ele. O
aplicativo também pede ao navegador para não apagar esses dados por conta
própria, e a tela de "esperando aprovação" explica o caso.

## 09/10/2026 — Seis ajustes do teste do dono (e um defeito de fuso achado no caminho)

1. **Nome da obra repetido** ("TESTE-PONTO — TESTE DO PONTO (obra de teste)").
   A regra que junta código e nome passou a reconhecer quando TODAS as palavras
   do código estão no nome — aí basta o nome. Vale no aplicativo
   (`rotuloObra`), no comprovante (`cadastros.rotulo_obra`) e no ERP inteiro:
   o `rotuloObra` do `erp_base.html` ganhou a mesma regra (antes, só "um
   contém o outro").
2. **"Encarregado" virou "administração da obra"** em todo texto que a pessoa
   lê ("pode ser o encarregado, o almoxarife, o DP"), inclusive as opções de
   Quem valida ("Administração da obra", "Administração da obra e depois o
   DP"). O valor gravado continua `ENCARREGADO`.
3. **"Bater e mandar para conferência" era recusado** ("fora da área da obra,
   16 mil km"). A tela oferecia a lista de obras sempre que não achava a
   cerca, mas a obra BLOQUEIA fora dela (o padrão desde 04/10) — prometia o
   que o servidor não fazia. Agora a lista só traz as obras em que a batida
   será aceita (as que mandam para conferência, em Configuração › Cerca das
   obras, e as sem coordenada); se nenhuma, o botão de bater fica desligado e
   a tela diz por quê. A regra de bloquear NÃO mudou. Distância acima de
   100 km ganha o aviso "confira a localização do celular e a coordenada da
   obra" — 16 mil km não é "fora da cerca", é coordenada ou localização errada
   (não deu para saber qual: conferir o mapa da obra de teste).
4. **O pedido pendente aparece no dia**, no "Meu mês" e na folha da consulta:
   o horário pedido vira um chip ⏳ tracejado ("Ajuste pedido — esperando
   validação"); atestado e licença, um selo âmbar. O horário pedido já não
   aparecia como "faltando", mas a pessoa não via que tinha pedido.
5. **Corrigir sem escala mostrava "Batida 1 / Batida 2"**: agora oferece as
   que ainda cabem no dia, com o nome (Entrada, Saída para o intervalo, Volta
   do intervalo, Saída).
6. **Obra que só bate entrada e saída** (convenção — intervalo PRÉ-ASSINALADO,
   CLT art. 74, § 2º): migração **009** (`obra_config.intervalo_pre_assinalado`),
   marcada em Configuração › Cerca das obras › "Batidas por dia: 2". Na obra
   marcada, o dia com só a entrada e a saída fica completo, a escala espera duas
   marcas, e a conta desconta o intervalo da escala (sem isso, o almoço viraria
   hora extra e alerta de intervalo curto). Quem bater as quatro, conta as
   quatro. A regra olha a obra da primeira batida do dia.

**Defeito achado ao testar o item 6:** o espelho entregava à apuração a hora
das batidas em UTC (como o banco devolve), e a apuração conta minutos desde a
meia-noite do fuso da batida. As horas trabalhadas saíam certas (são
diferenças), mas **o atraso, a tolerância de 5/10 min e a hora noturna saíam 3 h
deslocados** — toda entrada às 7h aparecia como 3 h de atraso, e quem bateu às
19h ganhava hora noturna. Corrigido no `espelho.py` (`horario.para_local`), com
teste que trava. O ponto ainda não está em uso real com a equipe, então não há
espelho fechado errado; o que estava na tela se recalcula sozinho.

Testes: `tests/test_ponto_duas_batidas_banco.py` e dois casos novos em
`test_ponto_apuracao.py`.

## 08/10/2026 — A tela inicial, a consulta pela cerca da obra e o administrativo de obra

O dono descreveu a dinâmica inteira (ainda sem ter testado a de 07/10) e pediu
para conferir e apontar furos. Feito assim — regras em `core/papeis.py`, rotas
em `app_obra.py`, migração **008** (`pede_no_celular` e `administrativo_obra`
em `colaborador_config`; origem de pedido `RESPONSAVEL`):

1. **Tela inicial em todo aparelho** ("tem que ter uma tela inicial, e dali a
   gente é direcionado"): Bater ponto · Atestados, afastamentos e ajustes ·
   Consultar o ponto das pessoas · Meu ponto. Cada opção aparece conforme o
   aparelho e a pessoa (`GET /ponto/app/api/inicio`); a que não vale aparece
   apagada, dizendo por quê. Toda tela tem "‹ Início". Os links pequenos de
   baixo da tela da batida (de que ele não gostou) saíram: entregar atestado
   e "Meu ponto" foram para a tela inicial; ficou só "Esqueci meu QR Code". O
   ponto da obra reiniciado volta direto para a batida se era nela que estava.
2. **Consulta das pessoas da obra**, com busca por nome e mês a mês. Aparece
   quem **bateu ponto naquela obra no mês** (mesmo que tenha batido outros dias
   em outra) **e quem é da obra no cadastro** — o segundo grupo é o furo de quem
   passou o mês de atestado ou chegou hoje (sem batida, não haveria como lançar
   nada para ele). Fora do alcance, a pessoa responde "não encontrada".
3. **Na folha da pessoa, o dia leva ao pedido**, por dois ÍCONES ao lado do dia
   (o dono achou os textos "gigantes"): ✏️ corrigir a batida que faltou (o
   mesmo ajuste do dia, já com o que falta) e 🗓️ justificar a ausência
   (atestado, licença, afastamento, compensação). O 🩺 da primeira versão saiu
   porque "fica muito restrito à saúde" (um exame do Detran, por exemplo); entre
   📎, 📄 e 🗓️, o dono escolheu o calendário. A legenda aparece uma vez, no topo da lista. Vale também no
   "Meu mês". O pedido sai **em
   nome de quem lançou** (origem `RESPONSAVEL`) e vai para a validação de
   sempre — ninguém no aplicativo aprova nada. Atestado aparece como
   "Atestado", sem CID nem documento (só o DP vê).
4. **Quem consulta** ("o que vai dar poderes é ele ser administrativo de obra,
   e o celular de obra vai ter os mesmos poderes"):
   - **Ponto da obra**: bater e entregar atestado continuam SEM login (como
     antes). Consultar e pedir por outros pedem **CPF e PIN do responsável
     pelo aparelho ou de um administrativo de obra** — decisão minha, avisada
     ao dono: sem isso, qualquer um na frente do aparelho veria a folha de
     todos. Vale a obra cuja cerca contém o aparelho. Sai sozinho em 3 min.
   - **Administrativo de obra** (marcação na ficha da pessoa, ERP › Ponto ›
     Pessoas › "Acesso no aplicativo"): em qualquer aparelho — o celular dele,
     o ponto da obra, o computador. Vale a obra cuja cerca contém o aparelho
     agora **e as obras em que ele bateu ponto nos últimos 7 dias** — é o que
     vale no computador, a qualquer hora. (A primeira versão só valia com o
     ponto dele aberto naquele momento; o dono afrouxou no mesmo dia: "pode
     afrouxar mais (...) se precisar lançar algo fora do horário, deixa".)
     Mudou de obra, a antiga sai sozinha uma semana depois da última batida
     lá. O próprio ponto ele vê sempre, e pede por si.
   - **Ponto de equipe**: o responsável vê a equipe da lista (sem cerca — a
     equipe anda); pede por ela só com a marcação "faz pedidos pelo celular".
   - **Celular pessoal**: vê o próprio ponto; pede pelo celular só com a
     marcação "faz pedidos pelo celular" — **separada** da de "bate no próprio
     celular" (antes, uma dava a outra). Antes da 008, vale a regra de 06/10.
5. **O que NÃO mudou, de propósito**: para o celular de alguém **bater pelos
   outros** (QR/CPF), ele continua sendo aprovado como **Ponto da obra**, com a
   pessoa de responsável. A marcação de administrativo dá consulta e pedidos,
   não a batida pelos outros — que é o poder mais sensível e passa pela
   aprovação do RH (e vence em 90 dias). O ponto da obra também passou a
   aceitar o login do administrativo (antes, só o do responsável).

Furos que ficam, ditos ao dono: localização falsa (aplicativo que finge GPS)
engana a cerca da consulta como engana a da batida — o que segura é tudo ficar
em nome de quem fez e o pedido passar pela validação; e o administrativo que
tem acesso às obras em que bateu na última semana, a qualquer hora e de
qualquer lugar pelo computador (escolha do dono; o PIN dele é a única chave ali).

Conferido no navegador simulado: a tela inicial do ponto da obra (sem e com
login), a batida com "‹ Início", a consulta com o administrativo (lista, folha,
corrigir o dia, lançar atestado e licença), e a tela do João sem marcação
nenhuma (só "Meu ponto"; as outras apagadas com o motivo). **Não conferido** num
celular de verdade nem num computador de verdade (o navegador do computador
pode pedir a localização; recusada, valem as obras das batidas da semana).

Testes: `tests/test_ponto_papeis_banco.py`.

## 07/10/2026 — Responsável em todo aparelho, "Ponto da obra" e os defeitos do primeiro teste de verdade

O dono testou o cadastro e o modo da obra num celular e mandou a lista. Tudo
corrigido, sem migração nova:

1. **"Salvo e ele nem salva" / o grupo volta para tablet / os CPFs somem ao
   reabrir.** Duas causas. (a) O erro de validação ia para o aviso no alto da
   página e a janela FECHAVA — parecia que não tinha salvo. Agora a janela
   reabre com o erro dentro dela e com o que foi digitado. (b) "Alterar" abria
   em branco, porque a lista de aparelhos não traz o grupo: rota nova
   `GET /erp/api/ponto/dispositivos/<id>` devolve o aparelho com o grupo (nome
   e CPF) e a janela abre preenchida — tipo, responsável, grupo, prazo, obras.
   Conferido no navegador: salvar como ponto de equipe e reabrir mostra o tipo,
   o responsável e o grupo.
2. **Todo aparelho tem RESPONSÁVEL** (o dono: "tem que ter o CPF do dono, para
   saber quem está com aquele celular… acho que sim"). A coluna
   `dispositivos.colaborador_id` deixou de ser só o dono do celular pessoal: é
   o responsável de qualquer tipo. A tela do ERP recusa salvar sem ele ("diga o
   CPF do responsável"); ao alterar, vale o de antes se não redigitado. No ponto
   de equipe o responsável entra no grupo sozinho. A integração antiga (pela
   chave) continua aprovando ponto da obra sem responsável. **Responsável
   desligado bloqueia o aparelho** (não só o celular pessoal): ninguém responde
   mais por ele; reativa-se aprovando de novo com outra pessoa. Escolha minha,
   avisada ao dono — o risco é a obra ficar sem bater até o RH trocar o
   responsável.
3. **O nome "tablet" saiu.** O dono: "não tem tablet… é o celular da obra", e
   depois: "às vezes o celular não é da empresa — pode ser o telefone próprio
   de alguém designado". Os três tipos passaram a se chamar **Ponto da obra**
   (todos da obra batem; pode ser o celular da empresa ou o de alguém
   designado), **Celular pessoal** (só o dono bate) e **Ponto de equipe** (só
   a lista; passageiro). Nos valores gravados nada mudou (COMPARTILHADO,
   INDIVIDUAL, LISTA).
4. **Quem está com o ponto da obra quer ver o próprio ponto.** No ponto da obra
   e no de equipe, **só o responsável** entra no "Meu ponto" (botão "Meu ponto"
   na tela da batida); qualquer outro CPF é recusado ("este é o ponto da obra —
   só o responsável por ele entra"). Dentro, um aviso lembra que é o ponto da
   obra, com "Voltar para a batida agora"; sem uso por 3 minutos, volta
   sozinho. Antes disso ninguém entrava (regra de 05/10). Conferido no
   navegador com duas pessoas.
5. **Obra não detectada no modo da obra, nome duplicado.** O aparelho agora
   escolhe sozinho a obra cuja cerca o contém (raio + precisão do GPS, até
   150 m a mais) e diz na linha de cima "Obra detectada pela localização:
   CÓDIGO — Nome"; fora da cerca, pede para conferir. O nome aparece uma vez só
   quando a obra não tem nome diferente do código. Quem decide de verdade
   continua sendo o servidor, pela cerca, a cada batida.
6. **Toque rápido no teclado do CPF dava zoom** ("33" rápido virava zoom). A
   página do ponto não deixa mais ampliar e os botões do teclado não esperam o
   duplo toque. Conferido no navegador simulado (dois toques = "33", escala 1).
   **Não conferido num celular de verdade.**
7. **Escolher a câmera.** Botão de trocar frente/traseira na tela da batida; o
   aparelho lembra a escolha. **Não conferido com câmera de verdade** — no
   navegador simulado só há uma.

Pergunta do dono respondida, sem mudança: o celular pessoal da EXCEÇÃO (quem
bate no próprio celular) manda atestado, licença e ajuste por ele — foi a
decisão de 06/10 ("só quem pode bater faz pedido"). Oferecido restringir.

Testes: `tests/test_ponto_responsavel_banco.py` (CPF obrigatório, alterar com
o grupo, só o responsável no "Meu ponto" do ponto da obra, responsável
desligado bloqueia). Os testes que aprovavam aparelho pelo ERP sem CPF passaram
a mandar o do responsável.

## 06/10/2026 — Oito pedidos de uma vez: acesso de quem sai, falta no dia seguinte, grupo temporário, licenças da lei, quem faz pedido, fraude na hora, submenu

Pedidos do dono no mesmo dia (todos feitos, migração **006**):

1. **"O acesso precisa ser bloqueado automaticamente quando o colaborador for
   desligado."** (`core/desligamentos.py`) A entrada e a batida de desligado já
   eram recusadas; agora, a cada 15 min e na rotina do dia, o celular pessoal é
   BLOQUEADO, os QR são cancelados, os WhatsApps de QR na fila cancelados e a
   pessoa sai dos aparelhos de grupo. A sessão aberta cai também para quem foi
   marcado "não bate ponto".
2. **"Falta de batida de um dia, no dia seguinte o sistema já deveria apresentar
   como falta."** Já apresentava — para quem TEM escala. Faltava a escala: agora
   há a **escala padrão da empresa** (Configuração › Jornada), que vale para quem
   não tem escala própria (só semanal; 12x36 depende do dia de início).
3. **Grupo ("bater ponto de algumas pessoas") com caráter precário.** Vale até
   uma data (padrão 15 dias, máximo 90); vencido, para de bater. Validações
   mostra "Aparelho de grupo a rever" e SUGERE cancelar quando: venceu ou vence
   em 3 dias; nenhuma batida há 7 dias; gente do grupo que não bate nele há 7
   dias. Botões: cancelar, renovar, tirar quem não usa.
4. **"Base de dados de afastamentos permitidos por lei."** `ponto.tipos_licenca`
   com o CLT art. 473 (casamento 3, luto 2, doação de sangue 1/ano, alistamento
   2, pré-natal 2/ano, filho ao médico 1/ano, exame de câncer 3/ano, juízo,
   serviço militar, vestibular), paternidade 5 (CF) e mesário (Lei 9.504). O
   pedido escolhe o tipo; dias, limite e documento conferidos. O DP ajusta e
   cria tipo da convenção (Configuração › Jornada e licenças). Maternidade e
   INSS ficaram como AFASTAMENTO (DP).
5. **"Só quem pode anexar esses documentos são os responsáveis da obra, os
   mesmos que batem o ponto da obra, ou o celular da obra (…) ou aquele que bate
   ponto de algumas pessoas"** e **"o ajuste só pode ser solicitado por quem tem
   permissão de bater ponto; mesma coisa para compensações"**:
   - no próprio celular, pedido só de quem é EXCEÇÃO (bate nele); os outros veem
     "entregue no aparelho da obra ou pelo responsável";
   - no aparelho da obra e no de grupo: botão "Entregar atestado, licença ou
     pedir ajuste" → identifica por QR/CPF (bilhete de 10 min que NÃO bate) →
     formulário com o documento fotografado pela câmera (`/app/api/tablet/pedido`,
     origem APARELHO);
   - no ERP, o encarregado (tratar ponto) entrega atestado e licença com
     documento (Espelho › "Entregar atestado ou licença"); o DP valida.
     Férias, afastamento e abono continuam só do DP.
6. **Fraude** (pergunta do dono, respondida na conversa): a foto escura/lisa e a
   fila rápida no mesmo aparelho (5 pessoas diferentes, menos de 10 s entre
   cada) passam a mandar a batida para conferência NA HORA (antes, só o alerta
   do dia seguinte). Quem passou por fila rápida identificado por QR do WhatsApp
   tem o QR TROCADO na rotina do dia seguinte. ⚠️ Mudança de decisão: antes,
   "foto ruim é alerta, não análise"; agora vai para análise.
   **Em aberto, decisão dele:** a leitura das fotos por IA (pessoa diferente da
   foto cadastral, foto que não é de gente) — custo por foto; proposta de piloto
   numa obra com teto de gasto.
7. **Submenu da Configuração** (Geral, Pessoas, Obras e cerca, Aparelhos, Jornada
   e licenças, Quem valida, Mensagens, Teste), com a seção no endereço (#aparelhos).
8. Um celular pessoal por pessoa (aprovar o novo bloqueia o anterior);
   "Alterar"/"Reativar" aparelho; CPF do dono já preenchido.

9. **"Renovar a licença de quem bate a cada 90 dias, inclusive do celular da
   empresa"**, com o aviso para quem tem o aparelho e para o RH. TODO aparelho
   ganha `valido_ate` (90 dias; o de grupo, 15) e se renova em Validações
   ("Aparelho a renovar ou desativar"), em Configuração › Aparelhos ("renovar")
   ou no alerta "Aparelho com a liberação vencendo" (vai no resumo diário). O
   próprio aparelho mostra a faixa "para de bater em X dias" a partir de 15 dias
   (o de grupo, 3); vencido, para de bater. Celular ou tablet sem batida há 30
   dias aparece com a sugestão de desativar. A migração 006 dá 90 dias a quem já
   está aprovado.

10. **Conferência do rosto pela AWS — decidida pelo dono** (*"acho que devemos
   ativar a análise via AWS. Hoje pago 2 mil reais pelo ponto que usamos (…)
   devemos permitir colocar padrão para tudo, excluir alguma obra ou algum
   horário ou dia"*). `core/rosto.py`, migração **007**
   (`ponto.conferencias_rosto`), cartão Configuração › Fotos:
   - Amazon Rekognition CompareFaces, foto da batida × foto cadastral; de 10 em
     10 minutos, nas batidas com foto dos últimos 3 dias;
   - padrão: TODAS; exclui obras, dias da semana, faixas de horário (inclusive a
     que vira a noite) e "só a primeira batida do dia";
   - outra pessoa (semelhança < 90 %, ajustável) ou sem rosto → batida vai para
     conferência + alerta urgente "Rosto não confere";
   - sem foto cadastral → a da 1ª batida vira a cadastral (grátis, "confira no
     mosaico");
   - teto do mês (padrão US$ 10), gasto do mês e estimativa da regra na tela.
   **Custo (preço público de 06/10/2026):** US$ 0,001 por foto; 1.000 grátis/mês
   no 1º ano. 100 % das fotos ≈ 42 mil/mês ≈ US$ 42 (≈ R$ 230). "Só a primeira
   do dia" ≈ US$ 10 (≈ R$ 55) — é o que fica perto dos R$ 50 que o dono citou.
   **Depende do dono:** criar a conta AWS, desligar o uso de conteúdo para
   treino (opt-out de IA), gerar a chave só com permissão de Rekognition, pôr
   AWS_ACCESS_KEY_ID e AWS_SECRET_ACCESS_KEY no Render (nunca no chat), ligar a
   chave na tela; e o aviso de dado biométrico no termo do ponto (LGPD art. 11).
   Dependência nova `boto3` (avisada). Não verificado: a chamada real à AWS (sem
   conta aqui) — o formato foi conferido com o simulador da própria biblioteca.

**Testes:** `tests/test_ponto_regras_0610_banco.py`. A limpeza dos testes não
apaga mais `tipos_licenca` (é dado de migração).

## 05/10/2026 — O modo de teste

Pedido do dono: *"queria fazer teste comigo mesmo (…) se tivesse algum canto que
eu pudesse gravar uma geolocalização e colocar meu nome e CPF"*. Sem isso,
testar exigia uma obra falsa na C. Diários (que o painel, a emissão de NFS-e e a
Análise de SPs também leem) e a pessoa no Registro.

**O que foi feito** (`core/ensaio.py`, migração **005**, cartão "Modo de teste"
em Ponto › Configuração, só para quem configura o ponto):
- **Obra de teste** `TESTE-PONTO`, gravada no lugar onde a pessoa está ao
  apertar o botão (a localização do navegador; aviso quando a precisão passa de
  100 m). Status `TESTE_PONTO` no ERP — as listas de obra ATIVA não a mostram;
  no ponto ela vale em qualquer base (`obra_config.ensaio`).
- **Pessoa de teste**: nome, CPF, celular. CPF novo é criado no cadastro de
  colaboradores do ERP; CPF existente só ganha a obra de teste. Bate como ATIVA
  fora do Registro (`colaborador_config.ensaio`) e também no próprio celular.
- **Código de primeiro acesso na tela**, sem WhatsApp, só para pessoa de teste
  (404 para as demais). Vale o ÚLTIMO código gerado: pedir no celular primeiro,
  depois apertar o botão.
- **Desligar o teste** tira a obra do ponto; as batidas ficam (registro de ponto
  não se apaga).

**Fica com ele:** o celular de teste ainda precisa ser aprovado em Validações
(de propósito: é o fluxo real); a pessoa de teste aparece no cadastro de
colaboradores do ERP.

## 05/10/2026 — As obras vêm da C. Diários, só o aparelho da obra bate, e a revisão de brechas

Pedidos do dono, no mesmo dia: *"Quero utilizar temporariamente as obras de
C. Diários. Depois vamos usar o cadastro do ERP. Vou adicionar uma última coluna
com as coordenadas (…). Na coluna V tem o status da obra. Não exiba obra que
estão como 'Concluída', 'Concluída com Dívida' ou 'Distratada' (…) O cadastro das
obras precisa estar sempre atualizado"*; *"Coluna AM, o cabeçalho é
'Coordenadas Geográficas'"*; e *"em relação ao padrão de batida é só o aparelho
da obra que bate. Vamos cadastrar apenas as exceções. Banco de horas, mesma
coisa: a princípio ninguém tem, vamos cadastrar as exceções."*

**Base de obras (`core/base_obras.py`, migração 004 `ponto.obras_planilha`).**
Mesmo desenho da base de pessoas: a aba "C. Diários" da planilha "Bases de
Dados Pipefy" (a mesma do painel, `PAINEL_SHEET_PROJETOS`, com o id conhecido de
reserva) é copiada para o banco e o ponto lê a cópia por cima do ERP. Lida de 2
em 2 horas (6h–20h) numa linha separada, e pelo botão "Ler a planilha agora".
Chave para voltar ao ERP. Enquanto a planilha não foi lida nenhuma vez, vale o
ERP — nunca esvazia a lista.

**Decisões tomadas sem ele, com motivo:**
- **Status pela POSIÇÃO (coluna V)**, porque foi o que ele disse; a coordenada
  pelo TÍTULO ("Coordenadas Geográficas"), com a AM de reserva. A tela mostra de
  qual coluna e título cada coisa foi lida — se a V não for o status, aparece ali
  (lista "Status encontrados").
- **Escondida = status começando por "Conclu" ou "Distrat"**, sem acento e sem
  caixa: cobre as três palavras e variações ("Concluída c/ Dívida"). Status vazio
  APARECE.
- **Obra do ERP que não está na planilha some do ponto** ("usar as obras de
  C. Diários"). O cartão conta quantas são ("Ativas no ERP fora da planilha").
- **Obra ativa da planilha que falta no ERP é CRIADA no ERP** com código e nome
  (a escrita que o importador já fazia): a batida precisa de uma linha de obra
  para se pendurar. ⚠️ Ela aparece no cadastro de obras do ERP. Encerrada que
  falta não é criada.
- **O casamento planilha → ERP**: pelo Código Primário, senão pelo código da
  coluna A (a emissão de NFS-e já aprendeu que a mesma obra tem os dois). O
  código da obra do Registro de Colaboradores também casa pelos dois.
- **Coordenada da planilha vence a do ERP**; sem ela, vale a do ERP. Nada é
  escrito na coordenada do ERP. O raio e o "bloquear/analisar" continuam no
  ponto.
- **Formato da coordenada**: graus decimais `lat, lon`. Aceita vírgula decimal
  com `;`, link do Google Maps e graus-minutos-segundos; desvira lat/lon trocados
  (com aviso). Recusa menos de 4 casas decimais (erro > 10 m), fora do Brasil e
  longitude sem o sinal de menos — cada recusa com o motivo na tela.

**Forma de bater (`core/forma_de_bater.py`, coluna `colaborador_config.
bate_no_celular`).** O padrão, que não se cadastra: só o aparelho da obra bate.
Exceção "também no próprio celular", marcada em Pessoas › Forma de bater — ou
sozinha ao aprovar o celular da pessoa como INDIVIDUAL. Desmarcar faz o celular
parar de bater na hora. Quem não é exceção não vê o botão de bater no "Meu
ponto" (vê "você bate no aparelho da obra"), não tem a localização pedida, e o
celular dele NÃO entra na fila de Validações (continua em Configuração ›
Aparelhos). A migração 004 marca como exceção quem já tinha celular aprovado —
ninguém perde o que funcionava. "Outra pessoa bate por ela" continua sendo o
aparelho de LISTA (exceção do aparelho). Banco de horas: o padrão já era "sem
banco". Cartão novo "Exceções ao padrão" lista as três exceções num lugar só.

**Revisão de brechas — fechadas agora:**
- **Aprovar o aparelho errado**: todo aparelho que abre o endereço entra como
  "Aparelho sem nome"; o celular de um funcionário aprovado como aparelho da obra
  bateria por todo mundo. Agora a tela do aparelho mostra um **código de 6
  letras** e a gestão vê o mesmo código antes de aprovar, com o aviso para
  conferir — e o alerta quando alguém entrou nele como pessoa.
- **Login no tablet da obra**: se alguém entrasse com CPF e PIN no tablet, ele
  deixava de ser da obra e mostrava o mês da pessoa a quem passasse. Agora o
  aparelho da obra abre sempre na batida (fecha a sessão) e o servidor recusa o
  login nele.
- **A fila de Validações inundada** por 400 celulares pendentes (acima).

**Brechas que ficam, e são decisão dele** (respondidas na conversa): tablet sem
GPS vai para conferência (não bloqueia); aparelho aprovado sem obra vale em
todas; status errado na planilha esconde a obra em até 2 h; batida sem internet
não existe ainda; limpar os dados do navegador do tablet o faz pedir aprovação de
novo (aprovar o novo, bloquear o antigo).

**Incidente de teste (resolvido):** depois de trazer a `main`, 4 testes do
ponto falharam só pela ORDEM dos arquivos: o "esta tabela não existe" que o ponto
guarda por 60 s (para ligar recurso novo sozinho depois do botão) ficava de um
arquivo que recriava o schema, e a base de pessoas caía no padrão. Agora aplicar
as atualizações do ponto zera essa memória — o que também faz o recurso novo
ligar na hora em produção, sem esperar o minuto.

**Não verificado:** a leitura real da planilha (aqui não há credencial do Google;
o painel já lê a mesma planilha com a mesma conta, então o acesso deve existir);
o título real da coluna V.

## 05/10/2026 — A base de pessoas se atualiza sozinha, e o ponto respeita o período de contrato

Pedidos do dono: *"como atualizo a base das pessoas? É algo que precisa ser
contínuo"* e *"não se pode bater ponto antes da data de início nem depois da data
de saída"*.

**O que foi feito:** o ponto pede à Análise de SPs, de 2 em 2 horas das 6h às 20h,
a mesma atualização do botão "Atualizar cadastro" (a leitura da planilha continua
lá, no processo separado e com a trava de lá), e cadastra no ERP quem falta a cada
15 minutos; a batida antes do início ou depois da saída é recusada.

**Decisões tomadas sem ele, com motivo:**
- **2 horas** entre cópias: admissão nova aparece no mesmo turno, sem ler a
  planilha de 3.500 linhas a toda hora. Fora das 6h–20h, nada.
- **O início é a menor data** entre "Data de Início" e "Data de Admissão": o
  diarista começa antes da carteira, e barrá-lo seria barrar trabalho de verdade.
- **O dia da saída ainda se bate** (desligado é saída que já PASSOU). A Análise de
  SPs, para pagamento, trata a saída "já chegou"; para ponto, o último dia é dia
  de trabalho. O "Último dia trabalhado" do Registro não foi usado — se for ele o
  certo para o ponto, é uma linha.
- **O pedido de cópia esbarra na trava da Análise de SPs**: se houver outra tarefa
  de lá rodando (a sincronização do dia, a carga do ponto), o ponto não insiste e
  tenta na rodada seguinte. Não toca em nada que esteja em andamento lá.

## 04/10/2026 — O Registro de Colaboradores vira a base de pessoas do ponto

Pedido do dono (ver README, "A base de pessoas"). **O que foi feito:** o ponto lê
a cópia da Análise de SPs, com o mesmo critério de lá; quem está só no ERP bate
para conferência; botão para cadastrar no ERP quem falta; chave para voltar ao ERP.

**Decisões tomadas sem ele, com motivo:**
- **Ler a cópia, não a planilha:** a Análise de SPs já guarda a aba no banco, e ler
  3.500 linhas por pergunta não cabe na instância. O preço: o ponto fica tão
  atualizado quanto o último "Atualizar cadastro" de lá (a Configuração mostra a hora).
- **A linha do ERP continua sendo a identidade:** batida, escala e pedido estão
  pendurados nela desde a fase 1; trocar isso seria refazer o banco do ponto.
- **Cadastrar no ERP é por botão, não sozinho:** cria gente no cadastro de outra
  área; quem aperta vê antes quantos são.
- **Só no ERP não recusa, vai para conferência:** recusar barraria quem trabalha e
  ainda não chegou à planilha.

**Pendente com o dono:** nada a apertar desde 05/10/2026 — a base se atualiza
sozinha (ver a entrada de 05/10).

## 04/10/2026 — Validações numa tela só, e quem valida configurável

Pedido do dono: *"todas essas solicitações precisam ser validadas (…) a gente vai
definir quem é que vai validar. Provavelmente (…) o próprio DP (…) uma tela onde
liste tudo que está pendente para validação (…) filtrar por obra, período (…) ver
os mosaicos também (…) umas coisas a gente põe para o encarregado de obra, outras
para o próprio RH, dependendo do que seja."*

**O que foi feito:** regra de quem valida, por tipo, na Configuração (padrão: DP);
a aba "Pendências" virou "Validações", com tudo numa lista, filtros, aprovação em
lote e o endereço da decisão em cada linha; rota própria do DP para batida em
conferência.

**Decisões tomadas sem ele, com motivo:**
- **O padrão mudou para o DP em tudo**, seguindo o "provavelmente o próprio DP".
  Antes, ajuste era do encarregado e compensação/folga passavam pelos dois. Os
  testes antigos agora configuram a regra antes de testar o caminho do encarregado.
- **Atestado e afastamento não se configuram**: só o DP vê dado de saúde.
- **Trocar a regra mexe na fila** (o que esperava uma etapa que sumiu vai para a
  que vale agora): sem isso, pedido ficaria preso esperando quem não decide mais.
- **Lote só aprova**: negar e rejeitar exigem motivo, um a um.

## 04/10/2026 — Ajuste de batida pelo dia, e aviso de batida fora do normal

Pedidos do dono no mesmo dia: (1) quando a lista de obras aparece, deixar claro
que "aquele não é o ponto regular (…) vai ter que ser analisado, algum erro
justificado"; (2) "como é que tá para uma pessoa solicitar um ajuste de ponto?
(…) no Mobponto, a gente fazia a solicitação via Pipefy. E o pessoal errava
muito: botava período que já tem ponto no meio batido, solicitava dia que já
tinha ponto (…) correção de um dia que é falta, já tinha sido registrada a
falta"; (3) se o encaminhamento de atestado já existe (existe, desde a fase 2).

**O que foi feito:** "Corrigir este dia" no Meu mês (só o que falta, com o
horário da escala sugerido, motivo de uma lista); o "Esqueci de bater" da aba
Pedidos passou a abrir o dia em vez do formulário em branco; travas no servidor
(ver README); aviso amarelo e justificativa obrigatória quando a obra é
escolhida na lista; o espelho do dia ganhou `previstas` e `faltando`.

**Decisões tomadas sem ele:** 30 min como "o mesmo horário"; um pedido por
horário (cada um vira uma batida e é decidido sozinho), mas enviados juntos e
recusados juntos; motivo de uma lista fixa (a lista é o que mais aparece numa
obra; "outro" exige texto). **Ficou de fora:** pedir para APAGAR uma batida
errada (bateu duas vezes, obra errada) — batida não se apaga; hoje o caminho é
o encarregado rejeitá-la quando estiver em análise. Se for comum, vale um
pedido "batida errada" próprio.

## 04/10/2026 — A cerca passa a BLOQUEAR, e a obra é detectada pela localização

Pedido do dono: *"não queremos permitir que a pessoa bata ponto fora das áreas
de obra. E quero ainda que a obra seja detectada automaticamente."* Muda a
decisão da fase 1 (fora da cerca entrava para análise).

**O que foi feito:** a obra da batida é a da cerca em que o aparelho está;
fora de todas, a batida é recusada com a distância; borda do GPS (até 150 m de
folga) e obra sem coordenada entram para conferência; celular sem localização é
recusado, tablet sem localização não; o modo é por obra (BLOQUEAR, padrão, ou
ANALISAR) e o raio também, num cartão novo da Configuração; alerta novo
"Tentou bater fora da área da obra". No celular, a tela diz onde a pessoa está
("Você está na obra X" / "Você está a 850 m da obra X") antes de ela bater. A
coluna nova entrou na migração **003**, que ainda não foi aplicada em lugar
nenhum.

**Correção de um exagero meu, registrada para não se repetir:** o código e o
README diziam que "a Portaria 671 veda impedir a marcação". Não é bem isso: ela
proíbe restringir o HORÁRIO da marcação, marcar sozinho e exigir autorização
para hora extra; restringir o LUGAR não está na lista. O risco que fica é
trabalhista, não de norma: quem trabalhou fora da obra e foi barrado tem as
horas reclamáveis — daí o alerta e o ajuste.

**Decisões tomadas sem ele, com motivo:**
- **Folga da borda de até 150 m** pela precisão do GPS: dentro de prédio o
  celular erra; sem folga, quem está na obra seria barrado.
- **Obra sem coordenada não bloqueia**: bloquear deixaria a obra inteira sem
  ponto. O cartão da cerca mostra em vermelho quem está sem coordenada.
- **Tablet sem localização entra para análise**: tablet de wi-fi muitas vezes
  não tem GPS, e ele já está preso às obras dele.
- **Identidade é conferida antes do lugar**: aparelho de outra pessoa é recusado
  por isso, não por estar sem localização (um teste antigo pegou a ordem errada).

**Pendente com o dono:** conferir se as obras ativas têm latitude e longitude
no ERP — obra sem coordenada não bloqueia, mas também não detecta.

## 03/10/2026 — QR Code pessoal, tablet de câmera ligada, mosaico e sinais de fraude

Pedido do dono, depois de discutir leitura facial, crachá e número de
funcionário: **"deixar só o CPF e QR Code"** — sem número de funcionário ("o
dígito verificador não muda nada, é só um número a mais") e **sem crachá
impresso** (crachá se empresta: 20 crachás na mão de uma pessoa batem 20
pontos). O QR vai para o WhatsApp da pessoa, ela mostra no próprio celular,
pode pedir de novo se esquecer, e ele **muda com frequência**, com envio
espaçado — "a API do WhatsApp não é oficial, isso pode gerar bloqueio". No
tablet, câmera sempre ativa: viu o QR, segue; ou digita o CPF em teclas
grandes; sem trocar de modo; confirmação com nome e função e foto. Mosaico das
fotos do dia por obra — opcional, obrigatório nas obras escolhidas, com um
responsável que recebe e confirma, e alerta de "falta validação". E: "tudo que
for coisas estranhas (…) batidas muito rápidas (…) o que você pensa que possa
designar fraude, você me diz", além de criticar e avisar quem não tira foto.

**O que foi feito** (migração **003**; ver README, seção do QR):
tablet em tela cheia com câmera ligada e teclado; QR do WhatsApp (troca 7–14
dias por pessoa) e QR do "Meu ponto" (muda a cada 30 s); bilhete assinado entre
identificar e bater; "esqueci meu QR" no tablet e no app; fila de WhatsApp com
ritmo; mosaico com conferência e foto suspeita; sinais medidos em cada foto;
sete alertas novos; a foto sobe para o Drive DEPOIS da resposta da batida.

**Decisões tomadas sem ele, com o motivo** (todas trocáveis):
- **O intervalo sugerido** (era pedido dele): QR troca a cada **7 a 14 dias**,
  sorteado por pessoa; envio automático de **seg. a sáb., 7h30–17h30**, hora
  sorteada; **30 a 90 s** entre mensagens; teto de **40/hora e 200/dia** (150
  automáticas); primeiro envio a todos espalhado em até **120 por dia**. Mais
  curto que 7 dias aumenta volume sem ganho (quem empresta o QR empresta o novo
  também — o que pega esse caso é a foto).
- **Envio automático nasce DESLIGADO.** Mandar WhatsApp para 400 pessoas é
  escrever em sistema de terceiro, e a hora de começar é do dono.
- **O QR antigo vale até o novo ser usado, ou 3 dias** — quem não abriu o
  WhatsApp não fica sem bater no dia da troca.
- **Dois QR em vez de um**: o do WhatsApp (pedido dele) e o do "Meu ponto", que
  muda a cada 30 s — quem tem o app não precisa do WhatsApp, e print não serve.
- **No tablet, batida sem foto vai para análise**; no celular da pessoa, só
  alerta. A câmera fica sempre ligada no tablet; foto faltando ali é exceção.
  Recusar a batida seria impedir a marcação (Portaria 671), por isso não recusa.
- **Foto ruim (escura, repetida) é alerta, não análise automática**: os limiares
  ainda não foram calibrados com foto de obra de verdade, e mandar batida para
  análise por palpite enche a fila de quem trata.
- **Quem confere o mosaico precisa de `tratar_ponto`** (não uma ação nova): é
  tratamento do ponto da obra, e a regra "a ação declarada decide sozinha quem
  entra" fica de pé. A tela de configuração avisa quando o responsável escolhido
  não tem a permissão ou não tem telefone.
- **O link do aviso do mosaico** usa o endereço de quem abre a gestão, aprendido
  sozinho (padrão `https://aplicacoes.bwsconstrucoes.com.br`) — sem variável nova.
- **jsQR** (Apache 2.0) é a primeira biblioteca de navegador do ponto: o iPhone
  não tem leitor de QR embutido no navegador. Servida pelo próprio sistema, sem
  CDN, e guardada pelo service worker. **Dependência nova — avisada ao dono.**

**Sinais de fraude: o que entrou e o que ficou de fora.** Entraram: sem foto,
foto escura/sem rosto, a mesma foto de novo (da pessoa em 4 semanas, ou de
pessoas diferentes na mesma obra e dia), fila rápida (5+ pessoas com menos de
10 s no mesmo aparelho), QR antigo, CPFs que não são de ninguém no tablet (5+
no dia), mosaico obrigatório sem conferência — além dos que já existiam (dois
lugares longe em poucos minutos, fora da cerca repetido). **Ficaram de fora:**
reconhecimento facial automático (custo, e foto de rosto para comparar é dado
biométrico — LGPD, dado sensível, pede consentimento e cuidado próprio; o
mosaico faz a comparação com olho humano), e "batida fora do horário da
escala" (o espelho já acusa trabalho em dia de descanso e extra acima de 2 h).

**Como foi verificado:** 23 testes puros (`test_ponto_qr.py`) e 11 com banco de
verdade (`test_ponto_qr_banco.py`: CPF e bilhete, QR do WhatsApp com troca,
convivência, QR antigo e revogação, "esqueci", QR do app, fila ligada/desligada
com teto, falha do WhatsApp sem QR valendo, mosaico obrigatório com aviso,
alerta, conferência e suspeita, foto escura/repetida/sem foto com aviso, foto de
cadastro escolhida de uma batida); a suíte inteira do repositório; e um ENSAIO
num navegador de verdade (Chromium) com câmera simulada mostrando um QR Code: o
tablet abriu direto na batida, o jsQR leu o QR em meio segundo, mostrou nome e
função, contou 2 s, fotografou e registrou (≈2,4 s da leitura ao "Ponto
registrado"); o mesmo pelo teclado com CPF. A aba Mosaico e os cartões novos da
Configuração foram fotografados no computador e no celular, sem erro de tela
nem rolagem lateral. Os
122 testes antigos do ponto (três ajustados ao comportamento novo: o teste de
cerca agora manda foto, e a foto sobe ao Drive depois da resposta).
**Não verificado:** WhatsApp e Drive de verdade (sem credenciais aqui), e o
tablet com câmera real — QR mostrado na tela de um celular tem reflexo e brilho
que a câmera simulada não tem; é a primeira coisa a olhar no piloto.

**Armadilha encontrada no ensaio, para quem for testar:** batida lançada com
hora no PASSADO (`agora=`) é tratada como repetição de qualquer batida da mesma
pessoa feita depois dela — a regra dos 60 s olha "depois de", não "perto de".
Na vida real a hora é sempre a do servidor, então não acontece; em teste e
importação com hora antiga, lançar em ordem de horário.

## 03/10/2026 — Fase 2 construída: gestão no ERP, Meu ponto, pedidos, banco, alertas

Decisões do dono (ver `PLANO_FASE2.md`): horários configuráveis; gestão no ERP;
"gestão de competências — permissões de aprovações"; quem tem banco de horas,
ok; só o DP vê atestado; "pode seguir". Decisões tomadas sem ele, com motivo:

- **"Gestão de competências" lida de dois jeitos, e as duas estão feitas**:
  quem aprova o quê (ações e seções do ERP, configuráveis por perfil e pessoa) e
  o fechamento do mês (competência = o mês da folha), que trava o ponto daquele
  mês até alguém reabrir com motivo.
- **A gestão é rota do blueprint do ERP**, não blueprint paralelo: herda login,
  guarda, menu e recorte por obra. Código na pasta do ponto; no ERP, só ações,
  seções, a migração 082 e uma importação protegida no fim do `routes.py`.
- **Cada etapa de aprovação tem rota própria** (supervisor / DP), e o atestado
  também: a ação declarada decide sozinha quem entra.
- **Telas lidas pelo caminho do arquivo** (`gestao._render`), não pelo
  carregador do Flask: abrem até num ERP montado sem o blueprint do ponto — que
  é como a homologação automática do ERP as desenha.
- **Banco do ponto atrasado responde 409 com o recado**, não 500: entre publicar
  e apertar o botão, a tela diz o que fazer.
- **PIN por WhatsApp** (proposta §8.4, seguida com o "pode seguir").
- **Aparelho continua precisando de aprovação**, mesmo o celular de quem entrou
  com PIN: o celular se identifica ("Celular de Fulano") e o DP aprova num toque.
  Aprovar sozinho seria mais rápido e mais fraco — fica para o dono decidir.
- **A IA só lê o atestado** (pelo leitor de documentos do ERP, com a chave e o
  registro de consumo que já existem), e o DP confere. Alerta é regra e padrão
  calculados, nunca texto inventado — o resumo do WhatsApp é contado, não escrito
  por IA.
- **Rotina diária sem agendador**: dispara na primeira requisição do ponto
  depois das 6h (toda manhã alguém bate ponto), com trava em memória e no Postgres.
- **A tela de gestão foi escrita por um agente** em paralelo e conferida: as 43
  chamadas que ela faz existem; as lacunas que ele apontou foram corrigidas
  (foto no painel Hoje, horas ilegíveis no banco, id da escala atual, mês pelo
  fuso de Fortaleza, falha de atualização respondendo `ok: false`).

**Incidentes durante a construção** (para não repetir):
- O PIN errado não contava: o registro da tentativa era desfeito junto com a
  recusa. Igual ao caso das recusas da fase 1 — escrita que precisa sobreviver
  a um erro vai em **transação própria**. Vale para o código do WhatsApp também.
- Telefones do resumo eram cortados no espaço de "(85) 99999-1111".
- Lançamento de banco feito hoje não entrava no saldo (o cálculo vai até ontem).
- A homologação do ERP (cada tela desenhada com o banco vazio) pegou as telas do
  ponto dando 500 sem as tabelas do ponto — daí o 409 com recado.

**Como foi verificado:** 122 testes do ponto (`test_ponto*.py`), entre eles 17
fluxos com banco de verdade e três operadores de perfis diferentes; os testes de
permissão e de homologação do ERP; o `app.main` de verdade com login real abrindo
as sete telas, as consultas e o app; a tela de gestão percorrida num navegador
com respostas simuladas (pelo agente). **Não verificado:** WhatsApp e Drive de
verdade (sem credenciais aqui), e uso num celular real.

## 03/10/2026 — Publicado

O dono criou no Render a `PONTO_API_KEY` e a `PONTO_DRIVE_PASTA` (uma pasta
`Ponto` dentro da pasta do Drive do ERP — conferido que o ERP não mexe nela) e
disse "pode prosseguir". Antes da junção a `main` tinha andado 14 commits (só
Análise de SPs, sem conflito); trazida para o ramo, a suíte inteira passou
(7.972 testes, nenhuma falha) e o `app.main` subiu com os 17 módulos.

## 03/10/2026 — Fase 1 entregue no ramo `feature/modulo-ponto`

**O que existe:** schema `ponto` (migração 001), API REST completa, três
scripts (migrar, importar obras, importar colaboradores), 71 testes (puros e
com banco), README. Nada fora da pasta `app/apps/ponto/` e dos dois arquivos de
teste. O serviço de verdade (`app.main`) foi carregado com o blueprint
registrado e todas as rotas respondem; o curinga do encurtador não as engole.

**A especificação de partida foi colada pelo dono de outro chat** (o nome
"Vitor" que apareceu na mensagem dele era erro de ditado — ninguém com esse nome
desenhou nada), e foi confrontada com o repositório antes de qualquer código — o `PLANO.md` registra os achados.
Decisões, com o motivo:

1. **Obras e pessoas são do ERP, não do ponto.** A especificação pedia
   `ponto_obras` e `ponto_colaboradores`. Seria a 2ª cópia de obras e a 3ª de
   pessoas (a Análise de SPs já espelha o Pipefy). O dono decidiu em 18/09/2026
   "100% no ERP, nada de sistema paralelo", e em 03/10/2026 que o ponto é a
   solução definitiva à qual as outras se conectam — as duas coisas combinam:
   cadastro no ERP, ponto como fonte da verdade do PONTO. Preço: amarra entre
   áreas; `test_contrato_com_o_erp` acusa se o ERP renomear uma coluna lida.
   O importador cria no ERP pessoa/obra que não existe (única escrita do ponto
   em tabela do ERP) e **não altera** pessoa que já existe — obra divergente é
   aviso, porque o ERP manda.
2. **Foto no Google Drive, reduzida, opcional.** O disco do Render é apagado
   a cada publicação; "salvar localmente" perderia tudo. A primeira versão
   guardava no banco; o dono corrigiu no mesmo dia: *"as fotos a gente pode
   armazenar no Google Drive, lá o espaço é virtualmente infinito; na base de
   dados não"*. Sobe pela rotina de Drive do ERP (`erp/core/documentos/drive.py`,
   importada), fechada (nunca pública por link), em subpasta por mês, na pasta
   `PONTO_DRIVE_PASTA`. No banco fica só a ficha (hash, tamanho, id no Drive).
   Se o Drive falhar na hora, a batida NÃO falha: os bytes esperam em
   `ponto.fotos.conteudo` até a rota/script de reenvio levá-los. JPEG de
   800 px, teto de 300 KB. Expurgo por prazo é rotina futura (coluna já existe).
3. **Identidade recusa; lugar e relógio vão para análise.** A especificação
   dizia "fora das regras é rejeitada". A Portaria 671 veda impedir a marcação
   do empregado; recusar quem está a 250 m da obra cria passivo. Recusa só por
   aparelho/pessoa/obra inválidos, com registro em `ponto.recusas`.
4. **Token por aparelho, não chave de API no celular.** Acréscimo de
   segurança: chave no PWA vazaria. Token entregue uma vez, hash no banco,
   morre com o bloqueio.
5. **NSR e hash encadeado desde já**, para o AFD da fase 3 não exigir refazer
   tabela com 400 pessoas batendo.
6. **Pessoa sem config no ponto bate com padrões** (PADRAO_4, ativa): o ERP
   diz quem existe; o ponto só afina.
7. **Repetição em 60 s não grava** (toque duplo). **Relógio** do aparelho
   difere >5 min → análise. **Consulta** até 62 dias e 20.000 linhas por
   chamada (instância de 2 GB).
8. **Formato da consulta é o do ponto**, não o do Mobponto (4 colunas por dia).
   Correção do dono no mesmo dia: "não adaptar o que temos ao Mobponto; o
   inverso". A Análise de SPs vai ler o ponto direto no banco (mesmo Postgres)
   quando chegar a hora — a API é para celular, iDFace e sistemas de fora.

**Incidentes durante a entrega** (para não repetir):
- A recusa era gravada na mesma transação da batida recusada e sumia com o
  rollback. Passou a ter transação própria (`marcacoes._recusar`).
- `aprovar`/`bloquear` devolviam o aparelho sem o nome do dono; passaram a usar
  a consulta detalhada.
- O Flask não aceita rota nova num blueprint já registrado; o teste do guarda
  usa um blueprint de mentira com o mesmo `before_request`.
- Os testes com banco gravam de verdade e deixavam três obras `PT-*` no banco
  ao terminar; com a suíte em paralelo, outro arquivo do mesmo trabalhador
  (`test_obra_do_documento_banco.py`) contava obras esperando zero e caía. A
  fixture passou a limpar também na SAÍDA. Regra para teste novo com banco neste
  módulo: o que grava, apaga ao sair.

**O que ficou de fora e por quê:** `conftest.py` não foi tocado: as fixtures
de banco do ponto moram no próprio arquivo de teste. Expurgo de fotos por prazo
e o conector do iDFace ficam para as próximas fases.

**Esclarecimentos dados ao dono em 03/10/2026, para constar:**
- **REP-P** = Registrador Eletrônico de Ponto via Programa. É a categoria da
  Portaria 671/2021 para ponto feito por software (celular, computador), em
  vez da máquina de parede (REP-C). O que a lei pede do REP-P: registrar cada
  marcação com número sequencial sem furo, não permitir alterar nem apagar,
  gerar o arquivo fiscal (AFD) e o espelho (AEJ), dar comprovante ao
  empregado e ter registro do programa com termo de responsabilidade do
  empregador. O schema já nasce com o que os arquivos precisam; os arquivos e
  o registro são fase 3 e assunto da contabilidade.
- **Juntar na `main` É publicar**: o Render publica na hora. Não existe
  "juntar sem publicar" neste repositório.

**Como foi verificado:** `tests/test_ponto.py` + `tests/test_ponto_banco.py`
(71 passando contra Postgres 16 local); os três scripts rodados de ponta a
ponta num banco descartável (simulação e gravação, com linhas boas e ruins
misturadas); `app.main` carregado com o blueprint e as rotas exercitadas sem
`DATABASE_URL` (health responde, chave ausente fecha com 503/401). A suíte
inteira foi rodada em paralelo antes do envio: 157 falhas na primeira rodada,
TODAS por biblioteca que faltava no ambiente de desenvolvimento (dropbox, pypdf,
gspread, erpbrasil…), nenhuma no ponto nem causada por ele; com as bibliotecas
instaladas, os arquivos que falharam passaram todos.
