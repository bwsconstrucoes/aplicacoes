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
   **Atualização de 29/10/2026 (o dono):** o WhatsApp **já está ligado** em ERP › Mensagens desde a
   ativação dos aparelhos — o trecho acima é a história de 05/10, não o estado de hoje.

0a. **INCIDENTE 21/10/2026, ~17h — o aplicativo do ponto (e o Render inteiro) parou de carregar na hora da
   saída.** O dono reiniciou o serviço. As métricas do Render que ele trouxe: **memória 30 %, CPU 20 %, ~350
   requisições no pico — NÃO foi excesso de uso**: o serviço estava parado, ESPERANDO. Os registros mostravam
   respostas em rajadas a cada ~45 s e nenhum erro. **Causa mais provável:** o pedido de código de acesso
   (`/ponto/app/api/codigo`, "Primeiro acesso ou esqueci o PIN") mandava o WhatsApp DENTRO da requisição, e o
   `notificador` espera o Z-API até **60 s, 3 tentativas** (mais de 3 minutos no pior caso), com a requisição
   e a conexão do banco presas. O WhatsApp vinha falhando; na saída, gente pedindo o código (e pedindo de
   novo, porque não chegava — o registro mostrou o teto de códigos por hora estourado) prendia as **4
   linhas** do serviço e as conexões do banco (10 para o Render inteiro) — e todas as áreas pararam juntas.
   **Conserto PUBLICADO em 21/10/2026 à noite (junção `878ec81`):** o código é gravado e a resposta sai na hora; o envio vai para uma fila em segundo
   plano (no máximo 2 ao mesmo tempo, sem conexão do banco presa). E, por precaução (a6a6b3c), o menu do
   aplicativo deixou de calcular a qualidade a cada abertura (era a primeira suspeita: a conta da equipe
   inteira refeita a cada batida), com guarda de 10 minutos e uma conta pesada por vez. **Fora do ponto, e
   fica com o chat da Mensageria:** o `notificador.py` com 60 s × 3 é uma armadilha para QUALQUER área que
   mande WhatsApp dentro de uma requisição (ver `CONTEXTO.md` §9, 21/10/2026). **A lição:** nada que
   dependa de serviço de fora (WhatsApp, Drive, AWS) roda dentro da requisição de uma tela que muita gente
   abre no mesmo minuto.

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
1zz11. **Publicado em 30/10/2026** (com o "pode" do dono, junto com 1zz7 a 1zz11; feito em 30/10/2026, noite, segunda) — COM A MIGRAÇÃO 021 DO PONTO**: o **turno da
   noite pela escala** — com a escala que passa da meia-noite (o 12x36 das 17h às 6h dos vigias), a saída da madrugada
   fica no dia da entrada, sem marcar "vigia noturno" na jornada; a 021 acerta as batidas já gravadas no dia errado. Vai
   junto com 1zz7 a 1zz10. Ao juntar, apertar "Aplicar atualizações do ponto" (a 020 e a 021); sem a 021, as batidas
   novas já nascem certas, mas as antigas dos vigias continuam partidas.
   **No mesmo lote: o ciclo 12x36 automático** — o sistema não pede mais "um dia de trabalho"; descobre pelas batidas
   quais noites são de trabalho e critica a noite fora do ciclo para o DP decidir.
1zz10. **Publicado em 30/10/2026** (com o "pode" do dono, junto com 1zz7 a 1zz11; feito em 30/10/2026, noite) — SEM MIGRAÇÃO**: no calendário de horas extras, **as horas a
   menos aparecem em vermelho** (as extras em azul), e a opção **"Abater os atrasos e as saídas antecipadas das horas
   extras"** (desligada de início); a tolerância do 1zz9 vale também para o atraso. Vai junto com 1zz7 a 1zz9.
1zz9. **Publicado em 30/10/2026** (com o "pode" do dono, junto com 1zz7 a 1zz11; feito em 30/10/2026, tarde, segunda) — SEM MIGRAÇÃO**: no fechamento, **desconsiderar a
   hora extra do dia de até N minutos** (escolhido na tela, guardado). Vai junto com 1zz7 e 1zz8.
1zz8. **Publicado em 30/10/2026** (com o "pode" do dono, junto com 1zz7 a 1zz11; feito em 30/10/2026, tarde) — COM A MIGRAÇÃO 020 DO PONTO**: a **declaração de
   comparecimento abonada por uma quantidade de horas** (e o "só o período" contando só o que cai dentro do expediente),
   e **clicar no dia do calendário de horas extras abre o espelho** da pessoa. Vai junto com o 1zz7. Ao juntar, apertar
   "Aplicar atualizações do ponto" (a 020); sem ela, só a opção "uma quantidade de horas" não funciona.
1zz7. **Publicado em 30/10/2026** (com o "pode" do dono, junto com 1zz7 a 1zz11; feito em 30/10/2026) — SEM MIGRAÇÃO**: a **compensação conferida** — passado o dia
   trabalhado, a folga só fica abonada no que ele rendeu; o resto vira falta (automático, decisão do dono); quem bate no
   dia da folga tem as horas contadas; e a compensação ainda a cumprir aparece na Conferência do fechamento.
1zz6. **Publicado em 29/10/2026** (com o "pode" do dono, junto com o 1zz5) — **COM A MIGRAÇÃO 019 DO PONTO**: o **abono da falta
   parcial** no Quadro (só as horas que faltaram), as **horas extras por obra na tela** do fechamento com **negar a
   hora extra** de um dia (e voltar a contar), e **só CTPS nas contas de horas** (a coluna "Tipo" da planilha). Vai
   junto com o 1zz5. Ao juntar, apertar "Aplicar atualizações do ponto" (a 019); sem ela, só o "negar" não funciona.
1zz5. **Publicado em 29/10/2026** (com o "pode" do dono) — **SEM MIGRAÇÃO**: o **abono pedido como os outros
   pedidos** — no aplicativo ("O que vai entregar ou pedir?", o pedido por outra pessoa, o Meu ponto) e no ERP
   (janela da pessoa › "Pedir abono"); vai para a validação ("Quem valida o quê" › Abono, nasce no DP).
1zz4. **Publicado em 29/10/2026** (com o "pode" do dono, junto com 1zz1 a 1zz3) — **SEM MIGRAÇÃO**: as **horas extras para
   a supervisão conferir** (calendário por obra, 26 a 25, em paisagem — imprimir ou Excel) e o **percentual da hora
   extra por estado ou por obra** (Configuração › Jornada e feriados), que decide o evento do .ps. Vai junto com o
   1zz3. Antes de gerar o primeiro .ps de verdade: cadastrar os percentuais de PE, SP e CE.
1zz3. **Publicado em 29/10/2026** (com o "pode" do dono) — **SEM MIGRAÇÃO NOVA**: o **fechamento da folha para a
   contabilidade** (Relatórios › "📦 Fechamento da folha"): o mês anterior (26 ao fim) em planilha; o corrente (1 a 25)
   nos dois arquivos .ps do Fortes (faltas e horas extras inteiras) e em planilha (faltas parciais e horas
   quebradas); tudo num Excel com uma aba por parte, ou os três arquivos num zip. **Antes de usar na folha de
   verdade: importar um .ps de teste no Fortes** (ver "O que confirmar" na seção do dia).
1zz2. **Publicado em 29/10/2026** (com o "pode" do dono) — **COM A MIGRAÇÃO 018 DO PONTO**: **"não bate ponto"
   à vista** na janela da pessoa e **em lote** na tela Pessoas, com o motivo, quem marcou e quando. Vai junto com o
   1zz1. Ao juntar, apertar "Aplicar atualizações do ponto" (a 018); sem ela, a marcação funciona, mas o motivo não
   fica guardado.
1zz1. **Publicado em 29/10/2026** (com o "pode" do dono) — **SEM MIGRAÇÃO**: a **falta parcial** — o dia com batida
   faltando (só a manhã, só a tarde, três batidas, uma só) vale o que está comprovado e o resto vira débito. Muda o
   débito e o banco de horas de dias que já passaram (o espelho é refeito a cada abertura).
1zz0. **Publicado em 29/10/2026** (com o "pode" do dono, junto com o 1zz e o 1zy) — **SEM MIGRAÇÃO**: **quem a
   planilha diz que não bate ponto sai do ponto** (PJ e pró-labore na coluna AY, a obra ESCRUSSAS na AA, e os
   afastados da AX saem das listas), e **a foto da batida abre em branco** ("Carregando a foto…") em vez de
   mostrar a anterior.
1zz. **Publicado em 29/10/2026** (com o "pode" do dono) — **SEM MIGRAÇÃO NOVA**: a **conta da qualidade
   com o ajuste como métrica principal** (todo ajuste tira 5 pontos, e todo dia com batida faltando também), o
   **ranking no aplicativo** (obras com 10 funcionários ou mais, os 25 primeiros funcionários com 10 dias ou mais,
   guardado e refeito de 2 em 2 horas) e o **espelho de um período escolhido** no aplicativo. Vai junto com o
   1zy (que tem a 017).
1zy. **Publicado em 29/10/2026** (com o "pode" do dono) — **COM A MIGRAÇÃO 017 DO PONTO**: a seção **Licenças e
   abonos** na Configuração (ver, cadastrar, excluir, e se cada tipo abona as horas) e a **barra de filtros que
   abre e fecha** (Validações, e o bloco Funcionários em todas as telas). Ao juntar, apertar "Aplicar
   atualizações do ponto" (a 017); sem ela, os abonos continuam os dois de sempre e toda licença abona.
1zx. **Publicado em 29/10/2026** (com o "pode" do dono; junção `155f040`) — **COM A MIGRAÇÃO 016 DO PONTO**: a negativa no espelho do
   ERP; o WhatsApp da negativa que não saía (a fila dormia antes da mensagem vencer); o **aviso de obra
   suspeita** no pedido de ajuste; e o **DP corrigindo a obra** do pedido que ainda espera decisão. Ao juntar,
   apertar "Aplicar atualizações do ponto" (a 016); sem ela, só o botão "Corrigir a obra…" não aparece.
1zw. **Publicado em 29/10/2026** (com o "pode" do dono; junção `7a18d48`) — **COM AS MIGRAÇÕES 014 E 015 DO PONTO**: o **espelho do
   ponto no aplicativo** (o seu e o de uma pessoa da obra, com o PDF), a **negativa avisada a quem pediu** (aviso
   no aplicativo, o negado no dia, quem pediu por outra pessoa e o WhatsApp), a **UF da obra pela coluna AC da
   C. Diários** (o feriado estadual) e o **filtro "Motivo" nas Validações**. Depois de apertar o botão, ler a
   planilha de novo (Base de obras › "Ler a planilha agora") para a UF entrar. Ao juntar, apertar **Ponto ›
   Configuração › "Aplicar atualizações do ponto"** no mesmo momento; sem isso, a negativa funciona como antes
   (sem aviso) e nada quebra. Para o WhatsApp sair, o WhatsApp precisa estar ligado em ERP › Mensagens. Ver as
   seções de 28/10/2026 (noite).
1zv. **Publicado em 28/10/2026 (tarde)** (com o "pode" do dono; sem migração; `74bff51`) — Pessoas de volta
   com a foto de cadastro, todas as telas de volta ao menu, as Validações agrupadas por pessoa e dia, a barra
   fixa, a troca da foto no mosaico e o quanto cada ajuste é crível. Ver a seção de 28/10/2026 (tarde).
1zu. **Publicado em 28/10/2026** (com o "pode" do dono; sem migração) — as anotações do dono sobre a
   Configuração, o espelho e a conferência do rosto: filtros na barra lateral com várias obras e funções;
   o espelho DENTRO da janela da pessoa (as telas Espelho e Pessoas saíram); corrigir a obra ou a hora de
   UMA batida; validar a conferência direto do espelho; o "sem rosto" que era a foto de cadastro; e o
   porquê do nível da conferência. Ver a seção de 28/10/2026.
1zt. **Publicado em 27/10/2026, junto com a 1zs** (com o "pode" do dono; sem migração) — o **ponto da obra** com o que o dono
   aprovou: a **busca pelo nome** (foto pequena, sem CPF, só com o responsável conectado), a **volta para a
   batida em 5 minutos** (conectado; sair dela depois disso pede o PIN) e o **som e a vibração** da batida.
   Ver a seção de 27/10/2026 (noite).
1zs. **Publicado em 27/10/2026, junto com a 1zt** (com o "pode" do dono; sem migração) — a **declaração de comparecimento**, com a
   escolha do abono na aprovação (o dia todo, só o período, nada). Ver a seção de 27/10/2026 (tarde).
1zr. **Publicado em 27/10/2026, junto com a 1zq** (com o "pode" do dono: "ao final de tudo, pode publicar"; sem
   migração) — **menos telas e um filtro só**: no menu, só **Quadro, Validações, Relatórios e Configuração**; o
   **filtro de funcionários** igual nas três primeiras, na barra lateral; Alertas, Qualidade, Rondas e Mosaico
   viraram partes do Quadro. Ver a seção de 27/10/2026.
1zq. **Publicado em 27/10/2026** (26/10/2026, sem migração) — a leva das anotações do dono: **Configuração ›
   Escalas** (a escala padrão no topo, quem está em cada escala, aplicar em lote), a **falta em destaque** no dia
   seguinte, o **feriado em várias obras**, **Viagem e Dispensa** como abonos, o **documento numa janela**, o
   **confronto da leitura da IA**, a tela das **fotos de cadastro** e os **Relatórios** (faltas, ausências
   justificadas, horas extras, banco de horas). Ver a seção de 26/10/2026.
   **Pendente com o dono:** escolher a escala padrão (Configuração › Escalas) e aplicar a dos vigias e a do
   escritório; o arquivo .ps da contabilidade (faltas e horas extras) — ele vai mandar o leiaute; o "layout do
   mês completo resumido" ficou em espera, a pedido dele.
1zp. **Publicado em 23/10/2026** (com o "pode" do dono; 22/10/2026, sem migração) — o que o dono viu no Quadro em produção:
   caixa de seleção em TODA linha e quatro ações em lote (incluir uma batida, lançar o dia inteiro pela escala,
   abonar a falta, definir a escala); o pedido de ajuste NO horário, clicável, com aprovar e negar ali; a
   JANELA DA BATIDA (como foi feita, aparelho, foto, quem pediu, quem aprovou, decisões) com validar,
   rejeitar e **desconsiderar a batida errada**; a batida fora do horário da escala em destaque; a legenda;
   a aba Aparelhos da janela da pessoa com Alterar/Aprovar/Reativar e "Ligar a ronda"; o aviso de "sem
   escala"; o botão "Montar o quadro" no topo. Ver a seção de 22/10/2026 (tarde).
   **Pendente com o dono:** escolher a ESCALA PADRÃO DA EMPRESA (Configuração › Jornada e licenças) — na
   produção ela não está escolhida, e por isso "todo mundo sem escala".
1zo. **Publicado em 22/10/2026** (com o "pode" do dono) (22/10/2026, sem migração, **peça nova: Leaflet**) — o **MAPA DO QUADRO**
   (Quadro › 🗺️ Mapa): uma bolha por obra, do tamanho de quantas pessoas bateram nela, na cor do quanto o ponto
   está completo. Ver a seção de 22/10/2026. **Não conferido aqui:** o fundo do mapa (ruas, vindo do
   OpenStreetMap) — o ambiente de desenvolvimento não alcança a internet; conferir na primeira abertura.
1zn. **Publicado em 22/10/2026** (com o "pode" do dono) (21/10/2026, sem migração, **NASCE DESLIGADO**) — a **base mista da folha
   do Análise de SPs**: MobPonto até 15/10, este ponto do início do ponto em diante (`core/para_folha.py` e o
   encaixe em `analisesps/ponto.py`). Chave em Configuração › Geral, "A folha lê o ponto eletrônico".
   **Pendente com o dono:** conferir a ficha de algumas pessoas e o rateio de uma quinzena, com o chat do
   Análise de SPs, e só então ligar. Decisões de tradução (obra por código, presença, falta, diarista só com
   batida) no cabeçalho de `para_folha.py`.
1zm. **Publicado em 22/10/2026** (com o "pode" do dono) (21/10/2026, sem migração) — a **tela gerencial integrada**: a JANELA DA
   PESSOA, as ETIQUETAS (vigia, ronda, celular, ponto da obra, equipe), o MAPA DA BATIDA em janela, a visão "Por
   obra" e o **Quadro como primeira aba** (Hoje e Pessoas saíram da barra; os endereços continuam abrindo).
   Ver a seção de 21/10/2026 (noite, segunda).
1zl. **Publicado em 22/10/2026** (com o "pode" do dono) (21/10/2026, sem migração) — o **QUADRO DO PONTO** (aba nova "Quadro", logo
   depois de Hoje) e a **inclusão de batida em lote**. Ver a seção de 21/10/2026 (noite). Primeiras telas
   mostradas ao dono; **esperando a opinião dele sobre os filtros** antes de levar a mesma barra para as outras
   telas (pedido dele: "o que a gente fechar de filtro é para valer para as outras telas").
1zk. **PUBLICADO em 21/10/2026** (junção `d6ab5eb`, sem migração) — **Validações diz QUEM PEDIU** (o dono: "não
   aparece a pessoa que solicitou o ajuste da batida ou encaminhou um atestado; é importante, e precisa ser
   pesquisável"). Coluna "Pedido por": "a própria pessoa" (pelo celular / no aparelho X) ou o nome de quem pediu
   por ela (responsável no aparelho da obra, DP pelo ERP). A busca "Pessoa ou quem pediu" acha pelo nome de quem
   pediu, e passou a achar também aparelhos (pelo dono, nome ou código), que antes sumiam ao buscar. O dado já era
   gravado desde a fase 2 (`solicitado_por`); só não aparecia.
1zj. **PUBLICADO em 17/10/2026 (junção `8a9d083`) — COM MIGRAÇÃO 013** — a **conferência do rosto que se adapta à
   pessoa** (o dono: "quanto melhor a pessoa, menos se confere; quanto pior, mais se confere"). Ver a seção de
   17/10/2026 (noite, segunda). **Falta o dono apertar** "Aplicar atualizações do ponto" (a 013), se ainda não
   apertou. Antes do botão nada quebra: confere tudo o que passa pela regra, como antes.
1zi. **PUBLICADO em 17/10/2026** (junção `71ed2e2`, sem migração), junto com a 1ze–1zh — **O PONTO VALE A PARTIR
   DE 16/10/2026** (o dono: "desconsiderar pra todos os efeitos qualquer lançamento antes disso"). Nada foi
   apagado; o que vem antes some de tudo (ver a seção de 17/10/2026, noite). A data se muda em Configuração ›
   Geral › "Início do ponto". E o botão **"Testar a AWS"** (Configuração › Fotos), que diz se o bloqueio da
   organização é de região (troca no próprio ponto) ou do serviço (só na conta que administra a organização).
   **AWS RESOLVIDA em 17/10/2026:** o dono rodou o "Testar a AWS" e era só a REGIÃO — a regra da organização
   barrava a região configurada e liberava outra; trocada pelo botão "Usar a região X", a conferência do rosto
   passou a funcionar. Nada mudou na AWS.
1zh. **PUBLICADO em 17/10/2026 (junção `71ed2e2`)** (17/10/2026, sem migração), junto com a 1ze–1zg — os **avisos na tela
   inicial do aplicativo** (o dono: "se aparecer alertas, tem que ser algo mais resumido (…) mas poder adentrar
   alguma área que detalhe melhor"): uma linha com o problema mais recente da pessoa (ou "✅ seu ponto está em
   dia"), e, para o responsável pelo ponto da obra, outra com a equipe ("ontem, N pessoas com problema"). O
   toque abre o detalhe: o selo, os números, "o que aconteceu" dia a dia e "como melhorar"; o da equipe abre
   "ontem" e as pessoas, piores primeiro. Vermelho = falta ou batida esquecida nos últimos 7 dias; âmbar = outro
   problema; verde = em dia. Telas mostradas ao dono antes de publicar.
1zg. **PUBLICADO em 17/10/2026 (junção `71ed2e2`)** (17/10/2026, sem migração), junto com a 1ze e a 1zf — a
   **QUALIDADE DO PONTO** (`core/qualidade.py`): aba **Qualidade** no ERP (ranking por obra, por responsável
   pelo ponto da obra e por pessoa, com índice "ponto em dia", fotos boas, faltas, batida faltando, ajustes,
   pontos e selo 🥇🥈🥉⚠️); no aplicativo, o quadro **"Meu ponto em dia"** em Meu mês e, para quem
   responde por um ponto da obra ou de equipe, **"🏆 Ponto da minha equipe"** no menu. Limite de ajustes
   configurável na própria aba (nasce em 3/mês). Ver a seção de 17/10/2026 (tarde).
1zf. **PUBLICADO em 17/10/2026 (junção `71ed2e2`)** (16–17/10/2026, sem migração), junto com a 1ze — as telas do ERP
   (pedido do dono: "melhorar o sidebar em relação a filtragens e deixar mais padronizado"):
   **Mosaico** com os filtros na barra da esquerda e, por padrão, só as obras COM batida no dia (com
   quantas pessoas); escolher a obra já carrega. **Espelho**: mês (MM/AAAA, em qualquer navegador — o
   campo de mês do Firefox/Safari mostrava 2026-10), obra (as com batida no mês) e só então a pessoa (quem
   bateu naquela obra no mês; quem bateu em duas aparece nas duas); coluna **Obra** em cada dia, em
   destaque quando não é a principal, com as horas de cada obra; no impresso, `[CÓDIGO]` ao lado da hora.
   **Pessoas**: filtros de obra do cadastro, obra em que está batendo (30 dias), função, escala, situação e
   "sem PIN"; colunas "Batendo em" e "Função". **Escala padrão**: já existia (Configuração › Jornada ›
   "Escala padrão da empresa") e o espelho já a usava, mas Pessoas, a ficha e o painel Hoje diziam "sem
   escala" — agora dizem a padrão (o painel Hoje passou a julgar por ela, como o espelho).
   **Em aberto com o dono:** o WhatsApp que bloqueia (proposta na seção de 17/10). Os dados de teste e o
   painel de problemas já foram decididos (1zi e 1zg).
1ze. **PUBLICADO em 17/10/2026 (junção `71ed2e2`)** (16/10/2026, sem migração). (1) **Correção de ponto em QUALQUER obra
   cadastrada** — decisão do dono: "não é para ser a obra do cadastro, é para aparecer a relação de obras
   cadastradas". O aplicativo lista as obras ativas (as da pessoa primeiro) e o servidor deixou de recusar
   "essa obra não é da pessoa"; quem valida confere. (2) "Celular bloqueado que sumiu dos aprovados": não era
   defeito achado — os caminhos que bloqueiam são o bloqueio à mão, o "Desativar" de Validações, o
   **arquivar** (excluir aparelho com batida: fica bloqueado e FORA da lista), aprovar outro celular da mesma
   pessoa e o desligamento. A ficha da pessoa passou a mostrar o motivo, a data e se foi arquivado.
1zd. **Batida SEM INTERNET no ponto da obra — PUBLICADA em 16/10/2026, com MIGRAÇÃO 012** (o dono: "pode
   prosseguir e publicar ao final"). **Falta o dono, no momento da publicação:** apertar **"Aplicar
   atualizações do ponto"** (a 012). Antes do botão nada se perde: o tablet guarda e o servidor responde
   "aplique as atualizações" (503), que ele trata como "tenta depois". **E nos tablets das obras:** abrir o
   ponto uma vez COM internet depois da publicação (baixa a casca nova e a lista de quem bate ali). Ver a
   seção de 16/10/2026 abaixo.
1zc. **PUBLICADO em 16/10/2026** (com o "pode" do dono, sem migração). Relato: "recebe o código, coloca, mas
   não dá match" e "o aparelho da pessoa não aparece pra validar"; a mensagem exata era **"aparelho não
   encontrado — feche e abra de novo"**. O código e o PIN FUNCIONAVAM (os dois códigos "usado"); o que travava
   era o pedido do aparelho. Causa, reproduzida no navegador: o celular guarda a identidade dele (um número e uma
   senha) e, se o servidor não a reconhece, o pedido falha PARA SEMPRE — fechar e abrir não muda nada. Dois jeitos
   de chegar lá: (a) o pedido EXCLUÍDO em Configuração › Aparelhos com o celular ainda lembrando dele; (b) a
   internet que cai no registro — o servidor cria o pedido e a resposta com a senha não chega. Agora o celular
   esquece a identidade que o servidor não reconhece e se registra de novo sozinho (testado nos dois casos e com a
   tela aberta). Junto: o botão "Vai bater o ponto por este celular? Pedir a aprovação" À VISTA no Meu ponto de
   quem bate na obra (antes só no fim do menu); navegador registrado sem pedido ganha o botão (antes ficava fora
   de Validações e sem botão); o código certo de quem está "não bate ponto" diz o motivo e não é gasto.
   **Cuidado que fica:** excluir um pedido em Aparelhos não avisa o celular — ele só se registra de novo quando a
   pessoa pede outra vez. O registro perdido pela internet sobra como um pedido "sem nome" em Validações: pode excluir.
1zb. **PUBLICADO em 15/10/2026, madrugada** (com o "pode" do dono, `main` em `246e8e1`, sem migração):
   pedidos de aparelho com o mais novo em cima, a hora do pedido e a lista se atualizando sozinha; na
   ficha da pessoa, "Código não chegou?" (o que impede o envio, o que o WhatsApp respondeu, e — para quem
   configura — um código entregue em mãos, sem telefone); vale qualquer código ainda no prazo, não só o
   último pedido; limites por rede maiores para o cadastro em grupo (códigos 100/h, entradas 300/h,
   aparelhos 120/h); o manual com a nota do código em mãos. **Falta o dono:** subir o teto do WhatsApp da
   empresa em ERP › Mensagens (sugestão ~150/h e 1000/dia na implantação — o padrão de 40/h segura os
   códigos em silêncio) e liberar a AWS (a política da organização, SCP, ainda bloqueia a comparação de
   rosto).
1za. **PUBLICADO em 15/10/2026** (com o "pode" do dono, sem migração): o `Procfile` com
   `--max-requests 5000 --max-requests-jitter 500`, igual ao Start Command que o dono já tinha trocado no
   Render. **Falta o dono:** olhar o gráfico de memória nas próximas semanas (passando de ~60% e ficando,
   volta para 1000, nos dois lugares).
1z. **PUBLICADO em 15/10/2026, fim da noite** (com o "pode" do dono, sem migração): o teste de carga
   com um play no ERP (Configuração › Geral › "Teste de carga": o computador simula N aparelhos, nada
   fica guardado) e a página opcional no celular (`/ponto/app/teste-carga/`). **Falta o dono:** rodar
   fora do horário de batida e contar os números (ida e volta, servidor, erros).
1y. **PUBLICADO em 15/10/2026, noite** (com o "pode" do dono, sem migração): o teste de carga de 500
   pessoas — "fila rápida" a 4 s (a fila real ia 80% para conferência) e a identificação no aparelho da
   obra tentando de novo quando a conexão cai na renovação do serviço. **Decisão em aberto do dono:**
   subir o `--max-requests` (hoje 1000; Procfile e Start Command do Render, os dois).
1x. **PUBLICADO em 15/10/2026, à tarde** (com o "pode" do dono, sem migração): o erro de permissão da
   AWS diz QUEM bloqueou (SCP da organização, limite de permissões, Deny ou falta de política).
   **Falta o dono:** rodar o simulador de políticas do IAM (ou "Conferir agora") e dizer o motivo.
1w. **PUBLICADO em 15/10/2026** (com o "pode" do dono, `main` em `7995036`, sem migração): o manual do
   colaborador no sistema (`/ponto/app/manual/` e `/ponto/app/manual.pdf`, link no app), os textos
   iguais aos botões (Pedidos na obra, "Anexar o documento") e o erro da AWS em português.
   **Falta o dono:** dar `rekognition:CompareFaces` ao usuário IAM `bws-ponto-rosto` e rodar a
   conferência do rosto de novo.
1v. **PUBLICADO em 14/10/2026, tarde** (com o "pode" do dono, `main` em `3b32843`, sem migração): o
   pedido de aprovação do aparelho só depois de entrar com CPF e PIN ("Este aparelho vai bater ponto?",
   no menu, com o nome de quem pediu); a busca de Pessoas diz por que alguém não aparece; os textos da
   Configuração sobre afastado. A Karla: a busca disse "não está no ERP nem na cópia do Registro" — a
   cópia estava velha; o caminho é "Atualizar cadastro" na Análise de SPs. Se depois disso ela ainda não
   aparecer, a linha dela está numa aba que a cópia não lê (a cópia lê "Dados Documentos"). **Falta o
   dono:** conferir o quadro "Google Drive e fila de envio" (fotos chegando ao Drive?).
1u. **PUBLICADO em 14/10/2026** (com o "pode" do dono, `main` em `e6578f9`): afastado não bate; "ver no
   mapa" na batida (espelho e Validações) e na ronda; a batida recusada guarda o
   lugar. Sem migração.
1t. **PUBLICADO em 13/10/2026, noite** (com o "pode" do dono, `main` em `42037b6`) — a RONDA SURPRESA dos
   vigias noturnos, com a **migração 011** — apertar "Aplicar atualizações do
   ponto" ao publicar. Sem ela, nada quebra: a marcação de vigia recusa ("aplique
   a 011") e a tela Rondas pede a atualização. Depois de publicar: marcar os
   vigias (Pessoas › Acesso no aplicativo), conferir em ERP › Mensagens que
   "Ronda surpresa do vigia" e "Aviso de ronda perdida" estão em "Só WhatsApp" e
   a chave geral do WhatsApp ligada, e ter telefones em Configuração › Resumo
   diário (é para eles que vai o aviso de ronda perdida).
   Vão juntos (ver a seção "13/10 (noite) — A obra do cadastro não trava"): a
   obra do cadastro deixou de interferir na batida, e a tela de aparelhos ganhou
   busca, filtros e excluir/arquivar (a coluna `arquivado_em` está na mesma 011).
1s. **PUBLICADO em 13/10/2026** (com o "pode" do dono, `main` em `a137aaf`, sem migração) — junto com a 1r: a batida de
   outra pessoa no ponto da obra ia para o nome de quem estava logado (CORRIGIDO;
   ver a seção de 13/10 — as batidas erradas que já existem na produção precisam
   ser conferidas pelo dono); documento em PDF dava "foto ilegível"; aparelho
   "não expira"; mesmo celular em outro navegador; batida repetida; nomes das
   batidas sem escala; hora futura no ajuste de hoje. Sem migração.
   **Em aberto com o dono:** bater sem internet (guardar no celular e mandar
   depois) — não existe hoje; é decisão dele (ver a seção de 13/10).
1r. **PUBLICADO em 13/10/2026** (junto com a 1s): ajuste de VÁRIOS DIAS num pedido
   só (Meu mês, consulta, Pedidos e ponto da obra); o dia de hoje e o feriado sem
   horário cadastrado passam a abrir para correção; feriado não espera mais as
   batidas da escala. Sem migração.
1q. **PUBLICADO em 10/10/2026** (com o "pode" do dono, `main` em `1042e5f`, sem migração): fora da obra, o aviso não cita
   mais a obra mais perto nem a distância; no ERP, a pessoa responsável por um
   ponto da obra aparece como "faz pedidos pelo celular"; e a marcação de pedidos
   pelo celular também na janela do aparelho, refletindo a da pessoa. Sem migração.
1p. **PUBLICADO em 10/10/2026, noite, segunda leva** (com o "pode" do dono, `main` em
   `d6308fb`): no ponto da
   obra, o "Bater ponto" da tela inicial fica desligado fora da obra (com a
   distância e "toque para procurar de novo"); a linha do alto diz onde o
   aparelho está agora e o papel de quem entrou; "‹ Início" na batida volta sem
   pedir PIN; o responsável por um ponto da obra pede o próprio no "Meu ponto";
   a aba da tela atual fica marcada e sem toque. Sem migração.
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

## 31/10/2026 (sétima) — O primeiro dia da escala do vigia (migração 022)

O dono, num vigia do 12x36 com a escala própria a partir de 16/10 (o começo do ponto): *"acho que tá errado essa
interpretação, talvez por conta da saída do dia 16"*. O espelho dava: 16 "descanso · batida faltando" (só o 07:01),
17 normal, **18 falta**, 19 feriado trabalhado.
- **O 07:01 do dia 16 é a saída do turno do dia 15.** Ficou no 16 porque, para a véspera (15), sem escala própria,
  valia a escala PADRÃO (de dia) — e a 021 tinha a mesma regra. Agora (`escalas.dia_de_trabalho`): se a véspera não
  tem escala própria e a escala própria da noite COMEÇA no dia da batida, a batida da manhã é da véspera. A
  **migração 022** acerta o que já ficou gravado assim (mesma regra, em SQL; mês fechado não é tocado).
- **O 18 virou falta por causa do 07:01**: no ciclo automático, o "16" votava como dia trabalhado, empatava com o 17
  e o empate ficava com o mais antigo. Agora **só vota no ciclo o dia em que a pessoa ENTROU**: duas batidas ou mais,
  ou alguma a partir do meio-dia (`escalas.dias_trabalhados`). A batida solta da manhã não vota.
- O 19 (Feriado do Trabalhador da Construção Civil, trabalhado) continua "trabalho em feriado", com as horas em
  extra — é o certo no 12x36 (o feriado trabalhado é pago em dobro, Súmula 444 do TST).
- Teste com o caso dele: a escala da noite a partir de d0, sem escala antes; o 07:01 de d0 vai para a véspera, d0 sem
  aviso, d2 folga (não falta); e a 022 acerta o gravado errado.
**No ramo, esperando o "pode"** — COM A MIGRAÇÃO 022 (apertar "Aplicar atualizações do ponto" ao publicar).

## 31/10/2026 (sexta) — "O PIN não funciona mais": o PIN certo de quem está fora do ponto dizia "PIN incorreto"

O dono: *"tá muito comum o pessoal dizer que o pin não funciona mais. acho que tem algum problema nisso. e ainda tem
essa dificuldade no recebimento da msg."*
- **O defeito**: `acesso.entrar` recusava com "CPF ou PIN incorreto" ANTES de conferir o PIN quando a pessoa estava
  desligada, marcada "não bate ponto" ou tirada do ponto pela planilha (PJ, Pró-labore, ESC RUSSAS — desde 29/10). Com o
  PIN certo, ela ouvia "PIN incorreto" — e o PIN "tinha parado de funcionar". O mesmo acontece se a base de pessoas a
  marca como desligada por engano (data de saída ou fase do Pipefy).
- **Agora**: o PIN é conferido primeiro. Errado (ou CPF que não existe): a frase de sempre, que não revela nada. Certo,
  mas cadastro fora do ponto: **"o seu cadastro está marcado como 'não bate ponto' (ou consta como desligado) — o PIN
  está certo; fale com o DP"** — quem acertou o PIN provou ser o dono do CPF (como já era no criar PIN, 16/10).
- **Para o DP**, na janela da pessoa › "🔑 código de acesso": além de "travado até", **quantos PINs errados seguidos**
  (no 5º, trava 15 min e volta a zero).
- A dificuldade de receber o código ficou visível na tela Mensagens (a entrega de antes, nesta mesma data).
- Não verificado com dado de produção: quantas das queixas são deste caso e quantas são PIN esquecido de verdade. O DP
  passa a ver qual é: "não bate ponto/desligado" na mensagem da pessoa; PINs errados seguidos na janela.
- Teste: PIN criado, pessoa marcada "não bate" → a mensagem com o motivo; PIN errado → a frase de sempre e o contador.
**Publicado em 31/10/2026** (com o "pode" do dono — "publica agora", só com os testes das áreas mexidas; a suíte inteira rodou depois) — SEM MIGRAÇÃO.

## 31/10/2026 (quinta) — A pessoa de teste passa por cima do "não bate" da planilha

O dono: *"preciso ter um usuário de teste para o aplicativo do ponto, no caso sou eu. Ele tá bloqueando pq meu
cadastro é de Pró-labore."* O Modo de teste (Configuração › Teste, `ensaio.py`, 05/10/2026) já fazia a pessoa de teste
bater como ativa fora do Registro de Colaboradores — mas a regra de 29/10 (PJ e Pró-labore, ESC RUSSAS: a planilha tira
do ponto) vinha depois e vencia. Agora, para quem é **pessoa de teste**, o "não bate" da planilha não vale
(`cadastros._trechos_colaborador`). Só ela: os outros Pró-labore continuam fora. As horas dela continuam fora das
contas de CTPS (relatórios e fechamento só contam CTPS). Teste no caso da planilha (Pró-labore marcado de teste entra;
desmarcado, sai de novo).
**Publicado em 31/10/2026** (com o "pode" do dono — "publica agora", só com os testes das áreas mexidas; a suíte inteira rodou depois) — SEM MIGRAÇÃO.

## 31/10/2026 (quarta) — A mensagem que o ponto não mandou aparece na tela Mensagens

O dono, sobre uma pessoa que pede o PIN e não recebe, com o teste da tela Mensagens funcionando: *"preciso que essas
mensagens mesmo que bloqueadas apareçam no mensageria, pq assim tenho como resolver a pane e visualizar a
tentativa."* O ponto segurava alguns envios ANTES de chamar o WhatsApp, e esses não deixavam rastro no registro.
- **Agora ficam anotados** (ERP › Mensagens, `origem = ponto`, `envios.registrar_nao_enviado`), com o motivo:
  - **"Sem destinatário"**: sem celular com DDD no cadastro — código de acesso, QR, ronda do vigia, aviso de batida sem
    foto, pedido negado, mosaico (responsável sem celular);
  - **"Barrado na origem"** (situação nova na tela da Mensageria): código pedido com CPF que não está no cadastro,
    pessoa desligada, mais de 3 códigos na hora; QR para pessoa desligada ou "não bate ponto", mais de 3 pedidos de QR
    no dia, QR cancelado na fila (a pessoa saiu antes do envio);
  - **"Falhou"**: o envio do código deu erro antes de chegar à mensageria.
- Quem pede continua recebendo a mesma resposta ("se o CPF estiver cadastrado…") — não revela se o CPF existe; o
  motivo só aparece para quem vê a tela Mensagens. O CPF digitado fica anotado (é o que mostra o "digitou errado").
- O que a mensageria já decidia continua como era (enviada, falhou, tipo desligado, teto do WhatsApp).
- Fora: a fila de QR esperando a janela do horário ou os tetos de hora/dia — não é bloqueio, sai depois (a fila do
  ponto mostra em Configuração › Mensagens).
- Na Mensageria, só a etiqueta e o filtro "Barrado na origem" (`mensageria/HISTORICO.md`, 31/10/2026).
- Testes: CPF não achado, teto da hora (o 4º não sai) e sem celular no código de acesso; QR sem celular.
**Publicado em 31/10/2026** (com o "pode" do dono — "publica agora", só com os testes das áreas mexidas; a suíte inteira rodou depois) — SEM MIGRAÇÃO.

## 31/10/2026 (terceira) — O intervalo pré-assinalado aparece no espelho, diferenciado

O dono, sobre as obras que, por convenção coletiva, só batem a entrada e a saída: *"não quero aplicar batidas. Quero
pré-configurar essas obras dessa forma. Teve batida no início e no fim, aparece a batida do intervalo diferenciada."*
- **A configuração já existia** (migração 009, 09/10/2026): Ponto › Configuração › Obras e cerca › coluna "Batidas por
  dia" → "2 (só entrada e saída)". O cálculo já completava o almoço pela escala (sem falta, sem batida faltando, sem
  intervalo curto, o almoço não vira extra, e o ranking conta o dia em dia) — mas o espelho só mostrava as duas batidas.
- **Agora o intervalo considerado aparece**, sem virar batida (nada é gravado — Portaria 671): no dia com a entrada e a
  saída numa obra assim, os horários do almoço da escala entram entre as duas, **em itálico, com borda pontilhada**
  ("intervalo pré-assinalado (convenção da obra): considerado pela escala, sem batida"). Na janela da pessoa (Dia e
  Espelho do mês), no Quadro, no "meu espelho" do aplicativo e na impressão ("(pré-assin.)", com a legenda).
- Só aparece quando o cálculo de fato completou: quem bateu as quatro, ou saiu antes do almoço, ou feriado — nada.
  `apuracao.intervalo_pre_assinalado` (pura) e o campo `pre_assinalado` do dia no espelho.
- Testes: a regra pura (as duas batidas → 11:00 e 12:00; quatro batidas, a manhã só ou feriado → nada) e, com banco, a
  obra marcada como 2 batidas — o dia mostra o intervalo e as batidas continuam só as duas. Conferido no navegador.
**Publicado em 31/10/2026** (com o "pode" do dono) — SEM MIGRAÇÃO.

## 31/10/2026 (segunda) — O calendário de horas extras: cabeçalho parado, um clique marca, dois abrem o espelho

O dono: *"como são muitos funcionários, na tela do calendário devemos fixar o cabeçalho com os dias. Outra coisa, como
quando clicamos no dia abre o espelho, a função de marcar pra negar as horas não tá funcionando. Talvez possamos abrir o
espelho se clicar duas vezes."*
- O calendário fica numa caixa com rolagem própria (altura da tela): o cabeçalho dos dias de cada obra fica parado ao
  rolar, e a coluna dos nomes ao rolar para o lado.
- **Um clique na hora extra marca/desmarca para negar** (a caixa "Marcar horas para negar" saiu); **dois cliques em
  qualquer dia abrem o espelho** — os dois cliques do duplo marcam e desmarcam, a hora fica como estava. O nome da
  pessoa continua abrindo com um clique.
- Conferido no navegador: um clique marca sem abrir nada; dois cliques abrem o espelho com a marca intacta; o cabeçalho
  fica no topo da caixa ao rolar.
**Publicado em 31/10/2026** (com o "pode" do dono) — SEM MIGRAÇÃO.

## 31/10/2026 — O feriado pela obra onde a pessoa trabalha

O dono: *"para efeito de feriado, a obra onde a pessoa está trabalhando funciona né?"* — funcionava só no dia COM
batida; no dia do feriado, que quase nunca tem batida, valia a obra do CADASTRO. Quem trabalha numa obra e está
cadastrado em outra perdia o feriado estadual, municipal ou da obra onde trabalha; quem não tem obra no cadastro (o
administrativo) só tinha o nacional — e o dia virava falta.
- **Agora** (espelho, e com ele quadro, relatórios e fechamento): o dia sem batida fica com a obra do **dia com batida
  mais perto** — o último antes dele; sem nenhum antes, o primeiro depois (`espelho.obra_vizinha`); só sem batida
  nenhuma no período, a do cadastro. O dia ganha `obra_do_dia`, que o fechamento (a falta no calendário da supervisão,
  o percentual da hora extra) e o quadro passam a usar.
- **O painel do dia (Hoje)**: sem batida hoje, a obra da última batida dos 60 dias; senão, a do cadastro.
- Teste: o João, cadastrado na A, bate segunda e terça na B; o feriado da obra B na quarta vale para ele (antes: falta).
**Publicado em 31/10/2026** (com o "pode" do dono) — SEM MIGRAÇÃO.

## 30/10/2026 (madrugada de 31, quarta) — A obra da pessoa é a do ponto

O dono, no espelho de um administrativo que bate todo dia na BWSNE e aparecia "sem obra": *"o sistema precisa inferir do
próprio ponto qual a obra dessa pessoa. Na falta do ponto que usamos o cadastro."*
- **Janela da pessoa**: o cabeçalho mostra a obra em que ela mais bateu nos **últimos 60 dias**
  (`cadastros.obra_pelo_ponto`); sem batida, a do cadastro; ao passar o mouse, de onde veio (e a do cadastro, se diferir).
- **Espelho** (tela, impressão e o "meu espelho" do aplicativo): a obra do período = a mais batida nele
  (`cadastros.obra_mais_batida`; empate: a mais recente); sem batida, a do cadastro. O `[CÓDIGO]` da impressão passa a
  marcar a batida fora dessa obra. O cadastro continua à parte (`obra_cadastro`).
- **Não mudou**: os filtros "Obras do cadastro" (continuam do cadastro — "Bateu nas obras" já filtra pelo ponto), a obra
  padrão do pedido de ajuste e o escopo de quem vê quem (o alcance do supervisor continua pelo cadastro e pelas batidas,
  como era).

## 30/10/2026 (madrugada de 31, terceira) — O ✓ do dia trabalhado no calendário de horas extras

O dono: *"no calendário, só vemos horas + ou -. Nos dias em que não há, mesmo que tenha tido batida, fica vazio. Qual sua
sugestão pra diferenciar aquele dia como um dia em que o colaborador trabalhou?"* — sugerido e feito: **✓ cinza** no dia
trabalhado sem hora a mais nem a menos (ao passar o mouse, as horas do dia). Fica: azul = extra, vermelho = a menos ou
falta, ✓ = trabalhou no horário, vazio = não trabalhou (folga, descanso, sem escala). Na tela, no papel (com a legenda) e
no Excel da supervisão. **Só de quem já está no calendário** (tem hora a mais ou a menos no período): o calendário é das
horas extras — com o ✓ de todo mundo, entraria a empresa inteira (`fechamento.trab_dias`). Se o dono quiser todo mundo,
é tirar esse filtro.

## 30/10/2026 (madrugada de 31, segunda) — A mensagem da qualidade para o WhatsApp, e o % de ajustes

O dono: *"crie uma mensagem para whatsapp da qualidade do ponto. Quero as top 10 obras. Top 20 funcionários. Quero
ainda um explicativo do cálculo e quero na tela de qualidade uma coluna com o % de ajustes de batidas do total."*
- **% de ajustes** = das batidas do mês, quantas entraram por pedido de ajuste (a incluída em lote pela gestão — falha
  do sistema — não conta, como já não contava nos problemas). Coluna nas tabelas de obras, responsáveis e pessoas (o
  "x de y" ao passar o mouse) e um número no topo. Conta todas as batidas válidas do período, também as de dia de
  descanso; a obra de cada batida é a do dia. Não muda a nota: é só para ver.
- **Mensagem para o WhatsApp**: botão na barra da Qualidade. Abre o texto pronto (`qualidade.mensagem_whatsapp`):
  título com o mês, as **10 melhores obras** (código, nome, nota, pessoas, % de ajustes) e os **20 melhores
  funcionários** (só o primeiro e o último nome, a obra, a nota e os ajustes) — só quem ENTRA no ranking, na ordem da
  tela, no alcance de quem pede —, e **"Como a nota é calculada"**, escrito a partir das próprias constantes da conta
  (se o peso mudar, o texto muda junto). O texto pode ser mexido; "Copiar" e "Abrir no WhatsApp" (o link do próprio
  WhatsApp, a pessoa escolhe o grupo). **Nada é enviado sozinho** — escolha feita para não escrever em grupo sem
  ninguém olhar; o envio automático (todo dia 1º, por exemplo) é um passo a mais, se o dono quiser.
- Testes: a regra pura do % e a mensagem (top 10/20, quem não entra fica de fora, nome curto, vazio), e a rota com banco.

## 30/10/2026 (madrugada de 31) — O começo do ciclo automático: as noites seguintes também votam

O dono, mostrando um vigia que trabalha nos dias ímpares desde 17/10: o dia 16 saiu **falta** e o 17 **noite fora do
ciclo**. Causa: no começo do ponto não havia noite trabalhada ANTES para votar, e a regra caía no "dia de trabalho"
informado ao dar a escala (o "16"), que passava por cima das batidas. Agora, com menos de 2 noites nos 30 dias antes,
votam também as dos 30 dias depois; e o dia informado só vale para quem não tem batida nenhuma. É cálculo, não dado
gravado: o espelho do vigia se corrige sozinho, sem botão. Teste com o caso dele (16 folga, 17 e 31 trabalho).

Na mesma rodada, dois casos do mesmo vigia:
- **23/10, com 17:05, 22:56, 04:48 e 07:07** — o dono: *"imagino que essas batidas tenham sido erro (…) não tem como o
  vigia sair e voltar"*. Antes, a conta fazia dois pares (saiu às 22:56, voltou às 04:48): 8h10 e 3h50 de falta. Agora,
  no turno de um período só que passa da meia-noite (também na noite de folga do 12x36), a batida do meio não é saída:
  vale da primeira à última, e o dia ganha o aviso **"Batida no meio do turno da noite — não contou como saída"**. A
  marcação continua no registro (Portaria 671). ⚠️ Escolha: se um vigia sair de verdade e voltar, a noite é paga
  inteira — o aviso e as rondas da noite (ronda perdida) são o que mostra isso ao DP.
- **21/10, entrou 16:45 e saiu 07:01 numa escala até 05:00** — a conta já dava 14h16 e 2h16 de extra, mas o espelho
  oferecia "Saída 05:00" para incluir, porque a saída estava a mais de 2 h da escala. Agora o dia completo (batidas em
  número par, ao menos as previstas) não pede batida nenhuma (`ajustes.faltantes`).
**Publicado em 31/10/2026** (com o "pode" do dono, junto com a mensagem da qualidade, o ✓ e a obra pelo ponto) — SEM MIGRAÇÃO.

## 30/10/2026 (noite, terceira) — O ciclo 12x36 automático, e a noite fora do ciclo para o DP decidir

O dono: *"essa questão do dia em que o ciclo começa, eu acho que acaba atrapalhando. Na minha visão, isso poderia ser
considerado de forma automática."* E logo depois: *"pode deixar tentar de forma automática, e se de repente tiver alguma
anormalidade, aí o sistema criticar e pedir para o DP decidir."*
- **A regra** (`apuracao.dia_do_ciclo`, pura): o dia é de trabalho se, nos **30 dias antes dele**, a pessoa trabalhou
  mais vezes na mesma paridade (dia sim, dia não) do que na outra. Empate: vale a mais antiga da janela — o ciclo que já
  vinha (uma noite a mais no começo não vira o ciclo). O próprio dia não vota: por isso a noite trabalhada fora do ciclo
  é **extra**, não muda o ciclo. Sem batida nos 30 dias: a paridade da última noite trabalhada. Nunca trabalhou: o dia
  informado ao dar a escala, se houver (os já cadastrados, como o "16" do dono, ficam só para isso); senão, nada é
  previsto até a primeira batida — e ela abre o ciclo.
- **Só conta o que é da vigência**: dar a escala de novo a partir de um dia recomeça o ciclo dali. É o remédio quando a
  turma muda de vez (sem isso, a maioria leva ~15 dias para virar).
- **A anormalidade, para o DP**: a noite trabalhada fora do ciclo ganha o alerta **"Noite fora do ciclo 12x36 — troca de
  plantão ou extra? O DP decide"** (`FORA_DO_CICLO`, atenção) e entra na aba **Conferência do fechamento**, dizendo o
  que fazer: troca de plantão = lançar a compensação (a folga e o dia trabalhado); extra = deixar; o ciclo mudou = dar a
  escala de novo a partir desse dia. A noite de trabalho sem batida já aparece como falta.
- **Telas**: as três perguntas do "dia de trabalho" saíram (trocar escala na janela da pessoa, definir em lote no Quadro,
  aplicar na tela de escalas); a coluna "Início do ciclo" do histórico de escalas saiu; o texto da escala 12x36 diz que o
  ciclo vem das batidas. A API continua aceitando o dia (quem mandar, vale como ponto de partida antes da 1ª batida).
- **Onde o cálculo lê**: espelho (e o Lote do quadro/relatórios/fechamento), painel do dia, rondas — os dias trabalhados
  são lidos de uma vez por pessoa (`escalas.dias_trabalhados`).
- **Testes**: a regra pura (próxima do ciclo, folga, noite a mais que não vira o ciclo, troca de turma que vira com o
  tempo, empate, sem batida na janela, nunca trabalhou) e, com banco, o 12x36 sem dia informado: terça folga, sexta
  falta, e a noite de terça trabalhada com o alerta e 13 h de extra.

## 30/10/2026 (noite, segunda) — O turno da noite pela escala (migração 021)

O dono, sobre os vigias: *"a escala está cadastrada, o pessoal está vinculado (…) a escala das pessoas está essa vigia
noturna 12x36, então deveria estar funcionando (…) se eu já estou dizendo que é vigia noturno, o dia que ele inicia de
tarde (…) é para ser considerado o horário de entrada dele. E amanhã, no dia seguinte, é de saída."*
- **O defeito**: o dia de trabalho da batida (`data_referencia`) é gravado na hora em que ela entra, e só a jornada
  "Vigia noturno (2 batidas)" — escondida no botão "Jornada e obras" da janela da pessoa, que o dono não achou — levava a
  batida antes das 10h para o dia anterior. Com a escala 12x36 17h–6h e a jornada padrão, a saída das 6h caía no dia
  seguinte: o turno partia em dois (falta parcial num dia, batida solta no outro).
- **Agora a escala decide** (`escalas.dia_de_trabalho`, usado na batida, no ajuste incluído, no pedido de ajuste e no
  aviso de fora do contrato): batida antes das 10h vai para a véspera se a escala da véspera passa da meia-noite —
  12x36 da noite sempre (até na noite de folga do ciclo, para a noite trabalhada fora do ciclo ficar inteira); semanal
  se o turno daquele dia passa, ou se todos os turnos da escala são de noite. A jornada "vigia noturno" continua valendo.
- **Dar ou trocar a escala, ou mudar o horário dela, refaz o dia das batidas** de quem está nela, desde a vigência
  (`realinhar_dias`). Só o dia de trabalho muda: hora, obra e o hash encadeado ficam (o hash não inclui o dia —
  Portaria 671). Mês fechado não é tocado.
- **A migração 021** faz o mesmo, uma vez, para o que já estava gravado no dia errado (mesma regra, em SQL).
- **O "dia de trabalho" do 12x36** (o que confundiu o dono) continua sendo pedido, porque é ele que diz quais noites são
  de trabalho e quais de folga — noite sem batida no dia de trabalho é falta; noite trabalhada no de folga é extra. O
  texto agora explica isso e diz que, no turno da noite, é o dia em que a pessoa ENTRA.
- De passagem: o filtro "Jornada" da Configuração nunca achava vigia (comparava com "VIGIA", e o valor é
  "VIGIA_NOTURNO_2"/"VIGIA_DIURNO_2"). Corrigido.
- **Testes**: a regra pura (12x36 de noite e de dia, semanal com uma noite, semanal só de noite) e, com banco, o turno
  17h–6h: a saída antiga volta para a véspera ao dar a escala, a nova já nasce certa, o espelho dá 13 h OK sem alerta de
  intervalo, a 021 acerta o gravado errado, e voltar à escala do dia desfaz.

## 30/10/2026 (noite) — As horas a menos no calendário, e abater os atrasos das extras

O dono: *"nesse calendário aqui de hora extra (…) é tudo hora positiva. Mas eu acredito que tenha pessoas com horas
negativas (…) colocar de azul as horas positivas, de vermelho as horas negativas. E, da mesma forma da tolerância, a gente
poder considerar ou não considerar as horas negativas (…) o cara tem meia hora negativa e tem uma hora de extra, aí ele
vai ficar só com meia hora de extra."*
- **Antes**: o calendário só mostrava a hora extra; o dia com horas a menos não aparecia, e nada era abatido.
- **Agora**, no calendário (tela, papel e Excel): extra em **azul**, hora a menos em **vermelho** (atraso, saída
  antecipada e falta parcial com as horas; a falta do dia inteiro escrita "falta"). Por pessoa, as colunas **Extras,
  Atrasos** e, com o abatimento ligado, **Saldo**.
- **"Abater os atrasos e as saídas antecipadas das horas extras"** (caixa na barra do Fechamento, guardada em
  `ponto.parametros`, "fechamento.abater_negativas"; desligada de início). Ligada, o atraso do período sai das horas extras
  do MESMO período (mês anterior com mês anterior, corrente com corrente), e o atraso abatido deixa de ir para a planilha de
  faltas parciais — senão seria descontado duas vezes.
- **Escolhas feitas (o dono pode mudar)**:
  - abate-se **só "Atraso ou saída antecipada"**. A falta parcial (batida que falta) e a falta inteira **não** são
    abatidas: vão para o desconto como antes, porque não há como provar que ele estava lá;
  - abate-se primeiro a hora extra de **percentual menor** (50% antes de 100%) — a mais barata para a empresa sai primeiro;
  - quem é do banco de horas não abate (o banco já faz essa conta).
- **A tolerância** do 1zz9 vale também para o atraso: o dia com atraso de até N minutos fica de fora (entre parênteses,
  em cinza), não é abatido nem descontado.
- ⚠️ **Risco, dito ao dono**: compensar atraso de um dia com extra de outro é, na prática, um banco de horas; sem acordo
  (individual ou coletivo) a fiscalização e a Justiça podem não aceitar, e a hora extra abatida pode ser cobrada depois.
- **Testes**: o abatimento puro (menor percentual primeiro, o que sobra), o saldo da supervisão, e o fechamento de verdade
  com a falta parcial no calendário, a escolha guardada e a falta parcial que não é abatida.

## 30/10/2026 (tarde, segunda) — A hora extra pequena, desconsiderada no fechamento

O dono: *"eu queria poder ter uma tolerância desses minutos para contabilizar hora extra (…) na hora de gerar o
relatório, ou depois de gerar (…) diferença de 10 minutos, desconsiderar e não pagar extra (…) às vezes tem muitos dias
com 2 minutos, 1 minuto, oito minutos, até 12 minutos, e eu queria poder decidir se isso vai entrar ou não."*
- No Fechamento da folha, o campo **"Desconsiderar a hora extra do dia de até (minutos)"**. O dia com hora extra até esse
  número não entra em nada do fechamento (o .ps, as quebradas, o mês anterior). **Mudou o número, monte de novo**: o
  fechamento é refeito na hora. A escolha fica guardada (`ponto.parametros`, "fechamento.he_minima"); 0 = tudo entra;
  até 120 min.
- O calendário de horas extras (tela, papel e Excel) mostra o dia desconsiderado **entre parênteses, em cinza**, fora do
  total; o Resumo e os números do topo dizem quantos dias e quantos minutos ficaram de fora.
- **Só o fechamento**: o espelho e os relatórios continuam mostrando a hora extra do dia como ela é (é a conta do ponto;
  a tolerância da lei — 5 min por batida, 10 no dia — já vale lá).
- ⚠️ **Risco, dito ao dono**: acima da tolerância da lei (10 min no dia), a hora extra é devida; desconsiderar 12 min é
  decisão da empresa, com o risco de cobrança depois.
- **Testes**: o fechamento de verdade com um dia de 8 min de extra e a tolerância de 10 (fica de fora, os 30 min das
  quebradas continuam), a escolha guardada e o limite de 120.

## 30/10/2026 (tarde) — A declaração por quantidade de horas, e o calendário que abre o espelho (migração 020)

**A declaração de comparecimento.** O dono: *"a declaração fala de um determinado horário, 6h20 às 9h45. A gente
precisa ter a opção de aprovar na totalidade, ou o período, ou uma determinada quantidade de horas — para a pessoa ficar
devendo ou levar falta. Da forma que está, acho que se eu aprovar, fica valendo o dia inteiro."* A aprovação já pedia
"o dia todo / só o período / nada" (27/10/2026); faltava a quantidade — e havia um erro no "só o período".
- Nova opção na aprovação (e no "Lançar afastamento" do DP): **"Uma quantidade de horas"**, com o campo "quantas horas"
  (ex.: 2:30). Guardada em `ocorrencias.abono_minutos` (**migração 020**) — `minutos` continua sendo o período escrito
  na declaração. O dia mostra "Declaração de comparecimento — 2h00 abonadas".
- **O "só o período" abona só o que cai DENTRO do expediente** (achado no exemplo dele): a declaração das 6h20 às 9h45,
  numa escala que começa às 7h, abona **2h45**, não 3h25 — o que veio antes do expediente não era hora devida. Antes
  abonava o período inteiro, e podia sobrar abono para cobrir atraso de outra hora do dia.
- **Testes**: `test_a_declaracao_abona_o_periodo_dentro_do_expediente_ou_uma_quantidade_de_horas` (chegou às 10h: devia
  3 h; o período abona 2h45 e sobram 15 min; 2 h abonadas deixam 1 h; zero horas é recusado).

**O calendário de horas extras abre o espelho.** O dono: *"no calendário (…) eu queria poder clicar no dia da pessoa e
visualizar o espelho do ponto, porque fica mais fácil de entender a situação."*
- Clicar em **qualquer dia** da pessoa (com hora extra ou não) abre a janela dela na aba **Espelho do mês**, no mês
  daquele dia. Para negar horas, liga-se a caixa **"Marcar horas para negar"** — aí o clique marca, em vez de abrir (os
  dois cliques no mesmo lugar se confundiriam). Ensaio no navegador: os dois modos.

## 30/10/2026 — A compensação conferida: a folga só vale se o dia trabalhado pagar

O dono: *"como o sistema trata uma compensação que ainda não aconteceu, e se ela não acontecer?"* Até aqui, aprovada,
a compensação valia de cara e para sempre: a folga ficava abonada e o dia trabalhado perdia a hora extra — sem conferir
se o dia trabalhado aconteceu. Dois buracos: **folga antes, trabalho depois, e a pessoa não vem** → folga de graça,
calada; **trabalho antes, folga depois, e a pessoa trabalha na folga** → trabalha dois dias, recebe por um. Propus
transformar em falta sozinho ou só avisar; o dono: *"isso, automático"*.
- **Enquanto o dia trabalhado não passou**, a folga vale (é o combinado), e o dia mostra "a cumprir". O fechamento da
  folha avisa na **Conferência** ("folga em dd/mm ainda a cumprir — se não for cumprido, a folga vira falta").
- **Passado o dia trabalhado**, a folga só fica abonada no que ele **rendeu além do previsto** (num dia de descanso,
  tudo o que trabalhou; num dia de trabalho, só o que passou da escala). Não rendeu nada → a folga vira **falta**;
  rendeu parte → **falta parcial** do resto. Aviso **"compensação não cumprida — a folga virou falta"** (espelho,
  Quadro, alertas). Folga de vários dias: o que rendeu cobre os dias em ordem.
- **Desfazer** é o de sempre: lançar o ajuste do dia trabalhado (se trabalhou e não bateu), ou cancelar/abonar.
- **Bateu no dia da folga**: as horas contam como hora extra (dia de descanso trabalhado), com o aviso **"trabalhou no
  dia da folga compensatória"** — antes ficavam escondidas atrás do "bateu em dia abonado".
- **Testes**: `test_a_compensacao_conferida` (o sábado sem batida → falta; 4 h no sábado → falta parcial de 5 h; o dia
  trabalhado no futuro → a folga vale e fica a cumprir; bateu na folga → as horas contam).

## 29/10/2026 (madrugada) — Abonar a falta parcial, negar a hora extra na conferência, e só CTPS nas horas (migração 019)

**O abono da falta parcial** (Quadro › marcar as linhas › "Abonar a falta"). O dono: *"abonar a falta parcial tem
sentido. Porque, de repente, aquela pessoa trabalha um período e o resto do período a gente quer abonar."*
- O botão passa a aceitar, além da falta do dia inteiro, a **falta parcial** e o dia com **horas a menos**: aí abona
  **só as horas que faltaram** (um ABONO com `minutos` = o débito do dia). O que a pessoa trabalhou continua contado; o
  dia fica certo, com "<abono> — Xh abonadas" no espelho, e sai do relatório de Faltas e do fechamento.
- No fechamento, **"Atraso ou saída antecipada" ficou com esse nome** (o dono preferiu manter).

**As horas extras por obra, na tela, e negar uma hora** (Relatórios › Fechamento da folha › "📋 Horas extras por
obra", a primeira aba). O dono: *"eu quero ter em tela também a possibilidade de visualizar aquilo ali (…) agrupado por
obra (…) bate o olho, e pode abrir o espelho da pessoa (…) eu preciso poder discordar com alguma hora: selecionar e
negar que aquela hora daquele dia entre no cálculo."*
- O mesmo calendário do papel (26 a 25, por obra), na tela: o nome abre a janela da pessoa no dia; **clicar na hora
  marca**; "🚫 Negar as horas marcadas…" pede o motivo; "↩️ Voltar a contar" desfaz.
- **A hora negada sai de todas as contas** — o espelho zera o extra do dia e mostra "hora extra negada na
  conferência"; daí o fechamento, o .ps, os relatórios e o banco de horas não contam. No calendário (tela, papel e
  Excel) ela aparece **riscada**, fora do total. As batidas não mudam (Portaria 671). Fica guardado o motivo, quem
  negou e quando, e quem voltou a contar (`ponto.horas_extras_negadas`, **migração 019**). Só o DP.
- ⚠️ **Risco, dito ao dono**: hora trabalhada é devida mesmo sem autorização; negar aqui tira da folha, mas não tira
  o direito da pessoa. É decisão dele, e o registro do motivo é o que sustenta a decisão.

**Só CTPS nas contas de horas.** O dono: *"tem um pessoal que é autônomo RPA (…) diarista (…) só entra para efeito
de hora o pessoal que na coluna D é CTPS. Demais, que são prestador de serviço, eles não entram no cálculo de hora
extra (…) eles batem ponto, tudo normal."*
- A coluna é o **"Tipo"** da planilha de colaboradores (CTPS × Prestador de Serviço — a mesma que a Análise de SPs usa
  para os diaristas, `analisesps.colaborador.tipo`). Quem não é CTPS **bate ponto normalmente** (Quadro, aplicativo,
  qualidade), mas **fica fora** do fechamento da folha (faltas, horas extras, .ps, horas extras por obra) e dos
  relatórios de **Faltas, Horas extras e Banco de horas**.
- **Vazio entra** (como CTPS) e o fechamento avisa na Conferência — para um buraco na planilha não sumir com as faltas
  de ninguém calado. Escolha minha.
- Com a base de pessoas no ERP (e não na planilha), não há o tipo: todos entram.

- **Testes**: o fechamento de verdade com a hora negada (some do .ps e do calendário, o espelho mostra, volta a
  contar) e o prestador de serviço fora; o abono parcial no `test_ponto_quadro_banco.py`. Ensaio no navegador: o
  calendário na tela, marcar e negar uma hora.

## 29/10/2026 (noite, última) — O abono pedido por quem faz requisição

O dono, procurando onde incluir um abono (só existia no "Lançar afastamento" do DP e no "Abonar a falta" do Quadro,
também do DP): *"não é pra ser só o DP. É pra ser pros mesmos que podem fazer requisição. Entendo que tenha que ser
uma das opções de 'O que você vai entregar ou pedir?'"* E: *"vai ser em Atestados, afastamentos e ajustes se for pra
outra pessoa, ou dentro de Meu mês, né isso?"* — sim.
- **No aplicativo**, a opção **"Abono (viagem a serviço, dispensa…)"** entrou nas três listas de pedido: o ponto da
  obra ("O que vai entregar ou pedir?"), o pedido **por outra pessoa** ("Atestados, afastamentos e ajustes") e o
  **Meu ponto** (Pedidos, e o 🗓️ do dia em Meu mês) — este só para quem faz pedidos pelo próprio celular, como os
  outros pedidos. Escolhe-se o abono (os da Configuração › Licenças e abonos, ou "Outro motivo"), os dias e o
  **motivo, obrigatório** (10 letras ou mais).
- **No ERP**, a janela da pessoa ganhou **"Pedir abono"** para quem trata o ponto (o supervisor). O DP continua
  lançando e aprovando de uma vez pelo "Lançar afastamento".
- **Vai para a validação**: "Quem valida o quê" ganhou a linha **Abono** (administração da obra, DP, ou os dois —
  nasce no DP, como era). Férias e afastamento continuam só do DP.
- **Testes**: `test_o_abono_e_pedido_por_quem_faz_requisicao_e_vai_para_validacao` (o supervisor pede, sem motivo não
  vai, o DP aprova e o dia fica abonado; férias continua recusada). Ensaio no navegador: "Pedir abono" no ERP e a
  opção no aplicativo do administrativo da obra.
- **Proposto e ainda sem resposta do dono**: o "Abonar a falta" do Quadro aceitar falta parcial; abono de só parte do
  dia ("das… às…"); e o nome "Atraso ou saída antecipada" do fechamento virar "Horas a menos no dia", com o porquê.

## 29/10/2026 (noite, sexta, segunda parte) — As horas extras para a supervisão, e o percentual por estado ou obra

**As horas extras para a supervisão conferir** (`fechamento.supervisao`). O dono: *"quando a gente faz esse
fechamento de horas extras, a gente sempre encaminha o relatório para os encarregados e supervisores de obra (…)
para saber se tem coerência aquela quantidade de horas extras de cada funcionário (…) visualmente é uma espécie de
calendário, cada coluna do dia (…) do dia 26 em diante (…) agrupado por obra (…) em cada dia a quantidade de horas
extras e um totalizador ao final (…) em paisagem, para caber tudo."*
- No Fechamento da folha, dois botões: **"📋 Horas extras por obra — imprimir (paisagem)"** (uma folha por obra, para
  mandar a cada encarregado a dele, com espaço para "conferido por" e data) e a mesma coisa em **Excel**.
- **Nome**: "Horas extras para conferência da supervisão" (escolha minha).
- O período é o da **folha inteira** (26 a 25). Fim de semana sombreado. A obra é **a do dia** (onde a pessoa bateu):
  quem trabalhou em duas obras aparece nas duas, cada uma com os seus dias.
- Entra **toda** hora extra, inclusive a de quem tem banco de horas (marcada "(banco)" — vai para o banco, não para a
  folha): a supervisão confere a coerência do que foi feito.

**O percentual da hora extra, por estado ou por obra** (`core/hora_extra.py`). O dono: *"tem a hora de 70%, de 100%
(…) isso varia em função de quê? Atualmente a gente tem obra em Pernambuco, em São Paulo e no Ceará (…) colocar a
UF, ou mesmo especificamente uma obra, e a gente definir qual é o percentual de hora extra no domingo, no sábado e no
feriado (…) a hora extra da semana é tantos por cento."*
- Configuração › **Jornada e feriados** › **"Percentual da hora extra"**: o **padrão da empresa** e as regras **por
  estado** e **por obra**, cada uma com **semana, sábado, domingo e feriado**. Escolhe-se entre **50%, 60%, 70% e
  100%** — os que o Fortes tem evento (006, 005, 004, 003).
- **A ordem**: a regra da obra vence a do estado, que vence a padrão. O feriado vence o dia da semana. O estado é a UF
  da obra (coluna AC da C. Diários). O dia vai pela obra em que a pessoa bateu.
- **O padrão nasce como estava**: 50% de segunda a sábado, 100% no domingo e no feriado. **Até alguém cadastrar PE, SP e
  CE, tudo sai pelo padrão** — é a primeira coisa a fazer antes de gerar o .ps de verdade.
- No mês anterior, o total de horas extras de cada pessoa sai separado nas quatro colunas (50, 60, 70, 100%).
- Guardado em `ponto.parametros` ("hora_extra.percentuais") — sem migração. Só quem configura o ponto mexe.
- **Testes**: `test_o_percentual_da_hora_extra_por_estado_e_obra`, `test_as_horas_extras_para_a_supervisao_por_obra`
  e o fechamento de verdade com a regra da obra (70% → evento 004 no .ps). Ensaio no navegador: cadastrar a regra da
  obra e imprimir as horas extras por obra.

## 29/10/2026 (noite, sexta) — O fechamento da folha para a contabilidade, com os arquivos .ps do Fortes

O dono mandou o documento `prompt_geracao_ps.md` (o formato do .ps de importação de eventos do Fortes RH / AC
Pessoal) e explicou o fechamento: *"o fechamento da folha é feito do dia 26 do mês anterior ao dia 25 do mês
corrente (…) esse período final pode necessitar algum ajuste de ponto, então a gente deixa esses dias para não ter
perigo de dar errado, e ter tempo de mandar com antecedência para a contabilidade."* E por que separar: *"o sistema
eu não consigo mais lançar as faltas do mês anterior via arquivo, nem horas extras (…) e eu só consigo lançar via
arquivo as faltas integrais ou as horas integrais (…) meia falta do dia não consigo lançar, meia hora também não."*

**O que sai** (`core/fechamento.py`; Relatórios › "📦 Fechamento da folha (contabilidade)", só o DP —
`aprovar_afastamento`). Escolhe-se a **folha do mês** (ex.: 11/2026) e o sistema separa:
- **Mês anterior** (26/10 a 31/10), em planilha: as **faltas** (integrais e parciais, com as horas que faltaram) e
  as **horas extras** (o total de cada pessoa, separado em 50% e 100%, mais uma aba dia a dia para conferir).
- **Mês corrente** (01/11 a 25/11): as **faltas integrais** no `.ps` de faltas (evento 001, uma linha por dia); as
  **horas extras inteiras** no `.ps` de horas extras; e em planilha as **faltas parciais** (com atrasos e saídas
  antecipadas além da tolerância, marcados) e as **horas quebradas** (os minutos que sobram).
- Uma aba **Conferência**: quem ficou fora do .ps por falta de ID Fortes, quem tem banco de horas, quem está
  afastado no cadastro.
- **Os arquivos**: o **relatório geral** (Excel, uma aba por parte, com o Resumo na frente), os **dois .ps**, cada
  aba sozinha em Excel, ou **os três num zip** — *"para a gente poder encaminhar em três arquivos"*.

**As contas são as do espelho** (a mesma régua do Quadro e dos relatórios, com a falta parcial desta noite).

**Escolhas minhas, a confirmar com o dono** (estão também no Resumo do Excel):
1. **Percentual da hora extra**: ~~domingo e feriado = 100%, os outros dias = 50%~~ — virou configuração por estado e
   por obra na mesma noite (ver a seção seguinte).
2. **Hora extra no .ps do mês corrente**: o **total do período** de cada pessoa e percentual, com a **data do dia
   25** — o inteiro no arquivo, os minutos na planilha de quebradas. **Confirmado pelo dono** (29/10/2026): *"para
   hora extra, não vai precisar por dia. Basta que seja tantas horas a tantos por cento."* A opção "dia a dia" saiu
   da tela (continua no código, `montar(por_dia=True)`, se um dia for preciso).
3. **Quem tem banco de horas** fica fora das horas extras e das faltas parciais (vão para o banco); a falta do dia
   inteiro entra.
4. O **ID Fortes** vem do cadastro da Análise de SPs (`analisesps.colaborador.id_fortes`, a aba "ID Fortes" da
   planilha). Sem ele, a pessoa não entra no .ps e aparece na Conferência; com mais de 6 dígitos, também (não se
   trunca calado).
5. **Afastado no cadastro** com falta no período vai para a Conferência: o dia pode ser do afastamento, sem a licença
   lançada.

**O que confirmar antes de usar na folha de verdade** (o próprio documento do dono avisa — o .ps é posicional):
o espaçamento do cabeçalho, os zeros do rodapé (o modelo do documento tem um caractere a mais que o exemplo; usei o
exemplo) e se a quantidade de horas é inteira pura (`0012` = 12 h). O caminho: importar no Fortes um .ps gerado
aqui, de um mês de teste, e comparar com um .ps que já foi aceito. Os três ficam em constantes no topo do
`fechamento.py`, para trocar num lugar só.

- **Testes**: `tests/test_ponto_fechamento_banco.py` (os períodos, o exemplo do documento linha por linha, as regras
  do dia, e o fechamento de verdade: 2h30 de extra viram 2 h no .ps e 30 min nas quebradas; a falta parcial; o ID
  Fortes; o Excel com as abas; o zip). Ensaio no navegador: montar a folha de 11/2026 e baixar o zip.
- **Ficou de fora**: o adicional noturno, o DSR e o 60%/70% — não foram pedidos.

## 29/10/2026 (noite, quinta) — "Não bate ponto" na janela da pessoa e em lote (migração 018)

O dono, antes de publicar a falta parcial: *"eu queria poder selecionar pessoas e dentro do cadastro dela dizer que
aquela pessoa não bate ponto. Como é que eu posso fazer?"* A marcação existia (`colaborador_config.ativo`), mas
escondida num campo do diálogo "Jornada e obras" — e sem guardar por quê.
- **Na janela da pessoa** (aba Cadastro): a linha **"Bate ponto?"** diz Sim, ou Não com o motivo, quem marcou e
  quando (ou "pela planilha de colaboradores: …", quando é a planilha que tira). E o botão **"🚫 Não bate ponto…"**
  (ou **"✅ Voltar a bater ponto…"**). O campo saiu do "Jornada e obras" — uma porta só.
- **Em lote, na tela Pessoas**: caixa de marcar em cada linha, "marcar todas" e os dois botões na barra de cima.
  Quem já não bate aparece ligando "Incluir quem não bate ponto". Quem a planilha tira do ponto não se marca (a
  planilha manda).
- **O motivo é obrigatório** para tirar do ponto; vai para a busca de Pessoas ("marcada no próprio ponto como não
  bate ponto — <motivo> (por <quem>)"). Guardado em `colaborador_config.inativo_motivo/_por/_em` (**migração 018**).
- Só quem trata o ponto (`tratar_ponto`) marca, e cada pessoa passa pelo escopo de quem marca: o supervisor preso a
  uma obra não alcança quem é de outra (responde "não encontrada", e ninguém da lista muda).
- **Testes**: `tests/test_ponto_nao_bate_banco.py`. Ensaio no navegador: marcar duas pessoas em lote, o motivo, a
  linha da janela da pessoa e o "voltar a bater".

## 29/10/2026 (noite, quarta) — A falta parcial: vale o que está comprovado

**A tolerância** ficou como estava (pergunta do dono: *"existe tolerância para esse cálculo de horas extras?"*). Já
existia: 5 min por batida, 10 min no dia (CLT art. 58, §1º); passou disso conta tudo (Súmula 366). Ofereci olhar só a
entrada e a saída do dia (o almoço fora da soma); ele respondeu *"vamos manter assim, vamos experimentar"* — as
quatro batidas continuam na soma.

**A falta parcial** (`apuracao.pares_comprovados`). Até aqui, o dia de 3 batidas ou de 1 batida ficava "Batida
faltando", **sem débito**, esperando o ajuste; e o dia de só a manhã (ou só a tarde) ficava "Normal", com o débito
e o aviso de saída antecipada (ou atraso), sem dizer que foi falta. Propus chamar o segundo de falta parcial e
deixar o primeiro como estava; o dono: *"não, ambos os casos eu quero considerar falta parcial. Por quê? Quem
garante que ele não saiu, apenas bateu o ponto e foi embora? Então ele tem a obrigação de bater."*
- **A regra**: vale o que está COMPROVADO. Cada batida vai para a posição da escala mais perto; só conta como
  trabalhado o período (manhã, tarde) com as duas pontas batidas, e só dentro do horário da escala (a falta parcial
  não gera extra). O resto do previsto é **débito**. A tolerância de 5 min de cada batida vale (7h03 conta como 7h).
- Exemplos, na escala 7–11 / 12–17: 7h, 11h, 12h (sem a saída) → 4 h e 5 h de débito; 11h, 12h, 17h (sem a entrada)
  → 5 h e 4 h de débito; 7h, 12h, 17h (sem a saída do almoço) → 5 h e 4 h de débito; só 7h → 9 h de débito; só a
  manhã → 5 h de débito; só a tarde → 4 h de débito.
- **Não é falta parcial**: só a entrada e a saída do dia (sem o almoço) — a pessoa estava no começo e no fim; segue
  como antes (intervalo curto; na obra de intervalo pré-assinalado, o dia completo). Nem a batida a mais, sem par,
  num dia já coberto (só o aviso "batida faltando").
- **Onde aparece**: "Falta parcial" no espelho (ERP, aplicativo e a folha impressa), no Quadro, no relatório de
  **Faltas** (agora com as colunas Tipo, Batidas e Faltou, e os números "Faltas parciais" e "Horas que faltaram"),
  nos alertas e na qualidade (conta como o dia com batida faltando: −5 pontos). O ajuste aprovado desfaz: a batida
  entra e o dia é refeito.
- **O débito vai para o banco de horas** de quem tem banco, como qualquer débito.
- ⚠️ **Muda o passado**: o espelho é refeito a cada abertura, então os dias de batida faltando desde 16/10/2026
  ganham débito na hora. Antes do fechamento da folha, vale o DP olhar o relatório de Faltas e cobrar os ajustes de
  quem só esqueceu de bater.
- **Para a folha (Análise de SPs)** nada muda no formato: o dia com batida continua "Presença" (o auxílio do dia
  segue a regra de lá); as horas trabalhadas passam a ser as comprovadas.
- O nome antigo "INCOMPLETO" ainda é entendido por quem lê a situação do dia (`apuracao.SITUACOES_PARCIAIS`). A tela
  "Hoje" (quem bateu hoje) tem a sua própria "Batida faltando", do dia corrente — não mudou.
- **Testes**: `test_batida_faltando_e_falta_parcial_vale_o_comprovado` em `test_ponto_apuracao.py`.

## 29/10/2026 (noite, terceira) — Quem a planilha diz que não bate sai do ponto, e a foto que não engana

**Quem não bate ponto, pela planilha** (`registro.nao_bate_sql`). O dono: *"como ainda estamos usando a planilha de
colaboradores como base, tem algumas pessoas que têm categorização de não bater ponto: na coluna AY, quem é PJ e
Pró-labore; na coluna AA, quem está na obra ESCRUSSAS; e na coluna AX, quem está como Colaboradores Afastados. Esse
pessoal é para sair, eles não batem ponto."*
- As três colunas já vinham na cópia que a Análise de SPs guarda (`analisesps.colaborador`): AX é a **Fase Atual**
  (`fase`), AY o **Tipo de Contrato** (`tipo_contrato` — a aba oculta da planilha mostra a mesma ordem: I = Fase,
  J = Tipo de Contrato) e AA a **obra** (`obra_codigo`; o extenso, `obra_cadastro`, também é olhado, porque não deu
  para confirmar de qual das duas a AA é). **Nada mudou na área da Análise de SPs.**
- **PJ, pró-labore e ESCRUSSAS** ficam como **"não bate ponto"** — o mesmo efeito da marcação à mão: somem do
  Quadro, das Validações, dos relatórios, do ranking e do aplicativo, e a batida é recusada com o motivo. A
  comparação ignora acento, espaço e pontuação ("Pró-labore" = "PRO LABORE"; "ESC RUSSAS" = "ESCRUSSAS").
  **A planilha manda**: marcar "bate ponto" à mão não vence — a janela da pessoa diz isso. Mudou na planilha,
  muda no ponto na próxima cópia (de 2 em 2 horas).
- **Os afastados** já não batiam (14/10/2026); agora **saem também das listas** (Quadro, Validações, relatórios,
  ranking, lista de envio). **Escolha minha, para não desfazer a decisão de 14/10/2026** ("pedido ele continua
  fazendo"): o afastado **continua entrando no aplicativo**, para mandar atestado e ver o mês. Quando a fase volta
  a "ativo" na planilha, ele volta sozinho.
- **Efeito colateral, dito**: quem foi afastado no meio do mês some do Quadro também nos dias em que trabalhou
  antes do afastamento. O espelho dele continua abrindo pela busca de Pessoas (que diz "afastada: a Fase Atual
  diz…") e pela janela da pessoa.
- A busca de Pessoas explica quem não aparece por esses motivos; a Configuração › Base de pessoas ganhou o número
  "Não batem pela planilha".
- **Testes**: `test_a_planilha_tira_do_ponto_pj_pro_labore_escrussas_e_afastado` em `test_ponto_registro_banco.py`.

**A foto da batida.** O dono: *"na hora que o modal abre, ele exibe a última foto que a gente abriu (…) você acaba
achando que é uma foto, e na verdade depois ele carrega a outra (…) ficar carregando até exibir a foto correta."*
A janela da foto (Espelho, Validações, Mosaico, Fotos, janela da pessoa — é uma só) agora tira a foto anterior antes
de abrir, mostra **"Carregando a foto…"** e só mostra a foto nova quando ela terminou de chegar; se a de um clique
anterior chegar atrasada, é ignorada. Conferido no navegador com uma foto que demora 1,5 s.

## 29/10/2026 (noite, segunda) — O ajuste vira a métrica principal, o ranking no aplicativo, e o espelho de um período

**A conta da qualidade** (`core/qualidade.py`). O dono perguntou como os pontos eram gerados e disse: *"ajustes é
fundamental (…) quanto mais ajuste, pior (…) tem que ser essa a métrica principal. A gente não quer que o pessoal
faça ajuste, quer que eles batam corretamente."* Até aqui só o ajuste **acima do limite** (3 por mês) tirava ponto —
quem pedia 3 ajustes não perdia nada. Agora:
- **pontos = 70% do índice de dias em dia + 30% das fotos boas − 5 por ajuste pedido (desde o primeiro) − 5 por dia
  com batida faltando**. Os negados continuam contando. O limite configurável só decide o aviso "muitos ajustes".
- **Por que o dia com batida faltando também tira 5** (escolha minha): sem isso, deixar o buraco sem pedir ajuste
  sairia mais barato que pedir — e o buraco é pior para a folha. Ele já pesa no índice (o dia não está "em dia");
  os 5 a mais igualam ao custo do ajuste.
- **Os pontos da obra** passam a ser a **média dos pontos das pessoas**, pesada pelos dias de cada uma na obra.
  Antes a obra só olhava os dias em dia — os ajustes nem entravam, e uma obra de uma pessoa só podia ficar em
  primeiro. O mesmo para a equipe do responsável e para o número geral (somar as penalidades da equipe inteira
  zeraria qualquer equipe grande).
- **Desempate**: menos ajustes, depois menos batidas faltando, menos faltas, mais dias de trabalho.
- **Efeito que ele vai sentir**: os pontos de todo mundo caem um pouco de uma vez (quem pedia 1 a 3 ajustes perde
  5 a 15). Não é piora do ponto — é a régua nova.

**O ranking** (`core/ranking.py`). *"Premiar as melhores obras (…) que tenham pelo menos 10 funcionários (…)
dentro do app (…) tanto das obras quanto dos funcionários (…) demora para calcular (…) cache? (…) 25 primeiros."*
Quem vê: *"quem bate ponto de obra"* — fiz assim: **todo mundo que entra no aplicativo** (menu "🏆 Ranking do
ponto", no menu pessoal e no do ponto da obra).
- **Obras**: só ganham posição as com **10 funcionários ou mais** no mês; as menores aparecem abaixo, sem posição,
  com o asterisco explicando. **Funcionários**: os **25 primeiros** entre quem tem **10 dias de trabalho ou mais**
  (as faltas contam como dia de trabalho previsto). Do funcionário aparecem só o primeiro e o último nome, a função
  e a obra em que mais trabalhou — nunca o CPF. Cada pessoa vê a própria posição; quem ainda não entra vê quantos
  dias tem. A posição dos outros não sai do servidor.
- **Mês atual e mês passado**, em duas abas.
- **Guardado, não calculado na hora**: a conta passa pelo espelho de todo mundo que bateu no mês, e é pesada. Ela
  roda separada, **de 2 em 2 horas das 6h às 20h** (o mês passado, uma vez por dia — ajuste aprovado tarde ainda
  muda o mês), acordada pelas próprias requisições do ponto, como a rotina do dia e a fila do WhatsApp. O resultado
  fica em `ponto.parametros` ("ranking.AAAA-MM") — por isso **não precisou de migração**. A tela mostra a hora da
  última conta; se ainda não houver conta, ela dispara uma e diz para voltar em alguns minutos.
- Na aba **Qualidade** do ERP, a lista de obras dá posição só às que entram no ranking, com a mesma nota.

**O espelho de um período, no aplicativo.** *"Hoje podemos baixar espelho PDF e imprimir a folha do mês no app.
Preciso permitir uma seleção de um período também."* Aprovada a sugestão: ao tocar "Baixar PDF" ou "Imprimir",
abre uma janela com **"O mês"** (como antes) ou **"Um período"** (de… até…, **até 62 dias**). A folha diz
"dd/mm/aaaa a dd/mm/aaaa" no título e "Totais do período". Vale para o seu espelho e para o de uma pessoa da obra.

- **Testes**: `tests/test_ponto_ranking_banco.py` (quem entra, os 25, a posição, o prazo da conta, o aplicativo lendo
  o guardado e sem a posição dos outros) e os novos casos de `test_ponto_qualidade_banco.py` e
  `test_ponto_espelho_app_banco.py`. Ensaio no navegador: a tela do ranking e a folha de um período.
- **Não verificado com dado de verdade**: os dados de teste não têm obra com 10 pessoas nem gente com 10 dias, então
  as listas saíram vazias (com as mensagens certas). A primeira conta de verdade acontece na produção, depois de
  publicar — vale o dono abrir a tela no dia seguinte e conferir se a ordem faz sentido.
- **Ficou de fora**: divulgar o ranking fora do aplicativo (mural, WhatsApp) e prêmio — decisão dele.

## 29/10/2026 (noite) — Licenças e abonos numa tela, e a barra de filtros que abre e fecha (migração 017)

**Os tipos de ausência** (`core/tipos_ausencia.py`). O dono: *"tem as licenças e afastamentos por lei, e tem as
demais (…) mas não tem nenhum canto no sistema que a gente possa visualizar e até cadastrar novas (…) eu tenho que
sempre estar pedindo por aqui (…) e mesmo excluir alguma. E tem que poder configurar se abona ou não abona horas."*
As licenças da lei já tinham quadro, mas escondido no fim de "Jornada"; os abonos (viagem, dispensa) eram fixos
no código.
- Configuração ganhou a seção **Licenças e abonos** ("Jornada e licenças" virou "Jornada e feriados"), com dois
  quadros: as **licenças da lei** (como antes, mais a coluna "Abona as horas?" e "excluir") e os **abonos da
  empresa** (`ponto.tipos_abono`, **migração 017**, com viagem e dispensa já cadastrados): "+ Novo abono",
  ajustar, excluir. O "Lançar afastamento" e o "Abonar a falta" do Quadro passam a listar os abonos de lá.
- **Abona as horas?** "Sim": o dia não é descontado (como era). "Não": o dia aparece justificado, com o nome, mas
  as horas são descontadas — a mesma regra que o dono aprovou para a declaração de comparecimento com "nada"
  (27/10/2026). No relatório de ausências justificadas, o tipo que não abona não conta dia abonado.
- **Excluir**: o tipo nunca usado sai de vez; o já usado em pedido é **desligado** (sai das listas; os pedidos
  antigos mantêm o nome).
- Só o DP (`aprovar_afastamento`) cadastra, ajusta e exclui. Abono desconhecido ou desligado é recusado no pedido.
- **Os testes** limpam as tabelas do ponto entre um e outro, menos as de catálogo: `tipos_abono` entrou nessa
  exceção (como `tipos_licenca`), e o teste novo devolve o catálogo como estava.

**A barra de filtros que abre e fecha.** O dono: *"o filtro está muito extenso, você tem que ficar rolando o
sidebar. Dá uma olhadinha como está o filtro lá da Análise de SPs: a gente clica, expande e seleciona (…) e tem que
ser pesquisável."* Cada lista virou um bloco que abre com um clique (o mesmo estilo dos filtros do ERP, `.filtro`
do erp.css), com o número de marcados no cabeçalho — fechado, dá para ver o que está valendo — e que abre sozinho
na primeira vez se já vier algo marcado. Nas Validações: "O quê", "Confiança do ajuste" e "Motivo" (com busca);
a pessoa, as datas e a obra continuam à vista no topo. No bloco **Funcionários** (Quadro, Validações, Relatórios):
obras do cadastro, bateu nas obras, funções e escalas (todas com busca), situação e tipo de pessoa.
- **Testes**: `tests/test_ponto_tipos_ausencia_banco.py`. Ensaio no navegador: a barra (abrir, buscar, marcar, o
  número no cabeçalho, fechar) e a seção nova (cadastrar um abono que não abona).
- **Não verificado no navegador**: o "Lançar afastamento" com a lista nova de abonos (coberto pela rota no teste).

## 29/10/2026 (tarde) — A obra errada no ajuste: o aviso, e o DP corrigindo o pedido (migração 016)

O dono: *"é muito fácil a pessoa selecionar errado quando ela vai pedir o ajuste (…) colocar um alerta de
que se a obra selecionada não for uma obra que a pessoa vem batendo ponto, ou se for uma obra que
ultimamente está sem batidas (…) E, nessa situação que a pessoa errou a obra, a gente do DP poderia fazer
essa alteração. Mas baseado na portaria, a gente pode fazer isso?"*

- **O aviso** (`core/obra_suspeita.py`): ao escolher a obra no pedido de ajuste do aplicativo (o próprio, ou o
  do responsável pela pessoa), aparece um quadro âmbar quando **a pessoa bateu nos últimos 30 dias e nunca
  nessa obra**, ou quando **ninguém bate na obra há mais de 7 dias** (ou nunca bateu). É aviso, não bloqueio:
  com aviso na tela, o primeiro toque em "Enviar" vira "Confirmo a obra X — enviar". O aparelho da obra não
  tem escolha de obra (usa a dele), então lá não aparece. Os mesmos dois sinais entram na **credibilidade**
  do ajuste nas Validações ("obra em que a pessoa não bate", "obra sem batidas há dias") e no filtro "Motivo".
- **O DP corrige a obra do pedido** (`ocorrencias.trocar_obra`, rota `/erp/api/ponto/ocorrencias/<id>/obra`,
  ação `aprovar_afastamento`): na janela do pedido, "Corrigir a obra…", com a obra certa e o motivo (10 letras
  ou mais). **A resposta sobre a Portaria 671:** pode, **enquanto o pedido espera decisão** — ele ainda não é
  batida, e a portaria protege as marcações; o que se exige é que fique registrado. A troca vai para
  `ponto.ocorrencia_alteracoes` (**migração 016**): de que obra, para qual, quem, quando e por quê, e aparece
  na janela do pedido ("Obra corrigida pelo DP"). Aprovado, a batida entra na obra corrigida. **Pedido já
  aprovado não se corrige** — a batida existe, e vale "Corrigir a obra ou a hora" da batida (a original fica).
- **Testes**: `tests/test_ponto_obra_do_pedido_banco.py`. Ensaio no navegador: o aviso no aplicativo (pedido pelo
  administrativo) e a correção na janela do pedido.

## 29/10/2026 — A UF da obra pela C. Diários, e o filtro "Motivo" nas Validações (migração 015)

**A UF pela coluna AC.** O dono cadastrou um feriado estadual de PE e perguntou como era o cruzamento com a
C. Diários. Não havia: o feriado estadual e o municipal procuravam a UF e o município no **cadastro de obras do
ERP**, em branco em muitas obras — e, sem UF, o feriado não vale e o dia vira falta (no feriado quase ninguém
bate, então vale a obra do cadastro da pessoa). A decisão dele: *"enquanto a gente não está usando o ERP por
completo e a gente está usando a C. Diários, vamos utilizar como UF a coluna AC"*.
- `base_obras.py` lê a coluna **AC pela posição** (como o status, coluna V), aceita a sigla ("PE", "pe") e o nome
  por extenso ("Pernambuco", com ou sem acento); o que não for UF vira problema na lista da leitura ("UF ilegível
  na coluna AC"). A UF vai para `ponto.obras_planilha.uf` (**migração 015**).
- Como a coordenada, **o que está na planilha vence o ERP**; sem planilha, vale o ERP. Escolha minha, para ser
  coerente: o **município** da planilha (coluna "Município", que já era lida) também passou a vencer o do ERP, para
  o feriado municipal. O cadastro de obras do ERP **não é alterado** — o ponto só lê a planilha por cima dele.
- A tela de Feriados mostra quantas obras ativas cada feriado estadual ou municipal alcança (em vermelho quando
  nenhuma) e avisa as **obras ativas sem UF**, com os códigos, e o que fazer.
- Depois de publicar: apertar "Aplicar atualizações do ponto" e **ler a planilha de novo** (Base de obras ›
  "Ler a planilha agora"), senão a UF só entra na próxima leitura automática (de 2 em 2 horas).

**O filtro "Motivo"** (`core/motivos.py`). O dono: *"quando eu boto batida em conferência, a gente tem
subinformações (…) o rosto que não é localizado, que é de internet, que é porque está fora da área, que é porque
obra sem coordenada, ou então ficou na borda da obra, ou sem foto no aparelho (…) e nos ajustes (…) dentro desses
outros já tem essas outras derivações."* Na barra das Validações, um bloco "Motivo" com a contagem de cada um:
- **Batida em conferência**: rosto não confere; rosto não encontrado; foto escura; foto suspeita no mosaico; sem
  foto no aparelho; sem internet; fora da área; na borda da cerca; obra sem coordenada; sem localização; relógio
  do aparelho; fila rápida; situação no cadastro; "a pessoa explicou o motivo"; e "Outro motivo" para o texto que
  não casar com nenhum (nunca some). O motivo da batida é texto; a categoria sai do começo de cada pedaço.
- **Ajuste de batida**: os motivos de atenção de `credibilidade.py` (dia sem batida, foto, em conferência, fora da
  área, obra diferente, longe da escala, descanso/feriado, pedido tardio, muitos ajustes, negado há pouco) e os
  dois fortes a favor (pedido por outra pessoa, só uma batida faltando). `sinais()` passou a devolver `codigos`.
- Marcar vários soma (um OU outro); a contagem respeita os outros filtros (tipo, confiança, "só o que eu valido").
- **Testes**: `tests/test_ponto_motivos.py`, `test_ponto_base_obras.py` (a UF), e
  `test_a_uf_da_coluna_ac_faz_valer_o_feriado_estadual` com banco. Ensaio no navegador: o filtro e o aviso das
  obras sem UF.
- **Não verificado**: a coluna AC da planilha de verdade (aqui não há acesso a ela) — se o cabeçalho ou a posição
  forem outros, a leitura mostra a coluna lida na tela da Base de obras ("UF … da coluna AC").

## 28/10/2026 (noite, terceira) — O aplicativo não sugere o que responder

O dono, sobre o "❌ Nenhuma batida neste dia: conta como falta. **Trabalhou? Toque em ✏️.** Faltou com
motivo? Toque em 🗓️." do Meu mês: *"Não quero essa mensagem (…) psicologicamente é um estímulo à
mentira"*. Saiu a pergunta; o dia diz só o fato ("conta como falta"). Pelo mesmo motivo, no "Corrigir
vários dias", o dia de folga dizia "trabalhou? toque para marcar" e agora diz "toque para abrir". Os
botões ✏️ e 🗓️ continuam no dia, com a legenda neutra do topo ("corrigir a batida que faltou ·
justificar a ausência"). **Regra para texto novo no aplicativo:** dizer o que aconteceu e onde fica cada
botão — nunca perguntar à pessoa se ela trabalhou, nem sugerir o que ela deve pedir.

## 28/10/2026 (noite, segunda) — A negativa avisada a quem pediu (migração 014)

O dono perguntou se já existia: *"quando eu nego uma validação, o ideal era que a pessoa recebesse, quem
solicitou recebesse, a negativa, e a gente ia colocar algum motivo. Para a pessoa saber que foi negado, ou
então entender que foi pedido errado e precisa ser pedido novamente."* O que havia: o motivo já era
obrigatório e aparecia na aba Pedidos do aplicativo — só para quem fosse procurar; no Meu mês o ⏳ sumia sem
explicação; e quem pediu por outra pessoa não tinha onde ver. Ele aprovou as quatro partes propostas, e
sobre o WhatsApp: *"o padrão é para enviar, mas (…) tem uma opção de não enviar"*.

- **A. O aviso na tela inicial do aplicativo** (`core/negativas.py` › `avisos`, no `/ponto/app/api/inicio`):
  vermelho, no topo, UM por pessoa e dia ("❌ 4 pedidos negados: 21/10 — Ajustes das 07:00, 11:00, 12:00 e
  17:00 — <motivo>"). Tocado, abre o motivo e, para quem pode pedir, "Pedir de novo" (leva ao mês, ou à
  pessoa da obra); fechar a janela faz o aviso sumir para quem tocou (`/ponto/app/api/negativas/<id>/vista`).
  Aviso de mais de 30 dias não aparece mais; na migração, os negados de mais de 7 dias já entram como vistos
  (para não cair de uma vez na tela de todo mundo).
- **B. O negado no dia** (Meu mês, a folha da pessoa da obra e o espelho do aplicativo): "❌ Negado: Ajuste das
  17:00 — <motivo>", enquanto aquele horário continuar em aberto. Sai sozinho quando a batida aparece perto da
  hora (30 min) ou há um pedido novo esperando para a mesma hora.
- **C. Quem pediu** (`ocorrencias.solicitado_colaborador_id`): a própria pessoa, pelo celular ou pelo aparelho
  da obra, ou quem pediu por ela no aplicativo (o responsável, o administrativo, quem estava conectado no
  ponto da obra). Vale para os pedidos novos; nos antigos, a migração recupera só o da própria pessoa pelo
  celular. O pedido lançado pela gestão no ERP não tem "quem pediu" no aplicativo — só a pessoa é avisada.
- **D. O WhatsApp**, padrão ligado. Ao negar — um pedido, o grupo do dia nas Validações, a janela do pedido ou
  o painel — o diálogo do motivo ganhou **"Avisar pelo WhatsApp"**, com "Sim" já marcado e "Não enviar o aviso
  pelo WhatsApp". A escolha fica no pedido (`negativa_whatsapp`). A mensagem vai pela fila do ponto (tipo
  NEGATIVA, prioridade logo depois do QR pedido e do mosaico), no horário de atendimento (6h–22h), nunca
  dentro da requisição. Negar as 4 batidas do dia manda **UMA** mensagem por pessoa, com o motivo uma vez: a
  mensagem espera 1 minuto antes de sair, e cada negado do mesmo dia que chega nesse minuto entra nela. Depois
  de negar, a tela diz para quem foi o WhatsApp e quem está sem telefone no cadastro.
- **Uma decisão de leitura, que pode ser trocada:** entendi o "opção de não enviar" como não mandar o
  WhatsApp. O aviso no aplicativo aparece de qualquer jeito (o motivo já aparecia na aba Pedidos antes).
- **O tipo novo na mensageria**: `ponto.pedido_negado` ("Pedido do ponto negado") no catálogo da tela
  Mensagens, registrado também no `mensageria/HISTORICO.md`.
- **Sem a 014 aplicada**, nada disto liga (as consultas perguntam antes se a coluna existe) e a negativa
  funciona como antes.
- **Testes**: `tests/test_ponto_negativa_avisada_banco.py` (a mensagem, a junção do dia, o aviso para a pessoa e
  para quem pediu, "visto" só para quem tocou, o negado no dia, e o "não enviar"). Ensaio no navegador: negar as
  4 do dia no ERP, o aviso do administrativo e o da pessoa no aplicativo, o dia no Meu mês.
- **O que não foi verificado**: a mensagem saindo de verdade pelo WhatsApp (aqui não há Z-API; o teste vê a
  mensagem na fila). E a negativa dada pela própria tela da janela da pessoa no ERP segue o mesmo caminho, mas
  não foi ensaiada no navegador.

## 28/10/2026 (noite) — O espelho do ponto no aplicativo

O dono pediu, antes de fazer, para ver em tela: *"gerar o espelho do ponto das pessoas (…) na parte do
aplicativo, tanto do meu (…) quanto de outras pessoas também, quando permitido, né, pessoal que tem ponto de
obra (…) me diz onde é que ficaria"*. A prévia (duas imagens: onde tocar e a tela) foi aprovada com
*"ok para todas as perguntas"*, o que fechou as quatro escolhas abaixo.

- **Três portas, uma tela só** (`#tela-espelho` no `app.html`):
  1. no menu inicial, a opção **"📄 Espelho do ponto"** (no celular da pessoa e no "Meu ponto" do ponto da obra);
  2. no **Meu mês**, o botão "📄 Ver o espelho do mês";
  3. na folha de uma pessoa da obra (Consultar o ponto › a pessoa), o botão "📄 Espelho do ponto".
- **A tela**: o cabeçalho (nome, CPF só pelo final, função, obra, escala, empregador, e "mês aberto" quando
  for o caso), a troca de mês, os seis números (trabalhado, previsto, extra, débito, faltas, abonado) e os
  dias em três colunas — o dia, as batidas em pares, a situação e as horas. A falta em vermelho com
  "nenhuma batida"; a batida que a escala esperava e não veio em vermelho tracejado; ✎ na incluída por
  ajuste; ⏳ na que está em conferência ou no pedido esperando validação. Os dias antes de 16/10/2026 não
  aparecem (o ponto começa ali).
- **O PDF é a MESMA folha do DP** (`espelho_impressao.html`), aberta por duas rotas novas do aplicativo:
  `/ponto/app/espelho/imprimir` (o meu) e `/ponto/app/equipe/<id>/espelho/imprimir` (o de outra pessoa).
  "Baixar o espelho (PDF)" abre a folha já com o "Imprimir/Salvar como PDF" do celular; "Imprimir" abre a
  folha. Na folha, o **CPF inteiro** (é a que se assina); na tela, só o final — escolha do dono.
- **Quem vê o de quem**: o de outra pessoa segue o MESMO alcance da consulta que já existia — o responsável
  e o administrativo, só de quem é da obra ou bateu nela; no ponto da obra, só da pessoa consultada pelo CPF
  agora há pouco. Fora disso, "não encontrada", também na folha (uma página, não um texto técnico).
- **Detalhes de execução, escolhidos sem perguntar:** no aplicativo instalado na tela inicial, a folha abre na
  MESMA janela (no iPhone, uma janela nova perde o login) e tem "‹ Voltar"; no navegador comum, abre numa
  aba nova. No celular, a folha A4 é reduzida para caber na largura (o papel sai no tamanho normal). O
  "Algo errado? Pedir ajuste" volta ao mês, onde cada dia tem o ✏️ e o 🗓️ — e só aparece para quem pode
  pedir. O toque num dia do espelho (ver a foto), que a prévia citava, ficou de fora: o Meu mês já faz isso.
- **Testes**: `tests/test_ponto_espelho_app_banco.py` (o CPF pelo final na tela e inteiro na folha; o
  alcance; a folha fora do alcance). Ensaio no navegador em tamanho de celular, com as três portas, a troca
  de mês, a folha e a volta.
- **O que não foi verificado**: a impressão de verdade num celular (o "Salvar como PDF" é do próprio
  aparelho); no iPhone instalado na tela inicial, o "Imprimir" depende da versão do sistema.

## 28/10/2026 (tarde) — Pessoas voltou, a foto de cadastro na janela, e o pedido que não dizia a obra

- **O quanto cada ajuste é crível** (`core/credibilidade.py`; o dono: *"a quantidade de solicitações de
  ajuste ainda é muito alta (…) o sistema sugerir entre ajustes mais críveis e outros que merecem mais
  atenção (…) um ponto pedido pelo administrativo da obra é mais confiável; uma única batida faltando de
  quem tem as outras na obra também; problemas com foto, de localização pedem atenção (…) selecionar um
  bloco grande e validar"*). Cada pedido de ajuste ganha um nível e os motivos, escritos:
  - **Crível ✅**: nada pede atenção e há um sinal forte — pedido por outra pessoa (o responsável, o
    administrativo, a gestão) ou só aquela batida faltando num dia em que as outras foram batidas —, e a
    hora casa com a escala (até 30 min).
  - **Pede atenção ⚠️**: dia sem batida nenhuma; foto do dia que não conferiu; batida do dia em
    conferência; tentativa de bater fora da área da obra no dia; obra pedida diferente da das batidas do
    dia; hora a mais de 1h30 da escala; descanso ou feriado; pedido mais de 7 dias depois; 6 dias ou mais
    com ajuste em 30 dias; pedido negado nos últimos 30 dias. As batidas em conferência entram aqui.
  - **Normal ⚪**: o resto (por exemplo, duas batidas faltando, pedidas pela própria pessoa).
  - Na barra das Validações, "Confiança do ajuste" filtra por nível, e **"Marcar todos os críveis da
    lista"** marca o bloco para aprovar de uma vez. O grupo do mesmo dia mostra o pior nível dele.
  - É **sugestão**: só filtra e marca; quem aprova continua sendo uma pessoa, com os motivos à vista. Os
    limites (30 min, 1h30, 7 dias, 6 dias) estão no começo do arquivo, para afinar com o uso.
- **Todas as telas voltaram ao menu** (o dono: *"as telas que foram ocultadas precisam voltar"*): Quadro,
  Pessoas, **Espelho** (a tela de antes, com a tabela nova — a mesma da janela da pessoa: a batida que
  falta em vermelho, cada batida abre a janela de decidir e corrigir), Validações, Hoje, Mosaico, Fotos de
  cadastro, Alertas, Qualidade, Rondas, Banco de horas, Relatórios e Configuração. Com 13, a faixa do
  menu não cabe inteira: o "Telas ▾" do ERP mostra a lista.
- **Validações: o mesmo dia da mesma pessoa numa linha só** (o dono: *"alguém pediu as 4 batidas do dia, e
  aparecem 4 validações. O ideal era aparecer consolidado, mas também ser possível aprovar separadamente ou
  recusar um turno"*): os ajustes de batida (e as batidas em conferência) da mesma pessoa no mesmo dia
  viram um grupo — "Ajuste de batida × 4", as horas em ordem, "Aprovar as 4", "Negar as 4…" (um motivo
  só, gravado em cada uma) e "ver uma a uma", que abre cada pedido com os seus botões. A marcação do lote
  marca o grupo inteiro.
- **O alto das Validações fica preso ao rolar** (o dono: *"o botão Validações deveria ficar fixo no topo"*):
  o título e o "aprovar os selecionados". O menu do ERP já ficava fixo; o que sumia era isso.
- **Mosaico: trocar a foto de cadastro ali mesmo** (sugestão do dono): "usar no cadastro" embaixo de cada
  foto de batida (antes só aparecia depois de abrir a foto) e "trocar…" na foto de cadastro, que abre a
  janela da pessoa para enviar uma foto nova.
- **Pessoas voltou ao menu** (o dono: *"precisa voltar a tela de pessoas. Preciso ainda ver a pessoa, ver a
  foto de cadastro e poder alterar a foto de cadastro"*): a lista com a **foto de cadastro** de cada um, o
  filtro de funcionários na barra (o mesmo do Quadro; "bateu nas obras" olha 30 dias), "com foto / sem foto"
  e "incluir quem não bate ponto". Clicar abre a janela da pessoa na aba Cadastro.
- **A foto de cadastro na janela** (aba Cadastro): a foto grande, e **enviar uma foto nova** (novo:
  `/pessoas/<id>/foto-cadastral/enviar`; no celular, abre a câmera), **escolher entre as fotos das batidas**
  ou **tirar** (a próxima foto boa vira a nova). A enviada vai para o Drive como as fotos das batidas.
- **O código de acesso do aplicativo** (o dono: *"em qual tela eu gero o código pro celular de alguém?"*):
  o botão "Código não chegou?", que ficava na tela Pessoas, virou "🔑 Código de acesso do aplicativo (PIN)",
  também no alto da janela. Os avisos de escala, jornada, acesso e banco apontavam para a tela que tinha
  saído e não apareciam; agora aparecem na janela.
- **O pedido de ajuste mostra a obra escolhida** (o dono: *"não aparece a obra que foi selecionada"*), e a do
  cadastro quando for outra. O "quem pediu" não repete mais o nome: o aparelho que ninguém renomeou guarda
  "Pedido por Fulano (CPF final 123)", e a frase virava *"pedido por no aparelho Pedido por JORDAO (…) —
  lançado por JORDAO"*; agora é "JORDAO, no próprio aparelho" (ou "no aparelho de Fulano").

## 28/10/2026 — Filtros na barra, o espelho na janela da pessoa, corrigir UMA batida e o "sem rosto"

**Os pedidos do dono** (vários, no mesmo dia): *"os filtros das telas, de maneira geral, devem estar no
sidebar. Para obras é mandatório poder selecionar mais de uma, mesma coisa para funções"*; *"a lista de
exceções ao padrão, a apresentação tá terrível"*; *"a cerca das obras não tem filtragem nenhuma"*; *"tem o
nome da pessoa, tem que ter a função também"*; *"o nome de uma pessoa aqui, eu clico, não faz nada"*;
*"você reduziu as telas, mas talvez só escondeu (…) existe e não existe"*; *"o espelho é algo que a gente
precisa com alguma frequência ver"*; *"não aparece a falta no espelho"*; *"fotos com rosto dizendo que
não foi detectado um rosto"*; *"validar a conferência a partir do espelho também"*; *"o cara bateu obra
tal, mas é para ser obra tal — eu quero editar somente aquele ponto"*.

**O filtro de funcionários com várias obras.** Quadro, Validações e Relatórios: "Obras do cadastro",
"Bateu nas obras", "Funções" e "Escalas" viraram listas de marcar, cada uma com a sua busca (nada marcado =
todas). O servidor aceita uma obra, várias (lista) ou "1,2,3" (`filtro_pessoas.ids`).

**A Configuração com barra lateral.** Cada seção com lista tem o seu filtro na barra (`FiltroDaLista`): os
grupos se montam com o que existe na lista, com quantos há de cada.
- Pessoas › Exceções: exceção, obras, funções.
- Escalas › Quem está em cada escala: escala de hoje, obras, funções e jornada. Os filtros de cima do
  cartão saíram.
- Obras e cerca: busca de obra, com ou sem coordenada, o que fazer fora da cerca, batidas por dia,
  barradas na semana.
- Aparelhos: situação, tipo, obras e função do responsável. Sem situação marcada, os arquivados ficam
  de fora, como antes.
- Fotos › Mosaico por obra: obrigatório ou não, responsável.
- As seções sem lista (Geral, Jornada, Quem valida, Mensagens, Teste) escondem a barra.

**A apresentação.**
- **Exceções:** uma linha por pessoa, com função, obra e o que foge do padrão (`forma_de_bater.excecoes`
  ganhou `pessoas`). Os contadores no alto marcam o filtro.
- **Base de obras:** um quadro só, "O que corrigir na planilha C. Diários" (a coordenada ilegível e os
  outros avisos, linha a linha). "Sem coordenada" leva à Cerca das obras já filtrada; os status lidos e as
  obras criadas ficam em "Detalhes da última leitura".
- **O nome abre a pessoa** em Exceções, Escalas, Aparelhos (o responsável, agora com a função), na lista
  dos níveis da conferência e no Mosaico.

**O espelho na janela da pessoa; as telas Espelho e Pessoas saíram de vez.**
- A aba "Mês" da janela virou **"Espelho do mês"**: os números do mês e a tabela dia a dia.
- **Cada batida abre a janela da batida**: foto, mapa, validar a conferência, corrigir, desconsiderar.
- O dia que **falta batida** mostra, em vermelho, qual faltou ("Saída 17:00"); o dia sem nenhuma, "nenhuma
  batida — esperava 4". **Era isso a "falta que não aparecia no espelho":** a falta do dia inteiro sempre
  esteve lá; a batida que faltava num dia com batidas, não — só "batida faltando".
- Os dias antes do início do ponto viram uma linha só.
- O Cadastro da janela ganhou o QR Code e o histórico de escalas, o que só a tela Pessoas tinha.
- `/erp/ponto/espelho` e `/erp/ponto/pessoas` abrem o Quadro com a janela da pessoa (`?abrir_pessoa=`).
  A impressão do espelho continua no mesmo endereço.
- Na aba Dia, toda batida tem "Abrir a batida" ("Decidir a conferência…" se estiver em conferência).

**Corrigir UMA batida** (`marcacoes.corrigir`, rota `/marcacoes/<id>/corrigir`, só quem aprova
afastamento — o DP).
- Na janela da batida, "Corrigir a obra ou a hora…": obra certa, hora certa, motivo (10 letras ou mais).
- A original **não muda**: a obra e a hora estão no hash encadeado (Portaria 671). Ela deixa de contar,
  com o motivo e quem corrigiu em `marcacao_decisoes` ("PG-A às 07:00 → PG-B às 07:05").
- No mesmo instante entra a corrigida, AJUSTADA, com NSR próprio e `marcacao_origem_id` apontando para a
  original — o caminho que a migração 001 já previa. A obra nova também tem de estar no alcance de quem
  corrige.
- O "ajustar o dia" (incluir a batida que faltou) continua como estava.

**O "sem rosto" com o rosto à vista — a causa provável.**
- Quem não tinha foto de cadastro ganhava a primeira batida "boa" como cadastral, e "boa" era só a luz
  e o contraste, não o rosto.
- A AWS recusa a comparação quando a foto de **cadastro** não tem rosto. O sistema lia isso como "a
  batida não tem rosto": se a cadastral veio sem rosto, **todas** as batidas da pessoa davam "sem rosto".
- **Agora:** nessa recusa, o sistema pergunta à AWS se a cadastral tem rosto (`detect_faces`). Se não
  tem, grava SEM_CADASTRAL "a foto de CADASTRO não tem um rosto que a AWS reconheça" e não põe a batida
  em conferência.
- A batida que vem mesmo sem rosto ganha uma **segunda tentativa**, com a foto clareada e ampliada (rosto
  pequeno ou escuro).
- Fotos de cadastro › "Provavelmente errada" passou a pegar também a cadastral sem rosto e o "sem rosto"
  em série (3 das últimas 5 conferências). Um "sem rosto" solto continua sendo tratado como foto escura.
- **Falso alarme:** a batida "sem rosto" que o DP validou não conta mais como falha. Antes, só a "não
  confere" validada deixava de contar, e a pessoa ficava presa "em observação" por fotos com o rosto à
  vista.
- **Custo:** cada pergunta extra à AWS custa o mesmo que uma conferência, só nos casos de falha, e não
  entra no gasto do mês mostrado. Não testado com a AWS de verdade (não há chave aqui).
- **As batidas "sem rosto" de antes continuam como estão.** Valide as que têm rosto (botão "Decidir")
  e troque a foto de cadastro de quem aparece em "Provavelmente errada".

**O porquê do nível da conferência** (`confianca.explicar`, rota `/pessoas/<id>/rosto`).
- A janela da pessoa ganhou a aba "Conferência do rosto".
- O nível vem numa frase: por que está nele (a conferência de tal dia deu tal resultado; ou "todo mundo
  começa assim"; ou reincidente, com quantas falhas), e quanto falta para subir.
- Embaixo, as últimas 15 conferências: as que contaram como falha e os falsos alarmes, cada uma com a
  foto e o botão para decidir a batida.
- Em Configuração › Fotos › "Ver quem está em cada nível", o nome abre essa aba.

**Testado:** `tests/test_ponto_ajustes_do_dono_2810_banco.py` (corrigir, endereço antigo, falso alarme
e porquê, exceções e várias obras) e `tests/test_ponto_rosto_cadastral.py` (cadastral sem rosto, segunda
tentativa, suspeita, a frase). No navegador: as seções da Configuração com a barra (marcar duas obras,
buscar obra), o espelho na janela vindo da Configuração e do endereço antigo, e corrigir uma batida de
AP-01 para BR-02.

## 27/10/2026 (noite) — O ponto da obra: busca pelo nome, 5 minutos e o som

**As respostas do dono** à análise da tarde: *"busca pelo nome, beleza. Com uma observação, se um
funcionário novo surgir, ele não vai estar naquela obra (…) Em relação ao tempo, ok, volta para a tela de
bater e coloca (…) 5 minutos. Quanto ao som, ok. (…) Vai ter Android, vai ter iPhone. Não quero QR Code
impresso."*

**A busca pelo nome** (`core/busca_nome.py`, rota `/ponto/app/api/obra/buscar-nome`).
- Para quem esqueceu o CPF e não está com o QR. Botão "🔎 Não sei o CPF — procurar pelo nome" na
  **tela de bater** (só aparece com alguém conectado) e no **Consultar o ponto de uma pessoa**.
- 3 letras ou mais; cada palavra digitada é começo de uma palavra do nome ("jos sous"). Até 6
  pessoas, com a **foto pequena** (a miniatura de cadastro), **primeiro e último nome**, função e obra.
  **Nunca o CPF.** Cada busca fica no log (aparelho e quem estava conectado).
- **Quem aparece:** quem é da obra para o aparelho (o mesmo alcance da consulta) — e, pela observação
  do dono, **quem foi admitido há até 30 dias e quem está sem obra no cadastro**. Se não achar, "Não
  achou? Procurar em todas as obras" (só na batida; na consulta, a folha continua só de quem é da obra).
- **Segurança:** a busca devolve, para cada pessoa, uma **escolha assinada** (vale 3 minutos, só neste
  aparelho) — o número da pessoa sozinho não identifica ninguém. Bater pela escolha exige o responsável
  (ou o administrativo) **conectado** no aparelho.
- **A batida** fica gravada como CPF (a lista de identificações da migração 003 não tem outra; sem
  migração) e marcada em `registrado_por`: "identificada pelo nome no aparelho da obra — <quem estava
  conectado>". Ela **sempre** passa pela conferência do rosto, fora da regra e do sorteio (`rosto.py`).
- **Decisão revista:** a regra de 10/10/2026 "a consulta é pelo CPF — nunca uma lista de nomes" deu lugar
  a esta busca contida, com o aceite do dono. A lista inteira da obra continua não existindo no ponto da
  obra.
- Sem internet, a busca não funciona (diz "digite o CPF").

**Os 5 minutos.** Fora da batida, 5 minutos parado (eram 3) e o aparelho **volta para a tela de
bater, conectado** — antes ele desconectava e caía na entrada, e a fila parava. O "Sair" continua
desconectando.
- **Um erro meu na análise, corrigido aqui:** escrevi ao dono que "sair da batida já pede o PIN" —
  não pedia desde 10/10/2026 (ele tinha tirado). Sem nada, quem passasse pela fila tocaria "‹ Início"
  e entraria no menu como o responsável. Por isso: **depois da volta sozinha**, sair da batida pede o
  **PIN de quem estava conectado** (ou "Não sou eu — entrar com outro CPF", que desconecta). Quando o
  próprio responsável abre a batida e sai dela, continua sem PIN, como ele pediu em 10/10.

**O som e a vibração.** Registrado: um bip curto; foi para conferência: dois bips; não registrou: um
tom grave e longo (também sem internet: guardada ou não guardada). A vibração acompanha — **só no
Android**: no iPhone, site não vibra (regra da Apple), e no iPhone com a chave do silencioso o som
também não toca. O 🔊 no alto da batida desliga o som (fica lembrado no aparelho); a vibração fica. O
navegador só deixa tocar depois do primeiro toque na tela: um aparelho que reinicia direto na batida
e só lê QR fica mudo até alguém tocar nele uma vez.

**Testado:** `tests/test_ponto_busca_nome_banco.py` (as partes puras, a escolha, a busca com e sem
login, o novato e o sem obra, a batida marcada, a consulta pela escolha). No navegador: a busca, a
batida, o som, a consulta pelo nome, a volta em 5 minutos (com o relógio adiantado) e o PIN para sair.
Não testado num celular de verdade (o som e a vibração dependem do aparelho).

## 27/10/2026 (tarde) — A declaração de comparecimento, e o que o ponto da obra pediu

**A declaração** (o dono: *"o cara vai no hospital, faz uma consulta rápida, não é dado atestado
médico, entrega a declaração e falta o resto do dia todinho (…) o DP precisa analisar e decidir se
vai abonar a totalidade das horas, só o período, ou não vai abonar nada"*).
- **Tipo novo** "Declaração de comparecimento (consulta, exame)" em todo lugar onde se entrega
  atestado: no celular, no ponto da obra, no pedido pela equipe e no ERP (entregar e lançar). Leva o
  **dia e o horário escritos nela** (das/às) e o documento.
- Guardada como ATESTADO com `subtipo` (sem migração): DECLARACAO enquanto espera; na **aprovação
  final**, quem aprova é perguntado — **só o período** (o padrão), **o dia todo** ou **nada** — e o
  subtipo guarda a escolha (DECL_PERIODO, DECL_TOTAL, DECL_NADA). Sem a escolha, o servidor não aprova.
  Nas etapas de antes (encarregado), não se pergunta. Aprovar em lote não serve para declaração: ela
  pede a escolha, uma a uma.
- **No espelho:** "o dia todo" abona como atestado; "só o período" tira os minutos declarados do débito
  do dia (se cobrir o dia previsto inteiro, abona); "nada" deixa o dia como estava. O dia mostra a
  declaração (Quadro, observação) e os Relatórios › Ausências justificadas dizem as **horas abonadas**,
  não dias.
- Sigilo: é documento de saúde, como o atestado (só o DP abre).

**O que o ponto da obra pediu** (busca pelo nome com foto, som/vibração, os 3 minutos): **análise
mandada ao dono, nada feito**, a pedido dele ("me questione antes de executar"). Resumo do que propus:
- Busca pelo NOME (3 letras, até 6 pessoas da obra), com foto pequena e **sem CPF** — mostrar o CPF é
  PIOR que a foto pela LGPD (é o dado que serve a golpe); a foto, usada também para a conferência do
  rosto, é dado sensível: mostrar só para identificar, contido, com registro de uso e a batida marcada
  "identificada pelo nome" (sempre com a conferência do rosto). Botão "Não sei meu CPF" na tela de
  bater. Validar com o jurídico. Alternativa: crachá com o QR impresso.
- Som (funciona em Android e iPhone, com o volume ligado) e vibração (só Android — a Apple não deixa
  site vibrar): sons diferentes para registrado, conferência e recusa; botão para desligar o som.
- Os 3 minutos: a batida já não sai sozinha; o que trava a fila é sair dela e, em 3 min parado,
  desconectar e ter de entrar com CPF e PIN de novo. Proposta: em vez de desconectar, **voltar para a
  tela de bater e continuar conectado** (sair dela já pede o PIN). Mais tempo (10–15 min, ou o turno)
  deixa o aparelho, fora da batida, aberto para qualquer um da fila consultar o ponto dos colegas e lançar
  pedidos no nome do responsável.

## 27/10/2026 — Menos telas, e um filtro de funcionários só, na barra lateral

**Pedido do dono** (depois de eu propor, sem executar, juntar as telas no Quadro): *"Vamos manter a de
validações, todavia o que eu venho dizendo, precisa da filtragem mais ampla no sidebar. Precisa ser
facilitado o filtro dos funcionários de uma maneira geral. Esses filtros na parte superior não ficam
legal."* — e concordou com ficar só Quadro, Validações, Relatórios e Configuração no menu.

**O que mudou:**
- **O menu do ponto tem 4 telas:** Quadro, Validações, Relatórios, Configuração. As outras (Hoje,
  Pessoas, Espelho, Mosaico, Fotos de cadastro, Banco de horas, Alertas, Qualidade, Rondas) **continuam
  abrindo pelo endereço** — os links das mensagens de WhatsApp não quebram — e estão listadas na barra do
  Quadro ("As outras telas"); a janela da pessoa abre o espelho e o banco dela.
- **O filtro de funcionários é um bloco só** (`core/filtro_pessoas.py` e o `FiltroPessoas` da tela), igual
  no Quadro, nas Validações e nos Relatórios: pessoa, obra do cadastro, obra em que bateu no período,
  **funções — várias de uma vez, com busca e quantas pessoas há em cada**, escala (a padrão marcada),
  situação no cadastro e tipo de pessoa (vigia, ronda, celular, ponto da obra, equipe…, com a contagem).
  As opções vêm de um endereço só (`/erp/api/ponto/filtros/pessoas`).
- **Validações com os filtros na barra lateral** (saíram do topo): "só o que eu valido", pessoa ou quem
  pediu, período, obra do pedido, os tipos e o bloco de funcionários. Com um filtro de funcionário
  escolhido, ficam só os itens DESSAS pessoas (o que não é de uma pessoa, como o mosaico da obra, sai).
- **No Quadro, as telas que saíram do menu viraram partes dele**, com o mesmo filtro e o mesmo período:
  - **📷 Fotos** — o mosaico: a foto de cadastro e as fotos das batidas, por pessoa e dia (até 150 linhas;
    a conferência obrigatória do mosaico de uma obra continua na tela do Mosaico, que a Validações abre);
  - **⭐ Qualidade** — por obra, por responsável e por pessoa, no período do Quadro (a mesma conta da tela
    Qualidade, que continua para ver o mês e os critérios);
  - **🔦 Rondas** — as noites dos vigias do recorte (até 14 noites), com cada ronda e a foto;
  - **Alertas** — o alerta aberto aparece na linha do dia ("⚠ Batida faltando"), clicável para resolver ou
    dispensar, e a situação nova **"Com alerta aberto"** filtra.

**Decisões:** a escala, no Quadro, continua sendo a de CADA DIA; nas Validações e nos Relatórios, é a que
vale no fim do período (a própria ou a padrão da empresa). As visões Qualidade e Rondas só calculam quando
abertas, e a Qualidade respeita "uma conta pesada por vez" (21/10).

**Conferido:** testes novos (o menu, o filtro igual nas telas, o alerta no Quadro, qualidade e rondas do
recorte) e os do ponto inteiros; no navegador, com dado de ensaio: o menu com 4 telas, duas funções
marcadas no Quadro, as visões Fotos, Qualidade e Rondas, o alerta aberto e tratado pela janela, a barra das
Validações e o filtro de função nos Relatórios.

## 26/10/2026 — As anotações do dono: escalas, falta, feriado, abonos, documento, IA, fotos e relatórios

O dono mandou uma lista de anotações, operando o sistema. Feitas todas, nesta ordem (a escala
primeiro porque é dela que depende a falta aparecer):

1. **Escalas** (*"como é que eu faço para aplicar a escala padrão para todo mundo? (…) selecionar
   os vigias todos e aplicar a escala para eles (…) cada funcionário novo já entrasse na escala de
   produção"*). A escala padrão existia desde 06/10, escondida em Configuração › Jornada. Agora há
   a seção **Configuração › Escalas**: a escala padrão NO TOPO (quem não tem escala própria e todo
   funcionário novo seguem ela, desde o primeiro dia, inclusive nos dias que já passaram), quantos
   estão em cada escala, e a lista com filtros (função, obra, jornada, escala de hoje) para marcar e
   **aplicar a escala de uma vez** (rota `/erp/api/ponto/escalas/aplicar`, `configurar_ponto`, numa
   transação). **Decisão:** "a partir de" substitui as trocas de escala marcadas DEPOIS dessa data —
   senão aplicar desde o início do ponto ficaria escondido atrás da troca seguinte. A data sugerida
   é o início do ponto (16/10), o certo na primeira vez.
2. **Falta no dia seguinte** (*"finalizou o dia, não teve ponto, dia seguinte já apresenta falta (…)
   não impedir os botões"*). Isso já acontece para quem tem escala — o que faltava era a escala (item
   1). O dia de falta ficou em destaque (fundo e barra vermelhos) no Meu mês do aplicativo, com a frase
   "conta como falta — trabalhou? ✏️; faltou com motivo? 🗓️", e no espelho do ERP. Os botões continuam.
3. **Feriado em várias obras** (*"a seleção de obras não está funcionando"*). **Causa:** a obra era uma
   lista de uma escolha só e só valia com "Vale para" em "Só de uma obra" — escolher a obra e esquecer o
   "Vale para" gravava um feriado NACIONAL. Agora "Vale para" decide o que aparece (todas as obras, algumas
   obras, um estado, um município) e as obras são caixas de marcar, com busca e "marcar todas". Um feriado
   de obra para cada uma (`feriados.gravar_para_obras`); a que já tinha o feriado no dia fica como está.
4. **Viagem e Dispensa** (*"a pessoa está em viagem pela empresa e aquelas horas vão ser abonadas (…)
   dispensa da mesma forma"*). São ABONO com nome próprio (`subtipo` VIAGEM / DISPENSA — sem migração): o
   dia não é descontado nem vira débito no banco, e aparece como "Viagem a serviço" / "Dispensa pela
   empresa" no espelho, no Quadro e na folha. Em "Lançar afastamento" e no "Abonar a falta" em lote do
   Quadro. **Fica para o dono confirmar:** abono não gera hora extra nem conta como hora trabalhada para
   extra — é o dia "pago sem trabalhar".
5. **Documento numa janela** (*"os atestados abrem, eles baixam"*). O atestado/documento abre numa janela
   (foto ou PDF dentro do ERP, com "abrir em outra aba"). O servidor passou a dizer o tipo do arquivo pelo
   próprio conteúdo (o gravado podia ser genérico, e o navegador baixava) e a mandar o nome sem acento no
   cabeçalho, com a versão completa ao lado.
6. **Confronto da IA** (*"cruzar o que foi lido pela IA com o que foi lançado — as datas, a quantidade de
   dias, o nome da pessoa"*). `leitura_atestado.confronto`: nome da pessoa (o do documento contido no do
   cadastro, primeiro nome igual), primeiro dia, dias, último dia, CID, CRM e médico — cada um BATE, NÃO
   BATE, SÓ NO DOCUMENTO ou NÃO LIDO —, e avisos: pouco legível, declaração de comparecimento (abona só o
   horário), emitido muitos dias depois (data para trás). Aparece em Validações, logo abaixo da leitura.
7. **Fotos de cadastro** (*"a primeira foto foi uma foto de teste (…) de outra pessoa, ou do chão (…) como
   regularizar de forma fácil?"*). A foto de cadastro nasce sozinha da primeira foto boa; se for errada, as
   seguintes "não conferem". Tela nova **Mosaico › 📷 Fotos de cadastro** (`/erp/ponto/fotos-cadastro`): a foto
   de cadastro ao lado das últimas 6 fotos das batidas, cada uma com o resultado da conferência; o filtro
   "provavelmente errada" (das últimas 5 conferidas, ao menos 2 e a maioria deram "outra pessoa"),
   "escolhida sozinha", "sem foto". **Usar como cadastro** num clique, ou **tirar a foto** (a próxima foto
   boa vira a nova). As conferências antigas continuam registradas. A foto de cadastro deixou de ficar
   guardada 1 h no navegador (trocada, a tela mostrava a antiga).
8. **Relatórios** (*"a gente não tem nenhum relatório nesse sistema novo"*). Aba nova **Relatórios**:
   **faltas** (por pessoa e dia a dia), **ausências justificadas** (atestados, licenças da lei, férias,
   afastamentos, abonos, folga do banco, compensação — lista e por tipo, CID só para o DP, documento
   clicável), **horas extras** (em dia de trabalho, em descanso, em feriado, noturno; por pessoa e dia a dia)
   e **banco de horas** (saldo e primeiro vencimento). Imprimir e baixar planilha (CSV que o Excel abre).
   **As contas são as do espelho** (`core/relatorios.py` usa o mesmo Lote do Quadro): bate com o espelho de
   cada um. Entra também quem foi desligado no meio do período, se tem batida ou pedido aprovado. Até 62
   dias; um relatório por vez. **Fica para depois, combinado:** o arquivo .ps da contabilidade.

**Em espera, a pedido do dono:** o layout do mês completo resumido.

**Conferido:** testes novos (escalas em lote, feriado em várias obras, viagem no lote, tipo do documento,
confronto, fotos de cadastro, relatórios batendo com o espelho) e os do ponto inteiros; no navegador, com
dado de ensaio: a seção Escalas (padrão no topo, filtrar, aplicar), o feriado em duas obras, os quatro
relatórios e a planilha, as fotos de cadastro com a "provavelmente errada", o confronto e o PDF na janela.

## 22/10/2026 (tarde) — O Quadro em uso: seleção, a janela da batida, a batida errada e a escala

O dono abriu o Quadro em produção, no dia, e mandou seis observações de uma vez:

1. *"Várias pessoas que eu poderia muito bem selecionar para fazer alguma ação, mas a única
   coisa que tem é 'marcar todas com batida faltando' (…) a seleção individual não está
   aparecendo."* **Era defeito de desenho:** a caixa só aparecia no dia com batida E com batida
   faltando PELA ESCALA — e, como na produção quase ninguém tem escala (item 6), quase nenhuma
   linha tinha caixa. Agora toda linha que se pode tratar tem caixa (menos dia futuro, fora do
   contrato e antes do ponto), há "marcar: todas · com batida faltando · sem nenhuma batida ·
   faltas · nenhuma", e a barra mostra QUANTAS das marcadas cada ação alcança — a que não cabe
   fica de fora com o porquê:
   - **Incluir uma batida** (dia com alguma batida). Com a hora digitada, vale também para quem
     não tem a batida prevista na escala (sem escala, horário fora do previsto);
   - **Lançar o dia inteiro pela escala** (dia sem nenhuma batida): todas as batidas da escala, num
     pedido aprovado na hora, com o motivo — para quem trabalhou e o ponto falhou. Ou entram todas
     as do dia, ou nenhuma. **O dia passa a contar como trabalhado na folha** — está escrito na janela;
   - **Abonar a falta** (só o DP; rota própria com `aprovar_afastamento`): um ABONO por pessoa e
     dia, com o motivo — chuva, obra parada, equipe liberada. Só no dia que o espelho chama de falta;
   - **Definir a escala** (pessoas marcadas, a partir de uma data; 12x36 pede o dia-base, o mesmo
     para todas).
2. *"Esse pedido de revisão, que a gente já consiga visualizar que aquele horário tem um pedido."*
   O pedido de ajuste esperando decisão aparece NO horário (⏳ 17:00, tracejado amarelo); os
   pedidos da observação também ficaram clicáveis. Clicar abre o pedido — quem pediu e por onde,
   quando, o motivo, cada etapa e quem a aprovou — com **Aprovar** e **Negar** ali, quando a etapa
   é de quem olha (GET novo `/erp/api/ponto/ocorrencias/<id>`, `ver_ponto`).
3. *"Eu clico nessa batida e não tem informação: quem aprovou, como foi, quando pediu, quem pediu —
   está muito pobre (…) gostaria de poder aprovar a partir dessa tela."* A janela do mapa virou a
   **janela da batida**: situação, como foi feita (celular da pessoa, ponto da obra com o nome do
   aparelho e o responsável, ou ajuste), registrada por quem, sem internet (e quando chegou), foto,
   NSR, o PEDIDO que criou a batida ajustada (com quem pediu e quem aprovou), as decisões já tomadas
   e, em conferência, **Validar** e **Rejeitar** ali; o mapa ficou embaixo. A **legenda** das batidas
   foi para o alto da tabela — o "celularzinho" (📵) é a batida feita no ponto da obra sem internet,
   enviada quando o sinal voltou.
4. *"O cara bateu 5 horas da manhã e depois 7 (…) faltou a última batida (…) isso aqui é errado."*
   Duas partes. **Ver:** a batida que não casa com nenhum horário da escala (o mesmo casamento que
   diz o que falta, até 2 h) ganha contorno pontilhado laranja e ⚠, e o dia entra no filtro novo
   "Com batida fora do horário da escala". **Tratar:** **"Desconsiderar esta batida"**, na janela
   da batida, só para o DP (`aprovar_afastamento`), com motivo. **Nada se apaga (Portaria 671):** a
   marcação continua no registro com NSR, hora e hash; passa a REJEITADA (deixa de contar no espelho)
   e a decisão vai para `marcacao_decisoes`, com de que situação saiu, quem e por quê. Depois, a
   batida que faltou entra pelo ajuste ou pelo lote. Sem migração: o status REJEITADA já existia.
   **Decisão minha, para o dono confirmar:** só o DP desconsidera — o encarregado continua só
   validando ou rejeitando o que está em conferência.
5. *"Vigia, celular liberado, sem ronda (…) em Aparelhos só dá a opção de bloquear. Se eu quiser
   editar as configurações dele, queria fazer a partir daqui."* A aba Aparelhos da janela da pessoa
   abre o MESMO editor de Configuração › Aparelhos (tipo, responsável, obras, validade, pedidos e a
   ronda do vigia): **Alterar** no aprovado, **Aprovar** no pendente, **Reativar** no bloqueado. Para
   o vigia sem ronda, um aviso com **"Ligar a ronda…"**, que abre o acesso no aplicativo da pessoa.
6. *"O pessoal está tudo sem escala. A gente tinha definido uma escala padrão, a Produção. Onde eu
   vinculo?"* **A escala padrão da empresa existe desde 06/10 (Configuração › Jornada e licenças ›
   "Escala padrão da empresa"), mas na produção ninguém a escolheu** — e sem ela, quem não tem
   escala própria não é julgado (nem falta, nem batida faltando). Escolhida, vale na hora para
   todos os que não têm escala própria, inclusive nos dias que já passaram. O Quadro agora avisa
   quando há gente sem escala, diz se a padrão está escolhida, leva à Configuração e tem "marcar as
   sem escala" + "Definir a escala" para dar escala própria a alguns (o vigia no 12x36).
7. (mensagem seguinte) *"O botão Montar quadro está muito no final; toda vida tem que rolar."* Foi
   para o topo da barra e fica preso lá ao rolar.

**Defeito achado no ensaio e corrigido antes de sair:** o botão novo "Definir a escala" nasceu com
o mesmo nome interno do filtro de escala da barra (`qd-escala`) — a barra perdia as opções do
filtro. Renomeado (`qd-definir-escala`); sem nomes repetidos novos na página.

**Conferido:** testes novos (lote do dia inteiro e do abono; a batida errada; o pedido no horário
decidido pela janela; a batida ajustada dizendo de que pedido veio; o casamento puro) e os do ponto
inteiros; no navegador, com dado de ensaio: caixas em todas as linhas, os quatro botões com as
contagens, a legenda, o aviso de sem escala, a janela da batida, o desconsiderar, o pedido aprovado
pela janela, a escala em lote, o botão preso no topo e a aba Aparelhos com o editor.

## 22/10/2026 — O mapa do quadro: onde tem mais gente batendo ponto

**Pedido do dono:** *"Quero um mapa que me permita visualizar geograficamente a qtd de
funcionários batendo ponto num determinado período ou dia. Pra entender visualmente onde
tem mais gente. Se tiver alguma ideia pra agregar…"*

**Onde ficou:** uma visão a mais do Quadro (🗺️ Mapa), ao lado de Pessoa e dia, Resumo por
dia, Por pessoa e Por obra. Usa o MESMO recorte do Quadro — período e todos os filtros da
barra lateral (função, escala, tipo de pessoa…) — então "onde estão os vigias esta semana"
é marcar Vigia e abrir o mapa. Nada de tela nova, como ele pediu na véspera.

**O que mostra:**
- uma **bolha por obra**, no ponto da coordenada do cadastro, com o NÚMERO de pessoas
  diferentes que bateram ali no período (ou no dia escolhido); tamanho pela raiz do número
  (uma obra de 100 não esconde as de 5);
- **cor pelo quanto o ponto está completo** nos dias julgáveis da obra: verde de 90% para
  cima, âmbar de 70 a 89%, vermelho abaixo, cinza quando ninguém bateu;
- a **janelinha da obra** no clique: quantas bateram, quantas da obra não bateram nenhuma
  vez, dias completos, com buraco, faltas, em conferência e o botão **"Ver as pessoas"** (abre
  Pessoa e dia filtrado na obra);
- **"Onde tem mais gente"**, a lista ao lado, em ordem; clicar centraliza a obra;
- **dia a dia**: escolher um dia, ou "▶ dia a dia" para passar os dias do período em
  sequência (1,3 s cada) — o mapa "se mexe" com a obra que encheu ou esvaziou;
- **onde cada batida foi feita** (opcional): um ponto por batida, vermelho quando foi FORA
  da cerca — até 4.000 pontos; acima disso, os mais recentes e um aviso;
- **obras sem coordenada** listadas embaixo (não dá para pôr no mapa).

**Decisões:**
- **Peça nova: Leaflet 1.9.4** (biblioteca de mapa, licença BSD, livre), guardada DENTRO do
  sistema (`app/apps/ponto/static/leaflet/`), servida pelo próprio ERP só para quem vê o
  ponto. Não depende de site de terceiro para funcionar o código. O **fundo do mapa** (ruas,
  bairros) vem do **OpenStreetMap**, gratuito e sem chave; se um dia o volume pedir, troca-se o
  endereço das imagens por um serviço pago sem mexer no resto. Avisado ao dono (regra "não
  introduzir dependência nova sem avisar").
- Conta "pessoas que bateram", e não "batidas": 4 batidas da mesma pessoa são 1 pessoa.
- Uma conta só (o mesmo `quadro.montar`), com a mesma trava de "uma por vez" do Quadro:
  pedido simultâneo recebe "aguarde" em vez de dobrar o trabalho do servidor.
- **Defeito achado no ensaio e corrigido:** com o teclado do mapa ligado (padrão da
  biblioteca), o clique foca o mapa, a tela rola uns pixels e o clique na bolha se perde.
  Desligado.

**Não conferido:** o fundo do mapa de verdade — no ensaio as imagens do OpenStreetMap foram
trocadas por um quadrado cinza (o ambiente não alcança a internet). Bolhas, números,
janelinha, "Ver as pessoas", dia a dia e pontos foram exercitados no navegador, sem erro.

**Ideias deixadas para o dono escolher:** mapa por HORA (quem chegou atrasado, onde);
esperado × presente por obra (quantos a escala previa e quantos vieram); os vigias da noite
com as rondas no mapa; quem passou por duas obras no mesmo dia (o trajeto).

## 21/10/2026 (noite, segunda) — Uma tela gerencial: a janela da pessoa, as etiquetas e o mapa

**Pedido do dono:** *"tem muita tela tratando de assuntos parecidos (…) uma tela gerencial
mais completa, que através dos filtros nos desse as informações, ao invés de ter aquele
bando de tela (…) sempre o nome, a função, os horários da batida e o local (…) clicar e ver
a geolocalização no modal, com a obra (…) permitir ajuste individual, compensação (…) dar
destaque se é vigia, se a ronda está ativa, se o celular está liberado (…) clicar na pessoa
e ver os pontos do dia, voltar no dia anterior (…) o cadastro com todas as informações, as
solicitações, se foram atendidas, e a partir daí atender (…) as coisas têm que estar
integradas e inter-relacionadas"* — a regra de sempre do ERP.

**O que ficou:**
- **A janela da pessoa** (`core/paineis.py`, rota `/erp/api/ponto/pessoas/<id>/painel`):
  abre de QUALQUER tela do ponto ao clicar no nome (Quadro, Validações, Pessoas, Qualidade,
  Hoje). Abas: **Dia** (as batidas com a obra, a situação, se foi dentro da cerca, mapa e
  foto; navega dia a dia; para quem responde por um ponto da obra ou de equipe, **quem
  bateu no aparelho dele naquele dia**, cada um clicável); **Mês** (calendário com as
  horas de cada dia, o que falta e os pedidos esperando; clicar no dia abre o Dia);
  **Pedidos** (todos, com a situação; aprovar e negar os que esperam quem está olhando;
  validar e rejeitar as batidas em conferência); **Aparelhos** (bloquear; aprovar leva à
  Configuração, onde se escolhe o tipo); **Cadastro** (tudo, e os botões que antes só
  existiam em Pessoas: trocar escala, jornada e obras, acesso no aplicativo, código não
  chegou, banco de horas). No alto: ajustar o dia, compensação, atestado ou licença,
  afastamento, espelho e impressão.
- **As etiquetas** (`core/etiquetas.py`): 🛡️ vigia, 🔦 ronda ativa, ⚠️ vigia sem ronda,
  📱 celular liberado / esperando aprovação, ⚠️ vigia sem celular liberado, 🏗️ responde por
  ponto da obra, 👥 ponto de equipe, 🗂️ administrativo da obra, 🔑 sem PIN (este só na
  janela, para não poluir as listas). No Quadro, ao lado do nome, com o filtro **"Tipo de
  pessoa"** — inclusive "Vigia com pendência (sem ronda ou sem celular)", o que o dono
  quer achar fácil ("o correto é os vigias todos com ronda e celular").
- **O mapa da batida** (`/erp/api/ponto/marcacoes/<id>/local`): janela com o mapa do Google
  (sem chave), a obra da batida, se foi dentro da cerca e a distância do centro da obra.
  Abre clicando na batida no Quadro e na janela da pessoa. **Não verificado:** o mapa
  dentro da janela (o ambiente de teste não acessa o Google).
- **"Por obra"** no Quadro (a obra do dia = a da primeira batida; sem batida, a do
  cadastro), com clique para as pessoas daquela obra.
- **O menu:** o **Quadro é a primeira aba** e a porta de entrada do Ponto; **Hoje e Pessoas
  saíram da barra** (`ABAS_FORA_DO_MENU`) — os endereços continuam abrindo, e o Quadro
  aponta para eles (o dia por obra que se atualiza sozinho; o cadastro de quem não bate
  ponto). Validações continua: é onde se decide em lote e se abre o documento do atestado.
  Voltar uma aba ao menu é uma linha em `gestao.py`.
- **Refatorado para servir às duas telas:** as ações do DP (ajuste, compensação, atestado,
  afastamento) e as do cadastro saíram do bloco de uma tela só para a parte comum, com
  "o que atualizar depois" como argumento.

## 21/10/2026 (noite) — O quadro do ponto e a inclusão em lote

**Pedido do dono**, depois do incidente da saída (muita gente sem a 4ª batida): *"para
evitar uma enxurrada de pedidos, é melhor que a gestão faça isso em lote (…) uma tela que
liste as pessoas, um dia ou dias, a pessoa em linha — pessoa, dia e os pontos dela — ou só
o dia (…) o que é falta e o que não é, quem tem presença e quem não tem (…) só tem uma
batida, duas, só a primeira (…) quem está 100 %, onde estão os buracos"*, com a barra
lateral cheia de filtros, e telas mostradas durante a construção.

**O que ficou (`core/quadro.py`, aba Quadro):**
- **Três visões:** pessoa e dia (as batidas em pílula, o que falta em vermelho com a hora
  que a escala esperava; dia sem batida nenhuma vira uma frase só); resumo por dia (clicar
  num número abre as pessoas daquele dia naquela situação); por pessoa (quem tem mais
  buraco e falta primeiro).
- **Filtros:** período (atalhos hoje, ontem, esta semana, 7 dias; até 31 dias), pessoa,
  obra do cadastro, obra em que bateu no período, função, escala, situação no cadastro; e
  do dia: sem batida, com batida, completo, com batida faltando, falta, justificado, não
  trabalha, em conferência, com pedido — quantas batidas (0 a 4+) — falta a batida X.
  Os números do topo contam o recorte inteiro e são clicáveis.
- **A conta é a do espelho.** O `espelho.montar` ganhou a leitura em bloco (`espelho.Lote`):
  as mesmas regras, uma leitura para todos. Medido: 413 pessoas, 1 dia em 0,14 s; 7 dias em
  0,31 s. Há teste conferindo que o quadro e o espelho dizem a mesma coisa dia a dia. Um
  quadro por vez (a lição do incidente) e nada se calcula ao abrir a tela — só no botão.
- **"Com batida faltando" é de quem bateu PARTE do dia.** Quem não bateu nada está em "sem
  batida" (e, sem justificativa, em "falta").
- **Inclusão em lote:** marca as linhas (ou "marcar todas com batida faltando"), escolhe
  qual batida (a tela mostra em quantas falta cada uma), a hora (em branco = a da escala
  de cada um; ou um horário só para todos) e o motivo. Cada inclusão vira um **ajuste da
  gestão aprovado na hora** por quem pode — o mesmo caminho e as mesmas travas de sempre
  (competência fechada, horário que já tem batida, pedido repetido, dia com todas as
  batidas). Vai de 40 em 40, cada uma na sua transação (a trava do NSR nunca fica presa
  pelo lote), e a tela diz quais não entraram e por quê.
- **Dia SEM batida nenhuma não aceita inclusão em lote** (seria inventar meio dia: é falta;
  o dia inteiro se lança pelo espelho, com o motivo).
- **Não pesa contra a pessoa:** o ajuste em lote leva o subtipo `LOTE_GESTAO`; a batida
  diz "incluída em lote pela gestão (pedido nº X): motivo"; a qualidade não conta esse
  ajuste nem tira o dia de "em dia".

**Respostas dadas ao dono na mesma conversa:**
- *Ajuste de batida que já está certa:* o ponto não deixa — o pedido é feito no dia, com os
  horários que faltam já marcados, e o servidor recusa horário que já tem batida perto, dia
  já completo, dia justificado, futuro, antes do ponto, fora do contrato, competência
  fechada e pedido repetido (`ajustes.conferir_pedido`).
- *Assinatura do espelho (Portaria 671):* pelo que conheço, a Portaria exige o comprovante de
  cada batida para o trabalhador (o aplicativo dá) e os registros disponíveis (espelho, AFD),
  mas **não obriga** a assinatura do espelho. Ela serve como prova em processo trabalhista,
  e alguma convenção coletiva pode pedir. Pedido ao dono confirmar com o advogado/contador e
  a convenção da construção; a orientação dele foi: se não for obrigatório, ignorar por ora.
- *Relatórios (para o sistema e para impressão) e envio da folha de ponto:* "depois a gente
  pensa" — não feito.

## 17/10/2026 (noite, segunda) — A conferência do rosto que se adapta à pessoa

**Pedido do dono:** *"quanto melhor a pessoa, menos se confere; quanto pior a pessoa, mais
se confere. (…) A gente começa conferindo tudo e, à medida que for compreendido que
determinadas pessoas não têm a necessidade de tanta conferência, a gente vai reduzindo.
Mas se, nas conferências aleatórias, aquela pessoa começar a aparecer de novo, a gente
volta ela para insistir na conferência."* E perguntou como parametrizar. Feito com os
números abaixo (escolha minha, ajustáveis na tela), em `core/confianca.py`:

- **Sequência limpa:** quantas conferências seguidas deram "mesma pessoa" desde a última
  falha. Falha = rosto que não confere ou foto sem rosto. **Rosto que não conferiu e o DP
  aprovou a batida é falso alarme** (foto cadastral ruim, luz): não conta como falha.
- **Níveis** (Configuração › Fotos): Em observação **100 %** (quem começa, ou falhou) →
  Boa **50 %** a partir de 20 limpas → Ótima **25 %** a partir de 60 → Exemplar **10 %** a
  partir de 150. Piso de 5 %: **ninguém sai do sorteio**.
- **Falhou, volta a conferir tudo** — inclusive o resto da mesma rodada.
- **Reincidente** (2 falhas em 60 dias): precisa do DOBRO para descer; o alerta "rosto não
  confere" passa a dizer "REINCIDENTE: Nª vez em 60 dias".
- **Garantia:** quem não teve conferência de verdade há 7 dias é conferido na próxima
  batida, sorteado ou não.
- **O sorteio é pela batida** (o número dela, embaralhado com um segredo do servidor): a
  mesma batida dá sempre o mesmo resultado, e ninguém consegue prever qual sai.
- Só conta o que veio depois do início do ponto (16/10/2026).
- **Onde se vê:** Configuração › Fotos (a tabela de níveis com quantas pessoas em cada um,
  e "Ver quem está em cada nível", reincidentes primeiro); a ficha da pessoa ganhou o
  quadro "Conferência do rosto" (nível, % e quanto falta para o próximo); o "Gasto do
  mês" diz quantas foram dispensadas pelo nível; a estimativa do mês já usa a chance de
  cada pessoa.
- **Migração 013:** a batida não sorteada ganha a linha `DISPENSADA` (custo zero, com o
  nível e a chance da hora) — senão voltaria a cada rodada. A que a regra exclui (obra,
  dia, horário) também: antes ela voltava a cada rodada e, com muitas, podia empurrar as
  batidas novas para fora do lote.
- **O que ficou de fora, de propósito:** usar o selo da Qualidade (faltas, ajustes) para
  travar o nível — o cálculo da qualidade é pesado para rodar a cada 10 minutos com 1
  worker. Se o dono quiser, o caminho é uma foto diária do selo, calculada à noite.

## 17/10/2026 (noite) — O ponto vale a partir de 16/10/2026; e o teste da AWS

**Decisão do dono:** *"os pontos deverão ser considerados a partir do dia 16/10/2026.
Desconsiderar pra todos os efeitos qualquer lançamento antes disso."* Isso responde à
pergunta "apagar os dados de teste até 15/10?" pelo caminho (C) da seção de 17/10:
**nada é apagado** — a corrente do NSR fica inteira e os comprovantes continuam
valendo — e o que vem antes é desconsiderado em tudo.

- `core/vigencia.py`: a data mora no parâmetro `ponto.vale_a_partir_de` (sem nada
  gravado, 16/10/2026), lida no máximo uma vez por minuto. Muda-se em Configuração ›
  Geral › "Início do ponto" (só quem configura o ponto).
- **Onde o corte vale:** espelho (o dia aparece como **"Antes do ponto"**, cinza, sem
  batida, sem falta, sem previsto — e por isso fora do cálculo, do banco de horas e
  da folha, que saem do espelho), impresso do espelho, filtros (obras e pessoas com
  batida), Validações, mosaico, alertas, rondas, painel Hoje, qualidade, e a consulta
  dos sistemas externos (`consultas.listar`, a do Análise de SPs).
- **O que não aceita mais nada para antes:** pedido de ajuste, correção, atestado e
  qualquer ocorrência que termine antes do início ("o ponto vale a partir de
  16/10/2026 — antes disso, nada se lança"); no aplicativo, os botões de corrigir e
  lançar somem nesses dias.
- **O que NÃO foi cortado, de propósito:** o arquivo legal das batidas (o registro
  do REP, com a corrente do NSR) continua com tudo — tirar o começo quebraria a
  corrente. Cadastros, aparelhos, PINs, QR e escalas também ficam.
- **Nos testes:** `tests/conftest.py` põe `PONTO_VALE_A_PARTIR_DE=2000-01-01`, senão
  todo teste com data antiga cairia em "antes do ponto". Registrado no `CONTEXTO.md`.

**A AWS** (o dono, 17/10: "0 conferida(s) — parou: a AWS recusou a comparação de
rostos (usuário bws-ponto-rosto): uma regra da ORGANIZAÇÃO da AWS (SCP) bloqueia").
Agora se sabe que não é o usuário: é a organização (ele mexeu no AWS Organizations
no opt-out de IA, em 15/10). Feito:

- Botão **"Testar a AWS"** em Configuração › Fotos: pergunta à AWS quem é a conta e
  tenta comparar duas imagens SEM rosto em seis regiões (não gasta o teto, não
  confere ninguém). Se alguma região passa, oferece "Usar a região X" — resolve sem
  mexer na AWS. Se todas barram, diz que é na conta que ADMINISTRA a organização, com
  o passo a passo.
- A frase do erro passou a separar os dois jeitos de uma SCP barrar: **"não PERMITE"**
  (a `FullAWSAccess` foi tirada da conta ou da unidade, ou a lista do que pode não tem
  o Rekognition — religar) e **"NEGAR"** (uma política com Deny pega o Rekognition —
  tirar a conta dela).
- Caminho B, se ele não tiver a conta que administra: uma conta da AWS nova, fora da
  organização, só para o ponto, e as chaves novas no Render (as chaves nunca no chat).
- **O botão só existe depois de publicar.**

## 17/10/2026 (tarde) — A qualidade do ponto: ranking, selo e o que cada um vê

O dono respondeu às perguntas da proposta: *"quantos ajustes por mês a pessoa
vira alerta? Define aí pra mim, pode ser algo configurável"*; quem vê o quê
depende do acesso — *"se ela apenas visualiza seus dados, vê só o dela; se for o
acesso de ponto da obra ou de equipe, vai ter uma visão ampla das pessoas
relacionadas com ele (…) ele é responsável por aquelas pessoas, precisa
batalhar para que o ponto seja batido por completo"*; e *"ranking e gamificação
já podem ser colocados agora dentro do ERP, na seção do ponto, para a gente já
estar entendendo; depois a gente pensa se divulga"*.

**As regras** (`core/qualidade.py`, com o porquê de cada uma no topo do arquivo):
dia de trabalho = situação Normal, Falta ou Batida faltando no espelho; **dia em
dia** = com todas as batidas previstas feitas na hora (sem falta, sem batida
faltando, sem batida incluída por ajuste); **índice** = dias em dia ÷ dias de
trabalho; **fotos boas** = batidas do aplicativo com foto e sem sinal; **ajustes**
= pedidos de ajuste de batida no mês (os negados contam); **pontos** = 70% do
índice + 30% das fotos − 5 por ajuste acima do limite; **selo** Ouro ≥ 95, Prata
≥ 85, Bronze ≥ 70, Atenção abaixo. Limite de ajustes: **3 por mês** (escolha minha,
como ele pediu), mudável na aba Qualidade por quem configura o ponto. A obra de
um dia é a da primeira batida; a falta vai para a obra em que a pessoa mais bateu
no mês (a do cadastro só para quem não bateu em nenhuma). A equipe de um
responsável = quem bateu nos aparelhos de obra ou de equipe dele no mês, mais a
lista do aparelho de equipe.

**Quem vê:** a gestão, na aba Qualidade, as obras que alcança (o supervisor
preso a obra não vê as outras); a pessoa, o dela (Meu mês); o responsável pelo
ponto da obra, a equipe (menu "Ponto da minha equipe" — só aparece para quem
responde por aparelho aprovado). Ranking e selo ficam por ora só para dentro
(ERP e responsável); divulgar para todos é decisão dele, depois.

**Desempenho:** o cálculo usa o espelho de cada pessoa (o mesmo da exportação
CSV); com centenas de pessoas, a primeira abertura de "todas as obras" leva
alguns segundos — recortar por obra é mais rápido. O resultado fica guardado 10
minutos, e é refeito quando chega batida ou pedido novo.

**Testes:** `tests/test_ponto_qualidade_banco.py` (regras puras do selo e do dia,
ranking da gestão com limite de ajustes e escopo do supervisor, a pessoa e o
responsável no aplicativo) e conferido no navegador (aba Qualidade, "Meu ponto
em dia", "Ponto da minha equipe").

## 17/10/2026 — Em aberto: dados de teste, WhatsApp e o painel de problemas

**Apagar os dados de teste até 15/10** (o dono: "toda a informação que a gente
coletou de 15/10 para trás é de teste; queria excluí-las"). NÃO feito — precisa
da escolha dele, porque a batida tem número sequencial (NSR) e cada uma é
encadeada à anterior (`hash_encadeado`; o "código de verificação" do comprovante
são os 16 primeiros caracteres dele). Apagar as de teste deixa as reais
começando no número seguinte (ex.: a primeira real seria a 1.137), com a
corrente presa a uma batida que não existe mais. Caminhos postos para ele:
(A) apagar e anotar onde a corrente recomeça — os números das reais não mudam,
os comprovantes já mostrados continuam valendo, mas o AFD começa num número
alto; (B) apagar e RENUMERAR as reais a partir de 1 — fica limpo, mas muda o NSR
e o código de verificação das batidas de 16/10 em diante (comprovantes já vistos
no aplicativo deixam de bater); (C) não apagar: marcar como teste e esconder de
espelho, mosaico, alertas e relatórios. Antes de qualquer um: confirmar com a
contabilidade como o AFD deve começar. Sai junto: fotos, pedidos, recusas,
alertas, rondas e conferências do período; ficam cadastros, aparelhos, PINs, QR,
escalas e configuração.

**O WhatsApp bloqueia** (o Z-API derruba com volume; ver a mensageria). Proposta:
no ponto, tirar volume (QR pelo aplicativo em vez do WhatsApp automático; código
de primeiro acesso em lote, impresso, no dia do cadastro); no médio prazo, a API
oficial do WhatsApp (Meta) — decisão e trabalho do chat da Mensageria.

**O painel de problemas do ponto** (o dono: "relatório de problemas, de
inconsistência, apontando pessoa, dia, qual é o problema (…) atacar pessoas que
são problema (…) o problema é não se levar a sério a batida e querer ficar
pedindo ajuste"). Proposta, sobre os alertas que já existem (`core/alertas.py`):
na entrada de cada um, o que é dele — a pessoa vê o próprio mês (dias completos,
batida faltando, foto sem rosto, ajustes pedidos); o responsável do ponto da
obra e o encarregado veem a obra; a gestão vê o ranking por obra, responsável e
pessoa, com um índice de "ponto em dia" e os ajustes pedidos por pessoa (com
alerta acima de um limite). Esperando o dono decidir limites, quem vê o quê e se
haverá ranking/gamificação.

## 16/10/2026 — A batida sem internet no ponto da obra

O dono: *"imagina que a gente está num local que não tem internet. E aí, como é
que a gente pode resolver?"*. Discutidos três caminhos (o aplicativo atual sem
internet, um aplicativo de loja com as mesmas telas, um aplicativo do zero); o
dono escolheu o primeiro, **só no ponto da obra** ("pode ser em quem bate ponto
da obra"), e **reconhecer CPF e QR pela lista** ("pode reconhecer por lista"),
aceitando o risco do CPF (abaixo). Antes, testado no navegador de verdade: o
que fica guardado sobrevive a fechar a aba e o navegador; some com aba anônima
e com "limpar dados"; o aplicativo abre sem internet só se já abriu com ela.

**Como ficou** (`core/sem_internet.py`, o `app.html` do tablet, migração 012):
- **A lista.** Com internet, o tablet baixa de 30 em 30 min a lista de quem
  costuma bater nele: no de obra, quem tem a obra dele no cadastro + quem bateu
  nele nos últimos 60 dias (aparelho sem obra NÃO lista a empresa inteira); no
  de equipe, a lista dele. Só quem bateria agora (sem desligado, afastado,
  inativo, fora do contrato). Vai o nome, a impressão digital (SHA-256) do CPF
  com um prefixo, e a do QR do WhatsApp (a mesma que o banco já guarda).
- **Sem internet** (o navegador diz que caiu, a chamada falha, ou não responde
  em 8 s — sinal fraco que fica pendurado): CPF e QR do WhatsApp são conferidos
  na lista. O QR do "Meu ponto" (30 s) **não funciona** sem internet — quem o
  gera é o servidor. CPF fora da lista **bate** (a Portaria não deixa impedir) e
  vai para conferência. Foto, GPS e cerca locais de sempre. A batida fica no
  IndexedDB do navegador ("Ponto guardado — HH:MM") e a faixa "📵 N batidas
  guardadas" aparece no alto. "Bater de novo?" também sem internet, pela fila.
- **A hora.** A cada resposta do servidor o tablet anota a hora dele (cabeçalho
  `Date`, ou a hora exata da lista) e o contador interno (`performance.now`).
  Sem internet: hora = servidor + tempo contado. Se contador e relógio do tablet
  discordam em mais de 2 min (relógio mexido, ou o aparelho dormiu e o contador
  parou) ou se a página recomeçou (o contador zera), a hora **não está
  comprovada**: a batida vai para conferência com as duas horas no motivo. No
  servidor (`decidir_hora`): hora no futuro vira a de chegada; hora anterior à
  última conexão do aparelho vai para conferência.
- **Duas travas contra hora forjada** (achadas na revisão antes de publicar: quem
  mexe no tablet aprovado poderia mandar "ontem às 7h" como comprovada): se o
  aparelho bateu COM internet depois da hora informada (fora de 3 min de folga
  da reconexão), a guardada devia ter subido ali — conferência; e a que chega
  mais de 24 h depois vai para conferência de qualquer jeito
  (`_chegada_suspeita`).
- **A chegada.** De 30 em 30 s (e quando o navegador avisa que a rede voltou) o
  tablet manda a fila, a mais antiga primeiro, para `/app/api/tablet/bater-sem-
  internet`. A batida passa pelo MESMO `marcacoes.registrar` (pessoa, aparelho,
  cerca, foto, fila rápida), com `agora` = hora da batida. `timestamp_servidor` é
  a hora da batida (o espelho e o cálculo seguem lendo dela); `recebida_em`, a
  chegada; o NSR é dado na chegada (ordem do registro — o mesmo princípio da
  batida incluída por ajuste). `id_no_aparelho` + índice único: mandar de novo
  devolve a primeira. Recusa de verdade (403/400/404) sai da fila e o motivo fica
  lembrado no tablet (`ponto_fila_recusadas`); conexão, servidor fora do ar e
  503 (migração não aplicada) ficam para depois.
- **Dois ajustes no `registrar` que valem para todos:** a busca de batida
  repetida e a da fila rápida passaram a olhar SÓ perto da hora da batida (antes,
  "tudo depois de"), senão a entrada guardada de 3 h atrás virava "repetida" de
  uma batida de agora. Com internet, nada muda (a hora é agora). E a batida
  guardada NÃO mexe nas rondas do vigia (chegariam vencidas).
- **O tablet abre sem internet** direto na batida: o estado do aparelho fica
  guardado e só a RESPOSTA do servidor o muda; o service worker (v3) usa a
  casca guardada também quando o servidor responde erro (renovação, 502).
- Se a conexão cai entre a identificação e o envio, a batida é guardada (o
  servidor, se tiver registrado e só a resposta se perdeu, devolve a repetida).
- No espelho do ERP, a batida guardada tem "📵" e a dica diz quando chegou.

**O risco aceito pelo dono:** a impressão digital do CPF protege de olhar
curioso, não de um técnico com o tablet na mão (CPF tem poucas combinações).
Por isso a lista leva só quem bate naquele aparelho.

**O que ficou de fora:** o celular de quem bate no próprio aparelho (o dono
escolheu só o ponto da obra; no iPhone o Safari apaga o guardado depois de ~7
dias sem uso); pedidos (atestado, ajuste) sem internet; o ERP saber que um tablet
tem batidas esperando (a fila mora nele — ver PERGUNTAS.md). Não testado: um
tablet Android de verdade em modo avião (o teste foi no Chromium, com a rede
cortada pelo navegador); reiniciar o aparelho.

**Testes:** `tests/test_ponto_sem_internet_banco.py` (lista, chegada com a hora
da batida, não duplicar, conferência por hora e por CPF, hora do futuro, a
repetida que não pode ser confundida, afastado recusado na chegada, a regra da
hora). No navegador: tablet aprovado, rede cortada, CPF da lista, CPF fora da
lista, QR do WhatsApp, QR do Meu ponto recusado, relógio adiantado 1 h na mão,
navegador fechado e reaberto sem internet, rede de volta — as 5 chegaram, com a
hora certa, os status certos e a foto.

## 16/10/2026 — "O código não chegou" e "código inválido ou vencido"

- O dono: *"a pessoa não recebe o código (…) e não chega nada no aparelhos.
  Consultei o telefone no cadastro, tá normal. E aí eu não sei o que fazer."* E
  depois: *"código inválido ou vencido — não sei em qual circunstância aparece."*
- **Por que não sai, em silêncio** (`acesso.pedir_codigo` responde igual em todos,
  de propósito): CPF fora do cadastro do ERP, pessoa DESLIGADA, celular sem DDD, 3
  códigos na última hora — ou o WhatsApp recusou/o tipo está desligado em Mensagens.
  ⚠️ O celular que o ponto usa vem do **Registro de Colaboradores** quando a pessoa
  está nele, não do ERP — "o telefone do cadastro está certo" pode ser o do ERP.
- **Ficha da pessoa › "Código não chegou?"** (`acesso.diagnostico`, quem trata o
  ponto, no escopo): o celular usado e de onde vem (avisando quando o do ERP é
  outro), o PIN (criado? travado?), o que impede o envio, e os 5 últimos códigos com
  o resultado do envio (cruzando `mensageria.envios` pelo número e pela hora).
- **"Gerar código para entregar em mãos"** (`acesso.codigo_em_maos`, só quem
  configura o ponto — o código deixa criar o PIN da pessoa): não precisa de celular,
  vale 15 min, fica no registro quem gerou. No app, "Já tenho um código (entregue
  pelo DP)" abre o campo sem mandar outro.
- **"Código inválido ou vencido" — o caso mais comum era nosso:** só o ÚLTIMO código
  valia; com o WhatsApp demorando, a pessoa pedia de novo e digitava o primeiro.
  Agora vale qualquer código ainda no prazo (os 5 últimos), uma vez cada; o erro
  conta tentativa em todos os válidos (chutar contra vários não rende mais chances).
- **Limites por REDE subiram** (o cadastro em grupo no Wi-Fi do escritório sai pelo
  mesmo IP): códigos 20 → 100 por hora, entradas 60 → 300, registro de aparelho 30
  → 120. As travas por pessoa ficaram iguais.
- ⚠️ **O teto de WhatsApp da EMPRESA** (ERP › Mensagens, padrão 40 por hora e 200 por
  dia, somando código, QR, avisos e rondas) corta o código em silêncio quando
  estoura — status LIMITE no registro, que a ficha "Código não chegou?" mostra como
  "passou do limite de mensagens". Ajuste com o dono (é da Mensageria).
- Manual: a nota do código em mãos e a linha "Código inválido ou vencido" na tabela
  de mensagens (página, PDF e a versão no Claude).

## 16/10/2026 — Os pedidos de aparelho na ordem em que chegam

- O dono, no primeiro dia de pedidos: *"tem alguns que estão demorando mais (…)
  a que informou depois chegou antes da pessoa que informou primeiro."* A tela de
  aparelhos punha os pendentes em **ordem de nome**: o pedido de quem pediu antes
  podia estar no meio da lista, parecendo não ter chegado. E a lista só mudava ao
  recarregar a página.
- Agora: pendentes com o **mais novo em cima** e a hora do pedido na coluna
  ("pedido dd/mm hh:mm" — `dispositivos.para_json` › `pedido_em`: o registro do
  navegador ou a última entrada nele; o pedido feito depois do login atualiza o
  "último uso"), o mesmo na fila de Validações, e a lista de aparelhos se
  atualiza sozinha a cada 30 s enquanto está na tela (não com janela aberta).
- Outras causas de "demora" que não são do sistema, ditas ao dono: a pessoa avisou
  no grupo antes de tocar em "Pedir a aprovação" (o link fica no fim da tela
  inicial); o código do WhatsApp do primeiro acesso chegou tarde; quem já está
  liberado no cadastro só gera o pedido ao abrir o "Meu ponto".

## 15/10/2026 (noite) — O teste de carga NA PRODUÇÃO, com celulares de verdade

- O dono: *"pegar uns 4–5 telefones já cadastrados, que acessem algum endereço e
  na prática começar a disparar uma simulação de envio do ponto (…) como se fossem
  as imagens (…) rodando cada um por alguns minutos ao mesmo tempo."*
- **Não grava batida, de propósito**: batida é registro de ponto (NSR sem furo,
  que não se apaga — Portaria 671). `core/carga.py` faz o trabalho de uma batida
  menos o registro: a foto do tamanho da real (o celular gera ~90 KB, 800 px,
  JPEG 0,8), o tratamento (reduzir, hash, sinais), as leituras, e GRAVA a foto
  em `ponto.fotos` com os bytes dentro de um SAVEPOINT desfeito. Não faz NSR,
  marcação, alerta nem Drive (que na batida real é depois da resposta).
- **Só ligado**: quem configura o ponto liga por até 60 min (Configuração › Geral
  › "Teste de carga", botão de 30 min); fora disso a rota recusa. Só aparelho
  APROVADO (qualquer tipo). Página: `/ponto/app/teste-carga/`, aberta no mesmo
  navegador em que o aparelho bate — usa a identidade dele.
- Cada celular mostra na tela: envios certos e com erro, KB por foto, ida e volta
  (mediana, 95%, pior — o que a pessoa sente) e o tempo no servidor. O quadro do
  ERP soma os aparelhos — na memória do serviço: se ele se renovar no meio, a
  conta recomeça (e os erros de "conexão caiu" na tela do celular mostram isso).
- Aqui (3 aparelhos simulados, 1 envio por segundo cada, 1 min): 182 envios, 0
  erro, ida e volta ~37 ms, servidor ~24 ms; nenhuma foto ficou no banco.
- **O "play" (mesmo dia):** o dono — *"deveria ser algo que eu dê um play no telefone
  e ele comece e fique disparando dados. Não é pra operar de verdade (…) ou mesmo
  um script no PC. Nenhum desses dados de teste precisam ficar armazenados."* O
  quadro "Teste de carga" do ERP ganhou "▶ Começar o teste": o COMPUTADOR de quem
  configura faz o papel de N aparelhos (1–10, padrão 5) por M minutos (1–15),
  cada um mandando uma imagem de ~90 KB por segundo
  (`/erp/api/ponto/teste-carga/simular`, aparelho simulado, mesma `carga.simular`).
  Liga o teste ao começar e desliga ao terminar. Aqui: 5 aparelhos, 1 min, 300
  envios, 0 erro, ida e volta ~80 ms, servidor ~40 ms; nada guardado (só a
  estatística de tempo por tela que o ERP já soma de minuto em minuto). A página
  do celular ficou como opção, para medir a internet de uma obra.
- **Na produção (o dono rodou):** 5 aparelhos, ~6 min — 1.832 certos, 10 com erro
  (0,5%), servidor ~107 ms (pior 1,1 s), ida e volta ~1,1 s (95%: 2,6 s, pior
  18,8 s), memória com pico de 32%. A ida e volta é quase toda viagem (5 fotos
  por segundo saindo da mesma internet); o pior caso e os erros eram a renovação
  do serviço (`--max-requests 1000`, ~15–20 s sem resposta no Render). Decisão
  do dono: `--max-requests 5000` (Procfile e Start Command). Repetido com **10
  aparelhos: zero erros**.
- ⚠️ A simulação é um pouco MAIS LEVE que a batida (no teste de carga local, a
  batida de verdade levou ~60 ms no servidor e a simulação ~24 ms): por isso a
  sugestão de 1 envio por segundo por celular — 5 celulares = 5 por segundo, umas
  6× o pico real de 500 pessoas em 10 min.

## 15/10/2026 — Teste de carga: 500 pessoas batendo em 10–15 min

O dono: *"bater o ponto de 500 funcionários num intervalo de 10–15 min vai ser
viável? Fiquei com receio do desempenho."* Medido, não estimado: 500 pessoas e 12
aparelhos de obra num banco de teste, o serviço com o MESMO comando da produção
(gunicorn, 1 worker, 4 threads, `--max-requests 1000`) preso a 1 processador,
cada batida com identificação + foto de ~60 KB (o que o tablet manda).

- **O servidor aguenta com folga.** No limite (12 aparelhos sem pausa): ~30
  batidas por segundo, cada uma em ~0,2 s. 500 pessoas em 10 min pedem menos de
  1 por segundo. No ritmo de fila de verdade (~7 s por pessoa): identificar em
  0,02 s e bater em 0,06 s (p95 0,14 s). Memória do serviço: ~210 MB.
- **O limite de verdade é o número de aparelhos**: cada um atende ~8 pessoas por
  minuto (2 s de contagem para a foto + 3 s de confirmação + a pessoa mostrar o
  QR) — ~75 pessoas em 10 min, ~110 em 15. Obra com 150 pessoas na mesma janela
  precisa de 2 aparelhos.
- **Achado 1 — a "fila rápida" mandava a fila legítima para conferência.** A
  regra de fraude (5+ pessoas com menos de 10 s entre uma e outra no mesmo
  aparelho → conferência na hora) pegou **192 de 240 batidas (80%)** no ritmo
  real. O aparelho não deixa bater em menos de ~5 s; o limite virou **4 s**
  (`alertas.SEQUENCIA_SEGUNDOS`, `marcacoes.SEQUENCIA_NA_HORA_SEGUNDOS`). No
  ritmo mais rápido possível (5,3 s), **0** em conferência. Quem digita o CPF de
  colega ausente leva o tempo normal e cai na foto (rosto, foto repetida, mosaico).
- **Achado 2 — o serviço se renova no meio da fila.** Com `--max-requests 1000`
  e ~2,5 acessos por batida, a renovação cai dentro do pico das 7h; a conexão que
  estava aberta cai e aquela identificação respondia "Sem internet agora". O
  aparelho da obra agora tenta a identificação de novo, uma vez, depois de 1,5 s
  (`chamarComNovaTentativa` — só a identificação, que não grava nada; a batida
  não se repete sozinha). Subir o `--max-requests` é decisão do dono (Procfile E
  Start Command do Render, os dois).
- As fotos sobem para o Drive DEPOIS, numa linha só (1–3 s cada): 500 fotos
  levam de 8 a 25 min para esvaziar a fila — a batida não espera por isso.
- Não medido: a internet de cada obra (cada batida manda ~60 KB, pouco para 4G)
  e o processador real do Render (o teste usou 1 núcleo deste ambiente).

## 15/10/2026 — A conferência do rosto: a chave da AWS sem permissão

- O dono criou a conta e a chave da AWS. Primeira rodada: "0 conferida(s), 0
  suspeita(s), 2 virou(aram) foto cadastral — parou: a AWS respondeu com erro:
  AccessDeniedException (…) user/bws-ponto-rosto is not authorized to per." —
  a chave funciona (a AWS reconheceu o usuário), mas o usuário IAM não tem a
  permissão `rekognition:CompareFaces`. As 2 que viraram foto cadastral não
  passam pela AWS (é a regra de quem não tem foto cadastral).
- **Pendente com o dono:** no IAM, dar ao usuário `bws-ponto-rosto` uma política
  com `rekognition:CompareFaces` (Resource "*") e rodar de novo.
- **Depois:** o dono mostrou que a política com `rekognition:CompareFaces` (Resource
  "*") já existia no usuário, pôs uma segunda igual, e a AWS seguiu recusando. Com
  a permissão dada, o bloqueio vem de outro lugar — SCP da organização (ele mexeu
  no AWS Organizations para o opt-out de IA), limite de permissões no usuário, um
  Deny, ou a chave do Render ser de outro usuário. A AWS diz qual no FIM da frase,
  justamente o pedaço que a tela cortava; a frase em português passou a dizer quem
  bloqueou e onde mexer. Sugerido ao dono o simulador de políticas do IAM, que
  responde na hora sem precisar publicar.
- A tela cortava a frase da AWS em 160 letras ("to per."). Agora os erros comuns
  viram frase em português com o que fazer (`rosto.explicar_erro_aws`): sem
  permissão, chave não reconhecida, conta não liberada, AWS pedindo calma.

## 15/10/2026 — Manual do colaborador (com as telas)

- Pedido do dono: um manual de uso para os colaboradores (não para o RH), para os
  três jeitos de usar — só consulta, bate no próprio celular, ponto da obra —, do
  primeiro acesso aos pedidos, regras e alertas, com telas.
- Publicado como página (Artifact, privado do dono, que compartilha):
  https://claude.ai/artifact/ULFBh1N3Drs3gZmXsgzw4W — 10 partes, 21 telas tiradas
  do aplicativo de verdade num banco de teste (nomes de exemplo). Para atualizar a
  partir de outro chat: ler a página pelo endereço e publicar no mesmo endereço.
  **Ao mudar uma tela ou uma regra que o colaborador vê, o manual fica velho** —
  vale conferir a parte correspondente.
- **No próprio sistema (mesmo dia, pedido do dono: "gerando PDF não ficou legal (…)
  podemos disponibilizar online? ponto/app/manual.html")**: o manual mora em
  `static/manual/` (index.html + telas/ + manual-bws-ponto.pdf) e abre sem login em
  `/ponto/app/manual/` (`/ponto/app/manual` e `/ponto/app/manual.html` levam até lá);
  o PDF em `/ponto/app/manual.pdf`. Link "📖 Como usar o aplicativo (manual)" na
  tela de entrada e na tela inicial do app. O PDF ruim tinha causa: o papel A4 tem
  ~690 px de largura e caía nas regras de CELULAR (fotos numa fila que rola para o
  lado, cortada no papel). Agora há um formato de papel próprio (`@media print`):
  cada parte começa numa página, texto em cima e telas lado a lado, sem quebrar um
  passo no meio, dúvidas abertas. **Como regenerar o PDF** depois de mudar a página:
  abrir `/ponto/app/manual/` no Chromium (Playwright), `emulateMedia print`, abrir os
  `<details>` e `page.pdf({format: "A4", printBackground: true})` — sem
  `loading="lazy"` nas imagens, senão elas saem pretas no PDF (aconteceu).
  A página no Claude (Artifact) é a mesma, sem o esqueleto HTML.
- Fotografando as telas, dois textos do aplicativo estavam errados e foram
  corrigidos: a aba Pedidos de quem entrega na obra mandava tocar em "Entregar
  atestado, licença ou pedir ajuste", e o botão no ponto da obra se chama
  "Atestados, afastamentos e ajustes"; e o anexo do celular dizia "Escolher foto
  ou arquivo" enquanto o do ponto da obra diz "📎 Anexar o documento — foto ou
  arquivo" (agora igual nos três lugares).

## 14/10/2026 (tarde) — "Este aparelho vai bater ponto?" só depois do CPF e PIN

- O dono, sobre a frase da tela de entrada ("Este aparelho vai ser o ponto da
  obra? Pedir a aprovação"): era específica demais — *"quem vai dizer se é da
  obra ou é o celular é quem vai aprovar, não quem está pedindo"* — e não podia
  aparecer sem login: *"antes de tudo precisa ser feito o cadastro do telefone e
  gerado um PIN. Aquilo é a segurança inicial. Depois desse momento é que eu
  poderia pedir para que o telefone fosse aprovado"*.
- **Agora:** a tela de entrada não pede aprovação de nada. Depois de entrar com
  CPF e PIN, num navegador que o sistema não conhece, o menu mostra "Este
  aparelho vai bater ponto? Pedir a aprovação". O pedido entra como "Pedido por
  NOME (CPF final xxx)", com a pessoa como responsável sugerida, e vai para a
  fila de Validações mesmo que ela não esteja liberada para bater no celular
  (pode ser o tablet da obra que ela está configurando). O TIPO e o responsável
  se escolhem na aprovação.
- Outra pessoa que entre no aparelho antes da aprovação não troca o nome de quem
  pediu (`dispositivos.foi_pedido`). Sem coluna nova: o pedido é marcado na
  descrição ("Pedido por "), para não depender de migração.
- O registro anônimo (`/ponto/api/dispositivo/registrar`) continua existindo —
  é ele que dá a credencial ao navegador —, mas a tela só o usa depois do login.
- Primeiro eu fiz uma pergunta "de quem é este aparelho?" para quem pede; ele não
  quis (o tipo é de quem aprova). Desfeita antes de publicar.

## 14/10/2026 (tarde) — A busca de Pessoas diz por que alguém não aparece

- O dono: *"tem uma pessoa em específico que não tá aparecendo em Ponto ›
  Pessoas"* (colou a linha dela do Registro: fase "Cadastro Concluído", sem data
  de saída à vista). Daqui não se vê o banco de produção, então a resposta ficou
  no sistema: buscando por CPF (11 dígitos, com ou sem máscara) ou por 3 letras
  ou mais do nome, quem casa e NÃO está na lista vem num quadro com o motivo —
  falta cadastro no ERP (com o botão que resolve), desligada pela data de saída
  ou pela Fase Atual (com a data/fase), CPF que não passa nos dígitos, "não bate
  ponto" marcado no ponto, ou não está em lugar nenhum (com a data da última
  cópia do Registro). `registro.por_que_nao_aparece`.
- Só para quem alcança todas as obras: para o preso a obra, dar o motivo de
  alguém de outra obra confirmaria que a pessoa existe.
- Os textos da Configuração diziam que afastado "bate, mas para conferência" —
  corrigidos para "não batem até voltar" (a decisão da manhã).
- **Em aberto:** a linha da Karla tem 20/06/2026 e 01/11/2024 lado a lado; não
  sei qual coluna é cada uma. Se 20/06/2026 for a Data de Saída, ela está
  desligada desde junho e a busca vai dizer isso.

## 14/10/2026 — Afastado não bate; onde o celular estava, no mapa

- **Afastado não bate** (o dono: *"o cara está afastado, ele não bate ponto, não
  está trabalhando, não pode estar trabalhando. Se retornou, a gente altera o
  cadastro dele"*). Antes ia para conferência; agora a batida é RECUSADA
  (`marcacoes.AFASTADO_NAO_BATE`), e o ponto da obra já recusa na identificação,
  antes da foto. Pedido (atestado etc.) ele continua entregando. "Afastado" é a
  fase do Registro de Colaboradores (ou a situação do ERP).
- **Onde o celular estava** (*"a geolocalização é coletada? a gente pode checar
  onde a pessoa estava no momento?"*): a batida aceita sempre guardou as
  coordenadas, mas nenhuma tela mostrava. Agora: no espelho do ERP, embaixo da
  batida fora da obra (ou em conferência), "📍 ver no mapa" (Google Maps no
  ponto) com a distância; o mesmo na Validações e em cada ronda feita na tela
  Rondas. A batida RECUSADA passou a guardar o lugar também (em
  `ponto.recusas.detalhes` — latitude e longitude); ainda não há tela que liste
  as recusas, só o alerta "tentou bater fora da área".
- O dado é lido só no momento da batida (e da ronda) — o sistema não acompanha
  ninguém. A consulta do mapa fica com quem já vê o espelho.

## 13/10/2026 (noite) — A obra do cadastro não trava; aparelhos com busca, filtro e excluir

- **O celular pessoal que não batia** (*"aparece 'nenhuma obra sua tem
  localização cadastrada, escolha a obra abaixo', não aparece a lista — e outro
  celular no mesmo lugar bate normal"*): a lista de obras do celular é a do
  CADASTRO da pessoa (principal + extras); sem nenhuma obra ativa ligada, a
  lista vinha vazia e a mensagem mentia. **Decisão do dono, na mesma hora:**
  *"se o cadastro não tiver obra ativa, isso não é para interferir. Se é
  colaborador ativo, é para bater ponto — tem que bater em uma obra, tendo
  geolocalização ou não. O que não pode é quem passou do último dia, está
  desligado ou não está ativo."* Então:
  - a obra da batida é a da LOCALIZAÇÃO, entre todas as obras ativas
    (`/app/api/obra-aqui`, quando a cerca não é de obra do cadastro);
  - "obra fora da lista da pessoa" DEIXOU de mandar para conferência
    (`marcacoes.decidir`) — vale para o celular e para o ponto da obra;
  - sem localização, a lista para escolher é a das obras do cadastro, e, sem
    nenhuma, TODAS as ativas (`obras_para_escolher` no `/app/api/eu`);
  - o que trava continua: desligado, inativo no ponto, antes da admissão ou
    depois da saída (já era assim). Afastado continua indo para conferência —
    confirmar com o dono se "não ativo" inclui o afastado.
- **Tela de aparelhos** (*"a lista vai aumentar — filtrar e buscar por pessoa,
  por situação; não tem como excluir? fica poluído"*): busca (nome, CPF, nome do
  aparelho, código), filtro de situação (esperando, liberados, vencendo,
  bloqueados, arquivados) e de tipo; pendentes primeiro. **Excluir:** aparelho
  SEM batida sai de vez; COM batida é ARQUIVADO (bloqueado e fora da lista,
  guardado) — cada batida guarda o aparelho que a registrou, e a Portaria 671
  pede a identificação do coletor. "Desarquivar" devolve à lista, bloqueado.

## 13/10/2026 (noite) — A ronda surpresa dos vigias noturnos

O dono: *"os vigias batem a entrada e a saída; nada garante que fiquem na obra o
tempo todo (…) vamos fazer a ronda surpresa pelo WhatsApp: entrada, saída e quatro
rondas surpresa. Só para os vigias noturnos, vinculado ao cadastro. Na tela de
liberação do aparelho, informar que a pessoa é vigia e que vai ter ronda. O vigia
começa por volta das 17–18 h e sai no outro dia de manhã, folga 36 h — não pode
receber ronda num dia que não esteja trabalhando; se não bater a entrada (pode
faltar), também não tem ronda."* E: *"vai ter uma tela própria para as rondas, ou
dentro do espelho?"* — as duas.

- **A ronda não é batida de ponto** (`ponto.rondas`, migração 011): não tem NSR
  nem entra na conta das horas; é registro de presença com foto, localização e
  hora. Motivo: a batida é da jornada (Portaria 671) — a ronda das 2 h viraria
  "saída" e desarrumaria o dia.
- **Quem é vigia:** marcação da PESSOA (`colaborador_config.faz_ronda`), em
  Pessoas › Acesso no aplicativo e, refletindo a mesma, na janela do aparelho —
  que também AVISA, em destaque, quando o responsável é vigia.
- **Quando nasce:** na ENTRADA do vigia (`rondas.ao_bater`, chamada no fim de
  `marcacoes.registrar`): a primeira batida dele entre 15 h e 23 h, sem turno
  aberto. Sem entrada — folga ou falta — nada é agendado. Batida de manhã sem
  turno aberto não abre turno (era a saída de um turno esquecido).
- **Os horários:** 4 por turno, sorteados — o turno (12 h, ou o da escala) sem a
  primeira e a última hora é dividido em 4 partes, e cada ronda cai no miolo
  sorteado da sua parte (espaçadas, imprevisíveis).
- **Na hora:** WhatsApp ao vigia com o link (`/ponto/app?ronda=…`); ele entra
  com CPF e PIN (se ainda não entrou), tem **10 min**, tira a foto e manda a
  localização — sem as duas, não vale. Feita até 30 min depois do prazo fica
  "atrasada"; sem nada, **perdida**, e sai na hora um WhatsApp para os telefones
  do resumo diário. A saída (passada a primeira meia hora) cancela as que
  faltam; turno sem saída se fecha em 16 h.
- **O relógio:** o serviço não tem agendador e de madrugada ninguém o acorda.
  Uma linha separada (`rondas.garantir_relogio`) roda a cada 30 s; nasce com o
  serviço (`bp.record_once`) e é conferida a cada requisição do ponto. Reiniciou
  o serviço, ela volta; ronda que passou meia hora da hora com o serviço fora vira
  "não enviada" (não se manda surpresa atrasada). Não liga nos testes nem sem
  `DATABASE_URL`. Conferido no ensaio: com o servidor de pé, ela agiu sozinha.
- **Onde se vê:** aba nova **ERP › Ponto › Rondas** (uma noite por vez, um vigia
  por linha: entrada, saída, cada ronda com hora, resultado, distância da obra e a
  foto); no espelho, as rondas do dia embaixo das batidas; no "Meu ponto" do
  vigia, as que já saíram (a próxima continua surpresa — a hora da agendada não
  aparece para ninguém fora da tela Rondas).
- **WhatsApp:** dois tipos novos na mensageria (`ponto.ronda`,
  `ponto.ronda_perdida`), "Só WhatsApp" — os primeiros que saem de madrugada de
  propósito; poucos por noite, fora da fila com ritmo do QR.
- **Fica de fora, para decidir:** a conferência do rosto (AWS) na foto da ronda
  (custo por foto); placas de QR Code nos pontos da obra (provar que andou pela
  obra); o número de rondas e o prazo ajustáveis na tela (hoje fixos: 4 e 10 min).

## 13/10/2026 — Sete achados do teste do dono

1. **A batida de outra pessoa no ponto da obra ia para quem estava logado**
   (*"bati o ponto de outra pessoa, diz que bateu, mas não aparece no
   espelho"*). Desde 10/10 o ponto da obra exige que o responsável entre com CPF
   e PIN — e o `/app/api/bater` olhava a SESSÃO antes do bilhete: a batida de
   quem se identificou (QR/CPF) era gravada no nome de quem entrou (se esse
   pudesse bater no próprio celular; senão era recusada). Agora o bilhete vem
   primeiro, e no ponto da obra a sessão sozinha não bate. Teste que reproduz o
   caso. **Na produção**, entre 10/10 e a publicação desta correção, as batidas
   feitas no ponto da obra com alguém logado ficaram no nome de quem estava
   logado (marcações com `identificacao = 'SESSAO'` em aparelho COMPARTILHADO).
   Não se apaga marcação (Portaria 671): o caminho é o DP tratar no espelho e a
   pessoa certa pedir o ajuste. Confirmar com o dono se foram só testes.
2. **"Foto em base64 ilegível" ao anexar PDF ou foto:** o servidor só tirava o
   prefixo `data:image/…` — o PDF (`data:application/pdf`) e a foto que o celular
   entrega sem tipo não passavam. `fotos.decodificar_base64` aceita qualquer
   `data:…;base64,`, e a frase do documento fala em documento.
3. **Ajuste depois de agora:** o "corrigir" de hoje recusa, na tela, horário
   depois da hora atual de Fortaleza ("23:58 ainda não chegou — agora são
   13:59"); o servidor já recusava.
4. **A primeira batida às 13:19 era sempre a "Entrada":** sem escala, os nomes
   das linhas do ajuste saíam da POSIÇÃO. Agora cada batida feita ocupa o nome
   mais perto do horário de costume (7, 11, 12, 17 h): com só 13:19, o ajuste
   oferece Entrada, Saída para o intervalo e Saída.
5. **"Liberado até": opção "não expira"** (celular pessoal e ponto da obra; o
   de equipe continua passageiro). Gravado como validade vazia — o sistema já
   tratava vazio como "não vence". Na aprovação e na renovação.
6. **Navegador novo / aba anônima = "celular novo":** uma página não reconhece o
   celular de um navegador para o outro (cada um guarda a sua identidade; a
   anônima apaga ao fechar). Agora, ao entrar com CPF e PIN num navegador
   desconhecido, quem é responsável por aparelho liberado vê "Este é o mesmo
   aparelho?" — com o "Sim", a liberação passa para este navegador
   (`dispositivos.mudar_de_navegador`) sem aprovação nova, e o de antes para de
   valer; o "Não" fica lembrado ali. No máximo 5 trocas por hora por pessoa.
   Risco aceito por mim, dito ao dono: quem tem o CPF e o PIN do responsável
   pode levar a liberação para outro celular (a batida continua com foto e
   localização, e o celular antigo para na hora).
7. **Batida repetida** (*"não deveria haver limite? e se eu bater duas vezes a
   entrada?"*). Bloquear não pode — o ponto eletrônico não pode impedir marcação
   (Portaria 671). Então: a batida a menos de 10 min da anterior é guardada, mas
   conta UMA vez no cálculo (`apuracao.sem_repetidas`) e o dia ganha o aviso
   "Batida repetida"; e antes de registrar, o ponto da obra e o celular
   perguntam "você já bateu às 07:00 — bater de novo?". Antes, a entrada
   repetida virava a saída para o intervalo e o dia inteiro saía errado.
- **Sem internet** (pergunta do dono): hoje não se bate sem internet — a tela diz
  "Sem internet agora", e o caminho é o ajuste com o motivo "Sem internet ou sem
  sinal na obra". Guardar a batida no celular e mandar depois é possível, mas a
  hora viria do relógio do celular (que se muda à mão): iria sempre para
  conferência. Decisão do dono. O que a Portaria 671 diz (pesquisado em
  13/10/2026, FAQ do MTE de 31/08/2023 e resumos de fornecedores — o texto do
  Anexo IX não foi lido direto): a marcação off-line NÃO é obrigatória no REP-P;
  onde existe (Anexo IX, itens 4 e 5), o coletor guarda e manda no primeiro
  momento em que voltar a se conectar, e o registro leva a hora da marcação e a
  hora da gravação, separadas.
- Dois testes antigos quebravam no dia seguinte a feriado em dia de semana
  (escolhiam "ontem" como dia de falta): pulam feriado agora.

## 12/10/2026 — Ajuste de vários dias de uma vez, e o feriado que não abria

O dono, no feriado: *"hoje é 12 de outubro, feriado (…) tentei abrir o dia do
ponto e ele não abriu (…) é para permitir a batida num feriado, porque a pessoa
pode trabalhar, extra"*; e *"eu tinha comentado de um ajuste de uma faixa de dias
— não tem como eu corrigir o ponto do dia 5 ao dia 9"* (o pedido de 10/10 dizia
"dois horários de um dia, intercalado, ou dois dias", e só o "Pedir outro dia" do
ponto da obra tinha sido feito).

- **Por que não abria:** não era o feriado, era o HOJE sem horário cadastrado.
  Sem escala (o dono ainda não tinha configurado), o dia de hoje sem batida é
  "em andamento", e a regra do ✏️ (`podeCorrigirDia`) só aceitava falta,
  incompleto e sem escala. Agora aceita também o dia em andamento e o de
  descanso/feriado, quando não há horário previsto — quem trabalhou digita.
- **Feriado não espera batida** (`espelho.py`): as `previstas` do feriado ficam
  vazias. Antes, com escala, o feriado vinha com 07:00…17:00 marcados como
  "faltou" — e o ajuste de vários dias pediria o feriado inteiro.
- **Vários dias num pedido só.** No Meu mês e na consulta, botão "✏️ Corrigir
  vários dias" (do dia / até o dia, no máximo 31); em Pedidos › Esqueci de bater,
  o campo "Até o dia"; no ponto da obra, o mesmo "Até o dia" no formulário. Um
  bloco por dia com os horários da escala já marcados; o dia que não cabe ajuste
  diz por quê (completo, justificado, mês fechado); folga e feriado vêm fechados
  ("trabalhou? toque para marcar"). "Repetir os horários do primeiro dia nos
  outros" serve a quem não tem escala. Vai tudo num pedido só, na mesma
  transação: um dia com problema, nada entra, e a frase começa pelo dia
  ("06/10: já existe batida às 07:00…").
- Servidor: `criar_ajuste_do_dia` limita por DIA (6 horários) e no máximo 31
  dias (`MAX_DIAS_DE_AJUSTE`); `/app/api/tablet/dia` aceita `ate` e devolve
  `dias` (cada um com `competencia_fechada`).
- Efeito na tela, de propósito: o ✏️ passa a aparecer também nos dias de
  descanso e feriados passados (o 🗓️ já aparecia).
- **A pergunta do dono — "o horário padrão vai funcionar quando eu
  configurar?":** sim. Com escala na pessoa (Pessoas › Trocar escala) ou a
  escala padrão (Configuração › Jornada e licenças), o ajuste abre com os
  horários dela marcados; o ensaio de hoje, com escala 7–11/12–17 e sexta até
  16h, trouxe exatamente isso. Sem escala, as linhas vêm em branco.

## 10/10/2026 (noite, segunda) — Fora da obra, nem entra na batida; e sair dela sem PIN

O dono, num ponto da obra fora da área: *"diz que tá fora da área da obra (…)
realmente. Mas quando eu coloco o CPF, ele vai para a tela e diz 'ponto não
registrado'. (…) Não era nem para entrar nessa tela de batida. Ele pede a senha
para sair da batida, não tem sentido, era só para voltar para a tela
anterior."* E: a tela inicial dizia "você entrou como ponto da obra · 📍 Teste do
ponto", como se o aparelho estivesse na obra — "a geolocalização não está
batendo".

- **Fora da obra, o "Bater ponto" fica desligado na tela inicial**, dizendo a
  obra mais perto e a distância ("fora da obra, a 3,3 km de AP-01"). Tocado,
  procura a localização de novo; dentro da cerca, liga e abre a batida. Sem
  localização, ou com as obras do aparelho sem coordenada, também desligado,
  com o motivo. A conta é a mesma da batida (`lugarDoAparelho`, no app.html):
  as duas telas não podem discordar.
- **A linha do alto diz ONDE o aparelho está agora** ("📍 dentro da obra X" /
  "📍 fora da obra (a N km de X)" / "📍 sem localização") — antes listava as
  obras do aparelho, o que parecia localização. E **"você entrou como"** é o
  papel da pessoa: "responsável por este aparelho" ou "administrativo da obra"
  (`papel_aqui` no `/app/api/inicio`), não "ponto da obra".
- **"‹ Início" na batida só volta** para a tela inicial, sem PIN. Desfaz a
  escolha minha da tarde (o PIN era para ninguém da fila chegar à consulta);
  o que segura agora é a saída sozinha em 3 minutos fora da batida. A rota
  `/app/api/confirmar-pin` ficou, sem uso na tela.
- **O responsável por um ponto da obra pede o PRÓPRIO no "Meu ponto"** (o dono:
  *"ela consegue pelo outro modo [os pedidos de todo mundo, no ponto da obra];
  é para conseguir aqui também — não fica sem sentido"*).
  `papeis.pede_pelo_proprio_celular` passou a incluir quem responde por um
  ponto da obra aprovado e dentro da validade. O ponto de EQUIPE continua
  pedindo pelos outros só com a marcação do cadastro (`_marcado_para_pedir`,
  decisão de 08/10) — ser responsável de um ponto da obra não estende isso.
  O aviso do alto do "Meu ponto" diz onde pedir, conforme a pessoa possa ou não.
- **A aba da tela em que a pessoa já está fica marcada e sem toque** (*"clico em
  Hoje, não tem batida, não acontece nada — a impressão que dá é que tem um
  bug"*): o "Hoje" de baixo é a aba da tela, não um botão do dia. Agora ela
  aparece selecionada (faixa azul em cima, fundo) e não reage ao toque.
- **Fora da obra, o aviso não cita a obra mais perto** (o dono, depois de
  publicado: *"às vezes a pessoa não tem nada a ver com aquela obra — está sendo
  sugerida uma obra só porque está mais ou menos perto"*). Diz só que a
  localização não confere com nenhuma obra cadastrada — no ponto da obra, no
  celular da pessoa e na recusa do servidor quando ninguém escolheu obra. Quando
  a PESSOA escolhe a obra (celular, batida fora da obra), a recusa continua
  dizendo a distância daquela obra: foi ela que a apontou.
- **Onde se libera o pedido pelo celular** (o dono: *"não vi na configuração do
  telefone para permitir ou não a inclusão de atestados"*): é por PESSOA, não
  por aparelho — ERP › Ponto › Pessoas › a pessoa › "Acesso no aplicativo" ›
  "Pedidos pelo próprio celular" (desde 08/10). O ponto da obra sempre recebe
  pedidos de quem bate nele (decisão de 06/10). Como desde hoje o responsável
  por um ponto da obra pede o próprio com ou sem a marcação, o cartão "Acesso no
  aplicativo" e a janela dizem isso (`responsavel_ponto_obra` no
  `/erp/api/ponto/pessoas/<id>`) — senão a marcação "Não" pareceria mentir.
- **A mesma marcação na janela do aparelho** (o dono: *"facilitaria se ficasse
  no cadastro do telefone, ou também, e uma coisa refletisse na outra"*). Em
  ERP › Ponto › Configuração › aparelhos, "Aprovar"/"Alterar" ganhou "Atestados
  e pedidos do responsável pelo próprio celular". Não é um dado novo: lê e grava
  a marcação da PESSOA responsável (`colaborador_config.pede_no_celular`), então
  mudar numa tela muda na outra. Só é enviada se foi mexida — trocar o
  responsável do aparelho não leva a marcação do anterior para o novo.

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
