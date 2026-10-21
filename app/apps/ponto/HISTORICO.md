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

0a. **INCIDENTE 21/10/2026, ~17h — o aplicativo do ponto (e o Render inteiro) parou de carregar na hora da
   saída.** O dono reiniciou o serviço. As métricas do Render que ele trouxe: **memória 30 %, CPU 20 %, ~350
   requisições no pico — NÃO foi excesso de uso**: o serviço estava parado, ESPERANDO. Os registros mostravam
   respostas em rajadas a cada ~45 s e nenhum erro. **Causa mais provável:** o pedido de código de acesso
   (`/ponto/app/api/codigo`, "Primeiro acesso ou esqueci o PIN") mandava o WhatsApp DENTRO da requisição, e o
   `notificador` espera o Z-API até **60 s, 3 tentativas** (mais de 3 minutos no pior caso), com a requisição
   e a conexão do banco presas. O WhatsApp vinha falhando; na saída, gente pedindo o código (e pedindo de
   novo, porque não chegava — o registro mostrou o teto de códigos por hora estourado) prendia as **4
   linhas** do serviço e as conexões do banco (10 para o Render inteiro) — e todas as áreas pararam juntas.
   **Conserto (no ramo):** o código é gravado e a resposta sai na hora; o envio vai para uma fila em segundo
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
