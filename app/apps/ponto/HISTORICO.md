# Ponto eletrônico — HISTÓRICO

Memória da área. Escrito para quem nunca viu a conversa. Ler antes de mexer,
junto com o `README.md` e o `PLANO.md`.

## Pendente AGORA

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
1c. **Coordenadas das obras no ERP**: a cerca agora BLOQUEIA e a obra é
   detectada pela localização — obra ativa sem latitude/longitude no cadastro
   do ERP não detecta (vai para conferência). Conferir em Ponto › Configuração
   › Cerca das obras.
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

**Pendente com o dono:** apertar "Atualizar cadastro" na Análise de SPs antes de
usar, e depois "Cadastrar no ERP os que faltam" no ponto.

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
