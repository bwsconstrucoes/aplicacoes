# HISTÓRICO — ERP · Obras e Arquivo

Memória **desta área**. O chat é descartável; isto não.

⚑ **ESTE ARQUIVO NASCEU EM 18/11/2026**, quando o dono dividiu o chat do ERP em
quatro. O pedido dele, com as palavras dele:

> *"Talvez assim separar mais: a parte de pessoal eu trato no chat, a parte de
> suprimento eu trato no outro chat, porque assim uma coisa que eu vejo numa
> área eu jogo lá, e a outra coisa que eu vejo na outra área eu jogo no outro
> chat. Eu acho que tem como evoluir mais rápido dessa forma."*

⚠️ **O PASSADO DESTA ÁREA NÃO ESTÁ AQUI — está em `HISTORICO.md`**, que até
18/11/2026 guardava o ERP inteiro (12 mil linhas). Ele **não foi quebrado**, de
propósito: repartir doze mil linhas de memória por adivinhação perderia
contexto, e perder memória é o pior estrago possível neste repositório. Então:

- **procurar o passado** → `HISTORICO.md`, buscando por "obra", "contrato", "diário", "arquivo", "documento", "agenda", "acompanhamento"
- **escrever o novo** → aqui

**Leitura desta área, além do `HISTORICO.md`:** `GESTAO_DOCUMENTOS.md`,
`ACOMPANHAMENTO.md`, `CRONOGRAMA.md`.

---

## Onde o trabalho está

Cadastro de obra (contrato, matrícula CNO, ART, OS), diário de obra,
o Arquivo (catálogo de documentos, validade, sigilo), agenda e
acompanhamento de processos.

---

## ⚑ PENDENTE AGORA — leia isto antes de qualquer coisa

**Falta publicar (24/11/2026):** o espelho da C. Diários — a aba corrigida,
treze campos novos e a fase. Está no ramo `claude/oi-vjvrn8`, testado, **sem
migração**, esperando o "pode" dele.

**⚠️ DEPOIS DE PUBLICAR, ELE PRECISA RODAR A IMPORTAÇÃO** em Importar ›
Cadastro das obras (C. Diários): apertar "Conferir sem gravar" — **não precisa
baixar nada, a leitura é online** — e, se bater, "Completar o cadastro".
**Nada muda sozinho.** Esperado: ~179 obras mudam de fase e **98 somem do
diário**.

Publicado e no ar: o filtro de fase no diário (19/11) e, de 24/11, a mudança
de fase como função de serviço (`core/cadastros/obras.mudar_fase`).

**Dito a ele e ainda não decidido:**

- **Ele vai enxugar as fases.** Palavras dele, em 19/11: *"depois eu vou dar
  até uma enxugada nessas fases."* Quando isso vier, mexe em `FASES_OBRA` e em
  `FASES_ENCERRADAS`, em `core/cadastros/obras.py`, e na lista repetida dentro
  do `erp_diario.html` (`FASES_FORA`) — os três juntos.
- **A mesma régua vale para outras telas?** O filtro de fase hoje é só do
  diário. A caixinha de obra do **pedido ao DP** (`portal.py`,
  `obras_para_escolher`) e a tela de **Obras** continuam mostrando tudo. Para
  o pedido ao DP isso parece certo (desligar alguém de obra que acabou é caso
  real), mas é decisão dele.
- **Ineficiência relatada, não consertada:** o painel "onde está faltando
  diário" faz **uma consulta ao banco por obra**, mais uma — com 80 obras são
  81 idas. O quadro do mês, ao lado, já resolve a mesma conta em uma consulta
  só. Esconder as concluídas cortou o número, mas a forma continua errada.

O que estava pendente antes da divisão dos chats está em `HISTORICO.md`, na
seção "PENDENTE AGORA".

⚠️ **Perguntar ao dono** se o que está escrito lá já aconteceu — o estado do
mundo muda entre uma sessão e outra, e ele é a única fonte disso.

---

## Decisões desta área

### 26/11/2026 (2) — No celular, o diário abre com os filtros à mostra

Ele tinha deixado a dúvida em aberto de manhã (*"na hora que abre, ele abre no
quadro do mês. Aí eu tenho que clicar em filtros para poder pesquisar para o
específico (…) eu não sei se essa seria a melhor tela"*) e decidiu à tarde:

> *"Quando abrir no celular, eu acho melhor o quadro do filtro."*

**O que mudou:** no telefone (até 720 px), a tela abre com a **gaveta de
filtros aberta** — e a primeira coisa à mão é *"Qual obra"*. Quem abre o diário
no celular quase sempre vem fazer **um dia de uma obra**, não olhar o panorama;
com a gaveta fechada, chegar na obra custava um toque escondido atrás de um
botão flutuante.

**O quadro do mês NÃO saiu de baixo.** Ele continua sendo a tela; a gaveta é uma
camada por cima, e tocar fora dela fecha e revela o panorama. **Trocar a porta
de entrada seria decisão maior, e ele não pediu isso** — ele pediu o filtro à
mão, não o quadro fora do caminho.

**Duas condições, e as duas têm teste:** só no celular (no computador a coluna
de filtros já está sempre à vista — a gaveta nem existe acima de 720 px) e só
quando **nenhuma obra está escolhida** (quem chegou com obra já sabia aonde ia).

**Decisão minha, dita para ele poder derrubar:** o campo *"Qual obra"* **não
recebe o cursor sozinho**. Receber abriria o teclado do celular por cima de
tudo, toda vez que a tela abre — inclusive para quem só queria olhar o quadro.
Se ele preferir o teclado já aberto, é uma linha.

**Arquivo mexido:** `templates/erp_diario.html`. A gaveta em si é do
`erp_base.html` e **não foi tocada** — a tela só liga a classe que já existe.
Sem migração.

---

### 26/11/2026 — A foto abre o dia; o quadro cabe no celular; a obra se clica

**Três coisas que ele viu usando, no telefone**, e as três viraram conserto:

> *"Eu não consigo adicionar uma foto. (…) Por que eu não adiciono a foto e
> automaticamente aquela foto já grava? (…) Se a pessoa quiser colocar uma foto
> na hora, no dia, e alimentar imediatamente, ela vai abrir o dia, adicionar a
> foto. E depois vai fazer o relato, concluir o diário. E da forma que tá, não
> tá assim."*
>
> *"Na hora que abre, ele abre no quadro do mês (…) não é boa a visualização no
> mês. Aliás, no celular. A tela ficou um pouquinho maior do que deveria."*
>
> *"A gente deveria poder clicar na obra do quadro do mês e já ir para a obra.
> Que tal? A gente já vai para o mês mais recente daquela obra. Facilita."*

#### 1. A FOTO ABRE O DIA — e o dia nasce **começado**

Anexar foto num dia sem diário era **recusado** ("grave o diário primeiro").
Ele está certo: quem está na obra **fotografa primeiro e escreve depois**.
Exigir o relato antes da foto obriga a pessoa a inventar um texto só para poder
guardar a imagem — ou a guardar a foto no celular e esquecer.

⚠️ **E AQUI ESTAVA O PERIGO, que não era óbvio:** se a foto criasse um diário
vazio, o dia deixaria de ser cobrado. O quadro mostraria preenchido, a
pendência sumiria, e **ninguém teria escrito o relato**. É exatamente o buraco
que a regra "dia com expediente precisa de relato" existe para tapar.

**A saída foi um estado novo, "COMEÇADO"** (`diario.estado_do_dia`): o dia
existe, aparece em **âmbar** no quadro e no calendário, e **continua contando**
como dia devendo diário — no quadro, no calendário e nas pendências por obra.
A foto adianta o trabalho; não o conclui. Escrever o relato é que fecha.

- Quem cria o dia assim é só o **anexar** (`diario_fotos._diario(comecar=True)`).
  Legendar e tirar foto continuam exigindo o dia — não se legenda o que não
  existe, e criar um dia para remover uma foto inexistente seria absurdo.
- **A recusa vem antes de criar:** arquivo vazio ou que não é imagem é barrado
  primeiro. Senão, um envio recusado deixaria um diário vazio para trás — que
  passaria a ser cobrado.
- O **Gravar continua exigindo relato**. `comecar_o_dia` não é atalho para o
  `salvar`: lá a pessoa diz "terminei", aqui diz "comecei".

#### 2. O QUADRO DO MÊS CABE NO CELULAR

A conta explica o que ele sentiu: a coluna da obra pedia 150–190 px e cada dia,
22 px. Um mês de 31 dias dava **~680 px numa tela de 368 px úteis** — apareciam
**7 dias**, e o resto só rolando uma faixa estreita. Quem abria no telefone via
a primeira semana e achava que era isso.

Agora, abaixo de 640 px: a coluna da obra mostra só o **código** (o nome
inteiro fica no toque e no `title`), o número de dias em falta por obra sai da
linha (ele já está somado no alto do cartão) e o dia vira uma barrinha de 8 px.
**O mês inteiro cabe.** Conferido no navegador a 400 px: 30 de 30 dias à vista.

#### 3. O NOME DA OBRA ABRE A OBRA

Clicar num quadradinho já levava ao DIA. Faltava o caminho de quem quer a
**obra**: agora o nome é botão e leva ao mês do **último diário** dela
(`ultimo_diario`, uma consulta a mais para o quadro inteiro — não uma por
obra). Obra sem diário nenhum fica no mês que está à vista.

No celular, esse botão é o alvo grande da linha: os quadradinhos de um mês
inteiro cabem, mas são finos demais para o dedo. A pessoa toca na obra e cai no
calendário, onde os dias têm tamanho de dedo.

⚠️ **Um defeito nasceu aqui e só apareceu no navegador:** o botão herdava o
espaçamento do `.link-btn`, ficava mais alto que a linha de 18 px do celular e
**transbordava por cima da linha de baixo** — a obra de cima ficava impossível
de clicar, porque o botão da obra seguinte recebia o toque. Pego dirigindo a
tela, não pelos testes.

#### O que ficou de fora, e por quê

Ele também perguntou: *"na hora que abre, ele abre no quadro do mês (…) eu não
sei se essa seria a melhor tela (…) aí eu tenho que clicar em filtros para
poder pesquisar para o específico."* **Não mexi na tela de abertura** — é
decisão dele qual deve ser, e trocar a porta de entrada do diário por conta
própria seria grande demais para uma dúvida em voz alta. Está perguntado na
resposta.

**Arquivos mexidos:** `core/diario.py`, `core/diario_fotos.py`,
`templates/erp_diario.html` e `static/erp.css` (**folha compartilhada** — as
regras novas são todas dentro de `.quadro-mes` e `.cal-dia`, mas quem mexer no
CSS em outro chat precisa saber). **Sem migração de banco.**

---

### 24/11/2026 — A foto do diário sabe onde foi tirada; e o clima saiu da gaveta

**Ele, cobrando o que tinha sido prometido e não feito:**

> *"Você disse que ia trazer baseado no registro do tempo, né? (…) E eu não vi
> em canto nenhum no diário. Isso aqui ficou feito?"*

Não tinha ficado. A coluna `clima_medido` nasceu na migração 092 em
08/11/2026, com a decisão dele (*"a chuva da estação meteorológica entra — eu
acho que é interessante sim colocar"*), e o próprio `HISTORICO.md` registrava:
*"falta ligar o serviço, que é dependência nova e ainda não entrou."* Ficou
**dezesseis dias** só como lugar vazio. Agora foi ligado.

#### ⚠️ O DEFEITO DE BASE DAS FOTOS, que ninguém tinha visto

Para caber no PDF, a foto era reduzida e **regravada** — e regravar **apaga o
EXIF**, que é onde o celular grava a hora e o GPS. Ou seja: **a informação
chegava e era destruída no envio**, desde 14/11. Não era "falta fazer": era
coisa sendo jogada fora todo dia.

**O conserto é a ORDEM**, e é o que o teste `test_o_local_sobrevive_ao_envio`
guarda: **ler → carimbar → guardar**. Invertida, a leitura acontece depois da
recompressão e não acha nada.

#### Os cinco itens que ele mandou fazer

1. **Ler o que a foto traz dentro** (hora e GPS), antes de qualquer redução.
2. **Pedir a posição ao aparelho** no envio, como REFORÇO.
3. **Mostrar o local na legenda**, na tela e no papel.
4. **Carimbar na imagem.**
5. **Ligar o clima** pela coordenada da obra.

#### ⚑ A FOTO SEMPRE GANHA DO APARELHO, e é a decisão mais importante daqui

O local honesto é **onde a foto foi tirada**, que está gravado dentro dela —
não onde está quem envia. Foto da galeria mandada três dias depois tem o local
certo dentro; a posição do celular naquele instante seria a do escritório.

Por isso a procedência viaja junto e aparece escrita:

- **"local gravado pela câmera"** (EXIF) — prova onde a foto foi tirada;
- **"local informado pelo aparelho no envio"** — prova onde estava quem
  enviou, que é outra coisa;
- **sem local** — e aí **não se carimba**.

⚠️ **Carimbo em foto sem local seria o sistema AFIRMANDO o que não sabe**, num
papel que a fiscalização assina. Foto sem localização entra igual, sem
carimbo, e a tela diz por quê.

#### O carimbo: véu, não tarja

A primeira maquete foi uma tarja preta embaixo. Ele corrigiu:

> *"O carimbo ele fica sobre a foto, mas ele não apaga os dados da foto. Ele
> fica como se fosse uma… não é uma marca d'água, porque a marca d'água
> normalmente ela é meio transparente, mas é como se fosse uma marca d'água
> viva sobre a imagem. Pelo menos é assim que as pessoas têm feito aqui
> quando eles tiram uma foto."*

Então: véu escuro de pouca opacidade e texto cheio com contorno por cima. O
que estava na foto continua visível. **O teste `test_o_carimbo_nao_apaga_a_foto`
prova isso objetivamente**: conta as cores na faixa do carimbo — tarja sólida
teria uma só.

#### O clima

Open-Meteo: gratuito, **sem cadastro nem chave** (chave seria mais um segredo
para guardar e renovar, contra a regra da casa). **Não é dependência nova de
código** — usa o `requests`, que já existe. ⚠️ **É host novo de saída**:
`archive-api.open-meteo.com`.

- **uma consulta por obra e por dia, UMA VEZ NA VIDA** — medido, fica
  guardado e não se pergunta de novo;
- **não mede o futuro** (previsão não é prova) nem **obra sem coordenada**
  (chutar pelo município poria a medição a quilômetros do canteiro);
- **não substitui o tempo informado pela obra** — são duas colunas, e quando
  divergem isso é informação, não erro;
- **nunca derruba a tela**: serviço fora do ar vira "não mediu".

⚑ **A coordenada da obra veio da C. Diários**, na importação publicada no
mesmo dia. A peça que faltava para o clima entrou por outro caminho.

**Na tela o clima agora se lê em português.** Antes a linha despejava o JSON
cru — e ninguém nunca viu, porque a coluna jamais foi preenchida.

**Migração 127.** Arquivo compartilhado mexido: `routes.py`.

---


### 24/11/2026 (fim do dia) — A planilha se lê ONLINE; download era trabalho à toa

**Ele, e é a terceira correção de rumo do mesmo dia:**

> *"Pra que eu vou fazer download de planilha se a planilha é online? Qual o
> sentido de eu ficar fazendo download para fazer importação? Por que tu não
> atualiza direto a planilha que está online?"*

**Razão nenhuma.** E o pior: **a emissão de NFS-e já lia ESTA MESMA planilha
ao vivo há meses** (`app/apps/emissaonf/cdiarios.py`), e a importação de
pessoas do DP também (`planilha_dp.carregar`, desde 16/11). Só esta pedia
arquivo. Terceiro lugar lendo a mesma planilha, e o único que obrigava
trabalho manual.

**Agora:** o botão "Conferir" lê a aba C. Diários direto do Google. Sem
baixar, sem subir. Uma leitura por importação — não uma por obra — com a
credencial que o monorepo já usa (`GOOGLE_CREDENTIALS_BASE64`), na cota
dividida com o painel, o Análise de SPs e a emissão de nota.

**O arquivo .xlsx ficou**, dobrado atrás de *"Google fora do ar? Mandar o
arquivo à mão"*, e a tela diz para que serve. Não é gosto: sem ele, uma pane
no Google ou uma credencial vencida deixariam o cadastro das obras sem
nenhum caminho. Escolhido um arquivo, ele manda — quem abriu aquela gaveta
fez por um motivo.

**Duas decisões que valem mais que o código:**

1. **A RÉGUA É UMA SÓ.** Entender o cabeçalho, casar coluna, ler data, partir
   coordenada e traduzir a fase mora em `_entender()`, e as duas fontes
   passam por ela. Há teste provando que online e arquivo devolvem
   **exatamente** a mesma coisa — duas leituras divergiriam no primeiro
   ajuste e ninguém perceberia até um campo entrar errado.
2. **O ERRO VOLTA POR EXTENSO PARA A TELA.** É a lição de 17/11/2026, que já
   custou uma rodada dele (*"Meu, não tá funcionando. Você precisa me mostrar
   o que tá acontecendo"*): credencial faltando, planilha renomeada, aba
   trocada — tudo isso viraria "não aconteceu nada" se ficasse só no log.
   Verificado na tela, sem credencial: aparece a frase inteira dizendo que a
   `GOOGLE_CREDENTIALS_BASE64` não está definida.

**O id da planilha** fica em `PLANILHA_ID`, com a variável
`ERP_CDIARIOS_SHEET_ID` para trocar sem mexer no código. É a mesma planilha
que a emissão de NFS-e abre.

⚠️ **Conferir e gravar são duas leituras.** Lendo online, a planilha pode
mudar entre uma e outra — por isso gravar relê, e o relatório que volta é o
do que foi gravado de verdade, não o da conferência.

---

### 24/11/2026 (tarde) — A C. Diários passa a ser espelhada, e a aba estava errada

**⚠️ O DEFEITO DE RAIZ, e ele é maior que a fase:** a aba da planilha se chama
**"C. Diários"**. A leitura aqui só aceitava **"Centro de Custo"** e, não
achando, **caía na PRIMEIRA aba do arquivo** — que no arquivo inteiro baixado
do Google é `ClientesOmie`. A importação lia a planilha errada, não casava
obra nenhuma e devolvia "nenhuma obra encontrada" sem ninguém entender por
quê. **É isto que deixou o cadastro das obras pela metade.**

A emissão de NFS-e lê a MESMA planilha e já tolerava os dois nomes há meses
(`app/apps/emissaonf/worker.py`, `ABA_CDIARIOS`). Dois lugares lendo a mesma
planilha com régua diferente, e o errado era o novo.

**Agora RECUSA em vez de adivinhar:** não achando a aba, a leitura para e diz
quais abas viu. Cair na primeira é ler a planilha errada calado.

#### O que a planilha tem e o ERP ignorava

Conferido na planilha de verdade (183 obras, 40 colunas, lida em 24/11/2026).
**Cinco campos estavam escritos no código como "nenhuma planilha traz"** —
responsável técnico, ART, ordem de serviço, início e término. Essa frase vinha
da planilha de DIÁRIOS (a de 1.427 linhas), onde vêm vazios mesmo, e nunca
tinha sido conferida contra a C. Diários. **São justamente os que fazem o
quadro de prazo do diário aparecer.**

Passaram a entrar: **fase**, endereço da obra, início, término, ordem de
serviço, responsável técnico, fiscalização, início e fim de vigência, ART,
órgão resumido, data base de reajuste e coordenadas. Sobraram só **CREA** e
**data da ordem de serviço** como digitados no cadastro.

#### A fase: os oito valores, contados

`Status da Obra` (coluna V), preenchida em 183 de 183:

| Na planilha | No ERP | Obras |
|---|---|---|
| Em Execução | EM_EXECUCAO | 80 |
| Concluída | CONCLUIDA | 50 |
| Acervo Técnico | ACERVO_TECNICO | 31 |
| Concluída com Dívida | CONCLUIDA_COM_DIVIDA | 14 |
| Distratada | DISTRATADA | 3 |
| Criação Centros | CRIACAO | 3 |
| Aguardando O.S | AGUARDANDO_OS | 1 |
| Especial | CONCLUIDA | 1 |

**"Especial" vira Concluída** — decisão dele no mesmo dia, depois de ver a
pergunta na tela: *"Especial tratar como obra Concluída tb, que não aparece
mais diário."* É 1 obra, e o que ele quer é que ela saia do diário.

⚠️ **A trava continua de pé para valor NOVO**: status que ninguém mapeou não
vira fase nenhuma — a obra fica com a que tem e o relatório pede a tradução.
Chutar mexeria no que aparece no diário.

⚑ **O histórico de fase guarda o texto da planilha** ("C. Diários: Especial").
Seis meses depois, *"por que esta obra está Concluída?"* se responde abrindo
o histórico dela.

⚠️ **Três fases do ERP nunca vêm daqui**: Paralisada, Recebimento provisório e
Recebimento definitivo não existem na planilha. Quem usar essas marca na mão.

⚑ **98 das 183 obras estão em fase de conclusão** (concluída, com dívida,
acervo técnico, distratada). É o que o filtro do diário de 19/11 vai esconder
assim que isto rodar.

#### Espelhar virou o padrão

> *"Os dados reais e atualizados estão lá (…) qualquer outro dado ele deve ser
> atualizado para o ERP, porque de repente tem alguma coisa que a gente mudou,
> porque estava mal escrito, porque precisou alterar o endereço (…) lá o
> trabalho manual já foi feito lá. Eu quero só replicar ele pro ERP. Porque
> senão o ERP vai nascer desatualizado."*

A caixinha "trocar o que já está preenchido" **nasce marcada** e se chama
**"Espelhar a planilha"**. Era o contrário: a regra 2 deste importador dizia
que o ERP vence a planilha, escrita quando se supunha que o ERP já era a
fonte. **O código da obra nunca é trocado** — é por ele que a obra é achada.

⚠️ **Isto se inverte quando o Pipefy sair.** O ERP passa a ser a fonte e esta
importação deixa de existir. É ferramenta de transição, com data para morrer.

#### Três decisões de implementação

1. **A FASE NÃO É UM UPDATE SOLTO.** Passa por `obras.mudar_fase`, que guarda
   histórico, trilha de auditoria e as datas de conclusão e recebimento.
   Escrita junto dos outros campos, perderia tudo isso em 183 obras de uma vez.
2. **Fase que já está certa não vira registro.** Rodar a sincronização duas
   vezes não pode encher o histórico de fase de toda obra com uma mudança que
   não houve.
3. **A fase tem bloco próprio no relatório**, com o rótulo em português. É ela
   que decide o que aparece no diário; misturada numa lista de campos, "179
   obras mudam de fase" passaria sem ninguém ver.

**Respondido por ele no mesmo dia:** "Especial" é Concluída.

---

### 24/11/2026 — A fase nunca veio nas importações; o caminho é sincronizar

**Ele, e a pergunta é o diagnóstico:** *"quando importei as obras pro sistema,
o sistema não copiou as informações de fase, e estão todas desatualizadas.
Essa importação deve ter sido da planilha C. Diários, como fazer pra
atualizar?"*

**Conferido: nenhuma das duas importações de obra traz fase.**

- o CSV de obras (`core/importadores/planilhas.py`) lê
  `codigo, nome, cno, municipio, uf, codigo_omie_depto` — e só;
- a C. Diários (`core/importadores/obras_cdiarios.py`) traz contrato, objeto,
  município, UF, CNO, cliente, CNPJ, valor, ISS e tributação. **A coluna da
  fase existe na planilha e o importador nunca olhou para ela.**

Toda obra nasceu no **padrão da coluna no banco, `CRIACAO`** — "Criação /
cadastro" — e lá ficou.

⚠️ **Isso também explica por que o filtro de 19/11 não escondeu nada:**
CRIACAO é uma das quatro fases "em andamento". Enquanto as fases não forem
acertadas, o diário continua mostrando todas as obras — os dois assuntos são
o mesmo.

#### ⚠️ UMA TELA FOI FEITA E DESFEITA NO MESMO DIA — e o motivo fica escrito

A primeira entrega foi um botão "Acertar as fases" na tela de Obras, para
escolher a fase de cada obra na mão e gravar em lote. **Ele desfez, em duas
cobranças:**

> *"Eu havia feito apenas uma pergunta."*

> *"Eu acho que a tela que você fez é desnecessária. Quando eu falo em
> sincronizar, isso só vai acontecer enquanto o ERP não está definitivo, uma
> vez que ainda estamos atualizando via pipe."*

E depois, fechando o desenho:

> *"Não é o Pipefy que entrega. O Pipefy sincroniza com a planilha C.
> Diários, e lá temos todos os dados das obras atualizados. Inclusive as
> fases."*

**Os dois erros, para não repetir:**

1. **Pergunta não é pedido.** Ele perguntou "como fazer pra atualizar?" e
   recebeu uma funcionalidade. A resposta certa era a conta — "a fase está na
   planilha, o importador não a lê, dá para ler" — e esperar.
2. **Trabalho na mão quando existe fonte.** Oitenta caixinhas para escolher à
   mão um dado que já está atualizado numa planilha, automaticamente, é a
   ineficiência óbvia que ele cobra desde 17/11. A pergunta que faltou:
   *de onde esse dado vem hoje?*

**O que FICOU da tela desfeita, e é bom que tenha ficado:** `mudar_fase` saiu
do `routes.py` para `core/cadastros/obras.py`, com teste. Quem trouxer a fase
de fora chama a mesma função, e o histórico de fase, a trilha de auditoria e
as datas automáticas saem iguais venha de onde vier.

**Dois cuidados achados ali que valem para a sincronização**, e os testes
guardam os dois:

- **regravar a fase que já está lá não faz nada.** Sem isso, cada rodada da
  sincronização escreveria no histórico de fases de TODA obra uma mudança que
  não houve — e esse histórico é documento;
- **data de conclusão não se reescreve.** Só é preenchida quando está vazia.
  Obra que acabou ano passado não pode receber a data de hoje porque a
  sincronização passou por ela. É o estrago mais silencioso dessa área.

#### O desenho combinado (ainda não construído)

A fase passa a vir da **C. Diários**, pelo importador que já existe. É
**ferramenta de transição, com data para morrer**: ele foi explícito que a
sincronização vale *"enquanto o ERP não está definitivo"*, porque a equipe
ainda move card no Pipefy e o Pipefy alimenta a planilha. Quando o Pipefy
sair, a fase passa a viver no ERP e esta leitura é desligada.

⚠️ **A fase é a ÚNICA coisa em que a planilha ganha do ERP.** Para todo o
resto, o importador não sobrescreve o que já está preenchido (regra 2 do
`obras_cdiarios.py`). Para a fase é o contrário, de propósito — senão a
segunda rodada não faz nada.

**Pendente:** o nome exato da coluna e os valores que ela usa. O mapa entre o
que a planilha escreve e as dez fases do ERP é onde isto erra em silêncio, e
é informação que só ele tem. Perguntado em 24/11/2026.

---

### 19/11/2026 — O diário esconde obra em fase de conclusão (e não proíbe)

**O defeito, e ele é dos que passam despercebidos por meses:** o diário dizia,
na própria tela, que obra concluída não aparecia. **Era mentira pela metade.**
O filtro era só `status <> 'ENCERRADA'` e **a fase era ignorada** — e ninguém
encerra o cadastro de uma obra no mesmo dia em que ela acaba na prática. Então
obra marcada "Concluída", "Recebimento definitivo" ou "Distratada" continuava
no quadro do mês, acumulando dias vermelhos que ninguém ia preencher.

**O que ele pediu, com as palavras dele:**

> *"As fases iniciais da obra, que são as fases que tem sentido você lançar
> diário (…) criação/cadastro, aguardando a ordem de serviço, em execução, ou
> paralisada. Daí em diante, as demais fases são todas fases associadas à
> conclusão da obra. (…) Não tem sentido eu fazer diário de uma obra que está
> concluída, (…) concluída com dívida, (…) aguardando recebimento provisório
> ou recebimento definitivo, (…) acervo técnico, ou que foi destratado."*

**E a forma, que é a parte importante:**

> *"Eu não digo nem que proíba a confecção do diário, mas que pelo menos a
> exibição não fique na tela inicial. (…) A tela inicial exibe só as obras
> ativas. As obras concluídas ficam ocultas. Aí eu altero o filtro e visualizo
> todas, se eu quiser. Porque senão fica aquela lista imensa de obras que
> deveriam estar concluídas."*

**O que foi feito:** a tela do diário ganhou o filtro **"Quais obras"**, com
duas opções — *Só as obras em andamento* (o padrão) e *Todas, inclusive as
concluídas*. Ele vale ao mesmo tempo para o quadro do mês, para a caixinha de
escolher obra, para a busca por digitação e para o painel de pendências.

**ESCONDE, NÃO PROÍBE, e isso é decisão registrada:** lançar diário em obra
concluída continua permitido. Quem tem diário atrasado de obra que já acabou
troca o filtro e chega nela. Há teste cobrando isso, para ninguém "melhorar"
a regra depois e virar trava sem ele pedir.

**Onde a régua mora:** `FASES_ENCERRADAS`, em `core/cadastros/obras.py` — a
mesma lista que o assistente já usava. ⚠️ Ela está **repetida** dentro do
`erp_diario.html` (`FASES_FORA`), porque a lista de obras que a tela já tem em
mãos traz a fase de cada uma, e buscar a régua no servidor seria uma ida de
rede só para isso. Os dois nomes são parecidos de propósito: **fase nova muda
nos dois lugares.** Há um teste que trava a composição da lista, então
acrescentar uma fase ao sistema obriga alguém a decidir de que lado ela fica.

**O que ficou de fora, de propósito:** a tela de Obras, o Arquivo, a Agenda,
os Contratos e a caixinha de obra do pedido ao DP continuam mostrando tudo.
São listas em que obra concluída ainda faz sentido.

**Não mexeu em arquivo compartilhado além do `routes.py`** (duas rotas do
diário).

---

## Incidentes

(vazio)
