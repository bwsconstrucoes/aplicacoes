# Conciliação fiscal — o que foi levantado, e o que falta decidir

**Estado: EM DISCUSSÃO. Nada foi construído.** Este arquivo existe para o
levantamento de 11/09/2026 não se perder se este chat acabar — ele custou
leitura da planilha de verdade, do script que roda nela hoje e do que o ERP já
tem. Quem retomar começa daqui, não do zero.

## O que o dono pediu

Uma tela nova no Análise de SPs — **Documentação Fiscal** — para fazer a
**conciliação das notas emitidas contra o CNPJ da BWS**. Nas palavras dele:
*"eu quero tentar automatizar um pouco e facilitar esse trabalho... da forma
mais automática, mais com segurança possível."*

O ciclo de hoje, na ordem: alguém lança a SP no Pipefy → exporta-se o relatório
do Pipefy e cola-se na aba **Lançamentos** → o **FSist** (serviço pago) baixa
as notas emitidas contra os CNPJs da BWS, e esse relatório vai para a aba
**Relatório FSIST** → **na mão**, alguém cruza os dois, acha a nota do
lançamento, e volta ao Pipefy para preencher cinco campos.

## O DEFEITO no script de análise de hoje

O dono disse: *"tinha de análise de dados ali mas ele está muito ruim, na
verdade seja dito."* Lendo o `gerarAnaliseDadosFiscais()`, há uma causa
concreta, e ela é de conceito.

A pontuação que escolhe a nota candidata (`scoreMatch_`) dá:

    +30  quando o CPF/CNPJ do credor bate com o DESTINATÁRIO da nota
     +8  quando bate com o EMITENTE da nota

**Está de cabeça para baixo.** O relatório FSIST é de notas emitidas CONTRA a
BWS: o **destinatário é sempre a BWS** (conferido: 78 das 110 linhas lidas são
"BWS CONSTRUCOES LTDA" e variações). Quem emitiu a nota é o **fornecedor** —
ou seja, o **credor do lançamento**.

Não é estatística, é estrutura: os dígitos 7 a 20 da chave de acesso são, pelo
layout da NF-e, **o CNPJ do emitente** — e eles batem exatamente com o CPF/CNPJ
do credor do lançamento. Conferido nos registros da planilha.

Resultado: o sinal mais forte que existe (o credor é o emitente) vale 8 pontos,
e 30 pontos são dados por bater com a própria BWS, o que quase nunca ajuda a
distinguir uma nota de outra.

> **O QUE NÃO SE SABE, e é honesto registrar:** quanto isso custa na prática
> NÃO foi medido. A tentativa de medir falhou porque o conector do Google
> **truncou as abas** — das 29 chaves dos lançamentos, só 2 estavam no pedaço
> do FSIST que deu para ler. Qualquer número de "taxa de acerto" tirado dali
> seria inventado. A medição de verdade só dá para fazer com as duas abas
> inteiras dentro do banco.

## Outras coisas notadas no script, que valem decisão

1. **A importação do Pipefy APAGA a aba Lançamentos inteira** a cada vez
   (`limparDestinoPipefy_`). Qualquer anotação feita ali à mão se perde.
2. **A importação do FSIST está bem feita e vale copiar**: deduplica por chave,
   atualiza só o status quando ele muda, e — o mais importante — **acha as
   colunas PELO NOME, com apelidos**, em vez de pela posição. É a mesma lição
   que custou caro em 10/09 no rateio ("Código Primário" virou "Obra").
3. **A análise é refeita do zero e jogada numa aba.** Não há memória do que já
   foi decidido, então o mesmo caso volta a aparecer todo dia.
4. **A janela de data é de ±180 dias**, o que praticamente não filtra nada.
5. **`deveAnalisarLancamento_` já tem a regra certa de escopo**: categorias
   como Seguros, Contrato, Fundo Fixo e Taxas ficam DE FORA da conciliação —
   elas não têm nota para casar. Isso se mantém.

## O que a base do Análise de SPs já tem (e a aba Lançamentos repete)

A base já traz, e se atualiza sozinha a cada 5 minutos: `id` (o card),
`credor`, `documento` (CPF/CNPJ), `valor`, `tipo_despesa`, `nf`, `anexo_link`,
`card_link`, `descricao`.

**Faltam quatro campos**, que hoje só existem na aba Lançamentos:
Documentação Fiscal, Chave de Acesso, Fase atual e Etiquetas.

## As regras que estão escondidas no histórico do dono

Tiradas da aba Lançamentos, não inventadas. Em **18 dos 19** tipos de despesa,
a categoria escolhida foi sempre a mesma:

| Tipo de Despesa (do card) | Documentação Fiscal |
|---|---|
| Veículos (Taxas, Impostos, Multas) | Seguros |
| Locação de Equipamentos · Água e Energia · Internet e Telefonia | Nota de Débito/Fatura |
| ~~Material Elétrico, Hidráulico, Pintura, Telhas, Ferragens, Ferramentas…~~ | ~~NF-e (Mercadoria)~~ **← DERRUBADA em 11/09, ver a correção abaixo** |
| Aluguéis e Condomínios | Contrato |
| Multas e Processos Trabalhistas | Rescisões (TRCT e Multa) |
| Cartórios, Crea, Taxas | Taxas Diversas |

Serve para **propor**, nunca para decidir sozinho — e a linha de mercadoria saiu de vez. **Leia a correção de 11/09 antes de usar esta tabela**: hoje só valem as despesas que NUNCA têm nota eletrônica.

E a tabela de dedutibilidade é do próprio dono, na aba de apoio da planilha:
são "Não Dedutível" apenas **Ausente, Nota Cancelada, Reanalisar, Emissão
Futura e Não Dedutível**; todo o resto é "Documentação OK". Duas opções do Pipefy não
estavam nessa tabela — **BeeVale e Férias ou PL** —, e o dono respondeu em
11/09: as duas são **dedutíveis**, assim como Rescisões.

## ONDE CADA INFORMAÇÃO MORA — e o que precisa ser atualizado em DOIS lugares

Pergunta do dono, em 11/09/2026: *"quando formos atualizar algum dado no
Pipefy, que seria um número de nota e a categoria — essa informação nem tem na
planilha SPsBD. O número da nota tem lá. A gente precisaria atualizar isso na
planilha também. Pelo menos o número de nota, porque os outros dados não têm na
planilha."*

**Conferido no código, não deduzido.** Ele está certo, e o levantamento é este:

| O que a conciliação decide | Está na SPsBD? | Está no card do Pipefy? | Onde escrever |
|---|---|---|---|
| **Nº da nota** | **SIM — coluna AA** (`nf`) | sim | **nos DOIS** |
| Documentação Fiscal (a categoria) | **não existe** | sim | só no card |
| Chave de acesso | **não existe** | sim | só no card |
| Gerou nota (Sim/Não) | **não existe** | sim | só no card |

Ou seja: **o número da nota é o único campo que vive nos dois lugares**, e é
exatamente por isso que ele é o único que pode ficar divergente. Os outros três
não têm onde divergir — a planilha não os conhece.

### A boa notícia: o caminho de volta para a planilha já existe e é o mesmo

Não é mecanismo novo. Toda alteração feita por aqui já percorre
**banco → fila → log → planilha** (`_gravar_alteracao` em `web.py`, `drenar_fila`
em `sincronizacao.py`): grava no banco na hora, enfileira a célula, registra
quem mexeu e qual era o valor anterior, e o processo separado escreve no Sheets
depois. Se a internet cair no meio, a célula continua na fila e sobe sozinha —
nada se perde. É assim que Status Pgt (coluna O) e Agendado (AB) já funcionam.

### A trava que existe hoje, e por que ela é boa

A coluna **AA (`Nº NF`) está marcada como SOMENTE LEITURA** (`EDITAVEIS`, em
`colunas.py`, tem só `status_pgt` e `agendado`). A rota de alteração recusa
qualquer outra coluna. Duas colunas escapam disso **por porta própria e de
propósito**: Validação (AH), que exige senha própria, e Análise (AL), escrita
pelo "Remover risco".

Esse é o desenho certo, e a conciliação fiscal deve segui-lo: **porta própria
para o Nº NF, não entrada na lista geral**. Se `nf` entrasse em `EDITAVEIS`,
qualquer operador passaria a poder reescrever o número da nota de qualquer SP
pela tela comum — e o número da nota é prova fiscal, não campo de trabalho.

### RESPONDIDO em 11/09: quem alimenta a SPsBD são SCRIPTS

O dono: *"em relação à planilha, quem alimenta a planilha são scripts."*

Isso resolve o desenho, e simplifica: **o card do Pipefy é a fonte; a planilha
é o destino.** Escrever o Nº da nota no card BASTA — o script leva o valor para
a coluna AA sozinho. Escrever nos dois lados criaria duas verdades para a mesma
informação, e o dia em que elas discordassem ninguém saberia qual vale.

**Portanto: a conciliação escreve NO CARD, e não toca na planilha.** A coluna
AA continua somente leitura, como está hoje.

**O efeito colateral, dito para não assustar:** entre a gravação no card e a
próxima rodada do script, a coluna Nº NF das telas de Solicitações e Lote ainda
mostra o número velho. A **tela de Documentação Fiscal não sofre disso**, porque
ela lê o registro paralelo (`sp_fiscal_analise`), que sabe o que foi decidido e
o que já foi escrito no card. Quem faz a conciliação vê o valor novo na hora.

## OS CAMPOS DO CARD — identificadores conferidos em 11/09/2026

Tirados da estrutura do pipe que o dono colou, conferidos um a um. Estão no
código em `pipefy.py`, com o UUID ao lado de cada um e um teste que os trava.

| Campo no card | Identificador | UUID |
|---|---|---|
| A despesa gerou emissão de Nota Fiscal? | `a_despesa_gerou_emiss_o_de_nota_fiscal` | `f2453ccf-…` |
| Nº da Nota Fiscal | `n_da_nota_fiscal` | `d19d97ad-…` |
| Documentação Fiscal | `documenta_o_fiscal` | `40c54379-…` |
| Chave de Acesso | `chave_de_acesso` | `fa6f8252-…` |
| Análise Dedutibilidade | `an_lise_dedutibilidade` | `969cf0da-…` |
| Etiquetas | `etiquetas` | `88ba0d09-…` |

**Por que isso está travado em teste.** Errar um identificador do Pipefy **não
dá erro**: a chamada é aceita e nada é gravado. Não haveria como perceber
olhando a tela do Análise de SPs — só abrindo o card e vendo que continua
vazio. O identificador muda se alguém renomear o campo na tela do Pipefy; o
UUID, não, e é por isso que ele fica anotado ao lado.

**As 22 opções de Documentação Fiscal batem exatamente** com a lista do pipe, na
mesma ordem — conferido por teste. Isso não é preciosismo: o Pipefy **recusa o
card inteiro** quando o texto não é uma das opções, então um acento diferente
não erraria uma SP, derrubaria a gravação do lote todo.

**Ainda não se sabe o TIPO de dois campos.** O JSON que o dono colou traz
identificador, rótulo e UUID, mas não o tipo. Para "Análise Dedutibilidade" —
onde ele quer o link da nota baixada — isso importa: se for campo de seleção e
não de texto, o link não cabe ali. Descobrir é uma consulta à API, e fica para
quando a gravação for construída.

## O PLANO DAS NOTAS, fechado com o dono em 12/09/2026

**Nem o FSist nem a Receita guardam o passado.** O dono confirmou o que a
investigação apontava:

> *"O FSist também não traz o passado não. O passado é o que eu tenho, que eu já
> baixei de relatório lá. O relatório mais antigo que eu tenho a gente vai
> importar pra dentro do Análise de SPs, e deixar lá dentro; e a partir de então
> você vai começar a fazer o download."*

**Então o desenho é este, e ele tem duas metades:**

| | De onde vem | Quando |
|---|---|---|
| **O passado** | os relatórios que o dono já baixou do FSist, colados na aba | uma vez, e fica |
| **Daqui para a frente** | a Receita, pela chave, com o certificado | sozinho |

### O que isso exige da importação, e por que virou teste

A aba do FSist é uma **janela** que ele troca a cada relatório; a tabela
`notas_fiscais` é o **arquivo**, e ela só cresce. Se a importação apagasse o que
não está no relatório do dia, o histórico dele **se perderia na primeira
colagem** — e é histórico que não dá para recuperar de lugar nenhum.

**Conferido e travado em teste** (12/09/2026): colar o relatório de janeiro e
depois o de fevereiro na mesma aba deixa as duas levas guardadas. E há um teste
varrendo o módulo inteiro atrás de qualquer `DELETE` nessa tabela — a garantia
não pode depender de alguém lembrar.

> **Para o dono, na prática:** cole o relatório mais antigo, mande atualizar as
> planilhas de apoio, cole o seguinte, mande de novo. Cada leva entra e fica. A
> tela de Configurações diz quantas entraram, quantas mudaram e quantas já
> tinha.

### Nota de serviço está FORA, e por decisão dele

*"Nota de serviço eu sei que não dá pra baixar direto da Receita pelo município,
e não é o que eu estou buscando. Pelo menos por enquanto."* A NFS-e é municipal
e não tem serviço nacional. Continua chegando pelo anexo do card, que é o
caminho que a IA já cobre.

## O DOWNLOAD AUTÔNOMO DAS NOTAS — o que falta, e o que trava

Pedido do dono em 11/09/2026: *"eu quero que sejam baixadas as notas também, de
forma autônoma. Eu não quero ter trabalho nenhum em ter que baixar no FSist."*

**O que já dá para fazer sem nada novo**, e já está feito: quando o anexo existe
no card, a IA lê o documento e tira dele a chave de acesso e o tipo. Isso cobre
o caso em que quem lançou já anexou alguma coisa.

**O que falta é buscar a nota QUANDO SÓ SE TEM A CHAVE.** E aqui há um bloqueio
real, que não se resolve programando:

- O portal da Receita exige **captcha** na consulta pública por chave. Não há
  como automatizar isso, e tentar seria construir uma coisa que quebra no
  primeiro dia.
- O caminho que funciona é o **certificado digital A1 da empresa** — que é
  justamente o que o dono levantou: *"a gente pode incluir os certificados
  digitais na análise de SPs e nem precisar mais do relatório do FSist."*
- Com o A1 instalado, o XML da nota é baixado direto do webservice da SEFAZ
  pela chave, e o `leitor.py` do ERP **já sabe ler esse XML por parser exato,
  sem IA nenhuma e sem custo**.

**O que isso muda quando entrar:** o FSist deixa de ser necessário para o
download (continua útil como fonte de descoberta — ele diz QUE a nota existe), a
leitura vira exata em vez de interpretada, e a IA sobra só para o que não é nota
eletrônica.

**O que é preciso do dono para destravar**, e é a parte que não é código:

1. O arquivo do **certificado A1** da BWS e a senha dele.
2. A decisão de onde ele fica guardado — é credencial sensível, e a regra da
   casa manda usar variável de ambiente no Render, não arquivo no repositório.
3. O aviso de quando ele vence: certificado A1 vale um ano, e no dia em que
   vencer o download para de funcionar em silêncio se ninguém tiver previsto.

**Enquanto isso não vier, o que fica:** o link do documento no campo "Análise
Dedutibilidade" do card — que o dono pediu — só pode apontar para o anexo que já
existe, e não para uma nota baixada da Receita. Está anotado para não se perder.

## A LEITURA DOS ANEXOS POR IA — o degrau seguinte, e AINDA NÃO EXISTE

Pergunta do dono, em 11/09/2026: *"está entrando aí a análise dos anexos?
quando a gente não conseguir cruzar de forma fácil os dados?"*

**Resposta honesta: não, ainda não.** O que está construído hoje cruza só
TEXTO — credor, CNPJ, valor, número da nota, data. Quando esse cruzamento não
fecha, a SP cai em "procurei e não achei" e para ali. O anexo não é aberto.

**E é exatamente aí que a IA entra**, porque é aí que o cruzamento textual
acabou. A ordem importa e é esta:

1. **Primeiro o cruzamento textual**, que é de graça, instantâneo e resolve a
   maioria. Mandar todo anexo para a IA seria pagar caro para responder o que
   já se sabia.
2. **Só o que sobrar** vai para a leitura do anexo — e o volume disso é o que
   decide o custo. Hoje esse número não é conhecido; ele aparece assim que a
   tela rodar uma vez contra a base inteira.
3. **A IA lê e PROPÕE**, nunca decide. Uma nota lida errado de um PDF torto é
   dedução indevida com cara de decisão tomada, e é o erro que este arquivo
   inteiro existe para não cometer.

**O que o dono quer que ela responda** (das mensagens de 11/09): se o anexo é
mesmo uma nota fiscal; qual a chave de acesso quando ela não veio no relatório
do FSist; e, quando não for nota, que documento é — para escolher a categoria
certa entre as 22.

**A decisão que está tomada:** a IA está no escopo, e não foi adiada. O que
falta é ela ser construída.

**O que ela custa, e é decisão do dono:** é dependência nova e é cobrada por
documento lido. Vale a pena medir o volume da fila antes de ligar — a conta
muda muito se forem 50 anexos por mês ou 5.000.

**Um atalho que pode dispensar boa parte disso**, e que o dono levantou: se os
certificados digitais da empresa entrarem no Análise de SPs, as notas podem ser
baixadas direto da Receita, com a chave, sem IA e sem FSist. Aí a IA sobraria
só para o que não é nota eletrônica. Não foi verificado ainda se isso é viável.

## A CORREÇÃO DE 11/09 QUE MUDOU O MIOLO: a nota é o balizador

Foi proposto ao dono apontar como divergência o caso "tipo de despesa é
Material Elétrico mas está classificado como Não Dedutível". **Ele recusou, e
com razão:**

> *"Categoria de despesa não vai ser regra para dedutibilidade ou não, porque
> você pode comprar um material elétrico sem nota fiscal. Então nesse caso vai
> ser não dedutível. O fato de ter a nota fiscal é que vai ser o balizador.
> A simples divergência de material elétrico nem adianta mostrar."*

**O que decide é a EXISTÊNCIA DA NOTA, não o tipo de despesa.** A regra tirada
do histórico dele continua útil, mas num papel bem menor do que se pensava.

### E a categoria não precisa ser adivinhada: ela está DENTRO da chave

Os dígitos 21 e 22 da chave de acesso são o **modelo do documento**, por
definição da Receita. Conferido nas chaves reais da planilha do dono:

| Chave | Modelo | Categoria |
|---|---|---|
| `...0724`**`55`**`2070...` | 55 | NF-e (Mercadoria) |
| `...0152`**`55`**`0100...` | 55 | NF-e (Mercadoria) |
| `...1889`**`57`**`0010...` | 57 | CT-e (Frete) |

Nos dois primeiros, é exatamente o que o dono classificou à mão. **Achada a
nota, a categoria é CERTEZA, não sugestão.**

### O papel que sobra para o tipo de despesa

Só quando **não há nota**, e só para as despesas que **nunca têm nota
eletrônica**: aluguel é Contrato, veículo é Seguros, água e energia e locação
são Nota de Débito/Fatura, cartório é Taxas Diversas. **Fora dessas, sem nota
é Ausente ou Não Dedutível** — e sugerir "NF-e" por ser material seria
justamente o erro que o dono apontou.

### Um achado que nasceu escrevendo os testes

Quando o card **tem chave** mas a nota **não veio no relatório do FSist**, ainda
dá para conferir alguma coisa **sem o relatório do FSist**: o CNPJ de quem
emitiu está dentro da própria chave (dígitos 7 a 20). Se ele não é o do credor
da SP, a chave veio de outro lançamento — a troca de anexo detectada sem
depender de achar a nota certa.

Quando o CNPJ bate, o caso é o normal: o relatório cobre um período, e nota
antiga fora dele não é erro de ninguém. A tela diz isso e não acusa.

### O que o sistema aponta, então

Só o que a nota sustenta:

| Situação | Por que é achado |
|---|---|
| Está como Não Dedutível / Ausente / Aguardando Nota **e a nota foi achada** | **É a correção que vale.** A nota é a prova |
| Está como Emissão Futura e a nota **apareceu agora** | Era antecipado; a nota saiu |
| Está como NF-e mas **não há chave nem nota achada** | Classificado sem documento |
| A chave do card **não bate** com a SP (valor ou emitente) | Provável troca de nota entre lançamentos |
| **A mesma nota em duas SPs** | Ou é parcelamento, ou é erro |
| **Nota no FSist sem lançamento** | A visão que fecha com a contabilidade |
| **Nota cancelada** em SP paga | O mais grave |

**NÃO entra na lista:** tipo de despesa discordando da categoria. Foi
explicitamente recusado pelo dono.

### E ele decidiu: PROPOR, não só apontar

Onde a nota foi achada, o sistema chega com a correção pronta e a pessoa
aprova em bloco. Com duas salvaguardas ditas na frente:

1. **Duas pilhas separadas.** O que não tem dúvida vem marcado para aprovação
   em bloco; o que tem dúvida **não vem marcado** e é decidido um a um.
2. **O motivo escrito em cada linha**, para discordar dar tão pouco trabalho
   quanto concordar.

> **O risco de propor, dito na frente:** se vinte e oito de trinta estão
> sempre certas, na terceira semana ninguém confere mais — é o mesmo olho
> cansado, só que mais rápido. As duas pilhas existem por causa disso.

## O ERP, e por que isto não pode ser feito duas vezes

O ERP **já está construindo o cruzamento de notas** (migração 044, ditada pelo
dono em 07/09, escrita em `erp/NOTAS_FISCAIS.md`): tabela de notas, ligação com
pedido e título, e a conferência humana (PENDENTE / CASADA / SEM_PAR /
IGNORADA).

A diferença real entre os dois:

- o **ERP** cruza nota ↔ **pedido e título** (o mundo do Omie);
- o que se pede aqui é nota ↔ **card do Pipefy**, e **escrever no card** — e é
  este módulo que já sabe escrever no Pipefy.

**Decisão pendente do dono:** onde mora o cadastro das notas. Dois cadastros
divergindo sobre as mesmas notas é o pior resultado possível.

## O certificado digital: a resposta é NÃO

O dono perguntou se dá para largar o FSist usando os certificados digitais que
o ERP passou a guardar. **Não, ainda não.** O certificado está lá (migração
053), mas para **emitir** nota de serviço e avisar do vencimento. O ERP **não
baixa nota da Receita** — ele importa os XMLs que o FSist já baixou, e isso foi
decisão registrada: puxar direto exige, além do certificado, **controle de
sequência (NSU) e manifestação do destinatário**.

**Baixar a nota só com a chave de acesso não existe** por caminho confiável e
legal. O FSist continua necessário.

## Decisões TOMADAS pelo dono em 11/09/2026

**1. Não existe cadastro de notas, e não vai existir aqui.** Resposta dele:
*"não existe. A gente usa o Pipefy para armazenar essas notas, e grava a
informação chave de acesso."* O **card do Pipefy continua sendo o lugar da
nota**. O que este módulo guarda é um **registro paralelo do que ELE mandou
para o card** — para cada SP: a documentação fiscal, a chave de acesso, e
quando/quem decidiu.

Isso resolve sozinho o risco de dois cadastros divergirem: não há segundo
cadastro. Há um diário do que foi escrito.

**2. BeeVale, Férias ou PL e Rescisões são DEDUTÍVEIS.** Fecha a tabela de
dedutibilidade, que agora cobre as 22 opções do Pipefy.

**3. "Emissão Futura" não é um fim de linha — é uma FILA.** Nas palavras dele:
*"na maioria das vezes são situações que a gente está pagando antecipado, não
tem a nota emitida e vai ser emitida posteriormente. Então vai ter que fazer à
medida que as informações do FSist vão sendo baixadas. São candidatos a esse
cruzamento futuro."*

Ou seja: o que fica como Emissão Futura **volta a ser conciliado toda vez que
chega relatório novo**, sozinho. É o oposto de arquivar.

**4. A base que já existe é a fonte, e o que falta grava-se AQUI.** A ideia é
dele, e é melhor do que a pergunta que foi feita:

> *"você vai utilizar a base de informações, e gravar em paralelo tudo que a
> gente enviar pro card. Tem que ver algum canto para gravar a informação de
> que aquela solicitação tem a dedutibilidade assim e a chave de acesso assim.
> E fazer dentro do Análise de SPs esse vínculo. Sem necessidade de baixar
> relatório, e mantendo a base de dados com o SPsBD da mesma forma, sem
> precisar adicionar nela."*

Três consequências, e as três são boas: a planilha SPsBD **não muda**; o
relatório do Pipefy **deixa de ser baixado toda vez**; e o módulo passa a saber
o que escreveu, sem perguntar a ninguém.

**5. A IA entra AGORA, não numa etapa futura.** *"Senão o trabalho não
funciona, tem que ser completo."* — decisão dele, com o custo e a dependência
ditos na frente.

## Requisito novo, dado junto com as respostas

A tela precisa ter **a mesma força de filtro e de números das Solicitações**:
*"a gente precisa também visualizar essa informação do que já foi e do que não
foi, fazer as filtragens, o que está resolvido, o que é dedutível, o que não
é, por que está pendente."* Não é uma lista de pendências — é uma tela de
gestão fiscal.

## O que o dono explicou em 11/09 sobre POR QUE isto existe

*"Eu quero minimizar a interação do humano. Primeiro que é muito trabalhoso; o
humano às vezes esquece de visualizar... ele erra na categorização, em coisas
até meio óbvias, de regras que a gente já definiu. É muito falho o olho humano.
E se eu colocar uma pessoa mais cara e que tenha mais capacidade, isso vai
custar muito tempo — e nós não temos esse tempo."*

**O alvo não é uma lista de pendências: é entregar a análise pronta**, "de mão
beijada", para quem só vai executar a atualização dos cards.

### Os quatro erros humanos que ele nomeou, e que o sistema tem de pegar

1. **Categoria errada em caso óbvio** — contra a regra que ele mesmo definiu.
   O sistema conhece a regra (a tabela por tipo de despesa) e pode apontar
   quando a categoria gravada no card discorda dela.
2. **"Não dedutível" posto cedo demais.** *"Colocado algo não dedutível de uma
   coisa que não foi localizada naquele momento, mas que depois ela surge."* Ou
   seja: **o que foi marcado como não dedutível tem de ser reavaliado** a cada
   relatório novo — a nota pode ter aparecido depois.
3. **A mesma nota em dois lançamentos** — o mesmo registro financeiro
   recebendo a nota que é de outro.
4. **Notas trocadas entre si** — a nota do lançamento A anexada no B e
   vice-versa, dentro dos anexos. *"A pessoa que analisa pode não perceber."*

E a contrapartida, que ele também disse: **o que está marcado como certo
também pode estar errado.** A reanálise vale nos dois sentidos.

### AS DUAS VISÕES — e a segunda é a que fecha com a contabilidade

Repetido duas vezes por ele, e é requisito, não enfeite:

- **do lançamento para a nota** — a que já está construída;
- **da NOTA para o lançamento** — *"a gente precisa também identificar as notas
  para poder associar ao registro financeiro, porque a gente precisa passar
  para a contabilidade essas informações... tentar zerar. Afinal, se tem uma
  nota emitida, tem uma despesa para estar associada."*

**"Zerar" é o objetivo real:** toda nota emitida contra o CNPJ da BWS tem de
estar ligada a uma despesa. Nota sem par é problema fiscal, e hoje ninguém a
enxerga.

### O que MUDOU de prioridade

- **Atualizar o card continua valendo, e em LOTE.** *"Tem a questão de
  requisições de API; a gente monta um lote e atualiza tudo de uma vez, então
  não teria problema."*
- **Anexar o PDF da nota ao card CAIU de prioridade.** *"Tendo a chave de
  acesso, a gente baixa a qualquer momento a nota se precisar. E a
  probabilidade de precisar é pequena."* — o Drive sai da frente da fila.
- **O volume é pequeno e recente:** a BWS entrou no lucro real este ano, então
  o histórico a reanalisar não é antigo.

## O que ainda precisa de resposta

- **O histórico já preenchido no Pipefy** (39 dos 116 lançamentos já têm chave)
  só existe no relatório do Pipefy. Para o registro paralelo nascer sabendo
  disso, é preciso importar esse relatório **UMA VEZ**. Depois nunca mais.

## O caminho proposto, em três etapas

1. **Enxergar e propor.** As duas importações passam a alimentar o banco (a do
   FSIST copiando o acerto de casar coluna pelo NOME). A tela mostra a
   conciliação com a pontuação **corrigida**, propõe chave e categoria, e
   guarda o que já foi decidido. **Nada é escrito no Pipefy.**
2. **Escrever no card**, só o que foi confirmado.
3. **A nota no Drive e a IA**, para o que sobrar.
