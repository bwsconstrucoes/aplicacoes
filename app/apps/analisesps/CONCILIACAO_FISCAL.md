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
| Material Elétrico, Hidráulico, Pintura, Telhas, Ferragens, Ferramentas… | NF-e (Mercadoria) |
| Aluguéis e Condomínios | Contrato |
| Multas e Processos Trabalhistas | Rescisões (TRCT e Multa) |
| Cartórios, Crea, Taxas | Taxas Diversas |

Serve para **propor**, nunca para decidir sozinho.

E a tabela de dedutibilidade é do próprio dono, na aba de apoio da planilha:
são "Não Dedutível" apenas **Ausente, Nota Cancelada, Reanalisar, Emissão
Futura e Não Dedutível**; todo o resto é "Documentação OK". Duas opções do Pipefy não
estavam nessa tabela — **BeeVale e Férias ou PL** —, e o dono respondeu em
11/09: as duas são **dedutíveis**, assim como Rescisões.

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
