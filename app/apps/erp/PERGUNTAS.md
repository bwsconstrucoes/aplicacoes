# Perguntas que o assistente precisa saber responder

> ⚠️ **ESTA LISTA É UM NORTE, NÃO UM LIMITE.** O dono corrigiu o rumo em
> 11/09/2026: *"o assistant não pode ficar somente focado nessas perguntas,
> né? Isso é só um norte"*. Desde então a tela tem um **campo de escrever**, e
> o que ela não entende é **guardado** — é essa lista de perguntas sem resposta
> que decide o que entra aqui em seguida. O catálogo é o chão, não o teto.
>
> **Para que serve este arquivo.** O dono pediu em 10/09/2026: *"a cada nova
> funcionalidade que nós temos, você já gera uma lista de possíveis perguntas,
> coisas mais óbvias, (…) pra que a gente minimize a possibilidade de alguma
> falha."*
>
> A razão é a do desenho do assistente: pergunta PREVISTA é respondida por
> código escrito e testado — exata, instantânea e sem custar IA. Pergunta
> imprevista cai numa consulta inventada na hora, que acerta quase sempre e
> **erra em silêncio** no resto. Cada linha deste arquivo é uma pergunta que
> deixa de correr esse risco.
>
> **REGRA QUE PASSA A VALER:** funcionalidade nova só está pronta quando as
> perguntas que ela torna possíveis entram aqui. Está escrito no `CLAUDE.md`.

---

## 1. Antes das perguntas: as PALAVRAS que precisam de uma definição só

Esta é a parte mais importante do arquivo, e a que mais evita número errado.
Quase toda pergunta do dono usa uma palavra do dia a dia que, nos dados, tem
mais de um significado possível. Se cada pergunta escolher um significado
diferente, dois relatórios sobre a mesma coisa vão discordar — e ninguém vai
saber qual está certo.

Cada palavra abaixo precisa de **uma definição escolhida e escrita**. Enquanto
não estiver decidida, o assistente **pergunta de volta** em vez de escolher.

| A palavra | Os significados possíveis | Decisão |
|---|---|---|
| **"a pagar"** | por vencimento da parcela, ou por competência do título? Inclui BLOQUEADO? Inclui o que ainda não foi aprovado? | ⬜ a decidir |
| **"custo da obra"** | regime de COMPETÊNCIA (quando aconteceu) ou de CAIXA (quando saiu o dinheiro)? | ⬜ a decidir |
| **"obra em andamento"** | pela situação no cadastro, pela data de término, ou por ter tido movimento nos últimos N dias? | ⬜ a decidir |
| **"quanto falta receber"** | são QUATRO leituras, todas legítimas — ver a régua abaixo | ✔ **decidido: o assistente PERGUNTA de volta** |
| **"resultado da obra"** | inclui rateio da administração? inclui os tributos? | ⬜ a decidir |
| **"gastei com fulano"** | pelo título lançado ou pelo pagamento feito? | ⬜ a decidir |
| **"este mês"** | mês da competência, do vencimento ou do pagamento? | ⬜ a decidir |

### A regra geral, decidida pelo dono em 10/09/2026

Diante de uma palavra ambígua, o assistente **não escolhe: pergunta de volta**.
Palavras dele: *"talvez valesse a pena questionar, se não tivesse sido bem
específica"*. Ou seja, a pergunta vaga é devolvida com as opções; a pergunta já
específica ("quanto falta receber do que já foi faturado") é respondida direto,
sem enrolação.

### A régua do recebimento — o caso que ensinou a regra

O dono desfez o "quanto falta receber" em quatro leituras, e todas as quatro
existem de verdade. Elas são etapas de uma mesma esteira:

```
CONTRATO (+aditivos)  →  MEDIDO  →  FATURADO (nota emitida)  →  RECEBIDO
```

| A pergunta, do jeito dele | A conta |
|---|---|
| "quanto falta receber do contrato inteiro, tendo sido medido ou não" | vigente − recebido |
| "quanto falta receber do que já está medido" | medido − recebido |
| "quanto falta receber do que já foi emitido nota" | faturado − recebido |
| "do que está medido, com nota ou sem nota" | medido sem nota, e faturado sem receber, separados |

✔ **FEITO em 10/09/2026.** As duas subtrações que faltavam
(`vigente − recebido` e `medido − recebido`) entraram no quadro do contrato, e
a pergunta **Quanto falta receber?** mostra a régua inteira, contrato a
contrato. Ela reusa o `quadro` da tela em vez de somar de novo — é o que
impede o número da pergunta e o da tela divergirem, e há teste exigindo que os
dois batam campo a campo.

**Como o assistente responde:** mostra **a régua inteira de uma vez**, com as
quatro linhas, em vez de um número solto. Assim a leitura que o dono queria já
está na tela, e ele não precisa ter acertado a pergunta. Só pergunta de volta
quando ele pedir explicitamente UM número.

Esse formato vale para toda pergunta com mais de uma leitura legítima:
**mostrar as leituras juntas costuma ser melhor que perguntar** — perguntar
fica para quando as opções mudarem o trabalho, não só o número.

**Três definições já estão fechadas** e o assistente pode usar sem perguntar:

- **Conta REDUTORA abate, não soma** (devolução, estorno, reembolso — grupo
  3.5). Todo total de custo já sai líquido.
- **O grupo 8 (compra de bens) É custo** desde 10/09/2026, e entra rateado na
  obra que o bem serve.
- **Fora do escopo responde "não encontrado"**, nunca "sem permissão" — vale
  também para o assistente. Dizer "sem permissão" sobre um número que existe já
  entrega que ele existe.

---

## 2. As perguntas, por área

Marcação: ⚠️ = a pergunta depende de uma palavra ainda sem definição, então o
assistente tem de confirmar antes de responder. 🔒 = a resposta muda conforme
quem pergunta (escopo por obra ou por autoria).

### Financeiro — títulos e pagamentos

✅ = **já responde**, em Financeiro › **Perguntar**. São funções escritas e
testadas, não consulta inventada na hora: todas passam pelo mesmo escopo por
obra e por autoria das telas, e cada resposta mostra de onde veio.

- ✅ **Como está o caixa dos próximos dias?** (vencido, hoje e os próximos 7,
  nas três faixas de uma vez) 🔒
- ✅ **O que tem a pagar num período?** — com obra opcional. Conta pelo
  VENCIMENTO da parcela, e a resposta diz isso 🔒
- ✅ **O que está vencido e não foi pago?** — com os dias de atraso 🔒
- ✅ **O que está parado esperando decisão, e de quem é a vez?** 🔒
- ✅ **Quais títulos estão sem documento anexado?** 🔒
- Quanto vou pagar para o fornecedor Y este mês? ⚠️
- Quanto já paguei este mês? E no mês passado?
- Quais títulos foram pagos duas vezes, ou têm risco de duplicidade?
- Quais títulos estão marcados como indedutíveis, e quanto somam?
- Qual o total por conta do plano, no período? ⚠️
- Quais lançamentos caíram em "Outros materiais" ou "Outras despesas"?
  (a pergunta que impede o plano de apodrecer)
- Quanto esta obra gastou em cimento este ano? 🔒
- Qual foi o maior gasto da obra X no mês? 🔒

### Obras, contratos e medições

- Quais obras estão em andamento? ⚠️
- Qual o resultado da obra X hoje? ⚠️ 🔒
- Quanto já foi medido na obra X, e quanto falta do contrato? 🔒 ⚠️
- Quais medições estão aprovadas e ainda não foram faturadas?
  (candidata a virar aviso proativo, não pergunta)
- ✅ **Quanto falta receber?** — responde com a RÉGUA inteira: do contrato, do
  medido e do faturado, lado a lado (§1)
- ✅ **O que já foi medido e ainda não virou nota?** — o serviço foi feito, o
  custo já saiu, e a cobrança nem começou
- ✅ **O que já tem nota emitida e ainda não entrou?**
- Quais notas emitidas ainda não foram recebidas, e há quantos dias?
- Quanto de reajuste a obra X tem a receber? Quais medições entram na conta?
- Quais contratos vencem nos próximos 60 dias?
- ✅ **Quais obras não emitem nota hoje por falta de cadastro?** — confere os
  MESMOS quatro campos que a emissão exige (CNO, código IBGE, alíquota de ISS
  e empresa) e diz, obra por obra, o que falta em cada uma. Acha o cadastro
  pela metade antes de ele travar a emissão 🔒
- ✅ **Qual seguro garantia está vencido ou perto de vencer?** — o prazo em
  dias é seu (60 por padrão). Dias negativos são apólices que JÁ venceram.
  Obra sem data preenchida **não aparece**, e a resposta avisa: não aparecer
  não é o mesmo que estar em dia 🔒
- ✅ **Qual obra aberta está com a vigência do contrato vencida?** — as que
  precisam de aditivo de prazo ou de encerramento. "Aberta" aqui é a FASE
  gravada no cadastro, e a resposta diz isso com todas as letras 🔒
- Qual a margem da obra X? Como ela mudou nos últimos 3 meses? ⚠️

> **Por que "quanto custou a obra tal" não está com ✅.** É a pergunta mais
> óbvia deste assunto, e é justamente a que falta. "Custo da obra" ainda não
> tem uma definição escolhida (§1): o que foi lançado? o que foi pago? entra o
> que está em análise? entra rateio de administração? Cada leitura dá um
> número diferente, **todos com cara de certo**. Responder hoje seria escolher
> uma delas por você, em silêncio. A suíte tem um teste que recusa qualquer
> pergunta do grupo de Obras que use as palavras "custo", "resultado",
> "lucro", "margem" ou "gastou" — quem for construir esbarra nele e vem
> combinar a palavra primeiro.

### Suprimentos

- ✅ **Quais insumos estão cadastrados numa categoria?** — sem categoria dita,
  o catálogo inteiro com a contagem por categoria. A busca **ignora acento**
- ✅ **Quanto já pagamos por um insumo?** — o último preço, quando e de onde veio
- ✅ **O que a obra pediu e ainda não foi resolvido?** 🔒
- ✅ **Quais insumos estão sem conta do plano financeiro?**
- Quais cotações estão abertas e quais fornecedores ainda não responderam?
- Qual o melhor preço já pago pelo insumo X, e quando, e de quem?
- Este preço que estão me cobrando está acima do que costumamos pagar?
- Quanto esta obra comprou de material este mês? 🔒
- Quais pedidos de compra foram feitos e não foram recebidos?
- Quais fornecedores atendem a categoria X e na região Y?

### Locações (dentro do grupo de Suprimentos)

- ✅ **O que está locado agora, e em qual obra?** — com o custo por período 🔒
- ✅ **Qual locação já pedia decisão (comprar ou devolver)?** — junta os três
  avisos: aluguel que já pagou a compra, devolução vencida e prazo estourado 🔒
- ✅ **Que aluguel já venceu e ainda não virou título?** 🔒

### Pessoal

- Quantos colaboradores estão alocados na obra X? 🔒
- Quanto custou a folha da obra X este mês? 🔒 ⚠️
- Quais colaboradores estão com exame ocupacional vencido?
- Quanto foi gasto com EPI na obra X?

### Conciliação e banco

- Quantos títulos ainda não estão conciliados? Quanto somam?
- Quais lançamentos do extrato não têm título correspondente?
- Qual o saldo de cada conta bancária?
- Quais pagamentos foram feitos pela conta errada e ainda não foram
  ressarcidos? (é a conta 9.1.03 com saldo diferente de zero)

### Uso do sistema (o relatório de trabalho)

- O que fulano fez no sistema hoje? E esta semana?
- Quantos títulos cada pessoa lançou este mês?
- Onde a fila está parada — quem tem mais coisa esperando decisão?
- Quanto gastamos de IA este mês, e por pessoa?
- Quem entrou no sistema hoje, e a que horas foi a primeira e a última ação?
  ⚠️ **Nunca apresentar isto como jornada de trabalho** — ver o porquê no
  `ROTEIRO.md`.

---

## 3. Perguntas que o sistema AINDA NÃO consegue responder

Vale mais do que parece: é a lista do que falta, escrita na forma como o dono
pergunta. O assistente deve responder "isto eu não sei, e é porque falta X" —
nunca chutar.

| A pergunta | O que falta |
|---|---|
| "Quanto esta obra deu de lucro depois de tudo?" | rateio da administração sobre as obras — não existe regra definida |
| "Este orçamento está batendo com o realizado?" | não há orçamento por obra cadastrado no ERP |
| "Quanto tempo leva do pedido até a entrega?" | o recebimento existe, mas não há medida de prazo montada |
| "Quem é o melhor fornecedor?" | não há nota de desempenho (prazo, qualidade, recusa) |
| "Quanto vou precisar de caixa nos próximos 90 dias?" | previsão existe por título; falta juntar com o previsto a receber |

---

## 3b. Perguntar escrevendo — e os três finais possíveis

A tela tem um campo de texto. O que a pessoa escreve passa por
`core/perguntas/entender.py`, que **não toca no banco** — só diz QUAL pergunta
a frase é. Ser uma operação de texto pura é o que permite essa rota ser aberta
a todo operador sem mentir: quem responde continua sendo a rota do grupo, com
a ação dela.

Três finais, e os três são honestos:

| Final | Quando | O que a tela faz |
|---|---|---|
| **Entendi** | uma pergunta ganha das outras com folga | responde, já com os filtros que a frase disse |
| **Qual delas?** | duas ou mais empatam no topo | mostra as empatadas e deixa escolher |
| **Ainda não sei** | nada casa | diz isso, guarda a pergunta e sugere as parecidas |

**O empate vira pergunta de volta, não escolha.** É a regra que o próprio dono
deu: *"talvez valesse a pena questionar, se não tivesse sido bem específica"*.
Sem ela, "o que está sem nota" — que empata entre três perguntas, porque
**"nota" quer dizer duas coisas no ERP** (a que recebemos, anexada ao título, e
a que emitimos ao cliente) — seria respondida com uma delas, com ar de certeza.

**A frase também diz os filtros.** "Me manda a lista dos insumos da categoria
hidráulico" tem a categoria escrita ali; respondê-la com o catálogo inteiro
seria ignorar metade do que a pessoa falou. (Aconteceu na primeira versão:
3.285 insumos em vez de 305.)

**Ainda é casamento por PALAVRAS, não IA** — e isso é escada, não teto. Quando
a chave da OpenAI existir em produção, a IA entra exatamente aí, escolhendo a
mesma chave com mais jeito, e o resto não muda uma linha. O que nem ela
entender continua caindo no mesmo lugar honesto: "isto eu não sei".

## 3c. Perguntar FALANDO, e perguntar sobre um DOCUMENTO

Desde 11/09/2026 a pergunta pode entrar de três jeitos. Os três terminam no
mesmo lugar — **menos um**, e a diferença é o que esta seção existe para
deixar clara.

| Como você pergunta | Quem responde | Dá para conferir? |
|---|---|---|
| Escrevendo | código escrito e testado | sim, a resposta diz de onde veio |
| **Falando** | código escrito e testado | sim — idem |
| **Anexando um documento** | **a IA lendo o arquivo** | sim, olhando o papel |

### Falar

O áudio **não responde nada**: ele vira texto, o texto cai na mesma caixa de
escrita, e **você lê antes de mandar responder**. Isso não é burocracia: "a
pagar" e "apagar" soam igual, e uma pergunta mal ouvida respondida em silêncio
seria o pior defeito que essa tela poderia ter.

O botão do microfone só aparece onde o navegador deixa gravar. Recusar o
microfone não quebra nada — a caixa de escrita continua ali.

### Anexar

Este é o **único** ponto da área de Perguntar em que a resposta vem da IA, e a
tela avisa em amarelo. Por que é aceitável aqui e não no resto: **o documento
está na sua mão.** Se a IA ler R$ 1.248,00 onde está R$ 12.480,00, você vê no
papel. Um total somado sobre dez mil lançamentos, não — e é por isso que aquele
é código e este pode ser IA.

Duas coisas que a tela deixa explícitas, de propósito:

- **Nada é gravado.** Ler não é arquivar. Para guardar o documento, o caminho
  continua sendo o Arquivo.
- **O que a IA não conseguiu ler, ela declara** — e isso aparece, em vez de
  ficar escondido atrás de uma tabela com cara de completa.

### O que ainda falta para os dois funcionarem

A chave do serviço de IA (`OPENAI_API_KEY`) **não está configurada em
produção**. Sem ela, falar e anexar recusam com uma frase que diz isso, e todo
o resto da tela continua funcionando igual — as perguntas sobre o que já está
no ERP não usam IA nenhuma.

## 4. Como esta lista vira código

Cada pergunta daqui vira uma função em `core/perguntas/respostas.py`, com nome,
parâmetros e teste — não uma consulta escrita pela IA. O assistente, quando
existir, só escolhe QUAL função chamar e com quais parâmetros; a conta é sempre
do sistema. O registro fica em `core/perguntas/catalogo.py`.

**A rota é POR GRUPO de pergunta.** Hoje só existe o grupo `financeiro`, que
vive sob `ver_erp` + escopo: todo operador pode perguntar, e cada um recebe a
conta feita apenas sobre o que já veria na tela de Títulos. Grupo novo
(suprimentos, contratos) ganha **rota própria com a ação própria dele** — uma
rota só, respondendo perguntas de pesos diferentes, obrigaria a conferir
permissão por dentro, e aí a ação declarada na rota mentiria.

**A resposta tem TETO de linhas** (`TETO_DE_LINHAS`, hoje 300), e isto tem uma
sutileza que não pode se perder: **a conta é sempre feita sobre TUDO** — o teto
corta só o que viaja para a tela, e a resposta continua dizendo quantas linhas
existem de verdade. Se um dia o número passar a ser o do corte, a resposta
mentirá. Há teste exigindo isso.

**A busca por texto ignora acento e maiúscula** (`_casa`). Ninguém digita
"Hidráulico" com acento: escreve "hidra". A primeira versão comparava direto e
respondia "nenhum insumo nessa categoria" sobre uma categoria com 305 itens —
o pior tipo de resposta errada, porque parece certa.

**Toda resposta devolve `de_onde_veio`** (a tela que reproduz o número) e, quando
a pergunta tem mais de uma leitura possível, uma `observacao` dizendo qual foi
usada. Isso não é enfeite: número sem caminho de volta não dá para auditar, e a
primeira resposta errada derruba a confiança em todas as outras.

Quando uma pergunta imprevista aparecer e for boa, ela entra neste arquivo e
vira função na sessão seguinte. É assim que a cobertura cresce e o caminho da
consulta inventada vai sendo usado cada vez menos.
