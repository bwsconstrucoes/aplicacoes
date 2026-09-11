# Perguntas que o assistente precisa saber responder

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
- Quais obras estão sem CNO, sem alíquota de ISS ou sem empresa ligada?
  (a pergunta que acha cadastro pela metade antes de ele travar a emissão)
- Qual a margem da obra X? Como ela mudou nos últimos 3 meses? ⚠️

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
