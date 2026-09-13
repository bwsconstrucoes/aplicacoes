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
| **"custo da obra"** | o que entra na conta? | ✔ **FECHADO em 12/09/2026: despesa DIRETA de DRE, nas visões COMPROMETIDO e EXECUTADO** — ver abaixo |
| **"obra em andamento"** | pela situação no cadastro, pela data de término, ou por ter tido movimento nos últimos N dias? | ⬜ a decidir |
| **"quanto falta receber"** | são QUATRO leituras, todas legítimas — ver a régua abaixo | ✔ **decidido: o assistente PERGUNTA de volta** |
| **"resultado da obra"** | inclui rateio da administração? inclui os tributos? | ✔ **decidido 11/09/2026: receita − custo direto** — ver abaixo |
| **"gastei com fulano"** | pelo título lançado ou pelo pagamento feito? | ⬜ a decidir |
| **"este mês"** | mês da competência, do vencimento ou do pagamento? | ⬜ a decidir |

### Custo e resultado da obra — decidido em 11/09/2026

Palavras do dono: *"quando eu pergunto o que é o custo da obra são as despesas
diretas da obra. Esse é o custo da obra. O resultado, eu abato a receita."*

Então:

- **Custo da obra = despesas DIRETAS da obra.** O que foi rateado naquela obra.
  **Fica de fora** o rateio da administração da empresa — despesa de escritório
  não vira custo de obra.
- **Resultado da obra = receita da obra − custo direto da obra.**

### O regime — FECHADO em 12/09/2026

Faltava dizer o regime, e o dono fechou, com estas palavras: *"o custo
normalmente está associado só às despesas de DRE, nada de fluxo. E é o custo
executado e o custo comprometido — são essas duas visões que a gente tem"*.

Então a pergunta **"quanto custou a obra X"** responde assim, e sempre com os
dois números juntos:

| Visão | O que é | O que entra |
|---|---|---|
| **Comprometido** | a obrigação já existe, tendo o dinheiro saído ou não | todo título lançado que ainda vale |
| **Executado** | o dinheiro já saiu do caixa | a soma dos pagamentos |

E três regras que vêm junto:

1. **Só conta de DRE** (natureza *resultado*). Transferência entre contas,
   aporte e principal de empréstimo **não são custo** — é dinheiro mudando de
   lugar, e somá-los inflaria o custo sem nada ter sido consumido.
2. **Rascunho, cancelado, estornado e devolvido não comprometem nada.**
3. **Continua valendo a despesa DIRETA**: rateio da administração da empresa
   não vira custo de obra.

A diferença entre comprometido e executado é, por definição, **o que ainda
falta sair do caixa** — e a resposta mostra essa terceira coluna também.

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

#### Cancelamento de lançamento — perguntas abertas em 12/09/2026

Desde 12/09/2026 quem lançou cancela o próprio lançamento (enquanto ninguém
baixou nem conciliou), e quem lançou é avisado com o motivo. Isso torna
possíveis perguntas que antes nem faziam sentido — e **nenhuma delas responde
ainda**, porque o cancelamento vive no registro de eventos e não numa consulta
pronta:

- ❌ **Quais lançamentos foram cancelados este mês, e por quê?** 🔒
  Falta: uma resposta que leia o registro de eventos do título, trazendo quem
  cancelou, quando e o motivo escrito.
- ❌ **Quem cancelou a SP tal, e qual foi o motivo?** 🔒
- ❌ **Algum lançamento meu foi cancelado?** 🔒 — a resposta muda conforme quem
  pergunta, por definição.
- ❌ **Quanto foi cancelado em valor, por obra?** 🔒 — cuidado com a palavra:
  "cancelado" não é perda nem economia; é lançamento que não devia existir.
  Somar isso como se fosse dinheiro poupado seria número errado com cara de
  certo.
- ❌ **Quem mais cancela lançamento?** — é pergunta de qualidade do
  lançamento, não de culpa. Muita coisa cancelada na mesma obra costuma
  significar processo confuso, não pessoa desatenta.

⚠️ **A palavra ambígua aqui é "cancelado".** Um título CANCELADO nunca foi
pago; um ESTORNADO foi pago e desfeito. São coisas diferentes no dinheiro e no
imposto, e qualquer resposta sobre "cancelados" tem de dizer qual das duas está
contando.

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

**Perguntas que a varredura de 11/09/2026 tornou possíveis — e que valem como
conferência do próprio sistema:**

- O extrato importado bate com o saldo do banco naquele mês?
  ⚠️ **Se der diferença em mês ANTIGO, provavelmente é linha perdida na
  importação** — dois pagamentos iguais no mesmo dia viravam um só até
  11/09/2026. Reimportar o OFX do período resolve.
- Alguma parcela tem mais de um pagamento registrado? (a partir da migração
  061 o banco não deixa mais entrar; a pergunta serve para o que entrou antes)
- Quais linhas do extrato foram conciliadas e depois desfeitas, e por quê?
- Quanto já foi PAGO e quanto ainda está em aberto, por obra?
  ⚠️ **Depende de quem pergunta** — cada pessoa vê só as obras dela.
  📌 Desde 11/09/2026 "pago" é a soma dos pagamentos de verdade, rateada pela
  obra; antes o relatório olhava a situação do título e mostrava título pago
  pela metade como inteiramente em aberto.

### Quem vê o quê (perguntas que o recorte por obra tornou possíveis)

Desde 12/09/2026 o padrão do ERP é **limitar a informação a quem está
associado à obra**. Isso muda a resposta de quase toda pergunta conforme quem
pergunta — e cria perguntas novas sobre o próprio acesso:

- Quais obras eu enxergo?
- Quem tem acesso à obra X? (e quem é de fora da empresa nessa lista)
- O parceiro fulano enxerga o quê, exatamente?
  ⚠️ **Depende de quem pergunta** — só quem cadastra operador alcança isso.
- Tem alguém cadastrado sem obra nenhuma associada? (essa pessoa não vê nada,
  e provavelmente é cadastro pela metade)
- 📌 **Toda pergunta de dinheiro, de equipe e de documento já responde
  recortada** pelas obras de quem pergunta. Duas pessoas fazendo a MESMA
  pergunta recebem números diferentes, e isso é o certo — não é defeito.

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

### Os dois dependem da chave de IA — e ela está ligada

Falar e anexar são as duas únicas coisas desta tela que usam IA, e portanto as
duas únicas que dependem da chave do serviço (`OPENAI_API_KEY`). **Ela está
configurada em produção**, com esse nome, e aparece em Configurações › Saúde do
sistema › "O que está ligado".

Se um dia ela sair do ar, os dois botões recusam com uma frase dizendo isso, e
**todo o resto da tela continua igual** — as perguntas sobre o que já está no
ERP são respondidas por código escrito e testado, sem IA nenhuma e sem custo.

## 3d. Onde se pergunta, e como se continua

Desde 11/09/2026 o assistente está no **canto de toda tela** — botão redondo,
painel que abre por cima. Não é mais uma aba do Financeiro: as perguntas já
alcançam obras, contratos e suprimentos, e ficar lá dava a entender que era
coisa de um módulo só.

A tela cheia continua em `/erp/perguntar` (o ⤢ do painel leva até ela), para
resposta com tabela grande.

### Dá para continuar a conversa

| Você escreve | O que acontece |
|---|---|
| "o que tem a pagar hoje" | responde |
| "e da obra Triunfo?" | **repete a anterior** trocando a obra — e diz isso |
| "e a elétrica?" | idem, quando a pergunta anterior tem um filtro só |
| "o que está sem nota da obra X" | **não** é continuação: tem assunto próprio |

A frase *"Repeti a pergunta anterior — X — trocando obra = Triunfo"* aparece
em cima da resposta, sempre. Sem ela, você acharia que ele entendeu a pergunta
nova — e não entendeu: ele repetiu a anterior.

Quando a frase curta empata entre duas perguntas, ele continua perguntando de
volta em vez de escolher.

## 3e. O que está ESCRITO nos documentos

Desde 11/09/2026 o assistente também responde sobre o que está nos documentos
arquivados — contrato, edital, norma —, e não só sobre os números do banco.

- ✅ **O que os documentos dizem sobre um assunto?** — "o que o contrato diz
  sobre reajuste", "qual o prazo de garantia", "procure multa por atraso" 🔒

**Esta é a única família de perguntas que não faz conta nenhuma.** A resposta é
um pedaço de texto que já estava escrito, e o que o sistema garante não é o
número: é **de qual documento saiu e em que trecho**. As « » marcam onde as
suas palavras aparecem.

### Três coisas que valem a pena você saber

**1. Só aparece o que você já podia ver.** A busca passa pelo mesmo recorte da
tela do Arquivo — faixa de sigilo e obra designada. Quem é preso a uma obra não
lê o contrato da outra; quem não alcança documento de pessoal não o encontra
aqui. Decisão sua, com todas as letras: *"quem vê o quê tem que estar associado
às suas permissões"*.

**2. A busca é por PALAVRA, não por sentido.** Ela entende família de palavra —
"reajuste" acha "reajustar" e "reajustados" — e ignora as palavras de ligação.
Mas **não** acha "correção monetária" quando você procura "reajuste". Quando
não achar, são duas causas bem diferentes, e a resposta diz as duas: ou o
documento não está no Arquivo, ou ele usa outras palavras.

O passo seguinte, se isso incomodar, é o índice por significado — que acha
mesmo com outras palavras e custa por documento. Decisão sua: *"vamos começar
do simples, depois a gente decide se parte pro caro"*.

**3. Quando aparecer uma frase de resumo em cima, ela foi escrita pela IA
lendo SÓ os trechos de baixo.** Ela não consulta o banco e não lembra de nada
por fora. Os trechos continuam ali, e são eles a resposta — a frase é só a
leitura em voz alta. Sem chave de IA, a frase some e os trechos ficam.

## 3f. Perguntar sobre UM documento — o contrato da obra, por exemplo

Desde 12/09/2026, pedido seu: *"tem um contrato de uma obra e eu quero
perguntar alguma coisa sobre ele"*. No Arquivo, cada documento tem o botão
**Perguntar**. Você aponta o documento e a IA lê o texto DELE.

**A diferença para a busca de cima, e ela é grande.** Lá você procura uma
palavra no acervo inteiro e recebe os trechos que casaram. Aqui você pergunta
sobre um documento só, e a IA lê o documento — não a palavra.

Perguntas que passam a funcionar (e não funcionavam antes):

- ✅ **Qual o prazo de garantia deste contrato?** 🔒
- ✅ **Como é o reajuste, e a partir de quando?** 🔒
- ✅ **Qual a multa por atraso?** 🔒
- ✅ **O que este contrato exige na entrega da medição?** 🔒
- ✅ **Quem são as partes e qual o objeto?** 🔒
- ✅ **Tem cláusula de retenção de garantia? De quanto?** 🔒
- ✅ **Esta certidão vale até quando?** 🔒
- ✅ **Este documento fala em multa? O que exatamente?** 🔒

🔒 = **a resposta depende de quem pergunta.** O documento passa pelo mesmo
recorte do Arquivo: quem é preso a uma obra não pergunta sobre o contrato da
outra — e ouve "documento não encontrado", nunca "sem permissão", porque dizer
"sem permissão" confirmaria que aquele documento existe.

### As três coisas que decidem se dá para confiar

**1. A resposta nunca vem sozinha.** Embaixo dela ficam os trechos do
documento, na íntegra. E o sistema PROCURA cada trecho dentro do documento
antes de mostrar: trecho que a IA escreveu mas que não está lá é descartado, e
a resposta sai marcada como **não conferida** — "trate como pista, não como
resposta". É a única defesa real contra um número inventado com cara de
citação.

**2. Documento que é FOTO ou digitalização não é lido.** Decisão sua em
12/09/2026: *"não ler escaneados por hora"*. O sistema responde "este documento
é uma imagem, não consigo ler o texto dele" — ele não chuta. Ler escaneado
exigiria IA olhando página por página, e isso custa por documento; é um
acréscimo à parte, para quando você quiser.

**3. Documento muito longo entra pelos trechos que falam do assunto**, não
inteiro — e a tela avisa quando isso acontece. É o que mantém o custo em
centavos por pergunta.

### O que ainda NÃO dá

- ❌ **"Compare este contrato com o da outra obra"** — hoje é um documento por
  pergunta. Falta poder apontar dois.
- ❌ **"Continue perguntando sobre o mesmo documento"** — cada pergunta é
  independente; ela não lembra da anterior.
- ❌ **Perguntar sobre documento escaneado** — falta a leitura por imagem, que
  custa por documento (acima).
- ❌ **"Qual dos meus contratos tem a multa mais alta?"** — isso é varrer o
  acervo comparando cláusulas, não ler um documento. Precisaria do índice por
  significado.

## 3g. A resposta vira RELATÓRIO — Excel e PDF

Desde 12/09/2026, pedido seu: *"se eu quiser, olha, gera um relatório em PDF de
um determinado assunto, gera um relatório em Excel com essas informações"*.

Toda resposta que tem TABELA ganha dois botões embaixo dela: **⬇ Excel** e
**⬇ PDF**. Vale no painel do cantinho e na tela cheia.

**Quem monta o arquivo é o sistema, não a IA.** São os mesmos números da
resposta, exportados — e isso é o que importa: relatório escrito por IA é
relatório que ninguém pode conferir.

Três coisas que valem saber:

- **O arquivo leva TODAS as linhas que a resposta trouxe**, mesmo quando a
  tela mostra só as primeiras. Exportar só o que está visível seria uma
  armadilha silenciosa.
- **O cabeçalho do arquivo diz de onde veio**: a frase da resposta, a tela de
  origem e quem gerou, com data. Planilha sem procedência é número sem origem,
  e três meses depois ninguém sabe o que era.
- **Resposta sem tabela não vira arquivo** — o botão nem aparece. É o caso do
  trecho de contrato: ele é texto, não planilha.

## 3h. Encaminhar a informação por WhatsApp

Desde 12/09/2026, pedido seu: *"às vezes a gente quer encaminhar alguma
informação pra alguém (…) referente a um título financeiro"*.

- **No lançamento** (Financeiro › Solicitações, abrindo a ficha): botão
  **↗ Encaminhar**. Vai credor, valor, forma de pagamento, conta, obra,
  vencimento de cada parcela, descrição e situação.
- **No documento** (Arquivo): botão **Enviar** — o nome do documento, o tipo e
  até quando vale.
- Para **operadores cadastrados** (a lista aparece pronta) ou para um **número
  avulso** com DDD. Dá para mandar o arquivo junto e escrever um recado.

**As três travas, para você saber o que está protegido:**

1. **Ninguém encaminha o que não pode ver** — passa pelo mesmo recorte por obra
   da tela. Sem isso, encaminhar seria a porta dos fundos do controle de acesso.
2. **Fica registrado quem mandou o quê para quem, e quando.** A mensagem sai do
   sistema e o ERP não controla o que acontece depois; o que ele pode fazer é
   dar nome ao que saiu.
3. **Número que não parece telefone é recusado** antes de sair, e a tela mostra
   o texto inteiro antes do disparo. Enviado não volta.

⚠️ **O que NÃO vai, nunca:** senha, chave de acesso e dado bancário completo.
Mensagem de WhatsApp é o lugar mais fácil de vazar que existe na empresa.

Perguntas que isso torna possíveis, e que **ainda não respondem**:

- ❌ **O que foi encaminhado deste lançamento, e para quem?** 🔒 — o registro
  existe; falta a consulta pronta.
- ❌ **Quem mais encaminha informação para fora?** — pergunta de processo, não
  de culpa: muito encaminhamento costuma significar que falta alguém ter
  acesso à tela.

## 3i. Os relatórios — e a palavra que faltava separar

Em 12/09/2026 a tela de Relatórios ganhou **fluxo de caixa projetado**, **curva
ABC**, **consolidado por empresa** e a exportação em **Excel e PDF** (era a
única tela do ERP que só dava CSV).

⚠️ **E uma palavra entrou para a lista das ambíguas: "total da obra".**

Até então, "totais por obra" somava o que a obra vai **RECEBER** com o que ela
**CUSTOU**, num número positivo só — porque a medição a receber e a nota a
pagar moram na mesma lista. Uma obra que gastou 10 mil e vai receber 50 mil
aparecia com "60 mil". Agora a **espécie** é escolha explícita na tela, e o
padrão é **a pagar**, porque este é um relatório de custo.

Perguntas que passam a funcionar:

- OK **Quanto eu tenho a pagar por semana, nas próximas 13 semanas?**
- OK **Em que semana o caixa fica negativo?** — a partir do saldo que você
  informar; o sistema **não sabe** o saldo do banco.
- OK **Quais fornecedores respondem por 80% do que eu gasto?**
- OK **Quanto cada empresa do grupo gastou no período?**
- OK **Quanto esta obra vai receber, separado do que ela custa?**

Todas mudam conforme quem pergunta (recorte por obra).

O que ainda **nao** responde:

- FALTA **"Este gasto esta dentro do previsto?"** — falta orcamento por obra.
  E a pergunta que o sistema inteiro ainda nao sabe responder, e a que mais
  muda uma decisao.
- FALTA **"Como esta este mes comparado com o mes passado?"** — nenhum
  relatorio compara periodos.
- FALTA **"Quanto esta vencido ha mais de 60 dias?"** — o fluxo mostra o
  vencido num total so, sem faixas.

## 3j. Perfil de acesso virou cadastro — e o que dá para perguntar sobre ele

Em 13/09/2026 o **perfil deixou de ser um nome de cargo escrito em código** e
virou **cadastro**, do jeito que o dono descreveu: cria-se o perfil, diz-se o
que ele abre em cada tela (**não acessa / só olhar / olhar e mexer**), e a
pessoa entra dentro do perfil. As **obras continuam sendo do cadastro da
pessoa**, e não do perfil — foi a diferença que ele mesmo apontou.

Perguntas que passam a funcionar:

- OK **Quem está no perfil X?**
- OK **O que este perfil abre, tela por tela?**
- OK **Quantas pessoas ficariam sem acesso se eu arquivar este perfil?** — o
  sistema recusa arquivar com gente dentro e diz quantas são.
- OK **Quem enxerga TODAS as obras da empresa?** — é uma marca no cadastro de
  cada pessoa, não mais uma lista em código.
- OK **Quem está sem obra nenhuma marcada?** — essa pessoa não enxerga nada,
  e quase sempre é cadastro pela metade.
- OK **Fulano pode pagar? E por quê — veio do perfil ou foi marcado nele?**
  ⚠️ **Depende de quem pergunta**: só quem cadastra operador alcança isso.

⚠️ **Palavra ambígua que entrou junto: "acesso".** Ela quer dizer duas coisas
diferentes, e a resposta muda conforme a que se pediu:

- **o que a pessoa FAZ** — vem do perfil (lançar, pagar, aprovar…);
- **ONDE ela faz** — vem das obras marcadas no cadastro dela.

Alguém pode ter o perfil mais completo da empresa e não enxergar lançamento
nenhum, por não ter obra marcada. Não é defeito: é o padrão NEGAR.

O que ainda **não** responde:

- FALTA **"O que mudou no acesso de fulano no último mês?"** — cada mudança
  fica registrada, mas não há pergunta pronta que leia esse histórico.
- FALTA **"Este perfil está sobrando?"** — ninguém mede quais seções de um
  perfil nunca foram usadas por quem está nele.

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
