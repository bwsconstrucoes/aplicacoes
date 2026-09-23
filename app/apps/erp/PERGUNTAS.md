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

Desde 13/09/2026 o perfil também **esconde a área que não libera**: quem não
tem Suprimentos não vê o módulo, nem as abas dele, nem a tela de início
oferecendo — decisão do dono, com estas palavras: *"se a pessoa está liberada
apenas pra visualizar lançamento financeiro, ela não tem que ver nada do
suprimento"*. Esconder não é a trava (a trava é a recusa da rota), é para o
menu não oferecer o que vai responder "sem permissão".

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

## 3k. PROJETO: um punhado de obras olhado somado

Em 13/09/2026 nasceu o **projeto**: um conjunto de obras que se olha junto.
A hierarquia do sistema passou a ser **empresa › projeto › obra**, e obra sem
projeto continua sendo o caso comum.

Perguntas que passam a funcionar:

- OK **Quanto custou o projeto Creches 2026?** — vem SOMADO, não obra a obra,
  nas mesmas duas visões: comprometido e executado.
- OK **Quanto cada projeto comprometeu no ano?**
- OK **Quais obras estão no projeto X?**
- OK **Qual obra está fora de qualquer projeto?**
- OK **Quanto esta empresa do grupo gastou?** — já existia; agora convive com
  o projeto na mesma tela de relatórios.

⚠️ **Duas palavras para não confundir**, e a diferença muda o número:

- **empresa** é o CNPJ que executa a obra;
- **projeto** é um agrupamento que VOCÊ define, e pode até misturar obras de
  empresas diferentes.

Somar "projeto" achando que é "empresa" dá um número que parece certo e não é.
Por isso a linha das obras sem projeto aparece com o nome **"Sem projeto"** —
número que some é pior que número errado, porque ninguém procura o que não
sabe que falta.

O que ainda **não** responde:

- FALTA **"Este projeto está dentro do orçamento?"** — continua faltando
  orçamento, que é a resposta que mais muda uma decisão.
- FALTA **"Qual o resultado do projeto?"** — depende de fechar a palavra
  "resultado da obra", que segue indefinida.

## 3l. De onde sai o e-mail de cada empresa

Em 14/09/2026 uma empresa passou a poder **usar a conta de e-mail de outra**:
o envio sai pela conta principal e aparece com o remetente de quem manda.

Perguntas que passam a funcionar:

- OK **Qual empresa manda e-mail por qual conta?**
- OK **Alguma empresa está sem conta de e-mail?** — ela não dispara cotação, e
  a tela de Empresas já conta quantas estão assim.
- OK **Quando foi o último teste de envio que deu certo, por empresa?**
- OK **O que falta na conta desta empresa?**

⚠️ **Palavra ambígua: "o e-mail da empresa".** São três coisas diferentes, e
confundi-las dá resposta errada com cara de certa:

- **a conta que entra no servidor** (usuário e senha) — pode ser de outra
  empresa;
- **como aparece para quem recebe** (o remetente) — é sempre da empresa que
  manda;
- **o e-mail do cadastro**, que é só contato e não manda nada.

O que ainda **não** responde:

- FALTA **"Este e-mail chegou ou caiu no spam?"** — o ERP registra o que ele
  entregou ao servidor; o que acontece depois é do provedor de quem recebe.
- FALTA **"Este domínio está com SPF e DKIM certos?"** — é DNS, fora do ERP.
  O que dá para fazer é mandar a mensagem de teste e olhar onde ela caiu.

## 3m. O índice do reajuste, e não só a variação

Em 14/09/2026 a tabela de índices ganhou o **número-índice** ao lado da
variação mensal.

Perguntas que passam a funcionar:

- OK **Qual o índice do INCC em tal mês?**
- OK **Qual o índice inicial e o final deste reajuste?** — a previsão da
  medição mostra os dois e a divisão entre eles.
- OK **De quanto foi o reajuste acumulado da data-base até aqui?**
- OK **Quais meses faltam na tabela do INCC?**

⚠️ **Palavra ambígua: "o índice".** São duas coisas, e confundi-las dá erro de
ordem de grandeza:

- **a variação do mês** — 0,42%, que é o que o Banco Central publica;
- **o número-índice** — 105,403139, que só serve dividido por outro.

⚠️ **Palavra ambígua: "a régua".** No mesmo dia 14/09/2026, vendo a coluna
nova, o dono disse: *"apareceram os índices, mas os números estão diferentes do
que eu costumo ver — veja o de 08/2026, 305,943822"*. O número estava certo e
noutra base. Desde então a tabela pode ser **alinhada ao boletim dele**: ele
informa o índice de UM mês, como o boletim mostra, e a série inteira se desloca.

Perguntas que isso acrescenta:

- OK **A tabela do INCC está alinhada com o meu boletim?** — a tela de índices
  diz qual régua está valendo e de onde veio o número.
- OK **Quem alinhou a tabela, quando, e com qual número?** — fica na auditoria.
- OK **Alinhar a tabela muda algum reajuste já calculado?** — não muda, e há
  teste cobrando isso: o fator é razão entre dois meses, e razão não sente
  mudança de base.

O que ainda **não** responde:

- FALTA **"Qual o índice publicado pela FGV neste mês?"** — a série do
  número-índice da FGV é licenciada; o Banco Central republica só a variação.
  O alinhamento resolve a leitura (os números passam a bater), mas quem informa
  o ponto de partida é uma pessoa, com o boletim na mão.
- FALTA **"O número que está na tela ainda bate com o boletim de hoje?"** — o
  sistema não tem como conferir sozinho contra a FGV. Se o Banco Central
  revisar uma variação passada, a série inteira se desloca a partir da âncora,
  e só quem olha o boletim percebe.
- ⚠️ **Marcar a régua com o índice ERRADO não é detectável pelo sistema.** Se
  alguém digitar o número de um INCC diferente (o -M em vez do -DI), os números
  da tela ficam plausíveis e errados. O reajuste continua certo — ele não usa o
  número absoluto —, mas a conferência visual passa a mentir.

## 3n. O documento arquivado e o que ele deixou em branco

Em 14/09/2026 as telas que leem documento passaram a **abrir os campos que a
leitura não achou** — as datas do documento e os campos de cadastro sem
resposta. Antes, o que a IA não extraía não existia na tela.

Perguntas que passam a funcionar:

- OK **Quais documentos estão sem data de validade?** — hoje nenhum de tipo que
  vence: a tela não deixa arquivar sem ela.
- OK **Quais obras estão sem fim de vigência no cadastro?** 🔒 (escopo por obra)
- OK **O que vence nos próximos 30 dias?** — já existia, e agora tem menos buraco,
  porque a data deixou de depender de a IA tê-la encontrado.
- OK **Quem informou esta data: o documento ou uma pessoa?** — a origem da
  leitura fica registrada com o documento.

⚠️ **Palavra ambígua: "vigência".** São duas datas próximas e diferentes:

- **a validade do DOCUMENTO** — até quando o papel vale, e é dela que sai o
  aviso da agenda;
- **o fim da vigência da OBRA** — campo do cadastro, que é o que o contrato
  determina.
  Num contrato de obra as duas coincidem, e por isso a tela pergunta **uma vez
  só**, com o nome do negócio. Em aditivo NÃO coincidem: o aditivo estende a
  vigência da obra sem mudar a validade do contrato original.

⚠️ **Palavra ambígua: "prazo".** Também são dois números diferentes, e trocá-los
dá data errada com cara de certa:

- **prazo de VIGÊNCIA** — por quanto tempo o contrato vale (365 dias, por
  exemplo). Vira a data final;
- **prazo de EXECUÇÃO** — por quanto tempo há obra para fazer. Costuma ser
  menor, e não define até quando o contrato vale.

O que ainda **não** responde:

- FALTA **"Este contrato tem reajuste, e por qual índice?"** respondido a partir
  do documento sozinho. O campo *Índice de reajuste* existe no cadastro da obra
  e o contrato pode preenchê-lo, mas quando o contrato descreve a fórmula em
  texto corrido a leitura não converte isso em regra de cálculo — quem decide
  data-base, índice e periodicidade continua sendo uma pessoa.

## 3o. Trocar o documento errado, ligar a obra à empresa, e o alerta que leva ao lugar

Três mudanças de 17/09/2026, todas nascidas de uso: o dono anexou uma ART
errada e não tinha como trocar; criou obra sem empresa e não tinha onde
consertar depois; e via os alertas do painel de obras sem poder clicar neles.

Perguntas que passam a funcionar:

- OK **Quais documentos já foram substituídos, e por quem?** — a troca fica no
  histórico do documento, com o arquivo que saiu, o que entrou e o motivo.
- OK **Quais obras estão sem empresa?** 🔒 (escopo por obra) — antes a resposta
  seria sempre "nenhuma", porque a obra nem nascia ligada; hoje é uma lista de
  verdade e o alerta do painel leva direto ao campo.
- OK **Quantas obras tenho em cada empresa?** 🔒 (escopo por obra)
- OK **Quais obras estão com alguma pendência de cadastro?** — sem empresa, sem
  responsável, sem ISS, sem CNO, vigência ou reajuste vencidos. É a mesma
  lista dos selos do painel.
- OK **Quem trocou este arquivo, e quando?** — o registro guarda a pessoa, a
  hora e o motivo escrito.

⚠️ **Palavra ambígua: "trocar o documento".** São duas coisas diferentes:

- **trocar o ARQUIVO** — o papel estava errado (foi a ART de outra obra). O
  registro continua o mesmo, com as mesmas datas e o mesmo tipo; só o PDF muda,
  e o antigo é apagado de propósito, porque ele não deveria estar ali;
- **ATUALIZAR o documento** — o papel venceu e veio uma via nova (licença
  renovada). Aí são dois documentos, e o antigo precisa continuar existindo,
  porque ele prova o que valia naquele período. Esse caso se resolve
  arquivando o novo, não trocando o arquivo do velho.

⚠️ **Palavra ambígua: "empresa da obra".** Na obra pública há duas empresas na
mesma conversa: a **contratante** (o órgão, que no cadastro chama-se *cliente*)
e a **contratada** (uma das empresas da casa, que assina e emite a nota). O
campo *Empresa* é sempre a segunda. Pergunta com "empresa" solta deve dizer qual
das duas usou.

O que ainda **não** responde:

- FALTA **"Quais obras mudaram de empresa depois de criadas?"** — a mudança fica
  registrada como evento no histórico da obra, mas não existe consulta que leia
  o histórico; hoje só dá para abrir obra por obra.

## 3o-bis. Documento corrigido e documento excluído (23/09/2026)

Pedido do dono, olhando os documentos de uma obra: *"caso eu adicione um
documento de forma equivocada e precise alterar, não tem opção pra isso. Ou até
mesmo excluir algo que esteja errado."* Corrigir a etiqueta (tipo e descrição) e
excluir passaram a existir nas fichas — e as duas coisas ficam na trilha.

Perguntas que passam a funcionar:

- OK **Quais documentos foram excluídos, de qual obra e por quem?** 🔒 (escopo
  por obra) — a exclusão registra o arquivo, o tipo e quem apagou.
- OK **Quem trocou o tipo deste documento, e o que ele era antes?** — a
  correção guarda o de/para.

⚠️ **Palavra ambígua: "alterar o documento".** De novo as três coisas que já
brigavam entre si, agora com uma quarta:

- **corrigir a ETIQUETA** — o papel está certo, o rótulo está errado (entrou
  como "Outro" e é o contrato). É o que passou a existir agora;
- **trocar o ARQUIVO** — o papel está errado. Só na tela do Arquivo, onde o
  registro tem vida própria;
- **ATUALIZAR** — veio uma via nova. São dois documentos, e o velho continua;
- **EXCLUIR** — não deveria estar ali. Apaga o arquivo junto, sem desfazer.

O que ainda **não** responde:

- FALTA **"Quantos documentos foram excluídos este mês, no sistema todo?"** — os
  eventos existem por obra e por título, mas não há consulta que os some.
- FALTA **trocar o TIPO de um documento na tela do Arquivo** — lá o tipo decide
  o nome do arquivo, quem enxerga e a pasta no Drive; não é a mesma correção
  barata da ficha, e continua de fora.

## 3p. O ACOMPANHAMENTO: processos que correm fora da BWS

Construído em 18/09/2026 (migração 072, pedaço 1). Guarda assunto burocrático
que depende de terceiro — aditivo de prazo, apostilamento, licença, protocolo —
com andamento em uma frase, situação e responsável.

Perguntas que passam a funcionar:

- OK **O que está parado?** 🔒 (escopo por obra) — e o "parado" é do SISTEMA, não
  de alguém marcar: sai dos dias sem andamento, com teto por tipo.
- OK **O que passou da previsão?** 🔒 — prometeram uma data e ela venceu.
- OK **O que está com exigência a responder?** 🔒 — é o único estado em que o
  órgão está esperando a BWS, e não o contrário.
- OK **Quais processos estão com fulano?** 🔒 — e é a pergunta que alguém faz na
  véspera das férias dele.
- OK **Esta obra tem processo aberto?** 🔒
- OK **Onde está o aditivo de prazo da obra X?** 🔒 — o "está com" é atualizado
  pelo próprio andamento.
- OK **Há quantos dias este processo não anda?** 🔒

⚠️ **Palavra ambígua: "processo".** No ERP ela já significava outras duas coisas
— o **processo administrativo do ÓRGÃO** (o número que vem no contrato) e o
processo judicial. O módulo chama-se **Acompanhamento** na tela por causa disso,
e pergunta com "processo" solto tem de dizer qual das três usou.

⚠️ **Palavra ambígua: "parado".** Aqui é *sem andamento lançado*, e não *sem
andamento real*: o órgão pode ter mexido sem ninguém da BWS ter ligado para
saber. A resposta mede o que o sistema sabe, que é o registro — e é justamente
por isso que ela serve de cutucão.

⚠️ **Palavra ambígua: "em dia".** Significa apenas "não está vencido, parado nem
com exigência". Não significa que vai sair no prazo.

Com os passos (pedaço 2, migração 073), mais estas:

- OK **Quanto já andou este processo?** 🔒 — quantos passos de quantos.
- OK **O que falta neste aditivo?** 🔒 — os passos ainda não marcados.
- OK **Quais processos nem começaram?** 🔒 — nenhum passo marcado e nenhum
  andamento lançado.

⚠️ **Palavra ambígua: "falta".** Um passo não marcado pode significar duas
coisas bem diferentes: *ainda não foi feito* ou *foi feito e ninguém marcou*. A
lista é lembrete, não controle — a resposta mede a marcação, não o mundo.

Com o pedaço 3 (migração 074), mais estas:

- OK **Quanto tempo a prefeitura X demora para publicar um aditivo?** 🔒 — média,
  mais rápido e mais lento, por órgão e por tipo.
- OK **Quantos ofícios já mandei para este órgão?** 🔒
- OK **Qual foi o último ofício expedido, e sobre o quê?** 🔒
- OK **Quais aditivos vieram de um processo de acompanhamento?** 🔒

⚠️ **Palavra ambígua: "demora".** A conta é de **protocolo até encerramento**, e
não do dia em que a BWS começou a preparar o pedido. O tempo que a própria BWS
levou para montar a documentação fica de fora — de propósito, porque a pergunta
é sobre o órgão.

⚠️ **Palavra ambígua: "média".** Só processos ENCERRADOS entram. Um órgão com
três aditivos travados há meses pode aparecer com média baixa: são os
concluídos que contam, e a coluna "em andamento" é que mostra o resto.

O que ainda **não** responde:
- FALTA **"O que costuma travar mais?"** — exige histórico de vários processos
  encerrados, e o módulo nasceu vazio por decisão do dono (nada de importar do
  Pipefy). A resposta melhora sozinha com o uso.

## 3q. Fornecedores com vários vendedores, e o disparo automático

Construído em 18/09/2026 (migração 075), depois de o dono explicar de onde
vinham os 126 CNPJs repetidos: não era só sujeira, era cadastro sem lugar para
o segundo vendedor.

Perguntas que passam a funcionar:

- OK **Quais fornecedores têm mais de um vendedor?**
- OK **Quem atende esta região neste fornecedor?** — pela observação do contato.
- OK **Quais fornecedores estão fora do disparo automático?**
- OK **Quais fornecedores não estão ATIVOS na Receita?** — depois de rodar o
  "Acertar o cadastro pela Receita".
- OK **Quais fornecedores têm nome diferente do da Receita?** — a consulta
  relata, não troca.
- OK **O que está esperando cotação, e há quanto tempo?** 🔒 (escopo por obra)
- OK **Quantos itens estão parados por falta de categoria no insumo?**

⚠️ **Palavra ambígua: "fornecedor duplicado".** Depois desta mudança há duas
coisas diferentes: o **CNPJ repetido** (que agora vira um cadastro só com vários
contatos) e o **fornecedor com nome parecido e CNPJ diferente** (que são duas
empresas de verdade, ou um CNPJ digitado errado — e o sistema não tem como
saber qual).

⚠️ **Palavra ambígua: "não recebe cotação".** São três coisas: o fornecedor
INATIVO, o que está FORA do disparo automático (mas entra na cotação montada à
mão), e o CONTATO com "recebe cotação" desmarcado. A resposta tem de dizer qual.

O que ainda **não** responde:

- ~~FALTA "Qual fornecedor responde mais rápido?"~~ — **feito em 18/09/2026**,
  ver 3r abaixo.

## 3r. A cobrança sugerida, a proposta lida por inteiro e a memória do fornecedor

Construído em 18/09/2026 (migrações 076 e 077).

Perguntas que passam a funcionar:

**Cobrança**

- OK **O que eu preciso cobrar hoje?** 🔒 (escopo por obra) — cotação aberta,
  e-mail enviado, mais de 2 dias sem preço lançado e sem marca de resposta.
- OK **Há quantos dias este fornecedor está com a nossa cotação?**
- OK **Quantas vezes já cobramos este fornecedor nesta cotação?**
- OK **Quais fornecedores responderam por fora do e-mail?** — pelo canal
  marcado (WhatsApp, telefone, pessoalmente).
- OK **Quem avisou que não vai cotar, e por quê?**

**Memória do fornecedor**

- OK **Qual fornecedor responde mais rápido?** — média de dias entre o envio e
  a resposta.
- OK **Quem responde às nossas cotações e quem some?** — quantas de quantas.
- OK **Quem entrega no prazo?** — previsão do pedido × data do recebimento,
  contando só pedido que chegou INTEIRO.
- OK **Por que o sistema sugeriu este fornecedor?** — a linha do porquê já vem
  escrita na tela de planejamento.

**Banco de preços**

- OK **Qual foi o último preço deste insumo, e de quem?**
- OK **Qual o menor preço deste insumo no último ano, e quem deu?**
- OK **Quantas vezes cotamos este insumo no último ano?**
- OK **Quanto este insumo variou de preço?** — menor, maior e média da janela.

⚠️ **Palavra ambígua: "não respondeu".** O sistema **não sabe** se o fornecedor
respondeu — sabe que nenhum preço foi lançado. Toda resposta sobre cobrança tem
de dizer isso com estas palavras, porque a diferença é o WhatsApp do comprador.

⚠️ **Palavra ambígua: "último preço".** São três: o último **cotado** (pode ter
sido recusado), o último **comprado** (o que a empresa aceitou pagar) e o último
**da planilha antiga** (importado, sem cotação no sistema). A resposta tem de
dizer qual, e a origem está gravada (`origem` = SISTEMA ou PLANILHA).

⚠️ **Palavra ambígua: "menor preço".** O menor da janela de **12 meses**, que é
o que a tela mostra, não é o menor de todos os tempos. Preço de dois anos atrás
não é referência — mas continua no banco e aparece no Banco de preços.

⚠️ **Palavra ambígua: "bom fornecedor".** Pode ser o mais barato, o que responde
mais rápido, o que entrega no prazo ou o que a empresa mais comprou. São quatro
respostas diferentes e o sistema mede as quatro separadas.

O que ainda **não** responde:

- FALTA **"Quanto a gente economizou comprando do mais barato?"** — exigiria
  comparar o preço fechado com as alternativas do mesmo mapa, e decidir o que
  fazer quando não houve mapa (compra direta).
- FALTA **"Este preço está caro?"** com resposta de sim/não — hoje o sistema
  mostra os números e quem julga é o comprador. Um "está caro" automático
  precisaria de correção pela inflação do período (o INCC já está no sistema) e
  ainda assim erraria em material sazonal.
- FALTA **"Quem deu o melhor preço nesta CATEGORIA no último ano?"** — o dado
  existe, mas a soma por categoria (e não por insumo) ainda não foi escrita.

## 3s. O que mudou no cadastro, e quem mudou

Construído em 20/09/2026, depois de o dono trocar a conta do plano de um insumo
sem querer e não saber qual tinha sido.

Perguntas que passam a funcionar:

- OK **O que mudou nos insumos hoje?** — e nos últimos dias.
- OK **Quem alterou este insumo, e o que ele era antes?**
- OK **Alguém trocou a conta do plano de algum insumo?**
- OK **Este fornecedor pode ser apagado?** — e, se não, **onde ele aparece**.

⚠️ **Palavra ambígua: "excluir".** São duas coisas: **apagar** (só para quem
nunca foi usado — some do banco) e **desativar** (sai das listas de escolha e o
histórico fica). A resposta tem de dizer qual das duas é possível para aquele
cadastro.

⚠️ **Palavra ambígua: "preço conhecido".** É o último preço do `precos_historico`
— que junta o cotado nas cotações daqui, o comprado nos pedidos, e o histórico
antigo importado da planilha. Não é "preço de tabela do fornecedor", que o
sistema não tem.

- OK **Quais fornecedores têm nome diferente do da Receita?** — e quais são os
  dois nomes.
- OK **Este CNPJ já está cadastrado?** — respondida no meio do cadastro, antes
  de a pessoa digitar o resto.

⚠️ **Palavra ambígua: "nome do fornecedor".** São três: a **razão social**
cadastrada (a que o comprador digitou), a **razão social na Receita** (a
oficial) e o **nome fantasia** (por onde a obra o conhece). A resposta tem de
dizer qual está usando — e, depois de alguém adotar o oficial, o nome antigo
passa a ser o fantasia.

- OK **Quais fornecedores atendem a obra X?** — cruzando o município e a UF da
  obra com a abrangência do fornecedor.
- OK **Quantos fornecedores estão sem região cadastrada?** — e quais.
- OK **Por que este fornecedor não foi sugerido na cotação?** — a tela do
  disparo separa "não atende a região" de "sem região cadastrada".
- OK **Quais fornecedores atendem todo o Brasil?** / **só o Ceará?** / **só a
  Grande Fortaleza?**

- OK **De qual empresa é esta conta bancária?** — e quais contas ainda estão
  sem dona.
- OK **De qual empresa é este título?** — vem da obra do rateio.
- OK **Que pagamentos saíram pela conta de outra empresa?** — a trilha guarda o
  recado com os dois nomes, em `detalhe.outra_empresa`.

- OK **Quais títulos estão bloqueados, e por quê?** — a crítica que bloqueou
  fica gravada na análise, com o código e o recado.
- OK **Quais credores têm conta bancária esperando homologação?**
- OK **Quem cadastrou este credor, e quando?** — a trilha guarda, inclusive
  quando o cadastro nasceu de dentro do lançamento.

⚠️ **Palavra ambígua: "conta do credor".** Uma conta PENDENTE existe no
cadastro mas não paga nada. Responder "o credor tem conta" sem dizer o status
faria alguém contar com um pagamento que o sistema vai bloquear.

⚠️ **Palavra ambígua: "conta".** São três coisas diferentes no ERP: a **conta
bancária** (de onde o dinheiro sai), a **conta do plano financeiro** (a
classificação do gasto) e a **conta do fornecedor** (onde ele recebe). A
resposta tem de dizer qual das três está usando.

⚠️ **Palavra ambígua: "quanto tem em caixa".** Por empresa ou somando todas? A
pergunta só passou a ter resposta por empresa depois da migração 080 — e o que
estiver em conta sem dona não entra em nenhuma das duas.

O que ainda **não** responde:

- FALTA **"qual o saldo da conta"** — o ERP guarda o extrato importado, não o
  saldo do banco. Somar os lançamentos daria um número parecido e errado nos
  dias em que faltar extrato.
- FALTA **"quanto a empresa A deve para a B"** por causa dos pagamentos
  cruzados. Cada um fica registrado, mas ninguém soma — e somar sem combinar o
  que conta como acerto daria um número que não bate com o do contador.

⚠️ **Palavra ambígua: "região do fornecedor".** São duas coisas diferentes: a
**cidade onde ele fica** (o endereço dele) e **até onde ele vende** (a
abrangência). Um fornecedor de São Paulo pode atender o Brasil inteiro, e um de
Fortaleza pode só entregar na Grande Fortaleza. Quem decide a cotação é a
segunda, nunca a primeira.

⚠️ **Palavra ambígua: "atende a região".** NACIONAL atende qualquer obra;
ESTADUAL, as obras nas UFs listadas; REGIONAL e LOCAL, só os municípios
listados. Quem está como NÃO INFORMADA **não atende ninguém** para efeito de
disparo — é falta de cadastro, não abrangência zero, e a resposta tem de dizer
isso com essas palavras.

⚠️ **Resposta muda conforme quem pergunta:** a lista de fornecedores por obra
respeita o escopo por obra — quem não alcança a obra não vê o cruzamento dela.

O que ainda **não** responde:

- FALTA **"qual fornecedor entrega em tal bairro / com tal prazo"** — o sistema
  guarda município, não bairro, e o prazo só existe depois de uma proposta.
- FALTA **"adotar o nome oficial de todos de uma vez"** — hoje é um a um, de
  propósito: cada troca é uma decisão sobre um fornecedor que alguém reconhece
  pelo nome antigo.
- FALTA **"o que mudou nos OUTROS cadastros"** (fornecedor, obra, colaborador)
  — a trilha guarda, mas só a tela de insumos lista. Vale repetir nas outras
  onde houver correção em linha.
- FALTA **"desfazer" para mais de uma alteração de uma vez** — hoje é uma por
  vez, e para o uso normal basta.

## 3t. A conta do credor: quem confere, e o que está parado esperando

Construído em 22/09/2026, depois de percorrer a cadeia inteira do ERP num banco
recém-criado — do cadastro do CNPJ até a conciliação. A homologação da conta do
credor existia como regra e não tinha porta nenhuma: num banco novo, nenhum
pagamento por Pix ou TED chegava ao fim, e nada na tela dizia por quê.

Perguntas que passam a funcionar:

- OK **Quais contas de credor estão esperando conferência?** — e há quanto tempo.
- OK **Quem cadastrou esta conta bancária?** — e quem a homologou depois.
- OK **Por que este título está bloqueado?** — quando é a conta, a resposta diz
  o que falta e quem pode liberar.
- OK **Que títulos estão parados esperando a conta do credor ser conferida?**
- OK **Este credor tem forma de pagamento cadastrada?**

⚠️ **Palavra ambígua: "conta do credor".** São duas coisas no mesmo nome: a
**conta bancária para onde o credor recebe** (é esta, que se homologa) e a
**conta do plano financeiro** em que a despesa dele é classificada. A resposta
tem de dizer qual das duas está respondendo.

⚠️ **Palavra ambígua: "liberar".** Também são duas: **homologar a conta**
(conferir o titular, o que destrava o título) e **aprovar o título** (liberar o
pagamento). Homologar NÃO aprova — são duas pessoas, de propósito.

⚠️ **Muda conforme quem pergunta.** Quem cadastrou a conta não pode homologá-la;
para essa pessoa a resposta tem de dizer que a conferência é de outra pessoa do
financeiro, e não só "não pode".

- FALTA **"quanto dinheiro está parado por causa de conta não conferida"** — dá
  para somar os títulos bloqueados por C2, mas ainda não existe a resposta
  pronta.
- FALTA **"avisar sozinho quem confere quando entra conta nova"** — hoje a fila
  aparece na tela de Pagamentos, e alguém precisa olhar.

## 3u. O elo que ainda falta: do pedido de compra ao título

Achado na mesma simulação de 22/09/2026, e é a maior lacuna que ela mostrou. O
pedido de compra gera a **previsão de pagamento** e o sistema até avisa —
*"Material recebido e a parcela ainda não virou título no financeiro"* —, mas
não existe caminho da previsão para o lançamento. Quem lança redigita tudo, e o
título nasce sem ligação com o pedido que o originou.

- FALTA **"lançar o título a partir do pedido de compra"** — o que hoje obriga a
  redigitar credor, valor, vencimento e obra.
- FALTA **"este pedido já virou título?"** — a previsão sabe, a tela do título
  não mostra.
- FALTA **"o que comprei e ainda não foi lançado no financeiro"** — a lista
  existe por pedido, não no conjunto.
- FALTA **"o valor pago bate com o pedido autorizado?"** — sem o elo, não há
  como comparar.

⚠️ **Cuidado com "pedido" na resposta.** O ERP tem DOIS: o **pedido de compra**
de Suprimentos (`pedidos_compra`, PC-0001) e o **pedido** antigo importado do
Pipefy/Omie (`pedidos`), que é o que o título guarda hoje. Responder um pelo
outro dá número errado com cara de certo.

## 3v. O contrato da obra: seguro, caução e prazo

Passaram a ser guardados de verdade em 22/09/2026 — apareciam na tela e nunca
eram gravados.

- OK **Qual o seguro-garantia desta obra, e até quando vale?**
- OK **Que obras estão com o seguro-garantia vencendo?**
- OK **Quanto foi retido de caução nesta obra?** — a porcentagem; o valor
  retido continua vindo das medições.
- OK **Qual o departamento desta obra no Omie?**

⚠️ **Palavra ambígua: "caução".** São duas: a **caução do contrato com o
cliente** (percentual que o órgão retém da BWS, este campo) e a **retenção de
garantia da empreita** (o que a BWS retém do empreiteiro). Trocar uma pela
outra inverte quem deve a quem.

- FALTA **"avisar quando o seguro-garantia estiver perto de vencer"** — a data
  agora existe; o aviso na agenda ainda não.

## 3w. O número que alguém mandou: que registro é esse?

Construído em 22/09/2026, junto com `/erp/ir/<numero>`. São as perguntas que
aparecem quando chega um número solto por WhatsApp.

- OK **Que registro é o número X?** — o sistema reconhece dez tipos pelo
  formato e diz qual é.
- OK **Abrir o registro de número X** — leva direto à tela dele.
- OK **Qual o link deste lançamento / desta obra / deste pedido?**

⚠️ **Palavra ambígua: "número".** No ERP são vários, e trocá-los dá resposta
errada com cara de certa: o **número da SP** (000123, o lançamento), o **número
do documento** (a nota fiscal do fornecedor), o **número do pedido de compra**
(PC-0001) e o **número interno** (o do banco, que ninguém vê). Quando alguém
diz só "o número", quase sempre é a SP.

⚠️ **Muda conforme quem pergunta.** O mesmo número pode abrir para uma pessoa e
não abrir para outra — e a resposta para quem não pode é a MESMA de número que
não existe, de propósito.

- FALTA **"achar pelo número da nota fiscal do fornecedor"** — hoje o atalho
  entende os números que o ERP gera, não os que vêm de fora.
- FALTA **medição** — ela é numerada dentro do contrato, e ainda não tem número
  único próprio. Para chegar nela, o caminho é o contrato.

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
