# Como o agente de DP trabalha — explicação para conferir

> Pedido dele em 17/11/2026: *"Como é que a gente vai trabalhar com o agente? O
> que é que já tem pronto, o que é que falta ajustar? (…) eu quero mandar um
> cadastro para assinar. Como é que isso vai? Como é que vão essas informações
> via e-mail? (…) Era interessante gerar uma espécie de explicativo do
> funcionamento do agente, com o que ele está fazendo, o que ele envia, como
> envia, como cobra, o prazo que cobra, tudo que ele faz."*

Este arquivo responde isso. É para **conferir**, não para programar — a
metodologia e o porquê de cada decisão estão em `AGENTE_DP.md`.

---

## A coisa mais importante, antes de tudo

**O agente nasce DESLIGADO. Todas as oito rotinas.** Cada uma tem três
posições, e a do meio é o que torna isto seguro:

| Posição | O que ela faz |
|---|---|
| **Desligada** | não roda. É como todas nascem |
| **Sugere** | analisa e escreve o que FARIA, para alguém aprovar com um clique |
| **Faz** | executa sozinha |

A ideia é ligar em "Sugere", olhar uma semana, e só então promover para "Faz" —
rotina por rotina. **Nada vira automático sem você ver antes o que seria feito.**

Onde se liga: **ERP › Pessoal › Agente**.

---

## O que o agente NUNCA faz, ligado ou não

1. Não desliga, não admite, não muda salário nem cargo por conta própria.
2. Não manda e-mail para fora **sem um pedido registrado antes, com autor**.
3. Não conclui procedimento com divergência.
4. Não sobrescreve o que uma pessoa digitou — só preenche o que está vazio.
5. Não inventa: o que ele não leu com certeza fica em branco e marcado.
6. Não age fora do que está ligado.
7. **Tudo o que ele faz fica na trilha com a marca "pelo agente".**

Isso está escrito no código, não só aqui — regra que só vive em documento não é
regra.

---

## As oito rotinas, e quatro delas não usam IA nenhuma

| # | Rotina | O que faz | Usa IA? |
|---|---|---|---|
| 1 | **Triagem** | confere a requisição que chega da obra: campos obrigatórios, CPF válido, pessoa existe, coerência (desligar quem já saiu, férias de quem tem menos de 12 meses, salário abaixo do piso) | não |
| 2 | **Leitura** | lê os documentos anexados e preenche o cadastro | **sim** |
| 3 | **Despacho** | monta e manda o pedido para a contabilidade | não |
| 4 | **Cobrança** | cobra o prazo vencido | não |
| 5 | **Caixa de entrada** | lê a resposta da contabilidade, baixa os anexos e arquiva | **sim** (para casar a resposta com o pedido) |
| 6 | **Conferência** | confere se o documento que voltou bate com o que foi pedido | **sim** |
| 7 | **Vigia** | avisa sobre cadastro parado além do prazo e papel que falta | não |
| 8 | **Relator** | o resumo do que aconteceu | não |

Quatro sem IA é de propósito: **o agente começa útil e burro, e fica inteligente
depois** — em vez de nascer inteligente e errado.

---

## "Quero mandar um cadastro para assinar" — como isso vai

É o **pedido de Assinatura de CTPS**, e o caminho é este:

```
Ficha da pessoa → "Pedir à contabilidade" → escolhe o tipo
   → o ERP monta o e-mail com os dados do cadastro
   → você confere o texto na tela
   → envia pela conta de e-mail da empresa
   → o prazo começa a contar
```

### O que vai no e-mail, exatamente

**Cabeçalho (vai em todos os tipos):** nome, CPF, data de nascimento, PIS/NIS,
CTPS (número e série), empresa de registro, obra (código e nome), cargo,
convenção, salário e data de admissão.

**Miolo da assinatura de CTPS:** a frase *"Solicitamos o registro formal da
admissão abaixo no eSocial"*, mais o **tipo de contrato** e o **contrato de
experiência**.

**Nada é digitado à mão.** Tudo sai do cadastro — é por isso que cadastro
incompleto trava o pedido antes de sair.

### O que o cadastro PRECISA ter, senão o pedido não sai

Para a assinatura de CTPS: **nome, CPF, data de nascimento, PIS, cargo, salário,
admissão e empresa**.

Falta um? O pedido volta para o DP em vez de sair pela metade. O motivo é
prático: e-mail incompleto custa dois dias de ida e volta com a contabilidade.

### Para onde vai, e por qual conta

- **Para**: o e-mail da contabilidade, cadastrado em **Pessoal › Contabilidade**,
  no bloco "Para onde vão os pedidos". Sem ele, o envio é recusado com essa
  frase na tela.
- **De**: a conta de e-mail **da empresa de registro** da pessoa. Cada empresa
  tem a sua.

### Se o e-mail não sair

O pedido **fica em rascunho com o erro escrito**, e nada é dado como enviado.
Pedido que "sumiu" por servidor fora do ar seria pior que pedido que não saiu.

---

## Os sete tipos de pedido, com prazo e cobrança

| Tipo | O que volta | 1ª cobrança | Cobra até | De quantos em quantos dias |
|---|---|---|---|---|
| **Assinatura de CTPS** (admissão) | Ficha de registro | 2 dias úteis | 3× | 3 dias |
| **Alteração de cargo e salário** | Alteração de contrato | 3 dias úteis | 3× | 3 dias |
| **Férias** | Aviso de férias | 3 dias úteis | 3× | 3 dias |
| **Dependente** | Alteração de contrato | 3 dias úteis | 2× | 5 dias |
| **Cônjuge** | Alteração de contrato | 3 dias úteis | 2× | 5 dias |
| **Desligamento** | Documentação rescisória | 5 dias úteis | 3× | 3 dias |
| **Cancelamento de desligamento** | Alteração de contrato | **1 dia** | 3× | 1 dia |

O prazo do cancelamento é curto de propósito: cancelamento que chega depois do
processamento da rescisão não cancela nada — virou acerto.

**A documentação rescisória não é um pedido separado**, e foi você quem disse
por quê: *"tem solicitação de documentação rescisória, mas isso fica dentro do
processo de demissão."* Ela é o retorno esperado do desligamento — e por isso o
desligamento só fecha quando ela chega.

---

## Como a cobrança é feita

Vencido o prazo, sai um e-mail com o assunto **"[2ª cobrança] " + o assunto
original**, dizendo a data do pedido, qual era o prazo, **qual documento ainda
não chegou**, e **repetindo o pedido original inteiro embaixo** — para a pessoa
do outro lado não precisar procurar o e-mail antigo.

Depois de cada cobrança o relógio reinicia pelo intervalo da tabela acima.

---

## Quando a resposta chega

A caixa de entrada tenta casar a resposta com o pedido de três formas: pelo
identificador da conversa, pelo assunto e pelo CPF. Achando, **baixa os anexos e
arquiva**. Depois a conferência compara o documento com o que foi pedido:

| Tipo | O que é conferido |
|---|---|
| Ficha de registro | nome, CPF, cargo, salário, admissão |
| Alteração de contrato | nome, CPF, cargo, salário |
| Rescisão | nome, CPF, admissão, data de saída |

**Bateu** → o pedido fecha, e nos dois casos em que isso muda a fila a etapa anda
sozinha (admissão → Concluído; desligamento → Desligado).
**Não bateu** → fica **divergente**, e espera gente. O agente não conclui nada
com divergência.

---

## O que FALTA — e é honesto dizer

1. **Nunca rodou em produção.** Tudo isto está construído e testado, e nenhuma
   rotina foi ligada de verdade ainda. O primeiro passo é ligar **uma** em
   "Sugere".
2. **O e-mail da contabilidade precisa estar cadastrado**, e a conta de envio de
   cada empresa também. Sem isso nada sai.
3. **Não há assinatura eletrônica.** "Mandar para assinar" aqui quer dizer
   *pedir à contabilidade que faça o registro*. Contrato de PJ para assinar é a
   etapa **"Assinar"** do cadastro, e hoje o ERP só a controla — quem gera e
   coleta a assinatura é gente.
4. **A leitura de documentos usa IA e custa.** Ela é a única rotina que tem
   custo por uso.

---

## O que eu sugiro, se você quiser começar amanhã

1. Cadastre o e-mail da contabilidade e confira a conta de envio das empresas.
2. Ligue **só a Triagem**, em "Sugere". Ela não manda nada para fora — só
   confere o que chega da obra.
3. Uma semana depois, ligue a **Cobrança** em "Sugere" e veja as cobranças que
   ela proporia.
4. Só então promova para "Faz", uma de cada vez.

**Despacho e Caixa de entrada por último**: são as duas que falam com gente de
fora da empresa.
