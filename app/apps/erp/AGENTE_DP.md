# O agente de DP — metodologia

Desenho de 05/11/2026, pedido dele com estas palavras:

> *"Há tempos eu queria criar um agente de IA. Imagina aí, eu solicito à
> contabilidade uma admissão, a gente precisa que eles deem um retorno em
> relação àquela admissão, normalmente através de uma documentação — ficha de
> cadastro; se for uma rescisão, é a documentação rescisória. E a gente vai
> precisar treinar o nosso agente para que ele compreenda aquelas informações,
> aquele retorno, e finalize aquele procedimento. (…) E o agente teria que fazer
> essas cobranças e analisar se está tudo certo."*

> *"A gente precisa poder ativar e desativar isso aí, que a gente não vai
> começar com ele ativo. A gente precisa definir esses parâmetros: o que é que
> analisa, o que é que não analisa, o que é esperado da contabilidade, qual é o
> tipo de documento."*

E, no mesmo dia: *"vá além em relação ao agente de DP. O que ele deveria fazer?
Quais rotinas ele deveria cuidar? Quais interações entre DP, Contabilidade e os
administrativos de obra que fazem as requisições ele deveria fazer, e como?"*

---

## 1. O PROBLEMA, dito sem o agente no meio

Hoje o trabalho do DP é **esperar e cobrar**. A obra pede, o DP confere, o DP
pede à contabilidade, e aí **alguém tem de lembrar** que pediu. O Pipefy não
lembrava: lembrar era função de uma pessoa olhando um quadro.

As três coisas que se perdem nesse esquema, e cada uma tem um custo medido:

| O que se perde | O custo |
|---|---|
| Requisição que chega faltando dado | O DP descobre ao abrir, e vai perguntar por WhatsApp. O pedido anda **um dia** por pergunta. |
| Pedido à contabilidade sem retorno | Ninguém sabe que está atrasado até alguém precisar do documento — normalmente **no dia do pagamento**. |
| Retorno que chega e ninguém confere | A ficha de registro vem com cargo ou salário diferente do combinado, e isso só aparece **no eSocial ou na rescisão**, meses depois. |

**O agente existe para cuidar dessas três.** Não para "ter IA".

---

## 2. OS TRÊS LADOS, e quem fala com quem

```
  ADMINISTRATIVO DE OBRA            DP                      CONTABILIDADE
  (supervisor, adm. de obra)     (quem resolve)              (fora da empresa)
          │                            │                            │
          │ 1. requisição              │                            │
          ├───────────── fila do ERP ──►                            │
          │                            │ 2. triagem + leitura       │
          │ ◄── devolve o que falta ───┤    dos documentos          │
          │                            │                            │
          │                            │ 3. pedido formal           │
          │                            ├──────── e-mail ───────────►│
          │                            │                            │
          │                            │ 4. cobrança no prazo       │
          │                            ├──────── e-mail ───────────►│
          │                            │                            │
          │                            │ 5. retorno com documento   │
          │                            │ ◄───── e-mail/anexo ───────┤
          │ ◄── aviso de concluído ────┤ 6. confere, arquiva,        │
          │                            │    finaliza                │
```

**O agente é o ERP fazendo os passos 2, 4 e 6 — e preparando o 3.** Os passos 1
e 5 são de gente; o 3 é do DP apertando um botão (ou do agente, quando ligado).

---

## 3. AS OITO ROTINAS

Cada uma liga e desliga **sozinha**, tem parâmetros próprios e **nasce
desligada**. Cada uma tem três modos:

| Modo | O que faz |
|---|---|
| `DESLIGADA` | nada. É como todas nascem. |
| `SUGERE` | faz a análise e **escreve o que faria**, para alguém aprovar com um clique. É o modo de confiança: dá para ligar sem medo e ver o agente acertando (ou não) por uma semana. |
| `FAZ` | executa. |

### R1 · Triagem da requisição que chega da obra

**Quando:** uma solicitação é aberta na fila do DP.

**O que analisa:**
- os campos obrigatórios **daquele tipo** vieram;
- o CPF é válido, e a pessoa existe (ou não existe, quando é pré-cadastro);
- **coerência**: desligamento de quem já está desligado; readmissão de quem está
  ativo; alteração de cargo para cargo que não existe; férias de quem tem menos
  de 12 meses de vínculo; salário pleiteado **abaixo do piso** da convenção;
  obra que não é a obra de quem pediu.

**O que faz:** devolve ao requerente com a frase exata do que falta, ou aprova a
entrada e põe na fila do DP com um resumo. **Não recusa por conta própria** nada
que seja decisão — só o que é dado faltando ou impossível.

**O que NÃO analisa:** mérito. Se vale a pena contratar, se o salário é justo, se
o desligamento é certo — isso é decisão de gente.

### R2 · Leitura dos documentos (é ela que encurta o formulário)

**Quando:** um documento é anexado ao cadastro de uma pessoa.

Esta é a rotina que realiza o pedido dele: *"a gente vai colocar o mínimo
possível de campos. Só os campos que não vão ter dentro de documentos — por
exemplo, qual vai ser o cargo, qual é a obra, qual é a empresa."*

**O que lê, de qual documento:**

| Documento | O que ele preenche |
|---|---|
| RG / CNH | nome, nascimento, filiação, RG, órgão, UF, emissão, naturalidade |
| CPF | CPF, nome, nascimento |
| CTPS (foto) | nome, nascimento, filiação |
| CTPS (informações) | nº, série, UF, emissão, PIS |
| Comprovante de endereço | CEP, logradouro, número, bairro, município, UF |
| Cartão CNPJ (prestador PJ) | razão social, CNPJ, endereço |

**O que faz:** preenche **em branco apenas** — nunca sobrescreve o que uma
pessoa digitou — e marca cada campo como "veio do documento, confira". O que ele
leu com pouca certeza vai destacado.

**O que fica SEMPRE de gente:** cargo, obra, empresa de registro, vínculo, tipo
de contrato, salário. Nenhum deles está em documento nenhum, e é esta a lista
que o formulário inicial precisa ter.

### R3 · Despachante da contabilidade

**Quando:** o cadastro entra na etapa *Na contabilidade*, ou o DP pede um dos
templates.

**O que analisa antes de mandar:** se o pedido tem **tudo o que a contabilidade
precisa** para fazer o ato. Pedido de admissão sem PIS volta para o DP em vez de
sair pela metade — porque o e-mail incompleto custa uma ida e volta de dois dias
com a contabilidade.

**O que faz:** monta o e-mail do template com os dados do cadastro, anexa os
papéis que aquele template exige, manda pela conta de e-mail da empresa, e
registra **o que se espera de volta** (tipo de documento e prazo).

### R4 · Cobrador de prazo

**Quando:** todo dia, uma vez.

**O que analisa:** cada pedido aberto contra o prazo do template.

**O que faz:**
1. **no vencimento** — cobra por e-mail, citando o pedido e a data;
2. **na segunda cobrança** (parâmetro, por padrão 3 dias depois) — cobra de novo
   e avisa o DP pela mensageria;
3. **na escalada** (padrão: 3 cobranças) — para de cobrar e avisa o dono. Cobrar
   para sempre é ruído; parar sem avisar é esquecer.

**O que NÃO faz:** cobrar no fim de semana, cobrar duas vezes no mesmo dia, e
cobrar pedido que já teve retorno mas está esperando conferência — a bola está
com a gente nesse caso, e cobrar quem já respondeu queima a relação.

### R8 · Caixa de entrada (construída em 05/11/2026)

**Quando:** a cada rodada do agente.

**O que faz:** lê as mensagens não lidas, casa cada uma com o pedido que a
originou, baixa os anexos e arquiva no Drive com o tipo esperado. Detalhe inteiro
na §6. **Não usa IA:** casar resposta com pedido é cabeçalho de e-mail e
expressão regular.

**Ela vem ANTES da cobrança na rodada**, de propósito: o retorno que chegou hoje
tem de ser casado antes de a cobrança olhar os prazos — senão o agente cobra um
pedido que já foi respondido, e cobrar quem já respondeu queima a relação.

### R5 · Conferente do retorno

**Quando:** um documento é anexado a um pedido (pelo DP, ou pela leitura da
caixa de e-mail, se ela for ligada — ver §6).

**O que analisa:** se o documento é o **esperado** daquele pedido, e se os dados
dentro dele **batem com o cadastro**: nome, CPF, cargo, salário, data de
admissão, data de saída, CNPJ da empresa de registro.

**O que faz:**
- **bate tudo** → arquiva no Drive com o tipo certo, marca o pedido como
  concluído e move a etapa do cadastro. É o *"finalize aquele procedimento"*
  dele;
- **não bate** → abre **divergência** com o de/para campo por campo, e não
  conclui nada. Uma ficha de registro com salário diferente do combinado é um
  problema que custa caro meses depois, e é exatamente o que ninguém pega hoje;
- **não dá para ler** → diz isso, e deixa para o DP conferir à mão. Nunca chuta.

### R6 · Vigia das pendências

**Quando:** todo dia.

**O que analisa:** o cadastro inteiro, procurando o que envelheceu —
cadastro parado na mesma etapa além do prazo; papel que falta para admitir; ASO
e certificado de NR vencendo; admissão de CTPS sem ficha de registro de volta;
desligamento sem documentação rescisória; pessoa ativa sem obra; salário abaixo
do piso depois de um dissídio.

**O que faz:** uma lista, uma vez por dia, para o DP. **Não cobra ninguém** por
aqui — quem cobra é a R4, e só o que tem pedido aberto.

### R7 · Relator

**Quando:** uma vez por dia para o DP, uma vez por semana para o dono.

**O que faz:** o resumo em português do que aconteceu: entraram X requisições,
saíram Y, estão atrasados Z pedidos (com nome e dias), e **com quem está a
bola** em cada um. Nada de número solto: cada linha diz o que fazer.

---

## 4. OS PARÂMETROS — e é aqui que a decisão fica com ele

Por **rotina**: modo (desligada / sugere / faz) e o horário em que roda.

Por **template de pedido à contabilidade**:

| Parâmetro | Para que serve | Padrão que eu propus |
|---|---|---|
| destinatários | para quem vai o e-mail | o e-mail da contabilidade, em Configurações |
| prazo | dias úteis até a primeira cobrança | 2 dias (5 para rescisão) |
| documento esperado | o tipo do catálogo que fecha o pedido | ficha de registro, TRCT, aviso de férias… |
| cobranças | quantas, e de quantos em quantos dias | 3, a cada 3 dias |
| escalada | quem recebe quando as cobranças acabam | o dono |
| o que conferir no retorno | a lista de campos que têm de bater | nome, CPF, cargo, salário, datas |
| pode concluir sozinho | se o agente fecha o procedimento ou só sugere | **não** — nasce sugerindo |

**Nenhum desses padrões é uma decisão minha definitiva.** Eles existem para o
sistema funcionar no primeiro dia sem ninguém preencher trinta campos. Todos
mudam na tela.

---

## 5. O QUE O AGENTE NUNCA FAZ

Esta lista é a parte mais importante do desenho, porque é ela que torna possível
ligar o agente sem medo:

1. **Não desliga ninguém, não admite ninguém, não muda salário nem cargo** por
   conta própria. Essas quatro coisas são atos com efeito em dinheiro e em
   direito, e exigem uma pessoa.
2. **Não manda e-mail para fora sem um pedido registrado antes.** Todo e-mail
   tem um pedido como origem, e o pedido tem um autor humano.
3. **Não conclui procedimento com divergência.** Se o documento não bate, abre
   divergência e para.
4. **Não apaga nem sobrescreve** o que uma pessoa digitou. Preenche o vazio.
5. **Não inventa.** Campo que ele não leu com certeza fica em branco e marcado,
   nunca chutado.
6. **Não age fora do que está ligado.** Rotina desligada não roda nem "só para
   avisar".
7. **Tudo o que ele faz fica na trilha com o nome dele** — "feito pelo agente",
   com a hora e o motivo. Quem audita precisa poder separar o que foi gente do
   que foi máquina.

---

## 6. A CAIXA DE ENTRADA — resolvido, e eu estava errado

Eu havia registrado aqui um "ponto cego": o ERP manda e-mail mas não recebe, e
alguém do DP teria de jogar o anexo na ficha. Ele derrubou isso no mesmo dia:

> *"Isso é fundamental. Que agente é esse que depende de uma intervenção
> humana? Aí não é agente. Todo e-mail que chegar, vou ter que jogar os anexos?
> Não tem sentido nenhum. Então o ERP precisa estar fazendo a leitura das caixas
> de entrada, baixando anexo de forma completamente autônoma para saber o que
> fazer."*

Ele está certo, e o custo que eu tinha apontado não existia: **`imaplib` e
`email` são da biblioteca padrão do Python** — nenhuma biblioteca nova entra. E
a credencial **não é variável de ambiente**: a senha fica cifrada no banco (a
mesma mecânica da senha de SMTP das empresas) e é configurada na tela, então
ligar não pede deploy.

### Como a resposta é casada com o pedido, em três tentativas

| Tentativa | O que usa | Por que nesta ordem |
|---|---|---|
| 1 | `In-Reply-To` / `References` contra o `Message-ID` que o ERP gerou ao enviar | é o casamento certo: não depende de ninguém manter nada intacto |
| 2 | a marca `[CT-00001]` no assunto | o cliente de e-mail do outro lado às vezes não devolve o cabeçalho |
| 3 | o CPF no assunto ou no corpo | só vale com **um** pedido aberto para aquela pessoa |

**Com mais de um pedido aberto da mesma pessoa, ele NÃO escolhe.** Anexar a
rescisão no pedido de férias é pior que não anexar: a mensagem fica como "sem
dono", com o motivo escrito, e vira uma linha na tela para alguém resolver.

### O que ela faz com o que acha

Baixa os anexos, **arquiva no Google Drive com o tipo que o pedido esperava** (o
tipo vem do pedido, não do nome do arquivo: "doc1.pdf" não diz nada) e entrega o
retorno ao pedido, que segue para a conferência.

**O que ela nunca faz:** apagar mensagem, responder sozinha, arquivar anexo em
pedido que não casou com certeza, e arquivar assinatura de imagem, ícone de
rodapé ou logotipo — que vêm como anexo em todo e-mail corporativo e encheriam
o Drive de lixo com cara de documento.

**Resposta de texto sem anexo não fecha nada.** "Estamos providenciando" é
registrado na trilha do pedido e o prazo continua correndo — tratar isso como
retorno fecharia o ciclo cedo, e é o tipo de erro que só aparece quando alguém
precisa do documento.

### O limite que fica

Mensagem sem anexo e sem casamento é **uma linha na tela com o motivo**, não um
palpite. É aqui que a ampliação que ele pediu acontece: *"cada situação, a gente
vai determinar certinho o que é esperado, que tipo de informação é esperada, o
que vai ser analisado"*. Cada situação nova é uma entrada em `TEMPLATES` —
documento esperado, prazo e campos a conferir — e a caixa passa a saber lidar
com ela sem código novo.

---

## 6-bis. OS CANAIS, decididos por ele em 05/11/2026

| Com quem | Canal | Por quê |
|---|---|---|
| **Contabilidade** | e-mail, nos dois sentidos | é como o trabalho dela já acontece, e e-mail tem anexo e rastro |
| **Administrativo de obra** | **WhatsApp**, para avisar e cobrar | *"a interação entre os requisitores de informação vai ser feita via WhatsApp, que é o pessoal administrativo de obra"*. A obra não tem Telegram |
| **Devolução de documento pela obra** | **o sistema, nunca o WhatsApp** | *"essa devolução não pode ser via WhatsApp. Tem que ser via sistema."* Foto de documento em conversa não é arquivo: é foto perdida num celular, sem tipo, sem dono e sem prazo |

**O fluxo que ele desenhou em voz alta, e que está construído:**

```
contabilidade devolve o TRCT  →  ERP arquiva (caixa de entrada)
                              →  DP abre uma DEVOLUÇÃO para a obra, com prazo
                              →  WhatsApp AVISA a obra (e cobra, se vencer)
                              →  a obra colhe as assinaturas
                              →  devolve o arquivo PELO PORTAL
                              →  arquivado no cadastro da pessoa, com tipo
```

---

## 6-ter. A PORTA DE QUEM PEDE — refeita em 07/11/2026

⚠️ **Esta seção foi reescrita por inteiro. O que estava aqui antes estava
errado, e vale guardar o erro: ele é instrutivo.**

**O que eu tinha escrito:** que o portal já existia e era o aplicativo do
ponto, e que bastava fazer uma ponte da sessão do ponto para dentro do ERP.
Construí assim, publiquei, e o dono olhou:

> *"eu entrei aqui no portal e acho que você não compreendeu bem como é que é a
> ideia (…) como é que funciona hoje? Eles têm um portal do Pipefy e lá eles
> fazem todo tipo de solicitações. Financeiro, suprimento, diário de obras,
> cadastro colaboradores, solicitação do departamento pessoal e etc. Só que o
> Pipe é só um formulário de entrada de dados. Por lá não se gerencia nada
> (…) a solução nem pode ser somente como Pipefy (…) o ponto fica só para
> ponto, e o ERP a gente utiliza um outro acesso mais completo, mais robusto.
> Mas aí, se eu quiser fazer essas solicitações rápidas (…) a gente faz
> acessando o ERP mesmo. O cara vai ter o login, acesso do ERP e vai fazer a
> requisição dele. Aí, só que num ambiente, digamos, mais enxuto para utilizar
> no celular."*

### Os dois erros, e eles são do mesmo tipo

1. **Eu recriei o defeito do Pipefy.** O que torna o Pipefy insuficiente não é
   o formulário — é que o formulário mora num sistema e a gestão mora em
   outro. Um portal separado faz isso de novo: um lugar para pedir, outro para
   acompanhar. E a obra, quando quiser *"entender melhor o que já foi
   solicitado"*, cai no mesmo vazio de hoje.
2. **Solicitação não é assunto do DP.** A obra pede de tudo — pagamento,
   material, diário de obra, colaborador, DP. Um portal de DP ou vira quatro
   portais, ou vira um ERP paralelo pela metade.

O erro comum aos dois: **eu otimizei a entrada e esqueci o acompanhamento.**
Formulário é a parte fácil; o que falta na empresa é saber em que pé está o
que foi pedido.

### O desenho que ficou: uma porta, duas caras

| | Quem entra | Como | O que decide |
|---|---|---|---|
| **Ponto** | gente de obra, para bater ponto | CPF + PIN | papéis do ponto |
| **ERP** | TODO o resto, da diretoria ao parceiro | e-mail e senha | perfil → ações → escopo (`core/auth/`) |

**O ponto ficou só para o ponto.** Tudo o mais entra pelo ERP, com UM login e
UM sistema de permissão. Gente de fora e parceiro entram pela mesma porta;
muda só a permissão.

**A tela enxuta é `/erp/portal`** — rota comum do ERP, com `@login_obrigatorio`
e ação declarada (`ver_erp` para abrir, `solicitar_dp` para pedir). Ela não usa
o `erp_base.html`, porque o menu de 18 telas num celular, para quem só quer
fazer um pedido, é o oposto de "ambiente mais enxuto". Quem quiser o ERP
inteiro vai nele — está a um endereço de distância, com o mesmo login.

### O que isso APAGOU de código, e é o melhor da mudança

A ponte com o ponto levava junto uma segunda regra de alcance e uma segunda
marcação de autoridade (`papeis.obras_recentes`, `papeis.e_administrativo`).
As duas sumiram. O alcance agora é `obras_do_usuario`, o mesmo do resto do
ERP; a autoridade para pedir por outros é a ação `solicitar_dp`, que já
existia. **Nenhuma ação nova foi criada** — e o `PARCEIRO` já estava de fora
dela, então a decisão do dono (*"Parceria, a princípio, é só visualização"*)
já era a regra em vigor, sem uma linha de código.

Duas regras de permissão em paralelo divergem no primeiro mês. Agora há uma.

### O que fica pendente, e depende dele

O Pipefy **sai de uma vez, sem paralelo** (decisão dele, 07/11). Isso quer
dizer que, antes do corte, os tipos de pedido das OUTRAS áreas — financeiro,
suprimentos, diário de obra — precisam existir aqui. Hoje só os do DP existem.
O que falta saber, e só ele tem: **quais são os tipos de pedido do portal do
Pipefy e quais a obra mais usa.** Os exports que chegaram até aqui são só os
pipes do DP.

## 7. A ORDEM DE CONSTRUÇÃO, e por que esta

1. **Os pedidos à contabilidade com prazo e cobrança** (R3 e R4), que são
   determinísticos: funcionam **sem IA nenhuma** e já resolvem o custo que mais
   dói, que é o pedido esquecido;
2. **A triagem** (R1), que também é determinística — é regra de negócio escrita,
   não adivinhação;
3. **O vigia e o relator** (R6 e R7), que são consulta;
4. **A leitura dos documentos** (R2), que usa a leitura que o ERP já tem;
5. **A conferência do retorno** (R5), que é a única que precisa de verdade de
   entender um documento livre — e por isso é a última, e a que nasce em modo
   *sugere*.

**O que isso quer dizer na prática: SEIS DAS OITO ROTINAS não dependem de IA
nenhuma.** Triagem, despacho, cobrança, caixa de entrada, vigia e relator são
regra de negócio, cabeçalho de e-mail e conta de prazo. Só duas precisam
entender documento livre: a leitura (R2) e a conferência do retorno (R5).

O agente começa útil e burro, e fica inteligente depois — em vez de nascer
inteligente e errado. E se a parte de IA nunca ficar boa, as cinco primeiras
continuam valendo sozinhas: o pedido esquecido, que é o custo que mais dói, é
resolvido por conta de calendário, não por inteligência.
