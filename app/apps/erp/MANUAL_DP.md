# O DP no ERP — como está funcionando, para você criticar antes de usar

Escrito em 09/11/2026, a pedido dele:

> *"Eu quero entender o que foi que você depreendeu de tudo aquilo e como é
> que está funcionando a parte das fases (…) como é que a gente está enviando
> essas informações para a contabilidade, como é que estão saindo esses
> e-mails (…) o que é que a gente precisa fazer para deixar os e-mails ativos
> e funcionando, como é que eu faço para testar (…) até para eu já fazer
> alguma crítica antes de colocar em prática."*

**Este arquivo é para LER, não para programar.** Ele diz o que o sistema faz
hoje, com quais campos, quem faz o quê, o que precisa ser ligado e como
testar. No fim há **a lista do que eu assumi e você precisa confirmar ou
derrubar** — é a parte que mais vale o seu tempo.

Os outros arquivos da área: `CADASTRO_DE_PESSOAS.md` (o apurado do Pipefy e do
Make), `AGENTE_DP.md` (o desenho do agente) e `HISTORICO.md` (as decisões, com
data).

---

## 1. O RESUMO EM UMA PÁGINA

Três coisas, e elas são separadas de propósito:

| | O que é | Onde fica |
|---|---|---|
| **O CADASTRO** | quem trabalha na empresa, com os dados e os papéis | Pessoal › Colaboradores |
| **A FILA DO DP** | o que a obra pede ao DP (11 tipos) | Pessoal › Solicitações |
| **OS PEDIDOS À CONTABILIDADE** | o que o DP pede para fora (7 tipos), por e-mail, com prazo e cobrança | Pessoal › Contabilidade |

E uma quarta, que liga as três: **as ETAPAS** do cadastro, que dizem *o que
falta fazer* em cada pessoa.

**O princípio que mandou em tudo** (ditado por você em 05/11): *"a gente vai
colocar o mínimo possível de campos. Só os campos que não vão ter dentro de
documentos."* Daí a divisão: o que está em documento, o documento preenche; o
que não está em documento nenhum — cargo, obra, empresa, vínculo, salário —
alguém digita.

---

## 2. O CADASTRO DE COLABORADOR

### 2.1 Não existe pré-cadastro — isso foi corrigido hoje

Você: *"você colocou lá pré-cadastro colaborador, não existe esse
pré-cadastro, existe o cadastro, ponto final. O cadastro já tem que vir com
documentação e tudo mais."*

Estava errado e foi trocado. O tipo da fila chamava-se *Pré-cadastro* e dizia
"a obra manda o nome e o CPF; o DP completa".

⚠️ **E eu errei duas vezes, não uma.** Escrevi aqui que a ideia vinha do
Pipefy — não vinha. Ele corrigiu em 10/11/2026: *"nunca existiu pré-cadastro.
O pipe de cadastro de colaboradores é completíssimo, ele tem todas as
informações."* **O pré-cadastro foi invenção minha.** Fica registrado porque o
erro é instrutivo: eu olhei um pipe com 7 fases, vi que a primeira se chamava
"Cadastro Inicial" e concluí que o cadastro entrava pela metade. O nome da
fase descreve **em que pé está a conferência**, não quanta informação chegou.

**A diferença não é de palavra, é de quem faz o quê:** no pré-cadastro o DP
ficava perseguindo documento por WhatsApp, que é justamente o trabalho que
esta fila existe para acabar. Agora o tipo é **Cadastro de colaborador**, a
obra abre **com os dados e os papéis**, e o DP **confere**.

### 2.2 O que é obrigatório — e são duas listas diferentes

**O cadastro SALVA com duas coisas só: nome e CPF.** Isso é de propósito: se
exigisse tudo na primeira tela, ninguém conseguiria começar e todo mundo
preencheria campo com "0" para a tela deixar passar — foi o que acontecia no
Pipefy.

O que existe no lugar da obrigatoriedade é **a lista do que falta para
ADMITIR**, que aparece no alto da ficha e é a mesma lista que o DP persegue:

| Para admitir, falta se não tiver |
|---|
| data de nascimento · nome da mãe · nº da identidade · órgão emissor |
| nº do PIS · nº da CTPS *(só para CTPS; prestador não precisa)* |
| endereço · município · UF · CEP · telefone |
| cargo · obra · data de admissão · salário |

**E a lista dos PAPÉIS, que é outra conversa:**

| Papel | De quem |
|---|---|
| documento de identificação (RG, CNH ou CPF) | todos |
| comprovante de endereço | todos |

Campo em branco é digitação que falta; papel em branco é documento que ninguém
pediu à pessoa. Por isso os dois avisos são separados na tela.

**A lista encolheu em 10/11/2026, por decisão sua** — e as três saídas ficam
registradas para ninguém recolocá-las achando que foi descuido:

| Saiu | Por quê, nas suas palavras |
|---|---|
| **ASO** | *"esse seria o correto, mas não. Não vamos colocar ele como obrigatório ainda."* |
| **Carteira de trabalho** | *"para quem não é prestador não fica mais obrigatório."* |
| **Foto do colaborador** | *"a foto vai ser alimentada pelo ponto."* |

⚠️ **Sair da lista não é sumir:** os três continuam no catálogo do Arquivo,
continuam podendo ser anexados e continuam aparecendo na ficha. O que mudou é
que a ficha **não diz mais "falta isto para admitir"** por causa deles.
Obrigatoriedade que a empresa não pratica vira aviso que todo mundo aprende a
ignorar — e aí o aviso de verdade some junto.

### 2.2-bis A FOTO VEM DO PONTO, e ninguém anexa nada

Você: *"a primeira foto do ponto que ele começar a usar, ele coloca como
cadastro, que isso aí facilita. A gente já vai ter uma imagem no documento que
foi encaminhado."*

**Feito.** A ficha do colaborador mostra a foto da pessoa, buscada no ponto,
nesta ordem:

1. a foto que alguém **escolheu** como cadastral na tela do ponto;
2. não havendo escolha, **a primeira foto que a pessoa bateu**.

Escolha de gente vence o automático: quem foi lá e marcou uma foto melhor fez
isso por um motivo.

⚠️ **O que eu NÃO fiz, e é de propósito:** gravar essa escolha dentro do
ponto. O ERP só **lê** — fixar a foto cadastral lá é mudança na área do ponto,
que tem o chat dela. Do seu lado não muda nada: a foto aparece igual. Se você
quiser que fique gravado lá também, é um pedido para aquele chat.

### 2.3 Os campos que existem, por grupo

São 83 colunas. Por grupo, para você ver se sobra ou falta:

- **Identificação:** nome, CPF, data de nascimento, sexo, estado civil,
  escolaridade, naturalidade e UF, nome da mãe, **nome do pai**, RG + órgão +
  UF + emissão, PIS, CTPS (número, série, UF, emissão, física/digital),
  primeiro emprego.
- **Endereço:** CEP, logradouro, número, complemento, bairro, município, UF.
- **Contato:** telefone, e-mail.
- **Vínculo:** CTPS ou prestador, tipo de contrato (9 opções), empresa do
  grupo, CNPJ de admissão, razão social e CNPJ do PJ, contrato de experiência,
  cargo, convenção, matrícula, obra, admissão, data de início.
- **Pagamento:** salário, mensalista ou diarista, valor da diária, valor do
  mês, chave Pix (tipo e valor), banco, agência, conta.
- **Benefícios:** categoria e valor do auxílio-alimentação, categoria e valor
  do auxílio-transporte, meio de transporte, gratificação, parcela única,
  recebe via SP, cesta básica extra, vale-refeição, adicional de
  transferência, observação da ajuda de custo, salário-família.
- **Família:** dependentes **sem teto** (o Pipefy parava em 5 filhos), com
  parentesco, CPF, nascimento e as marcas de salário-família e IR.
- **Desligamento:** aviso prévio, último dia, data de saída, forma.
- **Controle:** situação (ativo/desligado), etapa, requerente, responsável.

### 2.3-bis OS CAMPOS DE PAGAMENTO NÃO SÃO SUPÉRFLUOS — eu estava errado

Eu tinha proposto cortar *cesta básica extra*, *adicional de transferência*,
*parcela única*, *recebe via SP* e *id_fortes*. **Você desfez, e explicou por
quê.** Fica registrado porque o motivo vale mais que a lista:

> *"Tem muita coluna no Pipe porque na planilha tem uma série de informações
> que não têm nada a ver, apenas para a movimentação dentro do processo — às
> vezes são campos temporários. O que é o importante? São os campos
> cadastrais. Aí tem campos que refletem no pagamento, que não são
> obrigatórios, são opcionais, que a pessoa pode não ter."*

**A régua, então, não é "quantos campos" — é QUEM USA O CAMPO.** Campo que
alimenta um cálculo de pagamento fica, mesmo que só 20 pessoas o preencham.
Campo que existia para mover card no Pipefy sai.

### 2.3-ter COMO O CADASTRO VIRA PAGAMENTO — o que li no Análise de SPs

Você mandou: *"você verifica na Análise de SPs o que é necessário lá para a
gente poder trabalhar aqui."* Fui ler. **São SEIS folhas diferentes**, e cada
uma puxa campos distintos do cadastro:

| A folha | Quem entra | Os campos do cadastro que ela usa |
|---|---|---|
| **Folha da contabilidade** (CTPS) | vínculo CTPS | salário, admissão, cargo, convenção, **ID Fortes** |
| **Diaristas** | quem tem dia de diária no mês | **valor da diária**, data de início, data de admissão, obra, função |
| **Auxílio-alimentação** | quem tem categoria preenchida | **categoria** (Mês / Mensal / Seg-Sex / Seg-Qui) + **valor** |
| **Auxílio-transporte** | idem, menos quem tem "Cartão" | **categoria** (9 opções, incluindo Diário e Vale Transporte) + **valor** |
| **Gratificados e mensalistas** | tipo de contrato PJ / Pró-labore / Autônomo Mensalista / Estágio, **ou** marcado "recebe gratificação" | **valor da gratificação**, **valor do mês**, **parcela única**, obra |
| **Pagamento via SP** | marcado **"recebe via SP"** | o mesmo, mas gera **uma SP por pessoa**, com rateio |

**Três coisas que eu não sabia e que mudam o desenho:**

1. **O vínculo é decidido POR DIA, não por pessoa.** A mesma pessoa tem dias
   de diária e dias de CTPS no mês em que foi registrada: do começo até a
   admissão é diária; do registro em diante é CTPS. É por isso que **data de
   início e data de admissão** são campos diferentes e os dois importam.
2. **"Cartão" no transporte quer dizer NÃO PAGAR em dinheiro.** Foi a origem
   de um pagamento em dobro. O ERP manda a palavra inteira para a folha, de
   propósito.
3. **O autônomo mensalista não entra em folha nenhuma:** gera um registro de
   pagamento, como PJ — exatamente como você descreveu.

### 2.3-quater O ID FORTES: eu chamei de lixo, e é o contrário

Você: *"o ID Fortes, na verdade, é o registro do colaborador na
contabilidade. Na folha da contabilidade não tem CPF: ele utiliza o ID e o
nome da pessoa. Já na nossa, o documento principal é o CPF. Se houver um
homônimo na folha, vai ser distinguido através do ID."*

**É a CHAVE que liga os dois mundos**, e o Análise de SPs já depende dela: a
Folha Sintética da contabilidade traz código e nome, e tudo do nosso lado é
por CPF. Sem o de/para, gente cadastrada aparece na folha como "fora do
cadastro", sem pagamento.

O campo **já existe** no cadastro do ERP e **já sai** na leitura para a folha.
O que falta é o que você mesmo apontou: **a atualização por arquivo** — pegar
o arquivo da contabilidade de tempos em tempos e atualizar os códigos em
lote. Isso eu não construí, e é um pedido seu quando quiser.

### 2.3-quinquies O VALOR DA DIÁRIA, e os acréscimos

Você: *"o pessoal que é diarista, o valor da diária é importante para o
cálculo. A gente coloca uns acréscimos dos fins de semana e feriados. Outras
pessoas que são CTPS, mas que às vezes trabalham no fim de semana, às vezes a
gente define o valor da diária também. Porém, atualmente a gente está
preferindo pagar hora extra, que é o correto."*

O Análise de SPs hoje **sabe dizer quem tem dia de diária e quantos**, mas
**não sabe quanto pagar** — porque o valor da diária não vinha da planilha que
ele lê. **No ERP esse campo existe** (`valor_diaria`) e já sai na leitura para
a folha. Os acréscimos que estão escritos lá como regra: **+20 em feriado, +10
no sábado, +20 no domingo, e vigia fora**. ⚠️ Confirme se esses números ainda
valem — eles vêm de uma fórmula antiga.

⚠️ **Aqui é onde eu mais posso ter errado**, e é o que eu queria que você
olhasse: *"quanto menos informação desnecessária, melhor."* Esta lista veio
dos 51 campos do pipe mais as 74 colunas da planilha. **Qualquer campo daqui
que você disser que não serve, eu tiro** — agora sabendo que campo de
pagamento não é campo supérfluo.

### 2.4 As etapas — "o que falta fazer neste cadastro"

Você explicou que elas não eram enfeite: *"a parte de fase do card ela serve de
direcionamento pra entender o que precisa ser feito em relação àquele
cadastro."*

```
   CADASTRO INICIAL  →  EM CONFERÊNCIA DO DP
                                │
              ┌─────────────────┴─────────────────┐
         PRESTADOR                              CTPS
              │                                   │
       ┌──────┴──────┐                   NA CONTABILIDADE
      PJ          os outros            (registro no eSocial)
       │              │                         │
 CONTRATO A        CONCLUÍDO  ◄──────────── CONCLUÍDO
  ASSINAR                            (quando a ficha volta)
       │
   CONCLUÍDO
```

| Etapa | O que ela está dizendo |
|---|---|
| **Cadastro inicial** | o DP ainda não olhou |
| **Em conferência do DP** | conferir o que a leitura preencheu e completar |
| **Contrato a assinar** | prestador PJ: gerar o contrato, assinar, arquivar |
| **Na contabilidade** | a contabilidade está registrando; o ERP cobra o retorno |
| **Cadastro concluído** | nada pendente |
| **Afastado** | a pessoa continua cadastrada |
| **Desligado** | fora da folha |
| **Cadastro cancelado** | abandonado antes de virar gente na empresa |

**Três regras que valem a pena saber:**

1. **A bifurcação CTPS × prestador é AUTOMÁTICA.** Deixar escolher à mão foi o
   que, no Pipefy, fez prestador cair na fila da contabilidade e ficar lá
   esperando um e-mail que nunca ia sair.
2. **Não dá para pular a conferência.** De *inicial* não existe caminho para
   *concluído* — era o salto que o quadro do Pipefy permitia arrastando o card.
3. **ETAPA não é SITUAÇÃO.** *Situação* responde "esta pessoa está na
   empresa?" e manda na folha. *Etapa* responde "o que falta fazer neste
   cadastro?" e manda na fila do DP.

---

## 3. A FILA DO DP — o que a obra pede

Onze tipos. **Atender EXECUTA**, na mesma transação — não é mover card:

| Pedido | O que acontece ao atender |
|---|---|
| Cadastro de colaborador | **cria** o cadastro |
| Desligamento | **desliga** (e bloqueia o acesso ao sistema) |
| Readmissão | **readmite**, sem apagar o vínculo anterior |
| Prestador que passa a ter carteira | **troca** o vínculo |
| Alteração de cargo ou salário | **altera**, com histórico |
| Atualização de dados cadastrais | **atualiza** |
| Cadastro de dependente | **cadastra** o dependente |
| Ajuda de custo · Vale-transporte | **mexe** nos benefícios |
| Desconto em folha | registra (o lançamento é fora) |
| Outra solicitação | texto livre |

**A diferença que isso faz:** no Pipefy, atender era mover o card — e alguém
depois mexia no cadastro à mão. O pedido dizia uma coisa e o cadastro ficava
outra. Aqui, pedido que não pode ser executado é **recusado com o motivo**,
nunca atendido pela metade.

---

## 4. OS E-MAILS — esta é a parte que você perguntou em detalhe

### 4.1 São TRÊS contas de e-mail, e elas fazem coisas diferentes

| Conta | Para que serve | Onde se configura |
|---|---|---|
| **da empresa** (uma por CNPJ) | manda a **cotação** ao fornecedor e o **pedido à contabilidade** | Configurações › Empresas |
| **do sistema** | manda o **convite de primeiro acesso** e avisos do sistema | Configurações › **E-mail do sistema** ← *novo hoje* |
| **de entrada (IMAP)** | **LÊ** as respostas da contabilidade e baixa os anexos | Pessoal › Contabilidade › caixa de entrada |

A do DP que você vai criar pode ser **as três**, ou separadas — é escolha sua.
O mais comum: `dp@` manda os pedidos e recebe as respostas; `sistema@` ou
`naoresponda@` manda os convites.

### 4.2 Como sai o pedido à contabilidade

São **sete pedidos formais**, cada um com o que espera de volta e o prazo:

| Pedido | Espera de volta | Prazo | Cobranças |
|---|---|---|---|
| Assinatura de CTPS (admissão) | ficha de registro | 2 dias úteis | 3, a cada 3 dias |
| Alteração de cargo e salário | alteração de contrato | 3 | 3 |
| Solicitação de férias | aviso e recibo de férias | 3 | 3 |
| Cadastro de dependente | alteração de contrato | 3 | 2, a cada 5 |
| Cadastro de cônjuge | alteração de contrato | 3 | 2, a cada 5 |
| **Desligamento** | **documentação rescisória** | 5 | 3 |
| Cancelamento de desligamento | alteração de contrato | **1** | 3 |

**O e-mail sai assim** (exemplo de admissão):

```
Assunto: [CT-00001] Assinatura de CTPS (admissão) — João da Silva (123.456.789-00)

Solicitamos o registro formal da admissão abaixo no eSocial.

Tipo de contrato: CLT por prazo indeterminado
Contrato de experiência: 30 + 60 dias

--- Dados do colaborador ---
Nome: João da Silva
CPF: 123.456.789-00
Data de nascimento: 12/03/1990
PIS/NIS: 123.45678.90-1
CTPS: 1234567 série 0001-PE
Empresa de registro: BWS Construções e Serviços Ltda
Obra: 081 — Residencial Vale do Sol
Cargo: Pedreiro
Convenção: PE — Construção Civil
Salário: R$ 2.100,00
Admissão: 05/01/2026

Mensagem gerada pelo ERP da BWS Construções.
```

**Três decisões no formato, e cada uma tem motivo:**

1. **Texto simples, sem HTML.** O e-mail vai ser lido, respondido e às vezes
   colado num sistema do outro lado. HTML bonito atrapalha as três.
2. **O número entre colchetes no assunto** (`[CT-00001]`) não é enfeite: é por
   ele que a resposta da contabilidade é casada com o pedido quando o e-mail
   dela não devolve o cabeçalho técnico.
3. **Pedido incompleto não sai.** Admissão sem PIS volta para o DP em vez de
   sair pela metade — o e-mail incompleto custa dois dias de ida e volta.

**Depois que sai:** o prazo é contado em dias úteis, a cobrança é automática
(se a rotina estiver ligada), e o retorno que chegar é **conferido campo por
campo** contra o cadastro. Ficha de registro com salário diferente do
combinado **não conclui** — abre divergência com o de/para.

### 4.3 Como o ERP LÊ as respostas

A cada rodada do agente, ele abre a caixa por IMAP, pega as **não lidas** (no
máximo 40 por vez), e para cada uma tenta casar com o pedido em três
tentativas, da mais confiável para a menos:

1. o cabeçalho técnico da resposta contra o que o ERP gerou ao enviar;
2. a marca `[CT-00001]` no assunto;
3. o CPF no assunto ou no corpo — **só se houver UM pedido aberto** para
   aquela pessoa.

Com mais de um pedido aberto da mesma pessoa, **ele não escolhe**: a mensagem
fica como "sem dono", com o motivo escrito, numa lista para alguém resolver.
Anexar a rescisão no pedido de férias é pior que não anexar.

**O que ele faz com o que acha:** baixa os anexos (até 25 MB), arquiva no
Google Drive **com o tipo que o pedido esperava**, e entrega o retorno ao
pedido. **O que ele nunca faz:** apagar mensagem, responder sozinho, e
arquivar assinatura de imagem, ícone de rodapé e logotipo — que vêm como
anexo em todo e-mail corporativo e encheriam o Drive de lixo com cara de
documento.

**Resposta de texto sem anexo não fecha nada.** "Estamos providenciando" fica
registrado e o prazo continua correndo.

### 4.4 O QUE VOCÊ PRECISA FAZER PARA LIGAR — e como testar

**Passo 1 — a conta que MANDA os pedidos à contabilidade**
Configurações › Empresas › a empresa › servidor de saída, porta, segurança,
usuário e senha. Botão **"Mandar um teste"**. ⚠️ No Gmail e no Google
Workspace com verificação em duas etapas, a senha aqui é uma **senha de
aplicativo**, não a senha de entrar no e-mail.

**Passo 2 — para onde vão os pedidos**
Pessoal › Contabilidade › "Para onde vão os pedidos": os e-mails da
contabilidade, separados por vírgula. **Mais de um**, porque mandar só para
uma pessoa faz o pedido parar nas férias dela.

**Passo 3 — a conta que o ERP LÊ (o e-mail do DP)**
Pessoal › Contabilidade › caixa de entrada: servidor IMAP, porta (993),
usuário, senha e a pasta (INBOX). Três botões:
- **Testar** — conecta, conta quantas não lidas há e sai. É o que transforma
  "acho que configurei" em "está configurado".
- **Conferir** — roda uma leitura **SEM GRAVAR NADA** e mostra o que ela faria:
  quais mensagens casariam com quais pedidos e quais ficariam sem dono. **É
  por aqui que você testa de verdade**, sem risco.
- **Ler agora** — roda valendo.
- E a **chave de ligar/desligar**: enquanto estiver desligada, nada é lido.

**Como eu sugiro testar o ciclo inteiro, antes de usar com a contabilidade de
verdade:** abra um pedido de teste para você mesmo (ponha o seu e-mail no
lugar do da contabilidade), responda esse e-mail anexando um PDF qualquer, e
aperte **Conferir**. Você vai ver se ele casou a resposta com o pedido e qual
tipo de documento ele daria ao anexo.

**Passo 4 — a conta do SISTEMA** (para o convite de acesso)
Configurações › **E-mail do sistema**: servidor, porta, segurança, usuário,
senha e como aparece para quem recebe. Botão **"Mandar um teste para mim"**.

**Passo 5 — o agente**
Pessoal › Contabilidade › o agente. **Todas as oito rotinas nascem
DESLIGADAS**, e cada uma tem três modos: *desligada*, *sugere* (escreve o que
faria, para alguém aprovar com um clique) e *faz*. A ordem que eu recomendo:
ligar primeiro a **caixa de entrada** e a **cobrança** em modo *sugere* por
uma semana, e olhar se ela está acertando.

---

## 5. ACESSO AO SISTEMA — o que mudou hoje

### 5.1 Colaborador e operador agora são a mesma pessoa

Você: *"nosso cadastro tem o cadastro de colaboradores, tem o cadastro de
usuários do sistema. É para haver uma correlação entre isso aí."*

Não havia, e você estava certo. Agora há: na ficha do colaborador, na aba
**Endereço** (que é onde está o e-mail, que vira o login), há o bloco
**"Acesso ao ERP"**.

**Quando a pessoa é desligada, o acesso cai sozinho.** É o tipo de coisa que
ninguém lembra de fazer à mão e cujo esquecimento não aparece: o
ex-colaborador continua entrando e vendo obra, pagamento e gente, e nada
quebra para denunciar.

⚠️ **O login não é apagado, é bloqueado.** Apagar levaria junto a autoria de
tudo o que a pessoa lançou, e "quem aprovou este pagamento?" viraria uma
pergunta sem resposta.

### 5.2 O primeiro acesso é por convite — o seu desenho

> *"Eu vou cadastrar o e-mail da pessoa, configurar o perfil, e disparar um
> e-mail para ela gravar a senha dela."*

É assim que ficou:

1. você cadastra o operador com e-mail e perfil, **deixando a senha em
   branco**;
2. ao salvar, sai um e-mail pela conta do sistema;
3. a pessoa abre o link e escolhe a senha dela;
4. **ninguém mais sabe qual é** — nem você, nem quem tem acesso ao banco.

Isso é melhor que o de hoje **por segurança**, não só por comodidade: hoje
alguém escolhe a senha pela pessoa e manda por WhatsApp — a senha passa por um
terceiro, fica escrita numa conversa que ninguém apaga, e quase nunca é
trocada.

**O link vale 7 dias e serve UMA vez.** Reenviar (botão na ficha) cancela o
anterior. Se o e-mail não sair, a tela mostra o link para você entregar de
outro jeito.

Pela ficha do **colaborador** dá para fazer os dois passos num clique:
**"Liberar acesso e convidar"** cria o operador com o e-mail da pessoa, liga
ao cadastro, marca a obra dela como alcance e manda o convite.

### 5.3 Respondendo à sua pergunta: "é só cadastrar as pessoas e liberar o acesso?"

**Para o cadastro funcionar, sim** — depois dos quatro passos de e-mail acima e
do botão de atualização do banco. Mas há três coisas que dependem só de você e
que eu não posso fazer:

1. **O arquivo das ~575 pessoas ativas** para importar (Pessoal ›
   Colaboradores › "Importar pessoas de planilha"). A importação já existe e
   confere antes de gravar.
2. **Os perfis de acesso**: quem é só diário de obra, quem é DP, quem é obra.
   O perfil "Diário de obra" já vem pronto; os outros já existem.
3. **As contas de e-mail**, que são de verdade e você é quem cria no provedor.

---

## 6. O QUE VOCÊ JÁ RESPONDEU, E O QUE AINDA FALTA

### ✅ Respondido em 10/11/2026 — já aplicado

| Pergunta | Sua resposta | O que mudou |
|---|---|---|
| ASO obrigatório? | *"seria o correto, mas não. Não vamos colocar ainda."* | saiu da lista |
| CTPS obrigatória? | *"para quem não é prestador não fica mais obrigatório"* | saiu da lista |
| Foto do colaborador | *"alimentada pelo ponto"* | vem do ponto, ninguém anexa |
| Os campos de benefício sobram? | **não** — refletem no pagamento | todos ficam |
| ID Fortes serve? | é a **chave da folha da contabilidade** | fica, e é importante |
| Prazos da contabilidade | *"vamos começar nesses aí"* | 2, 3 e 5 dias úteis |
| Prestador vai direto a concluído? | **sim** — RPA, PJ e mensalista não dependem da contabilidade | mantido |
| "Outra solicitação" fica? | **sim** | mantida |
| Quem libera acesso ao sistema? | *"vai ser sempre o administrador. DP não."* | mantido como está |
| "Esqueci minha senha"? | *"é necessário. Não deixar a autonomia."* | **construído** |
| Escolher a pessoa na solicitação | buscar por nome/CPF, sem expor lista | **construído** |
| As etapas e a bifurcação | *"acho que ficou correto"* | mantidas |
| Os acréscimos da diária | valem, mas *"deve ficar dentro de uma configuração"* | **Configurações › Diárias** |
| A conferência atrasa o ponto? | *"não quero que impacte (…) a obra pediu, libera"* | **conferido e amarrado com trava** |

### ❓ O que ainda falta você dizer

1. **Os 83 campos do cadastro.** Agora sabendo que campo de pagamento fica, a
   pergunta encolheu: sobra algum campo que **não** é cadastral nem de
   pagamento? Meus candidatos restantes: `ref_pipefy` (o número do card, que
   morre com o Pipefy) e `requerente` em texto livre.
2. **O que conferir no retorno de cada pedido** — hoje: nome, CPF, cargo,
   salário e datas. Na rescisão acrescentei o que você listou (data de saída,
   aviso prévio, dias trabalhados, saldo de salário, faltas). **Falta algo?**
3. **A atualização do ID Fortes por arquivo** — você disse "mais na frente".
   Quando quiser, é meio dia de trabalho.
4. **A obra na solicitação.** Você disse que hoje é menos necessária porque o
   ponto cruza, mas que importa saber de onde a pessoa é. Hoje ela é
   **opcional** e o pedido guarda **quem solicitou** sempre. Confirma?
5. **O valor da diária de quem é CTPS.** Você contou que às vezes define
   diária para CTPS que trabalha no fim de semana, mas que *"atualmente a
   gente está preferindo pagar hora extra, que é o correto"*. O campo existe
   e continua funcionando — só quero saber se é para mantê-lo à vista no
   cadastro de CTPS ou escondê-lo.
6. **A gratificação quinzenal x mensal.** Você falou das duas; o cadastro tem
   hoje **um** valor de gratificação e a marca "parcela única". Isso cobre as
   duas, ou falta dizer a periodicidade em separado?

## 7. O QUE AINDA NÃO FUNCIONA, para não haver surpresa

- **O ERP não gera o PDF sozinho.** Diário, relatório e documento saem por
  Ctrl+P › Salvar como PDF. Não dá para anexar em e-mail automático ainda.
- **A leitura dos documentos (RG, CTPS, comprovante) para preencher o
  cadastro** está desenhada e ainda não liga em todos os tipos.
- **A conferência do retorno da contabilidade** usa IA e **nasce em modo
  "sugere"** — ela nunca conclui sozinha até você mandar.
- **O WhatsApp para a obra** está desenhado em `AGENTE_DP.md` e não
  construído.
- **Os tipos de pedido das outras áreas** (financeiro, suprimentos) ainda não
  existem no portal. Só os do DP.

  ⚠️ **E isso mudou de peso hoje:** você disse *"eu sei que a gente vai sair
  do Pipefy aos poucos, mas na totalidade (…) a cada maturação de cada
  processo a gente vai liberando"*. Antes eu tinha registrado que o Pipefy
  sairia **de uma vez**, o que obrigava todas as áreas a existirem aqui antes
  do corte. Saindo por processo — como já aconteceu com o MobPonto —, o DP
  pode sair sozinho assim que estiver maduro, sem esperar suprimentos e
  financeiro. **Isso destrava o calendário.**

- **A atualização do ID Fortes por arquivo** (§2.3-quater).
- **Os acréscimos da diária** ainda não são calculados no ERP: o campo do
  valor existe, a regra dos +10/+20 está no Análise de SPs.
