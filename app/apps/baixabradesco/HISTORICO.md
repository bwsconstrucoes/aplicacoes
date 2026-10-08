# BaixaBradesco — decisões, incidentes e o que falta

Este arquivo existe para uma sessão nova (ou uma pessoa nova) pegar o trabalho
sem repetir o que já foi discutido e sem repetir o que já deu errado.

O `README.md` ao lado explica **como a aplicação é**. Este aqui explica **por que
ela é assim**, e o que aconteceu no caminho. Leia os dois antes de mexer.

---

## Onde o trabalho está

A baixa de comprovantes era um cenário do Make.com montado peça por peça. Virou
esta aplicação, dentro do monorepo, em `/api/baixabradesco`. O Make continua
existindo: ele recebe o e-mail com o comprovante e chama a rota. Toda a decisão
— de quem é o comprovante, quanto foi pago, o que atualizar — passou para cá.

Está em produção e funcionando: Pix, boleto, transferência, FGTS, folha pela
Somapay e vale-alimentação BeeVale. Foi construída em sessões no Claude.ai, sem
memória entre elas, o que explica por que só agora ela ganhou documentação.

**Estado em 04/09/2026:** área sem documentação própria e **sem nenhum teste
automatizado**. Esta sessão começou por aí — primeiro escrever o que a aplicação
faz, depois cobrir com teste as três peças onde um erro custa dinheiro sem dar
sinal: os dois leitores de comprovante e o casador de pagamentos.

### O que está pendente AGORA

**Publicado em 11/09/2026** (junção `1a21605`), com o "pode" do dono: os **dois
caminhos da Somapay** funcionando — o depósito pago direto na Somapay e a
transferência Bradesco → Somapay —, a conta de destino vindo da BaseBancos pela
chave PIX, e a correção do erro que mandava baixar na conta do Bradesco. Sem
migração de banco.

**Publicado em 04/09/2026** (junção `16039ac`): as duas correções — comprovante
recusado pelo banco e trava de duplicidade — mais o `README.md`, este arquivo, a
linha da área no `CLAUDE.md` e o registro no `CONTEXTO.md`. Sem migração de
banco.

Falta ainda a rede de proteção dos **outros** tipos de comprovante: Pix, boleto,
transferência, FGTS e BeeVale não têm nenhum teste sobre a leitura dos campos
nem sobre a escolha da SP. Para escrever esses casos são necessários
**comprovantes de exemplo de cada tipo**, que só o dono tem. Antes de qualquer um
entrar no Git, nome, CPF, CNPJ, conta e código de barras viram fictícios,
mantendo o formato do texto.

**Conferir na primeira baixa real:** no retorno do Make, os campos
`recusados_nao_efetivados` e `duplicados_ja_baixados`; e, na primeira rescição
Somapay, que a baixa caiu na conta Somapay certa no Omie.

### As três divergências achadas na leitura do código — todas resolvidas

A documentação antiga descrevia três proteções que **o código não tinha**. As
três foram tratadas em 04/09/2026:

1. **Comprovante recusado pelo banco passava como pagamento feito.** Corrigido —
   ver o incidente abaixo.
2. **A trava contra pagar duas vezes só gravava, não conferia.** Corrigido — ver
   o incidente abaixo.
3. **O leitor do Sicredi nunca é chamado.** ❌ Registrado em 11/09/2026 como
   decisão do dono — **e estava errado, por mal-entendido meu**. Ele havia dito
   que não usava as outras duas contas **Somapay**; eu entendi Sicredi.
   Corrigido em 13/09/2026: o leitor foi ligado. Ver o incidente no fim.

## Decisões já tomadas (e por quê)

- **Cada página do PDF é um comprovante.** O banco emite vários numa folha só.
- **`modo_teste` é o padrão.** A aplicação só escreve se o Make mandar
  `modo_teste: false`. Um pedido malformado não vira baixa indevida.
- **O comprovante só vai para o Dropbox depois de casar com uma SP.** Antes,
  todo comprovante virava arquivo, inclusive os que ninguém sabia de quem eram.
- **O Omie é sempre alterado antes de baixar**, mesmo que pareça desnecessário.
  Garante que o título fique com o valor real do comprovante, e não com o que
  alguém digitou.
- **Duas candidatas sem desempate = não executa.** Vale mais um comprovante
  parado para conferência do que uma baixa no título errado.
- **O número que parece SP mas começa com `000201` é ignorado.** É o início do
  QR Code do Pix, e já foi confundido com número de SP.
- **Na folha Somapay, a semântica dos campos de transferência do Omie está
  invertida em relação ao que o nome sugere** — foi validado em produção assim.
  Está comentado no código: não "consertar".
- **Cota do Google estourada não é erro para o Make.** O pedido é guardado em
  disco e a resposta é "recebido, adiado"; um cron externo reprocessa a cada 5
  minutos. A alternativa era o cenário do Make quebrar no meio de um lote.
- **A fila em `/tmp` some quando o serviço reinicia.** Aceito: o Make pode
  reenviar. Publicar uma versão nova reinicia o serviço, então publicar durante
  um lote grande perde o que estava adiado.

## Incidentes

- **Julho de 2026 — a instância morria de falta de memória**, e esta aplicação
  era o pico residual. Três causas aqui dentro: a planilha de 52 mil linhas era
  baixada duas vezes por pedido; a gravação na SPsBD lia a planilha **inteira**
  só para achar uma linha, e fazia isso numa thread por comprovante (um lote de
  dez comprovantes disparava dez downloads simultâneos da planilha); e o
  download do comprovante por endereço não tinha teto. Corrigido: leitura única
  e compartilhada, gravação por busca só das colunas de filtro, e teto de 50 MB
  no download. **A regra que fica: nunca voltar a ler a planilha inteira.**
  Registro completo no `CONTEXTO.md` §9.
- **Regressões por remendo sobre versão velha.** Já se perderam, e foi preciso
  reescrever, o pulo do valor zerado, as funções de registro de duplicata e uma
  das estratégias de casamento — todas por editar um arquivo local desatualizado
  em relação ao que estava publicado. **Antes de editar, conferir se o arquivo
  tem os marcadores esperados.**
- **`Agendado` contra `agendado` custou dias de investigação.** Todo status
  vindo de planilha é comparado normalizado.
- **Cota do Google (erro 429) derrubando módulos vizinhos.** A conta de serviço
  é uma só para todo o monorepo: quando esta aplicação consome a janela do
  minuto, os outros módulos param junto. Existe uma nova tentativa automática
  com esperas de 30 e 65 segundos (a cota é por minuto, esperar 1 segundo é
  inútil). Por isso o tempo limite dos módulos HTTP no Make deve ser 300
  segundos.

- **04/09/2026 — comprovante que o banco não efetivou passava como pagamento
  feito.** O leitor barrava só a frase exata "Operação Não Realizada". Um
  comprovante real de 16/06/2026 dizia **"Transação Não Realizada"** — boleto
  recusado por saldo insuficiente, pendente de aprovação — e era lido como um
  boleto comum: valor, data, conta de débito e código de barras completos.
  Com isso o casador acharia a SP de verdade pelo código de barras, o Omie
  baixaria o título e a planilha marcaria a SP como paga. Dinheiro que nunca
  saiu do banco, registrado como pago.
  **Como ficou:** a recusa virou uma lista de frases, comparada contra o texto
  já sem acento e em minúsculas, cobrindo as redações que o banco usa
  ("operação/transação/pagamento não realizada/efetivada/efetuada", "não foi
  efetuada", "pendente de aprovação", "aguardando aprovação", "cancelada"). A
  checagem passou a acontecer **antes** de extrair qualquer campo, então um
  comprovante recusado nem chega ao casador com valor ou código de barras. O
  leitor do Sicredi ganhou a mesma trava. E o que antes era ignorado em silêncio
  passou a aparecer no resumo da resposta, em `recusados_nao_efetivados`.
  **O que ficou de fora, de propósito:** palavras soltas como "cancelado" ou
  "agendado" não entraram na lista. O rodapé de todo comprovante Bradesco tem
  "Cancelamentos, Reclamações" — uma palavra solta barraria pagamento bom. Há um
  teste justamente para segurar isso. Se aparecer alguma redação de recusa que
  não está na lista, é só acrescentar a frase.

- **04/09/2026 — a trava contra pagar duas vezes estava solta.** Cada página de
  comprovante ganha uma impressão digital, e ela era gravada na aba
  `LogBaixaBradesco` depois de cada baixa. Só que **ninguém consultava a lista
  antes de executar**: a função de conferência existia, era importada pelo
  `core.py` e nunca era chamada. Na prática, quem segurava o pagamento repetido
  era o Omie respondendo "título já pago" — proteção de terceiro, não nossa, e
  que não cobre o caso de o título ter sido reaberto ou de existir outro título
  com o mesmo valor.
  **Como ficou:** a lista é lida **uma vez por lote** e conferida em memória,
  página a página, antes de procurar a SP. A página processada agora entra na
  lista do próprio lote, então o mesmo PDF enviado duas vezes no mesmo pedido
  também é barrado. O que foi barrado aparece no resumo da resposta, em
  `duplicados_ja_baixados`.
  **O cuidado que não pode ser esquecido:** a leitura é UMA por lote, de
  propósito. Uma consulta por página faria um lote de dez comprovantes virar dez
  leituras da mesma coluna — exatamente o padrão que derrubou a instância em
  julho de 2026 e que estoura a cota do Google. Há teste segurando isso.
  **O limite que fica:** a impressão digital usa o conteúdo do arquivo **mais o
  nome dele**. O mesmo PDF reenviado com outro nome conta como novo. Mudar isso
  invalidaria todo o registro histórico, então ficou como está.

- **11/09/2026 — rescisão paga direto na Somapay passou a ser baixada.** O dono
  mandou um comprovante de verbas rescisórias emitido pela **própria Somapay** —
  não pelo Bradesco. O robô lia esse papel como comprovante genérico: pegava
  valor e data certos, mas lia o **número do depósito no lugar do CPF** (o número
  tem 14 dígitos, o tamanho de um CNPJ) e não achava SP nenhuma. Resultado: ficava
  parado, sem baixa.
  **O que passou a existir:** o leitor reconhece o comprovante da Somapay, lê o
  CPF de quem recebeu e o CNPJ de quem depositou, e o casamento é por **CPF +
  valor exato** — o papel não tem número de SP nem conta da empresa. Entre duas
  SPs da mesma pessoa e mesmo valor, desempata a de verba rescisória; empatando,
  não executa.
  **A decisão que importa:** são **dois caminhos Somapay diferentes**. Quando o
  dinheiro sai do Bradesco para a Somapay, o Omie recebe a transferência e
  depois a baixa. Quando o pagamento já saiu da conta Somapay — este caso — o
  Omie recebe **só a baixa**: lançar a transferência criaria um dinheiro que não
  andou. Por isso viraram tipos separados, e há teste garantindo que este não
  entra no caminho da transferência.
  **A conta em que a baixa cai** vem da BaseBancos, pelo **nome do depositante**:
  "BWS CONSTRUÇÕES" casa com a conta "Somapay BWS". A primeira versão usava o
  CNPJ e estava errada — o dono colou a planilha real e as **três** contas
  Somapay (BWS, INFRADENDE, IFPESANTACRUZ) têm o mesmo CNPJ, o da própria
  Somapay. O CNPJ não distingue nada; o nome distingue. Não batendo nenhum nome,
  ou batendo mais de um, o robô **não escolhe** — deixa pendente e diz o motivo.
  Hoje só a conta BWS é usada; as outras duas resolvem sozinhas se um dia
  chegar comprovante delas.
  **O que continua desligado:** o caminho Somapay **com** transferência. A
  máquina toda existe, mas o leitor nunca marca comprovante como sendo desse
  tipo. Não foi ligado porque não havia exemplo desse comprovante em mãos.

- **11/09/2026 — a planilha de verdade derrubou a regra do CPF, ainda no ramo.**
  A primeira versão do depósito Somapay casava a SP pelo **CPF do funcionário**.
  Com acesso à SPsBD pelo conector do Google, ficou claro que ela **nunca teria
  funcionado**: numa SP de rescisão o credor é a **empresa**, e a coluna
  CPF/CNPJ traz o CNPJ dela. O nome do funcionário aparece só na descrição, no
  formato `Conta Origem: 50024-0  TRCT <NOME>`.
  **Como ficou:** casamento por **nome do beneficiário + valor exato**, com o
  CPF aceito como sinal extra quando a aba o traz. O valor sozinho não serve —
  em 09/09/2026 havia quatro rescisões de R$ 452,40, de quatro pessoas.
  **Segundo achado:** a planilha guarda CPF como **número**, então
  008.115.554-96 vira `811555496` e perde os zeros da frente. Qualquer
  comparação de CPF devolve os zeros antes de comparar.
  **Lição que vale para a área inteira:** regra de casamento escrita sem olhar o
  dado real é chute com cara de engenharia. O conector do Google resolve isso —
  ver a limitação dele abaixo.

## O que ficou de fora, e é bom saber

- **Comprovante sem número de SP e sem casamento fica parado** como
  "pendente de validação". Não há tratamento automático além das tentativas
  descritas no `README.md`; alguém precisa olhar.
- **WhatsApp está implementado mas normalmente desligado** nos testes.
- **Não existe tela.** Só chamadas de máquina.
- **A senha da rota é opcional no código**: se `BAIXABRADESCO_SECRET` estiver
  vazia no Render, qualquer um que descubra o endereço consegue chamar.
- **A migração do Z-API para o gateway de WhatsApp próprio** está prevista no
  `CONTEXTO.md` §10 e ainda não foi feita aqui.

---

## Registro por sessão

### 04/09/2026 — a área ganha memória escrita

Primeira sessão dedicada a esta área. O código foi lido inteiro e virou
`README.md` (o que a aplicação faz, quem chama, de onde vêm os dados, variáveis
de ambiente, planilhas e serviços) e este histórico, consolidando o resumo das
sessões anteriores do Claude.ai. A área entrou na tabela do `CLAUDE.md`, e essa
mudança — que atravessa todas as áreas — ficou registrada no `CONTEXTO.md`.

Na leitura apareceram três divergências entre a documentação antiga e o código
de verdade: o leitor do Sicredi que nunca é chamado, a trava de duplicidade que
grava mas não confere, e a checagem estreita de comprovante recusado. As duas
primeiras estão descritas acima, aguardando decisão do dono.

A terceira foi corrigida no mesmo dia, depois que o dono mandou um comprovante
de "Transação Não Realizada" e disse que esse tipo não pode passar — detalhe no
incidente acima. Junto veio o primeiro teste automatizado da área
(`tests/test_baixabradesco_recusa.py`, 14 casos), com o comprovante real
guardado como exemplo em `tests/exemplos_baixabradesco/`, com conta, CNPJ,
nomes e código de barras trocados por fictícios.

**Verificado:** suíte inteira do repositório passando (789 testes, os de banco
pulados por falta de `ERP_TEST_DATABASE_URL` neste ambiente) e a aplicação
subindo com todos os blueprints.
**Não verificado:** nenhum comprovante recusado passou pelo caminho completo em
produção depois da correção — a primeira vez que o Make mandar um, vale conferir
no retorno o campo `recusados_nao_efetivados`.

Na sequência, o dono decidiu as outras duas: **ignorar o Sicredi** (não é mais
usado) e **ajustar a trava de duplicidade**, que foi ligada ao fluxo com mais
nove testes (`tests/test_baixabradesco_duplicidade.py`) — inclusive um que
segura a leitura única por lote, para ninguém reintroduzir o problema de memória
de julho.

**Não verificado, e vale conferir na primeira baixa real:** o campo
`duplicados_ja_baixados` no retorno, e que um comprovante legítimo **não** está
sendo barrado por engano. Se aparecer barrado à toa, o suspeito é um comprovante
que já havia sido processado e depois teve o título reaberto no Omie.

### 11/09/2026 — a rescisão da Somapay

O dono perguntou duas coisas e trouxe um comprovante.

**Sobre `falhaagendar`:** foi conferido no código e respondido. A lista de SPs
que o robô carrega já inclui `falhaagendar`, então quase todos os caminhos
encontram a SP normalmente. A exceção é o casamento por **valor + conta** de Pix
e transferência sem número de SP, que exige o status exatamente `agendado`. Está
anotado no `README.md`. **Igualar isso continua em aberto** — é mudança de regra
de negócio, e o dono ainda não decidiu.

**Sobre o comprovante:** entregue a baixa do depósito Somapay, descrita no
incidente acima, com 23 testes (`tests/test_baixabradesco_somapay_deposito.py`)
e o comprovante real guardado anonimizado em `tests/exemplos_baixabradesco/`.

**Verificado:** suíte inteira passando (2579 testes) e a aplicação subindo com
todos os blueprints.
**Verificado também contra a BaseBancos real**, colada pelo dono na conversa: o
depositante "BWS CONSTRUÇÕES" do comprovante resolve para a conta
"Somapay BWS - 22005-1". A planilha não foi lida pelo sistema (não há credencial
do Google nesta sessão) — o que foi conferido é a regra contra o conteúdo que o
dono mostrou.

**Não verificado:** nada disso passou por um comprovante de verdade em
produção.

### 11/09/2026 (tarde) — acesso às planilhas, e o que ele mostrou

O dono perguntou como dar acesso às planilhas. **O conector do Google Drive do
Claude já está ligado** e foi usado nesta sessão: a BaseBancos foi lida inteira,
e a SPsBD foi lida em parte.

**A limitação, que precisa ficar registrada:** a SPsBD tem ~52 mil linhas e o
conector devolveu **65 linhas** da aba principal. Serve para conferir formato,
nomes de coluna e amostras — **não serve para procurar um registro** no meio das
52 mil. Para busca de verdade, quem tem a chave é o próprio robô (conta de
serviço), pela rota de diagnóstico.

Mesmo truncada, a leitura pagou: derrubou a regra do CPF (acima) e mostrou o
formato real da SP de rescisão.

**Sobre o comprovante de transferência Bradesco → Somapay**, que o dono mandou:
duas transferências PIX de 11/09/2026, da conta Bradesco 50024-0. A **chave PIX
impressa no comprovante é exatamente a chave cadastrada na BaseBancos para a
conta `Somapay BWS - 22005-1`**. É o identificador exato que faltava: ele diz
que o dinheiro foi para a Somapay **e qual** das três contas, sem heurística
nenhuma. É o que deve substituir a regra antiga de "se a conta contém 2541".

**Em aberto, e é o que trava o caminho da transferência:** uma transferência
corresponde a UMA SP ou a um lote? Das duas do comprovante, R$ 8.128,17 bate
exatamente com uma rescisão (TRCT DIOGENES DAVID DA SILVA); R$ 7.350,48 **não
bate com nenhuma** das que deu para ler, nem com soma de rescisões daquele dia.
Pode ser truncamento da leitura, pode ser lote. Sem essa resposta, casar
transferência por valor é chute.

### 11/09/2026 (fim da tarde) — a transferência é UMA por rescisão, e há um erro vivo em produção

**Respondida a pergunta que travava o caminho da transferência: é um para um.**
As duas transferências PIX do comprovante batem cada uma com uma rescisão:

| Valor | SP | Funcionário |
|---|---|---|
| R$ 8.128,17 | 1442630306 | DIOGENES DAVID DA SILVA |
| R$ 7.350,48 | 1443274610 | ALEXANDRE DA CUNHA CABRAL |

O segundo foi localizado pelo dono e confirmado na planilha *Documentação
Fiscal* — ele não estava na parte da SPsBD que o conector entregou. **Não é
lote**: cada transferência corresponde a uma rescisão.

**O erro vivo, que não foi introduzido agora — está na `main` hoje:** o
comprovante Bradesco de transferência para a Somapay é lido como um Pix comum e
**casa** com a SP de rescisão por valor + conta + agendado. O robô então mandaria
**baixar o título na conta do Bradesco**, sem lançar a transferência. O dinheiro
saiu do Bradesco para a Somapay e só depois foi ao funcionário — baixar no
Bradesco faz o saldo da Somapay no Omie nunca receber nada. Exercitado com a SP
1442630306 no formato real: casa por `valor_conta_agendado`.

**O risco que sobra, mesmo acertando a regra:** o comprovante da transferência
não traz o nome do funcionário — só valor, data e conta. Quando duas rescisões
pendentes tiverem o mesmo valor (aconteceu: quatro de R$ 452,40 em 09/09/2026),
não há como distinguir e o comprovante para como pendente de validação. É o
comportamento certo, mas é bom saber que vai acontecer.

**Decisão em aberto, esperando o dono:** o que cada papel deve fazer quando os
dois chegam para a mesma rescisão. A recomendação registrada é **cada papel faz
o que ele prova**: o comprovante do Bradesco lança **só a transferência** entre
contas (a máquina para isso já existe, `lancar_movimentacao_omie_sem_sp`), e o
comprovante da Somapay **baixa** o título na conta Somapay. Isso dispensa casar
a transferência com uma SP — e com isso some o risco do valor repetido.

**A chave PIX é o identificador exato da conta Somapay**, e deve substituir a
regra antiga de "se a conta de débito contém 2541": cada uma das três contas
Somapay tem sua própria chave na BaseBancos, e a chave vem impressa no
comprovante.

### 11/09/2026 (noite) — o caminho da transferência ligado, com a chave PIX no lugar da regra velha

**Decisão do dono, perguntado explicitamente:** os dois comprovantes dão baixa.
O do Bradesco lança a transferência **e** baixa o título na conta Somapay,
anexando ele mesmo como comprovante; o da Somapay baixa o título na conta
Somapay. O dono sabe que o comprovante que fica anexado na SP costuma ser o do
Bradesco, e não o "real final" da Somapay, e pediu para manter assim. Minha
recomendação havia sido dividir (transferência só lança movimentação, baixa só
pelo comprovante da Somapay) — ele preferiu o desenho original, que era o que
tinha pedido desde o começo.

**O que passou a funcionar:**
- O comprovante do Bradesco de transferência é reconhecido pela instituição de
  destino (`Instituição destino: SOMAPAY`), e não pela palavra "somapay" solta —
  ela também aparece no comprovante emitido pela Somapay.
- A conta Somapay que recebeu vem da **chave PIX impressa no comprovante**,
  casada com a coluna Chave PIX da BaseBancos. Cada uma das três contas tem a
  sua. Sem chave cadastrada, não executa.
- A sequência no Omie é transferência → consultar → alterar → baixar, com os
  três últimos na conta Somapay.

**O que foi aposentado:** a regra `se a conta de débito contém 2541 então
IFPESANTACRUZ, senão BWS`, e os dois códigos de conta Omie escritos dentro do
código. Só conheciam duas das três contas, e dependiam da conta de débito em vez
de um identificador do destino. Agora tudo vem da planilha: cadastrar uma quarta
conta Somapay é mexer na BaseBancos, não no sistema.

**O risco que fica, e é conhecido:** o comprovante da transferência não traz o
nome do funcionário. Duas rescisões pendentes de mesmo valor na mesma conta não
têm como ser distinguidas e ficam paradas para conferência humana. Há teste
cobrindo, e o desempate por conta de débito ajuda quando as contas diferem.

**Verificado:** suíte inteira passando (2610 testes, 20 novos deste caminho) e a
aplicação subindo. Os dois comprovantes reais foram exercitados, anonimizados
como exemplo.
**Não verificado:** nada passou por produção. Em especial, a sequência de
transferência no Omie não foi executada contra o Omie de verdade desde a
mudança — a semântica invertida dos campos continua como estava, validada em
produção em 2026 e coberta por teste para não ser "corrigida" sem querer.

**Publicado em 11/09/2026, junção `1a21605`.** Conferir na primeira baixa real:
que a transferência aparece no Omie entre as contas certas, que a baixa caiu na
conta Somapay (e não na do Bradesco), e o campo `duplicados_ja_baixados` no
retorno quando os dois comprovantes da mesma rescisão chegarem.

### 11/09/2026 — primeira baixa real: o robô recusou, e estava certo

O dono enviou em produção o comprovante Somapay de uma rescisão (EDUARDO,
R$ 452,40) e recebeu `nao_localizado` — "Nenhum candidato encontrado" —, mesmo
com as quatro SPs de R$ 452,40 existindo na planilha.

**A causa, confirmada pelo dono olhando o registro:** a SP estava com a coluna
**Agendado vazia**. A lista de SPs candidatas (`load_spsbd_operacional`) só
carrega quem tem `agendar`, `agendado` ou `falhaagendar` ali. Sem isso a SP nem
chega a ser comparada — valor e nome estavam certos, mas ninguém olhou para
eles. Reproduzido em teste local: dá exatamente a mesma mensagem.

**A decisão do dono, perguntado diretamente: o robô agiu certo, o esquecimento
foi dele.** A coluna Agendado é controle de verdade — só o que foi agendado pode
ser baixado. O caminho é marcar a SP e reenviar o comprovante.

**O que foi feito e desfeito:** chegou a ser escrita uma lista nova
(`load_spsbd_folha`) que carregava despesas de folha com `O=Pagar` **sem** olhar
a coluna Agendado, com 9 testes. **Foi revertida** depois da decisão acima, e
nunca foi publicada. Fica registrado aqui porque a ideia vai reaparecer: se um
dia a rescisão pela Somapay deixar de passar pelo agendador, é esse o caminho —
e o filtro de `Status Pgt = Pagar` tem de continuar, senão SP já paga volta a
ser baixável.

**Melhoria sugerida, não feita:** quando o robô não acha candidato, ele diz
apenas "nenhum candidato encontrado". Se dissesse *"existe SP com este valor e
este nome, mas ela não está agendada"*, esta investigação inteira teria sido
uma linha. Vale a pena, e é barato — depende do dono pedir.

### 11/09/2026 — o robô passou a avisar o que NÃO baixou (publicado, `3b5165c`)

Consequência direta do caso acima: a explicação de por que um comprovante não
baixou existia, mas morria dentro da resposta devolvida ao Make. O dono pediu o
aviso, com o recorte dele: *"o que baixou normal, não preciso saber. Só o que
deu alguma falha, que de repente merece uma atenção ou uma melhoria na regra."*

**Como ficou:** no fim de cada lote, **uma** mensagem pelo WhatsApp com o que
ficou de fora e o motivo de cada um — sem SP encontrada, mais de uma candidata,
recusado pelo banco, ou falha ao baixar no Omie. Fora do aviso, de propósito: o
que baixou e o que foi barrado por duplicidade (o dono disse que não precisa
saber, e a trava já resolve).

**Decisões de desenho:**
- **Um aviso por lote, no máximo dez itens.** Comprovante chega em leva; um
  aviso por comprovante viraria barulho, e barulho faz parar de ler.
- **Dois destinatários.** Começou no celular do dono, passou para o número do
  financeiro (*"fica mais geral"*) e terminou nos **dois**, ainda em 11/09/2026:
  o financeiro resolve, o dono decide se a regra muda. Falha em um não impede o
  outro, e um número repetido na configuração não gera duas mensagens. Não confundir com o WhatsApp que vai
  ao responsável pela SP quando a baixa dá certo — aquele é anterior, tem outro
  propósito e continua indo para quem pediu o pagamento. Há teste travando que o
  aviso de falhas nunca alcança o telefone do solicitante, que circula no lote
  dentro dos dados do card do Pipefy.
- **WhatsApp, a pedido do dono** — e é mesmo o canal melhor aqui: chega direto
  pelo número, enquanto o Telegram só alcança quem já conversou com o bot. O
  Telegram vai de espelho, sem custo.
- **Reusa o envio que o módulo já tem** (`zapi.send_text`), o mesmo que avisa o
  responsável pela SP. Ele funciona em produção e aceita as credenciais Z-API
  vindas no pedido do Make — que é como elas chegam. ⚠️ Isso importa: o
  `notificador` comum lê `ZAPI_INSTANCE_TOKEN`, e este módulo usa
  `ZAPI_API_TOKEN`. Nomes diferentes para a mesma coisa; usar o envio próprio
  evita depender de qual das duas está configurada no Render. Se nenhuma
  estiver, cai no notificador como reserva.
- **Reusa `CHATBOT_MASTER_PHONE`**, a convenção que o chatbot e o
  processarnovasp já usam, em vez de escrever o número num terceiro lugar.
  `BAIXABRADESCO_AVISO_TELEFONE` troca o destino se um dia for outra pessoa.
- **Avisar nunca derruba a baixa.** O envio é protegido: se o Telegram cair, a
  baixa já aconteceu e a resposta sai normal.

**Limite conhecido:** o WhatsApp depende do toggle `NOTIFICAR_WHATSAPP` e das
credenciais Z-API. O espelho no Telegram só alcança quem está na aba
`TelegramID`. Se o aviso não chegar, conferir nessa ordem: toggle ligado,
credenciais presentes, número certo.

### 11/09/2026 (noite) — o empate que travava duas baixas virou distribuição (publicado, `546b45a`)

Caso real: duas rescisões de R$ 5.532,57 (CAIO e ALEXSANDRO), as duas agendadas,
e um PDF com dois comprovantes de transferência de R$ 5.532,57. Um a um, cada
comprovante via duas SPs possíveis e parava como `pendente_validacao`. As duas
baixas ficavam esperando conferência humana por uma ambiguidade que, olhando o
PDF inteiro, não existe: são dois pagamentos para duas SPs.

**Decisão do dono:** distribuir. *"Não importa saber exatamente quem é quem, o
que importa é que a gente consiga baixar."* Os papéis são intercambiáveis —
mesmo valor, mesma data, mesma conta, e o da transferência nem traz o nome do
funcionário. Vale para dois, três, quantos forem.

**Como ficou:** o processamento de cada anexo virou duas passadas. A primeira lê
e localiza a SP de cada página sem executar nada; entre as duas entra o
desempate por lote; a segunda salva o comprovante e monta os planos. A segunda
passada continua dentro do laço do anexo **de propósito**: é ali que os bytes do
PDF ainda existem, e tirá-los de lá significaria segurar todos os PDFs do lote
na memória — exatamente o que derrubou a instância em julho.

**As três travas, e por que cada uma existe:**

1. **Mesma quantidade dos dois lados.** Dois comprovantes para três SPs deixaria
   uma SP paga sem ter sido.
2. **Pagamentos comprovadamente diferentes.** Esta é a que segura dinheiro: se
   o mesmo comprovante for mandado duas vezes no mesmo PDF, distribuir baixaria
   **duas** SPs para **um** pagamento. O identificador de cada pagamento
   (`Identificador`, ou o número do documento) resolve. ⚠️ O `N° de controle`
   **não serve**: ele é do lote inteiro e se repete entre as páginas — conferido
   no comprovante real, onde as três páginas tinham o mesmo número de controle e
   identificadores diferentes. Por isso ele entra por último na extração.
3. **Emparelhamento estável.** Página na ordem, SP na ordem, para o mesmo lote
   reenviado não trocar as atribuições.

**Limite conhecido:** o desempate só enxerga o anexo atual. Dois comprovantes de
mesmo valor em PDFs separados, ainda que no mesmo envio, continuam pendentes.
Estender exigiria guardar a página isolada de cada pendente até o fim do lote —
é possível e barato (pendentes são poucos), mas não foi feito.

### 11/09/2026 — três acertos no aviso (publicado, `546b45a`)

**O destino virou o financeiro.** O dono trocou o próprio celular pelo número do
financeiro, para o recado chegar a quem resolve. Continua sendo **um destino
só**, e `BAIXABRADESCO_AVISO_TELEFONE` troca sem mexer no código.

**O aviso passou a trazer os números das SPs.** *"Você identificou que tinha
doze SPs mas não colocou qual é o número delas."* Sem os números, a mensagem
dizia que havia candidatas e não dizia quais — e quem lê não tinha por onde
começar. Agora cada linha traz o número da SP escolhida, ou a lista das
candidatas (até doze, e o total quando passa disso).

**O aviso passou a explicar o empate.** Quando o desempate por lote não acontece,
o motivo que ia na mensagem era o do casador — técnico ("retornou 2
candidatos"), e não dizia o que fazer. Agora o próprio desempate escreve a
explicação: *"quantidades diferentes, não dá para distribuir sem marcar alguma SP
como paga sem ter sido"* ou *"parecem ser o MESMO pagamento (identificador
repetido ou ausente)"*. Há teste garantindo que explicar melhor **não** mexe no
status — continua pendente, ninguém baixa.

---

## Estado no fim de 11/09/2026

Tudo publicado. A área fechou o dia com:

- os **dois caminhos da Somapay** funcionando (depósito direto e transferência
  Bradesco → Somapay), com a conta de destino vindo da BaseBancos pela chave PIX;
- comprovante recusado pelo banco barrado antes de virar baixa;
- a trava contra baixar duas vezes ligada de fato;
- **aviso por WhatsApp, só para o dono, do que não foi baixado.**

**Publicado até `546b45a`**, com a `main` de outro chat (Análise de SPs) trazida
para o ramo antes da junção, como manda o `CLAUDE.md`: 2706 testes verdes e os
blueprints subindo com os dois trabalhos juntos.

**O que conferir nos próximos lotes reais**, nesta ordem:

1. O aviso chega nos **dois** WhatsApp (financeiro e dono). Se não chegar:
   toggle `NOTIFICAR_WHATSAPP` ligado, credencial Z-API chegando no pedido do
   Make, número certo.
2. Dois comprovantes de mesmo valor no mesmo PDF, com duas SPs de mesmo valor,
   baixam os dois — e o aviso não menciona nenhum deles.
2. A transferência aparece no Omie entre as contas certas, e a baixa cai na
   conta Somapay — não na do Bradesco.
3. Nenhum comprovante bom sendo barrado por engano.

**O que continua em aberto:**

- Rescisão sem a coluna Agendado preenchida continua não sendo encontrada — é
  decisão do dono, a marcação é controle de verdade. O aviso agora conta quando
  isso acontecer, que era a peça que faltava para ele perceber.
- Pix, boleto, transferência comum, FGTS e BeeVale continuam **sem teste** sobre
  a leitura dos campos e a escolha da SP. Faltam comprovantes de exemplo de cada
  tipo.

### 13/09/2026 — o leitor do Sicredi estava desligado por um engano meu (publicado, `27f62fb`)

Um comprovante real do Sicredi, de R$ 10.861,20, voltou `nao_localizado`. O robô
mandava toda página para o leitor do **Bradesco**, e para esse papel ele saía
**sem valor, sem conta e sem número de SP** — nada com que procurar.

**A causa não foi o código, foi a leitura errada de uma frase.** Em 11/09/2026 o
dono disse que não usava "as outras" — falando das duas contas **Somapay**
(INFRADENDE e IFPESANTACRUZ), das quais só a BWS é usada para baixa. Eu entendi
que o **Sicredi** não era mais usado, registrei isso no README e no HISTORICO
como decisão dele, e deixei o leitor desligado. O Sicredi é usado normalmente.

**Lição:** decisão do dono que desliga um caminho inteiro merece ser repetida de
volta com o nome do caminho antes de virar registro. "Não usamos mais" é uma
frase curta demais para uma consequência dessa.

**O que o leitor certo enxerga, no mesmo papel:** valor 10.861,20 (o Sicredi
escreve "Valor Pago (R$):", com o "(R$)" no meio, que o leitor do Bradesco não
reconhece), cooperativa 02205 e conta de origem, e — o mais importante — **o
número da SP**, que vem em "Descrição do Pagamento". Com o número, o casamento é
o mais confiável que existe: não depende de valor nem de conta.

**Como ficou:** o fluxo escolhe o leitor pelo formato do papel (cooperativa +
conta de origem → Sicredi). O diagnóstico faz o mesmo desvio — diagnóstico que lê
diferente do fluxo real mente para quem investiga.

⚠️ **A palavra "sicredi" sozinha deixou de servir como pista**, de propósito: ela
aparece em comprovante do **Bradesco** quando o destino é uma conta Sicredi, e
mandá-lo para o leitor errado pode produzir valor errado. Não reconhecer é
barato (cai no leitor do Bradesco, não acha SP, fica pendente e o aviso conta);
ler errado, não.

**Publicado em 13/09/2026 (`27f62fb`)**, com a `main` de dois dias de outros
chats (40 commits) trazida para o ramo antes da junção: 2907 testes verdes e os
blueprints subindo com tudo junto.

**Conferir no primeiro comprovante Sicredi real:** que ele acha a SP pelo número
que vem em "Descrição do Pagamento", e que a baixa cai na conta Sicredi certa.
O leitor nunca rodou em produção — este será o primeiro uso de verdade.

**Aberto, e é de decidir:** o aviso chegou ao dono **pelo Telegram**, não pelo
WhatsApp. Isso quer dizer que a perna do WhatsApp não está entregando — e que o
**financeiro provavelmente não recebeu nada**, porque o Telegram só alcança quem
já conversou com o robô. O campo `aviso` da resposta ao Make diz o motivo em uma
linha; ninguém foi atrás ainda.

### 13/09/2026 — a baixa pela metade que o reenvio não consertava (publicado, `c4c4724`)

Pergunta do dono: *"se eu enviar um comprovante que já foi baixado, ele checa por
onde? É conferido se a baixa está no Omie e na planilha? Às vezes falha um dos
dois e, se eu enviar novamente, é pra concluir a baixa."*

**A resposta era não, e o desenho era o pior possível.** A baixa acontece em duas
etapas: o Omie primeiro, a planilha depois, em segundo plano. A impressão digital
do comprovante era registrada **assim que o Omie aceitava**. Se a gravação na
planilha falhasse em seguida, três coisas aconteciam juntas:

1. a SP ficava **"Pagar"** na planilha, para sempre;
2. o erro sumia — `execute_spsbd_updates` engolia qualquer exceção num
   `except: pass`, e o `_executar_sheets_async` engolia de novo;
3. o comprovante reenviado era **barrado como repetido**, em silêncio.

Ou seja: o único caminho de conserto estava fechado, e ninguém era avisado.

**O que mudou:**

- A lista de comprovantes já baixados passou a trazer **o número da SP** junto
  da impressão digital (as duas colunas numa leitura só — a regra de uma leitura
  por lote continua valendo).
- Na conferência, se a SP daquele comprovante **ainda está entre as que faltam
  pagar**, a baixa ficou pela metade e o reenvio **passa**. O Omie responde
  "título já pago", o robô pula essa parte e termina o que faltava na planilha.
  Esses casos aparecem no retorno em `baixas_concluidas`, separados dos
  `duplicados_ja_baixados`.
- **A gravação na planilha deixou de falhar em silêncio.** Ela agora diz se
  gravou, e o erro vai para a fila de tentativas — como já acontecia com Pipefy
  e WhatsApp. Era a única das três escritas que sumia sem deixar rastro.

**O que continua não sendo feito, e é bom saber:** o robô **não** consulta o Omie
nem lê a linha da SP para decidir se um comprovante é repetido. A decisão sai da
lista dele mais o estado da SP na carga do lote, que já está em memória. Consultar
o Omie por comprovante repetido custaria uma chamada por página, e a lista já
responde bem.

**Não verificado:** nada disso passou por produção. O caso exige que a gravação
na planilha falhe de verdade, o que não dá para provocar daqui.

### 13/09/2026 — a outra metade: planilha paga, Omie pendente, reenvio sem efeito (publicado, `c4c4724`)

Na mesma conversa, o dono achou **duas SPs** com a planilha gravada por inteiro e
o Omie **não** baixado — conferiu nas duas fontes. Reenviar o comprovante não
fazia nada.

**É o espelho do buraco anterior, e tinha causa própria.** Existe um caminho para
"planilha paga, Omie pendente" (`load_spsbd_omie_pendente`), mas ele exigia a
**data de pagamento vazia**. Só que a gravação escreve status, carimbo, data,
comprovante e conta **de uma vez**: uma SP com a planilha completa ficava fora do
índice. O caminho de conserto só servia para gravação pela metade — justamente o
caso que **não** era o dele.

**Como ficou:** o índice passou a aceitar também SP paga nos **últimos 30 dias**.
A janela existe por memória: sem ela, "Pago + com comprovante" traria dezenas de
milhares das ~52 mil linhas. Trinta dias é o tempo em que alguém ainda percebe e
reenvia.

**O que já funcionava e vale saber:** comprovante que traz o **número da SP** não
depende de nada disso — o robô vai direto ao Omie por aquele número, mesmo com a
SP já Pago na planilha. Quem dependia do índice era o comprovante sem número
(depósito Somapay, transferência, boleto sem ID).

**Não verificado:** as duas SPs do dono não foram consertadas por aqui. Depois de
publicado, reenviar os comprovantes delas deve resolver — e é a primeira coisa a
conferir.

**Publicado em 13/09/2026 (`c4c4724`).** Junto veio da `main` um achado de outro
chat que toca esta área: a tela de comprovantes do **Análise de SPs** chamava
este robô **em modo de ensaio** — `modo_teste` é `True` por padrão quando o
pedido não diz o contrário, e aquele pedido não dizia. Toda baixa feita por
aquela tela desde a estreia foi simulação, e a tela ainda dizia "Baixado". O
caminho do Make nunca foi afetado. Corrigido lá; fica registrado aqui porque o
padrão perigoso é **deste** módulo.

**Primeira coisa a conferir agora:** reenviar os comprovantes das duas SPs com
planilha paga e Omie pendente. Devem concluir.

---

## 16/09/2026 — ⚠️ "A chave de acesso não está preenchida ou não é válida"

**A frase que faltava há dois dias**, e ela é do Omie, num comprovante do Sicredi
enviado pela tela do Análise de SPs:

    Falha ao alterar título. Baixa cancelada.
    O Omie respondeu: A chave de acesso não está preenchida ou não é válida.

E a pergunta do dono, que é a que destrava: *"só não compreendo por que o
baixabradesco no método anterior funciona e via Análise não."*

**O robô é o MESMO. O que muda é de onde vem a chave de acesso do Omie:**

- o **Make** manda `app_key` e `app_secret` **dentro do pedido**;
- o pedido que sai do **Análise de SPs** não manda — ele conta com as variáveis
  de ambiente do serviço.

**E aqui está o defeito, que é de NOME e não de lógica** — confirmado pelo dono
mandando a lista de variáveis do Render:

| Onde | O que o código procurava | O que existe no Render |
|---|---|---|
| `painel`, `emissaonf` | `OMIE_KEY`, `OMIE_SECRET` (+ apelidos) | ✔ acha |
| `baixabradesco` | **só** `OMIE_BWS_APP_KEY`/`SECRET` | ✘ não acha |

Os outros dois módulos aceitam os dois apelidos **há meses**. Este aceitava só o
antigo. Por isso o painel e a emissão de NF funcionavam com o Omie e este robô
não — e pelo Make nunca apareceu, porque o Make manda a chave no pedido.

Faltando a variável, o pedido saía com a chave **vazia**, e o Omie respondia
*"A chave de acesso não está preenchida ou não é válida"*. "Chave de acesso",
no vocabulário do Omie, é a **credencial da API** — não é a chave do título. A
mensagem parecia falar do título, e a investigação olhou para o lado errado
durante dois dias.

⚠️ **E o `CONTEXTO.md` ajudou a esconder:** a seção 4.5 listava só
`OMIE_BWS_APP_KEY`/`SECRET` como se fossem os nomes em uso. Corrigido junto.

**A lição:** apelido de variável resolvido em três arquivos diferentes vira três
regras diferentes no dia em que alguém cadastra a variável com um dos nomes. Há
teste agora exigindo que os nomes aceitos aqui sejam os mesmos do painel.

### Os quatro consertos

1. **Sem credencial, não se manda nada.** A sequência para antes do primeiro
   pedido e diz **qual variável falta**. Antes, mandava com a chave vazia e
   colhia uma mensagem que falava de outra coisa.
2. **Consulta que não deu certo interrompe.** Antes só interrompia com HTTP 500
   ou faultcode `nao_encontrado` — e o Omie responde **200 com `faultstring`**
   em vários casos. Esses passavam e iam alterar um título que ninguém
   confirmou que existe.
3. **A alteração só acontece se algo diverge de verdade.** O passo rodava
   SEMPRE, apoiado num comentário que dizia *"verificação simplificada: tenta
   sempre, Omie idempotente"* — suposição nunca verificada, e falsa: quando o
   Omie recusa a alteração, a **baixa é cancelada**. Um passo que na maioria das
   vezes não precisava acontecer estava impedindo o que precisava. Agora o
   título consultado manda: valor e conta já certos, alteração pulada.
   ⚠️ **Na dúvida, altera** — consulta incompleta volta ao comportamento antigo,
   porque deixar de alterar um título que precisa seria baixar com valor errado.
4. **O Omie manda na planilha e no card.** Eram marcados como pagos
   **independente** do que o Omie respondesse — a dessincronia que o dono
   relatou em 14/09 (*"baixam na planilha, mas não baixam no Omie"*). Agora: se
   a baixa era para acontecer e não se confirmou, **não se marca nada**, e a
   tela diz que não marcou. "Já estava pago no Omie" conta como confirmação.

### A lição, e ela vale para o monorepo inteiro

**Mensagem de terceiro não se lê pelo que ela parece dizer.** "Chave de acesso"
podia ser a chave do título ou a credencial da API, e a diferença é tudo. O que
resolveu foi a mesma coisa de sempre: **guardar a frase inteira do outro lado**
e mostrá-la a quem pode agir.

**Não verificado:** nada disto rodou contra o Omie de verdade — não há
credencial neste ambiente. O que os testes provam é que, sem credencial, nenhum
pedido sai; que consulta falha interrompe; e que título já certo não é alterado.

### 17/09/2026 — dois Pix diferentes que o robô achou que eram o mesmo (publicado, `16e3a51`)

Dois Pix do Sicredi de R$ 7.300,00, para duas SPs de mesmo valor, as duas
agendadas (1445859706 e 1445866267). O desempate por lote recusou distribuir:
*"parecem ser o MESMO pagamento (identificador repetido ou ausente)"*.

Estava **ausente**, e eram dois descuidos somados, os dois meus:

1. **O leitor do Sicredi nunca preenchia o identificador.** Quando o liguei, em
   13/09, levei os campos de valor, data, conta e SP — e esqueci esse. Ele só
   existia no leitor do Bradesco.
2. **Mesmo preenchendo, os rótulos do Sicredi não estavam na lista.** Ela
   conhecia "Identificador", "Documento" e "N° de controle" (Bradesco). O
   Sicredi escreve **"ID da transação"**, **"Autenticação Eletrônica"** (com
   pontos) e **"Número de Controle"** (por extenso, sem o símbolo de grau).

**Como ficou:** o leitor do Sicredi preenche o identificador, e a lista passou a
conhecer os três rótulos. O "ID da transação" vem primeiro — é o identificador
canônico do Pix.

⚠️ **Os dois rótulos de "controle" continuam por último**, e por um motivo que
não é óbvio: no **Bradesco** o número de controle é do **lote inteiro** e se
repete entre as páginas; no **Sicredi** ele muda a cada pagamento. Mesma palavra,
semânticas opostas. Deixá-los no fim faz o Sicredi ser resolvido antes pela
autenticação, e o Bradesco continuar caindo no "Documento", que é o dele por
pagamento.

**A trava funcionou como devia.** Ela recusou distribuir porque não conseguia
provar que eram pagamentos diferentes — e era exatamente o que não dava para
provar. O defeito estava na leitura, não na regra.

**Verificado** com os dois comprovantes reais, guardados anonimizados: os
identificadores saem diferentes e as duas SPs são distribuídas.
**Não verificado:** não passou por produção.

**Publicado em 17/09/2026 (`16e3a51`)**, com a `main` de quatro dias de outros
chats (80 commits) trazida para o ramo antes da junção: 3122 testes verdes e os
blueprints subindo com tudo junto.

⚠️ **Dependência nova no monorepo** (`erpbrasil.edoc`, `erpbrasil.assinatura`),
trazida por outro chat para a conciliação fiscal do Análise de SPs. Sessão que
rodar a suíte sem instalar o `requirements.txt` atualizado vê cinco testes
falharem por módulo ausente — não é defeito.

**Nesta mesma junção veio, de outro chat, a causa raiz do "baixa na planilha e
não baixa no Omie"**: a credencial do Omie chega pelo pedido quando é o Make, e
por variável de ambiente quando é a tela do Análise de SPs — que estava sem
elas. A mensagem do Omie ("chave de acesso não está preenchida") fala da
CREDENCIAL DA API, não da chave do título, e isso desviou a investigação por
dois dias. Junto vieram três endurecimentos no `core.py` deste módulo: não
mandar pedido sem credencial, interromper quando a consulta não confirma o
título, e só alterar o título quando algo diverge de verdade. Vale ler o
registro daquele chat antes de mexer na sequência do Omie.

### 07/10/2026 — a não-atualização silenciosa da SPsBD: quatro causas, não uma

Queixa do dono: *"tem algo que tem acontecido com muita frequência: a não
atualização silenciosa da aba SPsBD. Precisa criar uma sistemática pra impedir
que isso aconteça."*

Em 13/09 eu tinha tratado **uma** das causas (o erro engolido num `except`
vazio). As outras três continuavam, e juntas explicam a frequência:

1. **A gravação rodava numa thread solta, e a resposta saía antes dela
   terminar.** O gunicorn recicla o trabalhador a cada mil pedidos
   (`--max-requests 1000`) e o serviço reinicia a cada publicação — nos dois
   casos a thread morre no meio, sem erro em lugar nenhum. **Era a causa
   principal**, e explica por que acontecia "com frequência" e sem padrão.
2. **O resultado da gravação era escrito no plano DEPOIS** de a resposta já ter
   sido montada. Quem lia o retorno do Make nunca via o que aconteceu — havia
   uma corrida entre a thread e a montagem da resposta.
3. **Ninguém conferia** se a célula ficou com o valor. O `batch_update` não
   reclama quando a escrita não vale.
4. **A fila de falhas só andava se alguém chamasse a rota à mão** — e o
   reprocessamento de planilha **ignorava o resultado** da gravação: marcava
   "reprocessado com sucesso" e tirava o item da fila sem ter gravado. Era o
   último lugar onde a perda acontecia em silêncio.

**A sistemática que ficou:**

- **Gravar, conferir, e só então responder.** Lê de volta a coluna de status e
  compara. Custo: cerca de um segundo por comprovante na resposta, contra o
  limite de 300 segundos do Make. Baixa errada custa mais do que um segundo.
- **Uma segunda tentativa imediata** antes de desistir (cota do Google é por
  minuto; resposta parcial costuma passar na segunda).
- **Falha confirmada vai para a fila e para o aviso**, com o resultado real no
  retorno.
- **Cada lote drena algumas pendências da fila** (limite 5, para não esticar a
  resposta). Comprovante chega sempre; a fila anda junto.
- **O reprocessamento não mente mais**: devolve o resultado da gravação.
- **Item que esgotou as cinco tentativas (FALHOU) pode voltar**, com
  `incluir_falhados` no pedido de reprocessamento. Antes ficava abandonado na
  planilha para sempre — não-atualização silenciosa com outro nome. O
  reprocessamento automático **não** os inclui: insistir de minuto em minuto no
  que já falhou cinco vezes só gasta cota.

**Sobre o lado do Omie**, que ele levantou junto: aquele caminho já estava
coberto desde 17/09 (não manda pedido sem credencial, interrompe quando a
consulta não confirma o título, só altera quando algo diverge) e a falha já ia
para a fila e para o aviso. O que faltava era a fila **andar** — e agora anda.

**O que a leitura da planilha mostrou, e é decisão do dono:** a aba
`BaixaBradescoFila` tem **cerca de 2.270 linhas** acumuladas. Não dá para dizer
daqui quantas são pendências de verdade e quantas são histórico, porque o
conector do Google devolve só o cabeçalho de abas grandes. Com a drenagem
automática, 5 por lote, uma fila de pendências antigas leva muitos lotes para
andar — se forem muitas, vale uma chamada manual à rota de reprocessar com
`limite` alto e `incluir_falhados`.

⚠️ **Achado de segurança, de passagem:** a planilha *Registro de SPs* guarda, na
aba `FilaAppWeb`, a **chave e o segredo da API do Omie** em texto, numa coluna do
payload. Quem tem acesso à planilha tem as credenciais do Omie. Não foi mexido
nem copiado para lugar nenhum — fica registrado para o dono decidir (o caminho
seria o Make e o Análise de SPs lerem de variável de ambiente, como o resto).

**Verificado:** 5.131 testes passando (14 novos desta entrega) e a aplicação
subindo. A única falha na rodada local é biblioteca ausente neste ambiente
(`erpbrasil`), e ela falha igual na `main` publicada sem o meu trabalho.
**Não verificado:** nada disso passou por produção. A prova é a primeira baixa
real depois de publicado — e, se a gravação falhar, o aviso tem de chegar.

---

### 08/10/2026 — "e essa fila, de 2.270 linhas, vai rodar?" — não ia

Pergunta do dono, logo depois da entrega anterior. A resposta honesta era
**não**: no ritmo automático de cinco por lote, uma fila de pendências antigas
não anda. Três coisas impediam, e as três foram tratadas.

**1. Ninguém sabia quantas das 2.270 linhas eram pendência de verdade.** A aba
guarda **tudo** que já passou por ela, concluído inclusive — então o número de
linhas não é o número de pendências. Eu disse isso na entrega anterior e parei
aí, o que é pouco: ficou uma pergunta sem meio de resposta. Agora existe
`GET /api/baixabradesco/fila-resumo`, que conta por situação (`PENDENTE`
vencido, `PENDENTE` agendado, `FALHOU`, `CONCLUIDO`), por etapa e por tipo de
falha, diz a data do registro mais antigo, e não grava nada. Diagnóstico se faz
pelo sistema, não abrindo a planilha na mão.

**2. Ler a fila custava a aba inteira.** `_rows_as_dicts` fazia
`get_all_values()` — as 2.270 linhas **com o JSON do payload de cada uma** — só
para achar cinco. E `ensure_fila_sheet` fazia o mesmo, em **toda** chamada,
apenas para conferir o cabeçalho. Isso é exatamente o que `CONTEXTO.md` §3.7
proíbe, e era o tipo de leitura que causou o OOM de julho de 2026. Agora: o
cabeçalho lê `A1:O1`; o filtro lê `A2:L` (as colunas leves, sem a mensagem de
erro nem o payload); o payload vem só das linhas escolhidas, em blocos de cem.

**3. Drenar de uma vez estouraria a cota de todo mundo.** A cota de escrita do
Google é **por minuto** e é do **mesmo usuário de serviço** que o ERP, o painel e
o Análise de SPs usam — uma drenagem de centenas de itens no soco não quebraria
só esta fila, tiraria os outros do ar. Três medidas: marcar a linha passou a
custar **uma** chamada de escrita em vez de duas; lote acima de 20 itens anda com
**pausa** entre eles (1,2 s, ajustável por `pausa_ms`); e a varredura **para
sozinha** na terceira recusa seguida por cota, devolve o que fez e deixa o resto
`PENDENTE` para a próxima passada.

**Uma trava de bom senso que entrou junto:** aviso de WhatsApp parado na fila há
mais de três dias **não é reenviado**. Drenar fila velha mandaria para os dois
celulares avisos sobre problemas provavelmente já resolvidos na mão — e aviso
demais faz a pessoa parar de ler, que é o oposto do que o aviso existe para
fazer. Ele sai da fila com o motivo escrito na linha. Quem quiser o contrário
manda `reenviar_avisos_antigos: true`. A trava vale **só** para aviso: baixa de
dois meses atrás continua sendo baixa, e o dinheiro não envelhece.

**O que NÃO é risco, e vale estar escrito:** repetir uma baixa não baixa duas
vezes. O reprocessamento do Omie consulta o título primeiro e, se já estiver
`PAGO`, dá a pendência por resolvida sem lançar nada. Então drenar fila antiga
não duplica pagamento no Omie. O reprocessamento de planilha regrava as mesmas
células — se alguém tiver corrigido aquela linha na mão com outra informação, a
regravação passa por cima. É o único efeito colateral conhecido da drenagem.

**O que continua fora do alcance da fila, e não tem volta por ela:** item que o
reprocessamento antigo marcou `CONCLUIDO` sem ter gravado (o defeito corrigido
em 07/10). Para a fila ele está resolvido; a pendência real, se existir, só
aparece na comparação entre a SPsBD e o Omie, não aqui.

**O caminho prático para zerar o acumulado**, depois de publicado: primeiro o
resumo, para saber o tamanho; depois `POST /api/baixabradesco/reprocessar-fila`
com `limite` alto e `incluir_falhados: true`, uma chamada por vez, olhando o
campo `interrompido` da resposta — se vier `cota_do_google`, esperar alguns
minutos e repetir.

**Verificado:** suíte inteira rodada, uma única falha e é a biblioteca ausente
deste ambiente (`erpbrasil`), que falha igual na `main` publicada sem o meu
trabalho; 16 testes novos cobrindo a contagem, a leitura limitada, a pausa, a
parada por cota e a trava de aviso antigo; aplicação subindo com os 18
blueprints e a rota nova no lugar.
**Não verificado:** nada disso encostou na planilha de verdade. Quantas das
2.270 linhas são pendência real continua sem resposta até alguém chamar o
resumo em produção — e é a primeira coisa a fazer depois de publicar.

---

### 08/10/2026 (depois de publicar) — o conferidor SPsBD × Omie

Publicado na `main` em `bacc190`, com o "pode" do dono: o conserto da
não-atualização silenciosa da SPsBD e a fila contável e drenável. Sem migração
de banco.

Na mesma resposta eu levantei um buraco que nenhuma das duas entregas fecha, e
em seguida o fechei em vez de esperar resposta — a regra do `CLAUDE.md` é clara
e o custo de esperar é horas paradas dele.

**O buraco:** até 07/10 o reprocessamento de planilha marcava o item como
"concluído com sucesso" **ignorando o resultado da gravação**. Então há itens
que saíram da fila sem nunca ter sido gravados. Para a fila eles estão
resolvidos — e nenhuma passada, por mais completa, os traz de volta. A pendência
real, se existir, só aparece comparando as duas fontes lado a lado: a planilha
diz "Pago", o Omie diz "Aberto". Isso é dinheiro que saiu da conta sem o título
baixar.

**O que foi feito:** `GET /api/baixabradesco/conferir-omie`. Lê a SPsBD, pergunta
ao Omie título por título, e relata. Decisões que importam:

- **Não grava nada**, nem na planilha nem no Omie. Relatório é relatório. Um
  conferidor que também corrigisse erraria em silêncio na primeira divergência
  de valor, e aí seria pior que não ter conferidor. Corrigir é decisão de quem
  lê — e, quando o dono pedir, a correção entra como passo separado e explícito.
- **Separa três coisas que não são a mesma:** planilha paga com Omie aberto (a
  baixa pela metade, a única que custa dinheiro); código de integração que não
  existe no Omie (cadastro errado na planilha); e falha de consulta (rede ou
  cota). Misturar os três faria o relatório mentir.
- **Lê sete colunas da SPsBD**, não `A:AK`. A aba tem ~52 mil linhas × 37
  colunas e a leitura inteira custa 150–250 MB — foi assim que o serviço caiu
  por memória em julho de 2026.
- **`apenas_contar=1` mede o tamanho sem gastar consulta ao Omie por linha**, e
  o `limite` segura quantas consultas vão por chamada (50 por padrão).
- **Sem credencial do Omie ele recusa** em vez de relatar "nenhuma divergência",
  que é a resposta mais perigosa possível para um conferidor.

**Decisão de janela:** 60 dias por padrão, ajustável por `dias`. Não é limite
técnico: a SPsBD tem anos de histórico e conferir tudo seriam dezenas de
milhares de consultas ao Omie. Sessenta dias cobre com folga o período em que o
defeito de 07/10 esteve vivo nesta forma.

**O que ficou pendente do dono, e trava trabalho de verdade:**

1. **Chamar a contagem da fila em produção.** Eu não consigo daqui: o endereço
   do serviço e o `BAIXABRADESCO_SECRET` ficam nas configurações do Render, e
   senha não entra no chat. Sem isso, "2.270 linhas" continua sendo um número
   sem significado — pode ser 50 pendências ou 2.000.
2. **Decidir o que fazer com as divergências** que o conferidor achar. A
   correção automática não foi escrita de propósito.

**Verificado:** 14 testes novos nesta entrega (30 somando com a da fila), a área
inteira passando, e a aplicação subindo com os 18 blueprints e as duas rotas
novas registradas.
**Não verificado:** o conferidor nunca encostou na planilha de verdade nem no
Omie de verdade. Os dublês cobrem a regra; a primeira chamada em produção é a
prova — e é ela que vai dizer se o defeito de 07/10 deixou prejuízo escondido.

---

### 08/10/2026 — a contagem da fila em produção: ela nunca andou, nem uma vez

O dono chamou `fila-resumo` em produção. A resposta mudou o entendimento do
problema, e vale copiada aqui porque é a prova:

```
linhas_na_aba          2269
por_status             PENDENTE 2269
pendentes_vencidos     2269
pendentes_agendados    0
concluidos             0
falhados               0
por_etapa              zapi 1943 | omie 238 | pipefy 88
por_tipo_falha         zapi_erro 1943 | omie_erro 238 | pipefy_erro 88
sem_etapa              0
registro_mais_antigo   18/06/2026 18:31:09
registro_mais_recente  07/10/2026 20:45:45
```

**Nenhuma concluída e nenhuma falhada.** Isso não é uma fila lenta: é uma fila
que **nunca andou, nem uma vez, em quase quatro meses**. Se tivesse andado,
haveria linhas `CONCLUIDO`; se tivesse insistido e desistido, haveria `FALHOU`.
A rota de reprocessar existia, funcionava, e ninguém nunca a chamou — porque ela
só andava à mão. Minha entrega da manhã (drenar 5 por lote) teria levado 454
lotes de comprovantes para dar uma volta.

**Três consequências que eu tinha anotado como risco e que a contagem desfez ou
confirmou:**

1. **O grupo "marcado CONCLUIDO sem ter gravado" está VAZIO** — `concluidos: 0`.
   Aquele defeito nunca chegou a apagar nada, porque o reprocessamento nunca
   rodou. O conferidor SPsBD × Omie continua útil, mas por outro motivo (ver a
   entrada anterior), não por este.
2. **`sheets: 0`.** A não-atualização silenciosa da SPsBD **nunca passou pela
   fila** — o que confirma o diagnóstico de 07/10: a gravação morria numa thread
   solta, antes de chegar ao `enqueue_failure`. De agora em diante uma gravação
   falhada aparece aqui.
3. **1.943 das 2.269 (86%) são `zapi`** — as mensagens *"Informação de
   Pagamento"* que avisam quem pediu a SP, com o comprovante em PDF. Ou seja:
   desde junho, quem pede uma SP vem não sendo avisado de que o pagamento saiu, e
   ninguém soube. Os 238 `omie` são o que custa dinheiro: baixa que não
   aconteceu.

**O que foi feito, em cima desses números:**

- **A fila pegou carona no cron que já roda de 5 em 5 minutos**
  (`/processar-fila-tardia`, já autenticado). Pedir que alguém chame a rota à mão
  foi o que falhou por quatro meses; não vale repetir o pedido com mais ênfase.
- **Drenagem por etapa, na ordem da importância**: `omie` (dinheiro), `sheets`,
  `pipefy`, `zapi` (recado) — 15, 15, 15 e 10 por disparo, ~180 itens por hora.
  Na ordem da planilha, 1.943 recados ficariam na frente de 238 baixas.
- **Filtro `etapas` na rota de reprocessar**, para drenar só o dinheiro quando
  for o caso.
- **Falta de credencial não consome tentativa** (`ERROS_DE_CONFIGURACAO`). Isto
  era urgente: a chave do Omie está na planilha, não no ambiente (ver o achado de
  segurança da entrada anterior). Se a drenagem automática subisse sem essa
  guarda e o ambiente não tivesse a credencial, cinco passadas marcariam as 238
  baixas como `FALHOU` — apagando a pendência sem resolver nenhuma. Agora a linha
  fica intacta e o relatório diz `o_que_falta_configurar`.
- **O cron não limpa o acumulado de avisos antigos.** Ele pula o que tem mais de
  três dias, sem gastar tentativa. Marcar 1.943 linhas é decisão do dono, e sai
  em blocos de 50 linhas por chamada quando ele pedir — uma escrita por linha
  seriam 1.943 escritas, meia hora travando a cota do ERP e do painel.

**E o item que estava aberto desde 13/09 foi fechado, porque a contagem explicou
o sintoma:** o aviso chegou ao dono **pelo Telegram**. O envio devolve sucesso
quando QUALQUER canal entrega, e o Telegram dele entrega quase sempre — então o
WhatsApp falhava para o financeiro, que não tem Telegram cadastrado, e tudo
reportava sucesso. Os 1.943 `zapi_erro` na fila são a escala disso. Dois
consertos: o resultado passou a dizer `entregues_no_whatsapp`,
`so_pelo_telegram` e `sem_entrega`, com alerta em português; e **credencial
presente com envio falhando ganhou segunda tentativa** pelo notificador comum —
antes a segunda tentativa só existia quando a credencial estava *ausente*.

**Decisões que tomei sozinho, e o dono pode desfazer:**

- 15/15/15/10 por disparo do cron. Conservador de propósito: a cota de escrita do
  Google é por minuto e é da mesma credencial do ERP, do painel e do Análise de
  SPs. Subir é fácil se a memória e a cota aguentarem.
- Três dias para o aviso perder a utilidade.
- O conferidor não corrige, só relata.

**O impedimento da credencial do Omie NÃO existe — eu errei o alarme.** Eu avisei
ao dono que a drenagem automática talvez não achasse a chave do Omie, porque ela
vive na planilha. Ele respondeu que o Render tem `OMIE_KEY` e `OMIE_SECRET`, e
esses são justamente os **primeiros** nomes que `NOMES_APP_KEY` /
`NOMES_APP_SECRET` procuram. A drenagem acha a credencial sozinha. (O achado de
*segurança* continua de pé por outro motivo: a chave também está em texto na aba
`FilaAppWeb`, e quem abre a planilha a lê.)

**Conferindo isso, apareceu o mesmo risco por outro caminho:** sem
`PIPEFY_API_TOKEN`, `execute_graphql` **levanta exceção** — e a exceção caía no
`except` geral do laço, que incrementa a tentativa. Cinco passadas sem token
marcariam os 88 cartões como `FALHOU` sem nunca ter tentado nada. Agora o token
é conferido antes, e a falta dele é problema de configuração, não tentativa
gasta. As três credenciais (`OMIE_*`, `PIPEFY_API_TOKEN`, `ZAPI_*`) têm teste
travando o nome da variável, porque o cron só lê do ambiente e trocar o nome em
silêncio pararia a drenagem inteira.

**O que continua pendente do dono:**

1. **Publicar** (isto e o conferidor estão no ramo, não na `main`).
2. **Decidir sobre os 1.943 avisos antigos**: descartar em massa (recomendação) ou
   reenviar.
3. **Conferir se `PIPEFY_API_TOKEN` e as três `ZAPI_*` estão no Render.** Se não
   estiverem, os 88 cartões e os avisos recentes ficam parados — e o relatório da
   drenagem vai dizer isso em `o_que_falta_configurar`, em vez de fingir que
   tentou.

**Um defeito que eu mesmo introduzi e achei antes de publicar:** pular o aviso
antigo **depois** de carregá-lo entupia a fila. Ela é lida em ordem — com 1.943
avisos de junho na frente, pedir "dez avisos" devolvia sempre os dez mais
velhos, que seriam pulados, e o aviso de ontem nunca era alcançado. A fila
engasgava com o que ela mesma ia descartar. O corte por idade passou para a
**escolha** das linhas. Vale só para `zapi`: baixa de junho continua sendo baixa.

**Verificado:** 27 testes de fila (13 novos), 11 de aviso, 7 de cron, 14 de
conferidor; suíte inteira rodada com a única falha sendo `erpbrasil` ausente
neste ambiente, que falha igual na `main` publicada; aplicação subindo com os 18
blueprints.
**Não verificado:** a drenagem pelo cron nunca rodou em produção. O primeiro
disparo depois de publicar é a prova — e o campo a olhar é
`fila_de_falhas.omie.o_que_falta_configurar`.

---

### 08/10/2026 (fim do dia) — publicado, e as respostas passaram a falar português

**Publicado na `main` em `26e9187`**, com o "pode" do dono e a confirmação de que
as variáveis do Render existem (`OMIE_KEY`, `OMIE_SECRET`, `PIPEFY_API_TOKEN`,
`ZAPI_*`). A `main` havia andado — outro chat publicou mexidas no painel —, então
a `main` veio para o ramo primeiro, a suíte rodou com as duas coisas juntas e só
então a junção. Sem conflito. Sem migração de banco.

Entrou: o conferidor SPsBD × Omie, a fila andando sozinha pelo cron de 5 em 5
minutos com o dinheiro na frente, o conserto do entupimento por aviso velho e a
guarda do token do Pipefy.

**Depois disso, uma coisa pequena e de efeito grande:** as respostas de
`fila-resumo` e `conferir-omie` ganharam um campo `em_portugues`, com uma frase
que diz o que os números querem dizer. O motivo é literal: o dono colou no chat
a resposta inteira de `fila-resumo`, campo por campo, para perguntar o que ela
significava. Ele lê isso pelo celular e não é programador — a resposta crua é
chave-e-número. A frase vem **junto** com os números, nunca em lugar deles.

Decisões pequenas registradas porque voltam a aparecer: a etapa aparece com nome
de gente ("baixa no Omie", "aviso de pagamento"), não com o nome técnico; a maior
quantidade vem primeiro; e a frase do conferidor **separa explicitamente** o que
é dinheiro (planilha paga, Omie aberto) do que é cadastro errado (código que não
existe no Omie) — juntar os dois assustaria sem motivo ou tranquilizaria sem
motivo.

**Verificado:** 9 testes novos sobre as frases, suíte inteira rodada (única falha
é `erpbrasil` ausente neste ambiente, que falha igual na `main` publicada),
aplicação subindo com os 18 blueprints.
**Não verificado:** a drenagem pelo cron ainda não foi observada em produção. O
número a acompanhar é `pendentes_vencidos`, que tem de cair dos 2.269.

### Pendente AGORA (para a próxima sessão desta área)

1. **Os 1.943 avisos antigos esperam decisão do dono** — descartar em massa
   (recomendação registrada) ou reenviar. O cron não toca neles.
2. **Confirmar que a drenagem andou**: `pendentes_vencidos` tem de cair. Se
   continuar em 2.269, olhar `fila_de_falhas` na resposta do cron.
3. **A chave do Omie em texto na aba `FilaAppWeb`** continua lá (achado de
   segurança). Não impede nada; é risco.
4. **Pix, boleto, transferência, FGTS e BeeVale seguem sem teste de campo** — falta
   um comprovante de exemplo de cada, que só o dono tem.

---

### 08/10/2026 (noite) — o dono apontou o alvo certo, e meu conferidor olhava para o outro lado

Com a drenagem já no ar, ele respondeu:

> *"está caindo o número, já vi. Esses comprovantes antigos eu já devo ter
> resolvido, e esses avisos antigos também. Fazemos conciliação bancária diária.
> No sistema Omie vai estar tudo atualizado. O furo pode ser mais na planilha e
> na movimentação do card."*

Três coisas nessa frase, e as três mudam o trabalho:

1. **A drenagem está funcionando** — o número cai. Primeira confirmação em
   produção.
2. **As 238 baixas do Omie provavelmente já estão pagas**, pela conciliação
   diária. O reprocessamento consulta antes e, se achar `PAGO`, resolve a
   pendência sem lançar nada — então a drenagem está fechando pendência de
   registro, não pagando nada de novo. Era o comportamento pretendido, e agora
   tem confirmação de por que ele era o certo.
3. **O furo é na planilha e no cartão — e o meu conferidor não enxergava isso.**

**O erro de direção, escrito para não se repetir:** o conferidor selecionava as
linhas em que a planilha diz **Pago** e perguntava ao Omie. Ou seja, só achava
"planilha paga, Omie aberto". O furo que ele descreve é o **contrário** — "Omie
pago, planilha não" —, e essa direção era **invisível** para o conferidor, porque
ela mora justamente nas linhas que a planilha ainda marca como "Pagar". Pela
conciliação bancária diária, é também a direção **mais provável** das duas.

E ela não aparece em lugar nenhum sem o conferidor: a fila de falhas tinha
**zero** pendências de planilha, porque a gravação morria antes de chegar ao
`enqueue_failure`. O sistema não tinha como saber que deixou de gravar.

**O que ficou:** o conferidor passou a rodar nos dois sentidos, e o relatório põe
o furo apontado por ele **na frente**, com nome próprio (`planilha_atrasada`).
`sentido` escolhe: `ambos`, `omie_pago` ou `planilha_paga`.

Decisões que valem registro porque são o tipo de coisa que se refaz errado:

- **As duas janelas usam datas diferentes, e têm de usar.** A direção antiga tem
  data de pagamento na planilha. A nova não tem — a planilha nem sabe que foi
  paga —, então a janela é pelo **vencimento**. Sem janela seriam ~52 mil
  consultas ao Omie. Vencimento muito à frente também sai: título que vence no
  ano que vem não é planilha atrasada.
- **Cada item da direção nova traz o link do cartão do Pipefy**, porque ele
  apontou os dois furos juntos. O conferidor não consulta o Pipefy (seria outra
  volta de API por item); entrega o link para quem for olhar.
- **O limite vale por direção**, não somado: pedir 50 faz até 50 consultas de
  cada lado, e não 25 de cada.
- **Continua sem corrigir nada.** Agora com mais razão: corrigir "planilha
  atrasada" é escrever na planilha a partir do que o Omie diz, e isso precisa de
  conferência de valor e de data — é um passo próprio, não um efeito colateral
  de um relatório.

**Um defeito meu no caminho, e conto porque é instrutivo:** ao reescrever a
seleção de candidatas, substituí um trecho grande de arquivo delimitado por
"daqui até a próxima função" — e a próxima função não era a que eu pensava.
Apaguei junto a função que monta a frase em português, sem perceber. A suíte
apontou na primeira rodada. Trecho grande se substitui por âncora exata, não por
intervalo.

**Decisão do dono registrada:** ele considera os comprovantes e os avisos antigos
já resolvidos. Então o descarte em massa dos 1.943 avisos deixa de ser dúvida e
passa a ser só uma chamada quando ele quiser — nada é apagado, a linha fica com o
motivo escrito.

**Verificado:** 23 testes no conferidor (10 novos, cobrindo a direção nova, as
duas janelas, o limite por direção e a ordem da frase), suíte inteira rodada com
a única falha sendo `erpbrasil` ausente neste ambiente, aplicação subindo com os
18 blueprints.
**Não verificado:** a direção nova nunca rodou contra a planilha de verdade. É a
primeira coisa a chamar depois de publicar, e com `apenas_contar=1` primeiro —
ela pode trazer centenas de linhas para conferir, e aí o custo é consulta ao
Omie.

---

### 08/10/2026 (noite, depois de publicar) — a continuação que a frase prometia e o código não cumpria

**Publicado na `main` em `7d4e0cc`**, com o "pode" do dono: o conferidor nos dois
sentidos, o conserto do segundo caminho de drenagem e as respostas em português.
A `main` havia andado outra vez (o chat do ponto publicou, com migração própria —
avisado ao dono), então a `main` veio para o ramo, a suíte rodou com as duas
coisas juntas, e só então a junção.

Revisando o meu próprio código depois de publicar, achei um defeito no que eu
tinha **escrito para o dono ler**: a frase em português dizia *"faltam N para
conferir — chame de novo para continuar"*, e isso era **mentira**. O conferidor
não grava nada, então nada sai do conjunto entre uma chamada e a seguinte:
chamar de novo reconsultaria as mesmas primeiras cinquenta linhas, para sempre.
Ele pagaria consulta ao Omie para reler o mesmo pedaço e nunca chegaria ao resto.

Entrou `pular`, que continua de onde parou, e a resposta devolve o
`proximo_pular` **pronto** — quem lê isto no celular não deve ter de calcular
nada. A frase só promete continuação quando existe continuação: na última
página ela não manda chamar de novo.

Registrado como lição porque é um tipo de erro que escapa fácil: **a frase em
português é interface, e interface que promete o que o código não faz é pior que
resposta crua.** Um teste cobre exatamente isso — frase sem continuação possível
não contém "chame de novo".

**Verificado:** 27 testes no conferidor (4 novos sobre a continuação), 11 sobre
as frases, suíte inteira rodada com a única falha sendo `erpbrasil` ausente neste
ambiente, aplicação subindo com os 18 blueprints.
**Não verificado:** nada do conferidor rodou contra a planilha de verdade ainda.

---

### 08/10/2026 — revisão do que passou a rodar sozinho, e uma ineficiência deixada de propósito

**Publicado na `main` em `6d307fe`:** a continuação do conferidor (`pular`). A
`main` havia andado outra vez (Análise de SPs, conciliação), mesmo
procedimento — `main` para o ramo, suíte inteira, junção. Sem migração.

Depois de publicar, reli o laço de drenagem com cuidado, porque ele agora roda
**sem ninguém olhando, a cada cinco minutos, contra os dados de verdade**. O que
a revisão mostrou:

**Está correto, e por quê, para não ser "consertado" errado depois:**

- **Os itens não são remartelados.** Quem falha recebe próxima tentativa em +10
  min, e a seleção só traz vencidos. Então cada disparo pega os *seguintes*, não
  os mesmos — é isso que faz 238 baixas levarem ~80 minutos em vez de girar no
  mesmo lugar.
- **As quatro leituras por disparo são de linhas estáveis.** `enqueue_failure`
  só acrescenta no fim e nada é apagado, então o número da linha não desloca
  entre uma etapa e a seguinte. Não há risco de marcar a linha errada.
- **O descarte em lote roda mesmo quando o laço para por cota**, e se a gravação
  falhar ali os itens ficam `PENDENTE` — o estado verdadeiro. Não se finge que
  gravou.

**A ineficiência, deixada como está de propósito:** drenar por etapa faz
`reprocessar_fila` ser chamada quatro vezes por disparo, e **cada chamada relê a
faixa de controle `A2:L` inteira**. Com 2.269 linhas são ~27 mil células por
leitura, quatro vezes a cada cinco minutos. Dá alguns megabytes por disparo —
longe dos 150–250 MB que causaram o OOM de julho, e dentro da cota de leitura.

O conserto seria ler uma vez e distribuir entre as etapas, o que obriga a
reorganizar `reprocessar_fila`. **Não fiz hoje, e a razão é a situação:** isso
acabou de entrar em produção e está drenando 2.269 pendências reais; mexer na
estrutura do laço agora troca uma ineficiência tolerada por risco de defeito no
que está funcionando. Fica anotado para quando a fila estiver vazia — aí o custo
de errar é baixo. Se a aba crescer muito (dezenas de milhares de linhas), isso
sai de "tolerável" e passa a ser o primeiro lugar a olhar.

---

### 08/10/2026 (manhã seguinte) — a segunda contagem do dono achou três coisas, e uma é grave

Ele voltou com a contagem e uma queixa: *"tenho várias baixas que não aconteceram
na planilha, mas acredito que foram depois das mudanças aqui, mas a fila ainda
não rodou. E só preciso que rode as coisas desse mês em diante. O que tá pra
trás, poderia zerar."*

```
linhas_na_aba      2273
por_status         PENDENTE 2213 | CONCLUIDO 57 | STATUS 3
por_etapa          zapi 1943 | omie 182 | pipefy 88
mais antiga        18/06/2026 | mais recente  08/10/2026 09:05:45
```

**Primeiro: a fila RODOU.** Ele achou que não, e os números mostram que sim — 57
concluídas (eram zero) e as baixas do Omie caindo de 238 para 182, 56 fechadas.
A drenagem pelo cron funciona. Vale anotado porque é o tipo de coisa que ele não
tem como ver: a planilha não mostra "o que mudou desde ontem".

**Segundo, e é o furo que ele relatou — 182 pendências de `omie` com ZERO de
`sheets`.** As duas coisas são o mesmo fato, e o fato é grave: a baixa falha no
Omie **antes** de a planilha ser gravada. Então a planilha nunca foi escrita, e
nunca houve pendência de planilha para enfileirar. Pior: a nova tentativa
resolvia o Omie, marcava `CONCLUIDO` e **deixava a planilha desatualizada para
sempre**. Ou seja, a drenagem que eu publiquei ontem estava fechando pendências e
criando exatamente o problema que ele descreveu.

Corrigido: quando o título está pago (inclusive quando já estava, pela
conciliação diária), a nova tentativa **termina o plano** — grava a planilha e
move o cartão. A gravação da planilha **segura** o item na fila se falhar: é o
registro do pagamento. O cartão do Pipefy **não** segura, ganha pendência própria
— registro certo nos dois sistemas não deve ficar preso por um cartão. Repetir é
seguro, e é o que sustenta o desenho: a consulta devolve "já pago" e não lança
nada de novo, e a regravação escreve os mesmos valores nas mesmas células.

**Terceiro: `STATUS 3` no `por_status` era defeito MEU, visível nos dados dele.**
Três linhas com Status = "STATUS" no meio da fila, contadas como pendência. Eu
troquei a conferência do cabeçalho por uma leitura de `A1:O1` (certo, para não
ler a aba inteira), mas deixei o `append_row` do caminho "cabeçalho ausente" —
e `append_row` acrescenta no **fim** da aba, não na linha 1. Bastou a leitura
voltar vazia uma vez, num soluço de rede, para nascer lixo. Agora: `update`
na faixa `A1:O1`, nunca `append_row`; leitura que falha **não** autoriza escrita
nenhuma; e a seleção ignora linha cujo Status seja "STATUS", para o estrago já
feito não voltar a contar.

**Quarto, o pedido dele: zerar o que está para trás.** Entrou
`POST /api/baixabradesco/zerar-fila-antiga` com `antes_de`. Decisões:

- **Nada é apagado.** A linha fica, marcada concluída, com o motivo e a data da
  decisão escritos — quem abrir a planilha em dezembro entende por quê.
- **`antes_de` é obrigatório.** Um "zerar tudo" sem data é fácil de disparar por
  engano, e desfazer linha por linha seria trabalho de horas.
- **Só POST**, pelo mesmo motivo: um endereço que o navegador ou a prévia de um
  aplicativo de mensagem busque sozinho dispararia isso por acidente.
- **Marcação em lote** (blocos de 50 linhas): 1.943 linhas não podem custar 1.943
  escritas de cota.
- A razão de negócio, porque é dele e não minha: ele faz **conciliação bancária
  diária**, então o que ficou para trás já foi resolvido na mão — a pendência é
  de registro, não de dinheiro.

**Verificado:** 47 testes de fila (16 novos), área inteira passando, aplicação
subindo com os 18 blueprints e a rota nova registrada.
**Não verificado:** a conclusão do plano na nova tentativa nunca rodou contra a
planilha de verdade. É o que vai dizer se as "baixas que não aconteceram na
planilha" param de aparecer.

---

### 08/10/2026 — a entrega estava pela metade, e eu só vi relendo o pedido

**Publicado na `main` em `8e8a5ff`:** os três consertos da entrada anterior.

Depois de publicar, reli o que ele tinha pedido — *"só preciso que rode as coisas
desse mês em diante. O que tá pra trás, poderia zerar"* — e vi que eu havia
entregado **a ferramenta, não o resultado**: a rota `zerar-fila-antiga` existia,
mas só aceita POST (de propósito), e ele lê o chat pelo celular. Ou seja, eu tinha
transformado um pedido dele numa tarefa para ele. Isso é estreitar o escopo
calado, e é tão ruim quanto parar no meio da fila.

**O que ficou:** o mutirão pega carona no mesmo cron, em blocos de 500 por
disparo, e **zera antes de drenar** — senão a drenagem gastaria a passada inteira
nas linhas que vão ser dispensadas dois segundos depois.

Decisões que valem registro:

- **A data de corte é FIXA (`01/10/2026`), não "o mês corrente".** Ele autorizou
  zerar o que estava para trás *naquele dia*. Uma regra que andasse com o
  calendário ficaria dispensando pendência nova todo dia primeiro — a forma mais
  silenciosa possível de perder trabalho, e exatamente o tipo de coisa que
  ninguém descobre por meses.
- **É um mutirão que se encerra sozinho.** Depois que as linhas antigas estão
  marcadas, nenhuma casa com o critério e a passada fica de graça. Não é política
  permanente.
- **Dá para desligar pelo ambiente** (`BAIXABRADESCO_ZERAR_ANTES_DE` vazia), sem
  mexer no código e sem esperar publicação.
- **Falha no mutirão não impede a drenagem.** São duas coisas independentes, e a
  drenagem é a que resolve dinheiro.

**Por que eu julguei que isto não precisava de novo "pode":** ele escreveu "o que
tá pra trás, poderia zerar" com todas as letras, nada é apagado (a linha fica com
o motivo e a data da decisão escritos), e o `CLAUDE.md` é explícito em que
pergunta já respondida não se repete. Ficou registrado aqui para ele poder
discordar — e `BAIXABRADESCO_ZERAR_ANTES_DE` vazia desliga na hora, sem
publicação.

**Verificado:** 14 testes de cron (7 novos), área inteira passando, suíte completa
rodada com a única falha sendo `erpbrasil` ausente neste ambiente, aplicação
subindo com os 18 blueprints.
**Não verificado:** o mutirão nunca rodou contra a planilha de verdade. O sinal
de que funcionou é `pendentes_vencidos` caindo em blocos de 500 e
`registro_mais_antigo` saltando para outubro.

---

### 08/10/2026 — publicado o mutirão, e ele passou a saber dizer que acabou

**Publicado na `main` em `7c26b0b`**, com o "pode ligar o serviço automático e
pode publicar ao concluir" do dono — que era exatamente o que havia acabado de
ser ligado.

Logo depois, um detalhe que ia sobrar para ele: o mutirão **não sabia dizer que
havia terminado**. Ele ficaria olhando a contagem sem saber o que esperar, e a
varredura seguiria custando uma leitura da faixa de controle a cada cinco
minutos, de graça, para sempre. Agora, quando não há mais nada anterior ao
corte, a resposta do cron traz `atraso_zerado.concluido` e a frase diz para
esvaziar `BAIXABRADESCO_ZERAR_ANTES_DE`.

Ficou anotado o encadeamento, porque é o tipo de coisa que a próxima sessão
precisa saber para não achar que está tudo certo: **enquanto ninguém esvaziar
aquela variável, a leitura extra continua.** Não quebra nada e está dentro da
cota — mas é a mesma ineficiência das quatro leituras por disparo, agora cinco.
O conserto de verdade é ler a faixa uma vez por disparo e distribuir entre as
etapas, e segue valendo o motivo de não fazer agora: há 2.213 pendências reais
passando por esse laço neste momento.

**Verificado:** 16 testes de cron (2 novos), suíte inteira rodada com a única
falha sendo `erpbrasil` ausente neste ambiente, aplicação subindo com os 18
blueprints.

---

### 08/10/2026 — relendo o módulo de avisos, um defeito no caminho que a produção usa

**Publicado na `main` em `df183ff`:** o mutirão avisando quando acaba.

Depois disso reli o `avisos.py` inteiro — ele manda mensagem para dois celulares
e foi mexido hoje — e achei um defeito que os testes de hoje não pegavam, porque
eu só havia coberto o caminho **com** credencial Z-API.

**O defeito:** quando a credencial Z-API não vem, o aviso sai pelo notificador
comum, que devolve um dicionário **por canal** (`{"whatsapp": {...}, "telegram":
{...}}`), **sem `ok` no topo**. Esse resultado era devolvido cru. Como o relatório
decide tudo por `r.get('ok')`, o aviso entregue pelo Telegram era contado como
**"nenhum canal entregou"**, e o `ok` do aviso inteiro ia para falso.

**Por que isso importa mais do que parece:** as credenciais Z-API chegam *dentro
do pedido do Make*. O serviço automático — o cron que acabou de ganhar a
drenagem e o mutirão — **não tem pedido nenhum**. Então esse é, muito
provavelmente, o caminho que a produção percorre, e o relatório mentiria
justamente onde mais se olha. É plausível que explique parte dos 1.943
`zapi_erro` acumulados.

Corrigido: a resposta do notificador passa a ser traduzida para o mesmo formato
dos outros envios, com `ok` no topo e o braço do WhatsApp separado — e a resposta
original fica guardada inteira, para quem for investigar.

**A lição, e ela é a mesma de hoje mais cedo:** cobri o caminho feliz e deixei o
caminho sem credencial sem teste. Os quatro defeitos que achei hoje por releitura
(a função apagada, o entupimento da fila, a frase que prometia continuação, o
cabeçalho no fim da aba) e este têm a mesma assinatura: **o caminho de exceção não
tinha teste.** Caminho de exceção em código que mexe com dinheiro é onde o defeito
mora, porque é o que ninguém exercita à mão.

**Verificado:** 15 testes de entrega de aviso (4 novos, todos no caminho sem
credencial), suíte inteira rodada com a única falha sendo `erpbrasil` ausente
neste ambiente, aplicação subindo com os 18 blueprints.
**Não verificado:** se o WhatsApp está de fato entregando em produção. O
relatório agora diz a verdade sobre isso — antes não dizia —, mas só a primeira
falha real depois disto vai mostrar.
