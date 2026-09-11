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
3. **O leitor do Sicredi nunca é chamado.** Fica como está, por decisão do dono:
   **a empresa não usa mais o Sicredi**. O arquivo continua no repositório, sem
   ligação com o fluxo. Se voltar a usar, é ligar o desvio e cobrir com teste
   antes.

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
- **Destinatário único: o dono.** Ele confirmou que o número passado é para
  receber este aviso e só ele deve receber. Não confundir com o WhatsApp que vai
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

---

## Estado no fim de 11/09/2026

Tudo publicado. A área fechou o dia com:

- os **dois caminhos da Somapay** funcionando (depósito direto e transferência
  Bradesco → Somapay), com a conta de destino vindo da BaseBancos pela chave PIX;
- comprovante recusado pelo banco barrado antes de virar baixa;
- a trava contra baixar duas vezes ligada de fato;
- **aviso por WhatsApp, só para o dono, do que não foi baixado.**

**O que conferir nos próximos lotes reais**, nesta ordem:

1. O aviso chega no WhatsApp. Se não chegar: toggle `NOTIFICAR_WHATSAPP` ligado,
   credencial Z-API chegando no pedido do Make, número certo.
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
- O leitor do Sicredi segue desligado (a empresa não usa mais).
