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

**Publicado em 04/09/2026** (junção `16039ac`, com o dono confirmando que não
havia carga do painel nem sincronização do Análise de SPs rodando): as duas
correções — comprovante recusado pelo banco e trava de duplicidade — mais o
`README.md`, este arquivo, a linha da área no `CLAUDE.md` e o registro no
`CONTEXTO.md`. **Sem migração de banco**: não foi preciso apertar "Aplicar
atualizações do banco".

Falta a segunda metade da rede de proteção: **testes dos leitores de comprovante
e do casador de pagamentos** cobrindo Pix, boleto, transferência, FGTS e
BeeVale. Hoje existem 23 testes, todos em volta das duas travas corrigidas —
nenhum sobre a leitura dos campos de um comprovante bom nem sobre a escolha da
SP certa.

Para escrever esses casos são necessários **comprovantes de exemplo de cada
tipo**, que só o dono tem. Antes de qualquer um entrar no Git, número de conta,
CNPJ, CPF, nome e código de barras viram fictícios, mantendo o formato do texto —
foi assim com o único exemplo que já está guardado.

**Conferir na primeira baixa real depois da publicação:** no retorno do Make, os
campos `recusados_nao_efetivados` e `duplicados_ja_baixados`; e que nenhum
comprovante legítimo está sendo barrado por engano.

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
