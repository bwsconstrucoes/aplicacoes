# Emissão de NFS-e — decisões, incidentes e o que falta

Este arquivo existe para uma sessão nova (ou uma pessoa nova) pegar o trabalho
sem repetir o que já foi discutido e sem repetir o que já deu errado.

O `README.md` ao lado explica **como a emissão é**. Este aqui explica **por que
ela é assim**, e o que aconteceu no caminho. Leia os dois antes de mexer.

---

## Como esta memória nasceu — 21/09/2026

Esta área entrou na tabela do `CLAUDE.md` **sem memória nenhuma**: era a única
das cinco sem `README.md` e sem `HISTORICO.md`, com ~7.000 linhas em 48
arquivos, e a dívida estava anotada no `CONTEXTO.md` há meses (*"ainda não
documentado aqui"*, *"documentar na próxima vez que mexer"*).

Os dois arquivos foram escritos a partir de **duas fontes**, e vale saber qual é
qual antes de confiar em cada linha:

- **O código**, lido de ponta a ponta nesta sessão. Tudo o que está no
  `README.md` foi conferido no código — se divergir, o código é que manda, e
  o arquivo é que está velho.
- **O relato do dono**, colado por ele de um chat antigo que se perdeu. É a
  única fonte do que aconteceu com notas de verdade: os incidentes, os valores,
  o que já foi corrigido no mundo e o que não foi. Onde o relato não trouxe
  data, este arquivo **não inventou uma** — diz "sem data registrada".

Onde os dois se contradisseram, está escrito qual venceu e por quê.

---

## Onde o trabalho está

O sistema **emite em produção há meses** e faz o ciclo inteiro sozinho: lê a
medição no Pipefy, calcula as retenções pela tributação da obra, assina com o
certificado A1, envia à prefeitura de Eusébio, e depois grava a planilha, ajusta
o título no Omie, preenche o card, arquiva os PDFs no Drive e avisa no WhatsApp.
A nota nacional fecha sozinha em minutos, pela SEFIN.

Não é um projeto em construção: é uma ferramenta em uso. O que existe de
trabalho pendente é **conserto e faxina**, não funcionalidade nova.

### O que está pendente AGORA

**O único item confirmado nesta sessão:** conferir na tela que o card
1447316614 (obra AREFORTAL09) carrega, agora que a busca olha os dois códigos
da obra. É o defeito que o dono trouxe, e o conserto está feito e testado —
falta o olho dele na tela, com o card de verdade.

**Todo o resto abaixo é pista, não tarefa.** Veio de um relato que o dono colou
de um chat antigo, e **nada disso foi conferido contra o mundo real** — nenhuma
planilha foi aberta, nenhuma nota foi consultada. Mais do que isso: perguntado
em 21/09/2026, **ele não recorda os itens 1 e 2** (ver a seção logo abaixo).

1. **O passivo de notas com ISS a maior na prefeitura.** É o item mais caro, e
   está explicado no incidente do `ValorDeducoes`, mais abaixo. Falta
   **levantar quais notas** foram emitidas com dedução de ISS durante o período
   do defeito e **substituí-las**. Notas `100/0` (sem dedução) não foram
   afetadas.
2. **Concluir a substituição da nota 3083** — pelo portal, seguida do
   `/emissao/recuperar`.
3. **Rodar o `/emissao/regerar`** nas notas que precisam do layout novo dos
   PDFs.
4. **Terminar o registro dos ~290 PDFs antigos** baixados do portal: ler o
   número de dentro do PDF, subir ao Drive com o nome no padrão e preencher a
   "Notas BWS Links". Junto disso, **deduplicar as ~20 mil linhas** dessa aba.
   Esses PDFs são anteriores à aplicação e **não têm DPS**, então a busca
   automática pela SEFIN nunca vai achá-los — é trabalho de script, uma vez.
5. **Decidir se a Exigibilidade do ISS vira campo** ("Não incidência" etc.),
   hoje fixa em "Exigível" no código. E, se virar, decidir se é **por obra** ou
   **por nota**.
6. **Uma das buscas de PDF deve varrer subpastas do Drive** — o dono pediu, mas
   ficou sem dizer qual das três. Pergunta em aberto.
7. **Vigiar o embaralhamento da "Notas BWS".** Se voltar mesmo com o
   `LockService`, o caminho é tirar a ordenação de cima do `onChange` e pôr num
   gatilho por tempo.

### O que o dono respondeu — 21/09/2026

Esta lista foi levada a ele no mesmo dia em que foi escrita. A resposta, na
íntegra:

> *"no mais tudo funciona normal. nao recordo de problema de iss, nem de
> substituicao."*

**Como ler isso, sem esticar nem encolher:**

- **O defeito do `ValorDeducoes` existiu** — não é lembrança, está provado no
  código, no comentário que explica por que o campo passou a ser enviado. E
  está corrigido desde 11/09/2026.
- **O passivo de notas afetadas NÃO está confirmado.** Ele não recorda o
  problema, e ninguém levantou a lista. Pode ser que o número de notas com
  dedução de ISS emitidas antes de 11/09 seja pequeno, ou zero; pode ser que
  não seja. **Ninguém olhou a planilha ainda.**
- **As substituições (3069→3081, 3070→3082, 3083) também não estão
  confirmadas.** Vieram do relato colado de um chat antigo, e ele não recorda.

**O que isso quer dizer na prática:** o que está na lista de pendências abaixo
veio do relato antigo, não de uma conferência. Antes de gastar tempo com
qualquer um daqueles itens, **abrir a planilha e ver se o problema existe** —
e não presumir que existe porque está escrito aqui. Um levantamento de meia
hora na "Notas BWS" decide se o item 1 é trabalho de dias ou não é trabalho
nenhum.

**E a notícia boa, que também é informação:** o sistema está rodando normal. O
único defeito que ele trouxe nesta sessão foi o card que não abria, e esse foi
corrigido.

### As perguntas que continuam em aberto

Estas ele não respondeu, e continuam valendo:

- **Qual das três buscas de PDF** deve varrer subpastas do Drive?
- **Existe um agendamento externo** chamando `/emissao/nacional` de tempos em
  tempos? O código fala num Cron "de 10 em 10 minutos" como rede de segurança,
  mas **não há nada no repositório que o configure** — se existe, é no
  cron-job.org, fora daqui. Se não existe, a rede de segurança não está armada,
  e uma nota que a busca automática não pegar nos primeiros cinco minutos fica
  esperando alguém clicar.

## Decisões tomadas, e por quê

### `SD` quer dizer NÃO RETER — e `100/0` é que retém sobre tudo

A categoria da obra tem quatro blocos
(`ONERADA - Ded.INSS - Ded.ISS - Impostos`). O bloco de dedução aceita `NN/MM`
(serviço/material) ou `SD`.

**`SD` significa base zero: aquele imposto não é retido.** Vale para o INSS e
para o ISS. Para reter sobre o valor inteiro, escreve-se **`100/0`**.

Isto corrigiu um entendimento anterior em que `SD` valia 100% — ou seja, o
sistema retinha onde não devia. Está escrito no próprio código
(`tributacao.py`), em comentário, para não se perder de novo.

### Imposto sem retenção não aparece na nota

Junto da correção acima: imposto com valor zero saiu da discriminação e da
tabela de apuração. Uma linha "ISS: 0,00" no corpo da nota faz quem lê procurar
erro onde não há.

### Categoria fora do padrão BARRA a emissão

O motor fiscal recusa qualquer coisa que não sejam os quatro blocos exatos, com
a crítica na tela. Chutar o significado de uma categoria estranha é errar
imposto numa nota que não se apaga — é melhor não emitir.

### O ISS é sempre retido, e a exigibilidade é sempre "Exigível"

As duas coisas estão fixas no código. Não foi decisão contra alternativa: foi o
único caso que apareceu até hoje. Tornar a exigibilidade configurável é uma das
pendências, e a pergunta que vem junto é **por obra ou por nota** — a resposta
muda onde o campo mora.

### Substituição: o caminho é o bloco dentro do XML (e ele tem limite)

Substituir uma nota na prefeitura foi implementado pelo bloco `RpsSubstituido`
dentro do XML ABRASF — confirmado contra o XSD do município.

**Só funciona para nota emitida por este sistema**, que tem RPS "de verdade"
(número igual ao da nota, série "1", tipo 1). Nota emitida **manualmente no
portal** tem RPS com série vazia e tipo 0, e aí a prefeitura se contradiz: ela
*guarda* o RPS assim, mas o XSD de *envio* recusa esses valores (a série exige
pelo menos um caractere, e o tipo só aceita 1, 2 ou 3).

O resultado prático, que custou tentativa: mandando série 1 dá **E76**; mandando
série vazia dá **erro de schema**. Não há terceira saída.

Chegou-se a criar uma opção "0" na tela para tentar mandar o padrão manual. **Foi
revertida** — gerava XML inválido. Hoje a tela orienta a ir pelo portal nesses
casos, e o `/recuperar` faz o resto.

> **Sobra de código a limpar:** o `web.py` ainda calcula duas variáveis
> (`_ss_0`/`_ss_1`) e carrega um campo escondido `serie_sub` que vieram daquela
> opção revertida. Não têm mais efeito nenhum — o seletor que as usava não
> existe. É lixo inofensivo, e fica anotado para quem for mexer ali não achar
> que está desativando alguma coisa.

**As substituições 3069→3081, 3070→3082 e a 3083 foram resolvidas assim:** botão
"Substituir" do portal (que usa o identificador interno dele) e, em seguida, o
`/recuperar` para os efeitos internos — Pipefy, Omie, Drive, WhatsApp, planilha.

### Substituir só por valor igual ou maior — e a tela barra antes de tentar

Regra do município. A tela confere e recusa antes de enviar, explicando que
valor menor (ou troca de competência) é caso de **cancelamento**, que é outro
assunto. Barrar antes vale mais que uma mensagem de erro da prefeitura depois.

### As retenções do Omie são da medição INTEGRAL, e só na primeira nota

Quando uma medição é faturada em várias notas parciais, só a **primeira** grava
as retenções no título — e grava as da medição inteira, não as da parcela. Da
segunda em diante, o sistema só **acrescenta o número** ao documento fiscal
(`3001/3072`). Duas notas gravando retenção no mesmo título dobrariam o valor.

### O nacional vem pela SEFIN, não pelo portal

O login do portal da prefeitura expira em cerca de uma hora, o que torna
impossível qualquer automação que dependa dele. A busca pela SEFIN usa **só o
certificado**, e acha a nota pelo ID da DPS, que é derivado do número. Por isso
a nota nacional fecha em minutos, sozinha, logo depois da emissão.

O ADN por NSU ficou como **rede de segurança** — é mais lento e entra na fila de
distribuição.

### Cada passo do pós-emissão falha sozinho

No `concluir.py`, cada um dos dez passos tem o próprio tratamento de erro e o
próprio log. Isso é deliberado: depois que a prefeitura devolve o número, a nota
**existe**, e o pior desfecho possível seria uma falha no Drive abortar a
gravação na planilha. Melhor dez passos independentes, com o log na tela,
e ferramentas para refazer o que faltou.

### As credenciais vêm da planilha, mas a variável de ambiente ganha

É o mesmo padrão do `analisesps` (que, aliás, copiou daqui). A aba `Credenciais`
existe para os scripts de linha de comando rodarem fora do Render; em produção,
o que vale é o que está no Render.

---

## Incidentes

### A obra que existia e o sistema dizia não existir — 21/09/2026

**O que aparecia na tela:** `Erro ao carregar o card 1447316614: KeyError: "Obra
'AREFORTAL09' não encontrada na C. Diários."` — e a obra estava lá.

**O que era:** a C. Diários identifica a mesma obra por **dois** códigos, o
primário (coluna "Código Primário") e o secundário (primeira coluna, A). O
carregador só indexava pelo primário. Card que trouxesse o secundário não achava
a obra, e a emissão parava antes de começar.

**Conserto:** o índice passou a ter os dois códigos, e a busca tenta o primário
primeiro; só cai no secundário se não achar. Só levanta erro depois de tentar os
dois — e a mensagem agora diz que tentou.

**A decisão que precisou ser tomada junto:** o que fazer quando o código
secundário de uma linha for igual ao primário de outra. **O primário ganha
sempre.** Um código secundário encobrindo o primário de outra obra emitiria a
nota com a tributação errada — que é exatamente o erro que não se desfaz. A
busca também passou a ignorar espaço e maiúscula, porque o código vem digitado
no card.

**Este incidente trouxe os primeiros testes automatizados da área**
(`tests/test_emissaonf_codigo_obra.py`, seis casos): os dois códigos achando a
obra, o primário não sendo encoberto, a linha que só tem secundário, e o erro
continuando a acontecer para código que não existe mesmo. É pouco perto do que
falta, mas é o começo de uma rede que aqui não existia.

**Não foi verificado com dado real:** a correção roda sobre linhas de planilha
montadas no teste. Ninguém abriu a C. Diários nesta sessão — vale abrir o card
1447316614 na tela e conferir que a obra carrega, com a tributação certa, antes
de emitir.

### O `ValorDeducoes` que nunca foi enviado — o mais grave

**O que acontecia:** o sistema calculava a dedução de material corretamente,
mostrava o valor certo no espelho e no PDF arquivado no Drive — mas **enviava
`ValorDeducoes` como 0,00 no XML**. A prefeitura calcula a base do ISS como
(valor dos serviços − deduções); recebendo zero, ela aplicava a alíquota sobre o
valor **cheio**.

**O tamanho do erro:** numa nota `80/20`, o ISS saiu **R$ 1.409,37** na
prefeitura contra **R$ 1.127,50** no nosso cálculo.

**O que isso significa:** toda nota emitida **com dedução de ISS** (`60/40`,
`80/20` e semelhantes) durante o período do defeito está com **ISS a maior na
prefeitura**. O PDF guardado no Drive mostrava o valor certo — que nunca chegou
lá. Notas `100/0` não foram afetadas, porque nelas a dedução é zero mesmo.

**Correção:** o XML passou a levar `ValorDeducoes = valor total − base do ISS`.

**⚠️ Correção ao relato do dono, conferida no código:** o relato dizia que o
conserto "precisa subir antes de qualquer emissão nova". **Ele já está no ar.**
A linha entrou na `main` em **11/09/2026** (commit `f2d93c8`) e continua lá na
`main` de hoje — e juntar na `main` publica no Render na hora. Conferido no
`montar_emissao.py` da `main`, não só no ramo. Ou seja: **emitir agora está
seguro quanto a isso.** O que continua pendente é o **passivo**: as notas
emitidas antes daquela data.

> Não deu para datar o **início** do defeito: o histórico de Git disponível
> nesta sessão é raso (começa em 04/09/2026) e esta pasta aparece nele uma vez
> só. Para levantar o passivo, o caminho confiável é a planilha — filtrar por
> tributação com dedução de ISS e emitir até 11/09/2026 —, não o Git.

### O Omie recusava a segunda escrita seguida

Na substituição, duas chamadas de alteração seguidas no mesmo título eram
bloqueadas ("já sendo processada", "consumo redundante, aguarde 55s"). O Omie
trava o registro por alguns segundos depois de cada escrita.

**Dois consertos, nesta ordem de importância:** o principal foi juntar remoção do
número antigo e inclusão do novo numa **única chamada**; o secundário foi
esperar e tentar de novo quando a mensagem de trava aparece.

### A discriminação vinha "corrida" nos PDFs

O texto do sistema nacional colapsa as quebras de linha, e a discriminação
chegava sem parágrafo na municipal regenerada e na DANFSe. Foi criado um
tratamento que **reinsere as quebras** nos pontos conhecidos do texto, usado nos
dois geradores.

Junto disso, a DANFSe nacional passou a ter **altura variável** no campo da
descrição do serviço — antes era fixa, e o texto longo escrevia por cima da
seção seguinte.

### O sufixo "R" da medição de reajuste não saía

Quando o "Tipo de Documento" do card é de reajuste, o número da medição deve ir
para a planilha com um "R" no fim (`7R`). Não ia, porque o campo simplesmente
**não estava sendo lido** do card. Passou a ser lido — e, de quebra, a busca de
campos no Pipefy ficou tolerante a acento, maiúscula e espaço, que é de onde
esse tipo de erro costuma vir.

### Nota com período invertido

A emissão passou a ser **barrada** quando o término da medição é anterior ao
início.

### As linhas embaralhadas da "Notas BWS"

**O que se temeu:** que a aplicação estivesse gravando fora de ordem ou por cima
de linha existente.

**O que era:** a aplicação só **acrescenta ao fim**, nunca ordena. A mistura
vinha do **Apps Script da própria planilha** — um gatilho `onChange` rodando
concorrente e reentrante, com dois `sort()` se atropelando.

**Conserto:** um `LockService` para serializar o gatilho. **A integridade das
notas nunca foi afetada** — prefeitura, Drive e Omie estavam certos; o que
embaralhava era a ordem na tela.

> Esse conserto vive **na planilha, não neste repositório**. Quem procurar o
> código dele aqui não vai achar. Se o problema voltar, o caminho combinado é
> tirar a ordenação do `onChange` e pôr num gatilho por tempo.

### O token da prefeitura ficou no histórico do Git

Registrado no `CONTEXTO.md` §9 e repetido aqui porque é precedente desta área:
um token da prefeitura foi commitado dentro do `consultar_status.py` (commit
`fa985ab`). O código passou a lê-lo de `EL_NFSE_TOKEN`, **mas o valor antigo
continua no histórico do repositório** até ser trocado na origem.

**A regra que ficou:** o certificado e a senha **nunca entram no chat**, e
nenhum segredo entra no código. Diagnóstico se faz pela tela `/emissao/diag`,
que diz o que está errado sem mostrar o que é.

---

## Ferramentas que nasceram desses incidentes

Não são funcionalidade: são conserto. Vale saber que existem antes de escrever
script novo.

- **`/emissao/recuperar`** — refaz o pós-emissão de uma nota já emitida, colando
  o XML. Sem "completo", refaz só a entrega (Drive, links, Descrição). Com
  "completo", refaz tudo — e tem trava anti-duplicação. É o que fecha o ciclo
  de uma substituição feita pelo portal.
- **`/emissao/regerar`** — regrava os PDFs de notas já concluídas com o layout
  atual, subindo com o mesmo nome: mesmo link, nada duplicado na planilha nem no
  card.
- **`/emissao/nacional_chave` e `/emissao/nacional_xml`** — fecham a parte
  nacional de uma nota pela chave ou pelo XML, quando ela ainda não apareceu na
  distribuição.
- **Os ~290 PDFs antigos** foram baixados do portal por script (consulta →
  identificador interno → exportação do relatório), salvos como `NF {número}.pdf`.
  Estão à espera do registro na planilha — item 4 das pendências.

---

## Regras que não se discutem

1. **Publicar na `main` espera o "pode" do dono.** Juntar publica no Render na
   hora.
2. **O certificado e a senha nunca entram no chat.** Nem o arquivo, nem o
   base64, nem a senha. Diagnóstico é pela tela.
3. **Nota emitida não se apaga.** Antes de mexer em qualquer coisa do caminho de
   emissão, lembrar que o teste é uma nota fiscal de verdade. Não existe
   ambiente de homologação em uso nesta área.
4. **Mudança no caminho da emissão exige olhar o espelho antes de clicar.** Não
   há teste automatizado nenhum aqui para segurar erro.
5. **Antes de publicar, perguntar se há carga do painel ou sincronização do
   Análise de SPs em andamento** — publicar reinicia o serviço e mata trabalho
   longo. Vale para todas as áreas, e já aconteceu.

---

## O que ficou de fora, e por quê

- **Cancelamento fiscal na prefeitura.** O módulo de substituição diz, em letras
  grandes, que **não cancela** a nota no município. Cancelar continua sendo
  passo manual no portal.
- **Município que não seja Eusébio/CE.** O código do município, o endereço do
  webservice, o código da DPS e o brasão estão fixos no código. Emitir para
  outra prefeitura é outro trabalho, não um parâmetro.
- **Teste automatizado.** Não há nenhum, e escrever um que exercite o caminho de
  emissão esbarra em não existir ambiente de homologação em uso.
- **Login de verdade.** A porta é o token na URL. E, diferente do resto do
  repositório, se o token não estiver configurado a tela **libera** em vez de
  recusar. Fica registrado como fraqueza conhecida, não como decisão defendida.
