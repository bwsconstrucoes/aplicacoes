# O cruzamento de notas fiscais — nota × pedido × título × fundo fixo

> Ditado pelo dono em **07/09/2026**, em uma conversa longa. Este documento é a
> memória dela. Nada aqui está construído ainda: é a especificação, escrita
> antes de qualquer linha de código, porque a decisão de negócio é dele e o
> desenho errado aqui custa meses.
>
> Palavras dele que resumem o porquê: *"esse cravamento dessas informações,
> essa conexão, é o que faz a diferença de um bom ERP, reduzindo a necessidade
> do trabalho humano nessa conferência… que pra fazer uma a uma, dentro do
> volume que nós temos, é muito difícil."*

---

## 1. O problema, do jeito que ele acontece hoje

A BWS opera com **mais de um CNPJ**. Fornecedores emitem nota contra esses
CNPJs o dia inteiro. Hoje:

1. Um sistema de terceiro (**FSist**, no relato dele) monitora os CNPJs e baixa
   as notas emitidas contra eles;
2. Alguém baixa **um relatório** dessa listagem;
3. E aí **na mão**, nota por nota, tenta descobrir: de que pedido é esta nota?
   Qual título financeiro paga ela? Ela bate com alguma coisa?

*"Isso é um trabalho absurdo, manual, às vezes não se localiza as coisas."*

O prejuízo não é só o tempo. É que **o que não se acha, não se confere** — e
nota emitida contra o CNPJ da empresa sem ninguém saber é um problema fiscal,
não um problema de organização.

## 2. O que torna isso difícil de verdade

Não é um-para-um. É aqui que a maioria dos sistemas erra, e foi o próprio dono
quem explicou:

> *"Pode ser que eu emita um pedido pra um fornecedor… ele mandou metade do
> material hoje que ele tinha disponível, mandou a outra metade depois. Ou
> então comprei dez carradas de brita, e o fornecedor emite a nota por carrada.
> Cada caminhão que sai é uma nota emitida… aquele pedido, ele não se fecha
> instantaneamente. São várias notas pra poder fechar ele."*

E a consequência financeira, que é a parte que dói:

> *"Aquela previsão, se eu tiver uma única previsão de dez carradas pra pagar
> as dez, já não é exatamente aquilo — aquilo ali vai se transformar
> provavelmente em dez notas e dez boletos, ou seja, dez contas a pagar."*

**Portanto: UM pedido gera N notas, e N notas geram N títulos.** Qualquer
desenho que assuma "um pedido = uma nota = um título" nasce errado e vai ser
refeito. A previsão de pagamento criada junto com o pedido é uma **previsão**,
e ela se desdobra conforme as notas chegam.

## 3. A tela que ele quer

Ele foi explícito: isso vira **tela nova** — provavelmente mais de uma — e ela
junta dois mundos.

> *"Imagina que você faz uma gerência de suprimento e financeiro numa única
> tela, concorda? Olha o poder que essa tela tem."*

O modelo mental é o que **já funciona na conciliação bancária** do ERP: o
sistema cruza o que consegue, e **expõe para uma pessoa decidir** o que ficou
duvidoso.

> *"Da mesma forma que a gente trabalha com a conciliação bancária, vai estar
> expondo pra alguém que vai fazer essa checagem: olha, esse aqui tem esse de
> um lado, esse do outro, confirma, está batendo? Ou então: essa nota aqui não
> foi localizado nada."*

O que a tela precisa responder, olhando de um lado para o outro **e de volta**:

- As notas emitidas contra cada CNPJ da empresa, todas;
- Para cada nota: cruza com **pedido**? com **título financeiro**? com
  **prestação de fundo fixo**? com nada?
- Para cada pedido: quanto já veio em nota, quanto falta, quantas notas;
- Situação em ambos os sentidos — **a pagar, pago, recebido, comprado**;
- E o alerta que interessa: **nota emitida contra a empresa que não cruza com
  nada.** Pode ser compra legítima que alguém esqueceu de lançar — ou nota
  emitida em nome da empresa **sem autorização**.

### O caso pequeno que ele fez questão de citar

> *"De repente uma notazinha pequena que foi emitida vai aparecer lá na
> prestação do fundo fixo… não foi o comprador, foi alguém lá da administração
> da obra que precisou fazer uma compra e mandou emitir a nota no CNPJ da
> empresa, cem reais que seja."*

Ou seja: **o fundo fixo é uma das pontas do cruzamento**, não um caso à parte.
Uma nota de R$ 100 sem pedido não é necessariamente irregular — ela pode estar
dentro de uma prestação de contas.

## 4. Dedutibilidade — e o risco de contar duas vezes

> *"Essa questão da dedutibilidade é algo importante… o fundo fixo, ele é
> dedutível. Mesmo que não tenha nota. Mas vai acontecer de aparecer uma nota
> fiscal que é de um fundo fixo. Então ele está sendo dedutível de duas formas,
> pela nota ou por ser fundo fixo — mas também não pode entrar duplicado na
> contabilidade."*

Este é o ponto mais perigoso do documento inteiro, porque o erro aqui **não
aparece na tela**: aparece na contabilidade, meses depois.

A regra a construir: uma despesa é dedutível **uma vez**. Se a nota já entrou
pelo fundo fixo, ela não entra de novo por ter sido capturada — e vice-versa.
O cruzamento tem de deixar visível **por qual porta** cada despesa entrou.

⚠️ *A confirmar com o dono antes de implementar: hoje os títulos já são
marcados como dedutíveis ou não (`dedutivel_padrao` na categoria). Falta saber
se a regra do fundo fixo já está certa ou se é isso que precisa nascer aqui.*

## 5. Como as notas chegam ao ERP (o que precisa ser estudado)

Ele pediu explicitamente: *"você tem que estudar aí as APIs de como é que a
gente vai fazer o download dessas informações."*

**O que é preciso saber antes de escolher o caminho**, e ainda NÃO foi
verificado por ninguém aqui:

- O canal oficial para um CNPJ puxar as notas emitidas **contra ele** é o
  serviço de distribuição de documentos da SEFAZ, autenticado por
  **certificado digital A1** e percorrido por um número de sequência. É o que
  produtos como o FSist automatizam por dentro.
- O que se baixa é o **XML** da nota. **O PDF se gera a partir do XML**, a
  qualquer momento — como o próprio dono supôs. Não é preciso guardar PDF.
- Existe a **manifestação do destinatário** (a empresa dizer "ciente" /
  "confirmo" / "desconheço"). Isso tem consequência fiscal e prazo, e é
  justamente onde "nota emitida sem autorização" deixa de ser um alerta na
  tela e vira um ato. **Decidir se o ERP vai manifestar ou só avisar.**
- Alternativa a estudar em paralelo: **continuar com o FSist** e só importar o
  relatório/XML que ele já baixa. Menos controle, muito menos trabalho, e
  resolve o cruzamento — que é a dor real. Talvez seja o primeiro passo certo.

**Recomendação a levar ao dono:** começar pelo CRUZAMENTO usando o que o FSist
já entrega, e só depois trocar a fonte por acesso direto à SEFAZ. O valor está
no cruzamento, não no download; e trocar a fonte depois não refaz o
cruzamento.

## 6. Certificado digital — e o cuidado que ele mesmo pediu

> *"Dentro do cadastro das empresas… a gente precisa trabalhar também com
> certificado digital, né? E aí entra a parte da segurança: você vai ter que
> bolar uma forma de não ter risco desse certificado vazar."*

O caminho já existe no ERP e é o mesmo da senha de e-mail (migração 038): o
arquivo e a senha ficam **cifrados** com a chave `ERP_CHAVE_SEGREDOS`, e **sem
a chave o sistema recusa gravar** em vez de guardar aberto.

Diferenças que o certificado traz, e que precisam de decisão:

- O certificado **assina em nome da empresa**. Quem puder baixá-lo de volta
  pode agir como a empresa. Regra a adotar: **entra, nunca sai** — o ERP usa,
  ninguém baixa de volta pela tela.
- Ele **vence**, tipicamente em um ano. Vencido, a captura de notas para
  calada. Daí o alerta que ele pediu.

## 7. A agenda de alertas

> *"Um acompanhamento, até de um alerta… pra gente saber se o certificado está
> vencendo ou não. Dentro da tela do sistema a gente vai precisar dessa
> agenda… receber alertas de situações importantes, em qualquer área, ou da
> empresa como um todo."*

Isso **já está no roteiro do ERP** como "Agenda do ERP: calendário de
obrigações", ainda não construído. O certificado vencendo é mais um assunto
dela — junto com reajuste de obra, obrigação fiscal e, agora, a conferência
mensal de locação e o cruzamento de notas em aberto.

Fica registrado que a agenda deixou de ser "seria bom ter": **ela é o lugar
onde os alertas de todas as áreas se encontram**, e três coisas já dependem
dela.

## 8-B. O QUE JÁ FOI CONSTRUÍDO — 09/09/2026

Os passos 2, 3, 4 e 5 da ordem abaixo estão **feitos** (migração 044, tela
Financeiro › Notas fiscais). Detalhe e provas em `HISTORICO.md`, seção "A tela
do cruzamento de notas fiscais". Em resumo:

- as notas entram por **importação de XML** (solto ou .zip), como recomendado;
- o cruzamento com **pedido, título e fundo fixo** funciona, com um pedido
  recebendo várias notas e o saldo diminuindo a cada uma;
- o casamento automático só acontece pela **chave de acesso**; indício vira
  proposta para uma pessoa confirmar;
- a **trava contra contar a mesma despesa duas vezes** está no lugar, nos dois
  sentidos, com caso de teste em banco de verdade;
- **ignorar uma nota exige motivo escrito**, e o banco recusa sem ele.

Continua **sem construir**: o passo 1 (certificado por empresa), o passo 6 (a
agenda) e a captura direta na SEFAZ — os três dependem de decisão dele (§9).

Das quatro perguntas do §9, uma foi **respondida** pelo dono em 07/09/2026:
*"fundo fixo é dedutível, ponto final"*. Uma foi **decidida na prática** aqui:
quem confere são os dois (financeiro por cargo, comprador por marcação), na
mesma tela. Duas continuam abertas: manifestação do destinatário, e trocar o
FSist pela SEFAZ.

## 8. Ordem sugerida (a combinar com o dono)

1. **Cadastro do certificado por empresa**, cifrado, com data de validade —
   é pré-requisito de qualquer captura, e reusa o que a migração 038 já fez.
2. **Receber as notas** — primeiro pela importação do que o FSist já baixa,
   que é o caminho curto para o valor.
3. **A tela do cruzamento**, no espírito da conciliação bancária: casado /
   duvidoso / sem par, com a pessoa confirmando o duvidoso.
4. **O casamento nota × pedido**, aceitando desde o primeiro dia que **um
   pedido tem N notas** e que a previsão se desdobra em N títulos.
5. **Fundo fixo e dedutibilidade**, com a trava contra contar duas vezes.
6. **A agenda**, recolhendo os alertas de todas as áreas.

## 9. O que ainda precisa da palavra dele

- A regra de dedutibilidade do fundo fixo (§4) — é o item de maior risco.
- Manifestação do destinatário: o ERP manifesta, ou só avisa? (§5)
- Começar pelo FSist ou ir direto à SEFAZ? (§5)
- Quem confere o cruzamento duvidoso — o financeiro, o comprador, ou os dois
  em telas diferentes?
