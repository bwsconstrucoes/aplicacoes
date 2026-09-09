# As três conciliações que faltam — comprovante, e-mail e nota

> Ditado pelo dono em **07/09/2026**. Nada aqui está construído. É a
> especificação, escrita antes do código, porque o desenho errado aqui custa
> meses e a decisão de negócio é dele.
>
> Palavras dele: *"são as coisas que realmente fazem muita diferença no dia a
> dia"*. E sobre o e-mail: *"isso é vital também"*.

---

## O fio que liga as três

O ERP hoje sabe o que a empresa **deve** e o que ela **comprou**. O que ele não
sabe é o que chega **de fora**: o comprovante de um pagamento feito, a nota que
o fornecedor emitiu, o boleto que veio no e-mail. Cada uma dessas três entradas
tem hoje um humano no meio, conferindo uma a uma — e, nas palavras do dono, no
volume que a BWS tem *"é muito difícil e muito complicado de ser feito a
contento"*.

As três seguem **o mesmo desenho**, e é isso que as torna um trabalho só:

1. algo chega (comprovante, e-mail, nota);
2. o sistema lê e tenta casar com o que já existe (título, pedido, contrato,
   fundo fixo);
3. **o que casa com certeza, casa**; o que não casa vai para uma tela onde uma
   pessoa confirma — igual à conciliação bancária que já funciona;
4. o que não casa com nada **vira alerta**, não silêncio.

O passo 3 não é fraqueza do sistema: é o desenho. *"Aquela coisa, e o que não
bater cem por cento, aí da mesma forma que a gente trabalha com a conciliação
bancária, vai estar expondo pra alguém que vai fazer essa checagem."*

---

## 1. Baixa pelo comprovante de pagamento

### Como funciona hoje, no mundo antigo

Já existe uma aplicação que faz isso: **`app/apps/baixabradesco/`**. Ela é
madura e vale conhecer antes de escrever qualquer linha:

- lê o PDF do comprovante (tem parser próprio para **Bradesco**, **Sicredi**,
  **BeeVale** e **SomaPay**);
- procura o número do registro escrito no comprovante e, quando não acha, casa
  por **código de barras**, por **valor + conta + tipo**, e por regras
  específicas (FGTS por valor, por exemplo);
- quando casa, dá a baixa **no Omie**, marca a planilha SPsBD, move o card no
  Pipefy e guarda o comprovante no Dropbox.

⚠️ **Ela não fala com o ERP.** Ela pertence ao mundo antigo — Pipefy, Omie e
planilha. O que se aproveita dela é o que é caro e está testado: **os parsers de
PDF e a lógica de casamento**. O destino muda.

O dono confirma que essa parte *"não está cem por cento configurada"*.

### A trava contra baixa em duplicidade — o que existe hoje, e por que não basta

O dono perguntou em 07/09/2026: *"acho que eu tentei colocar alguma trava na
aplicação, não sei se funciona, mas o sistema realmente não pode baixar duas
vezes, precisa barrar"*. Foi lido o código. **A trava existe. Ela pega o caso
óbvio e falha nos casos que acontecem de verdade** — e falha do pior jeito
possível: liberando.

**Como ela funciona hoje.** Cada página do PDF vira uma "impressão digital":
um resumo do NOME DO ARQUIVO + o conteúdo dele + o número da página. A lista
das impressões já processadas mora numa aba de planilha
(`LogBaixaBradesco`), é lida uma vez por lote e conferida em memória.

**Os cinco buracos, em ordem de gravidade:**

1. **O nome do arquivo entra na conta.** O mesmo comprovante chegando como
   `comprovante.pdf` e depois como `comprovante (1).pdf` tem impressões
   diferentes — e passa. Renomear acontece o tempo todo: o próprio navegador
   faz, o Make faz, e reencaminhar do banco gera outro nome.
2. **Falha de leitura vira "não tem nada processado".** A leitura da planilha
   está dentro de um `except` que devolve lista VAZIA. Google fora do ar, cota
   estourada, lentidão — e o lote inteiro parece novo. **A trava falha
   liberando**, que é o contrário do que uma trava deve fazer.
3. **Gravar a impressão também engole erro.** Se a escrita na planilha falhar,
   a baixa já aconteceu e a impressão nunca foi registrada. O mesmo comprovante
   passa de novo na próxima.
4. **Não é atômica.** Dois lotes ao mesmo tempo leem a lista antes de qualquer
   um escrever. Os dois acham que é novo. Os dois baixam.
5. **Ela conhece o ARQUIVO, não o PAGAMENTO.** O banco gerando um PDF novo do
   mesmo pagamento produz bytes diferentes — e passa.

**Como tem de ser no ERP** (e é isto que será construído):

- **A trava mora no BANCO, com restrição única** — não numa lista carregada em
  memória. O banco recusa a segunda; o código não tem como esquecer. É a mesma
  decisão do agente de cobrança (migração 040) e da conferência de locação.
- **Dois níveis, porque um só não pega tudo:** o ARQUIVO, pelo conteúdo e **sem
  o nome**; e o PAGAMENTO — título, valor, data e número do documento no banco.
  O segundo é o que pega o PDF regerado e o arquivo renomeado.
- **Falha FECHANDO.** Não conseguiu conferir? Não dá baixa. Baixa que não
  aconteceu é aborrecimento; baixa em dobro é dinheiro saindo duas vezes.
- **Repetido não é erro silencioso**: a tela diz *"este comprovante já deu
  baixa no título tal, em tal dia"*, com o link para o título.

⚠️ Nada disso conserta o `baixabradesco`, que continua rodando com a trava
atual contra o Omie. Consertar lá é outra decisão — e mexer no que está em
produção não pode ser efeito colateral desta obra.

### Como ele quer que passe a funcionar

Duas portas de entrada, e as duas importam:

**DECIDIDO em 07/09/2026: o Make continua por enquanto.** Ele já recebe o
e-mail, separa os anexos e manda para o servidor — é só o carteiro; quem lê o
PDF e dá a baixa é a aplicação. Trocar o destino no cenário do Make é um ajuste
de dois minutos, e devolve o caminho do e-mail no mesmo dia em que a tela
existir. O endereço próprio do ERP (item 2 deste documento) substitui o Make
depois, sem refazer nada — porque o miolo é o mesmo nas três portas.

**a) Por e-mail.** *"Quando eu estou no Bradesco, eu efetivo um pagamento, eu
posso selecionar aquelas contas que eu paguei e encaminhar os comprovantes pro
e-mail."* É o caminho que já existe na mão dele, dentro do banco, sem passo
extra. Não dá para tirar.

**b) Pela tela do ERP.** *"Alguma tela lá, algum local onde eu fosse anexar
comprovantes pra baixa."* Assim ele não depende de pasta no Drive nem de
encaminhar nada.

Hoje há um terceiro caminho — salvar numa pasta do Google Drive, e um script
manda o e-mail. Esse **deixa de ser necessário** quando as duas portas acima
existirem.

### O que muda no destino

A baixa passa a ser **no título do ERP**, não no Omie. Isso significa reusar o
que já existe em `core/pagamentos/`, e o comprovante fica anexado ao título —
não no Dropbox.

---

## 2. Monitorar as caixas de e-mail

### O problema, com o exemplo dele

*"Nós recebemos muitos e-mails… na parte de locação é muito comum atrasar,
porque não tem ninguém responsável efetivamente por essa leitura de e-mails. É
muito e-mail que é recebido, e não dá tempo de fazer a leitura."*

Dentro deles chegam **boletos, notas de débito, notas fiscais e cobranças**. E o
exemplo que define o trabalho: *"imagina que tem um determinado contrato de
locação, aí chega uma nota de débito no e-mail, tudo certinho. Ou seja, a gente
tem que tentar fazer esse vínculo."*

**São mais de uma caixa:** o e-mail de **compras** e o de **financeiro** — *"os
dois às vezes recebem"*.

### O que o sistema tem de fazer

1. **Ler as caixas** e separar o que é documento financeiro do que é conversa.
2. **Entender o anexo** — boleto, nota de débito, nota fiscal — e tirar dele o
   que interessa: emissor, valor, vencimento, número, linha digitável.
3. **Tentar o vínculo** com o que já existe no ERP: contrato de locação, pedido
   de compra, título, fornecedor.
4. **Marcar o e-mail como tratado, na própria caixa.** Pedido explícito dele:
   *"tem que ver uma forma de classificar eles, pra dizer que já foi analisado
   pela IA… de repente reorganizando em pasta, alguma coisa assim, pra gente
   saber que aquele e-mail já foi tratado."* Sem isso, ninguém sabe o que
   sobrou para ler.
5. **Pedir confirmação humana antes de valer.** *"Acho que tem que ter uma
   espécie de validação humana, pra confirmar que foi vinculado e já foi
   lançado, associado, tudo."* O sistema propõe; a pessoa confirma.

### O que precisa ser decidido antes de construir

- **Como o ERP entra nas caixas.** Ler a caixa por IMAP (usuário e senha de
  aplicativo, como já se faz para ENVIAR) é o caminho mais simples e reusa o
  cofre de segredos que já existe. A alternativa — conectar pela API do Google
  ou da Microsoft — dá mais controle sobre marcar e mover, e dá mais trabalho.
- **Quanto de inteligência artificial usar.** Ler o anexo e entender "é uma nota
  de débito da locadora X, do contrato Y" custa por documento. Com muitos
  e-mails por dia isso pesa, e o teto mensal ainda não está definido. Uma saída:
  a IA só entra quando as regras simples (CNPJ do emissor, valor, número do
  contrato no texto) não resolverem.
- **O que fazer com e-mail que não é nada disso.** Propaganda, conversa,
  cobrança repetida. Precisa de uma pasta "ignorado" e de uma regra para não
  reprocessar o mesmo e-mail eternamente.

---

## 3. As notas fiscais

Já está especificado em **`NOTAS_FISCAIS.md`**, nesta pasta. O ponto de contato:
uma nota pode chegar por **três caminhos diferentes** — capturada da SEFAZ,
recebida por e-mail, ou anexada na tela — e **não pode virar três lançamentos**.
Seja qual for a porta, ela é a mesma nota, e o sistema tem de reconhecê-la pela
chave de acesso.

---

## A ordem que eu recomendo, e por quê

1. **Baixa por comprovante, pela tela do ERP.** É a menor peça, o casamento já
   está resolvido em `baixabradesco`, e entrega valor sem depender de decidir
   nada sobre e-mail.
2. **Baixa por comprovante, por e-mail.** Acrescenta só a porta de entrada —
   e obriga a resolver "como o ERP lê uma caixa", que serve para tudo depois.
3. **A tela do cruzamento** (notas × pedido × título × fundo fixo), que é onde
   o valor aparece.
4. **Monitoramento das caixas de compras e financeiro**, com marcação e
   confirmação humana.
5. **Captura das notas na SEFAZ**, trocando a fonte sem refazer o cruzamento.

O motivo da ordem: cada passo **usa o anterior** e nenhum precisa ser refeito.
Começar pelo e-mail antes de existir a tela de conferência seria despejar
documento num lugar que ninguém olha.

---

## O que fica registrado como risco

- **`baixabradesco` não pode ser quebrado.** Ele está em produção, dando baixa
  no Omie todo dia. Aproveitar o código dele é copiar/extrair as partes boas —
  não é mexer no que está rodando.
- **Confirmação humana é o freio, e ele tem de ser real.** Um sistema que casa
  sozinho e lança sozinho erra silenciosamente. Aqui o silêncio custa dinheiro
  em conta contábil errada e pagamento em duplicidade.
- **Nenhum e-mail pode ser apagado ou movido para fora do alcance da pessoa.**
  Marcar é diferente de sumir.
