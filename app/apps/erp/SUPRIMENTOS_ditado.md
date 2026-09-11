# Módulo SUPRIMENTOS — o ditado do dono (fonte original)

> Palavras do dono, preservadas como foram ditadas em setembro/2026. A leitura
> consolidada e confrontada com os dados reais está em `SUPRIMENTOS.md`.

# Módulo SUPRIMENTOS — especificação (parte 1)

> Ditado por Marcelo Leitão e organizado para servir de base ao Claude Code.
> Cobre do cadastro até a autorização do pedido. O recebimento, a nota fiscal e
> a baixa ficam para a parte 2.
>
> **Anexos que vêm junto:** planilha de fornecedores (cabeçalhos), planilha de
> insumos, modelo do mapa de cotação e modelos de relatório.

---

## 1. Por que este módulo existe

Hoje o processo inteiro vive em planilhas: solicitação, cotação, mapa
comparativo e fechamento. Funciona, mas o acompanhamento se perde — não há como
saber, num lugar só, o que foi pedido, o que está cotado, o que foi comprado e o
que já chegou. O objetivo é trazer isso para o ERP reusando o que já existe:
obras, plano de contas, permissões, títulos a pagar e a leitura de documentos
por IA.

---

## 2. Cadastros necessários

### 2.1 Fornecedores (ampliar o cadastro atual)

O ERP já tem fornecedores. Faltam três campos que mudam a forma de comprar:

| Campo | Para que serve |
|---|---|
| **Área de atuação** | Município, estado ou Brasil. Como a BWS tem obras em vários locais, é o filtro que evita cotar com quem não entrega ali. |
| **Porte** | Fábrica, distribuidor, loja, depósito local. Serve para priorizar: quanto mais perto da fábrica, melhor o preço. Vira relatório de análise de compras. |
| **Contatos** | E-mail e telefone (WhatsApp) — são os canais de disparo da cotação. |

O restante (nome, CNPJ/CPF, localidade) já existe ou sai da planilha a importar.

### 2.2 Insumos (já existe parcialmente)

O cadastro de insumos foi criado junto com o módulo de Locações e já tem a marca
`locavel`, a categoria de suprimento e a conta do plano financeiro.

O que importa aqui é a **conta do plano**: ela é o que permite o pedido virar
previsão de pagamento com a apropriação correta, sem ninguém reclassificar
depois. Note que a categoria de suprimento e a conta financeira **divergem** em
alguns casos — são duas classificações distintas, propositalmente.

Há uma planilha de insumos a importar.

### 2.3 Unidades de compra (novo)

Unidade, metro, metro quadrado, metro cúbico, quilo, saco, etc. Toda linha de
solicitação tem quantidade + unidade.

### 2.4 Operadores — ampliação de perfis

O sistema de permissões precisa crescer para comportar as áreas. Hoje o perfil é
um só, global. O desenho pretendido:

- a pessoa é cadastrada e recebe acesso **por área** (financeiro, obras,
  pessoal, suprimentos);
- em cada área, com um nível: **administrador**, **operador** ou **leitor**;
- alguém pode ser administrador de suprimentos e leitor do financeiro.

> **Nota:** isto muda a matriz de permissões que acabou de ser endurecida.
> Vale desenhar antes de codar, porque toca as 125 rotas já anotadas.

---

## 3. Solicitação de suprimentos

É o começo do fluxo. Quem solicita costuma ser o administrativo de obra ou
alguém da sala técnica.

### 3.1 Cabeçalho da solicitação

- **Data do pedido** (automática)
- **Previsão de entrega** — a data em que quem pediu precisa do material em
  obra. Vem do cronograma da obra.
- **Título do pedido** — texto livre, ex.: "armadura da fundação". É o que torna
  a solicitação localizável depois. Vira elemento de busca.
- **Solicitante** (o operador logado)

### 3.2 Itens da solicitação

Cada linha: **insumo, quantidade, unidade e obra**.

> **Mudança em relação ao processo atual:** hoje a planilha obriga uma
> solicitação por obra. Isso deve acabar — uma solicitação pode conter itens de
> obras diferentes, e a obra fica **por item**. Cada obra tem seu endereço de
> entrega no cadastro, e isso precisa aparecer separado no relatório enviado ao
> fornecedor: "itens tais para a obra X, itens tais para a obra Y".

### 3.3 Entrada assistida por IA (importante)

Ao montar o cronograma, a quantidade de insumos costuma já estar tabulada numa
planilha. Deve ser possível **colar a planilha ou o texto** e a IA gerar as
linhas da solicitação — insumo, quantidade, unidade, obra — aproximando os nomes
contra o cadastro de insumos e apontando o que não reconheceu.

É o mesmo padrão já usado na leitura do contrato de locação.

### 3.4 Situações do item

O acompanhamento é **por item**, não pela solicitação inteira — porque os itens
de uma mesma solicitação seguem caminhos diferentes.

```
SOLICITADO → EM COTAÇÃO → PEDIDO EMITIDO → AGUARDANDO ENTREGA/COLETA
           → ENTREGUE → FINALIZADO
```

(a lista exata será confirmada com a planilha atual)

---

## 4. Cotação

### 4.1 Seleção

A tela de cotação mostra os **itens** de todas as solicitações abertas, com
filtros: obra, categoria de insumo, período, situação, solicitante.

O comprador filtra, seleciona os itens que quer cotar e escolhe os fornecedores
— usando **área de atuação** e **porte** como filtro, ou buscando pelo nome.

> Nem toda compra passa por cotação. Deve haver o caminho de fechar um pedido
> direto, sem mapa.

### 4.2 Disparo

A cotação vai por **e-mail (Google Workspace)** e/ou **WhatsApp (Z-API)** —
ambos já existem no monorepo.

**Sobre a confirmação de envio.** Hoje o comprador recebe cópia da cotação no
próprio e-mail e WhatsApp, como prova de que saiu. Isso deve mudar de forma: o
**sistema** passa a garantir e exibir o status do envio — enviado, entregue,
falhou — com o registro correspondente. O comprador pode continuar recebendo um
aviso curto ("cotação X disparada para N fornecedores"), mas como conveniência,
não como prova.

A orientação de ligar para o fornecedor confirmando continua valendo — e-mail
cai em spam, WhatsApp não entrega.

---

## 5. Mapa de cotação

Disparada a cotação, nasce o **mapa**, que é o coração do processo.

### 5.1 Estrutura

- **Cabeçalho:** os fornecedores para quem se cotou, cada um com forma de
  pagamento, se tem frete e o valor do frete.
- **Coluna da esquerda:** os insumos, com obra, situação e o ID da solicitação
  de origem.
- **Identificação do mapa:** título dado pelo comprador (pesquisável), obra,
  período, ID.

> **Limites:** a planilha atual trava em 50 insumos × 10 fornecedores por ser
> planilha. No sistema não há razão para esse limite — deve sair.

### 5.2 Preenchimento por IA

Este é o ponto de maior ganho e o de maior dificuldade.

As respostas chegam como PDF, foto, texto de WhatsApp ou e-mail, cada fornecedor
com sua nomenclatura. O sistema deve:

1. aceitar o arquivo/texto jogado **dentro do mapa aberto** (o mais simples, e
   resolve a associação: o mapa já está escolhido);
2. ler a proposta e **preencher os preços** aproximando os nomes dos insumos
   contra os itens do mapa;
3. **criticar o que não conseguiu** — item não reconhecido, preço ambíguo,
   fornecedor não identificado — pedindo a intervenção em vez de adivinhar;
4. opcionalmente, uma rotina de **leitura de e-mails** que faça o mesmo
   automaticamente, tentando associar ao mapa correto e pedindo indicação
   quando não conseguir.

### 5.3 Visualização

Na tela, os **menores preços destacados em cor**, como na planilha de hoje — é o
que permite o comprador enxergar de imediato onde comprar.

### 5.4 Relatórios do mapa (cinco, a detalhar com os modelos)

- melhor preço por item (compra pulverizada)
- melhor fornecedor único (compra concentrada, para facilitar logística —
  costuma sair mais caro no total e ainda assim ser a melhor decisão)
- os demais serão especificados junto com os modelos anexos

Os relatórios servem tanto para a análise interna quanto para enviar à
autorização.

---

## 6. Fechamento do pedido

No mapa, o comprador **seleciona os itens e o fornecedor** e dispara o pedido.

Um mesmo mapa gera vários pedidos, um por fornecedor. Os itens fechados mudam de
situação; os não fechados continuam disponíveis.

### 6.1 O comprador não compra sozinho

O pedido nasce **aguardando autorização**. Precisa haver uma tela de
acompanhamento dos pedidos fechados esperando alçada.

### 6.2 Tela de autorização

Quem autoriza precisa ver **o contexto, não só o número**:

- um resumo na lista (fornecedor, valor, obra, itens);
- ao abrir, **o mapa de cotação** como o comprador o fechou — com os itens
  selecionados e as alternativas visíveis, para dar para julgar se a escolha
  fazia sentido;
- poder **editar antes de autorizar**: desmarcar um item, recusar parte do
  pedido;
- **trilha de auditoria** de tudo isso.

> **Ponto para discutir:** aprovar o **mapa** (a decisão de compra) ou aprovar o
> **pedido** (o resultado dela)? Aprovar o mapa dá contexto e permite ajuste
> antes de virar pedido; aprovar o pedido é mais simples e mais próximo do que
> se faz hoje. Vale decidir antes de codar.

---

## 7. Ligação com o financeiro

É o que justifica ter feito o financeiro primeiro.

**Pedido autorizado gera previsão de pagamento.** Antes de autorizado, não gera
nada.

A conta do plano financeiro vem do **cadastro do insumo**, item a item — então a
apropriação nasce correta, sem reclassificação posterior. O rateio por obra sai
da obra de cada item.

Dois caminhos:

- **Pagamento antecipado** — alguns pedidos exigem pagamento antes da entrega.
  Deve gerar o título imediatamente, com vencimento, e permitir anexar os dados
  de pagamento.
- **Pagamento posterior** — gera a previsão. Mais tarde, no recebimento, associa-
  se a nota fiscal e os boletos, e a previsão vira título efetivo.

> A parte 2 detalha o recebimento, a conferência e a baixa.

---

## 8. Pontos em aberto para decidir antes de codar

1. **Aprovação: mapa ou pedido?** (item 6.2)
2. **Perfis por área** — como conviver com a matriz de permissões atual, que
   acabou de ser endurecida em 125 rotas.
3. **Etapas da obra** — o ideal seria vincular a solicitação à etapa do
   cronograma, mas fica para um segundo momento. Por ora, só o título livre.
4. **Situações do item** — confirmar a lista exata contra a planilha atual.
5. **Os cinco relatórios do mapa** — especificar com os modelos anexos.

---

## 9. O que já existe no ERP e deve ser reusado

| Peça | Onde está |
|---|---|
| Cadastro de insumos com categoria e conta do plano | `db/models/cadastros.py::Insumo` |
| Obras com endereço de entrega e conta de pagamento | `db/models/cadastros.py::Obra` |
| Leitura de documento por IA com registro de custo | `core/documentos/leitor.py`, `core/comum/ia_custo.py` |
| Aproximação de nomes contra cadastro | `core/locacoes.py::ler_contrato` |
| Títulos, rateio por obra e por conta | `core/titulos/service.py` |
| Permissões e escopo por obra | `core/auth/permissoes.py` |
| WhatsApp (Z-API) e Telegram | `app/apps/` (monorepo) |

**Não construir do zero o que já está aí.** O padrão de leitura por IA com
crítica do que não reconheceu, em particular, já foi resolvido três vezes no
sistema (documento fiscal, comprovante de fundo fixo, contrato de locação) e o
mapa de cotação é o quarto caso do mesmo problema.

---

# Módulo SUPRIMENTOS — especificação (parte 2)

> Continuação da parte 1. Cobre da autorização até o recebimento, o
> encontro com o financeiro e o tratamento de pendências.

---

## 10. Autorização — dois caminhos

Existem **dois tipos de pedido**, e ambos vão para a mesma alçada:

**Pedido direto.** Fechado com o fornecedor sem mapa de cotação. Usado quando o
valor é pequeno ou quando já se sabe que aquele fornecedor tem o melhor preço.

**Pedido via mapa.** Nasce da seleção de itens e fornecedor dentro do mapa, e a
autorização é feita **a partir do mapa** — quem aprova vê as alternativas que o
comprador tinha, não só a escolha final.

### 10.1 Reaproveitar um mapa em novo pedido

Situação real: duas obras iguais, os mesmos dez insumos, solicitações distintas.
Não faz sentido cotar de novo.

O desejado é **associar o novo mapa a uma cotação anterior**, trazendo os preços
já obtidos para preencher o novo mapa — com **destaque visual** de que aquele
preço veio de outra cotação e um **vínculo clicável** para abrir o mapa de
origem e conferir.

> É preciso deixar claro na tela que o preço é herdado, não recotado — senão
> alguém compra com base num preço de três meses atrás sem perceber.

### 10.2 Anexos da proposta no mapa

Ao jogar a proposta do fornecedor para leitura, ela deve ficar **anexada ao
mapa, na coluna daquele fornecedor**. Serve para duas coisas ao mesmo tempo:

1. alimentar a leitura automática dos preços;
2. ficar guardada como prova — na hora de autorizar, dá para abrir a proposta
   original e conferir se os preços lançados no mapa batem com o que o
   fornecedor mandou.

---

## 11. Formas de pagamento e dados do fornecedor

### 11.1 Cadastro de formas de pagamento (novo)

Há uma infinidade de arranjos e todos precisam caber: 30/60/90, boleto à vista,
faturado, cartão, cartão parcelado, 50% na compra e 50% na entrega, dinheiro
mais faturado. Hoje já existe uma lista em uso, que virá nos anexos.

Duas coisas a padronizar: **a forma de cadastrar** esses arranjos, e a
distinção entre **boleto à vista** (paga agora) e **faturado** (paga depois) —
que no jargão comercial são coisas diferentes, ainda que tecnicamente ambos
sejam faturamento.

### 11.2 Dados de pagamento no cadastro do fornecedor

Quando a compra é **à vista** — pagamento antes de receber o produto —, não vem
boleto depois. É preciso ter, no cadastro do fornecedor, **chave Pix e dados
bancários**, e o sistema deve **travar o fechamento** de um pedido à vista com
fornecedor sem esses dados.

> O ERP já tem `FornecedorConta` com Pix e TED homologados. Reusar, não criar
> outra estrutura.

### 11.3 Boleto informado no fechamento

Casos como compra no Mercado Livre já geram o boleto na hora. Ao fechar o
pedido, o comprador informa a forma, **anexa o boleto ou a proposta** e preenche
o código de barras — vale tanto para pedido direto quanto para pedido via mapa.

---

## 12. Cadastro de insumos com controle

Cadastro de insumo **não pode ser aberto a qualquer um**. Liberar para todos
produz duplicidade, nomenclatura inconsistente e categoria errada — o banco
vira bagunça e os relatórios param de significar coisa alguma.

O procedimento atual, que deve virar tela:

1. quem precisa **solicita o cadastro** do insumo;
2. um responsável (gestão de suprimentos, diretor ou administrador) analisa,
   **decide a nomenclatura**, a categoria de insumo e a conta do plano
   financeiro;
3. efetiva o cadastro;
4. **o solicitante é avisado** de que o insumo foi cadastrado.

> É um fluxo pequeno mas essencial: é o que mantém a base limpa.
> O aviso pode reusar o Telegram, como no resto do sistema.

---

## 13. Prioridade da solicitação

Na solicitação, um campo de **prioridade**: alta, média, normal.

A razão é operacional: a empresa pode estar num momento de caixa apertado e não
conseguir comprar tudo. A prioridade é o que permite ao comprador focar no que
não pode esperar — e distingue o pedido feito com antecedência do pedido que,
se atrasar, para a obra.

---

## 14. Acompanhamento logístico

Depois de autorizado, o pedido entra na logística. Este é o ponto que hoje mais
se perde, e serve a engenharia tanto quanto ao suprimento: **o engenheiro
precisa saber se o material vai chegar para se programar.**

### 14.1 Previsão de entrega

Ao fechar o pedido, o comprador informa a **data prevista de entrega**,
combinada com o fornecedor. É essa data que a obra acompanha e que o sistema
cobra depois.

### 14.2 Situações do pedido

```
PEDIDO EMITIDO → AGUARDANDO (algo) → AGUARDANDO ENTREGA / AGUARDANDO COLETA
               → EM TRÂNSITO → ENTREGUE → RECEBIDO → FINALIZADO
```

A tela de acompanhamento precisa permitir **atualizar a situação** e dar
visibilidade a todos.

### 14.3 O sistema cobra a atualização

Não basta permitir atualizar: o sistema tem de **cobrar**.

- previsão de entrega venceu e o pedido não foi entregue → alerta ao comprador,
  pedindo nova previsão ou a baixa;
- material entregue e a obra não deu o recebimento → alerta à obra;
- pedido recebido sem o lançamento financeiro → alerta ao comprador.

> A razão: o trabalho do suprimento só termina com **duas** coisas — o material
> entregue **e** a obrigação financeira lançada. Hoje qualquer uma das duas pode
> ficar para trás sem ninguém notar.

---

## 15. Recebimento na obra

Quem confere é a obra, não o suprimento.

### 15.1 O fluxo

O colaborador da obra procura entre os pedidos **da sua obra**, encontra o
pedido correspondente ao material que chegou, e associa a **nota fiscal**.

A IA faz o **confronto entre a nota e o pedido** — itens e quantidades —
apontando divergências. O que ela não conseguir casar, a pessoa resolve
manualmente.

### 15.2 O que se agrega no recebimento

- a **nota fiscal**;
- os **boletos**, quando vierem junto (nem sempre vêm — às vezes chegam por DDA
  ou por e-mail ao comprador);
- eventualmente um **título de frete**, que hoje é lançado solto e deveria ficar
  **associado ao pedido**.

### 15.3 O efeito no financeiro

Fechado o recebimento, a **previsão vira título** — com as parcelas, os boletos
e os documentos anexados.

---

## 16. O encontro entre suprimento e financeiro

Este é o ponto onde os dois lados hoje se desencontram, e onde o sistema mais
agrega.

### 16.1 Visão para o comprador

O comprador precisa ver, num lugar só, **as duas pendências** de cada pedido:

| | Situação |
|---|---|
| **Logística** | previsão de entrega, atraso, material recebido ou não |
| **Financeiro** | previsão lançada, título gerado, o que falta lançar |

Com isso ele consegue cobrar a obra pelo lançamento que não foi feito, em vez de
descobrir depois.

### 16.2 Caminho inverso: boleto antes do recebimento

Acontece de o comprador receber a nota e os boletos por e-mail **antes** de o
material chegar. Ele deve poder abrir o pedido, anexar a parte financeira e
**transformar a previsão em título** — mesmo com o recebimento ainda pendente.

### 16.3 Alerta para o financeiro (importante)

Quando o financeiro olhar um título vindo de compra, precisa enxergar **o estado
do suprimento**:

- material **recebido e conferido** → pode pagar tranquilo;
- material **ainda em trânsito ou a coletar**, com título vencendo → **alerta**.
  O financeiro procura o suprimento e pergunta: pode pagar mesmo?

> É a mesma lógica das críticas que já existem no ERP: não bloquear, mas não
> deixar passar em silêncio.

---

## 17. Pendências de recebimento parcial

Quando o material não chega integral, o que faltou vira **pendência**, e
pendência tem tratamento mais urgente que solicitação nova.

**Como é hoje:** a solicitação original é marcada como finalizada e se gera um
registro novo de pendência, em outra tabela. Marcelo tem dúvida se é a melhor
forma — e a dúvida procede.

**Sugestão a avaliar:** em vez de duas tabelas, a pendência é o **saldo do
próprio item**. O item da solicitação tem quantidade pedida e quantidade
recebida; o que sobra continua vivo, marcado como pendência, e pode entrar numa
nova cotação sem perder o vínculo com a origem.

Vantagem: preserva o histórico ("este item foi pedido em janeiro, recebido pela
metade em fevereiro, o resto em março") e evita duas verdades sobre o mesmo
material. É o mesmo padrão que já funciona na medição de empreita, onde cada
item tem saldo próprio.

---

## 18. Pontos em aberto (acumulado das duas partes)

1. **Aprovação: mapa ou pedido?** — na prática, os dois caminhos existem
   (pedido direto e via mapa), então talvez a resposta seja "ambos, com telas
   diferentes".
2. **Perfis por área** — como conviver com a matriz de permissões atual.
3. **Pendência: tabela nova ou saldo do item?** (item 17)
4. **Etapas da obra** — vinculação ao cronograma, adiada.
5. **Situações** — confirmar as listas exatas (item e pedido) contra a planilha.
6. **Os cinco relatórios do mapa** — especificar com os modelos.
7. **Formas de pagamento** — padronizar o cadastro a partir da lista atual.

---

## 19. Anexos que vêm junto

- planilha de fornecedores (cabeçalhos e área de atuação/porte)
- planilha de insumos
- modelo do mapa de cotação
- modelos dos cinco relatórios
- planilha de solicitações e a de pendências
- lista atual de formas de pagamento

---

## Anexo — lista de telas e planilhas de origem

```
Cadastro dos Compradores (precisa de email, tel contato e link assinatura pra geração de relatorios)
Cadastro dos Autorizadores (precisamos ampliar o cadastro das pessoas no sistema pra designar mais funções a cada um. Tipo alguém pode lancar SP e Autorizar um Pedido)
Cadastro de Fornecedores
Cadastro de Insumos
Solicitação de Suprimentos
Mapa de Cotação (Pode cotar mais de uma obra)
Pedido de Compra (Pedido para uma ou mais Obras. Relatório que será enviado ao fornecedor divide as informações de acordo com o endereço de entrega. Permitir entrega de mais de uma obra em um único endereço. ENdereço no cadastro da obra)
Autorização de Pedidos (Relatório Padronizado, envio ao fornecedor, gera previsão financeira)
Pedido com Antecipação de Pagamento
Acompanhamento dos Pedidos
Recebimento de Pedido (Inserir Imagem Comprovando) => Associação de Nota Fiscal à previsão financeira gerada pela emissão do pedido, IA auxilia na leitura.

Responsável da Obra precisa associar 



Solicitação de Suprimentos
https://docs.google.com/spreadsheets/d/1PvecWVPcqMmj1o056ZErevt0cjna6ggz48uNFTahu_M
Aba Pedidos

Registro de Suprimentos
Aba: Database
https://docs.google.com/spreadsheets/d/1h9dvtSW57vJreswJiDyMTbossGetZYlOYLgEKicVTPk
Tem unidades dos insumos
Tem Cores dos Status
Tem Status

Registro de Cotações e Pedidos
https://docs.google.com/spreadsheets/d/1JKhvjAUlTuqt2yMbqZNnzk4IGJ57Cx0MwMU4hGH_ajY
Aba Registros

Mapa de Cotação
https://docs.google.com/spreadsheets/d/16-Ch9Nbw6QjY-iMBz9RSsQzU2EhuYUIlAoYC-JaTmxQ
Aba Mapa

Cadastro de Fornecedores
https://docs.google.com/spreadsheets/d/1xIXuYhPRBgAnIk4aLV93kyWGikR7RKMAWVHPGiLYPQk
Aba Registro

Cadastro de Insumos
https://docs.google.com/spreadsheets/d/180HnGrcMGbpqv_rVGi-lPcCNZ35EJcE2W6x1t5U-rns
Aba Cadastrar```
