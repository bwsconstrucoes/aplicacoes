# Módulo SUPRIMENTOS — especificação consolidada

> Base: o ditado do dono (partes 1 e 2, setembro/2026) **confrontado com os dados
> reais** das planilhas em uso. Este arquivo é a memória da área: quem abrir uma
> sessão nova sobre Suprimentos lê isto antes de qualquer coisa.
>
> Estado em 04/09/2026: **especificação fechada, nada construído ainda.**

---

## 1. O que a varredura das planilhas mostrou

Cinco das seis planilhas foram lidas na íntegra em 04/09/2026.

| Planilha | O que tem hoje |
|---|---|
| Cadastro de Insumos | **115 insumos** cadastrados, com sub-categoria, conta do plano financeiro e **prazo de entrega em dias úteis**; tabela de prazos por sub-categoria (50 linhas); **624 municípios** classificados; calendário de feriados por UF; 32 contas marcadas como "bloqueio pedido de compra" |
| Registro de Suprimentos | **14 situações com cor**, **16 unidades de compra**, **121 formas de pagamento** já classificadas em famílias |
| Registro de Fornecedores | **111 fornecedores** com região de atuação, porte, categoria de insumo, canal de cotação; tabela separada de **cotadores** (a pessoa que responde pelo fornecedor) |
| Solicitação de Suprimentos | itens com insumo, **especificação**, quantidade, unidade, obra, situação, título, prazo e previsão; abas separadas para pendências e para recebimento |
| Mapa de Cotação | cabeçalho por fornecedor com pagamento, condição, entrega, frete, desconto em R$ e acréscimo/desconto em %, responsável e total; preço e seleção **por item** |
| Registro de Cotações | **não lida** — 39 MB, a conexão do Drive expira antes de terminar. Fica pendente |

### 1.1 As 14 situações (com a cor que usam hoje)

`SOLICITAÇÃO` · `SALA TÉCNICA` · `COTAÇÃO` · `ANÁL. PROPOSTAS` · `PEDIDO EMITIDO` ·
`ALMOXARIFADO` · `AGUARD. COLETA` · `AGUARD. ENTREGA` · `EM TRÂNSITO` · `ENTREGUE` ·
`RECEBIDO` · `PENDÊNCIA` · `CANCELADO` · `SUSPENSO`

Nos dados aparece ainda **`AUTORIZAÇÃO`**, que não está na tabela de situações —
divergência a resolver. E **não existe `FINALIZADO`**: o fluxo termina em `RECEBIDO`.

### 1.2 As 16 unidades

UN, M, VR (vara), KG, L, LT (lata), BD (balde), SC (saco), GL (galão), PCT, CAR
(carrada), M2, M3, PL (pallet), CX, T.

### 1.3 Porte e região do fornecedor — valores reais

- **Porte:** Fábrica (50), Distribuidor (47), Rep. de Fábrica (4), e na lista de
  opções ainda Fornecedor Local e Homecenter.
- **Região de atuação:** RMF (59), BR (23), CE (9), NE (5) — e **combinações**
  ("CE, RMF", "NE, CE, RMF"). É campo de vários valores, não um só.
- **Canal de cotação:** e-mail em 109 dos 111. WhatsApp é exceção, não regra.

---

## 2. Onde a planilha **confirma** o ditado

- Acompanhamento **por item**, não pela solicitação inteira.
- Mapa com fornecedor em coluna, insumo em linha, preço por célula e **seleção
  por item** para fechar o pedido.
- Cabeçalho do mapa com forma de pagamento, condição, frete e modo de entrega.
- Obra por item (a planilha já registra obras diferentes na mesma solicitação).
- Título livre da solicitação como elemento de busca ("PISO CONCRETO POLIDO").
- Categoria do insumo e conta do plano financeiro **divergem** de propósito.

---

## 3. O que a planilha mostrou e o ditado não previa

Onze achados. Os cinco primeiros mudam o desenho.

1. **Especificação por item.** Além do insumo do catálogo, cada linha tem um
   texto livre: "Tarucel p/ Junta de Dilatação" + **"6mm"**; "Cola/Selante PU
   Sache 800ml" + **"cinza"**. Sem esse campo, o catálogo precisaria de uma
   entrada para cada variação — e viraria bagunça em um mês.

2. **A previsão de entrega hoje é calculada, não digitada.** A planilha guarda
   prazo em **dias úteis** por sub-categoria e por insumo, separado para região
   metropolitana e interior, cruza com 624 municípios e desconta feriados por
   UF. É informação de valor que se perderia numa migração ingênua.

3. **32 contas do plano bloqueiam pedido de compra.** Regra de negócio em uso,
   nunca dita: há material que não se compra por este caminho.

4. **121 formas de pagamento**, já organizadas em famílias: à vista; dias
   simples (7, 10, 30…); dias múltiplos (30/60/90); percentual de entrada mais
   parcelas (30% + 28/56 dias); e Nx parcelas. Não é uma lista para digitar — é
   uma **regra de geração de parcelas**, e é ela que liga o pedido ao financeiro.

5. **Cotador é uma entidade própria.** Quem responde a cotação tem nome, função,
   telefone e e-mail, e um fornecedor pode ter vários. O mapa registra qual
   deles respondeu cada proposta.

6. **Frete, desconto em R$ e acréscimo/desconto em %** por fornecedor no mapa —
   o ditado só citava frete.

7. **Coleta × Entrega** é atributo da proposta, e muda a logística depois.

8. **`ALMOXARIFADO` é situação de item** — parte da demanda é atendida do
   estoque, sem compra.

9. **Assinatura do comprador** (link de imagem) entra nos relatórios enviados ao
   fornecedor.

10. **O campo "Observações" virou diário.** Hoje ele guarda, em texto corrido,
    quem encaminhou, quando, para quem e o número do pedido anterior. No ERP
    isso é trilha de auditoria, que já existe.

11. **Bloco de recebimento separado**, com responsável, data e tipo de
    recebimento, e uma aba própria de pendências.

---

## 4. Decisões propostas

Recomendação para cada ponto que estava em aberto. Onde o dono não disser o
contrário, é por aqui que se constrói.

### 4.1 Decidido pelo dono em 04/09/2026

| # | Questão | Decisão | Observação |
|---|---|---|---|
| 1 | Aprovar o mapa ou o pedido? | **Sempre o pedido.** Existem dois tipos — pedido com mapa e pedido sem mapa (direto) — e a mesma tela de autorização atende os dois: quando o pedido veio de um mapa, o mapa aparece junto, com as alternativas que o comprador tinha | Uma fila só de autorização, dois modos de exibição |
| 2 | Previsão de entrega calculada? | **Não.** A data é digitada por quem solicita e, depois, pelo comprador ao fechar com o fornecedor | A tabela de prazos por região e o calendário de feriados **não** serão migrados |
| 3 | Registro de Cotações (planilha de 39 MB) | **Descartada.** Era só a forma de armazenar os dados, não tem regra a preservar | Não precisa ser lida |
| 4 | Banco de preços | **Entra no módulo** — ver seção 4.3 | Pedido novo do dono, não estava no ditado |

### 4.2 Recomendações ainda em aberto

| # | Questão | Recomendação | Por quê |
|---|---|---|---|
| 5 | Pendência: tabela nova ou saldo do item? | **Saldo do item** | Mesmo padrão da medição de empreita, que já funciona. Preserva o histórico e evita duas verdades sobre o mesmo material |
| 6 | Perfis por área agora? | **Camada aditiva** (ver seção 6) | Manter o perfil atual como base e acrescentar funções por área. Não mexe nas 125 rotas já protegidas |
| 7 | Formas de pagamento | **Regra, não lista** | Entrada em %, primeiro vencimento, intervalo e número de parcelas geram as 121 combinações — e geram as parcelas do título sozinhas |
| 8 | Almoxarifado | **Só a situação, sem controle de estoque** | Registrar que o item foi atendido do estoque é barato; controlar estoque é outro módulo |
| 9 | Situações | **15, incluindo `AUTORIZAÇÃO`; sem `FINALIZADO`** | É o que os dados mostram em uso |

### 4.3 Banco de preços (pedido do dono, 04/09/2026)

Tela de consulta ao histórico de preços dos insumos — **cotados e comprados**.

**O que fica guardado.** Todo preço que entra num mapa e todo preço que vira
pedido gera um registro: insumo, especificação, unidade, quantidade, preço
unitário, fornecedor, data, condição de pagamento, frete, obra e o mapa ou
pedido de origem. Preço **cotado** e preço **comprado** ficam distinguidos — o
comprado vale mais, porque alguém aceitou pagar aquilo.

**O que a tela faz.** Busca por insumo, com filtro de período, fornecedor, obra
e categoria. Para cada insumo: último preço, menor, maior e média no período, e
a lista de ocorrências com **link para o mapa ou o pedido de origem**.

**Por que importa mais do que parece.** É o que responde "este preço está bom?"
na hora de fechar, sem depender de memória. E resolve de graça a herança de
preço de outro mapa (parte 2, item 10.1): o mapa novo puxa o preço do banco,
marcado como herdado, com a data e o link da origem.


---

## 6. Permissão: decidido e construído em 04/09/2026

O dono resolveu a questão com uma frase melhor do que a proposta original:
"cadastro uma pessoa, ela tem uma função principal e a gente vai agregando
permissões — pode fazer isso, não pode fazer aquilo, pode ver essa tela".

**Como ficou.** O cargo continua sendo a base. No cadastro do operador há agora
a lista de tudo o que o ERP sabe fazer, em português, com o que vem do cargo já
marcado. Marcar acrescenta; desmarcar tira. Só o que difere do cargo é guardado
(tabela `usuario_permissoes`, migração 032).

**Por que resolve mais do que parece.** Nas palavras dele: "o diretor sai de
férias e eu quero deixar outra pessoa responsável por autorizar". Sem isso,
seria preciso inventar um cargo novo para cada arranjo temporário.

**Alçada por valor** (autorizar até tanto) ficou registrada como desejável e
não é usada hoje. Entra quando o pedido de compra tiver a tela de autorização.

**A trava contra o tiro no pé.** O administrador não consegue desmarcar de si
mesmo as telas que consertam o sistema — Configurações, cadastro de operadores
e a entrada no ERP. Sem isso, um clique errado deixaria a empresa sem ninguém
que pudesse desfazer.

**A dívida, dita com todas as letras.** Por um tempo convivem dois jeitos de
dizer quem pode o quê: o cargo e as exceções. Enquanto forem poucas, é mais
simples do que a alternativa. Se um dia metade das pessoas tiver dez exceções,
é sinal de que os cargos precisam ser redesenhados — e aí vale o trabalho de
transformar área e nível em estrutura.

---

## 5. Plano de construção

Cinco fases. Cada uma entrega algo que funciona sozinho.

**Fase 1 — Cadastros e importação.** Unidades, insumos, fornecedores (com região,
porte, cotadores e dados de pagamento) e formas de pagamento como regra.
Importar os 111 fornecedores e os 115 insumos das planilhas. Fluxo de
**solicitação de cadastro de insumo** com aprovação. Sem prazos e sem
calendário — decisão 2.

**Fase 2 — Solicitação.** Cabeçalho (título, previsão, prioridade) e itens
(insumo, especificação, quantidade, unidade, obra). Entrada assistida por IA
colando planilha. Acompanhamento por item.

**Fase 3 — Cotação e mapa.** Seleção de itens e fornecedores, disparo por e-mail
e WhatsApp com registro de envio, mapa com preço por item, leitura das propostas
por IA com crítica do que não reconheceu, menor preço destacado, anexo da
proposta na coluna do fornecedor, herança de preço de mapa anterior.

**Fase 4 — Pedido, autorização e financeiro.** Fechamento por fornecedor, pedido
direto sem mapa, tela de autorização com o mapa embutido, relatório ao fornecedor
separado por endereço de entrega, geração de previsão de pagamento e de título
antecipado.

**Fase 5 — Logística, recebimento e banco de preços.** Situações do pedido, cobrança automática de
atualização, recebimento na obra com nota fiscal lida por IA, boletos, frete,
pendência por saldo, os alertas cruzados entre suprimento e financeiro e a tela
do banco de preços (que se alimenta desde a fase 3, mas só ganha valor com
histórico).

---

## 6. O que ainda não foi verificado

- Os **cinco relatórios do mapa** não foram especificados — dependem dos modelos.
- Não se sabe quantas solicitações e pedidos existem hoje, nem se há histórico a
  migrar ou se o sistema começa vazio.

---

## 7. Sugestões (opinião, não decisão)

Coisas que a leitura das planilhas sugeriu e que o dono ainda não pediu.

1. **Alerta de preço fora da curva.** Com o banco de preços funcionando, o mapa
   pode avisar na hora: "este preço está 40% acima do último comprado deste
   insumo". Não bloqueia — avisa, como as críticas que já existem no ERP. É o
   uso mais valioso do banco de preços, e sai quase de graça depois dele.

2. **Medir o ciclo.** Guardando as datas de cada mudança de situação, o sistema
   responde sozinho: quantos dias entre solicitar e receber, por obra, por
   comprador, por categoria. Hoje ninguém sabe esse número. Custa pouco, porque
   a trilha de auditoria já vai existir.

3. **Não migrar o histórico das planilhas.** Trazer os cadastros (insumos,
   fornecedores) sim; trazer solicitações e pedidos antigos, não. O histórico
   velho tem o vício da planilha e sujaria o banco de preços logo no começo. A
   decisão é do dono — mas a recomendação é começar limpo, como já foi feito
   com o plano de contas do Omie.

4. **As observações viram auditoria.** Hoje um único campo de texto guarda quem
   encaminhou, quando, para quem e o número do pedido anterior. No ERP isso é
   evento registrado, pesquisável e impossível de sobrescrever sem deixar
   rastro. O campo de observação continua existindo, mas para o que é
   observação de verdade.

5. **Portal do fornecedor, mais adiante.** Em vez de o fornecedor mandar PDF ou
   foto e a IA ler, ele receberia um link e digitaria os preços na tela. A
   leitura por IA continua necessária para quem não usar o link — mas cada
   fornecedor que usar é uma leitura a menos para conferir. Fica para depois de
   o mapa estar de pé.
