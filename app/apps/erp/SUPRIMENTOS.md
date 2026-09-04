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

| # | Questão | Recomendação | Por quê |
|---|---|---|---|
| 1 | Aprovar o mapa ou o pedido? | **O pedido** — com o mapa embutido na tela de aprovação | Quem autoriza assume um valor com um fornecedor, e isso é o pedido. O mapa ao lado dá o contexto sem criar duas alçadas |
| 2 | Pendência: tabela nova ou saldo do item? | **Saldo do item** | Mesmo padrão da medição de empreita, que já funciona. Preserva o histórico e evita duas verdades sobre o mesmo material |
| 3 | Perfis por área agora? | **Adiar** | Mexer na matriz de 125 rotas antes de o dono homologar o que acabou de ser endurecido é trocar o alicerce com a casa em pé. Suprimentos entra com ações próprias na matriz atual |
| 4 | Formas de pagamento | **Regra, não lista** | Entrada em %, primeiro vencimento, intervalo e número de parcelas geram as 121 combinações — e geram as parcelas do título sozinhas |
| 5 | Previsão de entrega | **Sugerida pelo sistema, editável** | Traz a tabela de prazos e o calendário. A obra para de chutar, e o comprador continua com a palavra final |
| 6 | Almoxarifado | **Só a situação, sem controle de estoque** | Registrar que o item foi atendido do estoque é barato; controlar estoque é outro módulo |
| 7 | Situações | **15, incluindo `AUTORIZAÇÃO`; sem `FINALIZADO`** | É o que os dados mostram em uso |

---

## 5. Plano de construção

Cinco fases. Cada uma entrega algo que funciona sozinho.

**Fase 1 — Cadastros e importação.** Unidades, insumos (com prazos), fornecedores
(com região, porte, cotadores e dados de pagamento), formas de pagamento como
regra, municípios e feriados. Importar os 111 fornecedores e os insumos das
planilhas. Fluxo de **solicitação de cadastro de insumo** com aprovação.

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

**Fase 5 — Logística e recebimento.** Situações do pedido, cobrança automática de
atualização, recebimento na obra com nota fiscal lida por IA, boletos, frete,
pendência por saldo e os alertas cruzados entre suprimento e financeiro.

---

## 6. O que ainda não foi verificado

- A planilha **Registro de Cotações** (39 MB) não foi lida. Pode conter campos do
  pedido que não apareceram em outro lugar.
- Os **cinco relatórios do mapa** não foram especificados — dependem dos modelos.
- Não se sabe quantas solicitações e pedidos existem hoje, nem se há histórico a
  migrar ou se o sistema começa vazio.
