# HISTÓRICO — ERP · Suprimentos

Memória **desta área**. O chat é descartável; isto não.

⚑ **ESTE ARQUIVO NASCEU EM 18/11/2026**, quando o dono dividiu o chat do ERP em
quatro. O pedido dele, com as palavras dele:

> *"Talvez assim separar mais: a parte de pessoal eu trato no chat, a parte de
> suprimento eu trato no outro chat, porque assim uma coisa que eu vejo numa
> área eu jogo lá, e a outra coisa que eu vejo na outra área eu jogo no outro
> chat. Eu acho que tem como evoluir mais rápido dessa forma."*

⚠️ **O PASSADO DESTA ÁREA NÃO ESTÁ AQUI — está em `HISTORICO.md`**, que até
18/11/2026 guardava o ERP inteiro (12 mil linhas). Ele **não foi quebrado**, de
propósito: repartir doze mil linhas de memória por adivinhação perderia
contexto, e perder memória é o pior estrago possível neste repositório. Então:

- **procurar o passado** → `HISTORICO.md`, buscando por "suprimento", "cotação", "fornecedor", "insumo", "pedido de compra", "nota"
- **escrever o novo** → aqui

**Leitura desta área, além do `HISTORICO.md`:** `SUPRIMENTOS.md`,
`HOMOLOGACAO_SUPRIMENTOS.md`, `SUPRIMENTOS_ditado.md`, `ENTRADAS.md` e
**`CADEIA_DE_COMPRAS.md`** (o caminho inteiro, passo a passo, com o que está
pronto e o que falta — comece por ele).

---

## Onde o trabalho está

Insumos, fornecedores, cotações (o mapa de preços), pedidos de compra,
notas de entrada e o cruzamento nota × pedido.

⚠️ **CONSTRUÍDO E NUNCA OPERADO** — ver `SUPRIMENTOS.md`. É a área com
mais código sem uso real do repositório, e isso muda o cuidado: o que
está escrito não foi conferido por ninguém usando.

---

## ⚑ PENDENTE AGORA — leia isto antes de qualquer coisa

**Ramo `claude/admiring-brahmagupta-ac2tfg` — NÃO PUBLICADO em 20/11/2026.**
Traz a **migração 120** (apertar "Aplicar atualizações do banco" junto com a
publicação) e mexe em arquivos compartilhados: `routes.py`,
`core/auth/permissoes.py`, `core/auth/secoes.py`, `db/models/cadastros.py`,
`core/manutencao/limpeza.py`.

**Aprovado por ele e AINDA NÃO FEITO** ("as melhorias acho que podemos
implementar"), em ordem sugerida:

1. **Aviso de preço fora da curva** no mapa. Cuidado: o preço digitado no mapa
   já entra no banco de preços como COTADO — a referência tem de ser o último
   COMPRADO de ANTES desta cotação, senão o preço se compara com ele mesmo.
2. **Melhor fornecedor por categoria** (hoje só por insumo).
3. **Correção pela inflação (INCC)** ao comparar preço antigo com novo.
4. **Cobrança ao fornecedor por WhatsApp** — encosta na área da Mensageria.
5. **Fila única de propostas para ler** — ele não tinha entendido o nome; foi
   explicada em 20/11 e espera o "sim" dele.

**Proposto e esperando a resposta dele:**

- **Pedido de compra em PDF** (logo, tabela, assinatura do comprador), no lugar
  do texto no corpo do e-mail. O modelo dele está descrito em
  `CADEIA_DE_COMPRAS.md`.
- **Tela do pedido direto, sem cotação** — a regra existe
  (`pedido.fechar_direto`), a tela não.
- **Anexar a nota na hora do recebimento** e ela já nascer vinculada ao pedido.
- **Avisar o coordenador por Telegram** quando chega incompleto ou a entrega atrasa.

**Perguntas abertas com ele:** (a) o parceiro de fora e o perfil de consulta
devem ver "Acompanhar compras" e a "Projeção"? Hoje veem, porque as duas usam
`ver_suprimentos`, como a tela de Compras. (b) O histórico de preços da
planilha já foi importado? (c) Alguém já usa o módulo de verdade?

---

## Decisões desta área

### ⚑ 20/11/2026 — A CADEIA REVISADA DE PONTA A PONTA: três quebras, duas visões

**O pedido dele**, depois de mandar o modelo do mapa (nº 9228) e o do pedido
(ID 19457) que a equipe usa hoje:

> *"O coordenador de suprimentos precisa monitorar e acompanhar o que ele
> compra. (…) precisa fechar todo o ciclo do pedido à entrega e poder
> acompanhar o pagamento. É necessária visão geral e cruzada das informações.
> Por outro lado o financeiro não pode ficar no escuro em relação ao
> planejamento do compras."*

E os dois problemas que ele frisou, porque existem hoje: **(1)** o ciclo só
fecha com a entrega INTEGRAL, e o que faltar tem de virar pendência na mão do
coordenador; **(2)** *"muitas vezes chega material e a obra atrasa o lançamento
das notas e boleto. E o financeiro fica cego."*

**O QUE A REVISÃO ACHOU — três quebras, as três do mesmo formato (regra escrita
e testada, sem porta na tela):**

| Quebra | Desde quando | O que era |
|---|---|---|
| **O mapa não tinha botão de fechar pedido** | 05/09/2026 | `fechar_do_mapa` e a rota existiam; nenhuma tela chamava. Dava para cotar, comparar e mandar e-mail — não dava para COMPRAR. O roteiro de homologação mandava fechar no mapa |
| **A obra não tinha onde receber** | 05/09/2026 | o recebimento morava dentro da tela de Pedidos (`ver_pedidos_compra`: só comprador e diretor). E o POST aceitava `ver_suprimentos`, sem recorte de obra: consulta e parceiro de fora registravam recebimento de qualquer obra |
| **O financeiro não via a previsão** | 05/09/2026 | `previsoes_pagamento` nasce na autorização e só aparecia DENTRO do pedido. Nenhuma tela do sistema a somava. `previsao.titulo_id` nunca é preenchido por ninguém |

⚠️ **LIÇÃO, e ela já está no `CLAUDE.md` pela etapa do DP:** "CONSTRUÍDO E
NUNCA OPERADO" escondia isto. A suíte prova a regra; **só percorrer a tela
prova a porta**. Ao revisar uma área, procurar no template quem CHAMA cada rota.

**O QUE FOI FEITO:**

1. **Fechar pedido no mapa** (`erp_suprimentos_cotacoes.html`,
   `pedido.preparar_fechamento`). Um diálogo: de quem, o quê (vêm marcados os
   de menor preço; o que já está em outro pedido aparece travado com o
   número), condição, data, frete, desconto, e onde desce. **Condição de
   pagamento e data de entrega são obrigatórias NA TELA** — sem elas a projeção
   do financeiro não tem como ser feita.
2. **Local de entrega** (migração 120, `pedido_entregas`, `entrega.py`). Regra
   dele: *"a princípio a entrega é na obra associada ao pedido"*; a exceção
   (sede, outro endereço) *"pode ser definida no fechamento"*. É **por obra
   dentro do pedido**. Decisões minhas: o endereço é gravado POR EXTENSO (o
   pedido é documento e não muda se o cadastro da sede mudar); a "sede" é a
   empresa da obra; dá para trocar depois enquanto NADA chegou; tabela própria
   e não coluna em `pedido_itens`, porque a tela de Notas do Financeiro lê essa
   tabela e quebraria até a migração ser aplicada.
3. **Receber material** (`erp_suprimentos_receber.html`,
   `recebimento.a_receber`). A porta da obra: só as obras dela, **sem preço**,
   campo já preenchido com o que falta. Ação nova `receber_material` (obra +
   quem compra). No topo, o lembrete "recebido e nota não lançada".
4. **Acompanhar compras** (`acompanhamento.py`). Uma linha por pedido: chegou?
   tem nota? foi pago? e **o próximo passo que falta, com de quem depende**.
   Nada é digitado ali — cruza recebimento × nota vinculada × título da nota.
   Ciclo FECHADO = entregue inteiro + nota + pago.
5. **Projeção de pagamentos** (`projecao.py`). O que ainda vai virar conta a
   pagar, por mês e por obra, em três graus (comprado / a autorizar / em
   cotação). **Desconta o que já tem título**, para não contar a dívida duas
   vezes. A parcela com data vencida e sem título sai em vermelho.
6. **O texto do pedido ao fornecedor** ganhou o que o modelo dele tinha e o
   nosso não: CNPJ para faturar, CNPJ do fornecedor, CEP, link do mapa
   (quando a obra tem coordenadas), nº da solicitação por item e as três
   instruções da nota fiscal.

**CUSTO, medido** (regra de 17/11): o painel e a projeção fazem **7 a 9
consultas para a tela inteira**, com dez ou dez mil pedidos. ⚠️ **O resto do
suprimento NÃO é assim, e não foi mexido:** `pedido.listar` lê a tabela
`pedido_itens` INTEIRA duas vezes POR PEDIDO da lista (`_itens` e `total`
filtram em Python), e `montar_mapa` lê todos os preços de todas as cotações. A
numeração antiga da empresa passa de 19 mil pedidos; com esse volume a tela de
Pedidos faria dezenas de milhares de leituras de tabela inteira por abertura.
A causa é o dublê de testes ignorar `WHERE` — o código foi escrito para passar
nele. **Relatado ao dono em 20/11; conserto esperando o "pode".**

**O QUE FICOU DE FORA, dito:** o pedido em PDF; pedido direto sem cotação;
anexo da nota no recebimento; aviso por Telegram; a ligação automática
previsão → título (hoje o caminho é pedido → nota vinculada → título com a
nota; título lançado SEM vincular a nota não é enxergado pelo painel).

**Conferido:** 23 testes novos com banco (`tests/test_suprimentos_ciclo_banco.py`),
as quatro telas percorridas no navegador com dois usuários (admin e
administrativo de obra), fechamento com entrega na sede feito pela tela.
Mapa completo em `CADEIA_DE_COMPRAS.md`.

---

## Incidentes

(vazio)
