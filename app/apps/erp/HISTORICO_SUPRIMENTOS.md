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

**Traz também a migração 121** (`pedido_titulos`) — as duas, 120 e 121, no
mesmo aperto do botão. E, desde a 3ª rodada, mexe em arquivos de OUTRAS áreas:
`templates/erp_lancar.html` e a rota `api_criar_titulo` (Financeiro),
`core/portal.py` (portal), `static/erp.css`.

**Aprovado por ele e AINDA NÃO FEITO**, em ordem sugerida:

1. **Aviso de preço fora da curva** no mapa. Cuidado: o preço digitado no mapa
   já entra no banco de preços como COTADO — a referência tem de ser o último
   COMPRADO de ANTES desta cotação, senão o preço se compara com ele mesmo.
2. **Melhor fornecedor por categoria** (hoje só por insumo).
3. **Correção pela inflação (INCC)** ao comparar preço antigo com novo.

**Falta, e ele ainda não pediu com todas as letras:**

- **Tela do pedido direto, sem cotação** — a regra existe
  (`pedido.fechar_direto`), a tela não.
- **Pedir material pelo portal** — o portal recebe e acompanha; pedir ainda é
  pela tela de Solicitações.
- **A imagem da assinatura do comprador** no PDF do pedido (hoje sai nome,
  e-mail e telefone de quem manda).
- **Avisar o coordenador por Telegram** quando chega incompleto ou atrasa.
- **Ler a caixa de e-mail sozinho** (as propostas hoje são JOGADAS no mapa;
  ninguém lê a caixa). É o item 2 de `ENTRADAS.md`.
- **Anexar documento também pela tela do ERP** (Suprimentos › Receber
  material); hoje só o portal anexa.
- **A ciência e a autorização da compra feita fora do processo** — proposta
  feita em 21/11, espera três respostas dele (ver a entrada de 21/11).
- `exportarMapa` em `erp_suprimentos_cotacoes.html` ficou sem uso (o PDF do
  mapa passou a ser feito no servidor) — pode sair.

**Perguntas abertas com ele:** (a) o histórico de preços da planilha já foi
importado? (b) alguém já usa o módulo de verdade? (c) a obra deve lançar a nota
pelo celular numa tela enxuta, ou a tela de Lançar do ERP serve?

**Decidido por ele em 20/11 (não perguntar de novo):** parceiro de fora e
perfil de consulta PODEM ver Painel, Acompanhar e Projeção ("ok, pode ver
essas telas"); o pedido vai em PDF por e-mail; cobrança por WhatsApp para quem
tem WhatsApp como canal no cadastro; a lentidão era para consertar.

---

## Decisões desta área

### ⚑ 21/11/2026 — O PORTAL EM LISTA, OS DOCUMENTOS NO RECEBIMENTO, E A COMPRA POR FORA

Três coisas que ele viu na imagem do portal:

**1. Lista que abre.** *"Tem três pedidos a serem recebidos — o ideal é a lista
e você clicar e abrir, porque pode ser um pedido grande com muitos insumos."*
Eu tinha entregue os três pedidos abertos, um embaixo do outro. Agora cada
pedido é uma linha (fornecedor, número, quantos itens faltam, previsão ou
atraso) e abre ao tocar — um por vez; o que foi digitado fica guardado ao
trocar de pedido.

**2. Anexar nota, boleto e mais.** *"Eu não vi nenhum campo de anexar nota
fiscal, boleto. A gente tem que prever já isso. E da mesma forma que no DP, a
gente precisa poder anexar vários documentos."* Os arquivos ficam presos AO
RECEBIMENTO (`anexos`, entidade `recebimento`), com tipo (nota, boleto, foto,
outro — palpite pelo nome, corrigível). Sobem depois do recebimento gravado;
falha em anexar não desfaz o recebimento. **A tela de Lançar os traz
sozinha** quando abre por aquele recebimento: ninguém escolhe o mesmo arquivo
duas vezes. Rotas próprias, com o recorte pela obra do pedido — a rota
genérica de anexos não conhece "recebimento" e não foi mexida.
⚠️ Só o PORTAL anexa. A tela Suprimentos › Receber material (ERP) ainda não
tem o campo.

**3. A compra que não passou pelo suprimento.** *"A gente tem que tentar
inibir a possibilidade de uma compra sem ter seguido o processo do suprimento.
Imagina um engenheiro da obra que foi lá e comprou um material (…) quando
fosse anexar uma nota e identificar que não identificou um pedido — por que
estão lançando isso? Aí tem que pensar na forma de dar ciência e alguém
autorizar."*

**Feita a metade que IDENTIFICA**, e ela não bloqueia nada: na tela de Lançar,
escolhido o credor, o sistema procura os pedidos de compra dele com conta a
lançar e pergunta "é deste pedido?" (`lancamento.pedidos_do_fornecedor_a_lancar`).
Um clique liga o lançamento ao pedido, mesmo depois de a nota já ter sido lida.

**NÃO FEITA a metade que dá ciência e exige autorização — de propósito, e a
razão é o dia de hoje:** nenhuma compra da empresa passa pelo ERP ainda. Ligar
essa trava agora marcaria 100% dos lançamentos de material como "compra por
fora" e pararia o financeiro. Proposta entregue a ele em 21/11, esperando três
respostas: (a) quem autoriza; (b) trava o pagamento ou só avisa; (c) a partir
de quando, e para quais obras. Desenho proposto: lançamento de MATERIAL sem
pedido exige o motivo escrito, nasce marcado "compra fora do processo", avisa
o coordenador de suprimentos e — se ele quiser a trava — fica bloqueado até a
ciência de quem autoriza, pela crítica do motor de análise do Financeiro
(`core/titulos/analise.py`), que é onde título bloqueado já mora. Com chave de
ligar em Configurações e data de início.

**Lição desta rodada (minha):** reescrevi a tela do portal recortando o
arquivo por marcadores de texto, um marcador casou no lugar errado e o código
ficou fora de ordem — a suíte passou, porque a página do portal não está no
teste de sintaxe das telas. Só o navegador pegou. Tela reescrita se grava
INTEIRA, e se abre no navegador antes de dizer que está pronta.

### ⚑ 20/11/2026 (3ª rodada) — O FINANCEIRO NASCE DO RECEBIMENTO, O PORTAL RECEBE, O MAPA VIRA PLANILHA

Ele leu a primeira entrega e corrigiu o rumo em sete pontos. Os três que mudam
o desenho:

**1. "EU QUERO GERAR O FINANCEIRO A PARTIR DO RECEBIMENTO."** Eu havia escrito
como "ponto fraco" que o pagamento só era visto se a nota fosse vinculada ao
pedido na tela de Notas. Resposta dele: *"na verdade, eu quero gerar o
financeiro a partir do recebimento, como eu já falei previamente, que
atualmente é assim que funciona. A pessoa vai receber o pedido e lança nota,
lança boleto, lança tudo e gera a obrigação financeira. Ou confirma uma
previsão que já existe."*

Como ficou (`core/suprimentos/lancamento.py`, migração 121):

- depois de receber, a obra clica em "Lançar a nota e o boleto" e cai na tela
  de **Lançar** com `?pedido_compra=N&recebimento=M`. A tela abre com credor,
  valor DO QUE CHEGOU (não do pedido inteiro), rateio por obra com a conta do
  plano de cada insumo, e as parcelas da previsão;
- o título nasce em `titulos.service.criar_titulo`, pelo caminho de sempre —
  **este módulo não cria título**. Depois de criado, `vincular` grava
  `pedido_titulos` e marca as previsões cobertas (`titulo_id`, que existia e
  ninguém preenchia). É a "previsão que já existe" sendo confirmada;
- o painel e a projeção passaram a enxergar esse título direto. Lançado assim,
  conta também como "tem nota" — a nota é jogada na própria tela de Lançar.

⚠️ **Decisão minha, dita a ele:** não fiz uma tela de lançamento própria do
suprimento. Duas telas de lançar divergiriam nas críticas fiscais e de
duplicidade, e `SUPRIMENTOS.md` §8.2 já dizia para não contornar o financeiro
por dentro. O custo: a obra sai do portal (celular) e cai na tela de Lançar do
ERP, que é de computador. Se isso incomodar, o caminho é uma tela enxuta de
lançar que chame as MESMAS rotas — não uma regra nova.

**2. "O RECEBIMENTO VAI SER PRINCIPALMENTE PELO PORTAL."** *"O pessoal
administrativo de obra tem uma visão mais limitada do sistema. Semelhante ao
que a gente faz com o DP."* A área "Material da obra" do portal abriu
(`/erp/portal/material`, `erp_portal_material.html`), com três abas: para
receber, em andamento (o que ainda está em cotação ou autorização — só
acompanhar) e falta a nota. Usa as MESMAS rotas de Suprimentos › Receber
material: duas portas, uma regra. A frase da área no portal NÃO promete "pedir
material", porque isso ainda não existe lá.

**3. O MAPA.** Três queixas e um pedido:

- *"o modal se torna pequeno (…) tem que ter um modal que expanda na tela"* —
  botão **Tela cheia** no mapa; os diálogos da tela (relatórios, fechar
  pedido) passaram a ocupar quase a tela inteira (`dialog.largo`);
- *"é uma planilha que a gente consegue visualizar bastante informação"* — o
  mapa foi refeito no formato do relatório que ele mandou (9228): condições de
  cada fornecedor em LINHAS, unitário e subtotal lado a lado, Nº SS e número
  do item, colunas da esquerda presas na rolagem, e no rodapé melhor
  fornecedor e melhor compra, sem e com frete;
- *"eu jogo o arquivo, aí o sistema faz a leitura e tenta preencher o mapa"* —
  já existia "ler proposta" por coluna; agora há uma área no mapa onde se joga
  o arquivo (vários de uma vez) e o sistema **descobre de quem é** pelo CNPJ
  (matriz e filial contam) ou pelo nome. Não reconheceu, PERGUNTA — não chuta;
- *"quero ver como fica o mapa para a gestão"* — o "Mapa em PDF" era a
  exportação genérica (só preço unitário). Virou `mapa_pdf.py`, no servidor:
  o mapa no formato da planilha + os quatro resumos + linha de aprovação.

**Defeito achado e consertado:** confirmar as condições lidas de uma proposta
recarregava o mapa e APAGAVA os preços que a IA tinha acabado de preencher e
ainda não estavam salvos (`recarregarMantendoDigitado`).

**Os outros quatro pontos:**

- **Pedido em PDF** (`pedido_pdf.py`): anexo ao e-mail; o texto continua no
  corpo. Um bloco por local de entrega com subtotal, parcelas com data e
  valor, instruções da nota. Falha ao gerar o PDF não segura o envio.
- **Cobrança por WhatsApp** (`cobranca.telefones_de_whatsapp`): só para
  fornecedor com WHATSAPP em `canais_cotacao`; número do contato escolhido no
  disparo, senão o do fornecedor. O e-mail continua saindo junto.
  ⚠️ Usa `notificar(canais=("whatsapp",))` SEM `finalidade` — não passa pela
  política da tela Mensagens. Se a Mensageria quiser governar isso, é criar a
  finalidade lá. ⚠️ E a cotação ainda exige a conta de e-mail da empresa
  configurada mesmo para quem só tem WhatsApp.
- **Lentidão**: `pedido.listar` caiu de 2 leituras da tabela inteira POR
  PEDIDO para 3 consultas no total; `montar_mapa`, envio, recebimento e
  proposta ganharam `.where`. O `if` em Python ficou ao lado, de propósito: a
  sessão dublada dos testes ignora `WHERE`. Sobram dezenas de leituras de
  tabela inteira em cadastro/planejamento/cobrança/desempenho — são de tabela
  pequena ou de tela pouco aberta; não foram tocadas.
- **"As propostas que chegaram" — ele perguntou se era o e-mail.** Não é:
  ninguém lê a caixa. A "fila única" que propus perdeu o sentido com o
  arquivo jogado no mapa, e saiu da lista.

**Conferido:** 55 testes em `tests/test_suprimentos_ciclo_banco.py`; o caminho
portal → receber → Lançar pré-preenchido percorrido num navegador em tamanho
de celular; o mapa em tela cheia e os dois PDFs abertos e lidos.
**Não conferido:** leitura de proposta de verdade pela IA (não há chave aqui —
o reconhecimento do fornecedor foi testado com a leitura simulada); envio real
de e-mail e de WhatsApp.

### ⚑ 20/11/2026 (2ª rodada) — O PAINEL DO COORDENADOR E O REGISTRO COMPLETO DOS PEDIDOS

Depois de ver a primeira entrega, ele pediu o que tinha "no método antigo":

> *"Uma tela onde eu listo todos os registros dos pedidos fechados (…) o
> status, se já foi entregue, se já tem lançamento financeiro, se já tem nota
> fiscal, se já foi pago, previsão de entrega — tudo isso pesquisável.
> Lembrando que a gente sempre está usando o sidebar de forma completa (…)
> busca por fornecedor, por tipo de insumo, por obra. Tem que ter um painel do
> coordenador, para ele bater o olho e compreender a coisa (…) o que é
> prioritário, o que não é, o que está atrasando, o que atrasou, o que deveria
> ter chegado e não chegou."*

**REGRA QUE FICA, e vale para toda tela de lista do ERP:** a barra lateral é
COMPLETA — um filtro para cada pergunta que a pessoa faz, com a contagem do
que sobraria em cada opção. Eu tinha entregue "Acompanhar compras" com dois
filtros e ele notou na hora.

**Não nasceu tela nova de pedidos: "Acompanhar compras" VIROU o registro.**
Ganhou filtros de entrega, nota fiscal, lançamento financeiro, pagamento, obra,
tipo de insumo, fornecedor e período da previsão; a busca acha o pedido por
qualquer material comprado; a linha aberta mostra todos os itens e leva ao
pedido e à cotação; e "incluir fechados" + "incluir recusados e cancelados"
fazem dela o registro de tudo.

**Tela nova, e é a primeira aba: Suprimentos › Painel**
(`acompanhamento.planejamento`). A linha é o ITEM que a obra pediu — é ele que
tem a data em que a obra precisa, e existe antes de haver cotação. Compara
três datas (precisa até × previsto pelo fornecedor × hoje) e classifica:
já deveria ter chegado · fornecedor estourou a data dele · vai chegar depois
do que a obra precisa · em risco · no prazo · sem data. **A lista vem
ordenada pelo pior prazo e, dentro dele, pela prioridade.** Os quadros de
baixo são as compras fechadas e levam ao registro já filtrado.

⚠️ **"Baseado em tempo de entrega" — ele perguntou se temos, e NÃO TEMOS.** A
tabela de prazos por insumo da planilha não foi migrada (decisão dele de
04/09). "Em risco" usa régua fixa: faltam 7 dias ou menos e não há pedido
autorizado (`FOLGA_DIAS`). O caminho certo, quando houver uso: medir o prazo
real de cada categoria pelos próprios pedidos (autorização → recebimento).

**Defeito achado de passagem e consertado:** o contador "Prazo estourado" da
tela de Solicitações contava material já recebido ou cancelado — olhava só a
data. Número vermelho que nunca zera ensina a ignorar o vermelho.

**Sobreposição que fica, dita:** a tela de Solicitações continua existindo e
é onde se TRABALHA o item (corrigir, selecionar para cotar); o Painel é onde
se ENXERGA a fila inteira até a chegada. As duas listam itens.

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
