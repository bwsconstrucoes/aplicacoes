# ERP — onde o trabalho está, decisões, incidentes e o que falta

Este arquivo existe para uma sessão nova (ou uma pessoa nova) pegar o ERP sem
repetir o que já foi discutido e sem repetir o que já deu errado.

- `ROTEIRO.md` é o **backlog**: o que já foi entregue e o que está na fila.
- `CONTEXTO.md` (raiz) §2.1 e §3.8–3.11 explicam **como** o ERP é feito.
- Este aqui diz **em que pé está agora, por que está assim e o que morde**.

Leia os três antes de mexer. Atualize este ao encerrar a sessão.

---

## Onde o trabalho está

ERP financeiro em `/erp`, Flask + Postgres no Render, 15 módulos no mesmo
serviço. Contas a pagar completo; Pessoal, Empreitas e Locações em uso;
**Suprimentos construído e nunca operado** — ver `SUPRIMENTOS.md`.

**Estado em 10/09/2026:** `main` publicada e **banco atualizado até a migração
054** — o dono apertou "Aplicar atualizações do banco" no mesmo momento da
junção. Nesta publicação foram quatro entregas: as listas com "carregar mais"
(solicitações), a **tela de saúde do sistema** (054), a **ficha do título em
card** com o encadeamento entre telas, e a **leitura do documento por IA no
Arquivo**. Suíte: 2.170 casos sem banco e 1.330 com banco de verdade. **Nada
pendente no ramo `claude/oi-vjvrn8`.**

⚠️ **Duas coisas só se provam em produção e ainda não foram provadas:** a
leitura de documento pela IA no Arquivo (não há chave da OpenAI no ambiente de
desenvolvimento) e a busca do INCC no Banco Central (a saída de internet de lá
é filtrada). Se qualquer uma falhar, é configuração no Render, não código —
mas ninguém confirmou ainda que funcionam.

**Estado anterior, em 05/09/2026 (noite):** `main` com a autorização padrão-NEGAR, o
alcance por operador (029), o consumo de IA com teto (030), as travas de
concorrência (031), a permissão fina por pessoa (032) e o **módulo de
Suprimentos** (033 a 037). Publicado também o **botão de zerar o movimento por área** e a **reforma das
telas de cadastro de Suprimentos** (detalhada abaixo), mais a correção das
três telas que nunca funcionaram (ver Incidentes). Suíte: 2.097 casos com
banco de verdade. **Nada pendente no ramo.**

### A reforma das telas de cadastro (05/09/2026, noite)

O dono abriu Suprimentos pela primeira vez e a tela de Cadastros não serviu.
A reclamação principal **não era de gosto, era um defeito**: não havia como
cadastrar categoria de insumo pelo sistema — e sem categoria não se cadastra
insumo, sem insumo não se pede material. O módulo inteiro estava intransitável
e nada acusava isso.

O que mudou:

| Antes | Agora |
|---|---|
| uma tela "Cadastros" com carga de CSV, condições, unidades, categorias e pedidos de insumo empilhados | quatro sub-telas: **Insumos**, **Fornecedores**, **Categorias/unidades/pagamento** e **Importações** |
| categoria de insumo só existia se viesse de importação | cadastra, renomeia e desativa pela tela |
| insumo só nascia por "pedir → decidir" | quem administra cadastra direto; o pedido continua para quem está na obra |
| a conta do plano oferecia o plano inteiro, receita incluída | só contas de despesa e material, em lista com busca |
| listas longas em caixinha de rolagem | busca em toda lista longa (insumo, conta, fornecedor, categoria) |
| filtros no topo, diferente do resto do ERP | filtros na **barra da esquerda**, como Títulos e Obras |
| sem visão de gestão | telas de insumos e fornecedores tipo planilha: filtro, busca, ordenação por coluna, edição na própria célula, KPIs e exportação do que está na tela |
| a cotação só nascia na tela de Cotações | seleciona-se os itens na tela de **Solicitações** e a cotação nasce dali, já sugerindo quem vende aquelas categorias |
| Configurações com oito blocos empilhados | uma seção por vez, com faixa de navegação (o endereço guarda a seção) |
| caixas de escolha longas em todo o ERP | qualquer `<select>` com mais de 12 opções ganha campo de filtro sozinho, inclusive dentro de janelas — quem não quiser marca `data-sem-busca` |
| quantidade "1.000" para UMA unidade | `quantidadeBR` mostra a casa decimal só quando ela existe |

Também novo: **dados de exemplo** (Importações › Dados de exemplo). Traz dez
categorias, 28 insumos, cinco fornecedores, duas condições e cinco
solicitações, e remove exatamente o que trouxe. **Os insumos, as categorias e
as solicitações são os DE VERDADE**, lidos das planilhas "Cadastro de Insumos"
(aba Cadastrar) e "Solicitação de Suprimentos" (aba Pedidos) — com o material,
a especificação, a quantidade e a obra como foram pedidos. Só os fornecedores
são fictícios, com "EXEMPLO" no nome: usar os reais faria a remoção apagar, um
dia, um cadastro que a importação tornou real. A tradução do plano financeiro
antigo (nome) para o plano do ERP (código) está em `exemplo.PLANO_DA_PLANILHA`
e **precisa da conferência do dono** — errar ali joga a compra na conta de
custo errada.
Os ids do que foi criado ficam guardados em `parametros` — a remoção não usa
heurística de nome, que erraria no dia em que alguém cadastrar "Cimento CP-II"
de verdade. Se algum insumo de exemplo já tiver entrado num pedido de verdade,
a remoção é recusada **inteira**.

Ação nova de permissão: `administrar_fornecedores` (cadastrar e corrigir
fornecedor pela tela de Suprimentos). Como toda ação, pode ser dada ou tirada
pessoa a pessoa no cadastro do operador.

### Empresa (mais de um CNPJ) e o disparo da cotação — 06/09/2026

No ramo `claude/oi-vjvrn8`, **com migração 038**, ainda não publicado.

O dono avisou que a BWS vai operar com mais de um CNPJ, e que a obra tem de
dizer por qual empresa ela corre. Três coisas passam a depender disso: de qual
e-mail a cotação sai, qual logo vai no relatório e, amanhã, qual CNPJ fatura.

| O que entrou | Onde |
|---|---|
| Cadastro de **empresa**: razão social, CNPJ, inscrições, endereço, telefone, e-mail, site e **logo** | Administração › Empresas |
| **Obra ligada à empresa** — e a lista das obras que ainda não têm, na barra da esquerda, para acertar clicando | idem |
| **Conta de e-mail por empresa** (servidor, porta, usuário, senha, segurança, remetente, responder-para) e o **botão de teste** que prova que funciona | idem, aba "Conta de e-mail" |
| **Disparar a cotação**: manda a lista de itens para os fornecedores do mapa, pela conta da empresa da obra | Suprimentos › Cotações |
| **Registro de cada envio**: para quem, quando, por quem, o texto exato e o resultado | tabela `envios_email`; a coluna de cada fornecedor no mapa mostra "enviada" ou o motivo de não ter saído |

Decisões, com o motivo:

- **SMTP da própria empresa, não um serviço de envio.** Porque o fornecedor
  RESPONDE: saindo de compras@ da empresa, a conversa continua onde ela já
  acontece. O custo é que o SMTP conta menos — ver o parágrafo seguinte.
- **"Enviado" quer dizer que o servidor de saída aceitou.** Não quer dizer
  entregue, e muito menos lido. Está escrito assim na tela. Certeza de entrega
  exige serviço de envio com retorno (aviso de entregue/rejeitado), que é
  outra decisão e outro custo — fica anotado como pendência.
- **A senha vai cifrada** (`core/comum/segredos.py`), com a chave em
  `ERP_CHAVE_SEGREDOS` na Environment do Render. **Sem a chave, gravar a senha
  é recusado** com o recado do que falta; o resto do cadastro salva normal.
  Trocar a chave invalida as senhas guardadas — é redigitar.
- **Cotação com obras de empresas diferentes faz o sistema PERGUNTAR**, nunca
  sortear: por qual CNPJ a compra corre é decisão do comprador.
- **Cada fornecedor recebe só a lista de itens, nunca o mapa** — mandar o mapa
  seria entregar ao fornecedor A o preço do fornecedor B.
- **Um fornecedor fora do ar não derruba os outros**: cada um é um envio e um
  registro, e quem falhou fica com o motivo escrito e o botão de reenviar.

⚠️ **A migração 038 acrescenta uma coluna à tabela `obras`.** Enquanto o botão
"Aplicar atualizações do banco" não for apertado, TODA tela que carrega obra
quebra — e obra é carregada em quase tudo. Juntar este ramo e apertar o botão
têm de acontecer no mesmo momento, com o dono na frente do computador.

### Mapa em formato de planilha, unidade destravada e correção do pedido — 06/09/2026

No ramo `claude/oi-vjvrn8`, junto com a empresa/CNPJ e a migração 038. **Sem
migração nova** — nada aqui exige apertar o botão do banco.

O dono viu o mapa de cotação e apontou quatro coisas. As quatro estão feitas:

**1. O mapa ficou parecido com a planilha.** As informações estavam
"espatifadas" e o mapa crescia demais. Agora é tabela densa: letra menor,
linhas zebradas, a coluna do insumo acompanha a rolagem lateral e o cabeçalho
de cada fornecedor cabe em três linhas curtas. Os mesmos 28 itens que ocupavam
1.900 pixels de altura ocupam 1.120 — cabe numa tela.

**2. A situação de cada insumo aparece ao lado dele, colorida.** Quem abre um
mapa de duas semanas atrás vê, sem sair da tela, o que já virou pedido, o que
já chegou e o que ainda está em cotação. Acima da tabela há a contagem por
situação. As 15 situações são exatamente as da planilha "Registro de
Suprimentos" — foram conferidas uma a uma.

**As cores são as da planilha** — resolvido em 06/09/2026, ver a seção
seguinte.

**3. A unidade de medida deixou de ser trava do insumo.** O motivo dele:
cerâmica normalmente se compra por metro quadrado, mas um dia vem por caixa;
cimento normalmente é saco, mas um dia é bag. Agora a unidade do cadastro é
só **sugestão** ("unidade usual") — quem faz o pedido escolhe a unidade
daquele pedido, e "sem unidade usual" deixou de contar como pendência. Quem
pede continua vendo a sugestão preenchida sozinha ao escolher o insumo.

Confirmado com ele o modelo: **cadastro genérico + especificação em texto
livre** ("cimento" no catálogo, "CP-II 50kg cinza" na especificação). Evita
agigantar a base de insumos com subcategoria para cada variação.

**4. O comprador corrige o pedido da obra — com motivo obrigatório.** A obra
pede em saco o que só vem em bag, escreve o material errado, confunde um insumo
com o vizinho. Antes isso voltava por telefone e o pedido ficava parado. Agora
há um botão "corrigir" na lista de Solicitações e um lápis na linha do próprio
mapa: dá para trocar insumo, especificação, quantidade, unidade e obra.

Decisões, com o motivo:

- **O motivo é obrigatório.** Correção sem assinatura vira "eu não pedi isso"
  duas semanas depois. Fica gravado o que era, o que passou a ser, quem mudou
  e quando — na trilha de auditoria, não num registro paralelo que possa
  divergir. A linha ganha a marca "corrigido N×"; clicando, vê-se o histórico.
- **Depois que o pedido de compra sai, corrigir aqui é RECUSADO.** O
  fornecedor recebeu uma coisa; o sistema não pode passar a dizer outra. Vale
  também para item com recebimento já lançado e para item preso a um pedido em
  pé. A tela não oferece o botão nesses casos, e há teste exigindo que a tela e
  o servidor concordem sempre.
- **Trocar o material ou a unidade APAGA os preços já digitados naquela linha
  do mapa aberto**, avisando na tela. Eram preços de outra coisa; deixá-los ali
  fecharia a compra pelo preço errado. Os valores apagados ficam no registro da
  correção. Mapa já fechado é histórico e não é tocado.
- **Corrigir só a especificação não apaga preço** — ajustar a grafia não muda o
  que está sendo comprado.

**Sobre o menor preço**: já funcionava e continua — a célula do menor preço da
linha fica verde, e o rodapé diz qual fornecedor sai melhor no total (com
frete, desconto e acréscimo dentro).

### O documento que o fornecedor recebe — 06/09/2026

Ramo `claude/oi-vjvrn8`, sem migração nova.

O dono perguntou pela ESPECIFICAÇÃO no documento do fornecedor ("o mesmo nome
de insumo com um detalhe diferente já é outra coisa"), pediu para ver como o
documento está, e listou o que ele tem de levar: itens e quantidades, o local
da entrega e a forma/condição de pagamento acertada, "pra firmar a compra".

São DOIS documentos diferentes, e agora os dois saem por e-mail pela conta da
empresa da obra:

| | Cotação | Pedido de compra |
|---|---|---|
| Para quê | pedir preço | FIRMAR a compra |
| Itens com **especificação** | sim (já tinha) | sim (já tinha) |
| Quantidade e unidade | sim | sim |
| **Endereço de entrega**, por obra | **passou a ter** | sim |
| Preço unitário, frete, desconto, TOTAL | **nunca** | **passou a ter** |
| Condição de pagamento | pede que o fornecedor informe | a que foi acertada |
| Prazo | pede retorno até tal dia | precisamos em obra até tal dia |
| CNPJ e endereço da empresa | sim | sim |

Decisões, com o motivo:

- **O endereço vai na cotação** porque o frete depende da distância: pedir
  preço sem dizer onde entregar é receber um preço que muda depois. Itens de
  obras diferentes saem em blocos separados, e a numeração NÃO reinicia (o
  fornecedor cita o número do item na proposta).
- **Obra sem endereço cadastrado diz "Endereço não informado"** em vez de sair
  em branco — em branco, o motorista descobre no caminho.
- **O preço vai no pedido** e não vai na cotação. No pedido é o que impede a
  discussão de nota com valor diferente do combinado; na cotação seria
  entregar ao fornecedor A o preço do fornecedor B.
- **Só sai pedido AUTORIZADO.** Mandar um pedido que ainda está na fila é
  comprar sem alçada: o fornecedor entrega e a conta chega. A tela mostra o
  documento mesmo assim, mas o botão fica desligado com o motivo escrito.
- **O CNPJ sai pontuado** (71.000.001/0001-84). O fornecedor confere esse
  número contra o cadastro dele antes de faturar.
- **Cada envio fica registrado** com o texto exato, como o da cotação — e vale
  a mesma ressalva: "enviado" quer dizer que o servidor de saída aceitou.

A tela de Pedidos deixou de mostrar um texto para copiar e colar: agora mostra
o documento como ele vai sair, diz de qual e-mail sai e para quem, e manda.

### As cores vieram da planilha — 06/09/2026

Estavam na **formatação condicional da coluna de status**, não numa legenda:
"Registro de Suprimentos", aba **Insumos**, coluna **L**, uma regra por status.
Foram lidas do arquivo, não escolhidas.

Como foram lidas, para quem precisar repetir: a planilha principal (1,9 MB) é
grande demais para o Google exportar pelo conector. A cópia **"Registro de
Suprimentos (Natan)"** exporta, carrega a mesma formatação, e foi de lá que
saíram. Baixada como `.xlsx` e lida com `openpyxl`
(`ws.conditional_formatting`) — o texto puro do Google **não** traz cor de
célula, e foi por isso que a primeira tentativa falhou.

| Situação | Fundo | Letra |
|---|---|---|
| SOLICITAÇÃO | `#E06666` vermelho | preta |
| SALA TÉCNICA | `#999999` cinza | preta |
| COTAÇÃO | `#F1C232` ouro | preta |
| ANÁL. PROPOSTAS | `#FFE599` amarelo claro | preta |
| **AUTORIZAÇÃO** | `#FFD966` | preta |
| PEDIDO EMITIDO | `#FDFD17` amarelo forte | preta |
| ALMOXARIFADO | `#F4CCCC` rosa claro | preta |
| AGUARD. COLETA | `#CFE2F3` azul claro | preta |
| AGUARD. ENTREGA | `#6FA8DC` azul médio | preta |
| EM TRÂNSITO | `#0B5394` azul escuro | branca |
| ENTREGUE | `#073763` azul marinho | branca |
| RECEBIDO | `#6AA84F` verde | preta |
| PENDÊNCIA | `#674EA7` roxo | branca |
| CANCELADO | sem cor (a linha fica branca) | cinza, riscado |
| SUSPENSO | `#434343` cinza escuro | branca |

⚠️ **AUTORIZAÇÃO é a única inventada**: a cópia de onde as cores saíram tem 14
status e não tem esse. Ficou num amarelo entre os dois vizinhos dela no fluxo.
**Confirmar com o dono.**

Duas escolhas nossas, ditas às claras: a **letra branca** nos quatro fundos
escuros (preta ali não se lê), e **CANCELADO riscado** — a planilha deixa a
linha inteira branca, o que não cabe numa etiqueta pequena.

A cor passou a aparecer também na **lista de Solicitações** e no **filtro de
situação** da esquerda, e não só no mapa: é onde a equipe procura pelo estado
do item.

### Locações mudou de lugar, e a tela de Solicitações estava morta — 06/09/2026

Ramo `claude/oi-vjvrn8`, sem migração nova.

**Locação de equipamento saiu do Financeiro e foi para Suprimentos.** O motivo,
nas palavras do dono: quem demanda a locação é a obra, e quem atende é
suprimentos — que resolve a mesma necessidade de três formas, remanejando o que
já existe, comprando ou **locando**. O endereço antigo (`/erp/locacoes`)
continua respondendo e redireciona, porque há link salvo e favorito.

**A obrigação de pagar continua sendo do financeiro**, e agora os dois lados se
enxergam:

- o **título** ganhou um bloco "de onde veio": contrato, locadora, obra,
  competência, valor previsto — e o botão "Abrir o contrato em Suprimentos",
  que abre a ficha direto (`?contrato=N`);
- a **parcela do contrato**, depois de lançada, mostra "ver o título ›" e abre
  a ficha do título direto (`?titulo=N`).

Sem isso, a conta de aluguel que chega todo mês é uma despesa órfã: quem
confere não sabe de qual contrato é nem se o valor bate com o que está em obra.

**Defeito corrigido no caminho**: o botão "Lançar título" da locação sempre
falhava quando não havia boleto. O financeiro exige conta HOMOLOGADA escolhida
(dados bancários vivem no cadastro, nunca no lançamento) e o modal nunca
perguntava a conta. Agora o modal oferece as contas homologadas da locadora, e
recusa na tela — dizendo o que fazer — quando não há nenhuma.

### A tela de Solicitações do financeiro estava quebrada — 06/09/2026

⚠️ **Achado grave, e por acaso.** `Financeiro › Solicitações` — a tela central
de contas a pagar — não carregava: mostrava "Não foi possível carregar" e uma
lista vazia. Duas causas somadas, as duas no JavaScript da tela:

1. o estado dos filtros não tinha o conjunto `conta`, e `montarFiltros()` lia
   `F.conta.has(...)`;
2. o painel de filtro `opcoes-conta` **nunca foi escrito no HTML**, e o código
   fazia `els("opcoes-conta").innerHTML = ...`.

Os dois estouravam **dentro de um `try`**, então o `catch` engolia: nada no
console, nenhum erro de servidor, nenhum teste falhando. O filtro também
apontava para um campo `conta_obra` que a API nunca mandou — o certo é
`categoria`. Ficou corrigido e o filtro "Conta do plano" passou a existir de
verdade.

**A brecha de teste foi fechada**: `tests/test_telas_javascript.py` ganhou
`test_a_tela_nao_procura_elemento_que_nao_existe` — todo id pedido por
`els("x")` tem de aparecer como `id="x"` em algum lugar da página. Foi provado
que o teste falha com o defeito de volta e passa sem ele. É a terceira vez que
um defeito só de tela passa por todos os testes; esta classe agora tem guarda.

**Varredura**: as 26 telas do ERP foram abertas num navegador de verdade, uma a
uma, conferindo aviso de erro e erro de JavaScript. Todas carregam.

### Exportar em Excel e PDF, em toda tela — 06/09/2026

Ramo `claude/oi-vjvrn8`, sem migração nova. Sem dependência nova: `openpyxl` e
`fpdf2` já estavam no serviço.

Antes: **duas** telas do ERP inteiro exportavam, e só em CSV. Agora **doze**
telas de lista têm os botões **Excel** e **PDF**, e a próxima tela que alguém
escrever ganha os dois com uma linha.

**Como funciona, e por que assim:** o exportador **lê a tabela já desenhada na
tela**, em vez de cada tela montar a lista de novo. Assim o arquivo não pode
divergir do que a pessoa está vendo — mesmo filtro, mesma ordem, mesmas linhas,
por construção. Ficam de fora a coluna da caixinha de seleção, as colunas
marcadas `sem-exportar`, e botões/listas dentro das células ("mover para…" é
comando, não conteúdo).

Decisões, com o motivo:

- **Os filtros ligados saem impressos no cabeçalho** do arquivo, junto com
  quem exportou e quando. Relatório sem a origem é número sem procedência:
  três meses depois ninguém sabe se aquilo era de uma obra ou de todas.
- **Coluna alinhada à direita vira NÚMERO de verdade** na planilha (é a
  convenção que o ERP já usa). Texto que parece número não soma, não ordena e
  não vira tabela dinâmica — é a reclamação nº 1 de quem recebe arquivo de
  sistema.
- **"Sem valor" continua vazio, nunca zero.** Em compras, "sem preço" e
  "preço zero" são coisas diferentes.
- **O PDF deita sozinho a partir de seis colunas** — pedido do dono olhando o
  mapa de cotação com muitos fornecedores. Dá para forçar em pé ou deitado.
- **O PDF corta em 3.000 linhas e DIZ que cortou**, mandando para o Excel.
  Um PDF de vinte mil linhas ninguém abre, e a instância tem 2 GB dividida com
  outros treze módulos.

Duas armadilhas que custaram tempo e ficaram registradas no código:

1. **`fpdf2` deixa o cursor à direita depois de `multi_cell`** — o subtítulo
   saía cortado na margem e o resto do cabeçalho sumia. Resolve com
   `new_x="LMARGIN", new_y="NEXT"` em toda chamada.
2. **`"1.000"` virava 1, não mil.** Só-ponto é ambíguo. A regra passou a ser a
   MESMA da tela (`paraNumero`, em `erp_base.html`): ponto só separa milhar
   quando sobram exatamente três dígitos depois dele. Tem de ser a mesma regra
   dos dois lados, senão o arquivo sai diferente da tela.

**Telas com exportação hoje**: Solicitações (suprimentos e financeiro), Banco
de preços, Pedidos, Locações, Insumos, Fornecedores, Colaboradores, Obras,
Despesas de colaborador, Empreitas e Receber.

**Ainda não exportam**: o mapa de cotação (vai junto com os quatro relatórios
do mapa), Conciliação, Pagamentos, Confirmar e Prestação de contas.

Verificado baixando os arquivos de verdade num navegador, tela por tela, e
abrindo o `.xlsx` e o `.pdf` produzidos. 21 testes novos.

### PUBLICADO NA MAIN EM 06/09/2026 — e as migrações foram aplicadas

O dono autorizou, confirmou que não havia carga do painel nem sincronização do
Análise de SPs rodando, juntou-se na `main`, e ele apertou "Aplicar
atualizações do banco" logo depois da subida. Ele confirmou: **rodou tudo
certo**. Então, em produção agora:

- migrações **029 a 038** aplicadas (inclusive a 038, que acrescenta a coluna
  de empresa em `obras`);
- empresa por CNPJ, conta de e-mail por empresa, disparo da cotação;
- os dois documentos do fornecedor (cotação e pedido de compra);
- mapa em formato de planilha, com as cores da planilha do dono;
- unidade destravada e a correção do item pelo comprador;
- Locações dentro de Suprimentos, ligada ao financeiro nos dois sentidos;
- exportação em Excel e PDF em doze telas;
- **a tela Financeiro › Solicitações, que estava morta, voltou a funcionar.**

**Ainda não conferido pelo dono na base real** — a lista sugerida a ele foi:
Financeiro › Solicitações (a que estava quebrada), Suprimentos › Cotações (as
cores), Administração › Empresas (cadastrar a BWS) e os botões Excel/PDF em
qualquer lista.

### Os quatro relatórios do mapa, e o mapa em PDF — 06/09/2026

Ramo `claude/oi-vjvrn8`, sem migração nova. **Publicado? NÃO** — este é o
primeiro trabalho depois da publicação da noite.

São os mesmos quatro das abas **R2 a R5** da planilha "Relatório Mapa de
Cotação", que a equipe já usa para decidir a compra. Ficam no botão
"Relatórios do mapa", dentro da tela de Cotações, em quatro abas:

| Aba | Pergunta que responde |
|---|---|
| **Por item** | de quem compro cada coisa pelo menor preço? |
| **Por fornecedor** | então o que compro de cada um? — é a lista de compra |
| **Comprando tudo de um** | e se eu fechar tudo com este aqui? |
| **Comparativo** | quem sai melhor no total, com frete e desconto? |

Decisões, com o motivo:

- **Os quatro saem do MESMO mapa** (`montar_mapa`). Se cada um refizesse a
  conta, um dia dois deles dariam números diferentes para a mesma cotação — e
  aí nenhum serviria para decidir. Há teste exigindo que "por item" e "por
  fornecedor" somem igual.
- **Cada relatório diz o que NÃO está na conta.** O de menor preço avisa que o
  frete de cada fornecedor não está somado; o comparativo avisa que quem cotou
  menos itens aparece com total menor por isso, e não por ser mais barato.
  Número sem essa ressalva engana quem decide.
- **Item que ninguém cotou continua aparecendo**, marcado "não cotado".
  Sumir com ele faria o comprador esquecer de cotá-lo.
- **"Comprando tudo de um" troca de fornecedor sem sair da tela** — é
  comparando "tudo do A" com "tudo do B" que se decide — e avisa quantos itens
  aquele fornecedor deixaria de fora.
- **O mapa inteiro em PDF sai deitado** quando há mais de dois fornecedores.

Três defeitos corrigidos no caminho, os três de formatação — e os três do tipo
que ninguém percebe porque não dá erro:

1. **`numero()` recebia texto e devolvia o texto cru.** A API manda valor como
   string ("38.5000", "945.00"), e `"38.5000".toLocaleString()` devolve a
   própria string. Agora passa por `paraNumero` antes. **Isso conserta toda
   tela do ERP que formatava valor vindo da API.**
2. **No PDF, o cabeçalho não era cortado na largura da coluna** — os nomes dos
   fornecedores se sobrepunham e ficavam ilegíveis. Passou a usar o mesmo
   corte do corpo, e o cabeçalho conta pela metade no cálculo da largura, para
   um nome comprido não roubar espaço de coluna com conteúdo.
3. **Os relatórios saíam com ponto decimal e quatro casas.** Agora saem em
   português (`_dinheiro_br`, `_quantidade_br`), porque vão para o papel.

Verificado: 2.286 testes (33 novos), as quatro abas percorridas num navegador,
o comparativo e o mapa baixados em PDF e abertos, e as 26 telas varridas de
novo — todas carregam.

### Conferência mensal dos equipamentos locados — 07/09/2026

**O problema, nas palavras do dono:** "muitas vezes eles são locados e deixam
de ser utilizados, não são devolvidos". O aluguel corre, ninguém devolve, e
meses depois já se pagou mais do que custaria comprar.

O ERP **já gritava isso** — o alerta "10 meses locado, o aluguel já paga a
compra" existe desde o começo. O que faltava era **alguém ser obrigado a
responder**. É só isso que esta entrega acrescenta, e é por isso que ela é
pequena: uma pergunta por mês, com nome de quem responde.

**Como ficou.** Todo mês abre uma conferência por contrato ativo. Quem
responde é o **administrativo da obra** (se a obra não tiver um, cai no
responsável do contrato). Para cada equipamento: está na obra? está sendo
usado? **onde está e para quê?** e **quando volta**. Marcar "devolver" ou
"remanejar" **faz a movimentação de verdade** no contrato — conferência que
registra intenção e não faz nada é papel, e papel não devolve equipamento.

**A previsão de devolução nasce na contratação**, por equipamento, não por
contrato: a betoneira fica a obra toda, as escoras eram para três semanas — e
é a escora que se esquece na obra. Vencido o prazo, o contrato passa a
mostrar, com o valor: "devolução prevista para 18/08/2026, 20 dias atrás —
cerca de R$ 576,00 de aluguel depois do combinado".

**Quatro decisões, todas do dono, escritas para ninguém desfazer sem saber:**

1. **Mensal**, e quem responde é o administrativo da obra.
2. **Sem foto.** Sem etiqueta no equipamento a foto prova pouco (metadado se
   falsifica e o WhatsApp apaga o que existe) e daria trabalho a todo mundo
   todo mês. O dono recusou etiqueta e foto.
3. **A conferência NÃO bloqueia o pagamento** do aluguel — bloquear trocaria
   equipamento esquecido por multa e briga com a locadora. Ela vira pendência
   e **avisa quem vai lançar a parcela**: "a conferência de 09/2026 está
   aberta com Fulano e ainda não foi respondida", junto do número do título.
4. Passados **10 dias** do fim do mês, a pendência deixa de ser lembrete e
   entra na lista de cobrança — é a base do agente de WhatsApp, que ainda não
   existe.

**Migração 039** — `devolucao_prevista` e `devolucao_prevista_original` por
item (a original fica para se saber que a data foi adiada, e por quem), mais
as tabelas da conferência, com uma por contrato por mês garantida pelo banco.

**Percorrido no navegador**, não só testado: a conferência recusa quem não diz
se o equipamento está na obra, recusa quem não diz onde ele está, mostra o
campo "para qual obra" só quando se escolhe remanejar, e ao confirmar o
gerador saiu do contrato de verdade — com o movimento gravado citando a
conferência. Depois disso a ficha passou a dizer "conferidos pela obra em…".

### Três defeitos achados no caminho, e as varreduras que fecham a classe

Nenhum dos três tem a ver com a conferência; apareceram porque a tela foi
aberta num navegador de verdade. Os três são do mesmo tipo: **silêncio**.

1. **A tela de Locações abria vazia.** A listagem citava um nome de variável
   que não existe ali. Python só reclama disso na hora em que a linha roda —
   e a suíte não rodava aquela linha. Corrigido, e agora
   `tests/test_nomes_indefinidos.py` lê **todo** o código Python das
   aplicações e recusa qualquer nome que o Python não vá encontrar. Passou nos
   266 arquivos; provei que ele pega o defeito reintroduzindo-o.
2. **O campo "conta bancária" do cadastro da obra abria sempre vazio.** A tela
   pedia um endereço que nunca existiu no servidor, e a chamada morria dentro
   de um `try` — sem erro na tela, sem nada vermelho. Criada a rota (leitura
   só, `ver_erp`; criar conta continua exigindo `configurar`), e agora
   `tests/test_telas_chamam_rota_que_existe.py` confere **cada endereço que
   cada tela pede** contra as rotas que o Flask registrou de verdade.
3. **Dinheiro saía em inglês nos alertas da locação** ("R$ 576.00"). Os
   formatadores em português que já existiam no relatório de cotação foram
   para `core/comum/formato.py` e agora servem aos dois — copiar teria feito
   as duas cópias divergirem. De quebra, "Lancada" e "Pendencia" voltaram a
   ter acento em toda tela que usa o rótulo genérico.

Essa é a quinta e a sexta tela morta encontradas assim. O padrão já é claro:
**a suíte prova regra, o navegador prova tela**. As duas varreduras novas são
baratas e rodam junto com o resto.

### O celular, etapa 1: tudo cabe, e o computador não mudou — 07/09/2026

O dono decidiu adaptar o sistema ao celular, e pôs uma condição: *"eu só não
queria mudar o que já está para o computador"*. Esta entrega faz as duas
coisas, e a segunda foi provada, não prometida.

**O que estava errado, medido numa tela de 390px** (e não era o que eu tinha
dito antes — a primeira medição pegou a tela de login 14 vezes seguidas,
porque o Postgres do contêiner tinha caído; a lição está nas "coisas pequenas
que mordem"):

1. **A página inteira tinha 687px de largura.** A culpa era da barra de cima:
   os cinco módulos em linha não cabem, e empurravam o documento todo. Quem
   usava via a página andar de lado e a marca "BWS" cortada.
2. **Os filtros ocupavam 857px de altura ANTES do conteúdo.** Abrir Títulos no
   telefone mostrava uma tela e meia de caixinhas antes da primeira linha da
   lista.
3. **13 de 19 botões tinham menos de 40px** — abaixo do que o dedo acerta.
4. Um nome de fornecedor longo quebrava em quatro linhas e a linha da tabela
   ficava com altura de parágrafo: três títulos e a tela acabava.

**O que ficou.** A barra de cima rola dentro dela mesma. Os filtros viraram
uma **gaveta** que abre por cima, com um botão flutuante que mostra quantos
filtros estão ligados ("Filtros 5") — conteúdo primeiro, filtro quando se
quiser. Alvos de 44px. Cartões de número apertados, dois por linha. Célula em
uma linha só, cortada com reticências, já que a tabela rola de lado — e a
**primeira coluna fica fixa**, para não se perder de qual linha é o número.

**A prova de que o computador não mudou.** Fotografei as 14 telas a 1500px
ANTES e DEPOIS da mudança e comparei os arquivos: **13 são byte a byte
idênticas**. A décima quarta difere em 6 pixels, na curva do canto do campo de
busca, cada um com diferença de 1 em 255 num único canal de cor — invisível, e
nenhum layout se moveu (a imagem tem exatamente a mesma altura). O mesmo
método serve para a próxima vez: guardar a mudança de lado (`git stash`),
fotografar, trazer de volta, fotografar e comparar.

Tudo o que este trabalho acrescentou vive dentro de `@media (max-width:720px)`
no `erp.css`. A única regra fora do `@media` é a que mantém o botão da gaveta
invisível no computador — e é ela que garante que ele nunca vaze para lá.

**Verificado:** 14 telas percorridas no celular, abrindo e fechando a gaveta em
cada uma, sem um único erro de JavaScript; nenhuma tela estoura a largura; e
no computador o botão não aparece em nenhuma delas.

**Ainda NÃO é a etapa 2.** Tabela com 10 colunas continua rolando de lado no
telefone: dá para consultar, não é confortável para operar. As telas que se
opera de pé — responder a conferência da locação, autorizar, consultar um
título — ganham desenho próprio na etapa seguinte.

### O celular, etapa 2: a conferência da locação — 07/09/2026

É a primeira tela desenhada para ser usada de pé, e foi escolhida a dedo: é a
única que **gente de obra** vai abrir todo mês, a partir de um link que chega
por WhatsApp. Se ela não funcionar no telefone, o agente de cobrança não tem
para onde mandar ninguém.

**Dois problemas, e o segundo não era do celular:**

1. **Os campos vazavam pela direita.** A causa era sutil e vale registrar: a
   grade dos formulários usa `auto-fit`, que sozinha já daria uma coluna numa
   tela estreita — mas alguns campos trazem `grid-column: span 3` escrito no
   HTML, e **um item que ocupa três colunas obriga a grade a ter três**. No
   telefone isso espremia "Está sendo usado?" em 42px e cortava "Motivo" na
   borda. Corrigido para TODO formulário do ERP, não só este.
2. **Seis perguntas por equipamento, todas de uma vez** — 3.100px de rolagem
   para cinco equipamentos. Agora só a primeira pergunta aparece ("está na
   obra?"), e as outras nascem conforme a resposta: quem diz "não está mais
   aqui" recebe só o campo de motivo, que é justamente o que a regra exige.
   Caiu para 1.270px, e **no computador a conferência inteira passou a caber
   numa tela só** — cinco equipamentos e o botão de confirmar sem rolar.

Nada foi escondido do resultado: o que está visível é exatamente o que
`responder`, em `core/locacoes_conferencia.py`, vai exigir. Se as duas regras
divergirem, a pessoa preenche o que a tela pede e leva recusa do servidor.

**Percorrido de ponta a ponta num telefone de 390px:** recusa com o nome do
equipamento quando falta resposta, cinco respostas gravadas (quatro ficam, um
saiu da obra com motivo), e o banco conferido depois — `onde_esta` só nos que
ficaram, motivo só no que saiu. Sem um erro de JavaScript.

### O agente que vai atrás de quem tem pendência — 07/09/2026

Pedido do dono: *"vamos pensar numa forma de como a gente teria uma espécie de
agente fazendo esse acompanhamento, cobrando, mandando mensagens… esse agente
ele vai servir pro sistema como um todo"*.

**Ele SÓ AVISA.** Manda texto e um link; não escuta, não interpreta resposta,
não decide nada. A resposta é dada no sistema, completa. As razões estão no
item 13 do pendente.

**A escada, aprovada pelo dono:** dia 5 lembra, dia 10 cobra, dia 15 a lista de
quem não respondeu **sai das mãos de quem não respondeu** e sobe para o dono e
o financeiro. Cada degrau vai UMA vez — quem está com 40 dias está em
ESCALADA, não em LEMBRETE de novo.

**Serve ao sistema todo desde o primeiro dia.** Assunto novo é uma função que
devolve pendências no formato `Pendencia` e se registra em `ASSUNTOS` — sem
tocar no agente. Hoje há um assunto: a conferência de locação, escolhida a dedo
para ser a primeira porque é a única cujo responsável **não fica na frente de
um computador** — é o administrativo da obra, no canteiro. Se funciona aqui,
funciona em qualquer lugar.

**Não vira spam, e a trava tem duas camadas.** O código pergunta antes ("já
mandei este degrau para esta pessoa?") e o banco recusa a repetição por chave
única — a segunda existe porque a primeira sozinha não segura duas execuções ao
mesmo tempo. Ambas provadas com Postgres de verdade.

**Como se roda:** `POST /erp/api/agente/rodar`, sem sessão, protegida por
`ERP_AGENTE_SECRET` no corpo — o mesmo padrão dos outros módulos do monorepo.
Ela **recusa também quando o segredo não está configurado no ambiente**, em vez
de liberar; há teste para as duas recusas, e a rota entrou na lista consciente
de rotas públicas (a suíte acusou quando ela nasceu, como devia).

**`simular: true`** monta tudo e não manda nem grava. É como se confere o que o
agente FARIA hoje — e foi assim que ele foi verificado antes de existir em
produção.

**O que a mensagem leva, e o que não leva.** Leva o que falta e o link. **Não
leva valor de contrato nem dado bancário**, e há teste que quebra se alguém
puser: mensagem de WhatsApp é lida em ônibus, em obra, e por quem pega o
telefone emprestado.

**O link abre a conferência sozinho.** `?conferencia=N` na tela de Locações —
percorrido num telefone de 390px: o endereço da mensagem abre o diálogo certo,
com os cinco equipamentos prontos para responder. Sem isso o agente mandaria
gente para uma lista, e quem está no canteiro não procura nada.

**O que ele já falou fica registrado** (migração 040), com nome, hora, canal e
se saiu mesmo. É a resposta para "o Ruan foi cobrado?" — a pergunta que aparece
quando a obra diz que não sabia.

### Compras: o que cada obra comprou, aberto até o insumo — 07/09/2026

O dono pediu duas coisas na mesma frase: *"de pedido a gente vai conseguir ver
tudo de uma obra"* e *"eu quero ver tudo que foi de cimento, tudo que foi de
cerâmica"*.

São **a mesma consulta lida dos dois lados**, e por isso é UMA tela, não duas —
duas divergiriam no primeiro mês. Um seletor troca o agrupamento: por obra
(abre até o insumo), por insumo (abre até a obra), por categoria ou por
fornecedor. Cada linha abre e fecha: fechada responde "quanto", aberta responde
"em quê", que é a pergunta seguinte, sempre.

**Só conta pedido AUTORIZADO.** Pedido esperando autorização ainda pode não
acontecer, e recusado não aconteceu — somá-los faria o relatório dizer que a
obra gastou o que não gastou. Quem quiser ver o que está em andamento marca a
caixa, e a tela explica por que ela existe.

**A obra e o insumo não vêm do pedido**, vêm da SOLICITAÇÃO que originou cada
linha dele — e a obra é do ITEM, não da solicitação, porque uma mesma
solicitação pede material para obras diferentes.

**O escopo entra na consulta.** Quem enxerga só as obras dele não descobre o
gasto das outras por um relatório — há teste com banco de verdade provando que
nem pelo caminho do insumo ele alcança a obra alheia. Um relatório não pode ser
a porta dos fundos por onde se vê o que a tela de títulos esconde.

Exporta em Excel e PDF **linha a linha**, não a árvore: planilha com hierarquia
não se soma nem se filtra.

### O sexto defeito silencioso — e a varredura que fecha a classe

A tela nasceu com `{% block script %}` em vez de `{% block scripts %}`. Uma
letra. A base não conhece `script`, então o conteúdo foi **jogado fora ao
montar a página**: nenhum erro, nenhum aviso, a tela abriu bonita e a tabela
ficaria vazia para sempre.

Agora `tests/test_telas_blocos.py` lê cada tela, vê de qual página ela herda, e
cobra que todo bloco declarado exista lá — dizendo, quando falha, quais a base
oferece. Provado contra o defeito real.

**E um erro meu do mesmo tipo, no mesmo dia:** os filtros de obra e fornecedor
abriam vazios porque eu li `d.obras` quando a API embrulha tudo em
`d.dados.obras` — e um `try` mudo engoliu a falha. É a terceira vez que um
`try` silencioso esconde um filtro vazio neste ERP. A tela agora **avisa** se
não conseguir carregar uma lista, em vez de fingir que está tudo bem.

### Despesa com colaborador: percorrida de ponta a ponta — 07/09/2026

O dono pediu o mesmo método que usamos em Suprimentos: **testar com dado de
verdade e mostrar as telas**. Foi o que se fez — não com teste, com o
navegador, uma pessoa de cada vez, cada etapa com quem realmente a executa.

**O caminho inteiro, e funciona:**

1. **A obra lança.** Três colaboradores, uma diária e duas produções. A diária
   é calculada do cadastro: 12 dias × R$ 180 = R$ 2.160,00, sem ninguém
   digitar o total.
2. **A conferência antes de enviar** pegou o que devia, com dado real:
   - *"Diária exige a quantidade de dias"* — bloqueio, quando faltou;
   - *"Maria do Socorro Alves não tem dados de pagamento no cadastro"* —
     atenção, e ela **acompanha até a planilha de pagamento**, para quem paga
     ver o problema antes de tentar pagar;
   - *"Antônio já recebeu diárias de R$ 2.160,00 na DC00001 — mesmo valor em
     menos de 10 dias"* — a crítica de repetição, que é a que mais pega erro,
     disparando sozinha na segunda tentativa.
3. **Supervisor aprova** → "agora aguarda departamento pessoal".
4. **Departamento pessoal aprova** → "agora aguarda diretor financeiro". Serviu
   também para confirmar na prática a mudança de visão do DP feita hoje: a
   despesa apareceu para ele, lançada por outra pessoa.
5. **Diretor aprova** → "pronta para faturar".
6. **Vira título:** "Título 000003 gerado (R$ 3.630,50, 1 obra(s))", com o
   aviso de atualizar o QR Code do BeeVale.

As aprovações ficam na ficha com **nome e hora de cada um** — supervisor, DP e
diretor.

**Um defeito de português corrigido:** o meio de pagamento aparecia cru na
tela, "A_DEFINIR", em dois lugares. Agora lê "A definir".

**O que ficou anotado como incômodo, não como defeito:** o envio pede
confirmação por uma caixa do navegador ("Há 1 ponto de atenção. Enviar mesmo
assim?"). Funciona, mas destoa do resto do ERP, que pergunta dentro da própria
tela. Vale trocar quando se mexer nessa tela de novo.

### Quem responde pela obra passa a ser ESCRITO — 07/09/2026

Decisão do dono: *"no cadastro da obra, a gente vai associar uma das pessoas,
um dos operadores, pra responder por aquela obra… e se por acaso tiverem dois,
a gente cadastrar dois, permitir também, os dois recebem"*. E marcável **pelos
dois lados**, *"porque facilita o manuseio do sistema"*.

**Um defeito antigo que nunca deu erro.** Até aqui o sistema ADIVINHAVA quem
responde: procurava, na obra, campos chamados `administrativo_id`,
`responsavel_id` e `encarregado_id` — **que não existem**. Como a busca era
tolerante, nunca houve erro: caía sempre no responsável do CONTRATO. Ou seja, a
cobrança ia para a pessoa errada, em silêncio, para sempre. É o tipo de defeito
que só aparece quando alguém vai conferir de onde veio o nome.

**O que ficou.** A ligação operador↔obra, que já existia para o escopo de
visão, ganhou uma marca: *esta pessoa responde por esta obra*. Aceita mais de
uma; todas recebem a cobrança do agente.

**A armadilha que isso destapou, e o que se fez com ela.** A tela do operador
APAGAVA todos os vínculos dele e recriava a cada salvamento. Marcar o
responsável na tela da obra e depois salvar o operador **apagaria a marca**, sem
erro e sem aviso. Por isso as duas telas passam por **uma função só**
(`core/cadastros/vinculos.py`), pelo mesmo motivo de `aplicar_escopo` ser único:
regra em dois lugares um dia discorda. Há teste com banco de verdade que
reproduz exatamente essa sequência.

**Duas regras de bom senso, escritas:** marcar alguém como responsável **dá a
ele acesso à obra** (quem responde precisa conseguir abrir o que foi cobrado); e
desmarcar **tira a responsabilidade, não o acesso** — tirar visão é outra
decisão e não pode acontecer de raspão.

**Percorrido no navegador, dos dois lados:** marquei duas pessoas pela tela da
obra, a mesma marca apareceu no cadastro do operador, e o agente passou a mandar
o lembrete **para as duas** — quando antes mandava para quem o contrato dizia.

### O plano de contas volta a falar a língua da BWS — 07/09/2026

O dono conferiu o de-para e recusou: *"você misturou algumas contas e renomeou
sem necessidade… as nomenclaturas estão de acordo com a nossa realidade, as
pessoas que lançam já estão acostumadas com elas"*.

Ele está certo, e o erro tem nome: eu tratei o plano de contas como problema de
organização, quando ele é **problema de quem digita**. Quem lança escolhe a
conta numa lista, sob pressão, dezenas de vezes por dia. Se o nome não é o que
essa pessoa tem na cabeça, ela erra — e **erro de conta não aparece na tela,
aparece na contabilidade meses depois**.

**O que estava errado.** As 32 categorias da planilha tinham virado 21 contas:
argamassa junto com cimento, vidro junto com esquadria, gás junto com
hidráulica, estrutura metálica junto com pré-moldado, cabeamento junto com
elétrico, bancada junto com louça. E quase todas renomeadas.

**O que ficou.** As 32 categorias, uma conta cada, **com o nome exato da
planilha**. Sete contas novas (3.1.21 a 3.1.27) para desfazer as junções,
numeradas no fim da sequência de propósito: os títulos já lançados apontam para
os números antigos, e renumerar quebraria o vínculo deles. Nada foi apagado.

**Como se aplica:** botão "Aplicar plano de contas" na tela de Configurações. É
idempotente — rodei duas vezes, a segunda não mexeu em nada — e **contas que
alguém renomeou à mão no ERP são preservadas** (só o que ainda era texto da BWS
foi trocado).

**Regra que fica escrita no código:** nome que existe na planilha da BWS não se
junta com outro e não se "melhora".

### Fundo fixo é dedutível, ponto final — 07/09/2026

Palavras do dono: *"fundo fixo é dedutível, ponto final. Não é só nota que é
dedutiva"*.

**O comportamento do sistema já estava certo** — o tipo T10 nasce dedutível, e
as contas 3.4.08 e 5.3.10 também. O que contradizia era o **texto de orientação
na tela**: "itens sem comprovante = indedutíveis". Era regra inventada aqui
dentro, não regra da empresa, e texto na tela é o que a pessoa lê. Corrigido: a
falta de comprovante é problema de prestação de contas, não de dedutibilidade.

**O que ele ainda quer, e está na fila:** uma tela ou relatório mostrando o que
está sendo dedutível e o que não. E ele deu o critério de onde: *"vai depender
de onde eu estou olhando — se estou analisando notas, vejo lá; se estou pelos
títulos, tenho um título de fundo fixo, vejo por lá também"*. Ou seja, a
dedutibilidade tem de aparecer **nos dois lugares**, não numa tela separada.

### A trava contra baixar o mesmo pagamento duas vezes — 08/09/2026

Pedido do dono, com estas palavras: *"acho que eu tentei colocar alguma trava
na aplicação, não sei se funciona, mas o sistema realmente não pode baixar duas
vezes, precisa barrar"*.

**Ele estava certo em desconfiar.** No ERP não havia trava nenhuma: o mesmo
comprovante processado de novo dava outra baixa, calado. E no `baixabradesco`,
onde havia, a trava tinha cinco buracos — o pior deles: qualquer erro ao ler a
lista de comprovantes já vistos devolvia lista vazia, e o lote inteiro passava
como novo. **Trava que falha liberando é pior que trava nenhuma**, porque dá
confiança falsa.

A trava nova (migração **042**) mora **no banco**, em restrição única, e em
dois níveis:

1. **O arquivo, pelo conteúdo** — e o nome do arquivo NÃO entra na conta.
   "comprovante.pdf" e "comprovante (1).pdf" são o mesmo documento; renomear
   acontece o tempo todo, e era assim que a trava antiga era furada.
2. **O pagamento** — a mesma parcela, com o mesmo valor, no mesmo dia. É o que
   pega o PDF **regerado** pelo banco: bytes diferentes, pagamento igual.

Três decisões que valem ser lembradas:

- **A conferência vem ANTES da leitura por IA.** Comprovante repetido não
  chega a gastar inteligência artificial — o resultado seria jogado fora.
- **O registro é gravado ANTES da baixa**, na mesma transação. Se a restrição
  recusar, a baixa não chega a ser tentada. A trava é o portão, não o aviso
  depois do fato.
- **Falha fechando.** Não conseguiu registrar, não baixa. Baixa que não
  aconteceu é aborrecimento; baixa em dobro é dinheiro saindo duas vezes.

**Pagamento parcial continua possível**: a mesma parcela aceita outra baixa em
outro dia, ou com outro valor. O que a trava barra é a repetição idêntica. E
quando o pagamento foi mesmo em dobro de verdade, a mensagem diz o caminho:
registrar pela tela de pagamentos, com justificativa.

**Provado de ponta a ponta**, com banco de verdade e o mesmo comprovante
entrando quatro vezes por portas diferentes (tela, e-mail, Make):

| tentativa | o que era | resultado |
|---|---|---|
| 1ª | comprovante.pdf, pela tela | **baixou** |
| 2ª | o mesmo arquivo, renomeado, por e-mail | **barrado** (trava do arquivo) |
| 3ª | PDF regerado, pelo Make | sem título: a parcela já estava paga |
| 4ª | PDF regerado, com a parcela reaberta de propósito | **barrado** (trava do pagamento) |

O título terminou com **uma** baixa. A 4ª existe porque é o único jeito de
exercitar a segunda trava: com a parcela paga, a primeira defesa já resolve.

Fica registrado também **tudo que foi lido e não virou baixa** (ilegível, sem
título, precisa confirmar, é tarifa) — assim o mesmo arquivo não é lido duas
vezes nem gasta IA duas vezes, e há onde olhar quando alguém pergunta "o que
aconteceu com aquele comprovante que mandei?".

### Velocidade: o que cresce e o que não cresce — 08/09/2026

O dono perguntou, e a pergunta é boa: *"e quando essa base de dados for
crescendo? Como é que é a estratégia de manter isso rápido?"*. Ele estava
comparando com o Análise de SPs, que anda devagar, e supôs que fosse por ler
uma planilha de 59 mil linhas.

**Não é a planilha, e não é o volume.** O Análise de SPs já lê Postgres, com
índice nas colunas por onde as telas filtram; o ERP tem 73 índices nas dele. 59
mil linhas é POUCO para um banco. A prova de que o gargalo é outro veio dele
mesmo: *"fui fazer um processamento volumoso numa outra aplicação e ficou bem
lento o Análise de SPs"*. Se fosse volume de dados, mexer em outra aplicação
não mudaria nada.

**O gargalo é a máquina compartilhada**: 18 módulos num processo só, 2 GB, e
`--workers 1` obrigatório por causa do estado em memória do `chatbot`. Um
trabalho pesado toma a fila e todo mundo espera.

O que NÃO fica lento crescendo (tem índice): abrir tela filtrada, procurar SP,
abrir título, listar vencimentos, lançar, aprovar, baixar.

O que VAI pesar, em ordem de chegada:
1. **Os anexos dentro do banco** — o que mais cresce em tamanho. Virou item de
   fila por decisão do dono (ver `ROTEIRO.md`).
2. **Os números do topo das telas**, que somam tudo.
3. **A busca "contém" em texto**, que índice comum não acelera.
4. **As listas param em 500 registros e não têm próxima página** — hoje isso é
   o que as mantém rápidas; um dia vira "não alcanço o que é antigo".

**A ordem para resolver, do barato ao caro**, registrada para não se inverter:
separar o trabalho pesado das telas (custo zero) → aumentar o plano do Render
(um botão) → tirar a trava do processo único (senão o plano maior rende pouco)
→ anexos para fora do banco → totais pré-calculados → serviço separado só do
ERP.

**Notícia que muda o horizonte, dada pelo dono no mesmo dia:** *"todas as
outras aplicações vão deixar de ser necessárias depois que o ERP estiver 100%"*.
Ou seja, o problema de vizinhança se resolve sozinho por encolhimento — mas
**dentro do ERP** continuará havendo trabalho pesado (leitura por IA,
importação, relatório, e o assistente virtual que ele quer), e esse é o que
precisa sair da frente das telas.

⚠️ **Não foi medido em produção** — daqui não há acesso a ela. O que foi lido
foi o código e a estrutura do banco. A tela de saúde do sistema (na fila)
existe justamente para trocar palpite por número.

### Os documentos podem morar no Google Drive — 09/09/2026

Decisão do dono, com a conta dele: o plano de banco é de 2 GB e a empresa já
paga 2 TB de Drive no Workspace. Anexo — comprovante, nota, foto de medição —
é o que mais cresce dentro do banco e o que menos precisa estar lá.

**Nada muda para quem usa.** O documento continua sendo aberto pelo endereço do
ERP, que confere permissão e escopo antes de entregar. **O link do Drive nunca
vai para a tela** — se fosse, qualquer um com o endereço abriria holerite e
comprovante bancário sem passar por login. Foi por isso que o jeito do
`emissaonf` (que marca os PDFs como "qualquer pessoa com o link pode ver") NÃO
foi copiado: dali se reaproveita a mecânica, não a permissão.

**Vem desligado.** Em Configurações › "Onde ficam os documentos" o dono cola o
endereço da pasta (pode ser o endereço inteiro, o sistema extrai o código),
testa — o botão escreve um arquivo, lê de volta e apaga, que é a única prova
que vale —, e só então liga. Sem pasta configurada, tudo segue como sempre foi.

**Duas etapas, nunca na mesma transação:** primeiro o documento NOVO passa a ir
para o Drive; depois o botão "mover documentos antigos" leva os antigos em
lotes de 25, e **confere cada cópia no Drive antes de apagar do banco**. Cópia
que não confere não apaga nada e ainda remove o arquivo ruim de lá.

**Falha guardando, não perdendo.** Se o Drive estiver fora do ar na hora de
anexar, o documento é guardado no banco assim mesmo. Perder o comprovante que a
pessoa acabou de anexar seria o pior desfecho; ocupar um pouco de banco é o
menor dos males, e o trabalho de mudança leva esse anexo depois.

**O banco garante que o documento existe em algum lugar** (migração 043): anexo
marcado como do Drive tem de ter o identificador do arquivo; anexo do banco tem
de ter os bytes. Sem essa restrição, um defeito de código produziria anexo que
não está em lugar nenhum — e ninguém descobriria antes de precisar dele.

Provado com banco de verdade e um Drive dublado (13 casos): desligado não toca
no Drive; ligado tira os bytes do banco; a leitura funciona pelos dois
caminhos; Drive fora do ar guarda no banco; cópia que não confere não apaga;
apagar o anexo apaga o arquivo lá; e o endereço colado da barra do navegador
vira o código certo.

⚠️ **Não foi testado contra o Google de verdade** — depende da pasta que o dono
está criando. O botão "Testar a pasta" existe exatamente para isso, e é o
primeiro passo quando a pasta existir.

### A dedutibilidade aparece na tela dos títulos — 09/09/2026

Pedido do dono: *"se estou analisando notas, vejo lá; se estou pelos títulos,
tenho um título de fundo fixo, vejo por lá também"*. Este é o lado dos títulos:
filtro próprio na barra da esquerda (Dedutível, Parcial, Indedutível, Pendente,
com a contagem de cada um) e dois quadrinhos no topo — quanto do que está em
tela é dedutível, e quanto ainda está por decidir. Clicar no quadrinho filtra a
lista.

O lado das NOTAS depende da tela de notas, que ainda não existe — ela é a do
cruzamento, próxima da fila. A dedutibilidade entra nela quando ela nascer.

### A tela do cruzamento de notas fiscais — 09/09/2026

A peça que o dono chamou de vital, ditada por ele em 07/09/2026 e especificada
em `NOTAS_FISCAIS.md`. Está em Financeiro › **Notas fiscais**.

**O que ela responde.** Para cada nota emitida contra um CNPJ da BWS: de que
pedido ela é, qual título paga ela, se ela está dentro de uma prestação de
fundo fixo — ou se não cruza com nada. E o caminho inverso: quanto de cada
pedido já veio em nota e quanto falta.

**Um pedido tem VÁRIAS notas, e isso está no desenho, não no remendo.**
Palavras dele: *"comprei dez carradas de brita e o fornecedor emite a nota por
carrada; aquele pedido não se fecha instantaneamente"*. Por isso a ligação com
o pedido mora do lado da NOTA, e o pedido só aparece como fechado quando as
notas somam o valor dele. Nada aqui casa por valor exato com o pedido — casar
assim perderia justamente esse caso.

**O sistema propõe, a pessoa decide** — o modelo é o da conciliação bancária,
que ele mesmo citou. O botão "cruzar o que der sozinho" casa **só o que é
prova**: a chave de acesso da nota. Mesmo credor e mesmo valor é pista, e pista
vira proposta na tela, nunca casamento automático — casar por indício é errar
igual à conferência manual, só que mais rápido e em silêncio.

**A trava contra contar a mesma despesa duas vezes.** É o ponto mais perigoso
do desenho todo, porque o erro não aparece na tela: aparece na contabilidade,
meses depois. Uma nota não pode ter título próprio E estar dentro de uma
prestação de fundo fixo. O sistema recusa a segunda ligação explicando o
porquê, a tela mostra um alerta vermelho se isso existir por outro caminho, e a
mesma linha de prestação não recebe duas notas (índice único no banco).

**Fundo fixo é dedutível, ponto final** — a tela diz por qual porta cada
despesa entrou (nota ou fundo fixo) e soma quanto do que está em tela é
dedutível. É o lado das NOTAS do pedido dele de ver dedutibilidade "por onde eu
estiver olhando"; o lado dos títulos foi entregue no mesmo dia.

**As notas entram por importação de XML** — soltos ou num .zip, que é o que o
serviço de monitoramento já baixa. A recomendação registrada era essa: o valor
está no cruzamento, não no download, e trocar a fonte depois (SEFAZ direto) não
refaz o cruzamento — vai desembocar no mesmo lugar. Certificado digital,
sequência da SEFAZ e manifestação do destinatário continuam sem decisão dele e
não bloqueiam nada.

**Quem confere:** ficou em aberto na especificação; a resposta prática adotada
foi "os dois, na mesma tela" — o financeiro por cargo, e quem compra pela
implicação de permissão, porque é o comprador que sabe de que pedido a nota é.
Ver é largo (inclui gestor e supervisor); cruzar é estreito.

**Ignorar uma nota exige motivo escrito**, e o banco recusa sem ele. Seis meses
depois ninguém lembra por que aquela nota foi posta de lado — e é exatamente o
que o fisco pergunta.

Provado com banco de verdade (21 casos) e com a tela aberta no navegador: as
três carradas somando até o pedido fechar, a recusa da dupla contagem nos dois
sentidos, a chave casando sozinha, o indício NÃO casando sozinho, e as
restrições do banco recusando situação inventada e "ignorada" sem motivo.

**Efeito colateral consertado no mesmo dia:** o Financeiro passou a ter onze
abas e a última sumia na borda da tela, sem nada indicando que havia mais.
Agora a barra mostra sombra nas pontas quando há o que rolar, rola com a roda
do mouse e traz a aba ativa para um lugar legível. Conferido nas 19 telas.

### O arquivo de documentos da empresa — 09/09/2026

Pedido do dono no mesmo dia, especificado inteiro em `GESTAO_DOCUMENTOS.md`
antes de qualquer código. Nas palavras dele: *"um ambiente onde eu pudesse
simplesmente jogar esse documento, ele fosse interpretado, lido, e a partir
dali categorizado, renomeado e salvo"*.

Entregue nesta rodada: **o catálogo, o arquivamento com nome padronizado e a
tela** (Administração › Arquivo, migração 045). A leitura por IA, os blocos e
os avisos de vencimento são os passos seguintes, e cada um entra sem refazer o
que já existe.

**A taxonomia.** Antes de "que tipo é" vem "a quem pertence": empresa, obra,
pessoa, parceiro ou lançamento — **exatamente um**, garantido pelo banco.
Documento pendurado em dois donos não é achado por nenhum dos dois. A
competência (o mês) não é dono, é recorte — é ela que vai fazer o compilado
fiscal funcionar.

**59 tipos** no catálogo inicial, em sete grupos: cadastrais, certidões,
licitação, obra, fiscal/trabalhista, pessoas e financeiro. O catálogo é
editável pela tela e **nunca apaga tipo** — tipo removido deixaria documento
órfão, que é o problema que o módulo veio resolver. Tipo que não serve mais é
desativado.

**A nomenclatura:** `TIPO_DONO[_REFERENCIA]_DATA.ext`, sem acento e sem espaço.
Não é preciosismo — portal de licitação e sistema de prefeitura ainda engasgam
com acento, e o arquivo volta corrompido ou é recusado. Documento que vence
leva a validade no nome (`val-2026-10-02`): bater o olho e saber até quando
vale é metade do problema. **O nome original nunca se perde** e a tela mostra
os dois — renomear é conveniência, não amnésia.

**Quem vê o quê não depende da permissão de rota**, e sim do SIGILO do tipo
(aberto, restrito, pessoal) mais o escopo por obra. Administrativo de obra vê
certidão e não vê folha de pagamento, mesmo tendo a ação marcada.

**O que o sistema recusa, e por quê:** documento que vence sem validade (senão
nunca avisa), documento de competência sem o mês, e documento sem dono ou com
dois. A competência é sempre gravada no dia 1 — sem isso "agosto" viraria
trinta e um valores diferentes e o compilado nunca fecharia.

**A busca já olha o texto de dentro do documento.** O campo existe e a consulta
usa; o texto passa a ser preenchido quando a leitura por IA entrar. Foi feito
nesta ordem de propósito: guardar o texto no momento da leitura é quase de
graça, e reprocessar dez mil arquivos depois é que sairia caro.

Provado com banco de verdade (27 casos) e com a tela aberta no navegador:
guardar uma certidão que vence em sete dias, ver o nome sair
`CRF-FGTS_BWS_val-…`, e o quadrinho "Vencendo" acender.

### O ERP vai capturar as notas sozinho — 09/09/2026

Resposta do dono a duas perguntas que estavam abertas: *"quanto à captura de
notas, basta avisar; a ideia é deixar de usar o FSist e fazer o trabalho
autônomo integrado"*.

1. **O ERP avisa, não manifesta.** Dizer ao fisco "ciente" ou "desconheço"
   continua sendo ato humano. É a escolha certa: manifestação tem consequência
   e prazo, e robô que manifesta sozinho erra em nome da empresa.
2. **O FSist sai, e a captura passa a ser direto na SEFAZ.** Isso promove o
   certificado digital por empresa de "adiado" a pré-requisito.

⚠️ Duas ressalvas que ficam registradas: a importação de XML **continua
existindo como rede** — desligar o FSist antes da captura própria estar
conferida seria trocar o certo pelo duvidoso; e **o serviço da SEFAZ ainda não
foi estudado por ninguém aqui** (limites, o que acontece ao perder o número de
sequência, se o certificado A1 da BWS tem o perfil necessário). Isso é estudo
antes de código, e antes de prometer data.

### Medições e emissão de nota: o desenho, e a resposta sobre Petrolina — 09/09/2026

Ditado pelo dono. Especificação inteira em `MEDICOES_E_NOTAS.md`; aqui ficam as
decisões que mudam o rumo e a pesquisa que ele pediu.

**O "Protocolos e Medições" do Pipefy vem para o ERP**, como o lado a RECEBER.
Com a ressalva que ele mesmo fez: nem toda coisa a receber é medição — título a
receber é o gênero, medição é a espécie.

**O tipo da medição não pode ser lista fechada.** Foi o ponto mais fácil de
errar. O caso simples é medição 1 e medição 1R (o reajuste dela). Mas ele
descreveu três desvios reais: órgão que numera o reajuste em sequência (virou a
medição 3), órgão que numera em paralelo (1 e 1R correlacionadas), e medições
subsidiárias por fontes diferentes. **Quem manda na nomenclatura é o órgão, não
o ERP** — então o tipo é categoria editável e o número é texto livre. Impor
"1, 2, 3" quebraria no primeiro contrato fora do padrão, e ele já viu isso.

**Entrar pelo CONTRATO, não pela medição.** Ele mudou de ideia no meio da fala e
a segunda ideia é a certa: contrato tem começo, meio e fim; medição é evento
dentro dele. O quadro financeiro do contrato mostra as medições e os totais —
contratado, aditivado, medido, faturado, recebido, a receber, retido.

**Protocolo com número e data** destrava um indicador que hoje não existe:
quantos dias entre protocolar e receber, por obra e por órgão.

**A emissão é por empresa: API ou MANUAL.** Decisão dele. Os dois caminhos
terminam no mesmo lugar. No manual, quem lê o PDF é a mesma IA que já lê nota
de entrada. **O manual vem primeiro de propósito** — não depende de
credenciamento nenhum, funciona no dia seguinte e serve de rede quando a API
falhar.

**Controle de notas emitidas é tela SEPARADA do título a receber**, e a crítica
que ele pediu é esta: uma medição pode virar duas notas (parcial), e uma nota
pode ser cancelada e substituída sem o título mudar. São dois eixos; juntá-los
esconderia justamente os casos que dão trabalho. Ficam duas telas irmãs,
ligadas nos dois sentidos.

**Boa notícia no cadastro:** a obra JÁ tem no modelo CNO, alíquota de ISS, ISS
retido, regime e conta de recebimento. O que falta é a TELA expor. Falta mesmo
só a **chave Pix** na conta bancária — e o uso que ele deu é copiar e mandar
quando alguém pede os dados da empresa.

#### A resposta sobre Petrolina

Ele perguntou se Petrolina/PE tem API como a que ele fez para Eusébio/CE.
**Tem, e é o MESMO fornecedor (E&L).** O endereço segue o mesmo molde
(`{uf}-{municipio}-pm-nfs-backend.cloud.el.com.br`), com padrão ABRASF 2.04 e
autenticação por certificado A1 — igual ao Eusébio.

Portanto **o emissor não precisa ser reescrito**. Hoje o endereço e o código
IBGE estão fixos no código (`el_nfse_abrasf.py`, `el_nfse_nacional.py`,
`job_nacional.py`); o trabalho é torná-los configuração por empresa — o que já
seria necessário de qualquer jeito, porque a BWS opera com mais de um CNPJ.

**E um prazo que muda o planejamento:** a LC 214/2025 tornou o **padrão
nacional obrigatório**, e a convivência com o ABRASF 2.04 está acabando ao
longo de 2026. O sistema já fala o padrão nacional (`el_nfse_nacional.py`).
**A aposta certa é o canal nacional** — investir agora em ABRASF por município
é construir sobre algo com data para acabar.

⚠️ **Depende de providência dele, não de código:** Inscrição Municipal em
Petrolina e credenciamento na Secretaria de Finanças, o token próprio do canal
de lá (o `EL_NFSE_TOKEN` atual é do Eusébio), e os códigos de serviço e a
alíquota de ISS de Petrolina.

⚠️ **Não verificado:** se a BWS já tem Inscrição Municipal em Petrolina, e a
data exata em que o município encerra o ABRASF. As duas se confirmam com a
prefeitura, não com pesquisa.

### Os blocos de documentos — 09/09/2026

Dúvida do dono: *"como é que esses blocos vão se associar a determinados
documentos? Se isso é fácil de resolver."*

**É fácil, e a resposta é a decisão de desenho:** o bloco **aponta para TIPOS,
não para documentos**. Um bloco é uma lista de tipos mais um recorte (esta
obra, esta competência), e o sistema procura. Se apontasse para documentos,
cada competência nova exigiria remontar o bloco à mão — que é o trabalho que
este módulo veio eliminar. Apontando para tipos, o bloco fiscal de agosto e o
de setembro são o **mesmo** bloco.

Cinco blocos nascem prontos (migração 046): **FISCAL** (obra + competência —
conteúdo confirmado por ele como "o que o cliente pede na medição"),
**HABILITAÇÃO**, **CADASTRO DE FORNECEDOR**, **DOSSIÊ DA OBRA** e **MEDIÇÃO**.
Editáveis, e aplicar de novo **não sobrescreve** — o conteúdo de um bloco é
decisão da BWS, e apagar ajuste de quem sabe seria estrago.

**O detalhe que faz o bloco fiscal funcionar:** dentro dele há documentos da
OBRA (folha, guia de FGTS) e da EMPRESA (recibo da DCTFWeb, DARF). Pedindo o
bloco de uma obra, o sistema resolve os itens de empresa **pela empresa daquela
obra**. Sem isso o bloco viria pela metade e ninguém entenderia por quê — na
tela, a empresa aparece sozinha, sem ninguém escolher.

**O zip traz um `CONFERENCIA.txt` como primeiro arquivo**, listando o que veio
e — mais importante — **o que falta**, separando obrigatório de opcional. Bloco
que entrega oito de dez arquivos calado é pior que bloco nenhum. Quando só
falta opcional, o texto diz "Nada OBRIGATÓRIO": o bloco está pronto para
entregar e quem lê precisa saber sem contar linha por linha.

**Vencido não entra**, e vai para a lista de faltas dizendo quando venceu.
Mandar certidão vencida é pior do que não mandar.

**Uma distinção que um caso de teste encontrou**, e que valeu a pena: certidão
é UMA (vai a de validade mais longa — duas válidas do mesmo tipo acontecem, e
mandar as duas confunde), mas aditivo são TODOS (mandar só o último esconderia
o histórico do contrato). O código tratava os dois igual; agora separa pelo
grupo do tipo.

**Quem não pode ver um tipo não recebe "está faltando" dele.** Seria mentira, e
já entregaria que o documento existe.

Provado com banco de verdade (15 casos) e na tela: baixei o bloco fiscal de uma
obra, abri o arquivo compactado e li a conferência.

### A melhoria dos lotes estava morta havia oito dias — 09/09/2026

**Incidente, e dos bons de aprender.**

Em 01/09 a ficha do lote foi melhorada: as fases (Aberto/Enviado/Pago/Cancelado)
saíram porque não existiam no processo real, e entraram os botões **Incluir
SPs** e **Excluir lote**, mais os quadrinhos "Já pagas" e "Em aberto".

O código foi escrito, revisado, publicado — e **nunca apareceu na tela**.

**Por quê:** a versão nova foi acrescentada NO TOPO do bloco de JavaScript, e a
versão antiga das mesmas funções ficou embaixo, sem ser removida. Em
JavaScript, quando duas funções com o mesmo nome são declaradas no mesmo
escopo, **a de baixo vence** — em silêncio, sem erro, sem aviso no navegador.

Oito funções estavam duplicadas na tela de Pagamentos; seis eram cópias
idênticas, e a `abrirLote` tinha duas versões diferentes rodando a errada. Em
06/09 uma sessão chegou a escrever a função `adicionarSPsAoLote` "que nunca
tinha sido escrita" — quando na verdade a chamada morta era da versão VELHA,
que ninguém sabia que estava no comando. Ou seja: o defeito custou trabalho
duas vezes.

A tela de Configurações tinha o mesmo problema, mais brando: `carregarDepara`
duplicada em cópia idêntica.

**A defesa, que é o que fica:** `tests/test_telas_sem_funcao_repetida.py` recusa
qualquer tela que declare a mesma função — ou a mesma constante de primeiro
nível — duas vezes. Roda junto com a suíte, em milésimos, sem navegador.

É a quarta varredura desta família, e todas nasceram do mesmo jeito: defeito
silencioso que só apareceu quando alguém foi usar.

| Varredura | Recusa |
|---|---|
| `test_nomes_indefinidos.py` | função que cita nome que o Python não acha |
| `test_telas_chamam_rota_que_existe.py` | tela que pede endereço que o servidor não tem |
| `test_telas_blocos.py` | `{% block %}` que a base não declara |
| `test_telas_sem_funcao_repetida.py` | função declarada duas vezes na mesma tela |

**A lição, escrita para a próxima sessão:** ao melhorar uma tela, PROCURAR a
versão antiga antes de escrever a nova. Acrescentar por cima não substitui — em
JavaScript, enterra.

Conferido no navegador depois do conserto: a ficha do lote abre com "Incluir
SPs", "Excluir lote" e os quadrinhos novos, sem erro de JavaScript.

### O cadastro que destrava a emissão de nota — 09/09/2026

Passo 1 do `MEDICOES_E_NOTAS.md`, migração **047**. Três coisas que ele pediu, e
uma que mudou de prioridade.

**A chave Pix da conta bancária**, com o motivo que ele deu: *"eventualmente a
gente precisa consultar, e tendo esse cadastro das contas é o local mais fácil"*.
Não é para pagar por ali — é para **copiar e mandar**. Por isso o botão
**"Copiar dados"** monta o bloco inteiro (razão social, CNPJ, banco, agência,
conta e as chaves), pronto para colar num WhatsApp: copiar campo por campo é
onde se erra um dígito, e dígito errado em dado bancário é dinheiro no lugar
errado. Várias chaves por conta, e o formato de cada uma é conferido — CPF com
cinco dígitos é recusado na hora, não meses depois.

**Os dados de emissão POR EMPRESA.** Isto ia ficar para o fim; mudou quando ele
respondeu que *a BWS não tem inscrição municipal em Petrolina, mas outra empresa
que vai operar tem*, e que *uma emite por API e a outra manual*. Ou seja: emitir
em mais de um município virou requisito do primeiro dia. Município, código IBGE,
endereço do serviço, canal, série, alíquota, código de serviço e token saíram do
código e viraram cadastro. Eusébio/CE e Petrolina/PE já vêm na lista — escolher
o município preenche endereço e código sozinho.

**Três defesas, e cada uma tem motivo:**

- **MANUAL é o padrão.** Empresa recém-cadastrada não sai emitindo nota fiscal
  sozinha porque alguém esqueceu de configurar.
- **HOMOLOGAÇÃO é o padrão.** Emitir é irreversível: em produção, cada emissão
  gera documento fiscal de verdade.
- **O token vai cifrado ou não vai.** Sem a `ERP_CHAVE_SEGREDOS` o sistema
  RECUSA gravar, em vez de guardar aberto — token de emissão assina em nome da
  empresa. E ele **nunca volta para a tela**: ela sabe que existe, não recebe o
  valor.

Ligar a API sem endereço e sem município é recusado duas vezes: no código, com
mensagem em português, e no banco, para o caso de um código futuro esquecer. E
quando falta algo, a tela **diz o que falta, item a item** — dizer só "não dá"
faria a pessoa adivinhar.

**O filtro por conta nos Pagamentos**, o incômodo diário que ele citou: *"às
vezes é mais fácil do que filtrar por obra"*. A conta vem da obra; título
rateado entre obras de contas diferentes aparece nos dois filtros, que é o
certo. Quando nenhuma obra tem conta, o filtro **diz isso** em vez de aparecer
vazio.

**Na tela da obra** entraram o regime de tributação e a conta que RECEBE a
medição — diferente da conta que paga, que já existia. Os dois já estavam no
banco; faltava a tela mostrar.

Provado com banco de verdade (22 casos) e no navegador: guardei uma chave Pix,
vi a chave torta ser recusada com a mensagem certa, escolhi Petrolina e vi o
endereço se preencher sozinho, e liguei o filtro por conta nos pagamentos.

### Três correções do dono sobre emissão de nota — 09/09/2026

**1. Por onde a nota sai NÃO se escolhe — se deriva.** Eu tinha apresentado
errado. Palavras dele: *"por onde vamos emitir não é algo que a gente
seleciona. Quem define é o centro de custo a que aquela medição está associada.
Se eu vou emitir um título da obra X, que está na empresa Y, eu vou usar a
solução da empresa Y."*

A cadeia é de mão única e o sistema desce ela sozinho: **medição → obra →
empresa → município, endereço, token, modo**. A tela de cadastro da empresa
existe para dizer UMA VEZ onde ela emite; na hora de emitir, ninguém escolhe.

E quando a cadeia quebra, o certo é **recusar**, não chutar: obra sem empresa
não emite, e o sistema manda arrumar o cadastro. Título rateado entre obras de
empresas diferentes também recusa — seriam duas notas, de CNPJs diferentes.
Emitir pelo CNPJ errado se conserta com cancelamento e carta ao cliente.

**2. O controle da numeração** (migração 048). Ele perguntou se dá para ver o
número da nota antes de emitir e manter a numeração correta. **Dá, e por um
motivo técnico:** no padrão nacional e no ABRASF, **quem numera a DECLARAÇÃO é
quem emite** — a prefeitura devolve o número da NOTA. São dois números:

- `numero_dps` — a sequência da empresa, por série. **O ERP é dono.**
- `numero_nota` — o que a prefeitura devolveu. O ERP só registra.

Três coisas que o sistema passa a garantir: o **duplicado é impossível**
(índice único por empresa, ambiente, série e número — vale mesmo com duas
pessoas emitindo ao mesmo tempo); **teste não queima número de produção**
(homologação tem sequência própria); e o **buraco fica visível**.

A conferência separa duas coisas que parecem iguais e não são: **buraco**
(número que nunca foi reservado — sinal de que alguém emitiu pelo portal da
prefeitura) e **queimado** (reservado, não virou nota, com motivo escrito).
Buraco é o preocupante; queimado tem resposta pronta.

O número é reservado ANTES de emitir e **não volta para a fila se falhar**: a
prefeitura pode ter recebido a declaração e só a resposta ter se perdido, e
reemitir com o mesmo número daria duplicidade do lado dela. Número de nota
fiscal não se apaga — se explica.

⚠️ O ponto de atenção dele é real: **manual e API na MESMA empresa e série** é
onde a numeração se perde. Na BWS não acontece (uma empresa é API, a outra
manual), mas o modo fica guardado em cada linha para a mistura ser visível se
um dia ocorrer.

**3. Título rateado entre obras de contas diferentes: BLOQUEADO.** Decisão
dele, com o argumento que fecha a questão: *"como é que eu vou pagar um boleto
de duas contas bancárias? É impossível."*

A recusa é no LANÇAMENTO de propósito — quem lança ainda pode pedir dois
boletos ao fornecedor; depois de lançado, dividir dá trabalho. A mensagem diz
**quais obras**, **quais contas** e **qual a saída**. Obra sem conta definida
não bloqueia: cadastro incompleto não pode parar o financeiro por um campo em
branco.

**Sobre o reajuste e o INCC** (pedido no mesmo dia, ainda por construir): a
especificação foi escrita em `MEDICOES_E_NOTAS.md` §7-C. O achado que importa é
que **dá para o sistema manter a tabela do INCC sozinho, de graça** — o Banco
Central republica a série no SGS, em API pública sem cadastro (INCC-DI é a
série 192), o que evita depender do FGVDados, que é licenciado.
⚠️ **Não verificado:** a chamada foi bloqueada pela filtragem de saída deste
contêiner; a primeira de verdade acontece no Render. E fica uma pergunta para
ele: os contratos usam INCC-**DI** ou INCC-**M**? São séries diferentes, e
índice errado dá valor errado com cara de certo.

### A medição completa e o quadro do contrato — 09/09/2026

Migração **049** e uma tela nova em **Obras › "Contratos e medições"**.

**O tipo da medição é tabela, não lista no código.** Foi o ponto que o dono
fez questão de detalhar, e o mais fácil de errar: *"às vezes o nosso sistema
não se adequa a cem por cento, porque teve uma medição 1 alguma coisa e outra
medição 1 alguma coisa, por conta de fontes diferentes, e o órgão trata dessa
forma."* Quem manda na nomenclatura é o ÓRGÃO. Por isso o número da medição
continua sendo texto livre ("1", "1R", "3", "1-FONTE-A", "02/2026") e os cinco
tipos que ele confirmou — normal, reajuste, aditivo, subsidiária, complementar
— vivem numa tabela que se edita sem mexer no sistema.

**A correlação do reajuste funciona nos dois jeitos de numerar.** No órgão que
numera em paralelo, o reajuste da medição 1 é a "1R". No que numera em
sequência, o mesmo reajuste é a "medição 3" e entra na fila como se fosse
normal. Em ambos, o reajuste APONTA para a medição que reajusta — e é essa
ligação que permite dizer "a medição 1 rendeu X, mais Y de reajuste". Sem ela
os dois valores ficam soltos e ninguém soma.

O sistema recusa três ligações que dariam valor errado com cara de certo:
reajuste de si mesma, reajuste de medição de OUTRO contrato, e reajuste de
reajuste.

**O protocolo destrava o indicador que ele pediu**: dias entre entregar a
medição no órgão e o dinheiro entrar. Só entra na média o que já foi recebido
— medição protocolada e não paga tem espera, não prazo, e misturar as duas
daria uma média que MELHORA sozinha quando o cliente atrasa.

**O quadro do contrato separa três coisas que costumam virar uma só:**

    medido  ≠  faturado  ≠  recebido

Medir não é faturar; faturar não é receber. São três colunas, e a tela ainda
lista de olho o que foi medido e não virou nota, o que virou nota e não entrou,
e o que não foi protocolado.

**O reajuste NÃO consome saldo do contrato** — é acréscimo por índice, não obra
executada a mais. Um contrato de 1,85 milhão com 1,02 milhão medido, dos quais
28,5 mil de reajuste, tem 855 mil de saldo (e não 826,5 mil).

Um defeito achado ao olhar a tela num navegador: a **lista** de contratos
descontava o reajuste do saldo e o **quadro** não — dois números diferentes
sobre o mesmo contrato, na mesma sessão. Quem visse isso perderia a confiança
nos dois, com razão. Agora a aritmética é a mesma nos dois lugares, e um teste
guarda isso.

**Quem vê:** ação nova `ver_contratos`, dada a administrador, diretor,
financeiro, gestor de obras e consulta. Perfil preso a obra ou a autoria fica
de fora **de propósito** — o quadro mostra o contrato inteiro e não há como
recortá-lo por obra designada sem mentir no total. Abrir depois é uma linha.

**Provado:** 27 testes com banco de verdade (`tests/test_medicao_quadro_banco.py`)
e a tela percorrida num navegador de ponta a ponta — abrir, classificar,
protocolar e voltar, sem um erro de JavaScript.

### A tela de controle das notas emitidas — 09/09/2026

Financeiro › **"Notas emitidas"**. É a tela IRMÃ da de títulos a receber, e a
crítica sobre isso foi o próprio dono quem pediu: *"talvez isso seja a mesma
coisa que o título a receber, ou não, não sei. Aí você vai fazer essa
crítica."*

**Não é a mesma coisa, e a diferença é justamente onde dá trabalho:** uma
medição pode virar DUAS notas (faturamento parcial), e uma nota pode ser
cancelada e substituída sem o título mudar uma vírgula. Título a receber ainda
inclui coisa que não é medição e não tem nota nenhuma. Forçar os dois na mesma
tela esconderia exatamente os casos que precisam aparecer.

**Cada tributo tem SUA coluna** — ISS, IR, INSS, PIS, COFINS, CSLL —, e não vai
tudo somado num "retido". O motivo é prático: a contabilidade lança ISS numa
conta e INSS em outra, e do total ninguém volta atrás. É o relatório que ele
descreveu: *"às vezes a contabilidade precisa gerar um relatório das
informações — valor da nota, tributos e tal."* A tela exporta em Excel e PDF
como todas as outras.

**Nota cancelada fica FORA dos totais, mas continua na lista.** Somar cancelada
com válida é como um relatório fiscal começa a mentir; sumir com ela é como se
perde o rastro de por que faltou um número na sequência.

**A conferência da numeração** responde à pergunta clássica da fiscalização, e
separa duas coisas que parecem iguais: **buraco** (número que nunca foi
reservado — alguém emitiu por fora do ERP) e **queimado** (reservado, não virou
nota, com o motivo escrito). Buraco é o preocupante.

**Registrar a nota que saiu pelo PORTAL** existe por causa disso: enquanto a
emissão automática não estiver de pé para as duas empresas, alguém vai emitir
no site da prefeitura — e se ninguém registrar, a conferência acusa buraco e
não se sabe por quê.

**Cancelar exige motivo, e o número não volta para a fila.** A prefeitura pode
ter recebido a declaração e só a resposta ter se perdido; reusar o número daria
duplicidade do lado dela. Número de nota fiscal não se apaga — se explica.

Um defeito corrigido no caminho: quando o sistema recusava a nota repetida, ele
desfazia a transação INTEIRA, levando junto o que mais estivesse pendente. Uma
recusa não pode apagar trabalho que ninguém mandou apagar. Agora desfaz só a
gravação recusada, e há teste guardando isso.

**Quem vê:** duas ações novas — `ver_notas_emitidas` (larga dentro do
escritório: é dessa tela que sai o relatório) e `emitir_nota` (estreita:
registrar e cancelar, só administrador, diretor e financeiro).

**Provado:** 18 testes com banco de verdade
(`tests/test_notas_emitidas_banco.py`) e a tela percorrida num navegador —
listar, conferir a numeração, registrar do portal, tentar registrar a mesma
nota de novo (recusada com a frase certa) e cancelar com motivo.

### A emissão da nota a partir da medição — modo MANUAL — 09/09/2026

Botão **"Emitir nota"** em cada medição do quadro do contrato. O manual vem
antes do automático de propósito: funciona no dia seguinte, sem credenciamento,
sem certificado e sem token — e continua servindo de rede quando a API falhar
ou a prefeitura estiver fora do ar. A segunda empresa, que vai operar em
Petrolina e ainda nem tem inscrição municipal, emite por aqui desde já.

**O que o ERP faz e o que ele NÃO faz.** Ele monta num bloco só tudo que o
portal pergunta — prestador, CNPJ, inscrição municipal, tomador, discriminação
do serviço e as retenções já calculadas — e a pessoa copia. Quem emite é ela,
no site da prefeitura. O ERP **não reserva número antes**: no manual quem
numera a nota é o portal, e reservar aqui criaria uma sequência paralela que
não existe lá.

**As retenções saem calculadas do cadastro da obra**, pelo mesmo cálculo que o
módulo de emissão já usa: INSS 11% sobre a parcela de serviço, ISS pela
alíquota do município (com dedução de material quando o município aceita), e as
federais conforme o contrato. Numa medição de 265 mil da Escola do Planalto
isso dá ISS 3.975, INSS 14.575, IRRF 3.180 e PCC 12.322,50 — líquido de
230.947,50, sem ninguém abrir calculadora.

**A discriminação vai montada** com medição, período, contrato, objeto e CNO.
É o campo que mais volta corrigido: sem o número da medição e o período, o
setor de empenho do órgão não sabe a que competência a nota se refere e devolve.

**A volta é com o PDF.** A IA lê a nota que a prefeitura devolveu e preenche
número, data, valor e retenções — e a tela diz a confiança da leitura e manda
conferir. É o MESMO leitor do comprovante e da nota de fornecedor: caminho de
leitura novo seria caminho novo para manter.

**Registrar sem informar retenção não grava zero** — usa o cálculo. Zero é uma
afirmação, não uma ausência, e o relatório da contabilidade sairia dizendo que
nada foi retido.

**Um defeito que enganava de verdade**, achado ao ler o bloco na tela: a
alíquota do IRRF saía escrita **"1.200%"**. Em português isso se lê como mil e
duzentos por cento — e a frase ia dentro do texto que a pessoa copia para o
portal. Agora sai "1,2%", e o dinheiro das explicações também saiu do formato
americano ("132500.00" virou "132.500,00").

**Provado:** 16 testes com banco de verdade
(`tests/test_emissao_manual_banco.py`) e o caminho inteiro percorrido num
navegador — abrir a medição, ver o bloco, tentar registrar sem número
(recusado), registrar com número e código de verificação, e ver a nota
aparecer no quadro do contrato e na tela de notas emitidas com cada tributo em
sua coluna.

⚠️ **O que continua faltando para a emissão AUTOMÁTICA** (item 6 do roteiro):
inscrição municipal, credenciamento, token e códigos de serviço da empresa que
vai operar em Petrolina. Nada disso trava o manual.

### O reajuste, e a tabela do INCC que o sistema mantém sozinho — 09/09/2026

Migração **050**. Pedido dele: *"dentro do cadastro do contrato a gente precisa
fazer alguma configuração que permita prever o recebimento de reajustes."*

**A data-base é CAMPO, e não regra — porque muda por contrato.** Ele foi
explícito: pode ser a do orçamento OU a da proposta da licitação. Fixar uma das
duas no código erraria metade dos contratos, e erraria num sentido perigoso: o
valor sairia calculado, com cara de certo, e ninguém confere um número que o
computador deu. Agora o contrato diz qual data é, e **de onde ela veio** — que
é a primeira coisa que o órgão pergunta quando contesta.

Contrato sem data-base própria **herda a da obra**, e a tela diz que herdou.
Sem isso, todo contrato antigo apareceria como "não configurado" mesmo com o
dado já no sistema.

**O direito nasce depois da periodicidade** (12 meses, como ele descreveu, e
configurável). Antes disso o sistema recusa dizendo quantos meses faltam, em
vez de calcular um valor que ainda não é devido.

**A conta acumula mês a mês, e começa no mês SEGUINTE à data-base**: a
data-base é o ponto zero, o mês dela já está dentro do preço contratado, e
incluí-lo cobraria um mês a mais. Dá para conferir a diferença: 1% ao mês por
doze meses é 12,68%, não 12% — e é a diferença entre os dois que aparece na
conta do contrato.

**Mês faltando na tabela vira RECUSA, não número menor.** Se a série do índice
não cobre o período inteiro, o sistema diz quais meses faltam e manda buscar ou
lançar. Calcular com metade da série daria um valor a menos que passaria
despercebido — que é justamente o erro que ninguém pega.

**A conta sai escrita por extenso**, em português: *"INCC-DI acumulado de
02/2025 a 01/2026 (12 meses) = 12,6825%. Reajuste = 100.000,00 × 12,6825% =
12.682,50."* É o que se manda para o órgão quando ele pergunta de onde saiu o
número.

**O valor é EDITÁVEL na hora de virar título**, porque ele avisou: *"pode ser
que o órgão tenha algum entendimento e mude algum centavo"*. O sistema estima;
quem fecha é o órgão. O título guarda o **previsto** ao lado do **lançado**,
então a diferença fica visível em vez de sumir. E o reajuste **não pode ser
gerado duas vezes** — dois cliques cobrariam o reajuste em dobro.

### A tabela do INCC, mantida pelo próprio sistema

Ele pediu: *"já coloque aí dentro da programação do sistema ele fazer essa
busca, atualizar a tabela e permitir todos esses cálculos."*

Está em **Configurações › "Índices (INCC)"**. O INCC é calculado pela FGV, e o
serviço de dados dela é licenciado — contrato, chave e conta a pagar. O **Banco
Central republica a série de graça** no SGS, em API pública e sem cadastro: o
INCC-DI é a série **192**, que é a que ele confirmou como usada nos contratos
da BWS. (O INCC tem três versões — DI, M e 10 —, com apurações diferentes;
usar a errada dá valor errado com cara de certo, então a série está escrita no
código com o nome por extenso.)

Três decisões que valem registro:

- **A busca é pelo BOTÃO, nunca no start do serviço** — mesma regra das
  migrações, e pelo mesmo motivo: uma chamada externa no boot derrubaria o
  monorepo inteiro se o Banco Central estivesse fora do ar.
- **A coleta nunca sobrescreve o que foi lançado à mão.** O INCC-DI do mês só
  sai por volta do dia 25, e num fechamento apertado alguém vai digitar o
  número do boletim da FGV. Se a coleta passasse por cima, apagaria a correção
  sem avisar ninguém. A tabela mostra qual linha é qual.
- **Guarda a variação do mês, não o acumulado.** Quem guarda variação produz o
  acumulado de qualquer período; quem guarda acumulado não consegue voltar — e
  reajuste é discussão com o órgão, então a conta tem de ser reproduzível dois
  anos depois.

⚠️ **A primeira chamada de verdade ao Banco Central só acontece no Render.** A
saída para a internet do ambiente onde o código foi escrito é filtrada e
bloqueia o endereço. O que ficou provado aqui foi o caminho do ERRO — a tabela
continua intacta e a tela explica em português o que houve — e a gravação, com
um dublê no lugar da rede. O caminho de sucesso contra o serviço real, não.

**Dois defeitos corrigidos no caminho**, os dois de leitura: o quadro do
contrato mostrava **"A receber: −R$ 465.000,00"** quando entrava dinheiro sem
nota emitida. Isso não é dívida ao contrário, é outra coisa — e das que a
contabilidade precisa ver. Agora "a receber" nunca é negativo e apareceu um
quadrinho **"Recebido sem nota"**. E o erro do Banco Central despejava dez
linhas de traçado técnico na tela; agora o detalhe vai para o registro e a
pessoa lê uma frase.

**Provado:** 25 testes com banco de verdade (`tests/test_reajuste_banco.py`) e
o caminho inteiro num navegador — informar a data-base, ver a previsão do
contrato, gerar o reajuste de uma medição com valor editado, ver o título
nascer correlacionado e o quadro somar certo, lançar um mês do índice à mão, e
tentar buscar no Banco Central com a rede bloqueada.

### A Agenda do ERP — 09/09/2026

Migração **051**. Está em **Obras › "Agenda"**, e o número aparece na porta de
entrada do ERP.

**Por que ela existe.** Quatro coisas construídas antes dela sabiam calcular a
própria data e não tinham onde AVISAR: o aniversário do reajuste da obra, a
conferência mensal dos equipamentos locados, o vencimento das certidões e o fim
da vigência do contrato. Um alerta que mora dentro da tela que a pessoa só abre
quando já lembrou do assunto não é alerta, é enfeite. O que faltava era um
lugar que se abre de manhã.

**Cada aviso tem prazo próprio, e cada prazo tem motivo:**

| Assunto | Avisa antes | Por quê |
|---|---|---|
| Aniversário de reajuste | 45 dias | dá tempo de juntar índice, calcular e protocolar |
| Fim da vigência | 60 dias | aditivo de prazo não se pede na véspera |
| Certidão | o que o tipo mandar | federal se tira no dia; alvará leva semanas |
| Conferência de locação | no mês | a resposta é sobre aquele mês |

**O aviso deduzido é RECALCULADO, não acumulado.** Cada um tem uma chave
estável, então sincronizar dez vezes no mesmo dia não empilha dez avisos — e o
que deixou de valer (certidão renovada, contrato encerrado) **some sozinho**.
Isso não é detalhe: agenda que acumula aviso velho é agenda que ninguém abre, e
uma agenda em que não se confia é pior que nenhuma, porque dá a sensação de que
alguém está olhando.

**Três coisas nunca somem, cada uma por um motivo diferente:** o RESOLVIDO é
histórico (quem tratou, quando, e o que escreveu); o DISPENSADO é decisão — se
voltasse, a pessoa dispensaria de novo, para sempre; e a ANOTAÇÃO manual
ninguém deduziu, então ninguém pode deduzir que sumiu.

**Dispensar exige motivo.** Três meses depois, "não se aplica" sem explicação é
indistinguível de esquecimento — e é justamente o que alguém vai querer
entender quando o problema aparecer.

**Só a certidão MAIS NOVA de cada tipo conta.** A anterior vencida é histórico;
avisar sobre ela seria avisar sobre um problema já resolvido.

**Um gerador com defeito não derruba a agenda inteira** — o resto dos avisos
continua valendo e a falha fica dita.

**Dois defeitos achados enquanto eu olhava a tela**, e os dois valem a pena
registrar porque a classe se repete:

1. **A opção "ver também o que vem depois" não mostrava nada.** Os geradores
   filtravam pela janela de aviso, então o que ainda não era hora nem chegava
   a existir. Corrigido invertendo a responsabilidade: os geradores descrevem o
   calendário INTEIRO e a leitura decide o que aparece. É o que transforma isto
   num calendário em vez de uma caixa de alarmes.
2. **A recusa do servidor custava o que a pessoa tinha digitado.** Dispensar
   sem motivo era recusado (certo) com a janela já fechada (errado): a mensagem
   aparecia atrás e quem escreveu meia frase começava do zero. Agora a janela
   volta preenchida, com o erro escrito em cima.

**Provado:** 26 testes com banco de verdade (`tests/test_agenda_banco.py`) e a
tela percorrida num navegador contra os dados de demonstração — que já trouxe
três conferências de locação em atraso e um certificado de FGTS vencendo em
sete dias, sem ninguém cadastrar nada para o teste.

⚠️ **A sincronização roda ao abrir a tela da agenda.** Hoje são quatro
consultas curtas e o custo não aparece; se um dia pesar, ela vira tarefa
separada — está no roteiro, junto com o resto do trabalho pesado que precisa
sair das telas.

### Empreita: retenção de garantia e alçada por valor — 09/09/2026

Migração **052**. Duas coisas que estavam no roteiro desde o começo.

**A RETENÇÃO DE GARANTIA** é o costume da construção: guarda-se uma parte de
cada medição — na BWS, 5% — e devolve-se no fim, quando o serviço passou pelo
período de garantia. Serve para o dia em que o empreiteiro some e o reparo
fica com a obra.

O defeito que ela corrige é sempre o mesmo na planilha: retém-se direitinho
por doze medições e, no fim, **ninguém sabe quanto ficou retido nem quando
devolver**. O dinheiro fica parado, o empreiteiro cobra, e alguém refaz a conta
de memória.

Agora o contrato diz o percentual, cada medição desconta sozinha, e o valor a
pagar já sai líquido. Numa empreita de 180 mil com três medições, os 6.750
retidos aparecem num quadrinho próprio, e quando o contrato termina a tela diz
**"o serviço terminou — é hora de devolver"**.

**Três decisões que valem registro:**

1. **A garantia incide sobre o MEDIDO, não sobre o líquido.** Ela é uma parte
   do serviço executado; o adiantamento é dinheiro que já saiu. Calcular sobre
   o líquido faria a retenção encolher justamente na medição que abate
   adiantamento — e no fim do contrato faltaria garantia.
2. **O valor retido fica GRAVADO na medição**, não recalculado pelo percentual
   atual. O percentual pode mudar por aditivo, e a medição de março tem de
   continuar dizendo quanto foi retido em março. Guardar só o percentual faria
   a conta do passado mudar sozinha — que é como se perde uma discussão com o
   empreiteiro.
3. **A devolução vira TÍTULO A PAGAR**, não acerto de planilha: passa pela
   mesma aprovação, a mesma baixa e o mesmo comprovante de qualquer pagamento,
   porque é dinheiro saindo. Não pode ser feita duas vezes, e antes do fim do
   contrato exige motivo escrito — é o caso em que alguém vai perguntar por
   quê, meses depois.

Uma lista nova responde a pergunta que a planilha não responde: **de quem a BWS
ainda está com garantia na mão**, e quais contratos já terminaram.

**A ALÇADA POR VALOR** fecha um buraco: antes dela, uma empreita de oitocentos
reais e uma de oitocentos mil passavam pela mesma porta — qualquer perfil de
obra aprovava as duas. As faixas nascem assim:

| Até | Quem aprova |
|---|---|
| R$ 50.000 | supervisor, gestor, financeiro e direção |
| R$ 200.000 | gestor, financeiro e direção |
| acima | só a direção |

**Elas são TABELA, não número no código**, porque o teto muda com o tamanho da
empresa — e quando mudar, quem muda é o dono. As faixas iniciais reproduzem o
que já valia para o contrato pequeno e só estreitam o de cima: migração que
muda quem pode o quê sem avisar é migração que quebra a operação na segunda de
manhã.

**O aditivo entra na conta**: contrato de 40 mil aditivado para 60 mil sai da
faixa do supervisor. Senão bastaria cadastrar pequeno e aditivar depois.

**Uma coisa que a devolução respeita, e é regra do ERP inteiro:** dado bancário
vive no CADASTRO, nunca no lançamento. Quando o prestador tem uma única conta
homologada, o sistema usa essa; com mais de uma, quem escolhe é a pessoa —
adivinhar aqui é escolher para onde o dinheiro vai.

**Provado:** 19 testes com banco de verdade
(`tests/test_empreita_garantia_banco.py`) e o caminho inteiro num navegador,
contra uma empreita de 180 mil com três medições: ver a garantia acumulada,
devolver, e o título nascer com o valor certo.

### O certificado digital da empresa — 09/09/2026

Migração **053**, em Administração › Empresas › "Emissão de nota".

**O problema, do jeito que ele acontece:** o arquivo .pfx vive no computador
de alguém, com a senha num papel ou numa conversa antiga. Aí ele vence num
sábado, ninguém sabe, e a obra para de faturar na segunda-feira.

Duas coisas já construídas dependiam dele. A **emissão automática** da nota —
no padrão nacional a declaração vai ASSINADA, e sem certificado não há
assinatura. E a **agenda**, onde ele era o quarto aviso prometido que ficou de
fora justamente porque o certificado não tinha onde morar. Agora estão os
cinco.

**Três decisões que valem registro:**

1. **O arquivo e a senha vão CIFRADOS**, com a mesma chave que já protege a
   senha de e-mail — a que mora na Environment do Render e nunca no banco.
   Certificado digital é a **assinatura da empresa**: quem o tem, assina no
   nome dela. Uma cópia do banco não pode bastar. E **sem a chave o sistema
   recusa guardar**: a alternativa — guardar em claro "só desta vez" — é como
   uma assinatura de empresa vaza sem ninguém perceber.

2. **A validade é lida de dentro do arquivo, nunca digitada.** Campo de data
   que a pessoa preenche é campo que ela erra ou esquece de atualizar — e aqui
   o erro só apareceria no dia em que a nota não sai. De quebra, abrir o
   arquivo **prova que a senha está certa**: certificado que não abre não
   entra, e a mensagem diz o que quase sempre é ("confira a senha").

3. **O arquivo nunca volta pela tela.** A tela mostra titular, CNPJ, validade e
   emissor; os bytes só saem por dentro, para quem vai assinar. Não existe rota
   de download — o que não tem porta não é arrombado.

**Duas recusas que evitam erro caro:** certificado de outro CNPJ é recusado
(trocar os arquivos de duas empresas faria a nota sair assinada pelo CNPJ
errado — difícil de descobrir, caro de desfazer), e certificado já vencido não
entra (guardá-lo criaria a impressão de que a empresa está em dia).

**O anterior não é apagado**, vira histórico: a nota assinada em março foi
assinada com AQUELE certificado, e um dia alguém vai perguntar com qual.

**O aviso na agenda sai 45 dias antes**, porque certificado se renova com a
contadora e isso leva dias. Quando o certificado é substituído, o aviso
**fecha sozinho** — não fica um velho ao lado do novo.

**Provado:** 15 testes com banco de verdade
(`tests/test_certificado_banco.py`, que fabrica um A1 de verdade em memória
em vez de versionar um .pfx no repositório — certificado versionado é
certificado vazado, mesmo de teste) e o caminho inteiro num navegador: senha
errada recusada com a frase certa, senha certa guardando, a validade lida do
arquivo aparecendo na tela, e o aviso nascendo na agenda.

**Um defeito corrigido no caminho:** a confirmação "certificado guardado"
aparecia no painel que era redesenhado logo em seguida — a frase morria antes
de ser lida, que é o mesmo que não ter avisado.

### As solicitações passam de 500 — e os números do topo pararam de mentir — 09/09/2026

A tela de Solicitações trazia os **500 títulos mais novos e não dizia**. O
corte em si era o menor dos três problemas.

**O primeiro: o filtro de situação não alcançava o que era antigo.** Ele era
aplicado depois, sobre os 500 já trazidos. Filtrar por "bloqueado" não achava
nada se os 500 mais novos não tivessem nenhum — mesmo havendo um bloqueado
desde o começo do ano. A pessoa concluía que não havia nenhum. Agora o filtro
entra na consulta, que é onde filtro mora.

**O segundo, e o mais grave: os quadrinhos do topo somavam só esses 500 e se
apresentavam como "total".** Uma lista cortada é um incômodo; um total que
soma metade da base e se chama total é um **número que mente** — e ninguém
confere um número que o sistema deu. Hoje os oito quadrinhos somam o filtro
inteiro, e o rótulo diz "no filtro inteiro". Quando você liga um filtro que
acontece na tela (obra, credor, dedutibilidade), eles voltam a contar o que
está visível e o rótulo muda para "no que está na tela" — porque a alternativa
seria mostrar um número que não corresponde à lista embaixo dele.

**O terceiro: as caixinhas do filtro contavam só o carregado.** Uma situação
sem nenhum registro na página aparecia "zerada", em cinza — o que desencoraja
o clique justamente quando existem registros mais antigos. Agora a contagem
vem da base inteira.

**Como a lista cresce agora:** um botão "Carregar mais" que **acrescenta** em
vez de trocar de página. Acrescentar e não paginar foi escolha: os filtros de
obra, credor e dedutibilidade acontecem no navegador, sobre o que está
carregado — trocar de página faria eles enxergarem só a página nova, e o
resultado seria pior que o corte que estamos consertando.

E a tela **diz sempre onde está**: "Mostrando 200 de 240" quando falta, "todas
as 240 estão na tela" quando não falta. O pior de uma lista cortada não é o
corte — é a pessoa não saber que houve corte e decidir achando que viu tudo.

**A regra que ficou escrita** (`core/comum/paginacao.py`): a consulta filtrada
é montada UMA VEZ e serve às três perguntas — a página, a contagem e as somas.
Elas não podem divergir porque não existem separadas. É o mesmo princípio do
escopo de obra: um caminho só. O escopo, aliás, continua valendo nas três — se
valesse só na lista, o total do topo entregaria o valor de obras que a pessoa
não pode ver.

⚠️ **Só a tela de Solicitações foi convertida.** As outras listas continuam
com corte silencioso (Notas fiscais, Notas emitidas, Arquivo, Agenda,
Conciliação e Extratos em 500; Empreitas, Locações, Despesa com colaborador e
Movimentações em 300). Nenhuma incomoda no volume de hoje — a de solicitações
incomodava. Estão listadas no `ROTEIRO.md`, e todas usam o mesmo ajudante
quando chegar a vez.

**Provado:** 15 testes com banco de verdade (`tests/test_paginacao_banco.py`),
incluindo os dois defeitos antigos, o escopo por obra valendo nas três
perguntas e a conferência de que nenhuma página repete ou pula registro. E a
tela percorrida num navegador com 240 solicitações: carregar mais, chegar ao
fim, e filtrar por "bloqueado" achando o mais antigo de todos.

### A tela de saúde do sistema — 10/09/2026

Migração **054**, em Configurações › **"Saúde do sistema"**.

**Por que ela existe.** Em 08/09 o dono perguntou se o sistema aguenta crescer,
e a resposta que dei foi de raciocínio: "o gargalo é a máquina compartilhada,
não o tamanho da base". Era provavelmente certa, mas era um argumento, não uma
medição. E a próxima pergunta dele vai ser sobre **gastar** — trocar de plano
no Render, subir o banco. Decisão de gastar não pode ser palpite.

**O que a tela mostra:** memória em uso contra o teto do plano (com o pico), o
tempo médio de abertura das telas, quantas passaram do limite em que a pessoa
percebe que esperou, quantas falharam, o tamanho do banco e quanto dele é
documento.

**As telas lentas saem ordenadas pelo TEMPO TOTAL, não pela média.** Uma tela
de três segundos aberta uma vez por mês incomoda menos que uma de meio segundo
aberta duzentas vezes por dia. O que se quer consertar é onde a equipe espera
mais no fim das contas — e a média sozinha aponta para o lugar errado.

**Como a medição é feita, e os três cuidados:**

1. **Medir não pode custar mais que o que se mede.** Os tempos se acumulam na
   memória do processo e descem ao banco de minuto em minuto, **agregados por
   dia e por rota**. Uma linha por requisição faria a tabela de medição virar,
   ela mesma, o problema que veio medir.
2. **A gravação SOMA em cima do que já existe**, porque o processo reinicia a
   cada 150 requisições (`--max-requests`) e o dia é montado em pedaços.
3. **A medição nunca derruba uma tela.** Os dois ganchos estão embrulhados: se
   a gravação falhar, o número se perde e a vida segue. Sistema que cai por
   causa do próprio termômetro é pior que sistema sem termômetro.

**Dois defeitos achados enquanto eu olhava a tela:**

- Uma consulta do painel que falhasse **apagava o painel inteiro**: no
  Postgres, uma consulta com erro aborta a transação e todas as seguintes
  falham junto. Uma tabela ainda não criada deixaria a tela em branco — e
  painel vazio faz a pessoa achar que o sistema parou. Agora cada leitura vive
  no seu ponto de salvamento e falta só o pedaço que falhou.
- A contagem de linhas por tabela mostrava **"0 linhas"** para tabelas que o
  Postgres ainda não analisou. Ao lado de uma tabela de 300 KB, "0 linhas" é
  uma afirmação falsa. Agora, quando não se sabe, a tela mostra um traço.

**Nenhuma dependência nova.** A memória é lida de `/proc/self/status`;
acrescentar biblioteca para ler um arquivo de texto seria caro pelo que
entrega.

**Provado:** 17 testes com banco de verdade (`tests/test_saude_banco.py`),
incluindo mil chamadas virando uma linha, o banco fora do ar sendo engolido, e
a ordenação por tempo total. E a tela aberta num navegador **depois de passear
por oito telas de verdade** — os números que apareceram nasceram de uso, não de
dado inventado: 93 aberturas, 35 ms de média, 186 MB de memória de 2 GB.

### Petrolina sai da conta — 10/09/2026

Palavras dele: *"esqueça por enquanto credenciamento Petrolina. É uma empresa
futura."*

O que isso muda, na prática: **a emissão automática de nota deixa de estar
bloqueada.** Ela estava esperando inscrição municipal, credenciamento, token e
códigos de serviço de Petrolina — e nada disso é necessário para a **BWS no
Eusébio**, que já tem os três primeiros e agora tem também o certificado
digital (migração 053).

Fica registrado para não se perder: o desenho de duas empresas em municípios
diferentes, uma por API e outra manual, **continua valendo** e está construído
(migração 047). Ele simplesmente não tem urgência enquanto a segunda empresa
não existir.

⚠️ **O que continua sem verificação:** a primeira chamada real ao serviço do
município só acontece no Render. A saída de internet do ambiente onde escrevo
é filtrada — foi assim com o Banco Central, e será assim com a prefeitura.

### A ficha do título vira card, e as telas passam a se ligar — 10/09/2026

Duas coisas que estavam no ROTEIRO desde o começo e nunca tinham vindo.

**1. O detalhe do título deixou de ser janela.** Ele existia, mas abria numa
janela por cima da tela e **só com clique duplo** — que ninguém adivinha.
Agora um clique na linha expande o card ali mesmo, embaixo dela: quem está
conferindo não perde o lugar da lista, fecha e continua de onde estava. Tudo
que já havia continua: apontamentos, parcelas, pagamentos, rateio, retenções,
anexos, assinaturas e o histórico completo.

A janela **não morreu** — ficou com duas funções que são dela: os formulários
(reclassificar, alterar parcelas, desfazer baixa) e o caso do endereço direto
(`?titulo=N`, que vem da parcela de locação) cair num título que os filtros de
hoje não mostram: aí não existe linha para expandir.

**2. Encadeamento.** O que o dono pediu como *"conexão database do Pipefy"*:
clicar na obra, na conta, no credor ou na compra e ir para o cadastro. Vale na
lista de solicitações (obra e credor) e dentro da ficha (credor, conta, obra,
cada obra do rateio, e o pedido de compra que originou o título). As quatro
telas de destino passaram a aceitar o registro pelo endereço e já abrem nele:
`/erp/obras?obra=N`, `/erp/configuracoes?conta=N#plano`,
`/erp/suprimentos/fornecedores?fornecedor=N`,
`/erp/suprimentos/pedidos?pedido=N`.

**O elo respeita a permissão do destino.** Um financeiro não vê o link para o
pedido de compra, porque a tela de pedidos é de ADMIN e diretoria — link que
responde "sem permissão" é pior que texto puro, promete uma porta que não
abre. A trava continua sendo o `@permissao` da rota; isto é só a tela não
oferecer. O helper é `elo(tipo, id, texto)` no `erp_base.html`, e serve
qualquer tela daqui para frente.

⚠️ **O que isso quebrou e foi consertado na hora:** a tela de INÍCIO não
recebia `pode` no molde dela. Como o `erp_base.html` passou a ler
`pode.ver_suprimentos`, a porta de entrada do ERP inteiro respondeu 500. Quem
pegou foi a homologação com banco de verdade, antes de sair daqui. Está com
teste próprio agora (`test_a_porta_de_entrada_tambem_conhece_as_permissoes`).

### O Arquivo passou a ler o documento — 10/09/2026

Item 3 da gestão de documentos, o que o dono descreveu como *"um ambiente onde
eu pudesse simplesmente jogar esse documento, ele fosse interpretado, lido, e
a partir dali categorizado, renomeado e salvo"*.

Em Administração › Arquivo › Guardar documento: escolhe o arquivo, aperta
**"Ler o documento"** (com uma dica opcional, tipo "é a CND do FGTS da BWS") e
o formulário volta preenchido — tipo, dono, emissão, validade, competência,
referência e o nome padronizado. A pessoa confere e grava.

Decisões que estão no código e não se mudam sem motivo:

- **Ler não é guardar.** A leitura não grava nada. Documento arquivado no tipo
  errado some do conjunto que o cliente pede na medição, e ninguém descobre
  até o dia da entrega.
- **A pergunta sai do catálogo QUE ESTÁ NO BANCO**, não de uma lista fixa no
  código. Tipo novo criado pela empresa hoje entra na leitura de amanhã.
- **Não achar o dono é resposta válida.** A comparação exige CNPJ/CPF igual ou
  nome que se contenha — nunca "o mais parecido". Quando não acha, a tela diz
  qual nome o documento traz e manda escolher. Pendurar no parecido faria o
  documento sumir da busca de quem procura.
- **Validade anterior à emissão é leitura trocada**: descartada, com a
  confiança rebaixada. Gravá-la faria o aviso de vencimento nascer errado.
- **A leitura diz o que ela mesma não resolveu** ("falta você preencher: até
  quando vale"). Sugestão que se apresenta como certeza é pior do que campo em
  branco, porque ninguém confere.
- **O texto do documento é guardado junto.** Extrair na entrada é barato;
  reprocessar depois, para poder buscar dentro do documento, seria caro. A
  busca do Arquivo já olhava esse campo — agora ele vem preenchido.

Dois buracos que apareceram no caminho e foram fechados: os tipos de documento
de **PESSOA** e de **PARCEIRO** não tinham lista de dono na tela (dizia "este
tipo ainda não tem lista aqui"), então metade do catálogo não tinha onde ser
pendurada. Agora têm, por um endereço próprio do Arquivo — e a **lista de
colaboradores só sai para quem enxerga documento PESSOAL**, a mesma faixa de
sigilo do módulo.

⚠️ **O que NÃO foi verificado:** a chamada real ao serviço de IA. Não há chave
da OpenAI neste ambiente. O caminho de erro foi exercitado no navegador (a
tela diz "leitura automática indisponível — preencha os campos manualmente" e
nada quebra), e o caminho de sucesso foi exercitado ponta a ponta com a IA
dublada: ler → preencher → guardar com o nome padronizado → achar o documento
buscando por uma palavra de DENTRO dele. O que falta provar é o acerto do
modelo contra documento de verdade, e isso só acontece no Render.

### A fila de trabalho pesado — 10/09/2026 (migração 055)

Nasceu da pergunta do dono em 08/09/2026 sobre o sistema aguentar crescer. A
resposta que rende mais não é máquina maior: é **parar de fazer trabalho
pesado enquanto alguém espera a tela**.

O que era trabalho pesado no clique: importar cem cards do Pipefy (cada um com
consulta e anexos para baixar) e recalcular a agenda inteira ao abri-la. Cada
um desses segurava UMA das quatro linhas de atendimento do serviço — que é o
mesmo serviço dos outros treze módulos. O sistema ficava pesado para todo
mundo e ninguém entendia por quê.

Agora o clique enfileira e volta na hora. Decisões que estão no código:

- **A fila vive no BANCO.** O serviço se reinicia sozinho de tempos em tempos
  (a faxina de memória do gunicorn). Fila na memória perderia o trabalho no
  meio, calada.
- **Uma linha de trabalho só.** Duas fariam duas importações grandes disputar a
  mesma máquina de 2 GB — o problema que viemos resolver, com outro nome.
- **Quem morre no meio volta para a fila.** O sinal de vida (`batida_em`) é o
  que separa "está trabalhando" de "morreu". Sem ele um trabalho ficaria
  "executando" para sempre.
- **Tentativa tem teto** (três), e há trabalho que **não repete nenhuma vez**:
  emitir nota. Repetir criaria duas notas de verdade na prefeitura.
- **A agenda abre com o que já está calculado** e manda recalcular por trás;
  quando termina, a lista se refaz sozinha. E dez pessoas abrindo a agenda não
  criam dez recálculos iguais.

Acompanhamento em Configurações › "Trabalhos em segundo plano": o que está na
fila, o que terminou, o que falhou, e o botão de tentar de novo.

⚠️ **A linha de fundo fica DESLIGADA na suíte** (`ERP_TAREFAS=0` no
`tests/conftest.py`): ela atravessaria os testes mexendo no banco por fora da
transação que cada teste desfaz. A fila continua sendo provada — os testes
enfileiram e mandam executar na hora.

### A nota fiscal sai sozinha — 10/09/2026 (migração 056)

Item 6 de `MEDICOES_E_NOTAS.md`, destravado quando Petrolina saiu da conta.
Botão **"Emitir agora"** na medição, dentro do quadro do contrato.

A ordem importa e está no código:

1. **Confere o cadastro ANTES de tocar em número.** Descobrir no meio que falta
   o CNO obrigaria a queimar um número por erro de cadastro. A tela mostra a
   lista do que falta, em português, com onde resolver.
2. **Reserva o número da declaração**, que é NOSSO — no padrão nacional quem
   numera a DPS é quem emite; a prefeitura devolve o número da NOTA.
3. **Assina com o certificado A1 da empresa, em memória.** O `.pfx` nunca vira
   arquivo em disco: arquivo escrito para "só assinar uma nota" sobrevive ao
   processo, entra em backup e vaza a assinatura da empresa.
4. **Envia pelo canal NACIONAL** (o ABRASF tem data para acabar) e guarda o
   número da nota, a chave de acesso, o identificador de processamento e o
   **XML anexado ao título**.
5. **Falhando, o número fica QUEIMADO com o motivo** e não volta para a fila. A
   prefeitura pode ter recebido a declaração e só a resposta ter se perdido.

Duas mensagens de erro, de propósito: o motivo GRAVADO guarda o texto técnico
inteiro (é o que se manda para o suporte da prefeitura); a frase que vai para a
TELA é em português. Despejar "ProxyError: Max retries exceeded" na cara de
quem está faturando não ajuda ninguém a decidir o que fazer.

**O que entrou junto, porque a declaração exigia:** o endereço do tomador. O
cadastro do cliente só tinha município e UF — bastava para pagar, não para
emitir. Agora tem CEP, logradouro, número, bairro e código IBGE, e a **consulta
de CNPJ na Receita preenche sozinha** ("Buscar na Receita" no cadastro do
fornecedor). Ela completa o que está em branco e **não sobrescreve** o que
alguém corrigiu à mão.

⚠️ **A primeira conversa real com a prefeitura só acontece no Render.** Aqui o
serviço do município é dublado nos testes, e no navegador o caminho de erro foi
exercitado de ponta a ponta: número reservado, emissão recusada pela rede
bloqueada, número queimado com motivo, e a tela de notas emitidas mostrando
"Falhou". O caminho de sucesso contra o serviço de verdade, não.

### O documento que nunca foi arquivado — 10/09/2026 (migração 057)

Itens 6 e 7 de `GESTAO_DOCUMENTOS.md`. O aviso que existia olhava para
documento que VAI VENCER. Faltava o outro lado, e é o que faz perder licitação
e atrasar medição: o documento que **nunca entrou**.

A diferença importa. Certidão vencida pelo menos existe e o sistema sabe de
quando é. A ausência é silêncio — ninguém repara até o dia em que o cliente
pede a pasta da medição e ela sai pela metade.

Agora o recálculo da agenda percorre obra por obra e empresa por empresa,
conferindo o bloco FISCAL das duas últimas competências e a HABILITAÇÃO de cada
empresa ativa, e avisa dizendo QUAIS documentos faltam. Só é viável porque o
recálculo passou a rodar em segundo plano (migração 055).

Detalhe que quase passou batido: a conferência do sistema roda **com todas as
faixas de sigilo**. Sem isso ela enxergaria só a faixa aberta e diria que a
pasta fiscal está completa quando está vazia — quase tudo nela é restrito. O
que sai daí é o NOME DO TIPO que falta ("folha de pagamento"), que é entrada de
catálogo, não conteúdo; nenhum documento, nome de pessoa ou valor atravessa.

**Os botões saíram da tela do Arquivo e foram para onde a pessoa está:** na
ficha do título (documentação fiscal daquela obra e competência, medição,
dossiê da obra), na aba Documentos da obra, e na ficha da empresa (habilitação
e cadastro como fornecedor).

⚠️ **Um defeito antigo apareceu no caminho e foi consertado:** quando o
certificado digital virou aviso (migração 053), a lista de origens do filtro da
agenda ficou para trás e o aviso novo não tinha como ser filtrado. Nada
quebrou, então ninguém viu. Agora a lista sai do servidor, de um lugar só.

### O que está pendente AGORA

1. **RESOLVIDO em 08/09/2026 — `ERP_CHAVE_SEGREDOS` está definida no Render.**
   É ela que cifra a senha da conta de e-mail das empresas. ⚠️ **Nunca trocar
   essa chave depois que houver senha guardada**: as senhas antigas viram lixo
   e têm de ser digitadas de novo.
2. **`EL_NFSE_TOKEN` SAIU DA URGÊNCIA (07/09/2026).** Ele pertence ao módulo
   `emissaonf`, que emite nota de serviço e está **em espera** por decisão do
   dono — ver item 15. Enquanto o módulo não for retomado, a variável não faz
   falta a ninguém: o único efeito de não defini-la é o script de consulta de
   NFS-e parar e explicar. **Trocar o token na origem está BLOQUEADO**: o dono
   informou que outra aplicação usa o mesmo token e trocá-lo derrubaria a
   outra junto. Ele segue legível no histórico do Git (`fa985ab`) e apagar do
   histórico não resolveria — quem já clonou continua com ele. Enquanto não
   puder ser trocado, o que reduz o risco é quem tem acesso ao repositório.
   Reabrir quando a outra aplicação puder ter um token próprio.
3. **Definir o teto mensal de IA** em Configurações › Consumo de IA.
4. **Homologação por perfil**: a parte mecânica (o que abre e o que é
   recusado, tela a tela, perfil a perfil) roda sozinha no GitHub a cada envio
   (`tests/test_homologacao_banco.py`). Para o olho humano ficou só o roteiro
   reduzido: visual, leitura de documento por IA, avalizar/pagar com dado real.
5. **RESOLVIDO em 07/09/2026 — o plano de contas fala a língua da BWS.** O
   dono conferiu o de-para, recusou as junções e os nomes novos, e o plano foi
   refeito com os 32 nomes da planilha dele, um para um. **Falta ele apertar
   "Aplicar plano de contas"** em Configurações — o código já está publicado, e
   sem esse botão as contas em produção continuam com os nomes antigos.
6. **Como começar a operar Suprimentos**: construído e com as telas de cadastro refeitas, mas
   **ainda não operado contra a base real** — é o que o dono precisa fazer
   primeiro. Caminho sugerido: Cadastros › Importações › **Dados de exemplo**
   para simular o fluxo inteiro sem digitar nada, e depois `Remover os dados
   de exemplo` antes de trazer os 111 fornecedores e os 115 insumos de
   verdade (exportar a aba da planilha como CSV e usar a prévia antes de
   gravar). **A carga não cria categoria de insumo** — cadastre as categorias
   primeiro, senão os fornecedores entram sem saber o que vendem e não
   recebem cotação nenhuma.
7. **Decidido em 06/09/2026**: o e-mail sai por SMTP da própria empresa, e
   cada empresa tem a sua conta. Falta o dono preencher servidor, porta,
   usuário e senha de aplicativo em Administração › Empresas, e apertar
   "Mandar mensagem de teste". **Continua em aberto**: se um dia a empresa
   quiser saber que o fornecedor RECEBEU (e não só que o servidor aceitou),
   isso exige um serviço de envio com retorno — outro custo, outra decisão.
8. **RESOLVIDO em 07/09/2026 — AUTORIZAÇÃO é BRANCA.** O dono confirmou: "não
   tinha porque era branco". Era a única das 15 cores inventada por mim. O selo
   ficou branco com um fio de contorno, senão sumiria no fundo claro da tela.
   As 15 cores agora vêm todas da planilha dele.
9. **Fila combinada com o dono**. Entregues: (a) os quatro relatórios do mapa
   — R2 resumo por item, R3 por fornecedor, R4 melhor fornecedor único, R5
   comparativo — mais o mapa em PDF deitado (publicado em 06/09); (b) previsão
   de devolução por equipamento e a conferência mensal de locação (07/09,
   **ainda em ramo, não publicado**). Falta, nesta ordem: (c) **o celular**, que
   passou à frente em 07/09 porque o agente manda link e o link tem de cair
   numa tela usável; (d) o agente de cobrança por WhatsApp — ele vai atrás de
   quem não respondeu a conferência, e depois serve ao sistema todo; (e)
   relatório de compras por obra, abrindo até o insumo; (f) despesa com
   colaboradores, testada e mostrada como se fez em Suprimentos.
10. **DECIDIDO em 07/09/2026 — o sistema vai ser adaptado ao celular.** O dono
   respondeu "é melhor adaptar o sistema ao uso via celular também". Não é
   mais um "se": virou dependência do item 13, porque a conferência da locação
   vai ser respondida NO SISTEMA, a partir de um link que chega por WhatsApp —
   e quem recebe esse link está no canteiro, com o telefone na mão. Ordem
   combinada: primeiro fazer as telas atuais caberem no celular (nada quebra,
   tudo fica legível), depois telas próprias para o punhado de coisas que se
   fazem mesmo de pé: responder a conferência, autorizar, consultar. Medição
   de partida: seis telas testadas ocupam 687px numa tela de 390px.
11. **RESOLVIDO em 07/09/2026 — o Departamento Pessoal enxerga O MUNDO DELE.**
   A decisão veio em duas partes, e a segunda corrigiu a primeira. Primeiro:
   "a trava de visualização é semelhante ao do financeiro" — ou seja, ele não
   pode ficar preso ao que ele mesmo lançou, porque a despesa que ele revisa
   foi lançada PELA OBRA, nunca por ele. Eu implementei isso como "vê tudo".
   Ele então perguntou: "o departamento pessoal me chega no financeiro das
   coisas relacionadas ao departamento pessoal, é isso?" — que é mais estreito
   do que eu tinha feito. **Vale o mais estreito**: ele enxerga folha e
   encargos, RPA, reembolso a colaborador e todo título nascido de uma despesa
   com colaborador, em TODAS as obras e lançado por QUALQUER pessoa. Compra de
   material não é assunto dele. Regra geral que ficou: instrução que comporta
   duas leituras se implementa pela que mostra MENOS — abrir depois é uma
   linha; fechar depois é conversa constrangedora sobre quem viu o que não
   devia. **Enxergar não é poder**: continua sem `aprovar`, `pagar`,
   `conciliar` e `ver_dados_pagamento`, com teste que quebra se alguém ampliar
   a alçada junto com a visão.
12. **RESOLVIDO em 08/09/2026 — as duas já estão no Render.** Ficam descritas
   abaixo para quem precisar entender o que cada uma faz (07/09/2026):
   `ERP_AGENTE_SECRET` (qualquer texto longo e secreto — sem ela a rotina
   recusa, de propósito) e `ERP_URL_PUBLICA` (o endereço do ERP, ex.
   `https://erp.bwsconstrucoes.com.br`) — sem esta o link da mensagem sai
   relativo e não abre no WhatsApp. **E falta agendar a chamada diária** de
   `POST /erp/api/agente/rodar` com `{"secret": "…"}` no corpo. Antes de soltar,
   rodar uma vez com `{"simular": true}` e ler o que ele mandaria.
13. **Marcar quem responde por cada obra, e pôr o telefone dele.** A tela
   existe desde 07/09 (no cadastro da obra e no do operador, dá no mesmo). Sem
   ninguém marcado, a conferência abre sem dono; sem telefone, o agente pula a
   pessoa dizendo o nome dela. **Precisa da migração 041**, que vai nesta
   publicação.
14. **RESOLVIDO — a migração 039 foi aplicada em 07/09/2026.** O dono
   confirmou. Fica a lição de operação: logo depois de publicar, o botão pode
   não mostrar migração nenhuma, porque o Render ainda está subindo o código
   novo. Esperar um ou dois minutos e olhar de novo. **A 040 (o agente) entra
   junto com esta publicação e precisa do botão de novo.**
15. **DECIDIDO em 07/09/2026 — o agente SÓ AVISA, e a escada está fechada.**
   A resposta é dada no sistema, completa, não por mensagem. Razão do dono:
   "responder as perguntas mais completas, até porque, por obra, sei lá, se
   tiver cinco, dez contratos de locação é algo que dá pra ser feito". **A
   escada de cobrança foi aprovada como proposta**: lembrete no dia 5 do mês
   seguinte, cobrança no dia 10, e no dia 15 a lista de quem não respondeu
   sobe para o dono e para o financeiro. Ficam de fora, por consequência:
   interpretar texto livre de mensagem (uma leitura errada de "acho que dá pra
   devolver" mexeria no contrato de verdade), depender de número de telefone
   para saber quem respondeu, e deixar o sistema escutando mensagem de fora.
16. **DECIDIDO em 07/09/2026 — quem responde a conferência sai do CADASTRO DA
   OBRA.** O dono: "no cadastro da obra, a gente vai associar uma das pessoas,
   um dos operadores, pra responder por aquela obra… e se por acaso tiverem
   dois, a gente cadastrar dois, permitir também, os dois recebem". E a
   associação tem de poder ser feita **pelos dois lados** — pelo cadastro do
   operador ou pelo cadastro da obra —, "porque facilita o manuseio do
   sistema". Ainda não construído; é o próximo trabalho.
17. **NOVO E VITAL — as três conciliações de entrada**, escritas inteiras em
   `ENTRADAS.md`, nesta pasta: baixa pelo comprovante de pagamento (duas
   portas: e-mail e tela do ERP), monitoramento das caixas de compras e
   financeiro (boleto, nota de débito, nota fiscal, cobrança — com o e-mail
   marcado como tratado na própria caixa e confirmação humana antes de valer),
   e o cruzamento das notas. As três seguem o mesmo desenho da conciliação
   bancária: casa o que é certo, expõe o duvidoso, alerta o que não casou com
   nada. **`app/apps/baixabradesco/` já resolve o casamento do comprovante** —
   com parsers de Bradesco, Sicredi, BeeVale e SomaPay — mas escreve no mundo
   antigo (Omie, Pipefy, planilha): o que se aproveita são os parsers e a
   lógica, não o destino, e ele NÃO pode ser quebrado, está em produção.
18. **NOVO E GRANDE — o cruzamento de notas fiscais.** Ditado pelo dono em
   07/09/2026 e escrito inteiro em `NOTAS_FISCAIS.md`, nesta pasta. Em uma
   frase: capturar todas as notas emitidas contra os CNPJs da empresa e cruzar
   cada uma com pedido de compra, título financeiro e prestação de fundo fixo,
   no espírito da conciliação bancária — o sistema casa o que consegue e expõe
   o duvidoso para uma pessoa confirmar. **O que não pode ser esquecido:** um
   pedido gera N notas (dez carradas de brita são dez notas e dez boletos), e
   qualquer desenho que assuma um-para-um nasce errado. Depende de certificado
   digital por empresa (cifrado, como a senha de e-mail) e traz junto a agenda
   de alertas. Quatro perguntas ainda esperam o dono — estão no §9 de lá.
19. **RETOMADA em 09/09/2026 — a emissão de NFS-e saiu da espera**, com
   desenho próprio ditado pelo dono e escrito em `MEDICOES_E_NOTAS.md`. Ela
   deixa de ser "o módulo antigo" e passa a nascer do lado a RECEBER: a
   medição do contrato emite a nota. O texto abaixo é de 07/09/2026 e fica
   como registro do que era antes. É
   módulo antigo do monorepo (`app/apps/emissaonf/`), que emite nota de
   SERVIÇO da empresa para o cliente dela — coisa diferente do cruzamento do
   item 14, que captura nota que o FORNECEDOR emite contra a empresa. Nunca
   foi trabalhado nestes chats e não foi verificado por mim. O
   `EL_NFSE_TOKEN` pertence a esse módulo parado, e por isso saiu da lista de
   urgências. Retomar quando ele pedir.

20. **RESOLVIDO em 08/09/2026 — as quatro variáveis foram criadas no Render
   pelo dono, e o deploy foi feito.** `ERP_CHAVE_SEGREDOS`,
   `ERP_AGENTE_SECRET`, `ERP_COMPROVANTE_SECRET` e `ERP_URL_PUBLICA` estão
   definidas. Falta conferir, quando o agente rodar pela primeira vez, se o
   link que chega na mensagem abre a tela certa — é o único jeito de saber se
   a `ERP_URL_PUBLICA` está com o endereço certo.

21. **RESOLVIDO em 09/09/2026 — as migrações 042 a 053 foram aplicadas**, em
   duas publicações no mesmo dia: a 042–051 primeiro, a 052 e a 053 em
   seguida. O dono publicou e apertou o botão no mesmo momento das duas
   vezes.

   Do que cada uma trouxe, para consulta: 042 a trava contra baixa em
   duplicidade; 043 o documento morando no Drive; 044 o cruzamento de notas;
   045 o arquivo de documentos; 046 os blocos; 047 a chave Pix e os dados de
   emissão por empresa; 048 o controle da numeração das notas; 049 o tipo da
   medição, a correlação do reajuste e o protocolo; 050 a data-base do
   reajuste e a tabela do INCC; 051 a agenda de obrigações.

23. **Criar a pasta do Drive e colar o endereço** em Configurações › "Onde
   ficam os documentos", apertar "Testar a pasta" e só então ligar a chave.
   Pasta num Drive compartilhado da empresa, com a conta de serviço do sistema
   como editor. Enquanto isso não acontecer, tudo segue guardando no banco,
   como sempre foi.

22. **Apontar o cenário do Make** para o endereço de lote dos comprovantes. A
   senha (`ERP_COMPROVANTE_SECRET`) já está no Render; falta o Make usá-la.

---

## Regras que não se discutem

### 1. Nada que rode antes de toda rota depende do ORM
A guarda de permissão (`before_request`) e o login leem o perfil por **SQL
direto** (`_perfil_bruto`). Motivo: em 02/09/2026 o código subiu com uma coluna
nova no modelo `Usuario` antes da migração ser aplicada; a guarda carregava o
`Usuario` pelo ORM e estourava em toda tela — inclusive na do botão que
aplicaria a migração. Impasse circular; ERP fora do ar. Há teste que segura.

### 2. Migração só pelo botão, nunca no boot
Uma migração com defeito no boot derrubaria os 15 módulos. E ao juntar ramo com
migração, o dono aperta o botão **no mesmo momento**.

### 3. Autorização: o padrão é NEGAR
Toda rota declara a ação; rota com id de registro confere escopo; fora do
escopo responde 404, nunca 403. Dois testes estruturais derivam isso do código.
Detalhe em `CONTEXTO.md` §3.9.

### 4. Escopo é UM caminho só
Listagem e detalhe passam pelo mesmo `aplicar_escopo`. Provado com banco de
verdade em `tests/test_escopo_banco.py`.

### 5. A produção não é alcançável a partir dos testes
`ERP_TEST_DATABASE_URL` só é aceita local e com "teste" no nome.

### 6. Como falar com o dono
Ele não é programador. Português simples, efeito antes de implementação, risco
e escolha sempre explícitos. Ver o topo do `CLAUDE.md`.

---

## Decisões já tomadas — não reabrir sem motivo novo

- **Alcance do operador é configuração por pessoa**, não regra do cargo
  (`escopo_visao`: PROPRIOS ou OBRAS_DESIGNADAS). Padrão: o mais restritivo.
- **Consumo de IA registra no ponto único do leitor**, em sessão própria, e
  o teto **só avisa** (Telegram aos ADMIN a 80% e a 100%). Nunca bloqueia.
- **Banco de teste no GitHub Actions**, não no PC nem num segundo banco no
  Render. Único caminho que também funciona para sessões do Claude na nuvem.
- **Fora do escopo = 404.** 403 num id que existe seria um oráculo.

---

## Incidentes

- **02/09/2026 — ERP fora do ar por banco atrasado.** Ver regra 1. Causa:
  outro chat juntou o ramo na `main` sem apertar o botão. Corrigido no mesmo
  dia; a tela "banco desatualizado" (503) substitui a página branca.
- **02/09/2026 — juntar na `main` matou a carga do painel OMIE.** Publicar
  reinicia o serviço. Regra em `app/apps/painel/HISTORICO.md`: perguntar
  antes de juntar.
- **05/09/2026 — três telas de Suprimentos nunca funcionaram, e ninguém viu.**
  Cotações, Pedidos e Banco de preços declaravam `function moeda(...)` por
  cima do `const moeda` da base. Em JavaScript isso é **erro de sintaxe**: o
  bloco inteiro de script da tela deixa de rodar, e ela abre só com o
  cabeçalho. O servidor respondia 200 com o HTML certo, então **nenhum dos
  2.000 testes via nada** — o erro só existe no navegador. Era isso que estava
  por trás do "não tem nada de mapa de cotação eu acho" do dono. Junto vieram
  `data(...)` em Pedidos e `adicionarSPsAoLote(...)` em Pagamentos: duas
  funções chamadas e nunca escritas.
  **Como se acha isso:** subir a aplicação num banco local e percorrer as
  telas num navegador de verdade, ouvindo o console. Meia hora. Está descrito
  em "Olhar as telas num navegador", abaixo.
- **05/09/2026 — publiquei o Banco de preços com erro 500.** Faltou um
  `{% endblock %}`. A homologação por perfil percorria só quatro perfis do
  roteiro e nenhum deles tem "comprar", então para essa tela ela conferia
  apenas o 403 — a página nunca chegava a ser desenhada. Corrigido no mesmo
  dia, com um caso por tela (`test_toda_tela_e_desenhada_por_quem_pode_abrir`)
  que abre cada uma das 27 telas com um perfil que PODE abri-la. Lição: teste
  que só confere recusa não prova que a tela existe.
- **05/09/2026 — inventei dados de exemplo em vez de ler as planilhas do
  dono.** Ele pediu "veja a minha planilha de novo de solicitações, veja os
  insumos"; eu escrevi cimento, areia e brita de cabeça. Refeito com os dados
  de verdade. Lição: quando o pedido cita uma fonte, a fonte é para ser lida.
- **05/09/2026 — Suprimentos entregue intransitável.** Não havia tela para
  criar categoria de insumo, e sem categoria não se cadastra insumo. A suíte
  passava inteira: ela cobria as regras de cada peça, nenhum teste percorria
  o caminho de quem chega numa base vazia. Corrigido no mesmo dia, com um
  teste que faz exatamente esse percurso contra Postgres de verdade
  (`tests/test_suprimentos_cadastro_banco.py`). Lição: regra testada não é
  fluxo testado — falta o caso do primeiro dia, com a base vazia.
- **01/09/2026 — painel de consumo de IA nunca funcionou.** A função de
  registro não existia e o painel não tinha lugar na tela. Lição: entrega que
  ninguém abriu na tela não foi entregue.

---

## O que falta (além do pendente AGORA)

- Consistência transacional: os quatro casos da `AUDITORIA_TRANSACIONAL.md`
  ganharam trava de linha (FOR UPDATE) e restrições únicas no banco
  (migração 031) em 03/09/2026. Ficaram de fora, ainda sem olhar: fechamento
  da prestação do fundo fixo, parcelas de locação, estorno, importação OFX,
  cancelamento em lote.
- Retenção de garantia na empreita; BeeVale/SomaPay; Suprimentos; Agenda —
  ver `ROTEIRO.md`.

## Olhar as telas num navegador

A suíte não abre tela nenhuma no navegador, e foi por aí que três telas mortas
chegaram à produção. Vale meia hora antes de publicar mudança de tela:

```
# 1. um Postgres descartável e um banco com o plano, um ADMIN e umas obras
initdb / pg_ctl start -o '-p 5433'   &&  createdb erp_olhada
DATABASE_URL=...erp_olhada  python  (schema.sql + aplicar_pendentes + aplicar_plano)

# 2. a aplicação apontada para ele
DATABASE_URL=...erp_olhada ERP_SECRET_KEY=qualquer PORT=5055 python app/main.py

# 3. o navegador, ouvindo o console
#    (Chromium já vem instalado em /opt/pw-browsers/chromium)
playwright: page.on("pageerror") e page.on("console") → qualquer erro é defeito
```

O que só isso mostra: erro de JavaScript que mata a tela inteira, tela que
carrega em branco, botão que não responde, número formatado errado. Nenhuma
dessas coisas aparece num teste que só olha o HTML que o servidor mandou.

## Coisas pequenas que mordem

- `pip install -r requirements-dev.txt` falha com o pip antigo do sistema
  (`docopt` não compila). Num venv com pip atualizado funciona.
- `pytest -q` na linha de comando vira `-qq` (o `pytest.ini` já tem `-q`) e
  esconde a linha de resumo. Rode sem `-q`.
- A sessão dublada dos testes ignora `WHERE`: regra de escopo nova ganha um
  caso em `tests/test_escopo_banco.py`, não só no dublê.
- **Antes de acreditar numa medição de tela, confira se está logado.** Uma
  varredura inteira mediu a tela de LOGIN 14 vezes e disse "está tudo certo",
  porque o Postgres do contêiner tinha caído e o login falhava em silêncio. O
  que salvou foi abrir a captura de tela e olhar. Medida sem foto engana.
- **O Postgres do contêiner cai sozinho** (memória). Religar com
  `pg_ctl -D /var/lib/postgresql/erpteste -o '-p 5433 …' start`.
- **Nome que não existe, endereço que não existe.** Duas varreduras rodam
  junto com a suíte e recusam as duas coisas:
  `tests/test_nomes_indefinidos.py` (nenhuma função pode citar um nome que o
  Python não vá achar) e `tests/test_telas_chamam_rota_que_existe.py` (nenhuma
  tela pode pedir um endereço que o servidor não tem). As duas nasceram de
  defeito que chegou à produção calado.
- **Dinheiro e número escritos pelo servidor** saem por
  `core/comum/formato.py` (`_dinheiro_br`, `_quantidade_br`); na tela, por
  `moeda` e `numero` do `erp_base.html`. Não escrever `f"R$ {v:.2f}"` — isso é
  formato americano, e o dono lê o sistema em português.
- **Ao melhorar uma tela, PROCURE a versão antiga antes de escrever a nova.**
  Acrescentar a função nova por cima não substitui a velha: em JavaScript a
  declaração DE BAIXO vence, calada, e a melhoria vira código morto. Foi assim
  que a ficha do lote ficou oito dias com o comportamento antigo. Agora
  `tests/test_telas_sem_funcao_repetida.py` recusa isso.
- **Nunca declare na tela um nome que a base já declara** (`moeda`, `numero`,
  `els`, `api`, `dataBR`…). Não é "a última vence": é erro de sintaxe e a tela
  inteira morre. `tests/test_telas_javascript.py` recusa isso agora.
- O encurtador trata `/favicon.ico` como código curto e vai ao Google Sheets a
  cada pedido do navegador. Não derruba nada, mas é uma ida à rede por aba
  aberta. Fica anotado — é outra área.
