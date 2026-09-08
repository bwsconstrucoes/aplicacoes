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

**Estado em 05/09/2026 (noite):** `main` com a autorização padrão-NEGAR, o
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
19. **A emissão de NFS-e fica em espera, por decisão do dono (07/09/2026).** É
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

21. **APERTAR "Aplicar atualizações do banco" para a migração 042** (a trava
   contra baixa em duplicidade), assim que o ramo entrar na `main`. Sem ela o
   ERP sobe, mas anexar comprovante pela tela dá erro — a tabela da trava não
   existe ainda. É o mesmo botão de sempre, em Configurações.

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
- **Nunca declare na tela um nome que a base já declara** (`moeda`, `numero`,
  `els`, `api`, `dataBR`…). Não é "a última vence": é erro de sintaxe e a tela
  inteira morre. `tests/test_telas_javascript.py` recusa isso agora.
- O encurtador trata `/favicon.ico` como código curto e vai ao Google Sheets a
  cada pedido do navegador. Não derruba nada, mas é uma ida à rede por aba
  aberta. Fica anotado — é outra área.
