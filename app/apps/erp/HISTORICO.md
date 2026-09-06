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

### O que está pendente AGORA

1. **Apertar "Aplicar atualizações do banco"** (Configurações, como ADMIN) para
   as migrações 029 e 030. Enquanto não apertar, o ERP mostra a tela "O banco
   está desatualizado". **Pergunte ao dono se já apertou.**
2. **Definir `ERP_CHAVE_SEGREDOS` na Environment do Render** — é ela que cifra
   a senha da conta de e-mail das empresas. Gera-se uma vez com
   `python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"`.
   Sem ela tudo funciona, menos guardar senha de e-mail.
3. **Definir `EL_NFSE_TOKEN` na Environment do Render** (token da prefeitura,
   que estava colado no código) e **trocar o token na origem** — ele continua
   no histórico do Git, commit `fa985ab`.
4. **Definir o teto mensal de IA** em Configurações › Consumo de IA.
5. **Homologação por perfil**: a parte mecânica (o que abre e o que é
   recusado, tela a tela, perfil a perfil) roda sozinha no GitHub a cada envio
   (`tests/test_homologacao_banco.py`). Para o olho humano ficou só o roteiro
   reduzido: visual, leitura de documento por IA, avalizar/pagar com dado real.
6. **Migrações 031 a 038**: apertar o botão ao juntar. A 031 são as restrições
   de concorrência; a 032 é a tabela das permissões por pessoa. Enquanto a 032
   não rodar, o ERP funciona normalmente **pelo cargo** — a tela de cadastro é
   que não consegue mostrar os ajustes.
7. **Suprimentos**: construído e com as telas de cadastro refeitas, mas
   **ainda não operado contra a base real** — é o que o dono precisa fazer
   primeiro. Caminho sugerido: Cadastros › Importações › **Dados de exemplo**
   para simular o fluxo inteiro sem digitar nada, e depois `Remover os dados
   de exemplo` antes de trazer os 111 fornecedores e os 115 insumos de
   verdade (exportar a aba da planilha como CSV e usar a prévia antes de
   gravar). **A carga não cria categoria de insumo** — cadastre as categorias
   primeiro, senão os fornecedores entram sem saber o que vendem e não
   recebem cotação nenhuma.
8. **Decidido em 06/09/2026**: o e-mail sai por SMTP da própria empresa, e
   cada empresa tem a sua conta. Falta o dono preencher servidor, porta,
   usuário e senha de aplicativo em Administração › Empresas, e apertar
   "Mandar mensagem de teste". **Continua em aberto**: se um dia a empresa
   quiser saber que o fornecedor RECEBEU (e não só que o servidor aceitou),
   isso exige um serviço de envio com retorno — outro custo, outra decisão.
9. **Confirmar com o dono a cor de AUTORIZAÇÃO** — é a única das 15 que não
   veio da planilha (a cópia de onde as cores saíram não tem esse status).
   Trocar mexe só no bloco `.sit-` do `erp.css`.
10. **Decisão do dono**: o Departamento Pessoal vê todas as despesas com
   colaborador, mas na lista de Títulos só o que ele lançou. É assim que deve
   ser? (item 4 do roteiro de homologação)

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
- **Nunca declare na tela um nome que a base já declara** (`moeda`, `numero`,
  `els`, `api`, `dataBR`…). Não é "a última vence": é erro de sintaxe e a tela
  inteira morre. `tests/test_telas_javascript.py` recusa isso agora.
- O encurtador trata `/favicon.ico` como código curto e vai ao Google Sheets a
  cada pedido do navegador. Não derruba nada, mas é uma ida à rede por aba
  aberta. Fica anotado — é outra área.
