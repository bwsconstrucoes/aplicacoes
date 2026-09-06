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

### O que está pendente AGORA

1. **Apertar "Aplicar atualizações do banco"** (Configurações, como ADMIN) para
   as migrações 029 e 030. Enquanto não apertar, o ERP mostra a tela "O banco
   está desatualizado". **Pergunte ao dono se já apertou.**
2. **Definir `EL_NFSE_TOKEN` na Environment do Render** (token da prefeitura,
   que estava colado no código) e **trocar o token na origem** — ele continua
   no histórico do Git, commit `fa985ab`.
3. **Definir o teto mensal de IA** em Configurações › Consumo de IA.
4. **Homologação por perfil**: a parte mecânica (o que abre e o que é
   recusado, tela a tela, perfil a perfil) roda sozinha no GitHub a cada envio
   (`tests/test_homologacao_banco.py`). Para o olho humano ficou só o roteiro
   reduzido: visual, leitura de documento por IA, avalizar/pagar com dado real.
5. **Migrações 031 a 037**: apertar o botão ao juntar. A 031 são as restrições
   de concorrência; a 032 é a tabela das permissões por pessoa. Enquanto a 032
   não rodar, o ERP funciona normalmente **pelo cargo** — a tela de cadastro é
   que não consegue mostrar os ajustes.
6. **Suprimentos**: construído e com as telas de cadastro refeitas, mas
   **ainda não operado contra a base real** — é o que o dono precisa fazer
   primeiro. Caminho sugerido: Cadastros › Importações › **Dados de exemplo**
   para simular o fluxo inteiro sem digitar nada, e depois `Remover os dados
   de exemplo` antes de trazer os 111 fornecedores e os 115 insumos de
   verdade (exportar a aba da planilha como CSV e usar a prévia antes de
   gravar). **A carga não cria categoria de insumo** — cadastre as categorias
   primeiro, senão os fornecedores entram sem saber o que vendem e não
   recebem cotação nenhuma.
7. **Decisão pendente do dono**: por qual conta sai o e-mail de cotação. O
   monorepo tem WhatsApp (Z-API) mas **não tem envio de e-mail** — e 109 dos
   111 fornecedores só recebem cotação por e-mail. Enquanto isso não se
   decide, o disparo automático não existe.
8. **Decisão do dono**: o Departamento Pessoal vê todas as despesas com
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
