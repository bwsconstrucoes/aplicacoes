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
Suprimentos ainda não começou.

**Estado em 04/09/2026:** `main` com a autorização padrão-NEGAR, o alcance por
operador (029), o consumo de IA com teto (030), as travas de concorrência (031)
e a homologação por perfil rodando sozinha no GitHub. No ramo, ainda não
publicado: a **permissão fina por pessoa** (032) e a **especificação de
Suprimentos**. Suíte: 836 sem banco, ~1.400 com banco.

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
5. **Migrações 031, 032 e 033**: apertar o botão ao juntar. A 031 são as restrições
   de concorrência; a 032 é a tabela das permissões por pessoa. Enquanto a 032
   não rodar, o ERP funciona normalmente **pelo cargo** — a tela de cadastro é
   que não consegue mostrar os ajustes.
6. **Suprimentos**: a fase 1 está pronta (migração 033) — cadastros, a regra
   das condições de pagamento, a tela Suprimentos › Cadastros e a carga por
   CSV. Para trazer os 111 fornecedores e os 115 insumos, o dono exporta a aba
   da planilha como CSV e usa a prévia antes de gravar. Falta ligar a
   solicitação de cadastro de insumo à tela: a tabela existe, o fluxo não.
   A fase 2 (solicitação de suprimentos) é o próximo passo.
7. **Decisão do dono**: o Departamento Pessoal vê todas as despesas com
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

## Coisas pequenas que mordem

- `pip install -r requirements-dev.txt` falha com o pip antigo do sistema
  (`docopt` não compila). Num venv com pip atualizado funciona.
- `pytest -q` na linha de comando vira `-qq` (o `pytest.ini` já tem `-q`) e
  esconde a linha de resumo. Rode sem `-q`.
- A sessão dublada dos testes ignora `WHERE`: regra de escopo nova ganha um
  caso em `tests/test_escopo_banco.py`, não só no dublê.
