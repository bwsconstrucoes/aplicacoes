# Ponto eletrônico próprio (REP-P) — PLANO da fase 1

> **Estado em 03/10/2026: EXECUTADO.** O dono respondeu ao plano com uma
> correção de princípio — *"não temos que adaptar o que temos ao Mobponto; é o
> inverso: criamos uma solução robusta, definitiva, e as outras se conectam a
> ela"* — e as recomendações abaixo seguiram como padrão. O que mudou com a
> correção: a consulta da API devolve o formato DO PONTO (uma linha por batida
> + resumo por dia), não as 4 colunas do Mobponto; quem consome se adapta. O
> que foi entregue está no `README.md` e no `HISTORICO.md`. Segunda correção
> dele, no mesmo dia: **foto no Google Drive, não no banco** ("lá o espaço é
> virtualmente infinito; na base de dados não") — a §2.2 abaixo ficou superada
> e a decisão vale como está no `README.md`.

Escrito em 03/10/2026, no ramo `feature/modulo-ponto`, **antes de qualquer
código**, para o dono aprovar. A especificação de partida foi a que o dono
colou de outro chat; o que está aqui é ela confrontada com o que o repositório
já tem.
Quando a fase 1 estiver pronta, este arquivo vira `README.md` + `HISTORICO.md`,
no padrão das outras áreas.

## 1. O que já existe no repositório — e muda o desenho

Nenhum destes pontos estava na especificação de partida. Todos foram lidos do
código em 03/10/2026.

| Achado | Onde | Efeito no ponto |
|---|---|---|
| **A empresa já bate ponto no Mobponto**, e a Análise de SPs carrega esse ponto mês a mês (4 batidas por dia, obra de cada batida) para apropriar a folha por obra. É "o gargalo de tudo na folha". | `analisesps/ponto.py`, tabelas `analisesps.ponto_carga` / `ponto_dia` | O ponto próprio é o **substituto** do Mobponto — e a fonte da verdade. A folha vai passar a ler daqui (uma linha por batida, com obra e status); o formato é o do ponto, não o do Mobponto. |
| **A Análise de SPs não é mais Streamlit.** Ela virou blueprint deste monorepo (`/analisesps`), no mesmo processo e no mesmo Postgres. | `analisesps/__init__.py` | Ela pode ler o ponto **direto no banco**, sem API nem chave. A API REST continua útil para o PWA, o iDFace e sistemas de fora — não para ela. |
| **Obras já existem no ERP, com latitude e longitude** (migração 024). | tabela `obras` | Criar `ponto_obras` seria a 2ª cópia do cadastro de obras. |
| **Colaboradores já existem no ERP** (CPF único, obra principal, situação, função), e há uma 3ª cópia espelhada do Pipefy em `analisesps.colaborador`. | tabela `colaboradores` (migração 026); `analisesps` 028 | Criar `ponto_colaboradores` seria a **3ª cópia** do cadastro de pessoas. Pessoa que troca de obra no ERP não trocaria no ponto. |
| **Decisão do dono de 18/09/2026: "100% da movimentação no ERP, nada de sistema paralelo"** (`CONTEXTO.md` §9). | — | Um módulo com cadastros próprios de obra e pessoa é exatamente um sistema paralelo. |
| **Precedente de 24/09/2026: a Análise de SPs IMPORTA código do ERP** em vez de copiar, com teste guardando o contrato. | `CONTEXTO.md` §9 | O ponto pode usar a conexão e os cadastros do ERP do mesmo jeito. |
| O disco do Render **não guarda arquivo**: cada publicação ou reinício (a cada ~1000 acessos) apaga o que foi salvo localmente. Anexos do ERP vão para o banco (migração 010) ou para o Drive (043). | `CONTEXTO.md` §9, Procfile | "Salvar foto localmente" perderia as fotos. |
| Migração **nunca roda no start** do gunicorn; roda por botão ou script, cada arquivo na própria transação; cada área tem seu `schema` e sua pasta. | `erp/core/comum/migracoes.py`, `analisesps/migracoes_runner.py` | O ponto segue igual: schema `ponto`, pasta própria, runner próprio. |
| Rota sem declaração de permissão é **recusada** pelo guarda, não liberada. Segredo é comparado com `compare_digest`. | `erp/core/auth`, `analisesps/auth.py` | A API do ponto nasce com a mesma disciplina. |
| Testes: regra pura sem banco; o que depende de SQL roda com Postgres descartável (`@pytest.mark.banco`), em paralelo. | `tests/conftest.py`, `.github/workflows/testes.yml` | Dois arquivos de teste, um de cada tipo. |
| Fuso: a casa usa UTC−3 (`America/Sao_Paulo` nas telas, `America/Fortaleza` no `sync_logs`). | `analisesps/horario.py`, `sync_logs/config.py` | `America/Fortaleza`, como pedido — hoje é a mesma hora. |

## 2. As decisões que são do dono

### 2.1 Cadastro de obras e pessoas: reusar o ERP ou criar cópia própria?

**Recomendação: reusar.** O módulo `ponto` **não cria** `ponto_obras` nem
`ponto_colaboradores`. Ele aponta para `obras` e `colaboradores` do ERP e
guarda, em tabelas suas, **só o que o ERP não tem**:

- `ponto.obra_config` → obra_id, raio_metros (padrão 200), ativo_para_ponto;
- `ponto.colaborador_config` → colaborador_id, tipo_jornada, foto_cadastral,
  ativo_para_ponto;
- `ponto.colaborador_obras` → as obras adicionais onde a pessoa pode bater.

Latitude e longitude ficam onde já estão (`obras`). CPF, nome, obra principal
e situação (ativo/desligado) vêm do ERP e **não se digitam duas vezes**.

O preço: o importador de pessoas precisa **criar no ERP** quem ainda não
estiver lá (nome, CPF, obra) — é escrita numa tabela do ERP, por isso só com o
seu "pode". E o ponto fica amarrado ao ERP: se o ERP renomear uma coluna, o
ponto sente. O teste de contrato (como o de 24/09) avisa antes de chegar à
produção.

**Alternativa (a da especificação colada):** tabelas próprias
`ponto_obras` e `ponto_colaboradores`, chave pelo CPF. Mais rápido de fazer,
zero toque no ERP — e três cadastros de pessoa para manter em sincronia à mão.

### 2.2 Foto da batida: onde guardar? — SUPERADA: vai para o Drive

**Recomendação original:** guardar a foto **no banco** (`ponto.marcacao_foto`, bytes +
hash SHA-256, teto de 300 KB por foto, redimensionada no servidor), porque o
disco do Render não guarda nada. Drive vem em fase futura, como a especificação
já previa. Só o hash, sem a foto, deixa a prova de identidade sem conteúdo.

Conta de espaço: 400 pessoas × 4 batidas × 22 dias × ~100 KB ≈ **3,5 GB por
mês** se todo mundo mandar foto. Isso é mais que o banco atual. Por isso a
proposta inclui um **prazo de guarda** configurável (ex.: 90 dias; depois fica
só o hash) e a foto é **opcional** na fase 1. Se preferir não guardar foto
nenhuma por enquanto, é tirar uma tabela.

### 2.3 Batida fora da cerca: recusar ou aceitar e marcar?

A especificação colada diz "batida fora das regras é rejeitada". Proponho separar:

- **Recusar** quando o problema é de **identidade**: aparelho não aprovado,
  aparelho não autorizado para a pessoa, pessoa inativa, obra não permitida
  para o aparelho. Cada recusa fica registrada em `ponto.recusas` (quem, quando,
  por quê) — sem isso ninguém investiga um aparelho clonado.
- **Aceitar e marcar `EM_ANALISE`** quando o problema é de **lugar ou relógio**:
  fora do raio da obra, ou relógio do celular muito diferente do servidor. A
  Portaria 671 veda ao empregador impedir a marcação do empregado; recusar a
  batida de quem está a 250 m da obra cria um passivo trabalhista. Fica para
  o encarregado confirmar ou rejeitar (fase 2, na tela).

### 2.4 Quem assina a batida do celular?

Chave de API serve para sistema (Análise de SPs, iDFace, planilha). **Não serve
para celular**: um PWA não tem onde esconder segredo — a chave vazaria no
primeiro aparelho inspecionado. Proposta: ao registrar, o aparelho recebe um
**token próprio** (devolvido uma vez, guardado no banco só o hash). A batida
exige `device_uuid` + token. Aparelho bloqueado → token morre na hora. Isso não
está na especificação colada; é acréscimo de segurança, e dá para tirar.

### 2.5 Portaria 671 desde o primeiro dia (sem retrabalho depois)

Para o sistema poder virar REP-P de verdade, a marcação já nasce com:
**NSR** (número sequencial, sem furo, por empresa), hash encadeado da linha
anterior (prova de que ninguém apagou no meio) e os campos do **AFD** (arquivo
fiscal que a fiscalização pede). Os arquivos AFD e AEJ e o comprovante da
batida para o empregado ficam para a fase 3 — mas a tabela já guarda o que
eles precisam. **O que eu não consigo confirmar daqui:** REP-P exige registro
do programa e termo de responsabilidade; isso é com a contabilidade/advogado,
antes de desligar o Mobponto.

## 3. Estrutura de arquivos proposta

```
app/apps/ponto/
├── __init__.py              exporta `bp`; import leve (só Flask), como a Análise de SPs
├── README.md                o que é, como roda, variáveis, endpoints   (nasce no fim da fase 1)
├── HISTORICO.md             memória da área, padrão das outras         (idem)
├── PLANO.md                 este arquivo (vira seção do README quando pronto)
├── routes.py                o blueprint /ponto — só valida entrada e chama o core
├── auth.py                  X-API-Key (compare_digest) e token de aparelho;
│                            rota sem declaração é recusada
├── db.py                    conexão: REUSA a engine do ERP (erp/db/database.py);
│                            schema `ponto`; preguiçosa
├── horario.py               agora() em America/Fortaleza; data_referencia por jornada
├── migracoes_runner.py      lista/aplica os .sql do schema `ponto` (cópia do padrão)
├── migracoes/
│   └── 001_ponto_base.sql   todas as tabelas da fase 1
├── core/
│   ├── __init__.py
│   ├── cadastros.py         obras e colaboradores para o ponto (lê o ERP + config)
│   ├── dispositivos.py      registrar, aprovar, bloquear, autorizar; regra por perfil
│   ├── geo.py               distância (haversine) e cerca — função pura, testável
│   ├── marcacoes.py         registrar batida: todas as validações, NSR, status, foto
│   ├── consultas.py         marcações por CPF/período no formato que a folha lê
│   └── recusas.py           registro de batida recusada e motivo
└── scripts/
    ├── importar_colaboradores.py   xlsx/csv → ERP colaboradores + ponto.colaborador_config
    ├── importar_obras.py           xlsx/csv → ERP obras (lat/long se vierem) + ponto.obra_config
    └── migrar.py                   aplica migrações pelo Shell (o botão vem com a tela)

tests/
├── test_ponto.py            regras puras: data_referencia (vigia noturno até 10h),
│                            haversine, autorização por perfil, validação de CPF/UUID
└── test_ponto_banco.py      API de ponta a ponta com Postgres descartável
                             (@pytest.mark.banco); fixture do schema `ponto` DENTRO
                             do arquivo, para não mexer no conftest.py

FORA DO MÓDULO (só com o "pode"):
├── app/main.py              2 linhas: importação protegida + register_blueprint,
│                            igual ao painel e à Análise de SPs
├── CLAUDE.md                uma linha na tabela de áreas
└── CONTEXTO.md              §2 (estrutura), §4 (variável PONTO_API_KEY), §5 (endpoints),
                             §9 (decisão: ponto reusa os cadastros do ERP)
```

Sem dependência nova: `openpyxl` (xlsx) e `SQLAlchemy` já estão no
`requirements.txt`. Redimensionar foto usa `pillow`, que também já está.

## 4. Tabelas — schema `ponto`, migração `001_ponto_base.sql`

Todas com `criado_em TIMESTAMPTZ DEFAULT now()`; horários guardados em UTC no
banco e convertidos para Fortaleza na saída (padrão da casa).

| Tabela | Colunas | Observação |
|---|---|---|
| `obra_config` | obra_id (PK, FK `public.obras`), raio_metros INT NOT NULL DEFAULT 200, centro_custo TEXT, ativo BOOL | raio e "participa do ponto" |
| `colaborador_config` | colaborador_id (PK, FK `public.colaboradores`), tipo_jornada (PADRAO_4 \| VIGIA_DIURNO_2 \| VIGIA_NOTURNO_2), centro_custo TEXT, foto_cadastral_id (FK foto), ativo BOOL | o resto (CPF, nome, obra principal, situação) vem do ERP |
| `colaborador_obras` | colaborador_id, obra_id (PK composta) | obras adicionais |
| `dispositivos` | id, device_uuid TEXT UNIQUE, token_hash TEXT, descricao, perfil (COMPARTILHADO \| INDIVIDUAL \| LISTA), colaborador_id FK nulo, status (PENDENTE \| APROVADO \| BLOQUEADO) DEFAULT PENDENTE, aprovado_por TEXT, aprovado_em, ultimo_uso_em, user_agent | `aprovado_por` é texto porque na fase 1 a aprovação é por API; quando a tela do ERP chegar, passa a ser usuário do ERP |
| `dispositivo_autorizados` | dispositivo_id, colaborador_id | perfil LISTA |
| `dispositivo_obras` | dispositivo_id, obra_id | vazio = todas |
| `marcacoes` | id, **nsr BIGINT UNIQUE**, colaborador_id FK, obra_id FK, timestamp_servidor TIMESTAMPTZ, timestamp_dispositivo TIMESTAMPTZ nulo, **data_referencia DATE**, latitude, longitude NUMERIC(10,6), dentro_da_cerca BOOL, distancia_metros NUMERIC(10,1), dispositivo_id FK nulo, foto_id FK nulo, foto_hash TEXT, origem (PWA \| IDFACE \| MANUAL), status (VALIDA \| EM_ANALISE \| AJUSTADA \| REJEITADA), motivo_analise TEXT, **hash_encadeado TEXT**, registrado_por TEXT (origem MANUAL) | índices: (colaborador_id, data_referencia), (obra_id, data_referencia), (nsr) |
| `marcacao_fotos` | id, sha256 TEXT, conteudo BYTEA, tamanho INT, mime TEXT, criado_em, expurgada_em | guarda por prazo; expurgo deixa o hash na marcação |
| `ajustes` | id, marcacao_id FK nulo, colaborador_id FK, data_referencia, tipo (INCLUSAO \| EXCLUSAO \| ALTERACAO_HORARIO \| ALTERACAO_OBRA \| ABONO), horario_proposto, obra_proposta_id, justificativa TEXT NOT NULL, solicitado_por TEXT, aprovado_por TEXT, status (PENDENTE \| APROVADO \| NEGADO), decidido_em | a marcação original **nunca é alterada**: ajuste aprovado gera marcação nova com status AJUSTADA e aponta para a antiga |
| `recusas` | id, device_uuid, cpf_informado, obra_id, motivo TEXT, detalhes JSONB, ip, criado_em | investigação de fraude e de aparelho mal configurado |
| `_migracoes` | nome, aplicada_em | controle, igual às outras áreas |

**Regra da `data_referencia`:** PADRAO_4 e VIGIA_DIURNO_2 → a data do
`timestamp_servidor` em Fortaleza. VIGIA_NOTURNO_2 → se a hora local for
**antes das 10h**, pertence ao dia anterior (o turno começou na véspera);
senão, ao dia da batida. A hora de corte (10h) fica em constante, não espalhada.

## 5. API — blueprint `/ponto` (JSON `{'ok': …}` da casa)

| Método e rota | Quem chama | Autenticação | O que faz |
|---|---|---|---|
| `GET /ponto/health` | Render / você | pública (declarada) | diz se o módulo subiu e se o schema está migrado |
| `POST /ponto/api/dispositivo/registrar` | PWA, 1ª abertura | nenhuma, com limite por IP | grava `device_uuid` como PENDENTE, devolve o **token do aparelho** (uma vez) |
| `POST /ponto/api/marcacao` | PWA / iDFace | token do aparelho (`device_uuid` + `X-Device-Token`) **ou** `X-API-Key` (iDFace e MANUAL) | a batida: todas as validações da §2.3, distância, cerca, NSR, data_referencia, foto opcional |
| `GET /ponto/api/marcacoes?cpf=&data_inicio=&data_fim=&obra=` | Análise de SPs, planilhas | `X-API-Key` | por pessoa e dia: batidas em ordem com hora local, obra, status e cerca — o formato que a folha lê hoje do Mobponto |
| `GET /ponto/api/colaboradores` | sistemas | `X-API-Key` | pessoas ativas para o ponto, com obra principal e adicionais |
| `GET /ponto/api/obras` | sistemas | `X-API-Key` | obras com coordenadas e raio |
| `GET /ponto/api/dispositivos?status=` | administração | `X-API-Key` | fila de aprovação |
| `POST /ponto/api/dispositivos/<id>/aprovar` · `/bloquear` · `/autorizar` | administração | `X-API-Key` | aprova (define perfil, dono, lista, obras), bloqueia, altera a lista |
| `POST /ponto/api/admin/migrar` | administração | `X-API-Key` | aplica migrações pendentes (o botão da tela vem na fase 2) |

Regras transversais, todas com teste:

- Chave em `PONTO_API_KEY` no Render. **Sem a variável, toda rota com chave
  responde "serviço não configurado"** — falha fechado, nunca aberto.
- CPF só dígitos, validado (reusa `erp/core/cadastros/validadores.py`).
- Período de consulta: no máximo 62 dias por chamada, para não derrubar a
  instância de 2 GB (`CONTEXTO.md` §3.7); página de 500 pessoas.
- Entrada ruim → 400 com o campo errado nomeado; segredo errado → 401;
  aparelho não autorizado → 403 **e** linha em `recusas`; recurso de outro →
  404 (nunca confirmar existência).
- Relógio do aparelho mais de 5 minutos fora do servidor → aceita e marca
  EM_ANALISE com o motivo.
- **Repetição:** mesma pessoa, mesmo aparelho, menos de 60 segundos → a segunda
  batida não grava e responde a primeira (toque duplo no celular não vira
  batida dupla). Valor em constante.
- Log informativo em toda decisão (aceita, em análise, recusada), sem CPF
  completo no log (só os 3 últimos dígitos).

## 6. Importadores (CLI, pasta `scripts/`)

Os dois leem `.xlsx` ou `.csv`, aceitam os nomes de coluna com ou sem acento e
em qualquer caixa, **mostram o que vão fazer** (`--simular`, padrão) e só gravam
com `--gravar`. Linha com erro é listada com o número dela; nenhuma linha boa
deixa de entrar por causa de uma ruim. Reexecutar não duplica (CPF e código da
obra são a chave).

- `importar_colaboradores.py`: colunas `cpf`, `nome`, `obra` (código ou nome
  exato do ERP), `centro_custo`, `tipo_jornada`. Pessoa que não existe no ERP
  é **criada** lá com nome, CPF e obra (decisão §2.1); a jornada e o centro de
  custo vão para `ponto.colaborador_config`. Pessoa desligada no ERP fica
  inativa no ponto.
- `importar_obras.py`: colunas `codigo`, `nome`, `centro_custo`, `latitude`,
  `longitude`, `raio_metros`. Obra que não existe no ERP é criada (código e
  nome). Latitude/longitude vazias ficam vazias — a cerca só vale quando a
  obra tiver coordenada, e até lá a batida entra como EM_ANALISE com motivo
  "obra sem coordenada".

Ambos rodam pelo Shell do Render ou no PC com `DATABASE_URL`; nunca no start.

## 7. Testes

- `tests/test_ponto.py` (sem banco, roda em menos de 1 s): data_referencia nos
  três tipos de jornada e nas bordas (09:59, 10:00, meia-noite); haversine em
  pontos conhecidos; autorização por perfil (COMPARTILHADO aceita todos da obra,
  INDIVIDUAL só o dono, LISTA só a lista); status por regra (§2.3); repetição em
  60 s; validação de CPF e UUID; leitura de planilha com colunas fora de ordem.
- `tests/test_ponto_banco.py` (`@pytest.mark.banco`): sobe o schema `ponto` com
  a migração de verdade, cria obra e pessoa no ERP, registra aparelho, aprova,
  bate dentro e fora da cerca, consulta por período, confere NSR sequencial e
  hash encadeado, confere que sem `PONTO_API_KEY` tudo fecha.
- Teste de contrato com o ERP: os campos de `obras` e `colaboradores` que o
  ponto lê (como o de 24/09 para o OFX).

## 8. O que fica de fora da fase 1, por decisão

PWA e tela de bater ponto; integração com o iDFace (a API já aceita
`origem=IDFACE`, falta o conector); telas no ERP (aprovar aparelho, analisar
batida, ajustes); upload ao Drive; arquivos AFD/AEJ e comprovante do empregado;
espelho de ponto e cálculo de horas; expurgo automático de fotos (a coluna
existe, a rotina vem com a fila de segundo plano do ERP).

## 9. Ordem de execução, quando aprovado

1. migração + `db.py` + `horario.py` + `geo.py` (com `test_ponto.py` junto);
2. `dispositivos.py`, `marcacoes.py`, `consultas.py`, `recusas.py`;
3. `auth.py` + `routes.py` + `migracoes_runner.py` (com `test_ponto_banco.py`);
4. os dois importadores;
5. `README.md`, `HISTORICO.md`, `PERGUNTAS.md` do ERP (as perguntas que o ponto
   passa a responder: "quem bateu ponto hoje na obra X?", "quantos dias o
   Fulano trabalhou em setembro?", "quais batidas estão em análise?");
6. as 2 linhas do `main.py` e as linhas do `CLAUDE.md`/`CONTEXTO.md` — só
   depois do seu "pode" para mexer fora do módulo.

Tudo numa sequência só, sem parar entre os passos; a suíte inteira roda antes
de propor a junção, e a junção na `main` espera o seu "pode".
