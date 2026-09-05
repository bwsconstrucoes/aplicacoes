# CONTEXTO — App Flask Render

> Documento de referência da aplicação. Mantém estrutura, convenções, recursos
> externos e ganchos de extensão pra que conversas futuras com Claude entrem
> direto no problema, sem ter que re-explicar o terreno.

---

## 1. Identificação

- **Nome do projeto:** Aplicações (monorepo Flask)
- **Hospedagem:** Render
- **Domínios customizados configurados:**
  - `link.bwsconstrucoes.com.br` → módulo `encurtador`
  - `pdf.bwsconstrucoes.com.br` → rotas de PDF (`/compilar`, `/pdf2texto`)
  - `aplicacoes.bwsconstrucoes.com.br` → app geral (visto nos logs 2026-07)
- **URL pública padrão Render:** ⚠️ preencher (ex.: `https://aplicacoes-xxx.onrender.com`)
- **Plano Render:** Pago (serviço não hiberna — confirmado 2026-07). Instância com **2 GB de RAM** (limite que causava OOM, ver §9).
- **Service ID Render:** `srv-d167o5qli9vc73cvq83g`
- **Repositório Git:** `https://github.com/bwsconstrucoes/aplicacoes.git` (branch principal `main`)
- **Branch deploy automático:** ⚠️ preencher
- **Região Render:** ⚠️ preencher

---

## 2. Stack e estrutura

- **Linguagem:** Python (pelo `pdfplumber==0.11.4` e `pypdf==5.1.0` provavelmente 3.10+)
- **Framework:** Flask + gunicorn (worker `gthread`, 1 worker / 4 threads)
- **Padrão:** factory pattern via `create_app()` em `app/main.py`
- **Comando de start (desde 2026-07-14, ajustado p/ conter OOM):**
  ```
  gunicorn app.main:app --timeout 3600 --graceful-timeout 120 --keep-alive 120 --workers 1 --threads 4 --worker-class gthread --max-requests 150 --max-requests-jitter 40 --log-level info
  ```
- ⚠️ **IMPORTANTE — Start Command vs Procfile:** o campo **Start Command nas
  Settings do Render SOBRESCREVE o Procfile**. Houve um período em que o campo
  estava preenchido com o comando antigo e o Procfile era ignorado (descoberto
  2026-07-14). Ao mudar o comando do gunicorn, alterar nos DOIS lugares — ou
  deixar o Start Command em branco pra valer o Procfile (preferido, versionado).
- **Entry-point real:** `app/main.py` (NÃO é `app.py` na raiz — esse é legado do
  pdf-processor que ainda existe no monorepo).

### Estrutura de pastas

```
aplicacoes/
├── Procfile
├── README.md
├── requirements.txt
├── app.py                    ← LEGADO (pdf-processor antigo, ainda funcional?)
├── data/
│   └── links.json            ← usado pelo encurtador
└── app/
    ├── __init__.py
    ├── main.py               ← entry-point real (create_app)
    └── apps/
        ├── pdf_processor/    ← rotas /compilar, /pdf2texto, /token-status
        ├── encurtador/       ← /encurtador/* + redirect global /<codigo>
        ├── email_financeiro/ ← /api/email_financeiro/*
        ├── sheets_sync/      ← /api/sheets_sync/sincronizar
        ├── atualizaspbotao/  ← /api/atualizaspbotao/executar
        ├── validasp/         ← /api/validasp/*
        ├── chatbot/          ← /api/chatbot/* (WhatsApp via Z-API)
        ├── whatsapp_gateway/ ← /instances/... (espelha Z-API → Evolution API)
        ├── baixabradesco/    ← /api/baixabradesco/* (routes, core, sheets, parser_*, omie, pipefy, zapi, storage, fila, matcher, models, utils, diagnostico)
        ├── processarnovasp/  ← /api/processarnovasp/executar (⚠️ existe em produção, ainda não documentado aqui)
        ├── sync_logs/        ← /api/sync_logs/* (⚠️ ainda não documentado aqui)
        ├── emissaonf/        ← /emissao/* (emissão de NFS-e; ⚠️ ainda não documentado aqui)
        ├── telegram/         ← /telegram/* (bot / autocadastro; ⚠️ ainda não documentado aqui)
        ├── notificador.py    ← helper `enviar_telegram`, usado pelo ERP para avisar baixas
        └── erp/              ← /erp/* — ERP (ver §2.1 e §5.12). NÃO é um blueprint
                                de arquivo único: tem camadas próprias
```

### Padrão de blueprint

Cada módulo em `app/apps/<nome>/` segue:
- `__init__.py` → `from .routes import bp`
- `routes.py` → cria `bp = Blueprint('<nome>', __name__)` e define rotas
- Outros arquivos auxiliares: `core.py`, `sheets.py`, `utils.py`, etc.
⚠️ **Exceção:** o `pdf_processor` NÃO segue o padrão — o módulo inteiro
(blueprint + rotas + helpers) vive no próprio `__init__.py`, sem `routes.py`.
Funciona porque `main.py` importa `bp` de lá do mesmo jeito.

⚠️ **Exceção (outra direção):** o `erp` vai muito ALÉM do padrão — tem `core/`,
`db/`, `templates/`, `static/` e `scripts/` próprios (ver §2.1). É o único
módulo com banco relacional e o único que serve HTML de tela cheia em vez de só
JSON. Ao mexer nele, siga a camada: regra de negócio em `core/`, nunca no
`routes.py`.

Registro centralizado em `app/main.py`:

```python
from app.apps.<nome> import bp as <nome>_bp
app.register_blueprint(<nome>_bp, url_prefix="/api/<nome>")
```

⚠️ **`pdf_processor`, `encurtador`, `whatsapp_gateway`, `telegram` e `erp` são
registrados SEM `url_prefix`** — expõem rotas na raiz (`/compilar`, `/<codigo>`,
`/instances/...`). No caso do `telegram` e do `erp` isso é deliberado: as rotas
já trazem o prefixo (`/telegram`, `/erp`) embutido no próprio módulo, então o
`url_prefix` duplicaria o caminho.

### 2.1 Estrutura interna do ERP (`app/apps/erp/`)

É o único módulo com arquitetura em camadas — os demais são um `routes.py` mais
alguns helpers. Vale a pena conhecer o desenho antes de mexer:

```
erp/
├── routes.py              ← blueprint: telas HTML + API JSON (~3.4k linhas)
├── ROTEIRO.md             ← backlog vivo do ERP (entregue / em andamento / fila)
├── db/
│   ├── database.py        ← engine SQLAlchemy única, inicialização PREGUIÇOSA
│   └── models/            ← 48 tabelas (cadastros.py, financeiro.py)
├── core/                  ← regra de negócio, por domínio
│   ├── auth/              ← autenticação e permissões (quem vê o quê)
│   ├── titulos/           ← ciclo do título: análise, aval, empreita, prestação,
│   │                        receber, enquadramento, duplicidade, estorno
│   ├── pagamentos/        ← boleto, OFX, comprovante, conciliação, lotes
│   ├── cadastros/         ← obras, fornecedores, plano de contas, de-para, Receita
│   ├── documentos/        ← leitura por IA (foto/PDF), NFe, armazenamento
│   ├── importadores/      ← cards do Pipefy, planilhas
│   ├── comum/             ← auditoria, migrações, custo de IA
│   ├── locacoes.py  pessoal.py  relatorios.py  notificacoes.py
├── scripts/
│   ├── migracoes/         ← .sql numerados (001…028), aplicados pela interface
│   └── migrar.py, seed_admin.py, criar_schema.py, resetar_senha.py
├── templates/             ← 19 telas (erp_base.html + uma por aba)
└── static/erp.css
```

Três decisões estruturais que se repetem no código:

1. **Migrações nunca no boot.** `core/comum/migracoes.py` aplica os `.sql`
   pendentes por um botão do ADMIN, com tabela de controle `_migracoes` e uma
   transação por arquivo (se a terceira falhar, as duas primeiras ficam). É
   deliberado: o ERP divide processo com `baixabradesco`, `emissaonf`,
   `telegram` e o gateway — uma migração ruim não pode derrubar todo mundo.
2. **Permissão é cláusula de consulta, não enfeite de tela.**
   `core/auth/permissoes.py` mapeia ação → perfis e injeta o escopo no `SELECT`,
   então o que está fora do alcance do usuário nem chega ao navegador. São 9
   perfis, de ADMIN a ADMINISTRATIVO_OBRA (que só enxerga o que ele lançou).
3. **Auditoria append-only.** `core/comum/auditoria.py::registrar_evento` grava
   na tabela `eventos` com detalhe em `jsonb`; todos os services chamam.

O estado do título é um enum de 11 valores (`StatusTitulo`, em
`db/models/financeiro.py`): RASCUNHO → EM_ANALISE → AGUARDANDO_AVAL →
AGUARDANDO_APROVACAO → APROVADO → PAGO_PARCIAL → PAGO, mais DEVOLVIDO,
BLOQUEADO, CANCELADO e ESTORNADO.

A navegação é por **MÓDULOS** (constante `MODULOS` no topo do `routes.py`):
Financeiro (11 abas), Obras, Pessoal e Administração — a barra de abas mostra só
o módulo em que a pessoa está. O sistema não é um financeiro com apêndices.

---

## 3. Convenções de código

### 3.1 Credenciais Google

- **Variável:** `GOOGLE_CREDENTIALS_BASE64` (JSON da service account em base64)
- **Padrão de uso:**
  ```python
  import os, json
  from base64 import b64decode
  import gspread
  from google.oauth2.service_account import Credentials

  SCOPES = [
      'https://www.googleapis.com/auth/spreadsheets',
      'https://www.googleapis.com/auth/drive',
  ]

  def _get_gc():
      creds_b64 = os.getenv('GOOGLE_CREDENTIALS_BASE64', '')
      if not creds_b64:
          raise RuntimeError('GOOGLE_CREDENTIALS_BASE64 não configurado.')
      creds_dict = json.loads(b64decode(creds_b64).decode('utf-8'))
      creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
      return gspread.authorize(creds)
  ```

⚠️ **Não criar nova envvar pra credencial.** Reusar `GOOGLE_CREDENTIALS_BASE64`.

### 3.2 Cliente Google Sheets

- **Biblioteca padrão:** `gspread==6.1.4` (não `google-api-python-client` direto, embora também esteja no requirements).
- Abrir planilha: `gc.open_by_key(spreadsheet_id)` → retorna spreadsheet
- Acessar aba: `ss.worksheet('NomeAba')` → retorna worksheet
- Ler tudo: `ws.get_all_values()`
- Escrever: `ws.update('A1', dados, value_input_option='USER_ENTERED')`
- Limpar range: `ws.batch_clear(['A1:F'])`
⚠️ **Regra de memória (aprendida no OOM de 2026-07, ver §9):** NUNCA usar
`get_all_values()` / `ws.get('A:AK')` em abas grandes (SPsBD tem ~52k linhas ≈
150-250 MB em RAM por fetch) quando só se precisa de poucas colunas ou de
localizar uma linha. Preferir `batch_get(['A2:A'])` das colunas necessárias e
`batch_update` pra gravar. Se precisar do range largo, buscar UMA vez por
request e compartilhar (padrão `load_spsbd_values` no baixabradesco).

### 3.3 Logging

```python
import logging
logger = logging.getLogger(__name__)
```

Usado em todos os módulos. `gunicorn --log-level info` no Procfile.

### 3.4 Autenticação por secret

Padrão: cada módulo crítico tem sua própria envvar `<MODULO>_SECRET`. Validação
no início da rota:

```python
def validar_payload(payload: dict):
    if payload.get('secret') != CONFIG['SECRET']:
        raise ValueError('Secret inválido.')
```

Exemplos: `ATUALIZASPBOTAO_SECRET`, `BAIXABRADESCO_SECRET`, `CHATBOT_WEBHOOK_SECRET`.

### 3.5 Formato padrão de resposta

```python
# Sucesso
return jsonify({'ok': True, ...dados}), 200

# Erro de validação
return jsonify({'ok': False, 'erro': 'mensagem'}), 400

# Erro de servidor
return jsonify({'ok': False, 'erro': str(e)}), 500
```

`atualizaspbotao` adicionalmente retorna campo `response` com HTML (resposta
visual pra exibir no portal) — não é padrão obrigatório.

⚠️ **Exceção deliberada:** o `whatsapp_gateway` NÃO usa `{'ok': ...}` nas rotas
espelhadas — devolve o formato do Z-API (`{zaapId, messageId, id}` / `{error}`)
justamente pra ser drop-in do Z-API. Rotas internas dele (webhook/health) seguem
o padrão da casa.

### 3.6 Idioma e estilo

- Código em inglês (variáveis, funções).
- Comentários e docstrings em PT-BR.
- Mensagens de erro pra usuário em PT-BR.
- Logs em PT-BR.

### 3.7 Regras de memória (instância de 2 GB)

Padrões obrigatórios pra qualquer código novo (origem: incidente OOM 2026-07):

- **Download externo:** sempre `requests.get(url, stream=True, timeout=60)` com
  teto de tamanho (padrão da casa: 50 MB, constante `MAX_*_BYTES` no módulo).
  Nunca `r.content` direto pra arquivos de tamanho desconhecido.
- **PyMuPDF (fitz):** todo `fitz.open()` precisa de `doc.close()` (usar
  `try/finally`); pixmaps devem ser soltos (`pix = None`) logo após uso.
- **Buffers grandes:** `del` + `gc.collect()` ao fim de requests pesados.
- **Threads em background:** nada de disparar N threads que fazem fetch pesado
  de planilha — o consumo multiplica e estoura o worker.

### 3.8 Acesso ao banco (só o ERP)

- **Sessão sempre pelo contexto:** `with get_session() as s:` (de
  `app/apps/erp/db/database.py`). Ele faz rollback no erro e fecha sempre.
- **A engine é preguiçosa de propósito.** Nada de banco acontece no import: a
  conexão só é criada na primeira chamada de `obter_engine()`. É o que permite o
  monorepo subir mesmo sem `DATABASE_URL` configurada — se o ERP conectasse no
  import, um problema de banco derrubaria os outros 13 blueprints junto.
  **Não mover conexão para o topo do módulo.**
- **Toda escrita relevante registra evento** via `registrar_evento(...)` — a
  trilha de auditoria é append-only e não se apaga.
- **Mudança de schema é migração numerada** em `erp/scripts/migracoes/`, nunca
  `ALTER TABLE` solto nem `create_all()` em produção.
- Pool pequeno de propósito (`pool_size=5, max_overflow=5`): o serviço tem 2 GB
  e divide espaço com os demais módulos.

### 3.9 Autorização do ERP (padrão NEGAR)

Vale só para o `erp` — os demais módulos são chamados por máquina e usam secret
próprio (§3.4). Aqui há gente operando, com perfis diferentes.

São **duas** camadas, e confundi-las é o erro clássico:

- **Alçada** — "este perfil pode esta ação?". Declarada em cada rota com
  `@permissao("pagar")`. O `before_request` do blueprint recusa endpoint que
  não esteja no registro, então **esquecer fecha a rota**, não abre.
- **Escopo de objeto** — "pode NESTE registro?". Um supervisor tem a ação de
  lançar; isso não o autoriza a abrir o título da obra de outro. Os
  `exigir_*_no_escopo` respondem isso, e todos passam pelo **mesmo**
  `aplicar_escopo` que a listagem usa — de modo que detalhe e lista não têm
  como divergir sem que alguém altere os dois.

Fora do escopo responde **404 "não encontrado", nunca 403**: o 403 num id que
existe seria um oráculo — bastaria varrer os ids para mapear o sistema sem
abrir um único registro.

Dois testes estruturais sustentam isso e não dependem de lista escrita à mão:
um exige declaração em toda rota registrada; o outro deriva do código as rotas
que recebem id com ação ampla e exige que cada uma confira escopo.

**O alcance de quem lança é por PESSOA, não por cargo** (migração 029). O
cadastro do operador tem o campo `escopo_visao`:

- `PROPRIOS` — só o que a própria pessoa lançou. É o **default**, inclusive
  para todo cadastro anterior à migração.
- `OBRAS_DESIGNADAS` — o que ela lançou **mais** o que estiver rateado nas obras
  associadas a ela em `usuario_obras`.

Vale para `ADMINISTRATIVO_OBRA` e `LANCADOR` — os perfis que filtravam por
autoria. Quem já enxergava tudo continua enxergando, e o `SUPERVISOR_OBRA`
mantém a regra dele. Campo vazio, cadastro antigo ou valor estranho no banco
caem todos em `PROPRIOS`: **a ausência de configuração fecha**. Quem está em
`OBRAS_DESIGNADAS` sem nenhuma obra associada enxerga só a autoria — uma lista
vazia não pode virar "vê tudo".

**A alçada também é ajustável por PESSOA** (migração 032). O cargo continua
sendo a base — é ele que responde por tudo que já está no ar —, e a tabela
`usuario_permissoes` guarda só as **exceções** marcadas no cadastro de alguém:
`concedida=TRUE` acrescenta uma ação que o cargo não dá, `concedida=FALSE` tira
uma que o cargo daria. Sem linha, vale o cargo, e a tabela nascer vazia não muda
o comportamento de ninguém.

Motivo prático, nas palavras do dono: "de repente o diretor sai de férias e eu
quero deixar outra pessoa responsável por autorizar alguma coisa" — sem
inventar um cargo novo para cada arranjo.

Três cuidados que sustentam isso:

- a decisão mora em **um lugar só**: `pode()` (com o objeto `Usuario`) e
  `decidir()` (com valores soltos, para a guarda) aplicam a mesma regra, e um
  teste percorre perfil × ação × marcação exigindo que as duas concordem;
- o **ADMIN não se tranca para fora**: `configurar`, `gerir_usuarios` e
  `ver_erp` não podem ser desmarcadas dele, senão um clique errado deixaria o
  sistema sem ninguém que consertasse;
- as exceções são lidas por **SQL direto**, como o perfil, e se a tabela ainda
  não existir a leitura falha em silêncio e vale o cargo. Pelo mesmo motivo do
  §3.8: o botão que aplica as migrações não pode depender da migração.

### 3.10 Consumo de IA: um ponto de registro, um teto que só avisa

Toda leitura por IA passa por `documentos/leitor._chamar_ia`, e é **ali** que o
consumo é gravado em `ia_uso` — não em cada tela. Quem chama só declara a
operação com `ia_custo.contexto(operacao="fatura_cartao")`; o usuário vem do
`before_request` do blueprint; o leitor sabe os tokens. Tela nova que use o
leitor já nasce contabilizada (sem declarar, cai em `leitura_documento`).
Falha da OpenAI também é registrada (`sucesso=False`) — a chamada pode ter
custado, e o painel precisa mostrar que a leitura está quebrando.

O registro roda em **sessão própria, com commit**: perder uma linha do painel
é aceitável, perder um lançamento não. Por isso ele sobrevive a um rollback da
operação principal — é intencional.

Teto mensal (`parametros.ia_teto_mensal_usd`, tela de Configurações): ao
passar de 80% os ADMIN com telefone/CPF recebem um Telegram; ao estourar,
outro. Uma vez por nível por mês (`parametros.ia_alerta_enviado`). **Nada é
bloqueado** — o aviso existe para a decisão ser tomada antes da fatura, não
para travar quem está lançando.

Preços em `PRECOS` (US$/milhão de tokens). Modelo fora da tabela é cobrado
pelo `PRECO_PADRAO` e aparece no painel como "preço estimado".

### 3.11 Testes com banco de verdade

A suíte sem banco (`SessaoFalsa`) cobre regra; **não cobre SQL** — e o escopo
por obra/autoria é um `WHERE`. Por isso há uma segunda camada, marcada
`@pytest.mark.banco`, que roda contra um Postgres **descartável**:

- `ERP_TEST_DATABASE_URL` é a única entrada. O `conftest` **recusa** host que
  não seja local e banco cujo nome não contenha "teste". A produção não tem
  como ser alcançada por engano.
- O banco é reconstruído do zero a cada sessão da suíte: `DROP SCHEMA`,
  `schema.sql`, depois as 30 migrações pelo mesmo `aplicar_pendentes` do
  botão de Configurações. Se uma migração quebrar em banco vazio, é aqui que
  aparece.
- Cada teste roda numa transação desfeita no fim (savepoint por `commit`).
  As rotas do Flask usam a **mesma** sessão do teste (`app_real`), então o
  que a rota grava também some.
- `.github/workflows/testes.yml` sobe o Postgres a cada push e roda tudo;
  `docker-compose.teste.yml` sobe o mesmo no PC (porta 5433).

---

## 4. Variáveis de ambiente

### 4.1 Globais (toda a aplicação)

- `PORT` — porta do servidor (Render injeta automaticamente, fallback 5000)
- `MALLOC_ARENA_MAX=2` — tuning do glibc (adicionada 2026-07-13, anti-OOM).
  Limita as arenas do malloc; sem isso, com threads o glibc criava dezenas de
  arenas que retinham memória liberada e inflavam o RSS. **Não remover.**
- `MALLOC_TRIM_THRESHOLD_=100000` — tuning do glibc (idem, o underscore final
  faz parte do nome). Força devolução de memória ao SO. **Não remover.**

### 4.2 Google

- `GOOGLE_CREDENTIALS_BASE64` — JSON da service account em base64 (usado em todos os módulos que acessam Sheets/Drive)
- `GOOGLE_FOLDER_ID` — pasta padrão do Drive (usado em `pdf_processor/compilar`)
- `GDRIVE_FOLDER_ID` — pasta do Drive (email_financeiro)
- `SPREADSHEET_ID` — ID da planilha do email_financeiro

### 4.3 Dropbox

- `DROPBOX_APP_KEY`
- `DROPBOX_APP_SECRET`
- `DROPBOX_REFRESH_TOKEN`

### 4.4 Z-API (WhatsApp) — legado, em migração p/ Evolution

- `ZAPI_INSTANCE_ID`
- `ZAPI_API_TOKEN`
- `ZAPI_CLIENT_TOKEN`

### 4.5 Omie

- `OMIE_BWS_APP_KEY`
- `OMIE_BWS_APP_SECRET`

### 4.6 Pipefy

- `PIPEFY_API_TOKEN`

### 4.7 Secrets de módulos

- `ATUALIZASPBOTAO_SECRET`
- `BAIXABRADESCO_SECRET`
- `BAIXABRADESCO_DEBUG` (opcional, "1"/"true"/"sim"/"yes")
- `CHATBOT_WEBHOOK_SECRET`
- `CHATBOT_MASTER_PHONE` (default `5585987846225`)

### 4.8 Outros

- `OCR_ENABLED` ("TRUE"/"FALSE", email_financeiro)

### 4.9 WhatsApp Gateway (Evolution API) — ver doc `whatsapp_gateway.md`

- `EVOLUTION_BASE_URL` — URL pública do serviço Evolution (Docker separado no Render)
- `WHATSAPP_GATEWAY_INSTANCES` — JSON: mapa `instance/token` do Z-API → instância + apikey Evolution + webhook do make
- `WHATSAPP_GATEWAY_WEBHOOK_SECRET` — valida webhooks vindos da Evolution
- `WHATSAPP_GATEWAY_CLIENT_TOKEN` — (opcional) espelha o header Client-Token do Z-API

### 4.10 ERP

- `DATABASE_URL` — connection string do Postgres do ERP (painel do Render >
  Database > Connect, **Internal Database URL**). Prefixo `postgres://` ou
  `postgresql://` é normalizado para `postgresql+psycopg2://` no
  `db/database.py`. Sem ela o ERP levanta `RuntimeError` na primeira consulta —
  mas o resto do monorepo continua de pé (ver §3.8).
- `ERP_SECRET_KEY` — assinatura da sessão do login. Lida em `app/main.py`, com
  cascata `ERP_SECRET_KEY` → `SECRET_KEY` → literal de desenvolvimento
  (ver a pendência em §10).
- `OPENAI_API_KEY` — leitura de documento por IA, sugestão de categoria e
  leitura de contrato de locação.
- `ERP_MODELO_IA` — modelo de texto (padrão `gpt-4o-mini`).
- `ERP_MODELO_IA_VISAO` — modelo de visão, para foto e PDF ruim (padrão `gpt-4o`).
- `ERP_MODO_TRANSICAO` — **padrão ligado** ("1"). Desligar com "0"/"false".
- `PIPEFY_API_TOKEN` / `PIPEFY_TOKEN` — importador de cards do Pipefy (o módulo
  aceita os dois nomes; o restante do monorepo usa `PIPEFY_API_TOKEN`).

O aviso de título pago sai pelo helper `app/apps/notificador.py`, que tem
chaves próprias: `NOTIFICAR_TELEGRAM`, `NOTIFICAR_WHATSAPP` e as `ZAPI_*`.

---

## 5. Endpoints expostos

### 5.1 Raiz / health

| Método | Rota | Função |
|---|---|---|
| GET | `/` | Lista módulos ativos |
| GET | `/token-status` | Verifica token Dropbox (existe no `pdf_processor` e no `app.py` legado) |

### 5.2 PDF Processor (sem prefixo)

| Método | Rota | Função |
|---|---|---|
| POST | `/compilar` | Compila múltiplos PDFs/imagens em 1 PDF, salva no Dropbox/GDrive |
| POST | `/pdf2texto` | Extrai texto página a página de PDFs |

Flags do `/compilar` (payload): `attachments`/`links`, `nome_arquivo`, `destino`
(dropbox|googledrive), `pasta`, `salvar`, `deletar`+`auto_delete`, e
`incluir_texto` (default true; mandar `false` quando o make não usa o campo
`texto` — economiza memória/banda; adicionado 2026-07-14).
Downloads por URL têm teto de 50 MB (`MAX_DOWNLOAD_BYTES` no módulo) — arquivo
maior é ignorado com log `❌ Ignorado (excede ...)`.

### 5.3 Encurtador (sem prefixo)

| Método | Rota | Função |
|---|---|---|
| GET | `/<codigo>` | Redireciona para URL longa |
| POST | `/encurtador/novo` | Cria link curto (visto nos logs; demais rotas administrativas a detalhar) |

### 5.4 Email Financeiro (`/api/email_financeiro`)

⚠️ Detalhar quando precisar mexer.

### 5.5 Sheets Sync (`/api/sheets_sync`)

| Método | Rota | Função |
|---|---|---|
| POST | `/sincronizar` | Sincroniza abas configuradas em `config.py` |

**Body:**
```json
{
  "spreadsheet_id": "ID_DESTINO",
  "spreadsheet_name": "Nome (substring que bate em PLANILHAS)"
}
```

**Planilhas suportadas hoje** (em `sheets_sync/config.py`):
- "Mapa de Cotação"
- "Cotação de Suprimentos"
Suporta 2 modos de cópia: `continuo` (sequencial) e `gap` (blocos com colunas vazias no meio, preservando colunas com fórmulas).

### 5.6 AtualizaSPBotão (`/api/atualizaspbotao`)

| Método | Rota | Função |
|---|---|---|
| POST | `/executar` | Atualiza Log + SPsBD + integra com Omie + Pipefy + valida boleto DDA |

**Body:** payload completo (mesmo formato enviado ao Apps Script). Campo `secret` obrigatório.

**Planilha:** `1lrP1HOvwqyXiVdP2kuTgG7sJjl2QXl0WT4lwkd392DA` (Registros de SP).

Retorna `response` com HTML pronto pra exibir.

### 5.7 ValidaSP (`/api/validasp`)

⚠️ Detalhar quando precisar mexer.

### 5.8 Chatbot (`/api/chatbot`)

⚠️ Detalhar quando precisar mexer. WhatsApp via Z-API.

### 5.9 BaixaBradesco (`/api/baixabradesco`)

| Método | Rota | Função |
|---|---|---|
| POST | `/executar` | Processa comprovantes Bradesco: parse do PDF → match com SP (SPsBD/SPsAgendar) → baixa Omie → atualiza SPsBD → Pipefy → WhatsApp opcional |
| GET | `/health` | Health check (`version: 2.0`) |
| POST | `/diagnostico` | Analisa sem executar |
| POST | `/reprocessar-fila` | Reprocessa falhas enfileiradas |

Auth: `secret` no payload ou header `X-BaixaBradesco-Secret` (`BAIXABRADESCO_SECRET`).
`modo_teste` default **true** (produção exige `modo_teste: false`).
Opções: `executar_omie`, `atualizar_spsbd`, `atualizar_pipefy`, `enviar_whatsapp`,
`salvar_comprovante`, `pasta_dropbox`.

Arquitetura interna (arquivos): `routes.py` → `core.py` (orquestra) →
`sheets.py` (SPsBD/BaseBancos/LogBaixaBradesco), `parser_pdf.py`,
`parser_bradesco.py`, `matcher.py`, `omie.py`, `pipefy.py`, `zapi.py`,
`storage.py` (Dropbox), `fila.py`, `models.py`, `utils.py`, `diagnostico.py`.

Pontos de memória (pós-correção 2026-07-14):
- `load_spsbd_values()` busca o range A:AK da SPsBD **uma vez por request** e é
  compartilhado por `load_spsbd_operacional` + `load_spsbd_omie_pendente`
  (loaders aceitam `values=` pré-carregado; sem o argumento, buscam sozinhos).
- `execute_spsbd_updates` localiza a linha via `batch_get` **só das colunas de
  filtro** (ex.: `A2:A`) e grava com `batch_update` único — NUNCA voltar ao
  `get_all_values()` ali (rodava em thread por comprovante e multiplicava
  ~200 MB por lote).
- Download de comprovante por URL: streaming com teto de 50 MB
  (`MAX_ATTACHMENT_BYTES` no core.py); estouro vira erro 400, não OOM.

### 5.10 ProcessarNovaSP (`/api/processarnovasp`)

| Método | Rota | Função |
|---|---|---|
| POST | `/executar` | ⚠️ Existe em produção (logs 2026-07) mas ainda não documentado; detalhar quando mexer |

### 5.11 WhatsApp Gateway (sem prefixo — espelha Z-API)

| Método | Rota | Função |
|---|---|---|
| POST | `/instances/<id>/token/<tk>/send-text` | Texto → Evolution sendText |
| POST | `/instances/<id>/token/<tk>/send-image` | Imagem → Evolution sendMedia |
| POST | `/instances/<id>/token/<tk>/send-document/<ext>` | Documento → Evolution sendMedia |
| POST | `/instances/<id>/token/<tk>/send-audio` | Áudio → Evolution sendWhatsAppAudio |
| POST | `/instances/<id>/token/<tk>/send-link` | Link → Evolution sendText (preview) |
| POST | `/api/whatsapp_gateway/webhook/<instance>` | Evolution → traduz p/ Z-API → make |
| GET | `/api/whatsapp_gateway/health` | Lista instâncias configuradas |

### 5.12 ERP (sem prefixo — as rotas já trazem `/erp`)

São 126 rotas no total, das quais 104 são a API JSON consumida pelas telas.
Não vale a pena listar uma a uma aqui; o que interessa é o mapa das telas, que
segue os MÓDULOS do §2.1. Autenticação é por **sessão** (login em `/erp/entrar`),
não por secret no payload — é a exceção ao §3.4, porque tem gente usando pela
tela, não máquina chamando.

| Método | Rota | Função |
|---|---|---|
| GET | `/erp/` e `/erp/inicio` | Entrada: escolha de módulo |
| GET/POST | `/erp/entrar`, `/erp/sair` | Login por sessão |
| GET | `/erp/health` | Health check |
| GET | `/erp/lancar` | Lançamento (categoria-first, rateio por obra) |
| GET | `/erp/titulos` | Solicitações de pagamento |
| GET | `/erp/confirmar` | Fila de aprovação, com as críticas destacadas |
| GET | `/erp/pagamentos` | Agenda, baixa em lote, Pix |
| GET | `/erp/prestacao` | Fundo fixo e cartão de crédito |
| GET | `/erp/empreitas`, `/erp/locacoes` | Contratos, medições e locações |
| GET | `/erp/conciliacao` | Conciliação linha a linha por conta |
| GET | `/erp/receber` | Títulos a receber e medições |
| GET | `/erp/relatorios` | Totais por 8 dimensões, DRE gerencial, CSV |
| GET | `/erp/importar` | Importador (Pipefy, OFX, planilhas) |
| GET | `/erp/obras` | Módulo Obras: contrato, aditivos, fases, tributação |
| GET | `/erp/dc`, `/erp/colaboradores` | Módulo Pessoal |
| GET | `/erp/configuracoes` | Plano de contas, operadores, migrações, auditoria |
| GET | `/erp/meu-cadastro` | Dados do próprio operador |
| GET | `/erp/anexo/<id>` | Serve anexo guardado **no banco** (não no Dropbox) |

⚠️ **Anexos do ERP não vão para o Dropbox.** Comprovantes, notas e documentos
ficam no Postgres, comprimidos — decisão consciente, diferente do resto do
monorepo (§6.2). Ao mexer em anexo do ERP, não procure `storage.py`.

---

## 6. Recursos externos

### 6.1 Planilhas Google Sheets

| ID | Nome | Função |
|---|---|---|
| `1lrP1HOvwqyXiVdP2kuTgG7sJjl2QXl0WT4lwkd392DA` | **Registros de SP** (SPsBD) | Fonte da verdade das SPs. **~52k linhas** na aba `SPsBD` (cuidado com memória!). Abas: `Log`, `SPsBD`, `SPsAgendar`, `LogBaixaBradesco` (controle de duplicatas do baixabradesco). Acessada por `atualizaspbotao` e `baixabradesco`. Mapa de colunas da SPsBD documentado em `baixabradesco/sheets.py::row_to_sp_record` (A=ID ... AK=Conta Pagamento). |
| `1C7MWQmr5uFGWuJ18osUNDapiojVXzQ_GxMMDQqxPsBk` | **Base Bancos** | Aba `BaseBancos`: mapa conta bancária → código Omie/Pipefy. Usada pelo `baixabradesco`. |
| `1em1QlCKx1MeleAUqUi3hbpH2Z69p2wyVlNlHMg76-N0` | **Análise de Pagamentos** | Planilha de operação. Existem 5 cópias estruturalmente idênticas, uma por operador. |
| `1PvecWVPcqMmj1o056ZErevt0cjna6ggz48uNFTahu_M` | **SS Espelho** (Suprimentos) | Origem das abas SSEspelho, SSEspelhoRecebidos, SSEspelhoCotações, SSEspelhoRecebimentoPedidos |
| `1JKhvjAUlTuqt2yMbqZNnzk4IGJ57Cx0MwMU4hGH_ajY` | **Registros de Cotação** | Origem da aba Registros |

⚠️ **Outras Análises** (4 cópias da Análise de Pagamentos): IDs ainda a registrar.

⚠️ **Service Account email:** preencher (formato `xxx@xxx.iam.gserviceaccount.com`). Necessário pra confirmar permissões em planilhas novas.

### 6.2 APIs externas

| API | Uso | Módulos |
|---|---|---|
| **Omie** | Cadastro/atualização de títulos a pagar | `atualizaspbotao`, `baixabradesco` |
| **Pipefy** | Movimentação de cards, criação de SPs | `atualizaspbotao`, `baixabradesco` |
| **Z-API** | Envio de WhatsApp (legado, em migração) | `chatbot`, `validasp`, `baixabradesco` |
| **Evolution API** | Envio/recebimento WhatsApp (self-hosted, substitui Z-API) | `whatsapp_gateway` |
| **Dropbox** | Armazenamento PDFs | `pdf_processor`, `chatbot`, `baixabradesco`, `app.py` legado |
| **Google Drive** | Armazenamento PDFs | `pdf_processor`, `email_financeiro` |
| **OpenAI** | Leitura de documento (foto/PDF), sugestão de categoria, leitura de contrato | `erp` |
| **Receita Federal (CNPJ)** | Consulta no cadastro do fornecedor; trava BAIXADA/INAPTA | `erp` |
| **Telegram** | Aviso de título pago, com comprovante | `erp` (via `notificador.py`), `telegram` |

### 6.3 Banco de dados

⚠️ Esta seção mudou com a chegada do ERP. Antes dele valia "não há banco
estruturado"; hoje convivem três situações **diferentes** no mesmo serviço:

- **ERP → Postgres no Render (`erp-db`).** Banco relacional de verdade, 48
  tabelas, acessado via SQLAlchemy por `app/apps/erp/db/database.py`. É a fonte
  da verdade do ERP: títulos, obras, fornecedores, plano de contas, auditoria e
  também os **anexos** (guardados como bytes comprimidos, não no Dropbox).
  Schema evolui por migrações `.sql` numeradas (§3.8).
- **Demais módulos → planilhas Google + `data/links.json`.** Nada mudou:
  `atualizaspbotao`, `baixabradesco` e companhia continuam com a SPsBD como
  fonte da verdade. **O ERP não lê nem escreve nessas planilhas** — são dois
  mundos, ligados só pelo importador do Pipefy e pelo trabalho de migração.
- **Postgres externo (Neon/Supabase)** — segue exclusivo da **Evolution API**
  (serviço separado), pra sessão do WhatsApp. Não é o banco do ERP e não é
  acessado pelo Flask.

O "considerar Supabase pro app no futuro" foi respondido pelo caminho: o ERP
resolveu com Postgres no próprio Render.

---

## 7. Diretrizes pra adicionar novo módulo

Pra criar um novo blueprint (ex.: `sync_logs`):

1. Criar pasta `app/apps/sync_logs/`
2. Arquivos mínimos:
   - `__init__.py` → `from .routes import bp`
   - `routes.py` → cria `bp = Blueprint('sync_logs', __name__)` e rotas
3. Reusar credenciais Google de `GOOGLE_CREDENTIALS_BASE64`.
4. Auth via secret próprio (ex.: `SYNC_LOGS_SECRET`).
5. Registrar em `app/main.py`:
   ```python
   from app.apps.sync_logs import bp as sync_logs_bp
   app.register_blueprint(sync_logs_bp, url_prefix="/api/sync_logs")
   ```
6. Não precisa mexer em `requirements.txt` se usar só libs já instaladas.
7. **Seguir as regras de memória do §3.7** (instância tem só 2 GB).

⚠️ **Isto vale para módulo de integração** (recebe payload, fala com API/planilha,
devolve JSON). Se o que você quer é uma **área nova do ERP** — Suprimentos,
Agenda, Contratos —, não crie blueprint novo: entre pelo `erp`, acrescentando um
módulo na constante `MODULOS`, a regra em `core/<dominio>/` e a migração em
`scripts/migracoes/`. O ERP tem navegação, login, permissão e auditoria próprios;
um blueprint paralelo ficaria fora de tudo isso.

---

## 8. Diretrizes pra Claude em conversas futuras

Quando eu pedir nova feature ou adaptação:

1. **Antes de codar**, considere o que já existe (este documento + arquivos do
   knowledge base do Project). Não duplique.
2. **Reuse padrões** — credenciais, blueprint, formato de erro, logging.
3. **Não introduza novas envvars** sem motivo claro. Reusar as existentes
   sempre que possível.
4. **Não introduza novas dependências** sem avisar.
5. Se faltar info crítica, pergunte UMA vez objetivamente e siga.
6. Português BR em respostas, comentários e logs. Inglês em código.
7. **Foco em deltas:** se for adicionar módulo, mostrar só o módulo novo + a
   linha a adicionar em `main.py`. Não regerar arquivos que não mudaram.
8. **Lembrar:** plano Render pago → serviço não hiberna. Estado persistente
   entre requests ainda deve ir pra planilha auxiliar ou Postgres externo.
9. **Memória é o recurso escasso** (2 GB, histórico de OOM — ver §9). Aplicar
   sempre as regras do §3.7.
10. **Se o assunto for o ERP, leia antes `app/apps/erp/ROTEIRO.md`.** É o
    backlog vivo do módulo, dividido em entregue / em andamento / fila, com os
    princípios que valem para tudo ("amplitude, não recorte"; "nada sem
    categoria, nada sem obra"; "dedutibilidade é do documento") e uma tabela de
    decisões já tomadas. Este CONTEXTO diz como o ERP é feito; o ROTEIRO diz o
    que falta e por quê. Ao entregar um item de lá, marcar `[x]` com o commit.

---

## 9. Histórico de decisões arquiteturais

> Lista para manter contexto de decisões já tomadas.

- **2026-09-04 — Permissão fina por pessoa, sem refazer a matriz de perfis.**
  O dono pediu que cada pessoa tenha uma função principal e, além dela,
  permissões marcadas uma a uma no cadastro. Duas saídas eram possíveis:
  substituir o perfil global por uma matriz área × nível, ou manter o perfil e
  acrescentar exceções. Escolhida a segunda (migração 032, tabela
  `usuario_permissoes`), porque a primeira exigiria refazer a proteção das 125
  rotas antes de o dono ter homologado a que acabou de ser endurecida — e o
  ganho prático que ele descreveu ("deixar outra pessoa autorizando enquanto o
  diretor está de férias") já sai da segunda. Detalhes em §3.9. A dívida está
  escrita: um dia as áreas viram estrutura, não exceção.
- **2026-09-05 — Suprimentos construído, das fases 1 a 5.** Migrações 033 a
  037: cadastros (unidades, condições de pagamento como regra, fornecedor com
  região/porte/cotador, solicitação de cadastro de insumo), solicitação com
  obra por item e 15 situações, mapa de cotação com banco de preços, pedido
  com autorização e previsão de pagamento, e recebimento com pendência como
  saldo do item. O que NÃO entrou, e por quê: o disparo da cotação por e-mail
  (o monorepo não tem envio de e-mail, e a conta é decisão do dono) e a
  conversão da previsão em título (passa pelas regras fiscais do ERP e não
  deve ser contornada por dentro do suprimento). Detalhes e pendências em
  `app/apps/erp/SUPRIMENTOS.md`.
- **2026-09-05 — A ação declarada decide sozinha quem entra.** Uma tela nova
  declarava uma ação e conferia outra por dentro; a homologação com banco
  acusou. Regra registrada no CLAUDE.md: quando duas ações precisam abrir a
  mesma tela, cria-se ação própria com a implicação em `ACOES_IMPLICADAS`.
- **2026-09-04 — Suprimentos: especificação antes de código.** As seis
  planilhas em uso foram lidas e confrontadas com o ditado do dono; o resultado
  está em `app/apps/erp/SUPRIMENTOS.md`, com as decisões dele e o plano em
  cinco fases. O `tests/conftest.py` ganhou o dublê das exceções de permissão
  (`permissoes_por_usuario`) — mudança que atravessa áreas.

- **2026-07-10 — WhatsApp: Z-API → Evolution API (self-hosted).** Escolhida a
  Evolution API (open source, multi-instância, envia mídia) em vez do WAHA
  (grátis limita a 1 número e sem mídia). Estratégia de transição: blueprint
  `whatsapp_gateway` que espelha o dialeto HTTP do Z-API, então no make.com
  troca-se **só o domínio base** da URL. Evolution roda como 2º Web Service no
  Render (Starter, ~US$7/mês) + Postgres externo grátis (Neon/Supabase).
  Detalhes completos no doc `whatsapp_gateway.md`. Sem novas dependências
  (usa `requests`). Código testado localmente (smoke test 17/17); pendente
  deploy da Evolution + criação das instâncias.
- **2026-07-13/14 — Incidente OOM e correções (instância 2 GB).**
  **Sintoma:** instância morria com "Ran out of memory (used over 2GB)" — em
  13/07 a cada ~4 minutos sob carga; após primeira leva de correções, caiu pra
  ~1 falha/dia (pico pontual).
  **Causas identificadas:**
  1. glibc criava arenas demais (container enxerga cores do host) → RSS
     inflado com memória "liberada mas não devolvida".
  2. Worker gunicorn nunca reciclava (sem `--max-requests`) + 8 threads
     dividindo os 2 GB.
  3. `pdf_processor`: downloads inteiros em RAM (`r.content`), documentos
     fitz nunca fechados (`doc.close()` ausente), pixmaps retidos.
  4. `baixabradesco` (o pico residual): (a) range A:AK da SPsBD (~52k linhas,
     ~150-250 MB) baixado **2x por request** pelos dois loaders; (b) pior:
     `execute_spsbd_updates` fazia `get_all_values()` da planilha INTEIRA só
     pra achar 1 linha pelo ID — e rodava em **thread por comprovante**, então
     um lote de N comprovantes disparava N downloads simultâneos da planilha
     (perfil exato do pico de OOM); (c) download de comprovante por URL sem
     teto.
  **Correções aplicadas:**
  1. Envvars `MALLOC_ARENA_MAX=2` + `MALLOC_TRIM_THRESHOLD_=100000` (Render).
  2. Start Command/Procfile: `--threads 8→4`, adicionado
     `--max-requests 150 --max-requests-jitter 40`. (Descoberto que o Start
     Command das Settings sobrescreve o Procfile — ver §2.)
  3. `pdf_processor/__init__.py`: helper `_baixar_para_bio` (streaming +
     timeout 60s + teto 50 MB), `doc.close()` em try/finally, pixmap solto
     após uso, `del`+`gc.collect()` ao fim, flag `incluir_texto`.
  4. `baixabradesco/sheets.py`: nova `load_spsbd_values()` (fetch único de
     A:AK compartilhado pelos loaders); `execute_spsbd_updates` reescrito com
     `batch_get` só das colunas de filtro + `batch_update` único (mesma
     semântica: 1ª linha com match exato, `USER_ENTERED`).
     `baixabradesco/core.py`: fetch compartilhado + `del` após uso; download
     de comprovante em streaming com teto 50 MB (`MAX_ATTACHMENT_BYTES`).
  **Efeito observado:** config (itens 1-2) reduziu de ~15 OOM/hora pra 1/dia.
  Itens 3-4 atacam o pico residual. Se AINDA ocorrer OOM após isso, próximos
  suspeitos: `processarnovasp` (não auditado), `validasp` (não auditado), ou
  subir instância pra 4 GB.
- **2026-08-30 — Nasce o ERP como blueprint do monorepo** (`0976b8f`, primeiro
  de 50 commits até 2026-09-01). Decisões que vieram junto:
  1. **Hospedar dentro do serviço `aplicacoes`**, não em serviço novo — sem
     custo adicional. A contrapartida é dividir os 2 GB com os outros 13
     blueprints, o que explica o pool pequeno e a regra das migrações abaixo.
  2. **Interface HTML própria servida pelo Flask; Streamlit abandonado.** A
     linguagem visual é a dos painéis: topo com abas, filtros laterais, KPIs,
     tabela densa em cartão, detalhe que expande como card.
  3. **Banco relacional próprio (Postgres no Render).** É o primeiro módulo do
     monorepo com banco — daí a §3.8 e a reescrita da §6.3.
  4. **Sistema novo, sem herdar vícios do Omie**: cadastro de fornecedor
     próprio por consulta de CNPJ e plano de contas novo. Nada é importado.
- **2026-08-30 — Migrações por botão, nunca no start do gunicorn.** Uma migração
  que falhasse no boot derrubaria `baixabradesco`, `emissaonf`, `telegram` e o
  gateway junto com o ERP. Então o ADMIN aplica pela tela de Configurações, uma
  transação por arquivo, com a tabela `_migracoes` controlando o que já rodou.
  Pelo mesmo motivo a engine é preguiçosa: sem `DATABASE_URL` o ERP falha
  sozinho e o resto do monorepo sobe normal. **Mesma família de decisão do
  Start Command (§2): nada que seja específico de um módulo pode ter o poder de
  derrubar o processo inteiro.**
- **2026-09-01 — Autorização do ERP invertida para o padrão NEGAR.** Uma
  auditoria das 125 rotas achou 94 sem verificação alguma: qualquer pessoa com
  login baixava qualquer anexo (comprovantes bancários inclusive, por ids
  sequenciais), via chave Pix e código de barras de qualquer parcela, cancelava
  títulos em lote, mexia no plano de contas e entrava na lista de avisos de
  título alheio — passando a receber o comprovante por Telegram.
  **Causa raiz:** `permissoes.exigir()` existia e era bem escrito, mas a
  autorização era *opt-in* por rota, e a maioria não tinha optado. A correção
  não foi tapar 94 buracos: foi inverter o default, para que o esquecimento
  feche em vez de abrir. Detalhe em §3.9.
  **Decisão de negócio que guiou:** cada perfil só vê o que compete à sua
  função, e o detalhe de um registro respeita exatamente o mesmo escopo que a
  listagem — sem exceções de leitura entre obras.
- **2026-09-02 — ERP fora do ar por banco atrasado: a guarda de permissão
  passou a ler o perfil por SQL direto.** A migração 029 acrescentou uma
  coluna ao modelo `Usuario`; o código subiu para o Render antes de alguém
  apertar "Aplicar atualizações do banco". A guarda (`before_request`)
  carregava o `Usuario` pelo ORM em TODA rota, o SELECT pedia a coluna
  inexistente e estourava em "Internal Server Error" — inclusive na tela de
  Configurações, onde fica o botão. O `_perfil_bruto` já existia para o
  caminho de manutenção, mas a guarda do bloco 0 não o usava. **Regra que
  fica:** nada que rode antes de toda rota pode depender do ORM; e erro de
  "coluna não existe" agora vira tela de "banco desatualizado" (503) com a
  instrução, em vez de página branca. Teste estrutural segura os dois.
- **2026-09-02 — Banco de teste descartável no GitHub Actions.** O dono
  pediu para cobrir com banco real o que o dublê não alcança. Avaliação:
  Postgres/Docker no PC exigem instalação e alguém lembrar de rodar; um
  segundo banco no Render custa e fica a um erro da produção. GitHub Actions
  sobe o banco sozinho a cada push, de graça, e é o único caminho que também
  funciona para as sessões do Claude (que não têm Docker). A trava de URL
  (§3.11) foi a primeira coisa escrita.
- **2026-09-02 — O registro de consumo de IA nunca funcionou; corrigido.** O
  commit `fd7e306` criou a tabela, o painel e a tabela de preços — mas a
  função que grava (`_registrar_consumo`) nunca foi escrita: a sugestão de
  conta por IA a importava, a importação falhava dentro de um `except
  Exception`, e o resultado era descartar uma resposta que já tinha custado
  dinheiro. E o painel escrevia num `div` que não existia no HTML. Correção:
  registro no ponto único do leitor (§3.10), operação declarada por contexto,
  cartão na tela, teto mensal com aviso. Lição: funcionalidade "entregue" que
  ninguém abriu na tela não foi entregue — o roteiro de homologação vale para
  isso também.
- **2026-09-02 — Alcance do operador virou configuração, não regra de cargo.**
  A pergunta em aberto era se o administrativo de obra devia ver a obra inteira
  ou só o que ele mesmo lançou. **Resposta do dono: depende da pessoa** — o
  administrativo de uma obra grande precisa da obra inteira, o de outra não. Em
  vez de escolher um dos dois para todo mundo, o alcance virou campo do cadastro
  (§3.9). **A decisão que importa é o default:** quem já está cadastrado fica no
  mais restritivo, e ampliar exige alguém escolher, operador por operador. Uma
  migração que ampliasse alcance sozinha seria um vazamento silencioso.
- **2026-09-02 — Senha do banco trocada no Render e usuário antigo apagado.** O
  `.gitignore` estava com marcadores de conflito de merge commitados dentro
  dele; foi reescrito. A varredura do histórico não achou senha de banco em
  lugar nenhum, mas achou um token da prefeitura colado no código
  (`emissaonf/consultar_status.py`, commit `fa985ab`) — o código passou a ler de
  `EL_NFSE_TOKEN`, e o token continua no histórico até ser trocado na origem.
- **2026-09-01 — Navegação por MÓDULOS** (`9ee893d`). O ERP deixou de ser "um
  financeiro com apêndices": Financeiro, Obras, Pessoal e Administração viraram
  áreas próprias, cada uma com suas telas, e a barra mostra só a do módulo
  corrente. Obras deixou de ser um cadastro e virou área (`43e85e1`).
- **2026-09-03 — `openpyxl` entra no `requirements.txt`** (ramo
  `painel-dre-fiel`). O painel voltou a exportar `.xlsx` em vez de CSV: o
  relatório completo tem oito assuntos, e em CSV isso vira oito arquivos
  soltos. A decisão anterior — "não acrescentar dependência sem combinar" —
  foi revista pelo dono, que pediu Excel. `openpyxl` escreve célula a célula,
  **sem `pandas`**, então não recria o problema de memória da §9 (o painel
  antigo estourava 179 MB só para abrir uma tela). Como mexe no
  `requirements.txt`, atinge os 18 blueprints: publicar reinicia o serviço
  inteiro, e vale a pergunta de sempre sobre carga do painel ou sincronização
  do Análise de SPs em andamento.

- **2026-09-04 — O `baixabradesco` vira a quarta área com chat próprio, e ganha
  memória escrita.** Até aqui a aplicação que dá baixa nos comprovantes
  bancários era a única em produção sem `README.md` nem `HISTORICO.md`: tudo o
  que se sabia dela vivia em resumos de chat, fora do repositório. Passou a ter
  os dois, e entrou na tabela do `CLAUDE.md` (por isso o registro aqui — o
  `CLAUDE.md` atravessa as áreas). O `§5.9` continua sendo o mapa de endpoints;
  o `README.md` da pasta é o detalhe.
- **2026-09-04 — Comprovante que o banco NÃO efetivou passava como pagamento
  feito.** O leitor barrava apenas a frase exata "Operação Não Realizada". Um
  comprovante real de 16/06/2026 dizia **"Transação Não Realizada"** (saldo
  insuficiente, pendente de aprovação) e era lido como boleto normal: valor,
  data, conta de débito e código de barras completos — tudo o que o casador
  precisa para achar a SP de verdade, baixar o título no Omie e marcar a SP
  como paga. Um pagamento que nunca saiu do banco viraria baixa.
  **Correção:** a recusa passou a ser uma lista de frases (`FRASES_RECUSA` em
  `baixabradesco/parser_bradesco.py`), comparada contra o texto já normalizado,
  cobrindo "operação/transação/pagamento não realizada/efetivada/efetuada",
  "não foi efetuada", "pendente de aprovação", "aguardando aprovação" e
  "cancelada". A checagem passou a rodar **antes** de qualquer extração, então
  um comprovante recusado não entrega nem valor nem código de barras. O leitor
  do Sicredi ganhou a mesma trava. E o que era ignorado em silêncio agora
  aparece no resumo da resposta (`recusados_nao_efetivados`), para o Make e para
  quem investiga. Coberto por `tests/test_baixabradesco_recusa.py`, com o
  comprovante real anonimizado como exemplo — inclusive o teste ao contrário,
  que garante que o rodapé "Cancelamentos, Reclamações" de todo comprovante
  Bradesco não barre um pagamento bom.

- **2026-09-04 — A trava contra baixa em duplicidade estava solta, e o Sicredi
  saiu de cena.** A impressão digital de cada página de comprovante era gravada
  na aba `LogBaixaBradesco` e **nunca conferida**: a função existia, era
  importada pelo `core.py` e não era chamada. Quem segurava pagamento repetido
  era o Omie respondendo "título já pago" — proteção de terceiro. Agora a lista
  é lida **uma vez por lote** (`load_fingerprints_processados`) e conferida em
  memória, antes de procurar a SP; a página processada entra na lista do próprio
  lote, cobrindo o PDF repetido dentro do mesmo pedido; o que foi barrado
  aparece no retorno em `duplicados_ja_baixados`. **A leitura única é
  obrigatória**: uma consulta por página recriaria o padrão que derrubou a
  instância em julho de 2026 (§9, item 4b) — há teste segurando isso. Limite
  aceito: a impressão digital inclui o nome do arquivo, então o mesmo PDF
  reenviado com outro nome conta como novo; mudar invalidaria o registro
  histórico. **Na mesma conversa o dono decidiu que a empresa não usa mais o
  Sicredi**: o `parser_sicredi.py` continua no repositório sem ligação com o
  fluxo, e ligar o desvio exige cobrir com teste antes.

---

## 10. Pendências conhecidas

- `app.py` na raiz é legado do pdf-processor antigo. As rotas dele (`/compilar`,
  `/pdf2texto`) parecem duplicadas com `app/apps/pdf_processor/`. Decidir se
  remove um.
- Field `last_row` em `sync.py:105` referencia método `getLastRow()` (camelCase
  típico de Apps Script, não gspread). Provavelmente é dead code, conferir.
- IDs das 4 outras Análises de Pagamentos não registrados aqui.
- **WhatsApp/Evolution:** deploy da Evolution, criar as 2 instâncias + QR,
  preencher `WHATSAPP_GATEWAY_INSTANCES` e trocar as URLs no make. Depois,
  migrar `chatbot`/`validasp`/`baixabradesco` pra apontar ao gateway em vez do
  Z-API direto (ou aposentar `ZAPI_*`).
- **`processarnovasp`:** módulo existe em produção mas não está documentado nem
  foi auditado pra memória. Documentar/auditar na próxima vez que mexer.
- **`validasp`:** não auditado pra memória.
- **Pós-OOM (validar em produção):** na primeira baixa real do `baixabradesco`
  após deploy de 2026-07-14, conferir na SPsBD que O/V/X/AG/AK foram gravados
  (único caminho de ESCRITA alterado). Observar logs por mensagens de teto:
  `❌ Ignorado (excede ...)` (pdf_processor) e `Comprovante excede o limite`
  (baixabradesco) — se aparecerem pra arquivos legítimos, subir os 50 MB.
- **`requirements.txt`:** 4 libs de PDF (PyMuPDF, PyPDF2, pdfplumber, pypdf) —
  consolidar um dia (pdf_processor usa só fitz+PyPDF2). Não mexer sem mapear
  quem usa o quê nos outros módulos.
- O doc `main.py` deste Project está desatualizado em relação à produção.
  A árvore do §2 foi conferida contra o `app/main.py` real em 2026-09-01 e hoje
  são **14 blueprints** registrados; `sync_logs`, `emissaonf`, `telegram` e
  `erp` faltavam aqui.
- **`sync_logs`, `emissaonf` e `telegram`:** registrados em produção, mas sem
  seção própria no §5. Documentar na próxima vez que mexer (o `emissaonf` tem
  ligação prevista com o ERP — ver abaixo).

### 10.1 ERP

O backlog detalhado é o `app/apps/erp/ROTEIRO.md` — 19 itens abertos, com o
porquê de cada um. Não duplicar a lista aqui; o que vale registrar neste
documento é o que atravessa o monorepo:

- **Módulos ainda não iniciados:** Suprimentos (pedido de compra, three-way
  match, cadastro de insumos) e Agenda do ERP (calendário de obrigações, que é
  pré-requisito do alerta de reajuste de obra).
- **Migração do `spsbd`:** geração BeeVale, as checagens de auditoria que ainda
  não vieram e adaptar o robô Bradesco para dar baixa pelo core do ERP em vez
  da planilha. ⚠️ A pasta `app/apps/spsbd_app/` existe na máquina local, é a
  origem desse trabalho e **não está versionada** — não é blueprint registrado
  e não vai para produção.
- **Integração com o `emissaonf`:** emitir a nota fiscal a partir da medição do
  ERP. É o primeiro ponto em que os dois mundos da §6.3 se encostam.
- **Cruzamento estilo tela do Bradesco:** conferir conta e credor do Pix contra
  o que foi lançado.
- **Open Finance / API bancária:** extrato e DDA automáticos (marcado como
  futuro no ROTEIRO).
- ⚠️ **Chave de sessão com fallback fixo.** `app/main.py` resolve
  `app.secret_key` como `ERP_SECRET_KEY` → `SECRET_KEY` → o literal
  `"bws-erp-dev"`. Se as duas envvars faltarem no Render, o login do ERP passa a
  assinar sessão com uma string conhecida e versionada. Confirmar que
  `ERP_SECRET_KEY` está setada em produção — e, quando for mexer, considerar
  falhar na subida em vez de cair no literal.
- **Aviso de pagamento pela metade:** o Telegram está pronto
  (`core/notificacoes.py`, para quem lançou e para os interessados, idempotente
  por pessoa), o WhatsApp não. O caminho já existe — `app/apps/notificador.py`
  expõe `notificar()`, que cobre os dois canais, enquanto o ERP chama só
  `enviar_telegram`.
