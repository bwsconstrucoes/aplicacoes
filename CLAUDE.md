# CLAUDE.md

Convenções deste repositório. Para o terreno completo — estrutura, endpoints,
variáveis de ambiente, histórico de decisões — ver `CONTEXTO.md` na raiz. Para o
backlog do ERP, `app/apps/erp/ROTEIRO.md`.

## Como responder ao dono da empresa

Quem pede o trabalho é o dono da BWS e cliente do ERP — **não é programador**.
Isso muda a resposta, não o cuidado com o código.

- **Português simples, sem jargão.** Termo técnico inevitável vem com uma linha
  de explicação junto.
- **Diga o efeito, não a implementação.** O que muda para quem usa o sistema, o
  que passa a ser possível ou impossível, o que a equipe vai sentir. Nome de
  arquivo, de função e de biblioteca só quando forem necessários para a decisão.
- **Nada de código na resposta**, a menos que seja pedido.
- **Risco e escolha, sempre.** Trade-off, o que ficou de fora e o que não foi
  verificado são decisão de negócio — e a decisão é dele. Não esconder atrás de
  "está pronto".

## Idioma

- **Código em inglês:** nomes de variáveis, funções, classes, argumentos.
- **Português em tudo que uma pessoa lê:** comentários, docstrings, mensagens de
  erro exibidas ao usuário e mensagens de log.

```python
def load_spsbd_values(sheet_id: str) -> list[list[str]]:
    """Busca o range A:AK da SPsBD uma única vez por request."""
    logger.info("SPsBD: carregando intervalo A:AK")
    if not sheet_id:
        raise ValueError("ID da planilha não informado.")
```

## Migrações do ERP

- Toda mudança de schema é um **`.sql` numerado** em
  `app/apps/erp/scripts/migracoes/` (sequência atual: `001` … `028`). Nunca
  `ALTER TABLE` solto, nunca `create_all()` em produção.
- **São aplicadas só pelo botão "Aplicar atualizações do banco"**, na tela de
  Configurações do ERP, por um usuário ADMIN. A tabela `_migracoes` controla o
  que já rodou; cada arquivo roda na sua própria transação.
- **Nunca no start do gunicorn.** O ERP divide processo com `baixabradesco`,
  `emissaonf`, `telegram` e o gateway: uma migração com defeito no boot
  derrubaria o monorepo inteiro, não só o ERP.
- Pelo mesmo motivo a engine do banco é **preguiçosa** (`db/database.py`): a
  conexão só nasce na primeira consulta. Não mover conexão para o topo do
  módulo — sem `DATABASE_URL`, o ERP deve falhar sozinho e deixar os outros 13
  blueprints subirem.

## Gunicorn

⚠️ **Há uma divergência a confirmar.** O `Procfile` versionado está com
**1 worker e 4 threads**:

```
web: gunicorn app.main:app --timeout 3600 --graceful-timeout 120 --keep-alive 120 \
     --workers 1 --threads 4 --worker-class gthread \
     --max-requests 150 --max-requests-jitter 40 --log-level info
```

As `4` threads vieram do commit `352782d` ("reduz threads e adiciona
max-requests p/ conter OOM"): a instância tem 2 GB e, com 8 threads, morria de
OOM em julho de 2026 (`CONTEXTO.md` §9). Há indicação de que a produção esteja
rodando com **8 threads** — o que é possível porque **o campo Start Command nas
Settings do Render sobrescreve o Procfile**.

Antes de mexer nesse comando: conferir qual dos dois vale hoje e alinhar os
dois lugares. Se 8 for mesmo o valor em produção, vigiar memória — foi essa a
configuração associada ao OOM.

- `--workers 1` é obrigatório e não está em discussão: há estado em memória por
  processo (sessão do `chatbot`), que quebra com mais de um worker.

## Antes de commitar

### 1. Rodar a suíte

```
pip install -r requirements-dev.txt
python -m pytest
```

São 108 testes sobre os fluxos críticos do ERP, em `tests/`: aval em duas
pessoas, atribuição ótima da conciliação, medição de empreita consumindo saldo
de item, cadeia supervisor → DP → diretor, e as críticas de duplicidade do
fundo fixo. Rodam em menos de um segundo — não há desculpa para pular.

**A suíte não sobe banco**, e é importante saber por quê antes de confiar nela:
os models usam `JSONB`, `ARRAY` e ENUM do Postgres, que não existem em SQLite, e
a única `DATABASE_URL` configurada aponta para a **produção** — nenhum teste
encosta nela. Em vez disso os models são instanciados em memória e a `Session` é
dublada (`tests/conftest.py`).

O limite disso, dito sem rodeio: a sessão dublada **ignora `WHERE`, `JOIN` e
`ORDER BY`** — devolve todos os objetos do tipo pedido. A suíte cobre a regra de
negócio (alçada, cadeia de aprovação, aritmética de saldo, custo de conciliação,
críticas). Ela **não** cobre SQL: um erro que só apareça no filtro de uma query
passa batido. Por isso os passos 2 a 4 continuam valendo.

Ao escrever teste novo, reusar `SessaoFalsa` e os construtores do `conftest.py`,
e preencher os campos `NOT NULL` que a regra sob teste realmente lê — um objeto
pela metade falha por motivo errado e faz perder tempo.

### 2. A aplicação sobe?

`python app/main.py` (porta 5000 por padrão). Uma importação quebrada em
qualquer módulo derruba os 14 blueprints juntos, então isso não é formalidade —
e é justamente o que a suíte, sozinha, não garante.

### 3. Exercitar a tela ou a rota que você tocou

Com dado real de verdade. Se mexeu no ERP, aplicar as migrações pendentes pelo
botão e conferir que a tela afetada carrega. Migração nova roda uma vez — reler
o `.sql` antes, porque metade aplicada dá trabalho para desfazer.

### 4. Se mexeu em memória ou em leitura de planilha

Ver `CONTEXTO.md` §3.7. Nada de `get_all_values()` em aba grande, nada de
download sem teto. Esse caminho não tem teste automatizado nenhum.

Commitar só o que foi verificado. Se algo não deu para testar, dizer isso na
mensagem do commit em vez de deixar implícito.

## Autorização no ERP: o padrão é NEGAR

Rota nova no ERP tem **duas** obrigações. Esquecer qualquer uma quebra a suíte
antes de chegar à produção — é de propósito.

1. **Declarar a ação exigida**, com `@permissao("pagar")` — ou por método,
   `@permissao(GET="ver_erp", POST="configurar")`, quando ler e escrever têm
   pesos diferentes. Rota sem declaração é recusada pelo guard, não liberada.
   Rota realmente pública usa `@permissao_publica("motivo")`, com o motivo
   escrito.
2. **Conferir o escopo do objeto**, quando a rota recebe o número de um
   registro (`<int:titulo_id>`, `<int:obra_id>`, …) e a ação é ampla o
   bastante para alcançar perfil preso a obra ou a autoria. Use o
   `exigir_*_no_escopo` correspondente, em `core/auth/permissoes.py`.

As duas coisas são diferentes: a primeira responde "este perfil pode esta
ação?"; a segunda, "pode NESTE registro?". Ter alçada para lançar não autoriza
a mexer no título da obra de outro.

**Fora do escopo responde 404 "não encontrado", nunca 403 "sem permissão".**
Dizer "sem permissão" para um número que existe confirma a existência dele, e
varrer os números mapearia o sistema sem abrir um registro. Levante
`ErroNaoEncontrado` e deixe o errorhandler do blueprint responder — se a rota
tiver um `except Exception`, reerga a exceção antes dele.

Nunca escreva escopo novo à mão: `pode_ver_titulo` passa pelo mesmo
`aplicar_escopo` da listagem, e é isso que garante que detalhe e lista não
divirjam. Se a regra de escopo mudar, muda num lugar só.

## Padrões que já existem — reusar, não recriar

- Resposta JSON: `{'ok': True, ...}` / `{'ok': False, 'erro': '...'}`.
- Credencial Google: sempre `GOOGLE_CREDENTIALS_BASE64`. Não criar envvar nova
  para credencial.
- Autenticação: secret por módulo (`<MODULO>_SECRET`) no payload. O ERP é a
  exceção — usa sessão, porque tem gente operando pela tela.
- Módulo novo de integração é blueprint em `app/apps/<nome>/` registrado no
  `app/main.py`. Mas **área nova do ERP** (Suprimentos, Agenda, Contratos) entra
  dentro do `erp`, não como blueprint paralelo: lá já existem navegação, login,
  permissão e auditoria.
- Regra de negócio do ERP mora em `core/<dominio>/`, nunca no `routes.py`.
- Não introduzir dependência nova sem avisar.
