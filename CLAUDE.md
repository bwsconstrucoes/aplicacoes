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

## Quatro áreas, quatro chats — e a memória fica no repositório

O dono trabalha com **um chat por área**, na nuvem (claude.ai/code), sem
depender do Claude Code no PC. Cada chat nasce de uma cópia limpa da `main` e
**não lembra de nada** que outro chat conversou. O que atravessa sessões é
**só o que está escrito no repositório**. Por isso:

| Área | Pasta | Leia ANTES de mexer | Registre o estado em |
|---|---|---|---|
| ERP financeiro | `app/apps/erp/` | `ROTEIRO.md`, `HISTORICO.md`, `CONTEXTO.md` §2.1 e §3.8–3.11 | `HISTORICO.md` e `ROTEIRO.md` |
| Painel OMIE | `app/apps/painel/` | `README.md`, `HISTORICO.md` | `HISTORICO.md` |
| Análise de SPs | `app/apps/analisesps/` | `README.md`, `HISTORICO.md` | `HISTORICO.md` |
| BaixaBradesco (baixa de comprovantes) | `app/apps/baixabradesco/` | `README.md`, `HISTORICO.md`, `CONTEXTO.md` §5.9 e §9 | `HISTORICO.md` |

**Ao começar** uma sessão numa área: ler os arquivos da linha, conferir a
seção "Pendente AGORA" e **perguntar ao dono** se aquilo já aconteceu — o
estado do mundo pode ter mudado desde que o arquivo foi escrito.

**Antes de encerrar, ou sempre que uma entrega ficar pronta**, atualizar o
`HISTORICO.md` da área: onde o trabalho está, o que está pendente agora, as
decisões tomadas (com o motivo), os incidentes e o que ficou de fora. Escrever
para quem nunca viu a conversa. Uma sessão que termina sem atualizar o
histórico **perdeu** o que aprendeu — o chat não é memória, o repositório é.

**Mudança que atravessa áreas** (este arquivo, `CONTEXTO.md`, `tests/conftest.py`,
`.github/workflows/`, `app/main.py`, `requirements.txt`) é registrada em
`CONTEXTO.md` › "Histórico de decisões", além do histórico da área.

**Juntar na `main` publica no Render na hora.** Regras:

1. Só com o dono dizendo "pode". Trabalho em ramo é seguro; a `main` é produção.
2. **Perguntar antes se há carga do painel ou sincronização do Análise de SPs
   em andamento**: publicar reinicia o serviço e mata o trabalho longo. Já
   aconteceu, causado por outro chat que não sabia.
3. Ramo com migração de banco: avisar o dono para apertar "Aplicar
   atualizações do banco" **no mesmo momento** da junção — ERP em
   Configurações, painel e Análise de SPs nas telas de configuração deles.
4. Se a `main` andou, trazer a `main` para o ramo, rodar a suíte, e só então
   juntar. O GitHub Actions roda a suíte com banco de verdade a cada envio.

**Mensagem de abertura** que o dono usa num chat novo (basta trocar a área):

> Trabalhe na área **[ERP / Painel OMIE / Análise de SPs / BaixaBradesco]** deste
> repositório.
> Leia o `CLAUDE.md` e os arquivos da área indicados nele, me diga em que pé o
> trabalho está e o que consta como pendente, e confirme comigo antes de
> começar. Não mexa nas outras áreas. Trabalhe no seu ramo e me pergunte antes
> de juntar na linha principal.

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
- **Coluna nova no modelo `Usuario` derruba o ERP se a migração não estiver
  aplicada** — o código sobe para o Render antes do botão ser apertado. Por
  isso nada que rode antes de toda rota (a guarda de permissão, o login) pode
  carregar o `Usuario` pelo ORM: usa-se `_perfil_bruto` / SQL direto. Ao
  juntar um ramo com migração, avisar o dono para apertar o botão **no mesmo
  momento**, não depois.
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

**O que o dublê não alcança roda com banco de verdade**, nos testes marcados
`@pytest.mark.banco` (hoje: o escopo por obra e por autoria, que vive no
`WHERE`). Eles precisam de `ERP_TEST_DATABASE_URL` apontando para um Postgres
**local e descartável, com "teste" no nome** — o `conftest.py` recusa qualquer
outra coisa, então a produção não é alcançável por aqui. Sem a variável, são
pulados e a suíte segue. O GitHub Actions (`.github/workflows/testes.yml`) sobe
esse banco sozinho a cada envio; no PC, `docker-compose.teste.yml` faz o mesmo.
Regra de escopo nova ganha um caso ali, não só no dublê.

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
