# CLAUDE.md

Convenções deste repositório. Para o terreno completo — estrutura, endpoints,
variáveis de ambiente, histórico de decisões — ver `CONTEXTO.md` na raiz. Para o
backlog do ERP, `app/apps/erp/ROTEIRO.md`.

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

**Não existe suíte automatizada neste repositório** — sem `pytest`, sem CI. Não
invente um `pytest` que não roda; testar aqui é manual:

1. **A aplicação sobe?** `python app/main.py` (porta 5000 por padrão). Uma
   importação quebrada em qualquer módulo derruba os 14 blueprints juntos, então
   isso não é formalidade.
2. **Exercitar a tela ou a rota que você tocou**, com dado real de verdade.
3. **Se mexeu no ERP:** aplicar as migrações pendentes pelo botão e conferir
   que a tela afetada carrega. Migração nova roda uma vez — reler o `.sql`
   antes, porque metade aplicada dá trabalho para desfazer.
4. **Se mexeu em memória ou em leitura de planilha:** ver `CONTEXTO.md` §3.7.
   Nada de `get_all_values()` em aba grande, nada de download sem teto.

Commitar só o que foi verificado. Se algo não deu para testar, dizer isso na
mensagem do commit em vez de deixar implícito.

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
