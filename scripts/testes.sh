#!/usr/bin/env bash
# ============================================================================
# RODAR A SUÍTE — um comando só.
#
# ⚠️ POR QUE ESTE ARQUIVO EXISTE. Cobrança do dono em 29/09/2026: *"está demorando
# meia hora, uma hora para fechar alguma coisa relativamente simples"*. Parte do
# tempo era a suíte; outra parte era a dança em volta dela — subir o Postgres,
# lembrar da variável de ambiente, descobrir que o banco tinha morrido, rodar de
# novo. Aqui isso vira um comando.
#
#   scripts/testes.sh                      a suíte inteira, em paralelo
#   scripts/testes.sh tests/test_x.py      só um arquivo (sem paralelismo)
#   scripts/testes.sh -k alguma_coisa      o que casar com o nome
#
# ⚠️ E CONSERTO PEQUENO NÃO RODA A SUÍTE INTEIRA — ver o CLAUDE.md. Rodar 7.700
# testes para trocar uma linha de CSS não é cuidado, é uma hora parada.
# ============================================================================
set -euo pipefail
cd "$(dirname "$0")/.."

PGDATA="${PGDATA:-/var/lib/postgresql/tstdata}"
PGBIN="${PGBIN:-/usr/lib/postgresql/16/bin}"
BANCO="${ERP_TEST_DATABASE_URL:-postgresql://postgres:teste@localhost:5432/erp_teste}"

# ⚠️ O POSTGRES PRECISA SER CONFERIDO A CADA VEZ, não uma vez por sessão: neste
# ambiente ele é um processo de fundo e morre sozinho entre um comando e outro.
# Descobrir isso pelo erro dos testes custa uma rodada inteira.
if ! pg_isready -q -h localhost -U postgres -d postgres 2>/dev/null; then
  echo "→ subindo o Postgres de teste…"
  # ⚠️ SEM DURABILIDADE, DE PROPÓSITO: este banco é descartável. `fsync=off` e
  # companhia cortam a maior parte da espera de disco. NUNCA em produção — com
  # fsync desligado, uma queda de energia corrompe o banco.
  su postgres -c "$PGBIN/pg_ctl -D $PGDATA -l /tmp/pg.log -o '-k /var/run/postgresql \
      -c fsync=off -c synchronous_commit=off -c full_page_writes=off \
      -c checkpoint_timeout=1h -c max_wal_size=4GB -c autovacuum=off' start" >/dev/null
fi

# ⚠️ O BANCO DE TESTE ENGORDA COM O USO e, quando engorda, a consulta que mede o
# tamanho dele passa a levar SEGUNDOS por teste — foram 14 s por teste em
# 29/09/2026, com 1,4 milhão de arquivos acumulados de schemas derrubados. Aqui
# ele é recriado quando passa do teto; a suíte o reconstrói sozinha.
ARQUIVOS=$(find "$PGDATA/base" -type f 2>/dev/null | wc -l)
# 300 mil é o teto: uma rodada inteira deixa ~47 mil desde que a limpeza entre
# testes passou a ser `DELETE` em vez de `TRUNCATE` (era mais de um milhão por
# rodada antes disso). Só engordando por semanas se chega perto do ponto em que a
# lentidão volta a doer.
if [ "$ARQUIVOS" -gt 300000 ]; then
  echo "→ o banco de teste acumulou $ARQUIVOS arquivos; recriando…"
  PGPASSWORD=teste psql -q -h localhost -U postgres -d postgres \
    -c "DROP DATABASE IF EXISTS erp_teste WITH (FORCE);" \
    -c "CREATE DATABASE erp_teste;" >/dev/null
fi

export ERP_TEST_DATABASE_URL="$BANCO"

# Com alvo (um arquivo, um -k), roda direto: o paralelismo custa uns segundos
# para subir os trabalhadores e não compensa em pouca coisa.
if [ "$#" -gt 0 ]; then
  exec python3 -m pytest "$@"
fi
exec python3 -m pytest -n auto --dist loadfile
