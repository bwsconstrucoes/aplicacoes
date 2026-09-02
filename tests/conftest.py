"""Base dos testes do ERP.

O ERP fala com Postgres (JSONB, ARRAY, ENUM do banco), então os models NÃO
sobem em SQLite e não há Postgres de teste nesta máquina. A única DATABASE_URL
existente aponta para a produção no Render, e teste algum encosta nela.

A saída: os models do SQLAlchemy podem ser INSTANCIADOS sem banco nenhum, e a
regra de negócio do ERP mora em funções que recebem a Session como argumento.
Então os testes montam os objetos em memória e passam uma sessão dublada.

O que isso cobre e o que não cobre — importante para ninguém ler mais confiança
do que existe aqui:
  COBRE     a regra de negócio: alçada, cadeia de aprovação, aritmética de
            saldo, custo de conciliação, críticas de duplicidade.
  NÃO COBRE SQL de verdade. `SessaoFalsa` ignora WHERE/JOIN/ORDER BY: ela
            devolve todos os objetos do tipo pedido. Um erro que só apareça no
            filtro de uma query passa despercebido aqui.
"""
from __future__ import annotations

import sys
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
if str(RAIZ) not in sys.path:
    sys.path.insert(0, str(RAIZ))

import pytest  # noqa: E402

from app.apps.erp.db.models.cadastros import (  # noqa: E402
    Colaborador, PerfilUsuario, Usuario, UsuarioObra,
)
from app.apps.erp.db.models.financeiro import (  # noqa: E402
    ContratoMedicao, ContratoServico, ContratoServicoItem, DespesaColaborador,
    Extrato, MedicaoItem, Pagamento, Rateio, StatusTitulo, Titulo, TituloItem,
)


# ---------------------------------------------------------------------------
# Sessão dublada
# ---------------------------------------------------------------------------
class ResultadoFalso:
    """Imita o retorno de Session.scalars()/execute()."""

    def __init__(self, linhas):
        self._linhas = list(linhas)

    def all(self):
        return list(self._linhas)

    def first(self):
        return self._linhas[0] if self._linhas else None

    def __iter__(self):
        return iter(self._linhas)


class SessaoFalsa:
    """Sessão de mentira que devolve objetos guardados em memória.

    `get()` procura por tipo + id. `scalars()` olha a entidade do SELECT e
    devolve TODOS os objetos daquele tipo — sem aplicar WHERE. Cada teste
    carrega só o que é relevante, então isso basta; ver a ressalva no topo.
    `scalar()` (usado nos SUM) consome a fila `escalares`.
    """

    def __init__(self, *objetos, escalares=None, linhas_sql=None):
        self.objetos = list(objetos)
        self.escalares = list(escalares or [])
        self.linhas_sql = list(linhas_sql or [])
        self.adicionados = []
        self.eventos = []

    # -- leitura ------------------------------------------------------------
    def get(self, modelo, ident, options=None):
        for o in self.objetos:
            if isinstance(o, modelo) and getattr(o, "id", None) == ident:
                return o
        return None

    @staticmethod
    def _entidade(stmt):
        try:
            return stmt.column_descriptions[0]["entity"]
        except Exception:  # pragma: no cover - statement sem entidade
            return None

    def scalars(self, stmt):
        entidade = self._entidade(stmt)
        if entidade is None:
            return ResultadoFalso([])
        return ResultadoFalso([o for o in self.objetos if isinstance(o, entidade)])

    def scalar(self, stmt):
        # Sem resposta preparada devolve None, não 0: uma checagem de escopo
        # pergunta "achou?" e 0 seria "achei", liberando o acesso por descuido
        # do dublê. O padrão do dublê tem de errar para o lado que fecha.
        # Os SUM do core já tratam None com `or 0`.
        return self.escalares.pop(0) if self.escalares else None

    def execute(self, stmt, params=None):
        texto = str(stmt)
        if "INSERT INTO eventos" in texto:
            self.eventos.append(params)
            return ResultadoFalso([])
        return ResultadoFalso(self.linhas_sql.pop(0) if self.linhas_sql else [])

    # -- escrita ------------------------------------------------------------
    def add(self, obj):
        self.adicionados.append(obj)

    def flush(self):
        pass

    def commit(self):  # pragma: no cover - nenhum teste comita
        pass


# ---------------------------------------------------------------------------
# Construtores — só os campos que a regra sob teste realmente lê
# ---------------------------------------------------------------------------
def novo_usuario(id_, perfil, nome=None, email=None, **extra):
    return Usuario(
        id=id_, perfil=perfil,
        nome=nome or f"Usuário {id_}",
        email=email or f"u{id_}@bws.test",
        **extra)


def novo_titulo(id_=1, *, solicitante_id=1, status=StatusTitulo.AGUARDANDO_AVAL, **extra):
    return Titulo(id=id_, solicitante_id=solicitante_id, status=status, **extra)


def novo_pagamento(id_, *, valor, data, parcela_id=None):
    return Pagamento(id=id_, valor_pago=Decimal(str(valor)),
                     data_pagamento=data, parcela_id=parcela_id or id_)


def novo_extrato(id_, *, valor, data, nome=None):
    """Saída de caixa é NEGATIVA no extrato; o par com o pagamento tem de zerar."""
    return Extrato(id=id_, valor=Decimal(str(valor)), data_lancamento=data,
                   nome_contraparte=nome)


def novo_item_contrato(id_, *, contrato_id=1, descricao="Serviço", unidade="m2",
                       quantidade="100", preco="10.00", aditivada="0", ordem=1):
    return ContratoServicoItem(
        id=id_, contrato_id=contrato_id, ordem=ordem, descricao=descricao,
        unidade=unidade, quantidade=Decimal(quantidade),
        preco_unitario=Decimal(preco), quantidade_aditivada=Decimal(aditivada))


def nova_medicao_item(id_, *, medicao_id, contrato_item_id, quantidade):
    return MedicaoItem(id=id_, medicao_id=medicao_id,
                       contrato_item_id=contrato_item_id,
                       quantidade=Decimal(str(quantidade)))


def novo_item_titulo(id_, *, titulo_id=1, descricao="", estabelecimento="",
                     documento=None, valor="0.00", data_despesa=None):
    return TituloItem(id=id_, titulo_id=titulo_id, ordem=id_, descricao=descricao,
                      estabelecimento=estabelecimento, documento=documento,
                      valor=Decimal(valor), data_despesa=data_despesa)


# ---------------------------------------------------------------------------
# Atalhos usados por vários testes
# ---------------------------------------------------------------------------
@pytest.fixture
def perfis():
    return PerfilUsuario


@pytest.fixture
def hoje():
    return date.today()


# ===========================================================================
# Banco de VERDADE — só para os testes marcados com @pytest.mark.banco
#
# A sessão dublada acima ignora WHERE/JOIN; o escopo por obra e por autoria
# vive exatamente no WHERE. Então esses testes rodam contra um Postgres
# descartável: esquema + migrações aplicados do zero, dados criados pelo
# próprio teste, e cada teste dentro de uma transação que é desfeita no fim.
#
# A TRAVA é o que importa aqui: a única DATABASE_URL conhecida na empresa é a
# da produção. Este bloco só aceita ERP_TEST_DATABASE_URL apontando para um
# host local e para um banco cujo nome contenha "teste". Qualquer outra coisa
# derruba a suíte com mensagem clara — nunca um "ops" contra o Render.
# ===========================================================================
import contextlib  # noqa: E402
import os  # noqa: E402
from urllib.parse import urlparse  # noqa: E402

VARIAVEL_BANCO_TESTE = "ERP_TEST_DATABASE_URL"
_HOSTS_LOCAIS = {"localhost", "127.0.0.1", "::1"}


def url_de_teste_segura(url: str) -> str:
    """Devolve a URL se — e só se — for de um banco local e de teste."""
    partes = urlparse(url)
    host = (partes.hostname or "").lower()
    nome = partes.path.lstrip("/").lower()
    problemas = []
    if host not in _HOSTS_LOCAIS:
        problemas.append(f"host '{host or '?'}' não é local")
    if "teste" not in nome:
        problemas.append(f"nome do banco '{nome or '?'}' não contém 'teste'")
    if not partes.scheme.startswith("postgres"):
        problemas.append("não é uma URL de Postgres")
    if problemas:
        raise RuntimeError(
            f"{VARIAVEL_BANCO_TESTE} recusada ({'; '.join(problemas)}). A suíte só "
            f"roda contra um Postgres LOCAL e DESCARTÁVEL, com 'teste' no nome — "
            f"nunca contra o banco da empresa.")
    return url


@pytest.fixture(scope="session")
def banco():
    """Aponta o processo para o banco de teste e o reconstrói do zero:
    esquema base + todas as migrações, pelos mesmos scripts da produção."""
    bruto = os.environ.get(VARIAVEL_BANCO_TESTE, "").strip()
    if not bruto:
        pytest.skip(f"{VARIAVEL_BANCO_TESTE} não definida — testes com banco pulados")
    url = url_de_teste_segura(bruto)

    from sqlalchemy import text
    from app.apps.erp.db import database
    from app.apps.erp.core.comum.migracoes import aplicar_pendentes

    os.environ["DATABASE_URL"] = url          # só neste processo, só para teste
    database.reiniciar_engine()
    eng = database.obter_engine()
    schema = (RAIZ / "app" / "apps" / "erp" / "schema.sql").read_text(encoding="utf-8")
    with eng.connect() as conn:
        conn.execute(text("DROP SCHEMA public CASCADE; CREATE SCHEMA public;"))
        conn.execute(text(schema))
        conn.commit()
    resultado = aplicar_pendentes()
    assert not resultado.get("erro"), f"migração falhou no banco de teste: {resultado}"
    yield eng
    database.reiniciar_engine()


@pytest.fixture
def sessao_real(banco):
    """Sessão de verdade dentro de UMA transação desfeita no fim do teste.

    `commit()` dentro do teste (ou dentro de uma rota) só libera um savepoint;
    a transação de fora nunca é confirmada — o banco volta limpo."""
    from sqlalchemy.orm import Session
    conn = banco.connect()
    trans = conn.begin()
    s = Session(bind=conn, join_transaction_mode="create_savepoint",
                autoflush=False, expire_on_commit=False)
    try:
        yield s
    finally:
        s.close()
        trans.rollback()
        conn.close()


@pytest.fixture
def app_real(sessao_real, monkeypatch):
    """App do ERP cujas rotas usam a MESMA sessão real do teste — assim o que
    a rota grava é desfeito junto, e o que ela lê é o cenário montado."""
    from flask import Flask
    from app.apps.erp import routes

    @contextlib.contextmanager
    def _mesma_sessao():
        yield sessao_real
    monkeypatch.setattr(routes, "get_session", _mesma_sessao)

    a = Flask(__name__)
    a.secret_key = "teste"
    a.register_blueprint(routes.bp)
    return a


def como(app, usuario_id: int):
    """Cliente HTTP já logado como o usuário dado."""
    c = app.test_client()
    with c.session_transaction() as sessao:
        sessao["erp_usuario_id"] = usuario_id
    return c
