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
