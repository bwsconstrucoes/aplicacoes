"""Críticas de duplicidade do fundo fixo (core/titulos/prestacao.py).

As três que interessam:
  F13  o comprovante já foi usado em prestação anterior
  F14  despesa idêntica (mesma data, valor e descrição) já prestada
  F15  a mesma despesa aparece duas vezes DENTRO da prestação
As três são BLOQUEIA — evidência objetiva de repetição, não palpite.
"""
from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

import pytest

from app.apps.erp.core.titulos import prestacao
from app.apps.erp.db.models.cadastros import PerfilUsuario as P
from app.apps.erp.db.models.financeiro import StatusTitulo

from conftest import SessaoFalsa, novo_item_titulo, novo_titulo, novo_usuario


def dia_util(dias_atras=3):
    """Data recente que não caia em fim de semana (senão a crítica F7 entra
    no meio e polui a asserção)."""
    d = date.today() - timedelta(days=dias_atras)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d


def operador(id_=7, *, autorizado=True, teto_item="500.00",
             teto_prestacao="5000.00", saldo="0"):
    return novo_usuario(
        id_, P.ADMINISTRATIVO_OBRA, nome="João Operador",
        ff_autorizado=autorizado,
        ff_teto_item=Decimal(teto_item) if teto_item else None,
        ff_teto_prestacao=Decimal(teto_prestacao) if teto_prestacao else None,
        ff_saldo_adiantamento=Decimal(saldo))


def prestacao_anterior(id_=1, *, valor="45.00"):
    """Prestação já fechada da mesma pessoa, para alimentar o histórico.

    `competencia` e `numero_sp` são NOT NULL no banco; o histórico os lê para
    montar a lista das últimas prestações, então precisam estar preenchidos.
    """
    return novo_titulo(id_, solicitante_id=7, status=StatusTitulo.PAGO,
                       modalidade="FUNDO_FIXO", numero_sp=f"SP-{id_:04d}",
                       competencia=date.today().replace(day=1),
                       valor_liquido=Decimal(valor))


def item(valor="45.00", *, descricao="Almoço da equipe", estabelecimento="PADARIA CENTRAL",
         documento=None, data=None, obra_id=10, anexo_id=1, **extra):
    return {"valor": valor, "descricao": descricao,
            "estabelecimento": estabelecimento, "documento": documento,
            "data_despesa": (data or dia_util()).isoformat(),
            "obra_id": obra_id, "anexo_id": anexo_id, **extra}


def codigos(resultado, indice=None):
    """Códigos das críticas: de uma linha, ou de todas + gerais."""
    if indice is not None:
        return {c["codigo"] for c in resultado["por_item"].get(str(indice), [])}
    de_itens = {c["codigo"] for lista in resultado["por_item"].values() for c in lista}
    return de_itens | {c["codigo"] for c in resultado["gerais"]}


# ---------------------------------------------------------------------------
# O caso limpo — a régua das demais asserções
# ---------------------------------------------------------------------------
def test_despesa_legitima_nao_gera_bloqueio_nem_critica():
    s = SessaoFalsa(operador())

    r = prestacao.criticar(s, [item()], solicitante_id=7)

    assert r["bloqueios"] == 0
    assert r["criticas"] == 0
    assert r["exige_atencao"] is False
    assert r["soma"] == 45.0


# ---------------------------------------------------------------------------
# F15 — repetição dentro da própria prestação
# ---------------------------------------------------------------------------
def test_mesma_despesa_duas_vezes_na_prestacao_bloqueia_as_duas_linhas():
    s = SessaoFalsa(operador())
    repetida = item(valor="45.00", documento="12345", estabelecimento="PADARIA CENTRAL")

    r = prestacao.criticar(s, [repetida, dict(repetida)], solicitante_id=7)

    assert "F15" in codigos(r, 0)
    assert "F15" in codigos(r, 1)
    assert r["bloqueios"] >= 2


def test_duas_despesas_diferentes_no_mesmo_estabelecimento_passam():
    """Comprar duas vezes na mesma padaria não é duplicidade."""
    s = SessaoFalsa(operador())
    itens = [item(valor="45.00", documento="111"),
             item(valor="32.00", documento="222")]

    r = prestacao.criticar(s, itens, solicitante_id=7)

    assert "F15" not in codigos(r)
    assert r["bloqueios"] == 0


def test_repeticao_conta_quantas_vezes_apareceu():
    s = SessaoFalsa(operador())
    repetida = item(valor="45.00", documento="12345")

    r = prestacao.criticar(s, [repetida, dict(repetida), dict(repetida)],
                           solicitante_id=7)

    mensagem = " ".join(c["msg"] for c in r["por_item"]["0"])
    assert "3×" in mensagem


# ---------------------------------------------------------------------------
# F13 — comprovante já usado em prestação anterior
# ---------------------------------------------------------------------------
def test_comprovante_ja_usado_em_prestacao_anterior_bloqueia():
    anterior = prestacao_anterior(1)
    ja_usado = novo_item_titulo(1, titulo_id=1, descricao="ALMOCO",
                                estabelecimento="PADARIA CENTRAL",
                                documento="12345", valor="45.00")
    s = SessaoFalsa(operador(), anterior, ja_usado)

    r = prestacao.criticar(
        s, [item(valor="45.00", documento="12345",
                 estabelecimento="PADARIA CENTRAL")], solicitante_id=7)

    assert "F13" in codigos(r, 0)


def test_documento_diferente_no_mesmo_estabelecimento_nao_bloqueia():
    anterior = prestacao_anterior(1)
    ja_usado = novo_item_titulo(1, titulo_id=1, descricao="ALMOCO",
                                estabelecimento="PADARIA CENTRAL",
                                documento="12345", valor="45.00")
    s = SessaoFalsa(operador(), anterior, ja_usado)

    r = prestacao.criticar(
        s, [item(valor="45.00", documento="99999",
                 estabelecimento="PADARIA CENTRAL")], solicitante_id=7)

    assert "F13" not in codigos(r, 0)


# ---------------------------------------------------------------------------
# F14 — despesa idêntica já prestada (mesma data, valor e descrição)
# ---------------------------------------------------------------------------
def test_despesa_identica_ja_prestada_bloqueia_mesmo_sem_documento():
    quando = dia_util()
    anterior = prestacao_anterior(1)
    igual = novo_item_titulo(1, titulo_id=1, descricao="Almoço da equipe",
                             estabelecimento="PADARIA CENTRAL",
                             valor="45.00", data_despesa=quando)
    s = SessaoFalsa(operador(), anterior, igual)

    r = prestacao.criticar(
        s, [item(valor="45.00", descricao="Almoço da equipe", data=quando)],
        solicitante_id=7)

    assert "F14" in codigos(r, 0)


def test_mesma_descricao_em_data_diferente_nao_bloqueia():
    """Almoçar de novo noutro dia é vida normal, não fraude."""
    anterior = prestacao_anterior(1)
    igual = novo_item_titulo(1, titulo_id=1, descricao="Almoço da equipe",
                             estabelecimento="PADARIA CENTRAL", valor="45.00",
                             data_despesa=dia_util(10))
    s = SessaoFalsa(operador(), anterior, igual)

    r = prestacao.criticar(
        s, [item(valor="45.00", descricao="Almoço da equipe", data=dia_util(3))],
        solicitante_id=7)

    assert "F14" not in codigos(r, 0)


# ---------------------------------------------------------------------------
# Conjunto: soma, tetos e autorização
# ---------------------------------------------------------------------------
def test_soma_dos_itens_tem_de_fechar_com_o_total_declarado():
    s = SessaoFalsa(operador())

    r = prestacao.criticar(s, [item(valor="45.00")], solicitante_id=7,
                           total_declarado="90.00")

    assert "F16" in codigos(r)
    assert r["bloqueios"] >= 1


def test_soma_que_fecha_nao_reclama():
    s = SessaoFalsa(operador())

    r = prestacao.criticar(s, [item(valor="45.00")], solicitante_id=7,
                           total_declarado="45.00")

    assert "F16" not in codigos(r)


def test_despesa_acima_do_teto_da_pessoa_e_critica():
    s = SessaoFalsa(operador(teto_item="100.00"))

    r = prestacao.criticar(s, [item(valor="480.00")], solicitante_id=7)

    assert "F1" in codigos(r, 0)


def test_total_acima_do_teto_da_prestacao_bloqueia():
    s = SessaoFalsa(operador(teto_item="500.00", teto_prestacao="100.00"))

    r = prestacao.criticar(s, [item(valor="480.00")], solicitante_id=7)

    assert "F22" in codigos(r)
    assert r["bloqueios"] >= 1


def test_pessoa_sem_autorizacao_de_fundo_fixo_e_critica():
    s = SessaoFalsa(operador(autorizado=False))

    r = prestacao.criticar(s, [item()], solicitante_id=7)

    assert "F20" in codigos(r)


def test_pessoa_sem_teto_cadastrado_gera_alerta():
    s = SessaoFalsa(operador(teto_item=None))

    r = prestacao.criticar(s, [item()], solicitante_id=7)

    assert "F21" in codigos(r)


# ---------------------------------------------------------------------------
# Críticas de linha que sustentam o resto
# ---------------------------------------------------------------------------
def test_despesa_sem_obra_bloqueia():
    """Nada sem centro de custo."""
    s = SessaoFalsa(operador())

    r = prestacao.criticar(s, [item(obra_id=None)], solicitante_id=7)

    assert "F9" in codigos(r, 0)


def test_data_no_futuro_bloqueia():
    s = SessaoFalsa(operador())

    r = prestacao.criticar(
        s, [item(data=date.today() + timedelta(days=2))], solicitante_id=7)

    assert "F4" in codigos(r, 0)


def test_fundo_fixo_sem_comprovante_anexado_e_critica():
    s = SessaoFalsa(operador())

    r = prestacao.criticar(s, [item(anexo_id=None)], solicitante_id=7)

    assert "F12" in codigos(r, 0)


def test_cartao_exige_categoria_em_cada_compra():
    s = SessaoFalsa(operador())

    r = prestacao.criticar(s, [item(anexo_id=None)], solicitante_id=7,
                           modalidade="CARTAO")

    assert "F10" in codigos(r, 0)


def test_descricao_vaga_e_critica():
    s = SessaoFalsa(operador())

    r = prestacao.criticar(s, [item(descricao="x")], solicitante_id=7)

    assert "F8" in codigos(r, 0)


def test_despesa_fora_do_periodo_da_prestacao_e_critica():
    s = SessaoFalsa(operador())

    r = prestacao.criticar(s, [item(data=dia_util(30))], solicitante_id=7,
                           periodo_inicio=date.today() - timedelta(days=7),
                           periodo_fim=date.today())

    assert "F6" in codigos(r, 0)


# ---------------------------------------------------------------------------
# Conversão de valor — "30.00" é trinta reais, "1.234" é mil e duzentos
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("entrada, esperado", [
    ("30.00", "30.00"),
    ("1.234", "1234.00"),
    ("1.234,56", "1234.56"),
    ("R$ 1.234,56", "1234.56"),
    ("45,90", "45.90"),
])
def test_leitura_de_valor_decimal(entrada, esperado):
    assert prestacao._dec(entrada) == Decimal(esperado)
