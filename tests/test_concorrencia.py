"""Duas pessoas na mesma operação no mesmo segundo.

A auditoria (AUDITORIA_TRANSACIONAL.md) mostrou que uma falha no meio de uma
operação não deixa o banco pela metade — mas que duas requisições simultâneas
podiam gerar dois títulos para a mesma despesa ou medição, e conciliar a mesma
linha do extrato duas vezes. Eram regras "leio, confiro, gravo" sem trava.

Duas defesas, e um teste para cada:
  1. FOR UPDATE nas quatro operações — provado lendo o código (AST), para a
     trava não sumir num refactor sem que alguém note;
  2. restrições únicas parciais no banco (migração 031) — provadas contra
     Postgres real, mais a prova de que a trava de linha bloqueia mesmo.
"""
from __future__ import annotations

import ast
import contextlib
import inspect

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError, OperationalError

from app.apps.erp.db.models.cadastros import PerfilUsuario as P

from conftest import SessaoFalsa, novo_usuario


# ---------------------------------------------------------------------------
# 1. As quatro operações travam a linha antes de "ler, conferir, gravar"
# ---------------------------------------------------------------------------
def _funcao_trava(fn) -> bool:
    """A função chama `s.get(..., with_for_update=True, populate_existing=True)`?

    As DUAS chaves: sem `populate_existing`, um objeto que a rota já carregou
    (ao conferir o escopo) volta da memória da sessão sem SELECT nenhum — e
    sem trava. Foi assim que a primeira versão falhou no Postgres de verdade.
    """
    arvore = ast.parse(inspect.getsource(fn))
    for no in ast.walk(arvore):
        if isinstance(no, ast.Call) and getattr(no.func, "attr", "") == "get":
            chaves = {kw.arg: getattr(kw.value, "value", None) for kw in no.keywords}
            if chaves.get("with_for_update") is True and chaves.get("populate_existing") is True:
                return True
    return False


def test_as_operacoes_de_risco_travam_a_linha():
    from app.apps.erp.core import locacoes, pessoal
    from app.apps.erp.core.titulos import empreita

    for fn in (pessoal.gerar_titulo, empreita.registrar_medicao,
               empreita.autorizar_medicao, locacoes.devolver):
        assert _funcao_trava(fn), (
            f"{fn.__module__}.{fn.__name__} lê, confere e grava sem FOR UPDATE — "
            f"duas requisições no mesmo segundo passam as duas pela checagem")


def test_a_trava_nao_muda_o_resultado_da_regra():
    """O dublê aceita a trava e a regra continua a mesma (aqui: DC sem título
    aprovada gera; DC já faturada recusa)."""
    from app.apps.erp.core import pessoal
    from app.apps.erp.core.comum.auditoria import ErroValidacao
    from app.apps.erp.db.models.financeiro import DespesaColaborador

    d = DespesaColaborador(id=1, numero="DC-1", status="FATURADA", titulo_id=99)
    s = SessaoFalsa(d, novo_usuario(1, P.ADMIN))

    with pytest.raises(ErroValidacao):
        pessoal.gerar_titulo(s, 1, {"meio_pagamento": "BEEVALE"}, novo_usuario(1, P.ADMIN))


# ---------------------------------------------------------------------------
# 2. Conciliação disputada vira recado, não erro 500
# ---------------------------------------------------------------------------
def test_conciliacao_simultanea_responde_409(monkeypatch):
    from flask import Flask
    from app.apps.erp import routes

    app = Flask(__name__)
    app.secret_key = "teste"
    app.register_blueprint(routes.bp)

    def _fake():
        return contextlib.nullcontext(SessaoFalsa(novo_usuario(1, P.FINANCEIRO)))
    monkeypatch.setattr(routes, "get_session", _fake)

    def explode(*a, **kw):
        raise IntegrityError("INSERT INTO conciliacoes", {}, Exception("uq_conciliacao_extrato_vigente"))
    import app.apps.erp.core.pagamentos.conciliacao as conc
    monkeypatch.setattr(conc, "conciliar_automatico", explode)

    with app.test_client() as c:
        with c.session_transaction() as sessao:
            sessao["erp_usuario_id"] = 1
        r = c.post("/erp/api/conciliacao/executar", json={})

    assert r.status_code == 409
    assert "execute de novo" in r.get_json()["erro"].lower()


# ---------------------------------------------------------------------------
# 3. Com banco de verdade: as restrições existem e a trava bloqueia mesmo
# ---------------------------------------------------------------------------
@pytest.mark.banco
def test_migracao_031_criou_as_tres_restricoes(sessao_real):
    linhas = sessao_real.execute(text(
        "SELECT indexname, indexdef FROM pg_indexes WHERE indexname IN "
        "('uq_despesa_colaborador_titulo','uq_medicao_titulo',"
        " 'uq_conciliacao_extrato_vigente')")).all()
    defs = {nome: definicao for nome, definicao in linhas}

    assert set(defs) == {"uq_despesa_colaborador_titulo", "uq_medicao_titulo",
                         "uq_conciliacao_extrato_vigente"}
    for definicao in defs.values():
        assert "UNIQUE" in definicao
    assert "titulo_id IS NOT NULL" in defs["uq_despesa_colaborador_titulo"]
    assert "titulo_id IS NOT NULL" in defs["uq_medicao_titulo"]
    assert "desfeita_em IS NULL" in defs["uq_conciliacao_extrato_vigente"]


@pytest.mark.banco
def test_duas_medicoes_nao_apontam_para_o_mesmo_titulo(sessao_real):
    """A restrição do banco segura mesmo que um caminho novo esqueça a trava."""
    from datetime import date
    from decimal import Decimal
    from app.apps.erp.core.auth.service import gerar_hash
    from app.apps.erp.db.models.cadastros import (
        Categoria, Fornecedor, Obra, RegimeTributario, TipoPessoa, Usuario,
    )
    from app.apps.erp.db.models.financeiro import (
        ContratoMedicao, ContratoServico, FormaPagamento, StatusTitulo, TipoTitulo, Titulo,
    )
    s = sessao_real
    u = Usuario(nome="T", email="conc@teste.bws.local", senha_hash=gerar_hash("senha-teste-123"),
                perfil=P.ADMIN)
    o = Obra(codigo="OBRA-C", nome="Obra C", status="ATIVA")
    f = Fornecedor(tipo_pessoa=TipoPessoa.PJ, cnpj_cpf="00000000000272",
                   razao_social="Empreiteiro", regime_tributario=RegimeTributario.NAO_INFORMADO)
    cat = Categoria(codigo="9.9.98", descricao="Empreita teste")
    s.add_all([u, o, f, cat]); s.flush()
    t = Titulo(numero_sp="SP-CONC-1", tipo=TipoTitulo.T5_EMPREITEIRO, fornecedor_id=f.id,
               descricao="x", valor_bruto=Decimal("10"), valor_liquido=Decimal("10"),
               competencia=date(2026, 9, 1), categoria_id=cat.id,
               forma_pagamento=FormaPagamento.PIX, status=StatusTitulo.APROVADO,
               solicitante_id=u.id)
    c = ContratoServico(numero="EMP-C-1", fornecedor_id=f.id, obra_id=o.id,
                        categoria_id=cat.id, objeto="teste", valor_total=Decimal("100"),
                        status="VIGENTE")
    s.add_all([t, c]); s.flush()
    s.add(ContratoMedicao(contrato_id=c.id, numero=1, valor_medido=Decimal("10"),
                          valor_adiantamento_abatido=Decimal("0"), valor_liquido=Decimal("10"),
                          status="FATURADA", titulo_id=t.id))
    s.flush()

    s.add(ContratoMedicao(contrato_id=c.id, numero=2, valor_medido=Decimal("10"),
                          valor_adiantamento_abatido=Decimal("0"), valor_liquido=Decimal("10"),
                          status="FATURADA", titulo_id=t.id))      # mesmo título
    with pytest.raises(IntegrityError):
        s.flush()


@pytest.mark.banco
def test_a_trava_de_linha_bloqueia_a_segunda_conexao(banco, sessao_real):
    """FOR UPDATE de verdade: quem chega depois espera — aqui, com um limite de
    espera curto, recebe erro em vez de passar por cima."""
    from app.apps.erp.core.auth.service import gerar_hash
    from app.apps.erp.db.models.cadastros import Usuario

    u = Usuario(nome="Trava", email="trava@teste.bws.local",
                senha_hash=gerar_hash("senha-teste-123"), perfil=P.ADMIN)
    sessao_real.add(u); sessao_real.commit()
    # o objeto JÁ está na sessão — é o cenário da rota, que carrega o registro
    # ao conferir o escopo antes de chamar a função que trava
    sessao_real.get(Usuario, u.id, with_for_update=True, populate_existing=True)

    with banco.connect() as outra:
        outra.execute(text("SET lock_timeout = '300ms'"))
        with pytest.raises(OperationalError):
            outra.execute(text("SELECT id FROM usuarios WHERE id = :i FOR UPDATE"), {"i": u.id})
