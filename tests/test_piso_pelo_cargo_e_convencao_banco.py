# -*- coding: utf-8 -*-
"""
CARGO + CONVENÇÃO = CATEGORIA, PISO E CBO.

⚑ POR QUE ESTE ARQUIVO EXISTE (18/11/2026). Ele:

> *"No Pipefy eu tenho uma base de dados onde vinculo cargo, convenção,
> categoria e salário. Qual sua sugestão pra fazermos aqui também? Preciso de
> algo simples de construir. A ideia é que uma vez que o DP preencher a
> convenção, o salário e a categoria já ficariam preenchidos, e o CBO."*

⚠️ **A BASE JÁ EXISTIA** — é `cargo_convencao`, importada da planilha "Cargo e
Convenção" do Pipefy em 05/11/2026 (63 cargos, 13 convenções, 135 cruzamentos),
com categoria, piso, CBO e **histórico de dissídio**. O que faltava era a TELA
olhar a base.

Estes testes defendem a base e as duas regras que o preenchimento tem de
respeitar — e a segunda só apareceu dirigindo a tela:

  1. o cruzamento responde categoria, piso e CBO para o par;
  2. **salário e categoria se comportam DIFERENTE**: salário é negociado (não
     se sobrescreve o que alguém digitou), categoria não é (ela acompanha o
     par, senão guarda dado errado).
"""
from __future__ import annotations

from decimal import Decimal

import pytest
from sqlalchemy import text

from app.apps.erp.core.cadastros import cargos as svc

pytestmark = pytest.mark.banco

# As cinco linhas que ele mandou, como estão na planilha dele.
DELE = [
    ("PEDREIRO", "71521 0", "CE-Fortaleza", "PROFISSIONAL", "2230.00"),
    ("PEDREIRO", "71521 0", "BA-Sobradinho", "OPERARIO QUALIFICADO", "1910.65"),
    ("CARPINTEIRO", "71550 5", "BA-Sobradinho", "OPERARIO QUALIFICADO", "1910.65"),
    ("ENCARREGADO DE SETOR", "41010 5", "Sem Convenção", "SEM CATEGORIA", "2788.30"),
    ("MESTRE DE OBRAS", "71020 5", "Sem Convenção", "SEM CATEGORIA", "2478.47"),
]


@pytest.fixture
def base(sessao_real):
    s = sessao_real
    for cargo, cbo, conv, cat, piso in DELE:
        cid = s.execute(text("SELECT id FROM cargos WHERE nome = :n"),
                        {"n": cargo}).scalar()
        if not cid:
            cid = s.execute(text("INSERT INTO cargos (nome, cbo) VALUES (:n, :c) "
                                 "RETURNING id"),
                            {"n": cargo, "c": cbo.replace(" ", "")}).scalar()
        vid = s.execute(text("SELECT id FROM convencoes WHERE nome = :n"),
                        {"n": conv}).scalar()
        if not vid:
            vid = s.execute(text("INSERT INTO convencoes (nome) VALUES (:n) "
                                 "RETURNING id"), {"n": conv}).scalar()
        s.execute(text("""
            INSERT INTO cargo_convencao (cargo_id, convencao_id, categoria, salario_piso)
            VALUES (:c, :v, :cat, :p)
            ON CONFLICT (cargo_id, convencao_id)
            DO UPDATE SET categoria = :cat, salario_piso = :p"""),
            {"c": cid, "v": vid, "cat": cat, "p": Decimal(piso)})
    s.flush()
    return True


def _achar(cruzamentos, cargo, convencao):
    return next((c for c in cruzamentos
                 if c["cargo"] == cargo and c["convencao"] == convencao), None)


def test_o_mesmo_cargo_em_duas_convencoes_tem_piso_e_categoria_DIFERENTES(
        sessao_real, base):
    """⚠️ É o caso que faz a base existir. PEDREIRO ganha 2.230,00 e é
    PROFISSIONAL em Fortaleza; o MESMO cargo ganha 1.910,65 e é OPERÁRIO
    QUALIFICADO em Sobradinho. Uma tabela só de cargos não saberia disso."""
    r = svc.listar(sessao_real)["cruzamentos"]

    ce = _achar(r, "PEDREIRO", "CE-Fortaleza")
    ba = _achar(r, "PEDREIRO", "BA-Sobradinho")
    assert ce["salario_piso"] == 2230.00 and ce["categoria"] == "PROFISSIONAL"
    assert ba["salario_piso"] == 1910.65 and ba["categoria"] == "OPERARIO QUALIFICADO"
    # e o CBO é do CARGO, igual nos dois — não da convenção
    assert ce["cbo"] == ba["cbo"] == "7152-10"


def test_o_CBO_sai_com_a_mascara_que_ele_usa(sessao_real, base):
    """Ele escreve `7152-10`. A planilha do Pipefy entregava cinco formatos,
    inclusive `715505.0` (o Sheets convertendo em número)."""
    r = svc.listar(sessao_real)["cruzamentos"]
    assert _achar(r, "CARPINTEIRO", "BA-Sobradinho")["cbo"] == "7155-05"
    assert _achar(r, "MESTRE DE OBRAS", "Sem Convenção")["cbo"] == "7102-05"


def test_SEM_CONVENCAO_tambem_e_uma_combinacao_da_base(sessao_real, base):
    """"Sem Convenção" não é ausência de dado: é uma convenção chamada assim,
    com piso e com categoria "SEM CATEGORIA". Tratá-la como vazio deixaria
    encarregado e mestre de obras sem piso nenhum."""
    r = svc.listar(sessao_real)["cruzamentos"]
    enc = _achar(r, "ENCARREGADO DE SETOR", "Sem Convenção")
    assert enc and enc["salario_piso"] == 2788.30
    assert enc["categoria"] == "SEM CATEGORIA"


def test_a_lista_para_a_tela_de_cadastro_NAO_leva_o_piso(sessao_real, base):
    """⚠️ A tela de cadastro monta a caixinha de cargos com a rota de OPÇÕES,
    que é aberta a quem tem `ver_pessoal` — supervisor de obra e parceiro.
    Mandar a tabela salarial da empresa por ali seria vazá-la. Quem preenche o
    piso é `/erp/api/cargos`, que pede `editar_colaboradores`."""
    aberta = svc.listar(sessao_real, com_salario=False)["cruzamentos"]
    assert aberta and all("salario_piso" not in c for c in aberta)
    assert all("categoria" in c for c in aberta)      # a categoria pode ir

    fechada = svc.listar(sessao_real)["cruzamentos"]
    assert all("salario_piso" in c for c in fechada)


def test_a_tela_pede_a_base_UMA_VEZ_e_nao_por_clique():
    """A base tem 135 linhas e chega inteira quando o formulário abre. Pedir
    por combinação custaria uma ida ao banco por clique numa caixinha que a
    pessoa mexe várias vezes."""
    from pathlib import Path
    tela = (Path(__file__).resolve().parents[1] / "app" / "apps" / "erp"
            / "templates" / "erp_colaboradores.html").read_text(encoding="utf-8")
    assert "if (CRUZAMENTOS) return CRUZAMENTOS;" in tela, (
        "o cache da base saiu — a tela voltaria a pedir a cada clique")


def test_o_SALARIO_nao_e_sobrescrito_e_a_CATEGORIA_acompanha_o_par():
    """⚠️ A regra que só apareceu dirigindo a tela.

    Salário é NEGOCIADO: ganhar mais que o piso é normal, e trocar por cima o
    valor digitado é dinheiro mudando sem ninguém ver. Categoria NÃO é
    negociada: é o que a convenção diz sobre o cargo, e deixar a da convenção
    anterior guarda dado errado.
    """
    from pathlib import Path
    tela = (Path(__file__).resolve().parents[1] / "app" / "apps" / "erp"
            / "templates" / "erp_colaboradores.html").read_text(encoding="utf-8")

    # o salário só é preenchido quando está vazio
    assert 'if (campo && !(campo.value || "").trim() && cc.salario_piso != null)' in tela
    # e há o caminho explícito para adotar o piso de propósito
    assert 'id="c-usar-piso"' in tela
    # a categoria é atribuída sem a guarda de "só se vazio"
    assert "cat.value = agora;" in tela
