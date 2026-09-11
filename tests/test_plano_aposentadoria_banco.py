"""Conta que sai do plano não leva o histórico junto — com banco de verdade.

O documento de alterações do plano (10/09/2026) abre com a exigência:

    "Antes de mexer: verifique quais contas já têm lançamento e avise se
    alguma alteração exigir migração de dados. Contas com movimento não podem
    simplesmente sumir — precisam ser migradas ou aposentadas com o histórico
    preservado."

É essa regra que este arquivo prova. Instalar o plano padrão desativa apenas a
conta que NUNCA foi usada; a que tem lançamento continua funcionando e sai no
relatório para o dono remanejar pela tela de substituir conta, que leva os
títulos junto.

COM BANCO DE VERDADE porque tudo aqui é `WHERE`: achar a conta pelo código,
achar a sucessora, contar título e rateio. A sessão dublada devolve todos os
objetos do tipo pedido e responderia certo por acidente.

De quebra, prova o que a conta REDUTORA faz no relatório: R$ 500 de devolução
não somam R$ 500 de custo — abatem.
"""
from __future__ import annotations

import pytest
from sqlalchemy import select, text

from app.apps.erp.core import relatorios
from app.apps.erp.core.cadastros.plano_padrao import aplicar_plano
from app.apps.erp.db.models.cadastros import Categoria

pytestmark = pytest.mark.banco


def _conta(s, codigo: str) -> Categoria:
    return s.scalars(select(Categoria).where(Categoria.codigo == codigo)).one()


def _plano_antigo(s) -> None:
    """Recria as contas que existiam antes das alterações — é o estado do banco
    de quem já instalou o plano uma vez."""
    for codigo, descricao in (
            ("1.2.01", "Devolução de material a fornecedor"),
            ("1.2.02", "Estorno de despesas"),
            ("2.1.06", "CSRF/PCC retido (PIS-COFINS-CSLL sobre serviços)"),
            ("9.4.03", "Pagamento de principal de parcelamento tributário"),
            ("3.3.05", "Locação de andaimes, escoramentos e formas")):
        s.add(Categoria(codigo=codigo, descricao=descricao, natureza="RESULTADO",
                        tipos_permitidos=[], ativo=True))
    s.flush()


@pytest.fixture
def cenario(sessao_real):
    _plano_antigo(sessao_real)
    aplicar_plano(sessao_real)
    return sessao_real


def test_conta_sem_lancamento_e_desativada_e_aponta_para_a_sucessora(cenario):
    velha = _conta(cenario, "1.2.01")
    nova = _conta(cenario, "3.5.01")
    assert velha.ativo is False
    assert velha.substituida_por_id == nova.id, \
        "sem o ponteiro, quem abrir um lançamento antigo não descobre para onde a conta foi"


def test_a_conta_nova_nasce_no_grupo_de_custo_e_redutora(cenario):
    nova = _conta(cenario, "3.5.01")
    assert nova.grupo_codigo == "3"
    assert nova.subgrupo_codigo == "3.5"
    assert nova.redutora is True
    assert nova.ativo is True


def test_conta_sem_destino_unico_nunca_e_aposentada_sozinha(cenario):
    """A retenção conjunta se reparte entre PIS, COFINS e CSLL — só uma pessoa
    pode dizer como."""
    assert _conta(cenario, "2.1.06").ativo is True


def test_a_locacao_de_andaimes_foi_absorvida_pela_conta_da_planilha(cenario):
    velha = _conta(cenario, "3.3.05")
    assert velha.ativo is False
    assert velha.substituida_por_id == _conta(cenario, "3.3.06").id


def test_reinstalar_o_plano_nao_reativa_o_que_foi_aposentado(cenario):
    """O botão de instalar é apertado mais de uma vez — o dono não tem como
    saber se já apertou."""
    relatorio = aplicar_plano(cenario)
    assert [a["codigo"] for a in relatorio["aposentadas"]] == []
    assert _conta(cenario, "1.2.01").ativo is False


# ---------------------------------------------------------------------------
# Conta com movimento
# ---------------------------------------------------------------------------
@pytest.fixture
def com_lancamento(sessao_real, cenario_de_titulo):
    """Um título lançado na conta velha, ANTES de o plano ser aplicado."""
    _plano_antigo(sessao_real)
    velha = _conta(sessao_real, "1.2.01")
    cenario_de_titulo(velha.id)
    return sessao_real


@pytest.fixture
def cenario_de_titulo(sessao_real):
    """Cria o mínimo que o banco exige para existir um título numa conta."""
    def _criar(categoria_id: int) -> None:
        sessao_real.execute(text("""
            INSERT INTO fornecedores (tipo_pessoa, cnpj_cpf, razao_social)
                 VALUES ('PJ', '11444777000161', 'FORNECEDOR TESTE')
            ON CONFLICT DO NOTHING"""))
        sessao_real.execute(text("""
            INSERT INTO usuarios (nome, email, senha_hash, perfil)
                 VALUES ('Teste', 't@bws.test', 'x', 'ADMIN')
            ON CONFLICT DO NOTHING"""))
        sessao_real.execute(text("""
            INSERT INTO titulos (numero_sp, fornecedor_id, categoria_id, descricao,
                                 valor_bruto, valor_liquido, competencia, status,
                                 solicitante_id, tipo, forma_pagamento)
            SELECT 'SP-APOS-1', f.id, :c, 'lançamento antigo', 100, 100,
                   date '2026-01-01', 'APROVADO', u.id, 'T1_MATERIAL_NFE', 'PIX'
              FROM fornecedores f, usuarios u LIMIT 1"""), {"c": categoria_id})
        sessao_real.flush()
    return _criar


def test_conta_com_lancamento_continua_ativa(com_lancamento):
    """Desativar uma conta em uso esconderia lançamento do relatório sem
    ninguém ter pedido."""
    aplicar_plano(com_lancamento)
    velha = _conta(com_lancamento, "1.2.01")
    assert velha.ativo is True
    assert velha.substituida_por_id is None


def test_a_conta_com_lancamento_e_relatada_com_a_contagem_e_o_motivo(com_lancamento):
    relatorio = aplicar_plano(com_lancamento)
    pendente = [p for p in relatorio["pendentes_de_migracao"]
                if p["codigo"] == "1.2.01"]
    assert pendente, "a conta em uso tem de aparecer para o dono decidir"
    assert pendente[0]["lancamentos"] == 1
    assert pendente[0]["destino"] == "3.5.01"
    assert pendente[0]["motivo"], "o relatório precisa dizer POR QUE a conta saiu"


def test_o_rateio_tambem_conta_como_lancamento(sessao_real, cenario_de_titulo):
    """O título aponta para uma conta, mas o rateio pode apontar para outra —
    e é o rateio que manda no relatório. Contar só o título deixaria aposentar
    uma conta com movimento."""
    _plano_antigo(sessao_real)
    outra = _conta(sessao_real, "1.2.02")
    velha = _conta(sessao_real, "1.2.01")
    cenario_de_titulo(outra.id)
    sessao_real.execute(text("""
        INSERT INTO obras (codigo, nome) VALUES ('OBR-APOS', 'Obra de teste')
        ON CONFLICT DO NOTHING"""))
    sessao_real.execute(text("""
        INSERT INTO rateios (titulo_id, obra_id, categoria_id, valor, percentual)
        SELECT t.id, o.id, :c, 100, 100
          FROM titulos t, obras o
         WHERE t.numero_sp = 'SP-APOS-1' AND o.codigo = 'OBR-APOS'"""),
        {"c": velha.id})
    sessao_real.flush()

    aplicar_plano(sessao_real)

    assert _conta(sessao_real, "1.2.01").ativo is True, \
        "a conta só aparece no rateio, mas está em uso do mesmo jeito"


# ---------------------------------------------------------------------------
# O efeito da conta redutora no relatório
# ---------------------------------------------------------------------------
def test_devolucao_abate_o_custo_em_vez_de_somar(sessao_real, cenario_de_titulo):
    """O exemplo do próprio documento: R$ 10.000 de compra e R$ 500 de
    devolução mostram R$ 9.500 de custo — e as duas linhas continuam no
    histórico, sem estornar o lançamento original."""
    aplicar_plano(sessao_real)
    compra = _conta(sessao_real, "3.1.01")          # cimento
    devolucao = _conta(sessao_real, "3.5.01")       # devolução de material
    cenario_de_titulo(compra.id)
    sessao_real.execute(text("""
        INSERT INTO obras (codigo, nome) VALUES ('OBR-RED', 'Obra de teste')
        ON CONFLICT DO NOTHING"""))
    for categoria_id, valor in ((compra.id, 10000), (devolucao.id, 500)):
        sessao_real.execute(text("""
            INSERT INTO rateios (titulo_id, obra_id, categoria_id, valor, percentual)
            SELECT t.id, o.id, :c, :v, 100
              FROM titulos t, obras o
             WHERE t.numero_sp = 'SP-APOS-1' AND o.codigo = 'OBR-RED'"""),
            {"c": categoria_id, "v": valor})
    sessao_real.flush()

    dre = relatorios.dre_gerencial(sessao_real, {})
    grupo3 = [g for g in dre["resultado"] if g["codigo"] == "3"]
    assert grupo3, "o grupo de custos precisa aparecer no resultado"
    assert grupo3[0]["total"] == pytest.approx(9500.0)
    reducoes = [x for x in grupo3[0]["subgrupos"] if x["codigo"] == "3.5"]
    assert reducoes and reducoes[0]["total"] == pytest.approx(-500.0), \
        "a devolução tem de aparecer NEGATIVA, e continuar visível"


def test_as_duas_linhas_continuam_no_analitico(sessao_real, cenario_de_titulo):
    """Abater o custo não é o mesmo que apagar: o detalhamento que o contador
    pede tem de mostrar a compra e a devolução."""
    aplicar_plano(sessao_real)
    compra = _conta(sessao_real, "3.1.01")
    devolucao = _conta(sessao_real, "3.5.01")
    cenario_de_titulo(compra.id)
    sessao_real.execute(text("""
        INSERT INTO obras (codigo, nome) VALUES ('OBR-RED2', 'Obra de teste')
        ON CONFLICT DO NOTHING"""))
    for categoria_id, valor in ((compra.id, 10000), (devolucao.id, 500)):
        sessao_real.execute(text("""
            INSERT INTO rateios (titulo_id, obra_id, categoria_id, valor, percentual)
            SELECT t.id, o.id, :c, :v, 100
              FROM titulos t, obras o
             WHERE t.numero_sp = 'SP-APOS-1' AND o.codigo = 'OBR-RED2'"""),
            {"c": categoria_id, "v": valor})
    sessao_real.flush()

    linhas = relatorios.analitico(sessao_real, {})
    valores = sorted(l["valor"] for l in linhas)
    assert valores == [-500.0, 10000.0]
