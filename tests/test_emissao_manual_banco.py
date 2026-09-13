"""A emissão da nota no modo MANUAL — com banco de verdade.

Passo 5 do `MEDICOES_E_NOTAS.md`, e ele vem antes do automático de propósito:
o caminho manual funciona no dia seguinte, sem credenciamento, sem certificado
e sem token — e continua servindo de rede quando a API falhar.

O que se prova:

  1. Por onde a nota sai NÃO se escolhe: desce a cadeia medição → obra →
     empresa. Correção do dono em 09/09/2026.
  2. As retenções vêm CALCULADAS do cadastro da obra, e cada tributo cai na
     sua coluna — o PCC (4,65% em conjunto) se desfaz em PIS, COFINS e CSLL.
  3. O bloco para copiar traz tudo que o portal pergunta, em português e com
     dinheiro em formato brasileiro.
  4. A discriminação traz medição, período e contrato — sem isso o órgão
     devolve a nota.
  5. Registrar sem informar retenção NÃO grava zero: usa o cálculo. Zero é
     uma afirmação, não uma ausência.
  6. Título que não é medição, obra sem empresa e título rateado entre obras
     são recusados, com o motivo em português.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from app.apps.erp.core.auth.service import gerar_hash
from app.apps.erp.core.comum.auditoria import ErroValidacao
from app.apps.erp.core.notas_emitidas import listagem as svc_lista
from app.apps.erp.core.notas_emitidas import manual as svc
from app.apps.erp.db.models.cadastros import (Categoria, ContaBancaria, Empresa,
                                              Fornecedor, Obra,
                                              PerfilUsuario as P,
                                              RegimeTributario, TipoPessoa,
                                              Usuario)
from app.apps.erp.db.models.financeiro import (EspecieTitulo, FormaPagamento,
                                               Rateio, StatusTitulo,
                                               TipoTitulo, Titulo)

pytestmark = pytest.mark.banco


@pytest.fixture
def cenario(sessao_real):
    s = sessao_real
    empresa = Empresa(razao_social="BWS Construções LTDA", nome_fantasia="BWS",
                      cnpj="11222333000181", inscricao_municipal="98765",
                      emissao_ambiente="PRODUCAO", emissao_serie="1",
                      emissao_modo="MANUAL", emissao_municipio="Eusébio")
    s.add(empresa)
    s.flush()
    obra = Obra(codigo="ESCPLANALTO", nome="Escola do Planalto",
                municipio="Eusébio", uf="CE", cno="12.345.67890/12",
                cliente="Prefeitura Municipal do Eusébio",
                cnpj_cliente="07396446000173", contrato="CT 014/2026",
                objeto="Construção da Escola do Planalto",
                empresa_id=empresa.id,
                aliquota_iss_pct=Decimal("3.0000"), iss_retido=True,
                aceita_deducao_material=True, pct_servico_iss=Decimal("50"),
                pct_servico_inss=Decimal("50"), inss_retido=True,
                federais_retidos=["IR", "PIS", "COFINS", "CSLL"])
    cliente = Fornecedor(razao_social="Prefeitura Municipal do Eusébio",
                         cnpj_cpf="07396446000173", tipo_pessoa=TipoPessoa.PJ,
                         ativo=True, regime_tributario=RegimeTributario.NAO_INFORMADO)
    cat = Categoria(codigo="1.1.01", descricao="Receita de medição")
    conta = ContaBancaria(descricao="Bradesco 1234-5", banco_codigo="237",
                          agencia="1234", conta="56789-0", ativo=True)
    u = Usuario(nome="Financeiro", email="emissao@teste.local", ativo=True,
                senha_hash=gerar_hash("senha-de-teste-123"), perfil=P.FINANCEIRO)
    s.add_all([obra, cliente, cat, conta, u])
    s.flush()
    return {"s": s, "empresa": empresa, "obra": obra, "cliente": cliente,
            "categoria": cat, "conta": conta, "usuario": u}


def _medicao(cenario, numero="1", valor="100000.00", obras=None):
    s = cenario["s"]
    t = Titulo(numero_sp=f"REC-{numero}", tipo=TipoTitulo.T1_MATERIAL_NFE,
               especie=EspecieTitulo.RECEBER, fornecedor_id=cenario["cliente"].id,
               descricao=f"Medição {numero}", valor_bruto=Decimal(valor),
               valor_liquido=Decimal(valor), competencia=date(2026, 8, 1),
               categoria_id=cenario["categoria"].id,
               forma_pagamento=FormaPagamento.BOLETO, status=StatusTitulo.APROVADO,
               solicitante_id=cenario["usuario"].id, numero_medicao=numero,
               periodo_inicio=date(2026, 8, 1), periodo_fim=date(2026, 8, 31))
    s.add(t)
    s.flush()
    for obra in (obras or [cenario["obra"]]):
        s.add(Rateio(titulo_id=t.id, obra_id=obra.id,
                     valor=Decimal(valor) / len(obras or [1]),
                     percentual=Decimal(100) / len(obras or [1])))
    s.flush()
    return t


# ---------------------------------------------------------------------------
# 1. Por onde sai não se escolhe
# ---------------------------------------------------------------------------
def test_a_empresa_vem_da_obra_e_ninguem_escolhe(cenario):
    """Correção do dono: "por onde vamos emitir não é algo que a gente
    seleciona. Quem define é o centro de custo"."""
    d = svc.preparar(cenario["s"], _medicao(cenario).id)
    assert d["destino"]["empresa_id"] == cenario["empresa"].id
    assert d["destino"]["cnpj"] == "11222333000181"
    assert d["destino"]["modo"] == "MANUAL"
    assert d["obra"] == "ESCPLANALTO"


def test_titulo_rateado_entre_obras_e_recusado(cenario):
    """Duas obras podem ser de empresas diferentes: seriam duas notas, de
    CNPJs diferentes."""
    s = cenario["s"]
    outra = Obra(codigo="CREPETERRA", nome="Creche Terra Nova",
                 empresa_id=cenario["empresa"].id)
    s.add(outra)
    s.flush()
    d = svc.preparar(s, _medicao(cenario, obras=[cenario["obra"], outra]).id)
    assert d["destino"]["pode"] is False
    assert "rateado" in d["destino"]["motivo"].lower()


def test_obra_sem_empresa_e_recusada_com_o_conserto(cenario):
    s = cenario["s"]
    cenario["obra"].empresa_id = None
    s.flush()
    d = svc.preparar(s, _medicao(cenario).id)
    assert d["destino"]["pode"] is False
    assert "cadastro da obra" in d["destino"]["motivo"]


def test_titulo_que_nao_e_medicao_e_recusado(cenario):
    s = cenario["s"]
    t = _medicao(cenario)
    t.numero_medicao = None
    s.flush()
    with pytest.raises(ErroValidacao) as e:
        svc.preparar(s, t.id)
    assert "não é uma medição" in str(e.value)


# ---------------------------------------------------------------------------
# 2. As retenções saem CALCULADAS do cadastro da obra
# ---------------------------------------------------------------------------
def test_as_retencoes_vem_calculadas_da_obra(cenario):
    """ISS 3% sobre metade (dedução de material), INSS 11% sobre metade,
    federais sobre o cheio."""
    d = svc.preparar(cenario["s"], _medicao(cenario, valor="100000.00").id)
    r = d["retencoes"]
    assert r["iss"] == 1500.00, "3% sobre 50.000 (base com dedução)"
    assert r["inss"] == 5500.00, "11% sobre 50.000 (parcela de serviço)"
    assert r["ir"] == 1200.00, "1,2% sobre o valor cheio"
    assert d["valor_liquido"] == 100000.00 - d["retido"]


def test_o_pcc_se_desfaz_em_tres_colunas(cenario):
    """A guia traz 4,65% numa linha só; a contabilidade precisa dos três
    separados de novo."""
    d = svc.preparar(cenario["s"], _medicao(cenario, valor="100000.00").id)
    r = d["retencoes"]
    assert r["pis"] == 650.00
    assert r["cofins"] == 3000.00
    assert r["csll"] == 1000.00
    assert round(r["pis"] + r["cofins"] + r["csll"], 2) == 4650.00


def test_obra_sem_aliquota_de_iss_avisa_em_vez_de_calcular_errado(cenario):
    s = cenario["s"]
    cenario["obra"].aliquota_iss_pct = None
    s.flush()
    d = svc.preparar(s, _medicao(cenario).id)
    assert d["retencoes"]["iss"] == 0
    assert any("alíquota de ISS" in a for a in d["avisos"])


# ---------------------------------------------------------------------------
# 3 e 4. O bloco para copiar
# ---------------------------------------------------------------------------
def test_o_bloco_traz_tudo_que_o_portal_pergunta(cenario):
    texto = svc.preparar(cenario["s"], _medicao(cenario).id)["texto_para_copiar"]
    for pedaco in ("BWS Construções LTDA", "11222333000181", "98765", "Eusébio",
                   "Prefeitura Municipal do Eusébio", "07396446000173",
                   "Medição nº 1", "CT 014/2026", "CNO 12.345.67890/12"):
        assert pedaco in texto, f"faltou {pedaco} no bloco"


def test_o_dinheiro_do_bloco_sai_em_portugues(cenario):
    """O bloco é copiado para dentro do portal. Formato americano ali vira
    valor errado na nota."""
    texto = svc.preparar(cenario["s"], _medicao(cenario, valor="1234567.89").id)["texto_para_copiar"]
    assert "1.234.567,89" in texto
    assert "1234567.89" not in texto


def test_a_discriminacao_tem_medicao_periodo_e_contrato(cenario):
    """Sem isso o setor de empenho não sabe a que competência a nota se
    refere, e devolve."""
    d = svc.preparar(cenario["s"], _medicao(cenario, "3").id)
    assert "Medição nº 3" in d["discriminacao"]
    assert "01/08/2026 a 31/08/2026" in d["discriminacao"]
    assert "CT 014/2026" in d["discriminacao"]


# ---------------------------------------------------------------------------
# 5. Registrar de volta
# ---------------------------------------------------------------------------
def test_registrar_sem_informar_retencao_usa_o_calculo(cenario):
    """Gravar zero seria pior que não ter a coluna: o relatório sairia dizendo
    que nada foi retido, o que é uma afirmação, não uma ausência."""
    s = cenario["s"]
    t = _medicao(cenario, valor="100000.00")
    nota = svc.registrar(s, t.id, numero_nota="1201", emissao=date(2026, 9, 1),
                         usuario=cenario["usuario"])
    linha = svc_lista.ler(s, nota)
    assert linha["retencoes"]["iss"] == 1500.00
    assert linha["retencoes"]["inss"] == 5500.00
    assert linha["numero_nota"] == "1201"
    assert linha["situacao"] == "EMITIDA"
    assert linha["obra"] == "ESCPLANALTO"


def test_a_retencao_informada_a_mao_manda(cenario):
    """A nota do portal é a verdade: se o valor veio diferente do cálculo,
    vale o que está no documento."""
    s = cenario["s"]
    t = _medicao(cenario, valor="100000.00")
    nota = svc.registrar(s, t.id, numero_nota="1201",
                         retencoes={"iss": "1800.00"}, usuario=cenario["usuario"])
    assert svc_lista.ler(s, nota)["retencoes"]["iss"] == 1800.00


def test_a_nota_registrada_aparece_no_preparar_seguinte(cenario):
    """Uma medição pode virar duas notas — mas a pessoa tem de VER a primeira
    antes de emitir a segunda."""
    s = cenario["s"]
    t = _medicao(cenario)
    svc.registrar(s, t.id, numero_nota="1201", usuario=cenario["usuario"])
    d = svc.preparar(s, t.id)
    assert len(d["ja_emitidas"]) == 1
    assert d["ja_emitidas"][0]["numero_nota"] == "1201"


def test_o_codigo_de_verificacao_e_guardado(cenario):
    s = cenario["s"]
    t = _medicao(cenario)
    nota = svc.registrar(s, t.id, numero_nota="1201",
                         codigo_verificacao="A1B2-C3D4",
                         usuario=cenario["usuario"])
    assert svc_lista.ler(s, nota)["codigo_verificacao"] == "A1B2-C3D4"


def test_a_aliquota_nao_pode_sair_como_mil_e_duzentos_por_cento(cenario):
    """1,2% de IRRF estava saindo escrito "1.200%".

    Em português "1.200%" se lê como MIL E DUZENTOS POR CENTO — e essa frase
    ia dentro do bloco que a pessoa copia para o portal. O número certo é
    1,2%.
    """
    d = svc.preparar(cenario["s"], _medicao(cenario).id)
    explicacoes = " ".join(r["explicacao"] for r in d["detalhe_retencoes"])
    assert "1,2%" in explicacoes
    assert "1.200%" not in explicacoes
    assert "3,0000%" not in explicacoes and "3.0000%" not in explicacoes


def test_o_dinheiro_da_explicacao_tambem_sai_em_portugues(cenario):
    d = svc.preparar(cenario["s"], _medicao(cenario, valor="100000.00").id)
    explicacoes = " ".join(r["explicacao"] for r in d["detalhe_retencoes"])
    assert "50.000,00" in explicacoes
    assert "50000.00" not in explicacoes
