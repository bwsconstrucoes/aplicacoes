"""O grupo de OBRAS das perguntas — conferência de cadastro, prazo e garantia.

POR QUE ESTE GRUPO NASCEU SEM "QUANTO CUSTOU A OBRA".
É a pergunta mais óbvia do assunto, e é justamente a que não está aqui. "Custo
da obra" ainda não tem uma definição combinada com o dono — o que foi lançado?
o que foi pago? entra o que está em análise? entra rateio de administração? —
e cada leitura dá um número diferente, todos com cara de certo. Este arquivo
tem um teste que CONGELA essa ausência: se alguém acrescentar a pergunta sem
a palavra estar decidida, ele acusa.

COM BANCO DE VERDADE porque a lista de obras que cada pessoa alcança vive no
`WHERE`, e o dublê da suíte ignora `WHERE`. Foi exatamente esse buraco que
escondeu a brecha das Locações até 11/09/2026.
"""
from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

import pytest

from app.apps.erp.core.auth.service import gerar_hash
from app.apps.erp.core.perguntas import catalogo
from app.apps.erp.db.models.cadastros import (
    Empresa, EscopoVisao, Obra, PerfilUsuario as P, Usuario, UsuarioObra,
)

pytestmark = pytest.mark.banco

HOJE = date.today()


@pytest.fixture
def base(sessao_real):
    """Quatro obras: uma completa, uma sem nada, uma com garantia vencida e
    uma com a vigência do contrato estourada."""
    s = sessao_real
    empresa = Empresa(razao_social="BWS Construções LTDA",
                      cnpj="12345678000199")
    s.add(empresa)
    s.flush()

    completa = Obra(
        codigo="OBRA-OK", nome="Creche do Eusébio", cliente="Prefeitura",
        cno="123456789", codigo_ibge="2304400",
        aliquota_iss_pct=Decimal("3.00"), empresa_id=empresa.id,
        fase="EM_EXECUCAO",
        seguro_garantia="AP-001", seguro_vigencia_fim=HOJE + timedelta(days=400),
        vigencia_fim=HOJE + timedelta(days=300))
    nua = Obra(codigo="OBRA-NUA", nome="Escola de Maracanaú",
               cliente="Prefeitura", fase="EM_EXECUCAO")
    garantia = Obra(
        codigo="OBRA-SEG", nome="Posto de saúde", cliente="Estado",
        cno="987654321", codigo_ibge="2304400",
        aliquota_iss_pct=Decimal("2.00"), empresa_id=empresa.id,
        fase="EM_EXECUCAO",
        seguro_garantia="AP-009", seguro_vigencia_fim=HOJE - timedelta(days=10),
        vigencia_fim=HOJE + timedelta(days=100))
    prazo = Obra(
        codigo="OBRA-PZ", nome="Quadra poliesportiva", cliente="Prefeitura",
        cno="555444333", codigo_ibge="2304400",
        aliquota_iss_pct=Decimal("5.00"), empresa_id=empresa.id,
        fase="EM_EXECUCAO", contrato="CT-2024/07",
        vigencia_fim=HOJE - timedelta(days=45))
    encerrada = Obra(
        codigo="OBRA-FIM", nome="Reforma antiga", cliente="Prefeitura",
        cno="111222333", codigo_ibge="2304400",
        aliquota_iss_pct=Decimal("5.00"), empresa_id=empresa.id,
        fase="CONCLUIDA", vigencia_fim=HOJE - timedelta(days=900))
    s.add_all([completa, nua, garantia, prazo, encerrada])
    s.flush()

    chefe = Usuario(nome="Marcelo", email="chefe@bws.test",
                    senha_hash=gerar_hash("senha-de-teste"), perfil=P.ADMIN)
    da_obra = Usuario(nome="Administrativo da OBRA-OK",
                      email="adm@bws.test",
                      senha_hash=gerar_hash("senha-de-teste"),
                      perfil=P.ADMINISTRATIVO_OBRA,
                      escopo_visao=EscopoVisao.OBRAS_DESIGNADAS)
    lancador = Usuario(nome="Lançador sem obra", email="lanc@bws.test",
                       senha_hash=gerar_hash("senha-de-teste"),
                       perfil=P.LANCADOR,
                       escopo_visao=EscopoVisao.PROPRIOS)
    s.add_all([chefe, da_obra, lancador])
    s.flush()
    s.add(UsuarioObra(usuario_id=da_obra.id, obra_id=completa.id))
    s.flush()
    return {"sessao": s, "chefe": chefe, "da_obra": da_obra,
            "lancador": lancador, "empresa": empresa}


def _responder(base, quem, chave, **parametros):
    return catalogo.responder(chave, base["sessao"], base[quem], parametros)


def _obras(resposta):
    return {l["obra"] for l in resposta["linhas"]}


# ---------------------------------------------------------------------------
# Cadastro incompleto — o que trava a emissão da nota
# ---------------------------------------------------------------------------
def test_a_obra_sem_nada_aparece_com_os_quatro_campos_faltando(base):
    r = _responder(base, "chefe", "cadastro_incompleto")
    nua = [l for l in r["linhas"] if l["obra"] == "OBRA-NUA"]
    assert len(nua) == 1
    assert nua[0]["quantos"] == 4
    for rotulo in ("CNO", "código IBGE", "alíquota de ISS", "empresa"):
        assert rotulo in nua[0]["falta"]


def test_a_obra_completa_nao_aparece(base):
    r = _responder(base, "chefe", "cadastro_incompleto")
    assert "OBRA-OK" not in _obras(r)


def test_o_filtro_por_obra_nao_liga_para_acento_nem_maiuscula(base):
    r = _responder(base, "chefe", "cadastro_incompleto", obra="maracanau")
    assert _obras(r) == {"OBRA-NUA"}


def test_a_frase_diz_quantas_obras_travam(base):
    r = _responder(base, "chefe", "cadastro_incompleto")
    assert "não emitem nota hoje" in r["frase"]
    assert str(r["quantas"]) in r["frase"]


def test_a_resposta_diz_de_onde_veio(base):
    r = _responder(base, "chefe", "cadastro_incompleto")
    assert r["de_onde_veio"]["tela"] == "/erp/obras"


# ---------------------------------------------------------------------------
# Seguro garantia
# ---------------------------------------------------------------------------
def test_garantia_vencida_aparece_com_dias_negativos(base):
    r = _responder(base, "chefe", "garantia_vencendo")
    seg = [l for l in r["linhas"] if l["obra"] == "OBRA-SEG"]
    assert len(seg) == 1
    assert seg[0]["dias"] < 0
    assert seg[0]["situacao"] == "VENCIDO"


def test_garantia_com_folga_nao_aparece(base):
    r = _responder(base, "chefe", "garantia_vencendo")
    assert "OBRA-OK" not in _obras(r)


def test_obra_sem_data_de_garantia_nao_aparece(base):
    """Não aparecer não é o mesmo que estar em dia — e a observação diz isso."""
    r = _responder(base, "chefe", "garantia_vencendo")
    assert "OBRA-NUA" not in _obras(r)
    assert "não sabe" in r["observacao"]


def test_o_prazo_em_dias_pode_ser_alargado(base):
    curto = _responder(base, "chefe", "garantia_vencendo", dias="5")
    longo = _responder(base, "chefe", "garantia_vencendo", dias="500")
    assert "OBRA-OK" not in _obras(curto)
    assert "OBRA-OK" in _obras(longo)


def test_prazo_bobagem_nao_derruba_a_resposta(base):
    r = _responder(base, "chefe", "garantia_vencendo", dias="ontem")
    assert "60 dias" in r["titulo"]


# ---------------------------------------------------------------------------
# Vigência de contrato
# ---------------------------------------------------------------------------
def test_obra_aberta_com_vigencia_estourada_aparece(base):
    r = _responder(base, "chefe", "vigencia_vencida")
    assert "OBRA-PZ" in _obras(r)


def test_obra_ja_concluida_nao_aparece_mesmo_com_vigencia_velha(base):
    """A obra encerrada venceu há 900 dias e NÃO é pendência de ninguém."""
    r = _responder(base, "chefe", "vigencia_vencida")
    assert "OBRA-FIM" not in _obras(r)


def test_a_observacao_explica_o_que_e_obra_aberta(base):
    """A palavra "aberta" é ambígua, então a resposta diz o que usou."""
    r = _responder(base, "chefe", "vigencia_vencida")
    assert "FASE" in r["observacao"]


# ---------------------------------------------------------------------------
# ESCOPO — a parte que o dublê da suíte não alcança
# ---------------------------------------------------------------------------
def test_quem_e_preso_a_uma_obra_so_enxerga_a_dele(base):
    """O administrativo da OBRA-OK não pode ver a pendência das outras."""
    r = _responder(base, "da_obra", "cadastro_incompleto")
    assert _obras(r) == set()      # a obra dele está completa
    g = _responder(base, "da_obra", "garantia_vencendo", dias="500")
    assert _obras(g) == {"OBRA-OK"}


def test_quem_enxerga_so_o_que_lanca_nao_ve_obra_nenhuma(base):
    """Obra não tem autor. Para quem enxerga por autoria, o único recorte
    possível é a obra designada — e sem nenhuma designada, nada. É o padrão
    NEGAR do ERP, e foi o que faltou nas Locações."""
    for chave in ("cadastro_incompleto", "garantia_vencendo", "vigencia_vencida"):
        r = _responder(base, "lancador", chave)
        assert r["linhas"] == [], f"{chave} vazou obra para quem vê só o que lança"


def test_o_chefe_enxerga_todas(base):
    r = _responder(base, "chefe", "cadastro_incompleto")
    assert "OBRA-NUA" in _obras(r)


# ---------------------------------------------------------------------------
# O que este grupo NÃO promete — e não pode passar a prometer por descuido
# ---------------------------------------------------------------------------
PALAVRAS_AINDA_NAO_DECIDIDAS = ("custo", "resultado", "lucro", "margem",
                                "gastou", "gastei")


def test_o_grupo_de_obras_nao_responde_custo_nem_resultado():
    """Trava proposital.

    Enquanto o dono não disser o que "custo da obra" e "resultado da obra"
    significam — competência ou caixa, com ou sem o que está em análise —,
    responder é escolher uma leitura por ele em silêncio. Quem for acrescentar
    a pergunta vai esbarrar aqui e lembrar de combinar a palavra primeiro.
    """
    for pergunta in catalogo.do_grupo("obras"):
        texto = (pergunta["pergunta"] + " " + " ".join(pergunta["exemplos"])).lower()
        for palavra in PALAVRAS_AINDA_NAO_DECIDIDAS:
            assert palavra not in texto, (
                f"A pergunta “{pergunta['pergunta']}” usa a palavra "
                f"“{palavra}”, que ainda não tem definição combinada.")


def test_as_travas_do_cadastro_sao_as_mesmas_que_a_emissao_exige():
    """Se a emissão passar a exigir outro campo, a resposta tem de saber.

    Prometer "cadastro completo" e a nota falhar mesmo assim é pior que não
    ter a pergunta: o dono confia e descobre no pior momento.
    """
    from app.apps.erp.core.perguntas.respostas import TRAVAS_DO_CADASTRO
    import inspect
    from app.apps.erp.core.notas_emitidas import automatica

    fonte = inspect.getsource(automatica)
    for campo, _, _ in TRAVAS_DO_CADASTRO:
        if campo == "empresa_id":
            continue        # a empresa entra pelo destino, não por obra.campo
        assert f"obra.{campo}" in fonte or f"_aliquota_iss(obra)" in fonte, (
            f"A conferência de {campo} sumiu da emissão automática — a "
            f"pergunta de cadastro incompleto ficou prometendo o que a "
            f"emissão não exige mais.")
