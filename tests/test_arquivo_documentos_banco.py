"""O arquivo de documentos da empresa — com banco de verdade.

Pedido do dono em 09/09/2026, especificado em `GESTAO_DOCUMENTOS.md`:
*"um ambiente onde eu pudesse simplesmente jogar esse documento, ele fosse
interpretado, lido, e a partir dali categorizado, renomeado e salvo."*

Com banco porque as garantias que importam são do BANCO: um documento tem
EXATAMENTE um dono, competência é sempre o primeiro dia do mês, e o mesmo
arquivo não é catalogado duas vezes. Nada disso o dublê de sessão finge.

O que se prova:

  1. O nome padronizado sai do jeito combinado — sem acento, sem espaço, com a
     validade legível, e a extensão preservada.
  2. Um documento tem um dono. Nem zero, nem dois.
  3. Documento que vence sem validade é recusado — senão o sistema nunca avisa.
  4. O que está vencendo aparece, e o que venceu aparece separado.
  5. O sigilo do tipo decide quem vê: folha de pagamento não é para todo mundo.
  6. A busca acha pelo texto de dentro do documento.
"""
from __future__ import annotations

from datetime import date, timedelta

import pytest
from sqlalchemy.exc import IntegrityError

from app.apps.erp.core.arquivo import catalogo, nomes
from app.apps.erp.core.arquivo import service as arq
from app.apps.erp.core.auth.service import gerar_hash
from app.apps.erp.core.comum.auditoria import ErroValidacao
from app.apps.erp.db.models.cadastros import (Empresa, Obra,
                                              PerfilUsuario as P, Usuario)
from app.apps.erp.db.models.financeiro import Documento

pytestmark = pytest.mark.banco

ARQUIVO = b"%PDF-1.4 documento de teste do arquivo"


@pytest.fixture
def cenario(sessao_real):
    s = sessao_real
    catalogo.aplicar(s)
    admin = Usuario(nome="Admin do arquivo", email="arquivo@teste.local", ativo=True,
                    senha_hash=gerar_hash("senha-de-teste-123"), perfil=P.ADMIN)
    obreiro = Usuario(nome="Administrativo da obra", email="obra.arq@teste.local",
                      ativo=True, senha_hash=gerar_hash("senha-de-teste-123"),
                      perfil=P.ADMINISTRATIVO_OBRA)
    emp = Empresa(razao_social="BWS Construções e Empreendimentos LTDA",
                  nome_fantasia="BWS", cnpj="11222333000181")
    obra = Obra(codigo="ESCPLANALTO", nome="Escola Planalto")
    s.add_all([admin, obreiro, emp, obra])
    s.flush()
    return {"s": s, "admin": admin, "obreiro": obreiro, "empresa": emp, "obra": obra}


# ---------------------------------------------------------------------------
# 1. A NOMENCLATURA
# ---------------------------------------------------------------------------
def test_o_nome_nao_leva_acento_nem_espaco():
    """Não é preciosismo: portal de licitação e sistema de prefeitura ainda
    engasgam com acento, e o arquivo volta corrompido ou é recusado."""
    assert nomes.limpar("Construções Planalto") == "CONSTRUCOES-PLANALTO"
    assert nomes.limpar("João da Silva") == "JOAO-DA-SILVA"
    assert nomes.limpar("  --Ação   Cível-- ") == "ACAO-CIVEL"


@pytest.mark.parametrize("original,esperado", [
    ("certidao.PDF", "pdf"), ("foto.JPEG", "jpeg"), ("sem-extensao", ""),
    ("estranho.p df", "pdf"), ("x.tar.gz", "gz"),
])
def test_a_extensao_vem_do_arquivo_e_e_domada(original, esperado):
    assert nomes.extensao(original) == esperado


def test_documento_que_vence_leva_a_validade_no_nome():
    """Bater o olho no nome e saber até quando vale é metade do problema."""
    n = nomes.montar(tipo_codigo="CRF-FGTS", dono="BWS",
                     validade=date(2026, 10, 2), emissao=date(2026, 9, 2),
                     nome_original="crf.pdf", vence=True)
    assert n == "CRF-FGTS_BWS_val-2026-10-02.pdf"


def test_documento_de_competencia_leva_o_mes():
    n = nomes.montar(tipo_codigo="GUIA-FGTS", dono="ESCPLANALTO",
                     competencia=date(2026, 8, 1), nome_original="guia.pdf",
                     por_competencia=True)
    assert n == "GUIA-FGTS_ESCPLANALTO_2026-08.pdf"


def test_referencia_entra_quando_existe_e_some_quando_nao():
    com = nomes.montar(tipo_codigo="ADITIVO", dono="ESCPLANALTO", referencia="02",
                       emissao=date(2026, 4, 18), nome_original="a.pdf")
    sem = nomes.montar(tipo_codigo="OS", dono="ESCPLANALTO",
                       emissao=date(2026, 4, 18), nome_original="a.pdf")
    assert com == "ADITIVO_ESCPLANALTO_02_2026-04-18.pdf"
    assert sem == "OS_ESCPLANALTO_2026-04-18.pdf"
    assert "__" not in com and "__" not in sem, "campo vazio não pode deixar buraco"


# ---------------------------------------------------------------------------
# 2 e 3. AS REGRAS DE ENTRADA
# ---------------------------------------------------------------------------
def test_arquivar_gera_o_nome_e_guarda(cenario):
    s = cenario["s"]
    d = arq.arquivar(s, ARQUIVO, "crf original.pdf", tipo_codigo="CRF-FGTS",
                     empresa_id=cenario["empresa"].id,
                     validade=date(2026, 10, 2), usuario=cenario["admin"])
    assert d.nome_padronizado == "CRF-FGTS_BWS_val-2026-10-02.pdf"
    assert d.nome_original == "crf original.pdf", "o nome de origem não se perde"
    assert d.anexo_id is not None


def test_documento_sem_dono_e_recusado(cenario):
    s = cenario["s"]
    with pytest.raises(ErroValidacao) as e:
        arq.arquivar(s, ARQUIVO, "x.pdf", tipo_codigo="CND-FEDERAL",
                     validade=date(2027, 1, 1), usuario=cenario["admin"])
    assert "UM dono" in str(e.value)


def test_documento_com_dois_donos_e_recusado(cenario):
    """Documento pendurado em dois lugares não é achado em nenhum."""
    s = cenario["s"]
    with pytest.raises(ErroValidacao):
        arq.arquivar(s, ARQUIVO, "x.pdf", tipo_codigo="CND-FEDERAL",
                     empresa_id=cenario["empresa"].id, obra_id=cenario["obra"].id,
                     validade=date(2027, 1, 1), usuario=cenario["admin"])


def test_o_banco_tambem_recusa_dois_donos(cenario):
    """Mesmo que um código futuro esqueça a regra."""
    s = cenario["s"]
    d = arq.arquivar(s, ARQUIVO, "x.pdf", tipo_codigo="CND-FEDERAL",
                     empresa_id=cenario["empresa"].id,
                     validade=date(2027, 1, 1), usuario=cenario["admin"])
    d.obra_id = cenario["obra"].id
    with pytest.raises(IntegrityError):
        s.flush()
    s.rollback()


def test_documento_que_vence_exige_validade(cenario):
    """Sem validade o sistema não avisa antes — e avisar é metade do valor."""
    s = cenario["s"]
    with pytest.raises(ErroValidacao) as e:
        arq.arquivar(s, ARQUIVO, "x.pdf", tipo_codigo="CND-FEDERAL",
                     empresa_id=cenario["empresa"].id, usuario=cenario["admin"])
    assert "até quando vale" in str(e.value)


def test_documento_de_competencia_exige_o_mes(cenario):
    s = cenario["s"]
    with pytest.raises(ErroValidacao) as e:
        arq.arquivar(s, ARQUIVO, "x.pdf", tipo_codigo="GUIA-FGTS",
                     obra_id=cenario["obra"].id, usuario=cenario["admin"])
    assert "competência" in str(e.value)


def test_a_competencia_e_sempre_o_primeiro_dia_do_mes(cenario):
    """Sem isso, 'agosto' viraria 31 valores e o compilado fiscal não fecharia."""
    s = cenario["s"]
    d = arq.arquivar(s, ARQUIVO, "folha.pdf", tipo_codigo="FOLHA",
                     obra_id=cenario["obra"].id,
                     competencia=date(2026, 8, 23), usuario=cenario["admin"])
    assert d.competencia == date(2026, 8, 1)
    assert d.nome_padronizado == "FOLHA_ESCPLANALTO_2026-08.pdf"


def test_o_mesmo_arquivo_nao_vira_dois_documentos(cenario):
    s = cenario["s"]
    a = arq.arquivar(s, ARQUIVO, "os.pdf", tipo_codigo="OS",
                     obra_id=cenario["obra"].id, emissao=date(2026, 1, 5),
                     usuario=cenario["admin"])
    b = arq.arquivar(s, ARQUIVO, "os.pdf", tipo_codigo="OS",
                     obra_id=cenario["obra"].id, emissao=date(2026, 1, 5),
                     usuario=cenario["admin"])
    assert a.id == b.id


# ---------------------------------------------------------------------------
# 4. VENCIMENTO — o que dá valor ao arquivo
# ---------------------------------------------------------------------------
def test_separa_vencido_de_vencendo_de_valido(cenario):
    s, hoje = cenario["s"], date.today()
    arq.arquivar(s, b"a", "1.pdf", tipo_codigo="CND-FEDERAL",
                 empresa_id=cenario["empresa"].id,
                 validade=hoje - timedelta(days=3), usuario=cenario["admin"])
    arq.arquivar(s, b"b", "2.pdf", tipo_codigo="CND-ESTADUAL",
                 empresa_id=cenario["empresa"].id,
                 validade=hoje + timedelta(days=5), usuario=cenario["admin"])
    arq.arquivar(s, b"c", "3.pdf", tipo_codigo="CND-MUNICIPAL",
                 empresa_id=cenario["empresa"].id,
                 validade=hoje + timedelta(days=300), usuario=cenario["admin"])
    r = arq.listar(s, usuario=cenario["admin"])["resumo"]
    assert (r["vencidos"], r["vencendo"], r["validos"]) == (1, 1, 1)


def test_vencendo_lista_o_que_precisa_de_acao(cenario):
    s, hoje = cenario["s"], date.today()
    arq.arquivar(s, b"a", "1.pdf", tipo_codigo="CRF-FGTS",
                 empresa_id=cenario["empresa"].id,
                 validade=hoje + timedelta(days=4), usuario=cenario["admin"])
    arq.arquivar(s, b"b", "2.pdf", tipo_codigo="CND-FEDERAL",
                 empresa_id=cenario["empresa"].id,
                 validade=hoje + timedelta(days=200), usuario=cenario["admin"])
    achados = arq.vencendo(s, dias=30, usuario=cenario["admin"])
    assert len(achados) == 1 and achados[0]["tipo"] == "CRF-FGTS"


# ---------------------------------------------------------------------------
# 5. SIGILO — folha de pagamento não é para todo mundo
# ---------------------------------------------------------------------------
def test_o_sigilo_do_tipo_decide_quem_ve(cenario):
    s = cenario["s"]
    arq.arquivar(s, b"folha", "folha.pdf", tipo_codigo="FOLHA",
                 obra_id=cenario["obra"].id, competencia=date(2026, 8, 1),
                 usuario=cenario["admin"])
    arq.arquivar(s, b"cnd", "cnd.pdf", tipo_codigo="CND-FEDERAL",
                 empresa_id=cenario["empresa"].id,
                 validade=date(2027, 1, 1), usuario=cenario["admin"])

    do_admin = arq.listar(s, usuario=cenario["admin"])["documentos"]
    do_obreiro = arq.listar(s, usuario=cenario["obreiro"])["documentos"]
    assert {d["tipo"] for d in do_admin} == {"FOLHA", "CND-FEDERAL"}
    assert {d["tipo"] for d in do_obreiro} == {"CND-FEDERAL"}, \
        "administrativo de obra não vê folha de pagamento"


def test_as_faixas_de_sigilo_por_perfil(cenario):
    assert arq.sigilos_visiveis(cenario["admin"]) == ("ABERTO", "RESTRITO", "PESSOAL")
    assert arq.sigilos_visiveis(cenario["obreiro"]) == ("ABERTO",)
    assert arq.sigilos_visiveis(None) == ("ABERTO",)


# ---------------------------------------------------------------------------
# 6. ENCONTRAR
# ---------------------------------------------------------------------------
def test_a_busca_acha_pelo_texto_de_dentro(cenario):
    """O texto é extraído na entrada justamente para isto."""
    s = cenario["s"]
    arq.arquivar(s, b"x", "contrato.pdf", tipo_codigo="CONTRATO-OBRA",
                 obra_id=cenario["obra"].id, validade=date(2027, 6, 1),
                 texto="cláusula décima: o reajuste será pelo INCC",
                 usuario=cenario["admin"])
    arq.arquivar(s, b"y", "os.pdf", tipo_codigo="OS", obra_id=cenario["obra"].id,
                 emissao=date(2026, 2, 1), usuario=cenario["admin"])
    achados = arq.listar(s, usuario=cenario["admin"], busca="INCC")["documentos"]
    assert len(achados) == 1 and achados[0]["tipo"] == "CONTRATO-OBRA"


def test_filtra_por_obra_e_por_competencia(cenario):
    s = cenario["s"]
    for mes in (7, 8):
        arq.arquivar(s, f"folha {mes}".encode(), f"f{mes}.pdf", tipo_codigo="FOLHA",
                     obra_id=cenario["obra"].id, competencia=date(2026, mes, 1),
                     usuario=cenario["admin"])
    r = arq.listar(s, usuario=cenario["admin"], obra_id=cenario["obra"].id,
                   competencia=date(2026, 8, 1))
    assert r["resumo"]["quantidade"] == 1


# ---------------------------------------------------------------------------
# O CATÁLOGO
# ---------------------------------------------------------------------------
def test_aplicar_o_catalogo_duas_vezes_nao_duplica(cenario):
    s = cenario["s"]
    antes = len(catalogo.listar(s))
    catalogo.aplicar(s)
    assert len(catalogo.listar(s)) == antes


def test_todo_tipo_do_catalogo_tem_dono_e_sigilo_validos(cenario):
    for t in catalogo.listar(cenario["s"]):
        assert t["dono"] in catalogo.DONOS, t
        assert t["sigilo"] in catalogo.SIGILOS, t
        assert t["grupo"] in catalogo.GRUPOS, t


def test_todo_tipo_que_vence_sabe_quando_avisar(cenario):
    """Tipo que vence sem prazo de aviso nunca avisaria ninguém — seria guardar
    a data e não usá-la, que é o defeito que este módulo veio corrigir."""
    faltando = [t["codigo"] for t in catalogo.listar(cenario["s"])
                if t["vence"] and not t["avisar_dias"]]
    assert not faltando, f"tipos que vencem sem prazo de aviso: {faltando}"


def test_excluir_o_documento_leva_o_arquivo_junto(cenario):
    from app.apps.erp.db.models.financeiro import Anexo
    s = cenario["s"]
    d = arq.arquivar(s, ARQUIVO, "x.pdf", tipo_codigo="OS",
                     obra_id=cenario["obra"].id, emissao=date(2026, 1, 1),
                     usuario=cenario["admin"])
    anexo_id, doc_id = d.anexo_id, d.id
    arq.excluir(s, doc_id, cenario["admin"])
    s.flush()
    assert s.get(Documento, doc_id) is None
    assert s.get(Anexo, anexo_id) is None, "arquivo órfão é lixo pago"
