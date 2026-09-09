"""Os blocos de documentos — o que sempre é pedido junto.

Nasce da dúvida do dono, em 09/09/2026: *"como é que esses blocos vão se
associar a determinados documentos? Se isso é fácil de resolver."*

A resposta que este arquivo prova: **o bloco aponta para TIPOS, não para
documentos**. Um bloco é uma lista de tipos mais um recorte (esta obra, esta
competência), e o sistema procura. É por isso que o bloco fiscal de agosto e o
de setembro são o MESMO bloco — ninguém remonta nada.

O que se prova:

  1. O mesmo bloco, com competências diferentes, traz documentos diferentes.
  2. O bloco fiscal de uma OBRA traz também os documentos da EMPRESA daquela
     obra (DCTFWeb, DARF) — sem isso ele viria pela metade.
  3. O que falta é dito, separando obrigatório de opcional.
  4. Certidão VENCIDA não entra: vai para a lista de faltas dizendo que venceu.
  5. O zip traz o CONFERENCIA.txt, e ele é o primeiro arquivo.
  6. Quem não pode ver um tipo não recebe "está faltando" — seria mentira, e
     entregaria a existência do documento.
"""
from __future__ import annotations

import zipfile
from datetime import date, timedelta
from io import BytesIO

import pytest

from app.apps.erp.core.arquivo import blocos, catalogo
from app.apps.erp.core.arquivo import service as arq
from app.apps.erp.core.auth.service import gerar_hash
from app.apps.erp.core.comum.auditoria import ErroValidacao
from app.apps.erp.db.models.cadastros import (Empresa, Obra,
                                              PerfilUsuario as P, Usuario)

pytestmark = pytest.mark.banco


@pytest.fixture
def cenario(sessao_real):
    s = sessao_real
    catalogo.aplicar(s)
    blocos.aplicar(s)
    admin = Usuario(nome="Admin dos blocos", email="blocos@teste.local", ativo=True,
                    senha_hash=gerar_hash("senha-de-teste-123"), perfil=P.ADMIN)
    obreiro = Usuario(nome="Administrativo", email="obra.bloco@teste.local",
                      ativo=True, senha_hash=gerar_hash("senha-de-teste-123"),
                      perfil=P.ADMINISTRATIVO_OBRA)
    emp = Empresa(razao_social="BWS Construções LTDA", nome_fantasia="BWS",
                  cnpj="11222333000181")
    s.add_all([admin, obreiro, emp])
    s.flush()
    obra = Obra(codigo="ESCPLANALTO", nome="Escola Planalto", empresa_id=emp.id)
    s.add(obra)
    s.flush()
    return {"s": s, "admin": admin, "obreiro": obreiro, "empresa": emp, "obra": obra}


def _guardar(cenario, tipo, *, competencia=None, validade=None, marca=b""):
    """Guarda um documento do tipo pedido, com o dono certo para o tipo."""
    s = cenario["s"]
    t = catalogo.obter(s, tipo)
    dono = ({"obra_id": cenario["obra"].id} if t.dono == "OBRA"
            else {"empresa_id": cenario["empresa"].id})
    return arq.arquivar(
        s, marca or f"conteudo {tipo} {competencia} {validade}".encode(),
        f"{tipo.lower()}.pdf", tipo_codigo=tipo,
        competencia=competencia, validade=validade,
        emissao=date(2026, 1, 1), usuario=cenario["admin"], **dono)


# ---------------------------------------------------------------------------
# 1 e 2 — o bloco aponta para TIPOS, e resolve a empresa pela obra
# ---------------------------------------------------------------------------
def test_o_mesmo_bloco_muda_de_conteudo_com_a_competencia(cenario):
    """É a prova de que o bloco não guarda lista de arquivos."""
    s = cenario["s"]
    _guardar(cenario, "FOLHA", competencia=date(2026, 7, 1))
    _guardar(cenario, "FOLHA", competencia=date(2026, 8, 1))

    julho = blocos.montar(s, "FISCAL", obra_id=cenario["obra"].id,
                          competencia=date(2026, 7, 1), usuario=cenario["admin"])
    agosto = blocos.montar(s, "FISCAL", obra_id=cenario["obra"].id,
                           competencia=date(2026, 8, 1), usuario=cenario["admin"])
    assert len(julho["documentos"]) == 1
    assert len(agosto["documentos"]) == 1
    assert julho["documentos"][0]["id"] != agosto["documentos"][0]["id"]
    assert julho["rotulo"] == "FISCAL_ESCPLANALTO_2026-07"


def test_o_bloco_fiscal_da_obra_traz_o_documento_da_empresa(cenario):
    """Sem isto o bloco viria pela metade e ninguém entenderia por quê: o
    recibo da DCTFWeb é da EMPRESA, e é pedido junto com a medição DA OBRA."""
    s = cenario["s"]
    _guardar(cenario, "FOLHA", competencia=date(2026, 8, 1))
    _guardar(cenario, "DCTFWEB-RECIBO", competencia=date(2026, 8, 1))

    r = blocos.montar(s, "FISCAL", obra_id=cenario["obra"].id,
                      competencia=date(2026, 8, 1), usuario=cenario["admin"])
    tipos = {d["tipo"] for d in r["documentos"]}
    assert tipos == {"FOLHA", "DCTFWEB-RECIBO"}
    assert r["empresa"] == "BWS", "a empresa saiu do cadastro da obra"


def test_bloco_de_obra_sem_obra_e_recusado(cenario):
    with pytest.raises(ErroValidacao) as e:
        blocos.montar(cenario["s"], "FISCAL", competencia=date(2026, 8, 1),
                      usuario=cenario["admin"])
    assert "obra" in str(e.value).lower()


def test_bloco_de_competencia_sem_competencia_e_recusado(cenario):
    with pytest.raises(ErroValidacao) as e:
        blocos.montar(cenario["s"], "FISCAL", obra_id=cenario["obra"].id,
                      usuario=cenario["admin"])
    assert "mês" in str(e.value) or "competência" in str(e.value)


# ---------------------------------------------------------------------------
# 3 — o que falta é dito, e separado
# ---------------------------------------------------------------------------
def test_diz_o_que_falta_separando_obrigatorio_de_opcional(cenario):
    """Bloco que entrega oito de dez arquivos calado é pior que bloco nenhum."""
    s = cenario["s"]
    _guardar(cenario, "FOLHA", competencia=date(2026, 8, 1))
    r = blocos.montar(s, "FISCAL", obra_id=cenario["obra"].id,
                      competencia=date(2026, 8, 1), usuario=cenario["admin"])
    assert not r["completo"]
    assert r["faltas_obrigatorias"] > 0
    faltando = {f["tipo"] for f in r["faltas"]}
    assert "GUIA-FGTS" in faltando
    opcionais = {f["tipo"] for f in r["faltas"] if not f["obrigatorio"]}
    assert "DARF-PIS-COFINS" in opcionais


def test_bloco_completo_diz_que_esta_completo(cenario):
    s = cenario["s"]
    for tipo in ("CONTRATO-OBRA", "ART", "MATRICULA-CEI-CNO"):
        _guardar(cenario, tipo,
                 validade=date(2027, 1, 1) if tipo == "CONTRATO-OBRA" else None)
    r = blocos.montar(s, "OBRA", obra_id=cenario["obra"].id, usuario=cenario["admin"])
    assert r["completo"] is True
    assert r["faltas_obrigatorias"] == 0


# ---------------------------------------------------------------------------
# 4 — certidão vencida NÃO entra
# ---------------------------------------------------------------------------
def test_certidao_vencida_nao_entra_e_aparece_como_falta(cenario):
    """Mandar certidão vencida é pior do que não mandar."""
    s, hoje = cenario["s"], date.today()
    _guardar(cenario, "CND-FEDERAL", validade=hoje - timedelta(days=2))
    r = blocos.montar(s, "HABILITACAO", empresa_id=cenario["empresa"].id,
                      usuario=cenario["admin"])
    assert not any(d["tipo"] == "CND-FEDERAL" for d in r["documentos"])
    falta = next(f for f in r["faltas"] if f["tipo"] == "CND-FEDERAL")
    assert "VENCIDO" in falta["motivo"]


def test_entre_duas_certidoes_validas_vai_a_de_validade_mais_longa(cenario):
    s, hoje = cenario["s"], date.today()
    _guardar(cenario, "CND-FEDERAL", validade=hoje + timedelta(days=10), marca=b"curta")
    _guardar(cenario, "CND-FEDERAL", validade=hoje + timedelta(days=200), marca=b"longa")
    r = blocos.montar(s, "HABILITACAO", empresa_id=cenario["empresa"].id,
                      usuario=cenario["admin"])
    escolhida = [d for d in r["documentos"] if d["tipo"] == "CND-FEDERAL"]
    assert len(escolhida) == 1, "só uma certidão de cada tipo vai no bloco"
    assert escolhida[0]["validade"] == (hoje + timedelta(days=200)).isoformat()


# ---------------------------------------------------------------------------
# 5 — o zip
# ---------------------------------------------------------------------------
def test_o_zip_traz_a_conferencia_primeiro_e_os_arquivos(cenario):
    s = cenario["s"]
    _guardar(cenario, "FOLHA", competencia=date(2026, 8, 1))
    _guardar(cenario, "GUIA-FGTS", competencia=date(2026, 8, 1))

    nome, dados, r = blocos.gerar_zip(s, "FISCAL", obra_id=cenario["obra"].id,
                                      competencia=date(2026, 8, 1),
                                      usuario=cenario["admin"])
    assert nome == "FISCAL_ESCPLANALTO_2026-08.zip"
    with zipfile.ZipFile(BytesIO(dados)) as z:
        nomes_no_zip = z.namelist()
        assert nomes_no_zip[0] == "CONFERENCIA.txt", \
            "a conferência tem de ser o primeiro nome que aparece"
        texto = z.read("CONFERENCIA.txt").decode("utf-8")
        assert "FOLHA_ESCPLANALTO_2026-08.pdf" in nomes_no_zip
        assert "GUIA-FGTS_ESCPLANALTO_2026-08.pdf" in nomes_no_zip
    assert "O QUE ESTÁ FALTANDO" in texto
    assert "OBRIGATÓRIO" in texto, "a falta obrigatória tem de gritar"
    assert "ATENÇÃO" in texto


def test_a_conferencia_diz_quando_esta_tudo_certo(cenario):
    s = cenario["s"]
    for tipo in ("CONTRATO-OBRA", "ART", "MATRICULA-CEI-CNO"):
        _guardar(cenario, tipo,
                 validade=date(2027, 1, 1) if tipo == "CONTRATO-OBRA" else None)
    _, dados, _ = blocos.gerar_zip(s, "OBRA", obra_id=cenario["obra"].id,
                                   usuario=cenario["admin"])
    with zipfile.ZipFile(BytesIO(dados)) as z:
        texto = z.read("CONFERENCIA.txt").decode("utf-8")
    assert "Nada OBRIGATÓRIO" in texto, \
        "bloco com só opcionais faltando ESTÁ pronto para entregar, e tem de dizer"
    assert "ATENÇÃO" not in texto


def test_todos_os_aditivos_vao_e_nao_se_atropelam_no_zip(cenario):
    """DUAS coisas de uma vez, e as duas foram achadas por este caso:

    1. Aditivo NÃO é certidão: os aditivos de um contrato são vários de
       propósito, e mandar só o último esconderia o histórico. (Certidão é o
       contrário: vai uma só, a de validade mais longa.)
    2. Dois aditivos têm o mesmo nome padronizado, e o zip não aceita repetido.
    """
    s = cenario["s"]
    for marca in (b"primeiro", b"segundo"):
        arq.arquivar(s, marca, "aditivo.pdf", tipo_codigo="ADITIVO",
                     obra_id=cenario["obra"].id, validade=date(2027, 1, 1),
                     emissao=date(2026, 3, 1), usuario=cenario["admin"])
    _, dados, _ = blocos.gerar_zip(s, "OBRA", obra_id=cenario["obra"].id,
                                   usuario=cenario["admin"])
    with zipfile.ZipFile(BytesIO(dados)) as z:
        aditivos = [n for n in z.namelist() if n.startswith("ADITIVO")]
    assert len(aditivos) == 2 and len(set(aditivos)) == 2


# ---------------------------------------------------------------------------
# 6 — sigilo
# ---------------------------------------------------------------------------
def test_quem_nao_pode_ver_o_tipo_nao_recebe_falta_dele(cenario):
    """Dizer "está faltando a folha" para quem não pode ver folha seria mentira
    — e já entregaria que o documento existe."""
    s = cenario["s"]
    _guardar(cenario, "FOLHA", competencia=date(2026, 8, 1))
    r = blocos.montar(s, "FISCAL", obra_id=cenario["obra"].id,
                      competencia=date(2026, 8, 1), usuario=cenario["obreiro"])
    tipos_citados = ({d["tipo"] for d in r["documentos"]}
                     | {f["tipo"] for f in r["faltas"]})
    assert "FOLHA" not in tipos_citados
    assert not r["documentos"], "administrativo de obra não leva folha nem guia"


# ---------------------------------------------------------------------------
# O catálogo de blocos
# ---------------------------------------------------------------------------
def test_aplicar_duas_vezes_nao_duplica_nem_sobrescreve(cenario):
    """O conteúdo de um bloco é decisão da BWS; aplicar de novo não pode apagar
    ajuste feito por quem sabe."""
    s = cenario["s"]
    fiscal = next(b for b in blocos.listar(s) if b["codigo"] == "FISCAL")
    antes = len(fiscal["itens"])
    r = blocos.aplicar(s)
    assert r["criados"] == 0
    depois = next(b for b in blocos.listar(s) if b["codigo"] == "FISCAL")
    assert len(depois["itens"]) == antes


def test_todo_item_de_bloco_aponta_para_tipo_que_existe(cenario):
    codigos = {t["codigo"] for t in catalogo.listar(cenario["s"])}
    for b in blocos.listar(cenario["s"]):
        for i in b["itens"]:
            assert i["tipo"] in codigos, f"{b['codigo']} cita tipo inexistente {i['tipo']}"


def test_todo_bloco_tem_recorte_valido(cenario):
    for b in blocos.listar(cenario["s"]):
        assert b["recorte"] in blocos.RECORTES, b
