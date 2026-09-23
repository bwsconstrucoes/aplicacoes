"""DOIS DEFEITOS QUE O DONO ACHOU USANDO O SISTEMA — 23/09/2026.

1. *"atualizei o cadastro da obra com a empresa, mas a crítica de sem empresa
   não saiu"*. A tag vermelha "Sem empresa" do painel de Obras lê o campo da
   LISTA de obras — e a lista nunca mandava a empresa. Gravava certo, a ficha
   mostrava certo, e o alerta ficava para sempre. Defeito pior que "não grava":
   o sistema afirmava, em vermelho, uma coisa falsa sobre um cadastro correto.

2. *"caso eu adicione um documento de forma equivocada e precise alterar, não
   tem opção pra isso. Ou até mesmo excluir algo que esteja errado."* Anexar
   era via de mão única fora da tela do Arquivo: errou o tipo, e não havia
   conserto.
"""
from __future__ import annotations

import pytest

from app.apps.erp.core.auth.service import gerar_hash
from app.apps.erp.core.comum.auditoria import ErroValidacao
from app.apps.erp.core.documentos.armazenamento import (
    corrigir, excluir, listar, salvar,
)
from app.apps.erp.db.models.cadastros import (
    Empresa, Obra, PerfilUsuario as P, Usuario,
)

from conftest import como

pytestmark = pytest.mark.banco


def _pessoa(s, apelido, perfil=P.ADMIN):
    u = Usuario(nome=apelido, email=f"{apelido}@teste.local", ativo=True,
                senha_hash=gerar_hash("senha-de-teste-123"), perfil=perfil,
                telefone="5585900000000")
    s.add(u)
    s.flush()
    return u


@pytest.fixture
def cenario(sessao_real):
    s = sessao_real
    empresa = Empresa(razao_social="BWS Documentos Ltda",
                      cnpj="11222333000181", ativo=True)
    s.add(empresa)
    obra = Obra(codigo="DOC-01", nome="Obra dos documentos", status="ATIVA")
    s.add(obra)
    s.flush()
    return {"s": s, "obra": obra, "empresa": empresa,
            "dono": _pessoa(s, "dono-documentos")}


# ---------------------------------------------------------------------------
# 1. A EMPRESA DA OBRA VOLTA NA LISTA — é o que apaga a tag "Sem empresa"
# ---------------------------------------------------------------------------
def _obra_na_lista(cli, obra_id):
    corpo = cli.get("/erp/api/obras").get_json()
    return next(o for o in corpo["obras"] if o["id"] == obra_id)


def test_obra_sem_empresa_vem_marcada_na_lista(cenario, app_real):
    cli = como(app_real, cenario["dono"].id)

    linha = _obra_na_lista(cli, cenario["obra"].id)

    assert "empresa_id" in linha, "sem este campo a tela não consegue nem avisar"
    assert not linha["empresa_id"]


def test_definir_a_empresa_apaga_a_critica_na_lista(cenario, app_real):
    """O defeito do dono, na íntegra: salvar a empresa e a lista continuar
    dizendo que não tem."""
    c = cenario
    cli = como(app_real, c["dono"].id)

    r = cli.post(f"/erp/api/obras/{c['obra'].id}",
                 json={"empresa_id": c["empresa"].id})
    assert r.status_code == 200, r.get_data(as_text=True)[:200]

    assert _obra_na_lista(cli, c["obra"].id)["empresa_id"] == c["empresa"].id


def test_o_atalho_da_tela_de_empresas_tambem_aparece_na_lista(cenario, app_real):
    """A empresa se define por dois caminhos — o cadastro da obra e o atalho da
    tela de Empresas. Os dois têm de chegar no mesmo lugar."""
    c = cenario
    cli = como(app_real, c["dono"].id)

    r = cli.post(f"/erp/api/obras/{c['obra'].id}/empresa",
                 json={"empresa_id": c["empresa"].id})
    assert r.status_code == 200, r.get_data(as_text=True)[:200]

    assert _obra_na_lista(cli, c["obra"].id)["empresa_id"] == c["empresa"].id


def test_tirar_a_empresa_traz_a_critica_de_volta(cenario, app_real):
    c = cenario
    cli = como(app_real, c["dono"].id)
    cli.post(f"/erp/api/obras/{c['obra'].id}", json={"empresa_id": c["empresa"].id})

    cli.post(f"/erp/api/obras/{c['obra'].id}", json={"empresa_id": ""})

    assert not _obra_na_lista(cli, c["obra"].id)["empresa_id"]


# ---------------------------------------------------------------------------
# 2. CORRIGIR E EXCLUIR O DOCUMENTO ANEXADO
# ---------------------------------------------------------------------------
def _anexar(c, categoria="OUTRO", descricao=""):
    return salvar(c["s"], b"conteudo do documento de teste", "contrato.txt",
                  entidade_tipo="obra", entidade_id=c["obra"].id,
                  categoria=categoria, descricao=descricao, usuario=c["dono"])


def test_corrigir_troca_o_tipo_e_a_descricao(cenario):
    c = cenario
    a = _anexar(c, categoria="OUTRO", descricao="")

    corrigir(c["s"], a.id, c["dono"], categoria="CONTRATO",
             descricao="Contrato da obra, assinado")

    lido = listar(c["s"], "obra", c["obra"].id)[0]
    assert lido["categoria"] == "CONTRATO"
    assert lido["descricao"] == "Contrato da obra, assinado"


def test_corrigir_so_a_descricao_nao_mexe_no_tipo(cenario):
    c = cenario
    a = _anexar(c, categoria="ART", descricao="errado")

    corrigir(c["s"], a.id, c["dono"], descricao="ART do engenheiro")

    lido = listar(c["s"], "obra", c["obra"].id)[0]
    assert lido["categoria"] == "ART"
    assert lido["descricao"] == "ART do engenheiro"


def test_descricao_em_branco_limpa_de_verdade(cenario):
    c = cenario
    a = _anexar(c, descricao="texto que não era pra estar aqui")

    corrigir(c["s"], a.id, c["dono"], descricao="   ")

    assert listar(c["s"], "obra", c["obra"].id)[0]["descricao"] in (None, "")


def test_tipo_inventado_e_recusado(cenario):
    c = cenario
    a = _anexar(c)

    with pytest.raises(ErroValidacao):
        corrigir(c["s"], a.id, c["dono"], categoria="NAO_EXISTE")


def test_corrigir_o_arquivo_em_si_nao_e_oferecido(cenario):
    """O arquivo não se troca por baixo do mesmo registro — de propósito. Quem
    anexou o papel errado exclui e anexa o certo, e a trilha guarda a
    diferença entre 'corrigi o rótulo' e 'é outro documento'."""
    c = cenario
    a = _anexar(c)
    antes = a.nome_arquivo, a.tamanho_bytes

    corrigir(c["s"], a.id, c["dono"], categoria="NOTA")

    assert (a.nome_arquivo, a.tamanho_bytes) == antes


def test_corrigir_fica_registrado_na_trilha(cenario):
    from app.apps.erp.db.models.financeiro import EventoAuditoria
    from sqlalchemy import select
    c = cenario
    a = _anexar(c, categoria="OUTRO")

    corrigir(c["s"], a.id, c["dono"], categoria="SEGURO")
    c["s"].flush()

    eventos = c["s"].scalars(select(EventoAuditoria).where(
        EventoAuditoria.entidade_tipo == "obra",
        EventoAuditoria.entidade_id == c["obra"].id,
        EventoAuditoria.acao == "ANEXO_CORRIGIDO")).all()
    assert len(eventos) == 1


def test_correcao_que_nao_muda_nada_nao_suja_a_trilha(cenario):
    from app.apps.erp.db.models.financeiro import EventoAuditoria
    from sqlalchemy import select
    c = cenario
    a = _anexar(c, categoria="NOTA", descricao="a mesma coisa")

    corrigir(c["s"], a.id, c["dono"], categoria="NOTA", descricao="a mesma coisa")
    c["s"].flush()

    assert not c["s"].scalars(select(EventoAuditoria).where(
        EventoAuditoria.acao == "ANEXO_CORRIGIDO")).all()


def test_corrigir_pela_tela_responde_o_que_ficou_gravado(cenario, app_real):
    c = cenario
    a = _anexar(c, categoria="OUTRO")
    c["s"].commit()

    r = como(app_real, c["dono"].id).patch(
        f"/erp/api/anexos/{a.id}",
        json={"categoria": "MEDICAO", "descricao": "Medição 03"})

    assert r.status_code == 200, r.get_data(as_text=True)[:200]
    assert r.get_json()["anexo"]["categoria"] == "MEDICAO"
    assert r.get_json()["anexo"]["descricao"] == "Medição 03"


def test_excluir_pela_tela_tira_o_documento_da_lista(cenario, app_real):
    c = cenario
    a = _anexar(c)
    c["s"].commit()
    cli = como(app_real, c["dono"].id)

    r = cli.delete(f"/erp/api/anexos/{a.id}")

    assert r.status_code == 200, r.get_data(as_text=True)[:200]
    lista = cli.get(f"/erp/api/anexos/obra/{c['obra'].id}").get_json()["anexos"]
    assert not lista


def test_quem_nao_alcanca_a_obra_nao_corrige_o_documento_dela(cenario, app_real):
    """Fora do escopo responde 404 — dizer 'sem permissão' confirmaria que o
    documento existe."""
    c = cenario
    a = _anexar(c, categoria="OUTRO")
    de_fora = _pessoa(c["s"], "preso-a-outra-obra", P.GESTOR_OBRA)
    c["s"].commit()

    r = como(app_real, de_fora.id).patch(
        f"/erp/api/anexos/{a.id}", json={"categoria": "CONTRATO"})

    assert r.status_code == 404, r.get_data(as_text=True)[:200]
    assert listar(c["s"], "obra", c["obra"].id)[0]["categoria"] == "OUTRO"
