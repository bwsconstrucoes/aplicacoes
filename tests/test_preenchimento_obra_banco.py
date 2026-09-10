"""Arquivar o documento E preencher o cadastro da obra, num gesto só.

Pedido do dono em 10/09/2026: *"gostaria que o sistema já preenchesse os campos
de cadastro de obra e ainda arquivasse o arquivo. Dessa forma não perco
tempo."* — e o princípio que ele tirou disso: *"matariamos duas ações...
cadastros e arquivo estarem associados quando fizer sentido"*.

A CHAMADA À IA É DUBLADA aqui. O que precisa de prova não é o modelo: é o que
o sistema FAZ com a resposta dele — e, principalmente, o que ele RECUSA fazer.

O que se prova:

  1. Cada tipo de documento só preenche o que ele PROVA. Uma licença ambiental
     não define valor de contrato, por mais que haja um número lá dentro.
  2. Campo em branco a leitura preenche; campo com valor DIFERENTE vira
     conflito e entra desmarcado. Trocar calado o que a pessoa digitou é a
     maneira mais rápida de o sistema perder a confiança dela.
  3. Quem grava é a pessoa: só o que ela confirmou entra, com o valor que ela
     confirmou — ela pode ter corrigido antes.
  4. O aditivo vira REGISTRO, não sobrescreve o contrato: o valor vigente é o
     original mais os aditivos, e a história é o que o órgão pergunta.
  5. Arquivar e preencher acontecem na MESMA transação: nada de guardar o
     arquivo e deixar o cadastro pela metade.
  6. O escopo por obra continua valendo — quem não enxerga a obra não escreve
     nela por este caminho novo.
"""
from __future__ import annotations

import json
from datetime import date
from decimal import Decimal

import pytest

from app.apps.erp.core.arquivo import catalogo, preenchimento
from app.apps.erp.core.auth.service import gerar_hash
from app.apps.erp.core.comum.auditoria import ErroValidacao
from app.apps.erp.db.models.cadastros import (Empresa, EscopoVisao, Obra,
                                              ObraAditivo, PerfilUsuario as P,
                                              Usuario, UsuarioObra)
from app.apps.erp.db.models.financeiro import Documento
from tests.conftest import como

pytestmark = pytest.mark.banco

PDF = b"%PDF-1.4 documento da obra"


@pytest.fixture
def cenario(sessao_real):
    s = sessao_real
    catalogo.aplicar(s)
    admin = Usuario(nome="Admin da obra", email="obra.doc@teste.local", ativo=True,
                    senha_hash=gerar_hash("senha-de-teste-123"), perfil=P.ADMIN)
    emp = Empresa(razao_social="BWS Construções LTDA", nome_fantasia="BWS",
                  cnpj="11222333000181", ativo=True)
    s.add_all([admin, emp])
    s.flush()
    obra = Obra(codigo="ESCPLANALTO", nome="Escola Planalto", empresa_id=emp.id,
                fase="EM_EXECUCAO")
    s.add(obra)
    s.flush()
    return {"s": s, "admin": admin, "empresa": emp, "obra": obra}


def _leitura(tipo, extraidos=None, aditivo=None):
    """O que a leitura devolveria — o formato de `leitura.sugerir`."""
    return {"tipo_codigo": tipo, "tipo_nome": tipo,
            "dados_extraidos": extraidos or {}, "aditivo": aditivo or {}}


# ---------------------------------------------------------------------------
# 1. CADA TIPO SÓ PREENCHE O QUE PROVA
# ---------------------------------------------------------------------------
def test_a_matricula_preenche_o_cno(cenario):
    r = preenchimento.sugerir_para_obra(
        cenario["s"], cenario["obra"].id, "MATRICULA-CEI-CNO",
        _leitura("MATRICULA-CEI-CNO", {"cno": "90.025.25410/76",
                                       "municipio": "RECIFE", "uf": "PE"}))
    campos = {c["campo"]: c for c in r["campos"]}
    assert campos["cno"]["valor"] == "90.025.25410/76"
    assert campos["cno"]["marcar"] is True, "campo vazio entra marcado"
    assert campos["municipio"]["valor"] == "RECIFE"


def test_a_licenca_nao_define_valor_de_contrato(cenario):
    """Por mais que haja um número dentro dela. A lista por tipo é trava."""
    r = preenchimento.sugerir_para_obra(
        cenario["s"], cenario["obra"].id, "LICENCA",
        _leitura("LICENCA", {"valor_contrato": "9999999.00",
                             "contrato": "CT 001/2026", "municipio": "RECIFE"}))
    campos = {c["campo"] for c in r["campos"]}
    assert "valor_contrato" not in campos
    assert "contrato" not in campos
    assert "municipio" in campos, "endereço a licença prova, e esse entra"


def test_tipo_que_nao_alimenta_cadastro_diz_isso(cenario):
    r = preenchimento.sugerir_para_obra(
        cenario["s"], cenario["obra"].id, "DIARIO-OBRA",
        _leitura("DIARIO-OBRA", {"contrato": "CT 001/2026"}))
    assert r["campos"] == []
    assert r["tipo_nao_preenche"] is True


def test_gravar_campo_fora_da_lista_do_tipo_e_recusado(cenario):
    """Nem que a tela mande: é a trava, não uma sugestão."""
    with pytest.raises(ErroValidacao) as e:
        preenchimento.aplicar_na_obra(
            cenario["s"], cenario["obra"].id, {"valor_contrato": "9999999.00"},
            tipo_codigo="LICENCA", usuario=cenario["admin"])
    assert "não preenche" in str(e.value)


# ---------------------------------------------------------------------------
# 2. O QUE JÁ ESTÁ PREENCHIDO NÃO É SOBRESCRITO SOZINHO
# ---------------------------------------------------------------------------
def test_valor_diferente_vira_conflito_desmarcado(cenario):
    cenario["obra"].contrato = "CT 268/2025"
    cenario["s"].flush()
    r = preenchimento.sugerir_para_obra(
        cenario["s"], cenario["obra"].id, "CONTRATO-OBRA",
        _leitura("CONTRATO-OBRA", {"contrato": "CT 999/2026"}))
    c = next(x for x in r["campos"] if x["campo"] == "contrato")
    assert c["conflito"] is True
    assert c["valor_atual"] == "CT 268/2025"
    assert c["marcar"] is False, "conflito entra DESMARCADO"


def test_valor_igual_nao_aparece_para_decidir(cenario):
    """'AV. BRASIL, 100' e 'Av Brasil 100' são a mesma rua."""
    cenario["obra"].endereco = "AV. BRASIL, 100"
    cenario["s"].flush()
    r = preenchimento.sugerir_para_obra(
        cenario["s"], cenario["obra"].id, "MATRICULA-CEI-CNO",
        _leitura("MATRICULA-CEI-CNO", {"endereco": "Av Brasil 100"}))
    assert not any(c["campo"] == "endereco" for c in r["campos"])


def test_campo_vazio_na_leitura_nao_apaga_o_que_existe(cenario):
    cenario["obra"].cno = "11.111.11111/11"
    cenario["s"].flush()
    r = preenchimento.sugerir_para_obra(
        cenario["s"], cenario["obra"].id, "MATRICULA-CEI-CNO",
        _leitura("MATRICULA-CEI-CNO", {"cno": ""}))
    assert not any(c["campo"] == "cno" for c in r["campos"])
    assert cenario["s"].get(Obra, cenario["obra"].id).cno == "11.111.11111/11"


# ---------------------------------------------------------------------------
# 3. QUEM GRAVA É A PESSOA
# ---------------------------------------------------------------------------
def test_grava_so_o_que_veio_e_com_o_valor_que_veio(cenario):
    """A pessoa pode ter corrigido o que a IA leu antes de confirmar."""
    s = cenario["s"]
    r = preenchimento.aplicar_na_obra(
        s, cenario["obra"].id,
        {"contrato": "CT 268/2025", "valor_contrato": "1.234.567,89"},
        tipo_codigo="CONTRATO-OBRA", usuario=cenario["admin"])
    obra = s.get(Obra, cenario["obra"].id)
    assert obra.contrato == "CT 268/2025"
    assert obra.valor_contrato == Decimal("1234567.89")
    assert r["quantidade"] == 2
    assert any("Valor do contrato" in m for m in r["mudancas"])


def test_o_que_mudou_fica_na_trilha(cenario):
    from app.apps.erp.db.models.financeiro import Evento
    s = cenario["s"]
    preenchimento.aplicar_na_obra(
        s, cenario["obra"].id, {"cno": "90.025.25410/76"},
        tipo_codigo="MATRICULA-CEI-CNO", usuario=cenario["admin"])
    ev = s.query(Evento).filter(Evento.entidade_tipo == "obra",
                                Evento.acao == "PREENCHIDA_POR_DOCUMENTO").all()
    assert len(ev) == 1
    assert "Matrícula CNO/CEI" in " ".join(ev[0].detalhe["mudancas"])


@pytest.mark.parametrize("bruto,esperado", [
    ("1.234.567,89", Decimal("1234567.89")),
    ("1234567.89", Decimal("1234567.89")),
    ("R$ 320.000,00", Decimal("320000.00")),
])
def test_dinheiro_escrito_de_varios_jeitos_chega_certo(cenario, bruto, esperado):
    """Foi assim que 30,00 virou 1.234 uma vez — a conversão é onde dói."""
    preenchimento.aplicar_na_obra(
        cenario["s"], cenario["obra"].id, {"valor_contrato": bruto},
        tipo_codigo="CONTRATO-OBRA", usuario=cenario["admin"])
    assert cenario["s"].get(Obra, cenario["obra"].id).valor_contrato == esperado


# ---------------------------------------------------------------------------
# 4. O ADITIVO É REGISTRO, NÃO SOBRESCRITA
# ---------------------------------------------------------------------------
def test_o_aditivo_nao_sobrescreve_o_valor_do_contrato(cenario):
    s = cenario["s"]
    cenario["obra"].valor_contrato = Decimal("1000000.00")
    s.flush()
    preenchimento.criar_aditivo(
        s, cenario["obra"].id,
        {"numero": "01", "tipo": "VALOR", "valor": "250000.00",
         "data_assinatura": "2026-05-10"}, usuario=cenario["admin"])
    obra = s.get(Obra, cenario["obra"].id)
    assert obra.valor_contrato == Decimal("1000000.00"), "o original não muda"
    a = s.query(ObraAditivo).filter(ObraAditivo.obra_id == obra.id).one()
    assert (a.numero, a.valor) == ("01", Decimal("250000.00"))


def test_aditivo_de_prazo_estende_a_vigencia_do_contrato(cenario):
    """A vigência sim se atualiza: é ela que manda nos alertas."""
    s = cenario["s"]
    cenario["obra"].vigencia_fim = date(2026, 12, 31)
    s.flush()
    preenchimento.criar_aditivo(
        s, cenario["obra"].id,
        {"numero": "02", "tipo": "PRAZO", "dias": "180",
         "nova_vigencia_fim": "2027-06-30"}, usuario=cenario["admin"])
    assert s.get(Obra, cenario["obra"].id).vigencia_fim == date(2027, 6, 30)


def test_aditivo_sem_numero_e_recusado(cenario):
    with pytest.raises(ErroValidacao) as e:
        preenchimento.criar_aditivo(
            cenario["s"], cenario["obra"].id, {"tipo": "VALOR", "valor": "100.00"},
            usuario=cenario["admin"])
    assert "número" in str(e.value)


def test_o_mesmo_aditivo_duas_vezes_e_recusado(cenario):
    s = cenario["s"]
    preenchimento.criar_aditivo(s, cenario["obra"].id,
                                {"numero": "01", "tipo": "VALOR", "valor": "100.00"},
                                usuario=cenario["admin"])
    with pytest.raises(ErroValidacao) as e:
        preenchimento.criar_aditivo(s, cenario["obra"].id,
                                    {"numero": "01", "tipo": "VALOR", "valor": "100.00"},
                                    usuario=cenario["admin"])
    assert "já está registrado" in str(e.value)


def test_so_o_termo_aditivo_propoe_aditivo(cenario):
    r = preenchimento.sugerir_para_obra(
        cenario["s"], cenario["obra"].id, "CONTRATO-OBRA",
        _leitura("CONTRATO-OBRA", {}, {"numero": "01", "valor": "100.00"}))
    assert r["aditivo"] is None


# ---------------------------------------------------------------------------
# 5 e 6. AS ROTAS: UMA TRANSAÇÃO, E O ESCOPO CONTINUA VALENDO
# ---------------------------------------------------------------------------
def _dublar_leitura(monkeypatch, resposta):
    def falso(conteudo, nome_arquivo, instrucao, dica=""):
        falso.instrucao = instrucao
        d = dict(resposta)
        d["texto_extraido"] = "texto do documento"
        d["origem_leitura"] = "PDF_TEXTO"
        return d
    falso.instrucao = ""
    monkeypatch.setattr("app.apps.erp.core.documentos.leitor.ler_com_instrucao", falso)
    return falso


def test_a_leitura_pergunta_pelos_campos_do_cadastro(app_real, cenario, monkeypatch):
    falso = _dublar_leitura(monkeypatch, {
        "tipo_codigo": "MATRICULA-CEI-CNO", "dono_especie": "OBRA",
        "dono_nome": "ESCPLANALTO", "confianca": "ALTA",
        "dados_extraidos": {"cno": "90.025.25410/76"}})
    r = como(app_real, cenario["admin"].id).post(
        f"/erp/api/obras/{cenario['obra'].id}/documento/ler",
        data={"arquivo": (__import__("io").BytesIO(PDF), "cno.pdf")},
        content_type="multipart/form-data")
    assert r.status_code == 200
    d = r.get_json()
    assert "dados_extraidos" in falso.instrucao, "a pergunta dos campos tem de ir junto"
    assert d["cadastro"]["campos"][0]["campo"] == "cno"
    assert cenario["s"].query(Documento).count() == 0, "ler não pode gravar"


def test_arquivar_e_preencher_acontecem_juntos(app_real, cenario, monkeypatch):
    import io as _io
    _dublar_leitura(monkeypatch, {})
    s = cenario["s"]
    r = como(app_real, cenario["admin"].id).post(
        f"/erp/api/obras/{cenario['obra'].id}/documento",
        data={"arquivo": (_io.BytesIO(PDF), "cno.pdf"),
              "tipo": "MATRICULA-CEI-CNO",
              "emissao": "2026-03-10",
              "campos": json.dumps({"cno": "90.025.25410/76"})},
        content_type="multipart/form-data")
    assert r.status_code == 200, r.get_data(as_text=True)
    d = r.get_json()
    assert d["preenchido"]["quantidade"] == 1
    assert s.get(Obra, cenario["obra"].id).cno == "90.025.25410/76"
    assert s.query(Documento).count() == 1
    assert d["documento"]["nome"].startswith("MATRICULA-CEI-CNO_ESCPLANALTO")


def test_campo_recusado_desfaz_o_arquivamento_junto(app_real, cenario, monkeypatch):
    """Guardar o arquivo e deixar o cadastro pela metade seria o pior dos dois
    mundos: a pessoa acharia que fez e não teria feito."""
    import io as _io
    _dublar_leitura(monkeypatch, {})
    s = cenario["s"]
    r = como(app_real, cenario["admin"].id).post(
        f"/erp/api/obras/{cenario['obra'].id}/documento",
        data={"arquivo": (_io.BytesIO(PDF), "licenca.pdf"),
              "tipo": "LICENCA", "validade": "2027-01-01",
              "campos": json.dumps({"valor_contrato": "999.00"})},
        content_type="multipart/form-data")
    assert r.status_code == 400
    s.rollback()
    assert s.query(Documento).count() == 0, "o documento não pode ter ficado"


def test_quem_nao_arquiva_e_barrado_na_acao(app_real, cenario, monkeypatch):
    """Duas travas diferentes, e esta é a primeira: 'este perfil pode
    arquivar?'. Administrativo de obra não pode, e para aqui."""
    import io as _io
    _dublar_leitura(monkeypatch, {})
    s = cenario["s"]
    obreiro = Usuario(nome="Administrativo de obra", email="ob.doc@teste.local",
                      ativo=True, senha_hash=gerar_hash("senha-de-teste-123"),
                      perfil=P.ADMINISTRATIVO_OBRA)
    s.add(obreiro)
    s.flush()
    r = como(app_real, obreiro.id).post(
        f"/erp/api/obras/{cenario['obra'].id}/documento",
        data={"arquivo": (_io.BytesIO(PDF), "cno.pdf"), "tipo": "MATRICULA-CEI-CNO",
              "campos": json.dumps({"cno": "90.025.25410/76"})},
        content_type="multipart/form-data")
    assert r.status_code == 403


def test_quem_arquiva_por_excecao_nao_escreve_em_obra_que_nao_e_dele(app_real, cenario,
                                                                     monkeypatch):
    """A segunda trava: 'pode NESTE registro?'.

    O caso existe de verdade desde a migração 032: um supervisor pode receber
    `arquivar` por marcação individual no cadastro. Aí a ação passa — e o que
    tem de segurá-lo é o escopo por obra. Fora do escopo responde 404, nunca
    403: dizer "sem permissão" confirmaria que aquela obra existe.
    """
    import io as _io
    from sqlalchemy import text as _text
    _dublar_leitura(monkeypatch, {})
    s = cenario["s"]
    outra = Obra(codigo="OUTRA", nome="Obra de outro", fase="EM_EXECUCAO")
    supervisor = Usuario(nome="Supervisor de obra", email="sup.doc@teste.local",
                         ativo=True, senha_hash=gerar_hash("senha-de-teste-123"),
                         perfil=P.SUPERVISOR_OBRA)
    s.add_all([outra, supervisor])
    s.flush()
    s.add(UsuarioObra(usuario_id=supervisor.id, obra_id=outra.id))
    s.execute(_text("INSERT INTO usuario_permissoes (usuario_id, acao, concedida) "
                    "VALUES (:u, 'arquivar', true)"), {"u": supervisor.id})
    s.flush()

    # na obra que é dele, passa
    ok = como(app_real, supervisor.id).post(
        f"/erp/api/obras/{outra.id}/documento",
        data={"arquivo": (_io.BytesIO(PDF), "cno.pdf"), "tipo": "MATRICULA-CEI-CNO",
              "campos": json.dumps({"cno": "90.025.25410/76"})},
        content_type="multipart/form-data")
    assert ok.status_code == 200, ok.get_data(as_text=True)

    # na obra que não é, responde "não encontrado"
    r = como(app_real, supervisor.id).post(
        f"/erp/api/obras/{cenario['obra'].id}/documento",
        data={"arquivo": (_io.BytesIO(PDF), "cno.pdf"), "tipo": "MATRICULA-CEI-CNO",
              "campos": json.dumps({"cno": "90.025.25410/76"})},
        content_type="multipart/form-data")
    assert r.status_code == 404, "fora do escopo é 404, nunca 403"


def test_a_previa_do_nome_e_a_mesma_que_vai_ser_guardada(app_real, cenario, monkeypatch):
    """Prometer um nome e entregar outro é pequeno, mas é o tipo de detalhe
    que faz a pessoa desconfiar do resto."""
    import io as _io
    _dublar_leitura(monkeypatch, {
        "tipo_codigo": "MATRICULA-CEI-CNO", "dono_especie": "OBRA",
        # de propósito: a leitura devolve o NOME da obra, e o padrão usa o CÓDIGO
        "dono_nome": "Escola Planalto", "emissao": "2026-03-10",
        "confianca": "ALTA", "dados_extraidos": {"cno": "90.025.25410/76"}})
    c = como(app_real, cenario["admin"].id)
    lido = c.post(f"/erp/api/obras/{cenario['obra'].id}/documento/ler",
                  data={"arquivo": (_io.BytesIO(PDF), "matricula.pdf")},
                  content_type="multipart/form-data").get_json()
    previa = lido["sugestao"]["nome_sugerido"]

    guardado = c.post(
        f"/erp/api/obras/{cenario['obra'].id}/documento",
        data={"arquivo": (_io.BytesIO(PDF), "matricula.pdf"),
              "tipo": "MATRICULA-CEI-CNO", "emissao": "2026-03-10",
              "campos": json.dumps({})},
        content_type="multipart/form-data").get_json()["documento"]["nome"]
    assert previa == guardado == "MATRICULA-CEI-CNO_ESCPLANALTO_2026-03-10.pdf"
