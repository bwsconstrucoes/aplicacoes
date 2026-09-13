"""O documento da pessoa arquiva E preenche o cadastro — com banco de verdade.

O mesmo princípio da obra, do lado das pessoas, como o dono pediu na mesma
mensagem de 10/09/2026: *"deveremos seguir pra parte de colaboradores"*.

A diferença que manda neste arquivo: aqui o erro caro não é preencher campo
errado — é preencher o cadastro da PESSOA ERRADA. Vai parar em holerite, em
pagamento e em histórico de alguém que não é.

O que se prova:

  1. O CPF do documento é CONFERIDO contra o do cadastro, e NUNCA gravado.
     Ele é a identidade: trocá-lo repontaria pagamento e histórico.
  2. CPF que não bate faz a tela gritar e NADA entra marcado — a dúvida passa
     a ser de quem é o documento, não de um campo.
  3. Cada tipo só preenche o que prova. ASO e certificado de NR não alimentam
     cadastro nenhum: eles valem pela validade, que já vira aviso na agenda.
  4. Função só entra se estiver cadastrada. Criar função a partir de leitura
     multiplicaria "PEDREIRO", "Pedreiro" e "Pedreiro(a)" em um mês — e a
     diária de referência, que mora na função, viraria três.
  5. Termo de rescisão que traz demissão fecha a situação junto: cadastro com
     data de demissão e situação ATIVO mente para quem monta a folha.
  6. Arquivar e preencher acontecem na mesma transação, e o escopo por obra
     continua valendo.
"""
from __future__ import annotations

import io as _io
import json
from datetime import date

import pytest

from app.apps.erp.core.arquivo import catalogo, preenchimento
from app.apps.erp.core.auth.service import gerar_hash
from app.apps.erp.core.comum.auditoria import ErroValidacao
from app.apps.erp.db.models.cadastros import (Colaborador, Funcao, Obra,
                                              PerfilUsuario as P, Usuario,
                                              UsuarioObra)
from app.apps.erp.db.models.financeiro import Documento
from tests.conftest import como

pytestmark = pytest.mark.banco

PDF = b"%PDF-1.4 documento da pessoa"
CPF = "52998224725"


@pytest.fixture
def cenario(sessao_real):
    s = sessao_real
    catalogo.aplicar(s)
    admin = Usuario(nome="Admin do pessoal", email="pes.doc@teste.local", ativo=True,
                    senha_hash=gerar_hash("senha-de-teste-123"), perfil=P.ADMIN)
    obra = Obra(codigo="ESCPLANALTO", nome="Escola Planalto", fase="EM_EXECUCAO")
    pedreiro = Funcao(nome="Pedreiro", ativo=True)
    s.add_all([admin, obra, pedreiro])
    s.flush()
    c = Colaborador(nome="João da Silva", cpf=CPF, obra_id=obra.id, situacao="ATIVO")
    s.add(c)
    s.flush()
    return {"s": s, "admin": admin, "obra": obra, "colaborador": c,
            "funcao": pedreiro}


def _leitura(tipo, extraidos=None):
    return {"tipo_codigo": tipo, "tipo_nome": tipo,
            "dados_extraidos": extraidos or {}}


# ---------------------------------------------------------------------------
# 1 e 2. O CPF CONFERE, MAS NÃO É GRAVADO
# ---------------------------------------------------------------------------
def test_o_cpf_do_documento_e_conferido_e_bate(cenario):
    r = preenchimento.sugerir_para_colaborador(
        cenario["s"], cenario["colaborador"].id, "FICHA-REGISTRO",
        _leitura("FICHA-REGISTRO", {"cpf": "529.982.247-25", "matricula": "1042"}))
    assert r["cpf_confere"] is True
    assert r["avisos"] == []
    assert next(c for c in r["campos"] if c["campo"] == "matricula")["marcar"] is True


def test_cpf_de_outra_pessoa_faz_a_tela_gritar_e_nada_entra_marcado(cenario):
    """Documento da pessoa errada preenchendo cadastro vai parar em holerite."""
    r = preenchimento.sugerir_para_colaborador(
        cenario["s"], cenario["colaborador"].id, "FICHA-REGISTRO",
        _leitura("FICHA-REGISTRO", {"cpf": "11144477735", "matricula": "9999",
                                    "telefone": "81999990000"}))
    assert r["cpf_confere"] is False
    assert any("não é o de João da Silva" in a for a in r["avisos"])
    assert all(c["marcar"] is False for c in r["campos"]), \
        "com a identidade em dúvida, nada entra marcado"


def test_o_cpf_nunca_e_gravado_por_documento(cenario):
    with pytest.raises(ErroValidacao) as e:
        preenchimento.aplicar_no_colaborador(
            cenario["s"], cenario["colaborador"].id, {"cpf": "11144477735"},
            tipo_codigo="FICHA-REGISTRO", usuario=cenario["admin"])
    assert "identidade" in str(e.value)
    assert cenario["s"].get(Colaborador, cenario["colaborador"].id).cpf == CPF


# ---------------------------------------------------------------------------
# 3. CADA TIPO SÓ PREENCHE O QUE PROVA
# ---------------------------------------------------------------------------
def test_o_rg_so_preenche_o_nome(cenario):
    r = preenchimento.sugerir_para_colaborador(
        cenario["s"], cenario["colaborador"].id, "DOC-IDENTIDADE",
        _leitura("DOC-IDENTIDADE", {"nome": "JOAO DA SILVA PEREIRA",
                                    "matricula": "1042", "admissao": "2026-01-05"}))
    campos = {c["campo"] for c in r["campos"]}
    assert campos == {"nome"}, "RG não prova matrícula nem admissão"


def test_o_aso_nao_alimenta_cadastro(cenario):
    """Ele vale pela VALIDADE, que já vira aviso na agenda."""
    r = preenchimento.sugerir_para_colaborador(
        cenario["s"], cenario["colaborador"].id, "ASO",
        _leitura("ASO", {"nome": "OUTRO NOME", "admissao": "2020-01-01"}))
    assert r["campos"] == []
    assert r["tipo_nao_preenche"] is False, "ASO é conhecido — só não preenche"


def test_gravar_campo_fora_da_lista_do_tipo_e_recusado(cenario):
    with pytest.raises(ErroValidacao) as e:
        preenchimento.aplicar_no_colaborador(
            cenario["s"], cenario["colaborador"].id, {"admissao": "2026-01-05"},
            tipo_codigo="DOC-IDENTIDADE", usuario=cenario["admin"])
    assert "não preenche" in str(e.value)


def test_a_ficha_de_registro_preenche_o_conjunto(cenario):
    s = cenario["s"]
    r = preenchimento.aplicar_no_colaborador(
        s, cenario["colaborador"].id,
        {"matricula": "1042", "admissao": "05/01/2026", "telefone": "(81) 99999-0000",
         "regime": "clt", "banco": "237", "agencia": "1234", "conta": "56789-0"},
        tipo_codigo="FICHA-REGISTRO", usuario=cenario["admin"])
    c = s.get(Colaborador, cenario["colaborador"].id)
    assert c.matricula == "1042"
    assert c.admissao == date(2026, 1, 5)
    assert c.telefone == "81999990000", "telefone entra só com dígitos"
    assert c.regime == "CLT"
    # Seis e não sete: o regime já era CLT (é o padrão do cadastro), e o que
    # não muda não vira mudança — nem na contagem, nem na trilha.
    assert r["quantidade"] == 6
    assert not any("Regime" in m for m in r["mudancas"])


# ---------------------------------------------------------------------------
# 4. A FUNÇÃO SÓ ENTRA SE EXISTIR
# ---------------------------------------------------------------------------
def test_funcao_cadastrada_entra_pelo_nome_sem_acento_nem_caixa(cenario):
    r = preenchimento.sugerir_para_colaborador(
        cenario["s"], cenario["colaborador"].id, "CTPS",
        _leitura("CTPS", {"funcao_nome": "PEDREIRO"}))
    campo = next(c for c in r["campos"] if c["campo"] == "funcao_id")
    assert campo["valor"] == "Pedreiro"
    assert campo["valor_id"] == cenario["funcao"].id


def test_funcao_que_nao_existe_nao_e_criada_sozinha(cenario):
    """Criar função a partir de leitura multiplicaria 'PEDREIRO', 'Pedreiro' e
    'Pedreiro(a)' em um mês — e a diária de referência viraria três."""
    s = cenario["s"]
    antes = s.query(Funcao).count()
    r = preenchimento.sugerir_para_colaborador(
        s, cenario["colaborador"].id, "CTPS",
        _leitura("CTPS", {"funcao_nome": "Servente de obras"}))
    assert not any(c["campo"] == "funcao_id" for c in r["campos"])
    assert any("não está cadastrada" in a for a in r["avisos"])
    assert s.query(Funcao).count() == antes


def test_funcao_inexistente_na_gravacao_e_recusada(cenario):
    with pytest.raises(ErroValidacao):
        preenchimento.aplicar_no_colaborador(
            cenario["s"], cenario["colaborador"].id, {"funcao_id": "99999"},
            tipo_codigo="CTPS", usuario=cenario["admin"])


# ---------------------------------------------------------------------------
# 5. A RESCISÃO FECHA A SITUAÇÃO
# ---------------------------------------------------------------------------
def test_demissao_lida_desliga_o_colaborador(cenario):
    """Cadastro com data de demissão e situação ATIVO mente para quem monta a
    folha do mês seguinte."""
    s = cenario["s"]
    r = preenchimento.aplicar_no_colaborador(
        s, cenario["colaborador"].id, {"demissao": "2026-08-31"},
        tipo_codigo="RESCISAO", usuario=cenario["admin"])
    c = s.get(Colaborador, cenario["colaborador"].id)
    assert c.demissao == date(2026, 8, 31)
    assert c.situacao == "DESLIGADO"
    assert any("Desligado" in m for m in r["mudancas"])


def test_o_que_mudou_fica_na_trilha(cenario):
    from app.apps.erp.db.models.financeiro import Evento
    s = cenario["s"]
    preenchimento.aplicar_no_colaborador(
        s, cenario["colaborador"].id, {"matricula": "1042"},
        tipo_codigo="FICHA-REGISTRO", usuario=cenario["admin"])
    ev = s.query(Evento).filter(Evento.entidade_tipo == "colaborador",
                                Evento.acao == "PREENCHIDO_POR_DOCUMENTO").all()
    assert len(ev) == 1
    assert "Matrícula" in " ".join(ev[0].detalhe["mudancas"])


# ---------------------------------------------------------------------------
# 6. AS ROTAS
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


def test_a_leitura_pergunta_pelos_campos_da_pessoa(app_real, cenario, monkeypatch):
    falso = _dublar_leitura(monkeypatch, {
        "tipo_codigo": "FICHA-REGISTRO", "dono_especie": "PESSOA",
        "dono_nome": "João da Silva", "confianca": "ALTA",
        "dados_extraidos": {"cpf": CPF, "matricula": "1042"}})
    r = como(app_real, cenario["admin"].id).post(
        f"/erp/api/colaboradores/{cenario['colaborador'].id}/documento/ler",
        data={"arquivo": (_io.BytesIO(PDF), "ficha.pdf")},
        content_type="multipart/form-data")
    assert r.status_code == 200
    d = r.get_json()
    assert "cpf" in falso.instrucao and "dados_extraidos" in falso.instrucao
    assert d["cadastro"]["cpf_confere"] is True
    assert cenario["s"].query(Documento).count() == 0, "ler não pode gravar"


def test_arquivar_e_preencher_acontecem_juntos(app_real, cenario, monkeypatch):
    _dublar_leitura(monkeypatch, {})
    s = cenario["s"]
    r = como(app_real, cenario["admin"].id).post(
        f"/erp/api/colaboradores/{cenario['colaborador'].id}/documento",
        data={"arquivo": (_io.BytesIO(PDF), "ficha.pdf"),
              "tipo": "FICHA-REGISTRO", "emissao": "2026-01-05",
              "campos": json.dumps({"matricula": "1042"})},
        content_type="multipart/form-data")
    assert r.status_code == 200, r.get_data(as_text=True)
    d = r.get_json()
    assert d["preenchido"]["quantidade"] == 1
    assert s.get(Colaborador, cenario["colaborador"].id).matricula == "1042"
    assert d["documento"]["nome"].startswith("FICHA-REGISTRO_JOAO-DA-SILVA")


def test_campo_recusado_desfaz_o_arquivamento_junto(app_real, cenario, monkeypatch):
    _dublar_leitura(monkeypatch, {})
    s = cenario["s"]
    r = como(app_real, cenario["admin"].id).post(
        f"/erp/api/colaboradores/{cenario['colaborador'].id}/documento",
        data={"arquivo": (_io.BytesIO(PDF), "rg.pdf"), "tipo": "DOC-IDENTIDADE",
              "campos": json.dumps({"admissao": "2026-01-05"})},
        content_type="multipart/form-data")
    assert r.status_code == 400
    s.rollback()
    assert s.query(Documento).count() == 0


def test_colaborador_de_obra_alheia_responde_nao_encontrado(app_real, cenario,
                                                            monkeypatch):
    from sqlalchemy import text as _text
    _dublar_leitura(monkeypatch, {})
    s = cenario["s"]
    outra = Obra(codigo="OUTRA", nome="Obra de outro", fase="EM_EXECUCAO")
    supervisor = Usuario(nome="Supervisor", email="sup.pes@teste.local", ativo=True,
                         senha_hash=gerar_hash("senha-de-teste-123"),
                         perfil=P.SUPERVISOR_OBRA)
    s.add_all([outra, supervisor])
    s.flush()
    s.add(UsuarioObra(usuario_id=supervisor.id, obra_id=outra.id))
    s.execute(_text("INSERT INTO usuario_permissoes (usuario_id, acao, concedida) "
                    "VALUES (:u, 'arquivar', true)"), {"u": supervisor.id})
    s.flush()

    r = como(app_real, supervisor.id).post(
        f"/erp/api/colaboradores/{cenario['colaborador'].id}/documento",
        data={"arquivo": (_io.BytesIO(PDF), "ficha.pdf"), "tipo": "FICHA-REGISTRO",
              "campos": json.dumps({"matricula": "1042"})},
        content_type="multipart/form-data")
    assert r.status_code == 404, "fora do escopo é 404, nunca 403"


def test_a_area_so_aparece_para_quem_ve_documento_pessoal(app_real, cenario):
    """Documento de pessoa tem sigilo PESSOAL. Oferecer 'arquive o ASO' a quem
    não vai conseguir abri-lo depois é convite à confusão."""
    s = cenario["s"]
    financeiro = Usuario(nome="Financeiro", email="fin.pes@teste.local", ativo=True,
                         senha_hash=gerar_hash("senha-de-teste-123"),
                         perfil=P.FINANCEIRO)
    s.add(financeiro)
    s.flush()

    do_admin = como(app_real, cenario["admin"].id).get(
        "/erp/colaboradores").get_data(as_text=True)
    do_fin = como(app_real, financeiro.id).get(
        "/erp/colaboradores").get_data(as_text=True)
    assert "const PODE_ARQUIVAR = true" in do_admin
    assert "const PODE_ARQUIVAR = false" in do_fin, \
        "o financeiro arquiva, mas não enxerga documento PESSOAL"
