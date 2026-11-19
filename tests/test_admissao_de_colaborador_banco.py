"""ADMISSÃO DE COLABORADOR — com banco de verdade (19/11/2026).

Pedido dele: *"Lá no portal, quando eu vou cadastrar o colaborador, eu quero
mudar o termo. Eu quero trocar por admissão de colaborador (…) A gente precisa
atender no cadastro as exigências do E-Social e os dados mínimos para o
SomaPay"* — nome, nome da mãe, nascimento, CPF; RG, emissão, órgão, estado;
data de início, telefone, tipo de contrato; o endereço; e a matrícula = CPF.

E sobre quem já teve cadastro: *"se for um colaborador desligado, nós já temos
os dados dele (…) eu queria poder pesquisar o nome da pessoa (…) e só
selecionar sem ter a necessidade de incluir de novos documentos."*

Cobre também o defeito achado no caminho: os documentos anexados no portal
eram LIDOS e nunca GUARDADOS.
"""
from __future__ import annotations

import pytest
from sqlalchemy import text

from app.apps.erp.core import documentos_de_pessoa as svc_doc
from app.apps.erp.core import pessoal_solicitacoes as svc
from app.apps.erp.core.cadastros import pessoas as svc_pessoas
from app.apps.erp.core.comum.auditoria import ErroValidacao

CPF_NOVO = "39053344705"
CPF_VOLTA = "52998224725"
PESSOA = {
    "nome_mae": "Maria Da Silva", "data_nascimento": "1990-03-15",
    "rg": "2001234567", "rg_emissao": "2010-05-20", "rg_orgao": "SSP",
    "rg_uf": "CE", "cep": "60000000", "logradouro": "Rua Um",
    "numero_endereco": "10", "bairro": "Centro", "municipio": "Fortaleza",
    "uf": "CE"}


# ---------------------------------------------------------------------------
# Sem banco: a regra
# ---------------------------------------------------------------------------
def test_o_nome_do_pedido_e_admissao_de_colaborador():
    assert svc.TIPOS["CADASTRO"]["nome"] == "Admissão de Colaborador"


def test_os_dados_do_esocial_e_do_somapay_sao_obrigatorios():
    obrig = set(svc.obrigatorios_do_tipo("CADASTRO"))
    for c in ("nome", "cpf", "nome_mae", "data_nascimento", "rg", "rg_emissao",
              "rg_orgao", "rg_uf", "data_inicio", "telefone", "tipo_contrato",
              "cep", "logradouro", "numero_endereco", "bairro", "municipio",
              "uf"):
        assert c in obrig, f"{c} tem de ser obrigatório na admissão"
    assert "complemento" not in obrig, "muita casa não tem complemento"


def test_a_leitura_so_aceita_dado_com_forma_de_dado():
    """CPF com 11 dígitos, data de verdade, UF com duas letras, CEP com oito.
    O resto vira pergunta — preferir vazio a chutar."""
    c = svc_doc.normalizar_campos({
        "cpf": "390.533.447-05", "data_nascimento": "15/03/1990",
        "rg_emissao": "ontem", "rg_uf": "ceará", "uf": "ce", "cep": "60000-000",
        "rg": "20.012.345-67", "nome_mae": "  Maria   Da Silva "})
    assert c["cpf"] == CPF_NOVO and c["data_nascimento"] == "1990-03-15"
    assert "rg_emissao" not in c and "rg_uf" not in c
    assert c["uf"] == "CE" and c["cep"] == "60000000"
    assert c["rg"] == "2001234567"
    # caixa alta e sem acento (decisão dele, 19/11/2026)
    assert c["nome_mae"] == "MARIA DA SILVA"


def test_a_busca_de_pessoa_nao_traz_o_dado_pessoal_so_a_marca():
    """O RG, a mãe e o endereço de alguém não descem para o celular de quem
    procurou um nome."""
    for c in ("rg", "nome_mae", "logradouro", "data_nascimento"):
        assert c in svc.SO_A_MARCA


# ---------------------------------------------------------------------------
# Com banco
# ---------------------------------------------------------------------------
@pytest.fixture
def mundo(sessao_real):
    from app.apps.erp.core.auth.service import gerar_hash
    from app.apps.erp.db.models.cadastros import (
        Obra, PerfilUsuario as P, Usuario,
    )

    def gente(apelido, perfil):
        u = Usuario(nome=apelido, email=f"{apelido}@adm.teste", ativo=True,
                    senha_hash=gerar_hash("senha-de-teste-123"), perfil=perfil,
                    telefone="5585900000000")
        sessao_real.add(u)
        return u

    dp = gente("dp-adm", P.DEPARTAMENTO_PESSOAL)
    obra_user = gente("obra-adm", P.SUPERVISOR_OBRA)
    o = Obra(codigo="ADM-01", nome="Obra da admissão", status="ATIVA")
    sessao_real.add(o)
    sessao_real.flush()
    cargo_id = sessao_real.execute(text(
        "INSERT INTO cargos (nome, ativo) VALUES ('Servente adm', TRUE) "
        "RETURNING id")).scalar()
    empresa_id = sessao_real.execute(text("""
        INSERT INTO empresas (razao_social, nome_fantasia, cnpj, ativo)
        VALUES ('BWS ADM LTDA', 'ADM', '00.079.526/0001-09', TRUE)
        RETURNING id""")).scalar()
    # quem já trabalhou e saiu, com o cadastro antigo pela metade (sem RG)
    volta = svc_pessoas.salvar(sessao_real, {
        "cpf": CPF_VOLTA, "nome": "Jose Que Volta", "admissao": "2024-01-05",
        "obra_id": o.id, "vinculo": "PRESTADOR", "matricula": "M-ANTIGA",
        "nome_mae": "Ana Antiga", "data_nascimento": "1985-02-02",
        "cep": "60000000", "logradouro": "Rua Velha", "numero_endereco": "5",
        "bairro": "Antigo", "municipio": "Fortaleza", "uf": "CE"}, dp)
    svc_pessoas.desligar(sessao_real, volta.id, {"data_saida": "2025-01-10"}, dp)
    sessao_real.flush()
    return {"s": sessao_real, "dp": dp, "obra_user": obra_user, "obra": o,
            "cargo_id": cargo_id, "empresa_id": empresa_id, "volta": volta}


def _admissao(mundo, **extra):
    base = {"tipo": "CADASTRO", "nome": "Gente Nova", "cpf": CPF_NOVO,
            "obra_id": mundo["obra"].id,
            "tipo_documentacao": "Prestador de Serviço",
            "tipo_contrato": "Autônomo (RPA)", "pagamento": "Diarista",
            "valor_diaria": "120,00",
            "cargo_pleiteado_id": mundo["cargo_id"],
            "empresa_id": mundo["empresa_id"],
            "telefone": "85988881111", "pix_tipo": "CPF", "pix_chave": CPF_NOVO,
            "categoria_alimentacao": "NAO_RECEBE",
            "categoria_transporte": "NAO_RECEBE",
            "data_inicio": "2026-11-20", **PESSOA,
            "documentos": ["DOC-IDENTIDADE", "CPF-DOC",
                           "COMPROVANTE-ENDERECO-PESSOA"]}
    base.update(extra)
    return base


pytestmark_banco = pytest.mark.banco


@pytestmark_banco
def test_sem_os_dados_do_somapay_o_pedido_nao_sai(mundo):
    with pytest.raises(ErroValidacao) as e:
        svc.abrir(mundo["s"], _admissao(mundo, rg="", rg_orgao=""),
                  mundo["obra_user"])
    assert "Nº do RG" in str(e.value) and "Órgão Emissor" in str(e.value)


@pytestmark_banco
def test_a_matricula_e_o_cpf_e_os_papeis_passam_para_a_pessoa(mundo):
    s = mundo["s"]
    p = svc.abrir(s, _admissao(mundo), mundo["obra_user"])
    guardados = svc.guardar_papeis(s, p["id"], None, [
        {"conteudo": b"%PDF-1.4 rg frente" * 50, "nome": "rg-frente.jpg",
         "tipo": "DOC-IDENTIDADE"},
        {"conteudo": b"%PDF-1.4 rg verso" * 50, "nome": "rg-verso.jpg",
         "tipo": "DOC-IDENTIDADE"},
        {"conteudo": b"%PDF-1.4 conta de luz" * 50, "nome": "luz.pdf",
         "tipo": "COMPROVANTE-ENDERECO-PESSOA"}])
    assert guardados == 3
    r = svc.atender(s, p["id"], {}, mundo["dp"])
    s.flush()
    c = s.get(svc_pessoas.Colaborador, r["colaborador_id"])
    assert c.matricula == CPF_NOVO
    assert c.rg == "2001234567" and c.rg_orgao == "SSP" and c.rg_uf == "CE"
    assert c.nome_mae == "MARIA DA SILVA", "caixa alta, sem acento"
    papeis = s.execute(text("""
        SELECT tipo_codigo FROM documentos WHERE colaborador_id = :c
         ORDER BY id"""), {"c": c.id}).scalars().all()
    assert papeis.count("DOC-IDENTIDADE") == 2, "frente e verso, os dois"
    assert "COMPROVANTE-ENDERECO-PESSOA" in papeis
    orfaos = s.execute(text("SELECT count(*) FROM documentos "
                            " WHERE dp_solicitacao_id = :s"),
                       {"s": p["id"]}).scalar()
    assert orfaos == 0, "o papel nunca fica órfão"


@pytestmark_banco
def test_o_desligado_escolhido_volta_sem_papel_e_so_completa_o_que_falta(mundo):
    """Escolhido pela busca: sem CPF digitado, sem documento. O cadastro antigo
    já tem a mãe, o nascimento e o endereço; falta o RG — e só ele é pedido."""
    s = mundo["s"]
    volta = mundo["volta"]
    sem_cpf = _admissao(mundo, cpf="", nome="", documentos=[])
    for c in PESSOA:
        sem_cpf.pop(c)
    sem_cpf["colaborador_id"] = volta.id
    with pytest.raises(ErroValidacao) as e:
        svc.abrir(s, sem_cpf, mundo["obra_user"])
    msg = str(e.value)
    assert "Nº do RG" in msg, "o que falta no cadastro antigo é pedido"
    assert "Nome da Mãe" not in msg and "Rua" not in msg, \
        "o que o cadastro já tem não se pede de novo"
    assert "anexar" not in msg.lower(), "readmissão não exige papel"

    p = svc.abrir(s, {**sem_cpf, "rg": "9988776655", "rg_emissao": "2015-01-01",
                      "rg_orgao": "SSP", "rg_uf": "CE"}, mundo["obra_user"])
    assert p["readmissao"] and p["colaborador_id"] == volta.id
    svc.atender(s, p["id"], {}, mundo["dp"])
    s.flush()
    c = s.get(svc_pessoas.Colaborador, volta.id)
    assert c.situacao == "ATIVO"
    assert c.rg == "9988776655", "o dado novo atualiza o cadastro"
    assert c.nome_mae == "Ana Antiga", "o que não veio fica como estava"
    assert c.matricula == CPF_VOLTA, "quem entra de novo ganha matrícula = CPF"
    vinculos = s.execute(text("SELECT count(*) FROM colaborador_vinculos "
                              " WHERE colaborador_id = :c"), {"c": c.id}).scalar()
    assert vinculos >= 2, "o histórico da primeira passagem fica"


@pytestmark_banco
def test_a_busca_acha_o_desligado_so_quando_pedida_e_sem_o_dado_pessoal(mundo,
                                                                         monkeypatch):
    from app.apps.erp.core import portal as svc_portal
    eu = {"colaborador_id": None, "nome": "obra", "usuario_id": None}
    monkeypatch.setattr(svc_portal, "quem_esta", lambda s, u: eu)
    monkeypatch.setattr(svc_portal, "pode_pedir_por_outros", lambda s, e: True)
    monkeypatch.setattr(svc_portal, "obras_que_alcanca", lambda s, e: [])
    ativos = svc_portal.procurar_pessoas(mundo["s"], None, "Jose Que")
    assert ativos == [], "a busca comum não traz quem saiu"
    achados = svc_portal.procurar_pessoas(mundo["s"], None, "Jose Que",
                                          desligados=True)
    assert [a["id"] for a in achados] == [mundo["volta"].id]
    cadastro = achados[0]["cadastro"]
    assert cadastro.get("nome_mae") == svc.MARCA_TEM, "só a marca, nunca o valor"
    assert "Ana" not in str(achados[0])


@pytestmark_banco
def test_o_que_a_leitura_achou_alem_do_formulario_chega_a_ficha(mundo):
    """Ele: *"outros dados que a leitura da IA conseguir coletar, mesmo que não
    sejam obrigatórios, inclusive o PIS, nome do pai (…) pode preencher."*"""
    s = mundo["s"]
    p = svc.abrir(s, _admissao(mundo, telefone="(85) 98888-1111",
                               lidos_do_documento={
                                   "nome_pai": "José Ávila", "sexo": "M",
                                   "estado_civil": "casado",
                                   "naturalidade": "Crato",
                                   "naturalidade_uf": "ce"}),
                  mundo["obra_user"])
    c = s.get(svc_pessoas.Colaborador, p["colaborador_id"])
    assert c.nome_pai == "JOSE AVILA" and c.sexo == "M"
    assert c.estado_civil == "CASADO" and c.naturalidade_uf == "CE"
    assert c.telefone == "85988881111", "o número, sem a máscara da tela"


def test_prestador_esconde_ctps_e_pis():
    exige = svc.exigencias("CADASTRO",
                           {"tipo_documentacao": "Prestador de Serviço"})["campos"]
    assert "num_ctps" not in exige and "num_pis" not in exige
    ocultos = svc.TIPOS["CADASTRO"]["ocultos_se"]["tipo_documentacao"]["PRESTADOR"]
    assert "num_ctps" in ocultos and "num_pis" in ocultos
