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
    assert c.matricula == "M-ANTIGA", "a matrícula que já existia fica"
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


@pytestmark_banco
def test_o_pedido_aparece_em_linguagem_de_gente(mundo):
    """Ele: *"cargo hífen pleiteado hífen ID, aí está a numeração de sistema
    (…) não recebe todo maiúsculo com hífen (…) data no padrão americano."*"""
    s = mundo["s"]
    p = svc.abrir(s, _admissao(mundo), mundo["obra_user"])
    leg = {x["campo"]: (x["rotulo"], x["valor"]) for x in p["dados_legiveis"]}
    assert leg["cargo_pleiteado_id"][1] == "Servente adm"
    assert leg["categoria_alimentacao"][1] == "Não recebe"
    assert leg["data_inicio"][1] == "20/11/2026"
    assert leg["cep"][1] == "60000-000"
    assert leg["empresa_id"][1] == "ADM"
    assert "pre_cadastro" not in leg, "marca interna não aparece"


@pytestmark_banco
def test_o_dp_corrige_o_pedido_e_o_pre_cadastro_junto(mundo):
    s = mundo["s"]
    p = svc.abrir(s, _admissao(mundo), mundo["obra_user"])
    r = svc.corrigir(s, p["id"], {"dados": {"nome_mae": "Mãe Corrigida",
                                            "rg": "1112223334"}}, mundo["dp"])
    assert r["dados"]["nome_mae"] == "MAE CORRIGIDA"
    c = s.get(svc_pessoas.Colaborador, p["colaborador_id"])
    assert c.nome_mae == "MAE CORRIGIDA" and c.rg == "1112223334"
    assert any(e["acao"] == "CORRIGIDO_PELO_DP" for e in r["eventos"])


@pytestmark_banco
def test_readmissao_mantem_a_matricula_que_ja_existia(mundo):
    """Ele: *"se for uma readmissão e o campo estiver em branco, bota o CPF.
    Se (…) o campo já existia, deixa o que já estava."*"""
    s = mundo["s"]
    volta = mundo["volta"]
    pedido = _admissao(mundo, cpf=CPF_VOLTA, nome="Jose Que Volta",
                       documentos=[])
    p = svc.abrir(s, pedido, mundo["obra_user"])
    svc.atender(s, p["id"], {}, mundo["dp"])
    s.flush()
    c = s.get(svc_pessoas.Colaborador, volta.id)
    assert c.matricula == "M-ANTIGA"
    assert c.etapa == "CONCLUIDO", "prestador readmitido volta direto a Concluído"
    assert c.demissao is None, "a saída anterior vai para o histórico"


@pytestmark_banco
def test_a_fila_do_dp_individualiza_a_pessoa_e_diz_quem_pediu(mundo, monkeypatch):
    """Ele: *"o ideal é que tenha o nome, a obra, CPF e o cargo"* — e a coluna
    de quem pediu vinha vazia para o DP."""
    s = mundo["s"]
    p = svc.abrir(s, _admissao(mundo), mundo["obra_user"])
    fila = svc.listar(s)["solicitacoes"]
    meu = next(x for x in fila if x["id"] == p["id"])
    assert meu["pessoa"] == "GENTE NOVA"
    assert meu["pessoa_cpf"] == "390.533.447-05"
    assert meu["cargo"] == "Servente adm"
    assert meu["solicitante"] == "obra-adm"
    leg = {x["campo"]: x["valor"] for x in p["dados_legiveis"]}
    assert leg["valor_diaria"] == "R$ 120,00"


# ---------------------------------------------------------------------------
# A admissão caminha dentro da solicitação (19/11/2026)
# ---------------------------------------------------------------------------
@pytestmark_banco
def test_admissao_de_autonomo_conclui_e_fecha_no_atendimento(mundo):
    s = mundo["s"]
    p = svc.abrir(s, _admissao(mundo), mundo["obra_user"])
    r = svc.atender(s, p["id"], {}, mundo["dp"])
    s.flush()
    assert r["situacao"] == "ATENDIDA"
    assert s.get(svc_pessoas.Colaborador, p["colaborador_id"]).etapa == "CONCLUIDO"


@pytestmark_banco
def test_admissao_de_pj_espera_o_contrato_anexado(mundo):
    s = mundo["s"]
    p = svc.abrir(s, _admissao(mundo, tipo_contrato="PJ",
                               razao_social_pj="GENTE NOVA SERVICOS LTDA",
                               cnpj_pj="71000003000106"), mundo["obra_user"])
    r = svc.atender(s, p["id"], {}, mundo["dp"])
    s.flush()
    assert r["situacao"] == "EM_ANDAMENTO", "sem contrato, segue na fila"
    passo = next(x for x in r["processo"]["passos"] if x["codigo"] == "CONTRATO")
    assert passo["estado"] == "PENDENTE"
    c = s.get(svc_pessoas.Colaborador, p["colaborador_id"])
    assert c.etapa == "CADASTRO_INICIAL", "não passa mais por 'Assinar'"
    # anexado o contrato, o DP conclui na ficha e o pedido fecha sozinho
    svc.guardar_papeis(s, p["id"], c.id, [{
        "conteudo": b"%PDF contrato" * 100, "nome": "contrato.pdf",
        "tipo": "CONTRATO-TRABALHO"}])
    svc_pessoas.mover_etapa(s, c.id, "CONCLUIDO", mundo["dp"])
    s.flush()
    assert svc.ler(s, p["id"])["situacao"] == "ATENDIDA"


@pytestmark_banco
def test_admissao_ctps_vai_para_contabilidade_e_fecha_com_o_registro(mundo):
    from app.apps.erp.core import contabilidade as svc_ct
    s = mundo["s"]
    p = svc.abrir(s, _admissao(mundo, tipo_documentacao="CTPS",
                               tipo_contrato="CLT (tempo indeterminado)",
                               num_ctps="1234567",
                               documentos=["DOC-IDENTIDADE", "CPF-DOC",
                                           "COMPROVANTE-ENDERECO-PESSOA",
                                           "CTPS-FOTO", "CTPS-INFORMACOES"]),
                  mundo["obra_user"])
    r = svc.atender(s, p["id"], {"dados": {"salario": "2000"}}, mundo["dp"])
    s.flush()
    assert r["situacao"] == "EM_ANDAMENTO"
    ped = svc_ct.abrir(s, {"template": "ASSINATURA_CTPS",
                           "colaborador_id": p["colaborador_id"],
                           "solicitacao_id": p["id"]}, mundo["dp"])
    c = s.get(svc_pessoas.Colaborador, p["colaborador_id"])
    assert c.etapa == "NA_CONTABILIDADE", "o quadro acompanha o pedido"
    s.execute(text("UPDATE dp_pedidos SET situacao = 'RESPONDIDO' WHERE id = :i"),
              {"i": ped["id"]})
    svc_ct.concluir(s, ped["id"], mundo["dp"])
    s.flush()
    assert c.etapa == "CONCLUIDO"
    assert svc.ler(s, p["id"])["situacao"] == "ATENDIDA", "fechou sozinho"


# ---------------------------------------------------------------------------
# 20/11/2026 — os pedidos que passam pela contabilidade
# ---------------------------------------------------------------------------
def _carteira(mundo, nome="Carteira Ativa", cpf="11144477735"):
    s = mundo["s"]
    c = svc_pessoas.salvar(s, {"cpf": cpf, "nome": nome,
                               "admissao": "2025-01-10", "obra_id": mundo["obra"].id,
                               "vinculo": "CTPS", "salario": "2000",
                               "cargo_id": mundo["cargo_id"],
                               "data_nascimento": "1990-01-01",
                               "empresa_id": mundo["empresa_id"],
                               "etapa": "CONCLUIDO"}, mundo["dp"])
    s.flush()
    return c


def _devolve_e_conclui(mundo, sol_id):
    from app.apps.erp.core import contabilidade as svc_ct
    s = mundo["s"]
    r = svc.ler(s, sol_id)
    passo = next(x for x in r["processo"]["passos"] if x["codigo"] == "CONTABILIDADE")
    if not passo.get("pedido"):
        svc.ler(s, sol_id)
        from app.apps.erp.core import contabil_processo
        contabil_processo.montar_pedido(s, svc.ler(s, sol_id), mundo["dp"])
        passo = next(x for x in svc.ler(s, sol_id)["processo"]["passos"]
                     if x["codigo"] == "CONTABILIDADE")
    s.execute(text("UPDATE dp_pedidos SET situacao = 'RESPONDIDO' WHERE id = :i"),
              {"i": passo["pedido"]["id"]})
    svc_ct.concluir(s, passo["pedido"]["id"], mundo["dp"])
    s.flush()


@pytestmark_banco
def test_ferias_passa_pela_contabilidade_e_fecha_com_o_retorno(mundo):
    s = mundo["s"]
    c = _carteira(mundo)
    p = svc.abrir(s, {"tipo": "FERIAS", "colaborador_id": c.id,
                      "ferias_inicio": "2026-12-15", "ferias_dias": "30",
                      "abono_pecuniario": "0", "adiantamento_13": "1"},
                  mundo["obra_user"])
    r = svc.atender(s, p["id"], {}, mundo["dp"])
    assert r["situacao"] == "EM_ANDAMENTO"
    _devolve_e_conclui(mundo, p["id"])
    _assina_termo(mundo, p["id"])
    assert svc.ler(s, p["id"])["situacao"] == "ATENDIDA"


@pytestmark_banco
def test_alteracao_de_cargo_muda_o_cargo_e_espera_a_contabilidade(mundo):
    s = mundo["s"]
    c = _carteira(mundo)
    novo = s.execute(text("INSERT INTO cargos (nome, ativo) VALUES "
                          "('Pedreiro adm', TRUE) RETURNING id")).scalar()
    p = svc.abrir(s, {"tipo": "ALTERACAO_CARGO", "colaborador_id": c.id,
                      "data_inicio": "2026-12-01", "cargo_pleiteado_id": novo,
                      "justificativa": "promovido na obra"}, mundo["obra_user"])
    r = svc.atender(s, p["id"], {"dados": {"salario": "2500"}}, mundo["dp"])
    s.flush()
    assert s.get(svc_pessoas.Colaborador, c.id).cargo_id == novo, \
        "o cargo pedido tem de chegar ao cadastro"
    assert r["situacao"] == "EM_ANDAMENTO"
    _devolve_e_conclui(mundo, p["id"])
    assert svc.ler(s, p["id"])["situacao"] == "ATENDIDA"


@pytestmark_banco
def test_dependente_sem_salario_familia_fecha_no_atendimento(mundo):
    s = mundo["s"]
    c = _carteira(mundo)
    p = svc.abrir(s, {"tipo": "DEPENDENTE", "colaborador_id": c.id,
                      "nome": "Filho Teste", "parentesco": "FILHO",
                      "data_nascimento": "2020-01-01",
                      "documentos": ["DOC-DEPENDENTE"]}, mundo["obra_user"])
    r = svc.atender(s, p["id"], {}, mundo["dp"])
    assert r["situacao"] == "ATENDIDA"
    p2 = svc.abrir(s, {"tipo": "DEPENDENTE", "colaborador_id": c.id,
                       "nome": "Filha Teste", "parentesco": "FILHO",
                       "data_nascimento": "2021-01-01",
                       "documentos": ["DOC-DEPENDENTE"]}, mundo["obra_user"])
    r2 = svc.atender(s, p2["id"], {"dados": {"salario_familia": "1"}}, mundo["dp"])
    assert r2["situacao"] == "EM_ANDAMENTO", "salário-família passa pela contabilidade"


@pytestmark_banco
def test_cancelar_desligamento_apaga_a_saida_e_cancela_o_processo(mundo):
    import datetime as dt
    s = mundo["s"]
    c = _carteira(mundo)
    d = svc.abrir(s, {"tipo": "DESLIGAMENTO", "colaborador_id": c.id,
                      "forma_desligamento": "Demissão com Aviso Trabalhado",
                      "aviso_previo": "2026-11-01",
                      "continua_prestador": "Não, está saindo da empresa"},
                  mundo["obra_user"])
    saida = (dt.date.today() + dt.timedelta(days=10)).isoformat()
    svc.atender(s, d["id"], {"dados": {"data_saida": saida,
                                       "ultimo_dia": saida}}, mundo["dp"])
    s.flush()
    assert s.get(svc_pessoas.Colaborador, c.id).demissao is not None
    canc = svc.abrir(s, {"tipo": "CANCELAR_DESLIGAMENTO", "colaborador_id": c.id,
                         "observacao": "a obra desistiu"}, mundo["dp"])
    r = svc.atender(s, canc["id"], {}, mundo["dp"])
    s.flush()
    pessoa = s.get(svc_pessoas.Colaborador, c.id)
    assert pessoa.demissao is None and pessoa.ultimo_dia is None
    desl = svc.ler(s, d["id"])
    assert desl["situacao"] == "CANCELADA"
    assert "continua trabalhando" in (desl["resposta"] or ""), \
        "quem pediu o desligamento precisa ler no portal que ele foi cancelado"
    # ⚑ 20/11/2026: de carteira, o cancelamento SEMPRE vai à contabilidade —
    # mesmo sem a rescisão ter sido pedida. Ele: *"esse cancelamento tem que
    # ser avisado à contabilidade, ela tem que estar ciente."*
    assert r["situacao"] == "EM_ANDAMENTO"
    _devolve_e_conclui(mundo, canc["id"])
    assert svc.ler(s, canc["id"])["situacao"] == "ATENDIDA"


@pytestmark_banco
def test_admissao_de_prestador_volta_para_inicial_e_espera_o_registro(mundo):
    s = mundo["s"]
    prest = svc_pessoas.salvar(s, {"cpf": "11144477735", "nome": "Prestador Vira CTPS",
                                   "admissao": "2025-01-10", "obra_id": mundo["obra"].id,
                                   "vinculo": "PRESTADOR", "etapa": "CONCLUIDO",
                                   **PESSOA}, mundo["dp"])
    s.flush()
    p = svc.abrir(s, {"tipo": "ADMISSAO_PRESTADOR", "colaborador_id": prest.id,
                      "num_ctps": "1234567", "salario_familia": "Não",
                      "documentos": ["CTPS-FOTO", "CTPS-INFORMACOES"]},
                  mundo["obra_user"])
    r = svc.atender(s, p["id"], {}, mundo["dp"])
    s.flush()
    c = s.get(svc_pessoas.Colaborador, prest.id)
    assert c.vinculo == "CTPS" and c.etapa == "CADASTRO_INICIAL"
    assert c.ctps_numero == "1234567"
    assert r["situacao"] == "EM_ANDAMENTO"


@pytestmark_banco
def test_o_vigia_avisa_do_registro_da_carteira_perto_do_inicio(mundo):
    """eSocial: o registro de quem entra de carteira sai ANTES do 1º dia."""
    import datetime as dt
    from app.apps.erp.core import agente_dp
    s = mundo["s"]
    amanha = (dt.date.today() + dt.timedelta(days=1)).isoformat()
    p = svc.abrir(s, _admissao(mundo, tipo_documentacao="CTPS",
                               tipo_contrato="CLT (tempo indeterminado)",
                               num_ctps="1234567", data_inicio=amanha,
                               documentos=["DOC-IDENTIDADE", "CPF-DOC",
                                           "COMPROVANTE-ENDERECO-PESSOA",
                                           "CTPS-FOTO", "CTPS-INFORMACOES"]),
                  mundo["obra_user"])
    svc.atender(s, p["id"], {"dados": {"salario": "2000"}}, mundo["dp"])
    s.flush()
    achados = agente_dp.vigia(s)
    assert any(a["tipo"] == "registro_antes_do_inicio"
               and a["colaborador_id"] == p["colaborador_id"] for a in achados)


# ===========================================================================
# A PROGRAMAÇÃO DE FÉRIAS (20/11/2026) — `core/ferias.py`, migração 116
# ===========================================================================
def _assina_termo(mundo, sid):
    """O que a obra faz no celular: o documento aberto da solicitação passa a
    assinado na tela — e o pedido segue como se tivesse vindo do portal."""
    from app.apps.erp.core import termos
    s = mundo["s"]
    t_ = termos.do_pedido(s, sid)
    s.execute(text("UPDATE dp_termos SET situacao = 'ASSINADO', forma = 'NA_TELA' "
                   "WHERE id = :i"), {"i": t_["id"]})
    svc.termo_voltou(s, t_["id"], None)
    s.flush()


def _dia(n):
    import datetime as dt
    return (dt.date.today() + dt.timedelta(days=n)).isoformat()


def _ferias_atendidas(mundo, c, inicio, dias="30"):
    s = mundo["s"]
    p = svc.abrir(s, {"tipo": "FERIAS", "colaborador_id": c.id,
                      "ferias_inicio": inicio, "ferias_dias": dias},
                  mundo["obra_user"])
    svc.atender(s, p["id"], {}, mundo["dp"])
    s.flush()
    return p


@pytestmark_banco
def test_ferias_so_ficam_confirmadas_com_o_retorno_da_contabilidade(mundo):
    from app.apps.erp.core import ferias as svc_ferias
    s = mundo["s"]
    c = _carteira(mundo)
    p = _ferias_atendidas(mundo, c, _dia(40))
    [f] = svc_ferias.listar(s, colaborador_id=c.id)
    assert f["situacao"] == "PEDIDA" and f["dias"] == 30
    assert f["fim"] == _dia(69) and f["retorno"] == _dia(70)
    _devolve_e_conclui(mundo, p["id"])
    [f] = svc_ferias.listar(s, colaborador_id=c.id)
    assert f["situacao"] == "CONFIRMADA"


@pytestmark_banco
def test_ferias_que_batem_com_outras_sao_recusadas(mundo):
    s = mundo["s"]
    c = _carteira(mundo)
    _ferias_atendidas(mundo, c, _dia(40))
    p = svc.abrir(s, {"tipo": "FERIAS", "colaborador_id": c.id,
                      "ferias_inicio": _dia(50), "ferias_dias": "10"},
                  mundo["obra_user"])
    with pytest.raises(ErroValidacao, match="alteração de férias"):
        svc.atender(s, p["id"], {}, mundo["dp"])


@pytestmark_banco
def test_a_alteracao_so_muda_a_data_quando_a_contabilidade_devolve(mundo):
    from app.apps.erp.core import ferias as svc_ferias
    s = mundo["s"]
    c = _carteira(mundo)
    _devolve_e_conclui(mundo, _ferias_atendidas(mundo, c, _dia(40))["id"])
    [f] = svc_ferias.listar(s, colaborador_id=c.id)
    alt = svc.abrir(s, {"tipo": "ALTERACAO_FERIAS", "colaborador_id": c.id,
                        "ferias_id": f["id"], "ferias_inicio": _dia(60),
                        "ferias_dias": "20", "observacao": "obra pediu adiar"},
                    mundo["obra_user"])
    r = svc.atender(s, alt["id"], {}, mundo["dp"])
    assert r["situacao"] == "EM_ANDAMENTO"
    [f] = svc_ferias.listar(s, colaborador_id=c.id)
    assert f["inicio"] == _dia(40), "sem o retorno, a data confirmada continua valendo"
    assert f["mudanca"]["tipo"] == "ALTERACAO"
    _devolve_e_conclui(mundo, alt["id"])
    [f] = svc_ferias.listar(s, colaborador_id=c.id)
    assert (f["inicio"], f["dias"], f["situacao"]) == (_dia(60), 20, "CONFIRMADA")
    assert f["mudanca"] is None and f["historico"][0]["inicio"] == _dia(40)
    _assina_termo(mundo, alt["id"])
    assert svc.ler(s, alt["id"])["situacao"] == "ATENDIDA"


@pytestmark_banco
def test_o_cancelamento_de_ferias_passa_pela_contabilidade(mundo):
    from app.apps.erp.core import ferias as svc_ferias
    s = mundo["s"]
    c = _carteira(mundo)
    _devolve_e_conclui(mundo, _ferias_atendidas(mundo, c, _dia(40))["id"])
    canc = svc.abrir(s, {"tipo": "CANCELAMENTO_FERIAS", "colaborador_id": c.id,
                         "observacao": "a pessoa desistiu"}, mundo["obra_user"])
    svc.atender(s, canc["id"], {}, mundo["dp"])
    s.flush()
    [f] = svc_ferias.listar(s, colaborador_id=c.id)
    assert f["situacao"] == "CONFIRMADA" and f["mudanca"]["tipo"] == "CANCELAMENTO"
    _devolve_e_conclui(mundo, canc["id"])
    assert svc_ferias.listar(s, colaborador_id=c.id) == []
    [f] = svc_ferias.listar(s, colaborador_id=c.id, com_canceladas=True)
    assert f["situacao"] == "CANCELADA"


@pytestmark_banco
def test_alteracao_de_ferias_de_antes_do_sistema_nasce_no_retorno(mundo):
    """Férias marcadas no Pipefy não têm linha: a alteração não trava o DP."""
    from app.apps.erp.core import ferias as svc_ferias
    s = mundo["s"]
    c = _carteira(mundo)
    alt = svc.abrir(s, {"tipo": "ALTERACAO_FERIAS", "colaborador_id": c.id,
                        "ferias_inicio": _dia(30), "ferias_dias": "15",
                        "observacao": "marcadas no Pipefy"}, mundo["obra_user"])
    svc.atender(s, alt["id"], {}, mundo["dp"])
    _devolve_e_conclui(mundo, alt["id"])
    [f] = svc_ferias.listar(s, colaborador_id=c.id)
    assert (f["inicio"], f["dias"], f["situacao"]) == (_dia(30), 15, "CONFIRMADA")


@pytestmark_banco
def test_ferias_chegando_aparecem_para_a_obra_e_para_o_agente(mundo):
    from app.apps.erp.core import agente_dp, portal as svc_portal
    s = mundo["s"]
    c = _carteira(mundo)
    _devolve_e_conclui(mundo, _ferias_atendidas(mundo, c, _dia(5), "10")["id"])
    achados = [a for a in agente_dp.vigia(s) if a["colaborador_id"] == c.id]
    assert any(a["tipo"] == "ferias_chegando" for a in achados)
    # O DP alcança a base inteira; quem é preso a obra, só as dele (a mesma
    # regra de alcance do portal — `obras_que_alcanca`).
    lista = svc_portal.ferias_da_equipe(s, mundo["dp"])
    assert [f["pessoa"] for f in lista] == [c.nome]
    assert lista[0]["faltam_dias"] == 5


@pytestmark_banco
def test_ferias_sem_confirmacao_perto_da_data_o_agente_cobra(mundo):
    from app.apps.erp.core import agente_dp
    s = mundo["s"]
    c = _carteira(mundo)
    _ferias_atendidas(mundo, c, _dia(20))
    tipos = {a["tipo"] for a in agente_dp.vigia(s) if a["colaborador_id"] == c.id}
    assert "ferias_sem_confirmacao" in tipos


@pytestmark_banco
def test_cancelamento_pedido_segura_a_virada_do_dia(mundo):
    """⚑ Ele: *"Imagina desligar uma pessoa que vai continuar trabalhando. É
    uma falha grave."* A saída chegou, o cancelamento está pedido e o DP ainda
    não atendeu: a virada NÃO desliga; o agente cobra; e o DP atende depois da
    data sem a pessoa cair na readmissão."""
    import datetime as dt
    from app.apps.erp.core import agente_dp
    from app.apps.erp.core.cadastros import desligamentos_do_dia
    s = mundo["s"]
    c = _carteira(mundo)
    saida = dt.date.today() + dt.timedelta(days=2)
    d = svc.abrir(s, {"tipo": "DESLIGAMENTO", "colaborador_id": c.id,
                      "forma_desligamento": "Demissão com Aviso Trabalhado",
                      "aviso_previo": "2026-11-01",
                      "continua_prestador": "Não, está saindo da empresa"},
                  mundo["obra_user"])
    svc.atender(s, d["id"], {"dados": {"data_saida": saida.isoformat(),
                                       "ultimo_dia": saida.isoformat()}}, mundo["dp"])
    canc = svc.abrir(s, {"tipo": "CANCELAR_DESLIGAMENTO", "colaborador_id": c.id,
                         "observacao": "vai continuar"}, mundo["obra_user"])
    s.flush()
    assert any(a["tipo"] == "cancelamento_de_desligamento" and a["colaborador_id"] == c.id
               for a in agente_dp.vigia(s))
    r = desligamentos_do_dia.aplicar(s, hoje=saida + dt.timedelta(days=3))
    assert c.id not in [q["id"] for q in r["quem"]], "a virada não pode desligar"
    s.expire_all()
    assert s.get(svc_pessoas.Colaborador, c.id).situacao == "ATIVO"
    # O DP atende depois da data: vale, porque foi pedido a tempo.
    import unittest.mock as um
    with um.patch.object(svc_pessoas, "ja_saiu", return_value=True):
        res = svc.atender(s, canc["id"], {}, mundo["dp"])
    assert res["situacao"] == "EM_ANDAMENTO"
    assert s.get(svc_pessoas.Colaborador, c.id).demissao is None


@pytestmark_banco
def test_o_portal_oferece_ferias_e_cancelamentos():
    from app.apps.erp.core import portal as svc_portal
    for t in ("FERIAS", "ALTERACAO_FERIAS", "CANCELAMENTO_FERIAS",
              "CANCELAR_DESLIGAMENTO"):
        assert t in svc_portal.TIPOS_DO_PORTAL



# ===========================================================================
# 20/11/2026 (noite): a contabilidade como ponte, corrigir e cancelar o que
# já foi, a análise do agente, os períodos de férias e a experiência
# ===========================================================================
@pytestmark_banco
def test_o_despacho_monta_o_pedido_a_contabilidade_no_atendimento(mundo):
    """Ele: *"a ideia de futuro é que o DP apenas olhe e dê o ok."*"""
    s = mundo["s"]
    c = _carteira(mundo)
    p = _ferias_atendidas(mundo, c, _dia(45))
    passo = next(x for x in svc.ler(s, p["id"])["processo"]["passos"]
                 if x["codigo"] == "CONTABILIDADE")
    assert passo.get("pedido"), "o agente devia ter montado o rascunho"
    assert "montado" in passo["detalhe"]


@pytestmark_banco
def test_atualizacao_de_nome_e_endereco_nao_vai_a_contabilidade(mundo):
    """Ele, 20/11/2026: *"Troca de nome e endereço não mandar pra
    contabilidade."*"""
    s = mundo["s"]
    c = _carteira(mundo)
    p = svc.abrir(s, {"tipo": "ATUALIZACAO_DADOS", "colaborador_id": c.id,
                      "descricao": "mudou de casa", "cep": "60000000",
                      "logradouro": "RUA NOVA", "numero_endereco": "10",
                      "bairro": "CENTRO", "municipio": "FORTALEZA", "uf": "CE"},
                  mundo["obra_user"])
    assert svc.atender(s, p["id"], {}, mundo["dp"])["situacao"] == "ATENDIDA"


@pytestmark_banco
def test_desconto_em_folha_pede_a_autorizacao_assinada_antes_da_contabilidade(mundo):
    from app.apps.erp.core import contabil_processo, termos
    s = mundo["s"]
    c = _carteira(mundo)
    p = svc.abrir(s, {"tipo": "DESCONTO_EM_FOLHA", "colaborador_id": c.id,
                      "valor_total": "300", "parcelas": "3",
                      "observacao": "ferramenta perdida"}, mundo["obra_user"])
    r = svc.atender(s, p["id"], {}, mundo["dp"])
    assert r["situacao"] == "EM_ANDAMENTO"
    t_ = termos.do_pedido(s, p["id"])
    assert t_["tipo"] == "AUTORIZACAO-DESCONTO" and "AUTORIZO" in t_["texto"]
    with pytest.raises(ErroValidacao, match="autorização"):
        contabil_processo.montar_pedido(s, svc.ler(s, p["id"]), mundo["dp"])
    s.execute(text("UPDATE dp_termos SET situacao = 'ASSINADO', forma = 'NA_TELA' "
                   "WHERE id = :i"), {"i": t_["id"]})
    svc.termo_voltou(s, t_["id"], mundo["dp"])
    _devolve_e_conclui(mundo, p["id"])
    assert svc.ler(s, p["id"])["situacao"] == "ATENDIDA"


@pytestmark_banco
def test_corrigir_o_que_ja_foi_a_contabilidade_pede_a_ciencia_dela(mundo):
    """Ele: *"se eu já tiver encaminhado alguma coisa para a contabilidade?
    (…) Preciso comunicar a correção, preciso ter confirmar que há a ciência
    daquela alteração."*"""
    from app.apps.erp.core import contabilidade as svc_ct
    from app.apps.erp.core import ferias as svc_ferias
    s = mundo["s"]
    c = _carteira(mundo)
    p = _ferias_atendidas(mundo, c, _dia(45))
    passo = next(x for x in svc.ler(s, p["id"])["processo"]["passos"]
                 if x["codigo"] == "CONTABILIDADE")
    s.execute(text("UPDATE dp_pedidos SET situacao = 'ENVIADO' WHERE id = :i"),
              {"i": passo["pedido"]["id"]})
    svc.corrigir(s, p["id"], {"dados": {"ferias_inicio": _dia(50)}}, mundo["dp"])
    sol = svc.ler(s, p["id"])
    extra = [x for x in sol["processo"]["passos"] if x["codigo"].startswith("EXTRA_")]
    assert extra and "Correção comunicada" in extra[0]["rotulo"]
    [f] = svc_ferias.listar(s, colaborador_id=c.id)
    assert f["inicio"] == _dia(50), "a programação ainda pedida acompanha a correção"
    # a contabilidade devolve o pedido principal: ainda falta a ciência da correção
    s.execute(text("UPDATE dp_pedidos SET situacao = 'RESPONDIDO' WHERE id = :i"),
              {"i": passo["pedido"]["id"]})
    svc_ct.concluir(s, passo["pedido"]["id"], mundo["dp"])
    assert svc.ler(s, p["id"])["situacao"] == "EM_ANDAMENTO"
    s.execute(text("UPDATE dp_pedidos SET situacao = 'RESPONDIDO' WHERE id = :i"),
              {"i": extra[0]["pedido"]["id"]})
    svc_ct.concluir(s, extra[0]["pedido"]["id"], mundo["dp"])
    assert svc.ler(s, p["id"])["situacao"] == "EM_ANDAMENTO", "falta o aviso assinado"
    _assina_termo(mundo, p["id"])
    assert svc.ler(s, p["id"])["situacao"] == "ATENDIDA"


@pytestmark_banco
def test_cancelar_depois_de_atendido_espera_a_contabilidade(mundo):
    from app.apps.erp.core import contabil_processo
    from app.apps.erp.core import contabilidade as svc_ct
    from app.apps.erp.core import ferias as svc_ferias
    s = mundo["s"]
    c = _carteira(mundo)
    p = _ferias_atendidas(mundo, c, _dia(45))
    ped = next(x for x in svc.ler(s, p["id"])["processo"]["passos"]
               if x["codigo"] == "CONTABILIDADE")["pedido"]
    s.execute(text("UPDATE dp_pedidos SET situacao = 'ENVIADO' WHERE id = :i"),
              {"i": ped["id"]})
    # a obra pede; o DP faz
    svc.pedir_mudanca(s, p["id"], {"o_que": "cancelar", "texto": "a pessoa desistiu"},
                      mundo["obra_user"])
    assert svc.ler(s, p["id"])["mudanca_pedida"]["o_que"] == "cancelar"
    r = contabil_processo.cancelar_em_andamento(s, svc.ler(s, p["id"]),
                                                "a pessoa desistiu", mundo["dp"])
    assert r["cancelamento"]
    assert svc.ler(s, p["id"])["situacao"] == "EM_ANDAMENTO", "falta a ciência"
    extra = next(x for x in svc.ler(s, p["id"])["processo"]["passos"]
                 if x["codigo"].startswith("EXTRA_"))
    s.execute(text("UPDATE dp_pedidos SET situacao = 'RESPONDIDO' WHERE id = :i"),
              {"i": extra["pedido"]["id"]})
    svc_ct.concluir(s, extra["pedido"]["id"], mundo["dp"])
    assert svc.ler(s, p["id"])["situacao"] == "CANCELADA"
    assert svc_ferias.listar(s, colaborador_id=c.id) == []


@pytestmark_banco
def test_cancelar_antes_de_enviar_cancela_na_hora(mundo):
    from app.apps.erp.core import contabil_processo
    s = mundo["s"]
    c = _carteira(mundo)
    p = _ferias_atendidas(mundo, c, _dia(45))
    r = contabil_processo.cancelar_em_andamento(s, svc.ler(s, p["id"]),
                                                "pedido em dobro", mundo["dp"])
    assert r == {"cancelada": True}
    assert svc.ler(s, p["id"])["situacao"] == "CANCELADA"


@pytestmark_banco
def test_a_analise_aponta_experiencia_no_pedido_de_demissao_com_aviso(mundo):
    """Ele: *"ela pediu uma demissão, mas é um encerramento de contrato de
    experiência (…) aí não precisa de aviso."*"""
    import datetime as dt
    s = mundo["s"]
    c = svc_pessoas.salvar(s, {"cpf": "12345678909", "nome": "Novato Exp",
                               "admissao": (dt.date.today() - dt.timedelta(days=20)).isoformat(),
                               "obra_id": mundo["obra"].id, "vinculo": "CTPS",
                               "salario": "2000", "cargo_id": mundo["cargo_id"],
                               "contrato_experiencia": "45+45 dias",
                               "etapa": "CONCLUIDO"}, mundo["dp"])
    s.flush()
    p = svc.abrir(s, {"tipo": "DESLIGAMENTO", "colaborador_id": c.id,
                      "forma_desligamento": "Demissão com Aviso Indenizado",
                      "aviso_previo": _dia(0),
                      "continua_prestador": "Não, está saindo da empresa"},
                  mundo["obra_user"])
    a = svc.ler(s, p["id"])["analise"]
    assert any(x["ok"] is False and "experiência" in x["texto"] for x in a)
    assert any("aviso de 30 dias" in x["texto"] for x in a)


@pytestmark_banco
def test_o_vigia_avisa_do_fim_da_experiencia(mundo):
    import datetime as dt
    from app.apps.erp.core import agente_dp
    s = mundo["s"]
    c = svc_pessoas.salvar(s, {"cpf": "98765432100", "nome": "Quase Efetivo",
                               "admissao": (dt.date.today() - dt.timedelta(days=40)).isoformat(),
                               "obra_id": mundo["obra"].id, "vinculo": "CTPS",
                               "contrato_experiencia": "45+45 dias",
                               "etapa": "CONCLUIDO"}, mundo["dp"])
    s.flush()
    assert any(a["tipo"] == "experiencia_vencendo" and a["colaborador_id"] == c.id
               for a in agente_dp.vigia(s))


@pytestmark_banco
def test_ferias_vencendo_aparecem_e_o_controle_informado_vale(mundo):
    """Admitido há ~2 anos e meio, com o 1º período quitado: o 2º período
    (completo) vence em poucos meses."""
    import datetime as dt
    from app.apps.erp.core import agente_dp
    from app.apps.erp.core import ferias as svc_ferias
    s = mundo["s"]
    hoje = dt.date.today()
    adm = hoje.replace(year=hoje.year - 2) - dt.timedelta(days=300)
    c = svc_pessoas.salvar(s, {"cpf": "45612378955", "nome": "Ferias Vencendo",
                               "admissao": adm.isoformat(), "obra_id": mundo["obra"].id,
                               "vinculo": "CTPS", "etapa": "CONCLUIDO"}, mundo["dp"])
    s.flush()
    est = {x["colaborador_id"]: x for x in svc_ferias.situacao_dos_periodos(s)}
    assert est[c.id]["estado"] == "nao_informado"
    svc_ferias.informar_quitado(s, c.id, (adm.replace(year=adm.year + 1)
                                          - dt.timedelta(days=1)).isoformat(), mundo["dp"])
    est = {x["colaborador_id"]: x for x in svc_ferias.situacao_dos_periodos(s)}
    assert est[c.id]["estado"] == "vencendo", est[c.id]
    assert any(a["tipo"] == "ferias_vencendo" and a["colaborador_id"] == c.id
               for a in agente_dp.vigia(s))
    # programar as férias do período tira o alerta
    p = _ferias_atendidas(mundo, c, _dia(40))
    _devolve_e_conclui(mundo, p["id"])
    est = {x["colaborador_id"]: x for x in svc_ferias.situacao_dos_periodos(s)}
    assert est[c.id]["estado"] == "em_dia", est[c.id]


@pytestmark_banco
def test_o_aviso_de_ferias_vai_para_a_obra_assinar_e_so_entao_fecha(mundo):
    """Ele, 20/11/2026: *"Aviso de férias em tela."*"""
    from app.apps.erp.core import termos
    s = mundo["s"]
    c = _carteira(mundo)
    p = _ferias_atendidas(mundo, c, _dia(40), "20")
    _devolve_e_conclui(mundo, p["id"])
    sol = svc.ler(s, p["id"])
    assert sol["situacao"] == "EM_ANDAMENTO", "falta a pessoa assinar o aviso"
    t_ = termos.do_pedido(s, p["id"])
    assert t_["tipo"] == "AVISO-FERIAS" and "20 dias" in t_["texto"]
    assert next(x for x in sol["processo"]["passos"]
                if x["codigo"] == "AVISO_FERIAS")["estado"] == "AGUARDANDO"
    s.execute(text("UPDATE dp_termos SET situacao = 'ASSINADO', forma = 'NA_TELA' "
                   "WHERE id = :i"), {"i": t_["id"]})
    svc.termo_voltou(s, t_["id"], None)
    assert svc.ler(s, p["id"])["situacao"] == "ATENDIDA"


@pytestmark_banco
def test_a_alteracao_de_ferias_tambem_gera_aviso_com_a_data_nova(mundo):
    from app.apps.erp.core import ferias as svc_ferias
    from app.apps.erp.core import termos
    s = mundo["s"]
    c = _carteira(mundo)
    p = _ferias_atendidas(mundo, c, _dia(40))
    _devolve_e_conclui(mundo, p["id"])
    [f] = svc_ferias.listar(s, colaborador_id=c.id)
    alt = svc.abrir(s, {"tipo": "ALTERACAO_FERIAS", "colaborador_id": c.id,
                        "ferias_id": f["id"], "ferias_inicio": _dia(70),
                        "ferias_dias": "30", "observacao": "obra pediu adiar"},
                    mundo["obra_user"])
    svc.atender(s, alt["id"], {}, mundo["dp"])
    _devolve_e_conclui(mundo, alt["id"])
    t_ = termos.do_pedido(s, alt["id"])
    import datetime as dt
    nova = dt.date.fromisoformat(_dia(70)).strftime("%d/%m/%Y")
    assert t_ is not None and nova in t_["texto"]


# ===========================================================================
# 20/11/2026: a obra atual pelo ponto e o fim da experiência para a obra
# ===========================================================================
@pytest.fixture(scope="session")
def _schema_ponto(banco):
    """O schema `ponto` pela migração de verdade (o mesmo de
    `test_diario_efetivo_do_ponto_banco.py`; não derruba no fim)."""
    from app.apps.ponto import migracoes_runner
    resultado = migracoes_runner.aplicar_pendentes()
    assert not resultado["erro"], resultado
    return True


def _bate(mundo, colaborador_id, obra_id, dia):
    import datetime as dt
    s = mundo["s"]
    nsr = s.execute(text("SELECT COALESCE(MAX(nsr), 0) + 1 FROM ponto.marcacoes")).scalar()
    s.execute(text("""
        INSERT INTO ponto.marcacoes (nsr, colaborador_id, obra_id, timestamp_servidor,
                                     data_referencia, origem, status, hash_encadeado)
        VALUES (:n, :c, :o, :t, :d, 'PWA', 'VALIDA', 'hash-teste-' || :n)"""),
        {"n": nsr, "c": colaborador_id, "o": obra_id,
         "t": dt.datetime.combine(dia, dt.time(11), tzinfo=dt.timezone.utc), "d": dia})


@pytestmark_banco
def test_a_obra_atual_vem_do_ponto_e_a_transferencia_e_avisada(_schema_ponto, mundo):
    """Ele: *"teremos a obra de cadastro e a obra atual. Para a obra atual,
    vamos buscar a informação do ponto. Uma rotina que atualize."*"""
    import datetime as dt
    from app.apps.erp.core import agente_dp, obra_atual
    from app.apps.erp.db.models.cadastros import Obra
    s = mundo["s"]
    c = _carteira(mundo)
    outra = Obra(codigo="ADM-02", nome="Outra obra", status="ATIVA")
    s.add(outra)
    s.flush()
    hoje = dt.date.today()
    for n in range(1, 4):
        _bate(mundo, c.id, mundo["obra"].id, hoje - dt.timedelta(days=20 + n))
    obra_atual.atualizar(s, hoje=hoje - dt.timedelta(days=12))
    assert obra_atual.da_pessoa(s, c.id)["obra_id"] == mundo["obra"].id
    for n in range(1, 6):
        _bate(mundo, c.id, outra.id, hoje - dt.timedelta(days=n))
    r = obra_atual.atualizar(s, hoje=hoje - dt.timedelta(days=8))
    r = obra_atual.atualizar(s, hoje=hoje)
    atual = obra_atual.da_pessoa(s, c.id)
    assert atual["obra_id"] == outra.id
    assert s.execute(text("SELECT count(*) FROM colaborador_obra_mudancas "
                          "WHERE colaborador_id = :c"), {"c": c.id}).scalar() == 1
    # a ficha mostra as duas
    ficha = svc_pessoas.ficha(s, c.id)
    assert ficha["obra_id"] == mundo["obra"].id and ficha["obra_atual"]["codigo"] == "ADM-02"
    # a folha recebe a obra atual; a de cadastro segue à parte
    linha = next(x for x in svc_pessoas.para_folha(s) if x["cpf"] == c.cpf)
    assert (linha["obra_codigo"], linha["obra_cadastro"]) == ("ADM-02", "ADM-01")
    _ = agente_dp


@pytestmark_banco
def test_o_fim_da_experiencia_aparece_para_a_obra_e_para_o_dp(mundo):
    """Ele: *"DP e o administrativo de obra devam ser alertados sobre o
    acontecimento. Poucos dias antes."*"""
    import datetime as dt
    from app.apps.erp.core import pessoal_experiencia
    s = mundo["s"]
    c = svc_pessoas.salvar(s, {"cpf": "98765432100", "nome": "Experiencia Acabando",
                               "admissao": (dt.date.today() - dt.timedelta(days=40)).isoformat(),
                               "obra_id": mundo["obra"].id, "vinculo": "CTPS",
                               "contrato_experiencia": "45+45 dias",
                               "etapa": "CONCLUIDO"}, mundo["dp"])
    s.flush()
    [x] = [x for x in pessoal_experiencia.terminando(s, [mundo["obra"].id])
           if x["colaborador_id"] == c.id]
    assert x["faltam_dias"] == 4 and "PRORROGA" in x["o_que_acontece"]
    assert pessoal_experiencia.terminando(s, [-1]) == []


# ===========================================================================
# 20/11/2026: o que o ponto lança e precisa da contabilidade vira solicitação
# ===========================================================================
def _ocorrencia_ponto(mundo, colaborador_id, *, status="AGUARDANDO_DP",
                      por="sistema (atestado nº 1)", dias=20):
    import datetime as dt
    s = mundo["s"]
    ini = dt.date.today() - dt.timedelta(days=2)
    return s.execute(text("""
        INSERT INTO ponto.ocorrencias (colaborador_id, tipo, data_inicio, data_fim,
                                       obra_id, descricao, status, origem, solicitado_por)
        VALUES (:c, 'AFASTAMENTO', :i, :f, :o, 'atestados somam 20 dias', :st,
                'GESTAO', :por) RETURNING id"""),
        {"c": colaborador_id, "i": ini, "f": ini + dt.timedelta(days=dias),
         "o": mundo["obra"].id, "st": status, "por": por}).scalar()


@pytestmark_banco
def test_o_afastamento_do_ponto_vira_solicitacao_e_vai_a_contabilidade(_schema_ponto, mundo):
    """Ele: *"automaticamente gera uma solicitação do DP para ser dado o devido
    tratamento junto à contabilidade."*"""
    from app.apps.erp.core import ponte_do_ponto
    from app.apps.erp.core.cadastros import afastamentos
    s = mundo["s"]
    c = _carteira(mundo)
    oc = _ocorrencia_ponto(mundo, c.id)
    assert ponte_do_ponto.trazer(s)["abertas"] >= 1
    assert ponte_do_ponto.trazer(s)["abertas"] == 0, "não abre duas vezes"
    sid = s.execute(text("SELECT id FROM dp_solicitacoes WHERE tipo = 'AFASTAMENTO' "
                         "AND colaborador_id = :c"), {"c": c.id}).scalar()
    p = svc.ler(s, sid)
    assert p["dados"]["ocorrencia_ponto"] == str(oc)
    assert p["dados"]["afastamento_natureza"] == "INSS"
    r = svc.atender(s, p["id"], {}, mundo["dp"])
    assert r["situacao"] == "EM_ANDAMENTO"
    assert afastamentos.aberto(s, c.id) is not None, "o afastamento entra na ficha"
    _devolve_e_conclui(mundo, p["id"])
    assert svc.ler(s, p["id"])["situacao"] == "ATENDIDA"


@pytestmark_banco
def test_afastamento_negado_no_ponto_cancela_a_solicitacao(_schema_ponto, mundo):
    from app.apps.erp.core import ponte_do_ponto
    s = mundo["s"]
    c = _carteira(mundo)
    oc = _ocorrencia_ponto(mundo, c.id, por="dp")
    ponte_do_ponto.trazer(s)
    s.execute(text("UPDATE ponto.ocorrencias SET status = 'NEGADA' WHERE id = :i"),
              {"i": oc})
    assert ponte_do_ponto.trazer(s)["canceladas"] == 1
    assert s.execute(text("SELECT situacao FROM dp_solicitacoes WHERE tipo = "
                          "'AFASTAMENTO' AND colaborador_id = :c"),
                     {"c": c.id}).scalar() == "CANCELADA"


@pytestmark_banco
def test_afastamento_sem_natureza_nao_e_atendido(mundo):
    s = mundo["s"]
    c = _carteira(mundo)
    p = svc.abrir(s, {"tipo": "AFASTAMENTO", "colaborador_id": c.id,
                      "afast_inicio": _dia(-1)}, mundo["obra_user"])
    with pytest.raises(ErroValidacao, match="natureza"):
        svc.atender(s, p["id"], {}, mundo["dp"])
    r = svc.atender(s, p["id"], {"dados": {"afastamento_natureza":
                                           "Acidente de trabalho ou doença ocupacional"}},
                    mundo["dp"])
    assert r["situacao"] == "EM_ANDAMENTO"


@pytestmark_banco
def test_acidente_sem_pedido_a_contabilidade_o_agente_cobra(mundo):
    from app.apps.erp.core import agente_dp
    s = mundo["s"]
    c = _carteira(mundo)
    svc.abrir(s, {"tipo": "AFASTAMENTO", "colaborador_id": c.id,
                  "afast_inicio": _dia(0),
                  "afastamento_natureza": "Acidente de trabalho ou doença ocupacional"},
              mundo["obra_user"])
    assert any(a["tipo"] == "acidente_sem_cat" and a["colaborador_id"] == c.id
               for a in agente_dp.vigia(s))


@pytestmark_banco
def test_afastar_pela_ficha_tambem_abre_a_solicitacao(mundo):
    from app.apps.erp.core.cadastros import afastamentos
    s = mundo["s"]
    c = _carteira(mundo)
    d = {"tipo": "MATERNIDADE", "data_inicio": _dia(0), "previsao_retorno": _dia(119)}
    afastamentos.abrir(s, c.id, d, mundo["dp"])
    numero = svc.comunicar_pela_ficha(s, c.id, "AFASTAMENTO", d, mundo["dp"])
    assert numero
    sid = s.execute(text("SELECT id FROM dp_solicitacoes WHERE numero = :n"),
                    {"n": numero}).scalar()
    r = svc.atender(s, sid, {}, mundo["dp"])
    assert r["situacao"] == "EM_ANDAMENTO", "já estava na ficha: só vai à contabilidade"


@pytestmark_banco
def test_a_obra_de_cadastro_muda_sozinha_para_a_obra_do_mes(_schema_ponto, mundo):
    """Ele: *"trabalhou 15 dias numa obra, 5 dias na outra (…) a obra que ela
    trabalhou os 15 dias. E todo mês isso a gente faz essa alteração. Não
    vamos botar nada para o DP."*"""
    import datetime as dt
    from app.apps.erp.core import obra_do_mes
    from app.apps.erp.db.models.cadastros import Obra
    s = mundo["s"]
    c = _carteira(mundo)                              # cadastro: ADM-01
    outra = Obra(codigo="ADM-03", nome="Obra do mes", status="ATIVA")
    s.add(outra)
    s.flush()
    comp = dt.date(2026, 10, 1)
    for d in range(1, 6):                             # 5 dias na de cadastro
        _bate(mundo, c.id, mundo["obra"].id, comp.replace(day=d))
    for d in range(6, 21):                            # 15 dias na outra
        _bate(mundo, c.id, outra.id, comp.replace(day=d))
        _bate(mundo, c.id, outra.id, comp.replace(day=d))   # 2 batidas = 1 dia
    r = obra_do_mes.se_preciso(s, hoje=dt.date(2026, 11, 2))
    assert r and r["mudaram"] >= 1
    s.expire_all()
    assert s.get(svc_pessoas.Colaborador, c.id).obra_id == outra.id
    [linha] = [x for x in obra_do_mes.do_mes(s, comp) if x["colaborador_id"] == c.id]
    assert linha["dias"] == 15 and linha["mudou"] and linha["obra_anterior"] == "ADM-01"
    assert linha["dias_por_obra"][str(mundo["obra"].id)] == 5
    # uma vez por mês: rodar de novo não mexe
    assert obra_do_mes.se_preciso(s, hoje=dt.date(2026, 11, 3)) is None
    # nenhum pedido ao DP
    assert s.execute(text("SELECT count(*) FROM dp_solicitacoes WHERE colaborador_id = :c "
                          "AND tipo = 'TRANSFERENCIA_OBRA'"), {"c": c.id}).scalar() == 0



# ===========================================================================
# 20/11/2026: as regras que ele ditou sobre o cadastro
# ===========================================================================
@pytestmark_banco
def test_readmissao_em_ate_um_ano_entra_sem_experiencia(mundo):
    """Ele: *"se o colaborador sair e retornar num prazo de um ano, na
    readmissão o contrato tem que ser indeterminado (…) não entra em período
    de experiência mais."*"""
    import datetime as dt
    s = mundo["s"]
    c = _carteira(mundo)
    s.execute(text("UPDATE colaborador_vinculos SET admissao = '2025-01-10' "
                   "WHERE colaborador_id = :c"), {"c": c.id})
    saida = dt.date.today() - dt.timedelta(days=90)
    svc_pessoas.desligar(s, c.id, {"data_saida": saida.isoformat()}, mundo["dp"])
    s.flush()
    svc_pessoas.readmitir(s, c.id, {"admissao": dt.date.today().isoformat(),
                                    "tipo_contrato": "CLT_DETERMINADO"}, mundo["dp"])
    s.flush()
    volta = s.get(svc_pessoas.Colaborador, c.id)
    assert volta.tipo_contrato == "CLT_INDETERMINADO"
    assert volta.contrato_experiencia == svc_pessoas.SEM_EXPERIENCIA


@pytestmark_banco
def test_autonomo_nao_tem_convencao_e_precisa_da_diaria():
    """Ele: *"Autônomo não tem convenção. Apenas se define o valor da
    diária."*"""
    from app.apps.erp.db.models.cadastros import Colaborador
    assert not svc_pessoas.campo_aparece("convencao_id", vinculo="PRESTADOR",
                                         tipo_contrato="AUTONOMO_RPA")
    rpa = Colaborador(nome="X", cpf="1", vinculo="PRESTADOR",
                      tipo_contrato="AUTONOMO_RPA", cargo_id=1)
    falta = svc_pessoas.o_que_falta(rpa)
    assert "valor da diária" in falta and "convenção" not in falta


@pytestmark_banco
def test_passada_a_experiencia_o_contrato_vira_indeterminado_sozinho(mundo):
    """Ele: *"Passado o período de experiência ele vira indeterminado (…)
    quero que altere sozinho o contrato."*"""
    import datetime as dt
    from app.apps.erp.core import pessoal_experiencia
    s = mundo["s"]
    hoje = dt.date.today()
    velho = svc_pessoas.salvar(s, {"cpf": "12345678909", "nome": "Experiencia Acabou",
                                   "admissao": (hoje - dt.timedelta(days=120)).isoformat(),
                                   "obra_id": mundo["obra"].id, "vinculo": "CTPS",
                                   "tipo_contrato": "CLT_DETERMINADO",
                                   "contrato_experiencia": "45+45 dias",
                                   "etapa": "CONCLUIDO"}, mundo["dp"])
    novo = svc_pessoas.salvar(s, {"cpf": "98765432100", "nome": "Ainda Em Experiencia",
                                  "admissao": (hoje - dt.timedelta(days=30)).isoformat(),
                                  "obra_id": mundo["obra"].id, "vinculo": "CTPS",
                                  "tipo_contrato": "CLT_DETERMINADO",
                                  "contrato_experiencia": "45+45 dias",
                                  "etapa": "CONCLUIDO"}, mundo["dp"])
    s.flush()
    r = pessoal_experiencia.virar_indeterminado(s, hoje)
    assert r["viraram"] >= 1
    s.expire_all()
    assert s.get(svc_pessoas.Colaborador, velho.id).tipo_contrato == "CLT_INDETERMINADO"
    assert s.get(svc_pessoas.Colaborador, novo.id).tipo_contrato == "CLT_DETERMINADO"
    assert pessoal_experiencia.virar_indeterminado(s, hoje)["viraram"] == 0


@pytestmark_banco
def test_a_admissao_confirmada_pelo_dp_e_a_que_vai_a_contabilidade(mundo):
    """Ele (25/11/2026): *"não necessariamente a data de início ela é igual à
    data de admissão (…) a gente gosta de admitir no primeiro dia ou no 16º
    dia (…) o que vale para a contabilidade é a data de admissão."* As duas
    ficam: o início para os dias de diária, a admissão para a carteira."""
    import datetime as dt
    from app.apps.erp.core import contabilidade as svc_ct
    s = mundo["s"]
    p = svc.abrir(s, _admissao(mundo, tipo_documentacao="CTPS",
                               tipo_contrato="CLT (tempo indeterminado)",
                               num_ctps="1234567", data_inicio="2026-11-05",
                               documentos=["DOC-IDENTIDADE", "CPF-DOC",
                                           "COMPROVANTE-ENDERECO-PESSOA",
                                           "CTPS-FOTO", "CTPS-INFORMACOES"]),
                  mundo["obra_user"])
    assert "admissao" in svc.ler(s, p["id"])["campos_do_dp"]
    svc.atender(s, p["id"], {"dados": {"salario": "2000",
                                       "admissao": "2026-11-16"}}, mundo["dp"])
    s.flush()
    c = s.get(svc_pessoas.Colaborador, p["colaborador_id"])
    s.refresh(c)
    assert c.data_inicio == dt.date(2026, 11, 5), "o início da obra fica"
    assert c.admissao == dt.date(2026, 11, 16), "a admissão é a do DP"
    v = svc_pessoas.vinculo_aberto(s, c.id)
    s.refresh(v)
    assert v.admissao == dt.date(2026, 11, 16), "o vínculo acompanha"
    ped = svc_ct.abrir(s, {"template": "ASSINATURA_CTPS",
                           "colaborador_id": c.id, "solicitacao_id": p["id"]},
                       mundo["dp"])
    corpo = s.execute(text("SELECT corpo FROM dp_pedidos WHERE id = :i"),
                      {"i": ped["id"]}).scalar() or ""
    assert "16/11/2026" in corpo and "05/11/2026" not in corpo


@pytestmark_banco
def test_sem_o_dp_mexer_a_admissao_e_o_inicio(mundo):
    import datetime as dt
    s = mundo["s"]
    p = svc.abrir(s, _admissao(mundo, tipo_documentacao="CTPS",
                               tipo_contrato="CLT (tempo indeterminado)",
                               num_ctps="1234567",
                               documentos=["DOC-IDENTIDADE", "CPF-DOC",
                                           "COMPROVANTE-ENDERECO-PESSOA",
                                           "CTPS-FOTO", "CTPS-INFORMACOES"]),
                  mundo["obra_user"])
    svc.atender(s, p["id"], {"dados": {"salario": "2000"}}, mundo["dp"])
    s.flush()
    c = s.get(svc_pessoas.Colaborador, p["colaborador_id"])
    assert c.admissao == c.data_inicio == dt.date(2026, 11, 20)


@pytestmark_banco
def test_readmissao_guarda_o_inicio_e_a_admissao_separados(mundo):
    import datetime as dt
    s = mundo["s"]
    volta = mundo["volta"]
    p = svc.abrir(s, _admissao(mundo, cpf=CPF_VOLTA, nome="Jose Que Volta",
                               data_inicio="2026-11-20", documentos=[]),
                  mundo["obra_user"])
    svc.atender(s, p["id"], {"dados": {"admissao": "2026-12-01"}}, mundo["dp"])
    s.flush()
    c = s.get(svc_pessoas.Colaborador, volta.id)
    assert c.data_inicio == dt.date(2026, 11, 20)
    assert c.admissao == dt.date(2026, 12, 1)


@pytestmark_banco
def test_alteracao_de_cargo_leva_cargo_convencao_e_valor_a_contabilidade(mundo):
    """Ele (25/11/2026): *"a obra faz o pleito de alteração, ela vai indicar o
    cargo e no DP (…) ele tem que selecionar a convenção e automaticamente
    aparecer valor para encaminhar no pedido que vai para a contabilidade.
    Cargo, convenção e valor."*"""
    s = mundo["s"]
    c = _carteira(mundo)
    novo = s.execute(text("INSERT INTO cargos (nome, ativo) VALUES "
                          "('Mestre de obras adm', TRUE) RETURNING id")).scalar()
    conv = s.execute(text("INSERT INTO convencoes (nome) VALUES "
                          "('Convenção Teste 2026') RETURNING id")).scalar()
    p = svc.abrir(s, {"tipo": "ALTERACAO_CARGO", "colaborador_id": c.id,
                      "data_inicio": "2026-12-01", "cargo_pleiteado_id": novo,
                      "justificativa": "promovido na obra"}, mundo["obra_user"])
    lido = svc.ler(s, p["id"])
    assert "convencao_id" in lido["campos_do_dp"]
    assert "pessoa_convencao_id" in lido
    svc.atender(s, p["id"], {"dados": {"cargo_deferido_id": str(novo),
                                       "convencao_id": str(conv),
                                       "salario": "3100"}}, mundo["dp"])
    s.flush()
    assert s.get(svc_pessoas.Colaborador, c.id).convencao_id == conv
    # O atendimento já monta o pedido à contabilidade (rascunho).
    corpo = s.execute(text("SELECT corpo FROM dp_pedidos WHERE solicitacao_id = :i "
                           "ORDER BY id DESC LIMIT 1"), {"i": p["id"]}).scalar() or ""
    assert "Mestre de obras adm" in corpo
    assert "Convenção Teste 2026" in corpo
    assert "3100" in corpo or "3.100" in corpo


@pytestmark_banco
def test_a_recusa_aparece_na_entrada_do_portal_ate_quem_pediu_ver(mundo):
    """Ele (25/11/2026): *"na hora que ela logasse no portal, ela já se
    deparasse com aquela mensagem, aquele alerta de recusa."*"""
    from app.apps.erp.core import portal as svc_portal
    from app.apps.erp.core.comum.auditoria import ErroNaoEncontrado
    s = mundo["s"]
    p = svc.abrir(s, _admissao(mundo), mundo["obra_user"])
    svc.recusar(s, p["id"], {"resposta": "faltou o comprovante de endereço"},
                mundo["dp"])
    s.flush()
    meu = next(x for x in svc_portal.meus_pedidos(s, mundo["obra_user"])
               if x["id"] == p["id"])
    assert meu["recusa_nao_vista"] is True
    assert "comprovante" in meu["resposta"]
    with pytest.raises(ErroNaoEncontrado):
        svc_portal.ciente_da_recusa(s, p["id"], mundo["dp"])   # não foi quem pediu
    svc_portal.ciente_da_recusa(s, p["id"], mundo["obra_user"])
    svc_portal.ciente_da_recusa(s, p["id"], mundo["obra_user"])  # não duplica
    s.flush()
    meu = next(x for x in svc_portal.meus_pedidos(s, mundo["obra_user"])
               if x["id"] == p["id"])
    assert meu["recusa_nao_vista"] is False
    eventos = [e["acao"] for e in svc.ler(s, p["id"])["eventos"]]
    assert eventos.count(svc_portal.RECUSA_VISTA) == 1


@pytestmark_banco
def test_o_dp_ve_e_abre_os_documentos_que_a_obra_mandou(mundo, app_real):
    """Ele (25/11/2026): *"a operadora do DP (…) não está visualizando os
    anexos. Onde é que eles ficam? Confirma se está nessa tela."* Caminho
    inteiro: a obra manda o pedido com os arquivos, o DP abre o pedido e abre
    cada arquivo."""
    from tests.conftest import como                      # type: ignore
    s = mundo["s"]
    p = svc.abrir(s, _admissao(mundo), mundo["obra_user"])
    cid = s.execute(text("SELECT colaborador_id FROM dp_solicitacoes "
                         "WHERE id = :i"), {"i": p["id"]}).scalar()
    assert cid, "a admissão nasce com o pré-cadastro"
    svc.guardar_papeis(s, p["id"], cid, [
        {"conteudo": b"%PDF-1.4 rg" * 80, "nome": "rg.pdf", "tipo": "DOC-IDENTIDADE"},
        {"conteudo": b"%PDF-1.4 cpf" * 80, "nome": "cpf.pdf", "tipo": "CPF-DOC"},
        {"conteudo": b"%PDF-1.4 luz" * 80, "nome": "luz.pdf",
         "tipo": "COMPROVANTE-ENDERECO-PESSOA"}])
    s.commit()
    d = como(app_real, mundo["dp"].id).get(
        f"/erp/api/dp/solicitacoes/{p['id']}").get_json()
    papeis = d.get("papeis") or (d.get("solicitacao") or {}).get("papeis") or []
    assert len(papeis) == 3, d.keys()
    for x in papeis:
        r = como(app_real, mundo["dp"].id).get(f"/erp/anexo/{x['anexo_id']}")
        assert r.status_code == 200, (x, r.get_data(as_text=True)[:200])
