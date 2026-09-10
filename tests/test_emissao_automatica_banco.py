"""A nota que sai sozinha — com banco de verdade.

Item 6 de `MEDICOES_E_NOTAS.md`, destravado quando o dono tirou Petrolina da
conta em 10/09/2026. O que sobrou — a BWS no Eusébio — já tinha inscrição,
token, controle de numeração e certificado digital.

⚠️ A PREFEITURA NÃO É CHAMADA AQUI. A saída de internet do ambiente de
desenvolvimento é filtrada, e mesmo que não fosse, uma suíte que emite nota de
verdade a cada execução seria um desastre. O serviço do município é DUBLADO.

O que se prova é o que erra na prática:

  1. A conferência acusa cadastro incompleto ANTES de qualquer número ser
     tomado — número de nota queimado por falta de CNO é dano evitável.
  2. O código IBGE da OBRA vai no local da prestação e o da EMPRESA no local
     de emissão. Trocar os dois manda o ISS para o município errado.
  3. As retenções calculadas pela obra chegam à declaração nos campos certos.
  4. A declaração sai ASSINADA e com o Id no formato que o padrão exige.
  5. Emitindo, o número da declaração é NOSSO e o da nota vem da prefeitura.
  6. Falhando, o número fica QUEIMADO com o motivo, e o próximo é outro — não
     se recicla: a prefeitura pode ter recebido e só a resposta ter se perdido.
  7. A fila NÃO repete a emissão sozinha, nem quando o serviço morre no meio.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest

from app.apps.erp.core.auth.service import gerar_hash
from app.apps.erp.core.comum import tarefas, trabalhos
from app.apps.erp.core.comum.auditoria import ErroValidacao
from app.apps.erp.core.notas_emitidas import automatica, numeracao
from app.apps.erp.db.models.cadastros import (Categoria, Empresa, Fornecedor, Obra,
                                              PerfilUsuario as P, RegimeTributario,
                                              TipoPessoa, Usuario)
from app.apps.erp.db.models.financeiro import (EspecieTitulo, FormaPagamento,
                                               NotaEmitida, Parcela, Rateio,
                                               StatusParcela, StatusTitulo, Tarefa,
                                               TipoTitulo, Titulo)
from tests.conftest import como

pytestmark = pytest.mark.banco

CHAVE_TESTE = "wsHQ6Rl6nJcU8mQVzD3s7pQ1nR2tYv0aBcDeFgHiJkL="
SENHA_PFX = "senha-do-certificado"
CNPJ_BWS = "11222333000181"
IBGE_EUSEBIO = "2304285"
IBGE_RECIFE = "2611606"
# Chave de acesso do padrão nacional: 50 dígitos. O número da nota mora numa
# faixa fixa dela — aqui, 42.
CHAVE_DE_ACESSO = "2304285" + "0" * 20 + "0000000000042" + "0" * 10


def _fabricar_pfx(*, cnpj: str = CNPJ_BWS, dias: int = 200) -> bytes:
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.hazmat.primitives.serialization import pkcs12
    from cryptography.x509.oid import NameOID

    chave = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    nome = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, f"BWS TESTE:{cnpj}"),
                      x509.NameAttribute(NameOID.COUNTRY_NAME, "BR")])
    agora = datetime.now(timezone.utc)
    cert = (x509.CertificateBuilder()
            .subject_name(nome).issuer_name(nome).public_key(chave.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(agora - timedelta(days=1))
            .not_valid_after(agora + timedelta(days=dias))
            .sign(chave, hashes.SHA256()))
    return pkcs12.serialize_key_and_certificates(
        b"teste", chave, cert, None,
        serialization.BestAvailableEncryption(SENHA_PFX.encode()))


@pytest.fixture
def cenario(sessao_real, monkeypatch):
    monkeypatch.setenv("ERP_CHAVE_SEGREDOS", CHAVE_TESTE)
    s = sessao_real
    admin = Usuario(nome="Admin da nota", email="nota.admin@teste.local", ativo=True,
                    senha_hash=gerar_hash("senha-de-teste-123"), perfil=P.ADMIN)
    empresa = Empresa(
        razao_social="BWS CONSTRUCOES LTDA", nome_fantasia="BWS", cnpj=CNPJ_BWS,
        inscricao_municipal="101084492",
        emissao_modo="API", emissao_canal="NACIONAL", emissao_ambiente="HOMOLOGACAO",
        emissao_municipio="Eusébio/CE", emissao_codigo_ibge=IBGE_EUSEBIO,
        emissao_url_base="https://exemplo.invalido/nfse40",
        emissao_serie="1", emissao_codigo_servico="702",
        emissao_aliquota_iss=Decimal("5.0000"))
    cliente = Fornecedor(
        razao_social="SECRETARIA DE EDUCACAO", cnpj_cpf="10572071000112",
        tipo_pessoa=TipoPessoa.PJ, ativo=True, e_cliente=True,
        regime_tributario=RegimeTributario.NAO_INFORMADO,
        municipio="RECIFE", uf="PE", cep="50810900",
        logradouro="AVENIDA AFONSO OLINDENSE", numero="1513",
        bairro="VARZEA", codigo_ibge=IBGE_RECIFE)
    obra = Obra(codigo="CRECHE02", nome="Creche Bloco 02",
                objeto="CONSTRUCAO DE CRECHES", contrato="268/2025",
                cno="90.025.25410/76", codigo_ibge="2601607",
                aliquota_iss_pct=Decimal("5.0000"), iss_retido=True,
                inss_retido=True, pct_servico_inss=Decimal("50"))
    cat = Categoria(codigo="1.1.01", descricao="Receita de obra")
    s.add_all([admin, empresa, cliente, obra, cat])
    s.flush()
    obra.empresa_id = empresa.id
    # O token do canal vai CIFRADO, como em produção — a chave de segredos foi
    # apontada para uma de teste no começo do fixture.
    from app.apps.erp.core.comum.segredos import cifrar
    empresa.emissao_token_cifrado = cifrar("token-de-teste-do-canal")
    s.flush()

    from app.apps.erp.core.cadastros import certificado as svc_cert
    svc_cert.guardar(s, empresa.id, _fabricar_pfx(), SENHA_PFX, usuario=admin)
    s.flush()

    titulo = Titulo(
        numero_sp="SP-90001", tipo=TipoTitulo.T1_MATERIAL_NFE,
        especie=EspecieTitulo.RECEBER, fornecedor_id=cliente.id,
        descricao="Pagamento da 10a medicao", numero_medicao="10",
        valor_bruto=Decimal("98720.04"), valor_liquido=Decimal("98720.04"),
        competencia=date(2026, 6, 1), categoria_id=cat.id,
        forma_pagamento=FormaPagamento.TED, status=StatusTitulo.APROVADO,
        solicitante_id=admin.id)
    s.add(titulo)
    s.flush()
    s.add(Parcela(titulo_id=titulo.id, numero=1, vencimento=date(2026, 7, 10),
                  valor=Decimal("98720.04"), status=StatusParcela.ABERTA))
    s.add(Rateio(titulo_id=titulo.id, obra_id=obra.id,
                 valor=Decimal("98720.04"), percentual=Decimal("100")))
    s.flush()
    return {"s": s, "admin": admin, "empresa": empresa, "cliente": cliente,
            "obra": obra, "titulo": titulo}


class _PrefeituraFalsa:
    """O serviço do município, dublado. Guarda o que recebeu, para conferir."""

    ultima = None

    def __init__(self, **kw):
        self.kw = kw
        _PrefeituraFalsa.ultima = self
        self.enviado = None

    def emitir_e_aguardar(self, dados, timeout_s=120, intervalo_s=5):
        self.enviado = dados
        return {"idDPS": "DPS-XYZ", "chaveAcesso": CHAVE_DE_ACESSO,
                "nfse_xml": "<NFSe><infNFSe>conteudo de teste</infNFSe></NFSe>"}


class _PrefeituraQueRecusa(_PrefeituraFalsa):
    def emitir_e_aguardar(self, dados, timeout_s=120, intervalo_s=5):
        raise RuntimeError("Rejeitada (HTTP 400): E123 - Inscrição municipal inválida")


def _dublar_prefeitura(monkeypatch, classe=_PrefeituraFalsa):
    monkeypatch.setattr("app.apps.emissaonf.el_nfse_nacional.ELNfseNacional", classe)
    return classe


# ---------------------------------------------------------------------------
# 1. A CONFERÊNCIA, ANTES DE QUALQUER NÚMERO
# ---------------------------------------------------------------------------
def test_cadastro_completo_libera_a_emissao(cenario):
    c = automatica.conferir(cenario["s"], cenario["titulo"].id)
    assert c["pode"] is True, c["faltas"]
    assert c["proximo_numero"] == 1
    assert c["ambiente"] == "HOMOLOGACAO"


def test_obra_sem_codigo_ibge_e_barrada_com_o_motivo(cenario):
    """O ISS é devido no local da OBRA. Sem o código, a prefeitura não tem
    como saber onde o serviço foi prestado."""
    cenario["obra"].codigo_ibge = None
    cenario["s"].flush()
    c = automatica.conferir(cenario["s"], cenario["titulo"].id)
    assert c["pode"] is False
    assert any("código IBGE" in f for f in c["faltas"])


def test_obra_sem_cno_e_barrada(cenario):
    cenario["obra"].cno = None
    cenario["s"].flush()
    c = automatica.conferir(cenario["s"], cenario["titulo"].id)
    assert any("CNO" in f for f in c["faltas"])


def test_cliente_sem_endereco_e_barrado_dizendo_o_que_falta(cenario):
    cenario["cliente"].cep = None
    cenario["cliente"].logradouro = None
    cenario["s"].flush()
    c = automatica.conferir(cenario["s"], cenario["titulo"].id)
    assert c["pode"] is False
    falta = next(f for f in c["faltas"] if "cliente" in f.lower())
    assert "CEP" in falta and "logradouro" in falta
    assert "Receita" in falta, "a tela tem de dizer COMO resolver"


def test_empresa_em_modo_manual_nao_emite_sozinha(cenario):
    cenario["empresa"].emissao_modo = "MANUAL"
    cenario["s"].flush()
    c = automatica.conferir(cenario["s"], cenario["titulo"].id)
    assert c["pode"] is False
    assert any("MANUAL" in f for f in c["faltas"])


def test_sem_certificado_nao_emite(cenario):
    from app.apps.erp.db.models.cadastros import EmpresaCertificado
    for cert in cenario["s"].query(EmpresaCertificado).all():
        cert.situacao = "SUBSTITUIDO"
    cenario["s"].flush()
    c = automatica.conferir(cenario["s"], cenario["titulo"].id)
    assert any("certificado" in f.lower() for f in c["faltas"])


def test_titulo_que_nao_e_medicao_e_recusado(cenario):
    cenario["titulo"].numero_medicao = None
    cenario["s"].flush()
    with pytest.raises(ErroValidacao) as e:
        automatica.conferir(cenario["s"], cenario["titulo"].id)
    assert "medição" in str(e.value)


# ---------------------------------------------------------------------------
# 2 e 3. O QUE VAI NA DECLARAÇÃO
# ---------------------------------------------------------------------------
def test_o_ibge_da_obra_vai_no_local_da_prestacao_e_o_da_empresa_no_de_emissao(cenario):
    """Trocar os dois manda o ISS para o município errado — e o erro só
    aparece quando a prefeitura cobra."""
    d = automatica.montar_declaracao(cenario["s"], cenario["titulo"].id, numero_dps=7)
    assert d.c_loc_prestacao == 2601607, "local da prestação é o município da OBRA"
    assert d.c_loc_emi == int(IBGE_EUSEBIO), "local de emissão é o da EMPRESA"


def test_o_prestador_e_a_empresa_da_obra_nunca_uma_constante(cenario):
    d = automatica.montar_declaracao(cenario["s"], cenario["titulo"].id, numero_dps=7)
    assert d.prest_cnpj == CNPJ_BWS
    assert d.prest_im == "101084492"


def test_o_tomador_e_o_cliente_com_endereco_completo(cenario):
    d = automatica.montar_declaracao(cenario["s"], cenario["titulo"].id, numero_dps=7)
    assert d.toma_doc == "10572071000112"
    assert d.toma_cmun == int(IBGE_RECIFE)
    assert (d.toma_cep, d.toma_nro, d.toma_bairro) == ("50810900", "1513", "VARZEA")


def test_as_retencoes_da_obra_chegam_aos_campos_certos(cenario):
    from app.apps.erp.core.titulos import tributacao
    calculo = tributacao.calcular(cenario["obra"], Decimal("98720.04"))
    esperado = {r.tipo: r.valor for r in calculo.retencoes}

    d = automatica.montar_declaracao(cenario["s"], cenario["titulo"].id, numero_dps=7)
    assert Decimal(d.v_ret_inss) == esperado.get("INSS", Decimal("0.00"))
    assert Decimal(d.v_ret_irrf) == esperado.get("IRRF", Decimal("0.00"))
    assert d.p_aliq == "5.00"
    assert d.tp_ret_issqn == 1, "a obra está com ISS retido na fonte"


def test_a_discriminacao_diz_medicao_contrato_e_cno(cenario):
    d = automatica.montar_declaracao(cenario["s"], cenario["titulo"].id,
                                     numero_dps=7, observacao="EMPENHO 2026NE000123")
    assert "MEDICAO 10" in d.x_desc_serv
    assert "CONTRATO 268/2025" in d.x_desc_serv
    assert "CNO 90.025.25410/76" in d.x_desc_serv
    assert "EMPENHO 2026NE000123" in d.x_desc_serv


def test_valor_zero_e_recusado(cenario):
    with pytest.raises(ErroValidacao):
        automatica.montar_declaracao(cenario["s"], cenario["titulo"].id,
                                     numero_dps=7, valor="0")


# ---------------------------------------------------------------------------
# 4. A DECLARAÇÃO SAI ASSINADA
# ---------------------------------------------------------------------------
def test_a_declaracao_e_assinada_com_o_certificado_da_empresa(cenario):
    """Sem assinatura o padrão nacional recusa. E o .pfx não pode virar arquivo
    em disco no caminho — por isso a chave sai em memória."""
    from lxml import etree

    from app.apps.emissaonf.el_nfse_nacional import assinar_dps, montar_dps_xml
    from app.apps.erp.core.cadastros.certificado import chave_e_certificado_pem

    d = automatica.montar_declaracao(cenario["s"], cenario["titulo"].id, numero_dps=7)
    chave_pem, cert_pem = chave_e_certificado_pem(cenario["s"], cenario["empresa"].id)
    assinado = assinar_dps(montar_dps_xml(d), chave_pem, cert_pem)
    xml = etree.tostring(assinado).decode()
    assert "Signature" in xml
    inf = assinado.find("{http://www.sped.fazenda.gov.br/nfse}infDPS")
    assert inf.get("Id").startswith("DPS"), "o Id da DPS segue o formato do padrão"


# ---------------------------------------------------------------------------
# 5 e 6. EMITIR — DANDO CERTO E DANDO ERRADO
# ---------------------------------------------------------------------------
def test_emitir_guarda_o_numero_da_prefeitura_e_o_xml(cenario, monkeypatch):
    _dublar_prefeitura(monkeypatch)
    s = cenario["s"]
    r = automatica.emitir(s, cenario["titulo"].id, usuario=cenario["admin"])

    nota = s.get(NotaEmitida, r["nota_id"])
    assert nota.situacao == "EMITIDA"
    assert nota.numero_dps == 1, "a numeração da declaração é NOSSA"
    assert nota.numero_nota == "42", "o número da nota vem da prefeitura"
    assert nota.chave_acesso == CHAVE_DE_ACESSO
    assert nota.id_dps == "DPS-XYZ"
    assert nota.modo == "API"
    assert nota.anexo_id is not None, "o XML da nota fica anexado ao título"


def test_o_que_foi_enviado_e_o_que_o_cadastro_manda(cenario, monkeypatch):
    _dublar_prefeitura(monkeypatch)
    automatica.emitir(cenario["s"], cenario["titulo"].id,
                      valor="50000.00", observacao="PARCIAL",
                      usuario=cenario["admin"])
    enviado = _PrefeituraFalsa.ultima.enviado
    assert enviado.v_serv == "50000.00", "o valor parcial tem de chegar lá"
    assert "PARCIAL" in enviado.x_desc_serv
    assert enviado.tp_amb == 2, "homologação é ambiente 2"


def test_falha_queima_o_numero_com_motivo_e_nao_recicla(cenario, monkeypatch):
    """A prefeitura pode ter recebido a declaração e só a resposta ter se
    perdido. Reemitir com o mesmo número daria duplicidade do lado dela."""
    _dublar_prefeitura(monkeypatch, _PrefeituraQueRecusa)
    s = cenario["s"]
    with pytest.raises(ErroValidacao) as e:
        automatica.emitir(s, cenario["titulo"].id, usuario=cenario["admin"])
    assert "queimado" in str(e.value)
    # A recusa DA PREFEITURA vem em português e é útil: passa inteira para a
    # tela. O que não pode chegar lá é erro de biblioteca.
    assert "recusou a declaração" in str(e.value)
    assert "Inscrição municipal inválida" in str(e.value)

    queimada = s.query(NotaEmitida).filter(NotaEmitida.numero_dps == 1).one()
    assert queimada.situacao == "FALHADA"
    assert "Inscrição municipal inválida" in (queimada.motivo or "")

    # a próxima emissão pega o número SEGUINTE
    _dublar_prefeitura(monkeypatch)
    r = automatica.emitir(s, cenario["titulo"].id, usuario=cenario["admin"])
    assert s.get(NotaEmitida, r["nota_id"]).numero_dps == 2


def test_erro_de_rede_vira_frase_em_portugues_na_tela(cenario, monkeypatch):
    """Despejar "ProxyError: Max retries exceeded" na cara de quem está
    faturando não ajuda ninguém a decidir o que fazer."""
    class _SemRede(_PrefeituraFalsa):
        def emitir_e_aguardar(self, dados, timeout_s=120, intervalo_s=5):
            raise RuntimeError("HTTPSConnectionPool(host='x', port=443): "
                               "Max retries exceeded (Caused by ProxyError(...))")

    _dublar_prefeitura(monkeypatch, _SemRede)
    s = cenario["s"]
    with pytest.raises(ErroValidacao) as e:
        automatica.emitir(s, cenario["titulo"].id, usuario=cenario["admin"])
    assert "não alcançou o endereço" in str(e.value)
    assert "ProxyError" not in str(e.value)
    # ...mas o texto técnico fica GUARDADO: é o que se manda para o suporte.
    queimada = s.query(NotaEmitida).filter(NotaEmitida.numero_dps == 1).one()
    assert "ProxyError" in (queimada.motivo or "")


def test_emitir_com_cadastro_incompleto_nao_toma_numero(cenario, monkeypatch):
    _dublar_prefeitura(monkeypatch)
    s = cenario["s"]
    cenario["obra"].cno = None
    s.flush()
    with pytest.raises(ErroValidacao):
        automatica.emitir(s, cenario["titulo"].id, usuario=cenario["admin"])
    assert s.query(NotaEmitida).count() == 0, "nenhum número pode ter sido tomado"


# ---------------------------------------------------------------------------
# 7. A FILA NÃO REPETE EMISSÃO
# ---------------------------------------------------------------------------
def test_a_emissao_nao_se_repete_sozinha_quando_falha(cenario, monkeypatch):
    """Repetir criaria duas notas de verdade na prefeitura — e desfazer isso é
    cancelamento com justificativa, não um clique."""
    _dublar_prefeitura(monkeypatch, _PrefeituraQueRecusa)
    trabalhos.registrar_todos()
    s = cenario["s"]
    t = tarefas.enfileirar(s, "emitir_nota", {"titulo_id": cenario["titulo"].id,
                                              "usuario_id": cenario["admin"].id},
                           rotulo="Emitir a nota da medição 10", usuario=cenario["admin"])
    depois = tarefas.executar_agora(s, t.id)
    assert depois.situacao == "FALHADA", "não pode voltar para a fila"
    assert depois.tentativas == 1


def test_emissao_interrompida_pelo_reinicio_nao_e_refeita_sozinha(cenario):
    """Pode ter chegado à prefeitura antes de o serviço morrer."""
    trabalhos.registrar_todos()
    s = cenario["s"]
    t = tarefas.enfileirar(s, "emitir_nota", {"titulo_id": cenario["titulo"].id},
                           rotulo="Emitir", usuario=cenario["admin"])
    t.situacao = "EXECUTANDO"
    t.batida_em = datetime.now(timezone.utc) - timedelta(hours=1)
    s.flush()
    tarefas.recuperar_orfas(s)
    s.flush()
    depois = s.get(Tarefa, t.id)
    assert depois.situacao == "FALHADA"
    assert "confira antes de pedir de novo" in (depois.erro or "").lower()


# ---------------------------------------------------------------------------
# 8. AS ROTAS
# ---------------------------------------------------------------------------
def test_a_rota_enfileira_em_vez_de_emitir_na_hora(app_real, cenario, monkeypatch):
    _dublar_prefeitura(monkeypatch)
    r = como(app_real, cenario["admin"].id).post(
        f"/erp/api/medicoes/{cenario['titulo'].id}/emitir-automatico", json={})
    assert r.status_code == 200
    t = r.get_json()["tarefa"]
    assert t["situacao"] == "PENDENTE"
    assert "medição 10" in t["rotulo"]
    assert cenario["s"].query(NotaEmitida).count() == 0, "nada pode ter sido emitido no clique"


def test_a_rota_recusa_com_a_lista_do_que_falta(app_real, cenario):
    cenario["obra"].cno = None
    cenario["obra"].codigo_ibge = None
    cenario["s"].flush()
    r = como(app_real, cenario["admin"].id).post(
        f"/erp/api/medicoes/{cenario['titulo'].id}/emitir-automatico", json={})
    assert r.status_code == 400
    faltas = r.get_json()["faltas"]
    assert any("CNO" in f for f in faltas)
    assert any("IBGE" in f for f in faltas)


def test_a_conferencia_tem_rota_propria_que_nao_emite(app_real, cenario):
    r = como(app_real, cenario["admin"].id).get(
        f"/erp/api/medicoes/{cenario['titulo'].id}/emitir-automatico")
    assert r.status_code == 200
    assert r.get_json()["conferencia"]["pode"] is True
    assert cenario["s"].query(NotaEmitida).count() == 0
