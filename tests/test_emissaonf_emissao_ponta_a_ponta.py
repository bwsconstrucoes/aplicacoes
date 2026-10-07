# -*- coding: utf-8 -*-
"""
O caminho INTEIRO da emissão, do clique até a página de resultado — sem
prefeitura e sem nota fiscal.

Por que isto existe: o ensaio em homologação é o primeiro passo de qualquer
mudança na emissão, e cada tentativa dele custa uma ida e volta do dono. Um erro
de ligação (um campo com nome trocado, um argumento fora de ordem) só aparecia
quando ele clicava. Aqui o caminho roda de verdade:

  - a nota é calculada pelo motor fiscal real;
  - a declaração é montada e **assinada de verdade**, com um certificado
    descartável criado no próprio teste;
  - a prefeitura é dublada: recebe a declaração, confere que ela chegou
    compactada como o manual manda, e devolve a nota;
  - o pós-emissão é dublado, porque ele escreve em planilha, Omie, card e Drive.

O que isto NÃO prova: que a prefeitura de verdade aceita a declaração. Só a
primeira emissão real prova isso.
"""
import base64
import gzip
import os
import sys

import pytest

_EMISSAONF = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                          "app", "apps", "emissaonf")
if _EMISSAONF not in sys.path:
    sys.path.insert(0, _EMISSAONF)

from app.main import app as app_real          # noqa: E402
import el_nfse_nacional as nac                # noqa: E402
import nfse_exemplo                            # noqa: E402

from test_emissaonf_dps import ObraFalsa, _dados_rps, _calculo   # noqa: E402

TOKEN = "TOKEN-DE-TESTE"
CARD = "1447316614"


# --------------------------------------------------------------------------- #
# Um certificado descartável, criado aqui: assinar de verdade é parte do caminho
# --------------------------------------------------------------------------- #
@pytest.fixture(scope="module")
def certificado():
    from datetime import datetime, timedelta, timezone
    from cryptography import x509
    from cryptography.x509.oid import NameOID
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa

    chave = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    nome = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "BWS TESTE")])
    agora = datetime.now(timezone.utc)
    cert = (x509.CertificateBuilder()
            .subject_name(nome).issuer_name(nome)
            .public_key(chave.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(agora - timedelta(days=1))
            .not_valid_after(agora + timedelta(days=30))
            .sign(chave, hashes.SHA256()))
    chave_pem = chave.private_bytes(serialization.Encoding.PEM,
                                    serialization.PrivateFormat.TraditionalOpenSSL,
                                    serialization.NoEncryption())
    return chave_pem, cert.public_bytes(serialization.Encoding.PEM)


def _card():
    """Card do Pipefy com o mínimo que a validação exige para liberar a emissão."""
    return {
        "card_id": CARD, "codigo_obra": "CREPEEXU", "numero_medicao": "10",
        "valor_medicao": "98720.04", "bdi": "0", "emissao_nf": "Sim",
        "tipo_medicao": "NORMAL", "banco": "Bradesco - Agência: 624-6 - Conta Corrente: 7011-4",
        "periodo_ini": "01/09/2026", "periodo_fim": "30/09/2026",
        "objeto": "CONSTRUCAO DE CRECHES", "contrato": "268/2025",
        "cnpj_contratante": "10572071000112", "contratante": "SECRETARIA DE EDUCACAO",
        "campos_raw": [], "campos_por_id": {}, "omie_integracao": "",
        "tipo_documento": "", "empenho": "",
    }


class _GoogleFalso:
    """Só o suficiente para o código chegar na planilha sem rede."""

    def open_by_key(self, _k):
        return object()


@pytest.fixture
def cenario(monkeypatch, certificado):
    """Liga a tela num mundo onde a prefeitura é dublada e nada é gravado."""
    chave_pem, cert_pem = certificado
    import web
    import emitir_dps

    r = _calculo()
    ctx = {"card": _card(), "obra": ObraFalsa(), "r": r, "ibge": 2601607,
           "prox": 3084, "ultimo": 3083, "dados_rps": _dados_rps(), "avisos": [],
           "end_tom": {}, "xml": "", "assinado": True,
           "chave_pem": chave_pem, "cert_pem": cert_pem, "senha_cert": "x",
           "gc": _GoogleFalso(), "cred": {"PIPEFY_TOKEN": "x",
                                          "EL_NFSE_TOKEN": "token-da-prefeitura"}}
    monkeypatch.setattr(web._worker, "preparar", lambda *a, **k: dict(ctx))

    # o espelho visual lê a planilha; aqui ele não é o que se testa
    monkeypatch.setattr(web._preview, "montar_preview_html", lambda *a, **k: "<i>espelho</i>")

    enviados = {}

    class SessaoDaPrefeitura:
        """Recebe a declaração, confere a embalagem e devolve a nota."""

        def mount(self, *a, **k):
            pass

        def request(self, metodo, url, **kw):
            enviados["url"] = url
            if metodo == "POST":
                b64 = kw["json"]["dpsXmlGZipB64"]
                # o manual exige GZip + Base64; se não for isso, isto estoura
                enviados["dps"] = gzip.decompress(base64.b64decode(b64)).decode("utf-8")
                return _Resposta({"idDPS": enviados.get("id_dps", "DPS-DE-TESTE")})
            if enviados.get("modo") == "recusa":
                # a plataforma nacional recusou: resposta definitiva, sem nota
                return _Resposta({"idDPS": enviados.get("id_dps", ""), "erros": [
                    {"Codigo": "E0037",
                     "Descricao": "O código do município emissor informado na DPS é "
                                  "inexistente no cadastro de convênio municipal"}]})
            if enviados.get("modo") == "processando":
                # a fila da prefeitura ainda não terminou — foi o caso da nota 3281
                return _Resposta({"nfseXmlGZipB64": "<em processamento no ambiente nacional>"})
            from lxml import etree
            dps = etree.fromstring(enviados["dps"].encode("utf-8"))
            xml_nac = nfse_exemplo.como_texto(dps, numero_nfse="3084")
            enviados["nfse"] = xml_nac
            return _Resposta({
                "chaveAcesso": "2" * 50,
                "nfseXmlGZipB64": base64.b64encode(
                    gzip.compress(xml_nac.encode("utf-8"))).decode("ascii"),
            })

    class _Resposta:
        def __init__(self, corpo):
            self.corpo, self.status_code, self.text = corpo, 200, "{}"

        def json(self):
            return self.corpo

    monkeypatch.setattr(nac.ELNfseNacional, "__init__",
                        lambda self, token, chave_pem, cert_pem, ambiente="homologacao",
                        urlbase=nac.URLBASE_EUSEBIO, timeout=60: (
                            setattr(self, "token", token),
                            setattr(self, "chave_pem", chave_pem),
                            setattr(self, "cert_pem", cert_pem),
                            setattr(self, "ambiente", ambiente),
                            setattr(self, "urlbase", urlbase.rstrip("/")),
                            setattr(self, "timeout", timeout),
                            setattr(self, "session", SessaoDaPrefeitura()),
                            None)[-1])
    monkeypatch.setattr(emitir_dps, "ESPERA_ENTRE_CONSULTAS_S", 0)

    monkeypatch.setenv("EMISSAO_NF_TOKEN", TOKEN)
    monkeypatch.setenv("EL_NFSE_TOKEN", "token-da-prefeitura")
    return app_real.test_client(), enviados


def _emitir(cliente, **extra):
    dados = {"card_id": CARD, "token": TOKEN, "confirmo": "on",
             "discriminacao": "PAGAMENTO DA 10a MEDICAO DA OBRA, CONTRATO 268/2025."}
    dados.update(extra)
    return cliente.post("/emissao/emitir", data=dados, follow_redirects=True)


# --------------------------------------------------------------------------- #
# O ensaio — o primeiro passo de qualquer mudança na emissão
# --------------------------------------------------------------------------- #
def test_o_ensaio_vai_ate_o_fim_e_diz_que_nao_vale_como_nota(cenario):
    cliente, enviados = cenario
    r = _emitir(cliente, ensaio="on")
    assert r.status_code == 200
    corpo = r.get_data(as_text=True)
    assert "Ensaio em homologação" in corpo
    assert "não vale como documento fiscal" in corpo
    assert "3084" in corpo


def test_o_ensaio_vai_para_o_ambiente_de_homologacao(cenario):
    cliente, enviados = cenario
    _emitir(cliente, ensaio="on")
    assert "/api/nacional/homologacao/" in enviados["url"]
    assert "<tpAmb>2</tpAmb>" in enviados["dps"]


def test_a_emissao_de_verdade_vai_para_producao(cenario):
    cliente, enviados = cenario
    _emitir(cliente)
    assert "/api/nacional/nfse" in enviados["url"]
    assert "/homologacao/" not in enviados["url"]
    assert "<tpAmb>1</tpAmb>" in enviados["dps"]


# --------------------------------------------------------------------------- #
# O que chega à prefeitura
# --------------------------------------------------------------------------- #
def test_a_declaracao_chega_assinada(cenario):
    """Sem assinatura a prefeitura recusa — e a assinatura é feita com o
    certificado da empresa, no caminho de verdade."""
    cliente, enviados = cenario
    _emitir(cliente, ensaio="on")
    assert "<Signature" in enviados["dps"] or "SignatureValue" in enviados["dps"]


def test_a_discriminacao_editada_na_tela_e_a_que_vai_na_nota(cenario):
    """O corpo da nota é o que a pessoa conferiu na tela, não o que o card
    trazia — é o ponto de toda a tela de emissão."""
    cliente, enviados = cenario
    _emitir(cliente, ensaio="on", discriminacao="TEXTO QUE A PESSOA CONFERIU NA TELA")
    assert "TEXTO QUE A PESSOA CONFERIU NA TELA" in enviados["dps"]


def test_a_deducao_de_material_chega_na_declaracao(cenario):
    """A causa do ISS a maior em setembro/2026, agora vigiada no caminho real."""
    cliente, enviados = cenario
    _emitir(cliente, ensaio="on")
    assert "<vDedRed>" in enviados["dps"]


def test_o_iss_chega_como_retido_pelo_tomador(cenario):
    """1 e 2 têm significados invertidos em relação à intuição; aqui se confere
    o que de fato sai pela rede."""
    cliente, enviados = cenario
    _emitir(cliente, ensaio="on")
    assert "<tpRetISSQN>2</tpRetISSQN>" in enviados["dps"]


# --------------------------------------------------------------------------- #
# O que a tela faz com a resposta
# --------------------------------------------------------------------------- #
def test_a_chave_de_acesso_aparece_na_tela_de_resultado(cenario):
    """Ela substituiu o código de verificação: é por ela que o cliente consulta
    a nota."""
    cliente, _ = cenario
    corpo = _emitir(cliente, ensaio="on").get_data(as_text=True)
    assert "Chave de acesso nacional" in corpo


def test_sem_o_token_da_prefeitura_a_tela_ensina_onde_conseguir(cenario, monkeypatch):
    cliente, _ = cenario
    for nome in ("EL_NFSE_TOKEN", "EL_TOKEN", "NFSE_TOKEN", "TOKEN_PREFEITURA",
                 "TOKEN_NFSE", "EMISSAO_NF_EL_TOKEN"):
        monkeypatch.delenv(nome, raising=False)
    # O módulo é carregado DUAS vezes, com dois nomes: "web" (import plano, como
    # os módulos desta pasta se importam) e "app.apps.emissaonf.web" (o pacote,
    # que é de onde o Flask registra o blueprint). Quem atende a requisição é o
    # segundo — então é nele que se troca a função.
    servindo = sys.modules["app.apps.emissaonf.web"]
    monkeypatch.setattr(servindo, "_token_prefeitura", lambda cred: "")
    corpo = _emitir(cliente, ensaio="on").get_data(as_text=True)
    assert "APIs de Integração" in corpo
    assert "CERTIFICADO" in corpo        # explica por que antes não precisava


def test_sem_marcar_a_confirmacao_nada_e_enviado(cenario):
    cliente, enviados = cenario
    corpo = cliente.post("/emissao/emitir",
                         data={"card_id": CARD, "token": TOKEN,
                               "discriminacao": "x"}).get_data(as_text=True)
    assert "marcar a confirmação" in corpo
    assert "dps" not in enviados


def test_substituir_pela_tela_explica_o_caminho_do_portal(cenario):
    """Deixou de funcionar com o modelo novo; a tela tem de dizer o que fazer em
    vez de deixar tentar e falhar."""
    cliente, enviados = cenario
    corpo = _emitir(cliente, ensaio="on", nota_substituida="3070").get_data(as_text=True)
    assert "portal" in corpo and "recuperar" in corpo
    assert "dps" not in enviados


# --------------------------------------------------------------------------- #
# Quando a prefeitura aceita e a nota não fica pronta na hora
# --------------------------------------------------------------------------- #
# Aconteceu de verdade em 07/10/2026, na nota 3281: a prefeitura aceitou a
# declaração e ainda estava processando quando a espera acabou. É o único aperto
# real desta área, porque a nota PODE existir — e emitir de novo criaria a
# segunda nota do mesmo serviço.

ID_DPS_3281 = "DPS230428520007952600010900001260000000003281"


def test_a_identificacao_da_declaracao_diz_de_que_nota_se_trata():
    """Ler 45 dígitos à mão é pedir erro; o número sai de dentro deles."""
    import emitir_dps
    assert emitir_dps.numero_da_declaracao(ID_DPS_3281) == ("3281", "2026")


def test_identificacao_curta_ou_vazia_nao_estoura():
    import emitir_dps
    assert emitir_dps.numero_da_declaracao("") == ("", "")
    assert emitir_dps.numero_da_declaracao("DPS123") == ("", "")


def test_quando_a_espera_estoura_a_mensagem_manda_conferir_e_proibe_reenviar(cenario, monkeypatch):
    cliente, enviados = cenario
    import emitir_dps
    enviados["modo"] = "processando"
    enviados["id_dps"] = ID_DPS_3281
    monkeypatch.setattr(emitir_dps, "ESPERA_TOTAL_S", 0)
    corpo = _emitir(cliente).get_data(as_text=True)

    assert "NÃO EMITA DE NOVO" in corpo
    assert "3281" in corpo                      # diz de que nota se trata
    assert "Conferir declaração" in corpo       # manda para a tela que RESOLVE
    assert ID_DPS_3281 in corpo                 # e dá o que colar lá


def test_a_tela_de_conferir_declaracao_avisa_para_nao_emitir(cenario):
    cliente, _ = cenario
    corpo = cliente.get(f"/emissao/declaracao?token={TOKEN}").get_data(as_text=True)
    assert "não emita de novo" in corpo
    assert "não se apaga" in corpo


def test_a_tela_de_conferir_mostra_de_que_nota_se_trata(cenario):
    cliente, _ = cenario
    corpo = cliente.get(
        f"/emissao/declaracao?token={TOKEN}&id_dps={ID_DPS_3281}").get_data(as_text=True)
    assert "nota 3281" in corpo


def test_identificacao_fora_do_padrao_e_recusada_com_explicacao(cenario):
    cliente, _ = cenario
    corpo = cliente.post("/emissao/declaracao",
                         data={"token": TOKEN, "id_dps": "12345"}).get_data(as_text=True)
    assert "começa com DPS" in corpo


def test_conferir_a_declaracao_termina_o_servico_quando_a_nota_saiu(cenario, monkeypatch):
    """O que fecha o aperto: a nota saiu, e o pós-emissão roda agora — sem emitir
    nada de novo."""
    cliente, enviados = cenario
    import emitir_dps

    r = _calculo()
    from lxml import etree
    import montar_dps
    dps = nac.montar_dps_xml(montar_dps.montar(
        card=_card(), obra=ObraFalsa(), r=r, dados_rps=_dados_rps(), numero_nota=3281,
        ibge_obra=2601607, data_emissao="2026-10-07", producao=True))
    xml_nac = nfse_exemplo.como_texto(dps, numero_nfse="3281")

    monkeypatch.setattr(emitir_dps, "consultar",
                        lambda ctx, id_dps, token, producao: emitir_dps.dados_da_nota(xml_nac))
    feito = {}
    servindo = sys.modules["app.apps.emissaonf.web"]
    monkeypatch.setattr(servindo._concluir, "concluir",
                        lambda *a, **k: feito.update(numero=a[1], nacional=k.get("nacional"),
                                                     chave=k.get("chave_nacional")))

    r2 = cliente.post("/emissao/declaracao",
                      data={"token": TOKEN, "id_dps": ID_DPS_3281, "card_id": CARD},
                      follow_redirects=True)
    assert r2.status_code == 200
    assert feito["numero"] == "3281"
    assert feito["nacional"] is True
    assert len(feito["chave"]) == 50
    assert "3281" in r2.get_data(as_text=True)


def test_sem_o_card_a_tela_diz_que_a_nota_saiu_mas_falta_terminar(cenario, monkeypatch):
    cliente, _ = cenario
    import emitir_dps
    servindo = sys.modules["app.apps.emissaonf.web"]
    monkeypatch.setattr(servindo, "_ctx_minimo", lambda: {"cred": {}, "chave_pem": b"x",
                                                          "cert_pem": b"x", "gc": None})
    monkeypatch.setattr(emitir_dps, "consultar", lambda *a, **k: {
        "numero": "3281", "chave": "2" * 50, "data_iso": "2026-10-07", "xml_nacional": "<x/>"})
    corpo = cliente.post("/emissao/declaracao",
                         data={"token": TOKEN, "id_dps": ID_DPS_3281}).get_data(as_text=True)
    assert "A nota SAIU" in corpo
    assert "Falta terminar o serviço" in corpo


def test_quando_ainda_esta_processando_a_tela_diz_para_esperar(cenario, monkeypatch):
    cliente, _ = cenario
    import emitir_dps

    def ainda(*a, **k):
        raise emitir_dps.AindaProcessando(
            "segue na fila da prefeitura\n\n>>> Isto NÃO é erro, e NÃO autoriza emitir de novo.")

    servindo = sys.modules["app.apps.emissaonf.web"]
    monkeypatch.setattr(servindo, "_ctx_minimo", lambda: {"cred": {}, "chave_pem": b"x",
                                                          "cert_pem": b"x", "gc": None})
    monkeypatch.setattr(emitir_dps, "consultar", ainda)
    corpo = cliente.post("/emissao/declaracao",
                         data={"token": TOKEN, "id_dps": ID_DPS_3281}).get_data(as_text=True)
    assert "ainda NÃO ficou pronta" in corpo
    assert "NÃO autoriza emitir de novo" in corpo


def test_a_tela_de_emissao_tem_link_para_conferir_declaracao(cenario):
    cliente, _ = cenario
    corpo = cliente.get(f"/emissao/?token={TOKEN}", follow_redirects=True).get_data(as_text=True)
    assert f"/emissao/declaracao?token={TOKEN}" in corpo


# --------------------------------------------------------------------------- #
# O terceiro desfecho: a plataforma RECUSOU a declaração
# --------------------------------------------------------------------------- #
# É o mais tranquilo dos três, e vinha disfarçado de "não consegui consultar".
# O manual é explícito: quando a resposta traz a lista de erros, a solicitação
# NÃO foi processada — e a mesma declaração pode ser reenviada com a correção,
# mantendo a mesma identificação. Ou seja: não existe nota, e reemitir com o
# mesmo número é o caminho previsto, não um risco de nota duplicada.

RECUSA = {"idDPS": ID_DPS_3281, "erros": [
    {"Codigo": "E0037", "Descricao": "O código do município emissor informado na DPS é "
                                     "inexistente no cadastro de convênio municipal"},
]}


def test_a_recusa_e_lida_como_recusa_e_nao_como_falha_de_consulta():
    import el_nfse_nacional as n
    assert n.ELNfseNacional.erros_da_resposta(RECUSA) == [
        "E0037 - O código do município emissor informado na DPS é inexistente no "
        "cadastro de convênio municipal"]
    assert n.ELNfseNacional.erros_da_resposta({"chaveAcesso": "x"}) == []
    assert n.ELNfseNacional.erros_da_resposta(None) == []


def test_a_emissao_para_na_hora_quando_a_declaracao_e_recusada(cenario, monkeypatch):
    """Antes a recusa era engolida e a espera rodava inteira — a pessoa esperava
    150s para receber um aviso que não dizia o motivo."""
    cliente, enviados = cenario
    enviados["modo"] = "recusa"
    enviados["id_dps"] = ID_DPS_3281

    import emitir_dps
    import time as _t
    chamadas = {"sleeps": 0}
    monkeypatch.setattr(emitir_dps.time, "sleep",
                        lambda s: chamadas.__setitem__("sleeps", chamadas["sleeps"] + 1))

    corpo = _emitir(cliente).get_data(as_text=True)
    assert "recusou a declaração" in corpo
    assert "Nenhuma nota foi criada" in corpo
    assert "E0037" in corpo                       # o motivo aparece
    assert "mesmo número" in corpo                # e a liberação para reemitir
    assert chamadas["sleeps"] == 0                # não esperou nada


def test_a_tela_de_conferir_diz_que_a_recusa_libera_reemitir(cenario, monkeypatch):
    cliente, _ = cenario
    import emitir_dps

    def recusou(*a, **k):
        raise emitir_dps.DeclaracaoRecusada(["E0037 - municipio sem convenio"],
                                            id_dps=ID_DPS_3281)

    servindo = sys.modules["app.apps.emissaonf.web"]
    monkeypatch.setattr(servindo, "_ctx_minimo", lambda: {"cred": {}, "chave_pem": b"x",
                                                          "cert_pem": b"x", "gc": None})
    monkeypatch.setattr(emitir_dps, "consultar", recusou)
    corpo = cliente.post("/emissao/declaracao",
                         data={"token": TOKEN, "id_dps": ID_DPS_3281}).get_data(as_text=True)
    # as duas rotas que podem cair na recusa usam a MESMA página, de propósito:
    # a mensagem não pode depender de por onde a pessoa chegou
    assert "recusou a declaração" in corpo
    assert "Nenhuma nota foi criada" in corpo
    assert "3281" in corpo
    assert "E0037" in corpo


def test_a_consulta_classifica_os_tres_desfechos(certificado, monkeypatch):
    """Pronta, recusada e ainda processando são três coisas diferentes, e tratá-las
    igual foi o que mandou o dono para a tela errada."""
    import emitir_dps
    chave_pem, cert_pem = certificado
    ctx = {"chave_pem": chave_pem, "cert_pem": cert_pem}

    def responder(payload):
        monkeypatch.setattr(nac.ELNfseNacional, "consultar_processamento_dps",
                            lambda self, id_dps, bruto=False: payload)

    # 1) recusada
    responder(RECUSA)
    with pytest.raises(emitir_dps.DeclaracaoRecusada) as e:
        emitir_dps.consultar(ctx, ID_DPS_3281, "tok", True)
    assert "E0037" in str(e.value)

    # 2) ainda processando
    responder({"nfseXmlGZipB64": "<em processamento no ambiente nacional>"})
    with pytest.raises(emitir_dps.AindaProcessando):
        emitir_dps.consultar(ctx, ID_DPS_3281, "tok", True)

    # 3) pronta
    import base64 as b64, gzip as gz, montar_dps
    dps = nac.montar_dps_xml(montar_dps.montar(
        card=_card(), obra=ObraFalsa(), r=_calculo(), dados_rps=_dados_rps(),
        numero_nota=3281, ibge_obra=2601607, data_emissao="2026-10-07", producao=True))
    xml_nac = nfse_exemplo.como_texto(dps, numero_nfse="3281")
    responder({"chaveAcesso": "2" * 50,
               "nfseXmlGZipB64": b64.b64encode(gz.compress(xml_nac.encode())).decode()})
    assert emitir_dps.consultar(ctx, ID_DPS_3281, "tok", True)["numero"] == "3281"


# --------------------------------------------------------------------------- #
# A tela não pode ficar pendurada
# --------------------------------------------------------------------------- #
# Em 07/10/2026 o dono desistiu do primeiro ensaio porque a tela girou 150s. No
# ensaio isso não compra nada: não há planilha, Omie, card nem Drive para
# completar depois. Então a espera do ensaio é curta, e o que a tela entrega é um
# BOTÃO para conferir — não um identificador de 45 caracteres para copiar à mão.

def test_o_ensaio_espera_muito_menos_que_a_emissao_de_verdade():
    import emitir_dps
    assert emitir_dps.ESPERA_ENSAIO_S < emitir_dps.ESPERA_TOTAL_S


def test_as_duas_esperas_sao_ajustaveis_sem_publicar(monkeypatch):
    """Até se saber como a fila da prefeitura se comporta, é melhor poder mexer
    no número do que adivinhar um bom valor."""
    import importlib
    import emitir_dps
    monkeypatch.setenv("EMISSAO_NF_ESPERA_S", "7")
    monkeypatch.setenv("EMISSAO_NF_ESPERA_ENSAIO_S", "3")
    recarregado = importlib.reload(emitir_dps)
    try:
        assert recarregado.ESPERA_TOTAL_S == 7
        assert recarregado.ESPERA_ENSAIO_S == 3
    finally:
        monkeypatch.undo()
        importlib.reload(emitir_dps)


def test_quando_ainda_processa_a_tela_entrega_um_botao_e_nao_um_codigo_para_copiar(cenario, monkeypatch):
    cliente, enviados = cenario
    enviados["modo"] = "processando"
    enviados["id_dps"] = ID_DPS_3281
    import emitir_dps
    monkeypatch.setattr(emitir_dps, "ESPERA_ENSAIO_S", 0)

    corpo = _emitir(cliente, ensaio="on").get_data(as_text=True)
    assert "ainda está processando" in corpo
    assert "não é erro" in corpo
    # o link já leva a identificação, o card e o ambiente dentro
    assert f"id_dps={ID_DPS_3281}" in corpo
    assert f"card_id={CARD}" in corpo
    assert "ambiente=homologacao" in corpo
    assert "Conferir se a nota saiu" in corpo


def test_no_ensaio_a_tela_diz_que_nao_ha_nada_pendente(cenario, monkeypatch):
    cliente, enviados = cenario
    enviados["modo"] = "processando"
    import emitir_dps
    monkeypatch.setattr(emitir_dps, "ESPERA_ENSAIO_S", 0)
    corpo = _emitir(cliente, ensaio="on").get_data(as_text=True)
    assert "não há nada pendente" in corpo


def test_na_emissao_de_verdade_a_tela_diz_que_a_conferencia_termina_o_servico(cenario, monkeypatch):
    cliente, enviados = cenario
    enviados["modo"] = "processando"
    import emitir_dps
    monkeypatch.setattr(emitir_dps, "ESPERA_TOTAL_S", 0)
    corpo = _emitir(cliente).get_data(as_text=True)
    assert "termina o serviço" in corpo
    assert "ambiente=producao" in corpo


def test_a_espera_nao_e_valor_padrao_congelado_de_argumento():
    """Valor padrão de argumento é congelado quando a função nasce — mudar a
    constante depois não teria efeito. Isso já aconteceu aqui e passou batido:
    um teste que tentou encurtar a espera rodou os 150 segundos inteiros, e o
    único sintoma foi a suíte ficando três vezes mais lenta.

    Este teste acusa a volta do problema olhando a assinatura, porque o sintoma
    é lento e silencioso — ninguém liga uma suíte devagar a isto."""
    import inspect
    import emitir_dps
    padrao = inspect.signature(emitir_dps.emitir).parameters["espera_total_s"].default
    assert padrao is None, ("a espera voltou a ser valor padrão congelado; "
                            "ela tem de ser resolvida DENTRO da função")


# --------------------------------------------------------------------------- #
# O erro cujo texto oficial engana
# --------------------------------------------------------------------------- #
def test_o_erro_E0037_e_traduzido_para_o_que_ele_realmente_significa():
    """O texto oficial dele diz que o município não existe no cadastro nacional.
    O manual da prefeitura explica, numa seção própria, que na prática significa
    que o município não habilitou o ambiente de TESTE. Sem traduzir, a pessoa
    procura o problema nos dados da nota — onde ele não está."""
    import emitir_dps
    explicacoes = emitir_dps.explicar_erros(
        ["E0037 - O código do município emissor informado na DPS é inexistente"])
    assert len(explicacoes) == 1
    assert "Produção Restrita" in explicacoes[0]
    assert "prefeitura" in explicacoes[0]


def test_erro_desconhecido_nao_ganha_explicacao_inventada():
    """Explicar errado é pior que não explicar: manda procurar no lugar errado."""
    import emitir_dps
    assert emitir_dps.explicar_erros(["E9999 - algo que nao conhecemos"]) == []
    assert emitir_dps.explicar_erros([]) == []
    assert emitir_dps.explicar_erros(None) == []


def test_a_tela_de_recusa_mostra_a_traducao_quando_existe(cenario, monkeypatch):
    cliente, enviados = cenario
    enviados["modo"] = "recusa"
    corpo = _emitir(cliente, ensaio="on").get_data(as_text=True)
    assert "E0037" in corpo                        # o texto cru, para registro
    assert "ambiente de TESTE" in corpo            # e o que ele quer dizer
    assert "Nenhuma nota foi criada" in corpo


# --------------------------------------------------------------------------- #
# A declaração é gravada ANTES de qualquer espera
# --------------------------------------------------------------------------- #
# Em 07/10/2026 a identificação de uma declaração aceita só foi reencontrada no
# portal da prefeitura: do nosso lado, o único registro era a tela aberta no
# navegador. Publicar o serviço, fechar a aba ou cair a conexão perdia o rastro.

def test_a_declaracao_e_registrada_assim_que_a_prefeitura_aceita(cenario, monkeypatch):
    cliente, enviados = cenario
    enviados["modo"] = "processando"       # a nota NÃO fica pronta
    enviados["id_dps"] = ID_DPS_3281
    import emitir_dps
    monkeypatch.setattr(emitir_dps, "ESPERA_TOTAL_S", 0)

    registradas = []
    servindo = sys.modules["app.apps.emissaonf.web"]
    monkeypatch.setattr(servindo._decl, "registrar",
                        lambda planilha, id_dps, numero, card_id, **k: registradas.append(
                            (id_dps, numero, card_id, k.get("producao"))))

    _emitir(cliente)
    assert registradas, "a declaração tem de ser gravada mesmo quando a nota não sai"
    assert registradas[0][0] == ID_DPS_3281
    assert registradas[0][2] == CARD
    assert registradas[0][3] is True       # produção


def test_o_registro_que_falha_nao_derruba_a_emissao(certificado, monkeypatch, capsys):
    """A declaração já está com a prefeitura: abortar aqui não desfaz nada — só
    esconderia o que aconteceu."""
    import emitir_dps
    chave_pem, cert_pem = certificado

    def explode(id_dps):
        raise RuntimeError("planilha fora do ar")

    monkeypatch.setattr(nac.ELNfseNacional, "__init__",
                        lambda self, **k: setattr(self, "token", "x"))
    monkeypatch.setattr(nac.ELNfseNacional, "enviar_dps",
                        lambda self, d: {"idDPS": ID_DPS_3281})
    monkeypatch.setattr(nac.ELNfseNacional, "consultar_processamento_dps",
                        lambda self, i, bruto=False: {"nfseXmlGZipB64": "em processamento"})
    with pytest.raises(emitir_dps.NotaTalvezTenhaSaido):
        emitir_dps.emitir({"chave_pem": chave_pem, "cert_pem": cert_pem},
                          None, "tok", True, espera_total_s=0, ao_aceitar=explode)
    assert "não consegui registrar" in capsys.readouterr().out


def test_a_espera_e_curta_porque_o_servico_atende_quatro_pedidos_por_vez():
    """Não é conforto de tela: cada emissão esperando prende uma das quatro
    linhas de atendimento do serviço, que é compartilhado com o ERP e o painel.
    Com a espera em 150s, poucas tentativas seguidas derrubavam o monorepo
    inteiro com Bad Gateway — aconteceu em 07/10/2026."""
    import emitir_dps
    assert emitir_dps.ESPERA_TOTAL_S <= 40, (
        "espera longa prende uma das 4 linhas de atendimento do serviço inteiro")
    assert emitir_dps.ESPERA_ENSAIO_S <= emitir_dps.ESPERA_TOTAL_S


def test_o_aviso_de_ainda_processando_explica_o_aguardando_transmissao(certificado, monkeypatch):
    """É o estado que o portal da prefeitura mostra, e sem explicação ele parece
    falha."""
    import emitir_dps
    chave_pem, cert_pem = certificado
    monkeypatch.setattr(nac.ELNfseNacional, "__init__", lambda self, **k: None)
    monkeypatch.setattr(nac.ELNfseNacional, "consultar_processamento_dps",
                        lambda self, i, bruto=False: {"nfseXmlGZipB64": "em processamento"})
    with pytest.raises(emitir_dps.AindaProcessando) as e:
        emitir_dps.consultar({"chave_pem": chave_pem, "cert_pem": cert_pem},
                             ID_DPS_3281, "tok", True)
    assert "Aguardando Transmissão" in str(e.value)
    assert "número reservado" in str(e.value) or "está reservado" in str(e.value)


# --------------------------------------------------------------------------- #
# A segunda fonte: perguntar DIRETO à plataforma nacional
# --------------------------------------------------------------------------- #
# Em 07/10/2026 a prefeitura passou a responder "em processamento adn nacional" —
# ou seja, ela JÁ TRANSMITIU e a fila agora é da plataforma nacional. A partir
# desse momento a prefeitura deixa de ser a melhor fonte: a nota pode já existir
# no nacional e a resposta dela continuar a mesma. O sistema já sabia perguntar
# direto lá (era assim que reencontrava nota antiga) e não estava usando isso.

def _ctx_cert(certificado):
    chave_pem, cert_pem = certificado
    return {"chave_pem": chave_pem, "cert_pem": cert_pem}


def test_quando_a_prefeitura_ainda_processa_a_nota_e_procurada_no_nacional(certificado, monkeypatch):
    import emitir_dps, montar_dps
    monkeypatch.setattr(nac.ELNfseNacional, "__init__", lambda self, **k: None)
    monkeypatch.setattr(nac.ELNfseNacional, "consultar_processamento_dps",
                        lambda self, i, bruto=False: {"nfseXmlGZipB64": "<em processamento adn nacional>"})

    dps = nac.montar_dps_xml(montar_dps.montar(
        card=_card(), obra=ObraFalsa(), r=_calculo(), dados_rps=_dados_rps(),
        numero_nota=3281, ibge_obra=2601607, data_emissao="2026-10-07", producao=True))
    xml_nac = nfse_exemplo.como_texto(dps, numero_nfse="3281")

    import adn_nfse
    monkeypatch.setattr(adn_nfse, "consultar_chave_por_dps", lambda c, k, i: "2" * 50)
    monkeypatch.setattr(adn_nfse, "consultar_nfse_por_chave", lambda c, k, ch: xml_nac)

    res = emitir_dps.consultar(_ctx_cert(certificado), ID_DPS_3281, "tok", True)
    assert res["numero"] == "3281", "a nota existia no nacional e tinha de ser achada"


def test_se_o_nacional_tambem_nao_tem_a_nota_a_resposta_e_esperar(certificado, monkeypatch):
    import emitir_dps, adn_nfse
    monkeypatch.setattr(nac.ELNfseNacional, "__init__", lambda self, **k: None)
    monkeypatch.setattr(nac.ELNfseNacional, "consultar_processamento_dps",
                        lambda self, i, bruto=False: {"nfseXmlGZipB64": "<em processamento adn nacional>"})
    monkeypatch.setattr(adn_nfse, "consultar_chave_por_dps", lambda c, k, i: None)

    with pytest.raises(emitir_dps.AindaProcessando) as e:
        emitir_dps.consultar(_ctx_cert(certificado), ID_DPS_3281, "tok", True)
    texto = str(e.value)
    assert "já TRANSMITIU" in texto                 # diz de quem é a fila
    assert "plataforma nacional" in texto
    assert "convênio" in texto                      # e o que perguntar à prefeitura


def test_a_fila_da_prefeitura_e_a_do_nacional_sao_explicadas_diferente(certificado, monkeypatch):
    """A quem se reclama muda: antes de transmitir é com a prefeitura; depois, a
    autorização é do ambiente nacional."""
    import emitir_dps
    monkeypatch.setattr(nac.ELNfseNacional, "__init__", lambda self, **k: None)
    monkeypatch.setattr(nac.ELNfseNacional, "consultar_processamento_dps",
                        lambda self, i, bruto=False: {"nfseXmlGZipB64": "em processamento"})
    with pytest.raises(emitir_dps.AindaProcessando) as e:
        emitir_dps.consultar(_ctx_cert(certificado), ID_DPS_3281, "tok", True)
    assert "ainda não transmitiu" in str(e.value)
    assert "convênio" not in str(e.value)


def test_a_segunda_fonte_que_nao_responde_nao_virou_erro_da_consulta(certificado, monkeypatch, capsys):
    """Falha de rede na segunda fonte é só uma fonte que não respondeu — melhor
    uma resposta incompleta que uma tela de erro."""
    import emitir_dps, adn_nfse
    monkeypatch.setattr(nac.ELNfseNacional, "__init__", lambda self, **k: None)
    monkeypatch.setattr(nac.ELNfseNacional, "consultar_processamento_dps",
                        lambda self, i, bruto=False: {"nfseXmlGZipB64": "<em processamento adn nacional>"})

    def cai(*a, **k):
        raise RuntimeError("timeout na SEFIN")

    monkeypatch.setattr(adn_nfse, "consultar_chave_por_dps", cai)
    with pytest.raises(emitir_dps.AindaProcessando):
        emitir_dps.consultar(_ctx_cert(certificado), ID_DPS_3281, "tok", True)
    assert "não respondeu" in capsys.readouterr().out


def test_no_ensaio_a_plataforma_nacional_de_producao_nao_e_consultada(certificado, monkeypatch):
    """Ensaio vive em outro ambiente; perguntar à produção daria resposta errada."""
    import emitir_dps, adn_nfse
    chamou = []
    monkeypatch.setattr(adn_nfse, "consultar_chave_por_dps",
                        lambda *a, **k: chamou.append(1))
    assert emitir_dps._consultar_no_nacional(_ctx_cert(certificado), ID_DPS_3281, False) is None
    assert not chamou


# --------------------------------------------------------------------------- #
# Quando a nota não aparece em lugar NENHUM
# --------------------------------------------------------------------------- #
# Em 07/10/2026 o dono conferiu os dois sites — o da prefeitura e o nacional — e
# a 3281 não estava em nenhum. Aí não é mais "esperar": é descobrir de quem é a
# vez. O diagnóstico pergunta em todos os lugares e mostra as respostas cruas,
# para servir de prova.

def test_o_diagnostico_pergunta_nos_dois_lados_e_mostra_as_respostas(certificado, monkeypatch):
    import emitir_dps, adn_nfse, requests

    class RespFalsa:
        def __init__(self, status, texto):
            self.status_code, self.text = status, texto

    monkeypatch.setattr(nac.ELNfseNacional, "__init__",
                        lambda self, **k: setattr(self, "token", k.get("token")))
    monkeypatch.setattr(nac.ELNfseNacional, "_chamar",
                        lambda self, m, c, **k: RespFalsa(200, '{"nfseXmlGZipB64":"em processamento adn nacional"}'))
    monkeypatch.setattr(adn_nfse, "_cert_temp", lambda c, k: ("/tmp/c", "/tmp/k"))
    monkeypatch.setattr(requests, "get", lambda *a, **k: RespFalsa(404, '{"erro":"nao encontrado"}'))

    chave_pem, cert_pem = certificado
    texto = emitir_dps.diagnostico(
        {"chave_pem": chave_pem, "cert_pem": cert_pem, "_token": "tok"},
        ID_DPS_3281, True)

    assert "3281" in texto                              # de que nota se trata
    assert "[prefeitura: processamento da declaração]" in texto
    assert "[prefeitura: chave da declaração]" in texto
    assert "em processamento adn nacional" in texto     # a resposta crua dela
    assert "conhece esta declaração?" in texto          # e a pergunta decisiva


def test_quando_o_nacional_nao_conhece_a_declaracao_o_diagnostico_aponta_o_responsavel(certificado, monkeypatch):
    """É a resposta que resolve o impasse: se o nacional não conhece a
    declaração e a prefeitura diz que transmitiu, as duas versões não fecham — e
    a transmissão é a prefeitura que faz."""
    import emitir_dps, adn_nfse, requests

    class RespFalsa:
        def __init__(self, status, texto):
            self.status_code, self.text = status, texto

    monkeypatch.setattr(nac.ELNfseNacional, "__init__", lambda self, **k: None)
    monkeypatch.setattr(nac.ELNfseNacional, "_chamar",
                        lambda self, m, c, **k: RespFalsa(200, "{}"))
    monkeypatch.setattr(adn_nfse, "_cert_temp", lambda c, k: ("/tmp/c", "/tmp/k"))
    monkeypatch.setattr(requests, "get", lambda *a, **k: RespFalsa(404, "{}"))

    chave_pem, cert_pem = certificado
    texto = emitir_dps.diagnostico(
        {"chave_pem": chave_pem, "cert_pem": cert_pem, "_token": "tok"}, ID_DPS_3281, True)
    assert "NÃO" in texto and "conhece esta declaração" in texto
    assert "É com ela" in texto


def test_o_diagnostico_nunca_mostra_o_token_nem_o_certificado(certificado, monkeypatch):
    """Regra da área: segredo não entra no chat nem na tela. E este texto existe
    justamente para ser copiado e mandado para fora."""
    import emitir_dps, adn_nfse, requests

    class RespFalsa:
        status_code, text = 200, "{}"

    monkeypatch.setattr(nac.ELNfseNacional, "__init__", lambda self, **k: None)
    monkeypatch.setattr(nac.ELNfseNacional, "_chamar", lambda self, m, c, **k: RespFalsa())
    monkeypatch.setattr(adn_nfse, "_cert_temp", lambda c, k: ("/tmp/c", "/tmp/k"))
    monkeypatch.setattr(requests, "get", lambda *a, **k: RespFalsa())

    chave_pem, cert_pem = certificado
    texto = emitir_dps.diagnostico(
        {"chave_pem": chave_pem, "cert_pem": cert_pem,
         "_token": "token-secreto-da-prefeitura"}, ID_DPS_3281, True)
    assert "token-secreto-da-prefeitura" not in texto
    assert "BEGIN" not in texto          # nada de PEM


def test_sem_token_o_diagnostico_diz_isso_em_vez_de_estourar(certificado):
    import emitir_dps
    chave_pem, cert_pem = certificado
    texto = emitir_dps.diagnostico(
        {"chave_pem": chave_pem, "cert_pem": cert_pem, "_token": ""}, ID_DPS_3281, False)
    assert "token de integração ausente" in texto


def test_o_aviso_de_ainda_processando_oferece_o_diagnostico(cenario, monkeypatch):
    cliente, _ = cenario
    import emitir_dps

    def ainda(*a, **k):
        raise emitir_dps.AindaProcessando("na fila")

    servindo = sys.modules["app.apps.emissaonf.web"]
    monkeypatch.setattr(servindo, "_ctx_minimo", lambda: {"cred": {}, "chave_pem": b"x",
                                                          "cert_pem": b"x", "gc": None})
    monkeypatch.setattr(emitir_dps, "consultar", ainda)
    corpo = cliente.post("/emissao/declaracao",
                         data={"token": TOKEN, "id_dps": ID_DPS_3281}).get_data(as_text=True)
    assert "Diagnóstico completo" in corpo
    assert "diagnostico=1" in corpo


# --------------------------------------------------------------------------- #
# Nota emitida NO PORTAL, à mão
# --------------------------------------------------------------------------- #
# Pedido do dono em 07/10/2026, com o canal da emissão fora do ar: "crie um botão
# de emissão manual; eu anexo o PDF ou o XML e você faz o processamento".
#
# A escolha de usar o XML para os DADOS não é preferência: dele saem número,
# chave, valores e datas exatos. Do PDF seria preciso LER números de um texto, e
# um valor mal lido iria para a planilha e para o Omie sem ninguém notar.

@pytest.fixture
def nota_do_portal():
    import montar_dps
    dps = nac.montar_dps_xml(montar_dps.montar(
        card=_card(), obra=ObraFalsa(), r=_calculo(), dados_rps=_dados_rps(),
        numero_nota=3281, ibge_obra=2601607, data_emissao="2026-10-07", producao=True))
    return nfse_exemplo.como_texto(dps, numero_nfse="3281")


@pytest.fixture
def portal(monkeypatch):
    """A tela, com o pós-emissão dublado — ele escreve em planilha, Omie e Drive."""
    monkeypatch.setenv("EMISSAO_NF_TOKEN", TOKEN)
    servindo = sys.modules["app.apps.emissaonf.web"]
    feito = {}

    def falso_concluir(card_id, numero, codigo, data_iso, caminho, **k):
        feito.update(card_id=card_id, numero=numero, data=data_iso,
                     nacional=k.get("nacional"), chave=k.get("chave_nacional"),
                     pdf=k.get("pdf_municipal"))
        with open(caminho, encoding="utf-8") as fh:
            feito["xml_recebido"] = fh.read()

    monkeypatch.setattr(servindo._concluir, "concluir", falso_concluir)
    return app_real.test_client(), feito


def _enviar(cliente, **campos):
    dados = {"token": TOKEN, "card_id": CARD}
    dados.update(campos)
    return cliente.post("/emissao/manual", data=dados, follow_redirects=True,
                        content_type="multipart/form-data")


def test_a_tela_explica_que_ela_nao_emite_nada(portal):
    cliente, _ = portal
    corpo = cliente.get(f"/emissao/manual?token={TOKEN}").get_data(as_text=True)
    assert "não emite nada" in corpo
    assert "à mão, no portal" in corpo


def test_o_xml_colado_dispara_o_processamento_completo(portal, nota_do_portal):
    cliente, feito = portal
    r = _enviar(cliente, xml=nota_do_portal)
    assert r.status_code == 200
    assert feito["numero"] == "3281"
    assert feito["card_id"] == CARD
    assert feito["nacional"] is True
    assert len(feito["chave"]) == 50
    assert feito["data"] == "2026-10-07"


def test_o_xml_como_ARQUIVO_tambem_vale(portal, nota_do_portal):
    """É o que o dono pediu: anexar o arquivo, não colar texto."""
    import io as _io
    cliente, feito = portal
    r = _enviar(cliente, arquivo_xml=(_io.BytesIO(nota_do_portal.encode("utf-8")),
                                      "NFSe3281.xml"))
    assert r.status_code == 200
    assert feito["numero"] == "3281"


def test_o_pdf_do_portal_entra_como_o_documento(portal, nota_do_portal):
    """O que o sistema desenha é réplica; o do portal é o original. Tendo o
    original, é ele que o cliente recebe."""
    import io as _io
    cliente, feito = portal
    _enviar(cliente, xml=nota_do_portal,
            arquivo_pdf=(_io.BytesIO(b"%PDF-1.4 nota oficial"), "NF3281.pdf"))
    assert feito["pdf"] == b"%PDF-1.4 nota oficial"


def test_sem_pdf_o_sistema_desenha_o_dele(portal, nota_do_portal):
    cliente, feito = portal
    _enviar(cliente, xml=nota_do_portal)
    assert feito["pdf"] is None


def test_nota_do_modelo_antigo_tambem_e_aceita(portal):
    """Nota emitida no portal pode vir no modelo antigo; a tela não pode exigir
    que a pessoa saiba em qual modelo ela está."""
    cliente, feito = portal
    antigo = """<?xml version="1.0"?><CompNfse><Nfse><InfNfse>
        <Numero>3070</Numero><CodigoVerificacao>ABC123</CodigoVerificacao>
        <DataEmissao>2026-09-15T09:00:00</DataEmissao></InfNfse></Nfse></CompNfse>"""
    _enviar(cliente, xml=antigo)
    assert feito["numero"] == "3070"
    assert feito["nacional"] is False


def test_sem_o_card_nada_e_processado(portal, nota_do_portal):
    cliente, feito = portal
    corpo = cliente.post("/emissao/manual",
                         data={"token": TOKEN, "xml": nota_do_portal},
                         content_type="multipart/form-data").get_data(as_text=True)
    assert "número do card" in corpo
    assert not feito


def test_sem_o_xml_nada_e_processado(portal):
    cliente, feito = portal
    corpo = cliente.post("/emissao/manual",
                         data={"token": TOKEN, "card_id": CARD},
                         content_type="multipart/form-data").get_data(as_text=True)
    assert "XML da nota" in corpo
    assert not feito


def test_xml_que_nao_e_nota_explica_o_que_baixar(portal):
    """Erro comum: baixar o XML da DECLARAÇÃO em vez do da NOTA."""
    cliente, feito = portal
    corpo = _enviar(cliente, xml="<DPS><infDPS/></DPS>").get_data(as_text=True)
    assert "XML da NOTA" in corpo
    assert not feito


def test_a_tela_de_emissao_tem_link_para_a_nota_do_portal(cenario):
    cliente, _ = cenario
    corpo = cliente.get(f"/emissao/?token={TOKEN}", follow_redirects=True).get_data(as_text=True)
    assert "Nota emitida no portal" in corpo
