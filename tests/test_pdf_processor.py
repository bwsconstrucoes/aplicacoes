"""PDF Processor — o /compilar (chamado pelo Make).

Duas coisas são travadas aqui:
  1. o FORMATO da resposta, de que o cenário do Make depende (ver
     app/apps/pdf_processor/README.md) — campo novo pode entrar, os antigos não mudam;
  2. o que acontece com cada tipo de arquivo (foto, PDF protegido, PDF com senha,
     arquivo que não é PDF), que em 13/11/2026 era o que custava tempo e memória.

Nada aqui fala com o Dropbox nem com a internet: o envio e o download são dublados.
"""
import base64
import io
import warnings

import pytest

fitz = pytest.importorskip("fitz")
pytest.importorskip("PyPDF2")
pytest.importorskip("dropbox")

from flask import Flask  # noqa: E402

import app.apps.pdf_processor as modulo  # noqa: E402

warnings.filterwarnings("ignore")


# ───────────────────────── fabricação de arquivos ─────────────────────────

def pdf_de_texto(paginas=2, marca="NOTA"):
    doc = fitz.open()
    for i in range(paginas):
        doc.new_page().insert_text((50, 80), f"{marca} pagina {i + 1}", fontsize=12)
    dados = doc.tobytes()
    doc.close()
    return dados


def pdf_protegido(senha_para_abrir=""):
    doc = fitz.open("pdf", pdf_de_texto(2, "COMPROVANTE"))
    dados = doc.tobytes(encryption=fitz.PDF_ENCRYPT_AES_256, user_pw=senha_para_abrir, owner_pw="dono")
    doc.close()
    return dados


def foto(largura=800, altura=1200, formato="jpeg"):
    pix = fitz.Pixmap(fitz.csRGB, fitz.IRect(0, 0, largura, altura))
    pix.clear_with(200)
    return pix.tobytes(formato)


def anexo(nome, dados):
    return {"filename": nome, "base64": base64.b64encode(dados).decode()}


# ───────────────────────────── ambiente ─────────────────────────────

@pytest.fixture
def enviados(monkeypatch):
    """Troca o envio ao Dropbox por uma lista: guarda o PDF que seria enviado."""
    lista = []

    def envio(bio, caminho):
        lista.append({"caminho": caminho, "pdf": bio.getvalue()})
        return "https://exemplo.invalid/compilado.pdf?dl=1"

    monkeypatch.setattr(modulo, "upload_dropbox", envio)
    return lista


@pytest.fixture
def cliente(enviados):
    app = Flask(__name__)
    app.register_blueprint(modulo.bp)
    return app.test_client()


def abrir(enviados):
    return fitz.open(stream=enviados[-1]["pdf"], filetype="pdf")


# ───────────────────────── formato da resposta ─────────────────────────

def test_resposta_tem_os_campos_de_que_o_make_depende(cliente, enviados):
    r = cliente.post("/compilar", json={"attachments": [anexo("nota.pdf", pdf_de_texto(2))],
                                        "nome_arquivo": "sp123"})
    assert r.status_code == 200
    j = r.get_json()
    # os quatro de sempre continuam; "ignorados" é o único acrescentado
    assert set(j) == {"status", "file", "link", "results", "ignorados"}
    assert j["status"] == "ok"
    assert j["file"] == "sp123.pdf"                      # ganha .pdf sozinho
    assert j["link"] == "https://exemplo.invalid/compilado.pdf?dl=1"
    assert j["ignorados"] == []
    assert len(j["results"]) == 1
    assert set(j["results"][0]) == {"filename", "texto"}
    assert j["results"][0]["filename"] == "nota.pdf"
    # texto é uma LISTA, uma posição por página
    assert isinstance(j["results"][0]["texto"], list) and len(j["results"][0]["texto"]) == 2
    assert "NOTA pagina 1" in j["results"][0]["texto"][0]
    assert enviados[-1]["caminho"] == "/pdf-compilados/sp123.pdf"


def test_sem_texto_devolve_lista_vazia(cliente):
    j = cliente.post("/compilar", json={"attachments": [anexo("nota.pdf", pdf_de_texto(3))],
                                        "incluir_texto": False}).get_json()
    assert j["results"] == [{"filename": "nota.pdf", "texto": []}]


def test_sem_salvar_nao_envia_e_link_e_nulo(cliente, enviados):
    j = cliente.post("/compilar", json={"attachments": [anexo("nota.pdf", pdf_de_texto(1))],
                                        "salvar": False}).get_json()
    assert j["link"] is None and enviados == []


def test_ordem_links_primeiro_depois_anexos(cliente, enviados, monkeypatch):
    monkeypatch.setattr(modulo, "_baixar",
                        lambda url, *a, **k: (io.BytesIO(pdf_de_texto(1, "DOLINK")), "application/pdf", None))
    j = cliente.post("/compilar", json={"attachments": [anexo("anexo.pdf", pdf_de_texto(1, "DOANEXO"))],
                                        "links": ["https://exemplo.invalid/pasta/boleto.pdf"]}).get_json()
    assert [x["filename"] for x in j["results"]] == ["boleto.pdf", "anexo.pdf"]
    doc = abrir(enviados)
    assert "DOLINK" in doc[0].get_text() and "DOANEXO" in doc[1].get_text()


def test_formato_hexadecimal_continua_aceito(cliente):
    j = cliente.post("/compilar", json={"attachments": [{"filename": "n.pdf", "data": pdf_de_texto(1).hex()}]}).get_json()
    assert len(j["results"]) == 1 and len(j["results"][0]["texto"]) == 1


# ───────────────────────── o que fica de fora ─────────────────────────

def test_arquivo_que_nao_e_pdf_fica_de_fora_e_e_avisado(cliente, enviados):
    j = cliente.post("/compilar", json={"attachments": [
        anexo("planilha.xlsx", b"isto nao e pdf"),
        anexo("nota.pdf", pdf_de_texto(2)),
    ]}).get_json()
    assert j["status"] == "ok"
    # results segue trazendo só o que entrou, como sempre
    assert [x["filename"] for x in j["results"]] == ["nota.pdf"]
    assert [x["filename"] for x in j["ignorados"]] == ["planilha.xlsx"]
    assert j["ignorados"][0]["motivo"]
    assert len(abrir(enviados)) == 2


def test_pdf_que_pede_senha_fica_de_fora_com_o_motivo(cliente):
    j = cliente.post("/compilar", json={"attachments": [anexo("extrato.pdf", pdf_protegido("1234"))]}).get_json()
    assert j["results"] == []
    assert j["ignorados"] == [{"filename": "extrato.pdf", "motivo": "o PDF pede senha para abrir"}]


def test_link_que_nao_baixa_fica_de_fora_com_o_motivo(cliente, monkeypatch):
    monkeypatch.setattr(modulo, "_baixar", lambda url, *a, **k: (None, None, "arquivo maior que 50 MB"))
    j = cliente.post("/compilar", json={"links": ["https://exemplo.invalid/grande.pdf"]}).get_json()
    assert j["ignorados"] == [{"filename": "grande.pdf", "motivo": "arquivo maior que 50 MB"}]


# ───────────────────────── foto ─────────────────────────

@pytest.mark.parametrize("formato,nome", [("jpeg", "foto.jpg"), ("png", "foto.png")])
def test_foto_entra_como_veio_numa_pagina_a4(cliente, enviados, formato, nome):
    dados = foto(1600, 2400, formato)
    j = cliente.post("/compilar", json={"attachments": [anexo(nome, dados)]}).get_json()
    assert j["results"] == [{"filename": nome, "texto": [""]}]   # uma página, sem texto — como antes
    doc = abrir(enviados)
    assert len(doc) == 1
    r = doc[0].rect
    assert r.width <= 595.5 and r.height <= 842.5                # cabe em A4 (antes: tamanho de cartaz)
    assert abs(r.width / r.height - 1600 / 2400) < 0.01          # sem deformar
    imagens = doc[0].get_images(full=True)
    assert len(imagens) == 1
    assert (imagens[0][2], imagens[0][3]) == (1600, 2400)        # nenhum ponto da foto se perdeu
    # e não foi redesenhada: o PDF não fica muito maior que a foto
    assert len(enviados[-1]["pdf"]) < len(dados) + 20_000


def test_foto_deitada_ganha_pagina_deitada(cliente, enviados):
    cliente.post("/compilar", json={"attachments": [anexo("f.jpg", foto(2400, 1600))]})
    r = abrir(enviados)[0].rect
    assert r.width > r.height and r.width <= 842.5 and r.height <= 595.5


def test_foto_de_celular_com_marca_de_rotacao_sai_em_pe(cliente, enviados):
    imagem = pytest.importorskip("PIL.Image")
    im = imagem.new("RGB", (400, 200), "white")      # crua: deitada
    marca = im.getexif()
    marca[0x0112] = 6                                # "gire 90° para exibir"
    b = io.BytesIO()
    im.save(b, "JPEG", exif=marca)
    cliente.post("/compilar", json={"attachments": [anexo("celular.jpg", b.getvalue())]})
    r = abrir(enviados)[0].rect
    assert r.height > r.width                        # exibida em pé, como o celular mostra


# ───────────────────────── PDF protegido ─────────────────────────

def test_pdf_protegido_que_abre_sem_senha_mantem_as_paginas_originais(cliente, enviados):
    dados = pdf_protegido("")
    j = cliente.post("/compilar", json={"attachments": [anexo("comprovante.pdf", dados)]}).get_json()
    assert j["ignorados"] == []
    assert len(j["results"][0]["texto"]) == 2
    assert "COMPROVANTE pagina 1" in j["results"][0]["texto"][0]
    doc = abrir(enviados)
    assert len(doc) == 2
    assert "COMPROVANTE pagina 2" in doc[1].get_text()           # texto de verdade, não imagem
    assert doc[0].get_images(full=True) == []                    # antes: virava imagem de baixa resolução
    assert len(enviados[-1]["pdf"]) < 50_000


# ───────────────────────── página duplicada ─────────────────────────

def test_falha_ao_ler_o_texto_nao_duplica_as_paginas(cliente, enviados, monkeypatch):
    from PyPDF2 import _page

    def quebra(self, *a, **k):
        raise RuntimeError("texto ilegível")

    monkeypatch.setattr(_page.PageObject, "extract_text", quebra)
    j = cliente.post("/compilar", json={"attachments": [anexo("nota.pdf", pdf_de_texto(3))]}).get_json()
    assert j["results"] == [{"filename": "nota.pdf", "texto": ["", "", ""]}]
    assert len(abrir(enviados)) == 3                             # antes: 6 (as 3 de novo, como imagem)


# ───────────────────────── esperas com prazo ─────────────────────────

class _RespostaLenta:
    status_code = 200
    headers = {"Content-Type": "application/pdf"}

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def iter_content(self, chunk_size=0):
        while True:
            yield b"x" * 10


def test_download_que_nao_termina_e_abandonado_no_prazo(monkeypatch):
    chamada = {}

    def get(url, **k):
        chamada.update(k)
        return _RespostaLenta()

    monkeypatch.setattr(modulo.requests, "get", get)
    bio, tipo, motivo = modulo._baixar("https://exemplo.invalid/lento.pdf", prazo=0.05)
    assert bio is None and "passou de" in motivo
    assert chamada["stream"] is True and chamada["timeout"]


def test_download_acima_do_teto_e_recusado(monkeypatch):
    monkeypatch.setattr(modulo.requests, "get", lambda url, **k: _RespostaLenta())
    bio, tipo, motivo = modulo._baixar("https://exemplo.invalid/grande.pdf", limite=1000)
    assert bio is None and "maior que" in motivo


def test_renovacao_do_acesso_ao_dropbox_tem_prazo(monkeypatch):
    visto = {}

    class _R:
        def raise_for_status(self):
            pass

        def json(self):
            return {"access_token": "t", "expires_in": 100}

    def post(url, **k):
        visto.update(k)
        return _R()

    monkeypatch.setattr(modulo.requests, "post", post)
    monkeypatch.setattr(modulo, "DROPBOX_TOKEN", None)
    monkeypatch.setattr(modulo, "DROPBOX_TOKEN_EXPIRATION", 0)
    modulo.refresh_dropbox_token()
    assert visto.get("timeout")
