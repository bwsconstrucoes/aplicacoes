# -*- coding: utf-8 -*-
"""
VER ANEXO E COMPROVANTE NUMA JANELA — 10/10/2026.

O dono: *"os anexos e comprovantes sempre remetem ao download (…) muitas vezes
deseja-se apenas dar uma olhada rápida. A abertura num modal é viável? (…) No
Pipefy é assim."* O que importa aqui é a TRAVA: o servidor só busca em
Dropbox e Pipefy, com teto de tamanho, e devolve "para ver".
"""
import pytest

from tests.test_analisesps_usuarios_banco import (  # noqa: F401 — fixtures
    SENHA_MESTRE_OPERADOR, app, banco_acesso)

pytestmark = pytest.mark.banco


class Resposta:
    def __init__(self, url, tipo="application/pdf", corpo=b"%PDF-1.4 ...", status=200,
                 tamanho=None, disposicao='attachment; filename="comp.pdf"'):
        self.url, self.status_code, self._corpo = url, status, corpo
        self.headers = {"Content-Type": tipo, "Content-Disposition": disposicao,
                        "Content-Length": str(tamanho if tamanho is not None else len(corpo))}
        self.fechada = False

    def iter_content(self, _n):
        yield self._corpo

    def close(self):
        self.fechada = True


class Sessao:
    def __init__(self, resposta):
        self.resposta, self.pedidos = resposta, []

    def get(self, url, **_k):
        self.pedidos.append(url)
        return self.resposta


def test_so_busca_em_endereco_conhecido():
    from app.apps.analisesps import previa_arquivo as pa
    assert pa._host_permitido("https://www.dropbox.com/s/x/comp.pdf?dl=0")
    assert pa._host_permitido("https://pipefy-prd-us-east-1.s3.amazonaws.com/a.pdf?X=1")
    assert not pa._host_permitido("https://dropbox.com.malicioso.com/a.pdf")
    assert not pa._host_permitido("http://169.254.169.254/latest/meta-data")
    assert not pa._host_permitido("file:///etc/passwd")
    assert pa.como_ver("https://drive.google.com/file/d/ABC123/view")["endereco"] \
        == "https://drive.google.com/file/d/ABC123/preview"
    assert pa.como_ver("https://exemplo.com/a.pdf")["modo"] == "aba"


def test_dropbox_vem_para_ver_e_o_redirecionamento_e_conferido():
    from app.apps.analisesps import previa_arquivo as pa
    sessao = Sessao(Resposta("https://dl.dropboxusercontent.com/s/x/comp.pdf"))
    pedacos, tipo, nome = pa.buscar("https://www.dropbox.com/s/x/comp.pdf?dl=0", sessao)
    assert sessao.pedidos == ["https://www.dropbox.com/s/x/comp.pdf?raw=1"]
    assert tipo == "application/pdf" and nome == "comp.pdf" and b"".join(pedacos).startswith(b"%PDF")
    fora = Sessao(Resposta("https://malicioso.com/x.pdf"))
    with pytest.raises(pa.ErroDaPrevia, match="endereço desconhecido"):
        pa.buscar("https://www.dropbox.com/s/x/comp.pdf", fora)


def test_grande_demais_ou_tipo_que_nao_se_ve_vira_recado():
    from app.apps.analisesps import previa_arquivo as pa
    with pytest.raises(pa.ErroDaPrevia, match="grande demais"):
        pa.buscar("https://app.pipefy.com/x.pdf",
                  Sessao(Resposta("https://app.pipefy.com/x.pdf", tamanho=pa.TETO_BYTES + 1)))
    with pytest.raises(pa.ErroDaPrevia, match="não se vê"):
        pa.buscar("https://app.pipefy.com/x.zip",
                  Sessao(Resposta("https://app.pipefy.com/x.zip", tipo="application/zip")))
    with pytest.raises(pa.ErroDaPrevia, match="vencido"):
        pa.buscar("https://app.pipefy.com/x.pdf",
                  Sessao(Resposta("https://app.pipefy.com/x.pdf", status=403)))


def test_a_ROTA_devolve_para_ver_e_recusa_endereco_estranho(app, monkeypatch):
    from app.apps.analisesps import previa_arquivo as pa
    monkeypatch.setattr(pa, "buscar", lambda url: (iter([b"%PDF-1.4"]), "application/pdf", "comp.pdf")
                        if "dropbox" in url else (_ for _ in ()).throw(pa.ErroDaPrevia("não abre aqui")))
    with app.test_client() as c:
        c.post("/analisesps/entrar", data={"senha": SENHA_MESTRE_OPERADOR})
        ok = c.get("/analisesps/arquivo/ver?u=https%3A%2F%2Fwww.dropbox.com%2Fs%2Fx%2Fcomp.pdf")
        ruim = c.get("/analisesps/arquivo/ver?u=https%3A%2F%2Fmalicioso.com%2Fa.pdf")
    assert ok.status_code == 200 and ok.mimetype == "application/pdf"
    assert ok.headers["Content-Disposition"].startswith("inline")
    assert "não abre aqui" in ruim.get_data(as_text=True) and "Abrir em outra aba" in ruim.get_data(as_text=True)


def test_os_icones_abrem_a_janela(app):
    from tests.test_analisesps_banco import semear, sp
    semear([sp("1000000701", forma_pagamento="Boleto", status_pgt="Pago", valor="10,00",
               vencimento="10/10/2026", centro_custo="OBRA",
               anexo_link="https://drive.google.com/file/d/ANEXO7/view",
               comprovante="https://www.dropbox.com/s/c/comp.pdf?dl=0")])
    with app.test_client() as c:
        c.post("/analisesps/entrar", data={"senha": SENHA_MESTRE_OPERADOR})
        lista = c.get("/analisesps/solicitacoes?f=1&busca=1000000701",
                      follow_redirects=True).get_data(as_text=True)
        ficha = c.get("/analisesps/sp/1000000701?modal=1").get_data(as_text=True)
    assert 'data-previa="https://drive.google.com/file/d/ANEXO7/view"' in lista
    assert 'data-previa="https://www.dropbox.com/s/c/comp.pdf?dl=0"' in lista
    assert "uc?export=download&amp;id=ANEXO7" in lista, "o href segue sendo o de baixar"
    assert ficha.count("data-previa=") == 2
    assert 'data-url-arquivo-ver="/analisesps/arquivo/ver"' in lista
