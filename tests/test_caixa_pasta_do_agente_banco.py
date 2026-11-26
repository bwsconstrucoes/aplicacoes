"""A leitura da caixa de entrada não marca como lido o que não é do agente
(26/11/2026). O dono: *"outros e-mails que não tem nada a ver com os assuntos,
ele está deixando lido. (…) Olha o perigo disso. (…) criar uma pasta agente.
Aí tudo que o agente lesse e ele conseguisse (…) cruzar a informação com o
sistema, ele trazia para a pasta dele (…) e ficava lá como lido. E tudo que ele
não identificasse, ele mantinha na caixa de entrada como não lido."*
"""
from __future__ import annotations

import re

import pytest
from sqlalchemy import text

pytestmark = pytest.mark.banco


def _msg(mid, assunto, corpo):
    return (f"Message-ID: {mid}\r\nFrom: contabilidade@escritorio.com\r\n"
            f"Subject: {assunto}\r\nDate: Wed, 26 Nov 2026 10:00:00 -0300\r\n"
            f"Content-Type: text/plain; charset=utf-8\r\n\r\n{corpo}\r\n").encode()


class CaixaFalsa:
    capabilities = ("IMAP4REV1", "MOVE", "UIDPLUS")

    def __init__(self, mensagens):
        self.msgs = dict(mensagens)          # uid(bytes) -> bytes
        self.lidas, self.movidas, self.criadas = set(), {}, []

    def uid(self, cmd, *args):
        cmd = cmd.upper()
        if cmd == "SEARCH":
            return "OK", [b" ".join(u for u in self.msgs if u not in self.lidas)]
        if cmd == "FETCH":
            conjunto, partes = args
            uids = conjunto.split(b",")
            saida = []
            for u in uids:
                bruto = self.msgs[u]
                if b"HEADER.FIELDS" in partes.encode():
                    cab = re.search(rb"Message-ID: [^\r\n]+\r\n", bruto).group(0) + b"\r\n"
                    saida.append((b"1 (UID " + u + b" BODY[HEADER.FIELDS (MESSAGE-ID)] {1}", cab))
                else:
                    assert "PEEK" in partes, "baixar sem PEEK marcaria como lida"
                    saida.append((b"1 (UID " + u + b" BODY[] {1}", bruto))
                saida.append(b")")
            return "OK", saida
        if cmd == "STORE":
            conjunto, op, flags = args
            if "Seen" in flags:
                self.lidas |= set(conjunto.split(b","))
            return "OK", []
        if cmd == "MOVE":
            conjunto, pasta = args
            for u in conjunto.split(b","):
                self.movidas[u] = pasta
            return "OK", []
        raise AssertionError(cmd)

    def list(self):
        return "OK", [b'(\\HasNoChildren) "." INBOX', b'(\\HasNoChildren) "." INBOX.enviadas']

    def create(self, pasta):
        self.criadas.append(pasta)
        return "OK", []

    def subscribe(self, pasta):
        return "OK", []

    def close(self): pass

    def logout(self): pass


def test_o_que_casou_vai_para_a_pasta_do_agente_e_o_resto_fica_nao_lido(sessao_real, monkeypatch):
    from app.apps.erp.core.comum import email_entrada as ee
    s = sessao_real
    pid = s.execute(text("INSERT INTO dp_pedidos (numero, template, situacao) "
                         "VALUES ('CT-09001', 'DEPENDENTE', 'ENVIADO') RETURNING id")).scalar()
    caixa = CaixaFalsa({
        b"11": _msg("<resposta@escritorio>", "Re: [CT-09001] Dependente — FULANO",
                    "Recebido, estamos providenciando."),
        b"12": _msg("<promocao@loja>", "Promoção de Black Friday", "Compre já"),
    })
    monkeypatch.setattr(ee, "configuracao", lambda s_: {"usavel": True})
    monkeypatch.setattr(ee, "_conectar", lambda s_: caixa)
    ee._COLUNA_CORPO.clear()
    r = ee.rodada(s, None, gravar=True)
    assert r["olhou"] == 2
    assert caixa.movidas == {b"11": "INBOX.Agente"}, "só a que casou vai para a pasta"
    assert caixa.criadas == ["INBOX.Agente"]
    assert b"12" not in caixa.lidas, "a que não é do agente fica NÃO LIDA para o DP"
    corpo = s.execute(text("SELECT corpo FROM email_entrada_mensagens "
                           "WHERE pedido_id = :p"), {"p": pid}).scalar()
    assert "estamos providenciando" in corpo, "o texto da resposta fica guardado"
    from app.apps.erp.core import contabilidade as svc_ct
    visto = svc_ct.o_que_voltou(s, pid, "CT-09001")
    assert "estamos providenciando" in visto["respostas"][0]["corpo"], \
        "o pedido mostra o que a contabilidade respondeu"
    # Na rodada seguinte, a não lida que já foi vista não é baixada de novo.
    r2 = ee.rodada(s, None, gravar=True)
    assert r2["olhou"] == 0


def test_ler_sem_gravar_nao_mexe_em_nada(sessao_real, monkeypatch):
    from app.apps.erp.core.comum import email_entrada as ee
    caixa = CaixaFalsa({b"21": _msg("<x@y>", "Qualquer", "texto")})
    monkeypatch.setattr(ee, "configuracao", lambda s_: {"usavel": True})
    monkeypatch.setattr(ee, "_conectar", lambda s_: caixa)
    ee.rodada(sessao_real, None, gravar=False)
    assert not caixa.lidas and not caixa.movidas
