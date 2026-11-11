# -*- coding: utf-8 -*-
"""
LER O PAPEL DO COLABORADOR e preencher o cadastro com o que ele diz.

Ele cobrou isto pela enésima vez em 10/11/2026: *"no cadastro colaborador não
tem campo para anexar os documentos. Como é que vai ser cadastrado esse
colaborador? Como é que a IA vai fazer a leitura do código, ler as
informações?"*

⚑ A CHAMADA DE VERDADE À IA NÃO É TESTADA AQUI, e nem poderia: ela custa
dinheiro e depende de rede. O que é testado é tudo o que decide o
comportamento — o que vira campo, o que vira aviso, e o que acontece quando a
leitura falha. O dublê devolve o JSON que o modelo devolveria.
"""
from __future__ import annotations

import pytest

from app.apps.erp.core import documentos_de_pessoa as svc


class _Resposta:
    class _Escolha:
        def __init__(self, texto):
            self.message = type("M", (), {"content": texto})()

    def __init__(self, texto):
        self.choices = [self._Escolha(texto)]
        self.usage = type("U", (), {"prompt_tokens": 1, "completion_tokens": 1})()


def _dublê(monkeypatch, devolve, guarda=None):
    import json as _json
    from app.apps.erp.core.documentos import leitor

    class _Cliente:
        class chat:
            class completions:
                @staticmethod
                def create(**kw):
                    if guarda is not None:
                        guarda.update(kw)
                    return _Resposta(_json.dumps(devolve, ensure_ascii=False))
    monkeypatch.setattr(leitor, "_cliente", lambda: _Cliente())


def test_o_que_o_papel_diz_vira_campo_do_formulario(monkeypatch):
    _dublê(monkeypatch, {
        "documento": "CNH", "nome": "José Ribamar Nunes",
        "cpf": "123.456.789-01", "data_nascimento": "15/03/1980",
        "telefone": "(85) 99999-8888", "legivel": True, "problema": ""})
    r = svc.ler(b"foto", "cnh.jpg", "DOC-IDENTIDADE")
    assert r["campos"] == {"nome": "José Ribamar Nunes",
                           "cpf": "12345678901",
                           "data_nascimento": "1980-03-15",
                           "telefone": "85999998888"}
    assert "José Ribamar Nunes" in r["resumo"]
    assert r["aviso"] == ""


def test_cpf_incompleto_NAO_entra(monkeypatch):
    """⚑ UM CPF ERRADO IMPEDE A PESSOA DE RECEBER. Preferir campo vazio a
    campo chutado — e a instrução que viaja diz isso ao modelo também."""
    _dublê(monkeypatch, {"documento": "RG", "cpf": "123.456", "nome": "Fulano"})
    assert "cpf" not in svc.ler(b"x", "rg.jpg", "DOC-IDENTIDADE")["campos"]


def test_data_em_qualquer_dos_dois_formatos(monkeypatch):
    for veio, esperado in (("1980-03-15", "1980-03-15"),
                           ("15/03/1980", "1980-03-15"),
                           ("15-03-1980", "1980-03-15"),
                           ("março de 1980", None)):
        _dublê(monkeypatch, {"documento": "RG", "data_nascimento": veio})
        campos = svc.ler(b"x", "rg.jpg", "DOC-IDENTIDADE")["campos"]
        assert campos.get("data_nascimento") == esperado


def test_papel_trocado_de_lugar_AVISA(monkeypatch):
    """⚑ É O GANHO REAL: a pessoa ainda está ao lado do colaborador, com os
    papéis na mão. Hoje o DP descobre isso três dias depois."""
    _dublê(monkeypatch, {"documento": "CTPS", "nome": "Fulano", "legivel": True})
    r = svc.ler(b"x", "foto.jpg", "COMPROVANTE-ENDERECO-PESSOA")
    assert "comprovante de endereço" in r["aviso"]
    assert "trocou o lugar" in r["aviso"]
    # ⚑ MAS OS CAMPOS CONTINUAM VINDO: avisar não é recusar.
    assert r["campos"]["nome"] == "Fulano"


def test_papel_certo_nao_avisa_nada(monkeypatch):
    _dublê(monkeypatch, {"documento": "Comprovante de endereço", "legivel": True})
    assert svc.ler(b"x", "c.jpg", "COMPROVANTE-ENDERECO-PESSOA")["aviso"] == ""


def test_a_propria_CTPS_serve_de_identificacao(monkeypatch):
    """Ele: *"a carteira de trabalho, ou a própria carteira de trabalho pode
    ser o documento de identificação."*"""
    _dublê(monkeypatch, {"documento": "CTPS", "nome": "Fulano", "legivel": True})
    assert svc.ler(b"x", "c.jpg", "DOC-IDENTIDADE")["aviso"] == ""


def test_foto_ruim_pede_outra_e_diz_o_problema(monkeypatch):
    _dublê(monkeypatch, {"documento": "RG", "legivel": False,
                         "problema": "cortado na metade"})
    aviso = svc.ler(b"x", "rg.jpg", "DOC-IDENTIDADE")["aviso"]
    assert "cortado na metade" in aviso and "outra foto" in aviso


def test_quando_a_leitura_FALHA_o_papel_continua_anexado(monkeypatch):
    """⚑ A FALHA DA IA NÃO PODE DERRUBAR O ANEXO. O arquivo já subiu; o que
    não houve foi a leitura. A tela mostra "anexado" e a vida segue."""
    from app.apps.erp.core.documentos import leitor

    def _explode():
        raise RuntimeError("sem rede")
    monkeypatch.setattr(leitor, "_cliente", _explode)
    r = svc.ler(b"x", "rg.jpg", "DOC-IDENTIDADE")
    assert r["campos"] == {} and r["aviso"] == ""
    assert "não consegui ler" in r["resumo"]


def test_a_instrucao_proibe_inventar_e_diz_o_que_se_espera(monkeypatch):
    """O que viaja tem de carregar a proibição — não basta estar no
    comentário do código."""
    pedido = {}
    _dublê(monkeypatch, {"documento": "RG"}, guarda=pedido)
    svc.ler(b"x", "rg.jpg", "COMPROVANTE-ENDERECO-PESSOA")
    texto = pedido["messages"][0]["content"][0]["text"]
    assert "NUNCA INVENTE" in texto
    assert "impede a pessoa de receber" in texto
    assert "comprovante de endereço" in texto     # o que se esperava
    assert pedido["temperature"] == 0


def test_so_os_campos_do_cadastro_voltam(monkeypatch):
    """O modelo pode devolver o que quiser; só entra o que o cadastro usa."""
    _dublê(monkeypatch, {"documento": "RG", "nome": "Fulano",
                         "salario": "9999", "cargo": "Diretor",
                         "orgao_emissor": "SSP"})
    assert set(svc.ler(b"x", "rg.jpg", "DOC-IDENTIDADE")["campos"]) == {"nome"}


def test_todo_papel_exigido_sabe_o_que_espera():
    """Papel sem expectativa escrita não daria aviso nenhum — e o aviso é
    metade do valor disto."""
    from app.apps.erp.core import pessoal_solicitacoes as svc_sp
    exigidos = set()
    for d in svc_sp.TIPOS.values():
        exigidos |= set(d.get("documentos", ()))
        for mapa in (d.get("documentos_se") or {}).values():
            for papeis in mapa.values():
                exigidos |= set(papeis)
    for codigo in exigidos:
        assert codigo in svc.O_QUE_ESPERO, (
            f"{codigo} é exigido em algum pedido e a leitura não sabe o que "
            f"esperar dele — nenhum aviso sairia")
