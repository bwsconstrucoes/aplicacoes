# -*- coding: utf-8 -*-
"""A gestão dos comprovantes (18/11/2026), com banco de verdade: a lista com
filtros, o link de cada comprovante e a cópia no Drive do que não baixou.

O dono: *"eu queria poder, no sidebar, fazer filtragem (…) aparece uma série de
coisas que deu erro. Aí eu quero entender o que é que deu erro (…) os
comprovantes que não foram baixados (…) deveriam ficar salvos no Google Drive."*"""
import os

import pytest

from tests.test_analisesps_telas import app  # noqa: F401 — a fixture

from tests.test_analisesps_comprovantes import pdf_com

pytestmark = pytest.mark.banco


@pytest.fixture
def lotes(banco_analisesps, tmp_path, monkeypatch):
    from app.apps.analisesps import comprovantes as cp
    from app.apps.analisesps.db import conexao
    monkeypatch.setattr(cp, "PASTA", str(tmp_path))
    with conexao() as conn:
        conn.execute("INSERT INTO analisesps.sps (id, credor, comprovante) VALUES "
                     "('1400000001', 'FULANO', 'https://www.dropbox.com/s/abc/comp.pdf?dl=0')")
        conn.commit()
    um = cp.guardar(pdf_com(3), "Bradesco_18112026.pdf", "marcelo", "MARCELO")
    dois = cp.guardar(pdf_com(1), "Sicredi.pdf", "ana", "ANA")
    with conexao() as conn:
        # Os envios já processados: o "aguardando" do envio dá lugar ao resultado.
        conn.execute("DELETE FROM analisesps.comprovantes_item WHERE situacao = 'AGUARDANDO'")
        cp._gravar_itens(conn, um, [
            {"pagina": 1, "situacao": cp.BAIXADO, "sp_id": "1400000001", "valor": "100,00",
             "recebedor": "FULANO", "motivo": "Baixa confirmada", "conversa_omie": ""},
            {"pagina": 2, "situacao": cp.NAO_LOCALIZADO, "sp_id": "", "valor": "250,00",
             "recebedor": "CICLANO DA SILVA", "motivo": "Não encontrei a SP",
             "conversa_omie": ""},
            {"pagina": 3, "situacao": cp.ERRO, "sp_id": "1400000009", "valor": "80,00",
             "recebedor": "BELTRANO", "motivo": "O Omie respondeu: título não encontrado",
             "conversa_omie": "consultar: FALHOU"}])
        cp._gravar_itens(conn, dois, [
            {"pagina": 1, "situacao": cp.DUPLICADO, "sp_id": "1400000001", "valor": "100,00",
             "recebedor": "FULANO", "motivo": "já baixado", "conversa_omie": ""}])
        conn.commit()
    return um, dois


def _filtro(**kw):
    from werkzeug.datastructures import MultiDict
    from app.apps.analisesps import comprovantes as cp
    return cp.filtros_da_tela(MultiDict({"f": "1", **kw}))


def test_a_lista_traz_cada_pagina_com_o_link_do_comprovante(lotes):
    from app.apps.analisesps import comprovantes as cp
    lista = cp.listar(_filtro())
    assert lista["total"] == 4 and len(lista["itens"]) == 4
    por = {(i["lote_id"], i["pagina"]): i for i in lista["itens"]}
    um, dois = lotes
    assert por[(um, 1)]["link_arquivado"].startswith("https://www.dropbox.com/")
    assert por[(dois, 1)]["link_arquivado"], "o duplicado também mostra o comprovante da SP"
    assert por[(um, 2)]["link_arquivado"] == "" and por[(um, 2)]["no_servidor"]
    assert dict((s, q) for s, _, q in lista["contagem"])[cp.NAO_LOCALIZADO] == 1
    assert lista["quem"] == ["ANA", "MARCELO"]


@pytest.mark.parametrize("filtro,esperado", [
    ({"situacao": ["ERRO", "NAO_LOCALIZADO"]}, {2, 3}),
    ({"busca": "ciclano"}, {2}),
    ({"busca": "1400000009"}, {3}),
    ({"quem": "ANA"}, {"dois"}),
])
def test_os_filtros(lotes, filtro, esperado):
    from werkzeug.datastructures import MultiDict
    from app.apps.analisesps import comprovantes as cp
    um, dois = lotes
    args = MultiDict({"f": "1"})
    for k, v in filtro.items():
        for x in (v if isinstance(v, list) else [v]):
            args.add(k, x)
    itens = cp.listar(cp.filtros_da_tela(args))["itens"]
    achados = {("dois" if i["lote_id"] == dois else i["pagina"]) for i in itens}
    assert achados == esperado
    # A contagem por situação ignora o filtro de situação (é o atalho da tela).
    if "situacao" in filtro:
        assert cp.listar(cp.filtros_da_tela(args))["total"] == 4


def test_o_que_nao_baixou_vai_para_o_Drive_uma_vez_so(lotes, monkeypatch):
    from app.apps.analisesps import beevale, comprovantes as cp, drive
    um, _ = lotes
    subidos = []
    monkeypatch.setattr(beevale, "pasta_do_drive", lambda: ("PASTA", "tela"))
    monkeypatch.setattr(drive, "pasta_filha", lambda pasta, nome: f"{pasta}/{nome}")
    monkeypatch.setattr(drive, "subir_arquivo", lambda conteudo, nome, pasta, **k: (
        subidos.append((nome, pasta, conteudo[:5])) or
        {"id": f"d{len(subidos)}", "link": f"https://drive/{len(subidos)}"}))
    saida = cp.guardar_paginas_no_drive(um)
    assert saida["guardadas"] == 2 and saida["falhas"] == []
    assert {n.split(" - ")[2] for n, _, _ in subidos} == {"pág 2", "pág 3"}
    assert all(p == "PASTA/Comprovantes não baixados" and c == b"%PDF-" for _, p, c in subidos)
    assert cp.paginas_no_drive(um) == {2: "https://drive/1", 3: "https://drive/2"}
    # De novo (ou depois de "Processar de novo"): nada sobe duas vezes.
    assert cp.guardar_paginas_no_drive(um)["guardadas"] == 0 and len(subidos) == 2
    por = {i["pagina"]: i for i in cp.listar(_filtro())["itens"] if i["lote_id"] == um}
    assert por[2]["link_drive"] == "https://drive/1" and por[1]["link_drive"] == ""


def test_falha_no_Drive_nao_derruba_nada(lotes, monkeypatch):
    from app.apps.analisesps import beevale, comprovantes as cp, drive
    um, _ = lotes
    monkeypatch.setattr(beevale, "pasta_do_drive", lambda: ("PASTA", "tela"))
    monkeypatch.setattr(drive, "pasta_filha", lambda *a: 1 / 0)
    assert cp.guardar_paginas_no_drive(um)["falhas"]


def test_a_pagina_ainda_no_servidor_abre(lotes):
    from app.apps.analisesps import comprovantes as cp
    um, _ = lotes
    assert cp.pagina_do_servidor(um, 2).startswith(b"%PDF")
    os.remove(os.path.join(cp.PASTA, f"lote-{um}.pdf"))
    with pytest.raises(cp.ErroDeComprovante):
        cp.pagina_do_servidor(um, 2)


def test_a_tela_mostra_a_lista_os_filtros_e_o_link(lotes, app):  # noqa: F811
    from tests.test_analisesps_telas import _como_mestre
    cliente = _como_mestre(app)
    html = cliente.get("/analisesps/comprovantes?f=1&situacao=BAIXADO").get_data(as_text=True)
    assert 'id="form-filtros-comp"' in html and 'id="tabela-comprovantes"' in html
    assert "ver comprovante" in html and "1400000001" in html
    assert "CICLANO" not in html, "o filtro de situação vale na lista"
    um, _ = lotes
    pagina = cliente.get(f"/analisesps/comprovantes/{um}/pagina/2")
    assert pagina.status_code == 200 and pagina.get_data().startswith(b"%PDF")


# --- baixar de novo (19/11/2026) ----------------------------------------------
def test_baixar_de_novo_manda_as_paginas_num_envio_novo_e_tira_da_conta(lotes):
    from app.apps.analisesps import comprovantes as cp
    from app.apps.analisesps.db import consultar_um
    um, _ = lotes
    por = {(i["lote_id"], i["pagina"]): i for i in cp.listar(_filtro())["itens"]}
    assert por[(um, 2)]["pode_reenviar"] and por[(um, 3)]["pode_reenviar"]
    assert not por[(um, 1)]["pode_reenviar"], "o que baixou não se reenvia"
    feito = cp.reenviar([(um, 2), (um, 3)], "marcelo", "MARCELO")
    assert feito["paginas"] == 2 and feito["faltaram"] == []
    novo = consultar_um("SELECT arquivo, paginas, situacao FROM analisesps.comprovantes_lote "
                        " WHERE id = ?", (feito["lote"],))
    assert novo == ("Reenvio - 2 comprovantes.pdf", 2, "ESPERANDO")
    lista = cp.listar(_filtro())
    contagem = {s: q for s, _, q in lista["contagem"]}
    assert contagem[cp.REENVIADO] == 2 and contagem[cp.ERRO] == 0
    antigo = {i["pagina"]: i for i in lista["itens"] if i["lote_id"] == um}
    assert antigo[2]["rotulo"] == "Mandado de novo" and not antigo[2]["pede_acao"]
    assert f"envio {feito['lote']}" in antigo[2]["motivo"]


def test_sem_o_PDF_no_servidor_a_pagina_vem_do_Drive(lotes, monkeypatch):
    from app.apps.analisesps import beevale, comprovantes as cp, drive
    um, _ = lotes
    monkeypatch.setattr(beevale, "pasta_do_drive", lambda: ("PASTA", "tela"))
    monkeypatch.setattr(drive, "pasta_filha", lambda pasta, nome: "SUB")
    monkeypatch.setattr(drive, "subir_arquivo", lambda conteudo, nome, pasta, **k: {
        "id": "x", "link": f"https://drive.google.com/uc?export=download&id=ARQ{nome.split('pág ')[1][0]}"})
    cp.guardar_paginas_no_drive(um)
    os.remove(os.path.join(cp.PASTA, f"lote-{um}.pdf"))
    pedidos = []
    monkeypatch.setattr(drive, "baixar_arquivo",
                        lambda arquivo_id: pedidos.append(arquivo_id) or pdf_com(1))
    assert {i["pagina"] for i in cp.listar(_filtro())["itens"]
            if i["lote_id"] == um and i["pode_reenviar"]} == {2, 3}
    feito = cp.reenviar([(um, 3)], "marcelo", "MARCELO")
    assert pedidos == ["ARQ3"] and feito["paginas"] == 1


def test_sem_servidor_e_sem_Drive_nao_ha_o_que_reenviar(lotes):
    from app.apps.analisesps import comprovantes as cp
    um, _ = lotes
    os.remove(os.path.join(cp.PASTA, f"lote-{um}.pdf"))
    assert not any(i["pode_reenviar"] for i in cp.listar(_filtro())["itens"])
    with pytest.raises(cp.ErroDeComprovante):
        cp.reenviar([(um, 2)], "marcelo", "MARCELO")


def test_o_botao_da_tela_manda_de_novo(lotes, app, monkeypatch):  # noqa: F811
    from app.apps.analisesps import tarefas
    from tests.test_analisesps_telas import _como_mestre
    disparos = []
    monkeypatch.setattr(tarefas, "disparar", lambda modo, **k: disparos.append(modo) or {})
    cliente = _como_mestre(app)
    um, _ = lotes
    html = cliente.get("/analisesps/comprovantes?f=1").get_data(as_text=True)
    assert f'name="so" value="{um}:2"' in html and "Baixar de novo os marcados" in html
    r = cliente.post("/analisesps/comprovantes/reenviar", data={"so": f"{um}:2"})
    assert r.status_code == 302 and "mandado" in r.headers["Location"]
    assert disparos == ["comprovantes"]


# --- a linha nasce no envio (21/11/2026) ---------------------------------------
def test_ao_mandar_a_pagina_ja_aparece_aguardando_e_e_trocada_pelo_resultado(
        banco_analisesps, tmp_path, monkeypatch):
    """*"Na hora que mandasse para a baixa, ele já alimentasse a tabela e ficasse
    aguardando a baixa."*"""
    from app.apps.analisesps import comprovantes as cp
    monkeypatch.setattr(cp, "PASTA", str(tmp_path))
    monkeypatch.setattr(cp, "guardar_paginas_no_drive", lambda *a, **k: {})
    lote = cp.guardar(pdf_com(12), "novo.pdf", "marcelo", "MARCELO")
    itens = [i for i in cp.listar(_filtro())["itens"] if i["lote_id"] == lote]
    assert len(itens) == 12
    assert {(i["situacao"], i["andamento"]) for i in itens} == {(cp.AGUARDANDO, "na fila")}
    assert not any(i["pede_acao"] or i["pode_reenviar"] for i in itens)

    # A 1ª leva (págs. 1–10) volta só com a 1 baixada; a 2ª (11–12) cai.
    respostas = iter([{"planos": [{"match": {"status": "localizado", "id": "1400000001"},
                                   "pode_executar": True, "acao": "baixar_titulo",
                                   "receipt": {"page": 1, "valor_pago": "10,00"},
                                   "responses": {"omie": [{"step": "baixar",
                                                           "response": {"ok": True}}]}}]}])

    def robo(pedaco, nome, anotar, onde):
        try:
            return next(respostas)
        except StopIteration:
            raise RuntimeError("503 Service Unavailable")
    monkeypatch.setattr(cp, "_mandar_com_paciencia", robo)
    cp.processar_um(lote)
    por = {i["pagina"]: i for i in cp.listar(_filtro())["itens"] if i["lote_id"] == lote}
    assert len(por) == 12, "nenhuma página dobrada nem perdida"
    assert por[1]["situacao"] == cp.BAIXADO
    assert por[2]["situacao"] == cp.ERRO and "não devolveu" in por[2]["motivo"]
    assert por[11]["situacao"] == cp.ERRO and "parou antes" in por[11]["motivo"]
    assert por[11]["pode_reenviar"], "o que ficou para trás pode ser baixado de novo"
    assert {s: q for s, _, q in cp.listar(_filtro())["contagem"]}[cp.AGUARDANDO] == 0
