# -*- coding: utf-8 -*-
"""
01/10/2026 — a folha ANALÍTICA da contabilidade, o cadastro completo e os
botões do analítico do funcionário.

As linhas do relatório aqui são INVENTADAS no formato do Fortes (lido do arquivo
real de 09/2026, que não entra no repositório: tem nome e salário de ~500
pessoas).
"""
from decimal import Decimal as D

import pytest

from tests.test_analisesps_telas import app  # noqa: F401 — a fixture
from tests.test_analisesps_telas import (SENHA_CONSULTA, _como_mestre,
                                         _dias_do_mes, _preparar_folha_aberta,
                                         como)


def _linha(**colunas):
    """Uma linha de 21 colunas, como o .xls do Fortes."""
    l = [""] * 21
    for k, v in colunas.items():
        l[int(k[1:])] = v
    return l


def _relatorio():
    return [
        _linha(c0="Folha de Pagamento", c20="Pag.: 1"),
        _linha(c0="Empresa:", c2="BWS CONSTRUCOES LTDA"),
        _linha(c0="Mês/Ano: 09/2026"),
        _linha(c0="Emissão: 30/09/2026"),
        _linha(c0="Código", c2="Empregado", c7="Evento", c16="Referência",
               c19="Provento", c20="Desconto"),
        _linha(c0="001 - CONSTRUTORA"),
        _linha(c0="001.01 - CONSTRUTORA/ESCRITORIO"),
        # Pessoa A: salário, hora extra, INSS e adiantamento.
        _linha(c0="000013", c2="FULANO DE TAL"),
        _linha(c0="Cargo: Pedreiro"),
        _linha(c7="011 Salário-Base", c16="30 dia(s)", c19=2000.0),
        _linha(c7="064 Hora Extra 70%", c16="4h48min", c19=150.5),
        _linha(c7="310 INSS", c16="9%", c20=180.0),
        _linha(c7="300 Adiantamento Compensação", c20=800.0),
        _linha(c19=2150.5, c20=980.0),
        _linha(c15="FGTS: 172,04", c19="Líquido a receber:", c20=1170.5),
        _linha(c8="Data:", c9="/", c11="/", c14="Assinatura:"),
        _linha(c1="Admissão", c2="Dep.", c4="Filhos", c5="Hr/mês", c7="Sal. Cont.",
               c10="BC-INSS", c13="BC-FGTS"),
        _linha(c1=45658.0, c2=1.0, c4=0.0, c5="220:00", c7=2150.5, c10=2150.5,
               c13=2150.5),
        # Pessoa B, afastada: sem evento nenhum, com a situação embaixo.
        _linha(c0="000387", c2="BELTRANA"),
        _linha(c0="Cargo: Servente"),
        _linha(c1="Admissão", c2="Dep."),
        _linha(c1=45000.0, c2=0.0),
        _linha(c0="Licença por motivo de doença (24/09/2026 a 30/10/2026)"),
        # O TOTAL DO SETOR repete eventos: não pode virar evento de ninguém.
        _linha(c0="Total: 001.01 - CONSTRUTORA/ESCRITORIO", c19="(2 empregados)"),
        _linha(c7="011 Salário-Base", c19=2000.0),
        _linha(c7="310 INSS", c20=180.0),
        _linha(c19=2150.5, c20=980.0),
        _linha(c20="Continua..."),
        _linha(c0="Folha de Pagamento", c20="Pag.: 2"),
        _linha(c0="001.07 - CONSTRUTORA/NORDESTE"),
        _linha(c0="000500", c2="CICLANO"),
        _linha(c7="011 Salário-Base", c16="30 dia(s)", c19="1.500,00"),
        _linha(c7="310 INSS", c20="112,50"),
        _linha(c19="1.500,00", c20="112,50"),
        _linha(c19="Líquido a receber:", c20="1.387,50"),
    ]


def test_le_cada_pessoa_com_os_eventos_e_PULA_o_total_do_setor():
    from app.apps.analisesps import folha_analitica as fan
    f = fan.interpretar(_relatorio())
    assert f.competencia == "09/2026"
    assert [p.id_fortes for p in f.pessoas] == ["000013", "000387", "000500"]
    a, b, c = f.pessoas
    assert a.cargo == "Pedreiro" and a.setor.startswith("001.01")
    assert [e.codigo for e in a.eventos] == ["011", "064", "310", "300"]
    assert a.eventos[1].referencia == "4h48min"
    assert a.liquido == D("1170.50") and a.fgts == D("172.04")
    assert a.fecha, "proventos − descontos = líquido"
    assert a.admissao and a.dependentes == "1" and a.horas_mes == "220:00"
    assert b.eventos == [] and "Licença" in b.situacao and b.liquido is None
    # O total do setor não entrou em ninguém.
    assert len(c.eventos) == 2 and c.liquido == D("1387.50") and c.fecha
    assert c.setor.startswith("001.07")


def test_confere_com_a_sintetica_pelo_LIQUIDO():
    from app.apps.analisesps import folha_analitica as fan
    f = fan.interpretar(_relatorio())
    r = fan.conferir_com_a_sintetica(f, [
        {"id_fortes": "000013", "valor": D("1170.50")},
        {"id_fortes": "500", "valor": D("1000.00")}])
    assert r["batem"] == 1 and r["diferentes"] == ["000500"]
    assert r["so_na_analitica"] == ["000387"]


def test_importar_acha_a_FOLHA_QUE_BATE_e_guarda(banco_analisesps, monkeypatch):
    from app.apps.analisesps import folha_analitica as fan, folha_arquivo
    from app.apps.analisesps import folha_analitica_guardada as fag
    lida = fan.interpretar(_relatorio())
    monkeypatch.setattr(fan, "ler", lambda conteudo: lida)
    folhas = {
        1: {"id": 1, "ano": 2026, "mes": 9, "tipo": "quinzena", "linhas": [
            {"id_fortes": "000013", "valor": D("800.00")}]},
        2: {"id": 2, "ano": 2026, "mes": 9, "tipo": "fim_de_mes", "linhas": [
            {"id_fortes": "000013", "valor": D("1170.50")},
            {"id_fortes": "000500", "valor": D("1387.50")}]},
    }
    monkeypatch.setattr(folha_arquivo, "listar", lambda teto=60: list(folhas.values()))
    monkeypatch.setattr(folha_arquivo, "abrir", lambda i: folhas.get(i))
    r = fag.importar(b"x", nome_do_arquivo="Folha_de_Pagamento_09-2026.xls", quem="M")
    assert r["folha_id"] == 2 and r["tipo"] == "fim_de_mes" and r["batem"] == 2
    c = fag.da_pessoa(2026, 9, "fim_de_mes", "13")
    assert c["liquido"] == D("1170.50") and c["fecha"]
    assert [e["codigo"] for e in c["eventos"]] == ["011", "064", "310", "300"]
    assert fag.da_pessoa(2026, 9, "quinzena", "000013") is None
    # Importar de novo SUBSTITUI, não duplica.
    fag.importar(b"x", quem="M")
    assert fag.da_folha(2026, 9, "fim_de_mes")["pessoas"] == 3


def test_analitica_que_nao_bate_com_nenhuma_folha_e_RECUSADA(banco_analisesps, monkeypatch):
    from app.apps.analisesps import folha_analitica as fan, folha_arquivo
    from app.apps.analisesps import folha_analitica_guardada as fag
    monkeypatch.setattr(fan, "ler", lambda c: fan.interpretar(_relatorio()))
    monkeypatch.setattr(folha_arquivo, "listar", lambda teto=60: [])
    with pytest.raises(fag.ErroDaAnalitica, match="Importe primeiro a sintética"):
        fag.importar(b"x")


def test_a_rota_de_importar_RECONHECE_a_analitica(app, monkeypatch):
    from app.apps.analisesps import folha_analitica as fan
    from app.apps.analisesps import folha_analitica_guardada as fag
    import io
    monkeypatch.setattr(fan, "e_analitica", lambda c: True)
    monkeypatch.setattr(fag, "importar", lambda c, nome_do_arquivo="", quem="": {
        "folha_id": 7, "competencia": "09/2026", "tipo": "fim_de_mes",
        "pessoas": 438, "batem": 415, "diferentes": 0, "so_na_analitica": 23})
    r = _como_mestre(app).post("/analisesps/api/folha/importar", data={
        "folha": (io.BytesIO(b"xls"), "Folha_de_Pagamento.xls")},
        content_type="multipart/form-data")
    d = r.get_json()
    assert d["ok"] and d["analitica"] and d["ir"].endswith("/folha/7")
    assert "415" in d["mensagem"]


# ---------------------------------------------------------------------------
# O analítico do funcionário: contracheque, cadastro, botões
# ---------------------------------------------------------------------------
def test_o_analitico_mostra_o_CONTRACHEQUE_e_o_cadastro(app, monkeypatch):
    import datetime as dt
    from app.apps.analisesps import colaboradores
    from app.apps.analisesps import folha_analitica_guardada as fag
    _preparar_folha_aberta(monkeypatch, dias=_dias_do_mes())
    monkeypatch.setattr(fag, "da_pessoa", lambda ano, mes, tipo, idf: {
        "eventos": [{"codigo": "011", "descricao": "Salário-Base", "referencia": "30 dia(s)",
                     "provento": D("1200.00"), "desconto": D("0")},
                    {"codigo": "310", "descricao": "INSS", "referencia": "9%",
                     "provento": D("0"), "desconto": D("125.36")}],
        "soma_proventos": D("1200.00"), "soma_descontos": D("125.36"),
        "liquido": D("1074.64"), "fecha": True, "fgts": D("96.00"),
        "base_inss": D("1200.00"), "horas_mes": "220:00", "dependentes": "0",
        "situacao": ""})
    monkeypatch.setattr(colaboradores, "nascimento_de", lambda cpf: dt.date(1990, 5, 17))
    html = _como_mestre(app).get(
        "/analisesps/folha/1/pessoa/99713349334?parcial=1").get_data(as_text=True)
    assert "Contracheque" in html and "Salário-Base" in html and "INSS" in html
    assert "Os eventos explicam o líquido" in html
    assert "Igual ao da folha sintética" in html
    assert "997.133.493-34" in html and "17/05/1990" in html
    assert ">Cadastro completo</button>" in html


def test_sem_analitica_o_analitico_DIZ_como_trazer(app, monkeypatch):
    _preparar_folha_aberta(monkeypatch, dias=_dias_do_mes())
    html = _como_mestre(app).get(
        "/analisesps/folha/1/pessoa/99713349334?parcial=1").get_data(as_text=True)
    assert "A folha analítica deste pagamento ainda não foi" in html


def test_botoes_do_analitico_sem_SUBLINHADO_e_com_nome_curto(app, monkeypatch):
    """*"não quero nada desses botões sublinhados (…) vamos botar só Pipefy."*"""
    import re
    from pathlib import Path
    _preparar_folha_aberta(monkeypatch, dias=_dias_do_mes(), cadastro={
        "99713349334": {"cpf": "99713349334", "nome": "GERLANIO", "fase": "",
                        "obra_cadastro": "", "obra_codigo": "", "situacao": "ativo",
                        "motivo": "", "link_pipefy": "https://app.pipefy.com/open-cards/1"}})
    html = _como_mestre(app).get(
        "/analisesps/folha/1/pessoa/99713349334?parcial=1").get_data(as_text=True)
    acoes = html[html.index('class="analitico-acoes"'):]
    acoes = acoes[:acoes.index("</div>")]
    assert "link-btn" not in acoes, "sobrou link sublinhado entre os botões"
    for rotulo in (">Relatório</a>", ">Atualizar ponto</button>",
                   ">Atualizar cadastro</button>", ">Cadastro completo</button>",
                   ">Pipefy</a>"):
        assert rotulo in acoes, rotulo
    css = Path("app/apps/analisesps/static/analisesps.css").read_text(encoding="utf-8")
    assert ".analitico-acoes .btn, .analitico-acoes a.btn { text-decoration: none; }" in css


def test_cadastro_completo_AGRUPA_e_tira_o_que_nao_e_da_pessoa(monkeypatch):
    from app.apps.analisesps import colaboradores as col
    monkeypatch.setattr(col, "_linha_da_pessoa", lambda cpf: (
        ["Nome Completo", "Data de Nascimento", "Nome da Mãe", "PIS", "Celular",
         "Cargo [ ]", "Data de Saída", "Link do Card", "Anexo RG", "Senha", "Hobby"],
        ["FULANO", "17/05/1990", "MARIA", "123", "(85) 9", "Pedreiro", "",
         "https://x", "https://y", "abc", "futebol"]))
    grupos = dict(col.ficha_completa("99713349334"))
    assert ("Data de Nascimento", "17/05/1990") in grupos["Pessoais"]
    assert ("Nome da Mãe", "MARIA") in grupos["Pessoais"]
    assert ("PIS", "123") in grupos["Documentos"]
    assert ("Cargo [ ]", "Pedreiro") in grupos["Contrato"]
    assert ("Hobby", "futebol") in grupos["Outros"]
    tudo = [c for campos in grupos.values() for c, _ in campos]
    assert "Link do Card" not in tudo and "Anexo RG" not in tudo and "Senha" not in tudo
    assert "Data de Saída" not in tudo, "vazio não aparece"


def test_a_rota_do_cadastro_completo(app, monkeypatch):
    from app.apps.analisesps import colaboradores as col
    _preparar_folha_aberta(monkeypatch, dias=_dias_do_mes())
    monkeypatch.setattr(col, "ficha_completa", lambda cpf: [
        ("Pessoais", [("Data de Nascimento", "17/05/1990")])])
    html = _como_mestre(app).get(
        "/analisesps/folha/1/pessoa/99713349334/cadastro").get_data(as_text=True)
    assert "Data de Nascimento" in html and "17/05/1990" in html
    fora = _como_mestre(app).get("/analisesps/folha/1/pessoa/52998224725/cadastro")
    assert fora.status_code == 404


def test_atualizar_cadastro_de_UMA_pessoa(app, monkeypatch):
    from app.apps.analisesps import colaboradores as col
    pedidos = []
    monkeypatch.setattr(col, "atualizar_uma",
                        lambda cpf: pedidos.append(cpf) or {"nome": "GERLANIO", "cpf": cpf})
    r = _como_mestre(app).post("/analisesps/api/folha/cadastro/pessoa",
                               json={"cpf": "997.133.493-34"})
    assert r.get_json()["ok"] and pedidos == ["99713349334"]
    r = como(app, SENHA_CONSULTA).post("/analisesps/api/folha/cadastro/pessoa",
                                       json={"cpf": "99713349334"})
    assert r.status_code in (302, 403, 404)


class _AbaDeUmaPessoa:
    """A aba "Dados Documentos" mínima: cabeçalho, a coluna do CPF e uma linha."""
    def __init__(self):
        self.cabecalho = ["CPF (Cadastro de Pessoa Física)", "Nome Completo",
                          "Cargo [ ]", "Data de Nascimento", "Fase Atual"]
        self.linhas = {3: ["111.222.333-96", "OUTRA", "Servente", "", ""],
                       4: ["997.133.493-34", "GERLANIO", "Pedreiro", "17/05/1990",
                           "Colaboradores ativos"]}
        self.pedidas = []

    def row_values(self, n):
        self.pedidas.append(n)
        return self.cabecalho if n == 1 else self.linhas.get(n, [])

    def col_values(self, n):
        return ["CPF", ""] + [self.linhas[k][n - 1] for k in sorted(self.linhas)]


def test_atualizar_uma_LE_SO_A_LINHA_da_pessoa(banco_analisesps, monkeypatch):
    from app.apps.analisesps import colaboradores as col
    aba = _AbaDeUmaPessoa()
    monkeypatch.setattr(col, "_aba", lambda planilha, nome: aba)
    r = col.atualizar_uma("99713349334")
    assert r["nome"] == "GERLANIO" and r["cargo"] == "Pedreiro"
    assert aba.pedidas == [1, 4], "leu mais que o cabeçalho e a linha dela"
    assert col.por_cpf("99713349334")["cargo"] == "Pedreiro"
    import datetime as dt
    assert col.nascimento_de("99713349334") == dt.date(1990, 5, 17)


# ---------------------------------------------------------------------------
# REIMPORTAR: o que mudou — *"esse veio com valor diferente, esse foi eliminado
# da folha, esse entrou."*
# ---------------------------------------------------------------------------
def _sintetica(*pessoas):
    from app.apps.analisesps.folha_sintetica import FolhaLida, LinhaDaFolha
    f = FolhaLida(titulo="Folha Sintética - Folha de Pagamento", empresa="BWS",
                  cnpj="", mes=9, ano=2026)
    for idf, nome, valor in pessoas:
        f.linhas.append(LinhaDaFolha(id_fortes=idf, nome=nome, valor=D(valor),
                                     filial_codigo="001", filial_nome="MATRIZ"))
    f.filiais = {"001": {"nome": "MATRIZ", "total": f.total}}
    return f


def test_comparar_versoes_diz_QUEM_ENTROU_SAIU_E_MUDOU():
    from app.apps.analisesps import folha_arquivo as fa
    novo = _sintetica(("000013", "A", "1100.00"), ("000500", "C", "900.00"))
    r = fa.comparar_versoes([("000013", "A", D("1000.00")), ("000387", "B", D("500.00"))],
                            novo.linhas)
    assert [x["id_fortes"] for x in r["mudaram"]] == ["000013"]
    assert r["mudaram"][0]["de"] == "1000.00" and r["mudaram"][0]["para"] == "1100.00"
    assert [x["id_fortes"] for x in r["entraram"]] == ["000500"]
    assert [x["id_fortes"] for x in r["sairam"]] == ["000387"]
    assert r["total_antes"] == "1500.00" and r["total_depois"] == "2000.00"
    assert not r["nada_mudou"]


def test_REIMPORTAR_guarda_o_que_mudou_e_a_primeira_vez_nao(banco_analisesps, monkeypatch):
    from app.apps.analisesps import folha_arquivo as fa, folha_sintetica as fs
    monkeypatch.setattr(fa, "casar_com_o_cadastro", lambda i: {"casadas": 0, "pendentes": 0})
    monkeypatch.setattr(fs, "ler", lambda c: _sintetica(("000013", "A", "1000.00"),
                                                         ("000387", "B", "500.00")))
    primeira = fa.importar(b"v1", tipo="fim_de_mes")
    assert fa.mudancas(primeira["id"]) is None, "na primeira importação não há o que comparar"
    monkeypatch.setattr(fs, "ler", lambda c: _sintetica(("000013", "A", "1100.00"),
                                                         ("000500", "C", "900.00")))
    segunda = fa.importar(b"v2", tipo="fim_de_mes")
    m = fa.mudancas(segunda["id"])
    assert len(m["mudaram"]) == 1 and len(m["entraram"]) == 1 and len(m["sairam"]) == 1
    assert m["fechada_antes"] is False


def test_a_folha_MOSTRA_o_que_mudou(app, monkeypatch):
    from app.apps.analisesps import folha_arquivo as fa
    _preparar_folha_aberta(monkeypatch, dias=_dias_do_mes())
    monkeypatch.setattr(fa, "mudancas", lambda i: {
        "mudaram": [{"id_fortes": "000013", "nome": "GERLANIO", "de": "1000.00",
                     "para": "1074.64", "diferenca": "74.64"}],
        "entraram": [], "sairam": [{"id_fortes": "000999", "nome": "FULANO", "valor": "300.00"}],
        "total_antes": "2662.56", "total_depois": "2437.20", "nada_mudou": False,
        "fechada_antes": True})
    html = _como_mestre(app).get("/analisesps/folha/1").get_data(as_text=True)
    assert "O que mudou nesta reimportação" in html
    assert "1 com valor diferente, 0 entraram," in html and "1 saíram" in html
    assert "refaça o fechamento" in html
    assert "saiu da folha" in html and "valor mudou" in html


# ---------------------------------------------------------------------------
# 01/10/2026 — *"Pendências: 2 já saíram da empresa (…) mas no filtro: já saiu (0)"*
# ---------------------------------------------------------------------------
def test_quem_SAIU_e_esta_SEM_OBRA_aparece_no_filtro_ja_saiu(app, monkeypatch):
    import datetime as dt
    from app.apps.analisesps import colaboradores
    # LUELIA não tem ponto (sem obra) e saiu no cadastro.
    _preparar_folha_aberta(monkeypatch, dias=_dias_do_mes(), cadastro={
        "99713349334": {"cpf": "99713349334", "nome": "GERLANIO", "fase": "Colaboradores ativos",
                        "obra_cadastro": "CREPEOLINDA", "obra_codigo": "",
                        "situacao": colaboradores.SITUACAO_ATIVO, "motivo": "", "link_pipefy": ""},
        "11122233396": {"cpf": "11122233396", "nome": "LUELIA", "fase": "Desligados",
                        "obra_cadastro": "CREPEOLINDA", "obra_codigo": "",
                        "situacao": colaboradores.SITUACAO_SAIU,
                        "motivo": "saiu em 31/08/2026", "link_pipefy": ""}})
    cliente = _como_mestre(app)
    html = cliente.get("/analisesps/folha/1").get_data(as_text=True)
    import re
    rotulo = re.search(r'value="saiu"[^>]*>\s*<span class="rotulo"[^>]*>([^<]+)<', html)
    assert rotulo and "(1)" in rotulo.group(1), rotulo and rotulo.group(1)
    so_saiu = cliente.get("/analisesps/folha/1?situacao=saiu").get_data(as_text=True)
    assert "LUELIA" in so_saiu and "GERLANIO GOMES" not in so_saiu
