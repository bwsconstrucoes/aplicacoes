# -*- coding: utf-8 -*-
"""
A TELA DE FATURAMENTO — 09/10/2026 (migração 053), com banco de verdade.

O dono: *"numa nova tela, que a gente pode chamar de Faturamento, eu quero fazer
o controle de notas — ver faturamento, fazer o download da nota, uma parte
gráfica de evolução"*. A fonte é a aba "Base Faturamento"; a obra (empresa, SCP)
vem da C. Diários. Filtro, soma e agrupamento vivem em SQL — por isso aqui.
"""
import datetime as dt
from decimal import Decimal

import pytest

from tests.test_analisesps_usuarios_banco import (  # noqa: F401 — fixtures
    SENHA_MESTRE_OPERADOR, app, banco_acesso)

pytestmark = pytest.mark.banco

CAB = ["nota_numero", "nota_sequencial", "modelo", "data_emissao", "competencia",
       "status", "observacao", "obra_codigo", "medicao_numero", "tomador_nome",
       "valor_total", "valor_liquido_previsto", "pis", "retem_pis", "iss",
       "retem_iss", "omie_pis", "data_recebimento", "valor_recebido",
       "link_xml", "link_nfse_nacional", "discriminacao"]


def nota(numero, seq, data, obra, valor, liquido="", status="valida",
         recebido="", data_rec="", pis="", tomador="PREFEITURA X"):
    d = dict.fromkeys(CAB, "")
    d.update(nota_numero=numero, nota_sequencial=seq, modelo="nacional",
             data_emissao=data, competencia=data[:7], status=status,
             obra_codigo=obra, tomador_nome=tomador, valor_total=valor,
             valor_liquido_previsto=liquido, pis=pis, retem_pis="S" if pis else "",
             data_recebimento=data_rec, valor_recebido=recebido,
             link_xml=f"https://drive/xml/{seq}", discriminacao="Medição 11")
    return [d[c] for c in CAB]


OBRAS = [["Código", "Código Primário", "Cliente", "Contrato", "Empresa", "SCP", "Município"],
         ["CREPE2", "CREPEEXU", "ESTADO PE", "CT-01", "BWS", "", "Exu-PE"],
         ["IFS2", "IFSPSAOJOSE", "IF SERTÃO", "CT-02", "BWS", "SCP IF", "São José"]]


class Aba:
    def __init__(self, valores):
        self.valores = valores

    def get_all_values(self):
        return self.valores


@pytest.fixture
def carregado(app, monkeypatch):
    from app.apps.analisesps import faturamento, sincronizacao, tarefas
    notas = [CAB,
             nota("2600000003283", "3283", "2026-10-08", "IFSPSAOJOSE", "10.000,00",
                  liquido="9.000,00", pis="65,00"),
             nota("2600000003284", "3284", "2026-10-09", "CREPEEXU", "5.000,00",
                  liquido="4.500,00", recebido="4.500,00", data_rec="2026-10-20"),
             nota("3100", "3100", "2026-08-15", "crepeexu", "2.000,00",
                  status="cancelada"),
             nota("3050", "3050", "15/07/2026", "IFS2", "1.000,00"),
             nota("2600000003283", "3283", "2026-10-08", "REPETIDA", "999,00")]
    abas = {(faturamento.PLANILHA_NOTAS, faturamento.ABA_BASE): Aba(notas),
            (faturamento.PLANILHA_OBRAS, "Centro de Custo"): Aba(OBRAS)}
    monkeypatch.setattr(sincronizacao, "_aba", lambda p, n: abas[(p, n)])
    monkeypatch.setattr(tarefas, "disparar", lambda *a, **k: {"ok": False})
    r = faturamento.carregar()
    assert r == {"notas": 4, "obras": 4, "avisos": []}, r
    return app


def test_a_carga_traz_as_notas_e_cruza_a_obra_pelos_DOIS_codigos(carregado):
    from app.apps.analisesps import faturamento
    n = faturamento.uma("2600000003283")
    assert n["obra"] == "IFSPSAOJOSE" and n["empresa"] == "SCP IF" and n["e_scp"]
    assert n["valor_total"] == Decimal("10000.00"), "a repetida não substitui a primeira"
    # pelo código secundário (coluna A) também acha a obra
    assert faturamento.uma("3050")["contrato"] == "CT-02"
    # minúscula/espaço no código da obra não separa
    assert faturamento.uma("3100")["empresa"] == "BWS"


def test_tributo_VAZIO_continua_vazio_e_nao_vira_zero(carregado):
    from app.apps.analisesps import faturamento
    pis = {t["nome"]: t for t in faturamento.uma("2600000003283")["tributos"]}
    assert pis["PIS"]["valor"] == Decimal("65.00") and pis["PIS"]["retem"] == "S"
    assert pis["COFINS"]["valor"] is None, "nota antiga sem tributo = não se sabe"


def test_resumo_evolucao_e_filtros(carregado):
    from app.apps.analisesps import faturamento
    tudo = {"de": dt.date(2026, 1, 1), "ate": None, "status": "valida"}
    r = faturamento.resumo(tudo)
    assert r["quantidade"] == 3, "a cancelada fica de fora por padrão"
    assert r["bruto"] == Decimal("16000.00")
    assert r["recebido"] == Decimal("4500.00")
    assert r["abertas"] == 2
    assert r["a_receber"] == Decimal("10000.00"), "9.000 líquido + 1.000 sem líquido"
    meses = faturamento.por_periodo(tudo, "mes")
    # mês sem nota aparece com zero — o "buraco" é informação
    assert [(m["rotulo"], m["bruto"]) for m in meses] == [
        ("07/2026", Decimal("1000.00")), ("08/2026", 0), ("09/2026", 0),
        ("10/2026", Decimal("15000.00"))]
    assert meses[-1]["fim"] == dt.date(2026, 10, 31)
    assert meses[-1]["recebido"] == Decimal("4500.00")
    tri = faturamento.por_periodo(tudo, "trimestre")
    assert [(t["rotulo"], t["bruto"]) for t in tri] == [
        ("3º tri/2026", Decimal("1000.00")), ("4º tri/2026", Decimal("15000.00"))]
    assert [a["rotulo"] for a in faturamento.por_periodo(tudo, "ano")] == ["2026"]
    assert faturamento.resumo(dict(tudo, empresas=["SCP IF"]))["quantidade"] == 2
    assert faturamento.resumo(dict(tudo, empresas=["SCP IF", "BWS"]))["quantidade"] == 3
    assert faturamento.resumo(dict(tudo, obras=["crepeexu"]))["quantidade"] == 1
    assert faturamento.resumo(dict(tudo, status="cancelada"))["bruto"] == Decimal("2000.00")
    assert faturamento.resumo(dict(tudo, recebimento="recebidas"))["quantidade"] == 1
    assert [n["sequencial"] for n in faturamento.listar(dict(tudo, busca="3284"))] == ["3284"]
    assert faturamento.opcoes()["empresas"] == ["BWS", "SCP IF"]


def test_a_TELA_mostra_as_notas_o_grafico_e_a_ficha(carregado):
    with carregado.test_client() as cliente:
        cliente.post("/analisesps/entrar", data={"senha": SENHA_MESTRE_OPERADOR})
        tela = cliente.get("/analisesps/faturamento?f=1&de=2026-01-01").get_data(as_text=True)
        periodos = cliente.get("/analisesps/faturamento/periodos?f=1&de=2026-01-01&agrupar=trimestre"
                               ).get_data(as_text=True)
        ficha = cliente.get("/analisesps/faturamento/nota/2600000003283").get_data(as_text=True)
        assert cliente.get("/analisesps/faturamento/nota/999").status_code == 404
    # a tela principal é a lista, como a planilha; o filtro mora na barra lateral
    assert "3283" in tela and 'id="form-filtros-fat"' in tela and "Total do filtro" in tela
    assert "fat-coluna" not in tela, "o gráfico mora na subtela Por período"
    assert "Faturado por trimestre" in periodos and periodos.count('class="fat-mes-col"') == 2
    assert "de=2026-10-01" in periodos and "ate=2026-12-31" in periodos, "o período leva às notas dele"
    assert "https://drive/xml/3283" in tela, "o download da nota"
    assert "SCP IF" in ficha and "Medição 11" in ficha and "65,00" in ficha
    # divergência VAZIA = não conferido, nunca "bate"
    assert "ainda não conferidos com o Omie" in ficha and "batem" not in ficha


def test_ATUALIZAR_com_outra_tarefa_rodando_fica_na_fila_e_comeca_sozinha(app, monkeypatch):
    """09/10/2026: *"cliquei em atualizar planilha e apareceu: já existe uma
    atualização em andamento (importando o cadastro de colaboradores)"*. Só roda
    uma tarefa de fundo por vez; a carga das notas fica pedida e começa quando a
    outra terminar."""
    from app.apps.analisesps import tarefas
    disparos = []
    monkeypatch.setattr(tarefas, "disparar", lambda modo, disparo="manual": (
        disparos.append(modo) or {"ok": False, "erro": "Já existe uma atualização"}))
    with app.test_client() as cliente:
        cliente.post("/analisesps/entrar", data={"senha": SENHA_MESTRE_OPERADOR})
        r = cliente.post("/analisesps/faturamento/atualizar", data={})
    assert "fila" in r.location
    assert tarefas._pedido_pendente("faturamento")
    monkeypatch.setattr(tarefas, "disparar", lambda modo, disparo="manual": (
        disparos.append(modo) or {"ok": True}))
    tarefas.encadear_comprovantes("colaboradores")     # a outra terminou
    assert disparos[-1] == "faturamento"
    assert not tarefas._pedido_pendente("faturamento"), "o pedido é atendido uma vez só"


def test_IMPORTAR_com_outra_tarefa_rodando_fica_na_fila_e_comeca_sozinha(app, monkeypatch):
    """09/10/2026: *"Outra tarefa de fundo está rodando agora (…) Tente de novo
    em alguns minutos."* Recusar e mandar voltar deixava o dono clicando de
    novo; a importação fica pedida, como o "Atualizar da planilha"."""
    from app.apps.analisesps import tarefas
    disparos = []
    monkeypatch.setattr(tarefas, "disparar", lambda modo, disparo="manual": (
        disparos.append(modo) or {"ok": False, "erro": "Já existe uma atualização"}))
    with app.test_client() as cliente:
        cliente.post("/analisesps/entrar", data={"senha": SENHA_MESTRE_OPERADOR})
        r = cliente.post("/analisesps/faturamento/importar", data={})
        tela = cliente.get("/analisesps/faturamento").get_data(as_text=True)
    assert "fila" in r.location and "Tente de novo" not in r.location
    assert tarefas._pedido_pendente("faturamento_antigas")
    assert "A importação das notas antigas está na fila" in tela
    monkeypatch.setattr(tarefas, "disparar", lambda modo, disparo="manual": (
        disparos.append(modo) or {"ok": True}))
    tarefas.encadear_comprovantes("colaboradores")     # a outra terminou
    assert disparos[-1] == "faturamento_antigas"
    assert not tarefas._pedido_pendente("faturamento_antigas")
    # a importação já termina trazendo as notas: a carga simples não vem depois
    assert not tarefas._pedido_pendente("faturamento")


def test_ABA_VAZIA_e_dita_com_todas_as_letras(app, monkeypatch):
    """A carga rodou e a aba só tinha o cabeçalho: a tela dizia "notas trazidas
    às 18:38" e não mostrava nada — sem explicar qual dos dois estava errado."""
    from app.apps.analisesps import faturamento, tarefas
    from app.apps.analisesps.db import conexao
    from app.apps.analisesps.sincronizacao import _meta_gravar
    monkeypatch.setattr(tarefas, "disparar", lambda modo, disparo="manual": {"ok": False})
    with conexao() as conn:
        conn.execute("DELETE FROM analisesps.faturamento_nota")
        conn.commit()
        _meta_gravar(conn, faturamento.CHAVE_META, "2026-10-09T18:38:00-03:00")
    monkeypatch.setattr("app.apps.analisesps.web._faturamento_desatualizado",
                        lambda _c: False)
    with app.test_client() as cliente:
        cliente.post("/analisesps/entrar", data={"senha": SENHA_MESTRE_OPERADOR})
        tela = cliente.get("/analisesps/faturamento").get_data(as_text=True)
    assert "0 nota(s)" in tela
    assert 'A aba "Base Faturamento" está vazia' in tela and "Importar notas antigas" in tela
