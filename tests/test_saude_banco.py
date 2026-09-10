"""A saúde do sistema — com banco de verdade.

Ela existe porque a pergunta do dono ("o sistema aguenta crescer?") foi
respondida com raciocínio, e a próxima — "vale a pena gastar mais?" — não pode
ser respondida com palpite.

O que se prova:

  1. Os tempos se acumulam na memória e descem ao banco AGREGADOS por dia e
     rota — uma linha por requisição faria a tabela de medição virar o problema
     que ela veio medir.
  2. Gravar duas vezes SOMA em cima, porque o processo reinicia a cada 150
     requisições e o dia é montado em pedaços.
  3. A medição NUNCA derruba nada: banco fora, erro engolido.
  4. As telas mais lentas saem ordenadas pelo TEMPO TOTAL, não pela média.
  5. Os avisos transformam número em decisão.
"""
from __future__ import annotations

from datetime import date, timedelta

import pytest
from sqlalchemy import text

from app.apps.erp.core.comum import saude

pytestmark = pytest.mark.banco


@pytest.fixture(autouse=True)
def limpar(sessao_real):
    """Cada teste começa com o acumulador e a tabela limpos."""
    with saude._trava:
        saude._acumulado.clear()
    sessao_real.execute(text("DELETE FROM saude_tempos"))
    sessao_real.flush()
    yield
    with saude._trava:
        saude._acumulado.clear()


def _gravar(sessao_real):
    """Grava usando a conexão do teste (a sessão real é uma transação que não
    se confirma — usar o motor de verdade escreveria fora dela)."""
    with saude._trava:
        pendente = dict(saude._acumulado)
        saude._acumulado.clear()
    for (dia, rota), v in pendente.items():
        sessao_real.execute(text("""
            INSERT INTO saude_tempos (dia, rota, chamadas, ms_total, ms_maior, erros, lentas)
            VALUES (:dia, :rota, :ch, :tot, :maior, :err, :lent)
            ON CONFLICT (dia, rota) DO UPDATE SET
                chamadas = saude_tempos.chamadas + EXCLUDED.chamadas,
                ms_total = saude_tempos.ms_total + EXCLUDED.ms_total,
                ms_maior = GREATEST(saude_tempos.ms_maior, EXCLUDED.ms_maior),
                erros    = saude_tempos.erros + EXCLUDED.erros,
                lentas   = saude_tempos.lentas + EXCLUDED.lentas
        """), {"dia": dia, "rota": rota, "ch": v["chamadas"], "tot": v["ms_total"],
               "maior": v["ms_maior"], "err": v["erros"], "lent": v["lentas"]})
    sessao_real.flush()
    return len(pendente)


# ---------------------------------------------------------------------------
# 1 e 2. A coleta
# ---------------------------------------------------------------------------
def test_mil_chamadas_viram_uma_linha_por_rota(sessao_real):
    """Uma linha por requisição faria a tabela de medição virar o problema que
    ela veio medir."""
    for i in range(1000):
        saude.registrar("erp.pagina_titulos", 100 + i % 10)
    _gravar(sessao_real)

    linhas = sessao_real.execute(text("SELECT rota, chamadas FROM saude_tempos")).all()
    assert len(linhas) == 1
    assert linhas[0][1] == 1000


def test_a_gravacao_soma_em_cima_da_anterior(sessao_real):
    """O processo reinicia a cada 150 requisições: o dia é montado em pedaços,
    e cada pedaço tem de somar ao que já estava lá."""
    saude.registrar("erp.pagina_titulos", 100)
    _gravar(sessao_real)
    saude.registrar("erp.pagina_titulos", 300)
    _gravar(sessao_real)

    ch, tot, maior = sessao_real.execute(text(
        "SELECT chamadas, ms_total, ms_maior FROM saude_tempos")).one()
    assert ch == 2
    assert tot == 400
    assert maior == 300, "o pior caso é o maior dos dois pedaços, não o último"


def test_a_chamada_lenta_e_contada_a_parte(sessao_real):
    """Não é erro — é o limite acima do qual a pessoa percebe que esperou."""
    saude.registrar("erp.pagina_titulos", 200)
    saude.registrar("erp.pagina_titulos", saude.LIMITE_LENTA_MS + 1)
    _gravar(sessao_real)
    ch, lentas = sessao_real.execute(text(
        "SELECT chamadas, lentas FROM saude_tempos")).one()
    assert (ch, lentas) == (2, 1)


def test_o_erro_e_contado_a_parte(sessao_real):
    saude.registrar("erp.api_titulos", 50, erro=True)
    saude.registrar("erp.api_titulos", 50)
    _gravar(sessao_real)
    assert sessao_real.execute(text("SELECT erros FROM saude_tempos")).scalar() == 1


def test_o_acumulador_tem_teto_contra_rota_gerada(sessao_real):
    """Trava de segurança: rota com identificador dentro viraria mil chaves
    diferentes e encheria a memória do processo."""
    for i in range(saude.MAX_ROTAS_NA_MEMORIA + 50):
        saude.registrar(f"rota_inventada_{i}", 10)
    with saude._trava:
        assert len(saude._acumulado) == saude.MAX_ROTAS_NA_MEMORIA


# ---------------------------------------------------------------------------
# 3. Medir nunca derruba
# ---------------------------------------------------------------------------
def test_registrar_nunca_levanta(sessao_real):
    """Está dentro do `after_request`: se levantasse, derrubaria a resposta."""
    saude.registrar(None, 10)
    saude.registrar("x" * 500, 10)
    saude.registrar("erp.x", 0)


def test_gravar_com_o_banco_fora_nao_levanta(sessao_real, monkeypatch):
    """Perder a medição é aceitável; derrubar a tela por causa dela não."""
    saude.registrar("erp.pagina_titulos", 100)

    def sem_banco():
        raise RuntimeError("banco fora do ar")
    monkeypatch.setattr("app.apps.erp.db.database.obter_engine", sem_banco)
    assert saude.gravar() == 0


def test_a_leitura_com_a_tabela_ausente_nao_levanta(sessao_real, monkeypatch):
    """Se a migração 054 ainda não rodou, a tela abre vazia em vez de quebrar."""
    sessao_real.execute(text("DROP TABLE IF EXISTS saude_tempos_falsa"))
    linhas = saude._consultar(sessao_real, "SELECT * FROM tabela_que_nao_existe")
    assert linhas == []


# ---------------------------------------------------------------------------
# 4. A leitura
# ---------------------------------------------------------------------------
def test_as_telas_saem_pelo_tempo_total_e_nao_pela_media(sessao_real):
    """Uma tela de três segundos aberta uma vez por mês incomoda menos que uma
    de meio segundo aberta duzentas vezes por dia."""
    saude.registrar("erp.relatorio_pesado", 3000)          # média 3000, total 3s
    for _ in range(200):
        saude.registrar("erp.pagina_titulos", 500)         # média 500, total 100s
    _gravar(sessao_real)

    t = saude.telas(sessao_real)
    assert t["rotas"][0]["rota"] == "erp.pagina_titulos"
    assert t["rotas"][0]["media_ms"] == 500
    assert t["rotas"][0]["segundos_no_periodo"] == 100.0
    assert t["rotas"][1]["rota"] == "erp.relatorio_pesado"


def test_o_periodo_recorta_de_verdade(sessao_real):
    sessao_real.execute(text("""
        INSERT INTO saude_tempos (dia, rota, chamadas, ms_total, ms_maior)
        VALUES (:velho, 'erp.antiga', 10, 1000, 100)
    """), {"velho": date.today() - timedelta(days=40)})
    saude.registrar("erp.nova", 100)
    _gravar(sessao_real)

    assert [r["rota"] for r in saude.telas(sessao_real, dias=7)["rotas"]] == ["erp.nova"]
    assert len(saude.telas(sessao_real, dias=60)["rotas"]) == 2


def test_a_memoria_sai_lida_do_sistema(sessao_real):
    m = saude.memoria()
    assert m["rss_mb"] and m["rss_mb"] > 0
    assert m["teto_mb"] == 2048
    assert 0 < m["pct"] < 100


def test_o_banco_diz_o_tamanho_e_o_que_mais_ocupa(sessao_real):
    b = saude.banco(sessao_real)
    assert b["total_mb"] > 0
    assert b["tabelas"], "as maiores tabelas"
    tamanhos = [t["mb"] for t in b["tabelas"]]
    assert tamanhos == sorted(tamanhos, reverse=True), "da maior para a menor"


def test_linha_desconhecida_volta_como_nada_e_nao_como_zero(sessao_real):
    """A contagem do Postgres é estimativa da última análise. Numa tabela
    ainda não analisada ela é desconhecida — e "0 linhas" ao lado de uma
    tabela de 300 KB seria uma afirmação falsa."""
    b = saude.banco(sessao_real)
    for t in b["tabelas"]:
        assert t["linhas"] is None or t["linhas"] > 0, \
            "ou se sabe quantas são, ou se diz que não se sabe"


# ---------------------------------------------------------------------------
# 5. Os avisos
# ---------------------------------------------------------------------------
def test_sem_medicao_o_panorama_explica_em_vez_de_ficar_mudo(sessao_real):
    p = saude.panorama(sessao_real)
    assert any("Ainda não há medições" in a for a in p["avisos"])


def test_o_aviso_de_lentidao_aparece_quando_passa_do_limite(sessao_real):
    for _ in range(10):
        saude.registrar("erp.pagina_titulos", saude.LIMITE_LENTA_MS + 100)
    _gravar(sessao_real)
    p = saude.panorama(sessao_real)
    assert any("passaram de" in a for a in p["avisos"])


def test_o_panorama_traz_tudo_que_a_tela_mostra(sessao_real):
    saude.registrar("erp.pagina_titulos", 120)
    _gravar(sessao_real)
    p = saude.panorama(sessao_real)
    for chave in ("memoria", "banco", "telas", "anexos", "por_dia", "avisos"):
        assert chave in p
    assert p["telas"]["chamadas"] == 1


def test_uma_consulta_que_falha_nao_apaga_o_painel_inteiro(sessao_real):
    """No Postgres, uma consulta que falha aborta a transação e todas as
    seguintes falham junto. Sem isolamento, uma tabela que ainda não existe
    deixaria o painel INTEIRO em branco — e painel vazio faz a pessoa achar
    que o sistema parou."""
    saude.registrar("erp.pagina_titulos", 100)
    _gravar(sessao_real)

    assert saude._consultar(sessao_real, "SELECT * FROM tabela_que_nao_existe") == []
    # e a sessão continua utilizável logo depois
    p = saude.panorama(sessao_real)
    assert p["telas"]["chamadas"] == 1
    assert p["banco"]["tabelas"], "as demais leituras continuam funcionando"
