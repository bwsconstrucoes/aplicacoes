# -*- coding: utf-8 -*-
"""
A PERGUNTA MONTADA NA HORA — o mapa, as travas e o recorte (27/11/2026).

⚑ POR QUE COM BANCO DE VERDADE: tudo que importa aqui é SQL. O mapa são
consultas; o recorte vive num `WHERE`; a trava do prazo é do Postgres. A
sessão dublada da suíte ignora exatamente isso.

⚠️ O QUE ESTE ARQUIVO NÃO TESTA, e está dito para ninguém se enganar: **a IA**.
Nenhum teste aqui chama a OpenAI — o que se testa é o que acontece com o SQL
*depois* que ela responde, inclusive quando ela responde besteira. É de
propósito: a parte que pode estragar o banco é esta, e ela tem de ser
conferível sem gastar um centavo nem depender da internet.
"""
from __future__ import annotations

from datetime import date, timedelta

import pytest
from sqlalchemy import text

from app.apps.erp.core.perguntas import consulta, mapa

pytestmark = pytest.mark.banco


# ---------------------------------------------------------------------------
@pytest.fixture
def duas_obras(sessao_real):
    a = sessao_real.execute(text(
        "INSERT INTO obras (codigo, nome, status) "
        "VALUES ('PERG-A', 'Obra da pergunta A', 'ATIVA') RETURNING id")).scalar()
    b = sessao_real.execute(text(
        "INSERT INTO obras (codigo, nome, status) "
        "VALUES ('PERG-B', 'Obra da pergunta B', 'ATIVA') RETURNING id")).scalar()
    sessao_real.flush()
    return a, b


@pytest.fixture
def equipe(sessao_real, duas_obras):
    a, b = duas_obras
    for nome, cpf, obra in (("Gente da A", "39053344705", a),
                            ("Outra da A", "88130257380", a),
                            ("Gente da B", "35524073009", b)):
        sessao_real.execute(text(
            "INSERT INTO colaboradores (nome, cpf, obra_id, situacao, vinculo, "
            "admissao) VALUES (:n, :c, :o, 'ATIVO', 'CTPS', :d)"),
            {"n": nome, "c": cpf, "o": obra,
             "d": date.today() - timedelta(days=100)})
    sessao_real.flush()
    return duas_obras


def _usuario(sessao_real, perfil, obras=()):
    from app.apps.erp.core.auth.service import gerar_hash
    from app.apps.erp.db.models.cadastros import (
        PerfilUsuario, Usuario, UsuarioObra)
    u = Usuario(nome=f"Quem pergunta {perfil}",
                email=f"{perfil.lower()}@pergunta.teste", ativo=True,
                senha_hash=gerar_hash("senha-de-teste-123"),
                perfil=PerfilUsuario[perfil], escopo_visao="OBRAS_DESIGNADAS")
    sessao_real.add(u)
    sessao_real.flush()
    for o in obras:
        sessao_real.add(UsuarioObra(usuario_id=u.id, obra_id=o))
    sessao_real.flush()
    return u


# ---------------------------------------------------------------------------
# O MAPA NÃO PODE ENVELHECER — foi assim que o PERGUNTAS.md morreu
# ---------------------------------------------------------------------------
def test_toda_visao_entrega_exatamente_as_colunas_que_promete(sessao_real, equipe):
    """A trava contra o apodrecimento.

    O mapa diz à IA quais colunas existem. Se alguém renomear um campo do
    banco, a visão passa a entregar outra coisa e a IA monta consulta com
    coluna que não existe — **e o dono recebe "não consegui" sem nunca saber
    por quê**. Aqui isso quebra a suíte, que é onde tem de quebrar.
    """
    u = _usuario(sessao_real, "ADMIN")
    for v in mapa.visoes_para(sessao_real, u):
        saida = consulta.rodar(sessao_real, f"SELECT * FROM {v['nome']}", [v])
        # roda mesmo sem linha: o que importa são as colunas declaradas
        entregues = set(saida["linhas"][0].keys()) if saida["linhas"] else None
        if entregues is None:
            resultado = sessao_real.execute(text(
                f"WITH {v['nome']} AS ({v['sql']}) SELECT * FROM {v['nome']} LIMIT 0"),
                v["parametros"])
            entregues = set(resultado.keys())
        prometidas = set(v["colunas"])
        assert entregues == prometidas, (
            f"a visão {v['nome']} promete {sorted(prometidas)} e entrega "
            f"{sorted(entregues)}")


def test_o_mapa_descreve_toda_coluna_que_mostra(sessao_real):
    """Coluna sem explicação é coluna que a IA vai usar errado."""
    u = _usuario(sessao_real, "ADMIN")
    for v in mapa.visoes_para(sessao_real, u):
        for coluna, texto in v["colunas"].items():
            assert texto and len(texto) > 8, (
                f"{v['nome']}.{coluna} está no mapa sem explicação de negócio")


# ---------------------------------------------------------------------------
# O RECORTE — o assistente não pode ser a porta dos fundos
# ---------------------------------------------------------------------------
def test_quem_so_alcanca_uma_obra_so_conta_a_gente_dela(sessao_real, equipe):
    a, b = equipe
    so_a = _usuario(sessao_real, "ADMINISTRATIVO_OBRA", obras=[a])
    visoes = mapa.visoes_para(sessao_real, so_a)
    r = consulta.rodar(sessao_real, "SELECT nome FROM pessoas ORDER BY nome", visoes)
    nomes = {l["nome"] for l in r["linhas"]}
    assert "Gente da A" in nomes and "Outra da A" in nomes
    assert "Gente da B" not in nomes, (
        "o assistente mostrou gente de obra que esta pessoa não alcança")


def test_quem_alcanca_tudo_ve_as_duas_obras(sessao_real, equipe):
    todos = _usuario(sessao_real, "ADMIN")
    visoes = mapa.visoes_para(sessao_real, todos)
    r = consulta.rodar(sessao_real, "SELECT nome FROM pessoas", visoes)
    nomes = {l["nome"] for l in r["linhas"]}
    assert {"Gente da A", "Gente da B"} <= nomes


def test_a_ia_nao_consegue_furar_o_recorte_filtrando_por_obra(sessao_real, equipe):
    """Mesmo que a consulta montada tente alcançar a outra obra pelo nome, as
    linhas dela nem existem na visão — o corte acontece ANTES."""
    a, b = equipe
    so_a = _usuario(sessao_real, "ADMINISTRATIVO_OBRA", obras=[a])
    visoes = mapa.visoes_para(sessao_real, so_a)
    r = consulta.rodar(sessao_real,
                       "SELECT nome FROM pessoas WHERE obra_codigo = 'PERG-B'",
                       visoes)
    assert r["linhas"] == []


# ---------------------------------------------------------------------------
# AS TRAVAS — uma a uma, porque é a parte que pode estragar o banco
# ---------------------------------------------------------------------------
VISOES_FALSAS = {"pessoas", "solicitacoes", "ferias"}


@pytest.mark.parametrize("sql, por_que", [
    ("SELECT 1 FROM pessoas; DROP TABLE colaboradores",
     "dois comandos emendados"),
    ("DELETE FROM pessoas", "não é leitura"),
    ("UPDATE pessoas SET nome = 'x'", "não é leitura"),
    ("WITH pessoas AS (SELECT 1 AS nome) SELECT * FROM pessoas",
     "redefiniria uma visão nossa"),
    ("SELECT * FROM usuarios", "tabela fora do mapa"),
    ("SELECT * FROM colaboradores", "tabela de verdade, não a visão"),
    ("SELECT * FROM ponto.marcacoes", "outro schema"),
    ("SELECT * FROM information_schema.tables", "metadado do banco"),
    ("SELECT pg_sleep(30) FROM pessoas", "derrubaria o serviço"),
    ("SELECT nome INTO outra FROM pessoas", "escreve"),
])
def test_a_consulta_perigosa_e_recusada(sql, por_que):
    with pytest.raises(consulta.ConsultaRecusada):
        consulta.conferir(sql, VISOES_FALSAS)


def test_nome_de_obra_em_portugues_nao_e_confundido_com_comando():
    """Defeito que eu mesmo escrevi e peguei relendo: a lista de palavras
    proibidas tem "do", "set", "call". Sem tirar o que está entre aspas, uma
    obra chamada "Residencial do Vale" seria recusada como se fosse comando."""
    ok = ("SELECT nome FROM pessoas WHERE obra ILIKE '%Residencial do Vale%' "
          "AND cargo = 'Encarregado'")
    assert consulta.conferir(ok, VISOES_FALSAS)


def test_a_consulta_boa_passa_e_perde_o_enfeite_da_ia():
    """A IA devolve ```sql ...``` mesmo mandada não devolver."""
    limpo = consulta.conferir("```sql\nSELECT count(*) FROM pessoas;\n```",
                              VISOES_FALSAS)
    assert limpo == "SELECT count(*) FROM pessoas"


def test_o_teto_de_linhas_corta_e_avisa(sessao_real, equipe):
    u = _usuario(sessao_real, "ADMIN")
    visoes = mapa.visoes_para(sessao_real, u)
    r = consulta.rodar(
        sessao_real,
        f"SELECT nome FROM pessoas, generate_series(1, {consulta.TETO_DE_LINHAS}) ",
        visoes)
    assert r["cortou"] is True
    assert len(r["linhas"]) == consulta.TETO_DE_LINHAS


def test_consulta_que_cita_campo_inexistente_nao_derruba_a_tela(sessao_real, equipe):
    """O savepoint. Sem ele, o erro envenenaria a transação da requisição e a
    tela cairia com um erro sem relação com a pergunta."""
    u = _usuario(sessao_real, "ADMIN")
    visoes = mapa.visoes_para(sessao_real, u)
    with pytest.raises(consulta.ConsultaRecusada):
        consulta.rodar(sessao_real, "SELECT campo_que_nao_existe FROM pessoas",
                       visoes)
    # a sessão continua utilizável — é isto que o savepoint garante
    r = consulta.rodar(sessao_real, "SELECT count(*) AS n FROM pessoas", visoes)
    assert r["linhas"][0]["n"] >= 1


# ---------------------------------------------------------------------------
# A PERGUNTA QUE ORIGINOU TUDO
# ---------------------------------------------------------------------------
def test_a_pergunta_que_o_dono_fez_passa_a_ter_resposta(sessao_real, equipe):
    """*"Quantos funcionários temos hoje na obra de CRECHESUAPE?"* — em
    27/11/2026 ela foi respondida com "o que tem a pagar num período?".

    Aqui se testa o que o sistema faz com a consulta que a IA montaria para
    ela: a resposta existe, é um número, e é só da obra perguntada.
    """
    a, b = equipe
    u = _usuario(sessao_real, "ADMIN")
    visoes = mapa.visoes_para(sessao_real, u)
    r = consulta.rodar(sessao_real, (
        "SELECT count(*) AS quantos FROM pessoas "
        "WHERE situacao <> 'DESLIGADO' AND demissao IS NULL "
        "AND (obra_codigo ILIKE '%PERG-A%' OR obra ILIKE '%pergunta A%')"
    ), visoes)
    assert r["linhas"] == [{"quantos": 2}]


# ---------------------------------------------------------------------------
# O CAMINHO INTEIRO, COM A IA DUBLADA (27/11/2026)
#
# ⚠️ O QUE ESTES TESTES ALCANÇAM E O QUE NÃO: eles percorrem tudo — mapa,
# travas, recorte, execução, o recado de cada "não sei" — trocando SÓ a ida à
# OpenAI por um dublê que devolve o JSON que o modelo devolveria. **O modelo
# em si continua sem teste**: ninguém aqui prova que ele escolhe a visão certa.
# Isso só se mede com chave e internet, pela varredura dos papéis
# (`scripts/varrer_perguntas.py`), e está escrito como pendência no histórico.
#
# O que eles PROVAM, e não é pouco: que uma consulta boa chega até a resposta;
# que uma consulta proibida vira recado em português em vez de estourar a
# tela; que "o mapa não alcança isso" é tratado como resposta, não como falha;
# e que resposta fora do formato não derruba nada.
# ---------------------------------------------------------------------------
from app.apps.erp.core.perguntas import na_hora  # noqa: E402
from app.apps.erp.core.comum.auditoria import ErroValidacao  # noqa: E402


def _dubl(**campos):
    """Um montador que devolve sempre a mesma coisa, como a IA devolveria."""
    def montador(pergunta, visoes):
        return campos
    return montador


# As consultas abaixo são as que um modelo competente escreveria lendo o mapa.
# Elas não vieram de um modelo de verdade — vieram da leitura do mapa, que é
# exatamente o que ele vai ler. Servem para provar que o MAPA é consultável.
PERGUNTAS_PLAUSIVEIS = [
    ("quantos funcionários temos hoje na obra PERG-A",
     "SELECT count(*) AS quantos FROM pessoas WHERE situacao <> 'DESLIGADO' "
     "AND demissao IS NULL AND (obra_codigo ILIKE '%PERG-A%' OR obra ILIKE '%PERG-A%')"),
    ("quem está na obra A",
     "SELECT nome, cargo FROM pessoas WHERE obra_codigo ILIKE '%PERG-A%' ORDER BY nome"),
    ("quais pedidos ao DP estão parados",
     "SELECT numero, tipo, pessoa, dias_parado FROM solicitacoes "
     "WHERE situacao IN ('Aberta', 'Em análise', 'Em andamento') ORDER BY dias_parado DESC"),
    ("quem sai de férias",
     "SELECT pessoa, inicio, fim, dias FROM ferias WHERE situacao <> 'CANCELADA' "
     "ORDER BY inicio"),
    ("quantos não têm acesso ao sistema",
     "SELECT count(*) AS quantos FROM pessoas WHERE tem_acesso_ao_sistema = false"),
    ("quem foi admitido este ano, por obra",
     "SELECT obra, count(*) AS quantos FROM pessoas "
     "WHERE admissao >= date_trunc('year', CURRENT_DATE) GROUP BY obra ORDER BY quantos DESC"),
]


@pytest.mark.parametrize("pergunta, sql", PERGUNTAS_PLAUSIVEIS)
def test_a_consulta_que_o_modelo_escreveria_chega_ate_a_resposta(
        sessao_real, equipe, pergunta, sql):
    u = _usuario(sessao_real, "ADMIN")
    r = na_hora.responder(sessao_real, pergunta, u, montador=_dubl(
        da_para=True, sql=sql, entendi="entendi assim",
        onde_olhei="nas pessoas", filtrei="pela obra"))
    assert r["respondeu"] is True, r
    assert r["montada_na_hora"] is True
    assert isinstance(r["linhas"], list)
    # O caminho vem SEMPRE — é o que o dono usa para conferir se eu entendi.
    assert r["caminho"]["entendi"] and r["caminho"]["onde_olhei"]


def test_quando_o_mapa_nao_alcanca_a_resposta_e_honesta(sessao_real, equipe):
    """"Qual o salário de fulano" — o mapa não tem salário, de propósito."""
    u = _usuario(sessao_real, "ADMIN")
    r = na_hora.responder(sessao_real, "qual o salário do José", u,
                          montador=_dubl(da_para=False,
                                         por_que_nao="o mapa não traz salário"))
    assert r["respondeu"] is False
    assert r["tipo"] == "fora_do_mapa"
    assert "salário" in r["recado"]


def test_consulta_proibida_do_modelo_vira_recado_e_nao_explosao(sessao_real, equipe):
    """Se o modelo tentar ler a tabela de usuários, a tela recebe um recado em
    português — não um erro de banco e não a tabela."""
    u = _usuario(sessao_real, "ADMIN")
    r = na_hora.responder(sessao_real, "me dá os logins", u, montador=_dubl(
        da_para=True, sql="SELECT * FROM usuarios", entendi="os acessos"))
    assert r["respondeu"] is False
    assert r["tipo"] == "consulta_recusada"
    assert "usuarios" in r["recado"]


def test_resposta_fora_do_formato_nao_derruba_a_tela(sessao_real, equipe):
    u = _usuario(sessao_real, "ADMIN")

    def montador_maluco(pergunta, visoes):
        return na_hora._json_da_resposta("isto não é JSON nenhum")

    with pytest.raises(ErroValidacao):
        na_hora.responder(sessao_real, "qualquer coisa", u,
                          montador=montador_maluco)


def test_pergunta_vazia_e_recusada_antes_de_gastar_ia(sessao_real):
    u = _usuario(sessao_real, "ADMIN")

    def nunca(pergunta, visoes):                            # pragma: no cover
        raise AssertionError("não devia ter chamado a IA")

    with pytest.raises(ErroValidacao):
        na_hora.responder(sessao_real, "   ", u, montador=nunca)


def test_o_recorte_vale_tambem_no_caminho_completo(sessao_real, equipe):
    """A mesma prova do recorte, mas percorrendo `responder` — é o caminho que
    a tela usa de verdade."""
    a, b = equipe
    so_a = _usuario(sessao_real, "ADMINISTRATIVO_OBRA", obras=[a])
    r = na_hora.responder(sessao_real, "quem trabalha aqui", so_a, montador=_dubl(
        da_para=True, sql="SELECT nome FROM pessoas", entendi="a equipe",
        onde_olhei="nas pessoas"))
    nomes = {l["nome"] for l in r["linhas"]}
    assert "Gente da B" not in nomes
