# -*- coding: utf-8 -*-
"""
A carga que retoma de onde parou.

Uma carga inicial leva horas, e o serviço reinicia a cada publicação de código.
Sem marcar o que já terminou, cada interrupção custava tudo de novo — inclusive
os 120 mil títulos, quando a carga tinha morrido lá na frente, nos movimentos.
Aconteceu duas vezes no primeiro dia de uso.

O que se prova aqui:
  - cada etapa concluída é lembrada, e a tela sabe dizer quantas faltam;
  - a marca só é feita quando a etapa termina INTEIRA — marcar antes faria a
    retomada pular dado que não chegou a entrar, e o painel mostraria uma
    receita incompleta como se fosse a receita da empresa;
  - "recomeçar do zero" apaga tudo, sem meia marca;
  - as marcas convivem com o estado do incremental na mesma tabela sem se
    atrapalharem — se vazassem, o incremental rebaixaria a base inteira.
"""
from __future__ import annotations

import inspect
import os

import pytest

pytestmark = pytest.mark.banco


@pytest.fixture()
def sem_marcas():
    from tests.conftest import VARIAVEL_BANCO_TESTE, url_de_teste_segura

    bruto = os.environ.get(VARIAVEL_BANCO_TESTE, "").strip()
    if not bruto:
        pytest.skip(f"{VARIAVEL_BANCO_TESTE} não definida — testes com banco pulados")
    os.environ["DATABASE_URL"] = url_de_teste_segura(bruto)

    from app.apps.painel import db as painel_db
    from app.apps.painel import migracoes_runner

    painel_db._engine = None
    resultado = migracoes_runner.aplicar_pendentes()
    assert not resultado.get("erro"), f"migração falhou: {resultado}"

    def _limpar():
        with painel_db.conexao() as conn:
            conn.execute("TRUNCATE TABLE sync_state")
            conn.commit()

    _limpar()
    yield
    _limpar()
    painel_db._engine = None


def test_no_comeco_nenhuma_etapa_esta_pronta(sem_marcas):
    from app.apps.painel import consultas
    from app.apps.painel.db import conexao
    from app.apps.painel.sync import espelho

    with conexao() as conn:
        assert espelho.etapas_concluidas(conn) == set()
    assert all(not e["pronta"] for e in consultas.etapas_da_carga())


def test_etapa_marcada_e_lembrada_e_a_tela_sabe_quantas_faltam(sem_marcas):
    from app.apps.painel import consultas
    from app.apps.painel.db import conexao
    from app.apps.painel.sync import espelho

    with conexao() as conn:
        espelho._marcar_etapa(conn, "contapagar")
        espelho._marcar_etapa(conn, "contareceber")
        assert espelho.etapas_concluidas(conn) == {"contapagar", "contareceber"}

    etapas = consultas.etapas_da_carga()
    assert len(etapas) == 7
    assert {e["chave"] for e in etapas if e["pronta"]} == {"contapagar", "contareceber"}
    assert sum(1 for e in etapas if not e["pronta"]) == 5
    # a tela mostra nomes que uma pessoa entende, não chaves de banco
    assert "contas a pagar" in [e["rotulo"] for e in etapas]


def test_recomecar_do_zero_apaga_todas_as_marcas(sem_marcas):
    """O botão existe para quem desconfia do que já entrou. Tem de apagar
    TUDO — meia marca faria a carga pular etapa que devia refazer."""
    from app.apps.painel.db import conexao
    from app.apps.painel.sync import espelho

    with conexao() as conn:
        for chave, _rotulo in espelho.ETAPAS_DA_CARGA:
            espelho._marcar_etapa(conn, chave)
        assert len(espelho.etapas_concluidas(conn)) == 7
        espelho.limpar_etapas(conn)
        assert espelho.etapas_concluidas(conn) == set()


def test_a_marca_da_carga_nao_atrapalha_o_incremental(sem_marcas):
    """As marcas moram na MESMA tabela onde o incremental anota onde parou. Um
    prefixo separa as duas coisas. Se vazassem uma na outra, o incremental
    perderia a referência e rebaixaria a base inteira toda madrugada."""
    from app.apps.painel.db import conexao, consultar
    from app.apps.painel.sync import espelho

    with conexao() as conn:
        conn.execute(
            "INSERT INTO sync_state (entidade, ultima_dalt) VALUES ('contapagar', ?)",
            ("01/09/2026",))
        conn.commit()
        espelho._marcar_etapa(conn, "contapagar")

        assert espelho.etapas_concluidas(conn) == {"contapagar"}
        (dalt,) = consultar(
            "SELECT ultima_dalt FROM sync_state WHERE entidade = 'contapagar'")[0]
        assert dalt == "01/09/2026"      # o incremental continua sabendo onde parou

        espelho.limpar_etapas(conn)
        assert consultar(
            "SELECT COUNT(*) FROM sync_state WHERE entidade = 'contapagar'")[0][0] == 1


def test_a_etapa_so_e_marcada_depois_de_terminar_inteira(sem_marcas):
    """Se a marca fosse feita no começo do bloco, uma carga morta no meio de
    "contas a receber" pularia essa etapa na retomada — e o painel mostraria
    receita incompleta como se fosse a receita da empresa.

    A ordem é lida do próprio código: é uma garantia estrutural, não um
    comentário que alguém pode contrariar sem perceber."""
    from app.apps.painel.sync import espelho

    codigo = inspect.getsource(espelho.carga_inicial)
    assert codigo.index('log.info("OK %s -> %s titulos') < \
        codigo.index("_marcar_etapa(conn, entidade)")


def test_todas_as_sete_etapas_sao_marcadas_em_algum_lugar(sem_marcas):
    """Etapa que ninguém marca nunca é pulada — e a retomada, que existe para
    poupar horas, não pouparia nada naquele trecho."""
    from app.apps.painel.sync import espelho

    codigo = inspect.getsource(espelho.carga_inicial)

    # As duas primeiras rodam num laço e são tratadas pela variável `entidade`;
    # as demais aparecem pelo nome. As duas formas contam.
    PELO_LACO = {"contapagar", "contareceber"}
    for chave, _rotulo in espelho.ETAPAS_DA_CARGA:
        if chave in PELO_LACO:
            assert "_marcar_etapa(conn, entidade)" in codigo
            assert "if entidade in feitas" in codigo
            continue
        assert f'_marcar_etapa(conn, "{chave}")' in codigo, \
            f"a etapa {chave} nunca é marcada como concluída"
        assert f'"{chave}" in feitas' in codigo, \
            f"a etapa {chave} nunca é pulada na retomada"


def test_carga_concluida_apaga_as_marcas(sem_marcas):
    """As marcas existem para sobreviver a uma interrupção. Se ficassem depois
    de a carga terminar inteira, uma próxima "Primeira carga" pularia as sete
    etapas e não faria nada — sem dizer por quê, que é o pior jeito de falhar."""
    from app.apps.painel.sync import espelho

    # A docstring da função também menciona `limpar_etapas`, e `index` acha a
    # primeira ocorrência — que seria a do texto, não a da chamada. Comparar com
    # a ÚLTIMA é o que responde à pergunta certa: a limpeza acontece no fim?
    codigo = inspect.getsource(espelho.carga_inicial)
    assert "limpar_etapas(conn)" in codigo
    assert codigo.index('_marcar_etapa(conn, "planilha")') < \
        codigo.rindex("limpar_etapas(conn)")


# ===========================================================================
# A retomada FINA: por página, dentro da etapa — 20/09/2026
# ===========================================================================
# O dono começou a carga inicial às 18:51 e uma publicação minha reiniciou o
# serviço. A marca só existia para etapa INTEIRA, e as contas a pagar — 118 mil
# títulos, a mais longa das sete — não tinham terminado: horas jogadas fora.
# Ele: "melhor, visto que posso iniciar e dar outro problema".

def test_a_marca_de_pagina_nao_conta_como_etapa_pronta(sem_marcas):
    """As duas marcas moram na mesma tabela e no mesmo prefixo. Se a marca de
    página vazasse para a lista de etapas prontas, a carga pularia uma etapa que
    não terminou — e o painel mostraria base incompleta como se fosse completa."""
    from app.apps.painel.db import conexao
    from app.apps.painel.sync import espelho

    with conexao() as conn:
        espelho._salvar_pagina(conn, "contapagar", 42)
        assert espelho.etapas_concluidas(conn) == set(), \
            "página salva é onde a etapa PAROU — o contrário de ter terminado"
        assert espelho._pagina_retomada(conn, "contapagar") == 42


def test_comecar_do_zero_apaga_tambem_as_marcas_de_pagina(sem_marcas):
    """Senão "recomeçar do zero" começaria no meio da página 42."""
    from app.apps.painel.db import conexao
    from app.apps.painel.sync import espelho

    with conexao() as conn:
        espelho._marcar_etapa(conn, "contapagar")
        espelho._salvar_pagina(conn, "contapagar", 42)
        espelho.limpar_etapas(conn)
        assert espelho.etapas_concluidas(conn) == set()
        assert espelho._pagina_retomada(conn, "contapagar") == 0


def test_a_marca_de_pagina_some_quando_a_etapa_termina(sem_marcas):
    """Se sobrasse, a carga seguinte começaria no meio de uma etapa completa."""
    from app.apps.painel.db import conexao
    from app.apps.painel.sync import espelho

    with conexao() as conn:
        espelho._salvar_pagina(conn, "movimentos", 300)
        espelho._esquecer_pagina(conn, "movimentos")
        assert espelho._pagina_retomada(conn, "movimentos") == 0


def test_titulo_retoma_um_passo_atras_e_movimento_retoma_exato():
    """A diferença entre as duas etapas, que é a parte fácil de errar.

    TÍTULO tem chave (o código do OMIE): regravar a mesma página atualiza, nunca
    duplica — então dá para voltar uma página e cobrir o deslocamento de
    paginação sem custo nenhum.

    MOVIMENTO não tem chave: regravar a mesma página duplicaria dinheiro. Por
    isso ele retoma exatamente na página seguinte, e a conferência do fim vale
    nos dois sentidos — faltando ou sobrando."""
    from app.apps.painel.sync import espelho

    titulos = inspect.getsource(espelho.carga_inicial)
    assert "max(1, salva - 1)" in titulos, \
        "o título perdeu o passo atrás: a retomada pode pular títulos"

    movimentos = inspect.getsource(espelho.carregar_movimentos_full)
    assert "salva + 1" in movimentos, \
        "o movimento ganhou passo atrás: isso duplica dinheiro, não atualiza"
    assert "contados > esperado" in movimentos, \
        "sem conferir a sobra, movimento duplicado passaria despercebido"


def test_a_tabela_de_movimentos_so_e_zerada_quando_se_comeca_do_inicio():
    """Zerar ao retomar apagaria tudo o que a tentativa anterior já baixou —
    ou seja, a retomada custaria exatamente o que ela existe para evitar."""
    from app.apps.painel.sync import espelho

    fonte = inspect.getsource(espelho.carregar_movimentos_full)
    corpo = fonte[fonte.index("def _baixar"):]
    antes_do_delete = corpo[:corpo.index("DELETE FROM movimentos")]
    assert "if inicial <= 1:" in antes_do_delete


def test_a_carga_confere_com_o_total_que_o_omie_informou():
    """Retomada que pula título é o risco de todo este mecanismo. A defesa não é
    confiar: é contar no fim e refazer a etapa se não fechar."""
    from app.apps.painel.sync import espelho

    fonte = inspect.getsource(espelho.carga_inicial)
    assert "contados < total_esperado" in fonte
    assert "Refazendo a etapa do zero" in fonte
    assert "queixas.append" in fonte, \
        "e se nem refazendo fechar, a tela tem de dizer — não pode ficar só no log"
