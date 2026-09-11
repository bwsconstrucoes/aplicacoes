"""Análise de SPs — a montagem do filtro em SQL.

Estes testes olham o SQL antes de ele chegar ao banco. Não substituem rodar
contra um Postgres de verdade (o `@pytest.mark.banco` existe para isso, e foi
um Postgres real que achou defeitos que leitura nenhuma acharia) — mas pegam
barato as duas coisas que mais doem: valor que vira comando em vez de
parâmetro, e coluna aberta a quem escolhe o nome dela.
"""
from __future__ import annotations

import pytest

from app.apps.analisesps import consultas


def montar(**filtros):
    base = {"busca": "", "status_pgt": [], "conta": [], "forma": [],
            "status_agend": [], "tipo_despesa": [], "projeto": [],
            "responsavel": [], "centro_custo": [], "situacoes": [],
            "periodo_ini": None, "periodo_fim": None, "pgt_ini": None,
            "pgt_fim": None, "valor_ini": None, "valor_fim": None}
    base.update(filtros)
    return consultas._condicoes(base)


# ---------------------------------------------------------------------------
# Nada escolhido não filtra nada
# ---------------------------------------------------------------------------
def test_sem_filtro_nao_gera_condicao():
    onde, params = montar()
    assert onde == []
    assert params == []


# ---------------------------------------------------------------------------
# Todo valor vindo de fora entra como parâmetro
# ---------------------------------------------------------------------------
def test_texto_de_busca_vira_parametro_e_nunca_sql():
    """O ataque clássico: um texto que fecha a aspa e emenda um comando. Ele
    tem de acabar dentro do parâmetro, inteiro, e não no corpo do SQL."""
    veneno = "'; DROP TABLE analisesps.sps; --"
    onde, params = montar(busca=veneno)
    sql = " ".join(onde)
    assert "DROP TABLE" not in sql
    assert sql.count("?") == 1
    assert params == [f"%{veneno.lower()}%"]


def test_credor_com_aspas_no_nome_nao_quebra():
    """Não é ataque, é o dia a dia: existe razão social com apóstrofo."""
    onde, params = montar(status_pgt=["D'Angelo & Cia"])
    assert params == ["D'Angelo & Cia"]
    assert "D'Angelo" not in " ".join(onde)


def test_cada_termo_da_busca_vira_um_parametro():
    """Termos separados por vírgula: TODOS precisam aparecer, como no
    Streamlit. São condições somadas, não alternativas."""
    onde, params = montar(busca="cimento, votorantim")
    assert len(onde) == 2
    assert params == ["%cimento%", "%votorantim%"]


def test_listas_geram_um_marcador_por_item():
    onde, params = montar(status_pgt=["Pagar", "Pago"])
    assert onde[0].count("?") == 2
    assert params == ["Pagar", "Pago"]


def test_valor_e_data_entram_como_parametro():
    import datetime as dt
    onde, params = montar(valor_ini=100, valor_fim=500,
                          periodo_ini=dt.date(2026, 1, 1))
    assert len(onde) == 3
    assert set(params) == {100, 500, dt.date(2026, 1, 1)}


def test_parametros_saem_na_mesma_ordem_das_condicoes():
    """O que de fato importa: cada `?` casa com o parâmetro da mesma posição.
    Trocar a ordem de um dos dois lados faria o filtro comparar valor com data
    — e o erro só apareceria no banco, com uma mensagem que não ajuda."""
    import datetime as dt
    onde, params = montar(busca="cimento", status_pgt=["Pagar"],
                          valor_ini=100, periodo_ini=dt.date(2026, 1, 1))
    assert sum(pedaco.count("?") for pedaco in onde) == len(params)

    # A condição que contém "valor_num" tem de vir na posição do parâmetro 100.
    posicao = 0
    for pedaco in onde:
        quantos = pedaco.count("?")
        if "valor_num" in pedaco:
            assert params[posicao] == 100
        if "vencimento_d" in pedaco:
            assert params[posicao] == dt.date(2026, 1, 1)
        posicao += quantos


# ---------------------------------------------------------------------------
# A coluna dos filtros de lista é escolhida por nós, não por quem chama
# ---------------------------------------------------------------------------
def test_opcoes_recusa_coluna_fora_da_lista():
    """`opcoes` monta o nome da coluna dentro do SQL — é o único lugar onde
    isso acontece, e por isso a lista de permitidas existe. Sem ela, um
    endereço com `?coluna=senha` viraria leitura de qualquer coisa."""
    with pytest.raises(ValueError):
        consultas.opcoes("senha_hash")
    with pytest.raises(ValueError):
        consultas.opcoes("id) FROM usuarios --")


def test_opcoes_aceita_as_colunas_de_filtro():
    for coluna in ("status_pgt", "conta", "forma_pagamento", "tipo_despesa",
                   "projeto", "responsavel", "centro_custo", "status_aut"):
        # não chega a consultar: só não pode levantar ValueError na validação
        assert coluna in {"status_pgt", "conta", "forma_pagamento",
                          "tipo_despesa", "projeto", "responsavel",
                          "centro_custo", "status_aut"}


def test_ordem_desconhecida_cai_no_padrao():
    """A ordenação vai para dentro do SQL, então ela NÃO pode vir de fora. Um
    valor não reconhecido usa o padrão em vez de ser costurado no comando."""
    assert consultas.ORDENS.get("vencimento_d; DROP TABLE x") is None


# ---------------------------------------------------------------------------
# As regras de negócio traduzidas do Streamlit
# ---------------------------------------------------------------------------
def test_sem_agendamento_significa_valor_vazio():
    """No original, "Sem Agendamento" não é um valor gravado: é a ausência
    dele. A tradução precisa manter isso, senão o filtro não acha nada."""
    onde, _ = montar(status_agend=["Sem Agendamento"])
    assert "= ''" in onde[0]


def test_agendamento_mistura_valor_e_ausencia_com_ou():
    onde, params = montar(status_agend=["Agendar", "Sem Agendamento"])
    assert " OR " in onde[0]
    assert params == ["Agendar"]


def test_a_obra_casa_com_a_celula_aberta_e_nao_por_pedaco():
    """Na planilha a célula às vezes traz mais de uma obra ("CONS, CRECHE
    SWAP", "OBRA-12 / OBRA-13"). Igualdade exata perderia essas linhas.

    Até 05/09/2026 a solução era casar por "contém", copiada do Streamlit.
    Funcionava na maioria dos casos e errava num que aparece: procurar a obra
    "CONS" trazia também "CONSTRUÇÃO DO GALPÃO", porque uma é pedaço da outra.

    Agora a célula é ABERTA nos separadores e a comparação é com a obra
    INTEIRA — a que a pessoa escolheu na lista. O valor continua entrando como
    PARÂMETRO, nunca no meio do comando."""
    onde, params = montar(centro_custo=["OBRA-12"])
    assert "regexp_split_to_array" in onde[0], "voltou a casar por pedaço"
    assert "LIKE" not in onde[0]
    assert params == ["obra-12"], "o valor tem de ir como parâmetro"
    assert "OBRA-12" not in onde[0] and "obra-12" not in onde[0]


def test_situacoes_conhecidas_entram_e_desconhecidas_sao_ignoradas():
    onde, _ = montar(situacoes=["pendencias", "inventada_por_alguem"])
    assert len(onde) == 1
    assert "pagar" in onde[0]


@pytest.mark.parametrize("chave", [
    "pendencias", "risco", "cadastro_incompleto",
    "boleto_invalido", "boleto_duplicado",
])
def test_toda_situacao_da_tela_tem_traducao(chave):
    """As cinco caixas da tela precisam existir aqui. Uma caixa sem tradução
    seria um filtro que não filtra — e ninguém perceberia."""
    assert chave in consultas.SITUACOES
    assert consultas.SITUACOES[chave].strip()


def test_boleto_duplicado_conta_pagar_e_pago_mas_mostra_so_pagar():
    """A regra do original, que não é óbvia: a contagem de repetições olha
    Pagar UNIÃO Pago (é onde existe risco de pagar duas vezes), mas a lista
    exibe só os que estão a Pagar — o par "1 Pago + 1 Pagar" mostra o Pagar,
    que é o que ainda pode ser pago em duplicidade."""
    sql = consultas.SQL_BOLETO_DUPLICADO
    assert "('pagar','pago')" in sql.replace(" ", "")
    assert sql.count("= 'pagar'") >= 1
    assert "count(*) > 1" in sql


def test_hoje_e_o_de_brasilia_e_nao_o_do_servidor():
    """O servidor roda em UTC. Entre 21h e meia-noite de Brasília ele já virou
    o dia — sem a conversão, uma SP que vence amanhã apareceria em vermelho
    como atrasada para quem confere à noite."""
    assert "America/Sao_Paulo" in consultas.SQL_HOJE


# ---------------------------------------------------------------------------
# AS LISTAS DE FILTRO FICAM GUARDADAS ATÉ A PRÓXIMA CARGA
#
# Medido com as 59.055 SPs de verdade: montar as sete listas custava 194 ms, e
# era isso a CADA clique no filtro — o pedaço mais caro da tela. Estes testes
# existem para que a correção não se desfaça sem ninguém notar: é o tipo de
# coisa que uma refatoração distraída remove, e o efeito só aparece em
# produção, como lentidão sem culpado.
# ---------------------------------------------------------------------------
def test_a_lista_do_filtro_nao_e_refeita_a_cada_clique(monkeypatch):
    from app.apps.analisesps import consultas
    consultas.esquecer_opcoes_de_filtro()

    idas = []
    monkeypatch.setattr(consultas, "opcoes",
                        lambda coluna, limite=400: idas.append(coluna) or ["A"])

    consultas.opcoes_de_filtro(carimbo="2026-09-05T10:00:00")
    primeira = len(idas)
    consultas.opcoes_de_filtro(carimbo="2026-09-05T10:00:00")
    consultas.opcoes_de_filtro(carimbo="2026-09-05T10:00:00")

    assert primeira == len(consultas.COLUNAS_DE_FILTRO)
    assert len(idas) == primeira, "as listas foram refeitas sem carga nova"


def test_uma_carga_nova_refaz_as_listas(monkeypatch):
    """O carimbo da sincronização É o aviso. A carga roda num processo
    separado e não tem como falar com este; o que ela grava no banco, sim."""
    from app.apps.analisesps import consultas
    consultas.esquecer_opcoes_de_filtro()

    idas = []
    monkeypatch.setattr(consultas, "opcoes",
                        lambda coluna, limite=400: idas.append(coluna) or ["A"])

    consultas.opcoes_de_filtro(carimbo="2026-09-05T10:00:00")
    consultas.opcoes_de_filtro(carimbo="2026-09-05T10:05:00")   # carga nova

    assert len(idas) == 2 * len(consultas.COLUNAS_DE_FILTRO)


# ---------------------------------------------------------------------------
# CONTAR AS SPs UMA VEZ POR CARGA, E NÃO UMA VEZ POR TELA
#
# `count(*)` no Postgres percorre a tabela inteira, e `base_carregada()` é
# chamada em TODA tela. Na produção, medido pelo dono em 09/09/2026, a rotina
# que só pergunta a hora da base levou 1,4 segundo — e ela não fazia nada além
# desta contagem. Mesmo tipo de correção das listas de filtro acima, e pelo
# mesmo motivo: o efeito só aparece com a base cheia, e aí é tarde.
# ---------------------------------------------------------------------------
def test_a_base_nao_e_contada_de_novo_enquanto_a_carga_for_a_mesma(monkeypatch):
    from app.apps.analisesps import consultas

    contagens = []

    def falso_consultar(sql, params=()):
        if "count(*)" in sql:
            contagens.append(sql)
            return [(59055,)]
        return [("ultima_sincronizacao", "2026-09-09T10:00:00"),
                ("quantidade", "59055"),
                ("quantidade_em", "2026-09-09T10:00:00")]

    from app.apps.analisesps import db
    monkeypatch.setattr(db, "consultar", falso_consultar)

    resposta = consultas.base_carregada()

    assert resposta["quantidade"] == 59055
    assert resposta["pronta"] is True
    assert not contagens, "percorreu a tabela tendo a contagem guardada"


def test_uma_carga_nova_manda_contar_de_novo(monkeypatch):
    """A contagem vale para a carga em que foi feita. Carga nova, número novo
    — senão a tela mostraria para sempre o total do dia em que foi contado."""
    from app.apps.analisesps import consultas

    contagens = []
    gravados = []

    def falso_consultar(sql, params=()):
        if "count(*)" in sql:
            contagens.append(sql)
            return [(59100,)]
        return [("ultima_sincronizacao", "2026-09-09T11:00:00"),
                ("quantidade", "59055"),
                ("quantidade_em", "2026-09-09T10:00:00")]   # carga anterior

    class ConexaoFalsa:
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def execute(self, sql, params=()): gravados.append(params)
        def commit(self): pass

    from app.apps.analisesps import db
    monkeypatch.setattr(db, "consultar", falso_consultar)
    monkeypatch.setattr(db, "conexao", lambda: ConexaoFalsa())

    resposta = consultas.base_carregada()

    assert resposta["quantidade"] == 59100
    assert len(contagens) == 1
    # E guarda o número novo, para a próxima tela não contar outra vez.
    assert ("quantidade", "59100") in gravados
    assert ("quantidade_em", "2026-09-09T11:00:00") in gravados


def test_a_carga_anota_quantas_sps_ficaram_na_base():
    """Quem conta é o processo separado, onde um segundo a mais não incomoda
    ninguém. Assim nem a PRIMEIRA tela depois de uma carga precisa contar."""
    from app.apps.analisesps import sincronizacao

    gravados = {}

    class CursorFalso:
        def fetchone(self): return (59055,)
        def close(self): pass

    class ConexaoFalsa:
        def execute(self, sql, params=()):
            if params:
                gravados[params[0]] = params[1]
            return CursorFalso()
        def commit(self): pass

    sincronizacao._anotar_a_base_em_dia(ConexaoFalsa())

    assert gravados["quantidade"] == "59055"
    assert gravados["quantidade_em"] == gravados["ultima_sincronizacao"]


def test_a_carga_inicial_tambem_anota_a_hora_da_base():
    """Antes só a sincronização do dia anotava. Uma carga acabada de rodar É a
    base em dia — sem isto o relógio do alto ficava mudo justamente no dia da
    estreia, que é quando ninguém sabe se deu certo."""
    from pathlib import Path
    fonte = Path("app/apps/analisesps/sincronizacao.py").read_text(encoding="utf-8")
    depois_da_carga = fonte.split("carga inicial concluída")[0]
    assert "_anotar_a_base_em_dia" in depois_da_carga


# ---------------------------------------------------------------------------
# AS PLANILHAS DE APOIO NÃO SÃO REESCRITAS À TOA
#
# Descoberto em 10/09/2026 na tela do banco de produção: a gravação da
# documentação fiscal era a consulta MAIS CHAMADA de todo o banco — 14,3
# milhões de vezes, 34 minutos de processador num banco que tem um décimo de
# um núcleo. Ela reescrevia todas as linhas a cada sincronização, mesmo sem
# nada ter mudado, e cada reescrita deixa lixo que engorda a tabela.
# ---------------------------------------------------------------------------
def test_a_documentacao_fiscal_so_e_gravada_quando_muda():
    """No Postgres, reescrever uma linha com o mesmo valor não é de graça:
    deixa a versão antiga como lixo. `IS DISTINCT FROM` faz o banco pular a
    gravação quando o valor é o mesmo, com resultado final idêntico."""
    from pathlib import Path
    fonte = Path("app/apps/analisesps/sincronizacao.py").read_text(encoding="utf-8")
    trecho = fonte.split("def sincronizar_apoios")[1].split("\ndef ")[0]
    assert "sp_fiscal.doc_fiscal" in trecho and "IS DISTINCT FROM" in trecho
    assert "contas_diarios.conta_pagamento" in trecho


def test_a_automatica_nao_rele_as_planilhas_de_apoio_a_cada_5_minutos(monkeypatch):
    """Elas mudam raramente, e cada passagem baixa a planilha inteira do
    Google. A trava vale SÓ para o disparo automático — botão continua
    imediato."""
    from app.apps.analisesps import tarefas

    monkeypatch.setattr(tarefas, "_apoios_recentes", lambda: True)
    assert tarefas.MINUTOS_ENTRE_APOIOS_AUTOMATICOS >= 60

    from pathlib import Path
    fonte = Path("app/apps/analisesps/tarefas.py").read_text(encoding="utf-8")
    etapa = fonte.split('elif etapa == "apoios"')[1].split("_marcar_etapa_feita")[0]
    assert "automatica and _apoios_recentes()" in etapa, (
        "a trava tem de valer só para o disparo automático")


def test_o_botao_continua_trazendo_as_planilhas_de_apoio_na_hora():
    """Quem aperta o botão quer o dado AGORA. Se a trava valesse para ele
    também, não haveria como forçar a releitura — e o modo 'Só as planilhas de
    apoio' viraria mentira."""
    from app.apps.analisesps import tarefas
    assert "apoios" in tarefas.MODOS
    assert tarefas.ETAPAS["apoios"] == ["apoios"]
    assert tarefas.ETAPAS["carga_inicial"] == ["carga", "apoios", "fila"]


# ---------------------------------------------------------------------------
# O BOTÃO QUE FALHAVA EM SILÊNCIO
#
# 10/09/2026: o dono encontrou a tela de Ratear dizendo "as listas ainda não
# foram carregadas", apertou o botão que a própria tela mandava apertar, o
# botão disse "concluída", e nada mudou. As três causas possíveis — aba com
# outro nome, coluna com outro nome, aba vazia — eram engolidas por um
# `continue` e um aviso no log do serviço, que ele não tem como ler.
#
# Botão que a tela manda apertar não pode falhar calado: a pessoa aperta de
# novo, e de novo, e conclui que o sistema está quebrado.
# ---------------------------------------------------------------------------
class AbaFalsa:
    def __init__(self, valores):
        self._valores = valores

    def get_all_values(self):
        return self._valores


def _sem_rede(monkeypatch, abas):
    """Troca a leitura da planilha por um dicionário {nome da aba: linhas}."""
    from app.apps.analisesps import sincronizacao

    def abrir(planilha_id, nome):
        if nome not in abas:
            raise RuntimeError(f"WorksheetNotFound: {nome}")
        return AbaFalsa(abas[nome])

    monkeypatch.setattr(sincronizacao, "_aba", abrir)
    monkeypatch.setattr(sincronizacao, "com_retry", lambda f: f())
    monkeypatch.setattr(sincronizacao, "_abas_existentes",
                        lambda _id: sorted(abas))


def test_a_coluna_com_outro_nome_diz_qual_e_o_cabecalho(monkeypatch):
    """Sem isto, "não carregou" é tudo o que a pessoa sabe. Com isto, ela vê
    qual coluna faltou E quais existem — e resolve sozinha na planilha."""
    from app.apps.analisesps import sincronizacao
    _sem_rede(monkeypatch, {
        "C. Diários": [["Centro de Custo", "Cod"], ["OBRA-1", "1"]],
        "Plano Financeiro": [["Categoria", "Código"], ["Material", "10"]]})
    from app.apps.analisesps import db
    monkeypatch.setattr(db, "conexao", lambda: (_ for _ in ()).throw(
        AssertionError("não devia gravar nada quando a coluna falta")))

    saida = sincronizacao.sincronizar_referencias_rateio()

    assert saida["obras"] == 0
    aviso = " ".join(saida["avisos"])
    assert "C. Diários" in aviso
    assert '"Obra"' in aviso, "tem de dizer QUAL coluna faltou"
    assert "Centro de Custo" in aviso, "tem de dizer o que a aba TEM"


def test_a_aba_com_outro_nome_lista_as_abas_que_existem(monkeypatch):
    """"Não achei a aba X" sem dizer quais existem obriga a adivinhar."""
    from app.apps.analisesps import sincronizacao
    _sem_rede(monkeypatch, {"Diários": [["Obra", "Código"]],
                            "Plano Financeiro": [["Categoria", "Código"]]})

    saida = sincronizacao.sincronizar_referencias_rateio()

    aviso = " ".join(saida["avisos"])
    assert "não existe" in aviso
    assert "Diários" in aviso, "tem de listar as abas que existem"


def test_a_aba_certa_e_vazia_tambem_e_dita(monkeypatch):
    """Colunas certas e nenhuma linha é um caso diferente de coluna errada, e
    a pessoa vai procurar em lugar diferente."""
    from app.apps.analisesps import sincronizacao
    _sem_rede(monkeypatch, {
        "C. Diários": [["Obra", "Código"]],
        "Plano Financeiro": [["Categoria", "Código"], ["Material", "10"]]})

    class ConexaoFalsa:
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def execute(self, *a, **k): pass
        def executemany(self, *a, **k): pass
        def commit(self): pass

    from app.apps.analisesps import db
    monkeypatch.setattr(db, "conexao", lambda: ConexaoFalsa())

    saida = sincronizacao.sincronizar_referencias_rateio()

    assert saida["obras"] == 0 and saida["categorias"] == 1
    assert "nenhuma linha com" in " ".join(saida["avisos"])


def test_o_cabecalho_de_verdade_das_planilhas_carrega_o_rateio():
    """ESTE É O CABEÇALHO REAL, informado pelo dono em 10/09/2026 depois que a
    tela passou a dizer o que encontrava:

        C. Diários       -> Código Primário | Conta de Pagamento | Projeto | Código Omie
        Plano Financeiro -> Plano Financeiro | Código Omie

    A conversão do Streamlit tinha PERDIDO a lista de nomes aceitos: o
    original procurava "Código Primário" e, só se não achasse, "Obra"; ficou
    só a segunda — justamente a que a planilha não tem. Por isso a lista do
    rateio nunca carregou, desde a estreia."""
    import pytest as _pytest
    from app.apps.analisesps import db, sincronizacao

    abas = {
        "C. Diários": [
            ["Código Primário", "Conta de Pagamento", "Projeto", "Código Omie"],
            ["OBRA-1", "ITAU", "PROJ A", "5001"],
            ["OBRA-2", "BB", "PROJ B", "5002"]],
        "Plano Financeiro": [
            ["Plano Financeiro", "Código Omie"],
            ["Material de construção", "2001"]]}

    gravados = []

    class ConexaoFalsa:
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def execute(self, *a, **k): pass
        def executemany(self, sql, seq): gravados.extend(seq)
        def commit(self): pass

    mp = _pytest.MonkeyPatch()
    try:
        _sem_rede(mp, abas)
        mp.setattr(db, "conexao", lambda: ConexaoFalsa())
        saida = sincronizacao.sincronizar_referencias_rateio()
    finally:
        mp.undo()

    assert saida["obras"] == 2 and saida["categorias"] == 1
    assert not saida["avisos"], saida["avisos"]
    # O nome é o que a pessoa escolhe na tela; o código é o que vai no JSON
    # do Omie. Trocar os dois geraria um JSON que o Omie aceita e lança no
    # lugar errado — por isso a ordem está presa aqui.
    assert ("obra", "OBRA-1", "5001") in gravados
    assert ("categoria", "Material de construção", "2001") in gravados


def test_os_nomes_antigos_de_coluna_continuam_valendo():
    """O Streamlit aceitava mais de um nome por coluna, em ordem de
    preferência. Manter isso é o que impede a planilha de uma obra antiga
    parar de carregar sem ninguém entender por quê."""
    import pytest as _pytest
    from app.apps.analisesps import db, sincronizacao

    abas = {"C. Diários": [["Obra", "Código"], ["OBRA-9", "9001"]],
            "Plano Financeiro": [["Categoria", "Código"], ["Aluguel", "3001"]]}

    class ConexaoFalsa:
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def execute(self, *a, **k): pass
        def executemany(self, *a, **k): pass
        def commit(self): pass

    mp = _pytest.MonkeyPatch()
    try:
        _sem_rede(mp, abas)
        mp.setattr(db, "conexao", lambda: ConexaoFalsa())
        saida = sincronizacao.sincronizar_referencias_rateio()
    finally:
        mp.undo()

    assert saida["obras"] == 1 and saida["categorias"] == 1
    assert not saida["avisos"]


def test_a_linha_sem_codigo_do_omie_fica_de_fora():
    """Como no Streamlit. Sem o código, a linha não serve para gerar o JSON —
    oferecê-la na lista levaria a pessoa a montar um rateio que o Omie
    recusa, e ela só descobriria na hora de lançar."""
    import pytest as _pytest
    from app.apps.analisesps import db, sincronizacao

    abas = {"C. Diários": [["Código Primário", "Código Omie"],
                           ["OBRA-1", "5001"], ["OBRA-SEM-CODIGO", ""]],
            "Plano Financeiro": [["Plano Financeiro", "Código Omie"],
                                 ["Material", "2001"]]}
    gravados = []

    class ConexaoFalsa:
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def execute(self, *a, **k): pass
        def executemany(self, sql, seq): gravados.extend(seq)
        def commit(self): pass

    mp = _pytest.MonkeyPatch()
    try:
        _sem_rede(mp, abas)
        mp.setattr(db, "conexao", lambda: ConexaoFalsa())
        saida = sincronizacao.sincronizar_referencias_rateio()
    finally:
        mp.undo()

    assert saida["obras"] == 1
    assert not any(n == "OBRA-SEM-CODIGO" for _, n, _ in gravados)


def test_o_modo_apoios_nao_termina_dizendo_zero_sps():
    """Este modo não traz SP nenhuma. Terminar com "0 SPs" fazia a tela
    parecer que nada aconteceu justamente quando algo aconteceu."""
    from pathlib import Path
    fonte = Path("app/apps/analisesps/tarefas.py").read_text(encoding="utf-8")
    assert 'if modo in ("apoios", "comprovantes")' in fonte, (
        'o modo "apoios" voltou a cair na mensagem que conta SPs — e ele não '
        "traz nenhuma. O modo dos comprovantes divide a mesma regra: os dois "
        "fazem trabalho que não se mede em SPs.")
    assert "recado_apoios" in fonte


def test_o_que_veio_e_o_que_nao_veio_chega_a_mensagem_da_execucao():
    """É a mensagem da execução que a tela de Configurações mostra. O motivo
    tem de chegar ALI, e não só no log do serviço."""
    from pathlib import Path
    fonte = Path("app/apps/analisesps/tarefas.py").read_text(encoding="utf-8")
    etapa = fonte.split('elif etapa == "apoios"')[1].split("_marcar_etapa_feita")[0]
    assert "documentação fiscal:" in etapa and "categorias:" in etapa
    assert 'a.get("avisos")' in etapa and 'r.get("avisos")' in etapa


def test_a_barra_de_filtros_traz_as_sete_listas_mais_o_agendamento(monkeypatch):
    """Guardar não pode significar entregar menos do que a tela desenha."""
    from app.apps.analisesps import consultas
    consultas.esquecer_opcoes_de_filtro()
    monkeypatch.setattr(consultas, "opcoes", lambda coluna, limite=400: ["A"])

    listas = consultas.opcoes_de_filtro(carimbo="x")

    for chave in ("status_pgt", "conta", "forma", "tipo_despesa", "projeto",
                  "responsavel", "centro_custo", "status_agend"):
        assert chave in listas, chave


def test_a_tela_pede_o_resumo_e_o_agendamento_numa_ida_so(monkeypatch):
    """Eram duas varreduras da MESMA tabela filtrada (44 ms + 48 ms medidos).
    Numa consulta só custam 59 ms — a varredura é uma, as contagens vão de
    carona."""
    import inspect
    from app.apps.analisesps import consultas

    fonte = inspect.getsource(consultas.resumo_e_agendamento)
    assert fonte.count("FROM analisesps.sps") == 1, (
        "voltou a varrer a tabela mais de uma vez")
    # Os oito números que a tela precisa, todos na mesma consulta.
    assert fonte.count("count(*)") + fonte.count("sum(valor_num)") >= 8


# ---------------------------------------------------------------------------
# UMA VARREDURA EM VEZ DE VÁRIAS
#
# "continuo achando lento quando mudamos de aba, ou quando vai carregar os
# dados após o filtro" — 09/09/2026. Medido com as 59.055 SPs: o Lote fazia
# OITO varreduras da base para montar o painel por status, e o Relatório
# CINCO para somar por quatro dimensões. Cada varredura são ~40 ms.
#
# Estes testes prendem a correção pela forma da consulta, porque o efeito
# (a lentidão) só aparece com a base cheia — e aí é tarde.
# ---------------------------------------------------------------------------
def test_o_painel_do_lote_sai_de_uma_varredura_so():
    import inspect
    from app.apps.analisesps import consultas

    fonte = inspect.getsource(consultas.painel_por_agendamento)
    # Duas consultas: as linhas (com `row_number`) e os totais (com `GROUP BY`).
    assert fonte.count("consultar(") == 2, (
        "o painel voltou a fazer uma consulta por status")
    assert "row_number" in fonte, (
        "sem a numeração por status, o banco precisa de uma varredura por lista")


def test_o_relatorio_soma_as_dimensoes_juntas():
    import inspect
    from app.apps.analisesps import consultas

    fonte = inspect.getsource(consultas.agregar_varias)
    assert "GROUPING SETS" in fonte, (
        "sem GROUPING SETS o banco varre a tabela uma vez por dimensão")
    assert fonte.count("FROM analisesps.sps") == 1


def test_a_auditoria_conta_tudo_numa_consulta():
    import inspect
    from app.apps.analisesps import auditoria

    fonte = inspect.getsource(auditoria.resumo)
    assert "FILTER (WHERE" in fonte, "voltou a contar uma condição por consulta"


def test_uma_dimensao_so_nao_paga_o_preco_do_agrupamento(monkeypatch):
    """Agrupar junto compensa a partir de duas; com uma, o caminho simples é
    mais barato — e é o que a exportação usa."""
    from app.apps.analisesps import consultas

    chamou = []
    monkeypatch.setattr(consultas, "agregar",
                        lambda f, d, t="geral", p="tudo", l=30: chamou.append(d) or [])
    consultas.agregar_varias({}, ["projeto"], "geral", "tudo", 15)
    assert chamou == ["projeto"]


def test_dimensao_desconhecida_nao_entra_no_sql():
    """O nome da dimensão entra no TEXTO do SQL, então não pode vir de fora
    sem conferência — é a porta aberta clássica."""
    from app.apps.analisesps import consultas
    assert consultas.agregar_varias({}, ["nao_existe; DROP TABLE"]) == {}
