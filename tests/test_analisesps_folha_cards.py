# -*- coding: utf-8 -*-
"""
Os cards do Pipefy da folha — desde 02/10/2026, SÓ A SOLICITAÇÃO DE PAGAMENTO.

O dono: *"a gente gerava dois cards (…) eu quero matar um. Eu quero deixar só o
do financeiro (…) gerar um card com rateio múltiplo"* — no padrão do script do
BeeVale: uma SP por conta, "Rateio múltiplo entre centros de custo" = Sim, o
campo "Rateio múltiplo" com o JSON do OMIE (distribuição por obra e categoria),
Pix com chave aleatória "Atualizar Chave", e a descrição com competência, valor
por obra e links.

⚠️ O QUE ESTES TESTES PROTEGEM. Card criado no Pipefy **não se apaga** pelo
sistema. Por isso: **nada é criado** com obra sem código do OMIE, tipo de
despesa ou categoria não encontrados pelo nome, campo sumido do pipe, ou
fechamento que mudou depois de gerar; e **parou no meio, continua de onde
parou**, sem criar o mesmo card de novo. E o card de Despesa NUNCA é criado.
"""
import datetime as dt
import json
from decimal import Decimal as D

import pytest

from app.apps.analisesps import folha_cards as fcd


def test_o_grupo_da_verba_da_o_titulo_da_sp():
    assert fcd.grupo_da_verba("folha", "quinzena")[0] == "Folha de Pagamento - Quinzena"
    assert fcd.grupo_da_verba("folha", "fim_de_mes")[0] == "Folha de Pagamento - Fim de Mês"
    assert fcd.grupo_da_verba("diaria", "quinzena")[0] == "Pagamento de Diárias"
    with pytest.raises(fcd.ErroDosCards):
        fcd.grupo_da_verba("cesta", "quinzena")


def test_o_tipo_de_despesa_da_folha_e_SALARIOS_E_ORDENADOS():
    """O dono: *"o tipo de despesa seria salários e ordenados"*."""
    assert fcd.DESCRICAO_DA_VERBA["folha"] == "Salários e Ordenados"
    assert fcd.DESCRICAO_DA_VERBA["diaria"] == "Salários e Ordenados"


def test_o_RATEIO_MULTIPLO_e_o_JSON_do_OMIE_no_formato_do_BeeVale():
    texto = fcd.rateio_multiplo(
        [{"obra": "CREPEOLINDA", "omie": "111", "valor": D("1000.00")},
         {"obra": "CREPEAREIAS", "omie": "222", "valor": D("500.00")}], "2.01.01")
    dist, cat = texto.split("\n")
    assert dist.startswith('"distribuicao": [') and cat.startswith('"categorias": [')
    itens = json.loads(dist.split(": ", 1)[1])
    assert itens[0] == {"cCodDep": "111", "cDesDep": "CREPEOLINDA",
                        "nPerDep": 66.6666667, "nValDep": None}
    assert round(sum(i["nPerDep"] for i in itens), 7) == 100, "fecha 100%"
    assert json.loads(cat.split(": ", 1)[1]) == [
        {"codigo_categoria": "2.01.01", "percentual": 100, "valor": 1500.0}]


AGORA = dt.datetime(2026, 10, 1, 14, 5)


def _sp(**m):
    base = {"conta": "50024", "valor": D("1500.00"), "pessoas": 2, "destino": "beevale",
            "link": "https://drive/pag1",
            "obras": [{"obra": "CREPEOLINDA", "valor": D("1000.00"), "omie": "111"},
                      {"obra": "CREPEAREIAS", "valor": D("500.00"), "omie": "222"}],
            "rateio": "RATEIO"}
    base.update(m)
    base["descricao"] = fcd.descricao_da_sp("09/2026", "quinzena", "folha", base,
                                            "https://drive/ana")
    return base


# ---------------------------------------------------------------------------
# O TIPO DE DESPESA QUANDO O PIPEFY NÃO DIZ A QUE A CONEXÃO ESTÁ LIGADA
# 03/10/2026: a prévia de 09/2026 parou em "conexão de tipo não suportado".
# ---------------------------------------------------------------------------
def test_sem_a_tabela_da_conexao_ela_e_DESCOBERTA_pelo_registro_do_Make(monkeypatch):
    from app.apps.analisesps import pipefy
    perguntados = []
    monkeypatch.setattr(pipefy, "tabela_do_registro", lambda rid, *a, **k: (
        perguntados.append(rid) or {"id": "T9", "nome": "Tipos de Despesa"}))
    monkeypatch.setattr(pipefy, "registros_da_tabela", lambda tid, *a, **k: [
        {"id": "777", "nome": "Salários e Ordenados"}] if tid == "T9" else [])
    achar, como = fcd._achador({"tipo": "connector", "ligado_a": None},
                               ["383928967"])
    assert achar("SALARIOS E ORDENADOS") == "777"
    assert perguntados == ["383928967"]
    assert "Tipos de Despesa" in como and "Make" in como


def test_sem_tabela_e_sem_sonda_a_tela_diz_O_QUE_o_Pipefy_respondeu(monkeypatch):
    from app.apps.analisesps import pipefy

    def recusa(*a, **k):
        raise pipefy.ErroDoPipefy("O Pipefy devolveu erro: registro não existe")
    monkeypatch.setattr(pipefy, "tabela_do_registro", recusa)
    achar, como = fcd._achador({"tipo": "connector"}, ["1"],
                               ["O Pipefy devolveu erro: Field 'connectedRepo'"])
    assert achar("Salários e Ordenados") is None
    assert "não informou a que tabela" in como
    assert "connectedRepo" in como and "registro não existe" in como


def test_conexao_com_PIPE_procura_o_card_pelo_titulo(monkeypatch):
    from app.apps.analisesps import pipefy
    monkeypatch.setattr(pipefy, "cards_por_titulo", lambda pipe, titulo, *a, **k: [
        {"id": "55", "nome": "Salários e Ordenados"},
        {"id": "56", "nome": "Salários e Ordenados - antigo"}])
    achar, como = fcd._achador({"tipo": "connector", "ligado_a": {
        "tipo": "pipe", "id": "300", "nome": "Tipos"}})
    assert achar("Salários e Ordenados") == "55"
    assert 'pipe "Tipos"' in como


def test_a_SP_tem_os_campos_do_padrao_BeeVale_com_PIX_e_ATUALIZAR_CHAVE():
    grupo = {"tipo_sp": "999"}
    campos = {c["campo"]: c["valor"] for c in fcd.campos_da_sp(
        grupo, _sp(), "https://drive/ana", AGORA)}
    assert campos["tipo_de_pagamento"] == "Pix"
    assert campos["tipo"] == "Aleatória"
    assert campos["chave_pix_aleat_ria"] == "Atualizar Chave"
    assert campos["rateio_m_ltiplo_entre_centros_de_custo"] == "Sim"
    assert campos["rateio_m_ltiplo"] == "RATEIO"
    assert campos["selecione_o_procedimento"] == "Solicitar Pagamento"
    assert campos["tipo_de_despesa"] == "999"
    assert campos["valor"] == "1500.00"
    assert campos["data"] == "01/10/2026 14:05"
    assert campos["cnpj"] == "31.749.082/0001-03", "arquivo BeeVale: a BeeVale recebe"
    soma = {c["campo"]: c["valor"] for c in fcd.campos_da_sp(
        grupo, _sp(destino="somapay"), "", AGORA)}
    assert soma["cnpj"] == "00.079.526/0001-09", "SomaPay: a conta Somapay da BWS"


def test_a_DESCRICAO_diz_competencia_obras_valores_e_links():
    texto = _sp()["descricao"]
    assert "Folha" in texto and "competência 09/2026" in texto
    assert "Conta de origem: 50024" in texto and "BeeVale" in texto
    assert "- CREPEOLINDA: R$ 1.000,00" in texto and "- CREPEAREIAS: R$ 500,00" in texto
    assert "https://drive/pag1" in texto and "https://drive/ana" in texto


def test_campo_de_fase_vai_DEPOIS_da_criacao():
    na, depois = fcd._separar(
        [{"campo": "valor", "valor": "1"}, {"campo": "etiquetas", "valor": "x"},
         {"campo": "descri_o", "valor": ""}],
        {"valor": {}, "descri_o": {}})
    assert [v["campo"] for v in na] == ["valor"]
    assert [v["campo"] for v in depois] == ["etiquetas"]


def test_a_DESCRICAO_traz_o_link_do_RELATORIO_em_PDF():
    """Dono, 03/10/2026: *"colocar o link tanto do arquivo de pagamento quanto o
    relatório também"*."""
    sp = dict(_sp(), link_relatorio="https://drive/rel1")
    texto = fcd.descricao_da_sp("09/2026", "quinzena", "folha", sp, "https://drive/ana")
    assert "Relatório (PDF): https://drive/rel1" in texto
    assert "https://drive/pag1" in texto


def test_LANCAMENTO_VIA_API_vai_na_criacao_mesmo_sendo_de_fase():
    """03/10/2026: o Pipefy recusou por "Anexos" obrigatório. "Lançamento via
    API" = Sim dispensa o anexo, mas só se for na criação — como o Make fazia."""
    na, depois = fcd._separar(
        [{"campo": "valor", "valor": "1"}, {"campo": "lan_amento_via_api", "valor": "Sim"},
         {"campo": "etiquetas", "valor": "x"}],
        {"valor": {}})
    assert [v["campo"] for v in na] == ["valor", "lan_amento_via_api"]
    assert [v["campo"] for v in depois] == ["etiquetas"]


def test_o_NOME_DO_CREDOR_vai_preenchido_tambem_na_SomaPay():
    """03/10/2026: *"O campo Nome do Credor é obrigatório"* — o "local" ia vazio
    quando o arquivo era SomaPay. O credor é a BWS."""
    campos = {c["campo"]: c["valor"] for c in fcd.campos_da_sp(
        {"tipo_sp": "1"}, _sp(destino="somapay"), "", AGORA)}
    assert campos["local"] == "BWS CONSTRUCOES LTDA"
    assert campos["cnpj"] == campos["cnpj_1"] == "00.079.526/0001-09"
    assert campos["radio_horizontal_t_tulo"] == "Pessoa Jurídica"
    assert campos["lan_amento_via_api"] == "Sim"


# ---------------------------------------------------------------------------
# PONTA A PONTA, COM BANCO: gerar → prévia → lançar
# ---------------------------------------------------------------------------
GERLANIO = "99713349334"
ANA = "03513441363"


def _campos_de(ids, obrigatorios=()):
    return {i: {"label": i, "tipo": "short_text", "opcoes": [],
                "obrigatorio": i in obrigatorios, "ligado_a": None} for i in ids}


class PipefyFalso:
    """O pipe de SP com os campos usados, e o registro do que foi criado."""

    def __init__(self, monkeypatch, tipos=None, falhar_na=None, categorias=None,
                 plano=None):
        from app.apps.analisesps import conciliacao_omie, pipefy, sincronizacao
        self.criados, self.atualizados = [], []
        self.falhar_na = falhar_na
        inicio = _campos_de([c for c in fcd.CAMPOS_DA_SP
                             if c not in ("valida_o_sp_1", "etiquetas")])
        inicio["tipo_de_despesa"].update(
            {"tipo": "connector", "ligado_a": {"tipo": "tabela", "id": "T9",
                                               "nome": "Tipos de Despesa"}})
        fases = _campos_de(["valida_o_sp_1", "etiquetas"])
        self.pipes = {fcd.PIPE_SP: {"id": fcd.PIPE_SP, "nome": "SP", "campos": inicio,
                                    "campos_das_fases": fases}}
        self.tipos = tipos if tipos is not None else [
            {"id": "777", "nome": "Salários e Ordenados"},
            {"id": "778", "nome": "Despesas com Alimentação"}]
        self.categorias = categorias if categorias is not None else [
            {"codigo": "2.01.01", "descricao": "SALARIOS E ORDENADOS",
             "inativa": False, "transferencia": False}]
        monkeypatch.setattr(pipefy, "campos_do_pipe",
                            lambda pipe, *a, **k: self.pipes[str(pipe)])
        monkeypatch.setattr(pipefy, "registros_da_tabela",
                            lambda *a, **k: list(self.tipos))
        monkeypatch.setattr(pipefy, "criar_card", self.criar)
        monkeypatch.setattr(pipefy, "atualizar_campos", self.atualizar)
        monkeypatch.setattr(conciliacao_omie, "categorias_do_omie",
                            lambda busca="", limite=400: list(self.categorias))
        # A aba "Plano Financeiro" da planilha das SPs. Vazia por padrão: os
        # testes antigos exercitam a reserva (Pipefy e espelho do painel).
        self.plano = plano or []
        monkeypatch.setattr(sincronizacao, "ler_plano_financeiro",
                            lambda: list(self.plano))

    def criar(self, pipe, titulo, valores, **k):
        from app.apps.analisesps import pipefy
        if self.falhar_na is not None and len(self.criados) == self.falhar_na:
            self.falhar_na = None
            raise pipefy.ErroDoPipefy("O Pipefy devolveu erro: caiu")
        novo = str(9000 + len(self.criados))
        self.criados.append({"pipe": str(pipe), "titulo": titulo, "id": novo,
                             "valores": {v["campo"]: v["valor"] for v in valores}})
        return {"id": novo, "titulo": titulo,
                "link": f"https://app.pipefy.com/open-cards/{novo}"}

    def atualizar(self, card, valores, **k):
        self.atualizados.append((str(card), {v["campo"]: v["valor"] for v in valores}))
        return len(valores)

    def do_pipe(self, pipe):
        return [c for c in self.criados if c["pipe"] == pipe]

    def atualizacoes_de(self, card):
        saida = {}
        for c, v in self.atualizados:
            if c == card:
                saida.update(v)
        return saida


@pytest.fixture
def banco_cards(banco_analisesps):
    from app.apps.analisesps.db import conexao
    with conexao() as conn:
        for nome, codigo in [("CREPEOLINDA", "111"), ("CREPEAREIAS", "222"),
                             ("SEMOMIE", "")]:
            conn.execute(
                "INSERT INTO analisesps.referencias_rateio (tipo, nome, codigo) "
                " VALUES ('obra', ?, ?)", (nome, codigo))
        for codigo, conta in [("CREPEOLINDA", "50024"), ("CREPEAREIAS", "50025"),
                              ("SEMOMIE", "50024")]:
            conn.execute(
                "INSERT INTO analisesps.contas_diarios (codigo, conta_pagamento) "
                " VALUES (?, ?)", (codigo, conta))
        conn.commit()
    return banco_analisesps


def _fechar(partes=(("CREPEOLINDA", "1000.00"), ("CREPEAREIAS", "500.00")),
            verba="folha"):
    from app.apps.analisesps import folha_apropriacao_guardada as ag
    pessoas = [{"cpf": cpf, "nome": cpf, "nome_cadastro": f"PESSOA {cpf}",
                "fora": False,
                "por_obra": [{"obra": obra, "dias": 11, "valor": D(valor),
                              "origem": "ponto"}]}
               for cpf, (obra, valor) in zip((GERLANIO, ANA, "11144477735"), partes)]
    total = sum((D(v) for _, v in partes), D("0"))
    ag.fechar(2026, 9, "quinzena", {"pessoas": pessoas, "total_da_folha": total,
                                    "total_apropriado": total, "fecha": True},
              verba=verba, quem="MARCELO")


def _gerar(monkeypatch):
    from app.apps.analisesps import (beevale, drive, folha_geracao as g,
                                     folha_pagamento as fp)
    subidos = []
    monkeypatch.setattr(beevale, "pasta_do_drive", lambda: ("PASTA", "teste"))
    monkeypatch.setattr(drive, "subir_arquivo", lambda conteudo, nome, pasta,
                        **k: subidos.append(nome) or
                        {"id": f"id{len(subidos)}",
                         "link": f"https://drive/{len(subidos)}"})
    fp.gerar(2026, 9, "quinzena", ["folha"], g.SOMAPAY, quem="MARCELO")
    return next(a["id"] for a in fp.log() if a["destino"] == "analise")


@pytest.mark.banco
def test_lancar_cria_SO_SPS_uma_por_conta_com_RATEIO_MULTIPLO(banco_cards, monkeypatch):
    from app.apps.analisesps import folha_pagamento as fp
    pipe = PipefyFalso(monkeypatch)
    _fechar()
    analise = _gerar(monkeypatch)

    vista = fcd.previa(analise)
    assert vista["bloqueios"] == []
    assert vista["grupos"][0]["sps"][0]["etiquetas"] == "Folha de Pgt + Somapay"
    # Gerada sem o relatório em PDF: a prévia AVISA (05/10/2026), não bloqueia.
    assert vista["avisos"] and "relatório em PDF" in vista["avisos"][0]
    assert vista["grupos"][0]["tipo_sp"] == "777"
    assert vista["grupos"][0]["categoria"] == "2.01.01"

    saida = fcd.lancar(analise, quem="MARCELO")
    assert pipe.do_pipe(fcd.PIPE_DESPESA) == [], "o card de Despesa NÃO é mais criado"
    sps = pipe.do_pipe(fcd.PIPE_SP)
    assert len(sps) == 2, "UMA SP por conta de origem (50024 e 50025)"
    por_conta = {s["valores"]["valor"]: s for s in sps}
    sp = por_conta["1000.00"]
    assert sp["titulo"] == "Folha de Pagamento - Quinzena"
    assert sp["valores"]["tipo_de_despesa"] == "777"
    assert sp["valores"]["rateio_m_ltiplo_entre_centros_de_custo"] == "Sim"
    assert '"cCodDep":"111"' in sp["valores"]["rateio_m_ltiplo"]
    assert '"codigo_categoria":"2.01.01"' in sp["valores"]["rateio_m_ltiplo"]
    assert sp["valores"]["chave_pix_aleat_ria"] == "Atualizar Chave"
    assert "CREPEOLINDA: R$ 1.000,00" in sp["valores"]["descri_o"]
    # Campo de fase vai depois.
    # As DUAS etiquetas (05/10/2026): "Folha de Pgt" + a do destino (SomaPay aqui).
    assert pipe.atualizacoes_de(sp["id"])["etiquetas"] == ["318116255", "318116254"]

    log = {a["id"]: a for a in fp.log()}
    assert set(log[analise]["card_pipefy"].split(",")) == {s["id"] for s in sps}
    assert len(saida["sps"]) == 2 and saida["despesas"] == []

    with pytest.raises(fcd.ErroDosCards) as erro:
        fcd.lancar(analise)
    assert "já foi lançado" in str(erro.value)
    assert len(pipe.criados) == 2


@pytest.mark.banco
def test_obra_SEM_CODIGO_OMIE_bloqueia_e_nada_e_criado(banco_cards, monkeypatch):
    pipe = PipefyFalso(monkeypatch)
    _fechar(partes=(("CREPEOLINDA", "1000.00"), ("SEMOMIE", "200.00")))
    analise = _gerar(monkeypatch)
    vista = fcd.previa(analise)
    assert any("SEMOMIE" in b and "Código Omie" in b for b in vista["bloqueios"])
    with pytest.raises(fcd.ErroDosCards):
        fcd.lancar(analise)
    assert pipe.criados == []


@pytest.mark.banco
def test_TIPO_DE_DESPESA_ou_CATEGORIA_nao_encontrados_bloqueiam(banco_cards, monkeypatch):
    """Os códigos são procurados pelo nome — e nunca escolhidos no escuro."""
    pipe = PipefyFalso(monkeypatch, tipos=[{"id": "1", "nome": "Outra coisa"}],
                       categorias=[])
    _fechar()
    analise = _gerar(monkeypatch)
    vista = fcd.previa(analise)
    assert any("Salários e Ordenados" in b and "Plano Financeiro" in b and "Pipefy" in b
               for b in vista["bloqueios"])
    assert any("plano financeiro do OMIE" in b for b in vista["bloqueios"])
    assert pipe.criados == []


@pytest.mark.banco
def test_o_RECORD_ID_e_o_CODIGO_OMIE_vem_da_aba_PLANO_FINANCEIRO(banco_cards,
                                                                monkeypatch):
    """O dono, 03/10/2026: *"o ID para lançar no Pipefy seria o Record ID"* da
    aba Plano Financeiro. Com a aba respondendo, o Pipefy nem é consultado para
    o tipo — nem quando ele não diz a que tabela a conexão está ligada."""
    from app.apps.analisesps import pipefy as _pipefy
    pipe = PipefyFalso(monkeypatch, tipos=[], categorias=[], plano=[
        {"nome": "SALÁRIOS E ORDENADOS", "record_id": "383928967",
         "codigo_omie": "2.01.99"},
        {"nome": "Despesas com Alimentação", "record_id": "1", "codigo_omie": "3"}])
    pipe.pipes[fcd.PIPE_SP]["campos"]["tipo_de_despesa"]["ligado_a"] = None
    monkeypatch.setattr(_pipefy, "tabela_do_registro", lambda *a, **k: pytest.fail(
        "com a aba respondendo, o Pipefy não é consultado"))
    _fechar()
    analise = _gerar(monkeypatch)
    vista = fcd.previa(analise)
    assert vista["bloqueios"] == []
    assert vista["grupos"][0]["tipo_sp"] == "383928967"
    assert vista["grupos"][0]["categoria"] == "2.01.99"
    assert "Plano Financeiro" in vista["como_tipo"]
    fcd.lancar(analise, quem="MARCELO")
    assert {s["valores"]["tipo_de_despesa"] for s in pipe.do_pipe(fcd.PIPE_SP)} == {
        "383928967"}


@pytest.mark.banco
def test_nome_REPETIDO_com_ids_diferentes_na_aba_nao_e_escolhido(banco_cards,
                                                                monkeypatch):
    PipefyFalso(monkeypatch, tipos=[], plano=[
        {"nome": "Salários e Ordenados", "record_id": "1", "codigo_omie": "2.01.01"},
        {"nome": "Salários e Ordenados", "record_id": "2", "codigo_omie": "2.01.01"}])
    _fechar()
    vista = fcd.previa(_gerar(monkeypatch))
    assert any("Record ID" in b and "dois ids" in b for b in vista["bloqueios"])


@pytest.mark.banco
def test_campo_que_SUMIU_do_pipe_bloqueia(banco_cards, monkeypatch):
    pipe = PipefyFalso(monkeypatch)
    del pipe.pipes[fcd.PIPE_SP]["campos"]["rateio_m_ltiplo"]
    _fechar()
    analise = _gerar(monkeypatch)
    assert any("rateio_m_ltiplo" in b for b in fcd.previa(analise)["bloqueios"])


@pytest.mark.banco
def test_fechamento_que_MUDOU_depois_de_gerar_bloqueia(banco_cards, monkeypatch):
    pipe = PipefyFalso(monkeypatch)
    _fechar()
    analise = _gerar(monkeypatch)
    _fechar(partes=(("CREPEOLINDA", "1200.00"), ("CREPEAREIAS", "500.00")))
    vista = fcd.previa(analise)
    assert any("fechamento foi alterado" in b for b in vista["bloqueios"])
    assert pipe.criados == []


@pytest.mark.banco
def test_parou_no_meio_CONTINUA_sem_repetir_o_que_ja_foi_criado(banco_cards,
                                                              monkeypatch):
    pipe = PipefyFalso(monkeypatch, falhar_na=1)   # cai na segunda SP
    _fechar()
    analise = _gerar(monkeypatch)
    with pytest.raises(fcd.ErroDosCards) as erro:
        fcd.lancar(analise)
    assert "Já criados" in str(erro.value)
    assert len(pipe.criados) == 1
    fcd.lancar(analise)
    assert len(pipe.do_pipe(fcd.PIPE_SP)) == 2, "a primeira não se repete"


@pytest.mark.banco
def test_o_lancamento_sai_pela_ANALISE_e_nao_por_arquivo_de_conta(banco_cards,
                                                                monkeypatch):
    from app.apps.analisesps import folha_pagamento as fp
    PipefyFalso(monkeypatch)
    _fechar()
    _gerar(monkeypatch)
    conta = next(a["id"] for a in fp.log() if a["destino"] != "analise")
    with pytest.raises(fcd.ErroDosCards) as erro:
        fcd.previa(conta)
    assert "ANÁLISE" in str(erro.value)


@pytest.mark.banco
def test_a_rodada_pega_SO_os_arquivos_da_mesma_geracao(banco_cards, monkeypatch):
    from app.apps.analisesps import folha_pagamento as fp
    PipefyFalso(monkeypatch)
    _fechar()
    primeira = _gerar(monkeypatch)
    segunda = _gerar(monkeypatch)
    a, b = fcd.rodada(primeira), fcd.rodada(segunda)
    assert len(a["arquivos"]) == len(b["arquivos"]) == 2
    assert not {x["id"] for x in a["arquivos"]} & {x["id"] for x in b["arquivos"]}
    assert len(fp.log()) == 6


@pytest.mark.banco
def test_lancar_CONTA_POR_CONTA_e_nao_repete(banco_cards, monkeypatch):
    from app.apps.analisesps import folha_pagamento as fp
    pipe = PipefyFalso(monkeypatch)
    _fechar()
    analise = _gerar(monkeypatch)
    fcd.lancar(analise, quem="MARCELO", contas=["50024"])
    assert [s["valores"]["valor"] for s in pipe.do_pipe(fcd.PIPE_SP)] == ["1000.00"]
    rodada = next(r for r in fp.rodadas() if r["analise"]["id"] == analise)
    assert not rodada["lancado"] and rodada["lancavel"], "falta a 50025"
    assert fcd.previa(analise, contas=["50024", "50025"])["bloqueios"]
    fcd.lancar(analise, quem="MARCELO", contas=["50025"])
    assert len(pipe.do_pipe(fcd.PIPE_SP)) == 2
    rodada = next(r for r in fp.rodadas() if r["analise"]["id"] == analise)
    assert rodada["lancado"] and not rodada["lancavel"]


# ---------------------------------------------------------------------------
# O CAMPO PARECIDO — continua valendo para a busca por rótulo
# ---------------------------------------------------------------------------
def test_o_rotulo_EXATO_ganha_do_parecido():
    from app.apps.analisesps import pipefy
    campos = {"valor_centro_de_custo_1": {"label": "Valor Centro de Custo 1"},
              "valor": {"label": "Valor"}}
    assert pipefy.achar_campo(campos, "valor", fora=("centro de custo",)) == "valor"


def test_achar_campo_sem_pedaco_nenhum_devolve_vazio():
    from app.apps.analisesps import pipefy
    assert pipefy.achar_campo({"a": {"label": "Qualquer"}}) == ""
    assert pipefy.achar_campo({}, "valor") == ""


