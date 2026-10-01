# -*- coding: utf-8 -*-
"""
Os cards do Pipefy da folha — cópia fiel do cenário do Make (01/10/2026).

⚠️ O QUE ESTES TESTES PROTEGEM. Card criado no Pipefy **não se apaga** pelo
sistema. A primeira versão criava um card por conta com três campos, e o Pipefy
recusou o primeiro lançamento de verdade por falta de obrigatórios. O dono:
*"existe a criação de dois cards. O script está funcionando 100%, você precisa
olhar com detalhe a forma que o card é criado, não precisa errar."*

Daí o desenho testado aqui, igual ao blueprint `DP - FIN - Botão Folha de
Pagamento (🆕SP)`:

  - **UM card de Despesa** por pagamento, com cada obra como centro de custo, o
    código do OMIE da "C. Diários", e os números fixos do Make;
  - **UMA SP de Transferência de Recursos por conta de origem**, ligada à
    Despesa nos dois sentidos;
  - **nada é criado** com obra sem código, obra sem centro no Pipefy, campo
    sumido do pipe, ou fechamento que mudou depois de gerar;
  - **parou no meio, continua de onde parou** — sem criar o mesmo card de novo.
"""
import datetime as dt
from decimal import Decimal as D

import pytest

from app.apps.analisesps import folha_cards as fcd


# ---------------------------------------------------------------------------
# OS IDS E OS NÚMEROS FIXOS DO MAKE
# ---------------------------------------------------------------------------
def test_os_pares_62_e_72_usam_o_id_que_o_pipefy_deu_ao_campo():
    """No blueprint, o valor do par 62 vai em `valor_centro_de_custo_63` e o do 63
    em `valor_centro_de_custo_63_1`. Não é defeito: é o id do campo no Pipefy."""
    assert fcd.campos_do_par(1) == ("centro_de_custo_1", "valor_centro_de_custo_1",
                                    "departamento_omie_c_digo_centro_de_custo_1")
    assert fcd.campos_do_par(62)[1] == "valor_centro_de_custo_63"
    assert fcd.campos_do_par(63)[1] == "valor_centro_de_custo_63_1"
    assert fcd.campos_do_par(72)[1] == "valor_centro_de_custo_73"
    assert fcd.campos_do_par(73)[1] == "valor_centro_de_custo_73_1"
    assert fcd.campos_do_par(74)[1] == "valor_centro_de_custo_74"


def test_o_grupo_e_os_tipos_de_despesa_sao_os_do_switch_do_make():
    assert fcd.grupo_da_verba("folha", "quinzena") == (
        "Folha de Pagamento - Quinzena", "386045084", "383928967")
    assert fcd.grupo_da_verba("folha", "fim_de_mes")[0] == \
        "Folha de Pagamento - Fim de Mês"
    assert fcd.grupo_da_verba("alimentacao", "quinzena") == (
        "Auxílio Alimentação", "386045055", "383846061")
    assert fcd.grupo_da_verba("transporte", "fim_de_mes") == (
        "Auxílio Transporte", "386045060", "383846062")
    assert fcd.grupo_da_verba("diaria", "quinzena")[1:] == ("404847566", "383928967")
    with pytest.raises(fcd.ErroDosCards):
        fcd.grupo_da_verba("cesta", "quinzena")


def _grupo(**mudancas):
    base = {"verba": "folha", "grupo": "Folha de Pagamento - Quinzena",
            "tipo_dc": "386045084", "tipo_sp": "383928967",
            "total": D("1500.00"), "pessoas": 2, "descricao": "Competência: 09/2026",
            "links_pagamento": ["https://drive/pag1", "https://drive/pag2"],
            "centros": [{"obra": "CREPEOLINDA", "valor": D("1000.00"),
                         "centro": "555", "omie": "111"},
                        {"obra": "CREPEAREIAS", "valor": D("500.00"),
                         "centro": "556", "omie": "222"}],
            "sps": [{"conta": "50024", "valor": D("1000.00"),
                     "link": "https://drive/pag1"}]}
    base.update(mudancas)
    return base


AGORA = dt.datetime(2026, 10, 1, 14, 5)


def test_o_card_de_despesa_tem_os_campos_do_make():
    campos = {c["campo"]: c["valor"] for c in fcd.campos_da_despesa(_grupo(), AGORA)}
    assert campos["data"] == "01/10/2026 14:05"
    assert campos["data_de_pagamento"] == "01/10/2026 14:05"
    assert campos["valor"] == "1500.00"
    assert campos["tipo_de_despesa"] == "386045084"
    # ⚠️ "Pgt Conjunto" e o responsável fixo: sem eles o Pipefy cobra campos
    # que o Make nunca precisou mandar.
    assert campos["op_o"] == "Pgt Conjunto"
    assert campos["respons_vel_pela_solicita_ox"] == "383926874"
    assert campos["centro_de_custo_1"] == "555"
    assert campos["valor_centro_de_custo_1"] == "1000.00"
    assert campos["departamento_omie_c_digo_centro_de_custo_1"] == "111"
    assert campos["centro_de_custo_2"] == "556"
    assert "centro_de_custo_3" not in campos
    assert campos["banco_do_pagamento"] == "395832004"
    assert campos["valor_total_pago"] == "1500.00"
    assert campos["valida_o_dc"] == "Sim"


def test_a_sp_de_transferencia_tem_os_campos_do_make():
    grupo = _grupo()
    campos = {c["campo"]: c["valor"] for c in fcd.campos_da_sp(
        grupo, grupo["sps"][0], "9001", "https://drive/ana", AGORA)}
    assert campos["descri_o"].startswith("Conta Origem: 50024\nCompetência")
    assert campos["data"] == "01/10/2026"
    assert campos["data_de_pagamento"] == "02/10/2026", "o Make põe o dia seguinte"
    assert campos["valor"] == "1000.00"
    assert campos["colaborador_solicitante"] == "383926874"
    assert campos["tipo_de_pagamento"] == "Pix"
    assert campos["tipo_de_despesa"] == "383928967"
    assert campos["selecione_o_procedimento"] == "Transferência de Recursos"
    assert campos["alimenta_o_de_equipe"] == "Não"
    assert campos["parcelas"] == "1x Parcela"
    assert campos["chave_pix_aleat_ria"] == "a7398865-d869-4437-b7a9-fc6fe904c4d7"
    assert campos["radio_horizontal_t_tulo"] == "Pessoa Jurídica"
    assert campos["tipo"] == "Aleatória"
    assert campos["cnpj"] == "00.079.526/0001-09"
    assert campos["link_planilha_de_an_lise"] == "https://drive/ana"
    assert campos["conex_o_dc_id"] == "9001"
    assert campos["etiquetas"] == "307726886"
    assert campos["valida_o_sp_1"] == "Sim"


def test_campo_de_fase_vai_DEPOIS_da_criacao():
    """O que está no formulário inicial vai na criação — é ali que o Pipefy cobra
    os obrigatórios. O resto é gravado no card já criado."""
    na, depois = fcd._separar(
        [{"campo": "valor", "valor": "1"}, {"campo": "banco_do_pagamento",
                                           "valor": "395832004"},
         {"campo": "descri_o", "valor": ""}],
        {"valor": {}, "descri_o": {}})
    assert [v["campo"] for v in na] == ["valor"]
    assert [v["campo"] for v in depois] == ["banco_do_pagamento"]


def test_a_descricao_do_card_diz_TUDO_o_que_alguem_vai_querer_saber():
    texto = fcd.descricao_do_card(
        "09/2026", "quinzena", ["alimentacao", "transporte"], "50024", 12,
        "4200.00", "https://drive/pag", "https://drive/ana")
    assert "Competência: 09/2026" in texto
    assert "Quinzena (dias 1 a 15)" in texto
    assert "Alimentação + Transporte" in texto
    assert "R$ 4.200,00" in texto, "o card é lido por gente, com vírgula"
    assert "https://drive/pag" in texto and "https://drive/ana" in texto


# ---------------------------------------------------------------------------
# PONTA A PONTA, COM BANCO: gerar → prévia → lançar
# ---------------------------------------------------------------------------
GERLANIO = "99713349334"
ANA = "03513441363"


def _campos_de(ids, obrigatorios=()):
    return {i: {"label": i, "tipo": "short_text", "opcoes": [],
                "obrigatorio": i in obrigatorios, "ligado_a": None} for i in ids}


class PipefyFalso:
    """Os dois pipes com os campos do Make, e o registro do que foi criado."""

    def __init__(self, monkeypatch, registros=None, falhar_na=None):
        from app.apps.analisesps import pipefy
        self.criados, self.atualizados = [], []
        self.falhar_na = falhar_na
        inicio_dc = _campos_de(
            ["data", "data_de_pagamento", "descri_o", "valor", "tipo_de_despesa",
             "op_o", "respons_vel_pela_solicita_ox"]
            + [c for n in range(1, 76) for c in fcd.campos_do_par(n)[:2]])
        inicio_dc["centro_de_custo_1"].update(
            {"tipo": "connector", "ligado_a": {"tipo": "tabela", "id": "T1",
                                               "nome": "Centros de Custo"}})
        fases_dc = _campos_de(
            ["banco_do_pagamento", "valor_total_pago", "valida_o_dc",
             "link_para_planilha_de_pagamento", "link_para_planilha_de_an_lise",
             "conex_o_sp"]
            + [fcd.campos_do_par(n)[2] for n in range(1, 76)])
        inicio_sp = _campos_de([c for c in fcd.CAMPOS_DA_SP
                                if c not in ("valida_o_sp_1", "conex_o_dc_id",
                                             "etiquetas", "conex_o_dc")])
        fases_sp = _campos_de(["valida_o_sp_1", "conex_o_dc_id", "etiquetas",
                               "conex_o_dc"])
        self.pipes = {
            fcd.PIPE_DESPESA: {"id": fcd.PIPE_DESPESA, "nome": "Despesa",
                               "campos": inicio_dc, "campos_das_fases": fases_dc},
            fcd.PIPE_SP: {"id": fcd.PIPE_SP, "nome": "SP", "campos": inicio_sp,
                          "campos_das_fases": fases_sp}}
        self.registros = registros if registros is not None else [
            {"id": "555", "nome": "CREPEOLINDA"},
            {"id": "556", "nome": "CREPEAREIAS"}]
        monkeypatch.setattr(pipefy, "campos_do_pipe",
                            lambda pipe, *a, **k: self.pipes[str(pipe)])
        monkeypatch.setattr(pipefy, "registros_da_tabela",
                            lambda *a, **k: list(self.registros))
        monkeypatch.setattr(pipefy, "criar_card", self.criar)
        monkeypatch.setattr(pipefy, "atualizar_campos", self.atualizar)

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
        self.atualizados.append((str(card), {v["campo"]: v["valor"]
                                             for v in valores}))
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
def test_lancar_cria_UMA_despesa_e_UMA_SP_POR_CONTA_ligadas(banco_cards,
                                                           monkeypatch):
    from app.apps.analisesps import folha_pagamento as fp
    pipe = PipefyFalso(monkeypatch)
    _fechar()
    analise = _gerar(monkeypatch)

    vista = fcd.previa(analise)
    assert vista["bloqueios"] == []
    assert vista["como_centro"] == 'tabela "Centros de Custo"'

    saida = fcd.lancar(analise, quem="MARCELO")
    despesas, sps = pipe.do_pipe(fcd.PIPE_DESPESA), pipe.do_pipe(fcd.PIPE_SP)
    assert len(despesas) == 1, "UM card de Despesa para o pagamento inteiro"
    assert len(sps) == 2, "UMA SP por conta de origem (50024 e 50025)"

    dc = despesas[0]
    assert dc["titulo"].count("/") == 2, "o título é a data, como no Make"
    assert dc["valores"]["valor"] == "1500.00"
    assert dc["valores"]["op_o"] == "Pgt Conjunto"
    # O centro de custo é o REGISTRO do Pipefy com o nome da obra.
    centros = {dc["valores"]["centro_de_custo_1"], dc["valores"]["centro_de_custo_2"]}
    assert centros == {"555", "556"}
    # O banco e os códigos do OMIE são campos de fase: vão depois.
    depois = pipe.atualizacoes_de(dc["id"])
    assert depois["banco_do_pagamento"] == "395832004"
    assert {depois["departamento_omie_c_digo_centro_de_custo_1"],
            depois["departamento_omie_c_digo_centro_de_custo_2"]} == {"111", "222"}
    assert depois["link_para_planilha_de_an_lise"].startswith("https://drive/")
    assert sorted(depois["conex_o_sp"]) == sorted(s["id"] for s in sps)

    por_conta = {s["valores"]["descri_o"].split("\n")[0]: s for s in sps}
    assert por_conta["Conta Origem: 50024"]["valores"]["valor"] == "1000.00"
    assert por_conta["Conta Origem: 50025"]["valores"]["valor"] == "500.00"
    for s in sps:
        assert s["titulo"] == "Folha de Pagamento - Quinzena"
        assert pipe.atualizacoes_de(s["id"])["conex_o_dc"] == [dc["id"]]
        assert pipe.atualizacoes_de(s["id"])["conex_o_dc_id"] == dc["id"]

    # O log amarra: a análise à Despesa, cada arquivo de conta à SP dele.
    log = {a["id"]: a for a in fp.log()}
    assert log[analise]["card_pipefy"] == dc["id"]
    contas = {a["conta"]: a["card_pipefy"] for a in log.values()
              if a["destino"] != "analise"}
    assert contas["50024"] == por_conta["Conta Origem: 50024"]["id"]
    assert saida["despesas"][0]["id"] == dc["id"]

    # E de novo não cria nada.
    with pytest.raises(fcd.ErroDosCards) as erro:
        fcd.lancar(analise)
    assert "já foi lançado" in str(erro.value)
    assert len(pipe.criados) == 3


@pytest.mark.banco
def test_obra_SEM_CODIGO_OMIE_bloqueia_e_nada_e_criado(banco_cards, monkeypatch):
    pipe = PipefyFalso(monkeypatch, registros=[
        {"id": "555", "nome": "CREPEOLINDA"}, {"id": "557", "nome": "SEMOMIE"}])
    _fechar(partes=(("CREPEOLINDA", "1000.00"), ("SEMOMIE", "200.00")))
    analise = _gerar(monkeypatch)

    vista = fcd.previa(analise)
    assert any("SEMOMIE" in b and "Código Omie" in b for b in vista["bloqueios"])
    with pytest.raises(fcd.ErroDosCards):
        fcd.lancar(analise)
    assert pipe.criados == []


@pytest.mark.banco
def test_obra_SEM_CENTRO_DE_CUSTO_no_pipefy_bloqueia(banco_cards, monkeypatch):
    pipe = PipefyFalso(monkeypatch, registros=[{"id": "555", "nome": "CREPEOLINDA"}])
    _fechar()
    analise = _gerar(monkeypatch)
    vista = fcd.previa(analise)
    assert any("CREPEAREIAS" in b and "centro de custo no Pipefy" in b
               for b in vista["bloqueios"])
    with pytest.raises(fcd.ErroDosCards):
        fcd.lancar(analise)
    assert pipe.criados == []


@pytest.mark.banco
def test_campo_que_SUMIU_do_pipe_bloqueia(banco_cards, monkeypatch):
    """Alguém renomeou um campo no Pipefy: o valor cairia em lugar nenhum."""
    pipe = PipefyFalso(monkeypatch)
    del pipe.pipes[fcd.PIPE_DESPESA]["campos"]["op_o"]
    _fechar()
    analise = _gerar(monkeypatch)
    vista = fcd.previa(analise)
    assert any("op_o" in b for b in vista["bloqueios"])


@pytest.mark.banco
def test_fechamento_que_MUDOU_depois_de_gerar_bloqueia(banco_cards, monkeypatch):
    """O card contaria uma história e o arquivo outra."""
    pipe = PipefyFalso(monkeypatch)
    _fechar()
    analise = _gerar(monkeypatch)
    _fechar(partes=(("CREPEOLINDA", "1200.00"), ("CREPEAREIAS", "500.00")))
    vista = fcd.previa(analise)
    assert any("fechamento mudou" in b for b in vista["bloqueios"])
    assert pipe.criados == []


@pytest.mark.banco
def test_parou_no_meio_CONTINUA_sem_repetir_o_que_ja_foi_criado(banco_cards,
                                                              monkeypatch):
    pipe = PipefyFalso(monkeypatch, falhar_na=2)   # cai na segunda SP
    _fechar()
    analise = _gerar(monkeypatch)

    with pytest.raises(fcd.ErroDosCards) as erro:
        fcd.lancar(analise)
    assert "Já criados" in str(erro.value)
    assert "continua de onde parou" in str(erro.value)
    assert len(pipe.criados) == 2          # a Despesa e a primeira SP

    fcd.lancar(analise)
    assert len(pipe.do_pipe(fcd.PIPE_DESPESA)) == 1, "a Despesa não se repete"
    assert len(pipe.do_pipe(fcd.PIPE_SP)) == 2
    dc = pipe.do_pipe(fcd.PIPE_DESPESA)[0]["id"]
    assert len(pipe.atualizacoes_de(dc)["conex_o_sp"]) == 2


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
    """Regerar é normal (D15): a segunda geração tem a sua análise e os seus
    arquivos, e o lançamento de uma não leva os da outra."""
    from app.apps.analisesps import folha_pagamento as fp
    PipefyFalso(monkeypatch)
    _fechar()
    primeira = _gerar(monkeypatch)
    segunda = _gerar(monkeypatch)
    a, b = fcd.rodada(primeira), fcd.rodada(segunda)
    assert len(a["arquivos"]) == len(b["arquivos"]) == 2
    assert not {x["id"] for x in a["arquivos"]} & {x["id"] for x in b["arquivos"]}
    assert len(fp.log()) == 6


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
