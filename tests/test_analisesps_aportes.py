# -*- coding: utf-8 -*-
"""
Aportes no OMIE — a REGRA, e a amarração dos dois títulos.

O dono escreveu no briefing por que estes testes existem, e não dá para dizer
melhor: *"a regra da tabela é o coração disto: se ela errar, meus relatórios
de aporte mentem e eu não tenho como perceber."*

"Não tenho como perceber" é a parte que decide o rigor daqui. Um título com a
categoria errada entra no OMIE, a tela diz "gravado", e o número aparece no
lugar errado de um relatório que ninguém confere linha a linha. Não há tela
que denuncie, não há alerta, não há erro. Só o teste.

Por isso a tabela do briefing é percorrida INTEIRA, operação por operação,
lado por lado — não uma amostra.
"""
from __future__ import annotations

from datetime import date

import pytest

from app.apps.analisesps import aportes, aportes_omie


# Nomes de conta só para a tela mostrar nome em vez de número. NÃO é cadastro:
# a conta de cada lado é escolhida em cada lançamento — *"não quero travar a
# conta Matriz e a da Parceria, tem mais de uma situação."*
DESCRICOES = {7011: "BWS MATRIZ", 22069: "PARCERIA OBRA X",
              33333: "PARCERIA OBRA Y"}
# ⚠️ CINCO CATEGORIAS, e as duas do aporte da BWS têm CÓDIGOS DIFERENTES:
# "Aportes BWS" sai da provedora, "Aporte BWS" entra na parceria. Foi
# exatamente isto que o dono corrigiu em 21/09/2026.
CATEGORIAS = {
    "aportes_bws_saida": {"codigo": "2.08.97", "descricao": "Aportes BWS"},
    "aporte_bws_entrada": {"codigo": "1.02.94", "descricao": "Aporte BWS"},
    "aporte_parceiros": {"codigo": "1.02.02", "descricao": "Aporte Parceiros"},
    "devolucao_aportes": {"codigo": "2.08.02",
                          "descricao": "Devolução de Aportes"},
    "devolucao_aportes_bws": {"codigo": "1.02.95",
                              "descricao": "Devolução de Aportes BWS"},
}


def planejar(**mudancas):
    base = dict(
        operacao="aporte_bws", conta_origem=7011, conta_destino=22069,
        valor="12.500,00", data="2026-09-20", fornecedor=99,
        fornecedor_nome="PARCEIRO LTDA", obra="OBRA-1", obra_nome="Obra Um",
        quem="Marcelo", descricoes=DESCRICOES, categorias=CATEGORIAS)
    base.update(mudancas)
    return aportes.planejar(**base)


# ---------------------------------------------------------------------------
# A TABELA DO BRIEFING, INTEIRA
# ---------------------------------------------------------------------------
# Conta      | Entra/Sai | Categoria no OMIE        | Código
# -----------+-----------+--------------------------+---------
# Provedora  | Entrada   | Devolução de Aportes BWS | 1.02.95
# Provedora  | Saída     | Aportes BWS              | 2.08.97
# Parceria   | Entrada   | Aporte Parceiros         | 1.02.02
# Parceria   | Entrada   | Aporte BWS               | 1.02.94
# Parceria   | Saída     | Devolução de Aportes     | 2.08.02
# ---------------------------------------------------------------------------
ESPERADO = {
    "aporte_bws": [
        (aportes.PROVEDORA, "saida", "Aportes BWS", "P"),
        (aportes.PARCERIA, "entrada", "Aporte BWS", "R"),
    ],
    "devolucao_bws": [
        (aportes.PARCERIA, "saida", "Devolução de Aportes", "P"),
        (aportes.PROVEDORA, "entrada", "Devolução de Aportes BWS", "R"),
    ],
    "aporte_parceiro": [
        (aportes.PARCERIA, "entrada", "Aporte Parceiros", "R"),
    ],
    "devolucao_parceiro": [
        (aportes.PARCERIA, "saida", "Devolução de Aportes", "P"),
    ],
}

CONTAS_DA_OPERACAO = {
    "aporte_bws": {"conta_origem": 7011, "conta_destino": 22069},
    "devolucao_bws": {"conta_origem": 22069, "conta_destino": 7011},
    "aporte_parceiro": {"conta_origem": None, "conta_destino": 22069},
    "devolucao_parceiro": {"conta_origem": 22069, "conta_destino": None},
}


@pytest.mark.parametrize("operacao", list(ESPERADO))
def test_cada_operacao_monta_exatamente_os_lados_da_tabela(operacao):
    plano = planejar(operacao=operacao, **CONTAS_DA_OPERACAO[operacao])
    saiu = [(t["papel"], t["sentido"], t["categoria_nome"], t["natureza"])
            for t in plano["titulos"]]
    assert saiu == ESPERADO[operacao]


def test_o_aporte_da_bws_usa_CATEGORIAS_DIFERENTES_nos_dois_lados():
    """⚠️ O DEFEITO QUE ELE PEGOU EM 21/09/2026: *"você colocou Aportes BWS
    que ENTRA, conta Parceria, o mesmo código do que SAI. Não são."*

    O briefing de setembro dizia o contrário (*"o mesmo nome dos dois
    lados"*), e a primeira versão foi construída assim. Os nomes são quase
    iguais — "Aportes BWS" e "Aporte BWS" — e os códigos não têm nada a ver um
    com o outro. Um lançamento com a categoria do lado errado não acusa nada
    em tela nenhuma: só este teste."""
    plano = planejar(operacao="aporte_bws")
    saida, entrada = plano["titulos"]
    assert saida["categoria_nome"] == "Aportes BWS"
    assert saida["codigo_categoria"] == "2.08.97"
    assert entrada["categoria_nome"] == "Aporte BWS"
    assert entrada["codigo_categoria"] == "1.02.94"
    assert saida["codigo_categoria"] != entrada["codigo_categoria"]
    assert {t["natureza"] for t in plano["titulos"]} == {"P", "R"}


def test_as_cinco_categorias_sao_cinco_codigos_distintos():
    """Nenhuma das cinco divide código com outra. Se duas dividissem, um
    relatório somaria lados opostos do mesmo dinheiro na mesma linha."""
    codigos = [c["codigo"] for c in CATEGORIAS.values()]
    assert len(set(codigos)) == 5
    assert set(CATEGORIAS) == set(aportes.CATEGORIAS)


def test_operacao_com_o_parceiro_gera_um_titulo_so():
    """O dinheiro vem de fora ou vai para fora: não existe contrapartida
    interna, e inventar uma criaria movimento que não aconteceu."""
    for operacao in ("aporte_parceiro", "devolucao_parceiro"):
        plano = planejar(operacao=operacao, **CONTAS_DA_OPERACAO[operacao])
        assert len(plano["titulos"]) == 1, operacao
        assert plano["interna"] is False


def test_sai_dinheiro_vira_conta_a_pagar_e_entra_vira_conta_a_receber():
    for operacao, pernas in ESPERADO.items():
        plano = planejar(operacao=operacao, **CONTAS_DA_OPERACAO[operacao])
        for t in plano["titulos"]:
            esperada = "P" if t["sentido"] == "saida" else "R"
            assert t["natureza"] == esperada


# ---------------------------------------------------------------------------
# O QUE A REGRA RECUSA — e por que recusar é o certo
# ---------------------------------------------------------------------------
def test_qualquer_conta_serve_em_qualquer_papel():
    """⚠️ A CORREÇÃO DE 20/09: *"não quero travar a conta Matriz e a da
    Parceria, tem mais de uma situação."*

    Há mais de uma parceria, e a mesma conta pode fazer papéis diferentes. O
    que a OPERAÇÃO decide é o papel, o sentido e a categoria de cada lado; ele
    diz só qual conta faz aquele papel desta vez. Então uma segunda parceria
    entra sem cadastro nenhum, e a categoria continua saindo da regra."""
    plano = planejar(operacao="aporte_bws", conta_origem=7011,
                     conta_destino=33333)
    assert [t["id_conta_corrente"] for t in plano["titulos"]] == [7011, 33333]
    assert [t["categoria_nome"] for t in plano["titulos"]] == \
        ["Aportes BWS", "Aporte BWS"]
    assert plano["titulos"][1]["conta_descricao"] == "PARCERIA OBRA Y"


def test_a_mesma_conta_dos_dois_lados_e_recusada():
    """Sem cadastro fixo, esta é a única incoerência que o sistema consegue
    enxergar sozinho — e ela é sempre engano: o dinheiro sairia e entraria no
    mesmo lugar."""
    with pytest.raises(aportes.ErroDeRegra) as erro:
        planejar(operacao="aporte_bws", conta_origem=7011, conta_destino=7011)
    assert "mesma" in str(erro.value)


def test_a_conta_que_falta_e_pedida_pelo_sentido_do_dinheiro():
    """O rótulo é a pergunta inteira, não "origem": é o sentido do dinheiro
    que a pessoa tem na cabeça."""
    with pytest.raises(aportes.ErroDeRegra) as erro:
        planejar(conta_origem=None)
    frase = str(erro.value)
    assert "SAI" in frase
    assert "Provedora" in frase


def test_o_aporte_do_parceiro_nao_pede_conta_de_origem():
    """*"Nem sempre a conta de origem vai ser necessária. Se eu estiver
    lançando um dinheiro do parceiro, ele não vem de conta nenhuma — vem de
    outra empresa, que não nos interessa."*"""
    plano = planejar(operacao="aporte_parceiro", conta_origem=None,
                     conta_destino=22069)
    assert len(plano["titulos"]) == 1
    assert plano["titulos"][0]["sentido"] == "entrada"
    assert plano["titulos"][0]["id_conta_corrente"] == 22069


def test_sem_obra_nao_planeja():
    """Decisão do dono (20/09): obra sempre. Sem departamento o aporte existe
    no OMIE mas some de qualquer visão por obra — e some calado."""
    with pytest.raises(aportes.ErroDeRegra) as erro:
        planejar(obra="", obra_nome="")
    assert "obra" in str(erro.value).lower()


def test_sem_fornecedor_nao_planeja():
    with pytest.raises(aportes.ErroDeRegra):
        planejar(fornecedor=None)


def test_categoria_sem_codigo_para_a_tela_em_vez_de_chutar():
    """A frase do briefing: *"se a descrição não for encontrada (…) a tela tem
    de parar e me dizer, em vez de escolher uma."*"""
    sem = dict(CATEGORIAS)
    sem["aportes_bws_saida"] = {"codigo": "", "descricao": ""}
    with pytest.raises(aportes.ErroDeRegra) as erro:
        planejar(categorias=sem)
    assert "Aportes BWS" in str(erro.value)


@pytest.mark.parametrize("ruim", ["", "0", "-5", "abc", "R$ 0,00"])
def test_valor_impossivel_e_recusado(ruim):
    with pytest.raises(aportes.ErroDeRegra):
        planejar(valor=ruim)


def test_valor_brasileiro_e_entendido():
    assert planejar(valor="12.500,00")["valor"] == 12500.00
    assert planejar(valor="1234.56")["valor"] == 1234.56
    assert planejar(valor="R$ 1.000,50")["valor"] == 1000.50


def test_data_dos_dois_jeitos():
    assert planejar(data="2026-09-20")["data"] == date(2026, 9, 20)
    assert planejar(data="20/09/2026")["data"] == date(2026, 9, 20)
    with pytest.raises(aportes.ErroDeRegra):
        planejar(data="não é data")


# ---------------------------------------------------------------------------
# AS CINCO SITUAÇÕES — o que a tela mostra
# ---------------------------------------------------------------------------
def test_sao_cinco_situacoes_com_cinco_categorias_proprias():
    """Cada situação (conta + sentido) tem a SUA categoria. Foi a correção de
    21/09: a tela dividia uma categoria entre dois lados que não a dividem."""
    assert len(aportes.SITUACOES) == 5
    assert len({c for _, _, c in aportes.SITUACOES}) == 5, \
        "cinco situações, cinco categorias — nenhuma se repete"
    # A que ele citou: usada dos dois lados, saindo e entrando.
    assert aportes.situacoes_da_categoria("aportes_bws_saida") == [
        (aportes.PROVEDORA, "saida")]
    assert aportes.situacoes_da_categoria("aporte_bws_entrada") == [
        (aportes.PARCERIA, "entrada")]
    # A ÚNICA que serve a duas operações é a devolução da parceria: ela é a
    # mesma quando o dinheiro volta para a BWS e quando vai para o parceiro.
    assert aportes.situacoes_da_categoria("devolucao_aportes") == [
        (aportes.PARCERIA, "saida")]


def test_as_situacoes_batem_com_as_pernas_das_operacoes():
    """A tabela que a tela mostra e a regra que grava não podem divergir: se
    divergirem, ele confere uma coisa e o OMIE recebe outra."""
    das_operacoes = set()
    for op in aportes.OPERACOES.values():
        for perna in op["pernas"]:
            das_operacoes.add((perna.papel, perna.sentido, perna.categoria))
    assert das_operacoes == set(aportes.SITUACOES)


def test_o_resumo_da_operacao_diz_o_movimento_sem_pedir_conta():
    """É o que a tela mostra assim que ele escolhe a operação, antes de pedir
    valor ou data."""
    resumo = aportes.resumo_das_pernas("aporte_bws")
    assert [r["sentido_rotulo"] for r in resumo] == ["Saída", "Entrada"]
    assert [r["natureza_rotulo"] for r in resumo] == \
        ["Conta a pagar", "Conta a receber"]
    assert [r["categoria_nome"] for r in resumo] == \
        ["Aportes BWS", "Aporte BWS"]
    assert len(aportes.resumo_das_pernas("aporte_parceiro")) == 1


# ---------------------------------------------------------------------------
# O QUE AMARRA OS DOIS LADOS
# ---------------------------------------------------------------------------
def test_os_dois_titulos_levam_o_mesmo_numero_de_documento():
    """Sem um número comum, dois títulos em contas diferentes são dois
    lançamentos soltos, e descobrir que um é a contrapartida do outro vira
    arqueologia."""
    plano = planejar()
    numeros = {t["numero_documento"] for t in plano["titulos"]}
    assert len(numeros) == 1
    assert plano["numero_documento"] in numeros


def test_o_codigo_de_integracao_e_diferente_em_cada_lado():
    """O número do documento é o mesmo; o código de integração NÃO pode ser —
    é ele que o OMIE usa para recusar lançamento repetido. Igual nos dois, o
    segundo título seria recusado como duplicata do primeiro."""
    plano = planejar()
    codigos = [t["codigo_integracao"] for t in plano["titulos"]]
    assert len(set(codigos)) == len(codigos)


def test_dois_planos_seguidos_nao_colidem():
    assert planejar()["grupo"] != planejar()["grupo"]


def test_a_observacao_automatica_diz_o_que_e_e_aponta_o_par():
    """Quem lê isto é o contador, dentro do OMIE, seis meses depois — sem esta
    tela à mão e sem saber que ela existe."""
    plano = planejar()
    for t in plano["titulos"]:
        assert plano["numero_documento"] in t["observacao"]
        assert "Marcelo" in t["observacao"]
    assert "Provedora" in plano["titulos"][0]["observacao"]


def test_numero_e_observacao_podem_ser_trocados_pelo_dono():
    """Decisão dele em 20/09: automáticos, e editáveis antes de gravar."""
    plano = planejar(numero="MEU-NUMERO",
                     observacoes={"provedora_saida": "texto meu"})
    assert plano["numero_documento"] == "MEU-NUMERO"
    assert plano["titulos"][0]["observacao"] == "texto meu"
    # O lado que ele NÃO trocou continua com o texto automático.
    assert "Contrapartida" in plano["titulos"][1]["observacao"]


# ---------------------------------------------------------------------------
# A CRÍTICA DE TRANSFERÊNCIA
# ---------------------------------------------------------------------------
def test_a_critica_avisa_e_nao_bloqueia():
    """*"É aviso, não bloqueio. Eu confirmo e sigo, mas confirmo sabendo."*"""
    plano = planejar()
    avisos = aportes.criticar(plano, [{
        "valor": 12500.0, "data": "20/09/2026",
        "conta_a": "BWS MATRIZ", "conta_b": "PARCERIA OBRA X",
        "categoria": "Transferência entre contas"}])
    assert avisos, "não avisou"
    texto = " ".join(avisos)
    assert "12.500,00" in texto
    assert "BWS MATRIZ" in texto and "PARCERIA OBRA X" in texto
    assert "transferência entre contas suas" in texto
    # E o plano continua de pé: criticar não levanta nada nem muda o plano.
    assert len(plano["titulos"]) == 2


def test_sem_nada_parecido_a_critica_fica_calada():
    assert aportes.criticar(planejar(), []) == []


def test_categoria_marcada_como_transferencia_e_denunciada():
    """Se a própria categoria da regra estiver marcada como transferência no
    plano financeiro, o lançamento nasce FORA do DRE — e some dos relatórios
    de aporte sem dizer nada."""
    marcadas = dict(CATEGORIAS)
    marcadas["aportes_bws_saida"] = dict(CATEGORIAS["aportes_bws_saida"],
                                         transferencia="S")
    plano = planejar(categorias=marcadas)
    avisos = aportes.criticar(plano, [])
    assert any("FORA do DRE" in a for a in avisos)


# ---------------------------------------------------------------------------
# O PACOTE QUE VAI PARA O OMIE
# ---------------------------------------------------------------------------
def test_o_ensaio_nao_fala_com_o_omie():
    """`ensaiar` não recebe cliente nenhum — não tem como chamar a API mesmo
    que alguém queira. É assim que o "nunca escrever sem ensaio" para de
    depender de disciplina."""
    ensaio = aportes_omie.ensaiar(planejar())
    assert len(ensaio) == 2
    assert ensaio[0]["chamada"] == "IncluirContaPagar"
    assert ensaio[1]["chamada"] == "IncluirContaReceber"
    assert "contapagar" in ensaio[0]["url"]


def test_o_pacote_leva_a_obra_inteira_num_departamento_so():
    param = aportes_omie.montar_inclusao(planejar()["titulos"][0])
    assert param["distribuicao"] == [{"cCodDep": "OBRA-1", "nPerDep": 100}]
    assert param["codigo_categoria"] == "2.08.97"
    assert param["id_conta_corrente"] == 7011
    assert param["valor_documento"] == 12500.0
    assert param["data_vencimento"] == "20/09/2026"


def test_desmarcar_a_baixa_tira_a_baixa_do_ensaio():
    sem = aportes_omie.ensaiar(planejar(baixar=False))
    assert all(i["baixa"] is None for i in sem)
    com = aportes_omie.ensaiar(planejar(baixar=True))
    assert all(i["baixa"] is not None for i in com)


# ---------------------------------------------------------------------------
# A AMARRAÇÃO: OU OS DOIS ENTRAM, OU O QUE ENTROU É DESFEITO
# ---------------------------------------------------------------------------
class OmieFalso:
    """Um OMIE de mentira que responde o que o teste mandar.

    `falhar_em` é o número da chamada de INCLUSÃO que deve estourar (1 = a
    primeira). `falhar_exclusao` simula a API recusando desfazer.
    """

    def __init__(self, falhar_em=None, falhar_exclusao=False,
                 falhar_baixa=False):
        self.falhar_em = falhar_em
        self.falhar_exclusao = falhar_exclusao
        self.falhar_baixa = falhar_baixa
        self.chamadas = []
        self._inclusoes = 0

    def _call(self, url, call, param):
        self.chamadas.append((call, param))
        if call.startswith("Incluir"):
            self._inclusoes += 1
            if self.falhar_em == self._inclusoes:
                raise RuntimeError("ERROR: codigo_categoria inválido")
            return {"codigo_lancamento_omie": 1000 + self._inclusoes}
        if call.startswith("Excluir"):
            if self.falhar_exclusao:
                raise RuntimeError("ERROR: título já possui baixa")
            return {"codigo_status": "0"}
        if self.falhar_baixa:
            raise RuntimeError("ERROR: conta corrente não permite baixa")
        return {"codigo_baixa": 555}


@pytest.fixture(autouse=True)
def sem_banco(monkeypatch):
    """O registro vai para o banco, e aqui não há banco. Ele já é à prova de
    falha em produção (registro que falha não desfaz o que deu certo no OMIE),
    mas dublar deixa o teste falar só do que interessa."""
    monkeypatch.setattr(aportes_omie, "_registrar", lambda *a, **k: None)
    monkeypatch.setattr(aportes_omie, "_marcar_desfeito", lambda *a, **k: None)


def test_os_dois_titulos_entram_e_sao_baixados():
    cli = OmieFalso()
    resultado = aportes_omie.gravar(planejar(), "Marcelo", cliente=cli)
    assert resultado["ok"] is True
    assert [l["codigo"] for l in resultado["titulos"]] == [1001, 1002]
    assert all(l["baixado"] for l in resultado["titulos"])
    chamadas = [c for c, _ in cli.chamadas]
    # A ORDEM IMPORTA: os dois títulos primeiro, as baixas depois.
    assert chamadas == ["IncluirContaPagar", "IncluirContaReceber",
                        "LancarPagamento", "LancarRecebimento"]


def test_se_o_segundo_titulo_falhar_o_primeiro_e_desfeito():
    """⚠️ O caso que o dono descreveu: *"se o segundo título falhar depois do
    primeiro ter sido criado, eu fico com meio aporte no OMIE — pior do que
    não ter lançado nada, porque nenhum relatório fecha e ninguém percebe."*"""
    cli = OmieFalso(falhar_em=2)
    resultado = aportes_omie.gravar(planejar(), "Marcelo", cliente=cli)
    assert resultado["ok"] is False
    assert "ExcluirContaPagar" in [c for c, _ in cli.chamadas]
    excluiu = next(p for c, p in cli.chamadas if c == "ExcluirContaPagar")
    assert excluiu["codigo_lancamento_omie"] == 1001
    assert not resultado["orfaos"], "deu para desfazer e mesmo assim gritou órfão"
    assert any("desfeito" in a for a in resultado["avisos"])
    # E nenhuma baixa foi tentada: não se baixa o que não existe inteiro.
    assert not [c for c, _ in cli.chamadas if c.startswith("Lancar")]


def test_falhando_o_primeiro_titulo_nao_ha_o_que_desfazer():
    cli = OmieFalso(falhar_em=1)
    resultado = aportes_omie.gravar(planejar(), "Marcelo", cliente=cli)
    assert resultado["ok"] is False
    assert not [c for c, _ in cli.chamadas if c.startswith("Excluir")]
    assert not resultado["orfaos"]


def test_nao_dando_para_desfazer_o_orfao_e_entregue_com_numero():
    """*"Se desfazer não for possível pela API, a tela tem de me dar o número
    do título que ficou órfão, para eu resolver à mão no OMIE."*"""
    cli = OmieFalso(falhar_em=2, falhar_exclusao=True)
    resultado = aportes_omie.gravar(planejar(), "Marcelo", cliente=cli)
    assert resultado["ok"] is False
    assert len(resultado["orfaos"]) == 1
    orfao = resultado["orfaos"][0]
    assert orfao["codigo"] == 1001
    assert "Provedora" in orfao["papel"]


def test_baixa_que_falha_nao_apaga_os_titulos():
    """Título correto e em aberto é chato, visível e fácil de resolver à mão.
    Apagar título que talvez já tenha baixa é trocar um problema pequeno por
    um grande."""
    cli = OmieFalso(falhar_baixa=True)
    resultado = aportes_omie.gravar(planejar(), "Marcelo", cliente=cli)
    assert resultado["ok"] is True, "a operação em si deu certo"
    assert not [c for c, _ in cli.chamadas if c.startswith("Excluir")]
    assert len(resultado["avisos"]) == 2
    assert all("FOI CRIADO" in a for a in resultado["avisos"])
    assert all("em aberto" in a for a in resultado["avisos"])


def test_titulo_aceito_sem_numero_de_volta_e_tratado_como_falha():
    """Sem o número não dá para desfazer nem para conferir. Dizer "gravado"
    nessa situação seria mentir com cara de sucesso."""
    class SemNumero(OmieFalso):
        def _call(self, url, call, param):
            self.chamadas.append((call, param))
            if call.startswith("Incluir"):
                return {"codigo_status": "0"}
            return {}

    resultado = aportes_omie.gravar(planejar(), "Marcelo", cliente=SemNumero())
    assert resultado["ok"] is False
    assert "número" in resultado["erro"]


# ---------------------------------------------------------------------------
# A SENHA DE ESCRITA
# ---------------------------------------------------------------------------
def test_sem_a_variavel_configurada_nada_grava(monkeypatch):
    monkeypatch.delenv(aportes_omie.VARIAVEL_SENHA, raising=False)
    assert aportes_omie.senha_configurada() is False
    with pytest.raises(aportes_omie.SemAutorizacao) as erro:
        aportes_omie.conferir_senha("qualquer")
    assert "Render" in str(erro.value)


def test_senha_errada_e_recusada(monkeypatch):
    monkeypatch.setenv(aportes_omie.VARIAVEL_SENHA, "a-senha-certa")
    with pytest.raises(aportes_omie.SemAutorizacao):
        aportes_omie.conferir_senha("a-senha-errada")
    aportes_omie.conferir_senha("a-senha-certa")      # não levanta


# ---------------------------------------------------------------------------
# LANÇAMENTO EM LOTE — 20/09/2026
# ---------------------------------------------------------------------------
# *"Quero poder fazer vários lançamentos do mesmo tipo. Apenas incluir mais
# datas e valores. Lançamento em lote."*
# ---------------------------------------------------------------------------
def lote(parcelas, **mudancas):
    base = dict(
        operacao="aporte_bws", conta_origem=7011, conta_destino=22069,
        fornecedor=99, fornecedor_nome="PARCEIRO LTDA", obra="OBRA-1",
        obra_nome="Obra Um", quem="Marcelo", descricoes=DESCRICOES,
        categorias=CATEGORIAS)
    base.update(mudancas)
    return aportes.planejar_lote(parcelas=parcelas, **base)


def test_cada_linha_vira_um_lancamento_com_o_resto_igual():
    planos = lote([{"data": "2026-09-20", "valor": "100,00"},
                   {"data": "2026-10-20", "valor": "200,00"},
                   {"data": "2026-11-20", "valor": "300,00"}])
    assert [p["valor"] for p in planos] == [100.0, 200.0, 300.0]
    assert [p["data_br"] for p in planos] == \
        ["20/09/2026", "20/10/2026", "20/11/2026"]
    # O que NÃO varia: operação, contas, categorias, fornecedor e obra.
    for p in planos:
        assert p["operacao"] == "aporte_bws"
        assert [t["id_conta_corrente"] for t in p["titulos"]] == [7011, 22069]
        assert [t["categoria_nome"] for t in p["titulos"]] == \
            ["Aportes BWS", "Aporte BWS"]
        assert all(t["cod_departamento"] == "OBRA-1" for t in p["titulos"])


def test_cada_linha_tem_numero_e_grupo_proprios():
    """⚠️ Um número comum a todas faria os pares de lançamentos DIFERENTES
    parecerem o mesmo par — e o número é justamente o que serve para achar a
    contrapartida de um título."""
    planos = lote([{"data": "2026-09-20", "valor": "100,00"},
                   {"data": "2026-10-20", "valor": "200,00"}])
    assert len({p["grupo"] for p in planos}) == 2
    assert len({p["numero_documento"] for p in planos}) == 2
    # E DENTRO de cada lançamento o número continua sendo o mesmo dos dois
    # lados: é isso que amarra o par.
    for p in planos:
        assert len({t["numero_documento"] for t in p["titulos"]}) == 1


def test_uma_linha_so_e_o_caminho_de_sempre():
    planos = lote([{"data": "2026-09-20", "valor": "12.500,00"}])
    assert len(planos) == 1
    assert planos[0]["valor"] == 12500.0


def test_linha_repetida_e_recusada_em_vez_de_avisada():
    """Duas vezes a mesma data e o mesmo valor quase sempre é linha duplicada
    sem querer — e dois aportes iguais no mesmo dia é o erro caro deste
    recurso. Aqui o certo é ele apagar a linha, não confirmar."""
    with pytest.raises(aportes.ErroDeRegra) as erro:
        lote([{"data": "2026-09-20", "valor": "100,00"},
              {"data": "2026-09-20", "valor": "100,00"}])
    assert "repetida" in str(erro.value)


def test_mesma_data_com_valores_diferentes_passa():
    planos = lote([{"data": "2026-09-20", "valor": "100,00"},
                   {"data": "2026-09-20", "valor": "250,00"}])
    assert len(planos) == 2


def test_a_linha_ruim_e_apontada_pelo_numero():
    """Numa lista de vinte linhas, "não entendi o valor" sem dizer onde é uma
    caça ao tesouro."""
    with pytest.raises(aportes.ErroDeRegra) as erro:
        lote([{"data": "2026-09-20", "valor": "100,00"},
              {"data": "2026-10-20", "valor": "abacaxi"}])
    assert "Linha 2" in str(erro.value)


def test_lote_vazio_e_lote_grande_demais_sao_recusados():
    with pytest.raises(aportes.ErroDeRegra):
        aportes.ler_parcelas([])
    demais = [{"data": "2026-09-20", "valor": str(i + 1)}
              for i in range(aportes.MAX_PARCELAS + 1)]
    with pytest.raises(aportes.ErroDeRegra) as erro:
        aportes.ler_parcelas(demais)
    assert str(aportes.MAX_PARCELAS) in str(erro.value)


def test_no_lote_uma_linha_que_falha_nao_para_as_outras():
    """⚠️ A DIFERENÇA ENTRE O LOTE E O LANÇAMENTO ÚNICO, e ela é deliberada.

    DENTRO de um lançamento os dois títulos são amarrados: ou os dois entram,
    ou o que entrou é desfeito. ENTRE lançamentos é o contrário — parar na
    terceira linha deixaria as outras por fazer sem motivo nenhum."""
    planos = lote([{"data": "2026-09-20", "valor": "100,00"},
                   {"data": "2026-10-20", "valor": "200,00"},
                   {"data": "2026-11-20", "valor": "300,00"}])

    cli = OmieFalso()
    resultados = aportes_omie.gravar_varios(planos, "Marcelo", cliente=cli)
    assert len(resultados) == 3
    assert all(r["ok"] for r in resultados)
    # Um cliente só para o lote inteiro: é ele que carrega o controle de
    # excesso de chamadas do OMIE.
    assert len([c for c, _ in cli.chamadas if c.startswith("Incluir")]) == 6


def test_o_lote_continua_depois_de_uma_linha_que_estoura():
    planos = lote([{"data": "2026-09-20", "valor": "100,00"},
                   {"data": "2026-10-20", "valor": "200,00"}])

    class MorreNoPrimeiro:
        def __init__(self):
            self.chamadas = 0

        def _call(self, url, call, param):
            self.chamadas += 1
            if self.chamadas == 1:
                raise RuntimeError("a rede caiu")
            return {"codigo_lancamento_omie": 900 + self.chamadas}

    resultados = aportes_omie.gravar_varios(planos, "Marcelo",
                                            cliente=MorreNoPrimeiro())
    assert resultados[0]["ok"] is False
    assert resultados[1]["ok"] is True, "a segunda linha não chegou a ser feita"
