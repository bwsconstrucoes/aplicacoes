# -*- coding: utf-8 -*-
"""Análise de SPs — a conciliação fiscal.

POR QUE ESTE ARQUIVO É DETALHADO. Casar a nota errada com a SP errada vira
dedução indevida, e o erro NÃO aparece na tela: aparece na contabilidade,
meses depois. É o tipo de defeito que passa despercebido por muito tempo,
então a regra fica presa aqui, caso a caso.
"""
from __future__ import annotations

import pytest

from app.apps.analisesps import fiscal


# A chave de acesso tem posição fixa: 2 dígitos de UF, 4 de ano/mês, 14 do CNPJ
# do emitente, e o resto. Estas são montadas para os testes, mas com a mesma
# estrutura das de verdade.
def chave(cnpj_emitente: str, resto: str = "550010000123456789012345") -> str:
    montada = "26" + "2609" + fiscal.so_digitos(cnpj_emitente) + resto
    montada = (montada + "0" * 44)[:44]
    assert len(montada) == 44
    return montada


CREDOR = "29066773000152"          # o fornecedor: quem emite
BWS = "10656452007869"             # a BWS: quem recebe, em TODAS as notas


def nota(**campos):
    base = {"chave": chave(CREDOR), "emitente_doc": CREDOR,
            "emitente": "SERTAO CASA E CONSTRUCAO",
            "destinatario_doc": BWS, "destinatario": "BWS CONSTRUCOES LTDA",
            "numero": "1430", "valor": "269,00", "status": "Autorizada",
            "emissao": "18/06/2026"}
    base.update(campos)
    return base


def sp(**campos):
    base = {"id": "1409289353", "credor": "SERTAO CASA E CONSTRUCAO",
            "documento": "29.066.773/0001-52", "valor": "269,00",
            "nf": "1430", "vencimento": "10/07/2026",
            "tipo_despesa": "Ferramentas"}
    base.update(campos)
    return base


# ---------------------------------------------------------------------------
# O ERRO DE CONCEITO QUE ESTE MÓDULO CORRIGE
# ---------------------------------------------------------------------------
def test_o_credor_do_lancamento_e_quem_EMITIU_a_nota():
    """É O TESTE MAIS IMPORTANTE DO ARQUIVO.

    A análise que rodava na planilha até 11/09/2026 dava 30 pontos quando o
    CNPJ do credor batia com o DESTINATÁRIO, e 8 quando batia com o EMITENTE.
    Está de cabeça para baixo: o relatório é de notas emitidas CONTRA a BWS,
    então o destinatário é a BWS em todas as linhas — não distingue nada."""
    pontos, porques = fiscal.pontuar(sp(), nota())
    assert pontos >= fiscal.PONTOS_EMITENTE
    assert any("emitiu" in p for p in porques)


def test_bater_com_o_destinatario_nao_vale_ponto_nenhum():
    """O destinatário é a BWS em TODAS as notas. Pontuar por ele seria dar a
    mesma nota a todas as candidatas — ruído puro."""
    lancamento = sp(documento=BWS, credor="BWS CONSTRUCOES LTDA", nf="",
                    valor="0,01", vencimento="")
    pontos, _ = fiscal.pontuar(lancamento, nota())
    assert pontos == 0


def test_o_cnpj_de_quem_emitiu_e_lido_de_dentro_da_chave():
    """Os dígitos 7 a 20 da chave SÃO o CNPJ do emitente, por definição da
    Receita. Ler dali salva a conciliação quando a coluna do relatório vem
    suja ou vazia."""
    assert fiscal.emitente_da_chave(chave(CREDOR)) == CREDOR
    sem_coluna = nota(emitente_doc="", emitente="")
    pontos, porques = fiscal.pontuar(sp(), sem_coluna)
    assert any("emitiu" in p for p in porques), "não leu o CNPJ de dentro da chave"


def test_chave_com_tamanho_errado_nao_inventa_emitente():
    assert fiscal.emitente_da_chave("123") == ""
    assert fiscal.emitente_da_chave("") == ""


# ---------------------------------------------------------------------------
# MATRIZ E FILIAL
# ---------------------------------------------------------------------------
def test_a_filial_que_entregou_casa_com_a_matriz_cadastrada():
    """A nota sai da filial; o cadastro do credor quase sempre tem a matriz.
    Exigir os catorze dígitos perderia justamente os casos comuns."""
    assert fiscal.mesmo_documento("29.066.773/0001-52", "29066773000899")


def test_empresas_diferentes_nao_casam():
    assert not fiscal.mesmo_documento("29066773000152", "54063528000724")


def test_cpf_casa_inteiro_e_nao_pela_raiz():
    """CPF não tem matriz e filial. Comparar só o começo casaria pessoas
    diferentes."""
    assert fiscal.mesmo_documento("879.993.713-00", "87999371300")
    assert not fiscal.mesmo_documento("87999371300", "87999371399")


# ---------------------------------------------------------------------------
# O NÚMERO DA NOTA
# ---------------------------------------------------------------------------
def test_o_numero_ignora_zeros_a_esquerda():
    assert fiscal.mesmo_numero_de_nota("000123", "123")
    assert fiscal.mesmo_numero_de_nota("1.430", "1430")


def test_numero_vazio_nunca_casa():
    """Senão dois lançamentos sem número casariam entre si."""
    assert not fiscal.mesmo_numero_de_nota("", "")
    assert not fiscal.mesmo_numero_de_nota("", "123")


# ---------------------------------------------------------------------------
# A ESCOLHA ENTRE CANDIDATAS
# ---------------------------------------------------------------------------
def test_nao_propoe_quando_duas_notas_estao_empatadas():
    """Duas notas do mesmo fornecedor no mesmo dia, com valores próximos.
    Escolher uma no par ou ímpar é o erro que este módulo existe para não
    cometer: a tela diz que há dúvida."""
    a = nota(chave=chave(CREDOR, "550010000000000000000001"), numero="1430")
    b = nota(chave=chave(CREDOR, "550010000000000000000002"), numero="1431")
    escolha = fiscal.melhor_nota(sp(nf=""), [a, b])
    assert escolha["empate"] is True
    assert escolha["propoe"] is False
    assert any("confira" in p for p in escolha["porques"])


def test_propoe_quando_a_melhor_esta_sozinha_na_frente():
    certa = nota()
    outra = nota(chave=chave("11222333000181"), emitente_doc="11222333000181",
                 emitente="OUTRA EMPRESA", numero="99", valor="10.000,00")
    escolha = fiscal.melhor_nota(sp(), [certa, outra])
    assert escolha["propoe"] is True
    assert escolha["nota"]["chave"] == certa["chave"]
    assert escolha["pontos"] >= fiscal.CONFIANCA_PARA_PROPOR


def test_sem_candidata_nenhuma_responde_que_procurou():
    """"Procurei e não achei" é diferente de "não procurei", e a tela precisa
    poder dizer os dois."""
    escolha = fiscal.melhor_nota(sp(), [])
    assert escolha["nota"] is None and escolha["propoe"] is False


def test_a_nota_cancelada_aparece_com_o_aviso_em_vez_de_sumir():
    """Se ela É a nota daquele lançamento, quem analisa PRECISA ver — pagar
    com nota cancelada é problema fiscal. Esconder seria pior."""
    escolha = fiscal.melhor_nota(sp(), [nota(status="Cancelada")])
    assert escolha["nota"] is not None
    assert any("CANCELADA" in p for p in escolha["porques"])


# ---------------------------------------------------------------------------
# A DATA
# ---------------------------------------------------------------------------
def test_a_data_confirma_mas_nao_escolhe_sozinha():
    """Vale poucos pontos de propósito: em qualquer mês há dezenas de notas
    com data compatível."""
    lancamento = sp(documento="", credor="", nf="", valor="")
    pontos, _ = fiscal.pontuar(lancamento, nota())
    assert 0 < pontos < fiscal.CONFIANCA_PARA_PROPOR


def test_nota_emitida_muito_depois_do_vencimento_nao_ganha_ponto_de_data():
    lancamento = sp(documento="", credor="", nf="", valor="",
                    vencimento="01/01/2026")
    pontos, _ = fiscal.pontuar(lancamento, nota(emissao="31/12/2026"))
    assert pontos == 0


# ---------------------------------------------------------------------------
# A DEDUTIBILIDADE — a tabela é do dono
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("categoria", sorted(fiscal.NAO_DEDUTIVEIS))
def test_as_cinco_que_nao_dao_deducao(categoria):
    assert fiscal.dedutivel(categoria) is False


@pytest.mark.parametrize("categoria", [
    "NF-e (Mercadoria)", "NFS-e (Serviço)", "CT-e (Frete)", "Seguros",
    "Contrato", "Fundo Fixo", "Presente",
    # As três que faltavam na tabela da planilha e o dono respondeu em 11/09:
    "BeeVale", "Férias ou PL", "Rescisões (TRCT e Multa)",
])
def test_as_que_dao_deducao(categoria):
    assert fiscal.dedutivel(categoria) is True


def test_todas_as_opcoes_do_pipefy_tem_dedutibilidade_definida():
    """Uma opção sem resposta viraria uma SP que ninguém sabe classificar."""
    assert len(fiscal.CATEGORIAS) == 22
    for c in fiscal.CATEGORIAS:
        assert isinstance(fiscal.dedutivel(c), bool)
    assert fiscal.NAO_DEDUTIVEIS <= set(fiscal.CATEGORIAS)


# ---------------------------------------------------------------------------
# A CATEGORIA SUGERIDA PELO TIPO DE DESPESA — e o papel dela é PEQUENO
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("tipo,esperado", [
    ("Veículos (Taxas, Impostos, Multas)", "Seguros"),
    ("Água e Energia", "Nota de Débito/Fatura"),
    ("Locação de Equipamentos", "Nota de Débito/Fatura"),
    ("Internet, Telefonia e Sistemas", "Nota de Débito/Fatura"),
    ("Aluguéis e Condomínios", "Contrato"),
    ("Multas e Processos Trabalhistas", "Rescisões (TRCT e Multa)"),
    ("Cartórios, Crea, Taxas", "Taxas Diversas"),
])
def test_so_o_que_NUNCA_tem_nota_eletronica_ganha_sugestao(tipo, esperado):
    """Aluguel tem contrato, veículo tem apólice, água tem fatura. Para essas
    despesas não existe nota eletrônica para procurar, então o tipo de despesa
    é a única pista que sobra — e aí ela vale."""
    assert fiscal.categoria_sugerida(tipo) == esperado


@pytest.mark.parametrize("tipo", [
    "Material Elétrico", "Parafusos, Ferragens e Acessórios", "Ferramentas",
    "Material Hidráulico",
])
def test_despesa_de_mercadoria_NAO_ganha_sugestao_por_palavra(tipo):
    """A REGRA QUE O DONO DERRUBOU EM 11/09/2026, com todas as letras:

        "Categoria de despesa não vai ser regra para dedutibilidade ou não,
         porque você pode comprar um material elétrico SEM nota fiscal. Então
         nesse caso vai ser não dedutível. O FATO DE TER A NOTA FISCAL é que
         vai ser o balizador. A simples divergência de material elétrico nem
         adianta mostrar."

    Sugerir "NF-e" para uma compra de material feita sem nota seria propor
    dedução de despesa que não dá dedução — o erro exato que ele apontou.
    Material elétrico só vira NF-e quando a NOTA é encontrada, e aí a
    categoria sai de dentro da chave, não de palpite por palavra."""
    assert fiscal.categoria_sugerida(tipo) == ""


def test_tipo_de_despesa_desconhecido_nao_sugere_nada():
    """Chutar uma categoria é pior do que deixar em branco: o chute entra no
    card como se fosse decisão."""
    assert fiscal.categoria_sugerida("Alguma Coisa Nova") == ""
    assert fiscal.categoria_sugerida("") == ""


# ---------------------------------------------------------------------------
# A CATEGORIA LIDA DE DENTRO DA CHAVE
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("modelo,esperado", [
    ("55", "NF-e (Mercadoria)"),
    ("57", "CT-e (Frete)"),
    ("65", "NFC-e (Cupom Fiscal eletrônico)"),
])
def test_o_modelo_do_documento_esta_nos_digitos_21_e_22(modelo, esperado):
    """Os dígitos 21 e 22 da chave SÃO o modelo, por definição da Receita.
    Conferido nas chaves reais da planilha do dono: as que ele classificou à
    mão como frete têm 57 ali, e as de mercadoria têm 55.

    É isso que faz a categoria proposta ser CERTEZA e não palpite."""
    assert fiscal.categoria_da_chave(
        chave(CREDOR, modelo + "0010000123456789012345")) == esperado


def test_chave_que_nao_tem_44_digitos_nao_diz_categoria_nenhuma():
    """Meia chave não é chave. Ler modelo de uma chave truncada devolveria
    dois dígitos quaisquer do meio do número."""
    assert fiscal.categoria_da_chave("123") == ""
    assert fiscal.categoria_da_chave("") == ""


def test_modelo_desconhecido_nao_inventa_categoria():
    assert fiscal.categoria_da_chave(
        chave(CREDOR, "990010000123456789012345")) == ""


def test_seguros_e_contrato_nao_entram_na_conciliacao():
    """Uma apólice e um contrato de aluguel não têm nota eletrônica para
    casar. Procurar par para elas só produziria ruído."""
    assert "Seguros" in fiscal.NAO_CONCILIA
    assert "Contrato" in fiscal.NAO_CONCILIA
    assert "Fundo Fixo" in fiscal.NAO_CONCILIA
    # E as que PRECISAM ser conciliadas continuam de fora dessa lista.
    for c in ("NF-e (Mercadoria)", "NFS-e (Serviço)", "CT-e (Frete)",
              "Emissão Futura", "Ausente", "Reanalisar"):
        assert c not in fiscal.NAO_CONCILIA


# ---------------------------------------------------------------------------
# A IMPORTAÇÃO DO RELATÓRIO DO FSIST
#
# O relatório muda de layout entre NF-e e CT-e (num é "Destinatário", noutro é
# "Tomador"), tem uma linha de título acima do cabeçalho e um totalizador no
# rodapé. Cada um destes testes é um desses fatos.
# ---------------------------------------------------------------------------
class _Aba:
    def __init__(self, valores):
        self._v = valores

    def get_all_values(self):
        return self._v


CABECALHO_NFE = ["Emissão", "Chave", "Número", "Série", "Tipo", "Valor",
                 "Status", "Emitente CNPJ", "Emitente", "Emitente IE",
                 "Emitente UF", "Destinatário CNPJ/CPF", "Destinatário",
                 "Destinatário IE", "Destinatário UF", "Chaves NFE Tranporte"]

CABECALHO_CTE = ["Emissão", "Chave", "Número", "Série", "Tipo", "Valor",
                 "Status", "Emitente CNPJ", "Emitente", "Emitente IE",
                 "Emitente UF", "Tomador CNPJ/CPF", "Tomador",
                 "Tomador IE", "Tomador UF", "NFe Chaves"]


def _linha_de_nota(chave_, numero="1430", emitente_doc="29.066.773/0001-52",
                   valor="R$ 269,00", status="Autorizada"):
    return ["18/06/2026", chave_, numero, "1", "Normal", valor, status,
            emitente_doc, "SERTAO CASA E CONSTRUCAO", "", "PE",
            "10.656.452/0078-69", "BWS CONSTRUCOES LTDA", "", "PE", ""]


def _importar(monkeypatch, linhas, gravou=None):
    from app.apps.analisesps import db, sincronizacao
    monkeypatch.setattr(sincronizacao, "com_retry", lambda f: f())
    monkeypatch.setattr(sincronizacao, "_aba",
                        lambda pid, nome: _Aba(linhas))
    monkeypatch.setattr(sincronizacao, "_abas_existentes", lambda pid: ["Relatório FSIST"])

    guardadas = [] if gravou is None else gravou

    class ConexaoFalsa:
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def execute(self, sql, params=()):
            class Cur:
                def fetchone(self_): return (len(guardadas),)
                def close(self_): pass
            return Cur()
        def executemany(self, sql, seq): guardadas.extend(seq)
        def commit(self): pass

    monkeypatch.setattr(db, "conexao", lambda: ConexaoFalsa())
    return sincronizacao.sincronizar_notas_fiscais(), guardadas


def test_o_cabecalho_nao_esta_na_primeira_linha(monkeypatch):
    """Na planilha do dono a linha 1 é o título "Relatório de Notas de
    Compras" e a 2 é o cabeçalho. Procurar a linha que TEM a coluna "Chave" é
    mais robusto do que fixar o número — o dia em que alguém inserir uma linha
    acima, nada quebra."""
    titulo = ["", "", "Relatório de Notas de Compras"] + [""] * 13
    _, gravadas = _importar(monkeypatch, [
        titulo, CABECALHO_NFE, _linha_de_nota(chave(CREDOR))])
    assert len(gravadas) == 1


def test_o_relatorio_de_frete_chama_a_BWS_de_TOMADOR(monkeypatch):
    """No CT-e quem paga o frete é o "Tomador"; na NF-e é o "Destinatário".
    São a mesma coisa para nós, e o layout muda entre os dois relatórios."""
    resposta, gravadas = _importar(monkeypatch, [
        CABECALHO_CTE, _linha_de_nota(chave(CREDOR))])
    assert len(gravadas) == 1
    assert not resposta["avisos"], resposta["avisos"]


def test_o_totalizador_do_rodape_nao_vira_nota(monkeypatch):
    """Linha sem chave não é nota — é rodapé, linha em branco ou soma."""
    resposta, gravadas = _importar(monkeypatch, [
        CABECALHO_NFE, _linha_de_nota(chave(CREDOR)),
        ["", "", "", "", "", "TOTAL", "", "", "", "", "", "", "", "", "", ""]])
    assert len(gravadas) == 1 and resposta["ignoradas"] == 1


def test_o_cnpj_do_emitente_e_lido_da_chave_quando_a_coluna_vem_vazia(monkeypatch):
    """Acontece no relatório de verdade. Sem isto a nota entraria sem o
    emitente, e o emitente é o sinal mais forte da conciliação."""
    linha = _linha_de_nota(chave(CREDOR), emitente_doc="")
    _, gravadas = _importar(monkeypatch, [CABECALHO_NFE, linha])
    assert gravadas[0][7] == CREDOR, "não leu o CNPJ de dentro da chave"


def test_chave_com_tamanho_errado_e_ignorada(monkeypatch):
    """44 dígitos ou não é chave. Deixar entrar meia chave envenenaria a
    conciliação inteira."""
    linha = _linha_de_nota("123456")
    resposta, gravadas = _importar(monkeypatch, [CABECALHO_NFE, linha])
    assert gravadas == [] and resposta["ignoradas"] == 1


def test_aba_sem_a_coluna_chave_diz_o_que_encontrou(monkeypatch):
    """Mesma regra do botão das planilhas de apoio: falhar calado numa
    importação é o que faz a pessoa apertar o botão de novo sem entender."""
    resposta, _ = _importar(monkeypatch, [
        ["Data", "Documento", "Valor"], ["01/01/2026", "x", "1"]])
    assert resposta["novas"] == 0
    aviso = " ".join(resposta["avisos"])
    assert "Chave" in aviso and "Documento" in aviso


def test_so_grava_a_nota_que_mudou():
    """Mesmo motivo que valeu 14,3 milhões de gravações inúteis em 10/09:
    regravar com o mesmo valor deixa lixo que engorda a tabela."""
    from pathlib import Path
    fonte = Path("app/apps/analisesps/sincronizacao.py").read_text(encoding="utf-8")
    trecho = fonte.split("def sincronizar_notas_fiscais")[1].split("\ndef ")[0]
    assert "IS DISTINCT FROM" in trecho
    assert "notas_fiscais.status IS DISTINCT FROM" in trecho


# ---------------------------------------------------------------------------
# O PASSADO: semear o diário com o que já está nos cards
#
# Um terço dos lançamentos já tem chave e categoria, preenchidos à mão ao longo
# dos meses, e isso SÓ existe no card. O relatório do Pipefy é lido UMA VEZ
# para o diário nascer sabendo — decisão do dono em 11/09/2026.
# ---------------------------------------------------------------------------
CABECALHO_LANCAMENTOS = [
    "ID SP", "Documentação Fiscal", "Fase atual", "Nome do Credor",
    "Tipo de Despesa", "Valor Total da Despesa", "Data de Vencimento",
    "Data do Pagamento", "Banco do Pagamento", "Nº da Nota Fiscal",
    "Chave de Acesso", "Responsável pela Solicitação", "Tipo de Pagamento",
    "Etiquetas", "Centro de Custo", "CPF/CNPJ Credor", "Dt Emissão", "Link"]


def _lancamento(sp_id, documentacao="", numero="", chave_=""):
    linha = [""] * 18
    linha[0], linha[1], linha[9], linha[10] = sp_id, documentacao, numero, chave_
    return linha


def _semear(monkeypatch, linhas, ja_semeado="", forcar=False):
    from app.apps.analisesps import db, sincronizacao
    monkeypatch.setattr(sincronizacao, "com_retry", lambda f: f())
    monkeypatch.setattr(sincronizacao, "_aba", lambda pid, nome: _Aba(linhas))
    monkeypatch.setattr(sincronizacao, "_abas_existentes", lambda pid: [])
    monkeypatch.setattr(sincronizacao, "_meta_ler",
                        lambda conn, chave_, padrao="": ja_semeado)
    monkeypatch.setattr(sincronizacao, "_meta_gravar",
                        lambda conn, chave_, valor: None)

    guardadas: list = []

    class ConexaoFalsa:
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def execute(self, sql, params=()):
            class Cur:
                def fetchone(self_): return (len(guardadas),)
                def close(self_): pass
            return Cur()
        def executemany(self, sql, seq):
            assert "DO NOTHING" in sql, (
                "a semeadura NUNCA pode sobrescrever decisão desta tela")
            guardadas.extend(seq)
        def commit(self): pass

    monkeypatch.setattr(db, "conexao", lambda: ConexaoFalsa())
    return sincronizacao.semear_analise_do_pipefy(forcar=forcar), guardadas


def test_a_semeadura_roda_uma_vez_so(monkeypatch):
    """"Não precisa ficar toda hora baixando, já que está gravando a
    informação complementar no outro canto." — o dono, em 11/09/2026."""
    resposta, gravadas = _semear(monkeypatch, [
        CABECALHO_LANCAMENTOS, _lancamento("123", "Seguros")],
        ja_semeado="2026-09-11T10:00:00")
    assert resposta["pulada"] is True and gravadas == []


def test_a_semeadura_traz_categoria_chave_e_dedutibilidade(monkeypatch):
    _, gravadas = _semear(monkeypatch, [
        CABECALHO_LANCAMENTOS,
        _lancamento("1435291289", "NF-e (Mercadoria)", "121", chave(CREDOR))])
    assert len(gravadas) == 1
    sp_id, situacao, documentacao, chave_gravada = gravadas[0][:4]
    assert sp_id == "1435291289"
    assert situacao == "ESCRITA", "o valor ESTÁ no card; não é pendência"
    assert documentacao == "NF-e (Mercadoria)"
    assert chave_gravada == chave(CREDOR)
    assert gravadas[0][6] is True, "NF-e é dedutível"


def test_a_semeadura_calcula_a_dedutibilidade_pela_tabela_do_dono(monkeypatch):
    _, gravadas = _semear(monkeypatch, [
        CABECALHO_LANCAMENTOS, _lancamento("1", "Não Dedutível"),
        _lancamento("2", "Seguros")])
    por_sp = {g[0]: g[6] for g in gravadas}
    assert por_sp["1"] is False and por_sp["2"] is True


def test_lancamento_sem_categoria_e_sem_chave_nao_e_semeado(monkeypatch):
    """Essa SP entra na fila normal. Semeá-la como "pendente" só encheria a
    tabela com o que já se sabe pela ausência."""
    _, gravadas = _semear(monkeypatch, [
        CABECALHO_LANCAMENTOS, _lancamento("1"), _lancamento("2", "Seguros")])
    assert [g[0] for g in gravadas] == ["2"]


def test_chave_pela_metade_nao_e_semeada_como_chave(monkeypatch):
    """44 dígitos ou não é chave. Uma meia chave no diário envenenaria a
    conciliação e ainda pareceria decidida."""
    _, gravadas = _semear(monkeypatch, [
        CABECALHO_LANCAMENTOS, _lancamento("1", "NF-e (Mercadoria)", "9", "12345")])
    assert gravadas[0][3] == ""


def test_a_semeadura_nunca_sobrescreve_o_que_esta_tela_decidiu(monkeypatch):
    """A verificação está dentro do dublê: o comando TEM de ser
    `ON CONFLICT DO NOTHING`. História velha não manda em decisão nova, e
    confiar em quem lembra da regra é como a regra se perde."""
    _, gravadas = _semear(monkeypatch, [
        CABECALHO_LANCAMENTOS, _lancamento("1", "Seguros")], forcar=True)
    assert len(gravadas) == 1


def test_aba_de_lancamentos_sem_as_colunas_diz_o_que_encontrou(monkeypatch):
    resposta, gravadas = _semear(monkeypatch, [
        ["Data", "Documento"], ["01/01/2026", "x"]])
    assert gravadas == []
    aviso = " ".join(resposta["avisos"])
    assert "ID SP" in aviso and "Documento" in aviso


# ---------------------------------------------------------------------------
# AS CRÍTICAS: o que a tela aponta, o que ela PROPÕE, e o que ela CALA
#
# As duas pilhas existem por causa de um risco real: se o sistema propõe e
# quase tudo está certo, em três semanas ninguém confere mais — é o mesmo olho
# cansado, só que mais rápido. Por isso `propoe` só é verdadeiro quando não há
# dúvida nenhuma, e cada teste daqui prende UM caso.
# ---------------------------------------------------------------------------
def escolha(nota_achada=None, pontos=0, propoe=False, porques=None):
    """O que `melhor_nota` devolveria, montado à mão para isolar `avaliar`."""
    return {"nota": nota_achada, "pontos": pontos, "propoe": propoe,
            "porques": porques or ["o CNPJ do credor é o de quem emitiu a nota"]}


def test_nota_do_card_cancelada_e_o_achado_mais_grave():
    """Despesa paga contra documento que não existe mais. Nunca é proposta:
    o que fazer com isso é decisão de gente, não correção automática."""
    a_nota = nota(status="Cancelada")
    r = fiscal.avaliar(
        sp(status_pgt="Pago"),
        {"documentacao": "NF-e (Mercadoria)", "chave": a_nota["chave"]},
        escolha(a_nota, 85, True))
    assert r["grupo"] == fiscal.CRITICO
    assert r["propoe"] is False, "cancelamento não se resolve marcando um card"
    assert "CANCELADA" in r["motivo"] and "paga" in r["motivo"]


def test_nota_cancelada_em_despesa_ainda_nao_paga_tambem_e_critica():
    a_nota = nota(status="Cancelada")
    r = fiscal.avaliar(sp(status_pgt="Em aberto"),
                       {"documentacao": "NF-e (Mercadoria)",
                        "chave": a_nota["chave"]},
                       escolha(a_nota, 85, True))
    assert r["grupo"] == fiscal.CRITICO
    assert "paga" not in r["motivo"], "não afirmar pagamento que não houve"


def test_a_nota_que_ja_esta_no_card_e_confere_nao_vira_tarefa():
    """O grosso da base está certo. Encher a tela com o que já está em ordem
    é o jeito mais rápido de fazer a pessoa parar de olhar a tela."""
    a_nota = nota()
    r = fiscal.avaliar(sp(), {"documentacao": "NF-e (Mercadoria)",
                             "chave": a_nota["chave"]},
                       escolha(a_nota, 90, True))
    assert r["grupo"] == fiscal.EM_DIA and r["propoe"] is False


def test_chave_do_card_diferente_da_nota_que_combina_levanta_a_troca():
    """O erro que o dono descreveu com essas palavras: "colocar uma nota de um
    registro para outro". Duas SPs do mesmo fornecedor, os anexos trocados."""
    outra = nota(chave=chave(CREDOR, "550010000999888777666555"))
    r = fiscal.avaliar(sp(), {"documentacao": "NF-e (Mercadoria)",
                             "chave": nota()["chave"]},
                       escolha(outra, 85, True))
    assert r["grupo"] == fiscal.DUVIDA
    assert r["propoe"] is False, "troca de nota se confere a olho, não em lote"
    assert "trocadas" in r["motivo"]


def test_achei_a_nota_e_o_card_diz_que_nao_ha_nota_e_a_correcao_que_vale():
    """Exatamente o caso que ele descreveu: "colocado algo não dedutível de uma
    coisa que não foi localizada naquele momento, mas que depois ela surge"."""
    r = fiscal.avaliar(sp(), {"documentacao": "Não Dedutível"},
                       escolha(nota(), 85, True))
    assert r["grupo"] == fiscal.CORRECAO
    assert r["propoe"] is True
    assert r["documentacao"] == "NF-e (Mercadoria)"
    assert r["chave"] == nota()["chave"]


def test_a_categoria_proposta_sai_de_dentro_da_chave_nao_de_palpite():
    """Um frete achado vira CT-e porque os dígitos 21-22 da chave dizem 57 —
    não porque a palavra "frete" apareceu em algum lugar."""
    frete = nota(chave=chave(CREDOR, "570010000123456789012345"))
    r = fiscal.avaliar(sp(tipo_despesa="Material Elétrico"),
                       {"documentacao": "Ausente"}, escolha(frete, 85, True))
    assert r["documentacao"] == "CT-e (Frete)"


@pytest.mark.parametrize("hoje", [
    "Ausente", "Não Dedutível", "Reanalisar", "Emissão Futura",
    "Aguardando Nota (Ilegível)", "Aguardando Nota (Não Anexada)", "",
])
def test_a_revarredura_alcanca_todas_as_categorias_de_ausencia(hoje):
    """"Emissão Futura" e "Não Dedutível" são justamente as que precisam ser
    revistas quando o relatório novo do FSist chega — a nota que faltava em
    julho pode estar no relatório de setembro."""
    r = fiscal.avaliar(sp(), {"documentacao": hoje}, escolha(nota(), 85, True))
    assert r["grupo"] == fiscal.CORRECAO and r["propoe"] is True


def test_nota_encontrada_sem_confianca_nao_e_proposta():
    """Abaixo da linha de confiança a candidata aparece, mas desmarcada. É a
    segunda pilha: decidida uma a uma, e não no lote."""
    r = fiscal.avaliar(sp(), {"documentacao": "Ausente"},
                       escolha(nota(), 35, False))
    assert r["grupo"] == fiscal.DUVIDA
    assert r["propoe"] is False
    assert r["documentacao"] == "NF-e (Mercadoria)", (
        "mesmo sem propor, mostrar o que seria — a pessoa decide olhando")
    assert "certeza" in r["motivo"]


def test_card_afirma_nota_eletronica_sem_chave_e_sem_nota_encontrada():
    """Alguém classificou como NF-e sem documento nenhum, ou a nota ainda não
    entrou no relatório. As duas hipóteses vão escritas — não se acusa."""
    r = fiscal.avaliar(sp(), {"documentacao": "NF-e (Mercadoria)"}, escolha())
    assert r["grupo"] == fiscal.DUVIDA and r["propoe"] is False
    assert "não há chave" in r["motivo"]


def test_despesa_de_mercadoria_sem_nota_NAO_vira_divergencia():
    """O TESTE QUE GUARDA A CORREÇÃO DO DONO, de 11/09/2026:

        "A simples divergência de material elétrico nem adianta mostrar."

    Material elétrico sem nota é compra sem nota — não dedutível, e correto do
    jeito que está. Se este teste começar a falhar porque alguém reintroduziu
    a regra por palavra, é a regra que está errada, não o teste."""
    r = fiscal.avaliar(sp(tipo_despesa="Material Elétrico"),
                       {"documentacao": "Não Dedutível"}, escolha())
    assert r["grupo"] == fiscal.SEM_PAR
    assert r["documentacao"] == "", "não propor NF-e para compra sem nota"
    assert r["propoe"] is False


def test_o_que_nunca_tem_nota_eletronica_ganha_sugestao_de_documento():
    """Aqui a sugestão pelo tipo de despesa continua valendo: aluguel não tem
    NF-e para procurar, tem contrato. E ela vem DESMARCADA."""
    r = fiscal.avaliar(sp(tipo_despesa="Aluguéis e Condomínios"),
                       {"documentacao": "Ausente"}, escolha())
    assert r["grupo"] == fiscal.DUVIDA
    assert r["documentacao"] == "Contrato"
    assert r["propoe"] is False


def test_sugestao_que_ja_e_o_que_esta_no_card_nao_vira_tarefa():
    r = fiscal.avaliar(sp(tipo_despesa="Aluguéis e Condomínios"),
                       {"documentacao": "Contrato"}, escolha())
    assert r["grupo"] == fiscal.SEM_PAR


def test_sem_nota_e_sem_pista_o_sistema_diz_que_procurou():
    """"Procurei e não achei" é diferente de "não procurei" — e quem lê a tela
    precisa saber qual dos dois é."""
    r = fiscal.avaliar(sp(), {}, escolha())
    assert r["grupo"] == fiscal.SEM_PAR
    assert "não encontrei" in r["motivo"]


def test_chave_no_card_emitida_por_outro_cnpj_e_apontada_sem_o_fsist():
    """Nem precisa achar a nota certa: o CNPJ de quem emitiu está DENTRO da
    chave. Se não é o do credor, a chave veio de outro lançamento."""
    r = fiscal.avaliar(sp(), {"documentacao": "NF-e (Mercadoria)",
                             "chave": chave("11222333000181")}, escolha())
    assert r["grupo"] == fiscal.DUVIDA
    assert "trocadas" in r["motivo"]


def test_chave_do_credor_certo_que_nao_veio_no_relatorio_nao_e_acusada():
    """O relatório do FSist cobre um período. Nota antiga fora dele não é
    erro de ninguém, e tratar como erro encheria a tela de falso alarme."""
    r = fiscal.avaliar(sp(), {"documentacao": "NF-e (Mercadoria)",
                             "chave": chave(CREDOR)}, escolha())
    assert r["grupo"] == fiscal.EM_DIA
    assert "FSist" in r["motivo"]


def test_a_confianca_vai_junto_para_a_tela():
    """O número que a pessoa lê para decidir se confere ou se confia."""
    r = fiscal.avaliar(sp(), {"documentacao": "Ausente"},
                       escolha(nota(), 85, True))
    assert r["confianca"] == 85


def test_avaliar_aguenta_analise_vazia_e_escolha_vazia():
    """A SP que nunca passou por aqui é a maioria da base. Estourar nela
    derrubaria a tela inteira na primeira carga."""
    r = fiscal.avaliar(sp(), None, None)
    assert r["grupo"] == fiscal.SEM_PAR and r["propoe"] is False


# ---------------------------------------------------------------------------
# A SEGUNDA VISÃO: as notas que não estão em lançamento nenhum
#
# É ela que fecha com a contabilidade. Nas palavras do dono: "se tem uma nota
# emitida, tem uma despesa para estar associada".
# ---------------------------------------------------------------------------
def test_nota_sem_lancamento_aparece():
    orfa = nota(chave=chave("11222333000181"))
    saida = fiscal.notas_sem_lancamento([nota(), orfa], {nota()["chave"]})
    assert [n["chave"] for n in saida] == [orfa["chave"]]


def test_a_chave_usada_e_comparada_so_pelos_digitos():
    """A chave vem do card digitada de mil jeitos — com espaço, com ponto. Se
    a comparação for literal, a mesma nota aparece como órfã."""
    suja = " ".join([nota()["chave"][:22], nota()["chave"][22:]])
    assert fiscal.notas_sem_lancamento([nota()], {suja}) == []


def test_nota_cancelada_sem_lancamento_nao_e_achado():
    """Nota cancelada sem despesa é o esperado, não um problema. Listá-la
    faria a segunda visão nascer cheia de ruído."""
    cancelada = nota(chave=chave("11222333000181"), status="Cancelada")
    assert fiscal.notas_sem_lancamento([cancelada], set()) == []


def test_nota_sem_chave_nao_entra_na_lista():
    """Sem chave não há como ligar a lançamento nenhum depois — listar só
    geraria uma linha que ninguém consegue resolver."""
    assert fiscal.notas_sem_lancamento([nota(chave="")], set()) == []


def test_sem_nenhuma_chave_usada_todas_as_notas_sao_orfas():
    """O estado do primeiro dia, antes de qualquer conciliação."""
    assert len(fiscal.notas_sem_lancamento([nota(), nota(
        chave=chave("11222333000181"))], set())) == 2


# ---------------------------------------------------------------------------
# OS CAMPOS DO CARD — travados aqui porque erram em SILÊNCIO
#
# Errar um identificador do Pipefy não dá erro: a chamada é aceita e nada é
# gravado. Não há como descobrir isso olhando a tela do Análise de SPs — só
# abrindo o card no Pipefy e vendo que ele continua vazio. Por isso os valores
# ficam presos aqui, conferidos contra a estrutura do pipe que o dono colou em
# 11/09/2026.
# ---------------------------------------------------------------------------
def test_os_identificadores_dos_campos_da_conciliacao():
    """Se alguém renomear o campo na tela do Pipefy, o identificador muda e a
    gravação passa a não fazer nada. O UUID ao lado de cada um, no código, é o
    que permite reencontrar o campo quando isso acontecer."""
    from app.apps.analisesps import pipefy
    assert pipefy.CAMPO_GEROU_NOTA == "a_despesa_gerou_emiss_o_de_nota_fiscal"
    assert pipefy.CAMPO_NUMERO_NOTA == "n_da_nota_fiscal"
    assert pipefy.CAMPO_CHAVE_ACESSO == "chave_de_acesso"
    assert pipefy.CAMPO_ANALISE_DEDUT == "an_lise_dedutibilidade"
    assert pipefy.CAMPO_DOC_FISCAL == "documenta_o_fiscal"


def test_as_categorias_sao_exatamente_as_opcoes_do_campo_do_pipefy():
    """O Pipefy RECUSA o card inteiro quando o texto não é uma das opções do
    campo. Uma categoria escrita com acento diferente aqui não erraria só
    aquela SP — derrubaria a gravação do lote todo.

    A lista é a do JSON do pipe, na ordem dele."""
    from app.apps.analisesps import fiscal
    assert fiscal.CATEGORIAS == [
        "NF-e (Mercadoria)", "NFS-e (Serviço)", "CT-e (Frete)",
        "NFC-e (Cupom Fiscal eletrônico)", "Guia de Tributo", "Seguros",
        "Taxas Diversas", "Contrato", "Contrato (Alterar Titularidade)",
        "BeeVale", "Nota de Débito/Fatura", "Ausente",
        "Aguardando Nota (Ilegível)", "Aguardando Nota (Não Anexada)",
        "Nota Cancelada", "Reanalisar", "Fundo Fixo", "Emissão Futura",
        "Rescisões (TRCT e Multa)", "Férias ou PL", "Presente",
        "Não Dedutível",
    ]


def test_tudo_que_o_sistema_propoe_cabe_no_campo_do_pipefy():
    """A ponta solta que este teste fecha: `avaliar` propõe uma categoria, e
    ela vai direto para o card. Se algum caminho do código produzisse um texto
    fora da lista, o Pipefy recusaria — e só se descobriria em produção."""
    from app.apps.analisesps import fiscal
    permitidas = set(fiscal.CATEGORIAS)
    for modelo in fiscal.MODELOS.values():
        assert modelo in permitidas, modelo
    for sugerida in fiscal.CATEGORIA_SEM_NOTA_ELETRONICA.values():
        assert sugerida in permitidas, sugerida


def test_gerou_nota_so_aceita_sim_ou_nao():
    """O campo é de duas opções. "SIM" ou "sim" seriam recusados."""
    from app.apps.analisesps import pipefy
    assert pipefy.GEROU_NOTA_SIM == "Sim"
    assert pipefy.GEROU_NOTA_NAO == "Não"


# ---------------------------------------------------------------------------
# A GRAVAÇÃO NOS CARDS DO PIPEFY
#
# É A CHAMADA SEM VOLTA deste módulo: o card é alterado de verdade, e não há
# desfazer. Por isso os testes aqui olham o que É MANDADO, e não só se a função
# roda — o estrago de mandar errado não aparece na tela, aparece no card.
# ---------------------------------------------------------------------------
class PipefyFalso:
    """Guarda a consulta que teria ido para a API, e responde o que ela
    responderia. Nenhum teste encosta no Pipefy de verdade."""

    def __init__(self, sucesso=True):
        self.consultas = []
        self.sucesso = sucesso

    def __call__(self, consulta, token=None):
        self.consultas.append(consulta)
        import re as _re
        return {m: {"success": self.sucesso}
                for m in _re.findall(r"(m\d+):", consulta)}


def _mandar(monkeypatch, atualizacoes, sucesso=True):
    from app.apps.analisesps import pipefy
    falso = PipefyFalso(sucesso)
    monkeypatch.setattr(pipefy, "graphql", falso)
    monkeypatch.setattr(pipefy, "_token", lambda: "fingido")
    resultado = pipefy.atualizar_documentacao_fiscal(atualizacoes)
    return resultado, " ".join(falso.consultas)


def test_a_categoria_e_a_chave_vao_para_os_campos_certos(monkeypatch):
    """Errar um identificador do Pipefy NÃO dá erro: a chamada é aceita e nada
    é gravado. Só se descobriria abrindo o card."""
    resultado, enviado = _mandar(monkeypatch, [{
        "card": "1409289353", "documentacao": "NF-e (Mercadoria)",
        "chave": chave(CREDOR)}])
    assert resultado["ok"] == ["1409289353"] and not resultado["falhas"]
    assert "documenta_o_fiscal" in enviado
    assert "chave_de_acesso" in enviado
    assert chave(CREDOR) in enviado
    assert "NF-e (Mercadoria)" in enviado


def test_achar_a_nota_marca_que_a_despesa_GEROU_nota():
    """Com a chave na mão, a resposta é sim — e é esse campo que destrava o
    resto do fluxo no Pipefy."""
    import inspect

    from app.apps.analisesps import pipefy
    codigo = inspect.getsource(pipefy.atualizar_documentacao_fiscal)
    assert "CAMPO_GEROU_NOTA" in codigo and "GEROU_NOTA_SIM" in codigo


def test_NAO_achar_a_nota_nunca_escreve_NAO_no_card(monkeypatch):
    """Não ter encontrado não prova que não existe — pode ser nota fora do
    relatório do FSist. Escrever "Não" ali seria afirmar o que este módulo não
    sabe, e o campo é usado por outras pessoas."""
    _, enviado = _mandar(monkeypatch, [{
        "card": "1", "documentacao": "Ausente", "chave": ""}])
    assert "a_despesa_gerou_emiss_o_de_nota_fiscal" not in enviado


def test_chave_vazia_NAO_e_mandada_para_nao_apagar_a_que_ja_existe(monkeypatch):
    """O PIOR EFEITO POSSÍVEL DESTA TELA seria apagar a chave que outra pessoa
    preencheu à mão. Mandar campo vazio é gravar vazio."""
    _, enviado = _mandar(monkeypatch, [{
        "card": "1", "documentacao": "Não Dedutível", "chave": ""}])
    assert "chave_de_acesso" not in enviado


def test_chave_pela_metade_nao_e_mandada(monkeypatch):
    """44 dígitos ou não é chave. Meia chave num card é pior que nenhuma:
    parece decidida."""
    _, enviado = _mandar(monkeypatch, [{
        "card": "1", "documentacao": "NF-e (Mercadoria)", "chave": "12345"}])
    assert "chave_de_acesso" not in enviado


def test_card_sem_nada_para_escrever_e_recusado_e_nao_mandado(monkeypatch):
    resultado, enviado = _mandar(monkeypatch, [{
        "card": "1", "documentacao": "", "chave": ""}])
    assert resultado["ok"] == []
    assert "1" in resultado["falhas"]
    assert "updateFieldsValues" not in enviado


def test_quem_o_Pipefy_recusa_e_devolvido_com_nome(monkeypatch):
    """Quem chama precisa saber QUAIS passaram, não só quantos: só esses podem
    ser marcados como escritos. Contar erraria para sempre."""
    resultado, _ = _mandar(monkeypatch, [
        {"card": "1", "documentacao": "NF-e (Mercadoria)", "chave": chave(CREDOR)},
        {"card": "2", "documentacao": "Seguros", "chave": ""}], sucesso=False)
    assert resultado["ok"] == []
    assert set(resultado["falhas"]) == {"1", "2"}


def test_falha_de_rede_num_bloco_nao_derruba_os_outros(monkeypatch):
    """Vinte cards por ida à API. Uma ida que falha não pode levar as outras
    junto — e os que passaram têm de ficar marcados como passados."""
    from app.apps.analisesps import pipefy

    chamadas = [0]

    def instavel(consulta, token=None):
        chamadas[0] += 1
        if chamadas[0] == 1:
            raise RuntimeError("a rede caiu")
        import re as _re
        return {m: {"success": True} for m in _re.findall(r"(m\d+):", consulta)}

    monkeypatch.setattr(pipefy, "graphql", instavel)
    monkeypatch.setattr(pipefy, "_token", lambda: "fingido")
    resultado = pipefy.atualizar_documentacao_fiscal([
        {"card": str(n), "documentacao": "Seguros"} for n in range(1, 26)])

    assert len(resultado["falhas"]) == 20, "o bloco que caiu"
    assert len(resultado["ok"]) == 5, "o bloco seguinte tinha de passar"
    assert "a rede caiu" in " ".join(resultado["falhas"].values())


def test_o_valor_que_VEM_DO_BANCO_conta_como_valor():
    """O DEFEITO QUE ESTE TESTE GUARDA, achado em 12/09/2026: a coluna do valor
    da nota é NUMERIC, e o banco devolve NUMERIC como `Decimal` — que não é
    `int` nem `float`.

    Sem tratar esse tipo, `Decimal("269.00")` caía no caminho do texto
    brasileiro, onde o ponto é separador de milhar: virava **26.900**. E o
    estrago não aparecia na tela — aparecia como ponto que faltava: o valor
    NUNCA batia, e toda conciliação perdia os 25 pontos do valor exato.

    Um erro de conciliação que some 25 pontos em TODO caso é o tipo de defeito
    que faz a tela "quase funcionar" para sempre."""
    from decimal import Decimal

    pontos, porques = fiscal.pontuar(sp(), nota(valor=Decimal("269.00")))
    assert any("valor é igual" in p for p in porques), (
        "o valor vindo do banco não foi reconhecido")
    assert pontos >= fiscal.PONTOS_EMITENTE + fiscal.PONTOS_VALOR_EXATO


def test_o_valor_do_lancamento_tambem_pode_vir_do_banco():
    from decimal import Decimal
    _, porques = fiscal.pontuar(sp(valor=Decimal("269.00")), nota())
    assert any("valor é igual" in p for p in porques)
