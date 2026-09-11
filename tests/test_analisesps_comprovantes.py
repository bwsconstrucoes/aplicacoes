# -*- coding: utf-8 -*-
"""Análise de SPs — os comprovantes arrastados para dentro da tela.

O QUE ESTE ARQUIVO GUARDA. O robô que dá a baixa já existe e roda em produção
(`baixabradesco`); o que é novo aqui é a PORTA DE ENTRADA e a MEMÓRIA. Então os
testes se concentram em três coisas que erram calado:

  1. o corte em levas de dez, que é o que o script do dono já faz hoje;
  2. a numeração da página, que é o que permite achar o comprovante no PDF;
  3. a leitura da resposta do robô — que hoje volta para o Make.com e morre lá.
"""
from __future__ import annotations

import io

import pytest

from app.apps.analisesps import comprovantes


def pdf_com(paginas: int) -> bytes:
    from pypdf import PdfWriter
    escritor = PdfWriter()
    for _ in range(paginas):
        escritor.add_blank_page(width=200, height=200)
    saco = io.BytesIO()
    escritor.write(saco)
    return saco.getvalue()


def paginas_de(pedaco: bytes) -> int:
    from pypdf import PdfReader
    return len(PdfReader(io.BytesIO(pedaco)).pages)


# ---------------------------------------------------------------------------
# AS LEVAS DE DEZ
#
# O número não é escolha deste código: é o que o script do dono já faz, dito
# por ele em 11/09/2026 — "ele divide o PDF em dez páginas; se eu mandar
# cinquenta num único PDF, ele quebra em cinco e manda um por um".
# ---------------------------------------------------------------------------
def test_cinquenta_paginas_viram_cinco_levas_de_dez():
    """O caso que o dono descreveu, com o número que ele deu."""
    pedacos = list(comprovantes.separar_em_levas(pdf_com(50)))
    assert len(pedacos) == 5
    assert [paginas_de(p) for _, p in pedacos] == [10, 10, 10, 10, 10]


def test_a_ultima_leva_leva_o_resto():
    pedacos = list(comprovantes.separar_em_levas(pdf_com(25)))
    assert [paginas_de(p) for _, p in pedacos] == [10, 10, 5]


def test_a_numeracao_e_a_do_ARQUIVO_ORIGINAL():
    """A página 1 da terceira leva é a página 21 do PDF que a pessoa soltou.

    Sem esse deslocamento a tela diria "página 1" três vezes, e achar o
    comprovante que não casou dentro de um PDF de cinquenta páginas viraria
    caça ao tesouro."""
    assert [n for n, _ in comprovantes.separar_em_levas(pdf_com(25))] == [1, 11, 21]


def test_pdf_de_uma_pagina_vira_uma_leva_so():
    pedacos = list(comprovantes.separar_em_levas(pdf_com(1)))
    assert len(pedacos) == 1 and paginas_de(pedacos[0][1]) == 1


@pytest.mark.parametrize("paginas,esperado", [
    (0, 0), (1, 1), (10, 1), (11, 2), (50, 5), (51, 6)])
def test_a_conta_das_levas(paginas, esperado):
    assert comprovantes.quantas_levas(paginas) == esperado


def test_a_conta_e_por_PAGINA_e_nao_por_arquivo():
    """O robô trata cada página como um comprovante separado. Cortar por
    quantidade de ARQUIVOS deixaria um PDF de cinquenta páginas passar inteiro
    numa chamada só — o caso que as levas existem para evitar."""
    assert comprovantes.quantas_levas(comprovantes.contar_paginas(pdf_com(50))) == 5


def test_contar_paginas_de_coisa_que_nao_e_pdf_nao_estoura():
    """Arquivo torto tem de virar recado, não erro 500 na cara de quem
    arrastou."""
    assert comprovantes.contar_paginas(b"isto nao e um pdf") == 0
    assert comprovantes.contar_paginas(b"") == 0


# ---------------------------------------------------------------------------
# LER O QUE O ROBÔ RESPONDEU
#
# Ele JÁ separa tudo o que o dono pediu: "esse deu certo, esse deu errado, esse
# tem duplicidade, esse faltou aquilo". Isso hoje volta para o Make.com e morre
# lá. Estes testes travam a tradução para as linhas que a tela mostra.
# ---------------------------------------------------------------------------
def _plano(status="localizado", pode=True, pagina=1, motivos=None, **recibo):
    base = {"page": pagina, "valor_pago": "1.234,56",
            "nome_recebedor": "SERTAO CASA E CONSTRUCAO", "id_pipefy": "1443253428"}
    base.update(recibo)
    return {"match": {"status": status}, "pode_executar": pode,
            "motivos_bloqueio": motivos or [], "receipt": base}


def test_o_que_baixou_vira_linha_BAIXADO():
    linhas = comprovantes.ler_resposta({"planos": [_plano()]})
    assert len(linhas) == 1
    assert linhas[0]["situacao"] == comprovantes.BAIXADO
    assert linhas[0]["sp_id"] == "1443253428"
    assert linhas[0]["valor"] == "1.234,56"


def test_o_que_nao_achou_a_SP_diz_isso_e_diz_por_que():
    linhas = comprovantes.ler_resposta({"planos": [
        _plano(status="nao_localizado", pode=False,
               motivos=["Nenhuma SP com esse valor nesta conta."])]})
    assert linhas[0]["situacao"] == comprovantes.NAO_LOCALIZADO
    assert "Nenhuma SP" in linhas[0]["motivo"], (
        "o motivo sumiu — e é ele que diz o que fazer")


def test_o_que_falta_liberar_nao_se_confunde_com_o_que_nao_achou():
    """São duas ações diferentes: um pede procurar a SP, o outro pede liberar
    a que já foi achada. Misturar os dois faria a pessoa procurar o que já
    estava na frente dela."""
    linhas = comprovantes.ler_resposta({"planos": [
        _plano(status="pendente_validacao", pode=False)]})
    assert linhas[0]["situacao"] == comprovantes.PENDENTE_VALIDACAO


def test_o_duplicado_aparece_como_duplicado_e_nao_como_erro():
    """A trava fez o trabalho dela. Mostrar isso como falha faria a pessoa ir
    caçar problema onde não há."""
    linhas = comprovantes.ler_resposta({"duplicados": [
        {"receipt": {"page": 2, "id_pipefy": "1"}, "motivo": "já baixado"}]})
    assert linhas[0]["situacao"] == comprovantes.DUPLICADO
    assert linhas[0]["pagina"] == 2


def test_o_recusado_aparece_separado():
    linhas = comprovantes.ler_resposta({"recusados": [
        {"receipt": {"page": 1}, "motivo": "pagamento não efetivado"}]})
    assert linhas[0]["situacao"] == comprovantes.RECUSADO


def test_plano_bloqueado_sem_motivo_ainda_assim_diz_alguma_coisa():
    """"Não foi possível" é pouco, mas é melhor do que uma linha em branco —
    que a pessoa lê como "deu certo"."""
    linhas = comprovantes.ler_resposta({"planos": [
        _plano(pode=False, motivos=[])]})
    assert linhas[0]["situacao"] == comprovantes.ERRO
    assert linhas[0]["motivo"]


def test_a_pagina_da_leva_e_traduzida_para_a_pagina_do_arquivo():
    """A terceira leva devolve "página 1"; no arquivo que a pessoa soltou,
    aquilo é a página 21."""
    linhas = comprovantes.ler_resposta(
        {"planos": [_plano(pagina=1), _plano(pagina=2)]}, primeira_pagina=21)
    assert [l["pagina"] for l in linhas] == [21, 22]


def test_resposta_vazia_nao_estoura():
    assert comprovantes.ler_resposta({}) == []
    assert comprovantes.ler_resposta(None) == []


def test_toda_situacao_tem_rotulo_em_portugues():
    """Situação sem rótulo apareceria na tela como NAO_LOCALIZADO, em caixa
    alta e com sublinhado — e quem lê a tela não é programador."""
    for situacao in comprovantes.ORDEM:
        assert comprovantes.ROTULOS.get(situacao)
        assert "_" not in comprovantes.ROTULOS[situacao]


def test_o_que_pede_acao_vem_ANTES_do_que_deu_certo():
    """Quem abre esta tela quer saber o que ficou de fora. O que baixou é o
    esperado, e esperado não é notícia."""
def test_o_que_pede_acao_vem_ANTES_do_que_deu_certo():
    """Quem abre esta tela quer saber o que ficou de fora. O que baixou é o
    esperado, e esperado não é notícia."""
    pede_acao = (comprovantes.ERRO, comprovantes.NAO_LOCALIZADO,
                 comprovantes.PENDENTE_VALIDACAO, comprovantes.RECUSADO)
    baixado = comprovantes.ORDEM.index(comprovantes.BAIXADO)
    for situacao in pede_acao:
        assert comprovantes.ORDEM.index(situacao) < baixado, situacao
