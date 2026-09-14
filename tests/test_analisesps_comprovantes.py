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
OMIE_OK = {"omie": [{"step": "baixar", "response": {"ok": True}}]}
OMIE_RECUSOU = {"omie": [{"step": "baixar", "response": {"ok": False}}]}


def _plano(status="localizado", pode=True, pagina=1, motivos=None,
           respostas=None, acao="baixar_omie_atualizar_pipefy_sheets",
           **recibo):
    base = {"page": pagina, "valor_pago": "1.234,56",
            "nome_recebedor": "SERTAO CASA E CONSTRUCAO", "id_pipefy": "1443253428"}
    base.update(recibo)
    return {"match": {"status": status}, "pode_executar": pode, "acao": acao,
            "motivos_bloqueio": motivos or [], "receipt": base,
            "responses": OMIE_OK if respostas is None else respostas}


def test_o_que_baixou_vira_linha_BAIXADO():
    linhas = comprovantes.ler_resposta({"planos": [_plano()]})
    assert len(linhas) == 1
    assert linhas[0]["situacao"] == comprovantes.BAIXADO
    assert linhas[0]["sp_id"] == "1443253428"
    assert linhas[0]["valor"] == "1.234,56"


# ---------------------------------------------------------------------------
# O DEFEITO DE 13/09/2026 — "estava baixado, mas na planilha não ficaram pagos"
#
# O robô assume MODO DE ENSAIO quando o pedido não diz nada. O pedido montado
# aqui só mandava o arquivo, então TODA baixa feita por esta tela desde a
# estreia foi simulação: o robô localizava a SP, montava o plano, respondia
# "dá para executar" — e não escrevia em lugar nenhum.
#
# A tela dizia "Baixado" porque lia "dá para executar" como "foi feito". Duas
# travas entram aqui, e elas são independentes de propósito: uma conserta o
# pedido, a outra impede a tela de anunciar baixa que não houve. Se um dia
# alguém mexer no pedido de novo, a segunda ainda pega.
# ---------------------------------------------------------------------------
def test_o_pedido_ao_robo_diz_EXPLICITAMENTE_que_nao_e_ensaio(monkeypatch):
    """A causa. Sem esta linha, o robô assume ensaio e não grava nada."""
    capturado = {}
    import sys
    import types

    falso = types.ModuleType("app.apps.baixabradesco.core")
    falso.processar_baixabradesco = lambda pedido: capturado.update(pedido) or {}
    monkeypatch.setitem(sys.modules, "app.apps.baixabradesco.core", falso)

    comprovantes._mandar_ao_robo(b"%PDF-", "comprovante.pdf")
    assert capturado["modo_teste"] is False, (
        "sem isto o robô roda em ensaio e a tela mente para quem usa")


def test_WhatsApp_fica_DESLIGADO_a_partir_desta_tela(monkeypatch):
    """Mandar mensagem para fornecedor é efeito para fora da empresa, e
    ninguém pediu isso a partir daqui."""
    capturado = {}
    import sys
    import types

    falso = types.ModuleType("app.apps.baixabradesco.core")
    falso.processar_baixabradesco = lambda pedido: capturado.update(pedido) or {}
    monkeypatch.setitem(sys.modules, "app.apps.baixabradesco.core", falso)

    comprovantes._mandar_ao_robo(b"%PDF-", "comprovante.pdf")
    assert capturado["opcoes"]["enviar_whatsapp"] is False


def test_resposta_em_ENSAIO_nunca_vira_BAIXADO():
    """A segunda trava, e a que teria pego o defeito no primeiro dia: o robô
    diz no corpo da resposta que rodou em ensaio."""
    linhas = comprovantes.ler_resposta({"modo_teste": True, "planos": [_plano()]})
    assert linhas[0]["situacao"] == comprovantes.ERRO
    assert "ENSAIO" in linhas[0]["motivo"]
    assert "reenvie" in linhas[0]["motivo"].lower()


def test_plano_bom_SEM_resposta_do_Omie_nao_e_baixa():
    """"Pode executar" não é "executou". Era exatamente essa confusão.

    ⚠️ A FRASE MUDOU em 14/09/2026, e a mudança é o conserto: dizer "não
    recebi confirmação" quando na verdade o Omie NEM FOI CHAMADO mandou o dono
    procurar no lugar errado por dois dias."""
    linhas = comprovantes.ler_resposta({"planos": [_plano(respostas={})]})
    assert linhas[0]["situacao"] == comprovantes.ERRO
    assert "NÃO chegou a chamar o Omie" in linhas[0]["motivo"]


def test_Omie_que_RECUSOU_aparece_como_erro_e_nao_como_baixa():
    linhas = comprovantes.ler_resposta({"planos": [_plano(respostas=OMIE_RECUSOU)]})
    assert linhas[0]["situacao"] == comprovantes.ERRO
    assert "recusou a baixa" in linhas[0]["motivo"]
    # E avisa da dessincronia: o robô grava planilha e card sem esperar o Omie.
    assert "planilha" in linhas[0]["motivo"].lower()


def test_baixa_SEM_SP_diz_que_nao_ha_SP_para_marcar_na_planilha():
    """A linha de FERNANDO CARVALHO em 13/09: "Baixado" com SP "—". A baixa é
    real (transferência lançada direto no Omie), mas quem lê "Baixado" vai
    procurar a SP na planilha e concluir que o sistema mentiu."""
    linhas = comprovantes.ler_resposta({"planos": [_plano(
        status="transferencia_sem_sp", acao="lancar_movimentacao_omie_sem_sp",
        id_pipefy="")]})
    assert linhas[0]["situacao"] == comprovantes.BAIXADO
    assert "Não há SP" in linhas[0]["motivo"]


def test_a_baixa_confirmada_NAO_promete_a_planilha_ja_atualizada():
    """O robô atualiza a planilha em segundo plano, então a resposta dele não
    diz se ela já mudou. Prometer o que não se sabe é como este defeito
    começou."""
    linhas = comprovantes.ler_resposta({"planos": [_plano()]})
    assert "logo em seguida" in linhas[0]["motivo"]


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


# ===========================================================================
# ⚠️ O ROBÔ SEMPRE DISSE POR QUE NÃO BAIXOU — era esta tela que jogava fora
#
# Relato do dono em 14/09/2026, com quatro páginas na mão: *"baixam na planilha
# mas não baixam no Omie. Não tem sentido. Por que que não está baixando no
# sistema Omie?"* — e as quatro linhas diziam a mesma frase vazia: *"Não recebi
# confirmação do Omie para esta baixa."*
#
# A frase era NOSSA. O robô interrompe a sequência do Omie em vários pontos e
# em cada um escreve o motivo; a leitura antiga procurava só um passo `baixar`
# com `ok`, não achava, e devolvia "não sei". A explicação morria no JSON.
# ===========================================================================
def _plano_omie(passos, acao="baixar_titulo", **extra):
    base = {"match": {"status": "localizado", "id": "144"},
            "pode_executar": True, "acao": acao,
            "responses": {"omie": passos}}
    base.update(extra)
    return base


def test_TITULO_NAO_ENCONTRADO_no_Omie_aparece_com_essas_palavras():
    """É trabalho para ele fazer no Omie — e a tela dizia "não recebi
    confirmação", que manda procurar no lugar errado."""
    from app.apps.analisesps import comprovantes

    situacao, motivo = comprovantes._situacao_do_plano(_plano_omie([
        {"step": "consultar", "response": {"ok": False}},
        {"step": "abort",
         "motivo": "Título não encontrado no Omie. Inclua o título primeiro."},
    ]))
    assert situacao == comprovantes.ERRO
    assert "não encontrado no Omie" in motivo
    assert "Inclua o título primeiro" in motivo


def test_TITULO_JA_PAGO_nao_e_erro(monkeypatch):
    """⚠️ Chamar de erro faz ele REENVIAR um comprovante que não precisa — e
    reenviar é o caminho para pagar duas vezes."""
    from app.apps.analisesps import comprovantes

    situacao, motivo = comprovantes._situacao_do_plano(_plano_omie([
        {"step": "consultar", "response": {"ok": True}},
        {"step": "skip", "motivo": "Título já consta PAGO no Omie."},
    ]))
    assert situacao == comprovantes.BAIXADO
    assert "já consta PAGO" in motivo


def test_a_BAIXA_RECUSADA_diz_o_motivo_E_avisa_da_planilha():
    """⚠️ A dessincronia tem de ser dita: o robô atualiza planilha e card SEM
    esperar a resposta do Omie. Quem lê precisa saber que os dois lados podem
    ter ficado diferentes."""
    from app.apps.analisesps import comprovantes

    situacao, motivo = comprovantes._situacao_do_plano(_plano_omie([
        {"step": "baixar", "response": {"ok": False}},
        {"step": "erro_baixa", "motivo": "Falha ao lançar pagamento no Omie."},
    ]))
    assert situacao == comprovantes.ERRO
    assert "Falha ao lançar pagamento" in motivo
    assert "planilha" in motivo.lower()


def test_quando_o_Omie_NEM_FOI_CHAMADO_a_tela_diz_isso():
    """"Não recebi confirmação" e "não cheguei a perguntar" são coisas
    diferentes, e só a segunda explica o que aconteceu."""
    from app.apps.analisesps import comprovantes

    situacao, motivo = comprovantes._situacao_do_plano(_plano_omie([]))
    assert situacao == comprovantes.ERRO
    assert "NÃO chegou a chamar o Omie" in motivo


def test_a_baixa_CONFIRMADA_continua_dizendo_que_deu_certo():
    from app.apps.analisesps import comprovantes

    situacao, motivo = comprovantes._situacao_do_plano(_plano_omie([
        {"step": "consultar", "response": {"ok": True}},
        {"step": "baixar", "response": {"ok": True}},
    ]))
    assert situacao == comprovantes.BAIXADO
    assert "confirmada no Omie" in motivo


def test_a_frase_VAZIA_antiga_nao_existe_mais():
    """Ela é o defeito em pessoa: dizia que não sabia, quando sabia."""
    import pathlib
    fonte = pathlib.Path(
        "app/apps/analisesps/comprovantes.py").read_text(encoding="utf-8")
    # Sobrou só dentro do comentário que conta a história.
    assert fonte.count("Não recebi confirmação do Omie") <= 1
