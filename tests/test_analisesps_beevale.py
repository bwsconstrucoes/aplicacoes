# -*- coding: utf-8 -*-
"""
O BeeVale — o cartão de benefício dos terceirizados.

O QUE ESTES TESTES PROTEGEM, e por que valem mais aqui do que em outros
lugares do módulo: este é o ÚNICO caminho que escreve fora da planilha SPsBD.
Ele sobe arquivo no Google Drive e reescreve o card no Pipefy, e nada disso
tem desfazer. Nenhum teste encosta nas duas coisas de verdade — as duas são
dubladas —, e é justamente por isso que a ORDEM das operações e as travas
precisam estar amarradas aqui.

O layout das colunas dos dois arquivos também é conferido: quem recebe é o
portal do BeeVale, que não sabe que o programa mudou de Streamlit para web.
"""
from __future__ import annotations

import io

import pytest

from app.apps.analisesps import beevale


# ---------------------------------------------------------------------------
# Arrumação dos dados — as regras vieram do Apps Script e não são invenção
# ---------------------------------------------------------------------------
def test_pega_o_cpf_dos_emails_que_o_portal_devolve():
    texto = ("Erro na linha 3: 01234567890@bwsconstrucoes.com.br\n"
             "Erro na linha 9: 98765432100@bwsconstrucoes.com.br")
    assert beevale.extrair_cpfs(texto) == ["01234567890", "98765432100"]


def test_email_ganha_do_numero_solto():
    """Um texto de erro costuma ter os dois — número de protocolo e e-mail. O
    e-mail é o confiável; o número solto pode ser qualquer coisa de 11 dígitos."""
    texto = "protocolo 11111111111 — 01234567890@bwsconstrucoes.com.br"
    assert beevale.extrair_cpfs(texto) == ["01234567890"]


def test_cpf_solto_serve_quando_nao_ha_email():
    assert beevale.extrair_cpfs("01234567890 e 98765432100") == [
        "01234567890", "98765432100"]


def test_nao_repete_o_mesmo_cpf():
    texto = ("01234567890@bwsconstrucoes.com.br\n"
             "01234567890@bwsconstrucoes.com.br")
    assert beevale.extrair_cpfs(texto) == ["01234567890"]


def test_o_zero_da_frente_do_cpf_volta():
    """A planilha guarda CPF como NÚMERO. O Google devolve "1234567890" para um
    CPF que começa com zero — e às vezes com ".0" no fim. Sem repor o zero, o
    CPF nunca casa e a pessoa "não existe"."""
    assert beevale.normaliza_cpf("1234567890") == "01234567890"
    assert beevale.normaliza_cpf("1234567890.0") == "01234567890"
    assert beevale.normaliza_cpf("012.345.678-90") == "01234567890"
    assert beevale.normaliza_cpf("") == ""


def test_nome_do_cartao_e_primeiro_e_ultimo():
    assert beevale.nome_impresso("José Carlos de Souza Lima") == "José Lima"
    assert beevale.nome_impresso("Madonna") == "Madonna"
    assert beevale.nome_impresso("  ") == ""


def test_data_de_nascimento_sai_em_portugues():
    assert beevale.data_do_cadastro("1990-04-25") == "25/04/1990"
    assert beevale.data_do_cadastro("25-04-1990") == "25/04/1990"
    assert beevale.data_do_cadastro("25/04/1990") == "25/04/1990"
    assert beevale.data_do_cadastro("") == ""


def test_celular_perde_o_codigo_do_pais():
    assert beevale.telefone_br("5548999887766") == "(48) 99988-7766"
    assert beevale.telefone_br("4832221100") == "(48) 3222-1100"
    assert beevale.telefone_br("") == ""


def test_valor_do_card_em_portugues():
    assert beevale.valor_do_card("1.234,56") == 1234.56
    assert beevale.valor_do_card("") == 0.0
    assert beevale.valor_do_card("qualquer coisa") == 0.0


# ---------------------------------------------------------------------------
# Os arquivos — o layout é contrato com o portal, não escolha de estilo
# ---------------------------------------------------------------------------
def _ler(conteudo: bytes):
    from openpyxl import load_workbook
    aba = load_workbook(io.BytesIO(conteudo)).active
    return aba, [c.value for c in aba[1]]


def test_o_cadastro_sai_com_as_colunas_que_o_portal_espera():
    conteudo = beevale.cadastro_xlsx([beevale.registro(
        "José Carlos Lima", "1990-04-25", "5548999887766", "01234567890")])
    aba, cabecalho = _ler(conteudo)
    assert cabecalho == beevale.COLUNAS_CADASTRO
    linha = [c.value for c in aba[2]]
    assert linha[0] == "José Carlos Lima"
    assert linha[1] == "José Lima"
    assert linha[2] == "012.345.678-90"
    assert linha[3] == "01234567890@bwsconstrucoes.com.br"


def test_o_cpf_sai_como_texto_no_arquivo():
    """Se o Excel entender o CPF como número, o zero da frente some — e o
    portal recusa o arquivo inteiro."""
    conteudo = beevale.cadastro_xlsx([beevale.registro(
        "Ana Silva", "", "", "01234567890")])
    aba, _ = _ler(conteudo)
    assert aba["C2"].number_format == "@"


def test_o_pagamento_sai_com_as_colunas_que_o_portal_espera():
    conteudo = beevale.pagamento_xlsx([{
        "nome": "Ana Silva", "email": "01234567890@bwsconstrucoes.com.br",
        "valor": 850.5, "cpf": "012.345.678-90", "card": "525982424"}])
    aba, cabecalho = _ler(conteudo)
    assert cabecalho == beevale.COLUNAS_PAGAMENTO
    linha = [c.value for c in aba[2]]
    assert linha[4] == 850.5                    # Valor, como número
    assert linha[9] == "525982424"              # Centro de Custo = o card
    assert aba["E2"].number_format == "0.00"


# ---------------------------------------------------------------------------
# A descrição do card — o único lugar do módulo que pode APAGAR texto de gente
# ---------------------------------------------------------------------------
def test_a_descricao_que_ja_existia_e_preservada():
    """É o histórico de quem pediu o quê. Perder isso não tem volta."""
    nova = beevale.montar_descricao(
        "Compra do mês de março\nAutorizado pelo João",
        "https://drive/pag", "https://drive/cad")
    assert "Compra do mês de março" in nova
    assert "Autorizado pelo João" in nova
    assert nova.startswith("Pagamento BeeVale: https://drive/pag")


def test_os_links_antigos_sao_trocados_e_nao_empilhados():
    """Gerar duas vezes não pode deixar quatro linhas de link no card — e a
    segunda geração é justamente o caso comum, quando a primeira saiu errada."""
    atual = ("Pagamento BeeVale: https://velho/pag\n"
             "Cadastro BeeVale: https://velho/cad\n"
             "\nTexto que fica")
    nova = beevale.montar_descricao(atual, "https://novo/pag", "https://novo/cad")
    assert "velho" not in nova
    assert nova.count("Pagamento BeeVale:") == 1
    assert "Texto que fica" in nova


def test_descricao_vazia_fica_so_com_os_links():
    nova = beevale.montar_descricao("", "https://a", "https://b")
    assert nova == "Pagamento BeeVale: https://a\nCadastro BeeVale: https://b"


# ---------------------------------------------------------------------------
# A geração — travas e ORDEM
# ---------------------------------------------------------------------------
def test_sem_pasta_do_drive_nao_comeca(monkeypatch):
    """A trava mais importante: sem lugar para guardar o arquivo, o link que
    iria para o card não existiria. Melhor não começar do que parar no meio."""
    monkeypatch.setattr(beevale, "pasta_do_drive", lambda: "")
    with pytest.raises(beevale.ErroDoBeeVale) as erro:
        beevale.gerar(["525982424"])
    assert "DRIVE_FOLDER_ID" in str(erro.value)


def test_tem_teto_de_sps_por_vez(monkeypatch):
    monkeypatch.setattr(beevale, "pasta_do_drive", lambda: "pasta")
    demais = [str(i) for i in range(beevale.MAXIMO_POR_VEZ + 1)]
    with pytest.raises(beevale.ErroDoBeeVale):
        beevale.gerar(demais)


def _pipefy_falso(monkeypatch, *, cpf="012.345.678-90", descricao="Original"):
    from app.apps.analisesps import pipefy
    monkeypatch.setattr(pipefy, "buscar_cards", lambda ids, token=None: {
        str(i): {"id": str(i), "campos": {
            pipefy.CAMPO_CADASTRO: cpf, pipefy.CAMPO_VALOR: "850,50",
            pipefy.CAMPO_DESCRICAO: descricao}} for i in ids})
    monkeypatch.setattr(pipefy, "buscar_cadastros", lambda cpfs, token=None: {
        cpf: {"id": "9", "nome_completo": "Ana Silva", "cpf": cpf,
              "data_de_nascimento": "1990-04-25",
              "telefone_celular": "5548999887766"}})
    return pipefy


def test_o_card_so_e_alterado_DEPOIS_de_o_arquivo_estar_no_drive(monkeypatch):
    """A ordem não é detalhe. Marcar o card e depois descobrir que o arquivo
    não subiu deixa um card dizendo que está pronto quando não está — e quem
    olhar não tem como saber."""
    from app.apps.analisesps import drive
    pipefy = _pipefy_falso(monkeypatch)
    monkeypatch.setattr(beevale, "pasta_do_drive", lambda: "pasta")

    aconteceu = []

    def subiu(conteudo, nome, pasta):
        aconteceu.append(("drive", nome))
        return {"id": "x", "link": "https://drive/" + nome}

    def escreveu(atualizacoes, token=None):
        aconteceu.append(("pipefy", len(atualizacoes)))
        return []

    monkeypatch.setattr(drive, "subir_xlsx", subiu)
    monkeypatch.setattr(pipefy, "atualizar_descricao_e_doc_fiscal", escreveu)

    resultado = beevale.gerar(["525982424"])

    assert [p[0] for p in aconteceu] == ["drive", "drive", "pipefy"]
    assert resultado["atualizados"] == ["525982424"]
    assert not resultado["erros"]


def test_drive_falhando_nao_toca_no_pipefy(monkeypatch):
    """Se o arquivo não sobe, o card fica exatamente como estava."""
    from app.apps.analisesps import drive
    pipefy = _pipefy_falso(monkeypatch)
    monkeypatch.setattr(beevale, "pasta_do_drive", lambda: "pasta")

    def recusa(conteudo, nome, pasta):
        raise drive.ErroDoDrive("cota estourada")

    escritas = []
    monkeypatch.setattr(drive, "subir_xlsx", recusa)
    monkeypatch.setattr(pipefy, "atualizar_descricao_e_doc_fiscal",
                        lambda a, token=None: escritas.append(a) or [])

    resultado = beevale.gerar(["525982424"])

    assert escritas == [], "o card foi alterado mesmo sem o arquivo existir"
    assert not resultado["feitos"]
    assert "cota estourada" in resultado["erros"][0]["motivo"]


def test_arquivo_no_drive_sem_o_card_atualizado_aparece_como_problema(monkeypatch):
    """Sucesso pela metade não pode ser contado como sucesso: o card continua
    dizendo que falta fazer, e só quem vê a tela sabe que os arquivos existem."""
    from app.apps.analisesps import drive
    pipefy = _pipefy_falso(monkeypatch)
    monkeypatch.setattr(beevale, "pasta_do_drive", lambda: "pasta")
    monkeypatch.setattr(drive, "subir_xlsx",
                        lambda c, n, p: {"id": "x", "link": "https://drive/" + n})
    monkeypatch.setattr(pipefy, "atualizar_descricao_e_doc_fiscal",
                        lambda a, token=None: [u["card"] for u in a])

    resultado = beevale.gerar(["525982424"])

    assert resultado["feitos"], "os arquivos subiram, isso não se perde"
    assert resultado["atualizados"] == []
    assert resultado["nao_atualizados"] == ["525982424"]
    assert resultado["erros"], "ficar sem atualizar o card tem de aparecer"


def test_card_sem_cpf_nao_para_os_outros(monkeypatch):
    """Uma SP com o campo vazio é comum. Ela sai da lista com o motivo escrito;
    as demais seguem."""
    from app.apps.analisesps import pipefy
    monkeypatch.setattr(beevale, "pasta_do_drive", lambda: "pasta")
    monkeypatch.setattr(pipefy, "buscar_cards", lambda ids, token=None: {
        "1": {"id": "1", "campos": {pipefy.CAMPO_CADASTRO: "",
                                    pipefy.CAMPO_VALOR: "10,00",
                                    pipefy.CAMPO_DESCRICAO: ""}},
        "2": {"id": "2", "campos": {pipefy.CAMPO_CADASTRO: "012.345.678-90",
                                    pipefy.CAMPO_VALOR: "20,00",
                                    pipefy.CAMPO_DESCRICAO: ""}}})
    monkeypatch.setattr(pipefy, "buscar_cadastros", lambda cpfs, token=None: {
        "012.345.678-90": {"id": "9", "nome_completo": "Ana Silva",
                           "cpf": "012.345.678-90",
                           "data_de_nascimento": "", "telefone_celular": ""}})

    preparado = beevale.preparar(["1", "2"])

    assert [p["sp"] for p in preparado["prontos"]] == ["2"]
    assert preparado["erros"][0]["sp"] == "1"
    assert "Cadastro BeeVale" in preparado["erros"][0]["motivo"]


# ---------------------------------------------------------------------------
# O Pipefy — a porta que escreve fora
# ---------------------------------------------------------------------------
def test_id_de_card_que_nao_e_numero_e_recusado():
    """Os ids entram na consulta SEM aspas, porque é assim que a API os quer.
    Texto solto ali seria injeção de GraphQL."""
    from app.apps.analisesps import pipefy
    with pytest.raises(pipefy.ErroDoPipefy):
        pipefy._numero_do_card('1) { x } mutation {')


def test_a_descricao_vai_escapada_para_o_graphql():
    """Descrição tem aspas e quebra de linha. Montar isso na mão é como se
    monta uma consulta quebrada — ou pior."""
    from app.apps.analisesps import pipefy
    saida = pipefy._texto_gql('linha 1\nele disse "oi"')
    assert saida.startswith('"') and saida.endswith('"')
    assert "\\n" in saida and '\\"' in saida


def test_cpf_do_conector_vem_de_json_ou_de_texto():
    from app.apps.analisesps import pipefy
    assert pipefy.extrair_cpf('["012.345.678-90"]') == "012.345.678-90"
    assert pipefy.extrair_cpf("CPF 012.345.678-90 do fulano") == "012.345.678-90"
    assert pipefy.extrair_cpf("") == ""


# ---------------------------------------------------------------------------
# O Drive — a armadilha da cota, que é a razão de isto ter ficado de fora
# ---------------------------------------------------------------------------
def test_sem_pasta_o_drive_diz_o_nome_da_variavel():
    from app.apps.analisesps import drive
    with pytest.raises(drive.ErroDoDrive) as erro:
        drive.subir_xlsx(b"x", "a.xlsx", "")
    assert "DRIVE_FOLDER_ID" in str(erro.value)


def test_erro_de_cota_vira_a_instrucao_de_mover_a_pasta():
    """O Google diz "storageQuotaExceeded", que faz pensar em falta de espaço.
    O conserto de verdade é mover a pasta para um Drive Compartilhado, e é
    isso que a tela precisa dizer."""
    from app.apps.analisesps import drive

    class RespostaFalsa:
        status_code = 403
        text = '{"error":{"errors":[{"reason":"storageQuotaExceeded"}]}}'

    mensagem = drive._explicar(RespostaFalsa())
    assert "DRIVE COMPARTILHADO" in mensagem.upper()


def test_pasta_fora_de_drive_compartilhado_e_avisada(monkeypatch):
    """Enxergar a pasta e poder gravar nela são coisas diferentes — e é na
    gravação que a cota morde. O aviso tem de vir antes, não no meio."""
    from app.apps.analisesps import drive

    class RespostaFalsa:
        status_code = 200

        @staticmethod
        def json():
            return {"id": "abc", "name": "BeeVale", "mimeType": "folder"}

    class SessaoFalsa:
        @staticmethod
        def get(*a, **k):
            return RespostaFalsa()

    monkeypatch.setattr(drive, "_sessao", lambda: SessaoFalsa())
    resultado = drive.conferir_pasta("abc")

    assert resultado["ok"] is True
    assert resultado["compartilhado"] is False
    assert resultado["aviso"]
