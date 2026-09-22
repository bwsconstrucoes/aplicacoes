# -*- coding: utf-8 -*-
"""
A obra tem DOIS códigos na C. Diários: o primário (coluna "Código Primário") e o
secundário (primeira coluna, A). O card do Pipefy pode trazer qualquer um dos
dois, e a emissão precisa achar a obra nos dois casos.

Veio de um caso real: a obra AREFORTAL09 existia na planilha e a emissão recusou
o card com "Obra 'AREFORTAL09' não encontrada na C. Diários", porque a busca só
olhava o código primário.
"""
import pytest

from app.apps.emissaonf.cdiarios import carregar_obras, buscar_obra


CABECALHO = ["Cód. Secundário", "Centro de Custo", "Município", "UF", "Valor",
             "Alíquota ISS", "Tributação", "CNO", "Cliente", "CNPJ Cliente",
             "Endereço Cliente", "Contrato", "Objeto", "Código Omie",
             "Conta de Pagamento", "Nº Centro de Custo", "Código Primário"]


def _linha(cod_secundario, cod_primario, **extras):
    row = [cod_secundario, "CC", "Fortaleza-CE", "CE", "100", "5", "ONERADA - 50/50 - 80/20 - IR",
           "123", "Cliente", "00000000000000", "Rua X, 1", "C-1", "Objeto", "OMIE-1",
           "Bradesco", "9", cod_primario]
    for chave, valor in extras.items():
        row[CABECALHO.index(chave)] = valor
    return row


def _planilha(*linhas):
    return carregar_obras([CABECALHO, *linhas])


def test_acha_pelo_codigo_primario():
    obras = _planilha(_linha("AREFORTAL09", "CREPEEXU"))
    assert buscar_obra("CREPEEXU", obras).codigo_primario == "CREPEEXU"


def test_acha_pelo_codigo_secundario_da_coluna_a():
    """O caso da AREFORTAL09: o card traz o secundário, e antes dava KeyError."""
    obras = _planilha(_linha("AREFORTAL09", "CREPEEXU"))
    obra = buscar_obra("AREFORTAL09", obras)
    assert obra.codigo_primario == "CREPEEXU"
    assert obra.codigo_secundario == "AREFORTAL09"


def test_a_busca_ignora_espaco_e_caixa():
    obras = _planilha(_linha("AREFORTAL09", "CREPEEXU"))
    assert buscar_obra("  arefortal09 ", obras).codigo_primario == "CREPEEXU"


def test_o_codigo_primario_nunca_e_encoberto_por_um_secundario():
    """Se o secundário de uma linha repetir o primário de outra, o primário vence
    — senão a nota sairia com a tributação da obra errada."""
    obras = _planilha(
        _linha("CREPEEXU", "OUTRAOBRA", **{"CNO": "999"}),   # secundário colide
        _linha("AREFORTAL09", "CREPEEXU", **{"CNO": "111"}),  # primário de verdade
    )
    assert buscar_obra("CREPEEXU", obras).cno == "111"


def test_linha_so_com_codigo_secundario_entra_no_indice():
    """Linha sem código primário não pode sumir: ela ainda identifica uma obra."""
    obras = _planilha(_linha("SOSECUNDARIO", ""))
    assert buscar_obra("SOSECUNDARIO", obras).codigo_secundario == "SOSECUNDARIO"


def test_codigo_que_nao_existe_continua_dando_erro_e_diz_que_tentou_os_dois():
    obras = _planilha(_linha("AREFORTAL09", "CREPEEXU"))
    with pytest.raises(KeyError) as erro:
        buscar_obra("NAOEXISTE", obras)
    mensagem = str(erro.value)
    assert "Código Primário" in mensagem
    assert "primeira coluna" in mensagem
