"""A lista de bancos (código FEBRABAN/COMPE) usada no cadastro de conta.

Pedido do dono em 10/09/2026: *"eu queria que tivesse uma base, você já puxasse
essa base com as informações bancárias da FEBRABAN"* — em vez de digitar "237"
de cabeça.

O que se prova:

  1. A lista funciona SEM internet e sem banco: é dado embutido no código.
     Banco não pode depender de o Banco Central estar no ar.
  2. O código é normalizado para três dígitos. Quem digita escreve "1" para o
     Banco do Brasil; guardar assim faria a mesma conta aparecer com dois
     códigos diferentes conforme quem cadastrou.
  3. A atualização no Banco Central é pelo BOTÃO, entende o CSV por CONTEÚDO
     (e não por posição de coluna) e, quando falha, não derruba nada: a lista
     que já existe continua valendo.
  4. Resposta estranha do Banco Central é RECUSADA em vez de substituir a
     lista boa por lixo.
"""
from __future__ import annotations

import json

import pytest

from app.apps.erp.core.cadastros import bancos
from app.apps.erp.core.comum.auditoria import ErroValidacao


# ---------------------------------------------------------------------------
# 1 e 2. A lista embutida, e o código de três dígitos
# ---------------------------------------------------------------------------
def test_os_bancos_que_a_bws_usa_estao_na_lista_embutida():
    """Sem internet nenhuma. Se estes faltarem, o cadastro de conta nasce manco."""
    for codigo, pedaco in (("001", "Brasil"), ("104", "Caixa"), ("237", "Bradesco"),
                           ("341", "Itaú"), ("033", "Santander"), ("756", "Sicoob"),
                           ("748", "Sicredi"), ("077", "Inter"), ("260", "Nubank"),
                           ("004", "Nordeste")):
        assert pedaco.lower() in bancos.EMBUTIDOS[codigo].lower(), codigo


def test_todo_codigo_embutido_tem_tres_digitos():
    fora = [c for c in bancos.EMBUTIDOS if not (len(c) == 3 and c.isdigit())]
    assert fora == []


@pytest.mark.parametrize("digitado, esperado", [
    ("1", "001"), ("01", "001"), ("001", "001"),
    ("33", "033"), ("237", "237"), (" 237 ", "237"),
    ("237-2", "237"), (None, ""), ("", ""), ("abc", ""),
])
def test_o_codigo_vira_tres_digitos(digitado, esperado):
    assert bancos.normalizar_codigo(digitado) == esperado


def test_o_rotulo_diz_o_nome_e_o_codigo():
    assert bancos.rotulo("1") == "001 · Banco do Brasil"


def test_codigo_desconhecido_nao_inventa_nome():
    """Banco que não está na lista continua cadastrável — só não ganha nome."""
    assert bancos.rotulo("999") == "999"
    assert bancos.nome("999") == ""


def test_a_lista_sai_ordenada_pelo_nome():
    lista = bancos.listar()
    nomes = [b["nome"].lower() for b in lista]
    assert nomes == sorted(nomes)
    assert len(lista) == len(bancos.EMBUTIDOS)


# ---------------------------------------------------------------------------
# 3. Ler o CSV do Banco Central por CONTEÚDO
# ---------------------------------------------------------------------------
CSV_BCB = (
    "ISPB,Nome_Reduzido,Participa_da_Compe,Acesso_Principal,Nome_Extenso,"
    "Inicio_da_Operacao\n"
    "00000000,BCO DO BRASIL S.A.,001,STR,BANCO DO BRASIL S.A.,26/11/2002\n"
    "60746948,BCO BRADESCO S.A.,237,STR,BANCO BRADESCO S.A.,22/04/2002\n"
    "60701190,ITAU UNIBANCO S.A.,341,STR,ITAU UNIBANCO S.A.,22/04/2002\n"
)


def test_o_csv_do_banco_central_e_lido_pelo_conteudo():
    achados = bancos.interpretar_csv(CSV_BCB)
    assert achados["001"].upper().startswith("BANCO DO BRASIL")
    assert "BRADESCO" in achados["237"].upper()
    assert "ITAU" in achados["341"].upper()


def test_colunas_em_outra_ordem_continuam_sendo_lidas():
    """O arquivo do BCB já trocou de colunas antes. Ler por posição quebraria
    calado — e lista de bancos errada é dinheiro no lugar errado."""
    trocado = ("Codigo,ISPB,Nome\n"
               "001,00000000,BANCO DO BRASIL S.A.\n"
               "237,60746948,BANCO BRADESCO S.A.\n")
    achados = bancos.interpretar_csv(trocado)
    assert "BRASIL" in achados["001"].upper()
    assert "BRADESCO" in achados["237"].upper()


# ---------------------------------------------------------------------------
# 4. A atualização, com banco de verdade
# ---------------------------------------------------------------------------
@pytest.mark.banco
def test_sem_atualizar_vale_a_lista_embutida(sessao_real):
    e = bancos.estado(sessao_real)

    assert e["oficial"] is False
    assert e["quantidade"] == len(bancos.EMBUTIDOS)


@pytest.mark.banco
def test_atualizar_troca_a_lista_e_guarda_a_data(sessao_real):
    muitos = "ISPB,Nome,Codigo\n" + "".join(
        f"0000{i:04d},BANCO DE TESTE NUMERO {i},{i:03d}\n" for i in range(1, 200))
    r = bancos.atualizar(sessao_real, None, baixar=lambda: muitos)
    sessao_real.flush()

    assert r["quantidade"] >= 199
    e = bancos.estado(sessao_real)
    assert e["oficial"] is True
    assert e["atualizada_em"]
    assert "TESTE" in bancos.nome("015", sessao_real).upper()


@pytest.mark.banco
def test_a_lista_embutida_continua_por_baixo_da_oficial(sessao_real):
    """Código que já está numa conta cadastrada não pode perder o nome porque
    a relação oficial daquele dia não o trouxe."""
    magra = "ISPB,Nome,Codigo\n" + "".join(
        f"0000{i:04d},BANCO DE TESTE NUMERO {i},{i:03d}\n" for i in range(500, 700))
    bancos.atualizar(sessao_real, None, baixar=lambda: magra)
    sessao_real.flush()

    assert bancos.nome("237", sessao_real) == "Bradesco"


@pytest.mark.banco
def test_banco_central_fora_do_ar_nao_estraga_a_lista(sessao_real):
    def explodir():
        raise RuntimeError("connection reset")

    with pytest.raises(ErroValidacao) as e:
        bancos.atualizar(sessao_real, None, baixar=explodir)

    assert "Banco Central" in str(e.value)
    assert bancos.estado(sessao_real)["oficial"] is False
    assert bancos.nome("237", sessao_real) == "Bradesco"


@pytest.mark.banco
def test_resposta_curta_demais_e_recusada(sessao_real):
    """Duas linhas onde deviam vir centenas é sinal de página de erro, não de
    lista. Substituir a lista boa por isso seria pior que não atualizar."""
    with pytest.raises(ErroValidacao) as e:
        bancos.atualizar(sessao_real, None, baixar=lambda: CSV_BCB)

    assert "não reconheci" in str(e.value)
    assert bancos.estado(sessao_real)["oficial"] is False


@pytest.mark.banco
def test_lista_guardada_ilegivel_cai_para_a_embutida(sessao_real):
    from app.apps.erp.db.models.cadastros import Parametro
    sessao_real.add(Parametro(chave=bancos.CHAVE_PARAMETRO, valor="{isto não é json"))
    sessao_real.flush()

    assert bancos.nome("237", sessao_real) == "Bradesco"


# ---------------------------------------------------------------------------
# O cadastro da conta em si
# ---------------------------------------------------------------------------
@pytest.mark.banco
def test_a_conta_nasce_com_a_chave_pix_junto(sessao_real):
    from app.apps.erp.core.cadastros import contas as svc
    from app.apps.erp.db.models.cadastros import ContaBancaria

    conta = svc.criar(sessao_real, {
        "descricao": "Bradesco BWS principal", "banco_codigo": "237",
        "agencia": "1234", "conta": "56789-0",
        "pix_tipo": "CNPJ", "pix_chave": "11222333000181",
        "pix_descricao": "recebimento de medição"})
    sessao_real.flush()

    guardada = sessao_real.get(ContaBancaria, conta.id)
    assert guardada.banco_codigo == "237"
    assert [k.chave for k in guardada.chaves_pix] == ["11222333000181"]


@pytest.mark.banco
def test_o_codigo_do_banco_e_normalizado_ao_cadastrar(sessao_real):
    from app.apps.erp.core.cadastros import contas as svc
    conta = svc.criar(sessao_real, {"descricao": "BB", "banco_codigo": "1",
                                    "agencia": "1", "conta": "2"})
    assert conta.banco_codigo == "001"


@pytest.mark.banco
def test_conta_sem_banco_e_recusada_dizendo_o_que_falta(sessao_real):
    from app.apps.erp.core.cadastros import contas as svc
    with pytest.raises(ErroValidacao) as e:
        svc.criar(sessao_real, {"descricao": "X", "agencia": "1", "conta": "2"})

    assert "o banco" in str(e.value)


@pytest.mark.banco
def test_a_listagem_de_contas_diz_o_nome_do_banco(sessao_real):
    from app.apps.erp.core.cadastros import contas as svc
    svc.criar(sessao_real, {"descricao": "Bradesco", "banco_codigo": "237",
                            "agencia": "1", "conta": "2"})
    sessao_real.flush()

    linha = svc.listar(sessao_real)[0]
    assert linha["banco_codigo"] == "237"
    assert linha["banco_nome"] == "Bradesco"
