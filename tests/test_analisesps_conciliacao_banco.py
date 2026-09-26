# -*- coding: utf-8 -*-
"""
A CONCILIAÇÃO BANCÁRIA, com banco de verdade — 24/09/2026.

⚠️ ESTES TESTES SÃO OS QUE IMPORTAM NESTA TELA. O dublê da suíte ignora
`WHERE`, `GROUP BY` e índice único — e a conciliação é feita ESSENCIALMENTE
dessas três coisas: o filtro da barra, a soma do saldo e a trava que impede a
mesma linha de entrar duas vezes. Um erro aqui não estoura: ele duplica um
lançamento, ou some com um, e o saldo passa a divergir do banco em silêncio.
"""
import datetime as dt
from decimal import Decimal

import pytest

pytestmark = pytest.mark.banco


@pytest.fixture
def banco_conc(banco, monkeypatch):
    """Sobe o schema pelas migrações de verdade — as mesmas que o botão aplica."""
    from sqlalchemy import text

    from app.apps.analisesps import db as db_analisesps

    url = str(banco.url.render_as_string(hide_password=False))
    monkeypatch.setenv("DATABASE_URL", url)
    db_analisesps._engine = None

    pasta = __import__("pathlib").Path(db_analisesps.__file__).parent / "migracoes"
    with banco.connect() as conn:
        conn.execute(text("DROP SCHEMA IF EXISTS analisesps CASCADE"))
        for caminho in sorted(pasta.glob("*.sql")):
            conn.execute(text(caminho.read_text(encoding="utf-8")))
        conn.commit()
    yield
    with banco.connect() as conn:
        conn.execute(text("DROP SCHEMA IF EXISTS analisesps CASCADE"))
        conn.commit()
    db_analisesps._engine = None


OFX_BASE = """OFXHEADER:100
<OFX><BANKMSGSRSV1><STMTTRNRS><STMTRS><CURDEF>BRL
<BANKACCTFROM><BANKID>{banco}<ACCTID>{conta}<ACCTTYPE>CHECKING</BANKACCTFROM>
<BANKTRANLIST><DTSTART>{ini}<DTEND>{fim}
{transacoes}
</BANKTRANLIST><LEDGERBAL><BALAMT>{saldo}<DTASOF>{fim}</LEDGERBAL>
</STMTRS></STMTTRNRS></BANKMSGSRSV1></OFX>"""

TRN = ("<STMTTRN><TRNTYPE>{tipo}<DTPOSTED>{data}<TRNAMT>{valor}"
       "{fitid}<MEMO>{memo}</STMTTRN>")


def ofx(transacoes, banco="237", conta="0007011-4", ini="20260901",
        fim="20260930", saldo="1000.00"):
    corpo = "\n".join(
        TRN.format(tipo=("CREDIT" if Decimal(t[1]) > 0 else "DEBIT"),
                   data=t[0], valor=t[1],
                   fitid=f"<FITID>{t[2]}" if len(t) > 2 and t[2] else "",
                   memo=t[3] if len(t) > 3 else "LANCAMENTO")
        for t in transacoes)
    return OFX_BASE.format(banco=banco, conta=conta, ini=ini, fim=fim,
                           saldo=saldo, transacoes=corpo).encode("utf-8")


def conta_de_teste(**extra):
    from app.apps.analisesps import conciliacao
    dados = {"nome": "BD 7011", "banco": "Bradesco", "numero": "70114",
             "ofx_bankid": "237", "ofx_acctid": "0007011-4"}
    dados.update(extra)
    return conciliacao.gravar_conta(dados, quem="TESTE")


# ---------------------------------------------------------------------------
# A migração e o cadastro
# ---------------------------------------------------------------------------
def test_a_migracao_da_conciliacao_roda_no_postgres(banco_conc):
    from app.apps.analisesps import conciliacao
    assert conciliacao.estado()["pronto"] is True


def test_o_OFX_descobre_sozinho_de_qual_conta_e(banco_conc):
    """⚠️ O pedido foi este, com todas as letras: *"eu queria que o arquivo OFX
    detectasse automático de qual conta é"*. O banco e a conta vêm dentro do
    arquivo; o cadastro guarda os dois."""
    from app.apps.analisesps import conciliacao, conciliacao_ofx
    conta_id = conta_de_teste()
    lido = conciliacao_ofx.ler(ofx([("20260910", "-100.00", "A1")]))

    achada = conciliacao.conta_do_extrato(lido.bankid, lido.acctid)
    assert achada and achada["id"] == conta_id


def test_a_conta_casa_mesmo_escrita_de_outro_jeito(banco_conc):
    """O mesmo banco escreve "0007011-4" num arquivo e "70114" no outro.
    Exigir igualdade exata mandaria o dono escolher a conta na mão — justo o
    que ele pediu para não ter de fazer."""
    from app.apps.analisesps import conciliacao
    conta_id = conta_de_teste()
    assert conciliacao.conta_do_extrato("237", "70114")["id"] == conta_id
    assert conciliacao.conta_do_extrato("0237", "00070114")["id"] == conta_id


def test_conta_desconhecida_nao_e_chutada(banco_conc):
    """⚠️ Escolher a conta errada joga o extrato de uma empresa dentro de
    outra conta. Na dúvida, o sistema pergunta — não adivinha."""
    from app.apps.analisesps import conciliacao
    conta_de_teste()
    assert conciliacao.conta_do_extrato("341", "9999") is None


# ---------------------------------------------------------------------------
# Conferir e importar
# ---------------------------------------------------------------------------
def test_conferir_diz_o_que_e_novo_SEM_gravar(banco_conc):
    """*"Eu jogar um OFX e o sistema me dizer: todos os lançamentos já estavam
    registrados."* Conferir não pode mudar o mundo que está descrevendo."""
    from app.apps.analisesps import conciliacao, conciliacao_ofx
    conta_id = conta_de_teste()
    lido = conciliacao_ofx.ler(ofx([("20260910", "-100.00", "A1"),
                                    ("20260911", "200.00", "A2")]))

    antes = conciliacao.conferir(conta_id, lido)
    assert len(antes["novas"]) == 2 and antes["ja_estavam"] == 0
    # E nada entrou.
    assert conciliacao.resumo({"conta_id": conta_id})["quantidade"] == 0

    conciliacao.importar(conta_id, lido, "setembro.ofx", "TESTE")
    depois = conciliacao.conferir(conta_id, lido)
    assert depois["novas"] == [] and depois["ja_estavam"] == 2


def test_reimportar_o_mesmo_extrato_nao_duplica(banco_conc):
    """A trava é o índice único do banco, não a conferência — duas pessoas
    soltando o mesmo arquivo no mesmo segundo passam pela conferência juntas."""
    from app.apps.analisesps import conciliacao, conciliacao_ofx
    conta_id = conta_de_teste()
    arquivo = ofx([("20260910", "-100.00", "A1"), ("20260911", "200.00", "A2")])

    conciliacao.importar(conta_id, conciliacao_ofx.ler(arquivo), "x.ofx", "T")
    segunda = conciliacao.importar(conta_id, conciliacao_ofx.ler(arquivo),
                                   "x.ofx", "T")

    assert segunda["gravadas"] == 0
    assert conciliacao.resumo({"conta_id": conta_id})["quantidade"] == 2


def test_extrato_que_cobre_periodo_maior_traz_SO_o_que_falta(banco_conc):
    """⚠️ É o caso do dono: *"nesse OFX aqui tinha um lançamento no dia tal que
    não estava lançado"*. O arquivo novo cobre o mesmo período e mais um dia."""
    from app.apps.analisesps import conciliacao, conciliacao_ofx
    conta_id = conta_de_teste()
    conciliacao.importar(
        conta_id, conciliacao_ofx.ler(ofx([("20260910", "-100.00", "A1")])),
        "parcial.ofx", "T")

    completo = conciliacao_ofx.ler(ofx([("20260910", "-100.00", "A1"),
                                        ("20260915", "-77.00", "A2")]))
    resultado = conciliacao.importar(conta_id, completo, "completo.ofx", "T")

    assert resultado["gravadas"] == 1
    assert resultado["ja_estavam"] == 1
    assert conciliacao.resumo({"conta_id": conta_id})["quantidade"] == 2


def test_dois_pagamentos_iguais_no_mesmo_dia_sao_DOIS(banco_conc):
    """⚠️ A armadilha que custou caro no ERP (11/09/2026): sem FITID, dois PIX
    de R$ 1.500 no mesmo dia para o mesmo favorecido viravam UM, e o extrato
    passava a divergir do banco em silêncio."""
    from app.apps.analisesps import conciliacao, conciliacao_ofx
    conta_id = conta_de_teste()
    arquivo = ofx([("20260910", "-1500.00", "", "PIX FULANO"),
                   ("20260910", "-1500.00", "", "PIX FULANO")])

    conciliacao.importar(conta_id, conciliacao_ofx.ler(arquivo), "x.ofx", "T")

    resumo = conciliacao.resumo({"conta_id": conta_id})
    assert resumo["quantidade"] == 2
    assert resumo["saldo"] == Decimal("-3000.00")
    # E reimportar continua não duplicando.
    conciliacao.importar(conta_id, conciliacao_ofx.ler(arquivo), "x.ofx", "T")
    assert conciliacao.resumo({"conta_id": conta_id})["quantidade"] == 2


def test_o_arquivo_identico_reenviado_e_reconhecido(banco_conc):
    """Antes de falar de linha, o sistema sabe dizer "este arquivo já veio"."""
    from app.apps.analisesps import conciliacao, conciliacao_ofx
    conta_id = conta_de_teste()
    arquivo = ofx([("20260910", "-100.00", "A1")])
    conciliacao.importar(conta_id, conciliacao_ofx.ler(arquivo), "set.ofx", "T")

    de_novo = conciliacao.conferir(conta_id, conciliacao_ofx.ler(arquivo))
    assert de_novo["arquivo_repetido"]
    assert de_novo["arquivo_repetido"]["nome_arquivo"] == "set.ofx"


def test_o_que_esta_aqui_e_nao_esta_no_extrato_e_apontado(banco_conc):
    """O outro lado da dúvida: uma linha digitada errada, ou um lançamento que
    o banco estornou, aparece como "só aqui" em vez de passar batido."""
    from app.apps.analisesps import conciliacao, conciliacao_ofx
    conta_id = conta_de_teste()
    conciliacao.acrescentar_a_mao(conta_id, dt.date(2026, 9, 12), "DIGITADA",
                                  Decimal("-50.00"), quem="T")

    conferido = conciliacao.conferir(
        conta_id, conciliacao_ofx.ler(ofx([("20260910", "-100.00", "A1")])))

    assert len(conferido["so_aqui"]) == 1
    assert conferido["so_aqui"][0]["descricao"] == "DIGITADA"


# ---------------------------------------------------------------------------
# O filtro, o saldo e as marcações
# ---------------------------------------------------------------------------
def test_o_saldo_corrido_e_da_CONTA_e_nao_da_pagina(banco_conc):
    """⚠️ Somar só as linhas da tela daria um saldo que recomeça a cada
    página: um número com cara de certo e sem sentido nenhum."""
    from app.apps.analisesps import conciliacao, conciliacao_ofx
    conta_id = conta_de_teste()
    conciliacao.importar(conta_id, conciliacao_ofx.ler(ofx([
        ("20260901", "1000.00", "A1"),
        ("20260902", "-300.00", "A2"),
        ("20260903", "-200.00", "A3")])), "x.ofx", "T")

    # Filtrando SÓ a última linha, o saldo dela continua sendo o da conta.
    linhas = conciliacao.listar({"conta_id": conta_id,
                                 "data_ini": dt.date(2026, 9, 3)})
    assert len(linhas) == 1
    assert linhas[0]["saldo"] == Decimal("500.00")


def test_o_saldo_da_conta_ignora_o_filtro_de_proposito(banco_conc):
    """Saldo com filtro não é saldo — e é ele que se compara com o banco."""
    from app.apps.analisesps import conciliacao, conciliacao_ofx
    conta_id = conta_de_teste()
    conciliacao.importar(conta_id, conciliacao_ofx.ler(ofx([
        ("20260901", "1000.00", "A1"), ("20260902", "-300.00", "A2")])),
        "x.ofx", "T")

    assert conciliacao.saldo_da_conta(conta_id) == Decimal("700.00")
    assert conciliacao.saldo_da_conta(conta_id, dt.date(2026, 9, 1)) == Decimal("1000.00")


@pytest.mark.parametrize("situacao,esperado", [
    ("todos", 3), ("pendentes", 2), ("conciliados", 1), ("com_observacao", 1)])
def test_os_filtros_da_barra_recortam_o_que_devem(banco_conc, situacao,
                                                  esperado):
    from app.apps.analisesps import conciliacao, conciliacao_ofx
    conta_id = conta_de_teste()
    conciliacao.importar(conta_id, conciliacao_ofx.ler(ofx([
        ("20260901", "1000.00", "A1"), ("20260902", "-300.00", "A2"),
        ("20260903", "-200.00", "A3")])), "x.ofx", "T")
    linhas = conciliacao.listar({"conta_id": conta_id})
    conciliacao.marcar([linhas[0]["id"]], True, "T")
    conciliacao.anotar(linhas[1]["id"], "conferir com o Nilo", "T")

    achadas = conciliacao.listar({"conta_id": conta_id, "situacao": situacao})
    assert len(achadas) == esperado


def test_a_busca_alcanca_a_observacao(banco_conc):
    """A observação é o que a planilha tinha de melhor. Não poder procurar nela
    faria a anotação virar um bilhete perdido."""
    from app.apps.analisesps import conciliacao, conciliacao_ofx
    conta_id = conta_de_teste()
    conciliacao.importar(conta_id, conciliacao_ofx.ler(ofx([
        ("20260901", "1000.00", "A1"), ("20260902", "-300.00", "A2")])),
        "x.ofx", "T")
    linhas = conciliacao.listar({"conta_id": conta_id})
    conciliacao.anotar(linhas[0]["id"], "PENDENCIA do cartorio", "T")

    assert len(conciliacao.listar({"conta_id": conta_id,
                                   "busca": "cartorio"})) == 1
    assert len(conciliacao.listar({"conta_id": conta_id,
                                   "busca": "cartorio, pendencia"})) == 1
    assert len(conciliacao.listar({"conta_id": conta_id,
                                   "busca": "cartorio, nilo"})) == 0


def test_desmarcar_apaga_quem_e_quando_conciliou(banco_conc):
    """Deixar o nome de quem conciliou numa linha desmarcada faria o histórico
    afirmar o contrário do que aconteceu."""
    from app.apps.analisesps import conciliacao, conciliacao_ofx
    conta_id = conta_de_teste()
    conciliacao.importar(conta_id, conciliacao_ofx.ler(
        ofx([("20260901", "1000.00", "A1")])), "x.ofx", "T")
    linha = conciliacao.listar({"conta_id": conta_id})[0]

    conciliacao.marcar([linha["id"]], True, "MARCELO")
    marcada = conciliacao.listar({"conta_id": conta_id})[0]
    assert marcada["conciliado"] and marcada["conciliado_por"] == "MARCELO"

    conciliacao.marcar([linha["id"]], False, "MARCELO")
    solta = conciliacao.listar({"conta_id": conta_id})[0]
    assert not solta["conciliado"]
    assert solta["conciliado_por"] == "" and solta["conciliado_em"] is None


def test_o_resumo_separa_entrada_saida_e_o_que_falta(banco_conc):
    from app.apps.analisesps import conciliacao, conciliacao_ofx
    conta_id = conta_de_teste()
    conciliacao.importar(conta_id, conciliacao_ofx.ler(ofx([
        ("20260901", "1000.00", "A1"), ("20260902", "-300.00", "A2")])),
        "x.ofx", "T")

    r = conciliacao.resumo({"conta_id": conta_id})
    assert r["entradas"] == Decimal("1000.00")
    assert r["saidas"] == Decimal("-300.00")
    assert r["saldo"] == Decimal("700.00")
    assert r["pendentes"] == 2


def test_uma_conta_nao_ve_o_extrato_da_outra(banco_conc):
    """⚠️ Misturar contas numa conciliação é o erro que faz o saldo bater por
    acaso e a conferência inteira perder o sentido."""
    from app.apps.analisesps import conciliacao, conciliacao_ofx
    uma = conta_de_teste()
    outra = conta_de_teste(nome="BB 1234", ofx_bankid="001",
                           ofx_acctid="1234-5")
    conciliacao.importar(uma, conciliacao_ofx.ler(
        ofx([("20260901", "1000.00", "A1")])), "a.ofx", "T")
    conciliacao.importar(outra, conciliacao_ofx.ler(
        ofx([("20260901", "50.00", "B1")], banco="001", conta="1234-5")),
        "b.ofx", "T")

    assert conciliacao.saldo_da_conta(uma) == Decimal("1000.00")
    assert conciliacao.saldo_da_conta(outra) == Decimal("50.00")
    assert len(conciliacao.listar({"conta_id": uma})) == 1


# ---------------------------------------------------------------------------
# A PLANILHA ANTIGA, e o encontro dela com o extrato do banco
# ---------------------------------------------------------------------------
def aba_falsa(linhas=None, aba="BD 7011"):
    from decimal import Decimal as D
    return {"aba": aba, "conciliadas": 0, "descartadas": [],
            "linhas": linhas or [
                {"data": dt.date(2026, 9, 10), "descricao": "PIX FULANO",
                 "documento": "946047", "valor": D("-1500.00"),
                 "conciliado": True, "observacao": "Obs. 1: conferir"},
                {"data": dt.date(2026, 9, 11), "descricao": "TED RECEBIDA",
                 "documento": "", "valor": D("2000.00"),
                 "conciliado": False, "observacao": ""}]}


def test_a_planilha_traz_a_marca_e_a_anotacao_junto(banco_conc):
    """⚠️ Trazer só os números e deixar o dono remarcar dois anos de
    conciliação tornaria a importação inútil."""
    from app.apps.analisesps import conciliacao
    conta_id = conta_de_teste()

    feito = conciliacao.importar_da_planilha(conta_id, aba_falsa(), "TESTE")
    assert feito["gravadas"] == 2

    linhas = {l["descricao"]: l for l in conciliacao.listar({"conta_id": conta_id})}
    assert linhas["PIX FULANO"]["conciliado"] is True
    assert linhas["PIX FULANO"]["observacao"] == "Obs. 1: conferir"
    assert linhas["PIX FULANO"]["origem"] == "planilha"
    assert linhas["TED RECEBIDA"]["conciliado"] is False


def test_reimportar_a_mesma_aba_nao_duplica(banco_conc):
    from app.apps.analisesps import conciliacao
    conta_id = conta_de_teste()
    conciliacao.importar_da_planilha(conta_id, aba_falsa(), "T")
    de_novo = conciliacao.importar_da_planilha(conta_id, aba_falsa(), "T")

    assert de_novo["gravadas"] == 0
    assert de_novo["repetidas"] == 2
    assert conciliacao.resumo({"conta_id": conta_id})["quantidade"] == 2


def test_dois_pagamentos_iguais_no_mesmo_dia_na_PLANILHA_sao_dois(banco_conc):
    from app.apps.analisesps import conciliacao
    from decimal import Decimal as D
    conta_id = conta_de_teste()
    duas = [{"data": dt.date(2026, 9, 10), "descricao": "PIX FULANO",
             "documento": "", "valor": D("-1500.00"), "conciliado": False,
             "observacao": ""}] * 2

    conciliacao.importar_da_planilha(conta_id, aba_falsa(duas), "T")
    assert conciliacao.resumo({"conta_id": conta_id})["quantidade"] == 2


def test_o_OFX_ADOTA_a_linha_da_planilha_em_vez_de_duplicar(banco_conc):
    """⚠️ O PROBLEMA QUE SÓ APARECE NA SEGUNDA SEMANA. O dono importa a
    planilha (anos de histórico anotado) e depois solta um OFX do mesmo
    período. As duas linhas são o MESMO lançamento — a do banco tem FITID, a
    da planilha não. Sem isto, o extrato duplicaria inteiro, e a cópia nova
    viria SEM a anotação dele."""
    from app.apps.analisesps import conciliacao, conciliacao_ofx
    conta_id = conta_de_teste()
    conciliacao.importar_da_planilha(conta_id, aba_falsa(), "T")

    extrato = ofx([("20260910", "-1500.00", "A1", "PIX DES: FULANO"),
                   ("20260911", "2000.00", "A2", "TED RECEBIDA")])
    feito = conciliacao.importar(conta_id, conciliacao_ofx.ler(extrato),
                                 "set.ofx", "T")

    assert feito["adotadas"] == 2
    assert feito["gravadas"] == 0
    # Continuam duas linhas, e a anotação e a marca sobreviveram.
    linhas = conciliacao.listar({"conta_id": conta_id})
    assert len(linhas) == 2
    anotada = [l for l in linhas if l["observacao"]][0]
    assert anotada["observacao"] == "Obs. 1: conferir"
    assert anotada["conciliado"] is True


def test_depois_de_adotada_a_linha_nao_e_adotada_de_novo(banco_conc):
    """Um segundo OFX com outro lançamento igual em data e valor não pode
    roubar a linha que o primeiro já casou."""
    from app.apps.analisesps import conciliacao, conciliacao_ofx
    conta_id = conta_de_teste()
    conciliacao.importar_da_planilha(conta_id, aba_falsa(), "T")
    conciliacao.importar(conta_id, conciliacao_ofx.ler(
        ofx([("20260910", "-1500.00", "A1")])), "um.ofx", "T")

    segundo = conciliacao.importar(conta_id, conciliacao_ofx.ler(
        ofx([("20260910", "-1500.00", "B9")])), "dois.ofx", "T")

    assert segundo["adotadas"] == 0
    assert segundo["gravadas"] == 1
    assert conciliacao.resumo({"conta_id": conta_id})["quantidade"] == 3


def test_reimportar_PREENCHE_a_observacao_que_faltou(banco_conc):
    """⚠️ A primeira importação de verdade trouxe as linhas SEM observação (um
    defeito de leitura, corrigido em 24/09/2026). Sem isto, o dono teria de
    apagar tudo e recomeçar para recuperar dois anos de anotação."""
    from app.apps.analisesps import conciliacao
    from decimal import Decimal as D
    conta_id = conta_de_teste()

    sem_obs = aba_falsa([{"data": dt.date(2026, 9, 10), "descricao": "PIX",
                          "documento": "", "valor": D("-100.00"),
                          "conciliado": False, "observacao": ""}])
    conciliacao.importar_da_planilha(conta_id, sem_obs, "T")

    com_obs = aba_falsa([{"data": dt.date(2026, 9, 10), "descricao": "PIX",
                          "documento": "946047", "valor": D("-100.00"),
                          "conciliado": True,
                          "observacao": "coluna H: falta nota"}])
    de_novo = conciliacao.importar_da_planilha(conta_id, com_obs, "T")

    assert de_novo["gravadas"] == 0        # não é linha nova
    assert de_novo["repetidas"] == 1
    linha = conciliacao.listar({"conta_id": conta_id})[0]
    assert linha["observacao"] == "coluna H: falta nota"
    assert linha["conciliado"] is True


def test_reimportar_NAO_APAGA_o_que_foi_escrito_no_sistema(banco_conc):
    """⚠️ A observação que ele escreveu AQUI vale mais que a da planilha, e
    desmarcar o que ele conferiu no sistema porque a planilha está atrasada
    seria pior do que não importar nada."""
    from app.apps.analisesps import conciliacao
    from decimal import Decimal as D
    conta_id = conta_de_teste()
    aba = aba_falsa([{"data": dt.date(2026, 9, 10), "descricao": "PIX",
                      "documento": "", "valor": D("-100.00"),
                      "conciliado": False, "observacao": "da planilha"}])
    conciliacao.importar_da_planilha(conta_id, aba, "T")

    linha = conciliacao.listar({"conta_id": conta_id})[0]
    conciliacao.anotar(linha["id"], "o que EU escrevi aqui", "MARCELO")
    conciliacao.marcar([linha["id"]], True, "MARCELO")

    conciliacao.importar_da_planilha(conta_id, aba, "T")

    depois = conciliacao.listar({"conta_id": conta_id})[0]
    assert depois["observacao"] == "o que EU escrevi aqui"
    assert depois["conciliado"] is True          # a planilha não desmarcou


# ---------------------------------------------------------------------------
# O SALDO INICIAL — por que o saldo "não batia"
# ---------------------------------------------------------------------------
def test_o_saldo_comeca_no_saldo_inicial_da_conta(banco_conc):
    """⚠️ O extrato importado começa num dia qualquer — o dia em que a planilha
    dele começou. Tudo o que a conta movimentou antes disso não existe aqui, e
    o saldo ficava errado exatamente do tamanho do que veio antes. Foi o que
    ele viu: *"o saldo não está batendo de uma determinada conta"*."""
    from app.apps.analisesps import conciliacao, conciliacao_ofx
    from decimal import Decimal as D
    conta_id = conta_de_teste(saldo_inicial="100.000,00",
                              saldo_inicial_em="31/08/2026")
    conciliacao.importar(conta_id, conciliacao_ofx.ler(ofx([
        ("20260901", "-1000.00", "A1"), ("20260902", "500.00", "A2")])),
        "x.ofx", "T")

    assert conciliacao.saldo_da_conta(conta_id) == D("99500.00")


def test_o_que_e_anterior_a_data_do_saldo_inicial_NAO_conta_duas_vezes(banco_conc):
    """⚠️ "No dia 31/08 a conta tinha 100 mil" quer dizer o saldo NO FIM
    daquele dia. Somar de novo os lançamentos daquele dia contaria o mesmo
    dinheiro duas vezes — e o erro seria silencioso."""
    from app.apps.analisesps import conciliacao, conciliacao_ofx
    from decimal import Decimal as D
    conta_id = conta_de_teste(saldo_inicial="100.000,00",
                              saldo_inicial_em="31/08/2026")
    conciliacao.importar(conta_id, conciliacao_ofx.ler(ofx([
        ("20260830", "-9999.00", "VELHO"),   # antes: já está no saldo inicial
        ("20260831", "-8888.00", "DIA"),     # no dia: idem
        ("20260901", "-1000.00", "A1")])), "x.ofx", "T")

    assert conciliacao.saldo_da_conta(conta_id) == D("99000.00")


def test_a_coluna_saldo_da_lista_concorda_com_o_numero_do_topo(banco_conc):
    """Se discordassem, não haveria como saber em qual acreditar."""
    from app.apps.analisesps import conciliacao, conciliacao_ofx
    conta_id = conta_de_teste(saldo_inicial="100.000,00",
                              saldo_inicial_em="31/08/2026")
    conciliacao.importar(conta_id, conciliacao_ofx.ler(ofx([
        ("20260901", "-1000.00", "A1"), ("20260902", "500.00", "A2")])),
        "x.ofx", "T")

    linhas = conciliacao.listar({"conta_id": conta_id})   # mais nova primeiro
    assert linhas[0]["saldo"] == conciliacao.saldo_da_conta(conta_id)


def test_sem_saldo_inicial_nada_muda(banco_conc):
    """Quem não usar o campo continua com o comportamento de antes."""
    from app.apps.analisesps import conciliacao, conciliacao_ofx
    from decimal import Decimal as D
    conta_id = conta_de_teste()
    conciliacao.importar(conta_id, conciliacao_ofx.ler(
        ofx([("20260901", "-1000.00", "A1")])), "x.ofx", "T")
    assert conciliacao.saldo_da_conta(conta_id) == D("-1000.00")


# ---------------------------------------------------------------------------
# O PANORAMA — "no que eu não posso confiar"
# ---------------------------------------------------------------------------
def test_o_panorama_acha_o_BURACO_no_meio_do_extrato(banco_conc):
    """⚠️ É o coração da tela. Um mês sem lançamento ENTRE dois que têm quer
    dizer que o extrato pulou um pedaço — e o saldo dali para a frente está
    errado **sem ninguém saber**. O dono: *"está faltando os meses tais e
    tais"*."""
    from app.apps.analisesps import conciliacao, conciliacao_ofx
    conta_id = conta_de_teste()
    conciliacao.importar(conta_id, conciliacao_ofx.ler(ofx([
        ("20250115", "-100.00", "A1"),
        ("20250320", "-200.00", "A2")])), "x.ofx", "T")   # fevereiro falta

    linha = conciliacao.panorama(2025)["contas"][0]
    assert linha["buracos"] == [2]
    assert linha["buracos_nome"] == "fev"
    assert linha["recado"]["grau"] == "ruim"
    assert "fev" in linha["recado"]["texto"]


def test_buraco_e_MES_QUE_AINDA_NAO_VEIO_sao_coisas_diferentes(banco_conc):
    """⚠️ São problemas diferentes e o operador faz coisas diferentes com cada
    um. Misturá-los num "faltam 4 meses" esconderia o que importa."""
    from app.apps.analisesps import conciliacao, conciliacao_ofx
    conta_id = conta_de_teste()
    conciliacao.importar(conta_id, conciliacao_ofx.ler(ofx([
        ("20200115", "-100.00", "A1"),
        ("20200320", "-200.00", "A2")])), "x.ofx", "T")

    linha = conciliacao.panorama(2020)["contas"][0]
    assert linha["buracos"] == [2]                 # entre janeiro e março
    assert linha["nao_vieram"] == list(range(4, 13))   # depois de março


def test_o_atraso_e_medido_pelo_ULTIMO_LANCAMENTO_e_nao_pela_importacao(
        banco_conc):
    """⚠️ Importar hoje um extrato velho deixaria "importado há 0 dias" numa
    conta que continua sem o mês passado — a tela estaria mentindo."""
    from app.apps.analisesps import conciliacao, conciliacao_ofx
    conta_id = conta_de_teste()
    conciliacao.importar(conta_id, conciliacao_ofx.ler(
        ofx([("20200115", "-100.00", "A1")])), "velho.ofx", "T")

    linha = conciliacao.panorama(2020)["contas"][0]
    assert linha["dias_sem_importar"] == 0          # importado agora
    assert linha["dias_sem_extrato"] > 1000         # mas o extrato é de 2020
    assert linha["recado"]["grau"] == "ruim"


def test_importado_agora_nunca_aparece_como_dias_NEGATIVOS(banco_conc):
    """⚠️ ESTE TESTE PEGOU UM DEFEITO DE VERDADE, e só entre 21h e meia-noite.

    A hora guardada pelo banco é UTC; a data da tela é de Brasília. Depois das
    21h (UTC-3), em UTC já é o dia seguinte — e a subtração dava **-1**, com a
    tela dizendo "importado há -1 dias". Quem lesse isso perderia a confiança
    no painel inteiro, e o defeito desaparecia sozinho de manhã.
    """
    from app.apps.analisesps import conciliacao, conciliacao_ofx
    conta_id = conta_de_teste()
    conciliacao.importar(conta_id, conciliacao_ofx.ler(
        ofx([("20200115", "-100.00", "A1")])), "velho.ofx", "T")

    linha = conciliacao.panorama(2020)["contas"][0]
    assert linha["dias_sem_importar"] >= 0
    assert linha["dias_sem_extrato"] >= 0


def test_o_recado_diz_UMA_coisa_so_e_a_mais_urgente(banco_conc):
    """Listar tudo o que está imperfeito em cada conta faria a tela virar um
    mural que ninguém lê. A ordem é a do estrago."""
    from app.apps.analisesps import conciliacao, conciliacao_ofx
    conta_id = conta_de_teste()
    # Tem buraco E tem pendente: o buraco manda.
    conciliacao.importar(conta_id, conciliacao_ofx.ler(ofx([
        ("20250115", "-100.00", "A1"),
        ("20250320", "-200.00", "A2")])), "x.ofx", "T")

    recado = conciliacao.panorama(2025)["contas"][0]["recado"]
    assert "faltam os meses" in recado["texto"]
    assert "por conciliar" not in recado["texto"]


def test_conta_em_dia_diz_que_esta_em_dia(banco_conc):
    from app.apps.analisesps import conciliacao, conciliacao_ofx
    from app.apps.analisesps.horario import agora
    hoje = agora().date()
    conta_id = conta_de_teste(saldo_inicial="1.000,00",
                              saldo_inicial_em="01/01/2020")
    conciliacao.importar(conta_id, conciliacao_ofx.ler(ofx([
        (hoje.strftime("%Y%m%d"), "-100.00", "A1")])), "hoje.ofx", "T")
    linhas = conciliacao.listar({"conta_id": conta_id})
    conciliacao.marcar([l["id"] for l in linhas], True, "T")

    linha = conciliacao.panorama(hoje.year)["contas"][0]
    assert linha["recado"]["grau"] == "bom"
    assert linha["recado"]["texto"] == "em dia"


def test_o_panorama_separa_entrada_saida_e_o_que_falta_por_conta(banco_conc):
    from app.apps.analisesps import conciliacao, conciliacao_ofx
    from decimal import Decimal as D
    conta_id = conta_de_teste()
    conciliacao.importar(conta_id, conciliacao_ofx.ler(ofx([
        ("20250115", "1000.00", "A1"), ("20250116", "-300.00", "A2")])),
        "x.ofx", "T")

    linha = conciliacao.panorama(2025)["contas"][0]
    assert linha["entradas"] == D("1000.00")
    assert linha["saidas"] == D("-300.00")
    assert linha["movimento"] == D("1300.00")
    assert linha["pendentes"] == 2
    assert linha["por_cento"] == 0


def test_o_resumo_conta_quantas_contas_precisam_de_alguem(banco_conc):
    from app.apps.analisesps import conciliacao, conciliacao_ofx
    uma = conta_de_teste()
    outra = conta_de_teste(nome="BB 1234", ofx_bankid="001", ofx_acctid="1234-5")
    conciliacao.importar(uma, conciliacao_ofx.ler(ofx([
        ("20250115", "-100.00", "A1"), ("20250320", "-200.00", "A2")])),
        "a.ofx", "T")
    conciliacao.importar(outra, conciliacao_ofx.ler(ofx(
        [("20250115", "-50.00", "B1")], banco="001", conta="1234-5")),
        "b.ofx", "T")

    resumo = conciliacao.panorama(2025)["resumo"]
    assert resumo["contas"] == 2
    assert resumo["com_buraco"] == 1
    assert resumo["sem_saldo_inicial"] == 2


def test_os_anos_com_movimento_saem_do_banco(banco_conc):
    from app.apps.analisesps import conciliacao, conciliacao_ofx
    conta_id = conta_de_teste()
    conciliacao.importar(conta_id, conciliacao_ofx.ler(ofx([
        ("20240115", "-100.00", "A1"), ("20250320", "-200.00", "A2")])),
        "x.ofx", "T")
    assert conciliacao.anos_com_movimento() == [2025, 2024]


def test_a_conta_corrente_do_OMIE_e_guardada_e_lida(banco_conc):
    """⚠️ Sem ela nada é lançado — e ela mora na CONTA, nunca no tipo. Lançar
    uma tarifa do Bradesco dentro da conta do Santander é o erro mais caro
    possível aqui."""
    from app.apps.analisesps import conciliacao
    conta_id = conta_de_teste(omie_conta_corrente="  1234567 ")

    conta = [c for c in conciliacao.contas() if c["id"] == conta_id][0]
    assert conta["omie_conta_corrente"] == 1234567

    conciliacao.gravar_conta({**conta, "omie_conta_corrente": "999"}, "T")
    de_novo = [c for c in conciliacao.contas() if c["id"] == conta_id][0]
    assert de_novo["omie_conta_corrente"] == 999


def test_lancar_no_OMIE_marca_a_linha_e_nao_deixa_lancar_de_novo(banco_conc):
    """⚠️ A trava de verdade é o OMIE recusar o código de integração repetido.
    Esta é a primeira: a linha já lançada nem chega a ser oferecida."""
    from app.apps.analisesps import (conciliacao, conciliacao_ofx,
                                     conciliacao_omie)
    conta_id = conta_de_teste(omie_conta_corrente="1234567")
    conciliacao.importar(conta_id, conciliacao_ofx.ler(
        ofx([("20260910", "-9.00", "A1", "TARIFA BANCARIA")])), "x.ofx", "T")
    linha = conciliacao.listar({"conta_id": conta_id})[0]
    conta = [c for c in conciliacao.contas() if c["id"] == conta_id][0]

    tipo_id = conciliacao_omie.gravar_tipo(
        {"nome": "Tarifa", "palavras": "TARIFA",
         "codigo_categoria": "2.01.05", "codigo_cliente": 111}, "T")

    class ClienteFalso:
        def __init__(self):
            self.chamadas = []

        def _call(self, url, acao, param):
            self.chamadas.append((acao, param))
            return {"codigo_lancamento_omie": 555}

    cli = ClienteFalso()
    plano = conciliacao_omie.planejar([linha], conta)
    assert len(plano["vai"]) == 1
    feito = conciliacao_omie.lancar(plano["vai"], "MARCELO", cli)

    assert feito["gravados"] == 1
    assert [a for a, _ in cli.chamadas] == ["IncluirContaPagar",
                                            "LancarPagamento"]

    # A linha ficou marcada, e um novo plano não a oferece mais.
    de_novo = conciliacao.listar({"conta_id": conta_id})[0]
    campos = conciliacao_omie.planejar([{**de_novo, "omie_codigo": 555}], conta)
    assert campos["vai"] == []
    assert "já foi lançada" in campos["nao_vai"][0]["motivo"]
    assert tipo_id


def test_o_titulo_criado_com_baixa_falhando_VIRA_PENDENCIA(banco_conc):
    """⚠️ Um título criado no OMIE cuja baixa falhou fica lá em aberto,
    dizendo que há algo a pagar que já foi pago — e ninguém descobre isso
    olhando o extrato daqui."""
    from app.apps.analisesps import (conciliacao, conciliacao_ofx,
                                     conciliacao_omie)
    conta_id = conta_de_teste(omie_conta_corrente="1234567")
    conciliacao.importar(conta_id, conciliacao_ofx.ler(
        ofx([("20260910", "-9.00", "A1", "TARIFA BANCARIA")])), "x.ofx", "T")
    linha = conciliacao.listar({"conta_id": conta_id})[0]
    conta = [c for c in conciliacao.contas() if c["id"] == conta_id][0]
    conciliacao_omie.gravar_tipo(
        {"nome": "Tarifa", "palavras": "TARIFA",
         "codigo_categoria": "2.01.05", "codigo_cliente": 111}, "T")

    class ClienteQueFalhaNaBaixa:
        def _call(self, url, acao, param):
            if "Lancar" in acao:
                raise RuntimeError("conta corrente bloqueada")
            return {"codigo_lancamento_omie": 777}

    plano = conciliacao_omie.planejar([linha], conta)
    feito = conciliacao_omie.lancar(plano["vai"], "T", ClienteQueFalhaNaBaixa())

    assert feito["gravados"] == 1          # o título entrou
    assert len(feito["falhas"]) == 1       # e a baixa não
    assert "FOI CRIADO" in feito["falhas"][0]["erro"]

    pendentes = conciliacao_omie.pendencias()
    assert len(pendentes) == 1
    assert pendentes[0]["omie_situacao"] == "sem_baixa"
    assert pendentes[0]["omie_codigo"] == 777


@pytest.mark.parametrize("filtro,esperado,porque", [
    ({"historico": "pix"}, 1, "só o que tem PIX no histórico"),
    ({"documento": "41024"}, 1, "só o do documento 41024"),
    ({"observacao": "nilo"}, 1, "só o que foi anotado"),
    ({"entrada": "1000"}, 1, "a entrada de 1.000"),
    ({"saida": "300"}, 1, "a saída de 300"),
    ({"entrada": "300"}, 0, "300 saiu, não entrou"),
    ({"historico": "pix", "observacao": "nilo"}, 0,
     "as duas caixinhas juntas, e nada casa as duas"),
])
def test_o_filtro_de_cada_COLUNA_recorta_so_a_dela(banco_conc, filtro,
                                                   esperado, porque):
    """⚠️ Procurar "pix" no histórico e procurar "pix" na observação são
    perguntas diferentes. Quem digita embaixo de um título quer aquela coluna
    — e duas caixinhas preenchidas se somam, não se substituem."""
    from app.apps.analisesps import conciliacao, conciliacao_ofx
    conta_id = conta_de_teste()
    conciliacao.importar(conta_id, conciliacao_ofx.ler(ofx([
        ("20260901", "1000.00", "A1", "TED RECEBIDA"),
        ("20260902", "-300.00", "A2", "PAGAMENTO PIX FULANO")])), "x.ofx", "T")
    # ⚠️ A LISTA VEM DA MAIS NOVA PARA A MAIS VELHA, e a anotação precisa cair
    # na linha CERTA — na primeira versão deste teste ela caiu na do PIX, e o
    # caso das "duas caixinhas juntas" passou a casar uma linha. O código
    # estava certo; o dado do teste é que estava trocado.
    por_descricao = {l["descricao"]: l
                     for l in conciliacao.listar({"conta_id": conta_id})}
    da_ted = por_descricao["TED RECEBIDA"]
    from app.apps.analisesps.db import conexao
    with conexao() as con:
        con.execute("UPDATE analisesps.conciliacao_extrato SET documento='41024'"
                    " WHERE id = ?", (da_ted["id"],))
        con.commit()
    conciliacao.anotar(da_ted["id"], "conferir com o Nilo", "T")

    achadas = conciliacao.listar(dict({"conta_id": conta_id}, **filtro))
    assert len(achadas) == esperado, porque


# ---------------------------------------------------------------------------
# ⚠️ O RELATÓRIO DA CONFERÊNCIA MENTIA — 24/09/2026
#
# O dono achou com um caso concreto: *"a leitura disse que nada no extrato
# havia sido importado, mas veja: 01/09/2026 PAGTO ELETRON COBRANCA
# 1423835099 −3.313,21 — e a importação da planilha tem essa mesma linha."*
#
# As duas estavam certas e a CONFERÊNCIA é que errava: a linha da planilha tem
# identidade própria (sem FITID), a do OFX tem outra, e olhando só a
# identidade a conferência via "não existe" e contava como NOVA — quando na
# gravação ela seria ADOTADA, não criada.
#
# O resultado final estava certo; o número estava errado. Mas um relatório que
# diz "47 novos" e grava 3 destrói a confiança na tela inteira — e é esta tela
# que existe para responder "o que falta importar?".
#
# ⚠️ E AQUI ELE TAMBÉM ME CORRIGIU: eu cheguei a trocar o casamento para
# "valor em módulo, e o banco decide o sinal", achando que havia erro de
# sinal. *"A do sistema está no canto certo e está em vermelho, é débito."* O
# que ele viu positivo era a coluna SAÍDA da tela, que mostra sem o sinal de
# propósito. A troca foi desfeita — casar por módulo faria o OFX de uma saída
# adotar uma ENTRADA de mesmo valor no mesmo dia e virar o sinal dela.
# ---------------------------------------------------------------------------
def test_a_conferencia_conta_a_parte_o_que_vem_da_planilha(banco_conc):
    from app.apps.analisesps import conciliacao, conciliacao_ofx
    from decimal import Decimal as D
    conta_id = conta_de_teste()
    conciliacao.importar_da_planilha(conta_id, aba_falsa([
        {"data": dt.date(2026, 9, 1),
         "descricao": "PAGTO ELETRON COBRANCA 1423835099", "documento": "",
         "valor": D("-3313.21"), "conciliado": True,
         "observacao": "conferir"}]), "T")

    conferido = conciliacao.conferir(conta_id, conciliacao_ofx.ler(ofx([
        ("20260901", "-3313.21", "A1", "PAGTO ELETRON COBRANCA 1423835099"),
        ("20260902", "-50.00", "A2", "OUTRA COISA")])))

    assert conferido["adotaveis"] == 1
    assert len(conferido["novas"]) == 1        # só a de 02/09
    assert conferido["ja_estavam"] == 0


def test_a_linha_ADOTADA_nao_aparece_como_sumida_do_extrato(banco_conc):
    """⚠️ A MESMA LINHA NOS DOIS LUGARES — achado pelo dono em 25/09/2026.

    Ele soltou um OFX e leu, no mesmo relatório:

        "Outros 110 já estão aqui vindos da planilha"
        "Atenção: 110 linha(s) que estão aqui e NÃO vêm neste extrato"

    com a MESMA linha nas duas listas (PAGAMENTO PIX CEDISA de 11/09, entre
    outras). As duas usavam réguas diferentes para a mesma pergunta: a
    primeira casa por data e valor; a segunda comparava pela IMPRESSÃO, que é
    a identidade do arquivo do banco — e a linha vinda da planilha nunca casa
    com nenhuma impressão do OFX.

    O estrago não é o número: é que esta lista existe para acusar linha
    digitada errada ou lançamento estornado, e afogar as poucas de verdade no
    meio de centenas de falsas faz ninguém ler a lista de novo.
    """
    from app.apps.analisesps import conciliacao, conciliacao_ofx
    from decimal import Decimal as D
    conta_id = conta_de_teste()
    conciliacao.importar_da_planilha(conta_id, aba_falsa([
        # esta vem no extrato: vai ser ADOTADA, e não pode constar como sumida
        {"data": dt.date(2026, 9, 11), "descricao": "PAGAMENTO PIX CEDISA",
         "documento": "", "valor": D("-26872.32"), "conciliado": True,
         "observacao": ""},
        # esta NÃO vem no extrato: é a única que a lista deve acusar
        {"data": dt.date(2026, 9, 11), "descricao": "DIGITADA ERRADA",
         "documento": "", "valor": D("-999.99"), "conciliado": False,
         "observacao": ""}]), "T")

    conferido = conciliacao.conferir(conta_id, conciliacao_ofx.ler(ofx(
        [("20260911", "-26872.32", "A1", "PAGAMENTO PIX 27244680000145 CEDISA")],
        ini="20260910", fim="20260925")))

    assert conferido["adotaveis"] == 1
    descricoes = [l["descricao"] for l in conferido["so_aqui"]]
    assert "PAGAMENTO PIX CEDISA" not in descricoes, (
        "a linha que vai ser adotada não pode aparecer como sumida do extrato")
    assert descricoes == ["DIGITADA ERRADA"]


def test_so_aqui_continua_acusando_o_que_de_fato_sumiu(banco_conc):
    """O outro lado: consertar o falso positivo não pode ter apagado a lista.
    Ela é o que acusa linha digitada errada e lançamento estornado."""
    from app.apps.analisesps import conciliacao, conciliacao_ofx
    from decimal import Decimal as D
    conta_id = conta_de_teste()
    conciliacao.importar_da_planilha(conta_id, aba_falsa([
        {"data": dt.date(2026, 9, 11), "descricao": "SO NA PLANILHA",
         "documento": "", "valor": D("-123.45"), "conciliado": False,
         "observacao": ""}]), "T")

    conferido = conciliacao.conferir(conta_id, conciliacao_ofx.ler(ofx(
        [("20260912", "-500.00", "B1", "OUTRA COISA")],
        ini="20260910", fim="20260925")))

    assert [l["descricao"] for l in conferido["so_aqui"]] == ["SO NA PLANILHA"]


def test_com_DUAS_iguais_e_o_extrato_trazendo_UMA_a_outra_e_acusada(banco_conc):
    """A régua fina: cada linha da planilha casa uma vez, então a segunda
    continua sendo cobrada — e é a linha CERTA que sobra na lista."""
    from app.apps.analisesps import conciliacao, conciliacao_ofx
    from decimal import Decimal as D
    conta_id = conta_de_teste()
    conciliacao.importar_da_planilha(conta_id, aba_falsa([
        {"data": dt.date(2026, 9, 11), "descricao": "PIX REPETIDO",
         "documento": "", "valor": D("-100.00"), "conciliado": False,
         "observacao": ""},
        {"data": dt.date(2026, 9, 11), "descricao": "PIX REPETIDO",
         "documento": "", "valor": D("-100.00"), "conciliado": False,
         "observacao": ""}]), "T")

    conferido = conciliacao.conferir(conta_id, conciliacao_ofx.ler(ofx(
        [("20260911", "-100.00", "C1", "PIX REPETIDO")],
        ini="20260910", fim="20260925")))

    assert conferido["adotaveis"] == 1
    assert len(conferido["so_aqui"]) == 1, (
        "uma casou e some da lista; a outra continua sendo cobrada")


def test_o_que_a_conferencia_promete_e_o_que_a_gravacao_faz(banco_conc):
    """⚠️ A prova de que o relatório não mente mais: o número que ele lê antes
    tem de ser o que acontece depois."""
    from app.apps.analisesps import conciliacao, conciliacao_ofx
    from decimal import Decimal as D
    conta_id = conta_de_teste()
    conciliacao.importar_da_planilha(conta_id, aba_falsa([
        {"data": dt.date(2026, 9, 1), "descricao": "PAGTO", "documento": "",
         "valor": D("-3313.21"), "conciliado": False, "observacao": ""}]), "T")

    arquivo = ofx([("20260901", "-3313.21", "A1", "PAGTO"),
                   ("20260902", "-50.00", "A2", "OUTRA")])
    prometido = conciliacao.conferir(conta_id, conciliacao_ofx.ler(arquivo))
    feito = conciliacao.importar(conta_id, conciliacao_ofx.ler(arquivo),
                                 "x.ofx", "T")

    assert len(prometido["novas"]) == feito["gravadas"]
    assert prometido["adotaveis"] == feito["adotadas"]
    assert len(conciliacao.listar({"conta_id": conta_id})) == 2


def test_duas_linhas_iguais_com_a_planilha_tendo_UMA(banco_conc):
    """⚠️ Cada linha da planilha casa UMA vez. Dois débitos iguais no mesmo
    dia, com a planilha tendo trazido só um, têm de dar "1 adotável e 1 nova"
    — e não "2 adotáveis", que faria a conferência prometer menos do que grava.
    """
    from app.apps.analisesps import conciliacao, conciliacao_ofx
    from decimal import Decimal as D
    conta_id = conta_de_teste()
    conciliacao.importar_da_planilha(conta_id, aba_falsa([
        {"data": dt.date(2026, 9, 1), "descricao": "PIX", "documento": "",
         "valor": D("-1500.00"), "conciliado": False, "observacao": ""}]), "T")

    conferido = conciliacao.conferir(conta_id, conciliacao_ofx.ler(ofx([
        ("20260901", "-1500.00", "", "PIX FULANO"),
        ("20260901", "-1500.00", "", "PIX FULANO")])))

    assert conferido["adotaveis"] == 1
    assert len(conferido["novas"]) == 1


def test_o_casamento_e_por_valor_EXATO_e_nao_por_modulo(banco_conc):
    """⚠️ Desfiz o casamento por módulo que eu tinha acabado de fazer: com ele,
    o OFX de uma SAÍDA de 100 adotaria uma ENTRADA de 100 do mesmo dia e
    viraria o sinal dela — trocando um lançamento verdadeiro por outro, em
    silêncio."""
    from app.apps.analisesps import conciliacao, conciliacao_ofx
    from decimal import Decimal as D
    conta_id = conta_de_teste()
    conciliacao.importar_da_planilha(conta_id, aba_falsa([
        {"data": dt.date(2026, 9, 1), "descricao": "ENTRADA DE VERDADE",
         "documento": "", "valor": D("100.00"), "conciliado": False,
         "observacao": "não me toque"}]), "T")

    conciliacao.importar(conta_id, conciliacao_ofx.ler(
        ofx([("20260901", "-100.00", "A1", "SAIDA")])), "x.ofx", "T")

    linhas = {l["descricao"]: l for l in conciliacao.listar({"conta_id": conta_id})}
    assert len(linhas) == 2, "a entrada foi adotada pela saída e virou de sinal"
    assert linhas["ENTRADA DE VERDADE"]["valor"] == D("100.00")


# ---------------------------------------------------------------------------
# DESFAZER UMA IMPORTAÇÃO
#
# *"Tem que ter alguma forma de retroceder um erro, né?"*
# ---------------------------------------------------------------------------
def test_desfazer_um_OFX_tira_so_o_que_AQUELE_arquivo_trouxe(banco_conc):
    from app.apps.analisesps import conciliacao, conciliacao_ofx
    conta_id = conta_de_teste()
    primeiro = conciliacao.importar(conta_id, conciliacao_ofx.ler(
        ofx([("20260901", "-100.00", "A1")])), "um.ofx", "T")
    conciliacao.importar(conta_id, conciliacao_ofx.ler(
        ofx([("20260902", "-200.00", "B1")])), "dois.ofx", "T")

    feito = conciliacao.desfazer(conta_id, arquivo_id=primeiro["arquivo_id"],
                                 quem="MARCELO")

    assert feito["apagadas"] == 1
    restantes = conciliacao.listar({"conta_id": conta_id})
    assert len(restantes) == 1
    assert restantes[0]["valor"] == Decimal("-200.00")


def test_desfazer_NAO_apaga_o_que_ja_foi_para_o_OMIE(banco_conc):
    """⚠️ Lá fora existe um título com aquele número. Sumir com a linha daqui
    deixaria o OMIE com um lançamento que nada mais explica."""
    from app.apps.analisesps import conciliacao, conciliacao_ofx
    from app.apps.analisesps.db import conexao
    conta_id = conta_de_teste()
    feito_import = conciliacao.importar(conta_id, conciliacao_ofx.ler(ofx([
        ("20260901", "-100.00", "A1"), ("20260902", "-200.00", "A2")])),
        "x.ofx", "T")
    linha = conciliacao.listar({"conta_id": conta_id})[0]
    with conexao() as con:
        con.execute("UPDATE analisesps.conciliacao_extrato "
                    "   SET omie_codigo = 555 WHERE id = ?", (linha["id"],))
        con.commit()

    feito = conciliacao.desfazer(conta_id,
                                 arquivo_id=feito_import["arquivo_id"],
                                 quem="T")

    assert feito["apagadas"] == 1
    assert feito["ficaram_no_omie"] == 1
    restantes = conciliacao.listar({"conta_id": conta_id})
    assert len(restantes) == 1
    assert restantes[0]["id"] == linha["id"]


def test_o_desfazer_CONTA_ANTES_o_que_vai_sumir(banco_conc):
    """⚠️ Conciliado e observação são trabalho de gente. Ele decide sabendo."""
    from app.apps.analisesps import conciliacao, conciliacao_ofx
    conta_id = conta_de_teste()
    feito_import = conciliacao.importar(conta_id, conciliacao_ofx.ler(ofx([
        ("20260901", "-100.00", "A1"), ("20260902", "-200.00", "A2")])),
        "x.ofx", "T")
    linhas = conciliacao.listar({"conta_id": conta_id})
    conciliacao.marcar([linhas[0]["id"]], True, "T")
    conciliacao.anotar(linhas[1]["id"], "pendência", "T")

    antes = conciliacao.o_que_o_desfazer_apaga(
        conta_id, arquivo_id=feito_import["arquivo_id"])

    assert antes["quantas"] == 2
    assert antes["conciliadas"] == 1
    assert antes["anotadas"] == 1
    # E nada foi apagado só por perguntar.
    assert len(conciliacao.listar({"conta_id": conta_id})) == 2


def test_desfazer_a_planilha_nao_leva_o_que_veio_do_OFX(banco_conc):
    """Desfazer uma aba tira o que a planilha trouxe — não o extrato do banco."""
    from app.apps.analisesps import conciliacao, conciliacao_ofx
    from decimal import Decimal as D
    conta_id = conta_de_teste()
    conciliacao.importar_da_planilha(conta_id, aba_falsa([
        {"data": dt.date(2026, 1, 5), "descricao": "DA PLANILHA",
         "documento": "", "valor": D("-50.00"), "conciliado": False,
         "observacao": ""}]), "T")
    conciliacao.importar(conta_id, conciliacao_ofx.ler(
        ofx([("20260901", "-100.00", "A1")])), "x.ofx", "T")

    feito = conciliacao.desfazer(conta_id, aba="BD 7011", quem="T")

    assert feito["apagadas"] == 1
    restantes = conciliacao.listar({"conta_id": conta_id})
    assert len(restantes) == 1
    assert restantes[0]["origem"] == "ofx"


# ---------------------------------------------------------------------------
# O PANORAMA, SEGUNDA CAMADA — 24/09/2026
#
# *"O panorama das contas tá legal, mas eu tô achando ainda meio pobre."*
#
# ⚠️ E "mais coisa" não é mais número: total ninguém age sobre. O que entra
# aqui responde pergunta — onde o dinheiro está parado, o que está velho, quem
# está fazendo o trabalho.
# ---------------------------------------------------------------------------
def test_a_pendencia_VELHA_e_separada_da_de_ontem(banco_conc):
    """⚠️ Pendência de ontem é fila; de três meses atrás é problema. Somá-las
    num número só apagaria exatamente essa diferença."""
    from app.apps.analisesps import conciliacao, conciliacao_ofx
    from app.apps.analisesps.horario import agora
    from decimal import Decimal as D
    hoje = agora().date()
    velha = (hoje - dt.timedelta(days=200)).strftime("%Y%m%d")
    media = (hoje - dt.timedelta(days=60)).strftime("%Y%m%d")
    nova = hoje.strftime("%Y%m%d")

    conta_id = conta_de_teste()
    conciliacao.importar(conta_id, conciliacao_ofx.ler(ofx([
        (velha, "-5000.00", "A1"), (media, "-300.00", "A2"),
        (nova, "-10.00", "A3")])), "x.ofx", "T")

    fundo = conciliacao.panorama_do_ano(hoje.year)
    assert fundo["pendente_velha"] == 1
    assert fundo["pendente_velha_valor"] == D("-5000.00")
    assert fundo["pendente_media"] == 1
    assert fundo["pendente_mais_antiga"] == hoje - dt.timedelta(days=200)


def test_os_MAIORES_sem_conferencia_vem_primeiro(banco_conc):
    """⚠️ Uma pendência de R$ 200 mil não é igual a cem de R$ 2 mil. É onde o
    risco está concentrado."""
    from app.apps.analisesps import conciliacao, conciliacao_ofx
    from decimal import Decimal as D
    conta_id = conta_de_teste()
    conciliacao.importar(conta_id, conciliacao_ofx.ler(ofx([
        ("20260901", "-100.00", "A1", "PEQUENO"),
        ("20260902", "-200000.00", "A2", "GRANDE"),
        ("20260903", "50000.00", "A3", "MEDIO")])), "x.ofx", "T")

    maiores = conciliacao.panorama_do_ano(2026)["maiores_pendentes"]
    assert [m["descricao"] for m in maiores] == ["GRANDE", "MEDIO", "PEQUENO"]
    assert maiores[0]["valor"] == D("-200000.00")
    # E o que já foi conciliado sai da lista: ela é do que FALTA.
    linhas = {l["descricao"]: l for l in conciliacao.listar({"conta_id": conta_id})}
    conciliacao.marcar([linhas["GRANDE"]["id"]], True, "T")
    de_novo = conciliacao.panorama_do_ano(2026)["maiores_pendentes"]
    assert [m["descricao"] for m in de_novo] == ["MEDIO", "PEQUENO"]


def test_o_mes_a_mes_mostra_a_curva_e_nao_so_o_total(banco_conc):
    from app.apps.analisesps import conciliacao, conciliacao_ofx
    from decimal import Decimal as D
    conta_id = conta_de_teste()
    conciliacao.importar(conta_id, conciliacao_ofx.ler(ofx([
        ("20260115", "1000.00", "A1"), ("20260116", "-300.00", "A2"),
        ("20260320", "-500.00", "A3")])), "x.ofx", "T")

    meses = {m["mes"]: m for m in conciliacao.panorama_do_ano(2026)["meses"]}
    assert meses[1]["entradas"] == D("1000.00")
    assert meses[1]["saidas"] == D("-300.00")
    assert meses[3]["saidas"] == D("-500.00")
    assert 2 not in meses            # fevereiro não teve movimento


def test_quem_conciliou_aparece_com_nome(banco_conc):
    """⚠️ Conciliação é trabalho de gente, e o gestor precisa saber se está
    tudo nas costas de uma pessoa."""
    from app.apps.analisesps import conciliacao, conciliacao_ofx
    conta_id = conta_de_teste()
    conciliacao.importar(conta_id, conciliacao_ofx.ler(ofx([
        ("20260901", "-100.00", "A1"), ("20260902", "-200.00", "A2")])),
        "x.ofx", "T")
    linhas = conciliacao.listar({"conta_id": conta_id})
    conciliacao.marcar([linhas[0]["id"]], True, "MARCELO")
    conciliacao.marcar([linhas[1]["id"]], True, "JAYNE")

    quem = {q["nome"]: q for q in conciliacao.panorama_do_ano(2026)["quem_conciliou"]}
    assert quem["MARCELO"]["quantas"] == 1
    assert quem["JAYNE"]["quantas"] == 1
    assert quem["MARCELO"]["ultima"] is not None


# ---------------------------------------------------------------------------
# O FORNECEDOR DO OMIE NA CONTA — 25/09/2026
# ---------------------------------------------------------------------------
def test_o_fornecedor_do_omie_grava_na_CRIACAO_da_conta(banco_conc):
    """⚠️ NA CRIAÇÃO, e não só na alteração. Foi exatamente esse o defeito do
    saldo inicial: o campo aceitava o número, a tela dizia que gravou, e o
    valor não estava lá. Uma vez basta para virar teste."""
    from app.apps.analisesps import conciliacao
    conta_id = conta_de_teste(omie_fornecedor="7777")
    guardada = [c for c in conciliacao.contas() if c["id"] == conta_id][0]
    assert guardada["omie_fornecedor"] == 7777


def test_o_fornecedor_do_omie_grava_na_ALTERACAO(banco_conc):
    from app.apps.analisesps import conciliacao
    conta_id = conta_de_teste()
    assert conciliacao.contas()[0]["omie_fornecedor"] is None
    conciliacao.gravar_conta({"id": conta_id, "nome": "BD 7011",
                              "omie_fornecedor": "8888"}, quem="T")
    assert conciliacao.contas()[0]["omie_fornecedor"] == 8888


def test_o_fornecedor_aceita_o_codigo_com_lixo_em_volta(banco_conc):
    """A pessoa cola "cod. 7777" da tela do OMIE. Guardar o texto inteiro faria
    o lançamento falhar lá, com uma mensagem que não ajuda ninguém."""
    from app.apps.analisesps import conciliacao
    conta_id = conta_de_teste(omie_fornecedor=" cod. 7.777 ")
    guardada = [c for c in conciliacao.contas() if c["id"] == conta_id][0]
    assert guardada["omie_fornecedor"] == 7777


def test_conta_sem_fornecedor_fica_NULA_e_nao_zero(banco_conc):
    """Zero é um código do OMIE que não existe; nulo é "não configurado". A
    diferença importa porque é ela que decide se vale a reserva do tipo."""
    from app.apps.analisesps import conciliacao
    conta_de_teste(omie_fornecedor="")
    assert conciliacao.contas()[0]["omie_fornecedor"] is None


# ---------------------------------------------------------------------------
# A ADOÇÃO TEM DE TER VOLTA — 25/09/2026
#
# Relato do dono: *"Ainda tá tendo alguma falha na detecção. Está se tentando
# colocar registro que já estão lançados. BD 50024 · Li 1006 lançamento(s):
# 0 já estavam aqui e 1006 são novos."*
#
# O mecanismo: adotar uma linha da planilha grava nela a identidade do banco e
# o FITID. Desfazer a importação apagava só as linhas de origem 'ofx' — a
# adotada tem origem 'planilha' e FICAVA, carregando o FITID de um arquivo que
# acabou de ser apagado. Com FITID preenchido ela deixa de ser adotável, e com
# a identidade de um arquivo morto não é reconhecida: a importação seguinte a
# criava outra vez.
#
# Este bloco é o ciclo inteiro, que é o único jeito de provar que fechou.
# ---------------------------------------------------------------------------
def _uma_linha_de_planilha(data, valor, descricao="PIX RECEBIDO SEFAZ"):
    return aba_falsa([{"data": data, "descricao": descricao, "documento": "",
                       "valor": valor, "conciliado": True,
                       "observacao": "conferido na planilha"}])


def test_adotar_guarda_a_identidade_de_planilha_para_poder_voltar(banco_conc):
    from app.apps.analisesps import conciliacao, conciliacao_ofx
    from app.apps.analisesps.db import consultar_um
    from decimal import Decimal as D

    conta_id = conta_de_teste()
    conciliacao.importar_da_planilha(
        conta_id, _uma_linha_de_planilha(dt.date(2026, 9, 1), D("2407.68")), "T")
    antes = consultar_um(
        "SELECT impressao FROM analisesps.conciliacao_extrato "
        " WHERE conta_id = ? AND origem = 'planilha'", (conta_id,))[0]

    lido = conciliacao_ofx.ler(ofx([("20260901", "2407.68", "FIT-1",
                                     "PIX RECEBIDO REM: SECRETARIA DA FAZENDA")]))
    feito = conciliacao.importar(conta_id, lido, "extrato.ofx", "T")
    assert feito["adotadas"] == 1

    depois = consultar_um(
        "SELECT impressao_planilha, arquivo_id, fitid "
        "  FROM analisesps.conciliacao_extrato "
        " WHERE conta_id = ? AND origem = 'planilha'", (conta_id,))
    assert depois[0] == antes, "a identidade de planilha tem de ficar guardada"
    assert depois[1] == feito["arquivo_id"], "faltou anotar quem adotou"
    assert depois[2] == "FIT-1"


def test_desfazer_DEVOLVE_a_linha_da_planilha_em_vez_de_deixa_la_presa(banco_conc):
    """O coração do defeito. Depois de desfazer, a linha tem de estar como
    antes: sem FITID e com a identidade de planilha de volta."""
    from app.apps.analisesps import conciliacao, conciliacao_ofx
    from app.apps.analisesps.db import consultar_um
    from decimal import Decimal as D

    conta_id = conta_de_teste()
    conciliacao.importar_da_planilha(
        conta_id, _uma_linha_de_planilha(dt.date(2026, 9, 1), D("2407.68")), "T")
    antes = consultar_um(
        "SELECT impressao FROM analisesps.conciliacao_extrato "
        " WHERE conta_id = ? AND origem = 'planilha'", (conta_id,))[0]

    lido = conciliacao_ofx.ler(ofx([("20260901", "2407.68", "FIT-1", "PIX SEFAZ")]))
    feito = conciliacao.importar(conta_id, lido, "extrato.ofx", "T")

    desfeito = conciliacao.desfazer(conta_id, feito["arquivo_id"], quem="T")
    assert desfeito["devolvidas"] == 1

    linha = consultar_um(
        "SELECT impressao, fitid, impressao_planilha, arquivo_id, conciliado, "
        "       observacao FROM analisesps.conciliacao_extrato "
        " WHERE conta_id = ? AND origem = 'planilha'", (conta_id,))
    assert linha[0] == antes, "a identidade de planilha não voltou"
    assert linha[1] == "", "ficou com o FITID de um arquivo apagado"
    assert linha[2] == ""
    assert linha[3] is None
    # O trabalho de gente não é tocado: marcar e anotar não vieram do arquivo.
    assert linha[4] is True
    assert "conferido na planilha" in (linha[5] or "")


def test_depois_de_desfazer_o_MESMO_extrato_volta_a_ser_adotado(banco_conc):
    """⚠️ ESTE É O TESTE QUE PROVA O QUE O DONO VIU. Sem a devolução, aqui dava
    "1 nova" e a linha entrava em duplicidade — duas linhas do mesmo
    lançamento, e o saldo passando a divergir do banco em silêncio."""
    from app.apps.analisesps import conciliacao, conciliacao_ofx
    from app.apps.analisesps.db import consultar_um
    from decimal import Decimal as D

    conta_id = conta_de_teste()
    conciliacao.importar_da_planilha(
        conta_id, _uma_linha_de_planilha(dt.date(2026, 9, 1), D("2407.68")), "T")

    arquivo = ofx([("20260901", "2407.68", "FIT-1", "PIX SEFAZ")])
    feito = conciliacao.importar(conta_id, conciliacao_ofx.ler(arquivo), "e.ofx", "T")
    conciliacao.desfazer(conta_id, feito["arquivo_id"], quem="T")

    conferido = conciliacao.conferir(conta_id, conciliacao_ofx.ler(arquivo))
    assert conferido["adotaveis"] == 1, "voltou a contar como NOVA — duplicaria"
    assert len(conferido["novas"]) == 0
    assert conferido["presas"] == []

    de_novo = conciliacao.importar(conta_id, conciliacao_ofx.ler(arquivo), "e.ofx", "T")
    assert de_novo["adotadas"] == 1
    assert de_novo["gravadas"] == 0
    quantas = consultar_um(
        "SELECT count(*) FROM analisesps.conciliacao_extrato WHERE conta_id = ?",
        (conta_id,))[0]
    assert quantas == 1, f"duplicou: {quantas} linhas para um lançamento só"


def test_a_linha_PRESA_e_acusada_antes_de_deixar_gravar(banco_conc):
    """O aviso que faltava na tela. Uma linha da planilha com FITID de outro
    arquivo não é reconhecida nem adotável — e gravar duplicaria."""
    from app.apps.analisesps import conciliacao, conciliacao_ofx
    from app.apps.analisesps.db import conexao
    from decimal import Decimal as D

    conta_id = conta_de_teste()
    conciliacao.importar_da_planilha(
        conta_id, _uma_linha_de_planilha(dt.date(2026, 9, 1), D("2407.68")), "T")
    # O estado herdado: adotada por um arquivo antigo, sem caminho de volta
    # (é como ficavam as linhas adotadas antes da migração 026).
    with conexao() as con:
        con.execute(
            "UPDATE analisesps.conciliacao_extrato "
            "   SET fitid = 'FIT-VELHO', impressao = 'digital-de-arquivo-morto' "
            " WHERE conta_id = ? AND origem = 'planilha'", (conta_id,))
        con.commit()

    conferido = conciliacao.conferir(conta_id, conciliacao_ofx.ler(
        ofx([("20260901", "2407.68", "FIT-NOVO", "PIX SEFAZ")])))

    assert len(conferido["novas"]) == 1      # é o que ela seria: duplicidade
    assert conferido["adotaveis"] == 0
    assert len(conferido["presas"]) == 1, "a tela deixaria duplicar sem avisar"
    assert conferido["presas"][0]["valor"] == D("2407.68")


def test_soltar_as_presas_devolve_a_identidade_e_a_adocao_volta_a_funcionar(banco_conc):
    """Conserta estrago já feito: as linhas adotadas antes da migração 026 não
    sabem quem as adotou, mas a identidade de planilha é reconstruível a partir
    dos dados da própria linha."""
    from app.apps.analisesps import conciliacao, conciliacao_ofx
    from app.apps.analisesps.db import conexao, consultar_um
    from decimal import Decimal as D

    conta_id = conta_de_teste()
    conciliacao.importar_da_planilha(
        conta_id, _uma_linha_de_planilha(dt.date(2026, 9, 1), D("2407.68")), "T")
    original = consultar_um(
        "SELECT impressao FROM analisesps.conciliacao_extrato "
        " WHERE conta_id = ? AND origem = 'planilha'", (conta_id,))[0]
    with conexao() as con:
        con.execute(
            "UPDATE analisesps.conciliacao_extrato "
            "   SET fitid = 'FIT-VELHO', impressao = 'digital-de-arquivo-morto' "
            " WHERE conta_id = ? AND origem = 'planilha'", (conta_id,))
        con.commit()

    soltas = conciliacao.devolver_presas(conta_id, "T")
    assert soltas == {"devolvidas": 1, "conflitos": 0}
    assert consultar_um(
        "SELECT impressao FROM analisesps.conciliacao_extrato "
        " WHERE conta_id = ? AND origem = 'planilha'", (conta_id,))[0] == original

    arquivo = ofx([("20260901", "2407.68", "FIT-NOVO", "PIX SEFAZ")])
    conferido = conciliacao.conferir(conta_id, conciliacao_ofx.ler(arquivo))
    assert conferido["adotaveis"] == 1
    assert conferido["presas"] == []
    feito = conciliacao.importar(conta_id, conciliacao_ofx.ler(arquivo), "e.ofx", "T")
    assert feito["gravadas"] == 0 and feito["adotadas"] == 1
    assert consultar_um(
        "SELECT count(*) FROM analisesps.conciliacao_extrato WHERE conta_id = ?",
        (conta_id,))[0] == 1


def test_soltar_duas_linhas_iguais_nao_bate_no_indice_unico(banco_conc):
    """Duas linhas idênticas da planilha têm identidades diferentes (a ordem da
    repetição entra nelas). Reconstruir sem respeitar a ordem faria a segunda
    receber a identidade da primeira e bater no índice único."""
    from app.apps.analisesps import conciliacao
    from app.apps.analisesps.db import conexao, consultar
    from decimal import Decimal as D

    conta_id = conta_de_teste()
    duas = [{"data": dt.date(2026, 9, 1), "descricao": "PIX IGUAL",
             "documento": "", "valor": D("1500.00"), "conciliado": False,
             "observacao": ""} for _ in range(2)]
    conciliacao.importar_da_planilha(conta_id, aba_falsa(duas), "T")
    with conexao() as con:
        con.execute(
            "UPDATE analisesps.conciliacao_extrato SET fitid = 'X' "
            " WHERE conta_id = ? AND origem = 'planilha'", (conta_id,))
        con.commit()

    soltas = conciliacao.devolver_presas(conta_id, "T")
    assert soltas["devolvidas"] == 2, f"conflitos: {soltas}"
    marcas = [l[0] for l in consultar(
        "SELECT impressao FROM analisesps.conciliacao_extrato "
        " WHERE conta_id = ? ORDER BY id", (conta_id,))]
    assert len(set(marcas)) == 2, "as duas receberam a mesma identidade"


def test_o_desfazer_conta_as_que_serao_DEVOLVIDAS_separado(banco_conc):
    """Se elas entrassem em "quantas", a tela avisaria que vai apagar linha da
    planilha — o que é falso, e faria qualquer um desistir de desfazer."""
    from app.apps.analisesps import conciliacao, conciliacao_ofx
    from decimal import Decimal as D

    conta_id = conta_de_teste()
    conciliacao.importar_da_planilha(
        conta_id, _uma_linha_de_planilha(dt.date(2026, 9, 1), D("2407.68")), "T")
    feito = conciliacao.importar(conta_id, conciliacao_ofx.ler(ofx([
        ("20260901", "2407.68", "FIT-1", "PIX SEFAZ"),
        ("20260902", "-500.00", "FIT-2", "PAGTO ELETRON")])), "e.ofx", "T")
    assert feito["adotadas"] == 1 and feito["gravadas"] == 1

    conta = conciliacao.o_que_o_desfazer_apaga(conta_id, feito["arquivo_id"])
    assert conta["quantas"] == 1, "a adotada não é apagada, é devolvida"
    assert conta["devolvidas"] == 1


def test_o_sinal_trocado_na_planilha_e_ACUSADO_sem_ser_adotado(banco_conc):
    """⚠️ AVISA, NÃO JUNTA. A adoção casa por valor exato de propósito: casar
    por módulo faria uma entrada de 100 adotar uma saída de 100 e virar o sinal
    dela. Mas sem o aviso, uma aba importada com o sinal trocado aparece como
    "tudo novo" e a causa fica invisível até alguém gravar em duplicidade."""
    from app.apps.analisesps import conciliacao, conciliacao_ofx
    from decimal import Decimal as D

    conta_id = conta_de_teste()
    conciliacao.importar_da_planilha(conta_id, aba_falsa([
        {"data": dt.date(2026, 9, 1), "descricao": "PIX RECEBIDO SEFAZ",
         "documento": "", "valor": D("-2407.68"),   # errado: é entrada
         "conciliado": False, "observacao": ""}]), "T")

    conferido = conciliacao.conferir(conta_id, conciliacao_ofx.ler(
        ofx([("20260901", "2407.68", "FIT-1", "PIX RECEBIDO SEFAZ")])))

    assert conferido["adotaveis"] == 0, "juntou por módulo e viraria o sinal"
    assert len(conferido["novas"]) == 1
    assert len(conferido["sinal_trocado"]) == 1
    assert conferido["sinal_trocado"][0]["valor"] == D("-2407.68")


def test_sinal_igual_nao_e_confundido_com_sinal_trocado(banco_conc):
    """O aviso não pode disparar no caso normal, senão vira ruído e ninguém lê
    mais nenhum aviso desta tela."""
    from app.apps.analisesps import conciliacao, conciliacao_ofx
    from decimal import Decimal as D

    conta_id = conta_de_teste()
    conciliacao.importar_da_planilha(
        conta_id, _uma_linha_de_planilha(dt.date(2026, 9, 1), D("2407.68")), "T")

    conferido = conciliacao.conferir(conta_id, conciliacao_ofx.ler(
        ofx([("20260901", "2407.68", "FIT-1", "PIX RECEBIDO SEFAZ")])))

    assert conferido["adotaveis"] == 1
    assert conferido["sinal_trocado"] == []


# ---------------------------------------------------------------------------
# APAGAR UMA LINHA DO EXTRATO — 26/09/2026
#
# Pedido do dono, com a ressalva dele junto: *"era interessante a gente poder
# excluir um lançamento do extrato (…) e a exclusão tem uma confirmação, né?
# (…) Porque não é o certo estar excluindo linhas, mas…"*
#
# É a operação mais perigosa desta tela: o extrato é a cópia do que o banco
# diz, e uma linha que sai faz o saldo daqui deixar de bater com o banco — sem
# deixar rastro na conta, porque a linha sumiu.
# ---------------------------------------------------------------------------
def test_apagar_uma_linha_tira_ela_do_extrato_e_do_saldo(banco_conc):
    from app.apps.analisesps import conciliacao
    from app.apps.analisesps.db import consultar_um
    from decimal import Decimal as D

    conta_id = conta_de_teste()
    linha_id = conciliacao.acrescentar_a_mao(
        conta_id, dt.date(2026, 9, 1), "DUPLICADA POR FALHA", D("-1500.00"),
        quem="T")
    antes = conciliacao.saldo_da_conta(conta_id)

    feito = conciliacao.apagar_linha(linha_id, "veio duplicada da importação", "T")

    assert feito["apagadas"] == 1
    assert consultar_um(
        "SELECT count(*) FROM analisesps.conciliacao_extrato WHERE id = ?",
        (linha_id,))[0] == 0
    assert conciliacao.saldo_da_conta(conta_id) == antes + D("1500.00")


def test_apagar_SEM_motivo_e_recusado(banco_conc):
    """⚠️ O MOTIVO NÃO É BUROCRACIA: quem olhar o saldo em março e vir que não
    bate com o banco precisa conseguir descobrir por quê. Sem ele, a única
    resposta possível é "alguém apagou uma linha em setembro"."""
    from app.apps.analisesps import conciliacao
    from app.apps.analisesps.db import consultar_um
    from decimal import Decimal as D

    conta_id = conta_de_teste()
    linha_id = conciliacao.acrescentar_a_mao(
        conta_id, dt.date(2026, 9, 1), "X", D("-10.00"), quem="T")

    for vazio in ("", "   ", None):
        with pytest.raises(conciliacao.ErroDaConciliacao) as erro:
            conciliacao.apagar_linha(linha_id, vazio, "T")
        assert "por que" in str(erro.value).lower()

    assert consultar_um(
        "SELECT count(*) FROM analisesps.conciliacao_extrato WHERE id = ?",
        (linha_id,))[0] == 1, "apagou mesmo sem motivo"


def test_a_linha_JA_LANCADA_no_omie_nao_pode_ser_apagada(banco_conc):
    """Lá fora existe um título com aquele número. Sumir com a linha daqui
    deixaria o OMIE com um lançamento que nada mais explica, e ninguém
    descobriria a origem."""
    from app.apps.analisesps import conciliacao
    from app.apps.analisesps.db import conexao, consultar_um
    from decimal import Decimal as D

    conta_id = conta_de_teste()
    linha_id = conciliacao.acrescentar_a_mao(
        conta_id, dt.date(2026, 9, 1), "TARIFA", D("-9.00"), quem="T")
    with conexao() as con:
        con.execute("UPDATE analisesps.conciliacao_extrato "
                    "   SET omie_codigo = 987654 WHERE id = ?", (linha_id,))
        con.commit()

    conta = conciliacao.o_que_apagar_a_linha_leva(linha_id)
    assert conta["pode"] is False
    assert "987654" in conta["erro"]
    assert "OMIE" in conta["erro"]

    with pytest.raises(conciliacao.ErroDaConciliacao):
        conciliacao.apagar_linha(linha_id, "quero tirar", "T")
    assert consultar_um(
        "SELECT count(*) FROM analisesps.conciliacao_extrato WHERE id = ?",
        (linha_id,))[0] == 1


def test_a_conferencia_antes_de_apagar_diz_o_que_a_linha_E(banco_conc):
    """A tela pergunta com informação, não no escuro: se estava conciliada, por
    quem, e se tem observação escrita."""
    from app.apps.analisesps import conciliacao
    from decimal import Decimal as D

    conta_id = conta_de_teste()
    linha_id = conciliacao.acrescentar_a_mao(
        conta_id, dt.date(2026, 9, 1), "PIX FULANO", D("-1500.00"),
        observacao="conferido no banco", quem="T")
    conciliacao.marcar([linha_id], True, "MARCELO")

    conta = conciliacao.o_que_apagar_a_linha_leva(linha_id)

    assert conta["pode"] is True
    assert conta["data"] == "2026-09-01"
    assert conta["descricao"] == "PIX FULANO"
    # Vai para a tela em JSON: o valor sai como texto, para o navegador não
    # arredondar centavo nenhum no caminho.
    assert conta["valor"] == "-1500.00"
    assert conta["origem"] == "mao"
    assert conta["conciliado"] is True
    assert conta["conciliado_por"] == "MARCELO"
    assert conta["observacao"] == "conferido no banco"


def test_apagar_uma_linha_que_nao_existe_mais_responde_frase_e_nao_estouro(
        banco_conc):
    """Duas pessoas na mesma tela: a segunda clica no × de uma linha que a
    primeira já apagou."""
    from app.apps.analisesps import conciliacao

    conta = conciliacao.o_que_apagar_a_linha_leva(999999)
    assert conta["pode"] is False
    assert "não existe mais" in conta["erro"]

    with pytest.raises(conciliacao.ErroDaConciliacao):
        conciliacao.apagar_linha(999999, "qualquer", "T")
