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
