# -*- coding: utf-8 -*-
"""
A CONCILIAÇÃO BANCÁRIA — a tela e as rotas (sem banco).

O que vale de verdade nesta funcionalidade está em
`test_analisesps_conciliacao_banco.py`: filtro, saldo e a trava contra
duplicar são `WHERE`, `SUM OVER` e índice único, e o dublê ignora os três.
Aqui ficam as coisas que não dependem do banco — e uma que é mais importante
que todas: a amarra com o parser de OFX do ERP.
"""
from decimal import Decimal

import pytest
from flask import Flask

from app.apps.analisesps import conciliacao, preferencias, web

SENHA_OPERADOR = "senha-de-teste-operador"
SENHA_CONSULTA = "senha-de-teste-consulta"


# ---------------------------------------------------------------------------
# ⚠️ A AMARRA COM O ERP — o teste que existe para a quebra aparecer na suíte,
# e não na tela do dono.
# ---------------------------------------------------------------------------
def test_o_parser_de_OFX_do_ERP_continua_onde_esta_e_do_jeito_que_era():
    """A Conciliação NÃO tem parser próprio: usa o do ERP, que roda em produção
    há meses e já resolveu a armadilha dos dois PIX iguais no mesmo dia.

    Se o chat do ERP mover ou mudar aquele arquivo, esta tela para de ler
    extrato — e sem este teste isso apareceria na tela do dono, no meio de uma
    conciliação. A alternativa era copiar as 148 linhas, e cópia diverge
    calada.
    """
    from app.apps.erp.core.pagamentos import ofx

    assert hasattr(ofx, "parsear_ofx")
    assert hasattr(ofx, "ErroOFX")
    assert hasattr(ofx, "_decodificar")

    lidos = ofx.parsear_ofx(EXTRATO.encode("utf-8"), 0)
    assert len(lidos) == 2
    primeiro = lidos[0]
    # Os campos que a Conciliação lê de cada lançamento.
    for campo in ("data", "valor", "memo", "nome", "documento", "fitid",
                  "hash_linha"):
        assert hasattr(primeiro, campo), f"o parser perdeu o campo {campo}"


EXTRATO = """OFXHEADER:100
<OFX><BANKMSGSRSV1><STMTTRNRS><STMTRS><CURDEF>BRL
<BANKACCTFROM><BANKID>237<ACCTID>0007011-4<ACCTTYPE>CHECKING</BANKACCTFROM>
<BANKTRANLIST><DTSTART>20260901<DTEND>20260930
<STMTTRN><TRNTYPE>DEBIT<DTPOSTED>20260910<TRNAMT>-1500.00<FITID>A1<MEMO>PIX DES: FULANO</STMTTRN>
<STMTTRN><TRNTYPE>CREDIT<DTPOSTED>20260911<TRNAMT>2000.00<FITID>A2<MEMO>TED RECEBIDA</STMTTRN>
</BANKTRANLIST><LEDGERBAL><BALAMT>500.00<DTASOF>20260930</LEDGERBAL>
</STMTRS></STMTTRNRS></BANKMSGSRSV1></OFX>"""


# ---------------------------------------------------------------------------
# A leitura do arquivo
# ---------------------------------------------------------------------------
def test_o_arquivo_diz_de_qual_conta_ele_e_e_que_periodo_cobre():
    from app.apps.analisesps import conciliacao_ofx
    import datetime as dt

    lido = conciliacao_ofx.ler(EXTRATO.encode("utf-8"))
    assert lido.bankid == "237"
    assert lido.acctid == "0007011-4"
    assert lido.periodo_ini == dt.date(2026, 9, 1)
    assert lido.periodo_fim == dt.date(2026, 9, 30)
    assert lido.saldo == Decimal("500.00")
    assert len(lido.lancamentos) == 2


def test_sem_periodo_declarado_ele_sai_das_proprias_transacoes():
    """Nem todo banco manda DTSTART/DTEND. Sem período, a conferência não teria
    como dizer "o que está aqui e não vem neste extrato"."""
    from app.apps.analisesps import conciliacao_ofx
    import datetime as dt

    sem_periodo = EXTRATO.replace("<DTSTART>20260901<DTEND>20260930", "")
    lido = conciliacao_ofx.ler(sem_periodo.encode("utf-8"))
    assert lido.periodo_ini == dt.date(2026, 9, 10)
    assert lido.periodo_fim == dt.date(2026, 9, 11)


def test_o_periodo_DECLARADO_pelo_banco_nao_pode_encolher_o_arquivo():
    """⚠️ O BANCO MENTE O PERÍODO — achado em 25/09/2026, num extrato do
    Bradesco que o dono trouxe: o cabeçalho dizia DTSTART = DTEND = 25/09 e
    dentro vinham 1.006 lançamentos, o mais antigo de 01/09. Ele escreveu ali a
    data do download.

    E o período não é enfeite: é a janela em que a conferência procura "o que
    está aqui e NÃO vem neste extrato" — a lista que acusa linha digitada
    errada e lançamento estornado. Com a janela de um dia só, ela não achava
    nada e a tela passava a impressão de que estava tudo conferido."""
    from app.apps.analisesps import conciliacao_ofx
    import datetime as dt

    mentiroso = EXTRATO.replace("<DTSTART>20260901<DTEND>20260930",
                                "<DTSTART>20260925<DTEND>20260925")
    lido = conciliacao_ofx.ler(mentiroso.encode("utf-8"))
    # A união: cobre o que o banco declarou E o que o arquivo de fato traz.
    assert lido.periodo_ini == dt.date(2026, 9, 10), (
        "o período começou depois do lançamento mais antigo do arquivo")
    assert lido.periodo_fim == dt.date(2026, 9, 25)


def test_o_periodo_declarado_MAIOR_que_o_conteudo_continua_valendo():
    """Um extrato de mês fechado sem movimento no fim do mês: o banco declara
    até 30/09 e a última linha é 11/09. A janela é a do banco — encolher faria
    a conferência deixar de olhar os dias em que só existe linha nossa."""
    from app.apps.analisesps import conciliacao_ofx
    import datetime as dt

    lido = conciliacao_ofx.ler(EXTRATO.encode("utf-8"))
    assert lido.periodo_ini == dt.date(2026, 9, 1)
    assert lido.periodo_fim == dt.date(2026, 9, 30)


@pytest.mark.parametrize("conteudo,pedaco", [
    (b"", "vazio"),
    (b"isto nao e um extrato", "OFX"),
])
def test_arquivo_errado_e_recusado_com_frase_e_nao_com_traco_de_pilha(
        conteudo, pedaco):
    from app.apps.analisesps import conciliacao_ofx
    with pytest.raises(conciliacao_ofx.ErroDoExtrato) as erro:
        conciliacao_ofx.ler(conteudo)
    assert pedaco.lower() in str(erro.value).lower()


def test_arquivo_grande_demais_e_recusado_antes_de_ser_lido():
    """Este serviço já morreu de falta de memória uma vez (CONTEXTO §9)."""
    from app.apps.analisesps import conciliacao_ofx
    with pytest.raises(conciliacao_ofx.ErroDoExtrato) as erro:
        conciliacao_ofx.ler(b"x" * (13 * 1024 * 1024))
    assert "12 MB" in str(erro.value)


# ---------------------------------------------------------------------------
# A tela
# ---------------------------------------------------------------------------
@pytest.fixture
def app(monkeypatch):
    monkeypatch.setenv("ANALISESPS_SENHA_OPERADOR", SENHA_OPERADOR)
    monkeypatch.setenv("ANALISESPS_SENHA_CONSULTA", SENHA_CONSULTA)
    monkeypatch.setattr(preferencias, "ler", lambda pessoa, chave: {})
    monkeypatch.setattr(preferencias, "gravar",
                        lambda pessoa, chave, valor: None)
    a = Flask(__name__)
    a.secret_key = "teste"
    a.register_blueprint(web.bp)
    a.config["TESTING"] = True
    return a


def como(app, senha=SENHA_OPERADOR, nome="MARCELO"):
    cliente = app.test_client()
    cliente.post("/analisesps/entrar", data={"senha": senha, "nome": nome})
    return cliente


def test_sem_a_migracao_a_tela_ensina_o_que_fazer_em_vez_de_estourar(
        app, monkeypatch):
    """⚠️ O código sobe para o Render ANTES de o dono apertar o botão. Uma tela
    que estourasse nesse intervalo derrubaria a confiança na publicação."""
    monkeypatch.setattr(conciliacao, "estado",
                        lambda: {"pronto": False, "contas": 0, "linhas": 0})
    resposta = como(app).get("/analisesps/conciliacao")

    assert resposta.status_code == 200
    html = resposta.get_data(as_text=True)
    assert "Aplicar atualizações do banco" in html


def test_sem_conta_cadastrada_a_tela_diz_por_onde_comecar(app, monkeypatch):
    monkeypatch.setattr(conciliacao, "estado",
                        lambda: {"pronto": True, "contas": 0, "linhas": 0})
    monkeypatch.setattr(conciliacao, "contas", lambda so_ativas=True: [])
    html = como(app).get("/analisesps/conciliacao").get_data(as_text=True)
    assert "cadastrando uma conta" in html


def conta_falsa(**extra):
    base = {"id": 1, "nome": "BD 7011", "banco": "Bradesco", "agencia": "",
            "numero": "70114", "ofx_bankid": "237", "ofx_acctid": "0007011-4",
            "aba_planilha": "", "ativa": True, "ordem": 0, "observacao": ""}
    base.update(extra)
    return base


def linha_falsa(**extra):
    import datetime as dt
    base = {"id": 10, "data": dt.date(2026, 9, 10), "descricao": "PIX FULANO",
            "documento": "", "valor": Decimal("-1500.00"), "conciliado": False,
            "observacao": "", "origem": "ofx", "conciliado_por": "",
            "conciliado_em": None, "saldo": Decimal("500.00")}
    base.update(extra)
    return base


@pytest.fixture
def app_com_dados(app, monkeypatch):
    monkeypatch.setattr(conciliacao, "estado",
                        lambda: {"pronto": True, "contas": 1, "linhas": 2})
    monkeypatch.setattr(conciliacao, "contas",
                        lambda so_ativas=True: [conta_falsa()])
    monkeypatch.setattr(conciliacao, "listar", lambda f, pagina=1: [
        linha_falsa(), linha_falsa(id=11, valor=Decimal("2000.00"),
                                   conciliado=True, conciliado_por="MARCELO",
                                   descricao="TED RECEBIDA")])
    monkeypatch.setattr(conciliacao, "resumo", lambda f: {
        "quantidade": 2, "entradas": Decimal("2000.00"),
        "saidas": Decimal("-1500.00"), "saldo": Decimal("500.00"),
        "pendentes": 1, "pendentes_valor": Decimal("-1500.00"),
        "com_observacao": 0})
    monkeypatch.setattr(conciliacao, "saldo_da_conta",
                        lambda conta_id, ate=None: Decimal("500.00"))
    monkeypatch.setattr(conciliacao, "ultimos_arquivos",
                        lambda conta_id, quantos=8: [])
    return app


def test_a_tela_monta_com_entrada_saida_e_saldo(app_com_dados):
    html = como(app_com_dados).get(
        "/analisesps/conciliacao").get_data(as_text=True)

    assert "PIX FULANO" in html
    assert "1.500,00" in html      # a saída, sem o sinal
    assert "2.000,00" in html      # a entrada
    assert "500,00" in html        # o saldo


def test_a_linha_ja_conciliada_vem_marcada(app_com_dados):
    """A tela apaga o que já foi conferido para o pendente saltar aos olhos."""
    html = como(app_com_dados).get(
        "/analisesps/conciliacao").get_data(as_text=True)
    assert "linha-extrato conciliada" in html
    assert "tique ligado" in html


def test_quem_so_consulta_nao_ve_o_tique_nem_o_campo_de_anotar(app_com_dados):
    """⚠️ Esconder o botão não é a segurança — a rota recusa de qualquer jeito.
    Mas mostrar um botão que vai recusar é maltratar quem clica."""
    html = como(app_com_dados, SENHA_CONSULTA).get(
        "/analisesps/conciliacao").get_data(as_text=True)

    assert "PIX FULANO" in html          # ele VÊ o extrato
    assert 'class="tique' not in html    # e não pode mexer
    assert 'class="anotacao"' not in html
    assert "Solte o arquivo OFX" not in html


def test_quem_so_consulta_e_recusado_na_rota_que_grava(app_com_dados):
    resposta = como(app_com_dados, SENHA_CONSULTA).post(
        "/analisesps/api/conciliacao/marcar", json={"ids": [10],
                                                    "conciliado": True})
    assert resposta.status_code in (302, 401, 403)


def test_conta_desconhecida_nao_e_chutada_pela_rota(app_com_dados, monkeypatch):
    """⚠️ Jogar o extrato de uma empresa dentro da conta de outra é um estrago
    que ninguém percebe olhando a tela. Na dúvida, o sistema pergunta."""
    monkeypatch.setattr(conciliacao, "conta_do_extrato",
                        lambda bankid, acctid: None)
    from io import BytesIO

    resposta = como(app_com_dados).post(
        "/analisesps/api/conciliacao/conferir",
        data={"extrato": (BytesIO(EXTRATO.encode("utf-8")), "set.ofx")},
        content_type="multipart/form-data")
    dados = resposta.get_json()

    assert dados["ok"] is False
    assert dados["desconhecida"] is True
    assert "237" in dados["erro"]


def test_conferir_NAO_grava(app_com_dados, monkeypatch):
    """A resposta "estava tudo lá" não pode ser dada por quem acabou de mudar
    o mundo que está descrevendo."""
    gravou = []
    monkeypatch.setattr(conciliacao, "conta_do_extrato",
                        lambda bankid, acctid: conta_falsa())
    monkeypatch.setattr(conciliacao, "conferir", lambda conta_id, lido: {
        "conta_id": conta_id, "periodo_ini": lido.periodo_ini,
        "periodo_fim": lido.periodo_fim, "saldo": lido.saldo,
        "saldo_em": lido.saldo_em, "lidas": 2, "novas": [], "ja_estavam": 2,
        "so_aqui": [], "arquivo_repetido": None})
    monkeypatch.setattr(conciliacao, "importar",
                        lambda *a, **k: gravou.append(a) or {})
    from io import BytesIO

    resposta = como(app_com_dados).post(
        "/analisesps/api/conciliacao/conferir",
        data={"extrato": (BytesIO(EXTRATO.encode("utf-8")), "set.ofx")},
        content_type="multipart/form-data")

    assert resposta.get_json()["ok"] is True
    assert resposta.get_json()["ja_estavam"] == 2
    assert gravou == []


def test_a_tela_aparece_no_menu():
    chaves = [c for c, _, _ in web.TELAS]
    assert "conciliacao" in chaves
    assert web.TELAS[chaves.index("conciliacao")][2] == "analisesps.tela_conciliacao"


def test_sem_conta_nenhuma_o_formulario_de_cadastro_EXISTE(app, monkeypatch):
    """⚠️ O botão "+ Nova conta" do estado vazio apontava para um cartão que só
    era desenhado quando JÁ havia conta — ou seja, exatamente quando não
    precisava. Quem abrisse a tela pela primeira vez clicava e não acontecia
    nada."""
    monkeypatch.setattr(conciliacao, "estado",
                        lambda: {"pronto": True, "contas": 0, "linhas": 0})
    monkeypatch.setattr(conciliacao, "contas", lambda so_ativas=True: [])
    html = como(app).get("/analisesps/conciliacao").get_data(as_text=True)

    assert 'id="btn-nova-conta"' in html
    assert 'id="form-conta"' in html
    assert 'id="cartao-contas"' in html


def test_o_cadastro_de_conta_tem_UMA_PORTA_so(app, monkeypatch):
    """Regra da casa (17/09/2026): nada de um botão "digitando" e outro "pelo
    documento" lado a lado. É um formulário só, e o extrato é o atalho DENTRO
    dele — quem solta um OFX de conta desconhecida liga os dois ali mesmo."""
    monkeypatch.setattr(conciliacao, "estado",
                        lambda: {"pronto": True, "contas": 1, "linhas": 0})
    monkeypatch.setattr(conciliacao, "contas",
                        lambda so_ativas=True: [conta_falsa()])
    monkeypatch.setattr(conciliacao, "listar", lambda f, pagina=1: [])
    monkeypatch.setattr(conciliacao, "resumo", lambda f: {})
    monkeypatch.setattr(conciliacao, "saldo_da_conta",
                        lambda conta_id, ate=None: Decimal("0"))
    monkeypatch.setattr(conciliacao, "ultimos_arquivos",
                        lambda conta_id, quantos=8: [])
    html = como(app).get("/analisesps/conciliacao").get_data(as_text=True)

    assert html.count('id="form-conta"') == 1
    assert "Cadastrar pelo extrato" not in html
    assert "não precisam ser" in html   # o recado de que o OFX preenche


# ---------------------------------------------------------------------------
# ⚠️ A COLISÃO DE CLASSE DE CSS — 24/09/2026
#
# O dono abriu a tela e disse: *"cada linha está ocupando um espaço absurdo,
# uma única linha está dando mais do que toda a tela (…) tem uma parte
# escura"*.
#
# A causa: as células de valor usavam `class="entrada"` e `class="saida"`. E
# `.entrada` JÁ EXISTIA nesta folha de estilo, para a TELA DE LOGIN — com
# `min-height: 100vh` e fundo azul-escuro. Cada linha do extrato virou um
# bloco mais alto que a tela.
#
# ⚠️ E ESSA LIÇÃO JÁ ESTAVA PAGA NO MESMO REPOSITÓRIO. O chat do painel caiu
# na MESMA classe no dia anterior e escreveu no histórico dele: *"a classe
# `entrada` já existia no CSS, para a tela de login (...) o quadradinho virou
# um bloco de 900 px de altura. As classes do calendário levam o prefixo
# `cal-` por isso."* Eu não li.
#
# Este teste existe para a próxima tela não repetir.
# ---------------------------------------------------------------------------
CLASSES_QUE_JA_TEM_DONO = {
    # classe -> para que ela serve nesta folha, e por que colide
    "entrada": "a TELA DE LOGIN (min-height: 100vh, fundo escuro)",
    "recado": "as telas de recado (caixa centralizada de 560px)",
    "marca": "o brasão da tela de login",
    "kpis": "a faixa de números do topo",
    "cartao": "o cartão branco das telas",
    "filtro": "um bloco da barra de filtros",
    "sps": "a tabela padrão do módulo",
}


def _classes_do_template(nome: str) -> set:
    """Todas as classes usadas num template, uma a uma."""
    import pathlib
    import re
    caminho = (pathlib.Path(web.__file__).parent / "templates" / nome)
    html = caminho.read_text(encoding="utf-8")
    achadas = set()
    for bloco in re.findall(r'class="([^"]*)"', html):
        # O Jinja entra no meio do atributo; o que sobra ainda serve.
        for pedaco in re.sub(r"\{[{%].*?[%}]\}", " ", bloco).split():
            achadas.add(pedaco)
    return achadas


def test_a_conciliacao_nao_usa_classe_de_CSS_de_outra_tela():
    """⚠️ Uma classe com dono não é "estilo parecido": é o estilo DAQUELA
    tela, inteiro, caindo em cima desta. Foi assim que cada linha do extrato
    virou um bloco maior que a tela."""
    usadas = _classes_do_template("analisesps_conciliacao.html")
    invasoras = usadas & set(CLASSES_QUE_JA_TEM_DONO) - {
        # Estas são reuso LEGÍTIMO: a tela usa o componente inteiro, como ele
        # foi feito para ser usado.
        "kpis", "cartao", "filtro", "sps", "marca", "recado",
    }
    assert not invasoras, (
        "classe com dono usada na Conciliação: "
        + ", ".join(f"{c} (é {CLASSES_QUE_JA_TEM_DONO[c]})"
                    for c in sorted(invasoras)))


def test_a_folha_de_estilo_nao_tem_regra_solta_para_entrada_e_saida():
    """A correção foi renomear para `valor-entrada`/`valor-saida`. Se alguém
    reintroduzir `.entrada` numa tabela, isto acusa."""
    import pathlib
    css = (pathlib.Path(web.__file__).parent / "static"
           / "analisesps.css").read_text(encoding="utf-8")

    assert "table.conciliacao .valor-entrada" in css
    assert "table.conciliacao .entrada " not in css
    assert "table.conciliacao .entrada{" not in css


def test_a_observacao_aceita_quebra_de_linha(app_com_dados):
    """Pedido do dono: *"na observação, permita a quebra de linha"*. Campo de
    uma linha só corta o que ele escreve sem avisar."""
    html = como(app_com_dados).get(
        "/analisesps/conciliacao").get_data(as_text=True)

    assert '<textarea class="anotacao"' in html
    assert '<input class="anotacao"' not in html


def test_o_ENTER_quebra_a_linha_em_vez_de_gravar(app_com_dados):
    """Se Enter gravasse, não haveria como escrever a segunda linha."""
    html = como(app_com_dados).get(
        "/analisesps/conciliacao").get_data(as_text=True)
    assert "ctrlKey" in html          # Ctrl+Enter é que grava
    assert 'if (e.key === "Enter") campo.blur()' not in html


def test_o_soltar_o_extrato_fica_na_BARRA_LATERAL(app_com_dados):
    """⚠️ Ele estava no topo do conteúdo e o dono pediu para sair: *"queria que
    ficasse no sidebar, abaixo dos filtros; está a ocupar a parte de cima da
    tela"*. Trazer extrato é semanal; olhar o extrato é o dia inteiro."""
    html = como(app_com_dados).get(
        "/analisesps/conciliacao").get_data(as_text=True)

    lateral = html.index('class="lateral-extrato"')
    conteudo = html.index("<h2>Conciliação")
    assert lateral < conteudo, "o bloco de soltar o arquivo saiu da barra lateral"
    # E a RESPOSTA da conferência fica no meio da tela, onde há largura.
    assert html.index('id="saida-extrato"') > conteudo


def test_os_numeros_do_topo_SAO_FILTROS(app_com_dados):
    """Pedido dele: *"se eu clicar em falta conciliar, eu já sei listado
    imediatamente as que faltam. Isso aqui não está acontecendo."*"""
    html = como(app_com_dados).get(
        "/analisesps/conciliacao?conta_id=1&busca=cimento").get_data(as_text=True)

    assert "situacao=pendentes" in html
    assert "situacao=com_observacao" in html
    assert "situacao=conciliados" in html
    # ⚠️ E o clique NÃO joga fora o resto do recorte: conta, período e busca
    # ficam. Limpá-los faria o clique parecer um "voltar ao início".
    assert "busca=cimento" in html


def test_os_ajustes_saem_do_RODAPE_e_viram_janela(app_com_dados):
    """⚠️ *"O trazer a planilha antiga e contas está no final da tela, então
    você vai manuseando e isso vai estar sempre aparecendo."* O que se
    configura uma vez não divide espaço com o que se faz o dia inteiro."""
    html = como(app_com_dados).get(
        "/analisesps/conciliacao").get_data(as_text=True)

    assert '<dialog class="dialogo" id="cartao-contas">' in html
    assert '<dialog class="dialogo" id="cartao-planilha">' in html
    assert 'id="btn-abrir-contas"' in html
    assert 'id="btn-abrir-planilha"' in html
    # E os botões que as abrem ficam na barra lateral, antes do conteúdo.
    assert html.index('id="btn-abrir-contas"') < html.index("<h2>Conciliação")


def test_a_largura_das_colunas_e_declarada(app_com_dados):
    """⚠️ Só o Histórico ficava sem largura, e por isso engolia toda a sobra da
    tabela — empurrando a observação para um canto. O dono: *"o histórico é o
    campo que estica a tela (...) ficou grande demais"*."""
    html = como(app_com_dados).get(
        "/analisesps/conciliacao").get_data(as_text=True)

    assert "<colgroup>" in html
    assert 'class="c-historico"' in html
    assert 'class="c-observacao"' in html


def test_o_saldo_avisa_quando_a_conta_nao_tem_saldo_inicial(app_com_dados):
    """⚠️ É a explicação de "o saldo não está batendo": sem saldo inicial, o
    número soma só o que foi importado, e o que veio antes disso falta."""
    html = como(app_com_dados).get(
        "/analisesps/conciliacao").get_data(as_text=True)
    assert "sem saldo inicial" in html


def test_o_historico_quebra_a_linha_em_vez_de_sumir(app_com_dados):
    """⚠️ Ele era uma linha só com reticências, e numa tela estreita o texto
    ficava INALCANÇÁVEL: o `title` do mouse não existe no celular, e não havia
    clique que o mostrasse. O dono: *"quando a gente diminui a tela, deixa de
    aparecer as informações; mesmo clicando você não as vê de forma alguma"*.
    """
    import pathlib
    css = (pathlib.Path(web.__file__).parent / "static"
           / "analisesps.css").read_text(encoding="utf-8")
    bloco = css[css.index("table.conciliacao .historico"):][:400]

    assert "white-space: pre-wrap" in bloco
    assert "white-space: nowrap" not in bloco
    # E o que passar de três linhas abre com um clique.
    assert "table.conciliacao .historico.aberto" in css
    html = como(app_com_dados).get(
        "/analisesps/conciliacao").get_data(as_text=True)
    assert 'classList.toggle("aberto")' in html


def test_a_linha_da_tabela_nao_tem_altura_travada(app_com_dados):
    """O teto que eu tinha posto era o que cortava o histórico. Quem decide o
    tamanho da linha é o conteúdo dela."""
    import pathlib
    css = (pathlib.Path(web.__file__).parent / "static"
           / "analisesps.css").read_text(encoding="utf-8")
    assert "table.conciliacao tr.linha-extrato { height:" not in css
    assert "max-height: 46px" not in css


def test_o_campo_de_observacao_nao_cresce_mais_do_que_precisa(app_com_dados):
    """⚠️ Com UMA linha escrita ele abria como se tivesse duas ou três. A causa
    era o `textarea` medir a altura sem contar o preenchimento interno do mesmo
    jeito — sem `box-sizing: border-box`, a soma sobrava."""
    import pathlib
    css = (pathlib.Path(web.__file__).parent / "static"
           / "analisesps.css").read_text(encoding="utf-8")
    bloco = css[css.index(".anotacao {"):][:400]

    assert "box-sizing: border-box" in bloco
    html = como(app_com_dados).get(
        "/analisesps/conciliacao").get_data(as_text=True)
    # O `auto` antes da medida é o que deixa o campo ENCOLHER também.
    assert 'campo.style.height = "auto"' in html


def test_os_numeros_do_topo_nao_ficam_com_cara_de_link(app_com_dados):
    """*"Não precisa ficar com esse tracinho embaixo, fica feio, aquele
    sublinhado de link."* Ele continua clicável — a mão do mouse e o realce ao
    passar já dizem isso."""
    import pathlib
    css = (pathlib.Path(web.__file__).parent / "static"
           / "analisesps.css").read_text(encoding="utf-8")
    assert "a.kpi, a.kpi:hover, a.kpi:visited { text-decoration: none;" in css
    assert "a.kpi { cursor: pointer; }" in css


def test_desconciliar_pede_confirmacao_e_conciliar_nao(app_com_dados):
    """⚠️ O dono perguntou se desconciliar não deveria pedir dois cliques *"para
    ninguém fazer acidentalmente"*. A ideia está certa; clique duplo não é uma
    boa trava — não se descobre sozinho, e acontece por acidente justamente
    com quem está marcando várias linhas seguidas.

    Aqui o botão VIRA UMA PERGUNTA ("↺?") e o segundo clique confirma. Mesma
    trava, mas ela se explica. Marcar continua sendo um clique só."""
    html = como(app_com_dados).get(
        "/analisesps/conciliacao").get_data(as_text=True)

    assert "perguntando" in html
    assert "clique de novo para desconciliar" in html
    assert "ESPERA_DA_CONFIRMACAO" in html


def test_varios_OFX_de_uma_vez(app_com_dados):
    """*"Vou jogar vários arquivos OFX de uma determinada conta, aí o sistema
    importa eles tudinho. Ou tentar importar — ele vai barrar quando detectar
    que já foi importado."*"""
    html = como(app_com_dados).get(
        "/analisesps/conciliacao").get_data(as_text=True)

    assert 'id="arq-extrato" accept=".ofx,.OFX" multiple' in html
    assert "conferirVarios" in html
    # ⚠️ Confere TUDO antes de gravar QUALQUER coisa: gravar cada um assim que
    # é lido tiraria dele a chance de olhar o conjunto.
    assert "btn-gravar-leva" in html


# ---------------------------------------------------------------------------
# O PANORAMA
# ---------------------------------------------------------------------------
@pytest.fixture
def app_panorama(app, monkeypatch):
    import datetime as dt
    monkeypatch.setattr(conciliacao, "estado",
                        lambda: {"pronto": True, "contas": 2, "linhas": 10})
    monkeypatch.setattr(conciliacao, "anos_com_movimento", lambda: [2026, 2025])
    monkeypatch.setattr(conciliacao, "panorama", lambda ano: {
        "pronto": True, "ano": ano, "meses": conciliacao.MESES_CURTOS,
        "contas": [
            {"id": 1, "nome": "BD 7011", "banco": "Bradesco",
             "tem_saldo_inicial": False, "saldo": Decimal("1000.00"),
             "quantidade": 20, "pendentes": 3,
             "pendentes_valor": Decimal("-500.00"), "conciliados": 17,
             "por_cento": 85, "com_observacao": 2,
             "entradas": Decimal("5000.00"), "saidas": Decimal("-4000.00"),
             "movimento": Decimal("9000.00"),
             "meses": {m: (5 if m in (1, 3) else 0) for m in range(1, 13)},
             "buracos": [2], "buracos_nome": "fev",
             "nao_vieram": [4, 5], "nao_vieram_nome": "abr, mai",
             "ate": dt.date(2026, 3, 20), "dias_sem_extrato": 90,
             "importado_em": None, "dias_sem_importar": None,
             "pendentes_total": 3,
             "recado": {"grau": "ruim", "texto": "faltam os meses de fev"}},
        ],
        "resumo": {"contas": 1, "em_dia": 0, "atencao": 0, "ruim": 1,
                   "com_buraco": 1, "pendentes": 3,
                   "pendentes_valor": Decimal("-500.00"), "com_observacao": 2,
                   "movimento": Decimal("9000.00"),
                   "entradas": Decimal("5000.00"),
                   "saidas": Decimal("-4000.00"), "sem_saldo_inicial": 1},
    })
    return app


def test_o_panorama_mostra_o_buraco_antes_dos_numeros(app_panorama):
    """⚠️ A pergunta desta tela não é "quanto tem", é "no que eu não posso
    confiar". Uma conta 100% conciliada com extrato de três meses atrás está
    pior que uma com pendências e extrato de ontem."""
    html = como(app_panorama).get(
        "/analisesps/conciliacao/panorama").get_data(as_text=True)

    assert "faltam os meses de fev" in html
    assert "Contas com buraco no extrato" in html
    # O recado vem ANTES das colunas de número, DENTRO da tabela — a coluna
    # do que fazer é a segunda, logo depois do nome da conta.
    cabecalho = html[html.index("<table class=\"sps panorama\">"):]
    cabecalho = cabecalho[:cabecalho.index("</thead>")]
    assert cabecalho.index("O que ela precisa") < cabecalho.index("Saldo")
    assert cabecalho.index("O que ela precisa") < cabecalho.index("Movimento")


def test_o_panorama_desenha_a_fita_dos_doze_meses(app_panorama):
    html = como(app_panorama).get(
        "/analisesps/conciliacao/panorama").get_data(as_text=True)
    assert 'class="fita-meses"' in html
    assert "buraco no extrato" in html
    assert "ainda não veio" in html


def test_o_panorama_avisa_das_contas_sem_saldo_inicial(app_panorama):
    html = como(app_panorama).get(
        "/analisesps/conciliacao/panorama").get_data(as_text=True)
    assert "sem saldo inicial" in html


def test_do_panorama_da_para_ir_direto_ao_que_falta(app_panorama):
    """Panorama que só informa é relatório. Ele tem de levar ao trabalho."""
    html = como(app_panorama).get(
        "/analisesps/conciliacao/panorama").get_data(as_text=True)
    assert "situacao=pendentes" in html
    assert "conta_id=1" in html


# ---------------------------------------------------------------------------
# LANÇAR NO OMIE — a tela
# ---------------------------------------------------------------------------
def test_o_botao_do_OMIE_nao_lanca_direto_ele_ENSAIA(app_com_dados):
    """⚠️ Lançamento no OMIE não se desfaz num clique. São dois passos: o
    ensaio mostra linha a linha o que vai acontecer, e o segundo botão manda —
    o mesmo desenho dos aportes."""
    html = como(app_com_dados).get(
        "/analisesps/conciliacao").get_data(as_text=True)

    assert 'id="btn-omie"' in html
    assert "conciliacao_omie_ensaiar" in html or "omie/ensaiar" in html
    assert "btn-lancar-omie" in html


def test_a_tela_tem_onde_configurar_a_categoria_de_cada_tipo(app_com_dados):
    """Pedido dele: *"eu poder gravar a conta do plano financeiro OMIE que está
    relacionada a cada ação. Se for tarifa, essa conta."*"""
    html = como(app_com_dados).get(
        "/analisesps/conciliacao").get_data(as_text=True)

    assert 'id="cartao-tipos"' in html
    assert 'name="codigo_categoria"' in html
    assert 'name="palavras"' in html
    assert 'id="btn-abrir-tipos"' in html


def test_quem_so_consulta_nao_ve_o_botao_de_lancar_no_OMIE(app_com_dados):
    html = como(app_com_dados, SENHA_CONSULTA).get(
        "/analisesps/conciliacao").get_data(as_text=True)
    assert 'id="btn-omie"' not in html


def test_quem_so_consulta_e_recusado_na_rota_que_lanca(app_com_dados):
    """⚠️ Esconder o botão não é a segurança. A rota recusa."""
    resposta = como(app_com_dados, SENHA_CONSULTA).post(
        "/analisesps/api/conciliacao/omie/lancar",
        json={"ids": [10], "conta_id": 1})
    assert resposta.status_code in (302, 401, 403)


def test_o_que_vai_para_o_OMIE_e_lido_do_BANCO_e_nao_do_navegador(
        app_com_dados, monkeypatch):
    """⚠️ O navegador manda só os NÚMEROS das linhas. Aceitar dele o valor, a
    data ou a descrição deixaria o que vai para o OMIE nas mãos de quem abrir
    o console — e o que sai daqui é lançamento contábil."""
    from app.apps.analisesps import web as web_

    vistos = {}
    monkeypatch.setattr(web_, "_linhas_para_o_omie",
                        lambda ids, conta_id: (vistos.setdefault("ids", ids)
                                               or [], None))
    resposta = como(app_com_dados).post(
        "/analisesps/api/conciliacao/omie/ensaiar",
        json={"ids": [10, 11], "conta_id": 1,
              "valor": "999999", "descricao": "MENTIRA"})

    assert vistos["ids"] == [10, 11]
    assert resposta.get_json()["ok"] is False   # sem linhas, recusa


def test_os_codigos_do_OMIE_sao_ESCOLHIDOS_e_nao_digitados(app_com_dados,
                                                           monkeypatch):
    """⚠️ Pedido dele: *"você pode utilizar a própria API dele para atualizar
    aqui (…) tem a questão do código dos departamentos também"*.

    E a resposta é que NÃO se chama a API do OMIE aqui: a carga do painel já
    traz tudo isso toda noite. Chamar de novo seria mais uma credencial para
    manter e duas cópias dos mesmos dados que um dia divergiriam."""
    from app.apps.analisesps import web as web_
    monkeypatch.setattr(web_, "_listas_do_omie", lambda: {
        "contas": [{"codigo": 777, "descricao": "BRADESCO 7011",
                    "numero_conta": "7011-4", "inativa": False}],
        "categorias": [{"codigo": "2.01.05", "descricao": "Tarifas bancárias",
                        "inativa": False, "transferencia": False}],
        "obras": [{"codigo": "OBRA1", "nome": "CRECHE SWAP"}],
        "erro": ""})

    html = como(app_com_dados).get(
        "/analisesps/conciliacao").get_data(as_text=True)

    assert '<select class="campo-largo" name="omie_conta_corrente">' in html
    assert "BRADESCO 7011" in html
    assert '<select class="campo-largo" name="codigo_categoria">' in html
    assert "Tarifas bancárias" in html
    assert "CRECHE SWAP" in html
    # E a tela diz de onde vem a lista, e qual é a idade dela.
    assert "carga do painel" in html


def test_sem_a_lista_do_OMIE_a_tela_ABRE_e_deixa_digitar(app_com_dados,
                                                         monkeypatch):
    """⚠️ Uma tela de configuração que não abre porque o espelho está vazio é
    pior do que uma que abre dizendo que a lista está vazia."""
    from app.apps.analisesps import web as web_
    monkeypatch.setattr(web_, "_listas_do_omie", lambda: {
        "contas": [], "categorias": [], "obras": [],
        "erro": "a carga do painel nunca rodou"})

    html = como(app_com_dados).get(
        "/analisesps/conciliacao").get_data(as_text=True)

    assert '<input class="campo-largo" name="codigo_categoria"' in html
    assert "a carga do painel nunca rodou" in html


def test_o_javascript_da_tela_e_sintaticamente_valido():
    """⚠️ ESTA TELA TEM MUITO JAVASCRIPT, e um erro de sintaxe nele NÃO aparece
    em nenhum outro teste: o HTML monta, a tela abre, e simplesmente nada
    funciona — o tique não marca, o arquivo não sobe, nada avisa. Aqui o
    script é extraído (sem o Jinja) e passado pelo Node.

    Se o Node não existir na máquina, o teste é pulado em vez de falhar: ele é
    uma rede, não um requisito de ambiente.
    """
    import pathlib
    import re
    import shutil
    import subprocess
    import tempfile

    if not shutil.which("node"):
        pytest.skip("node não está nesta máquina")

    caminho = (pathlib.Path(web.__file__).parent / "templates"
               / "analisesps_conciliacao.html")
    html = caminho.read_text(encoding="utf-8")
    ini = html.index("<script>", html.index("{% block scripts %}"))
    js = html[ini + len("<script>"):html.index("</script>", ini)]
    # O Jinja sai: `{{ ... }}` vira um literal, `{% ... %}` some.
    js = re.sub(r"\{\{[^}]*\}\}", "0", js)
    js = re.sub(r"\{%.*?%\}", "", js, flags=re.S)

    with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False,
                                     encoding="utf-8") as arquivo:
        arquivo.write(js)
        nome = arquivo.name
    saida = subprocess.run(["node", "--check", nome], capture_output=True,
                           text=True)
    assert saida.returncode == 0, saida.stderr[:900]


def test_a_transferencia_pede_a_conta_de_destino_na_tela(app_com_dados):
    """⚠️ Adivinhar o destino poria o dinheiro numa conta que ninguém pediu.
    O dono: *"você seleciona a conta origem e a conta destino, e já interfere
    nas duas pontas"*."""
    html = como(app_com_dados).get(
        "/analisesps/conciliacao").get_data(as_text=True)

    assert "destino-transf" in html
    assert "transferência — para " in html
    assert "qual conta o dinheiro foi?" in html
    assert 'name="natureza"' in html
    assert "Transferência entre contas" in html


# ---------------------------------------------------------------------------
# O FILTRO EM CADA COLUNA — 24/09/2026
#
# *"Tem data, tem histórico, tem observação, tem entrada, tem saída. Acho que
# em cada um desses dá para colocar o filtro de cabeçalho."*
#
# ⚠️ SUBSTITUIU a barra de três campos da leva anterior: ali "valor" não
# distinguia entrada de saída, e "histórico" procurava nas três colunas de
# texto ao mesmo tempo.
# ---------------------------------------------------------------------------
def test_cada_coluna_tem_a_sua_caixinha_de_filtro(app_com_dados):
    html = como(app_com_dados).get(
        "/analisesps/conciliacao").get_data(as_text=True)

    assert 'class="filtros-coluna"' in html
    for campo in ("data", "historico", "documento", "entrada", "saida",
                  "observacao"):
        assert f'name="{campo}" form="filtro-colunas"' in html or \
               f'type="date" name="{campo}" form="filtro-colunas"' in html, campo


def test_o_formulario_fica_FORA_da_tabela(app_com_dados):
    """⚠️ Um `<form>` no meio de `<tr>` não é HTML válido: o navegador o
    expulsa da tabela e as caixinhas param de enviar, sem erro nenhum na tela.
    Por isso ele fica fora, e os campos apontam para ele pelo atributo `form`.
    """
    html = como(app_com_dados).get(
        "/analisesps/conciliacao").get_data(as_text=True)

    assert 'id="filtro-colunas"' in html
    # O formulário vem ANTES da tabela, e não dentro dela.
    assert html.index('id="filtro-colunas"') < html.index(
        '<table class="sps conciliacao">')


def test_cada_caixinha_filtra_a_SUA_coluna(app_com_dados, monkeypatch):
    """⚠️ Procurar "pix" no histórico e procurar "pix" na observação são
    perguntas diferentes. Quem digita embaixo de um título quer aquela
    coluna."""
    vistos = []
    monkeypatch.setattr(conciliacao, "listar",
                        lambda f, pagina=1: vistos.append(f) or [])
    monkeypatch.setattr(conciliacao, "resumo", lambda f: {})

    como(app_com_dados).get("/analisesps/conciliacao?conta_id=1"
                            "&historico=pix&documento=41024&observacao=nilo")

    usado = vistos[0]
    assert usado["historico"] == "pix"
    assert usado["documento"] == "41024"
    assert usado["observacao"] == "nilo"


def test_entrada_e_saida_sao_o_mesmo_campo_com_o_sinal_decidindo():
    """Quem digita 1.500 em "Entrada" quer +1.500; em "Saída", quer -1.500."""
    from app.apps.analisesps import conciliacao as conc
    onde, params = conc._onde({"conta_id": 1, "entrada": "1500"})
    assert "valor = ?" in onde and Decimal("1500") in params

    onde, params = conc._onde({"conta_id": 1, "saida": "1500"})
    assert Decimal("-1500") in params

    # E "saída" digitada já negativa continua achando a saída.
    _onde2, params2 = conc._onde({"conta_id": 1, "saida": "-1500"})
    assert Decimal("-1500") in params2


def test_um_dia_no_cabecalho_vira_o_dia_inteiro(app_com_dados, monkeypatch):
    import datetime as dt
    vistos = []
    monkeypatch.setattr(conciliacao, "listar",
                        lambda f, pagina=1: vistos.append(f) or [])
    monkeypatch.setattr(conciliacao, "resumo", lambda f: {})

    como(app_com_dados).get("/analisesps/conciliacao?conta_id=1&data=2026-09-10")
    assert vistos[0]["data_ini"] == vistos[0]["data_fim"] == dt.date(2026, 9, 10)


def test_a_faixa_da_barra_lateral_continua_valendo(app_com_dados, monkeypatch):
    """O filtro de coluna é um atalho para UM dia e UM valor; quem precisa de
    faixa (de 1/9 a 30/9) continua com a barra lateral."""
    import datetime as dt
    vistos = []
    monkeypatch.setattr(conciliacao, "listar",
                        lambda f, pagina=1: vistos.append(f) or [])
    monkeypatch.setattr(conciliacao, "resumo", lambda f: {})

    como(app_com_dados).get("/analisesps/conciliacao"
                            "?conta_id=1&data_ini=2026-09-01&data_fim=2026-09-30")
    assert vistos[0]["data_ini"] == dt.date(2026, 9, 1)
    assert vistos[0]["data_fim"] == dt.date(2026, 9, 30)


# ---------------------------------------------------------------------------
# ⚠️ A MIGRAÇÃO DO OMIE É SEPARADA DA DA CONCILIAÇÃO — 24/09/2026
#
# O dono preencheu o cadastro de tipos inteiro e recebeu no Gravar a frase
# crua do Postgres: *"relation analisesps.conciliacao_tipo does not exist"*.
#
# A causa: a tela se dava por pronta olhando a tabela das CONTAS (migração
# 019), e a parte do OMIE veio depois, na 021. Entre uma e outra, a tela
# abria, o formulário aparecia inteiro, e só o Gravar quebrava.
#
# **Cada pedaço confere a SUA tabela**, e avisa ANTES de a pessoa digitar.
# ---------------------------------------------------------------------------
def test_sem_a_migracao_do_OMIE_a_tela_AVISA_antes_de_digitar(app_com_dados,
                                                              monkeypatch):
    from app.apps.analisesps import web as web_
    monkeypatch.setattr(web_, "_omie_ligado", lambda: False)

    html = como(app_com_dados).get(
        "/analisesps/conciliacao").get_data(as_text=True)

    assert "A parte do OMIE ainda não foi ligada no banco" in html
    assert "Aplicar atualizações do banco" in html
    # E o botão de lançar não fica oferecendo o que não funciona.
    assert "disabled" in html[html.index('id="btn-omie"'):
                              html.index('id="btn-omie"') + 200]


def test_gravar_tipo_sem_a_migracao_devolve_FRASE_e_nao_erro_de_banco(
        monkeypatch):
    """⚠️ A frase do Postgres não é para o dono ler. Ela não diz o que fazer."""
    from app.apps.analisesps import conciliacao_omie as co
    monkeypatch.setattr(co, "_pronto", lambda: False)

    with pytest.raises(co.ErroDoLancamento) as erro:
        co.gravar_tipo({"nome": "Tarifa"}, "T")

    assert "Aplicar atualizações do banco" in str(erro.value)
    assert "does not exist" not in str(erro.value)


def test_o_ensaio_sem_a_migracao_nao_manda_cadastrar_onde_nao_grava(
        monkeypatch):
    """⚠️ Sem a migração a lista de tipos vem VAZIA — e sem esta guarda o
    ensaio diria "não reconheci o tipo" para todas as linhas, mandando o dono
    cadastrar tipos num lugar que não grava."""
    from app.apps.analisesps import conciliacao_omie as co
    monkeypatch.setattr(co, "_pronto", lambda: False)
    monkeypatch.setattr(co, "tipos", lambda so_ativos=True: [])

    import datetime as dt
    plano = co.planejar(
        [{"id": 1, "data": dt.date(2026, 9, 10), "descricao": "TARIFA",
          "documento": "", "valor": Decimal("-9.00"), "omie_codigo": None}],
        {"id": 1, "nome": "X", "omie_conta_corrente": 1})

    assert plano["vai"] == []
    assert "Aplicar atualizações do banco" in plano["nao_vai"][0]["motivo"]
    assert "não reconheci" not in plano["nao_vai"][0]["motivo"]


# ---------------------------------------------------------------------------
# DOIS BOTÕES QUE NÃO FAZIAM NADA — 25/09/2026
#
# Relato do dono, numa frase: *"o botão desfazer o extrato importado não
# funciona. os filtros de cabeçalho tb não estão funcionando."*
#
# As duas coisas existiam na tela, as duas rotas existiam e estavam testadas —
# e nenhum dos dois estava LIGADO. É o pior tipo de defeito: nada estoura, nada
# aparece no log, e a suíte inteira passa. Estes testes são a guarda contra a
# ligação se perder outra vez.
# ---------------------------------------------------------------------------
def _template_da_conciliacao() -> str:
    import pathlib
    return (pathlib.Path(web.__file__).parent / "templates"
            / "analisesps_conciliacao.html").read_text(encoding="utf-8")


def test_o_botao_desfazer_esta_LIGADO_a_alguma_coisa():
    """O botão tinha a classe `desfazer-arquivo` e ninguém escutava por ela:
    clicar não dava erro, não dava recado, não fazia nada."""
    html = _template_da_conciliacao()
    assert 'class="btn secundario desfazer-arquivo"' in html
    assert '.desfazer-arquivo").forEach' in html, (
        "o botão Desfazer voltou a ficar sem quem o escute")
    assert "conciliacao_desfazer" in html, "o botão não chama a rota"


def test_o_desfazer_pergunta_ANTES_e_diz_o_que_se_perde():
    """A rota tem duas chamadas de propósito: a primeira só conta o que
    sumiria. Um botão que apagasse direto tornaria essa separação inútil."""
    html = _template_da_conciliacao()
    trecho = html[html.index('.desfazer-arquivo").forEach'):]
    trecho = trecho[:trecho.index("// --- os filtros de cabeçalho")]
    assert "confirmar: true" in trecho, "apagaria sem confirmar"
    assert trecho.index("confirm(") < trecho.index("confirmar: true"), (
        "confirmou depois de apagar")
    for palavra in ("CONCILIADAS", "OBSERVAÇÃO", "OMIE"):
        assert palavra in trecho, f"a pergunta não diz que se perde {palavra}"


def test_o_filtro_de_cabecalho_tem_botao_de_enviar():
    """⚠️ A REGRA DO HTML QUE FEZ O DEFEITO: num formulário SEM botão de enviar,
    o Enter só envia se houver UM único campo de texto. O filtro de cabeçalho
    tem seis (data, histórico, documento, entrada, saída, observação) — então o
    Enter não fazia absolutamente nada, sem erro nenhum na tela. Só a caixinha
    de situação funcionava, porque envia no `onchange`."""
    html = _template_da_conciliacao()
    inicio = html.index('id="filtro-colunas"')
    forma = html[inicio:html.index("</form>", inicio)]
    assert 'type="submit"' in forma, (
        "o formulário do filtro de cabeçalho voltou a ficar sem botão de "
        "enviar — e aí o Enter não aplica nada")


def test_o_filtro_de_cabecalho_nao_apaga_o_que_a_barra_lateral_filtrou():
    """Antes só `sentido` e `busca` viajavam. Digitar no cabeçalho apagava em
    silêncio a faixa de datas e de valores posta ao lado, e a pessoa via a
    lista mudar sem saber por quê."""
    html = _template_da_conciliacao()
    inicio = html.index('id="filtro-colunas"')
    forma = html[inicio:html.index("</form>", inicio)]
    for campo in ("sentido", "busca", "data_ini", "data_fim",
                  "valor_ini", "valor_fim"):
        assert f"'{campo}'" in forma, (
            f"o filtro de cabeçalho voltou a perder o {campo} da barra lateral")


def test_o_enter_no_filtro_de_cabecalho_tambem_e_tratado_no_script():
    """O botão já resolve pela regra do HTML; o script existe para não depender
    de navegador nenhum — e para a caixinha de data aplicar sozinha quando a
    pessoa escolhe o dia no calendário."""
    html = _template_da_conciliacao()
    assert '[form="filtro-colunas"]' in html
    assert 'e.key === "Enter"' in html


def test_a_tela_PARA_o_dono_quando_ha_linha_presa():
    """⚠️ Gravar com linha presa DUPLICA lançamento. O aviso tem de vir antes do
    botão de gravar, senão ele chega ao botão sem ter lido."""
    html = _template_da_conciliacao()
    assert "d.presas_total" in html, "a tela não usa o aviso das linhas presas"
    assert html.index("d.presas_total") < html.index('id="btn-gravar-extrato"'), (
        "o aviso das presas ficou DEPOIS do botão de gravar")
    assert "conciliacao_soltar_presas" in html, "não há como soltá-las pela tela"


def test_o_aviso_de_sinal_trocado_tambem_vem_antes_do_botao_de_gravar():
    """Mesmo motivo do aviso das presas: depois do botão, ninguém lê."""
    html = _template_da_conciliacao()
    assert "d.sinal_trocado_total" in html
    assert html.index("d.sinal_trocado_total") < html.index('id="btn-gravar-extrato"')


# ---------------------------------------------------------------------------
# O FILTRO QUE NÃO DÁ PARA DESFAZER — 26/09/2026
# ---------------------------------------------------------------------------
def test_filtrar_por_um_dia_VAZIO_nao_pode_sumir_com_os_filtros(app_com_dados,
                                                                monkeypatch):
    """⚠️ DEFEITO ACHADO PELO DONO: *"Se eu colocar uma data que não tem nada,
    ele some com o extrato — obviamente não tem nada, mas também ele some com
    os cabeçalhos. Aí você não pode alterar o filtro."*

    Os filtros de cabeçalho moram no cabeçalho da tabela, e a tabela inteira era
    engolida quando não havia linha. Filtrar por um dia vazio virava um beco sem
    saída: só o "Limpar" salvava, e quem não soubesse concluiria que travou.

    Um filtro que não pode ser desfeito de onde foi feito não é filtro, é
    armadilha."""
    monkeypatch.setattr(conciliacao, "listar", lambda f, pagina=1: [])
    monkeypatch.setattr(conciliacao, "resumo", lambda f: {
        "quantidade": 0, "entradas": Decimal("0"), "saidas": Decimal("0"),
        "saldo": Decimal("0"), "pendentes": 0,
        "pendentes_valor": Decimal("0"), "com_observacao": 0})

    html = como(app_com_dados).get(
        "/analisesps/conciliacao?data=2026-01-01").get_data(as_text=True)

    assert "Nada neste recorte" in html
    # E os filtros CONTINUAM na tela — é isto que estava faltando.
    assert 'id="filtro-colunas"' in html
    assert 'name="historico" form="filtro-colunas"' in html
    assert 'name="data" form="filtro-colunas"' in html
    assert "Mude o filtro acima" in html


def test_com_a_conta_sem_extrato_nenhum_a_tela_diz_isso_e_nao_culpa_o_filtro(
        app_com_dados, monkeypatch):
    """Base vazia e filtro vazio são coisas diferentes, e mandar mexer no filtro
    quando não há extrato nenhum faria a pessoa procurar o que não existe."""
    monkeypatch.setattr(conciliacao, "listar", lambda f, pagina=1: [])
    monkeypatch.setattr(conciliacao, "resumo", lambda f: {
        "quantidade": 0, "entradas": Decimal("0"), "saidas": Decimal("0"),
        "saldo": Decimal("0"), "pendentes": 0,
        "pendentes_valor": Decimal("0"), "com_observacao": 0})

    html = como(app_com_dados).get(
        "/analisesps/conciliacao").get_data(as_text=True)

    assert "ainda não tem extrato importado" in html
    assert "Mude o filtro acima" not in html


def test_sem_linha_nenhuma_a_barra_de_acoes_nao_aparece(app_com_dados,
                                                        monkeypatch):
    """Botão de "conciliar selecionados" com zero linhas na tela é botão que não
    faz nada — e botão que não faz nada é pior que botão nenhum."""
    monkeypatch.setattr(conciliacao, "listar", lambda f, pagina=1: [])
    monkeypatch.setattr(conciliacao, "resumo", lambda f: {
        "quantidade": 0, "entradas": Decimal("0"), "saidas": Decimal("0"),
        "saldo": Decimal("0"), "pendentes": 0,
        "pendentes_valor": Decimal("0"), "com_observacao": 0})

    html = como(app_com_dados).get(
        "/analisesps/conciliacao").get_data(as_text=True)
    assert 'id="btn-conciliar"' not in html


# ---------------------------------------------------------------------------
# APAGAR UMA LINHA — a tela
# ---------------------------------------------------------------------------
def test_cada_linha_tem_como_ser_apagada_e_quem_so_consulta_nao_ve(
        app_com_dados):
    """Pedido do dono em 26/09/2026. Quem só consulta não vê o ×: a rota recusa
    de qualquer jeito, mas mostrar botão que vai recusar é maltratar quem
    clica."""
    html = como(app_com_dados).get(
        "/analisesps/conciliacao").get_data(as_text=True)
    assert 'class="apagar-linha"' in html

    so_le = como(app_com_dados, SENHA_CONSULTA).get(
        "/analisesps/conciliacao").get_data(as_text=True)
    assert 'class="apagar-linha"' not in so_le


def test_apagar_uma_linha_pergunta_DUAS_vezes_e_exige_motivo():
    """⚠️ A CONFIRMAÇÃO FOI PEDIDA JUNTO COM O RECURSO: *"a exclusão tem uma
    confirmação, né? Para garantir que a pessoa está fazendo uma coisa correta.
    Porque não é o certo estar excluindo linhas."*"""
    html = _template_da_conciliacao()
    trecho = html[html.index('.apagar-linha").forEach'):]
    trecho = trecho[:trecho.index("// --- DESFAZER UMA IMPORTAÇÃO")]

    assert "confirm(" in trecho, "apagaria sem perguntar"
    assert "prompt(" in trecho, "não pede o motivo"
    assert trecho.index("confirm(") < trecho.index("confirmar: true"), (
        "perguntou depois de apagar")
    assert trecho.index("prompt(") < trecho.index("confirmar: true")
    # A pergunta tem de dizer o que se perde.
    assert "CONCILIADA" in trecho and "observação" in trecho
    assert "não tem volta" in trecho
    assert "Desfazer do extrato" in trecho, (
        "não aponta o caminho certo para quando o erro foi a importação toda")
