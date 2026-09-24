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
