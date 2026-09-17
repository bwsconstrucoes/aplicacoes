# -*- coding: utf-8 -*-
"""VER A NOTA FISCAL na tela, e sair com um PDF — 17/09/2026.

> *"Não tem problema abrir o XML em tela. Abre num modal? E desse modal poderia
> exportar em PDF? Que tal?"*

O QUE ESTE ARQUIVO PROTEGE, e por que cada parte importa:

  1. **a leitura do XML** — um campo lido do lugar errado vira número errado
     com cara de certo, que é o pior defeito possível num documento fiscal;
  2. **o que a nota NÃO informou não vira zero** — escrever "R$ 0,00" onde não
     houve informação é inventar dado;
  3. **o resumo não é desenhado como se fosse a nota** — seria uma folha quase
     vazia com cara de nota fiscal;
  4. **o aviso de que não é DANFE** está no papel — quem recebe um documento
     com cara de nota fiscal supõe que ele vale como uma;
  5. **"ainda não tenho o documento" não é "deu erro"** — é o estado normal de
     toda nota antes da ciência.
"""
from __future__ import annotations

import pytest

from app.apps.analisesps import nfe_leitura


CHAVE = "26260910656452007869550010000014301234567890"


def _nfe(itens=None, totais=None, extra=""):
    """Uma NF-e completa, como a Receita entrega depois da ciência."""
    itens = itens if itens is not None else [
        ("1", "CIM-50", "CIMENTO CP-II 50KG", "25232910", "5102", "SC",
         "100.0000", "32.5000", "3250.00")]
    totais = totais if totais is not None else {
        "vProd": "3250.00", "vFrete": "150.00", "vDesc": "0.00",
        "vBC": "3250.00", "vICMS": "585.00", "vNF": "3400.00"}
    linhas_itens = "".join(
        f'<det nItem="{n}"><prod><cProd>{cod}</cProd><xProd>{desc}</xProd>'
        f"<NCM>{ncm}</NCM><CFOP>{cfop}</CFOP><uCom>{un}</uCom>"
        f"<qCom>{qtd}</qCom><vUnCom>{unit}</vUnCom><vProd>{tot}</vProd>"
        f"</prod></det>"
        for n, cod, desc, ncm, cfop, un, qtd, unit, tot in itens)
    linhas_totais = "".join(f"<{k}>{v}</{k}>" for k, v in totais.items())
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<nfeProc xmlns="http://www.portalfiscal.inf.br/nfe" versao="4.00">'
        f'<NFe><infNFe Id="NFe{CHAVE}" versao="4.00">'
        "<ide><nNF>1430</nNF><serie>1</serie>"
        "<natOp>VENDA DE MERCADORIA</natOp>"
        "<dhEmi>2026-09-10T14:32:00-03:00</dhEmi></ide>"
        "<emit><CNPJ>11222333000181</CNPJ><xNome>SERTAO MATERIAIS LTDA</xNome>"
        "<xFant>SERTAO</xFant><IE>0123456789</IE>"
        "<enderEmit><xLgr>AV BOA VIAGEM</xLgr><nro>1200</nro>"
        "<xBairro>BOA VIAGEM</xBairro><xMun>RECIFE</xMun><UF>PE</UF>"
        "<CEP>51020000</CEP></enderEmit></emit>"
        "<dest><CNPJ>10656452007869</CNPJ>"
        "<xNome>BWS CONSTRUCOES LTDA</xNome><IE>9876543210</IE>"
        "<enderDest><xLgr>RUA DO SOL</xLgr><nro>45</nro>"
        "<xMun>OLINDA</xMun><UF>PE</UF><CEP>53010000</CEP></enderDest></dest>"
        + linhas_itens +
        f"<total><ICMSTot>{linhas_totais}</ICMSTot></total>"
        "<transp><modFrete>1</modFrete>"
        "<transporta><xNome>TRANSPORTES XYZ</xNome>"
        "<CNPJ>99888777000166</CNPJ></transporta>"
        "<veicTransp><placa>ABC1D23</placa></veicTransp></transp>"
        "<infAdic><infCpl>PEDIDO 8842 - OBRA RESIDENCIAL</infCpl></infAdic>"
        + extra +
        "</infNFe></NFe>"
        f"<protNFe><infProt><chNFe>{CHAVE}</chNFe><nProt>126260001234567</nProt>"
        "<dhRecbto>2026-09-10T14:35:00-03:00</dhRecbto>"
        "<cStat>100</cStat><xMotivo>Autorizado o uso da NF-e</xMotivo>"
        "</infProt></protNFe></nfeProc>")


# ---------------------------------------------------------------------------
# A LEITURA — cada campo vindo do lugar certo
# ---------------------------------------------------------------------------
def test_os_dados_da_nota_saem_do_lugar_certo():
    """Um campo lido do lugar errado vira número errado com cara de certo."""
    nota = nfe_leitura.ler(_nfe())

    assert nota["numero"] == "1430"
    assert nota["serie"] == "1"
    assert nota["natureza"] == "VENDA DE MERCADORIA"
    assert nota["emissao"].startswith("2026-09-10")
    assert nota["protocolo"] == "126260001234567"
    assert nota["situacao"] == "Autorizado o uso da NF-e"
    assert nota["chave"] == CHAVE


def test_a_chave_sai_do_ID_e_nao_de_qualquer_campo_de_44_digitos():
    """O `Id` do `infNFe` é a fonte; ele vem com o prefixo "NFe" colado."""
    nota = nfe_leitura.ler(_nfe())
    assert nota["chave"] == CHAVE, "a chave saiu com o 'NFe' colado ou truncada"


def test_quem_EMITIU_e_quem_RECEBEU_nao_se_trocam():
    """⚠️ Trocar os dois faz a nota dizer que a BWS vendeu o que ela comprou —
    e num papel que vai para o processo isso não se percebe lendo rápido."""
    nota = nfe_leitura.ler(_nfe())

    assert nota["emitente"]["nome"] == "SERTAO MATERIAIS LTDA"
    assert nota["emitente"]["doc"] == "11.222.333/0001-81"
    assert nota["destinatario"]["nome"] == "BWS CONSTRUCOES LTDA"
    assert nota["destinatario"]["doc"] == "10.656.452/0078-69"


def test_os_enderecos_tambem_nao_se_trocam():
    """Emitente e destinatário guardam o endereço em etiquetas DIFERENTES
    (`enderEmit` e `enderDest`) — ler a errada devolve vazio, ou pior, o
    endereço do outro."""
    nota = nfe_leitura.ler(_nfe())

    assert nota["emitente"]["endereco"]["municipio"] == "RECIFE"
    assert nota["emitente"]["endereco"]["cep"] == "51020-000"
    assert nota["destinatario"]["endereco"]["municipio"] == "OLINDA"
    assert nota["destinatario"]["endereco"]["cep"] == "53010-000"


def test_as_mercadorias_vem_com_quantidade_e_valor():
    from decimal import Decimal
    nota = nfe_leitura.ler(_nfe())

    assert len(nota["itens"]) == 1
    item = nota["itens"][0]
    assert item["descricao"] == "CIMENTO CP-II 50KG"
    assert item["ncm"] == "25232910"
    assert item["cfop"] == "5102"
    assert item["quantidade"] == Decimal("100.0000")
    assert item["valor_unitario"] == Decimal("32.5000")
    assert item["valor_total"] == Decimal("3250.00")


def test_a_nota_com_MUITOS_itens_traz_todos_na_ordem():
    """Uma nota de material de obra tem dezenas de linhas; parar no primeiro
    faria o papel sair incompleto sem avisar."""
    muitos = [(str(n), f"COD{n}", f"ITEM {n}", "25232910", "5102", "UN",
               "1.0000", "10.00", "10.00") for n in range(1, 31)]
    nota = nfe_leitura.ler(_nfe(itens=muitos))

    assert len(nota["itens"]) == 30
    assert [i["descricao"] for i in nota["itens"][:3]] == [
        "ITEM 1", "ITEM 2", "ITEM 3"]


def test_o_total_da_nota_e_o_vNF_e_nao_a_soma_dos_itens():
    """⚠️ Somar os itens daria 3250 e a nota vale 3400 — a diferença é o frete.
    Um papel que diz 3250 onde a nota diz 3400 é o defeito mais caro possível
    aqui: ele passa despercebido justamente por parecer razoável."""
    from decimal import Decimal
    nota = nfe_leitura.ler(_nfe())

    assert nota["totais"]["nota"] == Decimal("3400.00")
    assert nota["totais"]["produtos"] == Decimal("3250.00")
    assert nota["totais"]["frete"] == Decimal("150.00")


def test_o_que_a_nota_NAO_informou_fica_vazio_e_nao_vira_ZERO():
    """⚠️ "Não informou seguro" e "o seguro é zero" são coisas diferentes.
    Escrever R$ 0,00 onde não houve informação é inventar dado — e a tela
    esconde a linha em vez de mentir."""
    nota = nfe_leitura.ler(_nfe(totais={"vProd": "100.00", "vNF": "100.00"}))

    assert nota["totais"]["seguro"] is None
    assert nota["totais"]["frete"] is None
    assert nota["totais"]["nota"] is not None


def test_o_frete_vira_palavra_e_nao_numero():
    """O XML guarda "1"; quem lê a nota quer "por conta do destinatário"."""
    nota = nfe_leitura.ler(_nfe())
    assert nota["transporte"]["modalidade"] == "Por conta do destinatário"
    assert nota["transporte"]["transportador"] == "TRANSPORTES XYZ"
    assert nota["transporte"]["placa"] == "ABC1D23"


def test_as_informacoes_complementares_aparecem():
    """É onde vem o número do pedido e a obra — quase sempre o que se procura
    ao abrir a nota."""
    nota = nfe_leitura.ler(_nfe())
    assert "PEDIDO 8842" in nota["observacoes"]


def test_a_chave_sai_em_blocos_de_QUATRO_para_conferir():
    """44 dígitos corridos não se conferem contra o portal da Receita."""
    nota = nfe_leitura.ler(_nfe())
    assert nota["chave_bonita"].startswith("2626 0910 6564")
    assert nota["chave_bonita"].replace(" ", "") == CHAVE


# ---------------------------------------------------------------------------
# O QUE NÃO DÁ PARA LER — a recusa tem de explicar
# ---------------------------------------------------------------------------
def test_o_RESUMO_nao_e_desenhado_como_se_fosse_a_nota():
    """⚠️ O resumo tem oito campos e nenhuma mercadoria. Desenhá-lo daria uma
    folha quase vazia com cara de nota fiscal — pior do que dizer "ainda não
    tenho o documento"."""
    resumo = ('<resNFe xmlns="http://www.portalfiscal.inf.br/nfe">'
              f"<chNFe>{CHAVE}</chNFe><vNF>3400.00</vNF></resNFe>")
    with pytest.raises(nfe_leitura.XmlIlegivel) as erro:
        nfe_leitura.ler(resumo)
    # E a recusa diz O QUE FAZER, não só que não deu.
    assert "ciência" in str(erro.value)
    assert "próxima busca" in str(erro.value)


def test_arquivo_que_nao_e_XML_recusa_com_frase_de_gente():
    with pytest.raises(nfe_leitura.XmlIlegivel) as erro:
        nfe_leitura.ler("isto aqui não é um XML")
    assert "XML" in str(erro.value)


def test_arquivo_vazio_recusa_sem_estourar():
    for vazio in ("", "   ", None):
        with pytest.raises(nfe_leitura.XmlIlegivel):
            nfe_leitura.ler(vazio)


def test_nota_com_campo_torto_NAO_derruba_a_leitura_inteira():
    """Um valor ilegível num item não pode custar a nota toda: o resto dos
    dados continua servindo para conferir."""
    torta = _nfe(itens=[("1", "X", "ITEM", "1", "5102", "UN",
                         "isto-nao-e-numero", "10.00", "10.00")])
    nota = nfe_leitura.ler(torta)

    assert nota["itens"][0]["quantidade"] is None
    assert nota["itens"][0]["descricao"] == "ITEM"
    assert nota["numero"] == "1430", "um item torto derrubou a nota inteira"


# ---------------------------------------------------------------------------
# A TELA — o que sai no HTML, com o dado entrando pelo caminho de verdade
# ---------------------------------------------------------------------------
@pytest.fixture
def app_nota(monkeypatch):
    from flask import Flask

    from app.apps.analisesps import notas_arquivo, web

    monkeypatch.setenv("ANALISESPS_SENHA_OPERADOR", "op-teste")
    monkeypatch.setenv("ANALISESPS_SENHA_CONSULTA", "consulta-teste")
    monkeypatch.setattr(notas_arquivo, "baixar_xml", lambda chave: _nfe())

    a = Flask(__name__)
    a.secret_key = "teste"
    a.register_blueprint(web.bp)
    a.config["TESTING"] = True
    return a


def _logado(app):
    cliente = app.test_client()
    cliente.post("/analisesps/entrar",
                 data={"senha": "op-teste", "nome": "MARCELO"})
    return cliente


def test_a_nota_abre_desenhada_e_nao_como_etiquetas_de_XML(app_nota):
    """A entrega: *"abrir o XML em tela"* — mas lido, não cru."""
    html = _logado(app_nota).get(
        f"/analisesps/nota/{CHAVE}?modal=1").get_data(as_text=True)

    assert "SERTAO MATERIAIS LTDA" in html
    assert "CIMENTO CP-II 50KG" in html
    assert "VENDA DE MERCADORIA" in html
    # E nada de etiqueta de XML na cara de quem lê.
    assert "<infNFe" not in html and "nfeProc" not in html


def test_o_papel_AVISA_que_nao_e_um_DANFE(app_nota):
    """⚠️ Quem recebe um documento com cara de nota fiscal supõe que ele vale
    como uma. Este não vale — e o aviso fica no alto, não no rodapé."""
    html = _logado(app_nota).get(
        f"/analisesps/nota/{CHAVE}?modal=1").get_data(as_text=True)
    assert "não é um DANFE oficial" in html


def test_a_folha_de_impressao_chama_a_impressao_sozinha(app_nota):
    """É o "exportar em PDF": quem clicou já disse o que queria."""
    html = _logado(app_nota).get(
        f"/analisesps/nota/{CHAVE}?imprimir=1").get_data(as_text=True)

    assert "window.print()" in html
    assert "Salvar como PDF" in html, "não diz onde está o PDF na caixa"
    # A barra com o botão não pode sair impressa dentro do próprio PDF.
    assert "@media print" in html and "barra-impressao { display: none" in html
    # E a nota está lá, senão o PDF sai em branco.
    assert "CIMENTO CP-II 50KG" in html


def test_a_pagina_inteira_tem_o_botao_de_PDF(app_nota):
    html = _logado(app_nota).get(
        f"/analisesps/nota/{CHAVE}").get_data(as_text=True)
    assert "imprimir=1" in html
    assert "SERTAO MATERIAIS LTDA" in html


def test_sem_documento_a_tela_EXPLICA_em_vez_de_dizer_que_deu_erro(app_nota,
                                                                   monkeypatch):
    """⚠️ É o estado normal de toda nota antes da ciência. "Deu erro" mandaria
    alguém procurar defeito onde só falta esperar a rodada seguinte."""
    from app.apps.analisesps import notas_arquivo

    def sem(chave):
        raise notas_arquivo.SemDocumento(
            "O documento desta nota ainda não chegou. A Receita só entrega a "
            "nota inteira depois da ciência da operação — e ela chega na busca "
            "seguinte, não na hora.")
    monkeypatch.setattr(notas_arquivo, "baixar_xml", sem)

    resposta = _logado(app_nota).get(f"/analisesps/nota/{CHAVE}?modal=1")
    html = resposta.get_data(as_text=True)

    assert resposta.status_code == 200
    assert "ciência da operação" in html
    assert "aviso erro" not in html, "tratou 'ainda não chegou' como falha"


def test_o_Drive_fora_do_ar_e_dito_como_FALHA_de_verdade(app_nota, monkeypatch):
    """Aqui sim é erro — e a diferença entre os dois recados é o que decide se
    alguém vai procurar defeito ou esperar."""
    from app.apps.analisesps import notas_arquivo
    from app.apps.analisesps.drive import ErroDoDrive

    def caiu(chave):
        raise ErroDoDrive("cota da conta de serviço")
    monkeypatch.setattr(notas_arquivo, "baixar_xml", caiu)

    html = _logado(app_nota).get(
        f"/analisesps/nota/{CHAVE}?modal=1").get_data(as_text=True)
    assert "aviso erro" in html
    assert "cota da conta de serviço" in html


def test_a_nota_NAO_abre_para_quem_nao_entrou(app_nota):
    """A nota traz CNPJ, endereço e valores — não é tela pública."""
    resposta = app_nota.test_client().get(f"/analisesps/nota/{CHAVE}")
    assert resposta.status_code in (302, 401, 403), (
        "a nota fiscal abriu sem login")


def test_chave_invalida_nao_vira_CONSULTA_nem_ida_ao_Drive(app_nota,
                                                           monkeypatch):
    """A conferência é na ENTRADA da rota, antes de qualquer busca.

    Quem varrer endereços com números inventados não descobre nada e não custa
    nada ao serviço — nem uma consulta ao banco, nem uma ida ao Google."""
    from app.apps.analisesps import notas_arquivo

    def nao_devia(chave):
        raise AssertionError("foi buscar no Drive com chave inválida")
    monkeypatch.setattr(notas_arquivo, "baixar_xml", nao_devia)

    for inventada in ("123", "abc", "0" * 43, "0" * 45):
        resposta = _logado(app_nota).get(f"/analisesps/nota/{inventada}?modal=1")
        assert resposta.status_code == 404, f"passou: {inventada}"


def test_a_tabela_de_itens_rola_SOZINHA_e_nao_empurra_a_pagina(app_nota):
    """⚠️ São nove colunas. Num celular de 390px a tabela empurrava a PÁGINA
    inteira para o lado — e aí o aviso, o cabeçalho e os totais saíam do campo
    de visão junto com ela.

    Conferido num navegador de verdade a 390px em 17/09/2026: antes do
    envelope a página media 583px de largura contra 390 de tela; depois, 390
    contra 390."""
    html = _logado(app_nota).get(
        f"/analisesps/nota/{CHAVE}?modal=1").get_data(as_text=True)
    assert "nota-doc-itens-rolagem" in html, (
        "a tabela voltou a empurrar a página no celular")


def test_no_PAPEL_o_envelope_de_rolagem_nao_corta_a_tabela(app_nota):
    """Ele existe para a tela. No papel, cortaria a tabela na largura da folha
    em vez de deixar a tabela se ajustar — e as últimas colunas sumiriam do
    PDF sem ninguém perceber."""
    html = _logado(app_nota).get(
        f"/analisesps/nota/{CHAVE}?imprimir=1").get_data(as_text=True)
    assert "nota-doc-itens-rolagem { overflow: visible" in html
