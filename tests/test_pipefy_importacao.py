# -*- coding: utf-8 -*-
"""TRAZER DO PIPEFY O QUE FICOU LÁ (migração 101, 10/11/2026).

O dono: *"Olhando os cadastros, a planilha lá, dados e documentos, tem o
número do card do pipe, certo? Então, tu não consegue fazer logo essa
importação, não? Faz a leitura do card, baixa os documentos, importa para o
ERP. Só das pessoas que você já sabe que tem que estar."*

O QUE ESTES TESTES DEFENDEM, e cada um é um jeito conhecido de uma importação
estragar o cadastro:

1. **nunca sobrescrever** o que já está preenchido aqui;
2. **só quem tem que estar** — pessoa ativa com card, não os 3.664 do pipe;
3. **a segunda passada não baixa de novo** (e isso tem de valer SEM baixar);
4. **um arquivo que falha não derruba os outros** — a armadilha do Postgres,
   em que uma consulta recusada aborta a transação inteira;
5. **o que não entrou é DITO**, com o motivo.

⚑ NADA AQUI VAI À REDE. Os cards chegam por `ler_cards`, que a importação
aceita de fora justamente para o teste poder entregar card de mentira.
"""
from __future__ import annotations

import pytest
from sqlalchemy import text

from app.apps.erp.core.cadastros import pessoas as svc_pessoas
from app.apps.erp.core.pipefy import cadastro_importacao as imp
from app.apps.erp.core.comum.auditoria import ErroValidacao
from app.apps.erp.core.pipefy import cliente

pytestmark = pytest.mark.banco

CPF_A = "52998224725"
CPF_B = "11144477735"
PIS_A = "123.45678.90-0"


@pytest.fixture
def mundo(sessao_real):
    from app.apps.erp.core.auth.service import gerar_hash
    from app.apps.erp.db.models.cadastros import Obra, PerfilUsuario as P, Usuario

    from app.apps.erp.core.arquivo import catalogo
    catalogo.aplicar(sessao_real)

    dp = Usuario(nome="dp-pipefy", email="dp@pipefy.teste", ativo=True,
                 senha_hash=gerar_hash("senha-de-teste-123"),
                 perfil=P.DEPARTAMENTO_PESSOAL, telefone="5585900000009")
    obra = Obra(codigo="PIP-01", nome="Obra do pipe", status="ATIVA")
    sessao_real.add_all([dp, obra])
    sessao_real.flush()

    # Uma pessoa com card, o PIS JÁ preenchido e o resto vazio.
    com_card = svc_pessoas.salvar(sessao_real, {
        "cpf": CPF_A, "nome": "Pessoa Com Card", "obra_id": obra.id,
        "pis": PIS_A, "vinculo": "CTPS", "admissao": "2024-01-05"}, dp)
    sessao_real.execute(
        text("UPDATE colaboradores SET ref_pipefy = '853990032' WHERE id = :i"),
        {"i": com_card.id})

    # Outra sem card nenhum: não pode entrar na fila.
    sem_card = svc_pessoas.salvar(sessao_real, {
        "cpf": CPF_B, "nome": "Pessoa Sem Card", "obra_id": obra.id,
        "vinculo": "CTPS", "admissao": "2024-02-05"}, dp)
    sessao_real.flush()
    return {"s": sessao_real, "dp": dp, "obra": obra,
            "com_card": com_card, "sem_card": sem_card}


# ⚑ NÚMERO DE CARD DE VERDADE TEM 9 DÍGITOS, e o teste usa um assim de
# propósito: a leitura da lista colada só aceita 6 dígitos ou mais (a mesma
# regra do importador de SPs, que roda há meses). A primeira versão deste
# arquivo usava "111" e o card simplesmente não era pedido — o teste falhava
# por um motivo que não tinha nada a ver com o que ele defendia.
CARD = "853990032"


def _card(**campos):
    """Um card de mentira, no formato que `ler_cards` devolve.

    O CPF vai junto por padrão: é por ele que o card acha a pessoa."""
    campos.setdefault("cpf", CPF_A)
    return {"853990032": {"id": "853990032", "titulo": "Pessoa Com Card",
                    "fase": "Cadastro Concluído", "campos": dict(campos)}}


def _leitor(cards, falhas=None):
    def ler(ids):
        return cards, (falhas or {})
    return ler


# ---------------------------------------------------------------------------
# Quem entra na fila
# ---------------------------------------------------------------------------
def test_quem_nao_tem_card_fica_de_fora(mundo):
    fila = imp.quem_tem_card(mundo["s"])
    ids = {p["id"] for p in fila}
    assert mundo["com_card"].id in ids
    assert mundo["sem_card"].id not in ids


def test_desligado_nao_entra_por_padrao(mundo):
    """*"Só das pessoas que você já sabe que tem que estar."* O pipe tem 3.664
    cards; baixar o histórico de quem saiu há cinco anos são horas de rede e
    um monte de documento de gente que não trabalha mais aqui."""
    mundo["s"].execute(
        text("UPDATE colaboradores SET situacao = 'DESLIGADO' WHERE id = :i"),
        {"i": mundo["com_card"].id})
    assert imp.quem_tem_card(mundo["s"]) == []
    # Mas dá para pedir todo mundo de propósito.
    assert len(imp.quem_tem_card(mundo["s"], so_ativos=False)) == 1


def test_a_situacao_conta_quem_tem_card_e_quem_nao_tem(mundo):
    s = imp.situacao(mundo["s"])
    assert s["ativos"] == 2 and s["ativos_com_card"] == 1 and s["sem_card"] == 1
    assert "tem_token" in s


# ---------------------------------------------------------------------------
# A regra que mais importa: nunca sobrescrever
# ---------------------------------------------------------------------------
def test_campo_ja_preenchido_aqui_NAO_e_trocado_pelo_do_card(mundo):
    """O cadastro daqui foi conferido por gente; o card pode ter dois anos."""
    relatorio = imp.importar(
        mundo["s"], mundo["dp"], simular=False,
        cards="853990032", ler_cards=_leitor(_card(pis="99999999999", nome_da_m_e="Maria Mãe")))
    mundo["s"].flush()
    mundo["s"].refresh(mundo["com_card"])
    assert mundo["com_card"].pis == PIS_A          # o do ERP, não o do card
    assert mundo["com_card"].nome_mae == "Maria Mãe"   # este estava vazio
    assert relatorio["campos_preenchidos"] == 1


def test_o_que_o_card_tem_fora_da_lista_fechada_e_descartado(mundo):
    """"Divorciado em 2020" não é opção do cadastro. Entrar com ele faria a
    conferência do cadastro recusar a pessoa inteira."""
    achados = imp.dados_do_card({"estado_civil": "Divorciado em 2020",
                                 "sexo": "Masculino"})
    assert "estado_civil" not in achados
    # ⚑ E "Masculino" vira "M" pela TABELA, não porque M é o começo de
    # Masculino: acertar por acaso funciona no teste e erra no dado real.
    assert achados["sexo"] == "M"


def test_o_cadastro_e_gravado_pelas_funcoes_da_tela_e_recusa_dado_invalido(mundo):
    """Importação não pode ser a porta dos fundos por onde entra o que a tela
    barraria — um PIS inválido, por exemplo."""
    from app.apps.erp.core.comum.auditoria import ErroValidacao
    with pytest.raises(ErroValidacao):
        imp._gravar_campos(mundo["s"], mundo["com_card"].id,
                           {"pis": "11111111111"}, mundo["dp"])


# ---------------------------------------------------------------------------
# Os documentos
# ---------------------------------------------------------------------------
def test_cada_anexo_do_card_sai_com_o_tipo_do_catalogo(mundo):
    anexos = imp.anexos_do_card({
        "carteira_de_trabalho": '["https://x/ctps-foto.jpg"]',
        "ctps_1": '["https://x/conta-de-luz.pdf"]',
        "documentos_diversos": '["https://x/a.pdf", "https://x/b.pdf"]'})
    por_tipo = {a["tipo"] for a in anexos}
    assert "CTPS-FOTO" in por_tipo and "COMPROVANTE-ENDERECO-PESSOA" in por_tipo
    # Campo com DOIS arquivos vira DOIS documentos, com item 0 e 1 — sem isso
    # o segundo nunca entraria.
    diversos = sorted(a["item"] for a in anexos if a["campo"] == "documentos_diversos")
    assert diversos == [0, 1]


def test_todo_tipo_do_de_para_existe_no_catalogo(mundo):
    """Tipo inventado aqui faria TODOS os documentos daquele campo falharem,
    um por um, sem ninguém entender por quê."""
    from app.apps.erp.core.arquivo import catalogo
    escritos = {linha[0] for linha in catalogo.CATALOGO}
    for campo, tipo in imp.PAPEIS.items():
        assert tipo in escritos, f"{campo} aponta para {tipo}, que não existe"
    codigos = {l[0] for l in mundo["s"].execute(
        text("SELECT codigo FROM documento_tipos")).all()}
    for campo, tipo in imp.PAPEIS.items():
        assert tipo in codigos, f"{campo} aponta para {tipo}, que não existe"


def test_a_simulacao_nao_grava_nada_e_nao_baixa_nada(mundo, monkeypatch):
    def nao_pode_baixar(*a, **k):                # pragma: no cover
        raise AssertionError("a simulação não pode baixar arquivo")
    monkeypatch.setattr(cliente, "baixar", nao_pode_baixar)

    relatorio = imp.importar(
        mundo["s"], mundo["dp"], simular=True,
        cards="853990032", ler_cards=_leitor(_card(carteira_de_trabalho='["https://x/c.jpg"]',
                                nome_da_m_e="Maria Mãe")))
    assert relatorio["simulado"] is True
    assert relatorio["documentos_guardados"] == 1     # "VAI ENTRAR"
    mundo["s"].flush()
    mundo["s"].refresh(mundo["com_card"])
    assert mundo["com_card"].nome_mae is None
    assert mundo["s"].execute(
        text("SELECT count(*) FROM pipefy_importado")).scalar() == 0


def test_o_documento_entra_no_cadastro_da_pessoa(mundo, monkeypatch):
    monkeypatch.setattr(cliente, "baixar", lambda e, t: b"%PDF-1.4 fingindo")
    imp.importar(mundo["s"], mundo["dp"], simular=False,
                 cards="853990032", ler_cards=_leitor(_card(ctps_1='["https://x/conta.pdf"]')))
    mundo["s"].flush()
    quantos = mundo["s"].execute(text("""
        SELECT count(*) FROM documentos
         WHERE colaborador_id = :i AND tipo_codigo = 'COMPROVANTE-ENDERECO-PESSOA'
           AND origem = 'PIPEFY'"""), {"i": mundo["com_card"].id}).scalar()
    assert quantos == 1


def test_a_segunda_passada_nao_baixa_de_novo(mundo, monkeypatch):
    """⚑ E TEM DE VALER SEM BAIXAR. O ERP já recusa guardar duas vezes o mesmo
    arquivo — só que, para comparar, precisa TER BAIXADO. Com 575 pessoas
    seriam milhares de downloads para concluir que nada mudou."""
    monkeypatch.setattr(cliente, "baixar", lambda e, t: b"%PDF-1.4 fingindo")
    cards = _card(ctps_1='["https://x/conta.pdf"]')
    imp.importar(mundo["s"], mundo["dp"], simular=False,
                 cards="853990032", ler_cards=_leitor(cards))
    mundo["s"].flush()

    def nao_pode_baixar(*a, **k):                # pragma: no cover
        raise AssertionError("a segunda passada não pode baixar de novo")
    monkeypatch.setattr(cliente, "baixar", nao_pode_baixar)
    de_novo = imp.importar(mundo["s"], mundo["dp"], simular=False,
                           cards="853990032", ler_cards=_leitor(cards))
    assert de_novo["documentos_ja_tinha"] == 1
    assert de_novo["documentos_guardados"] == 0


# ---------------------------------------------------------------------------
# O que dá errado — e tem de ser dito
# ---------------------------------------------------------------------------
def test_um_arquivo_que_falha_NAO_derruba_os_outros(mundo, monkeypatch):
    """A armadilha do Postgres: uma consulta recusada aborta a transação
    INTEIRA, mesmo pega por `except`. Sem savepoint por arquivo, um anexo
    problemático levaria junto as 49 pessoas seguintes da rodada."""
    def as_vezes(endereco, teto):
        if "quebrado" in endereco:
            raise cliente.ErroDoPipefy("o arquivo não veio (HTTP 404).")
        return b"%PDF-1.4 fingindo"
    monkeypatch.setattr(cliente, "baixar", as_vezes)

    relatorio = imp.importar(
        mundo["s"], mundo["dp"], simular=False,
        cards="853990032", ler_cards=_leitor(_card(ctps_1='["https://x/quebrado.pdf"]',
                                attachment_t_tulo='["https://x/cpf.pdf"]')))
    assert relatorio["documentos_que_falharam"] == 1
    assert relatorio["documentos_guardados"] == 1
    assert relatorio["falhas"] and "404" in relatorio["falhas"][0]["motivo"]
    mundo["s"].flush()
    # O que deu certo ficou gravado, apesar do que falhou.
    assert mundo["s"].execute(text("""
        SELECT count(*) FROM documentos
         WHERE colaborador_id = :i AND tipo_codigo = 'CPF-DOC'"""),
        {"i": mundo["com_card"].id}).scalar() == 1


def test_a_falha_fica_registrada_com_o_motivo(mundo, monkeypatch):
    monkeypatch.setattr(cliente, "baixar", lambda e, t: (_ for _ in ()).throw(
        cliente.ErroDoPipefy("arquivo acima de 25 MB — não baixei.")))
    imp.importar(mundo["s"], mundo["dp"], simular=False,
                 cards="853990032", ler_cards=_leitor(_card(foto='["https://x/f.jpg"]')))
    mundo["s"].flush()
    linha = mundo["s"].execute(text("""
        SELECT resultado, detalhe FROM pipefy_importado
         WHERE card = '853990032' AND campo = 'foto'""")).first()
    assert linha is not None and linha[0] == "FALHOU"
    assert "25 MB" in linha[1]


def test_card_que_o_pipefy_nao_devolve_vira_linha_do_relatorio(mundo):
    relatorio = imp.importar(
        mundo["s"], mundo["dp"], simular=True,
        cards="853990032", ler_cards=_leitor({}, {"853990032": "card apagado"}))
    assert relatorio["pessoas_lidas"] == 0
    assert relatorio["cards_sem_resposta"][0]["card"] == "853990032"


def test_o_relatorio_diz_o_que_ficou_de_fora(mundo):
    """O ASO não entra porque vence e o card não diz até quando. Omitir isso
    faria o DP achar que o atestado foi importado."""
    relatorio = imp.importar(mundo["s"], mundo["dp"], simular=True,
                             cards="853990032", ler_cards=_leitor(_card()))
    assert "aso" in relatorio["de_fora"]


def test_o_anexo_entra_vindo_COMO_ENDERECO_OU_COMO_CAMINHO(mundo):
    """⚑ ESTE TESTE JÁ DEFENDEU O CONTRÁRIO, e a história importa.

    Ele exigia que SÓ o endereço completo valesse — porque o importador de SPs
    deste monorepo faz assim e funciona há meses. Em 13/11/2026 ele usou a
    importação do cadastro e: *"a documentação que está anexada no pipe não
    veio para cá (…) não veio nenhum documento"*.

    São pipes diferentes, e o Pipefy devolve o campo de anexo ora como
    endereço completo, ora como o caminho do arquivo no armazenamento.
    Recusar o caminho era jogar fora TODOS os documentos — que é exatamente o
    que aconteceu.

    O que continua valendo: texto que não se parece com nenhuma das duas
    formas NÃO vira endereço inventado.
    """
    assert cliente.endereco_do_anexo("https://x/a.pdf") == "https://x/a.pdf"
    assert cliente.endereco_do_anexo({"url": "https://x/d.pdf"}) == "https://x/d.pdf"
    caminho = cliente.endereco_do_anexo("orgs/1/uploads/2/b.pdf")
    assert caminho.startswith(cliente.RAIZ_DO_ARQUIVO)
    assert caminho.endswith("orgs/1/uploads/2/b.pdf")
    # Lixo continua sendo lixo.
    assert cliente.endereco_do_anexo("sei la o que") == ""
    assert cliente.endereco_do_anexo("") == ""

    relatorio = imp.importar(
        mundo["s"], mundo["dp"], simular=True, cards=CARD,
        ler_cards=_leitor(_card(foto='["orgs/1/uploads/2/f.jpg"]')))
    assert relatorio["documentos_guardados"] == 1      # VAI ENTRAR
    assert relatorio["documentos_que_falharam"] == 0


def test_a_conferencia_mostra_o_que_o_pipe_devolveu_em_cada_anexo(mundo):
    """⚑ A trava contra o mistério. Quando nenhum documento veio, o relatório
    não dizia NADA — campo vazio e campo que eu não soube ler davam o mesmo
    silêncio, e descobrir o porquê custou uma ida e volta com ele.

    Agora a conferência mostra, campo por campo, quantos itens vieram, de que
    forma, e uma amostra do que o Pipefy devolveu."""
    relatorio = imp.importar(
        mundo["s"], mundo["dp"], simular=True, cards=CARD,
        ler_cards=_leitor(_card(ctps_1='["orgs/9/uploads/8/conta.pdf"]',
                                foto='["https://x/f.jpg"]')))
    raio = {x["campo"]: x for x in relatorio["por_pessoa"][0]["anexos_do_card"]}
    assert raio["ctps_1"]["quantos"] == 1
    assert raio["ctps_1"]["formas"] == ["caminho no armazenamento"]
    assert "orgs/9" in raio["ctps_1"]["amostra"]
    assert raio["foto"]["formas"] == ["endereço completo"]
    # Campo que o card não tem aparece com zero — e não some do relatório.
    assert raio["documentos_diversos"]["quantos"] == 0
    assert relatorio["anexos_no_pipe"] == 2


def test_numero_de_card_que_nao_e_numero_nao_vira_consulta(mundo):
    """Os ids entram na consulta SEM aspas — texto solto ali seria injeção."""
    assert cliente.card_valido("853990032") is True
    assert cliente.card_valido("111) { pipes { id } } #") is False
    with pytest.raises(cliente.ErroDoPipefy):
        cliente._numero_do_card("não é número")




# ---------------------------------------------------------------------------
# A LISTA É DELE (13/11/2026, a segunda correção do mesmo dia)
#
# A primeira versão varria o pipe inteiro e casava pelo CPF. Ele viu o
# resultado e derrubou:
#
#   *"eu prefiro listar os cards, porque você está trazendo card duplicado, só
#   que aí tem card que está cancelado, isso aqui não me interessa. Eu vou
#   listar os cards que eu quero que você busque. E você faz a pesquisa card a
#   card e puxa as informações e grava."*
#
# Tem razão: card cancelado e card repetido são lixo que só QUEM CONHECE o
# pipe sabe separar — e ele tem a planilha com os números certos.
# ---------------------------------------------------------------------------
def test_a_lista_colada_aceita_todo_jeito_de_colar():
    """Coluna de planilha, vírgula, endereço do card. Repetido entra uma vez."""
    achados = imp.ids_dos_cards(
        "853990032\n1329563540, 1332040352\n"
        "https://app.pipefy.com/open-cards/1329567174\n853990032")
    assert achados == ["853990032", "1329563540", "1332040352", "1329567174"]


def test_a_lista_ignora_o_que_nao_e_numero_de_card():
    """Cabeçalho de coluna e número curto não viram card."""
    assert imp.ids_dos_cards("Nº do card\nabc 12 345678901 x") == ["345678901"]
    assert imp.ids_dos_cards("") == []
    assert imp.ids_dos_cards(None) == []


def test_sem_lista_a_importacao_nao_faz_nada(mundo):
    """E diz por quê — em vez de varrer o pipe por conta própria."""
    r = imp.importar(mundo["s"], mundo["dp"], simular=True, cards="")
    assert r["pessoas_na_fila"] == 0 and r["motivo"]


def test_busca_SO_os_cards_da_lista(mundo, monkeypatch):
    """⚑ O teste que guarda o pedido dele: o que não está na lista não é
    buscado. Card cancelado e card repetido ficam de fora porque ELE não os
    listou — não porque o sistema tentou adivinhar."""
    pedidos = {}

    def ler(ids):
        pedidos["ids"] = list(ids)
        return _card(cpf=CPF_A), {}

    r = imp.importar(mundo["s"], mundo["dp"], simular=True,
                     cards=f"{CARD}\n1332040352\n{CARD}", ler_cards=ler)
    assert pedidos["ids"] == [CARD, "1332040352"]          # sem o repetido
    assert r["cards_pedidos"] == 2


def test_a_pessoa_vem_do_CPF_DO_CARD_e_nao_do_cadastro(mundo):
    """O cadastro não tem o número do card — foi o que quebrou na primeira
    versão. Quem liga card e pessoa é o CPF que vem dentro do card."""
    mundo["s"].execute(text("UPDATE colaboradores SET ref_pipefy = NULL"))
    mundo["s"].flush()
    r = imp.importar(mundo["s"], mundo["dp"], simular=True, cards="853990032",
                     ler_cards=_leitor(_card(cpf=CPF_A)))
    assert r["pessoas_na_fila"] == 1
    assert r["por_pessoa"][0]["colaborador_id"] == mundo["com_card"].id


def test_card_sem_CPF_e_CPF_que_nao_existe_aqui_viram_linha_do_relatorio(mundo):
    r = imp.importar(mundo["s"], mundo["dp"], simular=True, cards="853990032",
                     ler_cards=_leitor(_card(cpf="")))
    assert r["pessoas_na_fila"] == 0
    assert "CPF" in r["cards_sem_pessoa"][0]["motivo"]

    r2 = imp.importar(mundo["s"], mundo["dp"], simular=True, cards="853990032",
                      ler_cards=_leitor(_card(cpf="63511291024")))
    assert r2["pessoas_na_fila"] == 0
    assert "não cria pessoa" in r2["cards_sem_pessoa"][0]["motivo"]


def test_gravar_de_verdade_guarda_o_numero_do_card_na_pessoa(mundo, monkeypatch):
    """A procedência: de qual card veio a ficha. E evita buscar de novo."""
    monkeypatch.setattr(cliente, "baixar", lambda e, t: b"%PDF-1.4 fingindo")
    mundo["s"].execute(text("UPDATE colaboradores SET ref_pipefy = NULL"))
    mundo["s"].flush()
    outro = "1329567174"
    imp.importar(mundo["s"], mundo["dp"], simular=False, cards=outro,
                 ler_cards=_leitor({outro: {"id": outro, "titulo": "x",
                                            "fase": "", "campos": {"cpf": CPF_A}}}))
    mundo["s"].flush()
    assert mundo["s"].execute(
        text("SELECT ref_pipefy FROM colaboradores WHERE id = :i"),
        {"i": mundo["com_card"].id}).scalar() == outro


# ---------------------------------------------------------------------------
# O QUE O CARD NÃO DISSE, O PAPEL PODE DIZER (13/11/2026)
#
# Ele: *"teve um determinado cadastro aí que a gente tem o PIS gravado no
# pipe, mas ele não identificou o PIS, deve ter dado alguma divergência (…) eu
# quero assim: vai importar, se tem alguma coisa faltando, pô, lê os
# documentos, lê os anexos do pipe."*
# ---------------------------------------------------------------------------
def _leitor_de_papel(**campos):
    """Um leitor de documento de mentira — nada vai à IA nestes testes."""
    def ler(conteudo, nome, tipo):
        return {"campos": dict(campos), "resumo": "", "aviso": ""}
    return ler


def test_o_papel_preenche_o_que_o_card_deixou_em_branco(mundo, monkeypatch):
    monkeypatch.setattr(cliente, "baixar", lambda e, t: b"%PDF-1.4 fingindo")
    mundo["s"].execute(text("UPDATE colaboradores SET pis = NULL WHERE id = :i"),
                       {"i": mundo["com_card"].id})
    mundo["s"].flush()
    # ⚑ `expire_all` depois de um UPDATE cru: senão o objeto do ORM continua
    # com o valor antigo na memória, e gravar o MESMO valor não gera UPDATE
    # nenhum — o teste falharia por um motivo que não existe em produção (lá o
    # objeto é lido fresco do banco).
    mundo["s"].expire_all()
    r = imp.importar(
        mundo["s"], mundo["dp"], simular=False, cards=CARD,
        ler_cards=_leitor(_card(
            ctps_p_gina_com_as_informa_es_do_colaborador='["https://x/ctps.jpg"]')),
        ler_papel=_leitor_de_papel(num_pis=PIS_A))
    mundo["s"].flush()
    assert r["campos_do_documento"] == 1
    mundo["s"].refresh(mundo["com_card"])
    assert mundo["com_card"].pis == PIS_A


def test_o_papel_NAO_troca_o_que_ja_estava_preenchido(mundo, monkeypatch):
    """A leitura entra pela mesma porta dos outros campos: só buraco."""
    monkeypatch.setattr(cliente, "baixar", lambda e, t: b"%PDF-1.4 fingindo")
    r = imp.importar(
        mundo["s"], mundo["dp"], simular=False, cards=CARD,
        ler_cards=_leitor(_card(
            ctps_p_gina_com_as_informa_es_do_colaborador='["https://x/ctps.jpg"]')),
        ler_papel=_leitor_de_papel(num_pis="99999999999"))
    mundo["s"].flush()
    assert r["campos_do_documento"] == 0
    mundo["s"].refresh(mundo["com_card"])
    assert mundo["com_card"].pis == PIS_A


def test_a_simulacao_NAO_le_documento(mundo):
    """Leitura por IA custa dinheiro, e a conferência não baixa arquivo."""
    def nao_pode(*a, **k):                               # pragma: no cover
        raise AssertionError("a simulação não pode ler documento")
    r = imp.importar(
        mundo["s"], mundo["dp"], simular=True, cards=CARD,
        ler_cards=_leitor(_card(
            ctps_p_gina_com_as_informa_es_do_colaborador='["https://x/c.jpg"]')),
        ler_papel=nao_pode)
    assert r["campos_do_documento"] == 0


def test_quem_esta_completo_nao_gasta_leitura(mundo, monkeypatch):
    """Só lê quem tem buraco — senão a carga inicial viraria uma fatura."""
    monkeypatch.setattr(cliente, "baixar", lambda e, t: b"%PDF-1.4 fingindo")
    mundo["s"].execute(text("""
        UPDATE colaboradores SET pis = :p, ctps_numero = '123',
               data_nascimento = '1990-01-01', telefone = '5585999990000'
         WHERE id = :i"""), {"p": PIS_A, "i": mundo["com_card"].id})
    mundo["s"].flush()

    def nao_pode(*a, **k):                               # pragma: no cover
        raise AssertionError("não devia ler: a pessoa está completa")
    imp.importar(
        mundo["s"], mundo["dp"], simular=False, cards=CARD,
        ler_cards=_leitor(_card(
            ctps_p_gina_com_as_informa_es_do_colaborador='["https://x/c.jpg"]')),
        ler_papel=nao_pode)


# ---------------------------------------------------------------------------
# REGRAVAR — só para a carga inicial
# ---------------------------------------------------------------------------
def test_regravar_traz_de_novo_e_por_cima(mundo, monkeypatch):
    """Ele: *"isso aí vai acontecer só nesse começo. Então, se eu precisar
    reimportar, eu quero reimportar e regravar as coisas."*"""
    monkeypatch.setattr(cliente, "baixar", lambda e, t: b"%PDF-1.4 fingindo")
    cards = _card(ctps_1='["https://x/conta.pdf"]', nome_da_m_e="Mãe Antiga")
    imp.importar(mundo["s"], mundo["dp"], simular=False, cards=CARD,
                 ler_cards=_leitor(cards), ler_papel=False)
    mundo["s"].flush()

    # Sem regravar, a segunda passada não mexe em nada.
    de_novo = imp.importar(mundo["s"], mundo["dp"], simular=False, cards=CARD,
                           ler_cards=_leitor(_card(nome_da_m_e="Mãe Nova")),
                           ler_papel=False)
    assert de_novo["campos_preenchidos"] == 0

    # Regravando, o card manda.
    forcado = imp.importar(mundo["s"], mundo["dp"], simular=False, cards=CARD,
                           ler_cards=_leitor(_card(nome_da_m_e="Mãe Nova")),
                           regravar=True, ler_papel=False)
    mundo["s"].flush()
    assert forcado["regravando"] is True
    assert forcado["campos_preenchidos"] >= 1
    mundo["s"].refresh(mundo["com_card"])
    assert mundo["com_card"].nome_mae == "Mãe Nova"


# ---------------------------------------------------------------------------
# A EMPRESA PELO CNPJ, e a matrícula que saiu
# ---------------------------------------------------------------------------
def test_a_empresa_vem_pelo_CNPJ_do_card(mundo):
    """Ele: *"o que o ERP grava é um pouquinho diferente do que está na
    planilha, mas na planilha dá para localizar o CNPJ."* O texto difere dos
    dois lados; os 14 dígitos, não."""
    from app.apps.erp.db.models.cadastros import Empresa
    emp = Empresa(razao_social="BWS CONSTRUCOES LTDA", cnpj="00.079.526/0001-09",
                  nome_fantasia="BWS")
    mundo["s"].add(emp)
    mundo["s"].flush()
    achada = imp.empresa_do_card(
        mundo["s"], {"cnpj_para_admiss_o": "00.079.526/0001-09 - MATRIZ/CE"})
    assert achada == emp.id
    # O outro campo do pipe também serve, e com pontuação diferente.
    assert imp.empresa_do_card(
        mundo["s"], {"empresa_para_registro": "00079526000109 - BWS"}) == emp.id
    # CNPJ que não está no cadastro não inventa empresa.
    assert imp.empresa_do_card(
        mundo["s"], {"cnpj_para_admiss_o": "11.222.333/0001-81 - X"}) is None


def test_a_matricula_nao_e_mais_importada(mundo):
    """Ele: *"essa informação de matrícula, cara, isso aqui não precisa não.
    A gente vai usar o ID Fortes, o CPF aqui dentro do nosso sistema."*"""
    achados = imp.dados_do_card({"matr_cula": "12345", "n_identidade": "99"})
    assert "matricula" not in achados
    assert achados["rg"] == "99"


# ---------------------------------------------------------------------------
# O CONFRONTO: cadastro × card × documento (13/11/2026)
#
# Ele: *"a gente tem a IA aqui para trabalhar com a gente. Seria a hora, na
# importação, da gente confrontar dados dos documentos. Verificar se o nome
# cadastrado está batendo com o que está no pipe, com o que está no documento
# da pessoa (…) porque o que tem mais força é o documento da pessoa."*
# ---------------------------------------------------------------------------
def test_quando_os_tres_concordam_nao_ha_divergencia(mundo):
    r = imp.confrontar(mundo["s"], mundo["com_card"].id,
                       {"nome_completo": "Pessoa Com Card"},
                       {"nome": "Pessoa Com Card"})
    assert r == []


def test_diferenca_so_de_acento_e_de_caixa_NAO_e_divergencia(mundo):
    """Senão a tela encheria de ruído e ninguém olharia o que importa."""
    mundo["s"].execute(
        text("UPDATE colaboradores SET nome = 'José Da Silva' WHERE id = :i"),
        {"i": mundo["com_card"].id})
    mundo["s"].flush()
    r = imp.confrontar(mundo["s"], mundo["com_card"].id,
                       {"nome_completo": "JOSE DA SILVA"},
                       {"nome": "jose da silva"})
    assert r == []
    # E o mesmo para número com e sem pontuação.
    r2 = imp.confrontar(mundo["s"], mundo["com_card"].id,
                        {"pis": "12345678900"}, {"num_pis": "123.45678.90-0"})
    assert [x["campo"] for x in r2] == []


def test_o_documento_tem_mais_forca_que_o_card_e_que_o_cadastro(mundo):
    r = imp.confrontar(mundo["s"], mundo["com_card"].id,
                       {"nome_completo": "Nome Do Card"},
                       {"nome": "Nome Do Documento"})
    assert len(r) == 1
    linha = r[0]
    assert linha["campo"] == "nome"
    assert linha["no_cadastro"] == "Pessoa Com Card"
    assert linha["no_card"] == "Nome Do Card"
    assert linha["no_documento"] == "Nome Do Documento"
    assert linha["mais_forte"] == "documento"
    assert linha["sugerido"] == "Nome Do Documento"


def test_sem_documento_quem_manda_e_o_card(mundo):
    r = imp.confrontar(mundo["s"], mundo["com_card"].id,
                       {"nome_completo": "Nome Do Card"}, {})
    assert r[0]["mais_forte"] == "card" and r[0]["sugerido"] == "Nome Do Card"


def test_uma_fonte_so_nao_contradiz_ninguem(mundo):
    """Card vazio e documento vazio não são "divergência com o cadastro"."""
    assert imp.confrontar(mundo["s"], mundo["com_card"].id, {}, {}) == []


def test_a_divergencia_NAO_grava_sozinha(mundo, monkeypatch):
    """⚑ Quem lê o documento é uma IA, que erra em silêncio. Trocar o nome de
    alguém na folha por causa de uma leitura torta é estrago que só aparece
    quando o pagamento volta. A divergência é mostrada; alguém confirma."""
    monkeypatch.setattr(cliente, "baixar", lambda e, t: b"%PDF-1.4 fingindo")
    antes = mundo["com_card"].nome
    r = imp.importar(
        mundo["s"], mundo["dp"], simular=False, cards=CARD,
        ler_cards=_leitor(_card(
            nome_completo="Nome Diferente Do Card",
            ctps_p_gina_com_as_informa_es_do_colaborador='["https://x/c.jpg"]')),
        ler_papel=_leitor_de_papel(nome="Nome Do Documento"))
    mundo["s"].flush()
    mundo["s"].refresh(mundo["com_card"])
    assert mundo["com_card"].nome == antes          # nada foi trocado
    assert r["divergencias"] >= 1
    assert r["por_pessoa"][0]["divergencias"][0]["mais_forte"] == "documento"


# ---------------------------------------------------------------------------
# A DIVERGÊNCIA VIRA PERGUNTA, E O HUMANO DECIDE (migração 102, 13/11/2026)
#
# Ele: *"em relação a IA errar a leitura, eu acho que pode fazer o seguinte:
# quando houver divergência entre o que a IA lê e o que está cadastrado, aí
# você questiona. E o humano decide."*
# ---------------------------------------------------------------------------
def _abrir_uma_divergencia(mundo, **papel):
    from unittest.mock import patch
    with patch.object(cliente, "baixar", lambda e, t: b"%PDF-1.4 fingindo"):
        return imp.importar(
            mundo["s"], mundo["dp"], simular=False, cards=CARD,
            ler_cards=_leitor(_card(
                nome_completo="Nome Do Card",
                ctps_p_gina_com_as_informa_es_do_colaborador='["https://x/c.jpg"]')),
            ler_papel=_leitor_de_papel(**papel))


def test_a_divergencia_fica_guardada_para_alguem_decidir(mundo):
    """O relatório é de uma rodada: fechou a tela, perdeu-se. Com 575 pessoas,
    "anote num papel o que não bateu" não é plano."""
    _abrir_uma_divergencia(mundo, nome="Nome Do Documento")
    mundo["s"].flush()
    abertas = imp.perguntas_abertas(mundo["s"])
    assert len(abertas) == 1
    p = abertas[0]
    assert p["campo"] == "nome" and p["rotulo"] == "Nome"
    assert p["no_cadastro"] == "Pessoa Com Card"
    assert p["no_card"] == "Nome Do Card"
    assert p["no_documento"] == "Nome Do Documento"


def test_reimportar_NAO_duplica_a_mesma_pergunta(mundo):
    """Na carga inicial ele vai reimportar; sem esta trava, a lista do que
    falta decidir cresceria sem ninguém ter decidido nada."""
    _abrir_uma_divergencia(mundo, nome="Nome Do Documento")
    mundo["s"].flush()
    _abrir_uma_divergencia(mundo, nome="Nome Do Documento")
    mundo["s"].flush()
    assert len(imp.perguntas_abertas(mundo["s"])) == 1


def test_a_simulacao_mostra_mas_NAO_enfileira(mundo):
    r = imp.importar(mundo["s"], mundo["dp"], simular=True, cards=CARD,
                     ler_cards=_leitor(_card(nome_completo="Outro Nome")))
    assert r["divergencias"] == 1
    assert imp.perguntas_abertas(mundo["s"]) == []


def test_escolher_o_documento_grava_no_cadastro(mundo):
    _abrir_uma_divergencia(mundo, nome="Nome Do Documento")
    mundo["s"].flush()
    um = imp.perguntas_abertas(mundo["s"])[0]
    r = imp.decidir(mundo["s"], um["id"], "documento", mundo["dp"])
    mundo["s"].flush()
    assert r["situacao"] == "RESOLVIDA"
    mundo["s"].refresh(mundo["com_card"])
    assert mundo["com_card"].nome == "Nome Do Documento"
    assert imp.perguntas_abertas(mundo["s"]) == []


def test_manter_o_do_ERP_fecha_a_pergunta_sem_mexer_no_cadastro(mundo):
    _abrir_uma_divergencia(mundo, nome="Nome Do Documento")
    mundo["s"].flush()
    um = imp.perguntas_abertas(mundo["s"])[0]
    imp.decidir(mundo["s"], um["id"], "cadastro", mundo["dp"])
    mundo["s"].flush()
    mundo["s"].refresh(mundo["com_card"])
    assert mundo["com_card"].nome == "Pessoa Com Card"
    assert imp.perguntas_abertas(mundo["s"]) == []


def test_decidir_duas_vezes_e_recusado(mundo):
    """Dois cliques seguidos não gravam duas vezes."""
    _abrir_uma_divergencia(mundo, nome="Nome Do Documento")
    mundo["s"].flush()
    um = imp.perguntas_abertas(mundo["s"])[0]
    imp.decidir(mundo["s"], um["id"], "card", mundo["dp"])
    mundo["s"].flush()
    with pytest.raises(ErroValidacao):
        imp.decidir(mundo["s"], um["id"], "card", mundo["dp"])


def test_a_decisao_passa_pelas_conferencias_do_cadastro(mundo):
    """Importação não pode ser a porta dos fundos: PIS inválido é recusado
    aqui como seria na tela."""
    mundo["s"].execute(text("UPDATE colaboradores SET pis = NULL WHERE id = :i"),
                       {"i": mundo["com_card"].id})
    mundo["s"].execute(text("""
        INSERT INTO cadastro_divergencias
               (colaborador_id, campo, no_cadastro, no_card, mais_forte)
        VALUES (:i, 'pis', '', '11111111111', 'card')"""),
        {"i": mundo["com_card"].id})
    mundo["s"].flush()
    um = imp.perguntas_abertas(mundo["s"])[0]
    with pytest.raises(ErroValidacao):
        imp.decidir(mundo["s"], um["id"], "card", mundo["dp"])


def test_fonte_sem_valor_nao_grava_vazio(mundo):
    _abrir_uma_divergencia(mundo, nome="Nome Do Documento")
    mundo["s"].flush()
    um = imp.perguntas_abertas(mundo["s"])[0]
    mundo["s"].execute(
        text("UPDATE cadastro_divergencias SET no_card = '' WHERE id = :i"),
        {"i": um["id"]})
    mundo["s"].flush()
    with pytest.raises(ErroValidacao):
        imp.decidir(mundo["s"], um["id"], "card", mundo["dp"])


def test_a_importacao_em_massa_NAO_renomeia_ninguem(mundo, monkeypatch):
    """⚑ A outra metade da mesma regra, e ela é a que protege.

    Decidir uma divergência PODE mudar o nome — é para isso que serve. A
    importação em massa, NÃO: ela preenche buraco. Sem esta separação, uma
    leitura torta de documento renomearia gente em lote, calada."""
    monkeypatch.setattr(cliente, "baixar", lambda e, t: b"%PDF-1.4 fingindo")
    antes = mundo["com_card"].nome
    imp.importar(
        mundo["s"], mundo["dp"], simular=False, cards=CARD, regravar=True,
        ler_cards=_leitor(_card(nome_completo="Nome Que Nao Deve Entrar")),
        ler_papel=False)
    mundo["s"].flush()
    mundo["s"].refresh(mundo["com_card"])
    assert mundo["com_card"].nome == antes


def test_o_CPF_nunca_muda_por_decisao(mundo):
    """Trocar o CPF criaria um segundo cadastro em vez de corrigir o
    primeiro — e é por ele que o pagamento acha a pessoa."""
    assert "cpf" not in imp.A_CONFRONTAR
