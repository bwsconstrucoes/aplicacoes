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
        text("UPDATE colaboradores SET ref_pipefy = '111' WHERE id = :i"),
        {"i": com_card.id})

    # Outra sem card nenhum: não pode entrar na fila.
    sem_card = svc_pessoas.salvar(sessao_real, {
        "cpf": CPF_B, "nome": "Pessoa Sem Card", "obra_id": obra.id,
        "vinculo": "CTPS", "admissao": "2024-02-05"}, dp)
    sessao_real.flush()
    return {"s": sessao_real, "dp": dp, "obra": obra,
            "com_card": com_card, "sem_card": sem_card}


def _card(**campos):
    """Um card de mentira, no formato que `ler_cards` devolve."""
    return {"111": {"id": "111", "titulo": "Pessoa Com Card",
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
        ler_cards=_leitor(_card(pis="99999999999", nome_da_m_e="Maria Mãe")))
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
        ler_cards=_leitor(_card(carteira_de_trabalho='["https://x/c.jpg"]',
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
                 ler_cards=_leitor(_card(ctps_1='["https://x/conta.pdf"]')))
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
                 ler_cards=_leitor(cards))
    mundo["s"].flush()

    def nao_pode_baixar(*a, **k):                # pragma: no cover
        raise AssertionError("a segunda passada não pode baixar de novo")
    monkeypatch.setattr(cliente, "baixar", nao_pode_baixar)
    de_novo = imp.importar(mundo["s"], mundo["dp"], simular=False,
                           ler_cards=_leitor(cards))
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
        ler_cards=_leitor(_card(ctps_1='["https://x/quebrado.pdf"]',
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
                 ler_cards=_leitor(_card(foto='["https://x/f.jpg"]')))
    mundo["s"].flush()
    linha = mundo["s"].execute(text("""
        SELECT resultado, detalhe FROM pipefy_importado
         WHERE card = '111' AND campo = 'foto'""")).first()
    assert linha is not None and linha[0] == "FALHOU"
    assert "25 MB" in linha[1]


def test_card_que_o_pipefy_nao_devolve_vira_linha_do_relatorio(mundo):
    relatorio = imp.importar(
        mundo["s"], mundo["dp"], simular=True,
        ler_cards=_leitor({}, {"111": "card apagado"}))
    assert relatorio["pessoas_lidas"] == 0
    assert relatorio["cards_sem_resposta"][0]["card"] == "111"


def test_o_relatorio_diz_o_que_ficou_de_fora(mundo):
    """O ASO não entra porque vence e o card não diz até quando. Omitir isso
    faria o DP achar que o atestado foi importado."""
    relatorio = imp.importar(mundo["s"], mundo["dp"], simular=True,
                             ler_cards=_leitor(_card()))
    assert "aso" in relatorio["de_fora"]


def test_anexo_sem_endereco_completo_NAO_vira_endereco_chutado(mundo):
    """⚑ Eu ia montar a raiz do armazenamento do Pipefy a partir de um caminho
    solto. O importador que este monorepo já roda em produção mostra que, nesta
    conta, o anexo vem com o endereço completo — e chutar a raiz daria um
    endereço que ou não responde, ou responde OUTRA COISA, e aí o arquivo de
    alguém entraria no cadastro de outra pessoa."""
    assert cliente.endereco_do_anexo("orgs/1/uploads/2/b.pdf") == ""
    assert cliente.endereco_do_anexo("https://x/a.pdf") == "https://x/a.pdf"
    assert cliente.endereco_do_anexo({"url": "https://x/d.pdf"}) == "https://x/d.pdf"

    relatorio = imp.importar(
        mundo["s"], mundo["dp"], simular=True,
        ler_cards=_leitor(_card(foto='["orgs/1/uploads/2/f.jpg"]')))
    assert relatorio["documentos_que_falharam"] == 1
    assert relatorio["documentos_guardados"] == 0
    assert "endereço" in relatorio["falhas"][0]["motivo"]


def test_numero_de_card_que_nao_e_numero_nao_vira_consulta(mundo):
    """Os ids entram na consulta SEM aspas — texto solto ali seria injeção."""
    assert cliente.card_valido("853990032") is True
    assert cliente.card_valido("111) { pipes { id } } #") is False
    with pytest.raises(cliente.ErroDoPipefy):
        cliente._numero_do_card("não é número")
