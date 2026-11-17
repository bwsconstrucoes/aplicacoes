# -*- coding: utf-8 -*-
"""A FICHA DE REGISTRO DO FORTES, LIDA DIRETO DO PDF (16/11/2026).

Ele mandou a ficha e decidiu: *"como eu já tenho as fichas aqui, eu quero
trabalhar com as fichas mesmo."*

⚑ E NÃO PRECISOU DE IA. Eu havia recomendado exportar planilha do Fortes em
vez de ler 575 PDFs — leitura por IA custa dinheiro e erra justamente onde o
erro é caro (PIS e CTPS têm onze dígitos). Abrindo o arquivo, a recomendação
caiu: a ficha é PDF GERADO, com camada de texto. O número está escrito lá.

O QUE ESTES TESTES DEFENDEM, e cada um nasceu de um erro que eu cometi
escrevendo o leitor:

  · a série da CTPS vinha "00001 DV:", com o rótulo seguinte grudado;
  · a UF da CTPS vinha a do EMPREGADOR, porque "UF:" aparece seis vezes na
    ficha e eu pegava a primeira;
  · o salário vinha o último ESCRITO, e não o último por DATA — a ficha
    escreve quatro por linha, e a ordem do papel não é a do tempo.

O exemplo em `tests/dados/` é a ficha real com os documentos TROCADOS: teste
não guarda CPF, PIS nem RG de pessoa de verdade.
"""
from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from app.apps.erp.core.importadores import fichas_fortes as ff

EXEMPLO = Path("tests/dados/ficha_fortes_exemplo.txt").read_text(encoding="utf-8")


@pytest.fixture
def lida():
    return ff.ler_ficha(EXEMPLO, 1)


# ---------------------------------------------------------------------------
# O que a ficha MANDA
# ---------------------------------------------------------------------------
def test_le_os_campos_que_o_fortes_manda(lida):
    """PIS, CTPS, admissão, salário e demissão — decisão dele: *"concordo em
    relação ao Fortes mandar em PIS, CTPS, admissão, salário, demissão."*"""
    assert lida["legivel"] is True
    assert lida["pis"] == "12000000004"
    assert lida["ctps_numero"] == "06491760"
    assert lida["admissao"] == "2014-03-12"
    assert lida["demissao"] is None          # esta pessoa está na ativa
    assert lida["id_fortes"] == "000104"


def test_a_serie_da_ctps_nao_vem_com_o_rotulo_seguinte_grudado(lida):
    """⚑ ERRO MEU, pego na primeira leitura da ficha real: a ficha escreve
    "Série: 00001 DV:" com UM espaço, e a série entrava como "00001 DV:".

    O cadastro recusaria — ou pior, aceitaria, e a série da carteira de alguém
    ficaria com um rótulo dentro."""
    assert lida["ctps_serie"] == "00001"


def test_a_uf_da_ctps_e_a_da_pessoa_nao_a_do_empregador(lida):
    """⚑ ERRO MEU: "UF:" aparece SEIS vezes na ficha — empregador,
    naturalidade, endereço, CTPS, RG e órgão de classe. Eu pegava a primeira,
    que é a da BWS. Todo mundo entraria com a UF da empresa, e ninguém notaria
    enquanto as duas fossem CE."""
    assert lida["ctps_uf"] == "CE"
    # A prova de que é da linha certa: o rótulo âncora é o da CTPS.
    assert ff._na_linha_de(EXEMPLO.split("\n"), "CTPS Número:", "UF:") == "CE"


def test_o_salario_e_o_ultimo_POR_DATA_e_nao_o_ultimo_escrito():
    """⚑ ERRO MEU, e o mais caro dos três: a ficha escreve o histórico em
    quatro colunas por linha, e a ordem no papel é a de leitura, não a
    cronológica. Pegar "o que está escrito por último" daria 2.230,00 (06/2025)
    numa ficha cuja última mudança foi 06/2026 — a pessoa entraria no ERP
    ganhando menos do que ganha."""
    assert ff._ultimo_salario(EXEMPLO) == Decimal("2382.00")
    # E o último ESCRITO no papel é outro número, o que prova que a trava vale.
    assert "2.382,00" in EXEMPLO and "2.230,00" in EXEMPLO


def test_a_obra_sai_do_setor_pelo_numero_da_contabilidade(lida):
    """*"Em relação a setor (…) tem a coluna Centro de Custo da
    Contabilidade. E é exatamente esse setor aí."*"""
    assert lida["centro_custo"] == "164.01"
    assert "ESCVARZEA" in lida["setor_nome"]


def test_os_nomes_sao_lidos_para_CONFERIR(lida):
    """Ele: *"o ideal é que quem mande seja nós (…) mas eu gostaria que fosse
    feita a leitura também, para saber, porque às vezes o pessoal pode
    errar."*"""
    assert lida["nome"] == "FULANO DE TAL DA SILVA"
    assert lida["nome_pai"] == "PAI DE TAL DA SILVA"
    assert lida["nome_mae"] == "MAE DE TAL DOS SANTOS"
    assert lida["data_nascimento"] == "1983-10-06"


# ---------------------------------------------------------------------------
# O que a ficha NÃO traz, de propósito
# ---------------------------------------------------------------------------
def test_telefone_e_cargo_nao_entram():
    """Decisões dele, 16/11/2026:

    *"O telefone não é o da contabilidade, eu não sabia nem que eles tinham
    aí, então ignora isso aí."* — e tinha razão de sobra: na ficha o DDD está
    num campo e o número noutro, e o celular tem OITO dígitos (falta o 9 de
    2016). Importar isso encheria de telefone que não completa o campo que
    acabou de virar obrigatório.

    *"O cargo a gente vai utilizar a nossa nomenclatura. A dele está feia."*
    """
    lida = ff.ler_ficha(EXEMPLO, 1)
    for campo in ("telefone", "celular", "cargo", "cbo", "funcao"):
        assert campo not in lida, f"{campo} voltou a ser lido da ficha"
    assert "telefone" not in ff.DA_FICHA and "cargo" not in ff.DA_FICHA


def test_o_que_o_ERP_nao_guarda_fica_na_ficha():
    """*"O que a ficha tem a mais que no ERP, deixa na ficha. A gente não
    precisa. Só precisa dos dados que a gente já vem trabalhando."*"""
    lida = ff.ler_ficha(EXEMPLO, 1)
    for campo in ("matricula_esocial", "etnia", "raca", "titulo_eleitor",
                  "cipa", "exame_medico", "jornada", "contribuicao_sindical"):
        assert campo not in lida, f"{campo} entrou, e ele disse para deixar na ficha"


# ---------------------------------------------------------------------------
# O que não dá para ler é DITO, não adivinhado
# ---------------------------------------------------------------------------
def test_pagina_que_nao_e_ficha_e_recusada_com_o_motivo():
    r = ff.ler_ficha("ISTO AQUI É OUTRA COISA QUALQUER", 7)
    assert r["legivel"] is False and r["pagina"] == 7
    assert "ficha de registro" in r["motivo"]


def test_ficha_sem_CPF_e_recusada():
    """O CPF é a identidade: sem ele não dá para saber de quem é a ficha, e
    adivinhar pelo nome é o caminho para mexer no cadastro de outra pessoa."""
    sem_cpf = EXEMPLO.replace("CPF: 714.287.938-60", "CPF:")
    r = ff.ler_ficha(sem_cpf, 3)
    assert r["legivel"] is False and "CPF" in r["motivo"]


# ---------------------------------------------------------------------------
# A comparação de nome não briga por acento nem por caixa
# ---------------------------------------------------------------------------
def test_nome_sem_acento_e_em_maiusculas_NAO_e_divergencia():
    """⚑ SEM ISTO A FILA NASCERIA COM 575 PERGUNTAS FALSAS. O Fortes escreve
    tudo em maiúsculas e sem acento ("JOSE"); o ERP guarda como a pessoa
    escreveu ("José"). São a mesma pessoa — e lista de pendência que nasce
    cheia de pergunta boba é lista que alguém fecha sem ler."""
    assert ff._mesma_coisa("nome", "José Charles", "JOSE CHARLES") is True
    assert ff._mesma_coisa("nome", "José  Charles", "JOSE CHARLES") is True
    # Mas nome de verdade diferente CONTINUA sendo pergunta.
    assert ff._mesma_coisa("nome", "José Carlos", "JOSE CHARLES") is False


def test_data_compara_so_o_dia():
    assert ff._mesma_coisa("data_nascimento", "1983-10-06", "1983-10-06") is True
    assert ff._mesma_coisa("data_nascimento", "1983-10-07", "1983-10-06") is False


# ---------------------------------------------------------------------------
# A IMPORTAÇÃO, com Postgres de verdade
# ---------------------------------------------------------------------------
@pytest.fixture
def mundo(sessao_real):
    from sqlalchemy import text
    from app.apps.erp.core.auth.service import gerar_hash
    from app.apps.erp.core.cadastros import pessoas as svc
    from app.apps.erp.db.models.cadastros import Obra, PerfilUsuario as P, Usuario

    dp = Usuario(nome="dp-fichas", email="dp@fichas.teste", ativo=True,
                 senha_hash=gerar_hash("senha-de-teste-123"),
                 perfil=P.DEPARTAMENTO_PESSOAL, telefone="5585900000000")
    sessao_real.add(dp)
    # A obra com o número da contabilidade — é por ele que a ficha a acha.
    obra = Obra(codigo="FIC-01", nome="Obra da ficha", status="ATIVA",
                centro_custo_contabil="164.01")
    outra = Obra(codigo="FIC-02", nome="Outra obra", status="ATIVA",
                 centro_custo_contabil="999.99")
    sessao_real.add_all([obra, outra])
    sessao_real.flush()
    pessoa = svc.salvar(sessao_real, {
        "cpf": "71428793860", "nome": "Fulano de Tal da Silva",
        "obra_id": obra.id, "vinculo": "CTPS", "admissao": "2014-03-12"}, dp)
    sessao_real.flush()
    return {"s": sessao_real, "dp": dp, "obra": obra, "outra": outra,
            "pessoa": pessoa}


def _pdf_do_exemplo() -> bytes:
    """A ficha de teste virada PDF com camada de texto — que é o que a
    importação recebe de verdade."""
    import fitz
    doc = fitz.open()
    pagina = doc.new_page()
    pagina.insert_text((20, 20), EXEMPLO, fontsize=5, fontname="cour")
    dados = doc.tobytes()
    doc.close()
    return dados


@pytest.mark.banco
def test_a_ficha_atualiza_o_que_o_fortes_manda_e_so_isso(mundo):
    """De ponta a ponta: o PDF entra, o PIS e a CTPS da pessoa passam a ser os
    da ficha, e o nome continua o do ERP."""
    from app.apps.erp.core.importadores import fichas_fortes as svc_ff
    r = svc_ff.importar(mundo["s"], _pdf_do_exemplo(), gravar=True,
                        usuario=mundo["dp"])
    mundo["s"].flush()
    assert r["fichas_lidas"] == 1 and not r["ilegiveis"]

    mundo["s"].refresh(mundo["pessoa"])
    assert mundo["pessoa"].pis == "12000000004"
    assert mundo["pessoa"].ctps_numero == "06491760"
    assert str(mundo["pessoa"].salario) == "2382.00"
    assert mundo["pessoa"].id_fortes == "000104"
    # O nome NÃO foi mexido: o ERP manda nele.
    assert mundo["pessoa"].nome == "Fulano de Tal da Silva"


@pytest.mark.banco
def test_simular_nao_toca_no_banco(mundo):
    """Importação de cadastro roda uma vez e é vista por todo mundo depois —
    simular antes não é zelo, é o jeito de não refazer."""
    from app.apps.erp.core.importadores import fichas_fortes as svc_ff
    antes = mundo["pessoa"].pis
    r = svc_ff.importar(mundo["s"], _pdf_do_exemplo(), gravar=False,
                        usuario=mundo["dp"])
    mundo["s"].flush()
    mundo["s"].refresh(mundo["pessoa"])
    assert mundo["pessoa"].pis == antes
    # Mas o relatório já diz o que MUDARIA.
    assert r["atualizadas"] and any(
        m["campo"] == "pis" for m in r["atualizadas"][0]["mudancas"])
    assert r["gravou"] is False


@pytest.mark.banco
def test_nome_diferente_vira_PERGUNTA_e_nao_mudanca(mundo):
    """Ele: *"eu gostaria que fosse feita a leitura também, para saber, porque
    às vezes o pessoal pode errar."* A diferença vira pergunta na fila que já
    existe — ninguém troca o nome de alguém na folha por causa de uma leitura.
    """
    from sqlalchemy import text
    from app.apps.erp.core.importadores import fichas_fortes as svc_ff
    mundo["pessoa"].nome = "Fulano de Tal da Costa"        # o ERP diz outro
    mundo["s"].flush()

    r = svc_ff.importar(mundo["s"], _pdf_do_exemplo(), gravar=True,
                        usuario=mundo["dp"])
    mundo["s"].flush()
    assert any(d["campo"] == "nome" for d in r["divergencias"])
    # O nome continua o do ERP…
    mundo["s"].refresh(mundo["pessoa"])
    assert mundo["pessoa"].nome == "Fulano de Tal da Costa"
    # …e a pergunta está na fila, com a origem dizendo de onde veio.
    linha = mundo["s"].execute(text("""
        SELECT no_cadastro, no_documento, origem, mais_forte
          FROM cadastro_divergencias
         WHERE colaborador_id = :i AND campo = 'nome'"""),
        {"i": mundo["pessoa"].id}).mappings().first()
    assert linha is not None
    assert linha["origem"] == "FORTES"
    assert linha["no_documento"] == "FULANO DE TAL DA SILVA"
    assert linha["mais_forte"] == "cadastro"


@pytest.mark.banco
def test_centro_de_custo_sem_obra_nao_derruba_a_importacao(mundo):
    """Recusar a ficha inteira porque falta uma linha de planilha seria trocar
    um problema de cadastro por um de importação."""
    from app.apps.erp.core.importadores import fichas_fortes as svc_ff
    mundo["obra"].centro_custo_contabil = None            # ninguém reconhece 164.01
    mundo["s"].flush()
    r = svc_ff.importar(mundo["s"], _pdf_do_exemplo(), gravar=True,
                        usuario=mundo["dp"])
    assert r["fichas_lidas"] == 1
    assert r["sem_obra"] and r["sem_obra"][0]["centro_custo"] == "164.01"


@pytest.mark.banco
def test_obra_diferente_e_AVISADA_e_nao_trocada(mundo):
    """A obra do ERP é trabalho do DP. Quando a contabilidade discorda, o
    sistema diz — e não decide."""
    from app.apps.erp.core.importadores import fichas_fortes as svc_ff
    mundo["pessoa"].obra_id = mundo["outra"].id           # o ERP diz FIC-02
    mundo["s"].flush()
    r = svc_ff.importar(mundo["s"], _pdf_do_exemplo(), gravar=True,
                        usuario=mundo["dp"])
    mundo["s"].flush()
    assert r["mudou_de_obra"] and r["mudou_de_obra"][0]["na_contabilidade"] == "FIC-01"
    mundo["s"].refresh(mundo["pessoa"])
    assert mundo["pessoa"].obra_id == mundo["outra"].id   # NÃO mudou


@pytest.mark.banco
def test_quem_nao_esta_no_ERP_e_CONTADO_e_nao_criado_sozinho(mundo):
    """⚑ `criar_novos` NASCE DESLIGADO. A ficha não traz cargo (decisão dele:
    a nomenclatura é nossa), nem telefone, nem chave Pix — obrigatórios no
    cadastro de hoje. Quem nascesse por aí ficaria incompleto, e cadastro pela
    metade é dívida que ninguém paga."""
    from sqlalchemy import select
    from app.apps.erp.core.importadores import fichas_fortes as svc_ff
    from app.apps.erp.db.models.cadastros import Colaborador
    mundo["s"].delete(mundo["pessoa"])
    mundo["s"].flush()

    r = svc_ff.importar(mundo["s"], _pdf_do_exemplo(), gravar=True,
                        usuario=mundo["dp"])
    mundo["s"].flush()
    assert len(r["sem_cadastro"]) == 1 and not r["criadas"]
    assert mundo["s"].scalars(select(Colaborador).where(
        Colaborador.cpf == "71428793860")).first() is None


@pytest.mark.banco
def test_uma_ficha_ruim_nao_derruba_as_outras(mundo):
    """⚑ DEFEITO ACHADO PELO PRÓPRIO TESTE, em 16/11/2026: o cadastro CONFERE
    o que entra (PIS com dígito errado é recusado — e ainda bem, é o campo que
    o INSS cobra). Sem o `try` por ficha, a primeira com número torto abortava
    a importação INTEIRA, e quem rodasse não saberia nem qual era.

    Agora a ficha ruim é recusada com a página e o motivo, e as outras entram.
    """
    from app.apps.erp.core.importadores import fichas_fortes as svc_ff
    import fitz
    ruim = EXEMPLO.replace("120.00000.00-4", "111.11111.11-1")   # PIS inválido
    doc = fitz.open()
    doc.new_page().insert_text((20, 20), ruim, fontsize=5, fontname="cour")
    dados = doc.tobytes()
    doc.close()

    r = svc_ff.importar(mundo["s"], dados, gravar=True, usuario=mundo["dp"])
    mundo["s"].flush()
    assert r["fichas_lidas"] == 1
    assert r["recusadas"] and "PIS" in r["recusadas"][0]["motivo"]
    assert r["recusadas"][0]["pagina"] == 1
    # E o cadastro ficou como estava — nada entrou pela metade.
    mundo["s"].refresh(mundo["pessoa"])
    assert mundo["pessoa"].pis in (None, "")


@pytest.mark.banco
def test_campo_VAZIO_no_ERP_e_preenchido_pela_ficha_sem_virar_pergunta(mundo):
    """Campo vazio não é discordância, é buraco — e buraco se tapa.

    Pego exercitando a tela em 16/11/2026: nome do pai, nome da mãe e
    nascimento quase nunca estão no cadastro, e tratá-los como "o ERP manda"
    abria uma pergunta para cada um, em cada ficha — mais de mil perguntas
    sobre dados que ninguém tinha. A regra do dono ("quem manda somos nós")
    vale onde existem DUAS respostas diferentes.
    """
    from app.apps.erp.core.importadores import fichas_fortes as svc_ff
    assert not mundo["pessoa"].nome_pai and not mundo["pessoa"].nome_mae

    r = svc_ff.importar(mundo["s"], _pdf_do_exemplo(), gravar=True,
                        usuario=mundo["dp"])
    mundo["s"].flush()
    mundo["s"].refresh(mundo["pessoa"])

    assert mundo["pessoa"].nome_pai == "PAI DE TAL DA SILVA"
    assert mundo["pessoa"].nome_mae == "MAE DE TAL DOS SANTOS"
    assert str(mundo["pessoa"].data_nascimento) == "1983-10-06"
    # E nada disso virou pergunta.
    assert not [d for d in r["divergencias"]
                if d["campo"] in ("nome_pai", "nome_mae", "data_nascimento")]


@pytest.mark.banco
def test_campo_PREENCHIDO_e_diferente_continua_virando_pergunta(mundo):
    """O outro lado da moeda: havendo valor no ERP, a ficha não o troca."""
    from app.apps.erp.core.importadores import fichas_fortes as svc_ff
    mundo["pessoa"].nome_mae = "MARIA DE OUTRO NOME"
    mundo["s"].flush()

    r = svc_ff.importar(mundo["s"], _pdf_do_exemplo(), gravar=True,
                        usuario=mundo["dp"])
    mundo["s"].flush()
    mundo["s"].refresh(mundo["pessoa"])

    assert mundo["pessoa"].nome_mae == "MARIA DE OUTRO NOME"
    assert any(d["campo"] == "nome_mae" for d in r["divergencias"])


def test_data_e_dinheiro_saem_no_formato_de_quem_le():
    """O relatório é conferido CONTRA O PAPEL, e o papel diz 12/03/2014."""
    import datetime as _dt
    from decimal import Decimal as _D
    from app.apps.erp.core.importadores.fichas_fortes import _texto_do_campo
    assert _texto_do_campo(_dt.date(2014, 3, 12)) == "12/03/2014"
    assert _texto_do_campo(_D("2382.00")) == "2.382,00"
    assert _texto_do_campo(_D("1234567.5")) == "1.234.567,50"
    assert _texto_do_campo(None) == ""


@pytest.mark.banco
def test_admissao_IGUAL_nao_e_contada_como_mudanca(mundo):
    """A ficha entrega a data como texto e o cadastro guarda como DATA.

    Comparados crus, nunca batiam: a admissão de TODO MUNDO entrava como
    mudança em toda importação, e o relatório prometia 575 mudanças quando
    não havia nenhuma. Quem conferisse perderia a confiança no relatório —
    que é a única coisa que protege a gravação.
    """
    from app.apps.erp.core.importadores import fichas_fortes as svc_ff
    assert str(mundo["pessoa"].admissao) == "2014-03-12"   # a mesma da ficha

    r = svc_ff.importar(mundo["s"], _pdf_do_exemplo(), gravar=False,
                        usuario=mundo["dp"])
    mudancas = r["atualizadas"][0]["mudancas"] if r["atualizadas"] else []
    assert not [m for m in mudancas if m["campo"] == "admissao"]


@pytest.mark.banco
def test_a_importacao_NAO_escreve_no_historico_de_alteracoes_do_ERP(mundo):
    """A tabela de alterações é o que o ERP DECIDIU, não o que o Fortes sabe.

    Decisão do dono em 16/11/2026: *"o sistema que vai armazenar esses
    históricos de salário, de admissões, etc. e tal, é lá. Então, é
    desnecessário."* O ERP guarda a parte cadastral e o que é preciso para
    encaminhar à contabilidade — a história fica no Fortes.

    Sem esta trava, uma importação de 575 fichas escreveria 575 "alterações
    de salário" que ninguém decidiu, e a lista de promoções e transferências
    da BWS — que tem motivo e justificativa, e é consultada — viraria lixo.
    """
    from sqlalchemy import text
    from app.apps.erp.core.importadores import fichas_fortes as svc_ff

    antes = mundo["s"].execute(
        text("SELECT count(*) FROM colaborador_alteracoes")).scalar()
    r = svc_ff.importar(mundo["s"], _pdf_do_exemplo(), gravar=True,
                        usuario=mundo["dp"])
    mundo["s"].flush()
    # O salário entrou mesmo — senão o teste passaria por não ter feito nada.
    mundo["s"].refresh(mundo["pessoa"])
    assert str(mundo["pessoa"].salario) == "2382.00"
    assert r["atualizadas"]
    depois = mundo["s"].execute(
        text("SELECT count(*) FROM colaborador_alteracoes")).scalar()
    assert depois == antes
