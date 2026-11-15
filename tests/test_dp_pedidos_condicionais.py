# -*- coding: utf-8 -*-
"""O FORMULÁRIO DO DP QUE SE AJUSTA AO CASO (15/11/2026).

Quatro decisões dele, no mesmo pedido longo, e todas tratam do mesmo defeito:
**o formulário perguntava coisa que não se aplicava, ou obrigava a escolher o
tipo certo antes de saber a diferença entre eles.**

  1. *"Vamos ter um tipo só de desligamento (…) forma de desligamento só para
     CTPS, e para prestador só a data de saída."* — eram DOIS tipos na
     caixinha, e quem abria tinha de saber de antemão o vínculo da pessoa.
  2. *"Na admissão, quando digitar o CPF, o sistema tem que detectar que a
     pessoa já tem cadastro e oferecer a readmissão. Aí readmissão deixa de ser
     um tipo."* — antes era recusa seca, e quem não soubesse que existe um tipo
     chamado Readmissão criava a segunda ficha da mesma pessoa.
  3. *"Vale transporte não precisa ser um tipo à parte, ele é uma opção dentro
     de ajuda de custo."* — o mesmo pedido cabia em dois lugares.
  4. *"Quando é CTPS, o valor é o da convenção, então valor do mês e pagamento
     diarista ou mensalista não se aplicam."* — campo que não se aplica e fica
     na tela não é inofensivo: alguém preenche, e aí há duas respostas para
     "quanto esta pessoa ganha".

⚑ A TRAVA QUE MAIS IMPORTA AQUI é a do último teste do arquivo: as condições
são escritas com o CÓDIGO que o banco guarda ("CTPS", "DIARISTA") e a caixinha
mostra o RÓTULO do Pipe ("CTPS", "Diarista"). Num caso são iguais e no outro
não — que é justamente o jeito de o defeito passar na conferência e aparecer
só na tela.
"""
from __future__ import annotations

import pytest

from app.apps.erp.core import pessoal_solicitacoes as svc
from app.apps.erp.core.cadastros import pessoas as svc_pessoas
from app.apps.erp.core.comum.auditoria import ErroValidacao

CPF_A = "52998224725"
CPF_B = "11144477735"
CPF_C = "71428793860"


# ---------------------------------------------------------------------------
# SEM BANCO: as listas e as condições
# ---------------------------------------------------------------------------
def test_os_tres_tipos_saem_da_caixinha_mas_ficam_no_dicionario():
    """Pedido já gravado continua legível; quem abre não vê mais o tipo.

    Apagar a linha do dicionário deixaria o histórico de quem pediu um
    "Desligamento de Prestador de Serviço" sem rótulo, e o atendimento dele
    sem checklist.
    """
    na_tela = {t["valor"] for t in svc.tipos_para_tela()}
    for codigo in ("DESLIGAMENTO_PRESTADOR", "READMISSAO", "VALE_TRANSPORTE"):
        assert codigo not in na_tela, f"{codigo} voltou para a caixinha"
        assert codigo in svc.TIPOS, f"{codigo} saiu do dicionário"
        assert svc.TIPOS[codigo].get("oculto") is True
        # E continua tendo nome e checklist, que é para o que ele ficou.
        assert svc.TIPOS[codigo]["nome"]
        assert svc.TIPOS[codigo]["checklist"]


def test_desligamento_de_carteira_pede_forma_e_aviso():
    assert svc.obrigatorios_do_tipo("DESLIGAMENTO", "CTPS") == (
        "forma_desligamento", "aviso_previo", "continua_prestador")
    assert "data_saida" not in svc.campos_do_tipo("DESLIGAMENTO", "CTPS")


def test_o_desligamento_de_carteira_PERGUNTA_se_a_pessoa_continua():
    """⚑ A TRAVA DO BUG DO PONTO (15/11/2026).

    Ele: *"como é que a gente faz com o colaborador que é desligado da carteira
    e continua como prestador? (…) isso aí provoca de vez em quando um bug na
    parte do ponto."* Decisão dele: continua demitindo e readmitindo — a trava
    é esta pergunta.

    Ela é OBRIGATÓRIA de propósito. Pergunta opcional sobre algo que, se
    esquecido, deixa alguém sem bater o ponto, é pergunta que não foi feita.
    Custa um clique no desligamento comum; o esquecimento custa uma obra
    parada às sete da manhã.
    """
    assert "continua_prestador" in svc.obrigatorios_do_tipo(
        "DESLIGAMENTO", "CTPS")
    # E NÃO se pergunta a quem já é prestador: não há carteira de onde sair.
    assert "continua_prestador" not in svc.campos_do_tipo(
        "DESLIGAMENTO", "PRESTADOR")


def test_dizer_que_continua_acrescenta_os_passos_no_checklist():
    """O DP não pode fechar o pedido sem ter aberto a readmissão."""
    sem = svc.checklist_do_tipo("DESLIGAMENTO", "CTPS",
                                pedido={"continua_prestador": "0"})
    com = svc.checklist_do_tipo("DESLIGAMENTO", "CTPS",
                                pedido={"continua_prestador": "1"})
    assert len(com) == len(sem) + 2
    assert any("readmissão" in x for x in com)
    assert any("ponto" in x for x in com[-2:])


def test_desligamento_de_prestador_pede_so_a_data_de_saida():
    """Prestador não tem aviso prévio — não há o que avisar."""
    assert svc.obrigatorios_do_tipo("DESLIGAMENTO", "PRESTADOR") == ("data_saida",)
    campos = svc.campos_do_tipo("DESLIGAMENTO", "PRESTADOR")
    assert "aviso_previo" not in campos
    assert "forma_desligamento" not in campos
    assert "data_saida" in campos


def test_sem_saber_o_vinculo_vale_o_mais_exigente():
    """Padrão frouxo seria pior: um pedido sem pessoa ligada passaria sem forma
    de desligamento nenhuma, e o DP receberia "desligue o fulano" e nada mais."""
    assert svc.obrigatorios_do_tipo("DESLIGAMENTO", None) == (
        "forma_desligamento", "aviso_previo")


def test_a_data_de_saida_e_do_DP_na_carteira_e_da_obra_no_prestador():
    """O MESMO CAMPO, DONOS DIFERENTES — e é por isso que a lista é por vínculo.

    Na carteira a data de saída é CONTA do DP (último dia + aviso prévio). No
    prestador é o fim combinado da prestação, que só a obra sabe. Tê-la nos
    dois lados ao mesmo tempo abriria a porta para duas datas diferentes no
    mesmo pedido.
    """
    assert "data_saida" in svc.campos_do_dp_do_tipo("DESLIGAMENTO", "CTPS")
    assert "data_saida" not in svc.campos_do_tipo("DESLIGAMENTO", "CTPS")
    assert "data_saida" in svc.campos_do_tipo("DESLIGAMENTO", "PRESTADOR")
    assert "data_saida" not in svc.campos_do_dp_do_tipo("DESLIGAMENTO", "PRESTADOR")


def test_o_checklist_do_prestador_nao_tem_passo_de_rescisao():
    """Onze passos de rescisão (TRCT, GRRF, folha de ponto) para quem não tem
    carteira seria dar ao DP uma lista que ele nunca vai marcar — e lista que
    não se marca é lista que ninguém olha."""
    ctps = svc.checklist_do_tipo("DESLIGAMENTO", "CTPS")
    prest = svc.checklist_do_tipo("DESLIGAMENTO", "PRESTADOR")
    assert len(ctps) == 11 and any("TRCT" in x for x in ctps)
    assert len(prest) == 3 and not any("TRCT" in x for x in prest)


def test_o_vale_transporte_continua_alcancavel_pela_ajuda_de_custo():
    """O tipo saiu, a função não: ele já era uma das opções de categoria."""
    ajuda = next(t for t in svc.tipos_para_tela() if t["valor"] == "AJUDA_DE_CUSTO")
    opcoes = ajuda["opcoes"]["categoria_transporte"]
    assert "VALE_TRANSPORTE" in opcoes
    assert "DIARIO_E_VT" in opcoes
    # E a pergunta que faltava: descontar em folha ou a empresa absorve.
    assert "descontar_vt" in ajuda["campos"]


def test_o_desconto_do_vt_so_e_exigido_quando_ha_vale_transporte():
    """`visiveis_se` ao contrário de `ocultos_se`: o campo não nasce até a
    escolha que o justifica. Perguntar antes é perguntar algo que ainda não
    existe."""
    assert svc._nao_cabe_agora(
        svc.TIPOS["AJUDA_DE_CUSTO"], "descontar_vt", {}) is True
    assert svc._nao_cabe_agora(
        svc.TIPOS["AJUDA_DE_CUSTO"], "descontar_vt",
        {"categoria_transporte": "VEICULO"}) is True
    assert svc._nao_cabe_agora(
        svc.TIPOS["AJUDA_DE_CUSTO"], "descontar_vt",
        {"categoria_transporte": "VALE_TRANSPORTE"}) is False


def test_cadastro_de_carteira_nao_pergunta_pagamento_nem_valor_do_mes():
    """*"Quando é CTPS, o valor é o da convenção."* Dois valores para o mesmo
    salário é o que esta regra evita."""
    exige = svc.exigencias("CADASTRO", {"tipo_documentacao": "CTPS"})["campos"]
    for proibido in ("pagamento", "valor_mes", "tipo_contrato"):
        assert proibido not in exige, f"{proibido} continua exigido em CTPS"
    # Mas a carteira e o PIS passam a ser exigidos.
    assert "num_ctps" in exige and "num_pis" in exige


def test_cadastro_de_prestador_pergunta_o_tipo_de_contrato():
    exige = svc.exigencias("CADASTRO", {"tipo_documentacao": "PRESTADOR"})["campos"]
    assert "tipo_contrato" in exige and "pagamento" in exige
    assert "num_ctps" not in exige


def test_o_tipo_de_contrato_do_prestador_tem_as_seis_opcoes_dele():
    """*"PJ, autônomo RPA, autônomo mensalista, jovem aprendiz, estagiário,
    pró-labore."* As duas de CLT existem no cadastro e NÃO entram aqui: em
    carteira o tipo não se escolhe."""
    from app.apps.erp.core.cadastros.pessoas import TIPOS_CONTRATO
    codigos = [svc.DO_PIPE_PARA_O_CODIGO["tipo_contrato"][o]
               for o in svc.CATEGORIAS["tipo_contrato"]]
    assert codigos == ["PJ", "AUTONOMO_RPA", "AUTONOMO_MENSALISTA",
                       "JOVEM_APRENDIZ", "ESTAGIO", "PRO_LABORE"]
    # Todo código tem de existir no cadastro, senão gravar quebra na hora.
    for c in codigos:
        assert c in TIPOS_CONTRATO, f"{c} não existe no cadastro da pessoa"


def test_o_valor_do_mes_so_e_exigido_de_mensalista():
    """Diarista não tem valor de mês — tem diária."""
    mens = svc.exigencias("CADASTRO", {"tipo_documentacao": "PRESTADOR",
                                       "pagamento": "MENSALISTA"})["campos"]
    diar = svc.exigencias("CADASTRO", {"tipo_documentacao": "PRESTADOR",
                                       "pagamento": "DIARISTA"})["campos"]
    assert "valor_mes" in mens
    assert "valor_mes" not in diar


def test_telefone_pix_e_diaria_sao_obrigatorios_em_todo_cadastro():
    """*"Telefone é obrigatoriíssimo (…) é vital"*, *"chave Pix obrigatória"*,
    valor da diária *"vamos colocar como obrigatório"*.

    O motivo é o mesmo dos documentos: sem telefone ninguém avisa a pessoa de
    nada, e sem chave Pix ela não recebe. Campo que o pagamento exige não é
    campo opcional — é pendência que alguém vai cobrar por WhatsApp depois.
    """
    for forma in ("CTPS", "PRESTADOR"):
        exige = svc.exigencias("CADASTRO", {"tipo_documentacao": forma})["campos"]
        for campo in ("telefone", "pix_tipo", "pix_chave", "valor_diaria",
                      "cnpj_admissao"):
            assert campo in exige, f"{campo} não é obrigatório em {forma}"


def test_toda_opcao_de_categoria_tem_a_conta_escrita():
    """*"Explica cada modo de cálculo."* "Mensal" e "Mês" ficam lado a lado na
    mesma caixinha e dão contas DIFERENTES — uma multiplica pelos dias do mês,
    a outra é o valor cheio. Duas palavras parecidas, duas contas, nada
    escrito: o erro só aparece na folha."""
    for campo in ("categoria_alimentacao", "categoria_transporte",
                  "descontar_vt"):
        mapa = svc.EXPLICACAO_DA_OPCAO[campo]
        for opcao in svc.CATEGORIAS[campo]:
            assert mapa.get(opcao), f"{campo}/{opcao} está sem explicação"
    # E "Mensal" e "Mês" não podem ter a MESMA explicação: se tivessem, a
    # explicação não explicaria nada.
    a = svc.EXPLICACAO_DA_OPCAO["categoria_alimentacao"]
    assert a["MENSAL"] != a["MES"]


def test_a_categoria_do_auxilio_tem_UMA_fonte_so():
    """⚑ ACHADO EM 15/11/2026, escrevendo a explicação de cada opção: havia
    DUAS listas para o mesmo campo — a do DP, com os rótulos do Pipe ("Vale
    Transporte"), e a do cadastro, com os códigos que a coluna guarda
    ("VALE_TRANSPORTE").

    A tela usava a do cadastro; o pedido gravava a do DP. Ou seja: o pedido de
    ajuda de custo guardava um texto que o cadastro não reconhece, e o
    atendimento não tinha como aplicá-lo ao benefício da pessoa. A diferença
    só apareceria na hora de valer.

    A fonte é o CADASTRO, porque é lá que o valor fica gravado e é de lá que a
    folha lê.
    """
    from app.apps.erp.core.cadastros import pessoas as p
    assert svc.CATEGORIAS["categoria_alimentacao"] == tuple(
        p.CATEGORIAS_ALIMENTACAO)
    assert svc.CATEGORIAS["categoria_transporte"] == tuple(
        c for c in p.CATEGORIAS_TRANSPORTE
        if c not in p.CATEGORIAS_TRANSPORTE_FORA_DE_USO)
    # E o que saiu de uso NÃO volta para a caixinha de pedido novo.
    assert "CARTAO" not in svc.CATEGORIAS["categoria_transporte"]


def test_o_aviso_previo_SOME_da_tela_no_fim_de_experiencia():
    """Deixar o campo à mostra, mesmo opcional, convida a preencher — e data de
    aviso prévio onde não há aviso prévio vira conta errada na rescisão."""
    oculta = svc.TIPOS["DESLIGAMENTO"]["ocultos_se"]["forma_desligamento"]
    assert "aviso_previo" in oculta["FIM_EXPERIENCIA"]
    assert "aviso_previo" not in svc.exigencias(
        "DESLIGAMENTO", {"forma_desligamento": "FIM_EXPERIENCIA"},
        None, "CTPS")["campos"]


def test_a_tela_recebe_a_traducao_de_todo_valor_usado_nas_condicoes():
    """⚑ A TRAVA DO RÓTULO ↔ CÓDIGO, e é o defeito mais fácil de deixar passar.

    As condições (`ocultos_se`, `obrigatorios_se`) são escritas com o código do
    banco; a caixinha mostra o rótulo do Pipe. Para "CTPS" os dois são iguais e
    a conferência passa; para "Diarista"→"DIARISTA" não são, e aí a condição da
    tela nunca dispara — o campo que devia sumir fica.

    Então: todo valor usado numa condição tem de ser ALCANÇÁVEL pela tradução
    que a tela recebe.
    """
    for t in svc.tipos_para_tela():
        traducoes = t["traducoes"]
        for chave in ("ocultos_se", "obrigatorios_se"):
            for gatilho, mapa in (t[chave] or {}).items():
                for valor in mapa:
                    alcancaveis = set(traducoes.get(gatilho, {}).values())
                    # Sem tradução declarada, o valor da caixinha é o próprio
                    # rótulo — e aí ele tem de estar entre as opções.
                    if not alcancaveis:
                        alcancaveis = set(t["opcoes"].get(gatilho, []))
                    assert valor in alcancaveis, (
                        f"{t['valor']}/{chave}: '{valor}' (gatilho {gatilho}) "
                        f"não é alcançável pelo que a tela mostra — a condição "
                        f"nunca vai disparar")
        # `visiveis_se` é escrito com o RÓTULO (a categoria não tem tradução).
        for campo, regras in (t["visiveis_se"] or {}).items():
            for gatilho, aceitos in regras.items():
                opcoes = set(t["opcoes"].get(gatilho, []))
                for valor in aceitos:
                    assert valor in opcoes, (
                        f"{t['valor']}/visiveis_se: '{valor}' não está entre as "
                        f"opções de {gatilho}")


# ---------------------------------------------------------------------------
# COM BANCO: o pedido inteiro, de ponta a ponta
# ---------------------------------------------------------------------------
@pytest.fixture
def mundo(sessao_real):
    from sqlalchemy import text
    from app.apps.erp.core.auth.service import gerar_hash
    from app.apps.erp.db.models.cadastros import Obra, PerfilUsuario as P, Usuario

    def gente(apelido, perfil):
        u = Usuario(nome=apelido, email=f"{apelido}@cond.teste", ativo=True,
                    senha_hash=gerar_hash("senha-de-teste-123"), perfil=perfil,
                    telefone="5585900000000")
        sessao_real.add(u)
        return u

    dp = gente("dp-cond", P.DEPARTAMENTO_PESSOAL)
    obra = gente("obra-cond", P.SUPERVISOR_OBRA)
    o = Obra(codigo="COND-01", nome="Obra das condições", status="ATIVA")
    o2 = Obra(codigo="COND-02", nome="Obra de destino", status="ATIVA")
    sessao_real.add_all([o, o2])
    sessao_real.flush()
    cargo_id = sessao_real.execute(text("""
        INSERT INTO cargos (nome, ativo) VALUES ('Servente de teste', TRUE)
        RETURNING id""")).scalar()
    carteira = svc_pessoas.salvar(sessao_real, {
        "cpf": CPF_A, "nome": "Carteira Da Silva", "admissao": "2024-01-05",
        "obra_id": o.id, "salario": "1800", "vinculo": "CTPS"}, dp)
    prestador = svc_pessoas.salvar(sessao_real, {
        "cpf": CPF_B, "nome": "Prestador De Souza", "admissao": "2024-03-10",
        "obra_id": o.id, "vinculo": "PRESTADOR"}, dp)
    sessao_real.flush()
    return {"s": sessao_real, "dp": dp, "obra_user": obra, "obra": o,
            "obra2": o2, "cargo_id": cargo_id,
            "carteira": carteira, "prestador": prestador}


def _cad(mundo, **extra):
    base = {"tipo": "CADASTRO", "nome": "Gente Nova De Teste", "cpf": CPF_C,
            "obra_id": mundo["obra"].id,
            "tipo_documentacao": "Prestador de Serviço",
            "tipo_contrato": "PJ", "pagamento": "Diarista",
            "cargo_pleiteado_id": mundo["cargo_id"],
            "cnpj_admissao": "00.079.526/0001-09 - MATRIZ/CE",
            "telefone": "85988881111", "pix_tipo": "CPF", "pix_chave": CPF_C,
            "valor_diaria": "130,00", "data_inicio": "2026-06-01",
            "documentos": ["DOC-IDENTIDADE", "CPF-DOC",
                           "COMPROVANTE-ENDERECO-PESSOA"]}
    base.update(extra)
    return base


@pytest.mark.banco
def test_desligar_prestador_sem_a_data_de_saida_e_recusado(mundo):
    with pytest.raises(ErroValidacao) as e:
        svc.abrir(mundo["s"], {"tipo": "DESLIGAMENTO",
                               "colaborador_id": mundo["prestador"].id},
                  mundo["obra_user"])
    assert "Data de Saída" in str(e.value)


@pytest.mark.banco
def test_desligar_prestador_nao_pede_aviso_previo(mundo):
    """A prova de que o tipo único funciona: o MESMO tipo, a mesma tela, e o
    prestador passa sem a data do aviso prévio que a carteira exige."""
    p = svc.abrir(mundo["s"], {"tipo": "DESLIGAMENTO",
                               "colaborador_id": mundo["prestador"].id,
                               "data_saida": "2026-07-31"}, mundo["obra_user"])
    assert p["dados"]["data_saida"] == "2026-07-31"
    assert "aviso_previo" not in p["dados"]
    # E o checklist é o de prestador: três passos, sem rescisão.
    assert len(p["itens"]) == 3


@pytest.mark.banco
def test_desligar_de_carteira_pelo_mesmo_tipo_continua_pedindo_o_aviso(mundo):
    with pytest.raises(ErroValidacao) as e:
        svc.abrir(mundo["s"], {"tipo": "DESLIGAMENTO",
                               "colaborador_id": mundo["carteira"].id,
                               "forma_desligamento": "Demissão com Aviso Trabalhado",
                               "continua_prestador": "Não, está saindo da empresa"},
                  mundo["obra_user"])
    assert "Data do Aviso Prévio" in str(e.value)


@pytest.mark.banco
def test_o_campo_do_outro_vinculo_nao_entra_no_pedido(mundo):
    """Mandar `data_saida` num desligamento de carteira não grava nada: o
    campo não é deste vínculo. Sem esta peneira, a obra informaria uma data de
    saída que o DP ia recalcular — e ninguém saberia qual valia."""
    p = svc.abrir(mundo["s"], {
        "tipo": "DESLIGAMENTO", "colaborador_id": mundo["carteira"].id,
        "forma_desligamento": "Demissão com Aviso Trabalhado",
        "continua_prestador": "Não, está saindo da empresa",
        "aviso_previo": "2026-07-01",
        "data_saida": "2026-12-31"}, mundo["obra_user"])
    assert "data_saida" not in p["dados"]


@pytest.mark.banco
def test_o_pedido_sobre_quem_ja_tem_ficha_pega_a_obra_da_ficha(mundo):
    """*"A obra está no cadastro. Ignora isso aí, tira isso aí."*"""
    p = svc.abrir(mundo["s"], {"tipo": "ATUALIZACAO_DADOS",
                               "colaborador_id": mundo["carteira"].id,
                               "descricao": "trocou de telefone"},
                  mundo["obra_user"])
    assert p["obra_id"] == mundo["obra"].id


@pytest.mark.banco
def test_cadastro_com_CPF_de_desligado_vira_readmissao(mundo):
    """⚑ A CONVERSÃO. Antes era recusa seca, e quem não soubesse que existe um
    tipo chamado Readmissão tentava de novo com o nome escrito diferente — e
    nascia a segunda ficha da mesma pessoa."""
    svc_pessoas.desligar(mundo["s"], mundo["prestador"].id,
                         {"data_saida": "2026-02-28",
                          "forma_desligamento": "PEDIDO_COLABORADOR"}, mundo["dp"])
    mundo["s"].flush()

    p = svc.abrir(mundo["s"], _cad(mundo, cpf=CPF_B, nome="Nome Digitado Errado",
                                   pix_chave=CPF_B, data_inicio="2026-08-01",
                                   obra_id=mundo["obra2"].id),
                  mundo["obra_user"])
    # Virou readmissão: ligada à pessoa que já existia, e DIZENDO que é.
    assert p["colaborador_id"] == mundo["prestador"].id
    assert p["readmissao"] is True
    assert p["tipo_rotulo"] == "Readmissão de Colaborador"
    # O nome do PEDIDO não vence o do cadastro — a pessoa é a que já existe.
    assert p["pessoa"] == "Prestador De Souza"
    # E o checklist é o da readmissão, não o do cadastro novo.
    assert any("saída anterior" in i["descricao"] for i in p["itens"])


@pytest.mark.banco
def test_atender_essa_readmissao_traz_a_pessoa_de_volta_sem_segunda_ficha(mundo):
    svc_pessoas.desligar(mundo["s"], mundo["prestador"].id,
                         {"data_saida": "2026-02-28",
                          "forma_desligamento": "PEDIDO_COLABORADOR"}, mundo["dp"])
    mundo["s"].flush()
    p = svc.abrir(mundo["s"], _cad(mundo, cpf=CPF_B, pix_chave=CPF_B,
                                   data_inicio="2026-08-01",
                                   obra_id=mundo["obra2"].id),
                  mundo["obra_user"])
    r = svc.atender(mundo["s"], p["id"], {}, mundo["dp"])
    mundo["s"].flush()

    assert r["colaborador_id"] == mundo["prestador"].id
    f = svc_pessoas.ficha(mundo["s"], mundo["prestador"].id)
    assert f["situacao"] == "ATIVO"
    # Dois vínculos: a passagem anterior NÃO é apagada — é o que a rescisão e
    # o INSS perguntam.
    assert len(f["vinculos"]) == 2
    # E a obra do pedido é a de DESTINO, não a de onde ela saiu.
    assert f["obra_id"] == mundo["obra2"].id


@pytest.mark.banco
def test_cadastro_com_CPF_de_quem_esta_ATIVO_continua_recusado(mundo):
    """Aí não há nada para readmitir: há uma ficha para corrigir."""
    with pytest.raises(ErroValidacao) as e:
        svc.abrir(mundo["s"], _cad(mundo, cpf=CPF_A, pix_chave=CPF_A),
                  mundo["obra_user"])
    assert "ATIVO" in str(e.value)


@pytest.mark.banco
def test_o_cadastro_novo_nasce_com_a_obra_do_pedido(mundo):
    """Defeito achado ao fazer a readmissão: o pedido de cadastro guardava a
    obra na solicitação, mas ela NÃO chegava ao cadastro criado — a pessoa
    nascia sem obra, e quem é preso a uma obra não a enxergava."""
    p = svc.abrir(mundo["s"], _cad(mundo, obra_id=mundo["obra2"].id),
                  mundo["obra_user"])
    r = svc.atender(mundo["s"], p["id"], {}, mundo["dp"])
    mundo["s"].flush()
    assert svc_pessoas.ficha(mundo["s"], r["colaborador_id"])["obra_id"] == \
        mundo["obra2"].id


@pytest.mark.banco
def test_a_readmissao_esquecida_aparece_e_sai_quando_e_feita(mundo):
    """⚑ A LISTA QUE AVISA ANTES DAS SETE DA MANHÃ.

    Desligar corta o acesso ao aparelho da obra na data de saída. Quando o
    desligamento diz "continua como prestador" e a readmissão é esquecida, a
    pessoa aparece para trabalhar e não consegue bater — e quem descobre é a
    obra, sem saber o motivo.

    A conta tem três condições, e todas têm de valer: o pedido disse que
    continua, a pessoa está desligada, e não há vínculo aberto. Feita a
    readmissão, a pendência desaparece sozinha — ninguém precisa "dar baixa"
    nela, que é o tipo de lista que ninguém mantém.
    """
    p = svc.abrir(mundo["s"], {
        "tipo": "DESLIGAMENTO", "colaborador_id": mundo["carteira"].id,
        "forma_desligamento": "Demissão com Aviso Trabalhado",
        "aviso_previo": "2026-06-01",
        "continua_prestador": "Sim, continua como prestador de serviço"},
        mundo["obra_user"])
    mundo["s"].flush()
    # Antes de atender ninguém está desligado, então nada pende.
    assert svc.readmissoes_esquecidas(mundo["s"]) == []

    svc.atender(mundo["s"], p["id"], {"dados": {"data_saida": "2026-06-30"}},
                mundo["dp"])
    mundo["s"].flush()
    pend = svc.readmissoes_esquecidas(mundo["s"])
    assert len(pend) == 1
    assert pend[0]["pessoa"] == "Carteira Da Silva"
    assert pend[0]["numero"] == p["numero"]

    # Feita a readmissão (pelo cadastro, que se converte), a pendência sai.
    r = svc.abrir(mundo["s"], _cad(mundo, cpf=CPF_A, pix_chave=CPF_A,
                                   data_inicio="2026-07-01"),
                  mundo["obra_user"])
    svc.atender(mundo["s"], r["id"], {}, mundo["dp"])
    mundo["s"].flush()
    assert svc.readmissoes_esquecidas(mundo["s"]) == []


@pytest.mark.banco
def test_desligamento_comum_nao_cria_pendencia(mundo):
    """Quem respondeu "está saindo da empresa" não vira cobrança eterna."""
    p = svc.abrir(mundo["s"], {
        "tipo": "DESLIGAMENTO", "colaborador_id": mundo["carteira"].id,
        "forma_desligamento": "Demissão com Aviso Trabalhado",
        "aviso_previo": "2026-06-01",
        "continua_prestador": "Não, está saindo da empresa"}, mundo["obra_user"])
    svc.atender(mundo["s"], p["id"], {"dados": {"data_saida": "2026-06-30"}},
                mundo["dp"])
    mundo["s"].flush()
    assert svc.readmissoes_esquecidas(mundo["s"]) == []
