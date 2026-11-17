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
    salário é o que esta regra evita.

    ⚑ O TIPO DE CONTRATO SAIU DESTA LISTA em 17/11/2026, e isso é correção de
    premissa. Ele tinha mandado fechá-lo para CTPS em 15/11 ("já é CLT"), e
    depois viu a consequência: *"Se Jovem Aprendiz é CTPS, precisamos poder
    selecionar o tipo de contrato."* Aprendizagem é contrato formal, com
    carteira, e não é CLT — então carteira TEM tipo de contrato, só que com
    outra lista. O que continua proibido é o pagamento e o valor do mês, que
    é o ponto deste teste.
    """
    exige = svc.exigencias("CADASTRO", {"tipo_documentacao": "CTPS"})["campos"]
    for proibido in ("pagamento", "valor_mes"):
        assert proibido not in exige, f"{proibido} continua exigido em CTPS"
    # Mas a carteira, o PIS e o tipo de contrato passam a ser exigidos.
    assert "num_ctps" in exige and "num_pis" in exige
    assert "tipo_contrato" in exige


def test_cadastro_de_prestador_pergunta_o_tipo_de_contrato():
    exige = svc.exigencias("CADASTRO", {"tipo_documentacao": "PRESTADOR"})["campos"]
    assert "tipo_contrato" in exige and "pagamento" in exige
    assert "num_ctps" not in exige


def test_cada_documentacao_tem_a_sua_lista_de_tipo_de_contrato():
    """17/11/2026 — o jovem aprendiz mudou esta regra, e vale contar por quê.

    Em 15/11 a lista era UMA, com seis opções, e o jovem aprendiz estava entre
    elas como se fosse prestador. Ele corrigiu: *"CTPS e jovem aprendiz na
    verdade estão corretos (…) não é CLT mas é Jovem aprendiz."* Contrato de
    aprendizagem tem carteira assinada — então ele pertence ao lado CTPS, e o
    lado CTPS deixou de ser vazio.

    Agora são duas listas sem interseção nenhuma. É a interseção que permitia
    gravar alguém de carteira como "PJ", e é por isso que o teste confere
    justamente ela.
    """
    from app.apps.erp.core.cadastros.pessoas import TIPOS_CONTRATO
    mapa = svc.TIPOS["CADASTRO"]["opcoes_se"]["tipo_contrato"]["tipo_documentacao"]
    codigo = svc.DO_PIPE_PARA_O_CODIGO["tipo_contrato"]

    carteira = [codigo[o] for o in mapa["CTPS"]]
    prestador = [codigo[o] for o in mapa["PRESTADOR"]]

    assert carteira == ["CLT_INDETERMINADO", "CLT_DETERMINADO", "JOVEM_APRENDIZ"]
    assert prestador == ["PJ", "AUTONOMO_RPA", "AUTONOMO_MENSALISTA",
                         "ESTAGIO", "PRO_LABORE"]
    assert not set(carteira) & set(prestador)

    # Todo código tem de existir no cadastro, senão gravar quebra na hora.
    for c in carteira + prestador:
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
        for campo in ("telefone", "pix_tipo", "pix_chave", "empresa_id"):
            assert campo in exige, f"{campo} não é obrigatório em {forma}"

    # ⚑ A DIÁRIA MUDOU DE REGRA EM 17/11/2026, e foi ele quem mudou: *"Valor da
    # Diária → exibir campo para CLT e se for Prestador de Serviço (Autônomo
    # (RPA)); demais, não exibir."*
    #
    # Ela era obrigatória SEMPRE — inclusive para PJ e pró-labore, que não têm
    # diária. Campo que não se aplica e é exigido é pedido que não sai, e a
    # pessoa não tem como resolver. Agora a exigência segue o TIPO DE CONTRATO,
    # e só onde o campo aparece.
    for contrato in ("CLT_INDETERMINADO", "CLT_DETERMINADO", "AUTONOMO_RPA"):
        exige = svc.exigencias("CADASTRO", {"tipo_contrato": contrato})["campos"]
        assert "valor_diaria" in exige, f"a diária é obrigatória em {contrato}"
    for contrato in ("PJ", "PRO_LABORE", "ESTAGIO", "JOVEM_APRENDIZ"):
        exige = svc.exigencias("CADASTRO", {"tipo_contrato": contrato})["campos"]
        assert "valor_diaria" not in exige, (
            f"{contrato} não tem diária — exigi-la trava o pedido sem saída")

    # ⚑ E O PJ PASSOU A PEDIR OS DADOS DA EMPRESA DELE (mesma data): *"quando
    # for pessoa PJ, abrir campos Razão social (PJ) e CNPJ (PJ)."* Sem eles o
    # DP recebe um PJ e vai pedir a nota fiscal sem saber em nome de quem sai.
    pj = svc.exigencias("CADASTRO", {"tipo_contrato": "PJ"})["campos"]
    assert "razao_social_pj" in pj and "cnpj_pj" in pj


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
        # ⚑ `visiveis_se` SEGUE A MESMA REGRA (15/11/2026), e ela pegou um
        # defeito de verdade: a condição do "Tipo de Contrato" estava escrita
        # com o código ("PRESTADOR") e a caixinha manda o rótulo ("Prestador
        # de Serviço") — o campo nunca aparecia. A tela passou a traduzir, e
        # esta trava agora aceita os dois caminhos, como os outros dois mapas.
        for campo, regras in (t["visiveis_se"] or {}).items():
            for gatilho, aceitos in regras.items():
                alcancaveis = set(t["traducoes"].get(gatilho, {}).values())
                alcancaveis |= set(t["opcoes"].get(gatilho, []))
                # "*" é o curinga (qualquer resposta serve) — não é um valor
                # da caixinha, e por isso não passa por esta conferência.
                alcancaveis.add("*")
                for valor in aceitos:
                    assert valor in alcancaveis, (
                        f"{t['valor']}/visiveis_se: '{valor}' (gatilho "
                        f"{gatilho}) não é alcançável pelo que a tela mostra — "
                        f"o campo '{campo}' nunca vai aparecer")


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
    # ⚑ DUAS EMPRESAS, e a segunda SEM CNPJ de propósito: é o caso real do
    # grupo (a C3 está cadastrada sem CNPJ) e era por causa dele que a
    # caixinha mostrava três de quatro.
    empresa_id = sessao_real.execute(text("""
        INSERT INTO empresas (razao_social, nome_fantasia, cnpj, ativo)
        VALUES ('BWS COND LTDA', 'MATRIZ/COND', '00.079.526/0001-09', TRUE)
        RETURNING id""")).scalar()
    empresa_sem_cnpj = sessao_real.execute(text("""
        INSERT INTO empresas (razao_social, nome_fantasia, cnpj, ativo)
        VALUES ('C3 COND LTDA', 'C3/COND', '', TRUE)
        RETURNING id""")).scalar()
    carteira = svc_pessoas.salvar(sessao_real, {
        "cpf": CPF_A, "nome": "Carteira Da Silva", "admissao": "2024-01-05",
        "obra_id": o.id, "salario": "1800", "vinculo": "CTPS"}, dp)
    prestador = svc_pessoas.salvar(sessao_real, {
        "cpf": CPF_B, "nome": "Prestador De Souza", "admissao": "2024-03-10",
        "obra_id": o.id, "vinculo": "PRESTADOR"}, dp)
    sessao_real.flush()
    return {"s": sessao_real, "dp": dp, "obra_user": obra, "obra": o,
            "obra2": o2, "cargo_id": cargo_id, "empresa_id": empresa_id,
            "empresa_sem_cnpj": empresa_sem_cnpj,
            "carteira": carteira, "prestador": prestador}


def _cad(mundo, **extra):
    base = {"tipo": "CADASTRO", "nome": "Gente Nova De Teste", "cpf": CPF_C,
            "obra_id": mundo["obra"].id,
            "tipo_documentacao": "Prestador de Serviço",
            "tipo_contrato": "PJ", "pagamento": "Diarista",
            "cargo_pleiteado_id": mundo["cargo_id"],
            "empresa_id": mundo["empresa_id"],
            "telefone": "85988881111", "pix_tipo": "CPF", "pix_chave": CPF_C,
            # ⚑ PJ PEDE OS DADOS DA EMPRESA DELE desde 17/11/2026 (ele: *"quando
            # for pessoa PJ, abrir campos Razão social (PJ) e CNPJ (PJ)"*) — e
            # as duas ajudas passaram a ser obrigatórias na mesma data.
            "razao_social_pj": "GENTE NOVA SERVICOS LTDA",
            "cnpj_pj": "71000003000106",
            "categoria_alimentacao": "NAO_RECEBE",
            "categoria_transporte": "NAO_RECEBE",
            # A diária NÃO é mais exigida de PJ (ele: *"exibir para CLT e
            # Autônomo (RPA); demais, não exibir"*), mas informá-la aqui não
            # atrapalha: o que o teste prova é o resto do pedido.
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


# ---------------------------------------------------------------------------
# A EMPRESA: ELE COBROU TRÊS VEZES, E NAS DUAS PRIMEIRAS EU CONSERTEI O LUGAR
# ERRADO
# ---------------------------------------------------------------------------
@pytest.mark.banco
def test_a_empresa_SEM_CNPJ_tambem_aparece_na_lista(mundo):
    """⚑ O DEFEITO QUE ELE VIU TRÊS VEZES, e a terceira foi assim
    (15/11/2026): *"em relação à empresa, CNPJ da empresa, você continua
    errando. Você está pegando do cadastro do colaborador, eu acho. Tem que
    pegar do cadastro das empresas do ERP. Lá já estão cadastradas 4 empresas.
    E continua aparecendo errado, ou seja, não está sendo usado o cadastro
    correto."*

    **A lista VINHA do cadastro de Empresas. O erro era o CAMPO.** Ele pedia um
    CNPJ ("CNPJ para Admissão"), então a lista só podia oferecer empresa que
    TEM CNPJ preenchido — e a quarta do grupo, cadastrada sem CNPJ, sumia. Na
    segunda cobrança eu "consertei" escrevendo embaixo do campo quem havia
    ficado de fora: isso explica o sintoma e mantém o defeito.

    O campo certo é a EMPRESA. O CNPJ é um dado dela, que o cadastro dela
    guarda. Agora as quatro aparecem, e a que está sem CNPJ aparece DIZENDO
    que está — cadastrar o CNPJ que falta volta a ser arrumar o cadastro da
    empresa, não condição para abrir um pedido.
    """
    lista = svc.empresas_para_escolher(mundo["s"])
    ids = {x["id"] for x in lista}
    assert mundo["empresa_id"] in ids
    assert mundo["empresa_sem_cnpj"] in ids, (
        "a empresa sem CNPJ desapareceu da lista — é EXATAMENTE o defeito que "
        "ele viu três vezes")
    # E ela aparece dizendo o que falta, em vez de sumir calada.
    sem = next(x for x in lista if x["id"] == mundo["empresa_sem_cnpj"])
    assert "sem CNPJ no cadastro" in sem["rotulo"]


@pytest.mark.banco
def test_sem_empresa_cadastrada_a_lista_vem_VAZIA_e_nao_inventada(mundo):
    """⚑ A LISTA DE SOCORRO FOI APAGADA, e isto é o teste dela.

    Antes, se a consulta não achasse nada, a caixinha mostrava TRÊS empresas
    escritas dentro do código — e três era exatamente o número que ele
    reclamou de ver. Lista inventada que parece certa é pior que lista vazia:
    ninguém desconfia dela, e o pedido sai gravado com um CNPJ que ninguém
    cadastrou.
    """
    assert svc.empresas_para_escolher(None) == []
    # E nenhum CNPJ continua escrito à mão nas listas fechadas.
    for campo, opcoes in svc.CATEGORIAS.items():
        for o in opcoes:
            assert "/0001-" not in str(o), (
                f"voltou um CNPJ digitado dentro do código, em {campo}")


@pytest.mark.banco
def test_empresa_inativa_e_recusada_ao_abrir_o_pedido(mundo):
    """Tela esconde opção; servidor é que recusa. Sem esta trava, um pedido
    antigo reenviado registraria alguém numa empresa que a empresa fechou."""
    from sqlalchemy import text
    morta = mundo["s"].execute(text("""
        INSERT INTO empresas (razao_social, cnpj, ativo)
        VALUES ('EMPRESA FECHADA LTDA', '11.222.333/0001-81', FALSE)
        RETURNING id""")).scalar()
    mundo["s"].flush()
    with pytest.raises(ErroValidacao) as e:
        svc.abrir(mundo["s"], _cad(mundo, empresa_id=morta), mundo["obra_user"])
    assert "inativa" in str(e.value)


@pytest.mark.banco
def test_o_pedido_guarda_a_empresa_E_o_CNPJ_dela_para_o_DP_ler(mundo):
    """A escolha que vale é a empresa; o CNPJ viaja junto para o DP não ter de
    abrir outra tela. Um é derivado do outro, num lugar só, e por isso os dois
    não podem divergir."""
    p = svc.abrir(mundo["s"], _cad(mundo), mundo["obra_user"])
    assert str(p["dados"]["empresa_id"]) == str(mundo["empresa_id"])
    assert p["dados"]["cnpj_admissao"] == "00.079.526/0001-09"


@pytest.mark.banco
def test_atender_grava_a_empresa_na_ficha_da_pessoa(mundo):
    """O pedido dizia a empresa e o cadastro ficava sem ela: é o tipo de
    divergência que o atendimento existe para não deixar acontecer."""
    p = svc.abrir(mundo["s"], _cad(mundo), mundo["obra_user"])
    r = svc.atender(mundo["s"], p["id"], {}, mundo["dp"])
    mundo["s"].flush()
    from app.apps.erp.db.models.cadastros import Colaborador
    c = mundo["s"].get(Colaborador, r["colaborador_id"])
    assert c.empresa_id == mundo["empresa_id"]


# ---------------------------------------------------------------------------
# O VALOR DO AUXÍLIO: ELE PEDIU, E A DIVISÃO ANTERIOR ESTAVA ERRADA
# ---------------------------------------------------------------------------
def test_o_valor_dos_auxilios_e_campo_de_quem_PEDE():
    """⚑ 15/11/2026, ele: *"não foi colocado campo para colocar valor de
    transporte e alimentação."*

    Eu havia trazido a CATEGORIA para o formulário e deixado o VALOR com o DP.
    A divisão estava errada, e o próprio checklist do DP confessava: ele dizia
    "conferir o valor com a obra" — ou seja, o número nasce na obra. Perguntar
    ao DP um valor que ele vai ter de ligar para a obra para saber é pôr uma
    ligação telefônica dentro do sistema.
    """
    for chave in ("CADASTRO", "AJUDA_DE_CUSTO"):
        regra = svc.TIPOS[chave]
        for campo in ("aux_alimentacao", "aux_transporte"):
            assert campo in regra["campos"], (
                f"{chave} pergunta a categoria do auxílio e não o valor")
            assert campo not in regra.get("campos_do_dp", ()), (
                f"{chave}: o valor não pode estar nos dois lados")


def test_o_valor_so_aparece_depois_da_categoria():
    """Antes de escolher a categoria, "valor da alimentação" não quer dizer
    nada — e campo sem sentido na tela é campo que alguém preenche errado."""
    for chave in ("CADASTRO", "AJUDA_DE_CUSTO"):
        regra = svc.TIPOS[chave]
        assert svc._nao_cabe_agora(regra, "aux_alimentacao", {}) is True
        assert svc._nao_cabe_agora(
            regra, "aux_alimentacao", {"categoria_alimentacao": "SEG_QUI"}) is False
        assert svc._nao_cabe_agora(regra, "aux_transporte", {}) is True
        assert svc._nao_cabe_agora(
            regra, "aux_transporte", {"categoria_transporte": "DIARIO"}) is False


def test_o_curinga_vale_para_QUALQUER_resposta():
    """⚑ "*" EXISTE PARA NÃO TER DE LISTAR AS NOVE OPÇÕES uma por uma — e
    listar as nove é o jeito de esquecer a décima quando ela nascer.

    O valor do auxílio faz sentido assim que a categoria é escolhida, seja
    qual for ela.
    """
    regra = svc.TIPOS["AJUDA_DE_CUSTO"]
    for opcao in svc.CATEGORIAS["categoria_transporte"]:
        assert svc._nao_cabe_agora(
            regra, "aux_transporte", {"categoria_transporte": opcao}) is False, (
            f"com a categoria '{opcao}' escolhida, o valor devia aparecer")
    # E com a categoria em branco, não.
    assert svc._nao_cabe_agora(
        regra, "aux_transporte", {"categoria_transporte": ""}) is True


@pytest.mark.banco
def test_o_valor_informado_pela_obra_chega_ao_cadastro(mundo):
    """A prova de ponta a ponta: a obra informa, o atendimento aplica."""
    p = svc.abrir(mundo["s"], _cad(
        mundo, categoria_alimentacao="SEG_SEX", aux_alimentacao="25,00",
        categoria_transporte="DIARIO", aux_transporte="11,50"),
        mundo["obra_user"])
    assert p["dados"]["aux_alimentacao"] == "25,00"
    r = svc.atender(mundo["s"], p["id"], {}, mundo["dp"])
    mundo["s"].flush()
    from app.apps.erp.db.models.cadastros import Colaborador
    c = mundo["s"].get(Colaborador, r["colaborador_id"])
    assert float(c.aux_alimentacao) == 25.0
    assert float(c.aux_transporte) == 11.5
    assert c.categoria_alimentacao == "SEG_SEX"
    assert c.categoria_transporte == "DIARIO"


# ---------------------------------------------------------------------------
# O BECO SEM SAÍDA DA TELA DO DP
# ---------------------------------------------------------------------------
def test_a_tela_do_DP_nao_promete_tipo_que_exige_papel_sem_ter_onde_anexar():
    """⚑ TRÊS TIPOS NÃO SAÍAM DA TELA DO DP, e ela não dizia isso (15/11/2026).

    Achado respondendo a uma pergunta dele: *"o nome e CPF são preenchidos a
    partir dos documentos anexados? Se for assim, o correto é que o campo para
    anexar esse documento era para ser o primeiro, não é não?"*

    São preenchidos, sim — e a conferência mostrou que a tela do DP **não tem
    campo para anexar nada**. Cadastro de Colaborador, Admissão de Prestador e
    Cadastro de Dependente exigem papel: a pessoa preenchia DOZE campos,
    confirmava, e levava "Falta anexar…" sobre papéis que a janela nunca
    ofereceu onde pôr. Beco sem saída, e estava em produção.

    Esta trava é de TEXTO porque o defeito era de tela: ou a tela aprende a
    anexar, ou ela diz para onde ir. Hoje ela diz. Se alguém acrescentar um
    tipo com papel e a tela voltar a fingir que dá conta, isto quebra.
    """
    from pathlib import Path
    tela = Path("app/apps/erp/templates/erp_dp_solicitacoes.html").read_text(
        encoding="utf-8")
    # A tela trata o caso pelo que o SERVIDOR declara (`documentos`), e não
    # por uma lista de tipos escrita aqui — tipo novo nasce tratado.
    assert "(regra.documentos || []).length" in tela, (
        "a tela não desvia mais o tipo que exige papel: quem escolher vai "
        "preencher tudo para levar 'Falta anexar' no fim")
    # E todo papel exigido por algum tipo tem nome em português nesta tela.
    for regra in svc.TIPOS.values():
        for papel in tuple(regra.get("documentos", ())) + tuple(
                p for mapa in (regra.get("documentos_se") or {}).values()
                for lista in mapa.values() for p in lista):
            assert f'"{papel}":' in tela, (
                f"{papel} sairia com o código cru no recado da tela do DP")


def test_todo_tipo_que_exige_papel_existe_no_PORTAL():
    """O recado manda a pessoa para o portal — então o tipo tem de estar lá.

    Mandar para uma tela que não oferece aquele pedido seria trocar um beco sem
    saída por outro, com um passo a mais.
    """
    from app.apps.erp.core import portal as svc_portal
    for chave, regra in svc.TIPOS.items():
        if regra.get("oculto") or not regra.get("documentos"):
            continue
        assert chave in svc_portal.TIPOS_DO_PORTAL, (
            f"{chave} exige papel, a tela do DP manda para o portal, e o "
            f"portal não oferece este tipo")
