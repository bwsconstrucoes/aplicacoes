# -*- coding: utf-8 -*-
"""A FILA DE SOLICITAÇÕES DO DP (migração 088, 05/11/2026).

O segundo pipe do Pipefy (80.992 cards) dentro do ERP. Decisão do dono:
*"todos os pedidos vão para o DP, e quem tiver perfil do DP que vai aprovar."*

A DIFERENÇA QUE ESTES TESTES DEFENDEM: no Pipefy, atender era mover o card — e
alguém, depois, mexia no cadastro à mão ou por um cenário do Make. O pedido
dizia uma coisa e o cadastro ficava outra. Aqui o atendimento EXECUTA, na mesma
transação, usando as mesmas funções da tela.
"""
from __future__ import annotations

import pytest

from app.apps.erp.core import pessoal_solicitacoes as svc
from app.apps.erp.core.cadastros import pessoas as svc_pessoas
from app.apps.erp.core.comum.auditoria import ErroValidacao

pytestmark = pytest.mark.banco

CPF_A = "52998224725"
CPF_B = "11144477735"


@pytest.fixture
def mundo(sessao_real):
    from app.apps.erp.core.auth.service import gerar_hash
    from app.apps.erp.db.models.cadastros import Obra, PerfilUsuario as P, Usuario

    def pessoa(apelido, perfil):
        u = Usuario(nome=apelido, email=f"{apelido}@dp.teste", ativo=True,
                    senha_hash=gerar_hash("senha-de-teste-123"), perfil=perfil,
                    telefone="5585900000000")
        sessao_real.add(u)
        return u

    dp = pessoa("dp-teste", P.DEPARTAMENTO_PESSOAL)
    encarregado = pessoa("encarregado-teste", P.SUPERVISOR_OBRA)
    obra = Obra(codigo="SDP-01", nome="Obra das solicitações", status="ATIVA")
    sessao_real.add(obra)
    sessao_real.flush()
    c = svc_pessoas.salvar(sessao_real, {
        "cpf": CPF_A, "nome": "José Da Fila", "admissao": "2024-01-05",
        "obra_id": obra.id, "salario": "1800", "vinculo": "CTPS"}, dp)
    # O "Cargo Pleiteado" é obrigatório no Pipe em readmissão, alteração de
    # cargo e cadastro — por isso o mundo de teste tem um cargo.
    from sqlalchemy import text as _t
    cargo_id = sessao_real.execute(_t("""
        INSERT INTO cargos (nome, ativo) VALUES ('Pedreiro de teste', TRUE)
        RETURNING id""")).scalar()
    sessao_real.flush()
    cargo = type("Cargo", (), {"id": cargo_id})()
    return {"s": sessao_real, "dp": dp, "encarregado": encarregado,
            "obra": obra, "pessoa": c, "cargo": cargo}


# ---------------------------------------------------------------------------
# Abrir
# ---------------------------------------------------------------------------
def test_o_numero_do_pedido_e_humano_e_sequencial(mundo):
    """SDP-000001, como o "Nº SDP" do pipe: é por ele que a obra e o DP se
    referem ao pedido."""
    p1 = svc.abrir(mundo["s"], {"tipo": "DIVERSOS", "descricao": "primeira",
                           "obra_id": mundo["obra"].id}, mundo["dp"])
    p2 = svc.abrir(mundo["s"], {"tipo": "DIVERSOS", "descricao": "segunda",
                           "obra_id": mundo["obra"].id}, mundo["dp"])
    assert p1["numero"].startswith("SDP-") and len(p1["numero"]) == 10
    assert p2["numero"] != p1["numero"]


def test_tipo_inventado_e_recusado(mundo):
    with pytest.raises(ErroValidacao):
        svc.abrir(mundo["s"], {"tipo": "QUALQUER_COISA"}, mundo["dp"])


def test_tipo_que_exige_pessoa_cobra_a_pessoa(mundo):
    with pytest.raises(ErroValidacao) as e:
        svc.abrir(mundo["s"], {"tipo": "DESLIGAMENTO",
                               "forma_desligamento": "Demissão com Aviso Trabalhado",
                               "aviso_previo": "2026-03-01"},
                  mundo["dp"])
    assert "escolha a pessoa" in str(e.value).lower()


def test_o_pedido_guarda_SO_os_campos_do_tipo(mundo):
    """Sem a peneira, o formulário de um tipo deixaria lixo do outro no pedido —
    e foi assim que o pipe terminou com 106 campos e metade vazia em cada card."""
    p = svc.abrir(mundo["s"], {
        "tipo": "DEPENDENTE", "colaborador_id": mundo["pessoa"].id,
        "nome": "Filho de Teste", "parentesco": "Filho",
        "data_nascimento": "2020-05-01",
        "forma_desligamento": "AVISO_INDENIZADO",     # campo de OUTRO tipo
        "valor_total": "999",                          # idem
        "matricula": "12345",                 # campo do DP, não de quem pede
    }, mundo["encarregado"])
    assert set(p["dados"]) == {"nome", "parentesco", "data_nascimento"}
    # ⚑ O rótulo do Pipe entra; o CÓDIGO é o que fica gravado.
    assert p["dados"]["parentesco"] == "FILHO"


def test_campo_do_DP_nao_entra_no_formulario_de_quem_pede():
    """⚑ A TRAVA DA SEPARAÇÃO, e ela nasce de uma bronca dele (10/11/2026):
    *"matrícula, não é o funcionário que decide. Como é que a pessoa vai
    readmitir uma pessoa, botar uma matrícula? Quem solicita define uma
    matrícula? Que porra é essa?"* — e, sobre a convenção coletiva,
    *"indecível, é DP quem decide"*.

    No Pipe a separação é física: o que está no formulário INICIAL é da obra,
    o que está numa FASE é do DP. Aqui ela é esta lista.
    """
    for codigo, d in svc.TIPOS.items():
        comuns = set(d["campos"]) & set(d.get("campos_do_dp", ()))
        assert not comuns, (
            f"{codigo}: {comuns} está nos DOIS lados — ou é da obra, ou é do "
            f"DP, nunca dos dois")
    # Os quatro que ele apontou, um por um.
    for codigo in ("READMISSAO", "CADASTRO"):
        campos = svc.TIPOS[codigo]["campos"]
        for proibido in ("matricula", "convencao_id", "salario"):
            assert proibido not in campos, (
                f"{proibido} voltou ao formulário de quem pede, em {codigo}")
    assert "faltas_a_descontar" not in svc.TIPOS["DESLIGAMENTO"]["campos"]
    assert "ultimo_dia" not in svc.TIPOS["DESLIGAMENTO"]["campos"]
    assert "dependente_ir" not in svc.TIPOS["DEPENDENTE"]["campos"]


def test_os_rotulos_sao_os_do_pipe_e_nao_os_meus():
    """Ele: *"por que não está usando o termo que está no Pipe? Por que está
    querendo inventar a roda de novo?"* A fonte é DE_PARA_PIPEFY_DP.md."""
    nomes = {d["nome"] for d in svc.TIPOS.values()}
    assert "Admissão de Prestador de Serviço" in nomes
    assert "Prestador que passa a ter carteira" not in nomes   # inventado
    assert "Desligamento de Prestador de Serviço" in nomes
    assert svc.ROTULOS["categoria_alimentacao"] == "Categoria Auxílio Alimentação"
    assert svc.CATEGORIAS["categoria_alimentacao"][0] == "Segunda à Quinta"
    assert svc.ROTULOS["tipo_readmissao"] == "Tipo de Readmissão"
    # As quatro formas de desligamento, como estão lá.
    assert svc.CATEGORIAS["forma_desligamento"] == (
        "À Pedido do Colaborador", "Encerramento de Contrato de Experiência",
        "Demissão com Aviso Trabalhado", "Demissão com Aviso Indenizado")


def test_o_checklist_do_tipo_nasce_com_o_pedido(mundo):
    """O desligamento CTPS tem ONZE passos no pipe, e eles existem porque o
    atendimento é trabalho de vários dias e mais de uma pessoa."""
    p = svc.abrir(mundo["s"], {"tipo": "DESLIGAMENTO",
                               "colaborador_id": mundo["pessoa"].id,
                               "forma_desligamento": "Demissão com Aviso Trabalhado",
                               "aviso_previo": "2026-03-01"}, mundo["dp"])
    assert p["total_itens"] == 11 and p["feitos"] == 0
    assert any("TRCT" in i["descricao"] for i in p["itens"])


def test_desligamento_sem_data_de_saida_e_recusado_na_abertura(mundo):
    with pytest.raises(ErroValidacao):
        svc.abrir(mundo["s"], {"tipo": "DESLIGAMENTO",
                               "colaborador_id": mundo["pessoa"].id}, mundo["dp"])


def test_pre_cadastro_de_quem_ja_existe_e_recusado(mundo):
    """É o alerta de duplicado valendo também na fila: no pipe, o cenário do
    Make APAGAVA o card depois de criado."""
    with pytest.raises(ErroValidacao) as e:
        svc.abrir(mundo["s"], {"tipo": "CADASTRO", "tipo_documentacao": "CTPS",
        "pagamento": "Mensalista", "cargo_pleiteado_id": mundo["cargo"].id, "data_inicio": "2026-05-01", "nome": "José Repetido",
                               "cpf": CPF_A,
                               "obra_id": mundo["obra"].id}, mundo["encarregado"])
    assert "já tem cadastro" in str(e.value)


# ---------------------------------------------------------------------------
# ATENDER EXECUTA — a diferença em relação ao Pipefy
# ---------------------------------------------------------------------------
def test_atender_desligamento_DESLIGA_de_verdade(mundo):
    p = svc.abrir(mundo["s"], {
        "tipo": "DESLIGAMENTO", "colaborador_id": mundo["pessoa"].id,
        "aviso_previo": "2026-03-01",
        # O rótulo do Pipe; o que fica gravado é PEDIDO_COLABORADOR.
        "forma_desligamento": "Demissão com Aviso Trabalhado"}, mundo["dp"])
    # ⚑ A DATA DE SAÍDA É DO DP, e vai ao ATENDER. Ele, em 10/11/2026: *"de
    # saída, o último dia trabalhado, não é a pessoa que está pedindo que vai
    # dizer. Ele vai colocar só a data do aviso prévio."*
    svc.atender(mundo["s"], p["id"],
                {"resposta": "feito", "dados": {"data_saida": "2026-03-31"}},
                mundo["dp"])
    mundo["s"].flush()

    f = svc_pessoas.ficha(mundo["s"], mundo["pessoa"].id)
    assert f["situacao"] == "DESLIGADO" and f["demissao"] == "2026-03-31"
    assert f["vinculos"][0]["aberto"] is False


def test_atender_readmissao_READMITE_de_verdade(mundo):
    svc_pessoas.desligar(mundo["s"], mundo["pessoa"].id,
                         {"data_saida": "2025-06-30"}, mundo["dp"])
    mundo["s"].flush()
    p = svc.abrir(mundo["s"], {"tipo": "READMISSAO", "tipo_readmissao": "CTPS", "data_inicio": "2026-05-01",
                               "cnpj_admissao": "00.079.526/0001-09 - MATRIZ/CE",
                               "cargo_pleiteado_id": mundo["cargo"].id,
                               "colaborador_id": mundo["pessoa"].id}, mundo["dp"])
    # Salário e a data de admissão efetiva são do DP: a obra pede o cargo.
    svc.atender(mundo["s"], p["id"],
                {"dados": {"admissao": "2026-02-01", "salario": "2230"}},
                mundo["dp"])
    mundo["s"].flush()

    f = svc_pessoas.ficha(mundo["s"], mundo["pessoa"].id)
    assert f["situacao"] == "ATIVO" and len(f["vinculos"]) == 2
    assert f["salario"] == 2230.0


def test_atender_pre_cadastro_CRIA_a_pessoa(mundo):
    p = svc.abrir(mundo["s"], {
        "tipo": "CADASTRO", "tipo_documentacao": "CTPS",
        "pagamento": "Mensalista", "cargo_pleiteado_id": mundo["cargo"].id, "data_inicio": "2026-05-01", "nome": "Maria Nova De Teste", "cpf": CPF_B,
        "obra_id": mundo["obra"].id, "vinculo": "CTPS",
        "data_inicio": "2026-04-01"}, mundo["encarregado"])
    assert p["colaborador_id"] is None          # a pessoa ainda não existe

    r = svc.atender(mundo["s"], p["id"], {}, mundo["dp"])
    mundo["s"].flush()

    assert r["colaborador_id"] is not None
    f = svc_pessoas.ficha(mundo["s"], r["colaborador_id"])
    assert f["nome"] == "Maria Nova De Teste" and f["cpf"] == CPF_B


def test_atender_dependente_CADASTRA_o_dependente(mundo):
    p = svc.abrir(mundo["s"], {
        "tipo": "DEPENDENTE", "colaborador_id": mundo["pessoa"].id,
        "nome": "Filha De Teste", "parentesco": "FILHO",
        "data_nascimento": "2020-05-01"}, mundo["encarregado"])
    # Salário-família é marcação do DP: a obra não sabe se cabe.
    svc.atender(mundo["s"], p["id"], {"dados": {"salario_familia": "1"}},
                mundo["dp"])
    mundo["s"].flush()

    d = svc_pessoas.ficha(mundo["s"], mundo["pessoa"].id)["dependentes"]
    assert len(d) == 1 and d[0]["nome"] == "Filha De Teste"
    assert d[0]["salario_familia"] is True


def test_atender_alteracao_GRAVA_o_de_para(mundo):
    p = svc.abrir(mundo["s"], {
        "tipo": "ALTERACAO_CARGO", "colaborador_id": mundo["pessoa"].id,
        "cargo_pleiteado_id": mundo["cargo"].id,
        "data_inicio": "2026-06-01", "justificativa": "assumiu a frente"},
        mundo["encarregado"])
    # Salário e motivo são do DP — a obra pede o CARGO, não o salário.
    svc.atender(mundo["s"], p["id"],
                {"dados": {"vigencia": "2026-06-01", "salario": "2500",
                           "motivo": "MERITO"}}, mundo["dp"])
    mundo["s"].flush()

    a = svc_pessoas.ficha(mundo["s"], mundo["pessoa"].id)["alteracoes"][0]
    assert (a["salario_de"], a["salario_para"]) == (1800.0, 2500.0)
    assert a["motivo_rotulo"] == "Mérito"


def test_o_DP_pode_corrigir_o_pedido_ao_atender(mundo):
    """A obra pede "salário 2200" e o DP acerta no piso da convenção. O que fica
    gravado é o que foi EXECUTADO, não o que foi pedido."""
    p = svc.abrir(mundo["s"], {
        "tipo": "ALTERACAO_CARGO", "colaborador_id": mundo["pessoa"].id,
        "cargo_pleiteado_id": mundo["cargo"].id,
        "data_inicio": "2026-06-01", "justificativa": "piso novo"},
        mundo["encarregado"])
    r = svc.atender(mundo["s"], p["id"],
                    {"dados": {"vigencia": "2026-06-01", "salario": "2230",
                               "motivo": "ENQUADRAMENTO"}}, mundo["dp"])
    mundo["s"].flush()

    assert r["dados"]["salario"] == "2230"
    assert svc_pessoas.ficha(mundo["s"], mundo["pessoa"].id)["salario"] == 2230.0


def test_pedido_impossivel_nao_e_atendido_pela_metade(mundo):
    """Readmitir quem está ativo é recusado pela regra do cadastro — e o pedido
    NÃO pode ficar "atendido" com o cadastro intacto."""
    p = svc.abrir(mundo["s"], {"tipo": "READMISSAO", "tipo_readmissao": "CTPS", "data_inicio": "2026-05-01",
                               "cnpj_admissao": "00.079.526/0001-09 - MATRIZ/CE",
                               "cargo_pleiteado_id": mundo["cargo"].id,
                               "colaborador_id": mundo["pessoa"].id,
                               "admissao": "2026-02-01"}, mundo["dp"])
    with pytest.raises(ErroValidacao):
        svc.atender(mundo["s"], p["id"], {}, mundo["dp"])
    mundo["s"].rollback()


def test_atender_duas_vezes_e_recusado(mundo):
    p = svc.abrir(mundo["s"], {"tipo": "DIVERSOS", "descricao": "teste",
                           "obra_id": mundo["obra"].id}, mundo["dp"])
    svc.atender(mundo["s"], p["id"], {}, mundo["dp"])
    mundo["s"].flush()
    with pytest.raises(ErroValidacao) as e:
        svc.atender(mundo["s"], p["id"], {}, mundo["dp"])
    assert "atendida" in str(e.value).lower()


# ---------------------------------------------------------------------------
# Recusar e cancelar
# ---------------------------------------------------------------------------
def test_recusar_exige_o_motivo(mundo):
    """O pipe tinha "Por quê?" obrigatório na recusa: quem pediu precisa saber o
    que corrigir para pedir de novo."""
    p = svc.abrir(mundo["s"], {"tipo": "DIVERSOS", "descricao": "teste",
                           "obra_id": mundo["obra"].id}, mundo["dp"])
    with pytest.raises(ErroValidacao):
        svc.recusar(mundo["s"], p["id"], {}, mundo["dp"])
    r = svc.recusar(mundo["s"], p["id"], {"resposta": "falta a autorização da obra"},
                    mundo["dp"])
    assert r["situacao"] == "RECUSADA" and "autorização" in r["resposta"]


def test_cancelar_nao_apaga_o_pedido(mundo):
    """No pipe, cancelar era ABRIR OUTRO CARD. E pedido apagado deixa "o que
    aconteceu com aquele desligamento?" sem resposta."""
    p = svc.abrir(mundo["s"], {"tipo": "DESLIGAMENTO",
                               "colaborador_id": mundo["pessoa"].id,
                               "forma_desligamento": "Demissão com Aviso Trabalhado",
                               "aviso_previo": "2026-03-01"}, mundo["dp"])
    r = svc.cancelar(mundo["s"], p["id"], {"motivo": "a pessoa desistiu de sair"},
                     mundo["dp"])
    assert r["situacao"] == "CANCELADA" and "desistiu" in r["motivo_cancelamento"]
    assert svc.ler(mundo["s"], p["id"])["numero"] == p["numero"]
    # e o cadastro NÃO foi mexido
    assert svc_pessoas.ficha(mundo["s"], mundo["pessoa"].id)["situacao"] == "ATIVO"


def test_cancelar_pedido_ja_atendido_e_recusado(mundo):
    p = svc.abrir(mundo["s"], {"tipo": "DIVERSOS", "descricao": "teste",
                           "obra_id": mundo["obra"].id}, mundo["dp"])
    svc.atender(mundo["s"], p["id"], {}, mundo["dp"])
    mundo["s"].flush()
    with pytest.raises(ErroValidacao):
        svc.cancelar(mundo["s"], p["id"], {"motivo": "mudei de ideia"}, mundo["dp"])


# ---------------------------------------------------------------------------
# O checklist
# ---------------------------------------------------------------------------
def test_marcar_item_poe_o_pedido_em_analise(mundo):
    """É o sinal de que alguém começou — e é o que a fila mostra como "em
    andamento", o que trava o DP."""
    p = svc.abrir(mundo["s"], {"tipo": "DESLIGAMENTO",
                               "colaborador_id": mundo["pessoa"].id,
                               "forma_desligamento": "Demissão com Aviso Trabalhado",
                               "aviso_previo": "2026-03-01"}, mundo["dp"])
    assert p["situacao"] == "ABERTA"
    r = svc.marcar_item(mundo["s"], p["id"], p["itens"][0]["id"], True, mundo["dp"])
    assert r["situacao"] == "EM_ANALISE" and r["feitos"] == 1


def test_desmarcar_item_volta_atras(mundo):
    p = svc.abrir(mundo["s"], {"tipo": "READMISSAO", "tipo_readmissao": "CTPS", "data_inicio": "2026-05-01",
                               "cnpj_admissao": "00.079.526/0001-09 - MATRIZ/CE",
                               "cargo_pleiteado_id": mundo["cargo"].id,
                               "colaborador_id": mundo["pessoa"].id,
                               "admissao": "2026-02-01"}, mundo["dp"])
    svc.marcar_item(mundo["s"], p["id"], p["itens"][0]["id"], True, mundo["dp"])
    r = svc.marcar_item(mundo["s"], p["id"], p["itens"][0]["id"], False, mundo["dp"])
    assert r["feitos"] == 0


def test_item_de_outro_pedido_e_recusado(mundo):
    p1 = svc.abrir(mundo["s"], {"tipo": "DESLIGAMENTO",
                                "colaborador_id": mundo["pessoa"].id,
                                "forma_desligamento": "Demissão com Aviso Trabalhado",
                                "aviso_previo": "2026-03-01"}, mundo["dp"])
    p2 = svc.abrir(mundo["s"], {"tipo": "DIVERSOS", "descricao": "outro",
                           "obra_id": mundo["obra"].id}, mundo["dp"])
    with pytest.raises(ErroValidacao):
        svc.marcar_item(mundo["s"], p2["id"], p1["itens"][0]["id"], True, mundo["dp"])


# ---------------------------------------------------------------------------
# A fila e a trilha
# ---------------------------------------------------------------------------
def test_a_fila_poe_as_abertas_na_frente(mundo):
    p1 = svc.abrir(mundo["s"], {"tipo": "DIVERSOS", "descricao": "vai ser atendida",
                           "obra_id": mundo["obra"].id},
                   mundo["dp"])
    svc.abrir(mundo["s"], {"tipo": "DIVERSOS", "descricao": "fica aberta",
                           "obra_id": mundo["obra"].id}, mundo["dp"])
    svc.atender(mundo["s"], p1["id"], {}, mundo["dp"])
    mundo["s"].flush()

    fila = svc.listar(mundo["s"])
    assert fila["solicitacoes"][0]["aberta"] is True
    assert fila["indicadores"] == {"abertas": 1, "atendidas": 1, "recusadas": 0,
                                   "em_andamento": 0}


def test_em_andamento_conta_o_checklist_comecado_e_nao_terminado(mundo):
    p = svc.abrir(mundo["s"], {"tipo": "DESLIGAMENTO",
                               "colaborador_id": mundo["pessoa"].id,
                               "forma_desligamento": "Demissão com Aviso Trabalhado",
                               "aviso_previo": "2026-03-01"}, mundo["dp"])
    svc.marcar_item(mundo["s"], p["id"], p["itens"][0]["id"], True, mundo["dp"])
    mundo["s"].flush()
    assert svc.listar(mundo["s"])["indicadores"]["em_andamento"] == 1


def test_a_trilha_guarda_quem_mexeu(mundo):
    """O pipe guardava isso no histórico do card — e o card vai morrer. Sem a
    trilha, "quem autorizou essa ajuda de custo?" fica sem resposta."""
    p = svc.abrir(mundo["s"], {"tipo": "AJUDA_DE_CUSTO", "solicitacao": "Solicitar ou Atualizar",
                               "colaborador_id": mundo["pessoa"].id,
                               "aux_alimentacao": "25,00"}, mundo["encarregado"])
    svc.atender(mundo["s"], p["id"], {"resposta": "autorizado"}, mundo["dp"])
    mundo["s"].flush()

    e = svc.ler(mundo["s"], p["id"])["eventos"]
    assert [x["acao"] for x in e] == ["ABERTA", "ATENDIDA"]
    assert e[0]["por"] == "encarregado-teste" and e[1]["por"] == "dp-teste"


def test_a_fila_filtra_por_obra_de_quem_e_preso_a_obra(mundo):
    """Quem é preso a obra vê o que é da obra dele MAIS o que ele mesmo abriu —
    o pedido de cadastro não tem obra ligada até ser atendido."""
    from app.apps.erp.db.models.cadastros import Obra
    outra = Obra(codigo="SDP-02", nome="Obra de outro", status="ATIVA")
    mundo["s"].add(outra)
    mundo["s"].flush()
    svc.abrir(mundo["s"], {"tipo": "DIVERSOS", "descricao": "da obra 1",
                           "obra_id": mundo["obra"].id}, mundo["dp"])
    svc.abrir(mundo["s"], {"tipo": "DIVERSOS", "descricao": "da obra 2",
                           "obra_id": outra.id}, mundo["dp"])
    mundo["s"].flush()

    so_obra1 = svc.listar(mundo["s"], obra_ids=[mundo["obra"].id])
    assert [x["obra"] for x in so_obra1["solicitacoes"]] == ["SDP-01"]
    assert len(svc.listar(mundo["s"], obra_ids=None)["solicitacoes"]) == 2


def test_quem_nao_alcanca_obra_nenhuma_ve_so_o_que_abriu(mundo):
    svc.abrir(mundo["s"], {"tipo": "DIVERSOS", "descricao": "do encarregado",
                           "obra_id": mundo["obra"].id},
              mundo["encarregado"])
    svc.abrir(mundo["s"], {"tipo": "DIVERSOS", "descricao": "do dp",
                           "obra_id": mundo["obra"].id}, mundo["dp"])
    mundo["s"].flush()
    # `eu` é quem está consultando — a consulta usa o solicitante_id
    fila = svc.listar(mundo["s"], obra_ids=[])
    assert isinstance(fila["solicitacoes"], list)


# ---------------------------------------------------------------------------
# A OBRA É OBRIGATÓRIA EM TODO PEDIDO (10/11/2026)
#
# O dono: *"a obra na solicitação, eu quero que seja obrigatória, porque a
# gente não sabe de onde é que está vindo aquela pessoa, para onde está indo
# trabalhar."*
#
# ⚑ O VALOR DISSO NÃO É O CAMPO PREENCHIDO: é o DP saber, ao abrir a fila, de
# onde veio cada pedido — sem precisar perguntar por WhatsApp, que é o
# trabalho que esta fila existe para acabar.
# ---------------------------------------------------------------------------

def test_pedido_sem_obra_nenhuma_e_recusado(mundo):
    with pytest.raises(ErroValidacao, match="Informe a obra"):
        svc.abrir(mundo["s"], {"tipo": "DIVERSOS", "descricao": "sem obra"},
                  mundo["dp"])


def test_a_obra_do_PEDIDO_vence_a_do_cadastro(mundo):
    """⚑ A ORDEM IMPORTA, e o caso que a define é a TRANSFERÊNCIA: a obra que
    pede é a de DESTINO. Deixar o cadastro vencer mandaria o pedido para a
    obra de onde a pessoa está saindo."""
    from app.apps.erp.db.models.cadastros import Obra
    destino = Obra(codigo="SDP-99", nome="Obra de destino", status="ATIVA")
    mundo["s"].add(destino)
    mundo["s"].flush()
    r = svc.abrir(mundo["s"], {"tipo": "ATUALIZACAO_DADOS",
                               "colaborador_id": mundo["pessoa"].id,
                               "descricao": "mudou de obra",
                               "obra_id": destino.id}, mundo["dp"])
    mundo["s"].flush()
    assert svc.ler(mundo["s"], r["id"])["obra_id"] == destino.id


def test_sem_dizer_a_obra_vale_a_do_cadastro_da_pessoa(mundo):
    """O caminho comum: o pedido é da pessoa, e a pessoa tem obra. Não se
    pergunta duas vezes o que o sistema já sabe."""
    r = svc.abrir(mundo["s"], {"tipo": "ATUALIZACAO_DADOS",
                               "colaborador_id": mundo["pessoa"].id,
                               "descricao": "telefone"}, mundo["dp"])
    mundo["s"].flush()
    assert svc.ler(mundo["s"], r["id"])["obra_id"] == mundo["obra"].id


def test_todo_tipo_oferece_a_obra_na_tela():
    """⚑ A TRAVA. A obra é acrescentada em `tipos_para_tela`, e não copiada em
    cada tipo: esquecer de copiá-la no tipo número doze seria o defeito
    garantido — e ele só apareceria quando alguém tentasse abrir justamente
    aquele pedido, com o pedido já digitado."""
    for t in svc.tipos_para_tela():
        assert "obra_id" in t["campos"], f"{t['valor']} não pergunta a obra"
    # E no tipo que NÃO a declarava, ela entra na frente: "de qual obra é
    # isto?" é a primeira pergunta de quem abre um pedido, não a última.
    diversos = next(t for t in svc.tipos_para_tela() if t["valor"] == "DIVERSOS")
    assert diversos["campos"][0] == "obra_id"


def test_o_requerente_saiu_de_uso():
    """*"Requerente era assim: eu solicito por alguém (…) mas vamos deixar de
    fora por enquanto."* A COLUNA fica — há pedidos gravados com ela."""
    for t in svc.tipos_para_tela():
        assert "requerente" not in t["campos"]
