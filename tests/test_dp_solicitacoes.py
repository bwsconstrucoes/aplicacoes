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
from sqlalchemy import text

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
        "data_nascimento": "2020-05-01", "documentos": ["DOC-DEPENDENTE"],
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
        svc.abrir(mundo["s"], {"tipo": "CADASTRO", "tipo_documentacao": "Prestador de Serviço",
        "documentos": ["DOC-IDENTIDADE", "CPF-DOC", "COMPROVANTE-ENDERECO-PESSOA"],
        "pagamento": "Mensalista", "cargo_pleiteado_id": mundo["cargo"].id, "data_inicio": "2026-05-01", "nome": "José Repetido",
                               "cpf": CPF_A,
                               "cnpj_admissao": "00.079.526/0001-09 - MATRIZ/CE",
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
        "tipo": "CADASTRO", "tipo_documentacao": "Prestador de Serviço",
        "documentos": ["DOC-IDENTIDADE", "CPF-DOC", "COMPROVANTE-ENDERECO-PESSOA"],
        "pagamento": "Mensalista", "cargo_pleiteado_id": mundo["cargo"].id, "data_inicio": "2026-05-01", "nome": "Maria Nova De Teste", "cpf": CPF_B,
        "cnpj_admissao": "00.079.526/0001-09 - MATRIZ/CE",
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
        "data_nascimento": "2020-05-01",
        "documentos": ["DOC-DEPENDENTE"]}, mundo["encarregado"])
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


# ---------------------------------------------------------------------------
# OS PAPÉIS SÃO OBRIGATÓRIOS — e o pedido não sai sem eles (10/11/2026)
#
# ⚑ EU PROPUS O CONTRÁRIO, E ELE DERRUBOU COM UM ARGUMENTO QUE EU NÃO TINHA.
# Minha proposta era deixar o cadastro entrar incompleto, para a pessoa não
# ficar sem registro. Ele:
#
#   *"Se você permitir que entre funcionário sem os documentos, esse documento
#   para ele voltar e ser concluído é um Deus nos acuda. (…) Não se consegue
#   pagar o funcionário no SomaPay se não tiver CPF, não se consegue fazer o
#   cadastro do funcionário se não tiver comprovante de endereço e CPF."*
#
# Isso muda a conta inteira: deixar passar não evita o mal que eu queria
# evitar — a pessoa continua sem receber — e ainda cria um cadastro pela
# metade que ninguém fecha.
# ---------------------------------------------------------------------------
def _cadastro(mundo, **extra):
    base = {"tipo": "CADASTRO", "nome": "Novo De Teste", "cpf": CPF_B,
            "obra_id": mundo["obra"].id, "tipo_documentacao": "Prestador de Serviço",
            "pagamento": "Diarista", "cargo_pleiteado_id": mundo["cargo"].id,
            # Desde 11/11/2026 o cadastro também diz POR QUAL EMPRESA — a
            # readmissão já perguntava, e quem cria a pessoa é este aqui.
            "cnpj_admissao": "00.079.526/0001-09 - MATRIZ/CE",
            "data_inicio": "2026-05-01"}
    base.update(extra)
    return base


def test_cadastro_sem_os_papeis_NAO_SAI(mundo):
    with pytest.raises(ErroValidacao) as e:
        svc.abrir(mundo["s"], _cadastro(mundo), mundo["encarregado"])
    recado = str(e.value)
    assert "Falta anexar" in recado
    for papel in ("documento de identificação", "CPF", "comprovante de endereço"):
        assert papel in recado
    # O recado diz POR QUE, e não só "falta documento".
    assert "pagamento" in recado


def test_cadastro_com_os_tres_papeis_sai(mundo):
    p = svc.abrir(mundo["s"], _cadastro(mundo, documentos=[
        "DOC-IDENTIDADE", "CPF-DOC", "COMPROVANTE-ENDERECO-PESSOA"]),
        mundo["encarregado"])
    assert p["id"]


def test_faltando_UM_papel_o_recado_diz_qual(mundo):
    with pytest.raises(ErroValidacao) as e:
        svc.abrir(mundo["s"], _cadastro(mundo, documentos=[
            "DOC-IDENTIDADE", "CPF-DOC"]), mundo["encarregado"])
    recado = str(e.value)
    assert "comprovante de endereço" in recado
    # E NÃO repete o que já veio.
    assert "documento de identificação" not in recado


def test_escolher_CTPS_passa_a_exigir_a_carteira_e_o_PIS(mundo):
    """Ele: *"se for CTPS, os dados da carteira são obrigatórios, PIS
    também."* Prestador de serviço não tem carteira, e não se pede."""
    exige_prestador = svc.exigencias(
        "CADASTRO", {"tipo_documentacao": "Prestador de Serviço"})
    assert "num_ctps" not in exige_prestador["campos"]
    assert not any(d["codigo"].startswith("CTPS")
                   for d in exige_prestador["documentos"])

    exige_ctps = svc.exigencias("CADASTRO", {"tipo_documentacao": "CTPS"})
    assert "num_ctps" in exige_ctps["campos"] and "num_pis" in exige_ctps["campos"]
    assert {"CTPS-FOTO", "CTPS-INFORMACOES"} <= {
        d["codigo"] for d in exige_ctps["documentos"]}


def test_cadastro_CTPS_sem_a_carteira_nao_sai(mundo):
    with pytest.raises(ErroValidacao) as e:
        svc.abrir(mundo["s"], _cadastro(
            mundo, tipo_documentacao="CTPS",
            documentos=["DOC-IDENTIDADE", "CPF-DOC",
                        "COMPROVANTE-ENDERECO-PESSOA"]),
            mundo["encarregado"])
    assert "Nº CTPS" in str(e.value) or "CTPS" in str(e.value)


def test_cadastro_CTPS_completo_sai(mundo):
    p = svc.abrir(mundo["s"], _cadastro(
        mundo, tipo_documentacao="CTPS", num_ctps="123456", num_pis="12345678901",
        documentos=["DOC-IDENTIDADE", "CPF-DOC", "COMPROVANTE-ENDERECO-PESSOA",
                    "CTPS-FOTO", "CTPS-INFORMACOES"]), mundo["encarregado"])
    assert p["id"]


def test_a_tela_sabe_o_que_falta_ANTES_de_tentar_enviar(mundo):
    """⚑ É ISTO QUE IMPEDE O BLOQUEIO DE VIRAR UMA PAREDE: a lista existe
    desde o primeiro clique, e vai se marcando sozinha enquanto a pessoa
    anexa. Ninguém preenche vinte campos para descobrir no fim que faltava
    um papel."""
    exige = svc.exigencias("CADASTRO", {})
    assert [d["rotulo"] for d in exige["documentos"]] == [
        "documento de identificação (RG, CNH ou CTPS)", "CPF",
        "comprovante de endereço"]
    assert "cpf" in exige["campos"] and "nome" in exige["campos"]


def test_pedido_que_nao_exige_papel_nenhum_continua_simples(mundo):
    """A exigência é do CADASTRO, não da fila inteira. Atualizar um telefone
    não pede documento."""
    assert svc.exigencias("ATUALIZACAO_DADOS", {})["documentos"] == []
    p = svc.abrir(mundo["s"], {"tipo": "ATUALIZACAO_DADOS",
                               "colaborador_id": mundo["pessoa"].id,
                               "descricao": "trocou de telefone"},
                  mundo["encarregado"])
    assert p["id"]


# ---------------------------------------------------------------------------
# VÁRIOS DEPENDENTES NO MESMO PEDIDO (10/11/2026)
#
# Ele: *"dependente tem que ser um a um, não posso adicionar outro, não, para
# pedir mais de um."* Quem tem três filhos abria três pedidos.
# ---------------------------------------------------------------------------
def _dependente(mundo, **extra):
    base = {"tipo": "DEPENDENTE", "colaborador_id": mundo["pessoa"].id,
            "obra_id": mundo["obra"].id, "documentos": ["DOC-DEPENDENTE"]}
    base.update(extra)
    return base


def test_tres_dependentes_num_pedido_so_cadastram_os_tres(mundo):
    p = svc.abrir(mundo["s"], _dependente(mundo, dependentes=[
        {"nome": "Filho Um", "parentesco": "Filho",
         "data_nascimento": "2015-01-10"},
        {"nome": "Filha Dois", "parentesco": "Filho",
         "data_nascimento": "2018-06-20"},
        {"nome": "Esposa Tres", "parentesco": "Cônjuge",
         "data_nascimento": "1990-02-02"}]), mundo["encarregado"])
    svc.atender(mundo["s"], p["id"], {}, mundo["dp"])
    mundo["s"].flush()
    nomes = {d["nome"] for d in
             svc_pessoas.ficha(mundo["s"], mundo["pessoa"].id)["dependentes"]}
    assert nomes == {"Filho Um", "Filha Dois", "Esposa Tres"}
    # A trilha registra quantos entraram — é o que o DP lê depois.
    eventos = svc.ler(mundo["s"], p["id"])["eventos"]
    atendida = [e for e in eventos if e["acao"] == "ATENDIDA"][0]
    assert atendida["detalhe"]["dependentes"] == 3


def test_o_rotulo_do_pipe_vale_em_cada_dependente_da_lista(mundo):
    """"Cônjuge" é o que a pessoa escolhe; CONJUGE é o que fica gravado."""
    p = svc.abrir(mundo["s"], _dependente(mundo, dependentes=[
        {"nome": "Esposa De Teste", "parentesco": "Cônjuge",
         "data_nascimento": "1990-02-02"}]), mundo["encarregado"])
    svc.atender(mundo["s"], p["id"], {}, mundo["dp"])
    mundo["s"].flush()
    d = svc_pessoas.ficha(mundo["s"], mundo["pessoa"].id)["dependentes"][0]
    assert d["parentesco"] == "CONJUGE"


def test_dependente_incompleto_na_lista_diz_QUAL_deles(mundo):
    """Com três na tela, "falta a data de nascimento" não ajuda ninguém."""
    with pytest.raises(ErroValidacao) as e:
        svc.abrir(mundo["s"], _dependente(mundo, dependentes=[
            {"nome": "Filho Um", "parentesco": "Filho",
             "data_nascimento": "2015-01-10"},
            {"nome": "Filha Dois", "parentesco": "Filho"}]),
            mundo["encarregado"])
    assert "2º dependente" in str(e.value)
    assert "Data de Nascimento do Dependente" in str(e.value)


def test_um_dependente_solto_continua_funcionando(mundo):
    """⚑ O FORMATO ANTIGO NÃO MORRE: é o que os pedidos JÁ ABERTOS têm
    gravado, e eles ainda vão ser atendidos."""
    p = svc.abrir(mundo["s"], _dependente(
        mundo, nome="Filho Sozinho", parentesco="Filho",
        data_nascimento="2015-01-10"), mundo["encarregado"])
    svc.atender(mundo["s"], p["id"], {}, mundo["dp"])
    mundo["s"].flush()
    assert svc_pessoas.ficha(
        mundo["s"], mundo["pessoa"].id)["dependentes"][0]["nome"] == "Filho Sozinho"


def test_so_o_dependente_aceita_varios(mundo):
    """`repete` é do tipo, não da fila: ninguém desliga três pessoas num
    pedido só."""
    assert svc.TIPOS["DEPENDENTE"]["repete"] is True
    for codigo, d in svc.TIPOS.items():
        if codigo != "DEPENDENTE":
            assert not d.get("repete"), f"{codigo} não deveria aceitar vários"


def test_a_tela_do_DP_recebe_rotulo_e_opcoes_dos_campos_dela():
    """⚑ OS CAMPOS DO DP TÊM DE TER ONDE APARECER. Tirá-los do formulário da
    obra sem pô-los na tela de quem atende seria perder a informação, não
    corrigir o lugar dela. Ele, em 10/11/2026: *"quem vai olhar isso aqui é o
    DP."*"""
    for t in svc.tipos_para_tela():
        for c in t["campos_do_dp"]:
            rot = t["rotulos_do_dp"].get(c)
            assert rot and rot != c, (
                f"{t['valor']}: o campo do DP {c!r} não tem rótulo de gente — "
                f"ele apareceria com o nome do banco na tela de quem atende")
            # Campo de lista fechada leva as opções junto; senão virava texto
            # livre justamente onde o dado tem de ser exato.
            if c in svc.CATEGORIAS:
                assert t["opcoes_do_dp"].get(c), f"{c} sem opções"


def test_nenhum_campo_do_DP_ficou_orfao():
    """Campo que saiu do formulário da obra e não entrou em `campos_do_dp`
    sumiu do sistema — é o jeito silencioso de perder dado."""
    saiu_da_obra = {"ultimo_dia", "faltas_a_descontar", "matricula",
                    "convencao_id", "dependente_ir"}
    tem_dono = set()
    for d in svc.TIPOS.values():
        tem_dono |= set(d.get("campos_do_dp", ()))
    assert saiu_da_obra <= tem_dono, (
        f"sem dono: {saiu_da_obra - tem_dono}")


def test_no_dependente_o_nome_e_do_DEPENDENTE_e_nao_do_colaborador():
    """⚑ A LISTAGEM DOS TIPOS REVELOU ISTO. O rótulo geral de `nome` é "Nome
    do Colaborador", e no pedido de dependente isso faria a tela pedir duas
    vezes o nome de quem já foi escolhido na busca. O que se pede ali é o
    nome do FILHO."""
    d = next(t for t in svc.tipos_para_tela() if t["valor"] == "DEPENDENTE")
    assert d["rotulos"]["nome"] == "Nome do Dependente"
    assert d["rotulos_do_dp"]["cpf"] == "CPF do Dependente"
    # E no cadastro ele continua sendo o do colaborador.
    c = next(t for t in svc.tipos_para_tela() if t["valor"] == "CADASTRO")
    assert c["rotulos"]["nome"] == "Nome do Colaborador"


def test_o_numero_da_carteira_sai_do_documento_nos_DOIS_pedidos_que_o_pedem():
    """Ele: *"isso aí a gente já vai ler os documentos."* Vale no cadastro e
    na admissão de prestador — os dois exigem a foto da carteira."""
    for codigo in ("CADASTRO", "ADMISSAO_PRESTADOR"):
        t = next(x for x in svc.tipos_para_tela() if x["valor"] == codigo)
        assert t["vem_do_documento"].get("num_ctps") == "CTPS-INFORMACOES"
        assert t["vem_do_documento"].get("num_pis") == "CTPS-INFORMACOES"


def test_campo_que_vem_do_documento_exige_o_documento_que_o_traz():
    """Prometer que o campo "sai do documento" sem exigir aquele documento
    seria uma promessa que o pedido não cumpre."""
    for t in svc.tipos_para_tela():
        exigidos = set(t["documentos"])
        for mapa in (svc.TIPOS[t["valor"]].get("documentos_se") or {}).values():
            for papeis in mapa.values():
                exigidos |= set(papeis)
        for campo, papel in t["vem_do_documento"].items():
            assert papel in exigidos, (
                f"{t['valor']}: {campo} diz sair de {papel}, e esse papel não "
                f"é exigido em lugar nenhum do pedido")


def test_o_cartao_do_transporte_nao_aparece_para_quem_pede():
    """No Pipe ele existe na FASE do cadastro (o DP marca), não no formulário.
    Oferecer no pedido o que lá só o DP escolhe é recriar a escolha que foi
    tirada de quem abre."""
    assert "Cartão" not in svc.CATEGORIAS["categoria_transporte"]


# ---------------------------------------------------------------------------
# AS TRÊS CORREÇÕES DE 10/11/2026 (depois de ele ler a lista dos tipos)
# ---------------------------------------------------------------------------
def test_o_desligamento_nao_pede_mais_a_chave_pix():
    """Ele: *"desligamento de colaborador, não precisa de chave Pix mais."*

    Ela existe no Pipe (para as verbas), mas o ERP já tem a chave no cadastro
    da pessoa. Pedir de novo abre a porta para digitarem uma chave diferente
    da cadastrada bem na hora de pagar a rescisão."""
    campos = svc.TIPOS["DESLIGAMENTO"]["campos"]
    assert "pix_tipo" not in campos and "pix_chave" not in campos


def test_a_atualizacao_de_dados_tem_o_endereco():
    """Ele: *"dados cadastrais pode ser telefone, chave Pix, endereço, né?
    Acho que faltou endereço aí."*

    ⚑ E a explicação do tipo JÁ PROMETIA endereço — é a mesma classe de
    defeito da porta do portal que anunciava férias e não tinha."""
    campos = set(svc.TIPOS["ATUALIZACAO_DADOS"]["campos"])
    assert {"cep", "logradouro", "numero_endereco", "bairro", "municipio",
            "uf"} <= campos
    # O que a explicação promete tem de existir.
    for palavra, campo in (("telefone", "telefone"), ("endereço", "logradouro"),
                           ("Pix", "pix_chave")):
        assert palavra.lower() in svc.TIPOS["ATUALIZACAO_DADOS"]["explicacao"].lower()
        assert campo in campos


def test_nao_se_pede_a_CTPS_de_quem_ja_tem_no_cadastro(sessao_real, mundo):
    """Ele: *"vamos requerer os dados. Se não tivermos, né? Acho que é
    interessante uma consulta, ver se tem CTPS, se tem PIS."*

    ⚑ PEDIR O QUE O SISTEMA JÁ SABE é o defeito mais caro de um formulário:
    cansa quem preenche e abre a porta para digitarem um número diferente do
    cadastrado."""
    sessao_real.execute(text(
        "UPDATE colaboradores SET ctps_numero = '998877', pis = '12345678901'"
        " WHERE id = :i"), {"i": mundo["pessoa"].id})
    sessao_real.flush()
    ja = svc.o_que_o_cadastro_ja_tem(sessao_real, "ADMISSAO_PRESTADOR",
                                     mundo["pessoa"].id)
    assert ja == {"num_ctps": "998877", "num_pis": "12345678901"}
    # Com os dois no cadastro, o pedido não os exige mais.
    exige = svc.exigencias("ADMISSAO_PRESTADOR", {}, ja)
    assert "num_ctps" not in exige["campos"] and "num_pis" not in exige["campos"]

    p = svc.abrir(sessao_real, {
        "tipo": "ADMISSAO_PRESTADOR", "colaborador_id": mundo["pessoa"].id,
        "obra_id": mundo["obra"].id, "salario_familia": "Não",
        "documentos": ["CTPS-FOTO", "CTPS-INFORMACOES"]}, mundo["encarregado"])
    # ⚑ E O NÚMERO ENTRA NO PEDIDO: o DP vê sem abrir a ficha ao lado.
    assert p["dados"]["num_ctps"] == "998877"


def test_quem_NAO_tem_no_cadastro_continua_sendo_perguntado(sessao_real, mundo):
    """A consulta tira a pergunta; ela não tira a exigência."""
    sessao_real.execute(text(
        "UPDATE colaboradores SET ctps_numero = NULL, pis = NULL WHERE id = :i"),
        {"i": mundo["pessoa"].id})
    sessao_real.flush()
    assert svc.o_que_o_cadastro_ja_tem(
        sessao_real, "ADMISSAO_PRESTADOR", mundo["pessoa"].id) == {}
    with pytest.raises(ErroValidacao) as e:
        svc.abrir(sessao_real, {
            "tipo": "ADMISSAO_PRESTADOR", "colaborador_id": mundo["pessoa"].id,
            "obra_id": mundo["obra"].id, "salario_familia": "Não",
            "documentos": ["CTPS-FOTO", "CTPS-INFORMACOES"]},
            mundo["encarregado"])
    assert "Nº CTPS" in str(e.value)


def test_so_metade_no_cadastro_pergunta_so_a_outra_metade(sessao_real, mundo):
    sessao_real.execute(text(
        "UPDATE colaboradores SET ctps_numero = '554433', pis = NULL"
        " WHERE id = :i"), {"i": mundo["pessoa"].id})
    sessao_real.flush()
    with pytest.raises(ErroValidacao) as e:
        svc.abrir(sessao_real, {
            "tipo": "ADMISSAO_PRESTADOR", "colaborador_id": mundo["pessoa"].id,
            "obra_id": mundo["obra"].id, "salario_familia": "Não",
            "documentos": ["CTPS-FOTO", "CTPS-INFORMACOES"]},
            mundo["encarregado"])
    recado = str(e.value)
    assert "Nº PIS" in recado and "Nº CTPS" not in recado


def test_a_consulta_ao_cadastro_so_vale_onde_foi_declarada():
    """`ja_no_cadastro` é do tipo. Um pedido que não declarou nada continua
    pedindo tudo — não há adivinhação de coluna."""
    assert svc.o_que_o_cadastro_ja_tem(None, "DIVERSOS", 1) == {}
    assert svc.o_que_o_cadastro_ja_tem(None, "ADMISSAO_PRESTADOR", None) == {}


def test_a_atualizacao_de_dados_grava_o_endereco_no_cadastro(mundo):
    """Campo na tela que não chega ao cadastro é pior que campo nenhum: quem
    preencheu acha que mudou, e o DP continua com o endereço velho."""
    p = svc.abrir(mundo["s"], {
        "tipo": "ATUALIZACAO_DADOS", "colaborador_id": mundo["pessoa"].id,
        "obra_id": mundo["obra"].id,
        "descricao": "mudou de casa", "cep": "60000000",
        "logradouro": "Rua Nova", "numero_endereco": "123",
        "bairro": "Centro", "municipio": "Fortaleza", "uf": "CE"},
        mundo["encarregado"])
    svc.atender(mundo["s"], p["id"], {}, mundo["dp"])
    mundo["s"].flush()
    c = mundo["pessoa"]
    mundo["s"].refresh(c)
    assert (c.cep, c.logradouro, c.numero_endereco) == ("60000000", "Rua Nova", "123")
    assert (c.bairro, c.municipio, c.uf) == ("Centro", "Fortaleza", "CE")


def test_o_nome_so_muda_quando_o_pedido_traz_um_nome_novo(mundo):
    """O corpo do pedido NÃO pode devolver o nome antigo por cima do atual —
    por isso o campo tem nome próprio (`nome_novo`)."""
    p = svc.abrir(mundo["s"], {
        "tipo": "ATUALIZACAO_DADOS", "colaborador_id": mundo["pessoa"].id,
        "obra_id": mundo["obra"].id,
        "descricao": "mudou o telefone", "telefone": "5585999990000"},
        mundo["encarregado"])
    svc.atender(mundo["s"], p["id"], {}, mundo["dp"])
    mundo["s"].flush()
    mundo["s"].refresh(mundo["pessoa"])
    assert mundo["pessoa"].nome == "José Da Fila"

    p2 = svc.abrir(mundo["s"], {
        "tipo": "ATUALIZACAO_DADOS", "colaborador_id": mundo["pessoa"].id,
        "obra_id": mundo["obra"].id,
        "descricao": "casou", "nome_novo": "José Da Fila Rocha"},
        mundo["encarregado"])
    svc.atender(mundo["s"], p2["id"], {}, mundo["dp"])
    mundo["s"].flush()
    mundo["s"].refresh(mundo["pessoa"])
    assert mundo["pessoa"].nome == "José Da Fila Rocha"


def test_o_cpf_nunca_muda_por_atualizacao_de_dados(mundo):
    """O CPF é a identidade da pessoa no sistema — e é por ele que o pagamento
    acha a pessoa no SomaPay."""
    p = svc.abrir(mundo["s"], {
        "tipo": "ATUALIZACAO_DADOS", "colaborador_id": mundo["pessoa"].id,
        "obra_id": mundo["obra"].id,
        "descricao": "tentando trocar o CPF"},
        mundo["encarregado"])
    # Mesmo o DP mandando outro CPF no atendimento, vale o da pessoa: trocar
    # o CPF aqui criaria um segundo cadastro, não corrigiria o primeiro.
    svc.atender(mundo["s"], p["id"], {"dados": {"cpf": CPF_B}}, mundo["dp"])
    mundo["s"].flush()
    mundo["s"].refresh(mundo["pessoa"])
    assert mundo["pessoa"].cpf == CPF_A


def test_todo_tipo_que_ADMITE_alguem_pergunta_por_qual_empresa(mundo):
    """⚑ A pergunta dele em 11/11/2026: *"na readmissão ou no cadastro de
    colaborador pergunta qual empresa?"* A readmissão perguntava; o cadastro,
    que é justamente quem CRIA a pessoa, não.

    Esta trava é por CLASSE, não por tipo: todo pedido que registra alguém
    numa empresa tem de dizer em qual. Tipo novo do mesmo feitio nasce com a
    pergunta, ou quebra aqui."""
    ADMITEM = ("CADASTRO", "READMISSAO")
    for chave in ADMITEM:
        regra = svc.TIPOS[chave]
        assert "cnpj_admissao" in regra["campos"], (
            f"{chave} registra alguém numa empresa e não pergunta qual")
        assert "cnpj_admissao" in regra["obrigatorios"], (
            f"{chave} pergunta a empresa mas deixa passar em branco — o DP "
            f"ficaria sem saber por qual das três registrar")


def test_a_empresa_e_perguntada_UMA_vez_so(mundo):
    """No Pipe há "Empresa para Registro" E "CNPJ para Admissão". As três
    empresas SÃO os três CNPJs: perguntar as duas coisas é a duplicidade que
    ele já derrubou no diário ("ou é um ou é o outro")."""
    for chave, regra in svc.TIPOS.items():
        campos = set(regra["campos"]) | set(regra.get("campos_do_dp", ()))
        assert not ("cnpj_admissao" in campos and "empresa_id" in campos), (
            f"{chave} pergunta a empresa duas vezes")


def test_o_cadastro_sem_a_empresa_e_recusado(mundo):
    with pytest.raises(ErroValidacao) as e:
        svc.abrir(mundo["s"], {
            "tipo": "CADASTRO", "nome": "Maria Sem Empresa", "cpf": CPF_B,
            "tipo_documentacao": "CTPS", "data_inicio": "2026-05-01",
            "cargo_pleiteado_id": mundo["cargo"].id, "pagamento": "Mensalista",
            "obra_id": mundo["obra"].id,
            "documentos": ["DOC-IDENTIDADE", "CPF-DOC",
                           "COMPROVANTE-ENDERECO-PESSOA", "CTPS-FOTO",
                           "CTPS-INFORMACOES"]}, mundo["encarregado"])
    assert "CNPJ para Admissão" in str(e.value)
