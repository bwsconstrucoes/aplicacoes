# -*- coding: utf-8 -*-
"""
O DIÁRIO DE OBRA (migração 092).

Quase tudo aqui precisa de BANCO DE VERDADE, e não é preguiça: o valor deste
módulo está no que ele BUSCA — o efetivo do ponto, o material de suprimentos,
o feriado, o relato de ontem. Tudo isso vive em `WHERE` e `JOIN`, que é
exatamente o que a sessão dublada da suíte ignora.
"""
from __future__ import annotations

from datetime import date, timedelta

import pytest
from sqlalchemy import text

from app.apps.erp.core import diario as svc
from app.apps.erp.core.comum.auditoria import ErroNaoEncontrado, ErroValidacao

banco_ = pytest.mark.banco


# ---------------------------------------------------------------------------
# SEM BANCO — o contrato do módulo
# ---------------------------------------------------------------------------
def test_a_observacao_da_fiscalizacao_nao_esta_entre_os_campos_da_obra():
    """ESTE É O TESTE MAIS IMPORTANTE DO ARQUIVO.

    O que dá valor jurídico ao diário é a parte que a obra NÃO controla. Se
    `obs_fiscalizacao` entrar em `CAMPOS_DA_OBRA`, a obra passa a poder
    escrever — e reescrever — a observação da fiscalização, e o documento
    inteiro deixa de provar qualquer coisa.
    """
    assert "obs_fiscalizacao" not in svc.CAMPOS_DA_OBRA
    assert "obs_fiscalizacao_por" not in svc.CAMPOS_DA_OBRA


def test_todo_tempo_tem_rotulo_em_portugues():
    for t in svc.TEMPOS:
        assert svc.TEMPO_ROTULOS.get(t), f"{t} sem rótulo de gente"


# ---------------------------------------------------------------------------
# COM BANCO
# ---------------------------------------------------------------------------
@pytest.fixture
def dono(sessao_real):
    """Quem lança o diário: o ADMINISTRATIVO da obra. Decisão dele em
    08/11/2026 — *"quem lança realmente é o administrativo da obra, não é
    encarregado, não."*"""
    from app.apps.erp.core.auth.service import gerar_hash
    from app.apps.erp.db.models.cadastros import PerfilUsuario, Usuario
    u = Usuario(nome="Administrativo da obra", email="adm@diario.teste",
                ativo=True, senha_hash=gerar_hash("senha-de-teste-123"),
                perfil=PerfilUsuario.ADMINISTRATIVO_OBRA,
                telefone="5585900000009")
    sessao_real.add(u)
    sessao_real.flush()
    return u


@pytest.fixture
def obra(sessao_real):
    oid = sessao_real.execute(text("""
        INSERT INTO obras (codigo, nome, status, objeto, contrato,
                           data_inicio, data_termino, responsavel_tecnico)
        VALUES ('DI-01', 'Obra do diário', 'ATIVA', 'Construção de teste',
                'CT-2026/001', :ini, :fim, 'Eng. Fulano de Tal')
        RETURNING id"""),
        {"ini": date(2026, 3, 1), "fim": date(2027, 3, 1)}).scalar()
    sessao_real.flush()
    return oid


@banco_
def test_o_cabecalho_conta_o_prazo_sozinho(sessao_real, obra):
    """Ninguém calcula dia decorrido na mão — e ninguém COPIA a conta para o
    diário, senão obra com aditivo passa a ter diário com o prazo velho."""
    cab = svc.cabecalho(sessao_real, obra, date(2026, 3, 10))
    assert cab["prazo"]["decorridos"] == 10
    assert cab["prazo"]["total"] == 366
    assert cab["prazo"]["faltam"] == (date(2027, 3, 1) - date(2026, 3, 10)).days
    assert cab["responsavel_tecnico"] == "Eng. Fulano de Tal"


@banco_
def test_obra_que_passou_do_prazo_mostra_numero_negativo(sessao_real, obra):
    """Zero esconderia o atraso. O diário tem de dizer em voz alta."""
    cab = svc.cabecalho(sessao_real, obra, date(2027, 4, 1))
    assert cab["prazo"]["faltam"] < 0


@banco_
def test_obra_que_nao_existe_responde_nao_encontrado(sessao_real):
    with pytest.raises(ErroNaoEncontrado):
        svc.cabecalho(sessao_real, 99999999, date.today())


@banco_
def test_abrir_o_dia_nao_grava_nada(sessao_real, obra):
    """Folhear os dias de uma obra não pode encher a tabela de diários vazios
    — senão o calendário de pendências passa a mentir."""
    dia = date(2026, 3, 10)
    svc.abrir_dia(sessao_real, obra, dia)
    svc.abrir_dia(sessao_real, obra, dia)
    n = sessao_real.execute(text(
        "SELECT count(*) FROM diarios_obra WHERE obra_id = :o"), {"o": obra}).scalar()
    assert n == 0


@banco_
def test_o_dia_com_expediente_exige_relato(sessao_real, obra, dono):
    """Linha vazia contaria como "preenchido" no calendário, e a pendência
    sumiria sem o trabalho ter sido feito."""
    with pytest.raises(ErroValidacao, match="relato"):
        svc.salvar(sessao_real, obra, date(2026, 3, 10),
                   {"tempo_manha": "BOM"}, dono)


@banco_
def test_dia_sem_expediente_nao_exige_relato(sessao_real, obra, dono):
    r = svc.salvar(sessao_real, obra, date(2026, 3, 7),
                   {"sem_expediente": True}, dono)
    assert r["criado"] is True


@banco_
def test_sabado_e_domingo_nascem_sem_expediente(sessao_real, obra):
    """*"Fim de semana e feriados, a menos que seja digitado alguma coisa."*"""
    sabado = date(2026, 3, 7)
    domingo = date(2026, 3, 8)
    assert sabado.weekday() == 5 and domingo.weekday() == 6
    for d in (sabado, domingo):
        sugere, motivo = svc.nasce_sem_expediente(sessao_real, obra, d)
        assert sugere is True
        assert motivo


@banco_
def test_dia_de_semana_nao_nasce_sem_expediente(sessao_real, obra):
    sugere, _ = svc.nasce_sem_expediente(sessao_real, obra, date(2026, 3, 10))
    assert sugere is False


@banco_
def test_tempo_inventado_e_recusado(sessao_real, obra, dono):
    with pytest.raises(ErroValidacao, match="Tempo"):
        svc.salvar(sessao_real, obra, date(2026, 3, 10),
                   {"relato": "x", "tempo_manha": "ENSOLARADISSIMO"}, dono)


@banco_
def test_a_obra_nao_escreve_na_observacao_da_fiscalizacao(sessao_real, obra, dono):
    """Mesmo mandando o campo no corpo do pedido, ele NÃO entra. A regra mora
    no código e não na tela — campo escondido se mostra com o navegador."""
    svc.salvar(sessao_real, obra, date(2026, 3, 10),
               {"relato": "Concretagem.",
                "obs_fiscalizacao": "A obra escrevendo como se fosse o fiscal"},
               dono)
    linha = sessao_real.execute(text(
        "SELECT obs_fiscalizacao FROM diarios_obra WHERE obra_id = :o"),
        {"o": obra}).mappings().first()
    assert linha["obs_fiscalizacao"] is None


@banco_
def test_a_fiscalizacao_escreve_so_no_campo_dela(sessao_real, obra, dono):
    dia = date(2026, 3, 10)
    svc.salvar(sessao_real, obra, dia, {"relato": "Concretagem."}, dono)
    svc.observar_como_fiscalizacao(sessao_real, obra, dia,
                                   "Acompanhei a concretagem.", dono)
    linha = sessao_real.execute(text(
        "SELECT relato, obs_fiscalizacao, obs_fiscalizacao_por"
        "  FROM diarios_obra WHERE obra_id = :o"), {"o": obra}).mappings().first()
    assert linha["relato"] == "Concretagem."          # intacto
    assert "Acompanhei" in linha["obs_fiscalizacao"]
    assert linha["obs_fiscalizacao_por"]


@banco_
def test_a_fiscalizacao_escreve_mesmo_com_o_diario_fechado(sessao_real, obra, dono):
    """Ela costuma visitar depois. Travá-la pelo fechamento da obra seria
    impedir justamente a parte independente do documento."""
    dia = date(2026, 3, 10)
    svc.salvar(sessao_real, obra, dia, {"relato": "Concretagem."}, dono)
    svc.fechar(sessao_real, obra, dia, dono)
    svc.observar_como_fiscalizacao(sessao_real, obra, dia, "Vistoria em 12/03.", dono)
    linha = sessao_real.execute(text(
        "SELECT obs_fiscalizacao FROM diarios_obra WHERE obra_id = :o"),
        {"o": obra}).mappings().first()
    assert "Vistoria" in linha["obs_fiscalizacao"]


@banco_
def test_corrigir_depois_de_fechado_exige_motivo(sessao_real, obra, dono):
    dia = date(2026, 3, 10)
    svc.salvar(sessao_real, obra, dia, {"relato": "Concretagem."}, dono)
    svc.fechar(sessao_real, obra, dia, dono)
    with pytest.raises(ErroValidacao, match="motivo"):
        svc.salvar(sessao_real, obra, dia, {"relato": "Outra coisa."}, dono)


@banco_
def test_a_correcao_guarda_o_que_estava_e_o_que_passou_a_estar(sessao_real,
                                                               obra, dono):
    """Corrigir pode; apagar o rastro, não. É o que faz o diário ser
    documento e não rascunho."""
    dia = date(2026, 3, 10)
    svc.salvar(sessao_real, obra, dia, {"relato": "Concretagem da laje."}, dono)
    svc.fechar(sessao_real, obra, dia, dono)
    svc.salvar(sessao_real, obra, dia, {"relato": "Concretagem do pilar."},
               dono, motivo="errei o elemento")
    c = sessao_real.execute(text("""
        SELECT campo, antes, depois, motivo FROM diario_correcoes
         ORDER BY id DESC LIMIT 1""")).mappings().first()
    assert c["campo"] == "relato"
    assert "laje" in c["antes"] and "pilar" in c["depois"]
    assert c["motivo"] == "errei o elemento"


@banco_
def test_nao_se_fecha_um_dia_sem_relato(sessao_real, obra, dono):
    dia = date(2026, 3, 10)
    sessao_real.execute(text(
        "INSERT INTO diarios_obra (obra_id, data) VALUES (:o, :d)"),
        {"o": obra, "d": dia})
    with pytest.raises(ErroValidacao, match="sem relato"):
        svc.fechar(sessao_real, obra, dia, dono)


@banco_
def test_um_diario_por_obra_por_dia(sessao_real, obra, dono):
    """A trava que impede dois administrativos abrirem o mesmo dia e um
    sobrescrever o outro."""
    dia = date(2026, 3, 10)
    a = svc.salvar(sessao_real, obra, dia, {"relato": "primeiro"}, dono)
    b = svc.salvar(sessao_real, obra, dia, {"relato": "segundo"}, dono)
    assert a["id"] == b["id"]
    assert b["criado"] is False


@banco_
def test_o_relato_de_ontem_volta_para_reaproveitar(sessao_real, obra, dono):
    """*"é muito comum o relato do dia anterior ser similar ao do dia
    seguinte (…) havia uma forma de facilitar esse resgate."*"""
    svc.salvar(sessao_real, obra, date(2026, 3, 10),
               {"relato": "Alvenaria do 2º pavimento."}, dono)
    d = svc.abrir_dia(sessao_real, obra, date(2026, 3, 11))
    assert "Alvenaria" in d["relato_anterior"]["relato"]
    assert d["relato_anterior"]["data"] == "2026-03-10"


@banco_
def test_o_resgate_pula_o_fim_de_semana_sozinho(sessao_real, obra, dono):
    """Sexta → segunda. Se parasse no sábado vazio, não traria nada."""
    svc.salvar(sessao_real, obra, date(2026, 3, 6), {"relato": "Fôrmas."}, dono)
    svc.salvar(sessao_real, obra, date(2026, 3, 7), {"sem_expediente": True}, dono)
    d = svc.abrir_dia(sessao_real, obra, date(2026, 3, 9))
    assert "Fôrmas" in d["relato_anterior"]["relato"]


@banco_
def test_o_relato_nunca_e_preenchido_sozinho(sessao_real, obra, dono):
    """Ele vem para a tela OFERECER, não para entrar. Preenchido sozinho, o
    diário vira cópia-cola e deixa de ser relato."""
    svc.salvar(sessao_real, obra, date(2026, 3, 10), {"relato": "Alvenaria."}, dono)
    d = svc.abrir_dia(sessao_real, obra, date(2026, 3, 11))
    assert d["diario"]["relato"] == ""


@banco_
def test_o_terceirizado_entra_pelo_efetivo_extra(sessao_real, obra, dono):
    """Ele trabalhou e não bate ponto aqui — é o único efetivo que o sistema
    não sabe sozinho."""
    dia = date(2026, 3, 10)
    svc.salvar(sessao_real, obra, dia, {"relato": "Pintura."}, dono)
    svc.acrescentar_extra(sessao_real, obra, dia,
                          {"descricao": "Equipe Silva Pinturas",
                           "funcao": "pintura", "quantidade": 3,
                           "empresa": "Silva Pinturas ME"}, dono)
    d = svc.abrir_dia(sessao_real, obra, dia)
    assert d["diario"]["extras"][0]["quantidade"] == 3
    assert d["diario"]["extras"][0]["descricao"] == "Equipe Silva Pinturas"


@banco_
def test_o_extra_sem_nome_e_recusado(sessao_real, obra, dono):
    dia = date(2026, 3, 10)
    svc.salvar(sessao_real, obra, dia, {"relato": "x"}, dono)
    with pytest.raises(ErroValidacao):
        svc.acrescentar_extra(sessao_real, obra, dia, {"quantidade": 2}, dono)


@banco_
def test_o_efetivo_nao_derruba_o_diario_quando_o_ponto_nao_esta_la(sessao_real,
                                                                   obra):
    """O schema do ponto nasce das migrações DELE, que são de outra área. Num
    banco sem elas, o diário tem de abrir com o efetivo vazio e um recado —
    nunca com erro na cara de quem está na obra."""
    e = svc.efetivo_do_ponto(sessao_real, obra, date(2026, 3, 10))
    assert e["total"] == 0
    assert isinstance(e["pessoas"], list)


@banco_
def test_o_dia_abre_inteiro_mesmo_sem_ponto_e_sem_suprimentos(sessao_real, obra):
    d = svc.abrir_dia(sessao_real, obra, date(2026, 3, 10))
    for chave in ("obra", "efetivo", "material", "relato_anterior", "tempos"):
        assert chave in d


# ---------------------------------------------------------------------------
# O CALENDÁRIO DE PENDÊNCIAS
# ---------------------------------------------------------------------------
@banco_
def test_o_calendario_marca_o_que_falta_e_o_que_nao_era_para_ter(sessao_real,
                                                                 obra, dono):
    svc.salvar(sessao_real, obra, date(2026, 3, 10), {"relato": "Concretagem."}, dono)
    c = svc.calendario(sessao_real, obra, 2026, 3)
    por_dia = {d["dia"]: d["estado"] for d in c["dias"]}
    assert por_dia[10] == "aberto"
    assert por_dia[7] == "descanso"      # sábado
    assert por_dia[8] == "descanso"      # domingo
    assert por_dia[11] == "falta"


@banco_
def test_fim_de_semana_nao_conta_como_pendencia(sessao_real, obra):
    """Se contasse, o calendário nasceria com 104 faltas por ano e ninguém
    olharia para ele — painel que grita sempre não avisa nunca."""
    c = svc.calendario(sessao_real, obra, 2026, 3)
    descanso = [d for d in c["dias"] if d["estado"] == "descanso"]
    assert len(descanso) == 9            # 5 sábados + 4 domingos em março/2026 (1 a 31)
    assert all(d["semana"] >= 5 for d in descanso)


@banco_
def test_o_dia_fora_do_prazo_da_obra_nao_e_pendencia(sessao_real, obra):
    """Obra que começou em março não deve diário de janeiro."""
    c = svc.calendario(sessao_real, obra, 2026, 1)
    assert all(d["estado"] == "fora" for d in c["dias"])
    assert c["faltam"] == 0


@banco_
def test_dia_que_ainda_nao_chegou_nao_e_pendencia(sessao_real, obra):
    futuro = date.today() + timedelta(days=400)
    c = svc.calendario(sessao_real, obra, futuro.year, futuro.month)
    assert c["faltam"] == 0


@banco_
def test_o_dia_fechado_aparece_diferente_do_aberto(sessao_real, obra, dono):
    dia = date(2026, 3, 10)
    svc.salvar(sessao_real, obra, dia, {"relato": "Concretagem."}, dono)
    svc.fechar(sessao_real, obra, dia, dono)
    c = svc.calendario(sessao_real, obra, 2026, 3)
    assert {d["dia"]: d["estado"] for d in c["dias"]}[10] == "fechado"


@banco_
def test_o_painel_diz_por_qual_obra_comecar(sessao_real, obra):
    """O calendário mostra uma obra; este diz por onde correr."""
    linhas = svc.pendencias_por_obra(sessao_real, obra_ids=[obra], dias=10)
    assert linhas and linhas[0]["obra_id"] == obra
    assert linhas[0]["faltam"] >= 0
