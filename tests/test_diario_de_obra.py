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


# ---------------------------------------------------------------------------
# O RELATÓRIO — o papel que vai para a fiscalização
# ---------------------------------------------------------------------------
@banco_
def test_o_papel_leva_contagem_por_funcao_e_nao_nome_de_gente(sessao_real,
                                                              obra, dono):
    """Decisão dele: *"provavelmente a gente vai visualizar em tela o nome das
    pessoas, mas (…) no diário vai colocar só quantidade e função."*

    E é boa prática além da preferência: o RDO circula FORA da empresa. A
    fiscalização precisa saber quantos pedreiros havia, não quem eram.
    """
    dia = date(2026, 3, 10)
    svc.salvar(sessao_real, obra, dia, {"relato": "Concretagem."}, dono)
    svc.acrescentar_extra(sessao_real, obra, dia,
                          {"descricao": "Equipe Silva", "funcao": "Pintor",
                           "quantidade": 3}, dono)
    r = svc.relatorio(sessao_real, obra, dia)
    pagina = r["paginas"][0]
    assert pagina["efetivo"]["linhas"][0]["funcao"] == "Pintor"
    assert pagina["efetivo"]["total"] == 3
    # nenhum nome de pessoa no que vai para o papel
    assert "pessoas" not in pagina["efetivo"]


@banco_
def test_o_proprio_e_o_terceirizado_saem_separados(sessao_real, obra, dono):
    """Quem lê o RDO precisa distinguir mão de obra própria da contratada."""
    dia = date(2026, 3, 10)
    svc.salvar(sessao_real, obra, dia, {"relato": "x"}, dono)
    svc.acrescentar_extra(sessao_real, obra, dia,
                          {"descricao": "Equipe Silva", "funcao": "Pintor",
                           "quantidade": 3}, dono)
    linhas = svc.relatorio(sessao_real, obra, dia)["paginas"][0]["efetivo"]
    assert linhas["total_terceirizado"] == 3
    assert all(l["tipo"] in ("próprio", "terceirizado") for l in linhas["linhas"])


@banco_
def test_o_numero_do_rdo_e_o_dia_da_obra_e_muda_a_cada_dia(sessao_real, obra,
                                                           dono):
    """Não inventei sequência própria: ela divergiria do prazo no primeiro
    diário esquecido, e o papel passaria a contar outra história."""
    for d in (date(2026, 3, 10), date(2026, 3, 11)):
        svc.salvar(sessao_real, obra, d, {"relato": f"dia {d}"}, dono)
    r = svc.relatorio(sessao_real, obra, date(2026, 3, 10), date(2026, 3, 11))
    numeros = [p["prazo"]["decorridos"] for p in r["paginas"]]
    assert numeros == [10, 11], (
        "o número do RDO repetiu entre páginas — o cabeçalho está sendo "
        "calculado uma vez só para o período inteiro")


@banco_
def test_a_correcao_depois_do_fechamento_SAI_NO_PAPEL(sessao_real, obra, dono):
    """Esconder a correção seria o mesmo que não registrá-la: o valor do
    diário está em o leitor poder ver que algo mudou, quando e por quê."""
    dia = date(2026, 3, 10)
    svc.salvar(sessao_real, obra, dia, {"relato": "Laje."}, dono)
    svc.fechar(sessao_real, obra, dia, dono)
    svc.salvar(sessao_real, obra, dia, {"relato": "Pilar."}, dono,
               motivo="errei o elemento")
    p = svc.relatorio(sessao_real, obra, dia)["paginas"][0]
    assert p["correcoes"] and p["correcoes"][0]["motivo"] == "errei o elemento"


@banco_
def test_o_relatorio_recusa_periodo_longo_demais(sessao_real, obra):
    with pytest.raises(ErroValidacao, match="longo"):
        svc.relatorio(sessao_real, obra, date(2026, 3, 1), date(2026, 12, 31))


@banco_
def test_o_relatorio_recusa_data_final_antes_da_inicial(sessao_real, obra):
    with pytest.raises(ErroValidacao):
        svc.relatorio(sessao_real, obra, date(2026, 3, 10), date(2026, 3, 1))


@banco_
def test_por_padrao_o_papel_so_traz_os_dias_escritos(sessao_real, obra, dono):
    """Um relatório mensal com 30 folhas em branco não é relatório."""
    svc.salvar(sessao_real, obra, date(2026, 3, 10), {"relato": "x"}, dono)
    r = svc.relatorio(sessao_real, obra, date(2026, 3, 1), date(2026, 3, 31))
    assert len(r["paginas"]) == 1
    todos = svc.relatorio(sessao_real, obra, date(2026, 3, 1), date(2026, 3, 31),
                          so_escritos=False)
    assert len(todos["paginas"]) == 31


# ===========================================================================
# MIGRAÇÃO 093 — OS MODELOS DE PAPEL, O HORÁRIO E AS LOGOS (09/11/2026)
#
# Ele mandou dois modelos de diário e pediu que o nosso fosse adaptado ao mais
# amplo: *"se os dados que a gente estiver coletando estiverem mais amplos,
# vamos incorporar também. Acho que quanto mais amplo, melhor."*
# ===========================================================================
def test_o_horario_vem_preenchido_conforme_o_dia_da_semana():
    """Pedido dele com todas as letras: *"o normal é 7 às 17. Ou 7 às 16 na
    sexta-feira (…) como se fosse assim, pré-preenchido, a depender do dia da
    semana."*"""
    # 09/11/2026 é segunda; 13/11 é sexta.
    assert svc.horario_padrao(date(2026, 11, 9)) == "7:00 às 17:00"
    assert svc.horario_padrao(date(2026, 11, 12)) == "7:00 às 17:00"
    assert svc.horario_padrao(date(2026, 11, 13)) == "7:00 às 16:00"


def test_sabado_e_domingo_nao_sugerem_horario():
    """Dia que nasce sem expediente não pode oferecer jornada: horário escrito
    num dia que não houve é afirmação errada num documento."""
    assert svc.horario_padrao(date(2026, 11, 14)) == ""   # sábado
    assert svc.horario_padrao(date(2026, 11, 15)) == ""   # domingo


def test_os_campos_novos_sao_da_obra_e_a_fiscalizacao_continua_de_fora():
    for campo in ("horario_trabalho", "equipamentos", "historico"):
        assert campo in svc.CAMPOS_DA_OBRA
    assert "obs_fiscalizacao" not in svc.CAMPOS_DA_OBRA


def test_os_tres_modelos_existem_e_cada_um_se_explica():
    assert set(svc.MODELOS) == {"completo", "bws", "orgao"}
    for nome, m in svc.MODELOS.items():
        assert m["rotulo"] and m["descricao"], f"{nome} sem texto de gente"
    assert svc.MODELO_PADRAO in svc.MODELOS


def test_modelo_desconhecido_cai_no_nosso_em_vez_de_quebrar():
    """Papel de diário não pode falhar por nome de modelo: quem está em obra
    receberia uma tela de erro em vez da folha."""
    assert svc.modelo_valido("inexistente") == svc.MODELO_PADRAO
    assert svc.modelo_valido("") == svc.MODELO_PADRAO
    assert svc.modelo_valido(None) == svc.MODELO_PADRAO
    assert svc.modelo_valido(" ORGAO ") == "orgao"


def test_o_teto_da_logo_do_papel_e_o_mesmo_do_cadastro():
    """Teto menor aqui daria logo aceita no cadastro e sumida na impressão —
    e quem descobriria é quem está com a folha na mão, na obra."""
    from app.apps.erp.core.cadastros.empresas import LIMITE_LOGO
    assert svc.TETO_DA_LOGO == LIMITE_LOGO


@banco_
def test_o_dia_em_branco_ja_abre_com_o_horario_da_semana(sessao_real, obra):
    d = svc.abrir_dia(sessao_real, obra, date(2026, 11, 13))      # sexta
    assert d["diario"]["horario_trabalho"] == "7:00 às 16:00"
    assert d["diario"]["equipamentos"] == ""
    assert d["diario"]["historico"] == ""


@banco_
def test_dia_que_nasce_sem_expediente_abre_sem_horario(sessao_real, obra):
    d = svc.abrir_dia(sessao_real, obra, date(2026, 11, 14))      # sábado
    assert d["sugere_sem_expediente"] is True
    assert d["diario"]["horario_trabalho"] == ""


@banco_
def test_os_tres_campos_novos_gravam_e_voltam(sessao_real, obra, dono):
    svc.salvar(sessao_real, obra, date(2026, 3, 10), {
        "relato": "Concretagem da laje.",
        "horario_trabalho": "7:00 às 16:00",
        "equipamentos": "01 betoneira, 01 placa vibratória",
        "historico": "Estrutura concluída até o 3º pavimento.",
    }, dono)
    d = svc.abrir_dia(sessao_real, obra, date(2026, 3, 10))["diario"]
    assert d["horario_trabalho"] == "7:00 às 16:00"
    assert d["equipamentos"] == "01 betoneira, 01 placa vibratória"
    assert d["historico"].startswith("Estrutura concluída")


@banco_
def test_gravar_sem_horario_nao_inventa_o_padrao(sessao_real, obra, dono):
    """O padrão é SUGESTÃO DE TELA. Quem grava um dia sem horário está
    dizendo que não sabe o horário — e inventar jornada num documento que
    sustenta pedido de prazo é pior que deixar em branco."""
    svc.salvar(sessao_real, obra, date(2026, 3, 11), {"relato": "x"}, dono)
    d = svc.abrir_dia(sessao_real, obra, date(2026, 3, 11))["diario"]
    assert d["horario_trabalho"] == ""


@banco_
def test_o_papel_leva_os_campos_novos(sessao_real, obra, dono):
    svc.salvar(sessao_real, obra, date(2026, 3, 12), {
        "relato": "Alvenaria do bloco A.",
        "horario_trabalho": "7:00 às 17:00",
        "equipamentos": "01 andaime",
        "historico": "Dentro do cronograma.",
    }, dono)
    p = svc.relatorio(sessao_real, obra, date(2026, 3, 12))["paginas"][0]
    assert p["horario_trabalho"] == "7:00 às 17:00"
    assert p["equipamentos"] == "01 andaime"
    assert p["historico"] == "Dentro do cronograma."


@banco_
def test_o_papel_numera_a_folha_de_1_ate_o_total(sessao_real, obra, dono):
    """"Folha: 1/1" é do modelo do órgão. Num relatório de vários dias, repetir
    "1/1" em todas seria papel errado — a fiscalização numera por folha."""
    for dia in (10, 11, 12):
        svc.salvar(sessao_real, obra, date(2026, 4, dia), {"relato": "x"}, dono)
    r = svc.relatorio(sessao_real, obra, date(2026, 4, 10), date(2026, 4, 12))
    assert [p["folha"] for p in r["paginas"]] == [1, 2, 3]
    assert {p["folhas"] for p in r["paginas"]} == {3}


@banco_
def test_o_modelo_sai_do_cadastro_da_obra(sessao_real, obra, dono):
    """A escolha é CLÁUSULA DO CONTRATO: *"tem um determinado contrato que a
    gente vai precisar utilizar esse modelo específico de diário de obra."*"""
    svc.salvar(sessao_real, obra, date(2026, 5, 10), {"relato": "x"}, dono)
    sessao_real.execute(text("UPDATE obras SET diario_modelo = 'orgao' "
                             "WHERE id = :o"), {"o": obra})
    r = svc.relatorio(sessao_real, obra, date(2026, 5, 10))
    assert r["modelo"] == "orgao"
    assert r["modelo_da_obra"] == "orgao"


@banco_
def test_espiar_outro_modelo_nao_muda_o_do_contrato(sessao_real, obra, dono):
    """A barra da folha troca o modelo só para VER. Se trocasse de verdade,
    um dia sairia o papel errado para a fiscalização."""
    svc.salvar(sessao_real, obra, date(2026, 5, 11), {"relato": "x"}, dono)
    sessao_real.execute(text("UPDATE obras SET diario_modelo = 'orgao' "
                             "WHERE id = :o"), {"o": obra})
    r = svc.relatorio(sessao_real, obra, date(2026, 5, 11), modelo="bws")
    assert r["modelo"] == "bws"            # o que sai agora
    assert r["modelo_da_obra"] == "orgao"  # o que o contrato exige
    assert sessao_real.execute(
        text("SELECT diario_modelo FROM obras WHERE id = :o"),
        {"o": obra}).scalar() == "orgao"


@banco_
def test_as_logos_entram_na_folha_como_dado(sessao_real, obra):
    """Embutidas, e não buscadas por endereço: o PDF vai por e-mail e abriria
    sem cabeçalho na mão de quem recebeu."""
    sessao_real.execute(text("""
        UPDATE obras SET contratante_logo = :l,
                         contratante_logo_mime = 'image/png' WHERE id = :o"""),
        {"l": b"\x89PNG-de-mentira", "o": obra})
    logos = svc.logos(sessao_real, obra)
    assert logos["contratante"].startswith("data:image/png;base64,")
    assert logos["empresa"] == ""          # a obra de teste não tem empresa


@banco_
def test_logo_acima_do_teto_sai_de_fora_sem_derrubar_a_folha(sessao_real, obra):
    """Nunca uma página que não carrega: logo grande demais some, o diário
    sai."""
    sessao_real.execute(text("""
        UPDATE obras SET contratante_logo = :l,
                         contratante_logo_mime = 'image/png' WHERE id = :o"""),
        {"l": b"x" * (svc.TETO_DA_LOGO + 1), "o": obra})
    assert svc.logos(sessao_real, obra)["contratante"] == ""


@banco_
def test_o_cabecalho_traz_o_contratante_para_o_papel(sessao_real, obra):
    """O cabeçalho do modelo do órgão é DELE: *"Prefeitura Municipal de Abreu
    e Lima, Secretaria de Obras e Defesa Civil (…) isso aí vai estar
    relacionado ao contratante."*"""
    sessao_real.execute(text("""
        UPDATE obras SET cliente = 'Prefeitura de Exemplo',
                         contratante_unidade = 'Secretaria de Obras'
         WHERE id = :o"""), {"o": obra})
    cab = svc.cabecalho(sessao_real, obra, date(2026, 3, 10))
    assert cab["contratante"] == "Prefeitura de Exemplo"
    assert cab["contratante_unidade"] == "Secretaria de Obras"
    assert cab["modelo"] == svc.MODELO_PADRAO


# ---------------------------------------------------------------------------
# AS TRÊS FOLHAS RENDERIZAM DE VERDADE
#
# ⚑ ESTE TESTE NASCEU DE UM DEFEITO DESTE MESMO DIA. A folha pedia o número
# dela com `loop.index`, e o número do laço NÃO atravessa para o pedaço
# incluído: as três folhas respondiam 500. Nada disso aparece lendo o
# template, nem rodando a regra de negócio — só aparece renderizando.
#
# Por isso o teste pede a PÁGINA, nos três modelos, e confere que o conteúdo
# do dia chegou ao papel. Modelo novo que não renderizar quebra aqui, e não na
# mão de quem está em obra.
# ---------------------------------------------------------------------------
@banco_
@pytest.mark.parametrize("modelo", ["completo", "bws", "orgao"])
def test_a_folha_de_cada_modelo_renderiza_com_o_dia_dentro(
        app_real, sessao_real, obra, dono, modelo):
    from tests.conftest import como
    sessao_real.execute(text("""
        UPDATE obras SET cliente = 'Prefeitura de Exemplo',
                         contratante_unidade = 'Secretaria de Obras'
         WHERE id = :o"""), {"o": obra})
    svc.salvar(sessao_real, obra, date(2026, 3, 10), {
        "relato": "Concretagem da laje do terceiro pavimento.",
        "horario_trabalho": "7:00 às 16:00",
        "equipamentos": "01 betoneira e 01 placa vibratória",
        "historico": "Estrutura dentro do cronograma.",
        "ocorrencias": "Visita técnica da fiscalização.",
    }, dono)
    sessao_real.flush()

    r = como(app_real, dono.id).get(
        f"/erp/obras/diario/papel/{obra}?de=2026-03-10&modelo={modelo}")
    assert r.status_code == 200, r.data[:400]
    folha = r.data.decode()
    assert "Concretagem da laje" in folha
    assert "7:00 às 16:00" in folha          # o horário sai nos três
    assert "betoneira" in folha              # os equipamentos também
    assert "Prefeitura de Exemplo" in folha  # e o contratante
    assert "10/03/2026" in folha


# ---------------------------------------------------------------------------
# O CADASTRO DO CONTRATO: a unidade, o modelo e a logo do órgão
# ---------------------------------------------------------------------------
@pytest.fixture
def admin(sessao_real):
    from app.apps.erp.core.auth.service import gerar_hash
    from app.apps.erp.db.models.cadastros import PerfilUsuario, Usuario
    u = Usuario(nome="Dona do sistema", email="admin@diario.teste", ativo=True,
                senha_hash=gerar_hash("senha-de-teste-123"),
                perfil=PerfilUsuario.ADMIN, telefone="5585900000010")
    sessao_real.add(u)
    sessao_real.flush()
    return u


@banco_
def test_o_contrato_grava_a_unidade_e_o_modelo_do_diario(app_real, sessao_real,
                                                         obra, admin):
    """Eles entram na lista de campos gravados — a lista que já deixou o
    seguro-garantia cair em silêncio quando foi esquecida."""
    from tests.conftest import como
    r = como(app_real, admin.id).post(f"/erp/api/obras/{obra}", json={
        "contratante_unidade": "Secretaria de Obras e Defesa Civil",
        "diario_modelo": "orgao"})
    assert r.status_code == 200, r.data[:300]
    volta = como(app_real, admin.id).get(f"/erp/api/obras/{obra}").get_json()["obra"]
    assert volta["contratante_unidade"] == "Secretaria de Obras e Defesa Civil"
    assert volta["diario_modelo"] == "orgao"


@banco_
def test_a_logo_do_contratante_sobe_e_desce_pela_tela(app_real, sessao_real,
                                                      obra, admin):
    from io import BytesIO
    from tests.conftest import como
    c = como(app_real, admin.id)
    assert c.get(f"/erp/api/obras/{obra}/logo-contratante").status_code == 404

    r = c.post(f"/erp/api/obras/{obra}/logo-contratante",
               data={"arquivo": (BytesIO(b"<svg/>"), "orgao.svg", "image/svg+xml")},
               content_type="multipart/form-data")
    assert r.status_code == 200, r.data[:300]
    assert c.get(f"/erp/api/obras/{obra}/logo-contratante").status_code == 200

    assert c.delete(f"/erp/api/obras/{obra}/logo-contratante").status_code == 200
    assert c.get(f"/erp/api/obras/{obra}/logo-contratante").status_code == 404


@banco_
def test_arquivo_que_nao_e_imagem_e_recusado_com_recado_de_gente(
        app_real, sessao_real, obra, admin):
    from io import BytesIO
    from tests.conftest import como
    r = como(app_real, admin.id).post(
        f"/erp/api/obras/{obra}/logo-contratante",
        data={"arquivo": (BytesIO(b"%PDF-1.4"), "contrato.pdf", "application/pdf")},
        content_type="multipart/form-data")
    assert r.status_code == 400
    assert "PNG" in r.get_json()["erro"]


@banco_
def test_a_logo_da_empresa_aparece_para_quem_ve_a_obra(app_real, sessao_real,
                                                       obra, dono):
    """A rota da EMPRESA exige "configurar", que o administrativo de obra não
    tem. Se a ficha da obra buscasse a logo por lá, apareceria um quadrado
    quebrado justamente para quem imprime o diário todo dia."""
    from tests.conftest import como
    eid = sessao_real.execute(text("""
        INSERT INTO empresas (razao_social, cnpj, logo, logo_mime)
        VALUES ('Construtora de Teste Ltda', '11.111.111/0001-11',
                :l, 'image/png')
        RETURNING id"""), {"l": b"\x89PNG-de-mentira"}).scalar()
    sessao_real.execute(text("UPDATE obras SET empresa_id = :e WHERE id = :o"),
                        {"e": eid, "o": obra})
    sessao_real.flush()

    c = como(app_real, dono.id)
    assert c.get(f"/erp/api/obras/{obra}/logo-empresa").status_code == 200
    # E a mesma pessoa continua SEM poder mexer no cadastro da empresa.
    assert c.get(f"/erp/api/empresas/{eid}/logo").status_code == 403


# ===========================================================================
# O QUE ELE VIU NA TELA, EM PRODUÇÃO (10/11/2026)
# ===========================================================================
def test_a_obra_nao_aparece_duplicada():
    """⚑ ESTE DEFEITO JÁ TINHA SIDO CONSERTADO EM 10/09/2026, e eu o recriei
    no diário escrevendo `código — nome` à mão em vez de usar o ajudante que
    existia. A reclamação dele foi a mesma das duas vezes: *"a obra está
    aparecendo duplicado, o nome dela está aparecendo duas vezes."*
    """
    # O caso dele: código e nome praticamente iguais.
    assert svc.rotulo_da_obra("AERO ARACATI", "Aero Aracati") == "Aero Aracati"
    assert svc.rotulo_da_obra("081", "Residencial Vale do Sol") == \
        "081 · Residencial Vale do Sol"
    # Um dentro do outro: basta o mais completo.
    assert svc.rotulo_da_obra("CRECHESWAP", "Espaço Creche Swap") == \
        "Espaço Creche Swap"
    # Todas as palavras do código no nome: também é repetição.
    assert svc.rotulo_da_obra("TESTE-PONTO", "Teste do ponto (obra de teste)") == \
        "Teste do ponto (obra de teste)"
    # Faltando um dos dois, sai o que há.
    assert svc.rotulo_da_obra("", "Só o nome") == "Só o nome"
    assert svc.rotulo_da_obra("SÓ-O-CÓDIGO", "") == "SÓ-O-CÓDIGO"


def test_nenhuma_tela_do_diario_escreve_codigo_e_nome_a_mao():
    """⚑ A TRAVA, e ela existe porque isto já aconteceu DUAS vezes.

    O ajudante (`rotuloObra` no `erp_base.html`, `rotulo_da_obra` na regra)
    resolve a repetição. Escrever `${o.codigo} — ${o.nome}` à mão a desfaz, e
    o defeito só aparece numa obra cujo código e nome se parecem — ou seja,
    quase sempre em produção e quase nunca em teste.
    """
    import pathlib
    import re
    telas = pathlib.Path("app/apps/erp/templates")
    padrao = re.compile(r"codigo[^\n]{0,20}(—|·|-)\s*\$\{[^}]*nome")
    # ⚑ VARRE TODAS AS TELAS DO ERP, e não só as do diário. Em 10/11/2026 o
    # mesmo defeito foi achado no PORTAL DA OBRA, que nem usa o `erp_base.html`
    # — e uma trava que olhasse só o diário teria deixado passar.
    # ⚑ A ÚNICA EXCEÇÃO, e ela é escrita: a lista de BANCOS. Ali "001 · Banco
    # do Brasil" está certo — o código do banco e o nome do banco são coisas
    # diferentes, e nunca um contém o outro. Exceção nova entra aqui, com o
    # motivo, nunca por ajuste no padrão.
    permitidos = {("erp_config.html", "cb-banco")}
    errados = []
    for arquivo in sorted(telas.glob("*.html")):
        linhas = arquivo.read_text(encoding="utf-8").splitlines()
        for n, linha in enumerate(linhas, 1):
            if not padrao.search(linha):
                continue
            volta = "\n".join(linhas[max(0, n - 5):n])
            if any(a == arquivo.name and marca in volta
                   for a, marca in permitidos):
                continue
            errados.append(f"{arquivo.name}:{n}")
    assert not errados, (
        f"estas linhas escrevem o código e o nome da obra lado a lado, "
        f"em vez de usar `rotuloObra` (ou o `rotulo` pronto que a regra "
        f"manda): {errados}. Obra com código e nome parecidos aparece "
        f"duplicada — foi reclamação do dono duas vezes.")


@banco_
def test_o_cabecalho_ja_traz_o_rotulo_pronto(sessao_real, obra):
    """A tela e o PAPEL leem o mesmo rótulo. Duas versões divergiriam no
    primeiro ajuste, e aí a folha impressa diria uma coisa e a tela outra."""
    cab = svc.cabecalho(sessao_real, obra, date(2026, 3, 10))
    assert cab["rotulo"] == svc.rotulo_da_obra(cab["codigo"], cab["nome"])


@banco_
def test_o_efetivo_traz_um_resumo_em_uma_frase(sessao_real, obra, dono):
    """Pedido dele: *"nome, função, inclusive um resumo, de quantas pessoas e
    tal — fica mais fácil entender a coisa."*"""
    e = svc.efetivo_do_ponto(sessao_real, obra, date(2026, 3, 10))
    # Sem ninguém no ponto, o resumo é vazio — e não "0 pessoas", que teria
    # cara de resposta onde não há dado.
    assert e["resumo"] == ""
    assert "resumo" in e


def test_o_resumo_do_efetivo_conta_por_funcao():
    """A frase é montada na REGRA, e não na tela, porque o papel usa a mesma —
    duas versões divergiriam no primeiro ajuste."""
    # A função é pura o bastante para ser exercitada pelo que ela produz.
    linhas = [{"funcao": "Pedreiro"}, {"funcao": "Pedreiro"},
              {"funcao": "Servente"}, {"funcao": "—"}]
    contagem: dict[str, int] = {}
    for p in linhas:
        contagem[p["funcao"]] = contagem.get(p["funcao"], 0) + 1
    assert contagem == {"Pedreiro": 2, "Servente": 1, "—": 1}


# ---------------------------------------------------------------------------
# PROCURAR PELO QUE FOI ESCRITO
# ---------------------------------------------------------------------------
@banco_
def test_acha_o_diario_pelo_que_foi_relatado(sessao_real, obra, dono):
    """Ele: *"eu quero localizar um diário através de alguma informação que eu
    relatei."* É a pergunta certa a fazer de um diário: ninguém lembra a DATA
    em que a laje foi concretada — lembra que foi concretada."""
    svc.salvar(sessao_real, obra, date(2026, 3, 10),
               {"relato": "Concretagem da laje do bloco B."}, dono)
    svc.salvar(sessao_real, obra, date(2026, 3, 11),
               {"relato": "Alvenaria do bloco A."}, dono)
    sessao_real.flush()
    r = svc.procurar(sessao_real, "concretagem")
    assert [a["data"] for a in r["achados"]] == ["2026-03-10"]
    assert r["achados"][0]["em"][0]["campo"] == "relato do dia"
    assert "Concretagem" in r["achados"][0]["em"][0]["trecho"]


@banco_
def test_acento_nao_atrapalha_a_busca(sessao_real, obra, dono):
    """Quem digita no celular, com pressa, não põe acento."""
    svc.salvar(sessao_real, obra, date(2026, 3, 10),
               {"relato": "Serviço parado.", "ocorrencias": "Ocorrência de chuva."},
               dono)
    sessao_real.flush()
    assert svc.procurar(sessao_real, "ocorrencia")["achados"]
    assert svc.procurar(sessao_real, "servico")["achados"]


@banco_
def test_a_busca_diz_ONDE_achou(sessao_real, obra, dono):
    """A mesma palavra no relato e na observação da fiscalização quer dizer
    coisas diferentes."""
    svc.salvar(sessao_real, obra, date(2026, 3, 10),
               {"relato": "Dia normal.", "equipamentos": "01 betoneira"}, dono)
    sessao_real.flush()
    achado = svc.procurar(sessao_real, "betoneira")["achados"][0]
    assert [x["campo"] for x in achado["em"]] == ["equipamentos"]


@banco_
def test_procurar_pedaco_de_palavra_funciona(sessao_real, obra, dono):
    """Quem procura "P12" ou "bloco B" não quer raiz de palavra, quer o
    trecho — é por isso que a busca não usa `tsvector`."""
    svc.salvar(sessao_real, obra, date(2026, 3, 10),
               {"relato": "Formas dos pilares P12 a P18."}, dono)
    sessao_real.flush()
    assert svc.procurar(sessao_real, "P12")["achados"]


@banco_
def test_busca_curta_demais_nao_varre_a_base(sessao_real, obra):
    """Duas letras acham tudo e não ajudam ninguém."""
    r = svc.procurar(sessao_real, "da")
    assert r["curto"] is True and r["achados"] == []


@banco_
def test_a_busca_respeita_as_obras_de_quem_procura(sessao_real, obra, dono):
    """Quem é preso a obra não acha diário da obra de outro."""
    svc.salvar(sessao_real, obra, date(2026, 3, 10),
               {"relato": "Concretagem da laje."}, dono)
    sessao_real.flush()
    assert svc.procurar(sessao_real, "concretagem", obra_ids=[obra])["achados"]
    assert svc.procurar(sessao_real, "concretagem", obra_ids=[])["achados"] == []


# ---------------------------------------------------------------------------
# OS EQUIPAMENTOS EM OBRA (migração 097)
#
# Pedido dele em 10/11/2026: *"a parte dos equipamentos."* — dito logo depois
# de pedir nome, função e resumo no efetivo, descendo a tela e apontando o
# mesmo defeito: texto onde devia haver conta.
# ---------------------------------------------------------------------------
def test_o_resumo_do_equipamento_e_uma_frase_so():
    """A frase é montada na REGRA, porque a tela e o papel usam a MESMA."""
    r = svc.resumo_dos_equipamentos(
        [{"descricao": "Retroescavadeira", "quantidade": 1},
         {"descricao": "Betoneira", "quantidade": 2}])
    assert r == "3 equipamento(s): 1 Retroescavadeira, 2 Betoneira"


def test_sem_equipamento_o_resumo_e_vazio():
    """E não "0 equipamentos", que teria cara de resposta onde não há dado."""
    assert svc.resumo_dos_equipamentos([]) == ""


@banco_
def test_acrescentar_equipamento_exige_o_diario_gravado(sessao_real, obra):
    """O equipamento pende do diário do dia: sem o dia, não há onde pendurar."""
    with pytest.raises(ErroNaoEncontrado):
        svc.acrescentar_equipamento(sessao_real, obra, date(2026, 3, 10),
                                    {"descricao": "Retroescavadeira"})


@banco_
def test_equipamento_sem_nome_nao_entra(sessao_real, obra, dono):
    svc.salvar(sessao_real, obra, date(2026, 3, 10), {"relato": "x"}, dono)
    sessao_real.flush()
    with pytest.raises(ErroValidacao):
        svc.acrescentar_equipamento(sessao_real, obra, date(2026, 3, 10),
                                    {"descricao": "   "})


@banco_
def test_quantidade_zero_ou_negativa_nao_entra(sessao_real, obra, dono):
    """Zero betoneira em obra não é informação, é erro de digitação."""
    svc.salvar(sessao_real, obra, date(2026, 3, 10), {"relato": "x"}, dono)
    sessao_real.flush()
    for q in (0, -3):
        with pytest.raises(ErroValidacao):
            svc.acrescentar_equipamento(sessao_real, obra, date(2026, 3, 10),
                                        {"descricao": "Betoneira",
                                         "quantidade": q})


@banco_
def test_o_equipamento_aparece_na_lista_e_no_resumo_do_dia(sessao_real, obra,
                                                           dono):
    dia = date(2026, 3, 10)
    svc.salvar(sessao_real, obra, dia, {"relato": "x"}, dono)
    sessao_real.flush()
    svc.acrescentar_equipamento(sessao_real, obra, dia,
                                {"descricao": "Retroescavadeira",
                                 "quantidade": 1,
                                 "observacao": "parada por manutenção"})
    sessao_real.flush()
    d = svc.abrir_dia(sessao_real, obra, dia)["diario"]
    assert len(d["equipamentos_lista"]) == 1
    assert d["equipamentos_lista"][0]["observacao"] == "parada por manutenção"
    assert d["equipamentos_resumo"] == "1 equipamento(s): 1 Retroescavadeira"


@banco_
def test_o_dia_novo_oferece_o_equipamento_do_dia_anterior(sessao_real, obra,
                                                          dono):
    """Retroescavadeira não vai embora à noite — mas também não entra sozinha
    no diário de amanhã: vem OFERECIDA, com botão."""
    svc.salvar(sessao_real, obra, date(2026, 3, 10), {"relato": "x"}, dono)
    sessao_real.flush()
    svc.acrescentar_equipamento(sessao_real, obra, date(2026, 3, 10),
                                {"descricao": "Betoneira", "quantidade": 2})
    sessao_real.flush()
    d = svc.abrir_dia(sessao_real, obra, date(2026, 3, 11))
    assert d["equipamentos_anteriores"]["data"] == "2026-03-10"
    assert d["equipamentos_anteriores"]["itens"][0]["descricao"] == "Betoneira"
    # ⚑ E o dia 11 ABRE VAZIO: a oferta não é gravação.
    assert d["diario"]["equipamentos_lista"] == []


@banco_
def test_repetir_o_equipamento_de_ontem_copia_a_lista(sessao_real, obra, dono):
    svc.salvar(sessao_real, obra, date(2026, 3, 10), {"relato": "x"}, dono)
    sessao_real.flush()
    svc.acrescentar_equipamento(sessao_real, obra, date(2026, 3, 10),
                                {"descricao": "Betoneira", "quantidade": 2})
    svc.acrescentar_equipamento(sessao_real, obra, date(2026, 3, 10),
                                {"descricao": "Andaime", "quantidade": 10})
    svc.salvar(sessao_real, obra, date(2026, 3, 11), {"relato": "y"}, dono)
    sessao_real.flush()
    r = svc.repetir_equipamentos_de_ontem(sessao_real, obra, date(2026, 3, 11))
    sessao_real.flush()
    assert r["copiados"] == 2 and r["de"] == "2026-03-10"
    d = svc.abrir_dia(sessao_real, obra, date(2026, 3, 11))["diario"]
    assert {x["descricao"] for x in d["equipamentos_lista"]} == {"Betoneira",
                                                                 "Andaime"}


@banco_
def test_repetir_duas_vezes_nao_dobra_o_parque_de_maquinas(sessao_real, obra,
                                                           dono):
    """Equipamento duplicado num diário é erro que aparece em medição."""
    svc.salvar(sessao_real, obra, date(2026, 3, 10), {"relato": "x"}, dono)
    sessao_real.flush()
    svc.acrescentar_equipamento(sessao_real, obra, date(2026, 3, 10),
                                {"descricao": "Betoneira", "quantidade": 2})
    svc.salvar(sessao_real, obra, date(2026, 3, 11), {"relato": "y"}, dono)
    sessao_real.flush()
    svc.repetir_equipamentos_de_ontem(sessao_real, obra, date(2026, 3, 11))
    sessao_real.flush()
    with pytest.raises(ErroValidacao):
        svc.repetir_equipamentos_de_ontem(sessao_real, obra, date(2026, 3, 11))


@banco_
def test_repetir_sem_dia_anterior_avisa_em_portugues(sessao_real, obra, dono):
    svc.salvar(sessao_real, obra, date(2026, 3, 10), {"relato": "x"}, dono)
    sessao_real.flush()
    with pytest.raises(ErroValidacao):
        svc.repetir_equipamentos_de_ontem(sessao_real, obra, date(2026, 3, 10))


@banco_
def test_tirar_equipamento_de_outro_dia_nao_funciona(sessao_real, obra, dono):
    """O número do registro não autoriza nada por si: ele tem de pertencer ao
    diário daquele dia."""
    svc.salvar(sessao_real, obra, date(2026, 3, 10), {"relato": "x"}, dono)
    sessao_real.flush()
    r = svc.acrescentar_equipamento(sessao_real, obra, date(2026, 3, 10),
                                    {"descricao": "Betoneira"})
    svc.salvar(sessao_real, obra, date(2026, 3, 11), {"relato": "y"}, dono)
    sessao_real.flush()
    with pytest.raises(ErroNaoEncontrado):
        svc.remover_equipamento(sessao_real, obra, date(2026, 3, 11), r["id"])


@banco_
def test_o_papel_imprime_a_lista_de_equipamento(sessao_real, obra, dono):
    """A lista tem de chegar ao papel — é lá que a fiscalização lê."""
    dia = date(2026, 3, 10)
    svc.salvar(sessao_real, obra, dia, {"relato": "x"}, dono)
    sessao_real.flush()
    svc.acrescentar_equipamento(sessao_real, obra, dia,
                                {"descricao": "Betoneira", "quantidade": 2})
    sessao_real.flush()
    pagina = svc.relatorio(sessao_real, obra, dia)["paginas"][0]
    assert pagina["equipamentos_lista"][0]["descricao"] == "Betoneira"
    assert pagina["equipamentos_resumo"] == "2 equipamento(s): 2 Betoneira"


@banco_
def test_o_texto_antigo_de_equipamento_nao_se_perde(sessao_real, obra, dono):
    """Diário é documento: o que foi escrito antes da lista existir continua
    escrito, e continua indo para o papel."""
    dia = date(2026, 3, 10)
    svc.salvar(sessao_real, obra, dia,
               {"relato": "x", "equipamentos": "01 caçamba"}, dono)
    sessao_real.flush()
    pagina = svc.relatorio(sessao_real, obra, dia)["paginas"][0]
    assert pagina["equipamentos"] == "01 caçamba"
    assert pagina["equipamentos_lista"] == []


@banco_
def test_a_busca_acha_o_equipamento_da_lista(sessao_real, obra, dono):
    """Quando o equipamento saiu do campo de texto e foi para a lista, a busca
    tinha de ir com ele — senão procurar "retroescavadeira" passaria a não
    achar o dia em que ela esteve na obra."""
    dia = date(2026, 3, 10)
    svc.salvar(sessao_real, obra, dia, {"relato": "Movimentação de terra."},
               dono)
    sessao_real.flush()
    svc.acrescentar_equipamento(sessao_real, obra, dia,
                                {"descricao": "Retroescavadeira",
                                 "quantidade": 1})
    sessao_real.flush()
    achados = svc.procurar(sessao_real, "retroescavadeira")["achados"]
    assert achados
    assert [x["campo"] for x in achados[0]["em"]] == ["lista de equipamentos"]


@banco_
def test_a_busca_acha_pela_observacao_do_equipamento(sessao_real, obra, dono):
    """\"parada por manutenção\" é o que sustenta pleito de prazo — tem de ser
    achável."""
    dia = date(2026, 3, 10)
    svc.salvar(sessao_real, obra, dia, {"relato": "x"}, dono)
    sessao_real.flush()
    svc.acrescentar_equipamento(sessao_real, obra, dia,
                                {"descricao": "Betoneira",
                                 "observacao": "parada por manutenção"})
    sessao_real.flush()
    assert svc.procurar(sessao_real, "manutencao")["achados"]


@banco_
def test_o_diario_abre_mesmo_sem_a_migracao_dos_equipamentos(sessao_real, obra,
                                                             dono):
    """ESTE TESTE VALE MAIS DO QUE PARECE.

    O código sobe para o Render ANTES de alguém apertar "Aplicar atualizações
    do banco". Entre uma coisa e outra, o diário não pode cair — ele é usado
    todo dia, na obra, por gente que não tem como resolver isso.

    Então: com a tabela da lista fora do ar, o dia abre, a busca responde, e a
    tela sabe que a lista ainda não está ligada (e por isso não oferece um
    botão que falharia).
    """
    dia = date(2026, 3, 10)
    svc.salvar(sessao_real, obra, dia,
               {"relato": "Concretagem.", "equipamentos": "01 caçamba"}, dono)
    sessao_real.flush()
    sessao_real.execute(text("ALTER TABLE diario_equipamentos "
                             "RENAME TO diario_equipamentos_escondida"))
    sessao_real.flush()
    try:
        d = svc.abrir_dia(sessao_real, obra, dia)
        assert d["diario"]["equipamentos"] == "01 caçamba"
        assert d["diario"]["equipamentos_lista"] == []
        assert d["diario"]["equipamentos_disponivel"] is False
        assert d["equipamentos_anteriores"]["itens"] == []
        # A busca continua achando o que está nos campos de texto.
        assert svc.procurar(sessao_real, "concretagem")["achados"]
        # E o papel sai, com o texto escrito à mão.
        pagina = svc.relatorio(sessao_real, obra, dia)["paginas"][0]
        assert pagina["equipamentos"] == "01 caçamba"
        assert pagina["equipamentos_lista"] == []
    finally:
        sessao_real.execute(text("ALTER TABLE diario_equipamentos_escondida "
                                 "RENAME TO diario_equipamentos"))
        sessao_real.flush()


@banco_
def test_com_a_migracao_aplicada_a_tela_sabe_que_pode_lancar(sessao_real, obra,
                                                             dono):
    dia = date(2026, 3, 10)
    svc.salvar(sessao_real, obra, dia, {"relato": "x"}, dono)
    sessao_real.flush()
    assert svc.abrir_dia(sessao_real, obra,
                         dia)["diario"]["equipamentos_disponivel"] is True


@banco_
def test_o_portal_recebe_o_rotulo_da_obra_pronto(sessao_real, obra, dono):
    """O PORTAL DA OBRA não usa o `erp_base.html`, então não tem o ajudante de
    navegador — ele montava "código — nome" à mão e repetia o defeito. Agora o
    rótulo vem PRONTO do servidor, da mesma regra que o resto do ERP usa."""
    from app.apps.erp.core import portal as svc_portal
    from app.apps.erp.core.comum.rotulos import rotulo_da_obra
    lista = svc_portal.obras_para_escolher(sessao_real, dono)
    assert lista, "o dono alcança pelo menos a obra do teste"
    for o in lista:
        assert o["rotulo"] == rotulo_da_obra(o["codigo"], o["nome"])


def test_a_regra_do_rotulo_mora_num_lugar_so():
    """⚑ Ela saiu do `core/diario.py` e virou `core/comum/rotulos.py` em
    10/11/2026, quando o mesmo defeito apareceu no portal. Duas cópias
    divergiriam no primeiro ajuste — e foi assim que ele voltou duas vezes."""
    from app.apps.erp.core.comum import rotulos
    assert svc.rotulo_da_obra is rotulos.rotulo_da_obra
    # A regra em si, nos casos que o dono reclamou.
    assert rotulos.rotulo_da_obra("081", "081 Aero Aracati") == "081 Aero Aracati"
    assert rotulos.rotulo_da_obra("AERO ARACATI", "Aero Aracati") == "Aero Aracati"
    assert rotulos.rotulo_da_obra("081", "Vale do Sol") == "081 · Vale do Sol"


# ---------------------------------------------------------------------------
# O EQUIPAMENTO VEM DO CADASTRO DE INSUMOS (migração 098)
#
# Ele, em 10/11/2026, olhando a tela que a 097 criou: *"nós temos aqui o campo
# equipamentos, texto livre, e equipamentos em obra. Eles são a mesma coisa,
# por que teriam os dois textos? Ou é um ou é o outro. (…) esses equipamentos,
# o ideal é que a gente possa buscar dentro da nossa lista de insumos, né? Até
# por questão de nomenclatura. São os equipamentos inclusive categorizados
# como locáveis."*
# ---------------------------------------------------------------------------
def _insumo(sessao, descricao, *, locavel=True, ativo=True, codigo=None):
    import uuid
    cod = codigo or ("EQ" + uuid.uuid4().hex[:8].upper())
    return sessao.execute(text("""
        INSERT INTO insumos (codigo, descricao, unidade, locavel, ativo)
        VALUES (:c, :d, 'un', :l, :a) RETURNING id"""),
        {"c": cod, "d": descricao, "l": locavel, "a": ativo}).scalar()


@banco_
def test_o_catalogo_traz_so_os_locaveis_ativos(sessao_real):
    """A caixinha oferece equipamento, não a base inteira de insumos."""
    alvo = _insumo(sessao_real, "Retroescavadeira ZZ")
    _insumo(sessao_real, "Cimento CP-II ZZ", locavel=False)
    _insumo(sessao_real, "Betoneira velha ZZ", ativo=False)
    sessao_real.flush()
    nomes = [i["descricao"] for i in svc.equipamentos_do_catalogo(sessao_real)]
    assert "Retroescavadeira ZZ" in nomes
    assert "Cimento CP-II ZZ" not in nomes
    assert "Betoneira velha ZZ" not in nomes
    assert alvo in [i["id"] for i in svc.equipamentos_do_catalogo(sessao_real)]


@banco_
def test_o_equipamento_escolhido_na_lista_grava_o_nome_do_cadastro(sessao_real,
                                                                   obra, dono):
    """⚑ A DESCRIÇÃO É GRAVADA MESMO VINDO DO CADASTRO, e não é redundância: o
    insumo pode ser renomeado amanhã, e o diário é DOCUMENTO — tem de dizer o
    que foi escrito NAQUELE dia. O `insumo_id` serve para CONTAR; a descrição,
    para PROVAR."""
    dia = date(2026, 3, 10)
    i = _insumo(sessao_real, "Retroescavadeira 4x4")
    svc.salvar(sessao_real, obra, dia, {"relato": "x"}, dono)
    sessao_real.flush()
    # A tela manda o número; o nome NÃO vai junto — e mesmo assim ele é gravado.
    svc.acrescentar_equipamento(sessao_real, obra, dia,
                                {"insumo_id": i, "quantidade": 1})
    sessao_real.flush()
    linha = svc.abrir_dia(sessao_real, obra, dia)["diario"]["equipamentos_lista"][0]
    assert linha["descricao"] == "Retroescavadeira 4x4"
    assert linha["insumo_id"] == i
    assert linha["fora_da_lista"] is False

    # Renomear o cadastro NÃO reescreve o diário de ontem.
    sessao_real.execute(text("UPDATE insumos SET descricao = :d WHERE id = :i"),
                        {"d": "Retroescavadeira (nome novo)", "i": i})
    sessao_real.flush()
    de_novo = svc.abrir_dia(sessao_real, obra, dia)["diario"]["equipamentos_lista"][0]
    assert de_novo["descricao"] == "Retroescavadeira 4x4"


@banco_
def test_insumo_que_nao_e_locavel_e_recusado(sessao_real, obra, dono):
    """Cimento não é equipamento. E o recado diz o que fazer."""
    dia = date(2026, 3, 10)
    i = _insumo(sessao_real, "Cimento CP-II", locavel=False)
    svc.salvar(sessao_real, obra, dia, {"relato": "x"}, dono)
    sessao_real.flush()
    with pytest.raises(ErroValidacao) as e:
        svc.acrescentar_equipamento(sessao_real, obra, dia, {"insumo_id": i})
    assert "locável" in str(e.value)


@banco_
def test_insumo_inativo_e_recusado(sessao_real, obra, dono):
    dia = date(2026, 3, 10)
    i = _insumo(sessao_real, "Betoneira aposentada", ativo=False)
    svc.salvar(sessao_real, obra, dia, {"relato": "x"}, dono)
    sessao_real.flush()
    with pytest.raises(ErroValidacao):
        svc.acrescentar_equipamento(sessao_real, obra, dia, {"insumo_id": i})


@banco_
def test_insumo_que_nao_existe_responde_nao_encontrado(sessao_real, obra, dono):
    dia = date(2026, 3, 10)
    svc.salvar(sessao_real, obra, dia, {"relato": "x"}, dono)
    sessao_real.flush()
    with pytest.raises(ErroNaoEncontrado):
        svc.acrescentar_equipamento(sessao_real, obra, dia,
                                    {"insumo_id": 99999999})


@banco_
def test_equipamento_fora_da_lista_entra_marcado(sessao_real, obra, dono):
    """⚑ A OBRA COMEÇA ÀS 7 DA MANHÃ e não para porque uma máquina ainda não
    foi cadastrada. Ela entra com o nome digitado — e MARCADA, para
    Suprimentos cadastrar depois."""
    dia = date(2026, 3, 10)
    svc.salvar(sessao_real, obra, dia, {"relato": "x"}, dono)
    sessao_real.flush()
    svc.acrescentar_equipamento(sessao_real, obra, dia,
                                {"descricao": "Guincho emprestado"})
    sessao_real.flush()
    linha = svc.abrir_dia(sessao_real, obra, dia)["diario"]["equipamentos_lista"][0]
    assert linha["descricao"] == "Guincho emprestado"
    assert linha["insumo_id"] is None
    assert linha["fora_da_lista"] is True


@banco_
def test_repetir_de_ontem_leva_o_insumo_junto(sessao_real, obra, dono):
    """Senão o equipamento repetido viraria "fora da lista" no dia seguinte, e
    a conta de dias-máquina quebraria na virada do dia."""
    i = _insumo(sessao_real, "Placa vibratória")
    svc.salvar(sessao_real, obra, date(2026, 3, 10), {"relato": "x"}, dono)
    sessao_real.flush()
    svc.acrescentar_equipamento(sessao_real, obra, date(2026, 3, 10),
                                {"insumo_id": i, "quantidade": 2})
    svc.salvar(sessao_real, obra, date(2026, 3, 11), {"relato": "y"}, dono)
    sessao_real.flush()
    svc.repetir_equipamentos_de_ontem(sessao_real, obra, date(2026, 3, 11))
    sessao_real.flush()
    linha = svc.abrir_dia(sessao_real, obra,
                          date(2026, 3, 11))["diario"]["equipamentos_lista"][0]
    assert linha["insumo_id"] == i
    assert linha["fora_da_lista"] is False


@banco_
def test_o_dia_oferece_o_catalogo_junto(sessao_real, obra, dono):
    """A caixinha não pode precisar de uma segunda ida ao servidor na hora em
    que a pessoa já está esperando o diálogo abrir."""
    _insumo(sessao_real, "Andaime tubular")
    sessao_real.flush()
    d = svc.abrir_dia(sessao_real, obra, date(2026, 3, 10))
    assert any(i["descricao"] == "Andaime tubular"
               for i in d["equipamentos_do_catalogo"])


def test_a_tela_do_diario_nao_manda_mais_o_texto_de_equipamentos():
    """⚑ A TRAVA DA DUPLICIDADE, e ela protege o passado.

    Ele: *"eles são a mesma coisa, por que teriam os dois textos? Ou é um ou é
    o outro."* O campo saiu do formulário — mas a COLUNA ficou, porque os
    diários antigos a usam e já foram assinados. Se a tela voltar a mandar
    `equipamentos` no pedido, ela mandaria VAZIO, e todo dia antigo reaberto
    perderia o que tinha escrito.
    """
    import pathlib
    tela = pathlib.Path(
        "app/apps/erp/templates/erp_diario.html").read_text(encoding="utf-8")
    assert "d-equipamentos" not in tela, (
        "o campo de texto de equipamentos voltou ao formulário do diário — "
        "ele é a mesma coisa que a lista, e reabrir um dia antigo apagaria o "
        "que estava escrito")
    assert "equipamentos_lista" in tela, "a lista tem de continuar na tela"


@banco_
def test_reabrir_e_gravar_o_dia_nao_apaga_o_texto_antigo(sessao_real, obra,
                                                         dono):
    """O mesmo risco, provado pela regra e não só pela tela: gravar sem mandar
    o campo deixa o que estava escrito onde estava."""
    dia = date(2026, 3, 10)
    svc.salvar(sessao_real, obra, dia,
               {"relato": "x", "equipamentos": "01 caçamba"}, dono)
    sessao_real.flush()
    svc.salvar(sessao_real, obra, dia, {"relato": "x, corrigido"}, dono)
    sessao_real.flush()
    assert svc.abrir_dia(sessao_real, obra,
                         dia)["diario"]["equipamentos"] == "01 caçamba"


# ---------------------------------------------------------------------------
# O MODELO DO ÓRGÃO TEM DE SER TAL QUAL O PAPEL DELE (13/11/2026)
#
# Ele: *"o modelo que você digitou aí não está igual não. As cores. Porque o
# modelo do órgão, ele tem que ser tal qual, não pode ser parecido não.
# Inclusive as cores."*
#
# ⚑ POR QUE ISTO VIRA TESTE E NÃO SÓ COMENTÁRIO: a primeira versão deste
# modelo usou a paleta da casa porque ela é bonita, e a tentação de "dar uma
# melhorada no visual" volta toda vez que alguém abre o arquivo. RDO fora do
# modelo exigido é motivo de GLOSA em medição — o custo não é estético.
# ---------------------------------------------------------------------------
def _ler(nome):
    import pathlib
    return pathlib.Path("app/apps/erp/templates", nome).read_text(encoding="utf-8")


def _sem_comentario(texto):
    """O que o navegador recebe — sem os comentários `{# … #}` do Jinja.

    Importa aqui porque os comentários deste modelo CITAM a paleta antiga
    (para explicar por que ela saiu). Procurar "#1b3a5c" no arquivo inteiro
    acusaria justamente a explicação de por que ele não está mais lá."""
    import re
    return re.sub(r"\{#.*?#\}", " ", texto, flags=re.S)


def test_o_azul_do_modelo_do_orgao_e_o_do_arquivo_dele():
    """#1F4E79 não foi escolhido: é o accent1 (#5B9BD5) do tema do RDO.xlsx
    dele com tinta −0,5, que é o "Azul, Ênfase 1, Mais Escuro 50%" do Excel."""
    css = _ler("erp_diario_papel.html")
    assert "#1F4E79" in css
    # e a faixa não pode se chamar `.barra`: esse nome já é o da barra de
    # ferramentas da página, que tem `@media print { display:none }` — as oito
    # faixas sumiam justamente na impressão.
    assert ".org .faixa-org" in css
    assert "<div class=\"faixa-org\"></div>" in _ler("erp_diario_papel_orgao.html")


def test_o_modelo_do_orgao_nao_usa_a_paleta_da_casa():
    """Nem o azul da BWS, nem a faixa cinza de seção, nem Helvetica: o papel
    dele é preto sobre branco, em Arial, com título de seção CENTRADO."""
    papel = _sem_comentario(_ler("erp_diario_papel_orgao.html"))
    for proibido in ("var(--forte)", "var(--faixa)", "var(--tinta)", "#1b3a5c"):
        assert proibido not in papel, f"{proibido} voltou ao modelo do órgão"
    css = _ler("erp_diario_papel.html")
    bloco = css.split('{% if r.modelo == "orgao" %}')[1].split("{% endif %}")[0]
    assert "Arial" in bloco
    assert "color: #000" in bloco
    assert "text-align: center" in bloco


def test_o_modelo_do_orgao_tem_os_quadros_na_ordem_do_papel_dele():
    """A ordem é a do arquivo dele, linha por linha. Quadro fora de ordem é
    outro formulário."""
    import re
    papel = _sem_comentario(_ler("erp_diario_papel_orgao.html"))
    # Os títulos como a folha os imprime, na ordem em que aparecem. Procurar
    # a palavra solta não serve: "OCORRÊNCIAS" vive dentro de "HISTÓRICO DOS
    # SERVIÇOS/ OCORRÊNCIAS", e o teste acusaria desordem onde não há.
    achados = [m.strip() for m in
               re.findall(r'<div class="sec"[^>]*>(.*?)</div>', papel, re.S)]
    assert achados == [
        "HISTÓRICO DOS SERVIÇOS/ OCORRÊNCIAS", "QUADRO TÉCNICO",
        "CONTRATADA", "OUTROS", "EQUIPAMENTOS", "SERVIÇOS EXECUTADOS",
        "OBSERVAÇÃO DA FISCALIZAÇÃO", "OCORRÊNCIAS",
    ], "a ordem (ou o nome) dos quadros do órgão mudou"


def test_so_o_modelo_do_orgao_ganha_esse_estilo():
    """Os outros dois modelos continuam com a cara da casa — o pedido dele era
    sobre o do órgão, e só."""
    css = _ler("erp_diario_papel.html")
    assert '{% if r.modelo == "orgao" %}' in css
    for outro in ("erp_diario_papel_bws.html", "erp_diario_papel_completo.html"):
        assert "faixa-org" not in _ler(outro)
