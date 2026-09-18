"""ACOMPANHAMENTO — a gestão burocrática da obra, sem banco.

O módulo nasceu de um pedido do dono em 17/09/2026 e de uma exigência dele que
manda em tudo aqui: *"não quero uma coisa travada (…) simples de alimentar e de
atualizar (…) imagina que uma pessoa saiu de férias: quem for fazer esse
acompanhamento precisaria bater o olho e ver o que está pendente"*.

O que estes testes seguram:

1. **Abrir custa pouco** — assunto, tipo e um dono. Exigir mais faria a pessoa
   deixar para abrir depois, e depois é nunca.
2. **Lançar andamento é uma frase**, e essa mesma frase pode mudar onde está,
   a previsão e a situação de uma vez.
3. **Nada trava nada.** Não há transição proibida entre situações: o órgão não
   segue ordem, e obrigar ordem faria a pessoa mentir para o sistema.
4. **A urgência é CALCULADA e explicada.** "Parado" sai dos dias sem andamento,
   não de alguém marcar — campo que depende de lembrança estará errado um dia.

O escopo por obra vive no `WHERE` e está em `test_acompanhamento_banco.py`.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest

from app.apps.erp.core.acompanhamento import processos as svc
from app.apps.erp.core.comum.auditoria import ErroValidacao
from app.apps.erp.db.models.cadastros import (
    Empresa, Obra, PerfilUsuario as P, Processo, ProcessoAndamento,
)

from conftest import SessaoFalsa, novo_usuario

HOJE = date(2026, 9, 18)


@pytest.fixture
def admin():
    return novo_usuario(1, P.ADMIN)


@pytest.fixture
def sessao():
    return SessaoFalsa(Obra(id=10, codigo="OB-01", nome="Escola Municipal"),
                       Empresa(id=5, razao_social="BWS CONSTRUÇÕES LTDA"))


def _processo(**extra):
    base = dict(id=1, numero="AC-000001", assunto="Aditivo de prazo nº 2",
                tipo="ADITIVO_PRAZO", obra_id=10, situacao="EM_ANALISE",
                criado_em=datetime(2026, 9, 1, tzinfo=timezone.utc))
    base.update(extra)
    return Processo(**base)


# ---------------------------------------------------------------------------
# Abrir custa pouco
# ---------------------------------------------------------------------------
def test_abrir_exige_assunto_tipo_e_um_dono(sessao, admin):
    p = svc.criar(sessao, {"assunto": "Aditivo de prazo nº 2",
                           "tipo": "ADITIVO_PRAZO", "obra_id": 10}, admin)

    assert p.numero == "AC-000001"
    assert p.situacao == "RASCUNHO"
    assert p.responsavel_id == admin.id, "quem abre responde, até passar adiante"


def test_sem_assunto_nao_abre(sessao, admin):
    with pytest.raises(ErroValidacao, match="assunto"):
        svc.criar(sessao, {"tipo": "LICENCA", "obra_id": 10}, admin)


def test_processo_e_de_uma_obra_ou_da_empresa_nunca_dos_dois(sessao, admin):
    """Sem dono não há escopo, e com dois o escopo fica ambíguo."""
    with pytest.raises(ErroValidacao, match="UMA obra ou da empresa"):
        svc.criar(sessao, {"assunto": "Certidão", "tipo": "CERTIDAO",
                           "obra_id": 10, "empresa_id": 5}, admin)
    with pytest.raises(ErroValidacao, match="UMA obra ou da empresa"):
        svc.criar(sessao, {"assunto": "Certidão", "tipo": "CERTIDAO"}, admin)


def test_processo_da_empresa_nao_precisa_de_obra(sessao, admin):
    """Certidão e alvará da sede não são de obra nenhuma."""
    p = svc.criar(sessao, {"assunto": "CND federal", "tipo": "CERTIDAO",
                           "empresa_id": 5}, admin)
    assert p.empresa_id == 5 and p.obra_id is None


def test_tipo_desconhecido_e_recusado(sessao, admin):
    with pytest.raises(ErroValidacao, match="desconhecido"):
        svc.criar(sessao, {"assunto": "x", "tipo": "FOGUETE", "obra_id": 10}, admin)


def test_a_numeracao_continua_de_onde_parou(admin):
    s = SessaoFalsa(Obra(id=10, codigo="OB-01", nome="Obra"),
                    _processo(id=7, numero="AC-000041"))
    p = svc.criar(s, {"assunto": "Outro", "tipo": "OUTRO", "obra_id": 10}, admin)
    assert p.numero == "AC-000042"


# ---------------------------------------------------------------------------
# O andamento é uma frase
# ---------------------------------------------------------------------------
def test_uma_frase_basta(sessao, admin):
    p = _processo()
    s = SessaoFalsa(p)

    a = svc.lancar_andamento(s, 1, {"texto": "Liguei, está com o jurídico."}, admin)

    assert a.texto == "Liguei, está com o jurídico."
    assert a.por_id == admin.id
    assert a.situacao is None, "frase sem situação não muda o processo"


def test_sem_texto_nao_lanca(sessao, admin):
    s = SessaoFalsa(_processo())
    with pytest.raises(ErroValidacao, match="andamento"):
        svc.lancar_andamento(s, 1, {"texto": "   "}, admin)


def test_a_mesma_frase_muda_onde_esta_previsao_e_situacao(admin):
    """Quem ligou descobre as três coisas de uma vez — três formulários fariam
    nenhuma ser registrada."""
    p = _processo()
    s = SessaoFalsa(p)

    svc.lancar_andamento(s, 1, {
        "texto": "Falei com a Ana: subiu para o gabinete, sai dia 30.",
        "onde_esta": "Gabinete do secretário",
        "previsao": "2026-09-30",
        "situacao": "EM_ANALISE"}, admin)

    assert p.onde_esta == "Gabinete do secretário"
    assert p.previsao == date(2026, 9, 30)
    assert p.situacao == "EM_ANALISE"


def test_situacao_desconhecida_e_recusada(admin):
    s = SessaoFalsa(_processo())
    with pytest.raises(ErroValidacao, match="Situação desconhecida"):
        svc.lancar_andamento(s, 1, {"texto": "x", "situacao": "PENSANDO"}, admin)


@pytest.mark.parametrize("de, para", [
    ("DEFERIDO", "EM_ANALISE"),        # deferido voltou atrás
    ("ARQUIVADO", "PROTOCOLADO"),      # reabriu o que estava morto
    ("RASCUNHO", "DEFERIDO"),          # saiu sem passar por protocolo
    ("EXIGENCIA", "RASCUNHO"),         # voltou para o começo
])
def test_nenhuma_transicao_e_proibida(de, para, admin):
    """O órgão não segue ordem. Obrigar ordem faria a pessoa mentir ao sistema."""
    p = _processo(situacao=de)
    s = SessaoFalsa(p)

    svc.lancar_andamento(s, 1, {"texto": "mudou", "situacao": para}, admin)

    assert p.situacao == para


def test_encerrar_carimba_a_data_e_reabrir_apaga(admin):
    p = _processo()
    s = SessaoFalsa(p)

    svc.lancar_andamento(s, 1, {"texto": "saiu", "situacao": "DEFERIDO"}, admin)
    assert p.encerrado_em is not None

    svc.lancar_andamento(s, 1, {"texto": "voltou", "situacao": "EXIGENCIA"}, admin)
    assert p.encerrado_em is None, "processo reaberto não pode constar encerrado"


# ---------------------------------------------------------------------------
# A urgência é calculada, e explicada
# ---------------------------------------------------------------------------
def test_exigencia_vem_antes_de_tudo():
    """É o único estado em que o órgão está esperando a BWS, e não o contrário."""
    p = _processo(situacao="EXIGENCIA")
    u = svc.urgencia(p, HOJE - timedelta(days=1), HOJE)
    assert u["chave"] == "EXIGENCIA"
    assert "pediu alguma coisa" in u["motivo"]


def test_previsao_vencida_vira_atrasado_com_os_dias_no_motivo():
    p = _processo(previsao=HOJE - timedelta(days=5))
    u = svc.urgencia(p, HOJE, HOJE)
    assert u["chave"] == "ATRASADO"
    assert "5 dia(s)" in u["motivo"]


def test_parado_sai_dos_dias_sem_andamento_e_o_teto_e_por_tipo():
    """Licença dorme semanas sem que isso signifique nada; aditivo de prazo não.

    Um teto único acenderia a luz nos dois lugares errados.
    """
    aditivo = _processo(tipo="ADITIVO_PRAZO")     # teto 10
    licenca = _processo(tipo="LICENCA")           # teto 30
    ha_doze_dias = HOJE - timedelta(days=12)

    assert svc.urgencia(aditivo, ha_doze_dias, HOJE)["chave"] == "PARADO"
    assert svc.urgencia(licenca, ha_doze_dias, HOJE)["chave"] == "EM_DIA"


def test_processo_nunca_tocado_conta_desde_a_abertura():
    """Aberto e esquecido é justamente o caso que mais interessa."""
    p = _processo(criado_em=datetime(2026, 8, 1, tzinfo=timezone.utc))
    u = svc.urgencia(p, None, HOJE)
    assert u["chave"] == "PARADO"
    assert u["dias_parado"] == 48


def test_previsao_chegando_acende_uma_semana_antes():
    p = _processo(previsao=HOJE + timedelta(days=3))
    assert svc.urgencia(p, HOJE, HOJE)["chave"] == "PROXIMO"


def test_previsao_distante_nao_acende():
    p = _processo(previsao=HOJE + timedelta(days=40))
    assert svc.urgencia(p, HOJE, HOJE)["chave"] == "EM_DIA"


def test_processo_encerrado_nao_fica_parado_nem_atrasado():
    """O que terminou não é pendência — e ocupar a tela com isso faz a pessoa
    parar de olhar a tela."""
    p = _processo(situacao="DEFERIDO", previsao=HOJE - timedelta(days=90))
    u = svc.urgencia(p, HOJE - timedelta(days=200), HOJE)
    assert u["chave"] == "EM_DIA"
    assert u["motivo"] == ""


def test_todo_motivo_e_escrito_em_portugues_e_nunca_vazio_quando_urge():
    """Urgência sem motivo obriga a abrir o processo para descobrir por quê —
    que é o trabalho que esta tela existe para evitar."""
    casos = [
        _processo(situacao="EXIGENCIA"),
        _processo(previsao=HOJE - timedelta(days=2)),
        _processo(criado_em=datetime(2026, 7, 1, tzinfo=timezone.utc)),
        _processo(previsao=HOJE + timedelta(days=2)),
    ]
    for p in casos:
        u = svc.urgencia(p, None, HOJE)
        assert u["chave"] != "EM_DIA"
        assert u["motivo"].strip(), f"{u['chave']} ficou sem motivo"


# ---------------------------------------------------------------------------
# Assumir — o botão do caso das férias
# ---------------------------------------------------------------------------
def test_assumir_troca_o_responsavel_de_varios_de_uma_vez(admin):
    outro = novo_usuario(9, P.SUPERVISOR_OBRA, nome="Ana")
    a, b = _processo(id=1, responsavel_id=1), _processo(id=2, numero="AC-000002",
                                                        responsavel_id=1)
    s = SessaoFalsa(a, b, outro)

    trocados = svc.assumir(s, [1, 2], 9, admin)

    assert trocados == 2
    assert a.responsavel_id == 9 and b.responsavel_id == 9


def test_assumir_quem_ja_e_responsavel_nao_conta_nem_registra(admin):
    outro = novo_usuario(9, P.SUPERVISOR_OBRA, nome="Ana")
    a = _processo(id=1, responsavel_id=9)
    s = SessaoFalsa(a, outro)

    assert svc.assumir(s, [1], 9, admin) == 0


def test_assumir_para_operador_que_nao_existe_e_recusado(admin):
    s = SessaoFalsa(_processo())
    with pytest.raises(ErroValidacao, match="Operador"):
        svc.assumir(s, [1], 999, admin)
