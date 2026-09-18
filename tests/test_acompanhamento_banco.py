"""ACOMPANHAMENTO — o escopo por obra, com banco de verdade.

Aqui vive a parte que o dublê de sessão não alcança: a listagem filtra por obra
designada, e o detalhe responde **404 "não encontrado"** — nunca 403 — para o
processo de uma obra que a pessoa não alcança. Dizer "sem permissão" para um
número que existe confirma que ele existe, e varrer os números mapearia os
processos da empresa inteira sem abrir um só.

A regra tem de valer nos DOIS caminhos, e é por isso que os testes de lista e
de detalhe estão no mesmo arquivo: se um dia divergirem, é aqui que aparece.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest

from app.apps.erp.core.acompanhamento import processos as svc
from app.apps.erp.core.auth.permissoes import obras_de_registro_sem_autor
from app.apps.erp.core.auth.service import gerar_hash
from app.apps.erp.db.models.cadastros import (
    Empresa, Obra, PerfilUsuario as P, Processo, ProcessoAndamento, Usuario,
    UsuarioObra,
)

from conftest import como

pytestmark = pytest.mark.banco


@pytest.fixture
def cenario(sessao_real):
    s = sessao_real
    minha = Obra(codigo="ACOMP-A", nome="Escola Municipal")
    outra = Obra(codigo="ACOMP-B", nome="Posto de Saúde")
    empresa = Empresa(razao_social="BWS ACOMP LTDA", cnpj="11444777000161")
    chefe = Usuario(nome="Chefe", email="chefe.ac@teste.local", ativo=True,
                    senha_hash=gerar_hash("senha-de-teste-123"), perfil=P.ADMIN)
    preso = Usuario(nome="Ruan", email="ruan.ac@teste.local", ativo=True,
                    senha_hash=gerar_hash("senha-de-teste-123"),
                    perfil=P.ADMINISTRATIVO_OBRA)
    s.add_all([minha, outra, empresa, chefe, preso]); s.flush()
    s.add(UsuarioObra(usuario_id=preso.id, obra_id=minha.id, responsavel=True))
    s.flush()

    da_minha = svc.criar(s, {"assunto": "Aditivo de prazo nº 2",
                             "tipo": "ADITIVO_PRAZO", "obra_id": minha.id}, chefe)
    da_outra = svc.criar(s, {"assunto": "Licença ambiental",
                             "tipo": "LICENCA", "obra_id": outra.id}, chefe)
    da_empresa = svc.criar(s, {"assunto": "CND federal", "tipo": "CERTIDAO",
                               "empresa_id": empresa.id}, chefe)
    s.flush()
    return {"s": s, "minha": minha, "outra": outra, "empresa": empresa,
            "chefe": chefe, "preso": preso, "da_minha": da_minha,
            "da_outra": da_outra, "da_empresa": da_empresa}


def _numeros(r):
    return {p["numero"] for p in r["processos"]}


# ---------------------------------------------------------------------------
# A lista
# ---------------------------------------------------------------------------
def test_quem_ve_tudo_ve_os_tres(cenario):
    r = svc.listar(cenario["s"], obras_permitidas=None)
    assert _numeros(r) == {cenario["da_minha"].numero, cenario["da_outra"].numero,
                           cenario["da_empresa"].numero}


def test_preso_a_obra_so_ve_a_obra_dele(cenario):
    s, preso = cenario["s"], cenario["preso"]
    r = svc.listar(s, obras_permitidas=obras_de_registro_sem_autor(s, preso))
    assert _numeros(r) == {cenario["da_minha"].numero}


def test_processo_da_empresa_nao_aparece_para_quem_e_preso_a_obra(cenario):
    """Certidão da sede não é de obra nenhuma: só quem enxerga a base inteira.

    É o padrão NEGAR do ERP, e não um esquecimento — abrir depois é uma linha,
    fechar depois é conversa constrangedora.
    """
    s, preso = cenario["s"], cenario["preso"]
    r = svc.listar(s, obras_permitidas=obras_de_registro_sem_autor(s, preso))
    assert cenario["da_empresa"].numero not in _numeros(r)


def test_sem_obra_designada_nao_ve_nenhum(cenario):
    """Lista vazia de obras significa NADA, não "sem filtro"."""
    r = svc.listar(cenario["s"], obras_permitidas=[])
    assert r["processos"] == []


def test_encerrados_saem_da_lista_por_padrao(cenario):
    s = cenario["s"]
    svc.atualizar(s, cenario["da_outra"].id, {"situacao": "DEFERIDO"},
                  cenario["chefe"])
    s.flush()

    assert cenario["da_outra"].numero not in _numeros(svc.listar(s, obras_permitidas=None))
    assert cenario["da_outra"].numero in _numeros(
        svc.listar(s, obras_permitidas=None, incluir_encerrados=True))


def test_a_ordem_poe_a_exigencia_na_frente(cenario):
    """Quem assumiu o assunto de outro precisa da linha que mais dói em cima."""
    s, chefe = cenario["s"], cenario["chefe"]
    svc.atualizar(s, cenario["da_outra"].id, {"situacao": "EXIGENCIA"}, chefe)
    s.flush()

    r = svc.listar(s, obras_permitidas=None)
    assert r["processos"][0]["numero"] == cenario["da_outra"].numero
    assert r["processos"][0]["urgencia"] == "EXIGENCIA"


def test_o_resumo_conta_por_urgencia_e_o_pendente_exclui_o_que_esta_em_dia(cenario):
    s, chefe = cenario["s"], cenario["chefe"]
    svc.atualizar(s, cenario["da_minha"].id, {"situacao": "EXIGENCIA"}, chefe)
    s.flush()

    r = svc.listar(s, obras_permitidas=None)
    assert r["resumo"]["EXIGENCIA"] == 1
    assert r["pendentes"] == sum(v for k, v in r["resumo"].items() if k != "EM_DIA")


def test_o_ultimo_andamento_decide_os_dias_parado(cenario):
    s, chefe = cenario["s"], cenario["chefe"]
    svc.lancar_andamento(s, cenario["da_minha"].id, {"texto": "liguei hoje"}, chefe)
    s.flush()

    linha = [p for p in svc.listar(s, obras_permitidas=None)["processos"]
             if p["numero"] == cenario["da_minha"].numero][0]
    assert linha["dias_parado"] == 0


def test_o_filtro_por_tipo_e_por_responsavel_funcionam_juntos(cenario):
    s = cenario["s"]
    r = svc.listar(s, obras_permitidas=None, tipo="LICENCA",
                   responsavel_id=cenario["chefe"].id)
    assert _numeros(r) == {cenario["da_outra"].numero}


# ---------------------------------------------------------------------------
# O detalhe, e o 404 que não confirma existência
# ---------------------------------------------------------------------------
def test_detalhe_traz_o_historico_do_mais_novo_para_o_mais_antigo(cenario):
    s, chefe = cenario["s"], cenario["chefe"]
    pid = cenario["da_minha"].id
    svc.lancar_andamento(s, pid, {"texto": "primeira"}, chefe)
    s.flush()
    svc.lancar_andamento(s, pid, {"texto": "segunda"}, chefe)
    s.flush()

    d = svc.detalhe(s, pid)
    assert [a["texto"] for a in d["andamentos"]] == ["segunda", "primeira"]


def test_pela_rota_o_processo_de_outra_obra_responde_404(cenario, app_real):
    """404 e não 403: "sem permissão" confirmaria que o número existe."""
    c = como(app_real, cenario["preso"].id)

    r = c.get(f"/erp/api/acompanhamento/{cenario['da_outra'].id}")

    assert r.status_code == 404


def test_pela_rota_o_processo_da_propria_obra_abre(cenario, app_real):
    c = como(app_real, cenario["preso"].id)

    r = c.get(f"/erp/api/acompanhamento/{cenario['da_minha'].id}")

    assert r.status_code == 200
    assert r.get_json()["processo"]["numero"] == cenario["da_minha"].numero


def test_pela_rota_nao_da_para_lancar_andamento_em_processo_de_fora(cenario, app_real):
    """A lista e o detalhe negam; escrever também tem de negar, ou a trava é
    enfeite."""
    c = como(app_real, cenario["preso"].id)

    r = c.post(f"/erp/api/acompanhamento/{cenario['da_outra'].id}/andamento",
               json={"texto": "tentando escrever na obra dos outros"})

    assert r.status_code == 404
    assert not [a for a in cenario["s"].query(ProcessoAndamento).all()
                if a.processo_id == cenario["da_outra"].id]


def test_pela_rota_nao_da_para_assumir_processo_de_fora(cenario, app_real):
    c = como(app_real, cenario["preso"].id)

    r = c.post("/erp/api/acompanhamento/assumir",
               json={"ids": [cenario["da_outra"].id],
                     "responsavel_id": cenario["preso"].id})

    assert r.status_code == 404
    assert cenario["da_outra"].responsavel_id == cenario["chefe"].id


def test_pela_rota_a_lista_do_preso_a_obra_ja_vem_recortada(cenario, app_real):
    c = como(app_real, cenario["preso"].id)

    r = c.get("/erp/api/acompanhamento")

    assert r.status_code == 200
    numeros = {p["numero"] for p in r.get_json()["processos"]}
    assert numeros == {cenario["da_minha"].numero}


def test_pela_rota_nao_da_para_abrir_processo_em_obra_de_fora(cenario, app_real):
    c = como(app_real, cenario["preso"].id)

    r = c.post("/erp/api/acompanhamento",
               json={"assunto": "Invadindo", "tipo": "OUTRO",
                     "obra_id": cenario["outra"].id})

    assert r.status_code == 404


# ---------------------------------------------------------------------------
# O que fica registrado
# ---------------------------------------------------------------------------
def test_abrir_lancar_e_assumir_deixam_rastro_na_trilha(cenario):
    from sqlalchemy import text

    s, chefe = cenario["s"], cenario["chefe"]
    pid = cenario["da_minha"].id
    svc.lancar_andamento(s, pid, {"texto": "liguei"}, chefe)
    svc.assumir(s, [pid], cenario["preso"].id, chefe)
    s.flush()

    acoes = {r[0] for r in s.execute(text(
        "SELECT acao FROM eventos WHERE entidade_tipo = 'processo' "
        "AND entidade_id = :i"), {"i": pid}).all()}
    assert {"PROCESSO_ABERTO", "PROCESSO_ANDAMENTO", "PROCESSO_ASSUMIDO"} <= acoes


# ---------------------------------------------------------------------------
# OS PASSOS e O AVISO QUE CHEGA SOZINHO (pedaço 2, migração 073)
# ---------------------------------------------------------------------------
def test_marcar_e_desmarcar_passo_guarda_quem_e_quando(cenario):
    s, chefe = cenario["s"], cenario["chefe"]
    passos = svc.passos_do_processo(s, cenario["da_minha"].id)
    assert len(passos) == 8, "o aditivo de prazo nasce com a lista do tipo"

    svc.marcar_passo(s, passos[0].id, True, chefe)
    assert passos[0].feito_em is not None and passos[0].feito_por == chefe.id

    svc.marcar_passo(s, passos[0].id, False, chefe)
    assert passos[0].feito_em is None and passos[0].feito_por is None


def test_marcar_fora_de_ordem_e_permitido(cenario):
    """O órgão não segue ordem, e obrigar ordem faria a pessoa mentir ao sistema."""
    s, chefe = cenario["s"], cenario["chefe"]
    passos = svc.passos_do_processo(s, cenario["da_minha"].id)

    svc.marcar_passo(s, passos[6].id, True, chefe)
    svc.marcar_passo(s, passos[2].id, True, chefe)

    feitos = [p.ordem for p in svc.passos_do_processo(s, cenario["da_minha"].id)
              if p.feito_em is not None]
    assert feitos == [2, 6]


def test_encerrar_com_passo_em_branco_nao_reclama(cenario):
    """A lista lembra, não barra. Se um dia isto falhar, o módulo virou o SEI."""
    s, chefe = cenario["s"], cenario["chefe"]

    svc.atualizar(s, cenario["da_minha"].id, {"situacao": "DEFERIDO"}, chefe)
    s.flush()

    assert cenario["da_minha"].situacao == "DEFERIDO"
    assert any(p.feito_em is None
               for p in svc.passos_do_processo(s, cenario["da_minha"].id))


def test_da_para_acrescentar_e_apagar_passo(cenario):
    """O passo que só aquela prefeitura pede — sem isso a lista do modelo
    viraria camisa de força com cara de ajuda."""
    s, chefe = cenario["s"], cenario["chefe"]
    pid = cenario["da_minha"].id

    novo = svc.acrescentar_passo(s, pid, "Levar a via impressa no balcão", chefe)
    assert novo.texto in [p.texto for p in svc.passos_do_processo(s, pid)]

    assert svc.apagar_passo(s, novo.id) is True
    assert novo.texto not in [p.texto for p in svc.passos_do_processo(s, pid)]


def test_a_lista_traz_quantos_passos_ja_foram(cenario):
    s, chefe = cenario["s"], cenario["chefe"]
    passos = svc.passos_do_processo(s, cenario["da_minha"].id)
    svc.marcar_passo(s, passos[0].id, True, chefe)
    svc.marcar_passo(s, passos[1].id, True, chefe)
    s.flush()

    linha = [p for p in svc.listar(s, obras_permitidas=None)["processos"]
             if p["numero"] == cenario["da_minha"].numero][0]
    assert (linha["passos_feitos"], linha["passos_total"]) == (2, 8)


def test_o_passo_de_outro_processo_nao_e_alcancavel_pela_rota(cenario, app_real):
    """Mandar o id de um passo de fora seria uma porta lateral para o escopo."""
    s = cenario["s"]
    de_fora = svc.passos_do_processo(s, cenario["da_outra"].id)
    if not de_fora:
        de_fora = [svc.acrescentar_passo(s, cenario["da_outra"].id, "passo",
                                         cenario["chefe"])]
    s.flush()
    c = como(app_real, cenario["chefe"].id)

    r = c.post(f"/erp/api/acompanhamento/{cenario['da_minha'].id}/passo",
               json={"passo_id": de_fora[0].id, "feito": True})

    assert r.status_code == 404
    assert de_fora[0].feito_em is None


# ---------------------------------------------------------------------------
# O aviso que chega sozinho: o processo travado entra na AGENDA
# ---------------------------------------------------------------------------
def test_processo_com_exigencia_vira_aviso_na_agenda(cenario):
    from app.apps.erp.core.agenda import geradores

    s, chefe = cenario["s"], cenario["chefe"]
    svc.atualizar(s, cenario["da_minha"].id, {"situacao": "EXIGENCIA"}, chefe)
    s.flush()

    eventos = geradores.processos(s, date(2026, 9, 18))

    meu = [e for e in eventos if cenario["da_minha"].numero in e["titulo"]]
    assert len(meu) == 1
    assert meu[0]["origem"] == "PROCESSO"
    assert meu[0]["obra_id"] == cenario["minha"].id
    assert "pediu alguma coisa" in meu[0]["detalhe"]
    assert meu[0]["link"] == "/erp/acompanhamento"


def test_processo_com_previsao_estourada_vira_aviso(cenario):
    from app.apps.erp.core.agenda import geradores

    s, chefe = cenario["s"], cenario["chefe"]
    svc.atualizar(s, cenario["da_minha"].id,
                  {"previsao": (date(2026, 9, 18) - timedelta(days=4)).isoformat()},
                  chefe)
    s.flush()

    eventos = geradores.processos(s, date(2026, 9, 18))

    assert any("4 dia(s)" in e["detalhe"]
               for e in eventos if cenario["da_minha"].numero in e["titulo"])


def test_processo_encerrado_nao_gera_aviso_nenhum(cenario):
    """O que terminou não é pendência, e uma agenda cheia do que já acabou é
    uma agenda que ninguém abre."""
    from app.apps.erp.core.agenda import geradores

    s, chefe = cenario["s"], cenario["chefe"]
    for chave in ("da_minha", "da_outra", "da_empresa"):
        svc.atualizar(s, cenario[chave].id, {"situacao": "ARQUIVADO"}, chefe)
    s.flush()

    assert geradores.processos(s, date(2026, 9, 18)) == []


def test_processo_em_dia_nao_enche_a_agenda(cenario):
    """Aviso que aparece sempre deixa de ser lido quando importa."""
    from app.apps.erp.core.agenda import geradores

    s, chefe = cenario["s"], cenario["chefe"]
    for chave in ("da_minha", "da_outra", "da_empresa"):
        svc.lancar_andamento(s, cenario[chave].id, {"texto": "andou hoje"}, chefe)
    s.flush()

    assert geradores.processos(s, date.today()) == []


def test_a_chave_do_aviso_e_estavel_para_nao_empilhar(cenario):
    """A sincronização roda todo dia: chave instável empilharia avisos iguais."""
    from app.apps.erp.core.agenda import geradores

    s, chefe = cenario["s"], cenario["chefe"]
    svc.atualizar(s, cenario["da_minha"].id, {"situacao": "EXIGENCIA"}, chefe)
    s.flush()

    um = geradores.processos(s, date(2026, 9, 18))
    dois = geradores.processos(s, date(2026, 9, 19))

    assert {e["chave"] for e in um} == {e["chave"] for e in dois}
