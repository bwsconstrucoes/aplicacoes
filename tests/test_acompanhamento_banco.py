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
