"""O QUE UMA VARREDURA DE USO ACHOU — 22/09/2026.

Pedido do dono: *"Faça e busque por erros operacionais. Simule alguém
utilizando o sistema."*

Foi montado um ERP com dados de verdade (várias obras, credores, títulos em
situações diferentes, colaboradores) e percorrido tela por tela, perfil por
perfil, com entrada boa e entrada ruim. Cada teste daqui defende um defeito
que a varredura achou — e todos são do mesmo tipo: o sistema respondia
"pronto" e a informação não estava lá, ou respondia "falha do sistema" quando
bastava explicar o que corrigir.
"""
from __future__ import annotations

from datetime import date

import pytest

from app.apps.erp.core.auth.service import gerar_hash
from app.apps.erp.core.comum.auditoria import ErroValidacao
from app.apps.erp.core.pessoal import listar_colaboradores, salvar_colaborador
from app.apps.erp.db.models.cadastros import (
    Colaborador, Obra, PerfilUsuario as P, Usuario,
)

from conftest import como

pytestmark = pytest.mark.banco


def _pessoa(s, apelido, perfil=P.ADMIN):
    u = Usuario(nome=apelido, email=f"{apelido}@teste.local", ativo=True,
                senha_hash=gerar_hash("senha-de-teste-123"), perfil=perfil,
                telefone="5585900000000")
    s.add(u)
    s.flush()
    return u


@pytest.fixture
def cenario(sessao_real):
    s = sessao_real
    obra = Obra(codigo="VARREDURA", nome="Obra da varredura de uso", status="ATIVA")
    s.add(obra)
    s.flush()
    return {"s": s, "obra": obra, "dono": _pessoa(s, "dono-varredura")}


# ---------------------------------------------------------------------------
# 1. CAMPOS DA OBRA QUE A TELA MOSTRAVA E O SISTEMA NUNCA GRAVOU
#
# Seguro-garantia, validade da apólice, caução e o departamento no Omie
# apareciam no cadastro da obra. A pessoa preenchia, o sistema respondia
# "Salvo." — e nada era gravado. Mesmo defeito do código da obra, quatro vezes.
# ---------------------------------------------------------------------------
CAMPOS_DO_CONTRATO = {
    "seguro_garantia": "APOLICE 1234567-8 PORTO SEGURO",
    "seguro_vigencia_fim": "2027-03-31",
    "caucao_pct": "5,00",
    "codigo_omie_depto": "DEP-VARREDURA",
}


def test_o_seguro_garantia_da_obra_e_gravado_e_volta(cenario, app_real):
    c = cenario
    r = como(app_real, c["dono"].id).post(
        f"/erp/api/obras/{c['obra'].id}", json=CAMPOS_DO_CONTRATO)
    assert r.status_code == 200, r.get_data(as_text=True)[:200]

    lido = como(app_real, c["dono"].id).get(
        f"/erp/api/obras/{c['obra'].id}").get_json()["obra"]
    assert lido["seguro_garantia"] == CAMPOS_DO_CONTRATO["seguro_garantia"]
    assert lido["seguro_vigencia_fim"] == "2027-03-31"
    assert float(lido["caucao_pct"]) == 5.0
    assert lido["codigo_omie_depto"] == "DEP-VARREDURA"


def test_salvar_outra_aba_nao_apaga_o_seguro(cenario, app_real):
    """O DEFEITO ERA PIOR QUE 'não grava': o campo voltava vazio, e o
    salvamento seguinte mandava o vazio de volta — apagando o que existia."""
    c = cenario
    cli = como(app_real, c["dono"].id)
    cli.post(f"/erp/api/obras/{c['obra'].id}", json=CAMPOS_DO_CONTRATO)

    cli.post(f"/erp/api/obras/{c['obra'].id}", json={"nome": "Outro nome"})

    lido = cli.get(f"/erp/api/obras/{c['obra'].id}").get_json()["obra"]
    assert lido["seguro_garantia"] == CAMPOS_DO_CONTRATO["seguro_garantia"]
    assert float(lido["caucao_pct"]) == 5.0


def test_departamento_do_omie_repetido_e_recusado_com_recado(cenario, app_real):
    """A coluna é única no banco: sem esta trava o salvamento estouraria com
    erro de banco, e a pessoa leria 'falha do sistema'."""
    c = cenario
    outra = Obra(codigo="VARREDURA2", nome="Segunda obra", status="ATIVA")
    c["s"].add(outra)
    c["s"].flush()
    cli = como(app_real, c["dono"].id)
    cli.post(f"/erp/api/obras/{c['obra'].id}", json={"codigo_omie_depto": "DEP-X"})

    r = cli.post(f"/erp/api/obras/{outra.id}", json={"codigo_omie_depto": "DEP-X"})

    assert r.status_code == 400
    assert "já está na obra" in r.get_json()["erro"]


# ---------------------------------------------------------------------------
# 2. ENTRADA RUIM NÃO PODE VIRAR "FALHA DO SISTEMA"
#
# Erro 500 aparece para quem usa como *"Não consegui concluir. Isso é falha do
# sistema"* — o recado que assusta e não ensina nada.
# ---------------------------------------------------------------------------
def test_data_impossivel_na_obra_explica_em_vez_de_quebrar(cenario, app_real):
    c = cenario
    r = como(app_real, c["dono"].id).post(
        f"/erp/api/obras/{c['obra'].id}", json={"vigencia_inicio": "31/02/2026"})

    assert r.status_code == 400, "data errada é engano de digitação, não falha do sistema"
    assert "Data inválida" in r.get_json()["erro"]


def test_prazo_negativo_e_recusado(cenario, app_real):
    c = cenario
    r = como(app_real, c["dono"].id).post(
        f"/erp/api/obras/{c['obra'].id}", json={"prazo_execucao_dias": "-5"})

    assert r.status_code == 400
    assert "negativo" in r.get_json()["erro"]


def test_selecao_estragada_na_acao_em_lote_vira_recado(cenario, app_real):
    """Tela recarregada no meio manda id que não é número. Isso estourava."""
    c = cenario
    r = como(app_real, c["dono"].id).post(
        "/erp/api/titulos/acao", json={"acao": "aprovar", "ids": ["abc"]})

    assert r.status_code == 200
    assert r.get_json()["erros"], "tem de dizer que a seleção se perdeu"
    assert "Seleção inválida" in r.get_json()["erros"][0]["erro"]


def test_conciliacao_manual_sem_os_dois_lados_pede_os_dois(cenario, app_real):
    c = cenario
    r = como(app_real, c["dono"].id).post("/erp/api/conciliacao/manual", json={})

    assert r.status_code == 400
    assert "pagamento" in r.get_json()["erro"].lower()


# ---------------------------------------------------------------------------
# 3. COLABORADOR: o tipo da chave Pix, e tirar da obra
# ---------------------------------------------------------------------------
def test_o_tipo_da_chave_pix_volta_na_leitura(cenario):
    """Ele não voltava, e o formulário remontava sempre em "CPF" — o primeiro
    da lista. Quem tinha chave de telefone via o tipo trocado no primeiro
    salvamento, sem pedir. Chave com tipo errado é pagamento que não sai."""
    c = cenario
    salvar_colaborador(c["s"], {
        "nome": "Francisco das Chagas de Oliveira", "cpf": "52998224725",
        "obra_id": c["obra"].id, "situacao": "ATIVO",
        "pix_tipo": "TELEFONE", "pix_chave": "85988887777"}, c["dono"])
    c["s"].flush()

    lista = listar_colaboradores(c["s"], usuario=c["dono"])
    alvo = [x for x in lista if x["cpf"] == "52998224725"][0]
    assert alvo["pix_tipo"] == "TELEFONE"


def test_escolher_o_traco_tira_o_colaborador_da_obra(cenario):
    """Antes o valor vazio caía fora do `if` e o vínculo antigo ficava: não
    havia como tirar ninguém da obra pela tela."""
    c = cenario
    salvar_colaborador(c["s"], {
        "nome": "Jose Raimundo da Silva", "cpf": "11144477735",
        "obra_id": c["obra"].id, "situacao": "ATIVO"}, c["dono"])
    c["s"].flush()

    salvar_colaborador(c["s"], {
        "nome": "Jose Raimundo da Silva", "cpf": "11144477735",
        "obra_id": "", "situacao": "ATIVO"}, c["dono"])
    c["s"].flush()

    from sqlalchemy import select
    col = c["s"].scalars(select(Colaborador).where(
        Colaborador.cpf == "11144477735")).first()
    assert col.obra_id is None


def test_data_impossivel_no_colaborador_avisa_em_vez_de_sumir(cenario):
    """Ela virava None em silêncio: a pessoa via "salvo" e o campo ficava
    vazio, sem nada dizendo que aquilo não foi guardado."""
    c = cenario
    with pytest.raises(ErroValidacao) as e:
        salvar_colaborador(c["s"], {
            "nome": "Maria Aparecida Ferreira", "cpf": "22255588846",
            "admissao": "2026-13-45"}, c["dono"])
    assert "Data inválida" in str(e.value)


def test_data_vazia_no_colaborador_continua_sendo_vazia(cenario):
    """Vazio é vazio — só o ERRADO virou erro."""
    c = cenario
    col = salvar_colaborador(c["s"], {
        "nome": "Antonio Carlos de Sousa", "cpf": "33366699957",
        "admissao": ""}, c["dono"])
    assert col.admissao is None


# ---------------------------------------------------------------------------
# 4. A TELA QUE O PERFIL NÃO ABRE MOSTRA UMA TELA, NÃO CÓDIGO
#
# Antes a recusa vinha como JSON cru ocupando a janela inteira:
#   {"erro":"Seu perfil não tem permissão para esta operação.","ok":false}
# Sem menu, sem caminho de volta, com cara de defeito.
# ---------------------------------------------------------------------------
def test_tela_recusada_responde_pagina_de_gente(cenario, app_real):
    c = cenario
    dp = _pessoa(c["s"], "dp-varredura", P.DEPARTAMENTO_PESSOAL)

    r = como(app_real, dp.id).get("/erp/pagamentos")

    assert r.status_code == 403
    corpo = r.get_data(as_text=True)
    assert "<html" in corpo.lower(), "tem de ser uma página, não JSON cru"
    assert "não está liberada" in corpo
    assert "Voltar ao início" in corpo, "e tem de ter caminho de volta"


def test_chamada_de_api_recusada_continua_respondendo_json(cenario, app_real):
    """A tela e as integrações precisam do JSON — só a PESSOA precisa de tela."""
    c = cenario
    dp = _pessoa(c["s"], "dp-api-varredura", P.DEPARTAMENTO_PESSOAL)

    r = como(app_real, dp.id).get("/erp/api/pagamentos/agenda")

    assert r.status_code == 403
    assert r.get_json()["ok"] is False


# ---------------------------------------------------------------------------
# 5. O ENDEREÇO QUE NÃO É LINK CURTO NÃO SAI À INTERNET
#
# O curinga do encurtador pegava QUALQUER endereço de um pedaço só que nenhum
# dos 18 módulos reconheceu — inclusive /favicon.ico, que o navegador pede
# sozinho. Cada um virava uma consulta à planilha do Google e um erro 500.
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("caminho", ["/favicon.ico", "/robots.txt", "/sitemap.xml",
                                     "/algo.php", "/.env"])
def test_endereco_que_nao_e_codigo_curto_responde_404_na_hora(caminho, app_real):
    r = app_real.test_client().get(caminho)
    assert r.status_code == 404
    assert b"Link n" in r.data or b"Not Found" in r.data
