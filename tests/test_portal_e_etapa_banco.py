"""O QUE ELE PEDIU EM 17/11/2026, na fila de ajustes do portal e do cadastro.

Dois assuntos, os dois com banco de verdade porque os dois vivem em `WHERE`:

  1. **A busca da admissão de prestador só oferece prestador.** Ele: *"na
     admissão de prestador, na busca aparecer somente os que são Prestador de
     Serviço."* É pedido que não tem como dar certo com a pessoa errada —
     admitir em carteira quem já está em carteira não existe.

  2. **A situação passou a vir da ETAPA.** Ele: *"dentro de vínculo e
     pagamento, a situação é um reflexo das etapas. Não precisa ter mais esse
     campo."*

     ⚠️ ESTA É A PARTE COM RISCO, e é a razão de o teste existir: até 17/11
     mover a etapa NÃO mexia na situação. Tirar o campo sem fechar esse laço
     deixaria o sistema sem jeito de desligar ninguém — e `situacao` é o que o
     PONTO lê para decidir quem pode bater, e o que a mensageria lê para não
     mandar aviso a quem saiu. Alguém arrastado para "Desligado" no quadro
     continuaria batendo ponto.
"""
from __future__ import annotations

import pytest
from sqlalchemy import text

from app.apps.erp.core import portal as svc_portal
from app.apps.erp.core.cadastros import pessoas as svc
from app.apps.erp.db.models.cadastros import (
    Colaborador, Obra, PerfilUsuario as P, Usuario, UsuarioObra,
)

pytestmark = pytest.mark.banco


@pytest.fixture
def cena(sessao_real):
    from app.apps.erp.core.auth.service import gerar_hash
    s = sessao_real
    obra = Obra(codigo="PRT-1", nome="Obra do Portal", status="ATIVA")
    s.add(obra)
    s.flush()
    u = Usuario(nome="Administrativo", email="adm.portal@teste.com",
                senha_hash=gerar_hash("x" * 12), ativo=True,
                perfil=P.ADMINISTRATIVO_OBRA)
    s.add(u)
    s.flush()
    s.add(UsuarioObra(usuario_id=u.id, obra_id=obra.id))
    pessoas = {}
    for nome, cpf, vinculo in (("ZE TESTE PRESTADOR", "39053344705", "PRESTADOR"),
                               ("ANA TESTE CARTEIRA", "11144477735", "CTPS")):
        c = Colaborador(nome=nome, cpf=cpf, situacao="ATIVO", obra_id=obra.id,
                        vinculo=vinculo, etapa="CONCLUIDO")
        s.add(c)
        s.flush()
        pessoas[vinculo] = c
    return {"s": s, "obra": obra, "usuario": u, **pessoas}


# ---------------------------------------------------------------------------
# 1. A busca da admissão de prestador
# ---------------------------------------------------------------------------
def test_a_admissao_de_prestador_so_oferece_prestador(cena):
    achados = svc_portal.procurar_pessoas(
        cena["s"], cena["usuario"], "teste", so_vinculo="PRESTADOR")

    nomes = {p["nome"] for p in achados}
    assert "ZE TESTE PRESTADOR" in nomes
    assert "ANA TESTE CARTEIRA" not in nomes, (
        "quem já tem carteira não pode aparecer: admiti-la em carteira de novo "
        "não é operação que exista")


def test_sem_filtro_a_busca_oferece_os_dois(cena):
    nomes = {p["nome"] for p in svc_portal.procurar_pessoas(
        cena["s"], cena["usuario"], "teste")}

    assert {"ZE TESTE PRESTADOR", "ANA TESTE CARTEIRA"} <= nomes


def test_quem_decide_o_filtro_e_a_REGRA_e_nao_a_tela(cena):
    """A tela manda a CHAVE do tipo; o servidor olha em `TIPOS`. Se a tela
    mandasse o vínculo, bastaria trocá-lo no navegador para ver todo mundo."""
    assert svc_portal.vinculo_do_tipo("ADMISSAO_PRESTADOR") == "PRESTADOR"
    assert svc_portal.vinculo_do_tipo("CADASTRO") == ""
    assert svc_portal.vinculo_do_tipo("inventado") == ""
    assert svc_portal.vinculo_do_tipo("") == ""


# ---------------------------------------------------------------------------
# 2. A situação vem da etapa
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("etapa, situacao", [
    ("DESLIGADO", "DESLIGADO"),
    ("CANCELADO", "DESLIGADO"),
    ("AFASTADO", "AFASTADO"),
    ("CONCLUIDO", "ATIVO"),
    ("CADASTRO_INICIAL", "ATIVO"),
    ("CONTRATO_A_ASSINAR", "ATIVO"),
    ("NA_CONTABILIDADE", "ATIVO"),
])
def test_mover_a_etapa_define_a_situacao(cena, etapa, situacao):
    c = cena["CTPS"]

    svc.mover_etapa(cena["s"], c.id, etapa, cena["usuario"], motivo="teste")

    assert c.situacao == situacao, (
        f"etapa {etapa} tem de deixar a situação em {situacao} — é a situação "
        f"que o ponto lê para decidir quem bate")


def test_desligar_pelo_quadro_tira_a_pessoa_do_ponto(cena):
    """O caminho inteiro do gesto dele: arrastar para "Desligado" no quadro tem
    de ter o MESMO efeito que o desligamento tinha pelo campo que saiu."""
    c = cena["PRESTADOR"]
    assert c.situacao == "ATIVO"

    svc.mover_etapa(cena["s"], c.id, "DESLIGADO", cena["usuario"],
                    motivo="movido no quadro")
    cena["s"].flush()

    linha = cena["s"].execute(text(
        "SELECT situacao, etapa FROM colaboradores WHERE id = :i"),
        {"i": c.id}).mappings().first()
    assert linha["situacao"] == "DESLIGADO" and linha["etapa"] == "DESLIGADO"


def test_voltar_para_uma_etapa_ativa_reativa(cena):
    """Readmissão e volta de afastamento: a situação acompanha de volta."""
    c = cena["CTPS"]
    svc.mover_etapa(cena["s"], c.id, "DESLIGADO", cena["usuario"])
    assert c.situacao == "DESLIGADO"

    svc.mover_etapa(cena["s"], c.id, "CADASTRO_INICIAL", cena["usuario"])

    assert c.situacao == "ATIVO"


def test_toda_etapa_tem_situacao(cena):
    """Etapa nova sem situação deixaria o cadastro com a situação velha — e
    ninguém perceberia até alguém não conseguir bater ponto."""
    for etapa in svc.ETAPAS:
        assert etapa in svc.SITUACAO_DA_ETAPA, etapa
        assert svc.SITUACAO_DA_ETAPA[etapa] in ("ATIVO", "AFASTADO", "DESLIGADO")
