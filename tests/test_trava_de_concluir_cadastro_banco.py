# -*- coding: utf-8 -*-
"""
"CONCLUÍDO" É UMA AFIRMAÇÃO, E ELA TEM DE SER VERDADE.

⚑ POR QUE ESTE ARQUIVO EXISTE. Ele, em 17/11/2026, com o exemplo pronto:

> *"Imagina, eu recebo um cadastro para ser enviado à contabilidade. Aí eu pego
> aquele cadastro, eu simplesmente concluo ele, jogo para concluído. Se for uma
> pessoa que é CLT, fica errado, né? Eu não completei. E aí, tem trava para
> isso?"*

**Não tinha, e a culpa foi de uma leitura minha.** Horas antes, no mesmo dia,
ele pediu que mover de etapa fosse simples e ganhasse quadro (*"isso é
burrice"*, *"tem que ser simples"*) — e eu tirei a lista de saltos proibidos
INTEIRA. Simples não quer dizer sem trava: quer dizer sem atrito onde o atrito
não protege nada.

**A diferença, e é ela que decide o desenho:** mover do Inicial para
Solicitações é gesto livre, e travar isso só ensina a contornar. Dizer
"concluído" sobre uma carteira que a contabilidade não registrou é **afirmar um
fato falso** — e o cadastro passa a mentir para a folha e para o eSocial.

Por isso a trava é UMA, estreita: ela não protege "a ordem das etapas", protege
a frase "este cadastro está pronto". E ela **diz o que destrava**.
"""
from __future__ import annotations

import datetime as dt

import pytest
from sqlalchemy import text

from app.apps.erp.core.cadastros import pessoas as svc
from app.apps.erp.core.comum.auditoria import ErroValidacao

pytestmark = pytest.mark.banco

CPF_CLT = "52998224725"
CPF_PJ = "11144477735"
CPF_RPA = "12345678909"


@pytest.fixture
def dono(sessao_real):
    from app.apps.erp.core.auth.service import gerar_hash
    from app.apps.erp.db.models.cadastros import PerfilUsuario, Usuario
    u = Usuario(nome="DP da trava", email="dp@trava.teste", ativo=True,
                senha_hash=gerar_hash("senha-de-teste-123"),
                perfil=PerfilUsuario.DEPARTAMENTO_PESSOAL, telefone="5585900000002")
    sessao_real.add(u)
    sessao_real.flush()
    return u


def _pessoa(sessao_real, dono, cpf, nome, *, vinculo, tipo_contrato=None):
    c = svc.salvar(sessao_real, {
        "cpf": cpf, "nome": nome, "admissao": "2026-01-05",
        "vinculo": vinculo, "tipo_contrato": tipo_contrato or "",
        "etapa": "NA_CONTABILIDADE"}, dono)
    sessao_real.flush()
    return c


# ---------------------------------------------------------------------------
# O CASO QUE ELE DESCREVEU
# ---------------------------------------------------------------------------
def test_carteira_assinada_nao_conclui_sem_o_registro_formal(sessao_real, dono):
    c = _pessoa(sessao_real, dono, CPF_CLT, "Pessoa Com Carteira",
                vinculo="CTPS", tipo_contrato="CLT_INDETERMINADO")

    with pytest.raises(ErroValidacao) as e:
        svc.mover_etapa(sessao_real, c.id, "CONCLUIDO", dono)

    recado = str(e.value)
    assert "Assinatura de CTPS" in recado
    # ⚠️ A TRAVA TEM DE DIZER O QUE DESTRAVA. Trava que só recusa ensina a
    # contornar; esta nomeia o caminho.
    assert "Demandas" in recado and "contabilidade" in recado


def test_e_conclui_depois_que_o_pedido_a_contabilidade_e_concluido(sessao_real, dono):
    from app.apps.erp.core import contabilidade as svc_ct
    c = _pessoa(sessao_real, dono, CPF_CLT, "Pessoa Com Carteira",
                vinculo="CTPS", tipo_contrato="CLT_INDETERMINADO")
    # o cadastro precisa do que o modelo exige para o pedido poder sair
    svc.salvar(sessao_real, {"cpf": CPF_CLT, "nome": "Pessoa Com Carteira",
                             "data_nascimento": "1990-05-17",
                             "pis": "12056412545", "salario": "2.000,00"}, dono)
    sessao_real.flush()
    sessao_real.execute(text("""
        INSERT INTO dp_pedidos (numero, template, colaborador_id, situacao,
                                documento_esperado, aberto_por)
        VALUES ('CT-90001', 'ASSINATURA_CTPS', :c, 'CONCLUIDO', 'FICHA-REGISTRO', :u)"""),
        {"c": c.id, "u": dono.id})
    sessao_real.flush()

    svc.mover_etapa(sessao_real, c.id, "CONCLUIDO", dono)
    assert (c.etapa or "").upper() == "CONCLUIDO"
    assert svc_ct is not None            # o módulo é o dono da regra


# ---------------------------------------------------------------------------
# PEDIDO EM ABERTO TRAVA QUALQUER VÍNCULO
# ---------------------------------------------------------------------------
def test_pedido_a_contabilidade_em_aberto_impede_concluir(sessao_real, dono):
    """Concluir com pedido de pé é perder o rastro do que a contabilidade deve."""
    c = _pessoa(sessao_real, dono, CPF_RPA, "Prestador Autonomo",
                vinculo="PRESTADOR", tipo_contrato="AUTONOMO_RPA")
    sessao_real.execute(text("""
        INSERT INTO dp_pedidos (numero, template, colaborador_id, situacao,
                                documento_esperado, aberto_por)
        VALUES ('CT-90002', 'FERIAS', :c, 'ENVIADO', 'RECIBO-FERIAS', :u)"""),
        {"c": c.id, "u": dono.id})
    sessao_real.flush()

    with pytest.raises(ErroValidacao) as e:
        svc.mover_etapa(sessao_real, c.id, "CONCLUIDO", dono)
    assert "CT-90002" in str(e.value)


# ---------------------------------------------------------------------------
# PRESTADOR PJ PRECISA DO CONTRATO ARQUIVADO
# ---------------------------------------------------------------------------
def test_prestador_PJ_nao_conclui_sem_o_contrato_arquivado(sessao_real, dono):
    c = _pessoa(sessao_real, dono, CPF_PJ, "Prestador PJ",
                vinculo="PRESTADOR", tipo_contrato="PJ")

    with pytest.raises(ErroValidacao) as e:
        svc.mover_etapa(sessao_real, c.id, "CONCLUIDO", dono)
    assert "contrato" in str(e.value).lower()
    assert "Arquivos" in str(e.value)


# ---------------------------------------------------------------------------
# O QUE A TRAVA **NÃO** FAZ — e isto importa tanto quanto o que ela faz
# ---------------------------------------------------------------------------
def test_prestador_que_nao_e_PJ_conclui_direto(sessao_real, dono):
    """Prestador não-PJ "morre no DP": não tem contabilidade nem contrato, e
    travá-lo seria inventar trabalho que não existe."""
    c = _pessoa(sessao_real, dono, CPF_RPA, "Prestador Autonomo",
                vinculo="PRESTADOR", tipo_contrato="AUTONOMO_RPA")

    svc.mover_etapa(sessao_real, c.id, "CONCLUIDO", dono)
    assert (c.etapa or "").upper() == "CONCLUIDO"


def test_mover_entre_as_outras_etapas_continua_LIVRE(sessao_real, dono):
    """⚠️ A trava é só sobre "concluído". Ele pediu que mover fosse simples, e
    arrastar no quadro não pode virar um "não pode" a cada gesto."""
    c = _pessoa(sessao_real, dono, CPF_CLT, "Pessoa Com Carteira",
                vinculo="CTPS", tipo_contrato="CLT_INDETERMINADO")

    for destino in ("CADASTRO_INICIAL", "CONTRATO_A_ASSINAR",
                    "NA_CONTABILIDADE", "AFASTADO", "DESLIGADO", "CANCELADO"):
        svc.mover_etapa(sessao_real, c.id, destino, dono)
        assert (c.etapa or "").upper() == destino


def test_o_retorno_da_contabilidade_conclui_mesmo_com_a_trava(sessao_real, dono):
    """O pedido concluído move a etapa sozinho (`automatico=True`), e a trava
    não pode barrar o caminho que ela mesma mandou seguir."""
    c = _pessoa(sessao_real, dono, CPF_CLT, "Pessoa Com Carteira",
                vinculo="CTPS", tipo_contrato="CLT_INDETERMINADO")

    svc.mover_etapa(sessao_real, c.id, "CONCLUIDO", dono, automatico=True,
                    motivo="retorno da contabilidade")
    assert (c.etapa or "").upper() == "CONCLUIDO"


def test_a_conferencia_concluida_nao_cai_na_trava(sessao_real, dono):
    """"Conferência concluída" manda CTPS para a contabilidade, não para
    concluído — então a trava nunca a alcança. Se um dia alcançar, o DP fica
    sem jeito de terminar a conferência."""
    c = _pessoa(sessao_real, dono, CPF_CLT, "Pessoa Com Carteira",
                vinculo="CTPS", tipo_contrato="CLT_INDETERMINADO")
    svc.mover_etapa(sessao_real, c.id, "CADASTRO_INICIAL", dono)

    r = svc.concluir_conferencia(sessao_real, c.id, dono)
    assert r["etapa"] == "NA_CONTABILIDADE"
