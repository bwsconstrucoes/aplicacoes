# -*- coding: utf-8 -*-
"""
DUAS DECISÕES DELE EM 10/11/2026, e as duas são travas de verdade.

**1. A CONFERÊNCIA DO DP NÃO PODE TRAVAR O PONTO.**

> *"Eu não quero que a conferência impacte no ponto, porque eu quero que a
> pessoa cadastre e, a partir daquele dia, ele já consiga bater ponto no ato
> do cadastro. Fica lá para o DP terminar de conferir (…) quem diz que é para
> admitir ou não é a obra. O DP vai intermediar ali para ver se a pessoa está
> no cargo correto, se o dia da assinatura vai ser aquele mesmo ou um
> pouquinho mais na frente. Mas a obra pediu, libera."*

⚑ **ETAPA ≠ SITUAÇÃO, e é aqui que a distinção ganha dente.** `etapa` diz "o
que falta fazer neste cadastro" e é trabalho do DP; `situacao` diz "esta
pessoa está na empresa" e manda no ponto e na folha. No dia em que alguém
ligar uma na outra — por zelo, achando que cadastro não conferido não devia
bater ponto —, a obra para de registrar quem está trabalhando. Estes testes
são a rede.

**2. OS ACRÉSCIMOS DA DIÁRIA SÃO CONFIGURAÇÃO.**

> *"Esses números ainda valem atualmente, mas isso é algo que deva ficar
> dentro de uma configuração, o acréscimo da diária."*
"""
from __future__ import annotations

import datetime as dt
from decimal import Decimal

import pytest

from app.apps.erp.core import diarias
from app.apps.erp.core.cadastros import pessoas as svc
from app.apps.erp.core.comum.auditoria import ErroValidacao

banco_ = pytest.mark.banco


@pytest.fixture
def dono(sessao_real):
    """Quem opera: um ADMIN, porque esta fila mexe em cadastro e em parâmetro
    de pagamento."""
    from app.apps.erp.core.auth.service import gerar_hash
    from app.apps.erp.db.models.cadastros import PerfilUsuario, Usuario
    u = Usuario(nome="Dona do sistema", email="admin.diarias@teste", ativo=True,
                senha_hash=gerar_hash("senha-de-teste-123"),
                perfil=PerfilUsuario.ADMIN, telefone="5585900000030")
    sessao_real.add(u)
    sessao_real.flush()
    return u


# ===========================================================================
# 1. A CONFERÊNCIA NÃO TRAVA O PONTO
# ===========================================================================
def test_o_ponto_nao_olha_a_ETAPA_do_cadastro():
    """⚑ A TRAVA ESTRUTURAL. Se alguém escrever `etapa` na regra de quem bate
    ponto, este teste quebra — e é para quebrar."""
    import inspect
    from app.apps.ponto.core import cadastros, marcacoes
    for modulo in (marcacoes, cadastros):
        fonte = inspect.getsource(modulo)
        # "etapa" pode aparecer em comentário; o que não pode é em SQL ou numa
        # comparação. Procuramos o nome da COLUNA do cadastro.
        for proibido in ("c.etapa", "colaboradores.etapa", '"etapa"', "'etapa'"):
            assert proibido not in fonte, (
                f"{modulo.__name__} passou a olhar a ETAPA do cadastro. A "
                f"conferência do DP não pode travar o ponto: *\"a obra pediu, "
                f"libera\"*.")


def test_quem_nasce_em_conferencia_nasce_ATIVO():
    """A pessoa cadastrada hoje está ATIVA hoje. A etapa fica em
    `CADASTRO_INICIAL` para o DP conferir, e isso não muda a situação."""
    from app.apps.erp.db.models.cadastros import Colaborador
    coluna = Colaborador.__table__.columns["situacao"]
    assert coluna.default.arg == "ATIVO"


def test_o_inicio_do_contrato_no_ponto_e_a_MENOR_das_duas_datas():
    """⚑ O BURACO QUE ESTA DECISÃO REVELOU.

    `data_inicio` é o primeiro dia na obra; `admissao` é o dia do registro, e
    elas podem ser semanas diferentes — é o DP ponderando *"se o dia da
    assinatura vai ser aquele mesmo ou um pouquinho mais na frente"*.

    Sem o `LEAST`, bastava o DP empurrar a data de admissão para a frente para
    alguém que vinha batendo ponto PARAR DE BATER, sem ninguém entender por
    quê. O trabalho já aconteceu; mudar a data do papel não apaga o direito de
    registrar o dia.
    """
    from app.apps.ponto.core.cadastros import _DO_ERP
    assert "LEAST" in _DO_ERP["admissao"], (
        "o caminho do ERP voltou a usar só a data de admissão — quem começou "
        "antes da carteira perde o ponto dos dias já trabalhados")
    assert "data_inicio" in _DO_ERP["admissao"]


def test_antes_do_primeiro_dia_ainda_nao_bate():
    """O que a liberação NÃO é: carta branca. Ponto antes do primeiro dia
    continua recusado — senão o cadastro de hoje valeria para o mês passado."""
    from app.apps.ponto.core.marcacoes import fora_do_contrato
    pessoa = {"admissao": dt.date(2026, 11, 10), "demissao": None}
    assert fora_do_contrato(pessoa, dt.date(2026, 11, 9))
    assert fora_do_contrato(pessoa, dt.date(2026, 11, 10)) is None


@banco_
def test_cadastro_novo_ja_bate_ponto_no_mesmo_dia(sessao_real, dono):
    """O caminho inteiro, como ele descreveu: a obra cadastra hoje, a pessoa
    bate hoje, e o cadastro fica em conferência."""
    from app.apps.ponto.core.marcacoes import fora_do_contrato
    hoje = dt.date.today()
    c = svc.salvar(sessao_real, {"cpf": "52998224725", "nome": "Entrou Hoje",
                                 "data_inicio": hoje.isoformat(),
                                 "admissao": hoje.isoformat()}, dono)
    sessao_real.flush()
    assert c.situacao == "ATIVO"
    assert c.etapa == "CADASTRO_INICIAL"      # o DP ainda vai conferir
    # E o ponto de hoje passa.
    assert fora_do_contrato({"admissao": c.admissao, "demissao": c.demissao},
                            hoje) is None


@banco_
def test_adiar_a_assinatura_nao_tira_o_ponto_ja_trabalhado(sessao_real, dono):
    """A ponderação do DP — *"se o dia da assinatura vai ser um pouquinho mais
    na frente"* — não pode apagar dias já trabalhados."""
    from app.apps.ponto.core.marcacoes import fora_do_contrato
    comecou = dt.date(2026, 11, 2)
    c = svc.salvar(sessao_real, {"cpf": "52998224725", "nome": "Comecou Antes",
                                 "data_inicio": comecou.isoformat(),
                                 "admissao": comecou.isoformat()}, dono)
    # O DP empurra a assinatura duas semanas.
    c.admissao = dt.date(2026, 11, 16)
    sessao_real.flush()
    # O início do contrato PARA O PONTO é a menor das duas — é o que a consulta
    # do ponto faz com LEAST, e aqui o equivalente em Python.
    inicio_no_ponto = min(d for d in (c.data_inicio, c.admissao) if d)
    assert inicio_no_ponto == comecou
    assert fora_do_contrato({"admissao": inicio_no_ponto, "demissao": None},
                            dt.date(2026, 11, 5)) is None


# ===========================================================================
# 2. OS ACRÉSCIMOS DA DIÁRIA
# ===========================================================================
@banco_
def test_sem_nada_configurado_valem_os_numeros_de_hoje(sessao_real):
    """⚑ Configuração nova que muda o resultado no primeiro dia é armadilha: o
    número muda, ninguém mexeu em nada, e a desconfiança cai no sistema."""
    c = diarias.configuracao(sessao_real)
    assert c["feriado"] == 20.0 and c["sabado"] == 10.0 and c["domingo"] == 20.0
    assert c["sem_diaria"] == ["VIGIA"]
    assert c["no_padrao"] is True


@banco_
def test_o_que_foi_configurado_passa_a_valer(sessao_real, dono):
    diarias.configurar(sessao_real, {"feriado": "25,00", "sabado": "12",
                                     "domingo": "25,00",
                                     "sem_diaria": "VIGIA, PORTEIRO"}, dono)
    c = diarias.configuracao(sessao_real)
    assert c["feriado"] == 25.0 and c["sabado"] == 12.0
    assert c["sem_diaria"] == ["VIGIA", "PORTEIRO"]
    assert c["no_padrao"] is False


@banco_
def test_feriado_VENCE_o_dia_da_semana_e_nao_soma(sessao_real):
    """Feriado que cai no sábado paga o acréscimo de feriado, e só. Somar
    seria pagar duas vezes pela mesma condição."""
    # 2026-11-14 é um sábado.
    assert diarias.acrescimo_do_dia(sessao_real, feriado=True,
                                    dia_da_semana=5) == Decimal("20.00")
    assert diarias.acrescimo_do_dia(sessao_real, feriado=False,
                                    dia_da_semana=5) == Decimal("10.00")
    assert diarias.acrescimo_do_dia(sessao_real, feriado=False,
                                    dia_da_semana=2) == Decimal("0.00")


@banco_
def test_vigia_noturno_continua_recebendo_diaria(sessao_real):
    """Igualdade, nunca "contém": trocar por "começa com" tiraria o pagamento
    de um turno inteiro sem ninguém pedir."""
    assert diarias.recebe_diaria(sessao_real, "VIGIA") is False
    assert diarias.recebe_diaria(sessao_real, "vigia") is False
    assert diarias.recebe_diaria(sessao_real, "VIGIA NOTURNO") is True
    assert diarias.recebe_diaria(sessao_real, "PEDREIRO") is True


@banco_
def test_acrescimo_absurdo_e_recusado_com_o_motivo(sessao_real, dono):
    """Acréscimo não é a diária. Um valor alto demais é quase certamente o
    valor do DIA digitado no campo errado — e pagá-lo dobraria o domingo de
    todo mundo."""
    with pytest.raises(ErroValidacao, match="campo errado"):
        diarias.configurar(sessao_real, {"domingo": "1500,00"}, dono)


@banco_
def test_valor_negativo_e_recusado(sessao_real, dono):
    with pytest.raises(ErroValidacao, match="negativo"):
        diarias.configurar(sessao_real, {"sabado": "-10"}, dono)


@banco_
def test_a_folha_de_diaristas_le_a_configuracao_do_ERP(sessao_real, dono):
    """⚑ A PONTE ENTRE AS DUAS ÁREAS. Sem este teste, a tela de configuração
    existiria e a conta continuaria com os números do código — o pior dos
    mundos, porque alguém mudaria o número e confiaria."""
    from app.apps.analisesps import folha_diaristas
    import datetime as _dt
    diarias.configurar(sessao_real, {"feriado": "33,00"}, dono)
    try:
        lidos = folha_diaristas.acrescimos_configurados(sessao_real)
        assert lidos["feriado"] == Decimal("33.00")
        valor, porque = folha_diaristas.adicional_do_dia(
            _dt.date(2026, 11, 2), True, lidos)
        assert valor == Decimal("33.00") and porque == "feriado"
    finally:
        diarias.configurar(sessao_real, {"feriado": "20,00"}, dono)
