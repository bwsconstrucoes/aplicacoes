"""AFASTAMENTO E RETORNO — com banco de verdade.

Pergunta dele em 18/11/2026: *"Colaborador estava afastado e vai retornar ao
trabalho. O que deve ser exigido dele e o que o sistema deve pedir pra travar o
simples retorno?"*

POR QUE `@pytest.mark.banco` e não o dublê: a trava do retorno é toda `WHERE` —
o ASO que vale é o emitido A PARTIR do início do afastamento (senão o da
admissão passaria), o afastamento aberto é o que tem `data_retorno IS NULL`, e o
índice único parcial é que garante um só por pessoa. A `SessaoFalsa` ignora
`WHERE` e aprovaria todas essas coisas erradas.

Cobre, nesta ordem:
  1. afastar exige tipo, e o tipo do passado não se escolhe;
  2. arrastar o cartão para “Afastado” é RECUSADO — a porta é uma só;
  3. a volta é recusada sem o ASO de retorno (decisão dele: *"tem que travar"*);
  4. o ASO da ADMISSÃO não serve — tem de ser novo;
  5. afastamento curto não exige exame (a lei pede de 30 dias em diante);
  6. acidente de trabalho deixa 12 meses de estabilidade a contar da VOLTA;
  7. maternidade conta os 5 meses a partir do INÍCIO, não da volta;
  8. arrastar o cartão para fora de “Afastado” é recusado;
  9. desligar quem está afastado CONTINUA livre — e a estabilidade é aviso;
 10. o afastamento antigo (sem natureza) só volta depois de dizer o tipo.
"""
from __future__ import annotations

import datetime as dt

import pytest
from sqlalchemy import text

from app.apps.erp.core.cadastros import afastamentos as svc
from app.apps.erp.core.cadastros import pessoas as svc_pessoas
from app.apps.erp.core.comum.auditoria import ErroValidacao
from app.apps.erp.db.models.cadastros import Colaborador, Obra

pytestmark = pytest.mark.banco

HOJE = dt.date.today()


@pytest.fixture
def obra(sessao_real):
    o = Obra(codigo="OBRAF", nome="Obra do Afastamento", status="ATIVA")
    sessao_real.add(o)
    sessao_real.flush()
    return o


@pytest.fixture
def pessoa(sessao_real, obra):
    c = Colaborador(nome="JOANA QUE SE AFASTOU", cpf="52998224725",
                    situacao="ATIVO", etapa="CONCLUIDO", obra_id=obra.id,
                    vinculo="CTPS", admissao=HOJE - dt.timedelta(days=900))
    sessao_real.add(c)
    sessao_real.flush()
    return c


def _anexo(sessao_real) -> int:
    """Um anexo qualquer: `documentos.anexo_id` é NOT NULL."""
    import uuid
    return sessao_real.execute(text("""
        INSERT INTO anexos (entidade_tipo, entidade_id, nome_arquivo,
                            hash_sha256, tamanho_bytes, mime_type, conteudo)
        VALUES ('colaborador', 1, 'aso.pdf', :h, 10, 'application/pdf',
                '\\x00'::bytea)
        RETURNING id"""), {"h": uuid.uuid4().hex}).scalar_one()


def _aso(sessao_real, colaborador_id: int, emissao: dt.date,
         tipo: str = "ASO") -> None:
    sessao_real.execute(text("""
        INSERT INTO documentos (tipo_codigo, anexo_id, nome_padronizado,
                                colaborador_id, emissao, origem)
        VALUES (:t, :a, :n, :c, :e, 'TESTE')"""),
        {"t": tipo, "a": _anexo(sessao_real),
         "n": f"{tipo} {emissao:%Y-%m-%d}", "c": colaborador_id, "e": emissao})
    sessao_real.flush()


# ---------------------------------------------------------------------------
# 1. afastar exige o tipo
# ---------------------------------------------------------------------------
def test_afastar_sem_tipo_e_recusado(sessao_real, pessoa):
    with pytest.raises(ErroValidacao) as e:
        svc.abrir(sessao_real, pessoa.id,
                  {"data_inicio": HOJE.isoformat()}, None)
    assert "tipo" in str(e.value).lower()


def test_o_tipo_do_passado_nao_se_escolhe(sessao_real, pessoa):
    """`NAO_INFORMADO` existe para a migração, não para a tela."""
    with pytest.raises(ErroValidacao):
        svc.abrir(sessao_real, pessoa.id,
                  {"tipo": "NAO_INFORMADO", "data_inicio": HOJE.isoformat()},
                  None)
    assert "NAO_INFORMADO" not in svc.TIPOS_NOVOS


def test_dois_afastamentos_abertos_nao_existem(sessao_real, pessoa):
    svc.abrir(sessao_real, pessoa.id,
              {"tipo": "INSS", "data_inicio": HOJE.isoformat()}, None)
    with pytest.raises(ErroValidacao) as e:
        svc.abrir(sessao_real, pessoa.id,
                  {"tipo": "ATESTADO", "data_inicio": HOJE.isoformat()}, None)
    assert "em curso" in str(e.value)


# ---------------------------------------------------------------------------
# 2. a porta é uma só: o quadro não afasta
# ---------------------------------------------------------------------------
def test_arrastar_o_cartao_para_afastado_e_recusado(sessao_real, pessoa):
    with pytest.raises(ErroValidacao) as e:
        svc_pessoas.mover_etapa(sessao_real, pessoa.id, "AFASTADO", None)
    assert "tipo" in str(e.value).lower()
    assert (sessao_real.get(Colaborador, pessoa.id).etapa or "") == "CONCLUIDO"


def test_afastar_leva_a_etapa_e_tira_a_pessoa_da_ativa(sessao_real, pessoa):
    svc.abrir(sessao_real, pessoa.id,
              {"tipo": "ACIDENTE", "data_inicio": HOJE.isoformat(),
               "motivo": "queda de andaime"}, None)
    c = sessao_real.get(Colaborador, pessoa.id)
    assert c.etapa == "AFASTADO"
    # ⚠️ É esta coluna que o PONTO lê para decidir quem pode bater.
    assert c.situacao == "AFASTADO"


# ---------------------------------------------------------------------------
# 3 e 4. O ASO DE RETORNO TRAVA — e tem de ser novo
# ---------------------------------------------------------------------------
def test_a_volta_e_recusada_sem_o_aso_de_retorno(sessao_real, pessoa):
    inicio = HOJE - dt.timedelta(days=60)
    svc.abrir(sessao_real, pessoa.id,
              {"tipo": "INSS", "data_inicio": inicio.isoformat()}, None)

    with pytest.raises(ErroValidacao) as e:
        svc.retornar(sessao_real, pessoa.id,
                     {"data_retorno": HOJE.isoformat()}, None)

    assert "ASO de retorno" in str(e.value)
    assert sessao_real.get(Colaborador, pessoa.id).etapa == "AFASTADO"


def test_o_aso_DA_ADMISSAO_nao_serve_para_voltar(sessao_real, pessoa):
    """O da admissão está arquivado desde sempre; aceitá-lo faria a trava passar
    sem ninguém examinar ninguém."""
    inicio = HOJE - dt.timedelta(days=60)
    _aso(sessao_real, pessoa.id, inicio - dt.timedelta(days=800))  # admissional
    svc.abrir(sessao_real, pessoa.id,
              {"tipo": "INSS", "data_inicio": inicio.isoformat()}, None)

    with pytest.raises(ErroValidacao) as e:
        svc.retornar(sessao_real, pessoa.id,
                     {"data_retorno": HOJE.isoformat()}, None)
    assert "ASO de retorno" in str(e.value)


def test_com_o_aso_novo_a_volta_acontece(sessao_real, pessoa):
    inicio = HOJE - dt.timedelta(days=60)
    svc.abrir(sessao_real, pessoa.id,
              {"tipo": "INSS", "data_inicio": inicio.isoformat()}, None)
    _aso(sessao_real, pessoa.id, HOJE - dt.timedelta(days=1))

    r = svc.retornar(sessao_real, pessoa.id,
                     {"data_retorno": HOJE.isoformat()}, None)

    assert r["ok"] and r["dias"] == 60
    c = sessao_real.get(Colaborador, pessoa.id)
    assert c.etapa == "CONCLUIDO" and c.situacao == "ATIVO"
    assert svc.aberto(sessao_real, pessoa.id) is None


# ---------------------------------------------------------------------------
# 5. afastamento curto não exige exame — a lei pede de 30 dias em diante
# ---------------------------------------------------------------------------
def test_afastamento_curto_volta_sem_exame(sessao_real, pessoa):
    inicio = HOJE - dt.timedelta(days=5)
    svc.abrir(sessao_real, pessoa.id,
              {"tipo": "ATESTADO", "data_inicio": inicio.isoformat()}, None)

    falta = svc.o_que_falta_para_voltar(sessao_real, pessoa.id)
    assert falta["impede"] == []
    # … e a tela DIZ que não está exigindo, e a partir de quando exigiria.
    assert any("30" in a for a in falta["avisos"])

    assert svc.retornar(sessao_real, pessoa.id,
                        {"data_retorno": HOJE.isoformat()}, None)["ok"]


def test_a_trava_usa_a_data_da_VOLTA_e_nao_hoje(sessao_real, pessoa):
    """Quem registra uma volta de ontem é conferido pelos dias de ontem.

    Sem isto, um afastamento de 29 dias cuja volta se registra uma semana
    depois passaria a exigir exame por causa da demora do DP em digitar.
    """
    inicio = HOJE - dt.timedelta(days=35)
    svc.abrir(sessao_real, pessoa.id,
              {"tipo": "ATESTADO", "data_inicio": inicio.isoformat()}, None)
    volta = inicio + dt.timedelta(days=29)        # voltou no 29º dia

    assert svc.o_que_falta_para_voltar(
        sessao_real, pessoa.id, data_retorno=volta)["impede"] == []
    assert svc.retornar(sessao_real, pessoa.id,
                        {"data_retorno": volta.isoformat()}, None)["ok"]


# ---------------------------------------------------------------------------
# 6 e 7. ESTABILIDADE — o item que custa dinheiro
# ---------------------------------------------------------------------------
def test_acidente_de_trabalho_deixa_12_meses_a_contar_da_volta(sessao_real, pessoa):
    inicio = HOJE - dt.timedelta(days=90)
    svc.abrir(sessao_real, pessoa.id,
              {"tipo": "ACIDENTE", "data_inicio": inicio.isoformat()}, None)
    _aso(sessao_real, pessoa.id, HOJE)

    r = svc.retornar(sessao_real, pessoa.id,
                     {"data_retorno": HOJE.isoformat()}, None)

    assert r["estabilidade_ate"] == svc._mais_meses(HOJE, 12).isoformat()
    est = svc.estabilidade(sessao_real, pessoa.id)
    assert est and est["ate"] == r["estabilidade_ate"]
    # A tela não promete ser a fonte oficial — ele: "a contabilidade conta".
    assert "contabilidade" in est["aviso"]


def test_maternidade_conta_os_5_meses_do_INICIO_nao_da_volta(sessao_real, pessoa):
    """A estabilidade da maternidade é a partir do PARTO. Contar da volta daria
    5 meses a mais do que a lei dá — e a empresa deixaria de poder desligar
    alguém que já podia."""
    inicio = HOJE - dt.timedelta(days=120)
    svc.abrir(sessao_real, pessoa.id,
              {"tipo": "MATERNIDADE", "data_inicio": inicio.isoformat()}, None)
    _aso(sessao_real, pessoa.id, HOJE)

    r = svc.retornar(sessao_real, pessoa.id,
                     {"data_retorno": HOJE.isoformat()}, None)

    assert r["estabilidade_ate"] == svc._mais_meses(inicio, 5).isoformat()


def test_inss_comum_nao_deixa_estabilidade(sessao_real, pessoa):
    inicio = HOJE - dt.timedelta(days=60)
    svc.abrir(sessao_real, pessoa.id,
              {"tipo": "INSS", "data_inicio": inicio.isoformat()}, None)
    _aso(sessao_real, pessoa.id, HOJE)

    r = svc.retornar(sessao_real, pessoa.id,
                     {"data_retorno": HOJE.isoformat()}, None)

    assert r["estabilidade_ate"] is None
    assert svc.estabilidade(sessao_real, pessoa.id) is None


def test_a_estabilidade_vencida_nao_aparece_mais(sessao_real, pessoa):
    inicio = HOJE - dt.timedelta(days=900)
    volta = inicio + dt.timedelta(days=90)        # voltou há 810 dias
    svc.abrir(sessao_real, pessoa.id,
              {"tipo": "ACIDENTE", "data_inicio": inicio.isoformat()}, None)
    _aso(sessao_real, pessoa.id, volta)
    r = svc.retornar(sessao_real, pessoa.id,
                     {"data_retorno": volta.isoformat()}, None)

    # a estabilidade existiu (12 meses da volta) e JÁ VENCEU há mais de um ano
    assert r["estabilidade_ate"] == svc._mais_meses(volta, 12).isoformat()
    assert svc.estabilidade(sessao_real, pessoa.id) is None


# ---------------------------------------------------------------------------
# 8 e 9. sair de “Afastado”: pelo retorno, sim; arrastando, não. Desligar, sim.
# ---------------------------------------------------------------------------
def test_arrastar_para_fora_de_afastado_e_recusado(sessao_real, pessoa):
    svc.abrir(sessao_real, pessoa.id,
              {"tipo": "INSS", "data_inicio": HOJE.isoformat()}, None)

    with pytest.raises(ErroValidacao) as e:
        svc_pessoas.mover_etapa(sessao_real, pessoa.id, "CONCLUIDO", None)

    assert "retorno" in str(e.value).lower()
    assert sessao_real.get(Colaborador, pessoa.id).etapa == "AFASTADO"


def test_desligar_quem_esta_afastado_CONTINUA_livre(sessao_real, pessoa):
    """Há desligamento que cabe na estabilidade (pedido da pessoa, justa causa).
    Travar prenderia gente num limbo — a estabilidade é aviso, não trava."""
    svc.abrir(sessao_real, pessoa.id,
              {"tipo": "ACIDENTE", "data_inicio": HOJE.isoformat()}, None)

    svc_pessoas.mover_etapa(sessao_real, pessoa.id, "DESLIGADO", None)

    assert sessao_real.get(Colaborador, pessoa.id).etapa == "DESLIGADO"


# ---------------------------------------------------------------------------
# 10. o afastamento antigo, sem natureza registrada
# ---------------------------------------------------------------------------
def test_afastamento_antigo_so_volta_depois_de_dizer_o_tipo(sessao_real, pessoa):
    """É o caso de quem já estava em “Afastado” quando o tipo passou a ser
    pedido (migração 111). Ele NÃO fica preso: diz o tipo e volta."""
    sessao_real.execute(text("""
        INSERT INTO colaborador_afastamentos (colaborador_id, tipo, data_inicio)
        VALUES (:c, 'NAO_INFORMADO', :i)"""),
        {"c": pessoa.id, "i": HOJE - dt.timedelta(days=100)})
    pessoa.etapa, pessoa.situacao = "AFASTADO", "AFASTADO"
    sessao_real.flush()

    falta = svc.o_que_falta_para_voltar(sessao_real, pessoa.id)
    assert any("tipo" in x.lower() for x in falta["impede"])

    # dizendo o tipo, a conferência passa a valer — e agora pede o exame
    with pytest.raises(ErroValidacao) as e:
        svc.retornar(sessao_real, pessoa.id,
                     {"data_retorno": HOJE.isoformat(), "tipo": "INSS"}, None)
    assert "ASO de retorno" in str(e.value)

    _aso(sessao_real, pessoa.id, HOJE)
    assert svc.retornar(sessao_real, pessoa.id,
                        {"data_retorno": HOJE.isoformat(), "tipo": "INSS"},
                        None)["ok"]


def test_datas_incoerentes_sao_recusadas(sessao_real, pessoa):
    svc.abrir(sessao_real, pessoa.id,
              {"tipo": "INSS", "data_inicio": HOJE.isoformat()}, None)
    with pytest.raises(ErroValidacao) as e:
        svc.retornar(sessao_real, pessoa.id,
                     {"data_retorno": (HOJE - dt.timedelta(days=10)).isoformat()},
                     None)
    assert "antes do início" in str(e.value)


# ---------------------------------------------------------------------------
# ⚑ O VIGIA AVISA ANTES DA VOLTA (18/11/2026)
#
# A razão de olhar a PREVISÃO e não a volta: o exame de retorno tem de estar
# pronto ANTES de a pessoa aparecer na obra. Descobrir no dia é descobrir
# tarde — ou ela trabalha sem exame (ilegal, e é o caso em que um acidente
# custa caro) ou volta para casa.
# ---------------------------------------------------------------------------
def test_o_vigia_avisa_da_volta_que_esta_chegando(sessao_real, pessoa):
    from app.apps.erp.core import agente_dp
    svc.abrir(sessao_real, pessoa.id,
              {"tipo": "INSS", "data_inicio": (HOJE - dt.timedelta(days=50)).isoformat(),
               "previsao_retorno": (HOJE + dt.timedelta(days=3)).isoformat()}, None)

    achados = agente_dp.vigia(sessao_real)
    meus = [a for a in achados
            if a["tipo"] == "volta_de_afastamento_sem_o_que_falta"
            and a["colaborador_id"] == pessoa.id]

    assert meus, "a volta chegando sem o exame tem de aparecer no vigia"
    assert "ASO de retorno" in meus[0]["o_que"]


def test_o_vigia_NAO_avisa_de_volta_distante(sessao_real, pessoa):
    """Avisar com dois meses de antecedência todo dia é o jeito de o aviso
    virar ruído — e o aviso de verdade sumir junto."""
    from app.apps.erp.core import agente_dp
    svc.abrir(sessao_real, pessoa.id,
              {"tipo": "INSS", "data_inicio": (HOJE - dt.timedelta(days=50)).isoformat(),
               "previsao_retorno": (HOJE + dt.timedelta(days=60)).isoformat()}, None)

    achados = agente_dp.vigia(sessao_real)

    assert not [a for a in achados
                if a["tipo"] == "volta_de_afastamento_sem_o_que_falta"
                and a["colaborador_id"] == pessoa.id]


def test_afastamento_SEM_previsao_aparece_sempre(sessao_real, pessoa):
    """É o que some da vista por mais tempo: ninguém sabe quando ela volta."""
    from app.apps.erp.core import agente_dp
    svc.abrir(sessao_real, pessoa.id,
              {"tipo": "INSS",
               "data_inicio": (HOJE - dt.timedelta(days=50)).isoformat()}, None)

    achados = agente_dp.vigia(sessao_real)
    meus = [a for a in achados
            if a["tipo"] == "volta_de_afastamento_sem_o_que_falta"
            and a["colaborador_id"] == pessoa.id]

    assert meus and "sem previsão" in meus[0]["o_que"]


def test_a_frase_do_vigia_na_tela_nao_promete_o_que_o_codigo_nao_faz():
    """⚑ ESTE TESTE NASCEU DE UM DEFEITO REAL (18/11/2026).

    A rotina ANUNCIAVA na tela do agente que analisava "ASO e NR vencendo", e
    o código não consultava nem um nem outro. Promessa na tela sem código
    atrás é pior que recurso faltando: ninguém vai procurar o que acredita já
    ter.

    O teste é grosseiro de propósito — ele não confere a lógica, confere que a
    frase e o código falam dos mesmos assuntos. Quem mexer num tem de mexer no
    outro.
    """
    import inspect

    from app.apps.erp.core import agente_dp
    frase = agente_dp.ROTINAS["vigia"]["analisa"].lower()
    codigo = inspect.getsource(agente_dp.vigia)

    if "aso" in frase or " nr " in frase:
        assert "ASO" in codigo and "CERTIFICADO-NR" in codigo, (
            "a frase do vigia promete olhar ASO/NR e o código não olha")
    if "afastamento" in frase:
        assert "colaborador_afastamentos" in codigo, (
            "a frase do vigia promete olhar afastamento e o código não olha")


def test_quem_volta_com_pedido_aberto_vai_para_SOLICITACOES(sessao_real, pessoa):
    """⚑ A volta não é sempre "Concluído".

    O retorno move a etapa com `automatico=True` (a conferência do retorno é
    outra, e exigir o registro da CTPS de quem JÁ trabalhava seria cobrar duas
    vezes o mesmo papel). Mas isso pulava a regra do pedido aberto — e o
    cadastro ia para "Concluído" afirmando o que não aconteceu.
    """
    import datetime as dt

    inicio = HOJE - dt.timedelta(days=60)
    svc.abrir(sessao_real, pessoa.id,
              {"tipo": "INSS", "data_inicio": inicio.isoformat()}, None)
    _aso(sessao_real, pessoa.id, HOJE)
    sessao_real.execute(text("""
        INSERT INTO dp_pedidos (numero, colaborador_id, template, situacao,
                                assunto, corpo)
        VALUES ('CT-99999', :c, 'FERIAS', 'ENVIADO', 'Férias', 'corpo')"""),
        {"c": pessoa.id})
    sessao_real.flush()

    r = svc.retornar(sessao_real, pessoa.id,
                     {"data_retorno": HOJE.isoformat()}, None)

    assert r["etapa"] == "NA_CONTABILIDADE"
    assert sessao_real.get(Colaborador, pessoa.id).etapa == "NA_CONTABILIDADE"
    # … e a situação volta para ATIVO de todo jeito: ela está trabalhando.
    assert sessao_real.get(Colaborador, pessoa.id).situacao == "ATIVO"


def test_quem_volta_sem_pedido_aberto_vai_para_CONCLUIDO(sessao_real, pessoa):
    import datetime as dt

    inicio = HOJE - dt.timedelta(days=60)
    svc.abrir(sessao_real, pessoa.id,
              {"tipo": "INSS", "data_inicio": inicio.isoformat()}, None)
    _aso(sessao_real, pessoa.id, HOJE)

    r = svc.retornar(sessao_real, pessoa.id,
                     {"data_retorno": HOJE.isoformat()}, None)

    assert r["etapa"] == "CONCLUIDO"
