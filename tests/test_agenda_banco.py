"""A agenda do ERP — com banco de verdade.

Ela existe porque quatro coisas construídas antes dela sabiam calcular a
própria data e não tinham onde AVISAR: o aniversário do reajuste, a conferência
mensal dos equipamentos locados, o vencimento das certidões e o fim da vigência
do contrato. Um alerta que mora dentro da tela que a pessoa só abre quando já
lembrou do assunto não é alerta.

O que se prova:

  1. Os quatro geradores produzem o aviso certo, na data certa.
  2. A sincronização é IDEMPOTENTE: rodar dez vezes não empilha dez avisos.
  3. O aviso que deixou de valer é APAGADO — certidão renovada, contrato
     encerrado. Agenda que acumula aviso velho é agenda que ninguém abre.
  4. RESOLVIDO, DISPENSADO e MANUAL nunca são apagados, cada um por um motivo
     diferente.
  5. Dispensar exige motivo.
  6. Só a certidão MAIS NOVA de cada tipo conta — a anterior é histórico.
"""
from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

import pytest

from app.apps.erp.core.agenda import geradores
from app.apps.erp.core.agenda import service as svc
from app.apps.erp.core.auth.service import gerar_hash
from app.apps.erp.core.comum.auditoria import ErroNaoEncontrado, ErroValidacao
from app.apps.erp.db.models.cadastros import (Contrato, Fornecedor,
                                              IndiceEconomico, Obra,
                                              PerfilUsuario as P,
                                              RegimeTributario, TipoPessoa,
                                              Usuario)
from app.apps.erp.db.models.financeiro import (AgendaEvento, Anexo, Documento,
                                               DocumentoTipo)

pytestmark = pytest.mark.banco

HOJE = date(2026, 9, 9)


@pytest.fixture
def cenario(sessao_real):
    s = sessao_real
    u = Usuario(nome="Financeiro", email="agenda@teste.local", ativo=True,
                senha_hash=gerar_hash("senha-de-teste-123"), perfil=P.FINANCEIRO)
    cliente = Fornecedor(razao_social="Prefeitura Exemplo", cnpj_cpf="11111111000191",
                         tipo_pessoa=TipoPessoa.PJ, ativo=True,
                         regime_tributario=RegimeTributario.NAO_INFORMADO)
    obra = Obra(codigo="ESCPLANALTO", nome="Escola Planalto")
    s.add_all([u, cliente, obra])
    s.flush()
    return {"s": s, "usuario": u, "cliente": cliente, "obra": obra}


def _contrato(cenario, **kw):
    s = cenario["s"]
    dados = dict(fornecedor_id=cenario["cliente"].id, obra_id=cenario["obra"].id,
                 tipo="OBRA", objeto="Construção da Escola Planalto",
                 valor_total=Decimal("1000000.00"),
                 vigencia_inicio=date(2025, 1, 1), status="VIGENTE")
    dados.update(kw)
    c = Contrato(**dados)
    s.add(c)
    s.flush()
    return c


def _certidao(cenario, *, codigo="CND-FEDERAL", validade: date,
              vence=True, avisar_dias=30, empresa_id=None, obra_id=None):
    s = cenario["s"]
    if s.get(DocumentoTipo, codigo) is None:
        s.add(DocumentoTipo(codigo=codigo, nome="Certidão federal",
                            grupo="CERTIDAO", dono="EMPRESA",
                            vence=vence, avisar_dias=avisar_dias))
        s.flush()
    import hashlib
    conteudo = f"{codigo}-{validade}".encode()
    anexo = Anexo(entidade_tipo="documento", entidade_id=0,
                  nome_arquivo=f"{codigo}.pdf", mime_type="application/pdf",
                  conteudo=conteudo, guardado_em="BANCO",
                  hash_sha256=hashlib.sha256(conteudo).hexdigest(),
                  tamanho_bytes=len(conteudo))
    s.add(anexo)
    s.flush()
    d = Documento(tipo_codigo=codigo, anexo_id=anexo.id,
                  nome_padronizado=f"{codigo}_{validade.isoformat()}.pdf",
                  obra_id=obra_id or cenario["obra"].id, validade=validade)
    s.add(d)
    s.flush()
    return d


# ---------------------------------------------------------------------------
# 1. Os geradores
# ---------------------------------------------------------------------------
def test_o_aniversario_do_reajuste_vira_aviso(cenario):
    """Avisado 45 dias antes: dá tempo de juntar índice, calcular e protocolar."""
    _contrato(cenario, data_base=date(2025, 10, 20), data_base_origem="PROPOSTA",
              indice_reajuste="INCC-DI")
    eventos = geradores.reajustes(cenario["s"], HOJE)
    aniversario = [e for e in eventos if e["chave"].startswith("REAJUSTE:")]
    assert len(aniversario) == 1
    assert aniversario[0]["quando"] == date(2026, 10, 20)
    assert aniversario[0]["avisar_em"] == date(2026, 10, 20) - timedelta(days=45)
    assert "INCC-DI" in aniversario[0]["detalhe"]


def test_contrato_sem_data_base_nao_gera_aviso_de_reajuste(cenario):
    """Sem data-base não há aniversário — e avisar sobre uma data que não
    existe treinaria a pessoa a ignorar a agenda."""
    _contrato(cenario)
    assert geradores.reajustes(cenario["s"], HOJE) == []


def test_o_aniversario_depois_do_fim_da_vigencia_nao_avisa(cenario):
    """Reajustar depois do fim do contrato é discussão, não rotina."""
    _contrato(cenario, data_base=date(2025, 10, 20),
              vigencia_fim=date(2026, 6, 30))
    assert [e for e in geradores.reajustes(cenario["s"], HOJE)
            if e["chave"].startswith("REAJUSTE:")] == []


def test_a_certidao_vencendo_avisa_com_o_prazo_do_tipo(cenario):
    """Certidão federal se tira no mesmo dia; alvará leva semanas. Avisar os
    dois com trinta dias trata como igual o que não é."""
    _certidao(cenario, validade=HOJE + timedelta(days=20), avisar_dias=30)
    eventos = geradores.certidoes(cenario["s"], HOJE)
    assert len(eventos) == 1
    assert eventos[0]["quando"] == HOJE + timedelta(days=20)
    assert "Vence" in eventos[0]["titulo"]


def test_a_certidao_longe_de_vencer_existe_mas_nao_incomoda(cenario):
    """Ela entra no calendário; o que a segura fora da tela é a data de aviso,
    decidida na leitura. Filtrar no gerador fazia a opção "mostrar o que ainda
    não é hora" não mostrar nada — porque não chegava a existir."""
    s = cenario["s"]
    _certidao(cenario, validade=HOJE + timedelta(days=200), avisar_dias=30)
    eventos = geradores.certidoes(s, HOJE)
    assert len(eventos) == 1
    assert eventos[0]["avisar_em"] > HOJE

    svc.sincronizar(s, hoje=HOJE)
    assert svc.listar(s, hoje=HOJE)["resumo"]["total"] == 0
    assert svc.listar(s, hoje=HOJE, incluir_futuros=True)["resumo"]["total"] == 1


def test_a_certidao_vencida_aparece_marcada(cenario):
    d = _certidao(cenario, validade=HOJE - timedelta(days=5))
    eventos = geradores.certidoes(cenario["s"], HOJE)
    assert "VENCIDA" in eventos[0]["titulo"]
    assert eventos[0]["chave"] == f"CERTIDAO:documento={d.id}"


def test_so_a_certidao_mais_nova_conta(cenario):
    """A anterior vencida é histórico. Avisar sobre ela seria avisar sobre um
    problema já resolvido."""
    _certidao(cenario, validade=HOJE - timedelta(days=60))
    nova = _certidao(cenario, validade=HOJE + timedelta(days=10))
    eventos = geradores.certidoes(cenario["s"], HOJE)
    assert len(eventos) == 1
    assert eventos[0]["chave"] == f"CERTIDAO:documento={nova.id}"


def test_o_fim_da_vigencia_avisa_com_60_dias(cenario):
    """Aditivo de prazo não se pede na véspera."""
    _contrato(cenario, vigencia_fim=HOJE + timedelta(days=30))
    eventos = geradores.contratos(cenario["s"], HOJE)
    assert len(eventos) == 1
    assert "Vigência termina" in eventos[0]["titulo"]
    assert eventos[0]["avisar_em"] == HOJE + timedelta(days=30 - 60)


def test_contrato_ja_vencido_aparece_marcado(cenario):
    _contrato(cenario, vigencia_fim=HOJE - timedelta(days=3))
    assert "VENCIDO" in geradores.contratos(cenario["s"], HOJE)[0]["titulo"]


# ---------------------------------------------------------------------------
# 2 e 3. A sincronização
# ---------------------------------------------------------------------------
def test_sincronizar_duas_vezes_nao_duplica(cenario):
    """A chave é única: rodar dez vezes no mesmo dia não cria dez avisos."""
    s = cenario["s"]
    _contrato(cenario, vigencia_fim=HOJE + timedelta(days=30))
    primeiro = svc.sincronizar(s, hoje=HOJE)
    segundo = svc.sincronizar(s, hoje=HOJE)
    assert primeiro["criados"] == 1
    assert segundo["criados"] == 0
    assert len(s.query(AgendaEvento).all()) == 1


def test_o_aviso_que_deixou_de_valer_e_apagado(cenario):
    """Certidão renovada, contrato encerrado: o aviso some sozinho. Agenda que
    acumula aviso velho é agenda que ninguém abre."""
    s = cenario["s"]
    c = _contrato(cenario, vigencia_fim=HOJE + timedelta(days=30))
    svc.sincronizar(s, hoje=HOJE)
    assert svc.listar(s, hoje=HOJE)["resumo"]["total"] == 1

    c.status = "ENCERRADO"
    s.flush()
    r = svc.sincronizar(s, hoje=HOJE)
    assert r["apagados"] == 1
    assert svc.listar(s, hoje=HOJE)["resumo"]["total"] == 0


def test_o_texto_do_aviso_se_atualiza(cenario):
    """O aviso acompanha o cadastro em vez de ficar mentindo. Aqui a obra
    ganha código novo, e o título do aviso muda junto — sem criar um segundo."""
    s = cenario["s"]
    _contrato(cenario, vigencia_fim=HOJE + timedelta(days=30))
    svc.sincronizar(s, hoje=HOJE)
    antes = svc.listar(s, hoje=HOJE)["eventos"][0]["titulo"]

    cenario["obra"].codigo = "ESCPLANALTO-2"
    s.flush()
    r = svc.sincronizar(s, hoje=HOJE)
    assert r["atualizados"] >= 1
    assert r["criados"] == 0, "mudar o texto não pode criar um aviso novo"
    depois = svc.listar(s, hoje=HOJE)["eventos"][0]["titulo"]
    assert depois != antes and "ESCPLANALTO-2" in depois


def test_mudar_a_data_do_contrato_troca_o_aviso(cenario):
    """A data faz parte da identidade do aviso: prorrogou a vigência, o aviso
    antigo não vale mais e nasce outro, com a data nova."""
    s = cenario["s"]
    c = _contrato(cenario, vigencia_fim=HOJE + timedelta(days=30))
    svc.sincronizar(s, hoje=HOJE)
    c.vigencia_fim = HOJE + timedelta(days=20)
    s.flush()
    svc.sincronizar(s, hoje=HOJE)
    eventos = svc.listar(s, hoje=HOJE)["eventos"]
    assert len(eventos) == 1
    assert eventos[0]["quando"] == (HOJE + timedelta(days=20)).isoformat()


def test_um_gerador_com_defeito_nao_derruba_a_agenda(cenario, monkeypatch):
    """O resto dos avisos continua valendo, e a falha fica dita."""
    s = cenario["s"]
    _contrato(cenario, vigencia_fim=HOJE + timedelta(days=30))
    def explode(sessao, hoje=None):
        raise RuntimeError("banco de índices fora")
    monkeypatch.setattr(geradores, "TODOS", (explode, geradores.contratos))
    r = svc.sincronizar(s, hoje=HOJE)
    assert r["criados"] == 1
    assert r["falhas"] and "banco de índices fora" in r["falhas"][0]


# ---------------------------------------------------------------------------
# 4. O que nunca se apaga
# ---------------------------------------------------------------------------
def test_resolvido_nao_volta_e_nao_some(cenario):
    s = cenario["s"]
    c = _contrato(cenario, vigencia_fim=HOJE + timedelta(days=30))
    svc.sincronizar(s, hoje=HOJE)
    evento = s.query(AgendaEvento).one()
    svc.resolver(s, evento.id, observacao="Aditivo assinado em 01/09.",
                 usuario=cenario["usuario"])

    svc.sincronizar(s, hoje=HOJE)                 # não recria
    assert svc.listar(s, hoje=HOJE)["resumo"]["total"] == 0
    resolvidos = svc.listar(s, situacao="RESOLVIDO", hoje=HOJE)["eventos"]
    assert len(resolvidos) == 1
    assert resolvidos[0]["resolvido_por"] == "Financeiro"

    c.status = "ENCERRADO"                        # e nem some com a limpeza
    s.flush()
    svc.sincronizar(s, hoje=HOJE)
    assert len(svc.listar(s, situacao="RESOLVIDO", hoje=HOJE)["eventos"]) == 1


def test_dispensado_exige_motivo(cenario):
    """Sem motivo, três meses depois "não se aplica" é indistinguível de
    esquecimento."""
    s = cenario["s"]
    _contrato(cenario, vigencia_fim=HOJE + timedelta(days=30))
    svc.sincronizar(s, hoje=HOJE)
    evento = s.query(AgendaEvento).one()
    with pytest.raises(ErroValidacao) as e:
        svc.resolver(s, evento.id, dispensar=True, usuario=cenario["usuario"])
    assert "motivo" in str(e.value)


def test_dispensado_nao_volta_na_proxima_sincronizacao(cenario):
    """Se voltasse, a pessoa dispensaria de novo, para sempre."""
    s = cenario["s"]
    _contrato(cenario, vigencia_fim=HOJE + timedelta(days=30))
    svc.sincronizar(s, hoje=HOJE)
    evento = s.query(AgendaEvento).one()
    svc.resolver(s, evento.id, dispensar=True,
                 observacao="Obra entregue, contrato não será aditivado.",
                 usuario=cenario["usuario"])
    svc.sincronizar(s, hoje=HOJE)
    assert svc.listar(s, hoje=HOJE)["resumo"]["total"] == 0


def test_a_anotacao_manual_sobrevive_a_sincronizacao(cenario):
    """Ninguém deduziu, então ninguém pode deduzir que sumiu."""
    s = cenario["s"]
    svc.criar_manual(s, titulo="Entregar a declaração anual",
                     quando=HOJE + timedelta(days=3), usuario=cenario["usuario"])
    svc.sincronizar(s, hoje=HOJE)
    assert svc.listar(s, hoje=HOJE)["resumo"]["total"] == 1


def test_aviso_deduzido_nao_se_apaga(cenario):
    """Apagar não adianta: ele volta na próxima conferência."""
    s = cenario["s"]
    _contrato(cenario, vigencia_fim=HOJE + timedelta(days=30))
    svc.sincronizar(s, hoje=HOJE)
    evento = s.query(AgendaEvento).one()
    with pytest.raises(ErroValidacao) as e:
        svc.apagar_manual(s, evento.id)
    assert "volta na próxima" in str(e.value)


def test_a_anotacao_se_apaga(cenario):
    s = cenario["s"]
    e = svc.criar_manual(s, titulo="Reunião com o cliente",
                         quando=HOJE + timedelta(days=2), usuario=cenario["usuario"])
    svc.apagar_manual(s, e.id, usuario=cenario["usuario"])
    assert svc.listar(s, hoje=HOJE)["resumo"]["total"] == 0


def test_reabrir_devolve_o_aviso(cenario):
    """Errou o clique. Reabrir é barato; perder o aviso, não."""
    s = cenario["s"]
    _contrato(cenario, vigencia_fim=HOJE + timedelta(days=30))
    svc.sincronizar(s, hoje=HOJE)
    evento = s.query(AgendaEvento).one()
    svc.resolver(s, evento.id, usuario=cenario["usuario"])
    svc.reabrir(s, evento.id, usuario=cenario["usuario"])
    assert svc.listar(s, hoje=HOJE)["resumo"]["total"] == 1


# ---------------------------------------------------------------------------
# 5. O que a tela mostra
# ---------------------------------------------------------------------------
def test_o_que_ainda_nao_e_hora_fica_fora_por_padrao(cenario):
    """Certidão que vence daqui a três meses existe, mas ocupar a tela com ela
    hoje é o caminho para a pessoa parar de olhar a tela."""
    s = cenario["s"]
    _certidao(cenario, validade=HOJE + timedelta(days=25), avisar_dias=30)
    _contrato(cenario, vigencia_fim=HOJE + timedelta(days=200))
    svc.sincronizar(s, hoje=HOJE)

    assert svc.listar(s, hoje=HOJE)["resumo"]["total"] == 1, \
        "só a certidão: o contrato ainda está a 200 dias"
    assert svc.listar(s, hoje=HOJE, incluir_futuros=True)["resumo"]["total"] == 2


def test_o_prazo_sai_escrito_em_portugues(cenario):
    s = cenario["s"]
    _contrato(cenario, vigencia_fim=HOJE - timedelta(days=3))
    svc.sincronizar(s, hoje=HOJE)
    linha = svc.listar(s, hoje=HOJE)["eventos"][0]
    assert linha["prazo"] == "vencido há 3 dia(s)"
    assert linha["dias"] == -3


def test_a_contagem_da_tela_de_inicio(cenario):
    s = cenario["s"]
    _contrato(cenario, vigencia_fim=HOJE - timedelta(days=3))
    _certidao(cenario, validade=HOJE + timedelta(days=10))
    svc.sincronizar(s, hoje=HOJE)
    c = svc.contagem(s, hoje=HOJE)
    assert c["abertos"] == 2
    assert c["vencidos"] == 1


def test_o_filtro_por_assunto_funciona(cenario):
    s = cenario["s"]
    _contrato(cenario, vigencia_fim=HOJE + timedelta(days=30))
    _certidao(cenario, validade=HOJE + timedelta(days=10))
    svc.sincronizar(s, hoje=HOJE)
    assert svc.listar(s, origem="CERTIDAO", hoje=HOJE)["resumo"]["total"] == 1
    assert svc.listar(s, origem="CONTRATO", hoje=HOJE)["resumo"]["total"] == 1


def test_aviso_inexistente_da_erro_claro(cenario):
    """Desde 12/09/2026 é `ErroNaoEncontrado`, e não `ErroValidacao`.

    A agenda passou a ter recorte por obra, e aviso FORA DO ESCOPO tem de
    responder exatamente o mesmo que aviso INEXISTENTE — senão a diferença
    entre as duas respostas conta que o número existe, e varrer os números
    mapearia a agenda das outras obras.
    """
    with pytest.raises(ErroNaoEncontrado):
        svc.resolver(cenario["s"], 999999)
