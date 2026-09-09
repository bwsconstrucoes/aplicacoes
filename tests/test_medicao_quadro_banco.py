"""A medição completa e o quadro do contrato — com banco de verdade.

Ditado pelo dono em 09/09/2026. O ponto que ele fez questão de detalhar, e que
é o mais fácil de errar: *"às vezes o nosso sistema não se adequa a cem por
cento, porque teve uma medição 1 alguma coisa e outra medição 1 alguma coisa,
por conta de fontes diferentes, e o órgão trata dessa forma. A gente precisa
ter um pouco mais de flexibilidade nisso."*

O que se prova:

  1. O número da medição é TEXTO LIVRE: "1", "1R", "3", "1-FONTE-A". Quem manda
     na nomenclatura é o órgão.
  2. O tipo vem de catálogo EDITÁVEL, não de lista no código.
  3. A correlação liga o reajuste à medição que ele reajusta — e funciona nos
     DOIS jeitos de numerar que ele descreveu.
  4. Reajuste de outro contrato, reajuste de reajuste e reajuste de si mesma
     são recusados.
  5. O protocolo destrava o indicador de tempo de recebimento.
  6. O quadro do contrato separa MEDIDO de FATURADO de RECEBIDO — e o reajuste
     não consome saldo do contratado.
"""
from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

import pytest
from sqlalchemy.exc import IntegrityError

from app.apps.erp.core.auth.service import gerar_hash
from app.apps.erp.core.comum.auditoria import ErroValidacao
from app.apps.erp.core.titulos import medicao as svc_med
from app.apps.erp.core.titulos import quadro as svc_quadro
from app.apps.erp.db.models.cadastros import (Categoria, Contrato, ContaBancaria,
                                              FormaPagamento, Fornecedor, Obra,
                                              PerfilUsuario as P,
                                              RegimeTributario, TipoPessoa,
                                              Usuario)
from app.apps.erp.db.models.financeiro import (EspecieTitulo, FormaPagamento,
                                               Pagamento, Parcela,
                                               StatusParcela, StatusTitulo,
                                               TipoTitulo, Titulo)

pytestmark = pytest.mark.banco


@pytest.fixture
def cenario(sessao_real):
    s = sessao_real
    svc_med.aplicar_tipos(s)
    u = Usuario(nome="Financeiro", email="medicao@teste.local", ativo=True,
                senha_hash=gerar_hash("senha-de-teste-123"), perfil=P.FINANCEIRO)
    cliente = Fornecedor(razao_social="Prefeitura Exemplo", cnpj_cpf="11111111000191",
                         tipo_pessoa=TipoPessoa.PJ, ativo=True,
                         regime_tributario=RegimeTributario.NAO_INFORMADO)
    obra = Obra(codigo="ESCPLANALTO", nome="Escola Planalto")
    cat = Categoria(codigo="1.1.01", descricao="Receita de medição")
    conta = ContaBancaria(descricao="Conta do teste", banco_codigo="237",
                          agencia="1", conta="1", ativo=True)
    s.add_all([u, cliente, obra, cat, conta])
    s.flush()
    contrato = Contrato(fornecedor_id=cliente.id, obra_id=obra.id, tipo="OBRA",
                        objeto="Construção da Escola Planalto",
                        valor_total=Decimal("1000000.00"),
                        vigencia_inicio=date(2026, 1, 1), status="VIGENTE",
                        indice_reajuste="INCC-DI")
    outro = Contrato(fornecedor_id=cliente.id, obra_id=obra.id, tipo="OBRA",
                     objeto="Outro contrato", valor_total=Decimal("500000.00"),
                     vigencia_inicio=date(2026, 1, 1), status="VIGENTE")
    s.add_all([contrato, outro])
    s.flush()
    return {"s": s, "usuario": u, "cliente": cliente, "obra": obra,
            "categoria": cat, "conta": conta, "contrato": contrato, "outro": outro}


def _medicao(cenario, numero, valor="100000.00", contrato=None, sp=None):
    s = cenario["s"]
    c = contrato or cenario["contrato"]
    t = Titulo(numero_sp=sp or f"SP-MED-{numero}", tipo=TipoTitulo.T1_MATERIAL_NFE,
               especie=EspecieTitulo.RECEBER, fornecedor_id=cenario["cliente"].id,
               descricao=f"Medição {numero}", valor_bruto=Decimal(valor),
               valor_liquido=Decimal(valor),
               competencia=date(2026, 8, 1), categoria_id=cenario["categoria"].id,
               forma_pagamento=FormaPagamento.BOLETO, status=StatusTitulo.APROVADO,
               solicitante_id=cenario["usuario"].id,
               contrato_id=c.id, numero_medicao=numero,
               periodo_inicio=date(2026, 8, 1), periodo_fim=date(2026, 8, 31))
    s.add(t)
    s.flush()
    return t


def _receber(cenario, titulo, valor="100000.00", em=None):
    s = cenario["s"]
    p = Parcela(titulo_id=titulo.id, numero=1, vencimento=date(2026, 9, 10),
                valor=Decimal(valor), status=StatusParcela.PAGA)
    s.add(p)
    s.flush()
    s.add(Pagamento(parcela_id=p.id, conta_bancaria_id=cenario["conta"].id,
                    valor_pago=Decimal(valor), meio=FormaPagamento.TED,
                    data_pagamento=em or date(2026, 9, 20)))
    s.flush()


# ---------------------------------------------------------------------------
# 1 e 2. Número livre, tipo editável
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("numero", ["1", "1R", "3", "1-FONTE-A", "02/2026"])
def test_o_numero_da_medicao_e_texto_livre(cenario, numero):
    """Quem manda na nomenclatura é o órgão. Impor "1, 2, 3" quebraria no
    primeiro contrato fora do padrão — e o dono já viu isso acontecer."""
    t = _medicao(cenario, numero, sp=f"SP-{numero}")
    assert t.numero_medicao == numero


def test_o_catalogo_de_tipos_nasce_com_os_cinco_e_e_editavel(cenario):
    tipos = {t["codigo"] for t in svc_med.listar_tipos(cenario["s"])}
    assert tipos == {"NORMAL", "REAJUSTE", "ADITIVO", "SUBSIDIARIA", "COMPLEMENTAR"}


def test_aplicar_tipos_duas_vezes_nao_duplica(cenario):
    antes = len(svc_med.listar_tipos(cenario["s"]))
    assert svc_med.aplicar_tipos(cenario["s"])["criados"] == 0
    assert len(svc_med.listar_tipos(cenario["s"])) == antes


def test_tipo_inventado_e_recusado(cenario):
    t = _medicao(cenario, "1")
    with pytest.raises(ErroValidacao) as e:
        svc_med.classificar(cenario["s"], t.id, tipo="MAIS_OU_MENOS")
    assert "desconhecido" in str(e.value)


# ---------------------------------------------------------------------------
# 3. A correlação, nos DOIS jeitos de numerar
# ---------------------------------------------------------------------------
def test_orgao_que_numera_em_paralelo_medicao_1_e_1R(cenario):
    s = cenario["s"]
    um = _medicao(cenario, "1")
    umR = _medicao(cenario, "1R", valor="3200.00")
    svc_med.classificar(s, um.id, tipo="NORMAL")
    svc_med.classificar(s, umR.id, tipo="REAJUSTE", medicao_de_id=um.id)

    lido = svc_med.ler(s, umR)
    assert lido["e_reajuste"] is True
    assert lido["reajuste_de"]["medicao"] == "1"


def test_orgao_que_numera_em_sequencia_o_reajuste_virou_a_3(cenario):
    """O caso que quebra sistema rígido: o reajuste entrou na fila como se
    fosse medição normal, mas continua sendo reajuste DA 1."""
    s = cenario["s"]
    um = _medicao(cenario, "1")
    _medicao(cenario, "2")
    tres = _medicao(cenario, "3", valor="3200.00")
    svc_med.classificar(s, tres.id, tipo="REAJUSTE", medicao_de_id=um.id)

    lido = svc_med.ler(s, tres)
    assert lido["e_reajuste"] is True
    assert lido["reajuste_de"]["medicao"] == "1", \
        "o número não diz nada; a correlação diz"


def test_medicoes_subsidiarias_convivem(cenario):
    """Duas medições "1", de fontes diferentes, como o órgão trata."""
    s = cenario["s"]
    a = _medicao(cenario, "1-FONTE-A", sp="SP-A")
    b = _medicao(cenario, "1-FONTE-B", sp="SP-B")
    svc_med.classificar(s, a.id, tipo="NORMAL")
    svc_med.classificar(s, b.id, tipo="SUBSIDIARIA")
    assert svc_med.ler(s, b)["tipo"] == "SUBSIDIARIA"


# ---------------------------------------------------------------------------
# 4. O que a correlação recusa
# ---------------------------------------------------------------------------
def test_medicao_nao_pode_ser_reajuste_dela_mesma(cenario):
    s = cenario["s"]
    t = _medicao(cenario, "1")
    with pytest.raises(ErroValidacao) as e:
        svc_med.classificar(s, t.id, tipo="REAJUSTE", medicao_de_id=t.id)
    assert "dela mesma" in str(e.value)


def test_o_banco_tambem_recusa_apontar_para_si(cenario):
    s = cenario["s"]
    t = _medicao(cenario, "1")
    t.medicao_de_id = t.id
    with pytest.raises(IntegrityError):
        s.flush()
    s.rollback()


def test_reajuste_de_medicao_de_outro_contrato_e_recusado(cenario):
    """Somaria valor no quadro errado, e o erro só apareceria na conferência."""
    s = cenario["s"]
    de_la = _medicao(cenario, "1", contrato=cenario["outro"], sp="SP-OUTRO")
    daqui = _medicao(cenario, "1R", valor="1000.00")
    with pytest.raises(ErroValidacao) as e:
        svc_med.classificar(s, daqui.id, tipo="REAJUSTE", medicao_de_id=de_la.id)
    assert "outro contrato" in str(e.value)


def test_reajuste_de_reajuste_e_recusado(cenario):
    s = cenario["s"]
    um = _medicao(cenario, "1")
    umR = _medicao(cenario, "1R", valor="3200.00")
    svc_med.classificar(s, umR.id, tipo="REAJUSTE", medicao_de_id=um.id)
    outro = _medicao(cenario, "1RR", valor="100.00")
    with pytest.raises(ErroValidacao) as e:
        svc_med.classificar(s, outro.id, tipo="REAJUSTE", medicao_de_id=umR.id)
    assert "medição original" in str(e.value)


def test_titulo_que_nao_e_medicao_nao_se_classifica(cenario):
    s = cenario["s"]
    t = _medicao(cenario, "1")
    t.numero_medicao = None
    s.flush()
    with pytest.raises(ErroValidacao) as e:
        svc_med.classificar(s, t.id, tipo="NORMAL")
    assert "não é uma medição" in str(e.value)


# ---------------------------------------------------------------------------
# 5. O protocolo e o indicador
# ---------------------------------------------------------------------------
def test_protocolo_registra_numero_e_data(cenario):
    s = cenario["s"]
    t = _medicao(cenario, "1")
    svc_med.protocolar(s, t.id, numero="4512", em=date(2026, 8, 5))
    lido = svc_med.ler(s, t)
    assert lido["protocolo"] == "4512"
    assert lido["protocolado_em"] == "2026-08-05"


def test_protocolo_no_futuro_e_recusado(cenario):
    s = cenario["s"]
    t = _medicao(cenario, "1")
    with pytest.raises(ErroValidacao):
        svc_med.protocolar(s, t.id, numero="1", em=date.today() + timedelta(days=1))


def test_protocolo_sem_numero_e_recusado(cenario):
    s = cenario["s"]
    t = _medicao(cenario, "1")
    with pytest.raises(ErroValidacao):
        svc_med.protocolar(s, t.id, numero="  ")


def test_o_indicador_conta_dias_entre_protocolar_e_receber(cenario):
    """O indicador que hoje não existe em lugar nenhum."""
    s = cenario["s"]
    t = _medicao(cenario, "1")
    svc_med.protocolar(s, t.id, numero="4512", em=date(2026, 8, 5))
    _receber(cenario, t, em=date(2026, 9, 20))

    r = svc_med.tempo_de_recebimento(s, contrato_id=cenario["contrato"].id)
    assert r["recebidas"] == 1
    assert r["media_dias"] == 46.0
    assert r["aguardando"] == 0


def test_medicao_protocolada_e_nao_paga_nao_entra_na_media(cenario):
    """Misturar daria uma média mentirosa, que MELHORA sozinha quando o
    cliente atrasa — exatamente ao contrário do que interessa."""
    s = cenario["s"]
    paga = _medicao(cenario, "1", sp="SP-PAGA")
    svc_med.protocolar(s, paga.id, numero="1", em=date(2026, 8, 5))
    _receber(cenario, paga, em=date(2026, 8, 15))

    esperando = _medicao(cenario, "2", sp="SP-ESPERA")
    svc_med.protocolar(s, esperando.id, numero="2", em=date(2026, 1, 1))

    r = svc_med.tempo_de_recebimento(s, contrato_id=cenario["contrato"].id)
    assert r["recebidas"] == 1 and r["media_dias"] == 10.0
    assert r["aguardando"] == 1
    assert r["mais_antiga"]["numero_sp"] == "SP-ESPERA"


# ---------------------------------------------------------------------------
# 6. O quadro do contrato
# ---------------------------------------------------------------------------
def test_o_quadro_separa_medido_faturado_e_recebido(cenario):
    """Medir não é faturar; faturar não é receber. São três perguntas."""
    s = cenario["s"]
    um = _medicao(cenario, "1", valor="100000.00")
    _medicao(cenario, "2", valor="88000.00")
    _receber(cenario, um, valor="92500.00")

    q = svc_quadro.quadro(s, cenario["contrato"].id)
    tot = q["totais"]
    assert tot["contratado"] == 1000000.00
    assert tot["medido"] == 188000.00
    assert tot["faturado"] == 0.00, "nenhuma nota emitida ainda"
    assert tot["recebido"] == 92500.00
    assert len(q["medicoes"]) == 2


def test_o_reajuste_nao_consome_saldo_do_contratado(cenario):
    """Reajuste é acréscimo por índice, não obra executada a mais. Somá-lo ao
    medido faria o saldo do contrato encolher sem ninguém ter construído nada."""
    s = cenario["s"]
    um = _medicao(cenario, "1", valor="100000.00")
    umR = _medicao(cenario, "1R", valor="3200.00")
    svc_med.classificar(s, umR.id, tipo="REAJUSTE", medicao_de_id=um.id)

    tot = svc_quadro.quadro(s, cenario["contrato"].id)["totais"]
    assert tot["medido"] == 103200.00
    assert tot["reajuste"] == 3200.00
    assert tot["medido_sem_reajuste"] == 100000.00
    assert tot["saldo"] == 900000.00, "o saldo desconta só o que foi medido de obra"


def test_o_quadro_aponta_o_que_falta(cenario):
    """A pergunta que a tela responde de olho: o que foi medido e não virou
    nota, e o que não foi protocolado."""
    s = cenario["s"]
    _medicao(cenario, "1")
    dois = _medicao(cenario, "2")
    svc_med.protocolar(s, dois.id, numero="4519", em=date(2026, 9, 1))

    p = svc_quadro.quadro(s, cenario["contrato"].id)["pendencias"]
    assert set(p["medido_sem_nota"]) == {"SP-MED-1", "SP-MED-2"}
    assert p["sem_protocolo"] == ["SP-MED-1"]


def test_contrato_inexistente_da_erro_claro(cenario):
    with pytest.raises(ErroValidacao):
        svc_quadro.quadro(cenario["s"], 999999)


def test_a_lista_de_contratos_traz_o_essencial(cenario):
    s = cenario["s"]
    _medicao(cenario, "1", valor="250000.00")
    lista = svc_quadro.listar_contratos(s, obra_id=cenario["obra"].id)
    daqui = next(c for c in lista if c["id"] == cenario["contrato"].id)
    assert daqui["medido"] == 250000.00
    assert daqui["saldo"] == 750000.00
    assert daqui["medicoes"] == 1


def test_a_lista_e_o_quadro_contam_a_mesma_coisa(cenario):
    """Saldo na lista e saldo no quadro têm de ser o MESMO número.

    Na primeira versão não eram: a lista descontava o reajuste do saldo e o
    quadro não. Os dois apareciam na mesma sessão, sobre o mesmo contrato,
    com valores diferentes — e quem visse isso perderia a confiança nos dois.
    """
    s = cenario["s"]
    um = _medicao(cenario, "1", valor="300000.00")
    reaj = _medicao(cenario, "1R", valor="21000.00")
    svc_med.classificar(s, reaj.id, tipo="REAJUSTE", medicao_de_id=um.id)

    tot = svc_quadro.quadro(s, cenario["contrato"].id)["totais"]
    daqui = next(c for c in svc_quadro.listar_contratos(s, obra_id=cenario["obra"].id)
                 if c["id"] == cenario["contrato"].id)
    assert daqui["saldo"] == tot["saldo"] == 700000.00
    assert daqui["medido"] == tot["medido"] == 321000.00
    assert daqui["reajuste"] == tot["reajuste"] == 21000.00
    assert daqui["vigente"] == tot["vigente"]
