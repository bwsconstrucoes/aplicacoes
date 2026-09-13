"""O controle da numeração das notas emitidas — com banco de verdade.

Pergunta do dono em 09/09/2026: *"você vai conseguir enxergar qual o número da
nota que tem que ser emitida, para registrar em sistema, e ser tranquilo? Eu
digo para a gente manter o controle da numeração corretamente."*

A resposta é sim, e por um motivo técnico: no padrão nacional e no ABRASF quem
numera a DECLARAÇÃO é quem emite; a prefeitura devolve o número da NOTA. São
dois números, e o ERP é dono do primeiro.

Com banco porque as travas que fazem isso funcionar são índices ÚNICOS — e o
que se prova aqui é justamente o que acontece quando duas emissões disputam o
mesmo número.

O que se prova:

  1. Dá para ver o próximo número antes de emitir.
  2. O mesmo número não sai duas vezes na mesma empresa, série e ambiente.
  3. Homologação e produção têm sequências separadas: teste não queima número
     de verdade.
  4. Número que falhou NÃO é reciclado, e falhar exige motivo escrito.
  5. A mesma nota da prefeitura não é registrada duas vezes — é o erro do modo
     manual, digitar duas vezes.
  6. A conferência separa BURACO (alguém emitiu por fora) de QUEIMADO (falhou,
     e tem explicação). São coisas muito diferentes.
"""
from __future__ import annotations

from datetime import date

import pytest
from sqlalchemy.exc import IntegrityError

from app.apps.erp.core.comum.auditoria import ErroValidacao
from app.apps.erp.core.notas_emitidas import numeracao
from app.apps.erp.db.models.cadastros import Empresa
from app.apps.erp.db.models.financeiro import NotaEmitida

pytestmark = pytest.mark.banco


@pytest.fixture
def cenario(sessao_real):
    s = sessao_real
    # A empresa no modo API precisa de destino — a trava do banco exige, e é o
    # certo: ligar a API sem endereço deixaria a nota falhando no fechamento
    # do mês.
    bws = Empresa(razao_social="BWS Construções LTDA", nome_fantasia="BWS",
                  cnpj="11222333000181", emissao_serie="1",
                  emissao_ambiente="PRODUCAO", emissao_modo="API",
                  emissao_codigo_ibge="2304285",
                  emissao_url_base="https://ce-eusebio-pm-nfs-backend.cloud.el.com.br/nfse40")
    outra = Empresa(razao_social="Segunda Construtora LTDA", nome_fantasia="Segunda",
                    cnpj="44555666000199", emissao_serie="1",
                    emissao_ambiente="PRODUCAO", emissao_modo="MANUAL")
    s.add_all([bws, outra])
    s.flush()
    return {"s": s, "bws": bws, "outra": outra}


# ---------------------------------------------------------------------------
# 1 e 2. Ver o próximo, e não repetir
# ---------------------------------------------------------------------------
def test_da_para_ver_o_proximo_numero_antes_de_emitir(cenario):
    """É a resposta direta à pergunta do dono."""
    s = cenario["s"]
    assert numeracao.proximo_numero(s, cenario["bws"]) == 1
    numeracao.reservar(s, cenario["bws"])
    assert numeracao.proximo_numero(s, cenario["bws"]) == 2


def test_a_sequencia_anda_de_um_em_um(cenario):
    s = cenario["s"]
    numeros = [numeracao.reservar(s, cenario["bws"]).numero_dps for _ in range(4)]
    assert numeros == [1, 2, 3, 4]


def test_cada_empresa_tem_a_sua_sequencia(cenario):
    """Duas empresas, dois CNPJs, duas numerações. Misturar seria o caos."""
    s = cenario["s"]
    numeracao.reservar(s, cenario["bws"])
    numeracao.reservar(s, cenario["bws"])
    assert numeracao.reservar(s, cenario["outra"]).numero_dps == 1


def test_o_banco_recusa_o_mesmo_numero_duas_vezes(cenario):
    """É esta trava que torna a duplicidade impossível, mesmo com duas pessoas
    emitindo ao mesmo tempo."""
    s = cenario["s"]
    numeracao.reservar(s, cenario["bws"])
    s.add(NotaEmitida(empresa_id=cenario["bws"].id, ambiente="PRODUCAO",
                      serie="1", numero_dps=1))
    with pytest.raises(IntegrityError):
        s.flush()
    s.rollback()


# ---------------------------------------------------------------------------
# 3. Teste não queima número de verdade
# ---------------------------------------------------------------------------
def test_homologacao_e_producao_tem_sequencias_separadas(cenario):
    s, e = cenario["s"], cenario["bws"]
    numeracao.reservar(s, e)
    numeracao.reservar(s, e)
    assert numeracao.proximo_numero(s, e) == 3

    e.emissao_ambiente = "HOMOLOGACAO"
    s.flush()
    assert numeracao.proximo_numero(s, e) == 1, \
        "teste não pode consumir número de produção"
    assert numeracao.reservar(s, e).numero_dps == 1

    e.emissao_ambiente = "PRODUCAO"
    s.flush()
    assert numeracao.proximo_numero(s, e) == 3, "produção não foi afetada"


def test_series_diferentes_nao_se_misturam(cenario):
    s, e = cenario["s"], cenario["bws"]
    numeracao.reservar(s, e)
    e.emissao_serie = "2"
    s.flush()
    assert numeracao.reservar(s, e).numero_dps == 1


# ---------------------------------------------------------------------------
# 4. Número queimado não volta para a fila
# ---------------------------------------------------------------------------
def test_numero_que_falhou_nao_e_reciclado(cenario):
    """A prefeitura pode ter recebido a declaração e só a resposta ter se
    perdido. Reemitir com o mesmo número daria duplicidade do lado dela."""
    s = cenario["s"]
    nota = numeracao.reservar(s, cenario["bws"])
    numeracao.falhar(s, nota.id, motivo="a prefeitura não respondeu")
    assert numeracao.proximo_numero(s, cenario["bws"]) == 2
    assert numeracao.reservar(s, cenario["bws"]).numero_dps == 2


def test_falhar_sem_motivo_e_recusado(cenario):
    """Número queimado sem explicação é o que o fisco pergunta."""
    s = cenario["s"]
    nota = numeracao.reservar(s, cenario["bws"])
    with pytest.raises(ErroValidacao) as e:
        numeracao.falhar(s, nota.id, motivo="   ")
    assert "explicação" in str(e.value) or "aconteceu" in str(e.value)


def test_o_banco_tambem_recusa_falha_sem_motivo(cenario):
    s = cenario["s"]
    nota = numeracao.reservar(s, cenario["bws"])
    nota.situacao = "FALHADA"
    with pytest.raises(IntegrityError):
        s.flush()
    s.rollback()


# ---------------------------------------------------------------------------
# 5. O número da prefeitura
# ---------------------------------------------------------------------------
def test_confirmar_guarda_o_numero_que_a_prefeitura_devolveu(cenario):
    s = cenario["s"]
    nota = numeracao.reservar(s, cenario["bws"])
    numeracao.confirmar(s, nota.id, numero_nota="2026/000123",
                        data_emissao=date(2026, 9, 9),
                        codigo_verificacao="ABC123")
    s.refresh(nota)
    assert nota.situacao == "EMITIDA"
    assert nota.numero_dps == 1, "o número da DECLARAÇÃO não muda"
    assert nota.numero_nota == "2026/000123", "o número da NOTA é outro"


def test_a_mesma_nota_nao_e_registrada_duas_vezes(cenario):
    """É o erro do modo MANUAL: digitar a mesma nota duas vezes."""
    s = cenario["s"]
    a = numeracao.reservar(s, cenario["outra"])
    numeracao.confirmar(s, a.id, numero_nota="555")
    b = numeracao.reservar(s, cenario["outra"])
    with pytest.raises(ErroValidacao) as e:
        numeracao.confirmar(s, b.id, numero_nota="555")
    assert "já está registrada" in str(e.value)


def test_confirmar_duas_vezes_a_mesma_reserva_e_recusado(cenario):
    s = cenario["s"]
    nota = numeracao.reservar(s, cenario["bws"])
    numeracao.confirmar(s, nota.id, numero_nota="111")
    with pytest.raises(ErroValidacao) as e:
        numeracao.confirmar(s, nota.id, numero_nota="222")
    assert "já está registrada como emitida" in str(e.value)


def test_nota_emitida_precisa_do_numero_da_prefeitura(cenario):
    """Sem ele não há o que informar ao cliente nem o que conciliar depois."""
    s = cenario["s"]
    nota = numeracao.reservar(s, cenario["bws"])
    nota.situacao = "EMITIDA"
    with pytest.raises(IntegrityError):
        s.flush()
    s.rollback()


# ---------------------------------------------------------------------------
# 6. A conferência que o fisco pergunta
# ---------------------------------------------------------------------------
def test_a_conferencia_diz_que_esta_integra(cenario):
    s = cenario["s"]
    for i in range(3):
        nota = numeracao.reservar(s, cenario["bws"])
        numeracao.confirmar(s, nota.id, numero_nota=f"NF-{i}")
    r = numeracao.conferir(s, cenario["bws"])
    assert r["integra"] is True
    assert r["emitidas"] == 3 and r["proximo"] == 4
    assert r["buracos"] == [] and r["aviso"] == ""


def test_buraco_na_sequencia_e_apontado_com_a_causa_provavel(cenario):
    """Buraco costuma significar que alguém emitiu por fora do ERP."""
    s = cenario["s"]
    numeracao.reservar(s, cenario["bws"])
    s.add(NotaEmitida(empresa_id=cenario["bws"].id, ambiente="PRODUCAO",
                      serie="1", numero_dps=4, situacao="RESERVADA"))
    s.flush()
    r = numeracao.conferir(s, cenario["bws"])
    assert r["integra"] is False
    assert r["buracos"] == [2, 3]
    assert "por fora do ERP" in r["aviso"]


def test_queimado_e_diferente_de_buraco(cenario):
    """Queimado tem resposta pronta; buraco é o preocupante."""
    s = cenario["s"]
    nota = numeracao.reservar(s, cenario["bws"])
    numeracao.falhar(s, nota.id, motivo="certificado vencido")
    numeracao.reservar(s, cenario["bws"])
    r = numeracao.conferir(s, cenario["bws"])
    assert r["integra"] is True, "número queimado NÃO é buraco na sequência"
    assert r["queimados"] == [{"numero_dps": 1, "situacao": "FALHADA",
                               "motivo": "certificado vencido"}]
