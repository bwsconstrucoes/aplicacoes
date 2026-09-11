"""A chave Pix da conta e os dados de emissão por empresa — com banco de verdade.

Duas coisas pedidas pelo dono em 09/09/2026, e a segunda mudou de prioridade
depois que ele respondeu que *"a BWS não tem inscrição municipal em Petrolina,
mas outra empresa que vamos operar sim"* e *"uma por API e outra manual"*.

O que se prova:

  1. A chave Pix é conferida no formato — chave com dígito faltando é o erro
     que só aparece quando o dinheiro não chega.
  2. O bloco para copiar sai pronto, com empresa, banco, conta e as chaves.
     Copiar campo por campo é onde se erra um dígito.
  3. Emissão é por EMPRESA: uma pode estar em Eusébio por API e a outra em
     Petrolina no manual.
  4. MANUAL e HOMOLOGAÇÃO são os padrões — empresa nova não sai emitindo nota
     fiscal de verdade porque alguém esqueceu de configurar.
  5. O token vai CIFRADO, e nunca volta pela tela.
  6. Ligar a emissão por API sem endereço e sem município é recusado, no código
     e no banco.
"""
from __future__ import annotations

import pytest
from sqlalchemy.exc import IntegrityError

from app.apps.erp.core.cadastros import contas as svc_contas
from app.apps.erp.core.cadastros import emissao as svc_emissao
from app.apps.erp.core.comum.auditoria import ErroValidacao
from app.apps.erp.db.models.cadastros import (ContaBancaria, ContaChavePix,
                                              Empresa)

pytestmark = pytest.mark.banco

CHAVE_TESTE = "wsHQ6Rl6nJcU8mQVzD3s7pQ1nR2tYv0aBcDeFgHiJkL="   # Fernet de teste


@pytest.fixture
def cenario(sessao_real, monkeypatch):
    monkeypatch.setenv("ERP_CHAVE_SEGREDOS", CHAVE_TESTE)
    s = sessao_real
    bws = Empresa(razao_social="BWS Construções LTDA", nome_fantasia="BWS",
                  cnpj="11222333000181", inscricao_municipal="12345")
    outra = Empresa(razao_social="Segunda Construtora LTDA", nome_fantasia="Segunda",
                    cnpj="44555666000199")
    conta = ContaBancaria(descricao="Bradesco 1234-5", banco_codigo="237",
                          agencia="1234", conta="56789-0", ativo=True)
    s.add_all([bws, outra, conta])
    s.flush()
    return {"s": s, "bws": bws, "outra": outra, "conta": conta}


# ---------------------------------------------------------------------------
# 1. A chave Pix é conferida
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("tipo,chave,esperado", [
    ("CNPJ", "11.222.333/0001-81", "11222333000181"),
    ("CPF", "123.456.789-09", "12345678909"),
    ("EMAIL", "Financeiro@BWS.com.BR", "financeiro@bws.com.br"),
    ("ALEATORIA", "123E4567-E89B-12D3-A456-426614174000",
     "123e4567-e89b-12d3-a456-426614174000"),
])
def test_a_chave_e_limpa_e_padronizada(tipo, chave, esperado):
    assert svc_contas.validar_chave(tipo, chave) == esperado


@pytest.mark.parametrize("tipo,chave", [
    ("CNPJ", "11222333000"),            # dígitos a menos
    ("CPF", "1234567890"),              # dígitos a menos
    ("EMAIL", "financeiro-arroba-bws"),
    ("ALEATORIA", "abc-123"),
    ("INVENTADO", "seja o que for"),
])
def test_chave_torta_e_recusada(tipo, chave):
    """Chave com dígito faltando é o erro que só aparece quando o dinheiro
    não chega — e aí já é tarde."""
    with pytest.raises(ErroValidacao):
        svc_contas.validar_chave(tipo, chave)


def test_a_mesma_chave_nao_entra_duas_vezes(cenario):
    s = cenario["s"]
    a = svc_contas.acrescentar_pix(s, cenario["conta"].id, tipo="CNPJ",
                                   chave="11.222.333/0001-81")
    b = svc_contas.acrescentar_pix(s, cenario["conta"].id, tipo="CNPJ",
                                   chave="11222333000181")
    assert a.id == b.id, "a mesma chave escrita de dois jeitos é a mesma chave"


def test_o_banco_tambem_recusa_a_chave_repetida(cenario):
    s = cenario["s"]
    svc_contas.acrescentar_pix(s, cenario["conta"].id, tipo="EMAIL",
                               chave="financeiro@bws.com.br")
    s.add(ContaChavePix(conta_id=cenario["conta"].id, tipo="EMAIL",
                        chave="financeiro@bws.com.br"))
    with pytest.raises(IntegrityError):
        s.flush()
    s.rollback()


# ---------------------------------------------------------------------------
# 2. O bloco para copiar
# ---------------------------------------------------------------------------
def test_o_bloco_sai_pronto_para_colar(cenario):
    """É o ponto do módulo: copiar campo por campo é onde se erra um dígito."""
    s = cenario["s"]
    svc_contas.acrescentar_pix(s, cenario["conta"].id, tipo="CNPJ",
                               chave="11222333000181")
    svc_contas.acrescentar_pix(s, cenario["conta"].id, tipo="EMAIL",
                               chave="financeiro@bws.com.br")
    texto = svc_contas.texto_para_copiar(cenario["conta"], cenario["bws"])
    assert "BWS Construções LTDA" in texto
    assert "CNPJ: 11.222.333/0001-81" in texto
    assert "Agência: 1234" in texto
    assert "Conta: 56789-0" in texto
    assert "Pix (CNPJ): 11.222.333/0001-81" in texto
    assert "Pix (E-mail): financeiro@bws.com.br" in texto


def test_conta_sem_pix_ainda_produz_bloco_util(cenario):
    texto = svc_contas.texto_para_copiar(cenario["conta"], cenario["bws"])
    assert "Agência: 1234" in texto
    assert "Pix" not in texto, "sem chave, não inventa linha de Pix"


# ---------------------------------------------------------------------------
# 3, 4 e 5. Emissão por empresa
# ---------------------------------------------------------------------------
def test_empresa_nova_nasce_manual_e_em_homologacao(cenario):
    """Empresa recém-cadastrada não sai emitindo nota fiscal de verdade porque
    alguém esqueceu de configurar."""
    e = cenario["outra"]
    assert e.emissao_modo == "MANUAL"
    assert e.emissao_ambiente == "HOMOLOGACAO"
    assert e.emissao_canal == "NACIONAL"


def test_duas_empresas_em_municipios_diferentes(cenario):
    """O caso real que o dono descreveu: uma em Eusébio por API, outra em
    Petrolina no manual."""
    s = cenario["s"]
    svc_emissao.definir(s, cenario["bws"].id, {
        "modo": "API", "codigo_ibge": "2304285", "municipio": "Eusébio/CE",
        "url_base": "https://ce-eusebio-pm-nfs-backend.cloud.el.com.br/nfse40",
        "token": "token-do-eusebio", "aliquota_iss": "2,5"})
    svc_emissao.definir(s, cenario["outra"].id, {
        "modo": "MANUAL", "codigo_ibge": "2611101", "municipio": "Petrolina/PE"})

    a = svc_emissao.ler(s, cenario["bws"])
    b = svc_emissao.ler(s, cenario["outra"])
    assert a["modo"] == "API" and a["municipio_conhecido"] == "Eusébio/CE"
    assert b["modo"] == "MANUAL" and b["municipio_conhecido"] == "Petrolina/PE"
    assert cenario["bws"].emissao_aliquota_iss is not None


def test_o_token_vai_cifrado_e_nao_volta_pela_tela(cenario):
    s = cenario["s"]
    svc_emissao.definir(s, cenario["bws"].id, {
        "modo": "MANUAL", "token": "segredo-do-canal"})
    guardado = cenario["bws"].emissao_token_cifrado
    assert guardado and "segredo-do-canal" not in guardado, "o token não pode ficar em claro"
    assert svc_emissao.token_de(cenario["bws"]) == "segredo-do-canal"

    lido = svc_emissao.ler(s, cenario["bws"])
    assert lido["tem_token"] is True
    assert "token" not in lido, "a tela sabe que existe; não recebe o valor"
    assert "segredo-do-canal" not in str(lido)


def test_sem_a_chave_de_segredos_o_token_nao_e_guardado_em_claro(cenario, monkeypatch):
    """Recusar é melhor que guardar aberto: o token assina em nome da empresa."""
    monkeypatch.delenv("ERP_CHAVE_SEGREDOS", raising=False)
    with pytest.raises(ErroValidacao) as e:
        svc_emissao.definir(cenario["s"], cenario["bws"].id,
                            {"modo": "MANUAL", "token": "seja o que for"})
    assert "ERP_CHAVE_SEGREDOS" in str(e.value)
    assert cenario["bws"].emissao_token_cifrado is None


# ---------------------------------------------------------------------------
# 6. Ligar a API sem ter para onde mandar
# ---------------------------------------------------------------------------
def test_api_sem_endereco_e_recusada_com_mensagem_em_portugues(cenario):
    with pytest.raises(ErroValidacao) as e:
        svc_emissao.definir(cenario["s"], cenario["outra"].id, {"modo": "API"})
    assert "endereço do serviço" in str(e.value)


def test_o_banco_tambem_recusa_api_sem_destino(cenario):
    """Mesmo que um código futuro esqueça a regra."""
    s = cenario["s"]
    e = cenario["outra"]
    e.emissao_modo = "API"
    e.emissao_url_base = None
    with pytest.raises(IntegrityError):
        s.flush()
    s.rollback()


def test_endereco_sem_https_e_recusado(cenario):
    """Token e nota assinada não viajam em claro."""
    with pytest.raises(ErroValidacao) as e:
        svc_emissao.definir(cenario["s"], cenario["bws"].id, {
            "modo": "API", "codigo_ibge": "2304285",
            "url_base": "http://ce-eusebio-pm-nfs-backend.cloud.el.com.br/nfse40"})
    assert "https" in str(e.value)


def test_codigo_ibge_torto_e_recusado(cenario):
    with pytest.raises(ErroValidacao) as e:
        svc_emissao.definir(cenario["s"], cenario["bws"].id,
                            {"modo": "MANUAL", "codigo_ibge": "230"})
    assert "7 dígitos" in str(e.value)


def test_diz_o_que_falta_para_emitir_por_api(cenario):
    """Dizer só "não dá" faria a pessoa adivinhar."""
    s = cenario["s"]
    e = cenario["outra"]
    e.emissao_modo = "API"
    e.emissao_url_base = "https://exemplo"
    e.emissao_codigo_ibge = "2611101"
    s.flush()
    pode, falta = svc_emissao.pode_emitir(e)
    assert pode is False
    assert "o token do canal" in falta
    assert "a inscrição municipal da empresa" in falta


# ---------------------------------------------------------------------------
# A TRAVA DO RATEIO ENTRE CONTAS DIFERENTES
#
# Decisão do dono, com o argumento que fecha a questão: *"como é que eu vou
# pagar um boleto de duas contas bancárias? É impossível."*
# ---------------------------------------------------------------------------
def _obra(s, codigo, conta_id=None):
    from app.apps.erp.db.models.cadastros import Obra
    o = Obra(codigo=codigo, nome=f"Obra {codigo}", conta_bancaria_id=conta_id)
    s.add(o)
    s.flush()
    return o


def _rateios(*obras):
    from decimal import Decimal
    from app.apps.erp.db.models.financeiro import Rateio
    return [Rateio(obra_id=o.id, valor=Decimal("100.00")) for o in obras]


def test_rateio_entre_obras_da_mesma_conta_passa(cenario):
    from app.apps.erp.core.titulos.service import _exigir_uma_conta_so
    s = cenario["s"]
    a = _obra(s, "OBRA-A", cenario["conta"].id)
    b = _obra(s, "OBRA-B", cenario["conta"].id)
    _exigir_uma_conta_so(s, _rateios(a, b))          # não levanta


def test_obra_sem_conta_nao_trava_o_lancamento(cenario):
    """Cadastro incompleto não pode parar o financeiro por um campo em branco."""
    from app.apps.erp.core.titulos.service import _exigir_uma_conta_so
    s = cenario["s"]
    a = _obra(s, "OBRA-C", cenario["conta"].id)
    b = _obra(s, "OBRA-D", None)
    _exigir_uma_conta_so(s, _rateios(a, b))          # não levanta


def test_rateio_entre_contas_diferentes_e_recusado_dizendo_quais(cenario):
    """A mensagem tem de dizer QUAIS obras e QUAIS contas — senão quem lançou
    fica adivinhando o que separar."""
    from app.apps.erp.core.comum.auditoria import ErroValidacao
    from app.apps.erp.core.titulos.service import _exigir_uma_conta_so
    from app.apps.erp.db.models.cadastros import ContaBancaria
    s = cenario["s"]
    outra_conta = ContaBancaria(descricao="Banco do Brasil 9876-5", banco_codigo="001",
                                agencia="9876", conta="54321-0", ativo=True)
    s.add(outra_conta)
    s.flush()
    a = _obra(s, "OBRA-E", cenario["conta"].id)
    b = _obra(s, "OBRA-F", outra_conta.id)

    with pytest.raises(ErroValidacao) as e:
        _exigir_uma_conta_so(s, _rateios(a, b))
    msg = str(e.value)
    assert "CONTAS DIFERENTES" in msg
    assert "OBRA-E" in msg and "OBRA-F" in msg
    assert "Bradesco 1234-5" in msg and "Banco do Brasil 9876-5" in msg
    assert "Separe em dois títulos" in msg, "a mensagem tem de dizer a saída"
