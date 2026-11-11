# -*- coding: utf-8 -*-
"""
Trocar a obra de uma TARIFA pelo Explorador.

10/11/2026: o dono filtrou "Taxas, Tarifas Bancárias e IOF" (~600 linhas),
trocou a obra de uma e o OMIE respondeu "Lançamento não cadastrado para o
Código [11266605823]". A tarifa é um lançamento feito direto na conta corrente,
não um título — e o painel perguntava por ela no endereço dos títulos.

O que se prova aqui:
  1. o lançamento de conta vai pelo endereço dele, partindo do lançamento
     CONSULTADO, com a obra inteira em valor;
  2. a trava do rateio vale para ele também;
  3. excluir não alcança lançamento de conta (o número dele, no endereço dos
     títulos, poderia apagar o título que tivesse o mesmo número);
  4. o que o OMIE aceitou aparece no painel na hora — e continua lá depois de
     refazer os números;
  5. linha sem número do OMIE não é marcável (marcar uma marcava todas).
"""
from __future__ import annotations

import json
import os

import pytest

from app.apps.painel.sync import omie_escrita

pytestmark = pytest.mark.banco

LANCAMENTO_DO_OMIE = {
    "nCodLanc": 11266605823,
    "cabecalho": {"nCodCC": 77, "dDtLanc": "04/08/2026", "nValorLanc": 6293.10},
    "detalhes": {"cCodCateg": "2.10.01", "cTipo": "DEB", "nCodCliente": 9,
                 "cObs": "TARIFA"},
    "info": {"dInc": "04/08/2026"},
}


# ===========================================================================
# 1. Preparar — sem banco, sem rede
# ===========================================================================
def test_a_obra_vai_inteira_em_valor_e_o_resto_volta_como_veio():
    novo, mudancas = omie_escrita.preparar_alteracao_cc(
        LANCAMENTO_DO_OMIE, 11266605823, cod_departamento="D9")
    assert novo["departamentos"] == [{"cCodDep": "D9", "nValDep": 6293.10}]
    assert novo["cabecalho"] == LANCAMENTO_DO_OMIE["cabecalho"]
    assert novo["detalhes"] == LANCAMENTO_DO_OMIE["detalhes"]
    assert novo["nCodLanc"] == 11266605823
    assert "info" not in novo
    assert mudancas == ["Departamento: — → D9 (100%)"]


def test_a_categoria_muda_nos_detalhes_sem_mexer_no_original():
    novo, mudancas = omie_escrita.preparar_alteracao_cc(
        LANCAMENTO_DO_OMIE, 1, codigo_categoria="2.20.02")
    assert novo["detalhes"]["cCodCateg"] == "2.20.02"
    assert LANCAMENTO_DO_OMIE["detalhes"]["cCodCateg"] == "2.10.01"
    assert mudancas == ["Categoria: 2.10.01 → 2.20.02"]
    assert "departamentos" not in novo


def test_ja_na_obra_pedida_nao_muda_nada():
    resposta = dict(LANCAMENTO_DO_OMIE,
                    departamentos=[{"cCodDep": "D9", "nPerDep": 100}])
    _novo, mudancas = omie_escrita.preparar_alteracao_cc(
        resposta, 1, cod_departamento="D9")
    assert mudancas == []


def test_o_percentual_vai_junto_quando_o_lancamento_ja_vinha_assim():
    resposta = dict(LANCAMENTO_DO_OMIE, departamentos=[
        {"cCodDep": "D1", "nPerDep": 50, "nValDep": 3146.55},
        {"cCodDep": "D2", "nPerDep": 50, "nValDep": 3146.55}])
    novo, mudancas = omie_escrita.preparar_alteracao_cc(
        resposta, 1, cod_departamento="D9")
    assert novo["departamentos"] == [
        {"cCodDep": "D9", "nValDep": 6293.10, "nPerDep": 100}]
    assert mudancas == ["Departamento: D1, D2 → D9 (100%)"]


def test_a_resposta_embrulhada_tambem_serve():
    novo, _ = omie_escrita.preparar_alteracao_cc(
        {"lancamento": LANCAMENTO_DO_OMIE}, 1, cod_departamento="D9")
    assert novo["cabecalho"]["nCodCC"] == 77


def test_resposta_sem_lancamento_nao_envia_nada():
    with pytest.raises(ValueError):
        omie_escrita.preparar_alteracao_cc({"faultstring": "x"}, 1,
                                           cod_departamento="D9")


# ===========================================================================
# 2. Com banco de verdade
# ===========================================================================
TARIFA, DIVIDIDO, TITULO = 11266605823, 11300000001, 501


@pytest.fixture()
def base_com_tarifa():
    from tests.conftest import VARIAVEL_BANCO_TESTE, url_de_teste_segura

    bruto = os.environ.get(VARIAVEL_BANCO_TESTE, "").strip()
    if not bruto:
        pytest.skip(f"{VARIAVEL_BANCO_TESTE} não definida — testes com banco pulados")
    os.environ["DATABASE_URL"] = url_de_teste_segura(bruto)

    from app.apps.painel import consultas, db as painel_db, migracoes_runner, saneamento
    from app.apps.painel.sync.fato import MARCA_LANCAMENTO_CC, NAO_APROP
    painel_db._engine = None
    resultado = migracoes_runner.aplicar_pendentes()
    assert not resultado.get("erro"), resultado

    def limpar(conn):
        conn.execute("TRUNCATE TABLE fato")
        conn.execute("DELETE FROM rateio")
        conn.execute("DELETE FROM alteracoes_omie")
        conn.execute("DELETE FROM apropriacao_lancamentos_cc")
        conn.execute("DELETE FROM movimentos_sem_titulo")
        conn.execute("DELETE FROM cat WHERE codigo IN ('2.10.01', '2.20.02')")

    with painel_db.conexao() as conn:
        limpar(conn)
        linhas = [(TARIFA, NAO_APROP, NAO_APROP, MARCA_LANCAMENTO_CC),
                  (DIVIDIDO, "CASA", "ALFA", MARCA_LANCAMENTO_CC),
                  (DIVIDIDO, "PREDIO", "ALFA", MARCA_LANCAMENTO_CC),
                  (TITULO, "CASA", "ALFA", None)]
        for codigo, dep, proj, obs in linhas:
            conn.execute(
                "INSERT INTO fato (codigo_lancamento, tipo, analise, situacao,"
                " categoria, codigo_categoria, grupo, departamento, projeto,"
                " conta_corrente, razao_social, observacao,"
                " numero_documento, pago_recebido, a_pagar_receber, juros, multa)"
                " VALUES (?,'2. Contas a Pagar','DRE','Pago','Tarifas','2.10.01',"
                "         'Despesas Financeiras',?,?,'Bradesco','BANCO',?,"
                "         '',-100,0,0,0)", (codigo, dep, proj, obs))
        # a obra D9 existe no espelho (é de lá que sai o nome)
        conn.execute("INSERT INTO rateio (codigo_lancamento_omie, seq, ccoddep,"
                     " cdesdep, nperdep, nvaldep) VALUES (1,1,'D9','OBRA NOVA',100,1)")
        conn.execute("INSERT INTO cat (codigo, descricao, grupo) VALUES"
                     " ('2.20.02', 'Juros pagos', 'Despesas Financeiras')")
        conn.commit()
    consultas.esquecer_listas()
    saneamento.esquecer_cadastros()
    yield
    saneamento.esquecer_cadastros()
    with painel_db.conexao() as conn:
        limpar(conn)
        conn.commit()


class OmieFalso:
    def __init__(self):
        self.consultados, self.enviados = [], []

    def consultar_lancamento_cc(self, codigo):
        self.consultados.append(("cc", codigo))
        return dict(LANCAMENTO_DO_OMIE, nCodLanc=codigo)

    def consultar_titulo(self, codigo, tipo):
        self.consultados.append((tipo, codigo))
        return {"codigo_lancamento_omie": codigo, "codigo_categoria": "2.10.01",
                "distribuicao": [{"cCodDep": "D1", "nPerDep": 100}]}

    def alterar_lancamento_cc(self, lancamento):
        self.enviados.append(("cc", lancamento))
        return {"cCodStatus": "0"}

    def alterar_titulo(self, cadastro, tipo):
        self.enviados.append((tipo, cadastro))
        return {"codigo_status": "0"}

    def excluir_titulo(self, codigo, tipo):
        self.enviados.append(("excluir", codigo))
        return {}


def test_a_tarifa_vai_pelo_endereco_dos_lancamentos_de_conta(base_com_tarifa, monkeypatch):
    from app.apps.painel import saneamento
    monkeypatch.setenv("PAINEL_SENHA_ESCRITA", "x")
    omie = OmieFalso()
    r = saneamento.aplicar([TARIFA, TITULO], departamento_novo="D9",
                           simulacao=False, cliente=omie)
    assert r["alterados"] == 2
    assert ("cc", TARIFA) in omie.consultados
    assert ("pagar", TITULO) in omie.consultados
    tipos = sorted(t for t, _ in omie.enviados)
    assert tipos == ["cc", "pagar"]
    enviado = next(l for t, l in omie.enviados if t == "cc")
    assert enviado["departamentos"] == [{"cCodDep": "D9", "nValDep": 6293.10}]
    assert r["titulos_alterados"] == 1


def test_o_lancamento_dividido_entre_obras_e_recusado(base_com_tarifa):
    from app.apps.painel import saneamento
    omie = OmieFalso()
    r = saneamento.aplicar([DIVIDIDO], departamento_novo="D9",
                           simulacao=True, cliente=omie)
    assert r["linhas"][0]["resultado"].startswith("RECUSADO: este lançamento")
    assert omie.consultados == []


def test_o_nao_apropriado_nao_conta_como_obra_para_a_trava(base_com_tarifa):
    from app.apps.painel import saneamento
    r = saneamento.aplicar([TARIFA], departamento_novo="D9", simulacao=True,
                           cliente=OmieFalso())
    assert r["linhas"][0]["resultado"] == "Ensaio: nada foi enviado."


def test_o_alterado_aparece_no_painel_na_hora(base_com_tarifa, monkeypatch):
    from app.apps.painel import saneamento
    from app.apps.painel.db import consultar
    monkeypatch.setenv("PAINEL_SENHA_ESCRITA", "x")
    saneamento.aplicar([{"codigo": TARIFA, "departamento": "D9",
                         "categoria": "2.20.02"}],
                       simulacao=False, cliente=OmieFalso())
    assert consultar("SELECT departamento, categoria, codigo_categoria FROM fato"
                     " WHERE codigo_lancamento = ?", (TARIFA,)) == [
        ("OBRA NOVA", "Juros pagos", "2.20.02")]
    # a apropriação guardada passa a ser a enviada: refazer os números a lê
    (bruto,), = consultar("SELECT bruto FROM apropriacao_lancamentos_cc"
                          " WHERE ncodmovcc = ?", (TARIFA,))
    assert json.loads(bruto)["departamentos"][0]["cCodDep"] == "D9"


def test_o_ensaio_nao_mexe_no_painel(base_com_tarifa):
    from app.apps.painel import saneamento
    from app.apps.painel.db import consultar
    from app.apps.painel.sync.fato import NAO_APROP
    saneamento.aplicar([TARIFA], departamento_novo="D9", simulacao=True,
                       cliente=OmieFalso())
    assert consultar("SELECT departamento FROM fato WHERE codigo_lancamento = ?",
                     (TARIFA,)) == [(NAO_APROP,)]


def test_excluir_nao_alcanca_lancamento_de_conta(base_com_tarifa, monkeypatch):
    from app.apps.painel import saneamento
    monkeypatch.setenv("PAINEL_SENHA_ESCRITA", "x")
    omie = OmieFalso()
    r = saneamento.excluir([TARIFA], simulacao=False, cliente=omie)
    assert r["linhas"][0]["resultado"].startswith("RECUSADO")
    assert omie.enviados == []


# ===========================================================================
# 3. A tela
# ===========================================================================
def test_linha_sem_numero_do_omie_nao_e_marcavel():
    """Ela saía com data-codigo="None", e todas as sem número viravam o mesmo
    título: marcar uma marcava todas."""
    from pathlib import Path
    html = (Path(__file__).resolve().parents[1] / "app" / "apps" / "painel"
            / "templates" / "painel_explorador.html").read_text(encoding="utf-8")
    assert "{% set tem_codigo = l.codigo_lancamento is not none %}" in html
    assert '{% if tem_codigo %}data-codigo="{{ l.codigo_lancamento }}"{% endif %}' in html


# A RESPOSTA DE VERDADE do OMIE (o dono baixou em 10/11/2026 pelo cartão
# "Baixar um lançamento de conta corrente"): tarifa de 28/12/2020 apropriada
# no OMIE e "(não apropriado)" no painel — que nunca tinha perguntado.
RESPOSTA_REAL = {
    "nCodLanc": 709793529, "cCodIntLanc": "", "nCodAgrup": 709793529,
    "cabecalho": {"dDtLanc": "28/12/2020", "nCodCC": 584262226, "nValorLanc": 1280.31},
    "detalhes": {"aCodCateg": [], "cCodCateg": "2.05.04", "cNumDoc": "",
                 "cObs": "ENCARGOS FINANC ROTATIVO", "cTipo": "99999",
                 "nCodCliente": 0, "nCodProjeto": 0},
    "transferencia": {"nCodCCDestino": 0},
    "departamentos": [{"cCodDep": "583753491", "nPerDep": 100, "nValDep": 1280.31}],
    "diversos": {"cNatureza": "P", "cOrigem": "EXTP", "dDtConc": "08/03/2021"},
    "info": {"dAlt": "08/03/2021", "dInc": "08/03/2021"},
}


def test_a_resposta_real_do_omie_e_lida_com_a_obra():
    from app.apps.painel.sync import fato
    assert fato._departamentos_do_bruto(RESPOSTA_REAL) == [("583753491", 1.0)]


def test_a_resposta_real_vira_alteracao_com_o_mesmo_vocabulario():
    novo, mudancas = omie_escrita.preparar_alteracao_cc(
        RESPOSTA_REAL, 709793529, codigo_categoria="2.05.05", cod_departamento="D9")
    assert novo["departamentos"] == [{"cCodDep": "D9", "nValDep": 1280.31, "nPerDep": 100}]
    assert novo["detalhes"]["cCodCateg"] == "2.05.05"
    assert "aCodCateg" not in novo["detalhes"] and "info" not in novo
    assert len(mudancas) == 2


# ===========================================================================
# 11/11/2026 — o dono tentou trocar a obra de uma tarifa e leu só "A Omie
# bloqueou as chamadas por consumo excessivo e pediu 59 segundos".
# ===========================================================================
class _Resposta:
    def __init__(self, status, corpo):
        self.status_code, self._corpo, self.headers = status, corpo, {}
        self.text = json.dumps(corpo)

    def json(self):
        return self._corpo

    def raise_for_status(self):
        raise AssertionError("não era para chegar aqui")


class _Sessao:
    """Um OMIE que responde, na ordem, o que estiver na fila."""

    def __init__(self, *respostas):
        self.fila, self.chamadas = list(respostas), 0

    def post(self, url, data=None, timeout=None):
        self.chamadas += 1
        return self.fila.pop(0)


def _cliente_com(*respostas):
    from app.apps.painel.sync.omie_escrita import OmieEscrita
    # o teto curto é o da tela (`de_ambiente` o põe): acima dele não se espera
    cli = OmieEscrita("k", "s", pausa_entre_chamadas=0, backoff_base=0.001,
                      teto_de_espera=30)
    cli.sessao = _Sessao(*respostas)
    return cli


def test_gravacao_recusada_nao_e_repetida_e_mostra_o_motivo():
    """Repetir a gravação que o OMIE recusou é o que gera o "consumo
    redundante" — e esconde o motivo. Uma chamada só, e o erro como veio."""
    from app.apps.painel.sync.omie_client import OmieAPIError
    cli = _cliente_com(_Resposta(500, {"faultstring": "ERROR: Tag [DIVERSOS] não faz parte da estrutura"}),
                       _Resposta(200, {"ok": True}))
    with pytest.raises(OmieAPIError) as e:
        cli.alterar_lancamento_cc({"nCodLanc": 1})
    assert "DIVERSOS" in str(e.value)
    assert cli.sessao.chamadas == 1


def test_o_bloqueio_conta_o_que_o_omie_disse_antes():
    """Na LEITURA a chamada ainda é repetida; se a repetição virar bloqueio, a
    mensagem leva a primeira resposta — que é o motivo de verdade."""
    from app.apps.painel.sync.omie_client import OmieBloqueada
    cli = _cliente_com(
        _Resposta(500, {"faultstring": "ERROR: lançamento com problema X"}),
        _Resposta(500, {"faultstring": "Consumo redundante detectado. Aguarde 59 segundos."}))
    with pytest.raises(OmieBloqueada) as e:
        cli.consultar_lancamento_cc(1)
    texto = str(e.value)
    assert "Consumo redundante" in texto and "lançamento com problema X" in texto
    assert e.value.segundos >= 59


def test_gravacao_bloqueada_de_cara_continua_sendo_bloqueio():
    from app.apps.painel.sync.omie_client import OmieBloqueada
    cli = _cliente_com(_Resposta(425, {"faultstring": "API bloqueada por consumo indevido. Tente novamente em 664 segundos."}))
    with pytest.raises(OmieBloqueada) as e:
        cli.alterar_lancamento_cc({"nCodLanc": 1})
    assert e.value.segundos >= 664 and cli.sessao.chamadas == 1


def test_a_gravacao_so_manda_o_que_e_da_gravacao():
    """`diversos` (os dados da conciliação), `info` e `nCodAgrup` vêm na
    consulta e não voltam; bloco vazio também não."""
    novo, _ = omie_escrita.preparar_alteracao_cc(RESPOSTA_REAL, 709793529,
                                                 cod_departamento="D9")
    assert set(novo) == {"nCodLanc", "cCodIntLanc", "cabecalho", "detalhes", "departamentos"}
    assert "aCodCateg" not in novo["detalhes"]
    assert novo["detalhes"]["cCodCateg"] == "2.05.04"
