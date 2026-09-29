# -*- coding: utf-8 -*-
"""
A carga do ponto (Mobponto) — 27/09/2026, com banco de verdade.

⚠️ A API É DUBLADA, e o contrato dela NÃO é suposto: foi lido dos Apps Script que
o dono mandou. O que o dublê devolve é a forma que os scripts dele tratam:

    {"result": {"total_paginas": N,
                "funcionarios": [{"cpf", "nome",
                                  "relatorio": [{"dia", "matricula", …}]}]}}

⚠️ OS CAMPOS DE CADA DIA SÃO DINÂMICOS — o script do dono os descobre em tempo de
execução, e eu também. Por isso os testes usam nomes de campo INVENTADOS de
propósito: se o código dependesse de um nome específico, eles quebrariam. O que
tem de funcionar é guardar o dia inteiro e ANOTAR quais campos vieram.
"""
import datetime as dt
import json
import pathlib

import pytest

pytestmark = pytest.mark.banco


@pytest.fixture
def banco_ponto(banco_analisesps, monkeypatch):
    """O schema `analisesps` limpo para este teste.

    ⚠️ ERA UM REFAZ-TUDO: `DROP SCHEMA` mais os 36 arquivos de migração, A CADA
    TESTE. Com 727 testes de banco na suíte, isso dava ~26 mil execuções de
    arquivo SQL por rodada para construir sempre a mesma coisa — e era a maior
    conta do tempo que o dono cobrou em 29/09/2026.

    Agora o schema nasce uma vez por sessão e as tabelas são esvaziadas entre os
    testes. O isolamento é o mesmo: tabelas vazias e contadores de `id` zerados.
    Ver `banco_analisesps` no `conftest.py`."""
    # As credenciais são de mentira: a API é dublada, nenhuma requisição sai.
    monkeypatch.setenv("MOBPONTO_AUTHORIZATION", "Basic de-mentira")
    monkeypatch.setenv("MOBPONTO_API_KEY", "chave-de-mentira")

def dia(numero, **extra):
    """Um dia como a API devolve. Os campos além de `dia` e `matricula` são
    inventados de propósito — ver o cabeçalho deste arquivo."""
    base = {"dia": f"{numero:02d}/08/2026", "matricula": "1234",
            "campo_que_eu_nao_conheco": "CREPEOLINDA",
            "outro_campo": "08:00"}
    base.update(extra)
    return base


def resposta(funcionarios, total_paginas=1):
    return {"result": {"total_paginas": total_paginas,
                       "funcionarios": funcionarios}}


def pessoa_no_ponto(cpf="997.133.493-34", nome="GERLANIO GOMES LIMA", dias=2):
    return {"cpf": cpf, "nome": nome,
            "relatorio": [dia(n) for n in range(1, dias + 1)]}


def dublar(monkeypatch, paginas):
    """`paginas` é uma lista: o que a API devolve na página 1, 2, 3…"""
    from app.apps.analisesps import ponto
    pedidas = []

    def falso(ano, mes, pagina):
        pedidas.append((ano, mes, pagina))
        return paginas[pagina - 1] if pagina - 1 < len(paginas) else \
            resposta([], total_paginas=len(paginas))

    monkeypatch.setattr(ponto, "_pedir_pagina", falso)
    return pedidas


def test_a_migracao_031_roda_no_postgres(banco_ponto):
    from app.apps.analisesps import ponto
    assert ponto._pronto() is True
    assert ponto.cargas() == []


def test_a_carga_guarda_os_dias_de_cada_pessoa(banco_ponto, monkeypatch):
    from app.apps.analisesps import ponto

    dublar(monkeypatch, [resposta([pessoa_no_ponto(dias=3)])])
    resultado = ponto.carregar(2026, 8, quem="MARCELO")

    assert resultado["pessoas"] == 1
    assert resultado["dias"] == 3
    assert resultado["paginas_lidas"] == 1

    carga = ponto.carga_do_mes(2026, 8)
    assert carga["competencia"] == "08/2026"
    assert carga["dias"] == 3
    assert carga["carregado_por"] == "MARCELO"
    assert carga["completa"] is True


def test_os_CAMPOS_QUE_VIERAM_ficam_anotados(banco_ponto, monkeypatch):
    """⚠️ É A DESCOBERTA QUE DESTRAVA A APROPRIAÇÃO. Sem saber quais campos a API
    manda em cada dia, mapear a obra e as marcações seria palpite — e palpite
    aqui decide em qual obra cai o salário de 500 pessoas."""
    from app.apps.analisesps import ponto

    dublar(monkeypatch, [resposta([pessoa_no_ponto()])])
    resultado = ponto.carregar(2026, 8)

    assert "campo_que_eu_nao_conheco" in resultado["campos"]
    assert "outro_campo" in resultado["campos"]
    assert "dia" in resultado["campos"]
    # E ficam guardados, para a tela mostrar depois.
    assert "campo_que_eu_nao_conheco" in ponto.carga_do_mes(2026, 8)["campos"]


def test_o_dia_INTEIRO_fica_guardado_como_veio(banco_ponto, monkeypatch):
    """Guardar só o que eu entendo hoje jogaria fora o que falta mapear — e aí
    seria preciso recarregar tudo da API para mapear a obra."""
    from app.apps.analisesps import ponto

    dublar(monkeypatch, [resposta([pessoa_no_ponto(dias=1)])])
    resultado = ponto.carregar(2026, 8)

    amostra = ponto.amostra_de_dias(resultado["id"])
    assert len(amostra) == 1
    assert amostra[0]["campos"]["campo_que_eu_nao_conheco"] == "CREPEOLINDA"
    assert amostra[0]["campos"]["outro_campo"] == "08:00"
    assert amostra[0]["data"] == dt.date(2026, 8, 1)
    assert amostra[0]["cpf"] == "99713349334", "o CPF entra só com dígitos"


def test_a_data_do_dia_e_resolvida_e_a_obra_NAO_e_adivinhada(banco_ponto,
                                                             monkeypatch):
    """⚠️ Coluna vazia é pergunta aberta; coluna preenchida por palpite é resposta
    errada com cara de certa. A obra fica NULA até o nome do campo ser conhecido."""
    from app.apps.analisesps import ponto
    from app.apps.analisesps.db import consultar

    dublar(monkeypatch, [resposta([pessoa_no_ponto(dias=1)])])
    resultado = ponto.carregar(2026, 8)

    linhas = consultar(
        "SELECT data, obra, presenca, falta FROM analisesps.ponto_dia "
        " WHERE carga_id = ?", (resultado["id"],))
    assert linhas[0][0] == dt.date(2026, 8, 1)
    assert linhas[0][1] is None, "a obra não pode ser adivinhada"
    assert linhas[0][2] is None
    assert linhas[0][3] is None


def test_a_carga_LE_TODAS_as_paginas(banco_ponto, monkeypatch):
    from app.apps.analisesps import ponto

    pedidas = dublar(monkeypatch, [
        resposta([pessoa_no_ponto(cpf="99713349334", dias=1)], total_paginas=3),
        resposta([pessoa_no_ponto(cpf="03513441363", dias=1)], total_paginas=3),
        resposta([pessoa_no_ponto(cpf="11144477735", dias=1)], total_paginas=3),
    ])
    resultado = ponto.carregar(2026, 8)

    assert [p[2] for p in pedidas] == [1, 2, 3]
    assert resultado["pessoas"] == 3
    assert resultado["dias"] == 3
    assert resultado["paginas_lidas"] == 3


def test_pagina_VAZIA_no_meio_para_a_leitura_em_vez_de_insistir(banco_ponto,
                                                               monkeypatch):
    """É o sinal de fim que a API dá quando `total_paginas` vem otimista."""
    from app.apps.analisesps import ponto

    pedidas = dublar(monkeypatch, [
        resposta([pessoa_no_ponto(dias=1)], total_paginas=9),
        resposta([], total_paginas=9),
    ])
    resultado = ponto.carregar(2026, 8)

    assert len(pedidas) == 2, "não insistiu nas outras sete"
    assert resultado["dias"] == 1
    assert ponto.carga_do_mes(2026, 8)["completa"] is False, \
        "leu 2 de 9: a tela tem de poder dizer que veio pela metade"


def test_recarregar_o_MESMO_mes_substitui(banco_ponto, monkeypatch):
    """Duas cargas de 08/2026 deixariam qualquer contagem de dias ambígua."""
    from app.apps.analisesps import ponto

    dublar(monkeypatch, [resposta([pessoa_no_ponto(dias=3)])])
    ponto.carregar(2026, 8)
    dublar(monkeypatch, [resposta([pessoa_no_ponto(dias=1)])])
    ponto.carregar(2026, 8)

    assert len(ponto.cargas()) == 1
    assert ponto.carga_do_mes(2026, 8)["dias"] == 1


def test_meses_diferentes_convivem(banco_ponto, monkeypatch):
    from app.apps.analisesps import ponto

    dublar(monkeypatch, [resposta([pessoa_no_ponto(dias=1)])])
    ponto.carregar(2026, 7)
    ponto.carregar(2026, 8)
    assert [(c["mes"], c["ano"]) for c in ponto.cargas()] == [(8, 2026), (7, 2026)]


def test_dia_SEM_DATA_legivel_continua_guardado_com_aviso(banco_ponto, monkeypatch):
    """Jogar a linha fora esconderia o problema. Guardada, alguém entende por
    quê — e o aviso diz quantas são."""
    from app.apps.analisesps import ponto

    dublar(monkeypatch, [resposta([{
        "cpf": "99713349334", "nome": "GERLANIO",
        "relatorio": [dia(1), {"dia": "não é data", "matricula": "1"}]}])])
    resultado = ponto.carregar(2026, 8)

    assert resultado["dias"] == 2
    assert any("sem data" in a for a in resultado["avisos"])


def test_mes_SEM_NINGUEM_e_recusado_com_frase_util(banco_ponto, monkeypatch):
    from app.apps.analisesps import ponto

    dublar(monkeypatch, [resposta([])])
    with pytest.raises(ponto.ErroDoPonto) as erro:
        ponto.carregar(2026, 8)
    assert "não devolveu ninguém" in str(erro.value)
    assert "ponto lançado" in str(erro.value)


def test_a_carga_respeita_o_TETO_de_paginas(banco_ponto, monkeypatch):
    """A API dizer um número absurdo não pode travar o processo por horas."""
    from app.apps.analisesps import ponto

    monkeypatch.setattr(ponto, "MAXIMO_DE_PAGINAS", 2)
    pedidas = dublar(monkeypatch, [
        resposta([pessoa_no_ponto(cpf="99713349334", dias=1)], total_paginas=999),
        resposta([pessoa_no_ponto(cpf="03513441363", dias=1)], total_paginas=999),
        resposta([pessoa_no_ponto(cpf="11144477735", dias=1)], total_paginas=999),
    ])
    resultado = ponto.carregar(2026, 8)

    assert len(pedidas) == 2
    assert any("teto" in a for a in resultado["avisos"])
    assert any("incompleto" in a for a in resultado["avisos"])


def test_a_carga_grava_em_BLOCOS(banco_ponto, monkeypatch):
    """60 mil dias num mês é normal. Gravar tudo de uma vez faria o pico de
    memória subir numa instância de 2 GB dividida com 17 módulos."""
    from app.apps.analisesps import ponto

    monkeypatch.setattr(ponto, "DIAS_POR_BLOCO", 3)
    dublar(monkeypatch, [resposta([pessoa_no_ponto(dias=10)])])
    resultado = ponto.carregar(2026, 8)
    assert resultado["dias"] == 10


def test_apagar_leva_os_dias_junto(banco_ponto, monkeypatch):
    from app.apps.analisesps import ponto
    from app.apps.analisesps.db import consultar_um

    dublar(monkeypatch, [resposta([pessoa_no_ponto(dias=3)])])
    resultado = ponto.carregar(2026, 8)
    assert ponto.apagar(resultado["id"], quem="EU") is True
    assert ponto.cargas() == []
    assert consultar_um("SELECT count(*) FROM analisesps.ponto_dia")[0] == 0


def test_sem_credencial_recusa_dizendo_ONDE_criar(banco_ponto, monkeypatch):
    """⚠️ O recado tem de dizer o que fazer. "Não autorizado" mandaria o dono
    adivinhar."""
    from app.apps.analisesps import ponto

    monkeypatch.delenv("MOBPONTO_AUTHORIZATION", raising=False)
    assert ponto.configurado() is False
    with pytest.raises(ponto.ErroDoPonto) as erro:
        ponto.carregar(2026, 8)
    assert "MOBPONTO_AUTHORIZATION" in str(erro.value)
    assert "Render" in str(erro.value)


def test_com_as_duas_credenciais_o_modulo_se_diz_configurado(banco_ponto):
    from app.apps.analisesps import ponto
    assert ponto.configurado() is True


# ---------------------------------------------------------------------------
# A CONVERSA COM A API — a parte que fala com o mundo
#
# ⚠️ Aqui o dublê é o `requests`, não o `_pedir_pagina`: é justamente a montagem
# da URL, os cabeçalhos e a retentativa que precisam de prova. Estes testes não
# precisam de banco.
# ---------------------------------------------------------------------------
class RespostaFalsa:
    def __init__(self, status=200, corpo=None, texto=""):
        self.status_code = status
        self._corpo = corpo
        self.text = texto or json.dumps(corpo or {})

    def json(self):
        if self._corpo is None:
            raise ValueError("não é JSON")
        return self._corpo


def dublar_requests(monkeypatch, respostas):
    """Cada chamada consome a próxima resposta. Guarda o que foi pedido."""
    import requests
    chamadas = []
    fila = list(respostas)

    # ⚠️ O `**resto` NÃO É PREGUIÇA. Dublê com assinatura fechada já quebrou esta
    # suíte duas vezes: quando o código real ganha um argumento novo (`verify`, a
    # confiança do TLS), a chamada estoura DENTRO do `try` do módulo e o teste
    # falha dizendo outra coisa — some com o motivo verdadeiro.
    def falso_get(url, params=None, headers=None, timeout=None, **resto):
        chamadas.append({"url": url, "params": params, "headers": headers,
                         "timeout": timeout, **resto})
        if not fila:
            raise AssertionError("pediu mais vezes do que o esperado")
        proxima = fila.pop(0)
        if isinstance(proxima, Exception):
            raise proxima
        return proxima

    monkeypatch.setattr(requests, "get", falso_get)
    return chamadas


@pytest.mark.parametrize("sem_banco", [True])
def test_a_url_e_os_tres_cabecalhos_sao_os_do_script(monkeypatch, sem_banco):
    """O contrato foi lido dos Apps Script do dono: `type_data=FOLHA_BWS_EXCEL`,
    `status=false`, mês, ano e página — e os três cabeçalhos."""
    from app.apps.analisesps import ponto

    monkeypatch.setenv("MOBPONTO_AUTHORIZATION", "Basic abc")
    monkeypatch.setenv("MOBPONTO_API_KEY", "chave")
    chamadas = dublar_requests(monkeypatch, [
        RespostaFalsa(corpo={"result": {"total_paginas": 1, "funcionarios": []}})])

    ponto._pedir_pagina(2026, 8, 3)

    pedido = chamadas[0]
    assert "mobponto.com.br" in pedido["url"]
    assert pedido["params"]["type_data"] == "FOLHA_BWS_EXCEL"
    assert pedido["params"]["status"] == "false"
    assert pedido["params"]["mes"] == "8"
    assert pedido["params"]["ano"] == "2026"
    assert pedido["params"]["pagina"] == "3"
    assert pedido["headers"]["Authorization"] == "Basic abc"
    assert pedido["headers"]["api-key"] == "chave"
    assert pedido["headers"]["api-version"] == "1.0.0"
    assert pedido["timeout"], "sem timeout, uma API pendurada segura o processo"


def test_credencial_recusada_NAO_e_repetida(monkeypatch):
    """⚠️ 401 não melhora na terceira tentativa — insistir só demora, e a frase
    tem de dizer o que conferir."""
    from app.apps.analisesps import ponto

    monkeypatch.setenv("MOBPONTO_AUTHORIZATION", "Basic errado")
    monkeypatch.setenv("MOBPONTO_API_KEY", "chave")
    chamadas = dublar_requests(monkeypatch, [RespostaFalsa(status=401)])

    with pytest.raises(ponto.ErroDoPonto) as erro:
        ponto._pedir_pagina(2026, 8, 1)
    assert len(chamadas) == 1, "não pode repetir credencial recusada"
    assert "recusou a credencial" in str(erro.value)
    assert "MOBPONTO_AUTHORIZATION" in str(erro.value)


def test_falha_de_rede_e_TENTADA_de_novo(monkeypatch):
    """A API cai de vez em quando, e uma página perdida deixa buraco no mês."""
    from app.apps.analisesps import ponto

    monkeypatch.setenv("MOBPONTO_AUTHORIZATION", "Basic abc")
    monkeypatch.setenv("MOBPONTO_API_KEY", "chave")
    monkeypatch.setattr(ponto.time, "sleep", lambda s: None)
    chamadas = dublar_requests(monkeypatch, [
        RuntimeError("conexão caiu"),
        RespostaFalsa(status=500),
        RespostaFalsa(corpo={"result": {"total_paginas": 1, "funcionarios": []}}),
    ])

    assert ponto._pedir_pagina(2026, 8, 1)["result"]["total_paginas"] == 1
    assert len(chamadas) == 3


def test_depois_de_TRES_tentativas_desiste_dizendo_a_pagina(monkeypatch):
    from app.apps.analisesps import ponto

    monkeypatch.setenv("MOBPONTO_AUTHORIZATION", "Basic abc")
    monkeypatch.setenv("MOBPONTO_API_KEY", "chave")
    monkeypatch.setattr(ponto.time, "sleep", lambda s: None)
    dublar_requests(monkeypatch, [RespostaFalsa(status=500)] * 3)

    with pytest.raises(ponto.ErroDoPonto) as erro:
        ponto._pedir_pagina(2026, 8, 7)
    frase = str(erro.value)
    assert "página 7" in frase
    assert "08/2026" in frase


def test_resposta_que_NAO_e_JSON_diz_o_comeco_dela(monkeypatch):
    """Quando a API devolve HTML de erro, ver o começo do texto é o que explica
    o que aconteceu."""
    from app.apps.analisesps import ponto

    monkeypatch.setenv("MOBPONTO_AUTHORIZATION", "Basic abc")
    monkeypatch.setenv("MOBPONTO_API_KEY", "chave")
    monkeypatch.setattr(ponto.time, "sleep", lambda s: None)
    dublar_requests(monkeypatch, [
        RespostaFalsa(corpo=None, texto="<html>erro do servidor</html>")] * 3)

    with pytest.raises(ponto.ErroDoPonto) as erro:
        ponto._pedir_pagina(2026, 8, 1)
    assert "erro do servidor" in str(erro.value)


def test_sem_credencial_a_montagem_dos_cabecalhos_ja_recusa(monkeypatch):
    from app.apps.analisesps import ponto

    monkeypatch.delenv("MOBPONTO_AUTHORIZATION", raising=False)
    monkeypatch.delenv("MOBPONTO_API_KEY", raising=False)
    with pytest.raises(ponto.ErroDoPonto) as erro:
        ponto._cabecalhos()
    assert "MOBPONTO_API_KEY" in str(erro.value)
    # E diz de onde tirar os valores, sem os valores.
    assert "Apps Script" in str(erro.value)
    assert "troque a chave na origem" in str(erro.value)
