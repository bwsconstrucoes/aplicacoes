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
    # A pausa entre páginas é para o Mobponto de verdade; aqui só atrasaria.
    from app.apps.analisesps import ponto as _ponto
    monkeypatch.setattr(_ponto, "PAUSA_ENTRE_PAGINAS", 0)
    # A guarda da migração 037 é uma pergunta ao banco guardada em memória; o
    # teste que tira a coluna de propósito não pode contaminar os seguintes.
    from app.apps.analisesps import db
    db.esquecer_colunas()

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
    assert "não retornou colaboradores" in str(erro.value)
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


def test_depois_de_SEIS_tentativas_desiste_dizendo_a_pagina(monkeypatch):
    """Eram três tentativas em sete segundos; o Mobponto lento derrubava o mês
    inteiro (30/09/2026). Agora são seis, em uns oito minutos — e a frase diz que
    o que entrou ficou guardado e que a próxima carga continua dali."""
    from app.apps.analisesps import ponto

    monkeypatch.setenv("MOBPONTO_AUTHORIZATION", "Basic abc")
    monkeypatch.setenv("MOBPONTO_API_KEY", "chave")
    esperas = []
    monkeypatch.setattr(ponto.time, "sleep", esperas.append)
    dublar_requests(monkeypatch, [RespostaFalsa(status=500)] * 6)

    with pytest.raises(ponto.ErroDoPonto) as erro:
        ponto._pedir_pagina(2026, 8, 7)
    frase = str(erro.value)
    assert "página 7" in frase
    assert "08/2026" in frase
    assert "continua da página 7" in frase
    assert esperas == [15, 30, 60, 120, 240], "a paciência cresce a cada falha"


def test_resposta_que_NAO_e_JSON_diz_o_comeco_dela(monkeypatch):
    """Quando a API devolve HTML de erro, ver o começo do texto é o que explica
    o que aconteceu."""
    from app.apps.analisesps import ponto

    monkeypatch.setenv("MOBPONTO_AUTHORIZATION", "Basic abc")
    monkeypatch.setenv("MOBPONTO_API_KEY", "chave")
    monkeypatch.setattr(ponto.time, "sleep", lambda s: None)
    dublar_requests(monkeypatch, [
        RespostaFalsa(corpo=None, texto="<html>erro do servidor</html>")] * 6)

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


# ---------------------------------------------------------------------------
# O CERTIFICADO QUE FALTA — 29/09/2026
#
# A carga do ponto morria com CERTIFICATE_VERIFY_FAILED. A primeira versão do
# conserto oferecia só uma saída: DESLIGAR a verificação. Oferecer apenas a saída
# insegura empurra para ela — então agora existe a saída certa também.
# ---------------------------------------------------------------------------
def test_o_certificado_que_FALTA_pode_ser_colado_sem_baixar_a_seguranca(
        monkeypatch, tmp_path):
    """⚠️ `MOBPONTO_CA_EXTRA`: a peça do meio da corrente, colada em texto. A
    verificação CONTINUA ligada — o que faltava era só a peça."""
    from app.apps.analisesps import ponto

    pem = ("-----BEGIN CERTIFICATE-----\n"
           "REMENDODEMENTIRAPARAOTESTE\n"
           "-----END CERTIFICATE-----\n")
    monkeypatch.delenv("MOBPONTO_TLS_INSEGURO", raising=False)
    monkeypatch.setenv("MOBPONTO_CA_EXTRA", pem)

    caminho = ponto._confianca_tls()
    assert isinstance(caminho, str) and caminho.endswith(".pem")
    conteudo = open(caminho, encoding="utf-8").read()
    assert "REMENDODEMENTIRAPARAOTESTE" in conteudo
    # ⚠️ E as raízes de sempre continuam lá: o extra SOMA, não substitui. Trocar o
    # pacote inteiro por um certificado só faria toda a internet deixar de ser
    # confiável para este caminho.
    assert len(conteudo) > len(pem) * 2


def test_o_certificado_tambem_pode_vir_em_BASE64(monkeypatch):
    """⚠️ O painel do Render engole quebra de linha em variável de ambiente com
    facilidade, e um PEM sem as quebras certas não vale nada. Com base64 não há
    como estragar no caminho."""
    import base64

    from app.apps.analisesps import ponto

    pem = ("-----BEGIN CERTIFICATE-----\n"
           "OUTROREMENDODEMENTIRA\n"
           "-----END CERTIFICATE-----\n")
    monkeypatch.delenv("MOBPONTO_TLS_INSEGURO", raising=False)
    monkeypatch.setenv("MOBPONTO_CA_EXTRA",
                       base64.b64encode(pem.encode()).decode())

    caminho = ponto._confianca_tls()
    assert "OUTROREMENDODEMENTIRA" in open(caminho, encoding="utf-8").read()


def test_texto_que_NAO_e_certificado_nao_estraga_a_verificacao(monkeypatch):
    """⚠️ Devolver o pacote normal é o certo: a falha original volta a aparecer
    por inteiro, em vez de virar um erro diferente e confuso."""
    from app.apps.analisesps import ponto

    monkeypatch.delenv("MOBPONTO_TLS_INSEGURO", raising=False)
    monkeypatch.setenv("MOBPONTO_CA_EXTRA", "isto aqui não é certificado nenhum")

    valor = ponto._confianca_tls()
    assert valor is not False, "não pode desligar a verificação por engano"
    assert "mobponto-ca-" not in str(valor)


def test_desligar_a_verificacao_continua_sendo_o_ULTIMO_recurso(monkeypatch):
    """Ela existe, é decisão dele, e o log grita quando está ligada."""
    from app.apps.analisesps import ponto

    monkeypatch.setenv("MOBPONTO_TLS_INSEGURO", "1")
    monkeypatch.setenv("MOBPONTO_CA_EXTRA", "-----BEGIN CERTIFICATE-----\nX\n"
                                            "-----END CERTIFICATE-----")
    # ⚠️ O desligar VENCE o extra: se ele ligou o inseguro, é porque o extra não
    # resolveu. Tentar o extra primeiro faria a carga falhar de novo com o mesmo
    # erro, depois de ele já ter tomado a decisão.
    assert ponto._confianca_tls() is False


def test_a_mensagem_do_erro_oferece_o_caminho_SEGURO_primeiro(monkeypatch):
    """⚠️ A primeira versão oferecia só o desligar — e oferecer apenas a saída
    insegura empurra para ela."""
    import requests

    from app.apps.analisesps import ponto

    monkeypatch.setenv("MOBPONTO_AUTHORIZATION", "Basic x")
    monkeypatch.setenv("MOBPONTO_API_KEY", "y")
    monkeypatch.delenv("MOBPONTO_TLS_INSEGURO", raising=False)
    monkeypatch.delenv("MOBPONTO_CA_EXTRA", raising=False)

    def explode(*a, **k):
        raise requests.exceptions.SSLError(
            "CERTIFICATE_VERIFY_FAILED: unable to get local issuer certificate")

    monkeypatch.setattr(requests, "get", explode)
    with pytest.raises(ponto.ErroDoPonto) as erro:
        ponto._pedir_pagina(2026, 9, 1)

    frase = str(erro.value)
    assert "MOBPONTO_CA_EXTRA" in frase
    assert frase.index("MOBPONTO_CA_EXTRA") < frase.index("MOBPONTO_TLS_INSEGURO")
    assert "sem perda de segurança" in frase


# ---------------------------------------------------------------------------
# COMPLETAR A CADEIA SOZINHO — 29/09/2026
#
# ⚠️ É O QUE O NAVEGADOR FAZ, e é a diferença entre "funciona no navegador" e
# "não funciona aqui". O certificado do site carrega dentro de si o ENDEREÇO de
# quem o assinou; o navegador vai lá e baixa a peça que falta. O `requests` não.
#
# ⚠️ E NÃO ABRE BURACO: o certificado baixado entra como CANDIDATO, não como
# confiança. A verificação continua acontecendo e só passa se a corrente terminar
# numa raiz que já era confiável — um intermediário falso não chega a raiz nenhuma.
# ---------------------------------------------------------------------------
def test_o_intermediario_e_baixado_do_endereco_que_o_certificado_indica(
        monkeypatch):
    import ssl

    import requests

    from app.apps.analisesps import ponto

    pem_falso = ("-----BEGIN CERTIFICATE-----\n"
                 "INTERMEDIARIODEMENTIRA\n"
                 "-----END CERTIFICATE-----\n")

    class FolhaFalsa:
        class extensions:
            @staticmethod
            def get_extension_for_class(_classe):
                class Valor:
                    def __iter__(self):
                        from cryptography import x509

                        class Acesso:
                            access_method = (x509.oid
                                             .AuthorityInformationAccessOID
                                             .CA_ISSUERS)

                            class access_location:
                                value = "http://ca.exemplo/intermediario.crt"
                        return iter([Acesso()])

                class Ext:
                    value = Valor()
                return Ext()

    monkeypatch.setattr(ssl, "get_server_certificate",
                        lambda *a, **k: "-----BEGIN CERTIFICATE-----\nX\n"
                                        "-----END CERTIFICATE-----")
    from cryptography import x509
    monkeypatch.setattr(x509, "load_pem_x509_certificate",
                        lambda *a, **k: FolhaFalsa())

    class Resposta:
        content = pem_falso.encode()

        def raise_for_status(self):
            pass

    monkeypatch.setattr(requests, "get", lambda *a, **k: Resposta())

    achado = ponto._intermediario_do_servidor(
        "https://www.mobponto.com.br/ponto/api/endpoint.php")
    assert "INTERMEDIARIODEMENTIRA" in achado


def test_sem_endereco_no_certificado_ele_NAO_INVENTA(monkeypatch):
    """⚠️ Devolver vazio é o certo: a falha original volta por inteiro, e a
    mensagem manda ele colar o certificado à mão. Inventar um caminho aqui daria
    um erro diferente do de verdade."""
    import ssl

    from app.apps.analisesps import ponto

    def sem_certificado(*a, **k):
        raise OSError("não deu para falar com o site")

    monkeypatch.setattr(ssl, "get_server_certificate", sem_certificado)
    assert ponto._intermediario_do_servidor("https://exemplo.com/x") == ""


def test_a_mensagem_diz_que_JA_TENTOU_sozinho(monkeypatch):
    """Sem isso, a primeira coisa que ele pensaria é "será que o sistema tentou?"
    — e a resposta tem de estar na frase, não na cabeça de quem escreveu."""
    import requests

    from app.apps.analisesps import ponto

    monkeypatch.setenv("MOBPONTO_AUTHORIZATION", "Basic x")
    monkeypatch.setenv("MOBPONTO_API_KEY", "y")
    monkeypatch.delenv("MOBPONTO_TLS_INSEGURO", raising=False)
    monkeypatch.delenv("MOBPONTO_CA_EXTRA", raising=False)
    monkeypatch.setattr(ponto, "_intermediario_do_servidor", lambda url: "")

    def explode(*a, **k):
        raise requests.exceptions.SSLError("CERTIFICATE_VERIFY_FAILED")

    monkeypatch.setattr(requests, "get", explode)
    with pytest.raises(ponto.ErroDoPonto) as erro:
        ponto._pedir_pagina(2026, 9, 1)
    assert "O SISTEMA TENTOU OBTÊ-LO AUTOMATICAMENTE" in str(erro.value)


def test_quando_o_intermediario_APARECE_a_chamada_e_refeita(monkeypatch):
    """⚠️ O teste que prova o ganho: o erro de certificado deixa de ser final. A
    primeira tentativa falha, a peça é baixada, e a SEGUNDA tentativa já vai com o
    pacote completo."""
    import requests

    from app.apps.analisesps import ponto

    monkeypatch.setenv("MOBPONTO_AUTHORIZATION", "Basic x")
    monkeypatch.setenv("MOBPONTO_API_KEY", "y")
    monkeypatch.delenv("MOBPONTO_TLS_INSEGURO", raising=False)
    monkeypatch.delenv("MOBPONTO_CA_EXTRA", raising=False)
    monkeypatch.setattr(
        ponto, "_intermediario_do_servidor",
        lambda url: "-----BEGIN CERTIFICATE-----\nPECAQUEFALTAVA\n"
                    "-----END CERTIFICATE-----\n")

    chamadas = []

    class Ok:
        status_code = 200

        @staticmethod
        def json():
            return {"result": {"total_paginas": 1, "funcionarios": []}}

    def falso_get(url, **k):
        chamadas.append(k.get("verify"))
        if len(chamadas) == 1:
            raise requests.exceptions.SSLError("CERTIFICATE_VERIFY_FAILED")
        return Ok()

    monkeypatch.setattr(requests, "get", falso_get)
    ponto._pedir_pagina(2026, 9, 1)

    assert len(chamadas) == 2, "a segunda tentativa tem de acontecer"
    assert chamadas[1] != chamadas[0], "e com o pacote NOVO"
    assert "PECAQUEFALTAVA" in open(chamadas[1], encoding="utf-8").read()
    assert chamadas[1] is not False, "sem nunca desligar a verificação"


# ---------------------------------------------------------------------------
# 29/09/2026 — A CARGA QUE CAI NO MEIO NÃO PODE LEVAR A ANTERIOR JUNTO
#
# O dono: *"O que acontece se o ponto der problema pra baixar no meio do
# caminho?"* Até aqui: a carga antiga era apagada ANTES de a nova começar, e a
# folha passava a usar o pedaço que sobrou como se fosse o mês inteiro — sem
# aviso. Agora a antiga só sai quando a nova termina.
# ---------------------------------------------------------------------------
def dublar_que_cai_na_pagina_2(monkeypatch, primeira):
    from app.apps.analisesps import ponto

    def falso(ano, mes, pagina):
        if pagina == 1:
            return primeira
        raise ponto.ErroDoPonto("a rede caiu na página 2")

    monkeypatch.setattr(ponto, "_pedir_pagina", falso)


def test_carga_que_FALHA_no_meio_nao_destroi_a_anterior(banco_ponto, monkeypatch):
    from app.apps.analisesps import ponto

    dublar(monkeypatch, [resposta([pessoa_no_ponto(dias=3)])])
    ponto.carregar(2026, 8)

    dublar_que_cai_na_pagina_2(
        monkeypatch, resposta([pessoa_no_ponto(dias=1)], total_paginas=2))
    with pytest.raises(ponto.ErroDoPonto):
        ponto.carregar(2026, 8)

    # O mês continua valendo a carga que TERMINOU.
    assert ponto.carga_do_mes(2026, 8)["dias"] == 3
    assert len(ponto.dias_por_cpf(2026, 8)["99713349334"]) == 3
    # E a que caiu aparece na lista, dita como tal — não como "0 de 2".
    lista = ponto.cargas()
    assert [c["interrompida"] for c in lista] == [True, False]


def test_a_carga_seguinte_COMPLETA_a_interrompida(banco_ponto, monkeypatch):
    """Um pedaço de mês não é mês; a chamada seguinte continua o pedaço."""
    from app.apps.analisesps import ponto

    dublar_que_cai_na_pagina_2(
        monkeypatch, resposta([pessoa_no_ponto(dias=1)], total_paginas=2))
    with pytest.raises(ponto.ErroDoPonto):
        ponto.carregar(2026, 8)
    assert ponto.carga_do_mes(2026, 8) is None, "pedaço de mês não é mês"

    pedidas = dublar(monkeypatch, [
        resposta([pessoa_no_ponto(dias=1)], total_paginas=2),
        resposta([pessoa_no_ponto(cpf="222.222.222-22", dias=2)], total_paginas=2)])
    ponto.carregar(2026, 8)
    assert [p[2] for p in pedidas] == [2], "só a página que faltava"
    assert len(ponto.cargas()) == 1
    assert ponto.carga_do_mes(2026, 8)["dias"] == 3


def test_a_carga_terminada_SUBSTITUI_a_anterior_de_uma_vez(banco_ponto, monkeypatch):
    """Ou o mês troca de carga inteiro, ou não troca: nunca duas terminadas."""
    from app.apps.analisesps import ponto

    dublar(monkeypatch, [resposta([pessoa_no_ponto(dias=3)])])
    ponto.carregar(2026, 8)
    dublar(monkeypatch, [resposta([pessoa_no_ponto(dias=5)])])
    ponto.carregar(2026, 8)

    assert len(ponto.cargas()) == 1
    assert ponto.carga_do_mes(2026, 8)["dias"] == 5


# ---------------------------------------------------------------------------
# O AUTOMÁTICO DO DIA — *"Ele já está configurado pra baixar automático
# diariamente?"* Não estava. Agora há um modo para o agendador chamar.
# ---------------------------------------------------------------------------
def test_o_ponto_diario_traz_o_mes_corrente_e_ate_o_dia_10_o_anterior():
    import datetime as dt
    from app.apps.analisesps import ponto

    assert ponto.meses_do_ponto_diario(dt.date(2026, 9, 15)) == [(2026, 9)]
    assert ponto.meses_do_ponto_diario(dt.date(2026, 9, 10)) == [(2026, 8), (2026, 9)]
    # A virada do ano não confunde o mês anterior.
    assert ponto.meses_do_ponto_diario(dt.date(2027, 1, 3)) == [(2026, 12), (2027, 1)]


def test_o_ponto_diario_e_um_modo_que_o_agendador_pode_chamar():
    from app.apps.analisesps import tarefas

    assert "ponto_diario" in tarefas.MODOS
    assert tarefas.ETAPAS["ponto_diario"] == ["ponto_diario"]
    # E continua fora dos botões de Configurações, como o "ponto": ele não é
    # para gente apertar sem saber o mês — é para a máquina.
    assert "ponto_diario" not in tarefas.MODOS_DA_BASE


def test_SEM_a_migracao_037_a_carga_continua_funcionando_e_AVISA(
        banco_analisesps_mutilado, banco_ponto, monkeypatch):
    """O intervalo entre o código subir e o botão ser apertado: o índice ainda é
    o antigo (uma carga por mês, terminada ou não). A carga tem de continuar
    entrando — do jeito antigo, apagando antes — e dizer que foi assim."""
    from sqlalchemy import text
    from app.apps.analisesps import ponto
    from app.apps.analisesps.db import obter_engine

    from app.apps.analisesps import db
    with obter_engine().connect() as conn:
        conn.execute(text("DROP INDEX analisesps.ix_analisesps_ponto_carga_competencia"))
        conn.execute(text("ALTER TABLE analisesps.ponto_carga DROP COLUMN terminada_em"))
        conn.execute(text("CREATE UNIQUE INDEX ix_analisesps_ponto_carga_competencia "
                          "ON analisesps.ponto_carga (ano, mes)"))
        conn.commit()
    db.esquecer_colunas()
    assert ponto._substituicao_segura() is False

    dublar(monkeypatch, [resposta([pessoa_no_ponto(dias=3)])])
    ponto.carregar(2026, 8)
    dublar(monkeypatch, [resposta([pessoa_no_ponto(dias=5)])])
    feito = ponto.carregar(2026, 8)

    assert len(ponto.cargas()) == 1
    assert ponto.carga_do_mes(2026, 8)["dias"] == 5
    assert any("037" in a for a in feito["avisos"])


# ---------------------------------------------------------------------------
# *"Precisa que caso a carga pare que possa ser retomada de onde parou e que
# sejamos avisados."* — 29/09/2026
# ---------------------------------------------------------------------------
def dublar_que_cai_uma_vez(monkeypatch, paginas, cai_na):
    """A API de mentira: cai na página `cai_na` UMA vez; depois responde."""
    from app.apps.analisesps import ponto
    pedidas = []
    caiu = []

    def falso(ano, mes, pagina):
        pedidas.append(pagina)
        if pagina == cai_na and not caiu:
            caiu.append(True)
            raise ponto.ErroDoPonto(f"a rede caiu na página {pagina}")
        return paginas[pagina - 1]

    monkeypatch.setattr(ponto, "_pedir_pagina", falso)
    return pedidas


def _tres_paginas():
    return [resposta([pessoa_no_ponto(cpf="111.111.111-11", nome="UM", dias=2)], 3),
            resposta([pessoa_no_ponto(cpf="222.222.222-22", nome="DOIS", dias=3)], 3),
            resposta([pessoa_no_ponto(cpf="333.333.333-33", nome="TRES", dias=4)], 3)]


def test_a_carga_que_caiu_RETOMA_da_pagina_em_que_parou(banco_ponto, monkeypatch):
    from app.apps.analisesps import ponto
    pedidas = dublar_que_cai_uma_vez(monkeypatch, _tres_paginas(), cai_na=3)

    with pytest.raises(ponto.ErroDoPonto):
        ponto.carregar(2026, 8)
    parada = ponto.carga_em_andamento(2026, 8)
    assert parada["paginas_lidas"] == 2, "as duas páginas gravadas contam"
    assert ponto.carga_do_mes(2026, 8) is None
    assert [c["competencia"] for c in ponto.cargas_paradas()] == ["08/2026"]

    pedidas.clear()
    feito = ponto.carregar(2026, 8)

    assert pedidas == [3], "retomou da página 3 — não voltou à 1"
    assert feito["retomada"] is True
    assert any("retomada da página 3 de 3" in a for a in feito["avisos"])
    carga = ponto.carga_do_mes(2026, 8)
    assert carga["id"] == parada["id"], "é a MESMA carga, completada"
    assert carga["dias"] == 2 + 3 + 4 and carga["pessoas"] == 3
    assert carga["completa"] and not carga["interrompida"]
    assert len(ponto.cargas()) == 1
    assert ponto.cargas_paradas() == []


def test_a_retomada_NAO_duplica_quem_vier_de_novo(banco_ponto, monkeypatch):
    """Entre uma tentativa e outra, a pessoa pode mudar de página no Mobponto:
    a página é gravada POR PESSOA, então ela entra uma vez só."""
    from app.apps.analisesps import ponto
    paginas = _tres_paginas()
    dublar_que_cai_uma_vez(monkeypatch, paginas, cai_na=3)
    with pytest.raises(ponto.ErroDoPonto):
        ponto.carregar(2026, 8)
    # Na volta, a pessoa UM (já gravada na página 1) aparece de novo na 3.
    paginas[2] = resposta([pessoa_no_ponto(cpf="111.111.111-11", nome="UM", dias=5),
                           pessoa_no_ponto(cpf="333.333.333-33", nome="TRES", dias=4)], 3)
    ponto.carregar(2026, 8)

    dias = ponto.dias_por_cpf(2026, 8)
    assert len(dias["11111111111"]) == 5, "vale a versão mais nova, uma vez só"
    assert ponto.carga_do_mes(2026, 8)["pessoas"] == 3


def test_uma_tentativa_VELHA_nao_e_retomada(banco_ponto, monkeypatch):
    """Depois de um dia o Mobponto já mudou demais: recomeça do zero."""
    from app.apps.analisesps import ponto
    from app.apps.analisesps.db import conexao
    pedidas = dublar_que_cai_uma_vez(monkeypatch, _tres_paginas(), cai_na=3)
    with pytest.raises(ponto.ErroDoPonto):
        ponto.carregar(2026, 8)
    with conexao() as conn:
        conn.execute("UPDATE analisesps.ponto_carga SET carregado_em = now() - "
                     "interval '2 days'")
        conn.commit()

    pedidas.clear()
    feito = ponto.carregar(2026, 8)
    assert pedidas == [1, 2, 3]
    assert feito["retomada"] is False
    assert len(ponto.cargas()) == 1


def test_o_aviso_diz_o_mes_a_pagina_e_que_retoma(banco_ponto, monkeypatch):
    from app.apps.analisesps import avisos_ponto, ponto
    dublar_que_cai_uma_vez(monkeypatch, _tres_paginas(), cai_na=3)
    with pytest.raises(ponto.ErroDoPonto):
        ponto.carregar(2026, 8)

    mandados = []
    import app.apps.notificador as notificador
    monkeypatch.setattr(notificador, "notificar",
                        lambda **kw: mandados.append(kw) or {"whatsapp": {"ok": True}})
    monkeypatch.setenv("ANALISESPS_AVISO_TELEFONE", "85 99999-0001; 85 99999-0002")

    resultado = avisos_ponto.avisar_que_parou("a rede caiu na página 3")

    assert sorted(resultado) == ["85999990001", "85999990002"]
    texto = mandados[0]["mensagem"]
    assert "08/2026" in texto
    assert "página 3 de 3" in texto
    assert "2 página(s) já guardada(s)" in texto
    assert "a rede caiu" in texto
    assert "continua de onde parou" in texto


def test_o_aviso_NUNCA_derruba_a_tarefa(monkeypatch):
    from app.apps.analisesps import avisos_ponto
    import app.apps.notificador as notificador

    def estoura(**kw):
        raise RuntimeError("Z-API fora do ar")
    monkeypatch.setattr(notificador, "notificar", estoura)
    monkeypatch.setenv("ANALISESPS_AVISO_TELEFONE", "85999990001")
    from app.apps.analisesps import ponto
    monkeypatch.setattr(ponto, "cargas_paradas", lambda: [])

    resultado = avisos_ponto.avisar_que_parou("x")
    assert resultado["85999990001"]["ok"] is False


def test_sem_telefone_configurado_vale_a_lista_do_baixabradesco(monkeypatch):
    from app.apps.analisesps import avisos_ponto
    monkeypatch.delenv("ANALISESPS_AVISO_TELEFONE", raising=False)
    from app.apps.baixabradesco.avisos import TELEFONES_AVISO
    assert avisos_ponto.telefones() == [t for t in TELEFONES_AVISO]


def test_a_execucao_do_ponto_que_MORREU_com_o_servico_avisa(banco_ponto, monkeypatch):
    """O serviço reiniciou no meio (uma publicação). Ninguém vê o erro: a
    execução só é dada por morta quando a próxima abre — e é aí que se avisa."""
    from app.apps.analisesps import avisos_ponto, tarefas
    from app.apps.analisesps.db import conexao

    avisados = []
    monkeypatch.setattr(avisos_ponto, "avisar_que_parou",
                        lambda motivo: avisados.append(motivo) or {})
    with conexao() as conn:
        conn.execute("INSERT INTO analisesps.execucoes (tipo, disparo, etapa, visto_em) "
                     "VALUES ('ponto', 'botão', 'trazendo', now() - interval '1 hour')")
        conn.commit()
        assert tarefas._fechar_orfas(conn) == 1
    assert len(avisados) == 1 and "reiniciou" in avisados[0]

    # A sincronização que morre não avisa ninguém: ela roda de 5 em 5 minutos.
    with conexao() as conn:
        conn.execute("INSERT INTO analisesps.execucoes (tipo, disparo, etapa, visto_em) "
                     "VALUES ('sincronizar', 'tela', 'lendo', now() - interval '1 hour')")
        conn.commit()
        assert tarefas._fechar_orfas(conn) == 1
    assert len(avisados) == 1


def test_a_execucao_do_ponto_que_FALHA_avisa_e_a_da_sincronizacao_nao(banco_ponto, monkeypatch):
    from app.apps.analisesps import avisos_ponto, ponto, tarefas
    from app.apps.analisesps.db import conexao

    avisados = []
    monkeypatch.setattr(avisos_ponto, "avisar_que_parou",
                        lambda motivo: avisados.append(motivo) or {})
    monkeypatch.setattr(ponto, "meses_do_ponto_diario", lambda hoje=None: [(2026, 8)])
    monkeypatch.setattr(tarefas.time, "sleep", lambda s: None)

    def cai(*a, **k):
        raise ponto.ErroDoPonto("a rede caiu")
    monkeypatch.setattr(ponto, "carregar", cai)

    with conexao() as conn:
        execucao = tarefas._abrir_execucao(conn, "ponto_diario", "agendador")
    assert tarefas.executar_trabalho("ponto_diario", execucao) is False
    assert avisados and "a rede caiu" in avisados[0]


# ---------------------------------------------------------------------------
# 30/09/2026 — *"ponto não conclui, não sai disso"*: "Read timed out (read
# timeout=60)" na página 7. O Mobponto monta cada página na hora e algumas
# levam mais de um minuto. O script dele que funciona pede UMA página por minuto
# e, quando uma falha, tenta de novo no minuto seguinte — nunca desiste do mês.
# ---------------------------------------------------------------------------
def test_uma_pagina_tem_TRES_MINUTOS_para_responder(monkeypatch):
    from app.apps.analisesps import ponto

    monkeypatch.setenv("MOBPONTO_AUTHORIZATION", "Basic abc")
    monkeypatch.setenv("MOBPONTO_API_KEY", "chave")
    chamadas = dublar_requests(monkeypatch, [
        RespostaFalsa(corpo={"result": {"total_paginas": 1, "funcionarios": []}})])
    ponto._pedir_pagina(2026, 9, 7)

    conectar, ler = chamadas[0]["timeout"]
    assert ler >= 180, "60 s foi pouco para a página 7"
    assert conectar <= 30, "servidor que nem atende não merece 3 minutos"


def test_o_tempo_esgotado_e_TENTADO_DE_NOVO_com_paciencia(monkeypatch):
    import requests
    from app.apps.analisesps import ponto

    monkeypatch.setenv("MOBPONTO_AUTHORIZATION", "Basic abc")
    monkeypatch.setenv("MOBPONTO_API_KEY", "chave")
    esperas = []
    monkeypatch.setattr(ponto.time, "sleep", esperas.append)
    dublar_requests(monkeypatch, [
        requests.exceptions.ReadTimeout("Read timed out. (read timeout=180)"),
        requests.exceptions.ReadTimeout("Read timed out. (read timeout=180)"),
        RespostaFalsa(corpo={"result": {"total_paginas": 9, "funcionarios": []}})])

    assert ponto._pedir_pagina(2026, 9, 7)["result"]["total_paginas"] == 9
    assert esperas == [15, 30]


def test_durante_a_espera_a_tarefa_DA_SINAL_DE_VIDA(monkeypatch):
    """Sem isto, 3 minutos calado fariam a tarefa ser dada por morta."""
    import threading
    from app.apps.analisesps import ponto

    batidas = []
    liberar = threading.Event()

    # Encurta o intervalo de 30 s para o teste não esperar.
    original = threading.Event.wait

    def espera_curta(self, timeout=None):
        return original(self, 0.01 if timeout == 30 else timeout)

    monkeypatch.setattr(threading.Event, "wait", espera_curta)
    with ponto._mantendo_vivo(lambda e, p: batidas.append(p), "trazendo o ponto",
                              "página 7 de 12 — esperando o Mobponto responder"):
        original(liberar, 0.2)

    assert batidas, "nenhum sinal de vida enquanto esperava"
    assert all("página 7 de 12" in b for b in batidas)


def test_o_sinal_de_vida_que_FALHA_nao_derruba_a_carga(monkeypatch):
    import threading
    from app.apps.analisesps import ponto

    original = threading.Event.wait
    monkeypatch.setattr(threading.Event, "wait",
                        lambda self, timeout=None: original(self, 0.01 if timeout == 30 else timeout))

    def estoura(e, p):
        raise RuntimeError("banco fora")
    with ponto._mantendo_vivo(estoura, "x", "y"):
        original(threading.Event(), 0.1)
    # Chegou aqui: o bloco terminou normalmente.


def test_ha_uma_PAUSA_entre_as_paginas(banco_ponto, monkeypatch):
    """O script dele espera um minuto entre páginas; emendar pedidos num servidor
    lento é o jeito mais fácil de fazê-lo parar de responder."""
    from app.apps.analisesps import ponto
    monkeypatch.setattr(ponto, "PAUSA_ENTRE_PAGINAS", 3)
    esperas = []
    monkeypatch.setattr(ponto.time, "sleep", esperas.append)
    dublar(monkeypatch, _tres_paginas())

    ponto.carregar(2026, 8)
    assert esperas == [3, 3], "uma pausa antes da 2ª e outra antes da 3ª página"


def test_o_botao_RETOMA_sozinho_uma_vez_quando_o_mobponto_para(banco_ponto, monkeypatch):
    """O botão faz o que o automático faz: se o Mobponto parar no meio, espera e
    continua de onde parou, sem ninguém apertar de novo."""
    from app.apps.analisesps import avisos_ponto, ponto, sincronizacao, tarefas
    from app.apps.analisesps.db import conexao

    monkeypatch.setattr(tarefas.time, "sleep", lambda s: None)
    monkeypatch.setattr(avisos_ponto, "avisar_que_parou", lambda m: {})
    pedidas = dublar_que_cai_uma_vez(monkeypatch, _tres_paginas(), cai_na=3)
    with conexao() as conn:
        sincronizacao._meta_gravar(conn, "ponto_competencia", "2026-8")
        execucao = tarefas._abrir_execucao(conn, "ponto", "MARCELO")

    assert tarefas.executar_trabalho("ponto", execucao) is True
    assert pedidas == [1, 2, 3, 3], "a segunda volta pediu SÓ a página 3"
    carga = ponto.carga_do_mes(2026, 8)
    assert carga["dias"] == 2 + 3 + 4 and carga["completa"]


def test_credencial_recusada_NAO_e_retomada(banco_ponto, monkeypatch):
    """Esperar 2 minutos para ouvir o mesmo "credencial recusada" só atrasa."""
    from app.apps.analisesps import avisos_ponto, ponto, sincronizacao, tarefas
    from app.apps.analisesps.db import conexao

    esperas = []
    monkeypatch.setattr(tarefas.time, "sleep", esperas.append)
    monkeypatch.setattr(avisos_ponto, "avisar_que_parou", lambda m: {})

    def recusa(*a, **k):
        raise ponto.ErroDoPonto("o Mobponto recusou a credencial (HTTP 401).")
    monkeypatch.setattr(ponto, "_pedir_pagina", recusa)
    with conexao() as conn:
        sincronizacao._meta_gravar(conn, "ponto_competencia", "2026-8")
        execucao = tarefas._abrir_execucao(conn, "ponto", "MARCELO")

    assert tarefas.executar_trabalho("ponto", execucao) is False
    assert esperas == []



# ---------------------------------------------------------------------------
# 30/09/2026 — O QUE SE APROVEITOU DO SCRIPT DA PLANILHA ("funciona perfeito")
# ---------------------------------------------------------------------------
def test_SEM_total_de_paginas_le_ATE_A_PAGINA_VAZIA(banco_ponto, monkeypatch):
    """O script trata total ausente como "infinito prático" e para na página
    vazia. O sistema tratava como "1 página" — o mês entrava com a primeira
    página só, dizendo que estava completo."""
    from app.apps.analisesps import ponto
    paginas = [resposta([pessoa_no_ponto(cpf="111.111.111-11", dias=2)], total_paginas=0),
               resposta([pessoa_no_ponto(cpf="222.222.222-22", dias=3)], total_paginas=0),
               resposta([], total_paginas=0)]
    pedidas = dublar(monkeypatch, paginas)

    feito = ponto.carregar(2026, 8)

    assert [p[2] for p in pedidas] == [1, 2, 3], "parou na vazia, não na primeira"
    carga = ponto.carga_do_mes(2026, 8)
    assert carga["dias"] == 5 and carga["pessoas"] == 2
    assert carga["paginas"] == 2 and carga["completa"], "o total é o que foi lido"
    assert feito["paginas"] == 2


def test_quando_o_mobponto_DEMORA_o_ritmo_vira_UMA_PAGINA_POR_MINUTO(banco_ponto, monkeypatch):
    """O ritmo do script dele. Começa rápido; na primeira página lenta, passa a
    esperar um minuto entre páginas até o fim da carga — e diz isso."""
    from app.apps.analisesps import ponto
    monkeypatch.setattr(ponto, "PAUSA_ENTRE_PAGINAS", 3)
    esperas = []
    monkeypatch.setattr(ponto.time, "sleep", esperas.append)
    relogio = iter([0, 45,      # página 2: 45 s — lenta
                    100, 101,   # página 3: rápida, mas o ritmo já mudou
                    ])
    monkeypatch.setattr(ponto.time, "monotonic", lambda: next(relogio))
    dublar(monkeypatch, _tres_paginas())

    feito = ponto.carregar(2026, 8)

    assert esperas == [3, 60]
    assert any("uma página por minuto" in a for a in feito["avisos"])


def test_com_o_mobponto_RAPIDO_o_ritmo_continua_rapido(banco_ponto, monkeypatch):
    from app.apps.analisesps import ponto
    monkeypatch.setattr(ponto, "PAUSA_ENTRE_PAGINAS", 3)
    esperas = []
    monkeypatch.setattr(ponto.time, "sleep", esperas.append)
    relogio = iter([0, 2, 10, 12])
    monkeypatch.setattr(ponto.time, "monotonic", lambda: next(relogio))
    dublar(monkeypatch, _tres_paginas())

    feito = ponto.carregar(2026, 8)
    assert esperas == [3, 3]
    assert not any("por minuto" in a for a in feito["avisos"])


def test_o_automatico_de_hora_em_hora_RETOMA_TRAZ_ou_PULA(banco_ponto, monkeypatch):
    """O "não mata o job" do script, com relógio de hora em hora: retoma o que
    parou, traz o que não veio hoje, pula o que já entrou inteiro hoje."""
    import datetime as dt
    from app.apps.analisesps import ponto
    from app.apps.analisesps.horario import agora
    hoje = agora().date()

    assert ponto.o_que_fazer_no_automatico(2026, 8, hoje) == "trazer"

    dublar_que_cai_uma_vez(monkeypatch, _tres_paginas(), cai_na=3)
    with pytest.raises(ponto.ErroDoPonto):
        ponto.carregar(2026, 8)
    assert ponto.o_que_fazer_no_automatico(2026, 8, hoje) == "retomar"

    ponto.carregar(2026, 8)
    assert ponto.o_que_fazer_no_automatico(2026, 8, hoje) == "pular"
    # No dia seguinte, traz de novo.
    assert ponto.o_que_fazer_no_automatico(2026, 8, hoje + dt.timedelta(days=1)) == "trazer"


def test_o_mes_que_veio_PELA_METADE_hoje_NAO_e_pulado(banco_ponto, monkeypatch):
    """Pular um mês incompleto seria deixá-lo incompleto até amanhã."""
    from app.apps.analisesps import ponto
    from app.apps.analisesps.horario import agora
    dublar(monkeypatch, [resposta([pessoa_no_ponto(dias=1)], total_paginas=3),
                         resposta([], total_paginas=3)])
    ponto.carregar(2026, 8)
    assert ponto.carga_do_mes(2026, 8)["completa"] is False
    assert ponto.o_que_fazer_no_automatico(2026, 8, agora().date()) == "trazer"


def test_o_automatico_PULA_o_mes_que_ja_entrou_hoje_sem_pedir_nada(banco_ponto, monkeypatch):
    from app.apps.analisesps import avisos_ponto, ponto, tarefas
    from app.apps.analisesps.db import conexao
    dublar(monkeypatch, [resposta([pessoa_no_ponto(dias=2)])])
    ponto.carregar(2026, 8)

    pedidas = dublar(monkeypatch, [resposta([pessoa_no_ponto(dias=9)])])
    monkeypatch.setattr(ponto, "meses_do_ponto_diario", lambda hoje=None: [(2026, 8)])
    monkeypatch.setattr(avisos_ponto, "avisar_que_parou", lambda m: {})
    with conexao() as conn:
        execucao = tarefas._abrir_execucao(conn, "ponto_diario", "agendador")

    assert tarefas.executar_trabalho("ponto_diario", execucao) is True
    assert pedidas == [], "o Mobponto não foi incomodado"
    assert ponto.carga_do_mes(2026, 8)["dias"] == 2


# ---------------------------------------------------------------------------
# 30/09/2026 — *"o ponto foi baixado, mas ninguém foi associado ao ponto."*
#
# Eu lia a data do campo `dia`. O script da planilha DESCARTA esse campo e o
# programa dele lê a data de `data`. Com `dia` sem data completa, todo dia do
# ponto ficava sem data — e ninguém casava.
# ---------------------------------------------------------------------------
def dia_de_verdade(numero, **extra):
    """Um dia com `data` completa e `dia` só com o número — o formato que o
    script da planilha sugere."""
    base = {"dia": f"{numero:02d}", "data": f"{numero:02d}/09/2026",
            "matricula": "1234", "hr_entrada": "07:00", "obra_entrada": "CRE1"}
    base.update(extra)
    return base


def test_a_data_vem_do_campo_DATA_e_nao_do_dia(banco_ponto, monkeypatch):
    import datetime as dt
    from app.apps.analisesps import ponto
    dublar(monkeypatch, [resposta([{"cpf": "997.133.493-34", "nome": "GERLANIO",
                                    "relatorio": [dia_de_verdade(1), dia_de_verdade(2)]}])])
    feito = ponto.carregar(2026, 9)

    dias = ponto.dias_por_cpf(2026, 9)["99713349334"]
    assert [d["data"] for d in dias] == [dt.date(2026, 9, 1), dt.date(2026, 9, 2)]
    assert not any("sem data" in a for a in feito["avisos"])


def test_sem_DATA_e_com_DIA_so_o_numero_usa_o_mes_da_carga(banco_ponto, monkeypatch):
    import datetime as dt
    from app.apps.analisesps import ponto
    dublar(monkeypatch, [resposta([{"cpf": "99713349334", "nome": "G",
                                    "relatorio": [{"dia": "7", "matricula": "1"}]}])])
    ponto.carregar(2026, 9)
    assert ponto.dias_por_cpf(2026, 9)["99713349334"][0]["data"] == dt.date(2026, 9, 7)


def test_o_CPF_sem_os_zeros_da_frente_ganha_os_zeros(banco_ponto, monkeypatch):
    """O programa dele faz `zfill(11)`: o Mobponto às vezes manda o CPF como
    número, e 035.134.413-63 chegaria como 3513441363."""
    from app.apps.analisesps import ponto
    dublar(monkeypatch, [resposta([{"cpf": 3513441363, "nome": "LUELIA",
                                    "relatorio": [dia_de_verdade(1)]}])])
    ponto.carregar(2026, 9)
    assert "03513441363" in ponto.dias_por_cpf(2026, 9)


def test_a_carga_JA_BAIXADA_sem_data_e_consertada_sem_baixar_de_novo(banco_ponto, monkeypatch):
    """O que ele já trouxe hoje não precisa vir de novo: o dia inteiro está
    guardado, e a data é recalculada dele na primeira leitura."""
    import datetime as dt
    import json as _json
    from app.apps.analisesps import ponto
    from app.apps.analisesps.db import conexao
    dublar(monkeypatch, [resposta([{"cpf": "99713349334", "nome": "G",
                                    "relatorio": [dia_de_verdade(3)]}])])
    ponto.carregar(2026, 9)
    # Deixa como a versão antiga deixava: data vazia, CPF sem o zero.
    with conexao() as conn:
        conn.execute("UPDATE analisesps.ponto_dia SET data = NULL")
        conn.commit()
    pedidas = dublar(monkeypatch, [])

    dias = ponto.dias_por_cpf(2026, 9)
    assert dias["99713349334"][0]["data"] == dt.date(2026, 9, 3)
    assert pedidas == [], "não pediu nada ao Mobponto"
    # E da segunda vez não há o que consertar.
    carga = ponto.carga_do_mes(2026, 9)
    assert ponto.consertar_carga(carga["id"], 2026, 9) == 0


def test_a_folha_ACHA_os_dias_do_ponto_de_verdade(banco_ponto, monkeypatch):
    """O efeito que ele viu, de ponta a ponta: com o formato de verdade, a pessoa
    da folha tem dias no período da quinzena."""
    from app.apps.analisesps import folha_apropriacao, ponto
    dublar(monkeypatch, [resposta([{"cpf": "997.133.493-34", "nome": "GERLANIO",
                                    "relatorio": [dia_de_verdade(n) for n in range(1, 16)]}])])
    ponto.carregar(2026, 9)
    ini, fim = folha_apropriacao.periodo_do_pagamento(2026, 9, "quinzena")
    dias = [d for d in ponto.dias_por_cpf(2026, 9)["99713349334"]
            if ini <= d["data"] <= fim]
    assert len(dias) == 15


# ---------------------------------------------------------------------------
# 30/09/2026 — TRAZER O PONTO DE UMA PESSOA SÓ
#
# *"É possível eu baixar só o ponto de um funcionário específico? (…) Se adivinhar
# qual página aquele funcionário está do relatório, tentar baixar só aquela
# página. Se não encontrar, vai na página seguinte ou na anterior. Pelo nome dá
# para entender em qual posição vai estar."*
# ---------------------------------------------------------------------------
def _pessoa(cpf, nome, dias=2, hora="07:00"):
    return {"cpf": cpf, "nome": nome,
            "relatorio": [dict(dia_de_verdade(n), hr_entrada=hora) for n in range(1, dias + 1)]}


def _mes_em_ordem_alfabetica():
    """Quatro páginas, duas pessoas por página, em ordem de nome."""
    return [resposta([_pessoa("11111111111", "ANA"), _pessoa("22222222222", "BRUNO")], 4),
            resposta([_pessoa("33333333333", "CARLA"), _pessoa("44444444444", "DIEGO")], 4),
            resposta([_pessoa("55555555555", "ELISA"), _pessoa("66666666666", "FABIO")], 4),
            resposta([_pessoa("77777777777", "GABI"), _pessoa("88888888888", "HUGO")], 4)]


def test_uma_pessoa_vai_DIRETO_a_pagina_guardada(banco_ponto, monkeypatch):
    from app.apps.analisesps import ponto
    paginas = _mes_em_ordem_alfabetica()
    dublar(monkeypatch, paginas)
    ponto.carregar(2026, 9)

    # No Mobponto, a hora de entrada de ELISA mudou.
    paginas[2] = resposta([_pessoa("55555555555", "ELISA", hora="06:30"),
                           _pessoa("66666666666", "FABIO")], 4)
    pedidas = dublar(monkeypatch, paginas)
    r = ponto.atualizar_pessoa(2026, 9, "555.555.555-55")

    assert r["achou"] and r["pagina"] == 3
    assert [p[2] for p in pedidas] == [3], "foi direto à página 3"
    dias = ponto.dias_por_cpf(2026, 9)["55555555555"]
    assert dias[0]["horas"][0] == "06:30"
    assert len(dias) == 2, "não duplicou"
    carga = ponto.carga_do_mes(2026, 9)
    assert carga["completa"] and carga["paginas_lidas"] == 4, "a carga do mês continua inteira"


def test_sem_pagina_guardada_ADIVINHA_pelo_nome_e_anda_na_direcao_certa(banco_ponto, monkeypatch):
    from app.apps.analisesps import ponto
    from app.apps.analisesps.db import conexao
    dublar(monkeypatch, _mes_em_ordem_alfabetica())
    ponto.carregar(2026, 9)
    with conexao() as conn:      # como uma carga feita antes da migração 039
        conn.execute("UPDATE analisesps.ponto_dia SET pagina = NULL")
        conn.commit()

    # O Mobponto de hoje empurrou todo mundo uma página para frente (gente nova
    # no começo da lista): GABI, que o chute põe na 4, está na... 4 ainda; e
    # CARLA, chutada na 2, foi para a 3.
    novas = [resposta([_pessoa("00000000191", "AARAO"), _pessoa("11111111111", "ANA")], 4),
             resposta([_pessoa("22222222222", "BRUNO"), _pessoa("20000000000", "BRUNA")], 4),
             resposta([_pessoa("33333333333", "CARLA", hora="06:00"),
                        _pessoa("44444444444", "DIEGO")], 4),
             resposta([_pessoa("55555555555", "ELISA"), _pessoa("66666666666", "FABIO")], 4)]
    pedidas = dublar(monkeypatch, novas)
    r = ponto.atualizar_pessoa(2026, 9, "33333333333")

    assert r["achou"] and r["pagina"] == 3
    assert [p[2] for p in pedidas] == [2, 3], "chutou a 2, viu nomes antes do dela, avançou"
    assert ponto.dias_por_cpf(2026, 9)["33333333333"][0]["horas"][0] == "06:00"


def test_quem_NAO_esta_no_mobponto_desiste_dizendo_onde_olhou(banco_ponto, monkeypatch):
    from app.apps.analisesps import ponto
    paginas = _mes_em_ordem_alfabetica()
    dublar(monkeypatch, paginas)
    ponto.carregar(2026, 9)
    # DIEGO saiu do Mobponto.
    paginas[1] = resposta([_pessoa("33333333333", "CARLA")], 4)
    dublar(monkeypatch, paginas)

    r = ponto.atualizar_pessoa(2026, 9, "44444444444")
    assert r["achou"] is False
    assert r["olhadas"][0] == 2 and len(r["olhadas"]) <= ponto.TENTATIVAS_POR_PESSOA
    assert len(set(r["olhadas"])) == len(r["olhadas"]), "não repete página"


def test_sem_o_ponto_do_mes_recusa_com_frase_util(banco_ponto, monkeypatch):
    from app.apps.analisesps import ponto
    with pytest.raises(ponto.ErroDoPonto) as erro:
        ponto.atualizar_pessoa(2026, 9, "55555555555")
    assert "Importe primeiro o mês completo" in str(erro.value)


def test_a_tarefa_de_UMA_PESSOA_roda_e_diz_o_que_fez(banco_ponto, monkeypatch):
    """O caminho de ANTES da fila (sem a migração 042): o pedido no `meta`."""
    from app.apps.analisesps import ponto, sincronizacao, tarefas
    from app.apps.analisesps.db import conexao
    monkeypatch.setattr(tarefas, "_fila_do_ponto_pronta", lambda: False)
    paginas = _mes_em_ordem_alfabetica()
    dublar(monkeypatch, paginas)
    ponto.carregar(2026, 9)
    dublar(monkeypatch, paginas)
    with conexao() as conn:
        sincronizacao._meta_gravar(conn, "ponto_pessoa_alvo", "2026|9|55555555555|ELISA")
        execucao = tarefas._abrir_execucao(conn, "ponto_pessoa", "MARCELO")

    assert tarefas.executar_trabalho("ponto_pessoa", execucao) is True
    ultima = tarefas.ultima_do_tipo("ponto_pessoa")
    assert ultima["ok"] is True
    assert "pela página 3" in (ultima["mensagem"] or "")


# ---------------------------------------------------------------------------
# 01/10/2026 — A PISTA DA PESSOA (migração 041). *"Já existe uma atualização em
# andamento (trazendo o ponto). Uma coisa não deveria ter nada a ver com a
# outra."*
# ---------------------------------------------------------------------------
def _abrir_viva(conn, tipo):
    conn.execute("INSERT INTO analisesps.execucoes (tipo, disparo, etapa, visto_em) "
                 "VALUES (?, 'teste', 'trabalhando', now())", (tipo,))
    conn.commit()


@pytest.fixture
def fecha_as_vivas_no_fim(banco_ponto):
    """Execução viva deixada para trás contamina o teste de tela seguinte, que lê
    a última execução do banco ainda apontado para cá."""
    yield
    from app.apps.analisesps.db import conexao
    with conexao() as conn:
        conn.execute("DELETE FROM analisesps.execucoes")
        conn.commit()


def test_o_ponto_de_UMA_PESSOA_corre_junto_com_a_carga_do_mes(banco_ponto, fecha_as_vivas_no_fim, monkeypatch):
    from app.apps.analisesps import tarefas
    from app.apps.analisesps.db import conexao

    iniciados = []
    monkeypatch.setattr(tarefas, "_iniciar_processo",
                        lambda modo, i: iniciados.append(modo))
    with conexao() as conn:
        _abrir_viva(conn, "ponto")          # a carga do mês, rodando
    for modo in ("ponto_lancar",):
        r = tarefas.disparar(modo, disparo="teste")
        assert r["ok"], r
    assert iniciados == ["ponto_lancar"]
    # A barra geral continua mostrando a carga do mês, não o lançamento.
    assert tarefas.estado()["detalhe"]["tipo"] == "ponto"
    assert tarefas.estado("pessoa")["detalhe"]["tipo"] == "ponto_lancar"


def test_cada_pista_continua_com_UMA_viva(banco_ponto, fecha_as_vivas_no_fim, monkeypatch):
    from app.apps.analisesps import tarefas
    from app.apps.analisesps.db import conexao

    monkeypatch.setattr(tarefas, "_iniciar_processo", lambda modo, i: None)
    with conexao() as conn:
        _abrir_viva(conn, "ponto")
        _abrir_viva(conn, "ponto_pessoa")
    # Duas da pessoa, ou duas gerais, ao mesmo tempo: não.
    assert not tarefas.disparar("ponto_lancar")["ok"]
    assert not tarefas.disparar("ponto")["ok"]
    # E o banco recusa mesmo quem passar pela pergunta (o índice da 041).
    import pytest as _pytest
    with conexao() as conn:
        with _pytest.raises(Exception):
            _abrir_viva(conn, "ponto_lancar")


# ---------------------------------------------------------------------------
# 01/10/2026 — A FILA DO PONTO POR PESSOA (migração 042). *"Sim, faça uma fila."*
# ---------------------------------------------------------------------------
def test_a_fila_ENFILEIRA_em_ordem_e_nao_repete_o_mesmo_pedido(banco_ponto):
    from app.apps.analisesps import ponto_fila as fila
    a = fila.enfileirar(fila.PESSOA, 2026, 9, "99713349334", "GERLANIO", quem="M")
    b = fila.enfileirar(fila.PESSOA, 2026, 9, "11122233396", "LUELIA", quem="M")
    de_novo = fila.enfileirar(fila.PESSOA, 2026, 9, "99713349334", "GERLANIO", quem="M")
    assert a["posicao"] == 0 and b["posicao"] == 1
    assert de_novo["repetido"] and de_novo["id"] == a["id"]
    assert [i["nome"] for i in fila.recentes()] == ["LUELIA", "GERLANIO"]


def test_o_trabalhador_resolve_TODOS_e_uma_falha_nao_para_a_fila(banco_ponto, monkeypatch):
    from app.apps.analisesps import ponto, ponto_edicao, ponto_fila as fila
    feitos = []

    def atualizar(ano, mes, cpf, nome="", anotar=None):
        feitos.append(cpf)
        if cpf == "11122233396":
            raise RuntimeError("Mobponto fora do ar")
        anotar("trazendo", "página 3")
        return {"achou": True, "dias": 15, "pagina": 3}
    monkeypatch.setattr(ponto, "atualizar_pessoa", atualizar)
    monkeypatch.setattr(ponto_edicao, "lancar", lambda pedido, anotar=None: {
        "plano": {"pulados": []}, "enviadas": [("14/09", "07:00")], "falhou": None})
    a = fila.enfileirar(fila.PESSOA, 2026, 9, "11122233396", "LUELIA")
    b = fila.enfileirar(fila.PESSOA, 2026, 9, "99713349334", "GERLANIO")
    c = fila.enfileirar(fila.LANCAR, 2026, 9, "99713349334", "GERLANIO",
                        {"de": "2026-09-14", "obra": "CRE1"})

    r = fila.processar()
    assert r == {"feitos": 2, "falhas": 1}
    assert feitos == ["11122233396", "99713349334"], "fora de ordem"
    assert fila.item(a["id"])["situacao"] == "falhou"
    assert "fora do ar" in fila.item(a["id"])["mensagem"]
    assert fila.item(b["id"])["situacao"] == "feito"
    assert "15 dia(s)" in fila.item(b["id"])["mensagem"]
    assert "1 batida(s) lançada(s)" in fila.item(c["id"])["mensagem"]
    assert fila.ultimo_da_pessoa("99713349334")["id"] == c["id"]


def test_CUTUCAR_devolve_o_que_ficou_rodando_e_dispara_o_trabalhador(
        banco_ponto, fecha_as_vivas_no_fim, monkeypatch):
    """A publicação mata o trabalhador no meio de um pedido; e um pedido pode
    entrar no instante em que o trabalhador está encerrando."""
    from app.apps.analisesps import ponto_fila as fila, tarefas
    from app.apps.analisesps.db import conexao
    iniciados = []
    monkeypatch.setattr(tarefas, "_iniciar_processo",
                        lambda modo, i: iniciados.append(modo))
    a = fila.enfileirar(fila.PESSOA, 2026, 9, "99713349334", "GERLANIO")
    fila._pegar_o_proximo()                   # ficou "rodando" e o processo morreu
    assert fila.item(a["id"])["situacao"] == "rodando"
    fila.cutucar()
    assert fila.item(a["id"])["situacao"] == "esperando"
    assert iniciados == ["ponto_pessoa"]
    # Com o trabalhador vivo, cutucar não mexe em nada.
    fila.cutucar()
    assert iniciados == ["ponto_pessoa"]
    with conexao() as conn:
        conn.execute("DELETE FROM analisesps.ponto_fila")
        conn.commit()


# ---------------------------------------------------------------------------
# 01/10/2026 — o lançamento põe na CÓPIA o que o Mobponto aceitou. *"A base de
# informações já existe, precisa somente aplicar."*
# ---------------------------------------------------------------------------
def test_as_batidas_lancadas_vao_para_a_COPIA_e_a_folha_as_ve(banco_ponto, monkeypatch):
    import datetime as _dt
    from app.apps.analisesps import ponto
    dublar(monkeypatch, _mes_em_ordem_alfabetica())
    ponto.carregar(2026, 9)

    # Dia sem registro na cópia: ganha um, com as batidas em ordem de hora.
    assert ponto.aplicar_batidas_na_copia(
        2026, 9, "555.555.555-55", "ELISA", "2026-09-16",
        [("17:00", "XYZ9"), ("07:00", "XYZ9"), ("12:00", "XYZ9"), ("13:00", "XYZ9")])
    dias = {d["data"]: d for d in ponto.dias_de_um_cpf(2026, 9, "55555555555")}
    novo = dias[_dt.date(2026, 9, 16)]
    assert novo["horas"] == ["07:00", "12:00", "13:00", "17:00"]
    assert novo["marcacoes"] == ["XYZ9"] * 4

    # Dia que já tinha batida: as novas entram junto, sem apagar a que havia.
    antes = dias[_dt.date(2026, 9, 1)]
    ja = [h for h in antes["horas"] if h]
    ponto.aplicar_batidas_na_copia(2026, 9, "55555555555", "ELISA", "2026-09-01",
                                   [("23:00", "XYZ9")])
    depois = {d["data"]: d for d in ponto.dias_de_um_cpf(2026, 9, "55555555555")}
    horas = [h for h in depois[_dt.date(2026, 9, 1)]["horas"] if h]
    assert all(h in horas for h in ja[:3])
    assert len(ponto.dias_de_um_cpf(2026, 9, "55555555555")) == 3, "não duplicou o dia"
    # A apropriação lê a cópia: o dia novo tem obra.
    assert ponto.dias_por_cpf(2026, 9)["55555555555"]


def test_sem_ponto_do_mes_nao_ha_onde_aplicar(banco_ponto):
    from app.apps.analisesps import ponto
    assert ponto.aplicar_batidas_na_copia(2026, 9, "55555555555", "", "2026-09-16",
                                          [("07:00", "A")]) is False
