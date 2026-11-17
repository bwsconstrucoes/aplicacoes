# -*- coding: utf-8 -*-
"""
Alterar a classificação de um título NO OMIE.

Este é o único lugar do painel que **escreve** num sistema de fora. Todo o
resto lê. Isso muda o que está em jogo: um erro aqui não mostra um número
torto numa tela — ele altera o cadastro da empresa no OMIE, e desfazer é
trabalho manual, título a título.

O QUE PODE SER ALTERADO, e só isso: a **categoria** e o **departamento**.
Valor, data e situação **nunca** — esses títulos têm baixa e conciliação, e
mexer neles quebraria a contabilidade.

O CUIDADO QUE NÃO É NEGOCIÁVEL: trocar o departamento coloca **100% no novo** e
desfaz qualquer rateio anterior. Num título rateado entre três obras, isso
apaga informação que ninguém consegue reconstruir. Por isso quem chama tem de
perguntar antes — ver `rateio_do_titulo` — e o painel recusa por padrão.

Escrito a partir do `omie_escrita.py` do painel Streamlit (documento de
passagem de 08/09/2026), com um aviso que veio de lá e continua valendo:
**a escrita nunca foi testada contra a API real**. O protocolo é simulação →
UM título conferido no OMIE → lote.
"""
from __future__ import annotations

import logging

from .omie_client import (TETO_DE_ESPERA_NA_TELA, OmieClient,
                          URL_CONTAPAGAR, URL_CONTARECEBER, URL_LANC_CC)

logger = logging.getLogger("painel.omie_escrita")

URL_DEPARTAMENTOS = "https://app.omie.com.br/api/v1/geral/departamentos/"

# Campos que o OMIE devolve na consulta e RECUSA no Alterar — ou que descrevem
# o estado da baixa, que não é nosso para mexer. Mandá-los de volta faz a
# chamada inteira falhar, e a mensagem do OMIE não diz qual foi o culpado.
SO_LEITURA = (
    "info", "status_titulo", "valor_pago", "valor_aberto", "nCodTitulo",
    "baixa_realizada", "nValorPago", "codigo_lancamento_integracao_aux",
)

# Como o OMIE chama cada operação, por tipo de título.
OPERACOES = {
    "pagar": (URL_CONTAPAGAR, "ConsultarContaPagar", "AlterarContaPagar"),
    "receber": (URL_CONTARECEBER, "ConsultarContaReceber", "AlterarContaReceber"),
}

# EXCLUIR é a única coisa aqui que NÃO TEM VOLTA.
#
# Alterar categoria ou departamento erra para o lado do reparável: basta alterar
# de novo. Excluir apaga o registro financeiro no OMIE, e recuperar é digitar
# tudo outra vez, à mão, título por título — quando se sabe o que havia.
#
# Pedido do dono em 21/09/2026, com a escolha dele registrada: apagar no OMIE
# mesmo (não só esconder do painel), e deixar o OMIE decidir sobre título com
# baixa, em vez de o painel recusar por conta própria.
EXCLUSOES = {
    "pagar": (URL_CONTAPAGAR, "ExcluirContaPagar"),
    "receber": (URL_CONTARECEBER, "ExcluirContaReceber"),
}


# O lançamento feito DIRETO na conta corrente (tarifa, IOF, aporte) não é título:
# a consulta é a mesma que a leitura da apropriação já usa (`ConsultaLancCC`,
# conferida em produção em 07/10/2026) e a gravação é a irmã dela.
# ⚠️ NÃO CONFERIDO contra o OMIE real: o nome `AlterarLancCC` segue o
# `IncluirLancCC` que o Make do dono usa há anos. Se o OMIE disser que o método
# não existe, o erro aparece na coluna Resultado e nada é gravado.
ALTERAR_LANC_CC = "AlterarLancCC"
# O que a consulta devolve e a gravação não recebe de volta. Em 11/11/2026 a
# resposta real de um lançamento (709793529) mostrou o que vem além do que o
# `IncluirLancCC` aceita: `info` (quem incluiu/alterou), `diversos` (os dados
# da CONCILIAÇÃO — origem, usuário, data) e `nCodAgrup`. Mandar bloco que não
# é da gravação faz o OMIE recusar a chamada inteira; e o estado da
# conciliação não é nosso para regravar.
SO_LEITURA_CC = ("info", "diversos", "nCodAgrup")


def tipo_do_titulo(tipo_do_fato: str) -> str:
    """"2. Contas a Pagar" -> "pagar". O fato guarda o rótulo por extenso."""
    return "pagar" if "2." in str(tipo_do_fato or "") else "receber"


class OmieEscrita(OmieClient):
    """O mesmo cliente do sync, com as duas chamadas que alteram.

    Com uma diferenca que importa: ele roda DENTRO da tela, no processo que
    atende todo mundo. Por isso o teto de espera e curto — segurar uma das 4
    vias de atendimento por um minuto trava o sistema inteiro, inclusive o ERP,
    que divide o mesmo processo. Melhor parar e dizer quanto falta esperar."""

    @classmethod
    def de_ambiente(cls, caminho_env=".env", **kwargs):
        kwargs.setdefault("teto_de_espera", TETO_DE_ESPERA_NA_TELA)
        return super().de_ambiente(caminho_env, **kwargs)

    def consultar_titulo(self, codigo: int, tipo: str) -> dict:
        url, consultar, _alterar = OPERACOES[tipo]
        resposta = self._call(url, consultar, {"codigo_lancamento_omie": int(codigo)})
        return resposta if isinstance(resposta, dict) else {}

    def alterar_titulo(self, cadastro: dict, tipo: str) -> dict:
        url, _consultar, alterar = OPERACOES[tipo]
        return self._call(url, alterar, cadastro, uma_vez=True)

    def excluir_titulo(self, codigo: int, tipo: str) -> dict:
        """Apaga o título NO OMIE. Não há desfazer.

        Manda só o código — é o que o OMIE pede, e é o que a consulta já usa.
        Mandar o cadastro inteiro aqui seria convidar a recusa por campo
        sobrando, e a mensagem dele não diz qual foi o culpado."""
        url, excluir = EXCLUSOES[tipo]
        return self._call(url, excluir, {"codigo_lancamento_omie": int(codigo)},
                          uma_vez=True)

    def alterar_lancamento_cc(self, lancamento: dict) -> dict:
        """Grava o lançamento de conta corrente alterado (tarifa, aporte, IOF —
        o que foi lançado direto na conta, sem título).

        10/11/2026: o dono tentou trocar a obra de uma tarifa do Bradesco pelo
        Explorador e o OMIE respondeu "Lançamento não cadastrado para o
        Código [11266605823]". O painel perguntava por ela como TÍTULO; ela é
        um lançamento da conta, e mora noutro endereço do OMIE."""
        return self._call(URL_LANC_CC, ALTERAR_LANC_CC, lancamento, uma_vez=True)

    def listar_departamentos(self, **kwargs):
        return self._listar(URL_DEPARTAMENTOS, "ListarDepartamentos",
                            "departamentos", **kwargs)


def preparar_alteracao(cadastro: dict, codigo_categoria=None,
                       cod_departamento=None) -> tuple[dict, list[str]]:
    """Monta o cadastro a enviar, partindo do que o OMIE devolveu.

    Parte do cadastro CONSULTADO e não de um dicionário montado à mão: o
    Alterar do OMIE substitui o título inteiro, então tudo que não for enviado
    de volta se perde. Enviar o que veio, com uma coisa trocada, é o que faz a
    alteração ser cirúrgica.

    Devolve `(novo_cadastro, mudancas)` — `mudancas` em português, para a tela
    mostrar o que vai acontecer ANTES de acontecer."""
    novo = {c: v for c, v in (cadastro or {}).items() if c not in SO_LEITURA}
    mudancas = []

    if codigo_categoria and str(cadastro.get("codigo_categoria") or "") != str(codigo_categoria):
        antes = str(cadastro.get("codigo_categoria") or "—")
        novo["codigo_categoria"] = str(codigo_categoria)
        # o array `categorias` é a categoria MÚLTIPLA; deixá-lo junto faz o
        # OMIE não saber qual das duas vale
        novo.pop("categorias", None)
        mudancas.append(f"Categoria: {antes} → {codigo_categoria}")

    if cod_departamento:
        distribuicao = cadastro.get("distribuicao") or []
        antes = ", ".join(str(d.get("cCodDep") or "?") for d in distribuicao) or "—"
        # JA ESTA ASSIM? Entao nao se manda nada.
        #
        # 21/09/2026: o dono viu "Departamento: 1651586723 → 1651586723 (100%)"
        # — uma alteracao que nao altera. Ela nao era inofensiva: gastava uma
        # chamada na API da Omie e, ao errar, mais 8 tentativas. Foi assim que
        # ele levou um bloqueio de 11 minutos por "consumo indevido".
        ja_esta = (len(distribuicao) == 1
                   and str(distribuicao[0].get("cCodDep") or "") == str(cod_departamento)
                   and float(distribuicao[0].get("nPerDep") or 0) == 100.0)
        if not ja_esta:
            novo["distribuicao"] = [{"cCodDep": str(cod_departamento), "nPerDep": 100}]
            mudancas.append(f"Departamento: {antes} → {cod_departamento} (100%)")

    return novo, mudancas


def _registro_do_lancamento(resposta, profundidade=0):
    """O lançamento dentro da resposta da consulta: o bloco que tem `cabecalho`.
    O formato exato da resposta não foi conferido — procurar pelo nome da chave
    serve às duas formas prováveis (o registro solto, ou embrulhado)."""
    if profundidade > 3:
        return None
    if isinstance(resposta, dict):
        if isinstance(resposta.get("cabecalho"), dict):
            return resposta
        for valor in resposta.values():
            achado = _registro_do_lancamento(valor, profundidade + 1)
            if achado is not None:
                return achado
    elif isinstance(resposta, list):
        for valor in resposta:
            achado = _registro_do_lancamento(valor, profundidade + 1)
            if achado is not None:
                return achado
    return None


def preparar_alteracao_cc(resposta: dict, codigo: int, codigo_categoria=None,
                          cod_departamento=None) -> tuple[dict, list[str]]:
    """O mesmo que `preparar_alteracao`, para o lançamento de conta corrente.

    Parte do lançamento CONSULTADO, com uma coisa trocada — pelo mesmo motivo
    do título: o que não for enviado de volta pode se perder.

    A obra vai em VALOR (`nValDep`), o lançamento inteiro nela — é o formato que
    o Make do dono usa para incluir lançamento de conta com departamento
    (`analisesps/conciliacao_omie.py`). Se o lançamento já vinha em percentual,
    o percentual vai junto (100)."""
    import copy
    registro = _registro_do_lancamento(resposta)
    if registro is None:
        raise ValueError("o OMIE não devolveu o lançamento (resposta sem "
                         "'cabecalho') — nada foi enviado")
    novo = {c: copy.deepcopy(v) for c, v in registro.items() if c not in SO_LEITURA_CC}
    novo["nCodLanc"] = int(registro.get("nCodLanc") or codigo)
    # bloco vazio não vai: transferência sem conta de destino, e a lista de
    # categorias múltiplas vazia
    if not ((novo.get("transferencia") or {}).get("nCodCCDestino")):
        novo.pop("transferencia", None)
    if isinstance(novo.get("detalhes"), dict) and not novo["detalhes"].get("aCodCateg"):
        novo["detalhes"].pop("aCodCateg", None)
    mudancas = []

    detalhes = novo.get("detalhes") if isinstance(novo.get("detalhes"), dict) else {}
    if codigo_categoria and str(detalhes.get("cCodCateg") or "") != str(codigo_categoria):
        antes = str(detalhes.get("cCodCateg") or "—")
        detalhes["cCodCateg"] = str(codigo_categoria)
        # a categoria MÚLTIPLA (`aCodCateg`, como veio do OMIE em 10/11/2026);
        # deixá-la junto faz o OMIE não saber qual das duas vale
        detalhes.pop("aCodCateg", None)
        detalhes.pop("categorias", None)
        novo["detalhes"] = detalhes
        mudancas.append(f"Categoria: {antes} → {codigo_categoria}")

    if cod_departamento:
        chave = next((k for k in ("departamentos", "distribuicao")
                      if isinstance(registro.get(k), list)), "departamentos")
        atuais = [d for d in (registro.get(chave) or []) if isinstance(d, dict)]
        codigo_de = lambda d: str(d.get("cCodDep") or d.get("cCodDepartamento") or "")
        antes = ", ".join(codigo_de(d) or "?" for d in atuais) or "—"
        ja_esta = len(atuais) == 1 and codigo_de(atuais[0]) == str(cod_departamento)
        if not ja_esta:
            valor = abs(float((novo.get("cabecalho") or {}).get("nValorLanc") or 0))
            parte = {"cCodDep": str(cod_departamento), "nValDep": round(valor, 2)}
            if any(k.startswith("nPer") for d in atuais for k in d):
                parte["nPerDep"] = 100
            novo[chave] = [parte]
            mudancas.append(f"Departamento: {antes} → {cod_departamento} (100%)")

    return novo, mudancas
