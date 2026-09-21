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
                          URL_CONTAPAGAR, URL_CONTARECEBER)

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
        return self._call(url, alterar, cadastro)

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
