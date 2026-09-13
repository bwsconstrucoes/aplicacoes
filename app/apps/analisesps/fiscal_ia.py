# -*- coding: utf-8 -*-
"""
A IA lendo o anexo — só quando o cruzamento de texto não resolveu.

O QUE ESTE ARQUIVO **NÃO** FAZ, e é a decisão que poupou um módulo inteiro:
ler documento. O ERP já tem um leitor pronto e rodando em produção
(`erp/core/documentos/leitor.py`), e ele já faz exatamente o que falta aqui —
XML de NFe por parser exato (sem IA nenhuma), PDF com texto, e foto ou PDF
escaneado por leitura visual, devolvendo a chave de acesso, o tipo do
documento, o emitente, o número, o valor e um nível de confiança.

Escrever um segundo leitor seria ter duas verdades sobre o mesmo PDF.

A ORDEM IMPORTA, e foi o dono quem a definiu em 11/09/2026:

  1. PRIMEIRO o cruzamento de texto (credor, CNPJ, valor, número), que é de
     graça, instantâneo e resolve a maioria.
  2. SÓ O QUE SOBRAR vai para a leitura do anexo. Mandar todo anexo para a IA
     seria pagar caro para responder o que já se sabia.
  3. E NUNCA automático: *"aí você pode até fazer a sugestão, analisar com IA,
     e a gente seleciona ou não seleciona, que me permita selecionar alguns
     que eu queira testar"*. Quem escolhe é ele, SP a SP.

A IA PROPÕE, NUNCA DECIDE. Uma nota lida errado de um PDF torto é dedução
indevida com cara de decisão tomada — o erro que este módulo inteiro existe
para não cometer. Por isso a leitura entra no diário como PROPOSTA, e não como
CONFIRMADA: continua precisando de uma pessoa dizer sim.

O CUSTO é dele, e ele decidiu: *"eu prefiro gastar um pouco com a IA do que ter
um funcionário fazendo isso"*. O volume que ele estimou foi de mil por mês no
total, e só uma parte chega até aqui.
"""
from __future__ import annotations

import logging

logger = logging.getLogger("analisesps.fiscal_ia")

# Teto do anexo baixado. A instância tem 2 GB divididos com 17 módulos e já
# morreu de falta de memória em julho de 2026.
MAXIMO_ANEXO = 20 * 1024 * 1024
SEGUNDOS_DE_ESPERA = 60

# O QUE O LEITOR DO ERP RESPONDE -> A CATEGORIA DO CAMPO DO PIPEFY.
#
# O leitor fala em tipo de DOCUMENTO ("NFE", "GUIA"); o campo do card fala em
# categoria fiscal ("NF-e (Mercadoria)", "Guia de Tributo"). A tradução é esta,
# e ela é um lugar só de propósito: espalhada, uma das pontas divergiria.
#
# O que NÃO tem tradução fica de fora e volta como dúvida — um orçamento ou um
# comprovante bancário não são documento fiscal de despesa, e chutar uma
# categoria para eles seria pior do que dizer "não sei".
CATEGORIA_DO_TIPO = {
    "NFE": "NF-e (Mercadoria)",
    "NFSE": "NFS-e (Serviço)",
    "CTE": "CT-e (Frete)",
    "NFCE": "NFC-e (Cupom Fiscal eletrônico)",
    "GUIA": "Guia de Tributo",
    "FATURA_CONCESSIONARIA": "Nota de Débito/Fatura",
    "CONTRATO": "Contrato",
    "TERMO_RESCISAO": "Rescisões (TRCT e Multa)",
}

# Leitura com confiança baixa não vira proposta. O leitor do ERP devolve
# ALTA/MEDIA/BAIXA, e "BAIXA" quer dizer que ele mesmo desconfia.
CONFIANCA_EM_NUMERO = {"ALTA": 85, "MEDIA": 60, "BAIXA": 30}


class SemAnexo(RuntimeError):
    """A SP não tem anexo para ler — não é falha, é ausência."""


def _baixar(url: str) -> tuple[bytes, str]:
    """Traz o anexo, com teto de tamanho e de tempo.

    STREAMING COM TETO, e não `resposta.content`: um arquivo gigante entraria
    inteiro na memória antes de qualquer verificação — que é exatamente como
    esta instância morreu em julho de 2026."""
    import os

    import requests

    endereco = str(url or "").strip()
    if not endereco.lower().startswith(("http://", "https://")):
        raise SemAnexo("A SP não tem link de anexo.")

    resposta = requests.get(endereco, stream=True, timeout=SEGUNDOS_DE_ESPERA)
    resposta.raise_for_status()
    pedacos, total = [], 0
    for pedaco in resposta.iter_content(64 * 1024):
        total += len(pedaco)
        if total > MAXIMO_ANEXO:
            raise RuntimeError(
                f"O anexo passa de {MAXIMO_ANEXO // (1024 * 1024)} MB.")
        pedacos.append(pedaco)

    nome = os.path.basename(endereco.split("?")[0]) or "anexo.pdf"
    return b"".join(pedacos), nome


def ler_anexo(sp: dict) -> dict:
    """Lê o anexo daquela SP e devolve o que a IA entendeu.

    Usa o leitor do ERP — que NÃO é alterado aqui. Ele é outra área; o que se
    faz é chamar a função pública dele, com o payload que ela já documenta."""
    from app.apps.erp.core.documentos.leitor import ErroLeitura, ler_documento

    conteudo, nome = _baixar(sp.get("anexo_link"))
    # A DICA MUDA O RESULTADO, e é de graça: dizer de quem é a despesa e quanto
    # ela custa deixa o leitor conferir o que extraiu contra o que se esperava.
    dica = (f"Despesa de {sp.get('credor') or 'fornecedor não informado'}, "
            f"CPF/CNPJ {sp.get('documento') or 'não informado'}, "
            f"valor {sp.get('valor') or 'não informado'}. "
            "Preciso da chave de acesso e do tipo do documento fiscal.")
    try:
        return ler_documento(conteudo, nome, dica)
    except ErroLeitura as e:
        raise RuntimeError(str(e)) from e


def proposta_da_leitura(sp: dict, lido: dict) -> dict:
    """O que a IA leu virando proposta de categoria e chave.

    Devolve {documentacao, chave, confianca, motivo, propoe}. `propoe` falso
    quer dizer "mostra, mas não marca" — a segunda pilha."""
    from . import fiscal

    tipo = str(lido.get("tipo_documento") or "").strip().upper()
    chave = fiscal.so_digitos(lido.get("chave_acesso"))
    confianca = CONFIANCA_EM_NUMERO.get(
        str(lido.get("confianca") or "").strip().upper(), 40)

    # A CHAVE MANDA SOBRE O QUE A IA ACHOU QUE ERA. Se ela leu 44 dígitos, a
    # categoria sai dos dígitos 21-22 da própria chave, que é definição da
    # Receita — e aí é certeza, não interpretação.
    categoria = fiscal.categoria_da_chave(chave) if len(chave) == 44 else ""
    if categoria:
        motivo = (f"a IA leu a chave de acesso no anexo; a categoria vem de "
                  f"dentro dela")
    else:
        categoria = CATEGORIA_DO_TIPO.get(tipo, "")
        motivo = (f"a IA identificou o anexo como {tipo.lower().replace('_', ' ')}"
                  if categoria else
                  f"a IA leu o anexo, mas ele não é documento fiscal de "
                  f"despesa ({tipo.lower().replace('_', ' ') or 'não identificado'})")
        confianca = min(confianca, 60)

    # O QUE A IA LEU TEM DE BATER COM A SP. Uma chave lida de um anexo que foi
    # parar no card errado é o erro mais caro possível — e ele acontece: o dono
    # descreveu "colocar uma nota de um registro para outro".
    emitente = fiscal.emitente_da_chave(chave) or fiscal.so_digitos(
        lido.get("emitente_documento"))
    if emitente and not fiscal.mesmo_documento(sp.get("documento"), emitente):
        return {"documentacao": categoria, "chave": chave, "confianca": 0,
                "propoe": False,
                "motivo": ("a IA leu o anexo, mas quem emitiu o documento não é "
                           "o credor desta SP. Confira se o anexo não é de "
                           "outro lançamento.")}

    if not categoria:
        return {"documentacao": "", "chave": "", "confianca": confianca,
                "propoe": False, "motivo": motivo + "."}

    return {"documentacao": categoria, "chave": chave, "confianca": confianca,
            "propoe": confianca >= fiscal.CONFIANCA_PARA_PROPOR,
            "motivo": motivo + "."}


def analisar(sp_ids: list, anotar=None) -> dict:
    """Lê o anexo das SPs escolhidas e grava a PROPOSTA no diário.

    GRAVA COMO PROPOSTA, nunca como confirmada: a IA propõe, quem confirma é
    gente. E grava uma a uma — se a décima falhar, as nove primeiras já estão
    lá, e quem pediu vê o resultado do que deu certo."""
    from . import consultas, fiscal
    from .db import conexao

    anotar = anotar or (lambda *a, **k: None)
    lidas, falhas, sem_anexo = 0, {}, 0

    for n, sp_id in enumerate(sp_ids, 1):
        anotar("lendo os anexos com IA", f"{n} de {len(sp_ids)}")
        sp = consultas.uma(str(sp_id))
        if not sp:
            falhas[str(sp_id)] = "SP não encontrada na base"
            continue
        try:
            lido = ler_anexo(sp)
        except SemAnexo:
            sem_anexo += 1
            continue
        except Exception as e:  # noqa: BLE001 — uma falha não derruba a fila
            logger.exception("Análise de SPs: falhou ler o anexo da SP %s", sp_id)
            falhas[str(sp_id)] = str(e)[:300]
            continue

        proposta = proposta_da_leitura(sp, lido)
        with conexao() as conn:
            conn.execute(
                "INSERT INTO analisesps.sp_fiscal_analise "
                "  (sp_id, situacao, documentacao, chave, dedutivel, "
                "   confianca, origem, motivo, decidida_em) "
                "VALUES (?, ?, ?, ?, ?, ?, 'IA', ?, now()) "
                "ON CONFLICT (sp_id) DO UPDATE SET "
                "  situacao = EXCLUDED.situacao, "
                "  documentacao = EXCLUDED.documentacao, "
                "  chave = EXCLUDED.chave, dedutivel = EXCLUDED.dedutivel, "
                "  confianca = EXCLUDED.confianca, origem = 'IA', "
                "  motivo = EXCLUDED.motivo, decidida_em = now()",
                (str(sp_id), fiscal.PROPOSTA, proposta["documentacao"],
                 proposta["chave"],
                 fiscal.dedutivel(proposta["documentacao"])
                 if proposta["documentacao"] else None,
                 int(proposta["confianca"]), proposta["motivo"]))
            conn.commit()
        lidas += 1

    logger.info("Análise de SPs: IA leu %d anexo(s), %d sem anexo, %d falha(s).",
                lidas, sem_anexo, len(falhas))
    return {"lidas": lidas, "sem_anexo": sem_anexo, "falhas": falhas}
