# ============================================================================
# ERP — core/importadores/pipefy_cards.py
# Importa CARDS do pipe "SOLICITAÇÕES FINANCEIRO" (301426645) e cria títulos
# no ERP. É a ponte de migração: o financeiro continua no Pipefy enquanto as
# obras escolhidas passam a existir aqui, com os mesmos dados.
#
# Uso: informar IDs de cards (colados da tela do Pipefy) ou varrer o pipe por
# fase. Reusa PIPEFY_API_TOKEN, que já existe no serviço.
#
# Idempotente: card já importado é reconhecido por titulos.ref_pipefy e
# devolvido como "já existente", nunca duplicado.
#
# O que NÃO é importado por decisão de projeto (ver DE_PARA_PIPEFY.md):
#   - dados bancários digitados no card (chave Pix, banco/agência/conta):
#     conta de pagamento vive no cadastro do fornecedor, homologada;
#     o importador apenas REPORTA a chave encontrada para conferência.
# ============================================================================
from __future__ import annotations

import json
import os
import re
import logging
import time
import urllib.request
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.apps.erp.core.cadastros import fornecedores as svc_forn
from app.apps.erp.core.cadastros.validadores import cnpj_valido, cpf_valido, somente_digitos
from app.apps.erp.core.comum.auditoria import ErroValidacao
from app.apps.erp.core.titulos import service as svc_tit
from app.apps.erp.db.models.cadastros import (
    Categoria, Fornecedor, Obra, StatusConta, TipoTitulo, Usuario,
)
from app.apps.erp.db.models.financeiro import Titulo

logger = logging.getLogger(__name__)

_URL = "https://api.pipefy.com/graphql"
PIPE_FINANCEIRO = "301426645"
_MAX_PARCELAS = 10


class ErroPipefy(Exception):
    """Falha de comunicação, token ausente ou card inexistente."""


# ---------------------------------------------------------------------------
# GraphQL
# ---------------------------------------------------------------------------
def _token() -> str:
    tk = os.environ.get("PIPEFY_API_TOKEN") or os.environ.get("PIPEFY_TOKEN") or ""
    if not tk.strip():
        raise ErroPipefy("PIPEFY_API_TOKEN não configurado no serviço.")
    return tk.strip()


def _gql(query: str, variaveis: dict[str, Any] | None = None) -> dict[str, Any]:
    corpo = json.dumps({"query": query, "variables": variaveis or {}}).encode("utf-8")
    ultimo: Optional[Exception] = None
    for tentativa in range(1, 4):
        try:
            req = urllib.request.Request(_URL, data=corpo, headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {_token()}"})
            with urllib.request.urlopen(req, timeout=60) as resp:
                if resp.status != 200:
                    raise ErroPipefy(f"Pipefy respondeu HTTP {resp.status}.")
                dados = json.loads(resp.read().decode("utf-8"))
            if dados.get("errors"):
                raise ErroPipefy("; ".join(e.get("message", "?") for e in dados["errors"]))
            return dados["data"]
        except ErroPipefy:
            raise
        except Exception as e:
            ultimo = e
            if tentativa < 3:
                time.sleep(2 ** tentativa)
    raise ErroPipefy(f"Falha de rede com o Pipefy: {ultimo}")


Q_CARD = """
query ($id: ID!) {
  card(id: $id) {
    id title createdAt
    current_phase { name }
    fields { field { id label type } name value report_value }
  }
}
"""

Q_FASE = """
query ($id: ID!, $after: String) {
  phase(id: $id) {
    name
    cards(first: 30, after: $after) {
      pageInfo { hasNextPage endCursor }
      edges { node { id title createdAt current_phase { name }
        fields { field { id label type } name value report_value } } }
    }
  }
}
"""


# ---------------------------------------------------------------------------
# Conversores
# ---------------------------------------------------------------------------
# Campos de anexo do pipe financeiro → categoria do anexo no ERP
CAMPOS_ANEXO = {
    "anexos": "OUTRO",
    "anexar_arquivos": "OUTRO",
    "danfe": "NOTA",
    "arquivo_xml": "NOTA",
    "anexo_presta_o_de_conta": "PRESTACAO_CONTAS",
    "comprovante": "COMPROVANTE",
    "comprovante_1_pagamento": "COMPROVANTE",
    "comprovante_2_pagamento": "COMPROVANTE",
    "comprovante_3_pagamento": "COMPROVANTE",
    "comprovante_4_pagamento": "COMPROVANTE",
    "comprovante_5_pagamento": "COMPROVANTE",
    "comprovante_6_pagamento": "COMPROVANTE",
}
MAX_ANEXO_BYTES = 20 * 1024 * 1024


def extrair_urls_anexo(valor: Any) -> list[str]:
    """O campo de anexo do Pipefy traz uma lista de URLs (JSON ou texto)."""
    if valor in (None, "", "[]"):
        return []
    if isinstance(valor, list):
        itens = valor
    else:
        texto = str(valor).strip()
        try:
            itens = json.loads(texto)
            if not isinstance(itens, list):
                itens = [itens]
        except json.JSONDecodeError:
            itens = [t for t in re.split(r"[\s,]+", texto) if t]
    urls = []
    for i in itens:
        u = i.get("url") if isinstance(i, dict) else str(i)
        if u and u.startswith("http"):
            urls.append(u)
    return urls


def baixar_anexos_do_card(s, card_campos: dict[str, Any], titulo_id: int,
                          usuario) -> list[dict[str, Any]]:
    """Traz os arquivos do card para o banco do ERP.

    Sem isso, a SP importada chega sem a nota e sem o comprovante — que é
    justamente o que se precisa consultar depois. Falha de download não
    interrompe a importação: fica relatada.
    """
    from urllib.parse import unquote, urlparse

    from app.apps.erp.core.documentos.armazenamento import salvar

    trazidos = []
    for campo, categoria in CAMPOS_ANEXO.items():
        for url in extrair_urls_anexo(card_campos.get(campo)):
            nome = unquote(os.path.basename(urlparse(url).path)) or f"{campo}.bin"
            try:
                resp = requests.get(url, timeout=60, stream=True)
                resp.raise_for_status()
                conteudo = b""
                for pedaco in resp.iter_content(64 * 1024):
                    conteudo += pedaco
                    if len(conteudo) > MAX_ANEXO_BYTES:
                        raise ValueError(f"arquivo acima de "
                                         f"{MAX_ANEXO_BYTES // (1024*1024)} MB")
                anexo = salvar(s, conteudo, nome, entidade_tipo="titulo",
                               entidade_id=titulo_id, categoria=categoria,
                               descricao=f"Importado do Pipefy ({campo})", usuario=usuario)
                trazidos.append({"campo": campo, "arquivo": anexo.nome_arquivo,
                                 "kb": round((anexo.tamanho_bytes or 0) / 1024, 1)})
            except Exception as e:
                logger.warning("Pipefy: anexo %s não veio (%s)", nome, e)
                trazidos.append({"campo": campo, "arquivo": nome, "erro": str(e)[:120]})
    return trazidos


def normalizar_codigo_barras(valor: Any) -> Optional[str]:
    """Extrai a linha digitável do campo do Pipefy.

    O campo não vem limpo: costuma trazer o VALOR colado no fim, separado por
    hífen ("0019...0000100000-1000,00"), e, quando o boleto não existe ou não
    passou na validação, vem a palavra INVALIDO ou só zeros. Nesses casos o
    título tem que ser importado SEM código de barras, e não com lixo dentro.
    """
    texto = _texto(valor).strip()
    if not texto:
        return None
    if "INVALID" in texto.upper() or "ERRO" in texto.upper():
        return None
    # o valor vem depois do último hífen: 0019...-1000,00
    if "-" in texto:
        texto = texto.rsplit("-", 1)[0]
    digitos = re.sub(r"\D", "", texto)
    if not digitos or set(digitos) == {"0"}:
        return None
    if len(digitos) not in (47, 48):
        logger.info("Pipefy: código de barras com %d dígitos ignorado (esperado 47 ou 48)",
                    len(digitos))
        return None
    return digitos


def _mapa_campos(card: dict[str, Any]) -> dict[str, str]:
    """{id_do_campo: valor legível}.

    Campos CONNECTOR do Pipefy devolvem em `value` o ID do card conectado
    (ex.: ["1329563540"]), não o nome. O nome vem em `report_value`. Sem esta
    distinção, "Tipo de Despesa" e "Centro de Custo" chegavam como número e
    nunca casavam com a categoria nem com a obra.
    """
    mapa: dict[str, str] = {}
    for f in card.get("fields") or []:
        campo = f.get("field") or {}
        fid = campo.get("id") or ""
        if not fid:
            continue
        tipo = (campo.get("type") or "").lower()
        val = f.get("value")
        rep = f.get("report_value")
        if tipo in ("connector", "connector_field", "database_connection"):
            # prefere o rótulo; só usa o ID se não houver rótulo
            escolhido = rep if rep not in (None, "", "[]") else val
        else:
            escolhido = val if val not in (None, "", "[]") else rep
        if escolhido not in (None, "", "[]"):
            mapa[fid] = escolhido
    return mapa


def _texto(valor: Optional[str]) -> str:
    """Campos connector/checklist vêm como JSON '["Sede"]'."""
    if not valor:
        return ""
    v = str(valor).strip()
    if v.startswith("["):
        try:
            lista = json.loads(v)
            return str(lista[0]).strip() if lista else ""
        except (json.JSONDecodeError, IndexError):
            return v.strip("[]\" ")
    return v


def _lista(valor: Optional[str]) -> list[str]:
    if not valor:
        return []
    v = str(valor).strip()
    if v.startswith("["):
        try:
            return [str(x).strip() for x in json.loads(v) if str(x).strip()]
        except json.JSONDecodeError:
            return []
    return [v]


def _valor(txt: Optional[str]) -> Optional[Decimal]:
    if txt in (None, ""):
        return None
    v = str(txt).strip().replace("R$", "").strip()
    if "," in v:                       # 1.234,56 → 1234.56
        v = v.replace(".", "").replace(",", ".")
    try:
        return Decimal(v).quantize(Decimal("0.01"))
    except InvalidOperation:
        return None


def _data(txt: Optional[str]):
    if not txt:
        return None
    v = str(txt).strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y-%m-%dT%H:%M:%S%z", "%d/%m/%Y %H:%M"):
        try:
            return datetime.strptime(v[:len(fmt) + 6] if "%z" in fmt else v[:10], fmt).date()
        except ValueError:
            continue
    return None


# ---------------------------------------------------------------------------
# Tradução card → payload de título
# ---------------------------------------------------------------------------
def extrair_dados(card: dict[str, Any]) -> dict[str, Any]:
    """Traduz um card do pipe financeiro para o formato de criar_titulo,
    mais os campos auxiliares que o importador usa para casar cadastros."""
    c = _mapa_campos(card)
    procedimento = _texto(c.get("selecione_o_procedimento"))

    # credor: nome + documento
    nome_credor = _texto(c.get("local")) or _texto(c.get("fornecedor"))
    cnpj = somente_digitos(_texto(c.get("cnpj")))
    cpf = somente_digitos(_texto(c.get("cpf")))
    documento = cnpj if cnpj_valido(cnpj) else (cpf if cpf_valido(cpf) else "")
    tipo_pessoa = "PJ" if documento and len(documento) == 14 else "PF"

    # parcelas: 1..10 ou parcela única
    parcelas: list[dict[str, Any]] = []
    for i in range(1, _MAX_PARCELAS + 1):
        venc = _data(c.get(f"data_de_vencimento_{i}"))
        val = _valor(c.get("valor_parcela_1" if i == 1 else
                           ("valor_da_parcela_2" if i == 2 else f"valor_parcela_{i}")))
        if venc and val:
            parcelas.append({"vencimento": venc, "valor": str(val),
                             "linha_digitavel": normalizar_codigo_barras(
                                 c.get(f"c_digo_de_barras_{i}"))})
    if not parcelas:
        venc = _data(c.get("data_de_pagamento"))
        val = _valor(c.get("valor"))
        if venc and val:
            parcelas.append({"vencimento": venc, "valor": str(val),
                             "linha_digitavel": normalizar_codigo_barras(
                                 c.get("c_digo_de_barras_11"))})

    # rateio: centros de custo 1..5
    rateios: list[dict[str, Any]] = []
    pares = [("centro_de_custo_1", "valor_centro_de_custo_1"),
             ("centro_de_custo_2", "valor_centro_de_custo_2"),
             ("centro_de_custo_3", "valor_centro_de_custo_3"),
             ("centro_de_custo_4_1", "valor_centro_de_custo_4"),
             ("centro_de_custo_5_1", "valor_centro_de_custo_5")]
    for campo_cc, campo_val in pares:
        cc = _texto(c.get(campo_cc))
        if not cc:
            continue
        rateios.append({"centro_custo": cc, "valor": _valor(c.get(campo_val))})

    total = _valor(c.get("valor"))
    forma_pipefy = _texto(c.get("tipo_de_pagamento")).upper()
    if "BOLETO" in forma_pipefy:
        forma = "BOLETO"
    elif "PIX" in forma_pipefy:
        forma = "PIX"
    elif "TRANSFER" in forma_pipefy or "TED" in forma_pipefy:
        forma = "TED"
    else:
        forma = "PIX"

    descricao = (_texto(c.get("descri_o")) or _texto(c.get("descri_o_da_solicita_o"))
                 or card.get("title") or "").strip()

    chave_pix = next((_texto(c.get(k)) for k in
                      ("cnpj_1", "chave_pix_cpf", "chave_pix_de_email",
                       "chave_pix_telefone", "chave_pix_aleat_ria") if c.get(k)), "")

    tipo_despesa = _texto(c.get("tipo_de_despesa"))
    cc_parece_id = tipo_despesa.isdigit() if tipo_despesa else False

    return {
        "card_id": str(card.get("id")),
        "campos_crus": c,
        "tipo_despesa_e_id": cc_parece_id,
        "titulo_card": card.get("title") or "",
        "fase": ((card.get("current_phase") or {}).get("name")) or "",
        "procedimento": procedimento,
        "numero_sp_origem": _texto(c.get("n_da_solicita_o")),
        "credor_nome": nome_credor,
        "credor_documento": documento,
        "credor_tipo": tipo_pessoa,
        "descricao": descricao,
        "valor_total": str(total) if total else None,
        "forma_pagamento": forma,
        "tipo_despesa": tipo_despesa,
        "data_solicitacao": _data(c.get("data")),
        "nota_fiscal": _texto(c.get("n_da_nota_fiscal")),
        "tem_contrato": bool(_texto(c.get("contrato_de_loca_o"))),
        "tem_pedido": bool(_texto(c.get("pedido_de_compra"))),
        "parcelas": parcelas,
        "rateios": rateios,
        "chave_pix_no_card": chave_pix,
        "anexos_link": _texto(c.get("anexos_link")),
    }


# ---------------------------------------------------------------------------
# Busca
# ---------------------------------------------------------------------------
def buscar_cards(ids: list[str]) -> list[dict[str, Any]]:
    cards = []
    for cid in ids:
        cid = somente_digitos(str(cid))
        if not cid:
            continue
        d = _gql(Q_CARD, {"id": cid})
        card = d.get("card")
        if card:
            cards.append(card)
        time.sleep(0.25)
    return cards


def buscar_por_fase(phase_id: str, maximo: int = 200) -> list[dict[str, Any]]:
    cards, cursor = [], None
    while len(cards) < maximo:
        d = _gql(Q_FASE, {"id": str(phase_id), "after": cursor})
        fase = d.get("phase") or {}
        bloco = fase.get("cards") or {}
        cards.extend(e["node"] for e in bloco.get("edges", []))
        if not (bloco.get("pageInfo") or {}).get("hasNextPage"):
            break
        cursor = bloco["pageInfo"]["endCursor"]
        time.sleep(0.3)
    return cards[:maximo]


def extrair_ids(texto: str) -> list[str]:
    """Aceita IDs colados de qualquer jeito: um por linha, separados por
    vírgula/espaço, ou URLs (https://app.pipefy.com/open-cards/123456789)."""
    return list(dict.fromkeys(re.findall(r"\b\d{6,}\b", texto or "")))


# ---------------------------------------------------------------------------
# Importação
# ---------------------------------------------------------------------------
def _achar_obra(s: Session, nome_cc: str) -> Optional[Obra]:
    """Casa o centro de custo do Pipefy com a obra do ERP (código ou nome)."""
    if not nome_cc:
        return None
    alvo = nome_cc.strip()
    obra = s.scalars(select(Obra).where(Obra.codigo == alvo.upper())).first()
    if obra:
        return obra
    obra = s.scalars(select(Obra).where(Obra.nome.ilike(alvo))).first()
    if obra:
        return obra
    return s.scalars(select(Obra).where(Obra.nome.ilike(f"%{alvo}%"))).first()


def _achar_categoria(s: Session, tipo_despesa: str) -> tuple[Optional[Categoria], str]:
    """Traduz o tipo de despesa do Pipefy (plano antigo) para a conta nova,
    via de-para. Devolve (categoria, como_resolveu)."""
    from app.apps.erp.core.cadastros.depara import resolver
    return resolver(s, tipo_despesa)


def importar_cards(s: Session, cards: list[dict[str, Any]], usuario: Usuario, *,
                   categoria_padrao_id: Optional[int] = None,
                   obra_padrao_id: Optional[int] = None,
                   criar_fornecedor: bool = True, baixar_anexos: bool = True) -> dict[str, Any]:
    """Cria títulos a partir dos cards. Devolve relatório detalhado —
    o que entrou, o que já existia e o que precisa de decisão humana."""
    importados, ja_existiam, pendencias = [], [], []

    for card in cards:
        d = extrair_dados(card)
        cid = d["card_id"]
        try:
            existente = s.scalars(select(Titulo).where(Titulo.ref_pipefy == cid)).first()
            if existente is not None:
                ja_existiam.append({"card": cid, "sp": existente.numero_sp})
                continue

            faltas = []
            if not d["parcelas"]:
                faltas.append("sem vencimento/valor de parcela no card")
            if not d["credor_documento"]:
                faltas.append(f"credor sem CNPJ/CPF válido no card "
                              f"(nome: {d['credor_nome'] or '—'})")
            if faltas:
                pendencias.append({"card": cid, "titulo": d["titulo_card"],
                                   "motivo": "; ".join(faltas)})
                continue

            # fornecedor: casa pelo documento; cria se autorizado
            forn = svc_forn.obter_por_documento(s, d["credor_documento"])
            if forn is None:
                if not criar_fornecedor:
                    pendencias.append({"card": cid, "titulo": d["titulo_card"],
                                       "motivo": f"fornecedor {d['credor_documento']} "
                                                 f"não cadastrado"})
                    continue
                forn = svc_forn.criar(s, {
                    "tipo_pessoa": d["credor_tipo"],
                    "cnpj_cpf": d["credor_documento"],
                    "razao_social": (d["credor_nome"] or "SEM NOME").upper(),
                    "origem": "IMPORTACAO_PIPEFY"}, usuario)

            # categoria: tipo de despesa do card → conta nova (via de-para)
            cat, como = _achar_categoria(s, d["tipo_despesa"])
            cat_id = cat.id if cat else categoria_padrao_id
            if not cat_id:
                if d.get("tipo_despesa_e_id"):
                    motivo = (f"o campo 'Tipo de Despesa' veio como ID de card "
                              f"({d['tipo_despesa']}) em vez do nome — o Pipefy não devolveu o "
                              f"rótulo do conector. Escolha a categoria padrão do lote ou "
                              f"cadastre a tradução deste ID em Configurações › Tradução.")
                else:
                    motivo = (f"tipo de despesa {d['tipo_despesa'] or '(vazio)'!r}: {como}. "
                              f"Defina a tradução em Configurações › Tradução do plano antigo.")
                pendencias.append({"card": cid, "titulo": d["titulo_card"],
                                   "tipo_despesa": d["tipo_despesa"], "motivo": motivo})
                continue

            # rateio: centros de custo → obras
            total_parcelas = sum(Decimal(p["valor"]) for p in d["parcelas"])
            rateios = []
            for r in d["rateios"]:
                obra = _achar_obra(s, r["centro_custo"])
                if obra is None:
                    continue
                rateios.append({"obra_id": obra.id,
                                "valor": str(r["valor"] or total_parcelas)})
            if not rateios:
                if obra_padrao_id:
                    rateios = [{"obra_id": obra_padrao_id, "valor": str(total_parcelas)}]
                else:
                    ccs = ", ".join(r["centro_custo"] for r in d["rateios"]) or "nenhum"
                    dica = (" (veio como ID de card, não como nome — escolha a obra padrão "
                            "do lote)" if any(r["centro_custo"].isdigit() for r in d["rateios"])
                            else "")
                    pendencias.append({"card": cid, "titulo": d["titulo_card"],
                                       "motivo": f"centro(s) de custo sem obra "
                                                 f"correspondente: {ccs}{dica}"})
                    continue
            # ajuste de centavos: rateio único fecha com o total
            if len(rateios) == 1:
                rateios[0]["valor"] = str(total_parcelas)

            # tipo interno derivado do que o card informa
            if d["tem_contrato"]:
                tipo = TipoTitulo.T4_LOCACAO
            elif d["nota_fiscal"]:
                tipo = TipoTitulo.T1_MATERIAL_NFE
            else:
                tipo = TipoTitulo.T14_EXCECAO_SEM_NOTA

            conta_id = None
            if d["forma_pagamento"] in ("PIX", "TED"):
                conta = next((c for c in forn.contas
                              if c.status == StatusConta.HOMOLOGADA
                              and c.forma.value == d["forma_pagamento"]), None)
                if conta is None:
                    # sem conta homologada: importa como boleto/guia é impossível,
                    # então registra pendência para o cadastro ser feito antes
                    pendencias.append({
                        "card": cid, "titulo": d["titulo_card"],
                        "motivo": f"{forn.razao_social} sem conta {d['forma_pagamento']} "
                                  f"HOMOLOGADA (chave no card: {d['chave_pix_no_card'] or '—'})"})
                    continue
                conta_id = conta.id

            payload = {
                "tipo": tipo.value,
                "fornecedor_id": forn.id,
                "categoria_id": cat_id,
                "descricao": d["descricao"] or d["titulo_card"],
                "valor_bruto": str(total_parcelas),
                "competencia": (d["data_solicitacao"] or d["parcelas"][0]["vencimento"]),
                "forma_pagamento": d["forma_pagamento"],
                "fornecedor_conta_id": conta_id,
                "parcelas": d["parcelas"],
                "rateios": rateios,
                "origem": "PIPEFY",
                "justificativa_excecao": (
                    f"Importado do card {cid} do Pipefy (SP {d['numero_sp_origem'] or '—'}, "
                    f"fase {d['fase']})" if tipo == TipoTitulo.T14_EXCECAO_SEM_NOTA else None),
            }
            titulo = svc_tit.criar_titulo(s, payload, usuario)
            titulo.ref_pipefy = cid
            s.flush()
            anexos = []
            if baixar_anexos:
                try:
                    anexos = baixar_anexos_do_card(s, d.get("campos_crus") or {},
                                                   titulo.id, usuario)
                except Exception as e:
                    logger.warning("Pipefy: falha ao trazer anexos do card %s (%s)", cid, e)

            importados.append({"card": cid, "sp": titulo.numero_sp,
                               "anexos": len([a for a in anexos if not a.get("erro")]),
                               "anexos_com_erro": [a for a in anexos if a.get("erro")],
                               "credor": forn.razao_social,
                               "categoria": f"{cat.codigo} · {cat.descricao}" if cat else "(padrão)",
                               "traducao": como if cat else "categoria padrão do lote",
                               "valor": float(titulo.valor_liquido),
                               "status": titulo.status.value,
                               "risco": titulo.score_risco})
            logger.info("ERP/Pipefy: card %s → %s", cid, titulo.numero_sp)
        except ErroValidacao as e:
            pendencias.append({"card": cid, "titulo": d.get("titulo_card", ""),
                               "motivo": str(e)})
        except Exception as e:  # falha inesperada não derruba o lote
            logger.exception("ERP/Pipefy: falha no card %s", cid)
            pendencias.append({"card": cid, "titulo": d.get("titulo_card", ""),
                               "motivo": f"erro inesperado: {e}"})

    return {"analisados": len(cards), "importados": importados,
            "ja_existiam": ja_existiam, "pendencias": pendencias}
