# -*- coding: utf-8 -*-
"""
ENCAMINHAR SPs PELO WHATSAPP — 10/10/2026.

O dono: *"eu queria poder selecionar, encaminhar informações pelo WhatsApp (…)
uma mini base de dados: nome, telefone (…) o botão de encaminhar, tanto no
lote quanto em solicitações (…) parecido com o modal do Consultar Omie (…)
três opções: manda informações, manda anexo, manda comprovante (…) uma
mensagenzinha padrão: número da SP, data que solicitou, vencimento, descrição,
valor, tipo de despesa, centro de custo, credor, CPF/CNPJ, responsável, forma
de pagamento — e, a depender da forma, a numeração do boleto ou a chave Pix
(…) por padrão manda tudo; desmarca o que não quer mandar."*

COMO SAI: pelo WhatsApp DE QUEM CLICA. A tela monta a mensagem e abre o
WhatsApp (o app, ou o WhatsApp Web) já com o texto na conversa do contato
escolhido — a pessoa só confere e aperta enviar. Anexo e comprovante vão como
LINK dentro da mensagem (o de baixar, quando o serviço tem um).

Por que não pelo robô (Z-API) da empresa: o número do robô é o mesmo dos avisos
do ponto e do ERP; o WhatsApp dele está desligado na tela Mensagens por causa
de bloqueio por volume, e mandar documento a fornecedor por ele é escrever em
nome da empresa sem ninguém conferir. Mudar isso é decisão do dono — está no
HISTORICO.

OS CONTATOS: uma lista só, da equipe toda, em `analisesps.meta` (sem migração):
nome e telefone. Quem encaminha escolhe na lista, ou "escolher na hora" — aí o
WhatsApp pergunta a conversa (serve para grupo).
"""
from __future__ import annotations

import json
import logging
import re
from urllib.parse import quote

logger = logging.getLogger("analisesps.encaminhar")

CHAVE_CONTATOS = "contatos_encaminhar"
MAX_SPS = 30
MAX_CONTATOS = 300

# Os campos da mensagem, na ordem em que saem. Todos vêm marcados; a pessoa
# desmarca o que não quer mandar.
CAMPOS = [
    ("id", "Nº da SP"),
    ("solicitacao", "Data da solicitação"),
    ("vencimento", "Vencimento"),
    ("descricao", "Descrição"),
    ("valor", "Valor"),
    ("tipo_despesa", "Tipo de despesa"),
    ("centro_custo", "Centro de custo"),
    ("credor", "Credor"),
    ("documento", "CPF/CNPJ"),
    ("responsavel", "Responsável"),
    ("forma_pagamento", "Forma de pagamento"),
    ("pagamento", "Boleto / chave Pix"),
    ("situacao", "Situação do pagamento"),
]
CAMPOS_POR_CHAVE = dict(CAMPOS)


# ---------------------------------------------------------------------------
# As SPs
# ---------------------------------------------------------------------------
def _sps(ids) -> list[dict]:
    from .db import consultar
    ids = list(dict.fromkeys(str(i).strip() for i in ids if str(i).strip()))[:MAX_SPS]
    if not ids:
        return []
    colunas = ("id", "solicitacao", "vencimento", "descricao", "valor_num", "valor",
               "tipo_despesa", "centro_custo", "credor", "documento", "responsavel",
               "forma_pagamento", "info_pgt", "codigo_barras", "status_pgt",
               "data_pagamento", "anexo_link", "comprovante")
    marcas = ", ".join("?" for _ in ids)
    linhas = consultar(f"SELECT {', '.join(colunas)} FROM analisesps.sps "
                       f" WHERE id IN ({marcas})", tuple(ids))
    por_id = {str(l[0]): dict(zip(colunas, l)) for l in linhas}
    # na ordem em que vieram marcadas
    return [por_id[i] for i in ids if i in por_id]


def _texto(v) -> str:
    return " ".join(str(v or "").split())


def linha_de_pagamento(sp: dict) -> tuple[str, str]:
    """(rótulo, valor) do que paga a SP, conforme a forma: boleto → a
    numeração; Pix → a chave; o resto → a informação para pagamento."""
    forma = _texto(sp.get("forma_pagamento")).lower()
    info = _texto(sp.get("info_pgt"))
    if "boleto" in forma:
        return "Código do boleto", _texto(sp.get("codigo_barras")) or info
    if "pix" in forma:
        return "Chave Pix", info
    return "Informação para pagamento", info


def _valores(sp: dict) -> dict:
    from .formatos import moeda
    pago = _texto(sp.get("status_pgt")).lower().startswith("pago")
    situacao = _texto(sp.get("status_pgt"))
    if pago and _texto(sp.get("data_pagamento")):
        situacao += f" em {_texto(sp.get('data_pagamento'))}"
    rotulo_pgt, valor_pgt = linha_de_pagamento(sp)
    valor = ("R$ " + moeda(sp["valor_num"]) if sp.get("valor_num") is not None
             else _texto(sp.get("valor")))
    return {"id": _texto(sp.get("id")), "solicitacao": _texto(sp.get("solicitacao")),
            "vencimento": _texto(sp.get("vencimento")),
            "descricao": _texto(sp.get("descricao")), "valor": valor,
            "tipo_despesa": _texto(sp.get("tipo_despesa")),
            "centro_custo": _texto(sp.get("centro_custo")),
            "credor": _texto(sp.get("credor")), "documento": _texto(sp.get("documento")),
            "responsavel": _texto(sp.get("responsavel")),
            "forma_pagamento": _texto(sp.get("forma_pagamento")),
            "pagamento": (rotulo_pgt, valor_pgt), "situacao": situacao}


def _links(texto) -> list[str]:
    from .formatos import link_de_download, links_da_celula
    return [link_de_download(u) for u in links_da_celula(texto)]


def previa(ids) -> list[dict]:
    """O que a janela lista: cada SP com o que ela TEM para mandar."""
    saida = []
    for sp in _sps(ids):
        v = _valores(sp)
        saida.append({"id": v["id"], "credor": v["credor"], "valor": v["valor"],
                      "situacao": v["situacao"],
                      "anexos": len(_links(sp.get("anexo_link"))),
                      "comprovantes": len(_links(sp.get("comprovante")))})
    return saida


def montar_mensagem(ids, campos=None, anexo=True, comprovante=True) -> str:
    """A mensagem pronta para o WhatsApp (negrito com *asteriscos*). Campo
    vazio não sai — "Chave Pix: " sem chave confunde mais que ajuda."""
    campos = [c for c in (campos if campos is not None else CAMPOS_POR_CHAVE)
              if c in CAMPOS_POR_CHAVE]
    blocos = []
    for sp in _sps(ids):
        v = _valores(sp)
        linhas = ["*Solicitação de Pagamento*"]
        for chave, rotulo in CAMPOS:
            if chave not in campos:
                continue
            if chave == "pagamento":
                rotulo, valor = v["pagamento"]
            else:
                valor = v[chave]
            if valor:
                linhas.append(f"*{rotulo}:* {valor}")
        if anexo:
            anexos = _links(sp.get("anexo_link"))
            for n, u in enumerate(anexos, start=1):
                linhas.append(f"📎 *Anexo{f' {n}' if len(anexos) > 1 else ''}:* {u}")
        if comprovante:
            comps = _links(sp.get("comprovante"))
            for n, u in enumerate(comps, start=1):
                linhas.append(f"🧾 *Comprovante{f' {n}' if len(comps) > 1 else ''}:* {u}")
        blocos.append("\n".join(linhas))
    return "\n\n———\n\n".join(blocos)


# ---------------------------------------------------------------------------
# Os contatos
# ---------------------------------------------------------------------------
def telefone_normalizado(bruto) -> str:
    """Só dígitos, com o 55 do Brasil na frente (o WhatsApp exige o país).
    Vazio se não parecer telefone (menos de 10 dígitos)."""
    digitos = re.sub(r"\D", "", str(bruto or ""))
    if len(digitos) < 10:
        return ""
    if not digitos.startswith("55") or len(digitos) <= 11:
        digitos = "55" + digitos
    return digitos


def listar_contatos() -> list[dict]:
    from .db import consultar_um
    try:
        linha = consultar_um("SELECT valor FROM analisesps.meta WHERE chave = ?",
                             (CHAVE_CONTATOS,))
        dados = json.loads(linha[0]) if linha and linha[0] else []
    except Exception:  # noqa: BLE001 — lista ruim não derruba a janela
        logger.exception("Encaminhar: não consegui ler os contatos")
        dados = []
    contatos = [c for c in dados if isinstance(c, dict) and c.get("telefone")]
    return sorted(contatos, key=lambda c: str(c.get("nome") or "").lower())


def _gravar(contatos: list[dict]) -> None:
    from .db import conexao
    from .sincronizacao import _meta_gravar
    with conexao() as conn:
        _meta_gravar(conn, CHAVE_CONTATOS, json.dumps(contatos, ensure_ascii=False))


def gravar_contato(nome, telefone, por: str = "") -> dict:
    nome = _texto(nome)[:80]
    tel = telefone_normalizado(telefone)
    if not nome:
        return {"ok": False, "erro": "Informe o nome do contato."}
    if not tel:
        return {"ok": False, "erro": "Telefone inválido — use DDD e número, ex.: 85 99999-9999."}
    contatos = [c for c in listar_contatos() if c.get("telefone") != tel]
    if len(contatos) >= MAX_CONTATOS:
        return {"ok": False, "erro": f"A lista chegou a {MAX_CONTATOS} contatos — apague algum."}
    contatos.append({"nome": nome, "telefone": tel, "por": _texto(por)[:80]})
    _gravar(contatos)
    return {"ok": True, "contatos": listar_contatos()}


def remover_contato(telefone) -> dict:
    tel = telefone_normalizado(telefone) or re.sub(r"\D", "", str(telefone or ""))
    _gravar([c for c in listar_contatos() if c.get("telefone") != tel])
    return {"ok": True, "contatos": listar_contatos()}


def link_whatsapp(telefone, texto) -> str:
    """O link que abre o WhatsApp com a mensagem pronta. Sem telefone, o
    WhatsApp pergunta para quem mandar (serve para grupo)."""
    tel = telefone_normalizado(telefone)
    return f"https://wa.me/{tel}?text={quote(texto or '')}" if tel \
        else f"https://wa.me/?text={quote(texto or '')}"
