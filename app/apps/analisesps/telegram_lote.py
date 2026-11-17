# -*- coding: utf-8 -*-
"""
O LOTE "WHATSAPP" ALIMENTADO PELO ROBÔ DO TELEGRAM — 08/10/2026 (migração 052).

O dono: *"Muitas pessoas me pedem para colocar para pagar alguma SP via
WhatsApp (…) eu copio essas informações, colo lá no Extrair SPs do lote (…)
como é que a gente poderia redirecionar essas mensagens e alimentar um lote
chamado WhatsApp (…) sempre o primeiro de todos (…) atrelar isso ao usuário."*

O caminho escolhido com ele: copiar as mensagens no WhatsApp (várias de uma
vez) e colar na conversa com o robô do Telegram da BWS — o mesmo que já manda
os avisos. O robô pesca os números de SP (o `extrair_ids` do Lote) e soma no
grupo "WhatsApp", no topo do lote de QUEM mandou.

COMO O ROBÔ SABE QUEM MANDOU: por um link de uso único gerado na tela do Lote,
por quem já entrou com a própria senha. O link abre o Telegram com
`/start lote_<código>`, e o robô liga aquela conversa ao usuário. Só o resumo
(sha256) do código fica no banco, e ele vale por `VALIDADE_DO_CONVITE` — um
link vazado depois disso não liga nada.

⚠️ NADA AQUI DERRUBA O ROBÔ. Ele atende os colaboradores no contracheque e no
cadastro; um erro do Análise de SPs não pode levar isso junto. Quem chama
(`telegram_bot.py`) embrulha em `try`, e daqui só sai texto ou None.
"""
from __future__ import annotations

import hashlib
import re
import logging
import os
import secrets

logger = logging.getLogger("analisesps.telegram_lote")

# O que vai depois do /start no link. O Telegram aceita até 64 caracteres de
# [A-Za-z0-9_-] — o código de `token_urlsafe(16)` tem 22.
PREFIXO_DO_CONVITE = "lote_"
VALIDADE_DO_CONVITE = "15 minutes"

MSG_NAO_LIGADO = (
    "Recebi número(s) de SP, mas esta conversa ainda não está ligada a um "
    "usuário do Análise de SPs. Entre no sistema, abra a tela Lote e toque em "
    "\"Ligar ao Telegram\".")


def pronto() -> bool:
    """A migração 052 já rodou? Antes dela, o robô segue como sempre foi."""
    from .db import tem_coluna
    return tem_coluna("lote_telegram", "sp")


def _resumo(codigo: str) -> str:
    return hashlib.sha256(str(codigo or "").encode("utf-8")).hexdigest()


def _endereco_do_robo() -> str:
    """O link do robô, como o `telegram_bot` já o conhece (TELEGRAM_BOT_LINK)."""
    bruto = os.environ.get("TELEGRAM_BOT_LINK", "t.me/bwsconstrucoesbotbot").strip()
    if not bruto.startswith("http"):
        bruto = "https://" + bruto.lstrip("/")
    return bruto.rstrip("/")


def _sem_markdown(texto: str) -> str:
    """O robô responde em Markdown: um "_" num nome abriria um itálico."""
    return "".join(c for c in str(texto or "") if c not in "_*[]`")


def _pessoa_do_lote(pessoa: dict) -> str:
    """A MESMA chave que a sessão usa ao entrar (`web.entrar`) — senão o robô
    escreveria num lote que a pessoa nunca abre."""
    from . import auth
    return auth.chave_pessoa(auth.limpar_nome(pessoa.get("nome") or pessoa["usuario"]))


def _alcanca_o_lote(pessoa: dict) -> bool:
    """Pode pôr SP no lote pela tela? Então pode pelo robô — e só então."""
    from . import auth
    if pessoa.get("mestre"):
        return True
    return bool(pessoa.get("pode_operar")) and "lote" in auth.telas_da_pessoa(pessoa)


# ---------------------------------------------------------------------------
# Ligar e desligar (a tela do Lote)
# ---------------------------------------------------------------------------
def gerar_convite(usuario_id: int) -> str:
    """Um link novo, de uso único. O anterior desta pessoa deixa de valer."""
    from .db import conexao
    codigo = secrets.token_urlsafe(16)
    with conexao() as conn:
        conn.execute("DELETE FROM analisesps.telegram_convite "
                     " WHERE usuario_id = ? OR criado_em < now() - interval '1 day'",
                     (int(usuario_id),))
        conn.execute("INSERT INTO analisesps.telegram_convite "
                     "(codigo_hash, usuario_id) VALUES (?, ?)",
                     (_resumo(codigo), int(usuario_id)))
        conn.commit()
    return f"{_endereco_do_robo()}?start={PREFIXO_DO_CONVITE}{codigo}"


def ligacao(usuario_id) -> dict | None:
    """Desde quando esta pessoa está ligada, ou None."""
    if not usuario_id or not pronto():
        return None
    from .db import consultar_um
    linha = consultar_um("SELECT ligado_em FROM analisesps.usuario_telegram "
                         " WHERE usuario_id = ?", (int(usuario_id),))
    return {"ligado_em": linha[0]} if linha else None


def desligar(usuario_id: int) -> bool:
    from .db import conexao
    with conexao() as conn:
        cur = conn.execute("DELETE FROM analisesps.usuario_telegram "
                           " WHERE usuario_id = ?", (int(usuario_id),))
        apagou = (cur.rowcount or 0) > 0
        conn.execute("DELETE FROM analisesps.telegram_convite WHERE usuario_id = ?",
                     (int(usuario_id),))
        conn.commit()
    return apagou


def ligar(codigo: str, chat_id) -> str:
    """O `/start lote_<código>`: liga a conversa ao dono do convite."""
    from . import usuarios
    from .db import conexao
    with conexao() as conn:
        # Apaga e devolve numa tacada só: dois toques no mesmo link não ligam
        # duas vezes, e o código morre aqui mesmo que algo falhe depois.
        cur = conn.execute(
            "DELETE FROM analisesps.telegram_convite WHERE codigo_hash = ? "
            "RETURNING usuario_id, criado_em > now() - interval "
            f"'{VALIDADE_DO_CONVITE}'", (_resumo(codigo),))
        linha = cur.fetchone()
        conn.commit()
    if not linha:
        return ("Este link de ligação não vale mais (ele é de uso único). "
                "Gere outro na tela Lote do Análise de SPs.")
    usuario_id, no_prazo = linha
    if not no_prazo:
        return ("Este link de ligação venceu (vale 15 minutos). Gere outro na "
                "tela Lote do Análise de SPs.")
    pessoa = usuarios.buscar_por_id(usuario_id)
    if not pessoa:
        return "Este usuário do Análise de SPs não está mais ativo."
    with conexao() as conn:
        # Uma conversa, uma pessoa — e uma pessoa, uma conversa. Ligar de novo
        # (celular novo) troca a anterior.
        conn.execute("DELETE FROM analisesps.usuario_telegram "
                     " WHERE chat_id = ? OR usuario_id = ?",
                     (int(chat_id), int(usuario_id)))
        conn.execute("INSERT INTO analisesps.usuario_telegram (usuario_id, chat_id) "
                     "VALUES (?, ?)", (int(usuario_id), int(chat_id)))
        conn.commit()
    logger.info("Análise de SPs: usuário %s ligado ao Telegram.", usuario_id)
    nome = _sem_markdown(pessoa.get("nome") or pessoa["usuario"])
    aviso = "" if _alcanca_o_lote(pessoa) else (
        "\n\nAtenção: o seu usuário ainda não pode alterar o Lote. Peça para "
        "liberarem a tela Lote com permissão de alterar.")
    return (f"Pronto, esta conversa está ligada ao usuário {nome} do Análise "
            "de SPs — e fica ligada de vez (só desliga pelo botão na tela do "
            "Lote).\n\nCole aqui as mensagens de pedido de pagamento (pode "
            "ser várias de uma vez): as SPs entram no grupo WhatsApp, no topo "
            f"do seu lote.{aviso}")


# ---------------------------------------------------------------------------
# Receber as SPs (o robô)
# ---------------------------------------------------------------------------
def _usuario_do_chat(chat_id):
    from . import usuarios
    from .db import consultar_um
    linha = consultar_um("SELECT usuario_id FROM analisesps.usuario_telegram "
                         " WHERE chat_id = ?", (int(chat_id),))
    return usuarios.buscar_por_id(linha[0]) if linha else None


def _parece_pedido_de_sp(texto: str) -> bool:
    """"Nº da SP", "SP 1426…", "Solicitação de Pagamento" — a cara das
    mensagens que o sistema manda e que são coladas aqui."""
    import unicodedata
    cru = unicodedata.normalize("NFKD", str(texto or ""))
    limpo = "".join(c for c in cru if not unicodedata.combining(c)).upper()
    return bool(re.search(r"\bSPS?\b|SOLICITACAO", limpo))


def receber(chat_id, texto: str) -> str | None:
    """A resposta do robô para esta mensagem — ou None, quando ela não é do
    Análise de SPs e o robô deve seguir o caminho de sempre (cadastro,
    contracheque)."""
    texto = str(texto or "").strip()
    if not texto or chat_id is None or not pronto():
        return None
    if texto.startswith("/start"):
        partes = texto.split(maxsplit=1)
        carga = partes[1].strip() if len(partes) > 1 else ""
        if carga.startswith(PREFIXO_DO_CONVITE):
            return ligar(carga[len(PREFIXO_DO_CONVITE):], chat_id)
        return None

    from . import lote
    ids = lote.extrair_ids(texto)
    if not ids:
        return None
    pessoa = _usuario_do_chat(chat_id)
    if not pessoa:
        # ⚠️ CONVERSA NÃO LIGADA é quase sempre um colaborador (contracheque,
        # cadastro). Um número de 10 dígitos sozinho — um telefone fixo com
        # DDD, por exemplo — não pode desviar ele do caminho de sempre: só
        # responde quem escreve como pedido de SP.
        return MSG_NAO_LIGADO if _parece_pedido_de_sp(texto) else None
    if not _alcanca_o_lote(pessoa):
        return ("O seu usuário do Análise de SPs não pode alterar o Lote. Peça "
                "para liberarem a tela Lote com permissão de alterar.")
    return _acrescentar(pessoa, ids)


# ⚠️ CADA ENVIO É UM GRUPO NOVO — WhatsApp1, WhatsApp2… (12/11/2026; ver
# `lote.juntar_num_grupo`). Mas o Telegram CORTA uma colagem longa em várias
# mensagens, que chegam juntas: o que chega até `JANELA_DO_MESMO_ENVIO_S` depois
# da mensagem anterior vai para o MESMO grupo, senão uma lista viraria três.
JANELA_DO_MESMO_ENVIO_S = 120
CHAVE_DO_ULTIMO_GRUPO = "telegram_lote.ultimo_grupo.{pessoa}"


def _ultimo_grupo(chave: str) -> dict:
    import json
    from .db import consultar_um
    try:
        linha = consultar_um("SELECT valor FROM analisesps.meta WHERE chave = ?",
                             (CHAVE_DO_ULTIMO_GRUPO.format(pessoa=chave),))
        dado = json.loads(linha[0]) if linha and linha[0] else {}
        return dado if isinstance(dado, dict) else {}
    except Exception:  # noqa: BLE001 — sem a memória, abre grupo novo
        return {}


def _lembrar_grupo(chave: str, titulo: str) -> None:
    import json
    from .db import conexao
    from .horario import agora
    from .sincronizacao import _meta_gravar
    with conexao() as conn:
        _meta_gravar(conn, CHAVE_DO_ULTIMO_GRUPO.format(pessoa=chave),
                     json.dumps({"grupo": titulo, "em": agora().isoformat()}))


def grupo_do_envio(chave: str, texto_atual: str) -> str:
    """O grupo desta mensagem: o da mensagem anterior, se ela chegou há pouco e o
    grupo ainda está no lote; senão, um novo (`lote.proximo_grupo_whatsapp`)."""
    import datetime as _dt
    from . import lote
    from .horario import agora
    ultimo = _ultimo_grupo(chave)
    titulos = {str(g.get("titulo") or "").strip().lower()
               for g in lote.separar_grupos(texto_atual)}
    try:
        quando = _dt.datetime.fromisoformat(ultimo.get("em") or "")
        recente = (agora() - quando).total_seconds() <= JANELA_DO_MESMO_ENVIO_S
    except (TypeError, ValueError):
        recente = False
    if recente and str(ultimo.get("grupo") or "").strip().lower() in titulos:
        return ultimo["grupo"]
    return lote.proximo_grupo_whatsapp(texto_atual)


def _acrescentar(pessoa: dict, ids: list[str]) -> str:
    from . import lote
    from .db import conexao, consultar
    chave = _pessoa_do_lote(pessoa)
    nome = pessoa.get("nome") or pessoa["usuario"]
    atual = lote.ler(chave)["conteudo"]
    ja_no_lote = {sp for g in lote.separar_grupos(atual) for sp in g["ids"]}
    titulo = grupo_do_envio(chave, atual)
    novo, entraram = lote.juntar_num_grupo(atual, ids, titulo)
    if entraram:
        _lembrar_grupo(chave, titulo)
        lote.salvar(novo, f"{nome} (Telegram)", chave)
        # A chegada leva a MESMA hora do lote salvo: a tela que abrir depois
        # carrega essa hora e, no "Salvar", não acha nada "chegado depois" —
        # e uma SP que a pessoa tirar de propósito não volta sozinha.
        salvo_em = lote.ler(chave).get("salvo_em")
        with conexao() as conn:
            for sp in entraram:
                conn.execute("INSERT INTO analisesps.lote_telegram "
                             "(pessoa, usuario_id, sp, chegou_em) "
                             "VALUES (?, ?, ?, COALESCE(?::timestamptz, now()))",
                             (chave, pessoa["id"], sp,
                              str(salvo_em) if salvo_em else None))
            conn.commit()

    marcadores = ", ".join("?" for _ in ids)
    na_base = {i for (i,) in consultar(
        f"SELECT id FROM analisesps.sps WHERE id IN ({marcadores})", tuple(ids))}
    onde = lote.onde_no_lote(ids, chave)

    linhas = []
    if entraram:
        linhas.append(f"{len(entraram)} SP(s) entraram no grupo {titulo} do seu lote:")
        linhas += [f"• {sp}" for sp in entraram]
    repetidas = [sp for sp in entraram if sp in ja_no_lote]
    if repetidas:
        linhas.append(f"{len(repetidas)} já estava(m) em outro grupo do seu lote e "
                      "entrou(aram) mesmo assim (fica repetida): " + ", ".join(repetidas))
    no_mesmo = [sp for sp in dict.fromkeys(ids) if sp not in entraram]
    if no_mesmo:
        linhas.append(f"{len(no_mesmo)} já estava(m) no grupo {titulo}: " + ", ".join(no_mesmo))
    fora = [sp for sp in ids if sp not in na_base]
    if fora:
        linhas.append("Não achei na base (a sincronização pode estar atrasada; "
                      "elas entraram mesmo assim): " + ", ".join(fora))
    for sp in ids:
        outros = (onde.get(sp) or {}).get("outros") or []
        if outros:
            linhas.append(f"Atenção: a {sp} também está no lote de "
                          + _sem_markdown(", ".join(outros)) + ".")
    return "\n".join(linhas)


def chegadas_desde(chave: str, desde) -> list[str]:
    """As SPs que chegaram pelo robô ao lote desta pessoa depois de `desde`.

    É a proteção da tela do Lote: quem abriu a janela às 9h e salvou às 9h30
    mandava de volta o texto das 9h — e o que o robô somou nesse meio tempo
    sumia sem ninguém ver. `desde` vazio quer dizer que a tela abriu com o
    lote nunca salvo: tudo o que chegou depois conta."""
    if not pronto():
        return []
    from .db import consultar
    if desde:
        linhas = consultar(
            "SELECT sp FROM analisesps.lote_telegram WHERE pessoa = ? "
            "   AND chegou_em > ?::timestamptz ORDER BY id", (chave, str(desde)))
    else:
        linhas = consultar("SELECT sp FROM analisesps.lote_telegram "
                           " WHERE pessoa = ? ORDER BY id", (chave,))
    return list(dict.fromkeys(sp for (sp,) in linhas))


def manter_chegadas(conteudo: str, chave: str, desde) -> tuple[str, list[str]]:
    """Devolve ao grupo WhatsApp o que chegou pelo robô depois que a tela abriu
    e não está no texto que ela mandou salvar."""
    from . import lote
    chegadas = chegadas_desde(chave, desde)
    if not chegadas:
        return conteudo, []
    presentes = {sp for g in lote.separar_grupos(conteudo) for sp in g["ids"]}
    faltam = [sp for sp in chegadas if sp not in presentes]
    if not faltam:
        return conteudo, []
    # Voltam para o grupo do último envio (ou um novo, se ele não está mais no texto).
    titulo = (_ultimo_grupo(chave).get("grupo") or "").strip()
    titulos = {str(g.get("titulo") or "").strip().lower() for g in lote.separar_grupos(conteudo)}
    if not titulo or titulo.lower() not in titulos:
        titulo = titulo or lote.proximo_grupo_whatsapp(conteudo)
    novo, entraram = lote.juntar_num_grupo(conteudo, faltam, titulo)
    return novo, entraram
